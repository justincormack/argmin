use std::fmt;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::control_plane::{
    AuthenticatedUnixControlPlaneClient, ClusterRuntimeMapSnapshot, ControlPlaneError,
    ControlPlaneRuntimeMapSource, PgMetadataProof, PgMetadataTransferProof, PgRouteSnapshot,
    UnixControlPlaneClient,
};
use crate::control_plane_auth::ControlPlaneScopedCredential;
use crate::peering::PgMetadataTransferArtifact;
use crate::storage_rpc_transport::StorageRpcClientEndpoint;
use crate::{
    ClusterEpoch, EcShape, FrontendStorageRpcClientCapability,
    LocalUnixStorageNodeClientAdmissionSettings, NodeId, PgId, PgState, StorageCluster,
};

enum LivePgMetadataTransferReadDispatch {
    Plain(UnixControlPlaneClient),
    Authenticated(AuthenticatedUnixControlPlaneClient),
}

enum LivePgMetadataTransferAdminDispatch {
    Plain(UnixControlPlaneClient),
    Authenticated(AuthenticatedUnixControlPlaneClient),
}

/// One authority-bound control-plane capability for live metadata transfer.
///
/// Read and mutation credentials are bound to one retained transport client
/// and their authenticated cluster identities are validated.
/// The full runtime-map source interface is deliberately not implemented.
pub struct LivePgMetadataTransferControlPlaneClient {
    read: LivePgMetadataTransferReadDispatch,
    admin: LivePgMetadataTransferAdminDispatch,
}

impl LivePgMetadataTransferControlPlaneClient {
    pub fn new(
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
        Ok(Self { read, admin })
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
        }
    }

    fn fence_with_source_lease(
        &self,
        pg_id: PgId,
    ) -> Result<crate::control_plane::FencedPgMetadataTransferRuntimeMap, ControlPlaneError> {
        match &self.admin {
            LivePgMetadataTransferAdminDispatch::Plain(client) => {
                client.fence_pg_for_metadata_transfer_runtime_map_with_source_lease_checked(pg_id)
            }
            LivePgMetadataTransferAdminDispatch::Authenticated(client) => client
                .fence_pg_for_metadata_transfer_runtime_map_with_source_lease_checked(
                    pg_id,
                    crate::clock::current_time_millis(),
                ),
        }
    }

    fn refresh_fence(&self, pg_id: PgId) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        match &self.admin {
            LivePgMetadataTransferAdminDispatch::Plain(client) => {
                client.fence_pg_for_metadata_transfer_runtime_map_checked(pg_id)
            }
            LivePgMetadataTransferAdminDispatch::Authenticated(client) => client
                .fence_pg_for_metadata_transfer_runtime_map_checked(
                    pg_id,
                    crate::clock::current_time_millis(),
                ),
        }
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
            LivePgMetadataTransferAdminDispatch::Authenticated(client) => client
                .set_pg_acting_set_with_metadata_transfer_runtime_map_checked(
                    pg_id,
                    acting_set,
                    transfer,
                    expected_destination_epoch,
                    crate::clock::current_time_millis(),
                ),
        }
    }
}

enum LivePgMetadataTransferStorageTransport {
    Unix {
        auth: Option<FrontendStorageRpcClientCapability>,
    },
    ConfiguredEndpoints {
        endpoints: Vec<(NodeId, StorageRpcClientEndpoint)>,
        auth: FrontendStorageRpcClientCapability,
    },
    #[cfg(test)]
    InProcess {
        data_dir: std::path::PathBuf,
        generation: std::sync::atomic::AtomicU64,
        export_route_refresh_failures: std::sync::atomic::AtomicU64,
        import_route_refresh_failures: std::sync::atomic::AtomicU64,
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
    _diagnostic: Box<str>,
}

impl LivePgMetadataTransferError {
    fn new(diagnostic: String) -> Self {
        let _ = observability::event(
            "storage_live_pg_transfer",
            "live_pg_metadata_transfer_error",
            Some(format_args!("diagnostic={diagnostic}")),
        );
        Self {
            _diagnostic: diagnostic.into_boxed_str(),
        }
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
            .field("diagnostic", &"<redacted>")
            .finish()
    }
}

impl fmt::Display for LivePgMetadataTransferError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("live PG metadata transfer failed")
    }
}

impl std::error::Error for LivePgMetadataTransferError {}

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
        self.imported_proof.applied_log_hash
    }

    #[must_use]
    pub fn imported_state_digest(&self) -> u64 {
        self.imported_proof.state_digest
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
pub struct LivePgMetadataTransferAdmin {
    control_plane: LivePgMetadataTransferControlPlaneClient,
    default_ec_shape: EcShape,
    admission_settings: LocalUnixStorageNodeClientAdmissionSettings,
    transport: LivePgMetadataTransferStorageTransport,
    failpoint: Option<LivePgMetadataTransferFailpoint>,
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
            transport: LivePgMetadataTransferStorageTransport::Unix { auth },
            failpoint: None,
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
                auth,
            },
            failpoint: None,
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
            },
            failpoint: None,
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
        self.transfer_typed(pg_id, acting_set)
            .map_err(LivePgMetadataTransferError::new)
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
            LivePgMetadataTransferStorageTransport::Unix { auth: Some(auth) } => {
                StorageCluster::from_runtime_map_with_unix_storage_node_client_admission_settings_and_frontend_auth(
                    metadata_primary_node_id,
                    runtime_map,
                    self.default_ec_shape,
                    self.admission_settings,
                    auth.clone(),
                )
            }
            LivePgMetadataTransferStorageTransport::Unix { auth: None } => {
                StorageCluster::from_runtime_map_with_unix_storage_node_client_admission_settings(
                    metadata_primary_node_id,
                    runtime_map,
                    self.default_ec_shape,
                    self.admission_settings,
                )
            }
            LivePgMetadataTransferStorageTransport::ConfiguredEndpoints { endpoints, auth } => {
                StorageCluster::from_runtime_map_with_storage_rpc_endpoints_and_frontend_auth(
                    metadata_primary_node_id,
                    runtime_map,
                    self.default_ec_shape,
                    self.admission_settings,
                    endpoints.iter().cloned(),
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
    ) -> Result<LivePgMetadataTransferSummary, String> {
        if let Some(summary) = self.completed_summary(pg_id, &acting_set)? {
            return Ok(summary);
        }
        let fenced = self
            .control_plane
            .fence_with_source_lease(pg_id)
            .map_err(|error| {
                format!(
                    "failed to fence live PG for metadata transfer: {}",
                    error.retained_diagnostic_message()
                )
            })?;
        let (fenced_runtime, source_lease_deadline_ms) = fenced.into_parts();
        let source_node_id = peering_source_node_id(&fenced_runtime, pg_id)?;
        if let Some(source_lease_deadline_ms) = source_lease_deadline_ms {
            wait_for_source_lease_to_expire(source_lease_deadline_ms);
        }
        let fenced_source_runtime = self.control_plane.refresh_fence(pg_id).map_err(|error| {
            format!(
                "failed to refresh fenced PG metadata transfer map: {}",
                error.retained_diagnostic_message()
            )
        })?;
        peering_source_route_matches(&fenced_source_runtime, pg_id, source_node_id)?;
        let expected_source_route = peering_route(&fenced_source_runtime, pg_id)?.clone();
        let source_runtime = self
            .control_plane
            .serving_pg_runtime_map_snapshot(pg_id, crate::clock::current_time_millis())
            .map_err(|error| {
                format!(
                    "failed to obtain serving metadata transfer source map: {}",
                    error.retained_diagnostic_message()
                )
            })?;
        let source_route = peering_route(&source_runtime, pg_id)?;
        if !route_matches_ignoring_global_epoch(&expected_source_route, source_route) {
            return Err(format!(
                "serving metadata transfer source for PG {} changed: expected route {:?}; actual route {:?}",
                pg_id.get(), expected_source_route, source_route
            ));
        }
        self.maybe_fail(LivePgMetadataTransferFailpoint::AfterFence)?;

        let (artifact, destination_runtime, import_epoch, imported_proof, source_node_id) =
            if let Some(existing_transfer) = source_route.peering_metadata_transfer() {
                if source_route.acting_set() != acting_set.as_slice() {
                    return Err(format!(
                        "PG {} already has transfer marker for acting set {:?}, not requested {:?}",
                        pg_id.get(),
                        source_route.acting_set(),
                        acting_set
                    ));
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
                            pg_id.get(), source_route_epoch.get(), error.retained_diagnostic_message()
                        )
                    })?;
                let export_route = peering_route(&export_runtime, pg_id)?;
                if export_route.primary_node_id() != existing_source_node_id {
                    return Err(format!(
                        "PG {} transfer marker source node {} does not match retained source route primary {}",
                        pg_id.get(), existing_source_node_id.as_u32(), export_route.primary_node_id().as_u32()
                    ));
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
                        pg_id.get(), artifact.source_metadata_proof(), artifact.cluster_epoch().get(), existing_transfer
                    ));
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
                        pg_id.get(), recomputed_imported_proof, existing_transfer.metadata_proof()
                    ));
                }
                let destination_runtime = source_runtime
                    .metadata_transfer_destination_runtime_map(
                        pg_id,
                        &acting_set,
                        existing_transfer,
                    )
                    .map_err(|error| {
                        format!(
                            "failed to authorize resumed PG {} metadata transfer destination route: {}",
                            pg_id.get(), error.retained_diagnostic_message()
                        )
                    })?;
                (
                    artifact,
                    destination_runtime,
                    destination_epoch,
                    existing_transfer.metadata_proof(),
                    existing_source_node_id,
                )
            } else {
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
                let install = self.install_transfer_retrying_epoch(
                    pg_id,
                    &acting_set,
                    source_route,
                    source_runtime.clone(),
                    &artifact,
                )?;
                match install {
                    TransferInstallOutcome::Ready {
                        destination_runtime,
                        destination_epoch,
                        imported_proof,
                    } => (
                        artifact,
                        destination_runtime,
                        destination_epoch,
                        imported_proof,
                        source_node_id,
                    ),
                    TransferInstallOutcome::Completed {
                        destination_epoch,
                        imported_proof,
                    } => {
                        self.maybe_fail(LivePgMetadataTransferFailpoint::AfterTransferInstall)?;
                        return Ok(summary(
                            source_node_id,
                            artifact.cluster_epoch(),
                            destination_epoch,
                            imported_proof,
                            false,
                        ));
                    }
                }
            };

        self.maybe_fail(LivePgMetadataTransferFailpoint::AfterTransferInstall)?;
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
                destination_epoch: import_epoch,
                expected_transfer,
                imported_proof,
            },
            &artifact,
        )?;
        if actual_imported_proof != imported_proof {
            return Err(format!(
                "imported PG metadata proof {:?} did not match expected {:?}",
                actual_imported_proof, imported_proof
            ));
        }
        self.maybe_fail(LivePgMetadataTransferFailpoint::AfterImport)?;
        Ok(summary(
            source_node_id,
            artifact.cluster_epoch(),
            import_epoch,
            imported_proof,
            false,
        ))
    }

    fn completed_summary(
        &self,
        pg_id: PgId,
        acting_set: &[NodeId],
    ) -> Result<Option<LivePgMetadataTransferSummary>, String> {
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
                    return Err(format!(
                        "failed to fetch control-plane PG {} runtime map before metadata transfer: {}",
                        pg_id.get(), error.retained_diagnostic_message()
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
        pg_id: PgId,
        acting_set: &[NodeId],
        source_route: &PgRouteSnapshot,
        mut source_runtime: ClusterRuntimeMapSnapshot,
        artifact: &PgMetadataTransferArtifact,
    ) -> Result<TransferInstallOutcome, String> {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let destination_epoch = source_runtime
                .cluster_epoch()
                .get()
                .checked_add(1)
                .and_then(ClusterEpoch::new)
                .ok_or_else(|| "destination cluster epoch overflowed".to_owned())?;
            let imported_proof = StorageCluster::metadata_transfer_imported_proof_at_epoch(
                artifact,
                destination_epoch,
            )
            .map_err(|error| format!("failed to compute imported PG metadata proof: {error}"))?;
            let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
                artifact.cluster_epoch(),
                artifact.source_metadata_proof(),
                imported_proof,
            );
            match self.control_plane.install_transfer(
                pg_id,
                acting_set.to_vec(),
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
                        ));
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
                                return Err(format!(
                                    "failed to obtain serving PG {} metadata transfer destination map after install: {}",
                                    pg_id.get(),
                                    error.retained_diagnostic_message()
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
                        ));
                    }
                    if active_route_matches(&serving_runtime, pg_id, acting_set, imported_proof)? {
                        return Ok(TransferInstallOutcome::Completed {
                            destination_epoch,
                            imported_proof,
                        });
                    }
                    let destination_runtime = serving_runtime
                        .metadata_transfer_destination_runtime_map(pg_id, acting_set, transfer)
                        .map_err(|error| {
                            format!(
                                "failed to authorize confirmed PG {} metadata transfer destination route: {}",
                                pg_id.get(),
                                error.retained_diagnostic_message()
                            )
                        })?;
                    return Ok(TransferInstallOutcome::Ready {
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
                        return Err("failed to install transfer-backed live PG acting set before the destination-epoch retry deadline".to_owned());
                    }
                    source_runtime = loop {
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
                                return Err(format!(
                                    "failed to refresh metadata transfer source map after destination epoch changed: {}",
                                    error.retained_diagnostic_message()
                                ));
                            }
                        }
                    };
                    let refreshed_route = peering_route(&source_runtime, pg_id)?;
                    if !route_matches_ignoring_global_epoch(source_route, refreshed_route) {
                        return Err(format!(
                            "metadata transfer source route for PG {} changed while rebasing the destination epoch: expected {:?}; actual {:?}",
                            pg_id.get(), source_route, refreshed_route
                        ));
                    }
                }
                Err(error) => {
                    return Err(format!(
                        "failed to install transfer-backed live PG acting set: {}",
                        error.retained_diagnostic_message()
                    ));
                }
            }
        }
    }

    fn export_retrying_stale_route(
        &self,
        initial_cluster: &Arc<StorageCluster>,
        context: ExportContext,
    ) -> Result<PgMetadataTransferArtifact, String> {
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
                        return Err(format!(
                            "timed out exporting PG metadata transfer artifact: {error}"
                        ));
                    }
                }
                Err(error) => {
                    return Err(format!(
                        "failed to export PG metadata transfer artifact: {error}"
                    ));
                }
            }
            thread::sleep(Duration::from_millis(100));
        }
    }

    fn refresh_export_route(
        &self,
        context: &ExportContext,
    ) -> Result<Option<Arc<StorageCluster>>, String> {
        let runtime_map = match self
            .control_plane
            .serving_pg_runtime_map_snapshot(context.pg_id, crate::clock::current_time_millis())
        {
            Ok(runtime_map) => runtime_map,
            Err(error) if error.is_retryable_runtime_map_observation_error() => return Ok(None),
            Err(error) => {
                return Err(format!(
                    "failed to refresh live PG {} metadata transfer source state: {}",
                    context.pg_id.get(),
                    error.retained_diagnostic_message()
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
        if !route_matches_ignoring_global_epoch(&context.expected_current_route, current_route) {
            return Err(format!(
                "refreshed metadata transfer source for PG {} changed: expected route {:?}; actual route {:?} at authoritative epoch {}",
                context.pg_id.get(), context.expected_current_route, current_route, runtime_map.cluster_epoch().get()
            ));
        }
        let export_runtime = runtime_map
            .metadata_transfer_source_runtime_map(
                &context.expected_current_route,
                context.export_epoch,
                context.source_node_id,
            )
            .map_err(|error| {
                format!(
                    "failed to authorize refreshed PG {} metadata transfer source route at epoch {}: {}",
                    context.pg_id.get(), context.export_epoch.get(), error.retained_diagnostic_message()
                )
            })?;
        let export_route = peering_route(&export_runtime, context.pg_id)?;
        if export_route.primary_node_id() != context.source_node_id {
            return Err(format!(
                "refreshed PG {} metadata transfer source route at epoch {} has primary {}, expected {}",
                context.pg_id.get(), context.export_epoch.get(), export_route.primary_node_id().as_u32(), context.source_node_id.as_u32()
            ));
        }
        self.build_cluster(&export_runtime).map(Some)
    }

    fn import_retrying_stale_route(
        &self,
        initial_cluster: &Arc<StorageCluster>,
        context: ImportContext<'_>,
        artifact: &PgMetadataTransferArtifact,
    ) -> Result<PgMetadataProof, String> {
        let mut cluster = Arc::clone(initial_cluster);
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut first_refresh_error = None;
        let mut refresh_failures = 0_u64;
        loop {
            let result = self.injected_route_refresh_failure(false).map_or_else(
                || cluster.import_pg_metadata_transfer_artifact_from_retained_log(artifact),
                Err,
            );
            match result {
                Ok(proof) => return Ok(proof),
                Err(error) if error.requires_route_refresh_retry() => {
                    refresh_failures = refresh_failures.saturating_add(1);
                    first_refresh_error.get_or_insert_with(|| error.to_string());
                    match self.refresh_import_route(&context)? {
                        ImportRouteRefresh::Completed => return Ok(context.imported_proof),
                        ImportRouteRefresh::Retry(refreshed) => cluster = refreshed,
                        ImportRouteRefresh::NotReady => {}
                    }
                    if Instant::now() >= deadline {
                        return Err(format!(
                            "timed out importing PG metadata transfer artifact after {refresh_failures} route-refresh failures (first: {}; last: {error})",
                            first_refresh_error.as_deref().unwrap_or("<unavailable>")
                        ));
                    }
                }
                Err(error) => {
                    return Err(format!(
                        "failed to import PG metadata transfer artifact: {error}"
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

    fn refresh_import_route(
        &self,
        context: &ImportContext<'_>,
    ) -> Result<ImportRouteRefresh, String> {
        let observed_runtime = match self
            .control_plane
            .pg_runtime_map_snapshot(context.pg_id, crate::clock::current_time_millis())
        {
            Ok(runtime) => runtime,
            Err(error) if error.is_retryable_runtime_map_observation_error() => {
                return Ok(ImportRouteRefresh::NotReady);
            }
            Err(error) => {
                return Err(format!(
                    "failed to refresh live PG {} metadata transfer state: {}",
                    context.pg_id.get(),
                    error.retained_diagnostic_message()
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
            ));
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
                return Err(format!(
                    "failed to obtain serving PG {} metadata transfer destination map: {}",
                    context.pg_id.get(),
                    error.retained_diagnostic_message()
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
        if !route_matches_ignoring_global_epoch(&expected_route, serving_route) {
            return Err(format!(
                "serving metadata transfer destination for PG {} changed after scoped observation",
                context.pg_id.get()
            ));
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
        self.build_cluster(&destination_runtime)
            .map(ImportRouteRefresh::Retry)
    }
}

struct ExportContext {
    pg_id: PgId,
    source_node_id: NodeId,
    expected_current_route: PgRouteSnapshot,
    export_epoch: ClusterEpoch,
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

fn route_matches_ignoring_global_epoch(
    expected: &PgRouteSnapshot,
    actual: &PgRouteSnapshot,
) -> bool {
    expected == &actual.with_cluster_epoch(expected.cluster_epoch())
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
    use std::sync::Mutex;

    use crate::control_plane::{
        ControlPlaneHeartbeatSink, ControlPlaneRpcServerListener, ControlPlaneRpcServerPolicy,
        ControlPlaneRpcServerRole, FileControlPlaneStore, NodeHeartbeat, NodeMembershipState,
        NodePgHeartbeatObservation, SingleAuthorityControlPlane, CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
    };
    use crate::control_plane_auth::{ControlPlaneAuthPrincipal, ControlPlaneScopedCredentialInput};

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
    fn live_admin_error_keeps_diagnostic_owner_local() {
        let secret = "route-proof-secret";
        let error = LivePgMetadataTransferError::new(format!("failed with {secret}"));

        assert!(error.retained_diagnostic_contains(secret));
        assert_eq!(error.to_string(), "live PG metadata transfer failed");
        assert_eq!(
            format!("{error:?}"),
            "LivePgMetadataTransferError { diagnostic: \"<redacted>\" }"
        );
        assert!(!error.to_string().contains(secret));
        assert!(!format!("{error:?}").contains(secret));
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
        assert_eq!(error.to_string(), "live PG metadata transfer failed");
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
        assert_eq!(error.to_string(), "live PG metadata transfer failed");
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
        assert_eq!(error.to_string(), "live PG metadata transfer failed");
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
            Some(PgMetadataProof {
                applied_log_index: summary.imported_log_index(),
                applied_log_hash: summary.imported_log_hash(),
                state_digest: summary.imported_state_digest(),
            })
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
        let imported_proof = PgMetadataProof {
            applied_log_index: 1,
            applied_log_hash: 2,
            state_digest: 3,
        };
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

        assert!(error.contains("does not match expected imported proof"));
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
    fn live_admin_refreshes_stale_source_and_destination_routes() {
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
            ],
        );
        let _time = crate::clock::test_time_override_guard(now_ms.saturating_add(100));

        let summary = live_transfer_admin(tmp.path(), &socket_path)
            .with_route_refresh_failures(1, 1)
            .transfer(pg_id.get(), vec![destination_node_id.as_u32()])
            .unwrap_or_else(|error| {
                panic!(
                    "route-refreshed live transfer failed: {}",
                    error._diagnostic
                )
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
        assert_eq!(route.acting_set(), &[destination_node_id]);
        assert!(route.peering_metadata_transfer().is_some());
    }
}
