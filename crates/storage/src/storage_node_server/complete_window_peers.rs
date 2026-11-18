// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Process-local receiver lookup routing. Never reconstructed from a shard
//! request or persisted as a second runtime-map format. A raw config install
//! deliberately removes this directory: only a complete map can repopulate it.

use super::*;
use crate::node_client::{LocalUnixStorageNodeClientAdmissionSettings, UnixStorageNodeClient};
use crate::storage_rpc_auth::StorageNodeStorageRpcClientCapability;
use crate::storage_rpc_transport::{StorageRpcClientEndpoint, StorageRpcEndpointAuthorityIdentity};

pub(super) struct PeerTransport {
    endpoints: BTreeMap<NodeId, StorageRpcClientEndpoint>,
    auth: StorageNodeStorageRpcClientCapability,
}

pub(super) struct WindowPeers {
    clients: BTreeMap<NodeId, UnixStorageNodeClient>,
}

fn invalid(message: &'static str) -> StorageNodeServerError {
    StoreError::Io {
        context: "receiver window peer configuration",
        source: io::Error::new(io::ErrorKind::InvalidInput, message),
    }
    .into()
}

impl PeerTransport {
    pub(super) fn require_auth(
        &self,
        config: &StorageNodeProcessConfig,
        auth: Option<&StorageRpcServerAuthConfig>,
    ) -> Result<(), StorageNodeServerError> {
        if !auth.is_some_and(|server| self.auth.matches_server(config.node_id, server)) {
            return Err(invalid(
                "receiver confirmation authentication domain mismatch",
            ));
        }
        Ok(())
    }
    fn new(
        endpoints: Vec<(NodeId, StorageRpcClientEndpoint)>,
        auth: StorageNodeStorageRpcClientCapability,
    ) -> Result<Self, StorageNodeServerError> {
        let mut selected = BTreeMap::new();
        for (node, endpoint) in endpoints {
            if selected.insert(node, endpoint).is_some() {
                return Err(invalid("duplicate receiver confirmation peer"));
            }
        }
        Ok(Self {
            endpoints: selected,
            auth,
        })
    }

    pub(super) fn bind(
        &self,
        config: &StorageNodeProcessConfig,
        map: &ClusterRuntimeMapSnapshot,
        auth: Option<&StorageRpcServerAuthConfig>,
    ) -> Result<WindowPeers, StorageNodeServerError> {
        self.require_auth(config, auth)?;
        let expected = StorageNodeProcessConfig::from_runtime_map(
            config.node_id,
            &config.data_dir,
            config.default_ec_shape,
            map,
        )?;
        if &expected != config {
            return Err(invalid(
                "receiver confirmation map differs from installed config",
            ));
        }
        if map.nodes().len() != self.endpoints.len() {
            return Err(invalid(
                "receiver confirmation endpoint set is incomplete or extraneous",
            ));
        }
        let topology = Arc::new(
            crate::PgTopology::new(&config.pg_ids)
                .map_err(|source| StoreError::InvalidPgTopology { source })?,
        );
        let rpc_auth = Some(Arc::new(self.auth.clone().into()));
        let mut clients = BTreeMap::new();
        for node in map.nodes() {
            let endpoint = self
                .endpoints
                .get(&node.node_id())
                .ok_or_else(|| invalid("receiver confirmation peer endpoint missing"))?;
            let matches = match endpoint.authority_identity() {
                StorageRpcEndpointAuthorityIdentity::Unix(path) => {
                    path == Path::new(node.endpoint())
                }
                StorageRpcEndpointAuthorityIdentity::Tcp(name) => name == node.endpoint(),
            };
            if !matches {
                return Err(invalid(
                    "receiver confirmation peer endpoint differs from runtime map",
                ));
            }
            clients.insert(
                node.node_id(),
                UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
                    node.node_id(),
                    config.cluster_epoch,
                    endpoint.clone(),
                    LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
                    rpc_auth.clone(),
                )
                .with_pg_topology(Arc::clone(&topology)),
            );
        }
        Ok(WindowPeers { clients })
    }
}

impl PreparedStorageNodeServer {
    /// Candidate bootstrap wiring, not a request-facing endpoint override.
    pub(in crate::node_runtime) fn with_window_confirmation_peers(
        mut self,
        map: ClusterRuntimeMapSnapshot,
        endpoints: Vec<(NodeId, StorageRpcClientEndpoint)>,
        auth: StorageNodeStorageRpcClientCapability,
    ) -> Result<Self, StorageNodeServerError> {
        let transport = Arc::new(PeerTransport::new(endpoints, auth)?);
        let peers = transport.bind(&self.config, &map, self.rpc_auth.as_deref())?;
        self.window_confirmation = Some((transport, Arc::new(peers)));
        Ok(self)
    }
}

/// Borrowed selection only, not physical authority. The receiver supplies the
/// object PG; primary and designated witness come exclusively from its map.
pub(super) struct Selection<'a> {
    pub(super) primary: &'a UnixStorageNodeClient,
    pub(super) witness: Option<&'a UnixStorageNodeClient>,
}

impl StorageNodeConnectionHandler {
    pub(super) fn window_confirmation_selection(
        &self,
        admission: &StorageNodeRouteAdmissionPermit,
        object_pg: PgId,
    ) -> Result<Selection<'_>, StorageRpcErrorResponse> {
        self.validate_active_admission(admission, "receiver window confirmation")?;
        let current = self
            .runtime_route_source
            .read()
            .unwrap_or_else(|e| e.into_inner());
        if !Arc::ptr_eq(&self.config, &current.config) {
            return Err(store_error_response(
                StoreError::RouteCapabilitySubjectMismatch {
                    operation: "receiver window confirmation captured runtime map",
                },
            ));
        }
        StorageNodeRouteFence::current(&self.config, self.route_map_lease).validate_rpc_at(
            crate::clock::current_time_millis(),
            crate::clock::monotonic_time_millis(),
        )?;
        let fail = || StorageRpcErrorResponse {
            code: StorageRpcErrorCode::PayloadDecode,
            message: "receiver window confirmation lacks active authoritative peer routing".into(),
        };
        let peers = self.window_confirmation_peers.as_ref().ok_or_else(fail)?;
        let route = self
            .config
            .pg_routes
            .iter()
            .find(|route| route.pg_id == object_pg.get())
            .ok_or_else(fail)?;
        if route.state != PgState::Active || route.cluster_epoch != self.config.cluster_epoch {
            return Err(fail());
        }
        let primary = peers.clients.get(&route.primary_node_id).ok_or_else(fail)?;
        let witness = route
            .acting_set
            .iter()
            .copied()
            .filter(|node| *node != route.primary_node_id)
            .min()
            .map(|node| peers.clients.get(&node).ok_or_else(fail))
            .transpose()?;
        Ok(Selection { primary, witness })
    }
}

#[cfg(test)]
#[path = "complete_window_peers_tests.rs"]
mod tests;

#[cfg(test)]
impl StorageNodeServer {
    /// Install only the fixture-owned endpoint directory. Real bootstrap/map
    /// publication validation is covered by the peer-routing owner tests.
    pub(in crate::node_runtime) fn test_install_window_receiver_peers(
        &self,
        endpoints: Vec<(NodeId, StorageRpcClientEndpoint)>,
        auth: StorageNodeStorageRpcClientCapability,
    ) {
        self.with_test_window_receiver(endpoints, auth, |handler, _| {
            self.runtime_route_state
                .write()
                .unwrap()
                .window_confirmation_peers = handler.window_confirmation_peers.clone();
        });
    }

    pub(in crate::node_runtime) fn test_disable_complete_log_operations(&mut self) {
        self.complete_log_operations = false;
    }

    pub(in crate::node_runtime) fn test_before_window_release(
        &self,
        hook: RuntimeConfigStageTestHook,
    ) {
        *self
            .metadata_command_before_release_test_hook
            .lock()
            .unwrap() = Some(hook);
    }

    pub(in crate::node_runtime) fn test_require_window_primary_released(&self, pg: PgId) {
        assert!(!self
            .metadata_command_locks
            .state
            .table
            .lock()
            .unwrap()
            .held
            .contains_key(&pg));
    }

    pub(in crate::node_runtime) fn test_receiver_route_validity(&self, millis: u64) {
        let mut config = self.config_snapshot();
        config.route_map_validity = RouteMapValidity::until_ms_saturating(millis);
        self.install_control_plane_runtime_config(config).unwrap();
    }
    /// The metadata fixture owns its route table and copied PG images. Only the
    /// process-local endpoint directory is injected here; production bootstrap
    /// and atomic directory publication have their separate owner-local matrix.
    pub(in crate::node_runtime) fn test_inspect_receiver_window(
        &self,
        endpoints: Vec<(NodeId, StorageRpcClientEndpoint)>,
        auth: StorageNodeStorageRpcClientCapability,
        request: crate::storage_rpc::complete_window_primary::Request<'_>,
        deadline: Instant,
        hook: impl FnMut(&'static str),
    ) -> Result<
        crate::node_runtime::pg_store::StagingWindowConfirmationObservation,
        StorageRpcErrorResponse,
    > {
        self.with_test_window_receiver(endpoints, auth, |handler, permit| {
            handler.inspect_receiver_window(
                permit,
                super::complete_window_receiver::ReceiverWindowRequest {
                    object_pg: request.route.pg,
                    descriptor: request.descriptor,
                    cutoff: request.route.deadline,
                },
                deadline,
                hook,
            )
        })
    }

    pub(super) fn with_test_window_receiver<T>(
        &self,
        endpoints: Vec<(NodeId, StorageRpcClientEndpoint)>,
        auth: StorageNodeStorageRpcClientCapability,
        action: impl FnOnce(&StorageNodeConnectionHandler, &StorageNodeRouteAdmissionPermit) -> T,
    ) -> T {
        let mut handler = self.connection_handler();
        assert!(auth.matches_server(handler.config.node_id, handler.rpc_auth.as_deref().unwrap()));
        let clients = endpoints
            .into_iter()
            .map(|(node, endpoint)| {
                (
                    node,
                    UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
                        node,
                        handler.config.cluster_epoch,
                        endpoint,
                        LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
                        Some(Arc::new(auth.clone().into())),
                    )
                    .with_pg_topology(Arc::new(handler.node.pg_topology().clone())),
                )
            })
            .collect();
        handler.window_confirmation_peers = Some(Arc::new(WindowPeers { clients }));
        let permit = self
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::Active);
        action(&handler, &permit)
    }
}
