use std::fmt;
use std::path::PathBuf;

use thiserror::Error;

use crate::control_plane::{
    AuthenticatedUnixControlPlaneClient, ClusterRuntimeMapSnapshot,
    ControlPlaneFrontendAuthCredential, ControlPlaneFrontendAuthCredentialInput,
    ControlPlaneHeartbeatRefresh, ControlPlaneHeartbeatRuntimeMapSource,
    ControlPlaneRpcClientEndpoint, ControlPlaneRuntimeMapDiagnostics, ControlPlaneRuntimeMapSource,
    ControlPlaneRuntimeMapStatus, ControlPlaneStorageNodeAuthCredential,
    ControlPlaneStorageNodeAuthCredentialInput, NodeHeartbeat,
    PendingMetadataCommandRecoveryListing, UnixControlPlaneClient,
};
use crate::control_plane_auth::ControlPlaneScopedCredential;
use crate::{NodeId, PgId};

enum ControlPlaneServiceClientDispatch {
    Plain(UnixControlPlaneClient),
    Authenticated(AuthenticatedUnixControlPlaneClient),
}

/// Opaque frontend control-plane transport and authentication capability.
pub struct ControlPlaneFrontendClient {
    dispatch: ControlPlaneServiceClientDispatch,
}

/// Opaque storage-node heartbeat transport and authentication capability.
pub struct ControlPlaneStorageNodeClient {
    dispatch: ControlPlaneServiceClientDispatch,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ControlPlaneServiceClientBootstrapError {
    #[error("control-plane service client transport configuration is invalid")]
    InvalidTransport,
    #[error("configured control-plane service authentication requires a cluster identity")]
    MissingClusterIdentity,
    #[error("configured frontend control-plane authentication requires a local instance id")]
    MissingFrontendInstanceIdentity,
    #[error(
        "configured frontend credentials do not include local frontend instance id {instance_id}"
    )]
    LocalFrontendCredentialUnavailable { instance_id: String },
    #[error("configured frontend signing credential is unavailable for instance {instance_id}")]
    SelectedFrontendCredentialUnavailable { instance_id: String },
    #[error("configured storage-node credentials do not include local storage node id {node_id}")]
    LocalStorageNodeCredentialUnavailable { node_id: u32 },
    #[error("configured storage-node signing credential is unavailable for node {node_id}")]
    SelectedStorageNodeCredentialUnavailable { node_id: u32 },
    #[error("configured control-plane service credential is invalid")]
    InvalidCredential,
}

impl ControlPlaneFrontendClient {
    pub fn with_endpoints(
        endpoints: impl IntoIterator<Item = ControlPlaneRpcClientEndpoint>,
        cluster_id: Option<&str>,
        instance_id: Option<&str>,
        credentials: Vec<ControlPlaneFrontendAuthCredentialInput>,
        selected_credential: Option<(String, u64)>,
    ) -> Result<Self, ControlPlaneServiceClientBootstrapError> {
        let client = UnixControlPlaneClient::with_endpoints(endpoints)
            .map_err(|_| ControlPlaneServiceClientBootstrapError::InvalidTransport)?;
        Self::bind(
            client,
            cluster_id,
            instance_id,
            credentials,
            selected_credential,
        )
    }

    pub fn with_socket_paths(
        socket_paths: impl IntoIterator<Item = PathBuf>,
        cluster_id: Option<&str>,
        instance_id: Option<&str>,
        credentials: Vec<ControlPlaneFrontendAuthCredentialInput>,
        selected_credential: Option<(String, u64)>,
    ) -> Result<Self, ControlPlaneServiceClientBootstrapError> {
        let client = UnixControlPlaneClient::with_socket_paths(socket_paths)
            .map_err(|_| ControlPlaneServiceClientBootstrapError::InvalidTransport)?;
        Self::bind(
            client,
            cluster_id,
            instance_id,
            credentials,
            selected_credential,
        )
    }

    fn bind(
        client: UnixControlPlaneClient,
        cluster_id: Option<&str>,
        instance_id: Option<&str>,
        credentials: Vec<ControlPlaneFrontendAuthCredentialInput>,
        selected_credential: Option<(String, u64)>,
    ) -> Result<Self, ControlPlaneServiceClientBootstrapError> {
        if credentials.is_empty() && selected_credential.is_none() {
            return Ok(Self {
                dispatch: ControlPlaneServiceClientDispatch::Plain(client),
            });
        }
        let cluster_id = required_cluster_id(cluster_id)?;
        let instance_id = instance_id
            .filter(|value| !value.is_empty())
            .ok_or(ControlPlaneServiceClientBootstrapError::MissingFrontendInstanceIdentity)?;
        let selected =
            select_frontend_credential(credentials, instance_id, selected_credential.as_ref())?;
        let credential = ControlPlaneFrontendAuthCredential::new(selected)
            .map_err(|_| ControlPlaneServiceClientBootstrapError::InvalidCredential)?
            .scoped_for_cluster(cluster_id)
            .map_err(|_| ControlPlaneServiceClientBootstrapError::InvalidCredential)?;
        Ok(Self::authenticated(client, credential))
    }

    fn authenticated(
        client: UnixControlPlaneClient,
        credential: ControlPlaneScopedCredential,
    ) -> Self {
        Self {
            dispatch: ControlPlaneServiceClientDispatch::Authenticated(
                AuthenticatedUnixControlPlaneClient::new(client, credential),
            ),
        }
    }

    pub(crate) fn retained_transport_and_credential(
        &self,
    ) -> (UnixControlPlaneClient, Option<ControlPlaneScopedCredential>) {
        match &self.dispatch {
            ControlPlaneServiceClientDispatch::Plain(client) => (client.clone(), None),
            ControlPlaneServiceClientDispatch::Authenticated(client) => {
                (client.inner().clone(), Some(client.credential().clone()))
            }
        }
    }

    #[must_use]
    pub fn is_authenticated(&self) -> bool {
        matches!(
            self.dispatch,
            ControlPlaneServiceClientDispatch::Authenticated(_)
        )
    }

    pub fn runtime_map_diagnostics(
        &self,
    ) -> Result<ControlPlaneRuntimeMapDiagnostics, crate::control_plane::ControlPlaneError> {
        match &self.dispatch {
            ControlPlaneServiceClientDispatch::Plain(client) => client.runtime_map_diagnostics(),
            ControlPlaneServiceClientDispatch::Authenticated(client) => {
                client.runtime_map_diagnostics(crate::clock::current_time_millis())
            }
        }
    }

    pub fn runtime_map_status_with_check_applied_timeout(
        &self,
    ) -> Result<ControlPlaneRuntimeMapStatus, crate::control_plane::ControlPlaneError> {
        match &self.dispatch {
            ControlPlaneServiceClientDispatch::Plain(client) => {
                client.runtime_map_status_with_check_applied_timeout()
            }
            ControlPlaneServiceClientDispatch::Authenticated(client) => client
                .runtime_map_status_with_check_applied_timeout(crate::clock::current_time_millis()),
        }
    }
}

impl ControlPlaneRuntimeMapSource for ControlPlaneFrontendClient {
    fn runtime_map_snapshot(
        &self,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, crate::control_plane::ControlPlaneError> {
        match &self.dispatch {
            ControlPlaneServiceClientDispatch::Plain(client) => {
                client.runtime_map_snapshot(authority_now_ms)
            }
            ControlPlaneServiceClientDispatch::Authenticated(client) => {
                client.runtime_map_snapshot(authority_now_ms)
            }
        }
    }

    fn runtime_map_status(
        &self,
        authority_now_ms: u64,
    ) -> Result<ControlPlaneRuntimeMapStatus, crate::control_plane::ControlPlaneError> {
        match &self.dispatch {
            ControlPlaneServiceClientDispatch::Plain(client) => {
                client.runtime_map_status(authority_now_ms)
            }
            ControlPlaneServiceClientDispatch::Authenticated(client) => {
                client.runtime_map_status(authority_now_ms)
            }
        }
    }

    fn pending_metadata_command_recoveries(
        &self,
        authority_now_ms: u64,
    ) -> Result<PendingMetadataCommandRecoveryListing, crate::control_plane::ControlPlaneError>
    {
        match &self.dispatch {
            ControlPlaneServiceClientDispatch::Plain(client) => {
                client.pending_metadata_command_recoveries()
            }
            ControlPlaneServiceClientDispatch::Authenticated(client) => {
                client.pending_metadata_command_recoveries(authority_now_ms)
            }
        }
    }

    fn pg_runtime_map_snapshot(
        &self,
        pg_id: PgId,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, crate::control_plane::ControlPlaneError> {
        match &self.dispatch {
            ControlPlaneServiceClientDispatch::Plain(client) => {
                client.pg_runtime_map_snapshot(pg_id, authority_now_ms)
            }
            ControlPlaneServiceClientDispatch::Authenticated(client) => {
                client.pg_runtime_map_snapshot(pg_id, authority_now_ms)
            }
        }
    }

    fn serving_pg_runtime_map_snapshot(
        &self,
        pg_id: PgId,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, crate::control_plane::ControlPlaneError> {
        match &self.dispatch {
            ControlPlaneServiceClientDispatch::Plain(client) => {
                client.serving_pg_runtime_map_snapshot(pg_id, authority_now_ms)
            }
            ControlPlaneServiceClientDispatch::Authenticated(client) => {
                client.serving_pg_runtime_map_snapshot(pg_id, authority_now_ms)
            }
        }
    }
}

impl ControlPlaneStorageNodeClient {
    pub fn with_endpoints(
        endpoints: impl IntoIterator<Item = ControlPlaneRpcClientEndpoint>,
        cluster_id: Option<&str>,
        node_id: u32,
        node_incarnation: u64,
        credentials: Vec<ControlPlaneStorageNodeAuthCredentialInput>,
        selected_credential: Option<(String, u64)>,
    ) -> Result<Self, ControlPlaneServiceClientBootstrapError> {
        let client = UnixControlPlaneClient::with_endpoints(endpoints)
            .map_err(|_| ControlPlaneServiceClientBootstrapError::InvalidTransport)?;
        Self::bind(
            client,
            cluster_id,
            node_id,
            node_incarnation,
            credentials,
            selected_credential,
        )
    }

    pub fn with_socket_paths(
        socket_paths: impl IntoIterator<Item = PathBuf>,
        cluster_id: Option<&str>,
        node_id: u32,
        node_incarnation: u64,
        credentials: Vec<ControlPlaneStorageNodeAuthCredentialInput>,
        selected_credential: Option<(String, u64)>,
    ) -> Result<Self, ControlPlaneServiceClientBootstrapError> {
        let client = UnixControlPlaneClient::with_socket_paths(socket_paths)
            .map_err(|_| ControlPlaneServiceClientBootstrapError::InvalidTransport)?;
        Self::bind(
            client,
            cluster_id,
            node_id,
            node_incarnation,
            credentials,
            selected_credential,
        )
    }

    fn bind(
        client: UnixControlPlaneClient,
        cluster_id: Option<&str>,
        node_id: u32,
        node_incarnation: u64,
        credentials: Vec<ControlPlaneStorageNodeAuthCredentialInput>,
        selected_credential: Option<(String, u64)>,
    ) -> Result<Self, ControlPlaneServiceClientBootstrapError> {
        if credentials.is_empty() && selected_credential.is_none() {
            return Ok(Self {
                dispatch: ControlPlaneServiceClientDispatch::Plain(client),
            });
        }
        let cluster_id = required_cluster_id(cluster_id)?;
        let selected =
            select_storage_node_credential(credentials, node_id, selected_credential.as_ref())?;
        let credential = ControlPlaneStorageNodeAuthCredential::new(selected)
            .map_err(|_| ControlPlaneServiceClientBootstrapError::InvalidCredential)?
            .scoped_for_cluster_and_incarnation(cluster_id, node_incarnation)
            .map_err(|_| ControlPlaneServiceClientBootstrapError::InvalidCredential)?;
        Ok(Self {
            dispatch: ControlPlaneServiceClientDispatch::Authenticated(
                AuthenticatedUnixControlPlaneClient::new(client, credential),
            ),
        })
    }

    #[must_use]
    pub fn is_authenticated(&self) -> bool {
        matches!(
            self.dispatch,
            ControlPlaneServiceClientDispatch::Authenticated(_)
        )
    }
}

impl ControlPlaneHeartbeatRuntimeMapSource for ControlPlaneStorageNodeClient {
    fn refresh_node_heartbeat(
        &mut self,
        heartbeat: NodeHeartbeat,
        authority_now_ms: u64,
    ) -> Result<ControlPlaneHeartbeatRefresh, crate::control_plane::ControlPlaneError> {
        match &mut self.dispatch {
            ControlPlaneServiceClientDispatch::Plain(client) => {
                client.refresh_node_heartbeat(heartbeat, authority_now_ms)
            }
            ControlPlaneServiceClientDispatch::Authenticated(client) => {
                client.refresh_node_heartbeat(heartbeat, authority_now_ms)
            }
        }
    }
}

impl fmt::Debug for ControlPlaneFrontendClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlPlaneFrontendClient")
            .field("authenticated", &self.is_authenticated())
            .field("transport", &"<opaque>")
            .finish()
    }
}

impl fmt::Debug for ControlPlaneStorageNodeClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlPlaneStorageNodeClient")
            .field("authenticated", &self.is_authenticated())
            .field("transport", &"<opaque>")
            .finish()
    }
}

fn required_cluster_id(
    cluster_id: Option<&str>,
) -> Result<&str, ControlPlaneServiceClientBootstrapError> {
    cluster_id
        .filter(|value| !value.is_empty())
        .ok_or(ControlPlaneServiceClientBootstrapError::MissingClusterIdentity)
}

fn select_frontend_credential(
    credentials: Vec<ControlPlaneFrontendAuthCredentialInput>,
    instance_id: &str,
    selected: Option<&(String, u64)>,
) -> Result<ControlPlaneFrontendAuthCredentialInput, ControlPlaneServiceClientBootstrapError> {
    let mut local = credentials
        .into_iter()
        .filter(|credential| credential.instance_id == instance_id);
    match selected {
        Some((credential_id, credential_version)) => local
            .find(|credential| {
                credential.credential_id == *credential_id
                    && credential.credential_version == *credential_version
            })
            .ok_or_else(|| {
                ControlPlaneServiceClientBootstrapError::SelectedFrontendCredentialUnavailable {
                    instance_id: instance_id.to_owned(),
                }
            }),
        None => local
            .max_by(|left, right| {
                left.credential_version
                    .cmp(&right.credential_version)
                    .then_with(|| left.credential_id.cmp(&right.credential_id))
            })
            .ok_or_else(|| {
                ControlPlaneServiceClientBootstrapError::LocalFrontendCredentialUnavailable {
                    instance_id: instance_id.to_owned(),
                }
            }),
    }
}

fn select_storage_node_credential(
    credentials: Vec<ControlPlaneStorageNodeAuthCredentialInput>,
    node_id: u32,
    selected: Option<&(String, u64)>,
) -> Result<ControlPlaneStorageNodeAuthCredentialInput, ControlPlaneServiceClientBootstrapError> {
    let mut local = credentials
        .into_iter()
        .filter(|credential| credential.node_id == NodeId::new(node_id));
    match selected {
        Some((credential_id, credential_version)) => local
            .find(|credential| {
                credential.credential_id == *credential_id
                    && credential.credential_version == *credential_version
            })
            .ok_or(
                ControlPlaneServiceClientBootstrapError::SelectedStorageNodeCredentialUnavailable {
                    node_id,
                },
            ),
        None => local
            .max_by(|left, right| {
                left.credential_version
                    .cmp(&right.credential_version)
                    .then_with(|| left.credential_id.cmp(&right.credential_id))
            })
            .ok_or(
                ControlPlaneServiceClientBootstrapError::LocalStorageNodeCredentialUnavailable {
                    node_id,
                },
            ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frontend_credential(
        instance_id: &str,
        credential_id: &str,
        credential_version: u64,
    ) -> ControlPlaneFrontendAuthCredentialInput {
        ControlPlaneFrontendAuthCredentialInput {
            instance_id: instance_id.to_owned(),
            credential_id: credential_id.to_owned(),
            credential_version,
            secret: format!("{instance_id}-{credential_id}-{credential_version}-secret")
                .into_bytes(),
        }
    }

    fn storage_credential(
        node_id: u32,
        credential_id: &str,
        credential_version: u64,
    ) -> ControlPlaneStorageNodeAuthCredentialInput {
        ControlPlaneStorageNodeAuthCredentialInput {
            node_id: NodeId::new(node_id),
            credential_id: credential_id.to_owned(),
            credential_version,
            secret: format!("{node_id}-{credential_id}-{credential_version}-secret").into_bytes(),
        }
    }

    #[test]
    fn frontend_client_selects_default_and_explicit_local_credentials() {
        let credentials = vec![
            frontend_credential("frontend-a", "older", 1),
            frontend_credential("frontend-b", "foreign", 9),
            frontend_credential("frontend-a", "newer", 2),
        ];
        let latest = ControlPlaneFrontendClient::with_socket_paths(
            [PathBuf::from("/tmp/control-plane.sock")],
            Some("cluster-a"),
            Some("frontend-a"),
            credentials.clone(),
            None,
        )
        .unwrap();
        let (_, latest_credential) = latest.retained_transport_and_credential();
        assert_eq!(
            latest_credential.unwrap().credential_id(),
            "newer",
            "default selection must use the latest local credential"
        );

        let explicit = ControlPlaneFrontendClient::with_socket_paths(
            [PathBuf::from("/tmp/control-plane.sock")],
            Some("cluster-a"),
            Some("frontend-a"),
            credentials,
            Some(("older".to_owned(), 1)),
        )
        .unwrap();
        let (_, explicit_credential) = explicit.retained_transport_and_credential();
        assert_eq!(explicit_credential.unwrap().credential_id(), "older");
        let debug = format!("{explicit:?}");
        assert!(!debug.contains("older"));
        assert!(!debug.contains("secret"));
        assert!(!debug.contains("control-plane.sock"));
    }

    #[test]
    fn storage_node_client_binds_node_incarnation_and_selected_credential() {
        let client = ControlPlaneStorageNodeClient::with_socket_paths(
            [PathBuf::from("/tmp/control-plane.sock")],
            Some("cluster-a"),
            7,
            41,
            vec![
                storage_credential(7, "older", 1),
                storage_credential(8, "foreign", 9),
                storage_credential(7, "newer", 2),
            ],
            Some(("older".to_owned(), 1)),
        )
        .unwrap();
        let ControlPlaneServiceClientDispatch::Authenticated(client) = &client.dispatch else {
            panic!("configured storage-node client must be authenticated")
        };
        assert_eq!(client.credential().credential_id(), "older");
        assert!(matches!(
            client.credential().principal(),
            crate::control_plane_auth::ControlPlaneAuthPrincipal::StorageNode {
                node_id,
                incarnation: 41,
            } if *node_id == NodeId::new(7)
        ));
    }

    #[test]
    fn service_clients_reject_crossed_and_incomplete_configuration_without_network_use() {
        assert_eq!(
            ControlPlaneFrontendClient::with_socket_paths(
                [PathBuf::from("/tmp/not-contacted.sock")],
                None,
                Some("frontend-a"),
                vec![frontend_credential("frontend-a", "key", 1)],
                None,
            )
            .unwrap_err(),
            ControlPlaneServiceClientBootstrapError::MissingClusterIdentity
        );
        assert_eq!(
            ControlPlaneFrontendClient::with_socket_paths(
                [PathBuf::from("/tmp/not-contacted.sock")],
                Some("cluster-a"),
                Some("frontend-a"),
                vec![frontend_credential("frontend-b", "key", 1)],
                None,
            )
            .unwrap_err(),
            ControlPlaneServiceClientBootstrapError::LocalFrontendCredentialUnavailable {
                instance_id: "frontend-a".to_owned(),
            }
        );
        assert_eq!(
            ControlPlaneStorageNodeClient::with_socket_paths(
                [PathBuf::from("/tmp/not-contacted.sock")],
                Some("cluster-a"),
                7,
                41,
                vec![storage_credential(8, "key", 1)],
                None,
            )
            .unwrap_err(),
            ControlPlaneServiceClientBootstrapError::LocalStorageNodeCredentialUnavailable {
                node_id: 7,
            }
        );
    }
}
