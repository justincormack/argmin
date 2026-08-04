use std::fmt;
use std::fmt::Write as _;
use std::sync::Arc;

use thiserror::Error;

use crate::control_plane::{
    ControlPlaneAdminAuthCredential, ControlPlaneAdminAuthCredentialInput,
    ControlPlaneFrontendAuthCredential, ControlPlaneFrontendAuthCredentialInput,
    ControlPlaneStorageNodeAuthCredential, ControlPlaneStorageNodeAuthCredentialInput,
    ControlPlaneUnixAuthVerifier,
};
use crate::control_plane_auth::validate_control_plane_auth_cluster_id;

/// Opaque authentication capability for control-plane RPC servers.
///
/// The process layer supplies logical credential configuration. Storage owns
/// credential validation, verifier construction, request-role binding, metrics,
/// and the diagnostic representation.
#[derive(Clone)]
pub struct ControlPlaneRpcServerAuth {
    pub(crate) verifier: Option<Arc<ControlPlaneUnixAuthVerifier>>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ControlPlaneRpcServerAuthError {
    #[error("configured control-plane server authentication requires a cluster identity")]
    MissingClusterIdentity,
    #[error("configured control-plane server cluster identity is invalid")]
    InvalidClusterIdentity,
    #[error(
        "admin control-plane credentials are required when server authentication is configured"
    )]
    AdminCredentialsRequired,
    #[error("configured admin credentials do not include local admin instance id {instance_id}")]
    LocalAdminCredentialUnavailable { instance_id: String },
    #[error("configured control-plane server credential is invalid")]
    InvalidCredential,
}

impl fmt::Debug for ControlPlaneRpcServerAuth {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlPlaneRpcServerAuth")
            .field("authenticated", &self.is_authenticated())
            .finish()
    }
}

impl ControlPlaneRpcServerAuth {
    pub fn new(
        cluster_id: Option<&str>,
        local_admin_instance_id: Option<&str>,
        storage_node_credentials: Vec<ControlPlaneStorageNodeAuthCredentialInput>,
        frontend_credentials: Vec<ControlPlaneFrontendAuthCredentialInput>,
        admin_credentials: Vec<ControlPlaneAdminAuthCredentialInput>,
    ) -> Result<Self, ControlPlaneRpcServerAuthError> {
        if storage_node_credentials.is_empty()
            && frontend_credentials.is_empty()
            && admin_credentials.is_empty()
        {
            return Ok(Self { verifier: None });
        }
        let cluster_id = cluster_id
            .filter(|value| !value.is_empty())
            .ok_or(ControlPlaneRpcServerAuthError::MissingClusterIdentity)?;
        validate_control_plane_auth_cluster_id(cluster_id)
            .map_err(|_| ControlPlaneRpcServerAuthError::InvalidClusterIdentity)?;
        if admin_credentials.is_empty() {
            return Err(ControlPlaneRpcServerAuthError::AdminCredentialsRequired);
        }

        let storage_node_credentials = storage_node_credentials
            .into_iter()
            .map(ControlPlaneStorageNodeAuthCredential::new)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| ControlPlaneRpcServerAuthError::InvalidCredential)?;
        let frontend_credentials = frontend_credentials
            .into_iter()
            .map(ControlPlaneFrontendAuthCredential::new)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| ControlPlaneRpcServerAuthError::InvalidCredential)?;
        let admin_credentials = admin_credentials
            .into_iter()
            .map(ControlPlaneAdminAuthCredential::new)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| ControlPlaneRpcServerAuthError::InvalidCredential)?;
        if let Some(instance_id) = local_admin_instance_id {
            if !admin_credentials
                .iter()
                .any(|credential| credential.instance_id() == instance_id)
            {
                return Err(
                    ControlPlaneRpcServerAuthError::LocalAdminCredentialUnavailable {
                        instance_id: instance_id.to_owned(),
                    },
                );
            }
        }

        let verifier = if storage_node_credentials.is_empty() {
            ControlPlaneUnixAuthVerifier::new_empty(cluster_id)
        } else {
            ControlPlaneUnixAuthVerifier::new(cluster_id, storage_node_credentials)
        }
        .map_err(|_| ControlPlaneRpcServerAuthError::InvalidCredential)?
        .with_frontend_credentials(frontend_credentials)
        .map_err(|_| ControlPlaneRpcServerAuthError::InvalidCredential)?
        .with_admin_credentials(admin_credentials)
        .map_err(|_| ControlPlaneRpcServerAuthError::InvalidCredential)?;

        Ok(Self {
            verifier: Some(Arc::new(verifier)),
        })
    }

    #[must_use]
    pub fn is_authenticated(&self) -> bool {
        self.verifier.is_some()
    }

    /// Returns the storage-owned, redacted server-authentication diagnostic.
    #[must_use]
    pub fn diagnostics(&self) -> Option<String> {
        let verifier = self.verifier.as_ref()?;
        let status = verifier.status_snapshot();
        let metrics = status.metrics();
        let mut diagnostics = format!(
            "control_plane_unix_auth required={} storage_node_heartbeat_required={} frontend_runtime_map_required={} admin_control_plane_required={} cluster_id={} storage_node_credentials={} frontend_credentials={} admin_credentials={} accepted_total={} rejected_total={}",
            status.required(),
            status.storage_node_heartbeat_required(),
            status.frontend_runtime_map_required(),
            status.admin_control_plane_required(),
            diagnostic_identifier(status.cluster_id()),
            status.storage_node_credentials().len(),
            status.frontend_credentials().len(),
            status.admin_credentials().len(),
            metrics.accepted_total(),
            metrics.rejected_total()
        );
        for credential in status.storage_node_credentials() {
            diagnostics.push('\n');
            write!(
                &mut diagnostics,
                "control_plane_unix_auth storage_node_credential{{node_id=\"{}\",credential_id={},credential_version=\"{}\"}} 1",
                credential.node_id().as_u32(),
                diagnostic_identifier(credential.credential_id()),
                credential.credential_version()
            )
            .expect("write to String should not fail");
        }
        for credential in status.frontend_credentials() {
            diagnostics.push('\n');
            write!(
                &mut diagnostics,
                "control_plane_unix_auth frontend_credential{{instance_id={},credential_id={},credential_version=\"{}\"}} 1",
                diagnostic_identifier(credential.instance_id()),
                diagnostic_identifier(credential.credential_id()),
                credential.credential_version()
            )
            .expect("write to String should not fail");
        }
        for credential in status.admin_credentials() {
            diagnostics.push('\n');
            write!(
                &mut diagnostics,
                "control_plane_unix_auth admin_credential{{instance_id={},credential_id={},credential_version=\"{}\"}} 1",
                diagnostic_identifier(credential.instance_id()),
                diagnostic_identifier(credential.credential_id()),
                credential.credential_version()
            )
            .expect("write to String should not fail");
        }
        for (operation, count) in metrics.accepted_by_operation() {
            diagnostics.push('\n');
            write!(
                &mut diagnostics,
                "control_plane_unix_auth accepted_by_operation{{operation=\"{operation:?}\"}} {count}"
            )
            .expect("write to String should not fail");
        }
        for (operation, count) in metrics.rejected_by_operation() {
            diagnostics.push('\n');
            write!(
                &mut diagnostics,
                "control_plane_unix_auth rejected_by_operation{{operation=\"{operation:?}\"}} {count}"
            )
            .expect("write to String should not fail");
        }
        for (reason, count) in metrics.rejected_by_reason() {
            diagnostics.push('\n');
            write!(
                &mut diagnostics,
                "control_plane_unix_auth rejected_by_reason{{reason=\"{reason:?}\"}} {count}"
            )
            .expect("write to String should not fail");
        }
        Some(diagnostics)
    }
}

fn diagnostic_identifier(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len() + 2);
    escaped.push('"');
    for byte in value.bytes() {
        match byte {
            b'a'..=b'z'
            | b'A'..=b'Z'
            | b'0'..=b'9'
            | b'-'
            | b'_'
            | b'.'
            | b':'
            | b'/'
            | b'@'
            | b'+' => escaped.push(char::from(byte)),
            _ => write!(&mut escaped, "\\x{byte:02x}").expect("write to String should not fail"),
        }
    }
    escaped.push('"');
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane_auth::ControlPlaneAuthOperation;
    use crate::NodeId;

    fn storage_credential(
        version: u64,
        secret: &[u8],
    ) -> ControlPlaneStorageNodeAuthCredentialInput {
        ControlPlaneStorageNodeAuthCredentialInput {
            node_id: NodeId::new(7),
            credential_id: "storage-node-7".to_owned(),
            credential_version: version,
            secret: secret.to_vec(),
        }
    }

    fn frontend_credential(version: u64, secret: &[u8]) -> ControlPlaneFrontendAuthCredentialInput {
        ControlPlaneFrontendAuthCredentialInput {
            instance_id: "frontend-1".to_owned(),
            credential_id: "frontend".to_owned(),
            credential_version: version,
            secret: secret.to_vec(),
        }
    }

    fn admin_credential(version: u64, secret: &[u8]) -> ControlPlaneAdminAuthCredentialInput {
        ControlPlaneAdminAuthCredentialInput {
            instance_id: "admin-1".to_owned(),
            credential_id: "admin".to_owned(),
            credential_version: version,
            secret: secret.to_vec(),
        }
    }

    #[test]
    fn server_auth_builds_all_roles_and_owns_redacted_diagnostics() {
        let auth = ControlPlaneRpcServerAuth::new(
            Some("control-auth"),
            Some("admin-1"),
            vec![
                storage_credential(3, b"storage-node-old-secret"),
                storage_credential(4, b"storage-node-new-secret"),
            ],
            vec![
                frontend_credential(5, b"frontend-old-secret"),
                frontend_credential(6, b"frontend-new-secret"),
            ],
            vec![
                admin_credential(7, b"admin-old-secret"),
                admin_credential(8, b"admin-new-secret"),
            ],
        )
        .unwrap();
        let verifier = auth.verifier.as_ref().unwrap();
        verifier
            .verify_storage_node_heartbeat_request_payload(b"not an auth envelope", 2_000)
            .expect_err("missing auth envelope should reject");

        let diagnostics = auth.diagnostics().unwrap();
        assert!(diagnostics.contains("required=true"), "{diagnostics}");
        assert!(
            diagnostics.contains("storage_node_heartbeat_required=true"),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains("frontend_runtime_map_required=true"),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains("admin_control_plane_required=true"),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains("cluster_id=\"control-auth\""),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains("storage_node_credentials=2"),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains("frontend_credentials=2"),
            "{diagnostics}"
        );
        assert!(diagnostics.contains("admin_credentials=2"), "{diagnostics}");
        assert!(diagnostics.contains("rejected_total=1"), "{diagnostics}");
        assert!(
            diagnostics.contains(&format!(
                "rejected_by_operation{{operation=\"{:?}\"}} 1",
                ControlPlaneAuthOperation::StorageRuntimeMapRefresh
            )),
            "{diagnostics}"
        );
        for secret in [
            "storage-node-old-secret",
            "storage-node-new-secret",
            "frontend-old-secret",
            "frontend-new-secret",
            "admin-old-secret",
            "admin-new-secret",
        ] {
            assert!(!diagnostics.contains(secret), "{diagnostics}");
        }
        let debug = format!("{auth:?}");
        assert_eq!(debug, "ControlPlaneRpcServerAuth { authenticated: true }");
    }

    #[test]
    fn server_auth_rejects_incomplete_and_crossed_configuration_without_network_use() {
        assert_eq!(
            ControlPlaneRpcServerAuth::new(
                None,
                Some("admin-1"),
                vec![storage_credential(1, b"storage-secret")],
                Vec::new(),
                vec![admin_credential(1, b"admin-secret")],
            )
            .unwrap_err(),
            ControlPlaneRpcServerAuthError::MissingClusterIdentity
        );
        assert_eq!(
            ControlPlaneRpcServerAuth::new(
                Some("control-auth"),
                None,
                vec![storage_credential(1, b"storage-secret")],
                Vec::new(),
                Vec::new(),
            )
            .unwrap_err(),
            ControlPlaneRpcServerAuthError::AdminCredentialsRequired
        );
        assert_eq!(
            ControlPlaneRpcServerAuth::new(
                Some("control-auth"),
                Some("admin-2"),
                Vec::new(),
                Vec::new(),
                vec![admin_credential(1, b"admin-secret")],
            )
            .unwrap_err(),
            ControlPlaneRpcServerAuthError::LocalAdminCredentialUnavailable {
                instance_id: "admin-2".to_owned(),
            }
        );
        assert_eq!(
            ControlPlaneRpcServerAuth::new(
                Some("control-auth"),
                Some("admin-2"),
                Vec::new(),
                Vec::new(),
                vec![admin_credential(1, b"")],
            )
            .unwrap_err(),
            ControlPlaneRpcServerAuthError::InvalidCredential,
            "invalid credentials must not be masked by local-identity selection"
        );
        let plain =
            ControlPlaneRpcServerAuth::new(None, None, Vec::new(), Vec::new(), Vec::new()).unwrap();
        assert!(!plain.is_authenticated());
        assert_eq!(plain.diagnostics(), None);
    }

    #[test]
    fn server_auth_validates_cluster_id_codec_boundary() {
        let maximum_cluster_id = "c".repeat(256);
        let maximum = ControlPlaneRpcServerAuth::new(
            Some(&maximum_cluster_id),
            Some("admin-1"),
            Vec::new(),
            Vec::new(),
            vec![admin_credential(1, b"admin-secret")],
        )
        .expect("256-byte cluster id should be representable by the auth envelope");
        assert!(maximum.is_authenticated());

        let overlong_cluster_id = "c".repeat(257);
        assert_eq!(
            ControlPlaneRpcServerAuth::new(
                Some(&overlong_cluster_id),
                Some("admin-1"),
                Vec::new(),
                Vec::new(),
                vec![admin_credential(1, b"admin-secret")],
            )
            .unwrap_err(),
            ControlPlaneRpcServerAuthError::InvalidClusterIdentity
        );
    }

    #[test]
    fn server_auth_diagnostics_escape_untrusted_identifiers() {
        assert_eq!(
            diagnostic_identifier("line\n\"\\ ={}é"),
            "\"line\\x0a\\x22\\x5c\\x20\\x3d\\x7b\\x7d\\xc3\\xa9\""
        );
        let frontend_instance = "frontend\n\"\\";
        let admin_instance = "admin\r\"\\";
        let auth = ControlPlaneRpcServerAuth::new(
            Some("cluster\n\"\\"),
            Some(admin_instance),
            Vec::new(),
            vec![ControlPlaneFrontendAuthCredentialInput {
                instance_id: frontend_instance.to_owned(),
                credential_id: "frontend\tcredential".to_owned(),
                credential_version: 1,
                secret: b"frontend-secret".to_vec(),
            }],
            vec![ControlPlaneAdminAuthCredentialInput {
                instance_id: admin_instance.to_owned(),
                credential_id: "admin credential".to_owned(),
                credential_version: 1,
                secret: b"admin-secret".to_vec(),
            }],
        )
        .unwrap();

        let diagnostics = auth.diagnostics().unwrap();
        assert_eq!(
            diagnostics.lines().count(),
            3,
            "identifier controls must not inject diagnostic records: {diagnostics}"
        );
        assert!(
            diagnostics.contains("cluster_id=\"cluster\\x0a\\x22\\x5c\""),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains("instance_id=\"frontend\\x0a\\x22\\x5c\""),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains("credential_id=\"frontend\\x09credential\""),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains("instance_id=\"admin\\x0d\\x22\\x5c\""),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains("credential_id=\"admin\\x20credential\""),
            "{diagnostics}"
        );
        assert!(!diagnostics.contains("frontend-secret"));
        assert!(!diagnostics.contains("admin-secret"));
    }
}
