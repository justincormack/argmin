use std::fmt;
use std::os::unix::ffi::OsStrExt as _;
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::control_plane::{
    ControlPlaneAdminAuthCredential, ControlPlaneAdminAuthCredentialInput,
    ControlPlaneRpcClientEndpoint, UnixControlPlaneClient,
};
use crate::control_plane_auth::ControlPlaneScopedCredential;

/// Opaque transport and authentication binding for operator control-plane
/// clients.
///
/// The process layer supplies resolved endpoints or logical Unix paths plus
/// unscoped credential configuration. Storage owns endpoint-set validation,
/// recovery-namespace derivation, credential selection, principal scoping, and
/// construction of operation-specific clients.
#[derive(Clone)]
pub struct ControlPlaneAdminClientBootstrap {
    pub(crate) client: UnixControlPlaneClient,
    pub(crate) credential: Option<ControlPlaneScopedCredential>,
}

/// Opaque, storage-validated admin credential selection.
///
/// Keeping this separate from transport construction lets a compound storage
/// operation bind its read and admin roles to one retained transport without
/// exposing the scoped credential to the process layer.
#[derive(Clone)]
pub struct ControlPlaneAdminCredentialBinding {
    pub(crate) credential: Option<ControlPlaneScopedCredential>,
}

impl fmt::Debug for ControlPlaneAdminCredentialBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlPlaneAdminCredentialBinding")
            .field("authenticated", &self.credential.is_some())
            .finish()
    }
}

impl fmt::Debug for ControlPlaneAdminClientBootstrap {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlPlaneAdminClientBootstrap")
            .field("authenticated", &self.credential.is_some())
            .field("transport", &"<opaque>")
            .finish()
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ControlPlaneAdminClientBootstrapError {
    #[error("control-plane admin client transport configuration is invalid")]
    InvalidTransport,
    #[error("configured admin control-plane auth requires a cluster identity")]
    MissingClusterIdentity,
    #[error("configured admin control-plane auth requires a local admin instance id")]
    MissingInstanceIdentity,
    #[error("configured admin credentials do not include the local admin instance")]
    LocalCredentialUnavailable,
    #[error("configured admin credential is invalid")]
    InvalidCredential,
}

impl ControlPlaneAdminClientBootstrap {
    pub fn with_endpoints(
        endpoints: impl IntoIterator<Item = ControlPlaneRpcClientEndpoint>,
        credential: ControlPlaneAdminCredentialBinding,
    ) -> Result<Self, ControlPlaneAdminClientBootstrapError> {
        let client = UnixControlPlaneClient::with_endpoints(endpoints)
            .map_err(|_| ControlPlaneAdminClientBootstrapError::InvalidTransport)?;
        Ok(Self {
            client,
            credential: credential.credential,
        })
    }

    pub fn with_socket_paths(
        socket_paths: impl IntoIterator<Item = PathBuf>,
        credential: ControlPlaneAdminCredentialBinding,
    ) -> Result<Self, ControlPlaneAdminClientBootstrapError> {
        let client = UnixControlPlaneClient::with_socket_paths(socket_paths)
            .map_err(|_| ControlPlaneAdminClientBootstrapError::InvalidTransport)?;
        Ok(Self {
            client,
            credential: credential.credential,
        })
    }

    pub fn with_derived_clock_recovery_socket_paths(
        ordinary_socket_paths: impl IntoIterator<Item = PathBuf>,
        credential: ControlPlaneAdminCredentialBinding,
    ) -> Result<Self, ControlPlaneAdminClientBootstrapError> {
        Self::with_socket_paths(
            ordinary_socket_paths
                .into_iter()
                .map(|path| control_plane_clock_recovery_socket_path(&path)),
            credential,
        )
    }

    #[must_use]
    pub fn is_authenticated(&self) -> bool {
        self.credential.is_some()
    }
}

impl ControlPlaneAdminCredentialBinding {
    pub fn new(
        cluster_id: Option<&str>,
        instance_id: Option<&str>,
        credentials: Vec<ControlPlaneAdminAuthCredentialInput>,
    ) -> Result<Self, ControlPlaneAdminClientBootstrapError> {
        if credentials.is_empty() {
            return Ok(Self { credential: None });
        }
        let cluster_id = cluster_id
            .filter(|cluster_id| !cluster_id.is_empty())
            .ok_or(ControlPlaneAdminClientBootstrapError::MissingClusterIdentity)?;
        let instance_id = instance_id
            .filter(|instance_id| !instance_id.is_empty())
            .ok_or(ControlPlaneAdminClientBootstrapError::MissingInstanceIdentity)?;
        let selected = credentials
            .into_iter()
            .filter(|credential| credential.instance_id == instance_id)
            .max_by(|left, right| {
                left.credential_version
                    .cmp(&right.credential_version)
                    .then_with(|| left.credential_id.cmp(&right.credential_id))
            })
            .ok_or(ControlPlaneAdminClientBootstrapError::LocalCredentialUnavailable)?;
        let credential = ControlPlaneAdminAuthCredential::new(selected)
            .map_err(|_| ControlPlaneAdminClientBootstrapError::InvalidCredential)?
            .scoped_for_cluster(cluster_id)
            .map_err(|_| ControlPlaneAdminClientBootstrapError::InvalidCredential)?;
        Ok(Self {
            credential: Some(credential),
        })
    }
}

/// Derives the storage-owned authority-clock recovery namespace for an
/// ordinary control-plane Unix socket.
#[must_use]
pub fn control_plane_clock_recovery_socket_path(control_plane_socket_path: &Path) -> PathBuf {
    const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut hash = FNV_OFFSET_BASIS;
    for byte in control_plane_socket_path.as_os_str().as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    control_plane_socket_path.with_file_name(format!(".c-{hash:016x}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn credential(
        instance_id: &str,
        credential_id: &str,
        credential_version: u64,
        secret: &[u8],
    ) -> ControlPlaneAdminAuthCredentialInput {
        ControlPlaneAdminAuthCredentialInput {
            instance_id: instance_id.to_owned(),
            credential_id: credential_id.to_owned(),
            credential_version,
            secret: secret.to_vec(),
        }
    }

    #[test]
    fn recovery_socket_derivation_has_stable_owner_local_fixtures() {
        assert_eq!(
            control_plane_clock_recovery_socket_path(std::path::Path::new(
                "/run/argmin/control-plane.sock"
            )),
            PathBuf::from("/run/argmin/.c-5718bf70c2bbb5b5")
        );
        assert_eq!(
            control_plane_clock_recovery_socket_path(std::path::Path::new(
                "/tmp/argmin-control-plane.sock"
            )),
            PathBuf::from("/tmp/.c-09cbb4c23c279b27")
        );
    }

    #[test]
    fn admin_bootstrap_selects_latest_local_credential_and_redacts_state() {
        let credential = ControlPlaneAdminCredentialBinding::new(
            Some("cluster-a"),
            Some("admin-a"),
            vec![
                credential("admin-a", "older", 1, b"older-secret-material"),
                credential("admin-b", "foreign", 9, b"foreign-secret-material"),
                credential("admin-a", "newer", 2, b"newer-secret-material"),
            ],
        )
        .unwrap();
        let credential_debug = format!("{credential:?}");
        assert_eq!(
            credential_debug,
            "ControlPlaneAdminCredentialBinding { authenticated: true }"
        );
        assert!(!credential_debug.contains("newer"));
        assert!(!credential_debug.contains("secret"));
        let bootstrap = ControlPlaneAdminClientBootstrap::with_socket_paths(
            [PathBuf::from("/tmp/control-plane.sock")],
            credential,
        )
        .unwrap();
        assert!(bootstrap.is_authenticated());
        assert_eq!(
            bootstrap.credential.as_ref().unwrap().credential_id(),
            "newer"
        );
        let debug = format!("{bootstrap:?}");
        assert!(!debug.contains("newer"));
        assert!(!debug.contains("secret"));
        assert!(!debug.contains("control-plane.sock"));
    }

    #[test]
    fn admin_bootstrap_rejects_missing_local_identity_before_network_use() {
        assert_eq!(
            ControlPlaneAdminCredentialBinding::new(
                Some("cluster-a"),
                Some("admin-a"),
                vec![credential("admin-b", "foreign", 1, b"secret-material")],
            )
            .unwrap_err(),
            ControlPlaneAdminClientBootstrapError::LocalCredentialUnavailable
        );
    }

    #[test]
    fn admin_bootstrap_rejects_incomplete_auth_and_transport_configuration() {
        assert_eq!(
            ControlPlaneAdminCredentialBinding::new(
                None,
                Some("admin-a"),
                vec![credential("admin-a", "key", 1, b"secret-material")],
            )
            .unwrap_err(),
            ControlPlaneAdminClientBootstrapError::MissingClusterIdentity
        );
        assert_eq!(
            ControlPlaneAdminCredentialBinding::new(
                Some("cluster-a"),
                None,
                vec![credential("admin-a", "key", 1, b"secret-material")],
            )
            .unwrap_err(),
            ControlPlaneAdminClientBootstrapError::MissingInstanceIdentity
        );
        assert_eq!(
            ControlPlaneAdminCredentialBinding::new(
                Some("cluster-a"),
                Some("admin-a"),
                vec![credential("admin-a", "key", 1, b"")],
            )
            .unwrap_err(),
            ControlPlaneAdminClientBootstrapError::InvalidCredential
        );

        let unauthenticated =
            ControlPlaneAdminCredentialBinding::new(None, None, Vec::new()).unwrap();
        assert_eq!(
            ControlPlaneAdminClientBootstrap::with_socket_paths([], unauthenticated).unwrap_err(),
            ControlPlaneAdminClientBootstrapError::InvalidTransport
        );
    }
}
