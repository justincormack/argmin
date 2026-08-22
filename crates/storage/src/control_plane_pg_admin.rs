// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::fmt;
use std::path::Path;

use crate::control_plane::ControlPlaneRuntimeMapSource as _;
use crate::control_plane::{
    AuthenticatedUnixControlPlaneClient, CanonicalStateDigest, ControlPlaneError,
    FileControlPlaneStore, MetadataCommandLogHash, PgMetadataProof, PgMetadataTransferProof,
    SingleAuthorityControlPlane, UnixControlPlaneClient,
};
use crate::control_plane_auth::ControlPlaneScopedCredential;
use crate::control_plane_client_bootstrap::ControlPlaneAdminClientBootstrap;
use crate::control_plane_service_client::ControlPlaneFrontendClient;
use crate::{ClusterEpoch, NodeId, PgId};

enum ControlPlanePgAdminDispatch {
    Plain(UnixControlPlaneClient),
    Authenticated(AuthenticatedUnixControlPlaneClient),
}

enum ControlPlanePgStatusDispatch {
    Plain(UnixControlPlaneClient),
    Authenticated(AuthenticatedUnixControlPlaneClient),
}

/// Opaque, authority-bound client for a PG-specific serving-readiness check.
pub struct ControlPlanePgStatusClient {
    dispatch: ControlPlanePgStatusDispatch,
}

impl ControlPlanePgStatusClient {
    pub(crate) fn new(
        client: UnixControlPlaneClient,
        credential: Option<ControlPlaneScopedCredential>,
    ) -> Self {
        let dispatch = match credential {
            Some(credential) => ControlPlanePgStatusDispatch::Authenticated(
                AuthenticatedUnixControlPlaneClient::new(client, credential),
            ),
            None => ControlPlanePgStatusDispatch::Plain(client),
        };
        Self { dispatch }
    }

    #[must_use]
    pub fn from_frontend_client(frontend: &ControlPlaneFrontendClient) -> Self {
        let (client, credential) = frontend.retained_transport_and_credential();
        Self::new(client, credential)
    }

    pub fn serving_epochs(
        &self,
        pg_id: u32,
        expected_acting_set: &[u32],
    ) -> Result<(u64, u64), ControlPlanePgAdminError> {
        let pg_id = PgId::new(pg_id);
        let expected_acting_set: Vec<_> = expected_acting_set
            .iter()
            .copied()
            .map(NodeId::new)
            .collect();
        let runtime_map =
            match &self.dispatch {
                ControlPlanePgStatusDispatch::Plain(client) => client
                    .serving_pg_runtime_map_snapshot(pg_id, crate::clock::current_time_millis()),
                ControlPlanePgStatusDispatch::Authenticated(client) => client
                    .serving_pg_runtime_map_snapshot(pg_id, crate::clock::current_time_millis()),
            }
            .map_err(|source| ControlPlanePgAdminError::operation("read serving status", source))?;
        let route = runtime_map
            .pg_routes()
            .iter()
            .find(|route| route.pg_id() == pg_id)
            .ok_or_else(|| {
                ControlPlanePgAdminError::semantic(
                    "read serving status",
                    ControlPlanePgAdminFailureReason::RequestedPgOmitted,
                )
            })?;
        if route.state() != crate::PgState::Active
            || route.primary_lease_deadline_ms().is_none()
            || route.acting_set() != expected_acting_set
        {
            return Err(ControlPlanePgAdminError::semantic(
                "read serving status",
                ControlPlanePgAdminFailureReason::NotServingOnExpectedActingSet,
            ));
        }
        Ok((
            runtime_map.cluster_epoch().get(),
            route.cluster_epoch().get(),
        ))
    }
}

impl fmt::Debug for ControlPlanePgStatusClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self { dispatch: _ } = self;
        formatter.write_str("ControlPlanePgStatusClient(<opaque>)")
    }
}

/// Opaque, authority-bound client for manual PG administration.
///
/// The process layer supplies only operator-visible integer identifiers and
/// explicitly version-qualified proof values. Storage owns PG identities,
/// proof decoding, route commands, authentication dispatch, retry
/// classification, and runtime-map response validation.
pub struct ControlPlanePgAdminClient {
    dispatch: ControlPlanePgAdminDispatch,
}

impl ControlPlanePgAdminClient {
    pub(crate) fn new(
        client: UnixControlPlaneClient,
        credential: Option<ControlPlaneScopedCredential>,
    ) -> Self {
        let dispatch = match credential {
            Some(credential) => ControlPlanePgAdminDispatch::Authenticated(
                AuthenticatedUnixControlPlaneClient::new(client, credential),
            ),
            None => ControlPlanePgAdminDispatch::Plain(client),
        };
        Self { dispatch }
    }

    #[must_use]
    pub fn from_bootstrap(bootstrap: &ControlPlaneAdminClientBootstrap) -> Self {
        Self::new(bootstrap.client.clone(), bootstrap.credential.clone())
    }

    pub fn set_acting_set(
        &self,
        pg_id: u32,
        acting_set: Vec<u32>,
    ) -> Result<u64, ControlPlanePgAdminError> {
        let pg_id = PgId::new(pg_id);
        let acting_set = acting_set.into_iter().map(NodeId::new).collect();
        let result = match &self.dispatch {
            ControlPlanePgAdminDispatch::Plain(client) => {
                client.set_pg_acting_set_checked(pg_id, acting_set)
            }
            ControlPlanePgAdminDispatch::Authenticated(client) => client.set_pg_acting_set_checked(
                pg_id,
                acting_set,
                crate::clock::current_time_millis(),
            ),
        };
        result
            .map(ClusterEpoch::get)
            .map_err(|source| ControlPlanePgAdminError::operation("set acting set", source))
    }

    pub fn fence_for_metadata_transfer(&self, pg_id: u32) -> Result<u64, ControlPlanePgAdminError> {
        let pg_id = PgId::new(pg_id);
        let result = match &self.dispatch {
            ControlPlanePgAdminDispatch::Plain(client) => {
                client.fence_pg_for_metadata_transfer_runtime_map_checked(pg_id)
            }
            ControlPlanePgAdminDispatch::Authenticated(client) => client
                .fence_pg_for_metadata_transfer_runtime_map_checked(
                    pg_id,
                    crate::clock::current_time_millis(),
                ),
        };
        result
            .map(|runtime_map| runtime_map.cluster_epoch().get())
            .map_err(|source| {
                ControlPlanePgAdminError::operation("fence for metadata transfer", source)
            })
    }

    pub fn install_metadata_transfer(
        &self,
        input: ControlPlanePgMetadataTransferInstall,
    ) -> Result<u64, ControlPlanePgAdminError> {
        let ControlPlanePgMetadataTransferInstall {
            pg_id,
            acting_set,
            transfer,
            expected_destination_epoch,
        } = input;
        let result = match &self.dispatch {
            ControlPlanePgAdminDispatch::Plain(client) => client
                .set_pg_acting_set_with_metadata_transfer_checked(
                    pg_id,
                    acting_set,
                    transfer,
                    expected_destination_epoch,
                ),
            ControlPlanePgAdminDispatch::Authenticated(client) => client
                .set_pg_acting_set_with_metadata_transfer_checked(
                    pg_id,
                    acting_set,
                    transfer,
                    expected_destination_epoch,
                    crate::clock::current_time_millis(),
                ),
        };
        result.map(ClusterEpoch::get).map_err(|source| {
            ControlPlanePgAdminError::operation("install metadata transfer", source)
        })
    }
}

impl fmt::Debug for ControlPlanePgAdminClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self { dispatch: _ } = self;
        formatter.write_str("ControlPlanePgAdminClient(<opaque>)")
    }
}

/// Logical operator input for installing a previously transferred PG state.
///
/// It deliberately exposes no route or proof representation after
/// construction.
pub struct ControlPlanePgMetadataTransferInstall {
    pg_id: PgId,
    acting_set: Vec<NodeId>,
    transfer: PgMetadataTransferProof,
    expected_destination_epoch: ClusterEpoch,
}

impl ControlPlanePgMetadataTransferInstall {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pg_id: u32,
        acting_set: Vec<u32>,
        source_epoch: u64,
        expected_destination_epoch: u64,
        source_applied_log_index: u64,
        source_applied_log_hash_version: u8,
        source_applied_log_hash: u64,
        source_state_digest_version: u8,
        source_state_digest: u64,
        imported_applied_log_index: u64,
        imported_applied_log_hash_version: u8,
        imported_applied_log_hash: u64,
        imported_state_digest_version: u8,
        imported_state_digest: u64,
    ) -> Result<Self, ControlPlanePgAdminInputError> {
        if acting_set.is_empty() {
            return Err(ControlPlanePgAdminInputError::new(
                "acting set must contain at least one node",
            ));
        }
        let source_epoch = ClusterEpoch::new(source_epoch)
            .ok_or_else(|| ControlPlanePgAdminInputError::new("source epoch must be nonzero"))?;
        let expected_destination_epoch =
            ClusterEpoch::new(expected_destination_epoch).ok_or_else(|| {
                ControlPlanePgAdminInputError::new("expected destination epoch must be nonzero")
            })?;
        let source_applied_log_hash = MetadataCommandLogHash::from_encoded_parts(
            source_applied_log_hash_version,
            source_applied_log_hash,
        )
        .map_err(|_| {
            ControlPlanePgAdminInputError::new(
                "source metadata-command log-hash version is unsupported",
            )
        })?;
        let source_state_digest = CanonicalStateDigest::from_encoded_parts(
            source_state_digest_version,
            source_state_digest,
        )
        .map_err(|_| {
            ControlPlanePgAdminInputError::new(
                "source canonical-state digest version is unsupported",
            )
        })?;
        let imported_applied_log_hash = MetadataCommandLogHash::from_encoded_parts(
            imported_applied_log_hash_version,
            imported_applied_log_hash,
        )
        .map_err(|_| {
            ControlPlanePgAdminInputError::new(
                "imported metadata-command log-hash version is unsupported",
            )
        })?;
        let imported_state_digest = CanonicalStateDigest::from_encoded_parts(
            imported_state_digest_version,
            imported_state_digest,
        )
        .map_err(|_| {
            ControlPlanePgAdminInputError::new(
                "imported canonical-state digest version is unsupported",
            )
        })?;
        Ok(Self {
            pg_id: PgId::new(pg_id),
            acting_set: acting_set.into_iter().map(NodeId::new).collect(),
            transfer: PgMetadataTransferProof::new_with_imported_metadata_proof(
                source_epoch,
                PgMetadataProof::from_carriers(
                    source_applied_log_index,
                    source_applied_log_hash,
                    source_state_digest,
                ),
                PgMetadataProof::from_carriers(
                    imported_applied_log_index,
                    imported_applied_log_hash,
                    imported_state_digest,
                ),
            ),
            expected_destination_epoch,
        })
    }
}

impl fmt::Debug for ControlPlanePgMetadataTransferInstall {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            pg_id: _,
            acting_set: _,
            transfer: _,
            expected_destination_epoch: _,
        } = self;
        formatter.write_str("ControlPlanePgMetadataTransferInstall(<opaque>)")
    }
}

pub struct ControlPlanePgAdminInputError(&'static str);

impl ControlPlanePgAdminInputError {
    fn new(reason: &'static str) -> Self {
        Self(reason)
    }
}

impl fmt::Debug for ControlPlanePgAdminInputError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self(_reason) = self;
        formatter.write_str("ControlPlanePgAdminInputError(<redacted>)")
    }
}

impl fmt::Display for ControlPlanePgAdminInputError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for ControlPlanePgAdminInputError {}

pub struct ControlPlanePgAdminError {
    operation: &'static str,
    failure: ControlPlanePgAdminFailure,
}

enum ControlPlanePgAdminFailure {
    Source(Box<ControlPlaneError>),
    UnconfirmedSource(Box<ControlPlaneError>),
    Semantic(ControlPlanePgAdminFailureReason),
}

enum ControlPlanePgAdminFailureReason {
    RequestedPgOmitted,
    NotServingOnExpectedActingSet,
}

impl ControlPlanePgAdminError {
    fn operation(operation: &'static str, source: ControlPlaneError) -> Self {
        let failure = if matches!(source, ControlPlaneError::RpcUnconfirmed { .. }) {
            ControlPlanePgAdminFailure::UnconfirmedSource(Box::new(source))
        } else {
            ControlPlanePgAdminFailure::Source(Box::new(source))
        };
        Self { operation, failure }
    }

    fn semantic(operation: &'static str, reason: ControlPlanePgAdminFailureReason) -> Self {
        Self {
            operation,
            failure: ControlPlanePgAdminFailure::Semantic(reason),
        }
    }
}

impl fmt::Debug for ControlPlanePgAdminError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            operation,
            failure: _,
        } = self;
        formatter
            .debug_struct("ControlPlanePgAdminError")
            .field("operation", operation)
            .field("diagnostic", &"<redacted>")
            .finish()
    }
}

impl fmt::Display for ControlPlanePgAdminError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "control-plane PG {} failed", self.operation)?;
        match &self.failure {
            ControlPlanePgAdminFailure::Source(_source) => Ok(()),
            ControlPlanePgAdminFailure::UnconfirmedSource(_source) => {
                formatter.write_str(": operation outcome could not be confirmed")
            }
            ControlPlanePgAdminFailure::Semantic(
                ControlPlanePgAdminFailureReason::RequestedPgOmitted,
            ) => formatter.write_str(": requested PG was omitted from the scoped response"),
            ControlPlanePgAdminFailure::Semantic(
                ControlPlanePgAdminFailureReason::NotServingOnExpectedActingSet,
            ) => formatter.write_str(": PG is not serving on the expected acting set"),
        }
    }
}

impl std::error::Error for ControlPlanePgAdminError {}

pub fn set_offline_control_plane_pg_acting_set(
    state_path: &Path,
    pg_id: u32,
    acting_set: Vec<u32>,
) -> Result<u64, ControlPlanePgAdminError> {
    let store = FileControlPlaneStore::new(state_path);
    let mut authority = SingleAuthorityControlPlane::open(store)
        .map_err(|source| ControlPlanePgAdminError::operation("open offline state", source))?;
    authority
        .set_pg_acting_set(
            PgId::new(pg_id),
            acting_set.into_iter().map(NodeId::new).collect(),
        )
        .map(|snapshot| snapshot.cluster_epoch().get())
        .map_err(|source| ControlPlanePgAdminError::operation("set offline acting set", source))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_transfer_install_rejects_invalid_logical_inputs_and_redacts_debug() {
        let empty = ControlPlanePgMetadataTransferInstall::new(
            7,
            Vec::new(),
            1,
            2,
            3,
            1,
            4,
            CanonicalStateDigest::CURRENT_ENCODING_VERSION,
            5,
            6,
            1,
            7,
            CanonicalStateDigest::CURRENT_ENCODING_VERSION,
            8,
        )
        .unwrap_err();
        assert_eq!(
            empty.to_string(),
            "acting set must contain at least one node"
        );
        assert_eq!(
            format!("{empty:?}"),
            "ControlPlanePgAdminInputError(<redacted>)"
        );

        for (source_epoch, destination_epoch, expected) in [
            (0, 2, "source epoch must be nonzero"),
            (1, 0, "expected destination epoch must be nonzero"),
        ] {
            let error = ControlPlanePgMetadataTransferInstall::new(
                7,
                vec![1],
                source_epoch,
                destination_epoch,
                3,
                1,
                4,
                CanonicalStateDigest::CURRENT_ENCODING_VERSION,
                5,
                6,
                1,
                7,
                CanonicalStateDigest::CURRENT_ENCODING_VERSION,
                8,
            )
            .unwrap_err();
            assert_eq!(error.to_string(), expected);
        }

        for (
            source_hash_version,
            source_digest_version,
            imported_hash_version,
            imported_digest_version,
            expected,
        ) in [
            (
                2,
                5,
                1,
                5,
                "source metadata-command log-hash version is unsupported",
            ),
            (
                1,
                6,
                1,
                5,
                "source canonical-state digest version is unsupported",
            ),
            (
                1,
                5,
                2,
                5,
                "imported metadata-command log-hash version is unsupported",
            ),
            (
                1,
                5,
                1,
                6,
                "imported canonical-state digest version is unsupported",
            ),
        ] {
            let error = ControlPlanePgMetadataTransferInstall::new(
                7,
                vec![1],
                1,
                2,
                3,
                source_hash_version,
                4,
                source_digest_version,
                5,
                6,
                imported_hash_version,
                7,
                imported_digest_version,
                8,
            )
            .unwrap_err();
            assert_eq!(error.to_string(), expected);
        }

        let input = ControlPlanePgMetadataTransferInstall::new(
            7,
            vec![1, 2],
            12,
            13,
            20,
            1,
            30,
            CanonicalStateDigest::CURRENT_ENCODING_VERSION,
            40,
            20,
            1,
            31,
            CanonicalStateDigest::CURRENT_ENCODING_VERSION,
            40,
        )
        .unwrap();
        assert_eq!(input.pg_id, PgId::new(7));
        assert_eq!(input.acting_set, vec![NodeId::new(1), NodeId::new(2)]);
        assert_eq!(input.expected_destination_epoch.get(), 13);
        assert_eq!(input.transfer.source_epoch().get(), 12);
        assert_eq!(
            input.transfer.source_metadata_proof(),
            PgMetadataProof::current(20, 30, 40)
        );
        assert_eq!(
            input.transfer.metadata_proof(),
            PgMetadataProof::current(20, 31, 40)
        );
        assert_eq!(
            format!("{input:?}"),
            "ControlPlanePgMetadataTransferInstall(<opaque>)"
        );
    }

    #[test]
    fn admin_error_retains_source_without_exposing_it_through_public_error_views() {
        const SENSITIVE_DIAGNOSTIC: &str =
            "route epoch 41 proof 012345 endpoint /secret/control-plane.sock";
        let error = ControlPlanePgAdminError::operation(
            "set acting set",
            ControlPlaneError::RpcUnconfirmed {
                message: SENSITIVE_DIAGNOSTIC.to_owned(),
            },
        );

        let ControlPlanePgAdminFailure::UnconfirmedSource(source) = &error.failure else {
            panic!("operation failure should retain its source inside storage");
        };
        assert!(matches!(
            source.as_ref(),
            ControlPlaneError::RpcUnconfirmed { message }
                if message == SENSITIVE_DIAGNOSTIC
        ));

        let display = error.to_string();
        assert_eq!(
            display,
            "control-plane PG set acting set failed: operation outcome could not be confirmed"
        );
        assert!(!display.contains(SENSITIVE_DIAGNOSTIC));

        let debug = format!("{error:?}");
        assert_eq!(
            debug,
            "ControlPlanePgAdminError { operation: \"set acting set\", diagnostic: \"<redacted>\" }"
        );
        assert!(!debug.contains(SENSITIVE_DIAGNOSTIC));
        assert!(std::error::Error::source(&error).is_none());
    }

    #[test]
    fn admin_error_does_not_misclassify_other_private_sources_as_unconfirmed() {
        let error = ControlPlanePgAdminError::operation(
            "set acting set",
            ControlPlaneError::UnknownPg { pg_id: 41 },
        );

        let ControlPlanePgAdminFailure::Source(source) = &error.failure else {
            panic!("operation failure should retain its source inside storage");
        };
        assert!(matches!(
            source.as_ref(),
            ControlPlaneError::UnknownPg { pg_id: 41 }
        ));
        assert_eq!(error.to_string(), "control-plane PG set acting set failed");
    }

    #[test]
    fn offline_pg_admin_accepts_only_logical_identifiers() {
        let temp = test_util::tempdir();
        let state_path = temp.path().join("control-plane.state");
        let mut authority =
            SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&state_path)).unwrap();
        for node_id in [1, 2] {
            authority
                .set_node_membership(
                    NodeId::new(node_id),
                    crate::control_plane::NodeMembershipState::Active,
                )
                .unwrap();
        }
        drop(authority);

        let epoch = set_offline_control_plane_pg_acting_set(&state_path, 7, vec![1, 2]).unwrap();
        let authority =
            SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&state_path)).unwrap();
        let snapshot = authority.snapshot();
        assert!(snapshot.cluster_epoch().get() >= epoch);
        assert_eq!(
            snapshot.pg(PgId::new(7)).unwrap().acting_set(),
            &[NodeId::new(1), NodeId::new(2)]
        );
    }
}
