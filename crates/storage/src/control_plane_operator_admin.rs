// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::fmt;

use crate::control_plane::{
    AuthenticatedUnixControlPlaneClient, ControlPlaneAuthorityClockBlockedReason,
    ControlPlaneAuthorityClockStatus, ControlPlaneError, UnixControlPlaneClient,
};
use crate::control_plane_auth::{ControlPlaneAuthPrincipal, ControlPlaneScopedCredential};
use crate::control_plane_client_bootstrap::ControlPlaneAdminClientBootstrap;
#[cfg(test)]
use crate::control_plane_client_bootstrap::ControlPlaneAdminCredentialBinding;

enum ControlPlaneRaftAdminDispatch {
    Plain(UnixControlPlaneClient),
    Authenticated(AuthenticatedUnixControlPlaneClient),
}

/// Opaque, authority-bound client for manual Raft administration.
///
/// The process layer supplies only an operator-visible node identifier.
/// Storage owns authenticated dispatch, operation-specific RPC behavior, and
/// retained transport diagnostics.
pub struct ControlPlaneRaftAdminClient {
    dispatch: ControlPlaneRaftAdminDispatch,
}

impl ControlPlaneRaftAdminClient {
    pub(crate) fn new(
        client: UnixControlPlaneClient,
        credential: Option<ControlPlaneScopedCredential>,
    ) -> Self {
        let dispatch = match credential {
            Some(credential) => ControlPlaneRaftAdminDispatch::Authenticated(
                AuthenticatedUnixControlPlaneClient::new(client, credential),
            ),
            None => ControlPlaneRaftAdminDispatch::Plain(client),
        };
        Self { dispatch }
    }

    #[must_use]
    pub fn from_bootstrap(bootstrap: &ControlPlaneAdminClientBootstrap) -> Self {
        Self::new(bootstrap.client.clone(), bootstrap.credential.clone())
    }

    pub fn transfer_leadership_to(
        &self,
        node_id: u64,
    ) -> Result<(), ControlPlaneOperatorAdminError> {
        let result = match &self.dispatch {
            ControlPlaneRaftAdminDispatch::Plain(client) => {
                client.transfer_raft_leadership_to(node_id)
            }
            ControlPlaneRaftAdminDispatch::Authenticated(client) => {
                client.transfer_raft_leadership_to(node_id, crate::clock::current_time_millis())
            }
        };
        result.map_err(|source| {
            ControlPlaneOperatorAdminError::mutating_operation("Raft leadership transfer", source)
        })
    }

    pub fn trigger_snapshot_and_purge(
        &self,
    ) -> Result<Option<u64>, ControlPlaneOperatorAdminError> {
        let result = match &self.dispatch {
            ControlPlaneRaftAdminDispatch::Plain(client) => {
                client.trigger_raft_snapshot_and_purge()
            }
            ControlPlaneRaftAdminDispatch::Authenticated(client) => {
                client.trigger_raft_snapshot_and_purge(crate::clock::current_time_millis())
            }
        };
        result.map_err(|source| {
            ControlPlaneOperatorAdminError::mutating_operation(
                "Raft snapshot/purge trigger",
                source,
            )
        })
    }

    pub fn trigger_election(&self) -> Result<(), ControlPlaneOperatorAdminError> {
        let result = match &self.dispatch {
            ControlPlaneRaftAdminDispatch::Plain(client) => client.trigger_raft_election(),
            ControlPlaneRaftAdminDispatch::Authenticated(client) => {
                client.trigger_raft_election(crate::clock::current_time_millis())
            }
        };
        result.map_err(|source| {
            ControlPlaneOperatorAdminError::mutating_operation("Raft election trigger", source)
        })
    }
}

impl fmt::Debug for ControlPlaneRaftAdminClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self { dispatch: _ } = self;
        formatter.write_str("ControlPlaneRaftAdminClient(<opaque>)")
    }
}

/// Opaque, authenticated client for authority-clock recovery administration.
///
/// Authority-clock recovery is deliberately unavailable without an admin
/// credential. The capability exposes only storage-rendered operator status,
/// not the underlying clock or Raft state representations.
pub struct ControlPlaneAuthorityClockAdminClient {
    client: AuthenticatedUnixControlPlaneClient,
}

impl ControlPlaneAuthorityClockAdminClient {
    pub(crate) fn new(
        client: UnixControlPlaneClient,
        credential: Option<ControlPlaneScopedCredential>,
    ) -> Result<Self, ControlPlaneOperatorAdminError> {
        let credential = credential.ok_or_else(|| {
            ControlPlaneOperatorAdminError::admin_credential_required(
                "authority-clock administration",
            )
        })?;
        if !matches!(
            credential.principal(),
            ControlPlaneAuthPrincipal::Admin { .. }
        ) {
            return Err(ControlPlaneOperatorAdminError::admin_credential_required(
                "authority-clock administration",
            ));
        }
        Ok(Self {
            client: AuthenticatedUnixControlPlaneClient::new(client, credential),
        })
    }

    pub fn from_bootstrap(
        bootstrap: &ControlPlaneAdminClientBootstrap,
    ) -> Result<Self, ControlPlaneOperatorAdminError> {
        Self::new(bootstrap.client.clone(), bootstrap.credential.clone())
    }

    pub fn status(
        &self,
    ) -> Result<ControlPlaneAuthorityClockAdminStatus, ControlPlaneOperatorAdminError> {
        self.client
            .authority_clock_status(crate::clock::current_time_millis())
            .map(ControlPlaneAuthorityClockAdminStatus)
            .map_err(|source| {
                ControlPlaneOperatorAdminError::read_only_operation(
                    "authority-clock status",
                    source,
                )
            })
    }

    pub fn reestablish(
        &self,
    ) -> Result<ControlPlaneAuthorityClockAdminStatus, ControlPlaneOperatorAdminError> {
        self.client
            .reestablish_authority_clock(crate::clock::current_time_millis())
            .map(ControlPlaneAuthorityClockAdminStatus)
            .map_err(|source| {
                ControlPlaneOperatorAdminError::mutating_operation(
                    "authority-clock re-establishment",
                    source,
                )
            })
    }
}

impl fmt::Debug for ControlPlaneAuthorityClockAdminClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self { client: _ } = self;
        formatter.write_str("ControlPlaneAuthorityClockAdminClient(<opaque>)")
    }
}

/// Opaque operator rendering of authority-clock recovery state.
pub struct ControlPlaneAuthorityClockAdminStatus(ControlPlaneAuthorityClockStatus);

impl fmt::Debug for ControlPlaneAuthorityClockAdminStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self(_status) = self;
        formatter.write_str("ControlPlaneAuthorityClockAdminStatus(<opaque>)")
    }
}

impl fmt::Display for ControlPlaneAuthorityClockAdminStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let status = self.0;
        write!(
            formatter,
            "generation={} established={} blocked_reason=",
            status.generation(),
            status.established(),
        )?;
        match status.blocked_reason() {
            None => formatter.write_str("None")?,
            Some(reason) => write!(
                formatter,
                "Some({})",
                authority_clock_blocked_reason_name(reason)
            )?,
        }
        write!(
            formatter,
            " committed_timestamp_high_water_ms={} bound_raft_term={} current_raft_term={} local_raft_authority_leader={} local_raft_authority_serving={}",
            display_optional_u64(status.committed_timestamp_high_water_ms()),
            display_optional_u64(status.bound_raft_leadership_term()),
            display_optional_u64(status.current_raft_leadership_term()),
            status.local_raft_authority_leader(),
            status.local_raft_authority_serving(),
        )
    }
}

fn display_optional_u64(value: Option<u64>) -> String {
    value.map_or_else(|| "-".to_owned(), |value| value.to_string())
}

fn authority_clock_blocked_reason_name(
    reason: ControlPlaneAuthorityClockBlockedReason,
) -> &'static str {
    match reason {
        ControlPlaneAuthorityClockBlockedReason::InitialTimestampDiscontinuity => {
            "InitialTimestampDiscontinuity"
        }
        ControlPlaneAuthorityClockBlockedReason::RaftLeadershipChanged => "RaftLeadershipChanged",
        ControlPlaneAuthorityClockBlockedReason::ClockSourceUnavailable => "ClockSourceUnavailable",
        ControlPlaneAuthorityClockBlockedReason::ClockHealthRegression => "ClockHealthRegression",
        ControlPlaneAuthorityClockBlockedReason::WallClockRegression => "WallClockRegression",
        ControlPlaneAuthorityClockBlockedReason::WallClockForwardJump => "WallClockForwardJump",
        ControlPlaneAuthorityClockBlockedReason::CheckpointPersistenceFailure => {
            "CheckpointPersistenceFailure"
        }
    }
}

pub struct ControlPlaneOperatorAdminError {
    operation: &'static str,
    failure: ControlPlaneOperatorAdminFailure,
}

enum ControlPlaneOperatorAdminFailure {
    Definite(Box<ControlPlaneError>),
    MutatingOutcomeUnconfirmed(Box<ControlPlaneError>),
    AdminCredentialRequired,
}

impl ControlPlaneOperatorAdminError {
    fn read_only_operation(operation: &'static str, source: ControlPlaneError) -> Self {
        Self {
            operation,
            failure: ControlPlaneOperatorAdminFailure::Definite(Box::new(source)),
        }
    }

    fn mutating_operation(operation: &'static str, source: ControlPlaneError) -> Self {
        let failure = if matches!(source, ControlPlaneError::RpcUnconfirmed { .. }) {
            ControlPlaneOperatorAdminFailure::MutatingOutcomeUnconfirmed(Box::new(source))
        } else {
            ControlPlaneOperatorAdminFailure::Definite(Box::new(source))
        };
        Self { operation, failure }
    }

    fn admin_credential_required(operation: &'static str) -> Self {
        Self {
            operation,
            failure: ControlPlaneOperatorAdminFailure::AdminCredentialRequired,
        }
    }
}

impl fmt::Debug for ControlPlaneOperatorAdminError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            operation,
            failure: _,
        } = self;
        formatter
            .debug_struct("ControlPlaneOperatorAdminError")
            .field("operation", operation)
            .field("diagnostic", &"<redacted>")
            .finish()
    }
}

impl fmt::Display for ControlPlaneOperatorAdminError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "control-plane {}", self.operation)?;
        match &self.failure {
            ControlPlaneOperatorAdminFailure::Definite(_source) => formatter.write_str(" failed"),
            ControlPlaneOperatorAdminFailure::MutatingOutcomeUnconfirmed(_source) => formatter
                .write_str(
                    " may have applied but could not be confirmed; do not retry without an operation-specific confirmation check",
                ),
            ControlPlaneOperatorAdminFailure::AdminCredentialRequired => {
                formatter.write_str(" requires authenticated admin credentials")
            }
        }
    }
}

impl std::error::Error for ControlPlaneOperatorAdminError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixListener;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use crate::control_plane::{
        ClusterControlSnapshot, ClusterRuntimeMapSnapshot, ControlPlaneAdmin,
        ControlPlaneAdminAuthCredential, ControlPlaneAdminAuthCredentialInput,
        ControlPlaneAuthorityClock, ControlPlaneAuthorityClockCheckpointBinding,
        ControlPlaneAuthorityClockCheckpointTarget, ControlPlaneAuthorityClockContext,
        ControlPlaneHeartbeatRefresh, ControlPlaneHeartbeatRuntimeMapSource,
        ControlPlaneRpcServerListener, ControlPlaneRpcServerPolicy, ControlPlaneRpcServerRole,
        ControlPlaneRuntimeMapSource, ControlPlaneUnixAuthVerifier,
        FencedPgMetadataTransferSnapshot, NodeHeartbeat, PgMetadataTransferProof,
    };
    use crate::control_plane_auth::ControlPlaneScopedCredentialInput;
    use crate::{ClusterEpoch, NodeId, PgId};

    #[derive(Default)]
    struct RaftAdminTestAuthority {
        leadership_transfers: Vec<u64>,
        snapshot_triggers: u64,
        election_triggers: u64,
    }

    impl ControlPlaneAdmin for RaftAdminTestAuthority {
        fn set_pg_acting_set(
            &mut self,
            _pg_id: PgId,
            _acting_set: Vec<NodeId>,
        ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
            unreachable!("Raft operator test must not dispatch PG administration")
        }

        fn fence_pg_for_metadata_transfer(
            &mut self,
            _pg_id: PgId,
        ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
            unreachable!("Raft operator test must not dispatch PG administration")
        }

        fn fence_pg_for_metadata_transfer_with_source_lease(
            &mut self,
            _pg_id: PgId,
        ) -> Result<FencedPgMetadataTransferSnapshot, ControlPlaneError> {
            unreachable!("Raft operator test must not dispatch PG administration")
        }

        fn set_pg_acting_set_with_metadata_transfer(
            &mut self,
            _pg_id: PgId,
            _acting_set: Vec<NodeId>,
            _transfer: PgMetadataTransferProof,
            _expected_destination_epoch: ClusterEpoch,
        ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
            unreachable!("Raft operator test must not dispatch PG administration")
        }

        fn transfer_raft_leadership_to(&mut self, node_id: u64) -> Result<(), ControlPlaneError> {
            self.leadership_transfers.push(node_id);
            Ok(())
        }

        fn trigger_raft_snapshot_and_purge(&mut self) -> Result<Option<u64>, ControlPlaneError> {
            self.snapshot_triggers += 1;
            Ok(Some(47))
        }

        fn trigger_raft_election(&mut self) -> Result<(), ControlPlaneError> {
            self.election_triggers += 1;
            Ok(())
        }
    }

    impl ControlPlaneHeartbeatRuntimeMapSource for RaftAdminTestAuthority {
        fn refresh_node_heartbeat(
            &mut self,
            _heartbeat: NodeHeartbeat,
            _authority_now_ms: u64,
        ) -> Result<ControlPlaneHeartbeatRefresh, ControlPlaneError> {
            unreachable!("Raft operator test must not dispatch heartbeat work")
        }
    }

    impl ControlPlaneRuntimeMapSource for RaftAdminTestAuthority {
        fn runtime_map_snapshot(
            &self,
            _authority_now_ms: u64,
        ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
            unreachable!("Raft operator test must not dispatch runtime-map work")
        }

        fn serving_pg_runtime_map_snapshot(
            &self,
            _pg_id: PgId,
            _authority_now_ms: u64,
        ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
            unreachable!("Raft operator test must not dispatch runtime-map work")
        }
    }

    fn admin_auth(
        cluster_id: &str,
    ) -> (
        ControlPlaneScopedCredential,
        Arc<ControlPlaneUnixAuthVerifier>,
    ) {
        let configured =
            ControlPlaneAdminAuthCredential::new(ControlPlaneAdminAuthCredentialInput {
                instance_id: "operator-1".to_owned(),
                credential_id: "operator-key".to_owned(),
                credential_version: 1,
                secret: b"operator-secret".to_vec(),
            })
            .unwrap();
        let credential = configured.scoped_for_cluster(cluster_id).unwrap();
        let verifier = Arc::new(
            ControlPlaneUnixAuthVerifier::new_empty(cluster_id)
                .unwrap()
                .with_admin_credentials(vec![configured])
                .unwrap(),
        );
        (credential, verifier)
    }

    fn admin_credential_binding(cluster_id: Option<&str>) -> ControlPlaneAdminCredentialBinding {
        let (instance_id, credentials) = match cluster_id {
            Some(_) => (
                Some("operator-1"),
                vec![ControlPlaneAdminAuthCredentialInput {
                    instance_id: "operator-1".to_owned(),
                    credential_id: "operator-key".to_owned(),
                    credential_version: 1,
                    secret: b"operator-secret".to_vec(),
                }],
            ),
            None => (None, Vec::new()),
        };
        ControlPlaneAdminCredentialBinding::new(cluster_id, instance_id, credentials).unwrap()
    }

    fn exercise_raft_admin_dispatch(authenticated: bool) {
        let directory = test_util::tempdir();
        let socket_path = directory.path().join("control-plane.sock");
        let listener = ControlPlaneRpcServerListener::unix(
            UnixListener::bind(&socket_path).unwrap(),
            1,
            1024 * 1024,
            Duration::from_secs(1),
        )
        .unwrap();
        let mut policy =
            ControlPlaneRpcServerPolicy::new(ControlPlaneRpcServerRole::Ordinary, 1, 1024 * 1024)
                .unwrap();
        if authenticated {
            let (_credential, verifier) = admin_auth("operator-cluster");
            policy = policy.with_auth_verifier(verifier);
        }
        let authority = Arc::new(Mutex::new(RaftAdminTestAuthority::default()));
        let server_authority = Arc::clone(&authority);
        let now_ms = crate::clock::current_time_millis();
        let server = std::thread::spawn(move || {
            listener
                .serve_shared_requests_for_test(
                    server_authority,
                    policy,
                    [now_ms, now_ms, now_ms],
                    |_| {},
                )
                .unwrap();
        });

        let bootstrap = ControlPlaneAdminClientBootstrap::with_socket_paths(
            [socket_path],
            admin_credential_binding(authenticated.then_some("operator-cluster")),
        )
        .unwrap();
        let client = ControlPlaneRaftAdminClient::from_bootstrap(&bootstrap);
        client.transfer_leadership_to(91).unwrap();
        assert_eq!(client.trigger_snapshot_and_purge().unwrap(), Some(47));
        client.trigger_election().unwrap();
        server.join().unwrap();

        let authority = authority.lock().unwrap();
        assert_eq!(authority.leadership_transfers, vec![91]);
        assert_eq!(authority.snapshot_triggers, 1);
        assert_eq!(authority.election_triggers, 1);
    }

    #[test]
    fn raft_admin_dispatches_every_operation_plain_and_authenticated() {
        exercise_raft_admin_dispatch(false);
        exercise_raft_admin_dispatch(true);
    }

    #[test]
    fn raft_admin_failure_before_request_publication_is_definite() {
        let directory = test_util::tempdir();
        let socket_path = directory.path().join("missing-control-plane.sock");
        let (credential, _verifier) = admin_auth("operator-cluster");
        let client = ControlPlaneRaftAdminClient::new(
            UnixControlPlaneClient::new(socket_path),
            Some(credential),
        );

        let error = client.transfer_leadership_to(91).unwrap_err();
        assert!(matches!(
            &error.failure,
            ControlPlaneOperatorAdminFailure::Definite(_)
        ));
        assert_eq!(
            error.to_string(),
            "control-plane Raft leadership transfer failed"
        );
        assert!(std::error::Error::source(&error).is_none());
    }

    #[test]
    fn authority_clock_capability_dispatches_authenticated_status() {
        let directory = test_util::tempdir();
        let state_path = directory.path().join("control-plane.state");
        let socket_path = directory.path().join("clock-recovery.sock");
        let listener = ControlPlaneRpcServerListener::unix(
            UnixListener::bind(&socket_path).unwrap(),
            1,
            1024 * 1024,
            Duration::from_secs(1),
        )
        .unwrap();
        let (credential, verifier) = admin_auth("clock-cluster");
        let now_ms = crate::clock::current_time_millis();
        let authority_clock = Arc::new(Mutex::new(
            ControlPlaneAuthorityClock::new(None, now_ms, crate::clock::clock_health_time_millis())
                .unwrap(),
        ));
        let checkpoint_target = Arc::new(ControlPlaneAuthorityClockCheckpointTarget::new(
            &state_path,
            ControlPlaneAuthorityClockCheckpointBinding::for_raft("clock-cluster", 1),
        ));
        let policy = ControlPlaneRpcServerPolicy::new(
            ControlPlaneRpcServerRole::AuthorityClockRecovery,
            1,
            1024 * 1024,
        )
        .unwrap()
        .with_auth_verifier(verifier)
        .with_authority_clock(authority_clock, checkpoint_target, false);
        let authority = Arc::new(Mutex::new(
            crate::control_plane::SingleAuthorityControlPlane::open(
                crate::control_plane::FileControlPlaneStore::new(&state_path),
            )
            .unwrap(),
        ));
        let server = std::thread::spawn(move || {
            listener
                .serve_shared_requests_for_test(authority, policy, [now_ms], |_| {})
                .unwrap();
        });

        let client = ControlPlaneAuthorityClockAdminClient::new(
            UnixControlPlaneClient::new(&socket_path),
            Some(credential),
        )
        .unwrap();
        let status = client.status().unwrap();
        assert!(
            status.to_string().contains("established=true"),
            "unexpected authority-clock operator status: {status}"
        );
        server.join().unwrap();
    }

    #[test]
    fn authority_clock_status_has_exact_operator_rendering_and_opaque_debug() {
        let clock = ControlPlaneAuthorityClock::new(None, 1_000, Some(500)).unwrap();
        let status = ControlPlaneAuthorityClockAdminStatus(clock.status(
            ControlPlaneAuthorityClockContext::new(Some(900), Some(7), true, true),
        ));

        assert_eq!(
            status.to_string(),
            "generation=1 established=true blocked_reason=None committed_timestamp_high_water_ms=900 bound_raft_term=- current_raft_term=7 local_raft_authority_leader=true local_raft_authority_serving=true"
        );
        assert_eq!(
            format!("{status:?}"),
            "ControlPlaneAuthorityClockAdminStatus(<opaque>)"
        );

        let blocked_clock =
            ControlPlaneAuthorityClock::new(Some(50_000), 1_000, Some(500)).unwrap();
        let blocked = ControlPlaneAuthorityClockAdminStatus(blocked_clock.status(
            ControlPlaneAuthorityClockContext::new(Some(50_000), None, false, false),
        ));
        assert_eq!(
            blocked.to_string(),
            "generation=1 established=false blocked_reason=Some(InitialTimestampDiscontinuity) committed_timestamp_high_water_ms=50000 bound_raft_term=- current_raft_term=- local_raft_authority_leader=false local_raft_authority_serving=false"
        );
    }

    #[test]
    fn authority_clock_capability_requires_admin_principal_without_network_access() {
        let client = UnixControlPlaneClient::new("/not/contacted/control-plane.sock");
        let error = ControlPlaneAuthorityClockAdminClient::new(client, None).unwrap_err();
        assert_eq!(
            error.to_string(),
            "control-plane authority-clock administration requires authenticated admin credentials"
        );
        assert_eq!(
            format!("{error:?}"),
            "ControlPlaneOperatorAdminError { operation: \"authority-clock administration\", diagnostic: \"<redacted>\" }"
        );

        let frontend = ControlPlaneScopedCredential::new(ControlPlaneScopedCredentialInput {
            cluster_id: "operator-cluster".to_owned(),
            credential_id: "frontend-key".to_owned(),
            credential_version: 1,
            principal: ControlPlaneAuthPrincipal::Frontend {
                instance_id: "frontend-1".to_owned(),
            },
            secret: b"frontend-secret".to_vec(),
        })
        .unwrap();
        let client = UnixControlPlaneClient::new("/also-not-contacted/control-plane.sock");
        let error = ControlPlaneAuthorityClockAdminClient::new(client, Some(frontend)).unwrap_err();
        assert_eq!(
            error.to_string(),
            "control-plane authority-clock administration requires authenticated admin credentials"
        );
        assert_eq!(
            format!("{error:?}"),
            "ControlPlaneOperatorAdminError { operation: \"authority-clock administration\", diagnostic: \"<redacted>\" }"
        );
    }

    #[test]
    fn operator_error_preserves_unconfirmed_mutation_without_exposing_its_source() {
        const SENSITIVE_DIAGNOSTIC: &str =
            "route epoch 41 proof 012345 endpoint /secret/control-plane.sock";
        let error = ControlPlaneOperatorAdminError::mutating_operation(
            "Raft leadership transfer",
            ControlPlaneError::RpcUnconfirmed {
                message: SENSITIVE_DIAGNOSTIC.to_owned(),
            },
        );

        let ControlPlaneOperatorAdminFailure::MutatingOutcomeUnconfirmed(source) = &error.failure
        else {
            panic!("unconfirmed mutation should retain its source inside storage");
        };
        assert!(matches!(
            source.as_ref(),
            ControlPlaneError::RpcUnconfirmed { message }
                if message == SENSITIVE_DIAGNOSTIC
        ));

        let display = error.to_string();
        assert_eq!(
            display,
            "control-plane Raft leadership transfer may have applied but could not be confirmed; do not retry without an operation-specific confirmation check"
        );
        assert!(!display.contains(SENSITIVE_DIAGNOSTIC));
        let debug = format!("{error:?}");
        assert_eq!(
            debug,
            "ControlPlaneOperatorAdminError { operation: \"Raft leadership transfer\", diagnostic: \"<redacted>\" }"
        );
        assert!(!debug.contains(SENSITIVE_DIAGNOSTIC));
        assert!(std::error::Error::source(&error).is_none());
    }

    #[test]
    fn operator_error_distinguishes_definite_failures_and_read_only_operations() {
        const SENSITIVE_DIAGNOSTIC: &str =
            "route epoch 42 proof 67890 endpoint /another/secret/control-plane.sock";
        let definite = ControlPlaneOperatorAdminError::mutating_operation(
            "Raft election trigger",
            ControlPlaneError::rpc_remote(SENSITIVE_DIAGNOSTIC.to_owned()),
        );
        assert!(matches!(
            &definite.failure,
            ControlPlaneOperatorAdminFailure::Definite(_)
        ));
        assert_eq!(
            definite.to_string(),
            "control-plane Raft election trigger failed"
        );
        assert!(!definite.to_string().contains(SENSITIVE_DIAGNOSTIC));

        let read_only = ControlPlaneOperatorAdminError::read_only_operation(
            "authority-clock status",
            ControlPlaneError::RpcUnconfirmed {
                message: SENSITIVE_DIAGNOSTIC.to_owned(),
            },
        );
        assert!(matches!(
            &read_only.failure,
            ControlPlaneOperatorAdminFailure::Definite(_)
        ));
        assert_eq!(
            read_only.to_string(),
            "control-plane authority-clock status failed"
        );
        assert!(!read_only.to_string().contains(SENSITIVE_DIAGNOSTIC));

        let clock_mutation = ControlPlaneOperatorAdminError::mutating_operation(
            "authority-clock re-establishment",
            ControlPlaneError::RpcUnconfirmed {
                message: SENSITIVE_DIAGNOSTIC.to_owned(),
            },
        );
        assert!(matches!(
            &clock_mutation.failure,
            ControlPlaneOperatorAdminFailure::MutatingOutcomeUnconfirmed(_)
        ));
        assert_eq!(
            clock_mutation.to_string(),
            "control-plane authority-clock re-establishment may have applied but could not be confirmed; do not retry without an operation-specific confirmation check"
        );
        assert!(!clock_mutation.to_string().contains(SENSITIVE_DIAGNOSTIC));
    }
}
