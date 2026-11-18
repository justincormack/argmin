// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::control_plane::{
    FileControlPlaneStore, NodeMembershipState, SingleAuthorityControlPlane,
};
use crate::control_plane_auth::{
    ControlPlaneAuthPrincipal, ControlPlaneScopedCredential, ControlPlaneScopedCredentialInput,
    ControlPlaneScopedCredentialStore,
};
use std::os::unix::fs::PermissionsExt;

const DIGEST: &str = "123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0";

fn credential_input(node: u32, cluster: &str) -> ControlPlaneScopedCredentialInput {
    ControlPlaneScopedCredentialInput {
        cluster_id: cluster.into(),
        credential_id: "receiver".into(),
        credential_version: 1,
        principal: ControlPlaneAuthPrincipal::StorageNodeProcess {
            node_id: NodeId::new(node),
        },
        secret: b"receiver-confirmation-test-secret".to_vec(),
    }
}

fn auth(
    node: u32,
    cluster: &str,
) -> (
    StorageRpcServerAuthConfig,
    StorageNodeStorageRpcClientCapability,
) {
    auth_with_binding(node, cluster, 1, DIGEST)
}

fn auth_with_binding(
    node: u32,
    cluster: &str,
    generation: u64,
    digest: &str,
) -> (
    StorageRpcServerAuthConfig,
    StorageNodeStorageRpcClientCapability,
) {
    let credential = ControlPlaneScopedCredential::new(credential_input(node, cluster)).unwrap();
    (
        StorageRpcServerAuthConfig::new(
            cluster,
            ControlPlaneScopedCredentialStore::new(vec![credential.clone()]).unwrap(),
            generation,
            digest,
        )
        .unwrap(),
        StorageNodeStorageRpcClientCapability::new(credential, generation, digest).unwrap(),
    )
}

struct Fixture {
    _dir: test_util::TempDir,
    config: StorageNodeProcessConfig,
    map: ClusterRuntimeMapSnapshot,
    endpoints: Vec<(NodeId, StorageRpcClientEndpoint)>,
}

impl Fixture {
    fn new() -> Self {
        Self::with_acting_set(vec![NodeId::new(4), NodeId::new(3), NodeId::new(2)])
    }

    fn with_acting_set(acting: Vec<NodeId>) -> Self {
        let dir = test_util::tempdir();
        let sockets = dir.path().join("sockets");
        fs::create_dir(&sockets).unwrap();
        fs::set_permissions(&sockets, fs::Permissions::from_mode(0o700)).unwrap();
        let mut authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
            dir.path().join("control"),
        ))
        .unwrap();
        let endpoints: Vec<_> = [4, 2, 3]
            .into_iter()
            .map(|id| {
                (
                    NodeId::new(id),
                    StorageRpcClientEndpoint::unix(sockets.join(format!("{id}.sock"))),
                )
            })
            .collect();
        for (id, _) in &endpoints {
            authority
                .set_node_membership(*id, NodeMembershipState::Active)
                .unwrap();
        }
        authority
            .set_pg_acting_set(PgId::new(0), acting.clone())
            .unwrap();
        let now = crate::clock::current_time_millis();
        // Every member reports the final epoch; endpoints come from accepted
        // heartbeats, not the receiver or a shard request.
        for phase in 0..3 {
            if phase == 2 {
                authority
                    .complete_pg_peering(PgId::new(0), NodeId::new(4), 1, now + phase)
                    .unwrap();
            }
            for (id, endpoint) in &endpoints {
                authority
                    .heartbeat(
                        NodeHeartbeat {
                            node_id: *id,
                            node_incarnation: 1,
                            endpoint: endpoint
                                .unix_socket_path()
                                .unwrap()
                                .to_str()
                                .unwrap()
                                .into(),
                            observed_epoch: authority.snapshot().cluster_epoch(),
                            requested_lease_duration_ms:
                                crate::control_plane::MAX_HEARTBEAT_LEASE_MS,
                            cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1)
                                .unwrap(),
                            cluster_map_history_route_references: Default::default(),
                            pg_observations: if acting.contains(id) {
                                vec![crate::control_plane::NodePgHeartbeatObservation {
                                    pg_id: PgId::new(0),
                                    state: if phase == 2 {
                                        PgState::Active
                                    } else {
                                        PgState::Peering
                                    },
                                    metadata_proof: crate::control_plane::PgMetadataProof::current(
                                        0, 0, 0,
                                    ),
                                    metadata_log_epoch: ClusterEpoch::INITIAL,
                                    pending_metadata_command: None,
                                }]
                            } else {
                                Vec::new()
                            },
                        },
                        now + phase,
                    )
                    .unwrap();
            }
        }
        let map = authority.snapshot().runtime_map(now + 3).unwrap();
        assert_eq!(map.pg_routes()[0].state(), PgState::Active);
        let config = StorageNodeProcessConfig::from_runtime_map(
            NodeId::new(3),
            dir.path().join("node"),
            EcShape { k: 2, m: 1 },
            &map,
        )
        .unwrap();
        Self {
            _dir: dir,
            config,
            map,
            endpoints,
        }
    }

    fn prepared(&self) -> PreparedStorageNodeServer {
        let (server, client) = auth(3, "receiver-test");
        PreparedStorageNodeServer::new(self.config.clone())
            .with_rpc_auth(server)
            .with_window_confirmation_peers(self.map.clone(), self.endpoints.clone(), client)
            .unwrap()
    }
}

#[test]
fn receiver_window_peers_select_exact_primary_and_lowest_witness_without_io() {
    let f = Fixture::new();
    let server = f.prepared().bind().unwrap();
    let handler = server.connection_handler();
    let permit = server
        .route_admission
        .acquire(StorageNodeRouteAdmissionClass::Active);
    let selected = handler
        .window_confirmation_selection(&permit, PgId::new(0))
        .unwrap();
    assert_eq!(selected.primary.node_id(), NodeId::new(4));
    assert_eq!(selected.witness.unwrap().node_id(), NodeId::new(2));
    assert!(handler
        .window_confirmation_selection(&permit, PgId::new(1))
        .is_err());
    let foreign =
        StorageNodeRouteAdmissionGate::default().acquire(StorageNodeRouteAdmissionClass::Active);
    assert!(handler
        .window_confirmation_selection(&foreign, PgId::new(0))
        .is_err());
    let retained = server
        .route_admission
        .acquire(StorageNodeRouteAdmissionClass::RetainedCleanup);
    assert!(handler
        .window_confirmation_selection(&retained, PgId::new(0))
        .is_err());
}

#[test]
fn receiver_window_peers_reject_crossed_map_endpoints_and_authentication() {
    let f = Fixture::new();
    for change in [
        "duplicate",
        "missing",
        "extra",
        "endpoint",
        "route",
        "epoch",
        "node-auth",
        "cluster-auth",
        "generation-auth",
        "digest-auth",
    ] {
        let mut config = f.config.clone();
        let mut endpoints = f.endpoints.clone();
        let (server, mut client) = auth(3, "receiver-test");
        match change {
            "duplicate" => endpoints.push(endpoints[0].clone()),
            "missing" => {
                endpoints.pop();
            }
            "extra" => endpoints.push((NodeId::new(9), StorageRpcClientEndpoint::unix("/unused"))),
            "endpoint" => endpoints[0].1 = StorageRpcClientEndpoint::unix("/crossed"),
            "route" => config.pg_routes[0].primary_node_id = NodeId::new(2),
            "epoch" => {
                config.cluster_epoch = ClusterEpoch::new(config.cluster_epoch.get() + 1).unwrap()
            }
            "node-auth" => client = auth(2, "receiver-test").1,
            "cluster-auth" => client = auth(3, "foreign-cluster").1,
            "generation-auth" => client = auth_with_binding(3, "receiver-test", 2, DIGEST).1,
            "digest-auth" => client = auth_with_binding(3, "receiver-test", 1, &"a".repeat(64)).1,
            _ => unreachable!(),
        }
        assert!(
            PreparedStorageNodeServer::new(config)
                .with_rpc_auth(server)
                .with_window_confirmation_peers(f.map.clone(), endpoints, client)
                .is_err(),
            "{change}"
        );
    }
}

#[test]
fn receiver_window_peers_publish_and_clear_with_exact_config_generation() {
    let f = Fixture::new();
    let server = f.prepared().bind().unwrap();
    let mut old = server.connection_handler();
    // A config-only install cannot assert where peers live, even at the same epoch.
    server
        .install_control_plane_runtime_config(f.config.clone())
        .unwrap();
    assert!(server
        .runtime_route_snapshot()
        .window_confirmation_peers
        .is_none());
    let permit = server
        .route_admission
        .acquire(StorageNodeRouteAdmissionClass::Active);
    assert!(old
        .window_confirmation_selection(&permit, PgId::new(0))
        .is_err());
    old.refresh_config_snapshot();
    assert!(Arc::ptr_eq(&old.config, &server.config_snapshot_arc()));
    assert!(old.window_confirmation_peers.is_none());
    assert!(old
        .window_confirmation_selection(&permit, PgId::new(0))
        .is_err());
    assert!(server
        .connection_handler()
        .window_confirmation_selection(&permit, PgId::new(0))
        .is_err());
    drop(permit);
    server
        .install_runtime_config_with_peer_map(f.config.clone(), Some(&f.map))
        .unwrap();
    old.refresh_config_snapshot();
    assert!(Arc::ptr_eq(
        old.window_confirmation_peers.as_ref().unwrap(),
        server
            .runtime_route_snapshot()
            .window_confirmation_peers
            .as_ref()
            .unwrap(),
    ));
    let handler = server.connection_handler();
    let permit = server
        .route_admission
        .acquire(StorageNodeRouteAdmissionClass::Active);
    assert!(handler
        .window_confirmation_selection(&permit, PgId::new(0))
        .is_ok());
    drop(permit);
    // Failure must not publish either half of the config/directory pair.
    let mut crossed = f.config.clone();
    crossed.pg_routes[0].primary_node_id = NodeId::new(2);
    crossed.cluster_epoch = ClusterEpoch::new(crossed.cluster_epoch.get() + 1).unwrap();
    crossed.pg_routes[0].cluster_epoch = crossed.cluster_epoch;
    let before = server.runtime_route_snapshot();
    assert!(server
        .install_runtime_config_with_peer_map(crossed, Some(&f.map))
        .is_err());
    let after = server.runtime_route_snapshot();
    assert!(Arc::ptr_eq(&before.config, &after.config));
    assert!(Arc::ptr_eq(
        before.window_confirmation_peers.as_ref().unwrap(),
        after.window_confirmation_peers.as_ref().unwrap()
    ));
}

#[test]
fn receiver_window_peers_recheck_auth_after_later_builder_options() {
    let f = Fixture::new();
    assert!(f
        .prepared()
        .with_rpc_auth(auth(3, "crossed").0)
        .bind()
        .is_err());
    assert!(!f.config.data_dir.exists());
}

#[test]
fn receiver_window_peers_require_verifier_accepted_credential_before_publication() {
    let f = Fixture::new();
    for change in ["id", "version", "secret", "principal"] {
        let mut changed = credential_input(3, "receiver-test");
        match change {
            "id" => changed.credential_id = "unknown-receiver".into(),
            "version" => changed.credential_version = 2,
            "secret" => changed.secret[0] ^= 1,
            "principal" => {
                changed.principal = ControlPlaneAuthPrincipal::StorageNode {
                    node_id: NodeId::new(3),
                    incarnation: 1,
                }
            }
            _ => unreachable!(),
        }
        let credential = ControlPlaneScopedCredential::new(changed).unwrap();
        let client =
            StorageNodeStorageRpcClientCapability::new(credential.clone(), 1, DIGEST).unwrap();
        let error = PreparedStorageNodeServer::new(f.config.clone())
            .with_rpc_auth(auth(3, "receiver-test").0)
            .with_window_confirmation_peers(f.map.clone(), f.endpoints.clone(), client)
            .err()
            .expect("unaccepted signer must not install a directory");
        assert!(
            matches!(error.retained_store_error(),
            Some(StoreError::Io { context: "receiver window peer configuration", source })
                if source.to_string() == "receiver confirmation authentication domain mismatch"),
            "{change}: {error}"
        );
        assert!(!f.config.data_dir.exists(), "{change}");

        // A later builder option replacing only the verifier must not bypass
        // the same acceptance check at bind, even with an unchanged domain.
        let changed_verifier = StorageRpcServerAuthConfig::new(
            "receiver-test",
            ControlPlaneScopedCredentialStore::new(vec![credential]).unwrap(),
            1,
            DIGEST,
        )
        .unwrap();
        assert!(
            f.prepared().with_rpc_auth(changed_verifier).bind().is_err(),
            "{change}"
        );
        assert!(!f.config.data_dir.exists(), "{change}");
    }
}

#[test]
fn receiver_window_peers_accept_an_exact_retained_rotation_credential() {
    let f = Fixture::new();
    let retained = ControlPlaneScopedCredential::new(credential_input(3, "receiver-test")).unwrap();
    let mut rotated = credential_input(3, "receiver-test");
    rotated.credential_version = 2;
    rotated.secret[0] ^= 1;
    let verifier = StorageRpcServerAuthConfig::new(
        "receiver-test",
        ControlPlaneScopedCredentialStore::new(vec![
            ControlPlaneScopedCredential::new(rotated).unwrap(),
            retained.clone(),
        ])
        .unwrap(),
        1,
        DIGEST,
    )
    .unwrap();
    let server = PreparedStorageNodeServer::new(f.config.clone())
        .with_rpc_auth(verifier)
        .with_window_confirmation_peers(
            f.map.clone(),
            f.endpoints.clone(),
            StorageNodeStorageRpcClientCapability::new(retained, 1, DIGEST).unwrap(),
        )
        .unwrap()
        .bind()
        .unwrap();
    let handler = server.connection_handler();
    let permit = server
        .route_admission
        .acquire(StorageNodeRouteAdmissionClass::Active);
    assert!(handler
        .window_confirmation_selection(&permit, PgId::new(0))
        .is_ok());
}

#[test]
fn receiver_window_peers_singleton_and_expired_receiver_route() {
    let f = Fixture::with_acting_set(vec![NodeId::new(4)]);
    let server = f.prepared().bind().unwrap();
    let mut handler = server.connection_handler();
    let permit = server
        .route_admission
        .acquire(StorageNodeRouteAdmissionClass::Active);
    assert!(handler
        .window_confirmation_selection(&permit, PgId::new(0))
        .unwrap()
        .witness
        .is_none());
    handler.route_map_lease = Some(BoundRouteMapLease::expired(
        f.config.route_map_valid_until_ms().unwrap(),
        crate::clock::monotonic_time_millis(),
    ));
    assert!(handler
        .window_confirmation_selection(&permit, PgId::new(0))
        .is_err());
}

#[test]
fn receiver_window_peers_never_select_a_peering_primary() {
    let f = Fixture::new();
    let server = f.prepared().bind().unwrap();
    // Keep a populated directory to isolate state validation rather than the
    // independent missing-directory rejection on a raw config install.
    {
        let mut state = server.runtime_route_state.write().unwrap();
        let mut config = (*state.config).clone();
        config.pg_routes[0].state = PgState::Peering;
        state.config = Arc::new(config);
    }
    let handler = server.connection_handler();
    let permit = server
        .route_admission
        .acquire(StorageNodeRouteAdmissionClass::Active);
    assert!(handler
        .window_confirmation_selection(&permit, PgId::new(0))
        .is_err());
}
