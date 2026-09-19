// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

#[cfg(test)]
mod runtime_map_refresh_invalidation_tests {
    use super::*;

    #[derive(Clone)]
    struct FixedRuntimeMapSource(ClusterRuntimeMapSnapshot);

    impl ControlPlaneRuntimeMapSource for FixedRuntimeMapSource {
        fn runtime_map_snapshot(
            &self,
            _authority_now_ms: u64,
        ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
            Ok(self.0.clone())
        }

        fn serving_pg_runtime_map_snapshot(
            &self,
            pg_id: PgId,
            _authority_now_ms: u64,
        ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
            self.0
                .pg_routes()
                .iter()
                .any(|route| route.pg_id() == pg_id)
                .then(|| self.0.clone())
                .ok_or(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })
        }
    }

    struct SnapshotCallRuntimeMapSource {
        snapshot: ClusterRuntimeMapSnapshot,
        snapshot_called: Mutex<Option<std::sync::mpsc::Sender<()>>>,
    }

    impl ControlPlaneRuntimeMapSource for SnapshotCallRuntimeMapSource {
        fn runtime_map_snapshot(
            &self,
            _authority_now_ms: u64,
        ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
            if let Some(snapshot_called) = self
                .snapshot_called
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take()
            {
                snapshot_called.send(()).unwrap();
            }
            Ok(self.snapshot.clone())
        }

        fn runtime_map_status(
            &self,
            _authority_now_ms: u64,
        ) -> Result<ControlPlaneRuntimeMapStatus, ControlPlaneError> {
            Ok(ControlPlaneRuntimeMapStatus::new(
                self.snapshot.cluster_epoch(),
                self.snapshot.pg_routes().len(),
                0,
            ))
        }

        fn serving_pg_runtime_map_snapshot(
            &self,
            pg_id: PgId,
            _authority_now_ms: u64,
        ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
            self.snapshot
                .pg_routes()
                .iter()
                .any(|route| route.pg_id() == pg_id)
                .then(|| self.snapshot.clone())
                .ok_or(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })
        }
    }

    fn frontend_storage_rpc_auth() -> crate::StorageRpcClientAuthConfig {
        let credential = crate::control_plane_auth::ControlPlaneScopedCredential::new(
            crate::control_plane_auth::ControlPlaneScopedCredentialInput {
                cluster_id: "refresh-auth-cluster".to_string(),
                credential_id: "frontend-key".to_string(),
                credential_version: 1,
                principal: crate::control_plane_auth::ControlPlaneAuthPrincipal::Frontend {
                    instance_id: "frontend-1".to_string(),
                },
                secret: b"refresh-auth-secret".to_vec(),
            },
        )
        .unwrap();
        crate::FrontendStorageRpcClientCapability::new(credential, 7, "a".repeat(64))
            .unwrap()
            .into()
    }

    fn maintenance_storage_rpc_capability() -> crate::MaintenanceStorageRpcClientCapability {
        let credential = crate::control_plane_auth::ControlPlaneScopedCredential::new(
            crate::control_plane_auth::ControlPlaneScopedCredentialInput {
                cluster_id: "refresh-auth-cluster".to_string(),
                credential_id: "maintenance-key".to_string(),
                credential_version: 1,
                principal: crate::control_plane_auth::ControlPlaneAuthPrincipal::LocalMaintenance {
                    process_id: "frontend-1".to_string(),
                },
                secret: b"maintenance-auth-secret".to_vec(),
            },
        )
        .unwrap();
        crate::MaintenanceStorageRpcClientCapability::new(credential, 7, "a".repeat(64)).unwrap()
    }

    fn static_route_authority_digest(cluster: &StorageCluster) -> [u8; 32] {
        match cluster.route_authority {
            StorageClusterRouteAuthority::Static(proof) => proof.content_digest.0,
            StorageClusterRouteAuthority::Dynamic(_) => {
                panic!("expected a static route-authority proof")
            }
        }
    }

    fn static_topology_cluster(socket_suffix: &str, ec: EcShape) -> Arc<StorageCluster> {
        let mut local_map = LocalClusterMap::open_frontend_topology_only_with_epoch(
            NodeId::new(1),
            [NodeId::new(1), NodeId::new(2)],
            &[31, 32],
            ec,
            ClusterEpoch::INITIAL,
        )
        .unwrap();
        local_map
            .install_unix_storage_node_clients([
                LocalUnixStorageNodeClientConfig::new(
                    NodeId::new(1),
                    format!("/tmp/static-route-node-1-{socket_suffix}.sock"),
                ),
                LocalUnixStorageNodeClientConfig::new(
                    NodeId::new(2),
                    format!("/tmp/static-route-node-2-{socket_suffix}.sock"),
                ),
            ])
            .unwrap();
        StorageCluster::from_static_local_map(Arc::new(local_map)).unwrap()
    }

    #[test]
    fn static_route_authority_digest_is_exact_and_binds_rpc_endpoints() {
        let cluster = static_topology_cluster("a", EcShape { k: 1, m: 1 });
        assert_eq!(
            static_route_authority_digest(&cluster),
            [
                16, 48, 229, 132, 127, 170, 85, 195, 16, 203, 117, 122, 71, 192, 255, 129,
                80, 74, 177, 205, 145, 84, 84, 253, 212, 12, 196, 104, 102, 21, 27, 86,
            ]
        );

        let changed_endpoint_cluster = static_topology_cluster("b", EcShape { k: 1, m: 1 });
        assert_ne!(
            static_route_authority_digest(&cluster),
            static_route_authority_digest(&changed_endpoint_cluster)
        );

        let changed_placement_cluster = static_topology_cluster("a", EcShape { k: 1, m: 0 });
        assert_ne!(
            static_route_authority_digest(&cluster),
            static_route_authority_digest(&changed_placement_cluster)
        );
    }

    #[test]
    fn static_route_authority_digest_distinguishes_non_utf8_unix_endpoints() {
        use std::os::unix::ffi::OsStringExt;

        fn cluster_for_endpoint(endpoint: Vec<u8>) -> Arc<StorageCluster> {
            let mut local_map = LocalClusterMap::open_frontend_topology_only_with_epoch(
                NodeId::new(1),
                [NodeId::new(1)],
                &[31],
                EcShape { k: 1, m: 0 },
                ClusterEpoch::INITIAL,
            )
            .unwrap();
            local_map
                .install_unix_storage_node_clients([LocalUnixStorageNodeClientConfig::new(
                    NodeId::new(1),
                    std::ffi::OsString::from_vec(endpoint),
                )])
                .unwrap();
            StorageCluster::from_static_local_map(Arc::new(local_map)).unwrap()
        }

        let first = cluster_for_endpoint(b"/tmp/static-route-\xff.sock".to_vec());
        let second = cluster_for_endpoint(b"/tmp/static-route-\xfe.sock".to_vec());
        assert_ne!(
            static_route_authority_digest(&first),
            static_route_authority_digest(&second)
        );
    }

    #[test]
    fn static_route_authority_digest_binds_embedded_node_directories() {
        let first_dir = test_util::tempdir();
        let second_dir = test_util::tempdir();
        let first = StorageCluster::open_static_local_nodes(
            first_dir.path(),
            &[NodeId::new(0)],
            &[31],
            EcShape { k: 1, m: 0 },
        )
        .unwrap();
        let second = StorageCluster::open_static_local_nodes(
            second_dir.path(),
            &[NodeId::new(0)],
            &[31],
            EcShape { k: 1, m: 0 },
        )
        .unwrap();

        assert_ne!(
            static_route_authority_digest(&first),
            static_route_authority_digest(&second)
        );
    }

    #[test]
    fn preflight_embedded_route_identity_matches_opened_static_authority() {
        let dir = test_util::tempdir();
        let node_id = NodeId::new(7);
        let pg_ids = [3, 9];
        let ec_shape = EcShape { k: 1, m: 0 };
        let epoch = ClusterEpoch::new(12).unwrap();
        let config = LocalNodeStoreConfig::new(node_id, dir.path().join("node"));

        let prepared = StorageCluster::prepare_standalone_embedded_topology(
            node_id,
            [config.clone()],
            &pg_ids,
            ec_shape,
            epoch,
        )
        .unwrap();
        let preflight = prepared.route_identity();
        assert!(
            !config.data_dir().join("pg-0003").exists(),
            "preflight must not open placement-group storage"
        );

        let cluster = prepared.open().unwrap();

        assert_eq!(preflight, cluster.standalone_route_identity().unwrap());
    }

    #[test]
    fn static_route_authority_rejects_bounded_validity() {
        let route = PgRouteSnapshot::reconstructed(
            ClusterEpoch::INITIAL,
            PgId::new(31),
            NodeId::new(1),
            vec![NodeId::new(1)],
            PgState::Active,
        );
        let local_map = LocalClusterMap::open_frontend_topology_only_with_pg_routes_and_validity(
            NodeId::new(1),
            [NodeId::new(1)],
            &[31],
            EcShape { k: 1, m: 0 },
            ClusterEpoch::INITIAL,
            [LocalPgRoute::from(&route)],
            RouteMapValidity::until_ms(10_000).unwrap(),
        )
        .unwrap();

        assert!(matches!(
            StorageCluster::from_static_local_map(Arc::new(local_map)),
            Err(ClusterBuildError::StaticRouteAuthorityBoundedValidity)
        ));
    }

    #[test]
    fn static_route_authority_cannot_acquire_runtime_map_capability() {
        let cluster = static_topology_cluster("capability", EcShape { k: 1, m: 1 });

        assert!(matches!(
            StorageClusterRuntimeMapHandle::new(cluster),
            Err(StorageClusterRuntimeMapRefreshError::StaticRouteAuthorityRefresh)
        ));
    }

    #[test]
    fn dynamic_route_authority_cannot_acquire_static_route_handle() {
        let cluster = active_test_cluster(RouteMapValidity::until_ms(10_000).unwrap());

        assert!(matches!(
            StorageClusterRouteHandle::from_static_cluster(Arc::clone(&cluster)),
            Err(ClusterBuildError::DynamicRouteAuthorityRequiresRuntimeMapHandle)
        ));
        assert!(matches!(
            cluster.standalone_route_identity(),
            Err(crate::StandaloneRouteIdentityError::DynamicAuthority)
        ));
    }

    fn two_node_runtime_map_with_endpoints(
        first_endpoint: &str,
        second_endpoint: &str,
    ) -> ClusterRuntimeMapSnapshot {
        let tmp = test_util::tempdir();
        let mut authority = crate::control_plane::SingleAuthorityControlPlane::open(
            crate::control_plane::FileControlPlaneStore::new(
                tmp.path().join("control-plane.state"),
            ),
        )
        .unwrap();
        authority
            .bootstrap_initial_cluster_map(
                vec![
                    (NodeId::new(1), first_endpoint.to_owned()),
                    (NodeId::new(2), second_endpoint.to_owned()),
                ],
                vec![PgId::new(31)],
            )
            .unwrap();
        authority.snapshot().runtime_map(1_000).unwrap()
    }

    fn two_node_runtime_map() -> ClusterRuntimeMapSnapshot {
        two_node_runtime_map_with_endpoints("/tmp/runtime-node-1.sock", "/tmp/runtime-node-2.sock")
    }

    fn one_node_runtime_map() -> ClusterRuntimeMapSnapshot {
        let tmp = test_util::tempdir();
        let mut authority = crate::control_plane::SingleAuthorityControlPlane::open(
            crate::control_plane::FileControlPlaneStore::new(
                tmp.path().join("control-plane.state"),
            ),
        )
        .unwrap();
        authority
            .bootstrap_initial_cluster_map(
                vec![(NodeId::new(1), "/tmp/runtime-node-1.sock".to_owned())],
                vec![PgId::new(31)],
            )
            .unwrap();
        authority.snapshot().runtime_map(1_000).unwrap()
    }

    #[test]
    fn role_scoped_runtime_clusters_share_process_local_reclaim_work() {
        let runtime_map = one_node_runtime_map();
        let admission = LocalUnixStorageNodeClientAdmissionSettings::DEFAULT;
        let foreground = StorageCluster::from_runtime_map_with_unix_storage_node_client_admission_settings_and_auth(
            NodeId::new(1),
            &runtime_map,
            EcShape { k: 1, m: 0 },
            admission,
            Some(frontend_storage_rpc_auth()),
        )
        .unwrap();
        let maintenance = StorageCluster::from_runtime_map_with_unix_storage_node_client_admission_settings_and_maintenance_auth_sharing_process_state(
            NodeId::new(1),
            &runtime_map,
            EcShape { k: 1, m: 0 },
            admission,
            maintenance_storage_rpc_capability(),
            &foreground,
        )
        .unwrap();
        let bucket = BucketName::try_from("shared-reclaim".to_string()).unwrap();
        let key = ObjectKey::try_from("object".to_string()).unwrap();

        foreground.enqueue_object_payload_reclaim(&bucket, &key, GenerationId::MIN);

        assert!(maintenance.try_take_reclaim_work().is_some());
        assert!(foreground.try_take_reclaim_work().is_none());
    }

    fn runtime_map_with_historical_route() -> ClusterRuntimeMapSnapshot {
        let tmp = test_util::tempdir();
        let mut authority = crate::control_plane::SingleAuthorityControlPlane::open(
            crate::control_plane::FileControlPlaneStore::new(
                tmp.path().join("control-plane.state"),
            ),
        )
        .unwrap();
        authority
            .bootstrap_initial_cluster_map(
                vec![
                    (NodeId::new(1), "/tmp/runtime-node-1.sock".to_owned()),
                    (NodeId::new(2), "/tmp/runtime-node-2.sock".to_owned()),
                ],
                vec![PgId::new(31)],
            )
            .unwrap();
        authority
            .set_pg_acting_set(PgId::new(31), vec![NodeId::new(2)])
            .unwrap();
        let runtime_map = authority.snapshot().runtime_map(1_000).unwrap();
        assert!(!runtime_map.historical_pg_routes().is_empty());
        assert!(!runtime_map.historical_cluster_epochs().is_empty());
        runtime_map
    }

    #[test]
    fn dynamic_proof_rejects_local_node_set_from_another_runtime_map() {
        let local_runtime_map = two_node_runtime_map();
        let proof_runtime_map = one_node_runtime_map();
        let local_map = LocalClusterMap::open_frontend_topology_only_with_runtime_map(
            NodeId::new(1),
            &local_runtime_map,
            EcShape { k: 1, m: 1 },
        )
        .unwrap();
        local_map.test_store_route_map_validity(proof_runtime_map.validity());

        assert!(matches!(
            StorageCluster::from_runtime_local_map(Arc::new(local_map), &proof_runtime_map),
            Err(ClusterBuildError::DynamicRouteAuthorityNodeSetMismatch)
        ));
    }

    #[test]
    fn dynamic_proof_rejects_local_advertised_endpoint_from_another_runtime_map() {
        let local_runtime_map = two_node_runtime_map();
        let proof_runtime_map = two_node_runtime_map_with_endpoints(
            "/tmp/other-runtime-node-1.sock",
            "/tmp/runtime-node-2.sock",
        );
        let local_map = LocalClusterMap::open_frontend_topology_only_with_runtime_map(
            NodeId::new(1),
            &local_runtime_map,
            EcShape { k: 1, m: 1 },
        )
        .unwrap();

        assert!(matches!(
            StorageCluster::from_runtime_local_map(Arc::new(local_map), &proof_runtime_map),
            Err(ClusterBuildError::DynamicRouteAuthorityNodeEndpointMismatch { id: 1 })
        ));
    }

    #[test]
    fn dynamic_proof_rejects_local_pg_routes_from_another_map() {
        let runtime_map = two_node_runtime_map();
        let local_map = LocalClusterMap::open_frontend_topology_only_with_runtime_map(
            NodeId::new(1),
            &runtime_map,
            EcShape { k: 1, m: 1 },
        )
        .unwrap();
        let authority_route = &runtime_map.pg_routes()[0];
        let mismatched_route = PgRouteSnapshot::reconstructed(
            authority_route.cluster_epoch(),
            authority_route.pg_id(),
            NodeId::new(2),
            vec![NodeId::new(2), NodeId::new(1)],
            PgState::Active,
        );
        let mismatched_local_map = local_map
            .test_clone_with_pg_routes(
                runtime_map.cluster_epoch(),
                [mismatched_route],
                runtime_map.historical_pg_routes().iter().cloned(),
            )
            .unwrap();
        mismatched_local_map.test_store_route_map_validity(runtime_map.validity());

        assert!(matches!(
            StorageCluster::from_runtime_local_map(Arc::new(mismatched_local_map), &runtime_map,),
            Err(ClusterBuildError::DynamicRouteAuthorityPgRoutesMismatch)
        ));
    }

    #[test]
    fn dynamic_proof_rejects_mismatched_historical_recovery_route() {
        let runtime_map = runtime_map_with_historical_route();
        let local_map = LocalClusterMap::open_frontend_topology_only_with_runtime_map(
            NodeId::new(1),
            &runtime_map,
            EcShape { k: 1, m: 1 },
        )
        .unwrap();
        let mut mismatched_routes = runtime_map.historical_pg_routes().to_vec();
        let authority_route = &mismatched_routes[0];
        let mismatched_primary = if authority_route.primary_node_id() == NodeId::new(1) {
            NodeId::new(2)
        } else {
            NodeId::new(1)
        };
        mismatched_routes[0] = PgRouteSnapshot::reconstructed(
            authority_route.cluster_epoch(),
            authority_route.pg_id(),
            mismatched_primary,
            vec![mismatched_primary],
            authority_route.state(),
        );
        let mismatched_local_map = local_map
            .test_clone_with_pg_routes(
                runtime_map.cluster_epoch(),
                runtime_map.pg_routes().iter().cloned(),
                mismatched_routes,
            )
            .unwrap();
        mismatched_local_map.test_store_route_map_validity(runtime_map.validity());

        assert!(matches!(
            StorageCluster::from_runtime_local_map(Arc::new(mismatched_local_map), &runtime_map),
            Err(ClusterBuildError::DynamicRouteAuthorityHistoricalPgRoutesMismatch)
        ));
    }

    #[test]
    fn dynamic_proof_rejects_mismatched_historical_recovery_epochs() {
        let runtime_map = runtime_map_with_historical_route();
        let mut local_map = LocalClusterMap::open_frontend_topology_only_with_runtime_map(
            NodeId::new(1),
            &runtime_map,
            EcShape { k: 1, m: 1 },
        )
        .unwrap();
        local_map.test_remove_historical_cluster_epoch(runtime_map.historical_cluster_epochs()[0]);

        assert!(matches!(
            StorageCluster::from_runtime_local_map(Arc::new(local_map), &runtime_map),
            Err(ClusterBuildError::DynamicRouteAuthorityHistoricalEpochsMismatch)
        ));
    }

    #[test]
    fn dynamic_rpc_constructor_rejects_mismatched_advertised_endpoint() {
        let runtime_map = two_node_runtime_map();
        let result = StorageCluster::from_runtime_map_with_storage_rpc_endpoints_and_auth(
            NodeId::new(1),
            &runtime_map,
            EcShape { k: 1, m: 1 },
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            [
                (
                    NodeId::new(1),
                    crate::storage_rpc_transport::StorageRpcClientEndpoint::unix(
                        "/tmp/wrong-node-1.sock",
                    ),
                ),
                (
                    NodeId::new(2),
                    crate::storage_rpc_transport::StorageRpcClientEndpoint::unix(
                        "/tmp/runtime-node-2.sock",
                    ),
                ),
            ],
            frontend_storage_rpc_auth(),
        );

        assert!(matches!(
            result,
            Err(ClusterBuildError::RemoteStorageNodeClientEndpointAuthorityMismatch { id: 1 })
        ));
    }

    #[test]
    fn dynamic_rpc_constructor_rejects_incomplete_endpoint_set() {
        let runtime_map = two_node_runtime_map();
        let result = StorageCluster::from_runtime_map_with_storage_rpc_endpoints_and_auth(
            NodeId::new(1),
            &runtime_map,
            EcShape { k: 1, m: 1 },
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            [(
                NodeId::new(1),
                crate::storage_rpc_transport::StorageRpcClientEndpoint::unix(
                    "/tmp/runtime-node-1.sock",
                ),
            )],
            frontend_storage_rpc_auth(),
        );

        assert!(matches!(
            result,
            Err(ClusterBuildError::RuntimeMapStorageNodeClientMissing { id: 2 })
        ));
    }

    #[test]
    fn local_recovery_map_cannot_fabricate_new_node() {
        let current = LocalClusterMap::open_frontend_topology_only_with_runtime_map(
            NodeId::new(1),
            &one_node_runtime_map(),
            EcShape { k: 1, m: 0 },
        )
        .unwrap();
        assert!(matches!(
            LocalClusterMap::open_runtime_map_with_existing_local_nodes(
                &current,
                &two_node_runtime_map()
            ),
            Err(ClusterBuildError::HistoricalRecoveryNodeSetMismatch { current, candidate })
                if current == vec![1] && candidate == vec![1, 2]
        ));
    }

    fn active_test_cluster(validity: RouteMapValidity) -> Arc<StorageCluster> {
        let route = PgRouteSnapshot::reconstructed(
            ClusterEpoch::INITIAL,
            PgId::new(31),
            NodeId::new(1),
            vec![NodeId::new(1)],
            PgState::Active,
        );
        let local_map = LocalClusterMap::open_frontend_topology_only_with_pg_routes_and_validity(
            NodeId::new(1),
            [NodeId::new(1)],
            &[31],
            EcShape { k: 1, m: 0 },
            ClusterEpoch::INITIAL,
            [LocalPgRoute::from(&route)],
            validity,
        )
        .unwrap();
        StorageCluster::test_from_local_map_with_epoch(Arc::new(local_map), ClusterEpoch::INITIAL)
            .unwrap()
    }

    #[test]
    fn bucket_write_owner_token_is_opaque() {
        let cluster = active_test_cluster(RouteMapValidity::until_ms(10_000).unwrap());
        let owner_token = cluster.bucket_write_owner_token();
        let owner_identity = owner_token
            .strip_prefix("bucket-write-owner-")
            .expect("bucket-write owner token should have an opaque type prefix");

        assert_eq!(owner_identity.len(), 32);
        assert!(owner_identity
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()));
        assert!(!owner_token.contains(&format!("{:p}", Arc::as_ptr(&cluster.local_map))));
    }

    #[test]
    fn process_local_registry_keys_are_unique_and_debug_redacted() {
        let first = active_test_cluster(RouteMapValidity::until_ms(10_000).unwrap())
            .process_local_registry_key();
        let second = active_test_cluster(RouteMapValidity::until_ms(10_000).unwrap())
            .process_local_registry_key();

        assert_ne!(first, second);
        assert_eq!(format!("{first:?}"), "ProcessLocalRegistryKey([redacted])");
    }

    #[test]
    fn authoritative_pending_metadata_refresh_failure_expires_same_epoch_generations() {
        let pinned = active_test_cluster(RouteMapValidity::until_ms(10_000).unwrap());
        let handle = StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&pinned));
        let current = handle.current();

        handle.expire_same_epoch_generations(5_000);

        assert_eq!(pinned.route_map_valid_until_ms(), Some(5_000));
        assert_eq!(current.route_map_valid_until_ms(), Some(5_000));
        assert_eq!(handle.current().route_map_valid_until_ms(), Some(5_000));
        assert!(matches!(
            handle.current().require_route_map_valid_at(5_000),
            Err(StoreError::RouteMapExpired {
                cluster_epoch,
                valid_until_ms: 5_000,
                now_ms: 5_000,
            }) if cluster_epoch == ClusterEpoch::INITIAL
        ));
    }

    #[test]
    fn content_changing_unix_runtime_map_refresh_retains_storage_rpc_auth() {
        let tmp = test_util::tempdir();
        let mut authority = crate::control_plane::SingleAuthorityControlPlane::open(
            crate::control_plane::FileControlPlaneStore::new(
                tmp.path().join("control-plane.state"),
            ),
        )
        .unwrap();
        authority
            .bootstrap_initial_cluster_map(
                vec![
                    (NodeId::new(1), "/tmp/refresh-node-1.sock".to_string()),
                    (NodeId::new(2), "/tmp/refresh-node-2.sock".to_string()),
                ],
                vec![PgId::new(31)],
            )
            .unwrap();
        let initial_runtime_map = authority.snapshot().runtime_map(1_000).unwrap();
        let cluster = StorageCluster::from_runtime_map_with_unix_storage_node_client_admission_settings_and_auth(
            NodeId::new(1),
            &initial_runtime_map,
            EcShape { k: 1, m: 0 },
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            Some(frontend_storage_rpc_auth()),
        )
        .unwrap();
        let owner_token = cluster.bucket_write_owner_token();
        authority
            .set_pg_acting_set(PgId::new(31), vec![NodeId::new(2)])
            .unwrap();
        let changed_runtime_map = authority.snapshot().runtime_map(1_001).unwrap();
        assert_ne!(
            initial_runtime_map.content_digest(),
            changed_runtime_map.content_digest()
        );

        let refreshed = cluster
            .refresh_from_control_plane_runtime_map_with_unix_storage_node_clients(
                &FixedRuntimeMapSource(changed_runtime_map),
                1_001,
                LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            )
            .unwrap();

        assert!(refreshed.rpc_auth.is_some());
        assert_eq!(refreshed.local_node_count(), 2);
        assert_eq!(refreshed.bucket_write_owner_token(), owner_token);
    }

    #[test]
    fn content_changing_refresh_fetches_snapshot_after_admitted_requests_drain() {
        let tmp = test_util::tempdir();
        let mut authority = crate::control_plane::SingleAuthorityControlPlane::open(
            crate::control_plane::FileControlPlaneStore::new(
                tmp.path().join("control-plane.state"),
            ),
        )
        .unwrap();
        authority
            .bootstrap_initial_cluster_map(
                vec![
                    (NodeId::new(1), "/tmp/refresh-node-1.sock".to_string()),
                    (NodeId::new(2), "/tmp/refresh-node-2.sock".to_string()),
                ],
                vec![PgId::new(31)],
            )
            .unwrap();
        let initial_runtime_map = authority.snapshot().runtime_map(1_000).unwrap();
        let cluster = StorageCluster::from_runtime_map_with_unix_storage_node_client_admission_settings_and_auth(
            NodeId::new(1),
            &initial_runtime_map,
            EcShape { k: 1, m: 0 },
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            Some(frontend_storage_rpc_auth()),
        )
        .unwrap();
        authority
            .set_pg_acting_set(PgId::new(31), vec![NodeId::new(2)])
            .unwrap();
        let changed_runtime_map = authority.snapshot().runtime_map(1_001).unwrap();
        assert_ne!(
            initial_runtime_map.content_digest(),
            changed_runtime_map.content_digest()
        );

        let runtime_handle = StorageClusterRuntimeMapHandle::new(cluster).unwrap();
        let route_handle = runtime_handle.route_handle();
        let admission = crate::clock::with_time_override(1_000, || {
            route_handle.admit_current_route().unwrap()
        });
        let (snapshot_called_tx, snapshot_called_rx) = std::sync::mpsc::channel();
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let refresh = thread::spawn(move || {
            let source = SnapshotCallRuntimeMapSource {
                snapshot: changed_runtime_map,
                snapshot_called: Mutex::new(Some(snapshot_called_tx)),
            };
            let result = crate::clock::with_time_override(1_001, || {
                runtime_handle.refresh_from_control_plane_runtime_map_with_unix_storage_node_clients(
                    &source,
                    1_001,
                    LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
                )
            });
            result_tx.send(result).unwrap();
        });

        route_handle.test_wait_until_route_publication_is_pending();
        assert!(matches!(
            snapshot_called_rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));

        drop(admission);
        snapshot_called_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("runtime-map snapshot was not fetched after admitted requests drained");
        result_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("runtime-map refresh did not complete")
            .unwrap();
        refresh.join().unwrap();
    }

    #[test]
    fn content_changing_runtime_map_refresh_retains_tls_storage_rpc_endpoints() {
        let tmp = test_util::tempdir();
        let mut authority = crate::control_plane::SingleAuthorityControlPlane::open(
            crate::control_plane::FileControlPlaneStore::new(
                tmp.path().join("control-plane.state"),
            ),
        )
        .unwrap();
        authority
            .bootstrap_initial_cluster_map(
                vec![(NodeId::new(1), "tcp://localhost:7701".to_string())],
                vec![PgId::new(31)],
            )
            .unwrap();
        let initial_runtime_map = authority.snapshot().runtime_map(1_000).unwrap();
        let mut tls =
            rustls::ClientConfig::builder_with_provider(tls_provider::configured_provider())
                .with_protocol_versions(&[&rustls::version::TLS13])
                .unwrap()
                .with_root_certificates(rustls::RootCertStore::empty())
                .with_no_client_auth();
        tls.alpn_protocols = vec![crate::storage_rpc_transport::STORAGE_RPC_TLS_ALPN.to_vec()];
        let endpoint = crate::storage_rpc_transport::StorageRpcClientEndpoint::tcp_with_config(
            "tcp://localhost:7701",
            vec!["127.0.0.1:7701".parse().unwrap()],
            "localhost",
            Arc::new(tls),
        )
        .unwrap();
        let cluster = StorageCluster::from_runtime_map_with_storage_rpc_endpoints_and_auth(
            NodeId::new(1),
            &initial_runtime_map,
            EcShape { k: 1, m: 0 },
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            [(NodeId::new(1), endpoint)],
            frontend_storage_rpc_auth(),
        )
        .unwrap();
        authority
            .set_node_membership(
                NodeId::new(2),
                crate::control_plane::NodeMembershipState::Active,
            )
            .unwrap();
        let changed_runtime_map = authority.snapshot().runtime_map(1_001).unwrap();

        let refreshed = cluster
            .refresh_from_control_plane_runtime_map_with_unix_storage_node_clients(
                &FixedRuntimeMapSource(changed_runtime_map),
                1_001,
                LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            )
            .unwrap();

        assert!(refreshed
            .rpc_endpoints
            .as_ref()
            .unwrap()
            .get(&NodeId::new(1))
            .is_some_and(crate::storage_rpc_transport::StorageRpcClientEndpoint::is_tls_tcp));
    }

    #[test]
    fn historical_recovery_cluster_retains_tls_storage_rpc_endpoints() {
        let tmp = test_util::tempdir();
        let mut authority = crate::control_plane::SingleAuthorityControlPlane::open(
            crate::control_plane::FileControlPlaneStore::new(
                tmp.path().join("control-plane.state"),
            ),
        )
        .unwrap();
        authority
            .bootstrap_initial_cluster_map(
                vec![
                    (NodeId::new(1), "tcp://localhost:7701".to_string()),
                    (NodeId::new(2), "tcp://localhost:7702".to_string()),
                ],
                vec![PgId::new(31)],
            )
            .unwrap();
        let initial_runtime_map = authority.snapshot().runtime_map(1_000).unwrap();
        authority
            .set_pg_acting_set(PgId::new(31), vec![NodeId::new(2)])
            .unwrap();
        let current_runtime_map = authority.snapshot().runtime_map(1_001).unwrap();
        let historical_runtime_map = current_runtime_map
            .runtime_map_at_epoch(initial_runtime_map.cluster_epoch())
            .unwrap();
        let mut tls =
            rustls::ClientConfig::builder_with_provider(tls_provider::configured_provider())
                .with_protocol_versions(&[&rustls::version::TLS13])
                .unwrap()
                .with_root_certificates(rustls::RootCertStore::empty())
                .with_no_client_auth();
        tls.alpn_protocols = vec![crate::storage_rpc_transport::STORAGE_RPC_TLS_ALPN.to_vec()];
        let tls = Arc::new(tls);
        let endpoints = [(1_u32, 7701_u16), (2, 7702)].map(|(node_id, port)| {
            (
                NodeId::new(node_id),
                crate::storage_rpc_transport::StorageRpcClientEndpoint::tcp_with_config(
                    format!("tcp://localhost:{port}"),
                    vec![format!("127.0.0.1:{port}").parse().unwrap()],
                    "localhost",
                    Arc::clone(&tls),
                )
                .unwrap(),
            )
        });
        let current = StorageCluster::from_runtime_map_with_storage_rpc_endpoints_and_auth(
            NodeId::new(1),
            &current_runtime_map,
            EcShape { k: 1, m: 1 },
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            endpoints,
            frontend_storage_rpc_auth(),
        )
        .unwrap();

        let recovery = current
            .historical_recovery_cluster_with_storage_rpc_clients(
                &historical_runtime_map,
                LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            )
            .unwrap();

        assert_eq!(
            recovery.operation_epoch(),
            initial_runtime_map.cluster_epoch()
        );
        assert!(recovery.rpc_auth.is_some());
        assert!(recovery
            .rpc_endpoints
            .as_ref()
            .unwrap()
            .values()
            .all(crate::storage_rpc_transport::StorageRpcClientEndpoint::is_tls_tcp));
    }

    #[test]
    fn scoped_pending_recovery_map_matches_full_inventory_with_unassigned_spare() {
        let tmp = test_util::tempdir();
        let mut authority = crate::control_plane::SingleAuthorityControlPlane::open(
            crate::control_plane::FileControlPlaneStore::new(
                tmp.path().join("control-plane.state"),
            ),
        )
        .unwrap();
        let nodes = (1..=4)
            .map(|id| (NodeId::new(id), format!("/tmp/recovery-node-{id}.sock")))
            .collect::<Vec<_>>();
        let pg_id = PgId::new(31);
        let pg_acting_sets = vec![(pg_id, vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)])];
        let topology = crate::control_plane::InitialClusterTopologyCertificate::new_for_bootstrap_map(
            7,
            [0x53; crate::control_plane::CONTROL_PLANE_TOPOLOGY_DIGEST_LEN],
            vec![1],
            &nodes,
            &pg_acting_sets,
            crate::control_plane::test_certified_storage_placement_policy(
                (1..=4).map(NodeId::new),
                3,
                2_000,
            ),
        )
        .unwrap();
        crate::control_plane::ControlPlaneLinearizedCommandSink::submit_control_plane_command(
            &mut authority,
            crate::control_plane_command::ControlPlaneCommand::BootstrapCertifiedInitialClusterMap {
                nodes: nodes.clone(),
                pg_acting_sets,
                topology,
            },
        )
        .unwrap();
        let current_map = authority.snapshot().runtime_map(1_000).unwrap();
        assert_eq!(current_map.nodes().len(), nodes.len());
        assert!(!current_map.pg_routes()[0]
            .acting_set()
            .contains(&NodeId::new(4)));
        let endpoints = nodes.iter().map(|(node_id, endpoint)| {
            (
                *node_id,
                crate::storage_rpc_transport::StorageRpcClientEndpoint::unix(endpoint),
            )
        });
        let current = StorageCluster::from_runtime_map_with_storage_rpc_endpoints_and_auth(
            NodeId::new(4),
            &current_map,
            EcShape { k: 2, m: 1 },
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            endpoints,
            frontend_storage_rpc_auth(),
        )
        .unwrap();
        let current_with_routed_primary =
            StorageCluster::from_runtime_map_with_storage_rpc_endpoints_and_auth(
                NodeId::new(1),
                &current_map,
                EcShape { k: 2, m: 1 },
                LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
                nodes.iter().map(|(node_id, endpoint)| {
                    (
                        *node_id,
                        crate::storage_rpc_transport::StorageRpcClientEndpoint::unix(endpoint),
                    )
                }),
                frontend_storage_rpc_auth(),
            )
            .unwrap();

        authority.set_pg_state(pg_id, PgState::Peering).unwrap();
        let scoped = crate::control_plane::ControlPlaneRuntimeMapSource::pg_runtime_map_snapshot(
            &authority, pg_id, 1_001,
        )
        .unwrap();
        assert_eq!(scoped.pg_routes().len(), 1);
        assert_eq!(scoped.pg_routes()[0].pg_id(), pg_id);
        assert_eq!(scoped.pg_routes()[0].state(), PgState::Peering);
        assert!(scoped
            .historical_pg_routes()
            .iter()
            .all(|route| !route.acting_set().contains(&NodeId::new(4))));
        assert_eq!(scoped.nodes().len(), nodes.len());
        assert_eq!(scoped.nodes(), current_map.nodes());

        LocalClusterMap::open_runtime_map_with_existing_local_nodes(&current.local_map, &scoped)
            .unwrap();

        let recovery = current
            .historical_recovery_cluster_with_storage_rpc_clients(
                &scoped,
                LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            )
            .unwrap();
        assert_eq!(recovery.metadata_node_id(), NodeId::new(4));
        assert_eq!(recovery.operation_epoch(), scoped.cluster_epoch());

        authority
            .set_pg_acting_set(pg_id, vec![NodeId::new(1), NodeId::new(2), NodeId::new(4)])
            .unwrap();
        authority
            .set_pg_acting_set(pg_id, vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)])
            .unwrap();
        authority
            .set_node_membership(NodeId::new(4), crate::control_plane::NodeMembershipState::Removed)
            .unwrap();
        assert!(authority
            .snapshot()
            .runtime_map(1_002)
            .unwrap()
            .nodes()
            .iter()
            .any(|node| node.node_id() == NodeId::new(4)));

        for step in 0..260 {
            let state = if step % 2 == 0 {
                PgState::Degraded
            } else {
                PgState::Peering
            };
            authority.set_pg_state(pg_id, state).unwrap();
        }
        assert!(authority.snapshot().cluster_map_history().iter().all(|history| {
            history
                .pgs()
                .iter()
                .all(|pg| !pg.acting_set().contains(&NodeId::new(4)))
        }));
        let current_map = authority.snapshot().runtime_map(2_000).unwrap();
        let scoped = crate::control_plane::ControlPlaneRuntimeMapSource::pg_runtime_map_snapshot(
            &authority, pg_id, 2_001,
        )
        .unwrap();
        assert_eq!(current_map.nodes().len(), 3);
        assert_eq!(scoped.nodes(), current_map.nodes());

        assert_eq!(current.metadata_node_id(), NodeId::new(4));
        assert!(current.rpc_endpoints.as_ref().unwrap().contains_key(&NodeId::new(4)));
        let refreshed = current
            .refresh_from_control_plane_runtime_map_with_unix_storage_node_clients(
                &FixedRuntimeMapSource(current_map.clone()),
                2_000,
                LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            )
            .unwrap();
        assert_eq!(refreshed.metadata_node_id(), NodeId::new(1));
        assert_eq!(refreshed.local_map.node_count(), 3);
        assert!(!refreshed.local_map.node_ids().any(|node_id| node_id == NodeId::new(4)));
        assert!(refreshed.rpc_endpoints.as_ref().unwrap().contains_key(&NodeId::new(4)));
        LocalClusterMap::open_runtime_map_with_existing_local_nodes(&refreshed.local_map, &scoped)
            .unwrap();
        refreshed
            .historical_recovery_cluster_with_storage_rpc_clients(
                &scoped,
                LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            )
            .unwrap();

        let refreshed_with_routed_primary = current_with_routed_primary
            .refresh_from_control_plane_runtime_map_with_unix_storage_node_clients(
                &FixedRuntimeMapSource(current_map),
                2_000,
                LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            )
            .unwrap();
        assert_eq!(refreshed_with_routed_primary.metadata_node_id(), NodeId::new(1));
        assert_eq!(refreshed_with_routed_primary.local_map.node_count(), 3);
        assert!(!refreshed_with_routed_primary
            .local_map
            .node_ids()
            .any(|node_id| node_id == NodeId::new(4)));
    }

    #[test]
    fn same_epoch_install_shrinks_pinned_generation_validity() {
        let (pinned, handle, old_current, candidate) =
            crate::clock::with_time_override(1_000, || {
                let pinned = active_test_cluster(RouteMapValidity::until_ms(5_000).unwrap());
                pinned.test_store_route_map_validity(RouteMapValidity::until_ms(5_000).unwrap());
                let handle =
                    StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&pinned));
                let old_current = handle.current();
                let candidate = active_test_cluster(RouteMapValidity::until_ms(4_000).unwrap());
                candidate.test_store_route_map_validity(RouteMapValidity::until_ms(4_000).unwrap());
                (pinned, handle, old_current, candidate)
            });

        crate::clock::with_time_override(1_000, || {
            handle.install(Arc::clone(&candidate)).unwrap();
        });

        assert_eq!(pinned.route_map_valid_until_ms(), Some(4_000));
        assert_eq!(old_current.route_map_valid_until_ms(), Some(4_000));
        assert_eq!(candidate.route_map_valid_until_ms(), Some(4_000));
        assert_eq!(handle.current().route_map_valid_until_ms(), Some(4_000));
        assert!(matches!(
            pinned.require_route_map_valid_at(4_000),
            Err(StoreError::RouteMapExpired {
                cluster_epoch,
                valid_until_ms: 4_000,
                now_ms: 4_000,
            }) if cluster_epoch == ClusterEpoch::INITIAL
        ));
    }

    #[test]
    fn same_epoch_new_authority_incarnation_does_not_extend_pinned_generation() {
        let (pinned, handle, mut candidate) = crate::clock::with_time_override(1_000, || {
            let pinned = active_test_cluster(RouteMapValidity::until_ms(5_000).unwrap());
            pinned.test_store_route_map_validity(RouteMapValidity::until_ms(5_000).unwrap());
            let handle = StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&pinned));
            let candidate = active_test_cluster(RouteMapValidity::until_ms(9_000).unwrap());
            candidate.test_store_route_map_validity(RouteMapValidity::until_ms(9_000).unwrap());
            (pinned, handle, candidate)
        });
        let candidate_authority = match candidate.route_authority {
            StorageClusterRouteAuthority::Static(_) => {
                panic!("bounded test cluster must have dynamic authority")
            }
            StorageClusterRouteAuthority::Dynamic(proof) => proof,
        };
        Arc::get_mut(&mut candidate)
            .expect("new candidate has one owner")
            .route_authority = StorageClusterRouteAuthority::Dynamic(DynamicRouteAuthorityProof {
            content_digest: candidate_authority.content_digest,
            freshness_proof: RuntimeMapFreshnessProof::SingleAuthority {
                authority_incarnation: crate::control_plane::AuthorityIncarnation::new(2).unwrap(),
                issued_at_ms: 1_000,
            },
        });

        crate::clock::with_time_override(1_000, || {
            handle.install(Arc::clone(&candidate)).unwrap();
        });

        assert_eq!(pinned.route_map_valid_until_ms(), Some(5_000));
        assert_eq!(handle.current().route_map_valid_until_ms(), Some(9_000));
        assert!(Arc::ptr_eq(&handle.current(), &candidate));

        let renewal = ControlPlaneRuntimeMapStatus::test_with_lease_renewal(
            ClusterEpoch::INITIAL,
            candidate_authority.content_digest,
            RouteMapValidity::until_ms(12_000).unwrap(),
            RuntimeMapFreshnessProof::SingleAuthority {
                authority_incarnation: crate::control_plane::AuthorityIncarnation::new(2).unwrap(),
                issued_at_ms: 1_000,
            },
        );
        crate::clock::with_time_override(1_000, || {
            handle
                .renew_from_runtime_map_status(renewal, 1_000, 1_000)
                .unwrap()
                .expect("matching new-authority renewal must extend the current generation");
        });

        assert_eq!(pinned.route_map_valid_until_ms(), Some(5_000));
        assert_eq!(candidate.route_map_valid_until_ms(), Some(12_000));
        assert_eq!(handle.current().route_map_valid_until_ms(), Some(12_000));
    }

    #[test]
    fn admitted_frontend_route_blocks_runtime_map_publication_until_release() {
        let (pinned, handle, admission, candidate) =
            crate::clock::with_time_override(1_000, || {
                let pinned = active_test_cluster(RouteMapValidity::until_ms(10_000).unwrap());
                pinned.test_store_route_map_validity(RouteMapValidity::until_ms(10_000).unwrap());
                let handle =
                    StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&pinned));
                let admission = handle.admit_current_route().unwrap();
                let candidate = active_test_cluster(RouteMapValidity::until_ms(9_000).unwrap());
                candidate.test_store_route_map_validity(RouteMapValidity::until_ms(9_000).unwrap());
                (pinned, handle, admission, candidate)
            });
        let installer_handle = handle.clone();
        let installed_candidate = Arc::clone(&candidate);
        let (installed_tx, installed_rx) = std::sync::mpsc::channel();
        let installer = thread::spawn(move || {
            crate::clock::with_time_override(1_000, || {
                installer_handle.install(installed_candidate).unwrap();
            });
            installed_tx.send(()).unwrap();
        });

        handle.route_admission.wait_until_publication_is_pending();
        assert!(matches!(
            installed_rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        assert!(Arc::ptr_eq(&handle.current(), &pinned));

        drop(admission);
        installed_rx.recv().unwrap();
        installer.join().unwrap();
        assert!(Arc::ptr_eq(&handle.current(), &candidate));
    }

    #[test]
    fn expired_candidate_is_rejected_after_admitted_requests_drain() {
        let (current, handle, admission, candidate) =
            crate::clock::with_time_override(1_000, || {
                let current = active_test_cluster(RouteMapValidity::until_ms(10_000).unwrap());
                current.test_store_route_map_validity(RouteMapValidity::until_ms(10_000).unwrap());
                let handle =
                    StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&current));
                let admission = handle.admit_current_route().unwrap();
                let candidate = active_test_cluster(RouteMapValidity::until_ms(1_500).unwrap());
                candidate.test_store_route_map_validity(RouteMapValidity::until_ms(1_500).unwrap());
                (current, handle, admission, candidate)
            });
        let installer_handle = handle.clone();
        let installed_candidate = Arc::clone(&candidate);
        let (drained_tx, drained_rx) = std::sync::mpsc::channel();
        let (advance_tx, advance_rx) = std::sync::mpsc::channel();
        let installer = thread::spawn(move || {
            let clock = crate::clock::test_time_override_guard(1_000);
            installer_handle.install_with_after_drain(installed_candidate, || {
                drained_tx.send(()).unwrap();
                advance_rx.recv().unwrap();
                clock.set(2_000);
            })
        });

        handle.route_admission.wait_until_publication_is_pending();
        drop(admission);
        drained_rx.recv().unwrap();
        advance_tx.send(()).unwrap();

        assert!(matches!(
            installer.join().unwrap(),
            Err(
                StorageClusterRuntimeMapRefreshError::ExpiredRouteMapValidity {
                    candidate: ClusterEpoch::INITIAL,
                    valid_until_ms: 1_500,
                    now_ms: 2_000,
                }
            )
        ));
        assert!(Arc::ptr_eq(&handle.current(), &current));
        assert!(!Arc::ptr_eq(&handle.current(), &candidate));
        crate::clock::with_time_override(2_000, || {
            handle.admit_current_route().unwrap();
        });
    }

    #[test]
    fn runtime_map_route_handles_share_the_capability_admission_domain() {
        let cluster = active_test_cluster(RouteMapValidity::until_ms(10_000).unwrap());
        let runtime = StorageClusterRuntimeMapHandle::new(cluster).unwrap();
        let handle = runtime.route_handle();
        let clone = handle.clone();
        let separately_derived = runtime.route_handle();

        assert!(handle.shares_route_admission_with(&clone));
        assert!(handle.shares_route_admission_with(&separately_derived));
    }

    #[test]
    fn admitted_frontend_route_rejects_a_different_runtime_map_generation() {
        crate::clock::with_time_override(1_000, || {
            let admitted_cluster = active_test_cluster(RouteMapValidity::until_ms(5_000).unwrap());
            let unrelated_cluster = active_test_cluster(RouteMapValidity::until_ms(5_000).unwrap());
            admitted_cluster
                .test_store_route_map_validity(RouteMapValidity::until_ms(5_000).unwrap());
            unrelated_cluster
                .test_store_route_map_validity(RouteMapValidity::until_ms(5_000).unwrap());
            let handle =
                StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&admitted_cluster));
            let admission = handle.admit_current_route().unwrap();

            admission
                .require_valid_now_for_raw(&admitted_cluster)
                .unwrap();
            assert!(matches!(
                admission.require_valid_now_for_raw(&unrelated_cluster),
                Err(StoreError::RouteAdmissionClusterMismatch {
                    admitted_epoch,
                    operation_epoch,
                }) if admitted_epoch == ClusterEpoch::INITIAL
                    && operation_epoch == ClusterEpoch::INITIAL
            ));
        });
    }

    #[test]
    fn admitted_frontend_route_rejects_a_different_publication_domain() {
        crate::clock::with_time_override(1_000, || {
            let cluster = active_test_cluster(RouteMapValidity::until_ms(5_000).unwrap());
            cluster.test_store_route_map_validity(RouteMapValidity::until_ms(5_000).unwrap());
            let admitted_handle =
                StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&cluster));
            let unrelated_handle =
                StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&cluster));
            let admission = admitted_handle.admit_current_route().unwrap();

            admitted_handle
                .require_admission_valid_now(&admission)
                .unwrap();
            let error = unrelated_handle
                .require_admission_valid_now(&admission)
                .unwrap_err();
            assert_eq!(
                error.class(),
                crate::StoreOperationFailureClass::RetryableConvergence
            );
            assert_eq!(error.diagnostic_cause_label(), "store_topology_failure");
            assert_eq!(error.to_string(), "storage operation failed");
            assert!(std::error::Error::source(&error).is_none());
            let debug = format!("{error:?}");
            assert!(!debug.contains("RouteAdmissionClusterMismatch"));
            assert!(!debug.contains("admitted_epoch"));
            assert!(!debug.contains("operation_epoch"));
        });
    }

    #[test]
    fn admitted_frontend_route_remaining_validity_uses_renewed_deadline_while_open() {
        let (cluster, admission) = crate::clock::with_time_override(1_000, || {
            let cluster = active_test_cluster(RouteMapValidity::until_ms(5_000).unwrap());
            cluster.test_store_route_map_validity(RouteMapValidity::until_ms(5_000).unwrap());
            let handle = StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&cluster));
            let admission = handle.admit_current_route().unwrap();
            (cluster, admission)
        });

        crate::clock::with_time_override(1_000, || {
            cluster.test_store_route_map_validity(RouteMapValidity::until_ms(10_000).unwrap());
        });
        crate::clock::with_time_override(2_000, || {
            assert_eq!(
                admission.remaining_validity().unwrap(),
                Some(Duration::from_secs(8))
            );
        });
        crate::clock::with_time_override(6_000, || {
            assert_eq!(
                admission.remaining_validity().unwrap(),
                Some(Duration::from_secs(4))
            );
        });
    }

    #[cfg(test)]
    #[test]
    fn active_bucket_route_uses_same_generation_renewal_while_open() {
        let cluster = crate::clock::with_time_override(1_000, || {
            let cluster = active_test_cluster(RouteMapValidity::until_ms(5_000).unwrap());
            cluster.test_store_route_map_validity(RouteMapValidity::until_ms(5_000).unwrap());
            cluster
        });
        let handle = StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&cluster));
        let admission =
            crate::clock::with_time_override(1_000, || handle.admit_current_route().unwrap());
        let bucket = BucketName::try_from("capability-bucket").unwrap();

        crate::clock::with_time_override(1_000, || {
            cluster.test_store_route_map_validity(RouteMapValidity::until_ms(10_000).unwrap());
        });
        crate::clock::with_time_override(6_000, || {
            cluster.require_route_map_valid_now().unwrap();
            admission.require_valid_now_raw().unwrap();
            admission.active_bucket_route(&bucket).unwrap();
        });
    }

    #[test]
    fn active_bucket_route_rejects_a_create_config_for_another_bucket() {
        crate::clock::with_time_override(1_000, || {
            let cluster = active_test_cluster(RouteMapValidity::until_ms(10_000).unwrap());
            cluster.test_store_route_map_validity(RouteMapValidity::until_ms(10_000).unwrap());
            let handle = StorageClusterRouteHandle::from_authorized_cluster(cluster);
            let admission = handle.admit_current_route().unwrap();
            let routed_bucket = BucketName::try_from("create-route-subject").unwrap();
            let other_bucket = BucketName::try_from("create-config-subject").unwrap();
            let route = admission.active_bucket_route(&routed_bucket).unwrap();
            let owner = CanonicalUserId::from_principal("owner");
            let acl_grants = AclGrants::default();

            let error = route
                .create_bucket_with_config_and_load_info(&crate::CreateBucketConfig {
                    name: other_bucket.as_str(),
                    owner_principal: "owner",
                    owner_canonical_id: &owner,
                    acl_grants: &acl_grants,
                    public_read: false,
                    public_write: false,
                    versioning: BucketVersioningState::Disabled,
                    object_lock: BucketObjectLockConfig::default(),
                    ownership_controls: BucketOwnershipControls {
                        object_ownership: crate::BucketObjectOwnership::ObjectWriter,
                    },
                })
                .unwrap_err();
            assert_eq!(
                error.kind(),
                &crate::BucketSnapshotLoadFailureKind::InternalError
            );
            assert_eq!(error.diagnostic_cause_label(), "store_topology_failure");
        });
    }

    #[test]
    fn admission_capture_serializes_with_same_generation_lease_replacement() {
        let (cluster, renewed, handle) = crate::clock::with_time_override(1_000, || {
            let cluster = active_test_cluster(RouteMapValidity::until_ms(5_000).unwrap());
            cluster.test_store_route_map_validity(RouteMapValidity::until_ms(5_000).unwrap());
            let renewed = active_test_cluster(RouteMapValidity::until_ms(10_000).unwrap());
            renewed.test_store_route_map_validity(RouteMapValidity::until_ms(10_000).unwrap());
            let handle = StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&cluster));
            (cluster, renewed, handle)
        });

        let admission = crate::clock::with_time_override(1_000, || {
            handle
                .admit_current_route_with_lease_capture_hook(|| {
                    assert!(
                        !cluster
                            .local_map
                            .test_try_replace_route_map_lease_from(&renewed.local_map),
                        "renewal must not acquire the lease write lock during capture"
                    );
                })
                .unwrap()
        });
        assert!(
            cluster
                .local_map
                .test_try_replace_route_map_lease_from(&renewed.local_map),
            "renewal must acquire the lease write lock after capture"
        );

        assert_eq!(
            admission.admitted_lease,
            LocalRouteMapLeaseSnapshot {
                validity: RouteMapValidity::until_ms(5_000).unwrap(),
                local_valid_until_monotonic_ms: Some(5_000),
            }
        );
        assert_eq!(
            cluster.local_map.route_map_lease_snapshot(),
            LocalRouteMapLeaseSnapshot {
                validity: RouteMapValidity::until_ms(10_000).unwrap(),
                local_valid_until_monotonic_ms: Some(10_000),
            }
        );
        crate::clock::with_time_override(6_000, || {
            cluster.require_route_map_valid_now().unwrap();
            admission.require_valid_now_raw().unwrap();
        });
    }

    #[test]
    fn pending_publication_freezes_admission_at_its_original_deadline() {
        let (cluster, handle, admission, candidate) =
            crate::clock::with_time_override(1_000, || {
                let cluster = active_test_cluster(RouteMapValidity::until_ms(5_000).unwrap());
                cluster.test_store_route_map_validity(RouteMapValidity::until_ms(5_000).unwrap());
                let handle =
                    StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&cluster));
                let admission = handle.admit_current_route().unwrap();
                let candidate = active_test_cluster(RouteMapValidity::until_ms(20_000).unwrap());
                candidate
                    .test_store_route_map_validity(RouteMapValidity::until_ms(20_000).unwrap());
                (cluster, handle, admission, candidate)
            });

        crate::clock::with_time_override(1_000, || {
            cluster.test_store_route_map_validity(RouteMapValidity::until_ms(10_000).unwrap());
        });
        let installer_handle = handle.clone();
        let installer = thread::spawn(move || {
            crate::clock::with_time_override(1_000, || {
                installer_handle.install(candidate).unwrap();
            });
        });
        handle.route_admission.wait_until_publication_is_pending();

        crate::clock::with_time_override(6_000, || {
            cluster.require_route_map_valid_now().unwrap();
            assert!(matches!(
                admission.require_valid_now_raw(),
                Err(StoreError::RouteMapExpired {
                    valid_until_ms: 5_000,
                    now_ms: 6_000,
                    ..
                })
            ));
        });

        drop(admission);
        installer.join().unwrap();
    }

    #[test]
    fn static_candidate_is_rejected_without_draining_admitted_requests() {
        let (handle, _admission) = crate::clock::with_time_override(1_000, || {
            let cluster = active_test_cluster(RouteMapValidity::until_ms(5_000).unwrap());
            cluster.test_store_route_map_validity(RouteMapValidity::until_ms(5_000).unwrap());
            let handle = StorageClusterRouteHandle::from_authorized_cluster(cluster);
            let admission = handle.admit_current_route().unwrap();
            (handle, admission)
        });
        let candidate = active_test_cluster(RouteMapValidity::Forever);

        assert!(matches!(
            handle.install(candidate),
            Err(StorageClusterRuntimeMapRefreshError::StaticRouteAuthorityRefresh)
        ));
    }

    #[test]
    fn expired_frontend_route_cannot_be_admitted() {
        crate::clock::with_time_override(5_000, || {
            let cluster = active_test_cluster(RouteMapValidity::until_ms(5_000).unwrap());
            let handle = StorageClusterRouteHandle::from_authorized_cluster(cluster);
            let error = handle
                .admit_current_route()
                .err()
                .expect("expired route must not be admitted");
            assert_eq!(
                error.class(),
                crate::StoreOperationFailureClass::RetryableConvergence
            );
            assert_eq!(error.diagnostic_cause_label(), "store_topology_failure");
        });
    }

    #[test]
    fn authoritative_pending_metadata_refresh_failure_predicate_is_narrow() {
        assert!(runtime_map_refresh_error_requires_current_map_invalidation(
            &StorageClusterRuntimeMapRefreshError::ControlPlane(
                ControlPlaneError::PgPeeringPendingMetadataCommand {
                    pg_id: 31,
                    node_id: 1,
                    cluster_epoch: ClusterEpoch::INITIAL,
                    pending: crate::control_plane::PendingMetadataCommandObservation::new(
                        ClusterEpoch::INITIAL,
                        std::num::NonZeroU64::MIN,
                        0,
                    ),
                },
            ),
        ));
        assert!(
            !runtime_map_refresh_error_requires_current_map_invalidation(
                &StorageClusterRuntimeMapRefreshError::ControlPlane(ControlPlaneError::rpc_remote(
                    "connect timeout".to_string()
                )),
            )
        );
        assert!(
            !runtime_map_refresh_error_requires_current_map_invalidation(
                &StorageClusterRuntimeMapRefreshError::ControlPlane(ControlPlaneError::io(
                    "connect control-plane RPC socket",
                    io::Error::new(io::ErrorKind::TimedOut, "timeout")
                )),
            )
        );
    }
}
