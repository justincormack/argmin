// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Owner-local receiver checks, before codec/transport selection. No socket
//! coverage is claimed by invoking this production admission/capture boundary.
use super::*;
use crate::storage_rpc::StorageRpcOperationDeadline;
type Expected = (crate::metadata_command::MetadataCommandId, Vec<u8>, Vec<u8>);
#[path = "complete_direct_introduction_parked_tests.rs"]
pub(super) mod parked;

fn query(
    server: &StorageNodeServer,
    expected: &Expected,
    bucket: &BucketName,
    key: &ObjectKey,
    attempt: &crate::SessionId,
    proof: &BucketWriteReservationProof,
) -> IntroductionQuery {
    IntroductionQuery::new(
        Route {
            node: server.connection_handler().config.node_id,
            epoch: expected.0.cluster_epoch(),
            pg: expected.0.pg_id(),
            deadline: StorageRpcOperationDeadline {
                portable_wall_valid_until_ms: crate::clock::current_time_millis() + 60_000,
            },
            topology: topology_digest(server._node.pg_topology()),
        },
        bucket.clone(),
        key.clone(),
        attempt.clone(),
        proof.clone(),
    )
}

fn held(handler: &StorageNodeConnectionHandler, route: Route) -> StorageNodeSession {
    let mut session =
        StorageNodeSession::new(Arc::clone(&handler.read_handles), Arc::clone(&handler.node));
    session
        .acquire_metadata_command_pg_lock(
            &handler.metadata_command_locks,
            handler.config.node_id,
            StorageNodeMetadataCommandLockBinding {
                pg_id: route.pg,
                cluster_epoch: route.epoch,
                authority: StorageNodeMetadataCommandLockAuthority::CurrentPrimary,
            },
            None,
        )
        .unwrap();
    session
}

impl StorageNodeServer {
    pub(in crate::node_runtime) fn test_direct_introduction_receiver_installation(
        &self,
        expected: &Expected,
        bucket: &BucketName,
        key: &ObjectKey,
        attempt: &crate::SessionId,
        proof: &BucketWriteReservationProof,
        work: (
            Duration,
            impl FnOnce(&mut crate::node_runtime::pg_store::RemoteIntroductionInstallation<'_>),
        ),
    ) {
        let query = query(self, expected, bucket, key, attempt, proof);
        let mut handler = self.connection_handler();
        let auth = handler.rpc_auth.as_ref().unwrap();
        let limits = auth.transport_limits();
        handler.rpc_auth = Some(Arc::new(
            (**auth).clone().with_transport_limits(
                crate::storage_rpc_auth::StorageRpcTransportLimits::new(
                    limits.max_frame_bytes(),
                    limits.max_connections(),
                    work.0,
                )
                .unwrap(),
            ),
        ));
        let session = held(&handler, query.route);
        let active = self
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::Active);
        let response = handler
            .prepare_direct_introduction(&session, &active, &query, |_| {})
            .unwrap()
            .encode_response()
            .unwrap();
        // These failures must not consume the still-usable preparation.
        for permit in [
            self.route_admission
                .acquire(StorageNodeRouteAdmissionClass::RetainedCleanup),
            StorageNodeRouteAdmissionGate::default()
                .acquire(StorageNodeRouteAdmissionClass::Active),
        ] {
            assert!(handler
                .claim_direct_introduction(&session, &permit, &query, &response)
                .is_err());
        }
        let mut changed = response.clone();
        *changed.last_mut().unwrap() ^= 1;
        assert!(handler
            .claim_direct_introduction(&session, &active, &query, &changed)
            .is_err());
        let stale = handler
            .prepare_direct_introduction(&session, &active, &query, |_| {})
            .unwrap();
        let mut owner = handler
            .claim_direct_introduction(&session, &active, &query, &response)
            .unwrap();
        assert_eq!(owner.id(), expected.0);
        assert!(handler
            .claim_direct_introduction(&session, &active, &query, &response)
            .is_err());
        assert!(handler
            .prepare_direct_introduction(&session, &active, &query, |_| panic!(
                "claimed preparation reached store"
            ))
            .is_err());
        assert!(
            stale.encode_response().is_err(),
            "outstanding capture reset claimed history"
        );
        work.1(&mut owner);
        drop(owner);
        assert!(
            handler
                .claim_direct_introduction(&session, &active, &query, &response)
                .is_err(),
            "Drop reset claim"
        );
        assert!(handler
            .prepare_direct_introduction(&session, &active, &query, |_| panic!(
                "Drop reset capture"
            ))
            .is_err());
        assert!(session.holds_metadata_command_pg_lock(query.route.pg));
        trait NotClone<A> {
            fn check() {}
        }
        impl<T: ?Sized> NotClone<()> for T {}
        impl<T: Clone> NotClone<u8> for T {}
        let _ = <crate::node_runtime::pg_store::RemoteIntroductionInstallation<'_> as NotClone<
            _,
        >>::check;
        let _ = <RemoteIntroductionInstallationInput<'_> as NotClone<_>>::check;
    }

    pub(in crate::node_runtime) fn test_direct_introduction_receiver_response(
        &self,
        expected: &Expected,
        bucket: &BucketName,
        key: &ObjectKey,
        attempt: &crate::SessionId,
        proof: &BucketWriteReservationProof,
    ) {
        let input = query(self, expected, bucket, key, attempt, proof);
        let encoded = wire::encode_request(&wire::Request {
            route: input.route,
            bucket: input.bucket,
            key: input.key,
            attempt: input.attempt,
            proof: input.proof,
        })
        .unwrap();
        let query = IntroductionQuery::decode(encoded.bytes()).unwrap();
        let handler = self.connection_handler();
        let session = held(&handler, query.route);
        let active = self
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::Active);
        assert!(session.metadata_command_guards[&query.route.pg]
            .introduction_preparation
            .get()
            .prepared()
            .is_none());
        let prepared = handler
            .prepare_direct_introduction(&session, &active, &query, |_| {})
            .unwrap();
        let other = handler
            .prepare_direct_introduction(&session, &active, &query, |_| {})
            .unwrap();
        assert!(
            other
                .captured
                .encode_response(&DirectIntroductionResponseScope(&prepared))
                .is_err(),
            "response scope accepted a different capture"
        );
        drop(other);
        let response = prepared.encode_response().unwrap();
        let decoded = encoded.decode_response(&response).unwrap();
        assert_eq!(decoded.command.command, expected.2);
        assert_eq!(
            decoded.command.checksum,
            checksum::crc64::checksum(&expected.2)
        );
        assert!(
            decoded.cutoff.portable_wall_valid_until_ms
                < query.route.deadline.portable_wall_valid_until_ms
        );
        handler
            .require_prepared_introduction_response(&session, &query, &response)
            .unwrap();
        // Fully resealed descriptive responses must not pass receiver minting.
        for field in 0..3 {
            let decoded = encoded.decode_response(&response).unwrap();
            let mut command = decoded.command.command.to_vec();
            let mut cutoff = decoded.cutoff;
            let mut crc = decoded.command.checksum;
            match field {
                0 => {
                    *command.last_mut().unwrap() ^= 1;
                    crc = checksum::crc64::checksum(&command);
                }
                1 => cutoff.portable_wall_valid_until_ms -= 1,
                2 => crc ^= 1,
                _ => unreachable!(),
            }
            let resealed = wire::encode_response(
                &encoded,
                crate::storage_rpc::complete_applied::BorrowedRequest {
                    command: &command,
                    checksum: crc,
                    ..decoded.command
                },
                cutoff,
            )
            .unwrap();
            encoded.decode_response(&resealed).unwrap();
            let error = handler
                .require_prepared_introduction_response(&session, &query, &resealed)
                .unwrap_err();
            assert!(
                error.to_string().contains("prepared response identity"),
                "{error}"
            );
        }
        let mut crossed = wire::decode_request(encoded.bytes()).unwrap();
        crossed.proof.reservation_id.push('x');
        let crossed = wire::encode_request(&crossed).unwrap();
        let decoded = encoded.decode_response(&response).unwrap();
        let resealed = wire::encode_response(&crossed, decoded.command, decoded.cutoff).unwrap();
        let crossed_query = IntroductionQuery::decode(crossed.bytes()).unwrap();
        assert!(handler
            .require_prepared_introduction_response(&session, &crossed_query, &resealed)
            .is_err());

        // Expired re-preparation cannot replace a previous receipt, and receipt
        // inspection still enforces its original deadline, not a renewed query.
        let identity = session.metadata_command_guards[&query.route.pg]
            .introduction_preparation
            .get();
        let expired = handler
            .prepare_direct_introduction(&session, &active, &query, |_| {})
            .unwrap();
        let clock =
            crate::clock::test_time_override_guard(decoded.cutoff.portable_wall_valid_until_ms);
        query.require_valid().unwrap();
        let error = handler
            .require_prepared_introduction_response(&session, &query, &response)
            .unwrap_err();
        assert!(
            error.to_string().contains("prepared response cutoff"),
            "{error}"
        );
        clock.set(query.route.deadline.portable_wall_valid_until_ms - 1);
        query.require_valid().unwrap();
        assert!(expired.encode_response().is_err());
        assert!(
            session.metadata_command_guards[&query.route.pg]
                .introduction_preparation
                .get()
                == identity
        );
        drop(clock);
        handler
            .require_prepared_introduction_response(&session, &query, &response)
            .unwrap();
        // The exported absolute cutoff is not a renewal of the shorter local
        // I/O bound. Keep the wall clock unchanged and expire only that Instant.
        std::thread::sleep(
            identity
                .prepared()
                .unwrap()
                .local_deadline
                .saturating_duration_since(Instant::now())
                + Duration::from_millis(1),
        );
        query.require_valid().unwrap();
        assert!(crate::clock::current_time_millis() < decoded.cutoff.portable_wall_valid_until_ms);
        let error = handler
            .require_prepared_introduction_response(&session, &query, &response)
            .unwrap_err();
        assert!(
            error.to_string().contains("prepared response cutoff"),
            "{error}"
        );
        drop(session);
        let reopened = held(&handler, query.route);
        let error = handler
            .require_prepared_introduction_response(&reopened, &query, &response)
            .unwrap_err();
        assert!(
            error.to_string().contains("prepared response identity"),
            "{error}"
        );
        assert!(reopened.metadata_command_guards[&query.route.pg]
            .introduction_preparation
            .get()
            .prepared()
            .is_none());
    }

    pub(in crate::node_runtime) fn test_direct_introduction_receiver_success(
        &self,
        expected: &Expected,
        bucket: &BucketName,
        key: &ObjectKey,
        attempt: &crate::SessionId,
        proof: &BucketWriteReservationProof,
    ) {
        let query = query(self, expected, bucket, key, attempt, proof);
        let handler = self.connection_handler();
        let session = held(&handler, query.route);
        let active = self
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::Active);
        let mut points = Vec::new();
        let prepared = handler
            .prepare_direct_introduction(&session, &active, &query, |point| points.push(point))
            .unwrap();
        assert_eq!(points, ["before store", "before capture", "after snapshot"]);
        assert_eq!(prepared.captured.id(), expected.0);
        prepared.captured.assert_context_for_test(&expected.1);
        prepared.require_valid().unwrap();
        // A fresh read has no effects to confirm. Dropping it must not install,
        // relinquish a reservation, or free the still-borrowed primary section.
        drop(prepared);
        assert!(session.holds_metadata_command_pg_lock(query.route.pg));
        let again = handler
            .prepare_direct_introduction(&session, &active, &query, |_| {})
            .unwrap();
        again.captured.assert_context_for_test(&expected.1);

        trait NotClone<A> {
            fn check() {}
        }
        impl<T: ?Sized> NotClone<()> for T {}
        impl<T: Clone> NotClone<u8> for T {}
        let _ = <RemoteDirectIntroductionAuthority<'_> as NotClone<_>>::check;
        let _ = <PreparedIntroduction<'_> as NotClone<_>>::check;
        let _ = <CapturedDirectIntroductionCore as NotClone<_>>::check;
    }

    pub(in crate::node_runtime) fn test_direct_introduction_receiver_rejections(
        &self,
        expected: &Expected,
        bucket: &BucketName,
        key: &ObjectKey,
        attempt: &crate::SessionId,
        proof: &BucketWriteReservationProof,
    ) {
        let mut query = query(self, expected, bucket, key, attempt, proof);
        let handler = self.connection_handler();
        let session = held(&handler, query.route);
        let active = self
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::Active);
        for (permit, message) in [
            (
                self.route_admission
                    .acquire(StorageNodeRouteAdmissionClass::RetainedCleanup),
                "requires active route admission",
            ),
            (
                StorageNodeRouteAdmissionGate::default()
                    .acquire(StorageNodeRouteAdmissionClass::Active),
                "different admission domain",
            ),
        ] {
            let error = handler
                .prepare_direct_introduction(&session, &permit, &query, |_| {
                    panic!("crossed admission reached storage")
                })
                .err()
                .unwrap();
            assert!(error.message.contains(message), "{error:?}");
        }
        let empty =
            StorageNodeSession::new(Arc::clone(&self.read_handles), Arc::clone(&self._node));
        let error = handler
            .prepare_direct_introduction(&empty, &active, &query, |_| {
                panic!("unheld primary reached storage")
            })
            .err()
            .unwrap();
        assert_eq!(error.code, StorageRpcErrorCode::MetadataCommandContention);
        // Matching binding scalars and the same node cannot stand in for the
        // handler's actual serialization domain.
        let mut crossed_handler = handler.clone();
        crossed_handler.metadata_command_locks = StorageNodeMetadataCommandLocks::default();
        let crossed_session = held(&crossed_handler, query.route);
        let error = handler
            .prepare_direct_introduction(&crossed_session, &active, &query, |_| {
                panic!("foreign lock reached storage")
            })
            .err()
            .unwrap();
        assert_eq!(error.code, StorageRpcErrorCode::MetadataCommandContention);

        let route = query.route;
        for changed in [
            Route {
                node: NodeId::new(0),
                ..route
            },
            Route {
                epoch: ClusterEpoch::new(route.epoch.get() + 1).unwrap(),
                ..route
            },
            Route {
                topology: [0; 32],
                ..route
            },
        ] {
            query.route = changed;
            handler
                .prepare_direct_introduction(&session, &active, &query, |_| {
                    panic!("crossed route reached storage")
                })
                .err()
                .unwrap();
        }
        query.route = route;
        for peering in [false, true] {
            let mut other_route = handler.clone();
            let configured = Arc::make_mut(&mut other_route.config)
                .pg_routes
                .iter_mut()
                .find(|configured| configured.pg_id == route.pg.get())
                .unwrap();
            if peering {
                configured.state = PgState::Peering;
            } else {
                configured.primary_node_id = NodeId::new(0);
            }
            other_route
                .prepare_direct_introduction(&session, &active, &query, |_| {
                    panic!("non-Active-primary reached storage")
                })
                .err()
                .unwrap();
        }
        // Same bucket and epoch, different operation: shared capture must reject
        // the proof before manufacturing any introduction context.
        query.proof.operation_kind = "put-object-stream-create".into();
        let error = handler
            .prepare_direct_introduction(&session, &active, &query, |_| {})
            .err()
            .unwrap();
        assert!(error.message.contains("scoped subject/proof"), "{error:?}");
    }

    pub(in crate::node_runtime) fn test_direct_introduction_receiver_expiry(
        &self,
        expected: &Expected,
        bucket: &BucketName,
        key: &ObjectKey,
        attempt: &crate::SessionId,
        proof: &BucketWriteReservationProof,
    ) {
        for server_first in [false, true] {
            for point in ["before store", "before capture", "after snapshot"] {
                let wall = crate::clock::current_time_millis();
                let monotonic = crate::clock::monotonic_time_millis();
                let mut handler = self.connection_handler();
                if server_first {
                    Arc::make_mut(&mut handler.config).route_map_validity = RouteMapValidity::Until(
                        crate::RouteMapValidUntilMs::new(wall + 5_000).unwrap(),
                    );
                    handler.route_map_lease =
                        bind_storage_node_route_map_lease(handler.config.route_map_validity)
                            .unwrap();
                }
                let query = query(self, expected, bucket, key, attempt, proof);
                let session = held(&handler, query.route);
                let active = self
                    .route_admission
                    .acquire(StorageNodeRouteAdmissionClass::Active);
                let mut clock = None;
                let error = handler
                    .prepare_direct_introduction(&session, &active, &query, |at| {
                        if at == point {
                            clock = Some(if server_first {
                                crate::clock::test_time_and_monotonic_override_guard(
                                    wall,
                                    handler
                                        .route_map_lease
                                        .unwrap()
                                        .local_valid_until_monotonic_ms(),
                                )
                            } else {
                                crate::clock::test_time_and_monotonic_override_guard(
                                    query.route.deadline.portable_wall_valid_until_ms,
                                    monotonic + 60_000,
                                )
                            });
                        }
                    })
                    .err()
                    .expect("expired receiver returned introduction");
                assert!(clock.is_some(), "must reach {point}");
                assert!(
                    error.message.contains("expired")
                        || error.message.contains("deadline")
                        || error.message.contains("cutoff"),
                    "{error:?}"
                );
                drop(clock);
            }
        }
    }
}
