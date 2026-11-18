// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Bounded, read-only designated-witness inspection. Candidate selection is
//! owner-test-only, separate from live raw replica-state observation.

use super::*;
use crate::node_runtime::pg_store::CompleteSettledPrefixError;
use crate::storage_rpc::complete_settled_prefix as wire;

#[cfg(test)]
thread_local! {
    static LOSE_REPLY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
pub(super) fn test_lose_reply(response: &StorageRpcFrame) -> bool {
    if response.kind != StorageRpcMessageKind::MetadataCommandReplicaState
        || !LOSE_REPLY.with(|flag| flag.replace(false))
    {
        return false;
    }
    assert!(
        crate::storage_rpc::decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .is_ok()
    );
    true
}

impl StorageNodeConnectionHandler {
    pub(super) fn metadata_command_state_dispatch(
        &self,
        session: &StorageNodeSession,
        _route: &StorageNodeRouteAdmissionPermit,
        payload: &[u8],
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        #[cfg(test)]
        if self.complete_log_operations {
            if payload.starts_with(crate::storage_rpc::complete_stream_file_primary::MAGIC) {
                return self.complete_stream_file_inspection_response(session, _route, payload);
            }
            if payload.starts_with(crate::storage_rpc::complete_window_primary::DIRECT_FILE_MAGIC) {
                return self.complete_window_primary_response_for(
                    session,
                    _route,
                    payload,
                    crate::storage_rpc::complete_window_primary::Purpose::DirectFileBinding,
                );
            }
            if payload.starts_with(crate::storage_rpc::complete_window_primary::MAGIC) {
                return self.complete_window_primary_response(session, _route, payload);
            }
            return self.complete_settled_witness_response(session, _route, payload);
        }
        match decode_metadata_command_state_request(payload) {
            Ok(request) => self.metadata_command_replica_state_response(session, request),
            Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: error.to_string(),
            }),
        }
    }

    pub(super) fn complete_settled_witness_response(
        &self,
        session: &StorageNodeSession,
        route: &StorageNodeRouteAdmissionPermit,
        payload: &[u8],
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let request = match wire::decode_request(payload) {
            Ok(request) => request,
            Err(error) => {
                return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: error.to_string(),
                })
            }
        };
        let result = (|| -> Result<Vec<u8>, StorageRpcErrorResponse> {
            self.validate_active_admission(route, "settled witness inspection")?;
            self.validate_pg_route(request.node, request.epoch, request.pg)?;
            if request.topology != wire::topology_digest(self.node.pg_topology()) {
                return Err(StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: "settled witness topology differs from the admitted primary".into(),
                });
            }
            let placement = self
                .config
                .pg_routes
                .iter()
                .find(|r| r.pg_id == request.pg.get())
                .expect("validated PG");
            let witness = placement
                .acting_set
                .iter()
                .copied()
                .filter(|node| *node != placement.primary_node_id)
                .min();
            if witness != Some(self.config.node_id) {
                return Err(StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::StaleShardLocation,
                    message: "settled inspection requires the designated publication witness"
                        .into(),
                });
            }
            // Capture both cutoffs before waiting. Same-epoch renewal must not
            // extend this request's server-side admission or wire cutoff.
            let fence =
                StorageNodeRouteFence::current(&self.config, self.current_route_map_lease());
            let started = Instant::now();
            let mut deadline = request.deadline.local_deadline().min(
                started
                    .checked_add(storage_node_rpc_io_timeout(self.rpc_auth.as_deref()))
                    .unwrap_or(started),
            );
            if let Some(local) = fence.local_valid_until_monotonic_ms {
                deadline = deadline.min(
                    started
                        .checked_add(Duration::from_millis(
                            local.saturating_sub(crate::clock::monotonic_time_millis()),
                        ))
                        .unwrap_or(started),
                );
            }
            let require = || -> Result<(), StorageRpcErrorResponse> {
                fence.validate_rpc_at(
                    crate::clock::current_time_millis(),
                    crate::clock::monotonic_time_millis(),
                )?;
                if Instant::now() >= deadline {
                    return Err(store_error_response(
                        StoreError::OperationDeadlineExceeded {
                            context: "settled witness inspection",
                        },
                    ));
                }
                Ok(())
            };
            require()?;
            let _guard =
                self.metadata_command_pg_guard_until(session, request.pg, Some(deadline))?;
            require()?;
            let pg = self
                .node
                .get_pg_until(request.pg.get(), deadline)
                .map_err(store_error_response)?;
            require()?;
            let read = pg
                .inspect_complete_settled_prefix(
                    request.node.as_u32(),
                    request.epoch,
                    self.node.pg_topology(),
                )
                .map_err(prefix_error)?;
            let state = *read.prefix();
            read.finish().map_err(prefix_error)?;
            require()?;
            Ok(wire::encode_response(request, state))
        })();
        match result {
            Ok(bytes) => Ok(encode_storage_rpc_success_response(&bytes)),
            Err(error) => encode_storage_rpc_error_response(&error),
        }
    }
}

pub(super) fn prefix_error(error: CompleteSettledPrefixError) -> StorageRpcErrorResponse {
    match error {
        CompleteSettledPrefixError::Store(error) => store_error_response(error),
        CompleteSettledPrefixError::ValidationRequired => StorageRpcErrorResponse {
            code: StorageRpcErrorCode::MetadataCommandContention,
            message: "settled witness requires explicit local validation".into(),
        },
        error => StorageRpcErrorResponse {
            code: StorageRpcErrorCode::Internal,
            message: error.to_string(),
        },
    }
}

#[cfg(test)]
impl StorageNodeServer {
    pub(in crate::node_runtime) fn test_accept_one_without_settled_reply(
        &self,
    ) -> Result<(), StorageNodeServerError> {
        struct Reset(bool);
        impl Drop for Reset {
            fn drop(&mut self) {
                LOSE_REPLY.with(|flag| flag.set(self.0));
            }
        }
        let _reset = Reset(LOSE_REPLY.with(|flag| flag.replace(true)));
        self.accept_one()
    }

    pub(in crate::node_runtime) fn test_settled_witness_permits_and_live_dispatch(&self, pg: PgId) {
        let mut handler = self.connection_handler();
        let request = wire::Request {
            node: handler.config.node_id,
            epoch: handler.config.cluster_epoch,
            pg,
            topology: wire::topology_digest(handler.node.pg_topology()),
            deadline: crate::storage_rpc::StorageRpcOperationDeadline::from_instant(
                Instant::now() + Duration::from_secs(30),
            ),
        };
        let bytes = wire::encode_request(request);
        let session =
            StorageNodeSession::new(Arc::clone(&self.read_handles), Arc::clone(&self._node));
        let active = self
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::Active);
        let positive = handler
            .complete_settled_witness_response(&session, &active, &bytes)
            .unwrap();
        let positive = crate::storage_rpc::decode_storage_rpc_response_payload(&positive)
            .unwrap()
            .unwrap();
        wire::decode_response(&positive, request).unwrap();
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
            let reply = handler
                .complete_settled_witness_response(&session, &permit, &bytes)
                .unwrap();
            let error = crate::storage_rpc::decode_storage_rpc_response_payload(&reply)
                .unwrap()
                .unwrap_err();
            assert_eq!(error.code, StorageRpcErrorCode::Internal);
            assert!(error.message.contains(message), "{error:?}");
        }
        handler.complete_log_operations = false;
        let reply = handler
            .metadata_command_state_dispatch(&session, &active, &bytes)
            .unwrap();
        let error = crate::storage_rpc::decode_storage_rpc_response_payload(&reply)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
    }

    /// Hold the real receiver serialization until its captured route expires,
    /// renewing the raw map only after the RPC has captured the old authority.
    pub(in crate::node_runtime) fn test_settled_witness_route_wait<R: Send>(
        &self,
        pg: PgId,
        action: impl FnOnce() -> R + Send,
    ) -> R {
        let mut config = (*self.runtime_route_snapshot().config).clone();
        let held = self
            .metadata_command_locks
            .acquire(config.node_id, pg, None)
            .unwrap();
        config.route_map_validity =
            RouteMapValidity::until_ms_saturating(crate::clock::current_time_millis() + 1_400);
        self.install_control_plane_runtime_config(config.clone())
            .unwrap();
        let cutoff = self
            .runtime_route_snapshot()
            .route_map_lease
            .unwrap()
            .local_valid_until_monotonic_ms();
        let (tx, rx) = std::sync::mpsc::channel();
        self.metadata_command_locks
            .set_before_wait_hook(Arc::new(move |actual| {
                assert_eq!(actual, pg);
                let _ = tx.send(());
            }));
        std::thread::scope(|scope| {
            let request = scope.spawn(action);
            rx.recv_timeout(Duration::from_secs(5))
                .expect("receiver reached serialization wait under captured route");
            config.route_map_validity =
                RouteMapValidity::until_ms_saturating(crate::clock::current_time_millis() + 60_000);
            self.install_control_plane_runtime_config(config).unwrap();
            let remaining = cutoff.saturating_sub(crate::clock::monotonic_time_millis());
            std::thread::sleep(Duration::from_millis(remaining + 1));
            drop(held);
            let result = request.join().unwrap();
            assert!(self.connection_handler().current_route_map_lease_is_valid());
            result
        })
    }
}
