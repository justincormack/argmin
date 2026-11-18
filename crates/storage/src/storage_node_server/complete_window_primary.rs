// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Current-primary exact-window inspection on an already held connection-local
//! metadata section. The reply is an observation, never a physical grant.

use super::*;
use crate::node_runtime::pg_store::{
    StagingWindowConfirmationError, StagingWindowInspectionDescription,
};
use crate::storage_rpc::{complete_settled_prefix as witness, complete_window_primary as wire};

impl StorageNodeConnectionHandler {
    pub(super) fn complete_window_primary_response(
        &self,
        session: &StorageNodeSession,
        admission: &StorageNodeRouteAdmissionPermit,
        bytes: &[u8],
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        self.complete_window_primary_response_for(
            session,
            admission,
            bytes,
            wire::Purpose::RetainedWindow,
        )
    }

    pub(super) fn complete_window_primary_response_for(
        &self,
        session: &StorageNodeSession,
        admission: &StorageNodeRouteAdmissionPermit,
        bytes: &[u8],
        purpose: wire::Purpose,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let request = match wire::decode_request_for(purpose, bytes) {
            Ok(request) => request,
            Err(error) => {
                return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: error.to_string(),
                })
            }
        };
        let result = (|| -> Result<Vec<u8>, StorageRpcErrorResponse> {
            let route = request.route;
            self.validate_active_admission(admission, "primary window inspection")?;
            self.validate_pg_route(route.node, route.epoch, route.pg)?;
            self.validate_primary_pg(route.pg, "primary window inspection")?;
            if route.topology != witness::topology_digest(self.node.pg_topology()) {
                return Err(StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: "primary window inspection topology mismatch".into(),
                });
            }
            let binding = StorageNodeMetadataCommandLockBinding {
                pg_id: route.pg,
                cluster_epoch: route.epoch,
                authority: StorageNodeMetadataCommandLockAuthority::CurrentPrimary,
            };
            if !session.holds_metadata_command_pg_lock_with_binding(binding) {
                return Err(StorageRpcErrorResponse { code: StorageRpcErrorCode::MetadataCommandContention, message: "primary window inspection requires its already-held current-primary section".into() });
            }
            // Capture once, before descriptor decoding/PG access. An intervening
            // same-epoch renewal cannot lengthen this observation's authority.
            let fence =
                StorageNodeRouteFence::current(&self.config, self.current_route_map_lease());
            let started = Instant::now();
            let mut deadline = route.deadline.local_deadline().min(
                started
                    .checked_add(storage_node_rpc_io_timeout(self.rpc_auth.as_deref()))
                    .unwrap_or(started),
            );
            if let Some(cutoff) = fence.local_valid_until_monotonic_ms {
                deadline = deadline.min(
                    started
                        .checked_add(Duration::from_millis(
                            cutoff.saturating_sub(crate::clock::monotonic_time_millis()),
                        ))
                        .unwrap_or(started),
                );
            }
            let description = StagingWindowInspectionDescription::decode(request.descriptor)
                .map_err(|error| StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: error.to_string(),
                })?;
            let window = description
                .request(self.node.pg_topology())
                .map_err(inspection_error)?;
            let (deadline, window_fence) = window
                .bind_deadline(route.pg, self.node.pg_topology(), route.epoch, deadline)
                .map_err(store_error_response)?;
            let require = || -> Result<(), StorageRpcErrorResponse> {
                fence.validate_rpc_at(
                    crate::clock::current_time_millis(),
                    crate::clock::monotonic_time_millis(),
                )?;
                window_fence
                    .require_valid_for(route.epoch)
                    .map_err(store_error_response)?;
                if Instant::now() >= deadline {
                    return Err(store_error_response(
                        StoreError::OperationDeadlineExceeded {
                            context: "primary window inspection",
                        },
                    ));
                }
                Ok(())
            };
            require()?;
            let pg = self
                .node
                .get_pg_until(route.pg.get(), deadline)
                .map_err(store_error_response)?;
            require()?;
            let read = window
                .inspect_primary(&pg, route.node, route.epoch)
                .map_err(inspection_error)?;
            if matches!(purpose, wire::Purpose::DirectFileBinding) {
                // Same PG lock and SQLite read snapshot as prefix/window
                // inspection. No allocation, SQL write, or physical grant.
                window
                    .require_direct_file_reservation(&pg, route.node)
                    .map_err(inspection_error)?;
            }
            let prefix = *read.prefix();
            read.finish()
                .map_err(super::complete_settled_prefix::prefix_error)?;
            require()?;
            let placement = self
                .config
                .pg_routes
                .iter()
                .find(|entry| entry.pg_id == route.pg.get())
                .expect("validated route");
            let peer = placement
                .acting_set
                .iter()
                .copied()
                .filter(|node| *node != placement.primary_node_id)
                .min();
            Ok(wire::encode_response_for(purpose, request, prefix, peer))
        })();
        match result {
            Ok(bytes) => Ok(encode_storage_rpc_success_response(&bytes)),
            Err(error) => encode_storage_rpc_error_response(&error),
        }
    }
}

fn inspection_error(error: StagingWindowConfirmationError) -> StorageRpcErrorResponse {
    match error {
        StagingWindowConfirmationError::Store(error) => store_error_response(error),
        StagingWindowConfirmationError::Prefix(error) => {
            super::complete_settled_prefix::prefix_error(error)
        }
        error => StorageRpcErrorResponse {
            code: StorageRpcErrorCode::MetadataCommandContention,
            message: error.to_string(),
        },
    }
}

#[cfg(test)]
impl StorageNodeServer {
    pub(in crate::node_runtime) fn test_direct_file_primary_admission(&self, bytes: &[u8]) {
        let mut handler = self.connection_handler();
        let session =
            StorageNodeSession::new(Arc::clone(&self.read_handles), Arc::clone(&self._node));
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
            let response = handler
                .complete_window_primary_response_for(
                    &session,
                    &permit,
                    bytes,
                    wire::Purpose::DirectFileBinding,
                )
                .unwrap();
            let error = crate::storage_rpc::decode_storage_rpc_response_payload(&response)
                .unwrap()
                .unwrap_err();
            assert_eq!(error.code, StorageRpcErrorCode::Internal);
            assert!(error.message.contains(message), "{error:?}");
        }
        handler.complete_log_operations = false;
        let active = self
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::Active);
        let response = handler
            .metadata_command_state_dispatch(&session, &active, bytes)
            .unwrap();
        assert_eq!(
            crate::storage_rpc::decode_storage_rpc_response_payload(&response)
                .unwrap()
                .unwrap_err()
                .code,
            StorageRpcErrorCode::PayloadDecode
        );
    }

    pub(in crate::node_runtime) fn test_window_primary_route_expiry(&self, bytes: &[u8]) {
        let request = wire::decode_request(bytes).unwrap();
        let mut config = (*self.runtime_route_snapshot().config).clone();
        config.route_map_validity =
            RouteMapValidity::until_ms_saturating(crate::clock::current_time_millis() + 1_400);
        self.install_control_plane_runtime_config(config.clone())
            .unwrap();
        let handler = self.connection_handler();
        let active = self
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::Active);
        let mut session =
            StorageNodeSession::new(Arc::clone(&self.read_handles), Arc::clone(&self._node));
        session
            .acquire_metadata_command_pg_lock(
                &self.metadata_command_locks,
                request.route.node,
                StorageNodeMetadataCommandLockBinding {
                    pg_id: request.route.pg,
                    cluster_epoch: request.route.epoch,
                    authority: StorageNodeMetadataCommandLockAuthority::CurrentPrimary,
                },
                None,
            )
            .unwrap();
        let held = self._node.get_pg(request.route.pg.get()).unwrap();
        // The frame handler has captured the old route. A raw same-epoch
        // renewal cannot lengthen that handler's PG-lock wait.
        config.route_map_validity =
            RouteMapValidity::until_ms_saturating(crate::clock::current_time_millis() + 60_000);
        self.install_control_plane_runtime_config(config).unwrap();
        let started = Instant::now();
        let reply = handler
            .complete_window_primary_response(&session, &active, bytes)
            .unwrap();
        let error = crate::storage_rpc::decode_storage_rpc_response_payload(&reply)
            .unwrap()
            .unwrap_err();
        assert_eq!(
            error.code,
            StorageRpcErrorCode::TransportTimeout,
            "{error:?}"
        );
        assert!(started.elapsed() < Duration::from_secs(2));
        drop(held);
        assert!(self.connection_handler().current_route_map_lease_is_valid());
    }

    pub(in crate::node_runtime) fn test_require_window_primary_held(&self, pg: PgId) {
        assert!(self
            .metadata_command_locks
            .state
            .table
            .lock()
            .unwrap()
            .held
            .contains_key(&pg));
    }

    pub(in crate::node_runtime) fn test_window_primary_admission(&self, bytes: &[u8]) {
        let request = wire::decode_request(bytes).unwrap();
        let mut handler = self.connection_handler();
        let mut session =
            StorageNodeSession::new(Arc::clone(&self.read_handles), Arc::clone(&self._node));
        let active = self
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::Active);
        let decode_error = |bytes: Vec<u8>| {
            crate::storage_rpc::decode_storage_rpc_response_payload(&bytes)
                .unwrap()
                .unwrap_err()
        };
        let reply = handler
            .complete_window_primary_response(&session, &active, bytes)
            .unwrap();
        assert_eq!(
            decode_error(reply).code,
            StorageRpcErrorCode::MetadataCommandContention
        );
        for authority in [
            StorageNodeMetadataCommandLockAuthority::HistoricalRecoveryPrimary,
            StorageNodeMetadataCommandLockAuthority::CurrentPrimary,
        ] {
            session
                .acquire_metadata_command_pg_lock(
                    &self.metadata_command_locks,
                    request.route.node,
                    StorageNodeMetadataCommandLockBinding {
                        pg_id: request.route.pg,
                        cluster_epoch: request.route.epoch,
                        authority,
                    },
                    None,
                )
                .unwrap();
            let reply = handler
                .complete_window_primary_response(&session, &active, bytes)
                .unwrap();
            if authority == StorageNodeMetadataCommandLockAuthority::HistoricalRecoveryPrimary {
                assert_eq!(
                    decode_error(reply).code,
                    StorageRpcErrorCode::MetadataCommandContention
                );
            } else {
                crate::storage_rpc::decode_storage_rpc_response_payload(&reply)
                    .unwrap()
                    .unwrap();
            }
            session.release_metadata_command_pg_lock(request.route.pg);
        }
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
            let error = decode_error(
                handler
                    .complete_window_primary_response(&session, &permit, bytes)
                    .unwrap(),
            );
            assert_eq!(error.code, StorageRpcErrorCode::Internal);
            assert!(error.message.contains(message), "{error:?}");
        }
        handler.complete_log_operations = false;
        assert_eq!(
            decode_error(
                handler
                    .metadata_command_state_dispatch(&session, &active, bytes)
                    .unwrap()
            )
            .code,
            StorageRpcErrorCode::PayloadDecode
        );
    }
}
