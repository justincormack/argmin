// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Current-primary allocation and read-only file-binding inspection on the
//! already held metadata section. Admission and placement are captured here,
//! never reconstructed by a storage caller.
use super::*;
use crate::node_runtime::payload_placement::PayloadPlacementInput;
use crate::node_runtime::pg_store::RemoteStreamAllocationQuery;
use crate::node_runtime::pg_store::{PgStore, StagingWindowConfirmationError};
use crate::storage_rpc::complete_stream_file_primary as files;

#[cfg(test)]
thread_local! {
    static EXPIRE_AT_UPDATE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static LOSE_REPLY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
pub(super) fn test_lose_reply(response: &StorageRpcFrame) -> bool {
    if response.kind != StorageRpcMessageKind::ObjectStreamSegmentAppendPrepare
        || !LOSE_REPLY.with(|slot| slot.replace(false))
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

#[cfg(test)]
impl StorageNodeServer {
    pub(in crate::node_runtime) fn test_stream_allocation_route_expiry(&self, bytes: &[u8]) {
        let file_request = bytes
            .starts_with(files::MAGIC)
            .then(|| files::decode_request(bytes).unwrap());
        let request = crate::storage_rpc::complete_stream_allocation::decode_request(
            file_request
                .as_ref()
                .map_or(bytes, |request| request.allocation),
        )
        .unwrap();
        let route = request.window.route;
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
                route.node,
                StorageNodeMetadataCommandLockBinding {
                    pg_id: route.pg,
                    cluster_epoch: route.epoch,
                    authority: StorageNodeMetadataCommandLockAuthority::CurrentPrimary,
                },
                None,
            )
            .unwrap();
        let held = self._node.get_pg(route.pg.get()).unwrap();
        config.route_map_validity =
            RouteMapValidity::until_ms_saturating(crate::clock::current_time_millis() + 60_000);
        self.install_control_plane_runtime_config(config).unwrap();
        let started = Instant::now();
        let reply = handler
            .complete_stream_allocation_response(&session, &active, bytes)
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
    pub(in crate::node_runtime) fn test_stream_allocation_admission(&self, bytes: &[u8]) {
        let mut handler = self.connection_handler();
        let session =
            StorageNodeSession::new(Arc::clone(&self.read_handles), Arc::clone(&self._node));
        for (permit, expected, message) in [
            (
                self.route_admission
                    .acquire(StorageNodeRouteAdmissionClass::Active),
                StorageRpcErrorCode::MetadataCommandContention,
                "held current-primary section",
            ),
            (
                self.route_admission
                    .acquire(StorageNodeRouteAdmissionClass::RetainedCleanup),
                StorageRpcErrorCode::Internal,
                "requires active route admission",
            ),
            (
                StorageNodeRouteAdmissionGate::default()
                    .acquire(StorageNodeRouteAdmissionClass::Active),
                StorageRpcErrorCode::Internal,
                "different admission domain",
            ),
        ] {
            let reply = handler
                .complete_stream_allocation_response(&session, &permit, bytes)
                .unwrap();
            let error = crate::storage_rpc::decode_storage_rpc_response_payload(&reply)
                .unwrap()
                .unwrap_err();
            assert_eq!(error.code, expected);
            assert!(error.message.contains(message), "{error:?}");
        }
        handler.complete_log_operations = false;
        let permit = self
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::Active);
        let reply = handler
            .stream_allocation_dispatch(&session, &permit, bytes)
            .unwrap();
        let error = crate::storage_rpc::decode_storage_rpc_response_payload(&reply)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::PayloadDecode);
    }
    pub(in crate::node_runtime) fn test_lose_stream_allocation_reply() {
        LOSE_REPLY.with(|slot| slot.set(true));
    }
    pub(in crate::node_runtime) fn test_expire_stream_allocation_at_update() {
        EXPIRE_AT_UPDATE.with(|slot| slot.set(true));
    }
}

pub(in crate::node_runtime) struct RemoteStreamAllocationAuthority<'a> {
    handler: &'a StorageNodeConnectionHandler,
    session: &'a StorageNodeSession,
    _admission: &'a StorageNodeRouteAdmissionPermit,
    store: &'a PgStore,
    query: &'a RemoteStreamAllocationQuery,
    fence: StorageNodeRouteFence,
    deadline: Instant,
    binding: StorageNodeMetadataCommandLockBinding,
}

impl RemoteStreamAllocationAuthority<'_> {
    pub(in crate::node_runtime) fn query(&self) -> &RemoteStreamAllocationQuery {
        self.query
    }
    pub(in crate::node_runtime) fn placement(&self) -> PayloadPlacementInput<'_> {
        PayloadPlacementInput::from_server_config(&self.handler.config)
    }
    pub(in crate::node_runtime) fn require_effect(&self, pg: &PgStore) -> Result<(), StoreError> {
        let route = self.query.route();
        if !std::ptr::eq(pg, self.store)
            || pg.pg_id() != route.pg.get()
            || self.query.topology() != self.handler.node.pg_topology()
            || !self
                .session
                .holds_metadata_command_pg_lock_with_binding(self.binding)
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "remote allocation exact store/section",
            });
        }
        self.fence.validate_store_at(
            crate::clock::current_time_millis(),
            crate::clock::monotonic_time_millis(),
        )?;
        self.query.require_valid()?;
        if Instant::now() >= self.deadline {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "remote allocation captured route/work cutoff",
            });
        }
        Ok(())
    }
}

impl StorageNodeConnectionHandler {
    pub(super) fn complete_stream_file_inspection_response(
        &self,
        session: &StorageNodeSession,
        admission: &StorageNodeRouteAdmissionPermit,
        bytes: &[u8],
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        // The storage-peer read kind must never reach the allocation arm.
        if let Err(error) = files::decode_request(bytes) {
            return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: error.to_string(),
            });
        }
        self.complete_stream_allocation_response(session, admission, bytes)
    }

    pub(super) fn stream_allocation_dispatch(
        &self,
        session: &StorageNodeSession,
        route: &StorageNodeRouteAdmissionPermit,
        bytes: &[u8],
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        #[cfg(test)]
        if self.complete_log_operations {
            return self.complete_stream_allocation_response(session, route, bytes);
        }
        let _ = session;
        match decode_stream_segment_append_prepare_request(bytes) {
            Ok(request) => self.stream_segment_append_prepare_response(route, request),
            Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: error.to_string(),
            }),
        }
    }

    fn complete_stream_allocation_response(
        &self,
        session: &StorageNodeSession,
        admission: &StorageNodeRouteAdmissionPermit,
        bytes: &[u8],
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        // Capture before decoding/PG admission. Same-epoch lease renewal cannot
        // lengthen an already received allocation attempt.
        let fence = StorageNodeRouteFence::current(&self.config, self.current_route_map_lease());
        let decoded: Result<_, crate::storage_rpc::StorageRpcPayloadError> = (|| {
            if bytes.starts_with(files::MAGIC) {
                let request = files::decode_request(bytes)?;
                Ok((
                    RemoteStreamAllocationQuery::decode(
                        request.allocation,
                        self.node.pg_topology(),
                    )?,
                    Some(request.segment),
                ))
            } else {
                Ok((
                    RemoteStreamAllocationQuery::decode(bytes, self.node.pg_topology())?,
                    None,
                ))
            }
        })();
        let (query, file_segment) = match decoded {
            Ok(query) => query,
            Err(error) => {
                return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: error.to_string(),
                })
            }
        };
        let action = || -> Result<Vec<u8>, StorageRpcErrorResponse> {
            let route = query.route();
            self.validate_active_admission(admission, "confirmed stream allocation")?;
            self.validate_pg_route(route.node, route.epoch, route.pg)?;
            self.validate_primary_pg(route.pg, "confirmed stream allocation")?;
            let binding = StorageNodeMetadataCommandLockBinding {
                pg_id: route.pg,
                cluster_epoch: route.epoch,
                authority: StorageNodeMetadataCommandLockAuthority::CurrentPrimary,
            };
            if !session.holds_metadata_command_pg_lock_with_binding(binding) {
                return Err(StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::MetadataCommandContention,
                    message: "stream allocation requires its held current-primary section".into(),
                });
            }
            let now = Instant::now();
            let mut deadline = query.deadline().min(
                now.checked_add(storage_node_rpc_io_timeout(self.rpc_auth.as_deref()))
                    .unwrap_or(now),
            );
            if let Some(cutoff) = fence.local_valid_until_monotonic_ms {
                deadline = deadline.min(
                    now.checked_add(Duration::from_millis(
                        cutoff.saturating_sub(crate::clock::monotonic_time_millis()),
                    ))
                    .unwrap_or(now),
                );
            }
            let pg = self
                .node
                .get_pg_until(route.pg.get(), deadline)
                .map_err(store_error_response)?;
            let authority = RemoteStreamAllocationAuthority {
                handler: self,
                session,
                _admission: admission,
                store: &pg,
                query: &query,
                fence,
                deadline,
                binding,
            };
            let inspection_error = |error| match error {
                StagingWindowConfirmationError::Store(error) => store_error_response(error),
                error => StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::MetadataCommandContention,
                    message: error.to_string(),
                },
            };
            match &file_segment {
                Some(segment) => {
                    // Same admitted current-primary section and bounded PG lock
                    // as allocation, but a strictly read-only SQLite snapshot.
                    authority
                        .require_effect(&pg)
                        .map_err(store_error_response)?;
                    query
                        .inspect_files(&pg, segment)
                        .map_err(inspection_error)?;
                    authority
                        .require_effect(&pg)
                        .map_err(store_error_response)?;
                    Ok(files::encode_response(bytes))
                }
                None => {
                    let segment = pg
                        .allocate_remote_confirmed_stream_window(&authority)
                        .map_err(inspection_error)?;
                    Ok(query.response(&segment))
                }
            }
        };
        #[cfg(test)]
        let result = if EXPIRE_AT_UPDATE.with(|slot| slot.replace(false)) {
            query.test_expire_at_update(action)
        } else {
            action()
        };
        #[cfg(not(test))]
        let result = action();
        match result {
            Ok(bytes) => Ok(encode_storage_rpc_success_response(&bytes)),
            Err(error) => encode_storage_rpc_error_response(&error),
        }
    }
}
