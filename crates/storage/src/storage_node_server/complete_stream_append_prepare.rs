// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Candidate append capture/installation on a held primary section. No peer IO.
use super::*;
use crate::node_runtime::payload_placement::PayloadPlacementInput;
use crate::node_runtime::pg_store::{
    PgStore, RemoteStreamAppendQuery, StagingWindowConfirmationError,
};

#[cfg(test)]
thread_local! {
    static EXPIRE_AFTER_CAPTURE: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
    static LOSE_REPLY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static EXPIRE_AT_INSTALL: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
}

#[cfg(test)]
pub(super) fn test_lose_reply(response: &StorageRpcFrame) -> bool {
    matches!(
        response.kind,
        StorageRpcMessageKind::StagingStreamAppendPrepare
            | StorageRpcMessageKind::StagingStreamAppendInstall
    ) && LOSE_REPLY.with(|slot| slot.replace(false))
}

#[cfg(test)]
impl StorageNodeServer {
    pub(in crate::node_runtime) fn test_expire_stream_append_at_install(server_route: bool) {
        EXPIRE_AT_INSTALL.with(|slot| slot.set(Some(server_route)));
    }
    pub(in crate::node_runtime) fn test_lose_stream_append_reply() {
        LOSE_REPLY.with(|slot| slot.set(true));
    }

    pub(in crate::node_runtime) fn test_expire_stream_append_after_capture(server_route: bool) {
        EXPIRE_AFTER_CAPTURE.with(|slot| slot.set(Some(server_route)));
    }

    pub(in crate::node_runtime) fn test_stream_append_admission(&self, bytes: &[u8]) {
        self.test_stream_append_admission_inner(bytes, false);
    }
    pub(in crate::node_runtime) fn test_stream_append_install_admission(&self, bytes: &[u8]) {
        self.test_stream_append_admission_inner(bytes, true);
    }
    fn test_stream_append_admission_inner(&self, bytes: &[u8], install: bool) {
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
                .complete_stream_append_response(&session, &permit, bytes, install)
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
        let reply = if install {
            handler.staging_stream_append_install_dispatch(&session, &permit, bytes)
        } else {
            handler.staging_stream_append_prepare_dispatch(&session, &permit, bytes)
        }
        .unwrap();
        assert_eq!(
            crate::storage_rpc::decode_storage_rpc_response_payload(&reply)
                .unwrap()
                .unwrap_err()
                .code,
            StorageRpcErrorCode::UnsupportedOperation
        );
    }
}

pub(in crate::node_runtime) struct RemoteStreamAppendAuthority<'a> {
    handler: &'a StorageNodeConnectionHandler,
    session: &'a StorageNodeSession,
    _admission: &'a StorageNodeRouteAdmissionPermit,
    store: &'a PgStore,
    digest: [u8; 32],
    fence: StorageNodeRouteFence,
    deadline: Instant,
    binding: StorageNodeMetadataCommandLockBinding,
    installation: Option<[u8; 32]>,
}

impl RemoteStreamAppendAuthority<'_> {
    #[cfg(test)]
    fn test_with_install_expiry<T>(
        &self,
        query: &RemoteStreamAppendQuery<'_>,
        action: impl FnOnce() -> T,
    ) -> T {
        let Some(server) = EXPIRE_AT_INSTALL.with(|slot| slot.replace(None)) else {
            return action();
        };
        let clock = std::rc::Rc::new(std::cell::RefCell::new(None));
        let retained = std::rc::Rc::clone(&clock);
        let wall = crate::clock::current_time_millis();
        let monotonic = crate::clock::monotonic_time_millis();
        let cutoff = query.route().deadline.portable_wall_valid_until_ms;
        let server_cutoff = self.fence.local_valid_until_monotonic_ms.unwrap();
        let effect = self
            .fence
            .intersect_effect_fence(query.effect_fence().unwrap())
            .unwrap();
        let result = PgStore::test_with_before_pending_slot_effect_hook(
            move |in_transaction| {
                assert!(in_transaction);
                let (wall, monotonic) = if server {
                    assert!(server_cutoff < monotonic + cutoff.saturating_sub(wall));
                    (wall, server_cutoff)
                } else {
                    // Advance past the client cutoff, while keeping the server
                    // route live. Do not infer equality from separately sampled
                    // wall/monotonic clocks, which can straddle a millisecond.
                    let expired = monotonic + cutoff.saturating_sub(wall) + 100;
                    assert!(expired < server_cutoff);
                    (cutoff + 100, expired)
                };
                *retained.borrow_mut() = Some(
                    crate::clock::test_time_and_monotonic_override_guard(wall, monotonic),
                );
                assert!(
                    effect.require_valid_for(effect.cluster_epoch()).is_err(),
                    "injected clock must expire captured effect fence"
                );
            },
            action,
        );
        assert!(
            clock.borrow().is_some(),
            "installation must reach the pre-SQL effect hook"
        );
        result
    }
    pub(in crate::node_runtime) fn require_installation(
        &self,
        pg: &PgStore,
        query: &RemoteStreamAppendQuery<'_>,
        bytes: &[u8],
    ) -> Result<AdmittedRouteEffectFence, StoreError> {
        if self.installation
            != Some(crate::storage_rpc::complete_stream_allocation::digest(
                bytes,
            ))
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "remote append installation admitted request",
            });
        }
        self.require_capture(pg, query)
    }
    pub(in crate::node_runtime) fn installation_deadline(&self) -> Instant {
        self.deadline
    }
    pub(in crate::node_runtime) fn require_completed_capture(
        &self,
        pg: &PgStore,
        query: &RemoteStreamAppendQuery<'_>,
    ) -> Result<(), StoreError> {
        // Test expiry after the complete read-only capture, not merely before
        // admission. The override lives only on this connection thread.
        #[cfg(test)]
        let _clock = EXPIRE_AFTER_CAPTURE
            .with(|slot| slot.replace(None))
            .map(|server_route| {
                let now = crate::clock::current_time_millis();
                let monotonic = crate::clock::monotonic_time_millis();
                let (wall, monotonic) = if server_route {
                    (
                        now,
                        self.fence
                            .local_valid_until_monotonic_ms
                            .expect("bounded server route"),
                    )
                } else {
                    let cutoff = query.route().deadline.portable_wall_valid_until_ms;
                    let elapsed = cutoff.saturating_sub(now);
                    assert!(
                        monotonic + elapsed < self.fence.local_valid_until_monotonic_ms.unwrap()
                    );
                    (cutoff, monotonic + elapsed)
                };
                crate::clock::test_time_and_monotonic_override_guard(wall, monotonic)
            });
        self.require_capture(pg, query)?;
        pg.require_live_allocation_lifetime()
    }

    pub(in crate::node_runtime) fn placement(&self) -> PayloadPlacementInput<'_> {
        PayloadPlacementInput::from_server_config(&self.handler.config)
    }
    pub(in crate::node_runtime) fn require_capture(
        &self,
        pg: &PgStore,
        query: &RemoteStreamAppendQuery<'_>,
    ) -> Result<AdmittedRouteEffectFence, StoreError> {
        let route = query.route();
        if !std::ptr::eq(pg, self.store)
            || pg.pg_id() != route.pg.get()
            || query.digest() != self.digest
            || route.topology
                != crate::storage_rpc::complete_settled_prefix::topology_digest(
                    self.handler.node.pg_topology(),
                )
            || !self
                .session
                .holds_metadata_command_pg_lock_with_binding(self.binding)
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "remote append exact request/store/section",
            });
        }
        self.fence.validate_store_at(
            crate::clock::current_time_millis(),
            crate::clock::monotonic_time_millis(),
        )?;
        query.require_valid()?;
        if Instant::now() >= self.deadline {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "remote append captured route/work cutoff",
            });
        }
        self.fence.intersect_effect_fence(query.effect_fence()?)
    }
}

impl StorageNodeConnectionHandler {
    pub(super) fn staging_stream_append_install_dispatch(
        &self,
        session: &StorageNodeSession,
        admission: &StorageNodeRouteAdmissionPermit,
        bytes: &[u8],
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        #[cfg(test)]
        if self.complete_log_operations {
            return self.complete_stream_append_response(session, admission, bytes, true);
        }
        let _ = (session, admission, bytes);
        encode_storage_rpc_error_response(&StorageRpcErrorResponse {
            code: StorageRpcErrorCode::UnsupportedOperation,
            message: "staging stream append installation is not activated".into(),
        })
    }
    pub(super) fn staging_stream_append_prepare_dispatch(
        &self,
        session: &StorageNodeSession,
        admission: &StorageNodeRouteAdmissionPermit,
        bytes: &[u8],
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        #[cfg(test)]
        if self.complete_log_operations {
            return self.complete_stream_append_prepare_response(session, admission, bytes);
        }
        let _ = (session, admission, bytes);
        encode_storage_rpc_error_response(&StorageRpcErrorResponse {
            code: StorageRpcErrorCode::UnsupportedOperation,
            message: "staging stream append preparation is not activated".into(),
        })
    }

    #[allow(dead_code)] // Candidate selection only, until coordinated activation.
    fn complete_stream_append_prepare_response(
        &self,
        session: &StorageNodeSession,
        admission: &StorageNodeRouteAdmissionPermit,
        bytes: &[u8],
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        self.complete_stream_append_response(session, admission, bytes, false)
    }

    fn complete_stream_append_response(
        &self,
        session: &StorageNodeSession,
        admission: &StorageNodeRouteAdmissionPermit,
        bytes: &[u8],
        install: bool,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let started = Instant::now();
        let fence = StorageNodeRouteFence::current(&self.config, self.current_route_map_lease());
        let action = || -> Result<Vec<u8>, StorageRpcErrorResponse> {
            self.validate_active_admission(admission, "staging stream append preparation")?;
            let preparation = if install {
                crate::storage_rpc::complete_stream_append_install::decode_request(bytes)
                    .map_err(|e| StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: e.to_string(),
                    })?
                    .preparation
            } else {
                bytes
            };
            let query = RemoteStreamAppendQuery::decode(preparation, self.node.pg_topology())
                .map_err(|e| StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: e.to_string(),
                })?;
            let route = query.route();
            self.validate_pg_route(route.node, route.epoch, route.pg)?;
            self.validate_primary_pg(route.pg, "staging stream append preparation")?;
            let binding = StorageNodeMetadataCommandLockBinding {
                pg_id: route.pg,
                cluster_epoch: route.epoch,
                authority: StorageNodeMetadataCommandLockAuthority::CurrentPrimary,
            };
            if !session.holds_metadata_command_pg_lock_with_binding(binding) {
                return Err(StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::MetadataCommandContention,
                    message: "append preparation requires its held current-primary section".into(),
                });
            }
            let mut deadline = query.deadline().min(
                started
                    .checked_add(storage_node_rpc_io_timeout(self.rpc_auth.as_deref()))
                    .unwrap_or(started),
            );
            if let Some(cutoff) = fence.local_valid_until_monotonic_ms {
                deadline = deadline.min(
                    Instant::now()
                        .checked_add(Duration::from_millis(
                            cutoff.saturating_sub(crate::clock::monotonic_time_millis()),
                        ))
                        .unwrap_or(started),
                );
            }
            let pg = self
                .node
                .get_pg_until(route.pg.get(), deadline)
                .map_err(store_error_response)?;
            let authority = RemoteStreamAppendAuthority {
                handler: self,
                session,
                _admission: admission,
                store: &pg,
                digest: query.digest(),
                fence,
                deadline,
                binding,
                installation: install
                    .then(|| crate::storage_rpc::complete_stream_allocation::digest(bytes)),
            };
            let result = if install {
                #[cfg(test)]
                {
                    authority.test_with_install_expiry(&query, || {
                        query.install_response(&pg, &authority, bytes)
                    })
                }
                #[cfg(not(test))]
                {
                    query.install_response(&pg, &authority, bytes)
                }
            } else {
                query.capture_response(&pg, &authority)
            };
            result.map_err(|error| match error {
                StagingWindowConfirmationError::Store(error) => store_error_response(error),
                error => StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::MetadataCommandContention,
                    message: error.to_string(),
                },
            })
        };
        match action() {
            Ok(body) => Ok(encode_storage_rpc_success_response(&body)),
            Err(error) => encode_storage_rpc_error_response(&error),
        }
    }
}
