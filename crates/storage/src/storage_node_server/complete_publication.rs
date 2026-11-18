// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Current-primary or exact map-authorized historical publication admission.
//! Only explicitly selected owner-local servers dispatch this candidate path yet.

use super::*;
use crate::node_runtime::pg_store::{
    CompletePublicationQuery, CompletePublicationRequest, PgStore,
};
use std::time::Instant;

#[path = "complete_publication/authority.rs"]
mod authority;
pub(in crate::node_runtime) use authority::CompletePublicationAuthority;

#[cfg(test)]
#[path = "complete_publication/api_tests.rs"]
mod api_tests;

#[cfg(test)]
thread_local! {
    static EXPIRE_BEFORE_STORE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static LOSE_COMMITTED_REPLY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
pub(super) fn test_lose_committed_reply(response: &StorageRpcFrame) -> bool {
    if response.kind != StorageRpcMessageKind::MetadataCommandPublicationStart
        || !LOSE_COMMITTED_REPLY.with(|slot| slot.replace(false))
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
    // Shared read-only admission predicate for publication and quiescent
    // retirement. Each owner still privately mints its own effect capability.
    pub(super) fn complete_publication_route(
        &self,
        request: &CompletePublicationRequest<'_>,
    ) -> Result<
        (StorageNodeMetadataCommandLockBinding, StorageNodeRouteFence),
        StorageRpcErrorResponse,
    > {
        let id = request.id();
        let (authority, fence) = if id.cluster_epoch() < self.config.cluster_epoch {
            self.validate_metadata_command_recovery_primary_lock(
                request.node_id(),
                id.cluster_epoch(),
                id.pg_id(),
            )?;
            // The lock is PG/epoch scoped. Publication additionally needs the
            // exact control-plane report, not any command in that section.
            let authorized =
                self.config
                    .pending_metadata_command_recoveries
                    .iter()
                    .any(|(pg, recovery)| {
                        *pg == id.pg_id()
                            && recovery.reporting_node_id() == self.config.node_id
                            && recovery.pending().cluster_epoch() == id.cluster_epoch()
                            && recovery.pending().log_index() == id.log_index().get()
                            && recovery.pending().command_checksum() == request.checksum_crc64()
                            && recovery.pending().command_sha256() == request.command_sha256()
                    });
            if !authorized {
                return Err(StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::StaleShardLocation,
                    message:
                        "complete publication lacks exact historical pending-command authorization"
                            .into(),
                });
            }
            (
                StorageNodeMetadataCommandLockAuthority::HistoricalRecoveryPrimary,
                self.bounded_historical_metadata_command_fence(id.cluster_epoch(), id.pg_id())?,
            )
        } else {
            // This also rejects future epochs. Historical recovery must never
            // fall back to ordinary current-primary authority.
            self.validate_pg_route(request.node_id(), id.cluster_epoch(), id.pg_id())?;
            self.validate_primary_pg(id.pg_id(), "complete publication start")?;
            (
                StorageNodeMetadataCommandLockAuthority::CurrentPrimary,
                StorageNodeRouteFence::current(&self.config, self.current_route_map_lease()),
            )
        };
        Ok((
            StorageNodeMetadataCommandLockBinding {
                pg_id: id.pg_id(),
                cluster_epoch: id.cluster_epoch(),
                authority,
            },
            fence,
        ))
    }

    pub(super) fn metadata_command_publication_dispatch(
        &self,
        session: &StorageNodeSession,
        _route: &StorageNodeRouteAdmissionPermit,
        payload: &[u8],
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        #[cfg(test)]
        if self.complete_log_operations {
            return self.complete_metadata_command_publication_response(session, _route, payload);
        }
        match decode_metadata_command_request(payload, &MetadataCommandDecodeAuthority::new()) {
            Ok(request) => self.metadata_command_publication_start_response(session, request),
            Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: error.to_string(),
            }),
        }
    }

    fn complete_metadata_command_publication_response(
        &self,
        session: &StorageNodeSession,
        route: &StorageNodeRouteAdmissionPermit,
        payload: &[u8],
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let query = match CompletePublicationQuery::decode(payload, self.node.pg_topology()) {
            Ok(query) => query,
            Err(error) => {
                return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: error.to_string(),
                })
            }
        };
        let request = query.request()?;
        let now = Instant::now();
        let deadline = query.deadline().min(
            now.checked_add(storage_node_rpc_io_timeout(self.rpc_auth.as_deref()))
                .unwrap_or(now),
        );
        let result = (|| -> Result<Vec<u8>, StorageRpcErrorResponse> {
            self.validate_active_admission(route, "complete publication start")?;
            let (binding, _) = self.complete_publication_route(&request)?;
            if !session.holds_metadata_command_pg_lock_with_binding(binding) {
                return Err(StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::StaleShardLocation,
                    message: "complete publication requires the exact held primary section".into(),
                });
            }
            #[cfg(test)]
            EXPIRE_BEFORE_STORE.with(|expire| {
                if expire.get() {
                    let captured = query.deadline();
                    assert!(
                        Instant::now() < captured,
                        "expiry canary must reach a live rebound cutoff"
                    );
                    while let Some(remaining) = captured.checked_duration_since(Instant::now()) {
                        std::thread::sleep(remaining);
                    }
                }
            });
            let pg = self
                .node
                .get_pg_until(request.id().pg_id().get(), deadline)
                .map_err(store_error_response)?;
            let authority =
                CompletePublicationAuthority::admit(self, session, route, &pg, &request, deadline)?;
            request.start_response(&pg, &authority).map_err(|error| {
                match error.into_store_error() {
                    Ok(error) => store_error_response(error),
                    Err(error) => StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::Internal,
                        message: error.to_string(),
                    },
                }
            })
        })();
        match result {
            Ok(payload) => Ok(encode_storage_rpc_success_response(&payload)),
            Err(error) => encode_storage_rpc_error_response(&error),
        }
    }
}

#[cfg(test)]
impl StorageNodeServer {
    pub(in crate::node_runtime) fn test_lose_publication_reply() {
        LOSE_COMMITTED_REPLY.with(|slot| slot.set(true));
    }

    /// Close the real connection after successful publication dispatch, before
    /// sending any reply bytes. The worker-local fault cannot affect another
    /// server/session or escape through a transferable guard.
    pub(in crate::node_runtime) fn test_accept_one_losing_publication_reply(
        &self,
    ) -> Result<(), StorageNodeServerError> {
        struct Reset(bool);
        impl Drop for Reset {
            fn drop(&mut self) {
                LOSE_COMMITTED_REPLY.with(|slot| slot.set(self.0));
            }
        }
        let _reset = Reset(LOSE_COMMITTED_REPLY.with(|slot| slot.replace(true)));
        self.accept_one()
    }

    /// Pause only after decoding/binding the publication cutoff on this worker.
    /// The override cannot escape the worker or affect another server's thread.
    pub(in crate::node_runtime) fn test_accept_one_expiring_publication(
        &self,
    ) -> Result<(), StorageNodeServerError> {
        struct Reset(bool);
        impl Drop for Reset {
            fn drop(&mut self) {
                EXPIRE_BEFORE_STORE.with(|expire| expire.set(self.0));
            }
        }
        let _reset = Reset(EXPIRE_BEFORE_STORE.with(|slot| slot.replace(true)));
        self.accept_one()
    }

    pub(in crate::node_runtime) fn test_complete_publication_rejects_wrong_permit(
        &self,
        foreign: &Self,
        request: &CompletePublicationRequest<'_>,
        retained_cleanup: bool,
        draining: bool,
    ) {
        assert!(!Arc::ptr_eq(
            &self.route_admission.inner,
            &foreign.route_admission.inner
        ));
        std::thread::scope(|scope| {
            // Acquire before draining. This exact permit must remain sufficient
            // to finish admitted publication while excluding route replacement.
            // Keep it inside the scope so a test panic drops it before joining.
            let active = self
                .route_admission
                .acquire(StorageNodeRouteAdmissionClass::Active);
            let transition = draining.then(|| {
                scope.spawn(|| {
                    let _guard = self.route_admission.begin_transition();
                })
            });
            if draining {
                let state = self.route_admission.inner.state.lock().unwrap();
                let (state, _) = self
                    .route_admission
                    .inner
                    .changed
                    .wait_timeout_while(state, std::time::Duration::from_secs(30), |state| {
                        state.transition != StorageNodeRouteTransitionState::Draining
                    })
                    .unwrap();
                assert_eq!(state.transition, StorageNodeRouteTransitionState::Draining);
            }
            let handler = self.connection_handler();
            let mut session =
                StorageNodeSession::new(Arc::clone(&self.read_handles), Arc::clone(&self._node));
            let (binding, _) = handler.complete_publication_route(request).unwrap();
            session
                .acquire_metadata_command_pg_lock(
                    &handler.metadata_command_locks,
                    handler.config.node_id,
                    binding,
                    None,
                )
                .unwrap();
            let pg = self._node.get_pg(request.id().pg_id().get()).unwrap();
            let deadline = Instant::now() + std::time::Duration::from_secs(30);
            {
                let valid = CompletePublicationAuthority::admit(
                    &handler, &session, &active, &pg, request, deadline,
                )
                .unwrap();
                valid.require_effect(&pg, request).unwrap();
            }
            let permit = if retained_cleanup {
                self.route_admission
                    .acquire(StorageNodeRouteAdmissionClass::RetainedCleanup)
            } else {
                foreign
                    .route_admission
                    .acquire(StorageNodeRouteAdmissionClass::Active)
            };
            let error = match CompletePublicationAuthority::admit(&handler, &session, &permit, &pg, request, deadline) {
                Ok(_) => panic!("invalid publication permit minted authority: cleanup={retained_cleanup}, draining={draining}"),
                Err(error) => error,
            };
            assert_eq!(error.code, StorageRpcErrorCode::Internal);
            assert_eq!(
                error.message,
                if retained_cleanup {
                    "complete publication start requires active route admission, got RetainedCleanup"
                } else {
                    "complete publication start route permit belongs to a different admission domain"
                }
            );
            if let Some(transition) = transition {
                assert!(
                    !transition.is_finished(),
                    "the retained Active permit must exclude publication"
                );
                drop(pg);
                drop(session);
                drop(permit);
                drop(active);
                transition.join().unwrap();
            }
        });
    }

    pub(in crate::node_runtime) fn test_complete_publication_rejects_wrong_sections(
        &self,
        request: &CompletePublicationRequest<'_>,
    ) {
        let route = self
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::Active);
        let handler = self.connection_handler();
        let (current, _) = handler.complete_publication_route(request).unwrap();
        let crossed_authority = match current.authority {
            StorageNodeMetadataCommandLockAuthority::CurrentPrimary => {
                StorageNodeMetadataCommandLockAuthority::HistoricalRecoveryPrimary
            }
            StorageNodeMetadataCommandLockAuthority::HistoricalRecoveryPrimary => {
                StorageNodeMetadataCommandLockAuthority::CurrentPrimary
            }
        };
        for binding in [
            None,
            Some(StorageNodeMetadataCommandLockBinding {
                authority: crossed_authority,
                ..current
            }),
            Some(StorageNodeMetadataCommandLockBinding {
                cluster_epoch: ClusterEpoch::new(current.cluster_epoch.get() + 1).unwrap(),
                ..current
            }),
            Some(StorageNodeMetadataCommandLockBinding {
                pg_id: PgId::new(current.pg_id.get() + 1),
                ..current
            }),
        ] {
            let mut session =
                StorageNodeSession::new(Arc::clone(&self.read_handles), Arc::clone(&self._node));
            if let Some(binding) = binding {
                session
                    .acquire_metadata_command_pg_lock(
                        &handler.metadata_command_locks,
                        handler.config.node_id,
                        binding,
                        None,
                    )
                    .unwrap();
            }
            let pg = self._node.get_pg(request.id().pg_id().get()).unwrap();
            let result = CompletePublicationAuthority::admit(
                &handler,
                &session,
                &route,
                &pg,
                request,
                Instant::now() + std::time::Duration::from_secs(60),
            );
            assert!(
                result.is_err(),
                "wrong section must not mint publication authority: {binding:?}"
            );
        }
    }

    pub(in crate::node_runtime) fn test_with_complete_publication_authority<T>(
        &self,
        request: &CompletePublicationRequest<'_>,
        deadline: Instant,
        action: impl FnOnce(&PgStore, &CompletePublicationAuthority<'_>) -> T,
    ) -> Result<T, StorageRpcErrorResponse> {
        let route = self
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::Active);
        let handler = self.connection_handler();
        // The fixture holds the same exact primary session binding required by
        // the production protocol. It does not invent an authority token.
        let (binding, _) = handler.complete_publication_route(request)?;
        let mut session =
            StorageNodeSession::new(Arc::clone(&self.read_handles), Arc::clone(&self._node));
        session.acquire_metadata_command_pg_lock(
            &handler.metadata_command_locks,
            handler.config.node_id,
            binding,
            None,
        )?;
        let pg = self
            ._node
            .get_pg_until(request.id().pg_id().get(), deadline)
            .map_err(store_error_response)?;
        let authority = CompletePublicationAuthority::admit(
            &handler, &session, &route, &pg, request, deadline,
        )?;
        Ok(action(&pg, &authority))
    }
}
