// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Admitted current or exact report-authorized historical complete application.
//! No arbitrary historical fallback or live RPC selection. Decoding retains the complete context
//! and supplies its receiver-bound immutable work cutoff to this closed operation.

use super::*;
use crate::metadata_command::MetadataCommandReplicaState;
use crate::node_runtime::pg_store::{
    CompleteApplicationError, CompleteApplicationRequest, PgStore,
};

#[cfg(test)]
#[path = "complete_application/api_tests.rs"]
mod api_tests;

pub(in crate::node_runtime) struct CompleteApplicationAuthority<'a> {
    handler: &'a StorageNodeConnectionHandler,
    _route: &'a StorageNodeRouteAdmissionPermit,
    _serialization: &'a StorageNodeMetadataCommandGuard,
    store: &'a PgStore,
    request: &'a CompleteApplicationRequest<'a>,
    fence: StorageNodeRouteFence,
    deadline: Instant,
}

impl CompleteApplicationAuthority<'_> {
    pub(in crate::node_runtime) fn node_id(&self) -> NodeId {
        self.handler.config.node_id
    }

    pub(in crate::node_runtime) fn request(&self) -> &CompleteApplicationRequest<'_> {
        self.request
    }

    pub(in crate::node_runtime) fn require_effect(
        &self,
        pg: &PgStore,
        request: &CompleteApplicationRequest<'_>,
    ) -> Result<(), StoreError> {
        if !std::ptr::eq(pg, self.store)
            || pg.pg_id() != self.request.id().pg_id().get()
            || request.node_id() != self.node_id()
            || request.id() != self.request.id()
            || request.command_sha256() != self.request.command_sha256()
            || request.topology() != self.handler.node.pg_topology()
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "complete application admitted command/store/topology",
            });
        }
        self.fence.validate_store_at(
            crate::clock::current_time_millis(),
            crate::clock::monotonic_time_millis(),
        )?;
        if Instant::now() >= self.deadline.min(request.deadline()) {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "complete application",
            });
        }
        Ok(())
    }
}

impl StorageNodeConnectionHandler {
    // Historical authority is for the exact reported pending command, not the
    // whole old epoch. Replicas serialize locally; the reporting primary must
    // retain its historical recovery session through application.
    fn complete_application_route(
        &self,
        request: &CompleteApplicationRequest<'_>,
    ) -> Result<
        (
            Option<StorageNodeMetadataCommandLockBinding>,
            StorageNodeRouteFence,
        ),
        StorageRpcErrorResponse,
    > {
        let id = request.id();
        if id.cluster_epoch() >= self.config.cluster_epoch {
            self.validate_pg_route(request.node_id(), id.cluster_epoch(), id.pg_id())?;
            return Ok((
                None,
                StorageNodeRouteFence::current(&self.config, self.current_route_map_lease()),
            ));
        }
        self.validate_historical_active_pg_route_for_metadata_command_recovery(
            request.node_id(),
            id.cluster_epoch(),
            id.pg_id(),
        )?;
        let historical = self
            .config
            .historical_pg_routes
            .iter()
            .find(|route| {
                route.pg_id == id.pg_id().get() && route.cluster_epoch == id.cluster_epoch()
            })
            .expect("validated historical route");
        let authorized =
            self.config
                .pending_metadata_command_recoveries
                .iter()
                .any(|(pg, recovery)| {
                    *pg == id.pg_id()
                        && recovery.reporting_node_id() == historical.primary_node_id
                        && historical
                            .acting_set
                            .contains(&recovery.reporting_node_id())
                        && recovery.pending().cluster_epoch() == id.cluster_epoch()
                        && recovery.pending().log_index() == id.log_index().get()
                        && recovery.pending().command_checksum() == request.checksum_crc64()
                        && recovery.pending().command_sha256() == request.command_sha256()
                });
        if !authorized {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::StaleShardLocation,
                message:
                    "complete application lacks exact historical pending-command authorization"
                        .into(),
            });
        }
        let binding = (self.config.node_id == historical.primary_node_id).then_some(
            StorageNodeMetadataCommandLockBinding {
                pg_id: id.pg_id(),
                cluster_epoch: id.cluster_epoch(),
                authority: StorageNodeMetadataCommandLockAuthority::HistoricalRecoveryPrimary,
            },
        );
        Ok((
            binding,
            self.bounded_historical_metadata_command_fence(id.cluster_epoch(), id.pg_id())?,
        ))
    }

    pub(super) fn apply_complete_command(
        &self,
        session: &StorageNodeSession,
        route: &StorageNodeRouteAdmissionPermit,
        request: &CompleteApplicationRequest<'_>,
    ) -> Result<
        Result<MetadataCommandReplicaState, CompleteApplicationError>,
        StorageRpcErrorResponse,
    > {
        self.with_complete_application(session, route, request, |pg, authority| {
            pg.apply_admitted_complete_metadata_command(authority)
        })
    }

    // Private owner: no sibling can supply a mutation callback or a detached
    // raw PG guard/store. Both replica-local and primary-session serialization
    // must belong to this exact server's lock registry.
    fn with_complete_application<T>(
        &self,
        session: &StorageNodeSession,
        route: &StorageNodeRouteAdmissionPermit,
        request: &CompleteApplicationRequest<'_>,
        action: impl FnOnce(&PgStore, &CompleteApplicationAuthority<'_>) -> T,
    ) -> Result<T, StorageRpcErrorResponse> {
        self.validate_active_admission(route, "complete application")?;
        let id = request.id();
        let (historical_primary, fence) = self.complete_application_route(request)?;
        if let Some(binding) = historical_primary {
            if !session.holds_metadata_command_pg_lock_with_binding(binding) {
                return Err(StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::StaleShardLocation,
                    message:
                        "complete application requires the exact held historical-primary section"
                            .into(),
                });
            }
        }
        let deadline = request.deadline();
        let now = Instant::now();
        let deadline = fence
            .local_valid_until_monotonic_ms
            .map_or(deadline, |cutoff| {
                deadline.min(
                    now.checked_add(Duration::from_millis(
                        cutoff.saturating_sub(crate::clock::monotonic_time_millis()),
                    ))
                    .unwrap_or(now),
                )
            });
        if now >= deadline {
            return Err(store_error_response(
                StoreError::OperationDeadlineExceeded {
                    context: "admit complete application",
                },
            ));
        }
        let acquired = self.metadata_command_pg_guard_until(session, id.pg_id(), Some(deadline))?;
        let serialization = match &acquired {
            Some(guard) => guard,
            None => {
                if id.cluster_epoch() < self.config.cluster_epoch && historical_primary.is_none() {
                    return Err(StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::StaleShardLocation,
                        message: "historical replica application cannot borrow a primary section"
                            .into(),
                    });
                }
                let binding = historical_primary.unwrap_or(StorageNodeMetadataCommandLockBinding {
                    pg_id: id.pg_id(),
                    cluster_epoch: id.cluster_epoch(),
                    authority: StorageNodeMetadataCommandLockAuthority::CurrentPrimary,
                });
                if historical_primary.is_none() {
                    self.validate_primary_pg(id.pg_id(), "complete application held section")?;
                }
                if !session.holds_metadata_command_pg_lock_with_binding(binding) {
                    return Err(StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::Internal,
                        message: "complete application requires an exact current-primary section"
                            .into(),
                    });
                }
                &session
                    .metadata_command_guards
                    .get(&id.pg_id())
                    .expect("validated held section")
                    ._guard
            }
        };
        if serialization.released
            || serialization.pg_id != id.pg_id()
            || !Arc::ptr_eq(
                &serialization.locks.state,
                &self.metadata_command_locks.state,
            )
        {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::Internal,
                message: "complete application serialization belongs to another PG/domain".into(),
            });
        }
        let pg = self
            .node
            .get_pg_until(id.pg_id().get(), deadline)
            .map_err(store_error_response)?;
        let authority = CompleteApplicationAuthority {
            handler: self,
            _route: route,
            _serialization: serialization,
            store: &pg,
            request,
            fence,
            deadline,
        };
        authority
            .require_effect(&pg, request)
            .map_err(store_error_response)?;
        Ok(action(&pg, &authority))
    }
}

#[cfg(test)]
impl StorageNodeServer {
    pub(in crate::node_runtime) fn test_complete_application_rejects_crossed_authority(
        &self,
        foreign: &Self,
        request: &CompleteApplicationRequest<'_>,
    ) {
        let handler = self.connection_handler();
        let active = self
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::Active);
        let foreign_permit = foreign
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::Active);
        let cleanup = self
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::RetainedCleanup);
        let session =
            StorageNodeSession::new(Arc::clone(&self.read_handles), Arc::clone(&self._node));
        handler
            .with_complete_application(&session, &active, request, |_, _| ())
            .unwrap();
        for permit in [&foreign_permit, &cleanup] {
            assert!(handler
                .with_complete_application(&session, permit, request, |_, _| panic!(
                    "crossed permit admitted"
                ))
                .is_err());
        }
        let id = request.id();
        for mode in ["exact", "epoch", "recovery", "foreign-locks"] {
            let mut session =
                StorageNodeSession::new(Arc::clone(&self.read_handles), Arc::clone(&self._node));
            let binding = StorageNodeMetadataCommandLockBinding {
                pg_id: id.pg_id(),
                cluster_epoch: if mode == "epoch" {
                    ClusterEpoch::new(id.cluster_epoch().get() + 1).unwrap()
                } else {
                    id.cluster_epoch()
                },
                authority: if mode == "recovery" {
                    StorageNodeMetadataCommandLockAuthority::HistoricalRecoveryPrimary
                } else {
                    StorageNodeMetadataCommandLockAuthority::CurrentPrimary
                },
            };
            let locks = if mode == "foreign-locks" {
                &foreign.metadata_command_locks
            } else {
                &self.metadata_command_locks
            };
            session
                .acquire_metadata_command_pg_lock(locks, handler.config.node_id, binding, None)
                .unwrap();
            let result = handler.with_complete_application(&session, &active, request, |_, _| ());
            assert_eq!(result.is_ok(), mode == "exact", "{mode}: {result:?}");
        }
    }

    pub(in crate::node_runtime) fn test_complete_application_serialization_is_bounded(
        &self,
        request: &CompleteApplicationRequest<'_>,
    ) {
        let handler = self.connection_handler();
        let _held = self
            .metadata_command_locks
            .acquire(handler.config.node_id, request.id().pg_id(), None)
            .unwrap();
        assert!(Instant::now() < request.deadline());
        let error = self
            .test_with_complete_application_authority(request, |_, _| {
                panic!("held serialization admitted")
            })
            .unwrap_err();
        assert_eq!(
            error.code,
            StorageRpcErrorCode::MetadataCommandContention,
            "{error:?}"
        );
    }

    pub(in crate::node_runtime) fn test_apply_complete_command(
        &self,
        request: &CompleteApplicationRequest<'_>,
    ) -> Result<
        Result<MetadataCommandReplicaState, CompleteApplicationError>,
        StorageRpcErrorResponse,
    > {
        let route = self
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::Active);
        let handler = self.connection_handler();
        let session =
            StorageNodeSession::new(Arc::clone(&self.read_handles), Arc::clone(&self._node));
        handler.apply_complete_command(&session, &route, request)
    }

    pub(in crate::node_runtime) fn test_with_complete_application_authority<T>(
        &self,
        request: &CompleteApplicationRequest<'_>,
        action: impl FnOnce(&PgStore, &CompleteApplicationAuthority<'_>) -> T,
    ) -> Result<T, StorageRpcErrorResponse> {
        let route = self
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::Active);
        let handler = self.connection_handler();
        let mut session =
            StorageNodeSession::new(Arc::clone(&self.read_handles), Arc::clone(&self._node));
        if let Some(binding) = handler.complete_application_route(request)?.0 {
            session.acquire_metadata_command_pg_lock(
                &handler.metadata_command_locks,
                handler.config.node_id,
                binding,
                None,
            )?;
        }
        handler.with_complete_application(&session, &route, request, action)
    }

    pub(in crate::node_runtime) fn test_complete_application_historical_sections(
        &self,
        request: &CompleteApplicationRequest<'_>,
    ) {
        let handler = self.connection_handler();
        let route = self
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::Active);
        let exact = handler
            .complete_application_route(request)
            .unwrap()
            .0
            .unwrap();
        for mode in ["exact", "current", "epoch", "pg", "cleanup"] {
            let mut session =
                StorageNodeSession::new(Arc::clone(&self.read_handles), Arc::clone(&self._node));
            let binding = StorageNodeMetadataCommandLockBinding {
                pg_id: if mode == "pg" {
                    PgId::new(exact.pg_id.get() + 1)
                } else {
                    exact.pg_id
                },
                cluster_epoch: if mode == "epoch" {
                    handler.config.cluster_epoch
                } else {
                    exact.cluster_epoch
                },
                authority: if mode == "current" {
                    StorageNodeMetadataCommandLockAuthority::CurrentPrimary
                } else {
                    exact.authority
                },
            };
            session
                .acquire_metadata_command_pg_lock(
                    &handler.metadata_command_locks,
                    handler.config.node_id,
                    binding,
                    None,
                )
                .unwrap();
            let cleanup = self
                .route_admission
                .acquire(StorageNodeRouteAdmissionClass::RetainedCleanup);
            let result = handler.with_complete_application(
                &session,
                if mode == "cleanup" { &cleanup } else { &route },
                request,
                |_, _| (),
            );
            assert_eq!(result.is_ok(), mode == "exact", "{mode}: {result:?}");
        }
    }
}
