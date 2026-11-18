// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Candidate snapshot, preparation and installation on an admitted held primary
//! section. Installation retains the complete request through SQL validation.
use super::*;
use crate::node_runtime::payload_placement::PayloadPlacementInput;
use crate::node_runtime::pg_store::{
    PgStore, RemoteDirectPreparationQuery, RemoteDirectSnapshotQuery,
    StagingWindowConfirmationError,
};

// All operations share the same admission gate.
// The immutable enclosing query owns the nested allocation bound by authority.
enum DirectReadQuery<'a> {
    Snapshot(RemoteDirectSnapshotQuery<'a>),
    Terminal(Box<RemoteDirectPreparationQuery<'a>>),
    Installation(Box<RemoteDirectPreparationQuery<'a>>, &'a [u8]),
}
impl DirectReadQuery<'_> {
    fn snapshot(&self) -> &RemoteDirectSnapshotQuery<'_> {
        match self {
            Self::Snapshot(q) => q,
            Self::Terminal(q) | Self::Installation(q, _) => q.snapshot_query(),
        }
    }
    fn capture(
        &self,
        pg: &PgStore,
        authority: &RemoteDirectSnapshotAuthority<'_, '_>,
    ) -> Result<Vec<u8>, StagingWindowConfirmationError> {
        match self {
            Self::Snapshot(q) => q.capture_response(pg, authority),
            Self::Terminal(q) => q.capture_response(pg, authority),
            Self::Installation(q, bytes) => {
                #[cfg(test)]
                {
                    authority.test_with_install_expiry(|| q.install_response(pg, authority, bytes))
                }
                #[cfg(not(test))]
                {
                    q.install_response(pg, authority, bytes)
                }
            }
        }
    }
}

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
        StorageRpcMessageKind::StagingDirectSnapshot
            | StorageRpcMessageKind::StagingDirectPrepare
            | StorageRpcMessageKind::StagingDirectInstall
    ) && LOSE_REPLY.with(|slot| slot.replace(false))
}

pub(in crate::node_runtime) struct RemoteDirectSnapshotAuthority<'a, 'wire> {
    handler: &'a StorageNodeConnectionHandler,
    session: &'a StorageNodeSession,
    _admission: &'a StorageNodeRouteAdmissionPermit,
    store: &'a PgStore,
    query: &'a RemoteDirectSnapshotQuery<'wire>,
    fence: StorageNodeRouteFence,
    deadline: Instant,
    binding: StorageNodeMetadataCommandLockBinding,
    installation: Option<[u8; 32]>,
}

/// One identity per held PG section, discarded when that section is released.
/// A later preparation replaces it; it is neither durable custody nor a cache.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct DirectPreparationIdentity {
    preparation: [u8; 32],
    command: [u8; 32],
}

impl RemoteDirectSnapshotAuthority<'_, '_> {
    pub(in crate::node_runtime) fn reinspection_bounds(
        &self,
        pg: &PgStore,
        query: &RemoteDirectSnapshotQuery<'_>,
    ) -> Result<(Instant, crate::types::AdmittedRouteEffectFence), StoreError> {
        self.require_capture(pg, query)?;
        Ok((
            self.deadline.min(query.deadline()),
            self.fence.intersect_effect_fence(query.effect_fence())?,
        ))
    }
    pub(in crate::node_runtime) fn retain_prepared_command(
        &self,
        pg: &PgStore,
        query: &RemoteDirectSnapshotQuery<'_>,
        preparation: &[u8],
        command: [u8; 32],
    ) -> Result<(), StoreError> {
        self.require_capture(pg, query)?;
        self.session
            .metadata_command_guards
            .get(&self.binding.pg_id)
            .expect("validated held section")
            .direct_preparation
            .set(Some(DirectPreparationIdentity {
                preparation: crate::storage_rpc::complete_stream_allocation::digest(preparation),
                command,
            }));
        Ok(())
    }

    pub(in crate::node_runtime) fn matches_prepared_command(
        &self,
        preparation: &[u8],
        command: [u8; 32],
    ) -> bool {
        self.session
            .metadata_command_guards
            .get(&self.binding.pg_id)
            .is_some_and(|held| {
                held.binding == self.binding
                    && held.direct_preparation.get()
                        == Some(DirectPreparationIdentity {
                            preparation: crate::storage_rpc::complete_stream_allocation::digest(
                                preparation,
                            ),
                            command,
                        })
            })
    }
    #[cfg(test)]
    fn test_with_install_expiry<T>(&self, action: impl FnOnce() -> T) -> T {
        let Some(server) = EXPIRE_AT_INSTALL.with(|slot| slot.replace(None)) else {
            return action();
        };
        let clock = std::rc::Rc::new(std::cell::RefCell::new(None));
        let retained = std::rc::Rc::clone(&clock);
        let wall = crate::clock::current_time_millis();
        let monotonic = crate::clock::monotonic_time_millis();
        let cutoff = self.query.route().deadline.portable_wall_valid_until_ms;
        let server_cutoff = self.fence.local_valid_until_monotonic_ms.unwrap();
        let effect = self
            .fence
            .intersect_effect_fence(self.query.effect_fence())
            .unwrap();
        let result = PgStore::test_with_before_pending_slot_effect_hook(
            move |in_transaction| {
                assert!(in_transaction);
                let (wall, monotonic) = if server {
                    assert!(server_cutoff < monotonic + cutoff.saturating_sub(wall));
                    (wall, server_cutoff)
                } else {
                    let expired = monotonic + cutoff.saturating_sub(wall) + 100;
                    assert!(expired < server_cutoff);
                    (cutoff + 100, expired)
                };
                *retained.borrow_mut() = Some(
                    crate::clock::test_time_and_monotonic_override_guard(wall, monotonic),
                );
                assert!(effect.require_valid_for(effect.cluster_epoch()).is_err());
            },
            action,
        );
        assert!(clock.borrow().is_some(), "must reach pre-SQL effect hook");
        result
    }
    pub(in crate::node_runtime) fn require_installation(
        &self,
        pg: &PgStore,
        query: &RemoteDirectSnapshotQuery<'_>,
        bytes: &[u8],
    ) -> Result<crate::types::AdmittedRouteEffectFence, StoreError> {
        self.require_capture(pg, query)?;
        if self.installation
            != Some(crate::storage_rpc::complete_stream_allocation::digest(
                bytes,
            ))
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "remote direct installation admitted request",
            });
        }
        self.fence.intersect_effect_fence(query.effect_fence())
    }
    pub(in crate::node_runtime) fn installation_deadline(&self) -> Instant {
        self.deadline
    }
    pub(in crate::node_runtime) fn placement(&self) -> PayloadPlacementInput<'_> {
        PayloadPlacementInput::from_server_config(&self.handler.config)
    }
    pub(in crate::node_runtime) fn require_capture(
        &self,
        pg: &PgStore,
        query: &RemoteDirectSnapshotQuery<'_>,
    ) -> Result<(), StoreError> {
        if !std::ptr::eq(pg, self.store)
            || !std::ptr::eq(query, self.query)
            || pg.pg_id() != query.route().pg.get()
            || !self
                .session
                .holds_metadata_command_pg_lock_with_binding(self.binding)
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "remote direct snapshot exact request/store/section",
            });
        }
        self.fence.validate_store_at(
            crate::clock::current_time_millis(),
            crate::clock::monotonic_time_millis(),
        )?;
        query.require_valid()?;
        if Instant::now() >= self.deadline {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "remote direct snapshot captured route/work cutoff",
            });
        }
        Ok(())
    }
    pub(in crate::node_runtime) fn require_completed_capture(
        &self,
        pg: &PgStore,
        query: &RemoteDirectSnapshotQuery<'_>,
    ) -> Result<(), StoreError> {
        #[cfg(test)]
        let _clock = EXPIRE_AFTER_CAPTURE
            .with(|slot| slot.replace(None))
            .map(|server| {
                let wall = crate::clock::current_time_millis();
                let monotonic = crate::clock::monotonic_time_millis();
                let (wall, monotonic) = if server {
                    (
                        wall,
                        self.fence
                            .local_valid_until_monotonic_ms
                            .expect("bounded route"),
                    )
                } else {
                    (
                        query.route().deadline.portable_wall_valid_until_ms,
                        monotonic,
                    )
                };
                crate::clock::test_time_and_monotonic_override_guard(wall, monotonic)
            });
        self.require_capture(pg, query)?;
        pg.require_live_allocation_lifetime()
    }
}

impl StorageNodeConnectionHandler {
    pub(super) fn staging_direct_install_dispatch(
        &self,
        session: &StorageNodeSession,
        admission: &StorageNodeRouteAdmissionPermit,
        bytes: &[u8],
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        #[cfg(test)]
        if self.complete_log_operations {
            return self.complete_direct_read_response(
                session,
                admission,
                bytes,
                StorageRpcMessageKind::StagingDirectInstall,
            );
        }
        let _ = (session, admission, bytes);
        encode_storage_rpc_error_response(&StorageRpcErrorResponse {
            code: StorageRpcErrorCode::UnsupportedOperation,
            message: "staging direct terminal installation is not activated".into(),
        })
    }
    pub(super) fn staging_direct_prepare_dispatch(
        &self,
        session: &StorageNodeSession,
        admission: &StorageNodeRouteAdmissionPermit,
        bytes: &[u8],
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        #[cfg(test)]
        if self.complete_log_operations {
            return self.complete_direct_read_response(
                session,
                admission,
                bytes,
                StorageRpcMessageKind::StagingDirectPrepare,
            );
        }
        let _ = (session, admission, bytes);
        encode_storage_rpc_error_response(&StorageRpcErrorResponse {
            code: StorageRpcErrorCode::UnsupportedOperation,
            message: "staging direct terminal preparation is not activated".into(),
        })
    }
    pub(super) fn staging_direct_snapshot_dispatch(
        &self,
        session: &StorageNodeSession,
        admission: &StorageNodeRouteAdmissionPermit,
        bytes: &[u8],
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        #[cfg(test)]
        if self.complete_log_operations {
            return self.complete_direct_snapshot_response(session, admission, bytes);
        }
        let _ = (session, admission, bytes);
        encode_storage_rpc_error_response(&StorageRpcErrorResponse {
            code: StorageRpcErrorCode::UnsupportedOperation,
            message: "staging direct payload snapshot is not activated".into(),
        })
    }

    fn complete_direct_snapshot_response(
        &self,
        session: &StorageNodeSession,
        admission: &StorageNodeRouteAdmissionPermit,
        bytes: &[u8],
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        self.complete_direct_read_response(
            session,
            admission,
            bytes,
            StorageRpcMessageKind::StagingDirectSnapshot,
        )
    }

    fn complete_direct_read_response(
        &self,
        session: &StorageNodeSession,
        admission: &StorageNodeRouteAdmissionPermit,
        bytes: &[u8],
        kind: StorageRpcMessageKind,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let started = Instant::now();
        let fence = StorageNodeRouteFence::current(&self.config, self.current_route_map_lease());
        let action = || -> Result<Vec<u8>, StorageRpcErrorResponse> {
            self.validate_active_admission(admission, "staging direct snapshot")?;
            let request = match kind {
                StorageRpcMessageKind::StagingDirectInstall => {
                    crate::storage_rpc::complete_direct_install::decode_request(bytes)
                        .and_then(|r| {
                            RemoteDirectPreparationQuery::decode(
                                r.preparation,
                                self.node.pg_topology(),
                            )
                        })
                        .map(|q| DirectReadQuery::Installation(Box::new(q), bytes))
                }
                StorageRpcMessageKind::StagingDirectPrepare => {
                    RemoteDirectPreparationQuery::decode(bytes, self.node.pg_topology())
                        .map(|query| DirectReadQuery::Terminal(Box::new(query)))
                }
                StorageRpcMessageKind::StagingDirectSnapshot => {
                    RemoteDirectSnapshotQuery::decode(bytes, self.node.pg_topology())
                        .map(DirectReadQuery::Snapshot)
                }
                _ => unreachable!("closed direct receiver"),
            }
            .map_err(|e| StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: e.to_string(),
            })?;
            let query = request.snapshot();
            let route = query.route();
            self.validate_pg_route(route.node, route.epoch, route.pg)?;
            self.validate_primary_pg(route.pg, "staging direct snapshot")?;
            let binding = StorageNodeMetadataCommandLockBinding {
                pg_id: route.pg,
                cluster_epoch: route.epoch,
                authority: StorageNodeMetadataCommandLockAuthority::CurrentPrimary,
            };
            if !session.holds_metadata_command_pg_lock_with_binding(binding) {
                return Err(StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::MetadataCommandContention,
                    message: "direct snapshot requires its held current-primary section".into(),
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
            let authority = RemoteDirectSnapshotAuthority {
                handler: self,
                session,
                _admission: admission,
                store: &pg,
                query,
                fence,
                deadline,
                binding,
                installation: (kind == StorageRpcMessageKind::StagingDirectInstall)
                    .then(|| crate::storage_rpc::complete_stream_allocation::digest(bytes)),
            };
            request
                .capture(&pg, &authority)
                .map_err(|error| match error {
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

#[cfg(test)]
impl StorageNodeServer {
    pub(in crate::node_runtime) fn test_lose_direct_snapshot_reply() {
        LOSE_REPLY.with(|slot| slot.set(true));
    }
    pub(in crate::node_runtime) fn test_direct_snapshot_admission(&self, bytes: &[u8]) {
        self.test_direct_read_admission(bytes, StorageRpcMessageKind::StagingDirectSnapshot);
    }
    pub(in crate::node_runtime) fn test_direct_prepare_admission(&self, bytes: &[u8]) {
        self.test_direct_read_admission(bytes, StorageRpcMessageKind::StagingDirectPrepare);
    }
    pub(in crate::node_runtime) fn test_direct_install_admission(&self, bytes: &[u8]) {
        self.test_direct_read_admission(bytes, StorageRpcMessageKind::StagingDirectInstall);
    }
    pub(in crate::node_runtime) fn test_expire_direct_at_install(server: bool) {
        EXPIRE_AT_INSTALL.with(|slot| slot.set(Some(server)));
    }
    fn test_direct_read_admission(&self, bytes: &[u8], kind: StorageRpcMessageKind) {
        let handler = self.connection_handler();
        let session =
            StorageNodeSession::new(Arc::clone(&self.read_handles), Arc::clone(&self._node));
        for (permit, code, message) in [
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
                .complete_direct_read_response(&session, &permit, bytes, kind)
                .unwrap();
            let error = crate::storage_rpc::decode_storage_rpc_response_payload(&reply)
                .unwrap()
                .unwrap_err();
            assert_eq!(error.code, code);
            assert!(error.message.contains(message), "{error:?}");
        }
    }
    pub(in crate::node_runtime) fn test_expire_direct_snapshot_after_capture(server: bool) {
        EXPIRE_AFTER_CAPTURE.with(|slot| slot.set(Some(server)));
    }
}
