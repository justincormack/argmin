// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Candidate replica-only receiver. Primary confirmation belongs to the owning
//! authenticated sender, not to a caller-provided wire certificate. Locally we
//! require Active admission, the exact current/historical cohort, serialization,
//! an immutable cutoff, and the complete-command identity before any mutation.

use super::*;
#[cfg(test)]
#[path = "complete_replica_abandonment/api_tests.rs"]
mod api_tests;
use crate::node_runtime::pg_store::{
    CompletePublicationRequest, CompleteReplicaAbandonmentQuery, CompleteReplicaAbandonmentRequest,
    PgStore,
};

pub(in crate::node_runtime) struct RemoteReplicaAbandonmentAuthority<'a> {
    handler: &'a StorageNodeConnectionHandler,
    _route: &'a StorageNodeRouteAdmissionPermit,
    _serialization: &'a StorageNodeMetadataCommandGuard,
    store: &'a PgStore,
    request: &'a CompleteReplicaAbandonmentRequest<'a>,
    fence: StorageNodeRouteFence,
    deadline: Instant,
}

impl RemoteReplicaAbandonmentAuthority<'_> {
    pub(in crate::node_runtime) fn require_received_request(
        &self,
        pg: &PgStore,
        request: &CompleteReplicaAbandonmentRequest<'_>,
    ) -> Result<(), StoreError> {
        if request.primary() != self.request.primary() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "replica abandonment confirmed primary",
            });
        }
        self.require_effect(pg, request.publication_request())
    }

    pub(in crate::node_runtime) fn request(&self) -> &CompletePublicationRequest<'_> {
        self.request.publication_request()
    }

    pub(in crate::node_runtime) fn require_effect(
        &self,
        pg: &PgStore,
        request: &CompletePublicationRequest<'_>,
    ) -> Result<(), StoreError> {
        let expected = self.request();
        if !std::ptr::eq(pg, self.store)
            || pg.pg_id() != expected.id().pg_id().get()
            || request.node_id() != self.handler.config.node_id
            || request.id() != expected.id()
            || request.command_sha256() != expected.command_sha256()
            || request.topology() != self.handler.node.pg_topology()
            || request.node_id() == self.request.primary()
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "replica abandonment admitted command/store/topology",
            });
        }
        self.fence.validate_store_at(
            crate::clock::current_time_millis(),
            crate::clock::monotonic_time_millis(),
        )?;
        if Instant::now() >= expected.bound_deadline(self.deadline) {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "replica abandonment",
            });
        }
        Ok(())
    }
}

fn rejected(message: &'static str) -> StorageRpcErrorResponse {
    StorageRpcErrorResponse {
        code: StorageRpcErrorCode::StaleShardLocation,
        message: message.into(),
    }
}

impl StorageNodeConnectionHandler {
    fn replica_abandonment_fence(
        &self,
        request: &CompleteReplicaAbandonmentRequest<'_>,
    ) -> Result<StorageNodeRouteFence, StorageRpcErrorResponse> {
        let command = request.publication_request();
        let id = command.id();
        let (primary, fence) = if id.cluster_epoch() < self.config.cluster_epoch {
            self.validate_historical_active_pg_route_for_metadata_command_recovery(
                command.node_id(),
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
                            && recovery.pending().command_checksum() == command.checksum_crc64()
                            && recovery.pending().command_sha256() == command.command_sha256()
                    });
            if !authorized {
                return Err(rejected(
                    "replica abandonment lacks exact historical pending-command authorization",
                ));
            }
            (
                historical.primary_node_id,
                self.bounded_historical_metadata_command_fence(id.cluster_epoch(), id.pg_id())?,
            )
        } else {
            self.validate_pg_route(command.node_id(), id.cluster_epoch(), id.pg_id())?;
            let route = self
                .config
                .pg_routes
                .iter()
                .find(|route| route.pg_id == id.pg_id().get())
                .expect("validated route");
            (
                route.primary_node_id,
                StorageNodeRouteFence::current(&self.config, self.current_route_map_lease()),
            )
        };
        if primary != request.primary() || primary == self.config.node_id {
            return Err(rejected(
                "replica abandonment requires the exact primary and a distinct replica",
            ));
        }
        Ok(fence)
    }

    // Only this owner can construct receiver authority. No production callback,
    // raw-store adapter or constructor is exposed to sibling modules.
    fn with_replica_abandonment<T>(
        &self,
        session: &StorageNodeSession,
        route: &StorageNodeRouteAdmissionPermit,
        request: &CompleteReplicaAbandonmentRequest<'_>,
        budget: Instant,
        action: impl FnOnce(&PgStore, &RemoteReplicaAbandonmentAuthority<'_>) -> T,
    ) -> Result<T, StorageRpcErrorResponse> {
        self.validate_active_admission(route, "replica abandonment")?;
        let fence = self.replica_abandonment_fence(request)?;
        let command = request.publication_request();
        let now = Instant::now();
        let deadline = command.bound_deadline(budget);
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
        let serialization = self
            .metadata_command_pg_guard_until(session, command.id().pg_id(), Some(deadline))?
            .ok_or_else(|| rejected("replica abandonment cannot borrow a primary section"))?;
        if serialization.released
            || serialization.pg_id != command.id().pg_id()
            || !Arc::ptr_eq(
                &serialization.locks.state,
                &self.metadata_command_locks.state,
            )
        {
            return Err(rejected(
                "replica abandonment serialization belongs to another PG/domain",
            ));
        }
        let pg = self
            .node
            .get_pg_until(command.id().pg_id().get(), deadline)
            .map_err(store_error_response)?;
        let authority = RemoteReplicaAbandonmentAuthority {
            handler: self,
            _route: route,
            _serialization: &serialization,
            store: &pg,
            request,
            fence,
            deadline,
        };
        authority
            .require_effect(&pg, command)
            .map_err(store_error_response)?;
        Ok(action(&pg, &authority))
    }

    pub(super) fn complete_replica_abandonment_response(
        &self,
        session: &StorageNodeSession,
        route: &StorageNodeRouteAdmissionPermit,
        payload: &[u8],
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let now = Instant::now();
        let budget = now
            .checked_add(storage_node_rpc_io_timeout(self.rpc_auth.as_deref()))
            .unwrap_or(now);
        let query = match CompleteReplicaAbandonmentQuery::decode(payload, self.node.pg_topology())
        {
            Ok(query) => query,
            Err(error) => {
                return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: error.to_string(),
                })
            }
        };
        let request = query.request()?;
        let result = self
            .with_replica_abandonment(session, route, &request, budget, |pg, authority| {
                request.abandon_response(pg, authority).map_err(|error| {
                    match error.into_store_error() {
                        Ok(error) => store_error_response(error),
                        Err(error) => StorageRpcErrorResponse {
                            code: StorageRpcErrorCode::Internal,
                            message: error.to_string(),
                        },
                    }
                })
            })
            .and_then(std::convert::identity);
        match result {
            Ok(reply) => Ok(encode_storage_rpc_success_response(&reply)),
            Err(error) => encode_storage_rpc_error_response(&error),
        }
    }
}

#[cfg(test)]
type StageHook = Box<dyn FnMut(&'static str)>;

#[cfg(test)]
thread_local! {
    static STAGE_HOOK: std::cell::RefCell<Option<StageHook>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(in crate::node_runtime) fn test_replica_abandonment_stage(stage: &'static str) {
    STAGE_HOOK.with(|slot| {
        if let Some(hook) = slot.borrow_mut().as_mut() {
            hook(stage);
        }
    });
}

#[cfg(test)]
impl StorageNodeServer {
    pub(in crate::node_runtime) fn test_accept_one_replica_abandonment_hook(
        &self,
        hook: impl FnMut(&'static str) + 'static,
        lose_reply: bool,
    ) -> Result<(), StorageNodeServerError> {
        struct Reset(Option<StageHook>);
        impl Drop for Reset {
            fn drop(&mut self) {
                STAGE_HOOK.with(|slot| {
                    *slot.borrow_mut() = self.0.take();
                });
            }
        }
        let _reset = Reset(STAGE_HOOK.with(|slot| slot.replace(Some(Box::new(hook)))));
        self.test_accept_one_abandonment_fault(false, lose_reply)
    }

    pub(in crate::node_runtime) fn test_replica_abandonment_crossed_admission(
        &self,
        foreign: &Self,
        request: &CompleteReplicaAbandonmentRequest<'_>,
    ) {
        let handler = self.connection_handler();
        for (gate, class) in [
            (
                &foreign.route_admission,
                StorageNodeRouteAdmissionClass::Active,
            ),
            (
                &self.route_admission,
                StorageNodeRouteAdmissionClass::RetainedCleanup,
            ),
        ] {
            let permit = gate.acquire(class);
            let session =
                StorageNodeSession::new(Arc::clone(&self.read_handles), Arc::clone(&self._node));
            assert!(handler
                .with_replica_abandonment(
                    &session,
                    &permit,
                    request,
                    Instant::now() + Duration::from_secs(1),
                    |_, _| panic!("crossed admission reached mutation")
                )
                .is_err());
        }
    }
}
