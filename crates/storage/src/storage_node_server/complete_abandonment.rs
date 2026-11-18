// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Candidate current or exact report-authorized historical unstarted abandonment. Sender custody
//! is not a wire certificate: independently enforce local admission,
//! an exact unstarted pending slot, no applied evidence and immutable deadlines.

use super::*;
use crate::node_runtime::pg_store::{
    CompleteAbandonmentQuery, CompleteAbandonmentRequest, PgStore,
};

#[cfg(test)]
#[path = "complete_abandonment/api_tests.rs"]
mod api_tests;
#[path = "complete_abandonment/authority.rs"]
mod authority;
pub(in crate::node_runtime) use authority::CompleteAbandonmentAuthority;

#[cfg(test)]
thread_local! {
    static LOSE_REPLY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static EXPIRE_AT_INSERT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
pub(super) fn test_lose_committed_reply(response: &StorageRpcFrame) -> bool {
    if response.kind != StorageRpcMessageKind::MetadataCommandRecordAbandoned
        || !LOSE_REPLY.with(|v| v.replace(false))
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
pub(in crate::node_runtime) fn test_abandonment_before_insert(
    request: &CompleteAbandonmentRequest<'_>,
) {
    if EXPIRE_AT_INSERT.with(std::cell::Cell::get) {
        let cutoff = request.bound_deadline(Instant::now() + Duration::from_secs(60));
        assert!(
            Instant::now() < cutoff,
            "expiry must occur inside the transaction"
        );
        while let Some(remaining) = cutoff.checked_duration_since(Instant::now()) {
            std::thread::sleep(remaining);
        }
    }
}

impl StorageNodeConnectionHandler {
    fn validate_abandonment_section(
        &self,
        session: &StorageNodeSession,
        route: &StorageNodeRouteAdmissionPermit,
        request: &CompleteAbandonmentRequest<'_>,
    ) -> Result<
        (StorageNodeMetadataCommandLockBinding, StorageNodeRouteFence),
        StorageRpcErrorResponse,
    > {
        self.validate_active_admission(route, "complete abandonment")?;
        let id = request.id();
        // Same exact primary/report rule as publication; old epoch alone is
        // never authority to record a primary tombstone. Capture the fence pre-wait.
        let (binding, fence) = self.complete_publication_route(request.publication_request())?;
        if !session.holds_metadata_command_pg_lock_with_binding(binding) {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::StaleShardLocation,
                message: "complete abandonment requires the exact held primary section".into(),
            });
        }
        let serialization = &session
            .metadata_command_guards
            .get(&id.pg_id())
            .expect("validated section")
            ._guard;
        if serialization.released
            || serialization.pg_id != id.pg_id()
            || !Arc::ptr_eq(
                &serialization.locks.state,
                &self.metadata_command_locks.state,
            )
        {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::Internal,
                message: "complete abandonment serialization belongs to another PG/domain".into(),
            });
        }
        Ok((binding, fence))
    }

    pub(super) fn metadata_command_record_abandonment_dispatch(
        &self,
        session: &StorageNodeSession,
        _route: &StorageNodeRouteAdmissionPermit,
        payload: &[u8],
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        #[cfg(test)]
        if self.complete_log_operations {
            if crate::storage_rpc::complete_replica_abandonment::is_request(payload) {
                return self.complete_replica_abandonment_response(session, _route, payload);
            }
            return self.complete_abandonment_response(session, _route, payload);
        }
        match decode_metadata_command_request(payload, &MetadataCommandDecodeAuthority::new()) {
            Ok(request) => self.metadata_command_record_abandoned_response(session, request),
            Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: error.to_string(),
            }),
        }
    }

    fn complete_abandonment_response(
        &self,
        session: &StorageNodeSession,
        route: &StorageNodeRouteAdmissionPermit,
        payload: &[u8],
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let now = Instant::now();
        let budget = now
            .checked_add(storage_node_rpc_io_timeout(self.rpc_auth.as_deref()))
            .unwrap_or(now);
        let query = match CompleteAbandonmentQuery::decode(payload, self.node.pg_topology()) {
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
            .with_abandonment(session, route, &request, budget, |pg, authority| {
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

    // Private, admitted owner. Production callers cannot supply a mutation
    // callback or construct authority through a sibling module.
    fn with_abandonment<T>(
        &self,
        session: &StorageNodeSession,
        route: &StorageNodeRouteAdmissionPermit,
        request: &CompleteAbandonmentRequest<'_>,
        budget: Instant,
        action: impl FnOnce(&PgStore, &CompleteAbandonmentAuthority<'_>) -> T,
    ) -> Result<T, StorageRpcErrorResponse> {
        let (binding, fence) = self.validate_abandonment_section(session, route, request)?;
        // Capture BEFORE waiting for SQLite. Same-epoch renewal cannot replace it.
        let now = Instant::now();
        let deadline = request.bound_deadline(budget);
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
        let pg = self
            .node
            .get_pg_until(request.id().pg_id().get(), deadline)
            .map_err(store_error_response)?;
        let authority = CompleteAbandonmentAuthority::admit(
            self,
            session,
            route,
            &pg,
            request,
            (binding, fence),
            deadline,
        )?;
        Ok(action(&pg, &authority))
    }
}

#[cfg(test)]
impl StorageNodeServer {
    pub(in crate::node_runtime) fn test_accept_one_abandonment_fault(
        &self,
        expire: bool,
        lose_reply: bool,
    ) -> Result<(), StorageNodeServerError> {
        struct Reset(bool, bool);
        impl Drop for Reset {
            fn drop(&mut self) {
                EXPIRE_AT_INSERT.with(|v| v.set(self.0));
                LOSE_REPLY.with(|v| v.set(self.1));
            }
        }
        let _reset = Reset(
            EXPIRE_AT_INSERT.with(|v| v.replace(expire)),
            LOSE_REPLY.with(|v| v.replace(lose_reply)),
        );
        self.accept_one()
    }

    pub(in crate::node_runtime) fn test_with_abandonment_authority<T>(
        &self,
        request: &CompleteAbandonmentRequest<'_>,
        deadline: Instant,
        action: impl FnOnce(&PgStore, &CompleteAbandonmentAuthority<'_>) -> T,
    ) -> Result<T, StorageRpcErrorResponse> {
        let handler = self.connection_handler();
        let active = self
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::Active);
        let mut session =
            StorageNodeSession::new(Arc::clone(&self.read_handles), Arc::clone(&self._node));
        let (binding, _) = handler.complete_publication_route(request.publication_request())?;
        session.acquire_metadata_command_pg_lock(
            &handler.metadata_command_locks,
            handler.config.node_id,
            binding,
            None,
        )?;
        handler.with_abandonment(&session, &active, request, deadline, action)
    }

    pub(in crate::node_runtime) fn test_abandonment_crossed_admission(
        &self,
        foreign: &Self,
        request: &CompleteAbandonmentRequest<'_>,
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
        let (expected, _) = handler
            .complete_publication_route(request.publication_request())
            .unwrap();
        for mode in [
            "exact",
            "missing",
            "wrong-pg",
            "wrong-epoch",
            "wrong-authority",
            "foreign-locks",
            "foreign-permit",
            "cleanup-permit",
        ] {
            let mut session =
                StorageNodeSession::new(Arc::clone(&self.read_handles), Arc::clone(&self._node));
            let id = request.id();
            if mode != "missing" {
                session
                    .acquire_metadata_command_pg_lock(
                        if mode == "foreign-locks" {
                            &foreign.metadata_command_locks
                        } else {
                            &self.metadata_command_locks
                        },
                        handler.config.node_id,
                        StorageNodeMetadataCommandLockBinding {
                            pg_id: if mode == "wrong-pg" {
                                PgId::new(id.pg_id().get() + 1)
                            } else {
                                id.pg_id()
                            },
                            cluster_epoch: if mode == "wrong-epoch" {
                                ClusterEpoch::new(id.cluster_epoch().get() + 1).unwrap()
                            } else {
                                id.cluster_epoch()
                            },
                            authority: if mode == "wrong-authority" {
                                match expected.authority {
                                    StorageNodeMetadataCommandLockAuthority::CurrentPrimary => StorageNodeMetadataCommandLockAuthority::HistoricalRecoveryPrimary,
                                    StorageNodeMetadataCommandLockAuthority::HistoricalRecoveryPrimary => StorageNodeMetadataCommandLockAuthority::CurrentPrimary,
                                }
                            } else {
                                expected.authority
                            },
                        },
                        None,
                    )
                    .unwrap();
            }
            let permit = match mode {
                "foreign-permit" => &foreign_permit,
                "cleanup-permit" => &cleanup,
                _ => &active,
            };
            let result = handler.with_abandonment(
                &session,
                permit,
                request,
                Instant::now() + Duration::from_secs(30),
                |_, _| (),
            );
            assert_eq!(result.is_ok(), mode == "exact", "{mode}: {result:?}");
        }
    }
}
