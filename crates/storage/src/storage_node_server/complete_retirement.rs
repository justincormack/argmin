// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Candidate current or exact report-authorized historical quiescent retirement. Sender all-member
//! custody is not a wire certificate: independently enforce local admission,
//! complete-context eligibility, exact Applied evidence and immutable deadlines.

use super::*;
use crate::node_runtime::pg_store::{CompleteRetirementQuery, CompleteRetirementRequest, PgStore};

#[cfg(test)]
#[path = "complete_retirement/api_tests.rs"]
mod api_tests;
#[path = "complete_retirement/authority.rs"]
mod authority;
pub(in crate::node_runtime) use authority::CompleteRetirementAuthority;

#[cfg(test)]
thread_local! {
    static LOSE_REPLY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static EXPIRE_AT_DELETE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static DROP_EXPIRED_REPLY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
impl StorageNodeServer {
    pub(in crate::node_runtime) fn test_lose_retirement_reply() {
        LOSE_REPLY.with(|v| v.set(true));
    }

    pub(in crate::node_runtime) fn test_expire_retirement_before_delete() {
        EXPIRE_AT_DELETE.with(|v| v.set(true));
        DROP_EXPIRED_REPLY.with(|v| v.set(true));
    }
}

#[cfg(test)]
pub(super) fn test_drop_expired_reply(response: &StorageRpcFrame) -> bool {
    if response.kind != StorageRpcMessageKind::MetadataCommandPendingSlotRemove
        || !DROP_EXPIRED_REPLY.with(|v| v.replace(false))
    {
        return false;
    }
    // The sender deliberately expires while the pre-delete hook is waiting.
    // Assert the actual rejection, then close instead of racing an error write
    // against its already-expired socket. Never mask another server failure.
    assert!(
        !EXPIRE_AT_DELETE.with(std::cell::Cell::get),
        "expiry must reach the pre-delete hook"
    );
    let error = crate::storage_rpc::decode_storage_rpc_response_payload(&response.payload)
        .unwrap()
        .expect_err("expired retirement must be rejected");
    assert_eq!(error.code, StorageRpcErrorCode::TransportTimeout);
    assert!(
        error.message.contains("complete retirement"),
        "{}",
        error.message
    );
    true
}

#[cfg(test)]
pub(super) fn test_lose_committed_reply(response: &StorageRpcFrame) -> bool {
    if response.kind != StorageRpcMessageKind::MetadataCommandPendingSlotRemove
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
pub(in crate::node_runtime) fn test_retirement_before_delete(
    request: &CompleteRetirementRequest<'_>,
) {
    if EXPIRE_AT_DELETE.with(|v| v.replace(false)) {
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
    fn validate_retirement_section(
        &self,
        session: &StorageNodeSession,
        route: &StorageNodeRouteAdmissionPermit,
        request: &CompleteRetirementRequest<'_>,
    ) -> Result<
        (StorageNodeMetadataCommandLockBinding, StorageNodeRouteFence),
        StorageRpcErrorResponse,
    > {
        self.validate_active_admission(route, "complete retirement")?;
        let id = request.id();
        // Same exact primary/report rule as publication; old epoch alone is
        // never authority to remove a pending slot. Capture the fence pre-wait.
        let (binding, fence) = self.complete_publication_route(request.publication_request())?;
        if !session.holds_metadata_command_pg_lock_with_binding(binding) {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::StaleShardLocation,
                message: "complete retirement requires the exact held primary section".into(),
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
                message: "complete retirement serialization belongs to another PG/domain".into(),
            });
        }
        Ok((binding, fence))
    }

    pub(super) fn metadata_command_retirement_dispatch(
        &self,
        session: &StorageNodeSession,
        _route: &StorageNodeRouteAdmissionPermit,
        payload: &[u8],
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        #[cfg(test)]
        if self.complete_log_operations {
            return self.complete_retirement_response(session, _route, payload);
        }
        match decode_metadata_command_pending_slot_request(
            payload,
            &MetadataCommandDecodeAuthority::new(),
        ) {
            Ok(request) => self.metadata_command_pending_slot_remove_response(session, request),
            Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: error.to_string(),
            }),
        }
    }

    fn complete_retirement_response(
        &self,
        session: &StorageNodeSession,
        route: &StorageNodeRouteAdmissionPermit,
        payload: &[u8],
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let now = Instant::now();
        let budget = now
            .checked_add(storage_node_rpc_io_timeout(self.rpc_auth.as_deref()))
            .unwrap_or(now);
        let query = match CompleteRetirementQuery::decode(payload, self.node.pg_topology()) {
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
            .with_retirement(session, route, &request, budget, |pg, authority| {
                request.retire_response(pg, authority).map_err(|error| {
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
    fn with_retirement<T>(
        &self,
        session: &StorageNodeSession,
        route: &StorageNodeRouteAdmissionPermit,
        request: &CompleteRetirementRequest<'_>,
        budget: Instant,
        action: impl FnOnce(&PgStore, &CompleteRetirementAuthority<'_>) -> T,
    ) -> Result<T, StorageRpcErrorResponse> {
        let (binding, fence) = self.validate_retirement_section(session, route, request)?;
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
        let authority = CompleteRetirementAuthority::admit(
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
    pub(in crate::node_runtime) fn test_accept_one_retirement_fault(
        &self,
        expire: bool,
        lose_reply: bool,
    ) -> Result<(), StorageNodeServerError> {
        struct Reset(bool, bool);
        impl Drop for Reset {
            fn drop(&mut self) {
                EXPIRE_AT_DELETE.with(|v| v.set(self.0));
                LOSE_REPLY.with(|v| v.set(self.1));
            }
        }
        let _reset = Reset(
            EXPIRE_AT_DELETE.with(|v| v.replace(expire)),
            LOSE_REPLY.with(|v| v.replace(lose_reply)),
        );
        self.accept_one()
    }

    pub(in crate::node_runtime) fn test_with_retirement_authority<T>(
        &self,
        request: &CompleteRetirementRequest<'_>,
        deadline: Instant,
        action: impl FnOnce(&PgStore, &CompleteRetirementAuthority<'_>) -> T,
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
        handler.with_retirement(&session, &active, request, deadline, action)
    }

    pub(in crate::node_runtime) fn test_retirement_crossed_admission(
        &self,
        foreign: &Self,
        request: &CompleteRetirementRequest<'_>,
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
            let result = handler.with_retirement(
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
