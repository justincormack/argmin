// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Installed candidate receiver, explicitly selected only by owner tests.
//! Current and exact report-authorized historical admission; no live selection.

use super::*;
use crate::node_runtime::pg_store::CompleteApplicationQuery;
use crate::storage_rpc::complete_application as wire;

#[cfg(test)]
thread_local! {
    static EXPIRE_AFTER_DECODE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static LOSE_COMMITTED_REPLY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
pub(super) fn test_lose_committed_reply(response: &StorageRpcFrame) -> bool {
    if response.kind != StorageRpcMessageKind::MetadataCommandApplyAndRecord
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
    pub(super) fn metadata_command_application_dispatch(
        &self,
        session: &StorageNodeSession,
        _route: &StorageNodeRouteAdmissionPermit,
        payload: &[u8],
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        #[cfg(test)]
        if self.complete_log_operations {
            return self.complete_metadata_command_application_response(session, _route, payload);
        }
        match decode_metadata_command_request(
            payload,
            &crate::node_runtime::MetadataCommandDecodeAuthority::new(),
        ) {
            Ok(request) => self.metadata_command_apply_and_record_response(session, request),
            Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: error.to_string(),
            }),
        }
    }

    fn complete_metadata_command_application_response(
        &self,
        session: &StorageNodeSession,
        route: &StorageNodeRouteAdmissionPermit,
        payload: &[u8],
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let now = Instant::now();
        let budget = now
            .checked_add(storage_node_rpc_io_timeout(self.rpc_auth.as_deref()))
            .unwrap_or(now);
        let query = match CompleteApplicationQuery::decode(payload, self.node.pg_topology()) {
            Ok(query) => query,
            Err(error) => {
                return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: error.to_string(),
                })
            }
        };
        #[cfg(test)]
        EXPIRE_AFTER_DECODE.with(|expire| {
            if expire.get() {
                let cutoff = query.deadline();
                assert!(
                    Instant::now() < cutoff,
                    "expiry canary must reach a live decoded cutoff"
                );
                while let Some(remaining) = cutoff.checked_duration_since(Instant::now()) {
                    std::thread::sleep(remaining);
                }
                assert!(
                    Instant::now() < budget,
                    "wire expiry must precede the independent server work budget"
                );
            }
        });
        let result = query.with_request(budget, |request| {
            let state = self
                .apply_complete_command(session, route, request)?
                .map_err(|error| match error.into_store_error() {
                    Ok(error) => store_error_response(error),
                    Err(error) => StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::Internal,
                        message: error.to_string(),
                    },
                })?;
            Ok::<_, StorageRpcErrorResponse>(wire::encode_confirmation(wire::Confirmation {
                node_id: request.node_id(),
                id: request.id(),
                command_sha256: request.command_sha256(),
                state,
            }))
        })?;
        match result {
            Ok(payload) => Ok(encode_storage_rpc_success_response(&payload)),
            Err(error) => encode_storage_rpc_error_response(&error),
        }
    }
}

#[cfg(test)]
impl StorageNodeServer {
    pub(in crate::node_runtime) fn test_lose_application_reply() {
        LOSE_COMMITTED_REPLY.with(|slot| slot.set(true));
    }

    pub(in crate::node_runtime) fn test_accept_one_complete_application_fault(
        &self,
        expire_after_decode: bool,
        lose_committed_reply: bool,
    ) -> Result<(), StorageNodeServerError> {
        struct Reset(bool, bool);
        impl Drop for Reset {
            fn drop(&mut self) {
                EXPIRE_AFTER_DECODE.with(|slot| slot.set(self.0));
                LOSE_COMMITTED_REPLY.with(|slot| slot.set(self.1));
            }
        }
        let _reset = Reset(
            EXPIRE_AFTER_DECODE.with(|slot| slot.replace(expire_after_decode)),
            LOSE_COMMITTED_REPLY.with(|slot| slot.replace(lose_committed_reply)),
        );
        self.accept_one()
    }
}
