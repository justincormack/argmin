// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Candidate capture and continuation of one retained receiver submission.
//! Preparation is read-only; installation mutates only the exact pending slot
//! and side indexes. Neither operation selects publication or physical effects.
use super::*;

#[cfg(test)]
thread_local! {
    static LOSE_REPLY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static INSTALL_FAULT: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
}
#[cfg(test)]
pub(in crate::node_runtime::server) fn test_lose_reply(response: &StorageRpcFrame) -> bool {
    matches!(
        response.kind,
        StorageRpcMessageKind::StagingDirectIntroductionPrepare
            | StorageRpcMessageKind::StagingDirectIntroductionInstall
    ) && LOSE_REPLY.with(|slot| slot.replace(false))
}
#[cfg(test)]
impl StorageNodeServer {
    pub(in crate::node_runtime) fn test_introduction_install_fault(lose_confirmation: bool) {
        INSTALL_FAULT.with(|slot| slot.set(Some(lose_confirmation)));
    }
    pub(in crate::node_runtime) fn test_lose_direct_introduction_reply() {
        LOSE_REPLY.with(|slot| slot.set(true));
    }
}

impl StorageNodeConnectionHandler {
    pub(in crate::node_runtime::server) fn staging_direct_introduction_install_dispatch(
        &self,
        session: &StorageNodeSession,
        admission: &StorageNodeRouteAdmissionPermit,
        bytes: &[u8],
    ) -> Result<Vec<u8>, StorageRpcPayloadError> {
        #[cfg(test)]
        if self.complete_log_operations {
            let action = || -> Result<Vec<u8>, StorageRpcErrorResponse> {
                use crate::storage_rpc::complete_direct_introduction_install as install;
                self.validate_active_admission(admission, "direct introduction installation")?;
                let invalid = |error: StorageRpcPayloadError| StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: error.to_string(),
                };
                let request = install::decode_request(bytes).map_err(invalid)?;
                // Prepare confirmation before effects. It reports success only
                // after installation of the ORIGINAL connection-owned holder.
                let confirmation = install::confirmation(bytes).map_err(invalid)?;
                let run = || {
                    self.install_parked_direct_introduction(
                        session,
                        admission,
                        request.preparation,
                        request.response,
                    )
                };
                match INSTALL_FAULT.with(|slot| slot.replace(None)) {
                    Some(true) => PgStore::test_with_lost_complete_install_confirmation(run),
                    Some(false) => {
                        let logical = wire::decode_request(request.preparation).map_err(invalid)?;
                        let encoded = wire::encode_request(&logical).map_err(invalid)?;
                        let cutoff = encoded
                            .decode_response(request.response)
                            .map_err(invalid)?
                            .cutoff
                            .portable_wall_valid_until_ms;
                        let wall = crate::clock::current_time_millis();
                        let monotonic = crate::clock::monotonic_time_millis();
                        let clock = std::rc::Rc::new(std::cell::RefCell::new(None));
                        let retained = std::rc::Rc::clone(&clock);
                        let result = PgStore::test_with_before_pending_slot_effect_hook(
                            move |in_transaction| {
                                assert!(in_transaction);
                                *retained.borrow_mut() =
                                    Some(crate::clock::test_time_and_monotonic_override_guard(
                                        cutoff + 100,
                                        monotonic + cutoff.saturating_sub(wall) + 100,
                                    ));
                            },
                            run,
                        );
                        assert!(
                            clock.borrow().is_some(),
                            "expiry must reach the final SQLite effect boundary"
                        );
                        result
                    }
                    None => run(),
                }?;
                Ok(confirmation)
            };
            return match action() {
                Ok(body) => Ok(encode_storage_rpc_success_response(&body)),
                Err(error) => encode_storage_rpc_error_response(&error),
            };
        }
        let _ = (session, admission, bytes);
        encode_storage_rpc_error_response(&StorageRpcErrorResponse {
            code: StorageRpcErrorCode::UnsupportedOperation,
            message: "staging direct introduction installation is not activated".into(),
        })
    }

    pub(in crate::node_runtime::server) fn staging_direct_introduction_dispatch(
        &self,
        session: &StorageNodeSession,
        admission: &StorageNodeRouteAdmissionPermit,
        bytes: &[u8],
    ) -> Result<Vec<u8>, StorageRpcPayloadError> {
        #[cfg(test)]
        if self.complete_log_operations {
            let action = || -> Result<Vec<u8>, StorageRpcErrorResponse> {
                self.validate_active_admission(admission, "direct introduction preparation")?;
                let query =
                    IntroductionQuery::decode(bytes).map_err(|error| StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    })?;
                let response = self
                    .prepare_direct_introduction(session, admission, &query, |_| {})?
                    .encode_response()?;
                let owner =
                    self.claim_direct_introduction(session, admission, &query, &response)?;
                owner.park().map_err(|failure| StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::Internal,
                    message: failure.error().to_string(),
                })?;
                Ok(response)
            };
            return match action() {
                Ok(body) => Ok(encode_storage_rpc_success_response(&body)),
                Err(error) => encode_storage_rpc_error_response(&error),
            };
        }
        let _ = (session, admission, bytes);
        encode_storage_rpc_error_response(&StorageRpcErrorResponse {
            code: StorageRpcErrorCode::UnsupportedOperation,
            message: "staging direct introduction preparation is not activated".into(),
        })
    }
}
