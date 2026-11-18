// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Candidate complete-command maintenance; only owner-local tests select it.
//! Reuses installed authorization, primary serialization and mutation admission.
//! No replay, pending-slot removal or physical cleanup is authorized here.

use super::*;
use crate::node_runtime::pg_store::{CompleteLogMaintenanceRequest, CompleteMaintenanceError};

#[cfg(test)]
#[path = "complete_maintenance/api_tests.rs"]
mod api_tests;
#[path = "complete_maintenance/authority.rs"]
mod authority;
pub(in crate::node_runtime) use authority::{CompleteLogMutationAuthority, Mutation};

#[derive(Clone, Copy)]
pub(super) enum Operation {
    Export,
    Record,
    Compact,
}

impl StorageNodeConnectionHandler {
    pub(super) fn complete_metadata_command_maintenance_response(
        &self,
        session: &StorageNodeSession,
        request: StorageRpcMetadataCommandStateRequest,
        operation: Operation,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let admission = match operation {
            Operation::Export => self.validate_pg_route_for_metadata_log_read(
                request.node_id,
                request.cluster_epoch,
                request.pg_id,
            ),
            Operation::Record | Operation::Compact => self
                .validate_pg_route(request.node_id, request.cluster_epoch, request.pg_id)
                .and_then(|()| {
                    self.validate_primary_pg(request.pg_id, "complete metadata command maintenance")
                }),
        };
        if let Err(error) = admission {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = metadata_command_pg_guard_or_return!(self, session, request.pg_id);
        if matches!(operation, Operation::Record | Operation::Compact) {
            metadata_mutation_route_guard_or_return!(self);
        }
        let pg = match self.node.get_pg(request.pg_id.get()) {
            Ok(pg) => pg,
            Err(error) => return encode_storage_rpc_error_response(&store_error_response(error)),
        };
        if matches!(operation, Operation::Record | Operation::Compact) {
            // Waiting for the SQLite owner mutex must not bypass route expiry.
            metadata_mutation_route_guard_or_return!(self);
        }
        let scope = match CompleteLogMaintenanceRequest::new(
            self.config.node_id,
            request.pg_id,
            request.cluster_epoch,
            self.node.pg_topology(),
        ) {
            Ok(scope) => scope,
            Err(error) => return encode_storage_rpc_error_response(&store_error_response(error)),
        };
        let mutation = match operation {
            Operation::Export => None,
            Operation::Record => Some(Mutation::Record),
            Operation::Compact => Some(Mutation::Compact),
        };
        let authority = match mutation
            .map(|mutation| {
                CompleteLogMutationAuthority::admit(
                    self, session, &_pg_guard, &pg, &request, mutation,
                )
            })
            .transpose()
        {
            Ok(authority) => authority,
            Err(error) => return encode_storage_rpc_error_response(&error),
        };
        match operation {
            Operation::Export => match scope.checkpoint(&pg) {
                Ok(checkpoint) => {
                    let payload = encode_metadata_command_checkpoint_response(
                        &StorageRpcMetadataCommandCheckpointResponse { checkpoint },
                    )?;
                    encode_metadata_command_checkpoint_success_response(
                        "complete metadata command checkpoint export",
                        &payload,
                        STORAGE_RPC_MAX_PAYLOAD_LEN,
                    )
                }
                Err(error) => failure(error),
            },
            Operation::Record => {
                match scope.record_checkpoint(&pg, authority.as_ref().expect("record admitted")) {
                    Ok(checkpoint) => Ok(encode_storage_rpc_success_response(
                        &encode_metadata_command_state_response(
                            &StorageRpcMetadataCommandStateResponse {
                                state: crate::metadata_command::MetadataCommandReplicaState {
                                    cluster_epoch: checkpoint.cluster_epoch,
                                    applied_log_index: checkpoint.applied_log_index,
                                    applied_log_hash: checkpoint.applied_log_hash,
                                    state_digest: checkpoint.state_digest,
                                },
                            },
                        ),
                    )),
                    Err(error) => failure(error),
                }
            }
            Operation::Compact => {
                match scope.compact(&pg, authority.as_ref().expect("compaction admitted")) {
                    Ok(status) => Ok(encode_storage_rpc_success_response(
                        &encode_metadata_command_log_compact_response(
                            &StorageRpcMetadataCommandLogCompactResponse { status },
                        ),
                    )),
                    Err(error) => failure(error),
                }
            }
        }
    }
}

#[cfg(test)]
impl StorageNodeServer {
    pub(in crate::node_runtime) fn test_with_complete_maintenance_authority<T>(
        &self,
        request: StorageRpcMetadataCommandStateRequest,
        mutation: Mutation,
        action: impl FnOnce(
            &crate::node_runtime::pg_store::PgStore,
            &CompleteLogMutationAuthority<'_>,
        ) -> T,
    ) -> Result<T, StorageRpcErrorResponse> {
        let _route = self
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::Active);
        let handler = self.connection_handler();
        let session =
            StorageNodeSession::new(Arc::clone(&self.read_handles), Arc::clone(&self._node));
        let serialization = handler.metadata_command_pg_guard(&session, request.pg_id)?;
        let pg = self
            ._node
            .get_pg(request.pg_id.get())
            .map_err(store_error_response)?;
        let authority = CompleteLogMutationAuthority::admit(
            &handler,
            &session,
            &serialization,
            &pg,
            &request,
            mutation,
        )?;
        Ok(action(&pg, &authority))
    }
}

fn failure(
    error: CompleteMaintenanceError,
) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
    let response = match error {
        CompleteMaintenanceError::Store(error) => store_error_response(error),
        CompleteMaintenanceError::Snapshot(error) => StorageRpcErrorResponse {
            code: StorageRpcErrorCode::Internal,
            message: error.to_string(),
        },
    };
    encode_storage_rpc_error_response(&response)
}
