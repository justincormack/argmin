// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Complete pending inspection shares authentication, captured-route validation
//! and metadata-command serialization with the installed current handler.

use super::*;
use crate::node_runtime::pg_store::{
    CompleteAbandonedQuery, CompleteAcceptanceKind, CompleteAcceptanceQuery, CompleteAppliedQuery,
    CompletePendingInspectionError, CompletePendingRequest,
};

impl StorageNodeConnectionHandler {
    pub(super) fn metadata_command_acceptance_dispatch(
        &self,
        session: &StorageNodeSession,
        payload: &[u8],
        kind: CompleteAcceptanceKind,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        #[cfg(test)]
        if self.complete_log_operations {
            return self.complete_metadata_command_acceptance_response(session, payload, kind);
        }
        match decode_metadata_command_request(payload, &MetadataCommandDecodeAuthority::new()) {
            Ok(request) => match kind {
                CompleteAcceptanceKind::Apply => {
                    self.metadata_command_acceptance_response(session, request)
                }
                CompleteAcceptanceKind::Abandon => {
                    self.metadata_command_abandon_acceptance_response(session, request)
                }
            },
            Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: error.to_string(),
            }),
        }
    }

    pub(super) fn complete_metadata_command_acceptance_response(
        &self,
        session: &StorageNodeSession,
        payload: &[u8],
        kind: CompleteAcceptanceKind,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let query = match CompleteAcceptanceQuery::decode(payload, self.node.pg_topology(), kind) {
            Ok(query) => query,
            Err(error) => {
                return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: error.to_string(),
                })
            }
        };
        let id = query.id();
        if let Err(error) = self.validate_pg_route_for_metadata_command_recovery(
            query.node_id(),
            id.cluster_epoch(),
            id.pg_id(),
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let _guard = metadata_command_pg_guard_or_return!(self, session, id.pg_id());
        let result = self
            .node
            .get_pg(id.pg_id().get())
            .map_err(CompletePendingInspectionError::Store)
            .and_then(|pg| query.read_response(&pg));
        complete_pending_response(result)
    }

    pub(super) fn metadata_command_abandoned_dispatch(
        &self,
        session: &StorageNodeSession,
        payload: &[u8],
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        #[cfg(test)]
        if self.complete_log_operations {
            return self.complete_metadata_command_abandoned_response(session, payload);
        }
        match decode_metadata_command_request(payload, &MetadataCommandDecodeAuthority::new()) {
            Ok(request) => self.metadata_command_abandoned_response(session, request),
            Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: error.to_string(),
            }),
        }
    }

    pub(super) fn complete_metadata_command_abandoned_response(
        &self,
        session: &StorageNodeSession,
        payload: &[u8],
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let query = match CompleteAbandonedQuery::decode(payload, self.node.pg_topology()) {
            Ok(query) => query,
            Err(error) => {
                return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: error.to_string(),
                })
            }
        };
        let id = query.id();
        if let Err(error) = self.validate_pg_route_for_metadata_command_recovery(
            query.node_id(),
            id.cluster_epoch(),
            id.pg_id(),
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let _guard = metadata_command_pg_guard_or_return!(self, session, id.pg_id());
        let result = self
            .node
            .get_pg(id.pg_id().get())
            .map_err(CompletePendingInspectionError::Store)
            .and_then(|pg| query.read_response(&pg));
        complete_pending_response(result)
    }

    pub(super) fn metadata_command_applied_hashes_dispatch(
        &self,
        session: &StorageNodeSession,
        payload: &[u8],
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        #[cfg(test)]
        if self.complete_log_operations {
            return self.complete_metadata_command_applied_response(session, payload);
        }
        match decode_metadata_command_request(payload, &MetadataCommandDecodeAuthority::new()) {
            Ok(request) => self.metadata_command_applied_hashes_response(session, request),
            Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: error.to_string(),
            }),
        }
    }

    pub(super) fn complete_metadata_command_applied_response(
        &self,
        session: &StorageNodeSession,
        payload: &[u8],
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        let query = match CompleteAppliedQuery::decode(payload, self.node.pg_topology()) {
            Ok(query) => query,
            Err(error) => {
                return encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: error.to_string(),
                })
            }
        };
        let id = query.id();
        if let Err(error) = self.validate_pg_route_for_metadata_command_recovery(
            query.node_id(),
            id.cluster_epoch(),
            id.pg_id(),
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let _guard = metadata_command_pg_guard_or_return!(self, session, id.pg_id());
        let result = self
            .node
            .get_pg(id.pg_id().get())
            .map_err(CompletePendingInspectionError::Store)
            .and_then(|pg| query.read_response(&pg));
        complete_pending_response(result)
    }

    pub(super) fn complete_metadata_command_pending_response(
        &self,
        session: &StorageNodeSession,
        request: StorageRpcMetadataCommandStateRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route_for_metadata_log_read(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let _guard = metadata_command_pg_guard_or_return!(self, session, request.pg_id);
        let scope = CompletePendingRequest::new(
            request.pg_id,
            request.cluster_epoch,
            self.node.pg_topology(),
        )?;
        let result = self
            .node
            .get_pg(request.pg_id.get())
            .map_err(CompletePendingInspectionError::Store)
            .and_then(|pg| scope.read_response(&pg, self.config.node_id.as_u32()));
        complete_pending_response(result)
    }
}

fn complete_pending_response(
    result: Result<Vec<u8>, CompletePendingInspectionError>,
) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
    let error = match result {
        Ok(payload) => return Ok(encode_storage_rpc_success_response(&payload)),
        Err(CompletePendingInspectionError::Store(error)) => store_error_response(error),
        Err(error @ CompletePendingInspectionError::Snapshot(_)) => StorageRpcErrorResponse {
            code: StorageRpcErrorCode::Internal,
            message: error.to_string(),
        },
        Err(error @ CompletePendingInspectionError::Response(_)) => StorageRpcErrorResponse {
            code: StorageRpcErrorCode::PayloadDecode,
            message: error.to_string(),
        },
    };
    encode_storage_rpc_error_response(&error)
}
