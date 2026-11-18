// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Complete-command retained reads, selected only by owner-local installed
//! tests until coordinated activation. Authentication, frame admission, route
//! validation and metadata-command serialization are shared with live dispatch.

use super::*;
use crate::node_runtime::pg_store::{
    CompleteLogHashRangeRequest, CompleteLogRangeRequest, CompleteLogReadError,
};

impl StorageNodeConnectionHandler {
    pub(super) fn complete_metadata_command_log_hash_range_response(
        &self,
        session: &StorageNodeSession,
        request: StorageRpcMetadataCommandLogHashRangeRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route_for_metadata_log_read(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = metadata_command_pg_guard_or_return!(self, session, request.pg_id);
        let scope = CompleteLogHashRangeRequest::new(
            request.pg_id,
            request.cluster_epoch,
            request.first_log_index,
            request.last_log_index,
            self.node.pg_topology(),
        )?;
        let result = self
            .node
            .get_pg(request.pg_id.get())
            .map_err(CompleteLogReadError::Store)
            .and_then(|pg| scope.read_response(&pg, self.config.node_id.as_u32()));
        complete_log_response(
            StorageRpcMessageKind::MetadataCommandRetainedLogHashes,
            &request,
            result,
        )
    }

    pub(super) fn complete_metadata_command_log_entry_range_response(
        &self,
        session: &StorageNodeSession,
        request: StorageRpcMetadataCommandLogHashRangeRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_pg_route_for_metadata_log_read(
            request.node_id,
            request.cluster_epoch,
            request.pg_id,
        ) {
            return encode_storage_rpc_error_response(&error);
        }
        let _pg_guard = metadata_command_pg_guard_or_return!(self, session, request.pg_id);
        let scope = CompleteLogRangeRequest::new(
            request.pg_id,
            request.cluster_epoch,
            request.first_log_index,
            request.last_log_index,
            self.node.pg_topology(),
        )?;
        let result = self
            .node
            .get_pg(request.pg_id.get())
            .map_err(CompleteLogReadError::Store)
            .and_then(|pg| scope.read_response(&pg, self.config.node_id.as_u32()));
        complete_log_response(
            StorageRpcMessageKind::MetadataCommandRetainedLogEntries,
            &request,
            result,
        )
    }
}

fn complete_log_response(
    kind: StorageRpcMessageKind,
    request: &StorageRpcMetadataCommandLogHashRangeRequest,
    result: Result<Vec<u8>, CompleteLogReadError>,
) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
    let error = match result {
        Ok(payload) => return Ok(encode_storage_rpc_success_response(&payload)),
        Err(CompleteLogReadError::Store(error)) => store_error_response(error),
        Err(error @ CompleteLogReadError::Snapshot(_)) => StorageRpcErrorResponse {
            code: StorageRpcErrorCode::Internal,
            message: error.to_string(),
        },
        Err(error @ CompleteLogReadError::Response(_)) => StorageRpcErrorResponse {
            code: StorageRpcErrorCode::PayloadDecode,
            message: error.to_string(),
        },
        Err(CompleteLogReadError::ValidationBudgetExceeded { next_log_index, .. }) => {
            // A typed outcome, not a page: retry the known safe prefix from the
            // SAME start. Do not make the client guess by repeated halving.
            return crate::storage_rpc::complete_log_budget::encode_response(
                kind,
                request,
                next_log_index,
            );
        }
    };
    encode_storage_rpc_error_response(&error)
}
