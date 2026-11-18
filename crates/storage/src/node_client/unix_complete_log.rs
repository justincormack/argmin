// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Candidate read-only integration, not selected by live recovery or transfer.
//! Pages retain the actual successfully inspected interval. Contexts and hash
//! observations remain with their PG-store owner, not recovery authority.

use super::*;
use crate::node_runtime::pg_store::{
    CompleteLogHashPage, CompleteLogHashRangeRequest, CompleteLogPage, CompleteLogRangeRequest,
};

#[derive(Clone, Copy)]
enum CompleteLogRead {
    Entries,
    Hashes,
}

impl CompleteLogRead {
    fn kind(self) -> StorageRpcMessageKind {
        match self {
            Self::Entries => StorageRpcMessageKind::MetadataCommandRetainedLogEntries,
            Self::Hashes => StorageRpcMessageKind::MetadataCommandRetainedLogHashes,
        }
    }
}

impl UnixStorageNodeClient {
    pub(in crate::node_runtime) fn complete_metadata_log_page_until(
        &self,
        pg_id: PgId,
        epoch: ClusterEpoch,
        first: MetadataCommandLogIndex,
        last: MetadataCommandLogIndex,
        deadline: Instant,
    ) -> Result<CompleteLogPage, StoreError> {
        const OPERATION: &str = "decode complete metadata command retained log entries response";
        let topology = self.complete_log_read_topology(pg_id, epoch, OPERATION)?;
        // Validate before any transport. Each subsequent reduction is a strict
        // subset; decode against the actual reduced interval, never the original.
        CompleteLogRangeRequest::new(pg_id, epoch, first, last, topology)
            .map_err(|error| self.rpc_payload_error(OPERATION, error.to_string()))?;
        let (last, bytes) = self.complete_log_response_until(
            CompleteLogRead::Entries,
            StorageRpcMetadataCommandLogHashRangeRequest {
                node_id: self.node_id,
                cluster_epoch: epoch,
                pg_id,
                first_log_index: first,
                last_log_index: last,
            },
            deadline,
        )?;
        CompleteLogRangeRequest::new(pg_id, epoch, first, last, topology)
            .and_then(|scope| scope.decode_page(&bytes))
            .map_err(|error| self.rpc_payload_error(OPERATION, error.to_string()))
    }

    pub(in crate::node_runtime) fn complete_metadata_log_hash_page_until(
        &self,
        pg_id: PgId,
        epoch: ClusterEpoch,
        first: MetadataCommandLogIndex,
        last: MetadataCommandLogIndex,
        deadline: Instant,
    ) -> Result<CompleteLogHashPage, StoreError> {
        const OPERATION: &str = "decode complete metadata command retained log hashes response";
        let topology = self.complete_log_read_topology(pg_id, epoch, OPERATION)?;
        CompleteLogHashRangeRequest::new(pg_id, epoch, first, last, topology)
            .map_err(|error| self.rpc_payload_error(OPERATION, error.to_string()))?;
        let (last, bytes) = self.complete_log_response_until(
            CompleteLogRead::Hashes,
            StorageRpcMetadataCommandLogHashRangeRequest {
                node_id: self.node_id,
                cluster_epoch: epoch,
                pg_id,
                first_log_index: first,
                last_log_index: last,
            },
            deadline,
        )?;
        CompleteLogHashRangeRequest::new(pg_id, epoch, first, last, topology)
            .and_then(|scope| scope.decode_page(&bytes))
            .map_err(|error| self.rpc_payload_error(OPERATION, error.to_string()))
    }

    fn complete_log_read_topology(
        &self,
        pg_id: PgId,
        epoch: ClusterEpoch,
        operation: &'static str,
    ) -> Result<&PgTopology, StoreError> {
        if epoch > self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        self.pg_topology.as_deref().ok_or_else(|| {
            self.rpc_payload_error(
                operation,
                "complete log read requires installed topology".into(),
            )
        })
    }

    fn complete_log_response_until(
        &self,
        read: CompleteLogRead,
        mut request: StorageRpcMetadataCommandLogHashRangeRequest,
        deadline: Instant,
    ) -> Result<(MetadataCommandLogIndex, Vec<u8>), StoreError> {
        let mut retried = false;
        loop {
            // Admission, transport and every reduced retry share one immutable
            // deadline. General capacity failures never trigger range retries.
            let result = self
                .rpc_request_result_until(
                    read.kind(),
                    encode_metadata_command_log_hash_range_request(&request),
                    deadline,
                )
                .map_err(StorageRpcRequestDispatchFailure::into_source)?;
            match result {
                Ok(bytes) => {
                    let Some(end) = crate::storage_rpc::complete_log_budget::decode_hint(
                        read.kind(),
                        &request,
                        &bytes,
                    )
                    .map_err(|error| {
                        self.rpc_payload_error(
                            "decode complete log validation budget hint",
                            error.to_string(),
                        )
                    })?
                    else {
                        return Ok((request.last_log_index, bytes));
                    };
                    if retried {
                        // Retained rows should not grow. A changed view or faulty
                        // peer must not restart a validation/reduction chain.
                        return Err(self.rpc_response_error(
                            read.kind(),
                            StorageRpcErrorResponse {
                                code: StorageRpcErrorCode::RetainedLogValidationBudgetExceeded,
                                message: "complete log prefix exceeded validation budget on retry"
                                    .into(),
                            },
                        ));
                    }
                    request.last_log_index = end;
                    retried = true;
                }
                Err(error) => return Err(self.rpc_response_error(read.kind(), error)),
            }
        }
    }
}
