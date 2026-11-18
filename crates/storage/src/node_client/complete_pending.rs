// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Complete pending inspection, not a raw command or a submission constructor.
//! Embedded and Unix clients share the durable owner's request-bound validator.

use super::*;
use crate::node_runtime::pg_store::{
    CompleteAbandonedEvidence, CompleteAcceptanceEvidence, CompleteAcceptanceKind,
    CompleteAppliedEvidence, CompletePendingEvidence, CompletePendingInspectionError,
    CompletePendingRequest,
};

impl LocalStorageNodeClient {
    pub(in crate::node_runtime) fn complete_command_acceptance_until<'a>(
        &self,
        pending: &'a CompletePendingEvidence,
        kind: CompleteAcceptanceKind,
        deadline: Instant,
    ) -> Result<CompleteAcceptanceEvidence<'a>, CompletePendingInspectionError> {
        let probe =
            pending.acceptance_probe(self.node_id, self.storage_node.pg_topology(), kind)?;
        let pg = self
            .storage_node
            .get_pg_until(probe.id().pg_id().get(), deadline)?;
        if Instant::now() >= deadline {
            return Err(
                storage_rpc_deadline_expired("local complete acceptance deadline expired").into(),
            );
        }
        probe.read(&pg)
    }

    pub(in crate::node_runtime) fn complete_abandoned_command_until<'a>(
        &self,
        pending: &'a CompletePendingEvidence,
        deadline: Instant,
    ) -> Result<CompleteAbandonedEvidence<'a>, CompletePendingInspectionError> {
        let probe = pending.abandoned_probe(self.node_id, self.storage_node.pg_topology())?;
        let pg = self
            .storage_node
            .get_pg_until(probe.id().pg_id().get(), deadline)?;
        if Instant::now() >= deadline {
            return Err(storage_rpc_deadline_expired(
                "local complete abandoned inspection deadline expired",
            )
            .into());
        }
        probe.read(&pg)
    }

    pub(in crate::node_runtime) fn complete_applied_command_until<'a>(
        &self,
        pending: &'a CompletePendingEvidence,
        deadline: Instant,
    ) -> Result<CompleteAppliedEvidence<'a>, CompletePendingInspectionError> {
        let probe = pending.applied_probe(self.node_id, self.storage_node.pg_topology())?;
        let pg = self
            .storage_node
            .get_pg_until(probe.id().pg_id().get(), deadline)?;
        if Instant::now() >= deadline {
            return Err(storage_rpc_deadline_expired(
                "local complete applied inspection deadline expired",
            )
            .into());
        }
        probe.read(&pg)
    }

    pub(in crate::node_runtime) fn complete_pending_command_until(
        &self,
        pg_id: PgId,
        epoch: ClusterEpoch,
        deadline: Instant,
    ) -> Result<CompletePendingEvidence, CompletePendingInspectionError> {
        let scope = CompletePendingRequest::new(pg_id, epoch, self.storage_node.pg_topology())?;
        let pg = self.storage_node.get_pg_until(pg_id.get(), deadline)?;
        if Instant::now() >= deadline {
            return Err(storage_rpc_deadline_expired(
                "local complete pending inspection deadline expired",
            )
            .into());
        }
        scope.read(&pg, self.node_id.as_u32())
    }
}

impl UnixStorageNodeClient {
    pub(in crate::node_runtime) fn complete_command_acceptance_until<'a>(
        &self,
        pending: &'a CompletePendingEvidence,
        kind: CompleteAcceptanceKind,
        deadline: Instant,
    ) -> Result<CompleteAcceptanceEvidence<'a>, StoreError> {
        const OPERATION: &str = "complete command acceptance";
        let topology = self.pg_topology.as_deref().ok_or_else(|| {
            self.rpc_payload_error(
                OPERATION,
                "complete acceptance requires installed topology".into(),
            )
        })?;
        let probe = pending
            .acceptance_probe(self.node_id, topology, kind)
            .map_err(|error| self.rpc_payload_error(OPERATION, error.to_string()))?;
        if probe.id().cluster_epoch() > self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: probe.id().pg_id().get(),
                operation_epoch: probe.id().cluster_epoch(),
                current_epoch: self.cluster_epoch,
            });
        }
        let payload = probe
            .encode_request()
            .map_err(|error| self.rpc_payload_error(OPERATION, error.to_string()))?;
        let response = self.rpc_request_until(kind.message_kind(), payload, deadline)?;
        probe.decode_response(&response)
    }

    pub(in crate::node_runtime) fn complete_abandoned_command_until<'a>(
        &self,
        pending: &'a CompletePendingEvidence,
        deadline: Instant,
    ) -> Result<CompleteAbandonedEvidence<'a>, StoreError> {
        const OPERATION: &str = "complete abandoned command probe";
        let topology = self.pg_topology.as_deref().ok_or_else(|| {
            self.rpc_payload_error(
                OPERATION,
                "complete abandoned probe requires installed topology".into(),
            )
        })?;
        let probe = pending
            .abandoned_probe(self.node_id, topology)
            .map_err(|error| self.rpc_payload_error(OPERATION, error.to_string()))?;
        if probe.id().cluster_epoch() > self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: probe.id().pg_id().get(),
                operation_epoch: probe.id().cluster_epoch(),
                current_epoch: self.cluster_epoch,
            });
        }
        let payload = probe
            .encode_request()
            .map_err(|error| self.rpc_payload_error(OPERATION, error.to_string()))?;
        let response = self.rpc_request_until(
            StorageRpcMessageKind::MetadataCommandAbandoned,
            payload,
            deadline,
        )?;
        probe.decode_response(&response)
    }

    pub(in crate::node_runtime) fn complete_applied_command_until<'a>(
        &self,
        pending: &'a CompletePendingEvidence,
        deadline: Instant,
    ) -> Result<CompleteAppliedEvidence<'a>, StoreError> {
        const OPERATION: &str = "complete applied command probe";
        let topology = self.pg_topology.as_deref().ok_or_else(|| {
            self.rpc_payload_error(
                OPERATION,
                "complete applied probe requires installed topology".into(),
            )
        })?;
        let probe = pending
            .applied_probe(self.node_id, topology)
            .map_err(|error| self.rpc_payload_error(OPERATION, error.to_string()))?;
        if probe.id().cluster_epoch() > self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: probe.id().pg_id().get(),
                operation_epoch: probe.id().cluster_epoch(),
                current_epoch: self.cluster_epoch,
            });
        }
        let payload = probe
            .encode_request()
            .map_err(|error| self.rpc_payload_error(OPERATION, error.to_string()))?;
        let response = self.rpc_request_until(
            StorageRpcMessageKind::MetadataCommandAppliedLogHashes,
            payload,
            deadline,
        )?;
        probe.decode_response(&response)
    }

    pub(in crate::node_runtime) fn complete_pending_command_until(
        &self,
        pg_id: PgId,
        epoch: ClusterEpoch,
        deadline: Instant,
    ) -> Result<CompletePendingEvidence, StoreError> {
        const OPERATION: &str = "decode complete pending command response";
        if epoch > self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        let topology = self.pg_topology.as_deref().ok_or_else(|| {
            self.rpc_payload_error(
                OPERATION,
                "complete pending read requires installed topology".into(),
            )
        })?;
        let scope = CompletePendingRequest::new(pg_id, epoch, topology)
            .map_err(|error| self.rpc_payload_error(OPERATION, error.to_string()))?;
        let bytes = self.rpc_request_until(
            StorageRpcMessageKind::MetadataCommandPendingEnvelope,
            encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
                node_id: self.node_id,
                cluster_epoch: epoch,
                pg_id,
            }),
            deadline,
        )?;
        scope
            .decode_response(&bytes)
            .map_err(|error| self.rpc_payload_error(OPERATION, error.to_string()))
    }
}
