// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Candidate-only acknowledgement receiver. Descriptions and file receipts are
//! not mutation authority: this owner independently confirms the exact window,
//! releases the metadata section, and only then enters the data-PG transaction.
use super::complete_window_receiver::{ReceiverWindowPurpose, ReceiverWindowRequest};
use super::*;
use crate::node_runtime::payload_placement::PayloadPlacementInput;
use crate::node_runtime::pg_store::StagingWindowInspectionDescription;
use crate::storage_rpc::{
    complete_staging_ack as wire, complete_stream_allocation, complete_stream_file_primary,
};

#[cfg(test)]
thread_local! {
    static LOSE_REPLY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
pub(super) fn test_lose_reply(response: &StorageRpcFrame) -> bool {
    response.kind == StorageRpcMessageKind::StagingShardAckRecord
        && LOSE_REPLY.with(|slot| slot.replace(false))
}

#[cfg(test)]
impl StorageNodeServer {
    pub(in crate::node_runtime) fn test_lose_staging_ack_reply() {
        LOSE_REPLY.with(|slot| slot.set(true));
    }
}

impl StorageNodeConnectionHandler {
    pub(super) fn staging_ack_dispatch(
        &self,
        _session: &StorageNodeSession,
        _admission: &StorageNodeRouteAdmissionPermit,
        _payload: &[u8],
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        #[cfg(test)]
        if self.complete_log_operations {
            let result = self.record_staging_acknowledgements(_session, _admission, _payload);
            return match result {
                Ok(()) => Ok(encode_storage_rpc_success_response(&wire::encode_response(
                    _payload,
                )?)),
                Err(error) => encode_storage_rpc_error_response(&error),
            };
        }
        encode_storage_rpc_error_response(&StorageRpcErrorResponse {
            code: StorageRpcErrorCode::UnsupportedOperation,
            message: "staging shard acknowledgement record is not activated".into(),
        })
    }

    // Kept private: callers cannot substitute a prior scalar confirmation or
    // independently supplied rows for receiver admission and inspection.
    #[allow(dead_code)] // Live selection awaits the coordinated custody cutover.
    fn record_staging_acknowledgements(
        &self,
        session: &StorageNodeSession,
        admission: &StorageNodeRouteAdmissionPermit,
        payload: &[u8],
    ) -> Result<(), StorageRpcErrorResponse> {
        let started = Instant::now();
        let route_fence =
            StorageNodeRouteFence::current(&self.config, self.current_route_map_lease());
        let route_deadline = route_fence.local_valid_until_monotonic_ms.map(|end| {
            started
                .checked_add(Duration::from_millis(
                    end.saturating_sub(crate::clock::monotonic_time_millis()),
                ))
                .unwrap_or(started)
        });
        self.validate_active_admission(admission, "staging acknowledgement receiver")?;
        if session.has_metadata_command_pg_locks() {
            return Err(malformed(
                "staging acknowledgements require released metadata sections",
            ));
        }
        let request = wire::decode_request(payload).map_err(malformed)?;
        self.validate_node_epoch(request.receiver.node, request.receiver.epoch)?;
        if request.receiver.topology
            != crate::storage_rpc::complete_settled_prefix::topology_digest(self.node.pg_topology())
        {
            return Err(malformed("staging acknowledgement receiver topology"));
        }
        let stream = match request.provenance {
            wire::Provenance::Direct { .. } => None,
            wire::Provenance::Stream { inspection } => {
                Some(complete_stream_file_primary::decode_request(inspection).map_err(malformed)?)
            }
        };
        let (descriptor, size, purpose) = match request.provenance {
            wire::Provenance::Direct {
                descriptor,
                logical_size,
            } => (
                descriptor,
                logical_size,
                ReceiverWindowPurpose::DirectFileBinding,
            ),
            wire::Provenance::Stream { inspection } => {
                let files = stream.as_ref().expect("decoded stream provenance");
                let allocation = complete_stream_allocation::decode_request(files.allocation)
                    .map_err(malformed)?;
                (
                    allocation.window.descriptor,
                    files.segment.size,
                    ReceiverWindowPurpose::StreamFileBinding(inspection),
                )
            }
        };
        let description =
            StagingWindowInspectionDescription::decode(descriptor).map_err(malformed)?;
        let window = description
            .request(self.node.pg_topology())
            .map_err(malformed)?;
        let mut deadline = request.receiver.deadline.local_deadline();
        if let Some(route_deadline) = route_deadline {
            deadline = deadline.min(route_deadline);
        }
        let (deadline, window_fence) = window
            .bind_deadline(
                request.receiver.pg,
                self.node.pg_topology(),
                request.receiver.epoch,
                deadline,
            )
            .map_err(store_error_response)?;
        let cutoff = request.receiver.deadline.portable_wall_valid_until_ms;
        let fence = route_fence
            .intersect_effect_fence(window_fence)
            .and_then(|fence| {
                fence.intersect(AdmittedRouteEffectFence::bind_portable(
                    request.receiver.epoch,
                    cutoff,
                    cutoff,
                ))
            })
            .map_err(store_error_response)?;
        // Reconstruct placement from the installed configuration, not merely
        // the topology digest or participant identities claimed by the carrier.
        for receipt in &request.receipts {
            let placement = PayloadPlacementInput::from_server_config(&self.config);
            let length = usize::try_from(receipt.ack.stored_size).map_err(malformed)?;
            if let Some(files) = &stream {
                window.stream_file_location(placement, &files.segment, receipt.shard_index, length)
            } else {
                window.direct_file_location(placement, receipt.shard_index, size, length)
            }
            .map_err(malformed)?;
        }
        let receipts = request
            .receipts
            .iter()
            .map(|r| (r.shard_index, r.ack))
            .collect();
        let batch = window
            .acknowledgement_batch(size, &receipts)
            .map_err(malformed)?;
        let (pg, primary, epoch) = batch.destination();
        if primary != self.config.node_id || epoch != request.receiver.epoch {
            return Err(malformed("staging acknowledgement primary/epoch"));
        }
        self.validate_pg_route(primary, epoch, pg)?;
        self.validate_primary_pg(pg, "staging acknowledgement record")?;
        fence
            .require_valid_for(epoch)
            .map_err(store_error_response)?;
        self.inspect_receiver_window_for(
            admission,
            ReceiverWindowRequest {
                object_pg: request.receiver.pg,
                descriptor,
                cutoff: request.receiver.deadline,
            },
            deadline,
            purpose,
            #[cfg(test)]
            |_| {},
        )?;
        // Inspection acknowledges remote section release before returning.
        // No peer IO occurs while this local PG mutex/transaction is held.
        let store = self
            .node
            .get_pg_until(pg.get(), deadline)
            .map_err(store_error_response)?;
        let rows = batch
            .rows()
            .iter()
            .map(|(key, ack)| (key, *ack))
            .collect::<Vec<_>>();
        store
            .register_written_shards_batch_exact_with_effect_fence(&rows, fence)
            .map_err(store_error_response)
    }
}

fn malformed(error: impl std::fmt::Display) -> StorageRpcErrorResponse {
    StorageRpcErrorResponse {
        code: StorageRpcErrorCode::PayloadDecode,
        message: error.to_string(),
    }
}
