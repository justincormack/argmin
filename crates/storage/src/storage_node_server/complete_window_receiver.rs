// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Independent receiver lookup, never a transferable physical grant. No local
//! PG/file lock spans network IO. The receiver privately owns the primary session
//! for the whole witness-first exchange and closes it before returning.

use super::*;
use crate::node_runtime::pg_store::{
    StagingWindowConfirmationObservation, StagingWindowInspectionDescription,
};
use crate::storage_rpc::{
    complete_settled_prefix as witness, complete_window_primary as primary,
    StorageRpcOperationDeadline,
};

fn peer_error(error: StoreError) -> StorageRpcErrorResponse {
    if let StoreError::StorageRpc { failure, .. } = &error {
        StorageRpcErrorResponse {
            code: *failure,
            message: error.to_string(),
        }
    } else {
        store_error_response(error)
    }
}

pub(super) struct ReceiverWindowRequest<'a> {
    pub(super) object_pg: PgId,
    pub(super) descriptor: &'a [u8],
    pub(super) cutoff: StorageRpcOperationDeadline,
}

pub(super) enum ReceiverWindowPurpose<'a> {
    RetainedWindow,
    DirectFileBinding,
    StreamFileBinding(&'a [u8]),
}

impl StorageNodeConnectionHandler {
    pub(super) fn inspect_receiver_window(
        &self,
        admission: &StorageNodeRouteAdmissionPermit,
        request: ReceiverWindowRequest<'_>,
        work_deadline: Instant,
        #[cfg(test)] hook: impl FnMut(&'static str),
    ) -> Result<StagingWindowConfirmationObservation, StorageRpcErrorResponse> {
        self.inspect_receiver_window_for(
            admission,
            request,
            work_deadline,
            ReceiverWindowPurpose::RetainedWindow,
            #[cfg(test)]
            hook,
        )
    }

    pub(super) fn inspect_receiver_window_for(
        &self,
        admission: &StorageNodeRouteAdmissionPermit,
        request: ReceiverWindowRequest<'_>,
        work_deadline: Instant,
        purpose: ReceiverWindowPurpose<'_>,
        #[cfg(test)] mut hook: impl FnMut(&'static str),
    ) -> Result<StagingWindowConfirmationObservation, StorageRpcErrorResponse> {
        let ReceiverWindowRequest {
            object_pg,
            descriptor,
            cutoff,
        } = request;
        let peers = self.window_confirmation_selection(admission, object_pg)?;
        let fence = StorageNodeRouteFence::current(&self.config, self.route_map_lease);
        let started = Instant::now();
        let malformed =
            |error: crate::storage_rpc::StorageRpcPayloadError| StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: error.to_string(),
            };
        // Bind the additional stream inspection to this exact lookup.
        let stream = if let ReceiverWindowPurpose::StreamFileBinding(bytes) = &purpose {
            let files = crate::storage_rpc::complete_stream_file_primary::decode_request(bytes)
                .map_err(malformed)?;
            let allocation =
                crate::storage_rpc::complete_stream_allocation::decode_request(files.allocation)
                    .map_err(malformed)?;
            let route = allocation.window.route;
            if route.node != peers.primary.node_id()
                || route.epoch != self.config.cluster_epoch
                || route.pg != object_pg
                || route.topology != witness::topology_digest(self.node.pg_topology())
                || allocation.window.descriptor != descriptor
                || route.deadline.portable_wall_valid_until_ms < cutoff.portable_wall_valid_until_ms
            {
                return Err(StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: "receiver stream inspection subject".into(),
                });
            }
            Some((files, allocation))
        } else {
            None
        };
        // Capture authority before decoding or waiting; a raw-map renewal cannot
        // lengthen this lookup. The inbound absolute upper bound is never rebuilt.
        let mut deadline = work_deadline.min(cutoff.local_deadline());
        if let Some(end) = fence.local_valid_until_monotonic_ms {
            deadline = deadline.min(
                started
                    .checked_add(Duration::from_millis(
                        end.saturating_sub(crate::clock::monotonic_time_millis()),
                    ))
                    .unwrap_or(started),
            );
        }
        if descriptor.is_empty() || descriptor.len() > primary::MAX_DESCRIPTOR_BYTES {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: "receiver window descriptor size".into(),
            });
        }
        let description = StagingWindowInspectionDescription::decode(descriptor).map_err(|e| {
            StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: e.to_string(),
            }
        })?;
        let window =
            description
                .request(self.node.pg_topology())
                .map_err(|e| StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: e.to_string(),
                })?;
        let epoch = self.config.cluster_epoch;
        let (deadline, window_fence) = window
            .bind_deadline(object_pg, self.node.pg_topology(), epoch, deadline)
            .map_err(store_error_response)?;
        let portable = StorageRpcOperationDeadline {
            portable_wall_valid_until_ms: cutoff
                .portable_wall_valid_until_ms
                .min(
                    StorageRpcOperationDeadline::from_instant(deadline)
                        .portable_wall_valid_until_ms,
                )
                .min(
                    window_fence
                        .deadline()
                        .expect("durable window is bounded")
                        .portable_wall_valid_until_ms(),
                ),
        };
        // IO timeout is a local waiting limit, not a fresh cross-host authority
        // lease. Exporting that one-second limit with one second of skew would
        // make every forwarded request expire before dispatch. Both peers also
        // impose their own bounded IO wait; the wire retains the authority cap.
        let deadline = deadline.min(
            started
                .checked_add(storage_node_rpc_io_timeout(self.rpc_auth.as_deref()))
                .unwrap_or(started),
        );
        let require = || -> Result<(), StorageRpcErrorResponse> {
            fence.validate_rpc_at(
                crate::clock::current_time_millis(),
                crate::clock::monotonic_time_millis(),
            )?;
            window_fence
                .require_valid_for(epoch)
                .map_err(store_error_response)?;
            if Instant::now() >= deadline {
                return Err(store_error_response(
                    StoreError::OperationDeadlineExceeded {
                        context: "receiver window confirmation",
                    },
                ));
            }
            // Includes exact configuration capture and admission-domain checks.
            self.window_confirmation_selection(admission, object_pg)?;
            Ok(())
        };
        require()?;
        let session = peers
            .primary
            .open_metadata_command_critical_section_until(object_pg, deadline)
            .map_err(peer_error)?;
        let mut held = session
            .borrow_complete_primary(peers.primary, object_pg, epoch, deadline)
            .map_err(store_error_response)?;
        #[cfg(test)]
        hook("before witness");
        require()?;
        let witnessed = peers
            .witness
            .map(|peer| {
                peer.complete_settled_witness_prefix_with_cutoff_until(
                    object_pg, epoch, deadline, portable,
                )
            })
            .transpose()
            .map_err(peer_error)?;
        #[cfg(test)]
        hook("after witness");
        require()?;
        let witness_id = peers.witness.map(|peer| peer.node_id());
        let inspection = primary::Request {
            route: witness::Request {
                node: peers.primary.node_id(),
                epoch,
                pg: object_pg,
                deadline: portable,
                topology: witness::topology_digest(self.node.pg_topology()),
            },
            descriptor,
        };
        let prefix = match purpose {
            ReceiverWindowPurpose::RetainedWindow | ReceiverWindowPurpose::StreamFileBinding(_) => {
                held.inspect_window_primary(inspection, witness_id)
            }
            ReceiverWindowPurpose::DirectFileBinding => {
                held.inspect_direct_file_primary(inspection, witness_id)
            }
        }
        .map_err(peer_error)?;
        if let Some((files, mut allocation)) = stream {
            if allocation.prefix != prefix {
                return Err(StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::MetadataCommandContention,
                    message: "receiver stream allocation prefix differs".into(),
                });
            }
            // Shorten the original portable authority; never allocate again.
            allocation.window.route.deadline = portable;
            let allocation =
                crate::storage_rpc::complete_stream_allocation::encode_request(&allocation)
                    .map_err(malformed)?;
            let bytes = crate::storage_rpc::complete_stream_file_primary::encode_request(
                &allocation,
                &files.segment,
            )
            .map_err(malformed)?;
            held.inspect_stream_file_primary_as_peer(&bytes)
                .map_err(peer_error)?;
        }
        #[cfg(test)]
        hook("after primary");
        require()?;
        if witnessed.is_some_and(|other| other != prefix) {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::MetadataCommandContention,
                message: "receiver window primary and designated witness prefixes differ".into(),
            });
        }
        held.release_until().map_err(peer_error)?;
        drop(session); // Already closed after the bounded release exchange.
        #[cfg(test)]
        hook("after section close");
        require()?;
        Ok(StagingWindowConfirmationObservation::settled(
            prefix, witness_id,
        ))
    }
}
