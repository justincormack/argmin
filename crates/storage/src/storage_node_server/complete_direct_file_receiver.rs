// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Candidate receiver-owned file effects. No live RPC dispatch yet.
//! Confirmation is performed here, not supplied as a transferable observation.
//! All network IO finishes before entering the existing file-mutation exclusion.
use super::complete_window_receiver::{ReceiverWindowPurpose, ReceiverWindowRequest};
use super::*;
use crate::node_runtime::payload_placement::PayloadPlacementInput;
use crate::node_runtime::pg_store::StagingWindowInspectionDescription;
use crate::storage_rpc::{
    complete_stream_allocation, complete_stream_file_primary, StorageRpcOperationDeadline,
};

#[cfg(test)]
thread_local! {
    static LOSE_REPLY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
pub(super) fn test_lose_reply(response: &StorageRpcFrame) -> bool {
    response.kind == StorageRpcMessageKind::StagingShardWrite
        && LOSE_REPLY.with(|slot| slot.replace(false))
}

pub(super) struct DirectFileRequest<'a> {
    pub(super) window: ReceiverWindowRequest<'a>,
    pub(super) shard_index: u8,
    pub(super) logical_size: u64,
    pub(super) bytes: &'a [u8],
}

enum FileShape<'a> {
    Direct(u64),
    Stream {
        segment: &'a StreamUploadSegmentRecord,
        inspection: &'a [u8],
    },
}

struct FileRequest<'a> {
    window: ReceiverWindowRequest<'a>,
    shape: FileShape<'a>,
    shard_index: u8,
    bytes: &'a [u8],
}

impl StorageNodeConnectionHandler {
    pub(super) fn staging_shard_write_dispatch(
        &self,
        _session: &StorageNodeSession,
        _admission: &StorageNodeRouteAdmissionPermit,
        _payload: &[u8],
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        #[cfg(test)]
        if self.complete_log_operations {
            return self.complete_staging_file_response(_session, _admission, _payload);
        }
        encode_storage_rpc_error_response(&StorageRpcErrorResponse {
            code: StorageRpcErrorCode::UnsupportedOperation,
            message: "staging shard write is not activated".into(),
        })
    }

    pub(super) fn complete_staging_file_response(
        &self,
        session: &StorageNodeSession,
        admission: &StorageNodeRouteAdmissionPermit,
        payload: &[u8],
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        use crate::storage_rpc::complete_staging_file as wire;
        // Capture route authority before parsing any nested provenance. A
        // concurrent same-epoch renewal must not extend this file effect.
        let route_fence =
            StorageNodeRouteFence::current(&self.config, self.current_route_map_lease());
        let malformed =
            |error: crate::storage_rpc::StorageRpcPayloadError| StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: error.to_string(),
            };
        let result = (|| {
            self.validate_active_admission(admission, "staging file dispatch")?;
            if session.has_metadata_command_pg_locks() {
                return Err(StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: "staging file dispatch requires released metadata sections".into(),
                });
            }
            let request = wire::decode_request(payload).map_err(malformed)?;
            self.validate_node_epoch(request.receiver.node, request.receiver.epoch)?;
            if request.receiver.topology
                != crate::storage_rpc::complete_settled_prefix::topology_digest(
                    self.node.pg_topology(),
                )
            {
                return Err(StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: "staging file receiver topology".into(),
                });
            }
            let deadline = request.receiver.deadline.local_deadline();
            let stream = match request.provenance {
                wire::Provenance::Direct { .. } => None,
                wire::Provenance::Stream { inspection } => Some(
                    complete_stream_file_primary::decode_request(inspection).map_err(malformed)?,
                ),
            };
            let (descriptor, shape) = match request.provenance {
                wire::Provenance::Direct {
                    descriptor,
                    logical_size,
                } => (descriptor, FileShape::Direct(logical_size)),
                wire::Provenance::Stream { inspection } => {
                    let files = stream.as_ref().expect("decoded stream provenance");
                    let allocation = complete_stream_allocation::decode_request(files.allocation)
                        .map_err(malformed)?;
                    (
                        allocation.window.descriptor,
                        FileShape::Stream {
                            segment: &files.segment,
                            inspection,
                        },
                    )
                }
            };
            let ack = self.write_file_receiver(
                admission,
                FileRequest {
                    window: ReceiverWindowRequest {
                        object_pg: request.receiver.pg,
                        descriptor,
                        // Do not replace a shorter outer cutoff with the
                        // original stream-allocation carrier's longer one.
                        cutoff: request.receiver.deadline,
                    },
                    shape,
                    shard_index: request.shard_index,
                    bytes: request.bytes,
                },
                route_fence,
                deadline,
                #[cfg(test)]
                |_| {},
            )?;
            wire::encode_response(payload, ack).map_err(malformed)
        })();
        match result {
            Ok(reply) => Ok(encode_storage_rpc_success_response(&reply)),
            Err(error) => encode_storage_rpc_error_response(&error),
        }
    }

    pub(super) fn write_direct_file_receiver(
        &self,
        admission: &StorageNodeRouteAdmissionPermit,
        request: DirectFileRequest<'_>,
        work_deadline: Instant,
        #[cfg(test)] hook: impl FnMut(&'static str),
    ) -> Result<WriteAck, StorageRpcErrorResponse> {
        let route_fence =
            StorageNodeRouteFence::current(&self.config, self.current_route_map_lease());
        self.write_file_receiver(
            admission,
            FileRequest {
                window: request.window,
                shape: FileShape::Direct(request.logical_size),
                shard_index: request.shard_index,
                bytes: request.bytes,
            },
            route_fence,
            work_deadline,
            #[cfg(test)]
            hook,
        )
    }

    pub(super) fn write_stream_file_receiver(
        &self,
        admission: &StorageNodeRouteAdmissionPermit,
        inspection: &[u8],
        shard: (u8, &[u8]),
        work_deadline: Instant,
        #[cfg(test)] hook: impl FnMut(&'static str),
    ) -> Result<WriteAck, StorageRpcErrorResponse> {
        self.validate_active_admission(admission, "stream staging file receiver")?;
        let route_fence =
            StorageNodeRouteFence::current(&self.config, self.current_route_map_lease());
        let malformed =
            |error: crate::storage_rpc::StorageRpcPayloadError| StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: error.to_string(),
            };
        let files = complete_stream_file_primary::decode_request(inspection).map_err(malformed)?;
        let allocation =
            complete_stream_allocation::decode_request(files.allocation).map_err(malformed)?;
        self.write_file_receiver(
            admission,
            FileRequest {
                window: ReceiverWindowRequest {
                    object_pg: allocation.window.route.pg,
                    descriptor: allocation.window.descriptor,
                    cutoff: allocation.window.route.deadline,
                },
                shape: FileShape::Stream {
                    segment: &files.segment,
                    inspection,
                },
                shard_index: shard.0,
                bytes: shard.1,
            },
            route_fence,
            work_deadline,
            #[cfg(test)]
            hook,
        )
    }

    fn write_file_receiver(
        &self,
        admission: &StorageNodeRouteAdmissionPermit,
        request: FileRequest<'_>,
        route_fence: StorageNodeRouteFence,
        work_deadline: Instant,
        #[cfg(test)] mut hook: impl FnMut(&'static str),
    ) -> Result<WriteAck, StorageRpcErrorResponse> {
        self.validate_active_admission(admission, "staging file receiver")?;
        // Capture before decoding, confirmation or waiting. Neither a renewal nor
        // an intervening successful inspection may extend this effect's authority.
        let epoch = self.config.cluster_epoch;
        let decode_error =
            |error: crate::node_runtime::pg_store::StagingWindowConfirmationError| {
                StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: error.to_string(),
                }
            };
        let description = StagingWindowInspectionDescription::decode(request.window.descriptor)
            .map_err(decode_error)?;
        let window = description
            .request(self.node.pg_topology())
            .map_err(decode_error)?;
        let deadline = work_deadline.min(request.window.cutoff.local_deadline());
        let (deadline, window_fence) = window
            .bind_deadline(
                request.window.object_pg,
                self.node.pg_topology(),
                epoch,
                deadline,
            )
            .map_err(store_error_response)?;
        let cutoff =
            request.window.cutoff.portable_wall_valid_until_ms.min(
                StorageRpcOperationDeadline::from_instant(deadline).portable_wall_valid_until_ms,
            );
        let fence = route_fence
            .intersect_effect_fence(window_fence)
            .and_then(|fence| {
                fence.intersect(AdmittedRouteEffectFence::bind_portable(
                    epoch, cutoff, cutoff,
                ))
            })
            .map_err(store_error_response)?;
        let placement = PayloadPlacementInput::from_server_config(&self.config);
        let (location, purpose) = match request.shape {
            FileShape::Direct(size) => (
                window.direct_file_location(
                    placement,
                    request.shard_index,
                    size,
                    request.bytes.len(),
                ),
                ReceiverWindowPurpose::DirectFileBinding,
            ),
            FileShape::Stream {
                segment,
                inspection,
            } => (
                window.stream_file_location(
                    placement,
                    segment,
                    request.shard_index,
                    request.bytes.len(),
                ),
                ReceiverWindowPurpose::StreamFileBinding(inspection),
            ),
        };
        let (location, key) = location.map_err(decode_error)?;
        if location.node_id() != self.config.node_id {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: "staging file receiver participant".into(),
            });
        }
        self.validate_pg_route(
            self.config.node_id,
            epoch,
            PgId::new(location.data_pg_id().get()),
        )?;
        fence
            .require_valid_for(epoch)
            .map_err(store_error_response)?;
        self.inspect_receiver_window_for(
            admission,
            request.window,
            deadline,
            purpose,
            #[cfg(test)]
            &mut hook,
        )?;
        #[cfg(test)]
        hook("before physical write");
        fence
            .require_valid_for(epoch)
            .map_err(store_error_response)?;
        // No PG mutex is held. The primitive rechecks this same fence after
        // acquiring shared file exclusion and holds exclusion through the ack.
        self.node
            .write_shard_file_if_absent_with_effect_fence(
                location.data_pg_id().get(),
                &key,
                request.bytes,
                fence,
            )
            .map_err(store_error_response)
    }
}

#[cfg(test)]
impl StorageNodeServer {
    pub(in crate::node_runtime) fn test_lose_staging_file_reply() {
        LOSE_REPLY.with(|slot| slot.set(true));
    }

    pub(in crate::node_runtime) fn test_write_stream_file_receiver(
        &self,
        endpoints: Vec<(
            NodeId,
            crate::storage_rpc_transport::StorageRpcClientEndpoint,
        )>,
        auth: crate::StorageNodeStorageRpcClientCapability,
        inspection: &[u8],
        shard: (u8, &[u8]),
        deadline: Instant,
        hook: impl FnMut(&'static str),
    ) -> Result<WriteAck, StorageRpcErrorResponse> {
        self.with_test_window_receiver(endpoints, auth, |handler, permit| {
            handler.write_stream_file_receiver(permit, inspection, shard, deadline, hook)
        })
    }

    pub(in crate::node_runtime) fn test_write_direct_file_receiver(
        &self,
        endpoints: Vec<(
            NodeId,
            crate::storage_rpc_transport::StorageRpcClientEndpoint,
        )>,
        auth: crate::StorageNodeStorageRpcClientCapability,
        request: crate::storage_rpc::complete_window_primary::Request<'_>,
        shard: (u8, u64, &[u8]),
        deadline: Instant,
        hook: impl FnMut(&'static str),
    ) -> Result<WriteAck, StorageRpcErrorResponse> {
        self.with_test_window_receiver(endpoints, auth, |handler, permit| {
            handler.write_direct_file_receiver(
                permit,
                DirectFileRequest {
                    window: ReceiverWindowRequest {
                        object_pg: request.route.pg,
                        descriptor: request.descriptor,
                        cutoff: request.route.deadline,
                    },
                    shard_index: shard.0,
                    logical_size: shard.1,
                    bytes: shard.2,
                },
                deadline,
                hook,
            )
        })
    }
}
