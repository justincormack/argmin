// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[cfg(test)]
thread_local! {
    static INTRODUCTION_RETIRE_BEFORE_DISPATCH: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
    static INTRODUCTION_INSTALL_BEFORE_DISPATCH: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
    static DIRECT_INSTALL_BEFORE_DISPATCH: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
    static APPEND_INSTALL_BEFORE_DISPATCH: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
    static APPEND_PUBLISH_BEFORE_DISPATCH: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
    static APPEND_APPLY_BEFORE_DISPATCH: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
}

#[cfg(test)]
impl UnixStorageNodeMetadataCommandSession {
    pub(in crate::node_runtime) fn test_introduction_retire_before_dispatch(&self, unwind: bool) {
        INTRODUCTION_RETIRE_BEFORE_DISPATCH.with(|slot| slot.set(Some(unwind)));
    }
    pub(in crate::node_runtime) fn test_introduction_install_before_dispatch(&self, unwind: bool) {
        INTRODUCTION_INSTALL_BEFORE_DISPATCH.with(|slot| slot.set(Some(unwind)));
    }
    pub(in crate::node_runtime) fn test_direct_install_before_dispatch(&self, unwind: bool) {
        DIRECT_INSTALL_BEFORE_DISPATCH.with(|slot| slot.set(Some(unwind)));
    }
    pub(in crate::node_runtime) fn test_append_install_before_dispatch(&self, unwind: bool) {
        APPEND_INSTALL_BEFORE_DISPATCH.with(|slot| slot.set(Some(unwind)));
    }
    pub(in crate::node_runtime) fn test_append_publish_before_dispatch(&self, unwind: bool) {
        APPEND_PUBLISH_BEFORE_DISPATCH.with(|slot| slot.set(Some(unwind)));
    }
    pub(in crate::node_runtime) fn test_append_apply_before_dispatch(&self, unwind: bool) {
        APPEND_APPLY_BEFORE_DISPATCH.with(|slot| slot.set(Some(unwind)));
    }
}

pub(crate) struct UnixStorageNodeReadHandleSession {
    node_id: NodeId,
    route_cluster_epoch: ClusterEpoch,
    connection: StorageRpcRequestConnection,
    next_request_id: u64,
    io_timeout: Duration,
    last_connection_reusable: bool,
    active_read_operation_ids: BTreeSet<String>,
    rpc_auth: Option<Arc<StorageRpcClientAuthConfig>>,
    _rpc_permit: Option<UnixStorageNodeRpcAdmissionPermit>,
    _object_payload_lease_permit: Option<UnixStorageNodeObjectPayloadLeaseAdmissionPermit>,
}

pub(crate) struct UnixStorageNodeMetadataCommandSession {
    origin: Arc<UnixStorageNodeClientInstance>,
    identity: Arc<()>,
    node_id: NodeId,
    cluster_epoch: ClusterEpoch,
    pg_id: PgId,
    rpc_auth: Option<Arc<StorageRpcClientAuthConfig>>,
    _rpc_permit: UnixStorageNodeRpcAdmissionPermit,
    inner: Mutex<UnixStorageNodeMetadataCommandSessionInner>,
}

/// Process-local identity of one acquired section, not its client or endpoint.
/// Retaining the allocation prevents address reuse after the session closes.
pub(crate) struct MetadataCommandSessionIdentity(Arc<()>);

struct UnixStorageNodeMetadataCommandSessionInner {
    stream: BoxStorageRpcStream,
    next_request_id: u64,
    io_timeout: Duration,
    released: bool,
}

/// Keeps the exact acquired session alive and excludes concurrent transport or
/// shutdown while an embedded replica applies or a witness is inspected. This
/// is not evidence that a
/// remote process cannot fail: primary confirmation is still separately needed.
pub(crate) struct CompletePrimarySessionBorrow<'a> {
    inner: std::sync::MutexGuard<'a, UnixStorageNodeMetadataCommandSessionInner>,
    session: &'a UnixStorageNodeMetadataCommandSession,
    deadline: Instant,
}

/// Successful preparation has already claimed receiver custody. Until the
/// owning decoder accepts it, every error/unwind must invalidate this exact
/// connection rather than strand an inaccessible preparation on a reusable one.
pub(in crate::node_runtime) struct UnvalidatedDirectIntroductionReply<'a> {
    held: CompletePrimarySessionBorrow<'a>,
    bytes: Vec<u8>,
    validated: bool,
}

impl UnvalidatedDirectIntroductionReply<'_> {
    pub(in crate::node_runtime) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(in crate::node_runtime) fn finish_validation(mut self) -> Result<Vec<u8>, StoreError> {
        require_metadata_command_operation_deadline(self.held.deadline)?;
        if self.held.inner.released {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "direct introduction preparation lost held section",
            });
        }
        self.validated = true;
        Ok(std::mem::take(&mut self.bytes))
    }
}

impl Drop for UnvalidatedDirectIntroductionReply<'_> {
    fn drop(&mut self) {
        if !self.validated {
            self.held.inner.released = true;
            let _ = self.held.inner.stream.shutdown(std::net::Shutdown::Both);
        }
    }
}

impl<'a> CompletePrimarySessionBorrow<'a> {
    pub(in crate::node_runtime) fn install_direct_introduction(
        &mut self,
        attempt: crate::node_runtime::pg_store::RemoteDirectIntroductionAttempt<'_, '_>,
    ) -> Result<(), crate::node_runtime::pg_store::StagingWindowConfirmationError> {
        if let Err(error) = attempt.require_session(self) {
            return Err(attempt.not_sent(error));
        }
        let kind = StorageRpcMessageKind::StagingDirectIntroductionInstall;
        let deadline = self.deadline.min(attempt.deadline());
        #[cfg(test)]
        let deadline = match INTRODUCTION_INSTALL_BEFORE_DISPATCH.with(|slot| slot.replace(None)) {
            Some(true) => panic!("injected introduction installation unwind before transport"),
            Some(false) => Instant::now(),
            None => deadline,
        };
        match self.session.rpc_request_result_locked(
            &mut self.inner,
            kind,
            attempt.bytes().to_vec(),
            deadline,
        ) {
            Err(StorageRpcRequestDispatchFailure::NotSent(error)) => Err(attempt.not_sent(error)),
            Err(StorageRpcRequestDispatchFailure::MayHaveApplied(error)) => {
                Err(attempt.unconfirmed(error))
            }
            Ok(Err(error)) => {
                Err(attempt.unconfirmed(self.session.rpc_response_error(kind, error)))
            }
            Ok(Ok(reply)) => attempt.confirm(&reply),
        }
    }

    pub(in crate::node_runtime) fn prepare_direct_introduction(
        mut self,
        request: &crate::storage_rpc::complete_direct_introduction::EncodedRequest,
    ) -> Result<UnvalidatedDirectIntroductionReply<'a>, StoreError> {
        let route = request.route();
        if route.node != self.session.node_id
            || route.pg != self.session.pg_id
            || route.epoch != self.session.cluster_epoch
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "direct introduction original primary section",
            });
        }
        let kind = StorageRpcMessageKind::StagingDirectIntroductionPrepare;
        let bytes = self
            .session
            .rpc_request_result_locked(
                &mut self.inner,
                kind,
                request.bytes().to_vec(),
                self.deadline,
            )
            .map_err(StorageRpcRequestDispatchFailure::into_source)?
            .map_err(|e| self.session.rpc_response_error(kind, e))?;
        Ok(UnvalidatedDirectIntroductionReply {
            held: self,
            bytes,
            validated: false,
        })
    }
    pub(in crate::node_runtime) fn install_direct_terminal(
        &mut self,
        attempt: crate::node_runtime::pg_store::RemoteDirectInstallationAttempt<'_, '_>,
    ) -> Result<(), crate::node_runtime::pg_store::StagingWindowConfirmationError> {
        if let Err(error) = attempt.require_session(self) {
            return Err(attempt.not_sent(error));
        }
        let kind = StorageRpcMessageKind::StagingDirectInstall;
        let deadline = self.deadline.min(attempt.deadline());
        #[cfg(test)]
        let deadline = match DIRECT_INSTALL_BEFORE_DISPATCH.with(|slot| slot.replace(None)) {
            Some(true) => panic!("injected direct installation unwind before transport"),
            Some(false) => Instant::now(),
            None => deadline,
        };
        match self.session.rpc_request_result_locked(
            &mut self.inner,
            kind,
            attempt.bytes().to_vec(),
            deadline,
        ) {
            Err(StorageRpcRequestDispatchFailure::NotSent(error)) => {
                Err(attempt.not_sent(error.into()))
            }
            Err(StorageRpcRequestDispatchFailure::MayHaveApplied(error)) => {
                Err(attempt.unconfirmed(error))
            }
            Ok(Err(error)) => {
                Err(attempt.unconfirmed(self.session.rpc_response_error(kind, error)))
            }
            Ok(Ok(reply)) => attempt.confirm(&reply),
        }
    }
    pub(in crate::node_runtime) fn prepare_direct_terminal(
        &mut self,
        client: &UnixStorageNodeClient,
        permit: &crate::node_runtime::pg_store::DirectTerminalPreparation<'_, '_>,
    ) -> Result<
        crate::node_runtime::pg_store::CapturedDirectTerminal,
        crate::node_runtime::pg_store::StagingWindowConfirmationError,
    > {
        self.session.require_origin(client)?;
        permit.require_remote_session(self)?;
        let request = permit.remote_request()?;
        let kind = StorageRpcMessageKind::StagingDirectPrepare;
        let reply = self
            .session
            .rpc_request_result_locked(
                &mut self.inner,
                kind,
                request.bytes().to_vec(),
                self.deadline.min(permit.deadline()),
            )
            .map_err(StorageRpcRequestDispatchFailure::into_source)?
            .map_err(|e| self.session.rpc_response_error(kind, e))?;
        if self.inner.released {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "remote direct terminal preparation lost held section",
            }
            .into());
        }
        crate::node_runtime::pg_store::CapturedDirectTerminal::decode_remote(permit, self, &reply)
    }
    pub(in crate::node_runtime) fn prepare_direct_snapshot(
        &mut self,
        client: &UnixStorageNodeClient,
        permit: &crate::node_runtime::pg_store::DirectStagingPreparation<'_, '_>,
    ) -> Result<
        crate::node_runtime::pg_store::DirectStagingSnapshot,
        crate::node_runtime::pg_store::StagingWindowConfirmationError,
    > {
        self.session.require_origin(client)?;
        permit.require_remote_session(self)?;
        let request = permit.remote_request()?;
        let kind = StorageRpcMessageKind::StagingDirectSnapshot;
        let reply = self
            .session
            .rpc_request_result_locked(
                &mut self.inner,
                kind,
                request.bytes().to_vec(),
                self.deadline.min(permit.deadline()),
            )
            .map_err(StorageRpcRequestDispatchFailure::into_source)?
            .map_err(|e| self.session.rpc_response_error(kind, e))?;
        if self.inner.released {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "remote direct snapshot lost held section",
            }
            .into());
        }
        permit.capture_remote_reply(client, self, &request, &reply)
    }
    pub(in crate::node_runtime) fn send_complete_retirement(
        &mut self,
        attempt: crate::node_runtime::pg_store::CompleteRetirementAttempt<'_>,
    ) -> Result<(), crate::node_runtime::pg_store::CompleteRetirementSendError> {
        let deadline = self.deadline.min(attempt.deadline());
        #[cfg(test)]
        let deadline = match INTRODUCTION_RETIRE_BEFORE_DISPATCH.with(|slot| slot.replace(None)) {
            Some(true) => panic!("injected introduction retirement unwind before transport"),
            Some(false) => Instant::now(),
            None => deadline,
        };
        self.session
            .send_complete_retirement_locked(&mut self.inner, attempt, deadline)
    }
    pub(in crate::node_runtime) fn send_complete_application(
        &mut self,
        attempt: crate::node_runtime::pg_store::CompleteApplicationAttempt<'_>,
    ) -> Result<
        MetadataCommandReplicaState,
        crate::node_runtime::pg_store::CompleteApplicationSendError,
    > {
        let deadline = self.deadline.min(attempt.deadline());
        #[cfg(test)]
        let deadline = match APPEND_APPLY_BEFORE_DISPATCH.with(|slot| slot.replace(None)) {
            Some(true) => panic!("injected append application unwind before transport"),
            Some(false) => Instant::now(),
            None => deadline,
        };
        self.session
            .send_complete_application_locked(&mut self.inner, attempt, deadline)
    }
    pub(in crate::node_runtime) fn send_complete_publication(
        &mut self,
        attempt: crate::node_runtime::pg_store::CompletePublicationAttempt<'_>,
    ) -> Result<(), crate::node_runtime::pg_store::CompletePublicationSendError> {
        let deadline = self.deadline.min(attempt.transport().deadline());
        #[cfg(test)]
        let deadline = match APPEND_PUBLISH_BEFORE_DISPATCH.with(|slot| slot.replace(None)) {
            Some(true) => panic!("injected append publication unwind before transport"),
            Some(false) => Instant::now(),
            None => deadline,
        };
        self.session
            .send_complete_publication_locked(&mut self.inner, attempt, deadline)
    }
    pub(in crate::node_runtime) fn install_stream_append(
        &mut self,
        attempt: crate::node_runtime::pg_store::RemoteStreamAppendInstallationAttempt<'_, '_>,
    ) -> Result<(), crate::node_runtime::pg_store::StagingWindowConfirmationError> {
        if let Err(error) = attempt.require_session(self) {
            return Err(attempt.not_sent(error));
        }
        let kind = StorageRpcMessageKind::StagingStreamAppendInstall;
        let deadline = self.deadline.min(attempt.deadline());
        #[cfg(test)]
        let deadline = match APPEND_INSTALL_BEFORE_DISPATCH.with(|slot| slot.replace(None)) {
            Some(true) => panic!("injected append installation unwind before transport"),
            Some(false) => Instant::now(),
            None => deadline,
        };
        let result = self.session.rpc_request_result_locked(
            &mut self.inner,
            kind,
            attempt.bytes().to_vec(),
            deadline,
        );
        match result {
            Err(StorageRpcRequestDispatchFailure::NotSent(error)) => {
                Err(attempt.not_sent(error.into()))
            }
            Err(StorageRpcRequestDispatchFailure::MayHaveApplied(error)) => {
                Err(attempt.unconfirmed(error))
            }
            Ok(Err(error)) => {
                Err(attempt.unconfirmed(self.session.rpc_response_error(kind, error)))
            }
            Ok(Ok(reply)) => attempt.confirm(&reply),
        }
    }
    pub(in crate::node_runtime) fn prepare_stream_append(
        &mut self,
        permit: &crate::node_runtime::pg_store::StreamAppendPreparation<'_, '_>,
    ) -> Result<
        crate::node_runtime::pg_store::CapturedStreamAppend,
        crate::node_runtime::pg_store::StagingWindowConfirmationError,
    > {
        permit.require_valid()?;
        let original = permit.publication_request()?;
        if original.node_id() != self.session.node_id
            || original.id().pg_id() != self.session.pg_id
            || original.id().cluster_epoch() != self.session.cluster_epoch
            || self.inner.released
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "acknowledged append preparation session",
            }
            .into());
        }
        let request = permit.remote_request()?;
        let kind = StorageRpcMessageKind::StagingStreamAppendPrepare;
        let reply = self
            .session
            .rpc_request_result_locked(
                &mut self.inner,
                kind,
                request.bytes().to_vec(),
                self.deadline.min(permit.deadline()),
            )
            .map_err(StorageRpcRequestDispatchFailure::into_source)?
            .map_err(|e| self.session.rpc_response_error(kind, e))?;
        if self.inner.released {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "remote append preparation lost held section",
            }
            .into());
        }
        crate::node_runtime::pg_store::CapturedStreamAppend::decode_remote(permit, &reply)
    }

    /// Consume this exact held section and confirm remote guard removal. Socket
    /// shutdown alone is only best-effort teardown, never release evidence.
    pub(in crate::node_runtime) fn release_until(mut self) -> Result<(), StoreError> {
        let kind = StorageRpcMessageKind::MetadataCommandPgLockRelease;
        let payload = self
            .session
            .encode_metadata_command_state_request(self.session.pg_id);
        let result = self
            .session
            .rpc_request_result_locked(&mut self.inner, kind, payload, self.deadline)
            .map_err(StorageRpcRequestDispatchFailure::into_source)
            .and_then(|reply| reply.map_err(|error| self.session.rpc_response_error(kind, error)))
            .and_then(|reply| {
                if reply.is_empty() {
                    require_metadata_command_operation_deadline(self.deadline)
                } else {
                    Err(self.session.rpc_payload_error(
                        "release complete primary section",
                        "nonempty release acknowledgement".into(),
                    ))
                }
            });
        // Never reuse/reconnect after either a confirmed or uncertain release.
        let _ = self.inner.stream.shutdown(std::net::Shutdown::Both);
        self.inner.released = true;
        result
    }

    pub(crate) fn require_identity(
        &self,
        expected: &MetadataCommandSessionIdentity,
    ) -> Result<(), StoreError> {
        self.session.require_identity(expected)
    }

    pub(in crate::node_runtime) fn send_stream_allocation(
        &mut self,
        attempt: crate::node_runtime::pg_store::RemoteStreamAllocationAttempt<'_, '_>,
    ) -> Result<
        StreamUploadSegmentRecord,
        crate::node_runtime::pg_store::StagingWindowConfirmationError,
    > {
        // Validate the retained confirmation identity under the held transport
        // mutex, before allocating a request ID or sending any allocation RPC.
        attempt.require_session(self)?;
        let session = self.session;
        if attempt.node_id() != session.node_id
            || attempt.id().pg_id() != session.pg_id
            || attempt.id().cluster_epoch() != session.cluster_epoch
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "stream allocation session",
            }
            .into());
        }
        let kind = StorageRpcMessageKind::ObjectStreamSegmentAppendPrepare;
        let reply = session
            .rpc_request_result_locked(
                &mut self.inner,
                kind,
                attempt.bytes().to_vec(),
                self.deadline.min(attempt.deadline()),
            )
            .map_err(StorageRpcRequestDispatchFailure::into_source)?
            .map_err(|e| session.rpc_response_error(kind, e))?;
        if self.inner.released {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "stream allocation lost held section",
            }
            .into());
        }
        attempt.confirm(&reply)
    }
    pub(in crate::node_runtime) fn inspect_window_primary(
        &mut self,
        request: crate::storage_rpc::complete_window_primary::Request<'_>,
        witness: Option<NodeId>,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        self.inspect_window_primary_for(
            crate::storage_rpc::complete_window_primary::Purpose::RetainedWindow,
            request,
            witness,
        )
    }

    /// Read-only exact-request observation, never an allocation or file grant.
    /// The consuming holder separately binds this borrow to its captured session.
    pub(in crate::node_runtime) fn inspect_stream_file_primary(
        &mut self,
        bytes: &[u8],
    ) -> Result<(), StoreError> {
        self.inspect_stream_file_primary_with_kind(
            bytes,
            StorageRpcMessageKind::ObjectStreamSegmentAppendPrepare,
        )
    }

    pub(in crate::node_runtime) fn inspect_stream_file_primary_as_peer(
        &mut self,
        bytes: &[u8],
    ) -> Result<(), StoreError> {
        self.inspect_stream_file_primary_with_kind(
            bytes,
            StorageRpcMessageKind::MetadataCommandReplicaState,
        )
    }

    fn inspect_stream_file_primary_with_kind(
        &mut self,
        bytes: &[u8],
        kind: StorageRpcMessageKind,
    ) -> Result<(), StoreError> {
        use crate::storage_rpc::{
            complete_stream_allocation as allocation, complete_stream_file_primary as files,
        };
        let session = self.session;
        let invalid = |error: crate::storage_rpc::StorageRpcPayloadError| {
            session.rpc_payload_error("stream file inspection", error.to_string())
        };
        let request = files::decode_request(bytes).map_err(invalid)?;
        let route = allocation::decode_request(request.allocation)
            .map_err(invalid)?
            .window
            .route;
        if route.node != session.node_id
            || route.pg != session.pg_id
            || route.epoch != session.cluster_epoch
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "stream file inspection session",
            });
        }
        let deadline = self.deadline.min(route.deadline.local_deadline());
        let reply = session
            .rpc_request_result_locked(&mut self.inner, kind, bytes.to_vec(), deadline)
            .map_err(StorageRpcRequestDispatchFailure::into_source)?
            .map_err(|error| session.rpc_response_error(kind, error))?;
        require_metadata_command_operation_deadline(deadline)?;
        if self.inner.released {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "stream file inspection lost its held section",
            });
        }
        files::decode_response(&reply, bytes).map_err(invalid)
    }

    /// Read-only prerequisite, never a file grant. The consuming owner must
    /// retain its original window and compare the returned settled prefix.
    pub(in crate::node_runtime) fn inspect_direct_file_primary(
        &mut self,
        request: crate::storage_rpc::complete_window_primary::Request<'_>,
        witness: Option<NodeId>,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        self.inspect_window_primary_for(
            crate::storage_rpc::complete_window_primary::Purpose::DirectFileBinding,
            request,
            witness,
        )
    }

    fn inspect_window_primary_for(
        &mut self,
        purpose: crate::storage_rpc::complete_window_primary::Purpose,
        request: crate::storage_rpc::complete_window_primary::Request<'_>,
        witness: Option<NodeId>,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        use crate::storage_rpc::complete_window_primary as wire;
        let session = self.session;
        if request.route.node != session.node_id
            || request.route.pg != session.pg_id
            || request.route.epoch != session.cluster_epoch
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "primary window inspection session",
            });
        }
        let deadline = self.deadline.min(request.route.deadline.local_deadline());
        let payload = wire::encode_request_for(purpose, request).map_err(|e| {
            session.rpc_payload_error("encode primary window inspection", e.to_string())
        })?;
        let kind = StorageRpcMessageKind::MetadataCommandReplicaState;
        let reply = session
            .rpc_request_result_locked(&mut self.inner, kind, payload, deadline)
            .map_err(StorageRpcRequestDispatchFailure::into_source)?
            .map_err(|error| session.rpc_response_error(kind, error))?;
        require_metadata_command_operation_deadline(deadline)?;
        if self.inner.released {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "primary window inspection lost its held section",
            });
        }
        wire::decode_response_for(purpose, &reply, request, witness).map_err(|error| {
            session.rpc_payload_error("decode primary window inspection", error.to_string())
        })
    }
}

struct UnixStorageNodeReadHandleLease {
    session: UnixStorageNodeReadHandleSession,
    read_operation_id: String,
    entries: Vec<(crate::cluster::ShardLocation, ShardKey)>,
    released: bool,
}

struct UnixShardReadHandleRoute<'a> {
    client: &'a UnixStorageNodeClient,
    read_operation_id: String,
    entries: Vec<(crate::cluster::ShardLocation, ShardKey)>,
}

struct UnixObjectPayloadLease {
    session: UnixStorageNodeReadHandleSession,
    bucket: BucketName,
    key: ObjectKey,
    generation_id: GenerationId,
    released: bool,
}

struct UnixObjectPayloadLeaseRoute<'a> {
    client: &'a UnixStorageNodeClient,
    bucket: BucketName,
    key: ObjectKey,
    generation_id: GenerationId,
}

struct UnixRetainedObjectPayloadReclaimRoute<'a> {
    client: &'a UnixStorageNodeClient,
    bucket: BucketName,
    key: ObjectKey,
    generation_id: GenerationId,
    authority: ObjectPayloadReclaimClaimProof,
}

impl UnixStorageNodeClient {
    fn connect_session_connection(
        &self,
        context: &'static str,
    ) -> Result<(StorageRpcRequestConnection, Duration), StoreError> {
        let io_timeout = storage_rpc_io_timeout(self.rpc_auth.as_deref());
        let deadline = Instant::now()
            .checked_add(io_timeout)
            .ok_or_else(|| StoreError::Io {
                context,
                source: io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "storage-node RPC session deadline overflowed",
                ),
            })?;
        let max_connections = self
            .rpc_auth
            .as_deref()
            .map_or(self.rpc_admission.limit, |auth| {
                auth.transport_limits().max_connections()
            });
        self.endpoint
            .connect_session(deadline, io_timeout, max_connections)
            .map(|connection| (connection, io_timeout))
            .map_err(|failure| storage_rpc_endpoint_connect_error(self.node_id, context, failure))
    }

    fn connect_session_stream_until(
        &self,
        context: &'static str,
        deadline: Instant,
    ) -> Result<BoxStorageRpcStream, StoreError> {
        if Instant::now() >= deadline {
            return Err(storage_rpc_endpoint_deadline_expired(self.node_id, context));
        }
        self.endpoint
            .connect_classified(deadline)
            .map_err(|failure| storage_rpc_endpoint_connect_error(self.node_id, context, failure))
    }

    #[cfg(test)]
    pub(crate) fn active_admitted_session_count_for_test(&self) -> usize {
        self.rpc_admission.active_session_count_for_test()
    }

    #[cfg(test)]
    pub(crate) fn idle_session_connection_count_for_test(&self) -> usize {
        self.endpoint.idle_session_connection_count_for_test()
    }

    fn open_read_handle_session(&self) -> Result<UnixStorageNodeReadHandleSession, StoreError> {
        let rpc_permit = self.acquire_rpc_admission(StorageRpcMessageKind::ReadHandlesAcquire)?;
        let (connection, io_timeout) =
            self.connect_session_connection("connect storage-node read-handle RPC endpoint")?;
        Ok(UnixStorageNodeReadHandleSession {
            node_id: self.node_id,
            route_cluster_epoch: self.cluster_epoch,
            connection,
            next_request_id: 1,
            io_timeout,
            last_connection_reusable: false,
            active_read_operation_ids: BTreeSet::new(),
            rpc_auth: self.rpc_auth.clone(),
            _rpc_permit: Some(rpc_permit),
            _object_payload_lease_permit: None,
        })
    }

    #[cfg(test)]
    pub(crate) fn read_full_placed_shard_with_handle_for_test(
        &self,
        location: crate::cluster::ShardLocation,
        key: &ShardKey,
        expected_ack: WriteAck,
    ) -> Result<Vec<u8>, StoreError> {
        let mut session = self.open_read_handle_session()?;
        session.acquire_read_handles("test-full-shard-read", vec![(location, key.clone())])?;
        let payload = session.read_full_placed_shard_for_test(location, key, expected_ack)?;
        session.release_read_handles("test-full-shard-read")?;
        Ok(payload)
    }

    #[cfg(test)]
    pub(crate) fn open_read_handle_session_for_test(
        &self,
    ) -> Result<UnixStorageNodeReadHandleSession, StoreError> {
        self.open_read_handle_session()
    }

    fn open_object_payload_lease_session(
        &self,
        kind: ObjectPayloadLeaseKind,
    ) -> Result<UnixStorageNodeReadHandleSession, StoreError> {
        let lease_permit = self.acquire_object_payload_lease_session_admission(kind)?;
        let (connection, io_timeout) = self
            .connect_session_connection("connect storage-node object-payload lease RPC endpoint")?;
        Ok(UnixStorageNodeReadHandleSession {
            node_id: self.node_id,
            route_cluster_epoch: self.cluster_epoch,
            connection,
            next_request_id: 1,
            io_timeout,
            last_connection_reusable: false,
            active_read_operation_ids: BTreeSet::new(),
            rpc_auth: self.rpc_auth.clone(),
            _rpc_permit: None,
            _object_payload_lease_permit: Some(lease_permit),
        })
    }

    fn acquire_object_payload_lease_session_admission(
        &self,
        kind: ObjectPayloadLeaseKind,
    ) -> Result<UnixStorageNodeObjectPayloadLeaseAdmissionPermit, StoreError> {
        let wait_timeout = self
            .rpc_admission
            .wait_timeout_for_class(UnixStorageNodeRpcAdmissionClass::Read);
        let broad = kind == ObjectPayloadLeaseKind::BroadSnapshot;
        match self.rpc_admission.acquire_object_payload_lease(broad) {
            UnixStorageNodeObjectPayloadLeaseAdmissionAcquire::Acquired(permit) => Ok(permit),
            UnixStorageNodeObjectPayloadLeaseAdmissionAcquire::TimedOut => {
                Err(StoreError::StorageRpcResourceExhausted {
                    node_id: self.node_id.as_u32(),
                    operation: match kind {
                        ObjectPayloadLeaseKind::BroadSnapshot => {
                            "broad object payload lease acquire"
                        }
                        ObjectPayloadLeaseKind::ShardLocations => {
                            "shard object payload lease acquire"
                        }
                    },
                    detail: crate::StorageNodeFailureDetail::new(format!(
                        "storage-node client object-payload lease session limit {} is exhausted after waiting {} ms",
                        self.rpc_admission.limit,
                        wait_timeout.as_millis()
                    )),
                })
            }
        }
    }

    fn object_payload_lease_control_request(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        operation: StorageRpcObjectPayloadLeaseControlOperation,
        reclaim_authority: Option<&ObjectPayloadReclaimClaimProof>,
    ) -> Result<u64, StoreError> {
        let request = StorageRpcObjectPayloadLeaseControlRequest {
            node_id: self.node_id,
            route_cluster_epoch: self.cluster_epoch,
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id,
            operation,
            reclaim_authority: reclaim_authority.cloned(),
        };
        let payload = encode_object_payload_lease_control_request(&request);
        let response =
            self.rpc_request(StorageRpcMessageKind::ObjectPayloadLeaseControl, payload)?;
        let response =
            decode_object_payload_lease_control_response(&response).map_err(|error| {
                self.rpc_payload_error(
                    "decode object-payload lease control response",
                    error.to_string(),
                )
            })?;
        Ok(response.value)
    }

    pub(crate) fn open_metadata_command_critical_section(
        &self,
        pg_id: PgId,
    ) -> Result<UnixStorageNodeMetadataCommandSession, StoreError> {
        let io_timeout = storage_rpc_io_timeout(self.rpc_auth.as_deref());
        let deadline = Instant::now()
            .checked_add(io_timeout)
            .ok_or_else(|| StoreError::Io {
                context: "compute metadata command session deadline",
                source: io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "metadata command session deadline overflowed",
                ),
            })?;
        self.open_metadata_command_critical_section_until(pg_id, deadline)
    }

    pub(crate) fn open_metadata_command_critical_section_until(
        &self,
        pg_id: PgId,
        deadline: Instant,
    ) -> Result<UnixStorageNodeMetadataCommandSession, StoreError> {
        self.open_metadata_command_section_at_epoch(pg_id, self.cluster_epoch, deadline)
    }

    pub(crate) fn open_complete_historical_primary(
        &self,
        route: &crate::cluster::HistoricalRemoteApplicationRoute<'_>,
        deadline: Instant,
    ) -> Result<UnixStorageNodeMetadataCommandSession, StoreError> {
        let (pg_id, epoch, deadline) = route.primary_session_parameters(self, deadline)?;
        let session = self.open_metadata_command_section_at_epoch(pg_id, epoch, deadline)?;
        route.primary_session_parameters(self, deadline)?;
        Ok(session)
    }

    pub(crate) fn open_complete_fanout_primary(
        &self,
        route: &crate::cluster::CompleteFanoutRoute<'_>,
        deadline: Instant,
    ) -> Result<UnixStorageNodeMetadataCommandSession, StoreError> {
        let (pg_id, epoch, deadline) = route.primary_session_parameters(self, deadline)?;
        let session = self.open_metadata_command_section_at_epoch(pg_id, epoch, deadline)?;
        route.primary_session_parameters(self, deadline)?;
        Ok(session)
    }

    fn open_metadata_command_section_at_epoch(
        &self,
        pg_id: PgId,
        epoch: ClusterEpoch,
        deadline: Instant,
    ) -> Result<UnixStorageNodeMetadataCommandSession, StoreError> {
        let rpc_permit = self.acquire_rpc_admission_with_class_until(
            StorageRpcMessageKind::MetadataCommandPgLockAcquire,
            storage_rpc_admission_class(StorageRpcMessageKind::MetadataCommandPgLockAcquire),
            deadline,
        )?;
        let stream = self.connect_session_stream_until(
            "connect storage-node metadata command RPC endpoint",
            deadline,
        )?;
        let session = UnixStorageNodeMetadataCommandSession {
            origin: Arc::clone(&self.instance),
            identity: Arc::new(()),
            node_id: self.node_id,
            cluster_epoch: epoch,
            pg_id,
            rpc_auth: self.rpc_auth.clone(),
            _rpc_permit: rpc_permit,
            inner: Mutex::new(UnixStorageNodeMetadataCommandSessionInner {
                stream,
                next_request_id: 1,
                io_timeout: storage_rpc_io_timeout(self.rpc_auth.as_deref()),
                released: false,
            }),
        };
        let payload = session.encode_metadata_command_state_request(pg_id);
        session.rpc_request_until(
            StorageRpcMessageKind::MetadataCommandPgLockAcquire,
            payload,
            deadline,
        )?;
        Ok(session)
    }
}

#[allow(dead_code)]
impl UnixStorageNodeReadHandleSession {
    pub(crate) fn acquire_read_handles(
        &mut self,
        read_operation_id: impl Into<String>,
        entries: Vec<(crate::cluster::ShardLocation, ShardKey)>,
    ) -> Result<Vec<crate::cluster::ShardLocation>, StoreError> {
        let (locations, shard_keys): (Vec<_>, Vec<_>) = entries.into_iter().unzip();
        let read_operation_id = read_operation_id.into();
        let request = StorageRpcReadHandleAcquireRequest {
            read_operation_id: read_operation_id.clone(),
            locations: locations.iter().copied().map(Into::into).collect(),
            shard_keys,
        };
        let payload = encode_read_handle_acquire_request(&request).map_err(|error| {
            self.rpc_payload_error("encode read handle acquire request", error.to_string())
        })?;
        let response = self.rpc_request(StorageRpcMessageKind::ReadHandlesAcquire, payload)?;
        let response = decode_read_handle_acquire_response(&response).map_err(|error| {
            self.rpc_payload_error("decode read handle acquire response", error.to_string())
        })?;
        if response.locations != request.locations {
            return Err(self.rpc_payload_error(
                "validate read handle acquire response",
                format!(
                    "expected locations {:?}, got {:?}",
                    request.locations, response.locations
                ),
            ));
        }
        self.active_read_operation_ids.insert(read_operation_id);
        Ok(locations)
    }

    pub(crate) fn release_read_handles(
        &mut self,
        read_operation_id: impl Into<String>,
    ) -> Result<(), StoreError> {
        let read_operation_id = read_operation_id.into();
        let request = StorageRpcReadHandleReleaseRequest {
            read_operation_id: read_operation_id.clone(),
        };
        let payload = encode_read_handle_release_request(&request).map_err(|error| {
            self.rpc_payload_error("encode read handle release request", error.to_string())
        })?;
        let response = self.rpc_request(StorageRpcMessageKind::ReadHandlesRelease, payload)?;
        decode_read_handle_release_response(&response).map_err(|error| {
            self.rpc_payload_error("decode read handle release response", error.to_string())
        })?;
        self.active_read_operation_ids.remove(&read_operation_id);
        if self.active_read_operation_ids.is_empty() {
            self.mark_connection_reusable_if_server_allows();
        }
        Ok(())
    }

    fn read_placed_shard_into(
        &mut self,
        location: crate::cluster::ShardLocation,
        key: &ShardKey,
        expected_ack: WriteAck,
        dst: &mut [u8],
    ) -> Result<(), StoreError> {
        if dst.len() as u64 != expected_ack.stored_size {
            return Err(self.rpc_payload_error(
                "read shard through read-handle session",
                format!(
                    "remote shard read buffer is {} bytes for expected {} byte shard",
                    dst.len(),
                    expected_ack.stored_size
                ),
            ));
        }
        let request = StorageRpcShardReadRangeRequest {
            location: location.into(),
            shard_key: key.clone(),
            expected_ack,
            offset: 0,
            length: dst.len() as u64,
        };
        let payload = encode_shard_read_range_request(&request).map_err(|error| {
            self.rpc_payload_error("encode read-handle shard read request", error.to_string())
        })?;
        let response = self.rpc_request(StorageRpcMessageKind::ShardReadRange, payload)?;
        let data = decode_shard_read_range_response(&response, dst.len()).map_err(|error| {
            self.rpc_payload_error("decode read-handle shard read response", error.to_string())
        })?;
        dst.copy_from_slice(&data);
        observability::record_payload_read_metrics(observability::PayloadReadMetricDelta {
            crc_bytes: u64::try_from(dst.len()).unwrap_or(u64::MAX),
            ..observability::PayloadReadMetricDelta::default()
        });
        let actual_crc = checksum::crc64::checksum(dst);
        if actual_crc != expected_ack.crc64 {
            return Err(StoreError::IntegrityError {
                expected: expected_ack.crc64,
                actual: actual_crc,
            });
        }
        Ok(())
    }

    fn read_placed_shard_range_into(
        &mut self,
        location: crate::cluster::ShardLocation,
        key: &ShardKey,
        expected_ack: WriteAck,
        offset: u64,
        dst: &mut [u8],
    ) -> Result<(), StoreError> {
        let request = StorageRpcShardReadRangeRequest {
            location: location.into(),
            shard_key: key.clone(),
            expected_ack,
            offset,
            length: u64::try_from(dst.len()).map_err(|_| {
                self.rpc_payload_error(
                    "read shard range through read-handle session",
                    "range length exceeds u64".to_string(),
                )
            })?,
        };
        let payload = encode_shard_read_range_request(&request).map_err(|error| {
            self.rpc_payload_error("encode read-handle shard range request", error.to_string())
        })?;
        let response = self.rpc_request(StorageRpcMessageKind::ShardReadRange, payload)?;
        let data = decode_shard_read_range_response(&response, dst.len()).map_err(|error| {
            self.rpc_payload_error("decode read-handle shard range response", error.to_string())
        })?;
        dst.copy_from_slice(&data);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn read_placed_shard_into_for_test(
        &mut self,
        location: crate::cluster::ShardLocation,
        key: &ShardKey,
        expected_ack: WriteAck,
        dst: &mut [u8],
    ) -> Result<(), StoreError> {
        self.read_placed_shard_into(location, key, expected_ack, dst)
    }

    #[cfg(test)]
    pub(crate) fn read_placed_shard_range_into_for_test(
        &mut self,
        location: crate::cluster::ShardLocation,
        key: &ShardKey,
        expected_ack: WriteAck,
        offset: u64,
        dst: &mut [u8],
    ) -> Result<(), StoreError> {
        self.read_placed_shard_range_into(location, key, expected_ack, offset, dst)
    }

    #[cfg(test)]
    pub(crate) fn read_full_placed_shard_for_test(
        &mut self,
        location: crate::cluster::ShardLocation,
        key: &ShardKey,
        expected_ack: WriteAck,
    ) -> Result<Vec<u8>, StoreError> {
        let request = StorageRpcShardReadRequest {
            location: location.into(),
            shard_key: key.clone(),
            expected_ack,
        };
        let payload = encode_shard_read_request(&request).map_err(|error| {
            self.rpc_payload_error(
                "encode full read-handle shard read request",
                error.to_string(),
            )
        })?;
        let response = self.rpc_request(StorageRpcMessageKind::ShardRead, payload)?;
        decode_shard_read_response(&response, expected_ack).map_err(|error| {
            self.rpc_payload_error(
                "decode full read-handle shard read response",
                error.to_string(),
            )
        })
    }

    fn object_payload_lease_control(
        &mut self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        operation: StorageRpcObjectPayloadLeaseControlOperation,
        reclaim_authority: Option<&ObjectPayloadReclaimClaimProof>,
    ) -> Result<u64, StoreError> {
        let request = StorageRpcObjectPayloadLeaseControlRequest {
            node_id: self.node_id,
            route_cluster_epoch: self.route_cluster_epoch,
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id,
            operation,
            reclaim_authority: reclaim_authority.cloned(),
        };
        let payload = encode_object_payload_lease_control_request(&request);
        let response =
            self.rpc_request(StorageRpcMessageKind::ObjectPayloadLeaseControl, payload)?;
        let response =
            decode_object_payload_lease_control_response(&response).map_err(|error| {
                self.rpc_payload_error(
                    "decode object-payload lease control response",
                    error.to_string(),
                )
            })?;
        Ok(response.value)
    }

    fn rpc_request(
        &mut self,
        kind: StorageRpcMessageKind,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, StoreError> {
        let deadline = Instant::now().checked_add(self.io_timeout).ok_or_else(|| {
            self.rpc_payload_error(
                "set read-handle RPC deadline",
                "storage-node read-handle RPC deadline overflowed".to_string(),
            )
        })?;
        self.connection
            .stream_mut()
            .set_operation_deadline(deadline)
            .map_err(|source| StoreError::Io {
                context: "set storage-node read-handle RPC deadline",
                source,
            })?;
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.checked_add(1).ok_or_else(|| {
            self.rpc_payload_error(
                "allocate read-handle request id",
                "storage-node read-handle request id overflowed".to_string(),
            )
        })?;
        let request = StorageRpcFrame {
            request_id,
            kind,
            payload,
        };
        let request_proof = write_unix_storage_rpc_request(
            self.connection.stream_mut(),
            self.node_id,
            self.rpc_auth.as_deref(),
            &request,
            "write read-handle RPC request",
        )?;
        let response = read_unix_storage_rpc_response(
            self.connection.stream_mut(),
            self.node_id,
            self.rpc_auth.as_deref(),
            request_proof.as_ref(),
            "read read-handle RPC response",
        )?;
        if response.request_id != request_id || response.kind != kind {
            return Err(self.rpc_payload_error(
                "validate read-handle RPC response",
                format!(
                    "expected request {request_id} kind {kind:?}, got request {} kind {:?}",
                    response.request_id, response.kind
                ),
            ));
        }
        let response =
            decode_storage_rpc_response_payload_with_connection_disposition(&response.payload)
                .map_err(|error| {
                    self.rpc_payload_error("decode read-handle RPC response", error.to_string())
                })?;
        self.last_connection_reusable = response.connection_reusable;
        match response.response {
            Ok(payload) => Ok(payload),
            Err(error) => Err(self.rpc_response_error(kind, error)),
        }
    }

    fn mark_connection_reusable_if_server_allows(&mut self) {
        if self.last_connection_reusable {
            self.connection.mark_reusable();
        }
    }

    fn rpc_response_error(
        &self,
        kind: StorageRpcMessageKind,
        error: StorageRpcErrorResponse,
    ) -> StoreError {
        storage_rpc_response_error(self.node_id, kind, error)
    }

    fn rpc_payload_error(&self, operation: &'static str, message: String) -> StoreError {
        StoreError::StorageRpc {
            node_id: self.node_id.as_u32(),
            operation,
            failure: StorageRpcErrorCode::PayloadDecode,
            detail: crate::StorageNodeFailureDetail::new(message),
        }
    }
}

impl UnixStorageNodeMetadataCommandSession {
    pub(in crate::node_runtime) fn identity(&self) -> MetadataCommandSessionIdentity {
        MetadataCommandSessionIdentity(Arc::clone(&self.identity))
    }

    #[cfg(test)]
    pub(in crate::node_runtime) fn test_close_primary_section(&self) {
        self.close_metadata_command_pg_lock_on_drop();
    }

    pub(in crate::node_runtime) fn send_stream_allocation(
        &self,
        client: &UnixStorageNodeClient,
        attempt: crate::node_runtime::pg_store::RemoteStreamAllocationAttempt<'_, '_>,
    ) -> Result<
        StreamUploadSegmentRecord,
        crate::node_runtime::pg_store::StagingWindowConfirmationError,
    > {
        let mut session = self.borrow_complete_primary(
            client,
            attempt.id().pg_id(),
            attempt.id().cluster_epoch(),
            attempt.deadline(),
        )?;
        session.send_stream_allocation(attempt)
    }
    #[cfg(test)]
    pub(in crate::node_runtime) fn test_direct_snapshot_request(
        &self,
        payload: Vec<u8>,
    ) -> Result<Result<Vec<u8>, StorageRpcErrorResponse>, StoreError> {
        self.rpc_request_result(StorageRpcMessageKind::StagingDirectSnapshot, payload)
    }

    #[cfg(test)]
    pub(in crate::node_runtime) fn test_direct_install_request(
        &self,
        payload: Vec<u8>,
    ) -> Result<Result<Vec<u8>, StorageRpcErrorResponse>, StoreError> {
        self.rpc_request_result(StorageRpcMessageKind::StagingDirectInstall, payload)
    }

    #[cfg(test)]
    pub(in crate::node_runtime) fn test_direct_introduction_install_request(
        &self,
        payload: Vec<u8>,
    ) -> Result<Result<Vec<u8>, StorageRpcErrorResponse>, StoreError> {
        self.rpc_request_result(
            StorageRpcMessageKind::StagingDirectIntroductionInstall,
            payload,
        )
    }

    #[cfg(test)]
    pub(in crate::node_runtime) fn test_direct_prepare_request(
        &self,
        payload: Vec<u8>,
    ) -> Result<Result<Vec<u8>, StorageRpcErrorResponse>, StoreError> {
        self.rpc_request_result(StorageRpcMessageKind::StagingDirectPrepare, payload)
    }

    #[cfg(test)]
    pub(in crate::node_runtime) fn test_stream_append_prepare_request(
        &self,
        payload: Vec<u8>,
    ) -> Result<Result<Vec<u8>, StorageRpcErrorResponse>, StoreError> {
        self.rpc_request_result(StorageRpcMessageKind::StagingStreamAppendPrepare, payload)
    }

    #[cfg(test)]
    pub(in crate::node_runtime) fn test_stream_append_install_request(
        &self,
        payload: Vec<u8>,
    ) -> Result<Result<Vec<u8>, StorageRpcErrorResponse>, StoreError> {
        self.rpc_request_result(StorageRpcMessageKind::StagingStreamAppendInstall, payload)
    }

    #[cfg(test)]
    pub(in crate::node_runtime) fn test_staging_file_request(
        &self,
        payload: Vec<u8>,
    ) -> Result<Result<Vec<u8>, StorageRpcErrorResponse>, StoreError> {
        self.rpc_request_result(StorageRpcMessageKind::StagingShardWrite, payload)
    }

    #[cfg(test)]
    pub(in crate::node_runtime) fn test_window_primary_request(
        &self,
        payload: Vec<u8>,
    ) -> Result<Result<Vec<u8>, StorageRpcErrorResponse>, StoreError> {
        self.rpc_request_result(StorageRpcMessageKind::MetadataCommandReplicaState, payload)
    }

    /// One abandonment attempt on the exact held primary section. The owning
    /// abandonment holder is the only producer; no reconnect or automatic retry.
    pub(in crate::node_runtime) fn send_complete_abandonment(
        &self,
        attempt: crate::node_runtime::pg_store::CompleteAbandonmentAttempt<'_>,
    ) -> Result<(), crate::node_runtime::pg_store::CompleteAbandonmentSendError> {
        const KIND: StorageRpcMessageKind = StorageRpcMessageKind::MetadataCommandRecordAbandoned;
        if attempt.node_id() != self.node_id
            || attempt.id().cluster_epoch() != self.cluster_epoch
            || attempt.id().pg_id() != self.metadata_command_pg_id()
        {
            return Err(
                attempt.not_sent(StoreError::RouteCapabilitySubjectMismatch {
                    operation: "complete abandonment session subject",
                }),
            );
        }
        let payload = match attempt.encode_request() {
            Ok(payload) => payload,
            Err(error) => return Err(attempt.not_sent(error)),
        };
        match self.rpc_request_result_until(KIND, payload, attempt.deadline()) {
            Ok(Ok(response)) => attempt.confirm(&response),
            Ok(Err(error)) => Err(attempt.unconfirmed(self.rpc_response_error(KIND, error))),
            Err(StorageRpcRequestDispatchFailure::NotSent(error)) => Err(attempt.not_sent(error)),
            Err(StorageRpcRequestDispatchFailure::MayHaveApplied(error)) => {
                Err(attempt.unconfirmed(error))
            }
        }
    }

    /// One retirement attempt on the exact held primary section. The owning
    /// terminal holder is the only producer; no reconnect or automatic retry.
    pub(in crate::node_runtime) fn send_complete_retirement(
        &self,
        attempt: crate::node_runtime::pg_store::CompleteRetirementAttempt<'_>,
    ) -> Result<(), crate::node_runtime::pg_store::CompleteRetirementSendError> {
        let deadline = attempt.deadline();
        let mut inner = match self.lock_until(deadline, "acquire metadata command session RPC") {
            Ok(inner) => inner,
            Err(error) => return Err(attempt.not_sent(error)),
        };
        self.send_complete_retirement_locked(&mut inner, attempt, deadline)
    }

    fn send_complete_retirement_locked(
        &self,
        inner: &mut UnixStorageNodeMetadataCommandSessionInner,
        attempt: crate::node_runtime::pg_store::CompleteRetirementAttempt<'_>,
        deadline: Instant,
    ) -> Result<(), crate::node_runtime::pg_store::CompleteRetirementSendError> {
        const KIND: StorageRpcMessageKind = StorageRpcMessageKind::MetadataCommandPendingSlotRemove;
        if attempt.node_id() != self.node_id
            || attempt.id().cluster_epoch() != self.cluster_epoch
            || attempt.id().pg_id() != self.metadata_command_pg_id()
        {
            return Err(
                attempt.not_sent(StoreError::RouteCapabilitySubjectMismatch {
                    operation: "complete retirement session subject",
                }),
            );
        }
        let payload = match attempt.encode_request() {
            Ok(payload) => payload,
            Err(error) => return Err(attempt.not_sent(error)),
        };
        match self.rpc_request_result_locked(inner, KIND, payload, deadline) {
            Ok(Ok(response)) => attempt.confirm(&response),
            Ok(Err(error)) => Err(attempt.unconfirmed(self.rpc_response_error(KIND, error))),
            Err(StorageRpcRequestDispatchFailure::NotSent(error)) => Err(attempt.not_sent(error)),
            Err(StorageRpcRequestDispatchFailure::MayHaveApplied(error)) => {
                Err(attempt.unconfirmed(error))
            }
        }
    }

    pub(in crate::node_runtime) fn require_origin(
        &self,
        client: &UnixStorageNodeClient,
    ) -> Result<(), StoreError> {
        if !Arc::ptr_eq(&self.origin, &client.instance) {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "complete fanout session belongs to another client instance",
            });
        }
        Ok(())
    }

    /// Identity is immutable for the lifetime of this session. Checking it does
    /// not acquire/release the transport mutex or claim that the section is open;
    /// the following operation still performs bounded, locked live admission.
    pub(in crate::node_runtime) fn require_identity(
        &self,
        expected: &MetadataCommandSessionIdentity,
    ) -> Result<(), StoreError> {
        if !Arc::ptr_eq(&self.identity, &expected.0) {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "confirmed window requires its original remote primary session",
            });
        }
        Ok(())
    }

    pub(crate) fn borrow_complete_primary(
        &self,
        client: &UnixStorageNodeClient,
        pg_id: PgId,
        epoch: ClusterEpoch,
        deadline: Instant,
    ) -> Result<CompletePrimarySessionBorrow<'_>, StoreError> {
        self.require_origin(client)?;
        let inner = self.lock_until(deadline, "borrow complete primary session")?;
        if self.node_id != client.node_id()
            || self.cluster_epoch != epoch
            || self.pg_id != pg_id
            || inner.released
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "complete embedded application requires the exact open primary session",
            });
        }
        Ok(CompletePrimarySessionBorrow {
            inner,
            session: self,
            deadline,
        })
    }

    // Borrowing and RPC sends share this wait. Neither may inherit another
    // operation's longer timeout while retaining its own admitted route.
    fn lock_until(
        &self,
        deadline: Instant,
        context: &'static str,
    ) -> Result<std::sync::MutexGuard<'_, UnixStorageNodeMetadataCommandSessionInner>, StoreError>
    {
        let inner = loop {
            if Instant::now() >= deadline {
                return Err(storage_rpc_deadline_expired(context));
            }
            match self.inner.try_lock() {
                Ok(inner) => break inner,
                Err(std::sync::TryLockError::Poisoned(error)) => break error.into_inner(),
                Err(std::sync::TryLockError::WouldBlock) => {
                    if let Some(remaining) = deadline.checked_duration_since(Instant::now()) {
                        std::thread::sleep(remaining.min(Duration::from_millis(1)));
                    }
                }
            }
        };
        // Keep the successfully acquired guard; do not probe then reacquire.
        if Instant::now() >= deadline {
            return Err(storage_rpc_deadline_expired(context));
        }
        Ok(inner)
    }
    pub(in crate::node_runtime) fn send_complete_application(
        &self,
        attempt: crate::node_runtime::pg_store::CompleteApplicationAttempt<'_>,
    ) -> Result<
        MetadataCommandReplicaState,
        crate::node_runtime::pg_store::CompleteApplicationSendError,
    > {
        let deadline = attempt.deadline();
        let mut inner = match self.lock_until(deadline, "acquire metadata command session RPC") {
            Ok(inner) => inner,
            Err(error) => return Err(attempt.not_sent(error)),
        };
        self.send_complete_application_locked(&mut inner, attempt, deadline)
    }

    fn send_complete_application_locked(
        &self,
        inner: &mut UnixStorageNodeMetadataCommandSessionInner,
        attempt: crate::node_runtime::pg_store::CompleteApplicationAttempt<'_>,
        deadline: Instant,
    ) -> Result<
        MetadataCommandReplicaState,
        crate::node_runtime::pg_store::CompleteApplicationSendError,
    > {
        const KIND: StorageRpcMessageKind = StorageRpcMessageKind::MetadataCommandApplyAndRecord;
        if attempt.node_id() != self.node_id
            || attempt.id().cluster_epoch() != self.cluster_epoch
            || attempt.id().pg_id() != self.metadata_command_pg_id()
        {
            return Err(
                attempt.not_sent(StoreError::RouteCapabilitySubjectMismatch {
                    operation: "complete application session subject",
                }),
            );
        }
        let payload = match attempt.encode_request() {
            Ok(payload) => payload,
            Err(error) => return Err(attempt.not_sent(error)),
        };
        match self.rpc_request_result_locked(inner, KIND, payload, deadline) {
            Ok(Ok(response)) => attempt.confirm(&response),
            Ok(Err(error)) => Err(attempt.unconfirmed(self.rpc_response_error(KIND, error))),
            Err(StorageRpcRequestDispatchFailure::NotSent(error)) => Err(attempt.not_sent(error)),
            Err(StorageRpcRequestDispatchFailure::MayHaveApplied(error)) => {
                Err(attempt.unconfirmed(error))
            }
        }
    }

    /// One typed attempt on the retained authenticated section. No reconnect or
    /// automatic retry, and no generic remote error establishes non-acceptance.
    pub(in crate::node_runtime) fn send_complete_publication(
        &self,
        attempt: crate::node_runtime::pg_store::CompletePublicationAttempt<'_>,
    ) -> Result<(), crate::node_runtime::pg_store::CompletePublicationSendError> {
        let deadline = attempt.transport().deadline();
        let mut inner = match self.lock_until(deadline, "acquire metadata command session RPC") {
            Ok(inner) => inner,
            Err(error) => return Err(attempt.not_sent(error)),
        };
        self.send_complete_publication_locked(&mut inner, attempt, deadline)
    }

    fn send_complete_publication_locked(
        &self,
        inner: &mut UnixStorageNodeMetadataCommandSessionInner,
        attempt: crate::node_runtime::pg_store::CompletePublicationAttempt<'_>,
        deadline: Instant,
    ) -> Result<(), crate::node_runtime::pg_store::CompletePublicationSendError> {
        const KIND: StorageRpcMessageKind = StorageRpcMessageKind::MetadataCommandPublicationStart;
        let transport = attempt.transport();
        if transport.node_id() != self.node_id
            || transport.id().cluster_epoch() != self.cluster_epoch
            || transport.id().pg_id() != self.metadata_command_pg_id()
        {
            return Err(
                attempt.not_sent(StoreError::RouteCapabilitySubjectMismatch {
                    operation: "complete publication session subject",
                }),
            );
        }
        let payload = match transport.encode_request() {
            Ok(payload) => payload,
            Err(error) => {
                return Err(attempt.not_sent(match error {
                    crate::node_runtime::pg_store::CompletePendingInspectionError::Store(error) => {
                        error
                    }
                    other => {
                        self.rpc_payload_error("encode complete publication", other.to_string())
                    }
                }))
            }
        };
        match self.rpc_request_result_locked(inner, KIND, payload, deadline) {
            Ok(Ok(response)) => attempt.confirm(&response),
            Ok(Err(error)) => Err(attempt.unconfirmed(self.rpc_response_error(KIND, error))),
            Err(StorageRpcRequestDispatchFailure::NotSent(error)) => Err(attempt.not_sent(error)),
            Err(StorageRpcRequestDispatchFailure::MayHaveApplied(error)) => {
                Err(attempt.unconfirmed(error))
            }
        }
    }

    /// Inspect the real session dispatch classification without making it a
    /// production publication/custody API. A later NotSent does not resolve an
    /// earlier uncertain attempt; that still belongs to the owning sender.
    #[cfg(test)]
    pub(crate) fn test_complete_publication_dispatch_until(
        &self,
        payload: Vec<u8>,
        deadline: Instant,
    ) -> Result<Result<Vec<u8>, StorageRpcErrorResponse>, MetadataCommandApplyError> {
        self.rpc_request_result_until(
            StorageRpcMessageKind::MetadataCommandPublicationStart,
            payload,
            deadline,
        )
        .map_err(StorageRpcRequestDispatchFailure::into_metadata_command_apply_error)
    }

    /// Owner-local malformed-wire and lost-response tests. Production callers
    /// must use the owning publication sender, not detach raw bytes.
    #[cfg(test)]
    pub(crate) fn test_complete_publication_payload_until(
        &self,
        payload: Vec<u8>,
        deadline: Instant,
    ) -> Result<Vec<u8>, StoreError> {
        self.rpc_request_until(
            StorageRpcMessageKind::MetadataCommandPublicationStart,
            payload,
            deadline,
        )
    }

    fn metadata_command_pg_id(&self) -> PgId {
        self.pg_id
    }

    #[cfg(test)]
    pub(in crate::node_runtime) fn test_retirement_payload(
        &self,
        payload: Vec<u8>,
        deadline: Instant,
    ) -> Result<Vec<u8>, StoreError> {
        self.rpc_request_until(
            StorageRpcMessageKind::MetadataCommandPendingSlotRemove,
            payload,
            deadline,
        )
    }

    #[cfg(test)]
    pub(in crate::node_runtime) fn test_stream_inspection_payload(
        &self,
        payload: Vec<u8>,
        deadline: Instant,
    ) -> Result<Vec<u8>, StoreError> {
        self.rpc_request_until(
            StorageRpcMessageKind::MetadataCommandReplicaState,
            payload,
            deadline,
        )
    }

    #[cfg(test)]
    pub(in crate::node_runtime) fn test_stream_allocation_payload(
        &self,
        payload: Vec<u8>,
        deadline: Instant,
    ) -> Result<Vec<u8>, StoreError> {
        self.rpc_request_until(
            StorageRpcMessageKind::ObjectStreamSegmentAppendPrepare,
            payload,
            deadline,
        )
    }

    #[cfg(test)]
    pub(in crate::node_runtime) fn test_abandonment_payload(
        &self,
        payload: Vec<u8>,
        deadline: Instant,
    ) -> Result<Vec<u8>, StoreError> {
        self.rpc_request_until(
            StorageRpcMessageKind::MetadataCommandRecordAbandoned,
            payload,
            deadline,
        )
    }

    fn encode_metadata_command_state_request(&self, pg_id: PgId) -> Vec<u8> {
        encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
        })
    }

    fn encode_metadata_command_request(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<Vec<u8>, StoreError> {
        let request = StorageRpcMetadataCommandRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            command: command.clone(),
        };
        encode_metadata_command_request(&request).map_err(|error| {
            self.rpc_payload_error("encode metadata command request", error.to_string())
        })
    }

    fn encode_metadata_command_pending_slot_operation_request(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        deadline: Option<Instant>,
    ) -> Result<Vec<u8>, StoreError> {
        encode_metadata_command_pending_slot_request(&StorageRpcMetadataCommandPendingSlotRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            command: command.clone(),
            scope_bucket: None,
            effect_deadline: None,
            operation_deadline: deadline.map(StorageRpcOperationDeadline::from_instant),
        })
        .map_err(|error| {
            self.rpc_payload_error(
                "encode metadata command pending slot operation request",
                error.to_string(),
            )
        })
    }

    fn decode_pending_metadata_command_slot_cleanup_response(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        response: &[u8],
    ) -> Result<bool, StoreError> {
        let response =
            decode_metadata_command_pending_slot_cleanup_response(response).map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command pending slot remove response",
                    error.to_string(),
                )
            })?;
        match response.outcome {
            StorageRpcMetadataCommandPendingSlotCleanupOutcome::Value(removed) => Ok(removed),
            StorageRpcMetadataCommandPendingSlotCleanupOutcome::TerminalEntryPending {
                node_id,
                pg_id: pending_pg_id,
                cluster_epoch,
                log_index,
            } => Err(metadata_command_terminal_entry_pending_error(
                self.node_id,
                command.id().cluster_epoch(),
                pg_id,
                command.id().log_index(),
                "decode metadata command pending slot remove response",
                |operation, message| self.rpc_payload_error(operation, message),
                MetadataCommandLogConflictRpcFields {
                    node_id,
                    pg_id: pending_pg_id,
                    cluster_epoch,
                    log_index,
                },
            )),
            StorageRpcMetadataCommandPendingSlotCleanupOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => Err(metadata_command_terminal_log_conflict_error(
                self.node_id,
                command.id().cluster_epoch(),
                pg_id,
                command.id().log_index(),
                "decode metadata command pending slot remove response",
                |operation, message| self.rpc_payload_error(operation, message),
                MetadataCommandLogConflictRpcFields {
                    node_id,
                    pg_id: conflict_pg_id,
                    cluster_epoch,
                    log_index,
                },
            )),
        }
    }

    fn applied_metadata_command_log_entry_hashes_request_until(
        &self,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<Option<(u64, u64)>, StoreError> {
        let pg_id = self.metadata_command_pg_id();
        let payload = self.encode_metadata_command_request(pg_id, command)?;
        let response = self.rpc_request_until(
            StorageRpcMessageKind::MetadataCommandAppliedLogHashes,
            payload,
            deadline,
        )?;
        let response =
            decode_metadata_command_applied_hashes_response(&response).map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command applied hashes response",
                    error.to_string(),
                )
            })?;
        match response.outcome {
            StorageRpcMetadataCommandAppliedHashesOutcome::Hashes(hashes) => Ok(hashes),
            StorageRpcMetadataCommandAppliedHashesOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => Err(metadata_command_log_conflict_error(
                self.cluster_epoch,
                pg_id,
                "decode metadata command applied hashes response",
                |operation, message| self.rpc_payload_error(operation, message),
                MetadataCommandLogConflictRpcFields {
                    node_id,
                    pg_id: conflict_pg_id,
                    cluster_epoch,
                    log_index,
                },
            )),
        }
    }

    fn pending_metadata_command_publication_started_request_until(
        &self,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<bool, StoreError> {
        let pg_id = self.metadata_command_pg_id();
        let payload = self.encode_metadata_command_state_request(pg_id);
        let response = self.rpc_request_until(
            StorageRpcMessageKind::MetadataCommandPendingEnvelope,
            payload,
            deadline,
        )?;
        let response = decode_metadata_command_pending_envelope_response(
            &response,
            &MetadataCommandDecodeAuthority::new(),
        )
        .map_err(|error| {
            self.rpc_payload_error(
                "decode metadata command publication state response",
                error.to_string(),
            )
        })?;
        match response.command {
            Some(pending)
                if pending.id() == command.id()
                    && pending.checksum_crc64() == command.checksum_crc64()
                    && pending.command_bytes() == command.command_bytes() =>
            {
                Ok(response.publication_started)
            }
            _ => Ok(false),
        }
    }

    fn mark_pending_metadata_command_publication_started_request_until(
        &self,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<(), MetadataCommandApplyError> {
        let pg_id = self.metadata_command_pg_id();
        let payload = self
            .encode_metadata_command_request(pg_id, command)
            .map_err(MetadataCommandApplyError::not_sent)?;
        let response = match self.rpc_request_result_until(
            StorageRpcMessageKind::MetadataCommandPublicationStart,
            payload,
            deadline,
        ) {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                return Err(MetadataCommandApplyError::definitive(
                    self.rpc_response_error(
                        StorageRpcMessageKind::MetadataCommandPublicationStart,
                        error,
                    ),
                ));
            }
            Err(error) => return Err(error.into_metadata_command_apply_error()),
        };
        self.decode_metadata_command_state_outcome(
            pg_id,
            command,
            &response,
            "decode metadata command publication start response",
        )
        .map(|_| ())
    }

    fn decode_record_metadata_command_abandoned_response(
        &self,
        pg_id: PgId,
        response: &[u8],
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let response =
            decode_metadata_command_state_outcome_response(response).map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command record abandoned response",
                    error.to_string(),
                )
            })?;
        match response.outcome {
            StorageRpcMetadataCommandStateOutcome::State(state) => Ok(state),
            StorageRpcMetadataCommandStateOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => Err(metadata_command_log_conflict_error(
                self.cluster_epoch,
                pg_id,
                "decode metadata command record abandoned response",
                |operation, message| self.rpc_payload_error(operation, message),
                MetadataCommandLogConflictRpcFields {
                    node_id,
                    pg_id: conflict_pg_id,
                    cluster_epoch,
                    log_index,
                },
            )),
            _ => Err(self.rpc_payload_error(
                "decode metadata command record abandoned response",
                "record abandoned response contained a mutation conflict".to_string(),
            )),
        }
    }

    fn rpc_request(
        &self,
        kind: StorageRpcMessageKind,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, StoreError> {
        match self.rpc_request_result(kind, payload)? {
            Ok(payload) => Ok(payload),
            Err(error) => Err(self.rpc_response_error(kind, error)),
        }
    }

    fn rpc_request_until(
        &self,
        kind: StorageRpcMessageKind,
        payload: Vec<u8>,
        deadline: Instant,
    ) -> Result<Vec<u8>, StoreError> {
        match self
            .rpc_request_result_until(kind, payload, deadline)
            .map_err(StorageRpcRequestDispatchFailure::into_source)?
        {
            Ok(payload) => Ok(payload),
            Err(error) => Err(self.rpc_response_error(kind, error)),
        }
    }

    fn rpc_request_result(
        &self,
        kind: StorageRpcMessageKind,
        payload: Vec<u8>,
    ) -> Result<Result<Vec<u8>, StorageRpcErrorResponse>, StoreError> {
        self.rpc_request_result_classified(kind, payload)
            .map_err(StorageRpcRequestDispatchFailure::into_source)
    }

    fn rpc_request_result_classified(
        &self,
        kind: StorageRpcMessageKind,
        payload: Vec<u8>,
    ) -> Result<Result<Vec<u8>, StorageRpcErrorResponse>, StorageRpcRequestDispatchFailure> {
        let io_timeout = self
            .inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .io_timeout;
        let deadline = Instant::now().checked_add(io_timeout).ok_or_else(|| {
            StorageRpcRequestDispatchFailure::NotSent(self.rpc_payload_error(
                "set metadata command session RPC deadline",
                "storage-node metadata command session RPC deadline overflowed".to_string(),
            ))
        })?;
        self.rpc_request_result_until(kind, payload, deadline)
    }

    fn rpc_request_result_until(
        &self,
        kind: StorageRpcMessageKind,
        payload: Vec<u8>,
        deadline: Instant,
    ) -> Result<Result<Vec<u8>, StorageRpcErrorResponse>, StorageRpcRequestDispatchFailure> {
        if Instant::now() >= deadline {
            return Err(StorageRpcRequestDispatchFailure::NotSent(
                storage_rpc_deadline_expired("start metadata command session RPC"),
            ));
        }
        let mut inner = self
            .lock_until(deadline, "acquire metadata command session RPC")
            .map_err(StorageRpcRequestDispatchFailure::NotSent)?;
        self.rpc_request_result_locked(&mut inner, kind, payload, deadline)
    }

    fn rpc_request_result_locked(
        &self,
        inner: &mut UnixStorageNodeMetadataCommandSessionInner,
        kind: StorageRpcMessageKind,
        payload: Vec<u8>,
        deadline: Instant,
    ) -> Result<Result<Vec<u8>, StorageRpcErrorResponse>, StorageRpcRequestDispatchFailure> {
        if Instant::now() >= deadline {
            return Err(StorageRpcRequestDispatchFailure::NotSent(
                storage_rpc_deadline_expired("start metadata command session RPC"),
            ));
        }
        if inner.released {
            return Err(StorageRpcRequestDispatchFailure::NotSent(
                self.rpc_payload_error(
                    "start metadata command session RPC",
                    "metadata command session transport is closed".to_string(),
                ),
            ));
        }
        let result = (|| {
            inner
                .stream
                .set_operation_deadline(deadline)
                .map_err(|source| {
                    StorageRpcRequestDispatchFailure::NotSent(StoreError::Io {
                        context: "set storage-node metadata command session RPC deadline",
                        source,
                    })
                })?;
            let request_id = inner.next_request_id;
            inner.next_request_id = inner.next_request_id.checked_add(1).ok_or_else(|| {
                StorageRpcRequestDispatchFailure::NotSent(self.rpc_payload_error(
                    "allocate metadata command session request id",
                    "metadata command session request id overflowed".to_string(),
                ))
            })?;
            let request = StorageRpcFrame {
                request_id,
                kind,
                payload,
            };
            let request_proof = write_unix_storage_rpc_request_classified(
                &mut inner.stream,
                self.node_id,
                self.rpc_auth.as_deref(),
                &request,
                "write metadata command session RPC request",
            )?;
            let response = read_unix_storage_rpc_response(
                &mut inner.stream,
                self.node_id,
                self.rpc_auth.as_deref(),
                request_proof.as_ref(),
                "read metadata command session RPC response",
            )
            .map_err(StorageRpcRequestDispatchFailure::MayHaveApplied)?;
            if response.request_id != request_id || response.kind != kind {
                return Err(StorageRpcRequestDispatchFailure::MayHaveApplied(
                    self.rpc_payload_error(
                        "validate metadata command session RPC response",
                        format!(
                            "expected request {request_id} kind {kind:?}, got request {} kind {:?}",
                            response.request_id, response.kind
                        ),
                    ),
                ));
            }
            decode_storage_rpc_response_payload_with_connection_disposition(&response.payload)
                .map_err(|error| {
                    StorageRpcRequestDispatchFailure::MayHaveApplied(self.rpc_payload_error(
                        "decode metadata command session RPC response",
                        error.to_string(),
                    ))
                })
        })();
        // A failed exchange cannot prove the remote critical section is still
        // held. Do not let it authorize a later local application (or reuse a
        // potentially desynchronized stream). A peer-declared closed connection
        // also revokes the borrow, even when its operation succeeded. Semantic
        // errors keep the session only when the peer explicitly retains it.
        if !result
            .as_ref()
            .is_ok_and(|response| response.connection_reusable)
        {
            let _ = inner.stream.shutdown(std::net::Shutdown::Both);
            inner.released = true;
        }
        result.map(|response| response.response)
    }

    fn rpc_response_error(
        &self,
        kind: StorageRpcMessageKind,
        error: StorageRpcErrorResponse,
    ) -> StoreError {
        storage_rpc_response_error(self.node_id, kind, error)
    }

    fn rpc_payload_error(&self, operation: &'static str, message: String) -> StoreError {
        StoreError::StorageRpc {
            node_id: self.node_id.as_u32(),
            operation,
            failure: StorageRpcErrorCode::PayloadDecode,
            detail: crate::StorageNodeFailureDetail::new(message),
        }
    }

    fn close_metadata_command_pg_lock_on_drop(&self) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if inner.released {
            return;
        }
        let _ = inner.stream.shutdown(std::net::Shutdown::Both);
        inner.released = true;
    }

    fn metadata_command_acceptance_request(
        &self,
        kind: StorageRpcMessageKind,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        let payload = self.encode_metadata_command_request(pg_id, command)?;
        let response = self.rpc_request(kind, payload)?;
        self.decode_metadata_command_acceptance_response(pg_id, &response)
    }

    fn metadata_command_acceptance_request_until(
        &self,
        kind: StorageRpcMessageKind,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        let payload = self.encode_metadata_command_request(pg_id, command)?;
        let response = self.rpc_request_until(kind, payload, deadline)?;
        self.decode_metadata_command_acceptance_response(pg_id, &response)
    }

    fn decode_metadata_command_acceptance_response(
        &self,
        pg_id: PgId,
        response: &[u8],
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        let response = decode_metadata_command_acceptance_response(response).map_err(|error| {
            self.rpc_payload_error(
                "decode metadata command acceptance response",
                error.to_string(),
            )
        })?;
        match response.outcome {
            StorageRpcMetadataCommandAcceptanceOutcome::Acceptance(acceptance) => Ok(acceptance),
            StorageRpcMetadataCommandAcceptanceOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => Err(metadata_command_log_conflict_error(
                self.cluster_epoch,
                pg_id,
                "decode metadata command acceptance response",
                |operation, message| self.rpc_payload_error(operation, message),
                MetadataCommandLogConflictRpcFields {
                    node_id,
                    pg_id: conflict_pg_id,
                    cluster_epoch,
                    log_index,
                },
            )),
        }
    }

    fn metadata_command_apply_and_record_with_kind(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        kind: StorageRpcMessageKind,
        decode_context: &'static str,
    ) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError> {
        let payload = self
            .encode_metadata_command_request(pg_id, command)
            .map_err(BucketSnapshotLoadError::Store)?;
        self.metadata_command_apply_and_record_with_payload(
            pg_id,
            command,
            kind,
            payload,
            decode_context,
        )
    }

    fn metadata_command_apply_and_record_with_payload(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        kind: StorageRpcMessageKind,
        payload: Vec<u8>,
        decode_context: &'static str,
    ) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError> {
        let io_timeout = self
            .inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .io_timeout;
        let deadline = Instant::now().checked_add(io_timeout).ok_or_else(|| {
            BucketSnapshotLoadError::Store(StoreError::Io {
                context: "compute metadata command session apply deadline",
                source: io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "metadata command session apply deadline overflowed",
                ),
            })
        })?;
        self.metadata_command_apply_and_record_with_payload_until(
            pg_id,
            command,
            kind,
            payload,
            decode_context,
            deadline,
        )
        .map_err(MetadataCommandApplyError::into_source)
    }

    fn metadata_command_apply_and_record_with_payload_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        kind: StorageRpcMessageKind,
        payload: Vec<u8>,
        decode_context: &'static str,
        deadline: Instant,
    ) -> Result<MetadataCommandReplicaState, MetadataCommandApplyError> {
        let response = match self.rpc_request_result_until(kind, payload, deadline) {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                return Err(MetadataCommandApplyError::definitive(
                    self.rpc_response_error(kind, error),
                ));
            }
            Err(error) => return Err(error.into_metadata_command_apply_error()),
        };
        self.decode_metadata_command_state_outcome(pg_id, command, &response, decode_context)
    }

    fn decode_metadata_command_state_outcome(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        response: &[u8],
        decode_context: &'static str,
    ) -> Result<MetadataCommandReplicaState, MetadataCommandApplyError> {
        let response = decode_metadata_command_state_outcome_response(response)
            .map_err(|error| self.rpc_payload_error(decode_context, error.to_string()))
            .map_err(MetadataCommandApplyError::may_have_applied)?;
        match response.outcome {
            StorageRpcMetadataCommandStateOutcome::State(state) => Ok(state),
            StorageRpcMetadataCommandStateOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => Err(MetadataCommandApplyError::definitive(
                BucketSnapshotLoadError::Store(metadata_command_log_conflict_error(
                    self.cluster_epoch,
                    pg_id,
                    decode_context,
                    |operation, message| self.rpc_payload_error(operation, message),
                    MetadataCommandLogConflictRpcFields {
                        node_id,
                        pg_id: conflict_pg_id,
                        cluster_epoch,
                        log_index,
                    },
                )),
            )),
            StorageRpcMetadataCommandStateOutcome::LogGap {
                node_id,
                pg_id: gap_pg_id,
                cluster_epoch,
                log_index,
                expected_log_index,
            } => Err(MetadataCommandApplyError::definitive(
                BucketSnapshotLoadError::Store(metadata_command_log_gap_error(
                    self.node_id,
                    self.cluster_epoch,
                    pg_id,
                    command.id().log_index(),
                    decode_context,
                    |operation, message| self.rpc_payload_error(operation, message),
                    MetadataCommandLogGapRpcFields {
                        node_id,
                        pg_id: gap_pg_id,
                        cluster_epoch,
                        log_index,
                        expected_log_index,
                    },
                )),
            )),
            StorageRpcMetadataCommandStateOutcome::ObjectGenerationReservationConflict {
                reservation_id,
                generation_id,
            } => Err(MetadataCommandApplyError::definitive(
                BucketSnapshotLoadError::Metadata(
                    MetadataError::ObjectGenerationReservationConflict {
                        reservation_id: reservation_id.into_string(),
                        generation_id: generation_id.get(),
                    },
                ),
            )),
            StorageRpcMetadataCommandStateOutcome::ObjectVersionReservationConflict {
                version_id,
            } => Err(MetadataCommandApplyError::definitive(
                BucketSnapshotLoadError::Metadata(
                    MetadataError::ObjectVersionReservationConflict { version_id },
                ),
            )),
            StorageRpcMetadataCommandStateOutcome::StaleBucketMetadataCommand {
                name,
                bucket_execution_generation,
            } => match stale_bucket_metadata_command_error(
                command,
                name,
                bucket_execution_generation,
                decode_context,
                |operation, message| self.rpc_payload_error(operation, message),
            ) {
                Ok(error) => Err(MetadataCommandApplyError::definitive(
                    BucketSnapshotLoadError::Metadata(error),
                )),
                Err(error) => Err(MetadataCommandApplyError::definitive(
                    BucketSnapshotLoadError::Store(error),
                )),
            },
            StorageRpcMetadataCommandStateOutcome::StaleObjectWriteCommand {
                bucket,
                key,
                write_sequence,
                generation_id,
            } => match stale_object_write_command_error(
                command,
                bucket,
                key,
                write_sequence,
                generation_id,
                decode_context,
                |operation, message| self.rpc_payload_error(operation, message),
            ) {
                Ok(error) => Err(MetadataCommandApplyError::definitive(
                    BucketSnapshotLoadError::Metadata(error),
                )),
                Err(error) => Err(MetadataCommandApplyError::definitive(
                    BucketSnapshotLoadError::Store(error),
                )),
            },
            StorageRpcMetadataCommandStateOutcome::StreamSegmentConflict { segment_index } => Err(
                MetadataCommandApplyError::definitive(BucketSnapshotLoadError::Metadata(
                    MetadataError::StreamSegmentConflict { segment_index },
                )),
            ),
            StorageRpcMetadataCommandStateOutcome::StreamUploadNoSuchUpload {
                session_id,
                upload_id,
            } => match stream_upload_no_such_upload_error(
                command,
                session_id,
                upload_id,
                decode_context,
                |operation, message| self.rpc_payload_error(operation, message),
            ) {
                Ok(error) => Err(MetadataCommandApplyError::definitive(
                    BucketSnapshotLoadError::Metadata(error),
                )),
                Err(error) => Err(MetadataCommandApplyError::definitive(
                    BucketSnapshotLoadError::Store(error),
                )),
            },
        }
    }

    fn try_insert_pending_metadata_command_slot_with_effect_deadline(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
        effect_deadline: Option<StorageRpcAdmittedRouteEffectDeadline>,
    ) -> Result<(), StoreError> {
        self.try_insert_pending_metadata_command_slot_with_effect_deadline_until(
            pg_id,
            command,
            bucket,
            effect_deadline,
            None,
        )
    }

    fn try_insert_pending_metadata_command_slot_with_effect_deadline_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
        effect_deadline: Option<StorageRpcAdmittedRouteEffectDeadline>,
        deadline: Option<Instant>,
    ) -> Result<(), StoreError> {
        let operation_deadline = deadline.map(StorageRpcOperationDeadline::from_instant);
        let effect_deadline = operation_deadline.map_or(effect_deadline, |operation_deadline| {
            Some(
                StorageRpcAdmittedRouteEffectDeadline::intersect_existing_operation_deadline(
                    effect_deadline,
                    operation_deadline.portable_wall_valid_until_ms,
                ),
            )
        });
        let request = StorageRpcMetadataCommandPendingSlotRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            command: command.clone(),
            scope_bucket: bucket.cloned(),
            effect_deadline,
            operation_deadline,
        };
        let payload = encode_metadata_command_pending_slot_request(&request).map_err(|error| {
            self.rpc_payload_error(
                "encode metadata command pending slot request",
                error.to_string(),
            )
        })?;
        let response = match deadline {
            Some(deadline) => self.rpc_request_until(
                StorageRpcMessageKind::MetadataCommandPendingSlotInsert,
                payload,
                deadline,
            )?,
            None => self.rpc_request(
                StorageRpcMessageKind::MetadataCommandPendingSlotInsert,
                payload,
            )?,
        };
        let response =
            decode_metadata_command_pending_slot_insert_response(&response).map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command pending slot insert response",
                    error.to_string(),
                )
            })?;
        match response.outcome {
            StorageRpcMetadataCommandPendingSlotInsertOutcome::Inserted => Ok(()),
            StorageRpcMetadataCommandPendingSlotInsertOutcome::PendingConflict {
                pg_id,
                cluster_epoch,
                existing_log_index,
                candidate_log_index,
            } => Err(StoreError::MetadataCommandPendingConflict {
                pg_id,
                cluster_epoch,
                existing_log_index,
                candidate_log_index,
            }),
            StorageRpcMetadataCommandPendingSlotInsertOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => Err(metadata_command_log_conflict_error(
                self.cluster_epoch,
                pg_id,
                "decode metadata command pending slot insert response",
                |operation, message| self.rpc_payload_error(operation, message),
                MetadataCommandLogConflictRpcFields {
                    node_id,
                    pg_id: conflict_pg_id,
                    cluster_epoch,
                    log_index,
                },
            )),
        }
    }
}
impl Drop for UnixStorageNodeMetadataCommandSession {
    fn drop(&mut self) {
        self.close_metadata_command_pg_lock_on_drop();
    }
}

impl MetadataCommandRecoveryCriticalSection for UnixStorageNodeMetadataCommandSession {
    fn max_metadata_command_log_index(&self) -> Result<u64, StoreError> {
        MetadataCommandNodeClient::max_metadata_command_log_index(
            self,
            self.metadata_command_pg_id(),
            self.cluster_epoch,
        )
    }

    fn max_metadata_command_log_index_until(&self, deadline: Instant) -> Result<u64, StoreError> {
        let payload = self.encode_metadata_command_state_request(self.metadata_command_pg_id());
        let response = self.rpc_request_until(
            StorageRpcMessageKind::MetadataCommandMaxLogIndex,
            payload,
            deadline,
        )?;
        decode_metadata_command_max_log_index_response(&response)
            .map(|response| response.max_log_index)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command max log index response",
                    error.to_string(),
                )
            })
    }

    fn pending_metadata_command_envelope(
        &self,
    ) -> Result<Option<MetadataCommandEnvelope>, StoreError> {
        MetadataCommandNodeClient::pending_metadata_command_envelope(
            self,
            self.metadata_command_pg_id(),
            self.cluster_epoch,
        )
    }

    fn pending_metadata_command_envelope_until(
        &self,
        deadline: Instant,
    ) -> Result<Option<MetadataCommandEnvelope>, StoreError> {
        let pg_id = self.metadata_command_pg_id();
        let payload = self.encode_metadata_command_state_request(pg_id);
        let response = self.rpc_request_until(
            StorageRpcMessageKind::MetadataCommandPendingEnvelope,
            payload,
            deadline,
        )?;
        let response = decode_metadata_command_pending_envelope_response(
            &response,
            &MetadataCommandDecodeAuthority::new(),
        )
        .map_err(|error| {
            self.rpc_payload_error(
                "decode metadata command pending envelope response",
                error.to_string(),
            )
        })?;
        if let Some(command) = response.command.as_ref() {
            if command.id().cluster_epoch() != self.cluster_epoch || command.id().pg_id() != pg_id {
                return Err(self.rpc_payload_error(
                    "decode metadata command pending envelope response",
                    "metadata command pending envelope route mismatch".to_string(),
                ));
            }
        }
        Ok(response.command)
    }

    fn metadata_command_acceptance(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        MetadataCommandNodeClient::metadata_command_acceptance(
            self,
            self.metadata_command_pg_id(),
            command,
        )
    }

    fn metadata_command_acceptance_until(
        &self,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        self.metadata_command_acceptance_request_until(
            StorageRpcMessageKind::MetadataCommandAcceptance,
            self.metadata_command_pg_id(),
            command,
            deadline,
        )
    }

    fn metadata_command_abandon_acceptance(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        MetadataCommandNodeClient::metadata_command_abandon_acceptance(
            self,
            self.metadata_command_pg_id(),
            command,
        )
    }

    fn pending_metadata_command_publication_started_until(
        &self,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<bool, StoreError> {
        self.pending_metadata_command_publication_started_request_until(command, deadline)
    }

    fn mark_pending_metadata_command_publication_started_until(
        &self,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<(), MetadataCommandApplyError> {
        self.mark_pending_metadata_command_publication_started_request_until(command, deadline)
    }

    fn applied_metadata_command_log_entry_hashes_until(
        &self,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<Option<(u64, u64)>, StoreError> {
        self.applied_metadata_command_log_entry_hashes_request_until(command, deadline)
    }

    fn metadata_command_abandon_acceptance_until(
        &self,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        self.metadata_command_acceptance_request_until(
            StorageRpcMessageKind::MetadataCommandAbandonAcceptance,
            self.metadata_command_pg_id(),
            command,
            deadline,
        )
    }

    fn replace_pending_metadata_command_slot_for_reissue(
        &self,
        previous: &MetadataCommandEnvelope,
        replacement: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
    ) -> Result<bool, MetadataCommandPendingSlotReplaceError> {
        let deadline = Instant::now()
            .checked_add(
                self.inner
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .io_timeout,
            )
            .ok_or_else(|| {
                MetadataCommandPendingSlotReplaceError::not_sent(self.rpc_payload_error(
                    "set metadata command pending slot replace deadline",
                    "metadata command pending slot replace deadline overflowed".to_string(),
                ))
            })?;
        self.replace_pending_metadata_command_slot_for_reissue_until(
            previous,
            replacement,
            bucket,
            deadline,
        )
    }

    fn replace_pending_metadata_command_slot_for_reissue_until(
        &self,
        previous: &MetadataCommandEnvelope,
        replacement: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
        deadline: Instant,
    ) -> Result<bool, MetadataCommandPendingSlotReplaceError> {
        let pg_id = self.metadata_command_pg_id();
        let request = StorageRpcMetadataCommandPendingSlotReplaceRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            previous: previous.clone(),
            replacement: replacement.clone(),
            scope_bucket: bucket.cloned(),
        };
        let payload =
            encode_metadata_command_pending_slot_replace_request(&request).map_err(|error| {
                MetadataCommandPendingSlotReplaceError::not_sent(self.rpc_payload_error(
                    "encode metadata command pending slot replace request",
                    error.to_string(),
                ))
            })?;
        let kind = StorageRpcMessageKind::MetadataCommandPendingSlotReplace;
        let response = match self.rpc_request_result_until(kind, payload, deadline) {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                let uncertain = error.code == StorageRpcErrorCode::MetadataCommandMutationUncertain;
                let source = self.rpc_response_error(kind, error);
                return Err(if uncertain {
                    MetadataCommandPendingSlotReplaceError::may_have_applied(source)
                } else {
                    MetadataCommandPendingSlotReplaceError::definitive(source)
                });
            }
            Err(error) => {
                return Err(error.into_metadata_command_pending_slot_replace_error());
            }
        };
        decode_metadata_command_pending_slot_remove_response(&response)
            .map(|response| response.removed)
            .map_err(|error| {
                MetadataCommandPendingSlotReplaceError::may_have_applied(self.rpc_payload_error(
                    "decode metadata command pending slot replace response",
                    error.to_string(),
                ))
            })
    }

    fn replace_pending_metadata_command_slot_for_recovery(
        &self,
        authorized_source: &MetadataCommandEnvelope,
        abandoned_source: Option<&MetadataCommandEnvelope>,
        previous: &MetadataCommandEnvelope,
        replacement: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
    ) -> Result<bool, MetadataCommandPendingSlotReplaceError> {
        let deadline = Instant::now()
            .checked_add(
                self.inner
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .io_timeout,
            )
            .ok_or_else(|| {
                MetadataCommandPendingSlotReplaceError::not_sent(
                    self.rpc_payload_error(
                        "set metadata command recovery pending slot replace deadline",
                        "metadata command recovery pending slot replace deadline overflowed"
                            .to_string(),
                    ),
                )
            })?;
        self.replace_pending_metadata_command_slot_for_recovery_until(
            authorized_source,
            abandoned_source,
            previous,
            replacement,
            bucket,
            deadline,
        )
    }

    fn replace_pending_metadata_command_slot_for_recovery_until(
        &self,
        authorized_source: &MetadataCommandEnvelope,
        abandoned_source: Option<&MetadataCommandEnvelope>,
        previous: &MetadataCommandEnvelope,
        replacement: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
        deadline: Instant,
    ) -> Result<bool, MetadataCommandPendingSlotReplaceError> {
        let pg_id = self.metadata_command_pg_id();
        let request = StorageRpcMetadataCommandRecoveryPendingSlotReplaceRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            authorized_source: authorized_source.clone(),
            abandoned_source: abandoned_source.cloned(),
            previous: previous.clone(),
            replacement: replacement.clone(),
            scope_bucket: bucket.cloned(),
        };
        let payload = encode_metadata_command_recovery_pending_slot_replace_request(&request)
            .map_err(|error| {
                MetadataCommandPendingSlotReplaceError::not_sent(self.rpc_payload_error(
                    "encode metadata command recovery pending slot replace request",
                    error.to_string(),
                ))
            })?;
        let kind = StorageRpcMessageKind::MetadataCommandRecoveryPendingSlotReplace;
        let response = match self.rpc_request_result_until(kind, payload, deadline) {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                let uncertain = error.code == StorageRpcErrorCode::MetadataCommandMutationUncertain;
                let source = self.rpc_response_error(kind, error);
                return Err(if uncertain {
                    MetadataCommandPendingSlotReplaceError::may_have_applied(source)
                } else {
                    MetadataCommandPendingSlotReplaceError::definitive(source)
                });
            }
            Err(error) => {
                return Err(error.into_metadata_command_pending_slot_replace_error());
            }
        };
        decode_metadata_command_pending_slot_remove_response(&response)
            .map(|response| response.removed)
            .map_err(|error| {
                MetadataCommandPendingSlotReplaceError::may_have_applied(self.rpc_payload_error(
                    "decode metadata command recovery pending slot replace response",
                    error.to_string(),
                ))
            })
    }

    fn apply_metadata_command_and_record_for_recovery(
        &self,
        authorized_source: &MetadataCommandEnvelope,
        abandoned_source: Option<&MetadataCommandEnvelope>,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError> {
        let pg_id = self.metadata_command_pg_id();
        let request = StorageRpcMetadataCommandRecoveryRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            authorized_source: authorized_source.clone(),
            abandoned_source: abandoned_source.cloned(),
            command: command.clone(),
        };
        let payload = encode_metadata_command_recovery_request(&request)
            .map_err(|error| {
                self.rpc_payload_error(
                    "encode metadata command recovery apply and record request",
                    error.to_string(),
                )
            })
            .map_err(BucketSnapshotLoadError::Store)?;
        self.metadata_command_apply_and_record_with_payload(
            pg_id,
            command,
            StorageRpcMessageKind::MetadataCommandRecoveryApplyAndRecord,
            payload,
            "decode metadata command recovery apply and record response",
        )
    }

    fn apply_metadata_command_and_record_for_recovery_until(
        &self,
        authorized_source: &MetadataCommandEnvelope,
        abandoned_source: Option<&MetadataCommandEnvelope>,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<MetadataCommandReplicaState, MetadataCommandApplyError> {
        let pg_id = self.metadata_command_pg_id();
        let request = StorageRpcMetadataCommandRecoveryRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            authorized_source: authorized_source.clone(),
            abandoned_source: abandoned_source.cloned(),
            command: command.clone(),
        };
        let payload = encode_metadata_command_recovery_request(&request)
            .map_err(|error| {
                self.rpc_payload_error(
                    "encode metadata command recovery apply and record request",
                    error.to_string(),
                )
            })
            .map_err(MetadataCommandApplyError::not_sent)?;
        self.metadata_command_apply_and_record_with_payload_until(
            pg_id,
            command,
            StorageRpcMessageKind::MetadataCommandRecoveryApplyAndRecord,
            payload,
            "decode metadata command recovery apply and record response",
            deadline,
        )
    }

    fn record_metadata_command_abandoned(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let pg_id = self.metadata_command_pg_id();
        let payload = self.encode_metadata_command_request(pg_id, command)?;
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandRecordAbandoned,
            payload,
        )?;
        self.decode_record_metadata_command_abandoned_response(pg_id, &response)
    }

    fn record_metadata_command_abandoned_until(
        &self,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let pg_id = self.metadata_command_pg_id();
        let payload = self.encode_metadata_command_request(pg_id, command)?;
        let response = self.rpc_request_until(
            StorageRpcMessageKind::MetadataCommandRecordAbandoned,
            payload,
            deadline,
        )?;
        self.decode_record_metadata_command_abandoned_response(pg_id, &response)
    }
}

impl MetadataCommandCriticalSection for UnixStorageNodeMetadataCommandSession {
    fn metadata_command_acceptance(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        MetadataCommandNodeClient::metadata_command_acceptance(
            self,
            self.metadata_command_pg_id(),
            command,
        )
    }

    fn metadata_command_acceptance_until(
        &self,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        self.metadata_command_acceptance_request_until(
            StorageRpcMessageKind::MetadataCommandAcceptance,
            self.metadata_command_pg_id(),
            command,
            deadline,
        )
    }

    fn applied_metadata_command_log_entry_hashes_until(
        &self,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<Option<(u64, u64)>, StoreError> {
        self.applied_metadata_command_log_entry_hashes_request_until(command, deadline)
    }

    fn metadata_command_abandon_acceptance_until(
        &self,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        self.metadata_command_acceptance_request_until(
            StorageRpcMessageKind::MetadataCommandAbandonAcceptance,
            self.metadata_command_pg_id(),
            command,
            deadline,
        )
    }

    fn pending_metadata_command_publication_started_until(
        &self,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<bool, StoreError> {
        self.pending_metadata_command_publication_started_request_until(command, deadline)
    }

    fn mark_pending_metadata_command_publication_started_until(
        &self,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<(), MetadataCommandApplyError> {
        self.mark_pending_metadata_command_publication_started_request_until(command, deadline)
    }

    fn apply_metadata_command_and_record(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError> {
        MetadataCommandNodeClient::apply_metadata_command_and_record(
            self,
            self.metadata_command_pg_id(),
            command,
        )
    }

    fn apply_metadata_command_and_record_until(
        &self,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<MetadataCommandReplicaState, MetadataCommandApplyError> {
        MetadataCommandNodeClient::apply_metadata_command_and_record_until(
            self,
            self.metadata_command_pg_id(),
            command,
            deadline,
        )
    }
}

impl UnixStorageNodeMetadataCommandSession {
    fn try_insert_bucket_control_pending_metadata_command_slot_with_effect_fence_inner(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: &BucketName,
        effect_fence: AdmittedRouteEffectFence,
        deadline: Option<Instant>,
    ) -> Result<bool, StoreError> {
        let operation_deadline = deadline.map(StorageRpcOperationDeadline::from_instant);
        let existing_effect_deadline =
            effect_fence
                .deadline()
                .map(|deadline| StorageRpcAdmittedRouteEffectDeadline {
                    authority_valid_until_ms: deadline.authority_valid_until_ms(),
                    portable_wall_valid_until_ms: deadline.portable_wall_valid_until_ms(),
                });
        let effect_deadline = operation_deadline.map_or(existing_effect_deadline, |operation| {
            Some(
                StorageRpcAdmittedRouteEffectDeadline::intersect_existing_operation_deadline(
                    existing_effect_deadline,
                    operation.portable_wall_valid_until_ms,
                ),
            )
        });
        let request = StorageRpcMetadataCommandPendingSlotRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            command: command.clone(),
            scope_bucket: Some(bucket.clone()),
            effect_deadline,
            operation_deadline,
        };
        let payload = encode_metadata_command_pending_slot_request(&request).map_err(|error| {
            self.rpc_payload_error(
                "encode metadata command bucket-control pending slot request",
                error.to_string(),
            )
        })?;
        let response = match deadline {
            Some(deadline) => self.rpc_request_until(
                StorageRpcMessageKind::MetadataCommandBucketControlPendingSlotInsert,
                payload,
                deadline,
            )?,
            None => self.rpc_request(
                StorageRpcMessageKind::MetadataCommandBucketControlPendingSlotInsert,
                payload,
            )?,
        };
        let response =
            decode_metadata_command_bool_outcome_response(&response).map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command bucket-control pending slot insert response",
                    error.to_string(),
                )
            })?;
        match response.outcome {
            StorageRpcMetadataCommandBoolOutcome::Value(value) => Ok(value),
            StorageRpcMetadataCommandBoolOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => Err(metadata_command_log_conflict_error(
                self.cluster_epoch,
                pg_id,
                "decode metadata command bucket-control pending slot insert response",
                |operation, message| self.rpc_payload_error(operation, message),
                MetadataCommandLogConflictRpcFields {
                    node_id,
                    pg_id: conflict_pg_id,
                    cluster_epoch,
                    log_index,
                },
            )),
        }
    }
}

impl MetadataCommandNodeClient for UnixStorageNodeMetadataCommandSession {
    fn open_metadata_command_critical_section(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<Box<dyn MetadataCommandCriticalSection>, StoreError> {
        if cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        let held_pg_id = self.pg_id;
        if pg_id != held_pg_id {
            return Err(self.rpc_payload_error(
                "open nested metadata command critical section",
                format!(
                    "session holds metadata command PG {}, not requested PG {}",
                    held_pg_id.get(),
                    pg_id.get()
                ),
            ));
        }
        Err(self.rpc_payload_error(
            "open nested metadata command critical section",
            "nested metadata command critical sections are not supported".to_string(),
        ))
    }

    fn max_metadata_command_log_index(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<u64, StoreError> {
        if cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        let payload = self.encode_metadata_command_state_request(pg_id);
        let response =
            self.rpc_request(StorageRpcMessageKind::MetadataCommandMaxLogIndex, payload)?;
        decode_metadata_command_max_log_index_response(&response)
            .map(|response| response.max_log_index)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command max log index response",
                    error.to_string(),
                )
            })
    }

    fn next_metadata_command_id_at_least(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        min_log_index: MetadataCommandLogIndex,
    ) -> Result<MetadataCommandId, StoreError> {
        if cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        let request = StorageRpcMetadataCommandNextIdRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            min_log_index: min_log_index.get(),
        };
        let payload = encode_metadata_command_next_id_request(&request);
        let response = self.rpc_request(StorageRpcMessageKind::MetadataCommandNextId, payload)?;
        let decoded = decode_metadata_command_next_id_response(&response).map_err(|error| {
            self.rpc_payload_error(
                "decode metadata command next id response",
                error.to_string(),
            )
        })?;
        match decoded.outcome {
            StorageRpcMetadataCommandNextIdOutcome::Allocated {
                cluster_epoch,
                pg_id: decoded_pg_id,
                log_index,
            } => {
                let Some(log_index) = MetadataCommandLogIndex::new(log_index) else {
                    return Err(self.rpc_payload_error(
                        "decode metadata command next id response",
                        "metadata command log index must not be zero".to_string(),
                    ));
                };
                if cluster_epoch != self.cluster_epoch || decoded_pg_id != pg_id {
                    return Err(self.rpc_payload_error(
                        "decode metadata command next id response",
                        "metadata command id route mismatch".to_string(),
                    ));
                }
                Ok(MetadataCommandId::new(
                    cluster_epoch,
                    decoded_pg_id,
                    log_index,
                ))
            }
            StorageRpcMetadataCommandNextIdOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => {
                if cluster_epoch != self.cluster_epoch || conflict_pg_id != pg_id.get() {
                    return Err(self.rpc_payload_error(
                        "decode metadata command next id response",
                        "metadata command log conflict route mismatch".to_string(),
                    ));
                }
                if MetadataCommandLogIndex::new(log_index).is_none() {
                    return Err(self.rpc_payload_error(
                        "decode metadata command next id response",
                        "metadata command log conflict index must not be zero".to_string(),
                    ));
                }
                Err(StoreError::MetadataCommandLogConflict {
                    node_id,
                    pg_id: conflict_pg_id,
                    cluster_epoch,
                    log_index,
                })
            }
        }
    }

    fn pending_metadata_command_envelope(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<Option<MetadataCommandEnvelope>, StoreError> {
        if cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        let payload = self.encode_metadata_command_state_request(pg_id);
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandPendingEnvelope,
            payload,
        )?;
        let response = decode_metadata_command_pending_envelope_response(
            &response,
            &MetadataCommandDecodeAuthority::new(),
        )
        .map_err(|error| {
            self.rpc_payload_error(
                "decode metadata command pending envelope response",
                error.to_string(),
            )
        })?;
        if let Some(command) = response.command.as_ref() {
            if command.id().cluster_epoch() != self.cluster_epoch || command.id().pg_id() != pg_id {
                return Err(self.rpc_payload_error(
                    "decode metadata command pending envelope response",
                    "metadata command pending envelope route mismatch".to_string(),
                ));
            }
        }
        Ok(response.command)
    }

    fn try_insert_pending_metadata_command_slot(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
    ) -> Result<(), StoreError> {
        self.try_insert_pending_metadata_command_slot_with_effect_deadline(
            pg_id, command, bucket, None,
        )
    }

    fn try_insert_pending_metadata_command_slot_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
        deadline: Instant,
    ) -> Result<(), StoreError> {
        self.try_insert_pending_metadata_command_slot_with_effect_deadline_until(
            pg_id,
            command,
            bucket,
            None,
            Some(deadline),
        )
    }

    fn try_insert_pending_metadata_command_slot_with_effect_fence(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<(), StoreError> {
        effect_fence.require_valid_for(command.id().cluster_epoch())?;
        self.try_insert_pending_metadata_command_slot_with_effect_deadline(
            pg_id,
            command,
            bucket,
            effect_fence
                .deadline()
                .map(|deadline| StorageRpcAdmittedRouteEffectDeadline {
                    authority_valid_until_ms: deadline.authority_valid_until_ms(),
                    portable_wall_valid_until_ms: deadline.portable_wall_valid_until_ms(),
                }),
        )
    }

    fn try_insert_pending_metadata_command_slot_with_effect_fence_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: Option<&BucketName>,
        effect_fence: AdmittedRouteEffectFence,
        deadline: Instant,
    ) -> Result<(), StoreError> {
        effect_fence.require_valid_for(command.id().cluster_epoch())?;
        self.try_insert_pending_metadata_command_slot_with_effect_deadline_until(
            pg_id,
            command,
            bucket,
            effect_fence
                .deadline()
                .map(|deadline| StorageRpcAdmittedRouteEffectDeadline {
                    authority_valid_until_ms: deadline.authority_valid_until_ms(),
                    portable_wall_valid_until_ms: deadline.portable_wall_valid_until_ms(),
                }),
            Some(deadline),
        )
    }

    fn try_insert_bucket_control_pending_metadata_command_slot(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: &BucketName,
    ) -> Result<bool, StoreError> {
        self.try_insert_bucket_control_pending_metadata_command_slot_with_effect_fence(
            pg_id,
            command,
            bucket,
            AdmittedRouteEffectFence::unbounded(command.id().cluster_epoch()),
        )
    }

    fn try_insert_bucket_control_pending_metadata_command_slot_with_effect_fence(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: &BucketName,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<bool, StoreError> {
        effect_fence.require_valid_for(command.id().cluster_epoch())?;
        self.try_insert_bucket_control_pending_metadata_command_slot_with_effect_fence_inner(
            pg_id,
            command,
            bucket,
            effect_fence,
            None,
        )
    }

    fn try_insert_bucket_control_pending_metadata_command_slot_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: &BucketName,
        deadline: Instant,
    ) -> Result<bool, StoreError> {
        self.try_insert_bucket_control_pending_metadata_command_slot_with_effect_fence_inner(
            pg_id,
            command,
            bucket,
            AdmittedRouteEffectFence::unbounded(command.id().cluster_epoch()),
            Some(deadline),
        )
    }

    fn try_insert_bucket_control_pending_metadata_command_slot_with_effect_fence_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        bucket: &BucketName,
        effect_fence: AdmittedRouteEffectFence,
        deadline: Instant,
    ) -> Result<bool, StoreError> {
        effect_fence.require_valid_for(command.id().cluster_epoch())?;
        self.try_insert_bucket_control_pending_metadata_command_slot_with_effect_fence_inner(
            pg_id,
            command,
            bucket,
            effect_fence,
            Some(deadline),
        )
    }

    fn remove_pending_metadata_command_slot(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, StoreError> {
        let payload =
            self.encode_metadata_command_pending_slot_operation_request(pg_id, command, None)?;
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandPendingSlotRemove,
            payload,
        )?;
        self.decode_pending_metadata_command_slot_cleanup_response(pg_id, command, &response)
    }

    fn remove_pending_metadata_command_slot_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<bool, StoreError> {
        let payload = self.encode_metadata_command_pending_slot_operation_request(
            pg_id,
            command,
            Some(deadline),
        )?;
        let response = self.rpc_request_until(
            StorageRpcMessageKind::MetadataCommandPendingSlotRemove,
            payload,
            deadline,
        )?;
        self.decode_pending_metadata_command_slot_cleanup_response(pg_id, command, &response)
    }

    fn metadata_command_replica_state(
        &self,
        pg_id: PgId,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let payload = self.encode_metadata_command_state_request(pg_id);
        let response =
            self.rpc_request(StorageRpcMessageKind::MetadataCommandReplicaState, payload)?;
        decode_metadata_command_state_response(&response)
            .map(|response| response.state)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command replica state response",
                    error.to_string(),
                )
            })
    }

    fn metadata_command_checkpoint(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandCheckpoint, StoreError> {
        if cluster_epoch > self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        let payload =
            encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
                node_id: self.node_id,
                cluster_epoch,
                pg_id,
            });
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandCheckpointExport,
            payload,
        )?;
        decode_metadata_command_checkpoint_response(&response)
            .map(|response| response.checkpoint)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command checkpoint response",
                    error.to_string(),
                )
            })
    }

    fn record_current_metadata_command_checkpoint(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        if cluster_epoch > self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        let payload =
            encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
                node_id: self.node_id,
                cluster_epoch,
                pg_id,
            });
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandCheckpointRecordCurrent,
            payload,
        )?;
        decode_metadata_command_state_response(&response)
            .map(|response| response.state)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command checkpoint record current response",
                    error.to_string(),
                )
            })
    }

    fn metadata_command_checkpoint_candidates(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        max_applied_log_index: u64,
        limit: usize,
    ) -> Result<Vec<MetadataCommandCheckpoint>, StoreError> {
        if cluster_epoch > self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        let limit = u32::try_from(limit).map_err(|_| StoreError::StorageRpc {
            operation: "metadata command checkpoint candidates",
            node_id: self.node_id.as_u32(),
            failure: StorageRpcErrorCode::PayloadDecode,
            detail: crate::StorageNodeFailureDetail::new(format!(
                "checkpoint candidate limit {limit} exceeds u32::MAX"
            )),
        })?;
        let payload = encode_metadata_command_checkpoint_candidates_request(
            &StorageRpcMetadataCommandCheckpointCandidatesRequest {
                node_id: self.node_id,
                cluster_epoch,
                pg_id,
                max_applied_log_index,
                limit,
            },
        );
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandCheckpointCandidates,
            payload,
        )?;
        decode_metadata_command_checkpoint_candidates_response(&response)
            .map(|response| response.checkpoints)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command checkpoint candidates response",
                    error.to_string(),
                )
            })
    }

    fn compact_metadata_command_log(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandLogCompactionStatus, StoreError> {
        if cluster_epoch > self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        let payload =
            encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
                node_id: self.node_id,
                cluster_epoch,
                pg_id,
            });
        let response =
            self.rpc_request(StorageRpcMessageKind::MetadataCommandLogCompact, payload)?;
        decode_metadata_command_log_compact_response(&response)
            .map(|response| response.status)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command log compact response",
                    error.to_string(),
                )
            })
    }

    fn metadata_command_acceptance(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        self.metadata_command_acceptance_request(
            StorageRpcMessageKind::MetadataCommandAcceptance,
            pg_id,
            command,
        )
    }

    fn metadata_command_abandon_acceptance(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        self.metadata_command_acceptance_request(
            StorageRpcMessageKind::MetadataCommandAbandonAcceptance,
            pg_id,
            command,
        )
    }

    fn applied_metadata_command_log_entry_hashes(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<Option<(u64, u64)>, StoreError> {
        let payload = self.encode_metadata_command_request(pg_id, command)?;
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandAppliedLogHashes,
            payload,
        )?;
        let response =
            decode_metadata_command_applied_hashes_response(&response).map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command applied hashes response",
                    error.to_string(),
                )
            })?;
        match response.outcome {
            StorageRpcMetadataCommandAppliedHashesOutcome::Hashes(hashes) => Ok(hashes),
            StorageRpcMetadataCommandAppliedHashesOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => Err(metadata_command_log_conflict_error(
                self.cluster_epoch,
                pg_id,
                "decode metadata command applied hashes response",
                |operation, message| self.rpc_payload_error(operation, message),
                MetadataCommandLogConflictRpcFields {
                    node_id,
                    pg_id: conflict_pg_id,
                    cluster_epoch,
                    log_index,
                },
            )),
        }
    }

    fn retained_metadata_command_log_hashes(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        first_log_index: MetadataCommandLogIndex,
        last_log_index: MetadataCommandLogIndex,
    ) -> Result<Vec<MetadataCommandLogHashRangeEntry>, StoreError> {
        if cluster_epoch > self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        let request = StorageRpcMetadataCommandLogHashRangeRequest {
            node_id: self.node_id,
            cluster_epoch,
            pg_id,
            first_log_index,
            last_log_index,
        };
        let payload = encode_metadata_command_log_hash_range_request(&request);
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandRetainedLogHashes,
            payload,
        )?;
        decode_metadata_command_log_hash_range_response(&response)
            .map(|response| response.entries)
            .map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command retained log hashes response",
                    error.to_string(),
                )
            })
    }

    fn retained_metadata_command_log_entries(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        first_log_index: MetadataCommandLogIndex,
        last_log_index: MetadataCommandLogIndex,
    ) -> Result<Vec<MetadataCommandLogRangeEntry>, StoreError> {
        if cluster_epoch > self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        let request = StorageRpcMetadataCommandLogHashRangeRequest {
            node_id: self.node_id,
            cluster_epoch,
            pg_id,
            first_log_index,
            last_log_index,
        };
        let payload = encode_metadata_command_log_hash_range_request(&request);
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandRetainedLogEntries,
            payload,
        )?;
        decode_metadata_command_log_entry_range_response(
            &response,
            &MetadataCommandDecodeAuthority::new(),
        )
        .map(|response| response.entries)
        .map_err(|error| {
            self.rpc_payload_error(
                "decode metadata command retained log entries response",
                error.to_string(),
            )
        })
    }

    fn has_matching_applied_metadata_command_log_entry(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        expected_previous_log_hash: u64,
    ) -> Result<bool, StoreError> {
        let request = StorageRpcMetadataCommandMatchingAppliedRequest {
            node_id: self.node_id,
            cluster_epoch: self.cluster_epoch,
            pg_id,
            command: command.clone(),
            expected_previous_log_hash,
        };
        let payload =
            encode_metadata_command_matching_applied_request(&request).map_err(|error| {
                self.rpc_payload_error(
                    "encode metadata command matching applied request",
                    error.to_string(),
                )
            })?;
        let response = self.rpc_request(
            StorageRpcMessageKind::MetadataCommandMatchingAppliedLog,
            payload,
        )?;
        let response =
            decode_metadata_command_bool_outcome_response(&response).map_err(|error| {
                self.rpc_payload_error(
                    "decode metadata command matching applied response",
                    error.to_string(),
                )
            })?;
        match response.outcome {
            StorageRpcMetadataCommandBoolOutcome::Value(value) => Ok(value),
            StorageRpcMetadataCommandBoolOutcome::LogConflict {
                node_id,
                pg_id: conflict_pg_id,
                cluster_epoch,
                log_index,
            } => Err(metadata_command_log_conflict_error(
                self.cluster_epoch,
                pg_id,
                "decode metadata command matching applied response",
                |operation, message| self.rpc_payload_error(operation, message),
                MetadataCommandLogConflictRpcFields {
                    node_id,
                    pg_id: conflict_pg_id,
                    cluster_epoch,
                    log_index,
                },
            )),
        }
    }

    fn apply_metadata_command_and_record(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError> {
        self.metadata_command_apply_and_record_with_kind(
            pg_id,
            command,
            StorageRpcMessageKind::MetadataCommandApplyAndRecord,
            "decode metadata command apply and record response",
        )
    }

    fn apply_metadata_command_and_record_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<MetadataCommandReplicaState, MetadataCommandApplyError> {
        let payload = self
            .encode_metadata_command_request(pg_id, command)
            .map_err(MetadataCommandApplyError::not_sent)?;
        self.metadata_command_apply_and_record_with_payload_until(
            pg_id,
            command,
            StorageRpcMessageKind::MetadataCommandApplyAndRecord,
            payload,
            "decode metadata command apply and record response",
            deadline,
        )
    }

    fn record_metadata_command_abandoned_on_replica(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let held_pg_id = self.metadata_command_pg_id();
        if pg_id != held_pg_id {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "record metadata command abandoned on bound replica",
            });
        }
        MetadataCommandRecoveryCriticalSection::record_metadata_command_abandoned(self, command)
    }
}
impl ShardReadHandleNodeClient for UnixStorageNodeClient {
    fn open_shard_read_handle_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        read_operation_id: &str,
        entries: Vec<(crate::cluster::ShardLocation, ShardKey)>,
    ) -> Result<Box<dyn ShardReadHandleRoute + '_>, StoreError> {
        if route_cluster_epoch != self.cluster_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id: entries
                    .first()
                    .map_or(0, |(location, _)| location.data_pg_id().get()),
                operation_epoch: route_cluster_epoch,
                current_epoch: self.cluster_epoch,
            });
        }
        if entries.is_empty() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "open shard read-handle route",
            });
        }
        for (location, key) in &entries {
            if location.node_id() != self.node_id
                || location.cluster_epoch() != route_cluster_epoch
                || location.shard_index() != key.shard_index()
            {
                return Err(StoreError::RouteCapabilitySubjectMismatch {
                    operation: "open shard read-handle route",
                });
            }
        }
        Ok(Box::new(UnixShardReadHandleRoute {
            client: self,
            read_operation_id: read_operation_id.to_string(),
            entries,
        }))
    }
}

impl ShardReadHandleRoute for UnixShardReadHandleRoute<'_> {
    fn acquire(self: Box<Self>) -> Result<Box<dyn ShardReadHandleLease>, StoreError> {
        let mut session = self.client.open_read_handle_session()?;
        session.acquire_read_handles(&self.read_operation_id, self.entries.clone())?;
        Ok(Box::new(UnixStorageNodeReadHandleLease {
            session,
            read_operation_id: self.read_operation_id,
            entries: self.entries,
            released: false,
        }))
    }
}

impl ObjectPayloadLeaseNodeClient for UnixStorageNodeClient {
    fn open_object_payload_lease_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<Box<dyn ObjectPayloadLeaseRoute + '_>, StoreError> {
        if route_cluster_epoch != self.cluster_epoch {
            return Err(StoreError::RouteAdmissionClusterMismatch {
                admitted_epoch: route_cluster_epoch,
                operation_epoch: self.cluster_epoch,
            });
        }
        Ok(Box::new(UnixObjectPayloadLeaseRoute {
            client: self,
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id,
        }))
    }
}

impl ObjectPayloadLeaseRoute for UnixObjectPayloadLeaseRoute<'_> {
    fn acquire_object_payload_lease(
        &self,
        kind: ObjectPayloadLeaseKind,
    ) -> Result<Option<Box<dyn ObjectPayloadLeaseNodeLease>>, StoreError> {
        let mut session = self.client.open_object_payload_lease_session(kind)?;
        let acquired = session.object_payload_lease_control(
            &self.bucket,
            &self.key,
            self.generation_id,
            StorageRpcObjectPayloadLeaseControlOperation::Acquire,
            None,
        )?;
        match acquired {
            0 => {
                session.mark_connection_reusable_if_server_allows();
                Ok(None)
            }
            1 => Ok(Some(Box::new(UnixObjectPayloadLease {
                session,
                bucket: self.bucket.clone(),
                key: self.key.clone(),
                generation_id: self.generation_id,
                released: false,
            }))),
            _ => Err(StoreError::Io {
                context: "validate storage-node object-payload lease acquire response",
                source: io::Error::new(io::ErrorKind::InvalidData, "invalid acquired value"),
            }),
        }
    }

    fn try_begin_object_payload_reclaim(
        &self,
        authority: &ObjectPayloadReclaimClaimProof,
    ) -> Result<bool, StoreError> {
        if authority.cluster_epoch != self.client.cluster_epoch {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "begin object payload reclaim",
            });
        }
        match self.client.object_payload_lease_control_request(
            &self.bucket,
            &self.key,
            self.generation_id,
            StorageRpcObjectPayloadLeaseControlOperation::ReclaimBegin,
            Some(authority),
        )? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(StoreError::Io {
                context: "validate storage-node object-payload reclaim begin response",
                source: io::Error::new(io::ErrorKind::InvalidData, "invalid acquired value"),
            }),
        }
    }

    fn object_payload_lease_count(&self) -> Result<usize, StoreError> {
        let count = self.client.object_payload_lease_control_request(
            &self.bucket,
            &self.key,
            self.generation_id,
            StorageRpcObjectPayloadLeaseControlOperation::Count,
            None,
        )?;
        usize::try_from(count).map_err(|_| StoreError::Io {
            context: "validate storage-node object-payload lease count response",
            source: io::Error::new(io::ErrorKind::InvalidData, "lease count exceeds usize"),
        })
    }
}

impl RetainedObjectPayloadReclaimNodeClient for UnixStorageNodeClient {
    fn open_retained_object_payload_reclaim_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        authority: &ObjectPayloadReclaimClaimProof,
    ) -> Result<Box<dyn RetainedObjectPayloadReclaimRoute + '_>, StoreError> {
        if route_cluster_epoch != self.cluster_epoch {
            return Err(StoreError::RouteAdmissionClusterMismatch {
                admitted_epoch: route_cluster_epoch,
                operation_epoch: self.cluster_epoch,
            });
        }
        if authority.cluster_epoch != route_cluster_epoch {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "open retained object payload reclaim route",
            });
        }
        Ok(Box::new(UnixRetainedObjectPayloadReclaimRoute {
            client: self,
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id,
            authority: authority.clone(),
        }))
    }
}

impl RetainedObjectPayloadReclaimRoute for UnixRetainedObjectPayloadReclaimRoute<'_> {
    fn finish_object_payload_reclaim(&self, keep_fence: bool) -> Result<(), StoreError> {
        let operation = if keep_fence {
            StorageRpcObjectPayloadLeaseControlOperation::ReclaimFinishKeepFence
        } else {
            StorageRpcObjectPayloadLeaseControlOperation::ReclaimFinish
        };
        let mut last_error = None;
        for _ in 0..2 {
            let result = self
                .client
                .object_payload_lease_control_request(
                    &self.bucket,
                    &self.key,
                    self.generation_id,
                    operation,
                    Some(&self.authority),
                )
                .map(|_| ());
            match result {
                Ok(()) => return Ok(()),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.expect("object-payload reclaim finish attempted at least once"))
    }

    fn clear_object_payload_reclaim_fence(&self) -> Result<(), StoreError> {
        let mut last_error = None;
        for _ in 0..2 {
            let result = self
                .client
                .object_payload_lease_control_request(
                    &self.bucket,
                    &self.key,
                    self.generation_id,
                    StorageRpcObjectPayloadLeaseControlOperation::ReclaimFenceClear,
                    Some(&self.authority),
                )
                .map(|_| ());
            match result {
                Ok(()) => return Ok(()),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.expect("object-payload reclaim fence clear attempted at least once"))
    }
}

impl ObjectPayloadLeaseNodeLease for UnixObjectPayloadLease {
    fn node_id(&self) -> NodeId {
        self.session.node_id
    }

    fn release(&mut self) -> Result<usize, StoreError> {
        if self.released {
            return Ok(0);
        }
        let remaining = self.session.object_payload_lease_control(
            &self.bucket,
            &self.key,
            self.generation_id,
            StorageRpcObjectPayloadLeaseControlOperation::Release,
            None,
        )?;
        self.released = true;
        self.session.mark_connection_reusable_if_server_allows();
        usize::try_from(remaining).map_err(|_| StoreError::Io {
            context: "validate storage-node object-payload lease release response",
            source: io::Error::new(io::ErrorKind::InvalidData, "lease count exceeds usize"),
        })
    }
}

impl Drop for UnixObjectPayloadLease {
    fn drop(&mut self) {
        let _ = self.release();
    }
}

impl ShardReadHandleLease for UnixStorageNodeReadHandleLease {
    fn read_placed_shard_range_into(
        &mut self,
        location: crate::cluster::ShardLocation,
        key: &ShardKey,
        expected_ack: WriteAck,
        offset: u64,
        dst: &mut [u8],
    ) -> Result<(), StoreError> {
        if self.released {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "read placed shard through released read-handle lease",
            });
        }
        if !self
            .entries
            .iter()
            .any(|entry| entry.0 == location && entry.1 == *key)
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "read placed shard through read-handle lease",
            });
        }
        self.session
            .read_placed_shard_range_into(location, key, expected_ack, offset, dst)
    }

    fn release(&mut self) -> Result<(), StoreError> {
        if self.released {
            return Ok(());
        }
        self.session
            .release_read_handles(self.read_operation_id.as_str())?;
        self.released = true;
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_rpc::{
        StorageRpcMetadataCommandLogEntryRangeResponse,
        StorageRpcMetadataCommandLogHashRangeResponse,
    };
    use crate::storage_rpc_transport::accepted_unix_stream;
    use std::os::unix::net::UnixStream;

    fn test_rpc_admission_permit() -> UnixStorageNodeRpcAdmissionPermit {
        Arc::new(UnixStorageNodeRpcAdmission::new(1))
            .try_acquire_for_test()
            .unwrap()
    }

    #[test]
    fn staging_complete_primary_borrow_holds_transport_mutex_and_rejects_epoch_or_closed_session() {
        // Private mechanical fixture; the separate installed Unix/TLS fanout
        // tests establish actual authenticated lock acquisition.
        let (stream, _peer) = UnixStream::pair().unwrap();
        let epoch = ClusterEpoch::new(7).unwrap();
        let pg_id = PgId::new(3);
        let deadline = Instant::now() + Duration::from_secs(5);
        let client = UnixStorageNodeClient::new(NodeId::new(4), epoch, "/unused");
        let session = UnixStorageNodeMetadataCommandSession {
            origin: Arc::clone(&client.instance),
            identity: Arc::new(()),
            node_id: NodeId::new(4),
            cluster_epoch: epoch,
            pg_id,
            rpc_auth: None,
            _rpc_permit: test_rpc_admission_permit(),
            inner: Mutex::new(UnixStorageNodeMetadataCommandSessionInner {
                stream: accepted_unix_stream(stream, deadline).unwrap(),
                next_request_id: 1,
                io_timeout: Duration::from_secs(5),
                released: false,
            }),
        };
        let held = session
            .borrow_complete_primary(&client, pg_id, epoch, deadline)
            .unwrap();
        assert!(matches!(
            session.inner.try_lock(),
            Err(std::sync::TryLockError::WouldBlock)
        ));
        drop(held);
        assert!(session.inner.try_lock().is_ok());
        assert!(session
            .borrow_complete_primary(&client, pg_id, ClusterEpoch::new(8).unwrap(), deadline)
            .is_err());
        assert!(session
            .borrow_complete_primary(&client, pg_id, epoch, Instant::now())
            .is_err());
        session.close_metadata_command_pg_lock_on_drop();
        assert!(session
            .borrow_complete_primary(&client, pg_id, epoch, deadline)
            .is_err());
    }

    #[test]
    fn peering_session_reads_retained_entries_from_an_older_log_epoch() {
        let (client_stream, mut server_stream) = UnixStream::pair().unwrap();
        let pg_id = PgId::new(3);
        let log_epoch = ClusterEpoch::new(4).unwrap();
        let route_epoch = ClusterEpoch::new(7).unwrap();
        let server = std::thread::spawn(move || {
            for kind in [
                StorageRpcMessageKind::MetadataCommandRetainedLogHashes,
                StorageRpcMessageKind::MetadataCommandRetainedLogEntries,
            ] {
                let request =
                    crate::storage_rpc::read_storage_rpc_frame_from(&mut server_stream).unwrap();
                assert_eq!(request.kind, kind);
                let range = crate::storage_rpc::decode_metadata_command_log_hash_range_request(
                    &request.payload,
                )
                .unwrap();
                assert_eq!(range.cluster_epoch, log_epoch);
                assert_eq!(range.pg_id, pg_id);
                let payload = match kind {
                    StorageRpcMessageKind::MetadataCommandRetainedLogHashes => {
                        crate::storage_rpc::encode_metadata_command_log_hash_range_response(
                            &StorageRpcMetadataCommandLogHashRangeResponse { entries: vec![] },
                        )
                        .unwrap()
                    }
                    StorageRpcMessageKind::MetadataCommandRetainedLogEntries => {
                        crate::storage_rpc::encode_metadata_command_log_entry_range_response(
                            &StorageRpcMetadataCommandLogEntryRangeResponse { entries: vec![] },
                        )
                        .unwrap()
                    }
                    _ => unreachable!(),
                };
                let mut response_payload =
                    crate::storage_rpc::encode_storage_rpc_success_response(&payload);
                crate::storage_rpc::set_storage_rpc_response_connection_reusable(
                    &mut response_payload,
                    true,
                )
                .unwrap();
                crate::storage_rpc::write_storage_rpc_frame_to(
                    &mut server_stream,
                    &StorageRpcFrame {
                        request_id: request.request_id,
                        kind,
                        payload: response_payload,
                    },
                )
                .unwrap();
            }
        });
        let io_timeout = Duration::from_secs(1);
        let session = UnixStorageNodeMetadataCommandSession {
            origin: Arc::new(UnixStorageNodeClientInstance),
            identity: Arc::new(()),
            node_id: NodeId::new(7),
            cluster_epoch: route_epoch,
            pg_id,
            rpc_auth: None,
            _rpc_permit: test_rpc_admission_permit(),
            inner: Mutex::new(UnixStorageNodeMetadataCommandSessionInner {
                stream: accepted_unix_stream(client_stream, Instant::now() + io_timeout).unwrap(),
                next_request_id: 1,
                io_timeout,
                released: false,
            }),
        };
        let first = MetadataCommandLogIndex::new(1).unwrap();
        assert!(session
            .retained_metadata_command_log_hashes(pg_id, log_epoch, first, first)
            .unwrap()
            .is_empty());
        assert!(session
            .retained_metadata_command_log_entries(pg_id, log_epoch, first, first)
            .unwrap()
            .is_empty());
        assert!(matches!(
            session.retained_metadata_command_log_entries(
                pg_id,
                ClusterEpoch::new(8).unwrap(),
                first,
                first,
            ),
            Err(StoreError::StalePayloadOperation { .. })
        ));
        server.join().unwrap();
    }

    #[test]
    fn storage_rpc_resource_exhaustion_decodes_to_typed_store_error() {
        let client = UnixStorageNodeClient::new(
            NodeId::new(7),
            ClusterEpoch::new(1).unwrap(),
            "/tmp/unopened-storage-node.sock",
        );
        let err = StorageRpcErrorResponse {
            code: StorageRpcErrorCode::ResourceExhausted,
            message: "active read handles exceed limit".to_string(),
        };
        assert!(matches!(
            client.rpc_response_error(StorageRpcMessageKind::ShardDelete, err),
            StoreError::StorageRpcResourceExhausted {
                node_id: 7,
                operation: "shard delete",
                ref detail,
            } if detail.as_str().contains("active read handles")
        ));

        let (read_stream, _read_peer) = UnixStream::pair().unwrap();
        let io_timeout = Duration::from_secs(1);
        let read_session = UnixStorageNodeReadHandleSession {
            node_id: NodeId::new(8),
            route_cluster_epoch: ClusterEpoch::new(1).unwrap(),
            connection: StorageRpcRequestConnection::unpooled_for_test(
                accepted_unix_stream(read_stream, Instant::now() + io_timeout).unwrap(),
            ),
            next_request_id: 1,
            io_timeout,
            last_connection_reusable: false,
            active_read_operation_ids: BTreeSet::new(),
            rpc_auth: None,
            _rpc_permit: Some(test_rpc_admission_permit()),
            _object_payload_lease_permit: None,
        };
        let err = StorageRpcErrorResponse {
            code: StorageRpcErrorCode::ResourceExhausted,
            message: "read handle session limit reached".to_string(),
        };
        assert!(matches!(
            read_session.rpc_response_error(StorageRpcMessageKind::ReadHandlesAcquire, err),
            StoreError::StorageRpcResourceExhausted {
                node_id: 8,
                operation: "read handles acquire",
                ref detail,
            } if detail.as_str().contains("session limit")
        ));

        let (metadata_stream, _metadata_peer) = UnixStream::pair().unwrap();
        let io_timeout = Duration::from_secs(1);
        let metadata_session = UnixStorageNodeMetadataCommandSession {
            origin: Arc::new(UnixStorageNodeClientInstance),
            identity: Arc::new(()),
            node_id: NodeId::new(9),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(3),
            rpc_auth: None,
            _rpc_permit: test_rpc_admission_permit(),
            inner: Mutex::new(UnixStorageNodeMetadataCommandSessionInner {
                stream: accepted_unix_stream(metadata_stream, Instant::now() + io_timeout).unwrap(),
                next_request_id: 1,
                io_timeout,
                released: true,
            }),
        };
        let err = StorageRpcErrorResponse {
            code: StorageRpcErrorCode::ResourceExhausted,
            message: "metadata command session limit reached".to_string(),
        };
        assert!(matches!(
            metadata_session
                .rpc_response_error(StorageRpcMessageKind::MetadataCommandPgLockAcquire, err),
            StoreError::StorageRpcResourceExhausted {
                node_id: 9,
                operation: "metadata command PG lock acquire",
                ref detail,
            } if detail.as_str().contains("session limit")
        ));

        let err = StorageRpcErrorResponse {
            code: StorageRpcErrorCode::ShardDeleteInProgress,
            message: "shard is being deleted".to_string(),
        };
        assert!(matches!(
            metadata_session
                .rpc_response_error(StorageRpcMessageKind::MetadataCommandPgLockAcquire, err),
            StoreError::StorageRpcShardDeleteInProgress {
                node_id: 9,
                operation: "metadata command PG lock acquire",
                ref detail,
            } if detail.as_str().contains("being deleted")
        ));
    }
}
