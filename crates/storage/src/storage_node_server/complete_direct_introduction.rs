// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Receiver-owned fresh introduction capture and same-section response identity.
//! Preparation RPC dispatch is test-selector gated; installation is still
//! receiver-local candidate work, with no frontend custody transfer.
use super::*;
use crate::node_runtime::payload_placement::PayloadPlacementInput;
use crate::node_runtime::pg_store::{
    CapturedDirectIntroductionCore, PgStore, StagingWindowConfirmationError,
};
use crate::storage_rpc::complete_direct_introduction as wire;
use crate::storage_rpc::complete_settled_prefix::{topology_digest, Request as Route};
use crate::storage_rpc::StorageRpcPayloadError;
use crate::types::AdmittedRouteEffectFence;

#[cfg(test)]
#[path = "complete_direct_introduction_tests.rs"]
mod tests;
#[cfg(test)]
pub(in crate::node_runtime) use tests::parked::ParkedIntroductionTest;

#[path = "complete_direct_introduction_dispatch.rs"]
mod dispatch;
#[cfg(test)]
pub(super) use dispatch::test_lose_reply;
#[path = "complete_direct_introduction_installation.rs"]
mod installation;
pub(super) use installation::ParkedIntroductionInstallation;
pub(in crate::node_runtime) use installation::{
    RemoteIntroductionInstallationAuthority, RemoteIntroductionInstallationInput,
};

// Logical request only: generations, command indices, timestamps, placement and
// descriptor cutoffs are selected by the admitted owner, never the caller.
struct IntroductionQuery {
    route: Route,
    bucket: BucketName,
    key: ObjectKey,
    attempt: crate::SessionId,
    proof: BucketWriteReservationProof,
    deadline: Instant,
    fence: AdmittedRouteEffectFence,
}

impl IntroductionQuery {
    fn decode(bytes: &[u8]) -> Result<Self, StorageRpcPayloadError> {
        let request = wire::decode_request(bytes)?;
        Ok(Self::new(
            request.route,
            request.bucket,
            request.key,
            request.attempt,
            request.proof,
        ))
    }
    fn new(
        route: Route,
        bucket: BucketName,
        key: ObjectKey,
        attempt: crate::SessionId,
        proof: BucketWriteReservationProof,
    ) -> Self {
        let cutoff = route.deadline.portable_wall_valid_until_ms;
        Self {
            route,
            bucket,
            key,
            attempt,
            proof,
            // One absolute, already-skew-shortened sender cutoff. No remaining
            // duration is restarted on receipt, store waits or repeated capture.
            deadline: route.deadline.local_deadline(),
            fence: AdmittedRouteEffectFence::bind_portable(route.epoch, cutoff, cutoff),
        }
    }
    fn require_valid(&self) -> Result<(), StoreError> {
        self.fence.require_valid_for(self.route.epoch)?;
        if Instant::now() >= self.deadline {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "remote direct introduction request cutoff",
            });
        }
        Ok(())
    }
}

/// Minted only after active-domain, current-primary and held-session admission.
/// The shared storage builder cannot substitute a store, query or raw callback.
pub(in crate::node_runtime) struct RemoteDirectIntroductionAuthority<'a> {
    handler: &'a StorageNodeConnectionHandler,
    session: &'a StorageNodeSession,
    _admission: &'a StorageNodeRouteAdmissionPermit,
    store: &'a PgStore,
    query: &'a IntroductionQuery,
    binding: StorageNodeMetadataCommandLockBinding,
    fence: AdmittedRouteEffectFence,
    deadline: Instant,
}

impl RemoteDirectIntroductionAuthority<'_> {
    pub(in crate::node_runtime) fn require_capture(&self, pg: &PgStore) -> Result<(), StoreError> {
        if !std::ptr::eq(pg, self.store)
            || pg.pg_id() != self.query.route.pg.get()
            || !holds_original_primary(self.handler, self.session, self.binding)
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "remote direct introduction exact store/section",
            });
        }
        self.query.require_valid()?;
        self.fence.require_valid_for(self.query.route.epoch)?;
        if Instant::now() >= self.deadline {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "remote direct introduction admitted cutoff",
            });
        }
        Ok(())
    }
    pub(in crate::node_runtime) fn route(&self) -> Route {
        self.query.route
    }
    pub(in crate::node_runtime) fn topology(&self) -> &crate::PgTopology {
        self.handler.node.pg_topology()
    }
    pub(in crate::node_runtime) fn placement(&self) -> PayloadPlacementInput<'_> {
        PayloadPlacementInput::from_server_config(&self.handler.config)
    }
    pub(in crate::node_runtime) fn effect_fence(&self) -> AdmittedRouteEffectFence {
        self.fence
    }
    pub(in crate::node_runtime) fn deadline(&self) -> Instant {
        self.deadline
    }
    pub(in crate::node_runtime) fn subject(
        &self,
    ) -> (
        &BucketName,
        &ObjectKey,
        &crate::SessionId,
        &BucketWriteReservationProof,
    ) {
        (
            &self.query.bucket,
            &self.query.key,
            &self.query.attempt,
            &self.query.proof,
        )
    }
}

fn holds_original_primary(
    handler: &StorageNodeConnectionHandler,
    session: &StorageNodeSession,
    binding: StorageNodeMetadataCommandLockBinding,
) -> bool {
    Arc::ptr_eq(&handler.node, &session.node)
        && session
            .metadata_command_guards
            .get(&binding.pg_id)
            .is_some_and(|held| {
                held.binding == binding
                    && !held._guard.released
                    && Arc::ptr_eq(
                        &held._guard.locks.state,
                        &handler.metadata_command_locks.state,
                    )
            })
}

#[must_use = "fresh capture is not installed authority or a transferable physical grant"]
struct PreparedIntroduction<'a> {
    captured: CapturedDirectIntroductionCore,
    handler: &'a StorageNodeConnectionHandler,
    query: &'a IntroductionQuery,
    session: &'a StorageNodeSession,
    _admission: &'a StorageNodeRouteAdmissionPermit,
    binding: StorageNodeMetadataCommandLockBinding,
}

/// Private constructor: encoding can only borrow an admitted, still-held read.
pub(in crate::node_runtime) struct DirectIntroductionResponseScope<'a, 'owner>(
    &'a PreparedIntroduction<'owner>,
);

impl DirectIntroductionResponseScope<'_, '_> {
    pub(in crate::node_runtime) fn require_capture(
        &self,
        core: &CapturedDirectIntroductionCore,
    ) -> Result<(), StoreError> {
        if !std::ptr::eq(core, &self.0.captured) {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "direct introduction response capture",
            });
        }
        self.0.require_valid()
    }

    pub(in crate::node_runtime) fn request(
        &self,
    ) -> Result<wire::EncodedRequest, StorageRpcPayloadError> {
        let q = self.0.query;
        wire::encode_request(&wire::Request {
            route: q.route,
            bucket: q.bucket.clone(),
            key: q.key.clone(),
            attempt: q.attempt.clone(),
            proof: q.proof.clone(),
        })
    }
}

/// One preparation in one held section. This does not establish installation or
/// physical authority. Releasing/reopening the section destroys the identity.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct IntroductionPreparationIdentity {
    request: [u8; 32],
    command: [u8; 32],
    response: [u8; 32],
    cutoff: crate::storage_rpc::StorageRpcOperationDeadline,
    local_deadline: Instant,
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(super) enum IntroductionPreparationState {
    #[default]
    Vacant,
    Prepared(IntroductionPreparationIdentity),
    Claimed(IntroductionPreparationIdentity),
}

impl IntroductionPreparationState {
    fn prepared(self) -> Option<IntroductionPreparationIdentity> {
        match self {
            Self::Prepared(identity) => Some(identity),
            Self::Vacant | Self::Claimed(_) => None,
        }
    }
}

impl PreparedIntroduction<'_> {
    fn encode_response(self) -> Result<Vec<u8>, StorageRpcErrorResponse> {
        let scope = DirectIntroductionResponseScope(&self);
        let (bytes, local_deadline) = self
            .captured
            .encode_response(&scope)
            .map_err(capture_error)?;
        let invalid = |e: StorageRpcPayloadError| StorageRpcErrorResponse {
            code: StorageRpcErrorCode::PayloadDecode,
            message: e.to_string(),
        };
        let request = scope.request().map_err(invalid)?;
        let response = request.decode_response(&bytes).map_err(invalid)?;
        self.require_valid().map_err(store_error_response)?;
        if response.cutoff.portable_wall_valid_until_ms <= crate::clock::current_time_millis() {
            return Err(store_error_response(
                StoreError::OperationDeadlineExceeded {
                    context: "direct introduction response cutoff",
                },
            ));
        }
        self.session
            .metadata_command_guards
            .get(&self.binding.pg_id)
            .expect("validated held section")
            .introduction_preparation
            .set(IntroductionPreparationState::Prepared(
                IntroductionPreparationIdentity {
                    request: request.digest(),
                    command: response.command_sha256,
                    response: crate::storage_rpc::complete_stream_allocation::digest(&bytes),
                    cutoff: response.cutoff,
                    local_deadline,
                },
            ));
        Ok(bytes)
    }

    fn require_valid(&self) -> Result<(), StoreError> {
        if !holds_original_primary(self.handler, self.session, self.binding) {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "remote direct introduction retained section",
            });
        }
        require_unclaimed(self.session, self.binding.pg_id)?;
        self.query.require_valid()?;
        self.captured.require_valid()
    }
}

fn require_unclaimed(session: &StorageNodeSession, pg: PgId) -> Result<(), StoreError> {
    if session
        .metadata_command_guards
        .get(&pg)
        .is_some_and(|held| {
            matches!(
                held.introduction_preparation.get(),
                IntroductionPreparationState::Claimed(_)
            )
        })
    {
        return Err(StoreError::RouteCapabilitySubjectMismatch {
            operation: "direct introduction preparation already claimed",
        });
    }
    Ok(())
}

fn capture_error(error: StagingWindowConfirmationError) -> StorageRpcErrorResponse {
    match error {
        StagingWindowConfirmationError::Store(error) => store_error_response(error),
        error => StorageRpcErrorResponse {
            code: StorageRpcErrorCode::MetadataCommandContention,
            message: error.to_string(),
        },
    }
}

impl StorageNodeConnectionHandler {
    // Exact preparation evidence only. A future installer must ALSO perform
    // active admission and transactional context/index validation.
    fn require_prepared_introduction_response(
        &self,
        session: &StorageNodeSession,
        query: &IntroductionQuery,
        bytes: &[u8],
    ) -> Result<(), StoreError> {
        let invalid = || StoreError::RouteCapabilitySubjectMismatch {
            operation: "direct introduction prepared response identity",
        };
        let binding = StorageNodeMetadataCommandLockBinding {
            pg_id: query.route.pg,
            cluster_epoch: query.route.epoch,
            authority: StorageNodeMetadataCommandLockAuthority::CurrentPrimary,
        };
        if !holds_original_primary(self, session, binding) {
            return Err(invalid());
        }
        query.require_valid()?;
        let request = wire::encode_request(&wire::Request {
            route: query.route,
            bucket: query.bucket.clone(),
            key: query.key.clone(),
            attempt: query.attempt.clone(),
            proof: query.proof.clone(),
        })
        .map_err(|_| invalid())?;
        let response = request.decode_response(bytes).map_err(|_| invalid())?;
        let held = session
            .metadata_command_guards
            .get(&binding.pg_id)
            .ok_or_else(invalid)?;
        let identity = held
            .introduction_preparation
            .get()
            .prepared()
            .ok_or_else(invalid)?;
        if identity.request != request.digest()
            || identity.command != response.command_sha256
            || identity.cutoff != response.cutoff
            || identity.response != crate::storage_rpc::complete_stream_allocation::digest(bytes)
        {
            return Err(invalid());
        }
        if Instant::now() >= identity.local_deadline
            || crate::clock::current_time_millis() >= identity.cutoff.portable_wall_valid_until_ms
        {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "direct introduction prepared response cutoff",
            });
        }
        Ok(())
    }

    fn prepare_direct_introduction<'a>(
        &'a self,
        session: &'a StorageNodeSession,
        admission: &'a StorageNodeRouteAdmissionPermit,
        query: &'a IntroductionQuery,
        #[cfg(test)] mut hook: impl FnMut(&'static str),
    ) -> Result<PreparedIntroduction<'a>, StorageRpcErrorResponse> {
        let started = Instant::now();
        let route_fence =
            StorageNodeRouteFence::current(&self.config, self.current_route_map_lease());
        self.validate_active_admission(admission, "direct introduction capture")?;
        let route = query.route;
        self.validate_pg_route(route.node, route.epoch, route.pg)?;
        self.validate_primary_pg(route.pg, "direct introduction capture")?;
        if route.topology != topology_digest(self.node.pg_topology()) {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: "direct introduction topology".into(),
            });
        }
        let binding = StorageNodeMetadataCommandLockBinding {
            pg_id: route.pg,
            cluster_epoch: route.epoch,
            authority: StorageNodeMetadataCommandLockAuthority::CurrentPrimary,
        };
        if !holds_original_primary(self, session, binding) {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::MetadataCommandContention,
                message: "direct introduction requires its held current-primary section".into(),
            });
        }
        require_unclaimed(session, binding.pg_id).map_err(store_error_response)?;
        query.require_valid().map_err(store_error_response)?;
        let fence = route_fence
            .intersect_effect_fence(query.fence)
            .map_err(store_error_response)?;
        fence
            .require_valid_for(route.epoch)
            .map_err(store_error_response)?;
        let mut deadline = query.deadline.min(
            started
                .checked_add(storage_node_rpc_io_timeout(self.rpc_auth.as_deref()))
                .unwrap_or(started),
        );
        if let Some(cutoff) = route_fence.local_valid_until_monotonic_ms {
            deadline = deadline.min(
                Instant::now()
                    .checked_add(Duration::from_millis(
                        cutoff.saturating_sub(crate::clock::monotonic_time_millis()),
                    ))
                    .unwrap_or(started),
            );
        }
        #[cfg(test)]
        hook("before store");
        let pg = self
            .node
            .get_pg_until(route.pg.get(), deadline)
            .map_err(store_error_response)?;
        let authority = RemoteDirectIntroductionAuthority {
            handler: self,
            session,
            _admission: admission,
            store: &pg,
            query,
            binding,
            fence,
            deadline,
        };
        let captured = CapturedDirectIntroductionCore::remote(
            &authority,
            &pg,
            #[cfg(test)]
            &mut hook,
        )
        .map_err(capture_error)?;
        let prepared = PreparedIntroduction {
            captured,
            handler: self,
            query,
            session,
            _admission: admission,
            binding,
        };
        prepared.require_valid().map_err(store_error_response)?;
        Ok(prepared)
    }
}
