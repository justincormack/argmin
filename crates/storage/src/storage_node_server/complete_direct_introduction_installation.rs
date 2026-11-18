// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Receiver-local owning installation, also used by the candidate socket continuation.
//! Claiming a preparation is one-way for this held session, including on error.
use super::*;
use crate::metadata_command::MetadataCommandId;
use crate::node_runtime::pg_store::{
    RemoteIntroductionInstallation, RemoteIntroductionInstallationState,
};

#[path = "complete_direct_introduction_parked.rs"]
mod parked;
pub(in crate::node_runtime::server) use parked::ParkedIntroductionInstallation;

pub(in crate::node_runtime) struct RemoteIntroductionInstallationInput<'a> {
    handler: &'a StorageNodeConnectionHandler,
    session: &'a StorageNodeSession,
    admission: &'a StorageNodeRouteAdmissionPermit,
    query: &'a IntroductionQuery,
    response: &'a [u8],
    identity: IntroductionPreparationIdentity,
    fence: AdmittedRouteEffectFence,
}

pub(in crate::node_runtime) struct RemoteIntroductionInstallationAuthority<'a, 'owner> {
    input: &'a RemoteIntroductionInstallationInput<'owner>,
    store: &'a PgStore,
}

impl RemoteIntroductionInstallationInput<'_> {
    fn require_held_claim(&self) -> Result<&StorageNodeSessionMetadataCommandGuard, StoreError> {
        let invalid = || StoreError::RouteCapabilitySubjectMismatch {
            operation: "direct introduction claimed preparation/section",
        };
        let route = self.query.route;
        let binding = StorageNodeMetadataCommandLockBinding {
            pg_id: route.pg,
            cluster_epoch: route.epoch,
            authority: StorageNodeMetadataCommandLockAuthority::CurrentPrimary,
        };
        if !holds_original_primary(self.handler, self.session, binding) {
            return Err(invalid());
        }
        let held = self
            .session
            .metadata_command_guards
            .get(&route.pg)
            .ok_or_else(invalid)?;
        if held.introduction_preparation.get()
            != IntroductionPreparationState::Claimed(self.identity)
        {
            return Err(invalid());
        }
        Ok(held)
    }

    pub(in crate::node_runtime) fn require_state(
        &self,
        id: MetadataCommandId,
        request: [u8; 32],
        response: [u8; 32],
        command: [u8; 32],
    ) -> Result<(), StoreError> {
        self.require_held_claim()?;
        let route = self.query.route;
        if id.pg_id() != route.pg
            || id.cluster_epoch() != route.epoch
            || request != self.identity.request
            || response != self.identity.response
            || command != self.identity.command
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "direct introduction original installation input",
            });
        }
        Ok(())
    }
    #[cfg(test)]
    pub(in crate::node_runtime) fn test_bounds(&self) -> (Instant, u64) {
        (
            self.identity.local_deadline,
            self.identity.cutoff.portable_wall_valid_until_ms,
        )
    }
    fn require_valid(&self) -> Result<(), StoreError> {
        let invalid = || StoreError::RouteCapabilitySubjectMismatch {
            operation: "direct introduction claimed preparation/section",
        };
        let route = self.query.route;
        self.handler
            .validate_active_admission(self.admission, "direct introduction installation")
            .map_err(|_| invalid())?;
        self.handler
            .validate_pg_route(route.node, route.epoch, route.pg)
            .map_err(|_| invalid())?;
        self.handler
            .validate_primary_pg(route.pg, "direct introduction installation")
            .map_err(|_| invalid())?;
        self.require_held_claim()?;
        self.query.require_valid()?;
        self.fence.require_valid_for(route.epoch)?;
        if Instant::now() >= self.identity.local_deadline {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "direct introduction original installation deadline",
            });
        }
        Ok(())
    }

    pub(in crate::node_runtime) fn response(&self) -> &[u8] {
        self.response
    }
    pub(in crate::node_runtime) fn request(
        &self,
    ) -> Result<wire::EncodedRequest, StorageRpcPayloadError> {
        let q = self.query;
        wire::encode_request(&wire::Request {
            route: q.route,
            bucket: q.bucket.clone(),
            key: q.key.clone(),
            attempt: q.attempt.clone(),
            proof: q.proof.clone(),
        })
    }
    pub(in crate::node_runtime) fn topology(&self) -> &crate::PgTopology {
        self.handler.node.pg_topology()
    }
    pub(in crate::node_runtime) fn install(
        &self,
        state: &mut RemoteIntroductionInstallationState,
    ) -> Result<(), StagingWindowConfirmationError> {
        self.require_valid()?;
        let pg = self
            .handler
            .node
            .get_pg_until(self.query.route.pg.get(), self.identity.local_deadline)?;
        let authority = RemoteIntroductionInstallationAuthority {
            input: self,
            store: &pg,
        };
        state.install_in_store(&authority, &pg)
    }
}

impl RemoteIntroductionInstallationAuthority<'_, '_> {
    pub(in crate::node_runtime) fn require_command(
        &self,
        pg: &PgStore,
        id: MetadataCommandId,
        request: [u8; 32],
        response: [u8; 32],
        command: [u8; 32],
    ) -> Result<(), StoreError> {
        self.input.require_valid()?;
        self.input.require_state(id, request, response, command)?;
        let route = self.input.query.route;
        if !std::ptr::eq(pg, self.store) || pg.pg_id() != route.pg.get() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "direct introduction original installation input",
            });
        }
        Ok(())
    }
    pub(in crate::node_runtime) fn node(&self) -> NodeId {
        self.input.query.route.node
    }
    pub(in crate::node_runtime) fn topology(&self) -> &crate::PgTopology {
        self.input.topology()
    }
    pub(in crate::node_runtime) fn fence(&self) -> AdmittedRouteEffectFence {
        self.input.fence
    }
    pub(in crate::node_runtime) fn deadline(&self) -> Instant {
        self.input.identity.local_deadline
    }
}

impl StorageNodeConnectionHandler {
    pub(super) fn claim_direct_introduction<'a>(
        &'a self,
        session: &'a StorageNodeSession,
        admission: &'a StorageNodeRouteAdmissionPermit,
        query: &'a IntroductionQuery,
        response: &'a [u8],
    ) -> Result<RemoteIntroductionInstallation<'a>, StorageRpcErrorResponse> {
        self.validate_active_admission(admission, "direct introduction installation")?;
        let route = query.route;
        self.validate_pg_route(route.node, route.epoch, route.pg)?;
        self.validate_primary_pg(route.pg, "direct introduction installation")?;
        if route.topology != topology_digest(self.node.pg_topology()) {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::PayloadDecode,
                message: "direct introduction installation topology".into(),
            });
        }
        self.require_prepared_introduction_response(session, query, response)
            .map_err(store_error_response)?;
        let held = session
            .metadata_command_guards
            .get(&route.pg)
            .expect("validated section");
        let identity = held
            .introduction_preparation
            .get()
            .prepared()
            .expect("validated preparation");
        // Capture before any store wait. A renewed current lease cannot extend
        // the preparation's retained local deadline or absolute response cutoff.
        let fence = StorageNodeRouteFence::current(&self.config, self.current_route_map_lease())
            .intersect_effect_fence(query.fence)
            .map_err(store_error_response)?
            .intersect(AdmittedRouteEffectFence::bind_portable(
                route.epoch,
                identity.cutoff.portable_wall_valid_until_ms,
                identity.cutoff.portable_wall_valid_until_ms,
            ))
            .map_err(store_error_response)?;
        fence
            .require_valid_for(route.epoch)
            .map_err(store_error_response)?;
        held.introduction_preparation
            .set(IntroductionPreparationState::Claimed(identity));
        let input = RemoteIntroductionInstallationInput {
            handler: self,
            session,
            admission,
            query,
            response,
            identity,
            fence,
        };
        // No restoration on failure or Drop: this section cannot mint another
        // independent holder or recapture fresh history for the same preparation.
        RemoteIntroductionInstallation::from_input(input).map_err(capture_error)
    }
}
