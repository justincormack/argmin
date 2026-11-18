// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Connection-owned continuation. No request-frame borrow or decoded-context
//! constructor is retained between calls; move the SAME submission/history.
use super::*;

pub(in crate::node_runtime::server) struct ParkedIntroductionInstallation {
    query: IntroductionQuery,
    identity: IntroductionPreparationIdentity,
    fence: AdmittedRouteEffectFence,
    section: Arc<()>,
    state: RemoteIntroductionInstallationState,
}

fn unavailable() -> StoreError {
    StoreError::RouteCapabilitySubjectMismatch {
        operation: "direct introduction retained connection custody",
    }
}

impl RemoteIntroductionInstallationInput<'_> {
    pub(in crate::node_runtime) fn park(
        &self,
        state: RemoteIntroductionInstallationState,
    ) -> Result<(), Box<(RemoteIntroductionInstallationState, StoreError)>> {
        // Retaining custody is permitted after expiry; it does not renew effect
        // authority. Errors return the entire state, never reset observations.
        if let Err(error) = state.require_parking_input(self) {
            return Err(Box::new((state, error)));
        }
        let held = self
            .session
            .metadata_command_guards
            .get(&self.query.route.pg)
            .expect("validated held claim");
        let Ok(mut slot) = held.introduction_installation.try_borrow_mut() else {
            return Err(Box::new((state, unavailable())));
        };
        if slot.is_some() {
            return Err(Box::new((state, unavailable())));
        }
        let q = self.query;
        *slot = Some(Box::new(ParkedIntroductionInstallation {
            query: IntroductionQuery {
                route: q.route,
                bucket: q.bucket.clone(),
                key: q.key.clone(),
                attempt: q.attempt.clone(),
                proof: q.proof.clone(),
                // Copy the captured clocks. IntroductionQuery::new/decode
                // would rebind a remaining duration and is forbidden here.
                deadline: q.deadline,
                fence: q.fence,
            },
            identity: self.identity,
            fence: self.fence,
            section: Arc::clone(&held.introduction_section_identity),
            state,
        }));
        Ok(())
    }
}

impl StorageNodeConnectionHandler {
    pub(in super::super) fn install_parked_direct_introduction(
        &self,
        session: &StorageNodeSession,
        admission: &StorageNodeRouteAdmissionPermit,
        request: &[u8],
        response: &[u8],
    ) -> Result<(), StorageRpcErrorResponse> {
        self.validate_active_admission(admission, "retained direct introduction installation")?;
        let invalid = |e: StorageRpcPayloadError| StorageRpcErrorResponse {
            code: StorageRpcErrorCode::PayloadDecode,
            message: e.to_string(),
        };
        let logical = wire::decode_request(request).map_err(invalid)?;
        let encoded = wire::encode_request(&logical).map_err(invalid)?;
        let decoded = encoded.decode_response(response).map_err(invalid)?;
        let held = session
            .metadata_command_guards
            .get(&logical.route.pg)
            .ok_or_else(|| store_error_response(unavailable()))?;
        let mut slot = held
            .introduction_installation
            .try_borrow_mut()
            .map_err(|_| store_error_response(unavailable()))?;
        let retained = slot
            .as_mut()
            .ok_or_else(|| store_error_response(unavailable()))?;
        if !Arc::ptr_eq(&retained.section, &held.introduction_section_identity)
            || retained.identity.request != encoded.digest()
            || retained.identity.response
                != crate::storage_rpc::complete_stream_allocation::digest(response)
            || retained.identity.command != decoded.command_sha256
            || retained.identity.cutoff != decoded.cutoff
        {
            return Err(store_error_response(unavailable()));
        }
        let input = RemoteIntroductionInstallationInput {
            handler: self,
            session,
            admission,
            query: &retained.query,
            response,
            identity: retained.identity,
            fence: retained.fence,
        };
        // No take/reinsert of the state: errors and unwinding leave its mutated
        // history in the original connection. The new frame grants no renewal.
        input.install(&mut retained.state).map_err(capture_error)
    }

    #[cfg(test)]
    pub(in super::super) fn parked_direct_introduction_observation(
        &self,
        session: &StorageNodeSession,
        pg: PgId,
    ) -> Option<(
        MetadataCommandId,
        crate::node_runtime::pg_store::DirectIntroductionInstallation,
        Instant,
        u64,
    )> {
        let held = session.metadata_command_guards.get(&pg)?;
        let slot = held.introduction_installation.try_borrow().ok()?;
        let retained = slot.as_ref()?;
        Some((
            retained.state.id(),
            retained.state.installation(),
            retained.identity.local_deadline,
            retained.identity.cutoff.portable_wall_valid_until_ms,
        ))
    }
}
