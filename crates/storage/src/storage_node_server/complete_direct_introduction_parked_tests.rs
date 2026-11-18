// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Multiple receiver calls using the actual held connection section. This is
//! owner-local coverage; no outer RPC dispatcher or socket client is selected.
use super::*;
use crate::node_runtime::pg_store::{
    DirectIntroductionInstallation, RemoteIntroductionInstallation,
};

pub(in crate::node_runtime) struct ParkedIntroductionTest<'a> {
    handler: &'a StorageNodeConnectionHandler,
    session: &'a mut StorageNodeSession,
    admission: &'a StorageNodeRouteAdmissionPermit,
    request: Vec<u8>,
    response: Vec<u8>,
    route: Route,
}

impl ParkedIntroductionTest<'_> {
    pub(in crate::node_runtime) fn install(&self) -> Result<(), StorageRpcErrorResponse> {
        self.handler.install_parked_direct_introduction(
            self.session,
            self.admission,
            &self.request,
            &self.response,
        )
    }
    pub(in crate::node_runtime) fn observation(&self) -> DirectIntroductionInstallation {
        self.handler
            .parked_direct_introduction_observation(self.session, self.route.pg)
            .unwrap()
            .1
    }
    pub(in crate::node_runtime) fn bounds(&self) -> (Instant, u64) {
        let value = self
            .handler
            .parked_direct_introduction_observation(self.session, self.route.pg)
            .unwrap();
        (value.2, value.3)
    }
    pub(in crate::node_runtime) fn reject_crossed_calls(&self) {
        let before = self.observation();
        for permit in [
            self.handler
                .route_admission
                .acquire(StorageNodeRouteAdmissionClass::RetainedCleanup),
            StorageNodeRouteAdmissionGate::default()
                .acquire(StorageNodeRouteAdmissionClass::Active),
        ] {
            assert!(self
                .handler
                .install_parked_direct_introduction(
                    self.session,
                    &permit,
                    &self.request,
                    &self.response
                )
                .is_err());
        }
        let logical = wire::decode_request(&self.request).unwrap();
        let encoded = wire::encode_request(&logical).unwrap();
        for field in 0..3 {
            let decoded = encoded.decode_response(&self.response).unwrap();
            let mut bytes = decoded.command.command.to_vec();
            let mut cutoff = decoded.cutoff;
            let mut checksum = decoded.command.checksum;
            match field {
                0 => {
                    *bytes.last_mut().unwrap() ^= 1;
                    checksum = checksum::crc64::checksum(&bytes);
                }
                1 => cutoff.portable_wall_valid_until_ms -= 1,
                2 => checksum ^= 1,
                _ => unreachable!(),
            }
            let response = wire::encode_response(
                &encoded,
                crate::storage_rpc::complete_applied::BorrowedRequest {
                    checksum,
                    command: &bytes,
                    ..decoded.command
                },
                cutoff,
            )
            .unwrap();
            encoded.decode_response(&response).unwrap();
            assert!(self
                .handler
                .install_parked_direct_introduction(
                    self.session,
                    self.admission,
                    &self.request,
                    &response
                )
                .is_err());
        }
        let mut crossed = logical;
        crossed.proof.owner_token.push('x');
        let crossed = wire::encode_request(&crossed).unwrap();
        let decoded = encoded.decode_response(&self.response).unwrap();
        let response = wire::encode_response(&crossed, decoded.command, decoded.cutoff).unwrap();
        assert!(self
            .handler
            .install_parked_direct_introduction(
                self.session,
                self.admission,
                crossed.bytes(),
                &response
            )
            .is_err());
        // Reentrant access returns an error, never panics or removes history.
        let _busy = self.session.metadata_command_guards[&self.route.pg]
            .introduction_installation
            .borrow_mut();
        assert!(self.install().is_err());
        drop(_busy);
        assert_eq!(self.observation(), before);
    }

    fn reject_reopened_section(&mut self) {
        // Deliberately move the opaque record to a new guard to pin the binding,
        // rather than obtaining rejection merely because the new guard is empty.
        let (retained, claim) = {
            let held = &self.session.metadata_command_guards[&self.route.pg];
            (
                held.introduction_installation.borrow_mut().take().unwrap(),
                held.introduction_preparation.get(),
            )
        };
        self.session.release_metadata_command_pg_lock(self.route.pg);
        self.session
            .acquire_metadata_command_pg_lock(
                &self.handler.metadata_command_locks,
                self.route.node,
                StorageNodeMetadataCommandLockBinding {
                    pg_id: self.route.pg,
                    cluster_epoch: self.route.epoch,
                    authority: StorageNodeMetadataCommandLockAuthority::CurrentPrimary,
                },
                None,
            )
            .unwrap();
        let held = &self.session.metadata_command_guards[&self.route.pg];
        held.introduction_preparation.set(claim);
        *held.introduction_installation.borrow_mut() = Some(retained);
        let error = self.install().unwrap_err();
        assert!(
            error.message.contains("retained connection custody"),
            "{error:?}"
        );
    }
}

impl StorageNodeServer {
    pub(in crate::node_runtime) fn test_direct_introduction_parked(
        &self,
        expected: &Expected,
        bucket: &BucketName,
        key: &ObjectKey,
        attempt: &crate::SessionId,
        proof: &BucketWriteReservationProof,
        work: (
            Duration,
            impl FnOnce(&mut RemoteIntroductionInstallation<'_>),
            impl FnOnce(&mut ParkedIntroductionTest<'_>),
        ),
    ) {
        let mut handler = self.connection_handler();
        let auth = handler.rpc_auth.as_ref().unwrap();
        let limits = auth.transport_limits();
        handler.rpc_auth = Some(Arc::new(
            (**auth).clone().with_transport_limits(
                crate::storage_rpc_auth::StorageRpcTransportLimits::new(
                    limits.max_frame_bytes(),
                    limits.max_connections(),
                    work.0,
                )
                .unwrap(),
            ),
        ));
        let q = query(self, expected, bucket, key, attempt, proof);
        let route = q.route;
        let mut session = held(&handler, route);
        let (request, response, bounds, observation) = {
            let active = self
                .route_admission
                .acquire(StorageNodeRouteAdmissionClass::Active);
            let response = handler
                .prepare_direct_introduction(&session, &active, &q, |_| {})
                .unwrap()
                .encode_response()
                .unwrap();
            let request = wire::encode_request(&wire::Request {
                route,
                bucket: q.bucket.clone(),
                key: q.key.clone(),
                attempt: q.attempt.clone(),
                proof: q.proof.clone(),
            })
            .unwrap();
            let mut owner = handler
                .claim_direct_introduction(&session, &active, &q, &response)
                .unwrap();
            work.1(&mut owner);
            let bounds = owner.test_bounds();
            let observation = owner.installation();
            // Failed transfer retains the exact owning value/history, even if
            // authority has expired; successful parking performs no DB effects.
            let busy = session.metadata_command_guards[&route.pg]
                .introduction_installation
                .borrow_mut();
            let failure = owner.park().expect_err("busy slot accepted parking");
            assert!(failure
                .error()
                .to_string()
                .contains("retained connection custody"));
            drop(busy);
            let owner = failure.into_owner();
            assert_eq!(owner.installation(), observation);
            assert_eq!(owner.test_bounds(), bounds);
            owner
                .park()
                .unwrap_or_else(|failure| panic!("{}", failure.error()));
            (request.bytes().to_vec(), response, bounds, observation)
        };
        drop(q); // Original query and per-frame admission no longer exist.
                 // Actual mutable session use between calls: no self-referential holder.
        session.set_current_rpc_context(77, StorageRpcMessageKind::MetadataCommandReplicaState);
        let active = self
            .route_admission
            .acquire(StorageNodeRouteAdmissionClass::Active);
        let mut resumed = ParkedIntroductionTest {
            handler: &handler,
            session: &mut session,
            admission: &active,
            request,
            response,
            route,
        };
        assert_eq!(resumed.observation(), observation);
        assert_eq!(resumed.bounds(), bounds);
        work.2(&mut resumed);
        resumed.reject_reopened_section();
    }
}
