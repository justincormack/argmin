// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Owning, descriptive preparation over the exact admitted primary connection.
//! No installable context, renewed deadline or physical authority is exported.
use super::*;
use crate::cluster::CompleteFanoutRoute;
use crate::node_client::{
    MetadataCommandSessionIdentity, UnixStorageNodeClient, UnixStorageNodeMetadataCommandSession,
};
use crate::storage_rpc::{complete_direct_introduction as wire, complete_settled_prefix};

#[path = "payload_staging_direct_introduction_sender.rs"]
mod sender;
pub(in crate::node_runtime::pg_store) use sender::RemoteIntroductionRetirementScope;
pub(in crate::node_runtime) use sender::{
    RemoteDirectIntroductionPublication, RemoteDirectIntroductionSubmission,
};
pub(in crate::node_runtime::pg_store) use sender::{
    RemoteIntroductionFileScope, RemoteIntroductionWindowScope,
};

#[must_use]
pub(in crate::node_runtime) struct RemoteDirectIntroductionPreparation<'a> {
    binding: RemoteDirectIntroductionBinding<'a>,
    submission: submission::ContextualSubmission,
}

pub(in crate::node_runtime::pg_store) struct RemoteDirectIntroductionBinding<'a> {
    route: &'a CompleteFanoutRoute<'a>,
    client: &'a UnixStorageNodeClient,
    session: &'a UnixStorageNodeMetadataCommandSession,
    identity: MetadataCommandSessionIdentity,
    request: wire::EncodedRequest,
    response: Vec<u8>,
    deadline: Instant,
    cutoff: StorageRpcOperationDeadline,
}

impl<'a> CompleteFanoutRoute<'a> {
    pub(in crate::node_runtime) fn prepare_direct_introduction_remote(
        &'a self,
        client: &'a UnixStorageNodeClient,
        session: &'a UnixStorageNodeMetadataCommandSession,
        subject: (
            &BucketName,
            &ObjectKey,
            &SessionId,
            &BucketWriteReservationProof,
        ),
        deadline: Instant,
    ) -> Result<RemoteDirectIntroductionPreparation<'a>, StagingWindowConfirmationError> {
        let (bucket, key, attempt, proof) = subject;
        // Current admission only; historical recovery cannot mint introductions.
        let deadline = self.window_confirmation_deadline(client, deadline)?;
        if self.topology().object_pg_for(bucket, key) != self.pg_id().get() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "direct introduction admitted object PG",
            }
            .into());
        }
        let (deadline, cutoff) = self.intersect_deadline(
            deadline,
            StorageRpcOperationDeadline::from_instant(deadline),
        );
        let logical = wire::Request {
            route: complete_settled_prefix::Request {
                node: self.primary(),
                epoch: self.epoch(),
                pg: self.pg_id(),
                deadline: cutoff,
                topology: complete_settled_prefix::topology_digest(self.topology()),
            },
            bucket: bucket.clone(),
            key: key.clone(),
            attempt: attempt.clone(),
            proof: proof.clone(),
        };
        let invalid = |error: String| StagingWindowConfirmationError::Window(error);
        let request = wire::encode_request(&logical).map_err(|e| invalid(e.to_string()))?;
        let identity = session.identity();
        let held = session.borrow_complete_primary(client, self.pg_id(), self.epoch(), deadline)?;
        self.require_valid()?;
        let reply = held.prepare_direct_introduction(&request)?;
        let response = reply.bytes();
        let decoded = request
            .decode_response(response)
            .map_err(|e| invalid(e.to_string()))?;
        let context = ContextualCommand::decode_command(
            decoded.command.command,
            decoded.command.checksum,
            self.topology(),
        )
        .map_err(|e| invalid(e.to_string()))?;
        context
            .require_remote_direct_introduction(
                &logical,
                decoded.cutoff.portable_wall_valid_until_ms,
            )
            .map_err(|e| invalid(e.to_string()))?;
        let canonical = context
            .encode_command()
            .map_err(|e| invalid(e.to_string()))?;
        if canonical.bytes() != decoded.command.command
            || canonical.command_sha256() != decoded.command_sha256
        {
            return Err(invalid("noncanonical direct introduction reply".into()));
        }
        drop(canonical);
        let cutoff = decoded.cutoff;
        let deadline = deadline.min(cutoff.local_deadline());
        // Keep the exact transport locked and armed through every fallible
        // validation. Failure cannot leave the receiver's sole parked state
        // stranded behind an apparently reusable connection.
        self.window_confirmation_deadline(client, deadline)?;
        session.require_identity(&identity)?;
        AdmittedRouteEffectFence::bind_portable(
            self.epoch(),
            cutoff.portable_wall_valid_until_ms,
            cutoff.portable_wall_valid_until_ms,
        )
        .require_valid_for(self.epoch())?;
        let response = reply.finish_validation()?;
        let prepared = RemoteDirectIntroductionPreparation {
            submission: context.into_submission(),
            binding: RemoteDirectIntroductionBinding {
                route: self,
                client,
                session,
                identity,
                request,
                response,
                deadline,
                cutoff,
            },
        };
        Ok(prepared)
    }
}

impl RemoteDirectIntroductionPreparation<'_> {
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.submission.observation().command_id
    }

    pub(in crate::node_runtime) fn require_valid(&self) -> Result<(), StoreError> {
        let _held = self.binding.borrow_primary()?;
        Ok(())
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_bytes(&self) -> (&[u8], &[u8]) {
        (self.binding.request.bytes(), &self.binding.response)
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_bounds(&self) -> (Instant, u64) {
        (
            self.binding.deadline,
            self.binding.cutoff.portable_wall_valid_until_ms,
        )
    }
}

impl RemoteDirectIntroductionBinding<'_> {
    fn require_authority(&self) -> Result<(), StoreError> {
        self.route
            .window_confirmation_deadline(self.client, self.deadline)?;
        self.session.require_identity(&self.identity)?;
        AdmittedRouteEffectFence::bind_portable(
            self.route.epoch(),
            self.cutoff.portable_wall_valid_until_ms,
            self.cutoff.portable_wall_valid_until_ms,
        )
        .require_valid_for(self.route.epoch())
    }

    fn borrow_primary(
        &self,
    ) -> Result<crate::node_client::CompletePrimarySessionBorrow<'_>, StoreError> {
        self.require_authority()?;
        let held = self.session.borrow_complete_primary(
            self.client,
            self.route.pg_id(),
            self.route.epoch(),
            self.deadline,
        )?;
        self.require_session(&held)?;
        Ok(held)
    }
    pub(in crate::node_runtime::pg_store) fn require_session(
        &self,
        held: &crate::node_client::CompletePrimarySessionBorrow<'_>,
    ) -> Result<(), StoreError> {
        self.require_authority()?;
        held.require_identity(&self.identity)
    }
    pub(in crate::node_runtime::pg_store) fn deadline(&self) -> Instant {
        self.deadline
    }

    pub(in crate::node_runtime::pg_store) fn installation_request(
        &self,
        id: MetadataCommandId,
        digest: [u8; 32],
    ) -> Result<Vec<u8>, StagingWindowConfirmationError> {
        let invalid = |e: crate::storage_rpc::StorageRpcPayloadError| {
            StagingWindowConfirmationError::Window(e.to_string())
        };
        let response = self
            .request
            .decode_response(&self.response)
            .map_err(invalid)?;
        if response.command_sha256 != digest
            || id.pg_id() != self.route.pg_id()
            || id.cluster_epoch() != self.route.epoch()
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "introduction exact prepared submission",
            }
            .into());
        }
        crate::storage_rpc::complete_direct_introduction_install::encode_request(
            crate::storage_rpc::complete_direct_introduction_install::Request {
                preparation: self.request.bytes(),
                response: &self.response,
            },
        )
        .map_err(invalid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn staging_direct_introduction_transport_sender_is_noncloneable() {
        trait NotClone<A> {
            fn check() {}
        }
        impl<T: ?Sized> NotClone<()> for T {}
        impl<T: Clone> NotClone<u8> for T {}
        let _ = <RemoteDirectIntroductionPreparation<'_> as NotClone<_>>::check;
    }
}
