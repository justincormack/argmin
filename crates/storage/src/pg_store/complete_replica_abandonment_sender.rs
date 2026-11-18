// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! One primary-confirmed remote replica attempt, borrowing original custody and
//! primary serialization. No extracted source, automatic retry or cleanup grant.

use super::*;
#[cfg(test)]
#[path = "complete_replica_abandonment_sender/api_tests.rs"]
mod api_tests;
use crate::node_runtime::clients::{
    CompletePrimarySessionBorrow, UnixStorageNodeClient, UnixStorageNodeMetadataCommandSession,
};

enum PrimarySerialization<'a> {
    Embedded(&'a EmbeddedPublicationSection<'a>),
    Remote(CompletePrimarySessionBorrow<'a>),
}

impl PrimarySerialization<'_> {
    fn require(&self, owner: &CompleteFanoutSender<'_>) -> Result<(), StoreError> {
        owner.require_authority()?;
        match self {
            Self::Embedded(section) => owner.route.require_embedded_section(section),
            // The exact session/domain/PG/epoch was checked while acquiring this
            // guard. Holding its mutex prevents release or concurrent reuse.
            Self::Remote(_guard) => Ok(()),
        }
    }
}

pub(in crate::node_runtime) struct CompleteReplicaAbandonmentAttempt<'a> {
    owner: &'a CompleteFanoutSender<'a>,
    primary: PrimarySerialization<'a>,
    transport: &'a CompletePublicationTransport<'a>,
    observation: &'a mut ReplicaAbandonmentObservation,
    previous: ReplicaAbandonmentObservation,
}

impl CompleteReplicaAbandonmentAttempt<'_> {
    pub(in crate::node_runtime::pg_store) fn require_transport(
        &self,
        transport: &CompletePublicationTransport<'_>,
    ) -> Result<(), StoreError> {
        if !std::ptr::eq(self.transport, transport) {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "replica abandonment exact attempt transport",
            });
        }
        Ok(())
    }

    pub(in crate::node_runtime) fn node_id(&self) -> NodeId {
        self.transport.node_id()
    }
    pub(in crate::node_runtime) fn deadline(&self) -> Instant {
        self.transport.deadline()
    }
    pub(in crate::node_runtime::pg_store) fn primary(&self) -> NodeId {
        self.owner.route.primary()
    }

    pub(in crate::node_runtime) fn require_client(
        &self,
        client: &UnixStorageNodeClient,
    ) -> Result<(), StoreError> {
        self.primary.require(self.owner)?;
        self.owner.route.require_client(client)?;
        if client.node_id() != self.node_id() || client.node_id() == self.primary() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "replica abandonment exact remote client",
            });
        }
        self.transport.require_fanout_route(&self.owner.route)
    }

    pub(in crate::node_runtime::pg_store) fn require_request(
        &self,
        request: &CompletePublicationRequest<'_>,
    ) -> Result<(), StoreError> {
        self.primary.require(self.owner)?;
        self.owner.route.require_command(
            request.id(),
            request.checksum_crc64(),
            request.command_sha256(),
        )?;
        let expected = self
            .owner
            .sender
            .source
            .request(self.node_id())
            .map_err(|_| StoreError::RouteCapabilitySubjectMismatch {
                operation: "replica abandonment retained request",
            })?;
        if request.node_id() != self.node_id()
            || request.node_id() == self.primary()
            || !self
                .owner
                .route
                .nodes()
                .any(|node| node == request.node_id())
            || request.id() != expected.id()
            || request.command_sha256() != expected.command_sha256()
            || request.topology() != expected.topology()
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "replica abandonment retained complete custody",
            });
        }
        Ok(())
    }

    pub(in crate::node_runtime) fn encode_request(&self) -> Result<Vec<u8>, StoreError> {
        self.transport
            .encode_replica_abandonment_request(self)
            .map_err(|error| match error {
                crate::node_runtime::pg_store::CompletePendingInspectionError::Store(error) => {
                    error
                }
                other => self.payload_error("encode replica abandonment", other.to_string()),
            })
    }

    pub(in crate::node_runtime) fn not_sent(
        self,
        error: StoreError,
    ) -> CompleteAbandonmentSendError {
        *self.observation = self.previous;
        CompleteAbandonmentSendError::NotSent(error)
    }

    pub(in crate::node_runtime) fn unconfirmed(
        self,
        error: StoreError,
    ) -> CompleteAbandonmentSendError {
        CompleteAbandonmentSendError::Unconfirmed(error)
    }

    pub(in crate::node_runtime) fn confirm(
        self,
        bytes: &[u8],
    ) -> Result<(), CompleteAbandonmentSendError> {
        self.transport
            .decode_replica_abandonment_confirmation(self.primary(), bytes)
            .map_err(|error| {
                CompleteAbandonmentSendError::Unconfirmed(
                    self.payload_error("confirm replica abandonment", error.to_string()),
                )
            })?;
        // Late exact evidence confirms this past attempt only. It cannot extend
        // the deadline or authorize another member, retirement or cleanup.
        *self.observation = ReplicaAbandonmentObservation::TombstoneConfirmed;
        Ok(())
    }

    fn payload_error(&self, operation: &'static str, detail: String) -> StoreError {
        StoreError::StorageRpc {
            node_id: self.node_id().as_u32(),
            operation,
            failure: crate::storage_rpc::StorageRpcErrorCode::PayloadDecode,
            detail: crate::StorageNodeFailureDetail::new(detail),
        }
    }
}

impl CompleteUnstartedAbandonment<'_> {
    pub(in crate::node_runtime) fn record_remote_replica_from_embedded_primary(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        replica: &UnixStorageNodeClient,
        deadline: Instant,
    ) -> Result<(), CompleteAbandonmentSendError> {
        self.with_replica_remote_attempt(
            replica,
            PrimarySerialization::Embedded(section),
            deadline,
            |attempt| replica.send_complete_replica_abandonment(attempt),
        )
    }

    pub(in crate::node_runtime) fn record_remote_replica_from_remote_primary(
        &mut self,
        primary: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
        replica: &UnixStorageNodeClient,
        deadline: Instant,
    ) -> Result<(), CompleteAbandonmentSendError> {
        self.require_replica(replica.node_id())
            .map_err(CompleteAbandonmentSendError::NotSent)?;
        self.owner
            .require_primary(primary, session)
            .map_err(CompleteAbandonmentSendError::NotSent)?;
        self.owner
            .route
            .require_client(replica)
            .map_err(CompleteAbandonmentSendError::NotSent)?;
        let deadline = deadline.min(self.owner.work_deadline());
        let serialization = session
            .borrow_complete_primary(
                primary,
                self.owner.route.pg_id(),
                self.owner.route.epoch(),
                deadline,
            )
            .map_err(CompleteAbandonmentSendError::NotSent)?;
        self.with_replica_remote_attempt(
            replica,
            PrimarySerialization::Remote(serialization),
            deadline,
            |attempt| replica.send_complete_replica_abandonment(attempt),
        )
    }

    fn with_replica_remote_attempt(
        &mut self,
        client: &UnixStorageNodeClient,
        primary: PrimarySerialization<'_>,
        deadline: Instant,
        send: impl FnOnce(
            CompleteReplicaAbandonmentAttempt<'_>,
        ) -> Result<(), CompleteAbandonmentSendError>,
    ) -> Result<(), CompleteAbandonmentSendError> {
        self.require_replica(client.node_id())
            .map_err(CompleteAbandonmentSendError::NotSent)?;
        self.owner
            .route
            .require_client(client)
            .map_err(CompleteAbandonmentSendError::NotSent)?;
        primary
            .require(&self.owner)
            .map_err(CompleteAbandonmentSendError::NotSent)?;
        if self.replica_observation(client.node_id())
            == ReplicaAbandonmentObservation::TombstoneConfirmed
        {
            return Ok(());
        }
        let request = self
            .owner
            .sender
            .source
            .request(client.node_id())
            .map_err(|_| {
                CompleteAbandonmentSendError::NotSent(StoreError::RouteCapabilitySubjectMismatch {
                    operation: "replica abandonment retained source",
                })
            })?;
        let transport = request.prepare_transport(deadline);
        let observation = self
            .replicas
            .entry(client.node_id())
            .or_insert(ReplicaAbandonmentObservation::NoAttemptObserved);
        let previous = *observation;
        *observation = ReplicaAbandonmentObservation::Unconfirmed;
        send(CompleteReplicaAbandonmentAttempt {
            owner: &self.owner,
            primary,
            transport: &transport,
            observation,
            previous,
        })
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_remote_replica_attempt(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        client: &UnixStorageNodeClient,
        deadline: Instant,
        action: impl FnOnce(
            CompleteReplicaAbandonmentAttempt<'_>,
        ) -> Result<(), CompleteAbandonmentSendError>,
    ) -> Result<(), CompleteAbandonmentSendError> {
        self.with_replica_remote_attempt(
            client,
            PrimarySerialization::Embedded(section),
            deadline,
            action,
        )
    }
}
