// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! One closed remote attempt; the owner keeps context, first cutoff and history.

use super::*;
use crate::node_runtime::clients::{UnixStorageNodeClient, UnixStorageNodeMetadataCommandSession};

#[derive(Debug, thiserror::Error)]
pub(in crate::node_runtime) enum CompleteAbandonmentSendError {
    #[error("complete abandonment not sent: {0}")]
    NotSent(StoreError),
    #[error("complete abandonment unconfirmed: {0}")]
    Unconfirmed(StoreError),
}

pub(in crate::node_runtime) struct CompleteAbandonmentAttempt<'a> {
    transport: &'a CompletePublicationTransport<'a>,
    custody: &'a UnstartedAbandonmentAuthority<'a, 'a>,
    observation: &'a mut UnstartedAbandonmentObservation,
    previous: UnstartedAbandonmentObservation,
}

impl CompleteAbandonmentAttempt<'_> {
    pub(in crate::node_runtime) fn node_id(&self) -> NodeId {
        self.transport.node_id()
    }
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.transport.id()
    }
    pub(in crate::node_runtime) fn deadline(&self) -> Instant {
        self.transport.deadline()
    }
    pub(in crate::node_runtime) fn encode_request(&self) -> Result<Vec<u8>, StoreError> {
        self.transport
            .encode_abandonment_request(self.custody)
            .map_err(|error| match error {
                crate::node_runtime::pg_store::CompletePendingInspectionError::Store(error) => {
                    error
                }
                other => self.payload_error("encode complete abandonment", other.to_string()),
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
            .decode_abandonment_confirmation(bytes)
            .map_err(|error| {
                CompleteAbandonmentSendError::Unconfirmed(
                    self.payload_error("confirm complete abandonment", error.to_string()),
                )
            })?;
        // A late exact reply confirms this past effect only; it grants no new
        // time, replica abandonment, slot retirement or physical cleanup.
        *self.observation = UnstartedAbandonmentObservation::PrimaryTombstoneConfirmed;
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
    pub(in crate::node_runtime) fn open_primary_session(
        &self,
        client: &UnixStorageNodeClient,
        deadline: Instant,
    ) -> Result<UnixStorageNodeMetadataCommandSession, StoreError> {
        self.owner.open_primary_session(client, deadline)
    }

    pub(in crate::node_runtime) fn record_primary_remote(
        &mut self,
        client: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
        deadline: Instant,
    ) -> Result<(), CompleteAbandonmentSendError> {
        self.owner
            .require_primary(client, session)
            .map_err(CompleteAbandonmentSendError::NotSent)?;
        self.with_remote_attempt(deadline, |attempt| {
            session.send_complete_abandonment(attempt)
        })
    }

    fn with_remote_attempt(
        &mut self,
        deadline: Instant,
        send: impl FnOnce(CompleteAbandonmentAttempt<'_>) -> Result<(), CompleteAbandonmentSendError>,
    ) -> Result<(), CompleteAbandonmentSendError> {
        self.owner
            .require_authority()
            .map_err(CompleteAbandonmentSendError::NotSent)?;
        if self.observation == UnstartedAbandonmentObservation::PrimaryTombstoneConfirmed {
            return Ok(());
        }
        let request = self
            .owner
            .sender
            .source
            .request(self.owner.route.primary())
            .map_err(|_| {
                CompleteAbandonmentSendError::NotSent(StoreError::RouteCapabilitySubjectMismatch {
                    operation: "complete abandonment retained request",
                })
            })?;
        let custody = UnstartedAbandonmentAuthority { owner: &self.owner };
        custody
            .require_request(&request)
            .map_err(CompleteAbandonmentSendError::NotSent)?;
        let transport = request.prepare_transport(deadline);
        let previous = self.observation;
        self.observation = UnstartedAbandonmentObservation::Unconfirmed;
        send(CompleteAbandonmentAttempt {
            transport: &transport,
            custody: &custody,
            observation: &mut self.observation,
            previous,
        })
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_remote_attempt(
        &mut self,
        deadline: Instant,
        action: impl FnOnce(CompleteAbandonmentAttempt<'_>) -> Result<(), CompleteAbandonmentSendError>,
    ) -> Result<(), CompleteAbandonmentSendError> {
        self.with_remote_attempt(deadline, action)
    }
}
