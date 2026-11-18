// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Application stays inside the original publication holder. Per-node history
//! is observational: it is not an acting-set completion or retirement proof.

use super::*;
use crate::cluster::EmbeddedApplicationTarget;
use crate::metadata_command::MetadataCommandReplicaState;
use crate::node_runtime::clients::UnixStorageNodeClient;
use crate::node_runtime::pg_store::{CompleteApplicationError, CompleteApplicationRequest};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::node_runtime) enum ApplicationObservation {
    NoAttemptObserved,
    Unconfirmed,
    Applied,
}

#[derive(Debug, thiserror::Error)]
pub(in crate::node_runtime) enum CompleteApplicationSendError {
    #[error("complete application not sent: {0}")]
    NotSent(StoreError),
    #[error("complete application unconfirmed: {0}")]
    Unconfirmed(StoreError),
}

#[derive(Debug, thiserror::Error)]
pub(in crate::node_runtime) enum ApplicationAttemptError {
    #[error("complete publication marker is not confirmed")]
    PublicationNotConfirmed,
    #[error(transparent)]
    Preparation(#[from] StorageRpcPayloadError),
    #[error(transparent)]
    Remote(#[from] CompleteApplicationSendError),
    #[error(transparent)]
    Embedded(#[from] CompleteApplicationError),
}

/// Borrowed only by a closed client operation. Constructing, dropping or
/// unwinding an attempt cannot detach the complete context or reset history.
pub(in crate::node_runtime) struct CompleteApplicationAttempt<'a> {
    transport: &'a CompletePublicationTransport<'a>,
    observation: &'a mut ApplicationObservation,
    previous: ApplicationObservation,
}

impl CompleteApplicationAttempt<'_> {
    pub(in crate::node_runtime) fn require_fanout_route(
        &self,
        route: &crate::cluster::CompleteFanoutRoute<'_>,
    ) -> Result<(), StoreError> {
        self.transport.require_fanout_route(route)
    }

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
            .encode_application_request()
            .map_err(|error| match error {
                CompletePendingInspectionError::Store(error) => error,
                other => self.payload_error("encode complete application", other.to_string()),
            })
    }

    pub(in crate::node_runtime) fn not_sent(
        self,
        error: StoreError,
    ) -> CompleteApplicationSendError {
        *self.observation = self.previous;
        CompleteApplicationSendError::NotSent(error)
    }

    pub(in crate::node_runtime) fn unconfirmed(
        self,
        error: StoreError,
    ) -> CompleteApplicationSendError {
        CompleteApplicationSendError::Unconfirmed(error)
    }

    pub(in crate::node_runtime) fn confirm(
        self,
        bytes: &[u8],
    ) -> Result<MetadataCommandReplicaState, CompleteApplicationSendError> {
        let state = self
            .transport
            .decode_application_confirmation(bytes)
            .map_err(|error| {
                CompleteApplicationSendError::Unconfirmed(
                    self.payload_error("confirm complete application", error.to_string()),
                )
            })?;
        *self.observation = ApplicationObservation::Applied;
        Ok(state)
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

impl CompletePublicationSender {
    pub(in crate::node_runtime) fn apply_embedded(
        &mut self,
        target: &EmbeddedApplicationTarget<'_>,
        deadline: Instant,
    ) -> Result<MetadataCommandReplicaState, ApplicationAttemptError> {
        self.apply_embedded_inner(
            target,
            deadline,
            #[cfg(test)]
            |_| Ok(()),
        )
    }

    fn apply_embedded_inner(
        &mut self,
        target: &EmbeddedApplicationTarget<'_>,
        deadline: Instant,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<MetadataCommandReplicaState, ApplicationAttemptError> {
        if self.observation != PublicationObservation::MarkerConfirmed {
            return Err(ApplicationAttemptError::PublicationNotConfirmed);
        }
        let request = self.source.request(target.node_id())?;
        let transport = request.prepare_transport(deadline);
        let application = CompleteApplicationRequest::from_publication_transport(&transport);
        let observation = self
            .applications
            .entry(target.node_id())
            .or_insert(ApplicationObservation::NoAttemptObserved);
        // Before entering the closed node operation, including mutex waits and
        // unwind. A prior confirmation survives subsequent failed exact retries.
        if *observation != ApplicationObservation::Applied {
            *observation = ApplicationObservation::Unconfirmed;
        }
        #[cfg(not(test))]
        let state = target.apply_complete_command(&application)?;
        #[cfg(test)]
        let state = target.test_with_application_authority(&application, |pg, authority| {
            pg.test_apply_embedded_complete_command_with_hook(authority, hook)
        })?;
        *observation = ApplicationObservation::Applied;
        Ok(state)
    }

    #[cfg(test)]
    pub(in crate::node_runtime) fn test_apply_embedded_with_hook(
        &mut self,
        target: &EmbeddedApplicationTarget<'_>,
        deadline: Instant,
        hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<MetadataCommandReplicaState, ApplicationAttemptError> {
        self.apply_embedded_inner(target, deadline, hook)
    }

    /// Owner test seam only: observe the exact prepared attempt or unwind from
    /// it. It can never return a manufactured successful confirmation.
    #[cfg(test)]
    pub(in crate::node_runtime) fn test_application_attempt(
        &mut self,
        node: NodeId,
        deadline: Instant,
        observe: impl FnOnce(&[u8], Instant),
    ) -> Result<MetadataCommandReplicaState, ApplicationAttemptError> {
        self.with_application_attempt(node, deadline, |attempt| {
            let payload = match attempt.encode_request() {
                Ok(payload) => payload,
                Err(error) => return Err(attempt.not_sent(error)),
            };
            observe(&payload, attempt.deadline());
            Err(attempt.unconfirmed(StoreError::OperationDeadlineExceeded {
                context: "injected complete application attempt interruption",
            }))
        })
    }

    pub(in crate::node_runtime) fn application_observation(
        &self,
        node: NodeId,
    ) -> ApplicationObservation {
        self.applications
            .get(&node)
            .copied()
            .unwrap_or(ApplicationObservation::NoAttemptObserved)
    }

    /// Replica send. Receiver admission proves current membership or exact
    /// report-authorized historical placement;
    /// these individual observations do not establish complete fanout coverage.
    pub(in crate::node_runtime) fn apply_remote(
        &mut self,
        client: &UnixStorageNodeClient,
        deadline: Instant,
    ) -> Result<MetadataCommandReplicaState, ApplicationAttemptError> {
        self.with_application_attempt(client.node_id(), deadline, |attempt| {
            client.send_complete_application(attempt)
        })
    }

    pub(super) fn apply_remote_in_fanout(
        &mut self,
        client: &UnixStorageNodeClient,
        route: &crate::cluster::CompleteFanoutRoute<'_>,
        deadline: Instant,
    ) -> Result<MetadataCommandReplicaState, ApplicationAttemptError> {
        self.with_application_attempt(client.node_id(), deadline, |attempt| {
            client.send_complete_fanout_application(route, attempt)
        })
    }

    /// Primary application on the already-held serialization session. Opening
    /// an unrelated connection here would contend with our own primary lock.
    pub(in crate::node_runtime) fn apply_remote_in_session(
        &mut self,
        session: &UnixStorageNodeMetadataCommandSession,
        deadline: Instant,
    ) -> Result<MetadataCommandReplicaState, ApplicationAttemptError> {
        self.with_application_attempt(self.node_id, deadline, |attempt| {
            session.send_complete_application(attempt)
        })
    }

    fn with_application_attempt(
        &mut self,
        node: NodeId,
        deadline: Instant,
        send: impl FnOnce(
            CompleteApplicationAttempt<'_>,
        ) -> Result<MetadataCommandReplicaState, CompleteApplicationSendError>,
    ) -> Result<MetadataCommandReplicaState, ApplicationAttemptError> {
        if self.observation != PublicationObservation::MarkerConfirmed {
            return Err(ApplicationAttemptError::PublicationNotConfirmed);
        }
        // This is the same source and OnceLock deadline used by publication,
        // never a reconstructed submission or a fresh application budget.
        let request = self.source.request(node)?;
        let transport = request.prepare_transport(deadline);
        let observation = self
            .applications
            .entry(node)
            .or_insert(ApplicationObservation::NoAttemptObserved);
        Ok(transport.with_application_attempt(observation, send)?)
    }

    pub(in crate::node_runtime::pg_store) fn apply_remote_held(
        &mut self,
        held: &mut crate::node_client::CompletePrimarySessionBorrow<'_>,
        deadline: Instant,
    ) -> Result<MetadataCommandReplicaState, ApplicationAttemptError> {
        self.with_application_attempt(self.node_id, deadline, |attempt| {
            held.send_complete_application(attempt)
        })
    }
}

impl CompletePublicationTransport<'_> {
    /// Closed transport reuse for an owning cleanup pair. The caller retains
    /// its source, cohort and history; no fresh sender/submission is created.
    pub(super) fn send_application_in_fanout(
        &self,
        route: &crate::cluster::CompleteFanoutRoute<'_>,
        client: &UnixStorageNodeClient,
        observation: &mut ApplicationObservation,
    ) -> Result<MetadataCommandReplicaState, CompleteApplicationSendError> {
        self.with_application_attempt(observation, |attempt| {
            client.send_complete_fanout_application(route, attempt)
        })
    }

    #[cfg(test)]
    pub(super) fn test_application_attempt(
        &self,
        observation: &mut ApplicationObservation,
        action: impl FnOnce(
            CompleteApplicationAttempt<'_>,
        )
            -> Result<MetadataCommandReplicaState, CompleteApplicationSendError>,
    ) -> Result<MetadataCommandReplicaState, CompleteApplicationSendError> {
        self.with_application_attempt(observation, action)
    }

    fn with_application_attempt(
        &self,
        observation: &mut ApplicationObservation,
        send: impl FnOnce(
            CompleteApplicationAttempt<'_>,
        ) -> Result<MetadataCommandReplicaState, CompleteApplicationSendError>,
    ) -> Result<MetadataCommandReplicaState, CompleteApplicationSendError> {
        let previous = *observation;
        if previous != ApplicationObservation::Applied {
            *observation = ApplicationObservation::Unconfirmed;
        }
        send(CompleteApplicationAttempt {
            transport: self,
            observation,
            previous,
        })
    }
}
