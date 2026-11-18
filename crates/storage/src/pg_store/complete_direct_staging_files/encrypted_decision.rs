// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! One request decision per attempt. This layer never drains contenders or
//! remaps request errors; the owner loop must reinspect and reevaluate on retry.
use super::*;

#[derive(Debug)]
pub(in crate::node_runtime) enum EncryptedDirectDecisionError<E> {
    Request(E),
    Storage(Error),
}

#[must_use]
pub(in crate::node_runtime) struct EncryptedDirectDecisionFailure<'route, E> {
    custody: Box<InspectedEncryptedDirectPayload<'route>>,
    error: EncryptedDirectDecisionError<E>,
}

impl<'route, E> EncryptedDirectDecisionFailure<'route, E> {
    pub(in crate::node_runtime) fn request_error(&self) -> Option<&E> {
        match &self.error {
            EncryptedDirectDecisionError::Request(error) => Some(error),
            _ => None,
        }
    }
    pub(in crate::node_runtime) fn storage_error(&self) -> Option<&Error> {
        match &self.error {
            EncryptedDirectDecisionError::Storage(error) => Some(error),
            _ => None,
        }
    }
    pub(in crate::node_runtime) fn error(&self) -> &EncryptedDirectDecisionError<E> {
        &self.error
    }
    pub(in crate::node_runtime) fn into_retry(self) -> InspectedEncryptedDirectPayload<'route> {
        *self.custody
    }
}

impl<'route> InspectedEncryptedDirectPayload<'route> {
    /// The same logical condition callback used by ordinary Direct PUT. Only
    /// borrowed request facts escape; success is not installation acceptance.
    pub(in crate::node_runtime) fn prepare_terminal_embedded_if<E>(
        self,
        section: &EmbeddedPublicationSection<'_>,
        input: &crate::StreamPutCommitInput,
        action: impl FnOnce(&crate::DirectPutCommitSnapshot) -> Result<(), E>,
    ) -> Result<PreparedEncryptedDirectTerminal<'route>, EncryptedDirectDecisionFailure<'route, E>>
    {
        let permit = DirectStagingPreparation {
            custody: &self.custody.prepared.custody,
        };
        let admitted = permit
            .require_section(section)
            .and_then(|()| section.require_effect().map_err(Into::into));
        self.decide(admitted, action)?
            .prepare_terminal_embedded(section, input)
            .map_err(Self::storage_decision_failure)
    }

    pub(in crate::node_runtime) fn prepare_terminal_remote_if<E>(
        self,
        client: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
        input: &crate::StreamPutCommitInput,
        action: impl FnOnce(&crate::DirectPutCommitSnapshot) -> Result<(), E>,
    ) -> Result<PreparedEncryptedDirectTerminal<'route>, EncryptedDirectDecisionFailure<'route, E>>
    {
        let admitted = (|| {
            let permit = DirectStagingPreparation {
                custody: &self.custody.prepared.custody,
            };
            permit.require_valid()?;
            let writer = self.custody.prepared.custody.writer();
            writer
                .window
                .terminal
                .owner
                .require_primary(client, session)?;
            let held = session.borrow_complete_primary(
                client,
                self.id().pg_id(),
                self.id().cluster_epoch(),
                permit.deadline(),
            )?;
            permit.require_remote_session(&held)
        })();
        // Release only the local session borrow, not the remotely held section.
        // Preparation below reacquires and rechecks the identical bound session.
        self.decide(admitted, action)?
            .prepare_terminal_remote(client, session, input)
            .map_err(Self::storage_decision_failure)
    }

    fn decide<E>(
        self,
        admitted: Result<(), Error>,
        action: impl FnOnce(&crate::DirectPutCommitSnapshot) -> Result<(), E>,
    ) -> Result<Self, EncryptedDirectDecisionFailure<'route, E>> {
        let result = self
            .work
            .require_deadline()
            .map_err(Error::from)
            .and(admitted)
            .map_err(EncryptedDirectDecisionError::Storage)
            .and_then(|()| {
                action(self.auth_snapshot()).map_err(EncryptedDirectDecisionError::Request)
            });
        match result {
            Ok(()) => Ok(self),
            Err(error) => Err(EncryptedDirectDecisionFailure {
                custody: Box::new(self),
                error,
            }),
        }
    }

    fn storage_decision_failure<E>(
        failure: StagingFileHandoffFailure<Self>,
    ) -> EncryptedDirectDecisionFailure<'route, E> {
        let (custody, error) = failure.into_parts();
        EncryptedDirectDecisionFailure {
            custody,
            error: EncryptedDirectDecisionError::Storage(error),
        }
    }
}
