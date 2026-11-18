// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Retain committed encryption until installation has transferred command custody.
//! Only definitive non-acceptance may discard a prepared command and snapshot.
use super::*;

#[path = "encrypted_publication.rs"]
mod publication;
use publication::BudgetedDirectOwner;

#[must_use]
pub(in crate::node_runtime) struct PreparedEncryptedDirectTerminal<'route> {
    prepared: PreparedDirectTerminal<'route>,
    encryption: CommittedObjectEncryption,
    work: DirectPutTerminalWork,
}

#[must_use]
pub(in crate::node_runtime) struct EncryptedDirectTerminalSubmission<'route> {
    submission: DirectTerminalSubmission<'route>,
    encryption: CommittedObjectEncryption,
    work: DirectPutTerminalWork,
}

#[must_use]
pub(in crate::node_runtime) struct EncryptedDirectSubmissionFailure<'route> {
    owner: Box<EncryptedDirectTerminalSubmission<'route>>,
    error: Error,
}

impl<'route> PreparedEncryptedDirectTerminal<'route> {
    pub(super) fn new(
        prepared: PreparedDirectTerminal<'route>,
        encryption: CommittedObjectEncryption,
        work: DirectPutTerminalWork,
    ) -> Self {
        Self {
            prepared,
            encryption,
            work,
        }
    }
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.prepared.id()
    }
    pub(in crate::node_runtime) fn into_submission(
        self,
    ) -> EncryptedDirectTerminalSubmission<'route> {
        EncryptedDirectTerminalSubmission {
            submission: self.prepared.into_submission(),
            encryption: self.encryption,
            work: self.work,
        }
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_command(
        &self,
    ) -> crate::metadata_command::MetadataCommandEnvelope {
        self.prepared.test_command()
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_binding_state(
        &self,
    ) -> (Instant, Option<ContextSubmissionObservation>) {
        self.prepared.test_binding_state()
    }
}

impl<'route> EncryptedDirectTerminalSubmission<'route> {
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_attempts(&self) -> usize {
        self.work.test_attempts()
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_limit_attempts(&mut self, limit: usize) {
        self.work.test_limit_attempts(limit);
    }
    pub(in crate::node_runtime) fn installation(&self) -> DirectTerminalInstallationObservation {
        self.submission.installation()
    }
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.submission.id()
    }
    pub(in crate::node_runtime) fn install_embedded(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<(), Error> {
        self.work.begin_attempt()?;
        self.submission.install_embedded(section)
    }
    pub(in crate::node_runtime) fn install_remote(
        &mut self,
        client: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
    ) -> Result<(), Error> {
        self.work.begin_attempt()?;
        self.submission.install_remote(client, session)
    }

    /// Drop only an unaccepted command and its stale snapshot. The returned
    /// owner must load fresh facts before another request decision/preparation;
    /// its original encryption, CRC, deadline and remote session remain bound.
    pub(in crate::node_runtime) fn into_reinspection(
        self,
    ) -> Result<AcknowledgedEncryptedDirectPayload<'route>, EncryptedDirectSubmissionFailure<'route>>
    {
        match self.submission.into_unaccepted_payload() {
            Ok(custody) => Ok(AcknowledgedEncryptedDirectPayload {
                crc64: custody.manifest.crc64(),
                custody: custody.prepared.custody,
                encryption: self.encryption,
                work: self.work,
                reinspect_prefix: false,
            }),
            Err(submission) => Err(EncryptedDirectSubmissionFailure {
                owner: Box::new(Self {
                    submission: *submission,
                    encryption: self.encryption,
                    work: self.work,
                }),
                error: Error::Window(
                    "direct reinspection requires no observed command acceptance".into(),
                ),
            }),
        }
    }

    pub(in crate::node_runtime) fn into_publication(
        self,
    ) -> Result<
        BudgetedDirectOwner<DirectTerminalPublication<'route>>,
        EncryptedDirectSubmissionFailure<'route>,
    > {
        if self.installation() != DirectTerminalInstallationObservation::Installed {
            return Err(EncryptedDirectSubmissionFailure {
                owner: Box::new(self),
                error: Error::Window("direct publication requires confirmed installation".into()),
            });
        }
        // The generic transition has exactly the same consuming predicate.
        // Only now may the extra frontend encryption evidence be discarded;
        // the installed command contains the exact committed metadata.
        let owner = self
            .submission
            .into_publication()
            .unwrap_or_else(|_| unreachable!("confirmed installation checked above"));
        Ok(BudgetedDirectOwner::new(owner, self.work))
    }
}

impl<'route> EncryptedDirectSubmissionFailure<'route> {
    pub(in crate::node_runtime) fn error(&self) -> &Error {
        &self.error
    }
    pub(in crate::node_runtime) fn into_submission(
        self,
    ) -> EncryptedDirectTerminalSubmission<'route> {
        *self.owner
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn staging_encrypted_installation_owners_are_noncloneable() {
        trait NotClone<A> {
            fn check() {}
        }
        impl<T: ?Sized> NotClone<()> for T {}
        impl<T: Clone> NotClone<u8> for T {}
        let _ = <PreparedEncryptedDirectTerminal<'_> as NotClone<_>>::check;
        let _ = <EncryptedDirectTerminalSubmission<'_> as NotClone<_>>::check;
        let _ = <EncryptedDirectSubmissionFailure<'_> as NotClone<_>>::check;
    }
}
