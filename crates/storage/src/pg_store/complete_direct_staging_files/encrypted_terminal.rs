// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Encryption/CRC remain attached through snapshot inspection and terminal
//! preparation. Only a completely bound terminal command exits this subtree.
use super::*;
use crate::cluster::DirectPutTerminalWork;

#[path = "encrypted_contender.rs"]
mod contender;

#[path = "encrypted_decision.rs"]
mod decision;

#[path = "encrypted_installation.rs"]
mod installation;
use installation::PreparedEncryptedDirectTerminal;

#[must_use]
pub(in crate::node_runtime) struct AcknowledgedEncryptedDirectPayload<'route> {
    custody: AcknowledgedDirectStagingPayload<'route>,
    encryption: CommittedObjectEncryption,
    crc64: u64,
    work: DirectPutTerminalWork,
    // A drained contender advances the settled prefix, not physical authority.
    // The next snapshot must reconfirm this same window before preparation.
    reinspect_prefix: bool,
}

#[must_use]
pub(in crate::node_runtime) struct InspectedEncryptedDirectPayload<'route> {
    custody: WitnessedDirectPayload<'route>,
    encryption: CommittedObjectEncryption,
    work: DirectPutTerminalWork,
}

impl<'route> EncryptedDirectStagingPayload<'route> {
    pub(in crate::node_runtime) fn into_acknowledged(
        mut self,
    ) -> Result<AcknowledgedEncryptedDirectPayload<'route>, StagingFileHandoffFailure<Self>> {
        if let Err(error) = self
            .preparation
            .writer
            .writer
            .acknowledgements
            .require_confirmed()
        {
            return Err(StagingFileHandoffFailure::new(self, error));
        }
        // This is the sole consuming entry into terminal work. Reinspection and
        // failed attempts retain these bounds; they cannot start a new budget.
        // Preserve the original portable cutoff as well as monotonic time: a
        // later wall-clock adjustment must never extend authority in transit.
        let remote = &mut self.preparation.writer;
        let work = remote
            .writer
            .window
            .terminal
            .owner
            .route
            .begin_direct_put_terminal_work(remote.writer.deadline, remote.cutoff);
        let (local, portable) = work.bounds();
        remote.writer.deadline = remote.writer.deadline.min(local);
        remote.cutoff.portable_wall_valid_until_ms = remote
            .cutoff
            .portable_wall_valid_until_ms
            .min(portable.portable_wall_valid_until_ms);
        let cutoff = remote.cutoff.portable_wall_valid_until_ms;
        let work_fence = crate::types::AdmittedRouteEffectFence::bind_portable(
            remote.writer.fence.cluster_epoch(),
            // This is a work cutoff, not a new authority lease. The portable
            // value already includes skew; do not subtract it again on export.
            u64::MAX,
            cutoff,
        );
        remote.writer.fence = remote
            .writer
            .fence
            .intersect(work_fence)
            .expect("terminal work fence uses the captured fence's epoch");
        Ok(AcknowledgedEncryptedDirectPayload {
            custody: AcknowledgedDirectStagingPayload {
                custody: Custody::Remote(self.preparation.writer),
                preparation_session: None,
            },
            encryption: self.encryption,
            crc64: self.stored_crc64,
            work,
            reinspect_prefix: false,
        })
    }
}

impl<'route> AcknowledgedEncryptedDirectPayload<'route> {
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_attempts(&self) -> usize {
        self.work.test_attempts()
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_limit_attempts(&mut self, limit: usize) {
        self.work.test_limit_attempts(limit);
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_terminal_bounds(&self) -> (Instant, u64) {
        let Custody::Remote(remote) = &self.custody.custody else {
            unreachable!()
        };
        (
            remote.writer.deadline,
            remote.cutoff.portable_wall_valid_until_ms,
        )
    }
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.custody.id()
    }

    pub(in crate::node_runtime) fn open_primary_session(
        &mut self,
        client: &UnixStorageNodeClient,
    ) -> Result<UnixStorageNodeMetadataCommandSession, Error> {
        self.work.require_deadline()?;
        self.custody.open_direct_primary_session(client)
    }

    pub(in crate::node_runtime) fn inspect_embedded(
        mut self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<InspectedEncryptedDirectPayload<'route>, StagingFileHandoffFailure<Self>> {
        if let Err(error) = self.work.begin_attempt() {
            return Err(StagingFileHandoffFailure::new(self, error.into()));
        }
        if self.reinspect_prefix {
            let refreshed = (|| {
                DirectStagingPreparation {
                    custody: &self.custody,
                }
                .require_section(section)?;
                let writer = match &mut self.custody.custody {
                    Custody::Embedded(writer) => writer,
                    Custody::Remote(remote) => &mut remote.writer,
                };
                writer.window.read.reinspect_direct_prefix_embedded(
                    section,
                    writer.window.terminal.owner.route.topology(),
                    writer.deadline,
                )
            })();
            if let Err(error) = refreshed {
                return Err(StagingFileHandoffFailure::new(self, error));
            }
            self.reinspect_prefix = false;
        }
        Self::finish_inspection(
            self.custody.prepare_direct_embedded(section),
            self.encryption,
            self.crc64,
            self.work,
        )
    }

    pub(in crate::node_runtime) fn inspect_remote(
        mut self,
        client: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
    ) -> Result<InspectedEncryptedDirectPayload<'route>, StagingFileHandoffFailure<Self>> {
        if self.reinspect_prefix {
            return Err(StagingFileHandoffFailure::new(
                self,
                Error::Window("post-contender reinspection requires the embedded primary".into()),
            ));
        }
        if let Err(error) = self.work.begin_attempt() {
            return Err(StagingFileHandoffFailure::new(self, error.into()));
        }
        Self::finish_inspection(
            self.custody.prepare_direct_remote(client, session),
            self.encryption,
            self.crc64,
            self.work,
        )
    }

    fn finish_inspection(
        result: Result<
            PreparedAcknowledgedDirectPayload<'route>,
            DirectStagingPreparationFailure<'route>,
        >,
        encryption: CommittedObjectEncryption,
        crc64: u64,
        work: DirectPutTerminalWork,
    ) -> Result<InspectedEncryptedDirectPayload<'route>, StagingFileHandoffFailure<Self>> {
        let (custody, error) = match result {
            Err(failure) => (*failure.custody, failure.error),
            Ok(prepared) => match prepared.with_integrity(crc64) {
                Ok(custody) => {
                    return Ok(InspectedEncryptedDirectPayload {
                        custody,
                        encryption,
                        work,
                    })
                }
                Err(failure) => (failure.prepared.custody, failure.error),
            },
        };
        Err(StagingFileHandoffFailure::new(
            Self {
                custody,
                encryption,
                crc64,
                work,
                reinspect_prefix: false,
            },
            error,
        ))
    }
}

impl<'route> InspectedEncryptedDirectPayload<'route> {
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_attempts(&self) -> usize {
        self.work.test_attempts()
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_terminal_bounds(&self) -> (Instant, u64) {
        let Custody::Remote(remote) = &self.custody.prepared.custody.custody else {
            unreachable!()
        };
        (
            remote.writer.deadline,
            remote.cutoff.portable_wall_valid_until_ms,
        )
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_cross_snapshot(&mut self) {
        self.custody.test_cross_snapshot();
    }
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.custody.id()
    }
    pub(in crate::node_runtime) fn auth_snapshot(&self) -> &crate::DirectPutCommitSnapshot {
        self.custody.auth_snapshot()
    }

    fn discard_snapshot(self) -> AcknowledgedEncryptedDirectPayload<'route> {
        AcknowledgedEncryptedDirectPayload {
            crc64: self.custody.manifest.crc64(),
            custody: self.custody.prepared.custody,
            encryption: self.encryption,
            work: self.work,
            reinspect_prefix: false,
        }
    }

    pub(in crate::node_runtime) fn reinspect_embedded(
        self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<Self, StagingFileHandoffFailure<AcknowledgedEncryptedDirectPayload<'route>>> {
        self.discard_snapshot().inspect_embedded(section)
    }

    pub(in crate::node_runtime) fn reinspect_remote(
        self,
        client: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
    ) -> Result<Self, StagingFileHandoffFailure<AcknowledgedEncryptedDirectPayload<'route>>> {
        self.discard_snapshot().inspect_remote(client, session)
    }

    pub(in crate::node_runtime) fn prepare_terminal_embedded(
        self,
        section: &EmbeddedPublicationSection<'_>,
        input: &crate::StreamPutCommitInput,
    ) -> Result<PreparedEncryptedDirectTerminal<'route>, StagingFileHandoffFailure<Self>> {
        if let Err(error) = self.work.require_deadline() {
            return Err(StagingFileHandoffFailure::new(self, error.into()));
        }
        Self::finish_terminal(
            self.custody
                .prepare_terminal_embedded(section, input, &self.encryption),
            self.encryption,
            self.work,
        )
    }

    pub(in crate::node_runtime) fn prepare_terminal_remote(
        self,
        client: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
        input: &crate::StreamPutCommitInput,
    ) -> Result<PreparedEncryptedDirectTerminal<'route>, StagingFileHandoffFailure<Self>> {
        if let Err(error) = self.work.require_deadline() {
            return Err(StagingFileHandoffFailure::new(self, error.into()));
        }
        Self::finish_terminal(
            self.custody
                .prepare_terminal_remote(client, session, input, &self.encryption),
            self.encryption,
            self.work,
        )
    }

    fn finish_terminal(
        result: Result<PreparedDirectTerminal<'route>, DirectTerminalPreparationFailure<'route>>,
        encryption: CommittedObjectEncryption,
        work: DirectPutTerminalWork,
    ) -> Result<PreparedEncryptedDirectTerminal<'route>, StagingFileHandoffFailure<Self>> {
        match result {
            Ok(prepared) => Ok(PreparedEncryptedDirectTerminal::new(
                prepared, encryption, work,
            )),
            Err(failure) => Err(StagingFileHandoffFailure::new(
                Self {
                    custody: *failure.custody,
                    encryption,
                    work,
                },
                failure.error,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn staging_encrypted_terminal_owners_are_noncloneable() {
        trait NotClone<A> {
            fn check() {}
        }
        impl<T: ?Sized> NotClone<()> for T {}
        impl<T: Clone> NotClone<u8> for T {}
        let _ = <DirectPutTerminalWork as NotClone<_>>::check;
        let _ = <AcknowledgedEncryptedDirectPayload<'_> as NotClone<_>>::check;
        let _ = <InspectedEncryptedDirectPayload<'_> as NotClone<_>>::check;
    }
}
