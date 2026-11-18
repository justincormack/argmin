// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Request encryption stays attached to original introduction/file custody.
//! This consuming handoff has no IO and cannot extract a bare physical writer.
use super::*;
use crate::{CommittedObjectEncryption, ObjectEncryptionSubject, SegmentEncryptionSubject};

#[path = "encrypted_terminal.rs"]
mod terminal;

#[must_use]
pub(in crate::node_runtime) struct DirectStagingEncryptionPreparation<'route> {
    writer: RemoteDirectStagingFileWriter<'route>,
    subject: SegmentEncryptionSubject,
    stored_size: u64,
}

#[must_use]
pub(in crate::node_runtime) struct EncryptedDirectStagingPayload<'route> {
    preparation: DirectStagingEncryptionPreparation<'route>,
    ciphertext: Vec<u8>,
    encryption: CommittedObjectEncryption,
    stored_crc64: u64,
}

impl<'route> RemoteDirectStagingFileWriter<'route> {
    pub(in crate::node_runtime) fn prepare_encryption(
        self,
        logical_size: u64,
    ) -> Result<DirectStagingEncryptionPreparation<'route>, StagingFileHandoffFailure<Self>> {
        let checked = (|| {
            self.writer.window.require_valid()?;
            self.writer
                .fence
                .require_valid_for(self.id().cluster_epoch())?;
            let size = self.writer.window.read.direct_ciphertext_size(
                self.writer.window.terminal.owner.route.topology(),
                logical_size,
            )?;
            if !self.writer.acknowledgements.is_unattempted() || !self.attempts.is_empty() {
                return Err(Error::Window(
                    "direct encryption must precede file attempts".into(),
                ));
            }
            let object = self.writer.window.read.direct_encryption_subject(
                self.writer.window.terminal.owner.route.topology(),
                &self.writer.provisional,
            )?;
            Ok((
                size,
                SegmentEncryptionSubject::standard_object(object, 0, logical_size),
            ))
        })();
        match checked {
            Ok((stored_size, subject)) => Ok(DirectStagingEncryptionPreparation {
                writer: self,
                subject,
                stored_size,
            }),
            Err(error) => Err(StagingFileHandoffFailure::new(self, error)),
        }
    }
}

impl<'route> DirectStagingEncryptionPreparation<'route> {
    pub(in crate::node_runtime) fn encryption_subject(&self) -> &SegmentEncryptionSubject {
        &self.subject
    }
    pub(in crate::node_runtime) fn object_encryption_subject(&self) -> &ObjectEncryptionSubject {
        self.subject.object_subject()
    }
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.writer.id()
    }

    /// Accept crypto-owner output only for this exact subject and captured size.
    /// A failed binding returns the same preparation, not an independent writer
    /// or new installation history. No storage crypto key is needed here.
    pub(in crate::node_runtime) fn bind_ciphertext(
        self,
        ciphertext: Vec<u8>,
        encryption: CommittedObjectEncryption,
    ) -> Result<EncryptedDirectStagingPayload<'route>, StagingFileHandoffFailure<Self>> {
        let checked = (|| {
            self.writer.writer.window.require_valid()?;
            self.writer
                .writer
                .fence
                .require_valid_for(self.id().cluster_epoch())?;
            if !encryption.matches(self.object_encryption_subject(), encryption.encryption()) {
                return Err(Error::Window(
                    "direct ciphertext committed subject mismatch".into(),
                ));
            }
            if ciphertext.len() as u64 != self.stored_size {
                return Err(Error::Window("direct ciphertext length mismatch".into()));
            }
            Ok(())
        })();
        if let Err(error) = checked {
            return Err(StagingFileHandoffFailure::new(self, error));
        }
        let stored_crc64 = checksum::crc64::checksum(&ciphertext);
        Ok(EncryptedDirectStagingPayload {
            preparation: self,
            ciphertext,
            encryption,
            stored_crc64,
        })
    }
}

impl EncryptedDirectStagingPayload<'_> {
    pub(in crate::node_runtime::pg_store) fn test_effect_fence(
        &self,
    ) -> crate::types::AdmittedRouteEffectFence {
        self.preparation.writer.writer.fence
    }

    /// Encode with the shared placement encoder. Transport choices are checked
    /// against captured placement; callers cannot replace bytes, size or epoch.
    /// On any error this same owner retains every preceding/uncertain attempt.
    pub(in crate::node_runtime) fn write_files(
        &mut self,
        remote: &[Option<&UnixStorageNodeClient>],
    ) -> Result<(), Error> {
        let writer = &mut self.preparation.writer;
        writer.writer.window.require_valid()?;
        writer
            .writer
            .fence
            .require_valid_for(writer.id().cluster_epoch())?;
        let route = &writer.writer.window.terminal.owner.route;
        let logical_size = self.preparation.subject.logical_plaintext_len();
        let (hash, generation, ec) = writer
            .writer
            .window
            .read
            .direct_encoding_geometry(route.topology(), logical_size)?;
        if remote.len() != usize::from(ec.k) + usize::from(ec.m) {
            return Err(Error::Window(
                "direct encrypted dispatch participant count".into(),
            ));
        }
        let encoder = route.staging_payload_encoder()?;
        encoder.encode(&hash, generation, &self.ciphertext, ec, |batch| {
            // Validate the complete dispatch before the first file effect.
            for (index, ((_, bytes), client)) in batch.iter().zip(remote).enumerate() {
                let route = &writer.writer.window.terminal.owner.route;
                let (placement, _) = route.staging_file_placement()?;
                let (location, _) = writer.writer.window.read.direct_file_location(
                    route.topology(),
                    placement,
                    index as u8,
                    logical_size,
                    bytes.len(),
                )?;
                match client {
                    Some(client) => route.require_staging_file_client(&location, client)?,
                    None => {
                        route.embedded_staging_file_runtime(&location)?;
                    }
                }
            }
            let mut written = Vec::with_capacity(batch.len());
            for (index, ((key, bytes), client)) in batch.iter().zip(remote).enumerate() {
                let ack = match client {
                    Some(client) => {
                        writer.write_remote(client, index as u8, logical_size, bytes)?
                    }
                    None => writer
                        .writer
                        .write_embedded(index as u8, logical_size, bytes)?,
                };
                written.push((key.clone(), ack));
            }
            Ok(written)
        })
    }

    pub(in crate::node_runtime) fn persist_acknowledgements(
        &mut self,
        remote: Option<&UnixStorageNodeClient>,
    ) -> Result<(), Error> {
        match remote {
            Some(client) => self
                .preparation
                .writer
                .persist_acknowledgements_remote(client),
            None => self
                .preparation
                .writer
                .writer
                .persist_acknowledgements_embedded(),
        }
    }
}

// No production accessor may detach the writer, bytes or evidence. Terminal
// preparation will consume this whole owner after acknowledgement confirmation.
#[cfg(test)]
impl EncryptedDirectStagingPayload<'_> {
    pub(in crate::node_runtime::pg_store) fn test_observe(
        &self,
    ) -> (MetadataCommandId, &[u8], &crate::ObjectEncryption, u64) {
        (
            self.preparation.id(),
            &self.ciphertext,
            self.encryption.encryption(),
            self.stored_crc64,
        )
    }
}

#[cfg(test)]
mod api {
    use super::*;
    #[test]
    fn staging_direct_request_encryption_owners_are_noncloneable() {
        trait NotClone<A> {
            fn check() {}
        }
        impl<T: ?Sized> NotClone<()> for T {}
        impl<T: Clone> NotClone<u8> for T {}
        let _ = <DirectStagingEncryptionPreparation<'_> as NotClone<_>>::check;
        let _ = <EncryptedDirectStagingPayload<'_> as NotClone<_>>::check;
        let _ = <CommittedObjectEncryption as NotClone<_>>::check;
    }
}
