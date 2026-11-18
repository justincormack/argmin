// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Consuming bridge to the shared file pipeline. No caller-selected session,
//! route, command or renewed deadline can substitute for introduction custody.
use super::*;
use crate::cluster::CompleteFanoutRoute;
use crate::node_runtime::pg_store::{DirectStagingFileFailure, RemoteDirectStagingFileWriter};

pub(in crate::node_runtime::pg_store) struct RemoteIntroductionFileScope<'route> {
    window: RemoteDirectIntroductionWindow<'route>,
    route: CompleteFanoutRoute<'route>,
}

#[must_use = "failed binding retains complete cleanup custody"]
pub(in crate::node_runtime) struct RemoteDirectIntroductionFileFailure<'route> {
    custody: FailureCustody<'route>,
}

enum FailureCustody<'route> {
    Introduction(
        Box<RemoteDirectIntroductionWindow<'route>>,
        StagingWindowConfirmationError,
    ),
    Files(Box<DirectStagingFileFailure<'route>>),
}

impl<'route> RemoteIntroductionFileScope<'route> {
    pub(in crate::node_runtime::pg_store) fn into_parts(
        self,
    ) -> (
        CompleteFanoutRoute<'route>,
        CompletePublicationSender,
        CapturedWindowRead,
        &'route UnixStorageNodeClient,
        &'route UnixStorageNodeMetadataCommandSession,
    ) {
        let owner = self.window.terminal.owner;
        (
            self.route,
            owner.sender,
            self.window.read,
            owner.binding.client,
            owner.binding.session,
        )
    }
}

impl<'route> RemoteDirectIntroductionWindow<'route> {
    pub(in crate::node_runtime) fn into_file_dispatch(
        self,
        provisional: &crate::ObjectEncryption,
    ) -> Result<RemoteDirectStagingFileWriter<'route>, RemoteDirectIntroductionFileFailure<'route>>
    {
        let checked = (|| {
            self.require_valid()?;
            // Validate before releasing the original session. A rejected
            // encryption binding must not perform even the release effect.
            self.read.direct_encryption_subject(
                self.terminal.owner.binding.route.topology(),
                provisional,
            )?;
            Ok(self.terminal.owner.binding.route.retain_current()?)
        })();
        let route = match checked {
            Ok(route) => route,
            Err(error) => {
                return Err(RemoteDirectIntroductionFileFailure {
                    custody: FailureCustody::Introduction(Box::new(self), error),
                })
            }
        };
        CompletePublicationSender::bind_remote_introduction_files(
            RemoteIntroductionFileScope {
                window: self,
                route,
            },
            provisional,
        )
        .map_err(|failure| RemoteDirectIntroductionFileFailure {
            custody: FailureCustody::Files(Box::new(failure)),
        })
    }
}

impl RemoteDirectIntroductionFileFailure<'_> {
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        match &self.custody {
            FailureCustody::Introduction(window, _) => window.id(),
            FailureCustody::Files(failure) => failure.id(),
        }
    }
    pub(in crate::node_runtime) fn error(&self) -> &StagingWindowConfirmationError {
        match &self.custody {
            FailureCustody::Introduction(_, error) => error,
            FailureCustody::Files(failure) => failure.error(),
        }
    }
}

#[cfg(test)]
mod api {
    use super::*;
    #[test]
    fn staging_direct_introduction_sender_files_custody_is_noncloneable() {
        trait NotClone<A> {
            fn check() {}
        }
        impl<T: ?Sized> NotClone<()> for T {}
        impl<T: Clone> NotClone<u8> for T {}
        let _ = <RemoteIntroductionFileScope<'_> as NotClone<_>>::check;
        let _ = <RemoteDirectIntroductionFileFailure<'_> as NotClone<_>>::check;
        let _ = <RemoteDirectStagingFileWriter<'_> as NotClone<_>>::check;
    }
}
