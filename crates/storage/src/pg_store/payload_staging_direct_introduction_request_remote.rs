// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Bind the existing exact-session sender to the frontend's proof-release
//! decision. Neither a raw preparation nor an uncertain sender can escape it.
use super::super::transport::{
    RemoteDirectIntroductionPublication, RemoteDirectIntroductionSubmission,
};
use super::*;
use crate::cluster::CompleteFanoutRoute;
use crate::node_client::{UnixStorageNodeClient, UnixStorageNodeMetadataCommandSession};

#[must_use]
pub(in crate::node_runtime) struct RequestRemoteDirectIntroduction<'request, 'route> {
    submission: RemoteDirectIntroductionSubmission<'route>,
    binding: RequestBinding<'request>,
}

#[must_use]
pub(in crate::node_runtime) struct RequestRemoteDirectIntroductionFailure<'request, 'route> {
    owner: Box<RequestRemoteDirectIntroduction<'request, 'route>>,
    error: StagingWindowConfirmationError,
}

impl<'route> CompleteFanoutRoute<'route> {
    pub(in crate::node_runtime) fn prepare_request_direct_introduction_remote<'request>(
        &'route self,
        scope: &'request DirectPutWriteScope<'_, '_>,
        client: &'route UnixStorageNodeClient,
        session: &'route UnixStorageNodeMetadataCommandSession,
        attempt: &SessionId,
        deadline: Instant,
    ) -> Result<RequestRemoteDirectIntroduction<'request, 'route>, StagingWindowConfirmationError>
    {
        let (bucket, key, proof, custody) = scope.introduction_subject_remote(self)?;
        let prepared = self.prepare_direct_introduction_remote(
            client,
            session,
            (bucket, key, attempt, proof),
            deadline,
        )?;
        // Descriptive preparation has no durable installation effect. Only a
        // validated response can claim the scope; failed preparation still releases.
        let binding = custody.claim()?;
        Ok(RequestRemoteDirectIntroduction {
            submission: prepared.into_submission(),
            binding,
        })
    }
}

impl<'request, 'route> RequestRemoteDirectIntroduction<'request, 'route> {
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.submission.id()
    }
    pub(in crate::node_runtime) fn installation(&self) -> DirectIntroductionInstallation {
        self.submission.installation()
    }
    pub(in crate::node_runtime) fn install(
        &mut self,
    ) -> Result<(), StagingWindowConfirmationError> {
        let attempt = self.binding.begin();
        let result = self.submission.install();
        attempt.finish(self.submission.installation());
        result
    }
    pub(in crate::node_runtime) fn into_publication(
        self,
    ) -> Result<
        RemoteDirectIntroductionPublication<'route>,
        RequestRemoteDirectIntroductionFailure<'request, 'route>,
    > {
        if !self.binding.installed() {
            return Err(RequestRemoteDirectIntroductionFailure {
                owner: Box::new(self),
                error: StagingWindowConfirmationError::Window(
                    "request introduction requires confirmed installation".into(),
                ),
            });
        }
        match self.submission.into_publication() {
            Ok(publication) => Ok(publication),
            Err(failure) => {
                let error = StagingWindowConfirmationError::Window(failure.error().to_string());
                Err(RequestRemoteDirectIntroductionFailure {
                    owner: Box::new(Self {
                        submission: failure.into_submission(),
                        binding: self.binding,
                    }),
                    error,
                })
            }
        }
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_bounds(&self) -> (Instant, u64) {
        self.submission.test_bounds()
    }
}

impl<'request, 'route> RequestRemoteDirectIntroductionFailure<'request, 'route> {
    pub(in crate::node_runtime) fn error(&self) -> &StagingWindowConfirmationError {
        &self.error
    }
    pub(in crate::node_runtime) fn into_introduction(
        self,
    ) -> RequestRemoteDirectIntroduction<'request, 'route> {
        *self.owner
    }
}

#[cfg(test)]
mod api {
    use super::*;
    #[test]
    fn staging_direct_introduction_request_remote_owners_are_noncloneable() {
        trait NotClone<A> {
            fn check() {}
        }
        impl<T: ?Sized> NotClone<()> for T {}
        impl<T: Clone> NotClone<u8> for T {}
        let _ = <RequestRemoteDirectIntroduction<'_, '_> as NotClone<_>>::check;
        let _ = <RequestRemoteDirectIntroductionFailure<'_, '_> as NotClone<_>>::check;
    }
}
