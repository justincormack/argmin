// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Candidate frontend binding. A prepared introduction cannot escape its
//! request's release decision until installation is positively confirmed.
use super::publication::DirectIntroductionPublication;
use super::*;
use crate::DirectPutWriteScope;
use std::cell::Cell;

#[path = "payload_staging_direct_introduction_request_remote.rs"]
mod remote;

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Unbound,
    NoAcceptance,
    Unconfirmed,
    Installed,
}

/// Request code may inspect the release decision, but only this concrete
/// introduction owner can claim it or change acceptance history.
pub(crate) struct DirectPutRequestCustody {
    state: Cell<State>,
}

impl Default for DirectPutRequestCustody {
    fn default() -> Self {
        Self {
            state: Cell::new(State::Unbound),
        }
    }
}

impl DirectPutRequestCustody {
    pub(crate) fn engaged(&self) -> bool {
        self.state.get() != State::Unbound
    }
    pub(crate) fn may_release(&self) -> bool {
        matches!(self.state.get(), State::Unbound | State::NoAcceptance)
    }
    fn claim(&self) -> Result<RequestBinding<'_>, StoreError> {
        if self.engaged() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "direct PUT request already owns an introduction",
            });
        }
        self.state.set(State::NoAcceptance);
        Ok(RequestBinding { custody: self })
    }
}

struct RequestBinding<'a> {
    custody: &'a DirectPutRequestCustody,
}
impl RequestBinding<'_> {
    fn installed(&self) -> bool {
        self.custody.state.get() == State::Installed
    }
    fn begin(&self) -> InstallationAttempt<'_> {
        let previous = self.custody.state.get();
        if previous != State::Installed {
            self.custody.state.set(State::Unconfirmed);
        }
        InstallationAttempt {
            custody: self.custody,
            previous,
        }
    }
}

// No Drop reset: panic/early exit conservatively retains the exact proof.
struct InstallationAttempt<'a> {
    custody: &'a DirectPutRequestCustody,
    previous: State,
}
impl InstallationAttempt<'_> {
    fn finish(self, observed: DirectIntroductionInstallation) {
        let state = match (self.previous, observed) {
            (State::Installed, _) | (_, DirectIntroductionInstallation::Installed) => {
                State::Installed
            }
            (State::Unconfirmed, _) | (_, DirectIntroductionInstallation::Unconfirmed) => {
                State::Unconfirmed
            }
            (_, DirectIntroductionInstallation::NoAcceptanceObserved) => State::NoAcceptance,
        };
        self.custody.state.set(state);
    }
}

#[must_use]
pub(in crate::node_runtime) struct RequestDirectIntroduction<'request, 'section, 'route> {
    captured: CapturedDirectIntroduction<'section, 'route>,
    binding: RequestBinding<'request>,
}

#[must_use]
pub(in crate::node_runtime) struct RequestDirectIntroductionFailure<'request, 'section, 'route> {
    owner: Box<RequestDirectIntroduction<'request, 'section, 'route>>,
    error: StagingWindowConfirmationError,
}

impl<'route> EmbeddedPublicationSection<'route> {
    pub(in crate::node_runtime) fn prepare_request_direct_introduction<'request, 'section>(
        &'section self,
        scope: &'request DirectPutWriteScope<'_, '_>,
        attempt: &SessionId,
    ) -> Result<RequestDirectIntroduction<'request, 'section, 'route>, StagingWindowConfirmationError>
    {
        let (bucket, key, proof, custody) = scope.introduction_subject(self)?;
        let captured = self.prepare_direct_introduction(bucket, key, attempt, proof)?;
        // Preparation is read-only. Claim only after successful capture; a
        // preparation rejection leaves ordinary frontend release responsibility.
        let binding = custody.claim()?;
        Ok(RequestDirectIntroduction { captured, binding })
    }
}

impl<'request, 'section, 'route> RequestDirectIntroduction<'request, 'section, 'route> {
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.captured.id()
    }
    pub(in crate::node_runtime) fn installation(&self) -> DirectIntroductionInstallation {
        self.captured.installation()
    }
    pub(in crate::node_runtime) fn install(
        &mut self,
    ) -> Result<(), StagingWindowConfirmationError> {
        // Arm before the first potentially mutating call, including unwinding.
        let attempt = self.binding.begin();
        let result = self.captured.install();
        attempt.finish(self.captured.installation());
        result
    }
    pub(in crate::node_runtime) fn into_publication(
        self,
    ) -> Result<
        DirectIntroductionPublication<'section, 'route>,
        RequestDirectIntroductionFailure<'request, 'section, 'route>,
    > {
        if !self.binding.installed() {
            return Err(RequestDirectIntroductionFailure {
                owner: Box::new(self),
                error: StagingWindowConfirmationError::Window(
                    "request introduction requires confirmed installation".into(),
                ),
            });
        }
        match self.captured.into_publication() {
            Ok(publication) => Ok(publication),
            Err(failure) => {
                let error = StagingWindowConfirmationError::Window(failure.error().to_string());
                Err(RequestDirectIntroductionFailure {
                    owner: Box::new(Self {
                        captured: failure.into_introduction(),
                        binding: self.binding,
                    }),
                    error,
                })
            }
        }
    }
}

impl<'request, 'section, 'route> RequestDirectIntroductionFailure<'request, 'section, 'route> {
    pub(in crate::node_runtime) fn error(&self) -> &StagingWindowConfirmationError {
        &self.error
    }
    pub(in crate::node_runtime) fn into_introduction(
        self,
    ) -> RequestDirectIntroduction<'request, 'section, 'route> {
        *self.owner
    }
}

#[cfg(test)]
mod api {
    use super::*;
    #[test]
    fn staging_direct_introduction_request_unfinished_attempt_cannot_reset_custody() {
        let custody = DirectPutRequestCustody::default();
        let binding = custody.claim().unwrap();
        assert!(custody.may_release());
        assert!(custody.claim().is_err());
        // An unwind can happen before the inner installer updates its own
        // history. A later preflight rejection still cannot prove that attempt absent.
        let _unfinished = binding.begin();
        assert!(!custody.may_release());
        binding
            .begin()
            .finish(DirectIntroductionInstallation::NoAcceptanceObserved);
        assert!(!custody.may_release());
        binding
            .begin()
            .finish(DirectIntroductionInstallation::Installed);
        assert!(binding.installed());
        binding
            .begin()
            .finish(DirectIntroductionInstallation::Unconfirmed);
        assert!(binding.installed());
        assert!(!custody.may_release());
    }
    #[test]
    fn staging_direct_introduction_request_owners_are_noncloneable() {
        trait NotClone<A> {
            fn check() {}
        }
        impl<T: ?Sized> NotClone<()> for T {}
        impl<T: Clone> NotClone<u8> for T {}
        let _ = <RequestDirectIntroduction<'_, '_, '_> as NotClone<_>>::check;
        let _ = <RequestDirectIntroductionFailure<'_, '_, '_> as NotClone<_>>::check;
        let _ = <RequestBinding<'_> as NotClone<_>>::check;
        let _ = <DirectPutRequestCustody as NotClone<_>>::check;
    }
}
