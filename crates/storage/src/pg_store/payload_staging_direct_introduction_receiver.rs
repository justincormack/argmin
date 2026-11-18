// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Exact decoded preparation plus non-resettable installation history. The
//! consumed input keeps actual server/session admission alive, not just scalars.
use super::*;
use crate::node_runtime::pg_store::PgStore;
use crate::node_runtime::server::{
    RemoteIntroductionInstallationAuthority, RemoteIntroductionInstallationInput,
};

#[must_use]
pub(in crate::node_runtime) struct RemoteIntroductionInstallation<'a> {
    input: RemoteIntroductionInstallationInput<'a>,
    state: RemoteIntroductionInstallationState,
}

pub(in crate::node_runtime) struct RemoteIntroductionInstallationState {
    submission: submission::ContextualSubmission,
    request: [u8; 32],
    response: [u8; 32],
    command: [u8; 32],
}

#[must_use]
pub(in crate::node_runtime) struct RemoteIntroductionParkingFailure<'a> {
    owner: RemoteIntroductionInstallation<'a>,
    error: StoreError,
}

impl<'a> RemoteIntroductionParkingFailure<'a> {
    pub(in crate::node_runtime) fn error(&self) -> &StoreError {
        &self.error
    }
    pub(in crate::node_runtime) fn into_owner(self) -> RemoteIntroductionInstallation<'a> {
        self.owner
    }
}

impl<'a> RemoteIntroductionInstallation<'a> {
    /// Move the complete state into its original held connection section. This
    /// is inert custody transfer, not new admission or an installation attempt.
    pub(in crate::node_runtime) fn park(
        self,
    ) -> Result<(), Box<RemoteIntroductionParkingFailure<'a>>> {
        let Self { input, state } = self;
        input.park(state).map_err(|failure| {
            let (state, error) = *failure;
            Box::new(RemoteIntroductionParkingFailure {
                owner: Self { input, state },
                error,
            })
        })
    }
    #[cfg(test)]
    pub(in crate::node_runtime) fn test_bounds(&self) -> (Instant, u64) {
        self.input.test_bounds()
    }
    pub(in crate::node_runtime) fn from_input(
        input: RemoteIntroductionInstallationInput<'a>,
    ) -> Result<Self, StagingWindowConfirmationError> {
        let invalid = |e: String| StagingWindowConfirmationError::Window(e);
        let request = input.request().map_err(|e| invalid(e.to_string()))?;
        let response = request
            .decode_response(input.response())
            .map_err(|e| invalid(e.to_string()))?;
        let context = ContextualCommand::decode_command(
            response.command.command,
            response.command.checksum,
            input.topology(),
        )
        .map_err(|e| invalid(e.to_string()))?;
        let encoded = context
            .encode_command()
            .map_err(|e| invalid(e.to_string()))?;
        if encoded.bytes() != response.command.command
            || encoded.command_sha256() != response.command_sha256
        {
            return Err(invalid(
                "direct introduction noncanonical prepared command".into(),
            ));
        }
        let command = encoded.command_sha256();
        drop(encoded);
        let state = RemoteIntroductionInstallationState {
            submission: context.into_submission(),
            request: request.digest(),
            response: crate::storage_rpc::complete_stream_allocation::digest(input.response()),
            command,
        };
        Ok(Self { input, state })
    }

    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.state.id()
    }
    pub(in crate::node_runtime) fn installation(&self) -> DirectIntroductionInstallation {
        self.state.installation()
    }
    pub(in crate::node_runtime) fn install(
        &mut self,
    ) -> Result<(), StagingWindowConfirmationError> {
        self.input.install(&mut self.state)
    }
}

impl RemoteIntroductionInstallationState {
    pub(in crate::node_runtime) fn require_parking_input(
        &self,
        input: &RemoteIntroductionInstallationInput<'_>,
    ) -> Result<(), StoreError> {
        input.require_state(self.id(), self.request, self.response, self.command)
    }
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.submission.observation().command_id
    }
    pub(in crate::node_runtime) fn installation(&self) -> DirectIntroductionInstallation {
        match self.submission.observation().installation {
            submission::InstallationObservation::NoAcceptanceObserved => {
                DirectIntroductionInstallation::NoAcceptanceObserved
            }
            submission::InstallationObservation::Unconfirmed => {
                DirectIntroductionInstallation::Unconfirmed
            }
            submission::InstallationObservation::Installed => {
                DirectIntroductionInstallation::Installed
            }
        }
    }
    pub(in crate::node_runtime) fn install_in_store(
        &mut self,
        authority: &RemoteIntroductionInstallationAuthority<'_, '_>,
        pg: &PgStore,
    ) -> Result<(), StagingWindowConfirmationError> {
        authority.require_command(
            pg,
            self.submission.observation().command_id,
            self.request,
            self.response,
            self.command,
        )?;
        if self.submission.observation().installation
            == submission::InstallationObservation::Installed
        {
            return Ok(());
        }
        self.submission
            .install(
                pg,
                authority.node().as_u32(),
                authority.topology(),
                authority.fence(),
                Some(authority.deadline()),
            )
            .map_err(|e| StagingWindowConfirmationError::Window(e.to_string()))
    }
}
