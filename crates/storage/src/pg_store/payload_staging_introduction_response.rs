// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Closed response encoder, not a general submission serialization escape.
use super::{ContextualSubmission, InstallationObservation};
use crate::node_runtime::pg_store::{
    CapturedDirectIntroductionCore, StagingWindowConfirmationError,
};
use crate::node_runtime::server::DirectIntroductionResponseScope;
use crate::storage_rpc::{complete_applied, complete_direct_introduction};

impl ContextualSubmission {
    pub(in crate::node_runtime::pg_store) fn encode_introduction_response(
        &self,
        core: &CapturedDirectIntroductionCore,
        scope: &DirectIntroductionResponseScope<'_, '_>,
    ) -> Result<Vec<u8>, StagingWindowConfirmationError> {
        let invalid = |error: String| StagingWindowConfirmationError::Window(error);
        scope.require_capture(core)?;
        let cutoff = core.require_response_submission(self)?;
        if self.installation != InstallationObservation::NoAcceptanceObserved {
            return Err(invalid("introduction response already submitted".into()));
        }
        let request = scope.request().map_err(|e| invalid(e.to_string()))?;
        let encoded = self
            .context
            .encode_command()
            .map_err(|e| invalid(e.to_string()))?;
        let id = encoded.id();
        let bytes = complete_direct_introduction::encode_response(
            &request,
            complete_applied::BorrowedRequest {
                node_id: request.route().node,
                epoch: id.cluster_epoch(),
                pg_id: id.pg_id(),
                checksum: encoded.checksum_crc64(),
                command: encoded.bytes(),
            },
            cutoff,
        )
        .map_err(|e| invalid(e.to_string()))?;
        scope.require_capture(core)?;
        Ok(bytes)
    }
}
