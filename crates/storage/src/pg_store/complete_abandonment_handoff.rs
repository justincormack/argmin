// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Consuming all-member abandonment custody. Confirmation is not a source
//! release, cleanup result, follow-up installation or pending-slot retirement.
//! Keep the complete original holder private until those closed transitions
//! exist; no command, submission, callback or refreshed deadline escapes here.

use super::*;

#[path = "complete_abandonment_preparation.rs"]
mod preparation;
pub(in crate::node_runtime) use preparation::AbandonedPartFinalizerRelease;
pub(in crate::node_runtime) use preparation::{
    AbandonedPartInstallation, AbandonedPartInstallationObservation, AbandonedPartPreparation,
    AbandonedPartPreparationError, AbandonedPartPreparationFailure, AbandonedPartPublication,
    AbandonedPartTerminalFailure, AbandonedPartTerminalHandoff, PreparedAbandonedPart,
    PreparedAbandonedPartBorrow,
};

#[must_use = "retain abandoned command custody until the admitted follow-up protocol completes"]
pub(in crate::node_runtime) struct CompleteAbandonmentHandoff<'route> {
    owner: CompleteUnstartedAbandonment<'route>,
}

#[must_use = "failed handoff retains the original abandonment holder and every observation"]
pub(in crate::node_runtime) struct AbandonmentHandoffFailure<'route> {
    owner: Box<CompleteUnstartedAbandonment<'route>>,
    error: AbandonmentHandoffError,
}

impl<'route> CompleteUnstartedAbandonment<'route> {
    pub(in crate::node_runtime) fn into_abandoned(
        self,
    ) -> Result<CompleteAbandonmentHandoff<'route>, AbandonmentHandoffFailure<'route>> {
        let validate = || {
            self.owner.require_authority()?;
            let required = self.owner.route.nodes().count();
            let confirmed = self
                .owner
                .route
                .nodes()
                .filter(|node| {
                    if *node == self.owner.route.primary() {
                        self.observation
                            == UnstartedAbandonmentObservation::PrimaryTombstoneConfirmed
                    } else {
                        self.replica_observation(*node)
                            == ReplicaAbandonmentObservation::TombstoneConfirmed
                    }
                })
                .count();
            if confirmed != required {
                return Err(AbandonmentHandoffError::Incomplete {
                    confirmed,
                    required,
                });
            }
            Ok(())
        };
        if let Err(error) = validate() {
            return Err(AbandonmentHandoffFailure {
                owner: Box::new(self),
                error,
            });
        }
        // Move, never rebuild: preserve inspected/submitted origin, complete
        // source context, installation history, all confirmations and the first
        // captured deadline. The historical cohort is never replaced by today's.
        Ok(CompleteAbandonmentHandoff { owner: self })
    }
}

impl<'route> AbandonmentHandoffFailure<'route> {
    pub(in crate::node_runtime) fn error(&self) -> &AbandonmentHandoffError {
        &self.error
    }

    pub(in crate::node_runtime) fn into_abandonment(self) -> CompleteUnstartedAbandonment<'route> {
        *self.owner
    }
}

impl CompleteAbandonmentHandoff<'_> {
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.owner.owner.id()
    }

    /// Past observations, not a claim that cleanup or retirement has completed.
    pub(in crate::node_runtime) fn confirmed_peers(&self) -> usize {
        self.owner.owner.route.nodes().count()
    }

    pub(in crate::node_runtime) fn require_valid(&self) -> Result<(), StoreError> {
        self.owner.owner.require_authority()
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn installation_observation(
        &self,
    ) -> Option<ContextSubmissionObservation> {
        self.owner.installation_observation()
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn assert_context_for_test(
        &self,
        expected: &ContextualCommand,
        topology: &crate::PgTopology,
    ) {
        let request = self
            .owner
            .owner
            .sender
            .source
            .request(self.owner.owner.route.primary())
            .unwrap();
        let encoded = expected.encode_command().unwrap();
        assert_eq!(request.id(), expected.id());
        assert_eq!(request.command_sha256(), encoded.command_sha256());
        assert_eq!(request.checksum_crc64(), encoded.checksum_crc64());
        assert_eq!(request.topology(), topology);
    }
}

#[cfg(test)]
#[path = "complete_abandonment_handoff/api_tests.rs"]
mod api_tests;
