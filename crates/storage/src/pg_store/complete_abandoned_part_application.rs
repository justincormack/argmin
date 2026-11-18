// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Apply the published cleanup while retaining both commands, the original
//! cohort, immutable deadline and all installation/publication observations.
//! Per-member confirmation is not a terminal handoff or cleanup capability.

use super::*;
use crate::node_runtime::clients::UnixStorageNodeClient;
use crate::node_runtime::pg_store::CompleteApplicationRequest;

impl PreparedAbandonedPart<'_> {
    pub(in crate::node_runtime) fn cleanup_application(
        &self,
        node: NodeId,
    ) -> ApplicationObservation {
        self.applications
            .get(&node)
            .copied()
            .unwrap_or(ApplicationObservation::NoAttemptObserved)
    }

    pub(in crate::node_runtime) fn apply_cleanup_embedded(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        node: NodeId,
    ) -> Result<MetadataCommandReplicaState, FanoutError> {
        self.apply_cleanup_embedded_inner(
            section,
            node,
            #[cfg(test)]
            |_| Ok(()),
        )
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_apply_cleanup_embedded(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        node: NodeId,
        hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<MetadataCommandReplicaState, FanoutError> {
        self.apply_cleanup_embedded_inner(section, node, hook)
    }

    fn apply_cleanup_embedded_inner(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        node: NodeId,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<MetadataCommandReplicaState, FanoutError> {
        let permit = AbandonedPartPublication {
            source: &self.owner,
            candidate: &self.candidate,
            installation: &self.installation,
        };
        permit.require_embedded_section(section)?;
        if self.publication != PublicationObservation::MarkerConfirmed {
            return Err(ApplicationAttemptError::PublicationNotConfirmed.into());
        }
        // Both target and ordering come from the original admitted cohort. An
        // equal PG/epoch in another runtime or a caller-selected target cannot
        // substitute for the section held during preparation/publication.
        let target = section.application_target(node)?;
        let sender = &self.owner.owner.owner;
        self.require_cleanup_application_order(node)?;
        let request = CompletePublicationRequest::abandoned_part_member_request(&permit, node)?;
        let transport = request.prepare_transport(sender.work_deadline());
        let application = CompleteApplicationRequest::from_publication_transport(&transport);
        let observation = self
            .applications
            .entry(node)
            .or_insert(ApplicationObservation::NoAttemptObserved);
        // Before entering the closed operation, including mutex waits and
        // unwind. A failed exact retry cannot erase an earlier confirmation.
        if *observation != ApplicationObservation::Applied {
            *observation = ApplicationObservation::Unconfirmed;
        }
        #[cfg(not(test))]
        let result = target.apply_complete_command(&application);
        #[cfg(test)]
        let result = target.test_with_application_authority(&application, |pg, authority| {
            pg.test_apply_embedded_complete_command_with_hook(authority, hook)
        });
        let state = result.map_err(ApplicationAttemptError::Embedded)?;
        *observation = ApplicationObservation::Applied;
        Ok(state)
    }

    fn require_cleanup_application_order(&self, node: NodeId) -> Result<(), FanoutError> {
        let sender = &self.owner.owner.owner;
        let primary = sender.route.primary();
        let witness = sender.witness();
        if node == primary {
            if witness.is_some_and(|peer| {
                self.cleanup_application(peer) != ApplicationObservation::Applied
            }) {
                return Err(FanoutError::WitnessNotConfirmed);
            }
        } else if Some(node) != witness
            && self.cleanup_application(primary) != ApplicationObservation::Applied
        {
            return Err(FanoutError::PrimaryNotConfirmed);
        }
        Ok(())
    }

    pub(in crate::node_runtime) fn apply_cleanup_remote(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        replica: &UnixStorageNodeClient,
    ) -> Result<MetadataCommandReplicaState, FanoutError> {
        self.with_cleanup_remote_transport(section, replica, |transport, route, observation| {
            transport.send_application_in_fanout(route, replica, observation)
        })
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_cleanup_remote_attempt(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        replica: &UnixStorageNodeClient,
        action: impl FnOnce(
            crate::node_runtime::pg_store::CompleteApplicationAttempt<'_>,
        )
            -> Result<MetadataCommandReplicaState, CompleteApplicationSendError>,
    ) -> Result<MetadataCommandReplicaState, FanoutError> {
        self.with_cleanup_remote_transport(section, replica, |transport, _, observation| {
            transport.test_application_attempt(observation, action)
        })
    }

    // Private and synchronous: the primary section, original pair and exclusive
    // history borrow remain live through the exact authenticated reply. There
    // is no public callback, detachable attempt or caller-selected cutoff.
    fn with_cleanup_remote_transport(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        replica: &UnixStorageNodeClient,
        send: impl FnOnce(
            &CompletePublicationTransport<'_>,
            &crate::cluster::CompleteFanoutRoute<'_>,
            &mut ApplicationObservation,
        ) -> Result<MetadataCommandReplicaState, CompleteApplicationSendError>,
    ) -> Result<MetadataCommandReplicaState, FanoutError> {
        let permit = AbandonedPartPublication {
            source: &self.owner,
            candidate: &self.candidate,
            installation: &self.installation,
        };
        permit.require_embedded_section(section)?;
        if self.publication != PublicationObservation::MarkerConfirmed {
            return Err(ApplicationAttemptError::PublicationNotConfirmed.into());
        }
        let sender = &self.owner.owner.owner;
        sender.route.require_client(replica)?;
        let node = replica.node_id();
        if node == sender.route.primary() {
            return Err(mismatch("abandoned part remote application requires a replica").into());
        }
        self.require_cleanup_application_order(node)?;
        let request = CompletePublicationRequest::abandoned_part_member_request(&permit, node)?;
        let transport = request.prepare_transport(sender.work_deadline());
        let observation = self
            .applications
            .entry(node)
            .or_insert(ApplicationObservation::NoAttemptObserved);
        Ok(
            send(&transport, &sender.route, observation)
                .map_err(ApplicationAttemptError::Remote)?,
        )
    }
}
