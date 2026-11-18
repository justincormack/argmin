// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! One captured, current-epoch, quiescent contender only. Each call performs at
//! most one effect; no scanning, sleeping, budget renewal or payload extraction.
use super::*;

#[must_use]
pub(in crate::node_runtime) struct DirectContenderPublication<'route> {
    payload: AcknowledgedEncryptedDirectPayload<'route>,
    sender: CompletePublicationSender,
}

#[must_use]
pub(in crate::node_runtime) struct DirectContenderTerminal<'route> {
    payload: AcknowledgedEncryptedDirectPayload<'route>,
    terminal: CompleteTerminalHandoff<'route>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::node_runtime) enum ContenderProgress {
    MarkerConfirmed,
    ReplicaApplied(NodeId),
    ReadyForTerminal,
}

impl ContenderProgress {
    pub(in crate::node_runtime) fn is_ready(self) -> bool {
        self == Self::ReadyForTerminal
    }
    pub(in crate::node_runtime) fn applied_node(self) -> Option<NodeId> {
        match self {
            Self::ReplicaApplied(node) => Some(node),
            _ => None,
        }
    }
}

impl AcknowledgedEncryptedDirectPayload<'_> {
    fn require_contender_section(
        &self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<(), Error> {
        self.work.require_deadline()?;
        DirectStagingPreparation {
            custody: &self.custody,
        }
        .require_section(section)?;
        section.require_effect()?;
        Ok(())
    }
}

impl<'route> DirectContenderInspection<'route> {
    pub(in crate::node_runtime) fn into_embedded_drain(
        mut self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<DirectContenderPublication<'route>, StagingFileHandoffFailure<Self>> {
        let checked = (|| {
            self.payload.require_contender_section(section)?;
            let Some((context, _)) = self.contender.pending.as_ref() else {
                return Err(Error::Window(
                    "direct contender drain requires a captured command".into(),
                ));
            };
            if !context.terminal_work().is_quiescent() {
                return Err(Error::Window(
                    "direct contender drain requires quiescent terminal work".into(),
                ));
            }
            Ok(())
        })();
        if let Err(error) = checked {
            return Err(StagingFileHandoffFailure::new(self, error));
        }
        let (local, portable) = self.payload.work.bounds();
        // Preserve both already-captured clocks, rather than deriving a new
        // portable cutoff from time remaining when the contender is selected.
        self.contender.publication_deadline =
            PublicationDeadline(std::sync::OnceLock::from(CapturedPublicationDeadline {
                local,
                portable,
            }));
        let primary = self
            .payload
            .custody
            .writer()
            .window
            .terminal
            .owner
            .route
            .primary();
        let sender = self.contender.into_publication_sender(primary, local);
        Ok(DirectContenderPublication {
            payload: self.payload,
            sender,
        })
    }
}

impl<'route> DirectContenderPublication<'route> {
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.sender.id().expect("captured contender")
    }
    pub(in crate::node_runtime) fn publication(&self) -> PublicationObservation {
        self.sender.observation()
    }
    pub(in crate::node_runtime) fn application(&self, node: NodeId) -> ApplicationObservation {
        self.sender.application_observation(node)
    }

    pub(in crate::node_runtime) fn advance_embedded(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<ContenderProgress, FanoutError> {
        self.advance_embedded_inner(
            section,
            #[cfg(test)]
            |_| Ok(()),
        )
    }

    fn advance_embedded_inner(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<ContenderProgress, FanoutError> {
        self.payload.require_contender_section(section)?;
        let route = &self.payload.custody.writer().window.terminal.owner.route;
        self.sender.require_cohort_subject(route)?;
        let deadline = self.payload.work.bounds().0;
        if self.sender.observation() != PublicationObservation::MarkerConfirmed {
            self.payload.work.begin_attempt()?;
            #[cfg(not(test))]
            self.sender.send_embedded(section, deadline)?;
            #[cfg(test)]
            self.sender
                .test_send_embedded_with_hook(section, deadline, hook)?;
            return Ok(ContenderProgress::MarkerConfirmed);
        }
        let unapplied =
            |node| self.sender.application_observation(node) != ApplicationObservation::Applied;
        let witness = route.nodes().filter(|node| *node != route.primary()).min();
        let next = witness
            .filter(|node| unapplied(*node))
            .or_else(|| unapplied(route.primary()).then_some(route.primary()))
            .or_else(|| route.nodes().filter(|node| unapplied(*node)).min());
        if let Some(node) = next {
            self.payload.work.begin_attempt()?;
            self.sender.apply_embedded_in_cohort(
                route,
                section,
                node,
                deadline,
                #[cfg(test)]
                hook,
            )?;
            return Ok(ContenderProgress::ReplicaApplied(node));
        }
        Ok(ContenderProgress::ReadyForTerminal)
    }

    pub(in crate::node_runtime) fn into_terminal(
        self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<DirectContenderTerminal<'route>, StagingFileHandoffFailure<Self>> {
        let checked = (|| {
            self.payload.require_contender_section(section)?;
            let route = &self.payload.custody.writer().window.terminal.owner.route;
            require_cohort_completion(route, &self.sender)
                .map_err(|e| Error::Window(e.to_string()))?;
            let (work, slot_work) = terminal_work_for_handoff(route, &self.sender)
                .map_err(|e| Error::Window(e.to_string()))?;
            Ok((route.retain_current()?, work, slot_work))
        })();
        let (route, work, slot_work) = match checked {
            Ok(checked) => checked,
            Err(error) => return Err(StagingFileHandoffFailure::new(self, error)),
        };
        Ok(DirectContenderTerminal {
            payload: self.payload,
            terminal: CompleteTerminalHandoff::from_confirmed_owner(
                CompleteFanoutSender {
                    route,
                    sender: self.sender,
                },
                work,
                slot_work,
            ),
        })
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_attempts(&self) -> usize {
        self.payload.work.test_attempts()
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_limit_attempts(&mut self, limit: usize) {
        self.payload.work.test_limit_attempts(limit);
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_advance_embedded_with_hook(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<ContenderProgress, FanoutError> {
        self.advance_embedded_inner(section, hook)
    }
}

impl<'route> DirectContenderTerminal<'route> {
    pub(in crate::node_runtime) fn retirement(&self) -> TerminalSlotRetirementObservation {
        self.terminal.slot_retirement_observation()
    }

    pub(in crate::node_runtime) fn retire_embedded(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<(), Error> {
        self.retire_embedded_inner(
            section,
            #[cfg(test)]
            |_| Ok(()),
        )
    }

    fn retire_embedded_inner(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), Error> {
        self.payload.require_contender_section(section)?;
        if self.retirement() == TerminalSlotRetirementObservation::Retired {
            return Ok(());
        }
        self.payload.work.begin_attempt()?;
        let deadline = self.payload.work.bounds().0;
        #[cfg(not(test))]
        let result = self.terminal.retire_embedded(section, deadline);
        #[cfg(test)]
        let result = self
            .terminal
            .test_retire_embedded_with_hook(section, deadline, hook);
        result.map_err(|e| Error::Window(e.to_string()))
    }

    /// Only confirmation of this exact slot ends the drain. The returned PUT
    /// must reinspect/revalidate before preparing; another contender is not
    /// silently selected here, and the original budget is never renewed.
    pub(in crate::node_runtime) fn into_payload(
        mut self,
    ) -> Result<AcknowledgedEncryptedDirectPayload<'route>, StagingFileHandoffFailure<Self>> {
        if self.retirement() != TerminalSlotRetirementObservation::Retired {
            return Err(StagingFileHandoffFailure::new(
                self,
                Error::Window("direct contender retirement is not confirmed".into()),
            ));
        }
        self.payload.reinspect_prefix = true;
        Ok(self.payload)
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_attempts(&self) -> usize {
        self.payload.work.test_attempts()
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_limit_attempts(&mut self, limit: usize) {
        self.payload.work.test_limit_attempts(limit);
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_retire_embedded_with_hook(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), Error> {
        self.retire_embedded_inner(section, hook)
    }
}
