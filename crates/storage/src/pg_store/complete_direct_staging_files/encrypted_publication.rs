// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Closed effect delegation: no Deref or extraction of the unbudgeted owner.
//! Errors and consuming handoffs retain both custody and the original budget.
use super::*;
use crate::node_runtime::pg_store::CompleteRetirementSendError;

#[path = "encrypted_progress.rs"]
mod progress;

#[must_use]
pub(in crate::node_runtime) struct BudgetedDirectOwner<T> {
    owner: T,
    budget: DirectPutTerminalWork,
}

impl<T> BudgetedDirectOwner<T> {
    pub(super) fn new(owner: T, budget: DirectPutTerminalWork) -> Self {
        Self { owner, budget }
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_attempts(&self) -> usize {
        self.budget.test_attempts()
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_limit_attempts(&mut self, limit: usize) {
        self.budget.test_limit_attempts(limit);
    }
}

#[must_use]
pub(in crate::node_runtime) struct BudgetedDirectHandoffFailure<'route> {
    failure: Box<(DirectPutTerminalFailure<'route>, DirectPutTerminalWork)>,
}

impl<'route> BudgetedDirectHandoffFailure<'route> {
    pub(in crate::node_runtime) fn error(&self) -> &FanoutError {
        self.failure.0.error()
    }
    pub(in crate::node_runtime) fn into_publication(
        self,
    ) -> BudgetedDirectOwner<DirectTerminalPublication<'route>> {
        let (failure, budget) = *self.failure;
        BudgetedDirectOwner {
            owner: failure.into_publication(),
            budget,
        }
    }
}

impl<'route> BudgetedDirectOwner<DirectTerminalPublication<'route>> {
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.owner.id()
    }
    pub(in crate::node_runtime) fn publication(&self) -> PublicationObservation {
        self.owner.publication()
    }
    pub(in crate::node_runtime) fn application(&self, node: NodeId) -> ApplicationObservation {
        self.owner.application(node)
    }
    pub(in crate::node_runtime) fn publish_embedded(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<(), FanoutError> {
        self.budget.begin_attempt()?;
        self.owner.publish_embedded(section)
    }
    pub(in crate::node_runtime) fn publish_remote(
        &mut self,
        client: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
    ) -> Result<(), FanoutError> {
        self.budget.begin_attempt()?;
        self.owner.publish_remote(client, session)
    }
    pub(in crate::node_runtime) fn apply_embedded(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        node: NodeId,
    ) -> Result<MetadataCommandReplicaState, FanoutError> {
        self.budget.begin_attempt()?;
        self.owner.apply_embedded(section, node)
    }
    pub(in crate::node_runtime) fn apply_remote(
        &mut self,
        client: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
        target: &UnixStorageNodeClient,
    ) -> Result<MetadataCommandReplicaState, FanoutError> {
        self.budget.begin_attempt()?;
        self.owner.apply_remote(client, session, target)
    }
    pub(in crate::node_runtime) fn apply_embedded_replica(
        &mut self,
        client: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
        node: NodeId,
    ) -> Result<MetadataCommandReplicaState, FanoutError> {
        self.budget.begin_attempt()?;
        self.owner.apply_embedded_replica(client, session, node)
    }
    pub(in crate::node_runtime) fn into_terminal(
        self,
    ) -> Result<BudgetedDirectOwner<DirectPutTerminal<'route>>, BudgetedDirectHandoffFailure<'route>>
    {
        let Self { owner, budget } = self;
        match owner.into_terminal() {
            Ok(owner) => Ok(BudgetedDirectOwner { owner, budget }),
            Err(failure) => Err(BudgetedDirectHandoffFailure {
                failure: Box::new((failure, budget)),
            }),
        }
    }
    pub(in crate::node_runtime) fn into_remote_terminal(
        self,
    ) -> Result<
        BudgetedDirectOwner<RemoteDirectPutTerminal<'route>>,
        BudgetedDirectHandoffFailure<'route>,
    > {
        let Self { owner, budget } = self;
        match owner.into_remote_terminal() {
            Ok(owner) => Ok(BudgetedDirectOwner { owner, budget }),
            Err(failure) => Err(BudgetedDirectHandoffFailure {
                failure: Box::new((failure, budget)),
            }),
        }
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_cutoff(&self) -> (Instant, u64) {
        self.owner.test_cutoff()
    }
}

impl BudgetedDirectOwner<DirectPutTerminal<'_>> {
    pub(in crate::node_runtime) fn reservation(&self) -> TerminalReservationObservation {
        self.owner.reservation()
    }
    pub(in crate::node_runtime) fn retirement(&self) -> TerminalSlotRetirementObservation {
        self.owner.retirement()
    }
    pub(in crate::node_runtime) fn release_reservation(
        &mut self,
    ) -> Result<(), crate::BucketSnapshotLoadError> {
        self.budget.begin_attempt()?;
        self.owner.release_reservation()
    }
    pub(in crate::node_runtime) fn retire_embedded(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<(), CompleteRetirementSendError> {
        self.budget
            .begin_attempt()
            .map_err(CompleteRetirementSendError::NotSent)?;
        self.owner.retire_embedded(section)
    }
}

impl BudgetedDirectOwner<RemoteDirectPutTerminal<'_>> {
    pub(in crate::node_runtime) fn reservation(&self) -> TerminalReservationObservation {
        self.owner.reservation()
    }
    pub(in crate::node_runtime) fn retirement(&self) -> TerminalSlotRetirementObservation {
        self.owner.retirement()
    }
    pub(in crate::node_runtime) fn release_reservation(
        &mut self,
        client: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
    ) -> Result<(), crate::BucketSnapshotLoadError> {
        self.budget.begin_attempt()?;
        self.owner.release_reservation(client, session)
    }
    pub(in crate::node_runtime) fn retire(
        &mut self,
        client: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
    ) -> Result<(), CompleteRetirementSendError> {
        self.budget
            .begin_attempt()
            .map_err(CompleteRetirementSendError::NotSent)?;
        self.owner.retire(client, session)
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_cutoff(&self) -> (Instant, u64) {
        self.owner.test_cutoff()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn staging_encrypted_publication_budget_owners_are_noncloneable() {
        trait NotClone<A> {
            fn check() {}
        }
        impl<T: ?Sized> NotClone<()> for T {}
        impl<T: Clone> NotClone<u8> for T {}
        let _ = <BudgetedDirectOwner<DirectTerminalPublication<'_>> as NotClone<_>>::check;
        let _ = <BudgetedDirectOwner<DirectPutTerminal<'_>> as NotClone<_>>::check;
        let _ = <BudgetedDirectOwner<RemoteDirectPutTerminal<'_>> as NotClone<_>>::check;
    }
}
