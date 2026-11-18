// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Admitted, read-only primary/witness confirmation of an active future window.
//! Keep primary serialization through witness inspection and final authority
//! checks. The embedded owner keeps a coherent PG snapshot; the remote owner
//! inspects the witness first, then opens the primary snapshot on the same held
//! connection section. No data-PG/file lock or physical IO.

use super::*;
use crate::cluster::CompleteFanoutRoute;
use crate::cluster::EmbeddedPublicationSection;
use crate::node_client::{UnixStorageNodeClient, UnixStorageNodeMetadataCommandSession};
use crate::node_runtime::pg_store::{
    StagingWindowConfirmationError, StagingWindowConfirmationObservation,
    StagingWindowConfirmationRequest,
};

impl EmbeddedPublicationSection<'_> {
    pub(in crate::node_runtime) fn inspect_staging_window(
        &self,
        request: &StagingWindowConfirmationRequest<'_>,
        deadline: Instant,
        #[cfg(test)] hook: impl FnMut(&'static str),
    ) -> Result<StagingWindowConfirmationObservation, StagingWindowConfirmationError> {
        self.inspect_staging_window_inner(
            request,
            deadline,
            false,
            #[cfg(test)]
            hook,
        )
    }

    pub(in crate::node_runtime) fn reinspect_staging_window(
        &self,
        request: &StagingWindowConfirmationRequest<'_>,
        deadline: Instant,
    ) -> Result<StagingWindowConfirmationObservation, StagingWindowConfirmationError> {
        self.inspect_staging_window_inner(
            request,
            deadline,
            true,
            #[cfg(test)]
            |_| {},
        )
    }

    fn inspect_staging_window_inner(
        &self,
        request: &StagingWindowConfirmationRequest<'_>,
        deadline: Instant,
        revalidate_prefix: bool,
        #[cfg(test)] mut hook: impl FnMut(&'static str),
    ) -> Result<StagingWindowConfirmationObservation, StagingWindowConfirmationError> {
        self.require_effect()?;
        // Also rejects historical sections. The section alone selects the peer.
        let witness = self.confirmation_witness()?;
        let runtime = self.runtime()?;
        let (deadline, window_fence) = request.bind_deadline(
            self.pg_id(),
            runtime.pg_topology(),
            self.epoch(),
            deadline.min(self.deadline()),
        )?;
        let require_authority = || -> Result<(), StoreError> {
            self.require_effect()?;
            window_fence.require_valid_for(self.epoch())?;
            if Instant::now() >= deadline {
                return Err(StoreError::OperationDeadlineExceeded {
                    context: "staging window confirmation",
                });
            }
            Ok(())
        };
        require_authority()?;
        let pg = runtime.node.get_pg_until(self.pg_id().get(), deadline)?;
        require_authority()?;
        if revalidate_prefix {
            request.validate_reinspection_prefix(
                &pg,
                self.node_id(),
                self.epoch(),
                deadline,
                window_fence,
            )?;
            require_authority()?;
        }
        let read = request.inspect_primary(&pg, self.node_id(), self.epoch())?;
        let prefix = *read.prefix();
        #[cfg(test)]
        hook("before witness");
        require_authority()?;
        if let Some(witness) = &witness {
            let witness_prefix = if let Some(witness_runtime) = witness.embedded_runtime()? {
                if witness_runtime.pg_topology() != runtime.pg_topology() {
                    return Err(StoreError::RouteCapabilitySubjectMismatch {
                        operation: "window confirmation witness topology",
                    }
                    .into());
                }
                let witness_pg = witness_runtime
                    .node
                    .get_pg_until(self.pg_id().get(), deadline)?;
                require_authority()?;
                if revalidate_prefix {
                    request.validate_reinspection_prefix(
                        &witness_pg,
                        witness.node_id(),
                        self.epoch(),
                        deadline,
                        window_fence,
                    )?;
                    require_authority()?;
                }
                let witness_read = witness_pg.inspect_complete_settled_prefix(
                    witness.node_id().as_u32(),
                    self.epoch(),
                    runtime.pg_topology(),
                )?;
                let state = *witness_read.prefix();
                witness_read.finish()?;
                state
            } else {
                witness.remote_prefix_until(deadline)?
            };
            require_authority()?;
            if witness_prefix != prefix {
                return Err(StagingWindowConfirmationError::PrefixDisagreement);
            }
        }
        #[cfg(test)]
        hook("after witness");
        require_authority()?;
        read.finish()?;
        // Teardown is part of the observation. Unconfirmed rollback retires the
        // store; expiry during teardown must not return a positive result.
        require_authority()?;
        Ok(StagingWindowConfirmationObservation::settled(
            prefix,
            witness.as_ref().map(|peer| peer.node_id()),
        ))
    }
}

impl CompleteFanoutRoute<'_> {
    pub(in crate::node_runtime) fn inspect_staging_window_remote(
        &self,
        request: &StagingWindowConfirmationRequest<'_>,
        client: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
        deadline: Instant,
        #[cfg(test)] mut hook: impl FnMut(&'static str),
    ) -> Result<
        (
            StagingWindowConfirmationObservation,
            Instant,
            crate::storage_rpc::StorageRpcOperationDeadline,
        ),
        StagingWindowConfirmationError,
    > {
        use crate::storage_rpc::{
            complete_settled_prefix as witness_wire, complete_window_primary as wire,
            StorageRpcOperationDeadline,
        };
        let deadline = self.window_confirmation_deadline(client, deadline)?;
        let (deadline, fence) =
            request.bind_deadline(self.pg_id(), self.topology(), self.epoch(), deadline)?;
        let (deadline, portable) = self.intersect_deadline(
            deadline,
            StorageRpcOperationDeadline::from_instant(deadline),
        );
        let require = || -> Result<(), StoreError> {
            self.require_valid()?;
            fence.require_valid_for(self.epoch())?;
            if Instant::now() >= deadline {
                return Err(StoreError::OperationDeadlineExceeded {
                    context: "remote primary window confirmation",
                });
            }
            Ok(())
        };
        require()?;
        // The exclusive session borrow prevents interleaved use/release of the
        // primary section while the designated witness is inspected. The final
        // primary read is also proof the same connection still owns that lock.
        let mut primary =
            session.borrow_complete_primary(client, self.pg_id(), self.epoch(), deadline)?;
        let witness = self.window_confirmation_witness()?;
        #[cfg(test)]
        hook("before witness");
        require()?;
        let witnessed = if let Some(peer) = &witness {
            let prefix = if let Some(runtime) = peer.embedded_runtime()? {
                if runtime.pg_topology() != self.topology() {
                    return Err(StoreError::RouteCapabilitySubjectMismatch {
                        operation: "window witness topology",
                    }
                    .into());
                }
                let pg = runtime.node.get_pg_until(self.pg_id().get(), deadline)?;
                require()?;
                let read = pg.inspect_complete_settled_prefix(
                    peer.node_id().as_u32(),
                    self.epoch(),
                    self.topology(),
                )?;
                let prefix = *read.prefix();
                read.finish()?;
                prefix
            } else {
                peer.remote_prefix_until(deadline)?
            };
            Some(prefix)
        } else {
            None
        };
        #[cfg(test)]
        hook("after witness");
        require()?;
        let descriptor = request.encode_descriptor()?;
        let peer = witness.as_ref().map(|peer| peer.node_id());
        let prefix = primary.inspect_window_primary(
            wire::Request {
                route: witness_wire::Request {
                    node: self.primary(),
                    epoch: self.epoch(),
                    pg: self.pg_id(),
                    deadline: portable,
                    topology: witness_wire::topology_digest(self.topology()),
                },
                descriptor: &descriptor,
            },
            peer,
        )?;
        #[cfg(test)]
        hook("after primary");
        require()?;
        if witnessed.is_some_and(|witness| witness != prefix) {
            return Err(StagingWindowConfirmationError::PrefixDisagreement);
        }
        // Return the exact effective bounds used for this inspection. A later
        // projection of the same Instant need not produce the same wall cutoff.
        Ok((
            StagingWindowConfirmationObservation::settled(prefix, peer),
            deadline,
            portable,
        ))
    }
}
