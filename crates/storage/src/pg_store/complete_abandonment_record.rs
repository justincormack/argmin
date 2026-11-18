// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! The shared recorder cannot select an unfenced complete identity in production.
//! The legacy arm accepts only its predecessor envelope; complete fixture setup
//! is test-only. Embedded mutation requires admitted authority plus the live
//! owning permit; remote mutation requires independently admitted receiver
//! authority. Replica mutation requires separate store-bound authority, with
//! primary-confirmed custody retained by the embedded owner or remote sender.
//! Every complete arm rechecks receiver authority beside INSERT.

use super::*;
use crate::node_runtime::engine::EmbeddedPublicationAuthority;

pub(super) enum AbandonmentRecord<'a> {
    Current(&'a MetadataCommandEnvelope),
    Complete {
        command: &'a payload_staging::EncodedContextualCommand<'a>,
        request: &'a CompletePublicationRequest<'a>,
        authority: &'a EmbeddedPublicationAuthority<'a>,
        custody: &'a UnstartedAbandonmentAuthority<'a, 'a>,
    },
    RemoteComplete {
        command: &'a payload_staging::EncodedContextualCommand<'a>,
        request: &'a CompletePublicationRequest<'a>,
        authority: &'a crate::node_runtime::server::CompleteAbandonmentAuthority<'a>,
    },
    ReplicaComplete {
        command: &'a payload_staging::EncodedContextualCommand<'a>,
        request: &'a CompletePublicationRequest<'a>,
        authority: &'a crate::node_runtime::engine::EmbeddedReplicaAbandonmentAuthority<'a>,
    },
    RemoteReplicaComplete {
        command: &'a payload_staging::EncodedContextualCommand<'a>,
        request: &'a CompletePublicationRequest<'a>,
        authority: &'a crate::node_runtime::server::RemoteReplicaAbandonmentAuthority<'a>,
    },
    #[cfg(test)]
    Fixture(&'a payload_staging::EncodedContextualCommand<'a>),
}

impl AbandonmentRecord<'_> {
    pub(super) fn identity(&self) -> TerminalCommandIdentity<'_> {
        match self {
            Self::Current(command) => TerminalCommandIdentity::current(command),
            Self::Complete { command, .. }
            | Self::RemoteComplete { command, .. }
            | Self::RemoteReplicaComplete { command, .. }
            | Self::ReplicaComplete { command, .. } => TerminalCommandIdentity::complete(command),
            #[cfg(test)]
            Self::Fixture(command) => TerminalCommandIdentity::complete(command),
        }
    }

    pub(super) fn require_effect(&self, pg: &PgStore, node: u32) -> Result<(), StoreError> {
        let (command, request) = match self {
            Self::Complete {
                command, request, ..
            }
            | Self::RemoteComplete {
                command, request, ..
            }
            | Self::ReplicaComplete {
                command, request, ..
            }
            | Self::RemoteReplicaComplete {
                command, request, ..
            } => (command, request),
            Self::Current(_) => return Ok(()),
            #[cfg(test)]
            Self::Fixture(_) => return Ok(()),
        };
        if node != request.node_id().as_u32()
            || command.id() != request.id()
            || command.command_sha256() != request.command_sha256()
            || command.topology() != request.topology()
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "complete abandonment recorder subject",
            });
        }
        match self {
            Self::Complete {
                custody, authority, ..
            } => {
                custody.require_request(request)?;
                authority.require_effect(pg, request)
            }
            Self::RemoteComplete { authority, .. } => authority.require_effect(pg, request),
            Self::ReplicaComplete { authority, .. } => authority.require_effect(pg, request),
            Self::RemoteReplicaComplete { authority, .. } => authority.require_effect(pg, request),
            Self::Current(_) => unreachable!("returned above"),
            #[cfg(test)]
            Self::Fixture(_) => unreachable!("returned above"),
        }
    }
}
