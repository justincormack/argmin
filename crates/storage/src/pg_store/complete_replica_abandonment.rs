// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Replica-local tombstone only, under admitted serialization. Embedded authority
//! retains primary-confirmed custody; remote authority admits the authenticated
//! sender's request independently. Pending protection and materialized state survive.

use super::*;
use crate::node_runtime::engine::EmbeddedReplicaAbandonmentAuthority;
use crate::node_runtime::server::RemoteReplicaAbandonmentAuthority;

#[path = "complete_replica_abandonment_query.rs"]
mod query;
pub(in crate::node_runtime) use query::{
    CompleteReplicaAbandonmentQuery, CompleteReplicaAbandonmentRequest,
};

enum Authority<'a> {
    Embedded(&'a EmbeddedReplicaAbandonmentAuthority<'a>),
    Remote(&'a RemoteReplicaAbandonmentAuthority<'a>),
}

impl Authority<'_> {
    fn request(&self) -> &CompletePublicationRequest<'_> {
        match self {
            Self::Embedded(a) => a.request(),
            Self::Remote(a) => a.request(),
        }
    }
    fn require_effect(
        &self,
        pg: &PgStore,
        request: &CompletePublicationRequest<'_>,
    ) -> Result<(), StoreError> {
        match self {
            Self::Embedded(a) => a.require_effect(pg, request),
            Self::Remote(a) => a.require_effect(pg, request),
        }
    }
    fn record<'a>(&'a self, request: &'a CompletePublicationRequest<'a>) -> AbandonmentRecord<'a> {
        match self {
            Self::Embedded(authority) => AbandonmentRecord::ReplicaComplete {
                command: &request.command,
                request,
                authority,
            },
            Self::Remote(authority) => AbandonmentRecord::RemoteReplicaComplete {
                command: &request.command,
                request,
                authority,
            },
        }
    }
}

impl PgStore {
    pub(in crate::node_runtime) fn record_complete_replica_abandonment(
        &self,
        authority: &EmbeddedReplicaAbandonmentAuthority<'_>,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), CompletePublicationError> {
        self.record_replica_abandonment_inner(
            Authority::Embedded(authority),
            #[cfg(test)]
            hook,
        )
    }

    pub(in crate::node_runtime) fn record_remote_complete_replica_abandonment(
        &self,
        authority: &RemoteReplicaAbandonmentAuthority<'_>,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), CompletePublicationError> {
        self.record_replica_abandonment_inner(
            Authority::Remote(authority),
            #[cfg(test)]
            hook,
        )
    }

    fn record_replica_abandonment_inner(
        &self,
        authority: Authority<'_>,
        #[cfg(test)] mut hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), CompletePublicationError> {
        let request = authority.request();
        let require_effect = || authority.require_effect(self, request);
        self.require_usable_connection()?;
        require_effect()?;
        if !self.conn.is_autocommit() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "replica abandonment inside caller transaction",
            }
            .into());
        }
        let transaction = MetadataTransaction::begin(self, "replica abandonment begin")?;
        let result = (|| -> Result<(), Failure> {
            let command = &request.command;
            let node = request.node_id().as_u32();
            let identity = TerminalCommandIdentity::complete(command);
            let slot =
                self.pending_metadata_command_slot_with_validator(node, |bytes, crc, id| {
                    if command.validate_slot_bytes(bytes, crc, id) {
                        Ok(())
                    } else {
                        Err(identity.conflict(node, self.pg_id))
                    }
                })?;
            if let Some(slot) = slot {
                if slot.publication_started
                    || slot.scope_bucket.as_ref() != Some(command.scope_bucket())
                {
                    return Err(identity.conflict(node, self.pg_id).into());
                }
                command.require_retained_pending_indexes(self, node, request.topology())?;
            } else {
                for table in [
                    "metadata_command_pending_route_dependencies",
                    "metadata_command_pending_staging_protection",
                    "metadata_command_pending_placed_reference_pages",
                    "metadata_command_pending_reclaim_reference_pages",
                ] {
                    let present: bool = self
                        .conn
                        .query_row(
                            &format!("SELECT EXISTS(SELECT 1 FROM {table})"),
                            [],
                            |row| row.get(0),
                        )
                        .map_err(|error| {
                            database("replica abandonment orphan index check", error)
                        })?;
                    if present {
                        return Err(identity.conflict(node, self.pg_id).into());
                    }
                }
            }
            let abandoned =
                match self.metadata_command_identity_startup_disposition(node, &identity)? {
                    MetadataCommandStartupDisposition::Absent => false,
                    MetadataCommandStartupDisposition::Abandoned { .. } => true,
                    MetadataCommandStartupDisposition::Applied { .. } => {
                        return Err(identity.conflict(node, self.pg_id).into())
                    }
                };
            require_effect()?;
            if !abandoned {
                self.record_metadata_command_abandoned_inner(
                    node,
                    &authority.record(request),
                    #[cfg(test)]
                    || hook("before-tombstone"),
                )?;
            }
            #[cfg(test)]
            hook("before-commit")?;
            require_effect()?;
            Ok(())
        })();
        finish_transaction(transaction, result, "replica abandonment commit")?;
        self.invalidate_clean_metadata_digest_revision();
        #[cfg(test)]
        hook("after-commit")?;
        Ok(())
    }
}
