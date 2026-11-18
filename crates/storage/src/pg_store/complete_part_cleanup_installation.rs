// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! One atomic source-slot -> source-bearing cleanup replacement. This is not a
//! generic reissue: custody must come from the admitted all-member owner.

use super::*;
use crate::node_runtime::pg_store::AbandonedPartInstallation;

impl CompletePublicationRequest<'_> {
    pub(in crate::node_runtime) fn install_abandoned_part(
        &self,
        pg: &PgStore,
        authority: &EmbeddedPublicationAuthority<'_>,
        attempt: &AbandonedPartInstallation<'_, '_>,
        #[cfg(test)] mut hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), CompletePublicationError> {
        let require_effect = || {
            attempt.require_request(self)?;
            authority.require_abandoned_part_installation(attempt)?;
            authority.require_effect(pg, self)
        };
        pg.require_usable_connection()?;
        require_effect()?;
        if !pg.conn.is_autocommit() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "abandoned part replacement inside caller transaction",
            }
            .into());
        }
        let cleanup = attempt
            .cleanup()
            .map_err(Failure::from)
            .map_err(CompletePublicationError)?;
        let transaction =
            MetadataTransaction::begin(pg, "begin abandoned part pending replacement")?;
        let result = (|| -> Result<(), Failure> {
            let node = self.node_id().as_u32();
            let conflict = || StoreError::MetadataCommandLogConflict {
                node_id: node,
                pg_id: pg.pg_id(),
                cluster_epoch: self.id().cluster_epoch(),
                log_index: self.id().log_index().get(),
            };
            let pending = pg
                .load_complete_pending_in_open_transaction(node, self.topology())?
                .ok_or_else(conflict)?;
            let current = pending.context.encode_command().map_err(|_| conflict())?;
            if current.bytes() == cleanup.bytes() {
                // The inspection above revalidated exact scope and all indexes,
                // using retained state even if cleanup has already applied.
                // Never rewrite a started marker or repair damaged protection.
                require_effect()?;
            } else {
                if current.bytes() != self.command.bytes()
                    || pending.observation.publication_started
                    || !matches!(
                        pending.observation.terminal,
                        MetadataCommandStartupDisposition::Abandoned { .. }
                    )
                    || pg.metadata_command_replica_state()? != attempt.prefix()
                    || cleanup.inspect_terminal_in_open_transaction(pg, node)?
                        != MetadataCommandStartupDisposition::Absent
                {
                    return Err(conflict().into());
                }
                attempt.require_fresh_closure(pg, node, &current)?;
                let scope = PendingInstallationTransaction::new(pg);
                let replacement = PendingInstallation::Complete {
                    direct: None,
                    command: &cleanup,
                    topology: self.topology(),
                };
                let projection = replacement.prepare(&scope, node)?;
                let PendingShardScavengerReferencePages {
                    placed_reference_count,
                    placed_pages,
                    reclaim_reference_count,
                    reclaim_pages,
                } = projection.pages();
                let mut update = pg.conn.prepare_cached(
                    "UPDATE metadata_command_pending_slot SET cluster_epoch = ?1, pg_id = ?2, log_index = ?3, command_checksum = ?4, command_bytes = ?5, scope_bucket = ?6, publication_started = 0, placed_segment_reference_count = ?7, reclaim_reference_count = ?8 WHERE singleton = 0 AND cluster_epoch = ?9 AND pg_id = ?10 AND log_index = ?11 AND command_checksum = ?12 AND command_bytes = ?13 AND scope_bucket = ?14 AND publication_started = 0"
                ).map_err(|e| database("prepare abandoned part pending replacement", e))?;
                #[cfg(test)]
                hook("before-effect")?;
                require_effect()?;
                let changed = update
                    .execute(params![
                        cleanup.id().cluster_epoch().get() as i64,
                        cleanup.id().pg_id().get(),
                        cleanup.id().log_index().get() as i64,
                        cleanup.checksum_crc64() as i64,
                        cleanup.bytes(),
                        cleanup.scope_bucket(),
                        *placed_reference_count,
                        *reclaim_reference_count,
                        self.id().cluster_epoch().get() as i64,
                        self.id().pg_id().get(),
                        self.id().log_index().get() as i64,
                        self.command.checksum_crc64() as i64,
                        self.command.bytes(),
                        self.command.scope_bucket(),
                    ])
                    .map_err(|e| database("replace abandoned part pending slot", e))?;
                if changed != 1 {
                    return Err(conflict().into());
                }
                drop(update);
                #[cfg(test)]
                hook("after-slot")?;
                pg.replace_pending_placed_reference_pages(*placed_reference_count, placed_pages)?;
                pg.replace_pending_reclaim_reference_pages(
                    *reclaim_reference_count,
                    reclaim_pages,
                )?;
                projection.install(pg)?;
            }
            #[cfg(test)]
            hook("before-commit")?;
            require_effect()?;
            Ok(())
        })();
        finish_transaction(
            transaction,
            result,
            "commit abandoned part pending replacement",
        )?;
        #[cfg(test)]
        hook("after-commit")?;
        Ok(())
    }
}
