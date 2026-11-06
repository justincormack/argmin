// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Metadata-only component schedules. These exercise real epoch initialization,
//! command acceptance and applied-log recording, not recovery-certificate minting
//! or authorization of fresh physical allocation under a predecessor issuer.

use super::*;
use crate::control_plane::MetadataCommandLogHash;
use crate::metadata_command::{MetadataCommandAcceptance, MetadataCommandEnvelope};
use crate::pg_store::PgStore;

pub(super) fn at_epoch_start(
    pg: &PgStore,
    command: &MetadataCommandEnvelope,
    epoch: ClusterEpoch,
) -> MetadataCommandEnvelope {
    // The component fixtures materialize their prerequisite rows directly.
    // Anchor that exact state before exercising the production epoch boundary.
    assert!(!pg.conn.is_autocommit());
    pg.refresh_metadata_command_state_digest().unwrap();
    pg.conn.execute_batch("COMMIT").unwrap();
    let old = pg.metadata_command_replica_state().unwrap();
    assert!(old.cluster_epoch < epoch);
    let initialized = pg
        .initialize_metadata_transfer_matching_state(
            0,
            epoch,
            0,
            MetadataCommandLogHash::genesis(),
            old.state_digest,
        )
        .unwrap();
    assert_eq!(initialized.cluster_epoch, epoch);
    assert_eq!(initialized.applied_log_index, 0);
    assert_eq!(initialized.state_digest, old.state_digest);
    let candidate = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            epoch,
            command.id().pg_id(),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        command.payload().clone(),
    );
    let decoded = crate::metadata_command::decode_metadata_command_envelope(
        &candidate.command_bytes(),
        &crate::node_runtime::MetadataCommandDecodeAuthority::new_for_test(),
    )
    .unwrap();
    assert_eq!(
        decoded.payload(),
        command.payload(),
        "reissue must not rewrite proof epochs or placement"
    );
    pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    assert_eq!(
        pg.metadata_command_acceptance(0, &decoded).unwrap(),
        MetadataCommandAcceptance::Apply
    );
    decoded
}

pub(super) fn record_and_reopen(
    pg: PgStore,
    path: &std::path::Path,
    command: &MetadataCommandEnvelope,
) -> PgStore {
    let expected = introduction::state(&pg);
    pg.record_metadata_command_applied_inner(0, command)
        .unwrap();
    pg.conn.execute_batch("COMMIT").unwrap();
    let state = pg.metadata_command_replica_state().unwrap();
    assert_eq!(state.cluster_epoch, command.id().cluster_epoch());
    assert_eq!(state.applied_log_index, 1);
    assert_eq!(
        state.state_digest.value(),
        pg.metadata_state_digest().unwrap()
    );
    let pg_id = pg.pg_id;
    drop(pg);
    let pg = PgStore::open(path, pg_id).unwrap();
    pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    assert_eq!(introduction::state(&pg), expected);
    assert_eq!(pg.metadata_command_replica_state().unwrap(), state);
    assert_eq!(
        pg.metadata_command_acceptance(0, command).unwrap(),
        MetadataCommandAcceptance::AlreadyApplied
    );
    pg
}

#[test]
fn staging_epoch_order_is_strict_and_pg_scoped() {
    let id = |pg, epoch, index| {
        MetadataCommandId::new(
            ClusterEpoch::new(epoch).unwrap(),
            PgId::new(pg),
            MetadataCommandLogIndex::new(index).unwrap(),
        )
    };
    let before = id(2, 5, 11);
    for (after, expected) in [
        (id(2, 5, 10), false),
        (id(2, 5, 11), false),
        (id(2, 5, 12), true),
        (id(2, 4, 100), false),
        (id(2, 6, 1), true),
        (id(2, 6, 11), true),
        (id(3, 6, 1), false),
        (id(1, 5, 12), false),
    ] {
        assert_eq!(
            command_follows(before, after),
            expected,
            "{before:?} -> {after:?}"
        );
    }
}
