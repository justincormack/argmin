// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Owner-local tests of the production PG lifetime hooks. Stream fixtures are
//! metadata-only. The durable creation issuer survives restart/import, while the
//! replacement local lifetime must never grant that old session new allocation.

use super::*;

fn identity(store: &PgStore) -> Option<[u8; 16]> {
    store.allocation_lifetime.active_identity()
}

#[test]
fn pg_allocation_lifetime_reopen_preserves_session_but_not_issuer_identity() {
    for part in [false, true] {
        let dir = test_util::tempdir();
        let store = PgStore::open(dir.path(), 1).unwrap();
        let original = identity(&store).unwrap();
        let create = fixture(&store, part);
        assert_eq!(create.allocation_issuer.lifetime, original);
        store
            .apply_metadata_command_and_record(0, &envelope(&create, 1))
            .unwrap();
        let session = store.get_stream_upload(&create.session.session_id).unwrap();
        let checkpoint = store
            .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
            .unwrap();
        assert_eq!(identity(&store), Some(original));
        drop(store);

        let reopened = PgStore::open(dir.path(), 1).unwrap();
        assert_ne!(identity(&reopened).unwrap(), original);
        assert_eq!(
            reopened
                .load_stream_allocation_issuer(&create.session.session_id)
                .unwrap(),
            create.allocation_issuer
        );
        assert_ne!(
            reopened
                .stream_allocation_issuer(crate::NodeId::new(0))
                .unwrap(),
            create.allocation_issuer
        );
        assert_eq!(
            reopened
                .get_stream_upload(&create.session.session_id)
                .unwrap(),
            session
        );
        assert_eq!(
            reopened
                .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
                .unwrap(),
            checkpoint,
            "the fresh local lifetime must not change checkpoint bytes or state digest"
        );
    }
}

#[test]
fn pg_allocation_lifetime_runtime_clones_share_identity_not_foreign_pgs_or_runtimes() {
    use crate::node_runtime::engine::LocalNodeRuntime;
    let dir = test_util::tempdir();
    let runtime = LocalNodeRuntime::open(
        placement::NodeId::new(0),
        &dir.path().join("first"),
        &[0, 1],
        EcShape { k: 1, m: 0 },
        ClusterEpoch::INITIAL,
    )
    .unwrap();
    let clone = runtime.clone();
    let original = identity(&runtime.test_node().get_pg(1).unwrap()).unwrap();
    assert_eq!(
        identity(&clone.test_node().get_pg(1).unwrap()),
        Some(original)
    );
    assert_ne!(
        identity(&runtime.test_node().get_pg(0).unwrap()).unwrap(),
        original
    );
    let foreign = LocalNodeRuntime::open(
        placement::NodeId::new(0),
        &dir.path().join("foreign"),
        &[0, 1],
        EcShape { k: 1, m: 0 },
        ClusterEpoch::INITIAL,
    )
    .unwrap();
    assert_ne!(
        identity(&foreign.test_node().get_pg(1).unwrap()).unwrap(),
        original
    );
    drop(runtime);
    assert_eq!(
        identity(&clone.test_node().get_pg(1).unwrap()),
        Some(original),
        "dropping one Arc does not terminate the shared PG lifetime"
    );
}

#[test]
fn pg_allocation_lifetime_apply_heartbeat_and_compaction_do_not_remint_or_write() {
    for part in [false, true] {
        let dir = test_util::tempdir();
        let store = PgStore::open(dir.path(), 1).unwrap();
        let original = identity(&store).unwrap();
        let create = fixture(&store, part);
        let checkpoint = evidence::compact_creation_out_of_exact_receipt_window(&store, &create);
        assert_eq!(identity(&store), Some(original));
        let changes = store.conn.total_changes();
        for _ in 0..3 {
            let state = store
                .metadata_command_replica_state_for_heartbeat(0, ClusterEpoch::INITIAL)
                .unwrap();
            assert_eq!(state.state_digest, checkpoint.state_digest);
            assert_eq!(identity(&store), Some(original));
        }
        assert_eq!(store.conn.total_changes(), changes);
    }
}

#[test]
fn pg_allocation_lifetime_checkpoint_replacement_and_rollback_retire_prior_issuer() {
    for part in [false, true] {
        let dir = test_util::tempdir();
        let store = PgStore::open(dir.path(), 1).unwrap();
        let create = fixture(&store, part);
        store
            .apply_metadata_command_and_record(0, &envelope(&create, 1))
            .unwrap();
        let original = identity(&store).unwrap();
        let checkpoint = store
            .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
            .unwrap();
        let epoch = ClusterEpoch::new(2).unwrap();
        // Fail after the importer clears its destination tables, not during
        // initial validation. SQL rolls back, but local issuer retirement must not.
        store
            .conn
            .execute_batch(
                "CREATE TEMP TRIGGER reject_session_import BEFORE INSERT ON stream_uploads \
                 BEGIN SELECT RAISE(ABORT, 'injected session import failure'); END",
            )
            .unwrap();
        let error = store
            .install_metadata_transfer_checkpoint_base(0, epoch, &checkpoint)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("injected session import failure"),
            "{error:?}"
        );
        assert_eq!(identity(&store), None);
        assert!(store.conn.is_autocommit());
        assert_eq!(
            store
                .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
                .unwrap(),
            checkpoint,
            "rollback must preserve the complete original materialized state and prefix"
        );
        store
            .conn
            .execute_batch("DROP TRIGGER reject_session_import")
            .unwrap();
        store
            .install_metadata_transfer_checkpoint_base(0, epoch, &checkpoint)
            .unwrap();
        let replaced = identity(&store).unwrap();
        assert_ne!(replaced, original);
        let imported = store.metadata_command_checkpoint(0, epoch).unwrap();
        assert_eq!(imported.table_blocks, checkpoint.table_blocks);
        assert_eq!(imported.state_digest, checkpoint.state_digest);
        assert_eq!(imported.applied_log_index, 0);
        // A rejected attempt on an already initialized epoch must not restore
        // that epoch's previous local issuer either.
        assert!(store
            .install_metadata_transfer_checkpoint_base(0, epoch, &checkpoint)
            .is_err());
        assert_eq!(identity(&store), None);
        assert_eq!(
            store.metadata_command_checkpoint(0, epoch).unwrap(),
            imported
        );
    }
}

#[test]
fn pg_allocation_lifetime_rebased_adoption_retires_even_unchanged_materialized_state() {
    for part in [false, true] {
        let dir = test_util::tempdir();
        let store = PgStore::open(dir.path(), 1).unwrap();
        let create = fixture(&store, part);
        let creation = envelope(&create, 1);
        let pre_state_digest = store.metadata_command_replica_state().unwrap().state_digest;
        let state = store
            .apply_metadata_command_and_record(0, &creation)
            .unwrap();
        let original = identity(&store).unwrap();
        let session = store.get_stream_upload(&create.session.session_id).unwrap();
        let epoch = ClusterEpoch::new(2).unwrap();
        let commands = [MetadataTransferCommand {
            command: rebase_probe_commands(epoch, PgId::new(1), &[creation]).remove(0),
            pre_state_digest,
            post_state_digest: state.state_digest,
        }];
        store
            .adopt_metadata_transfer_state_from_rebased_commands(
                0,
                epoch,
                &commands,
                state.state_digest,
            )
            .unwrap();
        let replaced = identity(&store).unwrap();
        assert_ne!(replaced, original);
        assert_eq!(
            store.get_stream_upload(&create.session.session_id).unwrap(),
            session
        );
        let checkpoint = store.metadata_command_checkpoint(0, epoch).unwrap();
        // Dirty expected evidence fails inside the transaction. Subsequent exact
        // retry may establish a new lifetime, never revive the retired one.
        assert!(matches!(
            store.adopt_metadata_transfer_state_from_rebased_commands(
                0,
                epoch,
                &commands,
                CanonicalStateDigest::for_test(state.state_digest.value() ^ 1)
            ),
            Err(StoreError::MetadataStateDigestMismatch { .. })
        ));
        assert_eq!(identity(&store), None);
        assert_eq!(
            store.metadata_command_checkpoint(0, epoch).unwrap(),
            checkpoint
        );
        store
            .adopt_metadata_transfer_state_from_rebased_commands(
                0,
                epoch,
                &commands,
                state.state_digest,
            )
            .unwrap();
        let retried = identity(&store).unwrap();
        assert_ne!(retried, original);
        assert_ne!(retried, replaced);
        assert_eq!(
            store.metadata_command_checkpoint(0, epoch).unwrap(),
            checkpoint
        );
    }
}

#[test]
fn pg_allocation_lifetime_empty_and_matching_transfer_initialization_retire_issuer() {
    for matching in [false, true] {
        let dir = test_util::tempdir();
        let store = PgStore::open(dir.path(), 1).unwrap();
        let original = identity(&store).unwrap();
        let state = store.metadata_command_replica_state().unwrap();
        let epoch = ClusterEpoch::new(2).unwrap();
        let initialize = |digest| {
            if matching {
                store.initialize_metadata_transfer_matching_state(
                    0,
                    epoch,
                    0,
                    MetadataCommandLogHash::genesis(),
                    digest,
                )
            } else {
                store.initialize_metadata_transfer_empty_state(0, epoch, digest)
            }
        };
        assert!(matches!(
            initialize(CanonicalStateDigest::for_test(
                state.state_digest.value() ^ 1
            )),
            Err(StoreError::MetadataStateDigestMismatch { .. })
        ));
        assert_eq!(identity(&store), None);
        assert_eq!(store.metadata_command_replica_state().unwrap(), state);
        let initialized = initialize(state.state_digest).unwrap();
        assert_ne!(identity(&store).unwrap(), original);
        assert_eq!(initialized.cluster_epoch, epoch);
        assert_eq!(initialized.state_digest, state.state_digest);
        assert_eq!(initialized.applied_log_index, 0);
    }
}

#[test]
fn pg_allocation_lifetime_failed_transfer_commit_does_not_activate_candidate() {
    let dir = test_util::tempdir();
    let store = PgStore::open(dir.path(), 1).unwrap();
    let original = identity(&store).unwrap();
    let before = store.metadata_command_replica_state().unwrap();
    // The update succeeds; only COMMIT fails its deferred FK check. These temp
    // objects are fault injection, not part of the durable format catalogue.
    store
        .conn
        .execute_batch(
            "CREATE TEMP TABLE lifetime_parent (id INTEGER PRIMARY KEY); \
         CREATE TEMP TABLE lifetime_child (parent INTEGER REFERENCES lifetime_parent(id) \
             DEFERRABLE INITIALLY DEFERRED); \
         CREATE TEMP TRIGGER fail_lifetime_commit AFTER UPDATE ON metadata_command_replica_state \
         BEGIN INSERT INTO lifetime_child VALUES (1); END",
        )
        .unwrap();
    let epoch = ClusterEpoch::new(2).unwrap();
    let initialize = || {
        store.initialize_metadata_transfer_matching_state(
            0,
            epoch,
            0,
            MetadataCommandLogHash::genesis(),
            before.state_digest,
        )
    };
    let error = initialize().unwrap_err();
    assert!(
        matches!(
            error,
            StoreError::Db {
                context: "commit metadata transfer matching state initialization",
                ..
            }
        ),
        "{error:?}"
    );
    assert_eq!(identity(&store), None);
    assert!(store.conn.is_autocommit());
    assert_eq!(store.metadata_command_replica_state().unwrap(), before);
    store
        .conn
        .execute_batch("DROP TRIGGER fail_lifetime_commit")
        .unwrap();
    initialize().unwrap();
    assert_ne!(identity(&store).unwrap(), original);
}
