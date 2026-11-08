// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::super::descriptor::{AppendDescriptor, IntroductionDescriptor};
use super::pending_index::install;
use super::*;
use crate::metadata_command::MetadataCommandEnvelope;
use crate::node_runtime::pg_store::{pending_staging_index::PendingStagingIndex, PgStore};
use crate::types::{
    ShardScavengerPayloadReference as Reference, ShardScavengerReferenceCursor as Cursor,
};
use std::num::NonZeroU16;

#[test]
fn staging_pending_paging_all_introductions_are_protection_only_and_make_progress() {
    for target in [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ] {
        let owner = fixture(target);
        let command = introduction::command(&owner);
        let dir = test_util::tempdir();
        let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
        install(&pg, &owner, &command);
        drop(pg);
        let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
        let writes = pg.conn.total_changes();
        let page = pg
            .list_shard_scavenger_reference_page(&topology(), None, NonZeroU16::new(1).unwrap())
            .unwrap();
        assert!(!page.complete);
        assert_eq!(page.items.len(), 1);
        let item = &page.items[0];
        assert!(matches!(item.cursor, Cursor::PendingStagingWindow { .. }));
        let Reference::StagingProtection(protection) = &item.reference else {
            panic!("unused window must not be readable");
        };
        assert_eq!(protection.file_nodes, [2, 0, 3].map(NodeId::new));
        assert_eq!(protection.acknowledgement_primary, NodeId::new(1));
        assert_eq!(protection.identity.okh, owner.windows[0].segment_hash);
        assert_eq!(protection.identity.generation_id, owner.windows[0].start);
        let next = pg
            .list_shard_scavenger_reference_page(
                &topology(),
                Some(&item.cursor),
                NonZeroU16::new(1).unwrap(),
            )
            .unwrap();
        assert!(next.complete && next.items.is_empty());
        let backfill = pg
            .list_placed_shard_scavenger_reference_page(None, NonZeroU16::new(64).unwrap())
            .unwrap();
        assert!(backfill.complete && backfill.items.is_empty());
        assert_eq!(
            pg.list_shard_scavenger_payload_references(&topology())
                .unwrap(),
            vec![Reference::ProtectionOnly(protection.identity.clone())]
        );
        assert_eq!(pg.conn.total_changes(), writes);
    }
}

#[test]
fn staging_pending_paging_reissue_and_materialization_restart_the_scan() {
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(StagingTarget::DirectPut).subject.object_pg.get(),
    )
    .unwrap();
    let (owner, command) = introduction::setup(&pg, StagingTarget::DirectPut);
    install(&pg, &owner, &command);
    let first = pg
        .list_shard_scavenger_reference_page(&topology(), None, NonZeroU16::new(1).unwrap())
        .unwrap();
    let reissued = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::new(6).unwrap(),
            command.id().pg_id(),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        command.payload().clone(),
    );
    install(&pg, &owner, &reissued);
    let second = pg
        .list_shard_scavenger_reference_page(
            &topology(),
            Some(&first.items[0].cursor),
            NonZeroU16::new(1).unwrap(),
        )
        .unwrap();
    assert_eq!(first.items[0].reference, second.items[0].reference);
    assert_ne!(first.items[0].cursor, second.items[0].cursor);
    let topology = topology();
    let descriptor = IntroductionDescriptor::capture(&owner.validate(&topology).unwrap()).unwrap();
    pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    descriptor
        .bind(&topology, &reissued)
        .unwrap()
        .apply_in_open_transaction(&pg)
        .unwrap();
    pg.test_clear_pending_metadata_command_slot().unwrap();
    pg.conn.execute_batch("COMMIT").unwrap();
    let materialized = pg
        .list_shard_scavenger_reference_page(
            &topology,
            Some(&second.items[0].cursor),
            NonZeroU16::new(1).unwrap(),
        )
        .unwrap();
    assert_eq!(materialized.items.len(), 1);
    assert!(matches!(
        materialized.items[0].cursor,
        Cursor::StagingWindow { .. }
    ));
    assert_eq!(materialized.items[0].reference, first.items[0].reference);
    let end = pg
        .list_shard_scavenger_reference_page(
            &topology,
            Some(&materialized.items[0].cursor),
            NonZeroU16::new(1).unwrap(),
        )
        .unwrap();
    assert!(end.complete && end.items.is_empty());
}

#[test]
fn staging_pending_paging_changed_index_under_same_command_restarts_the_scan() {
    let mut owner = fixture(StagingTarget::DirectPut);
    let command = introduction::command(&owner);
    let dir = test_util::tempdir();
    let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
    install(&pg, &owner, &command);
    let first = pg
        .list_shard_scavenger_reference_page(&topology(), None, NonZeroU16::new(1).unwrap())
        .unwrap();
    // Keep the full command and topology unchanged, varying only the bound
    // protection. The index checksum must independently identify the resume.
    owner.windows[0].participants.files.swap(0, 1);
    install(&pg, &owner, &command);
    let second = pg
        .list_shard_scavenger_reference_page(
            &topology(),
            Some(&first.items[0].cursor),
            NonZeroU16::new(1).unwrap(),
        )
        .unwrap();
    assert_eq!(second.items.len(), 1);
    assert_ne!(first.items[0].reference, second.items[0].reference);
    let mut expected = first.items[0].cursor.clone();
    let Cursor::PendingStagingWindow {
        protection_checksum: actual_checksum,
        ..
    } = second.items[0].cursor
    else {
        panic!("pending window")
    };
    let Cursor::PendingStagingWindow {
        protection_checksum,
        ..
    } = &mut expected
    else {
        unreachable!()
    };
    assert_ne!(*protection_checksum, actual_checksum);
    *protection_checksum = actual_checksum;
    assert_eq!(second.items[0].cursor, expected);
}

#[test]
fn staging_pending_paging_append_precedes_materialized_windows_without_hiding_either() {
    for target in [StagingTarget::StreamPut, multipart_target()] {
        let dir = test_util::tempdir();
        let pg =
            PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
        let owner = append::initialize(&pg, target);
        let window = append::next(&pg, &owner);
        let command = append::command(&owner);
        let topology = topology();
        let descriptor =
            AppendDescriptor::capture(&owner.validate(&topology).unwrap(), window.clone()).unwrap();
        let bound = descriptor.bind(&topology, &command).unwrap();
        let projection = bound.pending_protection();
        pg.test_replace_pending_metadata_command_slot(&command, None)
            .unwrap();
        PendingStagingIndex::from_bound(&projection)
            .install_in_open_transaction(&pg, &topology)
            .unwrap();
        pg.conn.execute_batch("COMMIT").unwrap();
        let first = pg
            .list_shard_scavenger_reference_page(&topology, None, NonZeroU16::new(1).unwrap())
            .unwrap();
        // The pending append's selected segment is already readable. Its unused
        // next window is a separate, protection-only index entry.
        assert!(matches!(
            first.items[0].cursor,
            Cursor::PendingPlacedCommand { .. }
        ));
        assert!(matches!(first.items[0].reference, Reference::Placed(_)));
        let selected = first.items[0].reference.clone();
        let first = pg
            .list_shard_scavenger_reference_page(
                &topology,
                Some(&first.items[0].cursor),
                NonZeroU16::new(1).unwrap(),
            )
            .unwrap();
        assert!(matches!(
            first.items[0].cursor,
            Cursor::PendingStagingWindow { .. }
        ));
        let Reference::StagingProtection(protection) = &first.items[0].reference else {
            panic!("unused append window must not be readable");
        };
        assert_eq!(protection.identity.okh, window.segment_hash);
        assert_eq!(protection.identity.generation_id, window.start);
        let second = pg
            .list_shard_scavenger_reference_page(
                &topology,
                Some(&first.items[0].cursor),
                NonZeroU16::new(1).unwrap(),
            )
            .unwrap();
        assert!(matches!(
            second.items[0].cursor,
            Cursor::StagingWindow {
                window_index: 0,
                ..
            }
        ));
        assert_ne!(first.items[0].reference, second.items[0].reference);
        let end = pg
            .list_shard_scavenger_reference_page(
                &topology,
                Some(&second.items[0].cursor),
                NonZeroU16::new(1).unwrap(),
            )
            .unwrap();
        assert!(end.complete && end.items.is_empty());
        let backfill = pg
            .list_placed_shard_scavenger_reference_page(None, NonZeroU16::new(64).unwrap())
            .unwrap();
        assert!(backfill.complete);
        assert_eq!(backfill.items.len(), 1);
        assert_eq!(backfill.items[0].reference, selected);
    }
}

#[test]
fn staging_pending_paging_crossed_cursor_and_missing_or_corrupt_index_fail_closed() {
    let owner = fixture(StagingTarget::DirectPut);
    let command = introduction::command(&owner);
    let dir = test_util::tempdir();
    let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
    install(&pg, &owner, &command);
    let page = pg
        .list_shard_scavenger_reference_page(&topology(), None, NonZeroU16::new(1).unwrap())
        .unwrap();
    let cursor = &page.items[0].cursor;
    for field in ["PG", "topology", "zero index", "large index", "large epoch"] {
        let mut bad = cursor.clone();
        let Cursor::PendingStagingWindow {
            pg_id,
            topology_digest,
            log_index,
            cluster_epoch,
            ..
        } = &mut bad
        else {
            unreachable!();
        };
        match field {
            "PG" => *pg_id = PgId::new(pg_id.get() + 1),
            "topology" => topology_digest[0] ^= 1,
            "zero index" => *log_index = 0,
            "large index" => *log_index = u64::MAX,
            "large epoch" => *cluster_epoch = ClusterEpoch::new(u64::MAX).unwrap(),
            _ => unreachable!(),
        }
        let writes = pg.conn.total_changes();
        assert!(
            pg.list_shard_scavenger_reference_page(
                &topology(),
                Some(&bad),
                NonZeroU16::new(1).unwrap()
            )
            .is_err(),
            "{field}"
        );
        assert!(pg.conn.is_autocommit());
        assert_eq!(pg.conn.total_changes(), writes);
    }
    for mutation in ["UPDATE metadata_command_pending_staging_protection SET protection_checksum = protection_checksum + 1", "DELETE FROM metadata_command_pending_staging_protection"] {
        pg.conn.execute(mutation, []).unwrap();
        for after in [None, Some(cursor)] {
            assert!(pg.list_shard_scavenger_reference_page(&topology(), after, NonZeroU16::new(1).unwrap()).is_err());
        }
    }
}

#[test]
fn staging_pending_paging_installed_unix_matches_local_and_rejects_missing_index() {
    use crate::node_runtime::client_facade::{
        LocalStorageNodeClient, ShardScavengerNodeClient, UnixStorageNodeClient,
    };
    use crate::storage_node_server::{
        StorageNodePgRoute, StorageNodeProcessConfig, StorageNodeServer,
    };
    use crate::{ObjectMetadataScanPgId, PgState, RouteMapValidity};
    use std::os::unix::fs::PermissionsExt;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    struct Guard {
        stop: Arc<AtomicBool>,
        socket: std::path::PathBuf,
        thread: Option<std::thread::JoinHandle<()>>,
    }
    impl Drop for Guard {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Release);
            let _ = std::os::unix::net::UnixStream::connect(&self.socket);
            if let Some(thread) = self.thread.take() {
                let result = thread.join();
                if !std::thread::panicking() {
                    result.unwrap();
                }
            }
        }
    }
    let dir = test_util::tempdir();
    let socket_dir = dir.path().join("sock");
    std::fs::create_dir(&socket_dir).unwrap();
    std::fs::set_permissions(&socket_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    let epoch = ClusterEpoch::new(6).unwrap();
    let config = StorageNodeProcessConfig {
        node_id: NodeId::new(7),
        cluster_epoch: epoch,
        route_map_validity: RouteMapValidity::Forever,
        data_dir: dir.path().join("node"),
        default_ec_shape: EcShape { k: 2, m: 1 },
        pg_ids: (0..8).collect(),
        socket_path: socket_dir.join("storage.sock"),
        pg_routes: (0..8)
            .map(|pg_id| StorageNodePgRoute {
                pg_id,
                cluster_epoch: epoch,
                state: PgState::Active,
                primary_node_id: NodeId::new(7),
                acting_set: vec![NodeId::new(7)],
                metadata_transfer_destination_epoch: None,
                metadata_read_route: None,
            })
            .collect(),
        pending_metadata_command_recoveries: vec![],
        historical_pg_routes: vec![],
    };
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let node = server.test_storage_node();
    let owner = fixture(StagingTarget::DirectPut);
    install(
        &node.get_pg(owner.subject.object_pg.get()).unwrap(),
        &owner,
        &introduction::command(&owner),
    );
    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = stop.clone();
    let _guard = Guard {
        stop,
        socket: config.socket_path.clone(),
        thread: Some(std::thread::spawn(move || loop {
            let result = server.accept_one();
            if thread_stop.load(Ordering::Acquire) {
                break;
            }
            result.unwrap();
        })),
    };
    let local = LocalStorageNodeClient::new(config.node_id, node.clone());
    let unix = UnixStorageNodeClient::new(config.node_id, epoch, config.socket_path);
    let pg_id = ObjectMetadataScanPgId::new_for_test(owner.subject.object_pg);
    let local_route = local
        .open_shard_scavenger_object_scan_route(epoch, pg_id)
        .unwrap();
    let unix_route = unix
        .open_shard_scavenger_object_scan_route(epoch, pg_id)
        .unwrap();
    let expected = local_route
        .list_shard_scavenger_reference_page(None, NonZeroU16::new(1).unwrap())
        .unwrap();
    assert!(matches!(
        expected.items[0].cursor,
        Cursor::PendingStagingWindow { .. }
    ));
    assert_eq!(
        unix_route
            .list_shard_scavenger_reference_page(None, NonZeroU16::new(1).unwrap())
            .unwrap(),
        expected
    );
    let cursor = &expected.items[0].cursor;
    for route in [&local_route, &unix_route] {
        let end = route
            .list_shard_scavenger_reference_page(Some(cursor), NonZeroU16::new(1).unwrap())
            .unwrap();
        assert!(end.complete && end.items.is_empty());
        assert!(route
            .list_placed_shard_scavenger_reference_page(None, NonZeroU16::new(64).unwrap())
            .unwrap()
            .items
            .is_empty());
    }
    node.get_pg(owner.subject.object_pg.get())
        .unwrap()
        .conn
        .execute(
            "DELETE FROM metadata_command_pending_staging_protection",
            [],
        )
        .unwrap();
    for route in [&local_route, &unix_route] {
        assert!(route
            .list_shard_scavenger_reference_page(Some(cursor), NonZeroU16::new(1).unwrap())
            .is_err());
    }
}
