// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Owner-local protection discovery, installed RPC, and production audit tests.
//! Responsibility does not authorize physical deletion or repair/backfill.

use super::super::super::super::inventory::{
    protection_page, InventoryError, ProtectedWindow, ProtectionCursor, ProtectionPage,
};
use super::*;
use crate::{types::ShardScavengerPayloadReference, ShardKey};
use std::num::NonZeroU16;

fn scan(store: &PgStore, limit: u16) -> Vec<ProtectedWindow> {
    let mut cursor = None;
    let mut windows = Vec::new();
    loop {
        store.conn.execute_batch("BEGIN").unwrap();
        let page = protection_page(
            &store.conn,
            PgId::new(store.pg_id()),
            &topology(),
            cursor.as_ref(),
            NonZeroU16::new(limit).unwrap(),
        )
        .unwrap();
        store.conn.execute_batch("COMMIT").unwrap();
        assert!(page.windows.len() <= usize::from(limit.min(64)));
        if !page.complete {
            assert!(!page.windows.is_empty(), "incomplete scan cannot advance");
        }
        if let Some(last) = page.windows.last() {
            assert_ne!(Some(&last.cursor), cursor.as_ref());
            cursor = Some(last.cursor.clone());
        }
        windows.extend(page.windows);
        if page.complete {
            return windows;
        }
    }
}

fn assert_exact_windows(actual: &[ProtectedWindow], owner: &StagingOwnerRecord) {
    assert_eq!(actual.len(), owner.windows.len());
    for (actual, window) in actual.iter().zip(&owner.windows) {
        assert_eq!(
            actual.selected.as_ref(),
            owner
                .references
                .iter()
                .find(|r| r.window_index == window.index)
        );
        for (index, node) in window.participants.files.iter().enumerate() {
            let key = ShardKey::new(&window.segment_hash, window.start.get(), index as u8);
            assert!(actual.protects_file(
                window.participants.data_pg,
                window.participants.placement_epoch,
                *node,
                &key,
            ));
            assert!(actual.protects_acknowledgement(
                window.participants.data_pg,
                window.participants.placement_epoch,
                window.participants.acknowledgement_primary,
                &key,
            ));
        }
    }
}

fn assert_protection_references(
    actual: &[ShardScavengerPayloadReference],
    owner: &StagingOwnerRecord,
) {
    assert_eq!(actual.len(), owner.windows.len());
    for (actual, window) in actual.iter().zip(&owner.windows) {
        let ShardScavengerPayloadReference::ProtectionOnly(actual) = actual else {
            panic!("retained scope must not mint readable/backfillable metadata");
        };
        assert_eq!(actual.data_pg_id, window.participants.data_pg.get());
        assert_eq!(actual.okh, window.segment_hash);
        assert_eq!(actual.generation_id, window.start);
        assert_eq!(actual.ec, window.geometry.ec);
    }
}

fn assert_paged_protections(
    items: &[crate::types::ShardScavengerReferencePageItem],
    owner: &StagingOwnerRecord,
) {
    use crate::types::ShardScavengerReferenceCursor;
    assert_eq!(items.len(), owner.windows.len());
    for (item, window) in items.iter().zip(&owner.windows) {
        assert!(
            matches!(&item.cursor, ShardScavengerReferenceCursor::StagingWindow {
            pg_id, attempt, introduction_epoch, introduction_index, window_index, ..
        } if *pg_id == owner.subject.object_pg && attempt == &owner.subject.id.attempt
            && *introduction_epoch == owner.subject.id.introduction.cluster_epoch()
            && *introduction_index == owner.subject.id.introduction.log_index().get()
            && *window_index == window.index)
        );
        let ShardScavengerPayloadReference::StagingProtection(actual) = &item.reference else {
            panic!("not staging protection")
        };
        assert_eq!(
            actual.identity.data_pg_id,
            window.participants.data_pg.get()
        );
        assert_eq!(actual.identity.okh, window.segment_hash);
        assert_eq!(actual.identity.generation_id, window.start);
        assert_eq!(actual.identity.ec, window.geometry.ec);
        assert_eq!(actual.placement_epoch, window.participants.placement_epoch);
        assert_eq!(actual.file_nodes, window.participants.files);
        assert_eq!(
            actual.acknowledgement_primary,
            window.participants.acknowledgement_primary
        );
    }
}

fn paged(store: &PgStore, limit: u16) -> Vec<crate::types::ShardScavengerReferencePageItem> {
    let mut items = Vec::new();
    loop {
        let after = items
            .last()
            .map(|item: &crate::types::ShardScavengerReferencePageItem| &item.cursor);
        let page = store
            .list_shard_scavenger_reference_page(
                &topology(),
                after,
                NonZeroU16::new(limit).unwrap(),
            )
            .unwrap();
        assert!(page.items.len() <= usize::from(limit));
        if !page.complete {
            assert!(!page.items.is_empty());
        }
        if let Some(last) = page.items.last() {
            assert_ne!(Some(&last.cursor), after);
        }
        items.extend(page.items);
        if page.complete {
            return items;
        }
    }
}

#[test]
fn staging_inventory_retains_unused_windows_across_every_lifecycle_and_restart() {
    for target in [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ] {
        let mut active = fixture(target);
        if active.subject.target != StagingTarget::DirectPut {
            append(&mut active);
            append(&mut active);
        }
        let closed = active.closed_unpublished().unwrap();
        let mut published = active.clone();
        publish(&mut published);
        let mut retained = published.clone();
        retained.lifecycle = StagingLifecycle::Retiring(StagingRetirement::PublishedRetained);
        for r in &mut retained.references {
            r.destination = StagingDestination::Reclaim {
                generation: retained.subject.published_generation(),
                part_number: match retained.subject.target {
                    StagingTarget::UploadPart { part_number, .. } => Some(part_number),
                    _ => None,
                },
            };
        }
        let mut unreferenced = retained.clone();
        unreferenced.lifecycle =
            StagingLifecycle::Retiring(StagingRetirement::PublishedUnreferenced);
        unreferenced.references.clear();
        for owner in [active, closed, published, retained, unreferenced] {
            let tmp = test_util::tempdir();
            {
                let store = PgStore::open(tmp.path(), owner.subject.object_pg.get()).unwrap();
                seed(&store, &owner);
            }
            let store = PgStore::open(tmp.path(), owner.subject.object_pg.get()).unwrap();
            // No other source may mask a discovery query restricted to manifests.
            for table in [
                "objects",
                "stream_uploads",
                "stream_upload_segments",
                "object_segments",
                "multipart_parts",
                "multipart_part_segments",
                "object_generation_reservations",
                "bucket_write_reservations",
                "metadata_command_pending_slot",
                "shards",
            ] {
                let count: u64 = store
                    .conn
                    .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                        row.get(0)
                    })
                    .unwrap();
                assert_eq!(count, 0, "{table}");
            }
            let digest = store.metadata_state_digest().unwrap();
            let writes = store.conn.total_changes();
            let actual = scan(&store, 1);
            assert_exact_windows(&actual, &owner);
            let references = store
                .list_shard_scavenger_payload_references(&topology())
                .unwrap();
            assert_protection_references(&references, &owner);
            let pages = paged(&store, 1);
            assert_paged_protections(&pages, &owner);
            assert!(store.conn.is_autocommit());
            let backfill = store
                .list_placed_shard_scavenger_reference_page(None, NonZeroU16::new(64).unwrap())
                .unwrap();
            assert!(backfill.complete);
            assert!(
                backfill.items.is_empty(),
                "owner-only scope is not readable"
            );
            assert_eq!(store.metadata_state_digest().unwrap(), digest);
            assert_eq!(store.conn.total_changes(), writes);

            let checkpoint = store
                .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
                .unwrap();
            let destination = test_util::tempdir();
            let imported =
                PgStore::open(destination.path(), owner.subject.object_pg.get()).unwrap();
            imported
                .install_metadata_transfer_checkpoint_base(
                    0,
                    ClusterEpoch::new(19).unwrap(),
                    &checkpoint,
                    &topology(),
                )
                .unwrap();
            assert_eq!(scan(&imported, 64), actual);
            assert_eq!(paged(&imported, 64), pages);
            assert_eq!(
                imported
                    .list_shard_scavenger_payload_references(&topology())
                    .unwrap(),
                references
            );
        }
    }
}

#[test]
fn staging_inventory_matches_exact_historical_file_and_independent_ack_participants() {
    let owner = fixture(StagingTarget::StreamPut);
    let window = &owner.windows[0];
    let placement = &window.participants;
    // Intentionally noncanonical NodeId order, with a spare acknowledgement node.
    assert_eq!(placement.files, [2, 0, 3].map(NodeId::new));
    assert!(!placement.files.contains(&placement.acknowledgement_primary));
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), owner.subject.object_pg.get()).unwrap();
    seed(&store, &owner);
    let actual = scan(&store, 64).remove(0);
    assert!(actual.selected.is_none());
    for (index, file_node) in placement.files.iter().enumerate() {
        let key = ShardKey::new(&window.segment_hash, window.start.get(), index as u8);
        let ack_node = placement.acknowledgement_primary;
        let check = |pg, epoch, key: &ShardKey, file_expected, ack_expected| {
            assert_eq!(
                actual.protects_file(pg, epoch, *file_node, key),
                file_expected
            );
            assert_eq!(
                actual.protects_acknowledgement(pg, epoch, ack_node, key),
                ack_expected
            );
        };
        check(
            placement.data_pg,
            placement.placement_epoch,
            &key,
            true,
            true,
        );
        assert!(!actual.protects_file(
            placement.data_pg,
            placement.placement_epoch,
            ack_node,
            &key
        ));
        assert!(!actual.protects_acknowledgement(
            placement.data_pg,
            placement.placement_epoch,
            *file_node,
            &key
        ));
        check(
            PgId::new(placement.data_pg.get() + 1),
            placement.placement_epoch,
            &key,
            false,
            false,
        );
        check(
            placement.data_pg,
            ClusterEpoch::new(placement.placement_epoch.get() + 1).unwrap(),
            &key,
            false,
            false,
        );
        let mut hash = window.segment_hash;
        hash[0] ^= 1;
        for crossed in [
            ShardKey::new(&hash, window.start.get(), index as u8),
            ShardKey::new(&window.segment_hash, window.start.get() - 1, index as u8),
            ShardKey::new(&window.segment_hash, window.end.get(), index as u8),
            ShardKey::new(
                &window.segment_hash,
                window.start.get(),
                placement.files.len() as u8,
            ),
        ] {
            check(
                placement.data_pg,
                placement.placement_epoch,
                &crossed,
                false,
                false,
            );
        }
        let other_index = (index + 1) % placement.files.len();
        let other_key = ShardKey::new(&window.segment_hash, window.start.get(), other_index as u8);
        check(
            placement.data_pg,
            placement.placement_epoch,
            &other_key,
            false,
            true,
        );
    }
}

fn named_owner(name: &str, generation: u64, segments: usize) -> StagingOwnerRecord {
    let mut owner = fixture(StagingTarget::StreamPut);
    owner.subject.id.attempt = crate::tests::stream_session_id(name);
    owner.subject.payload_generation = GenerationId::new(generation).unwrap();
    owner.windows[0] = window(&owner.subject, 0, owner.subject.first_vid());
    owner.reserved_end = owner.windows[0].end;
    owner.resources.generation = ResourceResponsibility::Live(StagingGenerationResource {
        reservation_id: owner.subject.id.attempt.clone(),
        generation: owner.subject.payload_generation,
    });
    for _ in 0..segments {
        append(&mut owner);
    }
    owner
}

#[test]
fn staging_inventory_pages_without_skipping_unused_windows_or_attempts() {
    let mut owners = [
        named_owner("z-last", 20, 1),
        named_owner("a-first", 21, 67),
        named_owner("m-middle", 22, 0),
    ];
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), owners[0].subject.object_pg.get()).unwrap();
    for owner in &owners {
        seed(&store, owner);
    }
    owners.sort_by(|left, right| {
        left.subject
            .id
            .attempt
            .as_str()
            .cmp(right.subject.id.attempt.as_str())
    });
    let references = store
        .list_shard_scavenger_payload_references(&topology())
        .unwrap();
    let mut offset = 0;
    for owner in &owners {
        assert_protection_references(&references[offset..offset + owner.windows.len()], owner);
        offset += owner.windows.len();
    }
    assert_eq!(references.len(), offset);
    let mut baseline = None;
    for limit in [1, 2, 64, u16::MAX] {
        let actual = scan(&store, limit);
        let mut offset = 0;
        for owner in &owners {
            assert_exact_windows(&actual[offset..offset + owner.windows.len()], owner);
            offset += owner.windows.len();
        }
        assert_eq!(actual.len(), offset);
        if let Some(baseline) = &baseline {
            assert_eq!(&actual, baseline);
        } else {
            baseline = Some(actual);
        }
    }
}

fn read_page(
    store: &PgStore,
    cursor: Option<&ProtectionCursor>,
) -> Result<ProtectionPage, InventoryError> {
    protection_page(
        &store.conn,
        PgId::new(store.pg_id()),
        &topology(),
        cursor,
        NonZeroU16::new(1).unwrap(),
    )
}

#[test]
fn staging_inventory_rejects_crossed_cursor_and_replaced_introduction() {
    let owner = fixture(StagingTarget::StreamPut);
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), owner.subject.object_pg.get()).unwrap();
    seed(&store, &owner);
    assert!(matches!(
        read_page(&store, None),
        Err(InventoryError::Persistence(
            sql::PersistenceError::TransactionRequired
        ))
    ));
    let cursor = scan(&store, 1).remove(0).cursor;
    store.conn.execute_batch("BEGIN").unwrap();
    for (pg, configured_topology) in [
        (PgId::new(owner.subject.object_pg.get() + 1), topology()),
        (
            owner.subject.object_pg,
            PgTopology::new(&[owner.subject.object_pg.get()]).unwrap(),
        ),
    ] {
        assert!(matches!(
            protection_page(
                &store.conn,
                pg,
                &configured_topology,
                Some(&cursor),
                NonZeroU16::new(1).unwrap()
            ),
            Err(InventoryError::CursorDomain)
        ));
    }
    store
        .conn
        .execute(
            "UPDATE payload_staging_owners SET introduction_index = introduction_index + 1",
            [],
        )
        .unwrap();
    assert!(matches!(
        read_page(&store, Some(&cursor)),
        Err(InventoryError::CursorReplaced)
    ));
    store.conn.execute_batch("ROLLBACK").unwrap();
    assert!(scan(&store, 64)[0].cursor == cursor);
}

#[test]
fn staging_inventory_rejects_malformed_or_unfenced_owners_without_partial_results() {
    for mutation in [
        "DELETE FROM payload_staging_windows",
        "DELETE FROM payload_staging_participants WHERE shard_index = 1",
        "UPDATE payload_staging_windows SET data_pg = 99",
        "UPDATE payload_staging_owners SET object_pg = 99",
        "DELETE FROM object_write_counters",
        "UPDATE object_write_counters SET max_committed_generation = 1",
    ] {
        let owner = fixture(StagingTarget::StreamPut);
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), owner.subject.object_pg.get()).unwrap();
        seed(&store, &owner);
        store.conn.execute_batch("BEGIN").unwrap();
        store.conn.execute_batch(mutation).unwrap();
        let writes = store.conn.total_changes();
        assert!(read_page(&store, None).is_err(), "{mutation}");
        assert!(
            matches!(
                store.list_shard_scavenger_payload_references(&topology()),
                Err(StoreError::ShardScavengerScanIncomplete {
                    context: "load retained staging protection references",
                    ..
                })
            ),
            "{mutation}"
        );
        assert!(
            !store.conn.is_autocommit(),
            "must preserve caller's transaction"
        );
        assert_eq!(store.conn.total_changes(), writes);
        store.conn.execute_batch("ROLLBACK").unwrap();
        assert_exact_windows(&scan(&store, 64), &owner);
    }
}

#[test]
fn staging_inventory_child_rows_share_the_callers_read_snapshot() {
    let owner = fixture(StagingTarget::StreamPut);
    let tmp = test_util::tempdir();
    let reader = PgStore::open(tmp.path(), owner.subject.object_pg.get()).unwrap();
    seed(&reader, &owner);
    let writer = PgStore::open(tmp.path(), owner.subject.object_pg.get()).unwrap();
    reader.conn.execute_batch("BEGIN").unwrap();
    // Establish the caller's snapshot, then commit different child placement
    // through another connection. No sleeps or process scheduling are needed.
    let introduction: u64 = reader
        .conn
        .query_row(
            "SELECT introduction_index FROM payload_staging_owners",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        introduction,
        owner.subject.id.introduction.log_index().get()
    );
    writer
        .conn
        .execute_batch(
            "BEGIN IMMEDIATE; \
         UPDATE payload_staging_windows SET placement_epoch = 17, acknowledgement_primary = 9; \
         UPDATE payload_staging_participants SET node_id = node_id + 10; \
         COMMIT",
        )
        .unwrap();
    let old = read_page(&reader, None).unwrap();
    assert_exact_windows(&old.windows, &owner);
    reader.conn.execute_batch("COMMIT").unwrap();
    let current = scan(&reader, 64);
    let mut changed = owner.clone();
    changed.windows[0].participants.placement_epoch = ClusterEpoch::new(17).unwrap();
    changed.windows[0].participants.acknowledgement_primary = NodeId::new(9);
    for node in &mut changed.windows[0].participants.files {
        *node = NodeId::new(node.as_u32() + 10);
    }
    assert_exact_windows(&current, &changed);
    assert_ne!(current, old.windows);
}

#[test]
fn staging_unpaged_inventory_errors_discard_the_prefix_and_release_only_owned_transactions() {
    let mut owners = [
        named_owner("prefix-one", 30, 0),
        named_owner("prefix-two", 31, 0),
    ];
    owners.sort_by(|a, b| {
        a.subject
            .id
            .attempt
            .as_str()
            .cmp(b.subject.id.attempt.as_str())
    });
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), owners[0].subject.object_pg.get()).unwrap();
    for owner in &owners {
        seed(&store, owner);
    }
    // The later owner is malformed, after the first protection was collected.
    store
        .conn
        .execute(
            "DELETE FROM payload_staging_windows WHERE attempt = ?1",
            [&owners[1].subject.id.attempt],
        )
        .unwrap();
    let assert_failure = || {
        assert!(matches!(
            store.list_shard_scavenger_payload_references(&topology()),
            Err(StoreError::ShardScavengerScanIncomplete {
                context: "load retained staging protection references",
                ..
            })
        ));
    };
    assert_failure();
    assert!(
        store.conn.is_autocommit(),
        "owned read transaction must roll back"
    );
    store.conn.execute_batch("BEGIN").unwrap();
    store
        .conn
        .execute(
            "UPDATE object_write_counters SET max_committed_generation = 100",
            [],
        )
        .unwrap();
    let writes = store.conn.total_changes();
    assert_failure();
    assert!(
        !store.conn.is_autocommit(),
        "must not end the enclosing transaction"
    );
    assert_eq!(store.conn.total_changes(), writes);
    let fence: u64 = store
        .conn
        .query_row(
            "SELECT max_committed_generation FROM object_write_counters",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(fence, 100, "unrelated outer work must remain intact");
    store.conn.execute_batch("ROLLBACK").unwrap();
}

#[test]
fn staging_unpaged_inventory_embedded_and_installed_unix_preserve_protection_not_backfill() {
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

    struct ServerGuard {
        stop: Arc<AtomicBool>,
        socket: std::path::PathBuf,
        thread: Option<std::thread::JoinHandle<()>>,
    }
    impl Drop for ServerGuard {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Release);
            let _ = std::os::unix::net::UnixStream::connect(&self.socket);
            if let Some(thread) = self.thread.take() {
                if let Err(panic) = thread.join() {
                    if !std::thread::panicking() {
                        std::panic::resume_unwind(panic);
                    }
                }
            }
        }
    }

    let tmp = test_util::tempdir();
    let socket_dir = tmp.path().join("sock");
    std::fs::create_dir(&socket_dir).unwrap();
    std::fs::set_permissions(&socket_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    let epoch = ClusterEpoch::new(6).unwrap();
    let config = StorageNodeProcessConfig {
        node_id: NodeId::new(7),
        cluster_epoch: epoch,
        route_map_validity: RouteMapValidity::Forever,
        data_dir: tmp.path().join("node"),
        default_ec_shape: EcShape { k: 2, m: 1 },
        pg_ids: (0..8).collect(),
        socket_path: socket_dir.join("storage.sock"),
        pg_routes: (0..8)
            .map(|pg_id| StorageNodePgRoute {
                pg_id,
                cluster_epoch: epoch,
                state: PgState::Active,
                primary_node_id: NodeId::new(7),
                metadata_transfer_destination_epoch: None,
                metadata_read_route: None,
                acting_set: vec![NodeId::new(7)],
            })
            .collect(),
        pending_metadata_command_recoveries: Vec::new(),
        historical_pg_routes: Vec::new(),
    };
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let node = server.test_storage_node();
    // Exercise more than one owner-local page, with a used prefix and unused
    // lookahead. The response must retain all of it after terminal closure.
    let owner = named_owner("unix-inventory", 30, 67)
        .closed_unpublished()
        .unwrap();
    seed(&node.get_pg(owner.subject.object_pg.get()).unwrap(), &owner);
    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = Arc::clone(&stop);
    let _guard = ServerGuard {
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
    let local = LocalStorageNodeClient::new(config.node_id, Arc::clone(&node));
    let unix = UnixStorageNodeClient::new(config.node_id, epoch, config.socket_path);
    let scan_pg = ObjectMetadataScanPgId::new_for_test(owner.subject.object_pg);
    for client in [&local as &dyn ShardScavengerNodeClient, &unix] {
        let route = client
            .open_shard_scavenger_object_scan_route(epoch, scan_pg)
            .unwrap();
        let actual = route.list_shard_scavenger_payload_references().unwrap();
        assert_protection_references(&actual, &owner);
        let mut pages = Vec::new();
        loop {
            let after = pages
                .last()
                .map(|item: &crate::types::ShardScavengerReferencePageItem| &item.cursor);
            let page = route
                .list_shard_scavenger_reference_page(after, NonZeroU16::new(64).unwrap())
                .unwrap();
            assert!(page.items.len() <= 64);
            if !page.complete {
                assert!(!page.items.is_empty());
            }
            pages.extend(page.items);
            if page.complete {
                break;
            }
        }
        assert_paged_protections(&pages, &owner);
        let backfill = route
            .list_placed_shard_scavenger_reference_page(None, NonZeroU16::new(64).unwrap())
            .unwrap();
        assert!(backfill.complete);
        assert!(
            backfill.items.is_empty(),
            "protection must not invent a backfill source"
        );
    }
    // A malformed materialized owner must fail the real installed response,
    // not silently yield an incomplete successful list.
    node.get_pg(owner.subject.object_pg.get())
        .unwrap()
        .conn
        .execute(
            "UPDATE payload_staging_windows SET data_pg = 99 WHERE window_index = 67",
            [],
        )
        .unwrap();
    assert!(matches!(
        local
            .open_shard_scavenger_object_scan_route(epoch, scan_pg)
            .unwrap()
            .list_shard_scavenger_payload_references(),
        Err(StoreError::ShardScavengerScanIncomplete { .. })
    ));
    let error = unix
        .open_shard_scavenger_object_scan_route(epoch, scan_pg)
        .unwrap()
        .list_shard_scavenger_payload_references()
        .unwrap_err();
    assert!(
        matches!(&error, StoreError::StorageRpc {
        failure: crate::storage_rpc::StorageRpcErrorCode::Internal,
        detail,
        ..
    } if detail.as_str().contains("load retained staging protection references")),
        "{error:?}"
    );
    for client in [&local as &dyn ShardScavengerNodeClient, &unix] {
        let route = client
            .open_shard_scavenger_object_scan_route(epoch, scan_pg)
            .unwrap();
        assert!(
            route
                .list_shard_scavenger_reference_page(None, NonZeroU16::new(64).unwrap())
                .is_err(),
            "a malformed last window must invalidate its complete owner, not emit a prefix"
        );
    }
}

#[test]
fn staging_paged_inventory_rejects_crossed_or_replaced_cursors_without_writes() {
    let owner = named_owner("paged-cursor", 30, 2);
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), owner.subject.object_pg.get()).unwrap();
    seed(&store, &owner);
    let page = store
        .list_shard_scavenger_reference_page(&topology(), None, NonZeroU16::new(1).unwrap())
        .unwrap();
    assert!(!page.complete);
    let cursor = &page.items[0].cursor;
    for case in 0..4 {
        let mut cursor = cursor.clone();
        let crate::types::ShardScavengerReferenceCursor::StagingWindow {
            pg_id,
            topology_digest,
            introduction_epoch,
            introduction_index,
            ..
        } = &mut cursor
        else {
            unreachable!()
        };
        match case {
            0 => *pg_id = PgId::new(pg_id.get() + 1),
            1 => topology_digest[0] ^= 1,
            2 => *introduction_epoch = ClusterEpoch::new(introduction_epoch.get() + 1).unwrap(),
            3 => *introduction_index += 1,
            _ => unreachable!(),
        }
        let writes = store.conn.total_changes();
        assert!(matches!(
            store.list_shard_scavenger_reference_page(
                &topology(),
                Some(&cursor),
                NonZeroU16::new(1).unwrap()
            ),
            Err(StoreError::ShardScavengerScanIncomplete { .. })
        ));
        assert!(store.conn.is_autocommit());
        assert_eq!(store.conn.total_changes(), writes);
    }
    store.conn.execute_batch("BEGIN").unwrap();
    let backfill = store
        .list_placed_shard_scavenger_reference_page(Some(cursor), NonZeroU16::new(64).unwrap())
        .unwrap();
    assert!(backfill.complete && backfill.items.is_empty());
    store
        .list_shard_scavenger_reference_page(&topology(), Some(cursor), NonZeroU16::new(1).unwrap())
        .unwrap();
    assert!(!store.conn.is_autocommit());
    store.conn.execute_batch("ROLLBACK").unwrap();
}

#[test]
fn staging_paged_audit_protects_original_files_and_independent_acks_without_inventing_repairs() {
    use crate::types::ShardScavengerObservationReason;
    for ack_is_current in [true, false] {
        let tmp = test_util::tempdir();
        let nodes = [0, 1, 2, 3].map(NodeId::new);
        let map = std::sync::Arc::new(
            crate::LocalClusterMap::open(
                tmp.path(),
                &nodes,
                &(0..8).collect::<Vec<_>>(),
                EcShape { k: 2, m: 1 },
            )
            .unwrap(),
        );
        let cluster =
            crate::StorageCluster::from_static_local_map(std::sync::Arc::clone(&map)).unwrap();
        let mut owner = fixture(StagingTarget::StreamPut)
            .closed_unpublished()
            .unwrap();
        let window = &mut owner.windows[0];
        let data_pg = window.participants.data_pg;
        let primary = map
            .metadata_pg_primary_node(ClusterEpoch::INITIAL, data_pg)
            .unwrap();
        let primary_id = primary.node_id();
        let files: Vec<_> = nodes
            .into_iter()
            .rev()
            .filter(|node| *node != primary_id)
            .collect();
        window.participants.files = files.clone();
        window.participants.acknowledgement_primary =
            if ack_is_current { primary_id } else { files[0] };
        assert_ne!(window.participants.placement_epoch, ClusterEpoch::INITIAL);
        let key0 = ShardKey::new(&window.segment_hash, window.start.get(), 0);
        let key1 = ShardKey::new(&window.segment_hash, window.start.get(), 1);
        let origin = map.node(files[0]).unwrap().test_node();
        let ack = {
            let pg = origin.get_pg(data_pg.get()).unwrap();
            PgStore::write_shard_file_durable(
                &pg.tmp_dir,
                &pg.shards_dir,
                &key0,
                b"scope need not contain a readable payload",
            )
            .unwrap()
        };
        // Original file exists without an ack; another index has an ack but no
        // file anywhere. The third index has never been written.
        primary
            .test_node()
            .get_pg(data_pg.get())
            .unwrap()
            .register_written_shard(&key1, ack)
            .unwrap();
        // Same key at the wrong file participant must NOT inherit protection.
        {
            let pg = primary.test_node().get_pg(data_pg.get()).unwrap();
            PgStore::write_shard_file_durable(
                &pg.tmp_dir,
                &pg.shards_dir,
                &key0,
                b"crossed location",
            )
            .unwrap();
        }
        let object_node = map
            .metadata_pg_primary_node(ClusterEpoch::INITIAL, owner.subject.object_pg)
            .unwrap();
        seed(
            &object_node
                .test_node()
                .get_pg(owner.subject.object_pg.get())
                .unwrap(),
            &owner,
        );
        let observations = cluster.audit_shard_storage_for_scavenger().unwrap();
        let unresolved: Vec<_> = observations
            .iter()
            .filter(|o| o.resolved_at.is_none())
            .collect();
        assert!(!unresolved
            .iter()
            .any(|o| o.key.node_id == files[0].as_u32() && o.key.shard_key == key0));
        assert!(unresolved
            .iter()
            .any(|o| o.key.node_id == primary_id.as_u32()
                && o.key.shard_key == key0
                && o.reason == ShardScavengerObservationReason::FileWithoutShardRow));
        assert_eq!(
            unresolved.iter().any(|o| o.key.shard_key == key1
                && o.reason == ShardScavengerObservationReason::ShardRowWithoutFile),
            !ack_is_current
        );
        assert!(cluster
            .list_placed_segment_shard_repairs(data_pg.get())
            .unwrap()
            .is_empty());
    }
}
