// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::cell::Cell;

fn row_status(store: &PgStore, key: &ShardKey) -> Option<i64> {
    store
        .conn
        .query_row(
            "SELECT status FROM shards WHERE shard_key = ?1",
            params![key.as_bytes().as_slice()],
            |row| row.get(0),
        )
        .optional()
        .unwrap()
}

#[test]
fn shard_deletion_sync_failure_preserves_row_and_retry_repeats_barrier() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 0).unwrap();
    let key = ShardKey::new(&[0xE1; 16], 17, 0);
    store.write_shard(&key, b"durable deletion").unwrap();
    assert_eq!(store.read_shard(&key).unwrap().data, b"durable deletion");
    let shard_path = store.shard_path(&key);
    let error = store
        .delete_shard_with_sync(&key, |path, _| {
            assert_eq!(path, shard_path.parent().unwrap());
            assert!(!shard_path.exists(), "sync follows unlink");
            assert_eq!(row_status(&store, &key), Some(ShardStatus::Deleting as i64));
            Err(std::io::Error::other("injected deletion sync failure"))
        })
        .unwrap_err();
    assert!(matches!(
        error,
        StoreError::Io {
            context: "fsync shard deletion directory",
            ..
        }
    ));
    assert!(!shard_path.exists());
    assert_eq!(row_status(&store, &key), Some(ShardStatus::Deleting as i64));
    drop(store);

    // Reopening is not a power-loss simulator: it pins retention of retry
    // bookkeeping and the repeated barrier despite the live namespace's ENOENT.
    let reopened = PgStore::open(tmp.path(), 0).unwrap();
    assert_eq!(
        row_status(&reopened, &key),
        Some(ShardStatus::Deleting as i64)
    );
    let synced = Cell::new(false);
    reopened
        .delete_shard_with_sync(&key, |path, directory| {
            assert_eq!(path, shard_path.parent().unwrap());
            assert!(!shard_path.exists());
            assert_eq!(
                row_status(&reopened, &key),
                Some(ShardStatus::Deleting as i64)
            );
            directory.sync_all()?;
            synced.set(true);
            Ok(())
        })
        .unwrap();
    assert!(synced.get());
    assert_eq!(row_status(&reopened, &key), None);
    reopened.delete_shard(&key).unwrap();
    drop(reopened);
    let reopened = PgStore::open(tmp.path(), 0).unwrap();
    assert_eq!(row_status(&reopened, &key), None);
    assert!(!shard_path.exists());
}

#[test]
fn shard_deletion_without_prefix_syncs_root_on_every_retry() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 0).unwrap();
    let key = ShardKey::new(&[0xE2; 16], 18, 0);
    let shard_path = store.shard_path(&key);
    assert!(!shard_path.parent().unwrap().exists());

    for _ in 0..2 {
        let error =
            PgStore::delete_shard_file_durable_with_sync(&store.shards_dir, &key, |path, _| {
                assert_eq!(path, store.shards_dir);
                Err(std::io::Error::other("injected root sync failure"))
            })
            .unwrap_err();
        assert!(matches!(
            error,
            StoreError::Io {
                context: "fsync shard deletion directory",
                ..
            }
        ));
    }
    let synced = Cell::new(false);
    PgStore::delete_shard_file_durable_with_sync(&store.shards_dir, &key, |path, directory| {
        assert_eq!(path, store.shards_dir);
        directory.sync_all()?;
        synced.set(true);
        Ok(())
    })
    .unwrap();
    assert!(synced.get());
    assert!(!shard_path.parent().unwrap().exists());
    assert_eq!(row_status(&store, &key), None);

    // Missing PG storage is not durable absence evidence.
    let error =
        PgStore::delete_shard_file_durable(&tmp.path().join("missing-pg"), &key).unwrap_err();
    assert!(matches!(
        error,
        StoreError::Io {
            context: "open shard root for deletion sync",
            ..
        }
    ));
}

#[test]
fn shard_deletion_concurrent_enoent_establishes_its_own_barrier() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 0).unwrap();
    let key = ShardKey::new(&[0xE3; 16], 19, 0);
    store.write_shard(&key, b"two deleters").unwrap();
    let shard_path = store.shard_path(&key);
    let shards_dir = store.shards_dir.clone();
    let first_key = key.clone();
    let first_path = shard_path.clone();
    let (unlinked_tx, unlinked_rx) = std::sync::mpsc::channel();
    let (finish_tx, finish_rx) = std::sync::mpsc::channel();
    let first = std::thread::spawn(move || {
        PgStore::delete_shard_file_durable_with_sync(&shards_dir, &first_key, |path, _| {
            assert_eq!(path, first_path.parent().unwrap());
            assert!(!first_path.exists());
            unlinked_tx.send(()).unwrap();
            finish_rx
                .recv_timeout(std::time::Duration::from_secs(10))
                .unwrap();
            Err(std::io::Error::other("first deleter never synced"))
        })
    });
    unlinked_rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .unwrap();
    let synced = Cell::new(false);
    PgStore::delete_shard_file_durable_with_sync(&store.shards_dir, &key, |path, directory| {
        assert_eq!(path, shard_path.parent().unwrap());
        assert!(!shard_path.exists());
        directory.sync_all()?;
        synced.set(true);
        Ok(())
    })
    .unwrap();
    assert!(synced.get());
    finish_tx.send(()).unwrap();
    assert!(matches!(
        first.join().unwrap(),
        Err(StoreError::Io {
            context: "fsync shard deletion directory",
            ..
        })
    ));
    // File-only deletion does not discharge the independent acknowledgement row.
    assert_eq!(row_status(&store, &key), Some(ShardStatus::Live as i64));
}

#[test]
fn shard_deletion_unlink_error_does_not_retire_row() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 0).unwrap();
    let key = ShardKey::new(&[0xE4; 16], 20, 0);
    store.write_shard(&key, b"blocked unlink").unwrap();
    let shard_path = store.shard_path(&key);
    fs::remove_file(&shard_path).unwrap();
    fs::create_dir(&shard_path).unwrap();
    let error = store
        .delete_shard_with_sync(&key, |_, _| panic!("unlink failure must stop before sync"))
        .unwrap_err();
    assert!(matches!(
        error,
        StoreError::Io {
            context: "unlink shard file",
            ..
        }
    ));
    assert_eq!(row_status(&store, &key), Some(ShardStatus::Deleting as i64));
}

#[test]
fn shard_deletion_rejects_symlinked_prefix_without_touching_target() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 0).unwrap();
    let key = ShardKey::new(&[0xE5; 16], 21, 0);
    let shard_path = store.shard_path(&key);
    let outside = tmp.path().join("outside");
    fs::create_dir(&outside).unwrap();
    let outside_file = outside.join(shard_path.file_name().unwrap());
    fs::write(&outside_file, b"untouched").unwrap();
    std::os::unix::fs::symlink(&outside, shard_path.parent().unwrap()).unwrap();
    assert!(matches!(
        PgStore::delete_shard_file_durable(&store.shards_dir, &key),
        Err(StoreError::Io {
            context: "open shard prefix for deletion sync",
            ..
        })
    ));
    assert_eq!(fs::read(outside_file).unwrap(), b"untouched");
}
