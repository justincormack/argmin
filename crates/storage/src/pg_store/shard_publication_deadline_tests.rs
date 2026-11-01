// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn write(
    store: &PgStore,
    key: &ShardKey,
    data: &[u8],
    if_absent: bool,
    fence: AdmittedRouteEffectFence,
) -> Result<WriteAck, StoreError> {
    if if_absent {
        PgStore::write_shard_file_durable_if_absent_with_effect_fence(
            &store.tmp_dir,
            &store.shards_dir,
            key,
            data,
            fence,
        )
    } else {
        PgStore::write_shard_file_durable_with_effect_fence(
            &store.tmp_dir,
            &store.shards_dir,
            key,
            data,
            fence,
        )
    }
}

#[test]
fn shard_publication_expiry_preserves_final_files_and_rows_and_removes_own_temp() {
    for if_absent in [false, true] {
        for occupied in [false, true] {
            let time = crate::clock::test_time_override_guard(1_000);
            let dir = test_util::tempdir();
            let store = PgStore::open(dir.path(), 0).unwrap();
            let key = ShardKey::new(&[0x51; 16], 1, 0);
            let canary = ShardKey::new(&[0x52; 16], 1, 0);
            let canary_ack = store.write_shard(&canary, b"independent shard").unwrap();
            let old_ack = occupied.then(|| store.write_shard(&key, b"original shard").unwrap());
            let data = if occupied && if_absent {
                b"original shard".as_slice()
            } else {
                b"new shard".as_slice()
            };
            let before_file = occupied.then(|| fs::read(store.shard_path(&key)).unwrap());
            let changes = store.conn.total_changes();
            let tmp_dir = store.tmp_dir.clone();
            // An unrelated leftover temporary file must not be removed by this
            // attempt's error cleanup.
            let residue = tmp_dir.join("unrelated-temp");
            fs::write(&residue, b"independent temporary file").unwrap();
            let control = time.control();
            let error = PgStore::test_with_before_shard_publication_hook(
                move || {
                    assert_eq!(
                        fs::read_dir(&tmp_dir).unwrap().count(),
                        2,
                        "hook must run after the attempt wrote its temporary payload"
                    );
                    control.set(4_500);
                },
                || {
                    write(
                        &store,
                        &key,
                        data,
                        if_absent,
                        AdmittedRouteEffectFence::bounded(ClusterEpoch::INITIAL, 5_000, 4_000),
                    )
                },
            )
            .unwrap_err();
            assert!(
                matches!(
                    error,
                    StoreError::RouteMapExpired {
                        valid_until_ms: 5_000,
                        now_ms: 4_500,
                        ..
                    }
                ),
                "if_absent={if_absent} occupied={occupied}: {error:?}"
            );
            assert_eq!(fs::read(store.shard_path(&key)).ok(), before_file);
            assert_eq!(store.conn.total_changes(), changes);
            assert_eq!(fs::read_dir(&store.tmp_dir).unwrap().count(), 1);
            assert_eq!(fs::read(&residue).unwrap(), b"independent temporary file");
            store
                .validate_written_shard_ack(&canary, canary_ack)
                .unwrap();
            assert_eq!(
                fs::read(store.shard_path(&canary)).unwrap(),
                b"independent shard"
            );
            if let Some(ack) = old_ack {
                store.validate_written_shard_ack(&key, ack).unwrap();
            } else {
                assert!(matches!(store.stat_shard(&key), Err(StoreError::NotFound)));
            }
            let ack = write(
                &store,
                &key,
                data,
                if_absent,
                AdmittedRouteEffectFence::bounded(ClusterEpoch::INITIAL, 20_000, 19_000),
            )
            .unwrap();
            assert_eq!(ack.stored_size, data.len() as u64);
            assert_eq!(ack.crc64, checksum::crc64::checksum(data));
            assert_eq!(fs::read(store.shard_path(&key)).unwrap(), data);
            assert_eq!(
                store.conn.total_changes(),
                changes,
                "file publication does not register an ack row"
            );
            assert_eq!(fs::read_dir(&store.tmp_dir).unwrap().count(), 1);
        }
    }
}

#[test]
fn shard_publication_rechecks_expiry_after_prefix_sync_before_temp_creation() {
    for if_absent in [false, true] {
        let time = crate::clock::test_time_override_guard(1_000);
        let dir = test_util::tempdir();
        let store = PgStore::open(dir.path(), 0).unwrap();
        let key = ShardKey::new(&[0x53; 16], 1, 0);
        let fence = AdmittedRouteEffectFence::bounded(ClusterEpoch::INITIAL, 5_000, 4_000);
        let sync_root = |path: &Path| {
            fsync_dir(path)?;
            time.control().set(4_500);
            Ok(())
        };
        let check = || fence.require_valid_for(ClusterEpoch::INITIAL);
        let error = if if_absent {
            PgStore::write_shard_file_durable_if_absent_checked(
                &store.tmp_dir,
                &store.shards_dir,
                &key,
                b"payload",
                sync_root,
                fsync_dir,
                check,
            )
        } else {
            PgStore::write_shard_file_durable_checked(
                &store.tmp_dir,
                &store.shards_dir,
                &key,
                b"payload",
                sync_root,
                check,
            )
        }
        .unwrap_err();
        assert!(
            matches!(error, StoreError::RouteMapExpired { .. }),
            "{error:?}"
        );
        assert!(!store.shard_path(&key).exists());
        assert!(fs::read_dir(&store.tmp_dir).unwrap().next().is_none());
    }
}

#[test]
fn accepted_shard_link_may_finish_directory_sync_after_cutoff() {
    for occupied in [false, true] {
        let time = crate::clock::test_time_override_guard(1_000);
        let dir = test_util::tempdir();
        let store = PgStore::open(dir.path(), 0).unwrap();
        let key = ShardKey::new(&[0x54; 16], 1, 0);
        let data = b"accepted payload";
        if occupied {
            store.write_shard(&key, data).unwrap();
        }
        let fence = AdmittedRouteEffectFence::bounded(ClusterEpoch::INITIAL, 5_000, 4_000);
        let synced = std::cell::Cell::new(false);
        let ack = PgStore::write_shard_file_durable_if_absent_checked(
            &store.tmp_dir,
            &store.shards_dir,
            &key,
            data,
            fsync_dir,
            |path| {
                assert_eq!(fs::read(store.shard_path(&key)).unwrap(), data);
                time.control().set(6_000);
                fsync_dir(path)?;
                synced.set(true);
                Ok(())
            },
            || fence.require_valid_for(ClusterEpoch::INITIAL),
        )
        .unwrap();
        assert!(synced.get());
        assert!(fence.require_valid_for(ClusterEpoch::INITIAL).is_err());
        assert_eq!(ack.crc64, checksum::crc64::checksum(data));
        assert_eq!(ack.stored_size, data.len() as u64);
        assert!(fs::read_dir(&store.tmp_dir).unwrap().next().is_none());
    }
}
