// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn fence(deadline: u64) -> AdmittedRouteEffectFence {
    AdmittedRouteEffectFence::bounded(ClusterEpoch::INITIAL, deadline, deadline - 1_000)
}

fn write_file(store: &PgStore, key: &ShardKey, bytes: &[u8]) -> WriteAck {
    PgStore::write_shard_file_durable(&store.tmp_dir, &store.shards_dir, key, bytes).unwrap()
}

#[test]
fn shard_ack_expiry_after_lookup_rejects_without_any_sql_change() {
    for occupied in [false, true] {
        let time = crate::clock::test_time_override_guard(1_000);
        let dir = test_util::tempdir();
        let store = PgStore::open(dir.path(), 0).unwrap();
        let key = ShardKey::new(&[0x51; 16], 1, 0);
        let next = ShardKey::new(&[0x52; 16], 1, 0);
        let canary = ShardKey::new(&[0x53; 16], 1, 0);
        let canary_ack = store.write_shard(&canary, b"independent shard").unwrap();
        let ack = write_file(&store, &key, b"first shard");
        let next_ack = write_file(&store, &next, b"second shard");
        if occupied {
            store.register_written_shard(&key, ack).unwrap();
        }
        let changes = store.conn.total_changes();
        let control = time.control();
        let error = PgStore::test_with_before_shard_ack_effect_hook(
            move |in_transaction| {
                assert!(in_transaction);
                control.set(4_500);
            },
            || {
                store.register_written_shards_batch_exact_with_effect_fence(
                    &[(&key, ack), (&next, next_ack)],
                    fence(5_000),
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
            "{error:?}"
        );
        assert!(store.conn.is_autocommit());
        assert_eq!(store.conn.total_changes(), changes);
        if occupied {
            store.validate_written_shard_ack(&key, ack).unwrap();
        } else {
            assert!(matches!(store.stat_shard(&key), Err(StoreError::NotFound)));
        }
        assert!(matches!(store.stat_shard(&next), Err(StoreError::NotFound)));
        store
            .validate_written_shard_ack(&canary, canary_ack)
            .unwrap();
        assert_eq!(fs::read(store.shard_path(&key)).unwrap(), b"first shard");
        assert_eq!(fs::read(store.shard_path(&next)).unwrap(), b"second shard");
        assert_eq!(
            fs::read(store.shard_path(&canary)).unwrap(),
            b"independent shard"
        );
        let batch = [(&key, ack), (&next, next_ack)];
        store
            .register_written_shards_batch_exact_with_effect_fence(&batch, fence(20_000))
            .unwrap();
        let after = store.conn.total_changes();
        store
            .register_written_shards_batch_exact_with_effect_fence(&batch, fence(20_000))
            .unwrap();
        assert_eq!(
            store.conn.total_changes(),
            after,
            "exact retry must not rewrite rows"
        );
        for (key, ack) in batch {
            store.validate_written_shard_ack(key, ack).unwrap();
        }
    }
}

#[test]
fn shard_ack_fenced_batch_rolls_back_on_later_mismatch_and_retries() {
    let _time = crate::clock::test_time_override_guard(1_000);
    let dir = test_util::tempdir();
    let store = PgStore::open(dir.path(), 0).unwrap();
    let first = ShardKey::new(&[0x61; 16], 1, 0);
    let second = ShardKey::new(&[0x62; 16], 1, 0);
    let first_ack = write_file(&store, &first, b"first");
    let second_ack = store.write_shard(&second, b"second").unwrap();
    let crossed = WriteAck {
        crc64: second_ack.crc64 ^ 1,
        ..second_ack
    };
    let error = store
        .register_written_shards_batch_exact_with_effect_fence(
            &[(&first, first_ack), (&second, crossed)],
            fence(5_000),
        )
        .unwrap_err();
    assert!(
        matches!(error, StoreError::ShardAckMismatch { .. }),
        "{error:?}"
    );
    assert!(store.conn.is_autocommit());
    assert!(matches!(
        store.stat_shard(&first),
        Err(StoreError::NotFound)
    ));
    store
        .validate_written_shard_ack(&second, second_ack)
        .unwrap();
    store
        .register_written_shards_batch_exact_with_effect_fence(
            &[(&first, first_ack), (&second, second_ack)],
            fence(5_000),
        )
        .unwrap();
    store.validate_written_shard_ack(&first, first_ack).unwrap();
}

#[test]
fn shard_ack_batch_rechecks_between_inserts_without_adding_transactions() {
    for expire_after_first in [false, true] {
        let time = crate::clock::test_time_override_guard(1_000);
        let dir = test_util::tempdir();
        let mut store = PgStore::open(dir.path(), 0).unwrap();
        let first = ShardKey::new(&[0x71; 16], 1, 0);
        let second = ShardKey::new(&[0x72; 16], 1, 0);
        let ack = write_file(&store, &first, b"first");
        let second_ack = write_file(&store, &second, b"second");
        let changes = store.conn.total_changes();
        let observed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured = std::sync::Arc::clone(&observed);
        let control = time.control();
        store
            .conn
            .trace_v2(
                TraceEventCodes::SQLITE_TRACE_STMT,
                Some(move |event: TraceEvent<'_>| {
                    if let TraceEvent::Stmt(_, sql) = event {
                        if !sql.starts_with("--") {
                            captured.lock().unwrap().push(sql.to_owned());
                            // The INSERT has already passed its effect check. It
                            // may finish, but must not authorize the next INSERT.
                            // Conversely, accepted rows may COMMIT after cutoff.
                            if (expire_after_first && sql.starts_with("INSERT"))
                                || (!expire_after_first && sql == "COMMIT")
                            {
                                control.set(4_500);
                            }
                        }
                    }
                }),
            )
            .unwrap();
        let result = store.register_written_shards_batch_exact_with_effect_fence(
            &[(&first, ack), (&second, second_ack)],
            fence(5_000),
        );
        let statements = observed.lock().unwrap();
        let kinds: Vec<_> = statements
            .iter()
            .map(|sql| sql.split_whitespace().next().unwrap().to_owned())
            .collect();
        drop(statements);
        assert!(store.conn.is_autocommit());
        if expire_after_first {
            assert!(
                matches!(result, Err(StoreError::RouteMapExpired { .. })),
                "{result:?}"
            );
            assert_eq!(kinds, ["BEGIN", "SELECT", "INSERT", "SELECT", "ROLLBACK"]);
            assert_eq!(store.conn.total_changes() - changes, 1);
            assert!(matches!(
                store.stat_shard(&first),
                Err(StoreError::NotFound)
            ));
            assert!(matches!(
                store.stat_shard(&second),
                Err(StoreError::NotFound)
            ));
        } else {
            result.unwrap();
            assert_eq!(
                kinds,
                ["BEGIN", "SELECT", "INSERT", "SELECT", "INSERT", "COMMIT"]
            );
            assert_eq!(store.conn.total_changes() - changes, 2);
            store.validate_written_shard_ack(&first, ack).unwrap();
            store
                .validate_written_shard_ack(&second, second_ack)
                .unwrap();
        }
    }
}
