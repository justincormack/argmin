// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::node::SharedStorageNode;
use crate::node_runtime::pg_store::PgStore;
use crate::node_runtime::traits::ShardStore;
use crate::types::AdmittedRouteEffectFence;
use crate::{ClusterEpoch, StoreError};
use std::sync::mpsc;
use std::time::Duration;

const WAIT: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug)]
enum Writer {
    LockedStore,
    Rename,
    Link,
    FencedRename,
    FencedLink,
}

fn write(node: &SharedStorageNode, key: &ShardKey, writer: Writer) {
    let data = b"guarded shard bytes";
    let fence = AdmittedRouteEffectFence::unbounded(ClusterEpoch::INITIAL);
    let ack = match writer {
        Writer::LockedStore => node.get_pg(0).unwrap().write_shard(key, data),
        Writer::Rename => node.write_shard_file(0, key, data),
        Writer::Link => node.write_shard_file_if_absent(0, key, data),
        Writer::FencedRename => node.write_shard_file_with_effect_fence(0, key, data, fence),
        Writer::FencedLink => {
            node.write_shard_file_if_absent_with_effect_fence(0, key, data, fence)
        }
    }
    .unwrap();
    assert_eq!(ack.stored_size, data.len() as u64);
    assert_eq!(ack.crc64, checksum::crc64::checksum(data));
}

#[test]
fn shard_file_mutations_exclude_deletion_through_publication_and_acknowledgement() {
    for writer in [
        Writer::LockedStore,
        Writer::Rename,
        Writer::Link,
        Writer::FencedRename,
        Writer::FencedLink,
    ] {
        for after_publication in [false, true] {
            for locked_delete in [false, true] {
                // Store/store already serializes at the PG mutex; the other
                // combinations must also share the file-level exclusion domain.
                if locked_delete && matches!(writer, Writer::LockedStore) {
                    continue;
                }
                exclusion_case(writer, after_publication, locked_delete);
            }
        }
    }
}

fn exclusion_case(writer: Writer, after_publication: bool, locked_delete: bool) {
    let tmp = test_util::tempdir();
    let node = Arc::new(SharedStorageNode::open(tmp.path(), &[0, 1]).unwrap());
    let gate = node.get_pg(0).unwrap().shard_file_mutations();
    let key = ShardKey::new(&[0xD1; 16], 17, 0);
    let other = ShardKey::new(&[0xD2; 16], 17, 0);
    let (paused_tx, paused_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let writer_node = Arc::clone(&node);
    let writer_key = key.clone();
    let writing = std::thread::spawn(move || {
        let hook = move || {
            paused_tx.send(()).unwrap();
            resume_rx.recv_timeout(WAIT).unwrap();
        };
        let action = || write(&writer_node, &writer_key, writer);
        if after_publication {
            super::super::shards::with_after_shard_publication_hook(hook, action);
        } else {
            PgStore::test_with_before_shard_publication_hook(hook, action);
        }
    });
    paused_rx.recv_timeout(WAIT).unwrap();
    assert_eq!(node.read_shard_file(0, &key).is_ok(), after_publication);

    let (blocked_tx, blocked_rx) = mpsc::channel();
    let expected_key = key.clone();
    *gate.before_delete_wait.lock().unwrap() = Some(Box::new(move |key| {
        assert_eq!(key, &expected_key);
        // Invoked only after try_write actually observes the publisher's guard.
        blocked_tx.send(()).unwrap();
    }));
    let (finished_tx, finished_rx) = mpsc::channel();
    let deleting_node = Arc::clone(&node);
    let deleting_key = key.clone();
    let deleting = std::thread::spawn(move || {
        if locked_delete {
            deleting_node
                .get_pg(0)
                .unwrap()
                .delete_shard(&deleting_key)
                .unwrap();
        } else {
            deleting_node.delete_shard_file(0, &deleting_key).unwrap();
        }
        finished_tx.send(()).unwrap();
    });
    blocked_rx.recv_timeout(WAIT).unwrap();
    assert!(matches!(
        finished_rx.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));

    // Neither another key in this PG nor the same key in another PG is blocked.
    node.write_shard_file(0, &other, b"other key").unwrap();
    node.write_shard_file(1, &key, b"other PG").unwrap();
    assert_eq!(node.read_shard_file(0, &other).unwrap(), b"other key");
    node.delete_shard_file(1, &key).unwrap();
    resume_tx.send(()).unwrap();
    writing.join().unwrap();
    finished_rx.recv_timeout(WAIT).unwrap();
    deleting.join().unwrap();
    assert!(matches!(
        node.read_shard_file(0, &key),
        Err(StoreError::NotFound)
    ));
    assert_eq!(node.read_shard_file(0, &other).unwrap(), b"other key");
    assert!(gate.entries.lock().unwrap().is_empty());
}

#[test]
fn shard_file_mutations_recheck_deadline_after_waiting_for_delete() {
    for if_absent in [false, true] {
        let tmp = test_util::tempdir();
        let node = Arc::new(SharedStorageNode::open(tmp.path(), &[0]).unwrap());
        let gate = node.get_pg(0).unwrap().shard_file_mutations();
        let key = ShardKey::new(&[0xD3; 16], 18, 0);
        let (blocked_tx, blocked_rx) = mpsc::channel();
        let writer_gate = Arc::clone(&gate);
        let writer_node = Arc::clone(&node);
        let writer_key = key.clone();
        let writing = gate.delete(&key, || {
            let writing = std::thread::spawn(move || {
                let clock = crate::clock::test_time_override_guard(1_000);
                let control = clock.control();
                let fence = AdmittedRouteEffectFence::bounded(ClusterEpoch::INITIAL, 5_000, 4_000);
                fence.require_valid_for(ClusterEpoch::INITIAL).unwrap();
                *writer_gate.before_publish_wait.lock().unwrap() = Some(Box::new(move |_| {
                    control.set(4_500);
                    blocked_tx.send(()).unwrap();
                }));
                if if_absent {
                    writer_node.write_shard_file_if_absent_with_effect_fence(
                        0,
                        &writer_key,
                        b"expired",
                        fence,
                    )
                } else {
                    writer_node.write_shard_file_with_effect_fence(
                        0,
                        &writer_key,
                        b"expired",
                        fence,
                    )
                }
            });
            blocked_rx.recv_timeout(WAIT).unwrap();
            writing
        });
        assert!(matches!(
            writing.join().unwrap(),
            Err(StoreError::RouteMapExpired { now_ms: 4_500, .. })
        ));
        assert!(matches!(
            node.read_shard_file(0, &key),
            Err(StoreError::NotFound)
        ));
        assert_eq!(
            std::fs::read_dir(tmp.path().join("pg-0000/tmp"))
                .unwrap()
                .count(),
            0
        );
        assert!(gate.entries.lock().unwrap().is_empty());
        write(&node, &key, Writer::FencedLink);
        assert_eq!(
            node.read_shard_file(0, &key).unwrap(),
            b"guarded shard bytes"
        );
    }
}

#[test]
fn shard_file_mutations_allow_concurrent_publishers_and_reclaim_entries() {
    let gate = Arc::new(ShardFileMutations::default());
    let key = ShardKey::new(&[0xD4; 16], 19, 0);
    let (entered_tx, entered_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let other_gate = Arc::clone(&gate);
    let other_key = key.clone();
    let first = std::thread::spawn(move || {
        other_gate.publish(&other_key, || {
            entered_tx.send(()).unwrap();
            resume_rx.recv_timeout(WAIT).unwrap();
        });
    });
    entered_rx.recv_timeout(WAIT).unwrap();
    let entry = gate.entry(&key);
    assert!(entry.lock.try_read().is_ok());
    assert!(matches!(
        entry.lock.try_write(),
        Err(TryLockError::WouldBlock)
    ));
    gate.publish(&key, || {});
    drop(entry);
    assert_eq!(gate.entries.lock().unwrap().len(), 1);
    resume_tx.send(()).unwrap();
    first.join().unwrap();
    assert!(gate.entries.lock().unwrap().is_empty());
}

#[test]
fn shard_file_mutations_release_after_errors_and_unwind() {
    let gate = ShardFileMutations::default();
    let key = ShardKey::new(&[0xD5; 16], 20, 0);
    for delete in [false, true] {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if delete {
                gate.delete(&key, || panic!("injected delete unwind"));
            } else {
                gate.publish(&key, || panic!("injected write unwind"));
            }
        }));
        assert!(result.is_err());
        assert!(gate.entries.lock().unwrap().is_empty());
        assert_eq!(
            gate.publish(&key, || Err::<(), _>("write error")),
            Err("write error")
        );
        assert_eq!(
            gate.delete(&key, || Err::<(), _>("delete error")),
            Err("delete error")
        );
        assert!(gate.entries.lock().unwrap().is_empty());
        gate.publish(&key, || {});
        gate.delete(&key, || {});
    }
}
