use crate::traits::ShardStore;
use crate::types::*;

/// Helper to create a test shard key.
fn test_key(index: u8) -> ShardKey {
    let hash = [index; 16];
    ShardKey::new(&hash, 1, index)
}

/// Run the common shard test suite against any ShardStore implementation.
fn shard_roundtrip(store: &dyn ShardStore) {
    let key = test_key(0);
    let data = b"hello shard world";

    // Write
    let ack = store.write_shard(&key, data).unwrap();
    assert_eq!(ack.stored_size, data.len() as u64);
    assert_eq!(ack.crc64, checksum::crc64::checksum(data));

    // Read back
    let read = store.read_shard(&key).unwrap();
    assert_eq!(read.data, data);
    assert_eq!(read.crc64, ack.crc64);

    // Stat
    let stat = store.stat_shard(&key).unwrap();
    assert_eq!(stat.size, data.len() as u64);
    assert_eq!(stat.crc64, ack.crc64);
    assert!(stat.created_at > 0);
}

fn shard_delete_idempotent(store: &dyn ShardStore) {
    let key = test_key(1);
    let data = b"to be deleted";

    store.write_shard(&key, data).unwrap();
    store.delete_shard(&key).unwrap();

    // Read after delete returns NotFound.
    let err = store.read_shard(&key).unwrap_err();
    assert!(matches!(err, crate::StoreError::NotFound));

    // Stat after delete returns NotFound.
    let err = store.stat_shard(&key).unwrap_err();
    assert!(matches!(err, crate::StoreError::NotFound));

    // Delete again is idempotent.
    store.delete_shard(&key).unwrap();
}

fn shard_not_found(store: &dyn ShardStore) {
    let key = test_key(99);
    let err = store.read_shard(&key).unwrap_err();
    assert!(matches!(err, crate::StoreError::NotFound));

    let err = store.stat_shard(&key).unwrap_err();
    assert!(matches!(err, crate::StoreError::NotFound));
}

fn shard_overwrite(store: &dyn ShardStore) {
    let key = test_key(2);
    let data1 = b"version one";
    let data2 = b"version two";

    store.write_shard(&key, data1).unwrap();
    let ack2 = store.write_shard(&key, data2).unwrap();

    let read = store.read_shard(&key).unwrap();
    assert_eq!(read.data, data2.as_slice());
    assert_eq!(read.crc64, ack2.crc64);
}

fn shard_empty_data(store: &dyn ShardStore) {
    let key = test_key(3);
    let data: &[u8] = b"";

    let ack = store.write_shard(&key, data).unwrap();
    assert_eq!(ack.stored_size, 0);
    assert_eq!(ack.crc64, checksum::crc64::checksum(b""));

    let read = store.read_shard(&key).unwrap();
    assert!(read.data.is_empty());
    assert_eq!(read.crc64, ack.crc64);
}

fn shard_key_validation() {
    // Valid key.
    let key = ShardKey::from_bytes(&[0u8; SHARD_KEY_LEN]);
    assert!(key.is_ok());

    // Too short.
    let err = ShardKey::from_bytes(&[0u8; 10]).unwrap_err();
    assert!(matches!(err, crate::StoreError::InvalidKeyLength { .. }));

    // Too long.
    let err = ShardKey::from_bytes(&[0u8; 30]).unwrap_err();
    assert!(matches!(err, crate::StoreError::InvalidKeyLength { .. }));
}

fn shard_key_hex() {
    let hash = [0xAB; 16];
    let key = ShardKey::new(&hash, 0x0102030405060708, 0xFF);
    let hex = key.hex();
    assert_eq!(hex.len(), SHARD_KEY_LEN * 2);
    assert!(hex.starts_with("ab")); // first byte
    assert_eq!(key.hex_prefix(), "ab");
}

fn shard_multiple_keys(store: &dyn ShardStore) {
    // Write several shards, read them all back.
    for i in 10..20u8 {
        let key = test_key(i);
        let data = vec![i; (i as usize) * 100];
        store.write_shard(&key, &data).unwrap();
    }

    for i in 10..20u8 {
        let key = test_key(i);
        let expected = vec![i; (i as usize) * 100];
        let read = store.read_shard(&key).unwrap();
        assert_eq!(read.data, expected);
    }
}

// --- MemoryPgStore tests ---

#[test]
fn memory_shard_roundtrip() {
    let store = crate::MemoryPgStore::new();
    shard_roundtrip(&store);
}

#[test]
fn memory_shard_delete_idempotent() {
    let store = crate::MemoryPgStore::new();
    shard_delete_idempotent(&store);
}

#[test]
fn memory_shard_not_found() {
    let store = crate::MemoryPgStore::new();
    shard_not_found(&store);
}

#[test]
fn memory_shard_overwrite() {
    let store = crate::MemoryPgStore::new();
    shard_overwrite(&store);
}

#[test]
fn memory_shard_empty_data() {
    let store = crate::MemoryPgStore::new();
    shard_empty_data(&store);
}

#[test]
fn memory_shard_multiple_keys() {
    let store = crate::MemoryPgStore::new();
    shard_multiple_keys(&store);
}

#[test]
fn test_shard_key_validation() {
    shard_key_validation();
}

#[test]
fn test_shard_key_hex() {
    shard_key_hex();
}

// --- PgStore (filesystem) tests ---

fn make_pg_store() -> (test_util::TempDir, crate::PgStore) {
    let dir = test_util::tempdir();
    let pg_dir = dir.path().join("pg-0000");
    let store = crate::PgStore::open(&pg_dir, 0).unwrap();
    (dir, store)
}

#[test]
fn file_shard_roundtrip() {
    let (_dir, store) = make_pg_store();
    shard_roundtrip(&store);
}

#[test]
fn file_shard_delete_idempotent() {
    let (_dir, store) = make_pg_store();
    shard_delete_idempotent(&store);
}

#[test]
fn file_shard_not_found() {
    let (_dir, store) = make_pg_store();
    shard_not_found(&store);
}

#[test]
fn file_shard_overwrite() {
    let (_dir, store) = make_pg_store();
    shard_overwrite(&store);
}

#[test]
fn file_shard_empty_data() {
    let (_dir, store) = make_pg_store();
    shard_empty_data(&store);
}

#[test]
fn file_shard_multiple_keys() {
    let (_dir, store) = make_pg_store();
    shard_multiple_keys(&store);
}

#[test]
fn file_shard_integrity_error() {
    let dir = test_util::tempdir();
    let pg_dir = dir.path().join("pg-0000");
    let store = crate::PgStore::open(&pg_dir, 0).unwrap();

    let key = test_key(50);
    let data = b"integrity test data";
    store.write_shard(&key, data).unwrap();

    // Corrupt the file on disk.
    let prefix = key.hex_prefix();
    let hex = key.hex();
    let shard_path = pg_dir.join("shards").join(prefix).join(hex);
    std::fs::write(&shard_path, b"corrupted data!!!").unwrap();

    let err = store.read_shard(&key).unwrap_err();
    assert!(matches!(err, crate::StoreError::IntegrityError { .. }));

    // After integrity error, the shard should be quarantined (NotFound on subsequent reads).
    let err = store.read_shard(&key).unwrap_err();
    assert!(matches!(err, crate::StoreError::NotFound));
}

#[test]
fn file_shard_orphan_cleanup() {
    let dir = test_util::tempdir();
    let pg_dir = dir.path().join("pg-0000");

    // Create the PG structure and add an orphan file in tmp/.
    std::fs::create_dir_all(pg_dir.join("shards")).unwrap();
    std::fs::create_dir_all(pg_dir.join("tmp")).unwrap();
    std::fs::write(pg_dir.join("tmp").join("orphan-shard"), b"leftover").unwrap();

    // Opening the store should clean up the orphan.
    let _store = crate::PgStore::open(&pg_dir, 0).unwrap();

    assert!(!pg_dir.join("tmp").join("orphan-shard").exists());
}

#[test]
fn file_shard_large_data() {
    let (_dir, store) = make_pg_store();
    let key = test_key(60);
    // 1 MB of data.
    let data = vec![0x42u8; 1_000_000];
    let ack = store.write_shard(&key, &data).unwrap();
    assert_eq!(ack.stored_size, 1_000_000);

    let read = store.read_shard(&key).unwrap();
    assert_eq!(read.data.len(), 1_000_000);
    assert_eq!(read.crc64, ack.crc64);
}
