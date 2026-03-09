use crate::traits::{PgMetadataStore, ShardStore, StorageNode};
use crate::types::*;

/// Integration test: write shard data + object metadata, read both back.
#[test]
fn shard_and_metadata_roundtrip() {
    let dir = test_util::tempdir();
    let pg_dir = dir.path().join("pg-0000");
    let store = crate::PgStore::open(&pg_dir, 0).unwrap();

    let hash = [0x42u8; 16];
    let shard_key = ShardKey::new(&hash, 1, 0);
    let shard_data = b"the actual object data payload";

    // Write shard.
    let ack = store.write_shard(&shard_key, shard_data).unwrap();

    // Write object metadata referencing the shard.
    let req = PutObjectReq::Live(PutLiveObjectReq {
        bucket: "test-bucket".into(),
        key: "my/object.txt".into(),
        version_id: VersionId::Null,
        size: shard_data.len() as u64,
        etag: ack.crc64.to_be_bytes().to_vec(),
        etag_kind: EtagKind::Crc64,
        ec: EcShape { k: 4, m: 2 },
        layout: ObjectLayout::ChunkManifest,
        metadata_blob: None,
    });
    store.put_object_meta(&req).unwrap();

    // Read back both.
    let read = store.read_shard(&shard_key).unwrap();
    assert_eq!(read.data, shard_data);
    assert_eq!(read.crc64, ack.crc64);

    let obj = store
        .get_object_meta("test-bucket", "my/object.txt")
        .unwrap();
    let live = obj.as_live().unwrap();
    assert_eq!(live.size, shard_data.len() as u64);
    assert_eq!(live.etag, ack.crc64.to_be_bytes().to_vec());
}

/// Integration test: LocalStorageNode with multiple PGs.
#[test]
fn local_storage_node_multi_pg() {
    let dir = test_util::tempdir();
    let node = crate::LocalStorageNode::open(dir.path(), &[0, 1, 2]).unwrap();

    assert_eq!(node.pg_ids(), &[0, 1, 2]);

    // Write to different PGs.
    for pg_id in 0..3u32 {
        let pg = node.get_pg(pg_id).unwrap();
        let hash = [pg_id as u8; 16];
        let key = ShardKey::new(&hash, 1, 0);
        pg.write_shard(&key, &[pg_id as u8; 100]).unwrap();
    }

    // Read back from each PG.
    for pg_id in 0..3u32 {
        let pg = node.get_pg(pg_id).unwrap();
        let hash = [pg_id as u8; 16];
        let key = ShardKey::new(&hash, 1, 0);
        let read = pg.read_shard(&key).unwrap();
        assert_eq!(read.data, vec![pg_id as u8; 100]);
    }

    // Non-existent PG.
    assert!(matches!(
        node.get_pg(99),
        Err(crate::StoreError::PgNotFound { pg_id: 99 })
    ));
}

/// Integration test: StorageNode trait (dyn dispatch).
#[test]
fn storage_node_trait() {
    let dir = test_util::tempdir();
    let node = crate::LocalStorageNode::open(dir.path(), &[5, 10]).unwrap();
    let node: &dyn StorageNode = &node;

    assert_eq!(node.pg_ids(), &[5, 10]);

    let pg = node.get_pg_store(5).unwrap();
    let key = ShardKey::new(&[0x55; 16], 1, 0);
    pg.write_shard(&key, b"trait test").unwrap();

    let read = pg.read_shard(&key).unwrap();
    assert_eq!(read.data, b"trait test");
}

/// Integration test: bucket + object metadata lifecycle on a single PG.
#[test]
fn full_lifecycle() {
    let dir = test_util::tempdir();

    // Create storage node.
    let node = crate::LocalStorageNode::open(&dir.path().join("data"), &[0]).unwrap();
    let pg = node.get_pg(0).unwrap();
    pg.create_bucket("my-bucket", "owner-1", false).unwrap();

    // PutObject: write shard + metadata.
    let hash = [0xAA; 16];
    let shard_key = ShardKey::new(&hash, 1, 0);
    let data = b"hello world";
    let ack = pg.write_shard(&shard_key, data).unwrap();

    pg.put_object_meta(&PutObjectReq::Live(PutLiveObjectReq {
        bucket: "my-bucket".into(),
        key: "greeting.txt".into(),
        version_id: VersionId::Null,
        size: data.len() as u64,
        etag: ack.crc64.to_be_bytes().to_vec(),
        etag_kind: EtagKind::Crc64,
        ec: EcShape { k: 4, m: 2 },
        layout: ObjectLayout::ChunkManifest,
        metadata_blob: None,
    }))
    .unwrap();

    // HeadBucket.
    let info = pg.head_bucket("my-bucket").unwrap();
    assert_eq!(info.name, "my-bucket");

    // GetObject: read metadata + shard.
    let obj = pg.get_object_meta("my-bucket", "greeting.txt").unwrap();
    assert_eq!(obj.as_live().unwrap().size, 11);

    let read = pg.read_shard(&shard_key).unwrap();
    assert_eq!(read.data, b"hello world");

    // DeleteObject: remove metadata + shard.
    pg.delete_object_meta("my-bucket", "greeting.txt").unwrap();
    pg.delete_shard(&shard_key).unwrap();

    // Verify gone.
    let err = pg.get_object_meta("my-bucket", "greeting.txt").unwrap_err();
    assert!(matches!(err, crate::MetadataError::ObjectNotFound));

    let err = pg.read_shard(&shard_key).unwrap_err();
    assert!(matches!(err, crate::StoreError::NotFound));
}

/// Test PgStore reopening with data persistence.
#[test]
fn pg_store_persistence() {
    let dir = test_util::tempdir();
    let pg_dir = dir.path().join("pg-0000");

    let key = ShardKey::new(&[0xBB; 16], 1, 0);
    let data = b"persistent data";

    // Write data, then drop the store.
    {
        let store = crate::PgStore::open(&pg_dir, 0).unwrap();
        store.write_shard(&key, data).unwrap();
        store
            .put_object_meta(&PutObjectReq::Live(PutLiveObjectReq {
                bucket: "b".into(),
                key: "k".into(),
                version_id: VersionId::Null,
                size: data.len() as u64,
                etag: vec![1],
                etag_kind: EtagKind::Crc64,
                ec: EcShape { k: 4, m: 2 },
                layout: ObjectLayout::ChunkManifest,
                metadata_blob: None,
            }))
            .unwrap();
    }

    // Reopen and verify data persists.
    {
        let store = crate::PgStore::open(&pg_dir, 0).unwrap();

        let read = store.read_shard(&key).unwrap();
        assert_eq!(read.data, data);

        let obj = store.get_object_meta("b", "k").unwrap();
        assert_eq!(obj.as_live().unwrap().size, data.len() as u64);
    }
}
