use crate::traits::GlobalService;

#[test]
fn bucket_create_and_head() {
    let db = crate::SqliteBucketDb::open_in_memory().unwrap();
    db.create_bucket("my-bucket", 1).unwrap();

    let info = db.head_bucket("my-bucket").unwrap();
    assert_eq!(info.name, "my-bucket");
    assert_eq!(info.owner_id, 1);
    assert!(info.created_at > 0);
    assert_eq!(info.versioning, 0);
}

#[test]
fn bucket_create_duplicate_fails() {
    let db = crate::SqliteBucketDb::open_in_memory().unwrap();
    db.create_bucket("dup", 1).unwrap();

    let err = db.create_bucket("dup", 1).unwrap_err();
    assert!(matches!(err, crate::MetadataError::BucketAlreadyExists));
}

#[test]
fn bucket_delete() {
    let db = crate::SqliteBucketDb::open_in_memory().unwrap();
    db.create_bucket("to-delete", 1).unwrap();
    db.delete_bucket("to-delete").unwrap();

    let err = db.head_bucket("to-delete").unwrap_err();
    assert!(matches!(err, crate::MetadataError::BucketNotFound { .. }));
}

#[test]
fn bucket_delete_nonexistent_fails() {
    let db = crate::SqliteBucketDb::open_in_memory().unwrap();
    let err = db.delete_bucket("nonexistent").unwrap_err();
    assert!(matches!(err, crate::MetadataError::BucketNotFound { .. }));
}

#[test]
fn bucket_head_nonexistent_fails() {
    let db = crate::SqliteBucketDb::open_in_memory().unwrap();
    let err = db.head_bucket("nonexistent").unwrap_err();
    assert!(matches!(err, crate::MetadataError::BucketNotFound { .. }));
}

#[test]
fn bucket_list_by_owner() {
    let db = crate::SqliteBucketDb::open_in_memory().unwrap();

    db.create_bucket("owner1-a", 1).unwrap();
    db.create_bucket("owner1-b", 1).unwrap();
    db.create_bucket("owner2-a", 2).unwrap();

    let buckets = db.list_buckets(1).unwrap();
    assert_eq!(buckets.len(), 2);
    assert_eq!(buckets[0].name, "owner1-a");
    assert_eq!(buckets[1].name, "owner1-b");

    let buckets = db.list_buckets(2).unwrap();
    assert_eq!(buckets.len(), 1);
    assert_eq!(buckets[0].name, "owner2-a");

    let buckets = db.list_buckets(99).unwrap();
    assert!(buckets.is_empty());
}

#[test]
fn bucket_list_empty() {
    let db = crate::SqliteBucketDb::open_in_memory().unwrap();
    let buckets = db.list_buckets(1).unwrap();
    assert!(buckets.is_empty());
}

#[test]
fn bucket_on_disk() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("buckets.db");

    {
        let db = crate::SqliteBucketDb::open(&db_path).unwrap();
        db.create_bucket("persistent", 1).unwrap();
    }

    // Reopen and verify data persists.
    {
        let db = crate::SqliteBucketDb::open(&db_path).unwrap();
        let info = db.head_bucket("persistent").unwrap();
        assert_eq!(info.name, "persistent");
    }
}
