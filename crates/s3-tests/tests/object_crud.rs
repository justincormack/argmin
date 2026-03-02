use aws_sdk_s3::primitives::ByteStream;
use s3_tests::{unique_bucket, CTX};

/// Create a bucket, returning its name. Tests are responsible for cleanup.
async fn setup_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    client.create_bucket().bucket(&bucket).send().await.unwrap();
    bucket
}

// ── PutObject / GetObject basic ──────────────────────────────────────

#[test]
fn test_object_write_file() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"hello world";

        client
            .put_object()
            .bucket(&bucket)
            .key("testobj")
            .body(ByteStream::from_static(body))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("testobj")
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], body);

        client
            .delete_object()
            .bucket(&bucket)
            .key("testobj")
            .send()
            .await
            .unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_object_write_empty() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("empty")
            .body(ByteStream::from_static(b""))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("empty")
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert!(data.is_empty());

        client
            .delete_object()
            .bucket(&bucket)
            .key("empty")
            .send()
            .await
            .unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_object_write_overwrite() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"first"))
            .send()
            .await
            .unwrap();

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"second"))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"second");

        client
            .delete_object()
            .bucket(&bucket)
            .key("obj")
            .send()
            .await
            .unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

// ── PutObject returns ETag ───────────────────────────────────────────

#[test]
fn test_object_write_check_etag() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let resp = client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let etag = resp.e_tag().expect("PutObject should return ETag");
        assert!(!etag.is_empty());

        client
            .delete_object()
            .bucket(&bucket)
            .key("obj")
            .send()
            .await
            .unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

// ── GetObject nonexistent ────────────────────────────────────────────

#[test]
fn test_object_read_not_exist() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let result = client
            .get_object()
            .bucket(&bucket)
            .key("no-such-key")
            .send()
            .await;
        assert!(result.is_err());

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_object_read_nonexistent_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let result = client.get_object().bucket(&bucket).key("key").send().await;
        assert!(result.is_err());
    });
}

// ── HeadObject ───────────────────────────────────────────────────────

#[test]
fn test_object_head_existing() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"head test content";

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(body))
            .send()
            .await
            .unwrap();

        let resp = client
            .head_object()
            .bucket(&bucket)
            .key("obj")
            .send()
            .await
            .unwrap();

        assert_eq!(resp.content_length(), Some(body.len() as i64));
        assert!(resp.e_tag().is_some());

        client
            .delete_object()
            .bucket(&bucket)
            .key("obj")
            .send()
            .await
            .unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_object_head_nonexistent() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let result = client
            .head_object()
            .bucket(&bucket)
            .key("no-such-key")
            .send()
            .await;
        assert!(result.is_err());

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

// ── DeleteObject ─────────────────────────────────────────────────────

#[test]
fn test_object_delete_existing() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("todelete")
            .body(ByteStream::from_static(b"bye"))
            .send()
            .await
            .unwrap();

        client
            .delete_object()
            .bucket(&bucket)
            .key("todelete")
            .send()
            .await
            .unwrap();

        // Verify it's gone
        let result = client
            .get_object()
            .bucket(&bucket)
            .key("todelete")
            .send()
            .await;
        assert!(result.is_err());

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_object_delete_nonexistent() {
    s3_tests::run(async {
        // S3 returns 204 for deleting nonexistent objects (idempotent)
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .delete_object()
            .bucket(&bucket)
            .key("nonexistent")
            .send()
            .await
            .unwrap();

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

// ── Content-Type ─────────────────────────────────────────────────────

#[test]
fn test_object_content_type() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("typed")
            .content_type("text/html")
            .body(ByteStream::from_static(b"<h1>hi</h1>"))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("typed")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_type(), Some("text/html"));

        client
            .delete_object()
            .bucket(&bucket)
            .key("typed")
            .send()
            .await
            .unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_object_default_content_type() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("noct")
            .body(ByteStream::from_static(b"binary"))
            .send()
            .await
            .unwrap();

        let resp = client
            .head_object()
            .bucket(&bucket)
            .key("noct")
            .send()
            .await
            .unwrap();
        let ct = resp.content_type().unwrap_or("");
        assert!(
            ct == "application/octet-stream" || ct.is_empty(),
            "unexpected content-type: {}",
            ct
        );

        client
            .delete_object()
            .bucket(&bucket)
            .key("noct")
            .send()
            .await
            .unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

// ── User Metadata (x-amz-meta-*) ────────────────────────────────────

#[test]
fn test_object_metadata_round_trip() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("meta")
            .metadata("color", "blue")
            .metadata("size", "42")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let resp = client
            .head_object()
            .bucket(&bucket)
            .key("meta")
            .send()
            .await
            .unwrap();

        let metadata = resp.metadata().unwrap();
        assert_eq!(metadata.get("color").map(|s| s.as_str()), Some("blue"));
        assert_eq!(metadata.get("size").map(|s| s.as_str()), Some("42"));

        client
            .delete_object()
            .bucket(&bucket)
            .key("meta")
            .send()
            .await
            .unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_object_metadata_in_get() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("meta2")
            .metadata("tag", "value")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("meta2")
            .send()
            .await
            .unwrap();

        let metadata = resp.metadata().unwrap();
        assert_eq!(metadata.get("tag").map(|s| s.as_str()), Some("value"));

        client
            .delete_object()
            .bucket(&bucket)
            .key("meta2")
            .send()
            .await
            .unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

// ── ETag consistency ─────────────────────────────────────────────────

#[test]
fn test_object_etag_matches_head_and_get() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let put_resp = client
            .put_object()
            .bucket(&bucket)
            .key("etag")
            .body(ByteStream::from_static(b"etag test"))
            .send()
            .await
            .unwrap();
        let put_etag = put_resp.e_tag().unwrap().to_string();

        let head_resp = client
            .head_object()
            .bucket(&bucket)
            .key("etag")
            .send()
            .await
            .unwrap();
        let head_etag = head_resp.e_tag().unwrap().to_string();

        let get_resp = client
            .get_object()
            .bucket(&bucket)
            .key("etag")
            .send()
            .await
            .unwrap();
        let get_etag = get_resp.e_tag().unwrap().to_string();

        assert_eq!(put_etag, head_etag);
        assert_eq!(put_etag, get_etag);

        client
            .delete_object()
            .bucket(&bucket)
            .key("etag")
            .send()
            .await
            .unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

// ── Same content produces same ETag ──────────────────────────────────

#[test]
fn test_object_same_content_same_etag() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"deterministic content";

        let resp1 = client
            .put_object()
            .bucket(&bucket)
            .key("obj1")
            .body(ByteStream::from_static(body))
            .send()
            .await
            .unwrap();

        let resp2 = client
            .put_object()
            .bucket(&bucket)
            .key("obj2")
            .body(ByteStream::from_static(body))
            .send()
            .await
            .unwrap();

        assert_eq!(resp1.e_tag(), resp2.e_tag());

        client
            .delete_object()
            .bucket(&bucket)
            .key("obj1")
            .send()
            .await
            .unwrap();
        client
            .delete_object()
            .bucket(&bucket)
            .key("obj2")
            .send()
            .await
            .unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

// ── Special key names ────────────────────────────────────────────────

#[test]
fn test_object_key_with_slashes() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("a/b/c/d")
            .body(ByteStream::from_static(b"nested"))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("a/b/c/d")
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"nested");

        client
            .delete_object()
            .bucket(&bucket)
            .key("a/b/c/d")
            .send()
            .await
            .unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_object_key_with_spaces() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("hello world")
            .body(ByteStream::from_static(b"spaces"))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("hello world")
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"spaces");

        client
            .delete_object()
            .bucket(&bucket)
            .key("hello world")
            .send()
            .await
            .unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

// ── Cache-Control ────────────────────────────────────────────────────

#[test]
fn test_object_write_cache_control() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("cached")
            .cache_control("max-age=3600")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let resp = client
            .head_object()
            .bucket(&bucket)
            .key("cached")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.cache_control(), Some("max-age=3600"));

        client
            .delete_object()
            .bucket(&bucket)
            .key("cached")
            .send()
            .await
            .unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

// ── Content-Disposition ──────────────────────────────────────────────

#[test]
fn test_object_content_disposition() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("file")
            .content_disposition("attachment; filename=\"report.pdf\"")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let resp = client
            .head_object()
            .bucket(&bucket)
            .key("file")
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.content_disposition(),
            Some("attachment; filename=\"report.pdf\"")
        );

        client
            .delete_object()
            .bucket(&bucket)
            .key("file")
            .send()
            .await
            .unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

// ── Content-Encoding ─────────────────────────────────────────────────

#[test]
fn test_object_content_encoding() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("gzipped")
            .content_encoding("gzip")
            .body(ByteStream::from_static(b"compressed"))
            .send()
            .await
            .unwrap();

        let resp = client
            .head_object()
            .bucket(&bucket)
            .key("gzipped")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_encoding(), Some("gzip"));

        client
            .delete_object()
            .bucket(&bucket)
            .key("gzipped")
            .send()
            .await
            .unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

// ── Content-Language ─────────────────────────────────────────────────

#[test]
fn test_object_content_language() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("french")
            .content_language("fr")
            .body(ByteStream::from_static(b"bonjour"))
            .send()
            .await
            .unwrap();

        let resp = client
            .head_object()
            .bucket(&bucket)
            .key("french")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_language(), Some("fr"));

        client
            .delete_object()
            .bucket(&bucket)
            .key("french")
            .send()
            .await
            .unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

// ── HEAD zero-byte object ───────────────────────────────────────────

#[test]
fn test_object_head_zero_bytes() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("empty")
            .body(ByteStream::from_static(b""))
            .send()
            .await
            .unwrap();

        let resp = client
            .head_object()
            .bucket(&bucket)
            .key("empty")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_length(), Some(0));
        assert!(resp.e_tag().is_some());

        client
            .delete_object()
            .bucket(&bucket)
            .key("empty")
            .send()
            .await
            .unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

// ── Read with unreadable key ────────────────────────────────────────

#[test]
fn test_object_read_unreadable() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "\u{2680}";

        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"data");

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

// ── Expires header ──────────────────────────────────────────────────

#[test]
fn test_object_write_expires() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let expires = aws_sdk_s3::primitives::DateTime::from_secs(4_102_444_800); // 2100-01-01
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .expires(expires)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let resp = client
            .head_object()
            .bucket(&bucket)
            .key("obj")
            .send()
            .await
            .unwrap();
        assert!(resp.expires_string().is_some());

        client
            .delete_object()
            .bucket(&bucket)
            .key("obj")
            .send()
            .await
            .unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

// ── Full lifecycle: write → read → update → read → delete ──────────

#[test]
fn test_object_write_read_update_read_delete() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        // Write
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"v1"))
            .send()
            .await
            .unwrap();

        // Read
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"v1");

        // Update
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"v2"))
            .send()
            .await
            .unwrap();

        // Read again
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"v2");

        // Delete
        client
            .delete_object()
            .bucket(&bucket)
            .key("obj")
            .send()
            .await
            .unwrap();

        // Verify gone
        let result = client
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send()
            .await;
        assert!(result.is_err());

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

// ── Write to nonexistent bucket ─────────────────────────────────────

#[test]
fn test_object_write_to_nonexist_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();

        let result = client
            .put_object()
            .bucket(&bucket)
            .key("key")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await;
        assert!(result.is_err());
    });
}

// ── Not implemented ─────────────────────────────────────────────────

#[test]
#[ignore = "not implemented: ACL grants"]
fn test_object_header_acl_grants() {
    s3_tests::run(async {});
}

#[test]
#[ignore = "not implemented: chunked transfer encoding"]
fn test_object_write_with_chunked_transfer_encoding() {
    s3_tests::run(async {});
}
