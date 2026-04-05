use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    AccessControlPolicy, Grant, Grantee, ObjectCannedAcl, ObjectOwnership, Owner,
    OwnershipControls, OwnershipControlsRule, Permission, Type,
};
use aws_sdk_s3::Client;
use ring::{digest, hmac};
use s3_tests::{
    assert_s3_err_code, create_public_write_bucket, delete_all_and_bucket,
    disable_bucket_public_access_block, err_status, unique_bucket, CTX,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const ALL_USERS_GROUP_URI: &str = "http://acs.amazonaws.com/groups/global/AllUsers";
const AUTHENTICATED_USERS_GROUP_URI: &str =
    "http://acs.amazonaws.com/groups/global/AuthenticatedUsers";
const AWS_EXEC_READ_CANONICAL_ID: &str =
    "6aa5a366c34c1cbe25dc49211496e913e0351eb0e8c37aa3477e40942ec6b97c";

/// Create a bucket, returning its name. Tests are responsible for cleanup.
async fn setup_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    bucket
}

async fn set_object_writer_ownership(bucket: &str) {
    let rule = OwnershipControlsRule::builder()
        .object_ownership(ObjectOwnership::ObjectWriter)
        .build()
        .unwrap();
    let controls = OwnershipControls::builder().rules(rule).build().unwrap();
    CTX.client()
        .put_bucket_ownership_controls()
        .bucket(bucket)
        .ownership_controls(controls)
        .send()
        .await
        .unwrap();
}

async fn setup_acl_enabled_bucket() -> String {
    let client = CTX.client();
    let bucket = setup_bucket().await;
    disable_bucket_public_access_block(client, &bucket).await;
    set_object_writer_ownership(&bucket).await;
    client
        .get_public_access_block()
        .bucket(&bucket)
        .send()
        .await
        .unwrap();
    client
        .get_bucket_ownership_controls()
        .bucket(&bucket)
        .send()
        .await
        .unwrap();
    bucket
}

async fn canonical_owner_id(client: &Client) -> String {
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    let owner_id = client
        .get_bucket_acl()
        .bucket(&bucket)
        .send()
        .await
        .unwrap()
        .owner()
        .and_then(|owner| owner.id())
        .expect("expected owner ID in GetBucketAcl")
        .to_string();
    client.delete_bucket().bucket(&bucket).send().await.unwrap();
    owner_id
}

fn canonical_user_grant(canonical_user_id: &str, permission: Permission) -> Grant {
    Grant::builder()
        .grantee(
            Grantee::builder()
                .id(canonical_user_id)
                .r#type(Type::CanonicalUser)
                .build()
                .expect("canonical grantee"),
        )
        .permission(permission)
        .build()
}

fn access_control_policy(owner_id: &str, grants: Vec<Grant>) -> AccessControlPolicy {
    AccessControlPolicy::builder()
        .owner(Owner::builder().id(owner_id).build())
        .set_grants(Some(grants))
        .build()
}

fn has_grant(
    grants: &[Grant],
    permission: Permission,
    canonical_user_id: Option<&str>,
    uri: Option<&str>,
) -> bool {
    grants.iter().any(|grant| {
        grant.permission() == Some(&permission)
            && grant
                .grantee()
                .is_some_and(|grantee| grantee.id() == canonical_user_id && grantee.uri() == uri)
    })
}

fn assert_exact_grants(
    grants: &[Grant],
    expected: &[(Permission, Option<&str>, Option<&str>)],
    context: &str,
) {
    assert_eq!(
        grants.len(),
        expected.len(),
        "unexpected grant count for {context}: {grants:?}"
    );
    for (permission, canonical_user_id, uri) in expected {
        assert!(
            has_grant(grants, permission.clone(), *canonical_user_id, *uri),
            "missing grant {permission:?} id={canonical_user_id:?} uri={uri:?} for {context}: {grants:?}"
        );
    }
}

async fn object_owner_id(client: &Client, bucket: &str, key: &str) -> String {
    client
        .get_object_acl()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap()
        .owner()
        .and_then(|owner| owner.id())
        .expect("expected owner ID in GetObjectAcl")
        .to_string()
}

async fn bucket_owner_id(client: &Client, bucket: &str) -> String {
    client
        .get_bucket_acl()
        .bucket(bucket)
        .send()
        .await
        .unwrap()
        .owner()
        .and_then(|owner| owner.id())
        .expect("expected owner ID in GetBucketAcl")
        .to_string()
}

async fn run_object_header_acl_grants_case(key: &str, body: Vec<u8>) {
    let client = CTX.client();
    let alt_client = CTX.alt_client();

    let bucket = setup_bucket().await;
    set_object_writer_ownership(&bucket).await;
    let alt_owner_id = canonical_owner_id(alt_client).await;

    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(body.clone()))
        .customize()
        .mutate_request({
            let alt_owner_id = alt_owner_id.clone();
            move |req| {
                req.headers_mut().insert(
                    "x-amz-grant-read",
                    format!("id=\"{}\"", alt_owner_id.clone()),
                );
                req.headers_mut().insert(
                    "x-amz-grant-read-acp",
                    format!("id=\"{}\"", alt_owner_id.clone()),
                );
                req.headers_mut().insert(
                    "x-amz-grant-write-acp",
                    format!("id=\"{}\"", alt_owner_id.clone()),
                );
                req.headers_mut().insert(
                    "x-amz-grant-full-control",
                    format!("id=\"{}\"", alt_owner_id.clone()),
                );
            }
        })
        .send()
        .await
        .unwrap();

    let acl = client
        .get_object_acl()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    let grants = acl.grants();
    assert!(has_grant(
        grants,
        Permission::Read,
        Some(&alt_owner_id),
        None
    ));
    assert!(has_grant(
        grants,
        Permission::ReadAcp,
        Some(&alt_owner_id),
        None
    ));
    assert!(has_grant(
        grants,
        Permission::WriteAcp,
        Some(&alt_owner_id),
        None
    ));
    assert!(has_grant(
        grants,
        Permission::FullControl,
        Some(&alt_owner_id),
        None
    ));
    let owner_id = acl
        .owner()
        .and_then(|owner| owner.id())
        .expect("expected owner ID in GetObjectAcl")
        .to_string();
    assert_eq!(
        grants.len(),
        4,
        "expected exact explicit grants without implicit owner FULL_CONTROL, got {grants:?}"
    );
    assert!(
        !has_grant(grants, Permission::FullControl, Some(&owner_id), None),
        "did not expect implicit owner FULL_CONTROL grant in {grants:?}"
    );

    let read = alt_get_object_eventually(&bucket, key).await;
    let read_body = read.body.collect().await.unwrap().into_bytes();
    assert_eq!(&read_body[..], body.as_slice());

    alt_client
        .get_object_acl()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .unwrap();

    alt_client
        .put_object_acl()
        .bucket(&bucket)
        .key(key)
        .access_control_policy(access_control_policy(
            &owner_id,
            vec![
                canonical_user_grant(&owner_id, Permission::FullControl),
                canonical_user_grant(&alt_owner_id, Permission::Read),
                canonical_user_grant(&alt_owner_id, Permission::ReadAcp),
                canonical_user_grant(&alt_owner_id, Permission::WriteAcp),
                canonical_user_grant(&alt_owner_id, Permission::FullControl),
            ],
        ))
        .send()
        .await
        .unwrap();

    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    client.delete_bucket().bucket(&bucket).send().await.unwrap();
}

async fn alt_get_object_eventually(
    bucket: &str,
    key: &str,
) -> aws_sdk_s3::operation::get_object::GetObjectOutput {
    const MAX_ATTEMPTS: usize = 10;

    for attempt in 0..MAX_ATTEMPTS {
        match CTX
            .alt_client()
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
        {
            Ok(output) => return output,
            Err(_) if attempt + 1 < MAX_ATTEMPTS => {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(err) => panic!("alternate GetObject failed unexpectedly: {err:?}"),
        }
    }

    unreachable!()
}

async fn assert_alt_get_object_allowed(bucket: &str, key: &str, expected_body: &[u8]) {
    let get = CTX
        .alt_client()
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    let body = get.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), expected_body);
}

async fn assert_alt_get_object_denied(bucket: &str, key: &str) {
    let result = CTX
        .alt_client()
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await;
    assert_eq!(err_status(&result), 403);
}

async fn assert_alt_get_object_acl_allowed(bucket: &str, key: &str) {
    CTX.alt_client()
        .get_object_acl()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap();
}

async fn assert_alt_get_object_acl_denied(bucket: &str, key: &str) {
    let result = CTX
        .alt_client()
        .get_object_acl()
        .bucket(bucket)
        .key(key)
        .send()
        .await;
    assert_eq!(err_status(&result), 403);
}

async fn assert_alt_put_object_acl_allowed(
    bucket: &str,
    key: &str,
    owner_id: &str,
    alt_owner_id: &str,
    alt_permission: Permission,
) {
    CTX.alt_client()
        .put_object_acl()
        .bucket(bucket)
        .key(key)
        .access_control_policy(access_control_policy(
            owner_id,
            vec![
                canonical_user_grant(owner_id, Permission::FullControl),
                canonical_user_grant(alt_owner_id, alt_permission),
            ],
        ))
        .send()
        .await
        .unwrap();
}

async fn assert_alt_put_object_acl_denied(bucket: &str, key: &str) {
    let result = CTX
        .alt_client()
        .put_object_acl()
        .bucket(bucket)
        .key(key)
        .acl(ObjectCannedAcl::Private)
        .send()
        .await;
    assert_eq!(err_status(&result), 403);
}

async fn setup_object_with_alt_acl_grant(permission: Permission) -> (String, String, String) {
    let client = CTX.client();
    let alt_client = CTX.alt_client();
    let bucket = setup_acl_enabled_bucket().await;

    client
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await
        .unwrap();

    let owner_id = object_owner_id(client, &bucket, "foo").await;
    let alt_owner_id = canonical_owner_id(alt_client).await;
    client
        .put_object_acl()
        .bucket(&bucket)
        .key("foo")
        .access_control_policy(access_control_policy(
            &owner_id,
            vec![
                canonical_user_grant(&owner_id, Permission::FullControl),
                canonical_user_grant(&alt_owner_id, permission),
            ],
        ))
        .send()
        .await
        .unwrap();

    (bucket, owner_id, alt_owner_id)
}

async fn run_object_acl_canonical_user_permission_case(
    permission: Permission,
    expect_get_object: bool,
    expect_get_object_acl: bool,
    expect_put_object_acl: bool,
) {
    let client = CTX.client();
    let (bucket, owner_id, alt_owner_id) =
        setup_object_with_alt_acl_grant(permission.clone()).await;

    let acl = client
        .get_object_acl()
        .bucket(&bucket)
        .key("foo")
        .send()
        .await
        .unwrap();
    assert_exact_grants(
        acl.grants(),
        &[
            (Permission::FullControl, Some(owner_id.as_str()), None),
            (permission.clone(), Some(alt_owner_id.as_str()), None),
        ],
        "object ACL canonical-user grant matrix setup",
    );

    if expect_get_object {
        assert_alt_get_object_allowed(&bucket, "foo", b"bar").await;
    } else {
        assert_alt_get_object_denied(&bucket, "foo").await;
    }

    if expect_get_object_acl {
        assert_alt_get_object_acl_allowed(&bucket, "foo").await;
    } else {
        assert_alt_get_object_acl_denied(&bucket, "foo").await;
    }

    if expect_put_object_acl {
        assert_alt_put_object_acl_allowed(&bucket, "foo", &owner_id, &alt_owner_id, permission)
            .await;
    } else {
        assert_alt_put_object_acl_denied(&bucket, "foo").await;
    }

    delete_all_and_bucket(client, &bucket, &["foo".to_string()]).await;
}

fn agent() -> ureq::Agent {
    s3_tests::test_agent()
}

fn sha256_hex(data: &[u8]) -> String {
    let d = digest::digest(&digest::SHA256, data);
    d.as_ref().iter().map(|b| format!("{:02x}", b)).collect()
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> hmac::Tag {
    hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), data)
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

fn derive_signing_key(secret: &str, date: &str, region: &str, service: &str) -> hmac::Tag {
    let k_secret = format!("AWS4{}", secret);
    let k_date = hmac_sha256(k_secret.as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(k_date.as_ref(), region.as_bytes());
    let k_service = hmac_sha256(k_region.as_ref(), service.as_bytes());
    hmac_sha256(k_service.as_ref(), b"aws4_request")
}

fn days_to_ymd(days: u64) -> (u64, u64, u64) {
    let z = days + 719468;
    let era = z / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

fn host() -> &'static str {
    CTX.endpoint()
        .trim_start_matches("http://")
        .trim_start_matches("https://")
}

struct SignedHeaders {
    authorization: String,
    amz_date: String,
    amz_content_sha256: String,
}

fn signed_put_with_content_encoding(bucket: &str, key: &str, body: &[u8], content_encoding: &str) {
    let path = format!("/{}/{}", bucket, key);
    let body_hash = sha256_hex(body);
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let days = secs / 86400;
    let (year, month, day) = days_to_ymd(days);
    let time_of_day = secs % 86400;
    let hour = time_of_day / 3600;
    let minute = (time_of_day % 3600) / 60;
    let second = time_of_day % 60;
    let date_long = format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        year, month, day, hour, minute, second
    );
    let date_short = &date_long[..8];
    let host_val = host();
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let canonical_headers = format!(
        "host:{}\nx-amz-content-sha256:{}\nx-amz-date:{}\n",
        host_val, body_hash, date_long
    );
    let canonical_request = format!(
        "PUT\n{}\n\n{}\n{}\n{}",
        path, canonical_headers, signed_headers, body_hash
    );
    let canonical_hash = sha256_hex(canonical_request.as_bytes());
    let scope = format!("{}/{}/s3/aws4_request", date_short, CTX.region());
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        date_long, scope, canonical_hash
    );
    let signing_key = derive_signing_key(CTX.secret_key(), date_short, CTX.region(), "s3");
    let signature = hmac_sha256(signing_key.as_ref(), string_to_sign.as_bytes());
    let headers = SignedHeaders {
        authorization: format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            CTX.access_key(),
            scope,
            signed_headers,
            hex_encode(signature.as_ref())
        ),
        amz_date: date_long,
        amz_content_sha256: body_hash,
    };
    let url = format!("{}{}", CTX.endpoint(), path);
    let resp = agent()
        .put(&url)
        .header("Authorization", &headers.authorization)
        .header("x-amz-date", &headers.amz_date)
        .header("x-amz-content-sha256", &headers.amz_content_sha256)
        .header("Content-Encoding", content_encoding)
        .send(body)
        .expect("transport error");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "plain signed PUT with Content-Encoding should succeed"
    );
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
fn test_get_object_expected_bucket_owner() {
    s3_tests::run(async {
        let account_id = CTX.account_id().to_string();
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"hello expected owner";

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
            .customize()
            .mutate_request({
                let account_id = account_id.clone();
                move |req| {
                    req.headers_mut()
                        .insert("x-amz-expected-bucket-owner", account_id.clone());
                }
            })
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
fn test_get_object_wrong_expected_bucket_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("testobj")
            .body(ByteStream::from_static(b"hello expected owner"))
            .send()
            .await
            .unwrap();

        let result = client
            .get_object()
            .bucket(&bucket)
            .key("testobj")
            .customize()
            .mutate_request(|req| {
                req.headers_mut()
                    .insert("x-amz-expected-bucket-owner", "000000000000");
            })
            .send()
            .await;
        assert_eq!(err_status(&result), 403);
        assert_s3_err_code(&result, "AccessDenied");

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
        assert_eq!(err_status(&result), 404);

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

/// Empty metadata value should be stored and retrieved as empty string.
#[test]
fn test_object_metadata_empty_value() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("meta-empty")
            .metadata("meta1", "")
            .body(ByteStream::from_static(b"bar"))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("meta-empty")
            .send()
            .await
            .unwrap();

        let metadata = resp.metadata().unwrap();
        assert_eq!(metadata.get("meta1").map(|s| s.as_str()), Some(""));

        client
            .delete_object()
            .bucket(&bucket)
            .key("meta-empty")
            .send()
            .await
            .unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

/// Overwriting metadata with empty value replaces the old value.
#[test]
fn test_object_metadata_overwrite_to_empty() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        // First put with a non-empty metadata value
        client
            .put_object()
            .bucket(&bucket)
            .key("foo")
            .metadata("meta1", "oldmeta")
            .body(ByteStream::from_static(b"bar"))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.metadata().unwrap().get("meta1").map(|s| s.as_str()),
            Some("oldmeta")
        );

        // Overwrite with empty metadata value
        client
            .put_object()
            .bucket(&bucket)
            .key("foo")
            .metadata("meta1", "")
            .body(ByteStream::from_static(b"bar"))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.metadata().unwrap().get("meta1").map(|s| s.as_str()),
            Some("")
        );

        client
            .delete_object()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

/// Re-putting an object without metadata clears all previous metadata.
#[test]
fn test_object_metadata_replaced_on_put() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        // Put with metadata
        client
            .put_object()
            .bucket(&bucket)
            .key("foo")
            .metadata("meta1", "bar")
            .body(ByteStream::from_static(b"bar"))
            .send()
            .await
            .unwrap();

        // Re-put same key without any metadata
        client
            .put_object()
            .bucket(&bucket)
            .key("foo")
            .body(ByteStream::from_static(b"bar"))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();

        // Metadata should be empty (or None)
        let metadata = resp.metadata();
        let is_empty = metadata.is_none() || metadata.unwrap().is_empty();
        assert!(
            is_empty,
            "metadata should be cleared on re-put without metadata"
        );

        client
            .delete_object()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

/// AWS accepts non-ASCII (unicode) metadata values.
#[test]
fn test_object_metadata_unicode_accepted() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let unicode_value = "Hello World\u{e9}"; // "Hello Worldé"
        client
            .put_object()
            .bucket(&bucket)
            .key("unicode-meta")
            .metadata("meta1", unicode_value)
            .body(ByteStream::from_static(b"bar"))
            .send()
            .await
            .unwrap();

        // Verify round-trip: both AWS and our server RFC 2047 encode
        // non-ASCII metadata values in response headers.
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("unicode-meta")
            .send()
            .await
            .unwrap();
        let meta = resp.metadata().unwrap();
        let returned = meta.get("meta1").unwrap();
        assert_eq!(
            returned, "=?UTF-8?Q?Hello_World=C3=83=C2=A9?=",
            "expected RFC 2047 Q-encoded value, got: {}",
            returned
        );

        client
            .delete_object()
            .bucket(&bucket)
            .key("unicode-meta")
            .send()
            .await
            .unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_object_metadata_too_large() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let oversized = "m".repeat(3000);

        let result = client
            .put_object()
            .bucket(&bucket)
            .key("metadata-too-large")
            .metadata("mint-test", oversized)
            .body(ByteStream::from_static(b""))
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "MetadataTooLarge");

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
        let result = client.get_object().bucket(&bucket).key("obj").send().await;
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
        assert_eq!(err_status(&result), 404);
    });
}

// ── Content-Encoding aws-chunked stripping ──────────────────────────

/// Port of Ceph test_object_content_encoding_aws_chunked.
/// Plain user-supplied Content-Encoding values are stored verbatim; the
/// transport-only aws-chunked token is stripped only for actual aws-chunked
/// streaming uploads.
#[test]
fn test_object_content_encoding_aws_chunked() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "ce-test";

        // 1. gzip only — returned as-is
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .content_encoding("gzip")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();
        let resp = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_encoding(), Some("gzip"));

        // 2. deflate, gzip — returned as-is
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .content_encoding("deflate, gzip")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();
        let resp = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_encoding(), Some("deflate, gzip"));

        // 3. gzip, aws-chunked — stored as-is for a plain non-streaming PUT.
        signed_put_with_content_encoding(&bucket, key, b"data", "gzip, aws-chunked");
        let resp = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_encoding(), Some("gzip, aws-chunked"));

        // 4. aws-chunked, gzip — stored as-is for a plain non-streaming PUT.
        signed_put_with_content_encoding(&bucket, key, b"data", "aws-chunked, gzip");
        let resp = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_encoding(), Some("aws-chunked, gzip"));

        // 5. aws-chunked only — stored as-is for a plain non-streaming PUT.
        signed_put_with_content_encoding(&bucket, key, b"data", "aws-chunked");
        let resp = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_encoding(), Some("aws-chunked"));

        // Cleanup
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

#[test]
fn test_object_acl_default() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("foo")
            .body(ByteStream::from_static(b"bar"))
            .send()
            .await
            .unwrap();

        let acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();
        let owner_id = acl
            .owner()
            .and_then(|owner| owner.id())
            .expect("expected owner ID in GetObjectAcl")
            .to_string();
        assert_exact_grants(
            acl.grants(),
            &[(Permission::FullControl, Some(owner_id.as_str()), None)],
            "default object ACL",
        );

        delete_all_and_bucket(client, &bucket, &["foo".to_string()]).await;
    });
}

#[test]
fn test_object_acl_canned_during_create() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_acl_enabled_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("foo")
            .acl(ObjectCannedAcl::PublicRead)
            .body(ByteStream::from_static(b"bar"))
            .send()
            .await
            .unwrap();

        let acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();
        let owner_id = acl
            .owner()
            .and_then(|owner| owner.id())
            .expect("expected owner ID in GetObjectAcl")
            .to_string();
        assert_exact_grants(
            acl.grants(),
            &[
                (Permission::Read, None, Some(ALL_USERS_GROUP_URI)),
                (Permission::FullControl, Some(owner_id.as_str()), None),
            ],
            "public-read object ACL during create",
        );

        delete_all_and_bucket(client, &bucket, &["foo".to_string()]).await;
    });
}

#[test]
fn test_put_object_acl_canned_private_round_trip() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        set_object_writer_ownership(&bucket).await;

        client
            .put_object()
            .bucket(&bucket)
            .key("foo")
            .body(ByteStream::from_static(b"bar"))
            .send()
            .await
            .unwrap();

        client
            .put_object_acl()
            .bucket(&bucket)
            .key("foo")
            .acl(ObjectCannedAcl::Private)
            .send()
            .await
            .unwrap();

        let acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();
        let owner_id = acl
            .owner()
            .and_then(|owner| owner.id())
            .expect("expected owner ID in GetObjectAcl")
            .to_string();
        assert_exact_grants(
            acl.grants(),
            &[(Permission::FullControl, Some(owner_id.as_str()), None)],
            "private object ACL via PutObjectAcl",
        );

        delete_all_and_bucket(client, &bucket, &["foo".to_string()]).await;
    });
}

#[test]
fn test_put_object_acl_canned_public_read_write_round_trip() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_acl_enabled_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("foo")
            .body(ByteStream::from_static(b"bar"))
            .send()
            .await
            .unwrap();

        client
            .put_object_acl()
            .bucket(&bucket)
            .key("foo")
            .acl(ObjectCannedAcl::PublicReadWrite)
            .send()
            .await
            .unwrap();

        let acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();
        let owner_id = acl
            .owner()
            .and_then(|owner| owner.id())
            .expect("expected owner ID in GetObjectAcl")
            .to_string();
        assert_exact_grants(
            acl.grants(),
            &[
                (Permission::Read, None, Some(ALL_USERS_GROUP_URI)),
                (Permission::Write, None, Some(ALL_USERS_GROUP_URI)),
                (Permission::FullControl, Some(owner_id.as_str()), None),
            ],
            "public-read-write object ACL via PutObjectAcl",
        );

        let mut anon_get = agent()
            .get(&format!("{}/{}/foo", CTX.endpoint(), bucket))
            .call()
            .expect("anonymous GET transport error");
        assert_eq!(anon_get.status().as_u16(), 200);
        assert_eq!(anon_get.body_mut().read_to_string().unwrap(), "bar");

        delete_all_and_bucket(client, &bucket, &["foo".to_string()]).await;
    });
}

#[test]
fn test_put_object_acl_canned_authenticated_read_round_trip() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_acl_enabled_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("foo")
            .body(ByteStream::from_static(b"bar"))
            .send()
            .await
            .unwrap();

        client
            .put_object_acl()
            .bucket(&bucket)
            .key("foo")
            .acl(ObjectCannedAcl::AuthenticatedRead)
            .send()
            .await
            .unwrap();

        let acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();
        let owner_id = acl
            .owner()
            .and_then(|owner| owner.id())
            .expect("expected owner ID in GetObjectAcl")
            .to_string();
        assert_exact_grants(
            acl.grants(),
            &[
                (Permission::Read, None, Some(AUTHENTICATED_USERS_GROUP_URI)),
                (Permission::FullControl, Some(owner_id.as_str()), None),
            ],
            "authenticated-read object ACL via PutObjectAcl",
        );

        let get = alt_get_object_eventually(&bucket, "foo").await;
        let body = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.as_ref(), b"bar");

        delete_all_and_bucket(client, &bucket, &["foo".to_string()]).await;
    });
}

#[test]
fn test_object_acl_canned_aws_exec_read_during_create() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_acl_enabled_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("foo")
            .acl(ObjectCannedAcl::AwsExecRead)
            .body(ByteStream::from_static(b"bar"))
            .send()
            .await
            .unwrap();

        let acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();
        let owner_id = acl
            .owner()
            .and_then(|owner| owner.id())
            .expect("expected owner ID in GetObjectAcl")
            .to_string();
        assert_exact_grants(
            acl.grants(),
            &[
                (Permission::Read, Some(AWS_EXEC_READ_CANONICAL_ID), None),
                (Permission::FullControl, Some(owner_id.as_str()), None),
            ],
            "aws-exec-read object ACL during create",
        );

        delete_all_and_bucket(client, &bucket, &["foo".to_string()]).await;
    });
}

#[test]
fn test_put_object_acl_canned_bucket_owner_read_round_trip() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_public_write_bucket(client).await;

        alt_client
            .put_object()
            .bucket(&bucket)
            .key("foo")
            .body(ByteStream::from_static(b"bar"))
            .send()
            .await
            .unwrap();

        alt_client
            .put_object_acl()
            .bucket(&bucket)
            .key("foo")
            .acl(ObjectCannedAcl::BucketOwnerRead)
            .send()
            .await
            .unwrap();

        let acl = alt_client
            .get_object_acl()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();
        let alt_owner_id = acl
            .owner()
            .and_then(|owner| owner.id())
            .expect("expected object owner ID in GetObjectAcl")
            .to_string();
        let bucket_owner_id = bucket_owner_id(client, &bucket).await;
        assert_exact_grants(
            acl.grants(),
            &[
                (Permission::FullControl, Some(alt_owner_id.as_str()), None),
                (Permission::Read, Some(bucket_owner_id.as_str()), None),
            ],
            "bucket-owner-read object ACL via PutObjectAcl",
        );

        delete_all_and_bucket(client, &bucket, &["foo".to_string()]).await;
    });
}

#[test]
fn test_put_object_acl_canned_aws_exec_read_round_trip() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_acl_enabled_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("foo")
            .body(ByteStream::from_static(b"bar"))
            .send()
            .await
            .unwrap();

        client
            .put_object_acl()
            .bucket(&bucket)
            .key("foo")
            .acl(ObjectCannedAcl::AwsExecRead)
            .send()
            .await
            .unwrap();

        let acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();
        let owner_id = acl
            .owner()
            .and_then(|owner| owner.id())
            .expect("expected owner ID in GetObjectAcl")
            .to_string();
        assert_exact_grants(
            acl.grants(),
            &[
                (Permission::Read, Some(AWS_EXEC_READ_CANONICAL_ID), None),
                (Permission::FullControl, Some(owner_id.as_str()), None),
            ],
            "aws-exec-read object ACL via PutObjectAcl",
        );

        delete_all_and_bucket(client, &bucket, &["foo".to_string()]).await;
    });
}

#[test]
fn test_put_object_acl_canned_bucket_owner_full_control_round_trip() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_public_write_bucket(client).await;

        alt_client
            .put_object()
            .bucket(&bucket)
            .key("foo")
            .body(ByteStream::from_static(b"bar"))
            .send()
            .await
            .unwrap();

        alt_client
            .put_object_acl()
            .bucket(&bucket)
            .key("foo")
            .acl(ObjectCannedAcl::BucketOwnerFullControl)
            .send()
            .await
            .unwrap();

        let acl = alt_client
            .get_object_acl()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();
        let alt_owner_id = acl
            .owner()
            .and_then(|owner| owner.id())
            .expect("expected object owner ID in GetObjectAcl")
            .to_string();
        let bucket_owner_id = bucket_owner_id(client, &bucket).await;
        assert_exact_grants(
            acl.grants(),
            &[
                (Permission::FullControl, Some(alt_owner_id.as_str()), None),
                (
                    Permission::FullControl,
                    Some(bucket_owner_id.as_str()),
                    None,
                ),
            ],
            "bucket-owner-full-control object ACL via PutObjectAcl",
        );

        client
            .get_object_acl()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();

        delete_all_and_bucket(client, &bucket, &["foo".to_string()]).await;
    });
}

#[test]
fn test_put_object_acl_without_content_length_header() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_acl_enabled_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("foo")
            .body(ByteStream::from_static(b"bar"))
            .send()
            .await
            .unwrap();

        client
            .put_object_acl()
            .bucket(&bucket)
            .key("foo")
            .acl(ObjectCannedAcl::PublicRead)
            .customize()
            .mutate_request(|req| {
                req.headers_mut().remove("content-length");
            })
            .send()
            .await
            .unwrap();

        let acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();
        let owner_id = acl
            .owner()
            .and_then(|owner| owner.id())
            .expect("expected owner ID in GetObjectAcl")
            .to_string();
        assert_exact_grants(
            acl.grants(),
            &[
                (Permission::Read, None, Some(ALL_USERS_GROUP_URI)),
                (Permission::FullControl, Some(owner_id.as_str()), None),
            ],
            "PutObjectAcl without Content-Length",
        );

        delete_all_and_bucket(client, &bucket, &["foo".to_string()]).await;
    });
}

#[test]
fn test_put_object_acl_full_control_verify_attributes() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = setup_bucket().await;
        set_object_writer_ownership(&bucket).await;

        client
            .put_object()
            .bucket(&bucket)
            .key("foo")
            .body(ByteStream::from_static(b"bar"))
            .send()
            .await
            .unwrap();

        let owner_id = object_owner_id(client, &bucket, "foo").await;
        let alt_owner_id = canonical_owner_id(alt_client).await;
        client
            .put_object_acl()
            .bucket(&bucket)
            .key("foo")
            .access_control_policy(access_control_policy(
                &owner_id,
                vec![
                    canonical_user_grant(&owner_id, Permission::FullControl),
                    canonical_user_grant(&alt_owner_id, Permission::FullControl),
                ],
            ))
            .send()
            .await
            .unwrap();

        let acl = alt_client
            .get_object_acl()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();
        assert_exact_grants(
            acl.grants(),
            &[
                (Permission::FullControl, Some(owner_id.as_str()), None),
                (Permission::FullControl, Some(alt_owner_id.as_str()), None),
            ],
            "cross-account FULL_CONTROL object ACL",
        );

        let get = alt_client
            .get_object()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();
        let body = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.as_ref(), b"bar");

        delete_all_and_bucket(client, &bucket, &["foo".to_string()]).await;
    });
}

#[test]
fn test_object_header_acl_grants() {
    s3_tests::run(async {
        run_object_header_acl_grants_case("testobj", b"header-acl".to_vec()).await;
    });
}

#[test]
fn test_object_header_acl_grants_streaming_put() {
    s3_tests::run(async {
        let body = vec![0x5Au8; server_core::coordinator::INTERNAL_SEGMENT_SIZE + 1];
        run_object_header_acl_grants_case("streaming-testobj", body).await;
    });
}

#[test]
fn test_object_header_acl_grants_authenticated_users_read() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_acl_enabled_bucket().await;
        let key = "auth-users-header-grant";

        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"authenticated-read"))
            .customize()
            .mutate_request(move |req| {
                req.headers_mut().insert(
                    "x-amz-grant-read",
                    format!("uri=\"{}\"", AUTHENTICATED_USERS_GROUP_URI),
                );
            })
            .send()
            .await
            .unwrap();

        let acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_exact_grants(
            acl.grants(),
            &[(Permission::Read, None, Some(AUTHENTICATED_USERS_GROUP_URI))],
            "authenticated users object ACL via header grant",
        );

        let resp = alt_get_object_eventually(&bucket, key).await;
        let body = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.as_ref(), b"authenticated-read");

        let mut anon = agent()
            .get(&format!("{}/{}/{}", CTX.endpoint(), bucket, key))
            .call()
            .expect("anonymous GET transport error");
        assert_eq!(anon.status().as_u16(), 403);
        let anon_body = anon.body_mut().read_to_string().unwrap();
        assert!(
            anon_body.contains("AccessDenied"),
            "expected AccessDenied for anonymous GET, got {anon_body}"
        );

        delete_all_and_bucket(client, &bucket, &[key.to_string()]).await;
    });
}

#[test]
fn test_put_object_acl_explicit_grants_do_not_add_owner_full_control() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = setup_acl_enabled_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("foo")
            .body(ByteStream::from_static(b"bar"))
            .send()
            .await
            .unwrap();

        let owner_id = object_owner_id(client, &bucket, "foo").await;
        let alt_owner_id = canonical_owner_id(alt_client).await;
        client
            .put_object_acl()
            .bucket(&bucket)
            .key("foo")
            .access_control_policy(access_control_policy(
                &owner_id,
                vec![canonical_user_grant(&alt_owner_id, Permission::FullControl)],
            ))
            .send()
            .await
            .unwrap();

        let acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();
        assert_exact_grants(
            acl.grants(),
            &[(Permission::FullControl, Some(alt_owner_id.as_str()), None)],
            "explicit PutObjectAcl grant without implicit owner FULL_CONTROL",
        );

        alt_client
            .get_object_acl()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();

        delete_all_and_bucket(client, &bucket, &["foo".to_string()]).await;
    });
}

#[test]
fn test_object_acl_grant_canonical_user_read() {
    s3_tests::run(async {
        run_object_acl_canonical_user_permission_case(Permission::Read, true, false, false).await;
    });
}

#[test]
fn test_object_acl_grant_canonical_user_read_acp() {
    s3_tests::run(async {
        run_object_acl_canonical_user_permission_case(Permission::ReadAcp, false, true, false)
            .await;
    });
}

#[test]
fn test_object_acl_grant_canonical_user_write_acp() {
    s3_tests::run(async {
        run_object_acl_canonical_user_permission_case(Permission::WriteAcp, false, false, true)
            .await;
    });
}

#[test]
fn test_object_acl_grant_canonical_user_full_control() {
    s3_tests::run(async {
        run_object_acl_canonical_user_permission_case(Permission::FullControl, true, true, true)
            .await;
    });
}
