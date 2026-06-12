//! Public access matrix tests for bucket ACL x object ACL behavior.
//!
//! These mirror the old Ceph matrix structure, but the expected write behavior
//! follows AWS: `public-read-write` allows a different signed account to create
//! a brand-new key, but not to overwrite an existing object.

use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketCannedAcl, ObjectCannedAcl, ObjectOwnership, OwnershipControls, OwnershipControlsRule,
};
use s3_tests::{
    assert_s3_err_code, delete_all_and_bucket, disable_bucket_public_access_block, err_status,
    retrying_operation_aborted, unique_bucket, SendRetryingOperationAborted, CTX,
};
use std::time::Duration;

const ACL_KEY: &str = "foo";
const DEFAULT_KEY: &str = "bar";
const NEW_KEY: &str = "new";
const SETUP_OPERATION_ATTEMPTS: usize = 20;

fn is_operation_aborted<E: ProvideErrorMetadata>(err: &aws_sdk_s3::error::SdkError<E>) -> bool {
    err.as_service_error().and_then(ProvideErrorMetadata::code) == Some("OperationAborted")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BucketAclCase {
    Private,
    PublicRead,
    PublicReadWrite,
}

impl BucketAclCase {
    fn canned_acl(self) -> BucketCannedAcl {
        match self {
            Self::Private => BucketCannedAcl::Private,
            Self::PublicRead => BucketCannedAcl::PublicRead,
            Self::PublicReadWrite => BucketCannedAcl::PublicReadWrite,
        }
    }

    const fn allows_list(self) -> bool {
        !matches!(self, Self::Private)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ObjectAclCase {
    Private,
    PublicRead,
    PublicReadWrite,
}

impl ObjectAclCase {
    fn canned_acl(self) -> ObjectCannedAcl {
        match self {
            Self::Private => ObjectCannedAcl::Private,
            Self::PublicRead => ObjectCannedAcl::PublicRead,
            Self::PublicReadWrite => ObjectCannedAcl::PublicReadWrite,
        }
    }

    const fn allows_read(self) -> bool {
        !matches!(self, Self::Private)
    }
}

#[derive(Clone, Copy, Debug)]
enum ListApi {
    V1,
    V2,
}

struct AccessMatrixFixture {
    bucket: String,
}

async fn set_bucket_owner_preferred(bucket: &str) {
    let rule = OwnershipControlsRule::builder()
        .object_ownership(ObjectOwnership::BucketOwnerPreferred)
        .build()
        .unwrap();
    let controls = OwnershipControls::builder().rules(rule).build().unwrap();

    retrying_operation_aborted(
        "put bucket ownership controls during ACL matrix setup",
        || {
            CTX.client()
                .put_bucket_ownership_controls()
                .bucket(bucket)
                .ownership_controls(controls.clone())
                .send()
        },
    )
    .await;
}

async fn put_object_retrying_operation_aborted(bucket: &str, key: &str, body: &'static [u8]) {
    retrying_operation_aborted("put object during ACL matrix setup", || {
        CTX.client()
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(body))
            .send()
    })
    .await;
}

async fn put_object_result_retrying_operation_aborted(
    bucket: &str,
    key: &str,
    body: &'static [u8],
) -> Result<
    aws_sdk_s3::operation::put_object::PutObjectOutput,
    aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::put_object::PutObjectError>,
> {
    for attempt in 0..SETUP_OPERATION_ATTEMPTS {
        match CTX
            .alt_client()
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(body))
            .send()
            .await
        {
            Err(err) if is_operation_aborted(&err) && attempt + 1 < SETUP_OPERATION_ATTEMPTS => {
                tokio::time::sleep(Duration::from_millis(10 * (attempt as u64 + 1))).await;
            }
            result => return result,
        }
    }
    unreachable!("put object result retry loop must return on final attempt");
}

async fn setup_access_matrix(
    bucket_acl: BucketAclCase,
    object_acl: ObjectAclCase,
) -> AccessMatrixFixture {
    let client = CTX.client();
    let bucket = unique_bucket();

    s3_tests::create_bucket(client, &bucket).await.unwrap();
    disable_bucket_public_access_block(client, &bucket).await;
    set_bucket_owner_preferred(&bucket).await;

    retrying_operation_aborted("put bucket ACL during ACL matrix setup", || {
        client
            .put_bucket_acl()
            .bucket(&bucket)
            .acl(bucket_acl.canned_acl())
            .send()
    })
    .await;

    put_object_retrying_operation_aborted(&bucket, ACL_KEY, b"foocontent").await;
    retrying_operation_aborted("put object ACL during ACL matrix setup", || {
        client
            .put_object_acl()
            .bucket(&bucket)
            .key(ACL_KEY)
            .acl(object_acl.canned_acl())
            .send()
    })
    .await;

    put_object_retrying_operation_aborted(&bucket, DEFAULT_KEY, b"barcontent").await;

    client
        .get_bucket_acl()
        .bucket(&bucket)
        .send_retrying_operation_aborted("get bucket ACL during ACL matrix setup")
        .await
        .unwrap();
    client
        .get_object_acl()
        .bucket(&bucket)
        .key(ACL_KEY)
        .send_retrying_operation_aborted("get object ACL during ACL matrix setup")
        .await
        .unwrap();

    AccessMatrixFixture { bucket }
}

fn assert_access_denied<T, E: std::fmt::Debug>(result: &Result<T, aws_sdk_s3::error::SdkError<E>>) {
    let status = err_status(result);
    assert_eq!(status, 403, "expected AccessDenied, got status {status}");
    assert_s3_err_code(result, "AccessDenied");
}

async fn assert_alt_get_object_body(bucket: &str, key: &str, expected_body: &[u8]) {
    let response = CTX
        .alt_client()
        .get_object()
        .bucket(bucket)
        .key(key)
        .send_retrying_operation_aborted("get object during ACL matrix test")
        .await
        .unwrap();
    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), expected_body);
}

async fn assert_alt_get_object_body_eventually(bucket: &str, key: &str, expected_body: &[u8]) {
    const MAX_ATTEMPTS: usize = 10;

    for attempt in 0..MAX_ATTEMPTS {
        match CTX
            .alt_client()
            .get_object()
            .bucket(bucket)
            .key(key)
            .send_retrying_operation_aborted("get object during ACL matrix test")
            .await
        {
            Ok(response) => {
                let body = response.body.collect().await.unwrap().into_bytes();
                if body.as_ref() == expected_body {
                    return;
                }
                if attempt + 1 < MAX_ATTEMPTS {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
                panic!(
                    "alt GetObject body did not converge for {bucket}/{key}: expected {:?}, got {:?}",
                    expected_body, body.as_ref()
                );
            }
            Err(_) if attempt + 1 < MAX_ATTEMPTS => {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(err) => panic!("alt GetObject failed for {bucket}/{key}: {err:?}"),
        }
    }
}

async fn assert_alt_get_object_denied(bucket: &str, key: &str) {
    let result = CTX
        .alt_client()
        .get_object()
        .bucket(bucket)
        .key(key)
        .send_retrying_operation_aborted("get object during ACL matrix test")
        .await;
    assert_access_denied(&result);
}

async fn assert_alt_get_object_denied_eventually(bucket: &str, key: &str) {
    const MAX_ATTEMPTS: usize = 10;

    for attempt in 0..MAX_ATTEMPTS {
        let result = CTX
            .alt_client()
            .get_object()
            .bucket(bucket)
            .key(key)
            .send_retrying_operation_aborted("get object during ACL matrix test")
            .await;
        if result.is_err() && err_status(&result) == 403 {
            assert_s3_err_code(&result, "AccessDenied");
            return;
        }
        if attempt + 1 < MAX_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }
        panic!("alt GetObject did not converge to AccessDenied for {bucket}/{key}: {result:?}");
    }
}

async fn assert_alt_put_object_denied(bucket: &str, key: &str, body: &'static [u8]) {
    let result = put_object_result_retrying_operation_aborted(bucket, key, body).await;
    assert_access_denied(&result);
}

async fn assert_alt_put_object_allowed(bucket: &str, key: &str, body: &'static [u8]) {
    retrying_operation_aborted("put object during ACL matrix test", || {
        CTX.alt_client()
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(body))
            .send()
    })
    .await;
}

async fn assert_alt_list_allowed(bucket: &str, api: ListApi) {
    let keys = match api {
        ListApi::V1 => CTX
            .alt_client()
            .list_objects()
            .bucket(bucket)
            .send_retrying_operation_aborted("list objects during ACL matrix test")
            .await
            .unwrap()
            .contents()
            .iter()
            .filter_map(|object| object.key().map(str::to_owned))
            .collect::<Vec<_>>(),
        ListApi::V2 => CTX
            .alt_client()
            .list_objects_v2()
            .bucket(bucket)
            .send_retrying_operation_aborted("list objects v2 during ACL matrix test")
            .await
            .unwrap()
            .contents()
            .iter()
            .filter_map(|object| object.key().map(str::to_owned))
            .collect::<Vec<_>>(),
    };

    assert_eq!(keys, vec![DEFAULT_KEY.to_string(), ACL_KEY.to_string()]);
}

async fn assert_alt_list_denied(bucket: &str, api: ListApi) {
    match api {
        ListApi::V1 => {
            let result = CTX
                .alt_client()
                .list_objects()
                .bucket(bucket)
                .send_retrying_operation_aborted("list objects during ACL matrix test")
                .await;
            assert_access_denied(&result);
        }
        ListApi::V2 => {
            let result = CTX
                .alt_client()
                .list_objects_v2()
                .bucket(bucket)
                .send_retrying_operation_aborted("list objects v2 during ACL matrix test")
                .await;
            assert_access_denied(&result);
        }
    }
}

async fn cleanup_access_matrix(bucket: &str) {
    delete_all_and_bucket(
        CTX.client(),
        bucket,
        &[
            ACL_KEY.to_string(),
            DEFAULT_KEY.to_string(),
            NEW_KEY.to_string(),
        ],
    )
    .await;
}

async fn run_access_matrix(
    bucket_acl: BucketAclCase,
    object_acl: ObjectAclCase,
    list_api: ListApi,
) {
    let fixture = setup_access_matrix(bucket_acl, object_acl).await;

    // The first data-plane assertion after the control-plane setup uses an
    // eventual helper: cross-account authorization can lag behind
    // PutBucketAcl / PutPublicAccessBlock / PutBucketOwnershipControls.
    // Once the first operation converges, subsequent assertions in the same
    // test can use the immediate variants because the authorization decision
    // has already propagated.
    if object_acl.allows_read() {
        assert_alt_get_object_body_eventually(&fixture.bucket, ACL_KEY, b"foocontent").await;
    } else if bucket_acl == BucketAclCase::Private {
        assert_alt_get_object_denied_eventually(&fixture.bucket, ACL_KEY).await;
    } else {
        assert_alt_get_object_denied(&fixture.bucket, ACL_KEY).await;
    }

    if bucket_acl == BucketAclCase::Private {
        assert_alt_get_object_denied(&fixture.bucket, DEFAULT_KEY).await;
        assert_alt_list_denied(&fixture.bucket, list_api).await;
        assert_alt_put_object_denied(&fixture.bucket, ACL_KEY, b"barcontent").await;
        assert_alt_put_object_denied(&fixture.bucket, DEFAULT_KEY, b"baroverwrite").await;
        assert_alt_put_object_denied(&fixture.bucket, NEW_KEY, b"newcontent").await;
        cleanup_access_matrix(&fixture.bucket).await;
        return;
    }

    // AWS allows a signed cross-account caller to create a brand-new object in
    // a public-write bucket, but not to overwrite an existing object.
    assert_alt_put_object_denied(&fixture.bucket, ACL_KEY, b"foooverwrite").await;

    assert_alt_get_object_denied(&fixture.bucket, DEFAULT_KEY).await;

    assert_alt_put_object_denied(&fixture.bucket, DEFAULT_KEY, b"baroverwrite").await;

    if bucket_acl.allows_list() {
        assert_alt_list_allowed(&fixture.bucket, list_api).await;
    } else {
        assert_alt_list_denied(&fixture.bucket, list_api).await;
    }

    if bucket_acl == BucketAclCase::PublicReadWrite {
        assert_alt_put_object_allowed(&fixture.bucket, NEW_KEY, b"newcontent").await;
        assert_alt_put_object_allowed(&fixture.bucket, NEW_KEY, b"overwritecontent").await;
        assert_alt_get_object_body(&fixture.bucket, NEW_KEY, b"overwritecontent").await;
    } else {
        assert_alt_put_object_denied(&fixture.bucket, NEW_KEY, b"newcontent").await;
    }

    cleanup_access_matrix(&fixture.bucket).await;
}

macro_rules! access_matrix_test {
    ($name:ident, $bucket_acl:expr, $object_acl:expr) => {
        #[test]
        fn $name() {
            s3_tests::run(async {
                run_access_matrix($bucket_acl, $object_acl, ListApi::V1).await;
            });
        }
    };
    ($name:ident, $bucket_acl:expr, $object_acl:expr, $list_api:expr) => {
        #[test]
        fn $name() {
            s3_tests::run(async {
                run_access_matrix($bucket_acl, $object_acl, $list_api).await;
            });
        }
    };
}

access_matrix_test!(
    test_access_bucket_private_object_private,
    BucketAclCase::Private,
    ObjectAclCase::Private
);
access_matrix_test!(
    test_access_bucket_private_objectv2_private,
    BucketAclCase::Private,
    ObjectAclCase::Private,
    ListApi::V2
);
access_matrix_test!(
    test_access_bucket_private_object_publicread,
    BucketAclCase::Private,
    ObjectAclCase::PublicRead
);
access_matrix_test!(
    test_access_bucket_private_objectv2_publicread,
    BucketAclCase::Private,
    ObjectAclCase::PublicRead,
    ListApi::V2
);
access_matrix_test!(
    test_access_bucket_private_object_publicreadwrite,
    BucketAclCase::Private,
    ObjectAclCase::PublicReadWrite
);
access_matrix_test!(
    test_access_bucket_private_objectv2_publicreadwrite,
    BucketAclCase::Private,
    ObjectAclCase::PublicReadWrite,
    ListApi::V2
);
access_matrix_test!(
    test_access_bucket_publicread_object_private,
    BucketAclCase::PublicRead,
    ObjectAclCase::Private
);
access_matrix_test!(
    test_access_bucket_publicread_object_publicread,
    BucketAclCase::PublicRead,
    ObjectAclCase::PublicRead
);
access_matrix_test!(
    test_access_bucket_publicread_object_publicreadwrite,
    BucketAclCase::PublicRead,
    ObjectAclCase::PublicReadWrite
);
access_matrix_test!(
    test_access_bucket_publicreadwrite_object_private,
    BucketAclCase::PublicReadWrite,
    ObjectAclCase::Private
);
access_matrix_test!(
    test_access_bucket_publicreadwrite_object_publicread,
    BucketAclCase::PublicReadWrite,
    ObjectAclCase::PublicRead
);
access_matrix_test!(
    test_access_bucket_publicreadwrite_object_publicreadwrite,
    BucketAclCase::PublicReadWrite,
    ObjectAclCase::PublicReadWrite
);
