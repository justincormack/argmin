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
    delete_all_and_bucket, disable_bucket_public_access_block, retrying_operation_aborted,
    retrying_operation_aborted_result, unique_bucket, SendRetryingOperationAborted, CTX,
};
use std::time::Duration;

const ACL_KEY: &str = "foo";
const DEFAULT_KEY: &str = "bar";
const NEW_KEY: &str = "new";
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
    retrying_operation_aborted_result(|| {
        CTX.alt_client()
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(body))
            .send()
    })
    .await
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

fn assert_access_denied<T, E: ProvideErrorMetadata + std::fmt::Debug>(
    context: &str,
    result: &Result<T, aws_sdk_s3::error::SdkError<E>>,
) {
    let err = match result {
        Ok(_) => panic!("{context}: expected AccessDenied, got success"),
        Err(err) => err,
    };
    let status = err
        .raw_response()
        .map(|response| response.status().as_u16())
        .unwrap_or_else(|| panic!("{context}: error has no raw HTTP response: {err:?}"));
    assert_eq!(
        status, 403,
        "{context}: expected AccessDenied, got status {status}: {err:?}"
    );
    let code = err.as_service_error().and_then(ProvideErrorMetadata::code);
    assert_eq!(
        code,
        Some("AccessDenied"),
        "{context}: expected AccessDenied, got code {code:?}: {err:?}"
    );
}

async fn assert_alt_get_object_body(context: &str, bucket: &str, key: &str, expected_body: &[u8]) {
    let response = CTX
        .alt_client()
        .get_object()
        .bucket(bucket)
        .key(key)
        .send_retrying_operation_aborted("get object during ACL matrix test")
        .await
        .unwrap_or_else(|err| {
            panic!("{context}: alt GetObject failed for {bucket}/{key}: {err:?}")
        });
    let body = response
        .body
        .collect()
        .await
        .unwrap_or_else(|err| {
            panic!("{context}: collect alt GetObject body for {bucket}/{key}: {err:?}")
        })
        .into_bytes();
    assert_eq!(
        body.as_ref(),
        expected_body,
        "{context}: unexpected alt GetObject body for {bucket}/{key}"
    );
}

async fn assert_alt_get_object_body_eventually(
    context: &str,
    bucket: &str,
    key: &str,
    expected_body: &[u8],
) {
    const MAX_ATTEMPTS: usize = 10;
    let mut observations = Vec::new();

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
                let body = match response.body.collect().await {
                    Ok(body) => body.into_bytes(),
                    Err(err) => {
                        observations.push(format!(
                            "attempt {} body collection error={err:?}",
                            attempt + 1
                        ));
                        if attempt + 1 < MAX_ATTEMPTS {
                            tokio::time::sleep(Duration::from_millis(200)).await;
                            continue;
                        }
                        panic!(
                            "{context}: alt GetObject did not converge for {bucket}/{key}: expected body {:?}; observations: {observations:?}",
                            expected_body
                        );
                    }
                };
                if body.as_ref() == expected_body {
                    return;
                }
                observations.push(format!("attempt {} body={:?}", attempt + 1, body.as_ref()));
                if attempt + 1 < MAX_ATTEMPTS {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
                panic!(
                    "{context}: alt GetObject body did not converge for {bucket}/{key}: expected {:?}; observations: {observations:?}",
                    expected_body
                );
            }
            Err(err) => {
                observations.push(format!("attempt {} error={err:?}", attempt + 1));
                if attempt + 1 < MAX_ATTEMPTS {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                } else {
                    panic!(
                        "{context}: alt GetObject did not converge for {bucket}/{key}: expected body {:?}; observations: {observations:?}",
                        expected_body
                    );
                }
            }
        }
    }
}

async fn assert_alt_get_object_denied(context: &str, bucket: &str, key: &str) {
    let result = CTX
        .alt_client()
        .get_object()
        .bucket(bucket)
        .key(key)
        .send_retrying_operation_aborted("get object during ACL matrix test")
        .await;
    assert_access_denied(context, &result);
}

async fn assert_alt_get_object_denied_eventually(context: &str, bucket: &str, key: &str) {
    const MAX_ATTEMPTS: usize = 10;
    let mut observations = Vec::new();

    for attempt in 0..MAX_ATTEMPTS {
        let result = CTX
            .alt_client()
            .get_object()
            .bucket(bucket)
            .key(key)
            .send_retrying_operation_aborted("get object during ACL matrix test")
            .await;
        if matches!(
            &result,
            Err(err)
                if err
                    .raw_response()
                    .is_some_and(|response| response.status().as_u16() == 403)
        ) {
            assert_access_denied(context, &result);
            return;
        }
        observations.push(format!("attempt {} result={result:?}", attempt + 1));
        if attempt + 1 < MAX_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }
        panic!(
            "{context}: alt GetObject did not converge to AccessDenied for {bucket}/{key}; observations: {observations:?}"
        );
    }
}

async fn assert_alt_put_object_denied(context: &str, bucket: &str, key: &str, body: &'static [u8]) {
    let result = put_object_result_retrying_operation_aborted(bucket, key, body).await;
    assert_access_denied(context, &result);
}

async fn assert_alt_put_object_allowed(
    context: &str,
    bucket: &str,
    key: &str,
    body: &'static [u8],
) {
    retrying_operation_aborted(context, || {
        CTX.alt_client()
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(body))
            .send()
    })
    .await;
}

async fn assert_alt_list_allowed(context: &str, bucket: &str, api: ListApi) {
    let keys = match api {
        ListApi::V1 => CTX
            .alt_client()
            .list_objects()
            .bucket(bucket)
            .send_retrying_operation_aborted("list objects during ACL matrix test")
            .await
            .unwrap_or_else(|err| panic!("{context}: alt ListObjects failed: {err:?}"))
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
            .unwrap_or_else(|err| panic!("{context}: alt ListObjectsV2 failed: {err:?}"))
            .contents()
            .iter()
            .filter_map(|object| object.key().map(str::to_owned))
            .collect::<Vec<_>>(),
    };

    assert_eq!(
        keys,
        vec![DEFAULT_KEY.to_string(), ACL_KEY.to_string()],
        "{context}: unexpected alt object listing"
    );
}

async fn assert_alt_list_denied(context: &str, bucket: &str, api: ListApi) {
    match api {
        ListApi::V1 => {
            let result = CTX
                .alt_client()
                .list_objects()
                .bucket(bucket)
                .send_retrying_operation_aborted("list objects during ACL matrix test")
                .await;
            assert_access_denied(context, &result);
        }
        ListApi::V2 => {
            let result = CTX
                .alt_client()
                .list_objects_v2()
                .bucket(bucket)
                .send_retrying_operation_aborted("list objects v2 during ACL matrix test")
                .await;
            assert_access_denied(context, &result);
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
    let case =
        format!("bucket_acl={bucket_acl:?}, object_acl={object_acl:?}, list_api={list_api:?}");
    let fixture = setup_access_matrix(bucket_acl, object_acl).await;

    // The first data-plane assertion after the control-plane setup uses an
    // eventual helper: cross-account authorization can lag behind
    // PutBucketAcl / PutPublicAccessBlock / PutBucketOwnershipControls.
    // Once the first operation converges, subsequent assertions in the same
    // test can use the immediate variants because the authorization decision
    // has already propagated.
    if object_acl.allows_read() {
        assert_alt_get_object_body_eventually(
            &format!("{case}: initial public object read"),
            &fixture.bucket,
            ACL_KEY,
            b"foocontent",
        )
        .await;
    } else if bucket_acl == BucketAclCase::Private {
        assert_alt_get_object_denied_eventually(
            &format!("{case}: initial private object read"),
            &fixture.bucket,
            ACL_KEY,
        )
        .await;
    } else {
        assert_alt_get_object_denied(
            &format!("{case}: object read without public object ACL"),
            &fixture.bucket,
            ACL_KEY,
        )
        .await;
    }

    if bucket_acl == BucketAclCase::Private {
        assert_alt_get_object_denied(
            &format!("{case}: private default-object read"),
            &fixture.bucket,
            DEFAULT_KEY,
        )
        .await;
        assert_alt_list_denied(
            &format!("{case}: private bucket listing"),
            &fixture.bucket,
            list_api,
        )
        .await;
        assert_alt_put_object_denied(
            &format!("{case}: overwrite ACL key in private bucket"),
            &fixture.bucket,
            ACL_KEY,
            b"barcontent",
        )
        .await;
        assert_alt_put_object_denied(
            &format!("{case}: overwrite default key in private bucket"),
            &fixture.bucket,
            DEFAULT_KEY,
            b"baroverwrite",
        )
        .await;
        assert_alt_put_object_denied(
            &format!("{case}: create new key in private bucket"),
            &fixture.bucket,
            NEW_KEY,
            b"newcontent",
        )
        .await;
        cleanup_access_matrix(&fixture.bucket).await;
        return;
    }

    // AWS allows a signed cross-account caller to create a brand-new object in
    // a public-write bucket, but not to overwrite an existing object.
    assert_alt_put_object_denied(
        &format!("{case}: overwrite existing ACL key"),
        &fixture.bucket,
        ACL_KEY,
        b"foooverwrite",
    )
    .await;

    assert_alt_get_object_denied(
        &format!("{case}: read private default key"),
        &fixture.bucket,
        DEFAULT_KEY,
    )
    .await;

    assert_alt_put_object_denied(
        &format!("{case}: overwrite existing default key"),
        &fixture.bucket,
        DEFAULT_KEY,
        b"baroverwrite",
    )
    .await;

    if bucket_acl.allows_list() {
        assert_alt_list_allowed(
            &format!("{case}: public bucket listing"),
            &fixture.bucket,
            list_api,
        )
        .await;
    } else {
        assert_alt_list_denied(
            &format!("{case}: denied bucket listing"),
            &fixture.bucket,
            list_api,
        )
        .await;
    }

    if bucket_acl == BucketAclCase::PublicReadWrite {
        assert_alt_put_object_allowed(
            &format!("{case}: create new key through public write"),
            &fixture.bucket,
            NEW_KEY,
            b"newcontent",
        )
        .await;
        assert_alt_put_object_allowed(
            &format!("{case}: overwrite alternate-owned key"),
            &fixture.bucket,
            NEW_KEY,
            b"overwritecontent",
        )
        .await;
        assert_alt_get_object_body(
            &format!("{case}: read alternate-owned overwritten key"),
            &fixture.bucket,
            NEW_KEY,
            b"overwritecontent",
        )
        .await;
    } else {
        assert_alt_put_object_denied(
            &format!("{case}: create new key without public write"),
            &fixture.bucket,
            NEW_KEY,
            b"newcontent",
        )
        .await;
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
