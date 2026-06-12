use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketCannedAcl, ObjectCannedAcl, ObjectOwnership, OwnershipControls, OwnershipControlsRule,
};
use s3_tests::{
    assert_s3_err_code, err_status, retrying_operation_aborted, retrying_operation_aborted_result,
    unique_bucket, CTX,
};

const WRONG_OWNER: &str = "000000000000";

fn assert_expected_bucket_owner_denied<T: std::fmt::Debug, E: std::fmt::Debug>(
    result: &Result<T, aws_sdk_s3::error::SdkError<E>>,
) {
    assert_eq!(err_status(result), 403, "unexpected result: {result:?}");

    let debug = format!("{result:?}");
    if debug.contains("AccessDenied") {
        assert_s3_err_code(result, "AccessDenied");
        return;
    }

    let body = result
        .as_ref()
        .err()
        .and_then(|sdk_err| sdk_err.raw_response())
        .and_then(|response| response.body().bytes());
    assert!(
        body.is_some_and(|body| body.is_empty()),
        "expected AccessDenied code or empty 403 body, got {debug}"
    );
}

macro_rules! expect_owner_ok {
    ($op:expr) => {{
        retrying_operation_aborted("expected-bucket-owner ACL success request", || async {
            $op.expected_bucket_owner(CTX.account_id()).send().await
        })
        .await
    }};
}

macro_rules! expect_owner_denied {
    ($op:expr) => {{
        let result = retrying_operation_aborted_result(|| async {
            $op.expected_bucket_owner(WRONG_OWNER).send().await
        })
        .await;
        assert_expected_bucket_owner_denied(&result);
    }};
}

fn bucket_owner_preferred_controls() -> OwnershipControls {
    let rule = OwnershipControlsRule::builder()
        .object_ownership(ObjectOwnership::BucketOwnerPreferred)
        .build()
        .unwrap();
    OwnershipControls::builder().rules(rule).build().unwrap()
}

fn object_writer_ownership_controls() -> OwnershipControls {
    let rule = OwnershipControlsRule::builder()
        .object_ownership(ObjectOwnership::ObjectWriter)
        .build()
        .unwrap();
    OwnershipControls::builder().rules(rule).build().unwrap()
}

async fn create_bucket() -> String {
    let bucket = unique_bucket();
    s3_tests::create_bucket(CTX.client(), &bucket)
        .await
        .unwrap();
    bucket
}

async fn set_object_writer_ownership(bucket: &str) {
    CTX.client()
        .put_bucket_ownership_controls()
        .bucket(bucket)
        .ownership_controls(object_writer_ownership_controls())
        .send()
        .await
        .unwrap();
}

async fn put_object_bytes(bucket: &str, key: &str, body: &[u8]) {
    CTX.client()
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(body.to_vec()))
        .send()
        .await
        .unwrap();
}

async fn cleanup_bucket(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        let _ = s3_tests::delete_object_retrying_operation_aborted(client, bucket, *key).await;
    }
    s3_tests::delete_bucket_retrying_operation_aborted(client, bucket).await;
}

#[test]
fn test_bucket_acl_expected_bucket_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket().await;

        client
            .put_bucket_ownership_controls()
            .bucket(&bucket)
            .ownership_controls(bucket_owner_preferred_controls())
            .send()
            .await
            .unwrap();

        expect_owner_denied!(client
            .put_bucket_acl()
            .bucket(&bucket)
            .acl(BucketCannedAcl::Private));
        expect_owner_ok!(client
            .put_bucket_acl()
            .bucket(&bucket)
            .acl(BucketCannedAcl::Private));

        expect_owner_denied!(client.get_bucket_acl().bucket(&bucket));
        expect_owner_ok!(client.get_bucket_acl().bucket(&bucket));

        cleanup_bucket(&bucket, &[]).await;
    });
}

#[test]
fn test_object_acl_expected_bucket_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket().await;
        let key = "obj";

        set_object_writer_ownership(&bucket).await;
        put_object_bytes(&bucket, key, b"acl").await;

        expect_owner_denied!(client
            .put_object_acl()
            .bucket(&bucket)
            .key(key)
            .acl(ObjectCannedAcl::Private));
        expect_owner_ok!(client
            .put_object_acl()
            .bucket(&bucket)
            .key(key)
            .acl(ObjectCannedAcl::Private));

        expect_owner_denied!(client.get_object_acl().bucket(&bucket).key(key));
        expect_owner_ok!(client.get_object_acl().bucket(&bucket).key(key));

        cleanup_bucket(&bucket, &[key]).await;
    });
}
