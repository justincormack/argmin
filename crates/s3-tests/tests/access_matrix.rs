//! Access-matrix tests for bucket ACL x object ACL behavior.
//!
//! These mirror the old Ceph matrix structure, but the expected write behavior
//! follows AWS: `public-read-write` allows a different signed account to create
//! a brand-new key, but not to overwrite an existing object.

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketCannedAcl, ObjectCannedAcl, ObjectOwnership, OwnershipControls, OwnershipControlsRule,
};
use s3_tests::{
    assert_s3_err_code, delete_all_and_bucket, disable_bucket_public_access_block, err_status,
    unique_bucket, CTX,
};

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

    CTX.client()
        .put_bucket_ownership_controls()
        .bucket(bucket)
        .ownership_controls(controls)
        .send()
        .await
        .unwrap();
}

async fn setup_access_matrix(
    bucket_acl: BucketAclCase,
    object_acl: ObjectAclCase,
) -> AccessMatrixFixture {
    let client = CTX.client();
    let bucket = unique_bucket();

    client.create_bucket().bucket(&bucket).send().await.unwrap();
    disable_bucket_public_access_block(client, &bucket).await;
    set_bucket_owner_preferred(&bucket).await;

    client
        .put_bucket_acl()
        .bucket(&bucket)
        .acl(bucket_acl.canned_acl())
        .send()
        .await
        .unwrap();

    client
        .put_object()
        .bucket(&bucket)
        .key(ACL_KEY)
        .body(ByteStream::from_static(b"foocontent"))
        .send()
        .await
        .unwrap();
    client
        .put_object_acl()
        .bucket(&bucket)
        .key(ACL_KEY)
        .acl(object_acl.canned_acl())
        .send()
        .await
        .unwrap();

    client
        .put_object()
        .bucket(&bucket)
        .key(DEFAULT_KEY)
        .body(ByteStream::from_static(b"barcontent"))
        .send()
        .await
        .unwrap();

    client
        .get_bucket_acl()
        .bucket(&bucket)
        .send()
        .await
        .unwrap();
    client
        .get_object_acl()
        .bucket(&bucket)
        .key(ACL_KEY)
        .send()
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
        .send()
        .await
        .unwrap();
    let body = response.body.collect().await.unwrap().into_bytes();
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
    assert_access_denied(&result);
}

async fn assert_alt_put_object_denied(bucket: &str, key: &str, body: &'static [u8]) {
    let result = CTX
        .alt_client()
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(body))
        .send()
        .await;
    assert_access_denied(&result);
}

async fn assert_alt_put_object_allowed(bucket: &str, key: &str, body: &'static [u8]) {
    CTX.alt_client()
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(body))
        .send()
        .await
        .unwrap();
}

async fn assert_alt_list_allowed(bucket: &str, api: ListApi) {
    let keys = match api {
        ListApi::V1 => CTX
            .alt_client()
            .list_objects()
            .bucket(bucket)
            .send()
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
            .send()
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
            let result = CTX.alt_client().list_objects().bucket(bucket).send().await;
            assert_access_denied(&result);
        }
        ListApi::V2 => {
            let result = CTX
                .alt_client()
                .list_objects_v2()
                .bucket(bucket)
                .send()
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

    if object_acl.allows_read() {
        assert_alt_get_object_body(&fixture.bucket, ACL_KEY, b"foocontent").await;
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
