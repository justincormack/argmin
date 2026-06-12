//! Request checksum tests for legacy ACL subresources.

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{BucketLocationConstraint, CreateBucketConfiguration, ObjectOwnership};
use s3_tests::{
    content_md5_header, sdk_checksum_headers, send_signed_request, unique_bucket,
    SendRetryingOperationAborted, CTX,
};

async fn create_bucket_in_test_region(object_ownership: ObjectOwnership) -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    let mut request = client
        .create_bucket()
        .bucket(&bucket)
        .object_ownership(object_ownership);
    if CTX.region() != "us-east-1" {
        let config = CreateBucketConfiguration::builder()
            .location_constraint(BucketLocationConstraint::from(CTX.region()))
            .build();
        request = request.create_bucket_configuration(config);
    }
    request
        .send_retrying_operation_aborted("create request checksum ACL bucket")
        .await
        .unwrap();
    bucket
}

async fn cleanup_bucket(bucket: &str) {
    s3_tests::delete_bucket_retrying_operation_aborted(CTX.client(), bucket).await;
}

async fn cleanup_bucket_with_keys(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        let _ = client
            .delete_object()
            .bucket(bucket)
            .key(*key)
            .send_retrying_operation_aborted("delete request checksum ACL cleanup object")
            .await;
    }
    cleanup_bucket(bucket).await;
}

async fn bucket_owner_id(bucket: &str) -> String {
    CTX.client()
        .get_bucket_acl()
        .bucket(bucket)
        .send_retrying_operation_aborted("get request checksum bucket owner")
        .await
        .unwrap()
        .owner()
        .and_then(|owner| owner.id())
        .unwrap()
        .to_string()
}

async fn object_owner_id(bucket: &str, key: &str) -> String {
    CTX.client()
        .get_object_acl()
        .bucket(bucket)
        .key(key)
        .send_retrying_operation_aborted("get request checksum object owner")
        .await
        .unwrap()
        .owner()
        .and_then(|owner| owner.id())
        .unwrap()
        .to_string()
}

fn canonical_user_full_control_acl_xml(owner_id: &str) -> String {
    format!(
        "<AccessControlPolicy><Owner><ID>{owner_id}</ID></Owner><AccessControlList>\
         <Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\">\
         <ID>{owner_id}</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant>\
         </AccessControlList></AccessControlPolicy>"
    )
}

fn assert_request_succeeds(
    name: &str,
    method: &str,
    url: &str,
    body: &[u8],
    headers: &[(String, String)],
) {
    let response = send_signed_request(
        method,
        url,
        body,
        headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str())),
    );
    assert!(
        response.status < 300,
        "{name}: expected success, got {} body {}",
        response.status,
        response.body
    );
}

#[test]
fn test_bucket_acl_checksum_requirements() {
    s3_tests::run(async {
        let bucket = create_bucket_in_test_region(ObjectOwnership::BucketOwnerPreferred).await;
        let url = format!("{}/{}?acl", CTX.endpoint(), bucket);
        let owner_id = bucket_owner_id(&bucket).await;
        let body = canonical_user_full_control_acl_xml(&owner_id);

        assert_request_succeeds("PutBucketAcl XML", "PUT", &url, body.as_bytes(), &[]);
        let md5_headers = [content_md5_header(body.as_bytes())];
        assert_request_succeeds(
            "PutBucketAcl XML",
            "PUT",
            &url,
            body.as_bytes(),
            &md5_headers,
        );
        let sdk_headers = sdk_checksum_headers(body.as_bytes());
        assert_request_succeeds(
            "PutBucketAcl XML",
            "PUT",
            &url,
            body.as_bytes(),
            &sdk_headers,
        );

        let header_only_headers = vec![("x-amz-acl".to_string(), "private".to_string())];
        assert_request_succeeds(
            "PutBucketAcl header-only",
            "PUT",
            &url,
            b"",
            &header_only_headers,
        );

        cleanup_bucket(&bucket).await;
    });
}

#[test]
fn test_object_acl_checksum_requirements() {
    s3_tests::run(async {
        let bucket = create_bucket_in_test_region(ObjectOwnership::BucketOwnerPreferred).await;
        let key = "obj";
        CTX.client()
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"hello"))
            .send()
            .await
            .unwrap();

        let acl_url = format!("{}/{}/{}?acl", CTX.endpoint(), bucket, key);
        let owner_id = object_owner_id(&bucket, key).await;
        let body = canonical_user_full_control_acl_xml(&owner_id);

        assert_request_succeeds("PutObjectAcl XML", "PUT", &acl_url, body.as_bytes(), &[]);
        let md5_headers = [content_md5_header(body.as_bytes())];
        assert_request_succeeds(
            "PutObjectAcl XML",
            "PUT",
            &acl_url,
            body.as_bytes(),
            &md5_headers,
        );
        let sdk_headers = sdk_checksum_headers(body.as_bytes());
        assert_request_succeeds(
            "PutObjectAcl XML",
            "PUT",
            &acl_url,
            body.as_bytes(),
            &sdk_headers,
        );

        let header_only_headers = vec![("x-amz-acl".to_string(), "private".to_string())];
        assert_request_succeeds(
            "PutObjectAcl header-only",
            "PUT",
            &acl_url,
            b"",
            &header_only_headers,
        );

        cleanup_bucket_with_keys(&bucket, &[key]).await;
    });
}
