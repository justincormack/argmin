use std::future::Future;
use std::time::Duration;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    AccessControlPolicy, BucketCannedAcl, BucketLocationConstraint, CompletedMultipartUpload,
    CompletedPart, CreateBucketConfiguration, Grant, Grantee, ObjectAttributes, ObjectCannedAcl,
    ObjectOwnership, Owner, OwnershipControls, OwnershipControlsRule, Permission, Tag, Tagging,
    Type,
};
use s3_tests::{
    assert_s3_err_code, content_md5_header, err_status, raw_bucket,
    shape::{assert_shape, id_headers, shape},
    unique_bucket, SendRetryingOperationAborted, CTX,
};
use serde_json::json;

/// Build an agent that returns all HTTP responses (including 4xx/5xx) as Ok.
fn agent() -> s3_tests::Agent {
    s3_tests::test_agent()
}

/// Cleanup helper.
async fn cleanup(bucket: &str) {
    let client = CTX.client();
    s3_tests::delete_bucket_retrying_operation_aborted(client, bucket).await;
}

async fn create_bucket_in_test_region(client: &aws_sdk_s3::Client, bucket: &str) {
    let mut request = s3_tests::create_bucket_request(client, bucket);
    if CTX.region() != "us-east-1" {
        let config = CreateBucketConfiguration::builder()
            .location_constraint(BucketLocationConstraint::from(CTX.region()))
            .build();
        request = request.create_bucket_configuration(config);
    }
    request
        .send_retrying_operation_aborted("create ownership test bucket")
        .await
        .unwrap();
}

async fn create_bucket_in_test_region_with_ownership(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    ownership: ObjectOwnership,
) {
    let mut request = s3_tests::create_bucket_request(client, bucket).object_ownership(ownership);
    if CTX.region() != "us-east-1" {
        let config = CreateBucketConfiguration::builder()
            .location_constraint(BucketLocationConstraint::from(CTX.region()))
            .build();
        request = request.create_bucket_configuration(config);
    }
    request
        .send_retrying_operation_aborted("create ownership test bucket with ownership")
        .await
        .unwrap();
}

async fn set_bucket_ownership(bucket: &str, ownership: ObjectOwnership) {
    let rule = OwnershipControlsRule::builder()
        .object_ownership(ownership)
        .build()
        .unwrap();
    let controls = OwnershipControls::builder().rules(rule).build().unwrap();
    CTX.client()
        .put_bucket_ownership_controls()
        .bucket(bucket)
        .ownership_controls(controls)
        .send_retrying_operation_aborted("set bucket ownership controls")
        .await
        .unwrap();
}

async fn put_bucket_policy_retrying(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    policy: serde_json::Value,
    context: &str,
) {
    client
        .put_bucket_policy()
        .bucket(bucket)
        .policy(policy.to_string())
        .send_retrying_operation_aborted(context)
        .await
        .unwrap();
}

async fn put_object_static_retrying(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    body: &'static [u8],
    acl: Option<ObjectCannedAcl>,
    context: &str,
) {
    s3_tests::retrying_operation_aborted(context, || {
        let mut request = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(body));
        if let Some(acl) = acl.clone() {
            request = request.acl(acl);
        }
        async move { request.send().await }
    })
    .await;
}

async fn eventually_ok<T, E, F, Fut>(description: &str, mut op: F) -> T
where
    E: std::fmt::Debug,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, aws_sdk_s3::error::SdkError<E>>>,
{
    const MAX_ATTEMPTS: usize = 20;

    for attempt in 0..MAX_ATTEMPTS {
        match op().await {
            Ok(output) => return output,
            Err(_) if attempt + 1 < MAX_ATTEMPTS => {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(err) => panic!("{description} failed unexpectedly: {err:?}"),
        }
    }

    unreachable!()
}

async fn delete_bucket_ownership(bucket: &str) {
    CTX.client()
        .delete_bucket_ownership_controls()
        .bucket(bucket)
        .send_retrying_operation_aborted("delete bucket ownership controls")
        .await
        .unwrap();
}

async fn put_alt_object_access_policy(bucket: &str) {
    let policy = json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "AllowAltObjectOwnershipExercises",
            "Effect": "Allow",
            "Principal": { "AWS": format!("arn:aws:iam::{}:root", CTX.alt_account_id()) },
            "Action": [
                "s3:GetObject",
                "s3:GetObjectAcl",
                "s3:PutObject",
                "s3:PutObjectAcl",
                "s3:AbortMultipartUpload"
            ],
            "Resource": format!("arn:aws:s3:::{bucket}/*")
        }]
    });

    CTX.client()
        .put_bucket_policy()
        .bucket(bucket)
        .policy(policy.to_string())
        .send_retrying_operation_aborted("put alternate object access bucket policy")
        .await
        .unwrap();
}

async fn bucket_owner_id(bucket: &str) -> String {
    CTX.client()
        .get_bucket_acl()
        .bucket(bucket)
        .send_retrying_operation_aborted("get ownership bucket ACL")
        .await
        .unwrap()
        .owner()
        .and_then(|owner| owner.id())
        .expect("expected owner ID in GetBucketAcl")
        .to_string()
}

async fn canonical_owner_id(client: &aws_sdk_s3::Client) -> String {
    let bucket = unique_bucket();
    create_bucket_in_test_region(client, &bucket).await;
    let owner_id = client
        .get_bucket_acl()
        .bucket(&bucket)
        .send_retrying_operation_aborted("get canonical owner bucket ACL")
        .await
        .unwrap()
        .owner()
        .and_then(|owner| owner.id())
        .expect("expected owner ID in GetBucketAcl")
        .to_string();
    s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    owner_id
}

fn canonical_user_grant(canonical_user_id: &str, permission: Permission) -> Grant {
    Grant::builder()
        .grantee(
            Grantee::builder()
                .r#type(Type::CanonicalUser)
                .id(canonical_user_id)
                .build()
                .expect("canonical grantee"),
        )
        .permission(permission)
        .build()
}

fn bucket_acl_policy(owner_id: &str, grants: Vec<Grant>) -> AccessControlPolicy {
    AccessControlPolicy::builder()
        .owner(Owner::builder().id(owner_id).build())
        .set_grants(Some(grants))
        .build()
}

async fn set_bucket_acl_with_alt_read_grant(bucket: &str) {
    let owner_id = bucket_owner_id(bucket).await;
    let alt_owner_id = canonical_owner_id(CTX.alt_client()).await;
    CTX.client()
        .put_bucket_acl()
        .bucket(bucket)
        .access_control_policy(bucket_acl_policy(
            &owner_id,
            vec![
                canonical_user_grant(&owner_id, Permission::FullControl),
                canonical_user_grant(&alt_owner_id, Permission::Read),
            ],
        ))
        .send_retrying_operation_aborted("set bucket ACL with alternate read grant")
        .await
        .unwrap();
}

#[test]
fn test_bucket_ownership_controls_raw_get_returns_canonical_xml() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        create_bucket_in_test_region(CTX.client(), &bucket).await;

        let body = br#"
            <OwnershipControls>
                <Rule>
                    <ObjectOwnership>BucketOwnerPreferred</ObjectOwnership>
                </Rule>
            </OwnershipControls>
        "#;

        let parsed = server_http::http::xml::parse_ownership_controls_xml(body).unwrap();
        let expected = server_http::http::xml::get_ownership_controls_xml(&parsed);

        let url = format!("{}/{}?ownershipControls", CTX.endpoint(), bucket);
        let put = s3_tests::send_signed_request("PUT", &url, body, [content_md5_header(body)]);
        assert_eq!(put.status, 200, "unexpected body: {}", put.body);

        let get =
            s3_tests::send_signed_request("GET", &url, b"", std::iter::empty::<(String, String)>());

        cleanup(&bucket).await;

        assert_eq!(get.status, 200, "unexpected body: {}", get.body);
        assert_eq!(get.body, expected);
    });
}

#[test]
fn test_bucket_ownership_controls_rejects_whitespace_padded_object_ownership() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        create_bucket_in_test_region(CTX.client(), &bucket).await;

        let body = br#"
            <OwnershipControls>
                <Rule>
                    <ObjectOwnership> BucketOwnerPreferred </ObjectOwnership>
                </Rule>
            </OwnershipControls>
        "#;

        let url = format!("{}/{}?ownershipControls", CTX.endpoint(), bucket);
        let put = s3_tests::send_signed_request("PUT", &url, body, [content_md5_header(body)]);

        cleanup(&bucket).await;

        assert_eq!(put.status, 400, "unexpected body: {}", put.body);
        assert!(
            put.body.contains("<Code>MalformedXML</Code>"),
            "unexpected body: {}",
            put.body
        );
    });
}

async fn object_owner_id(client: &aws_sdk_s3::Client, bucket: &str, key: &str) -> String {
    client
        .get_object_acl()
        .bucket(bucket)
        .key(key)
        .send_retrying_operation_aborted("get ownership object ACL")
        .await
        .unwrap()
        .owner()
        .and_then(|owner| owner.id())
        .expect("expected owner ID in GetObjectAcl")
        .to_string()
}

async fn object_owner_id_eventually(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    expected_owner_id: &str,
    description: &str,
) {
    const MAX_ATTEMPTS: usize = 50;

    for attempt in 0..MAX_ATTEMPTS {
        let owner_id = object_owner_id(client, bucket, key).await;
        if owner_id == expected_owner_id {
            return;
        }
        if attempt + 1 < MAX_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }
        panic!(
            "{description} did not converge to owner {expected_owner_id} for {bucket}/{key}; last owner {owner_id}"
        );
    }

    unreachable!()
}

async fn assert_alt_get_object_tagging_denied(bucket: &str, key: &str) {
    let result = CTX
        .alt_client()
        .get_object_tagging()
        .bucket(bucket)
        .key(key)
        .send()
        .await;
    assert_eq!(err_status(&result), 403);
}

async fn assert_alt_put_object_tagging_denied(bucket: &str, key: &str) {
    let tagging = aws_sdk_s3::types::Tagging::builder()
        .tag_set(
            aws_sdk_s3::types::Tag::builder()
                .key("alt")
                .value("denied")
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    let result = CTX
        .alt_client()
        .put_object_tagging()
        .bucket(bucket)
        .key(key)
        .tagging(tagging)
        .send()
        .await;
    assert_eq!(err_status(&result), 403);
}

fn has_grant(grants: &[Grant], permission: Permission, canonical_user_id: &str) -> bool {
    grants.iter().any(|grant| {
        grant.permission() == Some(&permission)
            && grant
                .grantee()
                .is_some_and(|grantee| grantee.id() == Some(canonical_user_id))
    })
}

fn assert_acl_not_supported<T, E: std::fmt::Debug>(
    result: &Result<T, aws_sdk_s3::error::SdkError<E>>,
    context: &str,
) {
    match result {
        Ok(_) => panic!("expected AccessControlListNotSupported for {context}, got Ok"),
        Err(_) => {
            assert_eq!(
                err_status(result),
                400,
                "expected 400 AccessControlListNotSupported for {context}"
            );
            assert_s3_err_code(result, "AccessControlListNotSupported");
        }
    }
}

async fn put_object_and_assert_owner(
    bucket: &str,
    key: &str,
    acl: Option<ObjectCannedAcl>,
    expected_owner_id: &str,
) {
    let alt = CTX.alt_client();
    s3_tests::retrying_operation_aborted("put ownership test object", || {
        let mut request = alt
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(b"data"));
        if let Some(acl) = acl.clone() {
            request = request.acl(acl);
        }
        async move { request.send().await }
    })
    .await;

    assert_eq!(object_owner_id(alt, bucket, key).await, expected_owner_id);
}

async fn complete_single_part_multipart_and_assert_owner(
    bucket: &str,
    key: &str,
    acl: Option<ObjectCannedAcl>,
    expected_owner_id: &str,
) {
    let alt = CTX.alt_client();
    let mut create = alt.create_multipart_upload().bucket(bucket).key(key);
    if let Some(acl) = acl {
        create = create.acl(acl);
    }
    let upload = create
        .send_retrying_operation_aborted("create ownership multipart upload")
        .await
        .unwrap();
    let upload_id = upload.upload_id().expect("expected upload ID");

    let part = s3_tests::retrying_operation_aborted("upload ownership multipart part", || {
        alt.upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from_static(b"data"))
            .send()
    })
    .await;
    let etag = part.e_tag().expect("expected upload part ETag");
    let completed = CompletedMultipartUpload::builder()
        .parts(CompletedPart::builder().part_number(1).e_tag(etag).build())
        .build();
    alt.complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send_retrying_operation_aborted("complete ownership multipart upload")
        .await
        .unwrap();

    assert_eq!(object_owner_id(alt, bucket, key).await, expected_owner_id);
}

async fn copy_object_and_assert_owner(
    bucket: &str,
    src_key: &str,
    dst_key: &str,
    acl: Option<ObjectCannedAcl>,
    expected_owner_id: &str,
) {
    let alt = CTX.alt_client();
    let mut request = alt
        .copy_object()
        .bucket(bucket)
        .key(dst_key)
        .copy_source(format!("{bucket}/{src_key}"));
    if let Some(acl) = acl {
        request = request.acl(acl);
    }
    request
        .send_retrying_operation_aborted("copy ownership test object")
        .await
        .unwrap();

    assert_eq!(
        object_owner_id(alt, bucket, dst_key).await,
        expected_owner_id
    );
}

async fn create_bucket_with_alt_object_access(ownership: ObjectOwnership) -> String {
    let bucket = unique_bucket();
    create_bucket_in_test_region(CTX.client(), &bucket).await;
    set_bucket_ownership(&bucket, ownership).await;
    put_alt_object_access_policy(&bucket).await;
    bucket
}

async fn run_cross_account_object_tagging_matrix_case(ownership: ObjectOwnership) {
    let client = CTX.client();
    let alt = CTX.alt_client();
    let bucket = create_bucket_with_alt_object_access(ownership).await;
    let key = "writer-owned-tags";

    put_object_static_retrying(
        alt,
        &bucket,
        key,
        b"data",
        None,
        "put cross-account ownership tagging object",
    )
    .await;

    let owner_initial = client
        .get_object_tagging()
        .bucket(&bucket)
        .key(key)
        .send_retrying_operation_aborted("get owner object tagging before ownership update")
        .await
        .unwrap();
    assert!(owner_initial.tag_set().is_empty());

    assert_alt_get_object_tagging_denied(&bucket, key).await;
    assert_alt_put_object_tagging_denied(&bucket, key).await;

    let tagging = aws_sdk_s3::types::Tagging::builder()
        .tag_set(
            aws_sdk_s3::types::Tag::builder()
                .key("owner")
                .value("updated")
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    client
        .put_object_tagging()
        .bucket(&bucket)
        .key(key)
        .tagging(tagging)
        .send_retrying_operation_aborted("put owner object tagging")
        .await
        .unwrap();

    let owner_updated = client
        .get_object_tagging()
        .bucket(&bucket)
        .key(key)
        .send_retrying_operation_aborted("get owner object tagging after ownership update")
        .await
        .unwrap();
    assert!(owner_updated
        .tag_set()
        .iter()
        .any(|tag| tag.key() == "owner" && tag.value() == "updated"));

    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .send_retrying_operation_aborted("delete cross-account ownership tagging object")
        .await
        .unwrap();
    s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
}

// ── test_create_bucket_no_ownership_controls ────────────────────────

/// A fresh bucket (no ownership header) should default to BucketOwnerEnforced.
#[test]
fn test_create_bucket_no_ownership_controls() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        // GET ownership controls should return BucketOwnerEnforced (AWS default)
        let resp = client
            .get_bucket_ownership_controls()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let rules = resp.ownership_controls().unwrap().rules();
        assert_eq!(rules.len(), 1);
        assert_eq!(
            rules[0].object_ownership,
            ObjectOwnership::BucketOwnerEnforced
        );

        cleanup(&bucket).await;
    });
}

#[test]
fn test_create_bucket_existing_bucket_does_not_overwrite_ownership_controls() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket_request(client, &bucket)
            .object_ownership(ObjectOwnership::ObjectWriter)
            .send_retrying_operation_aborted("create ObjectWriter ownership test bucket")
            .await
            .unwrap();

        let result = s3_tests::create_bucket_request(client, &bucket)
            .object_ownership(ObjectOwnership::BucketOwnerEnforced)
            .send()
            .await;

        assert_eq!(err_status(&result), 409);
        assert_s3_err_code(&result, "BucketAlreadyOwnedByYou");

        let resp = client
            .get_bucket_ownership_controls()
            .bucket(&bucket)
            .send_retrying_operation_aborted("get ownership controls after existing-bucket create")
            .await
            .unwrap();
        let rules = resp.ownership_controls().unwrap().rules();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].object_ownership, ObjectOwnership::ObjectWriter);

        cleanup(&bucket).await;
    });
}

#[test]
fn test_bucket_owner_cannot_get_private_object_written_by_other_user() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_with_alt_object_access(ObjectOwnership::ObjectWriter).await;

        put_object_static_retrying(
            alt_client,
            &bucket,
            "writer-owned",
            b"data",
            None,
            "put foreign-owned object",
        )
        .await;

        owner_get_object_access_denied_eventually(&bucket, "writer-owned").await;

        client
            .delete_object()
            .bucket(&bucket)
            .key("writer-owned")
            .send_retrying_operation_aborted("delete foreign-owned object")
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_bucket_policy_does_not_grant_bucket_owner_read_of_private_foreign_owned_object() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        create_bucket_in_test_region_with_ownership(client, &bucket, ObjectOwnership::ObjectWriter)
            .await;

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "AllowAltWriterPutObject",
                    "Effect": "Allow",
                    "Principal": { "AWS": format!("arn:aws:iam::{}:root", CTX.alt_account_id()) },
                    "Action": "s3:PutObject",
                    "Resource": format!("arn:aws:s3:::{bucket}/*"),
                },
                {
                    "Sid": "AllowBucketOwnerReadByBucketPolicy",
                    "Effect": "Allow",
                    "Principal": { "AWS": format!("arn:aws:iam::{}:root", CTX.account_id()) },
                    "Action": "s3:GetObject",
                    "Resource": format!("arn:aws:s3:::{bucket}/*"),
                }
            ],
        });
        put_bucket_policy_retrying(
            client,
            &bucket,
            policy,
            "put foreign-owned object read policy",
        )
        .await;

        put_object_static_retrying(
            alt_client,
            &bucket,
            "writer-owned",
            b"data",
            None,
            "put foreign-owned private object",
        )
        .await;

        owner_get_object_access_denied_eventually(&bucket, "writer-owned").await;

        cleanup_keys(&bucket, &["writer-owned"]).await;
    });
}

#[test]
fn test_create_time_object_writer_bucket_policy_does_not_grant_bucket_owner_read_of_private_foreign_owned_object(
) {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        let key = "writer-owned";
        create_bucket_in_test_region_with_ownership(client, &bucket, ObjectOwnership::ObjectWriter)
            .await;

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "AllowAltWriterPutObject",
                    "Effect": "Allow",
                    "Principal": { "AWS": format!("arn:aws:iam::{}:root", CTX.alt_account_id()) },
                    "Action": "s3:PutObject",
                    "Resource": format!("arn:aws:s3:::{bucket}/*"),
                },
                {
                    "Sid": "AllowBucketOwnerReadByBucketPolicy",
                    "Effect": "Allow",
                    "Principal": { "AWS": format!("arn:aws:iam::{}:root", CTX.account_id()) },
                    "Action": "s3:GetObject",
                    "Resource": format!("arn:aws:s3:::{bucket}/*"),
                }
            ],
        });
        put_bucket_policy_retrying(
            client,
            &bucket,
            policy,
            "put create-time foreign-owned object read policy",
        )
        .await;

        put_object_static_retrying(
            alt_client,
            &bucket,
            key,
            b"data",
            None,
            "put create-time foreign-owned object",
        )
        .await;

        owner_get_object_access_denied_eventually(&bucket, key).await;

        cleanup_keys(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_does_not_grant_bucket_owner_get_object_acl_of_private_foreign_owned_object() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        let key = "writer-owned";
        create_bucket_in_test_region_with_ownership(client, &bucket, ObjectOwnership::ObjectWriter)
            .await;

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "AllowAltWriterPutObject",
                    "Effect": "Allow",
                    "Principal": { "AWS": format!("arn:aws:iam::{}:root", CTX.alt_account_id()) },
                    "Action": "s3:PutObject",
                    "Resource": format!("arn:aws:s3:::{bucket}/*"),
                },
                {
                    "Sid": "AllowBucketOwnerReadAcl",
                    "Effect": "Allow",
                    "Principal": { "AWS": format!("arn:aws:iam::{}:root", CTX.account_id()) },
                    "Action": "s3:GetObjectAcl",
                    "Resource": format!("arn:aws:s3:::{bucket}/*"),
                }
            ],
        });
        put_bucket_policy_retrying(
            client,
            &bucket,
            policy,
            "put foreign-owned object ACL read policy",
        )
        .await;

        put_object_static_retrying(
            alt_client,
            &bucket,
            key,
            b"data",
            None,
            "put foreign-owned object for ACL read",
        )
        .await;

        let acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key(key)
            .send()
            .await;
        assert_s3_err_code(&acl, "AccessDenied");

        cleanup_keys(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_does_not_grant_bucket_owner_get_object_attributes_of_private_foreign_owned_object(
) {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        let key = "writer-owned";
        create_bucket_in_test_region_with_ownership(client, &bucket, ObjectOwnership::ObjectWriter)
            .await;

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "AllowAltWriterPutObject",
                    "Effect": "Allow",
                    "Principal": { "AWS": format!("arn:aws:iam::{}:root", CTX.alt_account_id()) },
                    "Action": "s3:PutObject",
                    "Resource": format!("arn:aws:s3:::{bucket}/*"),
                },
                {
                    "Sid": "AllowBucketOwnerReadAttributes",
                    "Effect": "Allow",
                    "Principal": { "AWS": format!("arn:aws:iam::{}:root", CTX.account_id()) },
                    "Action": "s3:GetObjectAttributes",
                    "Resource": format!("arn:aws:s3:::{bucket}/*"),
                }
            ],
        });
        put_bucket_policy_retrying(
            client,
            &bucket,
            policy,
            "put foreign-owned object attributes policy",
        )
        .await;

        put_object_static_retrying(
            alt_client,
            &bucket,
            key,
            b"data",
            None,
            "put foreign-owned object for attributes read",
        )
        .await;

        let attrs = client
            .get_object_attributes()
            .bucket(&bucket)
            .key(key)
            .object_attributes(ObjectAttributes::ObjectSize)
            .send_retrying_operation_aborted("get object attributes after ownership controls")
            .await;
        assert_s3_err_code(&attrs, "AccessDenied");

        cleanup_keys(&bucket, &[key]).await;
    });
}

#[test]
fn test_probe_bucket_owner_enforced_bucket_policy_get_object_sequence_for_pre_boe_foreign_owned_object(
) {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        let key = "writer-owned";
        let alt_owner = canonical_owner_id(alt_client).await;
        create_bucket_in_test_region_with_ownership(client, &bucket, ObjectOwnership::ObjectWriter)
            .await;

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "AllowAltWriterPutObject",
                    "Effect": "Allow",
                    "Principal": { "AWS": format!("arn:aws:iam::{}:root", CTX.alt_account_id()) },
                    "Action": "s3:PutObject",
                    "Resource": format!("arn:aws:s3:::{bucket}/*"),
                },
                {
                    "Sid": "AllowBucketOwnerReadByBucketPolicy",
                    "Effect": "Allow",
                    "Principal": { "AWS": format!("arn:aws:iam::{}:root", CTX.account_id()) },
                    "Action": "s3:GetObject",
                    "Resource": format!("arn:aws:s3:::{bucket}/*"),
                }
            ],
        });
        put_bucket_policy_retrying(
            client,
            &bucket,
            policy,
            "put pre-create BOE foreign-owned read policy",
        )
        .await;

        put_object_static_retrying(
            alt_client,
            &bucket,
            key,
            b"data",
            None,
            "put pre-create BOE foreign-owned object",
        )
        .await;

        assert_eq!(object_owner_id(alt_client, &bucket, key).await, alt_owner);
        owner_get_object_access_denied_eventually(&bucket, key).await;

        set_bucket_ownership(&bucket, ObjectOwnership::BucketOwnerEnforced).await;

        let bucket_owner = bucket_owner_id(&bucket).await;
        object_owner_id_eventually(
            client,
            &bucket,
            key,
            &bucket_owner,
            "pre-create ObjectWriter BOE owner flip",
        )
        .await;
        let during_boe =
            owner_get_object_eventually(&bucket, key, "create-time ObjectWriter BOE GetObject")
                .await
                .body
                .collect()
                .await
                .unwrap()
                .into_bytes();
        assert_eq!(&during_boe[..], b"data");

        delete_bucket_ownership(&bucket).await;

        object_owner_id_eventually(
            alt_client,
            &bucket,
            key,
            &alt_owner,
            "pre-create ObjectWriter BOE removal owner flip",
        )
        .await;
        owner_get_object_access_denied_after_boe_removal_eventually(
            &bucket,
            key,
            "create-time ObjectWriter BOE removal GetObject",
        )
        .await;

        cleanup_keys(&bucket, &[key]).await;
    });
}

#[test]
fn test_probe_bucket_owner_enforced_bucket_policy_get_object_sequence_for_post_creation_object_writer_foreign_owned_object(
) {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        let key = "writer-owned";
        let alt_owner = canonical_owner_id(alt_client).await;
        create_bucket_in_test_region(client, &bucket).await;
        set_bucket_ownership(&bucket, ObjectOwnership::ObjectWriter).await;

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "AllowAltWriterPutObject",
                    "Effect": "Allow",
                    "Principal": { "AWS": format!("arn:aws:iam::{}:root", CTX.alt_account_id()) },
                    "Action": "s3:PutObject",
                    "Resource": format!("arn:aws:s3:::{bucket}/*"),
                },
                {
                    "Sid": "AllowBucketOwnerReadByBucketPolicy",
                    "Effect": "Allow",
                    "Principal": { "AWS": format!("arn:aws:iam::{}:root", CTX.account_id()) },
                    "Action": "s3:GetObject",
                    "Resource": format!("arn:aws:s3:::{bucket}/*"),
                }
            ],
        });
        put_bucket_policy_retrying(
            client,
            &bucket,
            policy,
            "put post-create BOE foreign-owned read policy",
        )
        .await;

        put_object_static_retrying(
            alt_client,
            &bucket,
            key,
            b"data",
            None,
            "put post-create BOE foreign-owned object",
        )
        .await;

        assert_eq!(object_owner_id(alt_client, &bucket, key).await, alt_owner);
        owner_get_object_access_denied_eventually(&bucket, key).await;

        set_bucket_ownership(&bucket, ObjectOwnership::BucketOwnerEnforced).await;

        let bucket_owner = bucket_owner_id(&bucket).await;
        object_owner_id_eventually(
            client,
            &bucket,
            key,
            &bucket_owner,
            "post-creation ObjectWriter BOE owner flip",
        )
        .await;
        let during_boe =
            owner_get_object_eventually(&bucket, key, "post-creation ObjectWriter BOE GetObject")
                .await
                .body
                .collect()
                .await
                .unwrap()
                .into_bytes();
        assert_eq!(&during_boe[..], b"data");

        delete_bucket_ownership(&bucket).await;

        object_owner_id_eventually(
            alt_client,
            &bucket,
            key,
            &alt_owner,
            "post-creation ObjectWriter BOE removal owner flip",
        )
        .await;
        owner_get_object_access_denied_after_boe_removal_eventually(
            &bucket,
            key,
            "post-creation ObjectWriter BOE removal GetObject",
        )
        .await;

        cleanup_keys(&bucket, &[key]).await;
    });
}

#[test]
fn test_probe_bucket_owner_enforced_get_object_sequence_without_read_policy_for_pre_boe_foreign_owned_object(
) {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        let key = "writer-owned";
        let alt_owner = canonical_owner_id(alt_client).await;
        create_bucket_in_test_region_with_ownership(client, &bucket, ObjectOwnership::ObjectWriter)
            .await;

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Sid": "AllowAltWriterPutObject",
                "Effect": "Allow",
                "Principal": { "AWS": format!("arn:aws:iam::{}:root", CTX.alt_account_id()) },
                "Action": "s3:PutObject",
                "Resource": format!("arn:aws:s3:::{bucket}/*"),
            }],
        });
        put_bucket_policy_retrying(
            client,
            &bucket,
            policy,
            "put pre-create BOE foreign-owned write policy",
        )
        .await;

        put_object_static_retrying(
            alt_client,
            &bucket,
            key,
            b"data",
            None,
            "put pre-create BOE foreign-owned object without read policy",
        )
        .await;

        assert_eq!(object_owner_id(alt_client, &bucket, key).await, alt_owner);
        owner_get_object_access_denied_eventually(&bucket, key).await;

        set_bucket_ownership(&bucket, ObjectOwnership::BucketOwnerEnforced).await;

        let bucket_owner = bucket_owner_id(&bucket).await;
        object_owner_id_eventually(
            client,
            &bucket,
            key,
            &bucket_owner,
            "pre-create ObjectWriter BOE owner flip without read policy",
        )
        .await;
        let during_boe = owner_get_object_eventually(
            &bucket,
            key,
            "create-time ObjectWriter BOE GetObject without read policy",
        )
        .await
        .body
        .collect()
        .await
        .unwrap()
        .into_bytes();
        assert_eq!(&during_boe[..], b"data");

        delete_bucket_ownership(&bucket).await;

        object_owner_id_eventually(
            alt_client,
            &bucket,
            key,
            &alt_owner,
            "pre-create ObjectWriter BOE removal owner flip without read policy",
        )
        .await;
        owner_get_object_access_denied_after_boe_removal_eventually(
            &bucket,
            key,
            "create-time ObjectWriter BOE removal GetObject without read policy",
        )
        .await;

        cleanup_keys(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_foreign_owned_object_access_matrix_matches_aws() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        let key = "writer-owned";
        create_bucket_in_test_region_with_ownership(client, &bucket, ObjectOwnership::ObjectWriter)
            .await;

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "AllowAltWriterPutObject",
                    "Effect": "Allow",
                    "Principal": { "AWS": format!("arn:aws:iam::{}:root", CTX.alt_account_id()) },
                    "Action": "s3:PutObject",
                    "Resource": format!("arn:aws:s3:::{bucket}/*"),
                },
                {
                    "Sid": "AllowBucketOwnerObjectAccess",
                    "Effect": "Allow",
                    "Principal": { "AWS": format!("arn:aws:iam::{}:root", CTX.account_id()) },
                    "Action": [
                        "s3:GetObject",
                        "s3:GetObjectAcl",
                        "s3:GetObjectTagging",
                        "s3:GetObjectAttributes",
                        "s3:PutObjectAcl",
                        "s3:PutObjectTagging"
                    ],
                    "Resource": format!("arn:aws:s3:::{bucket}/*"),
                }
            ],
        });
        put_bucket_policy_retrying(
            client,
            &bucket,
            policy,
            "put foreign-owned object access matrix policy",
        )
        .await;

        put_object_static_retrying(
            alt_client,
            &bucket,
            key,
            b"data",
            None,
            "put foreign-owned access matrix object",
        )
        .await;

        let get_object = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("get object after ownership controls")
            .await;
        let head_object = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("head object after ownership controls")
            .await;
        let get_object_attributes = client
            .get_object_attributes()
            .bucket(&bucket)
            .key(key)
            .object_attributes(ObjectAttributes::ObjectSize)
            .send()
            .await;
        let get_object_acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key(key)
            .send()
            .await;
        let get_object_tagging = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(key)
            .send()
            .await;
        let put_object_acl = client
            .put_object_acl()
            .bucket(&bucket)
            .key(key)
            .acl(ObjectCannedAcl::Private)
            .send()
            .await;
        let put_object_tagging = client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .tagging(
                Tagging::builder()
                    .tag_set(Tag::builder().key("k").value("v").build().unwrap())
                    .build()
                    .unwrap(),
            )
            .send()
            .await;

        assert_s3_err_code(&get_object, "AccessDenied");
        assert_s3_err_code(&head_object, "AccessDenied");
        assert_s3_err_code(&get_object_attributes, "AccessDenied");
        assert_s3_err_code(&get_object_acl, "AccessDenied");
        get_object_tagging
            .expect("bucket owner policy should allow GetObjectTagging on foreign-owned object");
        // Probed on AWS 2026-07-07: bucket policies no longer grant the
        // bucket owner PutObjectAcl on a foreign-owned object ("no
        // resource-based policy allows the s3:PutObjectAcl action"),
        // aligning ACL writes with the already-denied ACL reads.
        assert_eq!(err_status(&put_object_acl), 403);
        assert_s3_err_code(&put_object_acl, "AccessDenied");
        put_object_tagging
            .expect("bucket owner policy should allow PutObjectTagging on foreign-owned object");

        cleanup_keys(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_allows_bucket_owner_put_object_overwrite_of_private_foreign_owned_object() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        let key = "writer-owned";
        create_bucket_in_test_region_with_ownership(client, &bucket, ObjectOwnership::ObjectWriter)
            .await;

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "AllowAltWriterPutObject",
                    "Effect": "Allow",
                    "Principal": { "AWS": format!("arn:aws:iam::{}:root", CTX.alt_account_id()) },
                    "Action": "s3:PutObject",
                    "Resource": format!("arn:aws:s3:::{bucket}/*"),
                },
                {
                    "Sid": "AllowBucketOwnerOverwrite",
                    "Effect": "Allow",
                    "Principal": { "AWS": format!("arn:aws:iam::{}:root", CTX.account_id()) },
                    "Action": "s3:PutObject",
                    "Resource": format!("arn:aws:s3:::{bucket}/*"),
                }
            ],
        });
        put_bucket_policy_retrying(client, &bucket, policy, "put bucket-owner overwrite policy")
            .await;

        put_object_static_retrying(
            alt_client,
            &bucket,
            key,
            b"writer-data",
            None,
            "put foreign-owned object before owner overwrite",
        )
        .await;

        eventually_ok(
            "bucket-owner PutObject overwrite of private foreign-owned object",
            || {
                client
                    .put_object()
                    .bucket(&bucket)
                    .key(key)
                    .body(ByteStream::from_static(b"owner-data"))
                    .send()
            },
        )
        .await;

        let body = eventually_ok(
            "bucket-owner GetObject after overwrite of foreign-owned object",
            || client.get_object().bucket(&bucket).key(key).send(),
        )
        .await
        .body
        .collect()
        .await
        .unwrap()
        .into_bytes();
        assert_eq!(&body[..], b"owner-data");

        cleanup_keys(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_owner_can_delete_private_foreign_owned_object() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        let key = "writer-owned";
        create_bucket_in_test_region_with_ownership(client, &bucket, ObjectOwnership::ObjectWriter)
            .await;

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Sid": "AllowAltWriterPutObject",
                "Effect": "Allow",
                "Principal": { "AWS": format!("arn:aws:iam::{}:root", CTX.alt_account_id()) },
                "Action": "s3:PutObject",
                "Resource": format!("arn:aws:s3:::{bucket}/*"),
            }],
        });
        put_bucket_policy_retrying(
            client,
            &bucket,
            policy,
            "put bucket-owner delete foreign-owned object policy",
        )
        .await;

        put_object_static_retrying(
            alt_client,
            &bucket,
            key,
            b"writer-data",
            None,
            "put foreign-owned object before owner delete",
        )
        .await;

        eventually_ok(
            "bucket-owner DeleteObject on private foreign-owned object",
            || client.delete_object().bucket(&bucket).key(key).send(),
        )
        .await;

        cleanup(&bucket).await;
    });
}

#[test]
fn test_bucket_policy_does_not_grant_bucket_owner_copy_object_from_private_foreign_owned_object() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        let src_key = "writer-owned";
        let dst_key = "copied";
        create_bucket_in_test_region_with_ownership(client, &bucket, ObjectOwnership::ObjectWriter)
            .await;

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "AllowAltWriterPutObject",
                    "Effect": "Allow",
                    "Principal": { "AWS": format!("arn:aws:iam::{}:root", CTX.alt_account_id()) },
                    "Action": "s3:PutObject",
                    "Resource": format!("arn:aws:s3:::{bucket}/*"),
                },
                {
                    "Sid": "AllowBucketOwnerReadForeignOwnedSource",
                    "Effect": "Allow",
                    "Principal": { "AWS": format!("arn:aws:iam::{}:root", CTX.account_id()) },
                    "Action": "s3:GetObject",
                    "Resource": format!("arn:aws:s3:::{bucket}/*"),
                }
            ],
        });
        put_bucket_policy_retrying(
            client,
            &bucket,
            policy,
            "put bucket-owner copy source read policy",
        )
        .await;

        put_object_static_retrying(
            alt_client,
            &bucket,
            src_key,
            b"writer-data",
            None,
            "put foreign-owned copy source object",
        )
        .await;

        let copy = client
            .copy_object()
            .bucket(&bucket)
            .key(dst_key)
            .copy_source(format!("{bucket}/{src_key}"))
            .send()
            .await;
        assert_s3_err_code(&copy, "AccessDenied");

        cleanup_keys(&bucket, &[src_key]).await;
    });
}

#[test]
fn test_bucket_policy_does_not_grant_bucket_owner_upload_part_copy_from_private_foreign_owned_object(
) {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        let src_key = "writer-owned";
        let dst_key = "copied-mpu";
        create_bucket_in_test_region_with_ownership(client, &bucket, ObjectOwnership::ObjectWriter)
            .await;

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "AllowAltWriterPutObject",
                    "Effect": "Allow",
                    "Principal": { "AWS": format!("arn:aws:iam::{}:root", CTX.alt_account_id()) },
                    "Action": "s3:PutObject",
                    "Resource": format!("arn:aws:s3:::{bucket}/*"),
                },
                {
                    "Sid": "AllowBucketOwnerReadForeignOwnedSource",
                    "Effect": "Allow",
                    "Principal": { "AWS": format!("arn:aws:iam::{}:root", CTX.account_id()) },
                    "Action": "s3:GetObject",
                    "Resource": format!("arn:aws:s3:::{bucket}/*"),
                }
            ],
        });
        put_bucket_policy_retrying(
            client,
            &bucket,
            policy,
            "put bucket-owner upload-part-copy source read policy",
        )
        .await;

        put_object_static_retrying(
            alt_client,
            &bucket,
            src_key,
            b"writer-data",
            None,
            "put foreign-owned upload-part-copy source object",
        )
        .await;

        let upload = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .send_retrying_operation_aborted("create ownership upload-part-copy MPU")
            .await
            .unwrap();
        let upload_id = upload.upload_id().expect("expected upload id").to_string();

        let copied_part = client
            .upload_part_copy()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(&upload_id)
            .part_number(1)
            .copy_source(format!("{bucket}/{src_key}"))
            .send()
            .await;
        assert_s3_err_code(&copied_part, "AccessDenied");

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(&upload_id)
            .send_retrying_operation_aborted("abort ownership upload-part-copy MPU")
            .await
            .unwrap();

        cleanup_keys(&bucket, &[src_key]).await;
    });
}

#[test]
fn test_object_writer_cross_account_object_tagging_matrix() {
    s3_tests::run(async {
        run_cross_account_object_tagging_matrix_case(ObjectOwnership::ObjectWriter).await;
    });
}

#[test]
fn test_bucket_owner_preferred_cross_account_object_tagging_matrix() {
    s3_tests::run(async {
        run_cross_account_object_tagging_matrix_case(ObjectOwnership::BucketOwnerPreferred).await;
    });
}

// ── test_bucket_create_delete_bucket_ownership ──────────────────────

/// Full PUT/GET/DELETE lifecycle for ownership controls.
#[test]
fn test_bucket_create_delete_bucket_ownership() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        // PUT ownership controls
        let rule = aws_sdk_s3::types::OwnershipControlsRule::builder()
            .object_ownership(ObjectOwnership::BucketOwnerEnforced)
            .build()
            .unwrap();
        let controls = aws_sdk_s3::types::OwnershipControls::builder()
            .rules(rule)
            .build()
            .unwrap();
        client
            .put_bucket_ownership_controls()
            .bucket(&bucket)
            .ownership_controls(controls)
            .send()
            .await
            .unwrap();

        // GET should return BucketOwnerEnforced
        let resp = client
            .get_bucket_ownership_controls()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let rules = resp.ownership_controls().unwrap().rules();
        assert_eq!(rules.len(), 1);
        assert_eq!(
            rules[0].object_ownership,
            ObjectOwnership::BucketOwnerEnforced
        );

        // DELETE
        client
            .delete_bucket_ownership_controls()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        // GET after delete should fail
        let err = client
            .get_bucket_ownership_controls()
            .bucket(&bucket)
            .send()
            .await
            .unwrap_err();
        let raw = format!("{:?}", err);
        assert!(
            raw.contains("OwnershipControlsNotFoundError") || raw.contains("404"),
            "expected OwnershipControlsNotFoundError after delete, got: {}",
            raw
        );

        // Second delete should be idempotent (matches Ceph behavior)
        client
            .delete_bucket_ownership_controls()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        cleanup(&bucket).await;
    });
}

// ── test_create_bucket_bucket_owner_enforced ────────────────────────

/// Create a bucket with BucketOwnerEnforced, verify GET returns it, then
/// exercise the BOE behavior matrix: put/copy with ACL blocked, without ACL
/// allowed, bucket-owner-full-control allowed, PutBucketAcl blocked.
/// Mirrors Ceph _test_object_ownership_bucket_owner_enforced.
#[test]
fn test_create_bucket_bucket_owner_enforced() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket_request(client, &bucket)
            .object_ownership(ObjectOwnership::BucketOwnerEnforced)
            .send()
            .await
            .unwrap();

        // GET should return BucketOwnerEnforced
        let resp = client
            .get_bucket_ownership_controls()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let rules = resp.ownership_controls().unwrap().rules();
        assert_eq!(rules.len(), 1);
        assert_eq!(
            rules[0].object_ownership,
            ObjectOwnership::BucketOwnerEnforced
        );

        client
            .put_object()
            .bucket(&bucket)
            .key("put-object-no-acl")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();
        client
            .put_object()
            .bucket(&bucket)
            .key("put-object-bofc")
            .acl(ObjectCannedAcl::BucketOwnerFullControl)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();
        let _put_private = client
            .put_object()
            .bucket(&bucket)
            .key("put-object-private")
            .acl(ObjectCannedAcl::Private)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();
        let put_public = client
            .put_object()
            .bucket(&bucket)
            .key("put-object-public")
            .acl(ObjectCannedAcl::PublicRead)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await;
        assert_eq!(err_status(&put_public), 400);
        assert_s3_err_code(&put_public, "AccessControlListNotSupported");
        let _put_bucket_owner_read = client
            .put_object()
            .bucket(&bucket)
            .key("put-object-bor")
            .acl(ObjectCannedAcl::BucketOwnerRead)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        client
            .copy_object()
            .bucket(&bucket)
            .key("copy-object-no-acl")
            .copy_source(format!("{}/put-object-no-acl", bucket))
            .send()
            .await
            .unwrap();
        client
            .copy_object()
            .bucket(&bucket)
            .key("copy-object-bofc")
            .copy_source(format!("{}/put-object-no-acl", bucket))
            .acl(ObjectCannedAcl::BucketOwnerFullControl)
            .send()
            .await
            .unwrap();
        let _copy_private = client
            .copy_object()
            .bucket(&bucket)
            .key("copy-object-private")
            .copy_source(format!("{}/put-object-no-acl", bucket))
            .acl(ObjectCannedAcl::Private)
            .send()
            .await
            .unwrap();
        let copy_public = client
            .copy_object()
            .bucket(&bucket)
            .key("copy-object-public")
            .copy_source(format!("{}/put-object-no-acl", bucket))
            .acl(ObjectCannedAcl::PublicRead)
            .send()
            .await;
        assert_eq!(err_status(&copy_public), 400);
        assert_s3_err_code(&copy_public, "AccessControlListNotSupported");
        let _copy_bucket_owner_read = client
            .copy_object()
            .bucket(&bucket)
            .key("copy-object-bor")
            .copy_source(format!("{}/put-object-no-acl", bucket))
            .acl(ObjectCannedAcl::BucketOwnerRead)
            .send()
            .await
            .unwrap();

        let mpu_no_acl = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("mpu-no-acl")
            .send()
            .await
            .unwrap();
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("mpu-no-acl")
            .upload_id(mpu_no_acl.upload_id().unwrap())
            .send()
            .await
            .unwrap();
        let mpu_bofc = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("mpu-bofc")
            .acl(ObjectCannedAcl::BucketOwnerFullControl)
            .send()
            .await
            .unwrap();
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("mpu-bofc")
            .upload_id(mpu_bofc.upload_id().unwrap())
            .send()
            .await
            .unwrap();
        let mpu_private = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("mpu-private")
            .acl(ObjectCannedAcl::Private)
            .send()
            .await
            .unwrap();
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("mpu-private")
            .upload_id(mpu_private.upload_id().unwrap())
            .send()
            .await
            .unwrap();
        let mpu_bucket_owner_read = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("mpu-bor")
            .acl(ObjectCannedAcl::BucketOwnerRead)
            .send()
            .await
            .unwrap();
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("mpu-bor")
            .upload_id(mpu_bucket_owner_read.upload_id().unwrap())
            .send()
            .await
            .unwrap();

        let put_bucket_acl = client
            .put_bucket_acl()
            .bucket(&bucket)
            .acl(BucketCannedAcl::Private)
            .send()
            .await;
        assert_eq!(err_status(&put_bucket_acl), 400);
        assert_s3_err_code(&put_bucket_acl, "AccessControlListNotSupported");

        // Cleanup objects
        for key in [
            "put-object-no-acl",
            "put-object-bofc",
            "put-object-private",
            "put-object-bor",
            "copy-object-no-acl",
            "copy-object-bofc",
            "copy-object-private",
            "copy-object-bor",
        ] {
            client
                .delete_object()
                .bucket(&bucket)
                .key(key)
                .send()
                .await
                .unwrap();
        }
        cleanup(&bucket).await;
    });
}

// ── test_put_bucket_ownership_enforced_rejects_external_bucket_acl ──

/// PUT ownership controls on a bucket with a non-owner ACL grant should fail
/// with InvalidBucketAclWithObjectOwnership. Setting ACL to private first,
/// then setting ownership to BucketOwnerEnforced should succeed.
#[test]
fn test_put_bucket_ownership_enforced_rejects_external_bucket_acl() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        create_bucket_in_test_region_with_ownership(
            client,
            &bucket,
            ObjectOwnership::BucketOwnerPreferred,
        )
        .await;
        set_bucket_acl_with_alt_read_grant(&bucket).await;

        // PUT BucketOwnerEnforced should fail while the bucket ACL grants another account.
        let oc_url = format!("{}/{}?ownershipControls", CTX.endpoint(), bucket);
        let body = b"<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>";
        let status = send_signed_put(&oc_url, body, &[]);
        assert_eq!(
            status, 400,
            "expected 400 for BucketOwnerEnforced on externally granted bucket, got {}",
            status
        );

        // Set ACL to private
        client
            .put_bucket_acl()
            .bucket(&bucket)
            .acl(BucketCannedAcl::Private)
            .send()
            .await
            .unwrap();

        // PUT BucketOwnerEnforced should now succeed
        let rule = aws_sdk_s3::types::OwnershipControlsRule::builder()
            .object_ownership(ObjectOwnership::BucketOwnerEnforced)
            .build()
            .unwrap();
        let controls = aws_sdk_s3::types::OwnershipControls::builder()
            .rules(rule)
            .build()
            .unwrap();
        client
            .put_bucket_ownership_controls()
            .bucket(&bucket)
            .ownership_controls(controls)
            .send()
            .await
            .unwrap();

        cleanup(&bucket).await;
    });
}

// ── test_bucket_owner_enforced_rejects_object_acl ───────────────────

/// PutObject with x-amz-acl on a BucketOwnerEnforced bucket should fail
/// with AccessControlListNotSupported. PutObject without ACL should succeed.
#[test]
fn test_bucket_owner_enforced_rejects_object_acl() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_owner = canonical_owner_id(CTX.alt_client()).await;
        let bucket = unique_bucket();
        s3_tests::create_bucket_request(client, &bucket)
            .object_ownership(ObjectOwnership::BucketOwnerEnforced)
            .send()
            .await
            .unwrap();

        // PutObject with an explicit non-owner ACL grant should fail.
        let obj_url = format!("{}/{}/testkey", CTX.endpoint(), bucket);
        let grant_read = format!("id=\"{alt_owner}\"");
        let status = send_signed_put(&obj_url, b"hello", &[("x-amz-grant-read", &grant_read)]);
        assert_eq!(
            status, 400,
            "expected 400 for PutObject with ACL on BucketOwnerEnforced, got {}",
            status
        );

        // PutObject without ACL should succeed
        client
            .put_object()
            .bucket(&bucket)
            .key("testkey")
            .body(aws_sdk_s3::primitives::ByteStream::from_static(b"hello"))
            .send()
            .await
            .unwrap();

        // Cleanup
        client
            .delete_object()
            .bucket(&bucket)
            .key("testkey")
            .send()
            .await
            .unwrap();
        cleanup(&bucket).await;
    });
}

// ── test_bucket_owner_enforced_allows_bucket_owner_full_control ─────

/// PutObject with x-amz-acl: bucket-owner-full-control on a
/// BucketOwnerEnforced bucket should succeed.
#[test]
fn test_bucket_owner_enforced_allows_bucket_owner_full_control() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket_request(client, &bucket)
            .object_ownership(ObjectOwnership::BucketOwnerEnforced)
            .send()
            .await
            .unwrap();

        // PutObject with bucket-owner-full-control should succeed
        client
            .put_object()
            .bucket(&bucket)
            .key("testkey2")
            .acl(ObjectCannedAcl::BucketOwnerFullControl)
            .body(aws_sdk_s3::primitives::ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        // Cleanup
        client
            .delete_object()
            .bucket(&bucket)
            .key("testkey2")
            .send()
            .await
            .unwrap();
        cleanup(&bucket).await;
    });
}

// ── test_put_bucket_ownership_bucket_owner_enforced ──────────────────

/// Mirrors Ceph test_put_bucket_ownership_bucket_owner_enforced:
/// 1. Create bucket with a non-owner canonical-user bucket ACL grant
/// 2. PutBucketOwnershipControls BOE fails (InvalidBucketAclWithObjectOwnership)
/// 3. Set ACL to private
/// 4. PutBucketOwnershipControls BOE succeeds
/// 5. Verify BOE behavior: PutObject/CopyObject ACL blocked, PutBucketAcl blocked
#[test]
fn test_put_bucket_ownership_bucket_owner_enforced_rejects_external_bucket_acl() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_owner = canonical_owner_id(CTX.alt_client()).await;
        let bucket = unique_bucket();
        create_bucket_in_test_region_with_ownership(
            client,
            &bucket,
            ObjectOwnership::BucketOwnerPreferred,
        )
        .await;
        set_bucket_acl_with_alt_read_grant(&bucket).await;

        // PutBucketOwnershipControls BOE should fail while the bucket ACL grants another account.
        let oc_url = format!("{}/{}?ownershipControls", CTX.endpoint(), bucket);
        let oc_body = b"<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>";
        let status = send_signed_put(&oc_url, oc_body, &[]);
        assert_eq!(
            status, 400,
            "BOE on externally granted bucket should fail, got {status}"
        );

        // Set ACL to private
        client
            .put_bucket_acl()
            .bucket(&bucket)
            .acl(BucketCannedAcl::Private)
            .send()
            .await
            .unwrap();

        // PutBucketOwnershipControls BOE should now succeed
        let rule = aws_sdk_s3::types::OwnershipControlsRule::builder()
            .object_ownership(ObjectOwnership::BucketOwnerEnforced)
            .build()
            .unwrap();
        let controls = aws_sdk_s3::types::OwnershipControls::builder()
            .rules(rule)
            .build()
            .unwrap();
        client
            .put_bucket_ownership_controls()
            .bucket(&bucket)
            .ownership_controls(controls)
            .send()
            .await
            .unwrap();

        // --- Verify BOE behavior matrix ---

        // PutObject without ACL should succeed
        client
            .put_object()
            .bucket(&bucket)
            .key("put-object-no-acl")
            .body(aws_sdk_s3::primitives::ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let _put_private = client
            .put_object()
            .bucket(&bucket)
            .key("put-object-private")
            .acl(ObjectCannedAcl::Private)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        // PutObject with an explicit non-owner ACL grant should fail.
        let obj_url2 = format!("{}/{}/put-object-grant-read", CTX.endpoint(), bucket);
        let grant_read = format!("id=\"{alt_owner}\"");
        let status = send_signed_put(&obj_url2, b"data", &[("x-amz-grant-read", &grant_read)]);
        assert_eq!(
            status, 400,
            "PutObject with explicit ACL grant should fail under BOE, got {status}"
        );

        let _copy_private = client
            .copy_object()
            .bucket(&bucket)
            .key("copy-object-private")
            .copy_source(format!("{}/put-object-no-acl", bucket))
            .acl(ObjectCannedAcl::Private)
            .send()
            .await
            .unwrap();

        // PutBucketAcl private should fail (all PutBucketAcl rejected under BOE)
        let acl_url = format!("{}/{}?acl", CTX.endpoint(), bucket);
        let status = send_signed_put(&acl_url, b"", &[("x-amz-acl", "private")]);
        assert_eq!(
            status, 400,
            "PutBucketAcl private should fail under BOE, got {status}"
        );

        // Cleanup
        for key in [
            "put-object-no-acl",
            "put-object-private",
            "copy-object-private",
        ] {
            client
                .delete_object()
                .bucket(&bucket)
                .key(key)
                .send()
                .await
                .unwrap();
        }
        cleanup(&bucket).await;
    });
}

// ── Ignored tests (need bucket policies or get_object_acl) ──────────

#[test]
fn test_create_bucket_bucket_owner_preferred() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket_request(client, &bucket)
            .object_ownership(ObjectOwnership::BucketOwnerPreferred)
            .send()
            .await
            .unwrap();

        let resp = client
            .get_bucket_ownership_controls()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let rules = resp.ownership_controls().unwrap().rules();
        assert_eq!(rules.len(), 1);
        assert_eq!(
            rules[0].object_ownership,
            ObjectOwnership::BucketOwnerPreferred
        );

        cleanup(&bucket).await;
    });
}

#[test]
fn test_create_bucket_object_writer() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket_request(client, &bucket)
            .object_ownership(ObjectOwnership::ObjectWriter)
            .send()
            .await
            .unwrap();

        let resp = client
            .get_bucket_ownership_controls()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let rules = resp.ownership_controls().unwrap().rules();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].object_ownership, ObjectOwnership::ObjectWriter);

        cleanup(&bucket).await;
    });
}

#[test]
fn test_put_bucket_ownership_bucket_owner_preferred() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let rule = aws_sdk_s3::types::OwnershipControlsRule::builder()
            .object_ownership(ObjectOwnership::BucketOwnerPreferred)
            .build()
            .unwrap();
        let controls = aws_sdk_s3::types::OwnershipControls::builder()
            .rules(rule)
            .build()
            .unwrap();
        client
            .put_bucket_ownership_controls()
            .bucket(&bucket)
            .ownership_controls(controls)
            .send()
            .await
            .unwrap();

        let resp = client
            .get_bucket_ownership_controls()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let rules = resp.ownership_controls().unwrap().rules();
        assert_eq!(rules.len(), 1);
        assert_eq!(
            rules[0].object_ownership,
            ObjectOwnership::BucketOwnerPreferred
        );

        cleanup(&bucket).await;
    });
}

#[test]
fn test_put_bucket_ownership_object_writer() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let rule = aws_sdk_s3::types::OwnershipControlsRule::builder()
            .object_ownership(ObjectOwnership::ObjectWriter)
            .build()
            .unwrap();
        let controls = aws_sdk_s3::types::OwnershipControls::builder()
            .rules(rule)
            .build()
            .unwrap();
        client
            .put_bucket_ownership_controls()
            .bucket(&bucket)
            .ownership_controls(controls)
            .send()
            .await
            .unwrap();

        let resp = client
            .get_bucket_ownership_controls()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let rules = resp.ownership_controls().unwrap().rules();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].object_ownership, ObjectOwnership::ObjectWriter);

        cleanup(&bucket).await;
    });
}

async fn cleanup_keys(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        let _ = s3_tests::delete_object_retrying_operation_aborted(client, bucket, key).await;
    }
    cleanup(bucket).await;
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
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
            Err(err) => panic!("alternate GetObject failed unexpectedly: {err:?}"),
        }
    }

    unreachable!()
}

async fn owner_get_object_access_denied_eventually(bucket: &str, key: &str) {
    const MAX_ATTEMPTS: usize = 30;

    for attempt in 0..MAX_ATTEMPTS {
        let result = CTX
            .client()
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await;
        if result.is_err() && err_status(&result) == 403 && {
            assert_s3_err_code(&result, "AccessDenied");
            true
        } {
            return;
        }
        if attempt + 1 < MAX_ATTEMPTS {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            continue;
        }
        panic!(
            "bucket-owner GetObject did not converge to AccessDenied for {bucket}/{key}: {:?}",
            result
        );
    }

    unreachable!()
}

async fn owner_get_object_access_denied_after_boe_removal_eventually(
    bucket: &str,
    key: &str,
    description: &str,
) {
    const MAX_ATTEMPTS: usize = 360;

    for attempt in 0..MAX_ATTEMPTS {
        let result = CTX
            .client()
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await;
        if result.is_err() && err_status(&result) == 403 && {
            assert_s3_err_code(&result, "AccessDenied");
            true
        } {
            return;
        }
        if attempt + 1 < MAX_ATTEMPTS {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            continue;
        }
        panic!(
            "{description} did not converge to AccessDenied for {bucket}/{key}: {:?}",
            result
        );
    }

    unreachable!()
}

async fn owner_get_object_eventually(
    bucket: &str,
    key: &str,
    description: &str,
) -> aws_sdk_s3::operation::get_object::GetObjectOutput {
    const MAX_ATTEMPTS: usize = 30;

    for attempt in 0..MAX_ATTEMPTS {
        match CTX
            .client()
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
        {
            Ok(output) => return output,
            Err(_) if attempt + 1 < MAX_ATTEMPTS => {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
            Err(err) => panic!(
                "{description} did not converge to allowed GetObject for {bucket}/{key}: {err:?}"
            ),
        }
    }

    unreachable!()
}

async fn alt_get_object_access_denied_after_boe_eventually(bucket: &str, key: &str) {
    const MAX_ATTEMPTS: usize = 50;

    for attempt in 0..MAX_ATTEMPTS {
        let result = CTX
            .alt_client()
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await;
        if result.is_err() && err_status(&result) == 403 && {
            assert_s3_err_code(&result, "AccessDenied");
            true
        } {
            return;
        }
        if attempt + 1 < MAX_ATTEMPTS {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            continue;
        }
        panic!(
            "alternate GetObject did not converge to AccessDenied after BOE for {bucket}/{key}: {:?}",
            result
        );
    }

    unreachable!()
}

async fn alt_get_object_restored_after_boe_eventually(
    bucket: &str,
    key: &str,
) -> aws_sdk_s3::operation::get_object::GetObjectOutput {
    const MAX_ATTEMPTS: usize = 50;

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
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
            Err(err) => panic!(
                "alternate GetObject did not converge to restored access after BOE removal for {bucket}/{key}: {err:?}"
            ),
        }
    }

    unreachable!()
}

async fn run_cross_account_object_ownership_matrix(
    ownership: ObjectOwnership,
    expected_no_acl_owner_is_bucket_owner: bool,
    expected_bofc_owner_is_bucket_owner: bool,
    expected_private_owner_is_bucket_owner: bool,
) {
    let client = CTX.client();
    let alt = CTX.alt_client();
    let bucket = create_bucket_with_alt_object_access(ownership).await;
    let bucket_owner = bucket_owner_id(&bucket).await;
    let alt_owner = canonical_owner_id(alt).await;

    client
        .put_object()
        .bucket(&bucket)
        .key("src")
        .body(ByteStream::from_static(b"src"))
        .send()
        .await
        .unwrap();

    let expected_no_acl_owner = if expected_no_acl_owner_is_bucket_owner {
        bucket_owner.as_str()
    } else {
        alt_owner.as_str()
    };
    let expected_bofc_owner = if expected_bofc_owner_is_bucket_owner {
        bucket_owner.as_str()
    } else {
        alt_owner.as_str()
    };
    let expected_private_owner = if expected_private_owner_is_bucket_owner {
        bucket_owner.as_str()
    } else {
        alt_owner.as_str()
    };

    put_object_and_assert_owner(&bucket, "put-no-acl", None, expected_no_acl_owner).await;
    put_object_and_assert_owner(
        &bucket,
        "put-bofc",
        Some(ObjectCannedAcl::BucketOwnerFullControl),
        expected_bofc_owner,
    )
    .await;
    put_object_and_assert_owner(
        &bucket,
        "put-private",
        Some(ObjectCannedAcl::Private),
        expected_private_owner,
    )
    .await;

    complete_single_part_multipart_and_assert_owner(
        &bucket,
        "mpu-no-acl",
        None,
        expected_no_acl_owner,
    )
    .await;
    complete_single_part_multipart_and_assert_owner(
        &bucket,
        "mpu-bofc",
        Some(ObjectCannedAcl::BucketOwnerFullControl),
        expected_bofc_owner,
    )
    .await;
    complete_single_part_multipart_and_assert_owner(
        &bucket,
        "mpu-private",
        Some(ObjectCannedAcl::Private),
        expected_private_owner,
    )
    .await;

    copy_object_and_assert_owner(&bucket, "src", "copy-no-acl", None, expected_no_acl_owner).await;
    copy_object_and_assert_owner(
        &bucket,
        "src",
        "copy-bofc",
        Some(ObjectCannedAcl::BucketOwnerFullControl),
        expected_bofc_owner,
    )
    .await;
    copy_object_and_assert_owner(
        &bucket,
        "src",
        "copy-private",
        Some(ObjectCannedAcl::Private),
        expected_private_owner,
    )
    .await;

    alt.put_object_acl()
        .bucket(&bucket)
        .key("put-no-acl")
        .acl(ObjectCannedAcl::Private)
        .send()
        .await
        .unwrap();

    cleanup_keys(
        &bucket,
        &[
            "src",
            "put-no-acl",
            "put-bofc",
            "put-private",
            "mpu-no-acl",
            "mpu-bofc",
            "mpu-private",
            "copy-no-acl",
            "copy-bofc",
            "copy-private",
        ],
    )
    .await;
}

#[test]
fn test_bucket_owner_preferred_cross_account_object_ownership_matrix() {
    s3_tests::run(async {
        run_cross_account_object_ownership_matrix(
            ObjectOwnership::BucketOwnerPreferred,
            false,
            true,
            false,
        )
        .await;
    });
}

#[test]
fn test_bucket_owner_enforced_rejects_remaining_canned_object_acls() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket_request(client, &bucket)
            .object_ownership(ObjectOwnership::BucketOwnerEnforced)
            .send()
            .await
            .unwrap();
        client
            .put_object()
            .bucket(&bucket)
            .key("src")
            .body(ByteStream::from_static(b"src"))
            .send()
            .await
            .unwrap();

        for (suffix, acl) in [
            ("public-read-write", ObjectCannedAcl::PublicReadWrite),
            ("authenticated-read", ObjectCannedAcl::AuthenticatedRead),
            ("aws-exec-read", ObjectCannedAcl::AwsExecRead),
        ] {
            let put_result = client
                .put_object()
                .bucket(&bucket)
                .key(format!("put-{suffix}"))
                .acl(acl.clone())
                .body(ByteStream::from_static(b"data"))
                .send()
                .await;
            assert_acl_not_supported(&put_result, &format!("PutObject {suffix}"));

            let copy_result = client
                .copy_object()
                .bucket(&bucket)
                .key(format!("copy-{suffix}"))
                .copy_source(format!("{bucket}/src"))
                .acl(acl.clone())
                .send()
                .await;
            assert_acl_not_supported(&copy_result, &format!("CopyObject {suffix}"));

            let mpu_result = client
                .create_multipart_upload()
                .bucket(&bucket)
                .key(format!("mpu-{suffix}"))
                .acl(acl)
                .send()
                .await;
            assert_acl_not_supported(&mpu_result, &format!("CreateMultipartUpload {suffix}"));
        }

        cleanup_keys(&bucket, &["src"]).await;
    });
}

#[test]
fn test_bucket_owner_enforced_rejects_explicit_object_acl_grants() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_owner = canonical_owner_id(CTX.alt_client()).await;
        let bucket = unique_bucket();
        s3_tests::create_bucket_request(client, &bucket)
            .object_ownership(ObjectOwnership::BucketOwnerEnforced)
            .send()
            .await
            .unwrap();
        client
            .put_object()
            .bucket(&bucket)
            .key("src")
            .body(ByteStream::from_static(b"src"))
            .send()
            .await
            .unwrap();

        let put_status = send_signed_request(
            "PUT",
            &format!("{}/{}/put-grant-read", CTX.endpoint(), bucket),
            b"data",
            &[("x-amz-grant-read", &format!("id=\"{alt_owner}\""))],
        );
        assert_eq!(
            put_status, 400,
            "expected 400 AccessControlListNotSupported for PutObject explicit grant, got {put_status}"
        );

        let copy_status = send_signed_request(
            "PUT",
            &format!("{}/{}/copy-grant-read", CTX.endpoint(), bucket),
            b"",
            &[
                ("x-amz-copy-source", &format!("{bucket}/src")),
                ("x-amz-grant-read", &format!("id=\"{alt_owner}\"")),
            ],
        );
        assert_eq!(
            copy_status, 400,
            "expected 400 AccessControlListNotSupported for CopyObject explicit grant, got {copy_status}"
        );

        let mpu_status = send_signed_request(
            "POST",
            &format!("{}/{}/mpu-grant-read?uploads", CTX.endpoint(), bucket),
            b"",
            &[("x-amz-grant-read", &format!("id=\"{alt_owner}\""))],
        );
        assert_eq!(
            mpu_status, 400,
            "expected 400 AccessControlListNotSupported for CreateMultipartUpload explicit grant, got {mpu_status}"
        );

        let put_acl_status = send_signed_request(
            "PUT",
            &format!("{}/{}/src?acl", CTX.endpoint(), bucket),
            b"",
            &[("x-amz-grant-read", &format!("id=\"{alt_owner}\""))],
        );
        assert_eq!(
            put_acl_status, 400,
            "expected 400 AccessControlListNotSupported for PutObjectAcl explicit grant, got {put_acl_status}"
        );

        cleanup_keys(&bucket, &["src"]).await;
    });
}

#[test]
fn test_object_writer_cross_account_object_ownership_matrix() {
    s3_tests::run(async {
        run_cross_account_object_ownership_matrix(
            ObjectOwnership::ObjectWriter,
            false,
            false,
            false,
        )
        .await;
    });
}

#[test]
fn test_bucket_owner_enforced_acl_read_and_restore_semantics() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt = CTX.alt_client();
        let bucket = create_bucket_with_alt_object_access(ObjectOwnership::ObjectWriter).await;
        let bucket_owner = bucket_owner_id(&bucket).await;
        let alt_owner = canonical_owner_id(alt).await;

        alt.put_object()
            .bucket(&bucket)
            .key("pre-boe")
            .body(ByteStream::from_static(b"before"))
            .send()
            .await
            .unwrap();
        assert_eq!(object_owner_id(alt, &bucket, "pre-boe").await, alt_owner);

        set_bucket_ownership(&bucket, ObjectOwnership::BucketOwnerEnforced).await;

        let boe_acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key("pre-boe")
            .send()
            .await
            .unwrap();
        assert_eq!(
            boe_acl.owner().and_then(|owner| owner.id()),
            Some(bucket_owner.as_str())
        );
        assert_eq!(boe_acl.grants().len(), 1);
        assert!(
            has_grant(boe_acl.grants(), Permission::FullControl, &bucket_owner),
            "expected bucket owner FULL_CONTROL during BOE, got {:?}",
            boe_acl.grants()
        );

        let body = owner_get_object_eventually(
            &bucket,
            "pre-boe",
            "BOE ACL read and restore semantics owner read during BOE",
        )
        .await
        .body
        .collect()
        .await
        .unwrap()
        .into_bytes();
        assert_eq!(&body[..], b"before");

        alt.put_object()
            .bucket(&bucket)
            .key("during-boe")
            .body(ByteStream::from_static(b"during"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            object_owner_id(client, &bucket, "during-boe").await,
            bucket_owner
        );

        delete_bucket_ownership(&bucket).await;

        assert_eq!(object_owner_id(alt, &bucket, "pre-boe").await, alt_owner);
        assert_eq!(
            object_owner_id(client, &bucket, "during-boe").await,
            bucket_owner
        );

        owner_get_object_access_denied_eventually(&bucket, "pre-boe").await;

        cleanup_keys(&bucket, &["pre-boe", "during-boe"]).await;
    });
}

#[test]
fn test_bucket_owner_enforced_disables_legacy_explicit_grantee_read_acl() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_owner = canonical_owner_id(CTX.alt_client()).await;
        let bucket = unique_bucket();
        s3_tests::create_bucket_request(client, &bucket)
            .object_ownership(ObjectOwnership::ObjectWriter)
            .send()
            .await
            .unwrap();

        client
            .put_object()
            .bucket(&bucket)
            .key("pre-boe-grant-read")
            .body(ByteStream::from_static(b"granted"))
            .customize()
            .mutate_request({
                let alt_owner = alt_owner.clone();
                move |req| {
                    req.headers_mut()
                        .insert("x-amz-grant-read", format!("id=\"{alt_owner}\""));
                }
            })
            .send()
            .await
            .unwrap();

        let body = alt_get_object_eventually(&bucket, "pre-boe-grant-read")
            .await
            .body
            .collect()
            .await
            .unwrap()
            .into_bytes();
        assert_eq!(&body[..], b"granted");

        set_bucket_ownership(&bucket, ObjectOwnership::BucketOwnerEnforced).await;

        alt_get_object_access_denied_after_boe_eventually(&bucket, "pre-boe-grant-read").await;

        cleanup_keys(&bucket, &["pre-boe-grant-read"]).await;
    });
}

#[test]
fn test_bucket_owner_enforced_retains_bucket_policy_get_object_access() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_with_alt_object_access(ObjectOwnership::ObjectWriter).await;

        client
            .put_object()
            .bucket(&bucket)
            .key("policy-read")
            .body(ByteStream::from_static(b"policy"))
            .send()
            .await
            .unwrap();

        let before = alt_get_object_eventually(&bucket, "policy-read").await;
        let before_body = before.body.collect().await.unwrap().into_bytes();
        assert_eq!(&before_body[..], b"policy");

        set_bucket_ownership(&bucket, ObjectOwnership::BucketOwnerEnforced).await;

        let after = alt_get_object_eventually(&bucket, "policy-read").await;
        let after_body = after.body.collect().await.unwrap().into_bytes();
        assert_eq!(&after_body[..], b"policy");

        cleanup_keys(&bucket, &["policy-read"]).await;
    });
}

#[test]
fn test_bucket_owner_enforced_allows_bucket_owner_full_control_equivalent_grant_headers() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt = CTX.alt_client();
        let bucket =
            create_bucket_with_alt_object_access(ObjectOwnership::BucketOwnerEnforced).await;
        let bucket_owner = bucket_owner_id(&bucket).await;
        let full_control_header = format!("id=\"{bucket_owner}\"");

        client
            .put_object()
            .bucket(&bucket)
            .key("src")
            .body(ByteStream::from_static(b"src"))
            .send()
            .await
            .unwrap();

        alt.put_object()
            .bucket(&bucket)
            .key("put-grant-full-control")
            .body(ByteStream::from_static(b"put"))
            .customize()
            .mutate_request({
                let full_control_header = full_control_header.clone();
                move |req| {
                    req.headers_mut()
                        .insert("x-amz-grant-full-control", full_control_header.clone());
                }
            })
            .send()
            .await
            .unwrap();
        assert_eq!(
            object_owner_id(client, &bucket, "put-grant-full-control").await,
            bucket_owner
        );

        alt.copy_object()
            .bucket(&bucket)
            .key("copy-grant-full-control")
            .copy_source(format!("{bucket}/src"))
            .customize()
            .mutate_request({
                let full_control_header = full_control_header.clone();
                move |req| {
                    req.headers_mut()
                        .insert("x-amz-grant-full-control", full_control_header.clone());
                }
            })
            .send()
            .await
            .unwrap();
        assert_eq!(
            object_owner_id(client, &bucket, "copy-grant-full-control").await,
            bucket_owner
        );

        let upload = alt
            .create_multipart_upload()
            .bucket(&bucket)
            .key("mpu-grant-full-control")
            .customize()
            .mutate_request({
                let full_control_header = full_control_header.clone();
                move |req| {
                    req.headers_mut()
                        .insert("x-amz-grant-full-control", full_control_header.clone());
                }
            })
            .send()
            .await
            .unwrap();
        alt.abort_multipart_upload()
            .bucket(&bucket)
            .key("mpu-grant-full-control")
            .upload_id(upload.upload_id().expect("expected upload id"))
            .send()
            .await
            .unwrap();

        cleanup_keys(
            &bucket,
            &["src", "put-grant-full-control", "copy-grant-full-control"],
        )
        .await;
    });
}

#[test]
fn test_bucket_owner_enforced_restores_legacy_explicit_grantee_read_acl() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_owner = canonical_owner_id(CTX.alt_client()).await;
        let bucket = unique_bucket();
        s3_tests::create_bucket_request(client, &bucket)
            .object_ownership(ObjectOwnership::ObjectWriter)
            .send()
            .await
            .unwrap();

        client
            .put_object()
            .bucket(&bucket)
            .key("pre-boe-grant-read")
            .body(ByteStream::from_static(b"granted"))
            .customize()
            .mutate_request({
                let alt_owner = alt_owner.clone();
                move |req| {
                    req.headers_mut()
                        .insert("x-amz-grant-read", format!("id=\"{alt_owner}\""));
                }
            })
            .send()
            .await
            .unwrap();

        let before = alt_get_object_eventually(&bucket, "pre-boe-grant-read").await;
        let before_body = before.body.collect().await.unwrap().into_bytes();
        assert_eq!(&before_body[..], b"granted");

        set_bucket_ownership(&bucket, ObjectOwnership::BucketOwnerEnforced).await;

        alt_get_object_access_denied_after_boe_eventually(&bucket, "pre-boe-grant-read").await;

        delete_bucket_ownership(&bucket).await;

        let after =
            alt_get_object_restored_after_boe_eventually(&bucket, "pre-boe-grant-read").await;
        let after_body = after.body.collect().await.unwrap().into_bytes();
        assert_eq!(&after_body[..], b"granted");

        cleanup_keys(&bucket, &["pre-boe-grant-read"]).await;
    });
}

#[test]
fn test_bucket_owner_enforced_bucket_acl_read_and_restore_semantics() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let mut request = s3_tests::create_bucket_request(client, &bucket)
            .object_ownership(ObjectOwnership::BucketOwnerEnforced);
        if CTX.region() != "us-east-1" {
            let config = CreateBucketConfiguration::builder()
                .location_constraint(BucketLocationConstraint::from(CTX.region()))
                .build();
            request = request.create_bucket_configuration(config);
        }
        request
            .send_retrying_operation_aborted("create BOE ownership bucket")
            .await
            .unwrap();
        let bucket_owner = bucket_owner_id(&bucket).await;

        let boe_acl = client
            .get_bucket_acl()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(
            boe_acl.owner().and_then(|owner| owner.id()),
            Some(bucket_owner.as_str())
        );
        assert_eq!(boe_acl.grants().len(), 1);
        assert!(
            has_grant(boe_acl.grants(), Permission::FullControl, &bucket_owner),
            "expected bucket owner FULL_CONTROL during BOE, got {:?}",
            boe_acl.grants()
        );

        cleanup(&bucket).await;
    });
}

// ── Helper: send a signed request via raw HTTP ───────────────────────

fn send_signed_request(
    method: &str,
    url_str: &str,
    body: &[u8],
    extra_headers: &[(&str, &str)],
) -> u16 {
    use std::time::SystemTime;

    let a = agent();

    let parsed = url::Url::parse(url_str).expect("parse URL");
    let path = parsed.path();
    let raw_query = parsed.query().unwrap_or("");
    let query = normalize_query(raw_query);

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    let secs = now.as_secs();
    let dt = format_amz_date(secs);
    let date_stamp = &dt[..8];

    let access_key = CTX.access_key();
    let secret_key = CTX.secret_key();
    let region = CTX.region();
    let service = "s3";

    let host = parsed
        .host_str()
        .map(|h| {
            if let Some(port) = parsed.port() {
                format!("{h}:{port}")
            } else {
                h.to_string()
            }
        })
        .unwrap();

    let payload_hash = sha256_hex(body);

    let mut header_map: Vec<(String, String)> = vec![
        ("host".to_string(), host.clone()),
        ("x-amz-content-sha256".to_string(), payload_hash.clone()),
        ("x-amz-date".to_string(), dt.clone()),
    ];
    for &(k, v) in extra_headers {
        header_map.push((k.to_lowercase(), v.to_string()));
    }
    header_map.sort_by(|a, b| a.0.cmp(&b.0));

    let signed_headers: String = header_map
        .iter()
        .map(|(k, _)| k.as_str())
        .collect::<Vec<_>>()
        .join(";");

    let canonical_headers: String = header_map
        .iter()
        .map(|(k, v)| format!("{k}:{v}\n"))
        .collect();

    let canonical_request =
        format!("{method}\n{path}\n{query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}");

    let cr_hash = sha256_hex(canonical_request.as_bytes());
    let scope = format!("{date_stamp}/{region}/{service}/aws4_request");
    let string_to_sign = format!("AWS4-HMAC-SHA256\n{dt}\n{scope}\n{cr_hash}");

    let k_date = hmac_sha256(
        format!("AWS4{secret_key}").as_bytes(),
        date_stamp.as_bytes(),
    );
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    let k_signing = hmac_sha256(&k_service, b"aws4_request");

    let signature = hex_encode(&hmac_sha256(&k_signing, string_to_sign.as_bytes()));

    let auth_header = format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
    );

    let mut request = match method {
        "PUT" => a.put(url_str),
        "POST" => a.post(url_str),
        other => panic!("unsupported method for signed request helper: {other}"),
    }
    .header("Authorization", &auth_header)
    .header("x-amz-date", &dt)
    .header("x-amz-content-sha256", &payload_hash);

    for (k, v) in extra_headers {
        request = request.header(*k, *v);
    }

    let resp = request.send(body).expect("transport error");
    resp.status().as_u16()
}

fn send_signed_put(url_str: &str, body: &[u8], extra_headers: &[(&str, &str)]) -> u16 {
    send_signed_request("PUT", url_str, body, extra_headers)
}

fn sha256_hex(data: &[u8]) -> String {
    use ring::digest;
    let d = digest::digest(&digest::SHA256, data);
    hex_encode(d.as_ref())
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    use ring::hmac;
    let k = hmac::Key::new(hmac::HMAC_SHA256, key);
    hmac::sign(&k, data).as_ref().to_vec()
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn format_amz_date(epoch_secs: u64) -> String {
    let secs = epoch_secs;
    let days_since_epoch = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;
    let (year, month, day) = days_to_date(days_since_epoch as i64);
    format!("{year:04}{month:02}{day:02}T{hours:02}{minutes:02}{seconds:02}Z")
}

fn normalize_query(raw: &str) -> String {
    if raw.is_empty() {
        return String::new();
    }
    let mut pairs: Vec<(String, String)> = raw
        .split('&')
        .filter(|s| !s.is_empty())
        .map(|pair| {
            let mut parts = pair.splitn(2, '=');
            let key = parts.next().unwrap_or("").to_string();
            let val = parts.next().unwrap_or("").to_string();
            (key, val)
        })
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn days_to_date(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

// ── GetBucketOwnershipControls response shape ───────────────────────

#[test]
fn test_get_bucket_ownership_controls_response_shape() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let ownership = aws_sdk_s3::types::OwnershipControls::builder()
            .rules(
                aws_sdk_s3::types::OwnershipControlsRule::builder()
                    .object_ownership(aws_sdk_s3::types::ObjectOwnership::BucketOwnerPreferred)
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();
        client
            .put_bucket_ownership_controls()
            .bucket(&bucket)
            .ownership_controls(ownership)
            .send()
            .await
            .expect("put bucket ownership controls");

        let response = raw_bucket("GET", &bucket, Some("ownershipControls="));
        assert_shape(
            "GetBucketOwnershipControls",
            &response,
            &shape().status(200).headers(id_headers()).body(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<OwnershipControls \
                     xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Rule>\
                     <ObjectOwnership>BucketOwnerPreferred</ObjectOwnership></Rule>\
                     </OwnershipControls>",
            ),
        );

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}
