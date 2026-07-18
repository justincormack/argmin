use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    AccessControlPolicy, Grant, Grantee, ObjectCannedAcl, ObjectOwnership, Owner, Permission, Tag,
    Tagging, Type,
};
use s3_tests::{
    create_acl_enabled_bucket, create_boe_bucket, delete_bucket_retrying_operation_aborted,
    delete_object_retrying_operation_aborted, err_status, object_url, open_flushed_partial_request,
    raw_alt_credentials, sign_request_headers_with_credentials, unique_bucket,
    FlushedPartialRequest, FlushedResponse, SendRetryingOperationAborted, CTX,
};
use serde_json::json;
use std::future::Future;
use std::time::{Duration, Instant};

const MULTI_SEGMENT_PUT_BYTES: usize = 4 * 1024 * 1024;
const STAGED_PUT_PREFIX_BYTES: usize = 64 * 1024;
const STREAMED_READ_BYTES: usize = 4 * 1024 * 1024;
const RESPONSE_STAGE_WINDOW: Duration = Duration::from_secs(2);

fn ordinary_alt_client() -> aws_sdk_s3::Client {
    CTX.alt_client().clone()
}

async fn open_alt_flushed_put(
    bucket: &str,
    key: &str,
    body: &[u8],
    flushed_prefix_bytes: usize,
    extra_headers: &[(&str, &str)],
) -> FlushedPartialRequest {
    let url = object_url(CTX.endpoint(), bucket, key, None);
    let signed = sign_request_headers_with_credentials(
        "PUT",
        &url,
        body,
        extra_headers.iter().copied(),
        raw_alt_credentials(),
    );
    let headers = signed.headers().collect::<Vec<_>>();
    open_flushed_partial_request(
        "PUT",
        &url,
        body.len(),
        &body[..flushed_prefix_bytes],
        &headers,
        CTX.tls_ca_pem(),
    )
    .await
    .expect("open and flush signed raw PutObject prefix")
}

fn alt_principal() -> serde_json::Value {
    json!({ "AWS": format!("arn:aws:iam::{}:root", CTX.alt_account_id()) })
}

fn alt_put_policy(bucket: &str, effect: &str) -> String {
    json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": effect,
            "Principal": alt_principal(),
            "Action": "s3:PutObject",
            "Resource": format!("arn:aws:s3:::{bucket}/*"),
        }],
    })
    .to_string()
}

async fn set_alt_put_policy(bucket: &str, effect: &str) {
    CTX.client()
        .put_bucket_policy()
        .bucket(bucket)
        .policy(alt_put_policy(bucket, effect))
        .send_retrying_operation_aborted("set auth timing bucket policy")
        .await
        .unwrap();
}

fn test_deadline() -> Instant {
    let timeout_secs = std::env::var("S3_TEST_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(30);
    Instant::now() + Duration::from_secs(timeout_secs)
}

fn revocation_soak_duration() -> Duration {
    let timeout_secs = std::env::var("S3_TEST_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(30);
    Duration::from_secs((timeout_secs / 4).clamp(5, 30))
}

async fn wait_for_put_allowed(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    stable_for: Duration,
) {
    let deadline = test_deadline();
    let mut stable_since = None;
    loop {
        let result = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(b"allow canary"))
            .send()
            .await;
        if result.is_ok() {
            let stable_since = stable_since.get_or_insert_with(Instant::now);
            if stable_since.elapsed() >= stable_for {
                return;
            }
        } else {
            stable_since = None;
        }
        if Instant::now() >= deadline {
            panic!("alternate PutObject allow did not converge: {result:?}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn wait_for_put_denied(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    stable_for: Duration,
) {
    let deadline = test_deadline();
    let mut stable_since = None;
    loop {
        let result = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(b"deny canary"))
            .send()
            .await;
        let denied = result.as_ref().err().is_some_and(|error| {
            error
                .raw_response()
                .map(|response| response.status().as_u16())
                == Some(403)
                && error
                    .as_service_error()
                    .and_then(ProvideErrorMetadata::code)
                    == Some("AccessDenied")
        });
        if denied {
            let stable_since = stable_since.get_or_insert_with(Instant::now);
            if stable_since.elapsed() >= stable_for {
                return;
            }
        } else {
            stable_since = None;
        }
        if Instant::now() >= deadline {
            panic!("alternate PutObject denial did not converge: {result:?}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn wait_while_keeping_body_active<F>(
    wait: F,
    request: &mut FlushedPartialRequest,
) -> (usize, bool)
where
    F: Future<Output = ()>,
{
    let mut wait = Box::pin(wait);
    let mut emitted = 0;
    let mut body_open = true;
    loop {
        tokio::select! {
            () = &mut wait => return (emitted, body_open),
            () = tokio::time::sleep(Duration::from_secs(1)), if body_open => {
                body_open = request.write_and_flush(b"k").await.is_ok();
                if body_open {
                    emitted += 1;
                }
            }
        }
    }
}

async fn wait_for_put_denied_while_keeping_body_active(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    stable_for: Duration,
    request: &mut FlushedPartialRequest,
) -> (usize, bool) {
    wait_while_keeping_body_active(
        wait_for_put_denied(client, bucket, key, stable_for),
        request,
    )
    .await
}

async fn canonical_owner_id(client: &aws_sdk_s3::Client) -> String {
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    let owner_id = client
        .get_bucket_acl()
        .bucket(&bucket)
        .send_retrying_operation_aborted("get auth timing canonical owner ID")
        .await
        .unwrap()
        .owner()
        .and_then(|owner| owner.id())
        .expect("expected canonical owner ID")
        .to_string();
    delete_bucket_retrying_operation_aborted(client, &bucket).await;
    owner_id
}

fn canonical_user_grant(canonical_user_id: &str, permission: Permission) -> Grant {
    Grant::builder()
        .grantee(
            Grantee::builder()
                .r#type(Type::CanonicalUser)
                .id(canonical_user_id)
                .build()
                .expect("canonical user grantee"),
        )
        .permission(permission)
        .build()
}

fn object_acl(owner_id: &str, grants: Vec<Grant>) -> AccessControlPolicy {
    AccessControlPolicy::builder()
        .owner(Owner::builder().id(owner_id).build())
        .set_grants(Some(grants))
        .build()
}

fn object_tagging(key: &str, value: &str) -> Tagging {
    Tagging::builder()
        .tag_set(Tag::builder().key(key).value(value).build().unwrap())
        .build()
        .unwrap()
}

fn streamed_read_payload() -> Vec<u8> {
    (0..STREAMED_READ_BYTES)
        .map(|offset| (offset % 251) as u8)
        .collect()
}

async fn wait_for_head_allowed(client: &aws_sdk_s3::Client, bucket: &str, key: &str) {
    let deadline = test_deadline();
    loop {
        let result = client.head_object().bucket(bucket).key(key).send().await;
        if result.is_ok() {
            return;
        }
        if Instant::now() >= deadline {
            panic!("alternate HeadObject allow did not converge: {result:?}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn cleanup_auth_timing_bucket(bucket: &str, keys: &[&str]) {
    let _ = CTX
        .client()
        .delete_bucket_policy()
        .bucket(bucket)
        .send_retrying_operation_aborted("delete auth timing bucket policy")
        .await;
    for key in keys {
        let _ = delete_object_retrying_operation_aborted(CTX.client(), bucket, key).await;
    }
    delete_bucket_retrying_operation_aborted(CTX.client(), bucket).await;
}

async fn assert_timing_dependent_put_result(
    bucket: &str,
    key: &str,
    expected_body: &[u8],
    response: &FlushedResponse,
) {
    match response.status() {
        200 => {
            let object = CTX
                .client()
                .get_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await
                .expect("successful timing-dependent PutObject must be readable");
            let body = object.body.collect().await.unwrap().into_bytes();
            assert_eq!(&body[..], expected_body);
        }
        403 => {
            let response_body = std::str::from_utf8(response.body())
                .expect("timing-dependent PutObject error response must be UTF-8");
            assert_eq!(
                s3_tests::shape::xml_tag_text(response_body, "Code"),
                Some("AccessDenied"),
                "unexpected timing-dependent PutObject error: {response_body}"
            );
            let head = CTX
                .client()
                .head_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await;
            assert_eq!(err_status(&head), 404, "{head:?}");
        }
        status => panic!("unexpected timing-dependent PutObject status {status}"),
    }
}

#[test]
fn test_get_object_stream_uses_acl_snapshot_after_strong_revocation() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt = CTX.alt_client();
        let bucket = create_acl_enabled_bucket(client, ObjectOwnership::ObjectWriter).await;
        let key = "streamed-get-acl-revocation";
        let payload = streamed_read_payload();

        s3_tests::retrying_operation_aborted("put ACL timing object", || {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from(payload.clone()))
                .send()
        })
        .await;
        let initial_acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("get initial ACL timing object ACL")
            .await
            .unwrap();
        let owner_id = initial_acl
            .owner()
            .and_then(|owner| owner.id())
            .expect("expected object owner canonical ID")
            .to_string();
        let alt_owner_id = canonical_owner_id(alt).await;
        client
            .put_object_acl()
            .bucket(&bucket)
            .key(key)
            .access_control_policy(object_acl(
                &owner_id,
                vec![
                    canonical_user_grant(&owner_id, Permission::FullControl),
                    canonical_user_grant(&alt_owner_id, Permission::Read),
                ],
            ))
            .send_retrying_operation_aborted("grant alternate read on ACL timing object")
            .await
            .unwrap();
        wait_for_head_allowed(alt, &bucket, key).await;

        let in_flight = alt
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("start ACL-authorized streamed GetObject")
            .await
            .unwrap();

        client
            .put_object_acl()
            .bucket(&bucket)
            .key(key)
            .acl(ObjectCannedAcl::Private)
            .send_retrying_operation_aborted("revoke alternate read on ACL timing object")
            .await
            .unwrap();
        let current_acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("read back revoked ACL timing object ACL")
            .await
            .unwrap();
        assert!(
            !current_acl.grants().iter().any(|grant| {
                grant.permission() == Some(&Permission::Read)
                    && grant
                        .grantee()
                        .is_some_and(|grantee| grantee.id() == Some(alt_owner_id.as_str()))
            }),
            "alternate READ grant remained after private ACL replacement: {:?}",
            current_acl.grants()
        );
        let fresh = alt.head_object().bucket(&bucket).key(key).send().await;
        assert_eq!(err_status(&fresh), 403, "{fresh:?}");

        let body = in_flight.body.collect().await.unwrap().into_bytes();
        assert_eq!(&body[..], payload.as_slice());

        delete_object_retrying_operation_aborted(client, &bucket, key)
            .await
            .unwrap();
        delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_get_object_stream_uses_existing_tag_snapshot_after_strong_mutation() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt = CTX.alt_client();
        let bucket = create_boe_bucket(client).await;
        let key = "streamed-get-existing-tag-mutation";
        let payload = streamed_read_payload();

        s3_tests::retrying_operation_aborted("put existing-tag timing object", || {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from(payload.clone()))
                .send()
        })
        .await;
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .tagging(object_tagging("security", "allow"))
            .send_retrying_operation_aborted("set allowing existing tag")
            .await
            .unwrap();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_principal(),
                        "Action": "s3:GetObject",
                        "Resource": format!("arn:aws:s3:::{bucket}/{key}"),
                        "Condition": {
                            "StringEquals": {
                                "s3:ExistingObjectTag/security": "allow"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send_retrying_operation_aborted("set existing-tag timing bucket policy")
            .await
            .unwrap();
        wait_for_head_allowed(alt, &bucket, key).await;

        let in_flight = alt
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("start tag-authorized streamed GetObject")
            .await
            .unwrap();

        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .tagging(object_tagging("security", "deny"))
            .send_retrying_operation_aborted("replace allowing existing tag")
            .await
            .unwrap();
        let current_tags = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("read back replaced existing tag")
            .await
            .unwrap();
        assert_eq!(current_tags.tag_set().len(), 1);
        assert_eq!(current_tags.tag_set()[0].key(), "security");
        assert_eq!(current_tags.tag_set()[0].value(), "deny");
        let fresh = alt.head_object().bucket(&bucket).key(key).send().await;
        assert_eq!(err_status(&fresh), 403, "{fresh:?}");

        let body = in_flight.body.collect().await.unwrap().into_bytes();
        assert_eq!(&body[..], payload.as_slice());

        cleanup_auth_timing_bucket(&bucket, &[key]).await;
    });
}

#[test]
fn test_boe_inflight_put_policy_revocation_preserves_latched_outcome_state() {
    s3_tests::run(async {
        let bucket = create_boe_bucket(CTX.client()).await;
        let client = ordinary_alt_client();
        let allow_canary = "revocation-allow-canary";
        let deny_canary = "revocation-deny-canary";
        let key = "inflight-policy-revocation";
        let total_bytes = MULTI_SEGMENT_PUT_BYTES;

        set_alt_put_policy(&bucket, "Allow").await;
        wait_for_put_allowed(&client, &bucket, allow_canary, Duration::from_secs(2)).await;

        let body = vec![b'k'; total_bytes];
        let mut request =
            open_alt_flushed_put(&bucket, key, &body, STAGED_PUT_PREFIX_BYTES, &[]).await;
        assert!(
            request
                .response_status_within(Duration::from_millis(250))
                .await
                .unwrap()
                .is_none(),
            "in-flight PutObject completed before policy revocation"
        );

        set_alt_put_policy(&bucket, "Deny").await;
        let soak = revocation_soak_duration();
        let (keepalive_bytes, body_open) = wait_for_put_denied_while_keeping_body_active(
            &client,
            &bucket,
            deny_canary,
            soak,
            &mut request,
        )
        .await;

        let response_visible = request
            .response_status_within(RESPONSE_STAGE_WINDOW)
            .await
            .unwrap()
            .is_some();
        let response = if response_visible || !body_open {
            request
                .read_response()
                .await
                .expect("read early in-flight PutObject response")
        } else if request
            .write_and_flush(&body[STAGED_PUT_PREFIX_BYTES + keepalive_bytes..total_bytes])
            .await
            .is_ok()
        {
            request
                .finish_and_read_response()
                .await
                .expect("read in-flight PutObject response after body completion")
        } else {
            request
                .read_response()
                .await
                .expect("read early in-flight PutObject response")
        };
        let status = response.status();
        println!(
            "BOE in-flight PutObject after converged policy revocation status: {status}, \
                 deny soak: {soak:?}"
        );
        assert_timing_dependent_put_result(&bucket, key, &body, &response).await;

        cleanup_auth_timing_bucket(&bucket, &[allow_canary, deny_canary, key]).await;
    });
}

#[test]
fn test_boe_inflight_put_policy_grant_preserves_latched_outcome_state() {
    s3_tests::run(async {
        let bucket = create_boe_bucket(CTX.client()).await;
        let client = ordinary_alt_client();
        let deny_canary = "grant-deny-canary";
        let allow_canary = "grant-allow-canary";
        let key = "inflight-policy-grant";
        let total_bytes = MULTI_SEGMENT_PUT_BYTES;

        set_alt_put_policy(&bucket, "Deny").await;
        wait_for_put_denied(&client, &bucket, deny_canary, Duration::from_secs(2)).await;

        let body = vec![b'g'; total_bytes];
        let mut request =
            open_alt_flushed_put(&bucket, key, &body, STAGED_PUT_PREFIX_BYTES, &[]).await;
        let early_status = request
            .response_status_within(RESPONSE_STAGE_WINDOW)
            .await
            .unwrap();

        set_alt_put_policy(&bucket, "Allow").await;
        wait_for_put_allowed(&client, &bucket, allow_canary, Duration::from_secs(5)).await;

        let response = if early_status.is_some() {
            request
                .read_response()
                .await
                .expect("read early policy-denied PutObject response")
        } else if request
            .write_and_flush(&body[STAGED_PUT_PREFIX_BYTES..])
            .await
            .is_ok()
        {
            request
                .finish_and_read_response()
                .await
                .expect("read policy-granted PutObject response")
        } else {
            request
                .read_response()
                .await
                .expect("read early policy-denied PutObject response")
        };
        let status = response.status();
        println!("BOE in-flight PutObject after converged policy grant status: {status}");
        assert_timing_dependent_put_result(&bucket, key, &body, &response).await;

        cleanup_auth_timing_bucket(&bucket, &[deny_canary, allow_canary, key]).await;
    });
}
