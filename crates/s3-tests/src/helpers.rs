use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::LazyLock;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use auth::canonical::canonical_query_string;
use aws_sdk_s3::client::customize::CustomizableOperation;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::operation::delete_objects::builders::DeleteObjectsFluentBuilder;
use aws_sdk_s3::operation::delete_objects::{DeleteObjectsError, DeleteObjectsOutput};
use aws_sdk_s3::operation::put_bucket_lifecycle_configuration::builders::PutBucketLifecycleConfigurationFluentBuilder;
use aws_sdk_s3::operation::put_bucket_lifecycle_configuration::{
    PutBucketLifecycleConfigurationError, PutBucketLifecycleConfigurationOutput,
};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BlockedEncryptionTypes, BucketCannedAcl, BucketLifecycleConfiguration,
    BucketLocationConstraint, BucketVersioningStatus, CreateBucketConfiguration, Delete,
    DeletedObject, EncryptionType, Error as DeleteObjectError, ObjectIdentifier, ObjectOwnership,
    ServerSideEncryption, ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration,
    ServerSideEncryptionRule, VersioningConfiguration,
};
use aws_sdk_s3::Client;
use base64::Engine;
use md5_legacy::Digest;
use ring::hmac;

use crate::{configured_test_timeout, CTX};

static BUCKET_COUNTER: AtomicU64 = AtomicU64::new(0);
static BUCKET_NAMESPACE: LazyLock<u64> = LazyLock::new(|| {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    now ^ ((std::process::id() as u64) << 32)
});

const TEST_SSE_C_KEY_BYTES: [u8; 32] = *b"abcdefghijklmnopqrstuvwxyzABCDEF";

/// Bucket name prefix, configurable via `S3_TEST_BUCKET_PREFIX`.
/// Defaults to `"test"`.
static BUCKET_PREFIX: LazyLock<String> = LazyLock::new(|| {
    if std::env::var("S3_TEST_ENDPOINT").is_ok() {
        std::env::var("S3_TEST_BUCKET_PREFIX").expect(
            "S3_TEST_BUCKET_PREFIX required with S3_TEST_ENDPOINT; use a dedicated prefix such as claude-s3- that matches the test IAM policy",
        )
    } else {
        std::env::var("S3_TEST_BUCKET_PREFIX").unwrap_or_else(|_| "test".to_string())
    }
});

/// Return the bucket prefix (from `S3_TEST_BUCKET_PREFIX` or `"test"`).
pub fn bucket_prefix() -> &'static str {
    &BUCKET_PREFIX
}

/// Fixed 32-byte customer key for SSE-C integration tests.
pub fn test_sse_c_key() -> [u8; 32] {
    TEST_SSE_C_KEY_BYTES
}

/// Return base64-encoded SSE-C key and key MD5 header values.
pub fn sse_c_header_values(key: &[u8; 32]) -> (String, String) {
    let key_b64 = base64::engine::general_purpose::STANDARD.encode(key);
    let md5 = md5_legacy::Md5::digest(key);
    let key_md5_b64 = base64::engine::general_purpose::STANDARD.encode(&md5[..]);
    (key_b64, key_md5_b64)
}

/// Generate a unique bucket name for a test.
///
/// Uses a monotonic counter + process ID to avoid collisions between
/// parallel test runs and between tests within the same run.
/// The prefix is configurable via `S3_TEST_BUCKET_PREFIX` (default `"test"`).
pub fn unique_bucket() -> String {
    let n = BUCKET_COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    format!("{}{pid}-{:016x}-{n}", bucket_prefix(), *BUCKET_NAMESPACE)
}

/// Generate a unique account-regional bucket name for this account and region.
///
/// The bucket is not created. Tests use this for AWS-facing missing-bucket
/// probes where a global namespace collision would make the oracle flaky.
pub fn unique_account_regional_bucket() -> String {
    let n = BUCKET_COUNTER.fetch_add(1, Ordering::Relaxed);
    let suffix = format!("-{}-{}-an", CTX.account_id(), CTX.region());
    let max_prefix_len = 63usize
        .checked_sub(suffix.len())
        .expect("account-regional bucket suffix must fit in S3 bucket name length");
    let prefix = format!("{}ar{:016x}{n:x}", bucket_prefix(), *BUCKET_NAMESPACE);
    assert!(
        prefix.len() <= max_prefix_len,
        "S3_TEST_BUCKET_PREFIX is too long for account-regional test bucket names: prefix {prefix:?}, suffix {suffix:?}"
    );
    format!("{prefix}{suffix}")
}

/// Configure bucket-level Public Access Block to allow public ACL and policy tests.
///
/// This only affects the bucket-level setting. Account-level or org-level block
/// public access can still override this configuration.
pub async fn disable_bucket_public_access_block(client: &Client, bucket: &str) {
    use aws_sdk_s3::types::PublicAccessBlockConfiguration;

    const RETRY_DELAY: Duration = Duration::from_millis(100);
    let deadline = std::time::Instant::now() + configured_test_timeout();
    loop {
        let pab = PublicAccessBlockConfiguration::builder()
            .block_public_acls(false)
            .ignore_public_acls(false)
            .block_public_policy(false)
            .restrict_public_buckets(false)
            .build();
        match client
            .put_public_access_block()
            .bucket(bucket)
            .public_access_block_configuration(pab)
            .send()
            .await
        {
            Ok(_) => break,
            Err(err)
                if s3_error_code(&err) == Some("OperationAborted")
                    && std::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(RETRY_DELAY).await;
            }
            Err(err) => panic!("disable bucket public access block: {err:?}"),
        }
    }
    wait_for_bucket_public_access_block_disabled(client, bucket).await;
}

async fn create_test_bucket(client: &Client, bucket: &str) {
    const RETRY_DELAY: Duration = Duration::from_millis(100);
    let deadline = std::time::Instant::now() + configured_test_timeout();

    loop {
        let mut request = client.create_bucket().bucket(bucket);
        if CTX.region() != "us-east-1" {
            request = request.create_bucket_configuration(
                CreateBucketConfiguration::builder()
                    .location_constraint(BucketLocationConstraint::from(CTX.region()))
                    .build(),
            );
        }
        match request.send().await {
            Ok(_) => return,
            Err(err) if is_create_bucket_lost_success_retry(&err) => {
                verify_bucket_exists_after_create_conflict(client, bucket, "create bucket").await;
                return;
            }
            Err(err)
                if (s3_error_code(&err) == Some("OperationAborted")
                    || s3_error_code(&err) == Some("SlowDown"))
                    && std::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(RETRY_DELAY).await;
            }
            Err(err) => panic!("create bucket: {err:?}"),
        }
    }
}

async fn create_test_bucket_with_ownership(
    client: &Client,
    bucket: &str,
    ownership: ObjectOwnership,
) {
    const RETRY_DELAY: Duration = Duration::from_millis(100);
    let deadline = std::time::Instant::now() + configured_test_timeout();

    loop {
        let mut request = client
            .create_bucket()
            .bucket(bucket)
            .object_ownership(ownership.clone());
        if CTX.region() != "us-east-1" {
            request = request.create_bucket_configuration(
                CreateBucketConfiguration::builder()
                    .location_constraint(BucketLocationConstraint::from(CTX.region()))
                    .build(),
            );
        }
        match request.send().await {
            Ok(_) => break,
            Err(err) if is_create_bucket_lost_success_retry(&err) => {
                verify_bucket_exists_after_create_conflict(
                    client,
                    bucket,
                    "create bucket with ownership",
                )
                .await;
                break;
            }
            Err(err)
                if (s3_error_code(&err) == Some("OperationAborted")
                    || s3_error_code(&err) == Some("SlowDown"))
                    && std::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(RETRY_DELAY).await;
            }
            Err(err) => panic!("create bucket with ownership: {err:?}"),
        }
    }
    wait_for_bucket_ownership_controls(client, bucket, ownership).await;
}

fn is_create_bucket_lost_success_retry(
    err: &aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::create_bucket::CreateBucketError>,
) -> bool {
    s3_error_code(err) == Some("BucketAlreadyOwnedByYou")
}

fn is_bucket_already_absent<E: ProvideErrorMetadata>(err: &aws_sdk_s3::error::SdkError<E>) -> bool {
    s3_error_code(err) == Some("NoSuchBucket")
}

fn is_bucket_not_empty<E: ProvideErrorMetadata>(err: &aws_sdk_s3::error::SdkError<E>) -> bool {
    s3_error_code(err) == Some("BucketNotEmpty")
}

fn s3_error_code<E: ProvideErrorMetadata>(err: &aws_sdk_s3::error::SdkError<E>) -> Option<&str> {
    err.as_service_error().and_then(ProvideErrorMetadata::code)
}

#[derive(Clone, Copy)]
enum OperationContentionRetryScope {
    OperationAbortedOnly,
    OperationAbortedOrSlowDown,
}

impl OperationContentionRetryScope {
    fn includes(self, code: Option<&str>) -> bool {
        match self {
            Self::OperationAbortedOnly => code == Some("OperationAborted"),
            Self::OperationAbortedOrSlowDown => {
                matches!(code, Some("OperationAborted" | "SlowDown"))
            }
        }
    }
}

fn is_retryable_operation_contention<E: ProvideErrorMetadata>(
    err: &aws_sdk_s3::error::SdkError<E>,
) -> bool {
    OperationContentionRetryScope::OperationAbortedOrSlowDown.includes(s3_error_code(err))
}

pub async fn retrying_operation_aborted<T, E, F, Fut>(context: &str, mut op: F) -> T
where
    E: ProvideErrorMetadata + std::fmt::Debug,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, aws_sdk_s3::error::SdkError<E>>>,
{
    match retrying_operation_aborted_result(&mut op).await {
        Ok(output) => output,
        Err(err) => panic!("{context}: {err:?}"),
    }
}

pub async fn retrying_operation_aborted_result<T, E, F, Fut>(
    mut op: F,
) -> Result<T, aws_sdk_s3::error::SdkError<E>>
where
    E: ProvideErrorMetadata + std::fmt::Debug,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, aws_sdk_s3::error::SdkError<E>>>,
{
    const RETRY_DELAY: Duration = Duration::from_millis(100);
    let deadline = std::time::Instant::now() + configured_test_timeout();

    loop {
        match op().await {
            Ok(output) => return Ok(output),
            Err(err)
                if is_retryable_operation_contention(&err)
                    && std::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(RETRY_DELAY).await;
            }
            Err(err) => return Err(err),
        }
    }
}

pub async fn get_object_body_retrying_operation_aborted(
    client: &Client,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    context: &str,
) -> Vec<u8> {
    const RETRY_DELAY: Duration = Duration::from_millis(100);
    let deadline = std::time::Instant::now() + configured_test_timeout();

    loop {
        let mut request = client.get_object().bucket(bucket).key(key);
        if let Some(version_id) = version_id {
            request = request.version_id(version_id);
        }

        match request.send().await {
            Ok(response) => match response.body.collect().await {
                Ok(body) => return body.into_bytes().to_vec(),
                Err(_error) if std::time::Instant::now() < deadline => {
                    tokio::time::sleep(RETRY_DELAY).await;
                    continue;
                }
                Err(error) => panic!("{context}: {error:?}"),
            },
            Err(error)
                if is_retryable_operation_contention(&error)
                    && std::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(RETRY_DELAY).await;
            }
            Err(error) => panic!("{context}: {error:?}"),
        }
    }
}

pub type RetrySendFuture<T, E> =
    Pin<Box<dyn Future<Output = Result<T, aws_sdk_s3::error::SdkError<E>>> + Send>>;

pub trait SendRetryingOperationAborted: Clone {
    type Output;
    type Error: ProvideErrorMetadata;

    fn send_once(self) -> RetrySendFuture<Self::Output, Self::Error>;

    fn send_retrying_operation_aborted(
        self,
        _description: &str,
    ) -> impl Future<Output = Result<Self::Output, aws_sdk_s3::error::SdkError<Self::Error>>> {
        send_with_operation_contention_retry(
            self,
            OperationContentionRetryScope::OperationAbortedOrSlowDown,
        )
    }

    fn send_retrying_exact_operation_aborted(
        self,
        _description: &str,
    ) -> impl Future<Output = Result<Self::Output, aws_sdk_s3::error::SdkError<Self::Error>>> {
        send_with_operation_contention_retry(
            self,
            OperationContentionRetryScope::OperationAbortedOnly,
        )
    }
}

fn operation_contention_retry_delay(
    deadline: std::time::Instant,
    now: std::time::Instant,
) -> Option<Duration> {
    const RETRY_DELAY: Duration = Duration::from_millis(100);

    let remaining = deadline.checked_duration_since(now)?;
    (!remaining.is_zero()).then_some(RETRY_DELAY.min(remaining))
}

async fn send_with_operation_contention_retry<B>(
    builder: B,
    scope: OperationContentionRetryScope,
) -> Result<B::Output, aws_sdk_s3::error::SdkError<B::Error>>
where
    B: SendRetryingOperationAborted,
{
    let deadline = std::time::Instant::now() + configured_test_timeout();
    send_with_operation_contention_retry_until(builder, scope, deadline).await
}

async fn send_with_operation_contention_retry_until<B>(
    builder: B,
    scope: OperationContentionRetryScope,
    deadline: std::time::Instant,
) -> Result<B::Output, aws_sdk_s3::error::SdkError<B::Error>>
where
    B: SendRetryingOperationAborted,
{
    let mut retry_error = None;

    loop {
        if let Some(err) = retry_error.take() {
            let Some(delay) = operation_contention_retry_delay(deadline, std::time::Instant::now())
            else {
                return Err(err);
            };
            tokio::time::sleep(delay).await;
            if std::time::Instant::now() >= deadline {
                return Err(err);
            }
        }

        match builder.clone().send_once().await {
            Err(err) if scope.includes(s3_error_code(&err)) => retry_error = Some(err),
            result => return result,
        }
    }
}

macro_rules! impl_send_retrying_operation_aborted {
    ($builder:path, $output:path, $error:path) => {
        impl SendRetryingOperationAborted for $builder {
            type Output = $output;
            type Error = $error;

            fn send_once(self) -> RetrySendFuture<Self::Output, Self::Error> {
                Box::pin(async move { self.send().await })
            }
        }
    };
}

impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::abort_multipart_upload::builders::AbortMultipartUploadFluentBuilder,
    aws_sdk_s3::operation::abort_multipart_upload::AbortMultipartUploadOutput,
    aws_sdk_s3::operation::abort_multipart_upload::AbortMultipartUploadError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::complete_multipart_upload::builders::CompleteMultipartUploadFluentBuilder,
    aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput,
    aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::copy_object::builders::CopyObjectFluentBuilder,
    aws_sdk_s3::operation::copy_object::CopyObjectOutput,
    aws_sdk_s3::operation::copy_object::CopyObjectError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::create_bucket::builders::CreateBucketFluentBuilder,
    aws_sdk_s3::operation::create_bucket::CreateBucketOutput,
    aws_sdk_s3::operation::create_bucket::CreateBucketError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::create_multipart_upload::builders::CreateMultipartUploadFluentBuilder,
    aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput,
    aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::delete_bucket::builders::DeleteBucketFluentBuilder,
    aws_sdk_s3::operation::delete_bucket::DeleteBucketOutput,
    aws_sdk_s3::operation::delete_bucket::DeleteBucketError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::delete_bucket_cors::builders::DeleteBucketCorsFluentBuilder,
    aws_sdk_s3::operation::delete_bucket_cors::DeleteBucketCorsOutput,
    aws_sdk_s3::operation::delete_bucket_cors::DeleteBucketCorsError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::delete_bucket_encryption::builders::DeleteBucketEncryptionFluentBuilder,
    aws_sdk_s3::operation::delete_bucket_encryption::DeleteBucketEncryptionOutput,
    aws_sdk_s3::operation::delete_bucket_encryption::DeleteBucketEncryptionError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::delete_bucket_lifecycle::builders::DeleteBucketLifecycleFluentBuilder,
    aws_sdk_s3::operation::delete_bucket_lifecycle::DeleteBucketLifecycleOutput,
    aws_sdk_s3::operation::delete_bucket_lifecycle::DeleteBucketLifecycleError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::delete_bucket_ownership_controls::builders::DeleteBucketOwnershipControlsFluentBuilder,
    aws_sdk_s3::operation::delete_bucket_ownership_controls::DeleteBucketOwnershipControlsOutput,
    aws_sdk_s3::operation::delete_bucket_ownership_controls::DeleteBucketOwnershipControlsError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::delete_bucket_tagging::builders::DeleteBucketTaggingFluentBuilder,
    aws_sdk_s3::operation::delete_bucket_tagging::DeleteBucketTaggingOutput,
    aws_sdk_s3::operation::delete_bucket_tagging::DeleteBucketTaggingError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::delete_bucket_policy::builders::DeleteBucketPolicyFluentBuilder,
    aws_sdk_s3::operation::delete_bucket_policy::DeleteBucketPolicyOutput,
    aws_sdk_s3::operation::delete_bucket_policy::DeleteBucketPolicyError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::delete_object::builders::DeleteObjectFluentBuilder,
    aws_sdk_s3::operation::delete_object::DeleteObjectOutput,
    aws_sdk_s3::operation::delete_object::DeleteObjectError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::delete_object_tagging::builders::DeleteObjectTaggingFluentBuilder,
    aws_sdk_s3::operation::delete_object_tagging::DeleteObjectTaggingOutput,
    aws_sdk_s3::operation::delete_object_tagging::DeleteObjectTaggingError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::delete_public_access_block::builders::DeletePublicAccessBlockFluentBuilder,
    aws_sdk_s3::operation::delete_public_access_block::DeletePublicAccessBlockOutput,
    aws_sdk_s3::operation::delete_public_access_block::DeletePublicAccessBlockError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::get_bucket_acl::builders::GetBucketAclFluentBuilder,
    aws_sdk_s3::operation::get_bucket_acl::GetBucketAclOutput,
    aws_sdk_s3::operation::get_bucket_acl::GetBucketAclError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::get_bucket_cors::builders::GetBucketCorsFluentBuilder,
    aws_sdk_s3::operation::get_bucket_cors::GetBucketCorsOutput,
    aws_sdk_s3::operation::get_bucket_cors::GetBucketCorsError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::get_bucket_encryption::builders::GetBucketEncryptionFluentBuilder,
    aws_sdk_s3::operation::get_bucket_encryption::GetBucketEncryptionOutput,
    aws_sdk_s3::operation::get_bucket_encryption::GetBucketEncryptionError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::get_bucket_location::builders::GetBucketLocationFluentBuilder,
    aws_sdk_s3::operation::get_bucket_location::GetBucketLocationOutput,
    aws_sdk_s3::operation::get_bucket_location::GetBucketLocationError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::get_bucket_ownership_controls::builders::GetBucketOwnershipControlsFluentBuilder,
    aws_sdk_s3::operation::get_bucket_ownership_controls::GetBucketOwnershipControlsOutput,
    aws_sdk_s3::operation::get_bucket_ownership_controls::GetBucketOwnershipControlsError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::get_bucket_policy::builders::GetBucketPolicyFluentBuilder,
    aws_sdk_s3::operation::get_bucket_policy::GetBucketPolicyOutput,
    aws_sdk_s3::operation::get_bucket_policy::GetBucketPolicyError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::get_bucket_policy_status::builders::GetBucketPolicyStatusFluentBuilder,
    aws_sdk_s3::operation::get_bucket_policy_status::GetBucketPolicyStatusOutput,
    aws_sdk_s3::operation::get_bucket_policy_status::GetBucketPolicyStatusError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::get_bucket_tagging::builders::GetBucketTaggingFluentBuilder,
    aws_sdk_s3::operation::get_bucket_tagging::GetBucketTaggingOutput,
    aws_sdk_s3::operation::get_bucket_tagging::GetBucketTaggingError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::get_bucket_versioning::builders::GetBucketVersioningFluentBuilder,
    aws_sdk_s3::operation::get_bucket_versioning::GetBucketVersioningOutput,
    aws_sdk_s3::operation::get_bucket_versioning::GetBucketVersioningError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::get_object::builders::GetObjectFluentBuilder,
    aws_sdk_s3::operation::get_object::GetObjectOutput,
    aws_sdk_s3::operation::get_object::GetObjectError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::get_object_acl::builders::GetObjectAclFluentBuilder,
    aws_sdk_s3::operation::get_object_acl::GetObjectAclOutput,
    aws_sdk_s3::operation::get_object_acl::GetObjectAclError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::get_object_lock_configuration::builders::GetObjectLockConfigurationFluentBuilder,
    aws_sdk_s3::operation::get_object_lock_configuration::GetObjectLockConfigurationOutput,
    aws_sdk_s3::operation::get_object_lock_configuration::GetObjectLockConfigurationError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::get_object_attributes::builders::GetObjectAttributesFluentBuilder,
    aws_sdk_s3::operation::get_object_attributes::GetObjectAttributesOutput,
    aws_sdk_s3::operation::get_object_attributes::GetObjectAttributesError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::get_object_tagging::builders::GetObjectTaggingFluentBuilder,
    aws_sdk_s3::operation::get_object_tagging::GetObjectTaggingOutput,
    aws_sdk_s3::operation::get_object_tagging::GetObjectTaggingError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::head_object::builders::HeadObjectFluentBuilder,
    aws_sdk_s3::operation::head_object::HeadObjectOutput,
    aws_sdk_s3::operation::head_object::HeadObjectError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::head_bucket::builders::HeadBucketFluentBuilder,
    aws_sdk_s3::operation::head_bucket::HeadBucketOutput,
    aws_sdk_s3::operation::head_bucket::HeadBucketError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::get_public_access_block::builders::GetPublicAccessBlockFluentBuilder,
    aws_sdk_s3::operation::get_public_access_block::GetPublicAccessBlockOutput,
    aws_sdk_s3::operation::get_public_access_block::GetPublicAccessBlockError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::list_buckets::builders::ListBucketsFluentBuilder,
    aws_sdk_s3::operation::list_buckets::ListBucketsOutput,
    aws_sdk_s3::operation::list_buckets::ListBucketsError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::list_multipart_uploads::builders::ListMultipartUploadsFluentBuilder,
    aws_sdk_s3::operation::list_multipart_uploads::ListMultipartUploadsOutput,
    aws_sdk_s3::operation::list_multipart_uploads::ListMultipartUploadsError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::list_objects::builders::ListObjectsFluentBuilder,
    aws_sdk_s3::operation::list_objects::ListObjectsOutput,
    aws_sdk_s3::operation::list_objects::ListObjectsError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::list_objects_v2::builders::ListObjectsV2FluentBuilder,
    aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output,
    aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::list_object_versions::builders::ListObjectVersionsFluentBuilder,
    aws_sdk_s3::operation::list_object_versions::ListObjectVersionsOutput,
    aws_sdk_s3::operation::list_object_versions::ListObjectVersionsError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::list_parts::builders::ListPartsFluentBuilder,
    aws_sdk_s3::operation::list_parts::ListPartsOutput,
    aws_sdk_s3::operation::list_parts::ListPartsError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::put_bucket_acl::builders::PutBucketAclFluentBuilder,
    aws_sdk_s3::operation::put_bucket_acl::PutBucketAclOutput,
    aws_sdk_s3::operation::put_bucket_acl::PutBucketAclError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::put_bucket_abac::builders::PutBucketAbacFluentBuilder,
    aws_sdk_s3::operation::put_bucket_abac::PutBucketAbacOutput,
    aws_sdk_s3::operation::put_bucket_abac::PutBucketAbacError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::put_bucket_cors::builders::PutBucketCorsFluentBuilder,
    aws_sdk_s3::operation::put_bucket_cors::PutBucketCorsOutput,
    aws_sdk_s3::operation::put_bucket_cors::PutBucketCorsError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::put_bucket_encryption::builders::PutBucketEncryptionFluentBuilder,
    aws_sdk_s3::operation::put_bucket_encryption::PutBucketEncryptionOutput,
    aws_sdk_s3::operation::put_bucket_encryption::PutBucketEncryptionError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::put_bucket_lifecycle_configuration::builders::PutBucketLifecycleConfigurationFluentBuilder,
    aws_sdk_s3::operation::put_bucket_lifecycle_configuration::PutBucketLifecycleConfigurationOutput,
    aws_sdk_s3::operation::put_bucket_lifecycle_configuration::PutBucketLifecycleConfigurationError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::put_bucket_ownership_controls::builders::PutBucketOwnershipControlsFluentBuilder,
    aws_sdk_s3::operation::put_bucket_ownership_controls::PutBucketOwnershipControlsOutput,
    aws_sdk_s3::operation::put_bucket_ownership_controls::PutBucketOwnershipControlsError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::put_bucket_policy::builders::PutBucketPolicyFluentBuilder,
    aws_sdk_s3::operation::put_bucket_policy::PutBucketPolicyOutput,
    aws_sdk_s3::operation::put_bucket_policy::PutBucketPolicyError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::put_bucket_tagging::builders::PutBucketTaggingFluentBuilder,
    aws_sdk_s3::operation::put_bucket_tagging::PutBucketTaggingOutput,
    aws_sdk_s3::operation::put_bucket_tagging::PutBucketTaggingError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::put_bucket_versioning::builders::PutBucketVersioningFluentBuilder,
    aws_sdk_s3::operation::put_bucket_versioning::PutBucketVersioningOutput,
    aws_sdk_s3::operation::put_bucket_versioning::PutBucketVersioningError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::put_object_tagging::builders::PutObjectTaggingFluentBuilder,
    aws_sdk_s3::operation::put_object_tagging::PutObjectTaggingOutput,
    aws_sdk_s3::operation::put_object_tagging::PutObjectTaggingError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::put_object_legal_hold::builders::PutObjectLegalHoldFluentBuilder,
    aws_sdk_s3::operation::put_object_legal_hold::PutObjectLegalHoldOutput,
    aws_sdk_s3::operation::put_object_legal_hold::PutObjectLegalHoldError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::put_object_lock_configuration::builders::PutObjectLockConfigurationFluentBuilder,
    aws_sdk_s3::operation::put_object_lock_configuration::PutObjectLockConfigurationOutput,
    aws_sdk_s3::operation::put_object_lock_configuration::PutObjectLockConfigurationError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::put_object_retention::builders::PutObjectRetentionFluentBuilder,
    aws_sdk_s3::operation::put_object_retention::PutObjectRetentionOutput,
    aws_sdk_s3::operation::put_object_retention::PutObjectRetentionError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::put_object_acl::builders::PutObjectAclFluentBuilder,
    aws_sdk_s3::operation::put_object_acl::PutObjectAclOutput,
    aws_sdk_s3::operation::put_object_acl::PutObjectAclError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::put_public_access_block::builders::PutPublicAccessBlockFluentBuilder,
    aws_sdk_s3::operation::put_public_access_block::PutPublicAccessBlockOutput,
    aws_sdk_s3::operation::put_public_access_block::PutPublicAccessBlockError
);
impl_send_retrying_operation_aborted!(
    aws_sdk_s3::operation::upload_part_copy::builders::UploadPartCopyFluentBuilder,
    aws_sdk_s3::operation::upload_part_copy::UploadPartCopyOutput,
    aws_sdk_s3::operation::upload_part_copy::UploadPartCopyError
);

async fn verify_bucket_exists_after_create_conflict(client: &Client, bucket: &str, context: &str) {
    client
        .head_bucket()
        .bucket(bucket)
        .send()
        .await
        .unwrap_or_else(|head_err| {
            panic!("{context}: BucketAlreadyOwnedByYou but HeadBucket failed for {bucket}: {head_err:?}");
        });
}

async fn wait_for_bucket_ownership_controls(
    client: &Client,
    bucket: &str,
    expected: ObjectOwnership,
) {
    const MAX_ATTEMPTS: usize = 20;

    for attempt in 0..MAX_ATTEMPTS {
        let result = client
            .get_bucket_ownership_controls()
            .bucket(bucket)
            .send()
            .await;
        if let Ok(resp) = result {
            if resp.ownership_controls().map(|controls| {
                controls
                    .rules()
                    .iter()
                    .any(|rule| rule.object_ownership() == &expected)
            }) == Some(true)
            {
                return;
            }
        }
        if attempt + 1 < MAX_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }
        panic!("ObjectOwnership={expected:?} did not converge for {bucket}");
    }
}

/// Create a bucket through the default S3 ownership path.
///
/// Current AWS behavior and this server's frontend default new buckets to
/// `BucketOwnerEnforced` when `x-amz-object-ownership` is absent. Use this
/// helper when a test intentionally exercises the modern BOE path.
pub async fn create_boe_bucket(client: &Client) -> String {
    let bucket = unique_bucket();
    create_test_bucket(client, &bucket).await;
    bucket
}

/// Create a bucket with an explicit object ownership mode.
pub async fn create_bucket_with_ownership(client: &Client, ownership: ObjectOwnership) -> String {
    let bucket = unique_bucket();
    create_test_bucket_with_ownership(client, &bucket, ownership).await;
    bucket
}

/// Create a bucket in legacy ACL mode and disable bucket-level public access block.
///
/// This helper is for tests that intentionally exercise ACL authorization. It
/// accepts `ObjectWriter` and `BucketOwnerPreferred`, but rejects BOE because
/// BOE disables ACL writes by design.
pub async fn create_acl_enabled_bucket(client: &Client, ownership: ObjectOwnership) -> String {
    assert!(
        matches!(
            ownership,
            ObjectOwnership::ObjectWriter | ObjectOwnership::BucketOwnerPreferred
        ),
        "ACL-enabled test buckets must use ObjectWriter or BucketOwnerPreferred"
    );
    let bucket = create_bucket_with_ownership(client, ownership).await;
    disable_bucket_public_access_block(client, &bucket).await;
    bucket
}

async fn wait_for_bucket_public_access_block_disabled(client: &Client, bucket: &str) {
    const MAX_ATTEMPTS: usize = 20;

    for attempt in 0..MAX_ATTEMPTS {
        let result = client
            .get_public_access_block()
            .bucket(bucket)
            .send_retrying_operation_aborted(
                "get public access block while waiting for disablement",
            )
            .await;
        if let Ok(resp) = result {
            if let Some(config) = resp.public_access_block_configuration() {
                if config.block_public_acls() == Some(false)
                    && config.ignore_public_acls() == Some(false)
                    && config.block_public_policy() == Some(false)
                    && config.restrict_public_buckets() == Some(false)
                {
                    return;
                }
            }
        }
        if attempt + 1 < MAX_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }
        panic!("bucket public access block disablement did not converge for {bucket}");
    }
}

fn sse_c_enabled_bucket_encryption() -> ServerSideEncryptionConfiguration {
    let default = ServerSideEncryptionByDefault::builder()
        .sse_algorithm(ServerSideEncryption::Aes256)
        .build()
        .unwrap();
    ServerSideEncryptionConfiguration::builder()
        .rules(
            ServerSideEncryptionRule::builder()
                .apply_server_side_encryption_by_default(default)
                .blocked_encryption_types(
                    BlockedEncryptionTypes::builder()
                        .encryption_type(EncryptionType::None)
                        .build(),
                )
                .build(),
        )
        .build()
        .unwrap()
}

fn bucket_encryption_blocks_sse_c(config: &ServerSideEncryptionConfiguration) -> Option<bool> {
    let rule = config.rules().first()?;
    let blocked = rule
        .blocked_encryption_types()
        .map(|types| {
            types
                .encryption_type()
                .iter()
                .map(|value| value.as_str())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    Some(blocked.contains(&"SSE-C"))
}

async fn wait_for_bucket_sse_c_enabled(client: &Client, bucket: &str) {
    const MAX_ATTEMPTS: usize = 20;

    for attempt in 0..MAX_ATTEMPTS {
        let result = client
            .get_bucket_encryption()
            .bucket(bucket)
            .send_retrying_operation_aborted(
                "get bucket encryption while waiting for SSE-C enablement",
            )
            .await;
        if let Ok(resp) = result {
            if let Some(config) = resp.server_side_encryption_configuration() {
                if bucket_encryption_blocks_sse_c(config) == Some(false) {
                    return;
                }
            }
        }
        if attempt + 1 < MAX_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }
        panic!("bucket SSE-C enablement did not converge for {bucket}");
    }
}

/// Explicitly allow SSE-C on a bucket.
///
/// AWS now disables SSE-C by default on new buckets until
/// `PutBucketEncryption` sets `BlockedEncryptionTypes = NONE`. External SSE-C
/// fixtures use this helper so their positive-path coverage stays focused on
/// SSE-C behavior rather than bucket-default gating.
pub async fn enable_bucket_sse_c(client: &Client, bucket: &str) {
    client
        .put_bucket_encryption()
        .bucket(bucket)
        .server_side_encryption_configuration(sse_c_enabled_bucket_encryption())
        .send_retrying_operation_aborted("enable bucket SSE-C")
        .await
        .expect("enable bucket SSE-C");
    wait_for_bucket_sse_c_enabled(client, bucket).await;
}

/// Create a bucket and explicitly allow SSE-C on it.
pub async fn create_bucket_with_sse_c_enabled(
    client: &Client,
    bucket: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    create_test_bucket(client, bucket).await;
    enable_bucket_sse_c(client, bucket).await;
    Ok(())
}

/// Create a bucket and populate it with `n` objects named "key0", "key1", ...
///
/// Returns the bucket name and the list of keys.
pub async fn create_objects(client: &Client, prefix: &str, n: usize) -> (String, Vec<String>) {
    let bucket = unique_bucket();
    create_test_bucket(client, &bucket).await;

    let mut keys = Vec::with_capacity(n);
    for i in 0..n {
        let key = format!("{}key{}", prefix, i);
        put_object_retrying_operation_aborted(client, &bucket, &key, b"content".to_vec()).await;
        keys.push(key);
    }
    (bucket, keys)
}

/// Create a bucket and populate it with objects whose keys are the given strings.
///
/// Each object body is `b"content"`. Returns `(bucket_name, keys_as_owned_strings)`.
pub async fn create_objects_with_keys(client: &Client, keys: &[&str]) -> (String, Vec<String>) {
    let bucket = unique_bucket();
    create_test_bucket(client, &bucket).await;

    let mut owned_keys = Vec::with_capacity(keys.len());
    for key in keys {
        put_object_retrying_operation_aborted(client, &bucket, key, b"content".to_vec()).await;
        owned_keys.push((*key).to_string());
    }
    (bucket, owned_keys)
}

/// Create a public-read bucket.
///
/// Disables bucket-level BlockPublicAccess, sets ObjectOwnership to
/// BucketOwnerPreferred, then applies the public-read ACL. Note that
/// account-level BlockPublicAccess (if enabled) can still override
/// bucket-level settings and cause these calls to fail.
pub async fn create_public_bucket(client: &Client) -> String {
    let bucket = create_acl_enabled_bucket(client, ObjectOwnership::BucketOwnerPreferred).await;

    retrying_operation_aborted("set public-read ACL", || {
        client
            .put_bucket_acl()
            .bucket(&bucket)
            .acl(BucketCannedAcl::PublicRead)
            .send()
    })
    .await;

    bucket
}

/// Create a public-read-write bucket.
///
/// Disables bucket-level BlockPublicAccess, sets ObjectOwnership to
/// BucketOwnerPreferred, then applies the public-read-write ACL.
pub async fn create_public_write_bucket(client: &Client) -> String {
    let bucket = create_acl_enabled_bucket(client, ObjectOwnership::BucketOwnerPreferred).await;

    retrying_operation_aborted("set public-read-write ACL", || {
        client
            .put_bucket_acl()
            .bucket(&bucket)
            .acl(BucketCannedAcl::PublicReadWrite)
            .send()
    })
    .await;

    // Issue one read-back pass after the control-plane writes. This is not a
    // convergence loop; it just gives AWS a moment to settle before tests make
    // anonymous data-plane requests against the bucket.
    client
        .get_public_access_block()
        .bucket(&bucket)
        .send_retrying_operation_aborted("read public access block")
        .await
        .expect("read public access block");
    client
        .get_bucket_ownership_controls()
        .bucket(&bucket)
        .send_retrying_operation_aborted("read ownership controls")
        .await
        .expect("read ownership controls");
    client
        .get_bucket_acl()
        .bucket(&bucket)
        .send_retrying_operation_aborted("read bucket ACL")
        .await
        .expect("read bucket ACL");

    bucket
}

/// Delete all listed keys from the bucket, then delete the bucket itself.
pub async fn delete_all_and_bucket(client: &Client, bucket: &str, keys: &[String]) {
    for key in keys {
        let _ = delete_object_retrying_operation_aborted(client, bucket, key).await;
    }
    delete_bucket_retrying_operation_aborted(client, bucket).await;
}

/// Build a DeleteObjects request that sets the required Content-MD5 header from
/// the serialized XML body before signing.
pub fn delete_objects_with_md5(
    client: &Client,
    bucket: &str,
    delete: Delete,
) -> CustomizableOperation<DeleteObjectsOutput, DeleteObjectsError, DeleteObjectsFluentBuilder> {
    client
        .delete_objects()
        .bucket(bucket)
        .delete(delete)
        .customize()
        .mutate_request(|req| {
            let body = req
                .body()
                .bytes()
                .expect("DeleteObjects body must be in-memory");
            let digest = md5_legacy::Md5::digest(body);
            let content_md5 = base64::engine::general_purpose::STANDARD.encode(&digest[..]);
            req.headers_mut().insert("content-md5", content_md5);
        })
}

pub async fn delete_objects_retrying_operation_aborted(
    client: &Client,
    bucket: &str,
    delete: Delete,
) -> DeleteObjectsOutput {
    const RETRY_DELAY: Duration = Duration::from_millis(100);
    let deadline = std::time::Instant::now() + configured_test_timeout();
    let quiet = delete.quiet();
    let all_objects = delete.objects().to_vec();
    let mut pending = all_objects.clone();
    let mut deleted = Vec::new();
    let mut errors = Vec::new();

    loop {
        let request_delete = Delete::builder()
            .set_objects(Some(pending.clone()))
            .set_quiet(quiet)
            .build()
            .unwrap();

        let resp = match delete_objects_with_md5(client, bucket, request_delete)
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(err)
                if is_retryable_operation_contention(&err)
                    && std::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(RETRY_DELAY).await;
                continue;
            }
            Err(err) => panic!("delete objects: {err:?}"),
        };

        deleted.extend(resp.deleted().iter().cloned());

        let mut retry = Vec::new();
        for error in resp.errors() {
            if matches!(error.code(), Some("OperationAborted" | "SlowDown"))
                && std::time::Instant::now() < deadline
            {
                retry.push(matching_delete_object(&all_objects, error));
            } else {
                errors.push(error.clone());
            }
        }

        if retry.is_empty() {
            return build_delete_objects_output(deleted, errors);
        }

        pending = retry;
        tokio::time::sleep(RETRY_DELAY).await;
    }
}

fn matching_delete_object(
    objects: &[ObjectIdentifier],
    error: &DeleteObjectError,
) -> ObjectIdentifier {
    let key = error.key().unwrap_or_default();
    let version_id = error.version_id();
    objects
        .iter()
        .find(|object| object.key() == key && object.version_id() == version_id)
        .cloned()
        .unwrap_or_else(|| {
            ObjectIdentifier::builder()
                .key(key)
                .set_version_id(version_id.map(ToString::to_string))
                .build()
                .unwrap()
        })
}

fn build_delete_objects_output(
    deleted: Vec<DeletedObject>,
    errors: Vec<DeleteObjectError>,
) -> DeleteObjectsOutput {
    DeleteObjectsOutput::builder()
        .set_deleted((!deleted.is_empty()).then_some(deleted))
        .set_errors((!errors.is_empty()).then_some(errors))
        .build()
}

/// Minimal response data for raw signed HTTP test requests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: String,
    pub body_read_error: Option<String>,
}

#[derive(Clone, Copy, Debug)]
pub struct SignedRequestCredentials<'a> {
    pub access_key: &'a str,
    pub secret_key: &'a str,
    pub region: &'a str,
    pub tls_ca_pem: Option<&'a [u8]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresignedRequest {
    uri: String,
    headers: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedRequestHeaders {
    headers: Vec<(String, String)>,
}

impl SignedRequestHeaders {
    pub fn headers(&self) -> impl Iterator<Item = (&str, &str)> {
        self.headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
    }
}

impl PresignedRequest {
    pub fn uri(&self) -> &str {
        &self.uri
    }

    pub fn headers(&self) -> impl Iterator<Item = (&str, &str)> {
        self.headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
    }
}

/// Build a `Content-MD5` header pair for the request body.
pub fn content_md5_header(body: &[u8]) -> (String, String) {
    ("Content-MD5".to_string(), md5_b64(body))
}

/// Build a CRC32-based SDK checksum header set for the request body.
pub fn sdk_checksum_headers(body: &[u8]) -> Vec<(String, String)> {
    vec![
        (
            "x-amz-sdk-checksum-algorithm".to_string(),
            "CRC32".to_string(),
        ),
        ("x-amz-checksum-crc32".to_string(), crc32_b64(body)),
    ]
}

pub fn object_url(endpoint: &str, bucket: &str, key: &str, query: Option<&str>) -> String {
    let encoded_key = auth::canonical::uri_encode_path(key);
    match query {
        Some(query) => format!("{endpoint}/{bucket}/{encoded_key}?{query}"),
        None => format!("{endpoint}/{bucket}/{encoded_key}"),
    }
}

pub fn bucket_location_url(endpoint: &str, bucket: &str) -> String {
    format!("{endpoint}/{bucket}?location")
}

pub fn expected_raw_bucket_location_constraint(region: &str) -> Option<&str> {
    match region {
        "us-east-1" => None,
        other => Some(other),
    }
}

pub fn assert_raw_bucket_location(response: &RawResponse, region: &str) {
    assert_eq!(
        response.status, 200,
        "expected GetBucketLocation success, got {response:?}"
    );
    assert_eq!(
        response.body_read_error, None,
        "failed to read GetBucketLocation body: {response:?}"
    );
    assert!(
        response.body.contains("<LocationConstraint"),
        "GetBucketLocation response missing LocationConstraint: {}",
        response.body
    );
    match expected_raw_bucket_location_constraint(region) {
        Some(expected) => assert!(
            response
                .body
                .contains(&format!(">{expected}</LocationConstraint>")),
            "expected raw LocationConstraint {expected:?}, got body: {}",
            response.body
        ),
        None => {
            assert!(
                response.body.contains("/>") || response.body.contains("></LocationConstraint>"),
                "expected empty raw LocationConstraint for {region}, got body: {}",
                response.body
            );
            assert!(
                !response.body.contains(">us-east-1</LocationConstraint>"),
                "us-east-1 must be represented as an empty LocationConstraint, got body: {}",
                response.body
            );
        }
    }
}

pub fn presign_url<K, V, I>(
    method: &str,
    url_str: &str,
    expires: Duration,
    extra_headers: I,
    payload_hash: Option<&str>,
) -> PresignedRequest
where
    K: AsRef<str>,
    V: AsRef<str>,
    I: IntoIterator<Item = (K, V)>,
{
    presign_url_with_credentials(
        method,
        url_str,
        expires,
        extra_headers,
        payload_hash,
        SignedRequestCredentials {
            access_key: CTX.access_key(),
            secret_key: CTX.secret_key(),
            region: CTX.region(),
            tls_ca_pem: CTX.tls_ca_pem(),
        },
    )
}

pub fn presign_url_with_credentials<K, V, I>(
    method: &str,
    url_str: &str,
    expires: Duration,
    extra_headers: I,
    payload_hash: Option<&str>,
    credentials: SignedRequestCredentials<'_>,
) -> PresignedRequest
where
    K: AsRef<str>,
    V: AsRef<str>,
    I: IntoIterator<Item = (K, V)>,
{
    presign_url_with_credentials_inner(
        method,
        url_str,
        expires,
        extra_headers,
        payload_hash,
        credentials,
        PresignSettings {
            service: "s3",
            include_host_signed_header: true,
            preserve_base_query_order: false,
        },
    )
}

pub fn presign_url_for_service_with_credentials<K, V, I>(
    method: &str,
    url_str: &str,
    expires: Duration,
    extra_headers: I,
    payload_hash: Option<&str>,
    service: &str,
    credentials: SignedRequestCredentials<'_>,
) -> PresignedRequest
where
    K: AsRef<str>,
    V: AsRef<str>,
    I: IntoIterator<Item = (K, V)>,
{
    presign_url_with_credentials_inner(
        method,
        url_str,
        expires,
        extra_headers,
        payload_hash,
        credentials,
        PresignSettings {
            service,
            include_host_signed_header: true,
            preserve_base_query_order: true,
        },
    )
}

pub fn presign_url_without_host_signed_header<K, V, I>(
    method: &str,
    url_str: &str,
    expires: Duration,
    extra_headers: I,
    payload_hash: Option<&str>,
    credentials: SignedRequestCredentials<'_>,
) -> PresignedRequest
where
    K: AsRef<str>,
    V: AsRef<str>,
    I: IntoIterator<Item = (K, V)>,
{
    presign_url_with_credentials_inner(
        method,
        url_str,
        expires,
        extra_headers,
        payload_hash,
        credentials,
        PresignSettings {
            service: "s3",
            include_host_signed_header: false,
            preserve_base_query_order: false,
        },
    )
}

fn canonicalize_request_headers(headers: &mut [(String, String)]) -> (String, String) {
    headers.sort_by(|left, right| left.0.cmp(&right.0));

    let mut signed_header_names = Vec::new();
    for (name, _) in headers.iter() {
        if signed_header_names.last().copied() != Some(name.as_str()) {
            signed_header_names.push(name.as_str());
        }
    }
    let signed_headers = signed_header_names.join(";");
    let header_refs = headers
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect::<Vec<_>>();
    let canonical_headers = auth::canonical::canonical_headers(&header_refs);

    (signed_headers, canonical_headers)
}

#[derive(Clone, Copy)]
struct PresignSettings<'a> {
    service: &'a str,
    include_host_signed_header: bool,
    preserve_base_query_order: bool,
}

fn presign_url_with_credentials_inner<K, V, I>(
    method: &str,
    url_str: &str,
    expires: Duration,
    extra_headers: I,
    payload_hash: Option<&str>,
    credentials: SignedRequestCredentials<'_>,
    settings: PresignSettings<'_>,
) -> PresignedRequest
where
    K: AsRef<str>,
    V: AsRef<str>,
    I: IntoIterator<Item = (K, V)>,
{
    let parsed = url::Url::parse(url_str).expect("parse URL");
    let path = parsed.path();
    let base_query = parsed.query().unwrap_or("");
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let amz_date = format_amz_date(secs);
    let date_stamp = &amz_date[..8];
    let host = parsed
        .host_str()
        .map(|host| {
            if let Some(port) = parsed.port() {
                format!("{host}:{port}")
            } else {
                host.to_string()
            }
        })
        .expect("URL host");

    let payload_hash = payload_hash.unwrap_or("UNSIGNED-PAYLOAD").to_string();
    let mut request_headers = Vec::new();
    if settings.include_host_signed_header {
        request_headers.push(("host".to_string(), host));
    }
    if payload_hash != "UNSIGNED-PAYLOAD" {
        request_headers.push(("x-amz-content-sha256".to_string(), payload_hash.clone()));
    }
    for (name, value) in extra_headers {
        request_headers.push((name.as_ref().to_lowercase(), value.as_ref().to_string()));
    }
    let (signed_headers, canonical_headers) = canonicalize_request_headers(&mut request_headers);
    let credential = format!(
        "{}/{}/{}/{}/aws4_request",
        credentials.access_key, date_stamp, credentials.region, settings.service
    );
    let mut raw_query_parts = Vec::new();
    if !base_query.is_empty() {
        raw_query_parts.push(base_query.to_string());
    }
    raw_query_parts.push("X-Amz-Algorithm=AWS4-HMAC-SHA256".to_string());
    raw_query_parts.push(format!("X-Amz-Credential={credential}"));
    raw_query_parts.push(format!("X-Amz-Date={amz_date}"));
    raw_query_parts.push(format!("X-Amz-Expires={}", expires.as_secs()));
    raw_query_parts.push(format!("X-Amz-SignedHeaders={signed_headers}"));
    let canonical_query = normalize_query(&raw_query_parts.join("&"));
    let wire_query = if settings.preserve_base_query_order && !base_query.is_empty() {
        format!(
            "{base_query}&{}",
            normalize_query(&raw_query_parts[1..].join("&"))
        )
    } else {
        canonical_query.clone()
    };
    let canonical_request = format!(
        "{method}\n{path}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
    );
    let scope = format!(
        "{date_stamp}/{}/{}/aws4_request",
        credentials.region, settings.service
    );
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );
    let signing_key = derive_signing_key_with_service(
        credentials.secret_key,
        date_stamp,
        credentials.region,
        settings.service,
    );
    let signature = hmac_sha256(&signing_key, string_to_sign.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let uri = format!(
        "{}{}?{}&X-Amz-Signature={signature}",
        parsed.origin().ascii_serialization(),
        path,
        wire_query
    );
    let headers = request_headers
        .into_iter()
        .filter(|(name, _)| name != "host")
        .collect();

    PresignedRequest { uri, headers }
}

pub fn sign_request_headers_with_credentials<K, V, I>(
    method: &str,
    url_str: &str,
    body: &[u8],
    extra_headers: I,
    credentials: SignedRequestCredentials<'_>,
) -> SignedRequestHeaders
where
    K: AsRef<str>,
    V: AsRef<str>,
    I: IntoIterator<Item = (K, V)>,
{
    sign_request_headers_for_service_with_credentials(
        method,
        url_str,
        body,
        extra_headers,
        "s3",
        credentials,
        true,
    )
}

fn sign_request_headers_for_service_with_credentials<K, V, I>(
    method: &str,
    url_str: &str,
    body: &[u8],
    extra_headers: I,
    service: &str,
    credentials: SignedRequestCredentials<'_>,
    include_host_signed_header: bool,
) -> SignedRequestHeaders
where
    K: AsRef<str>,
    V: AsRef<str>,
    I: IntoIterator<Item = (K, V)>,
{
    let parsed = url::Url::parse(url_str).expect("parse signed URL");
    let path = parsed.path();
    let query = normalize_query(parsed.query().unwrap_or(""));
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let amz_date = format_amz_date(secs);
    let date_stamp = amz_date[..8].to_string();
    let host = parsed
        .host_str()
        .map(|host| {
            if let Some(port) = parsed.port() {
                format!("{host}:{port}")
            } else {
                host.to_string()
            }
        })
        .expect("URL host");
    let payload_hash = sha256_hex(body);

    let mut request_headers: Vec<(String, String)> = vec![
        ("x-amz-content-sha256".to_string(), payload_hash.clone()),
        ("x-amz-date".to_string(), amz_date),
    ];
    if include_host_signed_header {
        request_headers.push(("host".to_string(), host));
    }
    for (name, value) in extra_headers {
        request_headers.push((name.as_ref().to_lowercase(), value.as_ref().to_string()));
    }
    let (signed_headers, canonical_headers) = canonicalize_request_headers(&mut request_headers);
    let canonical_request =
        format!("{method}\n{path}\n{query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}");

    let scope = format!("{date_stamp}/{}/{service}/aws4_request", credentials.region);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{scope}\n{}",
        request_headers
            .iter()
            .find(|(name, _)| name == "x-amz-date")
            .map(|(_, value)| value.as_str())
            .expect("signed request contains x-amz-date"),
        sha256_hex(canonical_request.as_bytes())
    );
    let signing_key = derive_signing_key_with_service(
        credentials.secret_key,
        &date_stamp,
        credentials.region,
        service,
    );
    let signature: String = hmac_sha256(&signing_key, string_to_sign.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        credentials.access_key
    );
    request_headers.push(("authorization".to_string(), authorization));

    SignedRequestHeaders {
        headers: request_headers,
    }
}

/// Send a raw signed S3 request, bypassing SDK auto-checksum behavior.
pub fn send_signed_request<K, V, I>(
    method: &str,
    url_str: &str,
    body: &[u8],
    extra_headers: I,
) -> RawResponse
where
    K: AsRef<str>,
    V: AsRef<str>,
    I: IntoIterator<Item = (K, V)>,
{
    send_signed_request_for_service_with_credentials(
        method,
        url_str,
        body,
        extra_headers,
        "s3",
        SignedRequestCredentials {
            access_key: CTX.access_key(),
            secret_key: CTX.secret_key(),
            region: CTX.region(),
            tls_ca_pem: CTX.tls_ca_pem(),
        },
    )
}

fn raw_request_url(bucket: &str, key: &str, query: Option<&str>) -> String {
    if key.is_empty() {
        match query {
            Some(query) => format!("{}/{bucket}?{query}", CTX.endpoint()),
            None => format!("{}/{bucket}", CTX.endpoint()),
        }
    } else {
        object_url(CTX.endpoint(), bucket, key, query)
    }
}

/// Send a bodyless raw signed object request with the primary credentials.
pub fn raw_object(method: &str, bucket: &str, key: &str) -> RawResponse {
    raw_object_with(method, bucket, key, b"", &[])
}

/// Send a bodyless raw signed object request with a query string, e.g. an
/// object subresource such as `tagging`.
pub fn raw_object_query(method: &str, bucket: &str, key: &str, query: &str) -> RawResponse {
    send_signed_request(
        method,
        &raw_request_url(bucket, key, Some(query)),
        b"",
        std::iter::empty::<(&str, &str)>(),
    )
}

/// Send a raw signed object request with a body and extra headers.
pub fn raw_object_with(
    method: &str,
    bucket: &str,
    key: &str,
    body: &[u8],
    extra_headers: &[(&str, &str)],
) -> RawResponse {
    send_signed_request(
        method,
        &raw_request_url(bucket, key, None),
        body,
        extra_headers.iter().copied(),
    )
}

pub fn raw_alt_credentials() -> SignedRequestCredentials<'static> {
    SignedRequestCredentials {
        access_key: CTX.alt_access_key(),
        secret_key: CTX.alt_secret_key(),
        region: CTX.region(),
        tls_ca_pem: CTX.tls_ca_pem(),
    }
}

#[derive(Clone, Copy)]
pub struct RawAltObjectRequest<'a> {
    method: &'a str,
    bucket: &'a str,
    key: &'a str,
    query: Option<&'a str>,
    extra_headers: &'a [(&'a str, &'a str)],
}

impl<'a> RawAltObjectRequest<'a> {
    pub fn new(method: &'a str, bucket: &'a str, key: &'a str) -> Self {
        Self {
            method,
            bucket,
            key,
            query: None,
            extra_headers: &[],
        }
    }

    pub fn query(self, query: &'a str) -> Self {
        Self {
            query: Some(query),
            ..self
        }
    }

    pub fn extra_headers(self, extra_headers: &'a [(&'a str, &'a str)]) -> Self {
        Self {
            extra_headers,
            ..self
        }
    }
}

pub fn raw_alt_object_request(request: RawAltObjectRequest<'_>) -> RawResponse {
    send_signed_request_with_credentials(
        request.method,
        &raw_request_url(request.bucket, request.key, request.query),
        b"",
        request.extra_headers.iter().copied(),
        raw_alt_credentials(),
    )
}

pub fn raw_response_header<'a>(response: &'a RawResponse, name: &str) -> Option<&'a str> {
    response
        .headers
        .iter()
        .find(|(header_name, _)| header_name.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

pub async fn eventually_raw_alt_object_status(
    description: &str,
    request: RawAltObjectRequest<'_>,
    expected_status: u16,
    required_header: Option<&str>,
) -> RawResponse {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);

    loop {
        let response = raw_alt_object_request(request);
        if response.status == expected_status
            && required_header.is_none_or(|name| raw_response_header(&response, name).is_some())
        {
            return response;
        }

        if tokio::time::Instant::now() >= deadline {
            panic!("{description} did not converge to {expected_status}: {response:?}");
        }

        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Send a bodyless raw signed bucket-level request with the primary
/// credentials, optionally with a query string, e.g. `list-type=2` or a
/// subresource such as `versioning`.
pub fn raw_bucket(method: &str, bucket: &str, query: Option<&str>) -> RawResponse {
    send_signed_request(
        method,
        &raw_request_url(bucket, "", query),
        b"",
        std::iter::empty::<(&str, &str)>(),
    )
}

/// Send an unauthenticated raw request against the shared test endpoint.
pub fn raw_anonymous(method: &str, bucket: &str, key: &str, query: Option<&str>) -> RawResponse {
    let url = raw_request_url(bucket, key, query);
    let agent = crate::test_agent();
    let mut response = match method {
        "GET" => agent.get(&url).call(),
        "HEAD" => agent.head(&url).call(),
        "DELETE" => agent.delete(&url).call(),
        other => panic!("raw_anonymous does not support method {other}"),
    }
    .expect("anonymous request transport error");
    let headers = response
        .headers()
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_string(),
                value
                    .to_str()
                    .expect("response header is valid utf-8")
                    .to_string(),
            )
        })
        .collect();
    let (body, body_read_error) = match response.body_mut().read_to_string() {
        Ok(body) => (body, None),
        Err(err) => (String::new(), Some(err.to_string())),
    };
    RawResponse {
        status: response.status().as_u16(),
        headers,
        body,
        body_read_error,
    }
}

/// Fetch a URL unauthenticated (e.g. a tampered presigned URL), capturing
/// the full raw response.
pub fn raw_fetch_url(url: &str, extra_headers: &[(&str, &str)]) -> RawResponse {
    let mut request = crate::test_agent().get(url);
    for (name, value) in extra_headers {
        request = request.header(*name, *value);
    }
    let mut response = request.call().expect("raw URL fetch transport error");
    let headers = response
        .headers()
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_string(),
                value
                    .to_str()
                    .expect("response header is valid utf-8")
                    .to_string(),
            )
        })
        .collect();
    let (body, body_read_error) = match response.body_mut().read_to_string() {
        Ok(body) => (body, None),
        Err(err) => (String::new(), Some(err.to_string())),
    };
    RawResponse {
        status: response.status().as_u16(),
        headers,
        body,
        body_read_error,
    }
}

/// Send an unauthenticated raw PUT with a body against the shared test
/// endpoint.
pub fn raw_anonymous_put(bucket: &str, key: &str, body: &[u8]) -> RawResponse {
    let url = raw_request_url(bucket, key, None);
    let mut response = crate::test_agent()
        .put(&url)
        .send(body)
        .expect("anonymous PUT transport error");
    let headers = response
        .headers()
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_string(),
                value
                    .to_str()
                    .expect("response header is valid utf-8")
                    .to_string(),
            )
        })
        .collect();
    let (body, body_read_error) = match response.body_mut().read_to_string() {
        Ok(body) => (body, None),
        Err(err) => (String::new(), Some(err.to_string())),
    };
    RawResponse {
        status: response.status().as_u16(),
        headers,
        body,
        body_read_error,
    }
}

/// Send a raw signed S3 request and preserve response metadata if the response
/// body races with an early server-side connection close.
pub fn send_signed_request_allow_response_body_error<K, V, I>(
    method: &str,
    url_str: &str,
    body: &[u8],
    extra_headers: I,
) -> RawResponse
where
    K: AsRef<str>,
    V: AsRef<str>,
    I: IntoIterator<Item = (K, V)>,
{
    send_signed_request_to_endpoint_for_service_with_credentials_inner(
        method,
        url_str,
        url_str,
        body,
        extra_headers,
        "s3",
        SignedRequestCredentials {
            access_key: CTX.access_key(),
            secret_key: CTX.secret_key(),
            region: CTX.region(),
            tls_ca_pem: CTX.tls_ca_pem(),
        },
        true,
        Vec::new(),
        true,
    )
}

/// Send a raw signed S3 request using explicit endpoint credentials.
pub fn send_signed_request_with_credentials<K, V, I>(
    method: &str,
    url_str: &str,
    body: &[u8],
    extra_headers: I,
    credentials: SignedRequestCredentials<'_>,
) -> RawResponse
where
    K: AsRef<str>,
    V: AsRef<str>,
    I: IntoIterator<Item = (K, V)>,
{
    send_signed_request_for_service_with_credentials(
        method,
        url_str,
        body,
        extra_headers,
        "s3",
        credentials,
    )
}

/// Send a raw signed request whose SigV4 `SignedHeaders` intentionally omits
/// `host`, while still sending the normal HTTP Host header on the wire.
pub fn send_signed_request_without_host_signed_header<K, V, I>(
    method: &str,
    url_str: &str,
    body: &[u8],
    extra_headers: I,
    credentials: SignedRequestCredentials<'_>,
) -> RawResponse
where
    K: AsRef<str>,
    V: AsRef<str>,
    I: IntoIterator<Item = (K, V)>,
{
    send_signed_request_to_endpoint_for_service_with_credentials_inner(
        method,
        url_str,
        url_str,
        body,
        extra_headers,
        "s3",
        credentials,
        false,
        Vec::new(),
        false,
    )
}

/// Send a raw signed request with additional headers present on the wire but
/// intentionally excluded from SigV4 `SignedHeaders`.
pub fn send_signed_request_with_unsigned_headers<K, V, I>(
    method: &str,
    url_str: &str,
    body: &[u8],
    extra_headers: I,
    unsigned_headers: &[(&str, &str)],
    credentials: SignedRequestCredentials<'_>,
) -> RawResponse
where
    K: AsRef<str>,
    V: AsRef<str>,
    I: IntoIterator<Item = (K, V)>,
{
    let unsigned_headers = unsigned_headers
        .iter()
        .map(|(name, value)| (name.to_lowercase(), (*value).to_string()))
        .collect();
    send_signed_request_to_endpoint_for_service_with_credentials_inner(
        method,
        url_str,
        url_str,
        body,
        extra_headers,
        "s3",
        credentials,
        false,
        unsigned_headers,
        true,
    )
}

/// Send a raw signed request using an explicit SigV4 service name.
pub fn send_signed_request_for_service_with_credentials<K, V, I>(
    method: &str,
    url_str: &str,
    body: &[u8],
    extra_headers: I,
    service: &str,
    credentials: SignedRequestCredentials<'_>,
) -> RawResponse
where
    K: AsRef<str>,
    V: AsRef<str>,
    I: IntoIterator<Item = (K, V)>,
{
    send_signed_request_to_endpoint_for_service_with_credentials(
        method,
        url_str,
        url_str,
        body,
        extra_headers,
        service,
        credentials,
    )
}

/// Send a raw signed request to one endpoint while signing and sending a
/// distinct `Host` header.
pub fn send_signed_request_to_endpoint_for_service_with_credentials<K, V, I>(
    method: &str,
    connect_url_str: &str,
    signed_url_str: &str,
    body: &[u8],
    extra_headers: I,
    service: &str,
    credentials: SignedRequestCredentials<'_>,
) -> RawResponse
where
    K: AsRef<str>,
    V: AsRef<str>,
    I: IntoIterator<Item = (K, V)>,
{
    send_signed_request_to_endpoint_for_service_with_credentials_inner(
        method,
        connect_url_str,
        signed_url_str,
        body,
        extra_headers,
        service,
        credentials,
        false,
        Vec::new(),
        true,
    )
}

#[allow(clippy::too_many_arguments)]
fn send_signed_request_to_endpoint_for_service_with_credentials_inner<K, V, I>(
    method: &str,
    connect_url_str: &str,
    signed_url_str: &str,
    body: &[u8],
    extra_headers: I,
    service: &str,
    credentials: SignedRequestCredentials<'_>,
    allow_response_body_error: bool,
    unsigned_headers: Vec<(String, String)>,
    include_host_signed_header: bool,
) -> RawResponse
where
    K: AsRef<str>,
    V: AsRef<str>,
    I: IntoIterator<Item = (K, V)>,
{
    let parsed = url::Url::parse(connect_url_str).expect("parse URL");
    let signed_parsed = url::Url::parse(signed_url_str).expect("parse signed URL");
    let endpoint = parsed.origin().ascii_serialization();
    let agent = crate::build_test_agent(
        &endpoint,
        credentials.tls_ca_pem,
        crate::configured_test_timeout(),
    );
    let host_header = signed_parsed
        .host_str()
        .map(|host| {
            if let Some(port) = signed_parsed.port() {
                format!("{host}:{port}")
            } else {
                host.to_string()
            }
        })
        .expect("URL host");
    let signed_request = sign_request_headers_for_service_with_credentials(
        method,
        signed_url_str,
        body,
        extra_headers,
        service,
        credentials,
        include_host_signed_header,
    );

    const MAX_SLOWDOWN_RETRIES: u32 = 4;
    const MAX_TRANSPORT_RETRIES: u32 = 3;
    let operation_aborted_deadline = std::time::Instant::now() + configured_test_timeout();
    let mut attempt = 0;
    loop {
        let response_result = if method == "HEAD" {
            let mut request = agent.head(connect_url_str).header("Host", &host_header);
            for (name, value) in &signed_request.headers {
                if name == "host" {
                    continue;
                }
                request = request.header(name, value);
            }
            for (name, value) in &unsigned_headers {
                request = request.header(name, value);
            }
            request.call()
        } else if method == "GET" {
            let mut request = agent.get(connect_url_str).header("Host", &host_header);
            for (name, value) in &signed_request.headers {
                if name == "host" {
                    continue;
                }
                request = request.header(name, value);
            }
            for (name, value) in &unsigned_headers {
                request = request.header(name, value);
            }
            request.call()
        } else if method == "DELETE" {
            let mut request = agent.delete(connect_url_str).header("Host", &host_header);
            for (name, value) in &signed_request.headers {
                if name == "host" {
                    continue;
                }
                request = request.header(name, value);
            }
            for (name, value) in &unsigned_headers {
                request = request.header(name, value);
            }
            request.call()
        } else {
            let mut request = match method {
                "PUT" => agent.put(connect_url_str),
                "POST" => agent.post(connect_url_str),
                other => agent.request(other, connect_url_str),
            }
            .header("Host", &host_header);
            for (name, value) in &signed_request.headers {
                if name == "host" {
                    continue;
                }
                request = request.header(name, value);
            }
            for (name, value) in &unsigned_headers {
                request = request.header(name, value);
            }
            if allow_response_body_error {
                request.send_allow_response_body_error(body)
            } else {
                request.send(body)
            }
        };
        let mut response = match response_result {
            Ok(response) => response,
            Err(_err) if attempt < MAX_TRANSPORT_RETRIES => {
                let backoff_ms = 100u64 << attempt;
                thread::sleep(Duration::from_millis(backoff_ms));
                attempt += 1;
                continue;
            }
            Err(err) => panic!("raw request transport error: {err}"),
        };
        let status = response.status().as_u16();
        let body_read_error = response.body_read_error().map(ToOwned::to_owned);
        let body_text = response.body_mut().read_to_string().unwrap_or_default();
        if status == 503
            && body_text.contains("<Code>SlowDown</Code>")
            && attempt < MAX_SLOWDOWN_RETRIES
        {
            let backoff_ms = 200u64 << attempt;
            thread::sleep(Duration::from_millis(backoff_ms));
            attempt += 1;
            continue;
        }
        if status == 409
            && body_text.contains("<Code>OperationAborted</Code>")
            && std::time::Instant::now() < operation_aborted_deadline
        {
            thread::sleep(Duration::from_millis(100));
            continue;
        }
        let headers = response
            .headers()
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_string(),
                    value
                        .to_str()
                        .expect("response header is valid utf-8")
                        .to_string(),
                )
            })
            .collect();
        return RawResponse {
            status,
            headers,
            body: body_text,
            body_read_error,
        };
    }
}

fn derive_signing_key_with_service(
    secret_key: &str,
    date_stamp: &str,
    region: &str,
    service: &str,
) -> Vec<u8> {
    let k_date = hmac_sha256(
        format!("AWS4{secret_key}").as_bytes(),
        date_stamp.as_bytes(),
    );
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

fn md5_b64(data: &[u8]) -> String {
    let digest = md5_legacy::Md5::digest(data);
    base64::engine::general_purpose::STANDARD.encode(&digest[..])
}

fn crc32_b64(data: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(checksum::crc32::checksum(data).to_be_bytes())
}

fn sha256_hex(data: &[u8]) -> String {
    let digest = ring::digest::digest(&ring::digest::SHA256, data);
    digest
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let key = hmac::Key::new(hmac::HMAC_SHA256, key);
    hmac::sign(&key, data).as_ref().to_vec()
}

pub(crate) fn format_amz_date(epoch_secs: u64) -> String {
    let days = epoch_secs / 86_400;
    let time_of_day = epoch_secs % 86_400;
    let (year, month, day) = days_to_date(days as i64);
    format!(
        "{year:04}{month:02}{day:02}T{:02}{:02}{:02}Z",
        time_of_day / 3_600,
        (time_of_day % 3_600) / 60,
        time_of_day % 60,
    )
}

fn days_to_date(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = (z - era * 146_097) as u32;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era as i64 + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = if month_prime < 10 {
        month_prime + 3
    } else {
        month_prime - 9
    };
    let year = if month <= 2 { year + 1 } else { year };
    (year, month, day)
}

fn normalize_query(raw: &str) -> String {
    canonical_query_string(raw)
}

/// Build a PutBucketLifecycleConfiguration request that sets the required
/// Content-MD5 header from the serialized XML body before signing.
pub fn put_bucket_lifecycle_with_md5(
    client: &Client,
    bucket: &str,
    lifecycle_configuration: BucketLifecycleConfiguration,
) -> CustomizableOperation<
    PutBucketLifecycleConfigurationOutput,
    PutBucketLifecycleConfigurationError,
    PutBucketLifecycleConfigurationFluentBuilder,
> {
    client
        .put_bucket_lifecycle_configuration()
        .bucket(bucket)
        .lifecycle_configuration(lifecycle_configuration)
        .customize()
        .mutate_request(|req| {
            let body = req
                .body()
                .bytes()
                .expect("PutBucketLifecycleConfiguration body must be in-memory");
            let digest = md5_legacy::Md5::digest(body);
            let content_md5 = base64::engine::general_purpose::STANDARD.encode(&digest[..]);
            req.headers_mut().insert("content-md5", content_md5);
        })
}

/// Build an `x-amz-copy-source` value for a specific object version.
///
/// The source key path is percent-encoded with `/` preserved as a path
/// separator. The `version_id` query component is always percent-encoded.
pub fn copy_source_with_version(bucket: &str, source_key: &str, version_id: &str) -> String {
    let encoded_key = auth::canonical::uri_encode_path(source_key);
    let encoded_version_id = auth::canonical::uri_encode(version_id);
    format!("{bucket}/{encoded_key}?versionId={encoded_version_id}")
}

/// Delete all object versions and delete markers in a bucket, then delete the bucket.
///
/// This is needed for versioned buckets on AWS where simple delete_object creates
/// delete markers rather than removing objects.
pub async fn cleanup_versioned_bucket(client: &Client, bucket: &str) {
    loop {
        let resp = match client
            .list_object_versions()
            .bucket(bucket)
            .send_retrying_operation_aborted("list object versions during versioned cleanup")
            .await
        {
            Ok(resp) => resp,
            Err(err) if is_bucket_already_absent(&err) => return,
            Err(err) => panic!("list object versions: {err:?}"),
        };

        let mut objects: Vec<aws_sdk_s3::types::ObjectIdentifier> = Vec::new();

        for v in resp.versions() {
            objects.push(object_identifier_for_listed_version(
                v.key().unwrap_or_default(),
                v.version_id(),
            ));
        }
        for m in resp.delete_markers() {
            objects.push(object_identifier_for_listed_version(
                m.key().unwrap_or_default(),
                m.version_id(),
            ));
        }

        if objects.is_empty() {
            break;
        }

        for chunk in objects.chunks(25) {
            let delete = aws_sdk_s3::types::Delete::builder()
                .set_objects(Some(chunk.to_vec()))
                .quiet(true)
                .build()
                .unwrap();
            let resp = delete_objects_retrying_operation_aborted(client, bucket, delete).await;
            assert!(
                resp.errors().is_empty(),
                "delete objects returned embedded errors: {:?}",
                resp.errors()
            );
        }
    }

    delete_bucket_retrying_operation_aborted(client, bucket).await;
}

fn object_identifier_for_listed_version(key: &str, version_id: Option<&str>) -> ObjectIdentifier {
    ObjectIdentifier::builder()
        .key(key)
        .set_version_id(
            version_id
                .filter(|id| !id.is_empty())
                .map(ToString::to_string),
        )
        .build()
        .unwrap()
}

pub async fn delete_bucket_retrying_operation_aborted(client: &Client, bucket: &str) {
    const RETRY_DELAY: Duration = Duration::from_millis(100);
    let deadline = std::time::Instant::now() + configured_test_timeout();
    let mut attempts = 0u32;
    let mut retryable_errors = 0u32;

    loop {
        attempts += 1;
        match client.delete_bucket().bucket(bucket).send().await {
            Ok(_) => return,
            Err(err) if is_bucket_already_absent(&err) => return,
            Err(err)
                if (s3_error_code(&err) == Some("OperationAborted")
                    || s3_error_code(&err) == Some("SlowDown")
                    || is_bucket_not_empty(&err))
                    && std::time::Instant::now() < deadline =>
            {
                retryable_errors += 1;
                tokio::time::sleep(RETRY_DELAY).await;
            }
            Err(err) => {
                let context = delete_bucket_failure_context(client, bucket).await;
                panic!(
                    "delete bucket after {attempts} attempt(s), {retryable_errors} retryable error(s): {err:?}\n{context}"
                );
            }
        }
    }
}

async fn delete_bucket_failure_context(client: &Client, bucket: &str) -> String {
    let mut context = Vec::new();
    context.push(format!("delete bucket failure context: bucket={bucket}"));

    match client.head_bucket().bucket(bucket).send().await {
        Ok(_) => context.push("head_bucket=exists".to_string()),
        Err(err) => context.push(format!("head_bucket={}", describe_sdk_error(&err))),
    }

    match client
        .list_objects_v2()
        .bucket(bucket)
        .max_keys(10)
        .send()
        .await
    {
        Ok(resp) => {
            let keys = resp
                .contents()
                .iter()
                .filter_map(|object| object.key())
                .take(10)
                .collect::<Vec<_>>();
            context.push(format!(
                "list_objects_v2 key_count={} truncated={} sample_keys={keys:?}",
                resp.key_count().unwrap_or_default(),
                resp.is_truncated().unwrap_or(false)
            ));
        }
        Err(err) => context.push(format!("list_objects_v2={}", describe_sdk_error(&err))),
    }

    match client
        .list_object_versions()
        .bucket(bucket)
        .max_keys(10)
        .send()
        .await
    {
        Ok(resp) => {
            let versions = resp
                .versions()
                .iter()
                .filter_map(|version| version.key().map(|key| (key, version.version_id())))
                .take(10)
                .collect::<Vec<_>>();
            let delete_markers = resp
                .delete_markers()
                .iter()
                .filter_map(|marker| marker.key().map(|key| (key, marker.version_id())))
                .take(10)
                .collect::<Vec<_>>();
            context.push(format!(
                "list_object_versions truncated={} sample_versions={versions:?} sample_delete_markers={delete_markers:?}",
                resp.is_truncated().unwrap_or(false)
            ));
        }
        Err(err) => context.push(format!("list_object_versions={}", describe_sdk_error(&err))),
    }

    match client
        .list_multipart_uploads()
        .bucket(bucket)
        .max_uploads(10)
        .send()
        .await
    {
        Ok(resp) => {
            let uploads = resp
                .uploads()
                .iter()
                .filter_map(|upload| upload.key().map(|key| (key, upload.upload_id())))
                .take(10)
                .collect::<Vec<_>>();
            context.push(format!(
                "list_multipart_uploads truncated={} sample_uploads={uploads:?}",
                resp.is_truncated().unwrap_or(false)
            ));
        }
        Err(err) => context.push(format!(
            "list_multipart_uploads={}",
            describe_sdk_error(&err)
        )),
    }

    context.join("\n")
}

fn describe_sdk_error<E: ProvideErrorMetadata + std::fmt::Debug>(
    err: &aws_sdk_s3::error::SdkError<E>,
) -> String {
    let code = s3_error_code(err).unwrap_or("<unknown>");
    let message = err
        .as_service_error()
        .and_then(ProvideErrorMetadata::message)
        .unwrap_or("<no message>");
    format!("code={code} message={message:?} debug={err:?}")
}

pub async fn put_object_retrying_operation_aborted(
    client: &Client,
    bucket: &str,
    key: &str,
    body: Vec<u8>,
) -> aws_sdk_s3::operation::put_object::PutObjectOutput {
    retrying_operation_aborted("put object", || {
        let body = body.clone();
        async move {
            client
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(ByteStream::from(body))
                .send()
                .await
        }
    })
    .await
}

pub async fn wait_for_versioned_writes_visible(client: &Client, bucket: &str) {
    const RETRY_DELAY: Duration = Duration::from_millis(250);
    const READINESS_KEY: &str = "__argmin-versioning-readiness";

    let mut last_error = None;

    for attempt in 0..40 {
        let body = format!("versioning-ready-{attempt}");
        let put =
            put_object_retrying_operation_aborted(client, bucket, READINESS_KEY, body.into_bytes())
                .await;

        if put.version_id().filter(|id| *id != "null").is_none() {
            last_error = Some("PUT did not return a real version id".to_string());
            cleanup_versioning_readiness_key(client, bucket).await;
            tokio::time::sleep(RETRY_DELAY).await;
            continue;
        }

        match client
            .get_object()
            .bucket(bucket)
            .key(READINESS_KEY)
            .send()
            .await
        {
            Ok(response) => match response.body.collect().await {
                Ok(bytes) => {
                    let bytes = bytes.into_bytes();
                    if bytes.as_ref() == format!("versioning-ready-{attempt}").as_bytes() {
                        cleanup_versioning_readiness_key(client, bucket).await;
                        return;
                    }
                    last_error = Some(format!(
                        "readiness GET returned unexpected body {:?}",
                        String::from_utf8_lossy(bytes.as_ref())
                    ));
                }
                Err(error) => {
                    last_error = Some(format!("readiness GET body collection failed: {error:?}"));
                }
            },
            Err(error)
                if error
                    .as_service_error()
                    .and_then(ProvideErrorMetadata::code)
                    == Some("NoSuchKey") =>
            {
                last_error = Some("readiness GET returned NoSuchKey".to_string());
            }
            Err(error) => panic!("versioning readiness GET: {error:?}"),
        }

        cleanup_versioning_readiness_key(client, bucket).await;
        tokio::time::sleep(RETRY_DELAY).await;
    }

    panic!(
        "versioned writes did not become visible for {bucket}: {}",
        last_error.unwrap_or_else(|| "no attempts completed".to_string())
    );
}

pub async fn enable_bucket_versioning(client: &Client, bucket: &str) {
    client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send_retrying_operation_aborted("enable bucket versioning")
        .await
        .unwrap();
    wait_for_versioned_writes_visible(client, bucket).await;
}

async fn cleanup_versioning_readiness_key(client: &Client, bucket: &str) {
    let _ = client
        .delete_object()
        .bucket(bucket)
        .key("__argmin-versioning-readiness")
        .send_retrying_operation_aborted("delete versioning readiness object")
        .await;

    loop {
        let resp = client
            .list_object_versions()
            .bucket(bucket)
            .prefix("__argmin-versioning-readiness")
            .send_retrying_operation_aborted("list versioning readiness object versions")
            .await
            .unwrap();

        let mut objects = Vec::new();
        for version in resp.versions() {
            if version.key() == Some("__argmin-versioning-readiness") {
                objects.push(
                    ObjectIdentifier::builder()
                        .key("__argmin-versioning-readiness")
                        .set_version_id(version.version_id().map(str::to_string))
                        .build()
                        .unwrap(),
                );
            }
        }
        for marker in resp.delete_markers() {
            if marker.key() == Some("__argmin-versioning-readiness") {
                objects.push(
                    ObjectIdentifier::builder()
                        .key("__argmin-versioning-readiness")
                        .set_version_id(marker.version_id().map(str::to_string))
                        .build()
                        .unwrap(),
                );
            }
        }

        if objects.is_empty() {
            return;
        }

        let resp = delete_objects_retrying_operation_aborted(
            client,
            bucket,
            Delete::builder()
                .set_objects(Some(objects))
                .quiet(true)
                .build()
                .unwrap(),
        )
        .await;
        assert!(
            resp.errors().is_empty(),
            "delete versioning readiness object returned embedded errors: {:?}",
            resp.errors()
        );
    }
}

pub async fn delete_object_retrying_operation_aborted(
    client: &Client,
    bucket: &str,
    key: &str,
) -> Result<
    aws_sdk_s3::operation::delete_object::DeleteObjectOutput,
    aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::delete_object::DeleteObjectError>,
> {
    client
        .delete_object()
        .bucket(bucket)
        .key(key)
        .send_retrying_operation_aborted("delete object")
        .await
}

/// Assert that an S3 SDK error contains the expected error code string.
pub fn assert_s3_err_code<T, E: std::fmt::Debug>(
    result: &Result<T, aws_sdk_s3::error::SdkError<E>>,
    expected_code: &str,
) {
    match result {
        Ok(_) => panic!("expected error with code {}, got Ok", expected_code),
        Err(e) => {
            if expected_code == "AccessDenied"
                && e.raw_response().map(|r| r.status().as_u16()) == Some(403)
            {
                return;
            }
            let msg = format!("{:?}", e);
            assert!(
                msg.contains(expected_code),
                "expected error code '{}' in error: {}",
                expected_code,
                msg
            );
        }
    }
}

/// Extract the HTTP status code from an S3 SDK error.
///
/// Panics if the result is `Ok` or if the error has no raw HTTP response.
pub fn err_status<T, E: std::fmt::Debug>(
    result: &Result<T, aws_sdk_s3::error::SdkError<E>>,
) -> u16 {
    match result {
        Ok(_) => panic!("expected error, got Ok"),
        Err(sdk_err) => sdk_err
            .raw_response()
            .map(|r| r.status().as_u16())
            .unwrap_or_else(|| panic!("error has no raw HTTP response: {:?}", sdk_err)),
    }
}

/// Return true for SDK errors caused by the server closing an in-flight request body.
///
/// Some tests intentionally race a streaming request against a server-side state
/// transition that can reject the request after the client has started writing.
/// In that shape there may be no S3 error response for the SDK to expose.
pub fn is_sdk_stream_disconnect<E: std::fmt::Debug>(err: &aws_sdk_s3::error::SdkError<E>) -> bool {
    if err.raw_response().is_some() {
        return false;
    }
    let message = format!("{err:?}");
    message.contains("DispatchFailure")
        && (message.contains("BodyWrite")
            || message.contains("BrokenPipe")
            || message.contains("Broken pipe")
            || message.contains("Connection reset")
            || message.contains("IncompleteMessage"))
}

/// Return true for SDK errors caused by a streaming request being rejected
/// while the client is still writing, accepting either a pure dispatch
/// disconnect or a response whose status arrived before the error body failed.
pub fn is_sdk_stream_disconnect_or_status<E: std::fmt::Debug>(
    err: &aws_sdk_s3::error::SdkError<E>,
    expected_status: u16,
) -> bool {
    if is_sdk_stream_disconnect(err) {
        return true;
    }
    if err.raw_response().map(|r| r.status().as_u16()) != Some(expected_status) {
        return false;
    }
    let message = format!("{err:?}");
    message.contains("ResponseError")
        && (message.contains("hyper::Error(Body")
            || message.contains("connection error")
            || message.contains("IncompleteMessage")
            || message.contains("incomplete message"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;

    #[derive(Clone)]
    struct FakeRetryBuilder {
        calls: Arc<AtomicUsize>,
        first_error_code: &'static str,
    }

    impl SendRetryingOperationAborted for FakeRetryBuilder {
        type Output = ();
        type Error = aws_sdk_s3::operation::delete_object::DeleteObjectError;

        fn send_once(self) -> RetrySendFuture<Self::Output, Self::Error> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                if call == 0 {
                    let metadata = aws_smithy_types::error::ErrorMetadata::builder()
                        .code(self.first_error_code)
                        .build();
                    let response = hyper::http::Response::builder()
                        .status(409)
                        .body(aws_smithy_types::body::SdkBody::empty())
                        .expect("build fake retry response")
                        .try_into()
                        .expect("convert fake retry response");
                    Err(aws_sdk_s3::error::SdkError::service_error(
                        aws_sdk_s3::operation::delete_object::DeleteObjectError::generic(metadata),
                        response,
                    ))
                } else {
                    Ok(())
                }
            })
        }
    }

    #[test]
    fn exact_operation_aborted_retry_scope_does_not_include_slow_down() {
        let exact = OperationContentionRetryScope::OperationAbortedOnly;
        assert!(exact.includes(Some("OperationAborted")));
        assert!(!exact.includes(Some("SlowDown")));
        assert!(!exact.includes(Some("InternalError")));
        assert!(!exact.includes(None));

        let contention = OperationContentionRetryScope::OperationAbortedOrSlowDown;
        assert!(contention.includes(Some("OperationAborted")));
        assert!(contention.includes(Some("SlowDown")));
    }

    #[test]
    fn operation_contention_retry_delay_is_capped_and_rejects_expired_deadline() {
        let now = std::time::Instant::now();
        assert_eq!(
            operation_contention_retry_delay(now + Duration::from_millis(250), now),
            Some(Duration::from_millis(100))
        );
        assert_eq!(
            operation_contention_retry_delay(now + Duration::from_millis(25), now),
            Some(Duration::from_millis(25))
        );
        assert_eq!(operation_contention_retry_delay(now, now), None);
        assert_eq!(
            operation_contention_retry_delay(now, now + Duration::from_millis(1)),
            None
        );
    }

    #[tokio::test]
    async fn exact_operation_aborted_retry_returns_slow_down_without_retrying() {
        let calls = Arc::new(AtomicUsize::new(0));
        let result = send_with_operation_contention_retry_until(
            FakeRetryBuilder {
                calls: Arc::clone(&calls),
                first_error_code: "SlowDown",
            },
            OperationContentionRetryScope::OperationAbortedOnly,
            std::time::Instant::now() + Duration::from_secs(1),
        )
        .await;

        assert_eq!(
            result.as_ref().err().and_then(s3_error_code),
            Some("SlowDown")
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn operation_contention_retry_does_not_send_after_deadline() {
        let calls = Arc::new(AtomicUsize::new(0));
        let result = send_with_operation_contention_retry_until(
            FakeRetryBuilder {
                calls: Arc::clone(&calls),
                first_error_code: "OperationAborted",
            },
            OperationContentionRetryScope::OperationAbortedOnly,
            std::time::Instant::now(),
        )
        .await;

        assert_eq!(
            result.as_ref().err().and_then(s3_error_code),
            Some("OperationAborted")
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn request_signing_combines_duplicate_header_values_in_wire_order() {
        let mut headers = vec![
            ("x-two".to_string(), "last".to_string()),
            ("x-one".to_string(), " first   value ".to_string()),
            ("x-one".to_string(), "second".to_string()),
        ];

        let (signed_headers, canonical_headers) = canonicalize_request_headers(&mut headers);

        assert_eq!(signed_headers, "x-one;x-two");
        assert_eq!(canonical_headers, "x-one:first value,second\nx-two:last\n");
        assert_eq!(
            headers,
            [
                ("x-one".to_string(), " first   value ".to_string()),
                ("x-one".to_string(), "second".to_string()),
                ("x-two".to_string(), "last".to_string()),
            ]
        );
    }

    #[test]
    fn copy_source_with_version_uses_strict_percent_encoding_for_version_id() {
        assert_eq!(
            copy_source_with_version("bucket", "key with space", "version with+plus"),
            "bucket/key%20with%20space?versionId=version%20with%2Bplus"
        );
    }

    #[test]
    fn presign_url_for_service_uses_requested_credential_scope() {
        let request = presign_url_for_service_with_credentials(
            "GET",
            "https://example.com/object",
            Duration::from_secs(900),
            Vec::<(&str, &str)>::new(),
            None,
            "sts",
            SignedRequestCredentials {
                access_key: "ACCESSKEY",
                secret_key: "secret",
                region: "test-region-1",
                tls_ca_pem: None,
            },
        );
        let parsed = url::Url::parse(request.uri()).expect("parse presigned URL");
        let credential = parsed
            .query_pairs()
            .find_map(|(name, value)| (name == "X-Amz-Credential").then_some(value.into_owned()))
            .expect("presigned URL has credential");

        assert!(credential.starts_with("ACCESSKEY/"));
        assert!(credential.ends_with("/test-region-1/sts/aws4_request"));
    }

    #[test]
    fn presign_url_for_service_preserves_base_query_wire_order() {
        let credentials = SignedRequestCredentials {
            access_key: "ACCESSKEY",
            secret_key: "secret",
            region: "test-region-1",
            tls_ca_pem: None,
        };
        let presign = |query: &str| {
            presign_url_for_service_with_credentials(
                "GET",
                &format!("https://example.com/object?{query}"),
                Duration::from_secs(900),
                Vec::<(&str, &str)>::new(),
                None,
                "sts",
                credentials,
            )
        };
        let first = presign("X-Amz-Security-Token=z&X-Amz-Security-Token=a");
        let reversed = presign("X-Amz-Security-Token=a&X-Amz-Security-Token=z");
        let token_values = |request: &PresignedRequest| {
            url::Url::parse(request.uri())
                .expect("parse presigned URL")
                .query_pairs()
                .filter_map(|(name, value)| {
                    (name == "X-Amz-Security-Token").then_some(value.into_owned())
                })
                .collect::<Vec<_>>()
        };

        assert_eq!(token_values(&first), ["z", "a"]);
        assert_eq!(token_values(&reversed), ["a", "z"]);
        assert_ne!(first.uri(), reversed.uri());
    }
}
