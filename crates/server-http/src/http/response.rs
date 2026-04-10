//! Build HTTP responses for S3 operations.

use crate::coordinator::{
    BucketSummary, CompleteMultipartUploadResult, CopyObjectResult, DeleteObjectResult,
    DeleteObjectsResult, GetBucketAclResult, GetObjectAclResult, GetObjectPartResult,
    GetObjectRangeResult, GetObjectResult, HeadObjectPartResult, HeadObjectResult,
    LifecycleAbortHeaders, LifecycleExpirationHeader, ListObjectVersionsResult, ListObjectsResult,
    ListPartsResult, PutObjectResult, ReadHandle,
};
use crate::error::ServerError;
use auth::canonical::uri_encode;
use checksum::{ChecksumAlgorithm, ChecksumType, RawChecksum};
use s3_types::{
    bucket_location_constraint, BucketObjectLockConfig, BucketVersioningState, CanonicalUserId,
    LegalHoldStatus, ObjectLockState, ObjectRetention, VersionId,
};
use server_core::sse::{SseCustomerResponseHeaders, SSE_CUSTOMER_ALGORITHM};
use server_core::system_metadata::SystemMetadata;
use storage::{EffectiveBucketEncryptionConfig, ManagedEncryptionAlgorithm};

use super::xml;

/// Format a `version_id` for S3 API responses.
/// Null version is displayed as "null".
/// Versioned IDs are displayed as decimal strings.
#[must_use]
pub fn format_version_id(version_id: VersionId) -> String {
    version_id.to_string()
}

/// RFC 2047 Q-encoding for non-ASCII header values.
///
/// AWS S3 returns non-ASCII user metadata (x-amz-meta-*) encoded as RFC 2047
/// encoded-words. This is a legacy HTTP convention (see RFC 9110 §5.5, RFC 2047)
/// that AWS follows for metadata round-tripping. Values are encoded as
/// `=?UTF-8?Q?...?=` where non-printable-ASCII bytes become `=XX` hex pairs
/// and spaces become underscores.
///
/// Reference: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html>
fn rfc2047_encode(value: &str) -> String {
    let mut encoded = String::from("=?UTF-8?Q?");
    for byte in value.bytes() {
        match byte {
            // Printable ASCII (except =, ?, _) pass through
            b'!'..=b'<' | b'>'..=b'>' | b'@'..=b'^' | b'`'..=b'~' => {
                encoded.push(byte as char);
            }
            // Space → underscore (RFC 2047 convention)
            b' ' => encoded.push('_'),
            // Everything else (non-ASCII, control chars, =, ?, _) → =XX
            _ => {
                encoded.push('=');
                encoded.push_str(&format!("{byte:02X}"));
            }
        }
    }
    encoded.push_str("?=");
    encoded
}

fn success_action_redirect_location(
    redirect_url: &str,
    bucket: &str,
    key: &str,
    etag: &str,
) -> Option<String> {
    let (base, fragment) = redirect_url
        .split_once('#')
        .map_or((redirect_url, ""), |(base, fragment)| (base, fragment));

    let mut location =
        String::with_capacity(redirect_url.len() + bucket.len() + key.len() + etag.len() + 32);
    location.push_str(base);

    if base.contains('?') {
        if !base.ends_with('?') && !base.ends_with('&') {
            location.push('&');
        }
    } else {
        location.push('?');
    }

    location.push_str("bucket=");
    location.push_str(&uri_encode(bucket));
    location.push_str("&key=");
    location.push_str(&uri_encode(key));
    location.push_str("&etag=");
    location.push_str(&uri_encode(etag));

    if !fragment.is_empty() {
        location.push('#');
        location.push_str(fragment);
    }

    http::header::HeaderValue::from_str(&location)
        .ok()
        .map(|_| location)
}

/// An HTTP response to send back.
pub struct S3Response {
    pub status_code: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub stream: Option<ReadHandle>,
}

pub struct CreateMultipartUploadResponseContext<'a> {
    pub managed_encryption: Option<ManagedEncryptionAlgorithm>,
    pub checksum_algorithm: Option<ChecksumAlgorithm>,
    pub checksum_type: Option<ChecksumType>,
    pub lifecycle_abort: Option<&'a LifecycleAbortHeaders>,
    pub sse_customer: Option<&'a SseCustomerResponseHeaders>,
}

impl S3Response {
    const TEST_REQUEST_ID: &'static str = "request-id";
    const TEST_HOST_ID: &'static str = "host-id";

    fn new(status_code: u16) -> Self {
        Self {
            status_code,
            headers: Vec::new(),
            body: Vec::new(),
            stream: None,
        }
    }

    fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_string(), value.to_string()));
        self
    }

    /// Set a metadata header, RFC 2047 encoding the value if it contains
    /// non-ASCII bytes (matching AWS S3 behavior).
    fn meta_header(self, name: &str, value: &str) -> Self {
        if value.is_ascii() {
            self.header(name, value)
        } else {
            self.header(name, &rfc2047_encode(value))
        }
    }

    fn xml_body(mut self, xml: String) -> Self {
        self.body = xml.into_bytes();
        self.stream = None;
        self.headers
            .push(("Content-Type".to_string(), "application/xml".to_string()));
        self.headers
            .push(("Content-Length".to_string(), self.body.len().to_string()));
        self
    }

    fn fixed_body(mut self, body: Vec<u8>) -> Self {
        self.body = body;
        self.stream = None;
        self.headers
            .push(("Content-Length".to_string(), self.body.len().to_string()));
        self
    }

    fn fixed_xml_body_no_content_type(self, xml: String) -> Self {
        self.fixed_body(xml.into_bytes())
    }

    fn chunked_xml_body_no_content_type(self, xml: String) -> Self {
        self.chunked_body(xml.into_bytes())
    }

    fn chunked_body(mut self, body: Vec<u8>) -> Self {
        self.body = Vec::new();
        self.stream = Some(ReadHandle::from_buffered_bytes(body));
        self
    }

    fn chunked_xml_body(self, xml: String) -> Self {
        self.header("Content-Type", "application/xml")
            .chunked_body(xml.into_bytes())
    }

    fn json_body(mut self, json: String) -> Self {
        self.body = json.into_bytes();
        self.stream = None;
        self.headers
            .push(("Content-Type".to_string(), "application/json".to_string()));
        self.headers
            .push(("Content-Length".to_string(), self.body.len().to_string()));
        self
    }

    fn streaming_body(mut self, body: ReadHandle, content_length: u64) -> Self {
        self.headers
            .push(("Content-Length".to_string(), content_length.to_string()));
        self.body = Vec::new();
        self.stream = Some(body);
        self
    }

    fn apply_sse_customer_headers(
        mut self,
        sse_customer: Option<&SseCustomerResponseHeaders>,
    ) -> Self {
        let Some(sse_customer) = sse_customer else {
            return self;
        };
        self = self.header(
            "x-amz-server-side-encryption-customer-algorithm",
            SSE_CUSTOMER_ALGORITHM,
        );
        self.header(
            "x-amz-server-side-encryption-customer-key-md5",
            &sse_customer.key_md5_b64,
        )
    }

    fn apply_managed_encryption_headers(
        self,
        managed_encryption: Option<ManagedEncryptionAlgorithm>,
    ) -> Self {
        let Some(managed_encryption) = managed_encryption else {
            return self;
        };
        self.header("x-amz-server-side-encryption", managed_encryption.as_str())
    }

    fn apply_system_metadata_headers(mut self, metadata: &SystemMetadata) -> Self {
        if let Some(content_type) = metadata.content_type() {
            self = self.header("Content-Type", content_type);
        } else {
            self = self.header("Content-Type", "application/octet-stream");
        }
        if let Some(content_encoding) = metadata.content_encoding() {
            self = self.header("Content-Encoding", content_encoding);
        }
        if let Some(cache_control) = metadata.cache_control() {
            self = self.header("Cache-Control", cache_control);
        }
        if let Some(content_disposition) = metadata.content_disposition() {
            self = self.header("Content-Disposition", content_disposition);
        }
        if let Some(content_language) = metadata.content_language() {
            self = self.header("Content-Language", content_language);
        }
        if let Some(expires) = metadata.expires() {
            self = self.header("Expires", expires);
        }
        self
    }

    fn apply_user_metadata_headers(
        mut self,
        metadata: &crate::metadata_blob::MetadataBlob,
    ) -> Self {
        for entry in metadata.iter() {
            if entry.key.starts_with("x-amz-meta-") {
                self = self.meta_header(&entry.key, &entry.value);
            }
        }
        self
    }

    fn apply_object_lock_headers(mut self, object_lock: ObjectLockState) -> Self {
        if let Some(retention) = object_lock.retention {
            self = self.header("x-amz-object-lock-mode", retention.mode.as_str());
            self = self.header(
                "x-amz-object-lock-retain-until-date",
                &format_object_lock_header_timestamp(retention.retain_until_unix_seconds),
            );
        }
        if let Some(legal_hold) = object_lock.legal_hold.as_legal_hold_status() {
            self = self.header("x-amz-object-lock-legal-hold", legal_hold.as_str());
        }
        self
    }

    fn apply_checksum_mode_headers(mut self, metadata: &SystemMetadata) -> Self {
        for (name, value) in metadata.checksum_header_pairs() {
            self = self.header(name, value);
        }
        self
    }

    fn apply_lifecycle_expiration_header(
        self,
        expiration: Option<&LifecycleExpirationHeader>,
    ) -> Self {
        let Some(expiration) = expiration else {
            return self;
        };
        let value = match &expiration.rule_id {
            Some(rule_id) => format!(
                "expiry-date=\"{}\", rule-id=\"{}\"",
                format_http_date(expiration.expiry_time_millis),
                uri_encode(rule_id)
            ),
            None => format!(
                "expiry-date=\"{}\"",
                format_http_date(expiration.expiry_time_millis)
            ),
        };
        self.header("x-amz-expiration", &value)
    }

    fn apply_lifecycle_abort_headers(mut self, abort: Option<&LifecycleAbortHeaders>) -> Self {
        let Some(abort) = abort else {
            return self;
        };
        self = self.header(
            "x-amz-abort-date",
            &format_http_date(abort.abort_time_millis),
        );
        if let Some(rule_id) = &abort.rule_id {
            self = self.header("x-amz-abort-rule-id", &uri_encode(rule_id));
        }
        self
    }

    /// Build a response for a successful `PutObject`.
    #[must_use]
    pub fn put_object(result: &PutObjectResult) -> Self {
        let mut resp = Self::new(200).header("ETag", &result.etag);
        if result.version_id.is_versioned() {
            let vid = format_version_id(result.version_id);
            resp = resp.header("x-amz-version-id", &vid);
        }
        resp.apply_checksum_mode_headers(&result.system_metadata)
            .apply_lifecycle_expiration_header(result.lifecycle_expiration.as_ref())
            .apply_managed_encryption_headers(result.managed_encryption)
    }

    /// Build a response for a successful POST Object.
    ///
    /// `success_status`: one of 200, 201, 204 (default).
    /// For 201, an XML body with bucket/key/etag is returned. If
    /// `success_redirect` is present and valid, return a 303 redirect instead.
    #[must_use]
    pub fn post_object(
        result: &PutObjectResult,
        bucket: &str,
        key: &str,
        success_status: u16,
        success_redirect: Option<&str>,
        location: Option<&str>,
    ) -> Self {
        let mut resp = if let Some(location) = success_redirect.and_then(|redirect_url| {
            success_action_redirect_location(redirect_url, bucket, key, &result.etag)
        }) {
            Self::new(303).header("Location", &location)
        } else {
            let status = match success_status {
                200 | 201 => success_status,
                _ => 204,
            };
            if status == 201 {
                let body = xml::post_response_xml(bucket, key, &result.etag);
                Self::new(201).xml_body(body)
            } else {
                Self::new(status)
            }
        };
        if success_redirect.is_none() {
            if let Some(location) = location {
                resp = resp.header("Location", location);
            }
            resp = resp.apply_checksum_mode_headers(&result.system_metadata);
            if let Some(checksum_type) = result.system_metadata.checksum_type() {
                resp = resp.header("x-amz-checksum-type", checksum_type.as_str());
            }
        }
        resp = resp.header("ETag", &result.etag);
        if result.version_id.is_versioned() {
            let vid = format_version_id(result.version_id);
            resp = resp.header("x-amz-version-id", &vid);
        }
        resp.apply_lifecycle_expiration_header(result.lifecycle_expiration.as_ref())
            .apply_managed_encryption_headers(result.managed_encryption)
    }

    /// Build a response for a successful `CopyObject`.
    #[must_use]
    pub fn copy_object(result: &CopyObjectResult) -> Self {
        let body = xml::copy_object_result_xml(
            &result.etag,
            result.last_modified,
            &result.system_metadata,
        );
        let mut resp = Self::new(200).xml_body(body);
        if result.version_id.is_versioned() {
            let vid = format_version_id(result.version_id);
            resp.headers.push(("x-amz-version-id".to_string(), vid));
        }
        resp.apply_lifecycle_expiration_header(result.lifecycle_expiration.as_ref())
            .apply_managed_encryption_headers(result.managed_encryption)
            .apply_sse_customer_headers(result.sse_customer.as_ref())
    }

    /// Build a response for a successful `GetObject`.
    /// If `checksum_mode` is `Some("ENABLED")`, include stored checksum headers.
    #[must_use]
    pub fn get_object(result: GetObjectResult, checksum_mode: Option<&str>) -> Self {
        let mut resp = Self::new(200)
            .header("ETag", &result.etag)
            .header("Last-Modified", &format_http_date(result.last_modified))
            .header("Accept-Ranges", "bytes");
        if result.version_id.is_versioned() {
            let vid = format_version_id(result.version_id);
            resp = resp.header("x-amz-version-id", &vid);
        }
        resp = resp
            .apply_system_metadata_headers(&result.system_metadata)
            .apply_user_metadata_headers(&result.metadata)
            .apply_object_lock_headers(result.object_lock);

        // Checksum headers (only when ChecksumMode=ENABLED)
        if checksum_mode.is_some_and(|m| m.eq_ignore_ascii_case("ENABLED")) {
            resp = resp.apply_checksum_mode_headers(&result.system_metadata);
        }

        resp.apply_lifecycle_expiration_header(result.lifecycle_expiration.as_ref())
            .apply_managed_encryption_headers(result.managed_encryption)
            .apply_sse_customer_headers(result.sse_customer.as_ref())
            .streaming_body(result.body, result.size)
    }

    #[cfg(test)]
    pub(crate) fn into_test_body_bytes(self) -> Result<Vec<u8>, ServerError> {
        match self.stream {
            Some(mut stream) => {
                let mut out = Vec::new();
                while let Some(chunk) =
                    stream.next_chunk(crate::coordinator::INTERNAL_SEGMENT_SIZE)?
                {
                    out.extend_from_slice(&chunk);
                }
                Ok(out)
            }
            None => Ok(self.body),
        }
    }

    /// Build a response for a successful `HeadObject`.
    /// If `checksum_mode` is `Some("ENABLED")`, include stored checksum headers.
    #[must_use]
    pub fn head_object(result: &HeadObjectResult, checksum_mode: Option<&str>) -> Self {
        let mut resp = Self::new(200)
            .header("ETag", &result.etag)
            .header("Content-Length", &result.size.to_string())
            .header("Last-Modified", &format_http_date(result.last_modified))
            .header("Accept-Ranges", "bytes");
        if result.version_id.is_versioned() {
            let vid = format_version_id(result.version_id);
            resp = resp.header("x-amz-version-id", &vid);
        }
        resp = resp
            .apply_system_metadata_headers(&result.system_metadata)
            .apply_user_metadata_headers(&result.metadata)
            .apply_object_lock_headers(result.object_lock);

        // Checksum headers (only when ChecksumMode=ENABLED)
        if checksum_mode.is_some_and(|m| m.eq_ignore_ascii_case("ENABLED")) {
            resp = resp.apply_checksum_mode_headers(&result.system_metadata);
        }

        resp.apply_lifecycle_expiration_header(result.lifecycle_expiration.as_ref())
            .apply_managed_encryption_headers(result.managed_encryption)
            .apply_sse_customer_headers(result.sse_customer.as_ref())
    }

    /// Build a response for `HeadObject` with partNumber.
    #[must_use]
    pub fn head_object_part(result: &HeadObjectPartResult) -> Self {
        let mut resp = Self::new(206)
            .header("ETag", &result.etag)
            .header("Content-Length", &result.part_size.to_string())
            .header("Last-Modified", &format_http_date(result.last_modified))
            .header("Accept-Ranges", "bytes")
            .header("x-amz-mp-parts-count", &result.parts_count.to_string());
        if result.part_size != 0 {
            let content_range = format!(
                "bytes {}-{}/{}",
                result.part_start, result.part_end, result.total_size
            );
            resp = resp.header("Content-Range", &content_range);
        }
        if result.version_id.is_versioned() {
            let vid = format_version_id(result.version_id);
            resp = resp.header("x-amz-version-id", &vid);
        }
        resp = resp
            .apply_system_metadata_headers(&result.system_metadata)
            .apply_user_metadata_headers(&result.metadata)
            .apply_object_lock_headers(result.object_lock);

        // Per-part checksum (always emitted for part-level requests)
        if let Some(ref cksum) = result.checksum {
            use base64::Engine;
            let b64 = base64::engine::general_purpose::STANDARD.encode(cksum.bytes());
            resp = resp.header(cksum.algorithm().header_name(), &b64);
        }
        if let Some(checksum_type) = result.system_metadata.checksum_type() {
            resp = resp.header("x-amz-checksum-type", checksum_type.as_str());
        }

        resp.apply_lifecycle_expiration_header(result.lifecycle_expiration.as_ref())
            .apply_managed_encryption_headers(result.managed_encryption)
    }

    /// Build a response for a successful range `GetObject` (206 Partial Content).
    #[must_use]
    pub fn get_object_range(result: GetObjectRangeResult) -> Self {
        let content_range = format!(
            "bytes {}-{}/{}",
            result.range_start, result.range_end, result.size
        );
        let mut resp = Self::new(206)
            .header("ETag", &result.etag)
            .header("Last-Modified", &format_http_date(result.last_modified))
            .header("Accept-Ranges", "bytes")
            .header("Content-Range", &content_range);
        if result.version_id.is_versioned() {
            let vid = format_version_id(result.version_id);
            resp = resp.header("x-amz-version-id", &vid);
        }
        resp = resp
            .apply_system_metadata_headers(&result.system_metadata)
            .apply_user_metadata_headers(&result.metadata)
            .apply_object_lock_headers(result.object_lock);

        resp.apply_lifecycle_expiration_header(result.lifecycle_expiration.as_ref())
            .apply_managed_encryption_headers(result.managed_encryption)
            .apply_sse_customer_headers(result.sse_customer.as_ref())
            .streaming_body(result.body, result.range_end - result.range_start + 1)
    }

    /// Build a response for a part-level `GetObject` (206 Partial Content).
    /// Per-part checksum and checksum-type are always emitted (Ceph/AWS
    /// return them without requiring ChecksumMode=ENABLED on part GETs).
    #[must_use]
    pub fn get_object_part(result: GetObjectPartResult) -> Self {
        let mut resp = Self::new(206)
            .header("ETag", &result.etag)
            .header("Last-Modified", &format_http_date(result.last_modified))
            .header("Accept-Ranges", "bytes")
            .header("x-amz-mp-parts-count", &result.parts_count.to_string());
        // Only emit Content-Range for non-empty parts; a zero-byte part has
        // no valid byte range to express.
        if result.part_size != 0 {
            let content_range = format!(
                "bytes {}-{}/{}",
                result.part_start, result.part_end, result.size
            );
            resp = resp.header("Content-Range", &content_range);
        }
        if result.version_id.is_versioned() {
            let vid = format_version_id(result.version_id);
            resp = resp.header("x-amz-version-id", &vid);
        }
        resp = resp
            .apply_system_metadata_headers(&result.system_metadata)
            .apply_user_metadata_headers(&result.metadata)
            .apply_object_lock_headers(result.object_lock);

        // Per-part checksum (always emitted for part-level GETs)
        if let Some(ref cksum) = result.checksum {
            use base64::Engine;
            let b64 = base64::engine::general_purpose::STANDARD.encode(cksum.bytes());
            resp = resp.header(cksum.algorithm().header_name(), &b64);
        }
        // Checksum type (e.g. COMPOSITE, FULL_OBJECT)
        if let Some(checksum_type) = result.system_metadata.checksum_type() {
            resp = resp.header("x-amz-checksum-type", checksum_type.as_str());
        }

        resp.apply_lifecycle_expiration_header(result.lifecycle_expiration.as_ref())
            .apply_managed_encryption_headers(result.managed_encryption)
            .apply_sse_customer_headers(result.sse_customer.as_ref())
            .streaming_body(result.body, result.part_size)
    }

    /// Build a 416 Range Not Satisfiable response.
    #[must_use]
    pub fn range_not_satisfiable(_total_size: u64) -> Self {
        let body = xml::error_xml(
            "InvalidRange",
            "The requested range is not satisfiable",
            "",
            "request-id",
        );
        Self::new(416).chunked_xml_body(body)
    }

    /// Build a response for `DeleteObject` (204 No Content).
    #[must_use]
    pub fn delete_object(result: &DeleteObjectResult) -> Self {
        let mut resp = Self::new(204);
        if result.version_id.is_versioned() {
            let vid = format_version_id(result.version_id);
            resp = resp.header("x-amz-version-id", &vid);
        }
        if result.delete_marker {
            resp = resp.header("x-amz-delete-marker", "true");
        }
        resp
    }

    /// Build a response for `HeadObject` against a delete-marker version.
    #[must_use]
    pub fn head_delete_marker_method_not_allowed(
        version_id: VersionId,
        last_modified: u64,
    ) -> Self {
        Self::new(405)
            .header("Allow", "DELETE")
            .header("x-amz-delete-marker", "true")
            .header("x-amz-version-id", &format_version_id(version_id))
            .header("Last-Modified", &format_http_date(last_modified))
    }

    /// Build a response for `CreateBucket`.
    #[must_use]
    pub fn create_bucket(location: &str) -> Self {
        Self::new(200).header("Location", &format!("/{location}"))
    }

    /// Build a response for `DeleteBucket`.
    #[must_use]
    pub fn delete_bucket() -> Self {
        Self::new(204)
    }

    /// Build a response for `HeadBucket`.
    #[must_use]
    pub fn head_bucket(info: &BucketSummary, region: &str) -> Self {
        Self::new(200)
            .header("Content-Type", "application/xml")
            .header("x-amz-access-point-alias", "false")
            .header("x-amz-bucket-arn", &format!("arn:aws:s3:::{}", info.name))
            .header("x-amz-bucket-region", region)
    }

    /// Build a response for `GetBucketLocation`.
    #[must_use]
    pub fn get_bucket_location(region: &str) -> Self {
        let body = xml::get_bucket_location_xml(bucket_location_constraint(region));
        Self::new(200).chunked_xml_body(body)
    }

    /// Build a response for `PutBucketVersioning`.
    #[must_use]
    pub fn put_bucket_versioning() -> Self {
        Self::new(200)
    }

    /// Build a response for `GetBucketVersioning`.
    #[must_use]
    pub fn get_bucket_versioning(state: BucketVersioningState) -> Self {
        let body = xml::get_bucket_versioning_xml(state);
        Self::new(200).chunked_xml_body_no_content_type(body)
    }

    /// Build a response for `PutObjectLockConfiguration`.
    #[must_use]
    pub fn put_bucket_object_lock_configuration() -> Self {
        Self::new(200)
    }

    /// Build a response for `GetObjectLockConfiguration`.
    #[must_use]
    pub fn get_bucket_object_lock_configuration(config: BucketObjectLockConfig) -> Self {
        let body = xml::get_bucket_object_lock_configuration_xml(config);
        Self::new(200).chunked_xml_body_no_content_type(body)
    }

    /// Build a response for `PutObjectRetention`.
    #[must_use]
    pub fn put_object_retention() -> Self {
        Self::new(200)
    }

    /// Build a response for `GetObjectRetention`.
    #[must_use]
    pub fn get_object_retention(retention: Option<ObjectRetention>) -> Self {
        let body = xml::get_object_retention_xml(retention);
        Self::new(200).chunked_xml_body_no_content_type(body)
    }

    /// Build a response for `PutObjectLegalHold`.
    #[must_use]
    pub fn put_object_legal_hold() -> Self {
        Self::new(200)
    }

    /// Build a response for `GetObjectLegalHold`.
    #[must_use]
    pub fn get_object_legal_hold(status: Option<LegalHoldStatus>) -> Self {
        let body = xml::get_object_legal_hold_xml(status);
        Self::new(200).chunked_xml_body_no_content_type(body)
    }

    /// Build a response for `PutBucketEncryption`.
    #[must_use]
    pub fn put_bucket_encryption() -> Self {
        Self::new(200)
    }

    /// Build a response for `DeleteBucketEncryption`.
    #[must_use]
    pub fn delete_bucket_encryption() -> Self {
        Self::new(204)
    }

    /// Build a response for `GetBucketEncryption`.
    #[must_use]
    pub fn get_bucket_encryption(config: EffectiveBucketEncryptionConfig) -> Self {
        let body = xml::get_bucket_encryption_xml(config);
        Self::new(200).chunked_xml_body_no_content_type(body)
    }

    /// Build a response for `ListBuckets`.
    #[must_use]
    pub fn list_buckets(
        buckets: &[BucketSummary],
        owner_display_name: &str,
        owner_canonical_id: &CanonicalUserId,
    ) -> Self {
        let body = xml::list_buckets_xml(buckets, owner_display_name, owner_canonical_id);
        Self::new(200).xml_body(body)
    }

    /// Build a response for `ListObjectsV2`.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn list_objects_v2(
        bucket: &str,
        region: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        encoding_type: Option<&str>,
        continuation_token: Option<&str>,
        start_after: Option<&str>,
        fetch_owner: bool,
        max_keys: u32,
        result: &ListObjectsResult,
    ) -> Self {
        let body = xml::list_objects_v2_xml(
            bucket,
            prefix,
            delimiter,
            encoding_type,
            continuation_token,
            start_after,
            fetch_owner,
            max_keys,
            result,
        );
        Self::new(200)
            .header("x-amz-bucket-region", region)
            .chunked_xml_body(body)
    }

    /// Build a response for `ListObjects` v1.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn list_objects_v1(
        bucket: &str,
        region: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        marker: Option<&str>,
        encoding_type: Option<&str>,
        max_keys: u32,
        result: &ListObjectsResult,
    ) -> Self {
        let body = xml::list_objects_v1_xml(
            bucket,
            prefix,
            delimiter,
            marker,
            encoding_type,
            max_keys,
            result,
        );
        Self::new(200)
            .header("x-amz-bucket-region", region)
            .chunked_xml_body(body)
    }

    /// Build a response for `DeleteObjects` (batch delete).
    #[must_use]
    pub fn delete_objects(result: &DeleteObjectsResult, quiet: bool) -> Self {
        let body = xml::delete_objects_result_xml(&result.deleted, &result.errors, quiet);
        Self::new(200).chunked_xml_body(body)
    }

    /// Build a response for `ListObjectVersions`.
    #[must_use]
    pub fn list_object_versions(
        bucket: &str,
        prefix: Option<&str>,
        key_marker: Option<&str>,
        encoding_type: Option<&str>,
        max_keys: u32,
        result: &ListObjectVersionsResult,
    ) -> Self {
        let body = xml::list_object_versions_xml(
            bucket,
            prefix,
            key_marker,
            encoding_type,
            max_keys,
            result,
        );
        Self::new(200).chunked_xml_body(body)
    }

    /// Build a 304 Not Modified response with `ETag` and Last-Modified headers, no body.
    #[must_use]
    pub fn not_modified(etag: &str, last_modified: u64) -> Self {
        Self::new(304)
            .header("ETag", etag)
            .header("Last-Modified", &format_http_date(last_modified))
    }

    /// Build a 412 Precondition Failed response with XML error body.
    #[must_use]
    pub fn precondition_failed() -> Self {
        let body = xml::error_xml(
            "PreconditionFailed",
            "At least one of the pre-conditions you specified did not hold",
            "",
            "request-id",
        );
        Self::new(412).chunked_xml_body(body)
    }

    /// Build a response for `PutBucketCors` (200 OK, no body).
    #[must_use]
    pub fn put_bucket_cors() -> Self {
        Self::new(200)
    }

    /// Build a response for `GetBucketCors` (200 OK, XML body).
    #[must_use]
    pub fn get_bucket_cors(config_xml: &str) -> Self {
        Self::new(200).chunked_xml_body_no_content_type(config_xml.to_string())
    }

    /// Build a response for `DeleteBucketCors` (204 No Content).
    #[must_use]
    pub fn delete_bucket_cors() -> Self {
        Self::new(204)
    }

    /// Build a response for `PutBucketTagging` (200 OK, no body).
    #[must_use]
    pub fn put_bucket_tagging() -> Self {
        Self::new(200)
    }

    /// Build a response for `GetBucketTagging` (200 OK, XML body).
    #[must_use]
    pub fn get_bucket_tagging(xml: &str) -> Self {
        Self::new(200).chunked_xml_body_no_content_type(xml.to_string())
    }

    /// Build a response for `DeleteBucketTagging` (204 No Content).
    #[must_use]
    pub fn delete_bucket_tagging() -> Self {
        Self::new(204)
    }

    /// Build a response for `PutBucketLifecycleConfiguration` (200 OK, no body).
    #[must_use]
    pub fn put_bucket_lifecycle() -> Self {
        Self::new(200)
    }

    /// Build a response for `GetBucketLifecycleConfiguration` (200 OK, XML body).
    #[must_use]
    pub fn get_bucket_lifecycle(xml: &str) -> Self {
        Self::new(200)
            .header(
                "x-amz-transition-default-minimum-object-size",
                "all_storage_classes_128K",
            )
            .fixed_xml_body_no_content_type(xml.to_string())
    }

    /// Build a response for `DeleteBucketLifecycle` (204 No Content).
    #[must_use]
    pub fn delete_bucket_lifecycle() -> Self {
        Self::new(204)
    }

    /// Build a response for `PutObjectTagging` (200 OK, no body).
    #[must_use]
    pub fn put_object_tagging() -> Self {
        Self::new(200)
    }

    /// Build a response for `GetObjectTagging` (200 OK, XML body).
    #[must_use]
    pub fn get_object_tagging(xml: &str) -> Self {
        Self::new(200).chunked_xml_body_no_content_type(xml.to_string())
    }

    /// Build a response for `DeleteObjectTagging` (204 No Content).
    #[must_use]
    pub fn delete_object_tagging() -> Self {
        Self::new(204)
    }

    /// Build a response for `PutBucketPublicAccessBlock` (200 OK, no body).
    #[must_use]
    pub fn put_bucket_public_access_block() -> Self {
        Self::new(200)
    }

    /// Build a response for `GetBucketPublicAccessBlock` (200 OK, XML body).
    #[must_use]
    pub fn get_bucket_public_access_block(config_xml: &str) -> Self {
        Self::new(200).chunked_xml_body_no_content_type(config_xml.to_string())
    }

    /// Build a response for `DeleteBucketPublicAccessBlock` (204 No Content).
    #[must_use]
    pub fn delete_bucket_public_access_block() -> Self {
        Self::new(204)
    }

    /// Build a response for `PutBucketOwnershipControls` (200 OK, no body).
    #[must_use]
    pub fn put_bucket_ownership_controls() -> Self {
        Self::new(200)
    }

    /// Build a response for `GetBucketOwnershipControls` (200 OK, XML body).
    #[must_use]
    pub fn get_bucket_ownership_controls(config_xml: &str) -> Self {
        Self::new(200).fixed_xml_body_no_content_type(config_xml.to_string())
    }

    /// Build a response for `DeleteBucketOwnershipControls` (204 No Content).
    #[must_use]
    pub fn delete_bucket_ownership_controls() -> Self {
        Self::new(204)
    }

    /// Build a response for `PutBucketPolicy` (204 No Content).
    #[must_use]
    pub fn put_bucket_policy() -> Self {
        Self::new(204)
    }

    /// Build a response for `GetBucketPolicy` (200 OK, JSON body).
    #[must_use]
    pub fn get_bucket_policy(policy: &str) -> Self {
        Self::new(200).json_body(policy.to_string())
    }

    /// Build a response for `GetBucketPolicyStatus` (200 OK, XML body).
    #[must_use]
    pub fn get_bucket_policy_status(is_public: bool) -> Self {
        let is_public = if is_public { "true" } else { "false" };
        Self::new(200).xml_body(format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><PolicyStatus xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><IsPublic>{is_public}</IsPublic></PolicyStatus>"
        ))
    }

    /// Build a response for `DeleteBucketPolicy` (204 No Content).
    #[must_use]
    pub fn delete_bucket_policy() -> Self {
        Self::new(204)
    }

    /// Build a response for `GetObjectAttributes` (200 OK, XML body).
    #[must_use]
    pub fn get_object_attributes(
        body_xml: &str,
        last_modified: u64,
        version_id: VersionId,
    ) -> Self {
        let mut resp = Self::new(200);
        resp.body = body_xml.as_bytes().to_vec();
        resp.headers
            .push(("Content-Length".to_string(), resp.body.len().to_string()));
        resp = resp.header("Last-Modified", &format_http_date(last_modified));
        if version_id.is_versioned() {
            resp = resp.header("x-amz-version-id", &format_version_id(version_id));
        }
        resp
    }

    /// Build a response for `PutBucketAcl` (200 OK, no body).
    #[must_use]
    pub fn put_bucket_acl() -> Self {
        Self::new(200)
    }

    /// Build a response for `GetBucketAcl`.
    #[must_use]
    pub fn get_bucket_acl(
        result: &GetBucketAclResult,
        owner_display_name: &str,
        grants: &[xml::RenderedAclGrant],
    ) -> Self {
        Self::new(200).chunked_xml_body(xml::acl_xml(
            owner_display_name,
            &result.owner_canonical_id,
            grants,
        ))
    }

    /// Build a response for `PutObjectAcl` (200 OK, optional version header).
    #[must_use]
    pub fn put_object_acl(version_id: VersionId) -> Self {
        let mut resp = Self::new(200);
        if version_id.is_versioned() {
            resp = resp.header("x-amz-version-id", &format_version_id(version_id));
        }
        resp
    }

    /// Build a response for `GetObjectAcl`.
    #[must_use]
    pub fn get_object_acl(
        result: &GetObjectAclResult,
        owner_display_name: &str,
        grants: &[xml::RenderedAclGrant],
    ) -> Self {
        let mut resp = Self::new(200).chunked_xml_body(xml::acl_xml(
            owner_display_name,
            &result.owner_canonical_id,
            grants,
        ));
        if result.version_id.is_versioned() {
            resp = resp.header("x-amz-version-id", &format_version_id(result.version_id));
        }
        resp
    }

    /// Build a response for `CreateMultipartUpload` (200 OK, XML body).
    #[must_use]
    pub fn create_multipart_upload(
        bucket: &str,
        key: &str,
        upload_id: &str,
        ctx: CreateMultipartUploadResponseContext<'_>,
    ) -> Self {
        let body = xml::initiate_multipart_upload_xml(
            bucket,
            key,
            upload_id,
            ctx.checksum_algorithm.map(ChecksumAlgorithm::as_str),
            ctx.checksum_type.map(ChecksumType::as_str),
        );
        let mut resp = Self::new(200).chunked_body(body.into_bytes());
        if let Some(algo) = ctx.checksum_algorithm {
            resp = resp.header("x-amz-checksum-algorithm", algo.as_str());
        }
        if let Some(checksum_type) = ctx.checksum_type {
            resp = resp.header("x-amz-checksum-type", checksum_type.as_str());
        }
        resp.apply_lifecycle_abort_headers(ctx.lifecycle_abort)
            .apply_managed_encryption_headers(ctx.managed_encryption)
            .apply_sse_customer_headers(ctx.sse_customer)
    }

    /// Build a response for `UploadPart` (200 OK, `ETag` header, optional checksum).
    #[must_use]
    pub fn upload_part(
        etag: &str,
        checksum: Option<&RawChecksum>,
        managed_encryption: Option<ManagedEncryptionAlgorithm>,
        sse_customer: Option<&SseCustomerResponseHeaders>,
    ) -> Self {
        let mut resp = Self::new(200).header("ETag", etag);
        if let Some(cksum) = checksum {
            use base64::Engine;
            let b64 = base64::engine::general_purpose::STANDARD.encode(cksum.bytes());
            resp = resp.header(cksum.algorithm().header_name(), &b64);
        }
        resp.apply_managed_encryption_headers(managed_encryption)
            .apply_sse_customer_headers(sse_customer)
    }

    /// Build a response for `UploadPartCopy` (200 OK, XML body with `CopyPartResult`).
    #[must_use]
    pub fn upload_part_copy(
        etag: &str,
        last_modified: u64,
        managed_encryption: Option<ManagedEncryptionAlgorithm>,
        sse_customer: Option<&SseCustomerResponseHeaders>,
    ) -> Self {
        let body = xml::copy_part_result_xml(etag, last_modified);
        Self::new(200)
            .xml_body(body)
            .apply_managed_encryption_headers(managed_encryption)
            .apply_sse_customer_headers(sse_customer)
    }

    /// Build a response for `CompleteMultipartUpload` (200 OK, XML body).
    #[must_use]
    pub fn complete_multipart_upload(
        bucket: &str,
        key: &str,
        result: &CompleteMultipartUploadResult,
    ) -> Self {
        let body = xml::complete_multipart_upload_xml(
            bucket,
            key,
            &result.etag,
            result.checksum_algorithm,
            result.checksum_type,
            result.checksum_value.as_deref(),
        );
        let mut resp = Self::new(200).chunked_xml_body(body);
        if result.version_id.is_versioned() {
            resp = resp.header("x-amz-version-id", &format_version_id(result.version_id));
        }
        resp.apply_lifecycle_expiration_header(result.lifecycle_expiration.as_ref())
            .apply_managed_encryption_headers(result.managed_encryption)
    }

    /// Build a response for `AbortMultipartUpload` (204 No Content).
    #[must_use]
    pub fn abort_multipart_upload() -> Self {
        Self::new(204)
    }

    /// Build a response for `ListMultipartUploads` (200 OK, XML body).
    #[must_use]
    pub fn list_multipart_uploads(
        bucket: &str,
        prefix: Option<&str>,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
        max_uploads: u32,
        result: &xml::RenderedListMultipartUploadsResult,
    ) -> Self {
        let body = xml::list_multipart_uploads_xml(
            bucket,
            prefix,
            key_marker,
            upload_id_marker,
            max_uploads,
            result,
        );
        Self::new(200).chunked_xml_body(body)
    }

    /// Build a response for `ListParts` (200 OK, XML body).
    #[must_use]
    pub fn list_parts(
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number_marker: Option<u32>,
        max_parts: u32,
        result: &ListPartsResult,
    ) -> Self {
        let body = xml::list_parts_xml(
            bucket,
            key,
            upload_id,
            part_number_marker,
            max_parts,
            result,
        );
        Self::new(200)
            .chunked_xml_body(body)
            .apply_lifecycle_abort_headers(result.lifecycle_abort.as_ref())
    }

    /// Build a 200 response for a CORS preflight (headers added by caller).
    #[must_use]
    pub fn cors_preflight() -> Self {
        Self::new(200)
    }

    /// Build a 403 Forbidden response.
    #[must_use]
    pub fn forbidden() -> Self {
        let body = xml::error_xml_with_host_id(
            "AccessDenied",
            "Access Denied",
            Self::TEST_REQUEST_ID,
            Self::TEST_HOST_ID,
        );
        Self::new(403).chunked_xml_body(body)
    }

    /// Build an error response.
    #[must_use]
    pub fn error(err: &ServerError, resource: &str) -> Self {
        const INTERNAL_ERROR_MESSAGE: &str = "We encountered an internal error. Please try again.";

        // Special cases that need extra XML elements
        match err {
            ServerError::BucketNotFound { name } => {
                let body =
                    xml::no_such_bucket_error_xml(name, Self::TEST_REQUEST_ID, Self::TEST_HOST_ID);
                return Self::new(404).chunked_xml_body(body);
            }
            ServerError::ObjectNotFound { key, .. } | ServerError::DeleteMarkerHit { key, .. } => {
                let body =
                    xml::no_such_key_error_xml(key, Self::TEST_REQUEST_ID, Self::TEST_HOST_ID);
                return Self::new(404).chunked_xml_body(body);
            }
            ServerError::HeadDeleteMarkerMethodNotAllowed {
                version_id,
                last_modified,
            } => {
                return Self::head_delete_marker_method_not_allowed(*version_id, *last_modified);
            }
            ServerError::NoSuchBucketPolicy { bucket } => {
                let body = xml::no_such_bucket_policy_error_xml(
                    bucket,
                    Self::TEST_REQUEST_ID,
                    Self::TEST_HOST_ID,
                );
                return Self::new(404).chunked_xml_body(body);
            }
            ServerError::AccessDenied
            | ServerError::Auth(auth::AuthError::MissingAuth)
            | ServerError::Auth(auth::AuthError::AccessDenied) => {
                let body = xml::error_xml_with_host_id(
                    "AccessDenied",
                    "Access Denied",
                    Self::TEST_REQUEST_ID,
                    Self::TEST_HOST_ID,
                );
                return Self::new(403).chunked_xml_body(body);
            }
            ServerError::XAmzContentSHA256Mismatch {
                client_hash,
                server_hash,
            } => {
                let body = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                     <Error>\
                     <Code>XAmzContentSHA256Mismatch</Code>\
                     <Message>The provided 'x-amz-content-sha256' header does not match what was computed.</Message>\
                     <ClientComputedContentSHA256>{}</ClientComputedContentSHA256>\
                     <S3ComputedContentSHA256>{}</S3ComputedContentSHA256>\
                     <Resource>{}</Resource>\
                     <RequestId>{}</RequestId>\
                     <HostId>{}</HostId>\
                     </Error>",
                    xml::xml_escape(client_hash),
                    xml::xml_escape(server_hash),
                    xml::xml_escape(resource),
                    Self::TEST_REQUEST_ID,
                    Self::TEST_HOST_ID,
                );
                return Self::new(400).chunked_xml_body(body);
            }
            ServerError::Auth(auth::AuthError::UnsignedHeaders { headers }) => {
                let headers_str = headers.join(";");
                let body = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                     <Error>\
                     <Code>AccessDenied</Code>\
                     <Message>There were headers present in the request which were not signed</Message>\
                     <HeadersNotSigned>{}</HeadersNotSigned>\
                     <Resource>{}</Resource>\
                     <RequestId>{}</RequestId>\
                     <HostId>{}</HostId>\
                     </Error>",
                    xml::xml_escape(&headers_str),
                    xml::xml_escape(resource),
                    Self::TEST_REQUEST_ID,
                    Self::TEST_HOST_ID,
                );
                return Self::new(403).chunked_xml_body(body);
            }
            ServerError::Auth(auth::AuthError::UnexpectedSecurityToken { token }) => {
                let body = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                     <Error>\
                     <Code>InvalidToken</Code>\
                     <Message>The provided token is malformed or otherwise invalid.</Message>\
                     <Token-0>{}</Token-0>\
                     <RequestId>{}</RequestId>\
                     <HostId>{}</HostId>\
                     </Error>",
                    xml::xml_escape(token),
                    Self::TEST_REQUEST_ID,
                    Self::TEST_HOST_ID,
                );
                return Self::new(400).chunked_xml_body(body);
            }
            ServerError::Auth(auth::AuthError::DuplicateAuthorizationHeader) => {
                let body = xml::header_not_implemented_xml(
                    "Authorization",
                    resource,
                    Self::TEST_REQUEST_ID,
                );
                return Self::new(501).chunked_xml_body(body);
            }
            ServerError::MaxMessageLengthExceeded {
                max_message_length_bytes,
            } => {
                let body = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                     <Error>\
                     <Code>MaxMessageLengthExceeded</Code>\
                     <Message>Your request was too big.</Message>\
                     <MaxMessageLengthBytes>{}</MaxMessageLengthBytes>\
                     <RequestId>{}</RequestId>\
                     <HostId>{}</HostId>\
                     </Error>",
                    max_message_length_bytes,
                    Self::TEST_REQUEST_ID,
                    Self::TEST_HOST_ID,
                );
                return Self::new(400).chunked_xml_body(body);
            }
            ServerError::HeaderNotImplemented { ref header } => {
                let body = xml::header_not_implemented_xml(header, resource, Self::TEST_REQUEST_ID);
                return Self::new(501).chunked_xml_body(body);
            }
            ServerError::QueryParameterNotImplemented {
                ref query_parameter,
            } => {
                let body = xml::query_parameter_not_implemented_xml(
                    query_parameter,
                    resource,
                    Self::TEST_REQUEST_ID,
                );
                return Self::new(501).chunked_xml_body(body);
            }
            ServerError::InvalidSseCustomerKeyMd5 => {
                let body = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                     <Error>\
                     <Code>InvalidArgument</Code>\
                     <Message>The calculated MD5 hash of the key did not match the hash that was provided.</Message>\
                     <ArgumentName>x-amz-server-side-encryption</ArgumentName>\
                     <Resource>{}</Resource>\
                     <RequestId>{}</RequestId>\
                     <HostId>{}</HostId>\
                     </Error>",
                    xml::xml_escape(resource),
                    Self::TEST_REQUEST_ID,
                    Self::TEST_HOST_ID,
                );
                return Self::new(400).chunked_xml_body(body);
            }
            ServerError::InvalidEncryptionAlgorithmError { value } => {
                let body = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                     <Error>\
                     <Code>InvalidEncryptionAlgorithmError</Code>\
                     <Message>The Encryption request you specified is not valid. Supported value: AES256.</Message>\
                     <ArgumentName>x-amz-server-side-encryption</ArgumentName>\
                     <ArgumentValue>{}</ArgumentValue>\
                     <Resource>{}</Resource>\
                     <RequestId>{}</RequestId>\
                     <HostId>{}</HostId>\
                     </Error>",
                    xml::xml_escape(value),
                    xml::xml_escape(resource),
                    Self::TEST_REQUEST_ID,
                    Self::TEST_HOST_ID,
                );
                return Self::new(400).chunked_xml_body(body);
            }
            ServerError::WrongRegion {
                provided_region,
                expected_region,
            } => {
                let body = xml::error_xml_with_region(
                    "AuthorizationHeaderMalformed",
                    &format!(
                        "The authorization header is malformed; the region '{provided_region}' is wrong; expecting '{expected_region}'"
                    ),
                    resource,
                    Self::TEST_REQUEST_ID,
                    expected_region,
                );
                return Self::new(400)
                    .header("x-amz-bucket-region", expected_region)
                    .chunked_xml_body(body);
            }
            ServerError::InvalidBucketNamespace {
                reason,
                bucket_namespace,
            } => {
                let body = xml::error_xml_with_bucket_namespace(
                    "InvalidBucketNamespace",
                    reason,
                    bucket_namespace,
                    Self::TEST_REQUEST_ID,
                );
                return Self::new(400).chunked_xml_body(body);
            }
            ServerError::KeyTooLongError {
                size,
                max_size_allowed,
            } => {
                let body =
                    xml::key_too_long_error_xml(*size, *max_size_allowed, Self::TEST_REQUEST_ID);
                return Self::new(400).chunked_xml_body(body);
            }
            _ => {}
        }

        let fallback;
        let message = match err {
            ServerError::InvalidRequest { reason } => reason.as_str(),
            ServerError::InvalidArgument { reason } => reason.as_str(),
            ServerError::InvalidBucketName { reason } => reason.as_str(),
            ServerError::InvalidBucketNamespace { reason, .. } => reason.as_str(),
            ServerError::NotImplemented { feature } => feature.as_str(),
            ServerError::HeaderNotImplemented { header } => header.as_str(),
            ServerError::QueryParameterNotImplemented { query_parameter } => {
                query_parameter.as_str()
            }
            ServerError::VersionNotFound { .. } => "The specified version does not exist.",
            ServerError::InternalError { .. }
            | ServerError::IntegrityError { .. }
            | ServerError::Store(_)
            | ServerError::Metadata(_)
            | ServerError::Ec(_)
            | ServerError::MetadataBlobError { .. } => INTERNAL_ERROR_MESSAGE,
            _ => {
                fallback = err.to_string();
                fallback.as_str()
            }
        };
        let body = xml::error_xml(
            err.s3_error_code(),
            message,
            resource,
            Self::TEST_REQUEST_ID,
        );
        let resp = Self::new(err.http_status()).chunked_xml_body(body);
        if matches!(err, ServerError::SlowDown) {
            resp.header("Retry-After", "1")
        } else {
            resp
        }
    }
}

fn format_object_lock_header_timestamp(unix_seconds: u64) -> String {
    let days_since_epoch = unix_seconds / 86400;
    let time_of_day = unix_seconds % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    let z = days_since_epoch as i64 + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = i64::from(yoe) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { y + 1 } else { y };

    format!("{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{seconds:02}Z")
}

/// Format a unix millisecond timestamp as HTTP date (RFC 7231).
fn format_http_date(millis: u64) -> String {
    let secs = millis / 1000;
    let days_since_epoch = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    let weekday = ((days_since_epoch + 4) % 7) as usize; // Jan 1 1970 = Thursday (4)
    let weekdays = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

    let (year, month, day) = days_to_date(days_since_epoch as i64);
    let months = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];

    format!(
        "{}, {:02} {} {:04} {:02}:{:02}:{:02} GMT",
        weekdays[weekday],
        day,
        months[(month - 1) as usize],
        year,
        hours,
        minutes,
        seconds
    )
}

/// Convert days since Unix epoch to (year, month, day).
fn days_to_date(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = i64::from(yoe) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

/// Parse an RFC 7231 HTTP date (e.g. `"Thu, 01 Jan 1970 00:00:00 GMT"`) into unix milliseconds.
/// Returns `None` for malformed dates. Only supports this one format (IMF-fixdate).
pub(crate) fn parse_http_date(s: &str) -> Option<u64> {
    // Format: "Day, DD Mon YYYY HH:MM:SS GMT"
    let s = s.trim();
    if s.len() < 29 || !s.ends_with("GMT") {
        return None;
    }

    let day: u32 = s[5..7].parse().ok()?;
    let month_str = &s[8..11];
    let year: i64 = s[12..16].parse().ok()?;
    let hours: u64 = s[17..19].parse().ok()?;
    let minutes: u64 = s[20..22].parse().ok()?;
    let seconds: u64 = s[23..25].parse().ok()?;

    let months = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    #[allow(clippy::cast_possible_truncation)]
    let month = months.iter().position(|&m| m == month_str)? as u32 + 1;

    if hours >= 24 || minutes >= 60 || seconds >= 60 || day == 0 || day > 31 || month > 12 {
        return None;
    }

    #[allow(clippy::cast_sign_loss)]
    let secs =
        date_to_days(year, month, day) as u64 * 86400 + hours * 3600 + minutes * 60 + seconds;
    Some(secs * 1000)
}

/// Convert (year, month, day) to days since Unix epoch. Inverse of `days_to_date`.
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn date_to_days(year: i64, month: u32, day: u32) -> i64 {
    // Civil calendar algorithm (inverse of days_to_date)
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u32;
    let m = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * m + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + i64::from(doe) - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::{
        GetObjectResult, HeadObjectResult, ListEntry, ListObjectsResult, PutObjectResult,
    };
    use crate::metadata_blob::MetadataBlob;
    use s3_types::{AclGrant, AclGrantee, AclGrants, AclPermission};

    fn system_metadata(headers: &[(&str, &str)]) -> SystemMetadata {
        SystemMetadata::from_pairs(headers)
    }

    fn find_header<'a>(resp: &'a S3Response, name: &str) -> Option<&'a str> {
        resp.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    // ── format_http_date ──────────────────────────────────────────────

    #[test]
    fn format_http_date_epoch() {
        assert_eq!(format_http_date(0), "Thu, 01 Jan 1970 00:00:00 GMT");
    }

    #[test]
    fn format_http_date_known_date() {
        // 2024-01-15 12:30:45 UTC
        // seconds: 1705321845, millis: 1705321845000
        assert_eq!(
            format_http_date(1705321845000),
            "Mon, 15 Jan 2024 12:30:45 GMT"
        );
    }

    // ── days_to_date ──────────────────────────────────────────────────

    #[test]
    fn days_to_date_epoch() {
        assert_eq!(days_to_date(0), (1970, 1, 1));
    }

    #[test]
    fn days_to_date_leap_year_feb29() {
        // 2000-02-29 is day 11016 since epoch
        // 2000-01-01 is day 10957, Feb 29 = 10957 + 31 + 28 = 11016
        assert_eq!(days_to_date(11016), (2000, 2, 29));
    }

    #[test]
    fn days_to_date_year_boundary() {
        // 1970-12-31 is day 364
        assert_eq!(days_to_date(364), (1970, 12, 31));
        // 1971-01-01 is day 365
        assert_eq!(days_to_date(365), (1971, 1, 1));
    }

    #[test]
    fn days_to_date_2024_leap() {
        // 2024-02-29 — 2024 is a leap year
        // 2024-01-01 is day 19723
        // Feb 29 = 19723 + 31 + 28 = 19782
        assert_eq!(days_to_date(19782), (2024, 2, 29));
    }

    // ── put_object ────────────────────────────────────────────────────

    #[test]
    fn put_object_response() {
        let result = PutObjectResult {
            etag: "\"abc123\"".to_string(),
            version_id: VersionId::Null,
            system_metadata: SystemMetadata::EMPTY,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::put_object(&result);
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "ETag"), Some("\"abc123\""));
        // version_id=0 means unversioned — no x-amz-version-id header
        assert_eq!(find_header(&resp, "x-amz-version-id"), None);
    }

    #[test]
    fn put_object_response_includes_managed_encryption_header() {
        let result = PutObjectResult {
            etag: "\"abc123\"".to_string(),
            version_id: VersionId::Null,
            system_metadata: SystemMetadata::EMPTY,
            managed_encryption: Some(ManagedEncryptionAlgorithm::Aes256),
            lifecycle_expiration: None,
        };
        let resp = S3Response::put_object(&result);
        assert_eq!(
            find_header(&resp, "x-amz-server-side-encryption"),
            Some("AES256")
        );
    }

    #[test]
    fn put_object_response_versioned() {
        let result = PutObjectResult {
            etag: "\"abc123\"".to_string(),
            version_id: VersionId::from_u64(42),
            system_metadata: SystemMetadata::EMPTY,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::put_object(&result);
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "ETag"), Some("\"abc123\""));
        assert_eq!(find_header(&resp, "x-amz-version-id"), Some("42"));
    }

    #[test]
    fn put_object_response_includes_checksum_headers() {
        let mut system_metadata = SystemMetadata::EMPTY;
        system_metadata.set_checksum(
            ChecksumAlgorithm::Crc64nvme,
            Some(ChecksumType::FullObject),
            "AAAAAA==".to_string(),
        );
        let result = PutObjectResult {
            etag: "\"abc123\"".to_string(),
            version_id: VersionId::Null,
            system_metadata,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::put_object(&result);
        assert_eq!(
            find_header(&resp, "x-amz-checksum-crc64nvme"),
            Some("AAAAAA==")
        );
        assert_eq!(
            find_header(&resp, "x-amz-checksum-type"),
            Some("FULL_OBJECT")
        );
    }

    #[test]
    fn get_object_attributes_response_matches_aws_header_shape() {
        let resp = S3Response::get_object_attributes(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<GetObjectAttributesResponse xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"></GetObjectAttributesResponse>",
            1_705_321_845_000,
            VersionId::Null,
        );
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "Content-Length"), Some("146"));
        assert_eq!(
            find_header(&resp, "Last-Modified"),
            Some("Mon, 15 Jan 2024 12:30:45 GMT")
        );
        assert_eq!(find_header(&resp, "Content-Type"), None);
        assert_eq!(find_header(&resp, "x-amz-server-side-encryption"), None);
        assert_eq!(
            std::str::from_utf8(&resp.body).ok(),
            Some(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<GetObjectAttributesResponse xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"></GetObjectAttributesResponse>"
            )
        );
    }

    #[test]
    fn post_object_response_redirects_with_success_query_params() {
        let result = PutObjectResult {
            etag: "\"abc123\"".to_string(),
            version_id: VersionId::Null,
            system_metadata: SystemMetadata::EMPTY,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::post_object(
            &result,
            "my-bucket",
            "folder/my file.txt",
            201,
            Some("https://example.com/success"),
            None,
        );
        assert_eq!(resp.status_code, 303);
        assert_eq!(
            find_header(&resp, "Location"),
            Some(
                "https://example.com/success?bucket=my-bucket&key=folder%2Fmy%20file.txt&etag=%22abc123%22"
            )
        );
        assert_eq!(find_header(&resp, "ETag"), Some("\"abc123\""));
        assert!(resp.body.is_empty());
    }

    #[test]
    fn post_object_response_includes_managed_encryption_header() {
        let result = PutObjectResult {
            etag: "\"abc123\"".to_string(),
            version_id: VersionId::Null,
            system_metadata: SystemMetadata::EMPTY,
            managed_encryption: Some(ManagedEncryptionAlgorithm::Aes256),
            lifecycle_expiration: None,
        };
        let resp = S3Response::post_object(
            &result,
            "my-bucket",
            "my-key",
            204,
            None,
            Some("https://s3.us-east-1.amazonaws.com/my-bucket/my-key"),
        );
        assert_eq!(
            find_header(&resp, "x-amz-server-side-encryption"),
            Some("AES256")
        );
        assert_eq!(
            find_header(&resp, "Location"),
            Some("https://s3.us-east-1.amazonaws.com/my-bucket/my-key")
        );
    }

    #[test]
    fn post_object_response_redirect_appends_to_existing_query() {
        let result = PutObjectResult {
            etag: "\"abc123\"".to_string(),
            version_id: VersionId::Null,
            system_metadata: SystemMetadata::EMPTY,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::post_object(
            &result,
            "my-bucket",
            "my-key",
            204,
            Some("https://example.com/success?foo=bar"),
            None,
        );
        assert_eq!(resp.status_code, 303);
        assert_eq!(
            find_header(&resp, "Location"),
            Some(
                "https://example.com/success?foo=bar&bucket=my-bucket&key=my-key&etag=%22abc123%22"
            )
        );
    }

    #[test]
    fn post_object_response_ignores_invalid_redirect() {
        let result = PutObjectResult {
            etag: "\"abc123\"".to_string(),
            version_id: VersionId::Null,
            system_metadata: SystemMetadata::EMPTY,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::post_object(
            &result,
            "my-bucket",
            "my-key",
            200,
            Some("https://example.com/\n"),
            Some("https://s3.us-east-1.amazonaws.com/my-bucket/my-key"),
        );
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "Location"), None);
        assert_eq!(find_header(&resp, "ETag"), Some("\"abc123\""));
    }

    #[test]
    fn put_object_response_includes_lifecycle_expiration_header() {
        let result = PutObjectResult {
            etag: "\"abc123\"".to_string(),
            version_id: VersionId::Null,
            system_metadata: SystemMetadata::EMPTY,
            managed_encryption: None,
            lifecycle_expiration: Some(LifecycleExpirationHeader {
                expiry_time_millis: 1_705_321_845_000,
                rule_id: Some("expire current".to_string()),
            }),
        };
        let resp = S3Response::put_object(&result);
        assert_eq!(
            find_header(&resp, "x-amz-expiration"),
            Some("expiry-date=\"Mon, 15 Jan 2024 12:30:45 GMT\", rule-id=\"expire%20current\"")
        );
    }

    // ── get_object ────────────────────────────────────────────────────

    #[test]
    fn get_object_with_content_type() {
        let result = GetObjectResult {
            sse_customer: None,
            body: ReadHandle::from_buffered_bytes(b"hello".to_vec()),
            metadata: MetadataBlob::new(),
            system_metadata: system_metadata(&[("content-type", "text/plain")]),
            object_lock: s3_types::ObjectLockState::default(),
            etag: "\"etag\"".into(),
            size: 5,
            last_modified: 0,
            version_id: VersionId::Null,
            tags: None,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::get_object(result, None);
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "Content-Type"), Some("text/plain"));
        assert_eq!(resp.into_test_body_bytes().unwrap(), b"hello");
    }

    #[test]
    fn get_object_default_content_type() {
        let result = GetObjectResult {
            sse_customer: None,
            body: ReadHandle::from_buffered_bytes(b"data".to_vec()),
            metadata: MetadataBlob::new(),
            system_metadata: SystemMetadata::new(),
            object_lock: s3_types::ObjectLockState::default(),
            etag: "\"etag\"".into(),
            size: 4,
            last_modified: 0,
            version_id: VersionId::Null,
            tags: None,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::get_object(result, None);
        assert_eq!(
            find_header(&resp, "Content-Type"),
            Some("application/octet-stream")
        );
    }

    #[test]
    fn get_object_with_amz_meta_headers() {
        let result = GetObjectResult {
            sse_customer: None,
            body: ReadHandle::from_buffered_bytes(vec![]),
            metadata: MetadataBlob::from_pairs(&[("x-amz-meta-author", "alice")]),
            system_metadata: SystemMetadata::new(),
            object_lock: s3_types::ObjectLockState::default(),
            etag: "\"e\"".into(),
            size: 0,
            last_modified: 0,
            version_id: VersionId::Null,
            tags: None,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::get_object(result, None);
        assert_eq!(find_header(&resp, "x-amz-meta-author"), Some("alice"));
    }

    #[test]
    fn get_object_with_all_standard_metadata() {
        let result = GetObjectResult {
            sse_customer: None,
            body: ReadHandle::from_buffered_bytes(vec![]),
            metadata: MetadataBlob::new(),
            system_metadata: system_metadata(&[
                ("content-type", "text/html"),
                ("content-encoding", "gzip"),
                ("cache-control", "max-age=3600"),
                ("content-disposition", "attachment"),
                ("content-language", "en-US"),
                ("expires", "Thu, 01 Jan 2099 00:00:00 GMT"),
            ]),
            object_lock: s3_types::ObjectLockState::default(),
            etag: "\"e\"".into(),
            size: 0,
            last_modified: 0,
            version_id: VersionId::Null,
            tags: None,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::get_object(result, None);
        assert_eq!(find_header(&resp, "Content-Type"), Some("text/html"));
        assert_eq!(find_header(&resp, "Content-Encoding"), Some("gzip"));
        assert_eq!(find_header(&resp, "Cache-Control"), Some("max-age=3600"));
        assert_eq!(
            find_header(&resp, "Content-Disposition"),
            Some("attachment")
        );
        assert_eq!(find_header(&resp, "Content-Language"), Some("en-US"));
        assert_eq!(
            find_header(&resp, "Expires"),
            Some("Thu, 01 Jan 2099 00:00:00 GMT")
        );
    }

    #[test]
    fn get_object_checksum_type_with_checksum_mode_enabled() {
        let result = GetObjectResult {
            sse_customer: None,
            body: ReadHandle::from_buffered_bytes(vec![]),
            metadata: MetadataBlob::new(),
            system_metadata: system_metadata(&[
                ("x-amz-checksum-crc32", "AAAAAA=="),
                ("x-amz-checksum-type", "FULL_OBJECT"),
            ]),
            object_lock: s3_types::ObjectLockState::default(),
            etag: "\"e\"".into(),
            size: 0,
            last_modified: 0,
            version_id: VersionId::Null,
            tags: None,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::get_object(result, Some("ENABLED"));
        assert_eq!(find_header(&resp, "x-amz-checksum-crc32"), Some("AAAAAA=="));
        assert_eq!(
            find_header(&resp, "x-amz-checksum-type"),
            Some("FULL_OBJECT")
        );
    }

    #[test]
    fn get_object_checksum_type_omitted_without_checksum_mode() {
        let result = GetObjectResult {
            sse_customer: None,
            body: ReadHandle::from_buffered_bytes(vec![]),
            metadata: MetadataBlob::new(),
            system_metadata: system_metadata(&[
                ("x-amz-checksum-crc32", "AAAAAA=="),
                ("x-amz-checksum-type", "FULL_OBJECT"),
            ]),
            object_lock: s3_types::ObjectLockState::default(),
            etag: "\"e\"".into(),
            size: 0,
            last_modified: 0,
            version_id: VersionId::Null,
            tags: None,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::get_object(result, None);
        assert_eq!(find_header(&resp, "x-amz-checksum-crc32"), None);
        assert_eq!(find_header(&resp, "x-amz-checksum-type"), None);
    }

    #[test]
    fn get_object_emits_object_lock_headers() {
        let result = GetObjectResult {
            sse_customer: None,
            body: ReadHandle::from_buffered_bytes(vec![]),
            metadata: MetadataBlob::new(),
            system_metadata: SystemMetadata::new(),
            object_lock: s3_types::ObjectLockState {
                retention: Some(ObjectRetention {
                    mode: s3_types::ObjectLockMode::Governance,
                    retain_until_unix_seconds: 1_775_001_600,
                }),
                legal_hold: s3_types::StoredLegalHoldStatus::On,
            },
            etag: "\"e\"".into(),
            size: 0,
            last_modified: 0,
            version_id: VersionId::Null,
            tags: None,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::get_object(result, None);
        assert_eq!(
            find_header(&resp, "x-amz-object-lock-mode"),
            Some("GOVERNANCE")
        );
        assert_eq!(
            find_header(&resp, "x-amz-object-lock-retain-until-date"),
            Some("2026-04-01T00:00:00Z")
        );
        assert_eq!(
            find_header(&resp, "x-amz-object-lock-legal-hold"),
            Some("ON")
        );
    }

    #[test]
    fn get_object_response_includes_lifecycle_expiration_without_rule_id() {
        let result = GetObjectResult {
            sse_customer: None,
            body: ReadHandle::from_buffered_bytes(b"hello".to_vec()),
            metadata: MetadataBlob::new(),
            system_metadata: SystemMetadata::new(),
            object_lock: s3_types::ObjectLockState::default(),
            etag: "\"etag\"".into(),
            size: 5,
            last_modified: 0,
            version_id: VersionId::Null,
            tags: None,
            managed_encryption: None,
            lifecycle_expiration: Some(LifecycleExpirationHeader {
                expiry_time_millis: 1_705_321_845_000,
                rule_id: None,
            }),
        };
        let resp = S3Response::get_object(result, None);
        assert_eq!(
            find_header(&resp, "x-amz-expiration"),
            Some("expiry-date=\"Mon, 15 Jan 2024 12:30:45 GMT\"")
        );
    }

    // ── head_object ───────────────────────────────────────────────────

    #[test]
    fn head_object_response() {
        let result = HeadObjectResult {
            sse_customer: None,
            metadata: MetadataBlob::new(),
            system_metadata: system_metadata(&[("content-type", "image/png")]),
            object_lock: s3_types::ObjectLockState::default(),
            etag: "\"etag\"".into(),
            size: 1024,
            last_modified: 0,
            version_id: VersionId::Null,
            tags: None,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::head_object(&result, None);
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "ETag"), Some("\"etag\""));
        assert_eq!(find_header(&resp, "Content-Length"), Some("1024"));
        assert_eq!(find_header(&resp, "Content-Type"), Some("image/png"));
        assert!(resp.body.is_empty());
    }

    #[test]
    fn head_object_default_content_type() {
        let result = HeadObjectResult {
            sse_customer: None,
            metadata: MetadataBlob::new(),
            system_metadata: SystemMetadata::new(),
            object_lock: s3_types::ObjectLockState::default(),
            etag: "\"e\"".into(),
            size: 0,
            last_modified: 0,
            version_id: VersionId::Null,
            tags: None,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::head_object(&result, None);
        assert_eq!(
            find_header(&resp, "Content-Type"),
            Some("application/octet-stream")
        );
    }

    #[test]
    fn head_object_with_encoding_and_cache() {
        let result = HeadObjectResult {
            sse_customer: None,
            metadata: MetadataBlob::new(),
            system_metadata: system_metadata(&[
                ("content-encoding", "br"),
                ("cache-control", "no-cache"),
            ]),
            object_lock: s3_types::ObjectLockState::default(),
            etag: "\"e\"".into(),
            size: 10,
            last_modified: 0,
            version_id: VersionId::Null,
            tags: None,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::head_object(&result, None);
        assert_eq!(find_header(&resp, "Content-Encoding"), Some("br"));
        assert_eq!(find_header(&resp, "Cache-Control"), Some("no-cache"));
    }

    #[test]
    fn head_object_with_amz_meta() {
        let result = HeadObjectResult {
            sse_customer: None,
            metadata: MetadataBlob::from_pairs(&[("x-amz-meta-tag", "value")]),
            system_metadata: SystemMetadata::new(),
            object_lock: s3_types::ObjectLockState::default(),
            etag: "\"e\"".into(),
            size: 0,
            last_modified: 0,
            version_id: VersionId::Null,
            tags: None,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::head_object(&result, None);
        assert_eq!(find_header(&resp, "x-amz-meta-tag"), Some("value"));
    }

    #[test]
    fn head_object_checksum_type_with_checksum_mode_enabled() {
        let result = HeadObjectResult {
            sse_customer: None,
            metadata: MetadataBlob::new(),
            system_metadata: system_metadata(&[
                ("x-amz-checksum-crc32", "AAAAAA=="),
                ("x-amz-checksum-type", "COMPOSITE"),
            ]),
            object_lock: s3_types::ObjectLockState::default(),
            etag: "\"e\"".into(),
            size: 0,
            last_modified: 0,
            version_id: VersionId::Null,
            tags: None,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::head_object(&result, Some("ENABLED"));
        assert_eq!(find_header(&resp, "x-amz-checksum-crc32"), Some("AAAAAA=="));
        assert_eq!(find_header(&resp, "x-amz-checksum-type"), Some("COMPOSITE"));
    }

    #[test]
    fn head_object_checksum_type_omitted_without_checksum_mode() {
        let result = HeadObjectResult {
            sse_customer: None,
            metadata: MetadataBlob::new(),
            system_metadata: system_metadata(&[
                ("x-amz-checksum-crc32", "AAAAAA=="),
                ("x-amz-checksum-type", "COMPOSITE"),
            ]),
            object_lock: s3_types::ObjectLockState::default(),
            etag: "\"e\"".into(),
            size: 0,
            last_modified: 0,
            version_id: VersionId::Null,
            tags: None,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::head_object(&result, None);
        assert_eq!(find_header(&resp, "x-amz-checksum-crc32"), None);
        assert_eq!(find_header(&resp, "x-amz-checksum-type"), None);
    }

    #[test]
    fn head_object_emits_object_lock_headers_and_omits_never_set_legal_hold() {
        let result = HeadObjectResult {
            sse_customer: None,
            metadata: MetadataBlob::new(),
            system_metadata: SystemMetadata::new(),
            object_lock: s3_types::ObjectLockState {
                retention: Some(ObjectRetention {
                    mode: s3_types::ObjectLockMode::Compliance,
                    retain_until_unix_seconds: 1_775_001_600,
                }),
                legal_hold: s3_types::StoredLegalHoldStatus::NotSet,
            },
            etag: "\"e\"".into(),
            size: 0,
            last_modified: 0,
            version_id: VersionId::Null,
            tags: None,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::head_object(&result, None);
        assert_eq!(
            find_header(&resp, "x-amz-object-lock-mode"),
            Some("COMPLIANCE")
        );
        assert_eq!(
            find_header(&resp, "x-amz-object-lock-retain-until-date"),
            Some("2026-04-01T00:00:00Z")
        );
        assert_eq!(find_header(&resp, "x-amz-object-lock-legal-hold"), None);
    }

    #[test]
    fn head_object_response_includes_lifecycle_expiration_header() {
        let result = HeadObjectResult {
            sse_customer: None,
            metadata: MetadataBlob::new(),
            system_metadata: SystemMetadata::new(),
            object_lock: s3_types::ObjectLockState::default(),
            etag: "\"e\"".into(),
            size: 0,
            last_modified: 0,
            version_id: VersionId::Null,
            tags: None,
            managed_encryption: None,
            lifecycle_expiration: Some(LifecycleExpirationHeader {
                expiry_time_millis: 1_705_321_845_000,
                rule_id: Some("expire/head".to_string()),
            }),
        };
        let resp = S3Response::head_object(&result, None);
        assert_eq!(
            find_header(&resp, "x-amz-expiration"),
            Some("expiry-date=\"Mon, 15 Jan 2024 12:30:45 GMT\", rule-id=\"expire%2Fhead\"")
        );
    }

    // ── delete_object ─────────────────────────────────────────────────

    #[test]
    fn delete_object_response() {
        use crate::coordinator::DeleteObjectResult;
        let result = DeleteObjectResult {
            version_id: VersionId::Null,
            delete_marker: false,
        };
        let resp = S3Response::delete_object(&result);
        assert_eq!(resp.status_code, 204);
        // version_id=0 means unversioned — no x-amz-version-id header
        assert_eq!(find_header(&resp, "x-amz-version-id"), None);
        assert!(resp.body.is_empty());
    }

    #[test]
    fn delete_object_versioned_with_marker() {
        use crate::coordinator::DeleteObjectResult;
        let result = DeleteObjectResult {
            version_id: VersionId::from_u64(5),
            delete_marker: true,
        };
        let resp = S3Response::delete_object(&result);
        assert_eq!(resp.status_code, 204);
        assert_eq!(find_header(&resp, "x-amz-version-id"), Some("5"));
        assert_eq!(find_header(&resp, "x-amz-delete-marker"), Some("true"));
    }

    #[test]
    fn head_delete_marker_method_not_allowed_response() {
        let resp = S3Response::head_delete_marker_method_not_allowed(
            VersionId::from_u64(5),
            1_705_321_845_000,
        );
        assert_eq!(resp.status_code, 405);
        assert_eq!(find_header(&resp, "Allow"), Some("DELETE"));
        assert_eq!(find_header(&resp, "x-amz-delete-marker"), Some("true"));
        assert_eq!(find_header(&resp, "x-amz-version-id"), Some("5"));
        assert_eq!(
            find_header(&resp, "Last-Modified"),
            Some("Mon, 15 Jan 2024 12:30:45 GMT")
        );
        assert!(resp.body.is_empty());
    }

    // ── create_bucket ─────────────────────────────────────────────────

    #[test]
    fn create_bucket_response() {
        let resp = S3Response::create_bucket("my-bucket");
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "Location"), Some("/my-bucket"));
    }

    // ── delete_bucket ─────────────────────────────────────────────────

    #[test]
    fn delete_bucket_response() {
        let resp = S3Response::delete_bucket();
        assert_eq!(resp.status_code, 204);
    }

    #[test]
    fn get_bucket_object_lock_configuration_response() {
        let resp = S3Response::get_bucket_object_lock_configuration(BucketObjectLockConfig {
            enabled: true,
            default_retention: Some(s3_types::ObjectLockDefaultRetention {
                mode: s3_types::ObjectLockMode::Governance,
                period: s3_types::RetentionPeriod::days(1).unwrap(),
            }),
        });
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "Content-Type"), None);
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("<ObjectLockEnabled>Enabled</ObjectLockEnabled>"));
        assert!(body.contains("<Days>1</Days>"));
    }

    #[test]
    fn get_object_retention_response() {
        let resp = S3Response::get_object_retention(Some(ObjectRetention {
            mode: s3_types::ObjectLockMode::Governance,
            retain_until_unix_seconds: 1_775_001_600,
        }));
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "Content-Type"), None);
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("<Retention"));
        assert!(body.contains("<Mode>GOVERNANCE</Mode>"));
    }

    #[test]
    fn get_object_legal_hold_response() {
        let resp = S3Response::get_object_legal_hold(Some(LegalHoldStatus::On));
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "Content-Type"), None);
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("<LegalHold"));
        assert!(body.contains("<Status>ON</Status>"));
    }

    // ── head_bucket ───────────────────────────────────────────────────

    #[test]
    fn head_bucket_response() {
        let info = BucketSummary {
            name: "b".into(),
            owner_principal: "owner".into(),
            owner_canonical_id: CanonicalUserId::from_principal("owner"),
            created_at: 0,
            acl_grants: AclGrants::default(),
            versioning: BucketVersioningState::Disabled,
            object_lock: s3_types::BucketObjectLockConfig::default(),
            public_read: false,
            public_write: false,
            public_access_block: None,
            ownership_controls: None,
            bucket_policy_present: false,
            bucket_policy_public: false,
            bucket_policy_generation: 0,
            bucket_lifecycle_present: false,
            bucket_lifecycle_generation: 0,
            encryption: EffectiveBucketEncryptionConfig::default(),
        };
        let resp = S3Response::head_bucket(&info, "us-west-2");
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "Content-Type"), Some("application/xml"));
        assert_eq!(
            find_header(&resp, "x-amz-access-point-alias"),
            Some("false")
        );
        assert_eq!(
            find_header(&resp, "x-amz-bucket-arn"),
            Some("arn:aws:s3:::b")
        );
        assert_eq!(find_header(&resp, "x-amz-bucket-region"), Some("us-west-2"));
        assert_eq!(find_header(&resp, "Content-Length"), None);
        assert!(resp.body.is_empty());
        assert!(resp.stream.is_none());
    }

    #[test]
    fn wrong_region_error_response_includes_bucket_region_hint() {
        let err = ServerError::WrongRegion {
            provided_region: "us-east-1".to_string(),
            expected_region: "us-west-2".to_string(),
        };
        let resp = S3Response::error(&err, "/bucket/key");
        assert_eq!(resp.status_code, 400);
        assert_eq!(find_header(&resp, "x-amz-bucket-region"), Some("us-west-2"));
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("<Code>AuthorizationHeaderMalformed</Code>"));
        assert!(body.contains("<Region>us-west-2</Region>"));
        assert!(body.contains("expecting 'us-west-2'"));
    }

    #[test]
    fn invalid_bucket_namespace_error_response_includes_bucket_namespace() {
        let err = ServerError::InvalidBucketNamespace {
            reason: "namespace mismatch".to_string(),
            bucket_namespace: "bucket-111122223333-us-east-1-an".to_string(),
        };
        let resp = S3Response::error(&err, "/bucket");
        assert_eq!(resp.status_code, 400);
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("<Code>InvalidBucketNamespace</Code>"));
        assert!(body.contains("<Message>namespace mismatch</Message>"));
        assert!(
            body.contains("<BucketNamespace>bucket-111122223333-us-east-1-an</BucketNamespace>")
        );
    }

    #[test]
    fn unexpected_security_token_error_response_matches_aws_shape() {
        let err = ServerError::Auth(auth::AuthError::UnexpectedSecurityToken {
            token: "bad-token-causes-400".to_string(),
        });
        let resp = S3Response::error(&err, "/bucket/key");
        assert_eq!(resp.status_code, 400);
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("<Code>InvalidToken</Code>"));
        assert!(body
            .contains("<Message>The provided token is malformed or otherwise invalid.</Message>"));
        assert!(body.contains("<Token-0>bad-token-causes-400</Token-0>"));
    }

    #[test]
    fn get_bucket_location_response_uses_legacy_us_east_1_null() {
        let resp = S3Response::get_bucket_location("us-east-1");
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "Content-Type"), Some("application/xml"));
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("<LocationConstraint"));
        assert!(body.contains("/>"));
        assert!(!body.contains(">us-east-1<"));
    }

    #[test]
    fn get_bucket_location_response_maps_eu_west_1_to_legacy_eu() {
        let resp = S3Response::get_bucket_location("eu-west-1");
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "Content-Type"), Some("application/xml"));
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains(">EU</LocationConstraint>"));
    }

    // ── list_buckets ──────────────────────────────────────────────────

    #[test]
    fn list_buckets_response() {
        let buckets = vec![BucketSummary {
            name: "test-bucket".into(),
            owner_principal: "owner".into(),
            owner_canonical_id: CanonicalUserId::from_principal("owner"),
            created_at: 1000,
            acl_grants: AclGrants::default(),
            versioning: BucketVersioningState::Disabled,
            object_lock: s3_types::BucketObjectLockConfig::default(),
            public_read: false,
            public_write: false,
            public_access_block: None,
            ownership_controls: None,
            bucket_policy_present: false,
            bucket_policy_public: false,
            bucket_policy_generation: 0,
            bucket_lifecycle_present: false,
            bucket_lifecycle_generation: 0,
            encryption: EffectiveBucketEncryptionConfig::default(),
        }];
        let owner_canonical_id = CanonicalUserId::from_principal("owner");
        let resp = S3Response::list_buckets(&buckets, "Owner A", &owner_canonical_id);
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "Content-Type"), Some("application/xml"));
        let body = String::from_utf8(resp.body).unwrap();
        assert!(body.contains("<?xml"));
        assert!(body.contains("test-bucket"));
        assert!(body.contains("ListAllMyBucketsResult"));
        assert!(body.contains(owner_canonical_id.as_str()));
        assert!(body.contains("<DisplayName>Owner A</DisplayName>"));
    }

    #[test]
    fn bucket_lifecycle_responses() {
        let put = S3Response::put_bucket_lifecycle();
        assert_eq!(put.status_code, 200);
        assert!(put.body.is_empty());

        let get = S3Response::get_bucket_lifecycle(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><LifecycleConfiguration/>",
        );
        assert_eq!(get.status_code, 200);
        assert_eq!(find_header(&get, "Content-Type"), None);
        assert_eq!(
            find_header(&get, "x-amz-transition-default-minimum-object-size"),
            Some("all_storage_classes_128K")
        );

        let delete = S3Response::delete_bucket_lifecycle();
        assert_eq!(delete.status_code, 204);
        assert!(delete.body.is_empty());
    }

    #[test]
    fn create_multipart_upload_response_includes_lifecycle_abort_headers() {
        let resp = S3Response::create_multipart_upload(
            "bucket",
            "key",
            "upload-1",
            CreateMultipartUploadResponseContext {
                managed_encryption: None,
                checksum_algorithm: Some(ChecksumAlgorithm::Sha256),
                checksum_type: Some(ChecksumType::FullObject),
                lifecycle_abort: Some(&LifecycleAbortHeaders {
                    abort_time_millis: 1_705_321_845_000,
                    rule_id: Some("abort upload".to_string()),
                }),
                sse_customer: None,
            },
        );
        assert_eq!(
            find_header(&resp, "x-amz-abort-date"),
            Some("Mon, 15 Jan 2024 12:30:45 GMT")
        );
        assert_eq!(
            find_header(&resp, "x-amz-abort-rule-id"),
            Some("abort%20upload")
        );
        assert_eq!(find_header(&resp, "Content-Type"), None);
        assert_eq!(find_header(&resp, "Content-Length"), None);
        assert_eq!(
            find_header(&resp, "x-amz-checksum-algorithm"),
            Some("SHA256")
        );
        assert_eq!(
            find_header(&resp, "x-amz-checksum-type"),
            Some("FULL_OBJECT")
        );
    }

    #[test]
    fn create_multipart_upload_response_includes_managed_encryption_header() {
        let resp = S3Response::create_multipart_upload(
            "bucket",
            "key",
            "upload-1",
            CreateMultipartUploadResponseContext {
                managed_encryption: Some(ManagedEncryptionAlgorithm::Aes256),
                checksum_algorithm: None,
                checksum_type: None,
                lifecycle_abort: None,
                sse_customer: None,
            },
        );
        assert_eq!(
            find_header(&resp, "x-amz-server-side-encryption"),
            Some("AES256")
        );
    }

    #[test]
    fn list_parts_response_includes_lifecycle_abort_headers() {
        let resp = S3Response::list_parts(
            "bucket",
            "key",
            "upload-1",
            None,
            1000,
            &ListPartsResult {
                parts: Vec::new(),
                is_truncated: false,
                next_part_number_marker: None,
                checksum_algorithm: None,
                checksum_type: None,
                lifecycle_abort: Some(LifecycleAbortHeaders {
                    abort_time_millis: 1_705_321_845_000,
                    rule_id: Some("abort upload".to_string()),
                }),
            },
        );
        assert_eq!(
            find_header(&resp, "x-amz-abort-date"),
            Some("Mon, 15 Jan 2024 12:30:45 GMT")
        );
        assert_eq!(
            find_header(&resp, "x-amz-abort-rule-id"),
            Some("abort%20upload")
        );
        assert_eq!(find_header(&resp, "Content-Type"), Some("application/xml"));
        assert_eq!(find_header(&resp, "Content-Length"), None);
    }

    #[test]
    fn complete_multipart_upload_response_includes_lifecycle_expiration_header() {
        let resp = S3Response::complete_multipart_upload(
            "bucket",
            "key",
            &CompleteMultipartUploadResult {
                etag: "\"etag\"".to_string(),
                version_id: VersionId::Null,
                managed_encryption: None,
                checksum_algorithm: None,
                checksum_type: None,
                checksum_value: None,
                lifecycle_expiration: Some(LifecycleExpirationHeader {
                    expiry_time_millis: 1_705_321_845_000,
                    rule_id: Some("complete".to_string()),
                }),
            },
        );
        assert_eq!(
            find_header(&resp, "x-amz-expiration"),
            Some("expiry-date=\"Mon, 15 Jan 2024 12:30:45 GMT\", rule-id=\"complete\"")
        );
        assert_eq!(find_header(&resp, "Content-Type"), Some("application/xml"));
        assert_eq!(find_header(&resp, "Content-Length"), None);
        assert_eq!(find_header(&resp, "x-amz-checksum-algorithm"), None);
        assert_eq!(find_header(&resp, "x-amz-checksum-type"), None);
    }

    #[test]
    fn get_bucket_policy_status_response() {
        let resp = S3Response::get_bucket_policy_status(true);
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "Content-Type"), Some("application/xml"));
        let body = String::from_utf8(resp.body).unwrap();
        assert!(body.contains("<PolicyStatus"));
        assert!(body.contains("<IsPublic>true</IsPublic>"));
    }

    // ── list_objects_v2 ───────────────────────────────────────────────

    #[test]
    fn list_objects_v2_response() {
        let result = ListObjectsResult {
            objects: vec![ListEntry {
                key: "key1".into(),
                size: 42,
                etag: "\"etag1\"".into(),
                last_modified: 0,
                checksum_algorithm: Some(ChecksumAlgorithm::Crc32),
                checksum_type: Some(ChecksumType::FullObject),
            }],
            common_prefixes: vec![],
            is_truncated: false,
            next_continuation_token: None,
            owner_principal: "owner".into(),
            owner_canonical_id: CanonicalUserId::from_principal("owner"),
        };
        let resp = S3Response::list_objects_v2(
            "bucket",
            "us-east-1",
            Some("pre"),
            None,
            None,
            None,
            None,
            true,
            1000,
            &result,
        );
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "Content-Type"), Some("application/xml"));
        assert_eq!(find_header(&resp, "Content-Length"), None);
        assert_eq!(find_header(&resp, "x-amz-bucket-region"), Some("us-east-1"));
        assert!(resp.body.is_empty());
        assert!(resp.stream.is_some());
    }

    // ── list_objects_v1 ───────────────────────────────────────────────

    #[test]
    fn list_objects_v1_response() {
        let result = ListObjectsResult {
            objects: vec![ListEntry {
                key: "key1".into(),
                size: 42,
                etag: "\"etag1\"".into(),
                last_modified: 0,
                checksum_algorithm: Some(ChecksumAlgorithm::Crc32),
                checksum_type: Some(ChecksumType::FullObject),
            }],
            common_prefixes: vec![],
            is_truncated: false,
            next_continuation_token: None,
            owner_principal: "owner".into(),
            owner_canonical_id: CanonicalUserId::from_principal("owner"),
        };
        let resp = S3Response::list_objects_v1(
            "bucket",
            "us-east-1",
            Some("pre"),
            None,
            None,
            None,
            1000,
            &result,
        );
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "Content-Type"), Some("application/xml"));
        assert_eq!(find_header(&resp, "Content-Length"), None);
        assert_eq!(find_header(&resp, "x-amz-bucket-region"), Some("us-east-1"));
        assert!(resp.body.is_empty());
        assert!(resp.stream.is_some());
    }

    #[test]
    fn get_bucket_acl_response_uses_canonical_owner_id() {
        let owner_canonical_id = CanonicalUserId::from_principal("owner");
        let result = GetBucketAclResult {
            owner_principal: "owner".into(),
            owner_canonical_id: owner_canonical_id.clone(),
            acl_grants: AclGrants::new(vec![
                AclGrant::new(
                    AclGrantee::CanonicalUser(owner_canonical_id.clone()),
                    AclPermission::FullControl,
                ),
                AclGrant::new(AclGrantee::AllUsers, AclPermission::Read),
            ]),
        };
        let grants = vec![
            xml::RenderedAclGrant {
                grantee: AclGrantee::CanonicalUser(owner_canonical_id.clone()),
                permission: AclPermission::FullControl,
                display_name: Some("owner".to_string()),
            },
            xml::RenderedAclGrant {
                grantee: AclGrantee::AllUsers,
                permission: AclPermission::Read,
                display_name: None,
            },
        ];
        let resp = S3Response::get_bucket_acl(&result, "owner", &grants);
        assert_eq!(resp.status_code, 200);
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains(owner_canonical_id.as_str()));
        assert!(!body.contains("<DisplayName>"));
    }

    // ── error ─────────────────────────────────────────────────────────

    #[test]
    fn error_response_404() {
        let err = ServerError::BucketNotFound { name: "b".into() };
        let resp = S3Response::error(&err, "/b");
        assert_eq!(resp.status_code, 404);
        assert_eq!(find_header(&resp, "Content-Type"), Some("application/xml"));
        assert_eq!(find_header(&resp, "Content-Length"), None);
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("NoSuchBucket"));
    }

    #[test]
    fn error_response_403() {
        let err = ServerError::Auth(auth::AuthError::MissingAuth);
        let resp = S3Response::error(&err, "/");
        assert_eq!(resp.status_code, 403);
        assert_eq!(find_header(&resp, "Content-Type"), Some("application/xml"));
        assert_eq!(find_header(&resp, "Content-Length"), None);
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("AccessDenied"));
    }

    #[test]
    fn error_response_500() {
        let err = ServerError::Store(storage::StoreError::NotFound);
        let resp = S3Response::error(&err, "/x");
        assert_eq!(resp.status_code, 500);
        assert_eq!(find_header(&resp, "Content-Type"), Some("application/xml"));
        assert_eq!(find_header(&resp, "Content-Length"), None);
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("InternalError"));
        assert!(body.contains("We encountered an internal error. Please try again."));
        assert!(!body.contains("shard not found"));
    }

    #[test]
    fn error_response_internal_reason_is_sanitized() {
        let err = ServerError::InternalError {
            reason: "sqlite path /tmp/secret.db".to_string(),
        };
        let resp = S3Response::error(&err, "/x");
        assert_eq!(resp.status_code, 500);
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("InternalError"));
        assert!(body.contains("We encountered an internal error. Please try again."));
        assert!(!body.contains("/tmp/secret.db"));
    }

    #[test]
    fn error_response_metadata_blob_reason_is_sanitized() {
        let err = ServerError::MetadataBlobError {
            reason: "invalid metadata bytes: 0xFF".to_string(),
        };
        let resp = S3Response::error(&err, "/x");
        assert_eq!(resp.status_code, 500);
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("InternalError"));
        assert!(body.contains("We encountered an internal error. Please try again."));
        assert!(!body.contains("0xFF"));
    }

    #[test]
    fn slow_down_response_has_retry_after() {
        let resp = S3Response::error(&ServerError::SlowDown, "/");
        assert_eq!(resp.status_code, 503);
        assert_eq!(find_header(&resp, "Retry-After"), Some("1"));
        assert_eq!(find_header(&resp, "Content-Length"), None);
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("SlowDown"));
    }

    #[test]
    fn error_response_has_xml_content_type() {
        let err = ServerError::MethodNotAllowed;
        let resp = S3Response::error(&err, "/");
        assert_eq!(find_header(&resp, "Content-Type"), Some("application/xml"));
        assert_eq!(find_header(&resp, "Content-Length"), None);
        assert!(resp.stream.is_some());
    }

    // ── delete_objects ───────────────────────────────────────────────

    #[test]
    fn delete_objects_response() {
        use crate::coordinator::{DeleteObjectsResult, DeletedObject};
        let result = DeleteObjectsResult {
            deleted: vec![DeletedObject {
                key: "key1".into(),
                version_id: VersionId::Null,
                delete_marker: false,
            }],
            errors: vec![],
        };
        let resp = S3Response::delete_objects(&result, false);
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "Content-Type"), Some("application/xml"));
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("DeleteResult"));
        assert!(body.contains("key1"));
    }

    // ── list_object_versions ────────────────────────────────────────

    #[test]
    fn list_object_versions_response() {
        use crate::coordinator::{ListObjectVersionsResult, VersionEntry};
        let result = ListObjectVersionsResult {
            versions: vec![VersionEntry {
                key: "key1".into(),
                version_id: VersionId::Null,
                is_latest: true,
                size: 42,
                etag: "\"etag1\"".into(),
                last_modified: 0,
                is_delete_marker: false,
                checksum_algorithm: None,
                checksum_type: None,
            }],
            is_truncated: false,
            next_key_marker: None,
            next_version_id_marker: None,
            owner_principal: "owner".into(),
            owner_canonical_id: CanonicalUserId::from_principal("owner"),
        };
        let resp = S3Response::list_object_versions("bucket", None, None, None, 1000, &result);
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "Content-Type"), Some("application/xml"));
        assert_eq!(find_header(&resp, "Content-Length"), None);
        assert!(resp.body.is_empty());
        assert!(resp.stream.is_some());
    }

    // ── parse_http_date ────────────────────────────────────────────

    #[test]
    fn parse_http_date_epoch() {
        assert_eq!(parse_http_date("Thu, 01 Jan 1970 00:00:00 GMT"), Some(0));
    }

    #[test]
    fn parse_http_date_known_date() {
        assert_eq!(
            parse_http_date("Mon, 15 Jan 2024 12:30:45 GMT"),
            Some(1705321845000)
        );
    }

    #[test]
    fn parse_http_date_round_trip() {
        let millis = 1705321845000u64;
        let formatted = format_http_date(millis);
        assert_eq!(parse_http_date(&formatted), Some(millis));
    }

    #[test]
    fn parse_http_date_round_trip_epoch() {
        let formatted = format_http_date(0);
        assert_eq!(parse_http_date(&formatted), Some(0));
    }

    #[test]
    fn parse_http_date_invalid() {
        assert_eq!(parse_http_date("not a date"), None);
        assert_eq!(parse_http_date(""), None);
    }

    #[test]
    fn date_to_days_epoch() {
        assert_eq!(date_to_days(1970, 1, 1), 0);
    }

    #[test]
    fn date_to_days_round_trip() {
        for d in [0i64, 1, 365, 10957, 11016, 19782] {
            let (y, m, day) = days_to_date(d);
            assert_eq!(date_to_days(y, m, day), d, "failed round-trip for day {d}");
        }
    }

    // ── not_modified / precondition_failed ────────────────────────

    #[test]
    fn not_modified_response_has_etag_and_last_modified() {
        let resp = S3Response::not_modified("\"abcdef1234567890\"", 1705321845000);
        assert_eq!(resp.status_code, 304);
        assert_eq!(find_header(&resp, "ETag"), Some("\"abcdef1234567890\""));
        assert_eq!(
            find_header(&resp, "Last-Modified"),
            Some("Mon, 15 Jan 2024 12:30:45 GMT")
        );
        assert!(resp.body.is_empty());
    }

    #[test]
    fn precondition_failed_response_412() {
        let resp = S3Response::precondition_failed();
        assert_eq!(resp.status_code, 412);
        assert_eq!(find_header(&resp, "Content-Type"), Some("application/xml"));
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("PreconditionFailed"));
    }
}
