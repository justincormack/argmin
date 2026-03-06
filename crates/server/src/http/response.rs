/// Build HTTP responses for S3 operations.
use crate::coordinator::{
    CopyObjectResult, DeleteObjectResult, DeleteObjectsResult, GetObjectPartResult,
    GetObjectRangeResult, GetObjectResult, HeadObjectPartResult, HeadObjectResult,
    ListMultipartUploadsResult, ListObjectVersionsResult, ListObjectsResult, ListPartsResult,
    PutObjectResult,
};
use crate::error::ServerError;
use storage::{BucketInfo, ChecksumAlgorithm, ChecksumType};

use super::xml;

/// Format a version_id for S3 API responses.
/// version_id 0 is the null version (displayed as "null").
/// Non-zero version IDs are displayed as decimal strings.
pub fn format_version_id(version_id: u64) -> String {
    if version_id == 0 {
        "null".to_string()
    } else {
        version_id.to_string()
    }
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
                encoded.push_str(&format!("{:02X}", byte));
            }
        }
    }
    encoded.push_str("?=");
    encoded
}

/// An HTTP response to send back.
pub struct S3Response {
    pub status_code: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl S3Response {
    fn new(status_code: u16) -> Self {
        Self {
            status_code,
            headers: Vec::new(),
            body: Vec::new(),
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
        self.headers
            .push(("Content-Type".to_string(), "application/xml".to_string()));
        self.headers
            .push(("Content-Length".to_string(), self.body.len().to_string()));
        self
    }

    fn data_body(mut self, data: Vec<u8>) -> Self {
        self.headers
            .push(("Content-Length".to_string(), data.len().to_string()));
        self.body = data;
        self
    }

    /// Build a response for a successful PutObject.
    pub fn put_object(result: &PutObjectResult) -> Self {
        let mut resp = Self::new(200).header("ETag", &result.etag);
        if result.version_id != 0 {
            let vid = format_version_id(result.version_id);
            resp = resp.header("x-amz-version-id", &vid);
        }
        resp
    }

    /// Build a response for a successful POST Object.
    ///
    /// `success_status`: one of 200, 201, 204 (default).
    /// For 201, an XML body with bucket/key/etag is returned.
    pub fn post_object(
        result: &PutObjectResult,
        bucket: &str,
        key: &str,
        success_status: u16,
    ) -> Self {
        let status = match success_status {
            200 | 201 => success_status,
            _ => 204,
        };
        let mut resp = if status == 201 {
            let body = xml::post_response_xml(bucket, key, &result.etag);
            Self::new(201).xml_body(body)
        } else {
            Self::new(status)
        };
        resp = resp.header("ETag", &result.etag);
        if result.version_id != 0 {
            let vid = format_version_id(result.version_id);
            resp = resp.header("x-amz-version-id", &vid);
        }
        resp
    }

    /// Build a response for a successful CopyObject.
    pub fn copy_object(result: &CopyObjectResult) -> Self {
        let body = xml::copy_object_result_xml(&result.etag, result.last_modified);
        let mut resp = Self::new(200).xml_body(body);
        if result.version_id != 0 {
            let vid = format_version_id(result.version_id);
            resp.headers.push(("x-amz-version-id".to_string(), vid));
        }
        resp
    }

    /// Build a response for a successful GetObject.
    /// If `checksum_mode` is `Some("ENABLED")`, include stored checksum headers.
    pub fn get_object(result: GetObjectResult, checksum_mode: Option<&str>) -> Self {
        let mut resp = Self::new(200)
            .header("ETag", &result.etag)
            .header("Last-Modified", &format_http_date(result.last_modified))
            .header("Accept-Ranges", "bytes");
        if result.version_id != 0 {
            let vid = format_version_id(result.version_id);
            resp = resp.header("x-amz-version-id", &vid);
        }

        // Add metadata headers
        if let Some(ct) = result.metadata.get("content-type") {
            resp = resp.header("Content-Type", ct);
        } else {
            resp = resp.header("Content-Type", "application/octet-stream");
        }
        if let Some(ce) = result.metadata.get("content-encoding") {
            resp = resp.header("Content-Encoding", ce);
        }
        if let Some(cc) = result.metadata.get("cache-control") {
            resp = resp.header("Cache-Control", cc);
        }
        if let Some(cd) = result.metadata.get("content-disposition") {
            resp = resp.header("Content-Disposition", cd);
        }
        if let Some(cl) = result.metadata.get("content-language") {
            resp = resp.header("Content-Language", cl);
        }
        if let Some(ex) = result.metadata.get("expires") {
            resp = resp.header("Expires", ex);
        }

        // x-amz-meta-* headers
        for entry in &result.metadata.entries {
            if entry.key.starts_with("x-amz-meta-") {
                resp = resp.meta_header(&entry.key, &entry.value);
            }
        }

        // Checksum headers (only when ChecksumMode=ENABLED)
        if checksum_mode
            .map(|m| m.eq_ignore_ascii_case("ENABLED"))
            .unwrap_or(false)
        {
            for entry in result.metadata.checksum_entries() {
                resp = resp.header(&entry.key, &entry.value);
            }
            if let Some(ct) = result.metadata.get("x-amz-checksum-type") {
                resp = resp.header("x-amz-checksum-type", ct);
            }
        }

        resp.data_body(result.data)
    }

    /// Build a response for a successful HeadObject.
    /// If `checksum_mode` is `Some("ENABLED")`, include stored checksum headers.
    pub fn head_object(result: &HeadObjectResult, checksum_mode: Option<&str>) -> Self {
        let mut resp = Self::new(200)
            .header("ETag", &result.etag)
            .header("Content-Length", &result.size.to_string())
            .header("Last-Modified", &format_http_date(result.last_modified))
            .header("Accept-Ranges", "bytes");
        if result.version_id != 0 {
            let vid = format_version_id(result.version_id);
            resp = resp.header("x-amz-version-id", &vid);
        }

        if let Some(ct) = result.metadata.get("content-type") {
            resp = resp.header("Content-Type", ct);
        } else {
            resp = resp.header("Content-Type", "application/octet-stream");
        }
        if let Some(ce) = result.metadata.get("content-encoding") {
            resp = resp.header("Content-Encoding", ce);
        }
        if let Some(cc) = result.metadata.get("cache-control") {
            resp = resp.header("Cache-Control", cc);
        }
        if let Some(cd) = result.metadata.get("content-disposition") {
            resp = resp.header("Content-Disposition", cd);
        }
        if let Some(cl) = result.metadata.get("content-language") {
            resp = resp.header("Content-Language", cl);
        }
        if let Some(ex) = result.metadata.get("expires") {
            resp = resp.header("Expires", ex);
        }

        for entry in &result.metadata.entries {
            if entry.key.starts_with("x-amz-meta-") {
                resp = resp.meta_header(&entry.key, &entry.value);
            }
        }

        // Checksum headers (only when ChecksumMode=ENABLED)
        if checksum_mode
            .map(|m| m.eq_ignore_ascii_case("ENABLED"))
            .unwrap_or(false)
        {
            for entry in result.metadata.checksum_entries() {
                resp = resp.header(&entry.key, &entry.value);
            }
            if let Some(ct) = result.metadata.get("x-amz-checksum-type") {
                resp = resp.header("x-amz-checksum-type", ct);
            }
        }

        resp
    }

    /// Build a response for HeadObject with partNumber.
    /// Returns 200 with Content-Length of the part and x-amz-mp-parts-count.
    pub fn head_object_part(result: &HeadObjectPartResult) -> Self {
        let mut resp = Self::new(200)
            .header("ETag", &result.etag)
            .header("Content-Length", &result.part_size.to_string())
            .header("Last-Modified", &format_http_date(result.last_modified))
            .header("Accept-Ranges", "bytes")
            .header("x-amz-mp-parts-count", &result.parts_count.to_string());
        if result.version_id != 0 {
            let vid = format_version_id(result.version_id);
            resp = resp.header("x-amz-version-id", &vid);
        }

        if let Some(ct) = result.metadata.get("content-type") {
            resp = resp.header("Content-Type", ct);
        } else {
            resp = resp.header("Content-Type", "application/octet-stream");
        }
        if let Some(ce) = result.metadata.get("content-encoding") {
            resp = resp.header("Content-Encoding", ce);
        }
        if let Some(cc) = result.metadata.get("cache-control") {
            resp = resp.header("Cache-Control", cc);
        }
        if let Some(cd) = result.metadata.get("content-disposition") {
            resp = resp.header("Content-Disposition", cd);
        }
        if let Some(cl) = result.metadata.get("content-language") {
            resp = resp.header("Content-Language", cl);
        }
        if let Some(ex) = result.metadata.get("expires") {
            resp = resp.header("Expires", ex);
        }

        for entry in &result.metadata.entries {
            if entry.key.starts_with("x-amz-meta-") {
                resp = resp.meta_header(&entry.key, &entry.value);
            }
        }

        // Per-part checksum (always emitted for part-level requests)
        if let Some((header_name, b64_value)) = &result.checksum {
            resp = resp.header(header_name, b64_value);
        }
        if let Some(ct) = result.metadata.get("x-amz-checksum-type") {
            resp = resp.header("x-amz-checksum-type", ct);
        }

        resp
    }

    /// Build a response for a successful range GetObject (206 Partial Content).
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
        if result.version_id != 0 {
            let vid = format_version_id(result.version_id);
            resp = resp.header("x-amz-version-id", &vid);
        }

        if let Some(ct) = result.metadata.get("content-type") {
            resp = resp.header("Content-Type", ct);
        } else {
            resp = resp.header("Content-Type", "application/octet-stream");
        }
        if let Some(ce) = result.metadata.get("content-encoding") {
            resp = resp.header("Content-Encoding", ce);
        }
        if let Some(cc) = result.metadata.get("cache-control") {
            resp = resp.header("Cache-Control", cc);
        }
        if let Some(cd) = result.metadata.get("content-disposition") {
            resp = resp.header("Content-Disposition", cd);
        }
        if let Some(cl) = result.metadata.get("content-language") {
            resp = resp.header("Content-Language", cl);
        }
        if let Some(ex) = result.metadata.get("expires") {
            resp = resp.header("Expires", ex);
        }

        for entry in &result.metadata.entries {
            if entry.key.starts_with("x-amz-meta-") {
                resp = resp.meta_header(&entry.key, &entry.value);
            }
        }

        resp.data_body(result.data)
    }

    /// Build a response for a part-level GetObject (206 Partial Content).
    /// Per-part checksum and checksum-type are always emitted (Ceph/AWS
    /// return them without requiring ChecksumMode=ENABLED on part GETs).
    pub fn get_object_part(result: GetObjectPartResult) -> Self {
        let mut resp = Self::new(206)
            .header("ETag", &result.etag)
            .header("Last-Modified", &format_http_date(result.last_modified))
            .header("Accept-Ranges", "bytes")
            .header("x-amz-mp-parts-count", &result.parts_count.to_string());
        // Only emit Content-Range for non-empty parts; a zero-byte part has
        // no valid byte range to express.
        if !result.data.is_empty() {
            let content_range = format!(
                "bytes {}-{}/{}",
                result.part_start, result.part_end, result.size
            );
            resp = resp.header("Content-Range", &content_range);
        }
        if result.version_id != 0 {
            let vid = format_version_id(result.version_id);
            resp = resp.header("x-amz-version-id", &vid);
        }

        if let Some(ct) = result.metadata.get("content-type") {
            resp = resp.header("Content-Type", ct);
        } else {
            resp = resp.header("Content-Type", "application/octet-stream");
        }
        if let Some(ce) = result.metadata.get("content-encoding") {
            resp = resp.header("Content-Encoding", ce);
        }
        if let Some(cc) = result.metadata.get("cache-control") {
            resp = resp.header("Cache-Control", cc);
        }
        if let Some(cd) = result.metadata.get("content-disposition") {
            resp = resp.header("Content-Disposition", cd);
        }
        if let Some(cl) = result.metadata.get("content-language") {
            resp = resp.header("Content-Language", cl);
        }
        if let Some(ex) = result.metadata.get("expires") {
            resp = resp.header("Expires", ex);
        }

        for entry in &result.metadata.entries {
            if entry.key.starts_with("x-amz-meta-") {
                resp = resp.meta_header(&entry.key, &entry.value);
            }
        }

        // Per-part checksum (always emitted for part-level GETs)
        if let Some((header_name, b64_value)) = &result.checksum {
            resp = resp.header(header_name, b64_value);
        }
        // Checksum type (e.g. COMPOSITE, FULL_OBJECT)
        if let Some(ct) = result.metadata.get("x-amz-checksum-type") {
            resp = resp.header("x-amz-checksum-type", ct);
        }

        resp.data_body(result.data)
    }

    /// Build a 416 Range Not Satisfiable response.
    pub fn range_not_satisfiable(total_size: u64) -> Self {
        let content_range = format!("bytes */{}", total_size);
        let body = xml::error_xml(
            "InvalidRange",
            "The requested range is not satisfiable",
            "",
            "request-id",
        );
        Self::new(416)
            .header("Content-Range", &content_range)
            .xml_body(body)
    }

    /// Build a response for DeleteObject (204 No Content).
    pub fn delete_object(result: &DeleteObjectResult) -> Self {
        let mut resp = Self::new(204);
        if result.version_id != 0 {
            let vid = format_version_id(result.version_id);
            resp = resp.header("x-amz-version-id", &vid);
        }
        if result.delete_marker {
            resp = resp.header("x-amz-delete-marker", "true");
        }
        resp
    }

    /// Build a response for CreateBucket.
    pub fn create_bucket(location: &str) -> Self {
        Self::new(200).header("Location", &format!("/{}", location))
    }

    /// Build a response for DeleteBucket.
    pub fn delete_bucket() -> Self {
        Self::new(204)
    }

    /// Build a response for HeadBucket.
    pub fn head_bucket(info: &BucketInfo) -> Self {
        let _ = info; // We could add x-amz-bucket-region etc.
        Self::new(200)
    }

    /// Build a response for PutBucketVersioning.
    pub fn put_bucket_versioning() -> Self {
        Self::new(200)
    }

    /// Build a response for GetBucketVersioning.
    pub fn get_bucket_versioning(state: u8) -> Self {
        let body = xml::get_bucket_versioning_xml(state);
        Self::new(200).xml_body(body)
    }

    /// Build a response for ListBuckets.
    pub fn list_buckets(buckets: &[BucketInfo], owner_principal: &str) -> Self {
        let body = xml::list_buckets_xml(buckets, owner_principal);
        Self::new(200).xml_body(body)
    }

    /// Build a response for ListObjectsV2.
    #[allow(clippy::too_many_arguments)]
    pub fn list_objects_v2(
        bucket: &str,
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
        Self::new(200).xml_body(body)
    }

    /// Build a response for ListObjects v1.
    pub fn list_objects_v1(
        bucket: &str,
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
        Self::new(200).xml_body(body)
    }

    /// Build a response for DeleteObjects (batch delete).
    pub fn delete_objects(result: &DeleteObjectsResult, quiet: bool) -> Self {
        let body = xml::delete_objects_result_xml(&result.deleted, &result.errors, quiet);
        Self::new(200).xml_body(body)
    }

    /// Build a response for ListObjectVersions.
    pub fn list_object_versions(
        bucket: &str,
        prefix: Option<&str>,
        key_marker: Option<&str>,
        max_keys: u32,
        result: &ListObjectVersionsResult,
    ) -> Self {
        let body = xml::list_object_versions_xml(bucket, prefix, key_marker, max_keys, result);
        Self::new(200).xml_body(body)
    }

    /// Build a 304 Not Modified response with ETag and Last-Modified headers, no body.
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
        Self::new(412).xml_body(body)
    }

    /// Build a response for PutBucketCors (200 OK, no body).
    pub fn put_bucket_cors() -> Self {
        Self::new(200)
    }

    /// Build a response for GetBucketCors (200 OK, XML body).
    pub fn get_bucket_cors(config_xml: &str) -> Self {
        Self::new(200).xml_body(config_xml.to_string())
    }

    /// Build a response for DeleteBucketCors (204 No Content).
    pub fn delete_bucket_cors() -> Self {
        Self::new(204)
    }

    /// Build a response for PutBucketTagging (200 OK, no body).
    pub fn put_bucket_tagging() -> Self {
        Self::new(200)
    }

    /// Build a response for GetBucketTagging (200 OK, XML body).
    pub fn get_bucket_tagging(xml: &str) -> Self {
        Self::new(200).xml_body(xml.to_string())
    }

    /// Build a response for DeleteBucketTagging (204 No Content).
    pub fn delete_bucket_tagging() -> Self {
        Self::new(204)
    }

    /// Build a response for PutObjectTagging (200 OK, no body).
    pub fn put_object_tagging() -> Self {
        Self::new(200)
    }

    /// Build a response for GetObjectTagging (200 OK, XML body).
    pub fn get_object_tagging(xml: &str) -> Self {
        Self::new(200).xml_body(xml.to_string())
    }

    /// Build a response for DeleteObjectTagging (204 No Content).
    pub fn delete_object_tagging() -> Self {
        Self::new(204)
    }

    /// Build a response for PutBucketPublicAccessBlock (200 OK, no body).
    pub fn put_bucket_public_access_block() -> Self {
        Self::new(200)
    }

    /// Build a response for GetBucketPublicAccessBlock (200 OK, XML body).
    pub fn get_bucket_public_access_block(config_xml: &str) -> Self {
        Self::new(200).xml_body(config_xml.to_string())
    }

    /// Build a response for DeleteBucketPublicAccessBlock (204 No Content).
    pub fn delete_bucket_public_access_block() -> Self {
        Self::new(204)
    }

    /// Build a response for PutBucketOwnershipControls (200 OK, no body).
    pub fn put_bucket_ownership_controls() -> Self {
        Self::new(200)
    }

    /// Build a response for GetBucketOwnershipControls (200 OK, XML body).
    pub fn get_bucket_ownership_controls(config_xml: &str) -> Self {
        Self::new(200).xml_body(config_xml.to_string())
    }

    /// Build a response for DeleteBucketOwnershipControls (204 No Content).
    pub fn delete_bucket_ownership_controls() -> Self {
        Self::new(204)
    }

    /// Build a response for GetObjectAttributes (200 OK, XML body).
    pub fn get_object_attributes(body_xml: &str, last_modified: u64, version_id: u64) -> Self {
        let mut resp = Self::new(200)
            .xml_body(body_xml.to_string())
            .header("Last-Modified", &format_http_date(last_modified));
        if version_id != 0 {
            resp = resp.header("x-amz-version-id", &format_version_id(version_id));
        }
        resp
    }

    /// Build a response for PutBucketAcl (200 OK, no body).
    pub fn put_bucket_acl() -> Self {
        Self::new(200)
    }

    /// Build a response for CreateMultipartUpload (200 OK, XML body).
    pub fn create_multipart_upload(
        bucket: &str,
        key: &str,
        upload_id: &str,
        checksum_algorithm: Option<storage::ChecksumAlgorithm>,
        checksum_type: Option<storage::ChecksumType>,
    ) -> Self {
        let body = xml::initiate_multipart_upload_xml(
            bucket,
            key,
            upload_id,
            checksum_algorithm.map(|a| a.as_str()),
            checksum_type.map(|t| t.as_str()),
        );
        Self::new(200).xml_body(body)
    }

    /// Build a response for UploadPart (200 OK, ETag header, optional checksum).
    pub fn upload_part(
        etag: &str,
        checksum_algorithm: Option<storage::ChecksumAlgorithm>,
        checksum_bytes: Option<&[u8]>,
    ) -> Self {
        let mut resp = Self::new(200).header("ETag", etag);
        if let (Some(algo), Some(bytes)) = (checksum_algorithm, checksum_bytes) {
            use base64::Engine;
            let b64 = base64::engine::general_purpose::STANDARD.encode(bytes);
            resp = resp.header(algo.header_name(), &b64);
        }
        resp
    }

    /// Build a response for UploadPartCopy (200 OK, XML body with CopyPartResult).
    pub fn upload_part_copy(etag: &str, last_modified: u64) -> Self {
        let body = xml::copy_part_result_xml(etag, last_modified);
        Self::new(200).xml_body(body)
    }

    /// Build a response for CompleteMultipartUpload (200 OK, XML body).
    pub fn complete_multipart_upload(
        bucket: &str,
        key: &str,
        etag: &str,
        version_id: u64,
        checksum_algorithm: Option<ChecksumAlgorithm>,
        checksum_type: Option<ChecksumType>,
        checksum_value: Option<&str>,
    ) -> Self {
        let body = xml::complete_multipart_upload_xml(
            bucket,
            key,
            etag,
            checksum_algorithm,
            checksum_value,
        );
        let mut resp = Self::new(200).xml_body(body);
        if version_id != 0 {
            resp = resp.header("x-amz-version-id", &format_version_id(version_id));
        }
        if let Some(algo) = checksum_algorithm {
            resp = resp.header("x-amz-checksum-algorithm", algo.as_str());
        }
        if let Some(ct) = checksum_type {
            resp = resp.header("x-amz-checksum-type", ct.as_str());
        }
        resp
    }

    /// Build a response for AbortMultipartUpload (204 No Content).
    pub fn abort_multipart_upload() -> Self {
        Self::new(204)
    }

    /// Build a response for ListMultipartUploads (200 OK, XML body).
    pub fn list_multipart_uploads(
        bucket: &str,
        prefix: Option<&str>,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
        max_uploads: u32,
        result: &ListMultipartUploadsResult,
    ) -> Self {
        let body = xml::list_multipart_uploads_xml(
            bucket,
            prefix,
            key_marker,
            upload_id_marker,
            max_uploads,
            result,
        );
        Self::new(200).xml_body(body)
    }

    /// Build a response for ListParts (200 OK, XML body).
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
        Self::new(200).xml_body(body)
    }

    /// Build a 200 response for a CORS preflight (headers added by caller).
    pub fn cors_preflight() -> Self {
        Self::new(200)
    }

    /// Build a 403 Forbidden response.
    pub fn forbidden() -> Self {
        let body = xml::error_xml("AccessDenied", "Access Denied", "", "request-id");
        Self::new(403).xml_body(body)
    }

    /// Build an error response.
    pub fn error(err: &ServerError, resource: &str) -> Self {
        // Special cases that need extra XML elements
        match err {
            ServerError::XAmzContentSHA256Mismatch {
                client_hash,
                server_hash,
            } => {
                let body = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                     <Error>\
                     <Code>XAmzContentSHA256Mismatch</Code>\
                     <Message>The provided &apos;x-amz-content-sha256&apos; header does not match what was computed.</Message>\
                     <ClientComputedContentSHA256>{}</ClientComputedContentSHA256>\
                     <S3ComputedContentSHA256>{}</S3ComputedContentSHA256>\
                     <Resource>{}</Resource>\
                     <RequestId>request-id</RequestId>\
                     </Error>",
                    xml::xml_escape(client_hash),
                    xml::xml_escape(server_hash),
                    xml::xml_escape(resource),
                );
                return Self::new(400).xml_body(body);
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
                     <RequestId>request-id</RequestId>\
                     </Error>",
                    xml::xml_escape(&headers_str),
                    xml::xml_escape(resource),
                );
                return Self::new(403).xml_body(body);
            }
            _ => {}
        }

        let fallback;
        let message = match err {
            ServerError::InvalidRequest { reason } => reason.as_str(),
            ServerError::InvalidArgument { reason } => reason.as_str(),
            ServerError::InvalidBucketName { reason } => reason.as_str(),
            ServerError::MetadataBlobError { reason } => reason.as_str(),
            _ => {
                fallback = err.to_string();
                fallback.as_str()
            }
        };
        let body = xml::error_xml(err.s3_error_code(), message, resource, "request-id");
        Self::new(err.http_status()).xml_body(body)
    }
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
    let y = yoe as i64 + era * 400;
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
    era * 146_097 + doe as i64 - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::{
        GetObjectResult, HeadObjectResult, ListEntry, ListObjectsResult, PutObjectResult,
    };
    use crate::metadata_blob::{MetadataBlob, MetadataEntry};

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
            version_id: 0,
        };
        let resp = S3Response::put_object(&result);
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "ETag"), Some("\"abc123\""));
        // version_id=0 means unversioned — no x-amz-version-id header
        assert_eq!(find_header(&resp, "x-amz-version-id"), None);
    }

    #[test]
    fn put_object_response_versioned() {
        let result = PutObjectResult {
            etag: "\"abc123\"".to_string(),
            version_id: 42,
        };
        let resp = S3Response::put_object(&result);
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "ETag"), Some("\"abc123\""));
        assert_eq!(find_header(&resp, "x-amz-version-id"), Some("42"));
    }

    // ── get_object ────────────────────────────────────────────────────

    #[test]
    fn get_object_with_content_type() {
        let result = GetObjectResult {
            data: b"hello".to_vec(),
            metadata: MetadataBlob {
                entries: vec![MetadataEntry {
                    key: "content-type".into(),
                    value: "text/plain".into(),
                }],
            },
            etag: "\"etag\"".into(),
            size: 5,
            last_modified: 0,
            version_id: 0,
            tags: None,
        };
        let resp = S3Response::get_object(result, None);
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "Content-Type"), Some("text/plain"));
        assert_eq!(resp.body, b"hello");
    }

    #[test]
    fn get_object_default_content_type() {
        let result = GetObjectResult {
            data: b"data".to_vec(),
            metadata: MetadataBlob::new(),
            etag: "\"etag\"".into(),
            size: 4,
            last_modified: 0,
            version_id: 0,
            tags: None,
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
            data: vec![],
            metadata: MetadataBlob {
                entries: vec![MetadataEntry {
                    key: "x-amz-meta-author".into(),
                    value: "alice".into(),
                }],
            },
            etag: "\"e\"".into(),
            size: 0,
            last_modified: 0,
            version_id: 0,
            tags: None,
        };
        let resp = S3Response::get_object(result, None);
        assert_eq!(find_header(&resp, "x-amz-meta-author"), Some("alice"));
    }

    #[test]
    fn get_object_with_all_standard_metadata() {
        let result = GetObjectResult {
            data: vec![],
            metadata: MetadataBlob {
                entries: vec![
                    MetadataEntry {
                        key: "content-type".into(),
                        value: "text/html".into(),
                    },
                    MetadataEntry {
                        key: "content-encoding".into(),
                        value: "gzip".into(),
                    },
                    MetadataEntry {
                        key: "cache-control".into(),
                        value: "max-age=3600".into(),
                    },
                    MetadataEntry {
                        key: "content-disposition".into(),
                        value: "attachment".into(),
                    },
                    MetadataEntry {
                        key: "content-language".into(),
                        value: "en-US".into(),
                    },
                    MetadataEntry {
                        key: "expires".into(),
                        value: "Thu, 01 Jan 2099 00:00:00 GMT".into(),
                    },
                ],
            },
            etag: "\"e\"".into(),
            size: 0,
            last_modified: 0,
            version_id: 0,
            tags: None,
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
            data: vec![],
            metadata: MetadataBlob {
                entries: vec![
                    MetadataEntry {
                        key: "x-amz-checksum-crc32".into(),
                        value: "AAAAAA==".into(),
                    },
                    MetadataEntry {
                        key: "x-amz-checksum-type".into(),
                        value: "FULL_OBJECT".into(),
                    },
                ],
            },
            etag: "\"e\"".into(),
            size: 0,
            last_modified: 0,
            version_id: 0,
            tags: None,
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
            data: vec![],
            metadata: MetadataBlob {
                entries: vec![
                    MetadataEntry {
                        key: "x-amz-checksum-crc32".into(),
                        value: "AAAAAA==".into(),
                    },
                    MetadataEntry {
                        key: "x-amz-checksum-type".into(),
                        value: "FULL_OBJECT".into(),
                    },
                ],
            },
            etag: "\"e\"".into(),
            size: 0,
            last_modified: 0,
            version_id: 0,
            tags: None,
        };
        let resp = S3Response::get_object(result, None);
        assert_eq!(find_header(&resp, "x-amz-checksum-crc32"), None);
        assert_eq!(find_header(&resp, "x-amz-checksum-type"), None);
    }

    // ── head_object ───────────────────────────────────────────────────

    #[test]
    fn head_object_response() {
        let result = HeadObjectResult {
            metadata: MetadataBlob {
                entries: vec![MetadataEntry {
                    key: "content-type".into(),
                    value: "image/png".into(),
                }],
            },
            etag: "\"etag\"".into(),
            size: 1024,
            last_modified: 0,
            version_id: 0,
            tags: None,
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
            metadata: MetadataBlob::new(),
            etag: "\"e\"".into(),
            size: 0,
            last_modified: 0,
            version_id: 0,
            tags: None,
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
            metadata: MetadataBlob {
                entries: vec![
                    MetadataEntry {
                        key: "content-encoding".into(),
                        value: "br".into(),
                    },
                    MetadataEntry {
                        key: "cache-control".into(),
                        value: "no-cache".into(),
                    },
                ],
            },
            etag: "\"e\"".into(),
            size: 10,
            last_modified: 0,
            version_id: 0,
            tags: None,
        };
        let resp = S3Response::head_object(&result, None);
        assert_eq!(find_header(&resp, "Content-Encoding"), Some("br"));
        assert_eq!(find_header(&resp, "Cache-Control"), Some("no-cache"));
    }

    #[test]
    fn head_object_with_amz_meta() {
        let result = HeadObjectResult {
            metadata: MetadataBlob {
                entries: vec![MetadataEntry {
                    key: "x-amz-meta-tag".into(),
                    value: "value".into(),
                }],
            },
            etag: "\"e\"".into(),
            size: 0,
            last_modified: 0,
            version_id: 0,
            tags: None,
        };
        let resp = S3Response::head_object(&result, None);
        assert_eq!(find_header(&resp, "x-amz-meta-tag"), Some("value"));
    }

    #[test]
    fn head_object_checksum_type_with_checksum_mode_enabled() {
        let result = HeadObjectResult {
            metadata: MetadataBlob {
                entries: vec![
                    MetadataEntry {
                        key: "x-amz-checksum-crc32".into(),
                        value: "AAAAAA==".into(),
                    },
                    MetadataEntry {
                        key: "x-amz-checksum-type".into(),
                        value: "COMPOSITE".into(),
                    },
                ],
            },
            etag: "\"e\"".into(),
            size: 0,
            last_modified: 0,
            version_id: 0,
            tags: None,
        };
        let resp = S3Response::head_object(&result, Some("ENABLED"));
        assert_eq!(find_header(&resp, "x-amz-checksum-crc32"), Some("AAAAAA=="));
        assert_eq!(find_header(&resp, "x-amz-checksum-type"), Some("COMPOSITE"));
    }

    #[test]
    fn head_object_checksum_type_omitted_without_checksum_mode() {
        let result = HeadObjectResult {
            metadata: MetadataBlob {
                entries: vec![
                    MetadataEntry {
                        key: "x-amz-checksum-crc32".into(),
                        value: "AAAAAA==".into(),
                    },
                    MetadataEntry {
                        key: "x-amz-checksum-type".into(),
                        value: "COMPOSITE".into(),
                    },
                ],
            },
            etag: "\"e\"".into(),
            size: 0,
            last_modified: 0,
            version_id: 0,
            tags: None,
        };
        let resp = S3Response::head_object(&result, None);
        assert_eq!(find_header(&resp, "x-amz-checksum-crc32"), None);
        assert_eq!(find_header(&resp, "x-amz-checksum-type"), None);
    }

    // ── delete_object ─────────────────────────────────────────────────

    #[test]
    fn delete_object_response() {
        use crate::coordinator::DeleteObjectResult;
        let result = DeleteObjectResult {
            version_id: 0,
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
            version_id: 5,
            delete_marker: true,
        };
        let resp = S3Response::delete_object(&result);
        assert_eq!(resp.status_code, 204);
        assert_eq!(find_header(&resp, "x-amz-version-id"), Some("5"));
        assert_eq!(find_header(&resp, "x-amz-delete-marker"), Some("true"));
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

    // ── head_bucket ───────────────────────────────────────────────────

    #[test]
    fn head_bucket_response() {
        let info = storage::BucketInfo {
            name: "b".into(),
            owner_principal: "owner".into(),
            created_at: 0,
            region: 0,
            versioning: 0,
            public_read: false,
            cors_config: None,
            tags: None,
            public_access_block: None,
            ownership_controls: None,
        };
        let resp = S3Response::head_bucket(&info);
        assert_eq!(resp.status_code, 200);
    }

    // ── list_buckets ──────────────────────────────────────────────────

    #[test]
    fn list_buckets_response() {
        let buckets = vec![storage::BucketInfo {
            name: "test-bucket".into(),
            owner_principal: "owner".into(),
            created_at: 1000,
            region: 0,
            versioning: 0,
            public_read: false,
            cors_config: None,
            tags: None,
            public_access_block: None,
            ownership_controls: None,
        }];
        let resp = S3Response::list_buckets(&buckets, "owner");
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "Content-Type"), Some("application/xml"));
        let body = String::from_utf8(resp.body).unwrap();
        assert!(body.contains("<?xml"));
        assert!(body.contains("test-bucket"));
        assert!(body.contains("ListAllMyBucketsResult"));
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
            }],
            common_prefixes: vec![],
            is_truncated: false,
            next_continuation_token: None,
            owner_principal: "owner".into(),
        };
        let resp = S3Response::list_objects_v2(
            "bucket",
            Some("pre"),
            None,
            None,
            None,
            None,
            false,
            1000,
            &result,
        );
        assert_eq!(resp.status_code, 200);
        let body = String::from_utf8(resp.body).unwrap();
        assert!(body.contains("ListBucketResult"));
        assert!(body.contains("key1"));
        assert!(body.contains("<Size>42</Size>"));
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
            }],
            common_prefixes: vec![],
            is_truncated: false,
            next_continuation_token: None,
            owner_principal: "owner".into(),
        };
        let resp =
            S3Response::list_objects_v1("bucket", Some("pre"), None, None, None, 1000, &result);
        assert_eq!(resp.status_code, 200);
        let body = String::from_utf8(resp.body).unwrap();
        assert!(body.contains("ListBucketResult"));
        assert!(body.contains("key1"));
        assert!(body.contains("<Marker/>"));
        assert!(!body.contains("<KeyCount>"));
    }

    // ── error ─────────────────────────────────────────────────────────

    #[test]
    fn error_response_404() {
        let err = ServerError::BucketNotFound { name: "b".into() };
        let resp = S3Response::error(&err, "/b");
        assert_eq!(resp.status_code, 404);
        let body = String::from_utf8(resp.body).unwrap();
        assert!(body.contains("NoSuchBucket"));
    }

    #[test]
    fn error_response_403() {
        let err = ServerError::Auth(auth::AuthError::MissingAuth);
        let resp = S3Response::error(&err, "/");
        assert_eq!(resp.status_code, 403);
        let body = String::from_utf8(resp.body).unwrap();
        assert!(body.contains("AccessDenied"));
    }

    #[test]
    fn error_response_500() {
        let err = ServerError::Store(storage::StoreError::NotFound);
        let resp = S3Response::error(&err, "/x");
        assert_eq!(resp.status_code, 500);
        let body = String::from_utf8(resp.body).unwrap();
        assert!(body.contains("InternalError"));
    }

    #[test]
    fn error_response_has_xml_content_type() {
        let err = ServerError::MethodNotAllowed;
        let resp = S3Response::error(&err, "/");
        assert_eq!(find_header(&resp, "Content-Type"), Some("application/xml"));
    }

    // ── delete_objects ───────────────────────────────────────────────

    #[test]
    fn delete_objects_response() {
        use crate::coordinator::{DeleteObjectsResult, DeletedObject};
        let result = DeleteObjectsResult {
            deleted: vec![DeletedObject {
                key: "key1".into(),
                version_id: 0,
                delete_marker: false,
            }],
            errors: vec![],
        };
        let resp = S3Response::delete_objects(&result, false);
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "Content-Type"), Some("application/xml"));
        let body = String::from_utf8(resp.body).unwrap();
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
                version_id: 0,
                is_latest: true,
                size: 42,
                etag: "\"etag1\"".into(),
                last_modified: 0,
                is_delete_marker: false,
            }],
            is_truncated: false,
            next_key_marker: None,
            next_version_id_marker: None,
        };
        let resp = S3Response::list_object_versions("bucket", None, None, 1000, &result);
        assert_eq!(resp.status_code, 200);
        let body = String::from_utf8(resp.body).unwrap();
        assert!(body.contains("ListVersionsResult"));
        assert!(body.contains("<VersionId>null</VersionId>"));
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
        let body = String::from_utf8(resp.body).unwrap();
        assert!(body.contains("PreconditionFailed"));
    }
}
