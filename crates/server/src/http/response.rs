/// Build HTTP responses for S3 operations.
use crate::coordinator::{
    DeleteObjectsResult, GetObjectRangeResult, GetObjectResult, HeadObjectResult,
    ListObjectsResult, PutObjectResult,
};
use crate::error::ServerError;
use storage::BucketInfo;

use super::xml;

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
        Self::new(200)
            .header("ETag", &result.etag)
            .header("x-amz-version-id", &result.version_id)
    }

    /// Build a response for a successful GetObject.
    pub fn get_object(result: GetObjectResult) -> Self {
        let mut resp = Self::new(200)
            .header("ETag", &result.etag)
            .header("Last-Modified", &format_http_date(result.last_modified))
            .header("Accept-Ranges", "bytes");

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
                resp = resp.header(&entry.key, &entry.value);
            }
        }

        resp.data_body(result.data)
    }

    /// Build a response for a successful HeadObject.
    pub fn head_object(result: &HeadObjectResult) -> Self {
        let mut resp = Self::new(200)
            .header("ETag", &result.etag)
            .header("Content-Length", &result.size.to_string())
            .header("Last-Modified", &format_http_date(result.last_modified))
            .header("Accept-Ranges", "bytes");

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

        for entry in &result.metadata.entries {
            if entry.key.starts_with("x-amz-meta-") {
                resp = resp.header(&entry.key, &entry.value);
            }
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
                resp = resp.header(&entry.key, &entry.value);
            }
        }

        resp.data_body(result.data)
    }

    /// Build a 416 Range Not Satisfiable response.
    pub fn range_not_satisfiable(total_size: u64) -> Self {
        let content_range = format!("bytes */{}", total_size);
        let body = xml::error_xml("InvalidRange", "The requested range is not satisfiable", "", "request-id");
        Self::new(416)
            .header("Content-Range", &content_range)
            .xml_body(body)
    }

    /// Build a response for DeleteObject (204 No Content).
    pub fn delete_object() -> Self {
        Self::new(204)
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

    /// Build a response for ListBuckets.
    pub fn list_buckets(buckets: &[BucketInfo]) -> Self {
        let body = xml::list_buckets_xml(buckets, "default-owner");
        Self::new(200).xml_body(body)
    }

    /// Build a response for ListObjectsV2.
    pub fn list_objects_v2(
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        max_keys: u32,
        result: &ListObjectsResult,
    ) -> Self {
        let body = xml::list_objects_v2_xml(bucket, prefix, delimiter, max_keys, result);
        Self::new(200).xml_body(body)
    }

    /// Build a response for ListObjects v1.
    pub fn list_objects_v1(
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        marker: Option<&str>,
        max_keys: u32,
        result: &ListObjectsResult,
    ) -> Self {
        let body = xml::list_objects_v1_xml(bucket, prefix, delimiter, marker, max_keys, result);
        Self::new(200).xml_body(body)
    }

    /// Build a response for DeleteObjects (batch delete).
    pub fn delete_objects(result: &DeleteObjectsResult, quiet: bool) -> Self {
        let body =
            xml::delete_objects_result_xml(&result.deleted, &result.errors, quiet);
        Self::new(200).xml_body(body)
    }

    /// Build a response for ListObjectVersions.
    pub fn list_object_versions(
        bucket: &str,
        prefix: Option<&str>,
        key_marker: Option<&str>,
        max_keys: u32,
        result: &ListObjectsResult,
    ) -> Self {
        let body =
            xml::list_object_versions_xml(bucket, prefix, key_marker, max_keys, result);
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

    /// Build an error response.
    pub fn error(err: &ServerError, resource: &str) -> Self {
        let body = xml::error_xml(
            err.s3_error_code(),
            &err.to_string(),
            resource,
            "request-id",
        );
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
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov",
        "Dec",
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
    let secs = date_to_days(year, month, day) as u64 * 86400 + hours * 3600 + minutes * 60 + seconds;
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
            version_id: "null".to_string(),
        };
        let resp = S3Response::put_object(&result);
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "ETag"), Some("\"abc123\""));
        assert_eq!(find_header(&resp, "x-amz-version-id"), Some("null"));
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
        };
        let resp = S3Response::get_object(result);
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
        };
        let resp = S3Response::get_object(result);
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
        };
        let resp = S3Response::get_object(result);
        assert_eq!(find_header(&resp, "x-amz-meta-author"), Some("alice"));
    }

    #[test]
    fn get_object_with_all_standard_metadata() {
        let result = GetObjectResult {
            data: vec![],
            metadata: MetadataBlob {
                entries: vec![
                    MetadataEntry { key: "content-type".into(), value: "text/html".into() },
                    MetadataEntry { key: "content-encoding".into(), value: "gzip".into() },
                    MetadataEntry { key: "cache-control".into(), value: "max-age=3600".into() },
                    MetadataEntry { key: "content-disposition".into(), value: "attachment".into() },
                    MetadataEntry { key: "content-language".into(), value: "en-US".into() },
                    MetadataEntry { key: "expires".into(), value: "Thu, 01 Jan 2099 00:00:00 GMT".into() },
                ],
            },
            etag: "\"e\"".into(),
            size: 0,
            last_modified: 0,
        };
        let resp = S3Response::get_object(result);
        assert_eq!(find_header(&resp, "Content-Type"), Some("text/html"));
        assert_eq!(find_header(&resp, "Content-Encoding"), Some("gzip"));
        assert_eq!(find_header(&resp, "Cache-Control"), Some("max-age=3600"));
        assert_eq!(find_header(&resp, "Content-Disposition"), Some("attachment"));
        assert_eq!(find_header(&resp, "Content-Language"), Some("en-US"));
        assert_eq!(find_header(&resp, "Expires"), Some("Thu, 01 Jan 2099 00:00:00 GMT"));
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
        };
        let resp = S3Response::head_object(&result);
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
        };
        let resp = S3Response::head_object(&result);
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
                    MetadataEntry { key: "content-encoding".into(), value: "br".into() },
                    MetadataEntry { key: "cache-control".into(), value: "no-cache".into() },
                ],
            },
            etag: "\"e\"".into(),
            size: 10,
            last_modified: 0,
        };
        let resp = S3Response::head_object(&result);
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
        };
        let resp = S3Response::head_object(&result);
        assert_eq!(find_header(&resp, "x-amz-meta-tag"), Some("value"));
    }

    // ── delete_object ─────────────────────────────────────────────────

    #[test]
    fn delete_object_response() {
        let resp = S3Response::delete_object();
        assert_eq!(resp.status_code, 204);
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

    // ── head_bucket ───────────────────────────────────────────────────

    #[test]
    fn head_bucket_response() {
        let info = storage::BucketInfo {
            name: "b".into(),
            owner_id: 0,
            created_at: 0,
            region: 0,
            versioning: 0,
        };
        let resp = S3Response::head_bucket(&info);
        assert_eq!(resp.status_code, 200);
    }

    // ── list_buckets ──────────────────────────────────────────────────

    #[test]
    fn list_buckets_response() {
        let buckets = vec![storage::BucketInfo {
            name: "test-bucket".into(),
            owner_id: 0,
            created_at: 1000,
            region: 0,
            versioning: 0,
        }];
        let resp = S3Response::list_buckets(&buckets);
        assert_eq!(resp.status_code, 200);
        assert_eq!(
            find_header(&resp, "Content-Type"),
            Some("application/xml")
        );
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
        };
        let resp = S3Response::list_objects_v2("bucket", Some("pre"), None, 1000, &result);
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
        };
        let resp =
            S3Response::list_objects_v1("bucket", Some("pre"), None, None, 1000, &result);
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
        assert_eq!(
            find_header(&resp, "Content-Type"),
            Some("application/xml")
        );
    }

    // ── delete_objects ───────────────────────────────────────────────

    #[test]
    fn delete_objects_response() {
        use crate::coordinator::{DeleteObjectsResult, DeletedObject};
        let result = DeleteObjectsResult {
            deleted: vec![DeletedObject {
                key: "key1".into(),
                version_id: "null".into(),
            }],
            errors: vec![],
        };
        let resp = S3Response::delete_objects(&result, false);
        assert_eq!(resp.status_code, 200);
        assert_eq!(
            find_header(&resp, "Content-Type"),
            Some("application/xml")
        );
        let body = String::from_utf8(resp.body).unwrap();
        assert!(body.contains("DeleteResult"));
        assert!(body.contains("key1"));
    }

    // ── list_object_versions ────────────────────────────────────────

    #[test]
    fn list_object_versions_response() {
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
        assert_eq!(
            find_header(&resp, "Content-Type"),
            Some("application/xml")
        );
        let body = String::from_utf8(resp.body).unwrap();
        assert!(body.contains("PreconditionFailed"));
    }
}
