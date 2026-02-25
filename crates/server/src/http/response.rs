/// Build HTTP responses for S3 operations.
use crate::coordinator::{GetObjectResult, HeadObjectResult, ListObjectsResult, PutObjectResult};
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
            .header("Last-Modified", &format_http_date(result.last_modified));

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
            .header("Last-Modified", &format_http_date(result.last_modified));

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
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}
