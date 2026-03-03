/// Hand-formatted XML for S3 responses. No XML library dependency.
use crate::coordinator::{DeleteError, DeletedObject, ListObjectVersionsResult, ListObjectsResult};
use crate::error::ServerError;
use auth::canonical::uri_encode_path;
use storage::BucketInfo;

use super::response::format_version_id;

/// Format a POST Object 201 response XML.
pub fn post_response_xml(bucket: &str, key: &str, etag: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <PostResponse>\
         <Bucket>{}</Bucket>\
         <Key>{}</Key>\
         <ETag>{}</ETag>\
         </PostResponse>",
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(etag),
    )
}

/// Format an S3 error response XML.
pub fn error_xml(code: &str, message: &str, resource: &str, request_id: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <Error>\
         <Code>{}</Code>\
         <Message>{}</Message>\
         <Resource>{}</Resource>\
         <RequestId>{}</RequestId>\
         </Error>",
        xml_escape(code),
        xml_escape(message),
        xml_escape(resource),
        xml_escape(request_id),
    )
}

/// Format a ListAllMyBucketsResult XML response.
pub fn list_buckets_xml(buckets: &[BucketInfo], owner_principal: &str) -> String {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Owner><ID>",
    );
    xml.push_str(&xml_escape(owner_principal));
    xml.push_str("</ID><DisplayName>");
    xml.push_str(&xml_escape(owner_principal));
    xml.push_str("</DisplayName></Owner><Buckets>");

    for bucket in buckets {
        xml.push_str("<Bucket><Name>");
        xml.push_str(&xml_escape(&bucket.name));
        xml.push_str("</Name><CreationDate>");
        xml.push_str(&format_timestamp(bucket.created_at));
        xml.push_str("</CreationDate></Bucket>");
    }

    xml.push_str("</Buckets></ListAllMyBucketsResult>");
    xml
}

/// Format a ListBucketResult (ListObjectsV2) XML response.
#[allow(clippy::too_many_arguments)]
pub fn list_objects_v2_xml(
    bucket: &str,
    prefix: Option<&str>,
    delimiter: Option<&str>,
    encoding_type: Option<&str>,
    continuation_token: Option<&str>,
    start_after: Option<&str>,
    fetch_owner: bool,
    max_keys: u32,
    result: &ListObjectsResult,
) -> String {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
    );

    xml.push_str("<Name>");
    xml.push_str(&xml_escape(bucket));
    xml.push_str("</Name>");

    if let Some(p) = prefix {
        xml.push_str("<Prefix>");
        xml.push_str(&xml_escape(p));
        xml.push_str("</Prefix>");
    } else {
        xml.push_str("<Prefix/>");
    }

    if let Some(d) = delimiter {
        xml.push_str("<Delimiter>");
        xml.push_str(&xml_escape(d));
        xml.push_str("</Delimiter>");
    }

    if let Some(e) = encoding_type {
        xml.push_str("<EncodingType>");
        xml.push_str(&xml_escape(e));
        xml.push_str("</EncodingType>");
    }

    if let Some(t) = continuation_token {
        xml.push_str("<ContinuationToken>");
        xml.push_str(&xml_escape(t));
        xml.push_str("</ContinuationToken>");
    }

    if let Some(s) = start_after {
        xml.push_str("<StartAfter>");
        xml.push_str(&xml_escape(s));
        xml.push_str("</StartAfter>");
    }

    xml.push_str("<MaxKeys>");
    xml.push_str(&max_keys.to_string());
    xml.push_str("</MaxKeys>");

    xml.push_str("<IsTruncated>");
    xml.push_str(if result.is_truncated { "true" } else { "false" });
    xml.push_str("</IsTruncated>");

    xml.push_str("<KeyCount>");
    xml.push_str(&(result.objects.len() + result.common_prefixes.len()).to_string());
    xml.push_str("</KeyCount>");

    if let Some(ref token) = result.next_continuation_token {
        xml.push_str("<NextContinuationToken>");
        xml.push_str(&xml_escape(token));
        xml.push_str("</NextContinuationToken>");
    }

    let owner_id = result.owner_principal.clone();
    let owner_name = owner_id.clone();

    for obj in &result.objects {
        xml.push_str("<Contents>");
        xml.push_str("<Key>");
        xml.push_str(&xml_escape(&encode_value(&obj.key, encoding_type)));
        xml.push_str("</Key>");
        xml.push_str("<LastModified>");
        xml.push_str(&format_timestamp(obj.last_modified));
        xml.push_str("</LastModified>");
        xml.push_str("<ETag>");
        xml.push_str(&xml_escape(&obj.etag));
        xml.push_str("</ETag>");
        xml.push_str("<Size>");
        xml.push_str(&obj.size.to_string());
        xml.push_str("</Size>");
        xml.push_str("<StorageClass>STANDARD</StorageClass>");
        if fetch_owner {
            xml.push_str("<Owner><ID>");
            xml.push_str(&xml_escape(&owner_id));
            xml.push_str("</ID><DisplayName>");
            xml.push_str(&xml_escape(&owner_name));
            xml.push_str("</DisplayName></Owner>");
        }
        xml.push_str("</Contents>");
    }

    for prefix in &result.common_prefixes {
        xml.push_str("<CommonPrefixes><Prefix>");
        xml.push_str(&xml_escape(&encode_value(prefix, encoding_type)));
        xml.push_str("</Prefix></CommonPrefixes>");
    }

    xml.push_str("</ListBucketResult>");
    xml
}

/// Format a ListBucketResult (ListObjects v1) XML response.
pub fn list_objects_v1_xml(
    bucket: &str,
    prefix: Option<&str>,
    delimiter: Option<&str>,
    marker: Option<&str>,
    encoding_type: Option<&str>,
    max_keys: u32,
    result: &ListObjectsResult,
) -> String {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
    );

    xml.push_str("<Name>");
    xml.push_str(&xml_escape(bucket));
    xml.push_str("</Name>");

    if let Some(p) = prefix {
        xml.push_str("<Prefix>");
        xml.push_str(&xml_escape(p));
        xml.push_str("</Prefix>");
    } else {
        xml.push_str("<Prefix/>");
    }

    if let Some(m) = marker {
        xml.push_str("<Marker>");
        xml.push_str(&xml_escape(m));
        xml.push_str("</Marker>");
    } else {
        xml.push_str("<Marker/>");
    }

    if let Some(d) = delimiter {
        xml.push_str("<Delimiter>");
        xml.push_str(&xml_escape(d));
        xml.push_str("</Delimiter>");
    }

    if let Some(e) = encoding_type {
        xml.push_str("<EncodingType>");
        xml.push_str(&xml_escape(e));
        xml.push_str("</EncodingType>");
    }

    xml.push_str("<MaxKeys>");
    xml.push_str(&max_keys.to_string());
    xml.push_str("</MaxKeys>");

    xml.push_str("<IsTruncated>");
    xml.push_str(if result.is_truncated { "true" } else { "false" });
    xml.push_str("</IsTruncated>");

    if result.is_truncated {
        if let Some(ref token) = result.next_continuation_token {
            xml.push_str("<NextMarker>");
            xml.push_str(&xml_escape(token));
            xml.push_str("</NextMarker>");
        }
    }

    let owner_id = result.owner_principal.clone();
    let owner_name = owner_id.clone();

    for obj in &result.objects {
        xml.push_str("<Contents>");
        xml.push_str("<Key>");
        xml.push_str(&xml_escape(&encode_value(&obj.key, encoding_type)));
        xml.push_str("</Key>");
        xml.push_str("<LastModified>");
        xml.push_str(&format_timestamp(obj.last_modified));
        xml.push_str("</LastModified>");
        xml.push_str("<ETag>");
        xml.push_str(&xml_escape(&obj.etag));
        xml.push_str("</ETag>");
        xml.push_str("<Size>");
        xml.push_str(&obj.size.to_string());
        xml.push_str("</Size>");
        xml.push_str("<StorageClass>STANDARD</StorageClass>");
        xml.push_str("<Owner><ID>");
        xml.push_str(&xml_escape(&owner_id));
        xml.push_str("</ID><DisplayName>");
        xml.push_str(&xml_escape(&owner_name));
        xml.push_str("</DisplayName></Owner>");
        xml.push_str("</Contents>");
    }

    for prefix in &result.common_prefixes {
        xml.push_str("<CommonPrefixes><Prefix>");
        xml.push_str(&xml_escape(&encode_value(prefix, encoding_type)));
        xml.push_str("</Prefix></CommonPrefixes>");
    }

    xml.push_str("</ListBucketResult>");
    xml
}

/// An entry in a DeleteObjects request.
pub struct DeleteObjectEntry {
    pub key: String,
    pub version_id: Option<String>,
}

/// Parse a DeleteObjects XML request body.
///
/// Returns the list of object entries and the quiet flag.
pub fn parse_delete_objects_xml(
    data: &[u8],
) -> Result<(Vec<DeleteObjectEntry>, bool), ServerError> {
    let text = std::str::from_utf8(data).map_err(|_| ServerError::InvalidRequest {
        reason: "invalid UTF-8 in delete XML body".to_string(),
    })?;

    // Require <Delete> wrapper
    if !text.contains("<Delete") {
        return Err(ServerError::InvalidRequest {
            reason: "missing <Delete> element".to_string(),
        });
    }

    // Detect quiet mode
    let quiet = extract_tag_content(text, "Quiet")
        .map(|v| v == "true")
        .unwrap_or(false);

    // Parse <Object> blocks
    let mut entries = Vec::new();
    let mut search_from = 0;
    while let Some(start) = text[search_from..].find("<Object>") {
        let abs_start = search_from + start + "<Object>".len();
        let end =
            text[abs_start..]
                .find("</Object>")
                .ok_or_else(|| ServerError::InvalidRequest {
                    reason: "unclosed <Object> element".to_string(),
                })?;
        let block = &text[abs_start..abs_start + end];

        let key = extract_tag_content(block, "Key").ok_or_else(|| ServerError::InvalidRequest {
            reason: "Object missing <Key> element".to_string(),
        })?;
        let key = xml_unescape(key);
        let version_id = extract_tag_content(block, "VersionId").map(xml_unescape);

        entries.push(DeleteObjectEntry { key, version_id });

        search_from = abs_start + end + "</Object>".len();
    }

    if entries.len() > 1000 {
        return Err(ServerError::InvalidRequest {
            reason: "delete objects list too large (max 1000)".to_string(),
        });
    }

    Ok((entries, quiet))
}

/// Extract the text content of a simple XML tag (no attributes, no nesting).
fn extract_tag_content<'a>(xml: &'a str, tag: &str) -> Option<&'a str> {
    let close = format!("</{}>", tag);
    // Try exact match first: <Tag>
    let open_exact = format!("<{}>", tag);
    if let Some(pos) = xml.find(&open_exact) {
        let start = pos + open_exact.len();
        let end = xml[start..].find(&close)? + start;
        return Some(&xml[start..end]);
    }
    // Try match with attributes: <Tag ...>
    let open_prefix = format!("<{} ", tag);
    let pos = xml.find(&open_prefix)?;
    let gt = xml[pos..].find('>')? + pos;
    let start = gt + 1;
    let end = xml[start..].find(&close)? + start;
    Some(&xml[start..end])
}

/// Format a DeleteResult XML response.
pub fn delete_objects_result_xml(
    deleted: &[DeletedObject],
    errors: &[DeleteError],
    quiet: bool,
) -> String {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
    );

    if !quiet {
        for d in deleted {
            let vid = super::response::format_version_id(d.version_id);
            xml.push_str("<Deleted><Key>");
            xml.push_str(&xml_escape(&d.key));
            xml.push_str("</Key><VersionId>");
            xml.push_str(&xml_escape(&vid));
            xml.push_str("</VersionId>");
            if d.delete_marker {
                xml.push_str("<DeleteMarker>true</DeleteMarker>");
                xml.push_str("<DeleteMarkerVersionId>");
                xml.push_str(&xml_escape(&vid));
                xml.push_str("</DeleteMarkerVersionId>");
            }
            xml.push_str("</Deleted>");
        }
    }

    for e in errors {
        xml.push_str("<Error><Key>");
        xml.push_str(&xml_escape(&e.key));
        xml.push_str("</Key><Code>");
        xml.push_str(&xml_escape(&e.code));
        xml.push_str("</Code><Message>");
        xml.push_str(&xml_escape(&e.message));
        xml.push_str("</Message></Error>");
    }

    xml.push_str("</DeleteResult>");
    xml
}

/// Parse a PutBucketVersioning XML request body.
///
/// Returns the versioning state: 1 = Enabled, 2 = Suspended.
pub fn parse_versioning_config_xml(data: &[u8]) -> Result<u8, ServerError> {
    let text = std::str::from_utf8(data).map_err(|_| ServerError::InvalidRequest {
        reason: "invalid UTF-8 in versioning XML body".to_string(),
    })?;

    if let Some(status) = extract_tag_content(text, "Status") {
        match status {
            "Enabled" => Ok(1),
            "Suspended" => Ok(2),
            other => Err(ServerError::InvalidRequest {
                reason: format!("invalid versioning status: {}", other),
            }),
        }
    } else {
        Err(ServerError::InvalidRequest {
            reason: "missing <Status> element in versioning configuration".to_string(),
        })
    }
}

/// Format a GetBucketVersioning XML response.
pub fn get_bucket_versioning_xml(state: u8) -> String {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
    );

    match state {
        1 => xml.push_str("<Status>Enabled</Status>"),
        2 => xml.push_str("<Status>Suspended</Status>"),
        _ => {} // Disabled (0): empty element per S3 spec
    }

    xml.push_str("</VersioningConfiguration>");
    xml
}

/// Format a ListVersionsResult XML response.
pub fn list_object_versions_xml(
    bucket: &str,
    prefix: Option<&str>,
    key_marker: Option<&str>,
    max_keys: u32,
    result: &ListObjectVersionsResult,
) -> String {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <ListVersionsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
    );

    xml.push_str("<Name>");
    xml.push_str(&xml_escape(bucket));
    xml.push_str("</Name>");

    if let Some(p) = prefix {
        xml.push_str("<Prefix>");
        xml.push_str(&xml_escape(p));
        xml.push_str("</Prefix>");
    } else {
        xml.push_str("<Prefix/>");
    }

    if let Some(km) = key_marker {
        xml.push_str("<KeyMarker>");
        xml.push_str(&xml_escape(km));
        xml.push_str("</KeyMarker>");
    } else {
        xml.push_str("<KeyMarker/>");
    }

    xml.push_str("<VersionIdMarker/>");

    xml.push_str("<MaxKeys>");
    xml.push_str(&max_keys.to_string());
    xml.push_str("</MaxKeys>");

    xml.push_str("<IsTruncated>");
    xml.push_str(if result.is_truncated { "true" } else { "false" });
    xml.push_str("</IsTruncated>");

    if let Some(ref nkm) = result.next_key_marker {
        xml.push_str("<NextKeyMarker>");
        xml.push_str(&xml_escape(nkm));
        xml.push_str("</NextKeyMarker>");
    }

    if let Some(nvm) = result.next_version_id_marker {
        xml.push_str("<NextVersionIdMarker>");
        xml.push_str(&format_version_id(nvm));
        xml.push_str("</NextVersionIdMarker>");
    }

    for entry in &result.versions {
        let vid = format_version_id(entry.version_id);
        let is_latest = if entry.is_latest { "true" } else { "false" };

        if entry.is_delete_marker {
            xml.push_str("<DeleteMarker>");
            xml.push_str("<Key>");
            xml.push_str(&xml_escape(&entry.key));
            xml.push_str("</Key>");
            xml.push_str("<VersionId>");
            xml.push_str(&vid);
            xml.push_str("</VersionId>");
            xml.push_str("<IsLatest>");
            xml.push_str(is_latest);
            xml.push_str("</IsLatest>");
            xml.push_str("<LastModified>");
            xml.push_str(&format_timestamp(entry.last_modified));
            xml.push_str("</LastModified>");
            xml.push_str("</DeleteMarker>");
        } else {
            xml.push_str("<Version>");
            xml.push_str("<Key>");
            xml.push_str(&xml_escape(&entry.key));
            xml.push_str("</Key>");
            xml.push_str("<VersionId>");
            xml.push_str(&vid);
            xml.push_str("</VersionId>");
            xml.push_str("<IsLatest>");
            xml.push_str(is_latest);
            xml.push_str("</IsLatest>");
            xml.push_str("<LastModified>");
            xml.push_str(&format_timestamp(entry.last_modified));
            xml.push_str("</LastModified>");
            xml.push_str("<ETag>");
            xml.push_str(&xml_escape(&entry.etag));
            xml.push_str("</ETag>");
            xml.push_str("<Size>");
            xml.push_str(&entry.size.to_string());
            xml.push_str("</Size>");
            xml.push_str("<StorageClass>STANDARD</StorageClass>");
            xml.push_str("</Version>");
        }
    }

    xml.push_str("</ListVersionsResult>");
    xml
}

fn encode_value(value: &str, encoding_type: Option<&str>) -> String {
    match encoding_type {
        Some("url") => uri_encode_path(value),
        _ => value.to_string(),
    }
}

/// Escape special XML characters.
fn xml_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(c),
        }
    }
    out
}

/// Unescape XML entity references used in S3 delete payloads.
fn xml_unescape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '&' {
            out.push(ch);
            continue;
        }
        let mut entity = String::new();
        while let Some(&c) = chars.peek() {
            chars.next();
            if c == ';' {
                break;
            }
            entity.push(c);
        }
        match entity.as_str() {
            "amp" => out.push('&'),
            "lt" => out.push('<'),
            "gt" => out.push('>'),
            "quot" => out.push('"'),
            "apos" => out.push('\''),
            _ => {
                out.push('&');
                out.push_str(&entity);
                out.push(';');
            }
        }
    }
    out
}

/// Parse a CORS configuration XML body.
///
/// Expected format:
/// ```xml
/// <CORSConfiguration>
///   <CORSRule>
///     <AllowedOrigin>...</AllowedOrigin>
///     <AllowedMethod>GET</AllowedMethod>
///     <AllowedHeader>...</AllowedHeader>
///     <ExposeHeader>...</ExposeHeader>
///     <MaxAgeSeconds>3600</MaxAgeSeconds>
///   </CORSRule>
/// </CORSConfiguration>
/// ```
pub fn parse_cors_config_xml(data: &[u8]) -> Result<crate::cors::CorsConfiguration, ServerError> {
    let text = std::str::from_utf8(data).map_err(|_| ServerError::InvalidRequest {
        reason: "invalid UTF-8 in CORS XML body".to_string(),
    })?;

    if !text.contains("<CORSConfiguration") {
        return Err(ServerError::InvalidRequest {
            reason: "missing <CORSConfiguration> element".to_string(),
        });
    }

    let valid_methods = ["GET", "PUT", "POST", "DELETE", "HEAD"];

    let mut rules = Vec::new();
    let mut search_from = 0;
    while let Some(start) = text[search_from..].find("<CORSRule>") {
        let abs_start = search_from + start + "<CORSRule>".len();
        let end =
            text[abs_start..]
                .find("</CORSRule>")
                .ok_or_else(|| ServerError::InvalidRequest {
                    reason: "unclosed <CORSRule> element".to_string(),
                })?;
        let block = &text[abs_start..abs_start + end];

        // Parse AllowedOrigin (1+ required)
        let allowed_origins: Vec<String> = extract_all_tag_contents(block, "AllowedOrigin")
            .into_iter()
            .map(|s| xml_unescape(&s))
            .collect();
        if allowed_origins.is_empty() {
            return Err(ServerError::InvalidRequest {
                reason: "CORSRule missing <AllowedOrigin> element".to_string(),
            });
        }

        // Parse AllowedMethod (1+ required)
        let allowed_methods: Vec<String> = extract_all_tag_contents(block, "AllowedMethod")
            .into_iter()
            .map(|s| xml_unescape(&s))
            .collect();
        if allowed_methods.is_empty() {
            return Err(ServerError::InvalidRequest {
                reason: "CORSRule missing <AllowedMethod> element".to_string(),
            });
        }
        for m in &allowed_methods {
            if !valid_methods.contains(&m.as_str()) {
                return Err(ServerError::InvalidRequest {
                    reason: format!("invalid CORS method: {}", m),
                });
            }
        }

        // Parse AllowedHeader (0+)
        let allowed_headers: Vec<String> = extract_all_tag_contents(block, "AllowedHeader")
            .into_iter()
            .map(|s| xml_unescape(&s))
            .collect();

        // Parse ExposeHeader (0+)
        let expose_headers: Vec<String> = extract_all_tag_contents(block, "ExposeHeader")
            .into_iter()
            .map(|s| xml_unescape(&s))
            .collect();

        // Parse MaxAgeSeconds (0 or 1)
        let max_age_seconds = extract_tag_content(block, "MaxAgeSeconds")
            .map(|s| {
                s.parse::<u32>().map_err(|_| ServerError::InvalidRequest {
                    reason: format!("invalid MaxAgeSeconds: {}", s),
                })
            })
            .transpose()?;

        rules.push(crate::cors::CorsRule {
            allowed_origins,
            allowed_methods,
            allowed_headers,
            expose_headers,
            max_age_seconds,
        });

        search_from = abs_start + end + "</CORSRule>".len();
    }

    if rules.is_empty() {
        return Err(ServerError::InvalidRequest {
            reason: "CORS configuration must contain at least one rule".to_string(),
        });
    }
    if rules.len() > 100 {
        return Err(ServerError::InvalidRequest {
            reason: "CORS configuration must contain at most 100 rules".to_string(),
        });
    }

    Ok(crate::cors::CorsConfiguration { rules })
}

/// Serialize a CORS configuration to XML.
pub fn get_cors_config_xml(config: &crate::cors::CorsConfiguration) -> String {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <CORSConfiguration>",
    );

    for rule in &config.rules {
        xml.push_str("<CORSRule>");

        for origin in &rule.allowed_origins {
            xml.push_str("<AllowedOrigin>");
            xml.push_str(&xml_escape(origin));
            xml.push_str("</AllowedOrigin>");
        }

        for method in &rule.allowed_methods {
            xml.push_str("<AllowedMethod>");
            xml.push_str(&xml_escape(method));
            xml.push_str("</AllowedMethod>");
        }

        for header in &rule.allowed_headers {
            xml.push_str("<AllowedHeader>");
            xml.push_str(&xml_escape(header));
            xml.push_str("</AllowedHeader>");
        }

        for header in &rule.expose_headers {
            xml.push_str("<ExposeHeader>");
            xml.push_str(&xml_escape(header));
            xml.push_str("</ExposeHeader>");
        }

        if let Some(max_age) = rule.max_age_seconds {
            xml.push_str("<MaxAgeSeconds>");
            xml.push_str(&max_age.to_string());
            xml.push_str("</MaxAgeSeconds>");
        }

        xml.push_str("</CORSRule>");
    }

    xml.push_str("</CORSConfiguration>");
    xml
}

/// Extract all occurrences of a simple XML tag's text content.
fn extract_all_tag_contents(xml: &str, tag: &str) -> Vec<String> {
    let open = format!("<{}>", tag);
    let close = format!("</{}>", tag);
    let mut results = Vec::new();
    let mut search_from = 0;
    while let Some(start_pos) = xml[search_from..].find(&open) {
        let abs_start = search_from + start_pos + open.len();
        if let Some(end_pos) = xml[abs_start..].find(&close) {
            results.push(xml[abs_start..abs_start + end_pos].to_string());
            search_from = abs_start + end_pos + close.len();
        } else {
            break;
        }
    }
    results
}

/// Format a CopyObjectResult XML response.
pub fn copy_object_result_xml(etag: &str, last_modified: u64) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <CopyObjectResult>\
         <ETag>{}</ETag>\
         <LastModified>{}</LastModified>\
         </CopyObjectResult>",
        xml_escape(etag),
        format_timestamp(last_modified),
    )
}

/// Format a unix millisecond timestamp as ISO 8601.
pub(crate) fn format_timestamp(millis: u64) -> String {
    let secs = millis / 1000;
    // Simple UTC formatting without pulling in chrono
    let days_since_epoch = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    // Convert days since epoch to date (civil calendar)
    let (year, month, day) = days_to_date(days_since_epoch as i64);

    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.000Z",
        year, month, day, hours, minutes, seconds
    )
}

/// Convert days since Unix epoch to (year, month, day).
fn days_to_date(days: i64) -> (i64, u32, u32) {
    // Algorithm from Howard Hinnant's date algorithms
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

/// Parse a `<Tagging>` XML request body into a list of (key, value) pairs.
///
/// Validates S3 constraints: key 1–128 chars, value 0–256 chars,
/// unique keys, no `aws:` key prefix.
///
/// `max_tags` sets the limit: 10 for object tags, 50 for bucket tags.
pub fn parse_tagging_xml(
    data: &[u8],
    max_tags: usize,
) -> Result<Vec<(String, String)>, ServerError> {
    let text = std::str::from_utf8(data).map_err(|_| ServerError::InvalidRequest {
        reason: "invalid UTF-8 in tagging XML body".to_string(),
    })?;

    // Require both wrapper elements
    let tagging_block =
        extract_tag_content(text, "Tagging").ok_or(ServerError::InvalidRequest {
            reason: "missing <Tagging> element in tagging XML".to_string(),
        })?;
    let tag_set =
        extract_tag_content(tagging_block, "TagSet").ok_or(ServerError::InvalidRequest {
            reason: "missing <TagSet> element in tagging XML".to_string(),
        })?;

    // Extract all <Tag> blocks
    let tag_blocks = extract_all_tag_contents(tag_set, "Tag");

    if tag_blocks.len() > max_tags {
        return Err(ServerError::InvalidTag {
            reason: format!(
                "tags cannot be greater than {}, got {}",
                max_tags,
                tag_blocks.len()
            ),
        });
    }

    let mut tags = Vec::with_capacity(tag_blocks.len());
    let mut seen_keys = std::collections::HashSet::new();

    for block in &tag_blocks {
        let key = extract_tag_content(block, "Key").ok_or(ServerError::InvalidTag {
            reason: "missing <Key> element in <Tag>".to_string(),
        })?;
        let key = xml_unescape(key);

        let key_chars = key.chars().count();
        if key_chars == 0 || key_chars > 128 {
            return Err(ServerError::InvalidTag {
                reason: format!("tag key must be 1-128 characters, got {}", key_chars),
            });
        }
        if key.starts_with("aws:") {
            return Err(ServerError::InvalidTag {
                reason: "tag key must not start with 'aws:'".to_string(),
            });
        }

        let value = extract_tag_content(block, "Value").unwrap_or("");
        let value = xml_unescape(value);

        let value_chars = value.chars().count();
        if value_chars > 256 {
            return Err(ServerError::InvalidTag {
                reason: format!("tag value must be 0-256 characters, got {}", value_chars),
            });
        }

        if !seen_keys.insert(key.clone()) {
            return Err(ServerError::InvalidTag {
                reason: format!("duplicate tag key: {}", key),
            });
        }

        tags.push((key, value));
    }

    Ok(tags)
}

/// Serialize a list of (key, value) tag pairs into S3 tagging XML.
pub fn get_tagging_xml(tags: &[(String, String)]) -> String {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <Tagging xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><TagSet>",
    );
    for (k, v) in tags {
        xml.push_str("<Tag><Key>");
        xml.push_str(&xml_escape(k));
        xml.push_str("</Key><Value>");
        xml.push_str(&xml_escape(v));
        xml.push_str("</Value></Tag>");
    }
    xml.push_str("</TagSet></Tagging>");
    xml
}

/// Parse URL-encoded tags from the `x-amz-tagging` header.
///
/// Format: `key1=value1&key2=value2`
/// Applies the same S3 validation constraints as `parse_tagging_xml`.
pub fn parse_url_encoded_tags(input: &str) -> Result<Vec<(String, String)>, ServerError> {
    if input.is_empty() {
        return Ok(Vec::new());
    }

    let mut tags = Vec::new();
    let mut seen_keys = std::collections::HashSet::new();

    for pair in input.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (raw_key, raw_value) = match pair.find('=') {
            Some(pos) => (&pair[..pos], &pair[pos + 1..]),
            None => (pair, ""),
        };

        let key = percent_decode_tag(raw_key)?;
        let value = percent_decode_tag(raw_value)?;

        let key_chars = key.chars().count();
        if key_chars == 0 || key_chars > 128 {
            return Err(ServerError::InvalidTag {
                reason: format!("tag key must be 1-128 characters, got {}", key_chars),
            });
        }
        if key.starts_with("aws:") {
            return Err(ServerError::InvalidTag {
                reason: "tag key must not start with 'aws:'".to_string(),
            });
        }
        let value_chars = value.chars().count();
        if value_chars > 256 {
            return Err(ServerError::InvalidTag {
                reason: format!("tag value must be 0-256 characters, got {}", value_chars),
            });
        }

        if !seen_keys.insert(key.clone()) {
            return Err(ServerError::InvalidTag {
                reason: format!("duplicate tag key: {}", key),
            });
        }

        tags.push((key, value));
    }

    if tags.len() > 10 {
        return Err(ServerError::InvalidTag {
            reason: format!("Object tags cannot be greater than 10, got {}", tags.len()),
        });
    }

    Ok(tags)
}

/// Count the number of tags in a stored tagging XML string.
pub fn count_tags_in_xml(xml: &str) -> usize {
    let tag_set = extract_tag_content(xml, "TagSet").unwrap_or("");
    extract_all_tag_contents(tag_set, "Tag").len()
}

/// Percent-decode a tag key or value from URL-encoded form.
///
/// Collects decoded bytes first, then converts to UTF-8, so multibyte
/// percent-encoded sequences (e.g. `%C3%A9` for `é`) decode correctly.
fn percent_decode_tag(input: &str) -> Result<String, ServerError> {
    let mut bytes = Vec::with_capacity(input.len());
    let mut iter = input.bytes();
    while let Some(b) = iter.next() {
        if b == b'+' {
            bytes.push(b' ');
        } else if b == b'%' {
            let hi = iter.next().ok_or(ServerError::InvalidTag {
                reason: "incomplete percent-encoding in tag".to_string(),
            })?;
            let lo = iter.next().ok_or(ServerError::InvalidTag {
                reason: "incomplete percent-encoding in tag".to_string(),
            })?;
            let byte = decode_hex_pair(hi, lo).ok_or(ServerError::InvalidTag {
                reason: "invalid percent-encoding in tag".to_string(),
            })?;
            bytes.push(byte);
        } else {
            bytes.push(b);
        }
    }
    String::from_utf8(bytes).map_err(|_| ServerError::InvalidTag {
        reason: "invalid UTF-8 in percent-decoded tag".to_string(),
    })
}

/// Decode a pair of hex characters into a byte.
fn decode_hex_pair(hi: u8, lo: u8) -> Option<u8> {
    let h = match hi {
        b'0'..=b'9' => hi - b'0',
        b'a'..=b'f' => hi - b'a' + 10,
        b'A'..=b'F' => hi - b'A' + 10,
        _ => return None,
    };
    let l = match lo {
        b'0'..=b'9' => lo - b'0',
        b'a'..=b'f' => lo - b'a' + 10,
        b'A'..=b'F' => lo - b'A' + 10,
        _ => return None,
    };
    Some(h << 4 | l)
}

/// Public access block configuration for a bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicAccessBlockConfig {
    pub block_public_acls: bool,
    pub ignore_public_acls: bool,
    pub block_public_policy: bool,
    pub restrict_public_buckets: bool,
}

/// Parse a `<PublicAccessBlockConfiguration>` XML request body.
///
/// Missing boolean elements default to `false`.
pub fn parse_public_access_block_xml(data: &[u8]) -> Result<PublicAccessBlockConfig, ServerError> {
    let text = std::str::from_utf8(data).map_err(|_| ServerError::InvalidRequest {
        reason: "invalid UTF-8 in public access block XML body".to_string(),
    })?;
    let inner = extract_tag_content(text, "PublicAccessBlockConfiguration").ok_or_else(|| {
        ServerError::InvalidRequest {
            reason: "missing PublicAccessBlockConfiguration element".to_string(),
        }
    })?;

    fn parse_bool_element(xml: &str, tag: &str) -> bool {
        extract_tag_content(xml, tag)
            .map(|v| v.trim().eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    }

    Ok(PublicAccessBlockConfig {
        block_public_acls: parse_bool_element(inner, "BlockPublicAcls"),
        ignore_public_acls: parse_bool_element(inner, "IgnorePublicAcls"),
        block_public_policy: parse_bool_element(inner, "BlockPublicPolicy"),
        restrict_public_buckets: parse_bool_element(inner, "RestrictPublicBuckets"),
    })
}

/// Serialize a `PublicAccessBlockConfig` into S3 response XML.
pub fn get_public_access_block_xml(config: &PublicAccessBlockConfig) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <PublicAccessBlockConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <BlockPublicAcls>{}</BlockPublicAcls>\
         <IgnorePublicAcls>{}</IgnorePublicAcls>\
         <BlockPublicPolicy>{}</BlockPublicPolicy>\
         <RestrictPublicBuckets>{}</RestrictPublicBuckets>\
         </PublicAccessBlockConfiguration>",
        if config.block_public_acls {
            "true"
        } else {
            "false"
        },
        if config.ignore_public_acls {
            "true"
        } else {
            "false"
        },
        if config.block_public_policy {
            "true"
        } else {
            "false"
        },
        if config.restrict_public_buckets {
            "true"
        } else {
            "false"
        },
    )
}

/// Parse an `<OwnershipControls>` XML request body.
///
/// Extracts the `ObjectOwnership` value from
/// `<OwnershipControls><Rule><ObjectOwnership>VALUE</ObjectOwnership></Rule></OwnershipControls>`.
/// Validates VALUE is one of `BucketOwnerEnforced`, `BucketOwnerPreferred`, or `ObjectWriter`.
pub fn parse_ownership_controls_xml(data: &[u8]) -> Result<String, ServerError> {
    let text = std::str::from_utf8(data).map_err(|_| ServerError::InvalidRequest {
        reason: "invalid UTF-8 in ownership controls XML body".to_string(),
    })?;
    let inner = extract_tag_content(text, "OwnershipControls").ok_or_else(|| {
        ServerError::InvalidRequest {
            reason: "missing OwnershipControls element".to_string(),
        }
    })?;
    let rule = extract_tag_content(inner, "Rule").ok_or_else(|| ServerError::InvalidRequest {
        reason: "missing Rule element in OwnershipControls".to_string(),
    })?;
    let value = extract_tag_content(rule, "ObjectOwnership")
        .ok_or_else(|| ServerError::InvalidRequest {
            reason: "missing ObjectOwnership element in Rule".to_string(),
        })?
        .trim();

    match value {
        "BucketOwnerEnforced" | "BucketOwnerPreferred" | "ObjectWriter" => Ok(value.to_string()),
        _ => Err(ServerError::InvalidArgument {
            reason: format!("invalid ObjectOwnership value: {value}"),
        }),
    }
}

/// Serialize an `ObjectOwnership` value into S3 response XML.
pub fn get_ownership_controls_xml(object_ownership: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <OwnershipControls xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Rule><ObjectOwnership>{}</ObjectOwnership></Rule>\
         </OwnershipControls>",
        xml_escape(object_ownership),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::ListEntry;

    #[test]
    fn error_xml_format() {
        let xml = error_xml(
            "NoSuchBucket",
            "The bucket does not exist",
            "/mybucket",
            "req-1",
        );
        assert!(xml.contains("<Code>NoSuchBucket</Code>"));
        assert!(xml.contains("<Message>The bucket does not exist</Message>"));
        assert!(xml.contains("<?xml"));
    }

    #[test]
    fn list_buckets_xml_format() {
        let buckets = vec![BucketInfo {
            name: "test-bucket".to_string(),
            owner_principal: "owner".to_string(),
            created_at: 1685000000000,
            region: 0,
            versioning: 0,
            public_read: false,
            cors_config: None,
            tags: None,
            public_access_block: None,
            ownership_controls: None,
        }];
        let xml = list_buckets_xml(&buckets, "owner");
        assert!(xml.contains("<Name>test-bucket</Name>"));
        assert!(xml.contains("ListAllMyBucketsResult"));
    }

    #[test]
    fn list_objects_xml_format() {
        let result = ListObjectsResult {
            objects: vec![ListEntry {
                key: "my-key".to_string(),
                size: 42,
                etag: "\"abc123\"".to_string(),
                last_modified: 1685000000000,
            }],
            common_prefixes: vec![],
            is_truncated: false,
            next_continuation_token: None,
            owner_principal: "owner".to_string(),
        };
        let xml = list_objects_v2_xml("bucket", None, None, None, None, None, false, 1000, &result);
        assert!(xml.contains("<Key>my-key</Key>"));
        assert!(xml.contains("<Size>42</Size>"));
        assert!(xml.contains("ListBucketResult"));
    }

    #[test]
    fn list_objects_xml_with_prefix_delimiter_and_truncation() {
        let result = ListObjectsResult {
            objects: vec![ListEntry {
                key: "photos/cat.jpg".to_string(),
                size: 100,
                etag: "\"aabbccdd\"".to_string(),
                last_modified: 1685000000000,
            }],
            common_prefixes: vec!["photos/2024/".to_string()],
            is_truncated: true,
            next_continuation_token: Some("photos/cat.jpg".to_string()),
            owner_principal: "owner".to_string(),
        };
        let xml = list_objects_v2_xml(
            "bucket",
            Some("photos/"),
            Some("/"),
            None,
            None,
            None,
            false,
            1,
            &result,
        );
        assert!(xml.contains("<Prefix>photos/</Prefix>"));
        assert!(xml.contains("<Delimiter>/</Delimiter>"));
        assert!(xml.contains("<IsTruncated>true</IsTruncated>"));
        assert!(xml.contains("<NextContinuationToken>photos/cat.jpg</NextContinuationToken>"));
        assert!(xml.contains("<CommonPrefixes><Prefix>photos/2024/</Prefix></CommonPrefixes>"));
    }

    #[test]
    fn list_objects_xml_key_count_includes_prefixes() {
        let result = ListObjectsResult {
            objects: vec![ListEntry {
                key: "root.txt".to_string(),
                size: 10,
                etag: "\"abc\"".to_string(),
                last_modified: 0,
            }],
            common_prefixes: vec!["photos/".to_string(), "docs/".to_string()],
            is_truncated: false,
            next_continuation_token: None,
            owner_principal: "owner".to_string(),
        };
        let xml = list_objects_v2_xml(
            "bucket",
            None,
            Some("/"),
            None,
            None,
            None,
            false,
            1000,
            &result,
        );
        // 1 object + 2 prefixes = 3
        assert!(xml.contains("<KeyCount>3</KeyCount>"));
    }

    #[test]
    fn list_objects_xml_empty_prefix() {
        let result = ListObjectsResult {
            objects: vec![],
            common_prefixes: vec![],
            is_truncated: false,
            next_continuation_token: None,
            owner_principal: "owner".to_string(),
        };
        // With prefix=None → should produce <Prefix/>
        let xml = list_objects_v2_xml("bucket", None, None, None, None, None, false, 1000, &result);
        assert!(xml.contains("<Prefix/>"));
        assert!(!xml.contains("<Delimiter>"));
        assert!(!xml.contains("<NextContinuationToken>"));
    }

    #[test]
    fn list_objects_v1_xml_format() {
        let result = ListObjectsResult {
            objects: vec![ListEntry {
                key: "my-key".to_string(),
                size: 42,
                etag: "\"abc123\"".to_string(),
                last_modified: 1685000000000,
            }],
            common_prefixes: vec![],
            is_truncated: false,
            next_continuation_token: None,
            owner_principal: "owner".to_string(),
        };
        let xml = list_objects_v1_xml("bucket", None, None, None, None, 1000, &result);
        assert!(xml.contains("<Key>my-key</Key>"));
        assert!(xml.contains("<Marker/>"));
        assert!(xml.contains("ListBucketResult"));
        // V1 should NOT have KeyCount or ContinuationToken
        assert!(!xml.contains("<KeyCount>"));
        assert!(!xml.contains("<ContinuationToken>"));
    }

    #[test]
    fn list_objects_v1_xml_with_prefix_delimiter_and_common_prefixes() {
        let result = ListObjectsResult {
            objects: vec![ListEntry {
                key: "photos/cat.jpg".to_string(),
                size: 100,
                etag: "\"aabbccdd\"".to_string(),
                last_modified: 1685000000000,
            }],
            common_prefixes: vec!["photos/2024/".to_string()],
            is_truncated: true,
            next_continuation_token: Some("photos/cat.jpg".to_string()),
            owner_principal: "owner".to_string(),
        };
        let xml = list_objects_v1_xml(
            "bucket",
            Some("photos/"),
            Some("/"),
            Some("a"),
            None,
            1,
            &result,
        );
        assert!(xml.contains("<Prefix>photos/</Prefix>"));
        assert!(xml.contains("<Delimiter>/</Delimiter>"));
        assert!(xml.contains("<Marker>a</Marker>"));
        assert!(xml.contains("<IsTruncated>true</IsTruncated>"));
        assert!(xml.contains("<NextMarker>photos/cat.jpg</NextMarker>"));
        assert!(xml.contains("<CommonPrefixes><Prefix>photos/2024/</Prefix></CommonPrefixes>"));
        // V1: no KeyCount, no ContinuationToken
        assert!(!xml.contains("<KeyCount>"));
        assert!(!xml.contains("<ContinuationToken>"));
    }

    #[test]
    fn list_objects_v1_xml_empty_prefix_no_delimiter() {
        let result = ListObjectsResult {
            objects: vec![],
            common_prefixes: vec![],
            is_truncated: false,
            next_continuation_token: None,
            owner_principal: "owner".to_string(),
        };
        let xml = list_objects_v1_xml("bucket", None, None, None, None, 1000, &result);
        assert!(xml.contains("<Prefix/>"));
        assert!(xml.contains("<Marker/>"));
        assert!(!xml.contains("<Delimiter>"));
        assert!(!xml.contains("<NextMarker>"));
    }

    #[test]
    fn list_objects_v1_xml_with_marker_and_truncation() {
        let result = ListObjectsResult {
            objects: vec![ListEntry {
                key: "key2".to_string(),
                size: 10,
                etag: "\"etag\"".to_string(),
                last_modified: 0,
            }],
            common_prefixes: vec![],
            is_truncated: true,
            next_continuation_token: Some("key2".to_string()),
            owner_principal: "owner".to_string(),
        };
        let xml = list_objects_v1_xml("bucket", None, None, Some("key1"), None, 1, &result);
        assert!(xml.contains("<Marker>key1</Marker>"));
        assert!(xml.contains("<NextMarker>key2</NextMarker>"));
        assert!(xml.contains("<IsTruncated>true</IsTruncated>"));
    }

    #[test]
    fn days_to_date_pre_epoch() {
        // 1969-12-31 is day -1 from epoch
        let (y, m, d) = days_to_date(-1);
        assert_eq!((y, m, d), (1969, 12, 31));
    }

    #[test]
    fn days_to_date_january() {
        // 2024-01-15: mp >= 10 branch, m <= 2 branch (January)
        // 2024-01-01 = day 19723 from epoch
        // 2024-01-15 = day 19737
        let (y, m, d) = days_to_date(19737);
        assert_eq!((y, m, d), (2024, 1, 15));
    }

    #[test]
    fn days_to_date_february() {
        // 2024-02-15: m <= 2 branch (February)
        // 2024-02-15 = day 19768
        let (y, m, d) = days_to_date(19768);
        assert_eq!((y, m, d), (2024, 2, 15));
    }

    #[test]
    fn format_timestamp_january_date() {
        // 2024-01-15T12:30:45.000Z
        // days=19737, time=12*3600+30*60+45=45045
        // total seconds = 19737*86400 + 45045 = 1705321845
        let ts = format_timestamp(1705321845000);
        assert_eq!(ts, "2024-01-15T12:30:45.000Z");
    }

    #[test]
    fn xml_escape_special_chars() {
        assert_eq!(
            xml_escape("a&b<c>d\"e'f"),
            "a&amp;b&lt;c&gt;d&quot;e&apos;f"
        );
    }

    // ── parse_delete_objects_xml ─────────────────────────────────────

    #[test]
    fn parse_delete_objects_basic() {
        let xml =
            b"<Delete><Object><Key>key1</Key></Object><Object><Key>key2</Key></Object></Delete>";
        let (entries, quiet) = parse_delete_objects_xml(xml).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].key, "key1");
        assert_eq!(entries[1].key, "key2");
        assert!(!quiet);
    }

    #[test]
    fn parse_delete_objects_with_version_id() {
        let xml = b"<Delete><Object><Key>key1</Key><VersionId>v1</VersionId></Object></Delete>";
        let (entries, _) = parse_delete_objects_xml(xml).unwrap();
        assert_eq!(entries[0].version_id.as_deref(), Some("v1"));
    }

    #[test]
    fn parse_delete_objects_quiet_mode() {
        let xml = b"<Delete><Quiet>true</Quiet><Object><Key>key1</Key></Object></Delete>";
        let (_, quiet) = parse_delete_objects_xml(xml).unwrap();
        assert!(quiet);
    }

    #[test]
    fn parse_delete_objects_empty_body_rejected() {
        assert!(parse_delete_objects_xml(b"").is_err());
    }

    #[test]
    fn parse_delete_objects_missing_key_rejected() {
        let xml = b"<Delete><Object><VersionId>v1</VersionId></Object></Delete>";
        assert!(parse_delete_objects_xml(xml).is_err());
    }

    // ── delete_objects_result_xml ────────────────────────────────────

    #[test]
    fn delete_result_xml_with_deletions_and_errors() {
        use crate::coordinator::{DeleteError, DeletedObject};
        let deleted = vec![DeletedObject {
            key: "key1".to_string(),
            version_id: 0,
            delete_marker: false,
        }];
        let errors = vec![DeleteError {
            key: "key2".to_string(),
            code: "AccessDenied".to_string(),
            message: "Access Denied".to_string(),
        }];
        let xml = delete_objects_result_xml(&deleted, &errors, false);
        assert!(xml.contains("<Deleted><Key>key1</Key>"));
        assert!(xml.contains("<Error><Key>key2</Key>"));
        assert!(xml.contains("<Code>AccessDenied</Code>"));
        assert!(xml.contains("DeleteResult"));
    }

    #[test]
    fn delete_result_xml_quiet_mode_omits_deleted() {
        use crate::coordinator::DeletedObject;
        let deleted = vec![DeletedObject {
            key: "key1".to_string(),
            version_id: 0,
            delete_marker: false,
        }];
        let xml = delete_objects_result_xml(&deleted, &[], true);
        assert!(!xml.contains("<Deleted>"));
        assert!(xml.contains("DeleteResult"));
    }

    // ── list_object_versions_xml ────────────────────────────────────

    #[test]
    fn list_object_versions_xml_format() {
        use crate::coordinator::{ListObjectVersionsResult, VersionEntry};
        let result = ListObjectVersionsResult {
            versions: vec![VersionEntry {
                key: "my-key".to_string(),
                version_id: 0,
                is_latest: true,
                size: 42,
                etag: "\"abc123\"".to_string(),
                last_modified: 1685000000000,
                is_delete_marker: false,
            }],
            is_truncated: false,
            next_key_marker: None,
            next_version_id_marker: None,
        };
        let xml = list_object_versions_xml("bucket", None, None, 1000, &result);
        assert!(xml.contains("ListVersionsResult"));
        assert!(xml.contains("<Version>"));
        assert!(xml.contains("<Key>my-key</Key>"));
        assert!(xml.contains("<VersionId>null</VersionId>"));
        assert!(xml.contains("<IsLatest>true</IsLatest>"));
        assert!(xml.contains("<Size>42</Size>"));
        assert!(xml.contains("<KeyMarker/>"));
        assert!(!xml.contains("<KeyCount>"));
    }

    #[test]
    fn list_object_versions_xml_empty() {
        use crate::coordinator::ListObjectVersionsResult;
        let result = ListObjectVersionsResult {
            versions: vec![],
            is_truncated: false,
            next_key_marker: None,
            next_version_id_marker: None,
        };
        let xml = list_object_versions_xml("bucket", None, None, 1000, &result);
        assert!(xml.contains("ListVersionsResult"));
        assert!(!xml.contains("<Version>"));
    }

    #[test]
    fn list_object_versions_xml_with_prefix_and_key_marker() {
        use crate::coordinator::ListObjectVersionsResult;
        let result = ListObjectVersionsResult {
            versions: vec![],
            is_truncated: false,
            next_key_marker: None,
            next_version_id_marker: None,
        };
        let xml = list_object_versions_xml("bucket", Some("photos/"), Some("key1"), 100, &result);
        assert!(xml.contains("<Prefix>photos/</Prefix>"));
        assert!(xml.contains("<KeyMarker>key1</KeyMarker>"));
    }

    #[test]
    fn format_timestamp_basic() {
        // 2023-05-25T00:00:00.000Z = 1684972800000 ms
        let ts = format_timestamp(1684972800000);
        assert_eq!(ts, "2023-05-25T00:00:00.000Z");
    }

    #[test]
    fn format_timestamp_epoch() {
        let ts = format_timestamp(0);
        assert_eq!(ts, "1970-01-01T00:00:00.000Z");
    }

    // ── copy_object_result_xml ────────────────────────────────────

    #[test]
    fn copy_object_result_xml_format() {
        let xml = copy_object_result_xml("\"abcdef1234567890\"", 1705321845000);
        assert!(xml.contains("<?xml"));
        assert!(xml.contains("<CopyObjectResult>"));
        assert!(xml.contains("<ETag>&quot;abcdef1234567890&quot;</ETag>"));
        assert!(xml.contains("<LastModified>2024-01-15T12:30:45.000Z</LastModified>"));
        assert!(xml.contains("</CopyObjectResult>"));
    }

    // ── versioning XML ──────────────────────────────────────────────

    #[test]
    fn parse_versioning_enabled() {
        let xml = b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>";
        assert_eq!(parse_versioning_config_xml(xml).unwrap(), 1);
    }

    #[test]
    fn parse_versioning_suspended() {
        let xml = b"<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>";
        assert_eq!(parse_versioning_config_xml(xml).unwrap(), 2);
    }

    #[test]
    fn parse_versioning_invalid_status() {
        let xml = b"<VersioningConfiguration><Status>Invalid</Status></VersioningConfiguration>";
        assert!(parse_versioning_config_xml(xml).is_err());
    }

    #[test]
    fn parse_versioning_missing_status() {
        let xml = b"<VersioningConfiguration></VersioningConfiguration>";
        assert!(parse_versioning_config_xml(xml).is_err());
    }

    #[test]
    fn get_bucket_versioning_disabled() {
        let xml = get_bucket_versioning_xml(0);
        assert!(xml.contains("VersioningConfiguration"));
        assert!(!xml.contains("<Status>"));
    }

    #[test]
    fn get_bucket_versioning_enabled() {
        let xml = get_bucket_versioning_xml(1);
        assert!(xml.contains("<Status>Enabled</Status>"));
    }

    #[test]
    fn get_bucket_versioning_suspended() {
        let xml = get_bucket_versioning_xml(2);
        assert!(xml.contains("<Status>Suspended</Status>"));
    }

    // ── CORS XML ─────────────────────────────────────────────────────

    #[test]
    fn parse_cors_config_basic() {
        let xml = b"\
            <CORSConfiguration>\
                <CORSRule>\
                    <AllowedOrigin>http://example.com</AllowedOrigin>\
                    <AllowedMethod>GET</AllowedMethod>\
                    <AllowedMethod>PUT</AllowedMethod>\
                    <AllowedHeader>*</AllowedHeader>\
                    <ExposeHeader>x-amz-request-id</ExposeHeader>\
                    <MaxAgeSeconds>3600</MaxAgeSeconds>\
                </CORSRule>\
            </CORSConfiguration>";
        let config = parse_cors_config_xml(xml).unwrap();
        assert_eq!(config.rules.len(), 1);
        let rule = &config.rules[0];
        assert_eq!(rule.allowed_origins, vec!["http://example.com"]);
        assert_eq!(rule.allowed_methods, vec!["GET", "PUT"]);
        assert_eq!(rule.allowed_headers, vec!["*"]);
        assert_eq!(rule.expose_headers, vec!["x-amz-request-id"]);
        assert_eq!(rule.max_age_seconds, Some(3600));
    }

    #[test]
    fn parse_cors_config_multiple_rules() {
        let xml = b"\
            <CORSConfiguration>\
                <CORSRule>\
                    <AllowedOrigin>http://a.com</AllowedOrigin>\
                    <AllowedMethod>GET</AllowedMethod>\
                </CORSRule>\
                <CORSRule>\
                    <AllowedOrigin>http://b.com</AllowedOrigin>\
                    <AllowedMethod>POST</AllowedMethod>\
                </CORSRule>\
            </CORSConfiguration>";
        let config = parse_cors_config_xml(xml).unwrap();
        assert_eq!(config.rules.len(), 2);
        assert_eq!(config.rules[0].allowed_origins, vec!["http://a.com"]);
        assert_eq!(config.rules[1].allowed_origins, vec!["http://b.com"]);
    }

    #[test]
    fn parse_cors_config_missing_origin() {
        let xml = b"\
            <CORSConfiguration>\
                <CORSRule>\
                    <AllowedMethod>GET</AllowedMethod>\
                </CORSRule>\
            </CORSConfiguration>";
        assert!(parse_cors_config_xml(xml).is_err());
    }

    #[test]
    fn parse_cors_config_missing_method() {
        let xml = b"\
            <CORSConfiguration>\
                <CORSRule>\
                    <AllowedOrigin>http://example.com</AllowedOrigin>\
                </CORSRule>\
            </CORSConfiguration>";
        assert!(parse_cors_config_xml(xml).is_err());
    }

    #[test]
    fn parse_cors_config_invalid_method() {
        let xml = b"\
            <CORSConfiguration>\
                <CORSRule>\
                    <AllowedOrigin>http://example.com</AllowedOrigin>\
                    <AllowedMethod>PATCH</AllowedMethod>\
                </CORSRule>\
            </CORSConfiguration>";
        assert!(parse_cors_config_xml(xml).is_err());
    }

    #[test]
    fn parse_cors_config_no_rules() {
        let xml = b"<CORSConfiguration></CORSConfiguration>";
        assert!(parse_cors_config_xml(xml).is_err());
    }

    #[test]
    fn parse_cors_config_missing_wrapper() {
        let xml = b"<CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule>";
        assert!(parse_cors_config_xml(xml).is_err());
    }

    #[test]
    fn get_cors_config_xml_round_trip() {
        let config = crate::cors::CorsConfiguration {
            rules: vec![crate::cors::CorsRule {
                allowed_origins: vec!["http://example.com".into()],
                allowed_methods: vec!["GET".into(), "PUT".into()],
                allowed_headers: vec!["*".into()],
                expose_headers: vec!["x-amz-request-id".into()],
                max_age_seconds: Some(3600),
            }],
        };
        let xml = get_cors_config_xml(&config);
        assert!(xml.contains("<CORSConfiguration>"));
        assert!(xml.contains("<AllowedOrigin>http://example.com</AllowedOrigin>"));
        assert!(xml.contains("<AllowedMethod>GET</AllowedMethod>"));
        assert!(xml.contains("<AllowedMethod>PUT</AllowedMethod>"));
        assert!(xml.contains("<AllowedHeader>*</AllowedHeader>"));
        assert!(xml.contains("<ExposeHeader>x-amz-request-id</ExposeHeader>"));
        assert!(xml.contains("<MaxAgeSeconds>3600</MaxAgeSeconds>"));

        // Parse it back
        let parsed = parse_cors_config_xml(xml.as_bytes()).unwrap();
        assert_eq!(parsed.rules.len(), 1);
        assert_eq!(parsed.rules[0].allowed_origins, vec!["http://example.com"]);
    }

    #[test]
    fn cors_xml_round_trip_multiple_origins() {
        let config = crate::cors::CorsConfiguration {
            rules: vec![crate::cors::CorsRule {
                allowed_origins: vec![
                    "http://first.com".into(),
                    "http://second.com".into(),
                    "http://*.example.com".into(),
                ],
                allowed_methods: vec!["GET".into()],
                allowed_headers: vec![],
                expose_headers: vec![],
                max_age_seconds: None,
            }],
        };
        let xml = get_cors_config_xml(&config);
        assert!(xml.contains("<AllowedOrigin>http://first.com</AllowedOrigin>"));
        assert!(xml.contains("<AllowedOrigin>http://second.com</AllowedOrigin>"));
        assert!(xml.contains("<AllowedOrigin>http://*.example.com</AllowedOrigin>"));

        let parsed = parse_cors_config_xml(xml.as_bytes()).unwrap();
        assert_eq!(parsed.rules.len(), 1);
        assert_eq!(
            parsed.rules[0].allowed_origins,
            vec![
                "http://first.com",
                "http://second.com",
                "http://*.example.com"
            ]
        );
    }

    // ── Tagging XML ─────────────────────────────────────────────────

    #[test]
    fn parse_tagging_xml_basic() {
        let xml =
            b"<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>";
        let tags = parse_tagging_xml(xml, 10).unwrap();
        assert_eq!(tags, vec![("env".to_string(), "prod".to_string())]);
    }

    #[test]
    fn parse_tagging_xml_multiple() {
        let xml = b"<Tagging><TagSet>\
            <Tag><Key>k1</Key><Value>v1</Value></Tag>\
            <Tag><Key>k2</Key><Value>v2</Value></Tag>\
            </TagSet></Tagging>";
        let tags = parse_tagging_xml(xml, 10).unwrap();
        assert_eq!(tags.len(), 2);
        assert_eq!(tags[0], ("k1".to_string(), "v1".to_string()));
        assert_eq!(tags[1], ("k2".to_string(), "v2".to_string()));
    }

    #[test]
    fn parse_tagging_xml_empty_value() {
        let xml = b"<Tagging><TagSet><Tag><Key>k</Key><Value></Value></Tag></TagSet></Tagging>";
        let tags = parse_tagging_xml(xml, 10).unwrap();
        assert_eq!(tags, vec![("k".to_string(), String::new())]);
    }

    #[test]
    fn parse_tagging_xml_empty_tagset() {
        let xml = b"<Tagging><TagSet></TagSet></Tagging>";
        let tags = parse_tagging_xml(xml, 10).unwrap();
        assert!(tags.is_empty());
    }

    #[test]
    fn parse_tagging_xml_too_many() {
        let mut xml = String::from("<Tagging><TagSet>");
        for i in 0..11 {
            xml.push_str(&format!("<Tag><Key>k{i}</Key><Value>v</Value></Tag>"));
        }
        xml.push_str("</TagSet></Tagging>");
        let err = parse_tagging_xml(xml.as_bytes(), 10).unwrap_err();
        assert!(matches!(err, ServerError::InvalidTag { .. }));
    }

    #[test]
    fn parse_tagging_xml_key_too_long() {
        let long_key = "k".repeat(129);
        let xml = format!(
            "<Tagging><TagSet><Tag><Key>{long_key}</Key><Value>v</Value></Tag></TagSet></Tagging>"
        );
        let err = parse_tagging_xml(xml.as_bytes(), 10).unwrap_err();
        assert!(matches!(err, ServerError::InvalidTag { .. }));
    }

    #[test]
    fn parse_tagging_xml_value_too_long() {
        let long_val = "v".repeat(257);
        let xml = format!(
            "<Tagging><TagSet><Tag><Key>k</Key><Value>{long_val}</Value></Tag></TagSet></Tagging>"
        );
        let err = parse_tagging_xml(xml.as_bytes(), 10).unwrap_err();
        assert!(matches!(err, ServerError::InvalidTag { .. }));
    }

    #[test]
    fn parse_tagging_xml_duplicate_keys() {
        let xml = b"<Tagging><TagSet>\
            <Tag><Key>k</Key><Value>v1</Value></Tag>\
            <Tag><Key>k</Key><Value>v2</Value></Tag>\
            </TagSet></Tagging>";
        let err = parse_tagging_xml(xml, 10).unwrap_err();
        assert!(matches!(err, ServerError::InvalidTag { .. }));
    }

    #[test]
    fn parse_tagging_xml_aws_prefix() {
        let xml = b"<Tagging><TagSet><Tag><Key>aws:internal</Key><Value>v</Value></Tag></TagSet></Tagging>";
        let err = parse_tagging_xml(xml, 10).unwrap_err();
        assert!(matches!(err, ServerError::InvalidTag { .. }));
    }

    #[test]
    fn parse_tagging_xml_missing_tagging_element() {
        let xml = b"<TagSet><Tag><Key>k</Key><Value>v</Value></Tag></TagSet>";
        let err = parse_tagging_xml(xml, 10).unwrap_err();
        assert!(matches!(err, ServerError::InvalidRequest { .. }));
    }

    #[test]
    fn parse_tagging_xml_missing_tagset_element() {
        let xml = b"<Tagging><Tag><Key>k</Key><Value>v</Value></Tag></Tagging>";
        let err = parse_tagging_xml(xml, 10).unwrap_err();
        assert!(matches!(err, ServerError::InvalidRequest { .. }));
    }

    #[test]
    fn get_tagging_xml_round_trip() {
        let tags = vec![
            ("env".to_string(), "prod".to_string()),
            ("team".to_string(), "platform".to_string()),
        ];
        let xml = get_tagging_xml(&tags);
        let parsed = parse_tagging_xml(xml.as_bytes(), 10).unwrap();
        assert_eq!(parsed, tags);
    }

    #[test]
    fn get_tagging_xml_empty() {
        let xml = get_tagging_xml(&[]);
        assert!(xml.contains("<TagSet></TagSet>"));
        let parsed = parse_tagging_xml(xml.as_bytes(), 10).unwrap();
        assert!(parsed.is_empty());
    }

    #[test]
    fn get_tagging_xml_escapes_special_chars() {
        let tags = vec![("k&1".to_string(), "v<2>".to_string())];
        let xml = get_tagging_xml(&tags);
        assert!(xml.contains("k&amp;1"));
        assert!(xml.contains("v&lt;2&gt;"));
        let parsed = parse_tagging_xml(xml.as_bytes(), 10).unwrap();
        assert_eq!(parsed, tags);
    }

    // ── URL-encoded tags ────────────────────────────────────────────

    #[test]
    fn parse_url_encoded_tags_basic() {
        let tags = parse_url_encoded_tags("key1=value1&key2=value2").unwrap();
        assert_eq!(tags.len(), 2);
        assert_eq!(tags[0], ("key1".to_string(), "value1".to_string()));
        assert_eq!(tags[1], ("key2".to_string(), "value2".to_string()));
    }

    #[test]
    fn parse_url_encoded_tags_empty() {
        let tags = parse_url_encoded_tags("").unwrap();
        assert!(tags.is_empty());
    }

    #[test]
    fn parse_url_encoded_tags_percent_encoded() {
        let tags = parse_url_encoded_tags("k%201=v%201").unwrap();
        assert_eq!(tags[0], ("k 1".to_string(), "v 1".to_string()));
    }

    #[test]
    fn parse_url_encoded_tags_plus_as_space() {
        let tags = parse_url_encoded_tags("k+1=v+1").unwrap();
        assert_eq!(tags[0], ("k 1".to_string(), "v 1".to_string()));
    }

    #[test]
    fn parse_url_encoded_tags_too_many() {
        let input: String = (0..11)
            .map(|i| format!("k{i}=v{i}"))
            .collect::<Vec<_>>()
            .join("&");
        let err = parse_url_encoded_tags(&input).unwrap_err();
        assert!(matches!(err, ServerError::InvalidTag { .. }));
    }

    #[test]
    fn parse_url_encoded_tags_empty_value() {
        let tags = parse_url_encoded_tags("k=").unwrap();
        assert_eq!(tags[0], ("k".to_string(), String::new()));
    }

    #[test]
    fn parse_url_encoded_tags_multibyte_utf8() {
        // é = U+00E9 = 0xC3 0xA9 in UTF-8
        let tags = parse_url_encoded_tags("caf%C3%A9=cr%C3%A8me").unwrap();
        assert_eq!(tags[0].0, "café");
        assert_eq!(tags[0].1, "crème");
    }

    #[test]
    fn parse_url_encoded_tags_invalid_utf8() {
        // 0xFF is not valid in any UTF-8 sequence
        let err = parse_url_encoded_tags("k=%FF").unwrap_err();
        assert!(matches!(err, ServerError::InvalidTag { .. }));
    }

    #[test]
    fn count_tags_basic() {
        let xml = get_tagging_xml(&[
            ("a".to_string(), "1".to_string()),
            ("b".to_string(), "2".to_string()),
        ]);
        assert_eq!(count_tags_in_xml(&xml), 2);
    }

    #[test]
    fn count_tags_empty() {
        let xml = get_tagging_xml(&[]);
        assert_eq!(count_tags_in_xml(&xml), 0);
    }

    // ── PublicAccessBlock XML ──────────────────────────────────────────

    #[test]
    fn parse_public_access_block_all_true() {
        let xml = b"<PublicAccessBlockConfiguration>\
            <BlockPublicAcls>true</BlockPublicAcls>\
            <IgnorePublicAcls>true</IgnorePublicAcls>\
            <BlockPublicPolicy>true</BlockPublicPolicy>\
            <RestrictPublicBuckets>true</RestrictPublicBuckets>\
            </PublicAccessBlockConfiguration>";
        let config = parse_public_access_block_xml(xml).unwrap();
        assert!(config.block_public_acls);
        assert!(config.ignore_public_acls);
        assert!(config.block_public_policy);
        assert!(config.restrict_public_buckets);
    }

    #[test]
    fn parse_public_access_block_all_false() {
        let xml = b"<PublicAccessBlockConfiguration>\
            <BlockPublicAcls>false</BlockPublicAcls>\
            <IgnorePublicAcls>false</IgnorePublicAcls>\
            <BlockPublicPolicy>false</BlockPublicPolicy>\
            <RestrictPublicBuckets>false</RestrictPublicBuckets>\
            </PublicAccessBlockConfiguration>";
        let config = parse_public_access_block_xml(xml).unwrap();
        assert!(!config.block_public_acls);
        assert!(!config.ignore_public_acls);
        assert!(!config.block_public_policy);
        assert!(!config.restrict_public_buckets);
    }

    #[test]
    fn parse_public_access_block_missing_elements_default_false() {
        let xml = b"<PublicAccessBlockConfiguration>\
            <BlockPublicAcls>true</BlockPublicAcls>\
            </PublicAccessBlockConfiguration>";
        let config = parse_public_access_block_xml(xml).unwrap();
        assert!(config.block_public_acls);
        assert!(!config.ignore_public_acls);
        assert!(!config.block_public_policy);
        assert!(!config.restrict_public_buckets);
    }

    #[test]
    fn parse_public_access_block_empty() {
        let xml = b"<PublicAccessBlockConfiguration></PublicAccessBlockConfiguration>";
        let config = parse_public_access_block_xml(xml).unwrap();
        assert!(!config.block_public_acls);
        assert!(!config.ignore_public_acls);
        assert!(!config.block_public_policy);
        assert!(!config.restrict_public_buckets);
    }

    #[test]
    fn parse_public_access_block_missing_wrapper() {
        let xml = b"<BlockPublicAcls>true</BlockPublicAcls>";
        assert!(parse_public_access_block_xml(xml).is_err());
    }

    #[test]
    fn public_access_block_xml_round_trip() {
        let config = PublicAccessBlockConfig {
            block_public_acls: true,
            ignore_public_acls: false,
            block_public_policy: true,
            restrict_public_buckets: false,
        };
        let xml = get_public_access_block_xml(&config);
        let parsed = parse_public_access_block_xml(xml.as_bytes()).unwrap();
        assert_eq!(parsed, config);
    }

    // ── Ownership controls XML tests ────────────────────────────────

    #[test]
    fn ownership_controls_xml_round_trip_enforced() {
        let xml = get_ownership_controls_xml("BucketOwnerEnforced");
        let parsed = parse_ownership_controls_xml(xml.as_bytes()).unwrap();
        assert_eq!(parsed, "BucketOwnerEnforced");
    }

    #[test]
    fn ownership_controls_xml_round_trip_preferred() {
        let xml = get_ownership_controls_xml("BucketOwnerPreferred");
        let parsed = parse_ownership_controls_xml(xml.as_bytes()).unwrap();
        assert_eq!(parsed, "BucketOwnerPreferred");
    }

    #[test]
    fn ownership_controls_xml_round_trip_object_writer() {
        let xml = get_ownership_controls_xml("ObjectWriter");
        let parsed = parse_ownership_controls_xml(xml.as_bytes()).unwrap();
        assert_eq!(parsed, "ObjectWriter");
    }

    #[test]
    fn parse_ownership_controls_xml_invalid_value() {
        let xml = b"<OwnershipControls><Rule><ObjectOwnership>Invalid</ObjectOwnership></Rule></OwnershipControls>";
        assert!(parse_ownership_controls_xml(xml).is_err());
    }

    #[test]
    fn parse_ownership_controls_xml_missing_rule() {
        let xml = b"<OwnershipControls></OwnershipControls>";
        assert!(parse_ownership_controls_xml(xml).is_err());
    }

    #[test]
    fn parse_ownership_controls_xml_missing_root() {
        let xml = b"<Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule>";
        assert!(parse_ownership_controls_xml(xml).is_err());
    }

    #[test]
    fn parse_ownership_controls_xml_invalid_utf8() {
        let xml: &[u8] = &[0xFF, 0xFE];
        assert!(parse_ownership_controls_xml(xml).is_err());
    }
}
