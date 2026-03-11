/// Hand-formatted XML for S3 responses. No XML library dependency.
use crate::coordinator::{
    BucketSummary, CompletePart, DeleteError, DeletedObject, ListMultipartUploadsResult,
    ListObjectVersionsResult, ListObjectsResult, ListPartsResult, ObjectPartsInfo,
};
use crate::error::ServerError;
use auth::canonical::uri_encode_path;
use checksum::ChecksumAlgorithm;
use s3_types::BucketVersioningState;
#[cfg(test)]
use s3_types::VersionId;

use super::response::format_version_id;

/// Format a POST Object 201 response XML.
#[must_use]
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
#[must_use]
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

/// Format a `ListAllMyBucketsResult` XML response.
#[must_use]
pub fn list_buckets_xml(buckets: &[BucketSummary], owner_principal: &str) -> String {
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

/// Format a `ListBucketResult` (`ListObjectsV2`) XML response.
#[allow(clippy::too_many_arguments)]
#[must_use]
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

/// Format a `ListBucketResult` (`ListObjects` v1) XML response.
#[must_use]
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

/// An entry in a `DeleteObjects` request.
#[derive(Debug)]
pub struct DeleteObjectEntry {
    pub key: String,
    pub version_id: Option<String>,
}

/// Parse a `DeleteObjects` XML request body.
///
/// Returns the list of object entries and the quiet flag.
pub fn parse_delete_objects_xml(
    data: &[u8],
) -> Result<(Vec<DeleteObjectEntry>, bool), ServerError> {
    let text = std::str::from_utf8(data).map_err(|_| ServerError::MalformedXML {
        reason: "invalid UTF-8 in delete XML body".to_string(),
    })?;

    // Require <Delete> wrapper
    if !text.contains("<Delete") {
        return Err(ServerError::MalformedXML {
            reason: "missing <Delete> element".to_string(),
        });
    }

    // Detect quiet mode
    let quiet = extract_tag_content(text, "Quiet").is_some_and(|v| v == "true");

    // Parse <Object> blocks
    let mut entries = Vec::new();
    let mut search_from = 0;
    while let Some(start) = text[search_from..].find("<Object>") {
        let abs_start = search_from + start + "<Object>".len();
        let end =
            text[abs_start..]
                .find("</Object>")
                .ok_or_else(|| ServerError::MalformedXML {
                    reason: "unclosed <Object> element".to_string(),
                })?;
        let block = &text[abs_start..abs_start + end];

        let key = extract_tag_content(block, "Key").ok_or_else(|| ServerError::MalformedXML {
            reason: "Object missing <Key> element".to_string(),
        })?;
        let key = xml_unescape(key);
        let version_id = extract_tag_content(block, "VersionId").map(xml_unescape);

        entries.push(DeleteObjectEntry { key, version_id });

        search_from = abs_start + end + "</Object>".len();
    }

    if entries.len() > 1000 {
        return Err(ServerError::MalformedXML {
            reason: "delete objects list too large (max 1000)".to_string(),
        });
    }

    Ok((entries, quiet))
}

/// Extract the text content of a simple XML tag (no attributes, no nesting).
fn extract_tag_content<'a>(xml: &'a str, tag: &str) -> Option<&'a str> {
    let close = format!("</{tag}>");
    // Try exact match first: <Tag>
    let open_exact = format!("<{tag}>");
    if let Some(pos) = xml.find(&open_exact) {
        let start = pos + open_exact.len();
        let end = xml[start..].find(&close)? + start;
        return Some(&xml[start..end]);
    }
    // Try match with attributes: <Tag ...>
    let open_prefix = format!("<{tag} ");
    let pos = xml.find(&open_prefix)?;
    let gt = xml[pos..].find('>')? + pos;
    let start = gt + 1;
    let end = xml[start..].find(&close)? + start;
    Some(&xml[start..end])
}

/// Format a `DeleteResult` XML response.
#[must_use]
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

/// Parse a `PutBucketVersioning` XML request body.
///
/// Returns the versioning state as a `BucketVersioningState` enum.
pub fn parse_versioning_config_xml(data: &[u8]) -> Result<BucketVersioningState, ServerError> {
    let text = std::str::from_utf8(data).map_err(|_| ServerError::MalformedXML {
        reason: "invalid UTF-8 in versioning XML body".to_string(),
    })?;

    if let Some(status) = extract_tag_content(text, "Status") {
        match status {
            "Enabled" => Ok(BucketVersioningState::Enabled),
            "Suspended" => Ok(BucketVersioningState::Suspended),
            _other => Err(ServerError::MalformedXML {
                reason: "The XML you provided was not well-formed or did not validate against our published schema".to_string(),
            }),
        }
    } else {
        Err(ServerError::IllegalVersioningConfiguration {
            reason: "The Versioning element must be specified".to_string(),
        })
    }
}

/// Format a `GetBucketVersioning` XML response.
#[must_use]
pub fn get_bucket_versioning_xml(state: BucketVersioningState) -> String {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
    );

    match state {
        BucketVersioningState::Enabled => xml.push_str("<Status>Enabled</Status>"),
        BucketVersioningState::Suspended => xml.push_str("<Status>Suspended</Status>"),
        BucketVersioningState::Disabled => {} // empty element per S3 spec
    }

    xml.push_str("</VersioningConfiguration>");
    xml
}

/// Format a `ListVersionsResult` XML response.
#[must_use]
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
#[must_use]
pub fn xml_escape(s: &str) -> String {
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
    let text = std::str::from_utf8(data).map_err(|_| ServerError::MalformedXML {
        reason: "invalid UTF-8 in CORS XML body".to_string(),
    })?;

    if !text.contains("<CORSConfiguration") {
        return Err(ServerError::MalformedXML {
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
                .ok_or_else(|| ServerError::MalformedXML {
                    reason: "unclosed <CORSRule> element".to_string(),
                })?;
        let block = &text[abs_start..abs_start + end];

        // Parse AllowedOrigin (1+ required)
        let allowed_origins: Vec<String> = extract_all_tag_contents(block, "AllowedOrigin")
            .into_iter()
            .map(|s| xml_unescape(&s))
            .collect();
        if allowed_origins.is_empty() {
            return Err(ServerError::MalformedXML {
                reason: "CORSRule missing <AllowedOrigin> element".to_string(),
            });
        }

        // Parse AllowedMethod (1+ required)
        let allowed_methods: Vec<String> = extract_all_tag_contents(block, "AllowedMethod")
            .into_iter()
            .map(|s| xml_unescape(&s))
            .collect();
        if allowed_methods.is_empty() {
            return Err(ServerError::MalformedXML {
                reason: "CORSRule missing <AllowedMethod> element".to_string(),
            });
        }
        for m in &allowed_methods {
            if !valid_methods.contains(&m.as_str()) {
                return Err(ServerError::InvalidRequest {
                    reason: format!(
                        "Found unsupported HTTP method in CORS config. Unsupported method is {m}"
                    ),
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
                s.parse::<u32>().map_err(|_| ServerError::MalformedXML {
                    reason: format!("invalid MaxAgeSeconds: {s}"),
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
        return Err(ServerError::MalformedXML {
            reason: "CORS configuration must contain at least one rule".to_string(),
        });
    }
    if rules.len() > 100 {
        return Err(ServerError::MalformedXML {
            reason: "CORS configuration must contain at most 100 rules".to_string(),
        });
    }

    Ok(crate::cors::CorsConfiguration { rules })
}

/// Serialize a CORS configuration to XML.
#[must_use]
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
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
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

/// Format a `CopyObjectResult` XML response.
#[must_use]
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

/// Format a `CopyPartResult` XML response.
#[must_use]
pub fn copy_part_result_xml(etag: &str, last_modified: u64) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <CopyPartResult>\
         <ETag>{}</ETag>\
         <LastModified>{}</LastModified>\
         </CopyPartResult>",
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

    format!("{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{seconds:02}.000Z")
}

/// Convert days since Unix epoch to (year, month, day).
fn days_to_date(days: i64) -> (i64, u32, u32) {
    // Algorithm from Howard Hinnant's date algorithms
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = i64::from(yoe) + era * 400;
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
    let text = std::str::from_utf8(data).map_err(|_| ServerError::MalformedXML {
        reason: "invalid UTF-8 in tagging XML body".to_string(),
    })?;

    // Require both wrapper elements
    let tagging_block =
        extract_tag_content(text, "Tagging").ok_or(ServerError::MalformedXML {
            reason: "missing <Tagging> element in tagging XML".to_string(),
        })?;
    let tag_set =
        extract_tag_content(tagging_block, "TagSet").ok_or(ServerError::MalformedXML {
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
                reason: format!("tag key must be 1-128 characters, got {key_chars}"),
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
                reason: format!("tag value must be 0-256 characters, got {value_chars}"),
            });
        }

        if !seen_keys.insert(key.clone()) {
            return Err(ServerError::InvalidTag {
                reason: format!("duplicate tag key: {key}"),
            });
        }

        tags.push((key, value));
    }

    Ok(tags)
}

/// Serialize a list of (key, value) tag pairs into S3 tagging XML.
#[must_use]
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
                reason: format!("tag key must be 1-128 characters, got {key_chars}"),
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
                reason: format!("tag value must be 0-256 characters, got {value_chars}"),
            });
        }

        if !seen_keys.insert(key.clone()) {
            return Err(ServerError::InvalidTag {
                reason: format!("duplicate tag key: {key}"),
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
#[must_use]
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
            let hi = iter.next().ok_or(ServerError::InvalidArgument {
                reason: "invalid percent-encoding in tagging header".to_string(),
            })?;
            let lo = iter.next().ok_or(ServerError::InvalidArgument {
                reason: "invalid percent-encoding in tagging header".to_string(),
            })?;
            let byte = decode_hex_pair(hi, lo).ok_or(ServerError::InvalidArgument {
                reason: "invalid percent-encoding in tagging header".to_string(),
            })?;
            bytes.push(byte);
        } else {
            bytes.push(b);
        }
    }
    String::from_utf8(bytes).map_err(|_| ServerError::InvalidTag {
        reason: "The TagValue you have provided is invalid".to_string(),
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
    let text = std::str::from_utf8(data).map_err(|_| ServerError::MalformedXML {
        reason: "invalid UTF-8 in public access block XML body".to_string(),
    })?;
    let inner = extract_tag_content(text, "PublicAccessBlockConfiguration").ok_or_else(|| {
        ServerError::MalformedXML {
            reason: "missing PublicAccessBlockConfiguration element".to_string(),
        }
    })?;

    fn parse_bool_element(xml: &str, tag: &str) -> bool {
        extract_tag_content(xml, tag).is_some_and(|v| v.trim().eq_ignore_ascii_case("true"))
    }

    Ok(PublicAccessBlockConfig {
        block_public_acls: parse_bool_element(inner, "BlockPublicAcls"),
        ignore_public_acls: parse_bool_element(inner, "IgnorePublicAcls"),
        block_public_policy: parse_bool_element(inner, "BlockPublicPolicy"),
        restrict_public_buckets: parse_bool_element(inner, "RestrictPublicBuckets"),
    })
}

/// Serialize a `PublicAccessBlockConfig` into S3 response XML.
#[must_use]
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
    let text = std::str::from_utf8(data).map_err(|_| ServerError::MalformedXML {
        reason: "invalid UTF-8 in ownership controls XML body".to_string(),
    })?;
    let inner = extract_tag_content(text, "OwnershipControls").ok_or_else(|| {
        ServerError::MalformedXML {
            reason: "missing OwnershipControls element".to_string(),
        }
    })?;
    let rule = extract_tag_content(inner, "Rule").ok_or_else(|| ServerError::MalformedXML {
        reason: "missing Rule element in OwnershipControls".to_string(),
    })?;
    let value = extract_tag_content(rule, "ObjectOwnership")
        .ok_or_else(|| ServerError::MalformedXML {
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
#[must_use]
pub fn get_ownership_controls_xml(object_ownership: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <OwnershipControls xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Rule><ObjectOwnership>{}</ObjectOwnership></Rule>\
         </OwnershipControls>",
        xml_escape(object_ownership),
    )
}

/// Recognized object attribute names for `GetObjectAttributes`.
const VALID_OBJECT_ATTRIBUTES: &[&str] = &[
    "ETag",
    "Checksum",
    "ObjectParts",
    "StorageClass",
    "ObjectSize",
];

/// Check whether an attribute name is valid for `GetObjectAttributes`.
#[must_use]
pub fn is_valid_object_attribute(name: &str) -> bool {
    VALID_OBJECT_ATTRIBUTES.contains(&name)
}

/// Build a `<GetObjectAttributesResponse>` XML body.
///
/// `requested` is the set of attribute names from the `x-amz-object-attributes`
/// header. Only requested attributes appear in the response.
///
/// `etag` should be the quoted `ETag` string (quotes will be stripped).
/// `checksum_entries` are the `x-amz-checksum-*` metadata entries.
#[must_use]
pub fn get_object_attributes_xml(
    requested: &[&str],
    etag: &str,
    size: u64,
    checksum_entries: &[(&str, &str)],
    object_parts: Option<&ObjectPartsInfo>,
    checksum_algorithm: Option<ChecksumAlgorithm>,
) -> String {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <GetObjectAttributesResponse>",
    );

    for &attr in requested {
        match attr {
            "ETag" => {
                // Strip surrounding quotes from etag
                let unquoted = etag.trim_matches('"');
                xml.push_str("<ETag>");
                xml.push_str(&xml_escape(unquoted));
                xml.push_str("</ETag>");
            }
            "Checksum" => {
                if !checksum_entries.is_empty() {
                    xml.push_str("<Checksum>");
                    for &(header_key, value) in checksum_entries {
                        if header_key == "x-amz-checksum-type" {
                            xml.push_str("<ChecksumType>");
                            xml.push_str(&xml_escape(value));
                            xml.push_str("</ChecksumType>");
                        } else if let Some(xml_tag) = checksum_header_to_xml_tag(header_key) {
                            // Strip the composite "-N" suffix (part count) from checksum
                            // values. GetObjectAttributes uses ChecksumType to convey
                            // composite vs full-object; the hash itself has no suffix.
                            let bare = strip_composite_suffix(value);
                            xml.push('<');
                            xml.push_str(xml_tag);
                            xml.push('>');
                            xml.push_str(&xml_escape(bare));
                            xml.push_str("</");
                            xml.push_str(xml_tag);
                            xml.push('>');
                        }
                    }
                    xml.push_str("</Checksum>");
                }
            }
            "StorageClass" => {
                xml.push_str("<StorageClass>STANDARD</StorageClass>");
            }
            "ObjectSize" => {
                xml.push_str("<ObjectSize>");
                xml.push_str(&size.to_string());
                xml.push_str("</ObjectSize>");
            }
            "ObjectParts" => {
                if let Some(parts_info) = object_parts {
                    xml.push_str("<ObjectParts>");
                    xml.push_str("<PartsCount>");
                    xml.push_str(&parts_info.total_parts_count.to_string());
                    xml.push_str("</PartsCount>");
                    if parts_info.has_detail {
                        xml.push_str("<PartNumberMarker>");
                        xml.push_str(&parts_info.part_number_marker.to_string());
                        xml.push_str("</PartNumberMarker>");
                        xml.push_str("<MaxParts>");
                        xml.push_str(&parts_info.max_parts.to_string());
                        xml.push_str("</MaxParts>");
                        xml.push_str("<IsTruncated>");
                        xml.push_str(if parts_info.is_truncated {
                            "true"
                        } else {
                            "false"
                        });
                        xml.push_str("</IsTruncated>");
                        if let Some(next) = parts_info.next_part_number_marker {
                            xml.push_str("<NextPartNumberMarker>");
                            xml.push_str(&next.to_string());
                            xml.push_str("</NextPartNumberMarker>");
                        }
                        for part in &parts_info.parts {
                            xml.push_str("<Part>");
                            xml.push_str("<PartNumber>");
                            xml.push_str(&part.part_number.to_string());
                            xml.push_str("</PartNumber>");
                            xml.push_str("<Size>");
                            xml.push_str(&part.size.to_string());
                            xml.push_str("</Size>");
                            if let (Some(algo), Some(ref val)) =
                                (checksum_algorithm, &part.checksum)
                            {
                                let elem = algo.xml_element_name();
                                xml.push_str(&format!("<{elem}>{}</{elem}>", xml_escape(val)));
                            }
                            xml.push_str("</Part>");
                        }
                    }
                    xml.push_str("</ObjectParts>");
                }
            }
            _ => {}
        }
    }

    xml.push_str("</GetObjectAttributesResponse>");
    xml
}

/// Strip the composite checksum "-N" suffix (e.g. "abc=-3" → "abc=").
///
/// AWS returns the bare hash (no part count) in `GetObjectAttributes`; the part
/// count is conveyed by `<ChecksumType>COMPOSITE</ChecksumType>` instead.
/// Standard base64 never contains '-', so a trailing "-\d+" is always the
/// composite suffix.
fn strip_composite_suffix(value: &str) -> &str {
    if let Some(pos) = value.rfind('-') {
        if value[pos + 1..].bytes().all(|b| b.is_ascii_digit()) && !value[pos + 1..].is_empty() {
            return &value[..pos];
        }
    }
    value
}

/// Map a metadata header key like `x-amz-checksum-sha256` to an XML element
/// name like `ChecksumSHA256`.
fn checksum_header_to_xml_tag(header: &str) -> Option<&'static str> {
    match header {
        "x-amz-checksum-sha256" => Some("ChecksumSHA256"),
        "x-amz-checksum-sha1" => Some("ChecksumSHA1"),
        "x-amz-checksum-crc32" => Some("ChecksumCRC32"),
        "x-amz-checksum-crc32c" => Some("ChecksumCRC32C"),
        "x-amz-checksum-crc64nvme" => Some("ChecksumCRC64NVME"),
        _ => None,
    }
}

// ── Multipart upload XML ─────────────────────────────────────────

/// Format an `InitiateMultipartUploadResult` XML response.
#[must_use]
pub fn initiate_multipart_upload_xml(
    bucket: &str,
    key: &str,
    upload_id: &str,
    checksum_algorithm: Option<&str>,
    checksum_type: Option<&str>,
) -> String {
    let mut xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Bucket>{}</Bucket>\
         <Key>{}</Key>\
         <UploadId>{}</UploadId>",
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(upload_id),
    );
    if let Some(algo) = checksum_algorithm {
        xml.push_str("<ChecksumAlgorithm>");
        xml.push_str(algo);
        xml.push_str("</ChecksumAlgorithm>");
    }
    if let Some(ctype) = checksum_type {
        xml.push_str("<ChecksumType>");
        xml.push_str(ctype);
        xml.push_str("</ChecksumType>");
    }
    xml.push_str("</InitiateMultipartUploadResult>");
    xml
}

/// Percent-encode a logical key for use in URLs, preserving '/'.
///
/// Unlike `uri_encode_path` (which preserves existing %XX sequences for
/// canonical request paths), this encodes every byte that needs encoding,
/// including literal '%' characters. Use this for Location URLs where the
/// input is a logical object key, not a raw request path.
fn uri_encode_key(s: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(b as char);
            }
            _ => {
                out.push('%');
                out.push(HEX[(b >> 4) as usize] as char);
                out.push(HEX[(b & 0x0F) as usize] as char);
            }
        }
    }
    out
}

/// Format a `CompleteMultipartUploadResult` XML response.
#[must_use]
pub fn complete_multipart_upload_xml(
    bucket: &str,
    key: &str,
    etag: &str,
    checksum_algorithm: Option<ChecksumAlgorithm>,
    checksum_value: Option<&str>,
) -> String {
    // Location uses path-style: http://s3.amazonaws.com/<bucket>/<key>
    let location = format!(
        "http://s3.amazonaws.com/{}/{}",
        uri_encode_key(bucket),
        uri_encode_key(key)
    );
    let checksum_xml = match (checksum_algorithm, checksum_value) {
        (Some(algo), Some(val)) => {
            let elem = algo.xml_element_name();
            format!("<{elem}>{}</{elem}>", xml_escape(val))
        }
        _ => String::new(),
    };
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Location>{}</Location>\
         <Bucket>{}</Bucket>\
         <Key>{}</Key>\
         <ETag>{}</ETag>\
         {}\
         </CompleteMultipartUploadResult>",
        xml_escape(&location),
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(etag),
        checksum_xml,
    )
}

/// Format a `ListMultipartUploadsResult` XML response.
#[must_use]
pub fn list_multipart_uploads_xml(
    bucket: &str,
    prefix: Option<&str>,
    key_marker: Option<&str>,
    upload_id_marker: Option<&str>,
    max_uploads: u32,
    result: &ListMultipartUploadsResult,
) -> String {
    let mut xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <ListMultipartUploadsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Bucket>{}</Bucket>",
        xml_escape(bucket),
    );
    if let Some(p) = prefix {
        xml.push_str(&format!("<Prefix>{}</Prefix>", xml_escape(p)));
    } else {
        xml.push_str("<Prefix/>");
    }
    if let Some(km) = key_marker {
        xml.push_str(&format!("<KeyMarker>{}</KeyMarker>", xml_escape(km)));
    } else {
        xml.push_str("<KeyMarker/>");
    }
    if let Some(um) = upload_id_marker {
        xml.push_str(&format!(
            "<UploadIdMarker>{}</UploadIdMarker>",
            xml_escape(um)
        ));
    } else {
        xml.push_str("<UploadIdMarker/>");
    }
    xml.push_str(&format!("<MaxUploads>{max_uploads}</MaxUploads>"));
    xml.push_str(&format!(
        "<IsTruncated>{}</IsTruncated>",
        result.is_truncated
    ));
    if let Some(ref nkm) = result.next_key_marker {
        xml.push_str(&format!(
            "<NextKeyMarker>{}</NextKeyMarker>",
            xml_escape(nkm)
        ));
    }
    if let Some(ref num) = result.next_upload_id_marker {
        xml.push_str(&format!(
            "<NextUploadIdMarker>{}</NextUploadIdMarker>",
            xml_escape(num)
        ));
    }
    for upload in &result.uploads {
        xml.push_str(&format!(
            "<Upload>\
             <Key>{}</Key>\
             <UploadId>{}</UploadId>\
             <Initiated>{}</Initiated>\
             </Upload>",
            xml_escape(&upload.key),
            xml_escape(&upload.upload_id),
            format_timestamp(upload.initiated),
        ));
    }
    xml.push_str("</ListMultipartUploadsResult>");
    xml
}

/// Format a `ListPartsResult` XML response.
#[must_use]
pub fn list_parts_xml(
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number_marker: Option<u32>,
    max_parts: u32,
    result: &ListPartsResult,
) -> String {
    let mut xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <ListPartsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Bucket>{}</Bucket>\
         <Key>{}</Key>\
         <UploadId>{}</UploadId>",
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(upload_id),
    );
    if let Some(pm) = part_number_marker {
        xml.push_str(&format!("<PartNumberMarker>{pm}</PartNumberMarker>"));
    } else {
        xml.push_str("<PartNumberMarker>0</PartNumberMarker>");
    }
    xml.push_str(&format!("<MaxParts>{max_parts}</MaxParts>"));
    xml.push_str(&format!(
        "<IsTruncated>{}</IsTruncated>",
        result.is_truncated
    ));
    if let Some(npm) = result.next_part_number_marker {
        xml.push_str(&format!(
            "<NextPartNumberMarker>{npm}</NextPartNumberMarker>"
        ));
    }
    if let Some(algo) = result.checksum_algorithm {
        xml.push_str(&format!(
            "<ChecksumAlgorithm>{}</ChecksumAlgorithm>",
            algo.as_str()
        ));
    }
    for part in &result.parts {
        xml.push_str(&format!(
            "<Part>\
             <PartNumber>{}</PartNumber>\
             <LastModified>{}</LastModified>\
             <ETag>{}</ETag>\
             <Size>{}</Size>",
            part.part_number,
            format_timestamp(part.last_modified),
            xml_escape(&part.etag),
            part.size,
        ));
        if let (Some(algo), Some(ref val)) = (result.checksum_algorithm, &part.checksum) {
            let elem = algo.xml_element_name();
            xml.push_str(&format!("<{elem}>{}</{elem}>", xml_escape(val)));
        }
        xml.push_str("</Part>");
    }
    xml.push_str("</ListPartsResult>");
    xml
}

/// S3 checksum XML element names mapped to their algorithms.
const CHECKSUM_ELEMENTS: &[(&str, ChecksumAlgorithm)] = &[
    ("ChecksumCRC32C", ChecksumAlgorithm::Crc32c),
    ("ChecksumCRC32", ChecksumAlgorithm::Crc32),
    ("ChecksumSHA1", ChecksumAlgorithm::Sha1),
    ("ChecksumSHA256", ChecksumAlgorithm::Sha256),
    ("ChecksumCRC64NVME", ChecksumAlgorithm::Crc64nvme),
];

/// Extract the checksum element from a `<Part>` XML fragment.
/// Returns the algorithm and base64 value. Rejects multiple checksum elements.
fn extract_checksum_element(
    part_content: &str,
) -> Result<Option<(ChecksumAlgorithm, String)>, ServerError> {
    let mut found: Option<(ChecksumAlgorithm, String)> = None;
    // Note: ChecksumCRC32C must be checked before ChecksumCRC32 to avoid
    // prefix-matching CRC32C as CRC32 (already ordered in CHECKSUM_ELEMENTS).
    for &(elem, algo) in CHECKSUM_ELEMENTS {
        let open = format!("<{elem}>");
        let close = format!("</{elem}>");
        if let Some(start) = part_content.find(&open) {
            if found.is_some() {
                return Err(ServerError::MalformedXML {
                    reason: "multiple checksum elements in a single Part".to_string(),
                });
            }
            let val_start = start + open.len();
            if let Some(end) = part_content[val_start..].find(&close) {
                // Reject duplicate of the same element type.
                let after_close = val_start + end + close.len();
                if part_content[after_close..].contains(&open) {
                    return Err(ServerError::MalformedXML {
                        reason: "multiple checksum elements in a single Part".to_string(),
                    });
                }
                found = Some((
                    algo,
                    part_content[val_start..val_start + end].trim().to_string(),
                ));
            }
        }
    }
    Ok(found)
}

/// Parse a `CompleteMultipartUpload` request XML body into a list of parts.
///
/// Expected format:
/// ```xml
/// <CompleteMultipartUpload>
///   <Part><PartNumber>1</PartNumber><ETag>"abc"</ETag></Part>
///   ...
/// </CompleteMultipartUpload>
/// ```
pub fn parse_complete_multipart_upload_xml(body: &[u8]) -> Result<Vec<CompletePart>, ServerError> {
    let malformed = || ServerError::MalformedXML {
        reason: "malformed CompleteMultipartUpload XML".to_string(),
    };

    let s = std::str::from_utf8(body).map_err(|_| malformed())?;

    let mut parts = Vec::new();
    let mut pos = 0;

    while let Some(i) = s[pos..].find("<Part>") {
        let part_start = pos + i + 6;
        let part_end = s[part_start..].find("</Part>").ok_or_else(malformed)?;
        let part_content = &s[part_start..part_start + part_end];
        pos = part_start + part_end + 7;

        // Extract PartNumber
        let pn_start = part_content.find("<PartNumber>").ok_or_else(malformed)? + 12;
        let pn_end = part_content[pn_start..]
            .find("</PartNumber>")
            .ok_or_else(malformed)?;
        let part_number: u32 = part_content[pn_start..pn_start + pn_end]
            .trim()
            .parse()
            .map_err(|_| malformed())?;

        // Extract ETag
        let etag_start = part_content.find("<ETag>").ok_or_else(malformed)? + 6;
        let etag_end = part_content[etag_start..]
            .find("</ETag>")
            .ok_or_else(malformed)?;
        let etag = xml_unescape(part_content[etag_start..etag_start + etag_end].trim());

        // Extract optional per-part checksum (ChecksumCRC32, ChecksumSHA256, etc.)
        let checksum = extract_checksum_element(part_content)?;

        parts.push(CompletePart {
            part_number,
            etag,
            checksum,
        });
    }

    if parts.is_empty() {
        return Err(malformed());
    }

    Ok(parts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conditional::{DeleteCondition, WriteCondition};
    use crate::coordinator::ListEntry;
    use crate::coordinator::{
        Coordinator, DeleteEntry, DeleteObjectsRequest, ListObjectVersionsRequest,
        ListObjectsV2Request, PutObjectAcl, PutObjectRequest, Requester,
    };
    use crate::metadata_blob::MetadataBlob;
    use ec::EcConfig;
    use std::sync::Arc;
    use storage::SharedStorageNode;

    fn setup_coordinator(dir: &std::path::Path) -> Coordinator {
        let pg_ids: Vec<u32> = (0..4).collect();
        let storage_node = Arc::new(SharedStorageNode::open(dir, &pg_ids).unwrap());
        let ec_config = EcConfig::new(4, 2).unwrap();
        Coordinator::new(storage_node, ec_config, "us-east-1".to_string()).unwrap()
    }

    const NO_WRITE: &WriteCondition = &WriteCondition::None;
    const NO_DELETE: &DeleteCondition = &DeleteCondition::None;
    const TEST_REQUESTER: Requester<'static> = Requester::principal("default-owner");
    const NO_PUT_OBJECT_ACL: PutObjectAcl<'static> = PutObjectAcl::None;

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
        let buckets = vec![BucketSummary {
            name: "test-bucket".to_string(),
            owner_principal: "owner".to_string(),
            created_at: 1685000000000,
            versioning: BucketVersioningState::Disabled,
            public_read: false,
            public_access_block: None,
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
            version_id: VersionId::Null,
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
            version_id: VersionId::Null,
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
                version_id: VersionId::Null,
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
        assert_eq!(
            parse_versioning_config_xml(xml).unwrap(),
            BucketVersioningState::Enabled
        );
    }

    #[test]
    fn parse_versioning_suspended() {
        let xml = b"<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>";
        assert_eq!(
            parse_versioning_config_xml(xml).unwrap(),
            BucketVersioningState::Suspended
        );
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
        let xml = get_bucket_versioning_xml(BucketVersioningState::Disabled);
        assert!(xml.contains("VersioningConfiguration"));
        assert!(!xml.contains("<Status>"));
    }

    #[test]
    fn get_bucket_versioning_enabled() {
        let xml = get_bucket_versioning_xml(BucketVersioningState::Enabled);
        assert!(xml.contains("<Status>Enabled</Status>"));
    }

    #[test]
    fn get_bucket_versioning_suspended() {
        let xml = get_bucket_versioning_xml(BucketVersioningState::Suspended);
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
        assert!(matches!(err, ServerError::MalformedXML { .. }));
    }

    #[test]
    fn parse_tagging_xml_missing_tagset_element() {
        let xml = b"<Tagging><Tag><Key>k</Key><Value>v</Value></Tag></Tagging>";
        let err = parse_tagging_xml(xml, 10).unwrap_err();
        assert!(matches!(err, ServerError::MalformedXML { .. }));
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

    // ── GetObjectAttributes XML tests ───────────────────────────────

    #[test]
    fn get_object_attributes_all() {
        let xml = get_object_attributes_xml(
            &["ETag", "Checksum", "StorageClass", "ObjectSize"],
            "\"abc123\"",
            1024,
            &[("x-amz-checksum-sha256", "base64hash==")],
            None,
            None,
        );
        assert!(xml.contains("<GetObjectAttributesResponse>"));
        assert!(xml.contains("<ETag>abc123</ETag>"));
        assert!(xml.contains("<StorageClass>STANDARD</StorageClass>"));
        assert!(xml.contains("<ObjectSize>1024</ObjectSize>"));
        assert!(xml.contains("<Checksum><ChecksumSHA256>base64hash==</ChecksumSHA256></Checksum>"));
        assert!(xml.contains("</GetObjectAttributesResponse>"));
    }

    #[test]
    fn get_object_attributes_etag_only() {
        let xml = get_object_attributes_xml(&["ETag"], "\"abcdef\"", 0, &[], None, None);
        assert!(xml.contains("<ETag>abcdef</ETag>"));
        assert!(!xml.contains("<StorageClass>"));
        assert!(!xml.contains("<ObjectSize>"));
        assert!(!xml.contains("<Checksum>"));
    }

    #[test]
    fn get_object_attributes_size_only() {
        let xml = get_object_attributes_xml(&["ObjectSize"], "\"x\"", 42, &[], None, None);
        assert!(xml.contains("<ObjectSize>42</ObjectSize>"));
        assert!(!xml.contains("<ETag>"));
    }

    #[test]
    fn get_object_attributes_no_checksum_entries() {
        let xml = get_object_attributes_xml(&["Checksum"], "\"x\"", 0, &[], None, None);
        // Checksum element should be omitted when there are no checksum entries
        assert!(!xml.contains("<Checksum>"));
    }

    #[test]
    fn get_object_attributes_multiple_checksums() {
        let xml = get_object_attributes_xml(
            &["Checksum"],
            "\"x\"",
            0,
            &[
                ("x-amz-checksum-crc32", "AAAAAA=="),
                ("x-amz-checksum-sha256", "BBBBBB=="),
            ],
            None,
            None,
        );
        assert!(xml.contains("<ChecksumCRC32>AAAAAA==</ChecksumCRC32>"));
        assert!(xml.contains("<ChecksumSHA256>BBBBBB==</ChecksumSHA256>"));
    }

    #[test]
    fn get_object_attributes_object_parts_omitted() {
        let xml = get_object_attributes_xml(&["ObjectParts"], "\"x\"", 0, &[], None, None);
        // ObjectParts should be omitted for non-multipart objects
        assert!(!xml.contains("<ObjectParts>"));
    }

    #[test]
    fn get_object_attributes_object_parts_rendered() {
        use crate::coordinator::{ObjectPartEntry, ObjectPartsInfo};
        let parts_info = ObjectPartsInfo {
            total_parts_count: 3,
            has_detail: true,
            parts: vec![
                ObjectPartEntry {
                    part_number: 1,
                    size: 5242880,
                    checksum: None,
                },
                ObjectPartEntry {
                    part_number: 2,
                    size: 1024,
                    checksum: None,
                },
            ],
            is_truncated: true,
            next_part_number_marker: Some(2),
            max_parts: 2,
            part_number_marker: 0,
        };
        let xml =
            get_object_attributes_xml(&["ObjectParts"], "\"x\"", 0, &[], Some(&parts_info), None);
        assert!(xml.contains("<ObjectParts>"));
        assert!(xml.contains("<PartsCount>3</PartsCount>"));
        assert!(xml.contains("<PartNumberMarker>0</PartNumberMarker>"));
        assert!(xml.contains("<MaxParts>2</MaxParts>"));
        assert!(xml.contains("<IsTruncated>true</IsTruncated>"));
        assert!(xml.contains("<NextPartNumberMarker>2</NextPartNumberMarker>"));
        assert!(xml.contains("<Part><PartNumber>1</PartNumber><Size>5242880</Size></Part>"));
        assert!(xml.contains("<Part><PartNumber>2</PartNumber><Size>1024</Size></Part>"));
        assert!(xml.contains("</ObjectParts>"));
    }

    #[test]
    fn get_object_attributes_object_parts_not_truncated() {
        use crate::coordinator::{ObjectPartEntry, ObjectPartsInfo};
        let parts_info = ObjectPartsInfo {
            total_parts_count: 1,
            has_detail: true,
            parts: vec![ObjectPartEntry {
                part_number: 1,
                size: 100,
                checksum: None,
            }],
            is_truncated: false,
            next_part_number_marker: None,
            max_parts: 1000,
            part_number_marker: 0,
        };
        let xml =
            get_object_attributes_xml(&["ObjectParts"], "\"x\"", 0, &[], Some(&parts_info), None);
        assert!(xml.contains("<IsTruncated>false</IsTruncated>"));
        assert!(!xml.contains("<NextPartNumberMarker>"));
        assert!(xml.contains("<PartsCount>1</PartsCount>"));
    }

    #[test]
    fn get_object_attributes_object_parts_no_detail() {
        use crate::coordinator::ObjectPartsInfo;
        let parts_info = ObjectPartsInfo {
            total_parts_count: 3,
            has_detail: false,
            parts: Vec::new(),
            is_truncated: false,
            next_part_number_marker: None,
            max_parts: 1000,
            part_number_marker: 0,
        };
        let xml =
            get_object_attributes_xml(&["ObjectParts"], "\"x\"", 0, &[], Some(&parts_info), None);
        assert!(xml.contains("<ObjectParts><PartsCount>3</PartsCount></ObjectParts>"));
        assert!(!xml.contains("<IsTruncated>"));
        assert!(!xml.contains("<PartNumberMarker>"));
        assert!(!xml.contains("<MaxParts>"));
        assert!(!xml.contains("<Part>"));
    }

    #[test]
    fn valid_object_attributes() {
        assert!(is_valid_object_attribute("ETag"));
        assert!(is_valid_object_attribute("Checksum"));
        assert!(is_valid_object_attribute("ObjectParts"));
        assert!(is_valid_object_attribute("StorageClass"));
        assert!(is_valid_object_attribute("ObjectSize"));
        assert!(!is_valid_object_attribute("etag"));
        assert!(!is_valid_object_attribute("Size"));
        assert!(!is_valid_object_attribute(""));
    }

    // ── Multipart XML tests ─────────────────────────────────────────

    #[test]
    fn parse_complete_multipart_basic() {
        let xml = b"<CompleteMultipartUpload>\
            <Part><PartNumber>1</PartNumber><ETag>\"abc\"</ETag></Part>\
            <Part><PartNumber>2</PartNumber><ETag>\"def\"</ETag></Part>\
            </CompleteMultipartUpload>";
        let parts = parse_complete_multipart_upload_xml(xml).unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].part_number, 1);
        assert_eq!(parts[0].etag, "\"abc\"");
        assert_eq!(parts[1].part_number, 2);
        assert_eq!(parts[1].etag, "\"def\"");
    }

    #[test]
    fn parse_complete_multipart_with_whitespace() {
        let xml = b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
            <CompleteMultipartUpload>\n\
            <Part>\n\
              <PartNumber> 1 </PartNumber>\n\
              <ETag> \"etag1\" </ETag>\n\
            </Part>\n\
            </CompleteMultipartUpload>";
        let parts = parse_complete_multipart_upload_xml(xml).unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].part_number, 1);
        assert_eq!(parts[0].etag, "\"etag1\"");
    }

    #[test]
    fn parse_complete_multipart_etag_without_quotes() {
        // Unquoted ETags are preserved as-is (unusual but valid)
        let xml = b"<CompleteMultipartUpload>\
            <Part><PartNumber>1</PartNumber><ETag>abc123</ETag></Part>\
            </CompleteMultipartUpload>";
        let parts = parse_complete_multipart_upload_xml(xml).unwrap();
        assert_eq!(parts[0].etag, "abc123");
    }

    #[test]
    fn parse_complete_multipart_empty_body() {
        let xml = b"<CompleteMultipartUpload></CompleteMultipartUpload>";
        assert!(parse_complete_multipart_upload_xml(xml).is_err());
    }

    #[test]
    fn parse_complete_multipart_invalid_utf8() {
        let xml = &[0xFF, 0xFE, 0x00];
        assert!(parse_complete_multipart_upload_xml(xml).is_err());
    }

    #[test]
    fn parse_complete_multipart_missing_part_number() {
        let xml = b"<CompleteMultipartUpload>\
            <Part><ETag>\"abc\"</ETag></Part>\
            </CompleteMultipartUpload>";
        assert!(parse_complete_multipart_upload_xml(xml).is_err());
    }

    #[test]
    fn parse_complete_multipart_missing_etag() {
        let xml = b"<CompleteMultipartUpload>\
            <Part><PartNumber>1</PartNumber></Part>\
            </CompleteMultipartUpload>";
        assert!(parse_complete_multipart_upload_xml(xml).is_err());
    }

    #[test]
    fn initiate_multipart_upload_xml_format() {
        let xml = initiate_multipart_upload_xml("mybucket", "mykey", "upload123", None, None);
        assert!(xml.contains("<Bucket>mybucket</Bucket>"));
        assert!(xml.contains("<Key>mykey</Key>"));
        assert!(xml.contains("<UploadId>upload123</UploadId>"));
        assert!(xml.contains("InitiateMultipartUploadResult"));
        // No checksum elements when not set.
        assert!(!xml.contains("ChecksumAlgorithm"));
        assert!(!xml.contains("ChecksumType"));
    }

    #[test]
    fn complete_multipart_upload_xml_format() {
        let xml = complete_multipart_upload_xml("mybucket", "mykey", "\"etag123\"", None, None);
        assert!(xml.contains("<Location>http://s3.amazonaws.com/mybucket/mykey</Location>"));
        assert!(xml.contains("<Bucket>mybucket</Bucket>"));
        assert!(xml.contains("<Key>mykey</Key>"));
        assert!(xml.contains("<ETag>&quot;etag123&quot;</ETag>"));
        assert!(xml.contains("CompleteMultipartUploadResult"));
    }

    #[test]
    fn complete_multipart_upload_xml_location_encodes_key() {
        let xml = complete_multipart_upload_xml("mybucket", "path/to/my key", "\"e\"", None, None);
        assert!(xml.contains("mybucket/path/to/my%20key"));
    }

    #[test]
    fn complete_multipart_upload_xml_location_encodes_literal_percent() {
        // A key containing literal %20 should encode the % as %25
        let xml = complete_multipart_upload_xml("mybucket", "key%20name", "\"e\"", None, None);
        assert!(xml.contains("mybucket/key%2520name"));
    }

    #[test]
    fn complete_multipart_upload_xml_with_checksum() {
        let xml = complete_multipart_upload_xml(
            "mybucket",
            "mykey",
            "\"etag\"",
            Some(ChecksumAlgorithm::Sha256),
            Some("abc123=="),
        );
        assert!(
            xml.contains("<ChecksumSHA256>abc123==</ChecksumSHA256>"),
            "missing checksum element: {xml}"
        );
    }

    #[test]
    fn parse_complete_multipart_with_part_checksums() {
        let body = b"\
            <CompleteMultipartUpload>\
            <Part><PartNumber>1</PartNumber><ETag>\"e1\"</ETag>\
            <ChecksumCRC32>AABBCC==</ChecksumCRC32></Part>\
            <Part><PartNumber>2</PartNumber><ETag>\"e2\"</ETag></Part>\
            </CompleteMultipartUpload>";
        let parts = parse_complete_multipart_upload_xml(body).unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(
            parts[0].checksum,
            Some((ChecksumAlgorithm::Crc32, "AABBCC==".to_string()))
        );
        assert_eq!(parts[1].checksum, None);
    }

    #[test]
    fn parse_complete_multipart_multiple_checksum_elements_rejected() {
        let body = b"\
            <CompleteMultipartUpload>\
            <Part><PartNumber>1</PartNumber><ETag>\"e1\"</ETag>\
            <ChecksumCRC32>AA==</ChecksumCRC32>\
            <ChecksumSHA256>BB==</ChecksumSHA256></Part>\
            </CompleteMultipartUpload>";
        let err = parse_complete_multipart_upload_xml(body).unwrap_err();
        assert!(
            matches!(err, ServerError::MalformedXML { .. }),
            "expected MalformedXML, got {err:?}"
        );
    }

    #[test]
    fn parse_complete_multipart_duplicate_same_checksum_element_rejected() {
        let body = b"\
            <CompleteMultipartUpload>\
            <Part><PartNumber>1</PartNumber><ETag>\"e1\"</ETag>\
            <ChecksumCRC32>AA==</ChecksumCRC32>\
            <ChecksumCRC32>BB==</ChecksumCRC32></Part>\
            </CompleteMultipartUpload>";
        let err = parse_complete_multipart_upload_xml(body).unwrap_err();
        assert!(
            matches!(err, ServerError::MalformedXML { .. }),
            "expected MalformedXML, got {err:?}"
        );
    }

    #[test]
    fn uri_encode_key_encodes_all_special_chars() {
        assert_eq!(uri_encode_key("a/b"), "a/b");
        assert_eq!(uri_encode_key("hello world"), "hello%20world");
        assert_eq!(uri_encode_key("100%"), "100%25");
        assert_eq!(uri_encode_key("a-b_c.d~e"), "a-b_c.d~e");
    }

    #[test]
    fn initiate_xml_escapes_special_chars() {
        let xml = initiate_multipart_upload_xml("my&bucket", "key<>", "id\"1", None, None);
        assert!(xml.contains("my&amp;bucket"));
        assert!(xml.contains("key&lt;&gt;"));
        assert!(xml.contains("id&quot;1"));
    }

    #[test]
    fn initiate_xml_includes_checksum_fields() {
        let xml = initiate_multipart_upload_xml("b", "k", "u", Some("CRC32"), Some("FULL_OBJECT"));
        assert!(xml.contains("<ChecksumAlgorithm>CRC32</ChecksumAlgorithm>"));
        assert!(xml.contains("<ChecksumType>FULL_OBJECT</ChecksumType>"));
    }

    #[test]
    fn initiate_xml_checksum_algorithm_only() {
        let xml = initiate_multipart_upload_xml("b", "k", "u", Some("SHA256"), None);
        assert!(xml.contains("<ChecksumAlgorithm>SHA256</ChecksumAlgorithm>"));
        assert!(!xml.contains("ChecksumType"));
    }

    // ── ListMultipartUploads XML tests ───────────────────────────────

    #[test]
    fn list_multipart_uploads_xml_empty() {
        let result = ListMultipartUploadsResult {
            uploads: vec![],
            is_truncated: false,
            next_key_marker: None,
            next_upload_id_marker: None,
        };
        let xml = list_multipart_uploads_xml("mybucket", None, None, None, 1000, &result);
        assert!(xml.contains("<Bucket>mybucket</Bucket>"));
        assert!(xml.contains("<Prefix/>"));
        assert!(xml.contains("<KeyMarker/>"));
        assert!(xml.contains("<UploadIdMarker/>"));
        assert!(xml.contains("<MaxUploads>1000</MaxUploads>"));
        assert!(xml.contains("<IsTruncated>false</IsTruncated>"));
        assert!(!xml.contains("<Upload>"));
        assert!(!xml.contains("<NextKeyMarker>"));
        assert!(!xml.contains("<NextUploadIdMarker>"));
    }

    #[test]
    fn list_multipart_uploads_xml_with_entries() {
        use crate::coordinator::MultipartUploadEntry;
        let result = ListMultipartUploadsResult {
            uploads: vec![
                MultipartUploadEntry {
                    key: "file1.txt".to_string(),
                    upload_id: "id1".to_string(),
                    initiated: 1700000000000,
                },
                MultipartUploadEntry {
                    key: "file2.txt".to_string(),
                    upload_id: "id2".to_string(),
                    initiated: 1700000001000,
                },
            ],
            is_truncated: false,
            next_key_marker: None,
            next_upload_id_marker: None,
        };
        let xml = list_multipart_uploads_xml("mybucket", Some("file"), None, None, 1000, &result);
        assert!(xml.contains("<Prefix>file</Prefix>"));
        assert!(xml.contains("<Key>file1.txt</Key>"));
        assert!(xml.contains("<UploadId>id1</UploadId>"));
        assert!(xml.contains("<Key>file2.txt</Key>"));
        assert!(xml.contains("<UploadId>id2</UploadId>"));
        assert!(xml.contains("<Initiated>"));
    }

    #[test]
    fn list_multipart_uploads_xml_truncated() {
        use crate::coordinator::MultipartUploadEntry;
        let result = ListMultipartUploadsResult {
            uploads: vec![MultipartUploadEntry {
                key: "key1".to_string(),
                upload_id: "uid1".to_string(),
                initiated: 0,
            }],
            is_truncated: true,
            next_key_marker: Some("key1".to_string()),
            next_upload_id_marker: Some("uid1".to_string()),
        };
        let xml = list_multipart_uploads_xml(
            "mybucket",
            None,
            Some("marker"),
            Some("uid-marker"),
            1,
            &result,
        );
        assert!(xml.contains("<IsTruncated>true</IsTruncated>"));
        assert!(xml.contains("<KeyMarker>marker</KeyMarker>"));
        assert!(xml.contains("<UploadIdMarker>uid-marker</UploadIdMarker>"));
        assert!(xml.contains("<NextKeyMarker>key1</NextKeyMarker>"));
        assert!(xml.contains("<NextUploadIdMarker>uid1</NextUploadIdMarker>"));
        assert!(xml.contains("<MaxUploads>1</MaxUploads>"));
    }

    #[test]
    fn list_multipart_uploads_xml_escapes_keys() {
        use crate::coordinator::MultipartUploadEntry;
        let result = ListMultipartUploadsResult {
            uploads: vec![MultipartUploadEntry {
                key: "key&<>".to_string(),
                upload_id: "id\"'".to_string(),
                initiated: 0,
            }],
            is_truncated: false,
            next_key_marker: None,
            next_upload_id_marker: None,
        };
        let xml = list_multipart_uploads_xml("mybucket", None, None, None, 1000, &result);
        assert!(xml.contains("<Key>key&amp;&lt;&gt;</Key>"));
        assert!(xml.contains("<UploadId>id&quot;&apos;</UploadId>"));
    }

    // ── ListParts XML tests ──────────────────────────────────────────

    #[test]
    fn list_parts_xml_empty() {
        let result = ListPartsResult {
            parts: vec![],
            is_truncated: false,
            next_part_number_marker: None,
            checksum_algorithm: None,
            checksum_type: None,
        };
        let xml = list_parts_xml("mybucket", "mykey", "uid1", None, 1000, &result);
        assert!(xml.contains("<Bucket>mybucket</Bucket>"));
        assert!(xml.contains("<Key>mykey</Key>"));
        assert!(xml.contains("<UploadId>uid1</UploadId>"));
        assert!(xml.contains("<PartNumberMarker>0</PartNumberMarker>"));
        assert!(xml.contains("<MaxParts>1000</MaxParts>"));
        assert!(xml.contains("<IsTruncated>false</IsTruncated>"));
        assert!(!xml.contains("<Part>"));
    }

    #[test]
    fn list_parts_xml_with_entries() {
        use crate::coordinator::PartEntry;
        let result = ListPartsResult {
            parts: vec![
                PartEntry {
                    part_number: 1,
                    size: 5242880,
                    etag: "\"abc\"".to_string(),
                    last_modified: 1700000000000,
                    checksum: None,
                },
                PartEntry {
                    part_number: 2,
                    size: 1024,
                    etag: "\"def\"".to_string(),
                    last_modified: 1700000001000,
                    checksum: None,
                },
            ],
            is_truncated: false,
            next_part_number_marker: None,
            checksum_algorithm: None,
            checksum_type: None,
        };
        let xml = list_parts_xml("mybucket", "mykey", "uid1", None, 1000, &result);
        assert!(xml.contains("<PartNumber>1</PartNumber>"));
        assert!(xml.contains("<Size>5242880</Size>"));
        assert!(xml.contains("<ETag>&quot;abc&quot;</ETag>"));
        assert!(xml.contains("<PartNumber>2</PartNumber>"));
        assert!(xml.contains("<Size>1024</Size>"));
        assert!(xml.contains("<LastModified>"));
    }

    #[test]
    fn list_parts_xml_truncated_with_marker() {
        use crate::coordinator::PartEntry;
        let result = ListPartsResult {
            parts: vec![PartEntry {
                part_number: 3,
                size: 100,
                etag: "\"e\"".to_string(),
                last_modified: 0,
                checksum: None,
            }],
            is_truncated: true,
            next_part_number_marker: Some(3),
            checksum_algorithm: None,
            checksum_type: None,
        };
        let xml = list_parts_xml("mybucket", "mykey", "uid1", Some(2), 1, &result);
        assert!(xml.contains("<PartNumberMarker>2</PartNumberMarker>"));
        assert!(xml.contains("<MaxParts>1</MaxParts>"));
        assert!(xml.contains("<IsTruncated>true</IsTruncated>"));
        assert!(xml.contains("<NextPartNumberMarker>3</NextPartNumberMarker>"));
    }

    // ── Step 5: ListParts with checksums ───────────────────────────────

    #[test]
    fn list_parts_xml_with_checksum_algorithm_and_parts() {
        use crate::coordinator::PartEntry;
        use checksum::ChecksumAlgorithm;
        let result = ListPartsResult {
            parts: vec![
                PartEntry {
                    part_number: 1,
                    size: 5242880,
                    etag: "\"abc\"".to_string(),
                    last_modified: 1700000000000,
                    checksum: Some("AAAAAA==".to_string()),
                },
                PartEntry {
                    part_number: 2,
                    size: 1024,
                    etag: "\"def\"".to_string(),
                    last_modified: 1700000001000,
                    checksum: Some("BBBBBB==".to_string()),
                },
            ],
            is_truncated: false,
            next_part_number_marker: None,
            checksum_algorithm: Some(ChecksumAlgorithm::Crc32),
            checksum_type: None,
        };
        let xml = list_parts_xml("mybucket", "mykey", "uid1", None, 1000, &result);
        assert!(xml.contains("<ChecksumAlgorithm>CRC32</ChecksumAlgorithm>"));
        assert!(xml.contains("<ChecksumCRC32>AAAAAA==</ChecksumCRC32>"));
        assert!(xml.contains("<ChecksumCRC32>BBBBBB==</ChecksumCRC32>"));
    }

    #[test]
    fn list_parts_xml_checksum_omitted_when_no_algorithm() {
        use crate::coordinator::PartEntry;
        let result = ListPartsResult {
            parts: vec![PartEntry {
                part_number: 1,
                size: 100,
                etag: "\"e\"".to_string(),
                last_modified: 0,
                checksum: Some("AAAAAA==".to_string()),
            }],
            is_truncated: false,
            next_part_number_marker: None,
            checksum_algorithm: None,
            checksum_type: None,
        };
        let xml = list_parts_xml("mybucket", "mykey", "uid1", None, 1000, &result);
        // Without checksum_algorithm, per-part checksum elements should not be rendered
        assert!(!xml.contains("<ChecksumCRC32>"));
        assert!(!xml.contains("<ChecksumAlgorithm>"));
    }

    // ── Step 5: GetObjectAttributes with per-part checksums ──────────

    #[test]
    fn get_object_attributes_object_parts_with_checksums() {
        use crate::coordinator::{ObjectPartEntry, ObjectPartsInfo};
        use checksum::ChecksumAlgorithm;
        let parts_info = ObjectPartsInfo {
            total_parts_count: 2,
            has_detail: true,
            parts: vec![
                ObjectPartEntry {
                    part_number: 1,
                    size: 5242880,
                    checksum: Some("AAAAAA==".to_string()),
                },
                ObjectPartEntry {
                    part_number: 2,
                    size: 1024,
                    checksum: Some("BBBBBB==".to_string()),
                },
            ],
            is_truncated: false,
            next_part_number_marker: None,
            max_parts: 1000,
            part_number_marker: 0,
        };
        let xml = get_object_attributes_xml(
            &["ObjectParts"],
            "\"x\"",
            0,
            &[],
            Some(&parts_info),
            Some(ChecksumAlgorithm::Sha256),
        );
        assert!(xml.contains("<ChecksumSHA256>AAAAAA==</ChecksumSHA256>"));
        assert!(xml.contains("<ChecksumSHA256>BBBBBB==</ChecksumSHA256>"));
    }

    #[test]
    fn get_object_attributes_checksum_type_rendered() {
        let xml = get_object_attributes_xml(
            &["Checksum"],
            "\"x\"",
            0,
            &[
                ("x-amz-checksum-crc32", "AAAAAA=="),
                ("x-amz-checksum-type", "FULL_OBJECT"),
            ],
            None,
            None,
        );
        assert!(xml.contains("<Checksum>"));
        assert!(xml.contains("<ChecksumCRC32>AAAAAA==</ChecksumCRC32>"));
        assert!(xml.contains("<ChecksumType>FULL_OBJECT</ChecksumType>"));
        assert!(xml.contains("</Checksum>"));
    }

    #[test]
    fn get_object_attributes_strips_composite_suffix() {
        let xml = get_object_attributes_xml(
            &["Checksum"],
            "\"x\"",
            0,
            &[
                (
                    "x-amz-checksum-sha256",
                    "uWBwpe1dxI4Vw8Gf0X9ynOdw/SS6VBzfWm9giiv1sf4=-3",
                ),
                ("x-amz-checksum-type", "COMPOSITE"),
            ],
            None,
            None,
        );
        assert!(xml.contains(
            "<ChecksumSHA256>uWBwpe1dxI4Vw8Gf0X9ynOdw/SS6VBzfWm9giiv1sf4=</ChecksumSHA256>"
        ));
        assert!(!xml.contains("-3</ChecksumSHA256>"));
        assert!(xml.contains("<ChecksumType>COMPOSITE</ChecksumType>"));
    }

    #[test]
    fn strip_composite_suffix_works() {
        assert_eq!(strip_composite_suffix("abc=-3"), "abc=");
        assert_eq!(strip_composite_suffix("abc=-123"), "abc=");
        assert_eq!(strip_composite_suffix("abc="), "abc=");
        assert_eq!(strip_composite_suffix("plain"), "plain");
        // Empty after dash is not a suffix
        assert_eq!(strip_composite_suffix("abc=-"), "abc=-");
    }

    // ── Multipart error XML coverage ─────────────────────────────────

    #[test]
    fn error_xml_no_such_upload() {
        let xml = error_xml(
            "NoSuchUpload",
            "no such upload: abc",
            "/bucket/key",
            "req-1",
        );
        assert!(xml.contains("<Code>NoSuchUpload</Code>"));
        assert!(xml.contains("<Message>no such upload: abc</Message>"));
    }

    #[test]
    fn error_xml_invalid_part() {
        let xml = error_xml(
            "InvalidPart",
            "invalid part: part 3",
            "/bucket/key",
            "req-1",
        );
        assert!(xml.contains("<Code>InvalidPart</Code>"));
    }

    #[test]
    fn error_xml_invalid_part_order() {
        let xml = error_xml(
            "InvalidPartOrder",
            "invalid part order",
            "/bucket/key",
            "req-1",
        );
        assert!(xml.contains("<Code>InvalidPartOrder</Code>"));
    }

    #[test]
    fn error_xml_entity_too_small() {
        let xml = error_xml(
            "EntityTooSmall",
            "entity too small: part 1 is 100 bytes (min 5242880)",
            "/bucket/key",
            "req-1",
        );
        assert!(xml.contains("<Code>EntityTooSmall</Code>"));
        assert!(xml.contains("5242880"));
    }

    // ── Parse CompleteMultipartUpload edge cases ─────────────────────

    #[test]
    fn parse_complete_multipart_many_parts() {
        let mut xml = String::from("<CompleteMultipartUpload>");
        for i in 1..=100 {
            xml.push_str(&format!(
                "<Part><PartNumber>{i}</PartNumber><ETag>\"etag{i}\"</ETag></Part>"
            ));
        }
        xml.push_str("</CompleteMultipartUpload>");
        let parts = parse_complete_multipart_upload_xml(xml.as_bytes()).unwrap();
        assert_eq!(parts.len(), 100);
        assert_eq!(parts[0].part_number, 1);
        assert_eq!(parts[99].part_number, 100);
        assert_eq!(parts[49].etag, "\"etag50\"");
    }

    #[test]
    fn parse_complete_multipart_with_xml_declaration_and_namespace() {
        let xml = b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
            <CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
            <Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part>\
            </CompleteMultipartUpload>";
        let parts = parse_complete_multipart_upload_xml(xml).unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].part_number, 1);
        assert_eq!(parts[0].etag, "\"a\"");
    }

    #[test]
    fn parse_complete_multipart_invalid_part_number_zero() {
        // part_number=0 parses fine; validation is in coordinator
        let xml = b"<CompleteMultipartUpload>\
            <Part><PartNumber>0</PartNumber><ETag>\"a\"</ETag></Part>\
            </CompleteMultipartUpload>";
        let parts = parse_complete_multipart_upload_xml(xml).unwrap();
        assert_eq!(parts[0].part_number, 0);
    }

    #[test]
    fn parse_complete_multipart_negative_part_number() {
        let xml = b"<CompleteMultipartUpload>\
            <Part><PartNumber>-1</PartNumber><ETag>\"a\"</ETag></Part>\
            </CompleteMultipartUpload>";
        assert!(parse_complete_multipart_upload_xml(xml).is_err());
    }

    #[test]
    fn ceph_cleanup_workflow() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("test-bucket").unwrap();
        coord
            .put_object(&PutObjectRequest {
                bucket: "test-bucket",
                key: "dir/file1.txt",
                data: b"hello",
                metadata: &MetadataBlob::new(),
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            })
            .unwrap();
        coord
            .put_object(&PutObjectRequest {
                bucket: "test-bucket",
                key: "dir/file2.txt",
                data: b"world",
                metadata: &MetadataBlob::new(),
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            })
            .unwrap();
        coord
            .put_object(&PutObjectRequest {
                bucket: "test-bucket",
                key: "root.txt",
                data: b"root",
                metadata: &MetadataBlob::new(),
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            })
            .unwrap();

        let versions_result = coord
            .list_object_versions(&ListObjectVersionsRequest {
                bucket: "test-bucket",
                prefix: None,
                key_marker: None,
                version_id_marker: None,
                max_keys: 1000,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(versions_result.versions.len(), 3);

        let versions_xml =
            list_object_versions_xml("test-bucket", None, None, 1000, &versions_result);
        assert!(versions_xml.contains("<Key>dir/file1.txt</Key>"));
        assert!(versions_xml.contains("<Key>dir/file2.txt</Key>"));
        assert!(versions_xml.contains("<Key>root.txt</Key>"));
        for _ in 0..3 {
            assert!(versions_xml.contains("<VersionId>null</VersionId>"));
        }

        let list_result = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: "test-bucket",
                prefix: None,
                delimiter: None,
                continuation_token: None,
                max_keys: 1000,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(list_result.objects.len(), 3);

        let mut delete_xml = String::from("<Delete>");
        for obj in &list_result.objects {
            delete_xml.push_str(&format!("<Object><Key>{}</Key></Object>", obj.key));
        }
        delete_xml.push_str("</Delete>");

        let (xml_entries, quiet) = parse_delete_objects_xml(delete_xml.as_bytes()).unwrap();
        assert_eq!(xml_entries.len(), 3);
        assert!(!quiet);
        let entries: Vec<DeleteEntry> = xml_entries
            .iter()
            .map(|e| DeleteEntry {
                key: &e.key,
                version_id: None,
            })
            .collect();

        let delete_result = coord
            .delete_objects(&DeleteObjectsRequest {
                bucket: "test-bucket",
                entries: &entries,
                cond: NO_DELETE,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(delete_result.deleted.len(), 3);
        assert!(delete_result.errors.is_empty());

        let list_after = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: "test-bucket",
                prefix: None,
                delimiter: None,
                continuation_token: None,
                max_keys: 1000,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert!(list_after.objects.is_empty());
        coord
            .delete_bucket(&crate::coordinator::DeleteBucketRequest {
                name: "test-bucket",
                requester: TEST_REQUESTER,
            })
            .unwrap();
    }

    #[test]
    fn ceph_cleanup_workflow_paginated() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        for i in 0..5 {
            let key = format!("key-{i:02}");
            coord
                .put_object(&PutObjectRequest {
                    bucket: "bucket",
                    key: &key,
                    data: b"data",
                    metadata: &MetadataBlob::new(),
                    cond: NO_WRITE,
                    requester: TEST_REQUESTER,
                    acl: NO_PUT_OBJECT_ACL,
                })
                .unwrap();
        }

        let page1 = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: "bucket",
                prefix: None,
                delimiter: None,
                continuation_token: None,
                max_keys: 2,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(page1.objects.len(), 2);
        assert!(page1.is_truncated);
        let token = page1.next_continuation_token.clone().unwrap();

        let page2 = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: "bucket",
                prefix: None,
                delimiter: None,
                continuation_token: Some(&token),
                max_keys: 2,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(page2.objects.len(), 2);
        let token2 = page2.next_continuation_token.clone().unwrap();

        let page3 = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: "bucket",
                prefix: None,
                delimiter: None,
                continuation_token: Some(&token2),
                max_keys: 2,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(page3.objects.len(), 1);
        assert!(!page3.is_truncated);

        let all_keys: Vec<String> = page1
            .objects
            .iter()
            .chain(page2.objects.iter())
            .chain(page3.objects.iter())
            .map(|o| o.key.clone())
            .collect();
        assert_eq!(all_keys.len(), 5);

        let mut delete_xml = String::from("<Delete><Quiet>true</Quiet>");
        for key in &all_keys {
            delete_xml.push_str(&format!("<Object><Key>{key}</Key></Object>"));
        }
        delete_xml.push_str("</Delete>");

        let (xml_entries, quiet) = parse_delete_objects_xml(delete_xml.as_bytes()).unwrap();
        assert_eq!(xml_entries.len(), 5);
        assert!(quiet);
        let entries: Vec<DeleteEntry> = xml_entries
            .iter()
            .map(|e| DeleteEntry {
                key: &e.key,
                version_id: None,
            })
            .collect();

        let delete_result = coord
            .delete_objects(&DeleteObjectsRequest {
                bucket: "bucket",
                entries: &entries,
                cond: NO_DELETE,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(delete_result.deleted.len(), 5);
        assert!(delete_result.errors.is_empty());

        let result_xml =
            delete_objects_result_xml(&delete_result.deleted, &delete_result.errors, quiet);
        assert!(!result_xml.contains("<Deleted>"));
        assert!(result_xml.contains("DeleteResult"));

        coord
            .delete_bucket(&crate::coordinator::DeleteBucketRequest {
                name: "bucket",
                requester: TEST_REQUESTER,
            })
            .unwrap();
    }
}
