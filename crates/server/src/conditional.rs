/// Conditional request types and evaluation logic for S3 operations.
///
/// Implements RFC 7232 §6 evaluation order for reads, plus S3-specific
/// conditional semantics for writes and deletes.
use crate::error::ServerError;
use crate::etag::parse_etag;
use crate::http::request::S3Request;
use crate::http::response::parse_http_date;

/// Conditions for read operations (GET, HEAD, Range GET).
#[derive(Debug, Default)]
pub struct ReadCondition {
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
    pub if_modified_since: Option<u64>,
    pub if_unmodified_since: Option<u64>,
}

/// Conditions for write operations (PUT).
#[derive(Debug, Default)]
pub struct WriteCondition {
    pub if_match: Option<String>,
    pub if_none_match_any: bool,
}

/// Conditions for delete operations (DELETE, `DeleteObjects`).
#[derive(Debug, Default)]
pub struct DeleteCondition {
    pub if_match: Option<String>,
}

/// Extract read conditions from an S3 request's headers.
pub fn read_condition_from_headers(req: &S3Request) -> ReadCondition {
    ReadCondition {
        if_match: req.header("if-match").map(str::to_string),
        if_none_match: req.header("if-none-match").map(str::to_string),
        if_modified_since: req.header("if-modified-since").and_then(parse_http_date),
        if_unmodified_since: req.header("if-unmodified-since").and_then(parse_http_date),
    }
}

/// Extract write conditions from an S3 request's headers.
pub fn write_condition_from_headers(req: &S3Request) -> WriteCondition {
    let if_none_match_any = req.header("if-none-match").is_some_and(|s| s.trim() == "*");
    WriteCondition {
        if_match: req.header("if-match").map(str::to_string),
        if_none_match_any,
    }
}

/// Extract delete conditions from an S3 request's headers.
pub fn delete_condition_from_headers(req: &S3Request) -> DeleteCondition {
    DeleteCondition {
        if_match: req.header("if-match").map(str::to_string),
    }
}

/// Compare a single `ETag` value against an object's etag for equality.
/// Strips quotes and compares the underlying hex values. Returns true if they match.
fn etag_matches_one(header_etag: &str, object_etag: &str) -> bool {
    let h = header_etag.trim();
    // Try to parse both as CRC64 hex values for canonical comparison
    match (parse_etag(h), parse_etag(object_etag)) {
        (Some(a), Some(b)) => a == b,
        // Fallback: string comparison after stripping quotes
        _ => h.trim_matches('"') == object_etag.trim_matches('"'),
    }
}

/// Check if an If-Match or If-None-Match header value matches an object's etag.
///
/// Per RFC 7232, these headers can be `*` (matches any) or a comma-separated
/// list of quoted `ETag` values. S3 primarily uses single `ETag` values, but
/// we handle lists for spec compliance.
fn etags_match(header_value: &str, object_etag: &str) -> bool {
    let trimmed = header_value.trim();
    if trimmed == "*" {
        return true;
    }
    // Split on commas and check each entry
    trimmed
        .split(',')
        .any(|entry| etag_matches_one(entry, object_etag))
}

/// Check read conditions against an existing object.
///
/// Per RFC 7232 §6 evaluation order:
///   1. If-Match → 412 if etag mismatch
///   2. If-Unmodified-Since → 412 if modified after date
///   3. If-None-Match → 304 if etag matches
///   4. If-Modified-Since → 304 if not modified since date
///
/// # Errors
///
/// Returns `PreconditionFailed` (412) or `NotModified` (304).
pub fn check_read_conditions(
    cond: &ReadCondition,
    etag: &str,
    last_modified: u64,
) -> Result<(), ServerError> {
    // Step 1: If-Match — `*` always passes, otherwise 412 if no etag in list matches
    if let Some(ref required) = cond.if_match {
        if !etags_match(required, etag) {
            return Err(ServerError::PreconditionFailed);
        }
    }

    // Step 2: If-Unmodified-Since (only evaluated if If-Match is absent)
    if cond.if_match.is_none() {
        if let Some(since) = cond.if_unmodified_since {
            if last_modified > since {
                return Err(ServerError::PreconditionFailed);
            }
        }
    }

    // Step 3: If-None-Match — `*` always triggers 304, otherwise 304 if any etag matches
    if let Some(ref unwanted) = cond.if_none_match {
        if etags_match(unwanted, etag) {
            return Err(ServerError::NotModified {
                etag: etag.to_string(),
                last_modified,
            });
        }
    }

    // Step 4: If-Modified-Since (only evaluated if If-None-Match is absent)
    if cond.if_none_match.is_none() {
        if let Some(since) = cond.if_modified_since {
            if last_modified <= since {
                return Err(ServerError::NotModified {
                    etag: etag.to_string(),
                    last_modified,
                });
            }
        }
    }

    Ok(())
}

/// Check write conditions.
///
/// - If-None-Match: * → 412 if object exists (create-only)
/// - If-Match: <etag> → 412 if mismatch or object absent
///
/// # Errors
///
/// Returns `PreconditionFailed` (412).
pub fn check_write_conditions(
    cond: &WriteCondition,
    existing_etag: Option<&str>,
) -> Result<(), ServerError> {
    if cond.if_none_match_any && existing_etag.is_some() {
        return Err(ServerError::PreconditionFailed);
    }

    if let Some(ref required_etag) = cond.if_match {
        match existing_etag {
            None => {
                // Object doesn't exist — precondition cannot be satisfied
                return Err(ServerError::PreconditionFailed);
            }
            Some(obj_etag) => {
                if !etags_match(required_etag, obj_etag) {
                    return Err(ServerError::PreconditionFailed);
                }
            }
        }
    }

    Ok(())
}

/// Check delete conditions.
///
/// - If-Match: * → pass (object existence already checked by caller)
/// - If-Match: <etag> → 412 if mismatch
///
/// # Errors
///
/// Returns `PreconditionFailed` (412) if the etag does not match.
pub fn check_delete_conditions(cond: &DeleteCondition, etag: &str) -> Result<(), ServerError> {
    if let Some(ref required_etag) = cond.if_match {
        if required_etag.trim() == "*" {
            return Ok(());
        }
        if !etags_match(required_etag, etag) {
            return Err(ServerError::PreconditionFailed);
        }
    }
    Ok(())
}

/// Extract copy-source conditions from an S3 request's `x-amz-copy-source-if-*` headers.
pub fn copy_source_condition_from_headers(req: &S3Request) -> ReadCondition {
    ReadCondition {
        if_match: req.header("x-amz-copy-source-if-match").map(str::to_string),
        if_none_match: req
            .header("x-amz-copy-source-if-none-match")
            .map(str::to_string),
        if_modified_since: req
            .header("x-amz-copy-source-if-modified-since")
            .and_then(parse_http_date),
        if_unmodified_since: req
            .header("x-amz-copy-source-if-unmodified-since")
            .and_then(parse_http_date),
    }
}

/// Check source conditions for CopyObject.
///
/// Same evaluation order as read conditions (RFC 7232 §6), but all
/// failures return `PreconditionFailed` (412) — never `NotModified`.
pub fn check_copy_source_conditions(
    cond: &ReadCondition,
    etag: &str,
    last_modified: u64,
) -> Result<(), ServerError> {
    // Step 1: If-Match
    if let Some(ref required) = cond.if_match {
        if !etags_match(required, etag) {
            return Err(ServerError::PreconditionFailed);
        }
    }

    // Step 2: If-Unmodified-Since (only if If-Match absent)
    if cond.if_match.is_none() {
        if let Some(since) = cond.if_unmodified_since {
            if last_modified > since {
                return Err(ServerError::PreconditionFailed);
            }
        }
    }

    // Step 3: If-None-Match — returns 412 (not 304)
    if let Some(ref unwanted) = cond.if_none_match {
        if etags_match(unwanted, etag) {
            return Err(ServerError::PreconditionFailed);
        }
    }

    // Step 4: If-Modified-Since (only if If-None-Match absent) — returns 412 (not 304)
    if cond.if_none_match.is_none() {
        if let Some(since) = cond.if_modified_since {
            if last_modified <= since {
                return Err(ServerError::PreconditionFailed);
            }
        }
    }

    Ok(())
}

impl ReadCondition {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.if_match.is_none()
            && self.if_none_match.is_none()
            && self.if_modified_since.is_none()
            && self.if_unmodified_since.is_none()
    }
}

impl WriteCondition {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.if_match.is_none() && !self.if_none_match_any
    }
}

impl DeleteCondition {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.if_match.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::etag::format_etag;

    fn test_etag() -> String {
        format_etag(0xABCDEF1234567890)
    }

    fn other_etag() -> String {
        format_etag(0x1111111111111111)
    }

    // ── Read conditions ──────────────────────────────────────────────

    #[test]
    fn read_if_match_passes() {
        let cond = ReadCondition {
            if_match: Some(test_etag()),
            ..Default::default()
        };
        assert!(check_read_conditions(&cond, &test_etag(), 1000).is_ok());
    }

    #[test]
    fn read_if_match_fails() {
        let cond = ReadCondition {
            if_match: Some(other_etag()),
            ..Default::default()
        };
        let err = check_read_conditions(&cond, &test_etag(), 1000).unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    #[test]
    fn read_if_none_match_returns_not_modified() {
        let cond = ReadCondition {
            if_none_match: Some(test_etag()),
            ..Default::default()
        };
        let err = check_read_conditions(&cond, &test_etag(), 1000).unwrap_err();
        assert!(matches!(err, ServerError::NotModified { .. }));
    }

    #[test]
    fn read_if_none_match_passes() {
        let cond = ReadCondition {
            if_none_match: Some(other_etag()),
            ..Default::default()
        };
        assert!(check_read_conditions(&cond, &test_etag(), 1000).is_ok());
    }

    #[test]
    fn read_if_modified_since_not_modified() {
        let cond = ReadCondition {
            if_modified_since: Some(2000),
            ..Default::default()
        };
        // Object last_modified (1000) <= since (2000) → NotModified
        let err = check_read_conditions(&cond, &test_etag(), 1000).unwrap_err();
        assert!(matches!(err, ServerError::NotModified { .. }));
    }

    #[test]
    fn read_if_modified_since_modified() {
        let cond = ReadCondition {
            if_modified_since: Some(500),
            ..Default::default()
        };
        // Object last_modified (1000) > since (500) → Ok
        assert!(check_read_conditions(&cond, &test_etag(), 1000).is_ok());
    }

    #[test]
    fn read_if_unmodified_since_passes() {
        let cond = ReadCondition {
            if_unmodified_since: Some(2000),
            ..Default::default()
        };
        // Object last_modified (1000) <= since (2000) → Ok
        assert!(check_read_conditions(&cond, &test_etag(), 1000).is_ok());
    }

    #[test]
    fn read_if_unmodified_since_fails() {
        let cond = ReadCondition {
            if_unmodified_since: Some(500),
            ..Default::default()
        };
        // Object last_modified (1000) > since (500) → PreconditionFailed
        let err = check_read_conditions(&cond, &test_etag(), 1000).unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    #[test]
    fn read_evaluation_order_if_match_before_if_none_match() {
        // If-Match fails → 412, even if If-None-Match would say 304
        let cond = ReadCondition {
            if_match: Some(other_etag()),
            if_none_match: Some(test_etag()),
            ..Default::default()
        };
        let err = check_read_conditions(&cond, &test_etag(), 1000).unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    // ── Write conditions ──────────────────────────────────────────────

    #[test]
    fn write_if_none_match_star_prevents_overwrite() {
        let cond = WriteCondition {
            if_none_match_any: true,
            ..Default::default()
        };
        let err = check_write_conditions(&cond, Some(&test_etag())).unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    #[test]
    fn write_if_none_match_star_allows_create() {
        let cond = WriteCondition {
            if_none_match_any: true,
            ..Default::default()
        };
        assert!(check_write_conditions(&cond, None).is_ok());
    }

    #[test]
    fn write_if_match_allows_matching_overwrite() {
        let cond = WriteCondition {
            if_match: Some(test_etag()),
            ..Default::default()
        };
        assert!(check_write_conditions(&cond, Some(&test_etag())).is_ok());
    }

    #[test]
    fn write_if_match_prevents_stale_overwrite() {
        let cond = WriteCondition {
            if_match: Some(other_etag()),
            ..Default::default()
        };
        let err = check_write_conditions(&cond, Some(&test_etag())).unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    #[test]
    fn write_if_match_nonexistent_returns_412() {
        let cond = WriteCondition {
            if_match: Some(test_etag()),
            ..Default::default()
        };
        let err = check_write_conditions(&cond, None).unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    // ── Delete conditions ──────────────────────────────────────────────

    #[test]
    fn delete_if_match_passes() {
        let cond = DeleteCondition {
            if_match: Some(test_etag()),
        };
        assert!(check_delete_conditions(&cond, &test_etag()).is_ok());
    }

    #[test]
    fn delete_if_match_fails() {
        let cond = DeleteCondition {
            if_match: Some(other_etag()),
        };
        let err = check_delete_conditions(&cond, &test_etag()).unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    #[test]
    fn delete_if_match_wildcard_passes() {
        let cond = DeleteCondition {
            if_match: Some("*".to_string()),
        };
        assert!(check_delete_conditions(&cond, &test_etag()).is_ok());
    }

    #[test]
    fn no_conditions_always_passes() {
        let read = ReadCondition::default();
        assert!(check_read_conditions(&read, &test_etag(), 1000).is_ok());

        let write = WriteCondition::default();
        assert!(check_write_conditions(&write, Some(&test_etag())).is_ok());
        assert!(check_write_conditions(&write, None).is_ok());

        let delete = DeleteCondition::default();
        assert!(check_delete_conditions(&delete, &test_etag()).is_ok());
    }

    // ── is_empty ──────────────────────────────────────────────────────

    #[test]
    fn read_condition_is_empty() {
        assert!(ReadCondition::default().is_empty());
        assert!(!ReadCondition {
            if_match: Some("x".into()),
            ..Default::default()
        }
        .is_empty());
    }

    #[test]
    fn write_condition_is_empty() {
        assert!(WriteCondition::default().is_empty());
        assert!(!WriteCondition {
            if_none_match_any: true,
            ..Default::default()
        }
        .is_empty());
    }

    #[test]
    fn delete_condition_is_empty() {
        assert!(DeleteCondition::default().is_empty());
        assert!(!DeleteCondition {
            if_match: Some("x".into()),
        }
        .is_empty());
    }

    // ── etags_match ──────────────────────────────────────────────────

    #[test]
    fn etags_match_quoted_vs_unquoted() {
        assert!(etags_match("\"abcdef1234567890\"", "abcdef1234567890"));
        assert!(etags_match("abcdef1234567890", "\"abcdef1234567890\""));
    }

    #[test]
    fn etags_match_different() {
        assert!(!etags_match("\"abcdef1234567890\"", "\"1111111111111111\""));
    }

    #[test]
    fn etags_match_wildcard() {
        assert!(etags_match("*", "\"abcdef1234567890\""));
        assert!(etags_match("*", "anything"));
    }

    #[test]
    fn etags_match_comma_separated_list() {
        let list = format!("\"1111111111111111\", {}", test_etag());
        assert!(etags_match(&list, &test_etag()));
    }

    #[test]
    fn etags_match_comma_separated_list_no_match() {
        let list = "\"1111111111111111\", \"2222222222222222\"";
        assert!(!etags_match(list, &test_etag()));
    }

    // ── Wildcard semantics in conditions ────────────────────────────

    #[test]
    fn read_if_match_wildcard_passes() {
        let cond = ReadCondition {
            if_match: Some("*".to_string()),
            ..Default::default()
        };
        assert!(check_read_conditions(&cond, &test_etag(), 1000).is_ok());
    }

    #[test]
    fn read_if_none_match_wildcard_returns_304() {
        let cond = ReadCondition {
            if_none_match: Some("*".to_string()),
            ..Default::default()
        };
        let err = check_read_conditions(&cond, &test_etag(), 1000).unwrap_err();
        assert!(matches!(err, ServerError::NotModified { .. }));
    }

    #[test]
    fn read_if_match_comma_list_one_matches() {
        let list = format!("\"1111111111111111\", {}", test_etag());
        let cond = ReadCondition {
            if_match: Some(list),
            ..Default::default()
        };
        assert!(check_read_conditions(&cond, &test_etag(), 1000).is_ok());
    }

    #[test]
    fn read_if_match_comma_list_none_match() {
        let cond = ReadCondition {
            if_match: Some("\"1111111111111111\", \"2222222222222222\"".to_string()),
            ..Default::default()
        };
        let err = check_read_conditions(&cond, &test_etag(), 1000).unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    #[test]
    fn read_if_none_match_comma_list_one_matches() {
        let list = format!("\"1111111111111111\", {}", test_etag());
        let cond = ReadCondition {
            if_none_match: Some(list),
            ..Default::default()
        };
        let err = check_read_conditions(&cond, &test_etag(), 1000).unwrap_err();
        assert!(matches!(err, ServerError::NotModified { .. }));
    }

    // ── Copy source conditions ──────────────────────────────────────

    #[test]
    fn copy_source_if_match_passes() {
        let cond = ReadCondition {
            if_match: Some(test_etag()),
            ..Default::default()
        };
        assert!(check_copy_source_conditions(&cond, &test_etag(), 1000).is_ok());
    }

    #[test]
    fn copy_source_if_match_fails() {
        let cond = ReadCondition {
            if_match: Some(other_etag()),
            ..Default::default()
        };
        let err = check_copy_source_conditions(&cond, &test_etag(), 1000).unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    #[test]
    fn copy_source_if_none_match_matching_returns_412() {
        // Key difference from read: returns 412, NOT 304
        let cond = ReadCondition {
            if_none_match: Some(test_etag()),
            ..Default::default()
        };
        let err = check_copy_source_conditions(&cond, &test_etag(), 1000).unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    #[test]
    fn copy_source_if_none_match_passes() {
        let cond = ReadCondition {
            if_none_match: Some(other_etag()),
            ..Default::default()
        };
        assert!(check_copy_source_conditions(&cond, &test_etag(), 1000).is_ok());
    }

    #[test]
    fn copy_source_if_modified_since_not_modified_returns_412() {
        let cond = ReadCondition {
            if_modified_since: Some(2000),
            ..Default::default()
        };
        // Object last_modified (1000) <= since (2000) → 412 (not 304)
        let err = check_copy_source_conditions(&cond, &test_etag(), 1000).unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    #[test]
    fn copy_source_if_unmodified_since_modified_returns_412() {
        let cond = ReadCondition {
            if_unmodified_since: Some(500),
            ..Default::default()
        };
        // Object last_modified (1000) > since (500) → 412
        let err = check_copy_source_conditions(&cond, &test_etag(), 1000).unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }
}
