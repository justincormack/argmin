/// Core conditional request types and evaluation logic for S3 operations.
///
/// Implements RFC 7232 §6 evaluation order for reads, plus S3-specific
/// conditional semantics for writes and deletes.
///
/// This module contains only the condition types and their evaluation — no
/// HTTP parsing. Header extraction lives in the HTTP/frontend crate.
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::ServerError;
use crate::etag::parse_etag;

/// Return the current time in milliseconds since the Unix epoch.
fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Conditions for read operations (GET, HEAD, Range GET).
#[derive(Debug, Default)]
pub struct ReadCondition {
    pub if_match: Option<EtagMatchList>,
    pub if_none_match: Option<EtagMatchList>,
    pub if_modified_since: Option<u64>,
    pub if_unmodified_since: Option<u64>,
}

/// A specific ETag value for conditional requests.
///
/// Rejects the raw `*` wildcard token at construction time. A quoted entity
/// tag whose opaque value is `*` is still a specific ETag, and callers must not
/// evaluate it with wildcard semantics. The stored value is the entity tag
/// itself; HTTP quote marks are stripped at construction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpecificEtag(String);

impl SpecificEtag {
    /// Construct a `SpecificEtag`, rejecting raw `*` and stripping HTTP quotes.
    pub fn new(value: String) -> Result<Self, &'static str> {
        if value.trim() == "*" {
            return Err("wildcard ETag not allowed in this context");
        }
        let value = value.trim();
        let value = value
            .strip_prefix('"')
            .and_then(|value| value.strip_suffix('"'))
            .unwrap_or(value);
        Ok(Self(value.to_string()))
    }

    /// The underlying entity tag value, without HTTP quote marks.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A single ETag match token from a conditional header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EtagMatchToken {
    /// `*`
    Any,
    /// A specific ETag token.
    Specific(SpecificEtag),
}

/// A parsed `If-Match`/`If-None-Match` header value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EtagMatchList(Vec<EtagMatchToken>);

impl EtagMatchList {
    /// Parse a raw header value into wildcard/specific match tokens.
    #[must_use]
    pub fn from_header_value(value: &str) -> Self {
        let trimmed = value.trim();
        if trimmed == "*" {
            return Self(vec![EtagMatchToken::Any]);
        }

        Self(
            trimmed
                .split(',')
                .map(str::trim)
                .map(|entry| {
                    if entry == "*" {
                        EtagMatchToken::Any
                    } else {
                        EtagMatchToken::Specific(
                            SpecificEtag::new(entry.to_string())
                                .expect("non-wildcard token must parse as SpecificEtag"),
                        )
                    }
                })
                .collect(),
        )
    }

    #[must_use]
    pub fn matches(&self, object_etag: &str) -> bool {
        self.0.iter().any(|token| match token {
            EtagMatchToken::Any => true,
            EtagMatchToken::Specific(etag) => etag_matches_one(etag.as_str(), object_etag),
        })
    }
}

impl From<String> for EtagMatchList {
    fn from(value: String) -> Self {
        Self::from_header_value(&value)
    }
}

impl From<&str> for EtagMatchList {
    fn from(value: &str) -> Self {
        Self::from_header_value(value)
    }
}

/// Conditions for write operations (PUT).
///
/// AWS S3 only supports `If-Match: <etag>` and `If-None-Match: *` on writes.
/// This enum makes invalid combinations (e.g. `If-None-Match: <specific-etag>`,
/// `If-Match: *`) unrepresentable.
#[derive(Debug, Clone, Default)]
pub enum WriteCondition {
    /// No condition — unconditional write.
    #[default]
    None,
    /// `If-Match: <etag>` — only overwrite if the existing object matches.
    IfMatch(SpecificEtag),
    /// `If-None-Match: *` — create-only, fail if object already exists.
    IfNoneMatchStar,
}

impl WriteCondition {
    #[must_use]
    pub fn if_match_policy_value(&self) -> Option<&str> {
        match self {
            Self::IfMatch(etag) => Some(etag.as_str()),
            Self::None | Self::IfNoneMatchStar => None,
        }
    }

    #[must_use]
    pub const fn if_none_match_policy_value(&self) -> Option<&'static str> {
        match self {
            Self::IfNoneMatchStar => Some("*"),
            Self::None | Self::IfMatch(_) => None,
        }
    }
}

/// Conditions for delete operations (DELETE, `DeleteObjects`).
///
/// AWS S3 only supports `If-Match` on DeleteObject for general-purpose buckets.
#[derive(Debug, Clone, Default)]
pub enum DeleteCondition {
    /// No condition — unconditional delete.
    #[default]
    None,
    /// `If-Match: <etag-or-wildcard>` — only delete if the ETag matches.
    IfMatch(EtagMatchList),
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
#[cfg(test)]
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
        if !required.matches(etag) {
            return Err(ServerError::PreconditionFailed);
        }
    }

    // Step 2: If-Unmodified-Since (only evaluated if If-Match is absent)
    // Compare at second precision since HTTP dates have no sub-second component.
    if cond.if_match.is_none() {
        if let Some(since) = cond.if_unmodified_since {
            if last_modified / 1000 > since / 1000 {
                return Err(ServerError::PreconditionFailed);
            }
        }
    }

    // Step 3: If-None-Match — `*` always triggers 304, otherwise 304 if any etag matches
    if let Some(ref unwanted) = cond.if_none_match {
        if unwanted.matches(etag) {
            return Err(ServerError::NotModified {
                etag: etag.to_string(),
                last_modified,
            });
        }
    }

    // Step 4: If-Modified-Since (only evaluated if If-None-Match is absent)
    // Per RFC 7232 §3.3: ignore if the date is in the future.
    // Compare at second precision since HTTP dates have no sub-second component.
    if cond.if_none_match.is_none() {
        if let Some(since) = cond.if_modified_since {
            if since <= now_millis() && last_modified / 1000 <= since / 1000 {
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
    match cond {
        WriteCondition::None => Ok(()),
        WriteCondition::IfNoneMatchStar => {
            if existing_etag.is_some() {
                Err(ServerError::PreconditionFailed)
            } else {
                Ok(())
            }
        }
        WriteCondition::IfMatch(required_etag) => match existing_etag {
            None => Err(ServerError::PreconditionFailed),
            Some(obj_etag) => {
                if etag_matches_one(required_etag.as_str(), obj_etag) {
                    Ok(())
                } else {
                    Err(ServerError::PreconditionFailed)
                }
            }
        },
    }
}

/// Check delete conditions.
///
/// - If-Match: * → pass (object existence already checked by caller)
/// - If-Match: <etag> → 412 if mismatch
///
/// # Errors
///
/// Returns `PreconditionFailed` (412) if any condition fails.
pub fn check_delete_conditions(cond: &DeleteCondition, etag: &str) -> Result<(), ServerError> {
    match cond {
        DeleteCondition::None => Ok(()),
        DeleteCondition::IfMatch(required_etag) => {
            if required_etag.matches(etag) {
                // Wildcard matches any existing object
                Ok(())
            } else {
                Err(ServerError::PreconditionFailed)
            }
        }
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
        if !required.matches(etag) {
            return Err(ServerError::PreconditionFailed);
        }
    }

    // Step 2: If-Unmodified-Since (only if If-Match absent)
    // Compare at second precision since HTTP dates have no sub-second component.
    if cond.if_match.is_none() {
        if let Some(since) = cond.if_unmodified_since {
            if last_modified / 1000 > since / 1000 {
                return Err(ServerError::PreconditionFailed);
            }
        }
    }

    // Step 3: If-None-Match — returns 412 (not 304)
    if let Some(ref unwanted) = cond.if_none_match {
        if unwanted.matches(etag) {
            return Err(ServerError::PreconditionFailed);
        }
    }

    // Step 4: If-Modified-Since (only if If-None-Match absent) — returns 412 (not 304)
    // Per RFC 7232 §3.3: ignore if the date is in the future.
    // Compare at second precision since HTTP dates have no sub-second component.
    if cond.if_none_match.is_none() {
        if let Some(since) = cond.if_modified_since {
            if since <= now_millis() && last_modified / 1000 <= since / 1000 {
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
        matches!(self, Self::None)
    }
}

impl DeleteCondition {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        matches!(self, Self::None)
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
            if_match: Some(test_etag().into()),
            ..Default::default()
        };
        assert!(check_read_conditions(&cond, &test_etag(), 1000).is_ok());
    }

    #[test]
    fn read_if_match_fails() {
        let cond = ReadCondition {
            if_match: Some(other_etag().into()),
            ..Default::default()
        };
        let err = check_read_conditions(&cond, &test_etag(), 1000).unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    #[test]
    fn read_if_none_match_returns_not_modified() {
        let cond = ReadCondition {
            if_none_match: Some(test_etag().into()),
            ..Default::default()
        };
        let err = check_read_conditions(&cond, &test_etag(), 1000).unwrap_err();
        assert!(matches!(err, ServerError::NotModified { .. }));
    }

    #[test]
    fn read_if_none_match_passes() {
        let cond = ReadCondition {
            if_none_match: Some(other_etag().into()),
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
    fn read_if_modified_since_future_ignored() {
        // Per RFC 7232 §3.3: future dates must be ignored → Ok
        let future = now_millis() + 60_000;
        let cond = ReadCondition {
            if_modified_since: Some(future),
            ..Default::default()
        };
        assert!(check_read_conditions(&cond, &test_etag(), 1000).is_ok());
    }

    #[test]
    fn read_if_modified_since_same_second() {
        // Object at 1500ms, header at 1000ms (same second) → NotModified
        let cond = ReadCondition {
            if_modified_since: Some(1000),
            ..Default::default()
        };
        assert!(check_read_conditions(&cond, &test_etag(), 1500).is_err());
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
            if_match: Some(other_etag().into()),
            if_none_match: Some(test_etag().into()),
            ..Default::default()
        };
        let err = check_read_conditions(&cond, &test_etag(), 1000).unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    // ── Write conditions ──────────────────────────────────────────────

    #[test]
    fn write_if_none_match_star_prevents_overwrite() {
        let cond = WriteCondition::IfNoneMatchStar;
        let err = check_write_conditions(&cond, Some(&test_etag())).unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    #[test]
    fn write_if_none_match_star_allows_create() {
        let cond = WriteCondition::IfNoneMatchStar;
        assert!(check_write_conditions(&cond, None).is_ok());
    }

    #[test]
    fn write_if_match_allows_matching_overwrite() {
        let cond = WriteCondition::IfMatch(SpecificEtag::new(test_etag()).unwrap());
        assert!(check_write_conditions(&cond, Some(&test_etag())).is_ok());
    }

    #[test]
    fn specific_etag_stores_unquoted_entity_tag() {
        let etag = SpecificEtag::new(test_etag()).unwrap();
        assert_eq!(etag.as_str(), "abcdef1234567890");
    }

    #[test]
    fn write_if_match_quoted_star_is_specific_not_wildcard() {
        let etag = SpecificEtag::new("\"*\"".to_string()).unwrap();
        assert_eq!(etag.as_str(), "*");
        let cond = WriteCondition::IfMatch(etag);
        let err = check_write_conditions(&cond, Some(&test_etag())).unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    #[test]
    fn write_if_match_policy_value_uses_entity_tag() {
        let cond = WriteCondition::IfMatch(SpecificEtag::new(test_etag()).unwrap());
        assert_eq!(cond.if_match_policy_value(), Some("abcdef1234567890"));
    }

    #[test]
    fn write_if_match_prevents_stale_overwrite() {
        let cond = WriteCondition::IfMatch(SpecificEtag::new(other_etag()).unwrap());
        let err = check_write_conditions(&cond, Some(&test_etag())).unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    #[test]
    fn write_if_match_nonexistent_returns_412() {
        let cond = WriteCondition::IfMatch(SpecificEtag::new(test_etag()).unwrap());
        let err = check_write_conditions(&cond, None).unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    // ── Delete conditions ──────────────────────────────────────────────

    #[test]
    fn delete_if_match_passes() {
        let cond = DeleteCondition::IfMatch(test_etag().into());
        assert!(check_delete_conditions(&cond, &test_etag()).is_ok());
    }

    #[test]
    fn delete_if_match_fails() {
        let cond = DeleteCondition::IfMatch(other_etag().into());
        let err = check_delete_conditions(&cond, &test_etag()).unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    #[test]
    fn delete_if_match_wildcard_passes() {
        let cond = DeleteCondition::IfMatch("*".into());
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
        assert!(!WriteCondition::IfNoneMatchStar.is_empty());
    }

    #[test]
    fn delete_condition_is_empty() {
        assert!(DeleteCondition::default().is_empty());
        assert!(!DeleteCondition::IfMatch("x".into()).is_empty());
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
            if_match: Some("*".into()),
            ..Default::default()
        };
        assert!(check_read_conditions(&cond, &test_etag(), 1000).is_ok());
    }

    #[test]
    fn read_if_none_match_wildcard_returns_304() {
        let cond = ReadCondition {
            if_none_match: Some("*".into()),
            ..Default::default()
        };
        let err = check_read_conditions(&cond, &test_etag(), 1000).unwrap_err();
        assert!(matches!(err, ServerError::NotModified { .. }));
    }

    #[test]
    fn read_if_match_comma_list_one_matches() {
        let list = format!("\"1111111111111111\", {}", test_etag());
        let cond = ReadCondition {
            if_match: Some(list.into()),
            ..Default::default()
        };
        assert!(check_read_conditions(&cond, &test_etag(), 1000).is_ok());
    }

    #[test]
    fn read_if_match_comma_list_none_match() {
        let cond = ReadCondition {
            if_match: Some("\"1111111111111111\", \"2222222222222222\"".into()),
            ..Default::default()
        };
        let err = check_read_conditions(&cond, &test_etag(), 1000).unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    #[test]
    fn read_if_none_match_comma_list_one_matches() {
        let list = format!("\"1111111111111111\", {}", test_etag());
        let cond = ReadCondition {
            if_none_match: Some(list.into()),
            ..Default::default()
        };
        let err = check_read_conditions(&cond, &test_etag(), 1000).unwrap_err();
        assert!(matches!(err, ServerError::NotModified { .. }));
    }

    // ── Copy source conditions ──────────────────────────────────────

    #[test]
    fn copy_source_if_match_passes() {
        let cond = ReadCondition {
            if_match: Some(test_etag().into()),
            ..Default::default()
        };
        assert!(check_copy_source_conditions(&cond, &test_etag(), 1000).is_ok());
    }

    #[test]
    fn copy_source_if_match_fails() {
        let cond = ReadCondition {
            if_match: Some(other_etag().into()),
            ..Default::default()
        };
        let err = check_copy_source_conditions(&cond, &test_etag(), 1000).unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    #[test]
    fn copy_source_if_none_match_matching_returns_412() {
        // Key difference from read: returns 412, NOT 304
        let cond = ReadCondition {
            if_none_match: Some(test_etag().into()),
            ..Default::default()
        };
        let err = check_copy_source_conditions(&cond, &test_etag(), 1000).unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    #[test]
    fn copy_source_if_none_match_passes() {
        let cond = ReadCondition {
            if_none_match: Some(other_etag().into()),
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
