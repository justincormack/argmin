/// HTTP header parsing for conditional request types.
///
/// Extracts `ReadCondition`, `WriteCondition`, and `DeleteCondition` from
/// raw S3 request headers. The core condition types and evaluation logic
/// live in `crate::conditional`; this module handles only the HTTP boundary.
use crate::conditional::{
    DeleteCondition, EtagMatchList, ReadCondition, SpecificEtag, WriteCondition,
};
use crate::error::ServerError;
use crate::http::request::S3Request;
use crate::http::response::parse_http_date;

/// Extract read conditions from an S3 request's headers.
pub fn read_condition_from_headers(req: &S3Request) -> ReadCondition {
    ReadCondition {
        if_match: req.header("if-match").map(EtagMatchList::from_header_value),
        if_none_match: req
            .header("if-none-match")
            .map(EtagMatchList::from_header_value),
        if_modified_since: req.header("if-modified-since").and_then(parse_http_date),
        if_unmodified_since: req.header("if-unmodified-since").and_then(parse_http_date),
    }
}

/// Extract write conditions from an S3 request's headers.
///
/// AWS S3 only supports `If-Match: <etag>` and `If-None-Match: *` on writes.
/// `If-Match: *` and `If-None-Match: <etag>` return 501 `NotImplemented`.
pub fn write_condition_from_headers(req: &S3Request) -> Result<WriteCondition, ServerError> {
    let if_match = req.header("if-match");
    let if_none_match = req.header("if-none-match");

    // Reject unsupported forms first.
    if let Some(val) = if_match {
        if val.trim() == "*" {
            return Err(ServerError::NotImplemented {
                feature: "A header you provided implies functionality that is not implemented"
                    .to_string(),
            });
        }
    }
    if let Some(val) = if_none_match {
        if val.trim() != "*" {
            return Err(ServerError::NotImplemented {
                feature: "A header you provided implies functionality that is not implemented"
                    .to_string(),
            });
        }
    }

    // S3 does not support both If-Match and If-None-Match on the same write.
    if if_match.is_some() && if_none_match.is_some() {
        return Err(ServerError::NotImplemented {
            feature: "A header you provided implies functionality that is not implemented"
                .to_string(),
        });
    }

    if let Some(val) = if_match {
        // Wildcard already rejected above, so this cannot fail.
        let etag = SpecificEtag::new(val.to_string()).expect("wildcard already rejected");
        if etag.as_str() == "*" {
            return Err(ServerError::NotImplemented {
                feature: "A header you provided implies functionality that is not implemented"
                    .to_string(),
            });
        }
        return Ok(WriteCondition::IfMatch(etag));
    }
    if if_none_match.is_some() {
        return Ok(WriteCondition::IfNoneMatchStar);
    }
    Ok(WriteCondition::None)
}

/// Extract delete conditions from an S3 request's headers.
///
/// AWS S3 only supports `If-Match` on `DeleteObject` (general-purpose buckets).
/// `x-amz-if-match-last-modified-time` and `x-amz-if-match-size` are
/// directory-bucket-only features and return 501 `NotImplemented`.
pub fn delete_condition_from_headers(req: &S3Request) -> Result<DeleteCondition, ServerError> {
    if req.header("x-amz-if-match-last-modified-time").is_some() {
        return Err(ServerError::NotImplemented {
            feature: "A header you provided implies functionality that is not implemented"
                .to_string(),
        });
    }
    if req.header("x-amz-if-match-size").is_some() {
        return Err(ServerError::NotImplemented {
            feature: "A header you provided implies functionality that is not implemented"
                .to_string(),
        });
    }
    Ok(match req.header("if-match") {
        Some(val) => DeleteCondition::IfMatch(EtagMatchList::from_header_value(val)),
        None => DeleteCondition::None,
    })
}

/// Extract copy-source conditions from an S3 request's `x-amz-copy-source-if-*` headers.
pub fn copy_source_condition_from_headers(req: &S3Request) -> ReadCondition {
    ReadCondition {
        if_match: req
            .header("x-amz-copy-source-if-match")
            .map(EtagMatchList::from_header_value),
        if_none_match: req
            .header("x-amz-copy-source-if-none-match")
            .map(EtagMatchList::from_header_value),
        if_modified_since: req
            .header("x-amz-copy-source-if-modified-since")
            .and_then(parse_http_date),
        if_unmodified_since: req
            .header("x-amz-copy-source-if-unmodified-since")
            .and_then(parse_http_date),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::request::S3Request;

    fn make_req_with_headers(headers: Vec<(&str, &str)>) -> S3Request {
        S3Request::new_for_test(
            http::Method::GET,
            "/",
            "",
            crate::http::request::header_map_from_owned(
                headers
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            ),
            vec![],
        )
    }

    // ── Write condition validation (501 rejection) ────────────────────

    #[test]
    fn write_if_match_wildcard_returns_not_implemented() {
        let req = make_req_with_headers(vec![("if-match", "*")]);
        let err = write_condition_from_headers(&req).unwrap_err();
        assert!(matches!(err, ServerError::NotImplemented { .. }));
    }

    #[test]
    fn write_if_none_match_specific_etag_returns_not_implemented() {
        let req = make_req_with_headers(vec![("if-none-match", "\"abcdef1234567890\"")]);
        let err = write_condition_from_headers(&req).unwrap_err();
        assert!(matches!(err, ServerError::NotImplemented { .. }));
    }

    #[test]
    fn write_if_match_specific_etag_accepted() {
        let req = make_req_with_headers(vec![("if-match", "\"abcdef1234567890\"")]);
        assert!(write_condition_from_headers(&req).is_ok());
    }

    #[test]
    fn write_if_match_quoted_star_returns_not_implemented() {
        let req = make_req_with_headers(vec![("if-match", "\"*\"")]);
        let err = write_condition_from_headers(&req).unwrap_err();
        assert!(matches!(err, ServerError::NotImplemented { .. }));
    }

    #[test]
    fn write_if_none_match_star_accepted() {
        let req = make_req_with_headers(vec![("if-none-match", "*")]);
        assert!(write_condition_from_headers(&req).is_ok());
    }

    #[test]
    fn write_both_if_match_and_if_none_match_rejected() {
        let req = make_req_with_headers(vec![
            ("if-match", "\"abcdef1234567890\""),
            ("if-none-match", "*"),
        ]);
        let err = write_condition_from_headers(&req).unwrap_err();
        assert!(matches!(err, ServerError::NotImplemented { .. }));
    }

    #[test]
    fn write_if_match_masks_unsupported_if_none_match_rejected() {
        // If-Match present alongside If-None-Match: <specific-etag> should still
        // reject the unsupported If-None-Match form, not silently drop it.
        let req = make_req_with_headers(vec![
            ("if-match", "\"abcdef1234567890\""),
            ("if-none-match", "\"1111111111111111\""),
        ]);
        let err = write_condition_from_headers(&req).unwrap_err();
        // Should be NotImplemented (unsupported If-None-Match form), not InvalidRequest
        assert!(matches!(err, ServerError::NotImplemented { .. }));
    }

    // ── Delete condition validation (501 rejection) ────────────────────

    #[test]
    fn delete_if_match_last_modified_time_returns_not_implemented() {
        let req = make_req_with_headers(vec![(
            "x-amz-if-match-last-modified-time",
            "Thu, 01 Jan 2026 00:00:00 GMT",
        )]);
        let err = delete_condition_from_headers(&req).unwrap_err();
        assert!(matches!(err, ServerError::NotImplemented { .. }));
    }

    #[test]
    fn delete_if_match_size_returns_not_implemented() {
        let req = make_req_with_headers(vec![("x-amz-if-match-size", "100")]);
        let err = delete_condition_from_headers(&req).unwrap_err();
        assert!(matches!(err, ServerError::NotImplemented { .. }));
    }

    #[test]
    fn delete_if_match_accepted() {
        let req = make_req_with_headers(vec![("if-match", "\"abcdef1234567890\"")]);
        assert!(delete_condition_from_headers(&req).is_ok());
    }
}
