// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! CORS configuration types, matching logic, and response header builders.

/// A single CORS rule within a bucket's CORS configuration.
#[derive(Debug, Clone)]
pub struct CorsRule {
    /// One or more allowed origin patterns (each may contain one `*` wildcard).
    pub allowed_origins: Vec<String>,
    /// One or more allowed HTTP methods (GET, PUT, POST, DELETE, HEAD).
    pub allowed_methods: Vec<String>,
    /// Allowed request header patterns (each may contain one `*` wildcard).
    pub allowed_headers: Vec<String>,
    /// Headers the browser is allowed to access from the response.
    pub expose_headers: Vec<String>,
    /// How long the browser may cache preflight results, in seconds.
    pub max_age_seconds: Option<u32>,
}

/// A bucket's full CORS configuration (list of rules).
#[derive(Debug, Clone)]
pub struct CorsConfiguration {
    pub rules: Vec<CorsRule>,
}

/// Match a value against a pattern that may contain at most one `*` wildcard.
///
/// The wildcard matches zero or more characters. Matching is case-sensitive.
#[must_use]
pub fn wildcard_match(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    match pattern.find('*') {
        None => pattern == value,
        Some(pos) => {
            let prefix = &pattern[..pos];
            let suffix = &pattern[pos + 1..];
            value.starts_with(prefix)
                && value.ends_with(suffix)
                && value.len() >= prefix.len() + suffix.len()
        }
    }
}

/// Check whether a CORS rule matches the given origin, method, and request headers.
///
/// For preflight requests, `request_headers` are from `Access-Control-Request-Headers`.
/// For actual requests, pass an empty slice.
///
/// Returns the matching origin pattern if matched, or `None`.
#[must_use]
pub fn match_rule<'a>(
    rule: &'a CorsRule,
    origin: &str,
    method: &str,
    request_headers: &[&str],
) -> Option<&'a str> {
    // Origin must match at least one allowed origin
    let matched_origin = rule
        .allowed_origins
        .iter()
        .find(|o| wildcard_match(o, origin))?;

    // Method must be in allowed methods
    if !rule
        .allowed_methods
        .iter()
        .any(|m| m.eq_ignore_ascii_case(method))
    {
        return None;
    }

    // Every requested header must match at least one allowed header pattern
    for rh in request_headers {
        let rh_lower = rh.trim().to_ascii_lowercase();
        if rh_lower.is_empty() {
            continue;
        }
        let matched = rule
            .allowed_headers
            .iter()
            .any(|ah| wildcard_match(&ah.to_ascii_lowercase(), &rh_lower));
        if !matched {
            return None;
        }
    }

    Some(matched_origin)
}

/// A matched CORS rule together with the specific origin pattern that matched.
pub struct CorsMatch<'a> {
    pub rule: &'a CorsRule,
    pub matched_origin: &'a str,
}

/// Find the first matching CORS rule for the given request parameters.
///
/// Returns `None` if no rule matches (no CORS headers should be added).
#[must_use]
pub fn find_matching_rule<'a>(
    config: &'a CorsConfiguration,
    origin: &str,
    method: &str,
    request_headers: &[&str],
) -> Option<CorsMatch<'a>> {
    for rule in &config.rules {
        if let Some(matched_origin) = match_rule(rule, origin, method, request_headers) {
            return Some(CorsMatch {
                rule,
                matched_origin,
            });
        }
    }
    None
}

/// Build CORS response headers for a preflight (OPTIONS) request.
///
/// `matched_origin` is the origin pattern from the rule that matched the request.
#[must_use]
pub fn preflight_response_headers(
    rule: &CorsRule,
    origin: &str,
    matched_origin: &str,
    request_headers: Option<&str>,
) -> Vec<(String, String)> {
    let mut headers = Vec::new();

    // Access-Control-Allow-Origin
    if matched_origin == "*" {
        headers.push(("Access-Control-Allow-Origin".into(), "*".into()));
    } else {
        headers.push(("Access-Control-Allow-Origin".into(), origin.to_string()));
    }

    // Access-Control-Allow-Methods
    headers.push((
        "Access-Control-Allow-Methods".into(),
        rule.allowed_methods.join(", "),
    ));

    // Access-Control-Allow-Headers (echo back the requested headers if they all matched)
    if let Some(rh) = request_headers {
        if !rh.is_empty() {
            headers.push(("Access-Control-Allow-Headers".into(), rh.to_string()));
        }
    }

    // Access-Control-Max-Age
    if let Some(max_age) = rule.max_age_seconds {
        headers.push(("Access-Control-Max-Age".into(), max_age.to_string()));
    }

    // Access-Control-Allow-Credentials (unless origin is *)
    if matched_origin != "*" {
        headers.push(("Access-Control-Allow-Credentials".into(), "true".into()));
    }

    // Vary
    headers.push((
        "Vary".into(),
        "Origin, Access-Control-Request-Headers, Access-Control-Request-Method".into(),
    ));

    headers
}

/// Build CORS response headers for an actual (non-preflight) request.
///
/// `matched_origin` is the origin pattern from the rule that matched the request.
#[must_use]
pub fn actual_response_headers(
    rule: &CorsRule,
    origin: &str,
    matched_origin: &str,
) -> Vec<(String, String)> {
    let mut headers = Vec::new();

    // Access-Control-Allow-Origin
    if matched_origin == "*" {
        headers.push(("Access-Control-Allow-Origin".into(), "*".into()));
    } else {
        headers.push(("Access-Control-Allow-Origin".into(), origin.to_string()));
    }

    // Access-Control-Allow-Methods
    headers.push((
        "Access-Control-Allow-Methods".into(),
        rule.allowed_methods.join(", "),
    ));

    // Access-Control-Expose-Headers
    if !rule.expose_headers.is_empty() {
        headers.push((
            "Access-Control-Expose-Headers".into(),
            rule.expose_headers.join(", "),
        ));
    }

    // Access-Control-Allow-Credentials (unless origin is *)
    if matched_origin != "*" {
        headers.push(("Access-Control-Allow-Credentials".into(), "true".into()));
    }

    // Vary
    headers.push((
        "Vary".into(),
        "Origin, Access-Control-Request-Headers, Access-Control-Request-Method".into(),
    ));

    headers
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── wildcard_match ──────────────────────────────────────────────

    #[test]
    fn wildcard_match_exact() {
        assert!(wildcard_match("http://example.com", "http://example.com"));
        assert!(!wildcard_match("http://example.com", "http://other.com"));
    }

    #[test]
    fn wildcard_match_star_only() {
        assert!(wildcard_match("*", "http://anything.com"));
        assert!(wildcard_match("*", ""));
    }

    #[test]
    fn wildcard_match_prefix() {
        assert!(wildcard_match(
            "http://*.example.com",
            "http://sub.example.com"
        ));
        assert!(wildcard_match(
            "http://*.example.com",
            "http://deep.sub.example.com"
        ));
        assert!(!wildcard_match(
            "http://*.example.com",
            "http://example.com"
        ));
    }

    #[test]
    fn wildcard_match_suffix() {
        assert!(wildcard_match("http://example.*", "http://example.com"));
        assert!(wildcard_match("http://example.*", "http://example.org"));
    }

    #[test]
    fn wildcard_match_middle() {
        assert!(wildcard_match("http://a*z.com", "http://abcz.com"));
        assert!(wildcard_match("http://a*z.com", "http://az.com"));
        assert!(!wildcard_match("http://a*z.com", "http://az.org"));
    }

    #[test]
    fn wildcard_match_no_overlap() {
        // Pattern "ab*cd" should not match "abcd" when prefix+suffix > value length
        // Actually "ab*cd" with value "abcd": prefix "ab", suffix "cd", len(4) >= 2+2
        // starts_with("ab") && ends_with("cd") => true
        assert!(wildcard_match("ab*cd", "abcd"));
        // But "ab*cd" with value "abc": len(3) < 2+2 => false
        assert!(!wildcard_match("ab*cd", "abc"));
    }

    // ── match_rule ──────────────────────────────────────────────────

    fn test_rule() -> CorsRule {
        CorsRule {
            allowed_origins: vec!["http://example.com".into()],
            allowed_methods: vec!["GET".into(), "PUT".into()],
            allowed_headers: vec!["x-custom-header".into(), "content-type".into()],
            expose_headers: vec!["x-amz-request-id".into()],
            max_age_seconds: Some(3600),
        }
    }

    #[test]
    fn match_rule_basic() {
        let rule = test_rule();
        let m = match_rule(&rule, "http://example.com", "GET", &[]);
        assert_eq!(m, Some("http://example.com"));
    }

    #[test]
    fn match_rule_wrong_origin() {
        let rule = test_rule();
        assert!(match_rule(&rule, "http://other.com", "GET", &[]).is_none());
    }

    #[test]
    fn match_rule_wrong_method() {
        let rule = test_rule();
        assert!(match_rule(&rule, "http://example.com", "DELETE", &[]).is_none());
    }

    #[test]
    fn match_rule_with_headers() {
        let rule = test_rule();
        assert!(match_rule(&rule, "http://example.com", "GET", &["x-custom-header"]).is_some());
        assert!(match_rule(&rule, "http://example.com", "GET", &["Content-Type"]).is_some());
    }

    #[test]
    fn match_rule_unmatched_header() {
        let rule = test_rule();
        assert!(match_rule(&rule, "http://example.com", "GET", &["x-unknown"]).is_none());
    }

    #[test]
    fn match_rule_wildcard_headers() {
        let rule = CorsRule {
            allowed_origins: vec!["*".into()],
            allowed_methods: vec!["GET".into()],
            allowed_headers: vec!["*".into()],
            expose_headers: vec![],
            max_age_seconds: None,
        };
        assert!(match_rule(&rule, "http://any.com", "GET", &["anything"]).is_some());
    }

    #[test]
    fn match_rule_method_case_insensitive() {
        let rule = test_rule();
        assert!(match_rule(&rule, "http://example.com", "get", &[]).is_some());
    }

    #[test]
    fn match_rule_multiple_origins() {
        let rule = CorsRule {
            allowed_origins: vec!["http://first.com".into(), "http://second.com".into()],
            allowed_methods: vec!["GET".into()],
            allowed_headers: vec![],
            expose_headers: vec![],
            max_age_seconds: None,
        };
        assert_eq!(
            match_rule(&rule, "http://first.com", "GET", &[]),
            Some("http://first.com")
        );
        assert_eq!(
            match_rule(&rule, "http://second.com", "GET", &[]),
            Some("http://second.com")
        );
        assert!(match_rule(&rule, "http://third.com", "GET", &[]).is_none());
    }

    // ── find_matching_rule ──────────────────────────────────────────

    #[test]
    fn find_matching_rule_first_wins() {
        let config = CorsConfiguration {
            rules: vec![
                CorsRule {
                    allowed_origins: vec!["http://first.com".into()],
                    allowed_methods: vec!["GET".into()],
                    allowed_headers: vec![],
                    expose_headers: vec![],
                    max_age_seconds: None,
                },
                CorsRule {
                    allowed_origins: vec!["*".into()],
                    allowed_methods: vec!["GET".into()],
                    allowed_headers: vec![],
                    expose_headers: vec![],
                    max_age_seconds: Some(100),
                },
            ],
        };
        let m = find_matching_rule(&config, "http://first.com", "GET", &[]).unwrap();
        assert_eq!(m.matched_origin, "http://first.com");
    }

    #[test]
    fn find_matching_rule_none() {
        let config = CorsConfiguration {
            rules: vec![CorsRule {
                allowed_origins: vec!["http://specific.com".into()],
                allowed_methods: vec!["GET".into()],
                allowed_headers: vec![],
                expose_headers: vec![],
                max_age_seconds: None,
            }],
        };
        assert!(find_matching_rule(&config, "http://other.com", "GET", &[]).is_none());
    }

    // ── preflight_response_headers ──────────────────────────────────

    #[test]
    fn preflight_headers_basic() {
        let rule = test_rule();
        let headers =
            preflight_response_headers(&rule, "http://example.com", "http://example.com", None);
        let h: std::collections::HashMap<&str, &str> = headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        assert_eq!(
            h.get("Access-Control-Allow-Origin"),
            Some(&"http://example.com")
        );
        assert_eq!(h.get("Access-Control-Allow-Methods"), Some(&"GET, PUT"));
        assert_eq!(h.get("Access-Control-Max-Age"), Some(&"3600"));
        assert_eq!(h.get("Access-Control-Allow-Credentials"), Some(&"true"));
        assert!(h.contains_key("Vary"));
    }

    #[test]
    fn preflight_headers_wildcard_origin_no_credentials() {
        let rule = CorsRule {
            allowed_origins: vec!["*".into()],
            allowed_methods: vec!["GET".into()],
            allowed_headers: vec![],
            expose_headers: vec![],
            max_age_seconds: None,
        };
        let headers = preflight_response_headers(&rule, "http://any.com", "*", None);
        let h: std::collections::HashMap<&str, &str> = headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        assert_eq!(h.get("Access-Control-Allow-Origin"), Some(&"*"));
        assert!(!h.contains_key("Access-Control-Allow-Credentials"));
    }

    #[test]
    fn preflight_headers_with_request_headers() {
        let rule = test_rule();
        let headers = preflight_response_headers(
            &rule,
            "http://example.com",
            "http://example.com",
            Some("x-custom-header"),
        );
        let h: std::collections::HashMap<&str, &str> = headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        assert_eq!(
            h.get("Access-Control-Allow-Headers"),
            Some(&"x-custom-header")
        );
    }

    // ── actual_response_headers ─────────────────────────────────────

    #[test]
    fn actual_headers_basic() {
        let rule = test_rule();
        let headers = actual_response_headers(&rule, "http://example.com", "http://example.com");
        let h: std::collections::HashMap<&str, &str> = headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        assert_eq!(
            h.get("Access-Control-Allow-Origin"),
            Some(&"http://example.com")
        );
        assert_eq!(
            h.get("Access-Control-Expose-Headers"),
            Some(&"x-amz-request-id")
        );
        assert_eq!(h.get("Access-Control-Allow-Credentials"), Some(&"true"));
        assert_eq!(h.get("Access-Control-Allow-Methods"), Some(&"GET, PUT"));
        assert!(!h.contains_key("Access-Control-Max-Age"));
    }

    #[test]
    fn actual_headers_no_expose() {
        let rule = CorsRule {
            allowed_origins: vec!["http://example.com".into()],
            allowed_methods: vec!["GET".into()],
            allowed_headers: vec![],
            expose_headers: vec![],
            max_age_seconds: None,
        };
        let headers = actual_response_headers(&rule, "http://example.com", "http://example.com");
        let h: std::collections::HashMap<&str, &str> = headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        assert!(!h.contains_key("Access-Control-Expose-Headers"));
    }
}
