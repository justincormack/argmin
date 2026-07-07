//! Golden response-shape assertions.
//!
//! These helpers pin the full shape of a raw response — status, the complete
//! normalized header set, and the full body — against expectations written in
//! the test. The expectations must hold against AWS and the local server
//! alike: tests never branch on the endpoint they are running against. For
//! documented divergences (see `guides/aws-compatibility.md`) a test accepts
//! either shape with [`assert_shape_one_of`].
//!
//! Body and header-value expectations are templates. A `{name}` placeholder
//! either matches a value supplied with [`ShapeSpec::sub`] exactly, or is one
//! of the built-in validated placeholders:
//!
//! - `{request_id}` — AWS request ID shape; repeated uses must capture the
//!   same value, and the value must equal the `x-amz-request-id` header
//! - `{host_id}` — AWS host ID shape; consistency as for `{request_id}`
//! - `{etag}` — quoted opaque ETag (`"…"` or `&quot;…&quot;`); consistent
//!   across repeated uses. ETag values are opaque integrity tokens, not AWS
//!   MD5s, so they are never asserted literally
//! - `{version_id}`, `{upload_id}`, `{owner_id}` — opaque non-empty tokens,
//!   consistent across repeated uses
//! - `{http_date}` — RFC 1123 date shape, each use independent
//! - `{iso8601}` — ISO 8601 timestamp shape, each use independent
//! - `{any}` — any non-empty text, each use independent
//! - `{ws}` — possibly-empty whitespace (AWS keep-alive padding), each use
//!   independent
//!
//! Error-response bodies are usually not written out literally: the
//! [`expected_error`] module builds the expected template by calling the
//! production error formatters with placeholder tokens. Against the local
//! server that comparison is tautological for the body, so each production
//! error formatter must also keep one literal-template test anchoring its
//! exact output, and the templates are validated against AWS by the external
//! `s3-tests` runs.

use std::collections::{BTreeMap, BTreeSet};

use crate::helpers::RawResponse;

pub const REQUEST_ID_HEADER: &str = "x-amz-request-id";
pub const HOST_ID_HEADER: &str = "x-amz-id-2";

/// Headers excluded from full-set header comparisons by default because they
/// vary by transport or connection rather than by S3 behavior.
pub const DEFAULT_IGNORED_HEADERS: &[&str] = &["connection", "date", "server"];

/// Whether a value has the AWS `x-amz-request-id` shape.
pub fn is_aws_request_id_shape(value: &str) -> bool {
    value.len() == 16
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || b.is_ascii_uppercase())
}

/// Whether a value has the AWS `x-amz-id-2` shape.
pub fn is_aws_host_id_shape(value: &str) -> bool {
    value.len() >= 40
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'/' | b'='))
}

fn is_etag_shape(value: &str) -> bool {
    let quoted = value.len() > 2 && value.starts_with('"') && value.ends_with('"');
    let entity_quoted =
        value.len() > 12 && value.starts_with("&quot;") && value.ends_with("&quot;");
    quoted || entity_quoted
}

fn is_opaque_token_shape(value: &str) -> bool {
    !value.is_empty() && !value.contains('<') && !value.contains('>')
}

fn is_http_date_shape(value: &str) -> bool {
    // e.g. "Mon, 06 Jul 2026 12:34:56 GMT"
    let bytes = value.as_bytes();
    value.len() == 29
        && value.ends_with(" GMT")
        && bytes[3] == b','
        && bytes[4] == b' '
        && bytes[5].is_ascii_digit()
        && bytes[6].is_ascii_digit()
        && value[12..16].chars().all(|c| c.is_ascii_digit())
        && bytes[19] == b':'
        && bytes[22] == b':'
}

fn is_iso8601_shape(value: &str) -> bool {
    // e.g. "2026-07-06T12:34:56.000Z", with or without fractional seconds
    let bytes = value.as_bytes();
    value.len() >= 20
        && value.ends_with('Z')
        && value[..4].chars().all(|c| c.is_ascii_digit())
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes[10] == b'T'
        && bytes[13] == b':'
        && bytes[16] == b':'
}

fn is_nonempty(value: &str) -> bool {
    !value.is_empty()
}

fn is_whitespace_only(value: &str) -> bool {
    value.chars().all(char::is_whitespace)
}

struct Placeholder {
    name: &'static str,
    validate: fn(&str) -> bool,
    /// Captured placeholders must match the same value at every use within
    /// one assertion; non-captured placeholders are shape-checked per use.
    capture: bool,
}

const PLACEHOLDERS: &[Placeholder] = &[
    Placeholder {
        name: "request_id",
        validate: is_aws_request_id_shape,
        capture: true,
    },
    Placeholder {
        name: "host_id",
        validate: is_aws_host_id_shape,
        capture: true,
    },
    Placeholder {
        name: "etag",
        validate: is_etag_shape,
        capture: true,
    },
    Placeholder {
        name: "version_id",
        validate: is_opaque_token_shape,
        capture: true,
    },
    Placeholder {
        name: "upload_id",
        validate: is_opaque_token_shape,
        capture: true,
    },
    Placeholder {
        name: "owner_id",
        validate: is_opaque_token_shape,
        capture: true,
    },
    Placeholder {
        name: "http_date",
        validate: is_http_date_shape,
        capture: false,
    },
    Placeholder {
        name: "iso8601",
        validate: is_iso8601_shape,
        capture: false,
    },
    Placeholder {
        name: "any",
        validate: is_nonempty,
        capture: false,
    },
    // Possibly-empty whitespace, e.g. the keep-alive padding AWS inserts
    // after the XML declaration on slow CompleteMultipartUpload responses.
    Placeholder {
        name: "ws",
        validate: is_whitespace_only,
        capture: false,
    },
];

fn placeholder_def(name: &str) -> Option<&'static Placeholder> {
    PLACEHOLDERS.iter().find(|p| p.name == name)
}

enum Segment<'t> {
    Literal(&'t str),
    Placeholder(&'t str),
}

/// Parse a template into literal and placeholder segments.
///
/// Panics on malformed templates (unbalanced braces, adjacent placeholders,
/// bad placeholder names): a malformed template is a bug in the test itself,
/// not a shape mismatch.
fn parse_template(template: &str) -> Vec<Segment<'_>> {
    let mut segments = Vec::new();
    let mut rest = template;
    while !rest.is_empty() {
        match rest.find(['{', '}']) {
            None => {
                segments.push(Segment::Literal(rest));
                rest = "";
            }
            Some(brace) => {
                assert!(
                    rest.as_bytes()[brace] != b'}',
                    "template has '}}' without matching '{{': {template:?}"
                );
                if brace > 0 {
                    segments.push(Segment::Literal(&rest[..brace]));
                }
                let after = &rest[brace + 1..];
                let end = after.find(['{', '}']).unwrap_or_else(|| {
                    panic!("template has unterminated placeholder: {template:?}")
                });
                assert!(
                    after.as_bytes()[end] == b'}',
                    "template has nested '{{' inside a placeholder: {template:?}"
                );
                let name = &after[..end];
                assert!(
                    !name.is_empty()
                        && name
                            .bytes()
                            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'),
                    "template has invalid placeholder name {name:?}: {template:?}"
                );
                if let Some(Segment::Placeholder(previous)) = segments.last() {
                    panic!(
                        "template has adjacent placeholders {{{previous}}}{{{name}}} with no \
                         literal text between them: {template:?}"
                    );
                }
                segments.push(Segment::Placeholder(name));
                rest = &after[end + 1..];
            }
        }
    }
    segments
}

fn context_window(text: &str, at: usize) -> &str {
    let start = at.saturating_sub(40);
    let end = (at + 80).min(text.len());
    let start = (start..=at)
        .find(|i| text.is_char_boundary(*i))
        .unwrap_or(at);
    let end = (end..text.len())
        .find(|i| text.is_char_boundary(*i))
        .unwrap_or(text.len());
    &text[start..end]
}

/// Match `actual` against `template`, resolving `{name}` placeholders from
/// `subs` (exact expected values) or the built-in validated placeholders.
/// Captured values accumulate in `captures` and must stay consistent.
fn match_template(
    context: &str,
    template: &str,
    actual: &str,
    subs: &BTreeMap<String, String>,
    captures: &mut BTreeMap<String, String>,
) -> Result<(), String> {
    let segments = parse_template(template);
    let mut pos = 0usize;
    for (index, segment) in segments.iter().enumerate() {
        match segment {
            Segment::Literal(literal) => {
                if !actual[pos..].starts_with(literal) {
                    return Err(format!(
                        "{context}: mismatch at byte {pos}\nexpected literal: {literal:?}\nactual from there: {:?}\ntemplate: {template:?}",
                        context_window(actual, pos),
                    ));
                }
                pos += literal.len();
            }
            Segment::Placeholder(name) => {
                let expected_exact = subs
                    .get(*name)
                    .or_else(|| match placeholder_def(name) {
                        Some(def) if def.capture => captures.get(*name),
                        _ => None,
                    })
                    .cloned();
                if let Some(exact) = expected_exact {
                    if !actual[pos..].starts_with(&exact) {
                        return Err(format!(
                            "{context}: mismatch at byte {pos}\nexpected {{{name}}} = {exact:?}\nactual from there: {:?}\ntemplate: {template:?}",
                            context_window(actual, pos),
                        ));
                    }
                    pos += exact.len();
                    continue;
                }
                let Some(def) = placeholder_def(name) else {
                    panic!(
                        "{context}: unknown placeholder {{{name}}} and no sub provided: {template:?}"
                    );
                };
                let value_end = match segments.get(index + 1) {
                    Some(Segment::Literal(next_literal)) => {
                        match actual[pos..].find(next_literal) {
                            Some(offset) => pos + offset,
                            None => {
                                return Err(format!(
                                    "{context}: mismatch at byte {pos}\nexpected {{{name}}} followed by {next_literal:?}, which never occurs\nactual from there: {:?}\ntemplate: {template:?}",
                                    context_window(actual, pos),
                                ));
                            }
                        }
                    }
                    Some(Segment::Placeholder(_)) => unreachable!("rejected at parse"),
                    None => actual.len(),
                };
                let value = &actual[pos..value_end];
                if !(def.validate)(value) {
                    return Err(format!(
                        "{context}: value {value:?} at byte {pos} does not have the {{{name}}} shape\ntemplate: {template:?}"
                    ));
                }
                if def.capture {
                    captures.insert((*name).to_string(), value.to_string());
                }
                pos = value_end;
            }
        }
    }
    if pos != actual.len() {
        return Err(format!(
            "{context}: unexpected trailing content at byte {pos}: {:?}\ntemplate: {template:?}",
            context_window(actual, pos),
        ));
    }
    Ok(())
}

/// The header value for `name`, case-insensitively, if present exactly once.
pub fn response_header_value<'a>(response: &'a RawResponse, name: &str) -> Option<&'a str> {
    response
        .headers
        .iter()
        .find(|(header_name, _)| header_name.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

/// The text inside the first `<tag>…</tag>` element, if any.
pub fn xml_tag_text<'a>(body: &'a str, tag: &str) -> Option<&'a str> {
    let start_tag = format!("<{tag}>");
    let end_tag = format!("</{tag}>");
    let start = body.find(&start_tag)? + start_tag.len();
    let end = body[start..].find(&end_tag)? + start;
    Some(&body[start..end])
}

/// Every complete `<tag>…</tag>` block, including the surrounding tags.
pub fn extract_xml_blocks<'a>(body: &'a str, tag: &str) -> Vec<&'a str> {
    let start_tag = format!("<{tag}>");
    let end_tag = format!("</{tag}>");
    let mut blocks = Vec::new();
    let mut search_from = 0;
    while let Some(relative_start) = body[search_from..].find(&start_tag) {
        let start = search_from + relative_start;
        let content_start = start + start_tag.len();
        let Some(relative_end) = body[content_start..].find(&end_tag) else {
            break;
        };
        let end = content_start + relative_end + end_tag.len();
        blocks.push(&body[start..end]);
        search_from = end;
    }
    blocks
}

/// Assert that the response's wire IDs have the AWS shape when present.
pub fn assert_response_id_shapes(operation: &str, response: &RawResponse) {
    if let Some(request_id) = response_header_value(response, REQUEST_ID_HEADER) {
        assert!(
            is_aws_request_id_shape(request_id),
            "{operation}: invalid {REQUEST_ID_HEADER} shape: {request_id}"
        );
    }
    if let Some(host_id) = response_header_value(response, HOST_ID_HEADER) {
        assert!(
            is_aws_host_id_shape(host_id),
            "{operation}: invalid {HOST_ID_HEADER} shape: {host_id}"
        );
    }
}

/// Assert that `RequestId`/`HostId` in an error body match the wire headers.
pub fn assert_error_ids_match_headers(operation: &str, response: &RawResponse) {
    if let (Some(header_value), Some(xml_value)) = (
        response_header_value(response, REQUEST_ID_HEADER),
        xml_tag_text(&response.body, "RequestId"),
    ) {
        assert_eq!(
            xml_value, header_value,
            "{operation}: RequestId XML/header mismatch\nresponse: {response:?}"
        );
    }
    if let (Some(header_value), Some(xml_value)) = (
        response_header_value(response, HOST_ID_HEADER),
        xml_tag_text(&response.body, "HostId"),
    ) {
        assert_eq!(
            xml_value, header_value,
            "{operation}: HostId XML/header mismatch\nresponse: {response:?}"
        );
    }
}

#[derive(Clone, Debug)]
enum BodySpec {
    Template(String),
    Empty,
}

/// A golden expectation for one response: status, complete normalized header
/// set, and full body template. Build with [`shape`], check with
/// [`assert_shape`] or [`assert_shape_one_of`].
#[derive(Clone, Debug, Default)]
pub struct ShapeSpec {
    status: Option<u16>,
    headers: Option<Vec<(String, String)>>,
    extra_ignored_headers: Vec<String>,
    body: Option<BodySpec>,
    subs: BTreeMap<String, String>,
}

/// Start building a [`ShapeSpec`].
pub fn shape() -> ShapeSpec {
    ShapeSpec::default()
}

impl ShapeSpec {
    pub fn status(mut self, status: u16) -> Self {
        self.status = Some(status);
        self
    }

    /// Add one expected header. Once any expected header is given, the
    /// header check is complete-set equality: every non-ignored actual
    /// header must be expected and vice versa.
    pub fn header(mut self, name: &str, value_pattern: &str) -> Self {
        self.headers
            .get_or_insert_with(Vec::new)
            .push((name.to_ascii_lowercase(), value_pattern.to_string()));
        self
    }

    pub fn headers<'a, I: IntoIterator<Item = (&'a str, &'a str)>>(mut self, headers: I) -> Self {
        for (name, value_pattern) in headers {
            self = self.header(name, value_pattern);
        }
        self
    }

    /// Ignore a header in the complete-set comparison, in addition to
    /// [`DEFAULT_IGNORED_HEADERS`].
    pub fn ignore_header(mut self, name: &str) -> Self {
        self.extra_ignored_headers.push(name.to_ascii_lowercase());
        self
    }

    /// Expect the full body to match this template.
    pub fn body(mut self, template: impl Into<String>) -> Self {
        self.body = Some(BodySpec::Template(template.into()));
        self
    }

    /// Expect an empty body.
    pub fn body_empty(mut self) -> Self {
        self.body = Some(BodySpec::Empty);
        self
    }

    /// Provide the exact expected value for a `{name}` placeholder, e.g.
    /// the bucket name, key, account ID, or region for this test.
    pub fn sub(mut self, name: &str, value: impl Into<String>) -> Self {
        self.subs.insert(name.to_string(), value.into());
        self
    }

    fn ignored(&self) -> BTreeSet<&str> {
        DEFAULT_IGNORED_HEADERS
            .iter()
            .copied()
            .chain(self.extra_ignored_headers.iter().map(String::as_str))
            .collect()
    }

    fn check(
        &self,
        operation: &str,
        response: &RawResponse,
    ) -> Result<BTreeMap<String, String>, String> {
        if let Some(read_error) = &response.body_read_error {
            return Err(format!(
                "{operation}: response body read error: {read_error}"
            ));
        }
        if let Some(expected_status) = self.status {
            if response.status != expected_status {
                return Err(format!(
                    "{operation}: status mismatch: expected {expected_status}, got {}\nresponse: {response:?}",
                    response.status,
                ));
            }
        }
        let mut captures = BTreeMap::new();
        if let Some(expected_headers) = &self.headers {
            self.check_headers(operation, response, expected_headers, &mut captures)?;
        }
        match &self.body {
            Some(BodySpec::Template(template)) => {
                match_template(
                    &format!("{operation}: body"),
                    template,
                    &response.body,
                    &self.subs,
                    &mut captures,
                )?;
            }
            Some(BodySpec::Empty) if !response.body.is_empty() => {
                return Err(format!(
                    "{operation}: expected empty body, got: {:?}",
                    context_window(&response.body, 0),
                ));
            }
            Some(BodySpec::Empty) | None => {}
        }
        for (name, header) in [
            ("request_id", REQUEST_ID_HEADER),
            ("host_id", HOST_ID_HEADER),
        ] {
            if let (Some(captured), Some(header_value)) =
                (captures.get(name), response_header_value(response, header))
            {
                if captured != header_value {
                    return Err(format!(
                        "{operation}: captured {{{name}}} {captured:?} does not match {header} header {header_value:?}"
                    ));
                }
            }
        }
        Ok(captures)
    }

    fn check_headers(
        &self,
        operation: &str,
        response: &RawResponse,
        expected_headers: &[(String, String)],
        captures: &mut BTreeMap<String, String>,
    ) -> Result<(), String> {
        let ignored = self.ignored();
        let mut actual: BTreeMap<String, Vec<&str>> = BTreeMap::new();
        for (name, value) in &response.headers {
            let name = name.to_ascii_lowercase();
            if ignored.contains(name.as_str()) {
                continue;
            }
            actual.entry(name).or_default().push(value.as_str());
        }
        let mut expected: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        for (name, pattern) in expected_headers {
            expected
                .entry(name.as_str())
                .or_default()
                .push(pattern.as_str());
        }
        let actual_names: BTreeSet<&str> = actual.keys().map(String::as_str).collect();
        let expected_names: BTreeSet<&str> = expected.keys().copied().collect();
        if actual_names != expected_names {
            let missing: Vec<&&str> = expected_names.difference(&actual_names).collect();
            let unexpected: Vec<&&str> = actual_names.difference(&expected_names).collect();
            return Err(format!(
                "{operation}: header set mismatch\nmissing: {missing:?}\nunexpected: {unexpected:?}\nactual headers: {:?}",
                response.headers,
            ));
        }
        for (name, patterns) in &expected {
            let values = &actual[*name];
            if values.len() != patterns.len() {
                return Err(format!(
                    "{operation}: header {name}: expected {} value(s), got {values:?}",
                    patterns.len(),
                ));
            }
            let mut used = vec![false; values.len()];
            for pattern in patterns {
                let mut matched = false;
                for (i, value) in values.iter().enumerate() {
                    if used[i] {
                        continue;
                    }
                    let mut trial = captures.clone();
                    if match_template(
                        &format!("{operation}: header {name}"),
                        pattern,
                        value,
                        &self.subs,
                        &mut trial,
                    )
                    .is_ok()
                    {
                        *captures = trial;
                        used[i] = true;
                        matched = true;
                        break;
                    }
                }
                if !matched {
                    return Err(format!(
                        "{operation}: header {name}: no value matches pattern {pattern:?}\nvalues: {values:?}"
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Assert a response matches the expectation, with the standard wire-ID
/// invariants (ID shapes, XML/header ID equality) checked as well.
///
/// Returns the placeholder captures so tests can pin consistency across
/// responses, e.g. the same `{etag}` from PUT, GET, and HEAD of one object.
pub fn assert_shape(
    operation: &str,
    response: &RawResponse,
    spec: &ShapeSpec,
) -> BTreeMap<String, String> {
    match spec.check(operation, response) {
        Ok(captures) => {
            assert_response_id_shapes(operation, response);
            assert_error_ids_match_headers(operation, response);
            captures
        }
        Err(message) => panic!("{message}"),
    }
}

/// Assert a response matches one of several expectations. Only for behavior
/// where a divergence is documented in `guides/aws-compatibility.md`: the
/// same alternatives are accepted against every endpoint, never selected by
/// endpoint.
///
/// Returns the index of the matching alternative and its captures.
pub fn assert_shape_one_of(
    operation: &str,
    response: &RawResponse,
    specs: &[ShapeSpec],
) -> (usize, BTreeMap<String, String>) {
    assert!(
        !specs.is_empty(),
        "{operation}: no shape alternatives given"
    );
    let mut failures = Vec::new();
    for (index, spec) in specs.iter().enumerate() {
        match spec.check(operation, response) {
            Ok(captures) => {
                assert_response_id_shapes(operation, response);
                assert_error_ids_match_headers(operation, response);
                return (index, captures);
            }
            Err(message) => failures.push(message),
        }
    }
    panic!(
        "{operation}: response matches none of the {} accepted shapes\n\n{}",
        specs.len(),
        failures.join("\n\n"),
    );
}

/// Assert that the repeated `<tag>…</tag>` blocks in `body` match the
/// expected block templates, in any order. Placeholder captures are local to
/// each block (a `{version_id}` in one block is independent of the next).
pub fn assert_unordered_xml_blocks(
    operation: &str,
    body: &str,
    tag: &str,
    expected_block_templates: &[String],
    subs: &BTreeMap<String, String>,
) {
    let blocks = extract_xml_blocks(body, tag);
    assert_eq!(
        blocks.len(),
        expected_block_templates.len(),
        "{operation}: expected {} <{tag}> block(s), found {}\nbody: {body}",
        expected_block_templates.len(),
        blocks.len(),
    );
    let mut used = vec![false; blocks.len()];
    for template in expected_block_templates {
        let mut matched = false;
        for (i, block) in blocks.iter().enumerate() {
            if used[i] {
                continue;
            }
            let mut captures = BTreeMap::new();
            if match_template(
                &format!("{operation}: <{tag}> block"),
                template,
                block,
                subs,
                &mut captures,
            )
            .is_ok()
            {
                used[i] = true;
                matched = true;
                break;
            }
        }
        assert!(
            matched,
            "{operation}: no <{tag}> block matches template {template:?}\nblocks: {blocks:?}"
        );
    }
}

/// The wire-ID header expectations present on every S3 response. Base set
/// for responses sized with `Content-Length`, e.g. GetBucketLifecycle.
pub fn id_headers() -> Vec<(&'static str, &'static str)> {
    vec![
        (REQUEST_ID_HEADER, "{request_id}"),
        (HOST_ID_HEADER, "{host_id}"),
    ]
}

/// Wire IDs plus chunked transfer encoding: the header set of XML responses
/// that carry no `Content-Type`, e.g. GetBucketVersioning.
pub fn chunked_response_headers() -> Vec<(&'static str, &'static str)> {
    let mut headers = id_headers();
    headers.push(("transfer-encoding", "chunked"));
    headers
}

/// The standard header set for a chunked XML response with `Content-Type`,
/// e.g. GetBucketLocation or GetBucketAcl.
pub fn xml_response_headers() -> Vec<(&'static str, &'static str)> {
    let mut headers = chunked_response_headers();
    headers.push(("content-type", "application/xml"));
    headers
}

/// Assert that `body` consists of exactly `envelope_template` plus the
/// repeated `<tag>…</tag>` blocks, which are matched unordered against
/// `block_templates`. The matched blocks are removed and the remainder must
/// match the envelope exactly, so unexpected sibling elements fail.
pub fn assert_body_with_unordered_blocks(
    operation: &str,
    body: &str,
    envelope_template: &str,
    tag: &str,
    block_templates: &[String],
    subs: &BTreeMap<String, String>,
) {
    assert_unordered_xml_blocks(operation, body, tag, block_templates, subs);
    let mut remaining = body.to_string();
    for block in extract_xml_blocks(body, tag) {
        remaining = remaining.replacen(block, "", 1);
    }
    let mut captures = BTreeMap::new();
    if let Err(message) = match_template(
        &format!("{operation}: envelope"),
        envelope_template,
        &remaining,
        subs,
        &mut captures,
    ) {
        panic!("{message}");
    }
}

/// The standard header set for an S3 XML error response. Extend per test
/// where AWS adds operation-specific headers.
pub fn error_response_headers() -> Vec<(&'static str, &'static str)> {
    xml_response_headers()
}

/// Expected error-body templates generated from the production error
/// formatters in `server_http::http::xml`.
///
/// Against the local server the body comparison is tautological (the server
/// renders errors with these same formatters); the checks that still bite
/// locally are status, the header set, and the wire-ID invariants. The body
/// templates are validated for real by the external AWS `s3-tests` runs, and
/// each formatter used here must keep one literal-template anchor test so a
/// formatter change fails locally rather than at the next AWS run.
pub mod expected_error {
    use server_http::http::xml;

    const REQUEST_ID: &str = "{request_id}";
    const HOST_ID: &str = "{host_id}";

    /// `<Code>{code}</Code><Message>{message}</Message>` with request and
    /// host IDs — the most common S3 error body shape.
    pub fn with_host_id(code: &str, message: &str) -> String {
        xml::error_xml_with_host_id(code, message, REQUEST_ID, HOST_ID)
    }

    /// Error body with a `<Resource>` element and no `HostId`.
    pub fn with_resource(code: &str, message: &str, resource: &str) -> String {
        xml::error_xml(code, message, resource, REQUEST_ID)
    }

    /// Error body with a `<Region>` element, e.g. SigV4 wrong-region.
    pub fn with_region(code: &str, message: &str, region: &str) -> String {
        xml::error_xml_with_region(code, message, REQUEST_ID, HOST_ID, region)
    }

    pub fn no_such_bucket(bucket: &str) -> String {
        xml::no_such_bucket_error_xml(bucket, REQUEST_ID, HOST_ID)
    }

    pub fn no_such_key(key: &str) -> String {
        xml::no_such_key_error_xml(key, REQUEST_ID, HOST_ID)
    }

    pub fn no_such_bucket_policy(bucket: &str) -> String {
        xml::no_such_bucket_policy_error_xml(bucket, REQUEST_ID, HOST_ID)
    }

    pub fn no_such_upload(upload_id: &str) -> String {
        xml::no_such_upload_error_xml(upload_id, REQUEST_ID, HOST_ID)
    }

    pub fn invalid_range(range_requested: &str, total_size: u64) -> String {
        xml::invalid_range_error_xml(range_requested, total_size, REQUEST_ID, HOST_ID)
    }

    /// `PreconditionFailed` naming the failing conditional header.
    pub fn precondition_failed(condition: &str) -> String {
        xml::precondition_failed_error_xml(condition, REQUEST_ID, HOST_ID)
    }

    pub fn metadata_too_large(size: usize, max_size_allowed: usize) -> String {
        xml::metadata_too_large_error_xml(size, max_size_allowed, REQUEST_ID, HOST_ID)
    }

    pub fn request_header_section_too_large(max_size_allowed: usize) -> String {
        xml::request_header_section_too_large_error_xml(max_size_allowed, REQUEST_ID, HOST_ID)
    }

    /// `InvalidArgument` naming the offending argument. The message text is
    /// supplied by the test so it stays pinned non-tautologically.
    pub fn invalid_argument(message: &str, argument_name: &str) -> String {
        xml::invalid_argument_error_xml(message, argument_name, None, REQUEST_ID, HOST_ID)
    }

    /// `InvalidArgument` naming the offending argument and echoing its value.
    pub fn invalid_argument_with_value(
        message: &str,
        argument_name: &str,
        argument_value: &str,
    ) -> String {
        xml::invalid_argument_error_xml(
            message,
            argument_name,
            Some(argument_value),
            REQUEST_ID,
            HOST_ID,
        )
    }

    /// `InvalidEncryptionAlgorithmError` echoing the rejected value.
    pub fn invalid_encryption_algorithm(message: &str, value: &str) -> String {
        xml::invalid_encryption_algorithm_error_xml(message, value, REQUEST_ID, HOST_ID)
    }

    /// `InvalidToken` echoing the rejected security token.
    pub fn invalid_token(message: &str, token: &str) -> String {
        xml::invalid_token_error_xml(message, token, REQUEST_ID, HOST_ID)
    }

    pub fn complete_multipart_no_such_upload(upload_id: &str) -> String {
        xml::complete_multipart_no_such_upload_error_xml(upload_id, REQUEST_ID, HOST_ID)
    }

    pub fn complete_multipart_invalid_part(
        upload_id: &str,
        part_number: u32,
        etag: &str,
    ) -> String {
        xml::complete_multipart_invalid_part_error_xml(
            upload_id,
            part_number,
            etag,
            REQUEST_ID,
            HOST_ID,
        )
    }

    pub fn complete_multipart_invalid_part_order(upload_id: &str) -> String {
        xml::complete_multipart_invalid_part_order_error_xml(upload_id, REQUEST_ID, HOST_ID)
    }

    pub fn complete_multipart_entity_too_small(
        proposed_size: u64,
        min_size_allowed: u64,
        part_number: u32,
        etag: &str,
    ) -> String {
        xml::complete_multipart_entity_too_small_error_xml(
            proposed_size,
            min_size_allowed,
            part_number,
            etag,
            REQUEST_ID,
            HOST_ID,
        )
    }

    pub fn complete_multipart_missing_part_checksum(algorithm: &str, part_number: u32) -> String {
        xml::complete_multipart_missing_part_checksum_error_xml(
            algorithm,
            part_number,
            REQUEST_ID,
            HOST_ID,
        )
    }

    pub fn complete_multipart_checksum_header_invalid(header_name: &str) -> String {
        xml::complete_multipart_checksum_header_invalid_error_xml(header_name, REQUEST_ID, HOST_ID)
    }

    pub fn upload_part_copy_invalid_range(range_header: &str, source_size: u64) -> String {
        xml::upload_part_copy_invalid_range_error_xml(
            range_header,
            source_size,
            REQUEST_ID,
            HOST_ID,
        )
    }

    pub fn upload_part_copy_precondition_failed(condition: &str) -> String {
        xml::upload_part_copy_precondition_failed_error_xml(condition, REQUEST_ID, HOST_ID)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    thread_local! {
        static SUPPRESS_EXPECTED_PANIC_OUTPUT: std::cell::Cell<bool> =
            const { std::cell::Cell::new(false) };
    }

    static EXPECTED_PANIC_HOOK: std::sync::Once = std::sync::Once::new();

    /// RAII guard silencing the default panic message while a should-panic
    /// test exercises an expected `assert_shape` failure.
    struct SuppressExpectedPanicOutput {
        previous_suppressed: bool,
    }

    impl SuppressExpectedPanicOutput {
        fn new() -> Self {
            EXPECTED_PANIC_HOOK.call_once(|| {
                let previous_hook = std::panic::take_hook();
                std::panic::set_hook(Box::new(move |panic_info| {
                    if SUPPRESS_EXPECTED_PANIC_OUTPUT.with(std::cell::Cell::get) {
                        return;
                    }
                    previous_hook(panic_info);
                }));
            });
            let previous_suppressed = SUPPRESS_EXPECTED_PANIC_OUTPUT.with(|suppressed| {
                let previous = suppressed.get();
                suppressed.set(true);
                previous
            });
            Self {
                previous_suppressed,
            }
        }
    }

    impl Drop for SuppressExpectedPanicOutput {
        fn drop(&mut self) {
            SUPPRESS_EXPECTED_PANIC_OUTPUT
                .with(|suppressed| suppressed.set(self.previous_suppressed));
        }
    }

    fn response(status: u16, headers: &[(&str, &str)], body: &str) -> RawResponse {
        RawResponse {
            status,
            headers: headers
                .iter()
                .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
                .collect(),
            body: body.to_string(),
            body_read_error: None,
        }
    }

    const REQUEST_ID: &str = "ABCDEF0123456789";
    const HOST_ID: &str = "aaaabbbbccccddddeeeeffffgggghhhh11112222333344==";

    fn error_body(code: &str) -> String {
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>{code}</Code>\
             <Message>msg</Message><RequestId>{REQUEST_ID}</RequestId>\
             <HostId>{HOST_ID}</HostId></Error>"
        )
    }

    fn error_template() -> String {
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>{code}</Code>\
         <Message>msg</Message><RequestId>{request_id}</RequestId>\
         <HostId>{host_id}</HostId></Error>"
            .to_string()
    }

    #[test]
    fn template_matches_and_captures() {
        let mut captures = BTreeMap::new();
        let subs = BTreeMap::from([("code".to_string(), "NoSuchKey".to_string())]);
        match_template(
            "test",
            &error_template(),
            &error_body("NoSuchKey"),
            &subs,
            &mut captures,
        )
        .expect("template should match");
        assert_eq!(captures["request_id"], REQUEST_ID);
        assert_eq!(captures["host_id"], HOST_ID);
    }

    #[test]
    fn template_rejects_wrong_literal() {
        let mut captures = BTreeMap::new();
        let subs = BTreeMap::from([("code".to_string(), "NoSuchKey".to_string())]);
        let err = match_template(
            "test",
            &error_template(),
            &error_body("AccessDenied"),
            &subs,
            &mut captures,
        )
        .expect_err("wrong code must not match");
        assert!(err.contains("NoSuchKey"), "unhelpful error: {err}");
    }

    #[test]
    fn template_rejects_bad_placeholder_shape() {
        let mut captures = BTreeMap::new();
        let err = match_template(
            "test",
            "<RequestId>{request_id}</RequestId>",
            "<RequestId>lowercase-is-wrong</RequestId>",
            &BTreeMap::new(),
            &mut captures,
        )
        .expect_err("bad request id shape must not match");
        assert!(err.contains("{request_id}"), "unhelpful error: {err}");
    }

    #[test]
    fn template_enforces_capture_consistency() {
        let mut captures = BTreeMap::new();
        let err = match_template(
            "test",
            "<A>{request_id}</A><B>{request_id}</B>",
            "<A>ABCDEF0123456789</A><B>ABCDEF9876543210</B>",
            &BTreeMap::new(),
            &mut captures,
        )
        .expect_err("inconsistent request ids must not match");
        assert!(err.contains("request_id"), "unhelpful error: {err}");
    }

    #[test]
    fn template_rejects_trailing_content() {
        let mut captures = BTreeMap::new();
        let err = match_template(
            "test",
            "<Code>X</Code>",
            "<Code>X</Code><Extra/>",
            &BTreeMap::new(),
            &mut captures,
        )
        .expect_err("trailing content must not match");
        assert!(err.contains("trailing"), "unhelpful error: {err}");
    }

    #[test]
    fn shape_spec_checks_full_header_set() {
        let resp = response(
            404,
            &[
                ("x-amz-request-id", REQUEST_ID),
                ("x-amz-id-2", HOST_ID),
                ("Content-Type", "application/xml"),
                ("Transfer-Encoding", "chunked"),
                ("Date", "ignored"),
                ("Server", "AmazonS3"),
            ],
            &error_body("NoSuchKey"),
        );
        let spec = shape()
            .status(404)
            .headers(error_response_headers())
            .body(error_template())
            .sub("code", "NoSuchKey");
        assert_shape("test", &resp, &spec);
    }

    #[test]
    #[should_panic(expected = "unexpected")]
    fn shape_spec_rejects_unexpected_header() {
        let _panic_guard = SuppressExpectedPanicOutput::new();
        let resp = response(
            404,
            &[
                ("x-amz-request-id", REQUEST_ID),
                ("x-amz-id-2", HOST_ID),
                ("Content-Type", "application/xml"),
                ("Transfer-Encoding", "chunked"),
                ("x-amz-surprise", "1"),
            ],
            &error_body("NoSuchKey"),
        );
        let spec = shape()
            .status(404)
            .headers(error_response_headers())
            .body(error_template())
            .sub("code", "NoSuchKey");
        assert_shape("test", &resp, &spec);
    }

    #[test]
    #[should_panic(expected = "missing")]
    fn shape_spec_rejects_missing_header() {
        let _panic_guard = SuppressExpectedPanicOutput::new();
        let resp = response(
            404,
            &[
                ("x-amz-request-id", REQUEST_ID),
                ("x-amz-id-2", HOST_ID),
                ("Content-Type", "application/xml"),
            ],
            &error_body("NoSuchKey"),
        );
        let spec = shape()
            .status(404)
            .headers(error_response_headers())
            .body(error_template())
            .sub("code", "NoSuchKey");
        assert_shape("test", &resp, &spec);
    }

    #[test]
    #[should_panic(expected = "{request_id}")]
    fn shape_spec_rejects_body_header_id_mismatch() {
        let _panic_guard = SuppressExpectedPanicOutput::new();
        let resp = response(
            404,
            &[
                ("x-amz-request-id", "9999999999999999"),
                ("x-amz-id-2", HOST_ID),
                ("Content-Type", "application/xml"),
                ("Transfer-Encoding", "chunked"),
            ],
            &error_body("NoSuchKey"),
        );
        let spec = shape()
            .status(404)
            .headers(error_response_headers())
            .body(error_template())
            .sub("code", "NoSuchKey");
        assert_shape("test", &resp, &spec);
    }

    #[test]
    fn one_of_accepts_second_alternative() {
        let resp = response(400, &[], &error_body("InvalidArgument"));
        assert_shape_one_of(
            "test",
            &resp,
            &[
                shape().status(500),
                shape()
                    .status(400)
                    .body(error_template())
                    .sub("code", "InvalidArgument"),
            ],
        );
    }

    #[test]
    #[should_panic(expected = "matches none")]
    fn one_of_rejects_when_nothing_matches() {
        let _panic_guard = SuppressExpectedPanicOutput::new();
        let resp = response(403, &[], "");
        assert_shape_one_of("test", &resp, &[shape().status(500), shape().status(400)]);
    }

    #[test]
    fn expected_error_builders_are_templates() {
        let template = expected_error::no_such_key("k.txt");
        let body = template
            .replace("{request_id}", REQUEST_ID)
            .replace("{host_id}", HOST_ID);
        let mut captures = BTreeMap::new();
        match_template("test", &template, &body, &BTreeMap::new(), &mut captures)
            .expect("builder output should be a valid template");
        assert_eq!(captures["request_id"], REQUEST_ID);
    }

    #[test]
    fn body_with_unordered_blocks_rejects_extra_siblings() {
        let envelope = "<DeleteResult></DeleteResult>";
        let blocks = ["<Deleted><Key>a</Key></Deleted>".to_string()];
        assert_body_with_unordered_blocks(
            "test",
            "<DeleteResult><Deleted><Key>a</Key></Deleted></DeleteResult>",
            envelope,
            "Deleted",
            &blocks,
            &BTreeMap::new(),
        );
        let result = std::panic::catch_unwind(|| {
            let _guard = SuppressExpectedPanicOutput::new();
            assert_body_with_unordered_blocks(
                "test",
                "<DeleteResult><Deleted><Key>a</Key></Deleted><Extra/></DeleteResult>",
                envelope,
                "Deleted",
                &blocks,
                &BTreeMap::new(),
            );
        });
        assert!(result.is_err(), "extra sibling element must fail");
    }

    #[test]
    fn unordered_blocks_match_in_any_order() {
        let body = "<DeleteResult><Deleted><Key>b</Key></Deleted>\
                    <Deleted><Key>a</Key></Deleted></DeleteResult>";
        assert_unordered_xml_blocks(
            "test",
            body,
            "Deleted",
            &[
                "<Deleted><Key>a</Key></Deleted>".to_string(),
                "<Deleted><Key>b</Key></Deleted>".to_string(),
            ],
            &BTreeMap::new(),
        );
    }

    /// Literal anchors for every production error formatter used by
    /// [`expected_error`]. Shape tests that build expectations from these
    /// formatters are tautological for the body against the local server, so
    /// each formatter's exact output is pinned here; a formatter change must
    /// fail this test locally, not wait for the next AWS run.
    #[test]
    fn expected_error_literal_anchors() {
        assert_eq!(
            expected_error::with_host_id("AccessDenied", "Access Denied"),
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>AccessDenied</Code>\
             <Message>Access Denied</Message><RequestId>{request_id}</RequestId>\
             <HostId>{host_id}</HostId></Error>"
        );
        assert_eq!(
            expected_error::with_resource("MethodNotAllowed", "msg", "/b/k"),
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>MethodNotAllowed</Code>\
             <Message>msg</Message><Resource>/b/k</Resource>\
             <RequestId>{request_id}</RequestId></Error>"
        );
        assert_eq!(
            expected_error::with_region("AuthorizationHeaderMalformed", "msg", "us-west-2"),
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error>\
             <Code>AuthorizationHeaderMalformed</Code><Message>msg</Message>\
             <Region>us-west-2</Region><RequestId>{request_id}</RequestId>\
             <HostId>{host_id}</HostId></Error>"
        );
        assert_eq!(
            expected_error::no_such_bucket("bkt"),
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>NoSuchBucket</Code>\
             <Message>The specified bucket does not exist</Message><BucketName>bkt</BucketName>\
             <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>"
        );
        assert_eq!(
            expected_error::no_such_key("k.txt"),
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>NoSuchKey</Code>\
             <Message>The specified key does not exist.</Message><Key>k.txt</Key>\
             <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>"
        );
        assert_eq!(
            expected_error::no_such_bucket_policy("bkt"),
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>NoSuchBucketPolicy</Code>\
             <Message>The bucket policy does not exist</Message><BucketName>bkt</BucketName>\
             <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>"
        );
        assert_eq!(
            expected_error::no_such_upload("upload-1"),
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>NoSuchUpload</Code>\
             <Message>The specified upload does not exist. The upload ID may be invalid, or \
             the upload may have been aborted or completed.</Message>\
             <UploadId>upload-1</UploadId><RequestId>{request_id}</RequestId>\
             <HostId>{host_id}</HostId></Error>"
        );
        assert_eq!(
            expected_error::invalid_range("bytes=2000-3000", 1024),
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>InvalidRange</Code>\
             <Message>The requested range is not satisfiable</Message>\
             <RangeRequested>bytes=2000-3000</RangeRequested>\
             <ActualObjectSize>1024</ActualObjectSize><RequestId>{request_id}</RequestId>\
             <HostId>{host_id}</HostId></Error>"
        );
        assert_eq!(
            expected_error::precondition_failed("If-Match"),
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <Error><Code>PreconditionFailed</Code><Message>At least one of the pre-conditions \
             you specified did not hold</Message>\
             <Condition>If-Match</Condition>\
             <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>"
        );
        assert_eq!(
            expected_error::request_header_section_too_large(8192),
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error>\
             <Code>RequestHeaderSectionTooLarge</Code>\
             <Message>Your request header section exceeds the maximum allowed size.</Message>\
             <MaxSizeAllowed>8192</MaxSizeAllowed>\
             <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>"
        );
        assert_eq!(
            expected_error::invalid_argument("msg", "x-amz-server-side-encryption"),
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>InvalidArgument</Code>\
             <Message>msg</Message>\
             <ArgumentName>x-amz-server-side-encryption</ArgumentName>\
             <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>"
        );
        assert_eq!(
            expected_error::invalid_argument_with_value("msg", "x-amz-checksum-crc32", "AAAA"),
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>InvalidArgument</Code>\
             <Message>msg</Message><ArgumentName>x-amz-checksum-crc32</ArgumentName>\
             <ArgumentValue>AAAA</ArgumentValue>\
             <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>"
        );
        assert_eq!(
            expected_error::invalid_encryption_algorithm("msg", "aws:kms"),
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error>\
             <Code>InvalidEncryptionAlgorithmError</Code><Message>msg</Message>\
             <ArgumentName>x-amz-server-side-encryption</ArgumentName>\
             <ArgumentValue>aws:kms</ArgumentValue>\
             <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>"
        );
        assert_eq!(
            expected_error::invalid_token("msg", "tok"),
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>InvalidToken</Code>\
             <Message>msg</Message><Token-0>tok</Token-0>\
             <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>"
        );
        assert_eq!(
            expected_error::complete_multipart_no_such_upload("uid-1"),
            "<Error><Code>NoSuchUpload</Code><Message>The specified upload does not exist. \
             The upload ID may be invalid, or the upload may have been aborted or \
             completed.</Message><UploadId>uid-1</UploadId>\
             <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>"
        );
        assert_eq!(
            expected_error::complete_multipart_invalid_part("uid-1", 1, "\"abcd\""),
            "<Error><Code>InvalidPart</Code><Message>One or more of the specified parts could \
             not be found.  The part may not have been uploaded, or the specified entity tag \
             may not match the part's entity tag.</Message><UploadId>uid-1</UploadId>\
             <PartNumber>1</PartNumber><ETag>abcd</ETag>\
             <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>"
        );
        assert_eq!(
            expected_error::complete_multipart_invalid_part_order("uid-1"),
            "<Error><Code>InvalidPartOrder</Code><Message>The list of parts was not in \
             ascending order. Parts must be ordered by part number.</Message>\
             <UploadId>uid-1</UploadId>\
             <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>"
        );
        assert_eq!(
            expected_error::complete_multipart_entity_too_small(100, 5242880, 1, "abcd"),
            "<Error><Code>EntityTooSmall</Code><Message>Your proposed upload is smaller than \
             the minimum allowed size</Message><ProposedSize>100</ProposedSize>\
             <MinSizeAllowed>5242880</MinSizeAllowed><PartNumber>1</PartNumber>\
             <ETag>abcd</ETag>\
             <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>"
        );
        assert_eq!(
            expected_error::complete_multipart_missing_part_checksum("sha256", 1),
            "<Error><Code>InvalidRequest</Code><Message>The upload was created using a sha256 \
             checksum. The complete request must include the checksum for each part. It was \
             missing for part 1 in the request.</Message>\
             <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>"
        );
        assert_eq!(
            expected_error::complete_multipart_checksum_header_invalid("x-amz-checksum-sha256"),
            "<Error><Code>InvalidRequest</Code><Message>Value for x-amz-checksum-sha256 header \
             is invalid.</Message>\
             <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>"
        );
        assert_eq!(
            expected_error::upload_part_copy_invalid_range("bytes=0-9999", 1000),
            "<Error><Code>InvalidArgument</Code><Message>Range specified is not valid for \
             source object of size: 1000</Message>\
             <ArgumentName>x-amz-copy-source-range</ArgumentName>\
             <ArgumentValue>bytes=0-9999</ArgumentValue>\
             <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>"
        );
        assert_eq!(
            expected_error::upload_part_copy_precondition_failed("x-amz-copy-source-If-Match"),
            "<Error><Code>PreconditionFailed</Code><Message>At least one of the pre-conditions \
             you specified did not hold</Message>\
             <Condition>x-amz-copy-source-If-Match</Condition>\
             <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>"
        );
        assert_eq!(
            expected_error::metadata_too_large(3000, 2048),
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>MetadataTooLarge</Code>\
             <Message>Your metadata headers exceed the maximum allowed metadata size</Message>\
             <Size>3000</Size><MaxSizeAllowed>2048</MaxSizeAllowed>\
             <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>"
        );
    }

    #[test]
    fn http_date_and_iso8601_shapes() {
        assert!(is_http_date_shape("Mon, 06 Jul 2026 12:34:56 GMT"));
        assert!(!is_http_date_shape("2026-07-06T12:34:56.000Z"));
        assert!(is_iso8601_shape("2026-07-06T12:34:56.000Z"));
        assert!(is_iso8601_shape("2026-07-06T12:34:56Z"));
        assert!(!is_iso8601_shape("Mon, 06 Jul 2026 12:34:56 GMT"));
    }
}
