use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CorsConfiguration, CorsRule};
use s3_tests::{content_md5_header, send_signed_request, unique_bucket, CTX};
use std::collections::HashMap;
use std::time::Duration;

/// Build an agent that returns all HTTP responses (including 4xx/5xx) as Ok.
fn agent() -> s3_tests::Agent {
    s3_tests::test_agent()
}

struct PreflightSnapshot {
    status: u16,
    headers: HashMap<String, String>,
}

fn preflight_snapshot(
    url: &str,
    origin: Option<&str>,
    request_method: Option<&str>,
    request_headers: Option<&str>,
) -> PreflightSnapshot {
    let mut req = agent().options(url);
    if let Some(origin) = origin {
        req = req.header("Origin", origin);
    }
    if let Some(request_method) = request_method {
        req = req.header("Access-Control-Request-Method", request_method);
    }
    if let Some(request_headers) = request_headers {
        req = req.header("Access-Control-Request-Headers", request_headers);
    }

    let mut resp = req.call().expect("transport error");
    let status = resp.status().as_u16();
    let headers = resp
        .headers()
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_string(), value.to_string()))
        })
        .collect();
    let _ = resp.body_mut().read_to_string();

    PreflightSnapshot { status, headers }
}

fn assert_error_code(body: &str, code: &str) {
    let expected = format!("<Code>{code}</Code>");
    assert!(
        body.contains(&expected),
        "expected {expected} in body: {body}"
    );
}

fn cors_config_xml_with_rules(rule_count: usize) -> String {
    let mut xml = String::from("<CORSConfiguration>");
    for i in 0..rule_count {
        xml.push_str("<CORSRule><AllowedOrigin>https://");
        xml.push_str(&i.to_string());
        xml.push_str(".example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule>");
    }
    xml.push_str("</CORSConfiguration>");
    xml
}

fn oversized_cors_config_xml() -> String {
    let mut xml = String::from("<CORSConfiguration><CORSRule><AllowedOrigin>https://");
    xml.push_str(&"a".repeat(65 * 1024));
    xml.push_str(".example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>");
    xml
}

fn put_bucket_cors_raw(bucket: &str, body: &[u8]) -> s3_tests::RawResponse {
    let url = format!("{}/{bucket}?cors", CTX.endpoint());
    let md5 = content_md5_header(body);
    send_signed_request("PUT", &url, body, [md5])
}

async fn preflight_status_eventually(
    url: &str,
    origin: Option<&str>,
    request_method: Option<&str>,
    request_headers: Option<&str>,
    expected_status: u16,
    description: &str,
) -> PreflightSnapshot {
    const MAX_ATTEMPTS: usize = 40;

    for attempt in 0..MAX_ATTEMPTS {
        let snapshot = preflight_snapshot(url, origin, request_method, request_headers);
        if snapshot.status == expected_status {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let confirmed = preflight_snapshot(url, origin, request_method, request_headers);
            if confirmed.status == expected_status {
                return confirmed;
            }
        }
        if attempt + 1 < MAX_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(250)).await;
            continue;
        }
        panic!(
            "{description} did not converge to HTTP {expected_status} for {url}, last status {}",
            snapshot.status
        );
    }

    unreachable!()
}

/// Create a bucket and set a CORS config on it.
async fn setup_cors_bucket(rules: Vec<CorsRule>) -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();

    let config = CorsConfiguration::builder()
        .set_cors_rules(Some(rules))
        .build()
        .unwrap();

    client
        .put_bucket_cors()
        .bucket(&bucket)
        .cors_configuration(config)
        .send()
        .await
        .unwrap();

    client
        .get_bucket_cors()
        .bucket(&bucket)
        .send()
        .await
        .unwrap();

    bucket
}

/// Simple CORS rule builder for tests.
fn simple_rule(origin: &str, methods: &[&str]) -> CorsRule {
    CorsRule::builder()
        .allowed_origins(origin)
        .set_allowed_methods(Some(methods.iter().map(|m| m.to_string()).collect()))
        .build()
        .unwrap()
}

/// Cleanup helper.
async fn cleanup(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        let _ = client.delete_object().bucket(bucket).key(*key).send().await;
    }
    s3_tests::delete_bucket_retrying_operation_aborted(client, bucket).await;
}

// ── PUT/GET/DELETE CORS configuration ───────────────────────────────────

#[test]
fn test_cors_set_get_delete() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        // PUT CORS config
        let rule = CorsRule::builder()
            .allowed_origins("http://example.com")
            .allowed_methods("GET")
            .allowed_methods("PUT")
            .allowed_headers("*")
            .expose_headers("x-amz-request-id")
            .max_age_seconds(3600)
            .build()
            .unwrap();

        let config = CorsConfiguration::builder()
            .cors_rules(rule)
            .build()
            .unwrap();

        client
            .put_bucket_cors()
            .bucket(&bucket)
            .cors_configuration(config)
            .send()
            .await
            .unwrap();

        // GET CORS config
        let result = client
            .get_bucket_cors()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        let rules = result.cors_rules();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].allowed_origins(), ["http://example.com"]);
        assert!(rules[0].allowed_methods().contains(&"GET".to_string()));
        assert!(rules[0].allowed_methods().contains(&"PUT".to_string()));

        // DELETE CORS config
        client
            .delete_bucket_cors()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        // GET after delete should fail
        let result = client.get_bucket_cors().bucket(&bucket).send().await;
        assert!(result.is_err());

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_cors_get_round_trip_multiple_rules_and_headers() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let rule1 = CorsRule::builder()
            .allowed_origins("http://example.com")
            .allowed_origins("https://example.org")
            .allowed_methods("GET")
            .allowed_methods("PUT")
            .allowed_headers("content-type")
            .allowed_headers("x-amz-meta-*")
            .expose_headers("etag")
            .expose_headers("x-amz-request-id")
            .max_age_seconds(3600)
            .build()
            .unwrap();
        let rule2 = CorsRule::builder()
            .allowed_origins("*")
            .allowed_methods("HEAD")
            .allowed_methods("POST")
            .allowed_headers("authorization")
            .expose_headers("x-amz-version-id")
            .max_age_seconds(60)
            .build()
            .unwrap();

        let config = CorsConfiguration::builder()
            .cors_rules(rule1)
            .cors_rules(rule2)
            .build()
            .unwrap();

        client
            .put_bucket_cors()
            .bucket(&bucket)
            .cors_configuration(config)
            .send()
            .await
            .unwrap();

        let resp = client
            .get_bucket_cors()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let rules = resp.cors_rules();
        assert_eq!(rules.len(), 2);

        assert_eq!(
            rules[0].allowed_origins(),
            ["http://example.com", "https://example.org"]
        );
        assert!(rules[0].allowed_methods().contains(&"GET".to_string()));
        assert!(rules[0].allowed_methods().contains(&"PUT".to_string()));
        assert!(rules[0]
            .allowed_headers()
            .contains(&"content-type".to_string()));
        assert!(rules[0]
            .allowed_headers()
            .contains(&"x-amz-meta-*".to_string()));
        assert!(rules[0].expose_headers().contains(&"etag".to_string()));
        assert!(rules[0]
            .expose_headers()
            .contains(&"x-amz-request-id".to_string()));
        assert_eq!(rules[0].max_age_seconds(), Some(3600));

        assert_eq!(rules[1].allowed_origins(), ["*"]);
        assert!(rules[1].allowed_methods().contains(&"HEAD".to_string()));
        assert!(rules[1].allowed_methods().contains(&"POST".to_string()));
        assert!(rules[1]
            .allowed_headers()
            .contains(&"authorization".to_string()));
        assert!(rules[1]
            .expose_headers()
            .contains(&"x-amz-version-id".to_string()));
        assert_eq!(rules[1].max_age_seconds(), Some(60));

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_cors_raw_get_returns_canonical_xml() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        s3_tests::create_bucket(CTX.client(), &bucket)
            .await
            .unwrap();

        let body = br#"
            <CORSConfiguration>
                <CORSRule>
                    <ExposeHeader>x-amz-request-id</ExposeHeader>
                    <AllowedMethod>PUT</AllowedMethod>
                    <AllowedOrigin>https://example.org</AllowedOrigin>
                    <AllowedHeader>x-amz-meta-*</AllowedHeader>
                    <AllowedMethod>GET</AllowedMethod>
                    <ExposeHeader>etag</ExposeHeader>
                    <AllowedHeader>content-type</AllowedHeader>
                    <MaxAgeSeconds>3600</MaxAgeSeconds>
                </CORSRule>
                <CORSRule>
                    <AllowedMethod>HEAD</AllowedMethod>
                    <AllowedOrigin>*</AllowedOrigin>
                </CORSRule>
            </CORSConfiguration>
        "#;

        let parsed = server_http::http::xml::parse_cors_config_xml(body).unwrap();
        let expected = server_http::http::xml::get_cors_config_xml(&parsed);

        let put = put_bucket_cors_raw(&bucket, body);
        assert_eq!(put.status, 200, "unexpected body: {}", put.body);

        let url = format!("{}/{}?cors", CTX.endpoint(), bucket);
        let get = send_signed_request("GET", &url, b"", std::iter::empty::<(String, String)>());

        cleanup(&bucket, &[]).await;

        assert_eq!(get.status, 200, "unexpected body: {}", get.body);
        assert_eq!(get.body, expected);
    });
}

#[test]
fn test_cors_put_max_rules() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let body = cors_config_xml_with_rules(100);
        let response = put_bucket_cors_raw(&bucket, body.as_bytes());
        assert_eq!(response.status, 200, "unexpected body: {}", response.body);

        let result = client
            .get_bucket_cors()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(result.cors_rules().len(), 100);

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_cors_put_too_many_rules_rejected() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let body = cors_config_xml_with_rules(101);
        let response = put_bucket_cors_raw(&bucket, body.as_bytes());
        assert_eq!(response.status, 400, "unexpected body: {}", response.body);
        assert_error_code(&response.body, "InvalidRequest");

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_cors_put_oversized_config_rejected() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let body = oversized_cors_config_xml();
        assert!(
            body.len() > 64 * 1024,
            "test body must exceed 64 KiB, got {}",
            body.len()
        );
        let response = put_bucket_cors_raw(&bucket, body.as_bytes());
        assert_eq!(response.status, 400, "unexpected body: {}", response.body);
        assert_error_code(&response.body, "MaxMessageLengthExceeded");
        assert!(
            response
                .body
                .contains("<MaxMessageLengthBytes>65536</MaxMessageLengthBytes>"),
            "unexpected body: {}",
            response.body
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_cors_get_no_config() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        // GET CORS with no config should return error
        let result = client.get_bucket_cors().bucket(&bucket).send().await;
        assert!(result.is_err());

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_cors_delete_no_config() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        // DELETE CORS when no config → should succeed (idempotent, 204)
        client
            .delete_bucket_cors()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        cleanup(&bucket, &[]).await;
    });
}

// ── Preflight (OPTIONS) tests ───────────────────────────────────────────

#[test]
fn test_cors_preflight_basic() {
    s3_tests::run(async {
        let rule = simple_rule("http://example.com", &["GET", "PUT"]);
        let bucket = setup_cors_bucket(vec![rule]).await;

        let url = format!("{}/{}", CTX.endpoint(), bucket);
        let resp = preflight_status_eventually(
            &url,
            Some("http://example.com"),
            Some("GET"),
            None,
            200,
            "basic CORS preflight",
        )
        .await;
        assert_eq!(
            resp.headers
                .get("access-control-allow-origin")
                .map(String::as_str),
            Some("http://example.com")
        );
        assert!(resp.headers.contains_key("access-control-allow-methods"));

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_cors_preflight_no_match() {
    s3_tests::run(async {
        let rule = simple_rule("http://example.com", &["GET"]);
        let bucket = setup_cors_bucket(vec![rule]).await;

        let url = format!("{}/{}", CTX.endpoint(), bucket);
        let resp = preflight_status_eventually(
            &url,
            Some("http://other.com"),
            Some("GET"),
            None,
            403,
            "non-matching CORS preflight",
        )
        .await;

        assert_eq!(resp.status, 403);

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_cors_preflight_missing_request_method() {
    s3_tests::run(async {
        let rule = CorsRule::builder()
            .allowed_origins("http://example.com")
            .allowed_methods("GET")
            .build()
            .unwrap();
        let bucket = setup_cors_bucket(vec![rule]).await;

        // OPTIONS with Origin but without Access-Control-Request-Method → 403
        let url = format!("{}/{}", CTX.endpoint(), bucket);
        let mut resp = agent()
            .options(&url)
            .header("Origin", "http://example.com")
            .call()
            .expect("transport error");
        let _ = resp.body_mut().read_to_string();

        assert_eq!(resp.status().as_u16(), 403);

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_cors_preflight_missing_origin() {
    s3_tests::run(async {
        let rule = CorsRule::builder()
            .allowed_origins("http://example.com")
            .allowed_methods("GET")
            .build()
            .unwrap();
        let bucket = setup_cors_bucket(vec![rule]).await;

        // OPTIONS without Origin header → 400
        let url = format!("{}/{}", CTX.endpoint(), bucket);
        let mut resp = agent()
            .options(&url)
            .header("Access-Control-Request-Method", "GET")
            .call()
            .expect("transport error");
        let _ = resp.body_mut().read_to_string();

        assert_eq!(resp.status().as_u16(), 400);

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_cors_preflight_with_headers() {
    s3_tests::run(async {
        let rule = CorsRule::builder()
            .allowed_origins("http://example.com")
            .allowed_methods("GET")
            .allowed_headers("x-custom-header")
            .allowed_headers("content-type")
            .build()
            .unwrap();
        let bucket = setup_cors_bucket(vec![rule]).await;

        let url = format!("{}/{}", CTX.endpoint(), bucket);
        let resp = preflight_status_eventually(
            &url,
            Some("http://example.com"),
            Some("GET"),
            Some("x-custom-header, content-type"),
            200,
            "CORS preflight with headers",
        )
        .await;

        assert_eq!(resp.status, 200);
        assert!(resp.headers.contains_key("access-control-allow-headers"));

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_cors_preflight_wildcard_origin() {
    s3_tests::run(async {
        let rule = simple_rule("*", &["GET"]);
        let bucket = setup_cors_bucket(vec![rule]).await;

        let url = format!("{}/{}", CTX.endpoint(), bucket);
        let resp = preflight_status_eventually(
            &url,
            Some("http://anything.com"),
            Some("GET"),
            None,
            200,
            "wildcard-origin CORS preflight",
        )
        .await;

        assert_eq!(resp.status, 200);
        assert_eq!(
            resp.headers
                .get("access-control-allow-origin")
                .map(String::as_str),
            Some("*")
        );
        // Wildcard origin should NOT set Allow-Credentials
        assert!(!resp
            .headers
            .contains_key("access-control-allow-credentials"));

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_cors_preflight_wildcard_headers() {
    s3_tests::run(async {
        let rule = CorsRule::builder()
            .allowed_origins("http://example.com")
            .allowed_methods("GET")
            .allowed_headers("*")
            .build()
            .unwrap();
        let bucket = setup_cors_bucket(vec![rule]).await;

        let url = format!("{}/{}", CTX.endpoint(), bucket);
        let resp = preflight_status_eventually(
            &url,
            Some("http://example.com"),
            Some("GET"),
            Some("x-anything, x-whatever"),
            200,
            "wildcard-header CORS preflight",
        )
        .await;

        assert_eq!(resp.status, 200);

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_cors_preflight_rejects_disallowed_request_headers() {
    s3_tests::run(async {
        let rule = CorsRule::builder()
            .allowed_origins("http://example.com")
            .allowed_methods("GET")
            .allowed_headers("x-amz-meta-header1")
            .build()
            .unwrap();
        let bucket = setup_cors_bucket(vec![rule]).await;

        let url = format!("{}/{}/missing", CTX.endpoint(), bucket);
        let resp = preflight_status_eventually(
            &url,
            Some("http://example.com"),
            Some("GET"),
            Some("x-amz-meta-header2"),
            403,
            "disallowed-header CORS preflight",
        )
        .await;

        assert_eq!(resp.status, 403);
        assert!(!resp.headers.contains_key("access-control-allow-origin"));
        assert!(!resp.headers.contains_key("access-control-allow-methods"));

        cleanup(&bucket, &[]).await;
    });
}

// ── Actual request CORS headers ─────────────────────────────────────────

#[test]
fn test_cors_actual_request_headers() {
    s3_tests::run(async {
        let client = CTX.client();
        let rule = CorsRule::builder()
            .allowed_origins("http://example.com")
            .allowed_methods("GET")
            .expose_headers("x-amz-request-id")
            .build()
            .unwrap();
        let bucket = setup_cors_bucket(vec![rule]).await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"hello"))
            .send()
            .await
            .unwrap();

        // Anonymous GET on private bucket returns 403, but CORS headers
        // should still be appended when origin matches.
        let url = format!("{}/{}/obj", CTX.endpoint(), bucket);
        let mut resp = agent()
            .get(&url)
            .header("Origin", "http://example.com")
            .call()
            .expect("transport error");
        let _ = resp.body_mut().read_to_string();

        assert_eq!(
            resp.headers()
                .get("Access-Control-Allow-Origin")
                .map(|h| h.to_str().unwrap()),
            Some("http://example.com"),
            "CORS headers should be present even on 403 responses"
        );

        cleanup(&bucket, &["obj"]).await;
    });
}

// ── Multiple rules ──────────────────────────────────────────────────────

#[test]
fn test_cors_multiple_rules() {
    s3_tests::run(async {
        let rule1 = CorsRule::builder()
            .allowed_origins("http://first.com")
            .allowed_methods("GET")
            .max_age_seconds(100)
            .build()
            .unwrap();
        let rule2 = CorsRule::builder()
            .allowed_origins("*")
            .allowed_methods("GET")
            .allowed_methods("POST")
            .max_age_seconds(200)
            .build()
            .unwrap();
        let bucket = setup_cors_bucket(vec![rule1, rule2]).await;

        // http://first.com should match rule1 (first match wins)
        let url = format!("{}/{}", CTX.endpoint(), bucket);
        let mut resp = agent()
            .options(&url)
            .header("Origin", "http://first.com")
            .header("Access-Control-Request-Method", "GET")
            .call()
            .expect("transport error");
        let _ = resp.body_mut().read_to_string();

        assert_eq!(resp.status().as_u16(), 200);
        // Should get max-age from rule1 (100), not rule2 (200)
        assert_eq!(
            resp.headers()
                .get("Access-Control-Max-Age")
                .map(|h| h.to_str().unwrap()),
            Some("100")
        );

        // http://other.com should match rule2 (wildcard)
        let mut resp = agent()
            .options(&url)
            .header("Origin", "http://other.com")
            .header("Access-Control-Request-Method", "GET")
            .call()
            .expect("transport error");
        let _ = resp.body_mut().read_to_string();

        assert_eq!(resp.status().as_u16(), 200);
        assert_eq!(
            resp.headers()
                .get("Access-Control-Max-Age")
                .map(|h| h.to_str().unwrap()),
            Some("200")
        );

        cleanup(&bucket, &[]).await;
    });
}

// ── Wildcard origin pattern ─────────────────────────────────────────────

#[test]
fn test_cors_origin_with_wildcard() {
    s3_tests::run(async {
        let rule = CorsRule::builder()
            .allowed_origins("http://*.example.com")
            .allowed_methods("GET")
            .build()
            .unwrap();
        let bucket = setup_cors_bucket(vec![rule]).await;

        let url = format!("{}/{}", CTX.endpoint(), bucket);

        // Subdomain should match
        let mut resp = agent()
            .options(&url)
            .header("Origin", "http://sub.example.com")
            .header("Access-Control-Request-Method", "GET")
            .call()
            .expect("transport error");
        let _ = resp.body_mut().read_to_string();
        assert_eq!(resp.status().as_u16(), 200);

        // Bare domain should not match
        let mut resp = agent()
            .options(&url)
            .header("Origin", "http://example.com")
            .header("Access-Control-Request-Method", "GET")
            .call()
            .expect("transport error");
        let _ = resp.body_mut().read_to_string();
        assert_eq!(resp.status().as_u16(), 403);

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_cors_preflight_origin_wildcard_matrix() {
    s3_tests::run(async {
        let bucket = setup_cors_bucket(vec![
            simple_rule("http://*suffix", &["GET"]),
            simple_rule("http://start*end", &["GET"]),
            simple_rule("http://prefix*", &["GET"]),
            simple_rule("http://*.put", &["PUT"]),
        ])
        .await;

        let url = format!("{}/{}", CTX.endpoint(), bucket);
        let cases = [
            (
                "http://foo.suffix",
                "GET",
                200,
                Some("http://foo.suffix"),
                Some("GET"),
            ),
            ("http://foo.suffix.get", "GET", 403, None, None),
            (
                "http://startend",
                "GET",
                200,
                Some("http://startend"),
                Some("GET"),
            ),
            (
                "http://start12end",
                "GET",
                200,
                Some("http://start12end"),
                Some("GET"),
            ),
            ("http://0start12end", "GET", 403, None, None),
            (
                "http://prefix",
                "GET",
                200,
                Some("http://prefix"),
                Some("GET"),
            ),
            (
                "http://prefix.suffix",
                "GET",
                200,
                Some("http://prefix.suffix"),
                Some("GET"),
            ),
            ("http://bla.prefix", "GET", 403, None, None),
            ("http://foo.put", "GET", 403, None, None),
            (
                "http://foo.put",
                "PUT",
                200,
                Some("http://foo.put"),
                Some("PUT"),
            ),
        ];

        for (origin, method, status, allow_origin, allow_methods) in cases {
            let mut resp = agent()
                .options(&url)
                .header("Origin", origin)
                .header("Access-Control-Request-Method", method)
                .call()
                .expect("transport error");
            let _ = resp.body_mut().read_to_string();

            assert_eq!(
                resp.status().as_u16(),
                status,
                "origin={origin} method={method}"
            );
            assert_eq!(
                resp.headers()
                    .get("Access-Control-Allow-Origin")
                    .map(|h| h.to_str().unwrap()),
                allow_origin,
                "origin={origin} method={method}"
            );
            assert_eq!(
                resp.headers()
                    .get("Access-Control-Allow-Methods")
                    .map(|h| h.to_str().unwrap()),
                allow_methods,
                "origin={origin} method={method}"
            );
        }

        cleanup(&bucket, &[]).await;
    });
}

// ── MaxAgeSeconds ───────────────────────────────────────────────────────

#[test]
fn test_cors_max_age() {
    s3_tests::run(async {
        let rule = CorsRule::builder()
            .allowed_origins("http://example.com")
            .allowed_methods("GET")
            .max_age_seconds(7200)
            .build()
            .unwrap();
        let bucket = setup_cors_bucket(vec![rule]).await;

        let url = format!("{}/{}", CTX.endpoint(), bucket);
        let mut resp = agent()
            .options(&url)
            .header("Origin", "http://example.com")
            .header("Access-Control-Request-Method", "GET")
            .call()
            .expect("transport error");
        let _ = resp.body_mut().read_to_string();

        assert_eq!(resp.status().as_u16(), 200);
        assert_eq!(
            resp.headers()
                .get("Access-Control-Max-Age")
                .map(|h| h.to_str().unwrap()),
            Some("7200")
        );

        cleanup(&bucket, &[]).await;
    });
}
