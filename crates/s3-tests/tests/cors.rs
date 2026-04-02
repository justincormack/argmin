use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CorsConfiguration, CorsRule, ObjectCannedAcl};
use s3_tests::{unique_bucket, CTX};
use std::time::Duration;

/// Build an agent that returns all HTTP responses (including 4xx/5xx) as Ok.
fn agent() -> ureq::Agent {
    s3_tests::test_agent()
}

/// Create a bucket and set a CORS config on it.
async fn setup_cors_bucket(rules: Vec<CorsRule>) -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    client.create_bucket().bucket(&bucket).send().await.unwrap();

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

    bucket
}

async fn setup_public_cors_bucket(rules: Vec<CorsRule>) -> String {
    let client = CTX.client();
    let bucket = s3_tests::create_public_bucket(client).await;

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
    client.delete_bucket().bucket(bucket).send().await.unwrap();
}

// ── PUT/GET/DELETE CORS configuration ───────────────────────────────────

#[test]
fn test_cors_set_get_delete() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

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
        client.create_bucket().bucket(&bucket).send().await.unwrap();

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
fn test_cors_get_no_config() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

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
        client.create_bucket().bucket(&bucket).send().await.unwrap();

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
                .get("Access-Control-Allow-Origin")
                .map(|h| h.to_str().unwrap()),
            Some("http://example.com")
        );
        assert!(resp.headers().get("Access-Control-Allow-Methods").is_some());

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_cors_preflight_no_match() {
    s3_tests::run(async {
        let rule = simple_rule("http://example.com", &["GET"]);
        let bucket = setup_cors_bucket(vec![rule]).await;

        let url = format!("{}/{}", CTX.endpoint(), bucket);
        let mut resp = agent()
            .options(&url)
            .header("Origin", "http://other.com")
            .header("Access-Control-Request-Method", "GET")
            .call()
            .expect("transport error");
        let _ = resp.body_mut().read_to_string();

        assert_eq!(resp.status().as_u16(), 403);

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
        let mut resp = agent()
            .options(&url)
            .header("Origin", "http://example.com")
            .header("Access-Control-Request-Method", "GET")
            .header(
                "Access-Control-Request-Headers",
                "x-custom-header, content-type",
            )
            .call()
            .expect("transport error");
        let _ = resp.body_mut().read_to_string();

        assert_eq!(resp.status().as_u16(), 200);
        assert!(resp.headers().get("Access-Control-Allow-Headers").is_some());

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_cors_preflight_wildcard_origin() {
    s3_tests::run(async {
        let rule = simple_rule("*", &["GET"]);
        let bucket = setup_cors_bucket(vec![rule]).await;

        let url = format!("{}/{}", CTX.endpoint(), bucket);
        let mut resp = agent()
            .options(&url)
            .header("Origin", "http://anything.com")
            .header("Access-Control-Request-Method", "GET")
            .call()
            .expect("transport error");
        let _ = resp.body_mut().read_to_string();

        assert_eq!(resp.status().as_u16(), 200);
        assert_eq!(
            resp.headers()
                .get("Access-Control-Allow-Origin")
                .map(|h| h.to_str().unwrap()),
            Some("*")
        );
        // Wildcard origin should NOT set Allow-Credentials
        assert!(resp
            .headers()
            .get("Access-Control-Allow-Credentials")
            .is_none());

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
        let mut resp = agent()
            .options(&url)
            .header("Origin", "http://example.com")
            .header("Access-Control-Request-Method", "GET")
            .header("Access-Control-Request-Headers", "x-anything, x-whatever")
            .call()
            .expect("transport error");
        let _ = resp.body_mut().read_to_string();

        assert_eq!(resp.status().as_u16(), 200);

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
        let mut resp = agent()
            .options(&url)
            .header("Origin", "http://example.com")
            .header("Access-Control-Request-Method", "GET")
            .header("Access-Control-Request-Headers", "x-amz-meta-header2")
            .call()
            .expect("transport error");
        let _ = resp.body_mut().read_to_string();

        assert_eq!(resp.status().as_u16(), 403);
        assert!(resp.headers().get("Access-Control-Allow-Origin").is_none());
        assert!(resp.headers().get("Access-Control-Allow-Methods").is_none());

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

#[test]
fn test_cors_actual_request_public_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = s3_tests::create_public_bucket(client).await;

        let rule = CorsRule::builder()
            .allowed_origins("http://example.com")
            .allowed_methods("GET")
            .expose_headers("x-amz-request-id")
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

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .acl(ObjectCannedAcl::PublicRead)
            .body(ByteStream::from_static(b"hello"))
            .send()
            .await
            .unwrap();

        // Anonymous GET with Origin header
        let url = format!("{}/{}/obj", CTX.endpoint(), bucket);
        let mut resp = agent()
            .get(&url)
            .header("Origin", "http://example.com")
            .call()
            .expect("transport error");
        let _ = resp.body_mut().read_to_vec();

        assert_eq!(resp.status().as_u16(), 200);
        assert_eq!(
            resp.headers()
                .get("Access-Control-Allow-Origin")
                .map(|h| h.to_str().unwrap()),
            Some("http://example.com")
        );
        assert!(resp
            .headers()
            .get("Access-Control-Expose-Headers")
            .is_some());
        assert_eq!(
            resp.headers()
                .get("Access-Control-Allow-Credentials")
                .map(|h| h.to_str().unwrap()),
            Some("true")
        );

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_cors_actual_request_no_match() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = s3_tests::create_public_bucket(client).await;

        let rule = CorsRule::builder()
            .allowed_origins("http://specific.com")
            .allowed_methods("GET")
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

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .acl(ObjectCannedAcl::PublicRead)
            .body(ByteStream::from_static(b"hello"))
            .send()
            .await
            .unwrap();

        // Anonymous GET with non-matching Origin — no CORS headers
        let url = format!("{}/{}/obj", CTX.endpoint(), bucket);
        let mut resp = agent()
            .get(&url)
            .header("Origin", "http://other.com")
            .call()
            .expect("transport error");
        let _ = resp.body_mut().read_to_vec();

        assert_eq!(resp.status().as_u16(), 200);
        assert!(
            resp.headers().get("Access-Control-Allow-Origin").is_none(),
            "no CORS headers should be present for non-matching origin"
        );

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_cors_actual_request_missing_object_headers() {
    s3_tests::run(async {
        let rule = CorsRule::builder()
            .allowed_origins("http://example.com")
            .allowed_methods("GET")
            .build()
            .unwrap();
        let bucket = setup_public_cors_bucket(vec![rule]).await;

        let url = format!("{}/{}/missing", CTX.endpoint(), bucket);
        let mut resp = agent()
            .get(&url)
            .header("Origin", "http://example.com")
            .call()
            .expect("transport error");
        let _ = resp.body_mut().read_to_string();

        assert_eq!(resp.status().as_u16(), 404);
        assert_eq!(
            resp.headers()
                .get("Access-Control-Allow-Origin")
                .map(|h| h.to_str().unwrap()),
            Some("http://example.com")
        );
        assert_eq!(
            resp.headers()
                .get("Access-Control-Allow-Methods")
                .map(|h| h.to_str().unwrap()),
            Some("GET")
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_cors_actual_request_put_uses_request_method_for_matching() {
    s3_tests::run(async {
        let rule = CorsRule::builder()
            .allowed_origins("http://example.put")
            .allowed_methods("PUT")
            .build()
            .unwrap();
        let bucket = setup_public_cors_bucket(vec![rule]).await;

        let url = format!("{}/{}/obj", CTX.endpoint(), bucket);
        let mut resp = agent()
            .put(&url)
            .header("Origin", "http://example.put")
            .header("Content-Length", "0")
            .send_empty()
            .expect("transport error");
        let _ = resp.body_mut().read_to_string();

        assert_eq!(resp.status().as_u16(), 403);
        assert_eq!(
            resp.headers()
                .get("Access-Control-Allow-Origin")
                .map(|h| h.to_str().unwrap()),
            Some("http://example.put")
        );
        assert_eq!(
            resp.headers()
                .get("Access-Control-Allow-Methods")
                .map(|h| h.to_str().unwrap()),
            Some("PUT")
        );

        cleanup(&bucket, &[]).await;
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
fn test_cors_actual_request_origin_wildcard_matrix() {
    s3_tests::run(async {
        let bucket = setup_public_cors_bucket(vec![
            simple_rule("http://*suffix", &["GET"]),
            simple_rule("http://start*end", &["GET"]),
            simple_rule("http://prefix*", &["GET"]),
        ])
        .await;
        let client = CTX.client();
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .acl(ObjectCannedAcl::PublicRead)
            .body(ByteStream::from_static(b"hello"))
            .send()
            .await
            .unwrap();

        let cases = [
            ("http://foo.suffix", Some("http://foo.suffix")),
            ("http://foo.bar", None),
            ("http://startend", Some("http://startend")),
            ("http://start12end", Some("http://start12end")),
            ("http://0start12end", None),
            ("http://prefix", Some("http://prefix")),
            ("http://prefix.suffix", Some("http://prefix.suffix")),
            ("http://bla.prefix", None),
        ];

        let url = format!("{}/{}/obj", CTX.endpoint(), bucket);
        for (origin, expected_origin) in cases {
            let mut resp = agent()
                .get(&url)
                .header("Origin", origin)
                .call()
                .expect("transport error");
            let _ = resp.body_mut().read_to_vec();

            assert_eq!(resp.status().as_u16(), 200, "origin={origin}");
            assert_eq!(
                resp.headers()
                    .get("Access-Control-Allow-Origin")
                    .map(|h| h.to_str().unwrap()),
                expected_origin,
                "origin={origin}"
            );
            assert_eq!(
                resp.headers()
                    .get("Access-Control-Allow-Methods")
                    .map(|h| h.to_str().unwrap()),
                expected_origin.map(|_| "GET"),
                "origin={origin}"
            );
        }

        cleanup(&bucket, &["obj"]).await;
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

#[test]
fn test_cors_presigned_get_object_preflight() {
    s3_tests::run(async {
        let bucket =
            setup_public_cors_bucket(vec![simple_rule("http://example.com", &["GET"])]).await;
        let client = CTX.client();
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .acl(ObjectCannedAcl::PublicRead)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let presigned = client
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .presigned(PresigningConfig::expires_in(Duration::from_secs(600)).unwrap())
            .await
            .unwrap();

        let mut resp = agent()
            .options(presigned.uri())
            .header("Origin", "http://example.com")
            .header("Access-Control-Request-Method", "GET")
            .call()
            .expect("transport error");
        let _ = resp.body_mut().read_to_string();

        assert_eq!(resp.status().as_u16(), 200);
        assert_eq!(
            resp.headers()
                .get("Access-Control-Allow-Origin")
                .map(|h| h.to_str().unwrap()),
            Some("http://example.com")
        );
        assert_eq!(
            resp.headers()
                .get("Access-Control-Allow-Methods")
                .map(|h| h.to_str().unwrap()),
            Some("GET")
        );

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_cors_presigned_put_object_preflight() {
    s3_tests::run(async {
        let bucket = setup_public_cors_bucket(vec![CorsRule::builder()
            .allowed_origins("http://example.com")
            .allowed_methods("PUT")
            .allowed_headers("content-type")
            .build()
            .unwrap()])
        .await;
        let client = CTX.client();

        let presigned = client
            .put_object()
            .bucket(&bucket)
            .key("upload")
            .presigned(PresigningConfig::expires_in(Duration::from_secs(600)).unwrap())
            .await
            .unwrap();

        let mut resp = agent()
            .options(presigned.uri())
            .header("Origin", "http://example.com")
            .header("Access-Control-Request-Method", "PUT")
            .header("Access-Control-Request-Headers", "content-type")
            .call()
            .expect("transport error");
        let _ = resp.body_mut().read_to_string();

        assert_eq!(resp.status().as_u16(), 200);
        assert_eq!(
            resp.headers()
                .get("Access-Control-Allow-Origin")
                .map(|h| h.to_str().unwrap()),
            Some("http://example.com")
        );
        assert_eq!(
            resp.headers()
                .get("Access-Control-Allow-Methods")
                .map(|h| h.to_str().unwrap()),
            Some("PUT")
        );
        assert_eq!(
            resp.headers()
                .get("Access-Control-Allow-Headers")
                .map(|h| h.to_str().unwrap()),
            Some("content-type")
        );

        cleanup(&bucket, &["upload"]).await;
    });
}
