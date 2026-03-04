use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CorsConfiguration, CorsRule};
use s3_tests::{unique_bucket, CTX};

/// Build an agent that returns all HTTP responses (including 4xx/5xx) as Ok.
fn agent() -> ureq::Agent {
    ureq::Agent::config_builder()
        .http_status_as_error(false)
        .build()
        .new_agent()
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

        // OPTIONS with Origin but without Access-Control-Request-Method → 400
        let url = format!("{}/{}", CTX.endpoint(), bucket);
        let mut resp = agent()
            .options(&url)
            .header("Origin", "http://example.com")
            .call()
            .expect("transport error");
        let _ = resp.body_mut().read_to_string();

        assert_eq!(resp.status().as_u16(), 400);

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
        use aws_sdk_s3::types::{BucketCannedAcl, ObjectOwnership};
        let client = CTX.client();
        let bucket = unique_bucket();
        client
            .create_bucket()
            .bucket(&bucket)
            .object_ownership(ObjectOwnership::BucketOwnerPreferred)
            .acl(BucketCannedAcl::PublicRead)
            .send()
            .await
            .unwrap();

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
        use aws_sdk_s3::types::{BucketCannedAcl, ObjectOwnership};
        let client = CTX.client();
        let bucket = unique_bucket();
        client
            .create_bucket()
            .bucket(&bucket)
            .object_ownership(ObjectOwnership::BucketOwnerPreferred)
            .acl(BucketCannedAcl::PublicRead)
            .send()
            .await
            .unwrap();

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
