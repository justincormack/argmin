use std::time::Duration;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CorsConfiguration, CorsRule, ObjectCannedAcl};
use s3_tests::{object_url, presign_url, CTX};

fn agent() -> s3_tests::Agent {
    s3_tests::test_agent()
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

    client
        .get_bucket_cors()
        .bucket(&bucket)
        .send()
        .await
        .unwrap();

    bucket
}

fn simple_rule(origin: &str, methods: &[&str]) -> CorsRule {
    CorsRule::builder()
        .allowed_origins(origin)
        .set_allowed_methods(Some(methods.iter().map(|m| m.to_string()).collect()))
        .build()
        .unwrap()
}

async fn cleanup(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        let _ = client.delete_object().bucket(bucket).key(*key).send().await;
    }
    client.delete_bucket().bucket(bucket).send().await.unwrap();
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

        let presigned = presign_url(
            "GET",
            &object_url(CTX.endpoint(), &bucket, "obj", None),
            Duration::from_secs(600),
            [] as [(&str, &str); 0],
            None,
        );

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
        let presigned = presign_url(
            "PUT",
            &object_url(CTX.endpoint(), &bucket, "upload", None),
            Duration::from_secs(600),
            [] as [(&str, &str); 0],
            None,
        );

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
