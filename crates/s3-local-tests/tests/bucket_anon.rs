// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use s3_tests::{build_test_agent, TestServer, RT};

fn run_local<F: std::future::Future>(f: F) -> F::Output {
    RT.block_on(f)
}

// Unauthenticated GET / behavior varies across implementations:
//   - Ceph/RGW: returns 200 with an empty bucket list (no owner -> no buckets).
//   - AWS S3:   redirects to https://aws.amazon.com/s3/ instead of returning an API response.
//   - argmin:   returns 403 AccessDenied because ListBuckets requires authentication.
//
// This is local-only because the AWS response is intentionally not an API-compatible
// ListBuckets result, so running it against AWS does not validate useful behavior.
#[test]
fn test_list_buckets_anonymous() {
    run_local(async {
        let server = TestServer::start().await;
        let url = format!("{}/", server.endpoint());
        let mut resp = build_test_agent(
            server.endpoint(),
            server.tls_ca_pem(),
            std::time::Duration::from_secs(5),
        )
        .get(&url)
        .call()
        .expect("transport error");
        let status = resp.status().as_u16();
        let body = resp.body_mut().read_to_string().unwrap();
        assert_eq!(
            status, 403,
            "expected 403 for anon ListBuckets, got {}",
            status
        );
        assert!(
            body.contains("<Code>AccessDenied</Code>"),
            "expected AccessDenied in body: {}",
            body
        );
    });
}
