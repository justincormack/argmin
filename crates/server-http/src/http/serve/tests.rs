// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::{Shutdown, TcpListener, TcpStream as StdTcpStream};
    use std::panic::{self, AssertUnwindSafe};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use auth::canonical::{
        canonical_headers, canonical_query_string, canonical_request, sha256_hex, string_to_sign,
    };
    use auth::sigv4::derive_signing_key;
    use hyper_util::rt::TokioIo;
    use ring::hmac;
    use server_core::sse::{ManagedWrappingKeyConfig, StaticManagedKeyProvider};
    use storage::{StorageCluster, StorageClusterRouteHandle, StorageClusterRuntimeMapHandle};

    use crate::metadata_blob::MetadataBlob;

    const TEST_ACCESS_KEY: &str = "AKID";
    const TEST_SECRET_KEY: &str = "test-secret";
    const TEST_SSE_S3_WRAPPING_KEY_B64: &str = "YWJjZGVmMDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODk=";

    fn test_storage_route_handle(initial: Arc<StorageCluster>) -> StorageClusterRouteHandle {
        match StorageClusterRuntimeMapHandle::new(Arc::clone(&initial)) {
            Ok(runtime) => runtime.route_handle(),
            Err(storage::StorageClusterRuntimeMapRefreshError::StaticRouteAuthorityRefresh) => {
                StorageClusterRouteHandle::from_static_cluster(initial).unwrap()
            }
            Err(error) => panic!("invalid test storage cluster route authority: {error}"),
        }
    }

    fn test_dynamic_storage_route_handles(
        initial: Arc<StorageCluster>,
    ) -> (StorageClusterRuntimeMapHandle, StorageClusterRouteHandle) {
        let runtime = StorageClusterRuntimeMapHandle::new(initial).unwrap();
        let route = runtime.route_handle();
        (runtime, route)
    }

    fn long_lived_test_route_map_validity() -> storage::RouteMapValidity {
        storage::RouteMapValidity::until_ms(
            storage::clock::current_time_millis().saturating_add(3_600_000),
        )
        .unwrap()
    }

    fn open_dynamic_test_storage_cluster(dir: &std::path::Path) -> Arc<StorageCluster> {
        storage::test_support::open_default_test_storage_cluster(dir)
            .test_clone_with_dynamic_route_map_validity(long_lived_test_route_map_validity())
            .expect("build dynamic test storage cluster")
    }

    struct ServerGuard(tokio::task::JoinHandle<()>);

    impl Drop for ServerGuard {
        fn drop(&mut self) {
            self.0.abort();
        }
    }

    #[test]
    fn internal_error_response_is_500_internal_error() {
        let resp = internal_error_response(&WireResponseIds::new("request-id", "host-id"));

        assert_eq!(resp.status_code, 500);
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("<Code>InternalError</Code>"));
        assert!(
            body.contains("<Message>We encountered an internal error. Please try again.</Message>")
        );
    }

    #[test]
    fn segment_buffer_pool_recovers_from_poisoned_lock() {
        static PANIC_HOOK_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _panic_hook_guard = PANIC_HOOK_LOCK
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        let previous_hook = panic::take_hook();
        panic::set_hook(Box::new(|_| {}));

        let pool = SegmentBufferPool::new(4);
        let poison_result = panic::catch_unwind(AssertUnwindSafe(|| {
            let _guard = pool.cached.lock().unwrap();
            panic!("poison segment buffer pool");
        }));
        panic::set_hook(previous_hook);
        assert!(poison_result.is_err());

        let mut buf = pool.checkout();
        buf.extend_from_slice(b"data");
        pool.recycle(buf);

        let recycled = pool.checkout();
        assert!(recycled.capacity() >= crate::coordinator::INTERNAL_SEGMENT_SIZE);
        assert!(recycled.is_empty());
    }

    #[test]
    fn buffered_body_limits_use_operation_specific_caps() {
        let parts = make_parts("PUT", "/bucket?encryption", &[]);
        assert_eq!(
            buffered_body_limit_for_request_parts(EndpointKind::S3Only, &parts),
            MAX_BUCKET_ENCRYPTION_CONFIGURATION_BYTES
        );

        let parts = make_parts("POST", "/bucket/key?uploadId=upload-id", &[]);
        assert_eq!(
            buffered_body_limit_for_request_parts(EndpointKind::S3Only, &parts),
            MAX_COMPLETE_MULTIPART_UPLOAD_XML_BYTES
        );

        let parts = make_parts("GET", "/bucket/key", &[]);
        assert_eq!(
            buffered_body_limit_for_request_parts(EndpointKind::S3Only, &parts),
            MAX_BUFFERED_CONTROL_BODY_SIZE
        );
    }

    #[test]
    fn streaming_body_frame_timeout_uses_captured_route_deadline() {
        let tmp = test_util::tempdir();
        let cluster = open_dynamic_test_storage_cluster(tmp.path());
        let handle = test_storage_route_handle(Arc::clone(&cluster));
        let admission = storage::clock::with_time_override(1_000, || {
            cluster
                .test_store_route_map_validity(storage::RouteMapValidity::until_ms(5_000).unwrap());
            handle.admit_current_route().unwrap()
        });

        storage::clock::with_time_override(2_000, || {
            assert_eq!(
                route_bounded_body_frame_timeout(Some(&admission), Duration::from_secs(30))
                    .unwrap(),
                Duration::from_secs(3)
            );
        });
        storage::clock::with_time_override(5_000, || {
            assert!(matches!(
                route_bounded_body_frame_timeout(Some(&admission), Duration::from_secs(30)),
                Err(ServerError::SlowDown)
            ));
        });
    }

    /// Build a minimal `http::request::Parts` for testing `is_streaming_write`.
    fn make_parts(method: &str, uri: &str, headers: &[(&str, &str)]) -> http::request::Parts {
        let mut builder = http::Request::builder().method(method).uri(uri);
        for (k, v) in headers {
            builder = builder.header(*k, *v);
        }
        let (parts, _body) = builder.body(()).unwrap().into_parts();
        parts
    }

    fn make_s3req(method: &str, uri: &str, headers: &[(&str, &str)]) -> S3Request {
        S3Request::from_hyper_headers(make_parts(method, uri, headers), TransportSecurity::Tls, 0)
            .unwrap()
    }

    fn setup_frontend(dir: &std::path::Path) -> Arc<HttpFrontend> {
        let storage_cluster = storage::test_support::open_default_test_storage_cluster(dir);
        let storage_handle = test_storage_route_handle(Arc::clone(&storage_cluster));
        setup_frontend_with_storage_handle(storage_handle)
    }

    fn setup_dynamic_frontend(dir: &std::path::Path) -> Arc<HttpFrontend> {
        let storage_cluster = open_dynamic_test_storage_cluster(dir);
        let storage_handle = test_storage_route_handle(Arc::clone(&storage_cluster));
        setup_frontend_with_storage_handle(storage_handle)
    }

    fn setup_frontend_with_storage_handle(
        storage_handle: storage::StorageClusterRouteHandle,
    ) -> Arc<HttpFrontend> {
        let test_storage_cluster = storage_handle.current();
        let sse_s3_provider = StaticManagedKeyProvider::single(
            ManagedWrappingKeyConfig::from_base64(1, TEST_SSE_S3_WRAPPING_KEY_B64).unwrap(),
        );
        let coordinator = server_core::coordinator::Coordinator::new_with_managed_key_provider_for_storage_cluster_route_handle_with_background_worker_mode(
                storage_handle,
                "us-east-1".to_string(),
                None,
                sse_s3_provider,
                server_core::coordinator::BackgroundWorkerMode::none(),
            )
            .unwrap();
        let mut credentials = auth::CredentialStore::new();
        credentials
            .add(
                TEST_ACCESS_KEY.to_string(),
                auth::SecretKey::new(TEST_SECRET_KEY.to_string()),
            )
            .unwrap();
        Arc::new(HttpFrontend {
            coordinator: Arc::new(coordinator),
            identity_provider: auth::IdentityProvider::in_memory(credentials)
                .expect("initialize session-token key ring"),
            host_id: Arc::<str>::from("host-id"),
            test_storage_cluster,
            actual_cors_metadata_lookup_count: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    fn create_test_bucket(frontend: &HttpFrontend, bucket: &str) {
        let requester = server_core::coordinator::test_helpers::requester(TEST_ACCESS_KEY);
        frontend
            .coordinator
            .create_bucket(&crate::coordinator::CreateBucketRequest {
                name: storage::BucketName::try_from(bucket.to_string()).unwrap(),
                requester,
                namespace: s3_types::BucketNamespace::Global,
                acl: crate::coordinator::CreateBucketAcl::DefaultPrivate,
                ownership: crate::coordinator::BucketObjectOwnership::ObjectWriter,
                object_lock_enabled: false,
            })
            .unwrap();
    }

    fn object_request(bucket: &str, key: &str) -> crate::coordinator::ObjectRequest<'static> {
        crate::coordinator::ObjectRequest::new(
            storage::BucketName::try_from(bucket.to_string()).unwrap(),
            storage::ObjectKey::try_from(key.to_string()).unwrap(),
            server_core::coordinator::test_helpers::requester(TEST_ACCESS_KEY),
            None,
        )
    }

    fn bucket_request(bucket: &str) -> crate::coordinator::BucketRequest<'static> {
        crate::coordinator::BucketRequest::new(
            storage::BucketName::try_from(bucket.to_string()).unwrap(),
            server_core::coordinator::test_helpers::requester(TEST_ACCESS_KEY),
            None,
        )
    }

    fn put_test_object(frontend: &HttpFrontend, bucket: &str, key: &str, data: &'static [u8]) {
        frontend
            .coordinator
            .put_object(&crate::coordinator::PutObjectRequest {
                object: object_request(bucket, key),
                data,
                metadata: &MetadataBlob::new(),
                system_metadata: &server_core::system_metadata::SystemMetadata::EMPTY,
                tags: None,
                cond: &crate::conditional::WriteCondition::default(),
                acl: crate::coordinator::PutObjectAcl::None.into(),
                policy_context: crate::coordinator::PutObjectPolicyContext::default(),
                object_lock: s3_types::ObjectLockState::default(),
                encryption: crate::coordinator::WriteEncryptionRequest::none(),
            })
            .unwrap();
    }

    fn create_test_bucket_and_upload(frontend: &HttpFrontend, bucket: &str, key: &str) -> String {
        create_test_bucket(frontend, bucket);
        let requester = server_core::coordinator::test_helpers::requester(TEST_ACCESS_KEY);
        frontend
            .coordinator
            .create_multipart_upload(&crate::coordinator::CreateMultipartUploadRequest {
                object: crate::coordinator::ObjectRequest::new(
                    storage::BucketName::try_from(bucket.to_string()).unwrap(),
                    storage::ObjectKey::try_from(key.to_string()).unwrap(),
                    requester,
                    None,
                ),
                metadata: &MetadataBlob::new(),
                system_metadata: &server_core::system_metadata::SystemMetadata::EMPTY,
                tags: None,
                checksum: None,
                acl: crate::coordinator::PutObjectAcl::None.into(),
                policy_context: crate::coordinator::PutObjectPolicyContext::default(),
                object_lock: s3_types::ObjectLockState::default(),
                encryption: crate::coordinator::WriteEncryptionRequest::none(),
            })
            .unwrap()
            .upload_id
            .to_string()
    }

    async fn start_test_server(frontend: Arc<HttpFrontend>) -> (String, ServerGuard) {
        start_test_server_with_config(frontend, ServeConfig::default(), 8).await
    }

    async fn start_test_server_with_config(
        frontend: Arc<HttpFrontend>,
        config: ServeConfig,
        request_slots: usize,
    ) -> (String, ServerGuard) {
        start_test_server_with_bind_addr(frontend, config, request_slots, "127.0.0.1:0").await
    }

    async fn start_test_server_with_bind_addr(
        frontend: Arc<HttpFrontend>,
        config: ServeConfig,
        request_slots: usize,
        bind_addr: &str,
    ) -> (String, ServerGuard) {
        let std_listener = TcpListener::bind(bind_addr).expect("bind");
        let addr = std_listener.local_addr().unwrap().to_string();
        std_listener.set_nonblocking(true).unwrap();
        let listener = tokio::net::TcpListener::from_std(std_listener).unwrap();

        let header_read_timeout = config.header_read_timeout;
        let host_id = frontend.host_id.clone();
        let segment_buffer_pool_slots = request_slots.max(1);
        let state = Arc::new(ServerState {
            pool: vec![frontend],
            host_id,
            counter: AtomicUsize::new(0),
            request_semaphore: Arc::new(tokio::sync::Semaphore::new(request_slots)),
            _request_admission_capacity_guard: observability::request_admission_capacity_guard(
                u64::try_from(request_slots).unwrap_or(u64::MAX),
            ),
            segment_buffer_pool: SegmentBufferPool::new(segment_buffer_pool_slots),
            config,
            endpoint_kind: EndpointKind::S3Only,
        });

        let handle = tokio::spawn(async move {
            loop {
                let (stream, addr) = listener.accept().await.expect("accept");
                let state = Arc::clone(&state);
                tokio::spawn(async move {
                    serve_connection(
                        state,
                        TokioIo::new(stream),
                        header_read_timeout,
                        TransportSecurity::InsecureHttp,
                        None,
                        Some(canonical_source_ip(addr.ip())),
                    )
                    .await;
                });
            }
        });

        (addr, ServerGuard(handle))
    }

    #[test]
    fn canonical_source_ip_maps_ipv4_mapped_ipv6_to_ipv4() {
        assert_eq!(
            canonical_source_ip("::ffff:127.0.0.1".parse().unwrap()),
            "127.0.0.1".parse::<IpAddr>().unwrap()
        );
        assert_eq!(
            canonical_source_ip("2001:db8::1".parse().unwrap()),
            "2001:db8::1".parse::<IpAddr>().unwrap()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ipv4_client_on_ipv6_wildcard_listener_matches_ipv4_source_ip_policy() {
        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        create_test_bucket(&frontend, "source-ip-bucket");
        put_test_object(
            &frontend,
            "source-ip-bucket",
            "source-ip-key",
            b"source-ip-body",
        );
        frontend
            .coordinator
            .put_bucket_policy(&crate::coordinator::PutBucketPolicyRequest {
                bucket: bucket_request("source-ip-bucket"),
                config: r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::source-ip-bucket/*","Condition":{"IpAddress":{"aws:SourceIp":"127.0.0.0/8"}}}]}"#,
                confirm_remove_self_bucket_access: false,
            })
            .unwrap();

        let (addr, _guard) =
            start_test_server_with_bind_addr(frontend, ServeConfig::default(), 8, "[::]:0").await;
        let port = addr
            .rsplit_once(':')
            .expect("IPv6 listener address should include port")
            .1;
        let mut stream = StdTcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
        stream
            .write_all(
                concat!(
                    "GET /source-ip-bucket/source-ip-key HTTP/1.1\r\n",
                    "Host: source-ip-bucket\r\n",
                    "Connection: close\r\n",
                    "\r\n",
                )
                .as_bytes(),
            )
            .unwrap();
        let response = read_http_response(&mut stream, Duration::from_secs(3));

        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert!(response.ends_with("source-ip-body"), "{response}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn request_policy_time_is_captured_before_admission_and_body_wait() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let clock = storage::test_support::test_time_override_guard(1_000);
        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        create_test_bucket(&frontend, "time-admission-bucket");
        put_test_object(&frontend, "time-admission-bucket", "time-key", b"time-body");
        frontend
            .coordinator
            .put_bucket_policy(&crate::coordinator::PutBucketPolicyRequest {
                bucket: bucket_request("time-admission-bucket"),
                config: r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::time-admission-bucket/*","Condition":{"DateEquals":{"aws:CurrentTime":"1970-01-01T00:00:01Z"}}}]}"#,
                confirm_remove_self_bucket_access: false,
            })
            .unwrap();

        let config = ServeConfig {
            body_idle_timeout: Duration::from_secs(5),
            ..ServeConfig::default()
        };
        let (addr, _guard) = start_test_server_with_config(frontend, config, 1).await;
        let inflight_before = observability::metrics_snapshot().inflight_requests;

        let mut holder = tokio::net::TcpStream::connect(&addr).await.unwrap();
        holder
            .write_all(
                concat!(
                    "GET /hold-bucket/hold-key HTTP/1.1\r\n",
                    "Host: hold-bucket\r\n",
                    "Content-Length: 4\r\n",
                    "Connection: close\r\n",
                    "\r\n",
                )
                .as_bytes(),
            )
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(1), async {
            while observability::metrics_snapshot().inflight_requests <= inflight_before {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("body collection must retain the admitted foreground-request signal");

        let mut delayed = tokio::net::TcpStream::connect(&addr).await.unwrap();
        delayed
            .write_all(
                concat!(
                    "GET /time-admission-bucket/time-key HTTP/1.1\r\n",
                    "Host: time-admission-bucket\r\n",
                    "Content-Length: 4\r\n",
                    "Connection: close\r\n",
                    "\r\n",
                )
                .as_bytes(),
            )
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;
        clock.set(2_000);
        drop(holder);
        tokio::time::sleep(Duration::from_millis(50)).await;
        delayed.write_all(b"body").await.unwrap();

        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(3), delayed.read_to_end(&mut response))
            .await
            .expect("response should arrive")
            .unwrap();
        let response = String::from_utf8_lossy(&response);
        assert!(
            response.starts_with("HTTP/1.1 200"),
            "request should use admission timestamp captured before clock advance: {response}"
        );
        assert!(response.ends_with("time-body"), "{response}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn buffered_body_absolute_deadline_does_not_reset_on_steady_chunks() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        let config = ServeConfig {
            body_idle_timeout: Duration::from_millis(200),
            pre_auth_body_timeout: Duration::from_millis(80),
            ..ServeConfig::default()
        };
        let (addr, _guard) = start_test_server_with_config(frontend, config, 8).await;
        let mut client = tokio::net::TcpStream::connect(&addr).await.unwrap();
        client
            .write_all(
                format!(
                    "POST /deadline-bucket?delete HTTP/1.1\r\n\
Host: {addr}\r\n\
Transfer-Encoding: chunked\r\n\
Connection: close\r\n\r\n\
1\r\na\r\n"
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        client.write_all(b"1\r\nb\r\n").await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        client.write_all(b"1\r\nc\r\n").await.unwrap();

        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(1), async {
            let mut chunk = [0u8; 4096];
            loop {
                let read = client.read(&mut chunk).await.unwrap();
                assert!(read > 0, "response ended before its body was complete");
                response.extend_from_slice(&chunk[..read]);
                let text = String::from_utf8_lossy(&response);
                if let Some(header_end) = text.find("\r\n\r\n") {
                    let headers = &text[..header_end];
                    if response_body_complete(&response, header_end, headers) {
                        break;
                    }
                }
            }
        })
        .await
        .expect("absolute pre-authentication body deadline should respond");

        let response = String::from_utf8_lossy(&response);
        assert!(response.starts_with("HTTP/1.1 400"), "{response}");
        assert!(
            response.contains("request body authentication deadline expired"),
            "{response}"
        );
    }

    #[test]
    fn pre_auth_body_frame_deadline_preserves_absolute_cap() {
        let absolute_deadline = TokioInstant::now() + Duration::from_secs(1);
        assert_eq!(
            pre_auth_body_frame_deadline(Duration::from_secs(30), absolute_deadline).unwrap(),
            absolute_deadline
        );
    }

    #[tokio::test(start_paused = true)]
    async fn pre_auth_body_frame_expiry_wins_over_a_ready_frame() {
        let absolute_deadline = TokioInstant::now() + Duration::from_secs(1);
        let frame_consumed = std::cell::Cell::new(false);
        let frame = async {
            tokio::time::advance(Duration::from_secs(2)).await;
            frame_consumed.set(true);
            b"ready frame"
        };

        let error = await_pre_auth_body_frame(frame, Duration::from_secs(30), absolute_deadline)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ServerError::InvalidRequest { reason }
                if reason == "request body authentication deadline expired"
        ));
        assert!(
            !frame_consumed.get(),
            "the timer must win before the expired ready frame is consumed"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn buffered_invalid_signature_precedes_expired_route_admission() {
        let tmp = test_util::tempdir();
        let storage_cluster = open_dynamic_test_storage_cluster(tmp.path());
        let storage_handle = test_storage_route_handle(Arc::clone(&storage_cluster));
        let frontend = setup_frontend_with_storage_handle(storage_handle);
        create_test_bucket(&frontend, "mybucket");
        frontend
            .coordinator
            .put_bucket_cors(&crate::coordinator::PutBucketConfigRequest {
                bucket: bucket_request("mybucket"),
                config: "<CORSConfiguration><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>",
            })
            .unwrap();
        let (addr, _guard) = start_test_server(Arc::clone(&frontend)).await;
        storage_cluster.test_store_route_map_validity(
            storage::RouteMapValidity::until_ms(storage::clock::current_time_millis()).unwrap(),
        );
        assert!(matches!(
            frontend.coordinator.admit_storage_route_for_request(),
            Err(ServerError::SlowDown)
        ));

        let signed = sign_headers("GET", "/mybucket/key", &addr, b"", &[]);
        let mut invalid_authorization = signed.authorization;
        let replacement = if invalid_authorization.ends_with('0') {
            "1"
        } else {
            "0"
        };
        invalid_authorization.replace_range(invalid_authorization.len() - 1.., replacement);
        let request = format!(
            "GET /mybucket/key HTTP/1.1\r\n\
Host: {addr}\r\n\
Authorization: {invalid_authorization}\r\n\
x-amz-date: {}\r\n\
x-amz-content-sha256: {}\r\n\
Origin: https://example.com\r\n\
Connection: close\r\n\r\n",
            signed.amz_date, signed.amz_content_sha256
        );
        let response = send_raw_http_request(&addr, &request, b"");

        assert!(
            response.starts_with("HTTP/1.1 403"),
            "expected SignatureDoesNotMatch rather than route admission failure: {response}"
        );
        assert!(
            response.contains("<Code>SignatureDoesNotMatch</Code>"),
            "{response}"
        );
        assert!(
            !response.contains("<Code>OperationAborted</Code>"),
            "{response}"
        );
        assert!(
            !response
                .to_ascii_lowercase()
                .contains("access-control-allow-origin"),
            "expired route admission must suppress CORS enrichment: {response}"
        );
        assert_eq!(
            frontend.test_actual_cors_metadata_lookup_count(),
            0,
            "expired route admission must prevent the CORS metadata lookup"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn in_flight_streaming_put_blocks_runtime_map_publication_until_cleanup_finishes() {
        use tokio::io::AsyncWriteExt;

        let tmp = test_util::tempdir();
        let initial = open_dynamic_test_storage_cluster(&tmp.path().join("initial"));
        let (runtime_handle, storage_handle) =
            test_dynamic_storage_route_handles(Arc::clone(&initial));
        let frontend = setup_frontend_with_storage_handle(storage_handle.clone());
        create_test_bucket(&frontend, "route-admission-bucket");
        let config = ServeConfig {
            body_idle_timeout: Duration::from_secs(60),
            ..ServeConfig::default()
        };
        let (addr, _guard) = start_test_server_with_config(frontend, config, 1).await;

        let payload = b"route-admission-body";
        let signed = sign_streaming_headers(
            "PUT",
            "/route-admission-bucket/key",
            &addr,
            payload.len(),
            &[],
        );
        let wire = build_signed_chunked_body(&signed, payload);
        let request = format!(
            "PUT /route-admission-bucket/key HTTP/1.1\r\n\
Host: {addr}\r\n\
Authorization: {}\r\n\
x-amz-date: {}\r\n\
x-amz-content-sha256: {}\r\n\
content-encoding: aws-chunked\r\n\
x-amz-decoded-content-length: {}\r\n\
Content-Length: {}\r\n\
Connection: close\r\n\r\n",
            signed.authorization,
            signed.amz_date,
            signed.amz_content_sha256,
            payload.len(),
            wire.len()
        );
        let mut client = tokio::net::TcpStream::connect(&addr).await.unwrap();
        client.write_all(request.as_bytes()).await.unwrap();
        client.write_all(&wire[..1]).await.unwrap();
        client.flush().await.unwrap();

        let admitted_handle = storage_handle.clone();
        tokio::task::spawn_blocking(move || {
            admitted_handle.test_wait_until_route_request_is_admitted();
        })
        .await
        .unwrap();

        let candidate = open_dynamic_test_storage_cluster(&tmp.path().join("candidate"));
        let install_handle = runtime_handle.clone();
        let installed_candidate = Arc::clone(&candidate);
        let installer = std::thread::spawn(move || {
            install_handle.install(installed_candidate).unwrap();
        });

        let pending_handle = storage_handle.clone();
        tokio::task::spawn_blocking(move || {
            pending_handle.test_wait_until_route_publication_is_pending();
        })
        .await
        .unwrap();
        assert!(Arc::ptr_eq(&storage_handle.current(), &initial));

        drop(client);
        tokio::time::timeout(
            Duration::from_secs(3),
            tokio::task::spawn_blocking(move || installer.join().unwrap()),
        )
        .await
        .expect("runtime-map publication should finish after the request disconnects")
        .unwrap();
        assert!(Arc::ptr_eq(&storage_handle.current(), &candidate));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn expired_streaming_put_route_releases_publication_before_client_disconnects() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        const PROGRESS_TIMEOUT: Duration = Duration::from_secs(30);

        let tmp = test_util::tempdir();
        let initial = open_dynamic_test_storage_cluster(&tmp.path().join("initial"));
        let (runtime_handle, storage_handle) =
            test_dynamic_storage_route_handles(Arc::clone(&initial));
        let frontend = setup_frontend_with_storage_handle(storage_handle.clone());
        create_test_bucket(&frontend, "route-expiry-bucket");
        initial.test_store_route_map_validity(
            storage::RouteMapValidity::until_ms(
                storage::clock::current_time_millis().saturating_add(3_600_000),
            )
            .unwrap(),
        );
        let config = ServeConfig {
            body_idle_timeout: Duration::from_secs(60),
            ..ServeConfig::default()
        };
        let (addr, _guard) = start_test_server_with_config(frontend, config, 1).await;

        let payload = vec![b'r'; crate::coordinator::INTERNAL_SEGMENT_SIZE + 1024];
        let signed = sign_headers("PUT", "/route-expiry-bucket/key", &addr, &payload, &[]);
        let request = format!(
            "PUT /route-expiry-bucket/key HTTP/1.1\r\n\
Host: {addr}\r\n\
Authorization: {}\r\n\
x-amz-date: {}\r\n\
x-amz-content-sha256: {}\r\n\
Content-Length: {}\r\n\
\r\n",
            signed.authorization,
            signed.amz_date,
            signed.amz_content_sha256,
            payload.len()
        );
        let mut client = tokio::net::TcpStream::connect(&addr).await.unwrap();
        client.write_all(request.as_bytes()).await.unwrap();
        client
            .write_all(&payload[..crate::coordinator::INTERNAL_SEGMENT_SIZE + 1])
            .await
            .unwrap();
        client.flush().await.unwrap();

        let admitted_handle = storage_handle.clone();
        tokio::task::spawn_blocking(move || {
            admitted_handle.test_wait_until_route_request_is_admitted();
        })
        .await
        .unwrap();

        let bucket = storage::BucketName::try_from("route-expiry-bucket".to_string()).unwrap();
        let key = storage::ObjectKey::try_from("key".to_string()).unwrap();
        let session_id = tokio::time::timeout(PROGRESS_TIMEOUT, async {
            loop {
                let session_ids = storage::test_support::stream_upload_session_ids_for_object(
                    &initial, &bucket, &key,
                )
                .unwrap();
                if let [session_id] = session_ids.as_slice() {
                    if initial
                        .test_capture_stream_upload_payload(&bucket, &key, session_id)
                        .is_ok_and(|payload| !payload.is_empty())
                    {
                        break session_id.clone();
                    }
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("streaming PUT must promote and publish a staged segment before expiry");
        let staged_payload = initial
            .test_capture_stream_upload_payload(&bucket, &key, &session_id)
            .unwrap();
        assert!(!staged_payload.is_empty());
        assert!(initial
            .test_stream_upload_payload_snapshot_is_fully_present(&staged_payload)
            .unwrap());
        assert!(initial
            .test_stream_upload_reservation_exists(&bucket, &key, &session_id)
            .unwrap());

        let candidate = open_dynamic_test_storage_cluster(&tmp.path().join("candidate"));
        let install_handle = runtime_handle.clone();
        let installed_candidate = Arc::clone(&candidate);
        let installer = std::thread::spawn(move || {
            install_handle.install(installed_candidate).unwrap();
        });

        let pending_handle = storage_handle.clone();
        tokio::task::spawn_blocking(move || {
            pending_handle.test_wait_until_route_publication_is_pending();
        })
        .await
        .unwrap();
        initial.test_store_route_map_validity(
            storage::RouteMapValidity::until_ms(storage::clock::current_time_millis()).unwrap(),
        );
        client
            .write_all(&payload[crate::coordinator::INTERNAL_SEGMENT_SIZE + 1..][..1])
            .await
            .unwrap();
        client.flush().await.unwrap();

        tokio::time::timeout(
            PROGRESS_TIMEOUT,
            tokio::task::spawn_blocking(move || installer.join().unwrap()),
        )
        .await
        .expect("route expiry should release publication while the client remains connected")
        .unwrap();
        assert!(Arc::ptr_eq(&storage_handle.current(), &candidate));
        initial.test_store_route_map_validity(long_lived_test_route_map_validity());
        assert!(!storage::test_support::stream_upload_session_exists(
            &initial,
            &bucket,
            &key,
            &session_id,
        )
        .unwrap());
        assert!(!initial
            .test_stream_upload_reservation_exists(&bucket, &key, &session_id)
            .unwrap());
        assert!(initial
            .test_stream_upload_payload_snapshot_is_fully_absent(&staged_payload)
            .unwrap());

        let mut response = Vec::new();
        tokio::time::timeout(PROGRESS_TIMEOUT, client.read_to_end(&mut response))
            .await
            .expect("expired streaming request should receive a response")
            .unwrap();
        let response = String::from_utf8_lossy(&response);
        assert!(response.starts_with("HTTP/1.1 503"), "{response}");
        assert!(
            response.to_ascii_lowercase().contains("connection: close"),
            "{response}"
        );
        assert!(response.contains("<Code>SlowDown</Code>"), "{response}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn completed_chunked_streaming_put_keeps_connection_reusable() {
        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        create_test_bucket(&frontend, "chunked-keepalive-bucket");
        let (addr, _guard) = start_test_server(frontend).await;

        let payload = b"complete chunked request";
        let signed = sign_streaming_headers(
            "PUT",
            "/chunked-keepalive-bucket/key",
            &addr,
            payload.len(),
            &[],
        );
        let wire = build_signed_chunked_body(&signed, payload);
        let put_request = format!(
            "PUT /chunked-keepalive-bucket/key HTTP/1.1\r\n\
Host: {addr}\r\n\
Authorization: {}\r\n\
x-amz-date: {}\r\n\
x-amz-content-sha256: {}\r\n\
content-encoding: aws-chunked\r\n\
x-amz-decoded-content-length: {}\r\n\
Transfer-Encoding: chunked\r\n\r\n",
            signed.authorization,
            signed.amz_date,
            signed.amz_content_sha256,
            payload.len()
        );

        tokio::task::spawn_blocking(move || {
            let mut client = StdTcpStream::connect(&addr).unwrap();
            client.write_all(put_request.as_bytes()).unwrap();
            write!(client, "{:x}\r\n", wire.len()).unwrap();
            client.write_all(&wire).unwrap();
            client.write_all(b"\r\n0\r\n\r\n").unwrap();
            client.flush().unwrap();

            let put_response = read_http_response(&mut client, Duration::from_secs(3));
            assert!(put_response.starts_with("HTTP/1.1 200"), "{put_response}");
            assert!(
                !put_response
                    .to_ascii_lowercase()
                    .contains("connection: close"),
                "fully consumed chunked request must preserve keep-alive: {put_response}"
            );

            let signed_get = sign_headers(
                "GET",
                "/chunked-keepalive-bucket/key",
                &addr,
                b"",
                &[],
            );
            let get_request = format!(
                "GET /chunked-keepalive-bucket/key HTTP/1.1\r\n\
Host: {addr}\r\n\
Authorization: {}\r\n\
x-amz-date: {}\r\n\
x-amz-content-sha256: {}\r\n\
Connection: close\r\n\r\n",
                signed_get.authorization, signed_get.amz_date, signed_get.amz_content_sha256,
            );
            client.write_all(get_request.as_bytes()).unwrap();
            client.flush().unwrap();

            let get_response = read_http_response(&mut client, Duration::from_secs(3));
            assert!(get_response.starts_with("HTTP/1.1 200"), "{get_response}");
            assert!(
                get_response.as_bytes().ends_with(payload),
                "second request on the keep-alive connection returned the wrong body: {get_response}"
            );
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn post_and_upload_part_acquire_cleanup_authority_before_creating_sessions() {
        let tmp = test_util::tempdir();
        let initial = open_dynamic_test_storage_cluster(&tmp.path().join("initial"));
        let storage_handle = test_storage_route_handle(Arc::clone(&initial));
        let frontend = setup_frontend_with_storage_handle(storage_handle);
        create_test_bucket(&frontend, "post-cleanup-authority-bucket");
        initial.test_store_route_map_validity(
            storage::RouteMapValidity::until_ms(
                storage::clock::current_time_millis().saturating_add(3_600_000),
            )
            .unwrap(),
        );

        let expire = Arc::clone(&initial);
        let hook = initial.test_install_before_retained_stream_cleanup_capability_hook(Arc::new(
            move || {
                expire.test_store_route_map_validity(
                    storage::RouteMapValidity::until_ms(storage::clock::current_time_millis())
                        .unwrap(),
                );
            },
        ));
        let request = make_s3req(
            "POST",
            "/post-cleanup-authority-bucket",
            &[("host", "localhost")],
        );
        let fields = sign_post_policy_fields("post-cleanup-authority-bucket", "key", &[], &[]);
        assert!(matches!(
            frontend.prepare_streaming_post_object(
                &request,
                "post-cleanup-authority-bucket",
                &fields,
                Some("upload.txt"),
            ),
            Err(ServerError::SlowDown)
        ));
        drop(hook);
        assert_eq!(
            storage::test_support::stream_upload_session_count(&initial).unwrap(),
            0
        );

        initial.test_store_route_map_validity(
            storage::RouteMapValidity::until_ms(
                storage::clock::current_time_millis().saturating_add(3_600_000),
            )
            .unwrap(),
        );
        let upload_id =
            create_test_bucket_and_upload(&frontend, "part-cleanup-authority-bucket", "key");
        let expire = Arc::clone(&initial);
        let hook = initial.test_install_before_retained_stream_cleanup_capability_hook(Arc::new(
            move || {
                expire.test_store_route_map_validity(
                    storage::RouteMapValidity::until_ms(storage::clock::current_time_millis())
                        .unwrap(),
                );
            },
        ));
        let uri = format!("/part-cleanup-authority-bucket/key?partNumber=1&uploadId={upload_id}");
        let signed = sign_headers("PUT", &uri, "localhost", b"", &[]);
        let request = make_s3req(
            "PUT",
            &uri,
            &[
                ("host", "localhost"),
                ("authorization", &signed.authorization),
                ("x-amz-date", &signed.amz_date),
                ("x-amz-content-sha256", &signed.amz_content_sha256),
            ],
        );
        assert!(matches!(
            frontend.prepare_streaming_part(
                &request,
                "part-cleanup-authority-bucket",
                "key",
                &upload_id,
                "1",
            ),
            Err(ServerError::SlowDown)
        ));
        drop(hook);
        assert_eq!(
            storage::test_support::stream_upload_session_count(&initial).unwrap(),
            0
        );
    }

    #[test]
    fn captured_admission_guards_put_post_and_upload_part_initial_mutations() {
        let clock = storage::test_support::test_time_override_guard(1_000);
        let tmp = test_util::tempdir();
        let frontend = setup_dynamic_frontend(tmp.path());
        let storage_cluster = frontend.coordinator.storage_node_for_request();

        create_test_bucket(&frontend, "initial-mutation-post-bucket");
        storage_cluster
            .test_store_route_map_validity(storage::RouteMapValidity::until_ms(2_000).unwrap());
        let renewed = Arc::clone(&storage_cluster);
        let hook_clock = clock.control();
        let hook = storage_cluster.test_install_after_retained_stream_cleanup_capability_hook(
            Arc::new(move || {
                renewed.test_store_route_map_validity(
                    storage::RouteMapValidity::until_ms(5_000).unwrap(),
                );
                hook_clock.set(2_000);
            }),
        );
        let post_request = make_s3req(
            "POST",
            "/initial-mutation-post-bucket",
            &[("host", "localhost")],
        );
        let fields = sign_post_policy_fields("initial-mutation-post-bucket", "key", &[], &[]);
        assert!(matches!(
            frontend.prepare_streaming_post_object(
                &post_request,
                "initial-mutation-post-bucket",
                &fields,
                Some("key"),
            ),
            Err(ServerError::SlowDown)
        ));
        assert!(frontend
            .coordinator
            .admit_storage_route_for_request()
            .is_ok());
        drop(hook);
        assert_eq!(
            storage::test_support::stream_upload_session_count(&storage_cluster).unwrap(),
            0
        );

        clock.set(1_000);
        create_test_bucket(&frontend, "initial-mutation-put-bucket");
        storage_cluster
            .test_store_route_map_validity(storage::RouteMapValidity::until_ms(2_000).unwrap());
        let renewed = Arc::clone(&storage_cluster);
        let hook_clock = clock.control();
        let hook = storage_cluster.test_install_after_retained_stream_cleanup_capability_hook(
            Arc::new(move || {
                renewed.test_store_route_map_validity(
                    storage::RouteMapValidity::until_ms(5_000).unwrap(),
                );
                hook_clock.set(2_000);
            }),
        );
        let put_body = b"body";
        let signed = sign_headers(
            "PUT",
            "/initial-mutation-put-bucket/key",
            "localhost",
            put_body,
            &[],
        );
        let content_length = put_body.len().to_string();
        let put_request = make_s3req(
            "PUT",
            "/initial-mutation-put-bucket/key",
            &[
                ("host", "localhost"),
                ("authorization", &signed.authorization),
                ("x-amz-date", &signed.amz_date),
                ("x-amz-content-sha256", &signed.amz_content_sha256),
                ("content-length", &content_length),
            ],
        );
        assert!(matches!(
            frontend.prepare_streaming_put(
                &put_request,
                "initial-mutation-put-bucket",
                "key",
                false,
            ),
            Err(ServerError::SlowDown)
        ));
        assert!(frontend
            .coordinator
            .admit_storage_route_for_request()
            .is_ok());
        drop(hook);
        assert_eq!(
            storage::test_support::stream_upload_session_count(&storage_cluster).unwrap(),
            0
        );

        clock.set(1_000);
        storage_cluster
            .test_store_route_map_validity(storage::RouteMapValidity::until_ms(5_000).unwrap());
        let upload_id =
            create_test_bucket_and_upload(&frontend, "initial-mutation-part-bucket", "key");
        storage_cluster
            .test_store_route_map_validity(storage::RouteMapValidity::until_ms(2_000).unwrap());
        let renewed = Arc::clone(&storage_cluster);
        let hook_clock = clock.control();
        let hook = storage_cluster.test_install_after_retained_stream_cleanup_capability_hook(
            Arc::new(move || {
                renewed.test_store_route_map_validity(
                    storage::RouteMapValidity::until_ms(5_000).unwrap(),
                );
                hook_clock.set(2_000);
            }),
        );
        let uri = format!("/initial-mutation-part-bucket/key?partNumber=1&uploadId={upload_id}");
        let signed = sign_headers("PUT", &uri, "localhost", b"", &[]);
        let part_request = make_s3req(
            "PUT",
            &uri,
            &[
                ("host", "localhost"),
                ("authorization", &signed.authorization),
                ("x-amz-date", &signed.amz_date),
                ("x-amz-content-sha256", &signed.amz_content_sha256),
            ],
        );
        assert!(matches!(
            frontend.prepare_streaming_part(
                &part_request,
                "initial-mutation-part-bucket",
                "key",
                &upload_id,
                "1",
            ),
            Err(ServerError::SlowDown)
        ));
        assert!(frontend
            .coordinator
            .admit_storage_route_for_request()
            .is_ok());
        drop(hook);
        assert_eq!(
            storage::test_support::stream_upload_session_count(&storage_cluster).unwrap(),
            0
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn expired_promoted_post_cleanup_handoff_does_not_block_route_publication() {
        let tmp = test_util::tempdir();
        let initial = open_dynamic_test_storage_cluster(&tmp.path().join("initial"));
        let (runtime_handle, storage_handle) =
            test_dynamic_storage_route_handles(Arc::clone(&initial));
        let frontend = setup_frontend_with_storage_handle(storage_handle.clone());
        create_test_bucket(&frontend, "post-route-expiry-bucket");
        initial.test_store_route_map_validity(
            storage::RouteMapValidity::until_ms(
                storage::clock::current_time_millis().saturating_add(3_600_000),
            )
            .unwrap(),
        );
        let request = make_s3req(
            "POST",
            "/post-route-expiry-bucket",
            &[("host", "localhost")],
        );
        let fields = sign_post_policy_fields("post-route-expiry-bucket", "key", &[], &[]);
        let ctx = Arc::new(
            frontend
                .prepare_streaming_post_object(
                    &request,
                    "post-route-expiry-bucket",
                    &fields,
                    Some("upload.txt"),
                )
                .unwrap(),
        );
        frontend
            .streaming_append_post_segment(&ctx, 0, b"promoted POST payload")
            .unwrap();
        let session_id = ctx.session_id().clone();
        let bucket = ctx.bucket().clone();
        let key = ctx.key().clone();
        let staged_payload = initial
            .test_capture_stream_upload_payload(&bucket, &key, &session_id)
            .unwrap();
        assert!(!staged_payload.is_empty());
        assert!(initial
            .test_stream_upload_payload_snapshot_is_fully_present(&staged_payload)
            .unwrap());
        let cleanup_after = storage::test_support::stream_upload_session_cleanup_after(
            &initial,
            &bucket,
            &key,
            &session_id,
        )
        .unwrap()
        .expect("HTTP stream creation must persist its route cleanup deadline");

        let state = Arc::new(ServerState {
            pool: vec![Arc::clone(&frontend)],
            host_id: frontend.host_id.clone(),
            counter: AtomicUsize::new(0),
            request_semaphore: Arc::new(tokio::sync::Semaphore::new(8)),
            _request_admission_capacity_guard: observability::request_admission_capacity_guard(8),
            segment_buffer_pool: SegmentBufferPool::new(8),
            config: ServeConfig::default(),
            endpoint_kind: EndpointKind::S3Only,
        });
        let abort_guard = StreamingAbortGuard::new(&state);
        abort_guard.arm_post(&ctx);
        let retained_abort_failure = initial.test_fail_retained_stream_abort_with_contention();

        let candidate = open_dynamic_test_storage_cluster(&tmp.path().join("candidate"));
        let install_handle = runtime_handle.clone();
        let installed_candidate = Arc::clone(&candidate);
        let installer = std::thread::spawn(move || {
            install_handle.install(installed_candidate).unwrap();
        });
        let pending_handle = storage_handle.clone();
        tokio::task::spawn_blocking(move || {
            pending_handle.test_wait_until_route_publication_is_pending();
        })
        .await
        .unwrap();
        initial.test_store_route_map_validity(
            storage::RouteMapValidity::until_ms(storage::clock::current_time_millis()).unwrap(),
        );
        drop(ctx);
        drop(abort_guard);

        tokio::time::timeout(
            Duration::from_secs(3),
            tokio::task::spawn_blocking(move || installer.join().unwrap()),
        )
        .await
        .expect("durable POST cleanup handoff and sleeping heartbeat must release publication")
        .unwrap();
        assert!(Arc::ptr_eq(&storage_handle.current(), &candidate));
        tokio::time::timeout(Duration::from_secs(3), async {
            while retained_abort_failure.invocation_count() == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("retained POST cleanup must be attempted before the durable handoff");
        initial.test_store_route_map_validity(long_lived_test_route_map_validity());
        assert!(storage::test_support::stream_upload_session_exists(
            &initial,
            &bucket,
            &key,
            &session_id,
        )
        .unwrap());
        drop(retained_abort_failure);
        storage::clock::with_time_override(cleanup_after, || {
            initial.test_store_route_map_validity(
                storage::RouteMapValidity::until_ms(cleanup_after.saturating_add(1_000)).unwrap(),
            );
            assert_eq!(
                storage::test_support::sweep_abandoned_stream_upload_sessions(&initial, 60_000,),
                1,
                "the durable deadline must let independent current-route cleanup finish"
            );
        });
        assert!(!storage::test_support::stream_upload_session_exists(
            &initial,
            &bucket,
            &key,
            &session_id,
        )
        .unwrap());
        assert!(!initial
            .test_stream_upload_reservation_exists(&bucket, &key, &session_id)
            .unwrap());
        assert!(initial
            .test_stream_upload_payload_snapshot_is_fully_absent(&staged_payload)
            .unwrap());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn expired_promoted_upload_part_cleanup_handoff_does_not_block_publication() {
        let tmp = test_util::tempdir();
        let initial = open_dynamic_test_storage_cluster(&tmp.path().join("initial"));
        let (runtime_handle, storage_handle) =
            test_dynamic_storage_route_handles(Arc::clone(&initial));
        let frontend = setup_frontend_with_storage_handle(storage_handle.clone());
        let upload_id = create_test_bucket_and_upload(&frontend, "part-route-expiry-bucket", "key");
        initial.test_store_route_map_validity(
            storage::RouteMapValidity::until_ms(
                storage::clock::current_time_millis().saturating_add(3_600_000),
            )
            .unwrap(),
        );
        let uri = format!("/part-route-expiry-bucket/key?partNumber=1&uploadId={upload_id}");
        let signed = sign_headers("PUT", &uri, "localhost", b"", &[]);
        let request = make_s3req(
            "PUT",
            &uri,
            &[
                ("host", "localhost"),
                ("authorization", &signed.authorization),
                ("x-amz-date", &signed.amz_date),
                ("x-amz-content-sha256", &signed.amz_content_sha256),
            ],
        );
        let ctx = Arc::new(
            frontend
                .prepare_streaming_part(
                    &request,
                    "part-route-expiry-bucket",
                    "key",
                    &upload_id,
                    "1",
                )
                .unwrap(),
        );
        frontend
            .streaming_append_part_segment(&ctx, 0, b"promoted UploadPart payload")
            .unwrap();
        let session_id = ctx.session_id().clone();
        let bucket = ctx.bucket().clone();
        let key = ctx.key().clone();
        let staged_payload = initial
            .test_capture_stream_upload_payload(&bucket, &key, &session_id)
            .unwrap();
        assert!(!staged_payload.is_empty());
        assert!(initial
            .test_stream_upload_payload_snapshot_is_fully_present(&staged_payload)
            .unwrap());
        let cleanup_after = storage::test_support::stream_upload_session_cleanup_after(
            &initial,
            &bucket,
            &key,
            &session_id,
        )
        .unwrap()
        .expect("UploadPart stream creation must persist its route cleanup deadline");

        let state = Arc::new(ServerState {
            pool: vec![Arc::clone(&frontend)],
            host_id: frontend.host_id.clone(),
            counter: AtomicUsize::new(0),
            request_semaphore: Arc::new(tokio::sync::Semaphore::new(8)),
            _request_admission_capacity_guard: observability::request_admission_capacity_guard(8),
            segment_buffer_pool: SegmentBufferPool::new(8),
            config: ServeConfig::default(),
            endpoint_kind: EndpointKind::S3Only,
        });
        let abort_guard = StreamingAbortGuard::new(&state);
        abort_guard.arm_part(&ctx);
        let retained_abort_failure = initial.test_fail_retained_stream_abort_with_contention();

        let candidate = open_dynamic_test_storage_cluster(&tmp.path().join("candidate"));
        let install_handle = runtime_handle.clone();
        let installed_candidate = Arc::clone(&candidate);
        let installer = std::thread::spawn(move || {
            install_handle.install(installed_candidate).unwrap();
        });
        let pending_handle = storage_handle.clone();
        tokio::task::spawn_blocking(move || {
            pending_handle.test_wait_until_route_publication_is_pending();
        })
        .await
        .unwrap();
        initial.test_store_route_map_validity(
            storage::RouteMapValidity::until_ms(storage::clock::current_time_millis()).unwrap(),
        );
        drop(ctx);
        drop(abort_guard);

        tokio::time::timeout(
            Duration::from_secs(3),
            tokio::task::spawn_blocking(move || installer.join().unwrap()),
        )
        .await
        .expect("durable UploadPart cleanup handoff must release route publication")
        .unwrap();
        assert!(Arc::ptr_eq(&storage_handle.current(), &candidate));
        tokio::time::timeout(Duration::from_secs(3), async {
            while retained_abort_failure.invocation_count() == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("retained UploadPart cleanup must be attempted before the durable handoff");
        initial.test_store_route_map_validity(long_lived_test_route_map_validity());
        assert!(storage::test_support::stream_upload_session_exists(
            &initial,
            &bucket,
            &key,
            &session_id,
        )
        .unwrap());
        drop(retained_abort_failure);
        storage::clock::with_time_override(cleanup_after, || {
            initial.test_store_route_map_validity(
                storage::RouteMapValidity::until_ms(cleanup_after.saturating_add(1_000)).unwrap(),
            );
            assert_eq!(
                storage::test_support::sweep_abandoned_stream_upload_sessions(&initial, 60_000,),
                1,
                "the durable deadline must independently clean UploadPart state"
            );
        });
        assert!(!storage::test_support::stream_upload_session_exists(
            &initial,
            &bucket,
            &key,
            &session_id,
        )
        .unwrap());
        assert!(initial
            .test_stream_upload_payload_snapshot_is_fully_absent(&staged_payload)
            .unwrap());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn local_debug_endpoint_is_disabled_by_default() {
        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        let (addr, _guard) = start_test_server(frontend).await;

        let mut stream = StdTcpStream::connect(&addr).unwrap();
        stream
            .write_all(
                concat!(
                    "GET /__argmin/debug/metrics HTTP/1.1\r\n",
                    "Host: localhost\r\n",
                    "Connection: close\r\n",
                    "\r\n",
                )
                .as_bytes(),
            )
            .unwrap();
        let response = read_http_response(&mut stream, Duration::from_secs(3));

        assert!(
            !response.starts_with("HTTP/1.1 200"),
            "debug endpoint unexpectedly enabled by default: {response}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn local_debug_metrics_endpoint_bypasses_request_admission() {
        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        let backfill_pg_id = u32::MAX - 17;
        for event in ["backfilled", "complete_succeeded"] {
            observability::emit_shard_backfill_event(
                "server_http_test",
                observability::ShardBackfillEventSummary {
                    pg_id: Some(backfill_pg_id),
                    event,
                    queue_depth: None,
                    shards_written: None,
                },
            );
        }
        let config = ServeConfig {
            local_debug_endpoint: true,
            ..ServeConfig::default()
        };
        let (addr, _guard) = start_test_server_with_config(frontend, config, 0).await;

        let mut stream = StdTcpStream::connect(&addr).unwrap();
        stream
            .write_all(
                concat!(
                    "GET /__argmin/debug/metrics HTTP/1.1\r\n",
                    "Host: localhost\r\n",
                    "Connection: close\r\n",
                    "\r\n",
                )
                .as_bytes(),
            )
            .unwrap();
        let response = read_http_response(&mut stream, Duration::from_secs(3));

        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert!(response
            .to_ascii_lowercase()
            .contains("content-type: text/plain; charset=utf-8"));
        let body = response
            .split_once("\r\n\r\n")
            .map(|(_, body)| body)
            .expect("metrics response should include a body");
        let fixed_metrics: std::collections::HashMap<_, _> = body
            .lines()
            .filter_map(|line| {
                let (name, value) = line.split_once(' ')?;
                let value = value.parse::<u64>().ok()?;
                Some((name, value))
            })
            .collect();
        for (name, _) in observability::MetricsSnapshot::default().iter_named() {
            assert!(
                fixed_metrics.contains_key(name),
                "fixed metric {name} should be present in response:\n{response}"
            );
        }
        assert!(
            fixed_metrics.contains_key("frontend_storage_cluster_epoch"),
            "{response}"
        );
        assert!(
            fixed_metrics.contains_key(
                format!("shard_backfill_backfilled_by_pg_total{{pg_id=\"{backfill_pg_id}\"}}")
                    .as_str()
            ),
            "{response}"
        );
        assert!(
            fixed_metrics.contains_key(
                format!(
                    "shard_backfill_complete_succeeded_by_pg_total{{pg_id=\"{backfill_pg_id}\"}}"
                )
                .as_str()
            ),
            "{response}"
        );
    }

    #[test]
    fn frontend_runtime_map_refresh_diagnostics_redact_and_retain_last_failure() {
        let sentinel = "sentinel-bucket/sentinel-object/sentinel-upload-id";
        let status = storage::StorageClusterRuntimeMapRefreshLoopStatus {
            attempts: 9,
            successes: 4,
            failures: 5,
            last_success: Some(storage::StorageClusterRuntimeMapRefreshLoopSuccess {
                cluster_epoch: storage::ClusterEpoch::new(17).unwrap(),
                route_map_validity: storage::RouteMapValidity::until_ms(42_000).unwrap(),
            }),
            last_failure: Some(storage::StorageClusterRuntimeMapRefreshLoopFailure {
                attempt: 7,
                kind: "control_plane_io_timeout",
            }),
            last_error: None,
        };
        let mut body = String::new();

        write_frontend_runtime_map_refresh_status(&mut body, &status);

        assert!(body.contains("frontend_runtime_map_refresh_attempt_total 9\n"));
        assert!(body.contains("frontend_runtime_map_refresh_success_total 4\n"));
        assert!(body.contains("frontend_runtime_map_refresh_failure_total 5\n"));
        assert!(body.contains("frontend_runtime_map_refresh_last_success_epoch 17\n"));
        assert!(body.contains("frontend_runtime_map_refresh_last_success_valid_until_ms 42000\n"));
        assert!(body.contains("frontend_runtime_map_refresh_last_failure_present 1\n"));
        assert!(body.contains("frontend_runtime_map_refresh_last_failure_attempt 7\n"));
        assert!(body.contains(
            "frontend_runtime_map_refresh_last_failure_info{kind=\"control_plane_io_timeout\"} 1\n"
        ));

        let mut status_with_sensitive_current_error = status;
        status_with_sensitive_current_error.last_error =
            Some(format!("failed metadata recovery for {sentinel}"));
        let mut sensitive_body = String::new();
        write_frontend_runtime_map_refresh_status(
            &mut sensitive_body,
            &status_with_sensitive_current_error,
        );
        assert!(!sensitive_body.contains(sentinel));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn local_debug_bucket_delete_attempt_endpoint_returns_absent_record() {
        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        create_test_bucket(&frontend, "debug-attempt-bucket");
        let config = ServeConfig {
            local_debug_endpoint: true,
            ..ServeConfig::default()
        };
        let (addr, _guard) = start_test_server_with_config(frontend, config, 0).await;

        let mut stream = StdTcpStream::connect(&addr).unwrap();
        stream
            .write_all(
                concat!(
                    "GET /__argmin/debug/bucket-delete-attempt/%64ebug-attempt-bucket HTTP/1.1\r\n",
                    "Host: localhost\r\n",
                    "Connection: close\r\n",
                    "\r\n",
                )
                .as_bytes(),
            )
            .unwrap();
        let response = read_http_response(&mut stream, Duration::from_secs(3));

        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert!(response.contains("present=0"), "{response}");
        assert!(
            response.contains("bucket=\"debug-attempt-bucket\""),
            "{response}"
        );
        assert!(response.contains("bucket_row=present"), "{response}");
        assert!(response.contains("state=active"), "{response}");
        assert!(response.contains("cluster_epoch="), "{response}");
        assert!(response.contains("operation_epoch="), "{response}");
        assert!(response.contains("route_map_valid_until_ms="), "{response}");
        assert!(
            response.contains("bucket_pg_primary_node_id="),
            "{response}"
        );
        assert!(
            response.contains("durable_write_drain=absent"),
            "{response}"
        );
        assert!(
            response.contains("pending_metadata_command=absent"),
            "{response}"
        );
        assert!(response.contains("finalize_claim=absent"), "{response}");
        assert!(
            response.contains("object_version_samples_count=0"),
            "{response}"
        );
        assert!(
            response.contains("object_version_sample_errors_count=0"),
            "{response}"
        );
        assert!(
            response.contains("payload_reclaim_roots_count=0"),
            "{response}"
        );
        assert!(
            response.contains("payload_reclaim_root_errors_count=0"),
            "{response}"
        );
        assert!(
            response.contains("payload_reclaim_claims_count=0"),
            "{response}"
        );
        assert!(
            response.contains("payload_reclaim_claim_errors_count=0"),
            "{response}"
        );
        assert!(response.contains("attempt_outcome=absent"), "{response}");
        assert!(response.contains("pg_id="), "{response}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn local_debug_object_payload_placement_reports_committed_segment() {
        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        create_test_bucket(&frontend, "debug-placement-bucket");
        put_test_object(
            &frontend,
            "debug-placement-bucket",
            "nested/key",
            b"payload",
        );
        let bucket = BucketName::try_from("debug-placement-bucket".to_string()).unwrap();
        let key = ObjectKey::try_from("nested/key".to_string()).unwrap();
        let expected = frontend
            .coordinator
            .object_payload_placement_diagnostic(&bucket, &key);
        assert_eq!(
            expected.outcome(),
            ObjectPayloadPlacementDiagnosticOutcome::Success
        );
        let expected_body = expected.into_text();
        let config = ServeConfig {
            local_debug_endpoint: true,
            ..ServeConfig::default()
        };
        let (addr, _guard) = start_test_server_with_config(frontend, config, 0).await;

        let mut stream = StdTcpStream::connect(&addr).unwrap();
        stream
            .write_all(
                concat!(
                    "GET /__argmin/debug/object-payload-placement/debug-placement-bucket/nested%2Fkey HTTP/1.1\r\n",
                    "Host: localhost\r\n",
                    "Connection: close\r\n",
                    "\r\n",
                )
                .as_bytes(),
            )
            .unwrap();
        let response = read_http_response(&mut stream, Duration::from_secs(3));

        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert!(response.ends_with(&expected_body), "{response}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn local_debug_object_payload_placement_failure_is_owner_rendered() {
        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        let config = ServeConfig {
            local_debug_endpoint: true,
            ..ServeConfig::default()
        };
        let (addr, _guard) = start_test_server_with_config(frontend, config, 0).await;

        let mut stream = StdTcpStream::connect(&addr).unwrap();
        stream
            .write_all(
                concat!(
                    "GET /__argmin/debug/object-payload-placement/missing-bucket/key HTTP/1.1\r\n",
                    "Host: localhost\r\n",
                    "Connection: close\r\n",
                    "\r\n",
                )
                .as_bytes(),
            )
            .unwrap();
        let response = read_http_response(&mut stream, Duration::from_secs(3));

        assert!(response.starts_with("HTTP/1.1 409"), "{response}");
        assert!(
            response.ends_with("object payload placement unavailable: metadata_failure\n"),
            "{response}"
        );
        assert!(!response.contains("bucket not found"), "{response}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn local_debug_metadata_checkpoint_record_endpoint_bypasses_request_admission() {
        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        let config = ServeConfig {
            local_debug_endpoint: true,
            ..ServeConfig::default()
        };
        let (addr, _guard) = start_test_server_with_config(frontend, config, 0).await;

        let mut stream = StdTcpStream::connect(&addr).unwrap();
        stream
            .write_all(
                concat!(
                    "POST /__argmin/debug/metadata-checkpoint/record/0 HTTP/1.1\r\n",
                    "Host: localhost\r\n",
                    "Content-Length: 0\r\n",
                    "Connection: close\r\n",
                    "\r\n",
                )
                .as_bytes(),
            )
            .unwrap();
        let response = read_http_response(&mut stream, Duration::from_secs(3));

        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert!(response
            .to_ascii_lowercase()
            .contains("content-type: text/plain; charset=utf-8"));
        assert!(response.contains("pg_id=0"), "{response}");
        assert!(response.contains("scanned=1"), "{response}");
        assert!(response.contains("skipped_empty=1"), "{response}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn local_debug_metadata_checkpoint_failure_is_owner_rendered() {
        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        let config = ServeConfig {
            local_debug_endpoint: true,
            ..ServeConfig::default()
        };
        let (addr, _guard) = start_test_server_with_config(frontend, config, 0).await;

        let mut stream = StdTcpStream::connect(&addr).unwrap();
        stream
            .write_all(
                concat!(
                    "POST /__argmin/debug/metadata-checkpoint/record/4294967295 HTTP/1.1\r\n",
                    "Host: localhost\r\n",
                    "Content-Length: 0\r\n",
                    "Connection: close\r\n",
                    "\r\n",
                )
                .as_bytes(),
            )
            .unwrap();
        let response = read_http_response(&mut stream, Duration::from_secs(3));

        assert!(response.starts_with("HTTP/1.1 409"), "{response}");
        assert!(
            response
                .ends_with("pg_id=4294967295 checkpoint_record_failed=store_topology_failure\n"),
            "{response}"
        );
        assert!(!response.contains("cluster epoch"), "{response}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn local_debug_metadata_checkpoint_selector_grammar_is_storage_owned() {
        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        let config = ServeConfig {
            local_debug_endpoint: true,
            ..ServeConfig::default()
        };
        let (addr, _guard) = start_test_server_with_config(frontend, config, 0).await;

        let mut stream = StdTcpStream::connect(&addr).unwrap();
        stream
            .write_all(
                concat!(
                    "POST /__argmin/debug/metadata-checkpoint/record/not-a-number HTTP/1.1\r\n",
                    "Host: localhost\r\n",
                    "Content-Length: 0\r\n",
                    "Connection: close\r\n",
                    "\r\n",
                )
                .as_bytes(),
            )
            .unwrap();
        let response = read_http_response(&mut stream, Duration::from_secs(3));

        assert!(response.starts_with("HTTP/1.1 400"), "{response}");
        assert!(response.ends_with("invalid pg id\n"), "{response}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn local_debug_flight_recorder_dump_endpoint_is_explicit_post_only() {
        SUPPRESS_LOCAL_DEBUG_FLIGHT_RECORDER_DUMP.store(true, Ordering::SeqCst);
        struct ResetFlightRecorderDumpSuppression;
        impl Drop for ResetFlightRecorderDumpSuppression {
            fn drop(&mut self) {
                SUPPRESS_LOCAL_DEBUG_FLIGHT_RECORDER_DUMP.store(false, Ordering::SeqCst);
            }
        }
        let _reset_suppression = ResetFlightRecorderDumpSuppression;
        let dump_calls_before = LOCAL_DEBUG_FLIGHT_RECORDER_DUMP_CALLS.load(Ordering::SeqCst);

        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        let config = ServeConfig {
            local_debug_endpoint: true,
            ..ServeConfig::default()
        };
        let (addr, _guard) = start_test_server_with_config(frontend, config, 0).await;

        let mut stream = StdTcpStream::connect(&addr).unwrap();
        stream
            .write_all(
                concat!(
                    "GET /__argmin/debug/flight-recorder/dump HTTP/1.1\r\n",
                    "Host: localhost\r\n",
                    "Connection: close\r\n",
                    "\r\n",
                )
                .as_bytes(),
            )
            .unwrap();
        let response = read_http_response(&mut stream, Duration::from_secs(3));
        assert!(response.starts_with("HTTP/1.1 404"), "{response}");
        assert_eq!(
            LOCAL_DEBUG_FLIGHT_RECORDER_DUMP_CALLS.load(Ordering::SeqCst),
            dump_calls_before
        );

        let mut stream = StdTcpStream::connect(&addr).unwrap();
        stream
            .write_all(
                concat!(
                    "POST /__argmin/debug/flight-recorder/dump HTTP/1.1\r\n",
                    "Host: localhost\r\n",
                    "Content-Length: 0\r\n",
                    "Connection: close\r\n",
                    "\r\n",
                )
                .as_bytes(),
            )
            .unwrap();
        let response = read_http_response(&mut stream, Duration::from_secs(3));
        assert!(response.starts_with("HTTP/1.1 204"), "{response}");
        assert_eq!(
            LOCAL_DEBUG_FLIGHT_RECORDER_DUMP_CALLS.load(Ordering::SeqCst),
            dump_calls_before + 1
        );
    }

    fn response_body_complete(buf: &[u8], header_end: usize, headers: &str) -> bool {
        let body_start = header_end + 4;
        if let Some(content_length) = headers.lines().find_map(|line| {
            let lower = line.to_lowercase();
            lower
                .strip_prefix("content-length: ")
                .map(|v| v.trim().parse::<usize>().unwrap_or(0))
        }) {
            return buf.len() >= body_start + content_length;
        }

        let is_chunked = headers.lines().any(|line| {
            let lower = line.to_lowercase();
            lower
                .strip_prefix("transfer-encoding: ")
                .is_some_and(|value| value.split(',').any(|part| part.trim() == "chunked"))
        });
        if !is_chunked {
            return body_start == buf.len();
        }

        let mut offset = body_start;
        while offset < buf.len() {
            let Some(line_end_rel) = buf[offset..].windows(2).position(|w| w == b"\r\n") else {
                return false;
            };
            let line_end = offset + line_end_rel;
            let size_line = match std::str::from_utf8(&buf[offset..line_end]) {
                Ok(line) => line,
                Err(_) => return false,
            };
            let size_hex = size_line.split(';').next().unwrap_or("").trim();
            let Ok(chunk_size) = usize::from_str_radix(size_hex, 16) else {
                return false;
            };
            offset = line_end + 2;
            if chunk_size == 0 {
                // The last-chunk line is followed directly by the trailer
                // section. With no trailers the section is one empty line,
                // so the complete terminator is `0\r\n\r\n`; there is no
                // additional chunk-data CRLF for the zero-sized last chunk.
                if buf.len() < offset + 2 {
                    return false;
                }
                if &buf[offset..offset + 2] == b"\r\n" {
                    return true;
                }
                return buf[offset..].windows(4).any(|window| window == b"\r\n\r\n");
            }
            let Some(chunk_end) = offset.checked_add(chunk_size) else {
                return false;
            };
            let Some(chunk_crlf_end) = chunk_end.checked_add(2) else {
                return false;
            };
            if buf.len() < chunk_crlf_end {
                return false;
            }
            if &buf[chunk_end..chunk_crlf_end] != b"\r\n" {
                return false;
            }
            offset = chunk_crlf_end;
        }

        false
    }

    #[test]
    fn response_body_complete_recognizes_chunked_last_chunk_and_trailers() {
        let headers = "HTTP/1.1 400 Bad Request\r\nTransfer-Encoding: chunked";
        let header_end = headers.len();

        let mut response = format!("{headers}\r\n\r\n").into_bytes();
        response.extend_from_slice(b"5\r\nhello\r\n0\r\n\r\n");
        assert!(response_body_complete(&response, header_end, headers));

        let mut incomplete = format!("{headers}\r\n\r\n").into_bytes();
        incomplete.extend_from_slice(b"5\r\nhello\r\n0\r\n");
        assert!(!response_body_complete(&incomplete, header_end, headers));

        let mut with_trailer = format!("{headers}\r\n\r\n").into_bytes();
        with_trailer.extend_from_slice(b"5\r\nhello\r\n0\r\nx-test: value\r\n\r\n");
        assert!(response_body_complete(&with_trailer, header_end, headers));
    }

    fn read_http_response(stream: &mut StdTcpStream, timeout: Duration) -> String {
        read_http_response_inner(stream, timeout, None)
            .unwrap_or_else(|message| panic!("{message}"))
    }

    fn read_http_response_stopping_writer(
        stream: &mut StdTcpStream,
        timeout: Duration,
        stop_writer: &AtomicBool,
    ) -> String {
        read_http_response_inner(stream, timeout, Some(stop_writer))
            .unwrap_or_else(|message| panic!("{message}"))
    }

    fn read_http_response_inner(
        stream: &mut StdTcpStream,
        timeout: Duration,
        stop_writer: Option<&AtomicBool>,
    ) -> Result<String, String> {
        let mut buf = Vec::with_capacity(8192);
        let mut tmp = [0u8; 4096];
        stream
            .set_read_timeout(Some(timeout))
            .expect("set read timeout");
        let mut writer_signaled = false;

        loop {
            match stream.read(&mut tmp) {
                Ok(0) => {
                    if stop_writer.is_some() {
                        return Err(format!(
                            "early HTTP response reached premature EOF before the response body was complete; partial response: {}",
                            String::from_utf8_lossy(&buf)
                        ));
                    }
                    break;
                }
                Ok(n) => {
                    buf.extend_from_slice(&tmp[..n]);
                }
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => {
                    if stop_writer.is_some() {
                        return Err(format!(
                            "early HTTP response read failed before the response body was complete: {error:?}; partial response: {}",
                            String::from_utf8_lossy(&buf)
                        ));
                    }
                    break;
                }
            }

            let text = String::from_utf8_lossy(&buf);
            if let Some(header_end) = text.find("\r\n\r\n") {
                // Stop uploading as soon as the early response starts
                // arriving, like a real client; the server's lingering
                // close then quiesces and delivers the rest of the body.
                if !writer_signaled {
                    if let Some(stop_writer) = stop_writer {
                        stop_writer.store(true, Ordering::Relaxed);
                        // Stop producing request bytes, but keep the socket's
                        // write half open until the complete response has
                        // arrived. Half-closing an incomplete Content-Length
                        // request can make Hyper terminate the connection
                        // while it is still delivering the response body.
                        writer_signaled = true;
                    }
                }
                let headers = &text[..header_end];
                if response_body_complete(&buf, header_end, headers) {
                    break;
                }
            }
        }

        Ok(String::from_utf8_lossy(&buf).into_owned())
    }

    #[test]
    fn early_http_response_reader_rejects_premature_chunked_eof() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test listener");
        let addr = listener.local_addr().expect("read test listener address");
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept test connection");
            stream
                .write_all(
                    b"HTTP/1.1 400 Bad Request\r\n\
Transfer-Encoding: chunked\r\n\
Connection: close\r\n\r\n",
                )
                .expect("write truncated response headers");
        });

        let mut stream = StdTcpStream::connect(addr).expect("connect to test listener");
        let stop_writer = AtomicBool::new(false);
        let error =
            read_http_response_inner(&mut stream, Duration::from_secs(1), Some(&stop_writer))
                .expect_err("truncated chunked response must fail");

        assert!(error.contains("premature EOF"), "{error}");
        assert!(error.contains("HTTP/1.1 400 Bad Request"), "{error}");
        assert!(stop_writer.load(Ordering::Relaxed));
        server.join().expect("join truncated response server");
    }

    fn denied_streaming_request_response(
        addr: &str,
        request_head: String,
        total_body_bytes: usize,
    ) -> (String, usize) {
        response_before_request_body_sent(addr, request_head, total_body_bytes)
    }

    fn response_before_request_body_sent(
        addr: &str,
        request_head: String,
        total_body_bytes: usize,
    ) -> (String, usize) {
        const WRITE_CHUNK_BYTES: usize = 1024;
        const WRITE_CHUNK_DELAY: Duration = Duration::from_millis(20);
        const RESPONSE_TIMEOUT: Duration = Duration::from_secs(15);

        let mut stream = StdTcpStream::connect(addr).unwrap();
        stream.set_nodelay(true).unwrap();

        let mut writer = stream.try_clone().unwrap();
        writer.set_nodelay(true).unwrap();

        let bytes_sent = Arc::new(AtomicUsize::new(0));
        let bytes_sent_writer = Arc::clone(&bytes_sent);
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop_writer = Arc::clone(&stop);
        let body_chunk = vec![b'x'; WRITE_CHUNK_BYTES];
        let writer_handle = std::thread::spawn(move || {
            writer.write_all(request_head.as_bytes()).unwrap();
            let chunk_count = total_body_bytes / WRITE_CHUNK_BYTES;
            for _ in 0..chunk_count {
                if stop_writer.load(Ordering::Relaxed) {
                    break;
                }
                match writer.write_all(&body_chunk) {
                    Ok(()) => {
                        bytes_sent_writer.fetch_add(WRITE_CHUNK_BYTES, Ordering::Relaxed);
                        std::thread::sleep(WRITE_CHUNK_DELAY);
                    }
                    Err(_) => break,
                }
            }
        });

        let response = read_http_response_stopping_writer(&mut stream, RESPONSE_TIMEOUT, &stop);
        stop.store(true, Ordering::Relaxed);
        writer_handle.join().unwrap();
        let _ = stream.shutdown(Shutdown::Write);
        (response, bytes_sent.load(Ordering::Relaxed))
    }

    fn denied_streaming_multipart_request_response(
        addr: &str,
        request_head: String,
        file_prefix: Vec<u8>,
        total_file_bytes: usize,
    ) -> (String, usize) {
        const WRITE_CHUNK_BYTES: usize = 1024;
        const WRITE_CHUNK_DELAY: Duration = Duration::from_millis(20);
        const RESPONSE_TIMEOUT: Duration = Duration::from_secs(15);

        let mut stream = StdTcpStream::connect(addr).unwrap();
        stream.set_nodelay(true).unwrap();
        stream.write_all(request_head.as_bytes()).unwrap();
        stream.write_all(&file_prefix).unwrap();

        let mut writer = stream.try_clone().unwrap();
        writer.set_nodelay(true).unwrap();

        let bytes_sent = Arc::new(AtomicUsize::new(0));
        let bytes_sent_writer = Arc::clone(&bytes_sent);
        let stop = Arc::new(AtomicBool::new(false));
        let stop_writer = Arc::clone(&stop);
        let body_chunk = vec![b'x'; WRITE_CHUNK_BYTES];
        let writer_handle = std::thread::spawn(move || {
            let chunk_count = total_file_bytes / WRITE_CHUNK_BYTES;
            for _ in 0..chunk_count {
                if stop_writer.load(Ordering::Relaxed) {
                    break;
                }
                match writer.write_all(&body_chunk) {
                    Ok(()) => {
                        bytes_sent_writer.fetch_add(WRITE_CHUNK_BYTES, Ordering::Relaxed);
                        std::thread::sleep(WRITE_CHUNK_DELAY);
                    }
                    Err(_) => break,
                }
            }
        });

        let response = read_http_response_stopping_writer(&mut stream, RESPONSE_TIMEOUT, &stop);
        stop.store(true, Ordering::Relaxed);
        writer_handle.join().unwrap();
        let _ = stream.shutdown(Shutdown::Write);
        (response, bytes_sent.load(Ordering::Relaxed))
    }

    fn hmac_sha256(key: &[u8], data: &[u8]) -> hmac::Tag {
        hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), data)
    }

    #[allow(clippy::format_collect)]
    fn hex_encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{:02x}", b)).collect()
    }

    fn days_to_ymd(days: u64) -> (u64, u64, u64) {
        let z = days + 719468;
        let era = z / 146097;
        let doe = z - era * 146097;
        let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
        let y = yoe + era * 400;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let d = doy - (153 * mp + 2) / 5 + 1;
        let m = if mp < 10 { mp + 3 } else { mp - 9 };
        let y = if m <= 2 { y + 1 } else { y };
        (y, m, d)
    }

    struct SignedHeaders {
        authorization: String,
        amz_date: String,
        amz_content_sha256: String,
    }

    struct SignedStreamingHeaders {
        authorization: String,
        amz_date: String,
        amz_content_sha256: &'static str,
        signing_key: [u8; 32],
        seed_signature: String,
        scope: String,
    }

    struct PresignedStreamingRequest {
        uri: String,
        headers: Vec<(String, String)>,
    }

    fn sign_headers(
        method: &str,
        uri: &str,
        host: &str,
        body: &[u8],
        extra_headers: &[(&str, &str)],
    ) -> SignedHeaders {
        let secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let days = secs / 86400;
        let (year, month, day) = days_to_ymd(days);
        let time_of_day = secs % 86400;
        let hour = time_of_day / 3600;
        let minute = (time_of_day % 3600) / 60;
        let second = time_of_day % 60;
        let date_long = format!(
            "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
            year, month, day, hour, minute, second
        );
        let date_short = &date_long[..8];
        let content_sha256 = sha256_hex(body);
        let (path, query) = uri.split_once('?').unwrap_or((uri, ""));
        let mut signed_header_pairs = vec![
            ("host", host),
            ("x-amz-content-sha256", content_sha256.as_str()),
            ("x-amz-date", date_long.as_str()),
        ];
        signed_header_pairs.extend_from_slice(extra_headers);
        signed_header_pairs.sort_by_key(|(name, _)| *name);

        let signed_headers = signed_header_pairs
            .iter()
            .map(|(name, _)| *name)
            .collect::<Vec<_>>()
            .join(";");
        let canonical_headers = canonical_headers(&signed_header_pairs);
        let canonical_request = canonical_request(
            method,
            path,
            &canonical_query_string(query),
            &canonical_headers,
            &signed_headers,
            &content_sha256,
        );
        let canonical_hash = sha256_hex(canonical_request.as_bytes());
        let scope = format!("{}/us-east-1/s3/aws4_request", date_short);
        let string_to_sign = string_to_sign(&date_long, &scope, &canonical_hash);
        let signing_key = derive_signing_key(
            &auth::SecretKey::new(TEST_SECRET_KEY.to_string()),
            date_short,
            "us-east-1",
            "s3",
        );
        let signature = hmac_sha256(signing_key.as_ref(), string_to_sign.as_bytes());
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            TEST_ACCESS_KEY,
            scope,
            signed_headers,
            hex_encode(signature.as_ref())
        );

        SignedHeaders {
            authorization,
            amz_date: date_long,
            amz_content_sha256: content_sha256,
        }
    }

    fn sign_streaming_headers(
        method: &str,
        uri: &str,
        host: &str,
        decoded_content_length: usize,
        extra_headers: &[(&str, &str)],
    ) -> SignedStreamingHeaders {
        let secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let days = secs / 86400;
        let (year, month, day) = days_to_ymd(days);
        let time_of_day = secs % 86400;
        let hour = time_of_day / 3600;
        let minute = (time_of_day % 3600) / 60;
        let second = time_of_day % 60;
        let date_long = format!(
            "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
            year, month, day, hour, minute, second
        );
        let date_short = &date_long[..8];
        let content_sha256 = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
        let decoded_content_length = decoded_content_length.to_string();
        let (path, query) = uri.split_once('?').unwrap_or((uri, ""));
        let mut signed_header_pairs = vec![
            ("content-encoding", "aws-chunked"),
            ("host", host),
            ("x-amz-content-sha256", content_sha256),
            ("x-amz-date", date_long.as_str()),
            (
                "x-amz-decoded-content-length",
                decoded_content_length.as_str(),
            ),
        ];
        signed_header_pairs.extend_from_slice(extra_headers);
        signed_header_pairs.sort_by_key(|(name, _)| *name);

        let signed_headers = signed_header_pairs
            .iter()
            .map(|(name, _)| *name)
            .collect::<Vec<_>>()
            .join(";");
        let canonical_headers = canonical_headers(&signed_header_pairs);
        let canonical_request = canonical_request(
            method,
            path,
            &canonical_query_string(query),
            &canonical_headers,
            &signed_headers,
            content_sha256,
        );
        let canonical_hash = sha256_hex(canonical_request.as_bytes());
        let scope = format!("{}/us-east-1/s3/aws4_request", date_short);
        let string_to_sign = string_to_sign(&date_long, &scope, &canonical_hash);
        let signing_key = derive_signing_key(
            &auth::SecretKey::new(TEST_SECRET_KEY.to_string()),
            date_short,
            "us-east-1",
            "s3",
        );
        let signature = hmac_sha256(signing_key.as_ref(), string_to_sign.as_bytes());
        let seed_signature = hex_encode(signature.as_ref());
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            TEST_ACCESS_KEY, scope, signed_headers, seed_signature
        );
        let mut signing_key_bytes = [0u8; 32];
        signing_key_bytes.copy_from_slice(signing_key.as_ref());

        SignedStreamingHeaders {
            authorization,
            amz_date: date_long,
            amz_content_sha256: content_sha256,
            signing_key: signing_key_bytes,
            seed_signature,
            scope,
        }
    }

    fn presign_streaming_request(
        method: &str,
        path_and_query: &str,
        host: &str,
        payload_hash: &str,
        decoded_content_length: usize,
        wire_content_length: usize,
        trailer: Option<&str>,
    ) -> PresignedStreamingRequest {
        let secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let days = secs / 86400;
        let (year, month, day) = days_to_ymd(days);
        let time_of_day = secs % 86400;
        let hour = time_of_day / 3600;
        let minute = (time_of_day % 3600) / 60;
        let second = time_of_day % 60;
        let amz_date = format!("{year:04}{month:02}{day:02}T{hour:02}{minute:02}{second:02}Z");
        let date = &amz_date[..8];
        let scope = format!("{date}/us-east-1/s3/aws4_request");
        let decoded_content_length = decoded_content_length.to_string();
        let wire_content_length = wire_content_length.to_string();
        let mut headers = vec![
            ("content-encoding".to_string(), "aws-chunked".to_string()),
            ("content-length".to_string(), wire_content_length),
            ("host".to_string(), host.to_string()),
            ("x-amz-content-sha256".to_string(), payload_hash.to_string()),
            (
                "x-amz-decoded-content-length".to_string(),
                decoded_content_length,
            ),
        ];
        if let Some(trailer) = trailer {
            headers.push(("x-amz-trailer".to_string(), trailer.to_string()));
        }
        headers.sort_by(|(left, _), (right, _)| left.cmp(right));
        let signed_headers = headers
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>()
            .join(";");
        let header_refs = headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect::<Vec<_>>();
        let canonical_headers = canonical_headers(&header_refs);
        let (path, operation_query) = path_and_query
            .split_once('?')
            .unwrap_or((path_and_query, ""));
        let credential = auth::canonical::uri_encode(&format!("{TEST_ACCESS_KEY}/{scope}"));
        let signed_headers_encoded = auth::canonical::uri_encode(&signed_headers);
        let mut query_without_signature = format!(
            "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential={credential}&X-Amz-Date={amz_date}&X-Amz-Expires=900&X-Amz-SignedHeaders={signed_headers_encoded}"
        );
        if !operation_query.is_empty() {
            query_without_signature.push('&');
            query_without_signature.push_str(operation_query);
        }
        let canonical_query = canonical_query_string(&query_without_signature);
        let canonical_request = canonical_request(
            method,
            path,
            &canonical_query,
            &canonical_headers,
            &signed_headers,
            payload_hash,
        );
        let string_to_sign =
            string_to_sign(&amz_date, &scope, &sha256_hex(canonical_request.as_bytes()));
        let signing_key = derive_signing_key(
            &auth::SecretKey::new(TEST_SECRET_KEY.to_string()),
            date,
            "us-east-1",
            "s3",
        );
        let signature =
            hex_encode(hmac_sha256(signing_key.as_ref(), string_to_sign.as_bytes()).as_ref());
        PresignedStreamingRequest {
            uri: format!("{path}?{query_without_signature}&X-Amz-Signature={signature}"),
            headers,
        }
    }

    fn chunk_signature(
        signing_key: &[u8],
        timestamp: &str,
        scope: &str,
        prev_sig: &str,
        chunk_data: &[u8],
    ) -> String {
        let empty_hash = sha256_hex(b"");
        let chunk_hash = sha256_hex(chunk_data);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256-PAYLOAD\n{timestamp}\n{scope}\n{prev_sig}\n{empty_hash}\n{chunk_hash}"
        );
        hex_encode(hmac_sha256(signing_key, string_to_sign.as_bytes()).as_ref())
    }

    fn build_signed_chunked_body(sign: &SignedStreamingHeaders, data: &[u8]) -> Vec<u8> {
        let chunk_sig = chunk_signature(
            &sign.signing_key,
            &sign.amz_date,
            &sign.scope,
            &sign.seed_signature,
            data,
        );
        let terminal_sig = chunk_signature(
            &sign.signing_key,
            &sign.amz_date,
            &sign.scope,
            &chunk_sig,
            b"",
        );

        let mut wire = Vec::new();
        wire.extend_from_slice(
            format!("{:x};chunk-signature={chunk_sig}\r\n", data.len()).as_bytes(),
        );
        wire.extend_from_slice(data);
        wire.extend_from_slice(b"\r\n");
        wire.extend_from_slice(format!("0;chunk-signature={terminal_sig}\r\n\r\n").as_bytes());
        wire
    }

    fn build_signed_chunked_body_with_bad_terminal_signature(
        sign: &SignedStreamingHeaders,
        data: &[u8],
    ) -> Vec<u8> {
        let chunk_sig = chunk_signature(
            &sign.signing_key,
            &sign.amz_date,
            &sign.scope,
            &sign.seed_signature,
            data,
        );

        let mut wire = Vec::new();
        wire.extend_from_slice(
            format!("{:x};chunk-signature={chunk_sig}\r\n", data.len()).as_bytes(),
        );
        wire.extend_from_slice(data);
        wire.extend_from_slice(b"\r\n");
        wire.extend_from_slice(format!("0;chunk-signature={}\r\n\r\n", "0".repeat(64)).as_bytes());
        wire
    }

    fn send_raw_http_request(addr: &str, request: &str, body: &[u8]) -> String {
        let mut stream = StdTcpStream::connect(addr).unwrap();
        stream.write_all(request.as_bytes()).unwrap();
        stream.write_all(body).unwrap();
        read_http_response(&mut stream, Duration::from_secs(5))
    }

    fn send_presigned_streaming_request(
        addr: &str,
        path_and_query: &str,
        payload_hash: &str,
        decoded_content_length: usize,
        trailer: Option<&str>,
        body: &[u8],
    ) -> String {
        let presigned = presign_streaming_request(
            "PUT",
            path_and_query,
            addr,
            payload_hash,
            decoded_content_length,
            body.len(),
            trailer,
        );
        let mut request = format!("PUT {} HTTP/1.1\r\n", presigned.uri);
        for (name, value) in presigned.headers {
            request.push_str(&format!("{name}: {value}\r\n"));
        }
        request.push_str("Connection: close\r\n\r\n");
        send_raw_http_request(addr, &request, body)
    }

    fn assert_signed_head_not_found(addr: &str, bucket: &str, key: &str) {
        let uri = format!("/{bucket}/{key}");
        let signed = sign_headers("HEAD", &uri, addr, &[], &[]);
        let request = format!(
            "HEAD {uri} HTTP/1.1\r\n\
Host: {addr}\r\n\
Authorization: {}\r\n\
x-amz-date: {}\r\n\
x-amz-content-sha256: {}\r\n\
Connection: close\r\n\r\n",
            signed.authorization, signed.amz_date, signed.amz_content_sha256
        );
        let response = send_raw_http_request(addr, &request, &[]);
        assert!(
            response.starts_with("HTTP/1.1 404"),
            "expected 404 for unpublished object, got: {}",
            response.lines().next().unwrap_or("")
        );
    }

    fn assert_signed_list_parts_empty(addr: &str, bucket: &str, key: &str, upload_id: &str) {
        let uri = format!("/{bucket}/{key}?uploadId={upload_id}");
        let signed = sign_headers("GET", &uri, addr, &[], &[]);
        let request = format!(
            "GET {uri} HTTP/1.1\r\n\
Host: {addr}\r\n\
Authorization: {}\r\n\
x-amz-date: {}\r\n\
x-amz-content-sha256: {}\r\n\
Connection: close\r\n\r\n",
            signed.authorization, signed.amz_date, signed.amz_content_sha256
        );
        let response = send_raw_http_request(addr, &request, &[]);
        assert!(
            response.starts_with("HTTP/1.1 200"),
            "expected ListParts success, got: {}",
            response.lines().next().unwrap_or("")
        );
        assert!(
            !response.contains("<Part>"),
            "aborted streaming UploadPart left a visible part: {response}"
        );
    }

    fn sign_post_policy_fields(
        bucket: &str,
        key: &str,
        extra_conditions: &[&str],
        extra_fields: &[(&str, &str)],
    ) -> Vec<(String, String)> {
        use base64::Engine;

        let secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let days = secs / 86400;
        let (year, month, day) = days_to_ymd(days);
        let time_of_day = secs % 86400;
        let hour = time_of_day / 3600;
        let minute = (time_of_day % 3600) / 60;
        let second = time_of_day % 60;
        let amz_date = format!(
            "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
            year, month, day, hour, minute, second
        );
        let date = amz_date[..8].to_string();
        let credential = format!("{TEST_ACCESS_KEY}/{date}/us-east-1/s3/aws4_request");
        let mut conditions = vec![
            format!(r#"{{"bucket":"{bucket}"}}"#),
            format!(r#"{{"key":"{key}"}}"#),
            r#"{"x-amz-algorithm":"AWS4-HMAC-SHA256"}"#.to_string(),
            format!(r#"{{"x-amz-credential":"{credential}"}}"#),
            format!(r#"{{"x-amz-date":"{amz_date}"}}"#),
        ];
        conditions.extend(
            extra_conditions
                .iter()
                .map(|condition| (*condition).to_string()),
        );
        let policy = format!(
            r#"{{"expiration":"2099-12-31T23:59:59Z","conditions":[{}]}}"#,
            conditions.join(",")
        );
        let policy_b64 = base64::engine::general_purpose::STANDARD.encode(policy.as_bytes());
        let signing_key = derive_signing_key(
            &auth::SecretKey::new(TEST_SECRET_KEY.to_string()),
            &date,
            "us-east-1",
            "s3",
        );
        let signature =
            hex_encode(hmac_sha256(signing_key.as_ref(), policy_b64.as_bytes()).as_ref());

        let mut fields = vec![
            ("key".to_string(), key.to_string()),
            (
                "x-amz-algorithm".to_string(),
                "AWS4-HMAC-SHA256".to_string(),
            ),
            ("x-amz-credential".to_string(), credential),
            ("x-amz-date".to_string(), amz_date),
            ("policy".to_string(), policy_b64),
            ("x-amz-signature".to_string(), signature),
        ];
        fields.extend(
            extra_fields
                .iter()
                .map(|(name, value)| (name.to_string(), value.to_string())),
        );
        fields
    }

    fn build_streaming_multipart_parts(
        fields: &[(String, String)],
        file_name: &str,
    ) -> (String, Vec<u8>, Vec<u8>) {
        let boundary = "----TestBoundary7MA4YWxkTrZu0gW";
        let mut prefix = Vec::new();

        for (name, value) in fields {
            prefix.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
            prefix.extend_from_slice(
                format!("Content-Disposition: form-data; name=\"{name}\"\r\n\r\n").as_bytes(),
            );
            prefix.extend_from_slice(value.as_bytes());
            prefix.extend_from_slice(b"\r\n");
        }

        prefix.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        prefix.extend_from_slice(
            format!("Content-Disposition: form-data; name=\"file\"; filename=\"{file_name}\"\r\n")
                .as_bytes(),
        );
        prefix.extend_from_slice(b"Content-Type: application/octet-stream\r\n\r\n");

        let suffix = format!("\r\n--{boundary}--\r\n").into_bytes();
        (
            format!("multipart/form-data; boundary={boundary}"),
            prefix,
            suffix,
        )
    }

    #[test]
    fn streaming_put_eligible() {
        let parts = make_parts(
            "PUT",
            "/mybucket/mykey",
            &[("x-amz-content-sha256", "UNSIGNED-PAYLOAD")],
        );
        let result = is_streaming_write(&parts);
        assert!(matches!(
            result,
            Ok(Some(StreamingWriteOp::PutObject { ref bucket, ref key, .. }))
            if bucket == "mybucket" && key == "mykey"
        ));
    }

    #[test]
    fn streaming_put_not_put_method() {
        let parts = make_parts(
            "POST",
            "/mybucket/mykey",
            &[("x-amz-content-sha256", "UNSIGNED-PAYLOAD")],
        );
        assert!(matches!(is_streaming_write(&parts), Ok(None)));
    }

    #[test]
    fn streaming_put_get_method() {
        let parts = make_parts(
            "GET",
            "/mybucket/mykey",
            &[("x-amz-content-sha256", "UNSIGNED-PAYLOAD")],
        );
        assert!(matches!(is_streaming_write(&parts), Ok(None)));
    }

    #[test]
    fn streaming_put_copy_source_excluded() {
        let parts = make_parts(
            "PUT",
            "/mybucket/mykey",
            &[
                ("x-amz-content-sha256", "UNSIGNED-PAYLOAD"),
                ("x-amz-copy-source", "/src-bucket/src-key"),
            ],
        );
        assert!(matches!(is_streaming_write(&parts), Ok(None)));
    }

    #[test]
    fn streaming_put_real_sha256_routed() {
        let parts = make_parts(
            "PUT",
            "/mybucket/mykey",
            &[(
                "x-amz-content-sha256",
                "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
            )],
        );
        let result = is_streaming_write(&parts);
        assert!(matches!(
            result,
            Ok(Some(StreamingWriteOp::PutObject { ref bucket, ref key, .. }))
            if bucket == "mybucket" && key == "mykey"
        ));
    }

    #[test]
    fn streaming_put_no_sha256_header_routed() {
        let parts = make_parts("PUT", "/mybucket/mykey", &[]);
        let result = is_streaming_write(&parts);
        assert!(matches!(
            result,
            Ok(Some(StreamingWriteOp::PutObject { ref bucket, ref key, .. }))
            if bucket == "mybucket" && key == "mykey"
        ));
    }

    #[test]
    fn parse_chunked_mode_signed_chunked() {
        let req = make_s3req(
            "PUT",
            "/mybucket/mykey",
            &[
                ("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"),
                ("content-encoding", "aws-chunked"),
                ("x-amz-decoded-content-length", "100"),
            ],
        );
        let mode = parse_chunked_mode(&req).unwrap();
        assert!(matches!(mode, ChunkedMode::Signed { expected_len: 100 }));
    }

    #[test]
    fn parse_chunked_mode_signed_trailer_chunked() {
        let req = make_s3req(
            "PUT",
            "/mybucket/mykey",
            &[
                (
                    "x-amz-content-sha256",
                    "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER",
                ),
                ("content-encoding", "aws-chunked"),
                ("x-amz-decoded-content-length", "100"),
            ],
        );
        let mode = parse_chunked_mode(&req).unwrap();
        assert!(matches!(
            mode,
            ChunkedMode::SignedTrailer { expected_len: 100 }
        ));
    }

    #[test]
    fn parse_chunked_mode_unsigned_trailer_chunked() {
        let req = make_s3req(
            "PUT",
            "/mybucket/mykey",
            &[
                ("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER"),
                ("content-encoding", "aws-chunked"),
                ("x-amz-decoded-content-length", "100"),
            ],
        );
        let mode = parse_chunked_mode(&req).unwrap();
        assert!(matches!(
            mode,
            ChunkedMode::UnsignedTrailer { expected_len: 100 }
        ));
    }

    #[test]
    fn parse_chunked_mode_unsigned_payload_alone_rejected() {
        // STREAMING-UNSIGNED-PAYLOAD (without -TRAILER) is rejected by AWS.
        let req = make_s3req(
            "PUT",
            "/mybucket/mykey",
            &[("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD")],
        );
        let err = parse_chunked_mode(&req).unwrap_err();
        assert!(matches!(err, ServerError::UnsupportedStreamingToken { .. }));
    }

    #[test]
    fn parse_chunked_mode_missing_content_encoding_allowed() {
        let req = make_s3req(
            "PUT",
            "/mybucket/mykey",
            &[
                ("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"),
                ("x-amz-decoded-content-length", "100"),
            ],
        );
        let mode = parse_chunked_mode(&req).unwrap();
        assert!(matches!(mode, ChunkedMode::Signed { expected_len: 100 }));
    }

    #[test]
    fn parse_chunked_mode_non_aws_content_encoding_allowed() {
        let req = make_s3req(
            "PUT",
            "/mybucket/mykey",
            &[
                ("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"),
                ("content-encoding", "gzip"),
                ("x-amz-decoded-content-length", "100"),
            ],
        );
        let mode = parse_chunked_mode(&req).unwrap();
        assert!(matches!(mode, ChunkedMode::Signed { expected_len: 100 }));
    }

    #[test]
    fn parse_chunked_mode_missing_decoded_length_rejected() {
        let req = make_s3req(
            "PUT",
            "/mybucket/mykey",
            &[
                ("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"),
                ("content-encoding", "aws-chunked"),
            ],
        );
        let err = parse_chunked_mode(&req).unwrap_err();
        assert!(matches!(err, ServerError::MissingContentLength));
    }

    #[test]
    fn parse_chunked_mode_decoded_length_over_object_limit_rejected() {
        let too_large = (MAX_OBJECT_SIZE + 1).to_string();
        let req = make_s3req(
            "PUT",
            "/mybucket/mykey",
            &[
                ("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"),
                ("content-encoding", "aws-chunked"),
                ("x-amz-decoded-content-length", too_large.as_str()),
            ],
        );
        let err = parse_chunked_mode(&req).unwrap_err();
        assert!(matches!(err, ServerError::ObjectTooLarge { .. }));
    }

    #[test]
    fn streaming_put_bucket_config_excluded() {
        // PUT /<bucket>?versioning is a bucket config op, not PutObject.
        let parts = make_parts(
            "PUT",
            "/mybucket?versioning",
            &[("x-amz-content-sha256", "UNSIGNED-PAYLOAD")],
        );
        assert!(matches!(is_streaming_write(&parts), Ok(None)));
    }

    #[test]
    fn streaming_put_object_retention_excluded() {
        // PUT /<bucket>/<key>?retention is an object-lock API, not PutObject.
        let parts = make_parts(
            "PUT",
            "/mybucket/mykey?retention",
            &[("x-amz-content-sha256", "UNSIGNED-PAYLOAD")],
        );
        assert!(matches!(is_streaming_write(&parts), Ok(None)));
    }

    #[test]
    fn streaming_put_deep_key() {
        let parts = make_parts(
            "PUT",
            "/mybucket/path/to/deep/key.txt",
            &[("x-amz-content-sha256", "UNSIGNED-PAYLOAD")],
        );
        let result = is_streaming_write(&parts);
        assert!(matches!(
            result,
            Ok(Some(StreamingWriteOp::PutObject { ref bucket, ref key, .. }))
            if bucket == "mybucket" && key == "path/to/deep/key.txt"
        ));
    }

    #[test]
    fn streaming_upload_part_routed() {
        let parts = make_parts(
            "PUT",
            "/mybucket/mykey?partNumber=3&uploadId=abc123",
            &[("x-amz-content-sha256", "UNSIGNED-PAYLOAD")],
        );
        let result = is_streaming_write(&parts);
        assert!(matches!(
            result,
            Ok(Some(StreamingWriteOp::UploadPart {
                ref bucket,
                ref key,
                ref upload_id,
                ref part_number,
                ..
            })) if bucket == "mybucket" && key == "mykey" && upload_id == "abc123" && part_number == "3"
        ));
    }

    #[test]
    fn streaming_upload_part_real_sha256_routed() {
        let parts = make_parts(
            "PUT",
            "/mybucket/mykey?partNumber=3&uploadId=abc123",
            &[(
                "x-amz-content-sha256",
                "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
            )],
        );
        let result = is_streaming_write(&parts);
        assert!(matches!(
            result,
            Ok(Some(StreamingWriteOp::UploadPart {
                ref bucket,
                ref key,
                ref upload_id,
                ref part_number,
            })) if bucket == "mybucket" && key == "mykey" && upload_id == "abc123" && part_number == "3"
        ));
    }

    #[test]
    fn streaming_upload_part_missing_upload_id_rejected() {
        let parts = make_parts(
            "PUT",
            "/mybucket/mykey?partNumber=3",
            &[("x-amz-content-sha256", "UNSIGNED-PAYLOAD")],
        );
        assert!(matches!(
            is_streaming_write(&parts),
            Err(ServerError::UploadPartMissingUploadId)
        ));
    }

    #[test]
    fn streaming_upload_part_invalid_upload_id_is_preserved_for_later_validation() {
        let invalid_upload_id = storage::UploadId::overlong_for_test();
        let parts = make_parts(
            "PUT",
            &format!("/mybucket/mykey?partNumber=3&uploadId={invalid_upload_id}"),
            &[("x-amz-content-sha256", "UNSIGNED-PAYLOAD")],
        );
        let result = is_streaming_write(&parts);
        assert!(matches!(
            result,
            Ok(Some(StreamingWriteOp::UploadPart {
                ref bucket,
                ref key,
                ref upload_id,
                ref part_number,
                ..
            })) if bucket == "mybucket" && key == "mykey" && upload_id == &invalid_upload_id && part_number == "3"
        ));
    }

    #[test]
    fn streaming_upload_part_invalid_part_number_is_preserved_for_target_validation() {
        let parts = make_parts(
            "PUT",
            "/mybucket/mykey?partNumber=abc&uploadId=abc123",
            &[("x-amz-content-sha256", "UNSIGNED-PAYLOAD")],
        );
        assert!(matches!(
            is_streaming_write(&parts),
            Ok(Some(StreamingWriteOp::UploadPart { part_number, .. })) if part_number == "abc"
        ));
    }

    #[test]
    fn streaming_upload_part_zero_part_number_is_preserved_for_target_validation() {
        let parts = make_parts(
            "PUT",
            "/mybucket/mykey?partNumber=0&uploadId=abc123",
            &[("x-amz-content-sha256", "UNSIGNED-PAYLOAD")],
        );
        assert!(matches!(
            is_streaming_write(&parts),
            Ok(Some(StreamingWriteOp::UploadPart { part_number, .. })) if part_number == "0"
        ));
    }

    #[test]
    fn put_multipart_upload_without_part_number_is_rejected_before_body_routing() {
        for headers in [
            vec![("x-amz-content-sha256", "UNSIGNED-PAYLOAD")],
            vec![
                ("x-amz-content-sha256", "UNSIGNED-PAYLOAD"),
                ("x-amz-copy-source", "/src/key"),
            ],
        ] {
            let parts = make_parts("PUT", "/mybucket/mykey?uploadId=abc", headers.as_slice());
            assert!(matches!(
                is_streaming_write(&parts),
                Err(ServerError::PutMultipartUploadMethodNotAllowed)
            ));
        }
    }

    #[test]
    fn streaming_upload_part_copy_excluded() {
        let parts = make_parts(
            "PUT",
            "/mybucket/mykey?partNumber=1&uploadId=abc",
            &[
                ("x-amz-content-sha256", "UNSIGNED-PAYLOAD"),
                ("x-amz-copy-source", "/src/key"),
            ],
        );
        assert!(matches!(is_streaming_write(&parts), Ok(None)));
    }

    #[test]
    fn post_object_detected() {
        let parts = make_parts("POST", "/mybucket", &[]);
        assert_eq!(
            post_object_bucket(&parts).as_ref().map(BucketName::as_str),
            Some("mybucket")
        );
    }

    #[test]
    fn post_delete_objects_not_detected_as_post_object() {
        let parts = make_parts("POST", "/mybucket?delete", &[]);
        assert!(post_object_bucket(&parts).is_none());
    }

    #[test]
    fn post_multipart_parser_boundary_and_header_split_across_feeds() {
        let boundary = "BoundaryX";
        let mut parser = PostMultipartParser::new(boundary);

        let c1 = b"--Bound";
        let c2 = b"aryX\r\nContent-Disposition: form-data; name=\"key\"\r\n\r\nvalue\r\n--BoundaryX\r\nContent-Disposition: form-data; name=\"file\"; filename=\"x\"\r\nContent-Type: application/octet-stream\r\n\r";
        let c3 = b"\nhello-world\r\n--BoundaryX--\r\n";

        let ev1 = parser.feed(c1).unwrap();
        assert!(ev1.is_empty());

        let ev2 = parser.feed(c2).unwrap();
        assert_eq!(ev2.len(), 1);
        match &ev2[0] {
            PostMultipartEvent::Field { name, value } => {
                assert_eq!(name, "key");
                assert_eq!(value, "value");
            }
            _ => panic!("expected a single field event"),
        }

        let ev3 = parser.feed(c3).unwrap();
        let mut saw_start = false;
        let mut saw_end = false;
        let mut file = Vec::new();
        for ev in ev3 {
            match ev {
                PostMultipartEvent::FileStart { file_name } => {
                    saw_start = true;
                    assert_eq!(file_name.as_deref(), Some("x"));
                }
                PostMultipartEvent::FileChunk(bytes) => file.extend_from_slice(&bytes),
                PostMultipartEvent::FileEnd => saw_end = true,
                PostMultipartEvent::Field { .. } => panic!("unexpected field event"),
            }
        }
        assert!(saw_start);
        assert!(saw_end);
        assert_eq!(file, b"hello-world");
        assert!(parser.is_done());
    }

    #[test]
    fn post_multipart_parser_file_data_integrity_with_split_delimiter() {
        let boundary = "BoundaryY";
        let delimiter = format!("\r\n--{boundary}--\r\n");

        let file_data: Vec<u8> = (0_u8..=127).cycle().take(1024).collect();
        let mut body = Vec::new();
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            b"Content-Disposition: form-data; name=\"file\"; filename=\"blob.bin\"\r\n",
        );
        body.extend_from_slice(b"Content-Type: application/octet-stream\r\n\r\n");
        body.extend_from_slice(&file_data);
        body.extend_from_slice(delimiter.as_bytes());

        let marker_pos = body
            .windows(delimiter.len())
            .position(|w| w == delimiter.as_bytes())
            .unwrap();
        // Split in the middle of the final delimiter to force cross-feed detection.
        let split_at = marker_pos + 3;

        let mut parser = PostMultipartParser::new(boundary);
        let ev1 = parser.feed(&body[..split_at]).unwrap();
        let ev2 = parser.feed(&body[split_at..]).unwrap();

        let mut saw_start = false;
        let mut saw_end = false;
        let mut reconstructed = Vec::new();
        for ev in ev1.into_iter().chain(ev2) {
            match ev {
                PostMultipartEvent::FileStart { file_name } => {
                    saw_start = true;
                    assert_eq!(file_name.as_deref(), Some("blob.bin"));
                }
                PostMultipartEvent::FileChunk(bytes) => reconstructed.extend_from_slice(&bytes),
                PostMultipartEvent::FileEnd => saw_end = true,
                PostMultipartEvent::Field { .. } => panic!("unexpected field event"),
            }
        }

        assert!(saw_start);
        assert!(saw_end);
        assert_eq!(reconstructed, file_data);
        assert!(parser.is_done());
    }

    #[test]
    fn post_multipart_parser_rejects_oversized_part_headers() {
        let boundary = "BoundaryZ";
        let mut parser = PostMultipartParser::new(boundary);

        let mut body = Vec::new();
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(b"Content-Disposition: form-data; name=\"");
        body.extend(std::iter::repeat_n(
            b'h',
            MAX_STREAMING_POST_PART_HEADER_BYTES,
        ));

        let err = parser.feed(&body).unwrap_err();
        match err {
            ServerError::InvalidRequest { reason } => {
                assert!(reason.contains("multipart part headers exceed maximum size"));
            }
            other => panic!("expected InvalidRequest, got {other:?}"),
        }
    }

    #[test]
    fn post_multipart_parser_rejects_oversized_non_file_field_without_boundary() {
        let boundary = "BoundaryField";
        let mut parser = PostMultipartParser::new(boundary);

        let mut body = Vec::new();
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(b"Content-Disposition: form-data; name=\"x-amz-date\"\r\n\r\n");
        body.extend(std::iter::repeat_n(
            b'0',
            MAX_STREAMING_POST_DATE_FIELD_BYTES + parser.delimiter.len(),
        ));

        let err = parser.feed(&body).unwrap_err();
        match err {
            ServerError::InvalidRequest { reason } => {
                assert!(reason.contains("multipart form field 'x-amz-date' exceeds maximum size"));
            }
            other => panic!("expected InvalidRequest, got {other:?}"),
        }
    }

    #[test]
    fn fuzz_post_multipart_parser_rejects_missing_final_boundary() {
        let boundary = "BoundaryEOF";
        let body = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"blob.bin\"\r\nContent-Type: application/octet-stream\r\n\r\nhello world"
        );

        let err = fuzz_post_multipart_parser(boundary, body.as_bytes(), &[3, 1, 4, 1]).unwrap_err();
        assert!(matches!(err, ServerError::IncompleteBody));
    }

    #[test]
    fn fuzz_post_multipart_parser_rejects_missing_file_field() {
        let boundary = "BoundaryNoFile";
        let body = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"key\"\r\n\r\nobject-key\r\n--{boundary}--"
        );

        let err = fuzz_post_multipart_parser(boundary, body.as_bytes(), &[2, 7, 1]).unwrap_err();
        assert!(matches!(
            err,
            ServerError::InvalidRequest { reason } if reason == "missing file field in multipart form"
        ));
    }

    #[test]
    fn streaming_post_field_budget_rejects_metadata_over_limit() {
        let mut budget = StreamingPostFieldBudget::default();
        let name = "x-amz-meta-limit";
        let value = "m".repeat(USER_METADATA_SIZE_LIMIT - name.len());

        budget.record(name, &value).unwrap();

        let err = budget.record(name, "x").unwrap_err();
        assert!(matches!(
            err,
            ServerError::MetadataTooLargeDetailed {
                max_size_allowed: USER_METADATA_SIZE_LIMIT,
                ..
            }
        ));
    }

    #[test]
    fn streaming_post_field_budget_rejects_total_non_file_bytes_over_limit() {
        let mut budget = StreamingPostFieldBudget::default();
        let value = "r".repeat(MAX_STREAMING_POST_DEFAULT_FIELD_BYTES);
        let field_bytes = "redirect".len() + value.len();
        while budget.total_bytes + field_bytes <= MAX_STREAMING_POST_NON_FILE_FORM_BYTES {
            budget.record("redirect", &value).unwrap();
        }

        let overflow = "o".repeat(MAX_STREAMING_POST_NON_FILE_FORM_BYTES - budget.total_bytes + 1);
        let err = budget.record("redirect", &overflow).unwrap_err();
        match err {
            ServerError::InvalidRequest { reason } => {
                assert!(reason.contains("multipart form fields exceed maximum total size"));
            }
            other => panic!("expected InvalidRequest, got {other:?}"),
        }
    }

    // ── Regression tests for P0–P2 security fixes ────────────────────

    #[test]
    fn extract_checksum_trailers_rejects_duplicates() {
        // P0: Duplicate trailer names must be rejected to prevent bypass.
        let trailers = vec![
            ("x-amz-checksum-crc32".to_string(), "AAAAAA==".to_string()),
            ("x-amz-checksum-crc32".to_string(), "BBBBBB==".to_string()),
        ];
        let err = extract_checksum_trailers(&trailers).unwrap_err();
        assert!(matches!(err, ServerError::InvalidRequest { .. }));
    }

    #[test]
    fn extract_checksum_trailers_rejects_mixed_case_duplicates() {
        // P0: Case-insensitive dedup — mixed-case duplicates are rejected.
        let trailers = vec![
            ("x-amz-checksum-crc32".to_string(), "AAAAAA==".to_string()),
            ("X-Amz-Checksum-CRC32".to_string(), "BBBBBB==".to_string()),
        ];
        let err = extract_checksum_trailers(&trailers).unwrap_err();
        assert!(matches!(err, ServerError::InvalidRequest { .. }));
    }

    #[test]
    fn extract_checksum_trailers_ignores_non_checksum() {
        // Non-checksum trailers and excluded names are not extracted.
        let trailers = vec![
            ("x-amz-checksum-algorithm".to_string(), "CRC32".to_string()),
            ("x-amz-checksum-type".to_string(), "FULL_OBJECT".to_string()),
            ("x-amz-request-id".to_string(), "abc".to_string()),
            ("x-amz-checksum-sha256".to_string(), "dGVzdA==".to_string()),
        ];
        let result = extract_checksum_trailers(&trailers).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, "x-amz-checksum-sha256");
    }

    #[test]
    fn trailing_hasher_case_insensitive() {
        // P1: Header name matching must be case-insensitive.
        assert!(TrailingChecksumHasher::from_trailer_header("X-Amz-Checksum-CRC32").is_some());
        assert!(TrailingChecksumHasher::from_trailer_header("x-amz-checksum-crc32").is_some());
        assert!(TrailingChecksumHasher::from_trailer_header("X-AMZ-CHECKSUM-CRC32C").is_some());
        assert!(TrailingChecksumHasher::from_trailer_header("X-Amz-Checksum-Sha256").is_some());
        assert!(TrailingChecksumHasher::from_trailer_header("x-amz-checksum-sha1").is_some());
        assert!(TrailingChecksumHasher::from_trailer_header("X-AMZ-CHECKSUM-CRC64NVME").is_some());
        assert!(TrailingChecksumHasher::from_trailer_header("x-amz-checksum-md5").is_some());
        assert!(TrailingChecksumHasher::from_trailer_header("X-Amz-Checksum-XXHash64").is_some());
        assert!(TrailingChecksumHasher::from_trailer_header("x-amz-checksum-xxhash3").is_some());
        assert!(TrailingChecksumHasher::from_trailer_header("X-Amz-Checksum-XXHash128").is_some());
        assert!(TrailingChecksumHasher::from_trailer_header("x-amz-checksum-sha512").is_some());
    }

    #[test]
    fn trailing_hasher_from_request_csv() {
        // P1: x-amz-trailer can be comma-separated; first recognized name wins.
        let req = make_s3req(
            "PUT",
            "/mybucket/mykey",
            &[("x-amz-trailer", "x-amz-checksum-type, x-amz-checksum-crc32")],
        );
        let hasher = trailing_hasher_from_request(&req);
        assert!(hasher.is_some());
        // Verify it's a CRC32 hasher by finalizing empty data.
        let cksum = hasher.unwrap().finalize_raw();
        assert_eq!(cksum.algorithm(), ChecksumAlgorithm::Crc32);
    }

    #[test]
    fn trailing_hasher_from_request_single() {
        let req = make_s3req(
            "PUT",
            "/mybucket/mykey",
            &[("x-amz-trailer", "x-amz-checksum-sha256")],
        );
        let hasher = trailing_hasher_from_request(&req);
        assert!(hasher.is_some());
        let cksum = hasher.unwrap().finalize_raw();
        assert_eq!(cksum.algorithm(), ChecksumAlgorithm::Sha256);
    }

    #[test]
    fn trailing_hasher_from_request_none_when_no_header() {
        let req = make_s3req("PUT", "/mybucket/mykey", &[]);
        assert!(trailing_hasher_from_request(&req).is_none());
    }

    #[test]
    fn claimed_payload_sha256_from_request_recognizes_fixed_hash() {
        let fixed = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
        let req = make_s3req("PUT", "/mybucket/mykey", &[("x-amz-content-sha256", fixed)]);
        assert_eq!(
            claimed_payload_sha256_from_request(&req).as_deref(),
            Some(fixed)
        );
    }

    #[test]
    fn claimed_payload_sha256_from_request_ignores_sentinel_values() {
        let req = make_s3req(
            "PUT",
            "/mybucket/mykey",
            &[("x-amz-content-sha256", "UNSIGNED-PAYLOAD")],
        );
        assert!(claimed_payload_sha256_from_request(&req).is_none());

        let req = make_s3req(
            "PUT",
            "/mybucket/mykey",
            &[("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")],
        );
        assert!(claimed_payload_sha256_from_request(&req).is_none());
    }

    #[test]
    fn crc32c_streaming_matches_canonical() {
        // P1: Incremental CRC32C must match checksum::crc32c::checksum().
        let data = b"123456789";
        let expected = checksum::crc32c::checksum(data);

        let mut hasher =
            TrailingChecksumHasher::from_trailer_header("x-amz-checksum-crc32c").unwrap();
        hasher.update(data);
        let cksum = hasher.finalize_raw();
        assert_eq!(cksum.algorithm(), ChecksumAlgorithm::Crc32c);
        assert_eq!(
            u32::from_be_bytes(cksum.bytes().try_into().unwrap()),
            expected,
            "streaming CRC32C mismatch for b\"123456789\""
        );
    }

    #[test]
    fn crc32c_streaming_incremental_matches_canonical() {
        // P1: Multi-chunk incremental CRC32C must also match.
        let data = b"hello world!";
        let expected = checksum::crc32c::checksum(data);

        let mut hasher =
            TrailingChecksumHasher::from_trailer_header("x-amz-checksum-crc32c").unwrap();
        hasher.update(b"hello ");
        hasher.update(b"world!");
        let cksum = hasher.finalize_raw();
        assert_eq!(
            u32::from_be_bytes(cksum.bytes().try_into().unwrap()),
            expected,
            "incremental streaming CRC32C mismatch"
        );
    }

    #[test]
    fn crc32c_streaming_empty_matches_canonical() {
        let expected = checksum::crc32c::checksum(b"");
        let hasher = TrailingChecksumHasher::from_trailer_header("x-amz-checksum-crc32c").unwrap();
        let cksum = hasher.finalize_raw();
        assert_eq!(
            u32::from_be_bytes(cksum.bytes().try_into().unwrap()),
            expected
        );
    }

    #[test]
    fn crc32_streaming_matches_canonical() {
        // Sanity check: CRC32 streaming also matches.
        let data = b"123456789";
        let expected = checksum::crc32::checksum(data);

        let mut hasher =
            TrailingChecksumHasher::from_trailer_header("x-amz-checksum-crc32").unwrap();
        hasher.update(data);
        let cksum = hasher.finalize_raw();
        assert_eq!(cksum.algorithm(), ChecksumAlgorithm::Crc32);
        assert_eq!(
            u32::from_be_bytes(cksum.bytes().try_into().unwrap()),
            expected
        );
    }

    #[test]
    fn crc64_streaming_matches_canonical() {
        let data = b"123456789";
        let expected = checksum::crc64::checksum(data);

        let mut hasher =
            TrailingChecksumHasher::from_trailer_header("x-amz-checksum-crc64nvme").unwrap();
        hasher.update(data);
        let cksum = hasher.finalize_raw();
        assert_eq!(cksum.algorithm(), ChecksumAlgorithm::Crc64nvme);
        assert_eq!(
            u64::from_be_bytes(cksum.bytes().try_into().unwrap()),
            expected
        );
    }

    #[test]
    fn sha256_streaming_matches_canonical() {
        let data = b"123456789";
        let expected = ring::digest::digest(&ring::digest::SHA256, data);

        let mut hasher =
            TrailingChecksumHasher::from_trailer_header("x-amz-checksum-sha256").unwrap();
        hasher.update(data);
        let cksum = hasher.finalize_raw();
        assert_eq!(cksum.algorithm(), ChecksumAlgorithm::Sha256);
        assert_eq!(cksum.bytes(), expected.as_ref());
    }

    #[test]
    fn sha1_streaming_matches_canonical() {
        let data = b"123456789";
        let expected = ring::digest::digest(&ring::digest::SHA1_FOR_LEGACY_USE_ONLY, data);

        let mut hasher =
            TrailingChecksumHasher::from_trailer_header("x-amz-checksum-sha1").unwrap();
        hasher.update(data);
        let cksum = hasher.finalize_raw();
        assert_eq!(cksum.algorithm(), ChecksumAlgorithm::Sha1);
        assert_eq!(cksum.bytes(), expected.as_ref());
    }

    #[test]
    fn sha512_streaming_matches_shared_checksum() {
        let data = b"123456789";
        let expected = checksum::compute_checksum(ChecksumAlgorithm::Sha512, data);

        let mut hasher =
            TrailingChecksumHasher::from_trailer_header("x-amz-checksum-sha512").unwrap();
        hasher.update(data);
        let cksum = hasher.finalize_raw();
        assert_eq!(cksum.algorithm(), ChecksumAlgorithm::Sha512);
        assert_eq!(cksum.bytes(), expected.bytes());
    }

    #[test]
    fn xxhash3_streaming_matches_shared_checksum() {
        let data = b"123456789";
        let expected = checksum::compute_checksum(ChecksumAlgorithm::XxHash3, data);

        let mut hasher =
            TrailingChecksumHasher::from_trailer_header("x-amz-checksum-xxhash3").unwrap();
        hasher.update(data);
        let cksum = hasher.finalize_raw();
        assert_eq!(cksum.algorithm(), ChecksumAlgorithm::XxHash3);
        assert_eq!(cksum.bytes(), expected.bytes());
    }

    #[test]
    fn sha256_streaming_incremental_matches_canonical() {
        let data = b"hello world!";
        let expected = ring::digest::digest(&ring::digest::SHA256, data);

        let mut hasher =
            TrailingChecksumHasher::from_trailer_header("x-amz-checksum-sha256").unwrap();
        hasher.update(b"hello ");
        hasher.update(b"world!");
        let cksum = hasher.finalize_raw();
        assert_eq!(cksum.bytes(), expected.as_ref());
    }

    // ── Inline checksum hasher tests ─────────────────────────────────

    #[test]
    fn inline_checksum_hasher_picks_up_crc32_header() {
        let req = make_s3req(
            "PUT",
            "/mybucket/mykey",
            &[("x-amz-checksum-crc32", "AAAAAA==")],
        );
        let result = inline_checksum_hasher_from_request(&req);
        assert!(result.is_some());
        let (h, claimed) = result.unwrap();
        assert_eq!(claimed, "AAAAAA==");
        let cksum = h.finalize_raw();
        assert_eq!(cksum.algorithm(), ChecksumAlgorithm::Crc32);
    }

    #[test]
    fn inline_checksum_hasher_none_when_no_checksum() {
        let req = make_s3req("PUT", "/mybucket/mykey", &[]);
        assert!(inline_checksum_hasher_from_request(&req).is_none());
    }

    #[test]
    fn inline_checksum_hasher_picks_sha256() {
        let req = make_s3req(
            "PUT",
            "/mybucket/mykey",
            &[("x-amz-checksum-sha256", "dGVzdA==")],
        );
        let result = inline_checksum_hasher_from_request(&req);
        assert!(result.is_some());
        let (h, claimed) = result.unwrap();
        assert_eq!(claimed, "dGVzdA==");
        let cksum = h.finalize_raw();
        assert_eq!(cksum.algorithm(), ChecksumAlgorithm::Sha256);
    }

    #[test]
    fn inline_checksum_hasher_picks_sha512() {
        let req = make_s3req(
            "PUT",
            "/mybucket/mykey",
            &[("x-amz-checksum-sha512", "dGVzdA==")],
        );
        let result = inline_checksum_hasher_from_request(&req);
        assert!(result.is_some());
        let (h, claimed) = result.unwrap();
        assert_eq!(claimed, "dGVzdA==");
        let cksum = h.finalize_raw();
        assert_eq!(cksum.algorithm(), ChecksumAlgorithm::Sha512);
    }

    #[test]
    fn inline_checksum_hasher_not_triggered_by_non_checksum_headers() {
        // x-amz-checksum-algorithm is not a checksum value header.
        let req = make_s3req(
            "PUT",
            "/mybucket/mykey",
            &[("x-amz-checksum-algorithm", "CRC32")],
        );
        assert!(inline_checksum_hasher_from_request(&req).is_none());
    }

    #[test]
    fn streaming_post_field_value_limit_covers_new_checksums() {
        assert_eq!(
            streaming_post_field_value_limit("x-amz-checksum-sha512"),
            MAX_STREAMING_POST_CHECKSUM_FIELD_BYTES
        );
        assert_eq!(
            streaming_post_field_value_limit("X-Amz-Checksum-XXHash128"),
            MAX_STREAMING_POST_CHECKSUM_FIELD_BYTES
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn streaming_upload_part_bad_checksum_aborts_session() {
        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        let upload_id = create_test_bucket_and_upload(&frontend, "mybucket", "mykey");
        let (addr, _guard) = start_test_server(Arc::clone(&frontend)).await;

        let body = b"part-data";
        let uri = format!("/mybucket/mykey?partNumber=1&uploadId={upload_id}");
        let checksum = "AAAAAA==";
        let signed = sign_headers(
            "PUT",
            &uri,
            &addr,
            body,
            &[("x-amz-checksum-crc32", checksum)],
        );

        let mut stream = StdTcpStream::connect(&addr).unwrap();
        let request = format!(
            "PUT {uri} HTTP/1.1\r\n\
Host: {addr}\r\n\
Authorization: {}\r\n\
x-amz-date: {}\r\n\
x-amz-content-sha256: {}\r\n\
x-amz-checksum-crc32: {}\r\n\
Content-Length: {}\r\n\
Connection: close\r\n\r\n",
            signed.authorization,
            signed.amz_date,
            signed.amz_content_sha256,
            checksum,
            body.len()
        );
        stream.write_all(request.as_bytes()).unwrap();
        stream.write_all(body).unwrap();

        let response = read_http_response(&mut stream, Duration::from_secs(5));
        assert!(
            response.starts_with("HTTP/1.1 400"),
            "expected 400 status, got: {}",
            response.lines().next().unwrap_or("")
        );
        assert!(
            response.contains("<Code>BadDigest</Code>"),
            "expected BadDigest body, got: {response}"
        );
        assert_eq!(
            storage::test_support::stream_upload_session_count(&frontend.test_storage_cluster,)
                .unwrap(),
            0,
            "streaming session leaked after UploadPart bad checksum"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn streaming_upload_part_bad_terminal_signature_aborts_promoted_session() {
        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        let upload_id = create_test_bucket_and_upload(&frontend, "mybucket", "mykey");
        let (addr, _guard) = start_test_server(Arc::clone(&frontend)).await;

        let body = vec![b'p'; crate::coordinator::INTERNAL_SEGMENT_SIZE + 1];
        let uri = format!("/mybucket/mykey?partNumber=1&uploadId={upload_id}");
        let signed = sign_streaming_headers("PUT", &uri, &addr, body.len(), &[]);
        let wire = build_signed_chunked_body_with_bad_terminal_signature(&signed, &body);
        let request = format!(
            "PUT {uri} HTTP/1.1\r\n\
Host: {addr}\r\n\
Authorization: {}\r\n\
x-amz-date: {}\r\n\
x-amz-content-sha256: {}\r\n\
content-encoding: aws-chunked\r\n\
x-amz-decoded-content-length: {}\r\n\
Content-Length: {}\r\n\
Connection: close\r\n\r\n",
            signed.authorization,
            signed.amz_date,
            signed.amz_content_sha256,
            body.len(),
            wire.len()
        );
        let response = send_raw_http_request(&addr, &request, &wire);

        assert!(
            response.starts_with("HTTP/1.1 403"),
            "expected 403 status, got: {}",
            response.lines().next().unwrap_or("")
        );
        assert!(
            response.contains("<Code>SignatureDoesNotMatch</Code>"),
            "expected SignatureDoesNotMatch body, got: {response}"
        );
        assert_eq!(
            storage::test_support::stream_upload_session_count(&frontend.test_storage_cluster,)
                .unwrap(),
            0,
            "streaming session leaked after UploadPart bad terminal signature"
        );
        assert_signed_list_parts_empty(&addr, "mybucket", "mykey", &upload_id);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn streaming_put_bad_checksum_aborts_promoted_session() {
        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        create_test_bucket(&frontend, "mybucket");
        let (addr, _guard) = start_test_server(Arc::clone(&frontend)).await;

        let body = vec![b'p'; crate::coordinator::INTERNAL_SEGMENT_SIZE + 1];
        let checksum = "AAAAAA==";
        let signed = sign_streaming_headers(
            "PUT",
            "/mybucket/mykey",
            &addr,
            body.len(),
            &[("x-amz-checksum-crc32", checksum)],
        );
        let wire = build_signed_chunked_body(&signed, &body);

        let request = format!(
            "PUT /mybucket/mykey HTTP/1.1\r\n\
Host: {addr}\r\n\
Authorization: {}\r\n\
x-amz-date: {}\r\n\
x-amz-content-sha256: {}\r\n\
content-encoding: aws-chunked\r\n\
x-amz-decoded-content-length: {}\r\n\
x-amz-checksum-crc32: {}\r\n\
Content-Length: {}\r\n\
Connection: close\r\n\r\n",
            signed.authorization,
            signed.amz_date,
            signed.amz_content_sha256,
            body.len(),
            checksum,
            wire.len()
        );
        let response = send_raw_http_request(&addr, &request, &wire);

        assert!(
            response.starts_with("HTTP/1.1 400"),
            "expected 400 status, got: {}",
            response.lines().next().unwrap_or("")
        );
        assert!(
            response.contains("<Code>BadDigest</Code>"),
            "expected BadDigest body, got: {response}"
        );
        assert_eq!(
            storage::test_support::stream_upload_session_count(&frontend.test_storage_cluster,)
                .unwrap(),
            0,
            "streaming session leaked after PutObject bad checksum"
        );
        assert_signed_head_not_found(&addr, "mybucket", "mykey");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn streaming_put_bad_terminal_signature_aborts_promoted_session() {
        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        create_test_bucket(&frontend, "mybucket");
        let (addr, _guard) = start_test_server(Arc::clone(&frontend)).await;

        let body = vec![b's'; crate::coordinator::INTERNAL_SEGMENT_SIZE + 1];
        let signed = sign_streaming_headers("PUT", "/mybucket/mykey", &addr, body.len(), &[]);
        let wire = build_signed_chunked_body_with_bad_terminal_signature(&signed, &body);

        let request = format!(
            "PUT /mybucket/mykey HTTP/1.1\r\n\
Host: {addr}\r\n\
Authorization: {}\r\n\
x-amz-date: {}\r\n\
x-amz-content-sha256: {}\r\n\
content-encoding: aws-chunked\r\n\
x-amz-decoded-content-length: {}\r\n\
Content-Length: {}\r\n\
Connection: close\r\n\r\n",
            signed.authorization,
            signed.amz_date,
            signed.amz_content_sha256,
            body.len(),
            wire.len()
        );
        let response = send_raw_http_request(&addr, &request, &wire);

        assert!(
            response.starts_with("HTTP/1.1 403"),
            "expected 403 status, got: {}",
            response.lines().next().unwrap_or("")
        );
        assert!(
            response.contains("<Code>SignatureDoesNotMatch</Code>"),
            "expected SignatureDoesNotMatch body, got: {response}"
        );
        assert_eq!(
            storage::test_support::stream_upload_session_count(&frontend.test_storage_cluster,)
                .unwrap(),
            0,
            "streaming session leaked after PutObject bad terminal signature"
        );
        assert_signed_head_not_found(&addr, "mybucket", "mykey");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn presigned_streaming_markers_validate_the_raw_put_body_like_aws() {
        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        create_test_bucket(&frontend, "mybucket");
        let (addr, _guard) = start_test_server(Arc::clone(&frontend)).await;

        let payload = b"hello";
        let zero_signature = "0".repeat(64);
        let signed_wire = format!(
            "5;chunk-signature={zero_signature}\r\nhello\r\n\
             0;chunk-signature={zero_signature}\r\n\r\n"
        )
        .into_bytes();
        let response = send_presigned_streaming_request(
            &addr,
            "/mybucket/signed-wire",
            "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
            payload.len(),
            None,
            &signed_wire,
        );
        assert!(response.starts_with("HTTP/1.1 400"), "{response}");
        assert!(
            response.contains("<Code>IncompleteBody</Code>"),
            "{response}"
        );
        assert!(
            response.contains("<NumberBytesExpected>5</NumberBytesExpected>"),
            "{response}"
        );
        assert!(
            response.contains(&format!(
                "<NumberBytesProvided>{}</NumberBytesProvided>",
                signed_wire.len()
            )),
            "{response}"
        );
        assert!(!response.contains("<Resource>"), "{response}");
        assert_signed_head_not_found(&addr, "mybucket", "signed-wire");

        let unsigned_wire = b"5\r\nhello\r\n0\r\n\r\n";
        let response = send_presigned_streaming_request(
            &addr,
            "/mybucket/missing-chunk-signatures",
            "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
            payload.len(),
            None,
            unsigned_wire,
        );
        assert!(response.starts_with("HTTP/1.1 400"), "{response}");
        assert!(
            response.contains("<Code>IncompleteBody</Code>"),
            "{response}"
        );
        assert!(
            response.contains(&format!(
                "<NumberBytesProvided>{}</NumberBytesProvided>",
                unsigned_wire.len()
            )),
            "{response}"
        );
        assert_signed_head_not_found(&addr, "mybucket", "missing-chunk-signatures");

        let response = send_presigned_streaming_request(
            &addr,
            "/mybucket/raw-exact",
            "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
            payload.len(),
            None,
            payload,
        );
        assert!(response.starts_with("HTTP/1.1 400"), "{response}");
        assert!(
            response.contains("<Code>XAmzContentSHA256Mismatch</Code>"),
            "{response}"
        );
        assert!(
            response.contains(
                "<ClientComputedContentSHA256>STREAMING-AWS4-HMAC-SHA256-PAYLOAD</ClientComputedContentSHA256>"
            ),
            "{response}"
        );
        assert!(
            response.contains(&format!(
                "<S3ComputedContentSHA256>{}</S3ComputedContentSHA256>",
                sha256_hex(payload)
            )),
            "{response}"
        );
        assert!(!response.contains("<Resource>"), "{response}");
        assert_signed_head_not_found(&addr, "mybucket", "raw-exact");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn presigned_streaming_put_trailer_bodies_use_safe_client_errors() {
        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        create_test_bucket(&frontend, "mybucket");
        let (addr, _guard) = start_test_server(Arc::clone(&frontend)).await;

        let zero_signature = "0".repeat(64);
        let signed_wire = format!(
            "5;chunk-signature={zero_signature}\r\nhello\r\n\
             0;chunk-signature={zero_signature}\r\n\
             x-amz-checksum-crc32:NhCmhg==\r\n\
             x-amz-trailer-signature:{zero_signature}\r\n\r\n"
        )
        .into_bytes();
        let unsigned_wire = b"5\r\nhello\r\n0\r\nx-amz-checksum-crc32:NhCmhg==\r\n\r\n";

        for (key, payload_hash, wire) in [
            (
                "signed-trailer",
                "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER",
                signed_wire.as_slice(),
            ),
            (
                "unsigned-trailer",
                "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
                unsigned_wire.as_slice(),
            ),
        ] {
            let response = send_presigned_streaming_request(
                &addr,
                &format!("/mybucket/{key}"),
                payload_hash,
                5,
                Some("x-amz-checksum-crc32"),
                wire,
            );
            assert!(response.starts_with("HTTP/1.1 400"), "{response}");
            assert!(
                response.contains("<Code>MalformedTrailerError</Code>"),
                "{response}"
            );
            assert_signed_head_not_found(&addr, "mybucket", key);

            let raw_key = format!("{key}-raw-exact");
            let response = send_presigned_streaming_request(
                &addr,
                &format!("/mybucket/{raw_key}"),
                payload_hash,
                5,
                Some("x-amz-checksum-crc32"),
                b"hello",
            );
            assert!(response.starts_with("HTTP/1.1 400"), "{response}");
            assert!(
                response.contains("<Code>XAmzContentSHA256Mismatch</Code>"),
                "{response}"
            );
            assert!(response.contains(payload_hash), "{response}");
            assert!(!response.contains("<Resource>"), "{response}");
            assert_signed_head_not_found(&addr, "mybucket", &raw_key);

            let short_key = format!("{key}-raw-short");
            let response = send_presigned_streaming_request(
                &addr,
                &format!("/mybucket/{short_key}"),
                payload_hash,
                5,
                Some("x-amz-checksum-crc32"),
                b"hell",
            );
            assert!(response.starts_with("HTTP/1.1 400"), "{response}");
            assert!(
                response.contains("<Code>IncompleteBody</Code>"),
                "{response}"
            );
            assert!(
                response.contains("<NumberBytesExpected>5</NumberBytesExpected>"),
                "{response}"
            );
            assert!(
                response.contains("<NumberBytesProvided>4</NumberBytesProvided>"),
                "{response}"
            );
            assert_signed_head_not_found(&addr, "mybucket", &short_key);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn presigned_streaming_marker_does_not_decode_or_commit_an_upload_part() {
        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        let upload_id = create_test_bucket_and_upload(&frontend, "mybucket", "mykey");
        let (addr, _guard) = start_test_server(Arc::clone(&frontend)).await;

        let zero_signature = "0".repeat(64);
        let wire = format!(
            "5;chunk-signature={zero_signature}\r\nhello\r\n\
             0;chunk-signature={zero_signature}\r\n\r\n"
        )
        .into_bytes();
        let response = send_presigned_streaming_request(
            &addr,
            &format!("/mybucket/mykey?partNumber=1&uploadId={upload_id}"),
            "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
            5,
            None,
            &wire,
        );

        assert!(response.starts_with("HTTP/1.1 400"), "{response}");
        assert!(
            response.contains("<Code>IncompleteBody</Code>"),
            "{response}"
        );

        let signed_trailer_wire = format!(
            "5;chunk-signature={zero_signature}\r\nhello\r\n\
             0;chunk-signature={zero_signature}\r\n\
             x-amz-checksum-crc32:NhCmhg==\r\n\
             x-amz-trailer-signature:{zero_signature}\r\n\r\n"
        )
        .into_bytes();
        let unsigned_trailer_wire = b"5\r\nhello\r\n0\r\nx-amz-checksum-crc32:NhCmhg==\r\n\r\n";
        for (payload_hash, body, expected_code) in [
            (
                "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER",
                signed_trailer_wire.as_slice(),
                "MalformedTrailerError",
            ),
            (
                "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER",
                b"hello".as_slice(),
                "MalformedTrailerError",
            ),
            (
                "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER",
                b"hell".as_slice(),
                "IncompleteBody",
            ),
            (
                "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
                unsigned_trailer_wire.as_slice(),
                "MalformedTrailerError",
            ),
            (
                "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
                b"hello".as_slice(),
                "MalformedTrailerError",
            ),
            (
                "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
                b"hell".as_slice(),
                "IncompleteBody",
            ),
        ] {
            let response = send_presigned_streaming_request(
                &addr,
                &format!("/mybucket/mykey?partNumber=1&uploadId={upload_id}"),
                payload_hash,
                5,
                Some("x-amz-checksum-crc32"),
                body,
            );
            assert!(response.starts_with("HTTP/1.1 400"), "{response}");
            assert!(
                response.contains(&format!("<Code>{expected_code}</Code>")),
                "{response}"
            );
            assert_signed_list_parts_empty(&addr, "mybucket", "mykey", &upload_id);
        }
        assert_eq!(
            storage::test_support::stream_upload_session_count(&frontend.test_storage_cluster,)
                .unwrap(),
            0,
            "streaming session leaked after presigned UploadPart rejection"
        );
        assert_signed_list_parts_empty(&addr, "mybucket", "mykey", &upload_id);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn streaming_put_abort_guard_cleans_promoted_session_on_drop() {
        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        create_test_bucket(&frontend, "mybucket");
        let body = vec![b'x'; crate::coordinator::INTERNAL_SEGMENT_SIZE + 1];
        let signed = sign_headers("PUT", "/mybucket/mykey", "localhost", &body, &[]);
        let content_length = body.len().to_string();
        let req = make_s3req(
            "PUT",
            "/mybucket/mykey",
            &[
                ("host", "localhost"),
                ("authorization", &signed.authorization),
                ("x-amz-date", &signed.amz_date),
                ("x-amz-content-sha256", &signed.amz_content_sha256),
                ("content-length", &content_length),
            ],
        );
        let ctx = Arc::new(
            frontend
                .prepare_streaming_put(&req, "mybucket", "mykey", false)
                .unwrap(),
        );
        let session_id = frontend.start_streaming_put_session(&ctx).unwrap();
        let state = Arc::new(ServerState {
            pool: vec![Arc::clone(&frontend)],
            host_id: frontend.host_id.clone(),
            counter: AtomicUsize::new(0),
            request_semaphore: Arc::new(tokio::sync::Semaphore::new(8)),
            _request_admission_capacity_guard: observability::request_admission_capacity_guard(8),
            segment_buffer_pool: SegmentBufferPool::new(8),
            config: ServeConfig::default(),
            endpoint_kind: EndpointKind::S3Only,
        });

        let guard = StreamingAbortGuard::new(&state);
        guard.arm_put(&ctx, &session_id);
        drop(guard);

        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            storage::test_support::stream_upload_session_count(&frontend.test_storage_cluster,)
                .unwrap(),
            0,
            "streaming PUT abort guard left a durable stream session behind"
        );
    }

    #[test]
    fn same_epoch_renewal_does_not_extend_streaming_put_effect_authority() {
        let clock = storage::test_support::test_time_override_guard(1_000);
        let tmp = test_util::tempdir();
        let frontend = setup_dynamic_frontend(tmp.path());
        create_test_bucket(&frontend, "captured-stream-effect-deadline");
        let storage_cluster = frontend.coordinator.storage_node_for_request();
        storage_cluster
            .test_store_route_map_validity(storage::RouteMapValidity::until_ms(2_000).unwrap());

        let body = b"body completed before captured route expiry";
        let signed = sign_headers(
            "PUT",
            "/captured-stream-effect-deadline/key",
            "localhost",
            body,
            &[],
        );
        let content_length = body.len().to_string();
        let req = make_s3req(
            "PUT",
            "/captured-stream-effect-deadline/key",
            &[
                ("host", "localhost"),
                ("authorization", &signed.authorization),
                ("x-amz-date", &signed.amz_date),
                ("x-amz-content-sha256", &signed.amz_content_sha256),
                ("content-length", &content_length),
            ],
        );
        let ctx = frontend
            .prepare_streaming_put(&req, "captured-stream-effect-deadline", "key", false)
            .unwrap();
        let session_id = frontend.start_streaming_put_session(&ctx).unwrap();
        frontend
            .streaming_append_segment(&ctx, &session_id, 0, body)
            .unwrap();

        // Renew the raw runtime-map generation before advancing beyond the
        // request's immutable captured deadline. The completed body must not
        // be committed through the renewed lease.
        storage_cluster
            .test_store_route_map_validity(storage::RouteMapValidity::until_ms(5_000).unwrap());
        clock.set(2_000);

        assert!(matches!(
            frontend.heartbeat_streaming_put_object(&ctx, &session_id),
            Err(ServerError::SlowDown)
        ));
        assert!(matches!(
            frontend.streaming_append_segment(&ctx, &session_id, 1, b"late"),
            Err(ServerError::SlowDown)
        ));
        assert!(matches!(
            frontend.put_single_segment_object(&ctx, body, &[]),
            Err(ServerError::SlowDown)
        ));
        assert!(matches!(
            frontend.finalize_streaming_put(
                &ctx,
                &session_id,
                checksum::crc64::checksum(body),
                body.len() as u64,
                &[],
            ),
            Err(ServerError::SlowDown)
        ));
        assert!(storage_cluster
            .load_stream_upload_session(ctx.bucket(), ctx.key(), &session_id,)
            .is_ok());

        frontend.abort_streaming_put(&ctx, &session_id);
        assert!(matches!(
            storage_cluster
                .load_stream_upload_session(ctx.bucket(), ctx.key(), &session_id)
                .map_err(|error| error.kind()),
            Err(storage::StreamUploadFailureKind::SessionNotFound)
        ));
    }

    #[test]
    fn same_epoch_renewal_does_not_extend_post_or_upload_part_effect_authority() {
        let clock = storage::test_support::test_time_override_guard(1_000);
        let tmp = test_util::tempdir();
        let frontend = setup_dynamic_frontend(tmp.path());
        create_test_bucket(&frontend, "captured-post-effect-deadline");
        let storage_cluster = frontend.coordinator.storage_node_for_request();
        storage_cluster
            .test_store_route_map_validity(storage::RouteMapValidity::until_ms(2_000).unwrap());

        let post_body = b"POST body completed before captured route expiry";
        let post_req = make_s3req(
            "POST",
            "/captured-post-effect-deadline",
            &[("host", "localhost")],
        );
        let fields = sign_post_policy_fields("captured-post-effect-deadline", "key", &[], &[]);
        let post_ctx = frontend
            .prepare_streaming_post_object(
                &post_req,
                "captured-post-effect-deadline",
                &fields,
                Some("upload.txt"),
            )
            .unwrap();
        frontend
            .streaming_append_post_segment(&post_ctx, 0, post_body)
            .unwrap();
        storage_cluster
            .test_store_route_map_validity(storage::RouteMapValidity::until_ms(5_000).unwrap());
        clock.set(2_000);

        assert!(matches!(
            frontend.streaming_append_post_segment(&post_ctx, 1, b"late"),
            Err(ServerError::SlowDown)
        ));
        assert!(matches!(
            frontend.finalize_streaming_post_object(
                &post_ctx,
                checksum::crc64::checksum(post_body),
                post_body.len() as u64,
                None,
            ),
            Err(ServerError::SlowDown)
        ));
        frontend.abort_streaming_post_object(&post_ctx);
        drop(post_ctx);

        clock.set(1_000);
        let upload_id =
            create_test_bucket_and_upload(&frontend, "captured-part-effect-deadline", "key");
        storage_cluster
            .test_store_route_map_validity(storage::RouteMapValidity::until_ms(2_000).unwrap());
        let part_body = b"UploadPart body completed before captured route expiry";
        let uri = format!("/captured-part-effect-deadline/key?partNumber=1&uploadId={upload_id}");
        let signed = sign_headers("PUT", &uri, "localhost", part_body, &[]);
        let part_req = make_s3req(
            "PUT",
            &uri,
            &[
                ("host", "localhost"),
                ("authorization", &signed.authorization),
                ("x-amz-date", &signed.amz_date),
                ("x-amz-content-sha256", &signed.amz_content_sha256),
            ],
        );
        let part_ctx = frontend
            .prepare_streaming_part(
                &part_req,
                "captured-part-effect-deadline",
                "key",
                &upload_id,
                "1",
            )
            .unwrap();
        frontend
            .streaming_append_part_segment(&part_ctx, 0, part_body)
            .unwrap();
        storage_cluster
            .test_store_route_map_validity(storage::RouteMapValidity::until_ms(5_000).unwrap());
        clock.set(2_000);

        assert!(matches!(
            frontend.streaming_append_part_segment(&part_ctx, 1, b"late"),
            Err(ServerError::SlowDown)
        ));
        assert!(matches!(
            frontend.finalize_streaming_part(
                &part_ctx,
                checksum::crc64::checksum(part_body),
                part_body.len() as u64,
                &[],
                None,
            ),
            Err(ServerError::SlowDown)
        ));
        frontend.abort_streaming_part(&part_ctx);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn streaming_put_abort_guard_cleans_session_created_after_request_drop() {
        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        create_test_bucket(&frontend, "mybucket");
        let body = vec![b'x'; crate::coordinator::INTERNAL_SEGMENT_SIZE + 1];
        let signed = sign_headers("PUT", "/mybucket/mykey", "localhost", &body, &[]);
        let content_length = body.len().to_string();
        let req = make_s3req(
            "PUT",
            "/mybucket/mykey",
            &[
                ("host", "localhost"),
                ("authorization", &signed.authorization),
                ("x-amz-date", &signed.amz_date),
                ("x-amz-content-sha256", &signed.amz_content_sha256),
                ("content-length", &content_length),
            ],
        );
        let ctx = Arc::new(
            frontend
                .prepare_streaming_put(&req, "mybucket", "mykey", false)
                .unwrap(),
        );
        let state = Arc::new(ServerState {
            pool: vec![Arc::clone(&frontend)],
            host_id: frontend.host_id.clone(),
            counter: AtomicUsize::new(0),
            request_semaphore: Arc::new(tokio::sync::Semaphore::new(8)),
            _request_admission_capacity_guard: observability::request_admission_capacity_guard(8),
            segment_buffer_pool: SegmentBufferPool::new(8),
            config: ServeConfig::default(),
            endpoint_kind: EndpointKind::S3Only,
        });

        let guard = StreamingAbortGuard::new(&state);
        let worker_guard = Arc::clone(&guard);
        let worker_frontend = Arc::clone(&frontend);
        let worker_ctx = Arc::clone(&ctx);
        let join = tokio::task::spawn_blocking(move || {
            let session_id = worker_frontend
                .start_streaming_put_session(&worker_ctx)
                .unwrap();
            worker_guard.arm_put(&worker_ctx, &session_id);
            session_id
        });
        drop(guard);

        let session_id = join.await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            storage::test_support::stream_upload_session_count(&frontend.test_storage_cluster,)
                .unwrap(),
            0,
            "streaming PUT abort guard left session {session_id:?} after request-side drop"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn streaming_post_abort_guard_cleans_session_created_after_request_drop() {
        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        create_test_bucket(&frontend, "mybucket");
        let req = make_s3req("POST", "/mybucket", &[("host", "localhost")]);
        let fields = sign_post_policy_fields("mybucket", "mykey", &[], &[]);
        let state = Arc::new(ServerState {
            pool: vec![Arc::clone(&frontend)],
            host_id: frontend.host_id.clone(),
            counter: AtomicUsize::new(0),
            request_semaphore: Arc::new(tokio::sync::Semaphore::new(8)),
            _request_admission_capacity_guard: observability::request_admission_capacity_guard(8),
            segment_buffer_pool: SegmentBufferPool::new(8),
            config: ServeConfig::default(),
            endpoint_kind: EndpointKind::S3Only,
        });

        let guard = StreamingAbortGuard::new(&state);
        let worker_guard = Arc::clone(&guard);
        let worker_frontend = Arc::clone(&frontend);
        let join = tokio::task::spawn_blocking(move || {
            let ctx = worker_frontend
                .prepare_streaming_post_object(&req, "mybucket", &fields, Some("upload.txt"))
                .unwrap();
            let ctx = Arc::new(ctx);
            worker_guard.arm_post(&ctx);
            (
                ctx.session_id().clone(),
                worker_guard.put_heartbeat_started.load(Ordering::Acquire),
            )
        });
        drop(guard);

        let (session_id, heartbeat_started) = join.await.unwrap();
        assert!(
            heartbeat_started,
            "streaming POST must renew the PutObject stream proof while the request is active"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            storage::test_support::stream_upload_session_count(&frontend.test_storage_cluster,)
                .unwrap(),
            0,
            "streaming POST abort guard left session {session_id:?} after request-side drop"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn streaming_part_abort_guard_cleans_session_created_after_request_drop() {
        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        let upload_id = create_test_bucket_and_upload(&frontend, "mybucket", "mykey");
        let uri = format!("/mybucket/mykey?partNumber=1&uploadId={upload_id}");
        let signed = sign_headers("PUT", &uri, "localhost", b"", &[]);
        let req = make_s3req(
            "PUT",
            &uri,
            &[
                ("host", "localhost"),
                ("authorization", &signed.authorization),
                ("x-amz-date", &signed.amz_date),
                ("x-amz-content-sha256", &signed.amz_content_sha256),
            ],
        );
        let state = Arc::new(ServerState {
            pool: vec![Arc::clone(&frontend)],
            host_id: frontend.host_id.clone(),
            counter: AtomicUsize::new(0),
            request_semaphore: Arc::new(tokio::sync::Semaphore::new(8)),
            _request_admission_capacity_guard: observability::request_admission_capacity_guard(8),
            segment_buffer_pool: SegmentBufferPool::new(8),
            config: ServeConfig::default(),
            endpoint_kind: EndpointKind::S3Only,
        });

        let guard = StreamingAbortGuard::new(&state);
        let worker_guard = Arc::clone(&guard);
        let worker_frontend = Arc::clone(&frontend);
        let upload_id_for_worker = upload_id.clone();
        let join = tokio::task::spawn_blocking(move || {
            let ctx = worker_frontend
                .prepare_streaming_part(&req, "mybucket", "mykey", &upload_id_for_worker, "1")
                .unwrap();
            let ctx = Arc::new(ctx);
            worker_guard.arm_part(&ctx);
            ctx.session_id().clone()
        });
        drop(guard);

        let session_id = join.await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            storage::test_support::stream_upload_session_count(&frontend.test_storage_cluster,)
                .unwrap(),
            0,
            "streaming UploadPart abort guard left session {session_id:?} after request-side drop"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn denied_streaming_put_responds_before_full_body_is_sent() {
        const TOTAL_BODY_BYTES: usize = 256 * 1024;

        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        create_test_bucket(&frontend, "mybucket");
        let (addr, _guard) = start_test_server(Arc::clone(&frontend)).await;

        let request = format!(
            "PUT /mybucket/mykey HTTP/1.1\r\n\
Host: {addr}\r\n\
Content-Length: {TOTAL_BODY_BYTES}\r\n\
Connection: keep-alive\r\n\r\n"
        );
        let (response, bytes_sent) =
            denied_streaming_request_response(&addr, request, TOTAL_BODY_BYTES);

        assert!(
            response.starts_with("HTTP/1.1 403"),
            "expected 403 status, got: {}",
            response.lines().next().unwrap_or("")
        );
        assert!(
            response.contains("<Code>AccessDenied</Code>"),
            "expected AccessDenied body, got: {response}"
        );
        assert!(
            response.to_ascii_lowercase().contains("connection: close"),
            "expected Connection: close header, got: {response}"
        );
        assert!(
            bytes_sent < TOTAL_BODY_BYTES,
            "server read the full denied PUT body before responding: sent {bytes_sent} bytes"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn denied_streaming_upload_part_responds_before_full_body_is_sent() {
        const TOTAL_BODY_BYTES: usize = 256 * 1024;

        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        let upload_id = create_test_bucket_and_upload(&frontend, "mybucket", "mykey");
        let (addr, _guard) = start_test_server(Arc::clone(&frontend)).await;

        let request = format!(
            "PUT /mybucket/mykey?partNumber=1&uploadId={upload_id} HTTP/1.1\r\n\
Host: {addr}\r\n\
Content-Length: {TOTAL_BODY_BYTES}\r\n\
Connection: keep-alive\r\n\r\n"
        );
        let (response, bytes_sent) =
            denied_streaming_request_response(&addr, request, TOTAL_BODY_BYTES);

        assert!(
            response.starts_with("HTTP/1.1 403"),
            "expected 403 status, got: {}",
            response.lines().next().unwrap_or("")
        );
        assert!(
            response.contains("<Code>AccessDenied</Code>"),
            "expected AccessDenied body, got: {response}"
        );
        assert!(
            response.to_ascii_lowercase().contains("connection: close"),
            "expected Connection: close header, got: {response}"
        );
        assert!(
            bytes_sent < TOTAL_BODY_BYTES,
            "server read the full denied UploadPart body before responding: sent {bytes_sent} bytes"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn put_multipart_upload_without_part_number_rejects_large_partial_body_immediately() {
        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        let upload_id = create_test_bucket_and_upload(&frontend, "mybucket", "mykey");
        let (addr, _guard) = start_test_server(Arc::clone(&frontend)).await;
        let declared_body_bytes = MAX_BUFFERED_CONTROL_BODY_SIZE + 1;

        for copy_source_header in ["", "x-amz-copy-source: mybucket/source\r\n"] {
            let mut stream = StdTcpStream::connect(&addr).unwrap();
            stream
                .write_all(
                    format!(
                        "PUT /mybucket/mykey?uploadId={upload_id} HTTP/1.1\r\n\
                         Host: {addr}\r\n\
                         {copy_source_header}\
                         Content-Length: {declared_body_bytes}\r\n\
                         Connection: keep-alive\r\n\r\n"
                    )
                    .as_bytes(),
                )
                .unwrap();
            stream.write_all(b"x").unwrap();

            // This URI-level rejection does not consume the declared body.
            // Keep the client write side open until the server's explicit
            // Connection: close response is complete; half-closing as soon as
            // headers arrive can make Hyper treat the incomplete request as a
            // disconnect while it is still delivering the chunked XML body.
            // Receiving the complete response first still proves routing did
            // not wait for the declared body, its size limit, or the body idle
            // timeout.
            let response = read_http_response(&mut stream, Duration::from_secs(3));
            let _ = stream.shutdown(Shutdown::Write);

            assert!(response.starts_with("HTTP/1.1 405"), "{response}");
            assert!(
                response.to_ascii_lowercase().contains("connection: close"),
                "{response}"
            );
            assert!(
                response.contains("<Code>MethodNotAllowed</Code>"),
                "{response}"
            );
            assert!(response.contains("<Method>PUT</Method>"), "{response}");
            assert!(
                response.contains("<ResourceType>UPLOAD</ResourceType>"),
                "{response}"
            );
            assert!(
                !response.contains("MaxMessageLengthExceeded"),
                "request shape must win over buffered body size: {response}"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn options_preflight_responds_before_full_body_is_sent() {
        const TOTAL_BODY_BYTES: usize = 256 * 1024;

        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        create_test_bucket(&frontend, "mybucket");
        let (addr, _guard) = start_test_server(Arc::clone(&frontend)).await;

        let request = format!(
            "OPTIONS /mybucket HTTP/1.1\r\n\
Host: {addr}\r\n\
Origin: http://example.com\r\n\
Access-Control-Request-Method: GET\r\n\
Content-Length: {TOTAL_BODY_BYTES}\r\n\
Connection: keep-alive\r\n\r\n"
        );
        let (response, bytes_sent) =
            response_before_request_body_sent(&addr, request, TOTAL_BODY_BYTES);

        assert!(
            response.starts_with("HTTP/1.1 403"),
            "expected 403 status, got: {}",
            response.lines().next().unwrap_or("")
        );
        assert!(
            !response.contains("MaxMessageLengthExceeded"),
            "OPTIONS preflight should not reject based on ignored body size: {response}"
        );
        assert!(
            response.to_ascii_lowercase().contains("connection: close"),
            "OPTIONS with an unread request body must close after its response: {response}"
        );
        assert!(
            bytes_sent < TOTAL_BODY_BYTES,
            "server read the full OPTIONS body before responding: sent {bytes_sent} bytes"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn admission_timeout_delivers_complete_response_before_closing_unread_body() {
        const TOTAL_BODY_BYTES: usize = 256 * 1024;

        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        let config = ServeConfig {
            request_wait_timeout: Duration::from_millis(20),
            ..ServeConfig::default()
        };
        let (addr, _guard) = start_test_server_with_config(frontend, config, 0).await;
        let request = format!(
            "PUT /admission-timeout-bucket/key HTTP/1.1\r\n\
Host: {addr}\r\n\
Content-Length: {TOTAL_BODY_BYTES}\r\n\
Connection: keep-alive\r\n\r\n"
        );

        let (response, bytes_sent) =
            response_before_request_body_sent(&addr, request, TOTAL_BODY_BYTES);

        assert!(response.starts_with("HTTP/1.1 503"), "{response}");
        assert!(response.contains("<Code>SlowDown</Code>"), "{response}");
        assert!(
            response.to_ascii_lowercase().contains("connection: close"),
            "admission timeout with an unread body must close after its response: {response}"
        );
        assert!(
            bytes_sent < TOTAL_BODY_BYTES,
            "admission timeout consumed the full request body: sent {bytes_sent} bytes"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn buffered_body_limit_error_delivers_complete_response_before_closing() {
        const TOTAL_BODY_BYTES: usize = MAX_CORS_CONFIGURATION_BYTES + (64 * 1024);

        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        let (addr, _guard) = start_test_server(frontend).await;
        let request = format!(
            "PUT /oversized-cors-bucket?cors HTTP/1.1\r\n\
Host: {addr}\r\n\
Content-Length: {TOTAL_BODY_BYTES}\r\n\
Connection: keep-alive\r\n\r\n"
        );

        let (response, bytes_sent) =
            response_before_request_body_sent(&addr, request, TOTAL_BODY_BYTES);

        assert!(response.starts_with("HTTP/1.1 400"), "{response}");
        assert!(
            response.contains("<Code>MaxMessageLengthExceeded</Code>"),
            "{response}"
        );
        assert!(
            response.to_ascii_lowercase().contains("connection: close"),
            "body-limit failure must close after its response: {response}"
        );
        assert!(
            bytes_sent < TOTAL_BODY_BYTES,
            "body-limit failure consumed the full request body: sent {bytes_sent} bytes"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn streaming_put_missing_content_sha256_closes_before_full_body_is_sent() {
        const TOTAL_BODY_BYTES: usize = 256 * 1024;

        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        create_test_bucket(&frontend, "mybucket");
        let (addr, _guard) = start_test_server(Arc::clone(&frontend)).await;

        let body = vec![b'x'; TOTAL_BODY_BYTES];
        let signed = sign_headers("PUT", "/mybucket/mykey", &addr, &body, &[]);
        let request = format!(
            "PUT /mybucket/mykey HTTP/1.1\r\n\
Host: {addr}\r\n\
Authorization: {}\r\n\
x-amz-date: {}\r\n\
Content-Length: {TOTAL_BODY_BYTES}\r\n\
Connection: keep-alive\r\n\r\n",
            signed.authorization, signed.amz_date
        );
        let (response, bytes_sent) =
            denied_streaming_request_response(&addr, request, TOTAL_BODY_BYTES);

        assert!(
            response.starts_with("HTTP/1.1 400"),
            "expected 400 status, got: {}",
            response.lines().next().unwrap_or("")
        );
        assert!(
            response.contains("Missing required header for this request: x-amz-content-sha256"),
            "expected missing x-amz-content-sha256 body, got: {response}"
        );
        assert!(
            response.to_ascii_lowercase().contains("connection: close"),
            "expected Connection: close header, got: {response}"
        );
        assert!(
            bytes_sent < TOTAL_BODY_BYTES,
            "server read the full missing-sha256 PUT body before responding: sent {bytes_sent} bytes"
        );
        assert_eq!(
            storage::test_support::stream_upload_session_count(&frontend.test_storage_cluster,)
                .unwrap(),
            0,
            "missing-sha256 PUT should not create a stream session"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn streaming_put_object_lock_without_checksum_closes_before_full_body_is_sent() {
        const TOTAL_BODY_BYTES: usize = 256 * 1024;

        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        create_test_bucket(&frontend, "mybucket");
        let (addr, _guard) = start_test_server(Arc::clone(&frontend)).await;

        let body = vec![b'x'; TOTAL_BODY_BYTES];
        let signed = sign_headers(
            "PUT",
            "/mybucket/mykey",
            &addr,
            &body,
            &[
                ("x-amz-object-lock-mode", "COMPLIANCE"),
                (
                    "x-amz-object-lock-retain-until-date",
                    "2099-01-01T00:00:00Z",
                ),
            ],
        );
        let request = format!(
            "PUT /mybucket/mykey HTTP/1.1\r\n\
Host: {addr}\r\n\
Authorization: {}\r\n\
x-amz-date: {}\r\n\
x-amz-content-sha256: {}\r\n\
x-amz-object-lock-mode: COMPLIANCE\r\n\
x-amz-object-lock-retain-until-date: 2099-01-01T00:00:00Z\r\n\
Content-Length: {TOTAL_BODY_BYTES}\r\n\
Connection: keep-alive\r\n\r\n",
            signed.authorization, signed.amz_date, signed.amz_content_sha256
        );
        let (response, bytes_sent) =
            denied_streaming_request_response(&addr, request, TOTAL_BODY_BYTES);

        assert!(
            response.starts_with("HTTP/1.1 400"),
            "expected 400 status, got: {}",
            response.lines().next().unwrap_or("")
        );
        assert!(
            response.contains(
                "Content-MD5 OR x-amz-checksum- HTTP header is required for Put Object requests with Object Lock parameters"
            ),
            "expected object-lock checksum requirement body, got: {response}"
        );
        assert!(
            response.to_ascii_lowercase().contains("connection: close"),
            "expected Connection: close header, got: {response}"
        );
        assert!(
            bytes_sent < TOTAL_BODY_BYTES,
            "server read the full object-lock checksum failure PUT body before responding: sent {bytes_sent} bytes"
        );
        assert_eq!(
            storage::test_support::stream_upload_session_count(&frontend.test_storage_cluster,)
                .unwrap(),
            0,
            "object-lock checksum failure PUT should not create a stream session"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn denied_streaming_post_policy_closes_before_full_body_is_sent() {
        const TOTAL_FILE_BYTES: usize = 256 * 1024;

        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        create_test_bucket(&frontend, "mybucket");
        let (addr, _guard) = start_test_server(Arc::clone(&frontend)).await;

        let fields = sign_post_policy_fields(
            "mybucket",
            "mykey",
            &[r#"{"Content-Type":"text/plain"}"#],
            &[("Content-Type", "image/png")],
        );
        let (content_type, prefix, suffix) = build_streaming_multipart_parts(&fields, "test.txt");
        let content_length = prefix.len() + TOTAL_FILE_BYTES + suffix.len();
        let request = format!(
            "POST /mybucket HTTP/1.1\r\n\
Host: {addr}\r\n\
Content-Type: {content_type}\r\n\
Content-Length: {content_length}\r\n\
Connection: keep-alive\r\n\r\n"
        );
        let (response, bytes_sent) =
            denied_streaming_multipart_request_response(&addr, request, prefix, TOTAL_FILE_BYTES);

        assert!(
            response.starts_with("HTTP/1.1 403"),
            "expected 403 status, got: {}",
            response.lines().next().unwrap_or("")
        );
        assert!(
            response.contains("<Code>AccessDenied</Code>"),
            "expected AccessDenied body, got: {response}"
        );
        assert!(
            response.to_ascii_lowercase().contains("connection: close"),
            "expected Connection: close header, got: {response}"
        );
        assert!(
            bytes_sent < TOTAL_FILE_BYTES,
            "server read the full denied POST body before responding: sent {bytes_sent} bytes"
        );
        assert_eq!(
            storage::test_support::stream_upload_session_count(&frontend.test_storage_cluster,)
                .unwrap(),
            0,
            "policy-denied POST should not create a stream session"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn streaming_post_content_length_range_max_abort_closes_before_full_body_is_sent() {
        const TOTAL_FILE_BYTES: usize = 256 * 1024;

        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        create_test_bucket(&frontend, "mybucket");
        let (addr, _guard) = start_test_server(Arc::clone(&frontend)).await;

        let fields = sign_post_policy_fields(
            "mybucket",
            "mykey",
            &[r#"["content-length-range",0,1024]"#],
            &[],
        );
        let (content_type, prefix, suffix) = build_streaming_multipart_parts(&fields, "test.txt");
        let content_length = prefix.len() + TOTAL_FILE_BYTES + suffix.len();
        let request = format!(
            "POST /mybucket HTTP/1.1\r\n\
Host: {addr}\r\n\
Content-Type: {content_type}\r\n\
Content-Length: {content_length}\r\n\
Connection: keep-alive\r\n\r\n"
        );
        let (response, bytes_sent) =
            denied_streaming_multipart_request_response(&addr, request, prefix, TOTAL_FILE_BYTES);

        assert!(
            response.starts_with("HTTP/1.1 400"),
            "expected 400 status, got: {}",
            response.lines().next().unwrap_or("")
        );
        assert!(
            response.contains("content-length-range"),
            "expected content-length-range failure body, got: {response}"
        );
        assert!(
            response.to_ascii_lowercase().contains("connection: close"),
            "expected Connection: close header, got: {response}"
        );
        assert!(
            bytes_sent < TOTAL_FILE_BYTES,
            "server read the full over-max POST body before responding: sent {bytes_sent} bytes"
        );
        assert_eq!(
            storage::test_support::stream_upload_session_count(&frontend.test_storage_cluster,)
                .unwrap(),
            0,
            "over-max POST should not leak a stream session"
        );
    }
}
