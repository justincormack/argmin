// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::{Coordinator, INTERNAL_SEGMENT_SIZE};
    use auth::canonical::{
        canonical_headers, canonical_query_string, canonical_request, sha256_hex, string_to_sign,
    };
    use auth::SecretKey;
    use ring::hmac;
    use server_core::sse::{ManagedWrappingKeyConfig, StaticManagedKeyProvider};
    use std::sync::Arc;

    const TEST_SIGV4_ACCESS_KEY: &str = "AKID";
    const TEST_SIGV4_SECRET: &str = "secret";
    const TEST_SSE_S3_WRAPPING_KEY_B64: &str = "YWJjZGVmMDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODk=";

    struct FailingIdentityProvider(auth::IdentityProviderError);

    impl auth::IdentityProviderBackend for FailingIdentityProvider {
        fn lookup_long_lived_credential(
            &self,
            _access_key_id: &str,
        ) -> Result<Option<Arc<auth::StoredCredential>>, auth::IdentityProviderError> {
            Err(self.0)
        }

        fn lookup_live_role_identity(
            &self,
            _stable_role_id: &auth::StableRoleId,
        ) -> Result<Option<Arc<auth::LiveRoleIdentity>>, auth::IdentityProviderError> {
            Err(self.0)
        }

        fn lookup_role_authorization(
            &self,
            _stable_role_id: &auth::StableRoleId,
        ) -> Result<Option<Arc<auth::RoleAuthorizationRecord>>, auth::IdentityProviderError>
        {
            Err(self.0)
        }

        fn lookup_role_authorization_by_arn(
            &self,
            _role_arn: &auth::IamRoleArn,
        ) -> Result<Option<Arc<auth::RoleAuthorizationRecord>>, auth::IdentityProviderError>
        {
            Err(self.0)
        }

        fn lookup_configured_principal_authorization(
            &self,
            _key: &auth::ConfiguredPrincipalAuthorizationKey,
        ) -> Result<
            Option<Arc<auth::ConfiguredPrincipalAuthorizationRecord>>,
            auth::IdentityProviderError,
        > {
            Err(self.0)
        }

        fn find_account_by_canonical_user_id(
            &self,
            _canonical_user_id: &s3_types::CanonicalUserId,
        ) -> Result<Option<auth::AccountIdentity>, auth::IdentityProviderError> {
            Err(self.0)
        }
    }

    static EXPECTED_PANIC_ON_500_HOOK: std::sync::Once = std::sync::Once::new();

    struct SuppressExpectedPanicOn500Diagnostics {
        previous_suppressed: bool,
    }

    impl SuppressExpectedPanicOn500Diagnostics {
        fn new() -> Self {
            EXPECTED_PANIC_ON_500_HOOK.call_once(|| {
                let previous_hook = std::panic::take_hook();
                std::panic::set_hook(Box::new(move |panic_info| {
                    if SUPPRESS_EXPECTED_PANIC_ON_500_DIAGNOSTICS.with(std::cell::Cell::get) {
                        return;
                    }
                    previous_hook(panic_info);
                }));
            });
            let previous_suppressed =
                SUPPRESS_EXPECTED_PANIC_ON_500_DIAGNOSTICS.with(|suppressed| {
                    let previous = suppressed.get();
                    suppressed.set(true);
                    previous
                });
            Self {
                previous_suppressed,
            }
        }
    }

    impl Drop for SuppressExpectedPanicOn500Diagnostics {
        fn drop(&mut self) {
            SUPPRESS_EXPECTED_PANIC_ON_500_DIAGNOSTICS
                .with(|suppressed| suppressed.set(self.previous_suppressed));
        }
    }

    fn setup_frontend(dir: &std::path::Path) -> HttpFrontend {
        setup_frontend_with_sse_s3(dir)
    }

    fn setup_frontend_with_sse_s3(dir: &std::path::Path) -> HttpFrontend {
        let storage_cluster = storage::test_support::open_default_test_storage_cluster(dir);
        let sse_s3_provider = StaticManagedKeyProvider::single(
            ManagedWrappingKeyConfig::from_base64(1, TEST_SSE_S3_WRAPPING_KEY_B64).unwrap(),
        );
        let coordinator = Coordinator::new_with_managed_key_provider_for_storage_cluster(
            Arc::clone(&storage_cluster),
            "us-east-1".to_string(),
            None,
            sse_s3_provider,
        )
        .unwrap();
        let mut credentials = auth::CredentialStore::new();
        credentials
            .add(
                TEST_SIGV4_ACCESS_KEY.to_string(),
                SecretKey::new(TEST_SIGV4_SECRET.to_string()),
            )
            .unwrap();
        HttpFrontend {
            coordinator: Arc::new(coordinator),
            identity_provider: auth::IdentityProvider::in_memory(credentials)
                .expect("initialize session-token key ring"),
            host_id: Arc::<str>::from("host-id"),
            test_storage_cluster: storage_cluster,
            actual_cors_metadata_lookup_count: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    #[test]
    fn identity_provider_failure_fails_authentication_and_rendering_closed() {
        let tmp = test_util::tempdir();
        let mut frontend = setup_frontend(tmp.path());
        frontend.identity_provider = auth::IdentityProvider::new(FailingIdentityProvider(
            auth::IdentityProviderError::Unavailable,
        ))
        .expect("initialize session-token key ring");

        let request = signed_v4_put_req(b"", Vec::new());
        assert!(matches!(
            frontend.authenticate(&request, None),
            Err(ServerError::IdentityProvider(
                auth::IdentityProviderError::Unavailable
            ))
        ));

        let canonical_id = s3_types::CanonicalUserId::from_principal("owner");
        assert!(matches!(
            frontend.acl_owner_display_name("owner", &canonical_id),
            Err(ServerError::IdentityProvider(
                auth::IdentityProviderError::Unavailable
            ))
        ));

        frontend.identity_provider = auth::IdentityProvider::new(FailingIdentityProvider(
            auth::IdentityProviderError::InvalidRecord,
        ))
        .expect("initialize session-token key ring");
        assert!(matches!(
            frontend.authenticate(&request, None),
            Err(ServerError::IdentityProvider(
                auth::IdentityProviderError::InvalidRecord
            ))
        ));
    }

    fn configured_identity(account: auth::AccountIdentity) -> auth::AuthenticatedIdentity {
        let principal = auth::ConfiguredPrincipalIdentity::new(account.principal());
        auth::AuthenticatedIdentity::configured(account, principal)
    }

    fn test_auth() -> auth::AuthContext {
        auth::AuthContext {
            mode: auth::AuthMode::HeaderSigV4,
            access_key_id: Some("AKID".to_string()),
            identity: Some(configured_identity(auth::AccountIdentity::from_principal(
                "testuser",
            ))),
            authorization_profile: auth::AuthorizationProfile::Standard,
            request_epoch_secs: Some(0),
            signing_region: Some("us-east-1".to_string()),
            streaming: None,
        }
    }

    #[test]
    fn put_object_policy_context_stores_if_match_entity_tag() {
        let policy_context =
            put_object_policy_context_from_request_fields(PutObjectPolicyContextFields {
                conditions: PutObjectConditionalHeaders {
                    if_match: Some(if_match_header_entity_tag_value("\"abcdef1234567890\"")),
                    if_none_match: None,
                },
                ..Default::default()
            });

        assert_eq!(policy_context.if_match, Some("abcdef1234567890"));
    }

    fn create_test_bucket(coord: &Coordinator, name: &str) {
        coord
            .create_bucket(&crate::coordinator::CreateBucketRequest {
                name: parse_bucket_name(name).unwrap(),
                requester: crate::coordinator::test_helpers::requester("testuser"),
                namespace: BucketNamespace::Global,
                acl: crate::coordinator::CreateBucketAcl::DefaultPrivate,
                ownership: crate::coordinator::BucketObjectOwnership::ObjectWriter,
                object_lock_enabled: false,
            })
            .unwrap();
    }

    fn create_sigv4_test_bucket(coord: &Coordinator, name: &str, object_lock_enabled: bool) {
        coord
            .create_bucket(&crate::coordinator::CreateBucketRequest {
                name: parse_bucket_name(name).unwrap(),
                requester: crate::coordinator::test_helpers::requester(TEST_SIGV4_ACCESS_KEY),
                namespace: BucketNamespace::Global,
                acl: crate::coordinator::CreateBucketAcl::DefaultPrivate,
                ownership: crate::coordinator::BucketObjectOwnership::ObjectWriter,
                object_lock_enabled,
            })
            .unwrap();
    }

    fn test_bucket_name(name: &str) -> BucketName {
        parse_bucket_name(name).unwrap()
    }

    #[allow(clippy::format_collect)]
    fn hex_lower(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn current_sigv4_timestamp() -> (String, String) {
        let now_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("current time should be after epoch")
            .as_millis() as u64;
        let timestamp = xml::format_timestamp(now_millis);
        let date = format!(
            "{}{}{}",
            &timestamp[0..4],
            &timestamp[5..7],
            &timestamp[8..10]
        );
        let amz_date = format!(
            "{}{}{}T{}{}{}Z",
            &timestamp[0..4],
            &timestamp[5..7],
            &timestamp[8..10],
            &timestamp[11..13],
            &timestamp[14..16],
            &timestamp[17..19]
        );
        (date, amz_date)
    }

    fn header_auth_request(
        method: http::Method,
        path: &str,
        access_key_id: &str,
        service: &str,
        security_tokens: &[&str],
    ) -> S3Request {
        let (date, amz_date) = current_sigv4_timestamp();
        let mut headers = vec![
            (
                "host".to_string(),
                "mybucket.s3.us-east-1.amazonaws.com".to_string(),
            ),
            ("x-amz-date".to_string(), amz_date),
        ];
        headers.extend(
            security_tokens
                .iter()
                .map(|token| ("x-amz-security-token".to_string(), (*token).to_string())),
        );
        let signed_headers = if security_tokens.is_empty() {
            "host;x-amz-date"
        } else {
            "host;x-amz-date;x-amz-security-token"
        };
        headers.push((
            "authorization".to_string(),
            format!(
                "AWS4-HMAC-SHA256 Credential={access_key_id}/{date}/us-east-1/{service}/aws4_request, SignedHeaders={signed_headers}, Signature={}",
                "0".repeat(64)
            ),
        ));
        new_req(method, path, "", headers, Vec::new())
    }

    fn presigned_auth_request(
        path: &str,
        access_key_id: &str,
        region: &str,
        service: &str,
    ) -> S3Request {
        let (date, amz_date) = current_sigv4_timestamp();
        let query = format!(
            "X-Amz-Algorithm=AWS4-HMAC-SHA256&\
             X-Amz-Credential={access_key_id}%2F{date}%2F{region}%2F{service}%2Faws4_request&\
             X-Amz-Date={amz_date}&\
             X-Amz-Expires=900&\
             X-Amz-SignedHeaders=host&\
             X-Amz-Signature={}",
            "0".repeat(64)
        );
        new_req(
            http::Method::GET,
            path,
            &query,
            vec![(
                "host".to_string(),
                "mybucket.s3.us-east-1.amazonaws.com".to_string(),
            )],
            Vec::new(),
        )
    }

    struct UnsupportedIdentityCredential {
        access_key_id: String,
        secret_key: SecretKey,
        token: String,
    }

    fn install_unsupported_identity_credential(
        frontend: &mut HttpFrontend,
    ) -> UnsupportedIdentityCredential {
        let stable_role_id = auth::StableRoleId::new("ARGR0123456789ABCDEFGHIJ").unwrap();
        let role = auth::IamRoleIdentity::new(
            auth::AwsAccountId::new("123456789012").unwrap(),
            stable_role_id.clone(),
            auth::RoleName::new("unsupported-role").unwrap(),
            auth::IamPath::new("/test/").unwrap(),
        );
        let live_role = auth::LiveRoleIdentity::new(
            s3_types::AccountIdentity::new(
                "123456789012",
                s3_types::CanonicalUserId::from_principal("123456789012"),
                "test account",
            ),
            role,
        )
        .unwrap();
        let mut roles = auth::RoleIdentityStore::new();
        roles.add(live_role).unwrap();
        frontend.identity_provider =
            auth::IdentityProvider::in_memory_with_roles(auth::CredentialStore::new(), roles)
                .unwrap();
        let issuer = frontend
            .identity_provider
            .lookup_live_role_identity(&stable_role_id)
            .unwrap()
            .unwrap();
        let material = auth::generate_session_credential_material().unwrap();
        let access_key_id = material.access_key_id().to_string();
        let secret_key = material.secret_key().clone();
        let now = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        )
        .unwrap();
        let token = frontend
            .identity_provider
            .seal_session_credential(
                material,
                &issuer,
                auth::RoleSessionName::new("unsupported-session").unwrap(),
                auth::SessionLifetime::new(now - 60, now + 3_600).unwrap(),
                None,
            )
            .unwrap();
        UnsupportedIdentityCredential {
            access_key_id,
            secret_key,
            token,
        }
    }

    fn signed_v4_req_for_path_with_credentials(
        method: http::Method,
        path: &str,
        body: &[u8],
        extra_headers: Vec<(String, String)>,
        access_key_id: &str,
        secret_key: &SecretKey,
        security_token: Option<&str>,
    ) -> S3Request {
        let (date, amz_date) = current_sigv4_timestamp();
        let body_hash = sha256_hex(body);
        let mut headers = vec![
            (
                "host".to_string(),
                "examplebucket.s3.amazonaws.com".to_string(),
            ),
            ("content-length".to_string(), body.len().to_string()),
            ("x-amz-content-sha256".to_string(), body_hash.clone()),
            ("x-amz-date".to_string(), amz_date.clone()),
        ];
        headers.extend(extra_headers);
        if let Some(security_token) = security_token {
            headers.push((
                "x-amz-security-token".to_string(),
                security_token.to_string(),
            ));
        }

        let mut signed_headers: Vec<&str> = headers.iter().map(|(name, _)| name.as_str()).collect();
        signed_headers.sort_unstable();
        let signed_headers_str = signed_headers.join(";");
        let canonical_headers_input: Vec<(&str, &str)> = headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect();
        let canonical_headers = canonical_headers(&canonical_headers_input);
        let canonical_query = canonical_query_string("");
        let canonical_req = canonical_request(
            method.as_str(),
            path,
            &canonical_query,
            &canonical_headers,
            &signed_headers_str,
            &body_hash,
        );
        let scope = format!("{date}/us-east-1/s3/aws4_request");
        let sts = string_to_sign(&amz_date, &scope, &sha256_hex(canonical_req.as_bytes()));
        let signing_key = auth::sigv4::derive_signing_key(secret_key, &date, "us-east-1", "s3");
        let signature = hex_lower(
            hmac::sign(
                &hmac::Key::new(hmac::HMAC_SHA256, signing_key.as_ref()),
                sts.as_bytes(),
            )
            .as_ref(),
        );
        headers.push((
            "authorization".to_string(),
            format!(
                "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
                access_key_id, scope, signed_headers_str, signature
            ),
        ));
        new_req(method, path, "", headers, body.to_vec())
    }

    fn signed_v4_req_with_credentials(
        method: http::Method,
        body: &[u8],
        extra_headers: Vec<(String, String)>,
        access_key_id: &str,
        secret_key: &SecretKey,
        security_token: Option<&str>,
    ) -> S3Request {
        signed_v4_req_for_path_with_credentials(
            method,
            "/",
            body,
            extra_headers,
            access_key_id,
            secret_key,
            security_token,
        )
    }

    fn signed_v4_put_req_with_credentials(
        body: &[u8],
        extra_headers: Vec<(String, String)>,
        access_key_id: &str,
        secret_key: &SecretKey,
        security_token: Option<&str>,
    ) -> S3Request {
        signed_v4_req_with_credentials(
            http::Method::PUT,
            body,
            extra_headers,
            access_key_id,
            secret_key,
            security_token,
        )
    }

    fn signed_v4_put_req(body: &[u8], extra_headers: Vec<(String, String)>) -> S3Request {
        signed_v4_put_req_with_credentials(
            body,
            extra_headers,
            TEST_SIGV4_ACCESS_KEY,
            &SecretKey::new(TEST_SIGV4_SECRET.to_string()),
            None,
        )
    }

    fn signed_post_policy_fields(
        bucket: &str,
        key: &str,
        extra_conditions: &[&str],
        extra_fields: &[(&str, &str)],
    ) -> Vec<(String, String)> {
        signed_post_policy_fields_for_region(
            bucket,
            key,
            "us-east-1",
            extra_conditions,
            extra_fields,
        )
    }

    fn signed_post_policy_fields_for_region(
        bucket: &str,
        key: &str,
        region: &str,
        extra_conditions: &[&str],
        extra_fields: &[(&str, &str)],
    ) -> Vec<(String, String)> {
        use base64::Engine;

        let (date, amz_date) = current_sigv4_timestamp();
        let credential = format!("{TEST_SIGV4_ACCESS_KEY}/{date}/{region}/s3/aws4_request");
        let mut conditions = vec![
            format!(r#"{{"bucket":"{bucket}"}}"#),
            format!(r#"{{"key":"{key}"}}"#),
            r#"{"x-amz-algorithm":"AWS4-HMAC-SHA256"}"#.to_string(),
            format!(r#"{{"x-amz-credential":"{credential}"}}"#),
            format!(r#"{{"x-amz-date":"{amz_date}"}}"#),
        ];
        conditions.extend(extra_conditions.iter().map(|value| (*value).to_string()));
        let policy = format!(
            r#"{{"expiration":"2099-12-31T23:59:59Z","conditions":[{}]}}"#,
            conditions.join(",")
        );
        let policy_b64 = base64::engine::general_purpose::STANDARD.encode(policy.as_bytes());
        let signing_key = auth::sigv4::derive_signing_key(
            &SecretKey::new(TEST_SIGV4_SECRET.to_string()),
            &date,
            region,
            "s3",
        );
        let signature = hex_lower(
            hmac::sign(
                &hmac::Key::new(hmac::HMAC_SHA256, signing_key.as_ref()),
                policy_b64.as_bytes(),
            )
            .as_ref(),
        );

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

    fn signed_post_fields_for_unsupported_identity(
        credential: &UnsupportedIdentityCredential,
    ) -> Vec<(String, String)> {
        use base64::Engine;

        let (date, amz_date) = current_sigv4_timestamp();
        let credential_scope = format!(
            "{}/{date}/us-east-1/s3/aws4_request",
            credential.access_key_id
        );
        let policy = format!(
            concat!(
                r#"{{"expiration":"2099-12-31T23:59:59Z","conditions":["#,
                r#"{{"bucket":"mybucket"}},{{"key":"mykey"}},"#,
                r#"{{"x-amz-algorithm":"AWS4-HMAC-SHA256"}},"#,
                r#"{{"x-amz-credential":"{}"}},{{"x-amz-date":"{}"}},"#,
                r#"{{"x-amz-security-token":"{}"}}]}}"#
            ),
            credential_scope, amz_date, credential.token
        );
        let policy_b64 = base64::engine::general_purpose::STANDARD.encode(policy.as_bytes());
        let signing_key =
            auth::sigv4::derive_signing_key(&credential.secret_key, &date, "us-east-1", "s3");
        let signature = hex_lower(
            hmac::sign(
                &hmac::Key::new(hmac::HMAC_SHA256, signing_key.as_ref()),
                policy_b64.as_bytes(),
            )
            .as_ref(),
        );

        vec![
            ("key".to_string(), "mykey".to_string()),
            (
                "x-amz-algorithm".to_string(),
                "AWS4-HMAC-SHA256".to_string(),
            ),
            ("x-amz-credential".to_string(), credential_scope),
            ("x-amz-date".to_string(), amz_date),
            ("policy".to_string(), policy_b64),
            ("x-amz-signature".to_string(), signature),
            ("x-amz-security-token".to_string(), credential.token.clone()),
        ]
    }

    #[test]
    fn validate_write_request_header_section_size_accepts_headers_at_limit() {
        let headers = [(
            "x-test-padding",
            "p".repeat(MAX_WRITE_REQUEST_HEADER_SECTION_SIZE - "x-test-padding".len()),
        )];
        let refs: Vec<(&str, &str)> = headers
            .iter()
            .map(|(name, value)| (*name, value.as_str()))
            .collect();
        validate_write_request_header_section_size(&refs).unwrap();
    }

    #[test]
    fn validate_write_request_header_section_size_rejects_headers_over_limit() {
        let headers = [(
            "x-test-padding",
            "p".repeat(MAX_WRITE_REQUEST_HEADER_SECTION_SIZE - "x-test-padding".len() + 1),
        )];
        let refs: Vec<(&str, &str)> = headers
            .iter()
            .map(|(name, value)| (*name, value.as_str()))
            .collect();
        let err = validate_write_request_header_section_size(&refs).unwrap_err();
        assert!(matches!(err, ServerError::RequestHeaderSectionTooLarge));
    }

    #[test]
    fn parse_request_metadata_accepts_user_metadata_at_limit() {
        let key = "x-amz-meta-limit";
        let value = "m".repeat(USER_METADATA_SIZE_LIMIT - "limit".len());

        let (metadata, system_metadata) = parse_request_metadata([(key, value.as_str())]).unwrap();

        assert_eq!(metadata.get("x-amz-meta-limit"), Some(value.as_str()));
        assert_eq!(system_metadata, SystemMetadata::EMPTY);
    }

    #[test]
    fn parse_put_object_request_metadata_ignores_invalid_checksum_type() {
        let (metadata, system_metadata) =
            parse_put_object_request_metadata([("x-amz-checksum-type", "BOGUS")]).unwrap();

        assert_eq!(metadata, MetadataBlob::new());
        assert_eq!(system_metadata, SystemMetadata::EMPTY);
    }

    #[test]
    fn parse_put_object_request_metadata_ignores_checksum_type_with_value() {
        let (metadata, system_metadata) = parse_put_object_request_metadata([
            ("x-amz-checksum-type", "COMPOSITE"),
            ("x-amz-checksum-crc32", "AAAAAA=="),
        ])
        .unwrap();

        assert_eq!(metadata, MetadataBlob::new());
        let checksum = system_metadata.checksum().expect("checksum metadata");
        assert_eq!(checksum.algorithm(), ChecksumAlgorithm::Crc32);
        assert_eq!(checksum.checksum_type(), None);
        assert_eq!(checksum.value(), "AAAAAA==");
    }

    #[test]
    fn parse_request_metadata_rejects_user_metadata_over_limit() {
        let key = "x-amz-meta-limit";
        let value = "m".repeat(USER_METADATA_SIZE_LIMIT - "limit".len() + 1);

        let err = parse_request_metadata([(key, value.as_str())]).unwrap_err();
        assert!(matches!(
            err,
            ServerError::MetadataTooLargeDetailed {
                max_size_allowed: USER_METADATA_SIZE_LIMIT,
                ..
            }
        ));
    }

    #[test]
    fn parse_request_metadata_accepts_system_metadata_at_limit() {
        let value = "v".repeat(SYSTEM_METADATA_SIZE_LIMIT - "content-disposition".len());

        let (metadata, system_metadata) =
            parse_request_metadata([("content-disposition", value.as_str())]).unwrap();

        assert_eq!(metadata, MetadataBlob::new());
        assert_eq!(
            system_metadata
                .content_disposition()
                .map(|value| value.as_str()),
            Some(value.as_str())
        );
    }

    #[test]
    fn parse_request_metadata_rejects_system_metadata_over_limit() {
        let value = "v".repeat(SYSTEM_METADATA_SIZE_LIMIT - "content-disposition".len() + 1);

        let err = parse_request_metadata([("content-disposition", value.as_str())]).unwrap_err();
        assert!(matches!(
            err,
            ServerError::MetadataTooLargeDetailed {
                max_size_allowed: SYSTEM_METADATA_SIZE_LIMIT,
                ..
            }
        ));
    }

    #[test]
    fn parse_request_metadata_rejects_redirect_plus_other_system_metadata_over_limit() {
        let redirect_len = SYSTEM_METADATA_SIZE_LIMIT - "x-amz-website-redirect-location".len();
        let redirect = format!("/{}", "r".repeat(redirect_len - 1));

        let err = parse_request_metadata([
            ("x-amz-website-redirect-location", redirect.as_str()),
            ("cache-control", "x"),
        ])
        .unwrap_err();
        assert!(matches!(
            err,
            ServerError::MetadataTooLargeDetailed {
                max_size_allowed: SYSTEM_METADATA_SIZE_LIMIT,
                ..
            }
        ));
    }

    #[test]
    fn parse_request_metadata_rejects_redirect_without_supported_prefix() {
        let err =
            parse_request_metadata([(WEBSITE_REDIRECT_LOCATION_HEADER_NAME, "docs/landing.html")])
                .unwrap_err();
        assert!(matches!(err, ServerError::InvalidRedirectLocation { .. }));
    }

    #[test]
    fn parse_request_metadata_rejects_redirect_with_unsupported_scheme() {
        let err = parse_request_metadata([(
            WEBSITE_REDIRECT_LOCATION_HEADER_NAME,
            "ftp://example.com/out",
        )])
        .unwrap_err();
        assert!(matches!(err, ServerError::InvalidRedirectLocation { .. }));
    }

    #[test]
    fn parse_copy_source_header_parses_typed_source() {
        let (bucket, key, version_id) =
            parse_copy_source_header("/source-bucket/path/to/key?versionId=42").unwrap();
        assert_eq!(bucket.as_str(), "source-bucket");
        assert_eq!(key.as_str(), "path/to/key");
        assert_eq!(version_id, Some(VersionId::from_u64(42)));
    }

    #[test]
    fn parse_copy_source_header_maps_invalid_source_bucket_to_bucket_not_found() {
        match parse_copy_source_header("/BadBucket/key") {
            Err(ServerError::BucketNotFound { name }) => assert_eq!(name, "BadBucket"),
            other => panic!("expected BucketNotFound, got {other:?}"),
        }
    }

    #[test]
    fn parse_copy_source_header_maps_oversized_source_bucket_to_bucket_not_found() {
        let oversized_bucket = "a".repeat(64);
        match parse_copy_source_header(&format!("/{oversized_bucket}/key")) {
            Err(ServerError::BucketNotFound { name }) => assert_eq!(name, oversized_bucket),
            other => panic!("expected BucketNotFound, got {other:?}"),
        }
    }

    #[test]
    fn unsupported_sigv2_error_returns_sigv4_required_message_in_eu_central_1() {
        let err = HttpFrontend::unsupported_sigv2_error(Some("AWS AKIA:signature"), "eu-central-1")
            .expect("expected eu-central-1 SigV2 to be rejected");
        match err {
            ServerError::InvalidRequest { reason } => assert_eq!(
                reason,
                "The authorization mechanism you have provided is not supported. Please use AWS4-HMAC-SHA256."
            ),
            other => panic!("expected InvalidRequest, got {other:?}"),
        }
    }

    #[test]
    fn unsupported_sigv2_error_does_not_override_other_regions() {
        assert!(
            HttpFrontend::unsupported_sigv2_error(Some("AWS AKIA:signature"), "us-west-2")
                .is_none()
        );
    }

    #[test]
    fn bucket_scoped_auth_errors_add_region_only_for_existing_non_post_bucket() {
        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        create_test_bucket(&frontend.coordinator, "mybucket");
        let wire_ids = WireResponseIds::new("request-id", "host-id");

        let existing_bucket = header_auth_request(
            http::Method::GET,
            "/mybucket",
            "UNKNOWNKEY123456",
            "s3",
            &[],
        );
        let response = frontend.handle_s3_request(&existing_bucket, &wire_ids);
        assert_eq!(response.status_code, 403);
        assert_eq!(
            find_header(&response, "x-amz-bucket-region"),
            Some("us-east-1")
        );
        let body = String::from_utf8(response.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("<Code>InvalidAccessKeyId</Code>"));

        for (method, path) in [
            (http::Method::GET, "/mybucket/key"),
            (http::Method::GET, "/missing"),
            (http::Method::POST, "/mybucket"),
        ] {
            let request = header_auth_request(method, path, "UNKNOWNKEY123456", "s3", &[]);
            let response = frontend.handle_s3_request(&request, &wire_ids);
            assert_eq!(response.status_code, 403);
            assert_eq!(find_header(&response, "x-amz-bucket-region"), None);
        }
    }

    #[test]
    fn existing_bucket_header_service_error_adds_bucket_region_end_to_end() {
        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        create_test_bucket(&frontend.coordinator, "mybucket");
        let request = header_auth_request(
            http::Method::GET,
            "/mybucket",
            TEST_SIGV4_ACCESS_KEY,
            "sts",
            &[],
        );
        let wire_ids = WireResponseIds::new("request-id", "host-id");

        let response = frontend.handle_s3_request(&request, &wire_ids);
        assert_eq!(response.status_code, 400);
        assert_eq!(
            find_header(&response, "x-amz-bucket-region"),
            Some("us-east-1")
        );
        let body = String::from_utf8(response.into_test_body_bytes().unwrap()).unwrap();
        assert_eq!(
            body,
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <Error>\
             <Code>AuthorizationHeaderMalformed</Code>\
             <Message>The authorization header is malformed; incorrect service \"sts\". This endpoint belongs to \"s3\".</Message>\
             <RequestId>request-id</RequestId>\
             <HostId>host-id</HostId>\
             </Error>"
        );
    }

    #[test]
    fn existing_bucket_presigned_region_error_adds_bucket_region_end_to_end() {
        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        create_test_bucket(&frontend.coordinator, "mybucket");
        let request = presigned_auth_request("/mybucket", TEST_SIGV4_ACCESS_KEY, "us-west-2", "s3");
        let wire_ids = WireResponseIds::new("request-id", "host-id");

        let response = frontend.handle_s3_request(&request, &wire_ids);
        assert_eq!(response.status_code, 400);
        assert_eq!(
            find_header(&response, "x-amz-bucket-region"),
            Some("us-east-1")
        );
        let body = String::from_utf8(response.into_test_body_bytes().unwrap()).unwrap();
        assert_eq!(
            body,
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <Error>\
             <Code>AuthorizationQueryParametersError</Code>\
             <Message>Error parsing the X-Amz-Credential parameter; the region 'us-west-2' is wrong; expecting 'us-east-1'</Message>\
             <Region>us-east-1</Region>\
             <RequestId>request-id</RequestId>\
             <HostId>host-id</HostId>\
             </Error>"
        );
    }

    #[test]
    fn bucket_region_mismatch_returns_wrong_region_for_existing_bucket() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let auth = auth::AuthContext {
            mode: auth::AuthMode::HeaderSigV4,
            access_key_id: Some("AKID".to_string()),
            identity: Some(configured_identity(auth::AccountIdentity::from_principal(
                "testuser",
            ))),
            authorization_profile: auth::AuthorizationProfile::Standard,
            request_epoch_secs: Some(0),
            signing_region: Some("us-west-2".to_string()),
            streaming: None,
        };
        let storage_route_admission = fe.coordinator.admit_storage_route_for_request().unwrap();

        match fe.enforce_bucket_region_for_operation(
            &storage_route_admission,
            &S3Operation::PutObject {
                bucket: test_bucket_name("mybucket"),
                key: "key".to_string(),
            },
            &auth,
        ) {
            Err(ServerError::WrongRegion {
                provided_region,
                expected_region,
                bucket_region_header,
            }) => {
                assert_eq!(provided_region, "us-west-2");
                assert_eq!(expected_region, "us-east-1");
                assert!(bucket_region_header);
            }
            other => panic!("expected WrongRegion, got {other:?}"),
        }
    }

    #[test]
    fn bucket_region_mismatch_returns_wrong_region_without_header_for_missing_bucket() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());

        let auth = auth::AuthContext {
            mode: auth::AuthMode::HeaderSigV4,
            access_key_id: Some("AKID".to_string()),
            identity: Some(configured_identity(auth::AccountIdentity::from_principal(
                "testuser",
            ))),
            authorization_profile: auth::AuthorizationProfile::Standard,
            request_epoch_secs: Some(0),
            signing_region: Some("us-west-2".to_string()),
            streaming: None,
        };
        let storage_route_admission = fe.coordinator.admit_storage_route_for_request().unwrap();

        match fe.enforce_bucket_region_for_operation(
            &storage_route_admission,
            &S3Operation::PutObject {
                bucket: test_bucket_name("missing"),
                key: "key".to_string(),
            },
            &auth,
        ) {
            Err(ServerError::WrongRegion {
                provided_region,
                expected_region,
                bucket_region_header,
            }) => {
                assert_eq!(provided_region, "us-west-2");
                assert_eq!(expected_region, "us-east-1");
                assert!(!bucket_region_header);
            }
            other => panic!("expected WrongRegion, got {other:?}"),
        }
    }

    fn test_bucket_request(name: &str) -> crate::coordinator::BucketRequest<'_> {
        crate::coordinator::BucketRequest::new(
            test_bucket_name(name),
            crate::coordinator::test_helpers::requester("testuser"),
            None,
        )
    }

    fn test_object_request<'a>(
        bucket: &'a str,
        key: &'a str,
    ) -> crate::coordinator::ObjectRequest<'a> {
        crate::coordinator::ObjectRequest::new(
            parse_bucket_name(bucket).unwrap(),
            parse_object_key(key).unwrap(),
            crate::coordinator::test_helpers::requester("testuser"),
            None,
        )
    }

    #[test]
    fn list_buckets_uses_owner_id_without_display_name() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        let owner_canonical_id = s3_types::CanonicalUserId::from_principal("custom-account-id");
        let account = auth::AccountIdentity::new("testuser", owner_canonical_id.clone(), "User A");

        fe.coordinator
            .create_bucket(&crate::coordinator::CreateBucketRequest {
                name: parse_bucket_name("mybucket").unwrap(),
                requester: crate::coordinator::Requester::authenticated(account.clone()),
                namespace: BucketNamespace::Global,
                acl: crate::coordinator::CreateBucketAcl::DefaultPrivate,
                ownership: crate::coordinator::BucketObjectOwnership::ObjectWriter,
                object_lock_enabled: false,
            })
            .unwrap();

        let admission = fe.coordinator.admit_storage_route_for_request().unwrap();
        let bucket = fe
            .coordinator
            .head_bucket_on_admitted_route(
                &admission,
                &crate::coordinator::BucketRequest::new(
                    parse_bucket_name("mybucket").unwrap(),
                    crate::coordinator::Requester::authenticated(account.clone()),
                    None,
                ),
            )
            .unwrap();
        assert_eq!(bucket.owner_canonical_id, owner_canonical_id);

        let auth = auth::AuthContext {
            mode: auth::AuthMode::HeaderSigV4,
            access_key_id: Some("AKID".to_string()),
            identity: Some(configured_identity(account)),
            authorization_profile: auth::AuthorizationProfile::Standard,
            request_epoch_secs: Some(0),
            signing_region: Some("us-east-1".to_string()),
            streaming: None,
        };
        let resp = fe
            .dispatch_routed(&make_req(""), &auth, S3Operation::ListBuckets)
            .unwrap();
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains(owner_canonical_id.as_str()));
        assert!(!body.contains("<DisplayName>"));
        assert!(body.contains("<BucketArn>arn:aws:s3:::mybucket</BucketArn>"));
    }

    #[test]
    fn get_bucket_location_dispatches_through_dedicated_bucket_location_checks() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(http::Method::GET, "/mybucket", "location", vec![], vec![]);
        let resp = fe
            .dispatch_routed(
                &req,
                &test_auth(),
                S3Operation::GetBucketLocation {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(resp.status_code, 200);
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("<LocationConstraint"));
        assert!(!body.contains(">us-east-1<"));
    }

    fn make_req(query: &str) -> S3Request {
        S3Request::new_for_test(
            http::Method::GET,
            "/",
            query,
            test_headers(vec![]),
            vec![],
            0,
        )
    }

    fn new_req(
        method: http::Method,
        path: &str,
        query: &str,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    ) -> S3Request {
        S3Request::new_for_test(method, path, query, test_headers(headers), body, 0)
    }

    fn test_headers(headers: Vec<(String, String)>) -> http::HeaderMap {
        request::header_map_from_owned(headers)
    }

    fn find_header<'a>(resp: &'a S3Response, name: &str) -> Option<&'a str> {
        resp.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    fn response_body(resp: S3Response) -> Vec<u8> {
        match resp.stream {
            Some(mut stream) => {
                let mut body = Vec::new();
                while let Some(chunk) = stream
                    .next_chunk(INTERNAL_SEGMENT_SIZE)
                    .expect("read streamed response body")
                {
                    body.extend_from_slice(&chunk);
                }
                body
            }
            None => resp.body,
        }
    }

    #[test]
    fn response_overrides_apply_valid_values() {
        let cases = [
            (
                "response-content-type=text%2Fplain",
                "Content-Type",
                "text/plain",
            ),
            (
                "response-content-disposition=attachment%3B%20filename%3D%22test.txt%22",
                "Content-Disposition",
                "attachment; filename=\"test.txt\"",
            ),
            ("response-content-encoding=gzip", "Content-Encoding", "gzip"),
            (
                "response-content-language=en-US",
                "Content-Language",
                "en-US",
            ),
            (
                "response-cache-control=max-age%3D60",
                "Cache-Control",
                "max-age=60",
            ),
            (
                "response-expires=Mon%2C%2015%20Jan%202024%2012%3A30%3A45%20GMT",
                "Expires",
                "Mon, 15 Jan 2024 12:30:45 GMT",
            ),
        ];

        for (query, header_name, expected) in cases {
            let req = make_req(query);
            let mut resp = S3Response {
                status_code: 200,
                headers: Vec::new(),
                body: Vec::new(),
                stream: None,
                error_diagnostic: None,
                include_wire_ids: true,
            };
            apply_response_overrides(&mut resp, &req);
            assert_eq!(find_header(&resp, header_name), Some(expected));
        }
    }

    #[test]
    fn response_overrides_sanitize_or_ignore_invalid_values() {
        let cases = [
            (
                "response-content-type=text%2Fplain%0D%0AInjected%3A%20x",
                "Content-Type",
                Some("text/plain  Injected: x"),
            ),
            (
                "response-content-disposition=attachment%0D%0AInjected%3A%20x",
                "Content-Disposition",
                Some("attachment  Injected: x"),
            ),
            (
                "response-content-encoding=gzip%0D%0AInjected%3A%20x",
                "Content-Encoding",
                Some("gzip  Injected: x"),
            ),
            (
                "response-content-language=en-US%0D%0AInjected%3A%20x",
                "Content-Language",
                Some("en-US  Injected: x"),
            ),
            (
                "response-cache-control=max-age%3D60%0D%0AInjected%3A%20x",
                "Cache-Control",
                Some("max-age=60  Injected: x"),
            ),
            ("response-expires=not-a-date", "Expires", Some("not-a-date")),
        ];

        for (query, header_name, expected) in cases {
            let req = make_req(query);
            let mut resp = S3Response {
                status_code: 200,
                headers: Vec::new(),
                body: Vec::new(),
                stream: None,
                error_diagnostic: None,
                include_wire_ids: true,
            };
            if header_name == "Content-Type" {
                resp.headers.push((
                    "Content-Type".to_string(),
                    "application/octet-stream".to_string(),
                ));
            }
            apply_response_overrides(&mut resp, &req);
            assert_eq!(find_header(&resp, header_name), expected);
        }
    }

    #[test]
    fn s3_response_to_hyper_invalid_header_returns_internal_error() {
        let mut resp = S3Response {
            status_code: 200,
            headers: Vec::new(),
            body: Vec::new(),
            stream: None,
            error_diagnostic: None,
            include_wire_ids: true,
        };
        resp.headers.push((
            "Content-Type".to_string(),
            "text/plain\r\nInjected: x".to_string(),
        ));

        let hyper_resp = s3_response_to_hyper(
            resp,
            None,
            8192,
            false,
            false,
            ResponseTraceMeta::new(
                crate::http::new_request_trace_context(),
                Arc::<str>::from("host-id"),
                "GET",
                "/",
                "",
            ),
        );
        assert_eq!(hyper_resp.status(), http::StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn s3_response_to_hyper_transfers_inflight_request_guard_without_recounting() {
        let before = observability::metrics_snapshot().inflight_requests;
        let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = semaphore
            .try_acquire_owned()
            .expect("request permit should be available");
        let admission = HttpRequestAdmission::new(permit);
        assert_eq!(
            observability::metrics_snapshot().inflight_requests,
            before + 1
        );

        let response = s3_response_to_hyper(
            S3Response {
                status_code: 200,
                headers: Vec::new(),
                body: b"ok".to_vec(),
                stream: None,
                error_diagnostic: None,
                include_wire_ids: true,
            },
            Some(admission),
            8192,
            false,
            false,
            ResponseTraceMeta::new(
                crate::http::new_request_trace_context(),
                Arc::<str>::from("host-id"),
                "GET",
                "/",
                "",
            ),
        );
        assert_eq!(
            observability::metrics_snapshot().inflight_requests,
            before + 1,
            "response conversion must transfer rather than duplicate the guard"
        );

        drop(response);
        assert_eq!(observability::metrics_snapshot().inflight_requests, before);
    }

    #[test]
    fn s3_response_to_hyper_invalid_header_records_diagnostic_before_panic() {
        let mut resp = S3Response {
            status_code: 200,
            headers: Vec::new(),
            body: Vec::new(),
            stream: None,
            error_diagnostic: None,
            include_wire_ids: true,
        };
        resp.headers.push((
            "Content-Type".to_string(),
            "text/plain\r\nInjected: x".to_string(),
        ));

        let result = {
            let _diagnostic_guard = SuppressExpectedPanicOn500Diagnostics::new();
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = s3_response_to_hyper(
                    resp,
                    None,
                    8192,
                    true,
                    false,
                    ResponseTraceMeta::new(
                        observability::TraceContext::from_ids(observability::TraceContextIds {
                            trace_id: "trace-conversion-error".to_string(),
                            request_id: "request-conversion-error".to_string(),
                        }),
                        Arc::<str>::from("host-id"),
                        "GET",
                        "/secret-bucket/secret-key",
                        "X-Amz-Signature=secret",
                    ),
                );
            }))
        };

        let panic_payload =
            result.expect_err("conversion error should panic when panic-on-500 is enabled");
        let panic_message = panic_payload
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic_payload.downcast_ref::<&str>().copied())
            .expect("panic should carry diagnostic string");
        assert!(panic_message.contains("HTTP response conversion produced InternalError"));

        let records = observability::flight_recorder_snapshot();
        let request_error_record = records
            .iter()
            .rev()
            .find(|record| {
                record.request_id == "request-conversion-error" && record.event == "request_error"
            })
            .expect("conversion error should be recorded before panic");
        assert!(request_error_record.detail.contains("status=500"));
        assert!(request_error_record.detail.contains("path_hash="));
        assert!(request_error_record.detail.contains("sigv4_query=true"));
        assert!(request_error_record
            .detail
            .contains("error_code=InternalError"));
        assert!(request_error_record
            .detail
            .contains("cause_label=internal_error"));
        assert!(!request_error_record.detail.contains("secret-bucket"));
        assert!(!request_error_record.detail.contains("secret-key"));
        assert!(!request_error_record.detail.contains("secret"));

        let cause_chain_record = records
            .iter()
            .rev()
            .find(|record| {
                record.request_id == "request-conversion-error"
                    && record.event == "request_500_cause_chain"
            })
            .expect("conversion error cause chain should be recorded before panic");
        assert!(cause_chain_record.detail.contains("status=500"));
        assert!(cause_chain_record.detail.contains("path_hash="));
        assert!(cause_chain_record.detail.contains("sigv4_query=true"));
        assert!(cause_chain_record
            .detail
            .contains("cause_label=internal_error"));
        assert!(cause_chain_record
            .detail
            .contains("cause_chain=\"server_error>internal_error\""));
        assert!(!cause_chain_record.detail.contains("secret-bucket"));
        assert!(!cause_chain_record.detail.contains("secret-key"));
        assert!(!cause_chain_record.detail.contains("secret"));
    }

    #[test]
    fn s3_response_to_hyper_panics_on_500_when_enabled() {
        let resp = S3Response {
            status_code: 500,
            headers: Vec::new(),
            body: b"<Error><Code>InternalError</Code></Error>".to_vec(),
            stream: None,
            error_diagnostic: None,
            include_wire_ids: true,
        };

        let result = {
            let _diagnostic_guard = SuppressExpectedPanicOn500Diagnostics::new();
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = s3_response_to_hyper(
                    resp,
                    None,
                    8192,
                    true,
                    false,
                    ResponseTraceMeta::new(
                        crate::http::new_request_trace_context(),
                        Arc::<str>::from("host-id"),
                        "GET",
                        "/",
                        "",
                    ),
                );
            }))
        };

        let panic_payload =
            result.expect_err("HTTP 500 response should panic when panic-on-500 is enabled");
        let panic_message = panic_payload
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic_payload.downcast_ref::<&str>().copied())
            .expect("panic should carry diagnostic string");
        assert!(panic_message.contains("server produced HTTP 500 response"));
    }

    #[test]
    fn s3_response_to_hyper_deduplicates_content_length() {
        let resp = S3Response {
            status_code: 200,
            headers: vec![
                ("Content-Length".to_string(), "5".to_string()),
                ("Content-Length".to_string(), "5".to_string()),
            ],
            body: b"hello".to_vec(),
            stream: None,
            error_diagnostic: None,
            include_wire_ids: true,
        };

        let hyper_resp = s3_response_to_hyper(
            resp,
            None,
            8192,
            false,
            false,
            ResponseTraceMeta::new(
                crate::http::new_request_trace_context(),
                Arc::<str>::from("host-id"),
                "GET",
                "/",
                "",
            ),
        );
        assert_eq!(
            hyper_resp
                .headers()
                .get_all(http::header::CONTENT_LENGTH)
                .iter()
                .count(),
            1
        );
        assert_eq!(
            hyper_resp
                .headers()
                .get(http::header::CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok()),
            Some("5")
        );
    }

    #[test]
    fn new_request_trace_context_matches_expected_id_shapes() {
        let ctx = crate::http::new_request_trace_context();
        assert_eq!(ctx.trace_id().len(), 32);
        assert_eq!(ctx.request_id().len(), 16);
        assert!(ctx.trace_id().bytes().all(|b| b.is_ascii_hexdigit()));
        assert!(ctx.trace_id().bytes().all(|b| !b.is_ascii_uppercase()));
        assert!(ctx
            .request_id()
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit()));
    }

    #[test]
    fn new_host_id_returns_visible_ascii_header_value() {
        let host_id = crate::http::new_host_id();
        assert!(!host_id.is_empty());
        assert!(host_id.bytes().all(|b| b.is_ascii_graphic()));
        assert!(http::header::HeaderValue::from_str(&host_id).is_ok());
    }

    #[test]
    fn s3_response_to_hyper_injects_request_and_host_headers() {
        let resp = S3Response {
            status_code: 200,
            headers: Vec::new(),
            body: Vec::new(),
            stream: None,
            error_diagnostic: None,
            include_wire_ids: true,
        };

        let hyper_resp = s3_response_to_hyper(
            resp,
            None,
            8192,
            false,
            false,
            ResponseTraceMeta::new(
                observability::TraceContext::from_ids(observability::TraceContextIds {
                    trace_id: "0123456789abcdef0123456789abcdef".to_string(),
                    request_id: "2VG1X5NNMZ52HKC0".to_string(),
                }),
                Arc::<str>::from("stable-host-id"),
                "GET",
                "/",
                "",
            ),
        );

        assert_eq!(
            hyper_resp
                .headers()
                .get("x-amz-request-id")
                .and_then(|value| value.to_str().ok()),
            Some("2VG1X5NNMZ52HKC0")
        );
        assert_eq!(
            hyper_resp
                .headers()
                .get("x-amz-id-2")
                .and_then(|value| value.to_str().ok()),
            Some("stable-host-id")
        );
    }

    #[test]
    fn streaming_body_error_diagnostic_preserves_response_status() {
        let mut trace = ResponseBodyTrace::new(
            ResponseTraceMeta::new(
                observability::TraceContext::from_ids(observability::TraceContextIds {
                    trace_id: "0123456789abcdef0123456789abcdef".to_string(),
                    request_id: "2VG1X5NNMZ52HKC0".to_string(),
                }),
                Arc::<str>::from("stable-host-id"),
                "GET",
                "/bucket/key",
                "partNumber=1",
            ),
            206,
            1024,
            true,
        );

        trace.emit_error(&ServerError::Store(
            storage::test_support::store_failure_for_operation_failure_class(
                storage::StoreOperationFailureClass::ResourceExhausted,
            ),
        ));

        assert_eq!(trace.status_code, 206);
        assert!(trace.terminal_event_emitted);

        let records = observability::flight_recorder_snapshot();
        let request_error_record = records
            .iter()
            .rev()
            .find(|record| {
                record.request_id == "2VG1X5NNMZ52HKC0" && record.event == "request_error"
            })
            .expect("streaming body error should record request error");
        assert!(request_error_record.detail.contains("status=206"));
        assert!(request_error_record
            .detail
            .contains("error_code=InternalError"));
        assert!(!records.iter().any(|record| {
            record.request_id == "2VG1X5NNMZ52HKC0" && record.event == "request_500_cause_chain"
        }));
    }

    #[test]
    fn object_read_failure_preserves_storage_diagnostic_in_http_telemetry() {
        let mut trace = ResponseBodyTrace::new(
            ResponseTraceMeta::new(
                observability::TraceContext::from_ids(observability::TraceContextIds {
                    trace_id: "object-read-diagnostic-trace".to_string(),
                    request_id: "object-read-diagnostic-request".to_string(),
                }),
                Arc::<str>::from("stable-host-id"),
                "GET",
                "/bucket/key",
                "",
            ),
            500,
            0,
            false,
        );
        let (failure, private_fragments) =
            storage::test_support::object_read_failure_diagnostic_fixture();
        trace.emit_error(&ServerError::ObjectRead(failure));

        let records = observability::flight_recorder_snapshot();
        let request_error = records
            .iter()
            .rev()
            .find(|record| {
                record.request_id == "object-read-diagnostic-request"
                    && record.event == "request_error"
            })
            .expect("object-read failure should record request diagnostics");
        assert!(request_error
            .detail
            .contains("cause_label=store_io_failure"));

        let cause_chain = records
            .iter()
            .rev()
            .find(|record| {
                record.request_id == "object-read-diagnostic-request"
                    && record.event == "request_500_cause_chain"
            })
            .expect("object-read failure should record its bounded cause chain");
        assert!(cause_chain.detail.contains("cause_label=store_io_failure"));
        assert!(cause_chain
            .detail
            .contains("cause_chain=\"server_error>object_read>store_io_failure\""));
        for private_fragment in private_fragments {
            assert!(!request_error.detail.contains(private_fragment));
            assert!(!cause_chain.detail.contains(private_fragment));
        }
    }

    #[test]
    fn s3_response_error_with_ids_embeds_explicit_request_and_host_ids() {
        let wire_ids =
            WireResponseIds::new("2VG1X5NNMZ52HKC0".to_string(), "stable-host-id".to_string());
        let resp = S3Response::error_with_ids(
            &ServerError::MetadataTooLargeDetailed {
                size: 2049,
                max_size_allowed: 2048,
            },
            "",
            &wire_ids,
        );
        let body = std::str::from_utf8(&resp.body).unwrap_or("");
        assert!(resp.stream.is_some());
        assert!(body.is_empty());
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("<RequestId>2VG1X5NNMZ52HKC0</RequestId>"));
        assert!(body.contains("<HostId>stable-host-id</HostId>"));
    }

    #[test]
    fn parse_bucket_namespace_defaults_to_global() {
        let req = new_req(http::Method::PUT, "/bucket", "", vec![], vec![]);
        let bucket = BucketName::try_from("bucket".to_string()).unwrap();
        assert_eq!(
            parse_bucket_namespace(&req, &bucket).unwrap(),
            BucketNamespace::Global
        );
    }

    #[test]
    fn parse_bucket_namespace_accepts_account_regional() {
        let req = new_req(
            http::Method::PUT,
            "/bucket",
            "",
            vec![(
                "x-amz-bucket-namespace".to_string(),
                "account-regional".to_string(),
            )],
            vec![],
        );
        let bucket = BucketName::try_from("bucket".to_string()).unwrap();
        assert_eq!(
            parse_bucket_namespace(&req, &bucket).unwrap(),
            BucketNamespace::AccountRegional
        );
    }

    #[test]
    fn parse_bucket_namespace_rejects_invalid_values() {
        let req = new_req(
            http::Method::PUT,
            "/bucket",
            "",
            vec![("x-amz-bucket-namespace".to_string(), "bogus".to_string())],
            vec![],
        );
        let bucket = BucketName::try_from("bucket".to_string()).unwrap();
        match parse_bucket_namespace(&req, &bucket).unwrap_err() {
            ServerError::InvalidArgument { reason } => {
                assert_eq!(reason, "invalid x-amz-bucket-namespace: bogus");
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn parse_bucket_namespace_requires_header_for_account_regional_name() {
        let req = new_req(http::Method::PUT, "/bucket", "", vec![], vec![]);
        let bucket = BucketName::try_from("bucket-111122223333-us-east-1-an".to_string()).unwrap();
        assert!(matches!(
            parse_bucket_namespace(&req, &bucket),
            Err(ServerError::MissingNamespaceHeader)
        ));
    }

    #[test]
    fn parse_bucket_namespace_rejects_global_header_for_account_regional_name() {
        let req = new_req(
            http::Method::PUT,
            "/bucket",
            "",
            vec![("x-amz-bucket-namespace".to_string(), "global".to_string())],
            vec![],
        );
        let bucket = BucketName::try_from("bucket-111122223333-us-east-1-an".to_string()).unwrap();
        assert!(matches!(
            parse_bucket_namespace(&req, &bucket),
            Err(ServerError::GlobalNamespaceHeaderRejectedForAccountRegionalBucket {
                bucket
            }) if bucket == "bucket-111122223333-us-east-1-an"
        ));
    }

    #[test]
    fn parse_sse_customer_request_mismatched_key_md5_is_invalid_argument() {
        use base64::Engine;

        let key_b64 = base64::engine::general_purpose::STANDARD.encode([0u8; 32]);
        let req = new_req(
            http::Method::PUT,
            "/",
            "",
            vec![
                (
                    SSE_C_ALGORITHM_HEADER.to_string(),
                    SSE_CUSTOMER_ALGORITHM.to_string(),
                ),
                (SSE_C_KEY_HEADER.to_string(), key_b64),
                (
                    SSE_C_KEY_MD5_HEADER.to_string(),
                    "AAAAAAAAAAAAAAAAAAAAAA==".to_string(),
                ),
            ],
            vec![],
        );

        match parse_sse_customer_request(&req) {
            Err(ServerError::InvalidSseCustomerKeyMd5) => {}
            other => panic!("expected InvalidSseCustomerKeyMd5, got {other:?}"),
        }
    }

    #[test]
    fn parse_sse_customer_form_fields_mismatched_key_md5_is_invalid_argument() {
        use base64::Engine;

        let key_b64 = base64::engine::general_purpose::STANDARD.encode([0u8; 32]);
        let form_fields = vec![
            (
                SSE_C_ALGORITHM_HEADER.to_string(),
                SSE_CUSTOMER_ALGORITHM.to_string(),
            ),
            (SSE_C_KEY_HEADER.to_string(), key_b64),
            (
                SSE_C_KEY_MD5_HEADER.to_string(),
                "AAAAAAAAAAAAAAAAAAAAAA==".to_string(),
            ),
        ];

        match parse_sse_customer_form_fields(TransportSecurity::Tls, &form_fields) {
            Err(ServerError::InvalidSseCustomerKeyMd5) => {}
            other => panic!("expected InvalidSseCustomerKeyMd5, got {other:?}"),
        }
    }

    #[test]
    fn parse_sse_customer_request_rejects_lowercase_algorithm() {
        use base64::Engine;

        let key_b64 = base64::engine::general_purpose::STANDARD.encode([0u8; 32]);
        let req = new_req(
            http::Method::PUT,
            "/",
            "",
            vec![
                (SSE_C_ALGORITHM_HEADER.to_string(), "aes256".to_string()),
                (SSE_C_KEY_HEADER.to_string(), key_b64),
                (
                    SSE_C_KEY_MD5_HEADER.to_string(),
                    "cLyPS3KoaSFGi/joRB3OUQ==".to_string(),
                ),
            ],
            vec![],
        );

        match parse_sse_customer_request(&req) {
            Err(ServerError::InvalidEncryptionAlgorithmError { value }) => {
                assert_eq!(value, "aes256");
            }
            other => panic!(
                "expected InvalidEncryptionAlgorithmError for lowercase SSE-C algorithm, got {other:?}"
            ),
        }
    }

    #[test]
    fn parse_sse_customer_form_fields_rejects_lowercase_algorithm() {
        use base64::Engine;

        let key_b64 = base64::engine::general_purpose::STANDARD.encode([0u8; 32]);
        let form_fields = vec![
            (SSE_C_ALGORITHM_HEADER.to_string(), "aes256".to_string()),
            (SSE_C_KEY_HEADER.to_string(), key_b64),
            (
                SSE_C_KEY_MD5_HEADER.to_string(),
                "cLyPS3KoaSFGi/joRB3OUQ==".to_string(),
            ),
        ];

        match parse_sse_customer_form_fields(TransportSecurity::Tls, &form_fields) {
            Err(ServerError::InvalidEncryptionAlgorithmError { value }) => {
                assert_eq!(value, "aes256");
            }
            other => panic!(
                "expected InvalidEncryptionAlgorithmError for lowercase POST SSE-C algorithm, got {other:?}"
            ),
        }
    }

    #[test]
    fn put_object_explicit_sse_s3_returns_encryption_headers() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend_with_sse_s3(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let put_req = new_req(
            http::Method::PUT,
            "",
            "",
            vec![(SSE_HEADER.to_string(), "AES256".to_string())],
            b"hello world".to_vec(),
        );
        let put_resp = fe
            .dispatch_routed(
                &put_req,
                &test_auth(),
                S3Operation::PutObject {
                    bucket: test_bucket_name("mybucket"),
                    key: "mykey".to_string(),
                },
            )
            .unwrap();
        assert_eq!(put_resp.status_code, 200);
        assert_eq!(
            find_header(&put_resp, "x-amz-server-side-encryption"),
            Some("AES256")
        );

        let head_req = new_req(http::Method::HEAD, "", "", vec![], vec![]);
        let head_resp = fe
            .dispatch_routed(
                &head_req,
                &test_auth(),
                S3Operation::HeadObject {
                    bucket: test_bucket_name("mybucket"),
                    key: "mykey".to_string(),
                },
            )
            .unwrap();
        assert_eq!(head_resp.status_code, 200);
        assert_eq!(
            find_header(&head_resp, "x-amz-server-side-encryption"),
            Some("AES256")
        );
    }

    #[test]
    fn put_object_rejects_sse_s3_with_sse_c_headers() {
        use base64::Engine;

        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let key_b64 = base64::engine::general_purpose::STANDARD.encode([0u8; 32]);
        let req = new_req(
            http::Method::PUT,
            "",
            "",
            vec![
                (SSE_HEADER.to_string(), "AES256".to_string()),
                (
                    SSE_C_ALGORITHM_HEADER.to_string(),
                    SSE_CUSTOMER_ALGORITHM.to_string(),
                ),
                (SSE_C_KEY_HEADER.to_string(), key_b64),
                (
                    SSE_C_KEY_MD5_HEADER.to_string(),
                    "cLyPS3KoaSFGi/joRB3OUQ==".to_string(),
                ),
            ],
            b"hello world".to_vec(),
        );

        match fe.dispatch_routed(
            &req,
            &test_auth(),
            S3Operation::PutObject {
                bucket: test_bucket_name("mybucket"),
                key: "mykey".to_string(),
            },
        ) {
            Err(ServerError::InvalidArgument { reason })
                if reason == "x-amz-server-side-encryption may not be used with SSE-C headers" => {}
            Err(err) => panic!("expected InvalidArgument, got {err:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn put_object_rejects_aes256_with_kms_key_id() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::PUT,
            "",
            "",
            vec![
                (SSE_HEADER.to_string(), "AES256".to_string()),
                (
                    SSE_KMS_KEY_ID_HEADER.to_string(),
                    "arn:aws:kms:us-east-1:111122223333:key/example".to_string(),
                ),
            ],
            b"hello world".to_vec(),
        );

        match fe.dispatch_routed(
            &req,
            &test_auth(),
            S3Operation::PutObject {
                bucket: test_bucket_name("mybucket"),
                key: "mykey".to_string(),
            },
        ) {
            Err(ServerError::InvalidArgument { reason })
                if reason
                    == "x-amz-server-side-encryption-aws-kms-key-id may not be used with x-amz-server-side-encryption: AES256" => {}
            Err(err) => panic!("expected InvalidArgument, got {err:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn head_object_rejects_managed_encryption_request_headers() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let put_req = new_req(http::Method::PUT, "", "", vec![], b"hello world".to_vec());
        fe.dispatch_routed(
            &put_req,
            &test_auth(),
            S3Operation::PutObject {
                bucket: test_bucket_name("mybucket"),
                key: "mykey".to_string(),
            },
        )
        .unwrap();

        let head_req = new_req(
            http::Method::HEAD,
            "",
            "",
            vec![(SSE_HEADER.to_string(), "AES256".to_string())],
            vec![],
        );
        match fe.dispatch_routed(
            &head_req,
            &test_auth(),
            S3Operation::HeadObject {
                bucket: test_bucket_name("mybucket"),
                key: "mykey".to_string(),
            },
        ) {
            Err(ServerError::InvalidManagedEncryptionReadHeader { context, header })
                if context == ManagedEncryptionReadHeaderContext::StandardObjectRead
                    && header
                        == (ManagedEncryptionReadHeader::ServerSideEncryption {
                            value: "AES256".to_string(),
                        }) => {}
            Err(err) => panic!("expected InvalidRequest, got {err:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn parse_acl_grants_accepts_supported_header_grantees() {
        let canonical_id = s3_types::CanonicalUserId::from_principal("grantee-a");
        let req = new_req(
            http::Method::PUT,
            "/",
            "",
            vec![
                (
                    "x-amz-grant-read".to_string(),
                    format!(
                        "id=\"{}\", uri=\"{}\"",
                        canonical_id.as_str(),
                        s3_types::AclGrantee::all_users_uri()
                    ),
                ),
                (
                    "x-amz-grant-full-control".to_string(),
                    format!("id=\"{}\"", canonical_id.as_str()),
                ),
            ],
            vec![],
        );

        let grants = parse_acl_grants(&req).unwrap();
        assert_eq!(
            grants,
            s3_types::AclGrants::new(vec![
                s3_types::AclGrant::new(
                    s3_types::AclGrantee::CanonicalUser(canonical_id.clone()),
                    s3_types::AclPermission::Read,
                ),
                s3_types::AclGrant::new(
                    s3_types::AclGrantee::AllUsers,
                    s3_types::AclPermission::Read,
                ),
                s3_types::AclGrant::new(
                    s3_types::AclGrantee::CanonicalUser(canonical_id),
                    s3_types::AclPermission::FullControl,
                ),
            ])
        );
    }

    #[test]
    fn parse_confirm_remove_self_bucket_access_accepts_supported_values() {
        assert!(!parse_confirm_remove_self_bucket_access(None).unwrap());
        assert!(parse_confirm_remove_self_bucket_access(Some("true")).unwrap());
        assert!(!parse_confirm_remove_self_bucket_access(Some("false")).unwrap());
    }

    #[test]
    fn parse_confirm_remove_self_bucket_access_rejects_invalid_value() {
        match parse_confirm_remove_self_bucket_access(Some("True")) {
            Err(ServerError::InvalidArgument { reason }) => {
                assert!(reason.contains("x-amz-confirm-remove-self-bucket-access"));
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn parse_acl_grants_rejects_body_and_headers_together() {
        let req = new_req(
            http::Method::PUT,
            "/",
            "",
            vec![(
                "x-amz-grant-read".to_string(),
                format!("uri=\"{}\"", s3_types::AclGrantee::all_users_uri()),
            )],
            b"<AccessControlPolicy/>".to_vec(),
        );

        match parse_acl_grants(&req) {
            Err(ServerError::InvalidArgument { reason }) => {
                assert!(reason.contains("cannot be combined"));
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn parse_create_bucket_acl_accepts_unquoted_grant_headers() {
        let canonical_id = s3_types::CanonicalUserId::from_principal("grantee-a");
        let req = new_req(
            http::Method::PUT,
            "/",
            "",
            vec![(
                "x-amz-grant-read".to_string(),
                format!("id={}", canonical_id.as_str()),
            )],
            vec![],
        );

        let acl = parse_create_bucket_acl(&req).unwrap();
        assert_eq!(
            acl,
            crate::coordinator::CreateBucketAcl::Grants(s3_types::AclGrants::new(vec![
                s3_types::AclGrant::new(
                    s3_types::AclGrantee::CanonicalUser(canonical_id),
                    s3_types::AclPermission::Read,
                ),
            ]))
        );
    }

    #[test]
    fn parse_acl_grants_rejects_unquoted_header_value() {
        let canonical_id = s3_types::CanonicalUserId::from_principal("grantee-a");
        let req = new_req(
            http::Method::PUT,
            "/",
            "",
            vec![(
                "x-amz-grant-read".to_string(),
                format!("id={}", canonical_id.as_str()),
            )],
            vec![],
        );

        match parse_acl_grants(&req) {
            Err(ServerError::InvalidArgument { .. }) => {}
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn parse_create_bucket_acl_rejects_acl_and_grants_together() {
        let canonical_id = s3_types::CanonicalUserId::from_principal("grantee-a");
        let req = new_req(
            http::Method::PUT,
            "/",
            "",
            vec![
                ("x-amz-acl".to_string(), "private".to_string()),
                (
                    "x-amz-grant-read".to_string(),
                    format!("id={}", canonical_id.as_str()),
                ),
            ],
            vec![],
        );

        match parse_create_bucket_acl(&req) {
            Err(ServerError::InvalidArgument { reason }) => {
                assert!(reason.contains("cannot be combined"));
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn parse_put_object_write_acl_rejects_acl_and_grants_together() {
        let canonical_id = s3_types::CanonicalUserId::from_principal("grantee-a");
        let req = new_req(
            http::Method::PUT,
            "/",
            "",
            vec![
                ("x-amz-acl".to_string(), "private".to_string()),
                (
                    "x-amz-grant-read".to_string(),
                    format!("id=\"{}\"", canonical_id.as_str()),
                ),
            ],
            b"data".to_vec(),
        );

        match parse_put_object_write_acl(&req) {
            Err(ServerError::InvalidArgument { reason }) => {
                assert!(reason.contains("cannot be combined"));
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn parse_acl_grants_rejects_malformed_header_value() {
        let req = new_req(
            http::Method::PUT,
            "/",
            "",
            vec![("x-amz-grant-read".to_string(), "id=".to_string())],
            vec![],
        );

        match parse_acl_grants(&req) {
            Err(ServerError::InvalidArgument { .. }) => {}
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn put_bucket_acl_accepts_header_grants_and_renders_them() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");
        let canonical_id = s3_types::CanonicalUserId::from_principal("grantee-a");

        let put_req = new_req(
            http::Method::PUT,
            "/",
            "acl",
            {
                let mut headers = vec![(
                    "x-amz-grant-read-acp".to_string(),
                    format!("id=\"{}\"", canonical_id.as_str()),
                )];
                headers.extend(checksum_header_pairs(&[]));
                headers
            },
            vec![],
        );
        fe.dispatch_routed(
            &put_req,
            &test_auth(),
            S3Operation::PutBucketAcl {
                bucket: test_bucket_name("mybucket"),
            },
        )
        .unwrap();

        let get_req = new_req(http::Method::GET, "/", "acl", vec![], vec![]);
        let resp = fe
            .dispatch_routed(
                &get_req,
                &test_auth(),
                S3Operation::GetBucketAcl {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains(canonical_id.as_str()));
    }

    #[test]
    fn bucket_policy_put_get_delete_round_trip() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let policy = "{\n  \"Statement\": {\n    \"Resource\": [\"arn:aws:s3:::mybucket\"],\n    \"Action\": [\"s3:ListBucket\"],\n    \"Principal\": {\"AWS\": \"arn:aws:iam::123456789012:root\"},\n    \"Effect\": \"Allow\",\n    \"Sid\": \"One\"\n  },\n  \"Version\": \"2012-10-17\"\n}";
        let expected_policy = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Sid\":\"One\",\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"arn:aws:iam::123456789012:root\"},\"Action\":\"s3:ListBucket\",\"Resource\":\"arn:aws:s3:::mybucket\"}]}";
        let put_req = new_req(
            http::Method::PUT,
            "/",
            "policy",
            checksum_header_pairs(policy.as_bytes()),
            policy.as_bytes().to_vec(),
        );
        let put_resp = fe
            .dispatch_routed(
                &put_req,
                &test_auth(),
                S3Operation::PutBucketPolicy {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(put_resp.status_code, 204);

        let get_req = new_req(http::Method::GET, "/", "policy", vec![], vec![]);
        let get_resp = fe
            .dispatch_routed(
                &get_req,
                &test_auth(),
                S3Operation::GetBucketPolicy {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(get_resp.status_code, 200);
        assert_eq!(String::from_utf8(get_resp.body).unwrap(), expected_policy);
        assert!(get_resp
            .headers
            .iter()
            .any(|(name, value)| { name == "Content-Type" && value == "application/json" }));

        let delete_req = new_req(http::Method::DELETE, "/", "policy", vec![], vec![]);
        let delete_resp = fe
            .dispatch_routed(
                &delete_req,
                &test_auth(),
                S3Operation::DeleteBucketPolicy {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(delete_resp.status_code, 204);

        match fe.dispatch_routed(
            &get_req,
            &test_auth(),
            S3Operation::GetBucketPolicy {
                bucket: test_bucket_name("mybucket"),
            },
        ) {
            Err(ServerError::NoSuchBucketPolicy { bucket }) => assert_eq!(bucket, "mybucket"),
            Err(e) => panic!("expected NoSuchBucketPolicy, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn put_bucket_policy_rejects_invalid_utf8() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let put_req = new_req(
            http::Method::PUT,
            "/",
            "policy",
            checksum_header_pairs(&[0xff, 0xfe, 0xfd]),
            vec![0xff, 0xfe, 0xfd],
        );
        match fe.dispatch_routed(
            &put_req,
            &test_auth(),
            S3Operation::PutBucketPolicy {
                bucket: test_bucket_name("mybucket"),
            },
        ) {
            Err(ServerError::InvalidArgument { reason }) => {
                assert!(reason.contains("bucket policy JSON body"));
            }
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn put_bucket_policy_rejects_invalid_json() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let put_req = new_req(
            http::Method::PUT,
            "/",
            "policy",
            checksum_header_pairs(b"{"),
            b"{".to_vec(),
        );
        match fe.dispatch_routed(
            &put_req,
            &test_auth(),
            S3Operation::PutBucketPolicy {
                bucket: test_bucket_name("mybucket"),
            },
        ) {
            Err(ServerError::MalformedPolicy { reason, .. }) => {
                assert!(reason.contains("Policies must be valid JSON"));
            }
            Err(e) => panic!("expected MalformedPolicy, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    #[allow(clippy::format_push_string)]
    fn put_bucket_policy_rejects_normalized_policy_over_20kb() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let mut policy = String::from("{\"Version\":\"2012-10-17\",\"Statement\":[");
        let mut statement_count = 0usize;
        while policy.len() <= auth::bucket_policy::MAX_BUCKET_POLICY_BYTES {
            if statement_count > 0 {
                policy.push(',');
            }
            policy.push_str(&format!(
                "{{\"Sid\":\"Stmt{statement_count:04}\",\"Effect\":\"Allow\",\"Principal\":{{\"AWS\":\"arn:aws:iam::123456789012:root\"}},\"Action\":\"s3:GetObject\",\"Resource\":\"arn:aws:s3:::mybucket/path-{statement_count:04}*\"}}"
            ));
            statement_count += 1;
        }
        policy.push_str("]}");
        let put_req = new_req(
            http::Method::PUT,
            "/",
            "policy",
            checksum_header_pairs(policy.as_bytes()),
            policy.into_bytes(),
        );
        match fe.dispatch_routed(
            &put_req,
            &test_auth(),
            S3Operation::PutBucketPolicy {
                bucket: test_bucket_name("mybucket"),
            },
        ) {
            Err(ServerError::MalformedPolicy { reason, .. }) => {
                assert_eq!(
                    reason,
                    format!(
                        "Normalized policy document exceeds the maximum allowed size of {} bytes",
                        auth::bucket_policy::MAX_BUCKET_POLICY_BYTES
                    )
                );
            }
            Err(e) => panic!("expected MalformedPolicy, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn get_bucket_policy_status_renders_xml() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");
        fe.coordinator
            .put_bucket_policy(&crate::coordinator::PutBucketPolicyRequest {
                bucket: test_bucket_request("mybucket"),
                config: r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::mybucket"}]}"#,
                confirm_remove_self_bucket_access: false,
            })
            .unwrap();

        let get_req = new_req(http::Method::GET, "/", "policyStatus", vec![], vec![]);
        let get_resp = fe
            .dispatch_routed(
                &get_req,
                &test_auth(),
                S3Operation::GetBucketPolicyStatus {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(get_resp.status_code, 200);
        let body = String::from_utf8(get_resp.body).unwrap();
        assert!(body.contains("<PolicyStatus"));
        assert!(body.contains("<IsPublic>true</IsPublic>"));
    }

    #[test]
    fn bucket_lifecycle_put_get_delete_round_trip() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let lifecycle = br#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
  <Rule>
    <ID>expire-current</ID>
    <Filter><Prefix>logs/</Prefix></Filter>
    <Status>Enabled</Status>
    <Expiration><Days>3</Days></Expiration>
  </Rule>
</LifecycleConfiguration>"#;
        let expected = xml::get_bucket_lifecycle_configuration_xml(
            &xml::parse_bucket_lifecycle_configuration_xml(lifecycle).unwrap(),
        );

        let put_req = new_req(
            http::Method::PUT,
            "/",
            "lifecycle",
            vec![("Content-MD5".to_string(), content_md5_value(lifecycle))],
            lifecycle.to_vec(),
        );
        let put_resp = fe
            .dispatch_routed(
                &put_req,
                &test_auth(),
                S3Operation::PutBucketLifecycle {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(put_resp.status_code, 200);

        let get_req = new_req(http::Method::GET, "/", "lifecycle", vec![], vec![]);
        let get_resp = fe
            .dispatch_routed(
                &get_req,
                &test_auth(),
                S3Operation::GetBucketLifecycle {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(get_resp.status_code, 200);
        assert_eq!(find_header(&get_resp, "Content-Type"), None);
        assert_eq!(
            String::from_utf8(get_resp.into_test_body_bytes().unwrap()).unwrap(),
            expected
        );

        let delete_req = new_req(http::Method::DELETE, "/", "lifecycle", vec![], vec![]);
        let delete_resp = fe
            .dispatch_routed(
                &delete_req,
                &test_auth(),
                S3Operation::DeleteBucketLifecycle {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(delete_resp.status_code, 204);

        match fe.dispatch_routed(
            &get_req,
            &test_auth(),
            S3Operation::GetBucketLifecycle {
                bucket: test_bucket_name("mybucket"),
            },
        ) {
            Err(ServerError::NoSuchLifecycleConfiguration { bucket }) => {
                assert_eq!(bucket, "mybucket");
            }
            Err(e) => panic!("expected NoSuchLifecycleConfiguration, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn bucket_lifecycle_put_without_ids_gets_generated_rule_ids() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let lifecycle = br#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
  <Rule>
    <Filter><Prefix>test1/</Prefix></Filter>
    <Status>Enabled</Status>
    <Expiration><Days>31</Days></Expiration>
  </Rule>
  <Rule>
    <Filter><Prefix>test2/</Prefix></Filter>
    <Status>Enabled</Status>
    <Expiration><Days>120</Days></Expiration>
  </Rule>
</LifecycleConfiguration>"#;

        let put_req = new_req(
            http::Method::PUT,
            "/",
            "lifecycle",
            vec![("Content-MD5".to_string(), content_md5_value(lifecycle))],
            lifecycle.to_vec(),
        );
        let put_resp = fe
            .dispatch_routed(
                &put_req,
                &test_auth(),
                S3Operation::PutBucketLifecycle {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(put_resp.status_code, 200);

        let get_req = new_req(http::Method::GET, "/", "lifecycle", vec![], vec![]);
        let get_resp = fe
            .dispatch_routed(
                &get_req,
                &test_auth(),
                S3Operation::GetBucketLifecycle {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(get_resp.status_code, 200);

        let body = String::from_utf8(get_resp.body).unwrap();
        let parsed = xml::parse_bucket_lifecycle_configuration_xml(body.as_bytes()).unwrap();
        assert_eq!(parsed.rules.len(), 2);
        let mut ids = std::collections::HashSet::new();
        for rule in &parsed.rules {
            let id = rule.id.as_deref().expect("generated lifecycle rule ID");
            assert!(!id.is_empty());
            assert!(ids.insert(id.to_string()), "duplicate generated ID {id}");
        }
    }

    #[test]
    fn get_bucket_lifecycle_absent_returns_no_such_lifecycle_configuration() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let get_req = new_req(http::Method::GET, "/", "lifecycle", vec![], vec![]);
        match fe.dispatch_routed(
            &get_req,
            &test_auth(),
            S3Operation::GetBucketLifecycle {
                bucket: test_bucket_name("mybucket"),
            },
        ) {
            Err(ServerError::NoSuchLifecycleConfiguration { bucket }) => {
                assert_eq!(bucket, "mybucket");
            }
            Err(e) => panic!("expected NoSuchLifecycleConfiguration, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn put_bucket_lifecycle_rejects_invalid_status() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let lifecycle = br#"<LifecycleConfiguration>
  <Rule>
    <Status>enabled</Status>
    <Expiration><Days>1</Days></Expiration>
  </Rule>
</LifecycleConfiguration>"#;
        let put_req = new_req(
            http::Method::PUT,
            "/",
            "lifecycle",
            vec![("Content-MD5".to_string(), content_md5_value(lifecycle))],
            lifecycle.to_vec(),
        );
        match fe.dispatch_routed(
            &put_req,
            &test_auth(),
            S3Operation::PutBucketLifecycle {
                bucket: test_bucket_name("mybucket"),
            },
        ) {
            Err(ServerError::MalformedXML { .. }) => {}
            Err(e) => panic!("expected MalformedXML, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn put_bucket_lifecycle_rejects_invalid_date_as_malformed_xml() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let lifecycle = br#"<LifecycleConfiguration>
  <Rule>
    <Status>Enabled</Status>
    <Expiration><Date>20200101</Date></Expiration>
  </Rule>
</LifecycleConfiguration>"#;
        let put_req = new_req(
            http::Method::PUT,
            "/",
            "lifecycle",
            vec![("Content-MD5".to_string(), content_md5_value(lifecycle))],
            lifecycle.to_vec(),
        );
        match fe.dispatch_routed(
            &put_req,
            &test_auth(),
            S3Operation::PutBucketLifecycle {
                bucket: test_bucket_name("mybucket"),
            },
        ) {
            Err(ServerError::MalformedXML { .. }) => {}
            Err(e) => panic!("expected MalformedXML, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn put_bucket_lifecycle_missing_content_md5_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let lifecycle = br#"<LifecycleConfiguration>
  <Rule>
    <Status>Enabled</Status>
    <Expiration><Days>1</Days></Expiration>
  </Rule>
</LifecycleConfiguration>"#;
        let put_req = new_req(
            http::Method::PUT,
            "/",
            "lifecycle",
            vec![],
            lifecycle.to_vec(),
        );
        match fe.dispatch_routed(
            &put_req,
            &test_auth(),
            S3Operation::PutBucketLifecycle {
                bucket: test_bucket_name("mybucket"),
            },
        ) {
            Err(ServerError::InvalidRequest { reason }) => {
                assert_eq!(
                    reason,
                    "Missing required header for this request: Content-MD5"
                );
            }
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn put_bucket_lifecycle_invalid_content_md5_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let lifecycle = br#"<LifecycleConfiguration>
  <Rule>
    <Status>Enabled</Status>
    <Expiration><Days>1</Days></Expiration>
  </Rule>
</LifecycleConfiguration>"#;
        let put_req = new_req(
            http::Method::PUT,
            "/",
            "lifecycle",
            vec![("Content-MD5".to_string(), "not-base64".to_string())],
            lifecycle.to_vec(),
        );
        match fe.dispatch_routed(
            &put_req,
            &test_auth(),
            S3Operation::PutBucketLifecycle {
                bucket: test_bucket_name("mybucket"),
            },
        ) {
            Err(ServerError::InvalidDigest) => {}
            Err(e) => panic!("expected InvalidDigest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn put_bucket_lifecycle_bad_content_md5_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let lifecycle = br#"<LifecycleConfiguration>
  <Rule>
    <Status>Enabled</Status>
    <Expiration><Days>1</Days></Expiration>
  </Rule>
</LifecycleConfiguration>"#;
        let put_req = new_req(
            http::Method::PUT,
            "/",
            "lifecycle",
            vec![(
                "Content-MD5".to_string(),
                "AAAAAAAAAAAAAAAAAAAAAA==".to_string(),
            )],
            lifecycle.to_vec(),
        );
        match fe.dispatch_routed(
            &put_req,
            &test_auth(),
            S3Operation::PutBucketLifecycle {
                bucket: test_bucket_name("mybucket"),
            },
        ) {
            Err(ServerError::ContentMd5Mismatch) => {}
            Err(e) => panic!("expected ContentMd5Mismatch, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn put_object_emits_lifecycle_expiration_header() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let lifecycle = br#"<LifecycleConfiguration>
  <Rule>
    <ID>expire-current</ID>
    <Filter><Prefix>logs/</Prefix></Filter>
    <Status>Enabled</Status>
    <Expiration><Days>1</Days></Expiration>
  </Rule>
</LifecycleConfiguration>"#;
        let put_lifecycle_req = new_req(
            http::Method::PUT,
            "/",
            "lifecycle",
            vec![("Content-MD5".to_string(), content_md5_value(lifecycle))],
            lifecycle.to_vec(),
        );
        fe.dispatch_routed(
            &put_lifecycle_req,
            &test_auth(),
            S3Operation::PutBucketLifecycle {
                bucket: test_bucket_name("mybucket"),
            },
        )
        .unwrap();

        let put_req = new_req(
            http::Method::PUT,
            "/",
            "",
            vec![],
            b"hello lifecycle".to_vec(),
        );
        let put_resp = fe
            .dispatch_routed(
                &put_req,
                &test_auth(),
                S3Operation::PutObject {
                    bucket: test_bucket_name("mybucket"),
                    key: "logs/app.txt".to_string(),
                },
            )
            .unwrap();
        let expiration = find_header(&put_resp, "x-amz-expiration").unwrap();
        assert!(expiration.contains("expiry-date=\""));
        assert!(expiration.contains("rule-id=\"expire-current\""));
    }

    #[test]
    fn get_object_explicit_current_version_suppresses_lifecycle_expiration_header() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");
        fe.coordinator
            .put_bucket_versioning(&crate::coordinator::PutBucketVersioningRequest {
                bucket: test_bucket_request("mybucket"),
                state: s3_types::BucketVersioningState::Enabled,
            })
            .unwrap();

        let lifecycle = br#"<LifecycleConfiguration>
  <Rule>
    <ID>expire-current</ID>
    <Filter><Prefix>logs/</Prefix></Filter>
    <Status>Enabled</Status>
    <Expiration><Days>1</Days></Expiration>
  </Rule>
</LifecycleConfiguration>"#;
        let put_lifecycle_req = new_req(
            http::Method::PUT,
            "/",
            "lifecycle",
            vec![("Content-MD5".to_string(), content_md5_value(lifecycle))],
            lifecycle.to_vec(),
        );
        fe.dispatch_routed(
            &put_lifecycle_req,
            &test_auth(),
            S3Operation::PutBucketLifecycle {
                bucket: test_bucket_name("mybucket"),
            },
        )
        .unwrap();

        let put_req = new_req(
            http::Method::PUT,
            "/",
            "",
            vec![],
            b"hello lifecycle".to_vec(),
        );
        let put_resp = fe
            .dispatch_routed(
                &put_req,
                &test_auth(),
                S3Operation::PutObject {
                    bucket: test_bucket_name("mybucket"),
                    key: "logs/app.txt".to_string(),
                },
            )
            .unwrap();
        let version_id = find_header(&put_resp, "x-amz-version-id")
            .expect("versioned put should return version id")
            .to_string();

        let get_req = new_req(
            http::Method::GET,
            "/",
            &format!("versionId={version_id}"),
            vec![],
            vec![],
        );
        let get_resp = fe
            .dispatch_routed(
                &get_req,
                &test_auth(),
                S3Operation::GetObject {
                    bucket: test_bucket_name("mybucket"),
                    key: "logs/app.txt".to_string(),
                },
            )
            .unwrap();
        assert!(find_header(&get_resp, "x-amz-expiration").is_none());
    }

    #[test]
    fn head_object_explicit_null_version_suppresses_lifecycle_expiration_header() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let lifecycle = br#"<LifecycleConfiguration>
  <Rule>
    <ID>expire-current</ID>
    <Filter><Prefix>logs/</Prefix></Filter>
    <Status>Enabled</Status>
    <Expiration><Days>1</Days></Expiration>
  </Rule>
</LifecycleConfiguration>"#;
        let put_lifecycle_req = new_req(
            http::Method::PUT,
            "/",
            "lifecycle",
            vec![("Content-MD5".to_string(), content_md5_value(lifecycle))],
            lifecycle.to_vec(),
        );
        fe.dispatch_routed(
            &put_lifecycle_req,
            &test_auth(),
            S3Operation::PutBucketLifecycle {
                bucket: test_bucket_name("mybucket"),
            },
        )
        .unwrap();

        let put_req = new_req(
            http::Method::PUT,
            "/",
            "",
            vec![],
            b"hello lifecycle".to_vec(),
        );
        fe.dispatch_routed(
            &put_req,
            &test_auth(),
            S3Operation::PutObject {
                bucket: test_bucket_name("mybucket"),
                key: "logs/app.txt".to_string(),
            },
        )
        .unwrap();

        let head_req = new_req(http::Method::HEAD, "/", "versionId=null", vec![], vec![]);
        let head_resp = fe
            .dispatch_routed(
                &head_req,
                &test_auth(),
                S3Operation::HeadObject {
                    bucket: test_bucket_name("mybucket"),
                    key: "logs/app.txt".to_string(),
                },
            )
            .unwrap();
        assert!(find_header(&head_resp, "x-amz-expiration").is_none());
    }

    #[test]
    fn multipart_lifecycle_abort_headers_are_emitted_on_create_and_list_parts() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let lifecycle = br#"<LifecycleConfiguration>
  <Rule>
    <ID>abort-stale</ID>
    <Filter><Prefix>uploads/</Prefix></Filter>
    <Status>Enabled</Status>
    <AbortIncompleteMultipartUpload><DaysAfterInitiation>7</DaysAfterInitiation></AbortIncompleteMultipartUpload>
  </Rule>
</LifecycleConfiguration>"#;
        let put_lifecycle_req = new_req(
            http::Method::PUT,
            "/",
            "lifecycle",
            vec![("Content-MD5".to_string(), content_md5_value(lifecycle))],
            lifecycle.to_vec(),
        );
        fe.dispatch_routed(
            &put_lifecycle_req,
            &test_auth(),
            S3Operation::PutBucketLifecycle {
                bucket: test_bucket_name("mybucket"),
            },
        )
        .unwrap();

        let create_req = new_req(http::Method::POST, "/", "uploads", vec![], vec![]);
        let create_resp = fe
            .dispatch_routed(
                &create_req,
                &test_auth(),
                S3Operation::CreateMultipartUpload {
                    bucket: test_bucket_name("mybucket"),
                    key: "uploads/archive.bin".to_string(),
                },
            )
            .unwrap();
        assert!(find_header(&create_resp, "x-amz-abort-date").is_some());
        assert_eq!(
            find_header(&create_resp, "x-amz-abort-rule-id"),
            Some("abort-stale")
        );

        let create_body = response_body(create_resp);
        let body = std::str::from_utf8(&create_body).unwrap();
        let start = body.find("<UploadId>").unwrap() + "<UploadId>".len();
        let end = start + body[start..].find("</UploadId>").unwrap();
        let upload_id = &body[start..end];

        let list_req = new_req(
            http::Method::GET,
            "/",
            &format!("uploadId={upload_id}"),
            vec![],
            vec![],
        );
        let list_resp = fe
            .dispatch_routed(
                &list_req,
                &test_auth(),
                S3Operation::ListParts {
                    bucket: test_bucket_name("mybucket"),
                    key: "uploads/archive.bin".to_string(),
                },
            )
            .unwrap();
        assert!(find_header(&list_resp, "x-amz-abort-date").is_some());
        assert_eq!(
            find_header(&list_resp, "x-amz-abort-rule-id"),
            Some("abort-stale")
        );
    }

    #[test]
    fn put_object_accepts_header_grants_and_renders_them() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        fe.coordinator
            .create_bucket(&crate::coordinator::CreateBucketRequest {
                name: parse_bucket_name("mybucket").unwrap(),
                requester: crate::coordinator::test_helpers::requester("testuser"),
                namespace: BucketNamespace::Global,
                acl: crate::coordinator::CreateBucketAcl::DefaultPrivate,
                ownership: crate::coordinator::BucketObjectOwnership::ObjectWriter,
                object_lock_enabled: false,
            })
            .unwrap();
        let canonical_id = s3_types::CanonicalUserId::from_principal("grantee-a");

        let put_req = new_req(
            http::Method::PUT,
            "/",
            "",
            vec![(
                "x-amz-grant-read-acp".to_string(),
                format!("id=\"{}\"", canonical_id.as_str()),
            )],
            b"data".to_vec(),
        );
        fe.dispatch_routed(
            &put_req,
            &test_auth(),
            S3Operation::PutObject {
                bucket: test_bucket_name("mybucket"),
                key: "mykey".to_string(),
            },
        )
        .unwrap();

        let get_req = new_req(http::Method::GET, "/", "acl", vec![], vec![]);
        let resp = fe
            .dispatch_routed(
                &get_req,
                &test_auth(),
                S3Operation::GetObjectAcl {
                    bucket: test_bucket_name("mybucket"),
                    key: "mykey".to_string(),
                },
            )
            .unwrap();
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains(canonical_id.as_str()));
    }

    #[test]
    fn put_bucket_acl_accepts_authenticated_users_header_grant() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let put_req = new_req(
            http::Method::PUT,
            "/",
            "acl",
            {
                let mut headers = vec![(
                    "x-amz-grant-read".to_string(),
                    format!(
                        "uri=\"{}\"",
                        s3_types::AclGrantee::authenticated_users_uri()
                    ),
                )];
                headers.extend(checksum_header_pairs(&[]));
                headers
            },
            vec![],
        );
        fe.dispatch_routed(
            &put_req,
            &test_auth(),
            S3Operation::PutBucketAcl {
                bucket: test_bucket_name("mybucket"),
            },
        )
        .unwrap();

        let get_req = new_req(http::Method::GET, "/", "acl", vec![], vec![]);
        let resp = fe
            .dispatch_routed(
                &get_req,
                &test_auth(),
                S3Operation::GetBucketAcl {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains(s3_types::AclGrantee::authenticated_users_uri()));
    }

    #[test]
    fn create_bucket_with_object_lock_header_enables_bucket_object_lock() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());

        let req = new_req(
            http::Method::PUT,
            "/mybucket",
            "",
            vec![(
                "x-amz-bucket-object-lock-enabled".to_string(),
                "true".to_string(),
            )],
            vec![],
        );
        let resp = fe
            .dispatch_routed(
                &req,
                &test_auth(),
                S3Operation::CreateBucket {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(resp.status_code, 200);
        let storage_route_admission = fe.coordinator.admit_storage_route_for_request().unwrap();
        assert_eq!(
            fe.coordinator
                .get_bucket_versioning_on_admitted_route(
                    &storage_route_admission,
                    &test_bucket_request("mybucket"),
                )
                .unwrap(),
            s3_types::BucketVersioningState::Enabled
        );
        assert_eq!(
            fe.coordinator
                .get_bucket_object_lock_configuration_on_admitted_route(
                    &storage_route_admission,
                    &test_bucket_request("mybucket"),
                )
                .unwrap(),
            s3_types::BucketObjectLockConfig {
                enabled: true,
                default_retention: None,
            }
        );
    }

    #[test]
    fn bucket_object_lock_operations_are_implemented() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());

        let create_req = new_req(http::Method::PUT, "/mybucket", "", vec![], vec![]);
        fe.dispatch_routed(
            &create_req,
            &test_auth(),
            S3Operation::CreateBucket {
                bucket: test_bucket_name("mybucket"),
            },
        )
        .unwrap();
        fe.coordinator
            .put_bucket_versioning(&crate::coordinator::PutBucketVersioningRequest {
                bucket: test_bucket_request("mybucket"),
                state: s3_types::BucketVersioningState::Enabled,
            })
            .unwrap();

        let put_req = new_req(
            http::Method::PUT,
            "/mybucket",
            "object-lock",
            checksum_header_pairs(
                br#"
                <ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                  <ObjectLockEnabled>Enabled</ObjectLockEnabled>
                  <Rule>
                    <DefaultRetention>
                      <Mode>GOVERNANCE</Mode>
                      <Days>1</Days>
                    </DefaultRetention>
                  </Rule>
                </ObjectLockConfiguration>
            "#,
            ),
            br#"
                <ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                  <ObjectLockEnabled>Enabled</ObjectLockEnabled>
                  <Rule>
                    <DefaultRetention>
                      <Mode>GOVERNANCE</Mode>
                      <Days>1</Days>
                    </DefaultRetention>
                  </Rule>
                </ObjectLockConfiguration>
            "#
            .to_vec(),
        );
        let put_resp = fe
            .dispatch_routed(
                &put_req,
                &test_auth(),
                S3Operation::PutBucketObjectLockConfiguration {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(put_resp.status_code, 200);

        let get_req = new_req(
            http::Method::GET,
            "/mybucket",
            "object-lock",
            vec![],
            vec![],
        );
        let get_resp = fe
            .dispatch_routed(
                &get_req,
                &test_auth(),
                S3Operation::GetBucketObjectLockConfiguration {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(get_resp.status_code, 200);
        let body = String::from_utf8(get_resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("<ObjectLockEnabled>Enabled</ObjectLockEnabled>"));
        assert!(body.contains("<Days>1</Days>"));
    }

    #[test]
    fn put_object_acl_accepts_write_header_grant() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");
        let metadata = crate::metadata_blob::MetadataBlob::default();
        let system_metadata = server_core::system_metadata::SystemMetadata::default();
        fe.coordinator
            .put_object(&crate::coordinator::PutObjectRequest {
                object: test_object_request("mybucket", "mykey"),
                data: b"data",
                metadata: &metadata,
                system_metadata: &system_metadata,
                tags: None,
                cond: &crate::conditional::WriteCondition::default(),
                acl: crate::coordinator::PutObjectAcl::None.into(),
                policy_context: crate::coordinator::PutObjectPolicyContext::default(),
                object_lock: Default::default(),
                encryption: crate::coordinator::WriteEncryptionRequest::none(),
            })
            .unwrap();

        let canonical_id = s3_types::CanonicalUserId::from_principal("grantee-a");
        let put_req = new_req(
            http::Method::PUT,
            "/",
            "acl",
            {
                let mut headers = vec![(
                    "x-amz-grant-write".to_string(),
                    format!("id=\"{}\"", canonical_id.as_str()),
                )];
                headers.extend(checksum_header_pairs(&[]));
                headers
            },
            vec![],
        );
        let put_resp = fe
            .dispatch_routed(
                &put_req,
                &test_auth(),
                S3Operation::PutObjectAcl {
                    bucket: test_bucket_name("mybucket"),
                    key: "mykey".to_string(),
                },
            )
            .unwrap();
        assert_eq!(put_resp.status_code, 200);

        let acl = fe
            .coordinator
            .get_object_acl(&ObjectVersionRequest::new(
                parse_bucket_name("mybucket").unwrap(),
                parse_object_key("mykey").unwrap(),
                None,
                crate::coordinator::test_helpers::requester("testuser"),
                None,
            ))
            .unwrap();
        assert!(acl
            .acl_grants
            .allows_canonical_user(&canonical_id, s3_types::AclPermission::Write));
    }

    fn content_md5_value(body: &[u8]) -> String {
        use base64::Engine;

        let digest = argmin_crypto::digest::md5(body);
        base64::engine::general_purpose::STANDARD.encode(&digest[..])
    }

    fn checksum_crc32_value(body: &[u8]) -> String {
        use base64::Engine;

        let crc = checksum::crc32::checksum(body);
        base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes())
    }

    fn checksum_header_pairs(body: &[u8]) -> Vec<(String, String)> {
        vec![
            (
                "x-amz-sdk-checksum-algorithm".to_string(),
                "CRC32".to_string(),
            ),
            (
                "x-amz-checksum-crc32".to_string(),
                checksum_crc32_value(body),
            ),
        ]
    }

    fn assert_missing_request_checksum_rejected(
        method: http::Method,
        query: &str,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
        op: S3Operation,
        expected_reason: &str,
    ) {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(method, "", query, headers, body);
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequest { reason }) => {
                assert_eq!(reason, expected_reason);
            }
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    fn assert_sdk_checksum_request_accepted(
        method: http::Method,
        query: &str,
        mut headers: Vec<(String, String)>,
        body: Vec<u8>,
        op: S3Operation,
    ) {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        headers.extend(checksum_header_pairs(&body));
        let req = new_req(method, "", query, headers, body);
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert!(
            matches!(resp.status_code, 200 | 204),
            "expected success, got status {}",
            resp.status_code
        );
    }

    // ── UploadPart validation ────────────────────────────────────────

    #[test]
    fn upload_part_copy_missing_upload_id() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::PUT,
            "/mybucket/mykey",
            "partNumber=1",
            vec![("x-amz-copy-source".to_string(), "/src/key".to_string())],
            vec![],
        );
        let op = S3Operation::UploadPart {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::UploadPartCopyMissingUploadId) => {}
            Err(e) => panic!("expected UploadPartCopyMissingUploadId, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn upload_part_invalid_upload_id_returns_no_such_upload() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let invalid_upload_id = storage::UploadId::overlong_for_test();
        let req = make_req(&format!("partNumber=1&uploadId={invalid_upload_id}"));
        let op = S3Operation::UploadPart {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::NoSuchUpload { upload_id }) => {
                assert_eq!(upload_id, invalid_upload_id);
            }
            Err(e) => panic!("expected NoSuchUpload, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn upload_part_invalid_part_number_does_not_hide_missing_upload() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = make_req("partNumber=abc&uploadId=xyz");
        let op = S3Operation::UploadPart {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::NoSuchUpload { upload_id }) => {
                assert_eq!(upload_id, "xyz");
            }
            Err(e) => panic!("expected NoSuchUpload, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn prepare_streaming_part_sigv4_header_auth_requires_content_sha256() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::PUT,
            "",
            "",
            vec![
                (
                    "authorization".to_string(),
                    "AWS4-HMAC-SHA256 Credential=test/20260318/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=deadbeef".to_string(),
                ),
                ("x-amz-date".to_string(), "20260318T000000Z".to_string()),
            ],
            b"hello world".to_vec(),
        );
        match fe.prepare_streaming_part(&req, "mybucket", "mykey", "upload-id", "1") {
            Err(ServerError::InvalidRequest { reason }) => {
                assert_eq!(
                    reason,
                    "Missing required header for this request: x-amz-content-sha256"
                );
            }
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn put_object_invalid_content_md5_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::PUT,
            "",
            "",
            vec![("Content-MD5".to_string(), "not-base64".to_string())],
            b"hello world".to_vec(),
        );
        let op = S3Operation::PutObject {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidDigest) => {}
            Err(e) => panic!("expected InvalidDigest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn put_object_bad_content_md5_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::PUT,
            "",
            "",
            vec![(
                "Content-MD5".to_string(),
                "AAAAAAAAAAAAAAAAAAAAAA==".to_string(),
            )],
            b"hello world".to_vec(),
        );
        let op = S3Operation::PutObject {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::ContentMd5Mismatch) => {}
            Err(e) => panic!("expected ContentMd5Mismatch, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn put_object_without_checksum_allowed_when_object_lock_not_requested() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(http::Method::PUT, "", "", vec![], b"hello world".to_vec());
        let op = S3Operation::PutObject {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);
    }

    #[test]
    fn put_bucket_versioning_missing_request_checksum_allowed() {
        let body =
            b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>".to_vec();
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");
        let req = new_req(http::Method::PUT, "", "", vec![], body);
        let resp = fe
            .dispatch_routed(
                &req,
                &test_auth(),
                S3Operation::PutBucketVersioning {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(resp.status_code, 200);
    }

    #[test]
    fn put_bucket_object_lock_configuration_missing_request_checksum_rejected() {
        let body = br#"<ObjectLockConfiguration>
  <ObjectLockEnabled>Enabled</ObjectLockEnabled>
  <Rule>
    <DefaultRetention>
      <Mode>GOVERNANCE</Mode>
      <Days>1</Days>
    </DefaultRetention>
  </Rule>
</ObjectLockConfiguration>"#
            .to_vec();
        assert_missing_request_checksum_rejected(
            http::Method::PUT,
            "",
            vec![],
            body,
            S3Operation::PutBucketObjectLockConfiguration {
                bucket: test_bucket_name("mybucket"),
            },
            "Missing required header for this request: Content-MD5 OR x-amz-checksum-*",
        );
    }

    #[test]
    fn put_bucket_encryption_missing_request_checksum_allowed() {
        let body = br#"<ServerSideEncryptionConfiguration>
  <Rule>
    <ApplyServerSideEncryptionByDefault>
      <SSEAlgorithm>AES256</SSEAlgorithm>
    </ApplyServerSideEncryptionByDefault>
  </Rule>
</ServerSideEncryptionConfiguration>"#
            .to_vec();
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");
        let req = new_req(http::Method::PUT, "", "", vec![], body);
        let resp = fe
            .dispatch_routed(
                &req,
                &test_auth(),
                S3Operation::PutBucketEncryption {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(resp.status_code, 200);
    }

    #[test]
    fn put_bucket_cors_missing_request_checksum_rejected() {
        let body = br#"<CORSConfiguration>
  <CORSRule>
    <AllowedMethod>GET</AllowedMethod>
    <AllowedOrigin>https://example.com</AllowedOrigin>
  </CORSRule>
</CORSConfiguration>"#
            .to_vec();
        assert_missing_request_checksum_rejected(
            http::Method::PUT,
            "",
            vec![],
            body,
            S3Operation::PutBucketCors {
                bucket: test_bucket_name("mybucket"),
            },
            "Missing required header for this request: Content-MD5 OR x-amz-checksum-*",
        );
    }

    #[test]
    fn put_bucket_tagging_missing_request_checksum_rejected() {
        let body =
            br#"<Tagging><TagSet><Tag><Key>a</Key><Value>b</Value></Tag></TagSet></Tagging>"#
                .to_vec();
        assert_missing_request_checksum_rejected(
            http::Method::PUT,
            "",
            vec![],
            body,
            S3Operation::PutBucketTagging {
                bucket: test_bucket_name("mybucket"),
            },
            "Missing required header for this request: Content-MD5 OR x-amz-checksum-*",
        );
    }

    #[test]
    fn put_bucket_abac_missing_request_checksum_rejected() {
        let body = br#"<AbacStatus><Status>Enabled</Status></AbacStatus>"#.to_vec();
        assert_missing_request_checksum_rejected(
            http::Method::PUT,
            "",
            vec![],
            body,
            S3Operation::PutBucketAbac {
                bucket: test_bucket_name("mybucket"),
            },
            "Missing required header for this request: Content-MD5 OR x-amz-checksum-*",
        );
    }

    #[test]
    fn put_bucket_abac_sdk_checksum_header_accepted() {
        let body = br#"<AbacStatus><Status>Enabled</Status></AbacStatus>"#.to_vec();
        assert_sdk_checksum_request_accepted(
            http::Method::PUT,
            "",
            vec![],
            body,
            S3Operation::PutBucketAbac {
                bucket: test_bucket_name("mybucket"),
            },
        );
    }

    #[test]
    fn put_bucket_abac_content_md5_accepted() {
        let body = br#"<AbacStatus><Status>Enabled</Status></AbacStatus>"#.to_vec();
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::PUT,
            "",
            "",
            vec![("Content-MD5".to_string(), content_md5_value(&body))],
            body,
        );
        let resp = fe
            .dispatch_routed(
                &req,
                &test_auth(),
                S3Operation::PutBucketAbac {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(resp.status_code, 200);
    }

    #[test]
    fn put_object_retention_missing_request_checksum_rejected() {
        let body = br#"<Retention>
  <Mode>GOVERNANCE</Mode>
  <RetainUntilDate>2099-01-01T00:00:00Z</RetainUntilDate>
</Retention>"#
            .to_vec();
        assert_missing_request_checksum_rejected(
            http::Method::PUT,
            "",
            vec![],
            body,
            S3Operation::PutObjectRetention {
                bucket: test_bucket_name("mybucket"),
                key: "mykey".to_string(),
            },
            "Missing required header for this request: Content-MD5 OR x-amz-checksum-*",
        );
    }

    #[test]
    fn put_object_legal_hold_missing_request_checksum_rejected() {
        let body = br#"<LegalHold><Status>ON</Status></LegalHold>"#.to_vec();
        assert_missing_request_checksum_rejected(
            http::Method::PUT,
            "",
            vec![],
            body,
            S3Operation::PutObjectLegalHold {
                bucket: test_bucket_name("mybucket"),
                key: "mykey".to_string(),
            },
            "Missing required header for this request: Content-MD5 OR x-amz-checksum-*",
        );
    }

    #[test]
    fn put_object_tagging_missing_request_checksum_allowed() {
        let body =
            br#"<Tagging><TagSet><Tag><Key>a</Key><Value>b</Value></Tag></TagSet></Tagging>"#
                .to_vec();
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");
        let metadata = crate::metadata_blob::MetadataBlob::default();
        let system_metadata = server_core::system_metadata::SystemMetadata::default();
        fe.coordinator
            .put_object(&crate::coordinator::PutObjectRequest {
                object: test_object_request("mybucket", "mykey"),
                data: b"hello",
                metadata: &metadata,
                system_metadata: &system_metadata,
                tags: None,
                cond: &crate::conditional::WriteCondition::default(),
                acl: crate::coordinator::PutObjectAcl::None.into(),
                policy_context: crate::coordinator::PutObjectPolicyContext::default(),
                object_lock: Default::default(),
                encryption: crate::coordinator::WriteEncryptionRequest::none(),
            })
            .unwrap();
        let req = new_req(http::Method::PUT, "", "", vec![], body);
        let resp = fe
            .dispatch_routed(
                &req,
                &test_auth(),
                S3Operation::PutObjectTagging {
                    bucket: test_bucket_name("mybucket"),
                    key: "mykey".to_string(),
                },
            )
            .unwrap();
        assert_eq!(resp.status_code, 200);
    }

    #[test]
    fn put_object_acl_missing_request_checksum_allowed() {
        let canonical_id = s3_types::CanonicalUserId::from_principal("testuser");
        let body = format!(
            "<AccessControlPolicy><AccessControlList>\
             <Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\">\
             <ID>{}</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant>\
             </AccessControlList></AccessControlPolicy>",
            canonical_id.as_str()
        )
        .into_bytes();
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");
        let metadata = crate::metadata_blob::MetadataBlob::default();
        let system_metadata = server_core::system_metadata::SystemMetadata::default();
        fe.coordinator
            .put_object(&crate::coordinator::PutObjectRequest {
                object: test_object_request("mybucket", "mykey"),
                data: b"hello",
                metadata: &metadata,
                system_metadata: &system_metadata,
                tags: None,
                cond: &crate::conditional::WriteCondition::default(),
                acl: crate::coordinator::PutObjectAcl::None.into(),
                policy_context: crate::coordinator::PutObjectPolicyContext::default(),
                object_lock: Default::default(),
                encryption: crate::coordinator::WriteEncryptionRequest::none(),
            })
            .unwrap();
        let req = new_req(http::Method::PUT, "", "", vec![], body);
        let resp = fe
            .dispatch_routed(
                &req,
                &test_auth(),
                S3Operation::PutObjectAcl {
                    bucket: test_bucket_name("mybucket"),
                    key: "mykey".to_string(),
                },
            )
            .unwrap();
        assert_eq!(resp.status_code, 200);
    }

    #[test]
    fn put_object_acl_header_only_without_checksum_allowed() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");
        let metadata = crate::metadata_blob::MetadataBlob::default();
        let system_metadata = server_core::system_metadata::SystemMetadata::default();
        fe.coordinator
            .put_object(&crate::coordinator::PutObjectRequest {
                object: test_object_request("mybucket", "mykey"),
                data: b"hello",
                metadata: &metadata,
                system_metadata: &system_metadata,
                tags: None,
                cond: &crate::conditional::WriteCondition::default(),
                acl: crate::coordinator::PutObjectAcl::None.into(),
                policy_context: crate::coordinator::PutObjectPolicyContext::default(),
                object_lock: Default::default(),
                encryption: crate::coordinator::WriteEncryptionRequest::none(),
            })
            .unwrap();

        let req = new_req(
            http::Method::PUT,
            "",
            "",
            vec![("x-amz-acl".to_string(), "private".to_string())],
            vec![],
        );
        let resp = fe
            .dispatch_routed(
                &req,
                &test_auth(),
                S3Operation::PutObjectAcl {
                    bucket: test_bucket_name("mybucket"),
                    key: "mykey".to_string(),
                },
            )
            .unwrap();
        assert_eq!(resp.status_code, 200);
    }

    #[test]
    fn put_object_acl_without_acl_payload_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");
        let metadata = crate::metadata_blob::MetadataBlob::default();
        let system_metadata = server_core::system_metadata::SystemMetadata::default();
        fe.coordinator
            .put_object(&crate::coordinator::PutObjectRequest {
                object: test_object_request("mybucket", "mykey"),
                data: b"hello",
                metadata: &metadata,
                system_metadata: &system_metadata,
                tags: None,
                cond: &crate::conditional::WriteCondition::default(),
                acl: crate::coordinator::PutObjectAcl::None.into(),
                policy_context: crate::coordinator::PutObjectPolicyContext::default(),
                object_lock: Default::default(),
                encryption: crate::coordinator::WriteEncryptionRequest::none(),
            })
            .unwrap();

        let req = new_req(http::Method::PUT, "", "", vec![], vec![]);
        match fe.dispatch_routed(
            &req,
            &test_auth(),
            S3Operation::PutObjectAcl {
                bucket: test_bucket_name("mybucket"),
                key: "mykey".to_string(),
            },
        ) {
            Err(ServerError::InvalidArgument { reason }) => {
                assert_eq!(reason, "missing ACL XML body");
            }
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
            Ok(_) => panic!("expected InvalidArgument, got Ok"),
        }
    }

    #[test]
    fn put_bucket_public_access_block_missing_request_checksum_allowed() {
        let body = br#"<PublicAccessBlockConfiguration>
  <BlockPublicAcls>true</BlockPublicAcls>
  <IgnorePublicAcls>true</IgnorePublicAcls>
  <BlockPublicPolicy>true</BlockPublicPolicy>
  <RestrictPublicBuckets>true</RestrictPublicBuckets>
</PublicAccessBlockConfiguration>"#
            .to_vec();
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");
        let req = new_req(http::Method::PUT, "", "", vec![], body);
        let resp = fe
            .dispatch_routed(
                &req,
                &test_auth(),
                S3Operation::PutBucketPublicAccessBlock {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(resp.status_code, 200);
    }

    #[test]
    fn put_bucket_ownership_controls_missing_request_checksum_allowed() {
        let body = br#"<OwnershipControls><Rule><ObjectOwnership>BucketOwnerPreferred</ObjectOwnership></Rule></OwnershipControls>"#
            .to_vec();
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");
        let req = new_req(http::Method::PUT, "", "", vec![], body);
        let resp = fe
            .dispatch_routed(
                &req,
                &test_auth(),
                S3Operation::PutBucketOwnershipControls {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(resp.status_code, 200);
    }

    #[test]
    fn put_bucket_policy_missing_request_checksum_allowed() {
        let body = br#"{"Version":"2012-10-17","Statement":[{"Sid":"AllowOwnerList","Effect":"Allow","Principal":{"AWS":"arn:aws:iam::test-account-id:root"},"Action":"s3:ListBucket","Resource":"arn:aws:s3:::mybucket"}]}"#.to_vec();
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");
        let req = new_req(http::Method::PUT, "", "", vec![], body);
        let resp = fe
            .dispatch_routed(
                &req,
                &test_auth(),
                S3Operation::PutBucketPolicy {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(resp.status_code, 204);
    }

    #[test]
    fn put_bucket_acl_missing_request_checksum_allowed() {
        let canonical_id = s3_types::CanonicalUserId::from_principal("testuser");
        let body = format!(
            "<AccessControlPolicy><AccessControlList>\
             <Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\">\
             <ID>{}</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant>\
             </AccessControlList></AccessControlPolicy>",
            canonical_id.as_str()
        )
        .into_bytes();
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");
        let req = new_req(http::Method::PUT, "", "", vec![], body);
        let resp = fe
            .dispatch_routed(
                &req,
                &test_auth(),
                S3Operation::PutBucketAcl {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(resp.status_code, 200);
    }

    #[test]
    fn put_bucket_acl_header_only_without_checksum_allowed() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::PUT,
            "",
            "",
            vec![("x-amz-acl".to_string(), "private".to_string())],
            vec![],
        );
        let resp = fe
            .dispatch_routed(
                &req,
                &test_auth(),
                S3Operation::PutBucketAcl {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(resp.status_code, 200);
    }

    #[test]
    fn put_bucket_acl_anonymous_request_denied_before_checksum_validation() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::PUT,
            "",
            "",
            vec![("x-amz-acl".to_string(), "private".to_string())],
            vec![],
        );
        match fe.dispatch_routed(
            &req,
            &auth::AuthContext::anonymous(),
            S3Operation::PutBucketAcl {
                bucket: test_bucket_name("mybucket"),
            },
        ) {
            Err(ServerError::AccessDenied) => {}
            Err(err) => panic!("expected AccessDenied, got Err({err:?})"),
            Ok(_) => panic!("expected AccessDenied, got Ok"),
        }
    }

    #[test]
    fn put_bucket_lifecycle_sdk_checksum_header_accepted() {
        let body = br#"<LifecycleConfiguration>
  <Rule>
    <ID>rule1</ID>
    <Filter><Prefix>logs/</Prefix></Filter>
    <Status>Enabled</Status>
    <Expiration><Days>30</Days></Expiration>
  </Rule>
</LifecycleConfiguration>"#
            .to_vec();
        assert_sdk_checksum_request_accepted(
            http::Method::PUT,
            "",
            vec![],
            body,
            S3Operation::PutBucketLifecycle {
                bucket: test_bucket_name("mybucket"),
            },
        );
    }

    #[test]
    fn put_bucket_policy_sdk_checksum_header_accepted() {
        let body = br#"{"Version":"2012-10-17","Statement":[{"Sid":"AllowOwnerList","Effect":"Allow","Principal":{"AWS":"arn:aws:iam::test-account-id:root"},"Action":"s3:ListBucket","Resource":"arn:aws:s3:::mybucket"}]}"#.to_vec();
        assert_sdk_checksum_request_accepted(
            http::Method::PUT,
            "",
            vec![],
            body,
            S3Operation::PutBucketPolicy {
                bucket: test_bucket_name("mybucket"),
            },
        );
    }

    #[test]
    fn put_bucket_acl_sdk_checksum_header_accepted() {
        assert_sdk_checksum_request_accepted(
            http::Method::PUT,
            "",
            vec![("x-amz-acl".to_string(), "private".to_string())],
            vec![],
            S3Operation::PutBucketAcl {
                bucket: test_bucket_name("mybucket"),
            },
        );
    }

    #[test]
    fn request_checksum_algorithm_without_value_header_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::PUT,
            "",
            "lifecycle",
            vec![(
                "x-amz-sdk-checksum-algorithm".to_string(),
                "CRC32".to_string(),
            )],
            br#"<LifecycleConfiguration><Rule><ID>rule1</ID><Filter><Prefix>logs/</Prefix></Filter><Status>Enabled</Status><Expiration><Days>30</Days></Expiration></Rule></LifecycleConfiguration>"#.to_vec(),
        );
        match fe.dispatch_routed(
            &req,
            &test_auth(),
            S3Operation::PutBucketLifecycle {
                bucket: test_bucket_name("mybucket"),
            },
        ) {
            Err(ServerError::InvalidRequestHostId { reason }) => {
                assert_eq!(
                    reason,
                    "x-amz-sdk-checksum-algorithm specified, but no corresponding x-amz-checksum-* or x-amz-trailer headers were found."
                );
            }
            Err(e) => panic!("expected InvalidRequestHostId, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn prepare_streaming_put_with_object_lock_requires_checksum() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::PUT,
            "",
            "",
            vec![
                (
                    "x-amz-object-lock-mode".to_string(),
                    "GOVERNANCE".to_string(),
                ),
                (
                    "x-amz-object-lock-retain-until-date".to_string(),
                    "2099-01-01T00:00:00Z".to_string(),
                ),
            ],
            b"hello world".to_vec(),
        );
        match fe.prepare_streaming_put(&req, "mybucket", "mykey", false) {
            Err(ServerError::InvalidRequest { reason }) => {
                assert_eq!(
                    reason,
                    "Content-MD5 OR x-amz-checksum- HTTP header is required for Put Object requests with Object Lock parameters"
                );
            }
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn prepare_streaming_put_sigv4_header_auth_requires_content_sha256() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::PUT,
            "",
            "",
            vec![
                (
                    "authorization".to_string(),
                    "AWS4-HMAC-SHA256 Credential=test/20260318/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=deadbeef".to_string(),
                ),
                ("x-amz-date".to_string(), "20260318T000000Z".to_string()),
            ],
            b"hello world".to_vec(),
        );
        match fe.prepare_streaming_put(&req, "mybucket", "mykey", false) {
            Err(ServerError::InvalidRequest { reason }) => {
                assert_eq!(
                    reason,
                    "Missing required header for this request: x-amz-content-sha256"
                );
            }
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn prepare_streaming_put_without_content_length_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::PUT,
            "",
            "",
            vec![(
                "x-amz-content-sha256".to_string(),
                sha256_hex(b"").to_string(),
            )],
            Vec::new(),
        );
        match fe.prepare_streaming_put(&req, "mybucket", "mykey", false) {
            Err(ServerError::MissingContentLength) => {}
            Err(e) => panic!("expected MissingContentLength, got {e:?}"),
            Ok(_) => panic!("expected MissingContentLength, got Ok"),
        }
    }

    #[test]
    fn prepare_streaming_put_denies_anonymous_write_to_private_bucket() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::PUT,
            "",
            "",
            vec![("content-length".to_string(), "11".to_string())],
            b"hello world".to_vec(),
        );
        match fe.prepare_streaming_put(&req, "mybucket", "mykey", false) {
            Err(ServerError::AccessDenied) => {}
            Err(err) => panic!("expected AccessDenied, got {err:?}"),
            Ok(_) => panic!("expected AccessDenied, got Ok"),
        }
    }

    #[test]
    fn prepare_streaming_post_object_denied_policy_does_not_create_session() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_sigv4_test_bucket(&fe.coordinator, "mybucket", false);

        let req = new_req(
            http::Method::POST,
            "/mybucket",
            "",
            vec![(
                "host".to_string(),
                "examplebucket.s3.amazonaws.com".to_string(),
            )],
            vec![],
        );
        let fields = signed_post_policy_fields(
            "mybucket",
            "mykey",
            &[r#"{"acl":"private"}"#],
            &[("acl", "public-read")],
        );

        match fe.prepare_streaming_post_object(&req, "mybucket", &fields, Some("upload.txt")) {
            Err(ServerError::PostPolicyAccessDenied { reason }) => {
                assert!(
                    reason.contains("'acl'"),
                    "unexpected denial reason: {reason}"
                );
            }
            Err(err) => panic!("expected PostPolicyAccessDenied, got {err:?}"),
            Ok(_) => panic!("expected PostPolicyAccessDenied, got Ok"),
        }

        assert_eq!(
            storage::test_support::stream_upload_session_count(&fe.test_storage_cluster).unwrap(),
            0,
            "policy-denied POST should not create a stream session"
        );
    }

    #[test]
    fn prepare_streaming_post_object_does_not_set_object_creation_operation_policy_condition() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_sigv4_test_bucket(&fe.coordinator, "mybucket", false);
        fe.coordinator
            .put_bucket_policy(&crate::coordinator::PutBucketPolicyRequest {
                bucket: crate::coordinator::BucketRequest::new(
                    test_bucket_name("mybucket"),
                    crate::coordinator::test_helpers::requester(TEST_SIGV4_ACCESS_KEY),
                    None,
                ),
                config: r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::mybucket/*","Condition":{"Bool":{"s3:ObjectCreationOperation":"true"},"Null":{"s3:if-none-match":"true"}}}]}"#,
                confirm_remove_self_bucket_access: false,
            })
            .unwrap();

        let req = new_req(
            http::Method::POST,
            "/mybucket",
            "",
            vec![(
                "host".to_string(),
                "examplebucket.s3.amazonaws.com".to_string(),
            )],
            vec![],
        );
        let fields = signed_post_policy_fields("mybucket", "mykey", &[], &[]);

        let ctx = fe
            .prepare_streaming_post_object(&req, "mybucket", &fields, Some("upload.txt"))
            .unwrap();
        fe.abort_streaming_post_object(&ctx);

        assert_eq!(
            storage::test_support::stream_upload_session_count(&fe.test_storage_cluster).unwrap(),
            0,
            "aborted POST should not leave a stream session"
        );
    }

    #[test]
    fn prepare_streaming_post_object_passes_if_none_match_to_bucket_policy() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_sigv4_test_bucket(&fe.coordinator, "mybucket", false);
        fe.coordinator
            .put_bucket_policy(&crate::coordinator::PutBucketPolicyRequest {
                bucket: crate::coordinator::BucketRequest::new(
                    test_bucket_name("mybucket"),
                    crate::coordinator::test_helpers::requester(TEST_SIGV4_ACCESS_KEY),
                    None,
                ),
                config: r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::mybucket/*","Condition":{"Null":{"s3:if-none-match":"true"}}}]}"#,
                confirm_remove_self_bucket_access: false,
            })
            .unwrap();

        let missing_header_req = new_req(
            http::Method::POST,
            "/mybucket",
            "",
            vec![(
                "host".to_string(),
                "examplebucket.s3.amazonaws.com".to_string(),
            )],
            vec![],
        );
        let fields = signed_post_policy_fields("mybucket", "mykey", &[], &[]);
        match fe.prepare_streaming_post_object(
            &missing_header_req,
            "mybucket",
            &fields,
            Some("upload.txt"),
        ) {
            Err(ServerError::AccessDenied) => {}
            Err(err) => panic!("expected AccessDenied, got {err:?}"),
            Ok(_) => panic!("expected AccessDenied, got Ok"),
        }

        let header_req = new_req(
            http::Method::POST,
            "/mybucket",
            "",
            vec![
                (
                    "host".to_string(),
                    "examplebucket.s3.amazonaws.com".to_string(),
                ),
                ("if-none-match".to_string(), "*".to_string()),
            ],
            vec![],
        );
        let ctx = fe
            .prepare_streaming_post_object(&header_req, "mybucket", &fields, Some("upload.txt"))
            .unwrap();
        fe.abort_streaming_post_object(&ctx);
    }

    #[test]
    fn prepare_streaming_post_object_wrong_region_returns_post_scope_error_before_policy_denial() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_sigv4_test_bucket(&fe.coordinator, "mybucket", false);

        let req = new_req(
            http::Method::POST,
            "/mybucket",
            "",
            vec![(
                "host".to_string(),
                "examplebucket.s3.amazonaws.com".to_string(),
            )],
            vec![],
        );
        let fields = signed_post_policy_fields_for_region(
            "mybucket",
            "mykey",
            "us-west-2",
            &[r#"{"acl":"private"}"#],
            &[("acl", "public-read")],
        );

        match fe.prepare_streaming_post_object(&req, "mybucket", &fields, Some("upload.txt")) {
            Err(ServerError::Auth(auth::AuthError::InvalidCredentialScopeRegion {
                provided_region,
                expected_region,
                ..
            })) => {
                assert_eq!(provided_region, "us-west-2");
                assert_eq!(expected_region, "us-east-1");
            }
            Err(err) => panic!("expected InvalidCredentialScopeRegion, got {err:?}"),
            Ok(_) => panic!("expected InvalidCredentialScopeRegion, got Ok"),
        }

        assert_eq!(
            storage::test_support::stream_upload_session_count(&fe.test_storage_cluster).unwrap(),
            0,
            "wrong-region POST should not create a stream session"
        );
    }

    #[test]
    fn prepare_streaming_put_with_object_lock_accepts_sdk_checksum_header() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_sigv4_test_bucket(&fe.coordinator, "mybucket", true);

        let body = b"hello world".to_vec();
        let mut headers = vec![
            (
                "x-amz-object-lock-mode".to_string(),
                "GOVERNANCE".to_string(),
            ),
            (
                "x-amz-object-lock-retain-until-date".to_string(),
                "2099-01-01T00:00:00Z".to_string(),
            ),
        ];
        headers.extend(checksum_header_pairs(&body));
        let req = signed_v4_put_req(&body, headers);
        let ctx = fe
            .prepare_streaming_put(&req, "mybucket", "mykey", false)
            .unwrap();
        assert_eq!(ctx.key().as_str(), "mykey");
    }

    #[test]
    fn start_streaming_put_session_uses_prepare_authorization_result() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_sigv4_test_bucket(&fe.coordinator, "mybucket", false);

        let body = b"hello world".to_vec();
        let req = signed_v4_put_req(&body, vec![]);
        let ctx = fe
            .prepare_streaming_put(&req, "mybucket", "mykey", false)
            .unwrap();

        fe.coordinator
            .put_bucket_policy(&crate::coordinator::PutBucketPolicyRequest {
                bucket: crate::coordinator::BucketRequest::new(
                    test_bucket_name("mybucket"),
                    crate::coordinator::test_helpers::requester(TEST_SIGV4_ACCESS_KEY),
                    None,
                ),
                config: r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":{"AWS":"AKID"},"Action":"s3:PutObject","Resource":"arn:aws:s3:::mybucket/*"}]}"#,
                confirm_remove_self_bucket_access: false,
            })
            .unwrap();

        let session_id = fe.start_streaming_put_session(&ctx).unwrap();
        fe.coordinator
            .abort_stream_upload_with_retained_cleanup(&ctx.stream_cleanup, &session_id)
            .unwrap();
    }

    #[test]
    fn put_object_invalid_acl_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::PUT,
            "",
            "",
            vec![(
                "x-amz-acl".to_string(),
                "definitely-not-a-real-acl".to_string(),
            )],
            b"hello world".to_vec(),
        );
        let op = S3Operation::PutObject {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgument { .. }) => {}
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    // ── CompleteMultipartUpload validation ────────────────────────────

    #[test]
    fn complete_multipart_missing_upload_id() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = make_req("");
        let op = S3Operation::CompleteMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequest { .. }) => {}
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn complete_multipart_invalid_upload_id_returns_no_such_upload() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let invalid_upload_id = storage::UploadId::overlong_for_test();
        let req = make_req(&format!("uploadId={invalid_upload_id}"));
        let op = S3Operation::CompleteMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 404);
        let body = String::from_utf8(response_body(resp)).unwrap();
        assert!(
            body.starts_with("<Error><Code>NoSuchUpload</Code>"),
            "{body}"
        );
        assert!(
            body.contains(&format!("<UploadId>{invalid_upload_id}</UploadId>")),
            "{body}"
        );
    }

    #[test]
    #[allow(clippy::format_push_string)]
    fn complete_multipart_multiple_checksum_headers_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let upload_id = create_upload_with_checksum(&fe, "mybucket", "k", Some("CRC32"));
        let xml = "<CompleteMultipartUpload>\
               <Part><PartNumber>1</PartNumber><ETag>\"x\"</ETag></Part>\
             </CompleteMultipartUpload>"
            .to_string();
        let req = new_req(
            http::Method::GET,
            "",
            &format!("uploadId={upload_id}"),
            vec![
                ("x-amz-checksum-crc32".to_string(), "AAAAAA==".to_string()),
                ("x-amz-checksum-sha256".to_string(), "BBBBBB==".to_string()),
            ],
            xml.into_bytes(),
        );
        let op = S3Operation::CompleteMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequest { .. }) => {}
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn complete_multipart_ignores_checksum_algorithm_mismatch() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let upload_id = create_upload_with_checksum(&fe, "mybucket", "k", Some("CRC32"));
        let xml = "<CompleteMultipartUpload>\
               <Part><PartNumber>1</PartNumber><ETag>\"x\"</ETag></Part>\
             </CompleteMultipartUpload>"
            .to_string();
        let req = new_req(
            http::Method::GET,
            "",
            &format!("uploadId={upload_id}"),
            vec![
                // CompleteMultipartUpload uses the concrete checksum header
                // name, not x-amz-checksum-algorithm, to identify the value.
                ("x-amz-checksum-algorithm".to_string(), "SHA256".to_string()),
                ("x-amz-checksum-crc32".to_string(), "AAAAAA==".to_string()),
            ],
            xml.into_bytes(),
        );
        let op = S3Operation::CompleteMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        fe.dispatch_routed(&req, &test_auth(), op).unwrap();
    }

    #[test]
    fn complete_multipart_duplicate_same_checksum_header_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let upload_id = create_upload_with_checksum(&fe, "mybucket", "k", Some("CRC32"));
        let xml = "<CompleteMultipartUpload>\
               <Part><PartNumber>1</PartNumber><ETag>\"x\"</ETag></Part>\
             </CompleteMultipartUpload>"
            .to_string();
        let req = new_req(
            http::Method::GET,
            "",
            &format!("uploadId={upload_id}"),
            vec![
                ("x-amz-checksum-crc32".to_string(), "AAAAAA==".to_string()),
                ("x-amz-checksum-crc32".to_string(), "BBBBBB==".to_string()),
            ],
            xml.into_bytes(),
        );
        let op = S3Operation::CompleteMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::DuplicateChecksumHeader { header, value }) => {
                assert_eq!(header, "x-amz-checksum-crc32");
                assert_eq!(value, "BBBBBB==");
            }
            Err(e) => panic!("expected DuplicateChecksumHeader, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn complete_multipart_ignores_checksum_algorithm_header() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let upload_id = create_upload_with_checksum(&fe, "mybucket", "k", Some("CRC32"));
        let xml = "<CompleteMultipartUpload>\
               <Part><PartNumber>1</PartNumber><ETag>\"x\"</ETag></Part>\
             </CompleteMultipartUpload>"
            .to_string();
        let req = new_req(
            http::Method::GET,
            "",
            &format!("uploadId={upload_id}"),
            vec![
                ("x-amz-checksum-algorithm".to_string(), "CRC32".to_string()),
                ("x-amz-checksum-algorithm".to_string(), "SHA256".to_string()),
                ("x-amz-checksum-crc32".to_string(), "AAAAAA==".to_string()),
            ],
            xml.into_bytes(),
        );
        let op = S3Operation::CompleteMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        fe.dispatch_routed(&req, &test_auth(), op).unwrap();
    }

    #[test]
    fn complete_multipart_checksum_algo_mismatch_upload_rejected() {
        // Upload created with CRC32 but complete sends SHA256 checksum header.
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let upload_id = create_upload_with_checksum(&fe, "mybucket", "k", Some("CRC32"));
        // Upload a part so complete has something to work with.
        let part_data = vec![0u8; 1024];
        let etag = stream_upload_part(
            &fe,
            "mybucket",
            "k",
            &upload_id,
            1,
            &part_data,
            Some(ChecksumAlgorithm::Crc32),
        )
        .etag;

        let xml = format!(
            "<CompleteMultipartUpload>\
               <Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part>\
             </CompleteMultipartUpload>"
        );
        let req = new_req(
            http::Method::GET,
            "",
            &format!("uploadId={upload_id}"),
            vec![(
                "x-amz-checksum-sha256".to_string(),
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string(),
            )],
            xml.into_bytes(),
        );
        let op = S3Operation::CompleteMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::CompleteMultipartMissingPartChecksum {
                algorithm,
                part_number,
            }) => {
                assert_eq!(algorithm, "crc32");
                assert_eq!(part_number, 1);
            }
            Err(e) => panic!("expected CompleteMultipartMissingPartChecksum, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn complete_multipart_invalid_mp_object_size_header_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let upload_id = create_upload_with_checksum(&fe, "mybucket", "k", None);
        let xml = "<CompleteMultipartUpload>\
               <Part><PartNumber>1</PartNumber><ETag>\"x\"</ETag></Part>\
             </CompleteMultipartUpload>"
            .to_string();
        let req = new_req(
            http::Method::GET,
            "",
            &format!("uploadId={upload_id}"),
            vec![(
                "x-amz-mp-object-size".to_string(),
                "not-a-number".to_string(),
            )],
            xml.into_bytes(),
        );
        let op = S3Operation::CompleteMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::CompleteMultipartExpectedSizeHeaderInvalid { value }) => {
                assert_eq!(value, "not-a-number");
            }
            Err(e) => panic!("expected CompleteMultipartExpectedSizeHeaderInvalid, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    // ── AbortMultipartUpload validation ──────────────────────────────

    #[test]
    fn abort_multipart_missing_upload_id() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = make_req("");
        let op = S3Operation::AbortMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequest { .. }) => {}
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn abort_multipart_invalid_upload_id_returns_no_such_upload() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let invalid_upload_id = storage::UploadId::overlong_for_test();
        let req = make_req(&format!("uploadId={invalid_upload_id}"));
        let op = S3Operation::AbortMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::NoSuchUpload { upload_id }) => {
                assert_eq!(upload_id, invalid_upload_id);
            }
            Err(e) => panic!("expected NoSuchUpload, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    // ── ListParts validation ─────────────────────────────────────────

    #[test]
    fn list_parts_missing_upload_id() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = make_req("");
        let op = S3Operation::ListParts {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequest { .. }) => {}
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn list_parts_invalid_part_number_marker() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");
        let upload_id = create_upload_with_checksum(&fe, "mybucket", "mykey", None);

        let req = make_req(&format!("uploadId={upload_id}&part-number-marker=xyz"));
        let op = S3Operation::ListParts {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgumentValue {
                reason,
                argument_name,
                argument_value,
            }) => {
                assert_eq!(
                    reason,
                    "Provided part-number-marker not an integer or within integer range"
                );
                assert_eq!(argument_name, "part-number-marker");
                assert_eq!(argument_value, "xyz");
            }
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn list_parts_invalid_max_parts() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");
        let upload_id = create_upload_with_checksum(&fe, "mybucket", "mykey", None);

        let req = make_req(&format!("uploadId={upload_id}&max-parts=notanumber"));
        let op = S3Operation::ListParts {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgumentValue {
                reason,
                argument_name,
                argument_value,
            }) => {
                assert_eq!(
                    reason,
                    "Provided max-parts not an integer or within integer range"
                );
                assert_eq!(argument_name, "max-parts");
                assert_eq!(argument_value, "notanumber");
            }
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn list_parts_clamps_max_parts_to_s3_limit() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");
        let upload_id = create_upload_with_checksum(&fe, "mybucket", "mykey", None);

        let req = make_req(&format!("uploadId={upload_id}&max-parts=2147483647"));
        let op = S3Operation::ListParts {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);
        let body = String::from_utf8(response_body(resp)).unwrap();
        assert!(
            body.contains("<MaxParts>1000</MaxParts>"),
            "unexpected ListParts body: {body}"
        );
        assert!(
            !body.contains("<MaxParts>2147483647</MaxParts>"),
            "unexpected ListParts body: {body}"
        );
    }

    #[test]
    fn list_parts_invalid_upload_id_returns_no_such_upload() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = make_req("uploadId=abc");
        let op = S3Operation::ListParts {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::NoSuchUpload { upload_id }) => assert_eq!(upload_id, "abc"),
            Err(e) => panic!("expected NoSuchUpload, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    // ── GetObjectAttributes header validation ──────────────────────

    #[test]
    fn get_object_attributes_invalid_max_parts() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::GET,
            "",
            "",
            vec![
                (
                    "x-amz-object-attributes".to_string(),
                    "ObjectParts".to_string(),
                ),
                ("x-amz-max-parts".to_string(), "notanumber".to_string()),
            ],
            vec![],
        );
        let op = S3Operation::GetObjectAttributes {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgument { .. }) => {}
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn get_object_attributes_invalid_part_number_marker() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::GET,
            "",
            "",
            vec![
                (
                    "x-amz-object-attributes".to_string(),
                    "ObjectParts".to_string(),
                ),
                ("x-amz-part-number-marker".to_string(), "xyz".to_string()),
            ],
            vec![],
        );
        let op = S3Operation::GetObjectAttributes {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgument { .. }) => {}
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn get_object_attributes_preserves_max_parts_above_s3_list_limit() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");
        let parts = [(1, vec![b'a'; 5 * 1024 * 1024]), (2, b"tail".to_vec())];
        do_multipart_upload(&fe, "mybucket", "mykey", &parts, Some("CRC32"));

        let req = new_req(
            http::Method::GET,
            "",
            "",
            vec![
                (
                    "x-amz-object-attributes".to_string(),
                    "ObjectParts".to_string(),
                ),
                ("x-amz-max-parts".to_string(), "4294967295".to_string()),
            ],
            vec![],
        );
        let op = S3Operation::GetObjectAttributes {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);
        let body = String::from_utf8(response_body(resp)).unwrap();
        assert!(
            body.contains("<MaxParts>4294967295</MaxParts>"),
            "unexpected GetObjectAttributes body: {body}"
        );
    }

    // ── End-to-end multipart upload flow ────────────────────────────

    #[test]
    fn multipart_upload_e2e_quoted_etags() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        // 1. CreateMultipartUpload
        let req = make_req("uploads");
        let op = S3Operation::CreateMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);
        let body_bytes = response_body(resp);
        let body = std::str::from_utf8(&body_bytes).unwrap();
        // Extract upload_id from <UploadId>...</UploadId>
        let uid_start = body.find("<UploadId>").unwrap() + "<UploadId>".len();
        let uid_end = uid_start + body[uid_start..].find("</UploadId>").unwrap();
        let upload_id = &body[uid_start..uid_end];
        assert!(!upload_id.is_empty());

        // 2. UploadPart — single part (last part is exempt from min-size)
        let part_body = vec![0u8; 1024];
        let etag =
            stream_upload_part(&fe, "mybucket", "mykey", upload_id, 1, &part_body, None).etag;
        // ETag must be quoted
        assert!(
            etag.starts_with('"') && etag.ends_with('"'),
            "ETag not quoted: {etag}"
        );

        // 3. CompleteMultipartUpload with quoted ETag from UploadPart response
        let complete_xml = format!(
            "<CompleteMultipartUpload>\
               <Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part>\
             </CompleteMultipartUpload>"
        );
        let req = new_req(
            http::Method::GET,
            "",
            &format!("uploadId={upload_id}"),
            vec![],
            complete_xml.into_bytes(),
        );
        let op = S3Operation::CompleteMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);
        let body_bytes = response_body(resp);
        let body = std::str::from_utf8(&body_bytes).unwrap();
        assert!(
            body.contains("<CompleteMultipartUploadResult"),
            "missing result element: {body}"
        );
        assert!(body.contains("<Key>mykey</Key>"), "missing key: {body}");
        assert!(body.contains("<ETag>"), "missing etag: {body}");
    }

    // ── ListMultipartUploads validation ──────────────────────────────

    #[test]
    fn list_multipart_uploads_invalid_max_uploads() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = make_req("uploads&max-uploads=abc");
        let op = S3Operation::ListMultipartUploads {
            bucket: test_bucket_name("mybucket"),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgumentValue {
                reason,
                argument_name,
                argument_value,
            }) => {
                assert_eq!(
                    reason,
                    "Provided max-uploads not an integer or within integer range"
                );
                assert_eq!(argument_name, "max-uploads");
                assert_eq!(argument_value, "abc");
            }
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn list_multipart_uploads_clamps_max_uploads_to_s3_limit() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = make_req("uploads&max-uploads=2147483647");
        let op = S3Operation::ListMultipartUploads {
            bucket: test_bucket_name("mybucket"),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);
        let body = String::from_utf8(response_body(resp)).unwrap();
        assert!(
            body.contains("<MaxUploads>1000</MaxUploads>"),
            "unexpected ListMultipartUploads body: {body}"
        );
        assert!(
            !body.contains("<MaxUploads>2147483647</MaxUploads>"),
            "unexpected ListMultipartUploads body: {body}"
        );
    }

    #[test]
    fn list_multipart_uploads_invalid_upload_id_marker() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = make_req("uploads&key-marker=mykey&upload-id-marker=bad");
        let op = S3Operation::ListMultipartUploads {
            bucket: test_bucket_name("mybucket"),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgumentValue {
                reason,
                argument_name,
                argument_value,
            }) => {
                assert_eq!(reason, "Invalid uploadId marker");
                assert_eq!(argument_name, "upload-id-marker");
                assert_eq!(argument_value, "bad");
            }
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn list_object_versions_invalid_max_keys() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = make_req("versions&max-keys=abc");
        let op = S3Operation::ListObjectVersions {
            bucket: test_bucket_name("mybucket"),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgument { .. }) => {}
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn list_object_versions_echoes_oversized_max_keys() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = make_req("versions&max-keys=5000");
        let op = S3Operation::ListObjectVersions {
            bucket: test_bucket_name("mybucket"),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);

        let body = String::from_utf8(response_body(resp)).unwrap();
        assert!(
            body.contains("<MaxKeys>5000</MaxKeys>"),
            "unexpected body: {body}"
        );
    }

    #[test]
    fn list_object_versions_invalid_version_id_marker() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = make_req("versions&version-id-marker=abc");
        let op = S3Operation::ListObjectVersions {
            bucket: test_bucket_name("mybucket"),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidVersionId {
                argument_name,
                argument_value,
            }) => {
                assert_eq!(argument_name, "version-id-marker");
                assert_eq!(argument_value, "abc");
            }
            Err(e) => panic!("expected InvalidVersionId, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn list_object_versions_rejects_version_id_marker_without_key_marker() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = make_req("versions&version-id-marker=1");
        let op = S3Operation::ListObjectVersions {
            bucket: test_bucket_name("mybucket"),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgument { reason }) => {
                assert_eq!(
                    reason,
                    "A version-id marker cannot be specified without a key marker."
                );
            }
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn anonymous_get_rejects_response_override_params() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());

        let req = new_req(
            http::Method::GET,
            "/mybucket/key",
            "response-content-type=text%2Fplain",
            vec![],
            vec![],
        );
        let op = S3Operation::GetObject {
            bucket: test_bucket_name("mybucket"),
            key: "key".to_string(),
        };
        match fe.dispatch_routed(&req, &auth::AuthContext::anonymous(), op) {
            Err(ServerError::InvalidRequest { reason }) => {
                assert_eq!(
                    reason,
                    "Request specific response headers cannot be used for anonymous GET requests."
                );
            }
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    // ── CreateMultipartUpload checksum validation ───────────────────

    #[test]
    fn create_multipart_rejects_acl_on_bucket_owner_enforced_bucket() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");
        fe.coordinator
            .put_bucket_ownership_controls(&crate::coordinator::PutBucketOwnershipControlsRequest {
                bucket: test_bucket_request("mybucket"),
                config: storage::BucketOwnershipControls {
                    object_ownership: storage::BucketObjectOwnership::BucketOwnerEnforced,
                },
            })
            .unwrap();

        let req = new_req(
            http::Method::GET,
            "",
            "uploads",
            vec![("x-amz-acl".to_string(), "public-read".to_string())],
            vec![],
        );
        let op = S3Operation::CreateMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::AccessControlListNotSupported) => {}
            Err(e) => panic!("expected AccessControlListNotSupported, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn create_multipart_invalid_checksum_algorithm() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::GET,
            "",
            "uploads",
            vec![("x-amz-checksum-algorithm".to_string(), "BOGUS".to_string())],
            vec![],
        );
        let op = S3Operation::CreateMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequestHostId { reason }) => {
                assert_eq!(reason, ServerError::UNSUPPORTED_CHECKSUM_ALGORITHM_MESSAGE);
            }
            Err(e) => panic!("expected InvalidRequestHostId, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn create_multipart_invalid_checksum_type() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::GET,
            "",
            "uploads",
            vec![
                ("x-amz-checksum-algorithm".to_string(), "CRC32".to_string()),
                ("x-amz-checksum-type".to_string(), "INVALID".to_string()),
            ],
            vec![],
        );
        let op = S3Operation::CreateMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequestHostId { reason }) => {
                assert_eq!(reason, "Value for x-amz-checksum-type header is invalid.");
            }
            Err(e) => panic!("expected InvalidRequestHostId, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn create_multipart_checksum_type_without_algorithm() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::GET,
            "",
            "uploads",
            vec![("x-amz-checksum-type".to_string(), "COMPOSITE".to_string())],
            vec![],
        );
        let op = S3Operation::CreateMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequestHostId { reason }) => {
                assert_eq!(
                    reason,
                    "The x-amz-checksum-type header can only be used with the x-amz-checksum-algorithm header."
                );
            }
            Err(e) => panic!("expected InvalidRequestHostId, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn create_multipart_sha_full_object_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::GET,
            "",
            "uploads",
            vec![
                ("x-amz-checksum-algorithm".to_string(), "SHA256".to_string()),
                ("x-amz-checksum-type".to_string(), "FULL_OBJECT".to_string()),
            ],
            vec![],
        );
        let op = S3Operation::CreateMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequestHostId { reason }) => {
                assert_eq!(
                    reason,
                    "The FULL_OBJECT checksum type cannot be used with the sha256 checksum algorithm."
                );
            }
            Err(e) => panic!("expected InvalidRequestHostId, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn create_multipart_with_checksum_reports_fields_in_headers_only() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::GET,
            "",
            "uploads",
            vec![
                ("x-amz-checksum-algorithm".to_string(), "CRC32".to_string()),
                ("x-amz-checksum-type".to_string(), "FULL_OBJECT".to_string()),
            ],
            vec![],
        );
        let op = S3Operation::CreateMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);
        assert_eq!(
            find_header(&resp, "x-amz-checksum-algorithm"),
            Some("CRC32")
        );
        assert_eq!(
            find_header(&resp, "x-amz-checksum-type"),
            Some("FULL_OBJECT")
        );
        let body_bytes = response_body(resp);
        let body = std::str::from_utf8(&body_bytes).unwrap();
        // AWS reports checksum configuration only in headers; the
        // InitiateMultipartUploadResult body carries just Bucket/Key/UploadId.
        assert!(
            !body.contains("ChecksumAlgorithm"),
            "unexpected ChecksumAlgorithm in body: {body}"
        );
        assert!(
            !body.contains("ChecksumType"),
            "unexpected ChecksumType in body: {body}"
        );
    }

    #[test]
    fn create_multipart_crc32_composite_accepted() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::GET,
            "",
            "uploads",
            vec![
                ("x-amz-checksum-algorithm".to_string(), "CRC32".to_string()),
                ("x-amz-checksum-type".to_string(), "COMPOSITE".to_string()),
            ],
            vec![],
        );
        let op = S3Operation::CreateMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);
        assert_eq!(
            find_header(&resp, "x-amz-checksum-algorithm"),
            Some("CRC32")
        );
        assert_eq!(find_header(&resp, "x-amz-checksum-type"), Some("COMPOSITE"));
    }

    #[test]
    fn create_multipart_algorithm_only_defaults_type() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::GET,
            "",
            "uploads",
            vec![("x-amz-checksum-algorithm".to_string(), "SHA256".to_string())],
            vec![],
        );
        let op = S3Operation::CreateMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);
        assert_eq!(
            find_header(&resp, "x-amz-checksum-algorithm"),
            Some("SHA256")
        );
        assert_eq!(find_header(&resp, "x-amz-checksum-type"), Some("COMPOSITE"));
    }

    // ── UploadPart checksum validation ──────────────────────────────

    /// Helper: create a multipart upload with optional checksum algorithm, return `upload_id`.
    fn create_upload_with_checksum(
        fe: &HttpFrontend,
        bucket: &str,
        key: &str,
        algo: Option<&str>,
    ) -> String {
        let mut headers = Vec::new();
        if let Some(a) = algo {
            headers.push(("x-amz-checksum-algorithm".to_string(), a.to_string()));
        }
        let req = new_req(http::Method::GET, "", "uploads", headers, vec![]);
        let op = S3Operation::CreateMultipartUpload {
            bucket: test_bucket_name(bucket),
            key: key.to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        let body_bytes = response_body(resp);
        let body = std::str::from_utf8(&body_bytes).unwrap();
        let start = body.find("<UploadId>").unwrap() + "<UploadId>".len();
        let end = start + body[start..].find("</UploadId>").unwrap();
        body[start..end].to_string()
    }

    fn compute_checksum_for_test(algo: ChecksumAlgorithm, data: &[u8]) -> RawChecksum {
        checksum::compute_checksum(algo, data)
    }

    #[test]
    fn validate_checksum_headers_accepts_all_algorithms() {
        use base64::Engine;

        let body = b"new checksum algorithms";
        for algorithm in ChecksumAlgorithm::ALL {
            let checksum = checksum::compute_checksum(algorithm, body);
            let encoded = base64::engine::general_purpose::STANDARD.encode(checksum.bytes());
            let req = new_req(
                http::Method::PUT,
                "",
                "",
                vec![(algorithm.header_name().to_string(), encoded)],
                body.to_vec(),
            );
            validate_checksum_headers(&req, true).unwrap();
        }
    }

    #[test]
    fn validate_checksum_headers_rejects_duplicate_same_header() {
        use base64::Engine;

        let body = b"duplicate checksum header";
        let algorithm = ChecksumAlgorithm::Crc32;
        let checksum = checksum::compute_checksum(algorithm, body);
        let encoded = base64::engine::general_purpose::STANDARD.encode(checksum.bytes());
        let req = new_req(
            http::Method::PUT,
            "",
            "",
            vec![
                (algorithm.header_name().to_string(), encoded.clone()),
                (algorithm.header_name().to_string(), "AAAAAA==".to_string()),
            ],
            body.to_vec(),
        );

        match validate_checksum_headers(&req, true) {
            Err(ServerError::DuplicateChecksumHeader { header, value }) => {
                assert_eq!(header, "x-amz-checksum-crc32");
                assert_eq!(value, "AAAAAA==");
            }
            other => panic!("expected duplicate header rejection, got {other:?}"),
        }
    }

    #[test]
    fn post_checksum_claim_from_fields_accepts_sha512() {
        use base64::Engine;

        let checksum = checksum::compute_checksum(ChecksumAlgorithm::Sha512, b"post-body");
        let encoded = base64::engine::general_purpose::STANDARD.encode(checksum.bytes());
        let fields = vec![
            ("key".to_string(), "mykey".to_string()),
            ("x-amz-checksum-algorithm".to_string(), "SHA512".to_string()),
            ("x-amz-checksum-sha512".to_string(), encoded),
        ];
        let claim = post_checksum_claim_from_fields(&fields)
            .unwrap()
            .expect("checksum field should be parsed");
        assert_eq!(claim.algorithm(), ChecksumAlgorithm::Sha512);
        assert_eq!(claim.expected_bytes(), checksum.bytes());
    }

    #[test]
    fn post_checksum_claim_from_fields_rejects_algorithm_mismatch() {
        let fields = vec![
            ("key".to_string(), "mykey".to_string()),
            ("x-amz-checksum-algorithm".to_string(), "SHA512".to_string()),
            (
                "x-amz-checksum-md5".to_string(),
                "AAAAAAAAAAAAAAAAAAAAAA==".to_string(),
            ),
        ];
        assert!(post_checksum_claim_from_fields(&fields).is_err());
    }

    fn stream_upload_part(
        fe: &HttpFrontend,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u32,
        data: &[u8],
        checksum_algorithm: Option<ChecksumAlgorithm>,
    ) -> crate::coordinator::UploadPartResult {
        let auth = test_auth();
        let supported = auth::ConfiguredOrAnonymousAuth::try_from(&auth).unwrap();
        let requester = crate::coordinator::Requester::from_auth(supported)
            .with_request_epoch_seconds(Some(storage::clock::current_time_millis() / 1_000));
        let bucket_name = test_bucket_name(bucket);
        let object_key = parse_object_key(key).unwrap();
        let upload_id = parse_present_upload_id(upload_id).unwrap();
        let admission = fe.coordinator.admit_storage_route_for_request().unwrap();
        let cleanup = fe
            .coordinator
            .retained_stream_upload_cleanup(&admission, &bucket_name, &object_key)
            .unwrap();
        let session = fe
            .coordinator
            .begin_stream_part_on_admitted_route(
                &admission,
                &BeginStreamPartRequest {
                    upload: multipart_object_request(
                        &bucket_name,
                        key,
                        upload_id.clone(),
                        requester.clone(),
                        None,
                    )
                    .unwrap(),
                    part_number,
                    policy_context: crate::coordinator::PutObjectPolicyContext::default(),
                    sse_customer: None,
                },
            )
            .unwrap();
        let result = (|| {
            use base64::Engine;

            for (segment_index, chunk) in data
                .chunks(crate::coordinator::INTERNAL_SEGMENT_SIZE)
                .enumerate()
            {
                fe.coordinator.append_stream_part_data_on_admitted_route(
                    &admission,
                    &crate::coordinator::AppendStreamPartRequest {
                        bucket: bucket_name.clone(),
                        key: object_key.clone(),
                        upload_id: &upload_id,
                        session_id: &session.session_id,
                        part_number,
                        segment_index: segment_index as u32,
                        data: chunk,
                        sse_customer: None,
                    },
                )?;
            }
            let computed_checksum =
                checksum_algorithm.map(|algo| compute_checksum_for_test(algo, data));
            let claimed_checksum = computed_checksum.as_ref().map(|expected| {
                let encoded = base64::engine::general_purpose::STANDARD.encode(expected.bytes());
                ChecksumClaim::from_base64(expected.algorithm(), &encoded)
                    .expect("checksum helper must round-trip through base64")
            });
            fe.coordinator.finalize_stream_part_with_storage_admission(
                &admission,
                FinalizeStreamPartRequest {
                    upload: multipart_object_request(
                        &bucket_name,
                        key,
                        upload_id.clone(),
                        requester,
                        None,
                    )
                    .unwrap(),
                    session_id: &session.session_id,
                    part_number,
                    crc64: checksum::crc64::checksum(data),
                    total_size: data.len() as u64,
                    claimed_checksum: claimed_checksum.as_ref(),
                    computed_checksum,
                },
            )
        })();
        if result.is_err() {
            let _ = fe
                .coordinator
                .abort_stream_upload_with_retained_cleanup(&cleanup, &session.session_id);
        }
        result.unwrap()
    }

    // ── GET ?partNumber=N tests ─────────────────────────────────────

    /// Helper: do a full multipart upload through the live streaming coordinator path.
    #[allow(clippy::format_push_string)]
    fn do_multipart_upload(
        fe: &HttpFrontend,
        bucket: &str,
        key: &str,
        parts: &[(u32, Vec<u8>)],
        algo: Option<&str>,
    ) {
        use base64::Engine;
        let b64 = base64::engine::general_purpose::STANDARD;
        let upload_id = create_upload_with_checksum(fe, bucket, key, algo);
        let checksum_algorithm = algo.map(|name| ChecksumAlgorithm::parse(name).unwrap());
        let mut part_info: Vec<(u32, String, Option<String>)> = Vec::new();
        for (part_number, data) in parts {
            let result = stream_upload_part(
                fe,
                bucket,
                key,
                &upload_id,
                *part_number,
                data,
                checksum_algorithm,
            );
            let checksum_b64 = result
                .checksum
                .as_ref()
                .map(|checksum| b64.encode(checksum.bytes()));
            part_info.push((*part_number, result.etag, checksum_b64));
        }
        let mut xml_parts = String::new();
        for (pn, etag, cksum) in &part_info {
            xml_parts.push_str(&format!(
                "<Part><PartNumber>{pn}</PartNumber><ETag>{etag}</ETag>"
            ));
            if let (Some(a), Some(val)) = (algo, cksum) {
                let algo_enum = ChecksumAlgorithm::parse(a).unwrap();
                let elem = algo_enum.xml_element_name();
                xml_parts.push_str(&format!("<{elem}>{val}</{elem}>"));
            }
            xml_parts.push_str("</Part>");
        }
        let xml = format!("<CompleteMultipartUpload>{xml_parts}</CompleteMultipartUpload>");
        let req = new_req(
            http::Method::GET,
            "",
            &format!("uploadId={upload_id}"),
            vec![],
            xml.into_bytes(),
        );
        let op = S3Operation::CompleteMultipartUpload {
            bucket: test_bucket_name(bucket),
            key: key.to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);
    }

    #[test]
    fn get_object_part_multipart() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        // 5 MiB minimum for non-final parts
        let part1 = vec![0xAA; 5 * 1024 * 1024];
        let part2 = vec![0xBB; 5 * 1024 * 1024];
        let part3 = vec![0xCC; 100];
        do_multipart_upload(
            &fe,
            "mybucket",
            "k",
            &[(1, part1.clone()), (2, part2.clone()), (3, part3.clone())],
            None,
        );

        // GET partNumber=2
        let req = make_req("partNumber=2");
        let op = S3Operation::GetObject {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 206);

        // Verify Content-Range
        let content_range = resp
            .headers
            .iter()
            .find(|(k, _)| k == "Content-Range")
            .map(|(_, v)| v.as_str())
            .unwrap();
        let total = 5 * 1024 * 1024 + 5 * 1024 * 1024 + 100;
        let start = 5 * 1024 * 1024;
        let end = 2 * 5 * 1024 * 1024 - 1;
        assert_eq!(content_range, format!("bytes {start}-{end}/{total}"));

        // Verify x-amz-mp-parts-count
        let parts_count = resp
            .headers
            .iter()
            .find(|(k, _)| k == "x-amz-mp-parts-count")
            .map(|(_, v)| v.as_str())
            .unwrap();
        assert_eq!(parts_count, "3");

        let content_length_count = resp
            .headers
            .iter()
            .filter(|(k, _)| k.eq_ignore_ascii_case("Content-Length"))
            .count();
        assert_eq!(content_length_count, 1);

        // Verify data
        assert_eq!(resp.into_test_body_bytes().unwrap(), part2);
    }

    #[test]
    fn get_object_part_invalid_zero() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        // PUT a simple object
        let req = new_req(http::Method::GET, "", "", vec![], b"hello".to_vec());
        let op = S3Operation::PutObject {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        fe.dispatch_routed(&req, &test_auth(), op).unwrap();

        // partNumber=0 → InvalidArgument
        let req = make_req("partNumber=0");
        let op = S3Operation::GetObject {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgument { .. }) => {}
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn get_object_part_out_of_range() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let part1 = vec![0xAA; 5 * 1024 * 1024];
        let part2 = vec![0xBB; 5 * 1024 * 1024];
        let part3 = vec![0xCC; 100];
        do_multipart_upload(
            &fe,
            "mybucket",
            "k",
            &[(1, part1), (2, part2), (3, part3)],
            None,
        );

        // partNumber=99 on a 3-part object → 416 InvalidPartNumber
        let req = make_req("partNumber=99");
        let op = S3Operation::GetObject {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidPartNumber {
                part_number: 99,
                parts_count: 3,
            }) => {}
            Err(e) => panic!("expected InvalidPartNumber, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn get_object_part_and_range_rejected_together() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let part1 = vec![0xAA; 5 * 1024 * 1024];
        let part2 = vec![0xBB; 100];
        do_multipart_upload(&fe, "mybucket", "k", &[(1, part1), (2, part2)], None);

        let req = new_req(
            http::Method::GET,
            "",
            "partNumber=2",
            vec![("Range".to_string(), "bytes=0-1".to_string())],
            vec![],
        );
        let op = S3Operation::GetObject {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequest { reason }) => {
                assert_eq!(
                    reason,
                    "Cannot specify both Range header and partNumber query parameter"
                );
            }
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn get_object_part_non_multipart() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let data = b"hello world";
        let req = new_req(http::Method::GET, "", "", vec![], data.to_vec());
        let op = S3Operation::PutObject {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        fe.dispatch_routed(&req, &test_auth(), op).unwrap();

        // partNumber=1 on inline object → 206 with full data
        let req = make_req("partNumber=1");
        let op = S3Operation::GetObject {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 206);

        assert!(
            resp.headers
                .iter()
                .all(|(name, _)| name != "x-amz-mp-parts-count"),
            "AWS omits the multipart parts count for a non-multipart object"
        );

        let content_range = resp
            .headers
            .iter()
            .find(|(k, _)| k == "Content-Range")
            .map(|(_, v)| v.as_str())
            .unwrap();
        assert_eq!(
            content_range,
            format!("bytes 0-{}/{}", data.len() - 1, data.len())
        );

        assert_eq!(resp.into_test_body_bytes().unwrap(), data);
    }

    #[test]
    fn get_object_part_non_multipart_out_of_range() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(http::Method::GET, "", "", vec![], b"hello".to_vec());
        let op = S3Operation::PutObject {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        fe.dispatch_routed(&req, &test_auth(), op).unwrap();

        // partNumber=2 on non-multipart → 416 InvalidPartNumber
        let req = make_req("partNumber=2");
        let op = S3Operation::GetObject {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidPartNumber {
                part_number: 2,
                parts_count: 1,
            }) => {}
            Err(e) => panic!("expected InvalidPartNumber, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn get_object_part_with_checksum() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let part1 = vec![0xAA; 5 * 1024 * 1024];
        let part2 = vec![0xBB; 100];
        do_multipart_upload(
            &fe,
            "mybucket",
            "k",
            &[(1, part1), (2, part2)],
            Some("CRC32"),
        );

        // GET partNumber=1 — checksum always emitted for part GETs
        let req = make_req("partNumber=1");
        let op = S3Operation::GetObject {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 206);

        // Should have per-part checksum header (always, not just with ENABLED)
        let has_checksum = resp
            .headers
            .iter()
            .any(|(k, _)| k == "x-amz-checksum-crc32");
        assert!(has_checksum, "expected x-amz-checksum-crc32 header");

        // Should also have checksum-type header
        let has_type = resp.headers.iter().any(|(k, _)| k == "x-amz-checksum-type");
        assert!(has_type, "expected x-amz-checksum-type header");
    }

    // ── aws-chunked decode edge cases ──────────────────────────────────

    #[test]
    fn unsupported_authenticated_identity_fails_closed_across_core_request_adapters() {
        let tmp = test_util::tempdir();
        let mut frontend = setup_frontend(tmp.path());
        create_sigv4_test_bucket(&frontend.coordinator, "mybucket", false);
        let session = install_unsupported_identity_credential(&mut frontend);
        let request = signed_v4_put_req_with_credentials(
            &[],
            Vec::new(),
            &session.access_key_id,
            &session.secret_key,
            Some(&session.token),
        );

        let auth = frontend
            .authenticate_with_payload_check(&request, false, Some("mybucket"))
            .expect("temporary credential should still authenticate");
        assert!(auth.identity.as_ref().unwrap().role_session().is_some());
        assert!(matches!(
            auth::ConfiguredOrAnonymousAuth::try_from(&auth),
            Err(auth::UnsupportedAuthorizationIdentity)
        ));

        // Buffered S3.
        assert!(matches!(
            frontend.dispatch_routed(&make_req(""), &auth, S3Operation::ListBuckets),
            Err(ServerError::AccessDenied)
        ));

        // S3 Control.
        let admission = frontend
            .coordinator
            .admit_storage_route_for_request()
            .unwrap();
        assert!(matches!(
            frontend.dispatch_s3_control(
                &make_req(""),
                &auth,
                &admission,
                S3ControlOperation::ListTagsForResource {
                    bucket: test_bucket_name("mybucket"),
                },
            ),
            Err(ServerError::AccessDenied)
        ));

        // POST Object.
        let post_request = new_req(
            http::Method::POST,
            "/mybucket",
            "",
            vec![(
                "host".to_string(),
                "examplebucket.s3.amazonaws.com".to_string(),
            )],
            Vec::new(),
        );
        let post_fields = signed_post_fields_for_unsupported_identity(&session);
        assert!(matches!(
            frontend.prepare_streaming_post_object(
                &post_request,
                "mybucket",
                &post_fields,
                Some("upload.txt"),
            ),
            Err(ServerError::AccessDenied)
        ));

        // Streaming PutObject and UploadPart.
        assert!(matches!(
            frontend.prepare_streaming_put(&request, "mybucket", "mykey", false),
            Err(ServerError::AccessDenied)
        ));
        assert!(matches!(
            frontend.prepare_streaming_part(&request, "mybucket", "mykey", "upload-id", "1",),
            Err(ServerError::AccessDenied)
        ));

        let anonymous = AuthContext::anonymous();
        assert!(auth::ConfiguredOrAnonymousAuth::try_from(&anonymous).is_ok());
        let configured = test_auth();
        assert!(auth::ConfiguredOrAnonymousAuth::try_from(&configured).is_ok());
    }

    #[test]
    fn signed_streaming_modes_without_context_are_internal_invariant_failures() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());

        // Auth context claims signed streaming but has no streaming context.
        let auth = auth::AuthContext {
            mode: auth::AuthMode::HeaderSigV4,
            access_key_id: Some("AKID".to_string()),
            identity: Some(configured_identity(auth::AccountIdentity::from_principal(
                "testuser",
            ))),
            authorization_profile: auth::AuthorizationProfile::Standard,
            request_epoch_secs: Some(0),
            signing_region: Some("us-east-1".to_string()),
            streaming: None, // missing!
        };

        for content_sha256 in [
            "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
            "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER",
        ] {
            let req = new_req(
                http::Method::PUT,
                "/mybucket/key",
                "",
                vec![
                    (
                        "x-amz-content-sha256".to_string(),
                        content_sha256.to_string(),
                    ),
                    ("content-encoding".to_string(), "aws-chunked".to_string()),
                    ("x-amz-decoded-content-length".to_string(), "5".to_string()),
                ],
                b"5\r\nhello\r\n0\r\n\r\n".to_vec(),
            );

            let Err(ServerError::InternalError { reason }) = fe.maybe_decode_chunked(&req, &auth)
            else {
                panic!("expected an internal signing-context invariant failure");
            };
            assert!(
                reason.contains("missing its authenticated signing context"),
                "unexpected internal error: {reason}"
            );
        }
    }

    #[test]
    fn non_numeric_decoded_content_length_returns_400() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());

        let req = new_req(
            http::Method::PUT,
            "/mybucket/key",
            "",
            vec![
                (
                    "x-amz-content-sha256".to_string(),
                    "STREAMING-UNSIGNED-PAYLOAD-TRAILER".to_string(),
                ),
                ("content-encoding".to_string(), "aws-chunked".to_string()),
                (
                    "x-amz-decoded-content-length".to_string(),
                    "not-a-number".to_string(),
                ),
                (
                    "x-amz-trailer".to_string(),
                    "x-amz-checksum-crc32".to_string(),
                ),
            ],
            b"5\r\nhello\r\n0\r\nx-amz-checksum-crc32:AAAA\r\n\r\n".to_vec(),
        );

        match fe.maybe_decode_chunked(&req, &test_auth()) {
            Err(ServerError::InvalidRequest { .. }) => {} // expected
            other => panic!("expected InvalidRequest, got {:?}", other.err()),
        }
    }

    #[test]
    fn unsigned_streaming_with_non_aws_content_encoding_decodes_and_preserves_header() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());

        let req = new_req(
            http::Method::PUT,
            "/mybucket/key",
            "",
            vec![
                (
                    "x-amz-content-sha256".to_string(),
                    "STREAMING-UNSIGNED-PAYLOAD-TRAILER".to_string(),
                ),
                ("content-encoding".to_string(), "gzip".to_string()),
                ("x-amz-decoded-content-length".to_string(), "5".to_string()),
                (
                    "x-amz-trailer".to_string(),
                    "x-amz-checksum-crc32".to_string(),
                ),
            ],
            b"5\r\nhello\r\n0\r\nx-amz-checksum-crc32:AAAA\r\n\r\n".to_vec(),
        );

        match fe.reject_streaming_fallthrough(&req) {
            Err(ServerError::InvalidRequest { .. }) => {}
            other => panic!("expected streaming-path rejection, got {:?}", other),
        }

        let decoded = fe
            .maybe_decode_chunked(&req, &test_auth())
            .expect("decode should succeed")
            .expect("streaming body should decode");
        assert_eq!(decoded.header("content-encoding"), Some("gzip"));
        assert_eq!(decoded.body, b"hello");
    }

    #[test]
    fn ecdsa_streaming_token_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());

        let req = new_req(
            http::Method::PUT,
            "/mybucket/key",
            "",
            vec![
                (
                    "x-amz-content-sha256".to_string(),
                    "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER".to_string(),
                ),
                ("content-encoding".to_string(), "aws-chunked".to_string()),
                ("x-amz-decoded-content-length".to_string(), "5".to_string()),
                (
                    "x-amz-trailer".to_string(),
                    "x-amz-checksum-crc32".to_string(),
                ),
            ],
            b"5\r\nhello\r\n0\r\nx-amz-checksum-crc32:AAAA\r\n\r\n".to_vec(),
        );

        match fe.maybe_decode_chunked(&req, &test_auth()) {
            Err(ServerError::UnsupportedStreamingToken { .. }) => {} // expected
            other => panic!("expected UnsupportedStreamingToken, got {:?}", other.err()),
        }
    }

    // ── DeleteObjects version-id validation ────────────────────────────

    #[test]
    fn delete_objects_invalid_version_id_returns_per_object_no_such_version() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let xml = br#"<?xml version="1.0"?>
<Delete>
  <Object><Key>key1</Key><VersionId>not-a-number</VersionId></Object>
</Delete>"#;
        let req = new_req(
            http::Method::POST,
            "/mybucket",
            "delete",
            vec![("Content-MD5".to_string(), content_md5_value(xml))],
            xml.to_vec(),
        );
        let op = S3Operation::DeleteObjects {
            bucket: test_bucket_name("mybucket"),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);
        let body = String::from_utf8(response_body(resp)).unwrap();
        assert!(body.contains("<Error><Key>key1</Key><VersionId>not-a-number</VersionId><Code>NoSuchVersion</Code><Message>The specified version does not exist.</Message></Error>"), "unexpected body: {body}");
    }

    #[test]
    fn delete_objects_null_version_id_accepted() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let xml = br#"<?xml version="1.0"?>
<Delete>
  <Object><Key>key1</Key><VersionId>null</VersionId></Object>
</Delete>"#;
        let req = new_req(
            http::Method::POST,
            "/mybucket",
            "delete",
            vec![("Content-MD5".to_string(), content_md5_value(xml))],
            xml.to_vec(),
        );
        let op = S3Operation::DeleteObjects {
            bucket: test_bucket_name("mybucket"),
        };
        // "null" is a valid version ID — should not error on parsing
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Ok(_) => {}
            Err(e) => panic!("expected Ok, got {e:?}"),
        }
    }

    #[test]
    fn delete_objects_missing_content_md5_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let xml = br#"<?xml version="1.0"?>
<Delete>
  <Object><Key>key1</Key></Object>
</Delete>"#;
        let req = new_req(
            http::Method::POST,
            "/mybucket",
            "delete",
            vec![],
            xml.to_vec(),
        );
        let op = S3Operation::DeleteObjects {
            bucket: test_bucket_name("mybucket"),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequest { .. }) => {}
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn delete_objects_invalid_content_md5_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let xml = br#"<?xml version="1.0"?>
<Delete>
  <Object><Key>key1</Key></Object>
</Delete>"#;
        let req = new_req(
            http::Method::POST,
            "/mybucket",
            "delete",
            vec![("Content-MD5".to_string(), "not-base64".to_string())],
            xml.to_vec(),
        );
        let op = S3Operation::DeleteObjects {
            bucket: test_bucket_name("mybucket"),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidDigest) => {}
            Err(e) => panic!("expected InvalidDigest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn delete_objects_bad_content_md5_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let xml = br#"<?xml version="1.0"?>
<Delete>
  <Object><Key>key1</Key></Object>
</Delete>"#;
        let req = new_req(
            http::Method::POST,
            "/mybucket",
            "delete",
            vec![(
                "Content-MD5".to_string(),
                "AAAAAAAAAAAAAAAAAAAAAA==".to_string(),
            )],
            xml.to_vec(),
        );
        let op = S3Operation::DeleteObjects {
            bucket: test_bucket_name("mybucket"),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::ContentMd5Mismatch) => {}
            Err(e) => panic!("expected ContentMd5Mismatch, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    // ── Abort-on-error path tests ──────────────────────────────────────

    #[test]
    fn put_object_abort_cleans_up_session_on_precondition_failure() {
        // Write an object, then PutObject with If-None-Match:* so finalize
        // fails with PreconditionFailed.  The handler's abort path must
        // clean up the streaming session so no session is left behind.
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        // First put succeeds.
        let req = new_req(http::Method::GET, "", "", vec![], b"hello".to_vec());
        let op = S3Operation::PutObject {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        fe.dispatch_routed(&req, &test_auth(), op).unwrap();

        // Second put with If-None-Match:* must fail.
        let req2 = new_req(
            http::Method::GET,
            "",
            "",
            vec![("if-none-match".to_string(), "*".to_string())],
            b"world".to_vec(),
        );
        let op2 = S3Operation::PutObject {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req2, &test_auth(), op2) {
            Err(ServerError::PreconditionFailed { .. }) => {}
            Err(e) => panic!("expected PreconditionFailed, got {e:?}"),
            Ok(_) => panic!("expected PreconditionFailed, got Ok"),
        }

        // No leaked streaming sessions.
        assert_eq!(
            storage::test_support::stream_upload_session_count(&fe.test_storage_cluster).unwrap(),
            0,
            "streaming session leaked after PutObject precondition failure"
        );
    }
}
