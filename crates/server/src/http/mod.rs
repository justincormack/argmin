/// HTTP frontend: parses requests, authenticates, dispatches to coordinator.
pub mod multipart;
pub mod request;
pub mod response;
pub mod router;
pub mod serve;
pub mod xml;

use std::time::{SystemTime, UNIX_EPOCH};

use auth::{authenticate_request, AuthContext, AuthMode, CredentialStore};
use bytes::Bytes;
use http_body_util::Full;

use crate::authz::{can_read_bucket, can_write_bucket, ResourceVisibility};
use crate::conditional::{
    copy_source_condition_from_headers, delete_condition_from_headers, read_condition_from_headers,
    write_condition_from_headers,
};
use crate::coordinator::Coordinator;
use crate::coordinator::MetadataDirective;
use crate::error::ServerError;
use request::S3Request;
use response::S3Response;
use router::{route, S3Operation};

/// Parse versionId query parameter from an S3 request.
fn parse_version_id(req: &S3Request) -> Option<u64> {
    req.query_param("versionId").and_then(|v| {
        if v == "null" {
            Some(0)
        } else {
            v.parse::<u64>().ok()
        }
    })
}

/// The HTTP frontend that handles incoming requests.
pub struct HttpFrontend {
    pub coordinator: Coordinator,
    pub credentials: CredentialStore,
}

impl HttpFrontend {
    /// Handle a parsed S3 request: authenticate, dispatch, and return the response.
    ///
    /// The caller (serve layer) is responsible for parsing the HTTP request into
    /// an S3Request and converting the S3Response back to an HTTP response.
    pub fn handle_s3_request(&self, s3req: &S3Request) -> S3Response {
        let auth = self.authenticate(s3req);
        let result = match auth {
            Ok(auth) => self.dispatch(s3req, &auth),
            Err(err) => Err(err),
        };

        match result {
            Ok(resp) => resp,
            Err(ServerError::NotModified {
                ref etag,
                last_modified,
            }) => S3Response::not_modified(etag, last_modified),
            Err(ServerError::PreconditionFailed) => S3Response::precondition_failed(),
            Err(ref err @ ServerError::DeleteMarkerHit { .. }) => {
                let mut resp = S3Response::error(err, &s3req.path);
                resp.headers
                    .push(("x-amz-delete-marker".to_string(), "true".to_string()));
                resp
            }
            Err(err) => S3Response::error(&err, &s3req.path),
        }
    }

    fn dispatch(&self, req: &S3Request, auth: &AuthContext) -> Result<S3Response, ServerError> {
        // Route
        let operation = route(&req.method, &req.path, &req.query_string)?;

        // Dispatch to coordinator
        match operation {
            S3Operation::ListBuckets => {
                let owner_principal = self.require_principal(auth)?;
                let buckets = self.coordinator.list_buckets_for_owner(owner_principal)?;
                Ok(S3Response::list_buckets(&buckets, owner_principal))
            }
            S3Operation::CreateBucket { bucket } => {
                let owner_principal = self.require_principal(auth)?;
                let public_read = parse_bucket_acl(req)?;
                self.coordinator
                    .create_bucket_for_owner(owner_principal, &bucket, public_read)?;
                Ok(S3Response::create_bucket(&bucket))
            }
            S3Operation::DeleteBucket { bucket } => {
                self.authorize_bucket_write(auth, &bucket)?;
                self.coordinator.delete_bucket(&bucket)?;
                Ok(S3Response::delete_bucket())
            }
            S3Operation::HeadBucket { bucket } => {
                self.authorize_bucket_read(auth, &bucket)?;
                let info = self.coordinator.head_bucket(&bucket)?;
                Ok(S3Response::head_bucket(&info))
            }
            S3Operation::ListObjectsV1 { bucket } => {
                self.authorize_bucket_read(auth, &bucket)?;
                let prefix = req.query_param("prefix");
                let delimiter = req.query_param("delimiter").filter(|d| !d.is_empty());
                let marker = req.query_param("marker");
                let encoding_type = req.query_param("encoding-type");
                let allow_unordered = req.query_param("allow-unordered");
                if allow_unordered.is_some() && delimiter.is_some() {
                    return Err(ServerError::InvalidArgument {
                        reason: "allow-unordered is not supported with delimiter".to_string(),
                    });
                }
                let max_keys: u32 = parse_max_keys(req.query_param("max-keys"))?;

                let result = self.coordinator.list_objects_v2(
                    &bucket,
                    prefix.as_deref(),
                    delimiter.as_deref(),
                    marker.as_deref(),
                    max_keys,
                )?;
                Ok(S3Response::list_objects_v1(
                    &bucket,
                    prefix.as_deref(),
                    delimiter.as_deref(),
                    marker.as_deref(),
                    encoding_type.as_deref(),
                    max_keys,
                    &result,
                ))
            }
            S3Operation::ListObjectsV2 { bucket } => {
                self.authorize_bucket_read(auth, &bucket)?;
                let prefix = req.query_param("prefix");
                let delimiter = req.query_param("delimiter").filter(|d| !d.is_empty());
                let encoding_type = req.query_param("encoding-type");
                let fetch_owner = req
                    .query_param("fetch-owner")
                    .map(|v| v == "true" || v == "1" || v == "True")
                    .unwrap_or(false);
                let allow_unordered = req.query_param("allow-unordered");
                if allow_unordered.is_some() && delimiter.is_some() {
                    return Err(ServerError::InvalidArgument {
                        reason: "allow-unordered is not supported with delimiter".to_string(),
                    });
                }

                let continuation_token_raw = req.query_param("continuation-token");
                let start_after_raw = req.query_param("start-after");
                let continuation_token = continuation_token_raw
                    .as_deref()
                    .or(start_after_raw.as_deref())
                    .filter(|v| !v.is_empty());
                let max_keys: u32 = parse_max_keys(req.query_param("max-keys"))?;

                let result = self.coordinator.list_objects_v2(
                    &bucket,
                    prefix.as_deref(),
                    delimiter.as_deref(),
                    continuation_token,
                    max_keys,
                )?;
                Ok(S3Response::list_objects_v2(
                    &bucket,
                    prefix.as_deref(),
                    delimiter.as_deref(),
                    encoding_type.as_deref(),
                    continuation_token_raw.as_deref(),
                    start_after_raw.as_deref(),
                    fetch_owner,
                    max_keys,
                    &result,
                ))
            }
            S3Operation::PutObject { bucket, key } => {
                if let Some(copy_source) = req.header("x-amz-copy-source") {
                    // CopyObject path
                    let (src_bucket, src_key, src_version_id_str) =
                        request::parse_copy_source(copy_source)?;
                    let src_version_id = src_version_id_str.and_then(|v| {
                        if v == "null" {
                            Some(0)
                        } else {
                            v.parse::<u64>().ok()
                        }
                    });
                    self.authorize_bucket_write(auth, &bucket)?;
                    self.authorize_bucket_read(auth, &src_bucket)?;
                    let src_cond = copy_source_condition_from_headers(req);
                    let dst_cond = write_condition_from_headers(req);
                    let directive = match req.header("x-amz-metadata-directive") {
                        Some(d) if d.eq_ignore_ascii_case("REPLACE") => MetadataDirective::Replace,
                        _ => MetadataDirective::Copy,
                    };
                    // Copy-to-self without REPLACE is invalid (AWS returns 400)
                    if matches!(directive, MetadataDirective::Copy)
                        && src_bucket == bucket
                        && src_key == key
                    {
                        return Err(ServerError::InvalidRequest {
                            reason: "This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes.".to_string(),
                        });
                    }
                    let header_pairs: Vec<(&str, &str)> = req
                        .headers
                        .iter()
                        .map(|(k, v)| (k.as_str(), v.as_str()))
                        .collect();
                    let result = self.coordinator.copy_object(
                        &src_bucket,
                        &src_key,
                        src_version_id,
                        &bucket,
                        &key,
                        &src_cond,
                        &dst_cond,
                        directive,
                        &header_pairs,
                    )?;
                    Ok(S3Response::copy_object(&result))
                } else {
                    // Normal PutObject path
                    self.authorize_bucket_write(auth, &bucket)?;
                    let header_pairs: Vec<(&str, &str)> = req
                        .headers
                        .iter()
                        .map(|(k, v)| (k.as_str(), v.as_str()))
                        .collect();
                    let cond = write_condition_from_headers(req);
                    let result = self.coordinator.put_object(
                        &bucket,
                        &key,
                        &req.body,
                        &header_pairs,
                        &cond,
                    )?;
                    Ok(S3Response::put_object(&result))
                }
            }
            S3Operation::GetObject { bucket, key } => {
                self.authorize_bucket_read(auth, &bucket)?;
                let cond = read_condition_from_headers(req);
                let vid = parse_version_id(req);
                if let Some(range_header) = req.header("range") {
                    let byte_range = crate::range::ByteRange::parse(range_header)?;
                    match self
                        .coordinator
                        .get_object_range(&bucket, &key, vid, byte_range, &cond)
                    {
                        Ok(result) => Ok(S3Response::get_object_range(result)),
                        Err(ServerError::InvalidRange { total_size }) => {
                            Ok(S3Response::range_not_satisfiable(total_size))
                        }
                        Err(e) => Err(e),
                    }
                } else {
                    let result = self.coordinator.get_object(&bucket, &key, vid, &cond)?;
                    let mut resp = S3Response::get_object(result);
                    apply_response_overrides(&mut resp, req);
                    Ok(resp)
                }
            }
            S3Operation::DeleteObject { bucket, key } => {
                self.authorize_bucket_write(auth, &bucket)?;
                let cond = delete_condition_from_headers(req);
                let vid = parse_version_id(req);
                let result = self.coordinator.delete_object(&bucket, &key, vid, &cond)?;
                Ok(S3Response::delete_object(&result))
            }
            S3Operation::HeadObject { bucket, key } => {
                self.authorize_bucket_read(auth, &bucket)?;
                let cond = read_condition_from_headers(req);
                let vid = parse_version_id(req);
                let result = self.coordinator.head_object(&bucket, &key, vid, &cond)?;
                Ok(S3Response::head_object(&result))
            }
            S3Operation::DeleteObjects { bucket } => {
                self.authorize_bucket_write(auth, &bucket)?;
                let (entries, quiet) = xml::parse_delete_objects_xml(&req.body)?;
                let cond = delete_condition_from_headers(req);
                let result = self.coordinator.delete_objects(&bucket, &entries, &cond)?;
                Ok(S3Response::delete_objects(&result, quiet))
            }
            S3Operation::PutBucketVersioning { bucket } => {
                self.authorize_bucket_write(auth, &bucket)?;
                let versioning_state = xml::parse_versioning_config_xml(&req.body)?;
                self.coordinator
                    .put_bucket_versioning(&bucket, versioning_state)?;
                Ok(S3Response::put_bucket_versioning())
            }
            S3Operation::GetBucketVersioning { bucket } => {
                self.authorize_bucket_read(auth, &bucket)?;
                let state = self.coordinator.get_bucket_versioning(&bucket)?;
                Ok(S3Response::get_bucket_versioning(state))
            }
            S3Operation::PostObject { bucket } => self.handle_post_object(req, auth, &bucket),
            S3Operation::ListObjectVersions { bucket } => {
                self.authorize_bucket_read(auth, &bucket)?;
                let prefix = req.query_param("prefix");
                let key_marker = req.query_param("key-marker");
                let version_id_marker = req
                    .query_param("version-id-marker")
                    .and_then(|s| s.parse::<u64>().ok());
                let max_keys: u32 = req
                    .query_param("max-keys")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(1000);

                let result = self.coordinator.list_object_versions(
                    &bucket,
                    prefix.as_deref(),
                    key_marker.as_deref(),
                    version_id_marker,
                    max_keys,
                )?;
                Ok(S3Response::list_object_versions(
                    &bucket,
                    prefix.as_deref(),
                    key_marker.as_deref(),
                    max_keys,
                    &result,
                ))
            }
        }
    }

    fn authenticate(&self, req: &S3Request) -> Result<AuthContext, ServerError> {
        let header_pairs = req.header_pairs();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let auth_result = authenticate_request(
            &req.method,
            &req.path,
            &req.query_string,
            &header_pairs,
            &req.body,
            &self.credentials,
            self.coordinator.region(),
            "s3",
            now,
        );

        let auth = match auth_result {
            Ok(auth) => auth,
            Err(auth::AuthError::MissingAuth) => AuthContext {
                mode: AuthMode::Anonymous,
                access_key_id: None,
                principal: None,
                request_epoch_secs: None,
            },
            Err(err) => return Err(ServerError::Auth(err)),
        };

        // Enforce ±15 minute time skew on x-amz-date to prevent replay attacks.
        // Reject malformed timestamps — skipping the check would weaken replay protection.
        if req.header("x-amz-date").is_some() {
            let request_epoch = auth.request_epoch_secs.ok_or(ServerError::InvalidRequest {
                reason: "malformed x-amz-date timestamp".to_string(),
            })?;
            let skew = now.abs_diff(request_epoch);
            if skew > 15 * 60 {
                return Err(ServerError::Auth(auth::AuthError::RequestExpired));
            }
        }

        // Verify payload integrity: if the client provided an actual content hash
        // (not UNSIGNED-PAYLOAD), recompute and compare to detect transit corruption.
        if let Some(claimed) = req.header("x-amz-content-sha256") {
            if claimed != "UNSIGNED-PAYLOAD" {
                let actual = auth::canonical::sha256_hex(&req.body);
                if actual != claimed {
                    return Err(ServerError::InvalidRequest {
                        reason: "payload content SHA-256 mismatch".to_string(),
                    });
                }
            }
        }

        Ok(auth)
    }

    fn require_principal<'a>(&self, auth: &'a AuthContext) -> Result<&'a str, ServerError> {
        auth.principal
            .as_deref()
            .ok_or(ServerError::Auth(auth::AuthError::AccessDenied))
    }

    fn authorize_bucket_read(&self, auth: &AuthContext, bucket: &str) -> Result<(), ServerError> {
        let info = self.coordinator.head_bucket(bucket)?;
        let visibility = if info.public_read {
            ResourceVisibility::PublicRead
        } else {
            ResourceVisibility::Private
        };
        if can_read_bucket(auth, &info.owner_principal, visibility) {
            Ok(())
        } else {
            Err(ServerError::Auth(auth::AuthError::AccessDenied))
        }
    }

    fn authorize_bucket_write(&self, auth: &AuthContext, bucket: &str) -> Result<(), ServerError> {
        let info = self.coordinator.head_bucket(bucket)?;
        if can_write_bucket(auth, &info.owner_principal) {
            Ok(())
        } else {
            Err(ServerError::Auth(auth::AuthError::AccessDenied))
        }
    }

    fn handle_post_object(
        &self,
        req: &S3Request,
        auth: &AuthContext,
        bucket: &str,
    ) -> Result<S3Response, ServerError> {
        // Extract multipart boundary from Content-Type
        let content_type =
            req.header("content-type")
                .ok_or_else(|| ServerError::InvalidRequest {
                    reason: "POST Object requires Content-Type: multipart/form-data".to_string(),
                })?;
        let boundary = multipart::extract_boundary(content_type).ok_or_else(|| {
            ServerError::InvalidRequest {
                reason: "POST Object requires multipart/form-data with boundary".to_string(),
            }
        })?;

        // Parse the multipart form
        let form = multipart::parse_multipart(&req.body, boundary)?;

        // Authenticate using form fields: auto-detect SigV4 vs SigV2
        let post_auth = if let Some(algo) = form.field("x-amz-algorithm") {
            // SigV4 POST
            auth::authenticate_post_sigv4(
                algo,
                form.field("x-amz-credential").ok_or_else(|| {
                    ServerError::InvalidRequest {
                        reason: "missing x-amz-credential".to_string(),
                    }
                })?,
                form.field("x-amz-date").ok_or_else(|| {
                    ServerError::InvalidRequest {
                        reason: "missing x-amz-date".to_string(),
                    }
                })?,
                form.field("policy").ok_or_else(|| {
                    ServerError::InvalidRequest {
                        reason: "missing policy".to_string(),
                    }
                })?,
                form.field("x-amz-signature").ok_or_else(|| {
                    ServerError::InvalidRequest {
                        reason: "missing x-amz-signature".to_string(),
                    }
                })?,
                &self.credentials,
            )
        } else {
            // SigV2 POST (legacy) or anonymous
            auth::authenticate_post(
                form.field("AWSAccessKeyId"),
                form.field("policy"),
                form.field("signature"),
                &self.credentials,
            )
        }
        .map_err(|e| match e {
            auth::AuthError::MissingAuth => ServerError::InvalidRequest {
                reason: "missing required POST authentication fields".to_string(),
            },
            other => ServerError::Auth(other),
        })?;

        // Resolve object key (with ${filename} substitution) before policy validation
        let key = form.resolve_key()?;

        // Validate policy if present
        if let Some(policy_b64) = form.field("policy") {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            // Build form field pairs for policy validation, using the resolved key
            let mut field_pairs: Vec<(&str, &str)> = form
                .fields
                .iter()
                .filter(|(k, _)| !k.eq_ignore_ascii_case("key"))
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            field_pairs.push(("key", &key));

            auth::validate_post_policy(policy_b64, &field_pairs, form.file_data.len(), bucket, now)
                .map_err(|e| match &e {
                    // Structural/format errors → 400
                    auth::PostPolicyError::Malformed(_) => ServerError::InvalidRequest {
                        reason: e.to_string(),
                    },
                    // Content-length-range violations → 400
                    auth::PostPolicyError::ConditionFailed("content-length-range") => {
                        ServerError::InvalidRequest {
                            reason: e.to_string(),
                        }
                    }
                    // Other condition failures and expiration → 403
                    auth::PostPolicyError::Expired | auth::PostPolicyError::ConditionFailed(_) => {
                        ServerError::Auth(auth::AuthError::AccessDenied)
                    }
                })?;
        }

        // Use POST auth context if authenticated, otherwise fall back to header auth
        let effective_auth = if post_auth.mode != AuthMode::Anonymous {
            &post_auth
        } else {
            auth
        };

        // Authorize write access
        self.authorize_bucket_write(effective_auth, bucket)?;

        // Verify checksum if provided
        if let Some(checksum_b64) = form.field("x-amz-checksum-sha256") {
            use base64::Engine;
            let digest = ring::digest::digest(&ring::digest::SHA256, &form.file_data);
            let actual_b64 = base64::engine::general_purpose::STANDARD.encode(digest.as_ref());
            if checksum_b64 != actual_b64 {
                return Err(ServerError::InvalidRequest {
                    reason: "checksum mismatch".to_string(),
                });
            }
        }

        // Build metadata headers from form fields
        let mut header_pairs: Vec<(String, String)> = Vec::new();
        if let Some(ct) = form.field("Content-Type") {
            header_pairs.push(("content-type".to_string(), ct.to_string()));
        }
        // Pass through x-amz-meta-* fields
        for (k, v) in &form.fields {
            if k.to_ascii_lowercase().starts_with("x-amz-meta-") {
                header_pairs.push((k.to_ascii_lowercase(), v.clone()));
            }
        }
        // Also pass cache-control, content-disposition, etc.
        for name in &[
            "cache-control",
            "content-disposition",
            "content-encoding",
            "content-language",
            "expires",
        ] {
            if let Some(val) = form.field(name) {
                header_pairs.push((name.to_string(), val.to_string()));
            }
        }
        let hp_refs: Vec<(&str, &str)> = header_pairs
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        // Call put_object (same as PUT)
        let cond = crate::conditional::WriteCondition::default();
        let result = self
            .coordinator
            .put_object(bucket, &key, &form.file_data, &hp_refs, &cond)?;

        // Build response based on success_action_status
        let success_status = form
            .field("success_action_status")
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(204);

        Ok(S3Response::post_object(
            &result,
            bucket,
            &key,
            success_status,
        ))
    }
}

/// Convert an S3Response into a hyper-compatible HTTP response.
pub fn s3_response_to_hyper(resp: S3Response) -> http::Response<Full<Bytes>> {
    let mut builder = http::Response::builder().status(resp.status_code);
    for (name, value) in &resp.headers {
        builder = builder.header(name.as_str(), value.as_str());
    }
    builder
        .body(Full::new(Bytes::from(resp.body)))
        .expect("response builder should not fail")
}

fn parse_max_keys(raw: Option<String>) -> Result<u32, ServerError> {
    match raw {
        None => Ok(1000),
        Some(s) => s.parse::<u32>().map_err(|_| ServerError::InvalidArgument {
            reason: "invalid max-keys".to_string(),
        }),
    }
}

/// Apply response-* query parameter overrides to a GET response.
fn apply_response_overrides(resp: &mut S3Response, req: &S3Request) {
    let overrides: &[(&str, &str)] = &[
        ("response-content-type", "Content-Type"),
        ("response-content-disposition", "Content-Disposition"),
        ("response-content-encoding", "Content-Encoding"),
        ("response-content-language", "Content-Language"),
        ("response-cache-control", "Cache-Control"),
        ("response-expires", "Expires"),
    ];
    for &(param, header_name) in overrides {
        if let Some(value) = req.query_param(param) {
            resp.headers
                .retain(|(k, _)| !k.eq_ignore_ascii_case(header_name));
            resp.headers.push((header_name.to_string(), value));
        }
    }
}

fn parse_bucket_acl(req: &S3Request) -> Result<bool, ServerError> {
    match req.header("x-amz-acl") {
        None | Some("private") => Ok(false),
        Some("public-read") => Ok(true),
        Some(other) => Err(ServerError::InvalidArgument {
            reason: format!("unsupported x-amz-acl value: {other}"),
        }),
    }
}
