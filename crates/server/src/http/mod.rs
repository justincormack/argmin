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
/// Returns `Ok(None)` if the parameter is absent, `Ok(Some(id))` if valid,
/// or `Err` if the value is present but not a valid version ID.
fn parse_version_id(req: &S3Request) -> Result<Option<u64>, ServerError> {
    match req.query_param("versionId") {
        None => Ok(None),
        Some(v) if v == "null" => Ok(Some(0)),
        Some(v) => v
            .parse::<u64>()
            .map(Some)
            .map_err(|_| ServerError::InvalidArgument {
                reason: format!("invalid versionId: {v}"),
            }),
    }
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
        // Route first to detect OPTIONS requests (which bypass auth).
        let operation = match route(&s3req.method, &s3req.path, &s3req.query_string) {
            Ok(op) => op,
            Err(err) => return S3Response::error(&err, &s3req.path),
        };

        // OPTIONS (preflight CORS) bypasses authentication.
        if let S3Operation::OptionsRequest { ref bucket, .. } = operation {
            return self.handle_options_request(s3req, bucket);
        }

        let auth = self.authenticate(s3req);
        let result = match auth {
            Ok(auth) => self.dispatch_routed(s3req, &auth, operation),
            Err(err) => Err(err),
        };

        let mut resp = match result {
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
        };

        // CORS response headers on actual (non-preflight) requests.
        if let Some(origin) = s3req.header("origin") {
            let bucket = self.extract_bucket_from_path(&s3req.path);
            if let Some(bucket) = bucket {
                self.apply_cors_headers(&mut resp, &bucket, origin, &s3req.method);
            }
        }

        resp
    }

    /// Handle an OPTIONS (CORS preflight) request. No auth required.
    fn handle_options_request(&self, req: &S3Request, bucket: &str) -> S3Response {
        let origin = match req.header("origin") {
            Some(o) => o,
            None => {
                return S3Response::error(
                    &ServerError::InvalidRequest {
                        reason: "Insufficient information. Origin request header needed."
                            .to_string(),
                    },
                    &req.path,
                )
            }
        };

        let request_method = match req.header("access-control-request-method") {
            Some(m) => m,
            None => {
                return S3Response::error(
                    &ServerError::InvalidRequest {
                        reason:
                            "Insufficient information. Access-Control-Request-Method request header needed."
                                .to_string(),
                    },
                    &req.path,
                )
            }
        };

        let request_headers_str = req.header("access-control-request-headers");
        let request_headers: Vec<&str> = request_headers_str
            .map(|h| h.split(',').map(|s| s.trim()).collect())
            .unwrap_or_default();

        // Load CORS config
        let cors_config_xml = match self.coordinator.get_bucket_cors(bucket) {
            Ok(Some(xml)) => xml,
            _ => return S3Response::forbidden(),
        };
        let config = match crate::http::xml::parse_cors_config_xml(cors_config_xml.as_bytes()) {
            Ok(c) => c,
            Err(_) => return S3Response::forbidden(),
        };

        match crate::cors::find_matching_rule(&config, origin, request_method, &request_headers) {
            Some(m) => {
                let headers = crate::cors::preflight_response_headers(
                    m.rule,
                    origin,
                    m.matched_origin,
                    request_headers_str,
                );
                let mut resp = S3Response::cors_preflight();
                for (k, v) in headers {
                    resp.headers.push((k, v));
                }
                resp
            }
            None => S3Response::forbidden(),
        }
    }

    /// Apply CORS headers to an actual (non-preflight) response if the request
    /// has an Origin header and a matching CORS rule exists.
    fn apply_cors_headers(&self, resp: &mut S3Response, bucket: &str, origin: &str, method: &str) {
        let cors_config_xml = match self.coordinator.get_bucket_cors(bucket) {
            Ok(Some(xml)) => xml,
            _ => return,
        };
        let config = match crate::http::xml::parse_cors_config_xml(cors_config_xml.as_bytes()) {
            Ok(c) => c,
            Err(_) => return,
        };

        if let Some(m) = crate::cors::find_matching_rule(&config, origin, method, &[]) {
            let headers = crate::cors::actual_response_headers(m.rule, origin, m.matched_origin);
            for (k, v) in headers {
                resp.headers.push((k, v));
            }
        }
    }

    /// Extract bucket name from the request path (first path segment).
    fn extract_bucket_from_path(&self, path: &str) -> Option<String> {
        let trimmed = path.strip_prefix('/').unwrap_or(path);
        if trimmed.is_empty() {
            return None;
        }
        let bucket = match trimmed.find('/') {
            Some(pos) => &trimmed[..pos],
            None => trimmed,
        };
        if bucket.is_empty() {
            None
        } else {
            Some(bucket.to_string())
        }
    }

    fn dispatch_routed(
        &self,
        req: &S3Request,
        auth: &AuthContext,
        operation: S3Operation,
    ) -> Result<S3Response, ServerError> {
        // Dispatch to coordinator
        match operation {
            S3Operation::ListBuckets => {
                let owner_principal = self.require_principal(auth)?;
                let buckets = self.coordinator.list_buckets_for_owner(owner_principal)?;
                Ok(S3Response::list_buckets(&buckets, owner_principal))
            }
            S3Operation::CreateBucket { bucket } => {
                let owner_principal = self.require_principal(auth)?;
                let acl = parse_bucket_acl(req)?;
                let public_read = match acl {
                    BucketAcl::Private => false,
                    BucketAcl::PublicRead => true,
                    BucketAcl::UnsupportedPublic => {
                        return Err(ServerError::NotImplemented {
                            feature: "public-read-write and authenticated-read ACLs".to_string(),
                        });
                    }
                };
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
                    let src_version_id = match src_version_id_str {
                        None => None,
                        Some(v) if v == "null" => Some(0),
                        Some(v) => {
                            Some(v.parse::<u64>().map_err(|_| ServerError::InvalidArgument {
                                reason: format!("invalid versionId in copy source: {v}"),
                            })?)
                        }
                    };
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
                    // Parse inline tags before writing so invalid tags don't leave orphan objects
                    let tagging_directive = match req.header("x-amz-tagging-directive") {
                        Some(d) if d.eq_ignore_ascii_case("REPLACE") => "REPLACE",
                        _ => "COPY",
                    };
                    let inline_tags_xml = if tagging_directive == "REPLACE" {
                        if let Some(tagging_header) = req.header("x-amz-tagging") {
                            let tags = xml::parse_url_encoded_tags(tagging_header)?;
                            if tags.is_empty() {
                                None
                            } else {
                                Some(xml::get_tagging_xml(&tags))
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    };
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
                    // Apply tagging based on directive
                    let dst_vid = Some(result.version_id);
                    if tagging_directive == "COPY" {
                        // Copy source object's tags to destination
                        if let Some(src_tags) = self.coordinator.get_object_tags(
                            &src_bucket,
                            &src_key,
                            src_version_id,
                        )? {
                            self.coordinator
                                .put_object_tags(&bucket, &key, dst_vid, &src_tags)?;
                        }
                    } else if let Some(tags_xml) = inline_tags_xml {
                        self.coordinator
                            .put_object_tags(&bucket, &key, dst_vid, &tags_xml)?;
                    }
                    Ok(S3Response::copy_object(&result))
                } else {
                    // Normal PutObject path
                    self.authorize_bucket_write(auth, &bucket)?;
                    validate_checksum_headers(req)?;
                    // Parse inline tags before writing so invalid tags don't leave orphan objects
                    let inline_tags_xml = if let Some(tagging_header) = req.header("x-amz-tagging")
                    {
                        let tags = xml::parse_url_encoded_tags(tagging_header)?;
                        if tags.is_empty() {
                            None
                        } else {
                            Some(xml::get_tagging_xml(&tags))
                        }
                    } else {
                        None
                    };
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
                    if let Some(tags_xml) = inline_tags_xml {
                        self.coordinator.put_object_tags(
                            &bucket,
                            &key,
                            Some(result.version_id),
                            &tags_xml,
                        )?;
                    }
                    let mut resp = S3Response::put_object(&result);
                    append_checksum_response_headers(&mut resp, req);
                    Ok(resp)
                }
            }
            S3Operation::GetObject { bucket, key } => {
                self.authorize_bucket_read(auth, &bucket)?;
                let cond = read_condition_from_headers(req);
                let vid = parse_version_id(req)?;
                if let Some(range_header) = req.header("range") {
                    let byte_range = crate::range::ByteRange::parse(range_header)?;
                    match self
                        .coordinator
                        .get_object_range(&bucket, &key, vid, byte_range, &cond)
                    {
                        Ok(result) => {
                            let tags = result.tags.clone();
                            let mut resp = S3Response::get_object_range(result);
                            if let Some(tags_xml) = tags {
                                let count = xml::count_tags_in_xml(&tags_xml);
                                if count > 0 {
                                    resp.headers.push((
                                        "x-amz-tagging-count".to_string(),
                                        count.to_string(),
                                    ));
                                }
                            }
                            Ok(resp)
                        }
                        Err(ServerError::InvalidRange { total_size }) => {
                            Ok(S3Response::range_not_satisfiable(total_size))
                        }
                        Err(e) => Err(e),
                    }
                } else {
                    let result = self.coordinator.get_object(&bucket, &key, vid, &cond)?;
                    let checksum_mode = req.header("x-amz-checksum-mode");
                    let tags = result.tags.clone();
                    let mut resp = S3Response::get_object(result, checksum_mode);
                    apply_response_overrides(&mut resp, req);
                    // Add x-amz-tagging-count if the object has tags
                    if let Some(tags_xml) = tags {
                        let count = xml::count_tags_in_xml(&tags_xml);
                        if count > 0 {
                            resp.headers
                                .push(("x-amz-tagging-count".to_string(), count.to_string()));
                        }
                    }
                    Ok(resp)
                }
            }
            S3Operation::DeleteObject { bucket, key } => {
                self.authorize_bucket_write(auth, &bucket)?;
                let cond = delete_condition_from_headers(req);
                let vid = parse_version_id(req)?;
                let result = self.coordinator.delete_object(&bucket, &key, vid, &cond)?;
                Ok(S3Response::delete_object(&result))
            }
            S3Operation::HeadObject { bucket, key } => {
                self.authorize_bucket_read(auth, &bucket)?;
                let cond = read_condition_from_headers(req);
                let vid = parse_version_id(req)?;
                let result = self.coordinator.head_object(&bucket, &key, vid, &cond)?;
                let checksum_mode = req.header("x-amz-checksum-mode");
                let mut resp = S3Response::head_object(&result, checksum_mode);
                // Add x-amz-tagging-count if the object has tags
                if let Some(tags_xml) = &result.tags {
                    let count = xml::count_tags_in_xml(tags_xml);
                    if count > 0 {
                        resp.headers
                            .push(("x-amz-tagging-count".to_string(), count.to_string()));
                    }
                }
                Ok(resp)
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
            S3Operation::PutBucketCors { bucket } => {
                self.authorize_bucket_write(auth, &bucket)?;
                let config = xml::parse_cors_config_xml(&req.body)?;
                let config_xml = xml::get_cors_config_xml(&config);
                self.coordinator.put_bucket_cors(&bucket, &config_xml)?;
                Ok(S3Response::put_bucket_cors())
            }
            S3Operation::GetBucketCors { bucket } => {
                self.authorize_bucket_read(auth, &bucket)?;
                match self.coordinator.get_bucket_cors(&bucket)? {
                    Some(config_xml) => Ok(S3Response::get_bucket_cors(&config_xml)),
                    None => Err(ServerError::NoSuchCorsConfiguration {
                        bucket: bucket.clone(),
                    }),
                }
            }
            S3Operation::DeleteBucketCors { bucket } => {
                self.authorize_bucket_write(auth, &bucket)?;
                self.coordinator.delete_bucket_cors(&bucket)?;
                Ok(S3Response::delete_bucket_cors())
            }
            S3Operation::PutBucketTagging { bucket } => {
                self.authorize_bucket_write(auth, &bucket)?;
                let tags = xml::parse_tagging_xml(&req.body, 50)?;
                let tags_xml = xml::get_tagging_xml(&tags);
                self.coordinator.put_bucket_tags(&bucket, &tags_xml)?;
                Ok(S3Response::put_bucket_tagging())
            }
            S3Operation::GetBucketTagging { bucket } => {
                self.authorize_bucket_read(auth, &bucket)?;
                match self.coordinator.get_bucket_tags(&bucket)? {
                    Some(tags_xml) => Ok(S3Response::get_bucket_tagging(&tags_xml)),
                    None => Err(ServerError::NoSuchTagSet {
                        resource: bucket.clone(),
                    }),
                }
            }
            S3Operation::DeleteBucketTagging { bucket } => {
                self.authorize_bucket_write(auth, &bucket)?;
                self.coordinator.delete_bucket_tags(&bucket)?;
                Ok(S3Response::delete_bucket_tagging())
            }
            S3Operation::PutObjectTagging { bucket, key } => {
                self.authorize_bucket_write(auth, &bucket)?;
                let vid = parse_version_id(req)?;
                let tags = xml::parse_tagging_xml(&req.body, 10)?;
                let tags_xml = xml::get_tagging_xml(&tags);
                self.coordinator
                    .put_object_tags(&bucket, &key, vid, &tags_xml)?;
                Ok(S3Response::put_object_tagging())
            }
            S3Operation::GetObjectTagging { bucket, key } => {
                self.authorize_bucket_read(auth, &bucket)?;
                let vid = parse_version_id(req)?;
                match self.coordinator.get_object_tags(&bucket, &key, vid)? {
                    Some(tags_xml) => Ok(S3Response::get_object_tagging(&tags_xml)),
                    None => {
                        // S3 returns empty TagSet (not 404) for objects with no tags
                        let empty = xml::get_tagging_xml(&[]);
                        Ok(S3Response::get_object_tagging(&empty))
                    }
                }
            }
            S3Operation::DeleteObjectTagging { bucket, key } => {
                self.authorize_bucket_write(auth, &bucket)?;
                let vid = parse_version_id(req)?;
                self.coordinator.delete_object_tags(&bucket, &key, vid)?;
                Ok(S3Response::delete_object_tagging())
            }
            S3Operation::PutBucketPublicAccessBlock { bucket } => {
                self.authorize_bucket_write(auth, &bucket)?;
                let config = xml::parse_public_access_block_xml(&req.body)?;
                let config_xml = xml::get_public_access_block_xml(&config);
                self.coordinator
                    .put_bucket_public_access_block(&bucket, &config_xml)?;
                Ok(S3Response::put_bucket_public_access_block())
            }
            S3Operation::GetBucketPublicAccessBlock { bucket } => {
                self.authorize_bucket_write(auth, &bucket)?;
                match self.coordinator.get_bucket_public_access_block(&bucket)? {
                    Some(config_xml) => Ok(S3Response::get_bucket_public_access_block(&config_xml)),
                    None => Err(ServerError::NoSuchPublicAccessBlockConfiguration {
                        bucket: bucket.clone(),
                    }),
                }
            }
            S3Operation::DeleteBucketPublicAccessBlock { bucket } => {
                self.authorize_bucket_write(auth, &bucket)?;
                self.coordinator
                    .delete_bucket_public_access_block(&bucket)?;
                Ok(S3Response::delete_bucket_public_access_block())
            }
            S3Operation::PutBucketAcl { bucket } => {
                self.authorize_bucket_write(auth, &bucket)?;
                let acl = parse_bucket_acl(req)?;
                match acl {
                    BucketAcl::Private => {
                        self.coordinator.put_bucket_acl(&bucket, false)?;
                    }
                    BucketAcl::PublicRead => {
                        // Enforce BlockPublicAcls
                        if let Some(pab_xml) =
                            self.coordinator.get_bucket_public_access_block(&bucket)?
                        {
                            let pab = xml::parse_public_access_block_xml(pab_xml.as_bytes())?;
                            if pab.block_public_acls {
                                return Err(ServerError::AccessDenied);
                            }
                        }
                        self.coordinator.put_bucket_acl(&bucket, true)?;
                    }
                    BucketAcl::UnsupportedPublic => {
                        // Check BlockPublicAcls first — return 403 if set
                        if let Some(pab_xml) =
                            self.coordinator.get_bucket_public_access_block(&bucket)?
                        {
                            let pab = xml::parse_public_access_block_xml(pab_xml.as_bytes())?;
                            if pab.block_public_acls {
                                return Err(ServerError::AccessDenied);
                            }
                        }
                        // Not blocked, but we don't support these ACL semantics
                        return Err(ServerError::NotImplemented {
                            feature: "public-read-write and authenticated-read ACLs".to_string(),
                        });
                    }
                }
                Ok(S3Response::put_bucket_acl())
            }
            // OptionsRequest is handled before auth in handle_s3_request
            S3Operation::OptionsRequest { .. } => {
                unreachable!("OPTIONS handled before dispatch")
            }
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
        let mut effective_public_read = info.public_read;
        // IgnorePublicAcls: treat public-read as private if set
        if effective_public_read {
            if let Some(ref pab_xml) = info.public_access_block {
                if let Ok(pab) = xml::parse_public_access_block_xml(pab_xml.as_bytes()) {
                    if pab.ignore_public_acls {
                        effective_public_read = false;
                    }
                }
            }
        }
        let visibility = if effective_public_read {
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
                form.field("x-amz-credential")
                    .ok_or_else(|| ServerError::InvalidRequest {
                        reason: "missing x-amz-credential".to_string(),
                    })?,
                form.field("x-amz-date")
                    .ok_or_else(|| ServerError::InvalidRequest {
                        reason: "missing x-amz-date".to_string(),
                    })?,
                form.field("policy")
                    .ok_or_else(|| ServerError::InvalidRequest {
                        reason: "missing policy".to_string(),
                    })?,
                form.field("x-amz-signature")
                    .ok_or_else(|| ServerError::InvalidRequest {
                        reason: "missing x-amz-signature".to_string(),
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
/// Checksum algorithm names and the corresponding header names.
const CHECKSUM_HEADERS: &[(&str, &str)] = &[
    ("SHA256", "x-amz-checksum-sha256"),
    ("CRC64NVME", "x-amz-checksum-crc64nvme"),
    ("CRC32", "x-amz-checksum-crc32"),
    ("CRC32C", "x-amz-checksum-crc32c"),
    ("SHA1", "x-amz-checksum-sha1"),
];

/// Validate checksum headers on PutObject. If a checksum header is present,
/// compute the actual checksum and compare. Returns `BadDigest` on mismatch.
///
/// Enforces that at most one checksum header is present, and if
/// `x-amz-checksum-algorithm` is set it must match the provided checksum header.
fn validate_checksum_headers(req: &S3Request) -> Result<(), ServerError> {
    use base64::Engine;

    let algo_header = req.header("x-amz-checksum-algorithm");
    let mut found_algo: Option<&str> = None;

    for &(algo, header) in CHECKSUM_HEADERS {
        if let Some(claimed) = req.header(header) {
            // Reject multiple checksum headers
            if found_algo.is_some() {
                return Err(ServerError::InvalidRequest {
                    reason: "only one checksum header may be specified".into(),
                });
            }
            found_algo = Some(algo);

            // If x-amz-checksum-algorithm is set, it must match this header
            if let Some(declared) = algo_header {
                if !declared.eq_ignore_ascii_case(algo) {
                    return Err(ServerError::InvalidRequest {
                        reason: format!(
                            "checksum algorithm mismatch: header says {} but got {}",
                            declared, algo
                        ),
                    });
                }
            }

            let actual_b64 = match algo {
                "SHA256" => {
                    let digest = ring::digest::digest(&ring::digest::SHA256, &req.body);
                    base64::engine::general_purpose::STANDARD.encode(digest.as_ref())
                }
                "SHA1" => {
                    let digest =
                        ring::digest::digest(&ring::digest::SHA1_FOR_LEGACY_USE_ONLY, &req.body);
                    base64::engine::general_purpose::STANDARD.encode(digest.as_ref())
                }
                "CRC32" => {
                    let crc = unsafe {
                        ec_sys::crc32_gzip_refl(0, req.body.as_ptr(), req.body.len() as u64)
                    };
                    base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes())
                }
                "CRC32C" => {
                    let crc = unsafe {
                        ec_sys::crc32_iscsi(req.body.as_ptr() as *mut _, req.body.len() as i32, 0)
                    };
                    base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes())
                }
                "CRC64NVME" => {
                    let crc = crc64::checksum(&req.body);
                    base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes())
                }
                _ => continue,
            };
            if claimed != actual_b64 {
                return Err(ServerError::BadDigest);
            }
        }
    }
    Ok(())
}

/// Append any checksum headers that were sent on PutObject to the response.
fn append_checksum_response_headers(resp: &mut S3Response, req: &S3Request) {
    for &(_, header) in CHECKSUM_HEADERS {
        if let Some(value) = req.header(header) {
            resp.headers.push((header.to_string(), value.to_string()));
        }
    }
}

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

enum BucketAcl {
    Private,
    PublicRead,
    /// ACL values that grant public access but whose specific semantics we don't implement
    /// (public-read-write, authenticated-read). Kept separate so BlockPublicAcls can reject
    /// them with 403 while normal requests get NotImplemented.
    UnsupportedPublic,
}

fn parse_bucket_acl(req: &S3Request) -> Result<BucketAcl, ServerError> {
    match req.header("x-amz-acl") {
        None | Some("private") => Ok(BucketAcl::Private),
        Some("public-read") => Ok(BucketAcl::PublicRead),
        Some("public-read-write") | Some("authenticated-read") => Ok(BucketAcl::UnsupportedPublic),
        Some(other) => Err(ServerError::InvalidArgument {
            reason: format!("unsupported x-amz-acl value: {other}"),
        }),
    }
}
