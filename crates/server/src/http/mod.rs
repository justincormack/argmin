/// HTTP frontend: parses requests, authenticates, dispatches to coordinator.
pub mod chunked;
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
use crate::coordinator::ChecksumClaim;
use crate::coordinator::Coordinator;
use crate::coordinator::CopyObjectRequest;
use crate::coordinator::CopySource;
use crate::coordinator::FinalizeStreamPartRequest;
use crate::coordinator::FinalizeStreamPutRequest;
use crate::coordinator::MetadataDirective;
use crate::coordinator::UploadPartCopyRequest;
use crate::error::ServerError;
use crate::metadata_blob::MetadataBlob;
use request::S3Request;
use response::S3Response;
use router::{route, S3Operation};
use storage::{ChecksumAlgorithm, ChecksumType};

/// Parse versionId query parameter from an S3 request.
/// Returns `Ok(None)` if the parameter is absent, `Ok(Some(id))` if valid,
/// or `Err` if the value is present but not a valid version ID.
/// Parse a version-id string into a typed `VersionId`.
fn parse_version_id_str(v: &str) -> Result<storage::VersionId, ServerError> {
    if v == "null" {
        Ok(storage::VersionId::Null)
    } else {
        v.parse::<u64>()
            .map(storage::VersionId::from_u64)
            .map_err(|_| ServerError::InvalidArgument {
                reason: format!("invalid versionId: {v}"),
            })
    }
}

fn parse_version_id(req: &S3Request) -> Result<Option<storage::VersionId>, ServerError> {
    match req.query_param("versionId") {
        None => Ok(None),
        Some(v) => parse_version_id_str(&v).map(Some),
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
            Ok(auth) => match self.maybe_decode_chunked(s3req, &auth) {
                Ok(decoded_opt) => {
                    let req = decoded_opt.as_ref().unwrap_or(s3req);
                    self.dispatch_routed(req, &auth, operation)
                }
                Err(err) => Err(err),
            },
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
            None => return S3Response::forbidden(),
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
                // Validate x-amz-object-ownership header before creating bucket
                let ownership_xml = if let Some(ownership) = req.header("x-amz-object-ownership") {
                    match ownership {
                        "BucketOwnerEnforced" | "BucketOwnerPreferred" | "ObjectWriter" => {
                            // BucketOwnerEnforced conflicts with public ACLs
                            if ownership == "BucketOwnerEnforced" && public_read {
                                return Err(ServerError::InvalidBucketAclWithObjectOwnership);
                            }
                            Some(xml::get_ownership_controls_xml(ownership))
                        }
                        _ => {
                            return Err(ServerError::InvalidArgument {
                                reason: format!(
                                    "invalid x-amz-object-ownership value: {ownership}"
                                ),
                            });
                        }
                    }
                } else {
                    // AWS default since April 2023: BucketOwnerEnforced
                    if public_read {
                        return Err(ServerError::InvalidBucketAclWithObjectOwnership);
                    }
                    Some(xml::get_ownership_controls_xml("BucketOwnerEnforced"))
                };
                self.coordinator
                    .create_bucket_for_owner(owner_principal, &bucket, public_read)?;
                if let Some(config_xml) = ownership_xml {
                    self.coordinator
                        .put_bucket_ownership_controls(&bucket, &config_xml)?;
                }
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
                    &crate::coordinator::ListObjectsV2Request {
                        bucket: &bucket,
                        prefix: prefix.as_deref(),
                        delimiter: delimiter.as_deref(),
                        continuation_token: marker.as_deref(),
                        max_keys,
                    },
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
                // AWS rejects empty continuation-token with InvalidArgument
                if let Some(ref ct) = continuation_token_raw {
                    if ct.is_empty() {
                        return Err(ServerError::InvalidArgument {
                            reason: "The continuation token provided is incorrect".to_string(),
                        });
                    }
                }
                let start_after_raw = req.query_param("start-after");
                let continuation_token = continuation_token_raw
                    .as_deref()
                    .or(start_after_raw.as_deref())
                    .filter(|v| !v.is_empty());
                let max_keys: u32 = parse_max_keys(req.query_param("max-keys"))?;

                let result = self.coordinator.list_objects_v2(
                    &crate::coordinator::ListObjectsV2Request {
                        bucket: &bucket,
                        prefix: prefix.as_deref(),
                        delimiter: delimiter.as_deref(),
                        continuation_token,
                        max_keys,
                    },
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
                        Some(v) if v == "null" => Some(storage::VersionId::Null),
                        Some(v) => Some(storage::VersionId::from_u64(v.parse::<u64>().map_err(
                            |_| ServerError::InvalidArgument {
                                reason: format!("invalid versionId in copy source: {v}"),
                            },
                        )?)),
                    };
                    self.authorize_bucket_write(auth, &bucket)?;
                    self.authorize_bucket_read(auth, &src_bucket)?;
                    // Enforce BucketOwnerEnforced on CopyObject with x-amz-acl
                    if let Some(acl_value) = req.header("x-amz-acl") {
                        if let Some(ref oc_xml) =
                            self.coordinator.get_bucket_ownership_controls(&bucket)?
                        {
                            if let Ok(val) = xml::parse_ownership_controls_xml(oc_xml.as_bytes()) {
                                if val == "BucketOwnerEnforced"
                                    && acl_value != "bucket-owner-full-control"
                                    && acl_value != "private"
                                {
                                    return Err(ServerError::AccessControlListNotSupported);
                                }
                            }
                        }
                    }
                    let src_cond = copy_source_condition_from_headers(req);
                    let dst_cond = write_condition_from_headers(req)?;
                    let header_pairs: Vec<(&str, &str)> = req
                        .headers
                        .iter()
                        .map(|(k, v)| (k.as_str(), v.as_str()))
                        .collect();
                    // Parse metadata and checksum algorithm at the HTTP boundary
                    // so the coordinator never sees raw headers.
                    let replace_metadata;
                    let replace_checksum_algo;
                    let directive = match req.header("x-amz-metadata-directive") {
                        Some(d) if d.eq_ignore_ascii_case("REPLACE") => {
                            let mut blob = MetadataBlob::from_headers(&header_pairs)?;
                            // Strip unverifiable checksum value headers — CopyObject
                            // has no body so these can't be verified.
                            const CHECKSUM_VALUE_HEADERS: &[&str] = &[
                                "x-amz-checksum-crc32",
                                "x-amz-checksum-crc32c",
                                "x-amz-checksum-crc64nvme",
                                "x-amz-checksum-sha256",
                                "x-amz-checksum-sha1",
                            ];
                            blob.entries
                                .retain(|e| !CHECKSUM_VALUE_HEADERS.contains(&e.key.as_str()));
                            replace_metadata = blob;

                            // Parse checksum algorithm if present.
                            replace_checksum_algo = match req.header("x-amz-checksum-algorithm") {
                                None => None,
                                Some(v) => Some(ChecksumAlgorithm::parse(v).ok_or_else(|| {
                                    ServerError::InvalidArgument {
                                        reason: format!("unsupported checksum algorithm: {v}"),
                                    }
                                })?),
                            };

                            MetadataDirective::Replace {
                                metadata: &replace_metadata,
                                checksum_algorithm: replace_checksum_algo,
                            }
                        }
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
                    let result = self.coordinator.copy_object(&CopyObjectRequest {
                        source: CopySource {
                            bucket: &src_bucket,
                            key: &src_key,
                            version_id: src_version_id,
                            condition: &src_cond,
                        },
                        dst_bucket: &bucket,
                        dst_key: &key,
                        dst_condition: &dst_cond,
                        directive,
                    })?;
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
                    // Enforce BucketOwnerEnforced: reject x-amz-acl unless bucket-owner-full-control or private
                    if let Some(acl_value) = req.header("x-amz-acl") {
                        if let Some(ref oc_xml) =
                            self.coordinator.get_bucket_ownership_controls(&bucket)?
                        {
                            if let Ok(val) = xml::parse_ownership_controls_xml(oc_xml.as_bytes()) {
                                if val == "BucketOwnerEnforced"
                                    && acl_value != "bucket-owner-full-control"
                                    && acl_value != "private"
                                {
                                    return Err(ServerError::AccessControlListNotSupported);
                                }
                            }
                        }
                    }
                    validate_checksum_headers(req, true)?;
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
                    let metadata_blob = MetadataBlob::from_headers(&header_pairs)?;
                    let cond = write_condition_from_headers(req)?;
                    let result = self.coordinator.put_object(
                        &crate::coordinator::PutObjectRequest {
                            bucket: &bucket,
                            key: &key,
                            data: &req.body,
                            metadata: &metadata_blob,
                            cond: &cond,
                        },
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
                // partNumber takes precedence over Range header (AWS behavior)
                if let Some(pn_str) = req.query_param("partNumber") {
                    let part_number: u32 =
                        pn_str.parse().map_err(|_| ServerError::InvalidArgument {
                            reason: "partNumber must be a positive integer".into(),
                        })?;
                    if part_number == 0 {
                        return Err(ServerError::InvalidArgument {
                            reason: "partNumber must be >= 1".into(),
                        });
                    }
                    let result = self
                        .coordinator
                        .get_object_part(&crate::coordinator::GetObjectPartRequest {
                            bucket: &bucket,
                            key: &key,
                            version_id: vid,
                            part_number,
                            cond: &cond,
                        })
                        .map_err(|e| match e {
                            ServerError::InvalidPart { .. } => {
                                ServerError::InvalidRange { total_size: 0 }
                            }
                            other => other,
                        })?;
                    let tags = result.tags.clone();
                    let mut resp = S3Response::get_object_part(result);
                    apply_response_overrides(&mut resp, req);
                    if let Some(tags_xml) = tags {
                        let count = xml::count_tags_in_xml(&tags_xml);
                        if count > 0 {
                            resp.headers
                                .push(("x-amz-tagging-count".to_string(), count.to_string()));
                        }
                    }
                    Ok(resp)
                } else if let Some(range_header) = req.header("range") {
                    let byte_range = crate::range::ByteRange::parse(range_header)?;
                    match self
                        .coordinator
                        .get_object_range(&crate::coordinator::GetObjectRangeRequest {
                            bucket: &bucket,
                            key: &key,
                            version_id: vid,
                            range: byte_range,
                            cond: &cond,
                        })
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
                    let result = self.coordinator.get_object(
                        &crate::coordinator::GetObjectRequest {
                            bucket: &bucket,
                            key: &key,
                            version_id: vid,
                            cond: &cond,
                        },
                    )?;
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
                let cond = delete_condition_from_headers(req)?;
                let vid = parse_version_id(req)?;
                let result = self.coordinator.delete_object(
                    &crate::coordinator::DeleteObjectRequest {
                        bucket: &bucket,
                        key: &key,
                        version_id: vid,
                        cond: &cond,
                    },
                )?;
                Ok(S3Response::delete_object(&result))
            }
            S3Operation::HeadObject { bucket, key } => {
                self.authorize_bucket_read(auth, &bucket)?;
                let cond = read_condition_from_headers(req);
                let vid = parse_version_id(req)?;
                if let Some(pn_str) = req.query_param("partNumber") {
                    let part_number: u32 =
                        pn_str.parse().map_err(|_| ServerError::InvalidArgument {
                            reason: "partNumber must be a positive integer".into(),
                        })?;
                    if part_number == 0 {
                        return Err(ServerError::InvalidArgument {
                            reason: "partNumber must be >= 1".into(),
                        });
                    }
                    let result = self
                        .coordinator
                        .head_object_part(&crate::coordinator::GetObjectPartRequest {
                            bucket: &bucket,
                            key: &key,
                            version_id: vid,
                            part_number,
                            cond: &cond,
                        })
                        .map_err(|e| match e {
                            ServerError::InvalidPart { .. } => {
                                ServerError::InvalidRange { total_size: 0 }
                            }
                            other => other,
                        })?;
                    let mut resp = S3Response::head_object_part(&result);
                    if let Some(tags_xml) = &result.tags {
                        let count = xml::count_tags_in_xml(tags_xml);
                        if count > 0 {
                            resp.headers
                                .push(("x-amz-tagging-count".to_string(), count.to_string()));
                        }
                    }
                    Ok(resp)
                } else {
                    let result = self.coordinator.head_object(
                        &crate::coordinator::GetObjectRequest {
                            bucket: &bucket,
                            key: &key,
                            version_id: vid,
                            cond: &cond,
                        },
                    )?;
                    let checksum_mode = req.header("x-amz-checksum-mode");
                    let mut resp = S3Response::head_object(&result, checksum_mode);
                    if let Some(tags_xml) = &result.tags {
                        let count = xml::count_tags_in_xml(tags_xml);
                        if count > 0 {
                            resp.headers
                                .push(("x-amz-tagging-count".to_string(), count.to_string()));
                        }
                    }
                    Ok(resp)
                }
            }
            S3Operation::GetObjectAttributes { bucket, key } => {
                self.authorize_bucket_read(auth, &bucket)?;
                // Parse x-amz-object-attributes header (required, comma-separated).
                // The AWS Rust SDK may send one header per list element; accept both
                // repeated headers and comma-delimited header values.
                let attr_values: Vec<&str> = req
                    .headers
                    .iter()
                    .filter(|(k, _)| k == "x-amz-object-attributes")
                    .map(|(_, v)| v.as_str())
                    .collect();
                if attr_values.is_empty() {
                    return Err(ServerError::InvalidArgument {
                        reason: "missing required header: x-amz-object-attributes".to_string(),
                    });
                }
                let requested: Vec<&str> = attr_values
                    .into_iter()
                    .flat_map(|value| value.split(','))
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                    .collect();
                if requested.is_empty() {
                    return Err(ServerError::InvalidArgument {
                        reason: "missing required header: x-amz-object-attributes".to_string(),
                    });
                }
                for &attr in &requested {
                    if !xml::is_valid_object_attribute(attr) {
                        return Err(ServerError::InvalidArgument {
                            reason: format!("invalid object attribute: {attr}"),
                        });
                    }
                }
                let want_parts = requested.contains(&"ObjectParts");
                let max_parts: u32 = match req.headers.iter().find(|(k, _)| k == "x-amz-max-parts")
                {
                    None => 1000,
                    Some((_, v)) => v.parse().map_err(|_| ServerError::InvalidArgument {
                        reason: "invalid x-amz-max-parts".to_string(),
                    })?,
                };
                let part_number_marker: Option<u32> = req
                    .headers
                    .iter()
                    .find(|(k, _)| k == "x-amz-part-number-marker")
                    .map(|(_, v)| {
                        v.parse().map_err(|_| ServerError::InvalidArgument {
                            reason: "x-amz-part-number-marker must be an integer".to_string(),
                        })
                    })
                    .transpose()?;

                let cond = read_condition_from_headers(req);
                let vid = parse_version_id(req)?;
                let result = self.coordinator.get_object_attributes(
                    &crate::coordinator::GetObjectAttributesRequest {
                        bucket: &bucket,
                        key: &key,
                        version_id: vid,
                        cond: &cond,
                        want_parts,
                        part_number_marker,
                        max_parts,
                    },
                )?;
                let checksum_entries: Vec<(&str, &str)> = result
                    .metadata
                    .checksum_entries_with_type()
                    .map(|e| (e.key.as_str(), e.value.as_str()))
                    .collect();
                // Extract checksum algorithm from metadata for per-part checksum XML elements.
                let obj_checksum_algo = result
                    .metadata
                    .get("x-amz-checksum-algorithm")
                    .and_then(ChecksumAlgorithm::parse);
                let body_xml = xml::get_object_attributes_xml(
                    &requested,
                    &result.etag,
                    result.size,
                    &checksum_entries,
                    result.object_parts.as_ref(),
                    obj_checksum_algo,
                );
                Ok(S3Response::get_object_attributes(
                    &body_xml,
                    result.last_modified,
                    result.version_id,
                ))
            }
            S3Operation::DeleteObjects { bucket } => {
                self.authorize_bucket_write(auth, &bucket)?;
                let (xml_entries, quiet) = xml::parse_delete_objects_xml(&req.body)?;
                let cond = delete_condition_from_headers(req)?;
                let entries: Vec<crate::coordinator::DeleteEntry> = xml_entries
                    .iter()
                    .map(|e| {
                        let version_id = e.version_id.as_deref()
                            .map(parse_version_id_str)
                            .transpose()?;
                        Ok(crate::coordinator::DeleteEntry {
                            key: &e.key,
                            version_id,
                        })
                    })
                    .collect::<Result<_, ServerError>>()?;
                let result = self.coordinator.delete_objects(
                    &crate::coordinator::DeleteObjectsRequest {
                        bucket: &bucket,
                        entries: &entries,
                        cond: &cond,
                    },
                )?;
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
            S3Operation::PutBucketOwnershipControls { bucket } => {
                self.authorize_bucket_write(auth, &bucket)?;
                let value = xml::parse_ownership_controls_xml(&req.body)?;
                if value == "BucketOwnerEnforced" {
                    let info = self.coordinator.head_bucket(&bucket)?;
                    if info.public_read {
                        return Err(ServerError::InvalidBucketAclWithObjectOwnership);
                    }
                }
                let config_xml = xml::get_ownership_controls_xml(&value);
                self.coordinator
                    .put_bucket_ownership_controls(&bucket, &config_xml)?;
                Ok(S3Response::put_bucket_ownership_controls())
            }
            S3Operation::GetBucketOwnershipControls { bucket } => {
                self.authorize_bucket_write(auth, &bucket)?;
                match self.coordinator.get_bucket_ownership_controls(&bucket)? {
                    Some(config_xml) => Ok(S3Response::get_bucket_ownership_controls(&config_xml)),
                    None => Err(ServerError::OwnershipControlsNotFound {
                        bucket: bucket.clone(),
                    }),
                }
            }
            S3Operation::DeleteBucketOwnershipControls { bucket } => {
                self.authorize_bucket_write(auth, &bucket)?;
                self.coordinator.delete_bucket_ownership_controls(&bucket)?;
                Ok(S3Response::delete_bucket_ownership_controls())
            }
            S3Operation::PutBucketAcl { bucket } => {
                self.authorize_bucket_write(auth, &bucket)?;
                let acl = parse_bucket_acl(req)?;
                match acl {
                    BucketAcl::Private => {
                        // Enforce BucketOwnerEnforced — all PutBucketAcl calls rejected
                        if let Some(ref oc_xml) =
                            self.coordinator.get_bucket_ownership_controls(&bucket)?
                        {
                            if let Ok(val) = xml::parse_ownership_controls_xml(oc_xml.as_bytes()) {
                                if val == "BucketOwnerEnforced" {
                                    return Err(ServerError::AccessControlListNotSupported);
                                }
                            }
                        }
                        self.coordinator.put_bucket_acl(&bucket, false)?;
                    }
                    BucketAcl::PublicRead => {
                        // Enforce BucketOwnerEnforced
                        if let Some(ref oc_xml) =
                            self.coordinator.get_bucket_ownership_controls(&bucket)?
                        {
                            if let Ok(val) = xml::parse_ownership_controls_xml(oc_xml.as_bytes()) {
                                if val == "BucketOwnerEnforced" {
                                    return Err(ServerError::AccessControlListNotSupported);
                                }
                            }
                        }
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
                        // Enforce BucketOwnerEnforced
                        if let Some(ref oc_xml) =
                            self.coordinator.get_bucket_ownership_controls(&bucket)?
                        {
                            if let Ok(val) = xml::parse_ownership_controls_xml(oc_xml.as_bytes()) {
                                if val == "BucketOwnerEnforced" {
                                    return Err(ServerError::AccessControlListNotSupported);
                                }
                            }
                        }
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
            S3Operation::CreateMultipartUpload { bucket, key } => {
                self.authorize_bucket_write(auth, &bucket)?;
                let header_pairs: Vec<(&str, &str)> = req
                    .headers
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_str()))
                    .collect();
                let metadata = MetadataBlob::from_headers(&header_pairs)?;

                // Parse optional checksum algorithm/type headers.
                let checksum_algorithm = match req.header("x-amz-checksum-algorithm") {
                    None => None,
                    Some(v) => Some(ChecksumAlgorithm::parse(v).ok_or_else(|| {
                        ServerError::InvalidArgument {
                            reason: format!("unsupported checksum algorithm: {v}"),
                        }
                    })?),
                };
                let checksum_type = match req.header("x-amz-checksum-type") {
                    None => None,
                    Some(v) => Some(ChecksumType::parse(v).ok_or_else(|| {
                        ServerError::InvalidArgument {
                            reason: format!("unsupported checksum type: {v}"),
                        }
                    })?),
                };

                // Validate: checksum-type without checksum-algorithm is invalid.
                if checksum_type.is_some() && checksum_algorithm.is_none() {
                    return Err(ServerError::InvalidArgument {
                        reason: "x-amz-checksum-type requires x-amz-checksum-algorithm".to_string(),
                    });
                }

                // Build validated config (rejects invalid algo+type combinations).
                let checksum = checksum_algorithm
                    .map(|algo| storage::MultipartChecksumConfig::new(algo, checksum_type))
                    .transpose()?;

                let result = self.coordinator.create_multipart_upload(
                    &crate::coordinator::CreateMultipartUploadRequest {
                        bucket: &bucket,
                        key: &key,
                        metadata: &metadata,
                        checksum,
                    },
                )?;
                Ok(S3Response::create_multipart_upload(
                    &bucket,
                    &key,
                    &result.upload_id,
                    checksum_algorithm,
                    checksum_type,
                ))
            }
            S3Operation::UploadPart { bucket, key } => {
                let upload_id =
                    req.query_param("uploadId")
                        .ok_or_else(|| ServerError::InvalidRequest {
                            reason: "missing uploadId query parameter".to_string(),
                        })?;
                let part_number: u32 = req
                    .query_param("partNumber")
                    .ok_or_else(|| ServerError::InvalidRequest {
                        reason: "missing partNumber query parameter".to_string(),
                    })?
                    .parse()
                    .map_err(|_| ServerError::InvalidArgument {
                        reason: "partNumber must be a positive integer".to_string(),
                    })?;

                if let Some(copy_source) = req.header("x-amz-copy-source") {
                    // UploadPartCopy path
                    let (src_bucket, src_key, src_version_id_str) =
                        request::parse_copy_source(copy_source)?;
                    let src_version_id = match src_version_id_str {
                        None => None,
                        Some(v) if v == "null" => Some(storage::VersionId::Null),
                        Some(v) => Some(storage::VersionId::from_u64(v.parse::<u64>().map_err(
                            |_| ServerError::InvalidArgument {
                                reason: format!("invalid versionId in copy source: {v}"),
                            },
                        )?)),
                    };
                    self.authorize_bucket_write(auth, &bucket)?;
                    self.authorize_bucket_read(auth, &src_bucket)?;
                    let src_cond = copy_source_condition_from_headers(req);
                    let copy_source_range =
                        if let Some(range_header) = req.header("x-amz-copy-source-range") {
                            Some(crate::range::parse_copy_source_range(range_header)?)
                        } else {
                            None
                        };
                    let result = self.coordinator.upload_part_copy(&UploadPartCopyRequest {
                        source: CopySource {
                            bucket: &src_bucket,
                            key: &src_key,
                            version_id: src_version_id,
                            condition: &src_cond,
                        },
                        dst_bucket: &bucket,
                        dst_key: &key,
                        upload_id: &upload_id,
                        part_number,
                        copy_source_range,
                    })?;
                    Ok(S3Response::upload_part_copy(
                        &result.etag,
                        result.last_modified,
                    ))
                } else {
                    // Normal UploadPart path
                    self.authorize_bucket_write(auth, &bucket)?;

                    // Extract claimed checksum from request headers (at most one).
                    let claimed_checksum = extract_checksum_header(req)?;

                    let result = self.coordinator.upload_part(
                        &crate::coordinator::UploadPartRequest {
                            bucket: &bucket,
                            key: &key,
                            upload_id: &upload_id,
                            part_number,
                            data: &req.body,
                            claimed_checksum: claimed_checksum.as_ref(),
                        },
                    )?;
                    Ok(S3Response::upload_part(
                        &result.etag,
                        result.checksum.as_ref(),
                    ))
                }
            }
            S3Operation::CompleteMultipartUpload { bucket, key } => {
                self.authorize_bucket_write(auth, &bucket)?;
                let upload_id =
                    req.query_param("uploadId")
                        .ok_or_else(|| ServerError::InvalidRequest {
                            reason: "missing uploadId query parameter".to_string(),
                        })?;
                let parts = xml::parse_complete_multipart_upload_xml(&req.body)?;
                // Extract object-level checksum claim from request headers as a raw
                // string. CompleteMultipartUpload checksums may be composite ("base64-N"),
                // so we cannot decode them as plain base64.
                let claimed_checksum = extract_checksum_header_raw(req)?;
                let claimed_ref = claimed_checksum
                    .as_ref()
                    .map(|(algo, val)| (*algo, val.as_str()));
                let result = self.coordinator.complete_multipart_upload(
                    &crate::coordinator::CompleteMultipartUploadRequest {
                        bucket: &bucket,
                        key: &key,
                        upload_id: &upload_id,
                        parts: &parts,
                        claimed_checksum: claimed_ref,
                    },
                )?;
                Ok(S3Response::complete_multipart_upload(
                    &bucket,
                    &key,
                    &result.etag,
                    result.version_id,
                    result.checksum_algorithm,
                    result.checksum_type,
                    result.checksum_value.as_deref(),
                ))
            }
            S3Operation::AbortMultipartUpload { bucket, key } => {
                self.authorize_bucket_write(auth, &bucket)?;
                let upload_id =
                    req.query_param("uploadId")
                        .ok_or_else(|| ServerError::InvalidRequest {
                            reason: "missing uploadId query parameter".to_string(),
                        })?;
                self.coordinator
                    .abort_multipart_upload(&crate::coordinator::AbortMultipartUploadRequest {
                        bucket: &bucket,
                        key: &key,
                        upload_id: &upload_id,
                    })?;
                Ok(S3Response::abort_multipart_upload())
            }
            S3Operation::ListMultipartUploads { bucket } => {
                self.authorize_bucket_read(auth, &bucket)?;
                let prefix = req.query_param("prefix");
                let key_marker = req.query_param("key-marker");
                let upload_id_marker = req.query_param("upload-id-marker");
                let max_uploads: u32 = match req.query_param("max-uploads") {
                    None => 1000,
                    Some(s) => s.parse().map_err(|_| ServerError::InvalidArgument {
                        reason: "invalid max-uploads".to_string(),
                    })?,
                };
                let result = self.coordinator.list_multipart_uploads(
                    &crate::coordinator::ListMultipartUploadsRequest {
                        bucket: &bucket,
                        prefix: prefix.as_deref(),
                        key_marker: key_marker.as_deref(),
                        upload_id_marker: upload_id_marker.as_deref(),
                        max_uploads,
                    },
                )?;
                Ok(S3Response::list_multipart_uploads(
                    &bucket,
                    prefix.as_deref(),
                    key_marker.as_deref(),
                    upload_id_marker.as_deref(),
                    max_uploads,
                    &result,
                ))
            }
            S3Operation::ListParts { bucket, key } => {
                self.authorize_bucket_read(auth, &bucket)?;
                let upload_id =
                    req.query_param("uploadId")
                        .ok_or_else(|| ServerError::InvalidRequest {
                            reason: "missing uploadId query parameter".to_string(),
                        })?;
                let part_number_marker: Option<u32> = req
                    .query_param("part-number-marker")
                    .map(|s| {
                        s.parse().map_err(|_| ServerError::InvalidArgument {
                            reason: "part-number-marker must be an integer".to_string(),
                        })
                    })
                    .transpose()?;
                let max_parts: u32 = match req.query_param("max-parts") {
                    None => 1000,
                    Some(s) => s.parse().map_err(|_| ServerError::InvalidArgument {
                        reason: "invalid max-parts".to_string(),
                    })?,
                };
                let result = self.coordinator.list_parts(
                    &crate::coordinator::ListPartsRequest {
                        bucket: &bucket,
                        key: &key,
                        upload_id: &upload_id,
                        part_number_marker,
                        max_parts,
                    },
                )?;
                Ok(S3Response::list_parts(
                    &bucket,
                    &key,
                    &upload_id,
                    part_number_marker,
                    max_parts,
                    &result,
                ))
            }
            // OptionsRequest is handled before auth in handle_s3_request
            S3Operation::OptionsRequest { .. } => {
                unreachable!("OPTIONS handled before dispatch")
            }
            S3Operation::ListObjectVersions { bucket } => {
                self.authorize_bucket_read(auth, &bucket)?;
                let prefix = req.query_param("prefix");
                let key_marker = req.query_param("key-marker");
                let version_id_marker = match req.query_param("version-id-marker") {
                    None => None,
                    Some(v) if v == "null" => Some(storage::VersionId::Null),
                    Some(v) => Some(storage::VersionId::from_u64(v.parse::<u64>().map_err(
                        |_| ServerError::InvalidArgument {
                            reason: format!("invalid version-id-marker: {v}"),
                        },
                    )?)),
                };
                let max_keys: u32 = req
                    .query_param("max-keys")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(1000);

                let result = self.coordinator.list_object_versions(
                    &crate::coordinator::ListObjectVersionsRequest {
                        bucket: &bucket,
                        prefix: prefix.as_deref(),
                        key_marker: key_marker.as_deref(),
                        version_id_marker,
                        max_keys,
                    },
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

        // Pre-auth time skew check for SigV4 requests: AWS rejects expired requests
        // before signature verification. Only applies to well-formed SigV4 auth headers.
        let is_sigv4 = req
            .header("authorization")
            .is_some_and(|h| h.starts_with("AWS4-HMAC-SHA256"));
        if is_sigv4 {
            if let Some(date_str) = req.header("x-amz-date") {
                if let Some(epoch) = auth::parse_amz_date(date_str) {
                    let skew = now.abs_diff(epoch);
                    if skew > 15 * 60 {
                        return Err(ServerError::Auth(auth::AuthError::RequestExpired));
                    }
                }
                // malformed date → fall through to auth which will handle it
            }
        }

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
                streaming: None,
            },
            // Missing x-amz-date when declared as signed → AccessDenied
            // AWS: "AWS authentication requires a valid Date or x-amz-date header"
            Err(auth::AuthError::MissingSignedHeader {
                header: "x-amz-date",
            }) => {
                return Err(ServerError::Auth(auth::AuthError::AccessDenied));
            }
            Err(err) => return Err(ServerError::Auth(err)),
        };

        // Verify payload integrity: if the client provided an actual content hash
        // (not UNSIGNED-PAYLOAD and not STREAMING-*), recompute and compare to
        // detect transit corruption. STREAMING-* bodies are verified by the
        // chunked decoder.
        if let Some(claimed) = req.header("x-amz-content-sha256") {
            if claimed != "UNSIGNED-PAYLOAD" && !claimed.starts_with("STREAMING-") {
                let actual = auth::canonical::sha256_hex(&req.body);
                if actual != claimed {
                    return Err(ServerError::XAmzContentSHA256Mismatch {
                        client_hash: claimed.to_string(),
                        server_hash: actual,
                    });
                }
            }
        }

        Ok(auth)
    }

    /// If the request uses aws-chunked encoding, decode the body and return
    /// a new S3Request with the decoded payload. Returns None for non-chunked requests.
    fn maybe_decode_chunked(
        &self,
        req: &S3Request,
        auth: &AuthContext,
    ) -> Result<Option<S3Request>, ServerError> {
        let content_sha = match req.header("x-amz-content-sha256") {
            Some(v) if v.starts_with("STREAMING-") => v,
            _ => return Ok(None),
        };

        // Whitelist allowed streaming tokens.
        match content_sha {
            "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
            | "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
            | "STREAMING-UNSIGNED-PAYLOAD-TRAILER" => {}
            _ => {
                return Err(ServerError::InvalidArgument {
                    reason: format!("unsupported streaming token: {}", content_sha),
                });
            }
        }

        // Require content-encoding contains aws-chunked.
        let has_aws_chunked = req
            .header("content-encoding")
            .map(|ce| {
                ce.split(',')
                    .any(|part| part.trim().eq_ignore_ascii_case("aws-chunked"))
            })
            .unwrap_or(false);
        if !has_aws_chunked {
            return Err(ServerError::MalformedTrailerError {
                reason: "content-encoding must contain aws-chunked for streaming uploads"
                    .to_string(),
            });
        }

        // Require x-amz-decoded-content-length.
        let expected_str = req
            .header("x-amz-decoded-content-length")
            .ok_or(ServerError::MissingContentLength)?;
        let expected_len =
            expected_str
                .parse::<usize>()
                .map_err(|_| ServerError::InvalidRequest {
                    reason: format!("invalid x-amz-decoded-content-length: {}", expected_str),
                })?;

        let is_trailer_mode = content_sha.ends_with("-TRAILER");

        let is_signed = content_sha.starts_with("STREAMING-AWS4-HMAC-SHA256");

        let streaming_ctx = if is_signed {
            Some(
                auth.streaming
                    .as_ref()
                    .ok_or_else(|| ServerError::Auth(auth::AuthError::SignatureMismatch))?,
            )
        } else {
            None
        };

        let decoded = chunked::decode_chunked_body(&req.body, streaming_ctx, is_trailer_mode)?;

        // Validate decoded length.
        if decoded.data.len() != expected_len {
            return Err(ServerError::MalformedChunkedBody {
                reason: format!(
                    "decoded content length mismatch: expected {}, got {}",
                    expected_len,
                    decoded.data.len()
                ),
            });
        }

        // Trailer declaration validation.
        // Content trailers = trailers excluding x-amz-trailer-signature.
        let content_trailers: Vec<&(String, String)> = decoded
            .trailers
            .iter()
            .filter(|(k, _)| k != "x-amz-trailer-signature")
            .collect();

        let declared_trailer = req.header("x-amz-trailer");

        if !is_trailer_mode && !content_trailers.is_empty() {
            return Err(ServerError::IncompleteBody);
        }

        if !content_trailers.is_empty() && declared_trailer.is_none() {
            return Err(ServerError::MalformedTrailerError {
                reason: "trailers present in body but x-amz-trailer header missing".to_string(),
            });
        }

        if let Some(declared) = declared_trailer {
            // Parse declared trailer names as comma-separated list.
            let declared_names: Vec<String> = declared
                .split(',')
                .map(|s| s.trim().to_ascii_lowercase())
                .filter(|s| !s.is_empty())
                .collect();

            if content_trailers.is_empty() {
                return Err(ServerError::MalformedTrailerError {
                    reason: format!(
                        "x-amz-trailer header declares {} but no trailers in body",
                        declared
                    ),
                });
            }
            // Check that all content trailers were declared.
            for (name, _) in &content_trailers {
                if !declared_names.iter().any(|d| d == name.as_str()) {
                    return Err(ServerError::MalformedTrailerError {
                        reason: format!(
                            "undeclared trailer in body: {} (declared: {})",
                            name, declared
                        ),
                    });
                }
            }
            // Check that all declared names appear in body (exact-set).
            let body_names: Vec<&str> = content_trailers.iter().map(|(k, _)| k.as_str()).collect();
            for name in &declared_names {
                if !body_names.contains(&name.as_str()) {
                    return Err(ServerError::MalformedTrailerError {
                        reason: format!(
                            "declared trailer missing from body: {} (declared: {})",
                            name, declared
                        ),
                    });
                }
            }
        }

        Ok(Some(req.with_decoded_body(decoded.data, decoded.trailers)))
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

        // Authenticate using SigV4 POST form fields only.
        let algo = form
            .field("x-amz-algorithm")
            .ok_or_else(|| ServerError::InvalidRequest {
                reason: "missing x-amz-algorithm".to_string(),
            })?;
        let post_auth = auth::authenticate_post_sigv4(
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
        let metadata_blob = MetadataBlob::from_headers(&hp_refs)?;

        // Call put_object (same as PUT)
        let cond = crate::conditional::WriteCondition::default();
        let result = self
            .coordinator
            .put_object(&crate::coordinator::PutObjectRequest {
                bucket,
                key: &key,
                data: &form.file_data,
                metadata: &metadata_blob,
                cond: &cond,
            })?;

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

    // ── Streaming write helpers ─────────────────────────────────────

    /// Prepare a streaming PutObject: authenticate, validate, begin session.
    ///
    /// Returns a context struct that the async streaming loop uses to drive
    /// chunk appends and finalization.
    pub fn prepare_streaming_put(
        &self,
        req: &S3Request,
        bucket: &str,
        key: &str,
    ) -> Result<StreamingPutContext, ServerError> {
        let auth = self.authenticate(req)?;
        self.authorize_bucket_write(&auth, bucket)?;

        // Enforce BucketOwnerEnforced ACL constraint.
        if let Some(acl_value) = req.header("x-amz-acl") {
            if let Some(ref oc_xml) = self.coordinator.get_bucket_ownership_controls(bucket)? {
                if let Ok(val) = xml::parse_ownership_controls_xml(oc_xml.as_bytes()) {
                    if val == "BucketOwnerEnforced"
                        && acl_value != "bucket-owner-full-control"
                        && acl_value != "private"
                    {
                        return Err(ServerError::AccessControlListNotSupported);
                    }
                }
            }
        }

        validate_checksum_headers(req, false)?;

        // Parse inline tags before starting the session.
        let inline_tags_xml = if let Some(tagging_header) = req.header("x-amz-tagging") {
            let tags = xml::parse_url_encoded_tags(tagging_header)?;
            if tags.is_empty() {
                None
            } else {
                Some(xml::get_tagging_xml(&tags))
            }
        } else {
            None
        };

        // Reject if both a trailing checksum (via x-amz-trailer) and an inline
        // checksum value header are present. AWS returns:
        //   InvalidRequest: Expecting a single x-amz-checksum- header
        let has_trailing_checksum = req
            .header("x-amz-trailer")
            .map(|v| {
                v.split(',')
                    .any(|name| checksum_algo_from_header(name.trim()).is_some())
            })
            .unwrap_or(false);

        if has_trailing_checksum {
            let checksum_value_headers: &[&str] = &[
                "x-amz-checksum-sha256",
                "x-amz-checksum-crc64nvme",
                "x-amz-checksum-crc32",
                "x-amz-checksum-crc32c",
                "x-amz-checksum-sha1",
            ];
            let has_inline_checksum = checksum_value_headers
                .iter()
                .any(|h| req.header(h).is_some());
            if has_inline_checksum {
                return Err(ServerError::InvalidRequest {
                    reason: "Expecting a single x-amz-checksum- header".to_string(),
                });
            }
        }

        let header_pairs: Vec<(&str, &str)> = req
            .headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let metadata_blob = crate::metadata_blob::MetadataBlob::from_headers(&header_pairs)?;
        let cond = write_condition_from_headers(req)?;

        // Collect checksum response headers to echo back in the response.
        let mut checksum_response: Vec<(String, String)> = Vec::new();
        for &(_, header) in CHECKSUM_HEADERS {
            if let Some(val) = req.header(header) {
                checksum_response.push((header.to_string(), val.to_string()));
            }
        }

        let session_id = self.coordinator.begin_stream_put(bucket, key)?;

        Ok(StreamingPutContext {
            session_id,
            bucket: bucket.to_string(),
            key: key.to_string(),
            metadata_blob,
            cond,
            inline_tags_xml,
            checksum_response,
            streaming_signing: auth.streaming,
        })
    }

    /// Append a chunk to a streaming session.
    pub fn streaming_append_chunk(
        &self,
        ctx: &StreamingPutContext,
        chunk_index: u32,
        data: &[u8],
    ) -> Result<(), ServerError> {
        self.coordinator.append_stream_chunk(
            &ctx.bucket,
            &ctx.key,
            &ctx.session_id,
            chunk_index,
            data,
        )
    }

    /// Finalize a streaming PutObject session and return an S3Response.
    ///
    /// `trailer_checksums` contains checksum headers extracted from aws-chunked
    /// trailers (e.g. `x-amz-checksum-crc32`). These are merged into the
    /// metadata blob for storage and echoed back in the response.
    pub fn finalize_streaming_put(
        &self,
        ctx: &StreamingPutContext,
        crc64: u64,
        total_size: u64,
        trailer_checksums: &[(String, String)],
    ) -> Result<S3Response, ServerError> {
        // Merge trailer checksums into metadata blob so they're persisted.
        // Trailer values override any matching initial header entries.
        let metadata_blob = if trailer_checksums.is_empty() {
            ctx.metadata_blob.clone()
        } else {
            let mut blob = ctx.metadata_blob.clone();
            for (k, v) in trailer_checksums {
                if let Some(entry) = blob.entries.iter_mut().find(|e| e.key == *k) {
                    entry.value = v.clone();
                } else {
                    blob.entries.push(crate::metadata_blob::MetadataEntry {
                        key: k.clone(),
                        value: v.clone(),
                    });
                }
            }
            blob
        };

        let result = self.coordinator.finalize_stream_put(&FinalizeStreamPutRequest {
            bucket: &ctx.bucket,
            key: &ctx.key,
            session_id: &ctx.session_id,
            crc64,
            total_size,
            metadata_blob: &metadata_blob,
            cond: &ctx.cond,
        })?;

        if let Some(ref tags_xml) = ctx.inline_tags_xml {
            self.coordinator.put_object_tags(
                &ctx.bucket,
                &ctx.key,
                Some(result.version_id),
                tags_xml,
            )?;
        }

        let mut resp = S3Response::put_object(&result);
        // Echo checksum headers. Trailer values override initial header values.
        for (name, value) in &ctx.checksum_response {
            if let Some((_, tv)) = trailer_checksums.iter().find(|(k, _)| k == name) {
                resp.headers.push((name.clone(), tv.clone()));
            } else {
                resp.headers.push((name.clone(), value.clone()));
            }
        }
        for (name, value) in trailer_checksums {
            if !ctx.checksum_response.iter().any(|(k, _)| k == name) {
                resp.headers.push((name.clone(), value.clone()));
            }
        }
        Ok(resp)
    }

    /// Abort a streaming session (best-effort cleanup).
    pub fn abort_streaming_put(&self, ctx: &StreamingPutContext) {
        let _ = self
            .coordinator
            .abort_stream_put(&ctx.bucket, &ctx.key, &ctx.session_id);
    }

    /// Prepare a streaming UploadPart session.
    pub fn prepare_streaming_part(
        &self,
        req: &S3Request,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u32,
    ) -> Result<StreamingPartContext, ServerError> {
        let auth = self.authenticate(req)?;
        self.authorize_bucket_write(&auth, bucket)?;

        let claimed_checksum = extract_checksum_header(req)?;

        let mut checksum_response: Vec<(String, String)> = Vec::new();
        for &(_, header) in CHECKSUM_HEADERS {
            if let Some(val) = req.header(header) {
                checksum_response.push((header.to_string(), val.to_string()));
            }
        }

        let begin = self
            .coordinator
            .begin_stream_part(bucket, key, upload_id, part_number)?;

        Ok(StreamingPartContext {
            session_id: begin.session_id,
            bucket: bucket.to_string(),
            key: key.to_string(),
            upload_id: upload_id.to_string(),
            part_number,
            upload_checksum_algorithm: begin.checksum_algorithm,
            claimed_checksum,
            checksum_response,
            streaming_signing: auth.streaming,
        })
    }

    /// Append a chunk to a streaming UploadPart session.
    pub fn streaming_append_part_chunk(
        &self,
        ctx: &StreamingPartContext,
        chunk_index: u32,
        data: &[u8],
    ) -> Result<(), ServerError> {
        self.coordinator.append_stream_chunk(
            &ctx.bucket,
            &ctx.key,
            &ctx.session_id,
            chunk_index,
            data,
        )
    }

    /// Finalize a streaming UploadPart session and return an S3Response.
    ///
    /// `trailer_checksums` contains checksum headers from aws-chunked trailers.
    /// `computed_checksum` is the incrementally computed checksum (algo, bytes).
    #[allow(clippy::too_many_arguments)]
    pub fn finalize_streaming_part(
        &self,
        ctx: &StreamingPartContext,
        crc64: u64,
        total_size: u64,
        trailer_checksums: &[(String, String)],
        computed_checksum: Option<storage::RawChecksum>,
    ) -> Result<S3Response, ServerError> {
        // If trailer checksums are present, use the first one as the claimed
        // checksum (overriding any from request headers). Trailing checksums
        // take precedence since they are computed after the body is sent.
        let trailer_claim = if let Some((k, v)) = trailer_checksums.first() {
            match checksum_algo_from_header(k) {
                Some(algo) => Some(ChecksumClaim::from_base64(algo, v)?),
                None => None,
            }
        } else {
            None
        };
        let effective_claim = trailer_claim.as_ref().or(ctx.claimed_checksum.as_ref());

        let result = self.coordinator.finalize_stream_part(FinalizeStreamPartRequest {
            bucket: &ctx.bucket,
            key: &ctx.key,
            session_id: &ctx.session_id,
            upload_id: &ctx.upload_id,
            part_number: ctx.part_number,
            crc64,
            total_size,
            claimed_checksum: effective_claim,
            computed_checksum,
        })?;

        let mut resp = S3Response::upload_part(&result.etag, result.checksum.as_ref());
        // The coordinator's result already includes the checksum via
        // S3Response::upload_part. Only echo headers NOT already present
        // (e.g. x-amz-checksum-type). Trailer values take precedence.
        let already_set: Option<&str> = result
            .checksum
            .as_ref()
            .map(|c| c.algorithm().header_name());
        for (name, value) in &ctx.checksum_response {
            if already_set == Some(name.as_str()) {
                continue; // Already set by S3Response::upload_part
            }
            if let Some((_, tv)) = trailer_checksums.iter().find(|(k, _)| k == name) {
                resp.headers.push((name.clone(), tv.clone()));
            } else {
                resp.headers.push((name.clone(), value.clone()));
            }
        }
        for (name, value) in trailer_checksums {
            if already_set == Some(name.as_str()) {
                continue; // Already set by S3Response::upload_part
            }
            if !ctx.checksum_response.iter().any(|(k, _)| k == name) {
                resp.headers.push((name.clone(), value.clone()));
            }
        }
        Ok(resp)
    }

    /// Abort a streaming UploadPart session (best-effort cleanup).
    pub fn abort_streaming_part(&self, ctx: &StreamingPartContext) {
        let _ = self
            .coordinator
            .abort_stream_put(&ctx.bucket, &ctx.key, &ctx.session_id);
    }
}

/// Context for an in-progress streaming PutObject.
///
/// Created by `prepare_streaming_put`, used across async/blocking boundaries.
pub struct StreamingPutContext {
    pub session_id: String,
    pub bucket: String,
    pub key: String,
    pub metadata_blob: crate::metadata_blob::MetadataBlob,
    pub cond: crate::conditional::WriteCondition,
    pub inline_tags_xml: Option<String>,
    pub checksum_response: Vec<(String, String)>,
    /// Signing context for aws-chunked modes, None for unsigned/plain.
    pub streaming_signing: Option<auth::StreamingSigningContext>,
}

/// Context for an in-progress streaming UploadPart.
///
/// Created by `prepare_streaming_part`, used across async/blocking boundaries.
pub struct StreamingPartContext {
    pub session_id: String,
    pub bucket: String,
    pub key: String,
    pub upload_id: String,
    pub part_number: u32,
    pub upload_checksum_algorithm: Option<ChecksumAlgorithm>,
    pub claimed_checksum: Option<ChecksumClaim>,
    pub checksum_response: Vec<(String, String)>,
    /// Signing context for aws-chunked modes, None for unsigned/plain.
    pub streaming_signing: Option<auth::StreamingSigningContext>,
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

/// Map a checksum header name (e.g. `x-amz-checksum-crc32`) to its
/// `ChecksumAlgorithm`. Returns `None` for unrecognized headers.
fn checksum_algo_from_header(header: &str) -> Option<ChecksumAlgorithm> {
    let lower = header.to_ascii_lowercase();
    for &(algo_name, h) in CHECKSUM_HEADERS {
        if h == lower {
            return ChecksumAlgorithm::parse(algo_name);
        }
    }
    None
}

/// Validate checksum headers on PutObject.
///
/// Enforces that at most one checksum header is present, validates base64
/// format/length, and if `x-amz-checksum-algorithm` is set it must match
/// the provided checksum header.
///
/// When `verify_body` is true, also computes the actual checksum from
/// `req.body` and returns `BadDigest` on mismatch. Pass `false` for
/// streaming paths where the body is not yet available.
fn validate_checksum_headers(req: &S3Request, verify_body: bool) -> Result<(), ServerError> {
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

            // Expected byte length for each algorithm.
            let expected_len = match algo {
                "SHA256" => 32,
                "SHA1" => 20,
                "CRC32" | "CRC32C" => 4,
                "CRC64NVME" => 8,
                _ => continue,
            };

            // Validate checksum value format (base64 decodes to correct length).
            let decoded_bytes = base64::engine::general_purpose::STANDARD
                .decode(claimed)
                .ok();
            match decoded_bytes {
                Some(ref bytes) if bytes.len() == expected_len => {}
                _ => {
                    return Err(ServerError::InvalidRequest {
                        reason: format!("Value for {} header is invalid.", header),
                    });
                }
            }

            if verify_body {
                let actual_b64 = match algo {
                    "SHA256" => {
                        let digest = ring::digest::digest(&ring::digest::SHA256, &req.body);
                        base64::engine::general_purpose::STANDARD.encode(digest.as_ref())
                    }
                    "SHA1" => {
                        let digest = ring::digest::digest(
                            &ring::digest::SHA1_FOR_LEGACY_USE_ONLY,
                            &req.body,
                        );
                        base64::engine::general_purpose::STANDARD.encode(digest.as_ref())
                    }
                    "CRC32" => {
                        let crc = checksum::crc32::checksum(&req.body);
                        base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes())
                    }
                    "CRC32C" => {
                        let crc = checksum::crc32c::checksum(&req.body);
                        base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes())
                    }
                    "CRC64NVME" => {
                        let crc = checksum::crc64::checksum(&req.body);
                        base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes())
                    }
                    _ => continue,
                };
                if claimed != actual_b64 {
                    return Err(ServerError::BadDigest);
                }
            }
        }
    }
    Ok(())
}

/// Count how many times a header name appears in the request.
fn header_count(req: &S3Request, name: &str) -> usize {
    req.headers.iter().filter(|(k, _)| k == name).count()
}

/// Extract a checksum header as a raw `(algorithm, value_string)` pair.
///
/// Shared validation for all checksum-header consumers. Rejects if:
/// - multiple distinct checksum value headers are present (e.g. crc32 + sha256)
/// - the same checksum header appears more than once
/// - `x-amz-checksum-algorithm` contradicts the value header's algorithm
fn extract_checksum_header_raw(
    req: &S3Request,
) -> Result<Option<(ChecksumAlgorithm, String)>, ServerError> {
    if header_count(req, "x-amz-checksum-algorithm") > 1 {
        return Err(ServerError::InvalidRequest {
            reason: "duplicate header: x-amz-checksum-algorithm".into(),
        });
    }
    let algo_header = req.header("x-amz-checksum-algorithm");
    let mut found: Option<(ChecksumAlgorithm, String)> = None;
    for &(algo_name, header) in CHECKSUM_HEADERS {
        if let Some(claimed) = req.header(header) {
            if found.is_some() {
                return Err(ServerError::InvalidRequest {
                    reason: "only one checksum header may be specified".into(),
                });
            }
            // Reject duplicate same-name headers (req.header returns only
            // the first, so a second with a different value would be silent).
            if header_count(req, header) > 1 {
                return Err(ServerError::InvalidRequest {
                    reason: format!("duplicate header: {header}"),
                });
            }
            // Cross-check x-amz-checksum-algorithm if present.
            if let Some(declared) = algo_header {
                if !declared.eq_ignore_ascii_case(algo_name) {
                    return Err(ServerError::InvalidRequest {
                        reason: format!(
                            "checksum algorithm mismatch: header says {} but got {}",
                            declared, algo_name
                        ),
                    });
                }
            }
            // CHECKSUM_HEADERS uses known-good algo names.
            let algo = ChecksumAlgorithm::parse(algo_name).unwrap();
            found = Some((algo, claimed.to_string()));
        }
    }
    Ok(found)
}

/// Extract a claimed checksum from request headers, decoded and validated.
///
/// Used by UploadPart and streaming paths where the value is always plain base64.
fn extract_checksum_header(
    req: &S3Request,
) -> Result<Option<ChecksumClaim>, ServerError> {
    match extract_checksum_header_raw(req)? {
        Some((algo, value)) => Ok(Some(ChecksumClaim::from_base64(algo, &value)?)),
        None => Ok(None),
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::Coordinator;
    use ec::EcConfig;
    use std::sync::Arc;
    use storage::SharedStorageNode;

    fn setup_frontend(dir: &std::path::Path) -> HttpFrontend {
        let pg_ids: Vec<u32> = (0..4).collect();
        let storage_node = Arc::new(SharedStorageNode::open(dir, &pg_ids).unwrap());
        let ec_config = EcConfig::new(4, 2).unwrap();
        let coordinator =
            Coordinator::new(storage_node, ec_config, "us-east-1".to_string()).unwrap();
        let credentials = auth::CredentialStore::new();
        HttpFrontend {
            coordinator,
            credentials,
        }
    }

    fn test_auth() -> auth::AuthContext {
        auth::AuthContext {
            mode: auth::AuthMode::HeaderSigV4,
            access_key_id: Some("AKID".to_string()),
            principal: Some("testuser".to_string()),
            request_epoch_secs: Some(0),
            streaming: None,
        }
    }

    fn make_req(query: &str) -> S3Request {
        S3Request {
            method: String::new(),
            path: String::new(),
            query_string: query.to_string(),
            headers: vec![],
            body: vec![],
        }
    }

    // ── UploadPart validation ────────────────────────────────────────

    #[test]
    fn upload_part_missing_upload_id() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

        let req = make_req("partNumber=1");
        let op = S3Operation::UploadPart {
            bucket: "mybucket".to_string(),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequest { .. }) => {}
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn upload_part_invalid_part_number() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

        let req = make_req("partNumber=abc&uploadId=xyz");
        let op = S3Operation::UploadPart {
            bucket: "mybucket".to_string(),
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
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

        let req = make_req("");
        let op = S3Operation::CompleteMultipartUpload {
            bucket: "mybucket".to_string(),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequest { .. }) => {}
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn complete_multipart_multiple_checksum_headers_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

        let upload_id = create_upload_with_checksum(&fe, "mybucket", "k", Some("CRC32"));
        let xml = "<CompleteMultipartUpload>\
               <Part><PartNumber>1</PartNumber><ETag>\"x\"</ETag></Part>\
             </CompleteMultipartUpload>"
            .to_string();
        let req = S3Request {
            method: String::new(),
            path: String::new(),
            query_string: format!("uploadId={upload_id}"),
            headers: vec![
                ("x-amz-checksum-crc32".to_string(), "AAAAAA==".to_string()),
                ("x-amz-checksum-sha256".to_string(), "BBBBBB==".to_string()),
            ],
            body: xml.into_bytes(),
        };
        let op = S3Operation::CompleteMultipartUpload {
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequest { .. }) => {}
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn complete_multipart_algorithm_header_contradicts_value_header() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

        let upload_id = create_upload_with_checksum(&fe, "mybucket", "k", Some("CRC32"));
        let xml = "<CompleteMultipartUpload>\
               <Part><PartNumber>1</PartNumber><ETag>\"x\"</ETag></Part>\
             </CompleteMultipartUpload>"
            .to_string();
        let req = S3Request {
            method: String::new(),
            path: String::new(),
            query_string: format!("uploadId={upload_id}"),
            headers: vec![
                // Algorithm header says SHA256 but value header is CRC32.
                ("x-amz-checksum-algorithm".to_string(), "SHA256".to_string()),
                ("x-amz-checksum-crc32".to_string(), "AAAAAA==".to_string()),
            ],
            body: xml.into_bytes(),
        };
        let op = S3Operation::CompleteMultipartUpload {
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequest { .. }) => {}
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn complete_multipart_duplicate_same_checksum_header_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

        let upload_id = create_upload_with_checksum(&fe, "mybucket", "k", Some("CRC32"));
        let xml = "<CompleteMultipartUpload>\
               <Part><PartNumber>1</PartNumber><ETag>\"x\"</ETag></Part>\
             </CompleteMultipartUpload>"
            .to_string();
        let req = S3Request {
            method: String::new(),
            path: String::new(),
            query_string: format!("uploadId={upload_id}"),
            headers: vec![
                ("x-amz-checksum-crc32".to_string(), "AAAAAA==".to_string()),
                ("x-amz-checksum-crc32".to_string(), "BBBBBB==".to_string()),
            ],
            body: xml.into_bytes(),
        };
        let op = S3Operation::CompleteMultipartUpload {
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequest { .. }) => {}
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn complete_multipart_duplicate_checksum_algorithm_header_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

        let upload_id = create_upload_with_checksum(&fe, "mybucket", "k", Some("CRC32"));
        let xml = "<CompleteMultipartUpload>\
               <Part><PartNumber>1</PartNumber><ETag>\"x\"</ETag></Part>\
             </CompleteMultipartUpload>"
            .to_string();
        let req = S3Request {
            method: String::new(),
            path: String::new(),
            query_string: format!("uploadId={upload_id}"),
            headers: vec![
                ("x-amz-checksum-algorithm".to_string(), "CRC32".to_string()),
                ("x-amz-checksum-algorithm".to_string(), "SHA256".to_string()),
                ("x-amz-checksum-crc32".to_string(), "AAAAAA==".to_string()),
            ],
            body: xml.into_bytes(),
        };
        let op = S3Operation::CompleteMultipartUpload {
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequest { .. }) => {}
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn complete_multipart_checksum_algo_mismatch_upload_rejected() {
        // Upload created with CRC32 but complete sends SHA256 checksum header.
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

        let upload_id = create_upload_with_checksum(&fe, "mybucket", "k", Some("CRC32"));
        // Upload a part so complete has something to work with.
        let req = S3Request {
            method: String::new(),
            path: String::new(),
            query_string: format!("partNumber=1&uploadId={upload_id}"),
            headers: vec![],
            body: vec![0u8; 1024],
        };
        let op = S3Operation::UploadPart {
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        let etag = resp
            .headers
            .iter()
            .find(|(k, _)| k == "ETag")
            .map(|(_, v)| v.clone())
            .unwrap();

        let xml = format!(
            "<CompleteMultipartUpload>\
               <Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part>\
             </CompleteMultipartUpload>"
        );
        let req = S3Request {
            method: String::new(),
            path: String::new(),
            query_string: format!("uploadId={upload_id}"),
            headers: vec![("x-amz-checksum-sha256".to_string(), "AAAAAA==".to_string())],
            body: xml.into_bytes(),
        };
        let op = S3Operation::CompleteMultipartUpload {
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequest { .. }) => {}
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    // ── AbortMultipartUpload validation ──────────────────────────────

    #[test]
    fn abort_multipart_missing_upload_id() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

        let req = make_req("");
        let op = S3Operation::AbortMultipartUpload {
            bucket: "mybucket".to_string(),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequest { .. }) => {}
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    // ── ListParts validation ─────────────────────────────────────────

    #[test]
    fn list_parts_missing_upload_id() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

        let req = make_req("");
        let op = S3Operation::ListParts {
            bucket: "mybucket".to_string(),
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
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

        let req = make_req("uploadId=abc&part-number-marker=xyz");
        let op = S3Operation::ListParts {
            bucket: "mybucket".to_string(),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgument { .. }) => {}
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn list_parts_invalid_max_parts() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

        let req = make_req("uploadId=abc&max-parts=notanumber");
        let op = S3Operation::ListParts {
            bucket: "mybucket".to_string(),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgument { .. }) => {}
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    // ── GetObjectAttributes header validation ──────────────────────

    #[test]
    fn get_object_attributes_invalid_max_parts() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

        let req = S3Request {
            method: String::new(),
            path: String::new(),
            query_string: String::new(),
            headers: vec![
                (
                    "x-amz-object-attributes".to_string(),
                    "ObjectParts".to_string(),
                ),
                ("x-amz-max-parts".to_string(), "notanumber".to_string()),
            ],
            body: vec![],
        };
        let op = S3Operation::GetObjectAttributes {
            bucket: "mybucket".to_string(),
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
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

        let req = S3Request {
            method: String::new(),
            path: String::new(),
            query_string: String::new(),
            headers: vec![
                (
                    "x-amz-object-attributes".to_string(),
                    "ObjectParts".to_string(),
                ),
                ("x-amz-part-number-marker".to_string(), "xyz".to_string()),
            ],
            body: vec![],
        };
        let op = S3Operation::GetObjectAttributes {
            bucket: "mybucket".to_string(),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgument { .. }) => {}
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    // ── End-to-end multipart upload flow ────────────────────────────

    #[test]
    fn multipart_upload_e2e_quoted_etags() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

        // 1. CreateMultipartUpload
        let req = make_req("uploads");
        let op = S3Operation::CreateMultipartUpload {
            bucket: "mybucket".to_string(),
            key: "mykey".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);
        let body = std::str::from_utf8(&resp.body).unwrap();
        // Extract upload_id from <UploadId>...</UploadId>
        let uid_start = body.find("<UploadId>").unwrap() + "<UploadId>".len();
        let uid_end = uid_start + body[uid_start..].find("</UploadId>").unwrap();
        let upload_id = &body[uid_start..uid_end];
        assert!(!upload_id.is_empty());

        // 2. UploadPart — single part (last part is exempt from min-size)
        let part_body = vec![0u8; 1024];
        let req = S3Request {
            method: String::new(),
            path: String::new(),
            query_string: format!("partNumber=1&uploadId={upload_id}"),
            headers: vec![],
            body: part_body,
        };
        let op = S3Operation::UploadPart {
            bucket: "mybucket".to_string(),
            key: "mykey".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);
        let etag = resp
            .headers
            .iter()
            .find(|(k, _)| k == "ETag")
            .map(|(_, v)| v.clone())
            .expect("UploadPart response must have ETag header");
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
        let req = S3Request {
            method: String::new(),
            path: String::new(),
            query_string: format!("uploadId={upload_id}"),
            headers: vec![],
            body: complete_xml.into_bytes(),
        };
        let op = S3Operation::CompleteMultipartUpload {
            bucket: "mybucket".to_string(),
            key: "mykey".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);
        let body = std::str::from_utf8(&resp.body).unwrap();
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
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

        let req = make_req("uploads&max-uploads=abc");
        let op = S3Operation::ListMultipartUploads {
            bucket: "mybucket".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgument { .. }) => {}
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    // ── CreateMultipartUpload checksum validation ───────────────────

    #[test]
    fn create_multipart_invalid_checksum_algorithm() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

        let req = S3Request {
            method: String::new(),
            path: String::new(),
            query_string: "uploads".to_string(),
            headers: vec![("x-amz-checksum-algorithm".to_string(), "BOGUS".to_string())],
            body: vec![],
        };
        let op = S3Operation::CreateMultipartUpload {
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgument { .. }) => {}
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn create_multipart_invalid_checksum_type() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

        let req = S3Request {
            method: String::new(),
            path: String::new(),
            query_string: "uploads".to_string(),
            headers: vec![
                ("x-amz-checksum-algorithm".to_string(), "CRC32".to_string()),
                ("x-amz-checksum-type".to_string(), "INVALID".to_string()),
            ],
            body: vec![],
        };
        let op = S3Operation::CreateMultipartUpload {
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgument { .. }) => {}
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn create_multipart_checksum_type_without_algorithm() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

        let req = S3Request {
            method: String::new(),
            path: String::new(),
            query_string: "uploads".to_string(),
            headers: vec![("x-amz-checksum-type".to_string(), "COMPOSITE".to_string())],
            body: vec![],
        };
        let op = S3Operation::CreateMultipartUpload {
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgument { .. }) => {}
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn create_multipart_sha_full_object_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

        let req = S3Request {
            method: String::new(),
            path: String::new(),
            query_string: "uploads".to_string(),
            headers: vec![
                ("x-amz-checksum-algorithm".to_string(), "SHA256".to_string()),
                ("x-amz-checksum-type".to_string(), "FULL_OBJECT".to_string()),
            ],
            body: vec![],
        };
        let op = S3Operation::CreateMultipartUpload {
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgument { .. }) => {}
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn create_multipart_with_checksum_returns_fields_in_xml() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

        let req = S3Request {
            method: String::new(),
            path: String::new(),
            query_string: "uploads".to_string(),
            headers: vec![
                ("x-amz-checksum-algorithm".to_string(), "CRC32".to_string()),
                ("x-amz-checksum-type".to_string(), "FULL_OBJECT".to_string()),
            ],
            body: vec![],
        };
        let op = S3Operation::CreateMultipartUpload {
            bucket: "mybucket".to_string(),
            key: "mykey".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);
        let body = std::str::from_utf8(&resp.body).unwrap();
        assert!(
            body.contains("<ChecksumAlgorithm>CRC32</ChecksumAlgorithm>"),
            "missing ChecksumAlgorithm: {body}"
        );
        assert!(
            body.contains("<ChecksumType>FULL_OBJECT</ChecksumType>"),
            "missing ChecksumType: {body}"
        );
    }

    #[test]
    fn create_multipart_crc32_composite_accepted() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

        let req = S3Request {
            method: String::new(),
            path: String::new(),
            query_string: "uploads".to_string(),
            headers: vec![
                ("x-amz-checksum-algorithm".to_string(), "CRC32".to_string()),
                ("x-amz-checksum-type".to_string(), "COMPOSITE".to_string()),
            ],
            body: vec![],
        };
        let op = S3Operation::CreateMultipartUpload {
            bucket: "mybucket".to_string(),
            key: "mykey".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);
        let body = std::str::from_utf8(&resp.body).unwrap();
        assert!(
            body.contains("<ChecksumAlgorithm>CRC32</ChecksumAlgorithm>"),
            "missing ChecksumAlgorithm: {body}"
        );
        assert!(
            body.contains("<ChecksumType>COMPOSITE</ChecksumType>"),
            "missing ChecksumType: {body}"
        );
    }

    #[test]
    fn create_multipart_algorithm_only_defaults_type() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

        let req = S3Request {
            method: String::new(),
            path: String::new(),
            query_string: "uploads".to_string(),
            headers: vec![("x-amz-checksum-algorithm".to_string(), "SHA256".to_string())],
            body: vec![],
        };
        let op = S3Operation::CreateMultipartUpload {
            bucket: "mybucket".to_string(),
            key: "mykey".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);
        let body = std::str::from_utf8(&resp.body).unwrap();
        assert!(
            body.contains("<ChecksumAlgorithm>SHA256</ChecksumAlgorithm>"),
            "missing ChecksumAlgorithm: {body}"
        );
        // When no type specified, no ChecksumType element emitted.
        assert!(
            !body.contains("ChecksumType"),
            "unexpected ChecksumType: {body}"
        );
    }

    // ── UploadPart checksum validation ──────────────────────────────

    /// Helper: create a multipart upload with optional checksum algorithm, return upload_id.
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
        let req = S3Request {
            method: String::new(),
            path: String::new(),
            query_string: "uploads".to_string(),
            headers,
            body: vec![],
        };
        let op = S3Operation::CreateMultipartUpload {
            bucket: bucket.to_string(),
            key: key.to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        let body = std::str::from_utf8(&resp.body).unwrap();
        let start = body.find("<UploadId>").unwrap() + "<UploadId>".len();
        let end = start + body[start..].find("</UploadId>").unwrap();
        body[start..end].to_string()
    }

    #[test]
    fn upload_part_bad_digest() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

        let upload_id = create_upload_with_checksum(&fe, "mybucket", "k", Some("CRC32"));
        let req = S3Request {
            method: String::new(),
            path: String::new(),
            query_string: format!("partNumber=1&uploadId={upload_id}"),
            headers: vec![(
                "x-amz-checksum-crc32".to_string(),
                "AAAAAAAA".to_string(), // wrong checksum
            )],
            body: vec![1, 2, 3, 4],
        };
        let op = S3Operation::UploadPart {
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequest { .. }) => {}
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn upload_part_multiple_checksum_headers_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

        let upload_id = create_upload_with_checksum(&fe, "mybucket", "k", Some("CRC32"));
        let req = S3Request {
            method: String::new(),
            path: String::new(),
            query_string: format!("partNumber=1&uploadId={upload_id}"),
            headers: vec![
                ("x-amz-checksum-crc32".to_string(), "AAAAAA==".to_string()),
                ("x-amz-checksum-sha256".to_string(), "BBBBBB==".to_string()),
            ],
            body: vec![1, 2, 3, 4],
        };
        let op = S3Operation::UploadPart {
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequest { .. }) => {}
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn upload_part_algorithm_mismatch_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

        // Upload configured with CRC32 but part sends SHA256 checksum.
        let upload_id = create_upload_with_checksum(&fe, "mybucket", "k", Some("CRC32"));
        let req = S3Request {
            method: String::new(),
            path: String::new(),
            query_string: format!("partNumber=1&uploadId={upload_id}"),
            headers: vec![("x-amz-checksum-sha256".to_string(), "AAAA".to_string())],
            body: vec![1, 2, 3, 4],
        };
        let op = S3Operation::UploadPart {
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequest { .. }) => {}
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn upload_part_correct_checksum_returns_header() {
        use base64::Engine;

        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

        let upload_id = create_upload_with_checksum(&fe, "mybucket", "k", Some("CRC32"));
        let data = b"hello world";
        let crc = checksum::crc32::checksum(data);
        let crc_b64 = base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes());

        let req = S3Request {
            method: String::new(),
            path: String::new(),
            query_string: format!("partNumber=1&uploadId={upload_id}"),
            headers: vec![("x-amz-checksum-crc32".to_string(), crc_b64.clone())],
            body: data.to_vec(),
        };
        let op = S3Operation::UploadPart {
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);

        // Response should include the checksum header.
        let resp_crc = resp
            .headers
            .iter()
            .find(|(k, _)| k == "x-amz-checksum-crc32")
            .map(|(_, v)| v.clone());
        assert_eq!(resp_crc.as_deref(), Some(crc_b64.as_str()));
    }

    #[test]
    fn upload_part_no_header_upload_algo_computes_checksum() {
        // Upload has checksum algorithm but part doesn't send a header.
        // Coordinator should compute the checksum from data.
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

        let upload_id = create_upload_with_checksum(&fe, "mybucket", "k", Some("CRC32"));
        let data = b"test data";

        let req = S3Request {
            method: String::new(),
            path: String::new(),
            query_string: format!("partNumber=1&uploadId={upload_id}"),
            headers: vec![],
            body: data.to_vec(),
        };
        let op = S3Operation::UploadPart {
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);

        // Response should include the computed checksum header.
        let resp_crc = resp
            .headers
            .iter()
            .find(|(k, _)| k == "x-amz-checksum-crc32");
        assert!(resp_crc.is_some(), "missing checksum header in response");
    }

    #[test]
    fn upload_part_reupload_preserves_latest_checksum() {
        use base64::Engine;

        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

        let upload_id = create_upload_with_checksum(&fe, "mybucket", "k", Some("CRC32"));

        // Upload part 1 with data "aaa".
        let data1 = b"aaa";
        let crc1 = checksum::crc32::checksum(data1);
        let crc1_b64 = base64::engine::general_purpose::STANDARD.encode(crc1.to_be_bytes());
        let req = S3Request {
            method: String::new(),
            path: String::new(),
            query_string: format!("partNumber=1&uploadId={upload_id}"),
            headers: vec![("x-amz-checksum-crc32".to_string(), crc1_b64)],
            body: data1.to_vec(),
        };
        let op = S3Operation::UploadPart {
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        fe.dispatch_routed(&req, &test_auth(), op).unwrap();

        // Re-upload part 1 with different data "bbb".
        let data2 = b"bbb";
        let crc2 = checksum::crc32::checksum(data2);
        let crc2_b64 = base64::engine::general_purpose::STANDARD.encode(crc2.to_be_bytes());
        let req = S3Request {
            method: String::new(),
            path: String::new(),
            query_string: format!("partNumber=1&uploadId={upload_id}"),
            headers: vec![("x-amz-checksum-crc32".to_string(), crc2_b64.clone())],
            body: data2.to_vec(),
        };
        let op = S3Operation::UploadPart {
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);

        // Response should have the NEW checksum, not the old one.
        let resp_crc = resp
            .headers
            .iter()
            .find(|(k, _)| k == "x-amz-checksum-crc32")
            .map(|(_, v)| v.clone())
            .expect("missing checksum header");
        assert_eq!(resp_crc, crc2_b64);
    }

    #[test]
    fn upload_part_checksum_accepted_when_upload_has_no_algorithm() {
        use base64::Engine;
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

        // Upload created without checksum algorithm.
        // AWS SDK v2+ sends CRC32 by default — it should be accepted and verified.
        let upload_id = create_upload_with_checksum(&fe, "mybucket", "k", None);
        let data = vec![1u8, 2, 3, 4];
        let correct_crc = base64::engine::general_purpose::STANDARD
            .encode(checksum::crc32::checksum(&data).to_be_bytes());
        let req = S3Request {
            method: String::new(),
            path: String::new(),
            query_string: format!("partNumber=1&uploadId={upload_id}"),
            headers: vec![("x-amz-checksum-crc32".to_string(), correct_crc)],
            body: data,
        };
        let op = S3Operation::UploadPart {
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);
    }

    #[test]
    fn upload_part_algorithm_header_contradicts_value_header() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

        let upload_id = create_upload_with_checksum(&fe, "mybucket", "k", Some("CRC32"));
        let req = S3Request {
            method: String::new(),
            path: String::new(),
            query_string: format!("partNumber=1&uploadId={upload_id}"),
            headers: vec![
                // Algorithm header says SHA256 but value header is CRC32.
                ("x-amz-checksum-algorithm".to_string(), "SHA256".to_string()),
                ("x-amz-checksum-crc32".to_string(), "AAAAAA==".to_string()),
            ],
            body: vec![1, 2, 3, 4],
        };
        let op = S3Operation::UploadPart {
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequest { .. }) => {}
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn upload_part_algorithm_header_only_no_value_header() {
        // x-amz-checksum-algorithm without a value header is fine —
        // treated as no claimed checksum; coordinator computes from upload config.
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

        let upload_id = create_upload_with_checksum(&fe, "mybucket", "k", Some("CRC32"));
        let req = S3Request {
            method: String::new(),
            path: String::new(),
            query_string: format!("partNumber=1&uploadId={upload_id}"),
            headers: vec![("x-amz-checksum-algorithm".to_string(), "CRC32".to_string())],
            body: vec![1, 2, 3, 4],
        };
        let op = S3Operation::UploadPart {
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);

        // Checksum should still be computed and returned from the upload config.
        let has_crc = resp
            .headers
            .iter()
            .any(|(k, _)| k == "x-amz-checksum-crc32");
        assert!(has_crc, "expected checksum header in response");
    }

    #[test]
    fn upload_part_duplicate_checksum_algorithm_header_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

        let upload_id = create_upload_with_checksum(&fe, "mybucket", "k", Some("CRC32"));
        let req = S3Request {
            method: String::new(),
            path: String::new(),
            query_string: format!("partNumber=1&uploadId={upload_id}"),
            headers: vec![
                ("x-amz-checksum-algorithm".to_string(), "CRC32".to_string()),
                ("x-amz-checksum-algorithm".to_string(), "SHA256".to_string()),
                ("x-amz-checksum-crc32".to_string(), "AAAAAA==".to_string()),
            ],
            body: vec![1, 2, 3, 4],
        };
        let op = S3Operation::UploadPart {
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequest { .. }) => {}
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    // ── GET ?partNumber=N tests ─────────────────────────────────────

    /// Helper: do a full multipart upload through the HTTP frontend.
    /// When `algo` is set, computes and includes per-part checksums.
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
        let mut part_info: Vec<(u32, String, Option<String>)> = Vec::new();
        for (part_number, data) in parts {
            let mut headers = Vec::new();
            let mut checksum_b64 = None;
            if let Some(a) = algo {
                let algo_enum = storage::ChecksumAlgorithm::parse(a).unwrap();
                let raw: Vec<u8> = match algo_enum {
                    storage::ChecksumAlgorithm::Crc32 => {
                        checksum::crc32::checksum(data).to_be_bytes().to_vec()
                    }
                    storage::ChecksumAlgorithm::Crc32c => {
                        checksum::crc32c::checksum(data).to_be_bytes().to_vec()
                    }
                    _ => unimplemented!("test only supports CRC32/CRC32C"),
                };
                let encoded = b64.encode(&raw);
                headers.push((algo_enum.header_name().to_string(), encoded.clone()));
                checksum_b64 = Some(encoded);
            }
            let req = S3Request {
                method: String::new(),
                path: String::new(),
                query_string: format!("partNumber={part_number}&uploadId={upload_id}"),
                headers,
                body: data.clone(),
            };
            let op = S3Operation::UploadPart {
                bucket: bucket.to_string(),
                key: key.to_string(),
            };
            let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
            let etag = resp
                .headers
                .iter()
                .find(|(k, _)| k == "ETag")
                .map(|(_, v)| v.clone())
                .unwrap();
            part_info.push((*part_number, etag, checksum_b64));
        }
        let mut xml_parts = String::new();
        for (pn, etag, cksum) in &part_info {
            xml_parts.push_str(&format!(
                "<Part><PartNumber>{pn}</PartNumber><ETag>{etag}</ETag>"
            ));
            if let (Some(a), Some(val)) = (algo, cksum) {
                let algo_enum = storage::ChecksumAlgorithm::parse(a).unwrap();
                let elem = algo_enum.xml_element_name();
                xml_parts.push_str(&format!("<{elem}>{val}</{elem}>"));
            }
            xml_parts.push_str("</Part>");
        }
        let xml = format!("<CompleteMultipartUpload>{xml_parts}</CompleteMultipartUpload>");
        let req = S3Request {
            method: String::new(),
            path: String::new(),
            query_string: format!("uploadId={upload_id}"),
            headers: vec![],
            body: xml.into_bytes(),
        };
        let op = S3Operation::CompleteMultipartUpload {
            bucket: bucket.to_string(),
            key: key.to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);
    }

    #[test]
    fn get_object_part_multipart() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

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
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 206);

        // Verify data
        assert_eq!(resp.body, part2);

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
    }

    #[test]
    fn get_object_part_invalid_zero() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

        // PUT a simple object
        let req = S3Request {
            method: String::new(),
            path: String::new(),
            query_string: String::new(),
            headers: vec![],
            body: b"hello".to_vec(),
        };
        let op = S3Operation::PutObject {
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        fe.dispatch_routed(&req, &test_auth(), op).unwrap();

        // partNumber=0 → InvalidArgument
        let req = make_req("partNumber=0");
        let op = S3Operation::GetObject {
            bucket: "mybucket".to_string(),
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
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

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

        // partNumber=99 on a 3-part object → 416 InvalidRange
        let req = make_req("partNumber=99");
        let op = S3Operation::GetObject {
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRange { .. }) => {}
            Err(e) => panic!("expected InvalidRange, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn get_object_part_non_multipart() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

        let data = b"hello world";
        let req = S3Request {
            method: String::new(),
            path: String::new(),
            query_string: String::new(),
            headers: vec![],
            body: data.to_vec(),
        };
        let op = S3Operation::PutObject {
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        fe.dispatch_routed(&req, &test_auth(), op).unwrap();

        // partNumber=1 on inline object → 206 with full data
        let req = make_req("partNumber=1");
        let op = S3Operation::GetObject {
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 206);
        assert_eq!(resp.body, data);

        let parts_count = resp
            .headers
            .iter()
            .find(|(k, _)| k == "x-amz-mp-parts-count")
            .map(|(_, v)| v.as_str())
            .unwrap();
        assert_eq!(parts_count, "1");

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
    }

    #[test]
    fn get_object_part_non_multipart_out_of_range() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

        let req = S3Request {
            method: String::new(),
            path: String::new(),
            query_string: String::new(),
            headers: vec![],
            body: b"hello".to_vec(),
        };
        let op = S3Operation::PutObject {
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        fe.dispatch_routed(&req, &test_auth(), op).unwrap();

        // partNumber=2 on non-multipart → 416 InvalidRange
        let req = make_req("partNumber=2");
        let op = S3Operation::GetObject {
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRange { .. }) => {}
            Err(e) => panic!("expected InvalidRange, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn get_object_part_with_checksum() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

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
            bucket: "mybucket".to_string(),
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
    fn signed_streaming_without_context_returns_signature_mismatch() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());

        // Auth context claims signed streaming but has no streaming context.
        let auth = auth::AuthContext {
            mode: auth::AuthMode::HeaderSigV4,
            access_key_id: Some("AKID".to_string()),
            principal: Some("testuser".to_string()),
            request_epoch_secs: Some(0),
            streaming: None, // missing!
        };

        let req = S3Request {
            method: "PUT".to_string(),
            path: "/mybucket/key".to_string(),
            query_string: String::new(),
            headers: vec![
                (
                    "x-amz-content-sha256".to_string(),
                    "STREAMING-AWS4-HMAC-SHA256-PAYLOAD".to_string(),
                ),
                ("content-encoding".to_string(), "aws-chunked".to_string()),
                ("x-amz-decoded-content-length".to_string(), "5".to_string()),
            ],
            body: b"5\r\nhello\r\n0\r\n\r\n".to_vec(),
        };

        match fe.maybe_decode_chunked(&req, &auth) {
            Err(ServerError::Auth(auth::AuthError::SignatureMismatch)) => {} // expected
            other => panic!("expected SignatureMismatch, got {:?}", other.err()),
        }
    }

    #[test]
    fn non_numeric_decoded_content_length_returns_400() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());

        let req = S3Request {
            method: "PUT".to_string(),
            path: "/mybucket/key".to_string(),
            query_string: String::new(),
            headers: vec![
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
            body: b"5\r\nhello\r\n0\r\nx-amz-checksum-crc32:AAAA\r\n\r\n".to_vec(),
        };

        match fe.maybe_decode_chunked(&req, &test_auth()) {
            Err(ServerError::InvalidRequest { .. }) => {} // expected
            other => panic!("expected InvalidRequest, got {:?}", other.err()),
        }
    }

    #[test]
    fn ecdsa_streaming_token_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());

        let req = S3Request {
            method: "PUT".to_string(),
            path: "/mybucket/key".to_string(),
            query_string: String::new(),
            headers: vec![
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
            body: b"5\r\nhello\r\n0\r\nx-amz-checksum-crc32:AAAA\r\n\r\n".to_vec(),
        };

        match fe.maybe_decode_chunked(&req, &test_auth()) {
            Err(ServerError::InvalidArgument { .. }) => {} // expected
            other => panic!("expected InvalidArgument, got {:?}", other.err()),
        }
    }

    // ── DeleteObjects version-id validation ────────────────────────────

    #[test]
    fn delete_objects_invalid_version_id_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

        let xml = br#"<?xml version="1.0"?>
<Delete>
  <Object><Key>key1</Key><VersionId>not-a-number</VersionId></Object>
</Delete>"#;
        let req = S3Request {
            method: "POST".to_string(),
            path: "/mybucket".to_string(),
            query_string: "delete".to_string(),
            headers: vec![],
            body: xml.to_vec(),
        };
        let op = S3Operation::DeleteObjects {
            bucket: "mybucket".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgument { .. }) => {}
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn delete_objects_null_version_id_accepted() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        fe.coordinator
            .create_bucket_for_owner("testuser", "mybucket", false)
            .unwrap();

        let xml = br#"<?xml version="1.0"?>
<Delete>
  <Object><Key>key1</Key><VersionId>null</VersionId></Object>
</Delete>"#;
        let req = S3Request {
            method: "POST".to_string(),
            path: "/mybucket".to_string(),
            query_string: "delete".to_string(),
            headers: vec![],
            body: xml.to_vec(),
        };
        let op = S3Operation::DeleteObjects {
            bucket: "mybucket".to_string(),
        };
        // "null" is a valid version ID — should not error on parsing
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Ok(_) => {}
            Err(e) => panic!("expected Ok, got {e:?}"),
        }
    }
}
