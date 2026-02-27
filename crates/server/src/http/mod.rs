/// HTTP frontend: parses requests, authenticates, dispatches to coordinator.
pub mod request;
pub mod response;
pub mod router;
pub mod xml;

use std::time::{SystemTime, UNIX_EPOCH};

use auth::{parse_amz_date, parse_auth_header, verify_request, CredentialStore};
use tiny_http::{Header, Response, StatusCode};

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

/// The HTTP frontend that handles incoming requests.
pub struct HttpFrontend {
    pub coordinator: Coordinator,
    pub credentials: CredentialStore,
}

impl HttpFrontend {
    /// Handle a single HTTP request.
    pub fn handle_request(&self, request: tiny_http::Request) {
        let mut request = request;
        let s3req = match S3Request::from_http(&mut request) {
            Ok(req) => req,
            Err(err) => {
                let resp = S3Response::error(&err, "");
                self.send_response(request, resp);
                return;
            }
        };
        let result = self.dispatch(&s3req);

        match result {
            Ok(resp) => self.send_response(request, resp),
            Err(ServerError::NotModified {
                ref etag,
                last_modified,
            }) => {
                let resp = S3Response::not_modified(etag, last_modified);
                self.send_response(request, resp);
            }
            Err(ServerError::PreconditionFailed) => {
                let resp = S3Response::precondition_failed();
                self.send_response(request, resp);
            }
            Err(err) => {
                let resp = S3Response::error(&err, &s3req.path);
                self.send_response(request, resp);
            }
        }
    }

    fn dispatch(&self, req: &S3Request) -> Result<S3Response, ServerError> {
        // Authenticate
        self.authenticate(req)?;

        // Route
        let operation = route(&req.method, &req.path, &req.query_string)?;

        // Dispatch to coordinator
        match operation {
            S3Operation::ListBuckets => {
                let buckets = self.coordinator.list_buckets()?;
                Ok(S3Response::list_buckets(&buckets))
            }
            S3Operation::CreateBucket { bucket } => {
                self.coordinator.create_bucket(&bucket)?;
                Ok(S3Response::create_bucket(&bucket))
            }
            S3Operation::DeleteBucket { bucket } => {
                self.coordinator.delete_bucket(&bucket)?;
                Ok(S3Response::delete_bucket())
            }
            S3Operation::HeadBucket { bucket } => {
                let info = self.coordinator.head_bucket(&bucket)?;
                Ok(S3Response::head_bucket(&info))
            }
            S3Operation::ListObjectsV1 { bucket } => {
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
                    let (src_bucket, src_key) = request::parse_copy_source(copy_source)?;
                    let src_cond = copy_source_condition_from_headers(req);
                    let dst_cond = write_condition_from_headers(req);
                    let directive = match req.header("x-amz-metadata-directive") {
                        Some(d) if d.eq_ignore_ascii_case("REPLACE") => MetadataDirective::Replace,
                        _ => MetadataDirective::Copy,
                    };
                    let header_pairs: Vec<(&str, &str)> = req
                        .headers
                        .iter()
                        .map(|(k, v)| (k.as_str(), v.as_str()))
                        .collect();
                    let result = self.coordinator.copy_object(
                        &src_bucket,
                        &src_key,
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
                let cond = read_condition_from_headers(req);
                if let Some(range_header) = req.header("range") {
                    let byte_range = crate::range::ByteRange::parse(range_header)?;
                    match self
                        .coordinator
                        .get_object_range(&bucket, &key, byte_range, &cond)
                    {
                        Ok(result) => Ok(S3Response::get_object_range(result)),
                        Err(ServerError::InvalidRange { total_size }) => {
                            Ok(S3Response::range_not_satisfiable(total_size))
                        }
                        Err(e) => Err(e),
                    }
                } else {
                    let result = self.coordinator.get_object(&bucket, &key, &cond)?;
                    Ok(S3Response::get_object(result))
                }
            }
            S3Operation::DeleteObject { bucket, key } => {
                let cond = delete_condition_from_headers(req);
                self.coordinator.delete_object(&bucket, &key, &cond)?;
                Ok(S3Response::delete_object())
            }
            S3Operation::HeadObject { bucket, key } => {
                let cond = read_condition_from_headers(req);
                let result = self.coordinator.head_object(&bucket, &key, &cond)?;
                Ok(S3Response::head_object(&result))
            }
            S3Operation::DeleteObjects { bucket } => {
                let (entries, quiet) = xml::parse_delete_objects_xml(&req.body)?;
                let cond = delete_condition_from_headers(req);
                let result = self.coordinator.delete_objects(&bucket, &entries, &cond)?;
                Ok(S3Response::delete_objects(&result, quiet))
            }
            S3Operation::ListObjectVersions { bucket } => {
                let prefix = req.query_param("prefix");
                let key_marker = req.query_param("key-marker");
                let max_keys: u32 = req
                    .query_param("max-keys")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(1000);

                let result = self.coordinator.list_objects_v2(
                    &bucket,
                    prefix.as_deref(),
                    None,
                    key_marker.as_deref(),
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

    fn authenticate(&self, req: &S3Request) -> Result<(), ServerError> {
        let auth_header = req
            .header("authorization")
            .ok_or(ServerError::Auth(auth::AuthError::MissingAuth))?;

        let auth = parse_auth_header(auth_header)?;
        let body_hash = req.body_hash();
        let header_pairs = req.header_pairs();

        verify_request(
            &req.method,
            &req.path,
            &req.query_string,
            &header_pairs,
            &body_hash,
            &auth,
            &self.credentials,
        )?;

        // Enforce ±15 minute time skew on x-amz-date to prevent replay attacks.
        // Reject malformed timestamps — skipping the check would weaken replay protection.
        if let Some(amz_date) = req.header("x-amz-date") {
            let request_epoch = parse_amz_date(amz_date).ok_or(ServerError::InvalidRequest {
                reason: "malformed x-amz-date timestamp".to_string(),
            })?;
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
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

        Ok(())
    }

    fn send_response(&self, request: tiny_http::Request, resp: S3Response) {
        let mut response =
            Response::from_data(resp.body).with_status_code(StatusCode(resp.status_code));

        for (name, value) in &resp.headers {
            if let Ok(header) = Header::from_bytes(name.as_bytes(), value.as_bytes()) {
                response.add_header(header);
            }
        }

        let _ = request.respond(response);
    }
}

fn parse_max_keys(raw: Option<String>) -> Result<u32, ServerError> {
    match raw {
        None => Ok(1000),
        Some(s) => s.parse::<u32>().map_err(|_| ServerError::InvalidArgument {
            reason: "invalid max-keys".to_string(),
        }),
    }
}
