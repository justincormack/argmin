/// HTTP frontend: parses requests, authenticates, dispatches to coordinator.
pub mod request;
pub mod response;
pub mod router;
pub mod xml;

use auth::{parse_auth_header, verify_request, CredentialStore};
use tiny_http::{Header, Response, StatusCode};

use crate::coordinator::Coordinator;
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
        let (s3req, request) = S3Request::from_http(request);
        let result = self.dispatch(&s3req);

        match result {
            Ok(resp) => self.send_response(request, resp),
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
            S3Operation::ListObjectsV2 { bucket } => {
                let prefix = req.query_param("prefix");
                let delimiter = req.query_param("delimiter");
                let continuation_token = req.query_param("continuation-token");
                let max_keys: u32 = req
                    .query_param("max-keys")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(1000);

                let result = self.coordinator.list_objects_v2(
                    &bucket,
                    prefix.as_deref(),
                    delimiter.as_deref(),
                    continuation_token.as_deref(),
                    max_keys,
                )?;
                Ok(S3Response::list_objects_v2(
                    &bucket,
                    prefix.as_deref(),
                    delimiter.as_deref(),
                    max_keys,
                    &result,
                ))
            }
            S3Operation::PutObject { bucket, key } => {
                let header_pairs: Vec<(&str, &str)> = req
                    .headers
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_str()))
                    .collect();
                let result =
                    self.coordinator
                        .put_object(&bucket, &key, &req.body, &header_pairs)?;
                Ok(S3Response::put_object(&result))
            }
            S3Operation::GetObject { bucket, key } => {
                let result = self.coordinator.get_object(&bucket, &key)?;
                Ok(S3Response::get_object(result))
            }
            S3Operation::DeleteObject { bucket, key } => {
                self.coordinator.delete_object(&bucket, &key)?;
                Ok(S3Response::delete_object())
            }
            S3Operation::HeadObject { bucket, key } => {
                let result = self.coordinator.head_object(&bucket, &key)?;
                Ok(S3Response::head_object(&result))
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

        Ok(())
    }

    fn send_response(&self, request: tiny_http::Request, resp: S3Response) {
        let mut response = Response::from_data(resp.body)
            .with_status_code(StatusCode(resp.status_code));

        for (name, value) in &resp.headers {
            if let Ok(header) = Header::from_bytes(name.as_bytes(), value.as_bytes()) {
                response.add_header(header);
            }
        }

        let _ = request.respond(response);
    }
}
