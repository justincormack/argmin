/// S3 operation routing from HTTP method + path + query.
use crate::error::ServerError;
use crate::http::request::{query_has_key, query_has_param};
use storage::{BucketName, ObjectKey};

/// Recognized S3 operations.
#[derive(Debug, PartialEq, Eq)]
pub enum S3Operation {
    ListBuckets,
    CreateBucket {
        bucket: BucketName,
    },
    DeleteBucket {
        bucket: BucketName,
    },
    HeadBucket {
        bucket: BucketName,
    },
    GetBucketLocation {
        bucket: BucketName,
    },
    ListObjectsV1 {
        bucket: BucketName,
    },
    ListObjectsV2 {
        bucket: BucketName,
    },
    PutObject {
        bucket: BucketName,
        key: String,
    },
    GetObject {
        bucket: BucketName,
        key: String,
    },
    DeleteObject {
        bucket: BucketName,
        key: String,
    },
    HeadObject {
        bucket: BucketName,
        key: String,
    },
    PostObject {
        bucket: BucketName,
    },
    DeleteObjects {
        bucket: BucketName,
    },
    ListObjectVersions {
        bucket: BucketName,
    },
    PutBucketVersioning {
        bucket: BucketName,
    },
    GetBucketVersioning {
        bucket: BucketName,
    },
    PutBucketObjectLockConfiguration {
        bucket: BucketName,
    },
    GetBucketObjectLockConfiguration {
        bucket: BucketName,
    },
    PutBucketEncryption {
        bucket: BucketName,
    },
    GetBucketEncryption {
        bucket: BucketName,
    },
    DeleteBucketEncryption {
        bucket: BucketName,
    },
    PutBucketCors {
        bucket: BucketName,
    },
    GetBucketCors {
        bucket: BucketName,
    },
    DeleteBucketCors {
        bucket: BucketName,
    },
    PutBucketTagging {
        bucket: BucketName,
    },
    GetBucketTagging {
        bucket: BucketName,
    },
    DeleteBucketTagging {
        bucket: BucketName,
    },
    PutBucketAbac {
        bucket: BucketName,
    },
    GetBucketAbac {
        bucket: BucketName,
    },
    PutBucketLifecycle {
        bucket: BucketName,
    },
    GetBucketLifecycle {
        bucket: BucketName,
    },
    DeleteBucketLifecycle {
        bucket: BucketName,
    },
    PutObjectTagging {
        bucket: BucketName,
        key: String,
    },
    GetObjectTagging {
        bucket: BucketName,
        key: String,
    },
    DeleteObjectTagging {
        bucket: BucketName,
        key: String,
    },
    PutObjectRetention {
        bucket: BucketName,
        key: String,
    },
    GetObjectRetention {
        bucket: BucketName,
        key: String,
    },
    PutObjectLegalHold {
        bucket: BucketName,
        key: String,
    },
    GetObjectLegalHold {
        bucket: BucketName,
        key: String,
    },
    PutObjectAcl {
        bucket: BucketName,
        key: String,
    },
    GetObjectAcl {
        bucket: BucketName,
        key: String,
    },
    PutBucketPublicAccessBlock {
        bucket: BucketName,
    },
    GetBucketPublicAccessBlock {
        bucket: BucketName,
    },
    DeleteBucketPublicAccessBlock {
        bucket: BucketName,
    },
    GetBucketAcl {
        bucket: BucketName,
    },
    PutBucketAcl {
        bucket: BucketName,
    },
    PutBucketOwnershipControls {
        bucket: BucketName,
    },
    GetBucketOwnershipControls {
        bucket: BucketName,
    },
    DeleteBucketOwnershipControls {
        bucket: BucketName,
    },
    GetObjectAttributes {
        bucket: BucketName,
        key: String,
    },
    CreateMultipartUpload {
        bucket: BucketName,
        key: String,
    },
    UploadPart {
        bucket: BucketName,
        key: String,
    },
    CompleteMultipartUpload {
        bucket: BucketName,
        key: String,
    },
    AbortMultipartUpload {
        bucket: BucketName,
        key: String,
    },
    ListMultipartUploads {
        bucket: BucketName,
    },
    ListParts {
        bucket: BucketName,
        key: String,
    },
    PutBucketPolicy {
        bucket: BucketName,
    },
    GetBucketPolicy {
        bucket: BucketName,
    },
    GetBucketPolicyStatus {
        bucket: BucketName,
    },
    DeleteBucketPolicy {
        bucket: BucketName,
    },
    OptionsRequest {
        bucket: BucketName,
        key: Option<String>,
    },
}

impl S3Operation {
    /// The object key for object-scoped operations. `None` for bucket-scoped
    /// and account-scoped operations.
    pub fn object_key(&self) -> Option<&str> {
        match self {
            Self::PutObject { key, .. }
            | Self::GetObject { key, .. }
            | Self::DeleteObject { key, .. }
            | Self::HeadObject { key, .. }
            | Self::GetObjectAcl { key, .. }
            | Self::PutObjectAcl { key, .. }
            | Self::GetObjectAttributes { key, .. }
            | Self::GetObjectTagging { key, .. }
            | Self::PutObjectTagging { key, .. }
            | Self::DeleteObjectTagging { key, .. }
            | Self::GetObjectRetention { key, .. }
            | Self::PutObjectRetention { key, .. }
            | Self::GetObjectLegalHold { key, .. }
            | Self::PutObjectLegalHold { key, .. }
            | Self::CreateMultipartUpload { key, .. }
            | Self::UploadPart { key, .. }
            | Self::CompleteMultipartUpload { key, .. }
            | Self::AbortMultipartUpload { key, .. }
            | Self::ListParts { key, .. } => Some(key),
            Self::OptionsRequest { key, .. } => key.as_deref(),
            _ => None,
        }
    }

    pub fn bucket_name(&self) -> Option<&BucketName> {
        match self {
            Self::ListBuckets => None,
            Self::CreateBucket { bucket }
            | Self::DeleteBucket { bucket }
            | Self::HeadBucket { bucket }
            | Self::GetBucketLocation { bucket }
            | Self::ListObjectsV1 { bucket }
            | Self::ListObjectsV2 { bucket }
            | Self::PostObject { bucket }
            | Self::DeleteObjects { bucket }
            | Self::ListObjectVersions { bucket }
            | Self::PutBucketVersioning { bucket }
            | Self::GetBucketVersioning { bucket }
            | Self::PutBucketObjectLockConfiguration { bucket }
            | Self::GetBucketObjectLockConfiguration { bucket }
            | Self::PutBucketEncryption { bucket }
            | Self::GetBucketEncryption { bucket }
            | Self::DeleteBucketEncryption { bucket }
            | Self::PutBucketCors { bucket }
            | Self::GetBucketCors { bucket }
            | Self::DeleteBucketCors { bucket }
            | Self::PutBucketTagging { bucket }
            | Self::GetBucketTagging { bucket }
            | Self::DeleteBucketTagging { bucket }
            | Self::PutBucketAbac { bucket }
            | Self::GetBucketAbac { bucket }
            | Self::PutBucketLifecycle { bucket }
            | Self::GetBucketLifecycle { bucket }
            | Self::DeleteBucketLifecycle { bucket }
            | Self::PutBucketPublicAccessBlock { bucket }
            | Self::GetBucketPublicAccessBlock { bucket }
            | Self::DeleteBucketPublicAccessBlock { bucket }
            | Self::GetBucketAcl { bucket }
            | Self::PutBucketAcl { bucket }
            | Self::PutBucketOwnershipControls { bucket }
            | Self::GetBucketOwnershipControls { bucket }
            | Self::DeleteBucketOwnershipControls { bucket }
            | Self::ListMultipartUploads { bucket }
            | Self::PutBucketPolicy { bucket }
            | Self::GetBucketPolicy { bucket }
            | Self::GetBucketPolicyStatus { bucket }
            | Self::DeleteBucketPolicy { bucket }
            | Self::OptionsRequest { bucket, .. } => Some(bucket),
            Self::PutObject { bucket, .. }
            | Self::GetObject { bucket, .. }
            | Self::DeleteObject { bucket, .. }
            | Self::HeadObject { bucket, .. }
            | Self::PutObjectTagging { bucket, .. }
            | Self::GetObjectTagging { bucket, .. }
            | Self::DeleteObjectTagging { bucket, .. }
            | Self::PutObjectRetention { bucket, .. }
            | Self::GetObjectRetention { bucket, .. }
            | Self::PutObjectLegalHold { bucket, .. }
            | Self::GetObjectLegalHold { bucket, .. }
            | Self::PutObjectAcl { bucket, .. }
            | Self::GetObjectAcl { bucket, .. }
            | Self::GetObjectAttributes { bucket, .. }
            | Self::CreateMultipartUpload { bucket, .. }
            | Self::UploadPart { bucket, .. }
            | Self::CompleteMultipartUpload { bucket, .. }
            | Self::AbortMultipartUpload { bucket, .. }
            | Self::ListParts { bucket, .. } => Some(bucket),
        }
    }
}

/// Trusted local endpoint configuration. This value is selected by the
/// listener entry point, never from `Host`, SNI, or another request field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EndpointKind {
    /// Ordinary S3 only. This is the only endpoint kind available over HTTP.
    S3Only,
    /// One TLS listener serving S3 and the initial S3 Control surface.
    SharedRegional,
    /// Regional STS Query API endpoint. It is selected by listener
    /// configuration and cannot be reached by changing request authority.
    StsOnly,
}

/// Service selected from the trusted endpoint and request target.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ServiceKind {
    S3,
    S3Control,
}

impl ServiceKind {
    #[must_use]
    pub const fn signing_service(self) -> auth::SigningService {
        match self {
            Self::S3 => auth::SigningService::S3,
            Self::S3Control => auth::SigningService::S3Control,
        }
    }

    #[must_use]
    pub fn canonical_signing_path(self, path: &str) -> String {
        match self {
            Self::S3 => path.to_string(),
            Self::S3Control => path
                .replacen("/v20180820/tags%2F", "/v20180820/tags/", 1)
                .replacen("/v20180820/tags%2f", "/v20180820/tags/", 1),
        }
    }
}

/// Recognized S3 Control operations on the versioned tag-resource path.
#[derive(Debug, PartialEq, Eq)]
pub enum S3ControlOperation {
    ListTagsForResource { bucket: BucketName },
    TagResource { bucket: BucketName },
    UntagResource { bucket: BucketName },
    HeadBucketTags,
    MethodNotAllowed { method: String },
    Options,
}

/// S3 Control routing failures selected before service authentication.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum S3ControlRouteError {
    InvalidUri { uri: String },
    EmptyBadRequest,
    FrontendBadRequest,
}

/// Routing failures retain the endpoint service family needed for rendering.
#[derive(Debug)]
pub(crate) enum ServiceRouteError {
    S3(ServerError),
    S3Control(S3ControlRouteError),
}

/// Service-level operation selected before authentication.
#[derive(Debug, PartialEq, Eq)]
pub enum ServiceOperation {
    S3(S3Operation),
    S3Control(S3ControlOperation),
}

impl ServiceOperation {
    #[must_use]
    pub(crate) const fn service_kind(&self) -> ServiceKind {
        match self {
            Self::S3(_) => ServiceKind::S3,
            Self::S3Control(_) => ServiceKind::S3Control,
        }
    }

    #[must_use]
    pub const fn s3(&self) -> Option<&S3Operation> {
        match self {
            Self::S3(operation) => Some(operation),
            Self::S3Control(_) => None,
        }
    }
}

fn s3_control_resource_path(path: &str) -> Option<&str> {
    let trimmed = path.strip_prefix('/').unwrap_or(path);
    trimmed
        .strip_prefix("v20180820/tags/")
        .or_else(|| trimmed.strip_prefix("v20180820/tags%2F"))
        .or_else(|| trimmed.strip_prefix("v20180820/tags%2f"))
}

fn has_malformed_percent_triplet(value: &str) -> bool {
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'%' {
            index += 1;
            continue;
        }
        if index + 2 >= bytes.len()
            || !bytes[index + 1].is_ascii_hexdigit()
            || !bytes[index + 2].is_ascii_hexdigit()
        {
            return true;
        }
        index += 3;
    }
    false
}

fn is_s3_control_candidate_path(path: &str) -> bool {
    let path = path.strip_prefix('/').unwrap_or(path);
    let path = path.strip_prefix('/').unwrap_or(path);
    let mut components = path.split('/');
    let Some(version) = components.next() else {
        return false;
    };
    let Some(resource_family) = components.next() else {
        return version == "v20180820" && path.ends_with("/tags");
    };
    let version_shape = version.len() == 9
        && matches!(version.as_bytes().first(), Some(b'v' | b'V'))
        && version.as_bytes()[1..].iter().all(u8::is_ascii_digit);
    version_shape && resource_family.starts_with("tag")
}

fn invalid_s3_control_uri(path: &str, decoded_resource: Option<&str>) -> String {
    if let Some(decoded_resource) = decoded_resource {
        if path.starts_with("/v20180820/tags/")
            || path.starts_with("/v20180820/tags%2F")
            || path.starts_with("/v20180820/tags%2f")
        {
            return format!("tags/{decoded_resource}");
        }
    }
    path.to_string()
}

fn parse_s3_control_bucket_resource(
    path: &str,
    resource_path: &str,
) -> Result<BucketName, S3ControlRouteError> {
    if has_malformed_percent_triplet(path) {
        return Err(S3ControlRouteError::EmptyBadRequest);
    }
    let resource_arn =
        crate::http::request::percent_decode_strict(resource_path).map_err(|_| {
            S3ControlRouteError::InvalidUri {
                uri: path.to_string(),
            }
        })?;
    let bucket = resource_arn
        .strip_prefix("arn:aws:s3:::")
        .filter(|bucket| !bucket.is_empty() && !bucket.contains('/'))
        .and_then(|bucket| BucketName::try_from(bucket.to_string()).ok())
        .ok_or_else(|| S3ControlRouteError::InvalidUri {
            uri: invalid_s3_control_uri(path, Some(&resource_arn)),
        })?;
    Ok(bucket)
}

fn route_s3_control(method: &str, path: &str) -> Result<S3ControlOperation, S3ControlRouteError> {
    if has_malformed_percent_triplet(path) {
        return Err(S3ControlRouteError::EmptyBadRequest);
    }
    if matches!(method, "PROPFIND" | "X-ARGMIN-PROBE") {
        return Err(S3ControlRouteError::FrontendBadRequest);
    }
    let Some(resource_path) = s3_control_resource_path(path) else {
        let decoded = crate::http::request::percent_decode_strict(path).map_err(|_| {
            S3ControlRouteError::InvalidUri {
                uri: path.to_string(),
            }
        })?;
        let uri = if let Some(relative) = decoded.strip_prefix("/v20180820/") {
            relative.to_string()
        } else {
            path.to_string()
        };
        return Err(S3ControlRouteError::InvalidUri { uri });
    };
    let bucket = parse_s3_control_bucket_resource(path, resource_path)?;
    match method {
        "GET" => Ok(S3ControlOperation::ListTagsForResource { bucket }),
        "POST" => Ok(S3ControlOperation::TagResource { bucket }),
        "DELETE" => Ok(S3ControlOperation::UntagResource { bucket }),
        "HEAD" => Ok(S3ControlOperation::HeadBucketTags),
        "OPTIONS" => Ok(S3ControlOperation::Options),
        "PUT" | "PATCH" => Ok(S3ControlOperation::MethodNotAllowed {
            method: method.to_string(),
        }),
        _ => Ok(S3ControlOperation::MethodNotAllowed {
            method: method.to_string(),
        }),
    }
}

/// Route a request using only trusted listener configuration and its request
/// target. Request authority text is deliberately absent from this API.
pub(crate) fn route_service(
    endpoint: EndpointKind,
    method: &str,
    path: &str,
    query: &str,
) -> Result<ServiceOperation, ServiceRouteError> {
    match endpoint {
        EndpointKind::SharedRegional if is_s3_control_candidate_path(path) => {
            route_s3_control(method, path)
                .map(ServiceOperation::S3Control)
                .map_err(ServiceRouteError::S3Control)
        }
        EndpointKind::S3Only | EndpointKind::SharedRegional => route(method, path, query)
            .map(ServiceOperation::S3)
            .map_err(ServiceRouteError::S3),
        EndpointKind::StsOnly => {
            unreachable!("the S3-family router cannot route an STS-only endpoint")
        }
    }
}

/// Validate an S3 bucket name per AWS rules.
///
/// Rules enforced:
/// - 3-63 characters long
/// - Only lowercase letters, digits, hyphens, and periods
/// - Must start and end with a letter or digit
/// - No consecutive periods (`..`)
/// - No dot-dash (`.-`) or dash-dot (`-.`)
/// - Not formatted as an IP address
/// - Must not start with `xn--` (reserved for IDN/Punycode)
fn parse_bucket_name(name: &str) -> Result<BucketName, ServerError> {
    BucketName::try_from(name).map_err(|error| ServerError::InvalidBucketName {
        reason: error.to_string(),
    })
}

/// Validate an S3 object key.
/// 1-1024 bytes, no null bytes.
pub(crate) fn validate_object_key(key: &str) -> Result<(), ServerError> {
    ObjectKey::try_from(key).map(|_| ()).map_err(|error| {
        if let storage::ObjectKeyError::InvalidLength { length } = error {
            if length > 1024 {
                return ServerError::KeyTooLongError {
                    size: length,
                    max_size_allowed: 1024,
                };
            }
        }
        ServerError::InvalidRequest {
            reason: error.to_string(),
        }
    })
}

/// Route an HTTP request to an S3 operation.
///
/// Path-style addressing only: `/<bucket>` or `/<bucket>/<key...>`.
pub fn route(method: &str, path: &str, query: &str) -> Result<S3Operation, ServerError> {
    // Split path into segments
    let trimmed = path.strip_prefix('/').unwrap_or(path);

    if trimmed.is_empty() {
        // Root path: GET / = ListBuckets
        return match method {
            "GET" => Ok(S3Operation::ListBuckets),
            _ => Err(ServerError::MethodNotAllowed),
        };
    }

    // Split into bucket and optional key (percent-decode key later)
    let (bucket, key) = match trimmed.find('/') {
        Some(pos) => {
            let bucket = &trimmed[..pos];
            let key = &trimmed[pos + 1..];
            (bucket, if key.is_empty() { None } else { Some(key) })
        }
        None => (trimmed, None),
    };

    let bucket = parse_bucket_name(bucket)?;

    let decoded_key = key
        .map(crate::http::request::percent_decode_strict)
        .transpose()?;
    if let Some(ref k) = decoded_key {
        validate_object_key(k)?;
    }

    match (method, decoded_key) {
        // OPTIONS requests (preflight CORS)
        ("OPTIONS", key) => Ok(S3Operation::OptionsRequest {
            bucket: bucket.clone(),
            key,
        }),

        // Bucket-level operations (no key)
        ("PUT", None) if query_has_key(query, "versioning") => {
            Ok(S3Operation::PutBucketVersioning {
                bucket: bucket.clone(),
            })
        }
        ("PUT", None) if query_has_key(query, "object-lock") => {
            Ok(S3Operation::PutBucketObjectLockConfiguration {
                bucket: bucket.clone(),
            })
        }
        ("PUT", None) if query_has_key(query, "encryption") => {
            Ok(S3Operation::PutBucketEncryption {
                bucket: bucket.clone(),
            })
        }
        ("PUT", None) if query_has_key(query, "cors") => Ok(S3Operation::PutBucketCors {
            bucket: bucket.clone(),
        }),
        ("PUT", None) if query_has_key(query, "tagging") => Ok(S3Operation::PutBucketTagging {
            bucket: bucket.clone(),
        }),
        ("PUT", None) if query_has_key(query, "abac") => Ok(S3Operation::PutBucketAbac {
            bucket: bucket.clone(),
        }),
        ("PUT", None) if query_has_key(query, "lifecycle") => Ok(S3Operation::PutBucketLifecycle {
            bucket: bucket.clone(),
        }),
        ("PUT", None) if query_has_key(query, "publicAccessBlock") => {
            Ok(S3Operation::PutBucketPublicAccessBlock {
                bucket: bucket.clone(),
            })
        }
        ("PUT", None) if query_has_key(query, "acl") => Ok(S3Operation::PutBucketAcl {
            bucket: bucket.clone(),
        }),
        ("PUT", None) if query_has_key(query, "ownershipControls") => {
            Ok(S3Operation::PutBucketOwnershipControls {
                bucket: bucket.clone(),
            })
        }
        ("PUT", None) if query_has_key(query, "policy") => Ok(S3Operation::PutBucketPolicy {
            bucket: bucket.clone(),
        }),
        ("PUT", None) => Ok(S3Operation::CreateBucket {
            bucket: bucket.clone(),
        }),
        ("DELETE", None) if query_has_key(query, "cors") => Ok(S3Operation::DeleteBucketCors {
            bucket: bucket.clone(),
        }),
        ("DELETE", None) if query_has_key(query, "encryption") => {
            Ok(S3Operation::DeleteBucketEncryption {
                bucket: bucket.clone(),
            })
        }
        ("DELETE", None) if query_has_key(query, "publicAccessBlock") => {
            Ok(S3Operation::DeleteBucketPublicAccessBlock {
                bucket: bucket.clone(),
            })
        }
        ("DELETE", None) if query_has_key(query, "ownershipControls") => {
            Ok(S3Operation::DeleteBucketOwnershipControls {
                bucket: bucket.clone(),
            })
        }
        ("DELETE", None) if query_has_key(query, "tagging") => {
            Ok(S3Operation::DeleteBucketTagging {
                bucket: bucket.clone(),
            })
        }
        ("DELETE", None) if query_has_key(query, "lifecycle") => {
            Ok(S3Operation::DeleteBucketLifecycle {
                bucket: bucket.clone(),
            })
        }
        ("DELETE", None) if query_has_key(query, "policy") => Ok(S3Operation::DeleteBucketPolicy {
            bucket: bucket.clone(),
        }),
        ("DELETE", None) => Ok(S3Operation::DeleteBucket {
            bucket: bucket.clone(),
        }),
        ("HEAD", None) => Ok(S3Operation::HeadBucket {
            bucket: bucket.clone(),
        }),
        ("GET", None) => {
            if query_has_key(query, "location") {
                return Ok(S3Operation::GetBucketLocation {
                    bucket: bucket.clone(),
                });
            }
            // Check for ?uploads → ListMultipartUploads
            if query_has_key(query, "uploads") {
                return Ok(S3Operation::ListMultipartUploads {
                    bucket: bucket.clone(),
                });
            }
            // Check for ?ownershipControls → GetBucketOwnershipControls
            if query_has_key(query, "ownershipControls") {
                return Ok(S3Operation::GetBucketOwnershipControls {
                    bucket: bucket.clone(),
                });
            }
            // Check for ?publicAccessBlock → GetBucketPublicAccessBlock
            if query_has_key(query, "publicAccessBlock") {
                return Ok(S3Operation::GetBucketPublicAccessBlock {
                    bucket: bucket.clone(),
                });
            }
            // Check for ?cors → GetBucketCors
            if query_has_key(query, "cors") {
                return Ok(S3Operation::GetBucketCors {
                    bucket: bucket.clone(),
                });
            }
            // Check for ?tagging → GetBucketTagging
            if query_has_key(query, "tagging") {
                return Ok(S3Operation::GetBucketTagging {
                    bucket: bucket.clone(),
                });
            }
            if query_has_key(query, "abac") {
                return Ok(S3Operation::GetBucketAbac {
                    bucket: bucket.clone(),
                });
            }
            if query_has_key(query, "lifecycle") {
                return Ok(S3Operation::GetBucketLifecycle {
                    bucket: bucket.clone(),
                });
            }
            // Check for ?policy → GetBucketPolicy
            if query_has_key(query, "policy") {
                return Ok(S3Operation::GetBucketPolicy {
                    bucket: bucket.clone(),
                });
            }
            // Check for ?policyStatus → GetBucketPolicyStatus
            if query_has_key(query, "policyStatus") {
                return Ok(S3Operation::GetBucketPolicyStatus {
                    bucket: bucket.clone(),
                });
            }
            // Check for ?acl → GetBucketAcl
            if query_has_key(query, "acl") {
                return Ok(S3Operation::GetBucketAcl {
                    bucket: bucket.clone(),
                });
            }
            // Check for ?versioning → GetBucketVersioning
            if query_has_key(query, "versioning") {
                return Ok(S3Operation::GetBucketVersioning {
                    bucket: bucket.clone(),
                });
            }
            // Check for ?object-lock → GetBucketObjectLockConfiguration
            if query_has_key(query, "object-lock") {
                return Ok(S3Operation::GetBucketObjectLockConfiguration {
                    bucket: bucket.clone(),
                });
            }
            // Check for ?encryption → GetBucketEncryption
            if query_has_key(query, "encryption") {
                return Ok(S3Operation::GetBucketEncryption {
                    bucket: bucket.clone(),
                });
            }
            // Check for ?versions → ListObjectVersions
            if query_has_key(query, "versions") {
                return Ok(S3Operation::ListObjectVersions {
                    bucket: bucket.clone(),
                });
            }
            // Check for list-type=2 → V2, otherwise → V1
            let is_v2 = query_has_param(query, "list-type", "2");
            if is_v2 {
                Ok(S3Operation::ListObjectsV2 {
                    bucket: bucket.clone(),
                })
            } else {
                Ok(S3Operation::ListObjectsV1 {
                    bucket: bucket.clone(),
                })
            }
        }
        ("POST", None) => {
            if query_has_key(query, "delete") {
                Ok(S3Operation::DeleteObjects {
                    bucket: bucket.clone(),
                })
            } else {
                Ok(S3Operation::PostObject {
                    bucket: bucket.clone(),
                })
            }
        }

        // Object-level tagging (must appear before catch-all)
        ("PUT", Some(key)) if query_has_key(query, "tagging") => {
            Ok(S3Operation::PutObjectTagging {
                bucket: bucket.clone(),
                key,
            })
        }
        ("PUT", Some(key)) if query_has_key(query, "acl") => Ok(S3Operation::PutObjectAcl {
            bucket: bucket.clone(),
            key,
        }),
        ("GET", Some(key)) if query_has_key(query, "tagging") => {
            Ok(S3Operation::GetObjectTagging {
                bucket: bucket.clone(),
                key,
            })
        }
        ("GET", Some(key)) if query_has_key(query, "acl") => Ok(S3Operation::GetObjectAcl {
            bucket: bucket.clone(),
            key,
        }),
        ("DELETE", Some(key)) if query_has_key(query, "tagging") => {
            Ok(S3Operation::DeleteObjectTagging {
                bucket: bucket.clone(),
                key,
            })
        }
        ("PUT", Some(key)) if query_has_key(query, "retention") => {
            Ok(S3Operation::PutObjectRetention {
                bucket: bucket.clone(),
                key,
            })
        }
        ("GET", Some(key)) if query_has_key(query, "retention") => {
            Ok(S3Operation::GetObjectRetention {
                bucket: bucket.clone(),
                key,
            })
        }
        ("PUT", Some(key)) if query_has_key(query, "legal-hold") => {
            Ok(S3Operation::PutObjectLegalHold {
                bucket: bucket.clone(),
                key,
            })
        }
        ("GET", Some(key)) if query_has_key(query, "legal-hold") => {
            Ok(S3Operation::GetObjectLegalHold {
                bucket: bucket.clone(),
                key,
            })
        }

        // GetObjectAttributes (must appear before catch-all GET)
        ("GET", Some(key)) if query_has_key(query, "attributes") => {
            Ok(S3Operation::GetObjectAttributes {
                bucket: bucket.clone(),
                key,
            })
        }

        // Multipart upload operations (must appear before catch-all object operations)
        ("POST", Some(key)) if query_has_key(query, "uploads") => {
            Ok(S3Operation::CreateMultipartUpload {
                bucket: bucket.clone(),
                key,
            })
        }
        ("POST", Some(key)) if query_has_key(query, "uploadId") => {
            Ok(S3Operation::CompleteMultipartUpload {
                bucket: bucket.clone(),
                key,
            })
        }
        ("PUT", Some(_))
            if query_has_key(query, "uploadId") && !query_has_key(query, "partNumber") =>
        {
            Err(ServerError::PutMultipartUploadMethodNotAllowed)
        }
        ("PUT", Some(key)) if query_has_key(query, "partNumber") => Ok(S3Operation::UploadPart {
            bucket: bucket.clone(),
            key,
        }),
        ("DELETE", Some(key)) if query_has_key(query, "uploadId") => {
            Ok(S3Operation::AbortMultipartUpload {
                bucket: bucket.clone(),
                key,
            })
        }
        ("GET", Some(key)) if query_has_key(query, "uploadId") => Ok(S3Operation::ListParts {
            bucket: bucket.clone(),
            key,
        }),

        // Object-level operations
        ("PUT", Some(key)) => Ok(S3Operation::PutObject {
            bucket: bucket.clone(),
            key,
        }),
        ("GET", Some(key)) => Ok(S3Operation::GetObject {
            bucket: bucket.clone(),
            key,
        }),
        ("DELETE", Some(key)) => Ok(S3Operation::DeleteObject {
            bucket: bucket.clone(),
            key,
        }),
        ("HEAD", Some(key)) => Ok(S3Operation::HeadObject {
            bucket: bucket.clone(),
            key,
        }),

        _ => Err(ServerError::MethodNotAllowed),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bucket_name(name: &str) -> BucketName {
        parse_bucket_name(name).unwrap()
    }

    #[test]
    fn list_buckets() {
        assert_eq!(route("GET", "/", "").unwrap(), S3Operation::ListBuckets);
    }

    #[test]
    fn shared_regional_routes_s3_control_operations_before_s3() {
        let path = "/v20180820/tags/arn%3Aaws%3As3%3A%3A%3Abucket";
        assert_eq!(
            route_service(EndpointKind::SharedRegional, "GET", path, "").unwrap(),
            ServiceOperation::S3Control(S3ControlOperation::ListTagsForResource {
                bucket: bucket_name("bucket"),
            })
        );
        assert_eq!(
            route_service(EndpointKind::SharedRegional, "POST", path, "").unwrap(),
            ServiceOperation::S3Control(S3ControlOperation::TagResource {
                bucket: bucket_name("bucket"),
            })
        );
        assert_eq!(
            route_service(
                EndpointKind::SharedRegional,
                "DELETE",
                path,
                "tagKeys=env&tagKeys=security"
            )
            .unwrap(),
            ServiceOperation::S3Control(S3ControlOperation::UntagResource {
                bucket: bucket_name("bucket"),
            })
        );
    }

    #[test]
    fn s3_only_endpoint_does_not_select_s3_control_from_reserved_path() {
        let routed = route_service(
            EndpointKind::S3Only,
            "GET",
            "/v20180820/tags/arn%3Aaws%3As3%3A%3A%3Abucket",
            "",
        )
        .unwrap();
        assert!(matches!(
            routed,
            ServiceOperation::S3(S3Operation::GetObject { .. })
        ));
    }

    #[test]
    fn shared_regional_accepts_encoded_s3_control_path_separator() {
        assert!(matches!(
            route_service(
                EndpointKind::SharedRegional,
                "GET",
                "/v20180820/tags%2Farn%3Aaws%3As3%3A%3A%3Abucket",
                "",
            ),
            Ok(ServiceOperation::S3Control(
                S3ControlOperation::ListTagsForResource { .. }
            ))
        ));
    }

    #[test]
    fn create_bucket() {
        assert_eq!(
            route("PUT", "/mybucket", "").unwrap(),
            S3Operation::CreateBucket {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn delete_bucket() {
        assert_eq!(
            route("DELETE", "/mybucket", "").unwrap(),
            S3Operation::DeleteBucket {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn head_bucket() {
        assert_eq!(
            route("HEAD", "/mybucket", "").unwrap(),
            S3Operation::HeadBucket {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn get_bucket_location() {
        assert_eq!(
            route("GET", "/mybucket", "location").unwrap(),
            S3Operation::GetBucketLocation {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn list_objects_v2() {
        assert_eq!(
            route("GET", "/mybucket", "list-type=2").unwrap(),
            S3Operation::ListObjectsV2 {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn list_objects_v1_default() {
        assert_eq!(
            route("GET", "/mybucket", "").unwrap(),
            S3Operation::ListObjectsV1 {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn list_objects_v1_without_list_type() {
        assert_eq!(
            route("GET", "/mybucket", "prefix=foo").unwrap(),
            S3Operation::ListObjectsV1 {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn list_objects_v1_explicit_list_type_1() {
        assert_eq!(
            route("GET", "/mybucket", "list-type=1").unwrap(),
            S3Operation::ListObjectsV1 {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn list_objects_v2_with_other_params() {
        assert_eq!(
            route("GET", "/mybucket", "list-type=2&prefix=foo&max-keys=10").unwrap(),
            S3Operation::ListObjectsV2 {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn list_objects_v2_when_duplicate_list_type_contains_two() {
        assert_eq!(
            route("GET", "/mybucket", "list-type=1&list-type=2").unwrap(),
            S3Operation::ListObjectsV2 {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn get_bucket_trailing_slash_is_list_v1() {
        assert_eq!(
            route("GET", "/mybucket/", "").unwrap(),
            S3Operation::ListObjectsV1 {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn put_object() {
        assert_eq!(
            route("PUT", "/bucket/key.txt", "").unwrap(),
            S3Operation::PutObject {
                bucket: bucket_name("bucket"),
                key: "key.txt".to_string()
            }
        );
    }

    #[test]
    fn get_object() {
        assert_eq!(
            route("GET", "/bucket/path/to/key", "").unwrap(),
            S3Operation::GetObject {
                bucket: bucket_name("bucket"),
                key: "path/to/key".to_string()
            }
        );
    }

    #[test]
    fn delete_object() {
        assert_eq!(
            route("DELETE", "/bucket/key", "").unwrap(),
            S3Operation::DeleteObject {
                bucket: bucket_name("bucket"),
                key: "key".to_string()
            }
        );
    }

    #[test]
    fn head_object() {
        assert_eq!(
            route("HEAD", "/bucket/key", "").unwrap(),
            S3Operation::HeadObject {
                bucket: bucket_name("bucket"),
                key: "key".to_string()
            }
        );
    }

    #[test]
    fn nested_key_path() {
        assert_eq!(
            route("GET", "/bucket/a/b/c/d.txt", "").unwrap(),
            S3Operation::GetObject {
                bucket: bucket_name("bucket"),
                key: "a/b/c/d.txt".to_string()
            }
        );
    }

    #[test]
    fn method_not_allowed_root() {
        assert!(route("PUT", "/", "").is_err());
    }

    #[test]
    fn valid_bucket_names() {
        assert!(route("HEAD", "/my-bucket", "").is_ok());
        assert!(route("HEAD", "/abc", "").is_ok());
        assert!(route("HEAD", "/my.bucket.name", "").is_ok());
        assert!(route("HEAD", "/123", "").is_ok());
    }

    #[test]
    fn invalid_bucket_names() {
        // Too short
        assert!(route("HEAD", "/ab", "").is_err());
        // Too long (64 chars)
        let long = "/".to_string() + &"a".repeat(64);
        assert!(route("HEAD", &long, "").is_err());
        // Leading hyphen
        assert!(route("HEAD", "/-bucket", "").is_err());
        // Trailing hyphen
        assert!(route("HEAD", "/bucket-", "").is_err());
        // Leading dot
        assert!(route("HEAD", "/.bucket", "").is_err());
        // Trailing dot
        assert!(route("HEAD", "/bucket.", "").is_err());
        // Uppercase
        assert!(route("HEAD", "/MyBucket", "").is_err());
        // Consecutive periods
        assert!(route("HEAD", "/my..bucket", "").is_err());
        // IP address format
        assert!(route("HEAD", "/192.168.1.1", "").is_err());
        // xn-- prefix (IDN reserved)
        assert!(route("HEAD", "/xn--bucket", "").is_err());
    }

    #[test]
    fn valid_object_keys() {
        assert!(route("GET", "/bucket/a", "").is_ok());
        assert!(route("GET", "/bucket/path/to/file.txt", "").is_ok());
        assert!(route("GET", "/bucket/key with spaces", "").is_ok());
    }

    #[test]
    fn object_key_allows_control_chars_except_nul() {
        assert!(route("GET", "/bucket/\u{008A}-", "").is_ok());
        assert!(route("GET", "/bucket/\u{0001}-", "").is_ok());
    }

    #[test]
    fn object_key_rejects_nul() {
        let err = route("GET", "/bucket/\0", "").unwrap_err();
        match err {
            ServerError::InvalidRequest { reason } => {
                assert_eq!(reason, "object key must not contain null bytes");
            }
            _ => panic!("expected InvalidRequest, got {err:?}"),
        }
    }

    #[test]
    fn key_with_trailing_slash() {
        assert_eq!(
            route("PUT", "/bucket/key/", "").unwrap(),
            S3Operation::PutObject {
                bucket: bucket_name("bucket"),
                key: "key/".to_string()
            }
        );
    }

    #[test]
    fn bucket_trailing_slash_is_bucket_op() {
        assert_eq!(
            route("HEAD", "/mybucket/", "").unwrap(),
            S3Operation::HeadBucket {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn nested_key_with_trailing_slash() {
        assert_eq!(
            route("GET", "/bucket/a/b/c/", "").unwrap(),
            S3Operation::GetObject {
                bucket: bucket_name("bucket"),
                key: "a/b/c/".to_string()
            }
        );
    }

    #[test]
    fn object_key_too_long() {
        let long_key = "k".repeat(1025);
        let path = format!("/bucket/{long_key}");
        assert!(route("GET", &path, "").is_err());
    }

    #[test]
    fn delete_objects_post_with_delete_query() {
        assert_eq!(
            route("POST", "/mybucket", "delete").unwrap(),
            S3Operation::DeleteObjects {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn delete_objects_post_with_delete_query_and_other_params() {
        assert_eq!(
            route("POST", "/mybucket", "delete&foo=bar").unwrap(),
            S3Operation::DeleteObjects {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn post_object() {
        assert_eq!(
            route("POST", "/mybucket", "").unwrap(),
            S3Operation::PostObject {
                bucket: bucket_name("mybucket")
            }
        );
        assert_eq!(
            route("POST", "/mybucket", "foo=bar").unwrap(),
            S3Operation::PostObject {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn list_object_versions() {
        assert_eq!(
            route("GET", "/mybucket", "versions").unwrap(),
            S3Operation::ListObjectVersions {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn list_object_versions_with_params() {
        assert_eq!(
            route("GET", "/mybucket", "versions&prefix=foo&max-keys=10").unwrap(),
            S3Operation::ListObjectVersions {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn put_bucket_versioning() {
        assert_eq!(
            route("PUT", "/mybucket", "versioning").unwrap(),
            S3Operation::PutBucketVersioning {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn get_bucket_versioning() {
        assert_eq!(
            route("GET", "/mybucket", "versioning").unwrap(),
            S3Operation::GetBucketVersioning {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn put_bucket_object_lock_configuration() {
        assert_eq!(
            route("PUT", "/mybucket", "object-lock").unwrap(),
            S3Operation::PutBucketObjectLockConfiguration {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn get_bucket_object_lock_configuration() {
        assert_eq!(
            route("GET", "/mybucket", "object-lock").unwrap(),
            S3Operation::GetBucketObjectLockConfiguration {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn put_bucket_encryption() {
        assert_eq!(
            route("PUT", "/mybucket", "encryption").unwrap(),
            S3Operation::PutBucketEncryption {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn get_bucket_encryption() {
        assert_eq!(
            route("GET", "/mybucket", "encryption").unwrap(),
            S3Operation::GetBucketEncryption {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn delete_bucket_encryption() {
        assert_eq!(
            route("DELETE", "/mybucket", "encryption").unwrap(),
            S3Operation::DeleteBucketEncryption {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn put_bucket_cors() {
        assert_eq!(
            route("PUT", "/mybucket", "cors").unwrap(),
            S3Operation::PutBucketCors {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn get_bucket_cors() {
        assert_eq!(
            route("GET", "/mybucket", "cors").unwrap(),
            S3Operation::GetBucketCors {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn delete_bucket_cors() {
        assert_eq!(
            route("DELETE", "/mybucket", "cors").unwrap(),
            S3Operation::DeleteBucketCors {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn put_bucket_lifecycle() {
        assert_eq!(
            route("PUT", "/mybucket", "lifecycle").unwrap(),
            S3Operation::PutBucketLifecycle {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn get_bucket_lifecycle() {
        assert_eq!(
            route("GET", "/mybucket", "lifecycle").unwrap(),
            S3Operation::GetBucketLifecycle {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn delete_bucket_lifecycle() {
        assert_eq!(
            route("DELETE", "/mybucket", "lifecycle").unwrap(),
            S3Operation::DeleteBucketLifecycle {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn options_bucket() {
        assert_eq!(
            route("OPTIONS", "/mybucket", "").unwrap(),
            S3Operation::OptionsRequest {
                bucket: bucket_name("mybucket"),
                key: None,
            }
        );
    }

    #[test]
    fn options_bucket_with_key() {
        assert_eq!(
            route("OPTIONS", "/mybucket/path/to/key", "").unwrap(),
            S3Operation::OptionsRequest {
                bucket: bucket_name("mybucket"),
                key: Some("path/to/key".to_string()),
            }
        );
    }

    #[test]
    fn put_bucket_versioning_takes_priority_over_create() {
        // PUT /bucket?versioning should be PutBucketVersioning, not CreateBucket
        assert_eq!(
            route("PUT", "/mybucket", "versioning").unwrap(),
            S3Operation::PutBucketVersioning {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn put_bucket_encryption_takes_priority_over_create() {
        assert_eq!(
            route("PUT", "/mybucket", "encryption").unwrap(),
            S3Operation::PutBucketEncryption {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn put_bucket_public_access_block() {
        assert_eq!(
            route("PUT", "/mybucket", "publicAccessBlock").unwrap(),
            S3Operation::PutBucketPublicAccessBlock {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn get_bucket_public_access_block() {
        assert_eq!(
            route("GET", "/mybucket", "publicAccessBlock").unwrap(),
            S3Operation::GetBucketPublicAccessBlock {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn delete_bucket_public_access_block() {
        assert_eq!(
            route("DELETE", "/mybucket", "publicAccessBlock").unwrap(),
            S3Operation::DeleteBucketPublicAccessBlock {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn put_bucket_acl() {
        assert_eq!(
            route("PUT", "/mybucket", "acl").unwrap(),
            S3Operation::PutBucketAcl {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn get_bucket_acl() {
        assert_eq!(
            route("GET", "/mybucket", "acl").unwrap(),
            S3Operation::GetBucketAcl {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn put_bucket_ownership_controls() {
        assert_eq!(
            route("PUT", "/mybucket", "ownershipControls").unwrap(),
            S3Operation::PutBucketOwnershipControls {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn get_bucket_ownership_controls() {
        assert_eq!(
            route("GET", "/mybucket", "ownershipControls").unwrap(),
            S3Operation::GetBucketOwnershipControls {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn put_bucket_abac() {
        assert_eq!(
            route("PUT", "/mybucket", "abac").unwrap(),
            S3Operation::PutBucketAbac {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn get_bucket_abac() {
        assert_eq!(
            route("GET", "/mybucket", "abac").unwrap(),
            S3Operation::GetBucketAbac {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn delete_bucket_ownership_controls() {
        assert_eq!(
            route("DELETE", "/mybucket", "ownershipControls").unwrap(),
            S3Operation::DeleteBucketOwnershipControls {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn put_bucket_policy() {
        assert_eq!(
            route("PUT", "/mybucket", "policy").unwrap(),
            S3Operation::PutBucketPolicy {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn get_bucket_policy() {
        assert_eq!(
            route("GET", "/mybucket", "policy").unwrap(),
            S3Operation::GetBucketPolicy {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn get_bucket_policy_status() {
        assert_eq!(
            route("GET", "/mybucket", "policyStatus").unwrap(),
            S3Operation::GetBucketPolicyStatus {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn delete_bucket_policy() {
        assert_eq!(
            route("DELETE", "/mybucket", "policy").unwrap(),
            S3Operation::DeleteBucketPolicy {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn get_object_attributes() {
        assert_eq!(
            route("GET", "/mybucket/mykey", "attributes").unwrap(),
            S3Operation::GetObjectAttributes {
                bucket: bucket_name("mybucket"),
                key: "mykey".to_string()
            }
        );
    }

    #[test]
    fn get_object_attributes_with_version() {
        assert_eq!(
            route("GET", "/mybucket/mykey", "attributes&versionId=123").unwrap(),
            S3Operation::GetObjectAttributes {
                bucket: bucket_name("mybucket"),
                key: "mykey".to_string()
            }
        );
    }

    #[test]
    fn get_object_without_attributes_is_get_object() {
        assert_eq!(
            route("GET", "/mybucket/mykey", "").unwrap(),
            S3Operation::GetObject {
                bucket: bucket_name("mybucket"),
                key: "mykey".to_string()
            }
        );
    }

    #[test]
    fn put_object_retention_takes_priority_over_put_object() {
        assert_eq!(
            route("PUT", "/mybucket/mykey", "retention&versionId=123").unwrap(),
            S3Operation::PutObjectRetention {
                bucket: bucket_name("mybucket"),
                key: "mykey".to_string()
            }
        );
    }

    #[test]
    fn get_object_retention_takes_priority_over_get_object() {
        assert_eq!(
            route("GET", "/mybucket/mykey", "retention").unwrap(),
            S3Operation::GetObjectRetention {
                bucket: bucket_name("mybucket"),
                key: "mykey".to_string()
            }
        );
    }

    #[test]
    fn put_object_legal_hold_takes_priority_over_put_object() {
        assert_eq!(
            route("PUT", "/mybucket/mykey", "legal-hold").unwrap(),
            S3Operation::PutObjectLegalHold {
                bucket: bucket_name("mybucket"),
                key: "mykey".to_string()
            }
        );
    }

    #[test]
    fn get_object_legal_hold_takes_priority_over_get_object() {
        assert_eq!(
            route("GET", "/mybucket/mykey", "legal-hold").unwrap(),
            S3Operation::GetObjectLegalHold {
                bucket: bucket_name("mybucket"),
                key: "mykey".to_string()
            }
        );
    }

    #[test]
    fn unknown_bucket_query_still_falls_through_to_list_objects() {
        assert_eq!(
            route("GET", "/mybucket", "foo=bar").unwrap(),
            S3Operation::ListObjectsV1 {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn unknown_object_query_still_falls_through_to_put_object() {
        assert_eq!(
            route("PUT", "/mybucket/mykey", "foo=bar").unwrap(),
            S3Operation::PutObject {
                bucket: bucket_name("mybucket"),
                key: "mykey".to_string()
            }
        );
    }

    // ── Multipart upload routing tests ───────────────────────────────

    #[test]
    fn create_multipart_upload() {
        assert_eq!(
            route("POST", "/mybucket/mykey", "uploads").unwrap(),
            S3Operation::CreateMultipartUpload {
                bucket: bucket_name("mybucket"),
                key: "mykey".to_string()
            }
        );
    }

    #[test]
    fn create_multipart_upload_nested_key() {
        assert_eq!(
            route("POST", "/mybucket/a/b/c.txt", "uploads").unwrap(),
            S3Operation::CreateMultipartUpload {
                bucket: bucket_name("mybucket"),
                key: "a/b/c.txt".to_string()
            }
        );
    }

    #[test]
    fn upload_part() {
        assert_eq!(
            route("PUT", "/mybucket/mykey", "partNumber=1&uploadId=abc").unwrap(),
            S3Operation::UploadPart {
                bucket: bucket_name("mybucket"),
                key: "mykey".to_string()
            }
        );
    }

    #[test]
    fn upload_part_just_part_number() {
        // partNumber alone routes to UploadPart (uploadId validation happens in dispatch)
        assert_eq!(
            route("PUT", "/mybucket/mykey", "partNumber=5").unwrap(),
            S3Operation::UploadPart {
                bucket: bucket_name("mybucket"),
                key: "mykey".to_string()
            }
        );
    }

    #[test]
    fn put_multipart_upload_without_part_number_is_method_not_allowed() {
        assert!(matches!(
            route("PUT", "/mybucket/mykey", "uploadId=abc"),
            Err(ServerError::PutMultipartUploadMethodNotAllowed)
        ));
    }

    #[test]
    fn complete_multipart_upload() {
        assert_eq!(
            route("POST", "/mybucket/mykey", "uploadId=abc123").unwrap(),
            S3Operation::CompleteMultipartUpload {
                bucket: bucket_name("mybucket"),
                key: "mykey".to_string()
            }
        );
    }

    #[test]
    fn abort_multipart_upload() {
        assert_eq!(
            route("DELETE", "/mybucket/mykey", "uploadId=abc123").unwrap(),
            S3Operation::AbortMultipartUpload {
                bucket: bucket_name("mybucket"),
                key: "mykey".to_string()
            }
        );
    }

    #[test]
    fn list_multipart_uploads() {
        assert_eq!(
            route("GET", "/mybucket", "uploads").unwrap(),
            S3Operation::ListMultipartUploads {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn list_multipart_uploads_with_params() {
        assert_eq!(
            route("GET", "/mybucket", "uploads&prefix=foo&max-uploads=10").unwrap(),
            S3Operation::ListMultipartUploads {
                bucket: bucket_name("mybucket")
            }
        );
    }

    #[test]
    fn list_parts() {
        assert_eq!(
            route("GET", "/mybucket/mykey", "uploadId=abc123").unwrap(),
            S3Operation::ListParts {
                bucket: bucket_name("mybucket"),
                key: "mykey".to_string()
            }
        );
    }

    #[test]
    fn list_parts_with_params() {
        assert_eq!(
            route(
                "GET",
                "/mybucket/mykey",
                "uploadId=abc&part-number-marker=5&max-parts=10"
            )
            .unwrap(),
            S3Operation::ListParts {
                bucket: bucket_name("mybucket"),
                key: "mykey".to_string()
            }
        );
    }

    // ── Multipart precedence tests ───────────────────────────────────

    #[test]
    fn post_uploads_takes_priority_over_catch_all() {
        // POST /bucket/key?uploads → CreateMultipartUpload, not a generic POST
        assert_eq!(
            route("POST", "/mybucket/mykey", "uploads").unwrap(),
            S3Operation::CreateMultipartUpload {
                bucket: bucket_name("mybucket"),
                key: "mykey".to_string()
            }
        );
    }

    #[test]
    fn put_part_number_takes_priority_over_put_object() {
        // PUT /bucket/key?partNumber=1 → UploadPart, not PutObject
        assert_eq!(
            route("PUT", "/mybucket/mykey", "partNumber=1").unwrap(),
            S3Operation::UploadPart {
                bucket: bucket_name("mybucket"),
                key: "mykey".to_string()
            }
        );
    }

    #[test]
    fn delete_upload_id_takes_priority_over_delete_object() {
        // DELETE /bucket/key?uploadId=x → AbortMultipartUpload, not DeleteObject
        assert_eq!(
            route("DELETE", "/mybucket/mykey", "uploadId=x").unwrap(),
            S3Operation::AbortMultipartUpload {
                bucket: bucket_name("mybucket"),
                key: "mykey".to_string()
            }
        );
    }

    #[test]
    fn get_upload_id_takes_priority_over_get_object() {
        // GET /bucket/key?uploadId=x → ListParts, not GetObject
        assert_eq!(
            route("GET", "/mybucket/mykey", "uploadId=x").unwrap(),
            S3Operation::ListParts {
                bucket: bucket_name("mybucket"),
                key: "mykey".to_string()
            }
        );
    }

    #[test]
    fn get_uploads_bucket_level_takes_priority_over_list_objects() {
        // GET /bucket?uploads → ListMultipartUploads, not ListObjectsV1
        assert_eq!(
            route("GET", "/mybucket", "uploads").unwrap(),
            S3Operation::ListMultipartUploads {
                bucket: bucket_name("mybucket")
            }
        );
    }
}
