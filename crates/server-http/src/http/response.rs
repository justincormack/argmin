//! Build HTTP responses for S3 operations.

use std::sync::Arc;

use crate::coordinator::{
    BucketSummary, CompleteMultipartUploadResult, CopyObjectResult, DeleteObjectResult,
    DeleteObjectsResult, GetBucketAclResult, GetObjectAclResult, GetObjectPartResult,
    GetObjectRangeResult, GetObjectResult, HeadObjectPartResult, HeadObjectResult,
    LifecycleAbortHeaders, LifecycleExpirationHeader, ListObjectVersionsResult, ListObjectsResult,
    ListPartsResult, PutObjectResult, ReadHandle,
};
use crate::error::ServerError;
use auth::canonical::uri_encode;
use checksum::{ChecksumAlgorithm, ChecksumType, RawChecksum};
use s3_types::{
    bucket_location_constraint, BucketObjectLockConfig, BucketVersioningState, CanonicalUserId,
    LegalHoldStatus, ObjectLockState, ObjectRetention, VersionId,
};
use server_core::sse::{SseCustomerResponseHeaders, SSE_CUSTOMER_ALGORITHM};
use server_core::system_metadata::SystemMetadata;
use storage::{EffectiveBucketEncryptionConfig, ManagedEncryptionAlgorithm, UploadId};

use super::xml;

const TEST_REQUEST_ID: &str = "request-id";
#[cfg(test)]
const TEST_HOST_ID: &str = "host-id";
const SDK_CHECKSUM_MISSING_VALUE_MESSAGE: &str =
    "x-amz-sdk-checksum-algorithm specified, but no corresponding x-amz-checksum-* or x-amz-trailer headers were found.";
const SDK_CHECKSUM_INVALID_VALUE_MESSAGE: &str =
    "Value for x-amz-sdk-checksum-algorithm header is invalid.";
const UNSUPPORTED_CHECKSUM_ALGORITHM_MESSAGE: &str = "Checksum algorithm provided is unsupported. Please try again with any of the valid types: [CRC32, CRC32C, CRC64NVME, MD5, SHA1, SHA256, SHA512, XXHASH128, XXHASH3, XXHASH64]";

fn is_host_id_invalid_request(reason: &str) -> bool {
    matches!(
        reason,
        SDK_CHECKSUM_MISSING_VALUE_MESSAGE
            | SDK_CHECKSUM_INVALID_VALUE_MESSAGE
            | UNSUPPORTED_CHECKSUM_ALGORITHM_MESSAGE
    ) || reason.starts_with(
        "Checksum Type mismatch occurred, expected checksum Type: null, actual checksum Type: ",
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WireResponseIds {
    request_id: Arc<str>,
    host_id: Arc<str>,
}

impl WireResponseIds {
    #[must_use]
    pub fn new(request_id: impl Into<Arc<str>>, host_id: impl Into<Arc<str>>) -> Self {
        Self {
            request_id: request_id.into(),
            host_id: host_id.into(),
        }
    }

    #[must_use]
    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    #[must_use]
    pub fn host_id(&self) -> &str {
        &self.host_id
    }
}

/// Format a `version_id` for S3 API responses.
/// Null version is displayed as "null".
/// Versioned IDs are displayed as decimal strings.
#[must_use]
pub fn format_version_id(version_id: VersionId) -> String {
    version_id.to_string()
}

/// RFC 2047 Q-encoding for non-ASCII header values.
///
/// AWS S3 returns non-ASCII user metadata (x-amz-meta-*) encoded as RFC 2047
/// encoded-words. This is a legacy HTTP convention (see RFC 9110 §5.5, RFC 2047)
/// that AWS follows for metadata round-tripping. Values are encoded as
/// `=?UTF-8?Q?...?=` where non-printable-ASCII bytes become `=XX` hex pairs
/// and spaces become underscores.
///
/// Reference: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html>
#[allow(clippy::format_push_string)]
fn rfc2047_encode(value: &str) -> String {
    let mut encoded = String::from("=?UTF-8?Q?");
    for byte in value.bytes() {
        match byte {
            // Printable ASCII (except =, ?, _) pass through
            b'!'..=b'<' | b'>'..=b'>' | b'@'..=b'^' | b'`'..=b'~' => {
                encoded.push(byte as char);
            }
            // Space → underscore (RFC 2047 convention)
            b' ' => encoded.push('_'),
            // Everything else (non-ASCII, control chars, =, ?, _) → =XX
            _ => {
                encoded.push('=');
                encoded.push_str(&format!("{byte:02X}"));
            }
        }
    }
    encoded.push_str("?=");
    encoded
}

fn success_action_redirect_location(
    redirect_url: &str,
    bucket: &str,
    key: &str,
    etag: &str,
) -> Option<String> {
    let (base, fragment) = redirect_url
        .split_once('#')
        .map_or((redirect_url, ""), |(base, fragment)| (base, fragment));

    let mut location =
        String::with_capacity(redirect_url.len() + bucket.len() + key.len() + etag.len() + 32);
    location.push_str(base);

    if base.contains('?') {
        if !base.ends_with('?') && !base.ends_with('&') {
            location.push('&');
        }
    } else {
        location.push('?');
    }

    location.push_str("bucket=");
    location.push_str(&uri_encode(bucket));
    location.push_str("&key=");
    location.push_str(&uri_encode(key));
    location.push_str("&etag=");
    location.push_str(&uri_encode(etag));

    if !fragment.is_empty() {
        location.push('#');
        location.push_str(fragment);
    }

    http::header::HeaderValue::from_str(&location)
        .ok()
        .map(|_| location)
}

/// An HTTP response to send back.
pub struct S3Response {
    pub status_code: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub stream: Option<ReadHandle>,
    pub(crate) error_diagnostic: Option<ErrorDiagnostic>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ErrorDiagnostic {
    pub status_code: u16,
    pub error_code: &'static str,
    pub cause_label: &'static str,
    pub cause_chain: String,
    pub server_detail: Option<String>,
}

pub struct CreateMultipartUploadResponseContext<'a> {
    pub managed_encryption: Option<ManagedEncryptionAlgorithm>,
    pub checksum_algorithm: Option<ChecksumAlgorithm>,
    pub checksum_type: Option<ChecksumType>,
    pub lifecycle_abort: Option<&'a LifecycleAbortHeaders>,
    pub sse_customer: Option<&'a SseCustomerResponseHeaders>,
}

fn client_error_message(err: &ServerError) -> String {
    const INTERNAL_ERROR_MESSAGE: &str = "We encountered an internal error. Please try again.";

    match err {
        ServerError::BucketNotFound { .. } => "The specified bucket does not exist".to_string(),
        ServerError::BucketAlreadyExists => "bucket already exists".to_string(),
        ServerError::BucketAlreadyOwnedByYou => "bucket already owned by you".to_string(),
        ServerError::InvalidBucketAclWithBlockPublicAccessError => {
            "bucket ACL cannot be public when block public access is enabled".to_string()
        }
        ServerError::BucketNotEmpty => "bucket not empty".to_string(),
        ServerError::ObjectNotFound { .. } | ServerError::DeleteMarkerHit { .. } => {
            "The specified key does not exist.".to_string()
        }
        ServerError::VersionNotFound { .. } => "The specified version does not exist.".to_string(),
        ServerError::Store(_)
        | ServerError::Metadata(_)
        | ServerError::Ec(_)
        | ServerError::MetadataBlobError { .. }
        | ServerError::InternalError { .. }
        | ServerError::IntegrityError { .. } => INTERNAL_ERROR_MESSAGE.to_string(),
        ServerError::Auth(auth::AuthError::MissingAuth)
        | ServerError::Auth(auth::AuthError::AccessDenied)
        | ServerError::AccessDenied
        | ServerError::BlockPublicPolicyAccessDenied { .. } => "Access Denied".to_string(),
        ServerError::SseCBlockedAccessDenied {
            requester_principal,
            action,
            resource,
        } => format!(
            "User: {} is not authorized to perform: {} on resource: \"{}\" because this bucket has blocked upload requests that specify Server Side Encryption with Customer provided keys (SSE-C). Please specify a different server-side encryption type.",
            requester_principal, action, resource
        ),
        ServerError::PostPolicyAccessDenied { reason } => reason.clone(),
        ServerError::Auth(auth::AuthError::MalformedAuth) => {
            "malformed Authorization header".to_string()
        }
        ServerError::Auth(auth::AuthError::UnsupportedAuthType) => {
            "unsupported Authorization type".to_string()
        }
        ServerError::Auth(auth::AuthError::MissingQueryParam { param }) => {
            format!("missing query auth parameter: {param}")
        }
        ServerError::Auth(auth::AuthError::InvalidQueryParam { param }) => {
            format!("invalid query auth parameter: {param}")
        }
        ServerError::Auth(auth::AuthError::InvalidQueryCredentialRegion {
            provided_region,
            expected_region,
            ..
        }) => format!(
            "Error parsing the X-Amz-Credential parameter; the region '{provided_region}' is wrong; expecting '{expected_region}'"
        ),
        ServerError::Auth(auth::AuthError::InvalidQueryCredentialService {
            provided_service,
            expected_service,
            ..
        }) => format!(
            "Error parsing the X-Amz-Credential parameter; incorrect service \"{provided_service}\". This endpoint belongs to \"{expected_service}\"."
        ),
        ServerError::Auth(auth::AuthError::InvalidCredentialScope { param }) => {
            format!("invalid credential scope: {param}")
        }
        ServerError::Auth(auth::AuthError::InvalidCredentialScopeRegion {
            provided_region,
            expected_region,
            ..
        }) => format!("the region '{provided_region}' is wrong; expecting '{expected_region}'"),
        ServerError::Auth(auth::AuthError::InvalidCredentialScopeService {
            provided_service,
            expected_service,
            ..
        }) => format!(
            "incorrect service \"{provided_service}\". This endpoint belongs to \"{expected_service}\"."
        ),
        ServerError::Auth(auth::AuthError::UnknownAccessKey) => {
            "unknown access key id".to_string()
        }
        ServerError::Auth(auth::AuthError::DuplicateAuthorizationHeader) => {
            "A header you provided implies functionality that is not implemented".to_string()
        }
        ServerError::Auth(auth::AuthError::SignatureMismatch) => "signature mismatch".to_string(),
        ServerError::Auth(auth::AuthError::InvalidToken) => "invalid session token".to_string(),
        ServerError::Auth(auth::AuthError::UnexpectedSecurityToken { .. }) => {
            "The provided token is malformed or otherwise invalid.".to_string()
        }
        ServerError::Auth(auth::AuthError::ExpiredToken) => "token expired".to_string(),
        ServerError::Auth(auth::AuthError::MissingSignedHeader { header }) => {
            format!("missing required signed header: {header}")
        }
        ServerError::Auth(auth::AuthError::RequestExpired) => {
            "request timestamp is too far from server time".to_string()
        }
        ServerError::Auth(auth::AuthError::PresignedRequestExpired) => {
            "Request has expired".to_string()
        }
        ServerError::Auth(auth::AuthError::RequestNotYetValid) => {
            "Request is not yet valid".to_string()
        }
        ServerError::Auth(auth::AuthError::UnsignedHeaders { .. }) => {
            "There were headers present in the request which were not signed".to_string()
        }
        ServerError::WrongRegion {
            provided_region,
            expected_region,
        } => format!(
            "The authorization header is malformed; the region '{provided_region}' is wrong; expecting '{expected_region}'"
        ),
        ServerError::InvalidRequest { reason }
        | ServerError::BadRequest { reason }
        | ServerError::InvalidArgument { reason }
        | ServerError::InvalidRedirectLocation { reason }
        | ServerError::InvalidURI { reason }
        | ServerError::InvalidBucketName { reason }
        | ServerError::MalformedPolicy { reason }
        | ServerError::InvalidPolicyDocument { reason }
        | ServerError::MalformedXML { reason }
        | ServerError::MalformedPOSTRequest { reason }
        | ServerError::MalformedChunkedBody { reason }
        | ServerError::MalformedTrailerError { reason }
        | ServerError::InvalidTag { reason } => reason.clone(),
        ServerError::DuplicateChecksumHeader { .. } => "Only one value may be specified.".to_string(),
        ServerError::UnexpectedContent => "This request does not support content".to_string(),
        ServerError::KeyTooLongError {
            size,
            max_size_allowed,
        } => format!("key too long: {size} bytes (max {max_size_allowed})"),
        ServerError::InvalidBucketNamespace { reason, .. } => reason.clone(),
        ServerError::ObjectTooLarge { size, max } => {
            format!("object too large: {size} bytes (max {max})")
        }
        ServerError::MetadataTooLarge | ServerError::MetadataTooLargeDetailed { .. } => {
            "Your metadata headers exceed the maximum allowed metadata size".to_string()
        }
        ServerError::RequestHeaderSectionTooLarge => {
            "Your request header section exceeds the maximum allowed size.".to_string()
        }
        ServerError::MaxMessageLengthExceeded { .. } => "Your request was too big.".to_string(),
        ServerError::MethodNotAllowed => "method not allowed".to_string(),
        ServerError::HeadDeleteMarkerMethodNotAllowed { .. } => {
            "head on delete marker version not allowed".to_string()
        }
        ServerError::InvalidRange { .. } => "invalid range".to_string(),
        ServerError::PreconditionFailed => "precondition failed".to_string(),
        ServerError::NotModified { .. } => "not modified".to_string(),
        ServerError::SlowDown => "Please reduce your request rate.".to_string(),
        ServerError::BadDigest => "bad digest".to_string(),
        ServerError::ChecksumDigestMismatch { algorithm } => {
            format!("The {algorithm} you specified did not match the calculated checksum.")
        }
        ServerError::InvalidDigest => "invalid digest".to_string(),
        ServerError::InvalidSseCustomerKeyMd5 => {
            "The calculated MD5 hash of the key did not match the hash that was provided."
                .to_string()
        }
        ServerError::MissingSseCustomerAlgorithm => {
            "Requests specifying Server Side Encryption with Customer provided keys must provide a valid encryption algorithm."
                .to_string()
        }
        ServerError::MissingSseCustomerKey => {
            "Requests specifying Server Side Encryption with Customer provided keys must provide an appropriate secret key."
                .to_string()
        }
        ServerError::MissingSseCustomerKeyMd5 => {
            "Requests specifying Server Side Encryption with Customer provided keys must provide the client calculated MD5 of the secret key."
                .to_string()
        }
        ServerError::InvalidEncryptionAlgorithmError { .. } => {
            "The Encryption request you specified is not valid. Supported value: AES256."
                .to_string()
        }
        ServerError::InvalidChunkSize {
            chunk,
            chunk_size,
            min_size,
        } => format!(
            "invalid chunk size: only the last chunk may be smaller than {min_size} bytes (chunk {chunk} was {chunk_size} bytes)"
        ),
        ServerError::NoSuchCorsConfiguration { .. } => "no CORS configuration".to_string(),
        ServerError::NoSuchTagSet { .. } => "no such tag set".to_string(),
        ServerError::NoSuchPublicAccessBlockConfiguration { .. } => {
            "no public access block configuration".to_string()
        }
        ServerError::NoSuchBucketPolicy { .. } => "The bucket policy does not exist".to_string(),
        ServerError::NoSuchLifecycleConfiguration { .. } => {
            "The lifecycle configuration does not exist".to_string()
        }
        ServerError::OwnershipControlsNotFound { .. } => {
            "ownership controls not found".to_string()
        }
        ServerError::ObjectLockConfigurationNotFound { .. } => {
            "Object Lock configuration does not exist for this bucket".to_string()
        }
        ServerError::ServerSideEncryptionConfigurationNotFound { .. } => {
            "The server-side encryption configuration was not found".to_string()
        }
        ServerError::InvalidBucketState => {
            "bucket is in an invalid state for this operation".to_string()
        }
        ServerError::OperationAborted => {
            "A conflicting conditional operation is currently in progress against this resource. Please try again.".to_string()
        }
        ServerError::AccessControlListNotSupported => {
            "ACLs are not supported for this bucket".to_string()
        }
        ServerError::InvalidBucketAclWithObjectOwnership => {
            "invalid bucket ACL with object ownership".to_string()
        }
        ServerError::AnonymousApiAccessDenied => {
            "Anonymous users cannot invoke this API. Please authenticate.".to_string()
        }
        ServerError::NoSuchUpload { .. } => {
            "The specified upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.".to_string()
        }
        ServerError::InvalidPart { part_number } => format!("invalid part: part {part_number}"),
        ServerError::InvalidPartOrder => "invalid part order".to_string(),
        ServerError::EntityTooSmall {
            part_number,
            size,
            min,
        } => format!("entity too small: part {part_number} is {size} bytes (min {min})"),
        ServerError::CompleteMultipartMissingPartChecksum {
            algorithm,
            part_number,
        } => format!(
            "The upload was created using a {algorithm} checksum. The complete request must include the checksum for each part. It was missing for part {part_number} in the request."
        ),
        ServerError::CompleteMultipartChecksumHeaderInvalid { header_name } => {
            format!("Value for {header_name} header is invalid.")
        }
        ServerError::UploadPartCopyInvalidRange {
            source_size, ..
        } => {
            format!("Range specified is not valid for source object of size: {source_size}")
        }
        ServerError::UploadPartCopyPreconditionFailed { .. } => {
            "At least one of the pre-conditions you specified did not hold".to_string()
        }
        ServerError::NotImplemented { feature } => feature.clone(),
        ServerError::HeaderNotImplemented { .. } => {
            "A header you provided implies functionality that is not implemented".to_string()
        }
        ServerError::QueryParameterNotImplemented { .. } => {
            "A query parameter you provided implies functionality that is not implemented"
                .to_string()
        }
        ServerError::XAmzContentSHA256Mismatch { .. } => {
            "The provided 'x-amz-content-sha256' header does not match what was computed."
                .to_string()
        }
        ServerError::IllegalVersioningConfiguration { reason } => reason.clone(),
        ServerError::IncompleteBody => "incomplete body".to_string(),
        ServerError::MissingContentLength => "missing content length".to_string(),
    }
}

fn current_request_id() -> String {
    TEST_REQUEST_ID.to_string()
}

impl S3Response {
    fn client_error_response_with_ids(
        err: &ServerError,
        resource: &str,
        wire_ids: &WireResponseIds,
    ) -> Self {
        let request_id = wire_ids.request_id();
        let host_id = wire_ids.host_id();
        match err {
            ServerError::BucketNotFound { name } => {
                let body = xml::no_such_bucket_error_xml(name, request_id, host_id);
                Self::new(404).chunked_xml_body(body)
            }
            ServerError::ObjectNotFound { key, .. } | ServerError::DeleteMarkerHit { key, .. } => {
                let body = xml::no_such_key_error_xml(key, request_id, host_id);
                Self::new(404).chunked_xml_body(body)
            }
            ServerError::HeadDeleteMarkerMethodNotAllowed {
                version_id,
                last_modified,
            } => Self::head_delete_marker_method_not_allowed(*version_id, *last_modified),
            ServerError::NoSuchBucketPolicy { bucket } => {
                let body = xml::no_such_bucket_policy_error_xml(bucket, request_id, host_id);
                Self::new(404).chunked_xml_body(body)
            }
            ServerError::AnonymousApiAccessDenied => {
                let body = xml::error_xml_with_host_id(
                    "AccessDenied",
                    &client_error_message(err),
                    request_id,
                    host_id,
                );
                Self::new(403).chunked_xml_body(body)
            }
            ServerError::BlockPublicPolicyAccessDenied {
                requester_principal,
                bucket,
            } => {
                let body = xml::error_xml_with_host_id(
                    "AccessDenied",
                    &format!(
                        "User: {} is not authorized to perform: s3:PutBucketPolicy on resource: \"arn:aws:s3:::{}\" because public policies are prevented by the BlockPublicPolicy setting in S3 Block Public Access.",
                        requester_principal, bucket
                    ),
                    request_id,
                    host_id,
                );
                Self::new(403).chunked_xml_body(body)
            }
            ServerError::SseCBlockedAccessDenied { .. } => {
                let body = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                     <Error>\
                     <Code>AccessDenied</Code>\
                     <Message>{}</Message>\
                     <RequestId>{}</RequestId>\
                     <HostId>{}</HostId>\
                     </Error>",
                    xml::xml_escape_text(&client_error_message(err)),
                    xml::xml_escape(request_id),
                    xml::xml_escape(host_id),
                );
                Self::new(403).chunked_xml_body(body)
            }
            ServerError::AccessDenied
            | ServerError::Auth(auth::AuthError::MissingAuth)
            | ServerError::Auth(auth::AuthError::AccessDenied)
            | ServerError::Auth(auth::AuthError::PresignedRequestExpired) => {
                let body = xml::error_xml_with_host_id(
                    "AccessDenied",
                    &client_error_message(err),
                    request_id,
                    host_id,
                );
                Self::new(403).chunked_xml_body(body)
            }
            ServerError::XAmzContentSHA256Mismatch {
                client_hash,
                server_hash,
            } => {
                let body = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                     <Error>\
                     <Code>XAmzContentSHA256Mismatch</Code>\
                     <Message>{}</Message>\
                     <ClientComputedContentSHA256>{}</ClientComputedContentSHA256>\
                     <S3ComputedContentSHA256>{}</S3ComputedContentSHA256>\
                     <Resource>{}</Resource>\
                     <RequestId>{}</RequestId>\
                     <HostId>{}</HostId>\
                     </Error>",
                    xml::xml_escape(&client_error_message(err)),
                    xml::xml_escape(client_hash),
                    xml::xml_escape(server_hash),
                    xml::xml_escape(resource),
                    xml::xml_escape(request_id),
                    xml::xml_escape(host_id),
                );
                Self::new(400).chunked_xml_body(body)
            }
            ServerError::Auth(auth::AuthError::UnsignedHeaders { headers }) => {
                let headers_str = headers.join(";");
                let body = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                     <Error>\
                     <Code>AccessDenied</Code>\
                     <Message>{}</Message>\
                     <HeadersNotSigned>{}</HeadersNotSigned>\
                     <Resource>{}</Resource>\
                     <RequestId>{}</RequestId>\
                     <HostId>{}</HostId>\
                     </Error>",
                    xml::xml_escape(&client_error_message(err)),
                    xml::xml_escape(&headers_str),
                    xml::xml_escape(resource),
                    xml::xml_escape(request_id),
                    xml::xml_escape(host_id),
                );
                Self::new(403).chunked_xml_body(body)
            }
            ServerError::Auth(auth::AuthError::UnexpectedSecurityToken { token }) => {
                let body = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                     <Error>\
                     <Code>InvalidToken</Code>\
                     <Message>{}</Message>\
                     <Token-0>{}</Token-0>\
                     <RequestId>{}</RequestId>\
                    <HostId>{}</HostId>\
                     </Error>",
                    xml::xml_escape(&client_error_message(err)),
                    xml::xml_escape(token),
                    xml::xml_escape(request_id),
                    xml::xml_escape(host_id),
                );
                Self::new(400).chunked_xml_body(body)
            }
            ServerError::Auth(auth::AuthError::DuplicateAuthorizationHeader) => {
                let body = xml::header_not_implemented_xml("Authorization", resource, request_id);
                Self::new(501).chunked_xml_body(body)
            }
            ServerError::MaxMessageLengthExceeded {
                max_message_length_bytes,
            } => {
                let body = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                     <Error>\
                     <Code>MaxMessageLengthExceeded</Code>\
                     <Message>{}</Message>\
                     <MaxMessageLengthBytes>{}</MaxMessageLengthBytes>\
                     <RequestId>{}</RequestId>\
                     <HostId>{}</HostId>\
                     </Error>",
                    xml::xml_escape(&client_error_message(err)),
                    max_message_length_bytes,
                    xml::xml_escape(request_id),
                    xml::xml_escape(host_id),
                );
                Self::new(400).chunked_xml_body(body)
            }
            ServerError::HeaderNotImplemented { header } => {
                let body = xml::header_not_implemented_xml(header, resource, request_id);
                Self::new(501).chunked_xml_body(body)
            }
            ServerError::QueryParameterNotImplemented { query_parameter } => {
                let body =
                    xml::query_parameter_not_implemented_xml(query_parameter, resource, request_id);
                Self::new(501).chunked_xml_body(body)
            }
            ServerError::InvalidSseCustomerKeyMd5
            | ServerError::MissingSseCustomerAlgorithm
            | ServerError::MissingSseCustomerKey
            | ServerError::MissingSseCustomerKeyMd5 => {
                let body = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                     <Error>\
                     <Code>InvalidArgument</Code>\
                     <Message>{}</Message>\
                     <ArgumentName>x-amz-server-side-encryption</ArgumentName>\
                     <RequestId>{}</RequestId>\
                     <HostId>{}</HostId>\
                    </Error>",
                    xml::xml_escape_text(&client_error_message(err)),
                    xml::xml_escape(request_id),
                    xml::xml_escape(host_id),
                );
                Self::new(400).chunked_xml_body(body)
            }
            ServerError::InvalidEncryptionAlgorithmError { value } => {
                let body = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                     <Error>\
                     <Code>InvalidEncryptionAlgorithmError</Code>\
                     <Message>{}</Message>\
                     <ArgumentName>x-amz-server-side-encryption</ArgumentName>\
                     <ArgumentValue>{}</ArgumentValue>\
                     <RequestId>{}</RequestId>\
                     <HostId>{}</HostId>\
                     </Error>",
                    xml::xml_escape_text(&client_error_message(err)),
                    xml::xml_escape(value),
                    xml::xml_escape(request_id),
                    xml::xml_escape(host_id),
                );
                Self::new(400).chunked_xml_body(body)
            }
            ServerError::DuplicateChecksumHeader { header, value } => {
                let body = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                     <Error>\
                     <Code>InvalidArgument</Code>\
                     <Message>{}</Message>\
                     <ArgumentName>{}</ArgumentName>\
                     <ArgumentValue>{}</ArgumentValue>\
                     <RequestId>{}</RequestId>\
                     <HostId>{}</HostId>\
                     </Error>",
                    xml::xml_escape_text(&client_error_message(err)),
                    xml::xml_escape(header),
                    xml::xml_escape(value),
                    xml::xml_escape(request_id),
                    xml::xml_escape(host_id),
                );
                Self::new(400).chunked_xml_body(body)
            }
            ServerError::WrongRegion {
                expected_region, ..
            } => {
                let body = xml::error_xml_with_region(
                    "AuthorizationHeaderMalformed",
                    &client_error_message(err),
                    request_id,
                    host_id,
                    expected_region,
                );
                Self::new(400)
                    .header("x-amz-bucket-region", expected_region)
                    .chunked_xml_body(body)
            }
            ServerError::InvalidBucketNamespace {
                bucket_namespace, ..
            } => {
                let body = xml::error_xml_with_bucket_namespace(
                    "InvalidBucketNamespace",
                    &client_error_message(err),
                    bucket_namespace,
                    request_id,
                );
                Self::new(400).chunked_xml_body(body)
            }
            ServerError::InvalidRedirectLocation { .. } => {
                let body = xml::error_xml_with_host_id(
                    err.s3_error_code(),
                    &client_error_message(err),
                    request_id,
                    host_id,
                );
                Self::new(400).chunked_xml_body(body)
            }
            ServerError::InvalidPolicyDocument { .. } => {
                let body = xml::error_xml_with_host_id(
                    "InvalidPolicyDocument",
                    &client_error_message(err),
                    request_id,
                    host_id,
                );
                Self::new(400).chunked_xml_body(body)
            }
            ServerError::InvalidRequest { reason } if is_host_id_invalid_request(reason) => {
                let body = xml::error_xml_with_host_id(
                    "InvalidRequest",
                    &client_error_message(err),
                    request_id,
                    host_id,
                );
                Self::new(400).chunked_xml_body(body)
            }
            ServerError::KeyTooLongError {
                size,
                max_size_allowed,
            } => {
                let body = xml::key_too_long_error_xml(*size, *max_size_allowed, request_id);
                Self::new(400).chunked_xml_body(body)
            }
            ServerError::MetadataTooLargeDetailed {
                size,
                max_size_allowed,
            } => {
                let body = xml::metadata_too_large_error_xml(
                    *size,
                    *max_size_allowed,
                    request_id,
                    host_id,
                );
                Self::new(400).chunked_xml_body(body)
            }
            ServerError::RequestHeaderSectionTooLarge => {
                let body = xml::request_header_section_too_large_error_xml(
                    super::MAX_WRITE_REQUEST_HEADER_SECTION_SIZE,
                    request_id,
                    host_id,
                );
                Self::new(400).chunked_xml_body(body)
            }
            ServerError::ChecksumDigestMismatch { .. } => {
                let body = xml::error_xml_with_host_id(
                    "BadDigest",
                    &client_error_message(err),
                    request_id,
                    host_id,
                );
                Self::new(400).chunked_xml_body(body)
            }
            ServerError::PostPolicyAccessDenied { reason } => {
                let body = xml::error_xml_with_host_id("AccessDenied", reason, request_id, host_id);
                Self::new(403).chunked_xml_body(body)
            }
            ServerError::NoSuchUpload { upload_id } => {
                let body = xml::no_such_upload_error_xml(upload_id, request_id, host_id);
                Self::new(404).chunked_xml_body(body)
            }
            ServerError::MalformedXML { .. } => {
                let body = xml::error_xml_with_host_id(
                    err.s3_error_code(),
                    &client_error_message(err),
                    request_id,
                    host_id,
                );
                Self::new(400).chunked_xml_body(body)
            }
            ServerError::Auth(auth::AuthError::InvalidQueryCredentialRegion {
                expected_region,
                ..
            }) => {
                let body = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                     <Error>\
                     <Code>AuthorizationQueryParametersError</Code>\
                     <Message>{}</Message>\
                     <Region>{}</Region>\
                     <RequestId>{}</RequestId>\
                     <HostId>{}</HostId>\
                     </Error>",
                    xml::xml_escape_text(&client_error_message(err)),
                    xml::xml_escape(expected_region),
                    xml::xml_escape(request_id),
                    xml::xml_escape(host_id),
                );
                Self::new(400).chunked_xml_body(body)
            }
            ServerError::Auth(auth::AuthError::InvalidQueryCredentialService { .. }) => {
                let body = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                     <Error>\
                     <Code>AuthorizationQueryParametersError</Code>\
                     <Message>{}</Message>\
                     <RequestId>{}</RequestId>\
                     <HostId>{}</HostId>\
                     </Error>",
                    xml::xml_escape_text(&client_error_message(err)),
                    xml::xml_escape(request_id),
                    xml::xml_escape(host_id),
                );
                Self::new(400).chunked_xml_body(body)
            }
            ServerError::Auth(auth::AuthError::InvalidCredentialScopeRegion {
                param,
                credential,
                expected_region,
                ..
            }) => {
                let body = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                     <Error>\
                     <Code>InvalidArgument</Code>\
                     <Message>{}</Message>\
                     <ArgumentName>{}</ArgumentName>\
                     <ArgumentValue>{}</ArgumentValue>\
                     <Region>{}</Region>\
                     <RequestId>{}</RequestId>\
                     <HostId>{}</HostId>\
                     </Error>",
                    xml::xml_escape_text(&client_error_message(err)),
                    xml::xml_escape(param),
                    xml::xml_escape(credential),
                    xml::xml_escape(expected_region),
                    xml::xml_escape(request_id),
                    xml::xml_escape(host_id),
                );
                Self::new(400).chunked_xml_body(body)
            }
            ServerError::Auth(auth::AuthError::InvalidCredentialScopeService {
                param,
                credential,
                ..
            }) => {
                let body = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                     <Error>\
                     <Code>InvalidArgument</Code>\
                     <Message>{}</Message>\
                     <ArgumentName>{}</ArgumentName>\
                     <ArgumentValue>{}</ArgumentValue>\
                     <RequestId>{}</RequestId>\
                     <HostId>{}</HostId>\
                     </Error>",
                    xml::xml_escape_text(&client_error_message(err)),
                    xml::xml_escape(param),
                    xml::xml_escape(credential),
                    xml::xml_escape(request_id),
                    xml::xml_escape(host_id),
                );
                Self::new(400).chunked_xml_body(body)
            }
            ServerError::CompleteMultipartMissingPartChecksum {
                algorithm,
                part_number,
            } => {
                let body = xml::complete_multipart_missing_part_checksum_error_xml(
                    algorithm,
                    *part_number,
                    request_id,
                    host_id,
                );
                Self::new(400).chunked_xml_body(body)
            }
            ServerError::CompleteMultipartChecksumHeaderInvalid { header_name } => {
                let body = xml::complete_multipart_checksum_header_invalid_error_xml(
                    header_name,
                    request_id,
                    host_id,
                );
                Self::new(400).chunked_xml_body(body)
            }
            ServerError::UploadPartCopyInvalidRange {
                range_header,
                source_size,
            } => {
                let body = xml::upload_part_copy_invalid_range_error_xml(
                    range_header,
                    *source_size,
                    request_id,
                    host_id,
                );
                Self::new(400).chunked_xml_body(body)
            }
            ServerError::UploadPartCopyPreconditionFailed { condition } => {
                let body = xml::upload_part_copy_precondition_failed_error_xml(
                    condition, request_id, host_id,
                );
                Self::new(412).chunked_xml_body(body)
            }
            _ => {
                let body = xml::error_xml(
                    err.s3_error_code(),
                    &client_error_message(err),
                    resource,
                    request_id,
                );
                let resp = Self::new(err.http_status()).chunked_xml_body(body);
                if matches!(err, ServerError::SlowDown) {
                    resp.header("Retry-After", "1")
                } else {
                    resp
                }
            }
        }
    }

    fn new(status_code: u16) -> Self {
        Self {
            status_code,
            headers: Vec::new(),
            body: Vec::new(),
            stream: None,
            error_diagnostic: None,
        }
    }

    fn with_error_diagnostic(mut self, err: &ServerError) -> Self {
        self.error_diagnostic = Some(ErrorDiagnostic {
            status_code: err.http_status(),
            error_code: err.s3_error_code(),
            cause_label: err.diagnostic_cause_label(),
            cause_chain: err.diagnostic_cause_chain(),
            server_detail: err.server_storage_rpc_detail(),
        });
        self
    }

    fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_string(), value.to_string()));
        self
    }

    /// Set a metadata header, RFC 2047 encoding the value if it contains
    /// non-ASCII bytes (matching AWS S3 behavior).
    fn meta_header(self, name: &str, value: &str) -> Self {
        if value.is_ascii() {
            self.header(name, value)
        } else {
            self.header(name, &rfc2047_encode(value))
        }
    }

    fn xml_body(mut self, xml: String) -> Self {
        self.body = xml.into_bytes();
        self.stream = None;
        self.headers
            .push(("Content-Type".to_string(), "application/xml".to_string()));
        self.headers
            .push(("Content-Length".to_string(), self.body.len().to_string()));
        self
    }

    fn fixed_body(mut self, body: Vec<u8>) -> Self {
        self.body = body;
        self.stream = None;
        self.headers
            .push(("Content-Length".to_string(), self.body.len().to_string()));
        self
    }

    fn fixed_xml_body_no_content_type(self, xml: String) -> Self {
        self.fixed_body(xml.into_bytes())
    }

    fn chunked_xml_body_no_content_type(self, xml: String) -> Self {
        self.chunked_body(xml.into_bytes())
    }

    fn chunked_body(mut self, body: Vec<u8>) -> Self {
        self.body = Vec::new();
        self.stream = Some(ReadHandle::from_buffered_bytes(body));
        self
    }

    fn chunked_xml_body(self, xml: String) -> Self {
        self.header("Content-Type", "application/xml")
            .chunked_body(xml.into_bytes())
    }

    fn json_body(mut self, json: String) -> Self {
        self.body = json.into_bytes();
        self.stream = None;
        self.headers
            .push(("Content-Type".to_string(), "application/json".to_string()));
        self.headers
            .push(("Content-Length".to_string(), self.body.len().to_string()));
        self
    }

    fn streaming_body(mut self, body: ReadHandle, content_length: u64) -> Self {
        self.headers
            .push(("Content-Length".to_string(), content_length.to_string()));
        self.body = Vec::new();
        self.stream = Some(body);
        self
    }

    fn apply_sse_customer_headers(
        mut self,
        sse_customer: Option<&SseCustomerResponseHeaders>,
    ) -> Self {
        let Some(sse_customer) = sse_customer else {
            return self;
        };
        self = self.header(
            "x-amz-server-side-encryption-customer-algorithm",
            SSE_CUSTOMER_ALGORITHM,
        );
        self.header(
            "x-amz-server-side-encryption-customer-key-md5",
            &sse_customer.key_md5_b64,
        )
    }

    fn apply_managed_encryption_headers(
        self,
        managed_encryption: Option<ManagedEncryptionAlgorithm>,
    ) -> Self {
        let Some(managed_encryption) = managed_encryption else {
            return self;
        };
        self.header("x-amz-server-side-encryption", managed_encryption.as_str())
    }

    fn apply_system_metadata_headers(mut self, metadata: &SystemMetadata) -> Self {
        if let Some(content_type) = metadata.content_type() {
            self = self.header("Content-Type", content_type.as_str());
        } else {
            self = self.header("Content-Type", "binary/octet-stream");
        }
        if let Some(content_encoding) = metadata.content_encoding() {
            self = self.header("Content-Encoding", content_encoding.as_str());
        }
        if let Some(cache_control) = metadata.cache_control() {
            self = self.header("Cache-Control", cache_control.as_str());
        }
        if let Some(content_disposition) = metadata.content_disposition() {
            self = self.header("Content-Disposition", content_disposition.as_str());
        }
        if let Some(content_language) = metadata.content_language() {
            self = self.header("Content-Language", content_language.as_str());
        }
        if let Some(expires) = metadata.expires() {
            self = self.header("Expires", expires.as_str());
        }
        if let Some(redirect) = metadata.website_redirect_location() {
            self = self.header("x-amz-website-redirect-location", redirect.as_str());
        }
        self
    }

    fn apply_user_metadata_headers(
        mut self,
        metadata: &crate::metadata_blob::MetadataBlob,
    ) -> Self {
        for entry in metadata.iter() {
            if entry.key.starts_with("x-amz-meta-") {
                self = self.meta_header(&entry.key, &entry.value);
            }
        }
        self
    }

    fn apply_object_lock_headers(mut self, object_lock: ObjectLockState) -> Self {
        if let Some(retention) = object_lock.retention {
            self = self.header("x-amz-object-lock-mode", retention.mode.as_str());
            self = self.header(
                "x-amz-object-lock-retain-until-date",
                &format_object_lock_header_timestamp(retention.retain_until_unix_seconds),
            );
        }
        if let Some(legal_hold) = object_lock.legal_hold.as_legal_hold_status() {
            self = self.header("x-amz-object-lock-legal-hold", legal_hold.as_str());
        }
        self
    }

    fn apply_checksum_mode_headers(mut self, metadata: &SystemMetadata) -> Self {
        for (name, value) in metadata.checksum_header_pairs() {
            self = self.header(name, value);
        }
        self
    }

    fn apply_lifecycle_expiration_header(
        self,
        expiration: Option<&LifecycleExpirationHeader>,
    ) -> Self {
        let Some(expiration) = expiration else {
            return self;
        };
        let value = match &expiration.rule_id {
            Some(rule_id) => format!(
                "expiry-date=\"{}\", rule-id=\"{}\"",
                format_http_date(expiration.expiry_time_millis),
                uri_encode(rule_id)
            ),
            None => format!(
                "expiry-date=\"{}\"",
                format_http_date(expiration.expiry_time_millis)
            ),
        };
        self.header("x-amz-expiration", &value)
    }

    fn apply_lifecycle_abort_headers(mut self, abort: Option<&LifecycleAbortHeaders>) -> Self {
        let Some(abort) = abort else {
            return self;
        };
        self = self.header(
            "x-amz-abort-date",
            &format_http_date(abort.abort_time_millis),
        );
        if let Some(rule_id) = &abort.rule_id {
            self = self.header("x-amz-abort-rule-id", &uri_encode(rule_id));
        }
        self
    }

    /// Build a response for a successful `PutObject`.
    #[must_use]
    pub fn put_object(result: &PutObjectResult) -> Self {
        let mut resp = Self::new(200).header("ETag", &result.etag);
        if result.version_id.is_versioned() {
            let vid = format_version_id(result.version_id);
            resp = resp.header("x-amz-version-id", &vid);
        }
        resp.apply_checksum_mode_headers(&result.system_metadata)
            .apply_lifecycle_expiration_header(result.lifecycle_expiration.as_ref())
            .apply_managed_encryption_headers(result.managed_encryption)
    }

    /// Build a response for a successful POST Object.
    ///
    /// `success_status`: one of 200, 201, 204 (default).
    /// For 201, an XML body with bucket/key/etag is returned. If
    /// `success_redirect` is present and valid, return a 303 redirect instead.
    #[must_use]
    pub fn post_object(
        result: &PutObjectResult,
        bucket: &str,
        key: &str,
        success_status: u16,
        success_redirect: Option<&str>,
        location: Option<&str>,
    ) -> Self {
        let mut resp = if let Some(location) = success_redirect.and_then(|redirect_url| {
            success_action_redirect_location(redirect_url, bucket, key, &result.etag)
        }) {
            Self::new(303).header("Location", &location)
        } else {
            let status = match success_status {
                200 | 201 => success_status,
                _ => 204,
            };
            if status == 201 {
                let body = xml::post_response_xml(bucket, key, &result.etag);
                Self::new(201).xml_body(body)
            } else {
                Self::new(status)
            }
        };
        if success_redirect.is_none() {
            if let Some(location) = location {
                resp = resp.header("Location", location);
            }
            resp = resp.apply_checksum_mode_headers(&result.system_metadata);
            if let Some(checksum_type) = result.system_metadata.checksum_type() {
                resp = resp.header("x-amz-checksum-type", checksum_type.as_str());
            }
        }
        resp = resp.header("ETag", &result.etag);
        if result.version_id.is_versioned() {
            let vid = format_version_id(result.version_id);
            resp = resp.header("x-amz-version-id", &vid);
        }
        resp.apply_lifecycle_expiration_header(result.lifecycle_expiration.as_ref())
            .apply_managed_encryption_headers(result.managed_encryption)
    }

    /// Build a response for a successful `CopyObject`.
    #[must_use]
    pub fn copy_object(result: &CopyObjectResult) -> Self {
        let body = xml::copy_object_result_xml(
            &result.etag,
            result.last_modified,
            &result.system_metadata,
        );
        let mut resp = Self::new(200).xml_body(body);
        if result.version_id.is_versioned() {
            let vid = format_version_id(result.version_id);
            resp.headers.push(("x-amz-version-id".to_string(), vid));
        }
        resp.apply_lifecycle_expiration_header(result.lifecycle_expiration.as_ref())
            .apply_managed_encryption_headers(result.managed_encryption)
            .apply_sse_customer_headers(result.sse_customer.as_ref())
    }

    /// Build a response for a successful `GetObject`.
    /// If `checksum_mode` is `Some("ENABLED")`, include stored checksum headers.
    #[must_use]
    pub fn get_object(result: GetObjectResult, checksum_mode: Option<&str>) -> Self {
        let mut resp = Self::new(200)
            .header("ETag", &result.etag)
            .header("Last-Modified", &format_http_date(result.last_modified))
            .header("Accept-Ranges", "bytes");
        if result.version_id.is_versioned() {
            let vid = format_version_id(result.version_id);
            resp = resp.header("x-amz-version-id", &vid);
        }
        resp = resp
            .apply_system_metadata_headers(&result.system_metadata)
            .apply_user_metadata_headers(&result.metadata)
            .apply_object_lock_headers(result.object_lock);

        // Checksum headers (only when ChecksumMode=ENABLED)
        if checksum_mode.is_some_and(|m| m.eq_ignore_ascii_case("ENABLED")) {
            resp = resp.apply_checksum_mode_headers(&result.system_metadata);
        }

        resp.apply_lifecycle_expiration_header(result.lifecycle_expiration.as_ref())
            .apply_managed_encryption_headers(result.managed_encryption)
            .apply_sse_customer_headers(result.sse_customer.as_ref())
            .streaming_body(result.body, result.size)
    }

    #[cfg(test)]
    pub(crate) fn into_test_body_bytes(self) -> Result<Vec<u8>, ServerError> {
        match self.stream {
            Some(mut stream) => {
                let mut out = Vec::new();
                while let Some(chunk) =
                    stream.next_chunk(crate::coordinator::INTERNAL_SEGMENT_SIZE)?
                {
                    out.extend_from_slice(&chunk);
                }
                Ok(out)
            }
            None => Ok(self.body),
        }
    }

    /// Build a response for a successful `HeadObject`.
    /// If `checksum_mode` is `Some("ENABLED")`, include stored checksum headers.
    #[must_use]
    pub fn head_object(result: &HeadObjectResult, checksum_mode: Option<&str>) -> Self {
        let mut resp = Self::new(200)
            .header("ETag", &result.etag)
            .header("Content-Length", &result.size.to_string())
            .header("Last-Modified", &format_http_date(result.last_modified))
            .header("Accept-Ranges", "bytes");
        if result.version_id.is_versioned() {
            let vid = format_version_id(result.version_id);
            resp = resp.header("x-amz-version-id", &vid);
        }
        resp = resp
            .apply_system_metadata_headers(&result.system_metadata)
            .apply_user_metadata_headers(&result.metadata)
            .apply_object_lock_headers(result.object_lock);

        // Checksum headers (only when ChecksumMode=ENABLED)
        if checksum_mode.is_some_and(|m| m.eq_ignore_ascii_case("ENABLED")) {
            resp = resp.apply_checksum_mode_headers(&result.system_metadata);
        }

        resp.apply_lifecycle_expiration_header(result.lifecycle_expiration.as_ref())
            .apply_managed_encryption_headers(result.managed_encryption)
            .apply_sse_customer_headers(result.sse_customer.as_ref())
    }

    /// Build a response for `HeadObject` with partNumber.
    #[must_use]
    pub fn head_object_part(result: &HeadObjectPartResult) -> Self {
        let mut resp = Self::new(206)
            .header("ETag", &result.etag)
            .header("Content-Length", &result.part_size.to_string())
            .header("Last-Modified", &format_http_date(result.last_modified))
            .header("Accept-Ranges", "bytes")
            .header("x-amz-mp-parts-count", &result.parts_count.to_string());
        if result.part_size != 0 {
            let content_range = format!(
                "bytes {}-{}/{}",
                result.part_start, result.part_end, result.total_size
            );
            resp = resp.header("Content-Range", &content_range);
        }
        if result.version_id.is_versioned() {
            let vid = format_version_id(result.version_id);
            resp = resp.header("x-amz-version-id", &vid);
        }
        resp = resp
            .apply_system_metadata_headers(&result.system_metadata)
            .apply_user_metadata_headers(&result.metadata)
            .apply_object_lock_headers(result.object_lock);

        // Per-part checksum (always emitted for part-level requests)
        if let Some(ref cksum) = result.checksum {
            use base64::Engine;
            let b64 = base64::engine::general_purpose::STANDARD.encode(cksum.bytes());
            resp = resp.header(cksum.algorithm().header_name(), &b64);
        }
        if let Some(checksum_type) = result.system_metadata.checksum_type() {
            resp = resp.header("x-amz-checksum-type", checksum_type.as_str());
        }

        resp.apply_lifecycle_expiration_header(result.lifecycle_expiration.as_ref())
            .apply_managed_encryption_headers(result.managed_encryption)
    }

    /// Build a response for a successful range `GetObject` (206 Partial Content).
    #[must_use]
    pub fn get_object_range(result: GetObjectRangeResult) -> Self {
        let content_range = format!(
            "bytes {}-{}/{}",
            result.range_start, result.range_end, result.size
        );
        let mut resp = Self::new(206)
            .header("ETag", &result.etag)
            .header("Last-Modified", &format_http_date(result.last_modified))
            .header("Accept-Ranges", "bytes")
            .header("Content-Range", &content_range);
        if result.version_id.is_versioned() {
            let vid = format_version_id(result.version_id);
            resp = resp.header("x-amz-version-id", &vid);
        }
        resp = resp
            .apply_system_metadata_headers(&result.system_metadata)
            .apply_user_metadata_headers(&result.metadata)
            .apply_object_lock_headers(result.object_lock);

        resp.apply_lifecycle_expiration_header(result.lifecycle_expiration.as_ref())
            .apply_managed_encryption_headers(result.managed_encryption)
            .apply_sse_customer_headers(result.sse_customer.as_ref())
            .streaming_body(result.body, result.range_end - result.range_start + 1)
    }

    /// Build a response for a part-level `GetObject` (206 Partial Content).
    /// Per-part checksum and checksum-type are always emitted (Ceph/AWS
    /// return them without requiring ChecksumMode=ENABLED on part GETs).
    #[must_use]
    pub fn get_object_part(result: GetObjectPartResult) -> Self {
        let mut resp = Self::new(206)
            .header("ETag", &result.etag)
            .header("Last-Modified", &format_http_date(result.last_modified))
            .header("Accept-Ranges", "bytes")
            .header("x-amz-mp-parts-count", &result.parts_count.to_string());
        // Only emit Content-Range for non-empty parts; a zero-byte part has
        // no valid byte range to express.
        if result.part_size != 0 {
            let content_range = format!(
                "bytes {}-{}/{}",
                result.part_start, result.part_end, result.size
            );
            resp = resp.header("Content-Range", &content_range);
        }
        if result.version_id.is_versioned() {
            let vid = format_version_id(result.version_id);
            resp = resp.header("x-amz-version-id", &vid);
        }
        resp = resp
            .apply_system_metadata_headers(&result.system_metadata)
            .apply_user_metadata_headers(&result.metadata)
            .apply_object_lock_headers(result.object_lock);

        // Per-part checksum (always emitted for part-level GETs)
        if let Some(ref cksum) = result.checksum {
            use base64::Engine;
            let b64 = base64::engine::general_purpose::STANDARD.encode(cksum.bytes());
            resp = resp.header(cksum.algorithm().header_name(), &b64);
        }
        // Checksum type (e.g. COMPOSITE, FULL_OBJECT)
        if let Some(checksum_type) = result.system_metadata.checksum_type() {
            resp = resp.header("x-amz-checksum-type", checksum_type.as_str());
        }

        resp.apply_lifecycle_expiration_header(result.lifecycle_expiration.as_ref())
            .apply_managed_encryption_headers(result.managed_encryption)
            .apply_sse_customer_headers(result.sse_customer.as_ref())
            .streaming_body(result.body, result.part_size)
    }

    /// Build a 416 Range Not Satisfiable response.
    #[must_use]
    pub fn range_not_satisfiable(_total_size: u64) -> Self {
        let request_id = current_request_id();
        let body = xml::error_xml(
            "InvalidRange",
            "The requested range is not satisfiable",
            "",
            &request_id,
        );
        Self::new(416).chunked_xml_body(body)
    }

    /// Build a 416 Range Not Satisfiable response with explicit wire IDs.
    #[must_use]
    pub fn range_not_satisfiable_with_ids(_total_size: u64, wire_ids: &WireResponseIds) -> Self {
        let body = xml::error_xml(
            "InvalidRange",
            "The requested range is not satisfiable",
            "",
            wire_ids.request_id(),
        );
        Self::new(416).chunked_xml_body(body)
    }

    /// Build a response for `DeleteObject` (204 No Content).
    #[must_use]
    pub fn delete_object(result: &DeleteObjectResult) -> Self {
        let mut resp = Self::new(204);
        if result.version_id.is_versioned() {
            let vid = format_version_id(result.version_id);
            resp = resp.header("x-amz-version-id", &vid);
        }
        if result.delete_marker {
            resp = resp.header("x-amz-delete-marker", "true");
        }
        resp
    }

    /// Build a response for `HeadObject` against a delete-marker version.
    #[must_use]
    pub fn head_delete_marker_method_not_allowed(
        version_id: VersionId,
        last_modified: u64,
    ) -> Self {
        Self::new(405)
            .header("Allow", "DELETE")
            .header("x-amz-delete-marker", "true")
            .header("x-amz-version-id", &format_version_id(version_id))
            .header("Last-Modified", &format_http_date(last_modified))
    }

    /// Build a response for `CreateBucket`.
    #[must_use]
    pub fn create_bucket(location: &str) -> Self {
        Self::new(200).header("Location", &format!("/{location}"))
    }

    /// Build a response for `DeleteBucket`.
    #[must_use]
    pub fn delete_bucket() -> Self {
        Self::new(204)
    }

    /// Build a response for `HeadBucket`.
    #[must_use]
    pub fn head_bucket(info: &BucketSummary, region: &str) -> Self {
        Self::new(200)
            .header("Content-Type", "application/xml")
            .header("x-amz-access-point-alias", "false")
            .header("x-amz-bucket-arn", &format!("arn:aws:s3:::{}", info.name))
            .header("x-amz-bucket-region", region)
    }

    /// Build a response for `GetBucketLocation`.
    #[must_use]
    pub fn get_bucket_location(region: &str) -> Self {
        let body = xml::get_bucket_location_xml(bucket_location_constraint(region));
        Self::new(200).chunked_xml_body(body)
    }

    /// Build a response for `PutBucketVersioning`.
    #[must_use]
    pub fn put_bucket_versioning() -> Self {
        Self::new(200)
    }

    /// Build a response for `GetBucketVersioning`.
    #[must_use]
    pub fn get_bucket_versioning(state: BucketVersioningState) -> Self {
        let body = xml::get_bucket_versioning_xml(state);
        Self::new(200).chunked_xml_body_no_content_type(body)
    }

    /// Build a response for `PutObjectLockConfiguration`.
    #[must_use]
    pub fn put_bucket_object_lock_configuration() -> Self {
        Self::new(200)
    }

    /// Build a response for `GetObjectLockConfiguration`.
    #[must_use]
    pub fn get_bucket_object_lock_configuration(config: BucketObjectLockConfig) -> Self {
        let body = xml::get_bucket_object_lock_configuration_xml(config);
        Self::new(200).chunked_xml_body_no_content_type(body)
    }

    /// Build a response for `PutObjectRetention`.
    #[must_use]
    pub fn put_object_retention() -> Self {
        Self::new(200)
    }

    /// Build a response for `GetObjectRetention`.
    #[must_use]
    pub fn get_object_retention(retention: Option<ObjectRetention>) -> Self {
        let body = xml::get_object_retention_xml(retention);
        Self::new(200).chunked_xml_body_no_content_type(body)
    }

    /// Build a response for `PutObjectLegalHold`.
    #[must_use]
    pub fn put_object_legal_hold() -> Self {
        Self::new(200)
    }

    /// Build a response for `GetObjectLegalHold`.
    #[must_use]
    pub fn get_object_legal_hold(status: Option<LegalHoldStatus>) -> Self {
        let body = xml::get_object_legal_hold_xml(status);
        Self::new(200).chunked_xml_body_no_content_type(body)
    }

    /// Build a response for `PutBucketEncryption`.
    #[must_use]
    pub fn put_bucket_encryption() -> Self {
        Self::new(200)
    }

    /// Build a response for `DeleteBucketEncryption`.
    #[must_use]
    pub fn delete_bucket_encryption() -> Self {
        Self::new(204)
    }

    /// Build a response for `GetBucketEncryption`.
    #[must_use]
    pub fn get_bucket_encryption(config: EffectiveBucketEncryptionConfig) -> Self {
        let body = xml::get_bucket_encryption_xml(config);
        Self::new(200).chunked_xml_body_no_content_type(body)
    }

    /// Build a response for `ListBuckets`.
    #[must_use]
    pub fn list_buckets(
        buckets: &[BucketSummary],
        owner_display_name: &str,
        owner_canonical_id: &CanonicalUserId,
    ) -> Self {
        let body = xml::list_buckets_xml(buckets, owner_display_name, owner_canonical_id);
        Self::new(200).xml_body(body)
    }

    /// Build a response for `ListObjectsV2`.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn list_objects_v2(
        bucket: &str,
        region: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        encoding_type: Option<&str>,
        continuation_token: Option<&str>,
        start_after: Option<&str>,
        fetch_owner: bool,
        max_keys: u32,
        result: &ListObjectsResult,
    ) -> Self {
        let body = xml::list_objects_v2_xml(
            bucket,
            prefix,
            delimiter,
            encoding_type,
            continuation_token,
            start_after,
            fetch_owner,
            max_keys,
            result,
        );
        Self::new(200)
            .header("x-amz-bucket-region", region)
            .chunked_xml_body(body)
    }

    /// Build a response for `ListObjects` v1.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn list_objects_v1(
        bucket: &str,
        region: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        marker: Option<&str>,
        encoding_type: Option<&str>,
        max_keys: u32,
        result: &ListObjectsResult,
    ) -> Self {
        let body = xml::list_objects_v1_xml(
            bucket,
            prefix,
            delimiter,
            marker,
            encoding_type,
            max_keys,
            result,
        );
        Self::new(200)
            .header("x-amz-bucket-region", region)
            .chunked_xml_body(body)
    }

    /// Build a response for `DeleteObjects` (batch delete).
    #[must_use]
    pub fn delete_objects(result: &DeleteObjectsResult, quiet: bool) -> Self {
        let body = xml::delete_objects_result_xml(&result.deleted, &result.errors, quiet);
        Self::new(200).chunked_xml_body(body)
    }

    /// Build a response for `ListObjectVersions`.
    #[must_use]
    pub fn list_object_versions(
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        key_marker: Option<&str>,
        encoding_type: Option<&str>,
        max_keys: u32,
        result: &ListObjectVersionsResult,
    ) -> Self {
        let body = xml::list_object_versions_xml(
            bucket,
            prefix,
            delimiter,
            key_marker,
            encoding_type,
            max_keys,
            result,
        );
        Self::new(200).chunked_xml_body(body)
    }

    /// Build a 304 Not Modified response with `ETag` and Last-Modified headers, no body.
    #[must_use]
    pub fn not_modified(etag: &str, last_modified: u64) -> Self {
        Self::new(304)
            .header("ETag", etag)
            .header("Last-Modified", &format_http_date(last_modified))
    }

    /// Build a 412 Precondition Failed response with XML error body.
    #[must_use]
    pub fn precondition_failed() -> Self {
        let request_id = current_request_id();
        let body = xml::error_xml(
            "PreconditionFailed",
            "At least one of the pre-conditions you specified did not hold",
            "",
            &request_id,
        );
        Self::new(412).chunked_xml_body(body)
    }

    /// Build a 412 Precondition Failed response with explicit wire IDs.
    #[must_use]
    pub fn precondition_failed_with_ids(wire_ids: &WireResponseIds) -> Self {
        let body = xml::error_xml(
            "PreconditionFailed",
            "At least one of the pre-conditions you specified did not hold",
            "",
            wire_ids.request_id(),
        );
        Self::new(412).chunked_xml_body(body)
    }

    /// Build a response for `PutBucketCors` (200 OK, no body).
    #[must_use]
    pub fn put_bucket_cors() -> Self {
        Self::new(200)
    }

    /// Build a response for `GetBucketCors` (200 OK, XML body).
    #[must_use]
    pub fn get_bucket_cors(config_xml: &str) -> Self {
        Self::new(200).chunked_xml_body_no_content_type(config_xml.to_string())
    }

    /// Build a response for `DeleteBucketCors` (204 No Content).
    #[must_use]
    pub fn delete_bucket_cors() -> Self {
        Self::new(204)
    }

    /// Build a response for `PutBucketTagging` (204 No Content).
    #[must_use]
    pub fn put_bucket_tagging() -> Self {
        Self::new(204)
    }

    /// Build a response for `GetBucketTagging` (200 OK, XML body).
    #[must_use]
    pub fn get_bucket_tagging(xml: &str) -> Self {
        Self::new(200).chunked_xml_body_no_content_type(xml.to_string())
    }

    /// Build a response for `DeleteBucketTagging` (204 No Content).
    #[must_use]
    pub fn delete_bucket_tagging() -> Self {
        Self::new(204)
    }

    /// Build a response for `TagResource` (204 No Content).
    #[must_use]
    pub fn tag_resource() -> Self {
        Self::new(204)
    }

    /// Build a response for `UntagResource` (204 No Content).
    #[must_use]
    pub fn untag_resource() -> Self {
        Self::new(204)
    }

    /// Build a response for `PutBucketAbac` (200 OK, no body).
    #[must_use]
    pub fn put_bucket_abac() -> Self {
        Self::new(200)
    }

    /// Build a response for `GetBucketAbac` (200 OK, XML body).
    #[must_use]
    pub fn get_bucket_abac(xml: &str) -> Self {
        Self::new(200).chunked_xml_body_no_content_type(xml.to_string())
    }

    /// Build a response for `PutBucketLifecycleConfiguration` (200 OK, no body).
    #[must_use]
    pub fn put_bucket_lifecycle() -> Self {
        Self::new(200)
    }

    /// Build a response for `GetBucketLifecycleConfiguration` (200 OK, XML body).
    #[must_use]
    pub fn get_bucket_lifecycle(xml: &str) -> Self {
        Self::new(200)
            .header(
                "x-amz-transition-default-minimum-object-size",
                "all_storage_classes_128K",
            )
            .fixed_xml_body_no_content_type(xml.to_string())
    }

    /// Build a response for `DeleteBucketLifecycle` (204 No Content).
    #[must_use]
    pub fn delete_bucket_lifecycle() -> Self {
        Self::new(204)
    }

    /// Build a response for `PutObjectTagging` (200 OK, no body).
    #[must_use]
    pub fn put_object_tagging() -> Self {
        Self::new(200)
    }

    /// Build a response for `GetObjectTagging` (200 OK, XML body).
    #[must_use]
    pub fn get_object_tagging(xml: &str) -> Self {
        Self::new(200).chunked_xml_body_no_content_type(xml.to_string())
    }

    /// Build a response for `DeleteObjectTagging` (204 No Content).
    #[must_use]
    pub fn delete_object_tagging() -> Self {
        Self::new(204)
    }

    /// Build a response for `PutBucketPublicAccessBlock` (200 OK, no body).
    #[must_use]
    pub fn put_bucket_public_access_block() -> Self {
        Self::new(200)
    }

    /// Build a response for `GetBucketPublicAccessBlock` (200 OK, XML body).
    #[must_use]
    pub fn get_bucket_public_access_block(config_xml: &str) -> Self {
        Self::new(200).chunked_xml_body_no_content_type(config_xml.to_string())
    }

    /// Build a response for `DeleteBucketPublicAccessBlock` (204 No Content).
    #[must_use]
    pub fn delete_bucket_public_access_block() -> Self {
        Self::new(204)
    }

    /// Build a response for `PutBucketOwnershipControls` (200 OK, no body).
    #[must_use]
    pub fn put_bucket_ownership_controls() -> Self {
        Self::new(200)
    }

    /// Build a response for `GetBucketOwnershipControls` (200 OK, XML body).
    #[must_use]
    pub fn get_bucket_ownership_controls(config_xml: &str) -> Self {
        Self::new(200).fixed_xml_body_no_content_type(config_xml.to_string())
    }

    /// Build a response for `DeleteBucketOwnershipControls` (204 No Content).
    #[must_use]
    pub fn delete_bucket_ownership_controls() -> Self {
        Self::new(204)
    }

    /// Build a response for `PutBucketPolicy` (204 No Content).
    #[must_use]
    pub fn put_bucket_policy() -> Self {
        Self::new(204)
    }

    /// Build a response for `GetBucketPolicy` (200 OK, JSON body).
    #[must_use]
    pub fn get_bucket_policy(policy: &str) -> Self {
        Self::new(200).json_body(policy.to_string())
    }

    /// Build a response for `GetBucketPolicyStatus` (200 OK, XML body).
    #[must_use]
    pub fn get_bucket_policy_status(is_public: bool) -> Self {
        let is_public = if is_public { "true" } else { "false" };
        Self::new(200).xml_body(format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><PolicyStatus xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><IsPublic>{is_public}</IsPublic></PolicyStatus>"
        ))
    }

    /// Build a response for `DeleteBucketPolicy` (204 No Content).
    #[must_use]
    pub fn delete_bucket_policy() -> Self {
        Self::new(204)
    }

    /// Build a response for `GetObjectAttributes` (200 OK, XML body).
    #[must_use]
    pub fn get_object_attributes(
        body_xml: &str,
        last_modified: u64,
        version_id: VersionId,
    ) -> Self {
        let mut resp = Self::new(200);
        resp.body = body_xml.as_bytes().to_vec();
        resp.headers
            .push(("Content-Length".to_string(), resp.body.len().to_string()));
        resp = resp.header("Last-Modified", &format_http_date(last_modified));
        if version_id.is_versioned() {
            resp = resp.header("x-amz-version-id", &format_version_id(version_id));
        }
        resp
    }

    /// Build a response for `PutBucketAcl` (200 OK, no body).
    #[must_use]
    pub fn put_bucket_acl() -> Self {
        Self::new(200)
    }

    /// Build a response for `GetBucketAcl`.
    #[must_use]
    pub fn get_bucket_acl(
        result: &GetBucketAclResult,
        owner_display_name: &str,
        grants: &[xml::RenderedAclGrant],
    ) -> Self {
        Self::new(200).chunked_xml_body(xml::acl_xml(
            owner_display_name,
            &result.owner_canonical_id,
            grants,
        ))
    }

    /// Build a response for `PutObjectAcl` (200 OK, optional version header).
    #[must_use]
    pub fn put_object_acl(version_id: VersionId) -> Self {
        let mut resp = Self::new(200);
        if version_id.is_versioned() {
            resp = resp.header("x-amz-version-id", &format_version_id(version_id));
        }
        resp
    }

    /// Build a response for `GetObjectAcl`.
    #[must_use]
    pub fn get_object_acl(
        result: &GetObjectAclResult,
        owner_display_name: &str,
        grants: &[xml::RenderedAclGrant],
    ) -> Self {
        let mut resp = Self::new(200).chunked_xml_body(xml::acl_xml(
            owner_display_name,
            &result.owner_canonical_id,
            grants,
        ));
        if result.version_id.is_versioned() {
            resp = resp.header("x-amz-version-id", &format_version_id(result.version_id));
        }
        resp
    }

    /// Build a response for `CreateMultipartUpload` (200 OK, XML body).
    #[must_use]
    pub fn create_multipart_upload(
        bucket: &str,
        key: &str,
        upload_id: &UploadId,
        ctx: CreateMultipartUploadResponseContext<'_>,
    ) -> Self {
        let body = xml::initiate_multipart_upload_xml(
            bucket,
            key,
            upload_id.as_str(),
            ctx.checksum_algorithm.map(ChecksumAlgorithm::as_str),
            ctx.checksum_type.map(ChecksumType::as_str),
        );
        let mut resp = Self::new(200).chunked_body(body.into_bytes());
        if let Some(algo) = ctx.checksum_algorithm {
            resp = resp.header("x-amz-checksum-algorithm", algo.as_str());
        }
        if let Some(checksum_type) = ctx.checksum_type {
            resp = resp.header("x-amz-checksum-type", checksum_type.as_str());
        }
        resp.apply_lifecycle_abort_headers(ctx.lifecycle_abort)
            .apply_managed_encryption_headers(ctx.managed_encryption)
            .apply_sse_customer_headers(ctx.sse_customer)
    }

    /// Build a response for `UploadPart` (200 OK, `ETag` header, optional checksum).
    #[must_use]
    pub fn upload_part(
        etag: &str,
        checksum: Option<&RawChecksum>,
        managed_encryption: Option<ManagedEncryptionAlgorithm>,
        sse_customer: Option<&SseCustomerResponseHeaders>,
    ) -> Self {
        let mut resp = Self::new(200).header("ETag", etag);
        if let Some(cksum) = checksum {
            use base64::Engine;
            let b64 = base64::engine::general_purpose::STANDARD.encode(cksum.bytes());
            resp = resp.header(cksum.algorithm().header_name(), &b64);
        }
        resp.apply_managed_encryption_headers(managed_encryption)
            .apply_sse_customer_headers(sse_customer)
    }

    /// Build a response for `UploadPartCopy` (200 OK, XML body with `CopyPartResult`).
    #[must_use]
    pub fn upload_part_copy(
        etag: &str,
        last_modified: u64,
        checksum: Option<&RawChecksum>,
        managed_encryption: Option<ManagedEncryptionAlgorithm>,
        sse_customer: Option<&SseCustomerResponseHeaders>,
    ) -> Self {
        let body = xml::copy_part_result_xml(etag, last_modified, checksum);
        Self::new(200)
            .xml_body(body)
            .apply_managed_encryption_headers(managed_encryption)
            .apply_sse_customer_headers(sse_customer)
    }

    /// Build a response for `CompleteMultipartUpload` (200 OK, XML body).
    #[must_use]
    pub fn complete_multipart_upload(
        bucket: &str,
        key: &str,
        result: &CompleteMultipartUploadResult,
    ) -> Self {
        let body = xml::complete_multipart_upload_xml(
            bucket,
            key,
            &result.etag,
            result.checksum_algorithm,
            result.checksum_type,
            result.checksum_value.as_deref(),
        );
        let mut resp = Self::new(200).chunked_xml_body(body);
        if result.version_id.is_versioned() {
            resp = resp.header("x-amz-version-id", &format_version_id(result.version_id));
        }
        resp.apply_lifecycle_expiration_header(result.lifecycle_expiration.as_ref())
            .apply_managed_encryption_headers(result.managed_encryption)
    }

    /// Build a complete-multipart `MalformedXML` response.
    #[must_use]
    pub fn complete_multipart_malformed_xml(wire_ids: &WireResponseIds) -> Self {
        let body = xml::complete_multipart_malformed_xml_error_xml(
            wire_ids.request_id(),
            wire_ids.host_id(),
        );
        Self::new(400).chunked_xml_body(body)
    }

    /// Build a complete-multipart `NoSuchUpload` response.
    #[must_use]
    pub fn complete_multipart_no_such_upload(upload_id: &str, wire_ids: &WireResponseIds) -> Self {
        let body = xml::complete_multipart_no_such_upload_error_xml(
            upload_id,
            wire_ids.request_id(),
            wire_ids.host_id(),
        );
        Self::new(404).chunked_xml_body(body)
    }

    /// Build a complete-multipart `InvalidPart` response.
    #[must_use]
    pub fn complete_multipart_invalid_part(
        upload_id: &str,
        part_number: u32,
        etag: &str,
        wire_ids: &WireResponseIds,
    ) -> Self {
        let body = xml::complete_multipart_invalid_part_error_xml(
            upload_id,
            part_number,
            etag,
            wire_ids.request_id(),
            wire_ids.host_id(),
        );
        Self::new(400).chunked_xml_body(body)
    }

    /// Build a complete-multipart `InvalidPartOrder` response.
    #[must_use]
    pub fn complete_multipart_invalid_part_order(
        upload_id: &str,
        wire_ids: &WireResponseIds,
    ) -> Self {
        let body = xml::complete_multipart_invalid_part_order_error_xml(
            upload_id,
            wire_ids.request_id(),
            wire_ids.host_id(),
        );
        Self::new(400).chunked_xml_body(body)
    }

    /// Build a complete-multipart `EntityTooSmall` response.
    #[must_use]
    pub fn complete_multipart_entity_too_small(
        proposed_size: u64,
        min_size_allowed: u64,
        part_number: u32,
        etag: &str,
        wire_ids: &WireResponseIds,
    ) -> Self {
        let body = xml::complete_multipart_entity_too_small_error_xml(
            proposed_size,
            min_size_allowed,
            part_number,
            etag,
            wire_ids.request_id(),
            wire_ids.host_id(),
        );
        Self::new(400).chunked_xml_body(body)
    }

    /// Build a response for `AbortMultipartUpload` (204 No Content).
    #[must_use]
    pub fn abort_multipart_upload() -> Self {
        Self::new(204)
    }

    /// Build a response for `ListMultipartUploads` (200 OK, XML body).
    #[must_use]
    pub fn list_multipart_uploads(
        bucket: &str,
        prefix: Option<&str>,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
        encoding_type: Option<&str>,
        max_uploads: u32,
        result: &xml::RenderedListMultipartUploadsResult,
    ) -> Self {
        let body = xml::list_multipart_uploads_xml(
            bucket,
            prefix,
            key_marker,
            upload_id_marker,
            encoding_type,
            max_uploads,
            result,
        );
        Self::new(200).chunked_xml_body(body)
    }

    /// Build a response for `ListParts` (200 OK, XML body).
    #[must_use]
    pub fn list_parts(
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number_marker: Option<u32>,
        max_parts: u32,
        result: &ListPartsResult,
    ) -> Self {
        let body = xml::list_parts_xml(
            bucket,
            key,
            upload_id,
            part_number_marker,
            max_parts,
            result,
        );
        Self::new(200)
            .chunked_xml_body(body)
            .apply_lifecycle_abort_headers(result.lifecycle_abort.as_ref())
    }

    /// Build a 200 response for a CORS preflight (headers added by caller).
    #[must_use]
    pub fn cors_preflight() -> Self {
        Self::new(200)
    }

    /// Build a 403 Forbidden response.
    #[must_use]
    pub fn forbidden(host_id: &str) -> Self {
        let request_id = current_request_id();
        let body =
            xml::error_xml_with_host_id("AccessDenied", "Access Denied", &request_id, host_id);
        Self::new(403).chunked_xml_body(body)
    }

    /// Build a 403 Forbidden response with explicit wire IDs.
    #[must_use]
    pub fn forbidden_with_ids(wire_ids: &WireResponseIds) -> Self {
        let body = xml::error_xml_with_host_id(
            "AccessDenied",
            "Access Denied",
            wire_ids.request_id(),
            wire_ids.host_id(),
        );
        Self::new(403).chunked_xml_body(body)
    }

    /// Build an error response.
    #[must_use]
    pub fn error(err: &ServerError, resource: &str, host_id: &str) -> Self {
        let wire_ids = WireResponseIds::new(TEST_REQUEST_ID, host_id);
        Self::client_error_response_with_ids(err, resource, &wire_ids).with_error_diagnostic(err)
    }

    /// Build an error response with explicit wire IDs.
    #[must_use]
    pub fn error_with_ids(err: &ServerError, resource: &str, wire_ids: &WireResponseIds) -> Self {
        Self::client_error_response_with_ids(err, resource, wire_ids).with_error_diagnostic(err)
    }
}

fn format_object_lock_header_timestamp(unix_seconds: u64) -> String {
    let days_since_epoch = unix_seconds / 86400;
    let time_of_day = unix_seconds % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    let z = days_since_epoch as i64 + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = i64::from(yoe) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { y + 1 } else { y };

    format!("{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{seconds:02}Z")
}

/// Format a unix millisecond timestamp as HTTP date (RFC 7231).
fn format_http_date(millis: u64) -> String {
    let secs = millis / 1000;
    let days_since_epoch = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    let weekday = ((days_since_epoch + 4) % 7) as usize; // Jan 1 1970 = Thursday (4)
    let weekdays = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

    let (year, month, day) = days_to_date(days_since_epoch as i64);
    let months = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];

    format!(
        "{}, {:02} {} {:04} {:02}:{:02}:{:02} GMT",
        weekdays[weekday],
        day,
        months[(month - 1) as usize],
        year,
        hours,
        minutes,
        seconds
    )
}

/// Convert days since Unix epoch to (year, month, day).
fn days_to_date(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = i64::from(yoe) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

/// Parse an RFC 7231 HTTP date (e.g. `"Thu, 01 Jan 1970 00:00:00 GMT"`) into unix milliseconds.
/// Returns `None` for malformed dates. Only supports this one format (IMF-fixdate).
pub(crate) fn parse_http_date(s: &str) -> Option<u64> {
    // Format: "Day, DD Mon YYYY HH:MM:SS GMT"
    let s = s.trim();
    let bytes = s.as_bytes();
    if bytes.len() < 29 || &bytes[bytes.len().checked_sub(3)?..] != b"GMT" {
        return None;
    }

    let day: u32 = std::str::from_utf8(bytes.get(5..7)?).ok()?.parse().ok()?;
    let month_str = std::str::from_utf8(bytes.get(8..11)?).ok()?;
    let year: i64 = std::str::from_utf8(bytes.get(12..16)?).ok()?.parse().ok()?;
    let hours: u64 = std::str::from_utf8(bytes.get(17..19)?).ok()?.parse().ok()?;
    let minutes: u64 = std::str::from_utf8(bytes.get(20..22)?).ok()?.parse().ok()?;
    let seconds: u64 = std::str::from_utf8(bytes.get(23..25)?).ok()?.parse().ok()?;

    let months = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    #[allow(clippy::cast_possible_truncation)]
    let month = months.iter().position(|&m| m == month_str)? as u32 + 1;

    if hours >= 24 || minutes >= 60 || seconds >= 60 || day == 0 || day > 31 || month > 12 {
        return None;
    }

    let days = date_to_days(year, month, day);
    if days < 0 {
        return None;
    }
    let secs = days as u64 * 86400 + hours * 3600 + minutes * 60 + seconds;
    Some(secs * 1000)
}

/// Convert (year, month, day) to days since Unix epoch. Inverse of `days_to_date`.
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn date_to_days(year: i64, month: u32, day: u32) -> i64 {
    // Civil calendar algorithm (inverse of days_to_date)
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u32;
    let m = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * m + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + i64::from(doe) - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::{
        GetObjectResult, HeadObjectResult, ListEntry, ListObjectsResult, PutObjectResult,
    };
    use crate::metadata_blob::MetadataBlob;
    use s3_types::{AclGrant, AclGrantee, AclGrants, AclPermission};

    fn system_metadata(headers: &[(&str, &str)]) -> SystemMetadata {
        SystemMetadata::from_pairs(headers).expect("test system metadata pairs must be valid")
    }

    fn find_header<'a>(resp: &'a S3Response, name: &str) -> Option<&'a str> {
        resp.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    // ── format_http_date ──────────────────────────────────────────────

    #[test]
    fn format_http_date_epoch() {
        assert_eq!(format_http_date(0), "Thu, 01 Jan 1970 00:00:00 GMT");
    }

    #[test]
    fn format_http_date_known_date() {
        // 2024-01-15 12:30:45 UTC
        // seconds: 1705321845, millis: 1705321845000
        assert_eq!(
            format_http_date(1705321845000),
            "Mon, 15 Jan 2024 12:30:45 GMT"
        );
    }

    // ── days_to_date ──────────────────────────────────────────────────

    #[test]
    fn days_to_date_epoch() {
        assert_eq!(days_to_date(0), (1970, 1, 1));
    }

    #[test]
    fn days_to_date_leap_year_feb29() {
        // 2000-02-29 is day 11016 since epoch
        // 2000-01-01 is day 10957, Feb 29 = 10957 + 31 + 28 = 11016
        assert_eq!(days_to_date(11016), (2000, 2, 29));
    }

    #[test]
    fn days_to_date_year_boundary() {
        // 1970-12-31 is day 364
        assert_eq!(days_to_date(364), (1970, 12, 31));
        // 1971-01-01 is day 365
        assert_eq!(days_to_date(365), (1971, 1, 1));
    }

    #[test]
    fn days_to_date_2024_leap() {
        // 2024-02-29 — 2024 is a leap year
        // 2024-01-01 is day 19723
        // Feb 29 = 19723 + 31 + 28 = 19782
        assert_eq!(days_to_date(19782), (2024, 2, 29));
    }

    // ── put_object ────────────────────────────────────────────────────

    #[test]
    fn put_object_response() {
        let result = PutObjectResult {
            etag: "\"abc123\"".to_string(),
            last_modified: 0,
            version_id: VersionId::Null,
            system_metadata: SystemMetadata::EMPTY,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::put_object(&result);
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "ETag"), Some("\"abc123\""));
        // version_id=0 means unversioned — no x-amz-version-id header
        assert_eq!(find_header(&resp, "x-amz-version-id"), None);
    }

    #[test]
    fn put_object_response_includes_managed_encryption_header() {
        let result = PutObjectResult {
            etag: "\"abc123\"".to_string(),
            last_modified: 0,
            version_id: VersionId::Null,
            system_metadata: SystemMetadata::EMPTY,
            managed_encryption: Some(ManagedEncryptionAlgorithm::Aes256),
            lifecycle_expiration: None,
        };
        let resp = S3Response::put_object(&result);
        assert_eq!(
            find_header(&resp, "x-amz-server-side-encryption"),
            Some("AES256")
        );
    }

    #[test]
    fn put_object_response_versioned() {
        let result = PutObjectResult {
            etag: "\"abc123\"".to_string(),
            last_modified: 0,
            version_id: VersionId::from_u64(42),
            system_metadata: SystemMetadata::EMPTY,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::put_object(&result);
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "ETag"), Some("\"abc123\""));
        assert_eq!(find_header(&resp, "x-amz-version-id"), Some("42"));
    }

    #[test]
    fn put_object_response_includes_checksum_headers() {
        let mut system_metadata = SystemMetadata::EMPTY;
        system_metadata.set_checksum(
            ChecksumAlgorithm::Crc64nvme,
            Some(ChecksumType::FullObject),
            "AAAAAA==".to_string(),
        );
        let result = PutObjectResult {
            etag: "\"abc123\"".to_string(),
            last_modified: 0,
            version_id: VersionId::Null,
            system_metadata,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::put_object(&result);
        assert_eq!(
            find_header(&resp, "x-amz-checksum-crc64nvme"),
            Some("AAAAAA==")
        );
        assert_eq!(
            find_header(&resp, "x-amz-checksum-type"),
            Some("FULL_OBJECT")
        );
    }

    #[test]
    fn get_object_attributes_response_matches_aws_header_shape() {
        let resp = S3Response::get_object_attributes(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<GetObjectAttributesResponse xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"></GetObjectAttributesResponse>",
            1_705_321_845_000,
            VersionId::Null,
        );
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "Content-Length"), Some("146"));
        assert_eq!(
            find_header(&resp, "Last-Modified"),
            Some("Mon, 15 Jan 2024 12:30:45 GMT")
        );
        assert_eq!(find_header(&resp, "Content-Type"), None);
        assert_eq!(find_header(&resp, "x-amz-server-side-encryption"), None);
        assert_eq!(
            std::str::from_utf8(&resp.body).ok(),
            Some(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<GetObjectAttributesResponse xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"></GetObjectAttributesResponse>"
            )
        );
    }

    #[test]
    fn post_object_response_redirects_with_success_query_params() {
        let result = PutObjectResult {
            etag: "\"abc123\"".to_string(),
            last_modified: 0,
            version_id: VersionId::Null,
            system_metadata: SystemMetadata::EMPTY,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::post_object(
            &result,
            "my-bucket",
            "folder/my file.txt",
            201,
            Some("https://example.com/success"),
            None,
        );
        assert_eq!(resp.status_code, 303);
        assert_eq!(
            find_header(&resp, "Location"),
            Some(
                "https://example.com/success?bucket=my-bucket&key=folder%2Fmy%20file.txt&etag=%22abc123%22"
            )
        );
        assert_eq!(find_header(&resp, "ETag"), Some("\"abc123\""));
        assert!(resp.body.is_empty());
    }

    #[test]
    fn post_object_response_includes_managed_encryption_header() {
        let result = PutObjectResult {
            etag: "\"abc123\"".to_string(),
            last_modified: 0,
            version_id: VersionId::Null,
            system_metadata: SystemMetadata::EMPTY,
            managed_encryption: Some(ManagedEncryptionAlgorithm::Aes256),
            lifecycle_expiration: None,
        };
        let resp = S3Response::post_object(
            &result,
            "my-bucket",
            "my-key",
            204,
            None,
            Some("https://s3.us-east-1.amazonaws.com/my-bucket/my-key"),
        );
        assert_eq!(
            find_header(&resp, "x-amz-server-side-encryption"),
            Some("AES256")
        );
        assert_eq!(
            find_header(&resp, "Location"),
            Some("https://s3.us-east-1.amazonaws.com/my-bucket/my-key")
        );
    }

    #[test]
    fn post_object_response_redirect_appends_to_existing_query() {
        let result = PutObjectResult {
            etag: "\"abc123\"".to_string(),
            last_modified: 0,
            version_id: VersionId::Null,
            system_metadata: SystemMetadata::EMPTY,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::post_object(
            &result,
            "my-bucket",
            "my-key",
            204,
            Some("https://example.com/success?foo=bar"),
            None,
        );
        assert_eq!(resp.status_code, 303);
        assert_eq!(
            find_header(&resp, "Location"),
            Some(
                "https://example.com/success?foo=bar&bucket=my-bucket&key=my-key&etag=%22abc123%22"
            )
        );
    }

    #[test]
    fn post_object_response_ignores_invalid_redirect() {
        let result = PutObjectResult {
            etag: "\"abc123\"".to_string(),
            last_modified: 0,
            version_id: VersionId::Null,
            system_metadata: SystemMetadata::EMPTY,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::post_object(
            &result,
            "my-bucket",
            "my-key",
            200,
            Some("https://example.com/\n"),
            Some("https://s3.us-east-1.amazonaws.com/my-bucket/my-key"),
        );
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "Location"), None);
        assert_eq!(find_header(&resp, "ETag"), Some("\"abc123\""));
    }

    #[test]
    fn put_object_response_includes_lifecycle_expiration_header() {
        let result = PutObjectResult {
            etag: "\"abc123\"".to_string(),
            last_modified: 0,
            version_id: VersionId::Null,
            system_metadata: SystemMetadata::EMPTY,
            managed_encryption: None,
            lifecycle_expiration: Some(LifecycleExpirationHeader {
                expiry_time_millis: 1_705_321_845_000,
                rule_id: Some("expire current".to_string()),
            }),
        };
        let resp = S3Response::put_object(&result);
        assert_eq!(
            find_header(&resp, "x-amz-expiration"),
            Some("expiry-date=\"Mon, 15 Jan 2024 12:30:45 GMT\", rule-id=\"expire%20current\"")
        );
    }

    // ── get_object ────────────────────────────────────────────────────

    #[test]
    fn get_object_with_content_type() {
        let result = GetObjectResult {
            sse_customer: None,
            body: ReadHandle::from_buffered_bytes(b"hello".to_vec()),
            metadata: MetadataBlob::new(),
            system_metadata: system_metadata(&[("content-type", "text/plain")]),
            object_lock: s3_types::ObjectLockState::default(),
            etag: "\"etag\"".into(),
            size: 5,
            last_modified: 0,
            version_id: VersionId::Null,
            tags: None,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::get_object(result, None);
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "Content-Type"), Some("text/plain"));
        assert_eq!(resp.into_test_body_bytes().unwrap(), b"hello");
    }

    #[test]
    fn get_object_default_content_type() {
        let result = GetObjectResult {
            sse_customer: None,
            body: ReadHandle::from_buffered_bytes(b"data".to_vec()),
            metadata: MetadataBlob::new(),
            system_metadata: SystemMetadata::new(),
            object_lock: s3_types::ObjectLockState::default(),
            etag: "\"etag\"".into(),
            size: 4,
            last_modified: 0,
            version_id: VersionId::Null,
            tags: None,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::get_object(result, None);
        assert_eq!(
            find_header(&resp, "Content-Type"),
            Some("binary/octet-stream")
        );
    }

    #[test]
    fn get_object_with_amz_meta_headers() {
        let result = GetObjectResult {
            sse_customer: None,
            body: ReadHandle::from_buffered_bytes(vec![]),
            metadata: MetadataBlob::from_pairs(&[("x-amz-meta-author", "alice")])
                .expect("test metadata pairs must be valid"),
            system_metadata: SystemMetadata::new(),
            object_lock: s3_types::ObjectLockState::default(),
            etag: "\"e\"".into(),
            size: 0,
            last_modified: 0,
            version_id: VersionId::Null,
            tags: None,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::get_object(result, None);
        assert_eq!(find_header(&resp, "x-amz-meta-author"), Some("alice"));
    }

    #[test]
    fn get_object_with_all_standard_metadata() {
        let result = GetObjectResult {
            sse_customer: None,
            body: ReadHandle::from_buffered_bytes(vec![]),
            metadata: MetadataBlob::new(),
            system_metadata: system_metadata(&[
                ("content-type", "text/html"),
                ("content-encoding", "gzip"),
                ("cache-control", "max-age=3600"),
                ("content-disposition", "attachment"),
                ("content-language", "en-US"),
                ("expires", "Thu, 01 Jan 2099 00:00:00 GMT"),
            ]),
            object_lock: s3_types::ObjectLockState::default(),
            etag: "\"e\"".into(),
            size: 0,
            last_modified: 0,
            version_id: VersionId::Null,
            tags: None,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::get_object(result, None);
        assert_eq!(find_header(&resp, "Content-Type"), Some("text/html"));
        assert_eq!(find_header(&resp, "Content-Encoding"), Some("gzip"));
        assert_eq!(find_header(&resp, "Cache-Control"), Some("max-age=3600"));
        assert_eq!(
            find_header(&resp, "Content-Disposition"),
            Some("attachment")
        );
        assert_eq!(find_header(&resp, "Content-Language"), Some("en-US"));
        assert_eq!(
            find_header(&resp, "Expires"),
            Some("Thu, 01 Jan 2099 00:00:00 GMT")
        );
    }

    #[test]
    fn get_object_emits_website_redirect_location_header() {
        let result = GetObjectResult {
            sse_customer: None,
            body: ReadHandle::from_buffered_bytes(vec![]),
            metadata: MetadataBlob::new(),
            system_metadata: system_metadata(&[(
                "x-amz-website-redirect-location",
                "/docs/get.html",
            )]),
            object_lock: s3_types::ObjectLockState::default(),
            etag: "\"e\"".into(),
            size: 0,
            last_modified: 0,
            version_id: VersionId::Null,
            tags: None,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::get_object(result, None);
        assert_eq!(
            find_header(&resp, "x-amz-website-redirect-location"),
            Some("/docs/get.html")
        );
    }

    #[test]
    fn get_object_checksum_type_with_checksum_mode_enabled() {
        let result = GetObjectResult {
            sse_customer: None,
            body: ReadHandle::from_buffered_bytes(vec![]),
            metadata: MetadataBlob::new(),
            system_metadata: system_metadata(&[
                ("x-amz-checksum-crc32", "AAAAAA=="),
                ("x-amz-checksum-type", "FULL_OBJECT"),
            ]),
            object_lock: s3_types::ObjectLockState::default(),
            etag: "\"e\"".into(),
            size: 0,
            last_modified: 0,
            version_id: VersionId::Null,
            tags: None,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::get_object(result, Some("ENABLED"));
        assert_eq!(find_header(&resp, "x-amz-checksum-crc32"), Some("AAAAAA=="));
        assert_eq!(
            find_header(&resp, "x-amz-checksum-type"),
            Some("FULL_OBJECT")
        );
    }

    #[test]
    fn get_object_checksum_type_omitted_without_checksum_mode() {
        let result = GetObjectResult {
            sse_customer: None,
            body: ReadHandle::from_buffered_bytes(vec![]),
            metadata: MetadataBlob::new(),
            system_metadata: system_metadata(&[
                ("x-amz-checksum-crc32", "AAAAAA=="),
                ("x-amz-checksum-type", "FULL_OBJECT"),
            ]),
            object_lock: s3_types::ObjectLockState::default(),
            etag: "\"e\"".into(),
            size: 0,
            last_modified: 0,
            version_id: VersionId::Null,
            tags: None,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::get_object(result, None);
        assert_eq!(find_header(&resp, "x-amz-checksum-crc32"), None);
        assert_eq!(find_header(&resp, "x-amz-checksum-type"), None);
    }

    #[test]
    fn get_object_emits_object_lock_headers() {
        let result = GetObjectResult {
            sse_customer: None,
            body: ReadHandle::from_buffered_bytes(vec![]),
            metadata: MetadataBlob::new(),
            system_metadata: SystemMetadata::new(),
            object_lock: s3_types::ObjectLockState {
                retention: Some(ObjectRetention {
                    mode: s3_types::ObjectLockMode::Governance,
                    retain_until_unix_seconds: 1_775_001_600,
                }),
                legal_hold: s3_types::StoredLegalHoldStatus::On,
            },
            etag: "\"e\"".into(),
            size: 0,
            last_modified: 0,
            version_id: VersionId::Null,
            tags: None,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::get_object(result, None);
        assert_eq!(
            find_header(&resp, "x-amz-object-lock-mode"),
            Some("GOVERNANCE")
        );
        assert_eq!(
            find_header(&resp, "x-amz-object-lock-retain-until-date"),
            Some("2026-04-01T00:00:00Z")
        );
        assert_eq!(
            find_header(&resp, "x-amz-object-lock-legal-hold"),
            Some("ON")
        );
    }

    #[test]
    fn get_object_response_includes_lifecycle_expiration_without_rule_id() {
        let result = GetObjectResult {
            sse_customer: None,
            body: ReadHandle::from_buffered_bytes(b"hello".to_vec()),
            metadata: MetadataBlob::new(),
            system_metadata: SystemMetadata::new(),
            object_lock: s3_types::ObjectLockState::default(),
            etag: "\"etag\"".into(),
            size: 5,
            last_modified: 0,
            version_id: VersionId::Null,
            tags: None,
            managed_encryption: None,
            lifecycle_expiration: Some(LifecycleExpirationHeader {
                expiry_time_millis: 1_705_321_845_000,
                rule_id: None,
            }),
        };
        let resp = S3Response::get_object(result, None);
        assert_eq!(
            find_header(&resp, "x-amz-expiration"),
            Some("expiry-date=\"Mon, 15 Jan 2024 12:30:45 GMT\"")
        );
    }

    // ── head_object ───────────────────────────────────────────────────

    #[test]
    fn head_object_response() {
        let result = HeadObjectResult {
            sse_customer: None,
            metadata: MetadataBlob::new(),
            system_metadata: system_metadata(&[("content-type", "image/png")]),
            object_lock: s3_types::ObjectLockState::default(),
            etag: "\"etag\"".into(),
            size: 1024,
            last_modified: 0,
            version_id: VersionId::Null,
            tags: None,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::head_object(&result, None);
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "ETag"), Some("\"etag\""));
        assert_eq!(find_header(&resp, "Content-Length"), Some("1024"));
        assert_eq!(find_header(&resp, "Content-Type"), Some("image/png"));
        assert!(resp.body.is_empty());
    }

    #[test]
    fn head_object_default_content_type() {
        let result = HeadObjectResult {
            sse_customer: None,
            metadata: MetadataBlob::new(),
            system_metadata: SystemMetadata::new(),
            object_lock: s3_types::ObjectLockState::default(),
            etag: "\"e\"".into(),
            size: 0,
            last_modified: 0,
            version_id: VersionId::Null,
            tags: None,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::head_object(&result, None);
        assert_eq!(
            find_header(&resp, "Content-Type"),
            Some("binary/octet-stream")
        );
    }

    #[test]
    fn head_object_with_encoding_and_cache() {
        let result = HeadObjectResult {
            sse_customer: None,
            metadata: MetadataBlob::new(),
            system_metadata: system_metadata(&[
                ("content-encoding", "br"),
                ("cache-control", "no-cache"),
            ]),
            object_lock: s3_types::ObjectLockState::default(),
            etag: "\"e\"".into(),
            size: 10,
            last_modified: 0,
            version_id: VersionId::Null,
            tags: None,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::head_object(&result, None);
        assert_eq!(find_header(&resp, "Content-Encoding"), Some("br"));
        assert_eq!(find_header(&resp, "Cache-Control"), Some("no-cache"));
    }

    #[test]
    fn head_object_emits_website_redirect_location_header() {
        let result = HeadObjectResult {
            sse_customer: None,
            metadata: MetadataBlob::new(),
            system_metadata: system_metadata(&[(
                "x-amz-website-redirect-location",
                "/docs/head.html",
            )]),
            object_lock: s3_types::ObjectLockState::default(),
            etag: "\"e\"".into(),
            size: 10,
            last_modified: 0,
            version_id: VersionId::Null,
            tags: None,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::head_object(&result, None);
        assert_eq!(
            find_header(&resp, "x-amz-website-redirect-location"),
            Some("/docs/head.html")
        );
    }

    #[test]
    fn head_object_with_amz_meta() {
        let result = HeadObjectResult {
            sse_customer: None,
            metadata: MetadataBlob::from_pairs(&[("x-amz-meta-tag", "value")])
                .expect("test metadata pairs must be valid"),
            system_metadata: SystemMetadata::new(),
            object_lock: s3_types::ObjectLockState::default(),
            etag: "\"e\"".into(),
            size: 0,
            last_modified: 0,
            version_id: VersionId::Null,
            tags: None,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::head_object(&result, None);
        assert_eq!(find_header(&resp, "x-amz-meta-tag"), Some("value"));
    }

    #[test]
    fn head_object_checksum_type_with_checksum_mode_enabled() {
        let result = HeadObjectResult {
            sse_customer: None,
            metadata: MetadataBlob::new(),
            system_metadata: system_metadata(&[
                ("x-amz-checksum-crc32", "AAAAAA=="),
                ("x-amz-checksum-type", "COMPOSITE"),
            ]),
            object_lock: s3_types::ObjectLockState::default(),
            etag: "\"e\"".into(),
            size: 0,
            last_modified: 0,
            version_id: VersionId::Null,
            tags: None,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::head_object(&result, Some("ENABLED"));
        assert_eq!(find_header(&resp, "x-amz-checksum-crc32"), Some("AAAAAA=="));
        assert_eq!(find_header(&resp, "x-amz-checksum-type"), Some("COMPOSITE"));
    }

    #[test]
    fn head_object_checksum_type_omitted_without_checksum_mode() {
        let result = HeadObjectResult {
            sse_customer: None,
            metadata: MetadataBlob::new(),
            system_metadata: system_metadata(&[
                ("x-amz-checksum-crc32", "AAAAAA=="),
                ("x-amz-checksum-type", "COMPOSITE"),
            ]),
            object_lock: s3_types::ObjectLockState::default(),
            etag: "\"e\"".into(),
            size: 0,
            last_modified: 0,
            version_id: VersionId::Null,
            tags: None,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::head_object(&result, None);
        assert_eq!(find_header(&resp, "x-amz-checksum-crc32"), None);
        assert_eq!(find_header(&resp, "x-amz-checksum-type"), None);
    }

    #[test]
    fn head_object_emits_object_lock_headers_and_omits_never_set_legal_hold() {
        let result = HeadObjectResult {
            sse_customer: None,
            metadata: MetadataBlob::new(),
            system_metadata: SystemMetadata::new(),
            object_lock: s3_types::ObjectLockState {
                retention: Some(ObjectRetention {
                    mode: s3_types::ObjectLockMode::Compliance,
                    retain_until_unix_seconds: 1_775_001_600,
                }),
                legal_hold: s3_types::StoredLegalHoldStatus::NotSet,
            },
            etag: "\"e\"".into(),
            size: 0,
            last_modified: 0,
            version_id: VersionId::Null,
            tags: None,
            managed_encryption: None,
            lifecycle_expiration: None,
        };
        let resp = S3Response::head_object(&result, None);
        assert_eq!(
            find_header(&resp, "x-amz-object-lock-mode"),
            Some("COMPLIANCE")
        );
        assert_eq!(
            find_header(&resp, "x-amz-object-lock-retain-until-date"),
            Some("2026-04-01T00:00:00Z")
        );
        assert_eq!(find_header(&resp, "x-amz-object-lock-legal-hold"), None);
    }

    #[test]
    fn head_object_response_includes_lifecycle_expiration_header() {
        let result = HeadObjectResult {
            sse_customer: None,
            metadata: MetadataBlob::new(),
            system_metadata: SystemMetadata::new(),
            object_lock: s3_types::ObjectLockState::default(),
            etag: "\"e\"".into(),
            size: 0,
            last_modified: 0,
            version_id: VersionId::Null,
            tags: None,
            managed_encryption: None,
            lifecycle_expiration: Some(LifecycleExpirationHeader {
                expiry_time_millis: 1_705_321_845_000,
                rule_id: Some("expire/head".to_string()),
            }),
        };
        let resp = S3Response::head_object(&result, None);
        assert_eq!(
            find_header(&resp, "x-amz-expiration"),
            Some("expiry-date=\"Mon, 15 Jan 2024 12:30:45 GMT\", rule-id=\"expire%2Fhead\"")
        );
    }

    // ── delete_object ─────────────────────────────────────────────────

    #[test]
    fn delete_object_response() {
        use crate::coordinator::DeleteObjectResult;
        let result = DeleteObjectResult {
            version_id: VersionId::Null,
            delete_marker: false,
        };
        let resp = S3Response::delete_object(&result);
        assert_eq!(resp.status_code, 204);
        // version_id=0 means unversioned — no x-amz-version-id header
        assert_eq!(find_header(&resp, "x-amz-version-id"), None);
        assert!(resp.body.is_empty());
    }

    #[test]
    fn delete_object_versioned_with_marker() {
        use crate::coordinator::DeleteObjectResult;
        let result = DeleteObjectResult {
            version_id: VersionId::from_u64(5),
            delete_marker: true,
        };
        let resp = S3Response::delete_object(&result);
        assert_eq!(resp.status_code, 204);
        assert_eq!(find_header(&resp, "x-amz-version-id"), Some("5"));
        assert_eq!(find_header(&resp, "x-amz-delete-marker"), Some("true"));
    }

    #[test]
    fn head_delete_marker_method_not_allowed_response() {
        let resp = S3Response::head_delete_marker_method_not_allowed(
            VersionId::from_u64(5),
            1_705_321_845_000,
        );
        assert_eq!(resp.status_code, 405);
        assert_eq!(find_header(&resp, "Allow"), Some("DELETE"));
        assert_eq!(find_header(&resp, "x-amz-delete-marker"), Some("true"));
        assert_eq!(find_header(&resp, "x-amz-version-id"), Some("5"));
        assert_eq!(
            find_header(&resp, "Last-Modified"),
            Some("Mon, 15 Jan 2024 12:30:45 GMT")
        );
        assert!(resp.body.is_empty());
    }

    // ── create_bucket ─────────────────────────────────────────────────

    #[test]
    fn create_bucket_response() {
        let resp = S3Response::create_bucket("my-bucket");
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "Location"), Some("/my-bucket"));
    }

    // ── delete_bucket ─────────────────────────────────────────────────

    #[test]
    fn delete_bucket_response() {
        let resp = S3Response::delete_bucket();
        assert_eq!(resp.status_code, 204);
    }

    #[test]
    fn get_bucket_object_lock_configuration_response() {
        let resp = S3Response::get_bucket_object_lock_configuration(BucketObjectLockConfig {
            enabled: true,
            default_retention: Some(s3_types::ObjectLockDefaultRetention {
                mode: s3_types::ObjectLockMode::Governance,
                period: s3_types::RetentionPeriod::days(1).unwrap(),
            }),
        });
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "Content-Type"), None);
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("<ObjectLockEnabled>Enabled</ObjectLockEnabled>"));
        assert!(body.contains("<Days>1</Days>"));
    }

    #[test]
    fn get_object_retention_response() {
        let resp = S3Response::get_object_retention(Some(ObjectRetention {
            mode: s3_types::ObjectLockMode::Governance,
            retain_until_unix_seconds: 1_775_001_600,
        }));
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "Content-Type"), None);
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("<Retention"));
        assert!(body.contains("<Mode>GOVERNANCE</Mode>"));
    }

    #[test]
    fn get_object_legal_hold_response() {
        let resp = S3Response::get_object_legal_hold(Some(LegalHoldStatus::On));
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "Content-Type"), None);
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("<LegalHold"));
        assert!(body.contains("<Status>ON</Status>"));
    }

    // ── head_bucket ───────────────────────────────────────────────────

    #[test]
    fn head_bucket_response() {
        let info = BucketSummary {
            name: storage::BucketName::try_from("bbb").unwrap(),
            owner_principal: "owner".into(),
            owner_canonical_id: CanonicalUserId::from_principal("owner"),
            created_at: 0,
            acl_grants: AclGrants::default(),
            versioning: BucketVersioningState::Disabled,
            object_lock: s3_types::BucketObjectLockConfig::default(),
            public_read: false,
            public_write: false,
            public_access_block: None,
            ownership_controls: None,
            bucket_policy_present: false,
            bucket_policy_public: false,
            bucket_policy_generation: 0,
            bucket_lifecycle_present: false,
            bucket_lifecycle_generation: 0,
            bucket_abac_enabled: false,
            encryption: EffectiveBucketEncryptionConfig::default(),
        };
        let resp = S3Response::head_bucket(&info, "us-west-2");
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "Content-Type"), Some("application/xml"));
        assert_eq!(
            find_header(&resp, "x-amz-access-point-alias"),
            Some("false")
        );
        assert_eq!(
            find_header(&resp, "x-amz-bucket-arn"),
            Some("arn:aws:s3:::bbb")
        );
        assert_eq!(find_header(&resp, "x-amz-bucket-region"), Some("us-west-2"));
        assert_eq!(find_header(&resp, "Content-Length"), None);
        assert!(resp.body.is_empty());
        assert!(resp.stream.is_none());
    }

    #[test]
    fn wrong_region_error_response_includes_bucket_region_hint() {
        let err = ServerError::WrongRegion {
            provided_region: "us-east-1".to_string(),
            expected_region: "us-west-2".to_string(),
        };
        let resp = S3Response::error(&err, "/bucket/key", TEST_HOST_ID);
        assert_eq!(resp.status_code, 400);
        assert_eq!(find_header(&resp, "x-amz-bucket-region"), Some("us-west-2"));
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("<Code>AuthorizationHeaderMalformed</Code>"));
        assert!(body.contains("<Region>us-west-2</Region>"));
        assert!(body.contains("expecting 'us-west-2'"));
        assert!(body.contains("<HostId>"));
        assert!(!body.contains("<Resource>"));
    }

    #[test]
    fn invalid_bucket_namespace_error_response_includes_bucket_namespace() {
        let err = ServerError::InvalidBucketNamespace {
            reason: "namespace mismatch".to_string(),
            bucket_namespace: "bucket-111122223333-us-east-1-an".to_string(),
        };
        let resp = S3Response::error(&err, "/bucket", TEST_HOST_ID);
        assert_eq!(resp.status_code, 400);
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("<Code>InvalidBucketNamespace</Code>"));
        assert!(body.contains("<Message>namespace mismatch</Message>"));
        assert!(
            body.contains("<BucketNamespace>bucket-111122223333-us-east-1-an</BucketNamespace>")
        );
    }

    #[test]
    fn unexpected_security_token_error_response_matches_aws_shape() {
        let err = ServerError::Auth(auth::AuthError::UnexpectedSecurityToken {
            token: "bad-token-causes-400".to_string(),
        });
        let resp = S3Response::error(&err, "/bucket/key", TEST_HOST_ID);
        assert_eq!(resp.status_code, 400);
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("<Code>InvalidToken</Code>"));
        assert!(body
            .contains("<Message>The provided token is malformed or otherwise invalid.</Message>"));
        assert!(body.contains("<Token-0>bad-token-causes-400</Token-0>"));
    }

    #[test]
    fn get_bucket_location_response_uses_legacy_us_east_1_null() {
        let resp = S3Response::get_bucket_location("us-east-1");
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "Content-Type"), Some("application/xml"));
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("<LocationConstraint"));
        assert!(body.contains("/>"));
        assert!(!body.contains(">us-east-1<"));
    }

    #[test]
    fn get_bucket_location_response_uses_current_eu_west_1_name() {
        let resp = S3Response::get_bucket_location("eu-west-1");
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "Content-Type"), Some("application/xml"));
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains(">eu-west-1</LocationConstraint>"));
    }

    // ── list_buckets ──────────────────────────────────────────────────

    #[test]
    fn list_buckets_response() {
        let buckets = vec![BucketSummary {
            name: storage::BucketName::try_from("test-bucket").unwrap(),
            owner_principal: "owner".into(),
            owner_canonical_id: CanonicalUserId::from_principal("owner"),
            created_at: 1000,
            acl_grants: AclGrants::default(),
            versioning: BucketVersioningState::Disabled,
            object_lock: s3_types::BucketObjectLockConfig::default(),
            public_read: false,
            public_write: false,
            public_access_block: None,
            ownership_controls: None,
            bucket_policy_present: false,
            bucket_policy_public: false,
            bucket_policy_generation: 0,
            bucket_lifecycle_present: false,
            bucket_lifecycle_generation: 0,
            bucket_abac_enabled: false,
            encryption: EffectiveBucketEncryptionConfig::default(),
        }];
        let owner_canonical_id = CanonicalUserId::from_principal("owner");
        let resp = S3Response::list_buckets(&buckets, "Owner A", &owner_canonical_id);
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "Content-Type"), Some("application/xml"));
        let body = String::from_utf8(resp.body).unwrap();
        assert!(body.contains("<?xml"));
        assert!(body.contains("test-bucket"));
        assert!(body.contains("ListAllMyBucketsResult"));
        assert!(body.contains(owner_canonical_id.as_str()));
        assert!(body.contains("<DisplayName>Owner A</DisplayName>"));
    }

    #[test]
    fn bucket_lifecycle_responses() {
        let put = S3Response::put_bucket_lifecycle();
        assert_eq!(put.status_code, 200);
        assert!(put.body.is_empty());

        let get = S3Response::get_bucket_lifecycle(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><LifecycleConfiguration/>",
        );
        assert_eq!(get.status_code, 200);
        assert_eq!(find_header(&get, "Content-Type"), None);
        assert_eq!(
            find_header(&get, "x-amz-transition-default-minimum-object-size"),
            Some("all_storage_classes_128K")
        );

        let delete = S3Response::delete_bucket_lifecycle();
        assert_eq!(delete.status_code, 204);
        assert!(delete.body.is_empty());
    }

    #[test]
    fn create_multipart_upload_response_includes_lifecycle_abort_headers() {
        let upload_id = UploadId::try_from(".".repeat(storage::UPLOAD_ID_LEN)).unwrap();
        let resp = S3Response::create_multipart_upload(
            "bucket",
            "key",
            &upload_id,
            CreateMultipartUploadResponseContext {
                managed_encryption: None,
                checksum_algorithm: Some(ChecksumAlgorithm::Sha256),
                checksum_type: Some(ChecksumType::FullObject),
                lifecycle_abort: Some(&LifecycleAbortHeaders {
                    abort_time_millis: 1_705_321_845_000,
                    rule_id: Some("abort upload".to_string()),
                }),
                sse_customer: None,
            },
        );
        assert_eq!(
            find_header(&resp, "x-amz-abort-date"),
            Some("Mon, 15 Jan 2024 12:30:45 GMT")
        );
        assert_eq!(
            find_header(&resp, "x-amz-abort-rule-id"),
            Some("abort%20upload")
        );
        assert_eq!(find_header(&resp, "Content-Type"), None);
        assert_eq!(find_header(&resp, "Content-Length"), None);
        assert_eq!(
            find_header(&resp, "x-amz-checksum-algorithm"),
            Some("SHA256")
        );
        assert_eq!(
            find_header(&resp, "x-amz-checksum-type"),
            Some("FULL_OBJECT")
        );
    }

    #[test]
    fn create_multipart_upload_response_includes_managed_encryption_header() {
        let upload_id = UploadId::try_from(".".repeat(storage::UPLOAD_ID_LEN)).unwrap();
        let resp = S3Response::create_multipart_upload(
            "bucket",
            "key",
            &upload_id,
            CreateMultipartUploadResponseContext {
                managed_encryption: Some(ManagedEncryptionAlgorithm::Aes256),
                checksum_algorithm: None,
                checksum_type: None,
                lifecycle_abort: None,
                sse_customer: None,
            },
        );
        assert_eq!(
            find_header(&resp, "x-amz-server-side-encryption"),
            Some("AES256")
        );
    }

    #[test]
    fn list_parts_response_includes_lifecycle_abort_headers() {
        let resp = S3Response::list_parts(
            "bucket",
            "key",
            "upload-1",
            None,
            1000,
            &ListPartsResult {
                parts: Vec::new(),
                is_truncated: false,
                next_part_number_marker: None,
                checksum_algorithm: None,
                checksum_type: None,
                lifecycle_abort: Some(LifecycleAbortHeaders {
                    abort_time_millis: 1_705_321_845_000,
                    rule_id: Some("abort upload".to_string()),
                }),
            },
        );
        assert_eq!(
            find_header(&resp, "x-amz-abort-date"),
            Some("Mon, 15 Jan 2024 12:30:45 GMT")
        );
        assert_eq!(
            find_header(&resp, "x-amz-abort-rule-id"),
            Some("abort%20upload")
        );
        assert_eq!(find_header(&resp, "Content-Type"), Some("application/xml"));
        assert_eq!(find_header(&resp, "Content-Length"), None);
    }

    #[test]
    fn complete_multipart_upload_response_includes_lifecycle_expiration_header() {
        let resp = S3Response::complete_multipart_upload(
            "bucket",
            "key",
            &CompleteMultipartUploadResult {
                etag: "\"etag\"".to_string(),
                version_id: VersionId::Null,
                managed_encryption: None,
                checksum_algorithm: None,
                checksum_type: None,
                checksum_value: None,
                lifecycle_expiration: Some(LifecycleExpirationHeader {
                    expiry_time_millis: 1_705_321_845_000,
                    rule_id: Some("complete".to_string()),
                }),
            },
        );
        assert_eq!(
            find_header(&resp, "x-amz-expiration"),
            Some("expiry-date=\"Mon, 15 Jan 2024 12:30:45 GMT\", rule-id=\"complete\"")
        );
        assert_eq!(find_header(&resp, "Content-Type"), Some("application/xml"));
        assert_eq!(find_header(&resp, "Content-Length"), None);
        assert_eq!(find_header(&resp, "x-amz-checksum-algorithm"), None);
        assert_eq!(find_header(&resp, "x-amz-checksum-type"), None);
    }

    #[test]
    fn get_bucket_policy_status_response() {
        let resp = S3Response::get_bucket_policy_status(true);
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "Content-Type"), Some("application/xml"));
        let body = String::from_utf8(resp.body).unwrap();
        assert!(body.contains("<PolicyStatus"));
        assert!(body.contains("<IsPublic>true</IsPublic>"));
    }

    // ── list_objects_v2 ───────────────────────────────────────────────

    #[test]
    fn list_objects_v2_response() {
        let result = ListObjectsResult {
            objects: vec![ListEntry {
                key: "key1".into(),
                size: 42,
                etag: "\"etag1\"".into(),
                last_modified: 0,
                checksum_algorithm: Some(ChecksumAlgorithm::Crc32),
                checksum_type: Some(ChecksumType::FullObject),
            }],
            common_prefixes: vec![],
            is_truncated: false,
            next_continuation_token: None,
            owner_principal: "owner".into(),
            owner_canonical_id: CanonicalUserId::from_principal("owner"),
        };
        let resp = S3Response::list_objects_v2(
            "bucket",
            "us-east-1",
            Some("pre"),
            None,
            None,
            None,
            None,
            true,
            1000,
            &result,
        );
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "Content-Type"), Some("application/xml"));
        assert_eq!(find_header(&resp, "Content-Length"), None);
        assert_eq!(find_header(&resp, "x-amz-bucket-region"), Some("us-east-1"));
        assert!(resp.body.is_empty());
        assert!(resp.stream.is_some());
    }

    // ── list_objects_v1 ───────────────────────────────────────────────

    #[test]
    fn list_objects_v1_response() {
        let result = ListObjectsResult {
            objects: vec![ListEntry {
                key: "key1".into(),
                size: 42,
                etag: "\"etag1\"".into(),
                last_modified: 0,
                checksum_algorithm: Some(ChecksumAlgorithm::Crc32),
                checksum_type: Some(ChecksumType::FullObject),
            }],
            common_prefixes: vec![],
            is_truncated: false,
            next_continuation_token: None,
            owner_principal: "owner".into(),
            owner_canonical_id: CanonicalUserId::from_principal("owner"),
        };
        let resp = S3Response::list_objects_v1(
            "bucket",
            "us-east-1",
            Some("pre"),
            None,
            None,
            None,
            1000,
            &result,
        );
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "Content-Type"), Some("application/xml"));
        assert_eq!(find_header(&resp, "Content-Length"), None);
        assert_eq!(find_header(&resp, "x-amz-bucket-region"), Some("us-east-1"));
        assert!(resp.body.is_empty());
        assert!(resp.stream.is_some());
    }

    #[test]
    fn get_bucket_acl_response_uses_canonical_owner_id() {
        let owner_canonical_id = CanonicalUserId::from_principal("owner");
        let result = GetBucketAclResult {
            owner_principal: "owner".into(),
            owner_canonical_id: owner_canonical_id.clone(),
            acl_grants: AclGrants::new(vec![
                AclGrant::new(
                    AclGrantee::CanonicalUser(owner_canonical_id.clone()),
                    AclPermission::FullControl,
                ),
                AclGrant::new(AclGrantee::AllUsers, AclPermission::Read),
            ]),
        };
        let grants = vec![
            xml::RenderedAclGrant {
                grantee: AclGrantee::CanonicalUser(owner_canonical_id.clone()),
                permission: AclPermission::FullControl,
                display_name: Some("owner".to_string()),
            },
            xml::RenderedAclGrant {
                grantee: AclGrantee::AllUsers,
                permission: AclPermission::Read,
                display_name: None,
            },
        ];
        let resp = S3Response::get_bucket_acl(&result, "owner", &grants);
        assert_eq!(resp.status_code, 200);
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains(owner_canonical_id.as_str()));
        assert!(!body.contains("<DisplayName>"));
    }

    // ── error ─────────────────────────────────────────────────────────

    #[test]
    fn error_response_404() {
        let err = ServerError::BucketNotFound { name: "b".into() };
        let resp = S3Response::error(&err, "/b", TEST_HOST_ID);
        assert_eq!(resp.status_code, 404);
        assert_eq!(find_header(&resp, "Content-Type"), Some("application/xml"));
        assert_eq!(find_header(&resp, "Content-Length"), None);
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("NoSuchBucket"));
    }

    #[test]
    fn error_response_no_such_upload_uses_aws_shape() {
        let err = ServerError::NoSuchUpload {
            upload_id: "abc".into(),
        };
        let resp = S3Response::error(&err, "/bucket/key", TEST_HOST_ID);
        assert_eq!(resp.status_code, 404);
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("<Code>NoSuchUpload</Code>"));
        assert!(body.contains(
            "<Message>The specified upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.</Message>"
        ));
        assert!(body.contains("<UploadId>abc</UploadId>"));
    }

    #[test]
    fn invalid_redirect_location_error_response_uses_host_id_without_resource() {
        let err = ServerError::InvalidRedirectLocation {
            reason: "The website redirect location must have a prefix of 'http://' or 'https://' or '/'.".to_string(),
        };
        let resp = S3Response::error(&err, "/bucket/key", TEST_HOST_ID);
        assert_eq!(resp.status_code, 400);
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("<Code>InvalidRedirectLocation</Code>"));
        assert!(body.contains("<HostId>"));
        assert!(!body.contains("<Resource>"));
    }

    #[test]
    fn metadata_too_large_detailed_error_response_includes_host_id() {
        let err = ServerError::MetadataTooLargeDetailed {
            size: 2049,
            max_size_allowed: 2048,
        };
        let resp = S3Response::error(&err, "/bucket/key", TEST_HOST_ID);
        assert_eq!(resp.status_code, 400);
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("<Code>MetadataTooLarge</Code>"));
        assert!(body.contains("<Size>2049</Size>"));
        assert!(body.contains("<MaxSizeAllowed>2048</MaxSizeAllowed>"));
        assert!(body.contains("<HostId>"));
        assert!(!body.contains("<Resource>"));
    }

    #[test]
    fn error_response_uses_attached_request_id() {
        let _trace = observability::AttachedTrace::new(observability::TraceContext::from_ids(
            "0123456789abcdef0123456789abcdef".to_string(),
            "2VG1X5NNMZ52HKC0".to_string(),
        ));
        let err = ServerError::Auth(auth::AuthError::MissingAuth);
        let resp = S3Response::error(&err, "/", TEST_HOST_ID);
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("<RequestId>request-id</RequestId>"));
        assert!(body.contains("<HostId>host-id</HostId>"));
    }

    #[test]
    fn error_response_403() {
        let err = ServerError::Auth(auth::AuthError::MissingAuth);
        let resp = S3Response::error(&err, "/", TEST_HOST_ID);
        assert_eq!(resp.status_code, 403);
        assert_eq!(find_header(&resp, "Content-Type"), Some("application/xml"));
        assert_eq!(find_header(&resp, "Content-Length"), None);
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("AccessDenied"));
    }

    #[test]
    fn presigned_expired_error_response_uses_access_denied_shape() {
        let err = ServerError::Auth(auth::AuthError::PresignedRequestExpired);
        let wire_ids = WireResponseIds::new("2VG1X5NNMZ52HKC0", TEST_HOST_ID);
        let resp = S3Response::error_with_ids(&err, "/bucket/key", &wire_ids);
        assert_eq!(resp.status_code, 403);
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("<Code>AccessDenied</Code>"));
        assert!(body.contains("<Message>Request has expired</Message>"));
        assert!(body.contains("<RequestId>2VG1X5NNMZ52HKC0</RequestId>"));
        assert!(body.contains("<HostId>host-id</HostId>"));
        assert!(!body.contains("<Resource>"));
    }

    #[test]
    fn invalid_sse_customer_key_md5_omits_resource() {
        let err = ServerError::InvalidSseCustomerKeyMd5;
        let wire_ids = WireResponseIds::new("2VG1X5NNMZ52HKC0", TEST_HOST_ID);
        let resp = S3Response::error_with_ids(&err, "/bucket/key", &wire_ids);
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("<ArgumentName>x-amz-server-side-encryption</ArgumentName>"));
        assert!(!body.contains("<Resource>"));
        assert!(body.contains("<HostId>host-id</HostId>"));
    }

    #[test]
    fn sse_c_blocked_access_denied_uses_detailed_message() {
        let err = ServerError::SseCBlockedAccessDenied {
            requester_principal: "arn:aws:iam::111122223333:user/test".to_string(),
            action: "s3:PutObject".to_string(),
            resource: "arn:aws:s3:::bucket/key".to_string(),
        };
        let wire_ids = WireResponseIds::new("2VG1X5NNMZ52HKC0", TEST_HOST_ID);
        let resp = S3Response::error_with_ids(&err, "/bucket/key", &wire_ids);
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("because this bucket has blocked upload requests"));
        assert!(body.contains("arn:aws:s3:::bucket/key"));
        assert!(body.contains("arn:aws:iam::111122223333:user/test"));
    }

    #[test]
    fn error_response_500() {
        let err = ServerError::Store(storage::StoreError::NotFound);
        let resp = S3Response::error(&err, "/x", TEST_HOST_ID);
        assert_eq!(resp.status_code, 500);
        assert_eq!(find_header(&resp, "Content-Type"), Some("application/xml"));
        assert_eq!(find_header(&resp, "Content-Length"), None);
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("InternalError"));
        assert!(body.contains("We encountered an internal error. Please try again."));
        assert!(!body.contains("shard not found"));
    }

    #[test]
    fn error_response_internal_reason_is_sanitized() {
        let err = ServerError::InternalError {
            reason: "sqlite path /tmp/secret.db".to_string(),
        };
        let resp = S3Response::error(&err, "/x", TEST_HOST_ID);
        assert_eq!(resp.status_code, 500);
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("InternalError"));
        assert!(body.contains("We encountered an internal error. Please try again."));
        assert!(!body.contains("/tmp/secret.db"));
    }

    #[test]
    fn error_response_metadata_blob_reason_is_sanitized() {
        let err = ServerError::MetadataBlobError {
            reason: "invalid metadata bytes: 0xFF".to_string(),
        };
        let resp = S3Response::error(&err, "/x", TEST_HOST_ID);
        assert_eq!(resp.status_code, 500);
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("InternalError"));
        assert!(body.contains("We encountered an internal error. Please try again."));
        assert!(!body.contains("0xFF"));
    }

    #[test]
    fn error_response_internal_storage_and_ec_errors_are_sanitized() {
        let cases: Vec<(ServerError, Vec<&str>)> = vec![
            (
                ServerError::Store(storage::StoreError::Io {
                    context: "read shard row",
                    source: std::io::Error::other("sqlite path /tmp/secret.db near table shards"),
                }),
                vec!["sqlite", "/tmp/secret.db", "read shard row", "table shards"],
            ),
            (
                ServerError::Metadata(storage::error::MetadataError::NotImplemented {
                    context: "sqlite path /tmp/secret.db UNIQUE constraint failed: objects.key",
                }),
                vec![
                    "UNIQUE constraint",
                    "objects.key",
                    "/tmp/secret.db",
                    "sqlite",
                ],
            ),
            (
                ServerError::Ec(ec::EcError::SmokeTestFailed {
                    reason: "matrix inversion failed in /tmp/secret-ec".to_string(),
                }),
                vec!["matrix inversion failed", "/tmp/secret-ec"],
            ),
        ];

        for (err, leaks) in cases {
            let body = String::from_utf8(
                S3Response::error(&err, "/x", TEST_HOST_ID)
                    .into_test_body_bytes()
                    .unwrap(),
            )
            .unwrap();
            assert!(body.contains("InternalError"));
            assert!(body.contains("We encountered an internal error. Please try again."));
            for leak in leaks {
                assert!(!body.contains(leak), "unexpected leak {leak:?} in {body}");
            }
        }
    }

    #[test]
    fn slow_down_response_has_retry_after() {
        let resp = S3Response::error(&ServerError::SlowDown, "/", TEST_HOST_ID);
        assert_eq!(resp.status_code, 503);
        assert_eq!(find_header(&resp, "Retry-After"), Some("1"));
        assert_eq!(find_header(&resp, "Content-Length"), None);
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("SlowDown"));
        assert!(body.contains("<Message>Please reduce your request rate.</Message>"));
    }

    #[test]
    fn error_response_has_xml_content_type() {
        let err = ServerError::MethodNotAllowed;
        let resp = S3Response::error(&err, "/", TEST_HOST_ID);
        assert_eq!(find_header(&resp, "Content-Type"), Some("application/xml"));
        assert_eq!(find_header(&resp, "Content-Length"), None);
        assert!(resp.stream.is_some());
    }

    // ── delete_objects ───────────────────────────────────────────────

    #[test]
    fn delete_objects_response() {
        use crate::coordinator::{DeleteObjectsResult, DeletedObject};
        let result = DeleteObjectsResult {
            deleted: vec![DeletedObject {
                key: "key1".into(),
                version_id: VersionId::Null,
                delete_marker: false,
            }],
            errors: vec![],
        };
        let resp = S3Response::delete_objects(&result, false);
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "Content-Type"), Some("application/xml"));
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("DeleteResult"));
        assert!(body.contains("key1"));
    }

    // ── list_object_versions ────────────────────────────────────────

    #[test]
    fn list_object_versions_response() {
        use crate::coordinator::{ListObjectVersionsResult, VersionEntry};
        let result = ListObjectVersionsResult {
            versions: vec![VersionEntry {
                key: "key1".into(),
                version_id: VersionId::Null,
                is_latest: true,
                size: 42,
                etag: "\"etag1\"".into(),
                last_modified: 0,
                is_delete_marker: false,
                checksum_algorithm: None,
                checksum_type: None,
            }],
            common_prefixes: vec![],
            is_truncated: false,
            next_key_marker: None,
            next_version_id_marker: None,
            owner_principal: "owner".into(),
            owner_canonical_id: CanonicalUserId::from_principal("owner"),
        };
        let resp =
            S3Response::list_object_versions("bucket", None, None, None, None, 1000, &result);
        assert_eq!(resp.status_code, 200);
        assert_eq!(find_header(&resp, "Content-Type"), Some("application/xml"));
        assert_eq!(find_header(&resp, "Content-Length"), None);
        assert!(resp.body.is_empty());
        assert!(resp.stream.is_some());
    }

    // ── parse_http_date ────────────────────────────────────────────

    #[test]
    fn parse_http_date_epoch() {
        assert_eq!(parse_http_date("Thu, 01 Jan 1970 00:00:00 GMT"), Some(0));
    }

    #[test]
    fn parse_http_date_known_date() {
        assert_eq!(
            parse_http_date("Mon, 15 Jan 2024 12:30:45 GMT"),
            Some(1705321845000)
        );
    }

    #[test]
    fn parse_http_date_round_trip() {
        let millis = 1705321845000u64;
        let formatted = format_http_date(millis);
        assert_eq!(parse_http_date(&formatted), Some(millis));
    }

    #[test]
    fn parse_http_date_round_trip_epoch() {
        let formatted = format_http_date(0);
        assert_eq!(parse_http_date(&formatted), Some(0));
    }

    #[test]
    fn parse_http_date_rejects_pre_epoch_date() {
        assert_eq!(parse_http_date("Wed, 31 Dec 1969 23:59:59 GMT"), None);
    }

    #[test]
    fn parse_http_date_invalid() {
        assert_eq!(parse_http_date("not a date"), None);
        assert_eq!(parse_http_date(""), None);
        assert_eq!(parse_http_date("����������������GMT"), None);
    }

    #[test]
    fn date_to_days_epoch() {
        assert_eq!(date_to_days(1970, 1, 1), 0);
    }

    #[test]
    fn date_to_days_round_trip() {
        for d in [0i64, 1, 365, 10957, 11016, 19782] {
            let (y, m, day) = days_to_date(d);
            assert_eq!(date_to_days(y, m, day), d, "failed round-trip for day {d}");
        }
    }

    // ── not_modified / precondition_failed ────────────────────────

    #[test]
    fn not_modified_response_has_etag_and_last_modified() {
        let resp = S3Response::not_modified("\"abcdef1234567890\"", 1705321845000);
        assert_eq!(resp.status_code, 304);
        assert_eq!(find_header(&resp, "ETag"), Some("\"abcdef1234567890\""));
        assert_eq!(
            find_header(&resp, "Last-Modified"),
            Some("Mon, 15 Jan 2024 12:30:45 GMT")
        );
        assert!(resp.body.is_empty());
    }

    #[test]
    fn precondition_failed_response_412() {
        let resp = S3Response::precondition_failed();
        assert_eq!(resp.status_code, 412);
        assert_eq!(find_header(&resp, "Content-Type"), Some("application/xml"));
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("PreconditionFailed"));
    }
}
