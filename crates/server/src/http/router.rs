/// S3 operation routing from HTTP method + path + query.
use crate::error::ServerError;

/// Recognized S3 operations.
#[derive(Debug, PartialEq, Eq)]
pub enum S3Operation {
    ListBuckets,
    CreateBucket { bucket: String },
    DeleteBucket { bucket: String },
    HeadBucket { bucket: String },
    ListObjectsV1 { bucket: String },
    ListObjectsV2 { bucket: String },
    PutObject { bucket: String, key: String },
    GetObject { bucket: String, key: String },
    DeleteObject { bucket: String, key: String },
    HeadObject { bucket: String, key: String },
    PostObject { bucket: String },
    DeleteObjects { bucket: String },
    ListObjectVersions { bucket: String },
    PutBucketVersioning { bucket: String },
    GetBucketVersioning { bucket: String },
    PutBucketCors { bucket: String },
    GetBucketCors { bucket: String },
    DeleteBucketCors { bucket: String },
    PutBucketTagging { bucket: String },
    GetBucketTagging { bucket: String },
    DeleteBucketTagging { bucket: String },
    PutObjectTagging { bucket: String, key: String },
    GetObjectTagging { bucket: String, key: String },
    DeleteObjectTagging { bucket: String, key: String },
    PutBucketPublicAccessBlock { bucket: String },
    GetBucketPublicAccessBlock { bucket: String },
    DeleteBucketPublicAccessBlock { bucket: String },
    PutBucketAcl { bucket: String },
    PutBucketOwnershipControls { bucket: String },
    GetBucketOwnershipControls { bucket: String },
    DeleteBucketOwnershipControls { bucket: String },
    GetObjectAttributes { bucket: String, key: String },
    CreateMultipartUpload { bucket: String, key: String },
    UploadPart { bucket: String, key: String },
    CompleteMultipartUpload { bucket: String, key: String },
    AbortMultipartUpload { bucket: String, key: String },
    ListMultipartUploads { bucket: String },
    ListParts { bucket: String, key: String },
    OptionsRequest { bucket: String, key: Option<String> },
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
fn validate_bucket_name(name: &str) -> Result<(), ServerError> {
    if name.len() < 3 || name.len() > 63 {
        return Err(ServerError::InvalidBucketName {
            reason: format!("bucket name must be 3-63 characters, got {}", name.len()),
        });
    }
    // Must start and end with a lowercase letter or digit
    let first = name.as_bytes()[0];
    let last = name.as_bytes()[name.len() - 1];
    if !(first.is_ascii_lowercase() || first.is_ascii_digit()) {
        return Err(ServerError::InvalidBucketName {
            reason: "bucket name must start with a lowercase letter or digit".to_string(),
        });
    }
    if !(last.is_ascii_lowercase() || last.is_ascii_digit()) {
        return Err(ServerError::InvalidBucketName {
            reason: "bucket name must end with a lowercase letter or digit".to_string(),
        });
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.')
    {
        return Err(ServerError::InvalidBucketName {
            reason: "bucket name must contain only lowercase letters, digits, hyphens, and periods"
                .to_string(),
        });
    }
    if name.contains("..") {
        return Err(ServerError::InvalidBucketName {
            reason: "bucket name must not contain consecutive periods".to_string(),
        });
    }
    if name.contains(".-") || name.contains("-.") {
        return Err(ServerError::InvalidBucketName {
            reason: "bucket name must not contain dot-dash or dash-dot".to_string(),
        });
    }
    // Reject xn-- prefix (reserved for Internationalized Domain Names)
    if name.starts_with("xn--") {
        return Err(ServerError::InvalidBucketName {
            reason: "bucket name must not start with xn-- (reserved for IDN)".to_string(),
        });
    }
    // Reject IP-address-formatted names (4 groups of digits separated by periods)
    let parts: Vec<&str> = name.split('.').collect();
    if parts.len() == 4 && parts.iter().all(|p| p.parse::<u8>().is_ok()) {
        return Err(ServerError::InvalidBucketName {
            reason: "bucket name must not be formatted as an IP address".to_string(),
        });
    }
    Ok(())
}

/// Validate an S3 object key.
/// 1-1024 bytes, no null bytes.
fn validate_object_key(key: &str) -> Result<(), ServerError> {
    if key.is_empty() || key.len() > 1024 {
        return Err(ServerError::InvalidRequest {
            reason: format!("object key must be 1-1024 bytes, got {}", key.len()),
        });
    }
    if key.as_bytes().contains(&0) {
        return Err(ServerError::InvalidRequest {
            reason: "object key must not contain null bytes".to_string(),
        });
    }
    if key.chars().any(|c| {
        let code = c as u32;
        (code <= 0x1F) || (0x7F..=0x9F).contains(&code)
    }) {
        return Err(ServerError::InvalidRequest {
            reason: "Couldn't parse the specified URI.".to_string(),
        });
    }
    Ok(())
}

/// Check if a bare query parameter key is present (e.g. "delete" in "?delete").
fn has_query_key(query: &str, target: &str) -> bool {
    query.split('&').filter(|s| !s.is_empty()).any(|pair| {
        let key = pair.split('=').next().unwrap_or("");
        key == target
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

    validate_bucket_name(bucket)?;

    let decoded_key = key
        .map(crate::http::request::percent_decode_strict)
        .transpose()?;
    if let Some(ref k) = decoded_key {
        validate_object_key(k)?;
    }

    match (method, decoded_key) {
        // OPTIONS requests (preflight CORS)
        ("OPTIONS", key) => Ok(S3Operation::OptionsRequest {
            bucket: bucket.to_string(),
            key,
        }),

        // Bucket-level operations (no key)
        ("PUT", None) if has_query_key(query, "versioning") => {
            Ok(S3Operation::PutBucketVersioning {
                bucket: bucket.to_string(),
            })
        }
        ("PUT", None) if has_query_key(query, "cors") => Ok(S3Operation::PutBucketCors {
            bucket: bucket.to_string(),
        }),
        ("PUT", None) if has_query_key(query, "tagging") => Ok(S3Operation::PutBucketTagging {
            bucket: bucket.to_string(),
        }),
        ("PUT", None) if has_query_key(query, "publicAccessBlock") => {
            Ok(S3Operation::PutBucketPublicAccessBlock {
                bucket: bucket.to_string(),
            })
        }
        ("PUT", None) if has_query_key(query, "acl") => Ok(S3Operation::PutBucketAcl {
            bucket: bucket.to_string(),
        }),
        ("PUT", None) if has_query_key(query, "ownershipControls") => {
            Ok(S3Operation::PutBucketOwnershipControls {
                bucket: bucket.to_string(),
            })
        }
        ("PUT", None) => Ok(S3Operation::CreateBucket {
            bucket: bucket.to_string(),
        }),
        ("DELETE", None) if has_query_key(query, "cors") => Ok(S3Operation::DeleteBucketCors {
            bucket: bucket.to_string(),
        }),
        ("DELETE", None) if has_query_key(query, "publicAccessBlock") => {
            Ok(S3Operation::DeleteBucketPublicAccessBlock {
                bucket: bucket.to_string(),
            })
        }
        ("DELETE", None) if has_query_key(query, "ownershipControls") => {
            Ok(S3Operation::DeleteBucketOwnershipControls {
                bucket: bucket.to_string(),
            })
        }
        ("DELETE", None) if has_query_key(query, "tagging") => {
            Ok(S3Operation::DeleteBucketTagging {
                bucket: bucket.to_string(),
            })
        }
        ("DELETE", None) => Ok(S3Operation::DeleteBucket {
            bucket: bucket.to_string(),
        }),
        ("HEAD", None) => Ok(S3Operation::HeadBucket {
            bucket: bucket.to_string(),
        }),
        ("GET", None) => {
            // Check for ?uploads → ListMultipartUploads
            if has_query_key(query, "uploads") {
                return Ok(S3Operation::ListMultipartUploads {
                    bucket: bucket.to_string(),
                });
            }
            // Check for ?ownershipControls → GetBucketOwnershipControls
            if has_query_key(query, "ownershipControls") {
                return Ok(S3Operation::GetBucketOwnershipControls {
                    bucket: bucket.to_string(),
                });
            }
            // Check for ?publicAccessBlock → GetBucketPublicAccessBlock
            if has_query_key(query, "publicAccessBlock") {
                return Ok(S3Operation::GetBucketPublicAccessBlock {
                    bucket: bucket.to_string(),
                });
            }
            // Check for ?cors → GetBucketCors
            if has_query_key(query, "cors") {
                return Ok(S3Operation::GetBucketCors {
                    bucket: bucket.to_string(),
                });
            }
            // Check for ?tagging → GetBucketTagging
            if has_query_key(query, "tagging") {
                return Ok(S3Operation::GetBucketTagging {
                    bucket: bucket.to_string(),
                });
            }
            // Check for ?versioning → GetBucketVersioning
            if has_query_key(query, "versioning") {
                return Ok(S3Operation::GetBucketVersioning {
                    bucket: bucket.to_string(),
                });
            }
            // Check for ?versions → ListObjectVersions
            if has_query_key(query, "versions") {
                return Ok(S3Operation::ListObjectVersions {
                    bucket: bucket.to_string(),
                });
            }
            // Check for list-type=2 → V2, otherwise → V1
            let is_v2 = query.split('&').filter(|s| !s.is_empty()).any(|pair| {
                let mut parts = pair.splitn(2, '=');
                let key = parts.next().unwrap_or("");
                let val = parts.next().unwrap_or("");
                key == "list-type" && val == "2"
            });
            if is_v2 {
                Ok(S3Operation::ListObjectsV2 {
                    bucket: bucket.to_string(),
                })
            } else {
                Ok(S3Operation::ListObjectsV1 {
                    bucket: bucket.to_string(),
                })
            }
        }
        ("POST", None) => {
            if has_query_key(query, "delete") {
                Ok(S3Operation::DeleteObjects {
                    bucket: bucket.to_string(),
                })
            } else {
                Ok(S3Operation::PostObject {
                    bucket: bucket.to_string(),
                })
            }
        }

        // Object-level tagging (must appear before catch-all)
        ("PUT", Some(key)) if has_query_key(query, "tagging") => {
            Ok(S3Operation::PutObjectTagging {
                bucket: bucket.to_string(),
                key,
            })
        }
        ("GET", Some(key)) if has_query_key(query, "tagging") => {
            Ok(S3Operation::GetObjectTagging {
                bucket: bucket.to_string(),
                key,
            })
        }
        ("DELETE", Some(key)) if has_query_key(query, "tagging") => {
            Ok(S3Operation::DeleteObjectTagging {
                bucket: bucket.to_string(),
                key,
            })
        }

        // GetObjectAttributes (must appear before catch-all GET)
        ("GET", Some(key)) if has_query_key(query, "attributes") => {
            Ok(S3Operation::GetObjectAttributes {
                bucket: bucket.to_string(),
                key,
            })
        }

        // Multipart upload operations (must appear before catch-all object operations)
        ("POST", Some(key)) if has_query_key(query, "uploads") => {
            Ok(S3Operation::CreateMultipartUpload {
                bucket: bucket.to_string(),
                key,
            })
        }
        ("POST", Some(key)) if has_query_key(query, "uploadId") => {
            Ok(S3Operation::CompleteMultipartUpload {
                bucket: bucket.to_string(),
                key,
            })
        }
        ("PUT", Some(key)) if has_query_key(query, "partNumber") => {
            Ok(S3Operation::UploadPart {
                bucket: bucket.to_string(),
                key,
            })
        }
        ("DELETE", Some(key)) if has_query_key(query, "uploadId") => {
            Ok(S3Operation::AbortMultipartUpload {
                bucket: bucket.to_string(),
                key,
            })
        }
        ("GET", Some(key)) if has_query_key(query, "uploadId") => {
            Ok(S3Operation::ListParts {
                bucket: bucket.to_string(),
                key,
            })
        }

        // Object-level operations
        ("PUT", Some(key)) => Ok(S3Operation::PutObject {
            bucket: bucket.to_string(),
            key,
        }),
        ("GET", Some(key)) => Ok(S3Operation::GetObject {
            bucket: bucket.to_string(),
            key,
        }),
        ("DELETE", Some(key)) => Ok(S3Operation::DeleteObject {
            bucket: bucket.to_string(),
            key,
        }),
        ("HEAD", Some(key)) => Ok(S3Operation::HeadObject {
            bucket: bucket.to_string(),
            key,
        }),

        _ => Err(ServerError::MethodNotAllowed),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_buckets() {
        assert_eq!(route("GET", "/", "").unwrap(), S3Operation::ListBuckets);
    }

    #[test]
    fn create_bucket() {
        assert_eq!(
            route("PUT", "/mybucket", "").unwrap(),
            S3Operation::CreateBucket {
                bucket: "mybucket".to_string()
            }
        );
    }

    #[test]
    fn delete_bucket() {
        assert_eq!(
            route("DELETE", "/mybucket", "").unwrap(),
            S3Operation::DeleteBucket {
                bucket: "mybucket".to_string()
            }
        );
    }

    #[test]
    fn head_bucket() {
        assert_eq!(
            route("HEAD", "/mybucket", "").unwrap(),
            S3Operation::HeadBucket {
                bucket: "mybucket".to_string()
            }
        );
    }

    #[test]
    fn list_objects_v2() {
        assert_eq!(
            route("GET", "/mybucket", "list-type=2").unwrap(),
            S3Operation::ListObjectsV2 {
                bucket: "mybucket".to_string()
            }
        );
    }

    #[test]
    fn list_objects_v1_default() {
        assert_eq!(
            route("GET", "/mybucket", "").unwrap(),
            S3Operation::ListObjectsV1 {
                bucket: "mybucket".to_string()
            }
        );
    }

    #[test]
    fn list_objects_v1_without_list_type() {
        assert_eq!(
            route("GET", "/mybucket", "prefix=foo").unwrap(),
            S3Operation::ListObjectsV1 {
                bucket: "mybucket".to_string()
            }
        );
    }

    #[test]
    fn list_objects_v1_explicit_list_type_1() {
        assert_eq!(
            route("GET", "/mybucket", "list-type=1").unwrap(),
            S3Operation::ListObjectsV1 {
                bucket: "mybucket".to_string()
            }
        );
    }

    #[test]
    fn list_objects_v2_with_other_params() {
        assert_eq!(
            route("GET", "/mybucket", "list-type=2&prefix=foo&max-keys=10").unwrap(),
            S3Operation::ListObjectsV2 {
                bucket: "mybucket".to_string()
            }
        );
    }

    #[test]
    fn get_bucket_trailing_slash_is_list_v1() {
        assert_eq!(
            route("GET", "/mybucket/", "").unwrap(),
            S3Operation::ListObjectsV1 {
                bucket: "mybucket".to_string()
            }
        );
    }

    #[test]
    fn put_object() {
        assert_eq!(
            route("PUT", "/bucket/key.txt", "").unwrap(),
            S3Operation::PutObject {
                bucket: "bucket".to_string(),
                key: "key.txt".to_string()
            }
        );
    }

    #[test]
    fn get_object() {
        assert_eq!(
            route("GET", "/bucket/path/to/key", "").unwrap(),
            S3Operation::GetObject {
                bucket: "bucket".to_string(),
                key: "path/to/key".to_string()
            }
        );
    }

    #[test]
    fn delete_object() {
        assert_eq!(
            route("DELETE", "/bucket/key", "").unwrap(),
            S3Operation::DeleteObject {
                bucket: "bucket".to_string(),
                key: "key".to_string()
            }
        );
    }

    #[test]
    fn head_object() {
        assert_eq!(
            route("HEAD", "/bucket/key", "").unwrap(),
            S3Operation::HeadObject {
                bucket: "bucket".to_string(),
                key: "key".to_string()
            }
        );
    }

    #[test]
    fn nested_key_path() {
        assert_eq!(
            route("GET", "/bucket/a/b/c/d.txt", "").unwrap(),
            S3Operation::GetObject {
                bucket: "bucket".to_string(),
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
    fn object_key_rejects_control_chars() {
        let err = route("GET", "/bucket/\u{008A}-", "").unwrap_err();
        match err {
            ServerError::InvalidRequest { reason } => {
                assert_eq!(reason, "Couldn't parse the specified URI.");
            }
            _ => panic!("expected InvalidRequest, got {err:?}"),
        }
    }

    #[test]
    fn key_with_trailing_slash() {
        assert_eq!(
            route("PUT", "/bucket/key/", "").unwrap(),
            S3Operation::PutObject {
                bucket: "bucket".to_string(),
                key: "key/".to_string()
            }
        );
    }

    #[test]
    fn bucket_trailing_slash_is_bucket_op() {
        assert_eq!(
            route("HEAD", "/mybucket/", "").unwrap(),
            S3Operation::HeadBucket {
                bucket: "mybucket".to_string()
            }
        );
    }

    #[test]
    fn nested_key_with_trailing_slash() {
        assert_eq!(
            route("GET", "/bucket/a/b/c/", "").unwrap(),
            S3Operation::GetObject {
                bucket: "bucket".to_string(),
                key: "a/b/c/".to_string()
            }
        );
    }

    #[test]
    fn object_key_too_long() {
        let long_key = "k".repeat(1025);
        let path = format!("/bucket/{}", long_key);
        assert!(route("GET", &path, "").is_err());
    }

    #[test]
    fn delete_objects_post_with_delete_query() {
        assert_eq!(
            route("POST", "/mybucket", "delete").unwrap(),
            S3Operation::DeleteObjects {
                bucket: "mybucket".to_string()
            }
        );
    }

    #[test]
    fn delete_objects_post_with_delete_query_and_other_params() {
        assert_eq!(
            route("POST", "/mybucket", "delete&foo=bar").unwrap(),
            S3Operation::DeleteObjects {
                bucket: "mybucket".to_string()
            }
        );
    }

    #[test]
    fn post_object() {
        assert_eq!(
            route("POST", "/mybucket", "").unwrap(),
            S3Operation::PostObject {
                bucket: "mybucket".to_string()
            }
        );
        assert_eq!(
            route("POST", "/mybucket", "foo=bar").unwrap(),
            S3Operation::PostObject {
                bucket: "mybucket".to_string()
            }
        );
    }

    #[test]
    fn list_object_versions() {
        assert_eq!(
            route("GET", "/mybucket", "versions").unwrap(),
            S3Operation::ListObjectVersions {
                bucket: "mybucket".to_string()
            }
        );
    }

    #[test]
    fn list_object_versions_with_params() {
        assert_eq!(
            route("GET", "/mybucket", "versions&prefix=foo&max-keys=10").unwrap(),
            S3Operation::ListObjectVersions {
                bucket: "mybucket".to_string()
            }
        );
    }

    #[test]
    fn put_bucket_versioning() {
        assert_eq!(
            route("PUT", "/mybucket", "versioning").unwrap(),
            S3Operation::PutBucketVersioning {
                bucket: "mybucket".to_string()
            }
        );
    }

    #[test]
    fn get_bucket_versioning() {
        assert_eq!(
            route("GET", "/mybucket", "versioning").unwrap(),
            S3Operation::GetBucketVersioning {
                bucket: "mybucket".to_string()
            }
        );
    }

    #[test]
    fn put_bucket_cors() {
        assert_eq!(
            route("PUT", "/mybucket", "cors").unwrap(),
            S3Operation::PutBucketCors {
                bucket: "mybucket".to_string()
            }
        );
    }

    #[test]
    fn get_bucket_cors() {
        assert_eq!(
            route("GET", "/mybucket", "cors").unwrap(),
            S3Operation::GetBucketCors {
                bucket: "mybucket".to_string()
            }
        );
    }

    #[test]
    fn delete_bucket_cors() {
        assert_eq!(
            route("DELETE", "/mybucket", "cors").unwrap(),
            S3Operation::DeleteBucketCors {
                bucket: "mybucket".to_string()
            }
        );
    }

    #[test]
    fn options_bucket() {
        assert_eq!(
            route("OPTIONS", "/mybucket", "").unwrap(),
            S3Operation::OptionsRequest {
                bucket: "mybucket".to_string(),
                key: None,
            }
        );
    }

    #[test]
    fn options_bucket_with_key() {
        assert_eq!(
            route("OPTIONS", "/mybucket/path/to/key", "").unwrap(),
            S3Operation::OptionsRequest {
                bucket: "mybucket".to_string(),
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
                bucket: "mybucket".to_string()
            }
        );
    }

    #[test]
    fn put_bucket_public_access_block() {
        assert_eq!(
            route("PUT", "/mybucket", "publicAccessBlock").unwrap(),
            S3Operation::PutBucketPublicAccessBlock {
                bucket: "mybucket".to_string()
            }
        );
    }

    #[test]
    fn get_bucket_public_access_block() {
        assert_eq!(
            route("GET", "/mybucket", "publicAccessBlock").unwrap(),
            S3Operation::GetBucketPublicAccessBlock {
                bucket: "mybucket".to_string()
            }
        );
    }

    #[test]
    fn delete_bucket_public_access_block() {
        assert_eq!(
            route("DELETE", "/mybucket", "publicAccessBlock").unwrap(),
            S3Operation::DeleteBucketPublicAccessBlock {
                bucket: "mybucket".to_string()
            }
        );
    }

    #[test]
    fn put_bucket_acl() {
        assert_eq!(
            route("PUT", "/mybucket", "acl").unwrap(),
            S3Operation::PutBucketAcl {
                bucket: "mybucket".to_string()
            }
        );
    }

    #[test]
    fn put_bucket_ownership_controls() {
        assert_eq!(
            route("PUT", "/mybucket", "ownershipControls").unwrap(),
            S3Operation::PutBucketOwnershipControls {
                bucket: "mybucket".to_string()
            }
        );
    }

    #[test]
    fn get_bucket_ownership_controls() {
        assert_eq!(
            route("GET", "/mybucket", "ownershipControls").unwrap(),
            S3Operation::GetBucketOwnershipControls {
                bucket: "mybucket".to_string()
            }
        );
    }

    #[test]
    fn delete_bucket_ownership_controls() {
        assert_eq!(
            route("DELETE", "/mybucket", "ownershipControls").unwrap(),
            S3Operation::DeleteBucketOwnershipControls {
                bucket: "mybucket".to_string()
            }
        );
    }

    #[test]
    fn get_object_attributes() {
        assert_eq!(
            route("GET", "/mybucket/mykey", "attributes").unwrap(),
            S3Operation::GetObjectAttributes {
                bucket: "mybucket".to_string(),
                key: "mykey".to_string()
            }
        );
    }

    #[test]
    fn get_object_attributes_with_version() {
        assert_eq!(
            route("GET", "/mybucket/mykey", "attributes&versionId=123").unwrap(),
            S3Operation::GetObjectAttributes {
                bucket: "mybucket".to_string(),
                key: "mykey".to_string()
            }
        );
    }

    #[test]
    fn get_object_without_attributes_is_get_object() {
        assert_eq!(
            route("GET", "/mybucket/mykey", "").unwrap(),
            S3Operation::GetObject {
                bucket: "mybucket".to_string(),
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
                bucket: "mybucket".to_string(),
                key: "mykey".to_string()
            }
        );
    }

    #[test]
    fn create_multipart_upload_nested_key() {
        assert_eq!(
            route("POST", "/mybucket/a/b/c.txt", "uploads").unwrap(),
            S3Operation::CreateMultipartUpload {
                bucket: "mybucket".to_string(),
                key: "a/b/c.txt".to_string()
            }
        );
    }

    #[test]
    fn upload_part() {
        assert_eq!(
            route("PUT", "/mybucket/mykey", "partNumber=1&uploadId=abc").unwrap(),
            S3Operation::UploadPart {
                bucket: "mybucket".to_string(),
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
                bucket: "mybucket".to_string(),
                key: "mykey".to_string()
            }
        );
    }

    #[test]
    fn complete_multipart_upload() {
        assert_eq!(
            route("POST", "/mybucket/mykey", "uploadId=abc123").unwrap(),
            S3Operation::CompleteMultipartUpload {
                bucket: "mybucket".to_string(),
                key: "mykey".to_string()
            }
        );
    }

    #[test]
    fn abort_multipart_upload() {
        assert_eq!(
            route("DELETE", "/mybucket/mykey", "uploadId=abc123").unwrap(),
            S3Operation::AbortMultipartUpload {
                bucket: "mybucket".to_string(),
                key: "mykey".to_string()
            }
        );
    }

    #[test]
    fn list_multipart_uploads() {
        assert_eq!(
            route("GET", "/mybucket", "uploads").unwrap(),
            S3Operation::ListMultipartUploads {
                bucket: "mybucket".to_string()
            }
        );
    }

    #[test]
    fn list_multipart_uploads_with_params() {
        assert_eq!(
            route("GET", "/mybucket", "uploads&prefix=foo&max-uploads=10").unwrap(),
            S3Operation::ListMultipartUploads {
                bucket: "mybucket".to_string()
            }
        );
    }

    #[test]
    fn list_parts() {
        assert_eq!(
            route("GET", "/mybucket/mykey", "uploadId=abc123").unwrap(),
            S3Operation::ListParts {
                bucket: "mybucket".to_string(),
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
                bucket: "mybucket".to_string(),
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
                bucket: "mybucket".to_string(),
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
                bucket: "mybucket".to_string(),
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
                bucket: "mybucket".to_string(),
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
                bucket: "mybucket".to_string(),
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
                bucket: "mybucket".to_string()
            }
        );
    }
}
