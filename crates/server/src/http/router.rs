/// S3 operation routing from HTTP method + path + query.
use crate::error::ServerError;

/// Recognized S3 operations.
#[derive(Debug, PartialEq, Eq)]
pub enum S3Operation {
    ListBuckets,
    CreateBucket { bucket: String },
    DeleteBucket { bucket: String },
    HeadBucket { bucket: String },
    ListObjectsV2 { bucket: String },
    PutObject { bucket: String, key: String },
    GetObject { bucket: String, key: String },
    DeleteObject { bucket: String, key: String },
    HeadObject { bucket: String, key: String },
}

/// Validate an S3 bucket name per AWS rules.
/// 3-63 characters, lowercase letters/digits/hyphens, no leading/trailing hyphen,
/// no consecutive periods, not formatted as an IP address.
fn validate_bucket_name(name: &str) -> Result<(), ServerError> {
    if name.len() < 3 || name.len() > 63 {
        return Err(ServerError::InvalidRequest {
            reason: format!("bucket name must be 3-63 characters, got {}", name.len()),
        });
    }
    if name.starts_with('-') || name.ends_with('-') {
        return Err(ServerError::InvalidRequest {
            reason: "bucket name must not start or end with a hyphen".to_string(),
        });
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.')
    {
        return Err(ServerError::InvalidRequest {
            reason: "bucket name must contain only lowercase letters, digits, hyphens, and periods"
                .to_string(),
        });
    }
    if name.contains("..") {
        return Err(ServerError::InvalidRequest {
            reason: "bucket name must not contain consecutive periods".to_string(),
        });
    }
    // Reject IP-address-formatted names (4 groups of digits separated by periods)
    let parts: Vec<&str> = name.split('.').collect();
    if parts.len() == 4 && parts.iter().all(|p| p.parse::<u8>().is_ok()) {
        return Err(ServerError::InvalidRequest {
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
    Ok(())
}

/// Route an HTTP request to an S3 operation.
///
/// Path-style addressing only: `/<bucket>` or `/<bucket>/<key...>`.
pub fn route(method: &str, path: &str, _query: &str) -> Result<S3Operation, ServerError> {
    // Normalize path: remove trailing slash (except for root)
    let path = if path.len() > 1 && path.ends_with('/') {
        &path[..path.len() - 1]
    } else {
        path
    };

    // Split path into segments
    let trimmed = path.strip_prefix('/').unwrap_or(path);

    if trimmed.is_empty() {
        // Root path: GET / = ListBuckets
        return match method {
            "GET" => Ok(S3Operation::ListBuckets),
            _ => Err(ServerError::MethodNotAllowed),
        };
    }

    // Split into bucket and optional key
    let (bucket, key) = match trimmed.find('/') {
        Some(pos) => {
            let bucket = &trimmed[..pos];
            let key = &trimmed[pos + 1..];
            (bucket, if key.is_empty() { None } else { Some(key) })
        }
        None => (trimmed, None),
    };

    validate_bucket_name(bucket)?;
    if let Some(k) = key {
        validate_object_key(k)?;
    }

    match (method, key) {
        // Bucket-level operations (no key)
        ("PUT", None) => Ok(S3Operation::CreateBucket {
            bucket: bucket.to_string(),
        }),
        ("DELETE", None) => Ok(S3Operation::DeleteBucket {
            bucket: bucket.to_string(),
        }),
        ("HEAD", None) => Ok(S3Operation::HeadBucket {
            bucket: bucket.to_string(),
        }),
        ("GET", None) => {
            // GET /<bucket> = ListObjectsV2 (check for list-type=2 param)
            // For simplicity, any GET on a bucket is ListObjectsV2
            Ok(S3Operation::ListObjectsV2 {
                bucket: bucket.to_string(),
            })
        }

        // Object-level operations
        ("PUT", Some(key)) => Ok(S3Operation::PutObject {
            bucket: bucket.to_string(),
            key: key.to_string(),
        }),
        ("GET", Some(key)) => Ok(S3Operation::GetObject {
            bucket: bucket.to_string(),
            key: key.to_string(),
        }),
        ("DELETE", Some(key)) => Ok(S3Operation::DeleteObject {
            bucket: bucket.to_string(),
            key: key.to_string(),
        }),
        ("HEAD", Some(key)) => Ok(S3Operation::HeadObject {
            bucket: bucket.to_string(),
            key: key.to_string(),
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
    fn list_objects() {
        assert_eq!(
            route("GET", "/mybucket", "list-type=2").unwrap(),
            S3Operation::ListObjectsV2 {
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
        // Uppercase
        assert!(route("HEAD", "/MyBucket", "").is_err());
        // Consecutive periods
        assert!(route("HEAD", "/my..bucket", "").is_err());
        // IP address format
        assert!(route("HEAD", "/192.168.1.1", "").is_err());
    }

    #[test]
    fn valid_object_keys() {
        assert!(route("GET", "/bucket/a", "").is_ok());
        assert!(route("GET", "/bucket/path/to/file.txt", "").is_ok());
        assert!(route("GET", "/bucket/key with spaces", "").is_ok());
    }

    #[test]
    fn object_key_too_long() {
        let long_key = "k".repeat(1025);
        let path = format!("/bucket/{}", long_key);
        assert!(route("GET", &path, "").is_err());
    }
}
