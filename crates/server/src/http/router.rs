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

    if bucket.is_empty() {
        return Err(ServerError::InvalidRequest {
            reason: "empty bucket name".to_string(),
        });
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
}
