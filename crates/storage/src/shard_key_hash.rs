// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use crate::types::{GenerationId, SessionId, UploadId};

fn sha256_truncated_16(input: &[u8]) -> [u8; 16] {
    let hash = checksum::sha256::digest(input);
    let mut result = [0u8; 16];
    result.copy_from_slice(&hash[..16]);
    result
}

/// Compute the 16-byte object key hash used in simple shard keys.
///
/// `object_key_hash = SHA-256(bucket + "/" + key)[:16]`
pub fn object_key_hash(bucket: &str, key: &str) -> [u8; 16] {
    sha256_truncated_16(format!("{bucket}/{key}").as_bytes())
}

/// Compute the 16-byte segment key hash for streaming upload shard keys.
///
/// `segment_okh = SHA-256("segment/" + session_id + "/" + segment_index)[:16]`
pub(crate) fn stream_segment_key_hash(session_id: &SessionId, segment_index: u32) -> [u8; 16] {
    sha256_truncated_16(format!("segment/{}/{segment_index}", session_id.as_str()).as_bytes())
}

/// Compute the 16-byte segment key hash for direct PUT staging shard keys.
///
/// `segment_okh = SHA-256("direct-put-segment/" + session_id + "/" + segment_index)[:16]`
pub(crate) fn direct_put_segment_key_hash(session_id: &SessionId, segment_index: u32) -> [u8; 16] {
    sha256_truncated_16(
        format!("direct-put-segment/{}/{segment_index}", session_id.as_str()).as_bytes(),
    )
}

/// Compute the 16-byte segment key hash for committed object segment shard keys.
///
/// `segment_okh = SHA-256("segment/" + bucket + "/" + key + "/" + generation_id + "/" +
/// segment_index)[:16]`
pub fn segment_key_hash(
    bucket: &str,
    key: &str,
    generation_id: GenerationId,
    segment_index: u32,
) -> [u8; 16] {
    sha256_truncated_16(
        format!(
            "segment/{}/{}/{}/{segment_index}",
            bucket,
            key,
            generation_id.get()
        )
        .as_bytes(),
    )
}

/// Compute the 16-byte segment key hash for multipart part segment shard keys.
///
/// `segment_okh = SHA-256("mpu-segment/" + upload_id + "/" + part_number + "/" + generation +
/// "/" + segment_index)[:16]`
pub fn multipart_part_segment_key_hash(
    upload_id: &UploadId,
    part_number: u32,
    generation: u32,
    segment_index: u32,
) -> [u8; 16] {
    sha256_truncated_16(
        format!("mpu-segment/{upload_id}/{part_number}/{generation}/{segment_index}").as_bytes(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SessionId, UploadId};

    fn upload_id() -> UploadId {
        UploadId::try_from(".".repeat(128)).expect("test upload id is valid")
    }

    fn session_id(value: &str) -> SessionId {
        SessionId::try_from(value).expect("test session id is valid")
    }

    #[test]
    fn stream_segment_key_hash_deterministic() {
        let session = session_id("0123456789abcdef0123456789abcdef");
        let a = stream_segment_key_hash(&session, 0);
        let b = stream_segment_key_hash(&session, 0);
        assert_eq!(a, b);
    }

    #[test]
    fn stream_segment_key_hash_different_indices() {
        let session = session_id("0123456789abcdef0123456789abcdef");
        let a = stream_segment_key_hash(&session, 0);
        let b = stream_segment_key_hash(&session, 1);
        assert_ne!(a, b);
    }

    #[test]
    fn stream_segment_key_hash_different_sessions() {
        let a = stream_segment_key_hash(&session_id("0123456789abcdef0123456789abcdef"), 0);
        let b = stream_segment_key_hash(&session_id("fedcba9876543210fedcba9876543210"), 0);
        assert_ne!(a, b);
    }

    #[test]
    fn stream_segment_key_hash_length() {
        let hash = stream_segment_key_hash(&session_id("0123456789abcdef0123456789abcdef"), 42);
        assert_eq!(hash.len(), 16);
    }

    #[test]
    fn direct_put_segment_key_hash_deterministic() {
        let session = session_id("0123456789abcdef0123456789abcdef");
        let a = direct_put_segment_key_hash(&session, 0);
        let b = direct_put_segment_key_hash(&session, 0);
        assert_eq!(a, b);
    }

    #[test]
    fn direct_put_segment_key_hash_different_sessions() {
        let a = direct_put_segment_key_hash(&session_id("0123456789abcdef0123456789abcdef"), 0);
        let b = direct_put_segment_key_hash(&session_id("fedcba9876543210fedcba9876543210"), 0);
        assert_ne!(a, b);
    }

    #[test]
    fn direct_put_segment_key_hash_is_separate_from_stream_hash() {
        let session = session_id("0123456789abcdef0123456789abcdef");
        assert_ne!(
            direct_put_segment_key_hash(&session, 0),
            stream_segment_key_hash(&session, 0)
        );
    }

    #[test]
    fn segment_key_hash_deterministic() {
        let generation_id = GenerationId::new(7).unwrap();
        let a = segment_key_hash("bucket", "key", generation_id, 0);
        let b = segment_key_hash("bucket", "key", generation_id, 0);
        assert_eq!(a, b);
    }

    #[test]
    fn segment_key_hash_different_generations() {
        let a = segment_key_hash("bucket", "key", GenerationId::new(7).unwrap(), 0);
        let b = segment_key_hash("bucket", "key", GenerationId::new(8).unwrap(), 0);
        assert_ne!(a, b);
    }

    #[test]
    fn segment_key_hash_different_indices() {
        let generation_id = GenerationId::new(7).unwrap();
        let a = segment_key_hash("bucket", "key", generation_id, 0);
        let b = segment_key_hash("bucket", "key", generation_id, 1);
        assert_ne!(a, b);
    }

    #[test]
    fn segment_key_hash_length() {
        let generation_id = GenerationId::new(7).unwrap();
        let hash = segment_key_hash("bucket", "key", generation_id, 42);
        assert_eq!(hash.len(), 16);
    }

    #[test]
    fn object_key_hash_deterministic() {
        let a = object_key_hash("bucket", "key");
        let b = object_key_hash("bucket", "key");
        assert_eq!(a, b);
    }

    #[test]
    fn object_key_hash_different_keys() {
        let a = object_key_hash("bucket", "key1");
        let b = object_key_hash("bucket", "key2");
        assert_ne!(a, b);
    }

    #[test]
    fn object_key_hash_length() {
        let hash = object_key_hash("bucket", "key");
        assert_eq!(hash.len(), 16);
    }

    #[test]
    fn multipart_part_segment_key_hash_deterministic() {
        let upload_id = upload_id();
        let a = multipart_part_segment_key_hash(&upload_id, 1, 0, 0);
        let b = multipart_part_segment_key_hash(&upload_id, 1, 0, 0);
        assert_eq!(a, b);
    }

    #[test]
    fn multipart_part_segment_key_hash_different_segments() {
        let upload_id = upload_id();
        let a = multipart_part_segment_key_hash(&upload_id, 1, 0, 0);
        let b = multipart_part_segment_key_hash(&upload_id, 1, 0, 1);
        assert_ne!(a, b);
    }

    #[test]
    fn multipart_part_segment_key_hash_different_generations() {
        let upload_id = upload_id();
        let a = multipart_part_segment_key_hash(&upload_id, 1, 0, 0);
        let b = multipart_part_segment_key_hash(&upload_id, 1, 1, 0);
        assert_ne!(a, b);
    }

    #[test]
    fn multipart_part_segment_key_hash_length() {
        let hash = multipart_part_segment_key_hash(&upload_id(), 1, 0, 0);
        assert_eq!(hash.len(), 16);
    }
}
