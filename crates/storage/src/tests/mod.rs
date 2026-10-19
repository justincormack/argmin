// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use crate::types::{
    BucketName, ObjectEncryption, ObjectKey, SessionId, SseS3ObjectState, UploadId, SESSION_ID_LEN,
    SSE_S3_SEGMENT_NONCE_PREFIX_LEN, SSE_S3_WRAPPED_DEK_LEN, SSE_S3_WRAP_NONCE_LEN, UPLOAD_ID_LEN,
};

mod integration_tests;
mod metadata_tests;
mod property_test_support;
mod shard_tests;

pub(super) fn bucket_name(name: impl Into<String>) -> BucketName {
    BucketName::try_from(name.into()).expect("storage tests must use valid bucket names")
}

pub(super) fn object_key(key: impl Into<String>) -> ObjectKey {
    ObjectKey::try_from(key.into()).expect("storage tests must use valid object keys")
}

/// Canonical encrypted state for metadata-only fixtures.
///
/// The wrapped key material is intentionally synthetic and cannot be used to read payload.
/// Payload-bearing tests must create encryption state and ciphertext through the production
/// crypto path instead.
pub(super) fn metadata_only_test_encryption() -> ObjectEncryption {
    ObjectEncryption::SseS3(SseS3ObjectState::new(
        1,
        [2; SSE_S3_WRAP_NONCE_LEN],
        [3; SSE_S3_WRAPPED_DEK_LEN],
        [4; SSE_S3_SEGMENT_NONCE_PREFIX_LEN],
    ))
}

pub(crate) fn object_tags(xml: &str) -> crate::SerializedTagSet {
    let tags = s3_types::TagSet::parse_canonical_xml(xml, s3_types::MAX_OBJECT_TAGS)
        .expect("storage tests must use valid object tags");
    crate::SerializedTagSet::from_tag_set(tags)
        .expect("storage tests must respect the object tag count limit")
}

pub(super) fn multipart_upload_id(upload_id: impl Into<String>) -> UploadId {
    let upload_id = upload_id.into();
    let mut encoded = String::with_capacity(UPLOAD_ID_LEN);
    for byte in upload_id.bytes() {
        use std::fmt::Write;
        write!(encoded, "{byte:02x}").unwrap();
    }
    encoded.extend(std::iter::repeat_n(
        '.',
        UPLOAD_ID_LEN.saturating_sub(encoded.len()),
    ));
    UploadId::try_from(encoded).expect("storage tests must use valid upload IDs")
}

pub(super) fn stream_session_id(session_id: impl Into<String>) -> SessionId {
    let session_id = session_id.into();
    let mut encoded = String::with_capacity(SESSION_ID_LEN);
    for byte in session_id.bytes() {
        use std::fmt::Write;
        write!(encoded, "{byte:02x}").unwrap();
    }
    encoded.extend(std::iter::repeat_n(
        '0',
        SESSION_ID_LEN.saturating_sub(encoded.len()),
    ));
    SessionId::try_from(encoded).expect("storage tests must use valid session IDs")
}
