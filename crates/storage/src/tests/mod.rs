// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use crate::types::{
    BucketName, ObjectEncryption, ObjectKey, SessionId, SseS3ObjectState, UploadId, SESSION_ID_LEN,
    SSE_C_SEGMENT_NONCE_SCOPE_LEN, SSE_S3_SEGMENT_NONCE_PREFIX_LEN, SSE_S3_WRAPPED_DEK_LEN,
    SSE_S3_WRAP_NONCE_LEN, UPLOAD_ID_LEN,
};
use argmin_crypto::aead::{Aes256GcmKey, AES_GCM_NONCE_LEN};

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

pub(super) struct SseS3SegmentTestFixture {
    pub encryption: ObjectEncryption,
    pub storage_bytes: Vec<u8>,
    pub logical_size: u64,
    pub storage_crc64: u64,
    pub payload_crc64: u64,
}

/// Build a production-compatible whole-segment SSE-S3 fixture.
///
/// Unlike `metadata_only_test_encryption`, the wrapped DEK and ciphertext are coherent. The
/// fixture uses the current object-scope nonce construction and persistent AES-GCM associated
/// data, so payload-bearing storage tests exercise the actual encrypted representation.
pub(super) fn sse_s3_segment_test_fixture(
    plaintext: &[u8],
    segment_index: u32,
) -> SseS3SegmentTestFixture {
    const MANAGED_WRAP_AAD: &[u8] = b"argmin:sse-s3:wrap:v1";
    const MANAGED_SEGMENT_AAD: &[u8] = b"argmin:sse-s3:segment:v1";

    let wrapping_key = [0x11; 32];
    let dek = [0x22; 32];
    let wrap_nonce = [0x33; SSE_S3_WRAP_NONCE_LEN];
    let segment_nonce_prefix = [0x44; SSE_S3_SEGMENT_NONCE_PREFIX_LEN];

    let mut wrapped_dek = dek.to_vec();
    Aes256GcmKey::new(&wrapping_key)
        .expect("test wrapping key must be valid")
        .seal_in_place_append_tag(wrap_nonce, MANAGED_WRAP_AAD, &mut wrapped_dek)
        .expect("test DEK wrapping must succeed");
    let wrapped_dek: [u8; SSE_S3_WRAPPED_DEK_LEN] = wrapped_dek
        .try_into()
        .expect("AES-GCM wrapped DEK must have the durable length");

    let mut segment_nonce = [0; AES_GCM_NONCE_LEN];
    segment_nonce[..SSE_S3_SEGMENT_NONCE_PREFIX_LEN].copy_from_slice(&segment_nonce_prefix);
    let scope_end = SSE_S3_SEGMENT_NONCE_PREFIX_LEN + SSE_C_SEGMENT_NONCE_SCOPE_LEN;
    segment_nonce[SSE_S3_SEGMENT_NONCE_PREFIX_LEN..scope_end].copy_from_slice(&0u16.to_be_bytes());
    segment_nonce[scope_end..].copy_from_slice(&segment_index.to_be_bytes());

    let mut storage_bytes = plaintext.to_vec();
    Aes256GcmKey::new(&dek)
        .expect("test DEK must be valid")
        .seal_in_place_append_tag(segment_nonce, MANAGED_SEGMENT_AAD, &mut storage_bytes)
        .expect("test segment encryption must succeed");

    SseS3SegmentTestFixture {
        encryption: ObjectEncryption::SseS3(SseS3ObjectState::new(
            7,
            wrap_nonce,
            wrapped_dek,
            segment_nonce_prefix,
        )),
        logical_size: plaintext.len() as u64,
        storage_crc64: checksum::crc64::checksum(&storage_bytes),
        payload_crc64: checksum::crc64::checksum(plaintext),
        storage_bytes,
    }
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
