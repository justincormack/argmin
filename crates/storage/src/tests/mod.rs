use crate::types::{BucketName, ObjectKey, UploadId, UPLOAD_ID_LEN};

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
