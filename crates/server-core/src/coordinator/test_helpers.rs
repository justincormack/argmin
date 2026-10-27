// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use s3_types::AccountIdentity;

use crate::sse::SseCustomerRequest;

/// Request for an UploadPart operation (test-only convenience wrapper).
#[derive(Debug)]
pub struct UploadPartRequest<'a> {
    pub upload: MultipartObjectRequest<'a>,
    pub part_number: u32,
    pub data: &'a [u8],
    pub claimed_checksum: Option<&'a ChecksumClaim>,
    pub sse_customer: Option<&'a SseCustomerRequest>,
}

#[must_use]
pub fn requester(principal: &str) -> Requester {
    Requester::authenticated(AccountIdentity::from_principal(principal))
}

/// Selects the internal payload codec for subsequently authorized writes.
///
/// Production constructors do not expose codec selection and remain pinned to
/// the current writer. Cross-crate protocol tests use this feature-gated seam
/// to exercise a supported reader/writer profile before it becomes the
/// production default.
pub fn select_new_payload_codec(
    coord: &mut Coordinator,
    payload_codec: storage::SegmentPayloadCodec,
) {
    coord.test_select_new_payload_codec(payload_codec);
}

/// Observe the durable payload codec for the current live object through the
/// ordinary admitted metadata-read route.
///
/// This is deliberately narrower than exporting storage test hooks to an
/// HTTP benchmark harness: it reveals only the self-describing codec selected
/// by the completed write and cannot mutate storage state.
pub fn observe_current_object_payload_codec(
    coord: &Coordinator,
    bucket: &str,
    key: &str,
) -> Result<storage::SegmentPayloadCodec, ServerError> {
    let bucket = storage::BucketName::try_from(bucket.to_string()).map_err(|error| {
        ServerError::InvalidBucketName {
            reason: error.to_string(),
        }
    })?;
    let key = storage::ObjectKey::try_from(key.to_string()).map_err(|error| {
        ServerError::InvalidArgument {
            reason: error.to_string(),
        }
    })?;
    let admission = coord.admit_storage_route_for_request()?;
    let route = admission
        .active_object_read_route(
            &bucket,
            &key,
            None,
            storage::ObjectReadSnapshotMode::MetadataOnly,
        )
        .map_err(super::map_store_failure)?;
    let observation = route
        .load_object_if(|stored| {
            Ok::<_, std::convert::Infallible>(
                stored.as_live().map(|live| live.encryption.payload_codec()),
            )
        })
        .map_err(super::map_object_read_failure)?;
    let payload_codec = match observation {
        Ok(payload_codec) => payload_codec,
        Err(infallible) => match infallible {},
    };
    payload_codec.ok_or_else(|| ServerError::ObjectNotFound {
        bucket: bucket.to_string(),
        key: key.to_string(),
    })
}

/// Put an object via the streaming path (begin -> append -> finalize).
///
/// Available in-crate during `#[cfg(test)]` and cross-crate via the
/// `test-utils` Cargo feature.
pub fn put_object(
    coord: &Coordinator,
    req: &PutObjectRequest<'_>,
) -> Result<PutObjectResult, ServerError> {
    coord.put_object(req)
}

/// Upload a multipart part via the streaming path (begin -> append -> finalize).
///
/// Aborts the streaming session on any append/finalize error to avoid
/// leaking session rows and staged shard data.
pub fn upload_part(
    coord: &Coordinator,
    req: &UploadPartRequest<'_>,
) -> Result<UploadPartResult, ServerError> {
    let admission = coord.admit_storage_route_for_request()?;
    let cleanup = coord.retained_stream_upload_cleanup(
        &admission,
        req.upload.object.bucket.name_typed(),
        req.upload.object.key_typed(),
    )?;
    let upload_id = req.upload.upload_id().clone();
    let session = coord.begin_stream_part_on_admitted_route(
        &admission,
        &BeginStreamPartRequest {
            upload: MultipartObjectRequest::new(
                req.upload.object.bucket.name_typed().clone(),
                req.upload.object.key_typed().clone(),
                upload_id.clone(),
                req.upload.requester().clone(),
                req.upload.expected_bucket_owner(),
            ),
            part_number: req.part_number,
            policy_context: PutObjectPolicyContext::default()
                .with_sse_customer_algorithm(req.sse_customer.map(SseCustomerRequest::algorithm)),
            sse_customer: req.sse_customer,
        },
    )?;
    let session_id = &session.session_id;
    let result = (|| {
        for (idx, chunk) in req.data.chunks(INTERNAL_SEGMENT_SIZE).enumerate() {
            coord.append_stream_part_data_on_admitted_route(
                &admission,
                &AppendStreamPartRequest {
                    bucket: req.upload.object.bucket.name_typed().clone(),
                    key: req.upload.object.key_typed().clone(),
                    upload_id: req.upload.upload_id(),
                    session_id,
                    part_number: req.part_number,
                    segment_index: idx as u32,
                    data: chunk,
                    sse_customer: req.sse_customer,
                },
            )?;
        }
        let crc = checksum::crc64::checksum(req.data);
        let computed_checksum = {
            let algo = req
                .claimed_checksum
                .map(ChecksumClaim::algorithm)
                .or(session.checksum_algorithm);
            algo.map(|a| compute_checksum(a, req.data))
        };
        coord.finalize_stream_part_with_storage_admission(
            &admission,
            FinalizeStreamPartRequest {
                upload: MultipartObjectRequest::new(
                    req.upload.object.bucket.name_typed().clone(),
                    req.upload.object.key_typed().clone(),
                    upload_id,
                    req.upload.requester().clone(),
                    req.upload.expected_bucket_owner(),
                ),
                session_id,
                part_number: req.part_number,
                crc64: crc,
                total_size: req.data.len() as u64,
                claimed_checksum: req.claimed_checksum,
                computed_checksum,
            },
        )
    })();
    if result.is_err() {
        let _ = coord.abort_stream_upload_with_retained_cleanup(&cleanup, session_id);
    }
    result
}
