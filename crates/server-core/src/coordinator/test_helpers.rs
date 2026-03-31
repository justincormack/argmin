use super::*;

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
    let session = coord.begin_stream_part(&BeginStreamPartRequest {
        upload: MultipartObjectRequest::new(
            req.upload.bucket_name(),
            req.upload.key(),
            req.upload.upload_id(),
            req.upload.requester().clone(),
            req.upload.expected_bucket_owner(),
        ),
        part_number: req.part_number,
        policy_context: PutObjectPolicyContext::default()
            .with_sse_customer_algorithm(req.sse_customer.map(SseCustomerRequest::algorithm)),
        sse_customer: req.sse_customer,
    })?;
    let session_id = &session.session_id;
    let result = (|| {
        for (idx, chunk) in req.data.chunks(INTERNAL_SEGMENT_SIZE).enumerate() {
            let data = if let Some(sse_customer) = session.sse_customer.as_ref() {
                sse_customer.encrypt_segment(idx as u32, chunk)?
            } else {
                chunk.to_vec()
            };
            coord.append_stream_segment(
                req.upload.bucket_name(),
                req.upload.key(),
                session_id,
                idx as u32,
                &data,
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
        coord.finalize_stream_part(FinalizeStreamPartRequest {
            upload: MultipartObjectRequest::new(
                req.upload.bucket_name(),
                req.upload.key(),
                req.upload.upload_id(),
                req.upload.requester().clone(),
                req.upload.expected_bucket_owner(),
            ),
            session_id,
            part_number: req.part_number,
            crc64: crc,
            total_size: req.data.len() as u64,
            claimed_checksum: req.claimed_checksum,
            computed_checksum,
        })
    })();
    if result.is_err() {
        let _ = coord.abort_stream_put(req.upload.bucket_name(), req.upload.key(), session_id);
    }
    result
}
