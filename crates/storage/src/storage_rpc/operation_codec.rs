// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

pub(crate) fn encode_metadata_command_item(
    item: &StorageRpcMetadataCommandItem,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if item.command_bytes.len() > STORAGE_RPC_MAX_METADATA_COMMAND_BYTES_LEN {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: item.command_bytes.len(),
            limit: STORAGE_RPC_MAX_METADATA_COMMAND_BYTES_LEN,
        });
    }
    if checksum::crc64::checksum(&item.command_bytes) != item.command_checksum {
        return Err(StorageRpcPayloadError::MetadataCommandChecksumMismatch);
    }
    validate_metadata_command_envelope_bytes(&item.command_bytes)
        .map_err(|_| StorageRpcPayloadError::InvalidMetadataCommandEnvelope)?;
    let mut out = Vec::new();
    put_u64(&mut out, item.command_checksum);
    put_bytes(&mut out, &item.command_bytes);
    Ok(out)
}

pub(crate) fn decode_metadata_command_item(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandItem, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let item = decoder.read_metadata_command_item()?;
    decoder.finish()?;
    Ok(item)
}

fn validate_metadata_command_item(
    command_checksum: u64,
    command_bytes: Vec<u8>,
) -> Result<StorageRpcMetadataCommandItem, StorageRpcPayloadError> {
    if checksum::crc64::checksum(&command_bytes) != command_checksum {
        return Err(StorageRpcPayloadError::MetadataCommandChecksumMismatch);
    }
    validate_metadata_command_envelope_bytes(&command_bytes)
        .map_err(|_| StorageRpcPayloadError::InvalidMetadataCommandEnvelope)?;
    Ok(StorageRpcMetadataCommandItem {
        command_checksum,
        command_bytes,
    })
}

fn metadata_command_envelope_from_item(
    item: &StorageRpcMetadataCommandItem,
    authority: &MetadataCommandDecodeAuthority,
) -> Result<crate::metadata_command::MetadataCommandEnvelope, StorageRpcPayloadError> {
    decode_metadata_command_envelope(&item.command_bytes, authority)
        .map_err(|_| StorageRpcPayloadError::InvalidMetadataCommandEnvelope)
}

pub(crate) fn encode_metadata_command_request(
    request: &StorageRpcMetadataCommandRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_metadata_command_route(request.cluster_epoch, request.pg_id, request.command.id())?;
    let item = StorageRpcMetadataCommandItem {
        command_checksum: request.command.checksum_crc64(),
        command_bytes: request.command.command_bytes(),
    };
    let command_payload = encode_metadata_command_item(&item)?;
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.cluster_epoch.get());
    put_u32(&mut out, request.pg_id.get());
    out.extend_from_slice(&command_payload);
    Ok(out)
}

pub(crate) fn decode_metadata_command_request(
    bytes: &[u8],
    authority: &MetadataCommandDecodeAuthority,
) -> Result<StorageRpcMetadataCommandRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let item = decoder.read_metadata_command_item()?;
    decoder.finish()?;
    let command = metadata_command_envelope_from_item(&item, authority)?;
    validate_metadata_command_route(cluster_epoch, pg_id, command.id())?;
    Ok(StorageRpcMetadataCommandRequest {
        node_id,
        cluster_epoch,
        pg_id,
        command,
    })
}

pub(crate) fn encode_metadata_command_recovery_request(
    request: &StorageRpcMetadataCommandRecoveryRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_metadata_command_route(
        request.cluster_epoch,
        request.pg_id,
        request.authorized_source.id(),
    )?;
    if let Some(abandoned_source) = request.abandoned_source.as_ref() {
        validate_metadata_command_route(
            request.cluster_epoch,
            request.pg_id,
            abandoned_source.id(),
        )?;
    }
    validate_metadata_command_route(request.cluster_epoch, request.pg_id, request.command.id())?;
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.cluster_epoch.get());
    put_u32(&mut out, request.pg_id.get());
    let source = StorageRpcMetadataCommandItem {
        command_checksum: request.authorized_source.checksum_crc64(),
        command_bytes: request.authorized_source.command_bytes(),
    };
    out.extend_from_slice(&encode_metadata_command_item(&source)?);
    match request.abandoned_source.as_ref() {
        None => put_u8(&mut out, 0),
        Some(abandoned_source) => {
            put_u8(&mut out, 1);
            let item = StorageRpcMetadataCommandItem {
                command_checksum: abandoned_source.checksum_crc64(),
                command_bytes: abandoned_source.command_bytes(),
            };
            out.extend_from_slice(&encode_metadata_command_item(&item)?);
        }
    }
    let command = StorageRpcMetadataCommandItem {
        command_checksum: request.command.checksum_crc64(),
        command_bytes: request.command.command_bytes(),
    };
    out.extend_from_slice(&encode_metadata_command_item(&command)?);
    Ok(out)
}

pub(crate) fn decode_metadata_command_recovery_request(
    bytes: &[u8],
    authority: &MetadataCommandDecodeAuthority,
) -> Result<StorageRpcMetadataCommandRecoveryRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let authorized_source =
        metadata_command_envelope_from_item(&decoder.read_metadata_command_item()?, authority)?;
    let abandoned_source = match decoder.read_u8()? {
        0 => None,
        1 => Some(metadata_command_envelope_from_item(
            &decoder.read_metadata_command_item()?,
            authority,
        )?),
        _ => {
            return Err(StorageRpcPayloadError::InvalidMetadataCommandEnvelope);
        }
    };
    let command =
        metadata_command_envelope_from_item(&decoder.read_metadata_command_item()?, authority)?;
    decoder.finish()?;
    validate_metadata_command_route(cluster_epoch, pg_id, authorized_source.id())?;
    if let Some(abandoned_source) = abandoned_source.as_ref() {
        validate_metadata_command_route(cluster_epoch, pg_id, abandoned_source.id())?;
    }
    validate_metadata_command_route(cluster_epoch, pg_id, command.id())?;
    Ok(StorageRpcMetadataCommandRecoveryRequest {
        node_id,
        cluster_epoch,
        pg_id,
        authorized_source,
        abandoned_source,
        command,
    })
}

pub(crate) fn encode_bucket_request(request: &StorageRpcBucketRequest) -> Vec<u8> {
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.cluster_epoch.get());
    put_u32(&mut out, request.pg_id.get());
    put_string(&mut out, request.bucket.as_str());
    out
}

pub(crate) fn decode_bucket_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let bucket = decoder.read_bucket_name()?;
    decoder.finish()?;
    Ok(StorageRpcBucketRequest {
        node_id,
        cluster_epoch,
        pg_id,
        bucket,
    })
}

pub(crate) fn encode_stream_uploads_list_request(
    request: &StorageRpcStreamUploadsListRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_cleanup_list_limit(request.limit)?;
    let mut out = encode_bucket_request(&request.bucket);
    put_optional_string(
        &mut out,
        request.session_id_marker.as_ref().map(SessionId::as_str),
    );
    put_u32(&mut out, request.limit);
    Ok(out)
}

pub(crate) fn decode_stream_uploads_list_request(
    bytes: &[u8],
) -> Result<StorageRpcStreamUploadsListRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let bucket = decoder.read_bucket_request()?;
    let session_id_marker = decoder.read_optional_session_id()?;
    let limit = decoder.read_u32()?;
    validate_cleanup_list_limit(limit)?;
    decoder.finish()?;
    Ok(StorageRpcStreamUploadsListRequest {
        bucket,
        session_id_marker,
        limit,
    })
}

pub(crate) fn encode_stream_uploads_pg_list_request(
    request: &StorageRpcStreamUploadsPgListRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_cleanup_list_limit(request.limit)?;
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.cluster_epoch.get());
    put_u32(&mut out, request.pg_id.get());
    put_optional_string(
        &mut out,
        request.session_id_marker.as_ref().map(SessionId::as_str),
    );
    put_u32(&mut out, request.limit);
    Ok(out)
}

pub(crate) fn decode_stream_uploads_pg_list_request(
    bytes: &[u8],
) -> Result<StorageRpcStreamUploadsPgListRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let session_id_marker = decoder.read_optional_session_id()?;
    let limit = decoder.read_u32()?;
    validate_cleanup_list_limit(limit)?;
    decoder.finish()?;
    Ok(StorageRpcStreamUploadsPgListRequest {
        node_id,
        cluster_epoch,
        pg_id,
        session_id_marker,
        limit,
    })
}

pub(crate) fn encode_bucket_list_request(
    request: &StorageRpcBucketListRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.owner_canonical_id.len() > STORAGE_RPC_MAX_BUCKET_OWNER_CANONICAL_ID_LEN {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: request.owner_canonical_id.len(),
            limit: STORAGE_RPC_MAX_BUCKET_OWNER_CANONICAL_ID_LEN,
        });
    }
    let mut out = encode_bucket_pg_request(&StorageRpcBucketPgRequest {
        node_id: request.node_id,
        cluster_epoch: request.cluster_epoch,
        pg_id: request.pg_id,
    })?;
    put_string(&mut out, &request.owner_canonical_id);
    Ok(out)
}

pub(crate) fn decode_bucket_list_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketListRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let owner_canonical_id = decoder.read_string_with_limit(
        STORAGE_RPC_MAX_BUCKET_OWNER_CANONICAL_ID_LEN,
        StorageRpcPayloadError::PayloadTooLarge {
            len: STORAGE_RPC_MAX_BUCKET_OWNER_CANONICAL_ID_LEN + 1,
            limit: STORAGE_RPC_MAX_BUCKET_OWNER_CANONICAL_ID_LEN,
        },
    )?;
    decoder.finish()?;
    Ok(StorageRpcBucketListRequest {
        node_id: route.node_id,
        cluster_epoch: route.cluster_epoch,
        pg_id: route.pg_id,
        owner_canonical_id,
    })
}

pub(crate) fn encode_bucket_list_response(
    response: &StorageRpcBucketListResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_bucket_metadata_item_count(response.buckets.len())?;
    let mut out = Vec::new();
    put_u32(
        &mut out,
        u32::try_from(response.buckets.len()).map_err(|_| {
            StorageRpcPayloadError::PayloadTooLarge {
                len: response.buckets.len(),
                limit: STORAGE_RPC_MAX_BUCKET_BATCH_ITEMS as usize,
            }
        })?,
    );
    for bucket in &response.buckets {
        put_bucket_info(&mut out, bucket);
    }
    Ok(out)
}

pub(crate) fn decode_bucket_list_response(
    bytes: &[u8],
) -> Result<StorageRpcBucketListResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let count = decoder.read_limited_bounded_remaining_count(
        1,
        "bucket list count exceeds payload",
        STORAGE_RPC_MAX_BUCKET_BATCH_ITEMS,
    )?;
    let mut buckets = Vec::new();
    for _ in 0..count {
        buckets.push(decoder.read_bucket_info()?);
    }
    decoder.finish()?;
    Ok(StorageRpcBucketListResponse { buckets })
}

pub(crate) fn encode_bucket_batch_request(
    request: &StorageRpcBucketBatchRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_bucket_metadata_item_count(request.buckets.len())?;
    let mut out = encode_bucket_pg_request(&StorageRpcBucketPgRequest {
        node_id: request.node_id,
        cluster_epoch: request.cluster_epoch,
        pg_id: request.pg_id,
    })?;
    put_u32(
        &mut out,
        u32::try_from(request.buckets.len()).map_err(|_| {
            StorageRpcPayloadError::PayloadTooLarge {
                len: request.buckets.len(),
                limit: STORAGE_RPC_MAX_BUCKET_BATCH_ITEMS as usize,
            }
        })?,
    );
    for bucket in &request.buckets {
        put_string(&mut out, bucket.as_str());
    }
    Ok(out)
}

pub(crate) fn decode_bucket_batch_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketBatchRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let count = decoder.read_limited_bounded_remaining_count(
        1,
        "bucket batch count exceeds payload",
        STORAGE_RPC_MAX_BUCKET_BATCH_ITEMS,
    )?;
    let mut buckets = Vec::new();
    for _ in 0..count {
        buckets.push(decoder.read_bucket_name()?);
    }
    decoder.finish()?;
    Ok(StorageRpcBucketBatchRequest {
        node_id: route.node_id,
        cluster_epoch: route.cluster_epoch,
        pg_id: route.pg_id,
        buckets,
    })
}

pub(crate) fn encode_bucket_execution_generations_response(
    response: &StorageRpcBucketExecutionGenerationsResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_bucket_metadata_item_count(response.generations.len())?;
    let mut out = Vec::new();
    put_u32(
        &mut out,
        u32::try_from(response.generations.len()).map_err(|_| {
            StorageRpcPayloadError::PayloadTooLarge {
                len: response.generations.len(),
                limit: STORAGE_RPC_MAX_BUCKET_BATCH_ITEMS as usize,
            }
        })?,
    );
    for (bucket, generation) in &response.generations {
        put_string(&mut out, bucket.as_str());
        put_u64(&mut out, *generation);
    }
    Ok(out)
}

pub(crate) fn decode_bucket_execution_generations_response(
    bytes: &[u8],
) -> Result<StorageRpcBucketExecutionGenerationsResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let count = decoder.read_limited_bounded_remaining_count(
        1,
        "bucket execution generation count exceeds payload",
        STORAGE_RPC_MAX_BUCKET_BATCH_ITEMS,
    )?;
    let mut generations = HashMap::new();
    for _ in 0..count {
        let bucket = decoder.read_bucket_name()?;
        let generation = decoder.read_u64()?;
        if generations.insert(bucket, generation).is_some() {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "duplicate bucket execution generation",
            ));
        }
    }
    decoder.finish()?;
    Ok(StorageRpcBucketExecutionGenerationsResponse { generations })
}

pub(crate) fn encode_bucket_fast_path_identities_response(
    response: &StorageRpcBucketFastPathIdentitiesResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_bucket_metadata_item_count(response.identities.len())?;
    let mut out = Vec::new();
    put_u32(
        &mut out,
        u32::try_from(response.identities.len()).map_err(|_| {
            StorageRpcPayloadError::PayloadTooLarge {
                len: response.identities.len(),
                limit: STORAGE_RPC_MAX_BUCKET_BATCH_ITEMS as usize,
            }
        })?,
    );
    for (bucket, identity) in &response.identities {
        put_string(&mut out, bucket.as_str());
        put_bucket_fast_path_identity(&mut out, *identity);
    }
    Ok(out)
}

pub(crate) fn decode_bucket_fast_path_identities_response(
    bytes: &[u8],
) -> Result<StorageRpcBucketFastPathIdentitiesResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let count = decoder.read_limited_bounded_remaining_count(
        1,
        "bucket fast-path identity count exceeds payload",
        STORAGE_RPC_MAX_BUCKET_BATCH_ITEMS,
    )?;
    let mut identities = HashMap::new();
    for _ in 0..count {
        let bucket = decoder.read_bucket_name()?;
        let identity = decoder.read_bucket_fast_path_identity()?;
        if identities.insert(bucket, identity).is_some() {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "duplicate bucket fast-path identity",
            ));
        }
    }
    decoder.finish()?;
    Ok(StorageRpcBucketFastPathIdentitiesResponse { identities })
}

pub(crate) fn encode_bucket_snapshot_request(request: &StorageRpcBucketSnapshotRequest) -> Vec<u8> {
    let mut out = encode_bucket_request(&request.bucket);
    put_bucket_snapshot_request(&mut out, request.request);
    out
}

pub(crate) fn decode_bucket_snapshot_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketSnapshotRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let request = decoder.read_rpc_bucket_snapshot_request()?;
    decoder.finish()?;
    Ok(request)
}

pub(crate) fn encode_object_request(request: &StorageRpcObjectRequest) -> Vec<u8> {
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.cluster_epoch.get());
    put_u32(&mut out, request.pg_id.get());
    put_string(&mut out, request.bucket.as_str());
    put_string(&mut out, request.key.as_str());
    out
}

pub(crate) fn decode_object_request(
    bytes: &[u8],
) -> Result<StorageRpcObjectRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let bucket = decoder.read_bucket_name()?;
    let key = decoder.read_object_key()?;
    decoder.finish()?;
    Ok(StorageRpcObjectRequest {
        node_id,
        cluster_epoch,
        pg_id,
        bucket,
        key,
    })
}

pub(crate) fn encode_object_payload_reclaim_exists_request(
    request: &StorageRpcObjectPayloadReclaimExistsRequest,
) -> Vec<u8> {
    let mut out = encode_object_request(&request.object);
    put_u64(&mut out, request.generation_id.get());
    out
}

pub(crate) fn decode_object_payload_reclaim_exists_request(
    bytes: &[u8],
) -> Result<StorageRpcObjectPayloadReclaimExistsRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let generation_id = decoder.read_generation_id()?;
    decoder.finish()?;
    Ok(StorageRpcObjectPayloadReclaimExistsRequest {
        object,
        generation_id,
    })
}

pub(crate) fn encode_object_payload_lease_control_request(
    request: &StorageRpcObjectPayloadLeaseControlRequest,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.route_cluster_epoch.get());
    put_string(&mut out, request.bucket.as_str());
    put_string(&mut out, request.key.as_str());
    put_u64(&mut out, request.generation_id.get());
    out.push(request.operation as u8);
    match request.reclaim_authority.as_ref() {
        Some(authority) => {
            out.push(1);
            put_u64(&mut out, authority.bucket_incarnation_generation);
            out.push(authority.reclaim_kind as u8);
            put_string(&mut out, &authority.claim_id);
            put_string(&mut out, &authority.owner_token);
            put_u64(&mut out, authority.cluster_epoch.get());
        }
        None => out.push(0),
    }
    out
}

pub(crate) fn decode_object_payload_lease_control_request(
    bytes: &[u8],
) -> Result<StorageRpcObjectPayloadLeaseControlRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let route_cluster_epoch = decoder.read_cluster_epoch()?;
    let bucket = decoder.read_bucket_name()?;
    let key = decoder.read_object_key()?;
    let generation_id = decoder.read_generation_id()?;
    let operation = match decoder.read_u8()? {
        1 => StorageRpcObjectPayloadLeaseControlOperation::Acquire,
        2 => StorageRpcObjectPayloadLeaseControlOperation::Release,
        3 => StorageRpcObjectPayloadLeaseControlOperation::ReclaimBegin,
        4 => StorageRpcObjectPayloadLeaseControlOperation::ReclaimFinish,
        5 => StorageRpcObjectPayloadLeaseControlOperation::ReclaimFinishKeepFence,
        6 => StorageRpcObjectPayloadLeaseControlOperation::ReclaimFenceClear,
        7 => StorageRpcObjectPayloadLeaseControlOperation::Count,
        _ => {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "unknown object payload lease control operation",
            ));
        }
    };
    let reclaim_authority = match decoder.read_u8()? {
        0 => None,
        1 => Some(ObjectPayloadReclaimClaimProof {
            bucket_incarnation_generation: decoder.read_u64()?,
            reclaim_kind: decoder.read_object_payload_reclaim_kind()?,
            claim_id: decoder.read_string_with_limit(
                STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN,
                StorageRpcPayloadError::InvalidObjectMetadataRequest(
                    "object payload reclaim claim id is too large",
                ),
            )?,
            owner_token: decoder.read_string_with_limit(
                STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN,
                StorageRpcPayloadError::InvalidObjectMetadataRequest(
                    "object payload reclaim owner token is too large",
                ),
            )?,
            cluster_epoch: decoder.read_cluster_epoch()?,
        }),
        _ => {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid optional object payload reclaim authority tag",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcObjectPayloadLeaseControlRequest {
        node_id,
        route_cluster_epoch,
        bucket,
        key,
        generation_id,
        operation,
        reclaim_authority,
    })
}

pub(crate) fn encode_object_payload_lease_control_response(
    response: StorageRpcObjectPayloadLeaseControlResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_u64(&mut out, response.value);
    out
}

pub(crate) fn decode_object_payload_lease_control_response(
    bytes: &[u8],
) -> Result<StorageRpcObjectPayloadLeaseControlResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let value = decoder.read_u64()?;
    decoder.finish()?;
    Ok(StorageRpcObjectPayloadLeaseControlResponse { value })
}

pub(crate) fn encode_object_payload_reclaim_response(
    response: &StorageRpcObjectPayloadReclaimResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_optional_object_payload_reclaim(&mut out, response.reclaim.as_ref());
    out
}

pub(crate) fn decode_object_payload_reclaim_response(
    bytes: &[u8],
) -> Result<StorageRpcObjectPayloadReclaimResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let reclaim = decoder.read_optional_object_payload_reclaim()?;
    decoder.finish()?;
    Ok(StorageRpcObjectPayloadReclaimResponse { reclaim })
}

pub(crate) fn encode_payload_reclaim_root_response(
    response: &StorageRpcPayloadReclaimRootResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_optional_payload_reclaim_root(&mut out, response.root.as_ref());
    out
}

pub(crate) fn decode_payload_reclaim_root_response(
    bytes: &[u8],
) -> Result<StorageRpcPayloadReclaimRootResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let root = decoder.read_optional_payload_reclaim_root()?;
    decoder.finish()?;
    Ok(StorageRpcPayloadReclaimRootResponse { root })
}

pub(crate) fn encode_object_payload_reclaim_claim_acquire_request(
    request: &StorageRpcObjectPayloadReclaimClaimAcquireRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_bucket_write_drain_identity(&request.claim_id, &request.owner_token)?;
    if request
        .lease_deadline
        .is_some_and(|lease_deadline| lease_deadline <= request.claimed_at)
    {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "claim lease deadline must be after claimed time",
        ));
    }
    let mut out = encode_object_payload_reclaim_exists_request(
        &StorageRpcObjectPayloadReclaimExistsRequest {
            object: request.object.clone(),
            generation_id: request.generation_id,
        },
    );
    put_u64(&mut out, request.bucket_incarnation_generation);
    put_u8(&mut out, request.reclaim_kind as u8);
    put_string(&mut out, &request.claim_id);
    put_string(&mut out, &request.owner_token);
    put_u64(&mut out, request.claimed_at);
    put_optional_u64(&mut out, request.lease_deadline);
    put_u64(&mut out, request.now);
    put_admitted_route_effect_deadline(&mut out, request.effect_deadline);
    Ok(out)
}

pub(crate) fn decode_object_payload_reclaim_claim_acquire_request(
    bytes: &[u8],
) -> Result<StorageRpcObjectPayloadReclaimClaimAcquireRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let generation_id = decoder.read_generation_id()?;
    let bucket_incarnation_generation = decoder.read_u64()?;
    let reclaim_kind = decoder.read_object_payload_reclaim_kind()?;
    let claim_id = decoder.read_string_with_limit(
        STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN,
        StorageRpcPayloadError::InvalidDurableClaimToken("claim id exceeds maximum length"),
    )?;
    let owner_token = decoder.read_string_with_limit(
        STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN,
        StorageRpcPayloadError::InvalidDurableClaimToken("owner token exceeds maximum length"),
    )?;
    let claimed_at = decoder.read_u64()?;
    let lease_deadline = decoder.read_optional_u64()?;
    let now = decoder.read_u64()?;
    let effect_deadline =
        decoder.read_admitted_route_effect_deadline("object payload reclaim claim acquire")?;
    decoder.finish()?;
    let request = StorageRpcObjectPayloadReclaimClaimAcquireRequest {
        object,
        bucket_incarnation_generation,
        generation_id,
        reclaim_kind,
        claim_id,
        owner_token,
        claimed_at,
        lease_deadline,
        now,
        effect_deadline,
    };
    encode_object_payload_reclaim_claim_acquire_request(&request)?;
    Ok(request)
}

pub(crate) fn encode_object_payload_reclaim_claim_optional_record_response(
    response: &StorageRpcObjectPayloadReclaimClaimOptionalRecordResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    match &response.record {
        Some(record) => {
            validate_object_payload_reclaim_claim_record(record)?;
            put_u8(&mut out, 1);
            put_object_payload_reclaim_claim_record(&mut out, record);
        }
        None => put_u8(&mut out, 0),
    }
    Ok(out)
}

pub(crate) fn decode_object_payload_reclaim_claim_optional_record_response(
    bytes: &[u8],
) -> Result<StorageRpcObjectPayloadReclaimClaimOptionalRecordResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let record = match decoder.read_u8()? {
        0 => None,
        1 => {
            let record = decoder.read_object_payload_reclaim_claim_record()?;
            validate_object_payload_reclaim_claim_record(&record)?;
            Some(record)
        }
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "unknown optional object payload reclaim claim record tag",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcObjectPayloadReclaimClaimOptionalRecordResponse { record })
}

pub(crate) fn encode_object_payload_reclaim_claim_record_request(
    request: &StorageRpcObjectPayloadReclaimClaimRecordRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.cluster_epoch != request.record.cluster_epoch {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "request route epoch must match claim epoch",
        ));
    }
    if request.pg_id.get() != request.record.pg_id {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "request route PG must match claim PG",
        ));
    }
    validate_object_payload_reclaim_claim_record(&request.record)?;
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.cluster_epoch.get());
    put_u32(&mut out, request.pg_id.get());
    put_object_payload_reclaim_claim_record(&mut out, &request.record);
    Ok(out)
}

pub(crate) fn decode_object_payload_reclaim_claim_record_request(
    bytes: &[u8],
) -> Result<StorageRpcObjectPayloadReclaimClaimRecordRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let record = decoder.read_object_payload_reclaim_claim_record()?;
    decoder.finish()?;
    if cluster_epoch != record.cluster_epoch {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "request route epoch must match claim epoch",
        ));
    }
    if pg_id.get() != record.pg_id {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "request route PG must match claim PG",
        ));
    }
    validate_object_payload_reclaim_claim_record(&record)?;
    Ok(StorageRpcObjectPayloadReclaimClaimRecordRequest {
        node_id,
        cluster_epoch,
        pg_id,
        record,
    })
}

pub(crate) fn encode_multipart_completion_stale_source_response(
    response: &StorageRpcMultipartCompletionStaleSourceResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_optional_stored_object(&mut out, response.source.as_ref());
    out
}

pub(crate) fn decode_multipart_completion_stale_source_response(
    bytes: &[u8],
) -> Result<StorageRpcMultipartCompletionStaleSourceResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let source = decoder.read_optional_stored_object()?;
    decoder.finish()?;
    Ok(StorageRpcMultipartCompletionStaleSourceResponse { source })
}

pub(crate) fn encode_multipart_upload_load_request(
    request: &StorageRpcMultipartUploadLoadRequest,
) -> Vec<u8> {
    let mut out = encode_object_request(&request.object);
    put_string(&mut out, request.upload_id.as_str());
    out
}

pub(crate) fn decode_multipart_upload_load_request(
    bytes: &[u8],
) -> Result<StorageRpcMultipartUploadLoadRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let upload_id = decoder.read_upload_id()?;
    decoder.finish()?;
    Ok(StorageRpcMultipartUploadLoadRequest { object, upload_id })
}

pub(crate) fn encode_multipart_upload_load_response(
    response: &StorageRpcMultipartUploadLoadResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcMultipartUploadLoadOutcome::Loaded(upload) => {
            put_u8(&mut out, 0);
            put_multipart_upload_record(&mut out, upload);
        }
        StorageRpcMultipartUploadLoadOutcome::NoSuchUpload { upload_id } => {
            put_u8(&mut out, 1);
            put_string(&mut out, upload_id.as_str());
        }
    }
    Ok(out)
}

pub(crate) fn decode_multipart_upload_load_response(
    bytes: &[u8],
) -> Result<StorageRpcMultipartUploadLoadResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => StorageRpcMultipartUploadLoadOutcome::Loaded(Box::new(
            decoder.read_multipart_upload_record()?,
        )),
        1 => StorageRpcMultipartUploadLoadOutcome::NoSuchUpload {
            upload_id: decoder.read_upload_id()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid multipart upload load outcome",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcMultipartUploadLoadResponse { outcome })
}

pub(crate) fn encode_multipart_completion_snapshot_request(
    request: &StorageRpcMultipartCompletionSnapshotRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_multipart_upload_record_identity(
        &request.object,
        &request.authorized_upload,
        "multipart completion snapshot request identity mismatch",
    )?;
    let mut out = encode_object_request(&request.object);
    put_multipart_upload_record(&mut out, &request.authorized_upload);
    put_u32(
        &mut out,
        u32::try_from(request.requested_part_numbers.len())
            .expect("requested part number count must fit in u32"),
    );
    for part_number in &request.requested_part_numbers {
        put_u32(&mut out, *part_number);
    }
    Ok(out)
}

pub(crate) fn decode_multipart_completion_snapshot_request(
    bytes: &[u8],
) -> Result<StorageRpcMultipartCompletionSnapshotRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let authorized_upload = decoder.read_multipart_upload_record()?;
    validate_multipart_upload_record_identity(
        &object,
        &authorized_upload,
        "multipart completion snapshot request identity mismatch",
    )?;
    let part_number_count = decoder.read_bounded_remaining_count(
        4,
        "multipart completion requested part count exceeds payload",
    )?;
    let mut requested_part_numbers = Vec::new();
    for _ in 0..part_number_count {
        requested_part_numbers.push(decoder.read_u32()?);
    }
    decoder.finish()?;
    Ok(StorageRpcMultipartCompletionSnapshotRequest {
        object,
        authorized_upload,
        requested_part_numbers,
    })
}

pub(crate) fn encode_multipart_completion_snapshot_response(
    response: &StorageRpcMultipartCompletionSnapshotResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcMultipartCompletionSnapshotOutcome::Loaded(snapshot) => {
            put_u8(&mut out, 0);
            put_multipart_completion_snapshot(&mut out, snapshot);
        }
        StorageRpcMultipartCompletionSnapshotOutcome::NoSuchUpload { upload_id } => {
            put_u8(&mut out, 1);
            put_string(&mut out, upload_id.as_str());
        }
        StorageRpcMultipartCompletionSnapshotOutcome::PartNotFound {
            upload_id,
            part_number,
        } => {
            put_u8(&mut out, 2);
            put_string(&mut out, upload_id.as_str());
            put_u32(&mut out, *part_number);
        }
    }
    Ok(out)
}

pub(crate) fn decode_multipart_completion_snapshot_response(
    bytes: &[u8],
    subject: MultipartCompletionSubject,
) -> Result<StorageRpcMultipartCompletionSnapshotResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => StorageRpcMultipartCompletionSnapshotOutcome::Loaded(Box::new(
            decoder.read_multipart_completion_snapshot(subject)?,
        )),
        1 => StorageRpcMultipartCompletionSnapshotOutcome::NoSuchUpload {
            upload_id: decoder.read_upload_id()?,
        },
        2 => StorageRpcMultipartCompletionSnapshotOutcome::PartNotFound {
            upload_id: decoder.read_upload_id()?,
            part_number: decoder.read_u32()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid multipart completion snapshot outcome",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcMultipartCompletionSnapshotResponse { outcome })
}

pub(crate) fn encode_multipart_completion_preflight_request(
    request: &StorageRpcMultipartCompletionPreflightRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_multipart_upload_record_identity(
        &request.object,
        &request.authorized_upload,
        "multipart completion preflight request identity mismatch",
    )?;
    let mut out = encode_object_request(&request.object);
    put_multipart_upload_record(&mut out, &request.authorized_upload);
    Ok(out)
}

pub(crate) fn decode_multipart_completion_preflight_request(
    bytes: &[u8],
) -> Result<StorageRpcMultipartCompletionPreflightRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let authorized_upload = decoder.read_multipart_upload_record()?;
    validate_multipart_upload_record_identity(
        &object,
        &authorized_upload,
        "multipart completion preflight request identity mismatch",
    )?;
    decoder.finish()?;
    Ok(StorageRpcMultipartCompletionPreflightRequest {
        object,
        authorized_upload,
    })
}

pub(crate) fn encode_multipart_completion_preflight_response(
    response: &StorageRpcMultipartCompletionPreflightResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcMultipartCompletionPreflightOutcome::Loaded(preflight) => {
            put_u8(&mut out, 0);
            put_optional_string(&mut out, preflight.existing_etag.as_deref());
        }
        StorageRpcMultipartCompletionPreflightOutcome::NoSuchUpload { upload_id } => {
            put_u8(&mut out, 1);
            put_string(&mut out, upload_id.as_str());
        }
    }
    out
}

pub(crate) fn decode_multipart_completion_preflight_response(
    bytes: &[u8],
) -> Result<StorageRpcMultipartCompletionPreflightResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => StorageRpcMultipartCompletionPreflightOutcome::Loaded(MultipartCompletionPreflight {
            existing_etag: decoder.read_optional_string()?,
        }),
        1 => StorageRpcMultipartCompletionPreflightOutcome::NoSuchUpload {
            upload_id: decoder.read_upload_id()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid multipart completion preflight outcome",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcMultipartCompletionPreflightResponse { outcome })
}

pub(crate) fn encode_multipart_parts_list_request(
    request: &StorageRpcMultipartPartsListRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_multipart_upload_record_identity(
        &request.object,
        &request.authorized_upload,
        "multipart parts list request identity mismatch",
    )?;
    let mut out = encode_object_request(&request.object);
    put_multipart_upload_record(&mut out, &request.authorized_upload);
    put_optional_u32(&mut out, request.part_number_marker);
    put_u32(&mut out, request.max_parts);
    Ok(out)
}

pub(crate) fn decode_multipart_parts_list_request(
    bytes: &[u8],
) -> Result<StorageRpcMultipartPartsListRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let authorized_upload = decoder.read_multipart_upload_record()?;
    validate_multipart_upload_record_identity(
        &object,
        &authorized_upload,
        "multipart parts list request identity mismatch",
    )?;
    let part_number_marker = decoder.read_optional_u32()?;
    let max_parts = decoder.read_u32()?;
    decoder.finish()?;
    Ok(StorageRpcMultipartPartsListRequest {
        object,
        authorized_upload,
        part_number_marker,
        max_parts,
    })
}

pub(crate) fn encode_multipart_parts_list_response(
    response: &StorageRpcMultipartPartsListResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcMultipartPartsListOutcome::Loaded(listed) => {
            put_u8(&mut out, 0);
            put_listed_multipart_parts(&mut out, listed);
        }
        StorageRpcMultipartPartsListOutcome::NoSuchUpload { upload_id } => {
            put_u8(&mut out, 1);
            put_string(&mut out, upload_id.as_str());
        }
    }
    Ok(out)
}

pub(crate) fn decode_multipart_parts_list_response(
    bytes: &[u8],
) -> Result<StorageRpcMultipartPartsListResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => StorageRpcMultipartPartsListOutcome::Loaded(Box::new(
            decoder.read_listed_multipart_parts()?,
        )),
        1 => StorageRpcMultipartPartsListOutcome::NoSuchUpload {
            upload_id: decoder.read_upload_id()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid multipart parts list outcome",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcMultipartPartsListResponse { outcome })
}

pub(crate) fn encode_multipart_management_lookup_response(
    response: &StorageRpcMultipartManagementLookupResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_multipart_upload_management_lookup(&mut out, &response.lookup);
    out
}

pub(crate) fn decode_multipart_management_lookup_response(
    bytes: &[u8],
) -> Result<StorageRpcMultipartManagementLookupResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let lookup = decoder.read_multipart_upload_management_lookup()?;
    decoder.finish()?;
    Ok(StorageRpcMultipartManagementLookupResponse { lookup })
}

pub(crate) fn encode_object_generation_reservation_request(
    request: &StorageRpcObjectGenerationReservationRequest,
) -> Vec<u8> {
    let mut out = encode_object_request(&request.object);
    put_string(&mut out, request.reservation_id.as_str());
    out
}

pub(crate) fn decode_object_generation_reservation_request(
    bytes: &[u8],
) -> Result<StorageRpcObjectGenerationReservationRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let bucket = decoder.read_bucket_name()?;
    let key = decoder.read_object_key()?;
    let reservation_id = decoder.read_session_id()?;
    decoder.finish()?;
    Ok(StorageRpcObjectGenerationReservationRequest {
        object: StorageRpcObjectRequest {
            node_id,
            cluster_epoch,
            pg_id,
            bucket,
            key,
        },
        reservation_id,
    })
}

pub(crate) fn encode_direct_put_commit_snapshot_request(
    request: &StorageRpcDirectPutCommitSnapshotRequest,
) -> Vec<u8> {
    let mut out = encode_object_request(&request.object);
    put_string(&mut out, request.reservation_id.as_str());
    put_u64(&mut out, request.generation_id.get());
    out
}

pub(crate) fn decode_direct_put_commit_snapshot_request(
    bytes: &[u8],
) -> Result<StorageRpcDirectPutCommitSnapshotRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let bucket = decoder.read_bucket_name()?;
    let key = decoder.read_object_key()?;
    let reservation_id = decoder.read_session_id()?;
    let generation_id = decoder.read_generation_id()?;
    decoder.finish()?;
    Ok(StorageRpcDirectPutCommitSnapshotRequest {
        object: StorageRpcObjectRequest {
            node_id,
            cluster_epoch,
            pg_id,
            bucket,
            key,
        },
        reservation_id,
        generation_id,
    })
}

pub(crate) fn encode_object_read_auth_subject_request(
    request: &StorageRpcObjectReadAuthSubjectRequest,
) -> Vec<u8> {
    let mut out = encode_object_request(&request.object);
    put_optional_version_id(&mut out, request.version_id);
    out
}

pub(crate) fn decode_object_read_auth_subject_request(
    bytes: &[u8],
) -> Result<StorageRpcObjectReadAuthSubjectRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let version_id = decoder.read_optional_version_id()?;
    decoder.finish()?;
    Ok(StorageRpcObjectReadAuthSubjectRequest { object, version_id })
}

pub(crate) fn encode_object_read_snapshot_request(
    request: &StorageRpcObjectReadSnapshotRequest,
) -> Vec<u8> {
    let mut out = encode_object_request(&request.object);
    put_optional_version_id(&mut out, request.version_id);
    put_stored_object(&mut out, request.expected_identity.stored());
    put_object_read_snapshot_mode(&mut out, request.snapshot_mode);
    out
}

pub(crate) fn decode_object_read_snapshot_request(
    bytes: &[u8],
) -> Result<StorageRpcObjectReadSnapshotRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let version_id = decoder.read_optional_version_id()?;
    let expected_stored = decoder.read_stored_object()?;
    let snapshot_mode = decoder.read_object_read_snapshot_mode()?;
    decoder.finish()?;
    Ok(StorageRpcObjectReadSnapshotRequest {
        object,
        version_id,
        expected_identity: ObjectReadAuthSubjectIdentity::for_stored(&expected_stored),
        snapshot_mode,
    })
}

pub(crate) fn encode_put_object_metadata_snapshot_request(
    request: &StorageRpcPutObjectMetadataSnapshotRequest,
) -> Vec<u8> {
    let mut out = encode_object_request(&request.object);
    put_optional_version_id(&mut out, request.version_id);
    out
}

pub(crate) fn decode_put_object_metadata_snapshot_request(
    bytes: &[u8],
) -> Result<StorageRpcPutObjectMetadataSnapshotRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let version_id = decoder.read_optional_version_id()?;
    decoder.finish()?;
    Ok(StorageRpcPutObjectMetadataSnapshotRequest { object, version_id })
}

pub(crate) fn encode_object_delete_snapshot_request(
    request: &StorageRpcObjectDeleteSnapshotRequest,
) -> Vec<u8> {
    let mut out = encode_object_request(&request.object);
    put_optional_version_id(&mut out, request.version_id);
    out
}

pub(crate) fn decode_object_delete_snapshot_request(
    bytes: &[u8],
) -> Result<StorageRpcObjectDeleteSnapshotRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let version_id = decoder.read_optional_version_id()?;
    decoder.finish()?;
    Ok(StorageRpcObjectDeleteSnapshotRequest { object, version_id })
}

pub(crate) fn encode_put_object_metadata_command_build_request(
    request: &StorageRpcPutObjectMetadataCommandBuildRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.expected_stored.bucket() != &request.object.bucket
        || request.expected_stored.key() != &request.object.key
        || request.bucket_write_reservation.bucket != request.object.bucket
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "object metadata command build request identity mismatch",
        ));
    }
    let mut out = encode_object_request(&request.object);
    put_optional_version_id(&mut out, request.requested_version_id);
    put_stored_object(&mut out, &request.expected_stored);
    put_u64(&mut out, request.version_id.to_u64());
    put_put_object_metadata_mutation(&mut out, &request.mutation);
    put_bucket_write_reservation_proof(&mut out, &request.bucket_write_reservation);
    Ok(out)
}

pub(crate) fn decode_put_object_metadata_command_build_request(
    bytes: &[u8],
) -> Result<StorageRpcPutObjectMetadataCommandBuildRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let requested_version_id = decoder.read_optional_version_id()?;
    let expected_stored = decoder.read_stored_object()?;
    let version_id = VersionId::from_u64(decoder.read_u64()?);
    let mutation = decoder.read_put_object_metadata_mutation()?;
    let bucket_write_reservation = decoder.read_bucket_write_reservation_proof()?;
    decoder.finish()?;
    if expected_stored.bucket() != &object.bucket
        || expected_stored.key() != &object.key
        || bucket_write_reservation.bucket != object.bucket
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "object metadata command build request identity mismatch",
        ));
    }
    Ok(StorageRpcPutObjectMetadataCommandBuildRequest {
        object,
        requested_version_id,
        expected_stored,
        version_id,
        mutation,
        bucket_write_reservation,
    })
}

pub(crate) fn encode_delete_specific_object_command_build_request(
    request: &StorageRpcDeleteSpecificObjectCommandBuildRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.expected_stored.as_ref().is_some_and(|stored| {
        stored.bucket() != &request.object.bucket || stored.key() != &request.object.key
    }) || request.expected_target.as_ref().is_some_and(|target| {
        !delete_target_matches_object(target, &request.object.bucket, &request.object.key)
    }) || request
        .expected_version_list
        .as_ref()
        .is_some_and(|versions| {
            versions.iter().any(|stored| {
                stored.bucket() != &request.object.bucket || stored.key() != &request.object.key
            })
        })
        || request.bucket_write_reservation.bucket != request.object.bucket
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "delete-specific command build request identity mismatch",
        ));
    }
    let mut out = encode_object_request(&request.object);
    put_u64(&mut out, request.version_id.to_u64());
    put_optional_stored_object(&mut out, request.expected_stored.as_ref());
    put_optional_delete_object_version_target(&mut out, request.expected_target.as_ref());
    put_optional_stored_object_list(&mut out, request.expected_version_list.as_deref());
    put_bucket_write_reservation_proof(&mut out, &request.bucket_write_reservation);
    Ok(out)
}

pub(crate) fn decode_delete_specific_object_command_build_request(
    bytes: &[u8],
) -> Result<StorageRpcDeleteSpecificObjectCommandBuildRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let version_id = VersionId::from_u64(decoder.read_u64()?);
    let expected_stored = decoder.read_optional_stored_object()?;
    let expected_target = decoder.read_optional_delete_object_version_target()?;
    let expected_version_list = decoder.read_optional_stored_object_list()?;
    let bucket_write_reservation = decoder.read_bucket_write_reservation_proof()?;
    decoder.finish()?;
    if expected_stored
        .as_ref()
        .is_some_and(|stored| stored.bucket() != &object.bucket || stored.key() != &object.key)
        || expected_target.as_ref().is_some_and(|target| {
            !delete_target_matches_object(target, &object.bucket, &object.key)
        })
        || expected_version_list.as_ref().is_some_and(|versions| {
            versions
                .iter()
                .any(|stored| stored.bucket() != &object.bucket || stored.key() != &object.key)
        })
        || bucket_write_reservation.bucket != object.bucket
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "delete-specific command build request identity mismatch",
        ));
    }
    Ok(StorageRpcDeleteSpecificObjectCommandBuildRequest {
        object,
        version_id,
        expected_stored,
        expected_target,
        expected_version_list,
        bucket_write_reservation,
    })
}

pub(crate) fn encode_delete_current_object_command_build_request(
    request: &StorageRpcDeleteCurrentObjectCommandBuildRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.expected_current.as_ref().is_some_and(|stored| {
        stored.bucket() != &request.object.bucket || stored.key() != &request.object.key
    }) || request.expected_target.as_ref().is_some_and(|target| {
        !delete_target_matches_object(target, &request.object.bucket, &request.object.key)
    }) || request.bucket_write_reservation.bucket != request.object.bucket
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "delete-current command build request identity mismatch",
        ));
    }
    let mut out = encode_object_request(&request.object);
    put_optional_stored_object(&mut out, request.expected_current.as_ref());
    put_optional_delete_object_version_target(&mut out, request.expected_target.as_ref());
    put_bucket_write_reservation_proof(&mut out, &request.bucket_write_reservation);
    Ok(out)
}

pub(crate) fn decode_delete_current_object_command_build_request(
    bytes: &[u8],
) -> Result<StorageRpcDeleteCurrentObjectCommandBuildRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let expected_current = decoder.read_optional_stored_object()?;
    let expected_target = decoder.read_optional_delete_object_version_target()?;
    let bucket_write_reservation = decoder.read_bucket_write_reservation_proof()?;
    decoder.finish()?;
    if expected_current
        .as_ref()
        .is_some_and(|stored| stored.bucket() != &object.bucket || stored.key() != &object.key)
        || expected_target.as_ref().is_some_and(|target| {
            !delete_target_matches_object(target, &object.bucket, &object.key)
        })
        || bucket_write_reservation.bucket != object.bucket
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "delete-current command build request identity mismatch",
        ));
    }
    Ok(StorageRpcDeleteCurrentObjectCommandBuildRequest {
        object,
        expected_current,
        expected_target,
        bucket_write_reservation,
    })
}

pub(crate) fn encode_insert_delete_marker_command_build_request(
    request: &StorageRpcInsertDeleteMarkerCommandBuildRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.expected_current.as_ref().is_some_and(|stored| {
        stored.bucket() != &request.object.bucket || stored.key() != &request.object.key
    }) || request
        .expected_stale_payload_source
        .as_ref()
        .is_some_and(|stored| {
            stored.bucket() != &request.object.bucket || stored.key() != &request.object.key
        })
        || request.bucket_write_reservation.bucket != request.object.bucket
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "insert-delete-marker command build request identity mismatch",
        ));
    }
    let mut out = encode_object_request(&request.object);
    put_optional_stored_object(&mut out, request.expected_current.as_ref());
    put_optional_stored_object(&mut out, request.expected_stale_payload_source.as_ref());
    put_u64(&mut out, request.version_id.to_u64());
    put_owner_identity(&mut out, &request.owner);
    put_insert_delete_marker_stale_payload(&mut out, &request.stale_payload);
    put_bucket_write_reservation_proof(&mut out, &request.bucket_write_reservation);
    Ok(out)
}

pub(crate) fn decode_insert_delete_marker_command_build_request(
    bytes: &[u8],
) -> Result<StorageRpcInsertDeleteMarkerCommandBuildRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let expected_current = decoder.read_optional_stored_object()?;
    let expected_stale_payload_source = decoder.read_optional_stored_object()?;
    let version_id = VersionId::from_u64(decoder.read_u64()?);
    let owner = decoder.read_owner_identity()?;
    let stale_payload = decoder.read_insert_delete_marker_stale_payload()?;
    let bucket_write_reservation = decoder.read_bucket_write_reservation_proof()?;
    decoder.finish()?;
    if expected_current
        .as_ref()
        .is_some_and(|stored| stored.bucket() != &object.bucket || stored.key() != &object.key)
        || expected_stale_payload_source
            .as_ref()
            .is_some_and(|stored| stored.bucket() != &object.bucket || stored.key() != &object.key)
        || bucket_write_reservation.bucket != object.bucket
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "insert-delete-marker command build request identity mismatch",
        ));
    }
    Ok(StorageRpcInsertDeleteMarkerCommandBuildRequest {
        object,
        expected_current,
        expected_stale_payload_source,
        version_id,
        owner,
        stale_payload,
        bucket_write_reservation,
    })
}

pub(crate) fn encode_stream_upload_match_request(
    request: &StorageRpcStreamUploadMatchRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_create_stream_upload_request_identity(&request.object, &request.request)?;
    if request.expected_command.as_ref().is_some_and(|command| {
        !create_stream_upload_command_matches_request(command, &request.request)
    }) {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "stream upload match expected command identity mismatch",
        ));
    }
    let mut out = encode_object_request(&request.object);
    put_create_stream_upload_req(&mut out, &request.request);
    put_optional_create_stream_upload_command(&mut out, request.expected_command.as_ref());
    Ok(out)
}

pub(crate) fn decode_stream_upload_match_request(
    bytes: &[u8],
) -> Result<StorageRpcStreamUploadMatchRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let request = decoder.read_create_stream_upload_req()?;
    let expected_command = decoder.read_optional_create_stream_upload_command()?;
    decoder.finish()?;
    validate_create_stream_upload_request_identity(&object, &request)?;
    if expected_command
        .as_ref()
        .is_some_and(|command| !create_stream_upload_command_matches_request(command, &request))
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "stream upload match expected command identity mismatch",
        ));
    }
    Ok(StorageRpcStreamUploadMatchRequest {
        object,
        request,
        expected_command,
    })
}

pub(crate) fn encode_multipart_upload_match_request(
    request: &StorageRpcMultipartUploadMatchRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_create_multipart_upload_request_identity(&request.object, &request.request)?;
    if request
        .expected_command
        .as_ref()
        .is_some_and(|command| !command.matches_request(&request.request))
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "multipart upload match expected command identity mismatch",
        ));
    }
    let mut out = encode_object_request(&request.object);
    put_create_multipart_upload_req(&mut out, &request.request);
    put_optional_create_multipart_upload_command(&mut out, request.expected_command.as_ref());
    Ok(out)
}

pub(crate) fn decode_multipart_upload_match_request(
    bytes: &[u8],
    authority: &MetadataCommandDecodeAuthority,
) -> Result<StorageRpcMultipartUploadMatchRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let request = decoder.read_create_multipart_upload_req()?;
    let expected_command = decoder.read_optional_create_multipart_upload_command(authority)?;
    decoder.finish()?;
    validate_create_multipart_upload_request_identity(&object, &request)?;
    if expected_command
        .as_ref()
        .is_some_and(|command| !command.matches_request(&request))
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "multipart upload match expected command identity mismatch",
        ));
    }
    Ok(StorageRpcMultipartUploadMatchRequest {
        object,
        request,
        expected_command,
    })
}

pub(crate) fn encode_stream_upload_match_response(
    response: &StorageRpcStreamUploadMatchResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_bool(&mut out, response.exists);
    out
}

pub(crate) fn decode_stream_upload_match_response(
    bytes: &[u8],
) -> Result<StorageRpcStreamUploadMatchResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let exists = decoder.read_bool()?;
    decoder.finish()?;
    Ok(StorageRpcStreamUploadMatchResponse { exists })
}

pub(crate) fn encode_stream_upload_session_request(
    request: &StorageRpcStreamUploadSessionRequest,
) -> Vec<u8> {
    let mut out = encode_object_request(&request.object);
    put_string(&mut out, request.session_id.as_str());
    out
}

pub(crate) fn decode_stream_upload_session_request(
    bytes: &[u8],
) -> Result<StorageRpcStreamUploadSessionRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let session_id = decoder.read_session_id()?;
    decoder.finish()?;
    Ok(StorageRpcStreamUploadSessionRequest { object, session_id })
}

pub(crate) fn encode_stream_upload_bucket_write_reservation_update_request(
    request: &StorageRpcStreamUploadBucketWriteReservationUpdateRequest,
) -> Vec<u8> {
    let mut out = encode_stream_upload_session_request(&StorageRpcStreamUploadSessionRequest {
        object: request.object.clone(),
        session_id: request.session_id.clone(),
    });
    put_bucket_write_reservation_proof(&mut out, &request.current);
    put_bucket_write_reservation_proof(&mut out, &request.renewed);
    put_admitted_route_effect_deadline(&mut out, request.effect_deadline);
    out
}

pub(crate) fn decode_stream_upload_bucket_write_reservation_update_request(
    bytes: &[u8],
) -> Result<StorageRpcStreamUploadBucketWriteReservationUpdateRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let session_id = decoder.read_session_id()?;
    let current = decoder.read_bucket_write_reservation_proof()?;
    let renewed = decoder.read_bucket_write_reservation_proof()?;
    let effect_deadline = decoder
        .read_admitted_route_effect_deadline("stream upload bucket write reservation update")?;
    decoder.finish()?;
    Ok(StorageRpcStreamUploadBucketWriteReservationUpdateRequest {
        object,
        session_id,
        current,
        renewed,
        effect_deadline,
    })
}

pub(crate) fn encode_stream_upload_session_response(
    response: &StorageRpcStreamUploadSessionResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcStreamUploadSessionOutcome::Loaded(session) => {
            put_u8(&mut out, 1);
            put_stream_upload_record(&mut out, session);
        }
        StorageRpcStreamUploadSessionOutcome::NotFound { session_id } => {
            put_u8(&mut out, 2);
            put_string(&mut out, session_id.as_str());
        }
    }
    out
}

pub(crate) fn decode_stream_upload_session_response(
    bytes: &[u8],
) -> Result<StorageRpcStreamUploadSessionResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        1 => StorageRpcStreamUploadSessionOutcome::Loaded(Box::new(
            decoder.read_stream_upload_record()?,
        )),
        2 => StorageRpcStreamUploadSessionOutcome::NotFound {
            session_id: decoder.read_session_id()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid stream upload session outcome",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcStreamUploadSessionResponse { outcome })
}

pub(crate) fn encode_stream_uploads_list_response(
    response: &StorageRpcStreamUploadsListResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let count = u32::try_from(response.uploads.len()).map_err(|_| {
        StorageRpcPayloadError::PayloadTooLarge {
            len: response.uploads.len(),
            limit: STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS as usize,
        }
    })?;
    if count > STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: count as usize,
            limit: STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS as usize,
        });
    }
    let mut out = Vec::new();
    put_u32(&mut out, count);
    for upload in &response.uploads {
        put_stream_upload_record(&mut out, upload);
    }
    put_optional_string(
        &mut out,
        response
            .next_session_id_marker
            .as_ref()
            .map(SessionId::as_str),
    );
    Ok(out)
}

pub(crate) fn decode_stream_uploads_list_response(
    bytes: &[u8],
) -> Result<StorageRpcStreamUploadsListResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let upload_count = decoder.read_limited_bounded_remaining_count(
        STORAGE_RPC_MIN_STREAM_UPLOAD_RECORD_LEN,
        "too many stream uploads",
        STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS,
    )?;
    let mut uploads = Vec::new();
    for _ in 0..upload_count {
        uploads.push(decoder.read_stream_upload_record()?);
    }
    let next_session_id_marker = decoder.read_optional_session_id()?;
    decoder.finish()?;
    Ok(StorageRpcStreamUploadsListResponse {
        uploads,
        next_session_id_marker,
    })
}

pub(crate) fn encode_stream_upload_segments_response(
    response: &StorageRpcStreamUploadSegmentsResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcStreamUploadSegmentsOutcome::Loaded(segments) => {
            put_u8(&mut out, 1);
            put_u32(
                &mut out,
                u32::try_from(segments.len()).map_err(|_| {
                    StorageRpcPayloadError::InvalidObjectMetadataRequest(
                        "too many stream upload segments",
                    )
                })?,
            );
            for segment in segments {
                put_stream_upload_segment_record(&mut out, segment);
            }
        }
        StorageRpcStreamUploadSegmentsOutcome::NotFound { session_id } => {
            put_u8(&mut out, 2);
            put_string(&mut out, session_id.as_str());
        }
    }
    Ok(out)
}

pub(crate) fn decode_stream_upload_segments_response(
    bytes: &[u8],
) -> Result<StorageRpcStreamUploadSegmentsResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        1 => {
            let segment_count = decoder.read_bounded_remaining_count(
                STORAGE_RPC_MIN_STREAM_UPLOAD_SEGMENT_RECORD_LEN,
                "too many stream upload segments",
            )?;
            let mut segments = Vec::new();
            for _ in 0..segment_count {
                segments.push(decoder.read_stream_upload_segment_record()?);
            }
            StorageRpcStreamUploadSegmentsOutcome::Loaded(segments)
        }
        2 => StorageRpcStreamUploadSegmentsOutcome::NotFound {
            session_id: decoder.read_session_id()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid stream upload segments outcome",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcStreamUploadSegmentsResponse { outcome })
}

pub(crate) fn encode_stream_segment_append_prepare_request(
    request: &StorageRpcStreamSegmentAppendPrepareRequest,
) -> Vec<u8> {
    let mut out = encode_object_request(&request.object);
    put_prepare_stream_segment_append_req(&mut out, &request.request);
    match request.effect_deadline {
        Some(deadline) => {
            put_u8(&mut out, 1);
            put_u64(&mut out, deadline.authority_valid_until_ms);
            put_u64(&mut out, deadline.portable_wall_valid_until_ms);
        }
        None => put_u8(&mut out, 0),
    }
    out
}

pub(crate) fn decode_stream_segment_append_prepare_request(
    bytes: &[u8],
) -> Result<StorageRpcStreamSegmentAppendPrepareRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let request = decoder.read_prepare_stream_segment_append_req()?;
    let effect_deadline = match decoder.read_u8()? {
        0 => None,
        1 => Some(StorageRpcAdmittedRouteEffectDeadline {
            authority_valid_until_ms: decoder.read_u64()?,
            portable_wall_valid_until_ms: decoder.read_u64()?,
        }),
        _ => {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid stream segment append effect deadline",
            ));
        }
    };
    if effect_deadline
        .is_some_and(|deadline| !admitted_route_effect_deadline_is_conservative(deadline))
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "stream segment append effect deadline exceeds authority",
        ));
    }
    decoder.finish()?;
    Ok(StorageRpcStreamSegmentAppendPrepareRequest {
        object,
        request,
        effect_deadline,
    })
}

pub(crate) fn encode_stream_segment_append_prepare_response(
    response: &StorageRpcStreamSegmentAppendPrepareResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcStreamSegmentAppendPrepareOutcome::Prepared { target, segment } => {
            put_u8(&mut out, 1);
            put_stream_upload_target(&mut out, target);
            put_stream_upload_segment_record(&mut out, segment);
        }
        StorageRpcStreamSegmentAppendPrepareOutcome::NotFound { session_id } => {
            put_u8(&mut out, 2);
            put_string(&mut out, session_id.as_str());
        }
    }
    out
}

pub(crate) fn decode_stream_segment_append_prepare_response(
    bytes: &[u8],
) -> Result<StorageRpcStreamSegmentAppendPrepareResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        1 => StorageRpcStreamSegmentAppendPrepareOutcome::Prepared {
            target: decoder.read_stream_upload_target()?,
            segment: Box::new(decoder.read_stream_upload_segment_record()?),
        },
        2 => StorageRpcStreamSegmentAppendPrepareOutcome::NotFound {
            session_id: decoder.read_session_id()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid stream segment append prepare outcome",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcStreamSegmentAppendPrepareResponse { outcome })
}

pub(crate) fn encode_multipart_upload_match_response(
    response: &StorageRpcMultipartUploadMatchResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_optional_u64(&mut out, response.initiated_at);
    out
}

pub(crate) fn decode_multipart_upload_match_response(
    bytes: &[u8],
) -> Result<StorageRpcMultipartUploadMatchResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let initiated_at = decoder.read_optional_u64()?;
    decoder.finish()?;
    Ok(StorageRpcMultipartUploadMatchResponse { initiated_at })
}

pub(crate) fn encode_create_stream_upload_command_build_request(
    request: &StorageRpcCreateStreamUploadCommandBuildRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_create_stream_upload_request_identity(&request.object, &request.request)?;
    validate_create_stream_upload_precondition_identity(&request.object, &request.precondition)?;
    if request.bucket_write_reservation.bucket != request.object.bucket {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "stream upload command build reservation bucket mismatch",
        ));
    }
    let mut out = encode_object_request(&request.object);
    put_create_stream_upload_req(&mut out, &request.request);
    put_optional_u64(&mut out, request.cleanup_after);
    put_create_stream_upload_precondition(&mut out, &request.precondition);
    put_bucket_write_reservation_proof(&mut out, &request.bucket_write_reservation);
    Ok(out)
}

pub(crate) fn decode_create_stream_upload_command_build_request(
    bytes: &[u8],
) -> Result<StorageRpcCreateStreamUploadCommandBuildRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let request = decoder.read_create_stream_upload_req()?;
    let cleanup_after = decoder.read_optional_u64()?;
    let precondition = decoder.read_create_stream_upload_precondition()?;
    let bucket_write_reservation = decoder.read_bucket_write_reservation_proof()?;
    decoder.finish()?;
    validate_create_stream_upload_request_identity(&object, &request)?;
    validate_create_stream_upload_precondition_identity(&object, &precondition)?;
    if bucket_write_reservation.bucket != object.bucket {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "stream upload command build reservation bucket mismatch",
        ));
    }
    Ok(StorageRpcCreateStreamUploadCommandBuildRequest {
        object,
        request,
        cleanup_after,
        precondition,
        bucket_write_reservation,
    })
}

pub(crate) fn encode_create_multipart_upload_command_build_request(
    request: &StorageRpcCreateMultipartUploadCommandBuildRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_create_multipart_upload_request_identity(&request.object, &request.request)?;
    if request.expected_current.as_ref().is_some_and(|stored| {
        stored.bucket() != &request.object.bucket || stored.key() != &request.object.key
    }) || request.bucket_write_reservation.bucket != request.object.bucket
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "multipart upload command build request identity mismatch",
        ));
    }
    let mut out = encode_object_request(&request.object);
    put_create_multipart_upload_req(&mut out, &request.request);
    put_optional_stored_object(&mut out, request.expected_current.as_ref());
    put_bucket_write_reservation_proof(&mut out, &request.bucket_write_reservation);
    Ok(out)
}

pub(crate) fn decode_create_multipart_upload_command_build_request(
    bytes: &[u8],
) -> Result<StorageRpcCreateMultipartUploadCommandBuildRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let request = decoder.read_create_multipart_upload_req()?;
    let expected_current = decoder.read_optional_stored_object()?;
    let bucket_write_reservation = decoder.read_bucket_write_reservation_proof()?;
    decoder.finish()?;
    validate_create_multipart_upload_request_identity(&object, &request)?;
    if expected_current
        .as_ref()
        .is_some_and(|stored| stored.bucket() != &object.bucket || stored.key() != &object.key)
        || bucket_write_reservation.bucket != object.bucket
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "multipart upload command build request identity mismatch",
        ));
    }
    Ok(StorageRpcCreateMultipartUploadCommandBuildRequest {
        object,
        request,
        expected_current,
        bucket_write_reservation,
    })
}

pub(crate) fn encode_stream_put_finalize_snapshot_request(
    request: &StorageRpcStreamPutFinalizeSnapshotRequest,
) -> Vec<u8> {
    let mut out = encode_object_request(&request.object);
    put_string(&mut out, request.session_id.as_str());
    out
}

pub(crate) fn decode_stream_put_finalize_snapshot_request(
    bytes: &[u8],
) -> Result<StorageRpcStreamPutFinalizeSnapshotRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let session_id = decoder.read_session_id()?;
    decoder.finish()?;
    Ok(StorageRpcStreamPutFinalizeSnapshotRequest { object, session_id })
}

pub(crate) fn encode_stream_put_finalize_snapshot_response(
    response: &StorageRpcStreamPutFinalizeSnapshotResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_stream_put_finalize_snapshot_identity(
        &StorageRpcObjectRequest {
            node_id: NodeId::new(0),
            cluster_epoch: ClusterEpoch::new(1).expect("nonzero epoch"),
            pg_id: PgId::new(0),
            bucket: response.snapshot.session.bucket.clone(),
            key: response.snapshot.session.key.clone(),
        },
        &response.snapshot.session.session_id,
        &response.snapshot,
    )?;
    let mut out = Vec::new();
    put_stream_put_finalize_storage_snapshot(&mut out, &response.snapshot);
    Ok(out)
}

pub(crate) fn decode_stream_put_finalize_snapshot_response(
    bytes: &[u8],
) -> Result<StorageRpcStreamPutFinalizeSnapshotResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let snapshot = decoder.read_stream_put_finalize_storage_snapshot()?;
    decoder.finish()?;
    Ok(StorageRpcStreamPutFinalizeSnapshotResponse { snapshot })
}

pub(crate) fn encode_stream_put_commit_command_build_request(
    request: &StorageRpcStreamPutCommitCommandBuildRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_stream_put_finalize_snapshot_identity(
        &request.object,
        &request.session_id,
        &request.expected_snapshot,
    )?;
    if request.bucket_write_reservation.bucket != request.object.bucket {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "stream PUT commit reservation bucket mismatch",
        ));
    }
    let mut out = encode_object_request(&request.object);
    put_string(&mut out, request.session_id.as_str());
    put_u64(&mut out, request.total_size);
    put_stream_put_finalize_storage_snapshot(&mut out, &request.expected_snapshot);
    put_stream_put_commit_input(&mut out, &request.commit);
    put_bucket_write_reservation_proof(&mut out, &request.bucket_write_reservation);
    put_admitted_route_effect_deadline(&mut out, request.effect_deadline);
    Ok(out)
}

pub(crate) fn decode_stream_put_commit_command_build_request(
    bytes: &[u8],
) -> Result<StorageRpcStreamPutCommitCommandBuildRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let session_id = decoder.read_session_id()?;
    let total_size = decoder.read_u64()?;
    let expected_snapshot = decoder.read_stream_put_finalize_storage_snapshot()?;
    let commit = decoder.read_stream_put_commit_input()?;
    let bucket_write_reservation = decoder.read_bucket_write_reservation_proof()?;
    let effect_deadline =
        decoder.read_admitted_route_effect_deadline("stream PUT commit command build")?;
    decoder.finish()?;
    validate_stream_put_finalize_snapshot_identity(&object, &session_id, &expected_snapshot)?;
    if bucket_write_reservation.bucket != object.bucket {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "stream PUT commit reservation bucket mismatch",
        ));
    }
    Ok(StorageRpcStreamPutCommitCommandBuildRequest {
        object,
        session_id,
        total_size,
        expected_snapshot,
        commit,
        bucket_write_reservation,
        effect_deadline,
    })
}

pub(crate) fn encode_stream_part_finalize_snapshot_request(
    request: &StorageRpcStreamPartFinalizeSnapshotRequest,
) -> Vec<u8> {
    let mut out = encode_object_request(&request.object);
    put_string(&mut out, request.upload_id.as_str());
    put_string(&mut out, request.session_id.as_str());
    put_u32(&mut out, request.part_number);
    out
}

pub(crate) fn decode_stream_part_finalize_snapshot_request(
    bytes: &[u8],
) -> Result<StorageRpcStreamPartFinalizeSnapshotRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let upload_id = decoder.read_upload_id()?;
    let session_id = decoder.read_session_id()?;
    let part_number = decoder.read_u32()?;
    decoder.finish()?;
    Ok(StorageRpcStreamPartFinalizeSnapshotRequest {
        object,
        upload_id,
        session_id,
        part_number,
    })
}

pub(crate) fn encode_stream_part_finalize_snapshot_response(
    response: &StorageRpcStreamPartFinalizeSnapshotResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let auth = &response.snapshot.auth_snapshot;
    let StreamUploadTarget::UploadPart {
        upload_id,
        part_number,
    } = &auth.session.target
    else {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "stream part finalize response target mismatch",
        ));
    };
    validate_stream_part_finalize_snapshot_identity(
        &StorageRpcObjectRequest {
            node_id: NodeId::new(0),
            cluster_epoch: ClusterEpoch::new(1).expect("nonzero epoch"),
            pg_id: PgId::new(0),
            bucket: auth.session.bucket.clone(),
            key: auth.session.key.clone(),
        },
        upload_id,
        &auth.session.session_id,
        *part_number,
        &response.snapshot,
    )?;
    let mut out = Vec::new();
    put_stream_part_finalize_storage_snapshot(&mut out, &response.snapshot);
    Ok(out)
}

pub(crate) fn decode_stream_part_finalize_snapshot_response(
    bytes: &[u8],
) -> Result<StorageRpcStreamPartFinalizeSnapshotResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let snapshot = decoder.read_stream_part_finalize_storage_snapshot()?;
    decoder.finish()?;
    Ok(StorageRpcStreamPartFinalizeSnapshotResponse { snapshot })
}

pub(crate) fn encode_stream_part_commit_command_build_request(
    request: &StorageRpcStreamPartCommitCommandBuildRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_stream_part_finalize_snapshot_identity(
        &request.object,
        &request.upload_id,
        &request.session_id,
        request.part_number,
        &request.expected_snapshot,
    )?;
    if request.part.upload_id != request.upload_id
        || request.part.part_number != request.part_number
        || request.bucket_write_reservation.bucket != request.object.bucket
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "stream part commit request identity mismatch",
        ));
    }
    for segment in &request.segments {
        if segment.bucket != request.object.bucket
            || segment.key != request.object.key
            || segment.upload_id != request.upload_id
            || segment.part_number != request.part_number
        {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "stream part commit segment identity mismatch",
            ));
        }
    }
    let mut out = encode_object_request(&request.object);
    put_string(&mut out, request.upload_id.as_str());
    put_string(&mut out, request.session_id.as_str());
    put_u32(&mut out, request.part_number);
    put_stream_part_finalize_storage_snapshot(&mut out, &request.expected_snapshot);
    put_multipart_part_record(&mut out, &request.part);
    put_u32(
        &mut out,
        u32::try_from(request.segments.len()).expect("stream part segment count must fit in u32"),
    );
    for segment in &request.segments {
        put_multipart_part_segment_record(&mut out, segment);
    }
    put_bucket_write_reservation_proof(&mut out, &request.bucket_write_reservation);
    put_admitted_route_effect_deadline(&mut out, request.effect_deadline);
    Ok(out)
}

pub(crate) fn decode_stream_part_commit_command_build_request(
    bytes: &[u8],
) -> Result<StorageRpcStreamPartCommitCommandBuildRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let upload_id = decoder.read_upload_id()?;
    let session_id = decoder.read_session_id()?;
    let part_number = decoder.read_u32()?;
    let expected_snapshot = decoder.read_stream_part_finalize_storage_snapshot()?;
    let part = decoder.read_multipart_part_record()?;
    let segment_count = decoder.read_bounded_remaining_count(
        STORAGE_RPC_MIN_MULTIPART_PART_SEGMENT_RECORD_LEN,
        "stream part commit segment count exceeds payload",
    )?;
    let mut segments = Vec::new();
    for _ in 0..segment_count {
        segments.push(decoder.read_multipart_part_segment_record()?);
    }
    let bucket_write_reservation = decoder.read_bucket_write_reservation_proof()?;
    let effect_deadline =
        decoder.read_admitted_route_effect_deadline("stream part commit command build")?;
    decoder.finish()?;
    validate_stream_part_finalize_snapshot_identity(
        &object,
        &upload_id,
        &session_id,
        part_number,
        &expected_snapshot,
    )?;
    if part.upload_id != upload_id
        || part.part_number != part_number
        || bucket_write_reservation.bucket != object.bucket
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "stream part commit request identity mismatch",
        ));
    }
    for segment in &segments {
        if segment.bucket != object.bucket
            || segment.key != object.key
            || segment.upload_id != upload_id
            || segment.part_number != part_number
        {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "stream part commit segment identity mismatch",
            ));
        }
    }
    Ok(StorageRpcStreamPartCommitCommandBuildRequest {
        object,
        upload_id,
        session_id,
        part_number,
        expected_snapshot,
        part,
        segments,
        bucket_write_reservation,
        effect_deadline,
    })
}

pub(crate) fn encode_complete_multipart_command_build_request(
    request: &StorageRpcCompleteMultipartCommandBuildRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_complete_multipart_request_identity(&request.object, &request.request)?;
    if request.bucket_write_reservation.bucket != request.object.bucket {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "complete multipart reservation bucket mismatch",
        ));
    }
    let mut out = encode_object_request(&request.object);
    put_complete_multipart_commit_request(&mut out, &request.request);
    put_u64(&mut out, request.version_id.to_u64());
    put_bucket_write_reservation_proof(&mut out, &request.bucket_write_reservation);
    Ok(out)
}

pub(crate) fn decode_complete_multipart_command_build_request(
    bytes: &[u8],
) -> Result<StorageRpcCompleteMultipartCommandBuildRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let request = decoder.read_complete_multipart_commit_request()?;
    let version_id = VersionId::from_u64(decoder.read_u64()?);
    let bucket_write_reservation = decoder.read_bucket_write_reservation_proof()?;
    decoder.finish()?;
    validate_complete_multipart_request_identity(&object, &request)?;
    if bucket_write_reservation.bucket != object.bucket {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "complete multipart reservation bucket mismatch",
        ));
    }
    Ok(StorageRpcCompleteMultipartCommandBuildRequest {
        object,
        request,
        version_id,
        bucket_write_reservation,
    })
}

pub(crate) fn encode_abort_multipart_command_build_request(
    request: &StorageRpcAbortMultipartCommandBuildRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.bucket_write_reservation.bucket != request.object.bucket {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "abort multipart reservation bucket mismatch",
        ));
    }
    let mut out = encode_object_request(&request.object);
    put_string(&mut out, request.upload_id.as_str());
    put_optional_abort_multipart_upload_cleanup(&mut out, request.expected_cleanup.as_ref());
    put_bucket_write_reservation_proof(&mut out, &request.bucket_write_reservation);
    put_admitted_route_effect_deadline(&mut out, request.effect_deadline);
    Ok(out)
}

pub(crate) fn decode_abort_multipart_command_build_request(
    bytes: &[u8],
) -> Result<StorageRpcAbortMultipartCommandBuildRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let upload_id = decoder.read_upload_id()?;
    let expected_cleanup = decoder.read_optional_abort_multipart_upload_cleanup()?;
    let bucket_write_reservation = decoder.read_bucket_write_reservation_proof()?;
    let effect_deadline =
        decoder.read_admitted_route_effect_deadline("abort multipart command build")?;
    decoder.finish()?;
    if bucket_write_reservation.bucket != object.bucket {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "abort multipart request identity mismatch",
        ));
    }
    if let Some(cleanup) = expected_cleanup.as_ref() {
        validate_abort_multipart_cleanup_identity(
            cleanup,
            &object,
            &upload_id,
            "abort multipart request identity mismatch",
        )?;
    }
    Ok(StorageRpcAbortMultipartCommandBuildRequest {
        object,
        upload_id,
        expected_cleanup,
        bucket_write_reservation,
        effect_deadline,
    })
}

fn validate_object_payload_reclaim_command_build_request(
    request: &StorageRpcObjectPayloadReclaimCommandBuildRequest,
) -> Result<(), StorageRpcPayloadError> {
    validate_object_payload_reclaim_claim_record(&request.claim)?;
    let payload_matches = match &request.payload {
        ObjectPayloadReclaimCommand::Segments(record) => {
            record.bucket == request.object.bucket
                && record.key == request.object.key
                && record.generation_id == request.generation_id
        }
        ObjectPayloadReclaimCommand::Multipart(record) => {
            record.bucket == request.object.bucket
                && record.key == request.object.key
                && record.generation_id == request.generation_id
        }
    };
    let claim = &request.claim;
    if !payload_matches
        || claim.pg_id != request.object.pg_id.get()
        || claim.cluster_epoch != request.object.cluster_epoch
        || claim.bucket != request.object.bucket
        || claim.key != request.object.key
        || claim.generation_id != request.generation_id
        || claim.reclaim_kind != request.payload.kind()
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "object payload reclaim command build request identity mismatch",
        ));
    }
    Ok(())
}

pub(crate) fn encode_object_payload_reclaim_command_build_request(
    request: &StorageRpcObjectPayloadReclaimCommandBuildRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_object_payload_reclaim_command_build_request(request)?;
    let mut out = encode_object_request(&request.object);
    put_u64(&mut out, request.generation_id.get());
    put_object_payload_reclaim(&mut out, &request.payload);
    put_object_payload_reclaim_claim_record(&mut out, &request.claim);
    put_admitted_route_effect_deadline(&mut out, request.effect_deadline);
    Ok(out)
}

pub(crate) fn decode_object_payload_reclaim_command_build_request(
    bytes: &[u8],
) -> Result<StorageRpcObjectPayloadReclaimCommandBuildRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let generation_id = decoder.read_generation_id()?;
    let payload = decoder.read_object_payload_reclaim()?;
    let claim = decoder.read_object_payload_reclaim_claim_record()?;
    let effect_deadline =
        decoder.read_admitted_route_effect_deadline("object payload reclaim command build")?;
    decoder.finish()?;
    let request = StorageRpcObjectPayloadReclaimCommandBuildRequest {
        object,
        generation_id,
        payload,
        claim,
        effect_deadline,
    };
    validate_object_payload_reclaim_command_build_request(&request)?;
    Ok(request)
}

pub(crate) fn encode_abort_multipart_cleanup_request(
    request: &StorageRpcAbortMultipartCleanupRequest,
) -> Vec<u8> {
    let mut out = encode_object_request(&request.object);
    put_string(&mut out, request.upload_id.as_str());
    out
}

pub(crate) fn decode_abort_multipart_cleanup_request(
    bytes: &[u8],
) -> Result<StorageRpcAbortMultipartCleanupRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let upload_id = decoder.read_upload_id()?;
    decoder.finish()?;
    Ok(StorageRpcAbortMultipartCleanupRequest { object, upload_id })
}

pub(crate) fn encode_abort_multipart_cleanup_response(
    response: &StorageRpcAbortMultipartCleanupResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_optional_abort_multipart_upload_cleanup(&mut out, response.cleanup.as_ref());
    out
}

pub(crate) fn decode_abort_multipart_cleanup_response(
    bytes: &[u8],
) -> Result<StorageRpcAbortMultipartCleanupResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let cleanup = decoder.read_optional_abort_multipart_upload_cleanup()?;
    decoder.finish()?;
    Ok(StorageRpcAbortMultipartCleanupResponse { cleanup })
}

pub(crate) fn encode_authorized_abort_multipart_command_build_request(
    request: &StorageRpcAuthorizedAbortMultipartCommandBuildRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.authorized_upload.bucket != request.object.bucket
        || request.authorized_upload.key != request.object.key
        || request.bucket_write_reservation.bucket != request.object.bucket
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "authorized abort multipart request identity mismatch",
        ));
    }
    let mut out = encode_object_request(&request.object);
    put_multipart_upload_record(&mut out, &request.authorized_upload);
    put_optional_abort_multipart_upload_cleanup(&mut out, request.expected_cleanup.as_ref());
    put_bucket_write_reservation_proof(&mut out, &request.bucket_write_reservation);
    put_admitted_route_effect_deadline(&mut out, request.effect_deadline);
    Ok(out)
}

pub(crate) fn decode_authorized_abort_multipart_command_build_request(
    bytes: &[u8],
) -> Result<StorageRpcAuthorizedAbortMultipartCommandBuildRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let authorized_upload = decoder.read_multipart_upload_record()?;
    let expected_cleanup = decoder.read_optional_abort_multipart_upload_cleanup()?;
    let bucket_write_reservation = decoder.read_bucket_write_reservation_proof()?;
    let effect_deadline =
        decoder.read_admitted_route_effect_deadline("authorized abort multipart command build")?;
    decoder.finish()?;
    if authorized_upload.bucket != object.bucket
        || authorized_upload.key != object.key
        || bucket_write_reservation.bucket != object.bucket
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "authorized abort multipart request identity mismatch",
        ));
    }
    if let Some(cleanup) = expected_cleanup.as_ref() {
        if cleanup.upload != authorized_upload {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "authorized abort multipart request identity mismatch",
            ));
        }
        validate_abort_multipart_cleanup_identity(
            cleanup,
            &object,
            &authorized_upload.upload_id,
            "authorized abort multipart request identity mismatch",
        )?;
    }
    Ok(StorageRpcAuthorizedAbortMultipartCommandBuildRequest {
        object,
        authorized_upload,
        expected_cleanup,
        bucket_write_reservation,
        effect_deadline,
    })
}

pub(crate) fn encode_direct_put_command_build_request(
    request: &StorageRpcDirectPutCommandBuildRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.object.bucket != request.request.bucket || request.object.key != request.request.key
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "object route must match direct PUT request",
        ));
    }
    if request.request.bucket_write_reservation != request.bucket_write_reservation
        || request.bucket_write_reservation.bucket != request.object.bucket
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "bucket write reservation proof must match direct PUT bucket",
        ));
    }
    let mut out = encode_object_request(&request.object);
    put_commit_direct_put_object_req(&mut out, &request.request);
    put_u64(&mut out, request.version_id.to_u64());
    put_direct_put_commit_storage_snapshot(&mut out, &request.expected_snapshot);
    put_bucket_write_reservation_proof(&mut out, &request.bucket_write_reservation);
    Ok(out)
}

pub(crate) fn decode_direct_put_command_build_request(
    bytes: &[u8],
) -> Result<StorageRpcDirectPutCommandBuildRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let object = decoder.read_rpc_object_request()?;
    let request = decoder.read_commit_direct_put_object_req()?;
    let version_id = VersionId::from_u64(decoder.read_u64()?);
    let expected_snapshot = decoder.read_direct_put_commit_storage_snapshot()?;
    let bucket_write_reservation = decoder.read_bucket_write_reservation_proof()?;
    decoder.finish()?;
    if object.bucket != request.bucket || object.key != request.key {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "object route must match direct PUT request",
        ));
    }
    if request.bucket_write_reservation != bucket_write_reservation
        || bucket_write_reservation.bucket != object.bucket
    {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "bucket write reservation proof must match direct PUT bucket",
        ));
    }
    Ok(StorageRpcDirectPutCommandBuildRequest {
        object,
        request,
        version_id,
        expected_snapshot,
        bucket_write_reservation,
    })
}

pub(crate) fn encode_object_generation_response(
    response: &StorageRpcObjectGenerationResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_u64(&mut out, response.generation_id.get());
    out
}

pub(crate) fn decode_object_generation_response(
    bytes: &[u8],
) -> Result<StorageRpcObjectGenerationResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let generation_id = decoder.read_generation_id()?;
    decoder.finish()?;
    Ok(StorageRpcObjectGenerationResponse { generation_id })
}

pub(crate) fn encode_object_version_response(
    response: &StorageRpcObjectVersionResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_u64(&mut out, response.version_id.to_u64());
    out
}

pub(crate) fn decode_object_version_response(
    bytes: &[u8],
) -> Result<StorageRpcObjectVersionResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let raw = decoder.read_u64()?;
    decoder.finish()?;
    let version_id = VersionId::from_u64(raw);
    if version_id.is_null() {
        return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
            "object version response must not contain null version",
        ));
    }
    Ok(StorageRpcObjectVersionResponse { version_id })
}

pub(crate) fn encode_object_read_auth_subject_response(
    response: &StorageRpcObjectReadAuthSubjectResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcObjectReadAuthSubjectOutcome::Loaded(subject) => {
            put_u8(&mut out, 0);
            put_object_read_auth_subject(&mut out, subject);
        }
        StorageRpcObjectReadAuthSubjectOutcome::ObjectNotFound => put_u8(&mut out, 1),
    }
    out
}

pub(crate) fn decode_object_read_auth_subject_response(
    bytes: &[u8],
) -> Result<StorageRpcObjectReadAuthSubjectResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => StorageRpcObjectReadAuthSubjectOutcome::Loaded(Box::new(
            decoder.read_object_read_auth_subject()?,
        )),
        1 => StorageRpcObjectReadAuthSubjectOutcome::ObjectNotFound,
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "invalid object read auth subject outcome tag",
            ))
        }
    };
    decoder.finish()?;
    Ok(StorageRpcObjectReadAuthSubjectResponse { outcome })
}

pub(crate) fn encode_object_read_snapshot_response(
    response: &StorageRpcObjectReadSnapshotResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcObjectReadSnapshotOutcome::Loaded(snapshot) => {
            put_u8(&mut out, 0);
            put_object_read_snapshot(&mut out, snapshot);
        }
        StorageRpcObjectReadSnapshotOutcome::StaleSubject => put_u8(&mut out, 1),
    }
    out
}

pub(crate) fn decode_object_read_snapshot_response(
    bytes: &[u8],
) -> Result<StorageRpcObjectReadSnapshotResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => StorageRpcObjectReadSnapshotOutcome::Loaded(Box::new(
            decoder.read_object_read_snapshot()?,
        )),
        1 => StorageRpcObjectReadSnapshotOutcome::StaleSubject,
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "invalid object read snapshot outcome tag",
            ))
        }
    };
    decoder.finish()?;
    Ok(StorageRpcObjectReadSnapshotResponse { outcome })
}

pub(crate) fn encode_put_object_metadata_snapshot_response(
    response: &StorageRpcPutObjectMetadataSnapshotResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcPutObjectMetadataSnapshotOutcome::Loaded(stored) => {
            put_u8(&mut out, 0);
            put_stored_object(&mut out, stored);
        }
        StorageRpcPutObjectMetadataSnapshotOutcome::ObjectNotFound => put_u8(&mut out, 1),
    }
    out
}

pub(crate) fn decode_put_object_metadata_snapshot_response(
    bytes: &[u8],
) -> Result<StorageRpcPutObjectMetadataSnapshotResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => StorageRpcPutObjectMetadataSnapshotOutcome::Loaded(Box::new(
            decoder.read_stored_object()?,
        )),
        1 => StorageRpcPutObjectMetadataSnapshotOutcome::ObjectNotFound,
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "invalid object metadata PUT snapshot outcome tag",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcPutObjectMetadataSnapshotResponse { outcome })
}

pub(crate) fn encode_object_delete_snapshot_response(
    response: &StorageRpcObjectDeleteSnapshotResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_optional_stored_object(&mut out, response.stored.as_ref());
    put_optional_delete_object_version_target(&mut out, response.target.as_ref());
    out
}

pub(crate) fn decode_object_delete_snapshot_response(
    bytes: &[u8],
) -> Result<StorageRpcObjectDeleteSnapshotResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let stored = decoder.read_optional_stored_object()?;
    let target = decoder.read_optional_delete_object_version_target()?;
    decoder.finish()?;
    Ok(StorageRpcObjectDeleteSnapshotResponse { stored, target })
}

pub(crate) fn encode_object_lifecycle_version_list_response(
    response: &StorageRpcObjectLifecycleVersionListResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_stored_object_list(&mut out, &response.versions);
    out
}

pub(crate) fn decode_object_lifecycle_version_list_response(
    bytes: &[u8],
) -> Result<StorageRpcObjectLifecycleVersionListResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let versions = decoder.read_stored_object_list()?;
    decoder.finish()?;
    Ok(StorageRpcObjectLifecycleVersionListResponse { versions })
}

pub(crate) fn encode_object_metadata_command_build_response(
    response: &StorageRpcObjectMetadataCommandBuildResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcObjectMetadataCommandBuildOutcome::Command(command) => {
            put_u8(&mut out, 0);
            put_metadata_command_envelope_response_item(&mut out, command);
        }
        StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot => put_u8(&mut out, 1),
        StorageRpcObjectMetadataCommandBuildOutcome::Missing => put_u8(&mut out, 2),
        StorageRpcObjectMetadataCommandBuildOutcome::LogConflict {
            node_id,
            pg_id,
            cluster_epoch,
            log_index,
        } => {
            put_u8(&mut out, 3);
            put_u32(&mut out, *node_id);
            put_u32(&mut out, *pg_id);
            put_u64(&mut out, cluster_epoch.get());
            put_u64(&mut out, *log_index);
        }
    }
    out
}

pub(crate) fn decode_object_metadata_command_build_response(
    bytes: &[u8],
    authority: &MetadataCommandDecodeAuthority,
) -> Result<StorageRpcObjectMetadataCommandBuildResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => StorageRpcObjectMetadataCommandBuildOutcome::Command(Box::new(
            decoder.read_metadata_command_envelope_response_item(authority)?,
        )),
        1 => StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot,
        2 => StorageRpcObjectMetadataCommandBuildOutcome::Missing,
        3 => StorageRpcObjectMetadataCommandBuildOutcome::LogConflict {
            node_id: decoder.read_u32()?,
            pg_id: decoder.read_u32()?,
            cluster_epoch: decoder.read_cluster_epoch()?,
            log_index: decoder.read_u64()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "invalid object metadata command build outcome tag",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcObjectMetadataCommandBuildResponse { outcome })
}

pub(crate) fn encode_object_generation_reservation_response(
    response: &StorageRpcObjectGenerationReservationResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcObjectGenerationReservationOutcome::Found(generation_id) => {
            put_u8(&mut out, 0);
            put_u64(&mut out, generation_id.get());
        }
        StorageRpcObjectGenerationReservationOutcome::NotFound { reservation_id } => {
            put_u8(&mut out, 1);
            put_string(&mut out, reservation_id.as_str());
        }
    }
    out
}

pub(crate) fn decode_object_generation_reservation_response(
    bytes: &[u8],
) -> Result<StorageRpcObjectGenerationReservationResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => StorageRpcObjectGenerationReservationOutcome::Found(decoder.read_generation_id()?),
        1 => StorageRpcObjectGenerationReservationOutcome::NotFound {
            reservation_id: decoder.read_session_id()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "unknown object generation reservation response tag",
            ))
        }
    };
    decoder.finish()?;
    Ok(StorageRpcObjectGenerationReservationResponse { outcome })
}

pub(crate) fn encode_direct_put_commit_snapshot_response(
    response: &StorageRpcDirectPutCommitSnapshotResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_direct_put_commit_storage_snapshot(&mut out, &response.snapshot);
    out
}

pub(crate) fn decode_direct_put_commit_snapshot_response(
    bytes: &[u8],
) -> Result<StorageRpcDirectPutCommitSnapshotResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let snapshot = decoder.read_direct_put_commit_storage_snapshot()?;
    decoder.finish()?;
    Ok(StorageRpcDirectPutCommitSnapshotResponse { snapshot })
}

pub(crate) fn encode_direct_put_command_build_response(
    response: &StorageRpcDirectPutCommandBuildResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcDirectPutCommandBuildOutcome::Command(command) => {
            put_u8(&mut out, 0);
            put_metadata_command_envelope_response_item(&mut out, command);
        }
        StorageRpcDirectPutCommandBuildOutcome::StaleSnapshot => put_u8(&mut out, 1),
        StorageRpcDirectPutCommandBuildOutcome::LogConflict {
            node_id,
            pg_id,
            cluster_epoch,
            log_index,
        } => {
            put_u8(&mut out, 2);
            put_u32(&mut out, *node_id);
            put_u32(&mut out, *pg_id);
            put_u64(&mut out, cluster_epoch.get());
            put_u64(&mut out, *log_index);
        }
    }
    out
}

pub(crate) fn decode_direct_put_command_build_response(
    bytes: &[u8],
    authority: &MetadataCommandDecodeAuthority,
) -> Result<StorageRpcDirectPutCommandBuildResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => StorageRpcDirectPutCommandBuildOutcome::Command(Box::new(
            decoder.read_metadata_command_envelope_response_item(authority)?,
        )),
        1 => StorageRpcDirectPutCommandBuildOutcome::StaleSnapshot,
        2 => StorageRpcDirectPutCommandBuildOutcome::LogConflict {
            node_id: decoder.read_u32()?,
            pg_id: decoder.read_u32()?,
            cluster_epoch: decoder.read_cluster_epoch()?,
            log_index: decoder.read_u64()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "invalid direct PUT command build outcome tag",
            ))
        }
    };
    decoder.finish()?;
    Ok(StorageRpcDirectPutCommandBuildResponse { outcome })
}

pub(crate) fn encode_create_bucket_command_build_request(
    request: &StorageRpcCreateBucketCommandBuildRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.bucket != request.config.name {
        return Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
            "request bucket must match create-bucket config name",
        ));
    }
    if request.command_id.cluster_epoch() != request.cluster_epoch
        || request.command_id.pg_id() != request.pg_id
    {
        return Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
            "command id route must match request route",
        ));
    }
    let mut out = encode_bucket_request(&StorageRpcBucketRequest {
        node_id: request.node_id,
        cluster_epoch: request.cluster_epoch,
        pg_id: request.pg_id,
        bucket: request.bucket.clone(),
    });
    put_u64(&mut out, request.command_id.log_index().get());
    put_create_bucket_config(&mut out, &request.config);
    Ok(out)
}

pub(crate) fn decode_create_bucket_command_build_request(
    bytes: &[u8],
) -> Result<StorageRpcCreateBucketCommandBuildRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let bucket = decoder.read_bucket_name()?;
    let log_index = crate::metadata_command::MetadataCommandLogIndex::new(decoder.read_u64()?)
        .ok_or(StorageRpcPayloadError::InvalidBucketMetadataRequest(
            "metadata command log index must not be zero",
        ))?;
    let config = decoder.read_create_bucket_config()?;
    decoder.finish()?;
    if bucket != config.name {
        return Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
            "request bucket must match create-bucket config name",
        ));
    }
    Ok(StorageRpcCreateBucketCommandBuildRequest {
        node_id,
        cluster_epoch,
        pg_id,
        bucket,
        command_id: crate::metadata_command::MetadataCommandId::new(
            cluster_epoch,
            pg_id,
            log_index,
        ),
        config,
    })
}

pub(crate) fn encode_multipart_completion_barrier_command_build_request(
    request: &StorageRpcMultipartCompletionBarrierCommandBuildRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.command_id.cluster_epoch() != request.cluster_epoch
        || request.command_id.pg_id() != request.pg_id
    {
        return Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
            "command id route must match request route",
        ));
    }
    if request.bucket_write_reservation.bucket != request.bucket
        || request.bucket_write_reservation.cluster_epoch != request.cluster_epoch
        || request.bucket_write_reservation.operation_kind
            != COMPLETE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND
        || request.bucket_write_reservation.target_context.as_deref()
            != Some(request.completion_target_context.as_str())
    {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "bucket write reservation proof must match multipart completion barrier request",
        ));
    }
    if request.completion_target_context.len() > STORAGE_RPC_MAX_BUCKET_WRITE_TARGET_CONTEXT_LEN {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "multipart completion barrier target context exceeds maximum length",
        ));
    }
    let mut out = encode_bucket_request(&StorageRpcBucketRequest {
        node_id: request.node_id,
        cluster_epoch: request.cluster_epoch,
        pg_id: request.pg_id,
        bucket: request.bucket.clone(),
    });
    put_u64(&mut out, request.command_id.log_index().get());
    put_string(&mut out, &request.completion_target_context);
    put_bucket_write_reservation_proof(&mut out, &request.bucket_write_reservation);
    Ok(out)
}

pub(crate) fn decode_multipart_completion_barrier_command_build_request(
    bytes: &[u8],
) -> Result<StorageRpcMultipartCompletionBarrierCommandBuildRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let bucket = decoder.read_bucket_name()?;
    let log_index = crate::metadata_command::MetadataCommandLogIndex::new(decoder.read_u64()?)
        .ok_or(StorageRpcPayloadError::InvalidBucketMetadataRequest(
            "metadata command log index must not be zero",
        ))?;
    let completion_target_context = decoder.read_string_with_limit(
        STORAGE_RPC_MAX_BUCKET_WRITE_TARGET_CONTEXT_LEN,
        StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "multipart completion barrier target context exceeds maximum length",
        ),
    )?;
    let bucket_write_reservation = decoder.read_bucket_write_reservation_proof()?;
    decoder.finish()?;
    if bucket_write_reservation.bucket != bucket
        || bucket_write_reservation.cluster_epoch != cluster_epoch
        || bucket_write_reservation.operation_kind
            != COMPLETE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND
        || bucket_write_reservation.target_context.as_deref()
            != Some(completion_target_context.as_str())
    {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "bucket write reservation proof must match multipart completion barrier request",
        ));
    }
    Ok(StorageRpcMultipartCompletionBarrierCommandBuildRequest {
        node_id,
        cluster_epoch,
        pg_id,
        bucket,
        command_id: crate::metadata_command::MetadataCommandId::new(
            cluster_epoch,
            pg_id,
            log_index,
        ),
        completion_target_context,
        bucket_write_reservation,
    })
}

pub(crate) fn encode_bucket_metadata_control_pending_match_request(
    request: &StorageRpcBucketMetadataControlPendingMatchRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.command.id().cluster_epoch() != request.bucket.cluster_epoch
        || request.command.id().pg_id() != request.bucket.pg_id
    {
        return Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
            "pending command route must match request route",
        ));
    }
    let mut out = encode_bucket_request(&request.bucket);
    put_bytes(&mut out, &request.command.command_bytes());
    put_bucket_metadata_control_mutation(&mut out, &request.mutation);
    Ok(out)
}

pub(crate) fn decode_bucket_metadata_control_pending_match_request(
    bytes: &[u8],
    authority: &MetadataCommandDecodeAuthority,
) -> Result<StorageRpcBucketMetadataControlPendingMatchRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let bucket = decoder.read_bucket_request()?;
    let command = decoder.read_metadata_command_envelope_bytes(authority)?;
    let mutation = decoder.read_bucket_metadata_control_mutation()?;
    decoder.finish()?;
    if command.id().cluster_epoch() != bucket.cluster_epoch || command.id().pg_id() != bucket.pg_id
    {
        return Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
            "pending command route must match request route",
        ));
    }
    Ok(StorageRpcBucketMetadataControlPendingMatchRequest {
        bucket,
        command,
        mutation,
    })
}

pub(crate) fn encode_bucket_metadata_control_command_build_request(
    request: &StorageRpcBucketMetadataControlCommandBuildRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.command_id.cluster_epoch() != request.bucket.cluster_epoch
        || request.command_id.pg_id() != request.bucket.pg_id
    {
        return Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
            "command id route must match request route",
        ));
    }
    let mut out = encode_bucket_request(&request.bucket);
    put_u64(&mut out, request.command_id.log_index().get());
    put_bucket_metadata_control_mutation(&mut out, &request.mutation);
    Ok(out)
}

pub(crate) fn decode_bucket_metadata_control_command_build_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketMetadataControlCommandBuildRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let bucket = decoder.read_bucket_request()?;
    let log_index = crate::metadata_command::MetadataCommandLogIndex::new(decoder.read_u64()?)
        .ok_or(StorageRpcPayloadError::InvalidBucketMetadataRequest(
            "metadata command log index must not be zero",
        ))?;
    let mutation = decoder.read_bucket_metadata_control_mutation()?;
    decoder.finish()?;
    Ok(StorageRpcBucketMetadataControlCommandBuildRequest {
        command_id: crate::metadata_command::MetadataCommandId::new(
            bucket.cluster_epoch,
            bucket.pg_id,
            log_index,
        ),
        bucket,
        mutation,
    })
}

pub(crate) fn encode_bucket_mark_deleting_command_build_request(
    request: &StorageRpcBucketMarkDeletingCommandBuildRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.command_id.cluster_epoch() != request.bucket.cluster_epoch
        || request.command_id.pg_id() != request.bucket.pg_id
    {
        return Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
            "command id route must match request route",
        ));
    }
    let mut out = encode_bucket_request(&request.bucket);
    put_u64(&mut out, request.command_id.log_index().get());
    Ok(out)
}

pub(crate) fn decode_bucket_mark_deleting_command_build_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketMarkDeletingCommandBuildRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let bucket = decoder.read_bucket_request()?;
    let log_index = crate::metadata_command::MetadataCommandLogIndex::new(decoder.read_u64()?)
        .ok_or(StorageRpcPayloadError::InvalidBucketMetadataRequest(
            "metadata command log index must not be zero",
        ))?;
    decoder.finish()?;
    Ok(StorageRpcBucketMarkDeletingCommandBuildRequest {
        command_id: crate::metadata_command::MetadataCommandId::new(
            bucket.cluster_epoch,
            bucket.pg_id,
            log_index,
        ),
        bucket,
    })
}

pub(crate) fn encode_bucket_subresource_get_request(
    request: &StorageRpcBucketSubresourceGetRequest,
) -> Vec<u8> {
    let mut out = encode_bucket_request(&request.bucket);
    put_bucket_subresource_kind(&mut out, request.kind);
    out
}

pub(crate) fn decode_bucket_subresource_get_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketSubresourceGetRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let bucket = decoder.read_bucket_request()?;
    let kind = decoder.read_bucket_subresource_kind()?;
    decoder.finish()?;
    Ok(StorageRpcBucketSubresourceGetRequest { bucket, kind })
}

pub(crate) fn encode_bucket_info_outcome_response(
    response: &StorageRpcBucketInfoOutcomeResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcBucketInfoOutcome::Info(info) => {
            put_u8(&mut out, 0);
            put_bucket_info(&mut out, info);
        }
        StorageRpcBucketInfoOutcome::BucketNotFound { name } => {
            put_u8(&mut out, 1);
            put_string(&mut out, name.as_str());
        }
    }
    out
}

pub(crate) fn decode_bucket_info_outcome_response(
    bytes: &[u8],
) -> Result<StorageRpcBucketInfoOutcomeResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => StorageRpcBucketInfoOutcome::Info(decoder.read_bucket_info()?),
        1 => StorageRpcBucketInfoOutcome::BucketNotFound {
            name: decoder.read_bucket_name()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "invalid bucket info outcome tag",
            ))
        }
    };
    decoder.finish()?;
    Ok(StorageRpcBucketInfoOutcomeResponse { outcome })
}

pub(crate) fn encode_bucket_snapshot_response(
    response: &StorageRpcBucketSnapshotResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcBucketSnapshotOutcome::Loaded(snapshot) => {
            put_u8(&mut out, 0);
            put_bucket_snapshot(&mut out, snapshot);
        }
        StorageRpcBucketSnapshotOutcome::BucketNotFound { name } => {
            put_u8(&mut out, 1);
            put_string(&mut out, name.as_str());
        }
    }
    out
}

pub(crate) fn decode_bucket_snapshot_response(
    bytes: &[u8],
) -> Result<StorageRpcBucketSnapshotResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => StorageRpcBucketSnapshotOutcome::Loaded(Box::new(decoder.read_bucket_snapshot()?)),
        1 => StorageRpcBucketSnapshotOutcome::BucketNotFound {
            name: decoder.read_bucket_name()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "invalid bucket snapshot outcome tag",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcBucketSnapshotResponse { outcome })
}

pub(crate) fn encode_create_bucket_command_build_response(
    response: &StorageRpcCreateBucketCommandBuildResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcCreateBucketCommandBuildOutcome::Exists(info) => {
            put_u8(&mut out, 0);
            put_bucket_info(&mut out, info);
        }
        StorageRpcCreateBucketCommandBuildOutcome::Command(command) => {
            put_u8(&mut out, 1);
            put_bytes(&mut out, &command.command_bytes());
        }
    }
    out
}

pub(crate) fn decode_create_bucket_command_build_response(
    bytes: &[u8],
    authority: &MetadataCommandDecodeAuthority,
) -> Result<StorageRpcCreateBucketCommandBuildResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => StorageRpcCreateBucketCommandBuildOutcome::Exists(decoder.read_bucket_info()?),
        1 => {
            let command = decoder.read_metadata_command_envelope_bytes(authority)?;
            StorageRpcCreateBucketCommandBuildOutcome::Command(Box::new(command))
        }
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "invalid create-bucket command build outcome tag",
            ))
        }
    };
    decoder.finish()?;
    Ok(StorageRpcCreateBucketCommandBuildResponse { outcome })
}

pub(crate) fn encode_multipart_completion_barrier_command_build_response(
    response: &StorageRpcMultipartCompletionBarrierCommandBuildResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_u64(&mut out, response.barrier_sequence);
    put_bytes(&mut out, &response.command.command_bytes());
    out
}

pub(crate) fn decode_multipart_completion_barrier_command_build_response(
    bytes: &[u8],
    authority: &MetadataCommandDecodeAuthority,
) -> Result<StorageRpcMultipartCompletionBarrierCommandBuildResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let barrier_sequence = decoder.read_u64()?;
    let command = decoder.read_metadata_command_envelope_bytes(authority)?;
    decoder.finish()?;
    Ok(StorageRpcMultipartCompletionBarrierCommandBuildResponse {
        barrier_sequence,
        command,
    })
}

pub(crate) fn encode_bucket_metadata_control_command_build_response(
    response: &StorageRpcBucketMetadataControlCommandBuildResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_bytes(&mut out, &response.command.command_bytes());
    out
}

pub(crate) fn decode_bucket_metadata_control_command_build_response(
    bytes: &[u8],
    authority: &MetadataCommandDecodeAuthority,
) -> Result<StorageRpcBucketMetadataControlCommandBuildResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let command = decoder.read_metadata_command_envelope_bytes(authority)?;
    decoder.finish()?;
    Ok(StorageRpcBucketMetadataControlCommandBuildResponse { command })
}

pub(crate) fn encode_bucket_mark_deleting_command_build_response(
    response: &StorageRpcBucketMarkDeletingCommandBuildResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcBucketMarkDeletingCommandBuildOutcome::AlreadyDeleting(info) => {
            put_u8(&mut out, 0);
            put_bucket_info(&mut out, info);
        }
        StorageRpcBucketMarkDeletingCommandBuildOutcome::Command(command) => {
            put_u8(&mut out, 1);
            put_bytes(&mut out, &command.command_bytes());
        }
    }
    out
}

pub(crate) fn decode_bucket_mark_deleting_command_build_response(
    bytes: &[u8],
    authority: &MetadataCommandDecodeAuthority,
) -> Result<StorageRpcBucketMarkDeletingCommandBuildResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => StorageRpcBucketMarkDeletingCommandBuildOutcome::AlreadyDeleting(
            decoder.read_bucket_info()?,
        ),
        1 => {
            let command = decoder.read_metadata_command_envelope_bytes(authority)?;
            StorageRpcBucketMarkDeletingCommandBuildOutcome::Command(Box::new(command))
        }
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "invalid bucket mark-deleting command build outcome tag",
            ))
        }
    };
    decoder.finish()?;
    Ok(StorageRpcBucketMarkDeletingCommandBuildResponse { outcome })
}

pub(crate) fn encode_bucket_subresource_get_response(
    response: &StorageRpcBucketSubresourceGetResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_optional_string(&mut out, response.body.as_deref());
    out
}

pub(crate) fn decode_bucket_subresource_get_response(
    bytes: &[u8],
) -> Result<StorageRpcBucketSubresourceGetResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let body = decoder.read_optional_string()?;
    decoder.finish()?;
    Ok(StorageRpcBucketSubresourceGetResponse { body })
}

pub(crate) fn encode_lifecycle_sweep_roots_request(
    request: &StorageRpcLifecycleSweepRootsRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.limit > STORAGE_RPC_MAX_LIFECYCLE_SWEEP_ROOTS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: request.limit,
            limit: STORAGE_RPC_MAX_LIFECYCLE_SWEEP_ROOTS,
        });
    }
    let mut out = encode_bucket_pg_request(&StorageRpcBucketPgRequest {
        node_id: request.node_id,
        cluster_epoch: request.cluster_epoch,
        pg_id: request.pg_id,
    })?;
    put_u64(&mut out, request.now);
    put_u64(
        &mut out,
        u64::try_from(request.limit).map_err(|_| StorageRpcPayloadError::PayloadTooLarge {
            len: request.limit,
            limit: u64::MAX as usize,
        })?,
    );
    Ok(out)
}

pub(crate) fn decode_lifecycle_sweep_roots_request(
    bytes: &[u8],
) -> Result<StorageRpcLifecycleSweepRootsRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let now = decoder.read_u64()?;
    let limit = usize::try_from(decoder.read_u64()?).map_err(|_| {
        StorageRpcPayloadError::PayloadTooLarge {
            len: usize::MAX,
            limit: usize::MAX,
        }
    })?;
    decoder.finish()?;
    if limit > STORAGE_RPC_MAX_LIFECYCLE_SWEEP_ROOTS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: limit,
            limit: STORAGE_RPC_MAX_LIFECYCLE_SWEEP_ROOTS,
        });
    }
    Ok(StorageRpcLifecycleSweepRootsRequest {
        node_id: route.node_id,
        cluster_epoch: route.cluster_epoch,
        pg_id: route.pg_id,
        now,
        limit,
    })
}

pub(crate) fn encode_lifecycle_sweep_roots_response(
    response: &StorageRpcLifecycleSweepRootsResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if response.roots.len() > STORAGE_RPC_MAX_LIFECYCLE_SWEEP_ROOTS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: response.roots.len(),
            limit: STORAGE_RPC_MAX_LIFECYCLE_SWEEP_ROOTS,
        });
    }
    let mut out = Vec::new();
    put_u32(
        &mut out,
        u32::try_from(response.roots.len()).map_err(|_| {
            StorageRpcPayloadError::PayloadTooLarge {
                len: response.roots.len(),
                limit: u32::MAX as usize,
            }
        })?,
    );
    for root in &response.roots {
        put_lifecycle_sweep_root(&mut out, root);
    }
    Ok(out)
}

pub(crate) fn decode_lifecycle_sweep_roots_response(
    bytes: &[u8],
) -> Result<StorageRpcLifecycleSweepRootsResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let count = decoder
        .read_bounded_remaining_count(4 + 8 + 1, "lifecycle sweep root count exceeds payload")?;
    if count > STORAGE_RPC_MAX_LIFECYCLE_SWEEP_ROOTS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: count,
            limit: STORAGE_RPC_MAX_LIFECYCLE_SWEEP_ROOTS,
        });
    }
    let mut roots = Vec::new();
    for _ in 0..count {
        roots.push(decoder.read_lifecycle_sweep_root()?);
    }
    decoder.finish()?;
    Ok(StorageRpcLifecycleSweepRootsResponse { roots })
}

fn validate_cleanup_list_limit(value: u32) -> Result<(), StorageRpcPayloadError> {
    if value == 0 {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "cleanup list limit must be nonzero",
        ));
    }
    if value > STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: value as usize,
            limit: STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS as usize,
        });
    }
    Ok(())
}

fn validate_list_page_item_limit(value: u32) -> Result<(), StorageRpcPayloadError> {
    if value > STORAGE_RPC_MAX_LIST_PAGE_ITEMS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: value as usize,
            limit: STORAGE_RPC_MAX_LIST_PAGE_ITEMS as usize,
        });
    }
    Ok(())
}

fn validate_list_page_item_count(value: usize) -> Result<(), StorageRpcPayloadError> {
    if value > STORAGE_RPC_MAX_LIST_PAGE_ITEMS as usize {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: value,
            limit: STORAGE_RPC_MAX_LIST_PAGE_ITEMS as usize,
        });
    }
    Ok(())
}

fn validate_bucket_metadata_item_count(value: usize) -> Result<(), StorageRpcPayloadError> {
    if value > STORAGE_RPC_MAX_BUCKET_BATCH_ITEMS as usize {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: value,
            limit: STORAGE_RPC_MAX_BUCKET_BATCH_ITEMS as usize,
        });
    }
    Ok(())
}

pub(crate) fn encode_list_objects_request(
    request: &StorageRpcListObjectsRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_list_page_item_limit(request.request.max_keys)?;
    let mut out = encode_bucket_pg_request(&StorageRpcBucketPgRequest {
        node_id: request.node_id,
        cluster_epoch: request.cluster_epoch,
        pg_id: request.pg_id,
    })?;
    put_string(&mut out, request.request.bucket.as_str());
    put_optional_string(
        &mut out,
        request.request.prefix.as_ref().map(|key| key.as_str()),
    );
    put_optional_string(
        &mut out,
        request.request.start_after.as_ref().map(|key| key.as_str()),
    );
    put_optional_string(
        &mut out,
        request.request.start_at.as_ref().map(|key| key.as_str()),
    );
    put_u32(&mut out, request.request.max_keys);
    Ok(out)
}

pub(crate) fn decode_list_objects_request(
    bytes: &[u8],
) -> Result<StorageRpcListObjectsRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let request = ListObjectsReq {
        bucket: decoder.read_bucket_name()?,
        prefix: decoder.read_optional_object_key()?,
        start_after: decoder.read_optional_object_key()?,
        start_at: decoder.read_optional_object_key()?,
        max_keys: decoder.read_u32()?,
    };
    decoder.finish()?;
    validate_list_page_item_limit(request.max_keys)?;
    Ok(StorageRpcListObjectsRequest {
        node_id: route.node_id,
        cluster_epoch: route.cluster_epoch,
        pg_id: route.pg_id,
        request,
    })
}

pub(crate) fn encode_list_objects_response(
    response: &StorageRpcListObjectsResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_list_page_item_count(response.response.objects.len())?;
    let mut out = Vec::new();
    put_stored_object_list(&mut out, &response.response.objects);
    put_bool(&mut out, response.response.is_truncated);
    put_optional_string(
        &mut out,
        response
            .response
            .next_start_after
            .as_ref()
            .map(|key| key.as_str()),
    );
    Ok(out)
}

pub(crate) fn decode_list_objects_response(
    bytes: &[u8],
) -> Result<StorageRpcListObjectsResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let objects = decoder.read_stored_object_list_with_limit(STORAGE_RPC_MAX_LIST_PAGE_ITEMS)?;
    let is_truncated = decoder.read_bool()?;
    let next_start_after = decoder.read_optional_object_key()?;
    decoder.finish()?;
    Ok(StorageRpcListObjectsResponse {
        response: ListObjectsResp {
            objects,
            is_truncated,
            next_start_after,
        },
    })
}

pub(crate) fn encode_list_object_versions_request(
    request: &StorageRpcListObjectVersionsRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_list_page_item_limit(request.request.max_keys)?;
    let mut out = encode_bucket_pg_request(&StorageRpcBucketPgRequest {
        node_id: request.node_id,
        cluster_epoch: request.cluster_epoch,
        pg_id: request.pg_id,
    })?;
    put_string(&mut out, request.request.bucket.as_str());
    put_optional_string(
        &mut out,
        request.request.prefix.as_ref().map(|key| key.as_str()),
    );
    put_optional_string(
        &mut out,
        request.request.key_marker.as_ref().map(|key| key.as_str()),
    );
    put_optional_version_id(&mut out, request.request.version_id_marker);
    put_optional_string(
        &mut out,
        request.request.start_at.as_ref().map(|key| key.as_str()),
    );
    put_u32(&mut out, request.request.max_keys);
    Ok(out)
}

pub(crate) fn decode_list_object_versions_request(
    bytes: &[u8],
) -> Result<StorageRpcListObjectVersionsRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let request = ListObjectVersionsReq {
        bucket: decoder.read_bucket_name()?,
        prefix: decoder.read_optional_object_key()?,
        key_marker: decoder.read_optional_object_key()?,
        version_id_marker: decoder.read_optional_version_id()?,
        start_at: decoder.read_optional_object_key()?,
        max_keys: decoder.read_u32()?,
    };
    decoder.finish()?;
    validate_list_page_item_limit(request.max_keys)?;
    Ok(StorageRpcListObjectVersionsRequest {
        node_id: route.node_id,
        cluster_epoch: route.cluster_epoch,
        pg_id: route.pg_id,
        request,
    })
}

pub(crate) fn encode_list_object_versions_response(
    response: &StorageRpcListObjectVersionsResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_list_page_item_count(response.response.versions.len())?;
    let mut out = Vec::new();
    put_stored_object_list(&mut out, &response.response.versions);
    put_bool(&mut out, response.response.is_truncated);
    put_optional_string(
        &mut out,
        response
            .response
            .next_key_marker
            .as_ref()
            .map(|key| key.as_str()),
    );
    put_optional_version_id(&mut out, response.response.next_version_id_marker);
    Ok(out)
}

pub(crate) fn decode_list_object_versions_response(
    bytes: &[u8],
) -> Result<StorageRpcListObjectVersionsResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let versions = decoder.read_stored_object_list_with_limit(STORAGE_RPC_MAX_LIST_PAGE_ITEMS)?;
    let is_truncated = decoder.read_bool()?;
    let next_key_marker = decoder.read_optional_object_key()?;
    let next_version_id_marker = decoder.read_optional_version_id()?;
    decoder.finish()?;
    Ok(StorageRpcListObjectVersionsResponse {
        response: ListObjectVersionsResp {
            versions,
            is_truncated,
            next_key_marker,
            next_version_id_marker,
        },
    })
}

pub(crate) fn encode_list_multipart_uploads_request(
    request: &StorageRpcListMultipartUploadsRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_list_page_item_limit(request.request.max_uploads)?;
    let mut out = encode_bucket_pg_request(&StorageRpcBucketPgRequest {
        node_id: request.node_id,
        cluster_epoch: request.cluster_epoch,
        pg_id: request.pg_id,
    })?;
    put_string(&mut out, request.request.bucket.as_str());
    put_optional_string(
        &mut out,
        request.request.prefix.as_ref().map(|key| key.as_str()),
    );
    match request.request.page_start.as_ref() {
        None => put_u8(&mut out, 0),
        Some(ListMultipartUploadsPageStart::After {
            key_marker,
            upload_id_marker,
        }) => {
            put_u8(&mut out, 1);
            put_string(&mut out, key_marker.as_str());
            match upload_id_marker {
                None => put_u8(&mut out, 0),
                Some(upload_id) => {
                    put_u8(&mut out, 1);
                    put_string(&mut out, upload_id.as_str());
                }
            }
        }
        Some(ListMultipartUploadsPageStart::At(key)) => {
            put_u8(&mut out, 2);
            put_string(&mut out, key.as_str());
        }
    }
    put_u32(&mut out, request.request.max_uploads);
    Ok(out)
}

pub(crate) fn decode_list_multipart_uploads_request(
    bytes: &[u8],
) -> Result<StorageRpcListMultipartUploadsRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let bucket = decoder.read_bucket_name()?;
    let prefix = decoder.read_optional_object_key()?;
    let page_start = match decoder.read_u8()? {
        0 => None,
        1 => Some(ListMultipartUploadsPageStart::After {
            key_marker: decoder.read_object_key()?,
            upload_id_marker: decoder.read_optional_upload_id()?,
        }),
        2 => Some(ListMultipartUploadsPageStart::At(
            decoder.read_object_key()?,
        )),
        _ => {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid multipart upload list page-start tag",
            ))
        }
    };
    let request = ListMultipartUploadsReq {
        bucket,
        prefix,
        page_start,
        max_uploads: decoder.read_u32()?,
    };
    decoder.finish()?;
    validate_list_page_item_limit(request.max_uploads)?;
    Ok(StorageRpcListMultipartUploadsRequest {
        node_id: route.node_id,
        cluster_epoch: route.cluster_epoch,
        pg_id: route.pg_id,
        request,
    })
}

pub(crate) fn encode_list_multipart_uploads_response(
    response: &StorageRpcListMultipartUploadsResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_list_page_item_count(response.response.uploads.len())?;
    let mut out = Vec::new();
    put_u32(
        &mut out,
        u32::try_from(response.response.uploads.len()).map_err(|_| {
            StorageRpcPayloadError::PayloadTooLarge {
                len: response.response.uploads.len(),
                limit: STORAGE_RPC_MAX_LIST_PAGE_ITEMS as usize,
            }
        })?,
    );
    for upload in &response.response.uploads {
        put_multipart_upload_record(&mut out, upload);
    }
    put_bool(&mut out, response.response.is_truncated);
    put_optional_string(
        &mut out,
        response
            .response
            .next_key_marker
            .as_ref()
            .map(|key| key.as_str()),
    );
    match response.response.next_upload_id_marker.as_ref() {
        None => put_u8(&mut out, 0),
        Some(upload_id) => {
            put_u8(&mut out, 1);
            put_string(&mut out, upload_id.as_str());
        }
    }
    Ok(out)
}

pub(crate) fn decode_list_multipart_uploads_response(
    bytes: &[u8],
) -> Result<StorageRpcListMultipartUploadsResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let upload_count = decoder.read_limited_bounded_remaining_count(
        4 + UPLOAD_ID_LEN + 4 + STORAGE_RPC_MAX_BUCKET_NAME_LEN + 4,
        "multipart upload list count exceeds payload",
        STORAGE_RPC_MAX_LIST_PAGE_ITEMS,
    )?;
    let mut uploads = Vec::new();
    for _ in 0..upload_count {
        uploads.push(decoder.read_multipart_upload_record()?);
    }
    let is_truncated = decoder.read_bool()?;
    let next_key_marker = decoder.read_optional_object_key()?;
    let next_upload_id_marker = decoder.read_optional_upload_id()?;
    decoder.finish()?;
    Ok(StorageRpcListMultipartUploadsResponse {
        response: ListMultipartUploadsResp {
            uploads,
            is_truncated,
            next_key_marker,
            next_upload_id_marker,
        },
    })
}

pub(crate) fn encode_lifecycle_sweep_buckets_response(
    response: &StorageRpcLifecycleSweepBucketsResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    put_u32(
        &mut out,
        u32::try_from(response.buckets.len()).map_err(|_| {
            StorageRpcPayloadError::PayloadTooLarge {
                len: response.buckets.len(),
                limit: u32::MAX as usize,
            }
        })?,
    );
    for bucket in &response.buckets {
        put_bucket_info(&mut out, bucket);
    }
    Ok(out)
}

pub(crate) fn decode_lifecycle_sweep_buckets_response(
    bytes: &[u8],
) -> Result<StorageRpcLifecycleSweepBucketsResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let lifecycle_count =
        decoder.read_bounded_remaining_count(1, "lifecycle bucket count exceeds payload")?;
    let mut lifecycle_buckets = Vec::new();
    for _ in 0..lifecycle_count {
        lifecycle_buckets.push(decoder.read_bucket_info()?);
    }
    decoder.finish()?;
    Ok(StorageRpcLifecycleSweepBucketsResponse {
        buckets: lifecycle_buckets,
    })
}

pub(crate) fn encode_aborting_multipart_upload_buckets_response(
    response: &StorageRpcAbortingMultipartUploadBucketsResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    put_u32(
        &mut out,
        u32::try_from(response.witnesses.len()).map_err(|_| {
            StorageRpcPayloadError::PayloadTooLarge {
                len: response.witnesses.len(),
                limit: u32::MAX as usize,
            }
        })?,
    );
    for witness in &response.witnesses {
        put_string(&mut out, witness.bucket.as_str());
        put_string(&mut out, witness.key.as_str());
    }
    Ok(out)
}

pub(crate) fn decode_aborting_multipart_upload_buckets_response(
    bytes: &[u8],
) -> Result<StorageRpcAbortingMultipartUploadBucketsResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let count = decoder.read_bounded_remaining_count(
        2,
        "aborting multipart upload bucket count exceeds payload",
    )?;
    let mut witnesses = Vec::with_capacity(count);
    for _ in 0..count {
        witnesses.push(AbortingMultipartUploadBucketWitness {
            bucket: decoder.read_bucket_name()?,
            key: decoder.read_object_key()?,
        });
    }
    decoder.finish()?;
    Ok(StorageRpcAbortingMultipartUploadBucketsResponse { witnesses })
}

pub(crate) fn encode_lifecycle_sweep_claim_acquire_request(
    request: &StorageRpcLifecycleSweepClaimAcquireRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_bucket_write_reservation_identity(
        &request.claim_id,
        &request.owner_token,
        "lifecycle-sweep",
        None,
    )?;
    let mut out = encode_bucket_request(&request.bucket);
    put_u64(&mut out, request.bucket_incarnation_generation);
    put_string(&mut out, &request.claim_id);
    put_string(&mut out, &request.owner_token);
    put_u64(&mut out, request.claimed_at);
    put_optional_u64(&mut out, request.lease_deadline);
    put_u64(&mut out, request.now);
    Ok(out)
}

pub(crate) fn decode_lifecycle_sweep_claim_acquire_request(
    bytes: &[u8],
) -> Result<StorageRpcLifecycleSweepClaimAcquireRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let bucket = decoder.read_bucket_request()?;
    let bucket_incarnation_generation = decoder.read_u64()?;
    let claim_id = decoder.read_string_with_limit(
        STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN,
        StorageRpcPayloadError::InvalidDurableClaimToken("claim id exceeds maximum length"),
    )?;
    let owner_token = decoder.read_string_with_limit(
        STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN,
        StorageRpcPayloadError::InvalidDurableClaimToken("owner token exceeds maximum length"),
    )?;
    let claimed_at = decoder.read_u64()?;
    let lease_deadline = decoder.read_optional_u64()?;
    let now = decoder.read_u64()?;
    decoder.finish()?;
    validate_bucket_write_reservation_identity(&claim_id, &owner_token, "lifecycle-sweep", None)?;
    Ok(StorageRpcLifecycleSweepClaimAcquireRequest {
        bucket,
        bucket_incarnation_generation,
        claim_id,
        owner_token,
        claimed_at,
        lease_deadline,
        now,
    })
}

pub(crate) fn encode_lifecycle_sweep_claim_record_request(
    request: &StorageRpcLifecycleSweepClaimRecordRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.cluster_epoch != request.claim.cluster_epoch {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "request route epoch must match claim epoch",
        ));
    }
    validate_lifecycle_sweep_claim_record(&request.claim)?;
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.cluster_epoch.get());
    put_u32(&mut out, request.pg_id.get());
    put_lifecycle_sweep_claim_record(&mut out, &request.claim);
    Ok(out)
}

pub(crate) fn decode_lifecycle_sweep_claim_record_request(
    bytes: &[u8],
) -> Result<StorageRpcLifecycleSweepClaimRecordRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let claim = decoder.read_lifecycle_sweep_claim_record()?;
    decoder.finish()?;
    if cluster_epoch != claim.cluster_epoch {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "request route epoch must match claim epoch",
        ));
    }
    validate_lifecycle_sweep_claim_record(&claim)?;
    Ok(StorageRpcLifecycleSweepClaimRecordRequest {
        node_id,
        cluster_epoch,
        pg_id,
        claim,
    })
}

pub(crate) fn encode_lifecycle_sweep_claim_heartbeat_request(
    request: &StorageRpcLifecycleSweepClaimHeartbeatRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = encode_lifecycle_sweep_claim_record_request(&request.record)?;
    put_u64(&mut out, request.heartbeat_at);
    put_optional_u64(&mut out, request.lease_deadline);
    Ok(out)
}

pub(crate) fn decode_lifecycle_sweep_claim_heartbeat_request(
    bytes: &[u8],
) -> Result<StorageRpcLifecycleSweepClaimHeartbeatRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let claim = decoder.read_lifecycle_sweep_claim_record()?;
    let heartbeat_at = decoder.read_u64()?;
    let lease_deadline = decoder.read_optional_u64()?;
    decoder.finish()?;
    let record = StorageRpcLifecycleSweepClaimRecordRequest {
        node_id,
        cluster_epoch,
        pg_id,
        claim,
    };
    if record.cluster_epoch != record.claim.cluster_epoch {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "request route epoch must match claim epoch",
        ));
    }
    validate_lifecycle_sweep_claim_record(&record.claim)?;
    Ok(StorageRpcLifecycleSweepClaimHeartbeatRequest {
        record,
        heartbeat_at,
        lease_deadline,
    })
}

pub(crate) fn encode_lifecycle_sweep_claim_error_request(
    request: &StorageRpcLifecycleSweepClaimErrorRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = encode_lifecycle_sweep_claim_record_request(&request.record)?;
    put_string(&mut out, &request.last_error);
    Ok(out)
}

pub(crate) fn decode_lifecycle_sweep_claim_error_request(
    bytes: &[u8],
) -> Result<StorageRpcLifecycleSweepClaimErrorRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let claim = decoder.read_lifecycle_sweep_claim_record()?;
    let last_error = decoder.read_string_with_limit(
        4096,
        StorageRpcPayloadError::InvalidDurableClaimToken("lifecycle error exceeds maximum length"),
    )?;
    decoder.finish()?;
    let record = StorageRpcLifecycleSweepClaimRecordRequest {
        node_id,
        cluster_epoch,
        pg_id,
        claim,
    };
    if record.cluster_epoch != record.claim.cluster_epoch {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "request route epoch must match claim epoch",
        ));
    }
    validate_lifecycle_sweep_claim_record(&record.claim)?;
    Ok(StorageRpcLifecycleSweepClaimErrorRequest { record, last_error })
}

pub(crate) fn encode_lifecycle_sweep_claim_optional_record_response(
    response: &StorageRpcLifecycleSweepClaimOptionalRecordResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    match &response.record {
        Some(record) => {
            validate_lifecycle_sweep_claim_record(record)?;
            put_u8(&mut out, 1);
            put_lifecycle_sweep_claim_record(&mut out, record);
        }
        None => put_u8(&mut out, 0),
    }
    Ok(out)
}

pub(crate) fn decode_lifecycle_sweep_claim_optional_record_response(
    bytes: &[u8],
) -> Result<StorageRpcLifecycleSweepClaimOptionalRecordResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let record = match decoder.read_u8()? {
        0 => None,
        1 => {
            let record = decoder.read_lifecycle_sweep_claim_record()?;
            validate_lifecycle_sweep_claim_record(&record)?;
            Some(record)
        }
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "unknown optional lifecycle sweep claim record tag",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcLifecycleSweepClaimOptionalRecordResponse { record })
}

pub(crate) fn encode_lifecycle_sweep_claim_record_response(
    response: &StorageRpcLifecycleSweepClaimRecordResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_lifecycle_sweep_claim_record(&response.record)?;
    let mut out = Vec::new();
    put_lifecycle_sweep_claim_record(&mut out, &response.record);
    Ok(out)
}

pub(crate) fn decode_lifecycle_sweep_claim_record_response(
    bytes: &[u8],
) -> Result<StorageRpcLifecycleSweepClaimRecordResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let record = decoder.read_lifecycle_sweep_claim_record()?;
    decoder.finish()?;
    validate_lifecycle_sweep_claim_record(&record)?;
    Ok(StorageRpcLifecycleSweepClaimRecordResponse { record })
}
