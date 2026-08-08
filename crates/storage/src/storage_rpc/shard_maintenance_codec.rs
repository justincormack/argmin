pub(crate) fn encode_shard_write_item(
    item: &StorageRpcShardWriteItem,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_shard_write_payload(item.expected_size, item.expected_crc64, &item.payload)?;
    let mut out = Vec::new();
    put_u64(&mut out, item.expected_size);
    put_u64(&mut out, item.expected_crc64);
    put_bytes(&mut out, &item.payload);
    Ok(out)
}

pub(crate) fn decode_shard_write_item(
    bytes: &[u8],
) -> Result<StorageRpcShardWriteItem, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let expected_size = decoder.read_u64()?;
    let expected_crc64 = decoder.read_u64()?;
    let payload = decoder.read_bytes()?.to_vec();
    decoder.finish()?;
    validate_shard_write_payload(expected_size, expected_crc64, &payload)?;
    Ok(StorageRpcShardWriteItem {
        expected_size,
        expected_crc64,
        payload,
    })
}

pub(crate) fn encode_shard_write_request(
    request: &StorageRpcShardWriteRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_shard_location_matches_key(&request.location, &request.shard_key)?;
    if request
        .effect_deadline
        .is_some_and(|deadline| !admitted_route_effect_deadline_is_conservative(deadline))
    {
        return Err(StorageRpcPayloadError::InvalidShardWriteRequest(
            "shard write effect deadline must not exceed authority deadline",
        ));
    }
    validate_shard_write_payload(
        request.expected_size,
        request.expected_crc64,
        &request.payload,
    )?;
    let mut out = Vec::new();
    put_shard_location(&mut out, request.location);
    put_bytes(&mut out, request.shard_key.as_bytes());
    put_u64(&mut out, request.expected_size);
    put_u64(&mut out, request.expected_crc64);
    match request.effect_deadline {
        None => put_u8(&mut out, 0),
        Some(deadline) => {
            put_u8(&mut out, 1);
            put_u64(&mut out, deadline.authority_valid_until_ms);
            put_u64(&mut out, deadline.portable_wall_valid_until_ms);
        }
    }
    put_bytes(&mut out, &request.payload);
    Ok(out)
}

pub(crate) fn decode_shard_write_request(
    bytes: &[u8],
) -> Result<StorageRpcShardWriteRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let location = decoder.read_shard_location()?;
    let shard_key = decoder.read_shard_key()?;
    let expected_size = decoder.read_u64()?;
    let expected_crc64 = decoder.read_u64()?;
    let effect_deadline = match decoder.read_u8()? {
        0 => None,
        1 => Some(StorageRpcAdmittedRouteEffectDeadline {
            authority_valid_until_ms: decoder.read_u64()?,
            portable_wall_valid_until_ms: decoder.read_u64()?,
        }),
        _ => {
            return Err(StorageRpcPayloadError::InvalidShardWriteRequest(
                "unknown shard write effect deadline tag",
            ));
        }
    };
    let payload = decoder.read_bytes()?.to_vec();
    decoder.finish()?;
    validate_shard_location_matches_key(&location, &shard_key)?;
    if effect_deadline
        .is_some_and(|deadline| !admitted_route_effect_deadline_is_conservative(deadline))
    {
        return Err(StorageRpcPayloadError::InvalidShardWriteRequest(
            "shard write effect deadline must not exceed authority deadline",
        ));
    }
    validate_shard_write_payload(expected_size, expected_crc64, &payload)?;
    Ok(StorageRpcShardWriteRequest {
        location,
        shard_key,
        expected_size,
        expected_crc64,
        effect_deadline,
        payload,
    })
}

pub(crate) fn encode_shard_write_ack(ack: WriteAck) -> Vec<u8> {
    let mut out = Vec::new();
    put_u64(&mut out, ack.stored_size);
    put_u64(&mut out, ack.crc64);
    out
}

pub(crate) fn decode_shard_write_ack(
    bytes: &[u8],
    expected_size: u64,
    expected_crc64: u64,
) -> Result<WriteAck, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let stored_size = decoder.read_u64()?;
    let crc64 = decoder.read_u64()?;
    decoder.finish()?;
    if stored_size != expected_size {
        return Err(StorageRpcPayloadError::ShardWriteSizeMismatch {
            expected: expected_size,
            actual: stored_size,
        });
    }
    if crc64 != expected_crc64 {
        return Err(StorageRpcPayloadError::ShardWriteChecksumMismatch);
    }
    Ok(WriteAck { stored_size, crc64 })
}

pub(crate) fn encode_shard_read_request(
    request: &StorageRpcShardReadRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_shard_location_matches_key(&request.location, &request.shard_key)?;
    let mut out = Vec::new();
    put_shard_location(&mut out, request.location);
    put_bytes(&mut out, request.shard_key.as_bytes());
    put_u64(&mut out, request.expected_ack.stored_size);
    put_u64(&mut out, request.expected_ack.crc64);
    Ok(out)
}

pub(crate) fn decode_shard_read_request(
    bytes: &[u8],
) -> Result<StorageRpcShardReadRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let location = decoder.read_shard_location()?;
    let shard_key = decoder.read_shard_key()?;
    let expected_ack = WriteAck {
        stored_size: decoder.read_u64()?,
        crc64: decoder.read_u64()?,
    };
    decoder.finish()?;
    validate_shard_location_matches_key(&location, &shard_key)?;
    Ok(StorageRpcShardReadRequest {
        location,
        shard_key,
        expected_ack,
    })
}

pub(crate) fn encode_historical_shard_read_request(
    request: &StorageRpcHistoricalShardReadRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_shard_location_matches_key(&request.location, &request.shard_key)?;
    let mut out = Vec::new();
    put_shard_location(&mut out, request.location);
    put_bytes(&mut out, request.shard_key.as_bytes());
    Ok(out)
}

pub(crate) fn decode_historical_shard_read_request(
    bytes: &[u8],
) -> Result<StorageRpcHistoricalShardReadRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let location = decoder.read_shard_location()?;
    let shard_key = decoder.read_shard_key()?;
    decoder.finish()?;
    validate_shard_location_matches_key(&location, &shard_key)?;
    Ok(StorageRpcHistoricalShardReadRequest {
        location,
        shard_key,
    })
}

pub(crate) fn encode_shard_read_range_request(
    request: &StorageRpcShardReadRangeRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_shard_location_matches_key(&request.location, &request.shard_key)?;
    validate_shard_read_range(
        request.expected_ack.stored_size,
        request.offset,
        request.length,
    )?;
    let mut out = Vec::new();
    put_shard_location(&mut out, request.location);
    put_bytes(&mut out, request.shard_key.as_bytes());
    put_u64(&mut out, request.expected_ack.stored_size);
    put_u64(&mut out, request.expected_ack.crc64);
    put_u64(&mut out, request.offset);
    put_u64(&mut out, request.length);
    Ok(out)
}

pub(crate) fn decode_shard_read_range_request(
    bytes: &[u8],
) -> Result<StorageRpcShardReadRangeRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let location = decoder.read_shard_location()?;
    let shard_key = decoder.read_shard_key()?;
    let expected_ack = WriteAck {
        stored_size: decoder.read_u64()?,
        crc64: decoder.read_u64()?,
    };
    let offset = decoder.read_u64()?;
    let length = decoder.read_u64()?;
    decoder.finish()?;
    validate_shard_location_matches_key(&location, &shard_key)?;
    validate_shard_read_range(expected_ack.stored_size, offset, length)?;
    Ok(StorageRpcShardReadRangeRequest {
        location,
        shard_key,
        expected_ack,
        offset,
        length,
    })
}

pub(crate) fn encode_shard_read_response(
    payload: &[u8],
    expected_ack: WriteAck,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_shard_payload_matches_ack(payload, expected_ack)?;
    let mut out = Vec::new();
    put_bytes(&mut out, payload);
    Ok(out)
}

pub(crate) fn decode_shard_read_response(
    bytes: &[u8],
    expected_ack: WriteAck,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let payload = decoder.read_bytes()?.to_vec();
    decoder.finish()?;
    validate_shard_payload_matches_ack(&payload, expected_ack)?;
    Ok(payload)
}

pub(crate) fn encode_historical_shard_read_response(
    payload: &[u8],
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let ack = WriteAck {
        stored_size: payload.len() as u64,
        crc64: checksum::crc64::checksum(payload),
    };
    let mut out = Vec::new();
    put_u64(&mut out, ack.stored_size);
    put_u64(&mut out, ack.crc64);
    put_bytes(&mut out, payload);
    Ok(out)
}

pub(crate) fn decode_historical_shard_read_response(
    bytes: &[u8],
) -> Result<(Vec<u8>, WriteAck), StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let ack = WriteAck {
        stored_size: decoder.read_u64()?,
        crc64: decoder.read_u64()?,
    };
    let payload = decoder.read_bytes()?.to_vec();
    decoder.finish()?;
    validate_shard_payload_matches_ack(&payload, ack)?;
    Ok((payload, ack))
}

pub(crate) fn encode_shard_read_range_response(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    put_bytes(&mut out, payload);
    out
}

pub(crate) fn decode_shard_read_range_response(
    bytes: &[u8],
    expected_len: usize,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let payload = decoder.read_bytes()?.to_vec();
    decoder.finish()?;
    if payload.len() != expected_len {
        return Err(StorageRpcPayloadError::ShardWriteSizeMismatch {
            expected: expected_len as u64,
            actual: payload.len() as u64,
        });
    }
    Ok(payload)
}

pub(crate) fn encode_shard_delete_request(
    request: &StorageRpcShardDeleteRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_shard_location_matches_key(&request.location, &request.shard_key)?;
    let mut out = Vec::new();
    put_shard_location(&mut out, request.location);
    put_bytes(&mut out, request.shard_key.as_bytes());
    Ok(out)
}

pub(crate) fn decode_shard_delete_request(
    bytes: &[u8],
) -> Result<StorageRpcShardDeleteRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let location = decoder.read_shard_location()?;
    let shard_key = decoder.read_shard_key()?;
    decoder.finish()?;
    validate_shard_location_matches_key(&location, &shard_key)?;
    Ok(StorageRpcShardDeleteRequest {
        location,
        shard_key,
    })
}

pub(crate) fn encode_shard_ack_batch_request(
    request: &StorageRpcShardAckBatchRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_shard_ack_batch(request.items.len())?;
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.cluster_epoch.get());
    put_u32(&mut out, request.pg_id.get());
    put_u32(
        &mut out,
        u32::try_from(request.items.len()).map_err(|_| {
            StorageRpcPayloadError::PayloadTooLarge {
                len: request.items.len(),
                limit: u32::MAX as usize,
            }
        })?,
    );
    for item in &request.items {
        put_bytes(&mut out, item.shard_key.as_bytes());
        put_u64(&mut out, item.ack.stored_size);
        put_u64(&mut out, item.ack.crc64);
    }
    Ok(out)
}

pub(crate) fn decode_shard_ack_batch_request(
    bytes: &[u8],
) -> Result<StorageRpcShardAckBatchRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let item_count = decoder.read_u32()? as usize;
    validate_shard_ack_batch(item_count)?;
    if decoder.remaining_len()
        != item_count * (STORAGE_RPC_SHARD_KEY_FIELD_LEN + STORAGE_RPC_WRITE_ACK_LEN)
    {
        return Err(StorageRpcPayloadError::Truncated);
    }
    let mut items = Vec::with_capacity(item_count);
    for _ in 0..item_count {
        let shard_key = decoder.read_shard_key()?;
        let stored_size = decoder.read_u64()?;
        let crc64 = decoder.read_u64()?;
        items.push(StorageRpcShardAckItem {
            shard_key,
            ack: WriteAck { stored_size, crc64 },
        });
    }
    decoder.finish()?;
    Ok(StorageRpcShardAckBatchRequest {
        node_id,
        cluster_epoch,
        pg_id,
        items,
    })
}

pub(crate) fn encode_shard_ack_item_request(request: &StorageRpcShardAckItemRequest) -> Vec<u8> {
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.cluster_epoch.get());
    put_u32(&mut out, request.pg_id.get());
    put_bytes(&mut out, request.shard_key.as_bytes());
    out
}

pub(crate) fn decode_shard_ack_item_request(
    bytes: &[u8],
) -> Result<StorageRpcShardAckItemRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let shard_key = decoder.read_shard_key()?;
    decoder.finish()?;
    Ok(StorageRpcShardAckItemRequest {
        node_id,
        cluster_epoch,
        pg_id,
        shard_key,
    })
}

pub(crate) fn encode_shard_ack_item_response(item: &StorageRpcShardAckItem) -> Vec<u8> {
    let mut out = Vec::new();
    put_bytes(&mut out, item.shard_key.as_bytes());
    put_u64(&mut out, item.ack.stored_size);
    put_u64(&mut out, item.ack.crc64);
    out
}

pub(crate) fn decode_shard_ack_item_response(
    bytes: &[u8],
) -> Result<StorageRpcShardAckItem, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let shard_key = decoder.read_shard_key()?;
    let stored_size = decoder.read_u64()?;
    let crc64 = decoder.read_u64()?;
    decoder.finish()?;
    Ok(StorageRpcShardAckItem {
        shard_key,
        ack: WriteAck { stored_size, crc64 },
    })
}

pub(crate) fn encode_scavenger_list_files_request(
    request: &StorageRpcScavengerListFilesRequest,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.cluster_epoch.get());
    put_u32(&mut out, request.data_pg_id.get());
    out
}

pub(crate) fn decode_scavenger_list_files_request(
    bytes: &[u8],
) -> Result<StorageRpcScavengerListFilesRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let data_pg_id = PgId::new(decoder.read_u32()?);
    decoder.finish()?;
    Ok(StorageRpcScavengerListFilesRequest {
        node_id,
        cluster_epoch,
        data_pg_id,
    })
}

pub(crate) fn encode_scavenger_list_files_response(scan: &ScavengerShardFileScan) -> Vec<u8> {
    let mut out = Vec::new();
    put_u32(
        &mut out,
        u32::try_from(scan.files.len()).expect("scavenger file count must fit in u32"),
    );
    for file in &scan.files {
        put_bytes(&mut out, file.key.as_bytes());
        put_u64(&mut out, file.size);
    }
    put_u32(
        &mut out,
        u32::try_from(scan.errors.len()).expect("scavenger scan error count must fit in u32"),
    );
    for error in &scan.errors {
        put_string(&mut out, error);
    }
    out
}

pub(crate) fn decode_scavenger_list_files_response(
    bytes: &[u8],
) -> Result<ScavengerShardFileScan, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let file_count = decoder.read_u32()? as usize;
    let file_bytes = file_count
        .checked_mul(STORAGE_RPC_SCAVENGER_FILE_RESPONSE_LEN)
        .ok_or(StorageRpcPayloadError::PayloadTooLarge {
            len: file_count,
            limit: usize::MAX / STORAGE_RPC_SCAVENGER_FILE_RESPONSE_LEN,
        })?;
    if file_bytes > decoder.remaining_len() {
        return Err(StorageRpcPayloadError::Truncated);
    }
    let mut files = Vec::with_capacity(file_count);
    for _ in 0..file_count {
        files.push(ScavengerShardFile {
            key: decoder.read_shard_key()?,
            size: decoder.read_u64()?,
        });
    }
    let error_count = decoder.read_u32()? as usize;
    if error_count > STORAGE_RPC_MAX_SCAVENGER_SCAN_ERRORS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: error_count,
            limit: STORAGE_RPC_MAX_SCAVENGER_SCAN_ERRORS,
        });
    }
    let mut errors = Vec::with_capacity(error_count);
    for _ in 0..error_count {
        errors.push(decoder.read_string_with_limit(
            STORAGE_RPC_MAX_SCAVENGER_SCAN_ERROR_LEN,
            StorageRpcPayloadError::PayloadTooLarge {
                len: STORAGE_RPC_MAX_SCAVENGER_SCAN_ERROR_LEN + 1,
                limit: STORAGE_RPC_MAX_SCAVENGER_SCAN_ERROR_LEN,
            },
        )?);
    }
    decoder.finish()?;
    Ok(ScavengerShardFileScan { files, errors })
}

pub(crate) fn encode_scavenger_shard_rows_response(rows: &[ScavengerShardRow]) -> Vec<u8> {
    let mut out = Vec::new();
    put_u32(
        &mut out,
        u32::try_from(rows.len()).expect("scavenger shard row count must fit in u32"),
    );
    for row in rows {
        put_bytes(&mut out, row.key.as_bytes());
        put_u64(&mut out, row.ack.stored_size);
        put_u64(&mut out, row.ack.crc64);
    }
    out
}

pub(crate) fn decode_scavenger_shard_rows_response(
    bytes: &[u8],
) -> Result<Vec<ScavengerShardRow>, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let count = decoder.read_u32()? as usize;
    if count > STORAGE_RPC_MAX_SCAVENGER_METADATA_ITEMS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: count,
            limit: STORAGE_RPC_MAX_SCAVENGER_METADATA_ITEMS,
        });
    }
    let item_bytes = count
        .checked_mul(STORAGE_RPC_SHARD_KEY_FIELD_LEN + STORAGE_RPC_WRITE_ACK_LEN)
        .ok_or(StorageRpcPayloadError::PayloadTooLarge {
            len: count,
            limit: usize::MAX / (STORAGE_RPC_SHARD_KEY_FIELD_LEN + STORAGE_RPC_WRITE_ACK_LEN),
        })?;
    if item_bytes > decoder.remaining_len() {
        return Err(StorageRpcPayloadError::Truncated);
    }
    let mut rows = Vec::with_capacity(count);
    for _ in 0..count {
        rows.push(ScavengerShardRow {
            key: decoder.read_shard_key()?,
            ack: WriteAck {
                stored_size: decoder.read_u64()?,
                crc64: decoder.read_u64()?,
            },
        });
    }
    decoder.finish()?;
    Ok(rows)
}

pub(crate) fn encode_scavenger_payload_references_response(
    references: &[ShardScavengerPayloadReference],
) -> Vec<u8> {
    let mut out = Vec::new();
    put_u32(
        &mut out,
        u32::try_from(references.len()).expect("scavenger reference count must fit in u32"),
    );
    for reference in references {
        put_scavenger_payload_reference(&mut out, reference);
    }
    out
}

pub(crate) fn decode_scavenger_payload_references_response(
    bytes: &[u8],
) -> Result<Vec<ShardScavengerPayloadReference>, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let count = decoder.read_u32()? as usize;
    if count > STORAGE_RPC_MAX_SCAVENGER_METADATA_ITEMS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: count,
            limit: STORAGE_RPC_MAX_SCAVENGER_METADATA_ITEMS,
        });
    }
    if count > decoder.remaining_len() / STORAGE_RPC_SCAVENGER_PAYLOAD_REFERENCE_MIN_LEN {
        return Err(StorageRpcPayloadError::Truncated);
    }
    let mut references = Vec::with_capacity(count);
    for _ in 0..count {
        references.push(decoder.read_scavenger_payload_reference()?);
    }
    decoder.finish()?;
    Ok(references)
}

pub(crate) fn encode_placed_segment_backfill_reference_page_request(
    request: &StorageRpcPlacedSegmentBackfillReferencePageRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.limit.get() > PLACED_SEGMENT_BACKFILL_REFERENCE_PAGE_LIMIT {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: usize::from(request.limit.get()),
            limit: usize::from(PLACED_SEGMENT_BACKFILL_REFERENCE_PAGE_LIMIT),
        });
    }
    let mut out = encode_bucket_pg_request(&request.route)?;
    match &request.after {
        None => put_u8(&mut out, 0),
        Some(cursor) => {
            put_u8(&mut out, 1);
            put_placed_segment_backfill_reference_cursor(&mut out, cursor);
        }
    }
    put_u16(&mut out, request.limit.get());
    Ok(out)
}

pub(crate) fn decode_placed_segment_backfill_reference_page_request(
    bytes: &[u8],
) -> Result<StorageRpcPlacedSegmentBackfillReferencePageRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let after = match decoder.read_u8()? {
        0 => None,
        1 => Some(decoder.read_placed_segment_backfill_reference_cursor()?),
        _ => {
            return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid backfill reference cursor option tag",
            ));
        }
    };
    let limit = NonZeroU16::new(decoder.read_u16()?).ok_or(
        StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "backfill reference page limit must not be zero",
        ),
    )?;
    decoder.finish()?;
    if limit.get() > PLACED_SEGMENT_BACKFILL_REFERENCE_PAGE_LIMIT {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: usize::from(limit.get()),
            limit: usize::from(PLACED_SEGMENT_BACKFILL_REFERENCE_PAGE_LIMIT),
        });
    }
    Ok(StorageRpcPlacedSegmentBackfillReferencePageRequest {
        route,
        after,
        limit,
    })
}

pub(crate) fn encode_placed_segment_backfill_reference_page_response(
    page: &PlacedSegmentBackfillReferencePage,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if page.items.len() > usize::from(PLACED_SEGMENT_BACKFILL_REFERENCE_PAGE_LIMIT) {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: page.items.len(),
            limit: usize::from(PLACED_SEGMENT_BACKFILL_REFERENCE_PAGE_LIMIT),
        });
    }
    let mut out = Vec::new();
    put_bool(&mut out, page.complete);
    put_u16(
        &mut out,
        u16::try_from(page.items.len()).expect("bounded backfill reference page fits in u16"),
    );
    for item in &page.items {
        put_placed_segment_backfill_reference_cursor(&mut out, &item.cursor);
        put_placed_scavenger_reference(&mut out, &item.reference);
    }
    Ok(out)
}

pub(crate) fn decode_placed_segment_backfill_reference_page_response(
    bytes: &[u8],
) -> Result<PlacedSegmentBackfillReferencePage, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let complete = decoder.read_bool()?;
    let count = usize::from(decoder.read_u16()?);
    if count > usize::from(PLACED_SEGMENT_BACKFILL_REFERENCE_PAGE_LIMIT) {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: count,
            limit: usize::from(PLACED_SEGMENT_BACKFILL_REFERENCE_PAGE_LIMIT),
        });
    }
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        items.push(PlacedSegmentBackfillReferencePageItem {
            cursor: decoder.read_placed_segment_backfill_reference_cursor()?,
            reference: decoder.read_placed_scavenger_reference()?,
        });
    }
    decoder.finish()?;
    Ok(PlacedSegmentBackfillReferencePage { items, complete })
}

pub(crate) fn encode_scavenger_observation_record_request(
    request: &StorageRpcScavengerObservationRecordRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = encode_bucket_pg_request(&request.route)?;
    put_scavenger_observation_record(&mut out, &request.observation);
    Ok(out)
}

pub(crate) fn decode_scavenger_observation_record_request(
    bytes: &[u8],
) -> Result<StorageRpcScavengerObservationRecordRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let observation = decoder.read_scavenger_observation_record()?;
    decoder.finish()?;
    Ok(StorageRpcScavengerObservationRecordRequest { route, observation })
}

pub(crate) fn encode_scavenger_observations_response(
    observations: &[ShardScavengerObservation],
) -> Vec<u8> {
    let mut out = Vec::new();
    put_u32(
        &mut out,
        u32::try_from(observations.len()).expect("scavenger observation count must fit in u32"),
    );
    for observation in observations {
        put_scavenger_observation(&mut out, observation);
    }
    out
}

pub(crate) fn decode_scavenger_observations_response(
    bytes: &[u8],
) -> Result<Vec<ShardScavengerObservation>, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let count = decoder.read_u32()? as usize;
    if count > STORAGE_RPC_MAX_SCAVENGER_METADATA_ITEMS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: count,
            limit: STORAGE_RPC_MAX_SCAVENGER_METADATA_ITEMS,
        });
    }
    if count > decoder.remaining_len() / STORAGE_RPC_SCAVENGER_OBSERVATION_MIN_LEN {
        return Err(StorageRpcPayloadError::Truncated);
    }
    let mut observations = Vec::with_capacity(count);
    for _ in 0..count {
        observations.push(decoder.read_scavenger_observation()?);
    }
    decoder.finish()?;
    Ok(observations)
}

pub(crate) fn encode_scavenger_observation_key_request(
    request: &StorageRpcScavengerObservationKeyRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = encode_bucket_pg_request(&request.route)?;
    put_scavenger_observation_key(&mut out, &request.key);
    Ok(out)
}

pub(crate) fn decode_scavenger_observation_key_request(
    bytes: &[u8],
) -> Result<StorageRpcScavengerObservationKeyRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let key = decoder.read_scavenger_observation_key()?;
    decoder.finish()?;
    Ok(StorageRpcScavengerObservationKeyRequest { route, key })
}

pub(crate) fn encode_placed_segment_shard_repair_record_request(
    request: &StorageRpcPlacedSegmentShardRepairRecordRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_placed_segment_shard_repair_work_item(&request.work_item)?;
    let mut out = encode_bucket_pg_request(&request.route)?;
    put_placed_segment_shard_repair_work_item(&mut out, &request.work_item);
    put_optional_string(&mut out, request.last_error.as_deref());
    Ok(out)
}

pub(crate) fn decode_placed_segment_shard_repair_record_request(
    bytes: &[u8],
) -> Result<StorageRpcPlacedSegmentShardRepairRecordRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let work_item = decoder.read_placed_segment_shard_repair_work_item()?;
    let last_error = decoder.read_optional_string_with_limit(
        PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN,
        StorageRpcPayloadError::PayloadTooLarge {
            len: PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN + 1,
            limit: PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN,
        },
    )?;
    decoder.finish()?;
    let request = StorageRpcPlacedSegmentShardRepairRecordRequest {
        route,
        work_item,
        last_error,
    };
    encode_placed_segment_shard_repair_record_request(&request)?;
    Ok(request)
}

pub(crate) fn encode_placed_segment_shard_repair_item_request(
    request: &StorageRpcPlacedSegmentShardRepairItemRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_placed_segment_shard_repair_work_item(&request.work_item)?;
    let mut out = encode_bucket_pg_request(&request.route)?;
    put_placed_segment_shard_repair_work_item(&mut out, &request.work_item);
    Ok(out)
}

pub(crate) fn decode_placed_segment_shard_repair_item_request(
    bytes: &[u8],
) -> Result<StorageRpcPlacedSegmentShardRepairItemRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let work_item = decoder.read_placed_segment_shard_repair_work_item()?;
    decoder.finish()?;
    let request = StorageRpcPlacedSegmentShardRepairItemRequest { route, work_item };
    encode_placed_segment_shard_repair_item_request(&request)?;
    Ok(request)
}

pub(crate) fn encode_placed_segment_shard_repair_claim_acquire_request(
    request: &StorageRpcPlacedSegmentShardRepairClaimAcquireRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_placed_segment_shard_repair_claim_identity(&request.claim_id, &request.owner_token)?;
    let Some(lease_deadline) = request.lease_deadline else {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "claim lease deadline is required",
        ));
    };
    if lease_deadline <= request.claimed_at {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "claim lease deadline must be after claimed time",
        ));
    }
    let mut out = encode_bucket_pg_request(&request.route)?;
    put_string(&mut out, &request.claim_id);
    put_string(&mut out, &request.owner_token);
    put_u64(&mut out, request.claimed_at);
    put_optional_u64(&mut out, request.lease_deadline);
    put_u64(&mut out, request.now);
    Ok(out)
}

pub(crate) fn decode_placed_segment_shard_repair_claim_acquire_request(
    bytes: &[u8],
) -> Result<StorageRpcPlacedSegmentShardRepairClaimAcquireRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let claim_id = decoder.read_string_with_limit(
        PLACED_SEGMENT_SHARD_REPAIR_CLAIM_ID_MAX_LEN,
        StorageRpcPayloadError::InvalidDurableClaimToken("claim id exceeds maximum length"),
    )?;
    let owner_token = decoder.read_string_with_limit(
        PLACED_SEGMENT_SHARD_REPAIR_OWNER_TOKEN_MAX_LEN,
        StorageRpcPayloadError::InvalidDurableClaimToken("owner token exceeds maximum length"),
    )?;
    let claimed_at = decoder.read_u64()?;
    let lease_deadline = decoder.read_optional_u64()?;
    let now = decoder.read_u64()?;
    decoder.finish()?;
    let request = StorageRpcPlacedSegmentShardRepairClaimAcquireRequest {
        route,
        claim_id,
        owner_token,
        claimed_at,
        lease_deadline,
        now,
    };
    encode_placed_segment_shard_repair_claim_acquire_request(&request)?;
    Ok(request)
}

pub(crate) fn encode_placed_segment_shard_repair_claim_optional_record_response(
    response: &StorageRpcPlacedSegmentShardRepairClaimOptionalRecordResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    match &response.record {
        Some(record) => {
            validate_placed_segment_shard_repair_claim_record(record)?;
            put_u8(&mut out, 1);
            put_placed_segment_shard_repair_claim_record(&mut out, record);
        }
        None => put_u8(&mut out, 0),
    }
    Ok(out)
}

pub(crate) fn decode_placed_segment_shard_repair_claim_optional_record_response(
    bytes: &[u8],
) -> Result<StorageRpcPlacedSegmentShardRepairClaimOptionalRecordResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let record = match decoder.read_u8()? {
        0 => None,
        1 => {
            let record = decoder.read_placed_segment_shard_repair_claim_record()?;
            validate_placed_segment_shard_repair_claim_record(&record)?;
            Some(record)
        }
        _ => {
            return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
                "placed segment repair claim optional record tag must be 0 or 1",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcPlacedSegmentShardRepairClaimOptionalRecordResponse { record })
}

pub(crate) fn encode_placed_segment_shard_repair_claim_record_request(
    request: &StorageRpcPlacedSegmentShardRepairClaimRecordRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_placed_segment_shard_repair_claim_record(&request.claim)?;
    let mut out = encode_bucket_pg_request(&request.route)?;
    put_placed_segment_shard_repair_claim_record(&mut out, &request.claim);
    Ok(out)
}

pub(crate) fn decode_placed_segment_shard_repair_claim_record_request(
    bytes: &[u8],
) -> Result<StorageRpcPlacedSegmentShardRepairClaimRecordRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let claim = decoder.read_placed_segment_shard_repair_claim_record()?;
    decoder.finish()?;
    let request = StorageRpcPlacedSegmentShardRepairClaimRecordRequest { route, claim };
    encode_placed_segment_shard_repair_claim_record_request(&request)?;
    Ok(request)
}

pub(crate) fn encode_placed_segment_shard_repair_claim_error_request(
    request: &StorageRpcPlacedSegmentShardRepairClaimErrorRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_placed_segment_shard_repair_claim_record(&request.claim)?;
    if request.last_error.len() > PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: request.last_error.len(),
            limit: PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN,
        });
    }
    let mut out = encode_bucket_pg_request(&request.route)?;
    put_placed_segment_shard_repair_claim_record(&mut out, &request.claim);
    put_string(&mut out, &request.last_error);
    put_u64(&mut out, request.next_attempt_after);
    Ok(out)
}

pub(crate) fn decode_placed_segment_shard_repair_claim_error_request(
    bytes: &[u8],
) -> Result<StorageRpcPlacedSegmentShardRepairClaimErrorRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let claim = decoder.read_placed_segment_shard_repair_claim_record()?;
    let last_error = decoder.read_string_with_limit(
        PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN,
        StorageRpcPayloadError::PayloadTooLarge {
            len: PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN + 1,
            limit: PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN,
        },
    )?;
    let next_attempt_after = decoder.read_u64()?;
    decoder.finish()?;
    let request = StorageRpcPlacedSegmentShardRepairClaimErrorRequest {
        route,
        claim,
        last_error,
        next_attempt_after,
    };
    encode_placed_segment_shard_repair_claim_error_request(&request)?;
    Ok(request)
}

pub(crate) fn encode_placed_segment_shard_repairs_response(
    repairs: &[PlacedSegmentShardRepairRecord],
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if repairs.len() > PLACED_SEGMENT_SHARD_REPAIR_LIST_LIMIT {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: repairs.len(),
            limit: PLACED_SEGMENT_SHARD_REPAIR_LIST_LIMIT,
        });
    }
    let mut out = Vec::new();
    put_u32(
        &mut out,
        u32::try_from(repairs.len()).map_err(|_| StorageRpcPayloadError::PayloadTooLarge {
            len: repairs.len(),
            limit: u32::MAX as usize,
        })?,
    );
    for repair in repairs {
        validate_placed_segment_shard_repair_work_item(&repair.work_item)?;
        if let Some(last_error) = repair.last_error.as_deref() {
            if last_error.len() > PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN {
                return Err(StorageRpcPayloadError::PayloadTooLarge {
                    len: last_error.len(),
                    limit: PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN,
                });
            }
        }
        put_placed_segment_shard_repair_record(&mut out, repair);
    }
    Ok(out)
}

pub(crate) fn decode_placed_segment_shard_repairs_response(
    bytes: &[u8],
) -> Result<Vec<PlacedSegmentShardRepairRecord>, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let count = decoder.read_u32()? as usize;
    if count > PLACED_SEGMENT_SHARD_REPAIR_LIST_LIMIT {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: count,
            limit: PLACED_SEGMENT_SHARD_REPAIR_LIST_LIMIT,
        });
    }
    if count > decoder.remaining_len() / STORAGE_RPC_PLACED_SEGMENT_REPAIR_RECORD_MIN_LEN {
        return Err(StorageRpcPayloadError::Truncated);
    }
    let mut repairs = Vec::with_capacity(count);
    for _ in 0..count {
        repairs.push(decoder.read_placed_segment_shard_repair_record()?);
    }
    decoder.finish()?;
    Ok(repairs)
}

pub(crate) fn encode_placed_segment_shard_backfill_record_request(
    request: &StorageRpcPlacedSegmentShardBackfillRecordRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_placed_segment_shard_backfill_work_item(&request.work_item)?;
    validate_placed_segment_shard_backfill_remaining_tolerance(
        &request.work_item,
        request.remaining_tolerance,
    )?;
    let mut out = encode_bucket_pg_request(&request.route)?;
    put_placed_segment_shard_backfill_work_item(&mut out, &request.work_item);
    put_u8(&mut out, request.remaining_tolerance);
    put_optional_string(&mut out, request.last_error.as_deref());
    Ok(out)
}

pub(crate) fn decode_placed_segment_shard_backfill_record_request(
    bytes: &[u8],
) -> Result<StorageRpcPlacedSegmentShardBackfillRecordRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let work_item = decoder.read_placed_segment_shard_backfill_work_item()?;
    let remaining_tolerance = decoder.read_u8()?;
    let last_error = decoder.read_optional_string_with_limit(
        PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN,
        StorageRpcPayloadError::PayloadTooLarge {
            len: PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN + 1,
            limit: PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN,
        },
    )?;
    decoder.finish()?;
    let request = StorageRpcPlacedSegmentShardBackfillRecordRequest {
        route,
        work_item,
        remaining_tolerance,
        last_error,
    };
    encode_placed_segment_shard_backfill_record_request(&request)?;
    Ok(request)
}

pub(crate) fn encode_placed_segment_shard_backfill_item_request(
    request: &StorageRpcPlacedSegmentShardBackfillItemRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_placed_segment_shard_backfill_work_item(&request.work_item)?;
    let mut out = encode_bucket_pg_request(&request.route)?;
    put_placed_segment_shard_backfill_work_item(&mut out, &request.work_item);
    Ok(out)
}

pub(crate) fn decode_placed_segment_shard_backfill_item_request(
    bytes: &[u8],
) -> Result<StorageRpcPlacedSegmentShardBackfillItemRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let work_item = decoder.read_placed_segment_shard_backfill_work_item()?;
    decoder.finish()?;
    let request = StorageRpcPlacedSegmentShardBackfillItemRequest { route, work_item };
    encode_placed_segment_shard_backfill_item_request(&request)?;
    Ok(request)
}

pub(crate) fn encode_placed_segment_shard_backfill_claim_acquire_request(
    request: &StorageRpcPlacedSegmentShardBackfillClaimAcquireRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_placed_segment_shard_backfill_claim_identity(&request.claim_id, &request.owner_token)?;
    let Some(lease_deadline) = request.lease_deadline else {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "claim lease deadline is required",
        ));
    };
    if lease_deadline <= request.claimed_at {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "claim lease deadline must be after claimed time",
        ));
    }
    let mut out = encode_bucket_pg_request(&request.route)?;
    put_string(&mut out, &request.claim_id);
    put_string(&mut out, &request.owner_token);
    put_u64(&mut out, request.claimed_at);
    put_optional_u64(&mut out, request.lease_deadline);
    put_u64(&mut out, request.now);
    Ok(out)
}

pub(crate) fn decode_placed_segment_shard_backfill_claim_acquire_request(
    bytes: &[u8],
) -> Result<StorageRpcPlacedSegmentShardBackfillClaimAcquireRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let claim_id = decoder.read_string_with_limit(
        PLACED_SEGMENT_SHARD_BACKFILL_CLAIM_ID_MAX_LEN,
        StorageRpcPayloadError::InvalidDurableClaimToken("claim id exceeds maximum length"),
    )?;
    let owner_token = decoder.read_string_with_limit(
        PLACED_SEGMENT_SHARD_BACKFILL_OWNER_TOKEN_MAX_LEN,
        StorageRpcPayloadError::InvalidDurableClaimToken("owner token exceeds maximum length"),
    )?;
    let claimed_at = decoder.read_u64()?;
    let lease_deadline = decoder.read_optional_u64()?;
    let now = decoder.read_u64()?;
    decoder.finish()?;
    let request = StorageRpcPlacedSegmentShardBackfillClaimAcquireRequest {
        route,
        claim_id,
        owner_token,
        claimed_at,
        lease_deadline,
        now,
    };
    encode_placed_segment_shard_backfill_claim_acquire_request(&request)?;
    Ok(request)
}

pub(crate) fn encode_placed_segment_shard_backfill_claim_optional_record_response(
    response: &StorageRpcPlacedSegmentShardBackfillClaimOptionalRecordResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    match &response.record {
        Some(record) => {
            validate_placed_segment_shard_backfill_claim_record(record)?;
            put_u8(&mut out, 1);
            put_placed_segment_shard_backfill_claim_record(&mut out, record);
        }
        None => put_u8(&mut out, 0),
    }
    Ok(out)
}

pub(crate) fn decode_placed_segment_shard_backfill_claim_optional_record_response(
    bytes: &[u8],
) -> Result<StorageRpcPlacedSegmentShardBackfillClaimOptionalRecordResponse, StorageRpcPayloadError>
{
    let mut decoder = StorageRpcDecoder::new(bytes);
    let record = match decoder.read_u8()? {
        0 => None,
        1 => {
            let record = decoder.read_placed_segment_shard_backfill_claim_record()?;
            validate_placed_segment_shard_backfill_claim_record(&record)?;
            Some(record)
        }
        _ => {
            return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
                "placed segment backfill claim optional record tag must be 0 or 1",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcPlacedSegmentShardBackfillClaimOptionalRecordResponse { record })
}

pub(crate) fn encode_placed_segment_shard_backfill_claim_record_request(
    request: &StorageRpcPlacedSegmentShardBackfillClaimRecordRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_placed_segment_shard_backfill_claim_record(&request.claim)?;
    let mut out = encode_bucket_pg_request(&request.route)?;
    put_placed_segment_shard_backfill_claim_record(&mut out, &request.claim);
    Ok(out)
}

pub(crate) fn decode_placed_segment_shard_backfill_claim_record_request(
    bytes: &[u8],
) -> Result<StorageRpcPlacedSegmentShardBackfillClaimRecordRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let claim = decoder.read_placed_segment_shard_backfill_claim_record()?;
    decoder.finish()?;
    let request = StorageRpcPlacedSegmentShardBackfillClaimRecordRequest { route, claim };
    encode_placed_segment_shard_backfill_claim_record_request(&request)?;
    Ok(request)
}

pub(crate) fn encode_placed_segment_shard_backfill_claim_error_request(
    request: &StorageRpcPlacedSegmentShardBackfillClaimErrorRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_placed_segment_shard_backfill_claim_record(&request.claim)?;
    if request.last_error.len() > PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: request.last_error.len(),
            limit: PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN,
        });
    }
    let mut out = encode_bucket_pg_request(&request.route)?;
    put_placed_segment_shard_backfill_claim_record(&mut out, &request.claim);
    put_string(&mut out, &request.last_error);
    put_u64(&mut out, request.next_attempt_after);
    Ok(out)
}

pub(crate) fn decode_placed_segment_shard_backfill_claim_error_request(
    bytes: &[u8],
) -> Result<StorageRpcPlacedSegmentShardBackfillClaimErrorRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let claim = decoder.read_placed_segment_shard_backfill_claim_record()?;
    let last_error = decoder.read_string_with_limit(
        PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN,
        StorageRpcPayloadError::PayloadTooLarge {
            len: PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN + 1,
            limit: PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN,
        },
    )?;
    let next_attempt_after = decoder.read_u64()?;
    decoder.finish()?;
    let request = StorageRpcPlacedSegmentShardBackfillClaimErrorRequest {
        route,
        claim,
        last_error,
        next_attempt_after,
    };
    encode_placed_segment_shard_backfill_claim_error_request(&request)?;
    Ok(request)
}

pub(crate) fn encode_placed_segment_shard_backfills_response(
    backfills: &[PlacedSegmentShardBackfillRecord],
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if backfills.len() > PLACED_SEGMENT_SHARD_BACKFILL_LIST_LIMIT {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: backfills.len(),
            limit: PLACED_SEGMENT_SHARD_BACKFILL_LIST_LIMIT,
        });
    }
    let mut out = Vec::new();
    put_u32(
        &mut out,
        u32::try_from(backfills.len()).map_err(|_| StorageRpcPayloadError::PayloadTooLarge {
            len: backfills.len(),
            limit: u32::MAX as usize,
        })?,
    );
    for backfill in backfills {
        validate_placed_segment_shard_backfill_work_item(&backfill.work_item)?;
        validate_placed_segment_shard_backfill_remaining_tolerance(
            &backfill.work_item,
            backfill.remaining_tolerance,
        )?;
        if let Some(last_error) = backfill.last_error.as_deref() {
            if last_error.len() > PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN {
                return Err(StorageRpcPayloadError::PayloadTooLarge {
                    len: last_error.len(),
                    limit: PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN,
                });
            }
        }
        put_placed_segment_shard_backfill_record(&mut out, backfill);
    }
    Ok(out)
}

pub(crate) fn decode_placed_segment_shard_backfills_response(
    bytes: &[u8],
) -> Result<Vec<PlacedSegmentShardBackfillRecord>, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let count = decoder.read_u32()? as usize;
    if count > PLACED_SEGMENT_SHARD_BACKFILL_LIST_LIMIT {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: count,
            limit: PLACED_SEGMENT_SHARD_BACKFILL_LIST_LIMIT,
        });
    }
    if count > decoder.remaining_len() / STORAGE_RPC_PLACED_SEGMENT_BACKFILL_RECORD_MIN_LEN {
        return Err(StorageRpcPayloadError::Truncated);
    }
    let mut backfills = Vec::with_capacity(count);
    for _ in 0..count {
        backfills.push(decoder.read_placed_segment_shard_backfill_record()?);
    }
    decoder.finish()?;
    Ok(backfills)
}

pub(crate) fn encode_placed_segment_shard_backfill_count_response(
    count: usize,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    put_u64(
        &mut out,
        u64::try_from(count).map_err(|_| StorageRpcPayloadError::PayloadTooLarge {
            len: count,
            limit: u64::MAX as usize,
        })?,
    );
    Ok(out)
}

pub(crate) fn decode_placed_segment_shard_backfill_count_response(
    bytes: &[u8],
) -> Result<usize, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let count = usize::try_from(decoder.read_u64()?).map_err(|_| {
        StorageRpcPayloadError::PayloadTooLarge {
            len: usize::MAX,
            limit: usize::MAX,
        }
    })?;
    decoder.finish()?;
    Ok(count)
}

pub(crate) fn encode_read_handle_acquire_request(
    request: &StorageRpcReadHandleAcquireRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_read_handle_acquire_request(request)?;
    let mut out = Vec::new();
    put_string(&mut out, &request.read_operation_id);
    put_u32(
        &mut out,
        u32::try_from(request.locations.len()).map_err(|_| {
            StorageRpcPayloadError::PayloadTooLarge {
                len: request.locations.len(),
                limit: u32::MAX as usize,
            }
        })?,
    );
    for location in &request.locations {
        put_shard_location(&mut out, *location);
    }
    for shard_key in &request.shard_keys {
        put_bytes(&mut out, shard_key.as_bytes());
    }
    Ok(out)
}

pub(crate) fn validate_read_handle_acquire_request(
    request: &StorageRpcReadHandleAcquireRequest,
) -> Result<(), StorageRpcPayloadError> {
    validate_read_operation_id(&request.read_operation_id)?;
    if request.locations.is_empty() {
        return Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
            "read handle acquire must include at least one shard location",
        ));
    }
    if request.locations.len() != request.shard_keys.len() {
        return Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
            "read handle acquire locations and shard keys must have the same length",
        ));
    }
    validate_read_handle_location_count(request.locations.len())?;
    validate_read_handle_locations(&request.locations)?;
    for (location, shard_key) in request.locations.iter().zip(request.shard_keys.iter()) {
        validate_shard_location_matches_key(location, shard_key)?;
    }
    Ok(())
}

pub(crate) fn decode_read_handle_acquire_request(
    bytes: &[u8],
) -> Result<StorageRpcReadHandleAcquireRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let read_operation_id = decoder.read_string_with_limit(
        STORAGE_RPC_MAX_READ_OPERATION_ID_LEN,
        StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
            "read operation id exceeds maximum length",
        ),
    )?;
    let location_count = decoder.read_u32()? as usize;
    if location_count == 0 {
        return Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
            "read handle acquire must include at least one shard location",
        ));
    }
    validate_read_handle_location_count(location_count)?;
    if location_count
        > decoder.remaining_len()
            / (STORAGE_RPC_SHARD_LOCATION_LEN + STORAGE_RPC_SHARD_KEY_FIELD_LEN)
    {
        return Err(StorageRpcPayloadError::Truncated);
    }
    let mut locations = Vec::with_capacity(location_count);
    for _ in 0..location_count {
        locations.push(decoder.read_shard_location()?);
    }
    let mut shard_keys = Vec::with_capacity(location_count);
    for _ in 0..location_count {
        shard_keys.push(decoder.read_shard_key()?);
    }
    decoder.finish()?;
    let request = StorageRpcReadHandleAcquireRequest {
        read_operation_id,
        locations,
        shard_keys,
    };
    validate_read_handle_acquire_request(&request)?;
    Ok(request)
}

pub(crate) fn encode_read_handle_acquire_response(
    response: &StorageRpcReadHandleAcquireResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if response.locations.is_empty() {
        return Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
            "read handle acquire response must include at least one shard location",
        ));
    }
    validate_read_handle_location_count(response.locations.len())?;
    validate_read_handle_locations(&response.locations)?;
    let mut out = Vec::new();
    put_u32(
        &mut out,
        u32::try_from(response.locations.len()).map_err(|_| {
            StorageRpcPayloadError::PayloadTooLarge {
                len: response.locations.len(),
                limit: u32::MAX as usize,
            }
        })?,
    );
    for location in &response.locations {
        put_shard_location(&mut out, *location);
    }
    Ok(out)
}

pub(crate) fn decode_read_handle_acquire_response(
    bytes: &[u8],
) -> Result<StorageRpcReadHandleAcquireResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let location_count = decoder.read_u32()? as usize;
    if location_count == 0 {
        return Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
            "read handle acquire response must include at least one shard location",
        ));
    }
    if location_count > decoder.remaining_len() / STORAGE_RPC_SHARD_LOCATION_LEN {
        return Err(StorageRpcPayloadError::Truncated);
    }
    let mut locations = Vec::with_capacity(location_count);
    for _ in 0..location_count {
        locations.push(decoder.read_shard_location()?);
    }
    decoder.finish()?;
    validate_read_handle_locations(&locations)?;
    Ok(StorageRpcReadHandleAcquireResponse { locations })
}

pub(crate) fn encode_read_handle_release_request(
    request: &StorageRpcReadHandleReleaseRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_read_handle_release_request(request)?;
    let mut out = Vec::new();
    put_string(&mut out, &request.read_operation_id);
    Ok(out)
}

pub(crate) fn decode_read_handle_release_request(
    bytes: &[u8],
) -> Result<StorageRpcReadHandleReleaseRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let read_operation_id = decoder.read_string_with_limit(
        STORAGE_RPC_MAX_READ_OPERATION_ID_LEN,
        StorageRpcPayloadError::InvalidReadHandleReleaseRequest(
            "read operation id exceeds maximum length",
        ),
    )?;
    decoder.finish()?;
    let request = StorageRpcReadHandleReleaseRequest { read_operation_id };
    validate_read_handle_release_request(&request)?;
    Ok(request)
}

pub(crate) fn validate_read_handle_release_request(
    request: &StorageRpcReadHandleReleaseRequest,
) -> Result<(), StorageRpcPayloadError> {
    validate_read_handle_release_operation_id(&request.read_operation_id)
}

pub(crate) fn encode_read_handle_release_response(
    _response: &StorageRpcReadHandleReleaseResponse,
) -> Vec<u8> {
    Vec::new()
}

pub(crate) fn decode_read_handle_release_response(
    bytes: &[u8],
) -> Result<StorageRpcReadHandleReleaseResponse, StorageRpcPayloadError> {
    let decoder = StorageRpcDecoder::new(bytes);
    decoder.finish()?;
    Ok(StorageRpcReadHandleReleaseResponse)
}

pub(crate) fn encode_claim_heartbeat_request(
    request: &StorageRpcClaimHeartbeatRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_claim_token(&request.token)?;
    if request
        .lease_deadline
        .is_some_and(|lease_deadline| lease_deadline <= request.heartbeat_at)
    {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "claim heartbeat lease deadline must be after heartbeat time",
        ));
    }
    let mut out = Vec::new();
    put_claim_token(&mut out, &request.token);
    put_u64(&mut out, request.heartbeat_at);
    put_optional_u64(&mut out, request.lease_deadline);
    Ok(out)
}

pub(crate) fn decode_claim_heartbeat_request(
    bytes: &[u8],
) -> Result<StorageRpcClaimHeartbeatRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let token = decoder.read_claim_token()?;
    let heartbeat_at = decoder.read_u64()?;
    let lease_deadline = decoder.read_optional_u64()?;
    decoder.finish()?;
    let request = StorageRpcClaimHeartbeatRequest {
        token,
        heartbeat_at,
        lease_deadline,
    };
    encode_claim_heartbeat_request(&request)?;
    Ok(request)
}

pub(crate) fn encode_claim_release_request(
    request: &StorageRpcClaimReleaseRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_claim_token(&request.token)?;
    let mut out = Vec::new();
    put_claim_token(&mut out, &request.token);
    Ok(out)
}

pub(crate) fn decode_claim_release_request(
    bytes: &[u8],
) -> Result<StorageRpcClaimReleaseRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let token = decoder.read_claim_token()?;
    decoder.finish()?;
    validate_claim_token(&token)?;
    Ok(StorageRpcClaimReleaseRequest { token })
}

pub(crate) fn encode_proof_release_request(
    request: &StorageRpcProofReleaseRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_bucket_write_reservation_proof(&request.proof)?;
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.route_cluster_epoch.get());
    put_u32(&mut out, request.pg_id.get());
    put_bucket_write_reservation_proof(&mut out, &request.proof);
    Ok(out)
}

pub(crate) fn decode_proof_release_request(
    bytes: &[u8],
) -> Result<StorageRpcProofReleaseRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let route_cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let proof = decoder.read_bucket_write_reservation_proof()?;
    decoder.finish()?;
    validate_bucket_write_reservation_proof(&proof)?;
    Ok(StorageRpcProofReleaseRequest {
        node_id,
        route_cluster_epoch,
        pg_id,
        proof,
    })
}

pub(crate) fn encode_bucket_write_reservation_acquire_request(
    request: &StorageRpcBucketWriteReservationAcquireRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request
        .effect_deadline
        .is_some_and(|deadline| !admitted_route_effect_deadline_is_conservative(deadline))
    {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "effect deadline is not conservatively delegated",
        ));
    }
    validate_bucket_write_reservation_identity(
        &request.reservation_id,
        &request.owner_token,
        &request.operation_kind,
        request.target_context.as_deref(),
    )?;
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.cluster_epoch.get());
    put_u32(&mut out, request.pg_id.get());
    put_string(&mut out, request.bucket.as_str());
    put_string(&mut out, &request.reservation_id);
    put_string(&mut out, &request.owner_token);
    put_string(&mut out, &request.operation_kind);
    put_u64(&mut out, request.created_at);
    put_u64(&mut out, request.lease_deadline);
    put_optional_string(&mut out, request.target_context.as_deref());
    match request.effect_deadline {
        None => put_u8(&mut out, 0),
        Some(deadline) => {
            put_u8(&mut out, 1);
            put_u64(&mut out, deadline.authority_valid_until_ms);
            put_u64(&mut out, deadline.portable_wall_valid_until_ms);
        }
    }
    Ok(out)
}

pub(crate) fn decode_bucket_write_reservation_acquire_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketWriteReservationAcquireRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let bucket = decoder.read_bucket_name().map_err(|_| {
        StorageRpcPayloadError::InvalidBucketWriteReservationProof("invalid bucket name")
    })?;
    let reservation_id = decoder.read_string_with_limit(
        STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN,
        StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "reservation id exceeds maximum length",
        ),
    )?;
    let owner_token = decoder.read_string_with_limit(
        STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN,
        StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "owner token exceeds maximum length",
        ),
    )?;
    let operation_kind = decoder.read_string_with_limit(
        STORAGE_RPC_MAX_BUCKET_WRITE_OPERATION_KIND_LEN,
        StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "operation kind exceeds maximum length",
        ),
    )?;
    let created_at = decoder.read_u64()?;
    let lease_deadline = decoder.read_u64()?;
    let target_context = decoder.read_optional_string_with_limit(
        STORAGE_RPC_MAX_BUCKET_WRITE_TARGET_CONTEXT_LEN,
        StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "target context exceeds maximum length",
        ),
    )?;
    let effect_deadline = match decoder.read_u8()? {
        0 => None,
        1 => Some(StorageRpcAdmittedRouteEffectDeadline {
            authority_valid_until_ms: decoder.read_u64()?,
            portable_wall_valid_until_ms: decoder.read_u64()?,
        }),
        _ => {
            return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                "invalid optional effect deadline tag",
            ))
        }
    };
    if effect_deadline
        .is_some_and(|deadline| !admitted_route_effect_deadline_is_conservative(deadline))
    {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "effect deadline is not conservatively delegated",
        ));
    }
    decoder.finish()?;
    validate_bucket_write_reservation_identity(
        &reservation_id,
        &owner_token,
        &operation_kind,
        target_context.as_deref(),
    )?;
    Ok(StorageRpcBucketWriteReservationAcquireRequest {
        node_id,
        cluster_epoch,
        pg_id,
        bucket,
        reservation_id,
        owner_token,
        operation_kind,
        created_at,
        lease_deadline,
        target_context,
        effect_deadline,
    })
}

pub(crate) fn encode_bucket_write_reservation_proof_request(
    request: &StorageRpcBucketWriteReservationProofRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    encode_proof_release_request(&StorageRpcProofReleaseRequest {
        node_id: request.node_id,
        route_cluster_epoch: request.route_cluster_epoch,
        pg_id: request.pg_id,
        proof: request.proof.clone(),
    })
}

pub(crate) fn decode_bucket_write_reservation_proof_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketWriteReservationProofRequest, StorageRpcPayloadError> {
    let request = decode_proof_release_request(bytes)?;
    Ok(StorageRpcBucketWriteReservationProofRequest {
        node_id: request.node_id,
        route_cluster_epoch: request.route_cluster_epoch,
        pg_id: request.pg_id,
        proof: request.proof,
    })
}

pub(crate) fn encode_bucket_write_reservation_heartbeat_request(
    request: &StorageRpcBucketWriteReservationHeartbeatRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_bucket_write_reservation_proof(&request.proof)?;
    if request
        .effect_deadline
        .is_some_and(|deadline| !admitted_route_effect_deadline_is_conservative(deadline))
    {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "effect deadline is not conservatively delegated",
        ));
    }
    let mut out = encode_bucket_write_reservation_proof_request(
        &StorageRpcBucketWriteReservationProofRequest {
            node_id: request.node_id,
            route_cluster_epoch: request.route_cluster_epoch,
            pg_id: request.pg_id,
            proof: request.proof.clone(),
        },
    )?;
    put_u64(&mut out, request.lease_deadline);
    put_admitted_route_effect_deadline(&mut out, request.effect_deadline);
    Ok(out)
}

pub(crate) fn decode_bucket_write_reservation_heartbeat_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketWriteReservationHeartbeatRequest, StorageRpcPayloadError> {
    if bytes.len() < 9 {
        return Err(StorageRpcPayloadError::Truncated);
    }
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let route_cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let proof = decoder.read_bucket_write_reservation_proof()?;
    let lease_deadline = decoder.read_u64()?;
    let effect_deadline =
        decoder.read_admitted_route_effect_deadline("bucket write reservation heartbeat")?;
    decoder.finish()?;
    Ok(StorageRpcBucketWriteReservationHeartbeatRequest {
        node_id,
        route_cluster_epoch,
        pg_id,
        proof,
        lease_deadline,
        effect_deadline,
    })
}

pub(crate) fn encode_bucket_write_reservation_record_request(
    request: &StorageRpcBucketWriteReservationRecordRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_bucket_write_reservation_record(&request.record)?;
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.route_cluster_epoch.get());
    put_u32(&mut out, request.pg_id.get());
    put_bucket_write_reservation_record(&mut out, &request.record);
    Ok(out)
}

pub(crate) fn decode_bucket_write_reservation_record_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketWriteReservationRecordRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let route_cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let record = decoder.read_bucket_write_reservation_record()?;
    decoder.finish()?;
    validate_bucket_write_reservation_record(&record)?;
    Ok(StorageRpcBucketWriteReservationRecordRequest {
        node_id,
        route_cluster_epoch,
        pg_id,
        record,
    })
}

pub(crate) fn encode_bucket_write_reservation_record_response(
    response: &StorageRpcBucketWriteReservationRecordResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcBucketWriteReservationAcquireOutcome::Acquired(record) => {
            validate_bucket_write_reservation_record(record)?;
            put_u8(&mut out, 0);
            put_bucket_write_reservation_record(&mut out, record);
        }
        StorageRpcBucketWriteReservationAcquireOutcome::Draining => {
            put_u8(&mut out, 1);
        }
        StorageRpcBucketWriteReservationAcquireOutcome::BucketNotFound { name } => {
            put_u8(&mut out, 2);
            put_string(&mut out, name.as_str());
        }
    }
    Ok(out)
}

pub(crate) fn decode_bucket_write_reservation_record_response(
    bytes: &[u8],
) -> Result<StorageRpcBucketWriteReservationRecordResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => {
            let record = decoder.read_bucket_write_reservation_record()?;
            validate_bucket_write_reservation_record(&record)?;
            StorageRpcBucketWriteReservationAcquireOutcome::Acquired(record)
        }
        1 => StorageRpcBucketWriteReservationAcquireOutcome::Draining,
        2 => StorageRpcBucketWriteReservationAcquireOutcome::BucketNotFound {
            name: decoder.read_bucket_name()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "unknown bucket write reservation acquire outcome tag",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcBucketWriteReservationRecordResponse { outcome })
}

pub(crate) fn encode_bucket_write_drain_begin_request(
    request: &StorageRpcBucketWriteDrainBeginRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request
        .effect_deadline
        .is_some_and(|deadline| !admitted_route_effect_deadline_is_conservative(deadline))
    {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "effect deadline is not conservatively delegated",
        ));
    }
    validate_bucket_write_drain_identity(&request.drain_id, &request.owner_token)?;
    let mut out = encode_bucket_request(&request.bucket);
    put_string(&mut out, &request.drain_id);
    put_string(&mut out, &request.owner_token);
    put_u64(&mut out, request.created_at);
    put_u64(&mut out, request.lease_deadline);
    match request.effect_deadline {
        None => put_u8(&mut out, 0),
        Some(deadline) => {
            put_u8(&mut out, 1);
            put_u64(&mut out, deadline.authority_valid_until_ms);
            put_u64(&mut out, deadline.portable_wall_valid_until_ms);
        }
    }
    Ok(out)
}

pub(crate) fn decode_bucket_write_drain_begin_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketWriteDrainBeginRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let bucket = decoder.read_bucket_request()?;
    let drain_id = decoder.read_string_with_limit(
        STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN,
        StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "drain id exceeds maximum length",
        ),
    )?;
    let owner_token = decoder.read_string_with_limit(
        STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN,
        StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "owner token exceeds maximum length",
        ),
    )?;
    let created_at = decoder.read_u64()?;
    let lease_deadline = decoder.read_u64()?;
    let effect_deadline = match decoder.read_u8()? {
        0 => None,
        1 => Some(StorageRpcAdmittedRouteEffectDeadline {
            authority_valid_until_ms: decoder.read_u64()?,
            portable_wall_valid_until_ms: decoder.read_u64()?,
        }),
        _ => {
            return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                "invalid optional effect deadline tag",
            ));
        }
    };
    decoder.finish()?;
    if effect_deadline
        .is_some_and(|deadline| !admitted_route_effect_deadline_is_conservative(deadline))
    {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "effect deadline is not conservatively delegated",
        ));
    }
    validate_bucket_write_drain_identity(&drain_id, &owner_token)?;
    Ok(StorageRpcBucketWriteDrainBeginRequest {
        bucket,
        drain_id,
        owner_token,
        created_at,
        lease_deadline,
        effect_deadline,
    })
}

pub(crate) fn encode_bucket_write_drain_begin_response(
    response: &StorageRpcBucketWriteDrainBeginResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    match &response.outcome {
        StorageRpcBucketWriteDrainBeginOutcome::Acquired(record) => {
            validate_bucket_write_drain_record(record)?;
            put_u8(&mut out, 0);
            put_bucket_write_drain_record(&mut out, record);
        }
        StorageRpcBucketWriteDrainBeginOutcome::Conflict => put_u8(&mut out, 1),
    }
    Ok(out)
}

pub(crate) fn decode_bucket_write_drain_begin_response(
    bytes: &[u8],
) -> Result<StorageRpcBucketWriteDrainBeginResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => {
            let record = decoder.read_bucket_write_drain_record()?;
            validate_bucket_write_drain_record(&record)?;
            StorageRpcBucketWriteDrainBeginOutcome::Acquired(record)
        }
        1 => StorageRpcBucketWriteDrainBeginOutcome::Conflict,
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "unknown bucket write drain begin outcome tag",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcBucketWriteDrainBeginResponse { outcome })
}

pub(crate) fn encode_bucket_write_drain_record_request(
    request: &StorageRpcBucketWriteDrainRecordRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_bucket_write_drain_record(&request.record)?;
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.route_cluster_epoch.get());
    put_u32(&mut out, request.pg_id.get());
    put_bucket_write_drain_record(&mut out, &request.record);
    Ok(out)
}

pub(crate) fn decode_bucket_write_drain_record_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketWriteDrainRecordRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let route_cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let record = decoder.read_bucket_write_drain_record()?;
    decoder.finish()?;
    validate_bucket_write_drain_record(&record)?;
    Ok(StorageRpcBucketWriteDrainRecordRequest {
        node_id,
        route_cluster_epoch,
        pg_id,
        record,
    })
}

pub(crate) fn encode_bucket_write_drain_heartbeat_request(
    request: &StorageRpcBucketWriteDrainHeartbeatRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_bucket_write_drain_record(&request.record)?;
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.route_cluster_epoch.get());
    put_u32(&mut out, request.pg_id.get());
    put_bucket_write_drain_record(&mut out, &request.record);
    put_u64(&mut out, request.lease_deadline);
    Ok(out)
}

pub(crate) fn decode_bucket_write_drain_heartbeat_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketWriteDrainHeartbeatRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let route_cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let record = decoder.read_bucket_write_drain_record()?;
    let lease_deadline = decoder.read_u64()?;
    decoder.finish()?;
    validate_bucket_write_drain_record(&record)?;
    Ok(StorageRpcBucketWriteDrainHeartbeatRequest {
        node_id,
        route_cluster_epoch,
        pg_id,
        record,
        lease_deadline,
    })
}

pub(crate) fn encode_bucket_write_drain_clear_expired_request(
    request: &StorageRpcBucketWriteDrainClearExpiredRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = encode_bucket_request(&request.bucket);
    put_u64(&mut out, request.now);
    Ok(out)
}

pub(crate) fn decode_bucket_write_drain_clear_expired_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketWriteDrainClearExpiredRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let bucket = decoder.read_bucket_request()?;
    let now = decoder.read_u64()?;
    decoder.finish()?;
    Ok(StorageRpcBucketWriteDrainClearExpiredRequest { bucket, now })
}

pub(crate) fn encode_bucket_write_drain_optional_record_response(
    response: &StorageRpcBucketWriteDrainOptionalRecordResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    match &response.record {
        Some(record) => {
            validate_bucket_write_drain_record(record)?;
            put_u8(&mut out, 1);
            put_bucket_write_drain_record(&mut out, record);
        }
        None => put_u8(&mut out, 0),
    }
    Ok(out)
}

pub(crate) fn decode_bucket_write_drain_optional_record_response(
    bytes: &[u8],
) -> Result<StorageRpcBucketWriteDrainOptionalRecordResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let record = match decoder.read_u8()? {
        0 => None,
        1 => {
            let record = decoder.read_bucket_write_drain_record()?;
            validate_bucket_write_drain_record(&record)?;
            Some(record)
        }
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "unknown optional bucket write drain record tag",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcBucketWriteDrainOptionalRecordResponse { record })
}

pub(crate) fn encode_bucket_delete_attempt_outcome_record_request(
    request: &StorageRpcBucketDeleteAttemptOutcomeRecordRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_bucket_delete_attempt_outcome_record(&request.record)?;
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.cluster_epoch.get());
    put_u32(&mut out, request.pg_id.get());
    put_bucket_delete_attempt_outcome_record(&mut out, &request.record);
    Ok(out)
}

pub(crate) fn decode_bucket_delete_attempt_outcome_record_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketDeleteAttemptOutcomeRecordRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let record = decoder.read_bucket_delete_attempt_outcome_record()?;
    decoder.finish()?;
    validate_bucket_delete_attempt_outcome_record(&record)?;
    Ok(StorageRpcBucketDeleteAttemptOutcomeRecordRequest {
        node_id,
        cluster_epoch,
        pg_id,
        record,
    })
}

pub(crate) fn encode_bucket_delete_attempt_outcome_optional_record_response(
    response: &StorageRpcBucketDeleteAttemptOutcomeOptionalRecordResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    match &response.record {
        Some(record) => {
            validate_bucket_delete_attempt_outcome_record(record)?;
            put_u8(&mut out, 1);
            put_bucket_delete_attempt_outcome_record(&mut out, record);
        }
        None => put_u8(&mut out, 0),
    }
    Ok(out)
}

pub(crate) fn decode_bucket_delete_attempt_outcome_optional_record_response(
    bytes: &[u8],
) -> Result<StorageRpcBucketDeleteAttemptOutcomeOptionalRecordResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let record = match decoder.read_u8()? {
        0 => None,
        1 => {
            let record = decoder.read_bucket_delete_attempt_outcome_record()?;
            validate_bucket_delete_attempt_outcome_record(&record)?;
            Some(record)
        }
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "unknown optional bucket delete attempt outcome record tag",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcBucketDeleteAttemptOutcomeOptionalRecordResponse { record })
}

pub(crate) fn encode_bucket_write_reservations_list_response(
    response: &StorageRpcBucketWriteReservationsListResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    put_u32(
        &mut out,
        u32::try_from(response.records.len()).map_err(|_| {
            StorageRpcPayloadError::PayloadTooLarge {
                len: response.records.len(),
                limit: u32::MAX as usize,
            }
        })?,
    );
    for record in &response.records {
        validate_bucket_write_reservation_record(record)?;
        put_bucket_write_reservation_record(&mut out, record);
    }
    Ok(out)
}

pub(crate) fn decode_bucket_write_reservations_list_response(
    bytes: &[u8],
) -> Result<StorageRpcBucketWriteReservationsListResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let count = decoder.read_bounded_remaining_count(
        STORAGE_RPC_BUCKET_WRITE_RECORD_MAX_LEN.min(1),
        "bucket write reservation count exceeds payload",
    )?;
    let mut records = Vec::new();
    for _ in 0..count {
        let record = decoder.read_bucket_write_reservation_record()?;
        validate_bucket_write_reservation_record(&record)?;
        records.push(record);
    }
    decoder.finish()?;
    Ok(StorageRpcBucketWriteReservationsListResponse { records })
}

pub(crate) fn encode_bucket_pg_request(
    request: &StorageRpcBucketPgRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.cluster_epoch.get());
    put_u32(&mut out, request.pg_id.get());
    Ok(out)
}

pub(crate) fn decode_bucket_pg_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketPgRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let request = decoder.read_bucket_pg_request()?;
    decoder.finish()?;
    Ok(request)
}

pub(crate) fn encode_bucket_delete_finalize_roots_request(
    request: &StorageRpcBucketDeleteFinalizeRootsRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.limit > STORAGE_RPC_MAX_BUCKET_DELETE_FINALIZE_ROOTS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: request.limit,
            limit: STORAGE_RPC_MAX_BUCKET_DELETE_FINALIZE_ROOTS,
        });
    }
    let mut out = encode_bucket_pg_request(&request.route)?;
    put_u64(&mut out, request.now);
    put_u32(
        &mut out,
        u32::try_from(request.limit).map_err(|_| StorageRpcPayloadError::PayloadTooLarge {
            len: request.limit,
            limit: u32::MAX as usize,
        })?,
    );
    Ok(out)
}

pub(crate) fn decode_bucket_delete_finalize_roots_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketDeleteFinalizeRootsRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let now = decoder.read_u64()?;
    let limit = decoder.read_u32()? as usize;
    decoder.finish()?;
    if limit > STORAGE_RPC_MAX_BUCKET_DELETE_FINALIZE_ROOTS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: limit,
            limit: STORAGE_RPC_MAX_BUCKET_DELETE_FINALIZE_ROOTS,
        });
    }
    Ok(StorageRpcBucketDeleteFinalizeRootsRequest { route, now, limit })
}

pub(crate) fn encode_bucket_delete_finalize_roots_response(
    response: &StorageRpcBucketDeleteFinalizeRootsResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if response.roots.len() > STORAGE_RPC_MAX_BUCKET_DELETE_FINALIZE_ROOTS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: response.roots.len(),
            limit: STORAGE_RPC_MAX_BUCKET_DELETE_FINALIZE_ROOTS,
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
        put_bucket_delete_finalize_root(&mut out, root);
    }
    Ok(out)
}

pub(crate) fn decode_bucket_delete_finalize_roots_response(
    bytes: &[u8],
) -> Result<StorageRpcBucketDeleteFinalizeRootsResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let count = decoder.read_bounded_remaining_count(
        STORAGE_RPC_BUCKET_DELETE_FINALIZE_ROOT_MAX_LEN.min(1),
        "bucket delete finalize root count exceeds payload",
    )?;
    if count > STORAGE_RPC_MAX_BUCKET_DELETE_FINALIZE_ROOTS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: count,
            limit: STORAGE_RPC_MAX_BUCKET_DELETE_FINALIZE_ROOTS,
        });
    }
    let mut roots = Vec::new();
    for _ in 0..count {
        roots.push(decoder.read_bucket_delete_finalize_root()?);
    }
    decoder.finish()?;
    Ok(StorageRpcBucketDeleteFinalizeRootsResponse { roots })
}

pub(crate) fn encode_bucket_delete_begin_roots_request(
    request: &StorageRpcBucketDeleteBeginRootsRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.limit > STORAGE_RPC_MAX_BUCKET_DELETE_BEGIN_ROOTS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: request.limit,
            limit: STORAGE_RPC_MAX_BUCKET_DELETE_BEGIN_ROOTS,
        });
    }
    let mut out = encode_bucket_pg_request(&request.route)?;
    put_u64(&mut out, request.now);
    put_optional_string(
        &mut out,
        request.start_after_bucket.as_ref().map(BucketName::as_str),
    );
    put_u32(
        &mut out,
        u32::try_from(request.limit).map_err(|_| StorageRpcPayloadError::PayloadTooLarge {
            len: request.limit,
            limit: u32::MAX as usize,
        })?,
    );
    Ok(out)
}

pub(crate) fn decode_bucket_delete_begin_roots_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketDeleteBeginRootsRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let route = decoder.read_bucket_pg_request()?;
    let now = decoder.read_u64()?;
    let start_after_bucket = decoder
        .read_optional_string_with_limit(
            STORAGE_RPC_MAX_BUCKET_NAME_LEN,
            StorageRpcPayloadError::InvalidDurableClaimToken("bucket name exceeds maximum length"),
        )?
        .map(BucketName::try_from)
        .transpose()
        .map_err(|_| StorageRpcPayloadError::InvalidDurableClaimToken("invalid bucket name"))?;
    let limit = decoder.read_u32()? as usize;
    decoder.finish()?;
    if limit > STORAGE_RPC_MAX_BUCKET_DELETE_BEGIN_ROOTS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: limit,
            limit: STORAGE_RPC_MAX_BUCKET_DELETE_BEGIN_ROOTS,
        });
    }
    Ok(StorageRpcBucketDeleteBeginRootsRequest {
        route,
        now,
        start_after_bucket,
        limit,
    })
}

pub(crate) fn encode_bucket_delete_begin_roots_response(
    response: &StorageRpcBucketDeleteBeginRootsResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if response.roots.len() > STORAGE_RPC_MAX_BUCKET_DELETE_BEGIN_ROOTS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: response.roots.len(),
            limit: STORAGE_RPC_MAX_BUCKET_DELETE_BEGIN_ROOTS,
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
        put_bucket_delete_begin_root(&mut out, root);
    }
    Ok(out)
}

pub(crate) fn decode_bucket_delete_begin_roots_response(
    bytes: &[u8],
) -> Result<StorageRpcBucketDeleteBeginRootsResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let count = decoder.read_bounded_remaining_count(
        STORAGE_RPC_BUCKET_DELETE_BEGIN_ROOT_MAX_LEN.min(1),
        "bucket delete begin root count exceeds payload",
    )?;
    if count > STORAGE_RPC_MAX_BUCKET_DELETE_BEGIN_ROOTS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: count,
            limit: STORAGE_RPC_MAX_BUCKET_DELETE_BEGIN_ROOTS,
        });
    }
    let mut roots = Vec::new();
    for _ in 0..count {
        roots.push(decoder.read_bucket_delete_begin_root()?);
    }
    decoder.finish()?;
    Ok(StorageRpcBucketDeleteBeginRootsResponse { roots })
}

pub(crate) fn encode_bucket_delete_finalize_claim_acquire_request(
    request: &StorageRpcBucketDeleteFinalizeClaimAcquireRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_bucket_write_drain_identity(&request.claim_id, &request.owner_token)?;
    let mut out = encode_bucket_request(&request.bucket);
    put_u64(&mut out, request.bucket_incarnation_generation);
    put_string(&mut out, &request.claim_id);
    put_string(&mut out, &request.owner_token);
    put_u64(&mut out, request.claimed_at);
    put_optional_u64(&mut out, request.lease_deadline);
    put_u64(&mut out, request.now);
    Ok(out)
}

pub(crate) fn decode_bucket_delete_finalize_claim_acquire_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketDeleteFinalizeClaimAcquireRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let bucket = decoder.read_bucket_request()?;
    let bucket_incarnation_generation = decoder.read_u64()?;
    let claim_id = decoder.read_string_with_limit(
        STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN,
        StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "claim id exceeds maximum length",
        ),
    )?;
    let owner_token = decoder.read_string_with_limit(
        STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN,
        StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "owner token exceeds maximum length",
        ),
    )?;
    let claimed_at = decoder.read_u64()?;
    let lease_deadline = decoder.read_optional_u64()?;
    let now = decoder.read_u64()?;
    decoder.finish()?;
    validate_bucket_write_drain_identity(&claim_id, &owner_token)?;
    Ok(StorageRpcBucketDeleteFinalizeClaimAcquireRequest {
        bucket,
        bucket_incarnation_generation,
        claim_id,
        owner_token,
        claimed_at,
        lease_deadline,
        now,
    })
}

pub(crate) fn encode_bucket_delete_finalize_claim_optional_record_response(
    response: &StorageRpcBucketDeleteFinalizeClaimOptionalRecordResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    match &response.record {
        Some(record) => {
            validate_bucket_delete_finalize_claim_record(record)?;
            put_u8(&mut out, 1);
            put_bucket_delete_finalize_claim_record(&mut out, record);
        }
        None => put_u8(&mut out, 0),
    }
    Ok(out)
}

pub(crate) fn decode_bucket_delete_finalize_claim_optional_record_response(
    bytes: &[u8],
) -> Result<StorageRpcBucketDeleteFinalizeClaimOptionalRecordResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let record = match decoder.read_u8()? {
        0 => None,
        1 => {
            let record = decoder.read_bucket_delete_finalize_claim_record()?;
            validate_bucket_delete_finalize_claim_record(&record)?;
            Some(record)
        }
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "unknown optional bucket delete finalize claim record tag",
            ));
        }
    };
    decoder.finish()?;
    Ok(StorageRpcBucketDeleteFinalizeClaimOptionalRecordResponse { record })
}

pub(crate) fn encode_bucket_delete_finalize_claim_record_request(
    request: &StorageRpcBucketDeleteFinalizeClaimRecordRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.cluster_epoch != request.record.cluster_epoch {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "request route epoch must match claim epoch",
        ));
    }
    validate_bucket_delete_finalize_claim_record(&request.record)?;
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.cluster_epoch.get());
    put_u32(&mut out, request.pg_id.get());
    put_bucket_delete_finalize_claim_record(&mut out, &request.record);
    Ok(out)
}

pub(crate) fn decode_bucket_delete_finalize_claim_record_request(
    bytes: &[u8],
) -> Result<StorageRpcBucketDeleteFinalizeClaimRecordRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let record = decoder.read_bucket_delete_finalize_claim_record()?;
    decoder.finish()?;
    if cluster_epoch != record.cluster_epoch {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "request route epoch must match claim epoch",
        ));
    }
    validate_bucket_delete_finalize_claim_record(&record)?;
    Ok(StorageRpcBucketDeleteFinalizeClaimRecordRequest {
        node_id,
        cluster_epoch,
        pg_id,
        record,
    })
}

pub(crate) fn encode_optional_checksum_metadata(checksum: Option<&ChecksumBytes>) -> Vec<u8> {
    let mut out = Vec::new();
    match checksum {
        None => put_u8(&mut out, 0),
        Some(checksum) => {
            put_u8(&mut out, 1);
            put_bytes(&mut out, checksum.as_slice());
        }
    }
    out
}

pub(crate) fn decode_optional_checksum_metadata(
    bytes: &[u8],
) -> Result<Option<ChecksumBytes>, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let tag = decoder.read_u8()?;
    let checksum = match tag {
        0 => None,
        1 => Some(
            ChecksumBytes::new(decoder.read_bytes()?).map_err(invalid_checksum_metadata_error)?,
        ),
        _ => {
            return Err(StorageRpcPayloadError::InvalidChecksumMetadata(
                "invalid optional checksum tag",
            ))
        }
    };
    decoder.finish()?;
    Ok(checksum)
}

fn invalid_checksum_metadata_error(err: checksum::ChecksumBytesError) -> StorageRpcPayloadError {
    StorageRpcPayloadError::InvalidChecksumMetadata(match err {
        checksum::ChecksumBytesError::Empty => "checksum metadata is empty",
        checksum::ChecksumBytesError::TooLong { .. } => "checksum metadata is too large",
    })
}

fn validate_shard_write_payload(
    expected_size: u64,
    expected_crc64: u64,
    payload: &[u8],
) -> Result<(), StorageRpcPayloadError> {
    validate_shard_payload_matches_ack(
        payload,
        WriteAck {
            stored_size: expected_size,
            crc64: expected_crc64,
        },
    )
}

fn validate_shard_payload_matches_ack(
    payload: &[u8],
    expected_ack: WriteAck,
) -> Result<(), StorageRpcPayloadError> {
    let actual_size = payload.len() as u64;
    if actual_size != expected_ack.stored_size {
        return Err(StorageRpcPayloadError::ShardWriteSizeMismatch {
            expected: expected_ack.stored_size,
            actual: actual_size,
        });
    }
    if checksum::crc64::checksum(payload) != expected_ack.crc64 {
        return Err(StorageRpcPayloadError::ShardWriteChecksumMismatch);
    }
    Ok(())
}

fn validate_shard_read_range(
    stored_size: u64,
    offset: u64,
    length: u64,
) -> Result<(), StorageRpcPayloadError> {
    let end = offset
        .checked_add(length)
        .ok_or(StorageRpcPayloadError::ShardWriteSizeMismatch {
            expected: stored_size,
            actual: u64::MAX,
        })?;
    if end > stored_size {
        return Err(StorageRpcPayloadError::ShardWriteSizeMismatch {
            expected: stored_size,
            actual: end,
        });
    }
    Ok(())
}

fn validate_shard_location_matches_key(
    location: &StorageRpcShardLocation,
    shard_key: &ShardKey,
) -> Result<(), StorageRpcPayloadError> {
    if location.shard_index != shard_key.shard_index() {
        return Err(StorageRpcPayloadError::ShardLocationMismatch);
    }
    Ok(())
}

fn validate_read_operation_id(id: &str) -> Result<(), StorageRpcPayloadError> {
    if id.is_empty() {
        return Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
            "read operation id must not be empty",
        ));
    }
    if id.len() > STORAGE_RPC_MAX_READ_OPERATION_ID_LEN {
        return Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
            "read operation id exceeds maximum length",
        ));
    }
    Ok(())
}

fn validate_read_handle_release_operation_id(id: &str) -> Result<(), StorageRpcPayloadError> {
    if id.is_empty() {
        return Err(StorageRpcPayloadError::InvalidReadHandleReleaseRequest(
            "read operation id must not be empty",
        ));
    }
    if id.len() > STORAGE_RPC_MAX_READ_OPERATION_ID_LEN {
        return Err(StorageRpcPayloadError::InvalidReadHandleReleaseRequest(
            "read operation id exceeds maximum length",
        ));
    }
    Ok(())
}

fn validate_read_handle_location_count(
    location_count: usize,
) -> Result<(), StorageRpcPayloadError> {
    if location_count > STORAGE_RPC_MAX_READ_HANDLE_LOCATIONS {
        return Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
            "read handle acquire includes too many shard locations",
        ));
    }
    Ok(())
}

fn validate_read_handle_locations(
    locations: &[StorageRpcShardLocation],
) -> Result<(), StorageRpcPayloadError> {
    for pair in locations.windows(2) {
        if shard_location_sort_key(pair[0]) >= shard_location_sort_key(pair[1]) {
            return Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
                "read handle acquire locations must be sorted and unique",
            ));
        }
    }
    Ok(())
}

fn shard_location_sort_key(location: StorageRpcShardLocation) -> (u64, u32, u8, u32) {
    (
        location.cluster_epoch.get(),
        location.pg_id.get(),
        location.shard_index.get(),
        location.node_id.as_u32(),
    )
}

fn validate_shard_ack_batch(item_count: usize) -> Result<(), StorageRpcPayloadError> {
    if item_count == 0 {
        return Err(StorageRpcPayloadError::InvalidShardAckBatchRequest(
            "shard ack batch must include at least one item",
        ));
    }
    if item_count > STORAGE_RPC_MAX_SHARD_ACK_ITEMS {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: item_count,
            limit: STORAGE_RPC_MAX_SHARD_ACK_ITEMS,
        });
    }
    Ok(())
}

fn validate_claim_token(token: &StorageRpcDurableClaimToken) -> Result<(), StorageRpcPayloadError> {
    let (claim_id, owner_token) = match token {
        StorageRpcDurableClaimToken::ObjectPayloadReclaim(token) => {
            (token.claim_id.as_str(), token.owner_token.as_str())
        }
        StorageRpcDurableClaimToken::BucketDeleteFinalize(token)
        | StorageRpcDurableClaimToken::LifecycleSweep(token) => {
            (token.claim_id.as_str(), token.owner_token.as_str())
        }
    };
    if claim_id.is_empty() {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "claim id must not be empty",
        ));
    }
    if owner_token.is_empty() {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "owner token must not be empty",
        ));
    }
    Ok(())
}

fn validate_placed_segment_shard_repair_claim_identity(
    claim_id: &str,
    owner_token: &str,
) -> Result<(), StorageRpcPayloadError> {
    if claim_id.is_empty() {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "claim id must not be empty",
        ));
    }
    if owner_token.is_empty() {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "owner token must not be empty",
        ));
    }
    if claim_id.len() > PLACED_SEGMENT_SHARD_REPAIR_CLAIM_ID_MAX_LEN {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "claim id exceeds maximum length",
        ));
    }
    if owner_token.len() > PLACED_SEGMENT_SHARD_REPAIR_OWNER_TOKEN_MAX_LEN {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "owner token exceeds maximum length",
        ));
    }
    Ok(())
}

fn validate_placed_segment_shard_repair_work_item(
    work_item: &PlacedSegmentShardRepairWorkItem,
) -> Result<(), StorageRpcPayloadError> {
    let total = work_item
        .request
        .ec
        .k
        .checked_add(work_item.request.ec.m)
        .ok_or(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "placed segment repair EC shard count overflow",
        ))?;
    if work_item.request.ec.k == 0 || work_item.shard_index.get() >= total {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "invalid placed segment shard repair work item",
        ));
    }
    Ok(())
}

fn validate_placed_segment_shard_repair_claim_record(
    record: &PlacedSegmentShardRepairClaimRecord,
) -> Result<(), StorageRpcPayloadError> {
    validate_placed_segment_shard_repair_work_item(&record.work_item)?;
    validate_placed_segment_shard_repair_claim_identity(&record.claim_id, &record.owner_token)?;
    if record.attempt_count == 0 {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "placed segment repair claim attempt count must be nonzero",
        ));
    }
    let Some(lease_deadline) = record.lease_deadline else {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "placed segment repair claim lease deadline is required",
        ));
    };
    if lease_deadline <= record.claimed_at {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "placed segment repair claim lease deadline must be after claimed time",
        ));
    }
    if let Some(last_error) = record.last_error.as_deref() {
        if last_error.len() > PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN {
            return Err(StorageRpcPayloadError::PayloadTooLarge {
                len: last_error.len(),
                limit: PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN,
            });
        }
    }
    Ok(())
}

fn validate_placed_segment_shard_backfill_claim_identity(
    claim_id: &str,
    owner_token: &str,
) -> Result<(), StorageRpcPayloadError> {
    if claim_id.is_empty() {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "claim id must not be empty",
        ));
    }
    if owner_token.is_empty() {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "owner token must not be empty",
        ));
    }
    if claim_id.len() > PLACED_SEGMENT_SHARD_BACKFILL_CLAIM_ID_MAX_LEN {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "claim id exceeds maximum length",
        ));
    }
    if owner_token.len() > PLACED_SEGMENT_SHARD_BACKFILL_OWNER_TOKEN_MAX_LEN {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "owner token exceeds maximum length",
        ));
    }
    Ok(())
}

fn validate_placed_segment_shard_backfill_work_item(
    work_item: &PlacedSegmentShardBackfillWorkItem,
) -> Result<(), StorageRpcPayloadError> {
    work_item
        .request
        .ec
        .k
        .checked_add(work_item.request.ec.m)
        .ok_or(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "placed segment backfill EC shard count overflow",
        ))?;
    if work_item.request.ec.k == 0 {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "invalid placed segment shard backfill work item",
        ));
    }
    if work_item.source_cluster_epoch.get() > work_item.desired_cluster_epoch.get() {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "placed segment shard backfill source epoch must not exceed desired epoch",
        ));
    }
    Ok(())
}

fn validate_placed_segment_shard_backfill_remaining_tolerance(
    work_item: &PlacedSegmentShardBackfillWorkItem,
    remaining_tolerance: u8,
) -> Result<(), StorageRpcPayloadError> {
    if remaining_tolerance > work_item.request.ec.m {
        return Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
            "placed segment shard backfill remaining tolerance exceeds EC m",
        ));
    }
    Ok(())
}

fn validate_placed_segment_shard_backfill_claim_record(
    record: &PlacedSegmentShardBackfillClaimRecord,
) -> Result<(), StorageRpcPayloadError> {
    validate_placed_segment_shard_backfill_work_item(&record.work_item)?;
    validate_placed_segment_shard_backfill_remaining_tolerance(
        &record.work_item,
        record.remaining_tolerance,
    )?;
    validate_placed_segment_shard_backfill_claim_identity(&record.claim_id, &record.owner_token)?;
    if record.attempt_count == 0 {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "placed segment backfill claim attempt count must be nonzero",
        ));
    }
    let Some(lease_deadline) = record.lease_deadline else {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "placed segment backfill claim lease deadline is required",
        ));
    };
    if lease_deadline <= record.claimed_at {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "placed segment backfill claim lease deadline must be after claimed time",
        ));
    }
    if let Some(last_error) = record.last_error.as_deref() {
        if last_error.len() > PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN {
            return Err(StorageRpcPayloadError::PayloadTooLarge {
                len: last_error.len(),
                limit: PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN,
            });
        }
    }
    Ok(())
}

fn validate_lifecycle_sweep_claim_record(
    record: &LifecycleSweepClaimRecord,
) -> Result<(), StorageRpcPayloadError> {
    validate_bucket_write_reservation_identity(
        &record.claim_id,
        &record.owner_token,
        "lifecycle-sweep",
        None,
    )?;
    if record.cluster_epoch.get() == 0 {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "claim epoch must not be zero",
        ));
    }
    Ok(())
}

fn validate_bucket_write_reservation_proof(
    proof: &BucketWriteReservationProof,
) -> Result<(), StorageRpcPayloadError> {
    validate_bucket_write_reservation_identity(
        &proof.reservation_id,
        &proof.owner_token,
        &proof.operation_kind,
        proof.target_context.as_deref(),
    )
}

fn validate_bucket_write_reservation_record(
    record: &BucketWriteReservationRecord,
) -> Result<(), StorageRpcPayloadError> {
    validate_bucket_write_reservation_identity(
        &record.reservation_id,
        &record.owner_token,
        &record.operation_kind,
        record.target_context.as_deref(),
    )
}

fn validate_bucket_write_drain_record(
    record: &BucketWriteDrainRecord,
) -> Result<(), StorageRpcPayloadError> {
    validate_bucket_write_drain_identity(&record.drain_id, &record.owner_token)
}

fn validate_bucket_delete_attempt_outcome_record(
    record: &BucketDeleteAttemptOutcomeRecord,
) -> Result<(), StorageRpcPayloadError> {
    if record.drain_id.is_empty()
        || record.drain_id.len() > STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN
    {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "drain id exceeds maximum length",
        ));
    }
    if record.detail.len() > BUCKET_DELETE_ATTEMPT_OUTCOME_DETAIL_MAX_LEN {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "bucket delete attempt outcome detail exceeds maximum length",
        ));
    }
    if record.cluster_epoch.get() == 0 {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "outcome epoch must not be zero",
        ));
    }
    Ok(())
}

fn validate_bucket_delete_finalize_claim_record(
    record: &BucketDeleteFinalizeClaimRecord,
) -> Result<(), StorageRpcPayloadError> {
    validate_bucket_write_drain_identity(&record.claim_id, &record.owner_token)?;
    if record.last_error.as_ref().is_some_and(|last_error| {
        last_error.len() > STORAGE_RPC_MAX_BUCKET_WRITE_TARGET_CONTEXT_LEN
    }) {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "last error exceeds maximum length",
        ));
    }
    Ok(())
}

fn validate_object_payload_reclaim_claim_record(
    record: &ObjectPayloadReclaimClaimRecord,
) -> Result<(), StorageRpcPayloadError> {
    validate_bucket_write_drain_identity(&record.claim_id, &record.owner_token)?;
    if record.last_error.as_ref().is_some_and(|last_error| {
        last_error.len() > STORAGE_RPC_MAX_BUCKET_WRITE_TARGET_CONTEXT_LEN
    }) {
        return Err(StorageRpcPayloadError::InvalidDurableClaimToken(
            "last error exceeds maximum length",
        ));
    }
    Ok(())
}

fn validate_bucket_write_drain_identity(
    drain_id: &str,
    owner_token: &str,
) -> Result<(), StorageRpcPayloadError> {
    if drain_id.is_empty() {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "drain id must not be empty",
        ));
    }
    if drain_id.len() > STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "drain id exceeds maximum length",
        ));
    }
    if owner_token.is_empty() {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "owner token must not be empty",
        ));
    }
    if owner_token.len() > STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "owner token exceeds maximum length",
        ));
    }
    Ok(())
}

fn validate_bucket_write_reservation_identity(
    reservation_id: &str,
    owner_token: &str,
    operation_kind: &str,
    target_context: Option<&str>,
) -> Result<(), StorageRpcPayloadError> {
    if reservation_id.is_empty() {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "reservation id must not be empty",
        ));
    }
    if reservation_id.len() > STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "reservation id exceeds maximum length",
        ));
    }
    if owner_token.is_empty() {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "owner token must not be empty",
        ));
    }
    if owner_token.len() > STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "owner token exceeds maximum length",
        ));
    }
    if operation_kind.is_empty() {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "operation kind must not be empty",
        ));
    }
    if operation_kind.len() > STORAGE_RPC_MAX_BUCKET_WRITE_OPERATION_KIND_LEN {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "operation kind exceeds maximum length",
        ));
    }
    if target_context
        .is_some_and(|context| context.len() > STORAGE_RPC_MAX_BUCKET_WRITE_TARGET_CONTEXT_LEN)
    {
        return Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
            "target context exceeds maximum length",
        ));
    }
    Ok(())
}

fn storage_rpc_frame_checksum(
    version: u16,
    request_id: u64,
    raw_kind: u16,
    payload_len: u32,
    payload: &[u8],
) -> u64 {
    let mut hasher = checksum::crc64::Hasher::new();
    hasher.update(STORAGE_RPC_FRAME_MAGIC);
    hasher.update(&version.to_le_bytes());
    hasher.update(&request_id.to_le_bytes());
    hasher.update(&raw_kind.to_le_bytes());
    hasher.update(&payload_len.to_le_bytes());
    hasher.update(payload);
    hasher.finalize()
}
