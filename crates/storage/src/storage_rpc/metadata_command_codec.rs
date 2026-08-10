// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

fn put_metadata_command_log_hash(out: &mut Vec<u8>, hash: MetadataCommandLogHash) {
    put_u8(out, hash.encoding_version());
    put_u64(out, hash.value());
}

fn read_metadata_command_log_hash(
    decoder: &mut StorageRpcDecoder<'_>,
) -> Result<MetadataCommandLogHash, StorageRpcPayloadError> {
    MetadataCommandLogHash::from_encoded_parts(decoder.read_u8()?, decoder.read_u64()?)
        .map_err(|_| StorageRpcPayloadError::UnsupportedMetadataProofCarrier("log-hash"))
}

fn put_canonical_state_digest(out: &mut Vec<u8>, digest: CanonicalStateDigest) {
    put_u8(out, digest.encoding_version());
    put_u64(out, digest.value());
}

fn read_canonical_state_digest(
    decoder: &mut StorageRpcDecoder<'_>,
) -> Result<CanonicalStateDigest, StorageRpcPayloadError> {
    CanonicalStateDigest::from_encoded_parts(decoder.read_u8()?, decoder.read_u64()?)
        .map_err(|_| StorageRpcPayloadError::UnsupportedMetadataProofCarrier("state-digest"))
}

pub(crate) fn encode_metadata_command_pending_slot_request(
    request: &StorageRpcMetadataCommandPendingSlotRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request
        .effect_deadline
        .is_some_and(|deadline| !admitted_route_effect_deadline_is_conservative(deadline))
    {
        return Err(
            StorageRpcPayloadError::InvalidMetadataCommandPendingSlotRequest(
                "effect deadline is not conservatively delegated",
            ),
        );
    }
    validate_metadata_command_route(request.cluster_epoch, request.pg_id, request.command.id())?;
    let command_request = StorageRpcMetadataCommandRequest {
        node_id: request.node_id,
        cluster_epoch: request.cluster_epoch,
        pg_id: request.pg_id,
        command: request.command.clone(),
    };
    let mut out = encode_metadata_command_request(&command_request)?;
    match request.scope_bucket.as_ref() {
        None => put_u8(&mut out, 0),
        Some(bucket) => {
            put_u8(&mut out, 1);
            put_string(&mut out, bucket.as_str());
        }
    }
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

pub(crate) fn decode_metadata_command_pending_slot_request(
    bytes: &[u8],
    authority: &MetadataCommandDecodeAuthority,
) -> Result<StorageRpcMetadataCommandPendingSlotRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let item = decoder.read_metadata_command_item()?;
    let command = metadata_command_envelope_from_item(&item, authority)?;
    validate_metadata_command_route(cluster_epoch, pg_id, command.id())?;
    let scope_bucket = match decoder.read_u8()? {
        0 => None,
        1 => Some(decoder.read_bucket_name().map_err(|_| {
            StorageRpcPayloadError::InvalidMetadataCommandPendingSlotRequest(
                "invalid scope bucket name",
            )
        })?),
        _ => {
            return Err(
                StorageRpcPayloadError::InvalidMetadataCommandPendingSlotRequest(
                    "invalid optional scope bucket tag",
                ),
            )
        }
    };
    let effect_deadline = match decoder.read_u8()? {
        0 => None,
        1 => Some(StorageRpcAdmittedRouteEffectDeadline {
            authority_valid_until_ms: decoder.read_u64()?,
            portable_wall_valid_until_ms: decoder.read_u64()?,
        }),
        _ => {
            return Err(
                StorageRpcPayloadError::InvalidMetadataCommandPendingSlotRequest(
                    "invalid optional effect deadline tag",
                ),
            )
        }
    };
    if effect_deadline
        .is_some_and(|deadline| !admitted_route_effect_deadline_is_conservative(deadline))
    {
        return Err(
            StorageRpcPayloadError::InvalidMetadataCommandPendingSlotRequest(
                "effect deadline is not conservatively delegated",
            ),
        );
    }
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandPendingSlotRequest {
        node_id,
        cluster_epoch,
        pg_id,
        command,
        scope_bucket,
        effect_deadline,
    })
}

pub(crate) fn encode_metadata_command_pending_slot_replace_request(
    request: &StorageRpcMetadataCommandPendingSlotReplaceRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate_metadata_command_route(request.cluster_epoch, request.pg_id, request.previous.id())?;
    validate_metadata_command_route(
        request.cluster_epoch,
        request.pg_id,
        request.replacement.id(),
    )?;
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.cluster_epoch.get());
    put_u32(&mut out, request.pg_id.get());
    let previous = StorageRpcMetadataCommandItem {
        command_checksum: request.previous.checksum_crc64(),
        command_bytes: request.previous.command_bytes(),
    };
    out.extend_from_slice(&encode_metadata_command_item(&previous)?);
    let replacement = StorageRpcMetadataCommandItem {
        command_checksum: request.replacement.checksum_crc64(),
        command_bytes: request.replacement.command_bytes(),
    };
    out.extend_from_slice(&encode_metadata_command_item(&replacement)?);
    match request.scope_bucket.as_ref() {
        None => put_u8(&mut out, 0),
        Some(bucket) => {
            put_u8(&mut out, 1);
            put_string(&mut out, bucket.as_str());
        }
    }
    Ok(out)
}

pub(crate) fn decode_metadata_command_pending_slot_replace_request(
    bytes: &[u8],
    authority: &MetadataCommandDecodeAuthority,
) -> Result<StorageRpcMetadataCommandPendingSlotReplaceRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let previous_item = decoder.read_metadata_command_item()?;
    let previous = metadata_command_envelope_from_item(&previous_item, authority)?;
    validate_metadata_command_route(cluster_epoch, pg_id, previous.id())?;
    let replacement_item = decoder.read_metadata_command_item()?;
    let replacement = metadata_command_envelope_from_item(&replacement_item, authority)?;
    validate_metadata_command_route(cluster_epoch, pg_id, replacement.id())?;
    let scope_bucket = match decoder.read_u8()? {
        0 => None,
        1 => Some(decoder.read_bucket_name().map_err(|_| {
            StorageRpcPayloadError::InvalidMetadataCommandPendingSlotRequest(
                "invalid scope bucket name",
            )
        })?),
        _ => {
            return Err(
                StorageRpcPayloadError::InvalidMetadataCommandPendingSlotRequest(
                    "invalid optional scope bucket tag",
                ),
            )
        }
    };
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandPendingSlotReplaceRequest {
        node_id,
        cluster_epoch,
        pg_id,
        previous,
        replacement,
        scope_bucket,
    })
}

pub(crate) fn encode_metadata_command_recovery_pending_slot_replace_request(
    request: &StorageRpcMetadataCommandRecoveryPendingSlotReplaceRequest,
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
    let ordinary = StorageRpcMetadataCommandPendingSlotReplaceRequest {
        node_id: request.node_id,
        cluster_epoch: request.cluster_epoch,
        pg_id: request.pg_id,
        previous: request.previous.clone(),
        replacement: request.replacement.clone(),
        scope_bucket: request.scope_bucket.clone(),
    };
    let mut out = Vec::new();
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
    out.extend_from_slice(&encode_metadata_command_pending_slot_replace_request(
        &ordinary,
    )?);
    Ok(out)
}

pub(crate) fn decode_metadata_command_recovery_pending_slot_replace_request(
    bytes: &[u8],
    authority: &MetadataCommandDecodeAuthority,
) -> Result<StorageRpcMetadataCommandRecoveryPendingSlotReplaceRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let authorized_source =
        metadata_command_envelope_from_item(&decoder.read_metadata_command_item()?, authority)?;
    let abandoned_source = match decoder.read_u8()? {
        0 => None,
        1 => Some(metadata_command_envelope_from_item(
            &decoder.read_metadata_command_item()?,
            authority,
        )?),
        _ => {
            return Err(
                StorageRpcPayloadError::InvalidMetadataCommandPendingSlotRequest(
                    "invalid optional abandoned recovery source tag",
                ),
            );
        }
    };
    let ordinary_offset = bytes.len() - decoder.remaining_len();
    let ordinary = decode_metadata_command_pending_slot_replace_request(
        &bytes[ordinary_offset..],
        authority,
    )?;
    validate_metadata_command_route(
        ordinary.cluster_epoch,
        ordinary.pg_id,
        authorized_source.id(),
    )?;
    if let Some(abandoned_source) = abandoned_source.as_ref() {
        validate_metadata_command_route(
            ordinary.cluster_epoch,
            ordinary.pg_id,
            abandoned_source.id(),
        )?;
    }
    Ok(StorageRpcMetadataCommandRecoveryPendingSlotReplaceRequest {
        node_id: ordinary.node_id,
        cluster_epoch: ordinary.cluster_epoch,
        pg_id: ordinary.pg_id,
        authorized_source,
        abandoned_source,
        previous: ordinary.previous,
        replacement: ordinary.replacement,
        scope_bucket: ordinary.scope_bucket,
    })
}

pub(crate) fn encode_metadata_command_pending_slot_insert_response(
    response: &StorageRpcMetadataCommandPendingSlotInsertResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match response.outcome {
        StorageRpcMetadataCommandPendingSlotInsertOutcome::Inserted => put_u8(&mut out, 0),
        StorageRpcMetadataCommandPendingSlotInsertOutcome::PendingConflict {
            pg_id,
            cluster_epoch,
            existing_log_index,
            candidate_log_index,
        } => {
            put_u8(&mut out, 1);
            put_u32(&mut out, pg_id);
            put_u64(&mut out, cluster_epoch.get());
            put_u64(&mut out, existing_log_index);
            put_u64(&mut out, candidate_log_index);
        }
        StorageRpcMetadataCommandPendingSlotInsertOutcome::LogConflict {
            node_id,
            pg_id,
            cluster_epoch,
            log_index,
        } => {
            put_u8(&mut out, 2);
            put_u32(&mut out, node_id);
            put_u32(&mut out, pg_id);
            put_u64(&mut out, cluster_epoch.get());
            put_u64(&mut out, log_index);
        }
    }
    out
}

pub(crate) fn decode_metadata_command_pending_slot_insert_response(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandPendingSlotInsertResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => StorageRpcMetadataCommandPendingSlotInsertOutcome::Inserted,
        1 => StorageRpcMetadataCommandPendingSlotInsertOutcome::PendingConflict {
            pg_id: decoder.read_u32()?,
            cluster_epoch: decoder.read_cluster_epoch()?,
            existing_log_index: decoder.read_u64()?,
            candidate_log_index: decoder.read_u64()?,
        },
        2 => StorageRpcMetadataCommandPendingSlotInsertOutcome::LogConflict {
            node_id: decoder.read_u32()?,
            pg_id: decoder.read_u32()?,
            cluster_epoch: decoder.read_cluster_epoch()?,
            log_index: decoder.read_u64()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "unknown metadata command pending slot insert outcome tag",
            ))
        }
    };
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandPendingSlotInsertResponse { outcome })
}

pub(crate) fn encode_metadata_command_pending_slot_remove_response(
    response: &StorageRpcMetadataCommandPendingSlotRemoveResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_u8(&mut out, u8::from(response.removed));
    out
}

pub(crate) fn decode_metadata_command_pending_slot_remove_response(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandPendingSlotRemoveResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let removed = match decoder.read_u8()? {
        0 => false,
        1 => true,
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "invalid metadata command pending slot remove outcome tag",
            ))
        }
    };
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandPendingSlotRemoveResponse { removed })
}

pub(crate) fn encode_metadata_command_next_id_request(
    request: &StorageRpcMetadataCommandNextIdRequest,
) -> Vec<u8> {
    let mut out = encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
        node_id: request.node_id,
        cluster_epoch: request.cluster_epoch,
        pg_id: request.pg_id,
    });
    put_u64(&mut out, request.min_log_index);
    out
}

pub(crate) fn decode_metadata_command_next_id_request(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandNextIdRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let min_log_index = decoder.read_u64()?;
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandNextIdRequest {
        node_id,
        cluster_epoch,
        pg_id,
        min_log_index,
    })
}

pub(crate) fn encode_metadata_command_log_hash_range_request(
    request: &StorageRpcMetadataCommandLogHashRangeRequest,
) -> Vec<u8> {
    let mut out = encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
        node_id: request.node_id,
        cluster_epoch: request.cluster_epoch,
        pg_id: request.pg_id,
    });
    put_u64(&mut out, request.first_log_index.get());
    put_u64(&mut out, request.last_log_index.get());
    out
}

pub(crate) fn decode_metadata_command_log_hash_range_request(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandLogHashRangeRequest, StorageRpcPayloadError> {
    decode_metadata_command_log_range_request_with_limit(
        bytes,
        STORAGE_RPC_MAX_METADATA_COMMAND_LOG_HASH_RANGE_ENTRIES,
        "metadata command hash range",
    )
}

pub(crate) fn decode_metadata_command_log_entry_range_request(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandLogHashRangeRequest, StorageRpcPayloadError> {
    decode_metadata_command_log_range_request_with_limit(
        bytes,
        STORAGE_RPC_MAX_METADATA_COMMAND_LOG_ENTRY_RANGE_ENTRIES,
        "metadata command entry range",
    )
}

fn decode_metadata_command_log_range_request_with_limit(
    bytes: &[u8],
    max_entries: u64,
    context: &'static str,
) -> Result<StorageRpcMetadataCommandLogHashRangeRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let first_log_index = MetadataCommandLogIndex::new(decoder.read_u64()?).ok_or({
        StorageRpcPayloadError::InvalidResponseEnvelope(
            "metadata command hash range first log index must not be zero",
        )
    })?;
    let last_log_index = MetadataCommandLogIndex::new(decoder.read_u64()?).ok_or({
        StorageRpcPayloadError::InvalidResponseEnvelope(
            "metadata command hash range last log index must not be zero",
        )
    })?;
    let (ordered_message, too_large_message) = match context {
        "metadata command entry range" => (
            "metadata command entry range must be ordered",
            "metadata command entry range is too large",
        ),
        _ => (
            "metadata command hash range must be ordered",
            "metadata command hash range is too large",
        ),
    };
    if last_log_index.get() < first_log_index.get() {
        return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
            ordered_message,
        ));
    }
    let requested = last_log_index.get() - first_log_index.get() + 1;
    if requested > max_entries {
        return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
            too_large_message,
        ));
    }
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandLogHashRangeRequest {
        node_id,
        cluster_epoch,
        pg_id,
        first_log_index,
        last_log_index,
    })
}

pub(crate) fn encode_metadata_command_max_log_index_response(
    response: &StorageRpcMetadataCommandMaxLogIndexResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_u64(&mut out, response.max_log_index);
    out
}

pub(crate) fn decode_metadata_command_max_log_index_response(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandMaxLogIndexResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let max_log_index = decoder.read_u64()?;
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandMaxLogIndexResponse { max_log_index })
}

pub(crate) fn encode_metadata_command_log_hash_range_response(
    response: &StorageRpcMetadataCommandLogHashRangeResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if response.entries.len() > STORAGE_RPC_MAX_METADATA_COMMAND_LOG_HASH_RANGE_ENTRIES as usize {
        return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
            "metadata command hash range response is too large",
        ));
    }
    let mut out = Vec::new();
    put_u32(
        &mut out,
        u32::try_from(response.entries.len()).expect("bounded response count fits u32"),
    );
    for entry in &response.entries {
        put_u64(&mut out, entry.log_index);
        put_metadata_command_log_hash(&mut out, entry.previous_log_hash);
        put_metadata_command_log_hash(&mut out, entry.log_hash);
    }
    Ok(out)
}

pub(crate) fn decode_metadata_command_log_hash_range_response(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandLogHashRangeResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let count = decoder.read_u32()?;
    if u64::from(count) > STORAGE_RPC_MAX_METADATA_COMMAND_LOG_HASH_RANGE_ENTRIES {
        return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
            "metadata command hash range response is too large",
        ));
    }
    let mut entries = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let log_index = decoder.read_u64()?;
        if MetadataCommandLogIndex::new(log_index).is_none() {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "metadata command hash range response log index must not be zero",
            ));
        }
        entries.push(MetadataCommandLogHashRangeEntry {
            log_index,
            previous_log_hash: read_metadata_command_log_hash(&mut decoder)?,
            log_hash: read_metadata_command_log_hash(&mut decoder)?,
        });
    }
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandLogHashRangeResponse { entries })
}

pub(crate) fn encode_metadata_command_log_entry_range_response(
    response: &StorageRpcMetadataCommandLogEntryRangeResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if response.entries.len() > STORAGE_RPC_MAX_METADATA_COMMAND_LOG_ENTRY_RANGE_ENTRIES as usize {
        return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
            "metadata command entry range response is too large",
        ));
    }
    let mut out = Vec::new();
    put_u32(
        &mut out,
        u32::try_from(response.entries.len()).expect("bounded response count fits u32"),
    );
    for entry in &response.entries {
        if MetadataCommandLogIndex::new(entry.log_index).is_none() {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "metadata command entry range response log index must not be zero",
            ));
        }
        put_u64(&mut out, entry.log_index);
        put_metadata_command_log_hash(&mut out, entry.previous_log_hash);
        put_metadata_command_log_hash(&mut out, entry.log_hash);
        match entry.pre_state_digest {
            Some(pre_state_digest) => {
                put_u8(&mut out, 1);
                put_canonical_state_digest(&mut out, pre_state_digest);
            }
            None => put_u8(&mut out, 0),
        }
        match entry.post_state_digest {
            Some(post_state_digest) => {
                put_u8(&mut out, 1);
                put_canonical_state_digest(&mut out, post_state_digest);
            }
            None => put_u8(&mut out, 0),
        }
        match &entry.kind {
            MetadataCommandLogRangeEntryKind::Applied(command) => {
                if command.id().log_index().get() != entry.log_index {
                    return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                        "metadata command entry range applied command log index mismatch",
                    ));
                }
                put_u8(&mut out, 0);
                let item = StorageRpcMetadataCommandItem {
                    command_checksum: command.checksum_crc64(),
                    command_bytes: command.command_bytes(),
                };
                out.extend_from_slice(&encode_metadata_command_item(&item)?);
            }
            MetadataCommandLogRangeEntryKind::Abandoned {
                original_command_checksum,
            } => {
                put_u8(&mut out, 1);
                put_u64(&mut out, *original_command_checksum);
            }
        }
    }
    Ok(out)
}

pub(crate) fn decode_metadata_command_log_entry_range_response(
    bytes: &[u8],
    authority: &MetadataCommandDecodeAuthority,
) -> Result<StorageRpcMetadataCommandLogEntryRangeResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let count = decoder.read_u32()?;
    if u64::from(count) > STORAGE_RPC_MAX_METADATA_COMMAND_LOG_ENTRY_RANGE_ENTRIES {
        return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
            "metadata command entry range response is too large",
        ));
    }
    let mut entries = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let log_index = decoder.read_u64()?;
        if MetadataCommandLogIndex::new(log_index).is_none() {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "metadata command entry range response log index must not be zero",
            ));
        }
        let previous_log_hash = read_metadata_command_log_hash(&mut decoder)?;
        let log_hash = read_metadata_command_log_hash(&mut decoder)?;
        let pre_state_digest = match decoder.read_u8()? {
            0 => None,
            1 => Some(read_canonical_state_digest(&mut decoder)?),
            _ => {
                return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                    "metadata command entry range pre-state digest flag is invalid",
                ));
            }
        };
        let post_state_digest = match decoder.read_u8()? {
            0 => None,
            1 => Some(read_canonical_state_digest(&mut decoder)?),
            _ => {
                return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                    "metadata command entry range post-state digest flag is invalid",
                ));
            }
        };
        let kind = match decoder.read_u8()? {
            0 => {
                let item = decoder.read_metadata_command_item()?;
                let command = metadata_command_envelope_from_item(&item, authority)?;
                if command.id().log_index().get() != log_index {
                    return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                        "metadata command entry range applied command log index mismatch",
                    ));
                }
                MetadataCommandLogRangeEntryKind::Applied(Box::new(command))
            }
            1 => MetadataCommandLogRangeEntryKind::Abandoned {
                original_command_checksum: decoder.read_u64()?,
            },
            _ => {
                return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                    "unknown metadata command entry range kind",
                ));
            }
        };
        entries.push(MetadataCommandLogRangeEntry {
            log_index,
            previous_log_hash,
            log_hash,
            pre_state_digest,
            post_state_digest,
            kind,
        });
    }
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandLogEntryRangeResponse { entries })
}

pub(crate) fn encode_metadata_command_next_id_response(
    response: &StorageRpcMetadataCommandNextIdResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match response.outcome {
        StorageRpcMetadataCommandNextIdOutcome::Allocated {
            cluster_epoch,
            pg_id,
            log_index,
        } => {
            put_u8(&mut out, 0);
            put_u64(&mut out, cluster_epoch.get());
            put_u32(&mut out, pg_id.get());
            put_u64(&mut out, log_index);
        }
        StorageRpcMetadataCommandNextIdOutcome::LogConflict {
            node_id,
            pg_id,
            cluster_epoch,
            log_index,
        } => {
            put_u8(&mut out, 1);
            put_u32(&mut out, node_id);
            put_u32(&mut out, pg_id);
            put_u64(&mut out, cluster_epoch.get());
            put_u64(&mut out, log_index);
        }
    }
    out
}

pub(crate) fn decode_metadata_command_next_id_response(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandNextIdResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => StorageRpcMetadataCommandNextIdOutcome::Allocated {
            cluster_epoch: decoder.read_cluster_epoch()?,
            pg_id: PgId::new(decoder.read_u32()?),
            log_index: decoder.read_u64()?,
        },
        1 => StorageRpcMetadataCommandNextIdOutcome::LogConflict {
            node_id: decoder.read_u32()?,
            pg_id: decoder.read_u32()?,
            cluster_epoch: decoder.read_cluster_epoch()?,
            log_index: decoder.read_u64()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "unknown metadata command next id outcome tag",
            ))
        }
    };
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandNextIdResponse { outcome })
}

pub(crate) fn encode_metadata_command_pending_envelope_response(
    response: &StorageRpcMetadataCommandPendingEnvelopeResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match response.command.as_ref() {
        None => put_u8(&mut out, 0),
        Some(command) => {
            put_u8(&mut out, 1);
            put_u64(&mut out, command.checksum_crc64());
            put_bytes(&mut out, &command.command_bytes());
        }
    }
    out
}

pub(crate) fn decode_metadata_command_pending_envelope_response(
    bytes: &[u8],
    authority: &MetadataCommandDecodeAuthority,
) -> Result<StorageRpcMetadataCommandPendingEnvelopeResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let command = match decoder.read_u8()? {
        0 => None,
        1 => {
            let item = decoder.read_metadata_command_item()?;
            Some(metadata_command_envelope_from_item(&item, authority)?)
        }
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "invalid metadata command pending envelope tag",
            ))
        }
    };
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandPendingEnvelopeResponse { command })
}

pub(crate) fn encode_metadata_command_matching_applied_request(
    request: &StorageRpcMetadataCommandMatchingAppliedRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let command_request = StorageRpcMetadataCommandRequest {
        node_id: request.node_id,
        cluster_epoch: request.cluster_epoch,
        pg_id: request.pg_id,
        command: request.command.clone(),
    };
    let mut out = encode_metadata_command_request(&command_request)?;
    put_u64(&mut out, request.expected_previous_log_hash);
    Ok(out)
}

pub(crate) fn decode_metadata_command_matching_applied_request(
    bytes: &[u8],
    authority: &MetadataCommandDecodeAuthority,
) -> Result<StorageRpcMetadataCommandMatchingAppliedRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let item = decoder.read_metadata_command_item()?;
    let command = metadata_command_envelope_from_item(&item, authority)?;
    validate_metadata_command_route(cluster_epoch, pg_id, command.id())?;
    let expected_previous_log_hash = decoder.read_u64()?;
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandMatchingAppliedRequest {
        node_id,
        cluster_epoch,
        pg_id,
        command,
        expected_previous_log_hash,
    })
}

pub(crate) fn encode_metadata_command_applied_hashes_response(
    response: &StorageRpcMetadataCommandAppliedHashesResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match response.outcome {
        StorageRpcMetadataCommandAppliedHashesOutcome::Hashes(None) => put_u8(&mut out, 0),
        StorageRpcMetadataCommandAppliedHashesOutcome::Hashes(Some((
            previous_log_hash,
            log_hash,
        ))) => {
            put_u8(&mut out, 1);
            put_u64(&mut out, previous_log_hash);
            put_u64(&mut out, log_hash);
        }
        StorageRpcMetadataCommandAppliedHashesOutcome::LogConflict {
            node_id,
            pg_id,
            cluster_epoch,
            log_index,
        } => {
            put_u8(&mut out, 2);
            put_u32(&mut out, node_id);
            put_u32(&mut out, pg_id);
            put_u64(&mut out, cluster_epoch.get());
            put_u64(&mut out, log_index);
        }
    }
    out
}

pub(crate) fn decode_metadata_command_applied_hashes_response(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandAppliedHashesResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => StorageRpcMetadataCommandAppliedHashesOutcome::Hashes(None),
        1 => StorageRpcMetadataCommandAppliedHashesOutcome::Hashes(Some((
            decoder.read_u64()?,
            decoder.read_u64()?,
        ))),
        2 => StorageRpcMetadataCommandAppliedHashesOutcome::LogConflict {
            node_id: decoder.read_u32()?,
            pg_id: decoder.read_u32()?,
            cluster_epoch: decoder.read_cluster_epoch()?,
            log_index: decoder.read_u64()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "unknown metadata command applied hashes outcome tag",
            ))
        }
    };
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandAppliedHashesResponse { outcome })
}

pub(crate) fn encode_metadata_command_bool_response(
    response: &StorageRpcMetadataCommandBoolResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_u8(&mut out, u8::from(response.value));
    out
}

pub(crate) fn decode_metadata_command_bool_response(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandBoolResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let value = match decoder.read_u8()? {
        0 => false,
        1 => true,
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "invalid metadata command bool response tag",
            ))
        }
    };
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandBoolResponse { value })
}

pub(crate) fn encode_metadata_command_bool_outcome_response(
    response: &StorageRpcMetadataCommandBoolOutcomeResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match response.outcome {
        StorageRpcMetadataCommandBoolOutcome::Value(value) => {
            put_u8(&mut out, 0);
            put_u8(&mut out, u8::from(value));
        }
        StorageRpcMetadataCommandBoolOutcome::LogConflict {
            node_id,
            pg_id,
            cluster_epoch,
            log_index,
        } => {
            put_u8(&mut out, 1);
            put_u32(&mut out, node_id);
            put_u32(&mut out, pg_id);
            put_u64(&mut out, cluster_epoch.get());
            put_u64(&mut out, log_index);
        }
    }
    out
}

pub(crate) fn decode_metadata_command_bool_outcome_response(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandBoolOutcomeResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => {
            let value = match decoder.read_u8()? {
                0 => false,
                1 => true,
                _ => {
                    return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                        "invalid metadata command bool outcome value tag",
                    ))
                }
            };
            StorageRpcMetadataCommandBoolOutcome::Value(value)
        }
        1 => StorageRpcMetadataCommandBoolOutcome::LogConflict {
            node_id: decoder.read_u32()?,
            pg_id: decoder.read_u32()?,
            cluster_epoch: decoder.read_cluster_epoch()?,
            log_index: decoder.read_u64()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "unknown metadata command bool outcome tag",
            ))
        }
    };
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandBoolOutcomeResponse { outcome })
}

pub(crate) fn encode_metadata_command_state_outcome_response(
    response: &StorageRpcMetadataCommandStateOutcomeResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match response.outcome {
        StorageRpcMetadataCommandStateOutcome::State(state) => {
            put_u8(&mut out, 0);
            put_u64(&mut out, state.cluster_epoch.get());
            put_u64(&mut out, state.applied_log_index);
            put_u8(&mut out, state.applied_log_hash.encoding_version());
            put_u64(&mut out, state.applied_log_hash.value());
            put_u8(&mut out, state.state_digest.encoding_version());
            put_u64(&mut out, state.state_digest.value());
        }
        StorageRpcMetadataCommandStateOutcome::LogConflict {
            node_id,
            pg_id,
            cluster_epoch,
            log_index,
        } => {
            put_u8(&mut out, 1);
            put_u32(&mut out, node_id);
            put_u32(&mut out, pg_id);
            put_u64(&mut out, cluster_epoch.get());
            put_u64(&mut out, log_index);
        }
        StorageRpcMetadataCommandStateOutcome::ObjectGenerationReservationConflict {
            ref reservation_id,
            generation_id,
        } => {
            put_u8(&mut out, 2);
            put_string(&mut out, reservation_id.as_str());
            put_u64(&mut out, generation_id.get());
        }
        StorageRpcMetadataCommandStateOutcome::ObjectVersionReservationConflict { version_id } => {
            put_u8(&mut out, 3);
            put_u64(&mut out, version_id.to_u64());
        }
        StorageRpcMetadataCommandStateOutcome::StaleBucketMetadataCommand {
            ref name,
            bucket_execution_generation,
        } => {
            put_u8(&mut out, 4);
            put_string(&mut out, name.as_str());
            put_u64(&mut out, bucket_execution_generation);
        }
        StorageRpcMetadataCommandStateOutcome::StaleObjectWriteCommand {
            ref bucket,
            ref key,
            write_sequence,
            generation_id,
        } => {
            put_u8(&mut out, 5);
            put_string(&mut out, bucket.as_str());
            put_string(&mut out, key.as_str());
            put_u64(&mut out, write_sequence);
            match generation_id {
                Some(generation_id) => {
                    put_u8(&mut out, 1);
                    put_u64(&mut out, generation_id.get());
                }
                None => put_u8(&mut out, 0),
            }
        }
        StorageRpcMetadataCommandStateOutcome::StreamSegmentConflict { segment_index } => {
            put_u8(&mut out, 6);
            put_u32(&mut out, segment_index);
        }
    }
    out
}

pub(crate) fn decode_metadata_command_state_outcome_response(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandStateOutcomeResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        0 => {
            let cluster_epoch = decoder.read_cluster_epoch()?;
            let applied_log_index = decoder.read_u64()?;
            let applied_log_hash = MetadataCommandLogHash::from_encoded_parts(
                decoder.read_u8()?,
                decoder.read_u64()?,
            )
            .map_err(|_| {
                StorageRpcPayloadError::UnsupportedMetadataProofCarrier("log-hash")
            })?;
            let state_digest = CanonicalStateDigest::from_encoded_parts(
                decoder.read_u8()?,
                decoder.read_u64()?,
            )
            .map_err(|_| {
                StorageRpcPayloadError::UnsupportedMetadataProofCarrier("state-digest")
            })?;
            StorageRpcMetadataCommandStateOutcome::State(MetadataCommandReplicaState {
                cluster_epoch,
                applied_log_index,
                applied_log_hash,
                state_digest,
            })
        }
        1 => StorageRpcMetadataCommandStateOutcome::LogConflict {
            node_id: decoder.read_u32()?,
            pg_id: decoder.read_u32()?,
            cluster_epoch: decoder.read_cluster_epoch()?,
            log_index: decoder.read_u64()?,
        },
        2 => StorageRpcMetadataCommandStateOutcome::ObjectGenerationReservationConflict {
            reservation_id: decoder.read_session_id()?,
            generation_id: decoder.read_generation_id()?,
        },
        3 => StorageRpcMetadataCommandStateOutcome::ObjectVersionReservationConflict {
            version_id: VersionId::from_u64(decoder.read_u64()?),
        },
        4 => StorageRpcMetadataCommandStateOutcome::StaleBucketMetadataCommand {
            name: decoder.read_bucket_name()?,
            bucket_execution_generation: decoder.read_u64()?,
        },
        5 => {
            let bucket = decoder.read_bucket_name()?;
            let key = decoder.read_object_key()?;
            let write_sequence = decoder.read_u64()?;
            let generation_id = match decoder.read_u8()? {
                0 => None,
                1 => Some(decoder.read_generation_id()?),
                _ => {
                    return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                        "invalid stale object write command generation presence tag",
                    ));
                }
            };
            StorageRpcMetadataCommandStateOutcome::StaleObjectWriteCommand {
                bucket,
                key,
                write_sequence,
                generation_id,
            }
        }
        6 => StorageRpcMetadataCommandStateOutcome::StreamSegmentConflict {
            segment_index: decoder.read_u32()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "unknown metadata command state outcome tag",
            ))
        }
    };
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandStateOutcomeResponse { outcome })
}

pub(crate) fn encode_metadata_command_state_request(
    request: &StorageRpcMetadataCommandStateRequest,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.cluster_epoch.get());
    put_u32(&mut out, request.pg_id.get());
    out
}

pub(crate) fn decode_metadata_command_state_request(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandStateRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandStateRequest {
        node_id,
        cluster_epoch,
        pg_id,
    })
}

pub(crate) fn encode_metadata_command_transfer_adopt_request(
    request: &StorageRpcMetadataCommandTransferAdoptRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
        node_id: request.node_id,
        cluster_epoch: request.cluster_epoch,
        pg_id: request.pg_id,
    });
    put_canonical_state_digest(&mut out, request.expected_state_digest);
    put_u32(
        &mut out,
        u32::try_from(request.commands.len()).map_err(|_| {
            StorageRpcPayloadError::InvalidResponseEnvelope(
                "metadata command transfer adopt request command count exceeds u32",
            )
        })?,
    );
    for transfer_command in &request.commands {
        let command = &transfer_command.command;
        validate_metadata_command_route(request.cluster_epoch, request.pg_id, command.id())?;
        put_canonical_state_digest(&mut out, transfer_command.pre_state_digest);
        put_canonical_state_digest(&mut out, transfer_command.post_state_digest);
        let item = StorageRpcMetadataCommandItem {
            command_checksum: command.checksum_crc64(),
            command_bytes: command.command_bytes(),
        };
        out.extend_from_slice(&encode_metadata_command_item(&item)?);
    }
    Ok(out)
}

pub(crate) fn decode_metadata_command_transfer_adopt_request(
    bytes: &[u8],
    authority: &MetadataCommandDecodeAuthority,
) -> Result<StorageRpcMetadataCommandTransferAdoptRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let expected_state_digest = read_canonical_state_digest(&mut decoder)?;
    let count = decoder.read_u32()?;
    let mut commands = Vec::new();
    for _ in 0..count {
        let pre_state_digest = read_canonical_state_digest(&mut decoder)?;
        let post_state_digest = read_canonical_state_digest(&mut decoder)?;
        let item = decoder.read_metadata_command_item()?;
        let command = metadata_command_envelope_from_item(&item, authority)?;
        validate_metadata_command_route(cluster_epoch, pg_id, command.id())?;
        commands.push(MetadataTransferCommand {
            command,
            pre_state_digest,
            post_state_digest,
        });
    }
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandTransferAdoptRequest {
        node_id,
        cluster_epoch,
        pg_id,
        expected_state_digest,
        commands,
    })
}

pub(crate) fn encode_metadata_command_transfer_empty_state_request(
    request: &StorageRpcMetadataCommandTransferEmptyStateRequest,
) -> Vec<u8> {
    let mut out = encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
        node_id: request.node_id,
        cluster_epoch: request.cluster_epoch,
        pg_id: request.pg_id,
    });
    put_canonical_state_digest(&mut out, request.expected_state_digest);
    out
}

pub(crate) fn decode_metadata_command_transfer_empty_state_request(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandTransferEmptyStateRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let expected_state_digest = read_canonical_state_digest(&mut decoder)?;
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandTransferEmptyStateRequest {
        node_id,
        cluster_epoch,
        pg_id,
        expected_state_digest,
    })
}

pub(crate) fn encode_metadata_command_transfer_matching_state_request(
    request: &StorageRpcMetadataCommandTransferMatchingStateRequest,
) -> Vec<u8> {
    let mut out = encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
        node_id: request.node_id,
        cluster_epoch: request.cluster_epoch,
        pg_id: request.pg_id,
    });
    put_u64(&mut out, request.applied_log_index);
    put_metadata_command_log_hash(&mut out, request.applied_log_hash);
    put_canonical_state_digest(&mut out, request.expected_state_digest);
    out
}

pub(crate) fn decode_metadata_command_transfer_matching_state_request(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandTransferMatchingStateRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let applied_log_index = decoder.read_u64()?;
    let applied_log_hash = read_metadata_command_log_hash(&mut decoder)?;
    let expected_state_digest = read_canonical_state_digest(&mut decoder)?;
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandTransferMatchingStateRequest {
        node_id,
        cluster_epoch,
        pg_id,
        applied_log_index,
        applied_log_hash,
        expected_state_digest,
    })
}

pub(crate) fn encode_metadata_command_transfer_checkpoint_base_request(
    request: &StorageRpcMetadataCommandTransferCheckpointBaseRequest,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
        node_id: request.node_id,
        cluster_epoch: request.cluster_epoch,
        pg_id: request.pg_id,
    });
    encode_metadata_command_checkpoint(&mut out, &request.checkpoint)?;
    Ok(out)
}

pub(crate) fn decode_metadata_command_transfer_checkpoint_base_request(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandTransferCheckpointBaseRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let checkpoint = decode_metadata_command_checkpoint(&mut decoder)?;
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandTransferCheckpointBaseRequest {
        node_id,
        cluster_epoch,
        pg_id,
        checkpoint,
    })
}

pub(crate) fn encode_metadata_command_checkpoint_response(
    response: &StorageRpcMetadataCommandCheckpointResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    encode_metadata_command_checkpoint(&mut out, &response.checkpoint)?;
    Ok(out)
}

pub(crate) fn decode_metadata_command_checkpoint_response(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandCheckpointResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let checkpoint = decode_metadata_command_checkpoint(&mut decoder)?;
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandCheckpointResponse { checkpoint })
}

pub(crate) fn encode_metadata_command_checkpoint_candidates_request(
    request: &StorageRpcMetadataCommandCheckpointCandidatesRequest,
) -> Vec<u8> {
    let mut out = encode_metadata_command_state_request(&StorageRpcMetadataCommandStateRequest {
        node_id: request.node_id,
        cluster_epoch: request.cluster_epoch,
        pg_id: request.pg_id,
    });
    put_u64(&mut out, request.max_applied_log_index);
    put_u32(&mut out, request.limit);
    out
}

pub(crate) fn decode_metadata_command_checkpoint_candidates_request(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandCheckpointCandidatesRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let max_applied_log_index = decoder.read_u64()?;
    let limit = decoder.read_u32()?;
    if usize::try_from(limit)
        .map(|limit| limit > STORAGE_RPC_MAX_METADATA_COMMAND_CHECKPOINT_CANDIDATES)
        .unwrap_or(true)
    {
        return Err(StorageRpcPayloadError::InvalidCount {
            field: "metadata command checkpoint candidate limit",
            count: u64::from(limit),
            max: STORAGE_RPC_MAX_METADATA_COMMAND_CHECKPOINT_CANDIDATES as u64,
        });
    }
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandCheckpointCandidatesRequest {
        node_id,
        cluster_epoch,
        pg_id,
        max_applied_log_index,
        limit,
    })
}

pub(crate) fn encode_metadata_command_checkpoint_candidates_response(
    response: &StorageRpcMetadataCommandCheckpointCandidatesResponse,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if response.checkpoints.len() > STORAGE_RPC_MAX_METADATA_COMMAND_CHECKPOINT_CANDIDATES {
        return Err(StorageRpcPayloadError::InvalidCount {
            field: "metadata command checkpoint candidate count",
            count: response.checkpoints.len() as u64,
            max: STORAGE_RPC_MAX_METADATA_COMMAND_CHECKPOINT_CANDIDATES as u64,
        });
    }
    let mut out = Vec::new();
    put_u32(&mut out, checked_u32_len(response.checkpoints.len())?);
    for checkpoint in &response.checkpoints {
        encode_metadata_command_checkpoint(&mut out, checkpoint)?;
    }
    Ok(out)
}

pub(crate) fn decode_metadata_command_checkpoint_candidates_response(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandCheckpointCandidatesResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let count = decoder.read_u32()?;
    if usize::try_from(count)
        .map(|count| count > STORAGE_RPC_MAX_METADATA_COMMAND_CHECKPOINT_CANDIDATES)
        .unwrap_or(true)
    {
        return Err(StorageRpcPayloadError::InvalidCount {
            field: "metadata command checkpoint candidate count",
            count: u64::from(count),
            max: STORAGE_RPC_MAX_METADATA_COMMAND_CHECKPOINT_CANDIDATES as u64,
        });
    }
    let mut checkpoints = Vec::with_capacity(count as usize);
    for _ in 0..count {
        checkpoints.push(decode_metadata_command_checkpoint(&mut decoder)?);
    }
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandCheckpointCandidatesResponse { checkpoints })
}

pub(crate) fn encode_metadata_command_log_compact_response(
    response: &StorageRpcMetadataCommandLogCompactResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match response.status {
        MetadataCommandLogCompactionStatus::NoCheckpoint { retained_entries } => {
            out.push(0);
            put_u64(&mut out, retained_entries);
        }
        MetadataCommandLogCompactionStatus::PendingCommand { retained_entries } => {
            out.push(1);
            put_u64(&mut out, retained_entries);
        }
        MetadataCommandLogCompactionStatus::Compacted {
            deleted_entries,
            compacted_before,
        } => {
            out.push(2);
            put_u64(&mut out, deleted_entries);
            put_u64(&mut out, compacted_before);
        }
    }
    out
}

pub(crate) fn decode_metadata_command_log_compact_response(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandLogCompactResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let tag = decoder.read_u8()?;
    let status = match tag {
        0 => MetadataCommandLogCompactionStatus::NoCheckpoint {
            retained_entries: decoder.read_u64()?,
        },
        1 => MetadataCommandLogCompactionStatus::PendingCommand {
            retained_entries: decoder.read_u64()?,
        },
        2 => MetadataCommandLogCompactionStatus::Compacted {
            deleted_entries: decoder.read_u64()?,
            compacted_before: decoder.read_u64()?,
        },
        _ => return Err(StorageRpcPayloadError::InvalidMetadataCommandLogCompactionStatus(tag)),
    };
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandLogCompactResponse { status })
}

pub(crate) fn encode_cluster_map_history_reference_summary_request(
    request: &StorageRpcClusterMapHistoryReferenceSummaryRequest,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_u32(&mut out, request.node_id.as_u32());
    put_u64(&mut out, request.cluster_epoch.get());
    out
}

pub(crate) fn decode_cluster_map_history_reference_summary_request(
    bytes: &[u8],
) -> Result<StorageRpcClusterMapHistoryReferenceSummaryRequest, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let node_id = NodeId::new(decoder.read_u32()?);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    decoder.finish()?;
    Ok(StorageRpcClusterMapHistoryReferenceSummaryRequest {
        node_id,
        cluster_epoch,
    })
}

pub(crate) fn encode_cluster_map_history_reference_summary_response(
    response: &StorageRpcClusterMapHistoryReferenceSummaryResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_u32(
        &mut out,
        u32::try_from(response.references.len())
            .expect("bounded history route reference count fits u32"),
    );
    for reference in response.references.iter() {
        out.push(match reference.kind() {
            PgClusterMapHistoryRouteReferenceKind::LivePlacement => 1,
            PgClusterMapHistoryRouteReferenceKind::DurableBackfillSource => 2,
            PgClusterMapHistoryRouteReferenceKind::DurableBackfillDesired => 3,
            PgClusterMapHistoryRouteReferenceKind::PendingMetadataCommand => 4,
            PgClusterMapHistoryRouteReferenceKind::ObjectPayloadReclaimClaim => 5,
        });
        put_u64(&mut out, reference.cluster_epoch().get());
        put_u32(&mut out, reference.pg_id().get());
    }
    out
}

pub(crate) fn decode_cluster_map_history_reference_summary_response(
    bytes: &[u8],
) -> Result<StorageRpcClusterMapHistoryReferenceSummaryResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let count = decoder.read_u32()? as usize;
    if count > MAX_PG_CLUSTER_MAP_HISTORY_ROUTE_REFERENCES {
        return Err(StorageRpcPayloadError::InvalidCount {
            field: "cluster-map history route references",
            count: count as u64,
            max: MAX_PG_CLUSTER_MAP_HISTORY_ROUTE_REFERENCES as u64,
        });
    }
    let mut references = PgClusterMapHistoryRouteReferences::default();
    let mut previous_reference = None;
    for _ in 0..count {
        let kind = match decoder.read_u8()? {
            1 => PgClusterMapHistoryRouteReferenceKind::LivePlacement,
            2 => PgClusterMapHistoryRouteReferenceKind::DurableBackfillSource,
            3 => PgClusterMapHistoryRouteReferenceKind::DurableBackfillDesired,
            4 => PgClusterMapHistoryRouteReferenceKind::PendingMetadataCommand,
            5 => PgClusterMapHistoryRouteReferenceKind::ObjectPayloadReclaimClaim,
            _ => {
                return Err(
                    StorageRpcPayloadError::InvalidClusterMapHistoryRouteReference(
                        "unknown reference kind",
                    ),
                )
            }
        };
        let cluster_epoch = ClusterEpoch::new(decoder.read_u64()?).ok_or(
            StorageRpcPayloadError::InvalidClusterMapHistoryRouteReference(
                "cluster epoch must not be zero",
            ),
        )?;
        let reference = PgClusterMapHistoryRouteReference::new(
            kind,
            cluster_epoch,
            PgId::new(decoder.read_u32()?),
        );
        if previous_reference.is_some_and(|previous| reference <= previous) {
            return Err(
                StorageRpcPayloadError::InvalidClusterMapHistoryRouteReference(
                    "references are not strictly ordered",
                ),
            );
        }
        references
            .insert(reference)
            .map_err(|_| StorageRpcPayloadError::InvalidCount {
                field: "cluster-map history route references",
                count: (references.len() + 1) as u64,
                max: MAX_PG_CLUSTER_MAP_HISTORY_ROUTE_REFERENCES as u64,
            })?;
        previous_reference = Some(reference);
    }
    decoder.finish()?;
    Ok(StorageRpcClusterMapHistoryReferenceSummaryResponse { references })
}

fn decode_optional_cluster_epoch(
    value: Option<u64>,
) -> Result<Option<ClusterEpoch>, StorageRpcPayloadError> {
    value
        .map(|value| {
            ClusterEpoch::new(value).ok_or(StorageRpcPayloadError::InvalidDurableClaimToken(
                "cluster epoch must not be zero",
            ))
        })
        .transpose()
}

pub(crate) fn encode_metadata_command_checkpoint_payload(
    checkpoint: &MetadataCommandCheckpoint,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let mut out = Vec::new();
    encode_metadata_command_checkpoint(&mut out, checkpoint)?;
    Ok(out)
}

pub(crate) fn decode_metadata_command_checkpoint_payload(
    bytes: &[u8],
) -> Result<MetadataCommandCheckpoint, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let checkpoint = decode_metadata_command_checkpoint(&mut decoder)?;
    decoder.finish()?;
    Ok(checkpoint)
}

fn encode_metadata_command_checkpoint(
    out: &mut Vec<u8>,
    checkpoint: &MetadataCommandCheckpoint,
) -> Result<(), StorageRpcPayloadError> {
    out.extend_from_slice(METADATA_COMMAND_CHECKPOINT_MAGIC);
    out.extend_from_slice(&METADATA_COMMAND_CHECKPOINT_ENCODING_VERSION.to_be_bytes());
    put_u64(out, checkpoint.cluster_epoch.get());
    put_u32(out, checkpoint.pg_id.get());
    put_u64(out, checkpoint.applied_log_index);
    put_u8(out, checkpoint.applied_log_hash.encoding_version());
    put_u64(out, checkpoint.applied_log_hash.value());
    put_u8(out, checkpoint.state_digest.encoding_version());
    put_u64(out, checkpoint.state_digest.value());
    put_u32(out, checked_u32_len(checkpoint.table_digests.len())?);
    for digest in &checkpoint.table_digests {
        put_string(out, &digest.table_name);
        put_u64(out, digest.row_count);
        put_u64(out, digest.row_hash_xor);
        put_u64(out, digest.row_hash_sum);
        put_u64(out, digest.table_digest);
    }
    put_u32(out, checked_u32_len(checkpoint.table_blocks.len())?);
    for block in &checkpoint.table_blocks {
        encode_metadata_checkpoint_table_block(out, block)?;
    }
    put_u64(out, checkpoint.checkpoint_crc64);
    Ok(())
}

fn decode_metadata_command_checkpoint(
    decoder: &mut StorageRpcDecoder<'_>,
) -> Result<MetadataCommandCheckpoint, StorageRpcPayloadError> {
    if decoder.read_exact(METADATA_COMMAND_CHECKPOINT_MAGIC.len())?
        != METADATA_COMMAND_CHECKPOINT_MAGIC
    {
        return Err(StorageRpcPayloadError::UnknownMetadataCheckpointMagic);
    }
    let checkpoint_version = u16::from_be_bytes(
        decoder
            .read_exact(2)?
            .try_into()
            .expect("two checkpoint-version bytes were read"),
    );
    if checkpoint_version != METADATA_COMMAND_CHECKPOINT_ENCODING_VERSION {
        return Err(
            StorageRpcPayloadError::UnsupportedMetadataCheckpointEncodingVersion {
                actual: checkpoint_version,
            },
        );
    }
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let pg_id = PgId::new(decoder.read_u32()?);
    let applied_log_index = decoder.read_u64()?;
    let applied_log_hash = MetadataCommandLogHash::from_encoded_parts(
        decoder.read_u8()?,
        decoder.read_u64()?,
    )
    .map_err(|_| StorageRpcPayloadError::UnsupportedMetadataProofCarrier("log-hash"))?;
    let state_digest =
        CanonicalStateDigest::from_encoded_parts(decoder.read_u8()?, decoder.read_u64()?)
            .map_err(|_| {
                StorageRpcPayloadError::UnsupportedMetadataProofCarrier("state-digest")
            })?;
    let table_digest_count =
        decoder.read_count_with_limit(STORAGE_RPC_MAX_METADATA_CHECKPOINT_TABLES)?;
    let mut table_digests = Vec::with_capacity(table_digest_count);
    for _ in 0..table_digest_count {
        table_digests.push(MetadataCheckpointTableDigest {
            table_name: decoder.read_string()?,
            row_count: decoder.read_u64()?,
            row_hash_xor: decoder.read_u64()?,
            row_hash_sum: decoder.read_u64()?,
            table_digest: decoder.read_u64()?,
        });
    }
    let table_block_count =
        decoder.read_count_with_limit(STORAGE_RPC_MAX_METADATA_CHECKPOINT_TABLES)?;
    let mut table_blocks = Vec::with_capacity(table_block_count);
    for _ in 0..table_block_count {
        table_blocks.push(decode_metadata_checkpoint_table_block(decoder)?);
    }
    let checkpoint_crc64 = decoder.read_u64()?;
    Ok(MetadataCommandCheckpoint {
        cluster_epoch,
        pg_id,
        applied_log_index,
        applied_log_hash,
        state_digest,
        table_digests,
        table_blocks,
        checkpoint_crc64,
    })
}

fn encode_metadata_checkpoint_table_block(
    out: &mut Vec<u8>,
    block: &MetadataCheckpointTableBlock,
) -> Result<(), StorageRpcPayloadError> {
    put_string(out, &block.table_name);
    put_string_vec(out, &block.columns)?;
    put_string_vec(out, &block.order_columns)?;
    put_string(out, &block.filter);
    put_u32(out, checked_u32_len(block.rows.len())?);
    for row in &block.rows {
        encode_metadata_checkpoint_row(out, row)?;
    }
    put_u64(out, block.row_count);
    put_u64(out, block.row_hash_xor);
    put_u64(out, block.row_hash_sum);
    put_u64(out, block.table_digest);
    Ok(())
}

fn decode_metadata_checkpoint_table_block(
    decoder: &mut StorageRpcDecoder<'_>,
) -> Result<MetadataCheckpointTableBlock, StorageRpcPayloadError> {
    let table_name = decoder.read_string()?;
    let columns =
        decoder.read_string_vec_with_limit(STORAGE_RPC_MAX_METADATA_CHECKPOINT_COLUMNS)?;
    let order_columns =
        decoder.read_string_vec_with_limit(STORAGE_RPC_MAX_METADATA_CHECKPOINT_COLUMNS)?;
    let filter = decoder.read_string()?;
    let row_count = decoder.read_count_with_limit(STORAGE_RPC_MAX_METADATA_CHECKPOINT_ROWS)?;
    let mut rows = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        rows.push(decode_metadata_checkpoint_row(decoder)?);
    }
    Ok(MetadataCheckpointTableBlock {
        table_name,
        columns,
        order_columns,
        filter,
        rows,
        row_count: decoder.read_u64()?,
        row_hash_xor: decoder.read_u64()?,
        row_hash_sum: decoder.read_u64()?,
        table_digest: decoder.read_u64()?,
    })
}

fn encode_metadata_checkpoint_row(
    out: &mut Vec<u8>,
    row: &MetadataCheckpointRow,
) -> Result<(), StorageRpcPayloadError> {
    put_u32(out, checked_u32_len(row.values.len())?);
    for value in &row.values {
        encode_metadata_checkpoint_value(out, value)?;
    }
    put_u64(out, row.row_digest);
    Ok(())
}

fn decode_metadata_checkpoint_row(
    decoder: &mut StorageRpcDecoder<'_>,
) -> Result<MetadataCheckpointRow, StorageRpcPayloadError> {
    let value_count =
        decoder.read_count_with_limit(STORAGE_RPC_MAX_METADATA_CHECKPOINT_ROW_VALUES)?;
    let mut values = Vec::with_capacity(value_count);
    for _ in 0..value_count {
        values.push(decode_metadata_checkpoint_value(decoder)?);
    }
    Ok(MetadataCheckpointRow {
        values,
        row_digest: decoder.read_u64()?,
    })
}

fn encode_metadata_checkpoint_value(
    out: &mut Vec<u8>,
    value: &MetadataCheckpointValue,
) -> Result<(), StorageRpcPayloadError> {
    match value {
        MetadataCheckpointValue::Null => put_u8(out, 0),
        MetadataCheckpointValue::Integer(value) => {
            put_u8(out, 1);
            put_u64(out, *value as u64);
        }
        MetadataCheckpointValue::RealBits(value) => {
            put_u8(out, 2);
            put_u64(out, *value);
        }
        MetadataCheckpointValue::Text(value) => {
            put_u8(out, 3);
            if value.len() > STORAGE_RPC_MAX_METADATA_CHECKPOINT_VALUE_BYTES_LEN {
                return Err(StorageRpcPayloadError::PayloadTooLarge {
                    len: value.len(),
                    limit: STORAGE_RPC_MAX_METADATA_CHECKPOINT_VALUE_BYTES_LEN,
                });
            }
            put_bytes(out, value);
        }
        MetadataCheckpointValue::Blob(value) => {
            put_u8(out, 4);
            if value.len() > STORAGE_RPC_MAX_METADATA_CHECKPOINT_VALUE_BYTES_LEN {
                return Err(StorageRpcPayloadError::PayloadTooLarge {
                    len: value.len(),
                    limit: STORAGE_RPC_MAX_METADATA_CHECKPOINT_VALUE_BYTES_LEN,
                });
            }
            put_bytes(out, value);
        }
    }
    Ok(())
}

fn decode_metadata_checkpoint_value(
    decoder: &mut StorageRpcDecoder<'_>,
) -> Result<MetadataCheckpointValue, StorageRpcPayloadError> {
    match decoder.read_u8()? {
        0 => Ok(MetadataCheckpointValue::Null),
        1 => Ok(MetadataCheckpointValue::Integer(decoder.read_u64()? as i64)),
        2 => Ok(MetadataCheckpointValue::RealBits(decoder.read_u64()?)),
        3 => Ok(MetadataCheckpointValue::Text(
            decoder
                .read_bytes_with_payload_limit(STORAGE_RPC_MAX_METADATA_CHECKPOINT_VALUE_BYTES_LEN)?
                .to_vec(),
        )),
        4 => Ok(MetadataCheckpointValue::Blob(
            decoder
                .read_bytes_with_payload_limit(STORAGE_RPC_MAX_METADATA_CHECKPOINT_VALUE_BYTES_LEN)?
                .to_vec(),
        )),
        _ => Err(StorageRpcPayloadError::InvalidResponseEnvelope(
            "invalid metadata checkpoint value tag",
        )),
    }
}

pub(crate) fn encode_metadata_command_state_response(
    response: &StorageRpcMetadataCommandStateResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    put_u64(&mut out, response.state.cluster_epoch.get());
    put_u64(&mut out, response.state.applied_log_index);
    put_u8(
        &mut out,
        response.state.applied_log_hash.encoding_version(),
    );
    put_u64(&mut out, response.state.applied_log_hash.value());
    put_u8(&mut out, response.state.state_digest.encoding_version());
    put_u64(&mut out, response.state.state_digest.value());
    out
}

pub(crate) fn decode_metadata_command_state_response(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandStateResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let cluster_epoch = decoder.read_cluster_epoch()?;
    let applied_log_index = decoder.read_u64()?;
    let applied_log_hash = MetadataCommandLogHash::from_encoded_parts(
        decoder.read_u8()?,
        decoder.read_u64()?,
    )
    .map_err(|_| StorageRpcPayloadError::UnsupportedMetadataProofCarrier("log-hash"))?;
    let state_digest =
        CanonicalStateDigest::from_encoded_parts(decoder.read_u8()?, decoder.read_u64()?)
            .map_err(|_| {
                StorageRpcPayloadError::UnsupportedMetadataProofCarrier("state-digest")
            })?;
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandStateResponse {
        state: MetadataCommandReplicaState {
            cluster_epoch,
            applied_log_index,
            applied_log_hash,
            state_digest,
        },
    })
}

pub(crate) fn encode_metadata_command_acceptance_response(
    response: &StorageRpcMetadataCommandAcceptanceResponse,
) -> Vec<u8> {
    let mut out = Vec::new();
    match response.outcome {
        StorageRpcMetadataCommandAcceptanceOutcome::Acceptance(
            MetadataCommandAcceptance::Apply,
        ) => put_u8(&mut out, 1),
        StorageRpcMetadataCommandAcceptanceOutcome::Acceptance(
            MetadataCommandAcceptance::AlreadyApplied,
        ) => put_u8(&mut out, 2),
        StorageRpcMetadataCommandAcceptanceOutcome::LogConflict {
            node_id,
            pg_id,
            cluster_epoch,
            log_index,
        } => {
            put_u8(&mut out, 3);
            put_u32(&mut out, node_id);
            put_u32(&mut out, pg_id);
            put_u64(&mut out, cluster_epoch.get());
            put_u64(&mut out, log_index);
        }
    }
    out
}

pub(crate) fn decode_metadata_command_acceptance_response(
    bytes: &[u8],
) -> Result<StorageRpcMetadataCommandAcceptanceResponse, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    let outcome = match decoder.read_u8()? {
        1 => {
            StorageRpcMetadataCommandAcceptanceOutcome::Acceptance(MetadataCommandAcceptance::Apply)
        }
        2 => StorageRpcMetadataCommandAcceptanceOutcome::Acceptance(
            MetadataCommandAcceptance::AlreadyApplied,
        ),
        3 => StorageRpcMetadataCommandAcceptanceOutcome::LogConflict {
            node_id: decoder.read_u32()?,
            pg_id: decoder.read_u32()?,
            cluster_epoch: decoder.read_cluster_epoch()?,
            log_index: decoder.read_u64()?,
        },
        _ => {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "unknown metadata command acceptance tag",
            ))
        }
    };
    decoder.finish()?;
    Ok(StorageRpcMetadataCommandAcceptanceResponse { outcome })
}

fn validate_metadata_command_route(
    cluster_epoch: ClusterEpoch,
    pg_id: PgId,
    command_id: crate::metadata_command::MetadataCommandId,
) -> Result<(), StorageRpcPayloadError> {
    if command_id.cluster_epoch() != cluster_epoch {
        return Err(StorageRpcPayloadError::MetadataCommandRouteMismatch(
            "command epoch does not match RPC route",
        ));
    }
    if command_id.pg_id() != pg_id {
        return Err(StorageRpcPayloadError::MetadataCommandRouteMismatch(
            "command PG does not match RPC route",
        ));
    }
    Ok(())
}
