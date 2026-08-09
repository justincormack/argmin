#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn metadata_command_decode_authority_for_test() -> MetadataCommandDecodeAuthority {
        MetadataCommandDecodeAuthority::new_for_test()
    }

    struct FlushFailureWriter;

    impl Write for FlushFailureWriter {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            Ok(buffer.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "injected frame flush failure",
            ))
        }
    }

    use crate::{
        metadata_command::{
            AdvanceMultipartCompletionBarrierCommand, CreateBucketCommand,
            MarkBucketDeletingCommand, MetadataCommandEnvelope, MetadataCommandId,
            MetadataCommandLogIndex, MetadataCommandPayload,
        },
        types::{
            AclGrants, BucketDeleteAttemptOutcomeKind, BucketDeleteAttemptOutcomeRecord,
            BucketDeleteFinalizeClaimRecord, BucketObjectLockConfig, BucketVersioningState,
            BucketWriteDrainRecord, BucketWriteDrainState, CanonicalUserId, ClusterEpoch,
            CreateBucketConfig, GenerationId, ObjectKey, PgId, SessionId,
        },
    };

    #[test]
    fn storage_rpc_object_tags_require_current_canonical_xml() {
        let noncanonical =
            "<Tagging><TagSet><Tag><Key>key</Key><Value>value</Value></Tag></TagSet></Tagging>";
        let mut encoded = Vec::new();
        put_optional_string(&mut encoded, Some(noncanonical));
        assert!(matches!(
            StorageRpcDecoder::new(&encoded).read_optional_serialized_tag_set(),
            Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "invalid canonical object tags"
            ))
        ));

        let canonical = s3_types::TagSet::from_pairs(
            vec![("key".to_string(), "value".to_string())],
            s3_types::MAX_OBJECT_TAGS,
        )
        .unwrap()
        .to_xml();
        let mut encoded = Vec::new();
        put_optional_string(&mut encoded, Some(&canonical));
        let decoded = StorageRpcDecoder::new(&encoded)
            .read_optional_serialized_tag_set()
            .unwrap()
            .unwrap();
        assert_eq!(
            decoded.tag_set().clone().into_pairs(),
            vec![("key".into(), "value".into())]
        );
    }

    #[test]
    fn storage_rpc_bucket_tags_require_current_canonical_xml() {
        let noncanonical =
            "<Tagging><TagSet><Tag><Key>key</Key><Value>value</Value></Tag></TagSet></Tagging>";
        let mut encoded = Vec::new();
        put_u8(&mut encoded, 1);
        put_bucket_subresource_kind(&mut encoded, BucketSubresourceKind::Tagging);
        put_string(&mut encoded, noncanonical);
        put_bucket_subresource_aux(&mut encoded, BucketSubresourceAux::None);
        assert!(matches!(
            StorageRpcDecoder::new(&encoded).read_bucket_subresource_mutation(),
            Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
                "invalid bucket tags"
            ))
        ));

        let canonical = s3_types::TagSet::from_pairs(
            vec![("key".to_string(), "value".to_string())],
            s3_types::MAX_BUCKET_TAGS,
        )
        .unwrap()
        .to_xml();
        let mut encoded = Vec::new();
        put_u8(&mut encoded, 1);
        put_bucket_subresource_kind(&mut encoded, BucketSubresourceKind::Tagging);
        put_string(&mut encoded, &canonical);
        put_bucket_subresource_aux(&mut encoded, BucketSubresourceAux::None);
        let decoded = StorageRpcDecoder::new(&encoded)
            .read_bucket_subresource_mutation()
            .unwrap();
        assert!(
            matches!(decoded, BucketSubresourceMutation::PutTagging(tags) if tags.tag_set().clone().into_pairs() == vec![("key".into(), "value".into())])
        );
    }

    #[test]
    fn storage_rpc_acl_grants_require_current_canonical_representation() {
        for noncanonical in [
            "group:all_users:READ",
            "group:all_users:READ\ngroup:all_users:READ\n",
        ] {
            let mut encoded = Vec::new();
            put_string(&mut encoded, noncanonical);
            assert!(matches!(
                StorageRpcDecoder::new(&encoded).read_acl_grants(),
                Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
                    "invalid ACL grants"
                ))
            ));
        }

        let canonical = "group:all_users:READ\n";
        let mut encoded = Vec::new();
        put_string(&mut encoded, canonical);
        let decoded = StorageRpcDecoder::new(&encoded).read_acl_grants().unwrap();
        assert_eq!(decoded.to_current_storage_string(), canonical);
    }

    #[test]
    fn storage_rpc_frame_round_trips() {
        let payload = b"hello rpc".to_vec();
        let bytes = encode_storage_rpc_frame(7, StorageRpcMessageKind::Health, &payload).unwrap();

        let decoded = decode_storage_rpc_frame(&bytes).unwrap();

        assert_eq!(decoded.request_id, 7);
        assert_eq!(decoded.kind, StorageRpcMessageKind::Health);
        assert_eq!(decoded.payload, payload);
    }

    #[test]
    fn storage_rpc_frame_encoding_is_stable() {
        let payload = b"abc";
        let bytes = encode_storage_rpc_frame(
            0x0102_0304_0506_0708,
            StorageRpcMessageKind::ShardWrite,
            payload,
        )
        .unwrap();
        let expected_checksum = storage_rpc_frame_checksum(
            STORAGE_RPC_FRAME_ENCODING_VERSION,
            0x0102_0304_0506_0708,
            StorageRpcMessageKind::ShardWrite as u16,
            3,
            payload,
        );

        let mut expected = Vec::new();
        expected.extend_from_slice(&24u32.to_le_bytes());
        expected.extend_from_slice(STORAGE_RPC_FRAME_MAGIC);
        expected.extend_from_slice(&16u16.to_le_bytes());
        expected.extend_from_slice(&0x0102_0304_0506_0708u64.to_le_bytes());
        expected.extend_from_slice(&(StorageRpcMessageKind::ShardWrite as u16).to_le_bytes());
        expected.extend_from_slice(&3u32.to_le_bytes());
        expected.extend_from_slice(&expected_checksum.to_le_bytes());
        expected.extend_from_slice(payload);

        assert_eq!(bytes, expected);
    }

    #[test]
    fn storage_rpc_frame_rejects_resealed_old_and_new_version_fixtures() {
        assert_eq!(
            decode_storage_rpc_frame(&raw_storage_rpc_frame_with_version(15)),
            Err(StorageRpcFrameError::UnsupportedVersion(15))
        );
        assert_eq!(
            decode_storage_rpc_frame(&raw_storage_rpc_frame_with_version(17)),
            Err(StorageRpcFrameError::UnsupportedVersion(17))
        );
    }

    #[test]
    fn object_payload_reclaim_claim_acquire_round_trips_effect_deadline() {
        let request = StorageRpcObjectPayloadReclaimClaimAcquireRequest {
            object: StorageRpcObjectRequest {
                node_id: NodeId::new(3),
                cluster_epoch: ClusterEpoch::new(9).unwrap(),
                pg_id: PgId::new(4),
                bucket: BucketName::try_from("bucket").unwrap(),
                key: ObjectKey::try_from("key".to_string()).unwrap(),
            },
            bucket_incarnation_generation: 7,
            generation_id: GenerationId::new(8).unwrap(),
            reclaim_kind: ObjectPayloadReclaimKind::ObjectSegments,
            claim_id: "claim".to_string(),
            owner_token: "owner".to_string(),
            claimed_at: 1_000,
            lease_deadline: Some(8_000),
            now: 1_000,
            effect_deadline: Some(StorageRpcAdmittedRouteEffectDeadline {
                authority_valid_until_ms: 7_000,
                portable_wall_valid_until_ms: 6_000,
            }),
        };

        let encoded = encode_object_payload_reclaim_claim_acquire_request(&request).unwrap();
        assert_eq!(
            decode_object_payload_reclaim_claim_acquire_request(&encoded).unwrap(),
            request
        );
    }

    #[test]
    fn maximum_object_payload_reclaim_claim_acquire_request_fits_kind_cap() {
        let request = StorageRpcObjectPayloadReclaimClaimAcquireRequest {
            object: StorageRpcObjectRequest {
                node_id: NodeId::new(u32::MAX),
                cluster_epoch: ClusterEpoch::new(u64::MAX).unwrap(),
                pg_id: PgId::new(u32::MAX),
                bucket: BucketName::try_from("a".repeat(STORAGE_RPC_MAX_BUCKET_NAME_LEN)).unwrap(),
                key: ObjectKey::try_from("k".repeat(STORAGE_RPC_MAX_OBJECT_KEY_LEN)).unwrap(),
            },
            bucket_incarnation_generation: u64::MAX,
            generation_id: GenerationId::new(u64::MAX).unwrap(),
            reclaim_kind: ObjectPayloadReclaimKind::Multipart,
            claim_id: "c".repeat(STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN),
            owner_token: "o".repeat(STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN),
            claimed_at: 1,
            lease_deadline: Some(u64::MAX),
            now: u64::MAX,
            effect_deadline: Some(StorageRpcAdmittedRouteEffectDeadline {
                authority_valid_until_ms: u64::MAX,
                portable_wall_valid_until_ms: u64::MAX
                    .saturating_sub(crate::control_plane_lease::CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS),
            }),
        };
        let payload = encode_object_payload_reclaim_claim_acquire_request(&request).unwrap();
        assert_eq!(
            payload.len(),
            STORAGE_RPC_MAX_OBJECT_PAYLOAD_RECLAIM_CLAIM_ACQUIRE_PAYLOAD_LEN
        );

        let frame = encode_storage_rpc_frame(
            1,
            StorageRpcMessageKind::ObjectPayloadReclaimClaimAcquire,
            &payload,
        )
        .unwrap();
        let decoded = read_storage_rpc_request_frame_from(&mut Cursor::new(frame)).unwrap();
        assert_eq!(decoded.payload, payload);
        assert_eq!(
            decode_object_payload_reclaim_claim_acquire_request(&decoded.payload).unwrap(),
            request
        );
    }

    #[test]
    fn bucket_write_drain_begin_request_round_trips_effect_deadline() {
        let request = StorageRpcBucketWriteDrainBeginRequest {
            bucket: StorageRpcBucketRequest {
                node_id: NodeId::new(3),
                cluster_epoch: ClusterEpoch::new(9).unwrap(),
                pg_id: PgId::new(4),
                bucket: BucketName::try_from("bucket").unwrap(),
            },
            drain_id: "drain".to_string(),
            owner_token: "owner".to_string(),
            created_at: 1_000,
            lease_deadline: 8_000,
            effect_deadline: Some(StorageRpcAdmittedRouteEffectDeadline {
                authority_valid_until_ms: 7_000,
                portable_wall_valid_until_ms: 6_000,
            }),
        };

        let encoded = encode_bucket_write_drain_begin_request(&request).unwrap();
        assert_eq!(
            decode_bucket_write_drain_begin_request(&encoded).unwrap(),
            request
        );
    }

    #[test]
    fn storage_rpc_frame_rejects_trailing_bytes() {
        let mut bytes = encode_storage_rpc_frame(1, StorageRpcMessageKind::Health, b"ok").unwrap();
        bytes.push(0);

        assert_eq!(
            decode_storage_rpc_frame(&bytes),
            Err(StorageRpcFrameError::TrailingBytes)
        );
    }

    #[test]
    fn storage_rpc_frame_rejects_retired_bucket_snapshot_pair_message_kind() {
        assert_eq!(
            decode_storage_rpc_frame(&raw_storage_rpc_frame_with_kind(42)),
            Err(StorageRpcFrameError::UnknownMessageKind(42))
        );
    }

    #[test]
    fn storage_rpc_frame_rejects_unknown_message_kind() {
        assert_eq!(
            decode_storage_rpc_frame(&raw_storage_rpc_frame_with_kind(999)),
            Err(StorageRpcFrameError::UnknownMessageKind(999))
        );
    }

    fn raw_storage_rpc_frame_with_kind(kind: u16) -> Vec<u8> {
        let payload = b"ok";
        let mut bytes = Vec::new();
        put_bytes(&mut bytes, STORAGE_RPC_FRAME_MAGIC);
        put_u16(&mut bytes, STORAGE_RPC_FRAME_ENCODING_VERSION);
        put_u64(&mut bytes, 1);
        put_u16(&mut bytes, kind);
        put_u32(&mut bytes, payload.len() as u32);
        put_u64(
            &mut bytes,
            storage_rpc_frame_checksum(
                STORAGE_RPC_FRAME_ENCODING_VERSION,
                1,
                kind,
                payload.len() as u32,
                payload,
            ),
        );
        bytes.extend_from_slice(payload);
        bytes
    }

    fn raw_storage_rpc_frame_with_version(version: u16) -> Vec<u8> {
        let payload = b"old or future version";
        let kind = StorageRpcMessageKind::Health as u16;
        let mut bytes = Vec::new();
        put_bytes(&mut bytes, STORAGE_RPC_FRAME_MAGIC);
        put_u16(&mut bytes, version);
        put_u64(&mut bytes, 1);
        put_u16(&mut bytes, kind);
        put_u32(&mut bytes, payload.len() as u32);
        put_u64(
            &mut bytes,
            storage_rpc_frame_checksum(version, 1, kind, payload.len() as u32, payload),
        );
        bytes.extend_from_slice(payload);
        bytes
    }

    #[test]
    fn storage_rpc_frame_rejects_valid_kind_flip() {
        let payload = b"ok";
        let mut bytes =
            encode_storage_rpc_frame(1, StorageRpcMessageKind::ShardRead, payload).unwrap();
        let kind_offset = 4 + STORAGE_RPC_FRAME_MAGIC.len() + 2 + 8;
        bytes[kind_offset..kind_offset + 2]
            .copy_from_slice(&(StorageRpcMessageKind::ShardDelete as u16).to_le_bytes());

        assert_eq!(
            decode_storage_rpc_frame(&bytes),
            Err(StorageRpcFrameError::PayloadChecksumMismatch)
        );
    }

    #[test]
    fn storage_rpc_frame_rejects_bad_payload_checksum_before_payload_decode() {
        let mut bytes =
            encode_storage_rpc_frame(1, StorageRpcMessageKind::ShardWrite, b"payload").unwrap();
        let checksum_offset = 4 + STORAGE_RPC_FRAME_MAGIC.len() + 2 + 8 + 2 + 4;
        bytes[checksum_offset] ^= 0x55;

        assert_eq!(
            decode_storage_rpc_frame(&bytes),
            Err(StorageRpcFrameError::PayloadChecksumMismatch)
        );
    }

    #[test]
    fn storage_rpc_frame_rejects_oversized_payload_on_encode_and_decode() {
        assert_eq!(
            encode_storage_rpc_frame_with_limit(1, StorageRpcMessageKind::Health, b"abcd", 3),
            Err(StorageRpcFrameError::PayloadTooLarge { len: 4, limit: 3 })
        );

        let bytes = encode_storage_rpc_frame(1, StorageRpcMessageKind::Health, b"abcd").unwrap();
        assert_eq!(
            decode_storage_rpc_frame_with_limit(&bytes, 3),
            Err(StorageRpcFrameError::PayloadTooLarge { len: 4, limit: 3 })
        );
    }

    #[test]
    fn payload_record_count_guards_include_placement_epoch() {
        for (min_record_len, old_record_len, message) in [
            (
                STORAGE_RPC_MIN_OBJECT_SEGMENT_RECORD_LEN,
                4 + 4 + 8 + 4 + 8 + 8 + 4 + 16 + 8 + 4 + 2,
                "object segment count exceeds payload",
            ),
            (
                STORAGE_RPC_MIN_OBJECT_PART_RECORD_LEN,
                4 + 4 + 8 + 4 + 8 + 8 + 4 + 1 + 8 + 2 + 4 + 1,
                "object part count exceeds payload",
            ),
            (
                STORAGE_RPC_MIN_STREAM_UPLOAD_SEGMENT_RECORD_LEN,
                4 + SESSION_ID_LEN + 4 + 8 + 8 + 8 + 4 + 16 + 8 + 4 + 2,
                "stream upload segment count exceeds payload",
            ),
            (
                STORAGE_RPC_MIN_MULTIPART_PART_RECORD_LEN,
                4 + UPLOAD_ID_LEN + 4 + 4 + 8 + 8 + 4 + 1 + 8 + 2 + 8 + 1,
                "multipart part count exceeds payload",
            ),
            (
                STORAGE_RPC_MIN_MULTIPART_PART_SEGMENT_RECORD_LEN,
                4 + 4 + 4 + UPLOAD_ID_LEN + 8 + 4 + 4 + 8 + 8 + 4 + 16 + 8 + 4 + 2,
                "multipart part segment count exceeds payload",
            ),
        ] {
            assert_eq!(min_record_len, old_record_len + 8);

            let mut bytes = Vec::new();
            put_u32(&mut bytes, 1);
            bytes.resize(bytes.len() + old_record_len, 0);

            let mut decoder = StorageRpcDecoder::new(&bytes);
            assert_eq!(
                decoder.read_bounded_remaining_count(min_record_len, message),
                Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                    message
                ))
            );
        }
    }

    #[test]
    fn storage_rpc_stream_frame_round_trips() {
        let frame = StorageRpcFrame {
            request_id: 11,
            kind: StorageRpcMessageKind::Health,
            payload: b"stream".to_vec(),
        };
        let mut bytes = Vec::new();
        write_storage_rpc_frame_to(&mut bytes, &frame).unwrap();

        let decoded = read_storage_rpc_frame_from(&mut Cursor::new(bytes)).unwrap();

        assert_eq!(decoded, frame);
    }

    #[test]
    fn storage_rpc_stream_frame_reports_flush_failure() {
        let frame = StorageRpcFrame {
            request_id: 11,
            kind: StorageRpcMessageKind::Health,
            payload: Vec::new(),
        };

        let error = write_storage_rpc_frame_to(&mut FlushFailureWriter, &frame).unwrap_err();

        assert!(matches!(
            error,
            StorageRpcStreamError::Io(error)
                if error.kind() == std::io::ErrorKind::BrokenPipe
        ));
    }

    #[test]
    fn storage_rpc_stream_frame_rejects_oversized_payload_before_allocating() {
        let payload = b"abcd";
        let bytes = encode_storage_rpc_frame(1, StorageRpcMessageKind::Health, payload).unwrap();

        let err = read_storage_rpc_frame_from_with_limit(&mut Cursor::new(bytes), 3).unwrap_err();

        assert!(matches!(
            err,
            StorageRpcStreamError::Frame(StorageRpcFrameError::PayloadTooLarge {
                len: 4,
                limit: 3
            })
        ));
    }

    #[test]
    fn storage_rpc_response_payload_round_trips_success_and_error() {
        let mut success = encode_storage_rpc_success_response(b"ok");
        assert_eq!(
            decode_storage_rpc_response_payload(&success).unwrap(),
            Ok(b"ok".to_vec())
        );
        set_storage_rpc_response_connection_reusable(&mut success, true).unwrap();
        assert_eq!(
            decode_storage_rpc_response_payload_with_connection_disposition(&success).unwrap(),
            DecodedStorageRpcResponsePayload {
                response: Ok(b"ok".to_vec()),
                connection_reusable: true,
            }
        );

        let error = StorageRpcErrorResponse {
            code: StorageRpcErrorCode::UnknownPg,
            message: "unknown PG 9".to_string(),
        };
        let error_bytes = encode_storage_rpc_error_response(&error).unwrap();
        assert_eq!(
            decode_storage_rpc_response_payload(&error_bytes).unwrap(),
            Err(error)
        );

        let mut invalid_disposition = success;
        invalid_disposition[1] = 2;
        assert!(matches!(
            decode_storage_rpc_response_payload(&invalid_disposition),
            Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "unknown response connection disposition"
            ))
        ));
    }

    #[test]
    fn storage_rpc_health_response_round_trips() {
        let response = StorageRpcHealthResponse {
            protocol_version: STORAGE_RPC_FRAME_ENCODING_VERSION,
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(3).unwrap(),
        };

        let bytes = encode_health_response(&response);

        assert_eq!(decode_health_response(&bytes).unwrap(), response);
    }

    #[test]
    fn metadata_command_item_rejects_stale_checksum() {
        let command = test_metadata_command();
        let command_bytes = command.command_bytes();
        let item = StorageRpcMetadataCommandItem {
            command_checksum: command.checksum_crc64(),
            command_bytes,
        };
        let mut bytes = encode_metadata_command_item(&item).unwrap();
        let last = bytes.last_mut().unwrap();
        *last ^= 0x01;

        assert_eq!(
            decode_metadata_command_item(&bytes),
            Err(StorageRpcPayloadError::MetadataCommandChecksumMismatch)
        );
    }

    #[test]
    fn metadata_command_item_rejects_non_canonical_bytes_with_matching_crc() {
        let command_bytes = b"metadata command".to_vec();
        let item = StorageRpcMetadataCommandItem {
            command_checksum: checksum::crc64::checksum(&command_bytes),
            command_bytes,
        };

        assert_eq!(
            encode_metadata_command_item(&item),
            Err(StorageRpcPayloadError::InvalidMetadataCommandEnvelope)
        );
    }

    #[test]
    fn metadata_command_item_rejects_oversized_command_before_allocating() {
        let mut bytes = Vec::new();
        put_u64(&mut bytes, 0);
        put_u32(
            &mut bytes,
            u32::try_from(STORAGE_RPC_MAX_METADATA_COMMAND_BYTES_LEN + 1).unwrap(),
        );

        assert_eq!(
            decode_metadata_command_item(&bytes),
            Err(StorageRpcPayloadError::PayloadTooLarge {
                len: STORAGE_RPC_MAX_METADATA_COMMAND_BYTES_LEN + 1,
                limit: STORAGE_RPC_MAX_METADATA_COMMAND_BYTES_LEN,
            })
        );
    }

    #[test]
    fn command_envelope_response_rejects_oversized_command_before_allocating() {
        let mut bytes = Vec::new();
        put_u8(&mut bytes, 1);
        put_u32(
            &mut bytes,
            u32::try_from(STORAGE_RPC_MAX_METADATA_COMMAND_BYTES_LEN + 1).unwrap(),
        );

        assert!(matches!(
            decode_create_bucket_command_build_response(
                &bytes,
                &metadata_command_decode_authority_for_test(),
            ),
            Err(StorageRpcPayloadError::PayloadTooLarge {
                len,
                limit,
            }) if len == STORAGE_RPC_MAX_METADATA_COMMAND_BYTES_LEN + 1
                && limit == STORAGE_RPC_MAX_METADATA_COMMAND_BYTES_LEN
        ));
    }

    #[test]
    fn metadata_command_request_carries_route_and_command_identity() {
        let command = test_metadata_command();
        let request = StorageRpcMetadataCommandRequest {
            node_id: NodeId::new(7),
            cluster_epoch: command.id().cluster_epoch(),
            pg_id: command.id().pg_id(),
            command: command.clone(),
        };

        let bytes = encode_metadata_command_request(&request).unwrap();
        let decoded = decode_metadata_command_request(
            &bytes,
            &metadata_command_decode_authority_for_test(),
        )
        .unwrap();

        assert_eq!(decoded, request);
        assert_eq!(decoded.command.command_bytes(), command.command_bytes());
    }

    #[test]
    fn metadata_command_request_rejects_route_command_mismatch() {
        let command = test_metadata_command();
        let wrong_pg = StorageRpcMetadataCommandRequest {
            node_id: NodeId::new(7),
            cluster_epoch: command.id().cluster_epoch(),
            pg_id: PgId::new(command.id().pg_id().get() + 1),
            command: command.clone(),
        };
        assert!(matches!(
            encode_metadata_command_request(&wrong_pg),
            Err(StorageRpcPayloadError::MetadataCommandRouteMismatch(_))
        ));

        let request = StorageRpcMetadataCommandRequest {
            node_id: NodeId::new(7),
            cluster_epoch: command.id().cluster_epoch(),
            pg_id: command.id().pg_id(),
            command,
        };
        let mut bytes = encode_metadata_command_request(&request).unwrap();
        bytes[12..16].copy_from_slice(&(request.pg_id.get() + 1).to_le_bytes());

        assert!(matches!(
            decode_metadata_command_request(&bytes, &metadata_command_decode_authority_for_test()),
            Err(StorageRpcPayloadError::MetadataCommandRouteMismatch(_))
        ));
    }

    #[test]
    fn metadata_command_recovery_request_round_trips_authorized_source_and_reissue() {
        let authorized_source = test_metadata_command();
        let abandoned_source = MetadataCommandEnvelope::new(
            crate::metadata_command::MetadataCommandId::new(
                authorized_source.id().cluster_epoch(),
                authorized_source.id().pg_id(),
                MetadataCommandLogIndex::new(authorized_source.id().log_index().get() + 1).unwrap(),
            ),
            authorized_source.payload().clone(),
        );
        let command = MetadataCommandEnvelope::new(
            crate::metadata_command::MetadataCommandId::new(
                authorized_source.id().cluster_epoch(),
                authorized_source.id().pg_id(),
                MetadataCommandLogIndex::new(authorized_source.id().log_index().get() + 2).unwrap(),
            ),
            authorized_source.payload().clone(),
        );
        let request = StorageRpcMetadataCommandRecoveryRequest {
            node_id: NodeId::new(7),
            cluster_epoch: authorized_source.id().cluster_epoch(),
            pg_id: authorized_source.id().pg_id(),
            authorized_source: authorized_source.clone(),
            abandoned_source: Some(abandoned_source.clone()),
            command: command.clone(),
        };

        let bytes = encode_metadata_command_recovery_request(&request).unwrap();
        let decoded = decode_metadata_command_recovery_request(
            &bytes,
            &metadata_command_decode_authority_for_test(),
        )
        .unwrap();

        assert_eq!(decoded, request);
        assert_eq!(
            decoded.authorized_source.command_bytes(),
            authorized_source.command_bytes()
        );
        assert_eq!(
            decoded.abandoned_source.unwrap().command_bytes(),
            abandoned_source.command_bytes()
        );
        assert_eq!(decoded.command.command_bytes(), command.command_bytes());
    }

    #[test]
    fn metadata_command_pending_slot_request_carries_scope_bucket() {
        let command = test_metadata_command();
        let request = StorageRpcMetadataCommandPendingSlotRequest {
            node_id: NodeId::new(7),
            cluster_epoch: command.id().cluster_epoch(),
            pg_id: command.id().pg_id(),
            command: command.clone(),
            scope_bucket: Some(BucketName::try_from("pending-scope").unwrap()),
            effect_deadline: Some(StorageRpcAdmittedRouteEffectDeadline {
                authority_valid_until_ms: 5_000,
                portable_wall_valid_until_ms: 4_000,
            }),
        };

        let bytes = encode_metadata_command_pending_slot_request(&request).unwrap();
        let decoded = decode_metadata_command_pending_slot_request(
            &bytes,
            &metadata_command_decode_authority_for_test(),
        )
        .unwrap();

        assert_eq!(decoded, request);
        assert_eq!(decoded.command.command_bytes(), command.command_bytes());

        let mut nonconservative = request.clone();
        nonconservative
            .effect_deadline
            .as_mut()
            .unwrap()
            .portable_wall_valid_until_ms = 4_001;
        assert!(matches!(
            encode_metadata_command_pending_slot_request(&nonconservative),
            Err(StorageRpcPayloadError::InvalidMetadataCommandPendingSlotRequest(_))
        ));
        let mut nonconservative_wire = bytes;
        let wire_len = nonconservative_wire.len();
        nonconservative_wire[wire_len - 8..].copy_from_slice(&4_001_u64.to_be_bytes());
        assert!(matches!(
            decode_metadata_command_pending_slot_request(
                &nonconservative_wire,
                &metadata_command_decode_authority_for_test(),
            ),
            Err(StorageRpcPayloadError::InvalidMetadataCommandPendingSlotRequest(_))
        ));
    }

    #[test]
    fn metadata_command_pending_slot_replace_request_round_trips() {
        let previous = test_metadata_command();
        let replacement = MetadataCommandEnvelope::new(
            crate::metadata_command::MetadataCommandId::new(
                previous.id().cluster_epoch(),
                previous.id().pg_id(),
                MetadataCommandLogIndex::new(previous.id().log_index().get() + 1).unwrap(),
            ),
            previous.payload().clone(),
        );
        let request = StorageRpcMetadataCommandPendingSlotReplaceRequest {
            node_id: NodeId::new(7),
            cluster_epoch: previous.id().cluster_epoch(),
            pg_id: previous.id().pg_id(),
            previous: previous.clone(),
            replacement: replacement.clone(),
            scope_bucket: Some(previous.bucket_name().clone()),
        };

        let bytes = encode_metadata_command_pending_slot_replace_request(&request).unwrap();
        let decoded = decode_metadata_command_pending_slot_replace_request(
            &bytes,
            &metadata_command_decode_authority_for_test(),
        )
        .unwrap();

        assert_eq!(decoded, request);
        assert_eq!(decoded.previous.command_bytes(), previous.command_bytes());
        assert_eq!(
            decoded.replacement.command_bytes(),
            replacement.command_bytes()
        );
    }

    #[test]
    fn metadata_command_recovery_pending_slot_replace_request_round_trips() {
        let authorized_source = test_metadata_command();
        let abandoned_source = MetadataCommandEnvelope::new(
            crate::metadata_command::MetadataCommandId::new(
                authorized_source.id().cluster_epoch(),
                authorized_source.id().pg_id(),
                MetadataCommandLogIndex::new(authorized_source.id().log_index().get() + 1).unwrap(),
            ),
            authorized_source.payload().clone(),
        );
        let replacement = MetadataCommandEnvelope::new(
            crate::metadata_command::MetadataCommandId::new(
                authorized_source.id().cluster_epoch(),
                authorized_source.id().pg_id(),
                MetadataCommandLogIndex::new(authorized_source.id().log_index().get() + 2).unwrap(),
            ),
            authorized_source.payload().clone(),
        );
        let request = StorageRpcMetadataCommandRecoveryPendingSlotReplaceRequest {
            node_id: NodeId::new(7),
            cluster_epoch: authorized_source.id().cluster_epoch(),
            pg_id: authorized_source.id().pg_id(),
            authorized_source: authorized_source.clone(),
            abandoned_source: Some(abandoned_source.clone()),
            previous: abandoned_source,
            replacement,
            scope_bucket: Some(BucketName::try_from("pending-scope").unwrap()),
        };

        let bytes =
            encode_metadata_command_recovery_pending_slot_replace_request(&request).unwrap();
        let decoded =
            decode_metadata_command_recovery_pending_slot_replace_request(
                &bytes,
                &metadata_command_decode_authority_for_test(),
            )
            .unwrap();

        assert_eq!(decoded, request);
    }

    #[test]
    fn metadata_command_recovery_frames_with_abandoned_source_fit_declared_caps() {
        assert_eq!(
            STORAGE_RPC_MAX_METADATA_COMMAND_RECOVERY_REQUEST_PAYLOAD_LEN,
            STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN
                + 1
                + 3 * STORAGE_RPC_MAX_METADATA_COMMAND_ITEM_PAYLOAD_LEN
        );
        assert_eq!(
            STORAGE_RPC_MAX_METADATA_COMMAND_RECOVERY_PENDING_SLOT_REPLACE_REQUEST_PAYLOAD_LEN,
            STORAGE_RPC_MAX_METADATA_COMMAND_PENDING_SLOT_REPLACE_REQUEST_PAYLOAD_LEN
                + 1
                + 2 * STORAGE_RPC_MAX_METADATA_COMMAND_ITEM_PAYLOAD_LEN
        );

        let authorized_source = test_metadata_command();
        let abandoned_source = MetadataCommandEnvelope::new(
            crate::metadata_command::MetadataCommandId::new(
                authorized_source.id().cluster_epoch(),
                authorized_source.id().pg_id(),
                MetadataCommandLogIndex::new(authorized_source.id().log_index().get() + 1).unwrap(),
            ),
            authorized_source.payload().clone(),
        );
        let replacement = MetadataCommandEnvelope::new(
            crate::metadata_command::MetadataCommandId::new(
                authorized_source.id().cluster_epoch(),
                authorized_source.id().pg_id(),
                MetadataCommandLogIndex::new(authorized_source.id().log_index().get() + 2).unwrap(),
            ),
            authorized_source.payload().clone(),
        );
        let apply_payload =
            encode_metadata_command_recovery_request(&StorageRpcMetadataCommandRecoveryRequest {
                node_id: NodeId::new(7),
                cluster_epoch: authorized_source.id().cluster_epoch(),
                pg_id: authorized_source.id().pg_id(),
                authorized_source: authorized_source.clone(),
                abandoned_source: Some(abandoned_source.clone()),
                command: replacement.clone(),
            })
            .unwrap();
        let replace_payload = encode_metadata_command_recovery_pending_slot_replace_request(
            &StorageRpcMetadataCommandRecoveryPendingSlotReplaceRequest {
                node_id: NodeId::new(7),
                cluster_epoch: authorized_source.id().cluster_epoch(),
                pg_id: authorized_source.id().pg_id(),
                authorized_source,
                abandoned_source: Some(abandoned_source.clone()),
                previous: abandoned_source,
                replacement,
                scope_bucket: Some(BucketName::try_from("pending-scope").unwrap()),
            },
        )
        .unwrap();

        for (kind, payload, limit) in [
            (
                StorageRpcMessageKind::MetadataCommandRecoveryApplyAndRecord,
                apply_payload.clone(),
                STORAGE_RPC_MAX_METADATA_COMMAND_RECOVERY_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::MetadataCommandRecoveryRecordAbandoned,
                apply_payload,
                STORAGE_RPC_MAX_METADATA_COMMAND_RECOVERY_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::MetadataCommandRecoveryPendingSlotReplace,
                replace_payload,
                STORAGE_RPC_MAX_METADATA_COMMAND_RECOVERY_PENDING_SLOT_REPLACE_REQUEST_PAYLOAD_LEN,
            ),
        ] {
            assert!(payload.len() <= limit);
            let frame = encode_storage_rpc_frame(7, kind, &payload).unwrap();
            let decoded = read_storage_rpc_request_frame_from(&mut Cursor::new(frame)).unwrap();
            assert_eq!(decoded.payload, payload);

            let mut boundary_payload = payload;
            boundary_payload.resize(limit, 0);
            let boundary_frame = encode_storage_rpc_frame(8, kind, &boundary_payload).unwrap();
            let decoded =
                read_storage_rpc_request_frame_from(&mut Cursor::new(boundary_frame)).unwrap();
            assert_eq!(decoded.payload.len(), limit);
        }
    }

    #[test]
    fn metadata_command_pending_slot_insert_response_round_trips_conflict() {
        let response = StorageRpcMetadataCommandPendingSlotInsertResponse {
            outcome: StorageRpcMetadataCommandPendingSlotInsertOutcome::PendingConflict {
                pg_id: 9,
                cluster_epoch: ClusterEpoch::new(3).unwrap(),
                existing_log_index: 7,
                candidate_log_index: 8,
            },
        };

        let bytes = encode_metadata_command_pending_slot_insert_response(&response);
        let decoded = decode_metadata_command_pending_slot_insert_response(&bytes).unwrap();

        assert_eq!(decoded, response);
    }

    #[test]
    fn metadata_command_pending_slot_insert_response_round_trips_log_conflict() {
        let response = StorageRpcMetadataCommandPendingSlotInsertResponse {
            outcome: StorageRpcMetadataCommandPendingSlotInsertOutcome::LogConflict {
                node_id: 7,
                pg_id: 9,
                cluster_epoch: ClusterEpoch::new(3).unwrap(),
                log_index: 11,
            },
        };

        let bytes = encode_metadata_command_pending_slot_insert_response(&response);
        let decoded = decode_metadata_command_pending_slot_insert_response(&bytes).unwrap();

        assert_eq!(decoded, response);
    }

    #[test]
    fn metadata_command_pending_slot_remove_response_round_trips() {
        for removed in [false, true] {
            let response = StorageRpcMetadataCommandPendingSlotRemoveResponse { removed };

            let bytes = encode_metadata_command_pending_slot_remove_response(&response);
            let decoded = decode_metadata_command_pending_slot_remove_response(&bytes).unwrap();

            assert_eq!(decoded, response);
        }
    }

    #[test]
    fn metadata_command_next_id_request_and_response_round_trip() {
        let request = StorageRpcMetadataCommandNextIdRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(3).unwrap(),
            pg_id: PgId::new(11),
            min_log_index: 9,
        };

        let bytes = encode_metadata_command_next_id_request(&request);
        let decoded = decode_metadata_command_next_id_request(&bytes).unwrap();

        assert_eq!(decoded, request);

        let response = StorageRpcMetadataCommandNextIdResponse {
            outcome: StorageRpcMetadataCommandNextIdOutcome::Allocated {
                cluster_epoch: ClusterEpoch::new(3).unwrap(),
                pg_id: PgId::new(11),
                log_index: 10,
            },
        };

        let bytes = encode_metadata_command_next_id_response(&response);
        let decoded = decode_metadata_command_next_id_response(&bytes).unwrap();

        assert_eq!(decoded, response);

        let conflict = StorageRpcMetadataCommandNextIdResponse {
            outcome: StorageRpcMetadataCommandNextIdOutcome::LogConflict {
                node_id: 7,
                pg_id: 11,
                cluster_epoch: ClusterEpoch::new(3).unwrap(),
                log_index: 12,
            },
        };

        let bytes = encode_metadata_command_next_id_response(&conflict);
        let decoded = decode_metadata_command_next_id_response(&bytes).unwrap();

        assert_eq!(decoded, conflict);
    }

    #[test]
    fn metadata_command_transfer_adopt_request_round_trips() {
        let command = test_metadata_command();
        let request = StorageRpcMetadataCommandTransferAdoptRequest {
            node_id: NodeId::new(7),
            cluster_epoch: command.id().cluster_epoch(),
            pg_id: command.id().pg_id(),
            expected_state_digest: 1234,
            commands: vec![MetadataTransferCommand {
                command: command.clone(),
                pre_state_digest: 4321,
                post_state_digest: 1234,
            }],
        };

        let bytes = encode_metadata_command_transfer_adopt_request(&request).unwrap();
        let decoded = decode_metadata_command_transfer_adopt_request(
            &bytes,
            &metadata_command_decode_authority_for_test(),
        )
        .unwrap();

        assert_eq!(decoded, request);
        assert_eq!(
            decoded.commands[0].command.command_bytes(),
            command.command_bytes()
        );
        assert_eq!(decoded.commands[0].pre_state_digest, 4321);
        assert_eq!(decoded.commands[0].post_state_digest, 1234);
    }

    #[test]
    fn metadata_command_transfer_empty_state_request_round_trips() {
        let request = StorageRpcMetadataCommandTransferEmptyStateRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(3).unwrap(),
            pg_id: PgId::new(11),
            expected_state_digest: 1234,
        };

        let bytes = encode_metadata_command_transfer_empty_state_request(&request);
        let decoded = decode_metadata_command_transfer_empty_state_request(&bytes).unwrap();

        assert_eq!(decoded, request);
    }

    #[test]
    fn metadata_command_transfer_matching_state_request_round_trips() {
        let request = StorageRpcMetadataCommandTransferMatchingStateRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(3).unwrap(),
            pg_id: PgId::new(11),
            applied_log_index: 4,
            applied_log_hash: 5678,
            expected_state_digest: 1234,
        };

        let bytes = encode_metadata_command_transfer_matching_state_request(&request);
        let decoded = decode_metadata_command_transfer_matching_state_request(&bytes).unwrap();

        assert_eq!(decoded, request);
    }

    #[test]
    fn metadata_command_transfer_checkpoint_base_request_round_trips() {
        let checkpoint = MetadataCommandCheckpoint {
            cluster_epoch: ClusterEpoch::new(3).unwrap(),
            pg_id: PgId::new(11),
            applied_log_index: 7,
            applied_log_hash: 0x1234,
            state_digest: 0x5678,
            canonical_state_encoding_version: METADATA_CANONICAL_STATE_ENCODING_VERSION,
            table_digests: vec![MetadataCheckpointTableDigest {
                table_name: "buckets".to_string(),
                row_count: 1,
                row_hash_xor: 0x11,
                row_hash_sum: 0x11,
                table_digest: 0x22,
            }],
            table_blocks: vec![MetadataCheckpointTableBlock {
                table_name: "buckets".to_string(),
                columns: vec![
                    "name".to_string(),
                    "created_at".to_string(),
                    "ratio".to_string(),
                    "body".to_string(),
                    "missing".to_string(),
                ],
                order_columns: vec!["name".to_string()],
                filter: "all_rows".to_string(),
                rows: vec![MetadataCheckpointRow {
                    values: vec![
                        MetadataCheckpointValue::Text(b"bucket".to_vec()),
                        MetadataCheckpointValue::Integer(-7),
                        MetadataCheckpointValue::RealBits(1.25f64.to_bits()),
                        MetadataCheckpointValue::Blob(vec![1, 2, 3]),
                        MetadataCheckpointValue::Null,
                    ],
                    row_digest: 0x33,
                }],
                row_count: 1,
                row_hash_xor: 0x33,
                row_hash_sum: 0x33,
                table_digest: 0x44,
            }],
            checkpoint_crc64: 0x99,
        };
        let request = StorageRpcMetadataCommandTransferCheckpointBaseRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(4).unwrap(),
            pg_id: PgId::new(11),
            checkpoint,
        };

        let bytes = encode_metadata_command_transfer_checkpoint_base_request(&request).unwrap();
        let decoded = decode_metadata_command_transfer_checkpoint_base_request(&bytes).unwrap();

        assert_eq!(decoded, request);

        let response = StorageRpcMetadataCommandCheckpointResponse {
            checkpoint: request.checkpoint.clone(),
        };
        let bytes = encode_metadata_command_checkpoint_response(&response).unwrap();
        let decoded = decode_metadata_command_checkpoint_response(&bytes).unwrap();

        assert_eq!(decoded, response);

        let bytes = encode_metadata_command_checkpoint_payload(&request.checkpoint).unwrap();
        let decoded = decode_metadata_command_checkpoint_payload(&bytes).unwrap();

        assert_eq!(decoded, request.checkpoint);

        let mut previous_version_request = request.clone();
        previous_version_request
            .checkpoint
            .canonical_state_encoding_version = 3;
        let bytes =
            encode_metadata_command_transfer_checkpoint_base_request(&previous_version_request)
                .unwrap();
        assert_eq!(
            decode_metadata_command_transfer_checkpoint_base_request(&bytes),
            Err(
                StorageRpcPayloadError::UnsupportedMetadataCanonicalStateEncodingVersion {
                    actual: 3,
                }
            )
        );
        let bytes =
            encode_metadata_command_checkpoint_payload(&previous_version_request.checkpoint)
                .unwrap();
        assert_eq!(
            decode_metadata_command_checkpoint_payload(&bytes),
            Err(
                StorageRpcPayloadError::UnsupportedMetadataCanonicalStateEncodingVersion {
                    actual: 3,
                }
            )
        );

        let candidates_request = StorageRpcMetadataCommandCheckpointCandidatesRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(3).unwrap(),
            pg_id: PgId::new(11),
            max_applied_log_index: 99,
            limit: STORAGE_RPC_MAX_METADATA_COMMAND_CHECKPOINT_CANDIDATES as u32,
        };
        let bytes = encode_metadata_command_checkpoint_candidates_request(&candidates_request);
        let decoded = decode_metadata_command_checkpoint_candidates_request(&bytes).unwrap();

        assert_eq!(decoded, candidates_request);

        let candidates_response = StorageRpcMetadataCommandCheckpointCandidatesResponse {
            checkpoints: vec![request.checkpoint],
        };
        let bytes =
            encode_metadata_command_checkpoint_candidates_response(&candidates_response).unwrap();
        let decoded = decode_metadata_command_checkpoint_candidates_response(&bytes).unwrap();

        assert_eq!(decoded, candidates_response);

        for status in [
            MetadataCommandLogCompactionStatus::NoCheckpoint {
                retained_entries: 3,
            },
            MetadataCommandLogCompactionStatus::PendingCommand {
                retained_entries: 4,
            },
            MetadataCommandLogCompactionStatus::Compacted {
                deleted_entries: 5,
                compacted_before: 6,
            },
        ] {
            let response = StorageRpcMetadataCommandLogCompactResponse { status };
            let bytes = encode_metadata_command_log_compact_response(&response);
            let decoded = decode_metadata_command_log_compact_response(&bytes).unwrap();

            assert_eq!(decoded, response);
        }

        let history_request = StorageRpcClusterMapHistoryReferenceSummaryRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(3).unwrap(),
        };
        let bytes = encode_cluster_map_history_reference_summary_request(&history_request);
        let decoded = decode_cluster_map_history_reference_summary_request(&bytes).unwrap();
        assert_eq!(decoded, history_request);

        let history_response = StorageRpcClusterMapHistoryReferenceSummaryResponse {
            references: PgClusterMapHistoryRouteReferences::try_from_iter([
                PgClusterMapHistoryRouteReference::new(
                    PgClusterMapHistoryRouteReferenceKind::LivePlacement,
                    ClusterEpoch::new(2).unwrap(),
                    PgId::new(7),
                ),
                PgClusterMapHistoryRouteReference::new(
                    PgClusterMapHistoryRouteReferenceKind::DurableBackfillSource,
                    ClusterEpoch::new(5).unwrap(),
                    PgId::new(8),
                ),
                PgClusterMapHistoryRouteReference::new(
                    PgClusterMapHistoryRouteReferenceKind::DurableBackfillDesired,
                    ClusterEpoch::new(6).unwrap(),
                    PgId::new(8),
                ),
                PgClusterMapHistoryRouteReference::new(
                    PgClusterMapHistoryRouteReferenceKind::PendingMetadataCommand,
                    ClusterEpoch::new(3).unwrap(),
                    PgId::new(9),
                ),
                PgClusterMapHistoryRouteReference::new(
                    PgClusterMapHistoryRouteReferenceKind::ObjectPayloadReclaimClaim,
                    ClusterEpoch::new(4).unwrap(),
                    PgId::new(9),
                ),
            ])
            .unwrap(),
        };
        let bytes = encode_cluster_map_history_reference_summary_response(&history_response);
        let decoded = decode_cluster_map_history_reference_summary_response(&bytes).unwrap();
        assert_eq!(decoded, history_response);

        let empty_history_response = StorageRpcClusterMapHistoryReferenceSummaryResponse {
            references: PgClusterMapHistoryRouteReferences::default(),
        };
        let bytes = encode_cluster_map_history_reference_summary_response(&empty_history_response);
        let decoded = decode_cluster_map_history_reference_summary_response(&bytes).unwrap();
        assert_eq!(decoded, empty_history_response);

        let mut oversized_history_response = Vec::new();
        put_u32(
            &mut oversized_history_response,
            (MAX_PG_CLUSTER_MAP_HISTORY_ROUTE_REFERENCES + 1) as u32,
        );
        assert!(matches!(
            decode_cluster_map_history_reference_summary_response(&oversized_history_response),
            Err(StorageRpcPayloadError::InvalidCount {
                field: "cluster-map history route references",
                ..
            })
        ));

        let mut duplicate_history_response = Vec::new();
        put_u32(&mut duplicate_history_response, 2);
        for _ in 0..2 {
            duplicate_history_response.push(1);
            put_u64(&mut duplicate_history_response, 2);
            put_u32(&mut duplicate_history_response, 7);
        }
        assert!(matches!(
            decode_cluster_map_history_reference_summary_response(&duplicate_history_response),
            Err(
                StorageRpcPayloadError::InvalidClusterMapHistoryRouteReference(
                    "references are not strictly ordered"
                )
            )
        ));

        let mut reversed_history_response = Vec::new();
        put_u32(&mut reversed_history_response, 2);
        reversed_history_response.push(1);
        put_u64(&mut reversed_history_response, 3);
        put_u32(&mut reversed_history_response, 7);
        reversed_history_response.push(1);
        put_u64(&mut reversed_history_response, 2);
        put_u32(&mut reversed_history_response, 7);
        assert!(matches!(
            decode_cluster_map_history_reference_summary_response(&reversed_history_response),
            Err(
                StorageRpcPayloadError::InvalidClusterMapHistoryRouteReference(
                    "references are not strictly ordered"
                )
            )
        ));

        let mut unknown_kind_history_response = Vec::new();
        put_u32(&mut unknown_kind_history_response, 1);
        unknown_kind_history_response.push(99);
        put_u64(&mut unknown_kind_history_response, 2);
        put_u32(&mut unknown_kind_history_response, 7);
        assert!(matches!(
            decode_cluster_map_history_reference_summary_response(&unknown_kind_history_response),
            Err(
                StorageRpcPayloadError::InvalidClusterMapHistoryRouteReference(
                    "unknown reference kind"
                )
            )
        ));

        assert!(matches!(
            decode_metadata_command_log_compact_response(&[99]),
            Err(StorageRpcPayloadError::InvalidMetadataCommandLogCompactionStatus(99))
        ));
    }

    #[test]
    fn metadata_command_max_log_index_response_round_trips() {
        let response = StorageRpcMetadataCommandMaxLogIndexResponse { max_log_index: 42 };

        let bytes = encode_metadata_command_max_log_index_response(&response);
        let decoded = decode_metadata_command_max_log_index_response(&bytes).unwrap();

        assert_eq!(decoded, response);
    }

    #[test]
    fn metadata_command_log_hash_range_request_round_trips() {
        let request = StorageRpcMetadataCommandLogHashRangeRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(3).unwrap(),
            pg_id: PgId::new(11),
            first_log_index: MetadataCommandLogIndex::new(2).unwrap(),
            last_log_index: MetadataCommandLogIndex::new(5).unwrap(),
        };

        let bytes = encode_metadata_command_log_hash_range_request(&request);
        let decoded = decode_metadata_command_log_hash_range_request(&bytes).unwrap();

        assert_eq!(decoded, request);
    }

    #[test]
    fn metadata_command_log_hash_range_request_rejects_invalid_ranges() {
        let mut bytes = encode_metadata_command_log_hash_range_request(
            &StorageRpcMetadataCommandLogHashRangeRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::new(3).unwrap(),
                pg_id: PgId::new(11),
                first_log_index: MetadataCommandLogIndex::new(5).unwrap(),
                last_log_index: MetadataCommandLogIndex::new(4).unwrap(),
            },
        );
        assert!(matches!(
            decode_metadata_command_log_hash_range_request(&bytes),
            Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "metadata command hash range must be ordered"
            ))
        ));

        bytes = encode_metadata_command_log_hash_range_request(
            &StorageRpcMetadataCommandLogHashRangeRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::new(3).unwrap(),
                pg_id: PgId::new(11),
                first_log_index: MetadataCommandLogIndex::new(1).unwrap(),
                last_log_index: MetadataCommandLogIndex::new(
                    STORAGE_RPC_MAX_METADATA_COMMAND_LOG_HASH_RANGE_ENTRIES + 1,
                )
                .unwrap(),
            },
        );
        assert!(matches!(
            decode_metadata_command_log_hash_range_request(&bytes),
            Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "metadata command hash range is too large"
            ))
        ));
    }

    #[test]
    fn metadata_command_log_hash_range_response_round_trips() {
        let response = StorageRpcMetadataCommandLogHashRangeResponse {
            entries: vec![
                MetadataCommandLogHashRangeEntry {
                    log_index: 2,
                    previous_log_hash: 0x11,
                    log_hash: 0x22,
                },
                MetadataCommandLogHashRangeEntry {
                    log_index: 4,
                    previous_log_hash: 0x33,
                    log_hash: 0x44,
                },
            ],
        };

        let bytes = encode_metadata_command_log_hash_range_response(&response).unwrap();
        let decoded = decode_metadata_command_log_hash_range_response(&bytes).unwrap();

        assert_eq!(decoded, response);
    }

    #[test]
    fn metadata_command_log_entry_range_request_rejects_large_ranges() {
        let request = StorageRpcMetadataCommandLogHashRangeRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(3).unwrap(),
            pg_id: PgId::new(11),
            first_log_index: MetadataCommandLogIndex::new(1).unwrap(),
            last_log_index: MetadataCommandLogIndex::new(
                STORAGE_RPC_MAX_METADATA_COMMAND_LOG_ENTRY_RANGE_ENTRIES + 1,
            )
            .unwrap(),
        };

        let bytes = encode_metadata_command_log_hash_range_request(&request);
        assert!(matches!(
            decode_metadata_command_log_entry_range_request(&bytes),
            Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "metadata command entry range is too large"
            ))
        ));
    }

    #[test]
    fn metadata_command_log_entry_range_response_round_trips() {
        let command = test_metadata_command();
        let response = StorageRpcMetadataCommandLogEntryRangeResponse {
            entries: vec![
                MetadataCommandLogRangeEntry {
                    log_index: command.id().log_index().get(),
                    previous_log_hash: 0x11,
                    log_hash: 0x22,
                    pre_state_digest: Some(0x21),
                    post_state_digest: Some(0x23),
                    kind: MetadataCommandLogRangeEntryKind::Applied(Box::new(command.clone())),
                },
                MetadataCommandLogRangeEntry {
                    log_index: 9,
                    previous_log_hash: 0x33,
                    log_hash: 0x44,
                    pre_state_digest: None,
                    post_state_digest: None,
                    kind: MetadataCommandLogRangeEntryKind::Abandoned {
                        original_command_checksum: 0x55,
                    },
                },
            ],
        };

        let bytes = encode_metadata_command_log_entry_range_response(&response).unwrap();
        let decoded = decode_metadata_command_log_entry_range_response(
            &bytes,
            &metadata_command_decode_authority_for_test(),
        )
        .unwrap();

        assert_eq!(decoded, response);
    }

    #[test]
    fn metadata_command_log_entry_range_worst_case_response_fits_frame_cap() {
        let worst_case_applied_entry_len =
            8 + 8 + 8 + 1 + 8 + 1 + 8 + 4 + STORAGE_RPC_MAX_METADATA_COMMAND_BYTES_LEN;
        let response_payload_len =
            4 + usize::try_from(STORAGE_RPC_MAX_METADATA_COMMAND_LOG_ENTRY_RANGE_ENTRIES).unwrap()
                * worst_case_applied_entry_len;
        let success_wrapped_len = 1 + 1 + 4 + response_payload_len;

        assert!(
            success_wrapped_len <= STORAGE_RPC_MAX_PAYLOAD_LEN,
            "entry range cap must fit a worst-case all-applied response after success wrapping"
        );
        let one_more_success_wrapped_len = success_wrapped_len + worst_case_applied_entry_len;
        assert!(
            one_more_success_wrapped_len > STORAGE_RPC_MAX_PAYLOAD_LEN,
            "test should prove the cap is tight against the frame limit"
        );
    }

    #[test]
    fn metadata_command_log_entry_range_response_rejects_unknown_kind() {
        let mut bytes = Vec::new();
        put_u32(&mut bytes, 1);
        put_u64(&mut bytes, 1);
        put_u64(&mut bytes, 0x11);
        put_u64(&mut bytes, 0x22);
        put_u8(&mut bytes, 0);
        put_u8(&mut bytes, 0);
        put_u8(&mut bytes, 9);

        assert!(matches!(
            decode_metadata_command_log_entry_range_response(
                &bytes,
                &metadata_command_decode_authority_for_test(),
            ),
            Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "unknown metadata command entry range kind"
            ))
        ));
    }

    #[test]
    fn metadata_command_pending_envelope_response_round_trips() {
        let command = test_metadata_command();
        for response in [
            StorageRpcMetadataCommandPendingEnvelopeResponse { command: None },
            StorageRpcMetadataCommandPendingEnvelopeResponse {
                command: Some(command.clone()),
            },
        ] {
            let bytes = encode_metadata_command_pending_envelope_response(&response);
            let decoded = decode_metadata_command_pending_envelope_response(
                &bytes,
                &metadata_command_decode_authority_for_test(),
            )
            .unwrap();

            assert_eq!(decoded, response);
        }
    }

    #[test]
    fn metadata_command_matching_applied_request_round_trips() {
        let command = test_metadata_command();
        let request = StorageRpcMetadataCommandMatchingAppliedRequest {
            node_id: NodeId::new(7),
            cluster_epoch: command.id().cluster_epoch(),
            pg_id: command.id().pg_id(),
            command: command.clone(),
            expected_previous_log_hash: 0xabc,
        };

        let bytes = encode_metadata_command_matching_applied_request(&request).unwrap();
        let decoded = decode_metadata_command_matching_applied_request(
            &bytes,
            &metadata_command_decode_authority_for_test(),
        )
        .unwrap();

        assert_eq!(decoded, request);
        assert_eq!(decoded.command.command_bytes(), command.command_bytes());
    }

    #[test]
    fn metadata_command_applied_hashes_response_round_trips_outcomes() {
        for response in [
            StorageRpcMetadataCommandAppliedHashesResponse {
                outcome: StorageRpcMetadataCommandAppliedHashesOutcome::Hashes(None),
            },
            StorageRpcMetadataCommandAppliedHashesResponse {
                outcome: StorageRpcMetadataCommandAppliedHashesOutcome::Hashes(Some((0x12, 0x34))),
            },
            StorageRpcMetadataCommandAppliedHashesResponse {
                outcome: StorageRpcMetadataCommandAppliedHashesOutcome::LogConflict {
                    node_id: 7,
                    pg_id: 11,
                    cluster_epoch: ClusterEpoch::new(3).unwrap(),
                    log_index: 12,
                },
            },
        ] {
            let bytes = encode_metadata_command_applied_hashes_response(&response);
            let decoded = decode_metadata_command_applied_hashes_response(&bytes).unwrap();

            assert_eq!(decoded, response);
        }
    }

    #[test]
    fn metadata_command_bool_response_round_trips() {
        for response in [
            StorageRpcMetadataCommandBoolResponse { value: false },
            StorageRpcMetadataCommandBoolResponse { value: true },
        ] {
            let bytes = encode_metadata_command_bool_response(&response);
            let decoded = decode_metadata_command_bool_response(&bytes).unwrap();

            assert_eq!(decoded, response);
        }
    }

    #[test]
    fn metadata_command_bool_outcome_response_round_trips() {
        for response in [
            StorageRpcMetadataCommandBoolOutcomeResponse {
                outcome: StorageRpcMetadataCommandBoolOutcome::Value(false),
            },
            StorageRpcMetadataCommandBoolOutcomeResponse {
                outcome: StorageRpcMetadataCommandBoolOutcome::Value(true),
            },
            StorageRpcMetadataCommandBoolOutcomeResponse {
                outcome: StorageRpcMetadataCommandBoolOutcome::LogConflict {
                    node_id: 7,
                    pg_id: 11,
                    cluster_epoch: ClusterEpoch::new(3).unwrap(),
                    log_index: 12,
                },
            },
        ] {
            let bytes = encode_metadata_command_bool_outcome_response(&response);
            let decoded = decode_metadata_command_bool_outcome_response(&bytes).unwrap();

            assert_eq!(decoded, response);
        }
    }

    #[test]
    fn metadata_command_state_outcome_response_round_trips() {
        for response in [
            StorageRpcMetadataCommandStateOutcomeResponse {
                outcome: StorageRpcMetadataCommandStateOutcome::State(
                    MetadataCommandReplicaState {
                        cluster_epoch: ClusterEpoch::new(3).unwrap(),
                        applied_log_index: 44,
                        applied_log_hash: 0x55,
                        state_digest: 0x66,
                    },
                ),
            },
            StorageRpcMetadataCommandStateOutcomeResponse {
                outcome: StorageRpcMetadataCommandStateOutcome::LogConflict {
                    node_id: 7,
                    pg_id: 11,
                    cluster_epoch: ClusterEpoch::new(3).unwrap(),
                    log_index: 12,
                },
            },
        ] {
            let bytes = encode_metadata_command_state_outcome_response(&response);
            let decoded = decode_metadata_command_state_outcome_response(&bytes).unwrap();

            assert_eq!(decoded, response);
        }
    }

    #[test]
    fn metadata_command_state_request_carries_route() {
        let request = StorageRpcMetadataCommandStateRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(3).unwrap(),
            pg_id: PgId::new(11),
        };

        let bytes = encode_metadata_command_state_request(&request);
        let decoded = decode_metadata_command_state_request(&bytes).unwrap();

        assert_eq!(decoded, request);
    }

    #[test]
    fn metadata_command_state_response_round_trips() {
        let response = StorageRpcMetadataCommandStateResponse {
            state: MetadataCommandReplicaState {
                cluster_epoch: ClusterEpoch::new(3).unwrap(),
                applied_log_index: 44,
                applied_log_hash: 0x55,
                state_digest: 0x66,
            },
        };

        let bytes = encode_metadata_command_state_response(&response);
        let decoded = decode_metadata_command_state_response(&bytes).unwrap();

        assert_eq!(decoded, response);
    }

    #[test]
    fn metadata_command_acceptance_response_round_trips() {
        for acceptance in [
            MetadataCommandAcceptance::Apply,
            MetadataCommandAcceptance::AlreadyApplied,
        ] {
            let response = StorageRpcMetadataCommandAcceptanceResponse {
                outcome: StorageRpcMetadataCommandAcceptanceOutcome::Acceptance(acceptance),
            };

            let bytes = encode_metadata_command_acceptance_response(&response);
            let decoded = decode_metadata_command_acceptance_response(&bytes).unwrap();

            assert_eq!(decoded, response);
        }

        let conflict = StorageRpcMetadataCommandAcceptanceResponse {
            outcome: StorageRpcMetadataCommandAcceptanceOutcome::LogConflict {
                node_id: 7,
                pg_id: 11,
                cluster_epoch: ClusterEpoch::new(3).unwrap(),
                log_index: 13,
            },
        };
        let bytes = encode_metadata_command_acceptance_response(&conflict);
        let decoded = decode_metadata_command_acceptance_response(&bytes).unwrap();
        assert_eq!(decoded, conflict);

        assert_eq!(
            decode_metadata_command_acceptance_response(&[99]),
            Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "unknown metadata command acceptance tag"
            ))
        );
    }

    #[test]
    fn shard_write_item_rejects_semantic_corruption_after_frame_decode() {
        let payload = b"shard payload".to_vec();
        let item = StorageRpcShardWriteItem {
            expected_size: payload.len() as u64,
            expected_crc64: checksum::crc64::checksum(&payload),
            payload,
        };
        let mut item_bytes = encode_shard_write_item(&item).unwrap();
        let last = item_bytes.last_mut().unwrap();
        *last ^= 0x80;
        let frame = encode_storage_rpc_frame(9, StorageRpcMessageKind::ShardWrite, &item_bytes)
            .expect("corrupted semantic payload still has valid transport frame");
        let decoded_frame = decode_storage_rpc_frame(&frame).unwrap();

        assert_eq!(
            decode_shard_write_item(&decoded_frame.payload),
            Err(StorageRpcPayloadError::ShardWriteChecksumMismatch)
        );
    }

    #[test]
    fn shard_write_request_carries_idempotency_identity() {
        let payload = b"payload bytes".to_vec();
        let request = StorageRpcShardWriteRequest {
            location: test_shard_location(2),
            shard_key: test_shard_key(2),
            expected_size: payload.len() as u64,
            expected_crc64: checksum::crc64::checksum(&payload),
            effect_deadline: Some(StorageRpcAdmittedRouteEffectDeadline {
                authority_valid_until_ms: 7_000,
                portable_wall_valid_until_ms: 6_000,
            }),
            payload,
        };

        let bytes = encode_shard_write_request(&request).unwrap();
        let decoded = decode_shard_write_request(&bytes).unwrap();

        assert_eq!(decoded, request);
    }

    #[test]
    fn shard_write_request_rejects_location_key_mismatch() {
        let payload = b"payload bytes".to_vec();
        let request = StorageRpcShardWriteRequest {
            location: test_shard_location(3),
            shard_key: test_shard_key(2),
            expected_size: payload.len() as u64,
            expected_crc64: checksum::crc64::checksum(&payload),
            effect_deadline: None,
            payload,
        };

        assert_eq!(
            encode_shard_write_request(&request),
            Err(StorageRpcPayloadError::ShardLocationMismatch)
        );
    }

    #[test]
    fn shard_write_ack_must_match_request_expectation() {
        let payload = b"payload bytes";
        let expected_size = payload.len() as u64;
        let expected_crc64 = checksum::crc64::checksum(payload);
        let ack = WriteAck {
            stored_size: expected_size,
            crc64: expected_crc64,
        };
        let bytes = encode_shard_write_ack(ack);
        let decoded = decode_shard_write_ack(&bytes, expected_size, expected_crc64).unwrap();

        assert_eq!(decoded.stored_size, ack.stored_size);
        assert_eq!(decoded.crc64, ack.crc64);
        assert!(matches!(
            decode_shard_write_ack(&bytes, expected_size, expected_crc64 ^ 1),
            Err(StorageRpcPayloadError::ShardWriteChecksumMismatch)
        ));
    }

    #[test]
    fn shard_read_request_and_response_carry_expected_ack() {
        let payload = b"payload bytes";
        let expected_ack = WriteAck {
            stored_size: payload.len() as u64,
            crc64: checksum::crc64::checksum(payload),
        };
        let request = StorageRpcShardReadRequest {
            location: test_shard_location(4),
            shard_key: test_shard_key(4),
            expected_ack,
        };

        let bytes = encode_shard_read_request(&request).unwrap();
        let decoded = decode_shard_read_request(&bytes).unwrap();
        assert_eq!(decoded, request);

        let response = encode_shard_read_response(payload, expected_ack).unwrap();
        assert_eq!(
            decode_shard_read_response(&response, expected_ack).unwrap(),
            payload
        );
        assert!(matches!(
            decode_shard_read_response(
                &response,
                WriteAck {
                    stored_size: expected_ack.stored_size,
                    crc64: expected_ack.crc64 ^ 1,
                },
            ),
            Err(StorageRpcPayloadError::ShardWriteChecksumMismatch)
        ));
    }

    #[test]
    fn historical_shard_read_response_carries_self_validating_ack() {
        let payload = b"historical payload";
        let request = StorageRpcHistoricalShardReadRequest {
            location: test_shard_location(4),
            shard_key: test_shard_key(4),
        };
        let request_bytes = encode_historical_shard_read_request(&request).unwrap();
        assert_eq!(
            decode_historical_shard_read_request(&request_bytes).unwrap(),
            request
        );

        let response = encode_historical_shard_read_response(payload).unwrap();
        let (decoded, ack) = decode_historical_shard_read_response(&response).unwrap();
        assert_eq!(decoded, payload);
        assert_eq!(ack.stored_size, payload.len() as u64);
        assert_eq!(ack.crc64, checksum::crc64::checksum(payload));

        let mut corrupted = response;
        *corrupted.last_mut().unwrap() ^= 0x80;
        assert!(matches!(
            decode_historical_shard_read_response(&corrupted),
            Err(StorageRpcPayloadError::ShardWriteChecksumMismatch)
        ));
    }

    #[test]
    fn shard_read_range_request_carries_expected_ack_and_range() {
        let payload = b"payload bytes";
        let expected_ack = WriteAck {
            stored_size: payload.len() as u64,
            crc64: checksum::crc64::checksum(payload),
        };
        let request = StorageRpcShardReadRangeRequest {
            location: test_shard_location(4),
            shard_key: test_shard_key(4),
            expected_ack,
            offset: 2,
            length: 5,
        };

        let bytes = encode_shard_read_range_request(&request).unwrap();
        let decoded = decode_shard_read_range_request(&bytes).unwrap();
        assert_eq!(decoded, request);

        let response = encode_shard_read_range_response(&payload[2..7]);
        assert_eq!(
            decode_shard_read_range_response(&response, request.length as usize).unwrap(),
            &payload[2..7]
        );
        assert!(matches!(
            decode_shard_read_range_response(&response, request.length as usize + 1),
            Err(StorageRpcPayloadError::ShardWriteSizeMismatch { .. })
        ));
        assert!(matches!(
            encode_shard_read_range_request(&StorageRpcShardReadRangeRequest {
                offset: payload.len() as u64,
                length: 1,
                ..request
            }),
            Err(StorageRpcPayloadError::ShardWriteSizeMismatch { .. })
        ));
    }

    #[test]
    fn shard_delete_request_carries_operation_key() {
        let request = StorageRpcShardDeleteRequest {
            location: test_shard_location(4),
            shard_key: test_shard_key(4),
        };

        let bytes = encode_shard_delete_request(&request).unwrap();
        let decoded = decode_shard_delete_request(&bytes).unwrap();

        assert_eq!(decoded, request);
    }

    #[test]
    fn shard_ack_batch_request_carries_route_and_exact_acks() {
        let request = StorageRpcShardAckBatchRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(3),
            items: vec![StorageRpcShardAckItem {
                shard_key: test_shard_key(2),
                ack: WriteAck {
                    stored_size: 123,
                    crc64: 0xBEEF,
                },
            }],
        };

        let bytes = encode_shard_ack_batch_request(&request).unwrap();
        let decoded = decode_shard_ack_batch_request(&bytes).unwrap();

        assert_eq!(decoded, request);
        assert!(matches!(
            encode_shard_ack_batch_request(&StorageRpcShardAckBatchRequest {
                items: Vec::new(),
                ..request
            }),
            Err(StorageRpcPayloadError::InvalidShardAckBatchRequest(_))
        ));
    }

    #[test]
    fn shard_ack_item_request_and_response_carry_identity() {
        let request = StorageRpcShardAckItemRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(3),
            shard_key: test_shard_key(2),
        };
        let response = StorageRpcShardAckItem {
            shard_key: request.shard_key.clone(),
            ack: WriteAck {
                stored_size: 123,
                crc64: 0xBEEF,
            },
        };

        let request_bytes = encode_shard_ack_item_request(&request);
        assert_eq!(
            decode_shard_ack_item_request(&request_bytes).unwrap(),
            request
        );

        let response_bytes = encode_shard_ack_item_response(&response);
        assert_eq!(
            decode_shard_ack_item_response(&response_bytes).unwrap(),
            response
        );
    }

    #[test]
    fn placed_segment_shard_repair_rpc_round_trips_and_rejects_bad_shard_index() {
        let route = StorageRpcBucketPgRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(3),
        };
        let work_item = PlacedSegmentShardRepairWorkItem {
            request: SegmentStoredBytesRequest {
                data_pg_id: 3,
                segment_okh: [0x5A; 16],
                segment_vid: GenerationId::new(88).unwrap(),
                stored_size: 4096,
                segment_crc64: 0xCAFE,
                ec: EcShape { k: 2, m: 1 },
            },
            shard_index: ShardIndex::new(2),
        };
        let record_request = StorageRpcPlacedSegmentShardRepairRecordRequest {
            route: route.clone(),
            work_item,
            last_error: Some("missing shard file".to_string()),
        };

        let record_bytes = encode_placed_segment_shard_repair_record_request(&record_request)
            .expect("repair record request should encode");
        assert_eq!(
            decode_placed_segment_shard_repair_record_request(&record_bytes).unwrap(),
            record_request
        );

        let item_request = StorageRpcPlacedSegmentShardRepairItemRequest {
            route: route.clone(),
            work_item,
        };
        let item_bytes = encode_placed_segment_shard_repair_item_request(&item_request)
            .expect("repair item request should encode");
        assert_eq!(
            decode_placed_segment_shard_repair_item_request(&item_bytes).unwrap(),
            item_request
        );

        let repair = PlacedSegmentShardRepairRecord {
            work_item,
            first_seen_at: 10,
            last_seen_at: 20,
            observation_count: 2,
            last_error: Some("still bad".to_string()),
        };
        assert!(matches!(
            encode_placed_segment_shard_repairs_response(&vec![
                repair.clone();
                PLACED_SEGMENT_SHARD_REPAIR_LIST_LIMIT
                    + 1
            ]),
            Err(StorageRpcPayloadError::PayloadTooLarge { .. })
        ));
        assert_eq!(
            decode_placed_segment_shard_repairs_response(
                &encode_placed_segment_shard_repairs_response(std::slice::from_ref(&repair))
                    .unwrap()
            )
            .unwrap(),
            vec![repair]
        );
        let claim_acquire = StorageRpcPlacedSegmentShardRepairClaimAcquireRequest {
            route: route.clone(),
            claim_id: "claim-1".to_string(),
            owner_token: "worker-1".to_string(),
            claimed_at: 10,
            lease_deadline: Some(20),
            now: 10,
        };
        let claim_acquire_bytes =
            encode_placed_segment_shard_repair_claim_acquire_request(&claim_acquire)
                .expect("repair claim acquire request should encode");
        assert_eq!(
            decode_placed_segment_shard_repair_claim_acquire_request(&claim_acquire_bytes).unwrap(),
            claim_acquire
        );
        let missing_lease_acquire = StorageRpcPlacedSegmentShardRepairClaimAcquireRequest {
            route: route.clone(),
            claim_id: "claim-1".to_string(),
            owner_token: "worker-1".to_string(),
            claimed_at: 10,
            lease_deadline: None,
            now: 10,
        };
        assert!(matches!(
            encode_placed_segment_shard_repair_claim_acquire_request(&missing_lease_acquire),
            Err(StorageRpcPayloadError::InvalidDurableClaimToken(_))
        ));

        let claim = PlacedSegmentShardRepairClaimRecord {
            work_item,
            claim_id: "claim-1".to_string(),
            owner_token: "worker-1".to_string(),
            cluster_epoch: route.cluster_epoch,
            claimed_at: 10,
            lease_deadline: Some(20),
            attempt_count: 1,
            last_error: Some("previous failure".to_string()),
        };
        let claim_response = StorageRpcPlacedSegmentShardRepairClaimOptionalRecordResponse {
            record: Some(claim.clone()),
        };
        assert_eq!(
            decode_placed_segment_shard_repair_claim_optional_record_response(
                &encode_placed_segment_shard_repair_claim_optional_record_response(&claim_response)
                    .unwrap()
            )
            .unwrap(),
            claim_response
        );
        let missing_lease_claim = PlacedSegmentShardRepairClaimRecord {
            lease_deadline: None,
            ..claim.clone()
        };
        assert!(matches!(
            encode_placed_segment_shard_repair_claim_optional_record_response(
                &StorageRpcPlacedSegmentShardRepairClaimOptionalRecordResponse {
                    record: Some(missing_lease_claim)
                }
            ),
            Err(StorageRpcPayloadError::InvalidDurableClaimToken(_))
        ));

        let claim_record_request = StorageRpcPlacedSegmentShardRepairClaimRecordRequest {
            route: route.clone(),
            claim: claim.clone(),
        };
        assert_eq!(
            decode_placed_segment_shard_repair_claim_record_request(
                &encode_placed_segment_shard_repair_claim_record_request(&claim_record_request)
                    .unwrap()
            )
            .unwrap(),
            claim_record_request
        );

        let claim_error_request = StorageRpcPlacedSegmentShardRepairClaimErrorRequest {
            route,
            claim,
            last_error: "repair still failed".to_string(),
            next_attempt_after: 30,
        };
        assert_eq!(
            decode_placed_segment_shard_repair_claim_error_request(
                &encode_placed_segment_shard_repair_claim_error_request(&claim_error_request)
                    .unwrap()
            )
            .unwrap(),
            claim_error_request
        );

        let mut bad = item_bytes;
        *bad.last_mut().unwrap() = 3;
        assert!(matches!(
            decode_placed_segment_shard_repair_item_request(&bad),
            Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(_))
        ));
    }

    #[test]
    fn placed_segment_shard_backfill_rpc_round_trips_and_rejects_reversed_epochs() {
        let route = StorageRpcBucketPgRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(2).unwrap(),
            pg_id: PgId::new(3),
        };
        let work_item = PlacedSegmentShardBackfillWorkItem {
            request: SegmentStoredBytesRequest {
                data_pg_id: 3,
                segment_okh: [0x5B; 16],
                segment_vid: GenerationId::new(89).unwrap(),
                stored_size: 4096,
                segment_crc64: 0xCAFE,
                ec: EcShape { k: 2, m: 1 },
            },
            source_cluster_epoch: ClusterEpoch::new(1).unwrap(),
            desired_cluster_epoch: ClusterEpoch::new(2).unwrap(),
        };
        let record_request = StorageRpcPlacedSegmentShardBackfillRecordRequest {
            route: route.clone(),
            work_item,
            remaining_tolerance: 1,
            last_error: Some("missing desired shard".to_string()),
        };

        let record_bytes = encode_placed_segment_shard_backfill_record_request(&record_request)
            .expect("backfill record request should encode");
        assert_eq!(
            decode_placed_segment_shard_backfill_record_request(&record_bytes).unwrap(),
            record_request
        );

        let item_request = StorageRpcPlacedSegmentShardBackfillItemRequest {
            route: route.clone(),
            work_item,
        };
        let item_bytes = encode_placed_segment_shard_backfill_item_request(&item_request)
            .expect("backfill item request should encode");
        assert_eq!(
            decode_placed_segment_shard_backfill_item_request(&item_bytes).unwrap(),
            item_request
        );

        let backfill = PlacedSegmentShardBackfillRecord {
            work_item,
            remaining_tolerance: 1,
            first_seen_at: 10,
            last_seen_at: 20,
            observation_count: 2,
            last_error: Some("still missing".to_string()),
        };
        assert!(matches!(
            encode_placed_segment_shard_backfills_response(&vec![
                backfill.clone();
                PLACED_SEGMENT_SHARD_BACKFILL_LIST_LIMIT
                    + 1
            ]),
            Err(StorageRpcPayloadError::PayloadTooLarge { .. })
        ));
        assert_eq!(
            decode_placed_segment_shard_backfills_response(
                &encode_placed_segment_shard_backfills_response(std::slice::from_ref(&backfill))
                    .unwrap()
            )
            .unwrap(),
            vec![backfill]
        );
        assert_eq!(
            decode_placed_segment_shard_backfill_count_response(
                &encode_placed_segment_shard_backfill_count_response(
                    PLACED_SEGMENT_SHARD_BACKFILL_LIST_LIMIT + 1
                )
                .unwrap()
            )
            .unwrap(),
            PLACED_SEGMENT_SHARD_BACKFILL_LIST_LIMIT + 1
        );

        let claim_acquire = StorageRpcPlacedSegmentShardBackfillClaimAcquireRequest {
            route: route.clone(),
            claim_id: "claim-1".to_string(),
            owner_token: "worker-1".to_string(),
            claimed_at: 10,
            lease_deadline: Some(20),
            now: 10,
        };
        let claim_acquire_bytes =
            encode_placed_segment_shard_backfill_claim_acquire_request(&claim_acquire)
                .expect("backfill claim acquire request should encode");
        assert_eq!(
            decode_placed_segment_shard_backfill_claim_acquire_request(&claim_acquire_bytes)
                .unwrap(),
            claim_acquire
        );
        let missing_lease_acquire = StorageRpcPlacedSegmentShardBackfillClaimAcquireRequest {
            route: route.clone(),
            claim_id: "claim-1".to_string(),
            owner_token: "worker-1".to_string(),
            claimed_at: 10,
            lease_deadline: None,
            now: 10,
        };
        assert!(matches!(
            encode_placed_segment_shard_backfill_claim_acquire_request(&missing_lease_acquire),
            Err(StorageRpcPayloadError::InvalidDurableClaimToken(_))
        ));

        let claim = PlacedSegmentShardBackfillClaimRecord {
            work_item,
            remaining_tolerance: 1,
            claim_id: "claim-1".to_string(),
            owner_token: "worker-1".to_string(),
            cluster_epoch: route.cluster_epoch,
            claimed_at: 10,
            lease_deadline: Some(20),
            attempt_count: 1,
            last_error: Some("previous failure".to_string()),
        };
        let claim_response = StorageRpcPlacedSegmentShardBackfillClaimOptionalRecordResponse {
            record: Some(claim.clone()),
        };
        assert_eq!(
            decode_placed_segment_shard_backfill_claim_optional_record_response(
                &encode_placed_segment_shard_backfill_claim_optional_record_response(
                    &claim_response
                )
                .unwrap()
            )
            .unwrap(),
            claim_response
        );
        let missing_lease_claim = PlacedSegmentShardBackfillClaimRecord {
            lease_deadline: None,
            ..claim.clone()
        };
        assert!(matches!(
            encode_placed_segment_shard_backfill_claim_optional_record_response(
                &StorageRpcPlacedSegmentShardBackfillClaimOptionalRecordResponse {
                    record: Some(missing_lease_claim)
                }
            ),
            Err(StorageRpcPayloadError::InvalidDurableClaimToken(_))
        ));

        let claim_record_request = StorageRpcPlacedSegmentShardBackfillClaimRecordRequest {
            route: route.clone(),
            claim: claim.clone(),
        };
        assert_eq!(
            decode_placed_segment_shard_backfill_claim_record_request(
                &encode_placed_segment_shard_backfill_claim_record_request(&claim_record_request)
                    .unwrap()
            )
            .unwrap(),
            claim_record_request
        );

        let claim_error_request = StorageRpcPlacedSegmentShardBackfillClaimErrorRequest {
            route,
            claim,
            last_error: "backfill still failed".to_string(),
            next_attempt_after: 30,
        };
        assert_eq!(
            decode_placed_segment_shard_backfill_claim_error_request(
                &encode_placed_segment_shard_backfill_claim_error_request(&claim_error_request)
                    .unwrap()
            )
            .unwrap(),
            claim_error_request
        );

        let mut reversed_epoch_bytes = item_bytes;
        let source_epoch_start = STORAGE_RPC_SHARD_ACK_ROUTE_LEN + 4 + 16 + 8 + 8 + 8 + 2;
        let desired_epoch_start = source_epoch_start + 8;
        reversed_epoch_bytes[source_epoch_start..source_epoch_start + 8]
            .copy_from_slice(&2_u64.to_be_bytes());
        reversed_epoch_bytes[desired_epoch_start..desired_epoch_start + 8]
            .copy_from_slice(&1_u64.to_be_bytes());
        assert!(matches!(
            decode_placed_segment_shard_backfill_item_request(&reversed_epoch_bytes),
            Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(_))
        ));
    }

    #[test]
    fn multipart_completion_snapshot_response_preserves_missing_part() {
        let upload_id = crate::tests::multipart_upload_id("completion-missing-part-upload");
        let response = StorageRpcMultipartCompletionSnapshotResponse {
            outcome: StorageRpcMultipartCompletionSnapshotOutcome::PartNotFound {
                upload_id: upload_id.clone(),
                part_number: 9999,
            },
        };

        let bytes = encode_multipart_completion_snapshot_response(&response).unwrap();
        let decoded = decode_multipart_completion_snapshot_response(
            &bytes,
            MultipartCompletionSubject::new(
                crate::tests::bucket_name("completion-missing-part-bucket"),
                crate::tests::object_key("completion-missing-part-key"),
                upload_id.clone(),
                GenerationId::new(1).unwrap(),
            ),
        )
        .unwrap();

        let StorageRpcMultipartCompletionSnapshotOutcome::PartNotFound {
            upload_id: decoded_upload_id,
            part_number,
        } = decoded.outcome
        else {
            panic!("expected missing part outcome");
        };
        assert_eq!(decoded_upload_id, upload_id);
        assert_eq!(part_number, 9999);
    }

    #[test]
    fn scavenger_list_files_request_and_response_round_trip() {
        let request = StorageRpcScavengerListFilesRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            data_pg_id: PgId::new(3),
        };

        let request_bytes = encode_scavenger_list_files_request(&request);
        assert_eq!(
            decode_scavenger_list_files_request(&request_bytes).unwrap(),
            request
        );

        let scan = ScavengerShardFileScan {
            files: vec![ScavengerShardFile {
                key: test_shard_key(2),
                size: 123,
            }],
            errors: vec!["bad prefix".to_string()],
        };
        let response_bytes = encode_scavenger_list_files_response(&scan);
        let decoded = decode_scavenger_list_files_response(&response_bytes).unwrap();

        assert_eq!(decoded.files.len(), 1);
        assert_eq!(decoded.files[0].key, scan.files[0].key);
        assert_eq!(decoded.files[0].size, scan.files[0].size);
        assert_eq!(decoded.errors, scan.errors);
    }

    #[test]
    fn scavenger_metadata_messages_round_trip() {
        let route = StorageRpcBucketPgRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(3),
        };
        let key = test_shard_key(2);
        let rows = vec![ScavengerShardRow {
            key: key.clone(),
            ack: WriteAck {
                stored_size: 123,
                crc64: 0xBEEF,
            },
        }];
        let decoded_rows =
            decode_scavenger_shard_rows_response(&encode_scavenger_shard_rows_response(&rows))
                .unwrap();
        assert_eq!(decoded_rows.len(), rows.len());
        assert_eq!(decoded_rows[0].key, rows[0].key);
        assert_eq!(decoded_rows[0].ack, rows[0].ack);

        let references = vec![
            ShardScavengerPayloadReference::Placed(ShardScavengerPlacedShardSetReference {
                data_pg_id: 3,
                okh: [7; 16],
                generation_id: GenerationId::new(5).unwrap(),
                placement_cluster_epoch: ClusterEpoch::new(11).unwrap(),
                stored_size: 4096,
                crc64: 0xBEEF,
                ec: EcShape { k: 2, m: 1 },
            }),
            ShardScavengerPayloadReference::ReclaimOnly(ShardScavengerReclaimShardSetReference {
                data_pg_id: 4,
                okh: [9; 16],
                generation_id: GenerationId::new(10).unwrap(),
                ec: EcShape { k: 2, m: 1 },
            }),
        ];
        assert_eq!(
            decode_scavenger_payload_references_response(
                &encode_scavenger_payload_references_response(&references)
            )
            .unwrap(),
            references
        );

        let page_cursor = PlacedSegmentBackfillReferenceCursor::ObjectSegment {
            bucket: crate::tests::bucket_name("page-bucket"),
            key: crate::tests::object_key("page-key"),
            version_id: 7,
            segment_index: 2,
        };
        let page_request = StorageRpcPlacedSegmentBackfillReferencePageRequest {
            route: route.clone(),
            after: Some(page_cursor.clone()),
            limit: NonZeroU16::new(32).unwrap(),
        };
        assert_eq!(
            decode_placed_segment_backfill_reference_page_request(
                &encode_placed_segment_backfill_reference_page_request(&page_request).unwrap()
            )
            .unwrap(),
            page_request
        );
        for cursor in [
            PlacedSegmentBackfillReferenceCursor::StreamUploadSegment {
                session_id: crate::tests::stream_session_id("page-session"),
                segment_index: 3,
            },
            PlacedSegmentBackfillReferenceCursor::MultipartPartSegment {
                bucket: crate::tests::bucket_name("page-bucket"),
                key: crate::tests::object_key("page-key"),
                upload_id: crate::tests::multipart_upload_id("page-upload"),
                part_number: 4,
                segment_index: 5,
            },
            PlacedSegmentBackfillReferenceCursor::PendingCommand {
                cluster_epoch: ClusterEpoch::new(7).unwrap(),
                pg_id: PgId::new(3),
                log_index: 8,
                command_checksum: 9,
                reference_index: 6,
            },
        ] {
            let request = StorageRpcPlacedSegmentBackfillReferencePageRequest {
                route: route.clone(),
                after: Some(cursor),
                limit: NonZeroU16::new(32).unwrap(),
            };
            assert_eq!(
                decode_placed_segment_backfill_reference_page_request(
                    &encode_placed_segment_backfill_reference_page_request(&request).unwrap()
                )
                .unwrap(),
                request
            );
        }
        let page = PlacedSegmentBackfillReferencePage {
            items: vec![PlacedSegmentBackfillReferencePageItem {
                cursor: page_cursor,
                reference: match &references[0] {
                    ShardScavengerPayloadReference::Placed(reference) => reference.clone(),
                    ShardScavengerPayloadReference::ReclaimOnly(_) => unreachable!(),
                },
            }],
            complete: false,
        };
        assert_eq!(
            decode_placed_segment_backfill_reference_page_response(
                &encode_placed_segment_backfill_reference_page_response(&page).unwrap()
            )
            .unwrap(),
            page
        );
        let oversized_request = StorageRpcPlacedSegmentBackfillReferencePageRequest {
            route: route.clone(),
            after: None,
            limit: NonZeroU16::new(PLACED_SEGMENT_BACKFILL_REFERENCE_PAGE_LIMIT + 1).unwrap(),
        };
        assert!(matches!(
            encode_placed_segment_backfill_reference_page_request(&oversized_request),
            Err(StorageRpcPayloadError::PayloadTooLarge { .. })
        ));

        let observation_key = ShardScavengerObservationKey {
            node_id: 7,
            data_pg_id: 3,
            shard_index: key.shard_index(),
            shard_key: key,
        };
        let record = ShardScavengerObservationRecord {
            key: observation_key.clone(),
            data_size: Some(123),
            crc64: Some(0xBEEF),
            file_exists: true,
            shard_row_exists: true,
            reason: ShardScavengerObservationReason::UnreferencedShardRowAndFile,
            last_error: Some("scan delayed".to_string()),
        };
        let record_request = StorageRpcScavengerObservationRecordRequest {
            route: route.clone(),
            observation: record.clone(),
        };
        assert_eq!(
            decode_scavenger_observation_record_request(
                &encode_scavenger_observation_record_request(&record_request).unwrap()
            )
            .unwrap(),
            record_request
        );

        let observations = vec![ShardScavengerObservation {
            key: observation_key.clone(),
            first_seen_at: 10,
            last_seen_at: 11,
            observation_count: 2,
            data_size: record.data_size,
            crc64: record.crc64,
            file_exists: record.file_exists,
            shard_row_exists: record.shard_row_exists,
            reason: record.reason,
            last_error: record.last_error.clone(),
            resolved_at: Some(12),
        }];
        assert_eq!(
            decode_scavenger_observations_response(&encode_scavenger_observations_response(
                &observations
            ))
            .unwrap(),
            observations
        );
        let minimal_observation = ShardScavengerObservation {
            key: observation_key.clone(),
            first_seen_at: 10,
            last_seen_at: 11,
            observation_count: 1,
            data_size: None,
            crc64: None,
            file_exists: false,
            shard_row_exists: false,
            reason: ShardScavengerObservationReason::ScanIncomplete,
            last_error: None,
            resolved_at: None,
        };
        assert_eq!(
            encode_scavenger_observations_response(&[minimal_observation]).len(),
            4 + STORAGE_RPC_SCAVENGER_OBSERVATION_MIN_LEN
        );

        let key_request = StorageRpcScavengerObservationKeyRequest {
            route,
            key: observation_key,
        };
        assert_eq!(
            decode_scavenger_observation_key_request(
                &encode_scavenger_observation_key_request(&key_request).unwrap()
            )
            .unwrap(),
            key_request
        );
    }

    #[test]
    fn scavenger_metadata_decoders_reject_oversized_counts_before_allocation() {
        let mut oversized = Vec::new();
        put_u32(
            &mut oversized,
            u32::try_from(STORAGE_RPC_MAX_SCAVENGER_METADATA_ITEMS + 1).unwrap(),
        );
        assert!(matches!(
            decode_scavenger_shard_rows_response(&oversized),
            Err(StorageRpcPayloadError::PayloadTooLarge { .. })
        ));
        assert!(matches!(
            decode_scavenger_payload_references_response(&oversized),
            Err(StorageRpcPayloadError::PayloadTooLarge { .. })
        ));
        assert!(matches!(
            decode_scavenger_observations_response(&oversized),
            Err(StorageRpcPayloadError::PayloadTooLarge { .. })
        ));

        let mut truncated = Vec::new();
        put_u32(
            &mut truncated,
            STORAGE_RPC_MAX_SCAVENGER_METADATA_ITEMS as u32,
        );
        assert!(matches!(
            decode_scavenger_payload_references_response(&truncated),
            Err(StorageRpcPayloadError::Truncated)
        ));
        assert!(matches!(
            decode_scavenger_observations_response(&truncated),
            Err(StorageRpcPayloadError::Truncated)
        ));
    }

    #[test]
    fn scavenger_observation_key_requires_matching_shard_index() {
        let route = StorageRpcBucketPgRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            pg_id: PgId::new(3),
        };
        let mut bytes = encode_bucket_pg_request(&route).unwrap();
        put_u32(&mut bytes, 7);
        put_u32(&mut bytes, 3);
        put_u8(&mut bytes, 1);
        put_bytes(&mut bytes, test_shard_key(2).as_bytes());

        assert!(matches!(
            decode_scavenger_observation_key_request(&bytes),
            Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(_))
        ));
    }

    #[test]
    fn read_handle_acquire_request_requires_idempotency_key_and_locations() {
        let request = StorageRpcReadHandleAcquireRequest {
            read_operation_id: "read-op-1".to_string(),
            locations: vec![test_shard_location(0), test_shard_location(1)],
            shard_keys: vec![test_shard_key(0), test_shard_key(1)],
        };

        let bytes = encode_read_handle_acquire_request(&request).unwrap();
        let decoded = decode_read_handle_acquire_request(&bytes).unwrap();

        assert_eq!(decoded, request);
        assert_eq!(
            encode_read_handle_acquire_request(&StorageRpcReadHandleAcquireRequest {
                read_operation_id: String::new(),
                locations: vec![test_shard_location(0)],
                shard_keys: vec![test_shard_key(0)],
            }),
            Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
                "read operation id must not be empty",
            ))
        );
        assert_eq!(
            encode_read_handle_acquire_request(&StorageRpcReadHandleAcquireRequest {
                read_operation_id: "x".repeat(STORAGE_RPC_MAX_READ_OPERATION_ID_LEN + 1),
                locations: vec![test_shard_location(0)],
                shard_keys: vec![test_shard_key(0)],
            }),
            Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
                "read operation id exceeds maximum length",
            ))
        );
        assert_eq!(
            encode_read_handle_acquire_request(&StorageRpcReadHandleAcquireRequest {
                read_operation_id: "read-op-2".to_string(),
                locations: Vec::new(),
                shard_keys: Vec::new(),
            }),
            Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
                "read handle acquire must include at least one shard location",
            ))
        );
        assert_eq!(
            encode_read_handle_acquire_request(&StorageRpcReadHandleAcquireRequest {
                read_operation_id: "read-op-too-many-locations".to_string(),
                locations: (0..=STORAGE_RPC_MAX_READ_HANDLE_LOCATIONS)
                    .map(test_shard_location_for_data_pg)
                    .collect(),
                shard_keys: (0..=STORAGE_RPC_MAX_READ_HANDLE_LOCATIONS)
                    .map(|_| test_shard_key(0))
                    .collect(),
            }),
            Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
                "read handle acquire includes too many shard locations",
            ))
        );
    }

    #[test]
    fn object_payload_lease_control_request_and_response_round_trip() {
        let operations = [
            StorageRpcObjectPayloadLeaseControlOperation::Acquire,
            StorageRpcObjectPayloadLeaseControlOperation::Release,
            StorageRpcObjectPayloadLeaseControlOperation::ReclaimBegin,
            StorageRpcObjectPayloadLeaseControlOperation::ReclaimFinish,
            StorageRpcObjectPayloadLeaseControlOperation::ReclaimFinishKeepFence,
            StorageRpcObjectPayloadLeaseControlOperation::ReclaimFenceClear,
            StorageRpcObjectPayloadLeaseControlOperation::Count,
        ];
        for operation in operations {
            let reclaim_authority = matches!(
                operation,
                StorageRpcObjectPayloadLeaseControlOperation::ReclaimBegin
                    | StorageRpcObjectPayloadLeaseControlOperation::ReclaimFinish
                    | StorageRpcObjectPayloadLeaseControlOperation::ReclaimFinishKeepFence
                    | StorageRpcObjectPayloadLeaseControlOperation::ReclaimFenceClear
            )
            .then(|| ObjectPayloadReclaimClaimProof {
                bucket_incarnation_generation: 13,
                reclaim_kind: ObjectPayloadReclaimKind::ObjectSegments,
                claim_id: "claim-1".to_string(),
                owner_token: "owner-1".to_string(),
                cluster_epoch: ClusterEpoch::new(3).unwrap(),
            });
            let request = StorageRpcObjectPayloadLeaseControlRequest {
                node_id: NodeId::new(7),
                route_cluster_epoch: ClusterEpoch::new(3).unwrap(),
                bucket: BucketName::new("lease-bucket").unwrap(),
                key: ObjectKey::new("lease-key").unwrap(),
                generation_id: GenerationId::new(11).unwrap(),
                operation,
                reclaim_authority,
            };
            let encoded = encode_object_payload_lease_control_request(&request);
            assert!(
                encoded.len() <= STORAGE_RPC_MAX_OBJECT_PAYLOAD_LEASE_CONTROL_REQUEST_PAYLOAD_LEN
            );
            assert_eq!(
                decode_object_payload_lease_control_request(&encoded).unwrap(),
                request
            );
        }

        let response = StorageRpcObjectPayloadLeaseControlResponse { value: 19 };
        assert_eq!(
            decode_object_payload_lease_control_response(
                &encode_object_payload_lease_control_response(response)
            )
            .unwrap(),
            response
        );

        let request = StorageRpcObjectPayloadLeaseControlRequest {
            node_id: NodeId::new(7),
            route_cluster_epoch: ClusterEpoch::new(3).unwrap(),
            bucket: BucketName::new("lease-bucket").unwrap(),
            key: ObjectKey::new("lease-key").unwrap(),
            generation_id: GenerationId::new(11).unwrap(),
            operation: StorageRpcObjectPayloadLeaseControlOperation::Acquire,
            reclaim_authority: None,
        };
        let mut corrupt = encode_object_payload_lease_control_request(&request);
        *corrupt.last_mut().unwrap() = u8::MAX;
        assert!(matches!(
            decode_object_payload_lease_control_request(&corrupt),
            Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(_))
        ));
    }

    #[test]
    fn read_handle_acquire_request_rejects_corrupt_location_count_before_allocating() {
        let mut bytes = Vec::new();
        put_string(&mut bytes, "read-op-oom");
        put_u32(&mut bytes, u32::MAX);

        assert_eq!(
            decode_read_handle_acquire_request(&bytes),
            Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
                "read handle acquire includes too many shard locations",
            ))
        );
    }

    #[test]
    fn read_handle_request_decoders_reject_oversized_ids_before_copying() {
        let mut acquire_bytes = Vec::new();
        put_u32(
            &mut acquire_bytes,
            u32::try_from(STORAGE_RPC_MAX_READ_OPERATION_ID_LEN + 1).unwrap(),
        );
        assert_eq!(
            decode_read_handle_acquire_request(&acquire_bytes),
            Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
                "read operation id exceeds maximum length",
            ))
        );

        let mut release_bytes = Vec::new();
        put_u32(
            &mut release_bytes,
            u32::try_from(STORAGE_RPC_MAX_READ_OPERATION_ID_LEN + 1).unwrap(),
        );
        assert_eq!(
            decode_read_handle_release_request(&release_bytes),
            Err(StorageRpcPayloadError::InvalidReadHandleReleaseRequest(
                "read operation id exceeds maximum length",
            ))
        );
    }

    #[test]
    fn read_handle_acquire_request_rejects_noncanonical_location_sets() {
        assert_eq!(
            encode_read_handle_acquire_request(&StorageRpcReadHandleAcquireRequest {
                read_operation_id: "read-op-duplicate".to_string(),
                locations: vec![test_shard_location(1), test_shard_location(1)],
                shard_keys: vec![test_shard_key(1), test_shard_key(1)],
            }),
            Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
                "read handle acquire locations must be sorted and unique",
            ))
        );
        assert_eq!(
            encode_read_handle_acquire_request(&StorageRpcReadHandleAcquireRequest {
                read_operation_id: "read-op-unsorted".to_string(),
                locations: vec![test_shard_location(1), test_shard_location(0)],
                shard_keys: vec![test_shard_key(1), test_shard_key(0)],
            }),
            Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
                "read handle acquire locations must be sorted and unique",
            ))
        );
    }

    #[test]
    fn storage_rpc_request_frame_rejects_payload_over_kind_limit_before_allocating() {
        for (kind, payload_len, limit) in [
            (
                StorageRpcMessageKind::Health,
                STORAGE_RPC_EMPTY_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_EMPTY_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ClusterMapHistoryReferenceSummary,
                4 + 8 + 1,
                4 + 8,
            ),
            (
                StorageRpcMessageKind::MetadataCommand,
                STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ShardWrite,
                STORAGE_RPC_MAX_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ReadHandlesAcquire,
                STORAGE_RPC_MAX_READ_HANDLE_ACQUIRE_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_READ_HANDLE_ACQUIRE_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ReadHandlesRelease,
                STORAGE_RPC_MAX_READ_HANDLE_RELEASE_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_READ_HANDLE_RELEASE_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectPayloadLeaseControl,
                STORAGE_RPC_MAX_OBJECT_PAYLOAD_LEASE_CONTROL_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_OBJECT_PAYLOAD_LEASE_CONTROL_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectPayloadReclaimCommandBuild,
                STORAGE_RPC_MAX_OBJECT_PAYLOAD_RECLAIM_COMMAND_BUILD_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_OBJECT_PAYLOAD_RECLAIM_COMMAND_BUILD_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ShardRead,
                STORAGE_RPC_MAX_SHARD_READ_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_SHARD_READ_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ShardReadRange,
                STORAGE_RPC_MAX_SHARD_READ_RANGE_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_SHARD_READ_RANGE_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ShardDelete,
                STORAGE_RPC_MAX_SHARD_DELETE_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_SHARD_DELETE_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ShardAckLoad,
                STORAGE_RPC_MAX_SHARD_ACK_ITEM_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_SHARD_ACK_ITEM_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ShardAckHistoricalLoad,
                STORAGE_RPC_MAX_SHARD_ACK_ITEM_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_SHARD_ACK_ITEM_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ShardAckDelete,
                STORAGE_RPC_MAX_SHARD_ACK_ITEM_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_SHARD_ACK_ITEM_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ShardAckRecord,
                STORAGE_RPC_MAX_SHARD_ACK_BATCH_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_SHARD_ACK_BATCH_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ShardAckValidate,
                STORAGE_RPC_MAX_SHARD_ACK_BATCH_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_SHARD_ACK_BATCH_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ShardScavengerListFiles,
                STORAGE_RPC_MAX_SCAVENGER_LIST_FILES_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_SCAVENGER_LIST_FILES_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ShardScavengerShardRows,
                STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ShardScavengerPayloadReferences,
                STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::PlacedSegmentBackfillReferencePage,
                STORAGE_RPC_MAX_PLACED_SEGMENT_BACKFILL_REFERENCE_PAGE_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_PLACED_SEGMENT_BACKFILL_REFERENCE_PAGE_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ShardScavengerObservations,
                STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ShardScavengerObservationRecord,
                STORAGE_RPC_MAX_SCAVENGER_OBSERVATION_RECORD_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_SCAVENGER_OBSERVATION_RECORD_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ShardScavengerObservationResolve,
                STORAGE_RPC_MAX_SCAVENGER_OBSERVATION_KEY_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_SCAVENGER_OBSERVATION_KEY_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ClaimHeartbeat,
                STORAGE_RPC_MAX_CLAIM_HEARTBEAT_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_CLAIM_HEARTBEAT_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ClaimRelease,
                STORAGE_RPC_MAX_CLAIM_RELEASE_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_CLAIM_RELEASE_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ProofRelease,
                STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_PROOF_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_PROOF_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::MetadataCommandReplicaState,
                STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::MetadataCommandAcceptance,
                STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::MetadataCommandAbandonAcceptance,
                STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::MetadataCommandPendingSlotInsert,
                STORAGE_RPC_MAX_METADATA_COMMAND_PENDING_SLOT_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_PENDING_SLOT_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::MetadataCommandPendingSlotRemove,
                STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::MetadataCommandAppliedLogHashes,
                STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::MetadataCommandMatchingAppliedLog,
                STORAGE_RPC_MAX_METADATA_COMMAND_MATCHING_APPLIED_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_MATCHING_APPLIED_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::MetadataCommandRetainedLogHashes,
                STORAGE_RPC_MAX_METADATA_COMMAND_LOG_HASH_RANGE_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_LOG_HASH_RANGE_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::MetadataCommandAbandoned,
                STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::MetadataCommandRecordAbandoned,
                STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::MetadataCommandPendingSlotReplace,
                STORAGE_RPC_MAX_METADATA_COMMAND_PENDING_SLOT_REPLACE_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_PENDING_SLOT_REPLACE_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::MetadataCommandBucketControlPendingSlotInsert,
                STORAGE_RPC_MAX_METADATA_COMMAND_PENDING_SLOT_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_PENDING_SLOT_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::MetadataCommandApplyAndRecord,
                STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::MetadataCommandRecoveryApplyAndRecord,
                STORAGE_RPC_MAX_METADATA_COMMAND_RECOVERY_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_RECOVERY_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::MetadataCommandRecoveryRecordAbandoned,
                STORAGE_RPC_MAX_METADATA_COMMAND_RECOVERY_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_RECOVERY_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::MetadataCommandRecoveryPendingSlotReplace,
                STORAGE_RPC_MAX_METADATA_COMMAND_RECOVERY_PENDING_SLOT_REPLACE_REQUEST_PAYLOAD_LEN
                    + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_RECOVERY_PENDING_SLOT_REPLACE_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectGenerationNext,
                STORAGE_RPC_MAX_OBJECT_GENERATION_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_OBJECT_GENERATION_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectGenerationReservation,
                STORAGE_RPC_MAX_OBJECT_GENERATION_RESERVATION_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_OBJECT_GENERATION_RESERVATION_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectStreamUploadBucketWriteReservationUpdate,
                STORAGE_RPC_MAX_STREAM_UPLOAD_BUCKET_WRITE_RESERVATION_UPDATE_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_STREAM_UPLOAD_BUCKET_WRITE_RESERVATION_UPDATE_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectStreamSegmentAppendPrepare,
                STORAGE_RPC_MAX_STREAM_SEGMENT_APPEND_PREPARE_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_STREAM_SEGMENT_APPEND_PREPARE_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectVersionNext,
                STORAGE_RPC_MAX_OBJECT_VERSION_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_OBJECT_VERSION_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectPayloadReclaimExists,
                STORAGE_RPC_MAX_OBJECT_PAYLOAD_RECLAIM_EXISTS_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_OBJECT_PAYLOAD_RECLAIM_EXISTS_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectBucketPayloadReclaimRoot,
                STORAGE_RPC_MAX_BUCKET_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_BUCKET_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::DirectPutCommitSnapshotLoad,
                STORAGE_RPC_MAX_DIRECT_PUT_SNAPSHOT_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_DIRECT_PUT_SNAPSHOT_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::DirectPutCommitCommandBuild,
                STORAGE_RPC_MAX_DIRECT_PUT_COMMAND_BUILD_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_DIRECT_PUT_COMMAND_BUILD_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectReadAuthSubjectLoad,
                STORAGE_RPC_MAX_OBJECT_READ_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_OBJECT_READ_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectReadSnapshotLoad,
                STORAGE_RPC_MAX_OBJECT_READ_SNAPSHOT_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_OBJECT_READ_SNAPSHOT_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectMetadataPutSnapshotLoad,
                STORAGE_RPC_MAX_OBJECT_METADATA_SNAPSHOT_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_OBJECT_METADATA_SNAPSHOT_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectDeleteCurrentSnapshotLoad,
                STORAGE_RPC_MAX_OBJECT_METADATA_SNAPSHOT_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_OBJECT_METADATA_SNAPSHOT_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectDeleteSpecificSnapshotLoad,
                STORAGE_RPC_MAX_OBJECT_METADATA_SNAPSHOT_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_OBJECT_METADATA_SNAPSHOT_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectLifecycleVersionListLoad,
                STORAGE_RPC_MAX_OBJECT_METADATA_SNAPSHOT_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_OBJECT_METADATA_SNAPSHOT_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectMetadataPutCommandBuild,
                STORAGE_RPC_MAX_OBJECT_METADATA_COMMAND_BUILD_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_OBJECT_METADATA_COMMAND_BUILD_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectDeleteSpecificCommandBuild,
                STORAGE_RPC_MAX_OBJECT_METADATA_COMMAND_BUILD_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_OBJECT_METADATA_COMMAND_BUILD_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectDeleteCurrentCommandBuild,
                STORAGE_RPC_MAX_OBJECT_METADATA_COMMAND_BUILD_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_OBJECT_METADATA_COMMAND_BUILD_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectInsertDeleteMarkerCommandBuild,
                STORAGE_RPC_MAX_OBJECT_METADATA_COMMAND_BUILD_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_OBJECT_METADATA_COMMAND_BUILD_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::BucketWriteReservationAcquire,
                STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ACQUIRE_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ACQUIRE_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::BucketWriteReservationValidate,
                STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_PROOF_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_PROOF_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::BucketWriteReservationHeartbeat,
                STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_HEARTBEAT_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_HEARTBEAT_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::BucketWriteReservationRelease,
                STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_RECORD_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_RECORD_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::BucketSnapshotLoad,
                STORAGE_RPC_MAX_BUCKET_SNAPSHOT_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_BUCKET_SNAPSHOT_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::BucketWriteDrainExists,
                STORAGE_RPC_MAX_BUCKET_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_BUCKET_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::BucketWriteDrainGet,
                STORAGE_RPC_MAX_BUCKET_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_BUCKET_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::BucketDeleteAttemptOutcomeRecord,
                STORAGE_RPC_MAX_BUCKET_DELETE_ATTEMPT_OUTCOME_RECORD_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_BUCKET_DELETE_ATTEMPT_OUTCOME_RECORD_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::BucketDeleteAttemptOutcomeGet,
                STORAGE_RPC_MAX_BUCKET_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_BUCKET_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::BucketMetadataControlPendingMatch,
                STORAGE_RPC_MAX_BUCKET_METADATA_CONTROL_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_BUCKET_METADATA_CONTROL_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::BucketMetadataControlCommandBuild,
                STORAGE_RPC_MAX_BUCKET_METADATA_CONTROL_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_BUCKET_METADATA_CONTROL_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::BucketMarkDeletingCommandBuild,
                STORAGE_RPC_MAX_BUCKET_METADATA_CONTROL_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_BUCKET_METADATA_CONTROL_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::BucketSubresourceGet,
                STORAGE_RPC_MAX_BUCKET_SUBRESOURCE_GET_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_BUCKET_SUBRESOURCE_GET_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::BucketList,
                STORAGE_RPC_MAX_BUCKET_LIST_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_BUCKET_LIST_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::BucketExecutionGenerations,
                STORAGE_RPC_MAX_BUCKET_BATCH_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_BUCKET_BATCH_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::BucketFastPathIdentities,
                STORAGE_RPC_MAX_BUCKET_BATCH_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_BUCKET_BATCH_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::LifecycleSweepBucketsList,
                STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectAbortingMultipartUploadBucketsList,
                STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_METADATA_COMMAND_STATE_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::LifecycleSweepRoots,
                STORAGE_RPC_MAX_LIFECYCLE_SWEEP_ROOTS_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_LIFECYCLE_SWEEP_ROOTS_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::LifecycleSweepClaimAcquire,
                STORAGE_RPC_MAX_LIFECYCLE_SWEEP_CLAIM_ACQUIRE_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_LIFECYCLE_SWEEP_CLAIM_ACQUIRE_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::LifecycleSweepClaimHeartbeat,
                STORAGE_RPC_MAX_LIFECYCLE_SWEEP_CLAIM_RECORD_PAYLOAD_LEN + 8 + 1 + 8 + 1,
                STORAGE_RPC_MAX_LIFECYCLE_SWEEP_CLAIM_RECORD_PAYLOAD_LEN + 8 + 1 + 8,
            ),
            (
                StorageRpcMessageKind::LifecycleSweepClaimError,
                STORAGE_RPC_MAX_LIFECYCLE_SWEEP_CLAIM_ERROR_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_LIFECYCLE_SWEEP_CLAIM_ERROR_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::LifecycleSweepClaimRelease,
                STORAGE_RPC_MAX_LIFECYCLE_SWEEP_CLAIM_RECORD_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_LIFECYCLE_SWEEP_CLAIM_RECORD_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectListPage,
                STORAGE_RPC_MAX_LIST_OBJECTS_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_LIST_OBJECTS_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectVersionListPage,
                STORAGE_RPC_MAX_LIST_OBJECT_VERSIONS_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_LIST_OBJECT_VERSIONS_REQUEST_PAYLOAD_LEN,
            ),
            (
                StorageRpcMessageKind::ObjectMultipartUploadListPage,
                STORAGE_RPC_MAX_LIST_MULTIPART_UPLOADS_REQUEST_PAYLOAD_LEN + 1,
                STORAGE_RPC_MAX_LIST_MULTIPART_UPLOADS_REQUEST_PAYLOAD_LEN,
            ),
        ] {
            let mut bytes = Vec::new();
            put_bytes(&mut bytes, STORAGE_RPC_FRAME_MAGIC);
            put_u16(&mut bytes, STORAGE_RPC_FRAME_ENCODING_VERSION);
            put_u64(&mut bytes, 7);
            put_u16(&mut bytes, kind as u16);
            put_u32(
                &mut bytes,
                u32::try_from(payload_len).expect("test payload length fits in u32"),
            );

            assert!(matches!(
                read_storage_rpc_request_frame_from(&mut Cursor::new(bytes)),
                Err(StorageRpcStreamError::Frame(StorageRpcFrameError::PayloadTooLarge {
                    len,
                    limit: actual_limit,
                })) if len == payload_len && actual_limit == limit
            ));
        }
    }

    #[test]
    fn object_payload_reclaim_command_build_frame_accepts_exact_kind_cap() {
        let payload =
            vec![0; STORAGE_RPC_MAX_OBJECT_PAYLOAD_RECLAIM_COMMAND_BUILD_REQUEST_PAYLOAD_LEN];
        let encoded = encode_storage_rpc_frame(
            11,
            StorageRpcMessageKind::ObjectPayloadReclaimCommandBuild,
            &payload,
        )
        .unwrap();
        let decoded = read_storage_rpc_request_frame_from(&mut Cursor::new(encoded)).unwrap();

        assert_eq!(
            decoded.kind,
            StorageRpcMessageKind::ObjectPayloadReclaimCommandBuild
        );
        assert_eq!(decoded.payload.len(), payload.len());
    }

    #[test]
    fn maximum_multipart_upload_list_request_fits_request_frame_cap() {
        let request = StorageRpcListMultipartUploadsRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::INITIAL,
            pg_id: PgId::new(3),
            request: ListMultipartUploadsReq {
                bucket: BucketName::try_from("b".repeat(STORAGE_RPC_MAX_BUCKET_NAME_LEN)).unwrap(),
                prefix: Some(
                    ObjectKey::try_from("p".repeat(STORAGE_RPC_MAX_OBJECT_KEY_LEN)).unwrap(),
                ),
                page_start: Some(ListMultipartUploadsPageStart::After {
                    key_marker: ObjectKey::try_from("k".repeat(STORAGE_RPC_MAX_OBJECT_KEY_LEN))
                        .unwrap(),
                    upload_id_marker: Some(UploadId::try_from("u".repeat(UPLOAD_ID_LEN)).unwrap()),
                }),
                max_uploads: STORAGE_RPC_MAX_LIST_PAGE_ITEMS,
            },
        };
        let payload = encode_list_multipart_uploads_request(&request).unwrap();
        assert_eq!(
            payload.len(),
            STORAGE_RPC_MAX_LIST_MULTIPART_UPLOADS_REQUEST_PAYLOAD_LEN
        );

        let encoded = encode_storage_rpc_frame(
            11,
            StorageRpcMessageKind::ObjectMultipartUploadListPage,
            &payload,
        )
        .unwrap();
        let decoded = read_storage_rpc_request_frame_from(&mut Cursor::new(encoded)).unwrap();
        assert_eq!(decoded.payload, payload);
        let decoded_request = decode_list_multipart_uploads_request(&decoded.payload).unwrap();
        assert_eq!(decoded_request.request.bucket, request.request.bucket);
        assert_eq!(decoded_request.request.prefix, request.request.prefix);
        assert_eq!(
            decoded_request.request.page_start,
            request.request.page_start
        );
        assert_eq!(
            decoded_request.request.max_uploads,
            request.request.max_uploads
        );
    }

    #[test]
    fn bucket_delete_coordination_max_requests_fit_kind_caps() {
        let bucket = BucketName::try_from("a".repeat(STORAGE_RPC_MAX_BUCKET_NAME_LEN)).unwrap();
        let cluster_epoch = ClusterEpoch::INITIAL;
        let route_cluster_epoch = ClusterEpoch::new(cluster_epoch.get() + 1).unwrap();
        let node_id = NodeId::new(1);
        let pg_id = PgId::new(2);
        let drain_id = "d".repeat(STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN);
        let owner_token = "o".repeat(STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN);

        let drain_begin_request = StorageRpcBucketWriteDrainBeginRequest {
            bucket: StorageRpcBucketRequest {
                node_id,
                cluster_epoch,
                pg_id,
                bucket: bucket.clone(),
            },
            drain_id: drain_id.clone(),
            owner_token: owner_token.clone(),
            created_at: 1,
            lease_deadline: 2,
            effect_deadline: Some(StorageRpcAdmittedRouteEffectDeadline {
                authority_valid_until_ms: 10_000,
                portable_wall_valid_until_ms: 10_000_u64
                    .saturating_sub(crate::control_plane_lease::CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS),
            }),
        };
        let drain_begin_payload =
            encode_bucket_write_drain_begin_request(&drain_begin_request).unwrap();
        assert_eq!(
            drain_begin_payload.len(),
            STORAGE_RPC_MAX_BUCKET_WRITE_DRAIN_BEGIN_PAYLOAD_LEN
        );
        let drain_begin_frame = encode_storage_rpc_frame(
            10,
            StorageRpcMessageKind::BucketWriteDrainBegin,
            &drain_begin_payload,
        )
        .unwrap();
        let decoded =
            read_storage_rpc_request_frame_from(&mut Cursor::new(drain_begin_frame)).unwrap();
        assert_eq!(decoded.payload, drain_begin_payload);
        assert_eq!(
            decode_bucket_write_drain_begin_request(&decoded.payload).unwrap(),
            drain_begin_request
        );

        let drain_payload =
            encode_bucket_write_drain_record_request(&StorageRpcBucketWriteDrainRecordRequest {
                node_id,
                route_cluster_epoch,
                pg_id,
                record: BucketWriteDrainRecord {
                    bucket: bucket.clone(),
                    drain_id: drain_id.clone(),
                    owner_token: owner_token.clone(),
                    cluster_epoch,
                    bucket_execution_generation: 3,
                    state: BucketWriteDrainState::Draining,
                    created_at: 4,
                    lease_deadline: 5,
                },
            })
            .unwrap();
        assert_eq!(
            drain_payload.len(),
            STORAGE_RPC_MAX_BUCKET_WRITE_DRAIN_RECORD_PAYLOAD_LEN
        );
        let drain_frame = encode_storage_rpc_frame(
            11,
            StorageRpcMessageKind::BucketWriteDrainClear,
            &drain_payload,
        )
        .unwrap();
        let decoded = read_storage_rpc_request_frame_from(&mut Cursor::new(drain_frame)).unwrap();
        assert_eq!(decoded.payload, drain_payload);
        let decoded_request = decode_bucket_write_drain_record_request(&drain_payload).unwrap();
        assert_eq!(decoded_request.route_cluster_epoch, route_cluster_epoch);
        assert_eq!(decoded_request.record.cluster_epoch, cluster_epoch);

        let heartbeat_request = StorageRpcBucketWriteDrainHeartbeatRequest {
            node_id,
            route_cluster_epoch,
            pg_id,
            record: decoded_request.record,
            lease_deadline: 6,
        };
        assert_eq!(
            decode_bucket_write_drain_heartbeat_request(
                &encode_bucket_write_drain_heartbeat_request(&heartbeat_request).unwrap()
            )
            .unwrap(),
            heartbeat_request
        );

        let mut oversized_drain_payload = drain_payload.clone();
        oversized_drain_payload.push(0);
        let oversized_drain_frame = encode_storage_rpc_frame(
            12,
            StorageRpcMessageKind::BucketWriteDrainClear,
            &oversized_drain_payload,
        )
        .unwrap();
        assert!(matches!(
            read_storage_rpc_request_frame_from(&mut Cursor::new(oversized_drain_frame)),
            Err(StorageRpcStreamError::Frame(
                StorageRpcFrameError::PayloadTooLarge { len, limit }
            )) if len == STORAGE_RPC_MAX_BUCKET_WRITE_DRAIN_RECORD_PAYLOAD_LEN + 1
                && limit == STORAGE_RPC_MAX_BUCKET_WRITE_DRAIN_RECORD_PAYLOAD_LEN
        ));

        let outcome_route_epoch = route_cluster_epoch;
        let outcome_payload = encode_bucket_delete_attempt_outcome_record_request(
            &StorageRpcBucketDeleteAttemptOutcomeRecordRequest {
                node_id,
                cluster_epoch: outcome_route_epoch,
                pg_id,
                record: BucketDeleteAttemptOutcomeRecord {
                    bucket: bucket.clone(),
                    drain_id: drain_id.clone(),
                    cluster_epoch,
                    bucket_execution_generation: 3,
                    outcome: BucketDeleteAttemptOutcomeKind::Retryable,
                    phase: BucketDeleteAttemptPhase::FinalVisibilityProven,
                    detail: "e".repeat(BUCKET_DELETE_ATTEMPT_OUTCOME_DETAIL_MAX_LEN),
                    post_reservation_next_object_pg_id: Some(7),
                    stream_cleanup_next_object_pg_id: Some(8),
                    stream_cleanup_next_session_id_marker: Some(
                        SessionId::try_from("ab".repeat(16)).unwrap(),
                    ),
                    stream_cleanup_aborted_uploads: true,
                    final_visibility_next_object_pg_id: Some(9),
                    finalizer_next_object_pg_id: Some(9),
                    updated_at: 6,
                },
            },
        )
        .unwrap();
        assert_eq!(
            outcome_payload.len(),
            STORAGE_RPC_MAX_BUCKET_DELETE_ATTEMPT_OUTCOME_RECORD_PAYLOAD_LEN
        );
        let outcome_frame = encode_storage_rpc_frame(
            13,
            StorageRpcMessageKind::BucketDeleteAttemptOutcomeRecord,
            &outcome_payload,
        )
        .unwrap();
        let decoded = read_storage_rpc_request_frame_from(&mut Cursor::new(outcome_frame)).unwrap();
        assert_eq!(decoded.payload, outcome_payload);
        let decoded_request =
            decode_bucket_delete_attempt_outcome_record_request(&outcome_payload).unwrap();
        assert_eq!(decoded_request.cluster_epoch, outcome_route_epoch);
        assert_eq!(decoded_request.record.cluster_epoch, cluster_epoch);

        let mut oversized_outcome_payload = outcome_payload.clone();
        oversized_outcome_payload.push(0);
        let oversized_outcome_frame = encode_storage_rpc_frame(
            14,
            StorageRpcMessageKind::BucketDeleteAttemptOutcomeRecord,
            &oversized_outcome_payload,
        )
        .unwrap();
        assert!(matches!(
            read_storage_rpc_request_frame_from(&mut Cursor::new(oversized_outcome_frame)),
            Err(StorageRpcStreamError::Frame(
                StorageRpcFrameError::PayloadTooLarge { len, limit }
            )) if len == STORAGE_RPC_MAX_BUCKET_DELETE_ATTEMPT_OUTCOME_RECORD_PAYLOAD_LEN + 1
                && limit == STORAGE_RPC_MAX_BUCKET_DELETE_ATTEMPT_OUTCOME_RECORD_PAYLOAD_LEN
        ));

        let claim_payload = encode_bucket_delete_finalize_claim_record_request(
            &StorageRpcBucketDeleteFinalizeClaimRecordRequest {
                node_id,
                cluster_epoch,
                pg_id,
                record: BucketDeleteFinalizeClaimRecord {
                    bucket,
                    bucket_incarnation_generation: 6,
                    claim_id: drain_id,
                    owner_token,
                    cluster_epoch,
                    pg_id: pg_id.get(),
                    claimed_at: 7,
                    lease_deadline: Some(8),
                    attempt_count: 9,
                    last_error: Some("e".repeat(STORAGE_RPC_MAX_BUCKET_WRITE_TARGET_CONTEXT_LEN)),
                },
            },
        )
        .unwrap();
        assert_eq!(
            claim_payload.len(),
            STORAGE_RPC_MAX_BUCKET_DELETE_FINALIZE_CLAIM_RECORD_PAYLOAD_LEN
        );
        let claim_frame = encode_storage_rpc_frame(
            13,
            StorageRpcMessageKind::BucketDeleteFinalizeClaimRelease,
            &claim_payload,
        )
        .unwrap();
        let decoded = read_storage_rpc_request_frame_from(&mut Cursor::new(claim_frame)).unwrap();
        assert_eq!(decoded.payload, claim_payload);

        let mut oversized_claim_payload = claim_payload;
        oversized_claim_payload.push(0);
        let oversized_claim_frame = encode_storage_rpc_frame(
            14,
            StorageRpcMessageKind::BucketDeleteFinalizeClaimRelease,
            &oversized_claim_payload,
        )
        .unwrap();
        assert!(matches!(
            read_storage_rpc_request_frame_from(&mut Cursor::new(oversized_claim_frame)),
            Err(StorageRpcStreamError::Frame(
                StorageRpcFrameError::PayloadTooLarge { len, limit }
            )) if len == STORAGE_RPC_MAX_BUCKET_DELETE_FINALIZE_CLAIM_RECORD_PAYLOAD_LEN + 1
                && limit == STORAGE_RPC_MAX_BUCKET_DELETE_FINALIZE_CLAIM_RECORD_PAYLOAD_LEN
        ));
    }

    #[test]
    fn maximum_lifecycle_sweep_claim_acquire_request_fits_kind_cap() {
        let request = StorageRpcLifecycleSweepClaimAcquireRequest {
            bucket: StorageRpcBucketRequest {
                node_id: NodeId::new(1),
                cluster_epoch: ClusterEpoch::INITIAL,
                pg_id: PgId::new(2),
                bucket: BucketName::try_from("b".repeat(STORAGE_RPC_MAX_BUCKET_NAME_LEN)).unwrap(),
            },
            bucket_incarnation_generation: 3,
            claim_id: "c".repeat(STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN),
            owner_token: "o".repeat(STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN),
            claimed_at: 4,
            lease_deadline: Some(5),
            now: 4,
        };
        let payload = encode_lifecycle_sweep_claim_acquire_request(&request).unwrap();
        assert_eq!(
            payload.len(),
            STORAGE_RPC_MAX_LIFECYCLE_SWEEP_CLAIM_ACQUIRE_PAYLOAD_LEN
        );
        let frame = encode_storage_rpc_frame(
            15,
            StorageRpcMessageKind::LifecycleSweepClaimAcquire,
            &payload,
        )
        .unwrap();
        let decoded = read_storage_rpc_request_frame_from(&mut Cursor::new(frame)).unwrap();
        assert_eq!(decoded.payload, payload);
        assert_eq!(
            decode_lifecycle_sweep_claim_acquire_request(&decoded.payload).unwrap(),
            request
        );
    }

    #[test]
    fn shard_read_response_can_exceed_shard_read_request_frame_limit() {
        let payload = vec![0x4a; STORAGE_RPC_MAX_SHARD_READ_PAYLOAD_LEN + 1];
        let frame_bytes =
            encode_storage_rpc_frame(9, StorageRpcMessageKind::ShardRead, &payload).unwrap();
        let frame = read_storage_rpc_frame_from(&mut Cursor::new(frame_bytes)).unwrap();

        assert_eq!(frame.kind, StorageRpcMessageKind::ShardRead);
        assert_eq!(frame.payload, payload);
    }

    #[test]
    fn read_handle_acquire_response_round_trips_canonical_locations() {
        let response = StorageRpcReadHandleAcquireResponse {
            locations: vec![test_shard_location(0), test_shard_location(1)],
        };

        let bytes = encode_read_handle_acquire_response(&response).unwrap();
        let decoded = decode_read_handle_acquire_response(&bytes).unwrap();

        assert_eq!(decoded, response);
        assert_eq!(
            encode_read_handle_acquire_response(&StorageRpcReadHandleAcquireResponse {
                locations: Vec::new(),
            }),
            Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
                "read handle acquire response must include at least one shard location",
            ))
        );
        assert_eq!(
            encode_read_handle_acquire_response(&StorageRpcReadHandleAcquireResponse {
                locations: vec![test_shard_location(1), test_shard_location(0)],
            }),
            Err(StorageRpcPayloadError::InvalidReadHandleAcquireRequest(
                "read handle acquire locations must be sorted and unique",
            ))
        );
    }

    #[test]
    fn read_handle_release_request_and_response_round_trip() {
        let request = StorageRpcReadHandleReleaseRequest {
            read_operation_id: "read-op-release".to_string(),
        };

        let request_bytes = encode_read_handle_release_request(&request).unwrap();
        let decoded_request = decode_read_handle_release_request(&request_bytes).unwrap();

        assert_eq!(decoded_request, request);
        assert_eq!(
            encode_read_handle_release_request(&StorageRpcReadHandleReleaseRequest {
                read_operation_id: String::new(),
            }),
            Err(StorageRpcPayloadError::InvalidReadHandleReleaseRequest(
                "read operation id must not be empty",
            ))
        );
        assert_eq!(
            encode_read_handle_release_request(&StorageRpcReadHandleReleaseRequest {
                read_operation_id: "x".repeat(STORAGE_RPC_MAX_READ_OPERATION_ID_LEN + 1),
            }),
            Err(StorageRpcPayloadError::InvalidReadHandleReleaseRequest(
                "read operation id exceeds maximum length",
            ))
        );
        let response_bytes =
            encode_read_handle_release_response(&StorageRpcReadHandleReleaseResponse);
        let decoded_response = decode_read_handle_release_response(&response_bytes).unwrap();
        assert_eq!(decoded_response, StorageRpcReadHandleReleaseResponse);
        assert_eq!(
            decode_read_handle_release_response(&[1]),
            Err(StorageRpcPayloadError::TrailingBytes)
        );
    }

    #[test]
    fn claim_heartbeat_and_release_requests_are_token_fenced() {
        let token = test_claim_token();
        let heartbeat = StorageRpcClaimHeartbeatRequest {
            token: token.clone(),
            heartbeat_at: 100,
            lease_deadline: Some(160),
        };
        let heartbeat_bytes = encode_claim_heartbeat_request(&heartbeat).unwrap();
        let decoded_heartbeat = decode_claim_heartbeat_request(&heartbeat_bytes).unwrap();

        assert_eq!(decoded_heartbeat, heartbeat);
        assert_eq!(
            encode_claim_heartbeat_request(&StorageRpcClaimHeartbeatRequest {
                token: token.clone(),
                heartbeat_at: 100,
                lease_deadline: Some(100),
            }),
            Err(StorageRpcPayloadError::InvalidDurableClaimToken(
                "claim heartbeat lease deadline must be after heartbeat time",
            ))
        );

        let release = StorageRpcClaimReleaseRequest { token };
        let release_bytes = encode_claim_release_request(&release).unwrap();
        let decoded_release = decode_claim_release_request(&release_bytes).unwrap();

        assert_eq!(decoded_release, release);
    }

    #[test]
    fn object_reclaim_claim_release_request_carries_full_work_identity() {
        let release = StorageRpcClaimReleaseRequest {
            token: test_object_reclaim_claim_token(),
        };

        let bytes = encode_claim_release_request(&release).unwrap();
        let decoded = decode_claim_release_request(&bytes).unwrap();

        assert_eq!(decoded, release);
    }

    #[test]
    fn proof_release_request_carries_full_reservation_identity() {
        let request = StorageRpcProofReleaseRequest {
            node_id: NodeId::new(7),
            route_cluster_epoch: ClusterEpoch::new(2).unwrap(),
            pg_id: PgId::new(3),
            proof: test_bucket_write_reservation_proof(),
        };

        let bytes = encode_proof_release_request(&request).unwrap();
        let decoded = decode_proof_release_request(&bytes).unwrap();

        assert_eq!(decoded, request);
    }

    #[test]
    fn object_listing_requests_and_responses_round_trip() {
        let bucket = BucketName::try_from("bucket").unwrap();
        let prefix = ObjectKey::try_from("prefix/").unwrap();
        let key_marker = ObjectKey::try_from("prefix/key").unwrap();
        let upload_id = UploadId::try_from("u".repeat(UPLOAD_ID_LEN)).unwrap();

        let objects = StorageRpcListObjectsRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::INITIAL,
            pg_id: PgId::new(3),
            request: ListObjectsReq {
                bucket: bucket.clone(),
                prefix: Some(prefix.clone()),
                start_after: Some(key_marker.clone()),
                start_at: None,
                max_keys: 9,
            },
        };
        let bytes = encode_list_objects_request(&objects).unwrap();
        let decoded = decode_list_objects_request(&bytes).unwrap();
        assert_eq!(decoded.node_id, objects.node_id);
        assert_eq!(decoded.cluster_epoch, objects.cluster_epoch);
        assert_eq!(decoded.pg_id, objects.pg_id);
        assert_eq!(decoded.request.bucket, objects.request.bucket);
        assert_eq!(decoded.request.prefix, objects.request.prefix);
        assert_eq!(decoded.request.start_after, objects.request.start_after);
        assert_eq!(decoded.request.start_at, objects.request.start_at);
        assert_eq!(decoded.request.max_keys, objects.request.max_keys);

        let response = StorageRpcListObjectsResponse {
            response: ListObjectsResp {
                objects: Vec::new(),
                is_truncated: true,
                next_start_after: Some(key_marker.clone()),
            },
        };
        let bytes = encode_list_objects_response(&response).unwrap();
        let decoded = decode_list_objects_response(&bytes).unwrap();
        assert!(decoded.response.objects.is_empty());
        assert!(decoded.response.is_truncated);
        assert_eq!(decoded.response.next_start_after, Some(key_marker.clone()));

        let versions = StorageRpcListObjectVersionsRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::INITIAL,
            pg_id: PgId::new(3),
            request: ListObjectVersionsReq {
                bucket: bucket.clone(),
                prefix: Some(prefix.clone()),
                key_marker: Some(key_marker.clone()),
                version_id_marker: Some(VersionId::from_u64(42)),
                start_at: None,
                max_keys: 10,
            },
        };
        let bytes = encode_list_object_versions_request(&versions).unwrap();
        let decoded = decode_list_object_versions_request(&bytes).unwrap();
        assert_eq!(decoded.node_id, versions.node_id);
        assert_eq!(decoded.cluster_epoch, versions.cluster_epoch);
        assert_eq!(decoded.pg_id, versions.pg_id);
        assert_eq!(decoded.request.bucket, versions.request.bucket);
        assert_eq!(decoded.request.prefix, versions.request.prefix);
        assert_eq!(decoded.request.key_marker, versions.request.key_marker);
        assert_eq!(
            decoded.request.version_id_marker,
            versions.request.version_id_marker
        );
        assert_eq!(decoded.request.start_at, versions.request.start_at);
        assert_eq!(decoded.request.max_keys, versions.request.max_keys);

        let response = StorageRpcListObjectVersionsResponse {
            response: ListObjectVersionsResp {
                versions: Vec::new(),
                is_truncated: true,
                next_key_marker: Some(key_marker.clone()),
                next_version_id_marker: Some(VersionId::from_u64(43)),
            },
        };
        let bytes = encode_list_object_versions_response(&response).unwrap();
        let decoded = decode_list_object_versions_response(&bytes).unwrap();
        assert!(decoded.response.versions.is_empty());
        assert!(decoded.response.is_truncated);
        assert_eq!(decoded.response.next_key_marker, Some(key_marker.clone()));
        assert_eq!(
            decoded.response.next_version_id_marker,
            Some(VersionId::from_u64(43))
        );

        let uploads = StorageRpcListMultipartUploadsRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::INITIAL,
            pg_id: PgId::new(3),
            request: ListMultipartUploadsReq {
                bucket,
                prefix: Some(prefix),
                page_start: Some(ListMultipartUploadsPageStart::After {
                    key_marker: key_marker.clone(),
                    upload_id_marker: Some(upload_id.clone()),
                }),
                max_uploads: 11,
            },
        };
        let bytes = encode_list_multipart_uploads_request(&uploads).unwrap();
        let decoded = decode_list_multipart_uploads_request(&bytes).unwrap();
        assert_eq!(decoded.node_id, uploads.node_id);
        assert_eq!(decoded.cluster_epoch, uploads.cluster_epoch);
        assert_eq!(decoded.pg_id, uploads.pg_id);
        assert_eq!(decoded.request.bucket, uploads.request.bucket);
        assert_eq!(decoded.request.prefix, uploads.request.prefix);
        assert_eq!(decoded.request.page_start, uploads.request.page_start);
        assert_eq!(decoded.request.max_uploads, uploads.request.max_uploads);

        let at_uploads = StorageRpcListMultipartUploadsRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::INITIAL,
            pg_id: PgId::new(3),
            request: ListMultipartUploadsReq {
                bucket: BucketName::try_from("bucket").unwrap(),
                prefix: None,
                page_start: Some(ListMultipartUploadsPageStart::At(
                    ObjectKey::try_from("prefix/next").unwrap(),
                )),
                max_uploads: 11,
            },
        };
        let bytes = encode_list_multipart_uploads_request(&at_uploads).unwrap();
        let decoded = decode_list_multipart_uploads_request(&bytes).unwrap();
        assert_eq!(decoded.request.page_start, at_uploads.request.page_start);

        let response = StorageRpcListMultipartUploadsResponse {
            response: ListMultipartUploadsResp {
                uploads: Vec::new(),
                is_truncated: true,
                next_key_marker: Some(key_marker),
                next_upload_id_marker: Some(upload_id),
            },
        };
        let bytes = encode_list_multipart_uploads_response(&response).unwrap();
        let decoded = decode_list_multipart_uploads_response(&bytes).unwrap();
        assert!(decoded.response.uploads.is_empty());
        assert!(decoded.response.is_truncated);
        assert_eq!(
            decoded.response.next_key_marker,
            response.response.next_key_marker
        );
        assert_eq!(
            decoded.response.next_upload_id_marker,
            response.response.next_upload_id_marker
        );
    }

    #[test]
    fn object_listing_requests_reject_unbounded_page_limits() {
        let bucket = BucketName::try_from("bucket").unwrap();
        let objects = StorageRpcListObjectsRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::INITIAL,
            pg_id: PgId::new(3),
            request: ListObjectsReq {
                bucket: bucket.clone(),
                prefix: None,
                start_after: None,
                start_at: None,
                max_keys: STORAGE_RPC_MAX_LIST_PAGE_ITEMS + 1,
            },
        };
        assert_eq!(
            encode_list_objects_request(&objects),
            Err(StorageRpcPayloadError::PayloadTooLarge {
                len: (STORAGE_RPC_MAX_LIST_PAGE_ITEMS + 1) as usize,
                limit: STORAGE_RPC_MAX_LIST_PAGE_ITEMS as usize,
            })
        );

        let mut bytes = encode_bucket_pg_request(&StorageRpcBucketPgRequest {
            node_id: objects.node_id,
            cluster_epoch: objects.cluster_epoch,
            pg_id: objects.pg_id,
        })
        .unwrap();
        put_string(&mut bytes, bucket.as_str());
        put_optional_string(&mut bytes, None);
        put_optional_string(&mut bytes, None);
        put_optional_string(&mut bytes, None);
        put_u32(&mut bytes, STORAGE_RPC_MAX_LIST_PAGE_ITEMS + 1);
        assert!(matches!(
            decode_list_objects_request(&bytes),
            Err(StorageRpcPayloadError::PayloadTooLarge {
                len,
                limit,
            }) if len == (STORAGE_RPC_MAX_LIST_PAGE_ITEMS + 1) as usize
                && limit == STORAGE_RPC_MAX_LIST_PAGE_ITEMS as usize
        ));
    }

    #[test]
    fn object_listing_responses_reject_oversized_counts_before_allocating() {
        let too_many = STORAGE_RPC_MAX_LIST_PAGE_ITEMS + 1;

        let mut object_bytes = Vec::new();
        put_u32(&mut object_bytes, too_many);
        assert!(matches!(
            decode_list_objects_response(&object_bytes),
            Err(StorageRpcPayloadError::PayloadTooLarge { len, limit })
                if len == too_many as usize
                    && limit == STORAGE_RPC_MAX_LIST_PAGE_ITEMS as usize
        ));

        let mut version_bytes = Vec::new();
        put_u32(&mut version_bytes, too_many);
        assert!(matches!(
            decode_list_object_versions_response(&version_bytes),
            Err(StorageRpcPayloadError::PayloadTooLarge { len, limit })
                if len == too_many as usize
                    && limit == STORAGE_RPC_MAX_LIST_PAGE_ITEMS as usize
        ));

        let mut upload_bytes = Vec::new();
        put_u32(&mut upload_bytes, too_many);
        assert!(matches!(
            decode_list_multipart_uploads_response(&upload_bytes),
            Err(StorageRpcPayloadError::PayloadTooLarge { len, limit })
                if len == too_many as usize
                    && limit == STORAGE_RPC_MAX_LIST_PAGE_ITEMS as usize
        ));
    }

    #[test]
    fn bucket_metadata_read_requests_and_responses_round_trip() {
        let bucket = BucketName::try_from("bucket").unwrap();
        let list_request = StorageRpcBucketListRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::INITIAL,
            pg_id: PgId::new(3),
            owner_canonical_id: CanonicalUserId::from_principal("owner").to_string(),
        };
        let bytes = encode_bucket_list_request(&list_request).unwrap();
        let decoded = decode_bucket_list_request(&bytes).unwrap();
        assert_eq!(decoded, list_request);

        let info = test_bucket_info("bucket");
        let bytes = encode_bucket_list_response(&StorageRpcBucketListResponse {
            buckets: vec![info.clone()],
        })
        .unwrap();
        let decoded = decode_bucket_list_response(&bytes).unwrap();
        assert_eq!(decoded.buckets.len(), 1);
        assert_eq!(decoded.buckets[0].name, info.name);
        assert_eq!(
            decoded.buckets[0].owner_canonical_id,
            info.owner_canonical_id
        );
        assert_eq!(
            decoded.buckets[0].bucket_execution_generation,
            info.bucket_execution_generation
        );

        let batch_request = StorageRpcBucketBatchRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::INITIAL,
            pg_id: PgId::new(3),
            buckets: vec![bucket.clone()],
        };
        let bytes = encode_bucket_batch_request(&batch_request).unwrap();
        let decoded = decode_bucket_batch_request(&bytes).unwrap();
        assert_eq!(decoded, batch_request);

        let generations = StorageRpcBucketExecutionGenerationsResponse {
            generations: HashMap::from([(bucket.clone(), 11)]),
        };
        let bytes = encode_bucket_execution_generations_response(&generations).unwrap();
        let decoded = decode_bucket_execution_generations_response(&bytes).unwrap();
        assert_eq!(decoded, generations);

        let identities = StorageRpcBucketFastPathIdentitiesResponse {
            identities: HashMap::from([(
                bucket,
                BucketFastPathIdentity {
                    bucket_execution_generation: 11,
                    bucket_incarnation_generation: 17,
                },
            )]),
        };
        let bytes = encode_bucket_fast_path_identities_response(&identities).unwrap();
        let decoded = decode_bucket_fast_path_identities_response(&bytes).unwrap();
        assert_eq!(decoded, identities);
    }

    #[test]
    fn bucket_mark_deleting_command_build_request_and_response_round_trip() {
        let bucket_request = StorageRpcBucketRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::INITIAL,
            pg_id: PgId::new(3),
            bucket: BucketName::try_from("bucket").unwrap(),
        };
        let command_id = MetadataCommandId::new(
            bucket_request.cluster_epoch,
            bucket_request.pg_id,
            MetadataCommandLogIndex::new(9).unwrap(),
        );
        let request = StorageRpcBucketMarkDeletingCommandBuildRequest {
            bucket: bucket_request.clone(),
            command_id,
        };

        let bytes = encode_bucket_mark_deleting_command_build_request(&request).unwrap();
        let decoded = decode_bucket_mark_deleting_command_build_request(&bytes).unwrap();

        assert_eq!(decoded, request);

        let wrong_command_id = MetadataCommandId::new(
            bucket_request.cluster_epoch,
            PgId::new(4),
            MetadataCommandLogIndex::new(9).unwrap(),
        );
        assert_eq!(
            encode_bucket_mark_deleting_command_build_request(
                &StorageRpcBucketMarkDeletingCommandBuildRequest {
                    bucket: bucket_request.clone(),
                    command_id: wrong_command_id,
                }
            ),
            Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
                "command id route must match request route",
            ))
        );

        let mut deleting_info = test_bucket_info("bucket");
        deleting_info.state = BucketState::Deleting;
        let already_deleting = StorageRpcBucketMarkDeletingCommandBuildResponse {
            outcome: StorageRpcBucketMarkDeletingCommandBuildOutcome::AlreadyDeleting(
                deleting_info,
            ),
        };
        let bytes = encode_bucket_mark_deleting_command_build_response(&already_deleting);
        let decoded = decode_bucket_mark_deleting_command_build_response(
            &bytes,
            &metadata_command_decode_authority_for_test(),
        )
        .unwrap();
        match decoded.outcome {
            StorageRpcBucketMarkDeletingCommandBuildOutcome::AlreadyDeleting(info) => {
                assert_eq!(info.name.as_str(), "bucket");
                assert_eq!(info.state, BucketState::Deleting);
                assert_eq!(info.bucket_execution_generation, 17);
            }
            StorageRpcBucketMarkDeletingCommandBuildOutcome::Command(_) => {
                panic!("expected already-deleting response")
            }
        }

        let command = test_mark_bucket_deleting_command(command_id);
        let command_response = StorageRpcBucketMarkDeletingCommandBuildResponse {
            outcome: StorageRpcBucketMarkDeletingCommandBuildOutcome::Command(Box::new(command)),
        };
        let bytes = encode_bucket_mark_deleting_command_build_response(&command_response);
        let decoded = decode_bucket_mark_deleting_command_build_response(
            &bytes,
            &metadata_command_decode_authority_for_test(),
        )
        .unwrap();
        match decoded.outcome {
            StorageRpcBucketMarkDeletingCommandBuildOutcome::Command(command) => {
                assert_eq!(command.id(), command_id);
                assert!(matches!(
                    command.payload(),
                    MetadataCommandPayload::MarkBucketDeleting(mark)
                        if mark.bucket.name.as_str() == "bucket"
                            && mark.bucket.state == BucketState::Deleting
                ));
            }
            StorageRpcBucketMarkDeletingCommandBuildOutcome::AlreadyDeleting(_) => {
                panic!("expected command response")
            }
        }

        let pending_match = StorageRpcBucketMetadataControlPendingMatchRequest {
            bucket: bucket_request,
            command: test_mark_bucket_deleting_command(command_id),
            mutation: StorageRpcBucketMetadataControlMutation::MarkDeleting,
        };
        let bytes = encode_bucket_metadata_control_pending_match_request(&pending_match).unwrap();
        let decoded = decode_bucket_metadata_control_pending_match_request(
            &bytes,
            &metadata_command_decode_authority_for_test(),
        )
        .unwrap();
        assert_eq!(decoded, pending_match);
    }

    #[test]
    fn bucket_metadata_read_responses_reject_oversized_counts_before_allocating() {
        let too_many = STORAGE_RPC_MAX_BUCKET_BATCH_ITEMS + 1;

        let mut list_bytes = Vec::new();
        put_u32(&mut list_bytes, too_many);
        assert!(matches!(
            decode_bucket_list_response(&list_bytes),
            Err(StorageRpcPayloadError::PayloadTooLarge { len, limit })
                if len == too_many as usize
                    && limit == STORAGE_RPC_MAX_BUCKET_BATCH_ITEMS as usize
        ));

        let mut generation_bytes = Vec::new();
        put_u32(&mut generation_bytes, too_many);
        assert!(matches!(
            decode_bucket_execution_generations_response(&generation_bytes),
            Err(StorageRpcPayloadError::PayloadTooLarge { len, limit })
                if len == too_many as usize
                    && limit == STORAGE_RPC_MAX_BUCKET_BATCH_ITEMS as usize
        ));

        let mut identity_bytes = Vec::new();
        put_u32(&mut identity_bytes, too_many);
        assert!(matches!(
            decode_bucket_fast_path_identities_response(&identity_bytes),
            Err(StorageRpcPayloadError::PayloadTooLarge { len, limit })
                if len == too_many as usize
                    && limit == STORAGE_RPC_MAX_BUCKET_BATCH_ITEMS as usize
        ));
    }

    #[test]
    fn bucket_snapshot_request_and_response_round_trip() {
        let request = StorageRpcBucketSnapshotRequest {
            bucket: StorageRpcBucketRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::INITIAL,
                pg_id: PgId::new(3),
                bucket: BucketName::try_from("bucket").unwrap(),
            },
            request: BucketSnapshotRequest {
                policy: true,
                tags: BucketSnapshotTagsRequest::Always,
                lifecycle: true,
                cors: true,
            },
        };
        let bytes = encode_bucket_snapshot_request(&request);
        let decoded = decode_bucket_snapshot_request(&bytes).unwrap();
        assert_eq!(decoded, request);

        let snapshot = BucketSnapshot {
            bucket: test_bucket_info("bucket"),
            request: request.request,
            policy: LoadedBucketSubresource::Loaded("{\"Statement\":[]}".to_string()),
            tags: LoadedBucketSubresource::Missing,
            lifecycle: LoadedBucketSubresource::Loaded("<LifecycleConfiguration/>".to_string()),
            cors: LoadedBucketSubresource::NotRequested,
        };
        let response = StorageRpcBucketSnapshotResponse {
            outcome: StorageRpcBucketSnapshotOutcome::Loaded(Box::new(snapshot)),
        };
        let bytes = encode_bucket_snapshot_response(&response);
        let decoded = decode_bucket_snapshot_response(&bytes).unwrap();
        match decoded.outcome {
            StorageRpcBucketSnapshotOutcome::Loaded(snapshot) => {
                assert_eq!(snapshot.bucket.name.as_str(), "bucket");
                assert_eq!(snapshot.request, request.request);
                assert!(matches!(
                    snapshot.policy,
                    LoadedBucketSubresource::Loaded(ref body)
                        if body == "{\"Statement\":[]}"
                ));
                assert!(matches!(snapshot.tags, LoadedBucketSubresource::Missing));
                assert!(matches!(
                    snapshot.cors,
                    LoadedBucketSubresource::NotRequested
                ));
            }
            StorageRpcBucketSnapshotOutcome::BucketNotFound { .. } => {
                panic!("expected loaded bucket snapshot response")
            }
        }

        let response = StorageRpcBucketSnapshotResponse {
            outcome: StorageRpcBucketSnapshotOutcome::BucketNotFound {
                name: BucketName::try_from("missing-bucket").unwrap(),
            },
        };
        let bytes = encode_bucket_snapshot_response(&response);
        let decoded = decode_bucket_snapshot_response(&bytes).unwrap();
        match decoded.outcome {
            StorageRpcBucketSnapshotOutcome::BucketNotFound { name } => {
                assert_eq!(name.as_str(), "missing-bucket");
            }
            StorageRpcBucketSnapshotOutcome::Loaded(_) => {
                panic!("expected bucket-not-found snapshot response")
            }
        }
    }

    #[test]
    fn bucket_write_reservation_requests_round_trip() {
        let acquire = StorageRpcBucketWriteReservationAcquireRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::INITIAL,
            pg_id: PgId::new(3),
            bucket: BucketName::try_from("bucket").unwrap(),
            reservation_id: "reservation-1".to_string(),
            owner_token: "owner-token-1".to_string(),
            operation_kind: "put-object".to_string(),
            created_at: 10,
            lease_deadline: 20,
            target_context: Some("key=a".to_string()),
            effect_deadline: Some(StorageRpcAdmittedRouteEffectDeadline {
                authority_valid_until_ms: 5_000,
                portable_wall_valid_until_ms: 4_000,
            }),
        };
        let bytes = encode_bucket_write_reservation_acquire_request(&acquire).unwrap();
        let decoded = decode_bucket_write_reservation_acquire_request(&bytes).unwrap();
        assert_eq!(decoded, acquire);

        let record = BucketWriteReservationRecord {
            bucket: acquire.bucket.clone(),
            reservation_id: acquire.reservation_id.clone(),
            owner_token: acquire.owner_token.clone(),
            cluster_epoch: acquire.cluster_epoch,
            bucket_execution_generation: 2,
            bucket_incarnation_generation: 3,
            operation_kind: acquire.operation_kind.clone(),
            created_at: acquire.created_at,
            lease_deadline: acquire.lease_deadline,
            target_context: acquire.target_context.clone(),
        };
        let response = StorageRpcBucketWriteReservationRecordResponse {
            outcome: StorageRpcBucketWriteReservationAcquireOutcome::Acquired(record.clone()),
        };
        let bytes = encode_bucket_write_reservation_record_response(&response).unwrap();
        let decoded = decode_bucket_write_reservation_record_response(&bytes).unwrap();
        assert_eq!(decoded, response);

        let response = StorageRpcBucketWriteReservationRecordResponse {
            outcome: StorageRpcBucketWriteReservationAcquireOutcome::Draining,
        };
        let bytes = encode_bucket_write_reservation_record_response(&response).unwrap();
        let decoded = decode_bucket_write_reservation_record_response(&bytes).unwrap();
        assert_eq!(decoded, response);

        let response = StorageRpcBucketWriteReservationRecordResponse {
            outcome: StorageRpcBucketWriteReservationAcquireOutcome::BucketNotFound {
                name: acquire.bucket.clone(),
            },
        };
        let bytes = encode_bucket_write_reservation_record_response(&response).unwrap();
        let decoded = decode_bucket_write_reservation_record_response(&bytes).unwrap();
        assert_eq!(decoded, response);

        let release = StorageRpcBucketWriteReservationRecordRequest {
            node_id: NodeId::new(7),
            route_cluster_epoch: ClusterEpoch::new(2).unwrap(),
            pg_id: PgId::new(3),
            record: record.clone(),
        };
        let bytes = encode_bucket_write_reservation_record_request(&release).unwrap();
        let decoded = decode_bucket_write_reservation_record_request(&bytes).unwrap();
        assert_eq!(decoded, release);

        let proof = StorageRpcBucketWriteReservationProofRequest {
            node_id: NodeId::new(7),
            route_cluster_epoch: ClusterEpoch::new(2).unwrap(),
            pg_id: PgId::new(3),
            proof: BucketWriteReservationProof::from(&record),
        };
        let bytes = encode_bucket_write_reservation_proof_request(&proof).unwrap();
        let decoded = decode_bucket_write_reservation_proof_request(&bytes).unwrap();
        assert_eq!(decoded, proof);

        let heartbeat = StorageRpcBucketWriteReservationHeartbeatRequest {
            node_id: NodeId::new(7),
            route_cluster_epoch: ClusterEpoch::new(2).unwrap(),
            pg_id: PgId::new(3),
            proof: BucketWriteReservationProof::from(&record),
            lease_deadline: 30,
            effect_deadline: Some(StorageRpcAdmittedRouteEffectDeadline {
                authority_valid_until_ms: 5_000,
                portable_wall_valid_until_ms: 4_000,
            }),
        };
        let bytes = encode_bucket_write_reservation_heartbeat_request(&heartbeat).unwrap();
        assert!(bytes.len() <= STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_HEARTBEAT_PAYLOAD_LEN);
        let decoded = decode_bucket_write_reservation_heartbeat_request(&bytes).unwrap();
        assert_eq!(decoded, heartbeat);

        let update = StorageRpcStreamUploadBucketWriteReservationUpdateRequest {
            object: StorageRpcObjectRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::new(2).unwrap(),
                pg_id: PgId::new(4),
                bucket: record.bucket.clone(),
                key: ObjectKey::try_from("key").unwrap(),
            },
            session_id: SessionId::try_from("0123456789abcdef0123456789abcdef").unwrap(),
            current: BucketWriteReservationProof::from(&record),
            renewed: BucketWriteReservationProof {
                lease_deadline: 30,
                ..BucketWriteReservationProof::from(&record)
            },
            effect_deadline: heartbeat.effect_deadline,
        };
        let bytes = encode_stream_upload_bucket_write_reservation_update_request(&update);
        let decoded = decode_stream_upload_bucket_write_reservation_update_request(&bytes).unwrap();
        assert_eq!(decoded, update);
    }

    #[test]
    fn stream_reservation_heartbeat_and_update_requests_fit_exact_kind_caps() {
        let bucket = BucketName::try_from("b".repeat(STORAGE_RPC_MAX_BUCKET_NAME_LEN)).unwrap();
        let key = ObjectKey::try_from("k".repeat(STORAGE_RPC_MAX_OBJECT_KEY_LEN)).unwrap();
        let proof = BucketWriteReservationProof {
            bucket: bucket.clone(),
            reservation_id: "r".repeat(STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN),
            owner_token: "w".repeat(STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN),
            cluster_epoch: ClusterEpoch::INITIAL,
            bucket_execution_generation: u64::MAX,
            bucket_incarnation_generation: u64::MAX,
            operation_kind: "o".repeat(STORAGE_RPC_MAX_BUCKET_WRITE_OPERATION_KIND_LEN),
            created_at: u64::MAX,
            lease_deadline: u64::MAX,
            target_context: Some("k".repeat(STORAGE_RPC_MAX_BUCKET_WRITE_TARGET_CONTEXT_LEN)),
        };
        let effect_deadline = Some(StorageRpcAdmittedRouteEffectDeadline {
            authority_valid_until_ms: u64::MAX,
            portable_wall_valid_until_ms: u64::MAX
                - crate::control_plane_lease::CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
        });
        let heartbeat = StorageRpcBucketWriteReservationHeartbeatRequest {
            node_id: NodeId::new(u32::MAX),
            route_cluster_epoch: ClusterEpoch::new(u64::MAX).unwrap(),
            pg_id: PgId::new(u32::MAX),
            proof: proof.clone(),
            lease_deadline: u64::MAX,
            effect_deadline,
        };
        let heartbeat_payload =
            encode_bucket_write_reservation_heartbeat_request(&heartbeat).unwrap();
        assert_eq!(
            heartbeat_payload.len(),
            STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_HEARTBEAT_PAYLOAD_LEN
        );
        let heartbeat_frame = encode_storage_rpc_frame(
            18,
            StorageRpcMessageKind::BucketWriteReservationHeartbeat,
            &heartbeat_payload,
        )
        .unwrap();
        let decoded_heartbeat_frame =
            read_storage_rpc_request_frame_from(&mut Cursor::new(heartbeat_frame)).unwrap();
        assert_eq!(
            decode_bucket_write_reservation_heartbeat_request(&decoded_heartbeat_frame.payload)
                .unwrap(),
            heartbeat
        );

        let update = StorageRpcStreamUploadBucketWriteReservationUpdateRequest {
            object: StorageRpcObjectRequest {
                node_id: NodeId::new(u32::MAX),
                cluster_epoch: ClusterEpoch::INITIAL,
                pg_id: PgId::new(u32::MAX),
                bucket,
                key,
            },
            session_id: SessionId::try_from("a".repeat(SESSION_ID_LEN)).unwrap(),
            current: proof.clone(),
            renewed: proof,
            effect_deadline,
        };
        let update_payload = encode_stream_upload_bucket_write_reservation_update_request(&update);
        assert_eq!(
            update_payload.len(),
            STORAGE_RPC_MAX_STREAM_UPLOAD_BUCKET_WRITE_RESERVATION_UPDATE_PAYLOAD_LEN
        );
        let update_frame = encode_storage_rpc_frame(
            19,
            StorageRpcMessageKind::ObjectStreamUploadBucketWriteReservationUpdate,
            &update_payload,
        )
        .unwrap();
        let decoded_update_frame =
            read_storage_rpc_request_frame_from(&mut Cursor::new(update_frame)).unwrap();
        assert_eq!(
            decode_stream_upload_bucket_write_reservation_update_request(
                &decoded_update_frame.payload
            )
            .unwrap(),
            update
        );
    }

    #[test]
    fn cleanup_list_requests_reject_limit_over_protocol_max() {
        let bucket = crate::tests::bucket_name("cleanup-list-limit");
        let stream_request = StorageRpcStreamUploadsListRequest {
            bucket: StorageRpcBucketRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::INITIAL,
                pg_id: PgId::new(3),
                bucket: bucket.clone(),
            },
            session_id_marker: Some(
                SessionId::try_from("0123456789abcdef0123456789abcdef").unwrap(),
            ),
            limit: STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS + 1,
        };
        assert_eq!(
            encode_stream_uploads_list_request(&stream_request),
            Err(StorageRpcPayloadError::PayloadTooLarge {
                len: (STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS + 1) as usize,
                limit: STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS as usize,
            })
        );

        let stream_pg_request = StorageRpcStreamUploadsPgListRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::INITIAL,
            pg_id: PgId::new(3),
            session_id_marker: Some(
                SessionId::try_from("0123456789abcdef0123456789abcdef").unwrap(),
            ),
            limit: STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS + 1,
        };
        assert_eq!(
            encode_stream_uploads_pg_list_request(&stream_pg_request),
            Err(StorageRpcPayloadError::PayloadTooLarge {
                len: (STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS + 1) as usize,
                limit: STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS as usize,
            })
        );
    }

    #[test]
    fn cleanup_list_responses_reject_count_over_protocol_max_before_items() {
        let mut bytes = Vec::new();
        put_u32(&mut bytes, STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS + 1);
        assert!(matches!(
            decode_stream_uploads_list_response(&bytes),
            Err(StorageRpcPayloadError::PayloadTooLarge {
                len,
                limit,
            }) if len == (STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS + 1) as usize
                && limit == STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS as usize
        ));
    }

    #[test]
    fn lifecycle_sweep_roots_request_rejects_limit_over_protocol_max() {
        let request = StorageRpcLifecycleSweepRootsRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::INITIAL,
            pg_id: PgId::new(3),
            now: 100,
            limit: STORAGE_RPC_MAX_LIFECYCLE_SWEEP_ROOTS + 1,
        };
        assert_eq!(
            encode_lifecycle_sweep_roots_request(&request),
            Err(StorageRpcPayloadError::PayloadTooLarge {
                len: STORAGE_RPC_MAX_LIFECYCLE_SWEEP_ROOTS + 1,
                limit: STORAGE_RPC_MAX_LIFECYCLE_SWEEP_ROOTS,
            })
        );

        let mut bytes = encode_bucket_pg_request(&StorageRpcBucketPgRequest {
            node_id: request.node_id,
            cluster_epoch: request.cluster_epoch,
            pg_id: request.pg_id,
        })
        .unwrap();
        put_u64(&mut bytes, request.now);
        put_u64(
            &mut bytes,
            u64::try_from(STORAGE_RPC_MAX_LIFECYCLE_SWEEP_ROOTS + 1).unwrap(),
        );
        assert_eq!(
            decode_lifecycle_sweep_roots_request(&bytes),
            Err(StorageRpcPayloadError::PayloadTooLarge {
                len: STORAGE_RPC_MAX_LIFECYCLE_SWEEP_ROOTS + 1,
                limit: STORAGE_RPC_MAX_LIFECYCLE_SWEEP_ROOTS,
            })
        );
    }

    #[test]
    fn stream_uploads_list_response_rejects_unbounded_item_count() {
        let mut bytes = Vec::new();
        put_u32(&mut bytes, STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS + 1);

        assert_eq!(
            decode_stream_uploads_list_response(&bytes),
            Err(StorageRpcPayloadError::PayloadTooLarge {
                len: (STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS + 1) as usize,
                limit: STORAGE_RPC_MAX_CLEANUP_LIST_PAGE_ITEMS as usize,
            })
        );
    }

    #[test]
    fn object_generation_reservation_request_and_response_round_trip() {
        let request = StorageRpcObjectGenerationReservationRequest {
            object: StorageRpcObjectRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::INITIAL,
                pg_id: PgId::new(3),
                bucket: BucketName::try_from("bucket").unwrap(),
                key: ObjectKey::try_from("key").unwrap(),
            },
            reservation_id: SessionId::try_from("0123456789abcdef0123456789abcdef").unwrap(),
        };

        let bytes = encode_object_generation_reservation_request(&request);
        let decoded = decode_object_generation_reservation_request(&bytes).unwrap();
        assert_eq!(decoded, request);

        let response = StorageRpcObjectGenerationReservationResponse {
            outcome: StorageRpcObjectGenerationReservationOutcome::Found(
                GenerationId::new(42).unwrap(),
            ),
        };
        let bytes = encode_object_generation_reservation_response(&response);
        let decoded = decode_object_generation_reservation_response(&bytes).unwrap();
        assert_eq!(decoded, response);

        let response = StorageRpcObjectGenerationReservationResponse {
            outcome: StorageRpcObjectGenerationReservationOutcome::NotFound {
                reservation_id: request.reservation_id,
            },
        };
        let bytes = encode_object_generation_reservation_response(&response);
        let decoded = decode_object_generation_reservation_response(&bytes).unwrap();
        assert_eq!(decoded, response);
    }

    #[test]
    fn multipart_upload_match_request_round_trips_ordered_command_for_provisional_id() {
        let bucket = BucketName::try_from("ordered-multipart-match-bucket").unwrap();
        let key = ObjectKey::try_from("ordered-multipart-match-key").unwrap();
        let upload_id_key = MultipartUploadIdKey::from_bytes([0x5a; 32]);
        let provisional_upload_id = upload_id_key.issue(&bucket, &key, "owner").unwrap();
        let ordered_upload_id = upload_id_key.with_listing_position(
            &bucket,
            &key,
            &provisional_upload_id,
            ClusterEpoch::INITIAL.get(),
            17,
        );
        let request = CreateMultipartUploadReq {
            upload_id: provisional_upload_id,
            bucket: bucket.clone(),
            key: key.clone(),
            tags: None,
            metadata_blob: SerializedMetadataBlob::default(),
            system_metadata_blob: SerializedSystemMetadataBlob::default(),
            initiator: OwnerIdentity::from_principal("owner"),
            owner: OwnerIdentity::from_principal("owner"),
            acl_grants: AclGrants::default(),
            public_read: false,
            object_lock: ObjectLockState::default(),
            checksum: None,
            encryption: ObjectEncryption::None,
        };
        let mut ordered_request = request.clone();
        ordered_request.upload_id = ordered_upload_id;
        let expected_command =
            CreateMultipartUploadCommand::from_request_with_bucket_write_reservation(
                ordered_request,
                GenerationId::new(9).unwrap(),
                None,
                123,
                BucketWriteReservationProof {
                    bucket: bucket.clone(),
                    reservation_id: "reservation-1".to_string(),
                    owner_token: "owner-token".to_string(),
                    cluster_epoch: ClusterEpoch::INITIAL,
                    bucket_execution_generation: 1,
                    bucket_incarnation_generation: 1,
                    operation_kind:
                        crate::metadata_command::CREATE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND
                            .to_string(),
                    created_at: 100,
                    lease_deadline: 200,
                    target_context: Some(key.as_str().to_string()),
                },
            );
        assert!(expected_command.matches_request(&request));
        let rpc_request = StorageRpcMultipartUploadMatchRequest {
            object: StorageRpcObjectRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::INITIAL,
                pg_id: PgId::new(3),
                bucket,
                key,
            },
            request,
            expected_command: Some(expected_command),
        };

        let bytes = encode_multipart_upload_match_request(&rpc_request).unwrap();
        assert_eq!(
            decode_multipart_upload_match_request(&bytes).unwrap(),
            rpc_request
        );
    }

    #[test]
    fn stream_segment_append_prepare_request_round_trips_effect_deadline_at_cap() {
        let request = StorageRpcStreamSegmentAppendPrepareRequest {
            object: StorageRpcObjectRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::INITIAL,
                pg_id: PgId::new(3),
                bucket: BucketName::try_from("b".repeat(STORAGE_RPC_MAX_BUCKET_NAME_LEN)).unwrap(),
                key: ObjectKey::try_from("k".repeat(STORAGE_RPC_MAX_OBJECT_KEY_LEN)).unwrap(),
            },
            request: PrepareStreamUploadSegmentAppendReq {
                session_id: SessionId::try_from("a".repeat(SESSION_ID_LEN)).unwrap(),
                segment_index: u32::MAX,
                size: u64::MAX,
                segment_crc64: u64::MAX,
                payload_crc64: u64::MAX,
                segment_okh: [u8::MAX; 16],
            },
            effect_deadline: Some(StorageRpcAdmittedRouteEffectDeadline {
                authority_valid_until_ms: u64::MAX,
                portable_wall_valid_until_ms: u64::MAX
                    - crate::control_plane_lease::CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
            }),
        };

        let payload = encode_stream_segment_append_prepare_request(&request);
        assert_eq!(
            payload.len(),
            STORAGE_RPC_MAX_STREAM_SEGMENT_APPEND_PREPARE_REQUEST_PAYLOAD_LEN
        );
        let frame = encode_storage_rpc_frame(
            17,
            StorageRpcMessageKind::ObjectStreamSegmentAppendPrepare,
            &payload,
        )
        .unwrap();
        let decoded_frame = read_storage_rpc_request_frame_from(&mut Cursor::new(frame)).unwrap();
        assert_eq!(
            decode_stream_segment_append_prepare_request(&decoded_frame.payload).unwrap(),
            request
        );
    }

    #[test]
    fn object_read_auth_subject_and_snapshot_round_trip() {
        let bucket = BucketName::try_from("bucket").unwrap();
        let key = ObjectKey::try_from("key").unwrap();
        let owner = OwnerIdentity {
            principal: "owner".to_string(),
            canonical_id: CanonicalUserId::from_principal("owner"),
        };
        let stored = StoredObject::Live(LiveObjectRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: VersionId::from_u64(7),
            owner: owner.clone(),
            acl_grants: AclGrants::default(),
            public_read: false,
            generation_id: GenerationId::new(9).unwrap(),
            size: 12,
            etag: ObjectEtag::single_part(55),
            last_modified: 10,
            became_noncurrent_at: None,
            storage_class: StorageClass::Standard,
            ec: EcShape { k: 4, m: 2 },
            layout: ObjectLayout::MultipartManifest {
                parts_count: NonZeroU32::new(1).unwrap(),
            },
            tags: None,
            metadata_blob: Some(SerializedMetadataBlob::new(vec![1, 2, 3])),
            system_metadata_blob: Some(SerializedSystemMetadataBlob::new(vec![4, 5, 6])),
            object_lock: ObjectLockState::default(),
            encryption: ObjectEncryption::None,
        });
        let request = StorageRpcObjectReadAuthSubjectRequest {
            object: StorageRpcObjectRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::INITIAL,
                pg_id: PgId::new(3),
                bucket: bucket.clone(),
                key: key.clone(),
            },
            version_id: Some(VersionId::from_u64(7)),
        };

        let bytes = encode_object_read_auth_subject_request(&request);
        let decoded = decode_object_read_auth_subject_request(&bytes).unwrap();
        assert_eq!(decoded, request);

        let subject = ObjectReadAuthSubject {
            identity: ObjectReadAuthSubjectIdentity::for_stored(&stored),
            stored: stored.clone(),
        };
        let response = StorageRpcObjectReadAuthSubjectResponse {
            outcome: StorageRpcObjectReadAuthSubjectOutcome::Loaded(Box::new(subject.clone())),
        };
        let bytes = encode_object_read_auth_subject_response(&response);
        let decoded = decode_object_read_auth_subject_response(&bytes).unwrap();
        assert_eq!(decoded, response);

        let response = StorageRpcObjectReadAuthSubjectResponse {
            outcome: StorageRpcObjectReadAuthSubjectOutcome::ObjectNotFound,
        };
        let bytes = encode_object_read_auth_subject_response(&response);
        let decoded = decode_object_read_auth_subject_response(&bytes).unwrap();
        assert_eq!(decoded, response);

        let snapshot_request = StorageRpcObjectReadSnapshotRequest {
            object: request.object,
            version_id: request.version_id,
            expected_identity: subject.identity,
            snapshot_mode: ObjectReadSnapshotMode::FullPayloadLayout,
        };
        let bytes = encode_object_read_snapshot_request(&snapshot_request);
        let decoded = decode_object_read_snapshot_request(&bytes).unwrap();
        assert_eq!(decoded, snapshot_request);

        let checksum = ChecksumBytes::new([1, 2, 3, 4]).unwrap();
        let object_segment = ObjectSegmentRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: VersionId::from_u64(7),
            segment_index: 0,
            size: 12,
            segment_crc64: 98,
            segment_okh: [2; 16],
            segment_vid: GenerationId::new(10).unwrap(),
            data_pg_id: 5,
            placement_cluster_epoch: ClusterEpoch::new(9).unwrap(),
            ec_k: 4,
            ec_m: 2,
        };
        let part = ObjectPartRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: VersionId::from_u64(7),
            part_number: 1,
            size: 12,
            payload_crc64: 99,
            etag: vec![8; 16],
            etag_kind: EtagKind::MultipartComposite,
            part_vid: GenerationId::new(11).unwrap(),
            placement_cluster_epoch: ClusterEpoch::new(10).unwrap(),
            ec_k: 4,
            ec_m: 2,
            data_pg_id: 5,
            checksum: Some(checksum),
        };
        let expected_part = part.clone();
        let segment = MultipartPartSegmentRecord {
            bucket,
            key,
            upload_id: UploadId::try_from("u".repeat(128)).unwrap(),
            version_id: 7,
            part_number: 1,
            segment_index: 0,
            size: 12,
            segment_crc64: 99,
            segment_okh: [4; 16],
            segment_vid: GenerationId::new(12).unwrap(),
            data_pg_id: 5,
            placement_cluster_epoch: ClusterEpoch::new(11).unwrap(),
            ec_k: 4,
            ec_m: 2,
        };
        let response = StorageRpcObjectReadSnapshotResponse {
            outcome: StorageRpcObjectReadSnapshotOutcome::Loaded(Box::new(
                ObjectReadSnapshot::from_records(
                    stored,
                    vec![object_segment],
                    vec![part],
                    vec![segment],
                )
                .unwrap(),
            )),
        };
        let bytes = encode_object_read_snapshot_response(&response);
        let decoded = decode_object_read_snapshot_response(&bytes).unwrap();
        assert_eq!(decoded, response);
        let StorageRpcObjectReadSnapshotOutcome::Loaded(decoded_snapshot) = &decoded.outcome else {
            panic!("object-read snapshot response must remain loaded after round-trip");
        };
        assert_eq!(decoded_snapshot.multipart_parts.len(), 1);
        assert_eq!(
            decoded_snapshot.multipart_parts[0].record(),
            &expected_part,
            "storage-owned RPC coverage must compare every hidden durable part field",
        );

        let response = StorageRpcObjectReadSnapshotResponse {
            outcome: StorageRpcObjectReadSnapshotOutcome::StaleSubject,
        };
        let bytes = encode_object_read_snapshot_response(&response);
        let decoded = decode_object_read_snapshot_response(&bytes).unwrap();
        assert_eq!(decoded, response);
    }

    #[test]
    fn direct_put_commit_snapshot_request_and_response_round_trip() {
        let bucket = BucketName::try_from("bucket").unwrap();
        let key = ObjectKey::try_from("key").unwrap();
        let request = StorageRpcDirectPutCommitSnapshotRequest {
            object: StorageRpcObjectRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::INITIAL,
                pg_id: PgId::new(3),
                bucket: bucket.clone(),
                key: key.clone(),
            },
            reservation_id: SessionId::try_from("0123456789abcdef0123456789abcdef").unwrap(),
            generation_id: GenerationId::new(9).unwrap(),
        };

        let bytes = encode_direct_put_commit_snapshot_request(&request);
        let decoded = decode_direct_put_commit_snapshot_request(&bytes).unwrap();
        assert_eq!(decoded, request);

        let response = StorageRpcDirectPutCommitSnapshotResponse {
            snapshot: DirectPutCommitStorageSnapshot {
                auth_snapshot: crate::DirectPutCommitSnapshot {
                    existing_etag: Some("\"0123456789abcdef\"".to_string()),
                },
                current: None,
                committed_segments: None,
                committed_stale_generation_id: None,
                stale_payload_source: None,
                stale_payload: None,
            },
        };
        let bytes = encode_direct_put_commit_snapshot_response(&response);
        let decoded = decode_direct_put_commit_snapshot_response(&bytes).unwrap();
        assert_eq!(decoded, response);

        let owner = OwnerIdentity {
            principal: "owner".to_string(),
            canonical_id: CanonicalUserId::from_principal("owner"),
        };
        let current = StoredObject::Live(LiveObjectRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: VersionId::Null,
            owner,
            acl_grants: AclGrants::default(),
            public_read: false,
            generation_id: GenerationId::new(10).unwrap(),
            size: 12,
            etag: ObjectEtag::single_part(55),
            last_modified: 99,
            became_noncurrent_at: None,
            storage_class: StorageClass::Standard,
            ec: EcShape { k: 4, m: 2 },
            layout: ObjectLayout::Standard,
            tags: None,
            metadata_blob: Some(SerializedMetadataBlob::new(vec![1, 2, 3])),
            system_metadata_blob: Some(SerializedSystemMetadataBlob::new(vec![4, 5, 6])),
            object_lock: ObjectLockState::default(),
            encryption: ObjectEncryption::None,
        });
        let response = StorageRpcDirectPutCommitSnapshotResponse {
            snapshot: DirectPutCommitStorageSnapshot {
                auth_snapshot: crate::DirectPutCommitSnapshot {
                    existing_etag: Some("\"0123456789abcdef\"".to_string()),
                },
                current: Some(current),
                committed_segments: Some(vec![ObjectSegmentRecord {
                    bucket,
                    key,
                    version_id: VersionId::Null,
                    segment_index: 0,
                    size: 12,
                    segment_crc64: 55,
                    segment_okh: [8; 16],
                    segment_vid: GenerationId::new(10).unwrap(),
                    data_pg_id: 3,
                    placement_cluster_epoch: ClusterEpoch::INITIAL,
                    ec_k: 4,
                    ec_m: 2,
                }]),
                committed_stale_generation_id: Some(GenerationId::new(9).unwrap()),
                stale_payload_source: None,
                stale_payload: None,
            },
        };
        let bytes = encode_direct_put_commit_snapshot_response(&response);
        let decoded = decode_direct_put_commit_snapshot_response(&bytes).unwrap();
        assert_eq!(decoded, response);
    }

    #[test]
    fn direct_put_command_build_request_and_stale_response_round_trip() {
        let bucket = BucketName::try_from("bucket").unwrap();
        let key = ObjectKey::try_from("key").unwrap();
        let reservation_id = SessionId::try_from("0123456789abcdef0123456789abcdef").unwrap();
        let owner = OwnerIdentity {
            principal: "owner".to_string(),
            canonical_id: CanonicalUserId::from_principal("owner"),
        };
        let proof = BucketWriteReservationProof {
            bucket: bucket.clone(),
            reservation_id: "reservation-1".to_string(),
            owner_token: "owner-token".to_string(),
            cluster_epoch: ClusterEpoch::INITIAL,
            bucket_execution_generation: 1,
            bucket_incarnation_generation: 1,
            operation_kind:
                crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND
                    .to_string(),
            created_at: 123,
            lease_deadline: 130,
            target_context: Some("key".to_string()),
        };
        let commit = CommitDirectPutObjectReq {
            bucket: bucket.clone(),
            key: key.clone(),
            generation_reservation_id: reservation_id,
            versioning: BucketVersioningState::Suspended,
            owner,
            acl_grants: AclGrants::default(),
            public_read: false,
            generation_id: GenerationId::new(9).unwrap(),
            size: 12,
            etag_crc64: 99,
            ec: EcShape { k: 4, m: 2 },
            tags: Some(SerializedTagSet::default()),
            metadata_blob: SerializedMetadataBlob::new(vec![1, 2, 3]),
            system_metadata_blob: SerializedSystemMetadataBlob::new(vec![4, 5, 6]),
            object_lock: ObjectLockState::default(),
            encryption: ObjectEncryption::None,
            segment_index: 0,
            segment_crc64: 99,
            segment_okh: [7; 16],
            segment_vid: GenerationId::new(10).unwrap(),
            data_pg_id: 3,
            bucket_write_reservation: proof.clone(),
        };
        let request = StorageRpcDirectPutCommandBuildRequest {
            object: StorageRpcObjectRequest {
                node_id: NodeId::new(7),
                cluster_epoch: ClusterEpoch::INITIAL,
                pg_id: PgId::new(3),
                bucket: bucket.clone(),
                key: key.clone(),
            },
            request: commit,
            version_id: VersionId::Null,
            expected_snapshot: DirectPutCommitStorageSnapshot {
                auth_snapshot: crate::DirectPutCommitSnapshot {
                    existing_etag: None,
                },
                current: None,
                committed_segments: None,
                committed_stale_generation_id: None,
                stale_payload_source: None,
                stale_payload: None,
            },
            bucket_write_reservation: proof,
        };

        let bytes = encode_direct_put_command_build_request(&request).unwrap();
        let decoded = decode_direct_put_command_build_request(&bytes).unwrap();
        assert_eq!(decoded.object, request.object);
        assert_eq!(decoded.request.bucket, bucket);
        assert_eq!(decoded.request.key, key);
        assert_eq!(decoded.request.generation_id, request.request.generation_id);
        assert_eq!(decoded.request.segment_okh, request.request.segment_okh);
        assert_eq!(
            decoded.bucket_write_reservation,
            request.bucket_write_reservation
        );

        let mut wrong_proof_request = request.clone();
        wrong_proof_request.bucket_write_reservation.bucket =
            BucketName::try_from("other-bucket").unwrap();
        assert_eq!(
            encode_direct_put_command_build_request(&wrong_proof_request),
            Err(StorageRpcPayloadError::InvalidObjectMetadataRequest(
                "bucket write reservation proof must match direct PUT bucket"
            ))
        );

        let response = StorageRpcDirectPutCommandBuildResponse {
            outcome: StorageRpcDirectPutCommandBuildOutcome::StaleSnapshot,
        };
        let bytes = encode_direct_put_command_build_response(&response);
        let decoded = decode_direct_put_command_build_response(
            &bytes,
            &metadata_command_decode_authority_for_test(),
        )
        .unwrap();
        assert_eq!(decoded, response);

        let response = StorageRpcDirectPutCommandBuildResponse {
            outcome: StorageRpcDirectPutCommandBuildOutcome::LogConflict {
                node_id: 7,
                pg_id: 3,
                cluster_epoch: ClusterEpoch::INITIAL,
                log_index: 9,
            },
        };
        let bytes = encode_direct_put_command_build_response(&response);
        let decoded = decode_direct_put_command_build_response(
            &bytes,
            &metadata_command_decode_authority_for_test(),
        )
        .unwrap();
        assert_eq!(decoded, response);
    }

    #[test]
    fn object_metadata_command_build_response_round_trips_log_conflict() {
        let response = StorageRpcObjectMetadataCommandBuildResponse {
            outcome: StorageRpcObjectMetadataCommandBuildOutcome::LogConflict {
                node_id: 7,
                pg_id: 3,
                cluster_epoch: ClusterEpoch::INITIAL,
                log_index: 9,
            },
        };

        let bytes = encode_object_metadata_command_build_response(&response);
        let decoded = decode_object_metadata_command_build_response(
            &bytes,
            &metadata_command_decode_authority_for_test(),
        )
        .unwrap();

        assert_eq!(decoded, response);
    }

    #[test]
    fn multipart_completion_barrier_command_build_request_and_response_round_trip() {
        let bucket = BucketName::try_from("completed-order-bucket").unwrap();
        let command_id = MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(3),
            MetadataCommandLogIndex::new(9).unwrap(),
        );
        let bucket_write_reservation = BucketWriteReservationProof {
            bucket: bucket.clone(),
            cluster_epoch: ClusterEpoch::INITIAL,
            operation_kind: COMPLETE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND.to_string(),
            target_context: Some("object-key".to_string()),
            ..test_bucket_write_reservation_proof()
        };
        let request = StorageRpcMultipartCompletionBarrierCommandBuildRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::INITIAL,
            pg_id: PgId::new(3),
            bucket: bucket.clone(),
            command_id,
            completion_target_context: "object-key".to_string(),
            bucket_write_reservation,
        };

        let bytes = encode_multipart_completion_barrier_command_build_request(&request).unwrap();
        let decoded = decode_multipart_completion_barrier_command_build_request(&bytes).unwrap();
        assert_eq!(decoded, request);

        let wrong_route = StorageRpcMultipartCompletionBarrierCommandBuildRequest {
            pg_id: PgId::new(4),
            ..request.clone()
        };
        assert_eq!(
            encode_multipart_completion_barrier_command_build_request(&wrong_route),
            Err(StorageRpcPayloadError::InvalidBucketMetadataRequest(
                "command id route must match request route"
            ))
        );

        let wrong_proof = StorageRpcMultipartCompletionBarrierCommandBuildRequest {
            bucket_write_reservation: test_bucket_write_reservation_proof(),
            ..request.clone()
        };
        assert_eq!(
            encode_multipart_completion_barrier_command_build_request(&wrong_proof),
            Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                "bucket write reservation proof must match multipart completion barrier request"
            ))
        );

        let wrong_target = StorageRpcMultipartCompletionBarrierCommandBuildRequest {
            completion_target_context: "other-key".to_string(),
            ..request.clone()
        };
        assert_eq!(
            encode_multipart_completion_barrier_command_build_request(&wrong_target),
            Err(StorageRpcPayloadError::InvalidBucketWriteReservationProof(
                "bucket write reservation proof must match multipart completion barrier request"
            ))
        );

        let command = MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::AdvanceMultipartCompletionBarrier(
                AdvanceMultipartCompletionBarrierCommand {
                    bucket: bucket.clone(),
                    barrier_sequence: 11,
                },
            ),
        );
        let response = StorageRpcMultipartCompletionBarrierCommandBuildResponse {
            barrier_sequence: 11,
            command,
        };
        let bytes = encode_multipart_completion_barrier_command_build_response(&response);
        let decoded = decode_multipart_completion_barrier_command_build_response(
            &bytes,
            &metadata_command_decode_authority_for_test(),
        )
        .unwrap();
        assert_eq!(decoded.barrier_sequence, 11);
        assert_eq!(decoded.command, response.command);
    }

    #[test]
    fn object_version_response_round_trip_rejects_null_version() {
        let response = StorageRpcObjectVersionResponse {
            version_id: VersionId::from_u64(42),
        };
        let bytes = encode_object_version_response(&response);
        let decoded = decode_object_version_response(&bytes).unwrap();
        assert_eq!(decoded, response);

        assert_eq!(
            decode_object_version_response(&0_u64.to_be_bytes()),
            Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "object version response must not contain null version"
            ))
        );
    }

    #[test]
    fn user_checksum_metadata_survives_rpc_payload_round_trip() {
        let checksum = ChecksumBytes::new([1u8, 2, 3, 4, 5, 6, 7, 8]).unwrap();
        let payload = encode_optional_checksum_metadata(Some(&checksum));
        let frame =
            encode_storage_rpc_frame(3, StorageRpcMessageKind::MetadataCommand, &payload).unwrap();
        let decoded_frame = decode_storage_rpc_frame(&frame).unwrap();
        let decoded_checksum = decode_optional_checksum_metadata(&decoded_frame.payload).unwrap();

        assert_eq!(decoded_checksum, Some(checksum));
    }

    fn test_metadata_command() -> MetadataCommandEnvelope {
        let owner = CanonicalUserId::from_principal("owner");
        let acl_grants = AclGrants::default();
        let command = CreateBucketCommand::from_config_for_test(
            &CreateBucketConfig {
                name: "bucket",
                owner_principal: "owner",
                owner_canonical_id: &owner,
                acl_grants: &acl_grants,
                public_read: false,
                public_write: false,
                versioning: BucketVersioningState::Enabled,
                object_lock: BucketObjectLockConfig::default(),
                ownership_controls: crate::BucketOwnershipControls {
                    object_ownership: crate::BucketObjectOwnership::ObjectWriter,
                },
            },
            123,
            7,
        )
        .unwrap();
        let id = MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(3),
            MetadataCommandLogIndex::new(9).unwrap(),
        );
        MetadataCommandEnvelope::new(id, MetadataCommandPayload::CreateBucket(command))
    }

    fn test_mark_bucket_deleting_command(command_id: MetadataCommandId) -> MetadataCommandEnvelope {
        let owner = CanonicalUserId::from_principal("owner");
        let acl_grants = AclGrants::default();
        let bucket = crate::metadata_command::BucketRecord::from_create_config(
            &CreateBucketConfig {
                name: "bucket",
                owner_principal: "owner",
                owner_canonical_id: &owner,
                acl_grants: &acl_grants,
                public_read: false,
                public_write: false,
                versioning: BucketVersioningState::Enabled,
                object_lock: BucketObjectLockConfig::default(),
                ownership_controls: crate::BucketOwnershipControls {
                    object_ownership: crate::BucketObjectOwnership::ObjectWriter,
                },
            },
            123,
            7,
        )
        .unwrap();
        MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::MarkBucketDeleting(MarkBucketDeletingCommand::from_bucket(
                bucket,
            )),
        )
    }

    fn test_bucket_info(name: &str) -> BucketInfo {
        BucketInfo {
            name: BucketName::try_from(name).unwrap(),
            owner_principal: "owner".to_string(),
            owner_canonical_id: CanonicalUserId::from_principal("owner"),
            created_at: 123,
            region: 7,
            state: BucketState::Active,
            versioning: BucketVersioningState::Enabled,
            object_lock: BucketObjectLockConfig::default(),
            acl_grants: AclGrants::default(),
            public_read: false,
            public_write: false,
            public_access_block: None,
            ownership_controls: None,
            bucket_policy_present: true,
            bucket_policy_public: false,
            bucket_policy_generation: 11,
            bucket_lifecycle_present: true,
            bucket_lifecycle_generation: 13,
            bucket_execution_generation: 17,
            bucket_incarnation_generation: 19,
            multipart_upload_id_key: MultipartUploadIdKey::from_bytes([1; 32]),
            bucket_abac_enabled: true,
            encryption: EffectiveBucketEncryptionConfig::default(),
        }
    }

    fn test_shard_location(shard_index: u8) -> StorageRpcShardLocation {
        StorageRpcShardLocation {
            cluster_epoch: ClusterEpoch::INITIAL,
            pg_id: PgId::new(11),
            shard_index: ShardIndex::new(shard_index),
            node_id: NodeId::new(u32::from(shard_index) + 100),
        }
    }

    fn test_shard_location_for_data_pg(data_pg_id: usize) -> StorageRpcShardLocation {
        StorageRpcShardLocation {
            cluster_epoch: ClusterEpoch::INITIAL,
            pg_id: PgId::new(u32::try_from(data_pg_id).expect("test PG id fits in u32")),
            shard_index: ShardIndex::new(0),
            node_id: NodeId::new(100),
        }
    }

    fn test_shard_key(shard_index: u8) -> ShardKey {
        ShardKey::new(&[0x42; 16], 77, shard_index)
    }

    fn test_claim_token() -> StorageRpcDurableClaimToken {
        StorageRpcDurableClaimToken::LifecycleSweep(StorageRpcBucketClaimToken {
            bucket: BucketName::try_from("bucket-claim").unwrap(),
            bucket_incarnation_generation: 17,
            claim_id: "claim-id".to_string(),
            owner_token: "owner-token".to_string(),
            cluster_epoch: ClusterEpoch::INITIAL,
            pg_id: 23,
        })
    }

    fn test_object_reclaim_claim_token() -> StorageRpcDurableClaimToken {
        StorageRpcDurableClaimToken::ObjectPayloadReclaim(
            StorageRpcObjectPayloadReclaimClaimToken {
                bucket: BucketName::try_from("bucket-reclaim").unwrap(),
                bucket_incarnation_generation: 17,
                key: ObjectKey::try_from("key").unwrap(),
                generation_id: GenerationId::new(19).unwrap(),
                reclaim_kind: ObjectPayloadReclaimKind::Multipart,
                claim_id: "claim-id".to_string(),
                owner_token: "owner-token".to_string(),
                cluster_epoch: ClusterEpoch::INITIAL,
                pg_id: 23,
            },
        )
    }

    #[test]
    fn aborting_multipart_upload_bucket_witnesses_round_trip() {
        let response = StorageRpcAbortingMultipartUploadBucketsResponse {
            witnesses: vec![AbortingMultipartUploadBucketWitness {
                bucket: BucketName::try_from("aborting-bucket").unwrap(),
                key: ObjectKey::try_from("aborting-key").unwrap(),
            }],
        };
        let encoded = encode_aborting_multipart_upload_buckets_response(&response).unwrap();
        assert_eq!(
            decode_aborting_multipart_upload_buckets_response(&encoded).unwrap(),
            response
        );
    }

    fn test_bucket_write_reservation_proof() -> BucketWriteReservationProof {
        BucketWriteReservationProof {
            bucket: BucketName::try_from("bucket-proof").unwrap(),
            reservation_id: "reservation-id".to_string(),
            owner_token: "owner-token".to_string(),
            cluster_epoch: ClusterEpoch::INITIAL,
            bucket_execution_generation: 31,
            bucket_incarnation_generation: 37,
            operation_kind: "put-object".to_string(),
            created_at: 41,
            lease_deadline: 43,
            target_context: Some("key/context".to_string()),
        }
    }
}
