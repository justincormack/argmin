// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Framing evidence only: metadata fixtures do not claim physical effects or
//! an installable complete command. Admitted owner integration is separate.
use super::*;

fn inspection() -> Vec<u8> {
    let frame = crate::tests::frozen_hex_fixture(include_str!("testdata/stream_file_primary_rpc_v37_payload_v1_descriptor_v1_state_v10_sse_s3_v5_request.hex"));
    decode_storage_rpc_frame(&frame).unwrap().payload
}

fn request(inspection: &[u8]) -> ack::Request<'_> {
    let files = complete_stream_file_primary::decode_request(inspection).unwrap();
    let allocation = complete_stream_allocation::decode_request(files.allocation).unwrap();
    ack::Request {
        receiver: allocation.window.route,
        provenance: ack::Provenance::Stream { inspection },
        receipts: (0..3)
            .map(|shard_index| ack::Receipt {
                shard_index,
                ack: WriteAck {
                    // Frozen Cobblestone geometry: 4 bytes + 56-byte header + 16-byte tag,
                    // padded across two systematic shards.
                    stored_size: 38,
                    crc64: 0x1234 + u64::from(shard_index),
                },
            })
            .collect(),
    }
}

fn prepared(bytes: &[u8]) -> command::BorrowedRequest<'_> {
    command::BorrowedRequest {
        node_id: NodeId::new(4),
        epoch: ClusterEpoch::new(5).unwrap(),
        pg_id: PgId::new(4),
        checksum: 0x12345678,
        command: bytes,
    }
}

#[test]
fn complete_stream_append_prepare_upload_part_and_put_are_distinct_requests() {
    let original = inspection();
    let files = complete_stream_file_primary::decode_request(&original).unwrap();
    let mut allocation = complete_stream_allocation::decode_request(files.allocation).unwrap();
    let Some(crate::ExpectedStreamAppendState::PutObject { mut session, .. }) =
        allocation.preparation.expected_append_state.take()
    else {
        panic!("frozen PUT input");
    };
    let upload_id = crate::tests::multipart_upload_id("append-wire-upload");
    session.target = StreamUploadTarget::UploadPart {
        upload_id: upload_id.clone(),
        part_number: 2,
    };
    let upload = MultipartUploadRecord {
        upload_id,
        bucket: session.bucket.clone(),
        key: session.key.clone(),
        initiated_at: 1000,
        state: UploadState::InProgress,
        tags: None,
        metadata_blob: SerializedMetadataBlob::default(),
        system_metadata_blob: SerializedSystemMetadataBlob::default(),
        initiator: OwnerIdentity::from_principal("owner"),
        owner: OwnerIdentity::from_principal("owner"),
        acl_grants: AclGrants::default(),
        public_read: false,
        object_generation_id: GenerationId::new(11).unwrap(),
        initiated_object_identity: None,
        object_lock: ObjectLockState::default(),
        checksum: None,
        encryption: session.encryption.clone(),
    };
    allocation.preparation.expected_append_state =
        Some(crate::ExpectedStreamAppendState::UploadPart {
            session,
            upload: Box::new(upload),
            part_generation_id: GenerationId::new(12).unwrap(),
        });
    let allocation = complete_stream_allocation::encode_request(&allocation).unwrap();
    let inspection =
        complete_stream_file_primary::encode_request(&allocation, &files.segment).unwrap();
    let part = encode_request(&request(&inspection)).unwrap();
    let put = encode_request(&request(&original)).unwrap();
    let reply = encode_response(part.bytes(), prepared(b"context")).unwrap();
    part.decode_response(&reply).unwrap();
    assert!(put.decode_response(&reply).is_err());
    let decoded = decode_request(part.bytes()).unwrap();
    assert_eq!(encode_request(&decoded).unwrap().bytes(), part.bytes());
}

#[test]
fn complete_stream_append_prepare_frozen_framing_and_borrowed_reply() {
    let inspection = inspection();
    let request = encode_request(&request(&inspection)).unwrap();
    // Deliberately opaque bytes: framing must not mint a decoded command.
    let body = b"untrusted complete context";
    let reply = encode_response(request.bytes(), prepared(body)).unwrap();
    assert_eq!(
        encode_request(&decode_request(request.bytes()).unwrap())
            .unwrap()
            .bytes(),
        request.bytes()
    );
    let decoded = request.decode_response(&reply).unwrap();
    assert_eq!(decoded.command, body);
    assert_eq!(decoded.command.as_ptr(), reply[66..].as_ptr());
    assert_eq!(decoded.checksum, 0x12345678);
    for (literal, bytes) in [
        (
            include_str!("testdata/stream_append_prepare_payload_v1_request.hex"),
            request.bytes(),
        ),
        (
            include_str!("testdata/stream_append_prepare_payload_v1_reply.hex"),
            reply.as_slice(),
        ),
    ] {
        assert_eq!(crate::tests::frozen_hex_fixture(literal), bytes);
    }
}

#[test]
fn complete_stream_append_prepare_distinct_kind_framing_and_admission_cap() {
    assert_eq!(
        StorageRpcMessageKind::StagingStreamAppendPrepare as u16,
        180
    );
    let mut frame = StorageRpcFrame {
        request_id: 17,
        kind: StorageRpcMessageKind::StagingStreamAppendPrepare,
        payload: vec![0; MAX_REQUEST_BYTES],
    };
    validate_storage_rpc_request_frame_payload_limit(&frame).unwrap();
    frame.payload.push(0);
    assert!(
        matches!(validate_storage_rpc_request_frame_payload_limit(&frame),
        Err(StorageRpcFrameError::PayloadTooLarge { len, limit })
        if len == MAX_REQUEST_BYTES + 1 && limit == MAX_REQUEST_BYTES)
    );
    for (name, literal) in [
        (
            "request",
            include_str!("testdata/stream_append_prepare_payload_v1_request.hex"),
        ),
        (
            "reply",
            include_str!("testdata/stream_append_prepare_payload_v1_reply.hex"),
        ),
    ] {
        let bytes = crate::tests::frozen_hex_fixture(literal);
        let payload = if name == "reply" {
            encode_storage_rpc_success_response(&bytes)
        } else {
            bytes
        };
        let frame = encode_storage_rpc_frame(
            17,
            StorageRpcMessageKind::StagingStreamAppendPrepare,
            &payload,
        )
        .unwrap();
        let decoded = decode_storage_rpc_frame(&frame).unwrap();
        assert_eq!(
            decoded.kind,
            StorageRpcMessageKind::StagingStreamAppendPrepare
        );
        assert_eq!(decoded.payload, payload);
        let digest = checksum::compute_checksum(checksum::ChecksumAlgorithm::Sha256, &frame)
            .bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        assert_eq!(
            digest,
            match name {
                "request" => "098a4beff69632e084c498b98c7b86a27f29d12449a0ad7e80a930e829dfddcd",
                "reply" => "0321266c1c5fb51de332f357341004e72f6a46c9082e80f090e80537a7b092f7",
                _ => unreachable!(),
            }
        );
    }
}

#[test]
fn complete_stream_append_prepare_rejects_crossed_request_fields_on_both_codecs() {
    let original = inspection();
    for field in [
        "primary",
        "pg",
        "epoch",
        "topology",
        "cutoff",
        "zero-cutoff",
        "missing",
        "extra",
        "size",
        "duplicate",
        "unordered",
        "direct",
    ] {
        let mut crossed = request(&original);
        match field {
            "primary" => crossed.receiver.node = NodeId::new(1),
            "pg" => crossed.receiver.pg = PgId::new(0),
            "epoch" => crossed.receiver.epoch = ClusterEpoch::INITIAL,
            "topology" => crossed.receiver.topology[0] ^= 1,
            "cutoff" => crossed.receiver.deadline.portable_wall_valid_until_ms += 1,
            "zero-cutoff" => crossed.receiver.deadline.portable_wall_valid_until_ms = 0,
            "missing" => {
                crossed.receipts.pop();
            }
            "extra" => crossed.receipts.push(ack::Receipt {
                shard_index: 3,
                ack: crossed.receipts[0].ack,
            }),
            "size" => {
                for receipt in &mut crossed.receipts {
                    receipt.ack.stored_size += 1;
                }
            }
            "duplicate" => crossed.receipts[1].shard_index = 0,
            "unordered" => crossed.receipts.swap(0, 1),
            "direct" => {
                crossed.provenance = ack::Provenance::Direct {
                    descriptor: b"descriptor",
                    logical_size: 4,
                }
            }
            _ => unreachable!(),
        }
        assert!(encode_request(&crossed).is_err(), "{field}");
        // Cases accepted by the nested ack grammar must independently fail at
        // the preparation decoder, not merely its safe encoder.
        if let Ok(nested) = ack::encode_request(&crossed) {
            let mut bytes = b"SAPP\x01\x00".to_vec();
            put_bytes(&mut bytes, nested.bytes());
            assert!(decode_request(&bytes).is_err(), "{field}");
        }
    }
    for field in [
        "session",
        "index",
        "size",
        "stored-crc",
        "logical-crc",
        "epoch",
        "zero-k",
        "ec-count",
    ] {
        let files = complete_stream_file_primary::decode_request(&original).unwrap();
        let mut segment = files.segment;
        match field {
            "session" => segment.session_id = crate::tests::stream_session_id("another-session"),
            "index" => segment.segment_index += 1,
            // 3 and 4 logical bytes both pad to 38-byte shards: this
            // independently pins size correspondence, not the receipt check.
            "size" => segment.size -= 1,
            "stored-crc" => segment.segment_crc64 ^= 1,
            "logical-crc" => segment.payload_crc64 ^= 1,
            "epoch" => segment.placement_cluster_epoch = ClusterEpoch::new(6).unwrap(),
            "zero-k" => segment.ec_k = 0,
            "ec-count" => segment.ec_m += 1,
            _ => unreachable!(),
        }
        let inspection =
            complete_stream_file_primary::encode_request(files.allocation, &segment).unwrap();
        let crossed = request(&inspection);
        assert!(encode_request(&crossed).is_err(), "{field}");
        let nested = ack::encode_request(&crossed).unwrap();
        let mut bytes = b"SAPP\x01\x00".to_vec();
        put_bytes(&mut bytes, nested.bytes());
        assert!(decode_request(&bytes).is_err(), "{field}");
    }
}

#[test]
fn complete_stream_append_prepare_rejects_malformed_and_cross_purpose_frames() {
    let inspection = inspection();
    let input = request(&inspection);
    let request = encode_request(&input).unwrap();
    let reply = encode_response(request.bytes(), prepared(b"context")).unwrap();
    for end in 0..request.bytes().len() {
        assert!(
            decode_request(&request.bytes()[..end]).is_err(),
            "request {end}"
        );
    }
    for end in 0..reply.len() {
        assert!(
            request.decode_response(&reply[..end]).is_err(),
            "reply {end}"
        );
    }
    for offset in [0, 4, 6] {
        let mut bad = request.bytes().to_vec();
        bad[offset] ^= 255;
        assert!(decode_request(&bad).is_err());
    }
    for offset in [0, 4, 6, 38, 42, 50] {
        let mut bad = reply.clone();
        bad[offset] ^= 255;
        assert!(request.decode_response(&bad).is_err());
    }
    for version in [0_u16, 2] {
        let mut bad = request.bytes().to_vec();
        bad[4..6].copy_from_slice(&version.to_le_bytes());
        assert!(decode_request(&bad).is_err());
        let mut bad = reply.clone();
        bad[4..6].copy_from_slice(&version.to_le_bytes());
        assert!(request.decode_response(&bad).is_err());
    }
    let mut bad = request.bytes().to_vec();
    bad.push(0);
    assert!(decode_request(&bad).is_err());
    let mut bad = reply;
    bad.push(0);
    assert!(request.decode_response(&bad).is_err());
    let nested = ack::encode_request(&input).unwrap();
    assert!(decode_request(nested.bytes()).is_err());
    assert!(ack::decode_request(request.bytes()).is_err());
    assert!(decode_request(&inspection).is_err());
    assert!(request
        .decode_response(&ack::encode_response(nested.bytes()).unwrap())
        .is_err());
    assert!(nested
        .decode_response(&encode_response(request.bytes(), prepared(b"context")).unwrap())
        .is_err());
}

#[test]
fn complete_stream_append_prepare_binds_replies_and_bounds_borrowed_complete_context() {
    let inspection = inspection();
    let mut input = request(&inspection);
    let request = encode_request(&input).unwrap();
    let reply = encode_response(request.bytes(), prepared(b"context")).unwrap();
    // Same route and geometry, different exact receipts or shorter cutoff.
    input.receipts[0].ack.crc64 ^= 1;
    let changed = encode_request(&input).unwrap();
    assert!(changed.decode_response(&reply).is_err());
    input.receipts[0].ack.crc64 ^= 1;
    input.receiver.deadline.portable_wall_valid_until_ms -= 1;
    assert!(encode_request(&input)
        .unwrap()
        .decode_response(&reply)
        .is_err());
    for receipt in &mut input.receipts {
        receipt.ack.crc64 = 0;
    }
    let zero_crc = encode_request(&input).unwrap();
    zero_crc
        .decode_response(&encode_response(zero_crc.bytes(), prepared(b"context")).unwrap())
        .unwrap();
    for field in ["primary", "epoch", "pg"] {
        let mut crossed = prepared(b"context");
        match field {
            "primary" => crossed.node_id = NodeId::new(1),
            "epoch" => crossed.epoch = ClusterEpoch::INITIAL,
            "pg" => crossed.pg_id = PgId::new(1),
            _ => unreachable!(),
        }
        assert!(
            encode_response(request.bytes(), crossed).is_err(),
            "{field}"
        );
    }
    let mut body = vec![0xa5; crate::metadata_command::context_carrier::MAX_COMPLETE_COMMAND_BYTES];
    let mut reply = encode_response(request.bytes(), prepared(&body)).unwrap();
    assert_eq!(reply.len(), MAX_RESPONSE_BYTES);
    assert_eq!(
        request.decode_response(&reply).unwrap().command.len(),
        body.len()
    );
    reply.push(0);
    assert!(matches!(
        request.decode_response(&reply),
        Err(StorageRpcPayloadError::PayloadTooLarge {
            limit: MAX_RESPONSE_BYTES,
            ..
        })
    ));
    body.push(0);
    assert!(encode_response(request.bytes(), prepared(&body)).is_err());
    assert!(encode_response(request.bytes(), prepared(b"")).is_err());
    assert!(matches!(
        decode_request(&vec![0; MAX_REQUEST_BYTES + 1]),
        Err(StorageRpcPayloadError::PayloadTooLarge {
            limit: MAX_REQUEST_BYTES,
            ..
        })
    ));
    let mut header = b"SAPP\x01\x00".to_vec();
    put_u32(&mut header, u32::MAX);
    assert!(matches!(
        decode_request(&header),
        Err(StorageRpcPayloadError::PayloadTooLarge { .. })
    ));
}
