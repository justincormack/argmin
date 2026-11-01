// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Freeze the existing reservation lifecycle before adding introduction custody.
//! This targeted family corpus does not enumerate all RPCs carrying a proof.

use super::*;
use std::collections::BTreeMap;

const REQUEST_ID: u64 = 0x0102_0304_0506_0708;

#[derive(Debug, PartialEq, Eq)]
enum Request {
    Acquire(StorageRpcBucketWriteReservationAcquireRequest),
    Validate(StorageRpcBucketWriteReservationProofRequest),
    Heartbeat(StorageRpcBucketWriteReservationHeartbeatRequest),
    Release(StorageRpcBucketWriteReservationRecordRequest),
    ProofRelease(StorageRpcProofReleaseRequest),
    Update(Box<StorageRpcStreamUploadBucketWriteReservationUpdateRequest>),
    List(StorageRpcBucketRequest),
}

impl Request {
    fn kind(&self) -> StorageRpcMessageKind {
        match self {
            Self::Acquire(_) => StorageRpcMessageKind::BucketWriteReservationAcquire,
            Self::Validate(_) => StorageRpcMessageKind::BucketWriteReservationValidate,
            Self::Heartbeat(_) => StorageRpcMessageKind::BucketWriteReservationHeartbeat,
            Self::Release(_) => StorageRpcMessageKind::BucketWriteReservationRelease,
            Self::ProofRelease(_) => StorageRpcMessageKind::ProofRelease,
            Self::Update(_) => {
                StorageRpcMessageKind::ObjectStreamUploadBucketWriteReservationUpdate
            }
            Self::List(_) => StorageRpcMessageKind::BucketWriteReservationsList,
        }
    }

    fn encode(&self) -> Vec<u8> {
        match self {
            Self::Acquire(value) => encode_bucket_write_reservation_acquire_request(value).unwrap(),
            Self::Validate(value) => encode_bucket_write_reservation_proof_request(value).unwrap(),
            Self::Heartbeat(value) => {
                encode_bucket_write_reservation_heartbeat_request(value).unwrap()
            }
            Self::Release(value) => encode_bucket_write_reservation_record_request(value).unwrap(),
            Self::ProofRelease(value) => encode_proof_release_request(value).unwrap(),
            Self::Update(value) => {
                encode_stream_upload_bucket_write_reservation_update_request(value)
            }
            Self::List(value) => encode_bucket_request(value),
        }
    }

    fn decode(&self, bytes: &[u8]) -> Result<Self, StorageRpcPayloadError> {
        match self {
            Self::Acquire(_) => {
                decode_bucket_write_reservation_acquire_request(bytes).map(Self::Acquire)
            }
            Self::Validate(_) => {
                decode_bucket_write_reservation_proof_request(bytes).map(Self::Validate)
            }
            Self::Heartbeat(_) => {
                decode_bucket_write_reservation_heartbeat_request(bytes).map(Self::Heartbeat)
            }
            Self::Release(_) => {
                decode_bucket_write_reservation_record_request(bytes).map(Self::Release)
            }
            Self::ProofRelease(_) => decode_proof_release_request(bytes).map(Self::ProofRelease),
            Self::Update(_) => decode_stream_upload_bucket_write_reservation_update_request(bytes)
                .map(|value| Self::Update(Box::new(value))),
            Self::List(_) => decode_bucket_request(bytes).map(Self::List),
        }
    }
}

enum Reply {
    Record(StorageRpcBucketWriteReservationRecordResponse),
    List(StorageRpcBucketWriteReservationsListResponse),
    Empty,
    Error(StorageRpcErrorResponse),
}

impl Reply {
    fn encode(&self) -> Vec<u8> {
        match self {
            Self::Record(value) => encode_storage_rpc_success_response(
                &encode_bucket_write_reservation_record_response(value).unwrap(),
            ),
            Self::List(value) => encode_storage_rpc_success_response(
                &encode_bucket_write_reservations_list_response(value).unwrap(),
            ),
            Self::Empty => encode_storage_rpc_success_response(&[]),
            Self::Error(error) => encode_storage_rpc_error_response(error).unwrap(),
        }
    }

    fn matches(&self, bytes: &[u8], reusable: bool) -> bool {
        let Ok(decoded) = decode_storage_rpc_response_payload_with_connection_disposition(bytes)
        else {
            return false;
        };
        if decoded.connection_reusable != reusable {
            return false;
        }
        match (self, decoded.response) {
            (Self::Record(value), Ok(body)) => {
                decode_bucket_write_reservation_record_response(&body).as_ref() == Ok(value)
            }
            (Self::List(value), Ok(body)) => {
                decode_bucket_write_reservations_list_response(&body).as_ref() == Ok(value)
            }
            (Self::Empty, Ok(body)) => body.is_empty(),
            (Self::Error(expected), Err(actual)) => *expected == actual,
            (Self::Record(_) | Self::List(_) | Self::Empty, Err(_)) | (Self::Error(_), Ok(_)) => {
                false
            }
        }
    }
}

enum Specimen {
    Request(Request),
    Reply {
        kind: StorageRpcMessageKind,
        body: Reply,
        reusable: bool,
    },
}

impl Specimen {
    fn kind(&self) -> StorageRpcMessageKind {
        match self {
            Self::Request(request) => request.kind(),
            Self::Reply { kind, .. } => *kind,
        }
    }

    fn payload(&self) -> Vec<u8> {
        match self {
            Self::Request(request) => request.encode(),
            Self::Reply { body, reusable, .. } => {
                let mut bytes = body.encode();
                set_storage_rpc_response_connection_reusable(&mut bytes, *reusable).unwrap();
                bytes
            }
        }
    }

    fn matches(&self, bytes: &[u8]) -> bool {
        match self {
            Self::Request(request) => request.decode(bytes).as_ref() == Ok(request),
            Self::Reply { body, reusable, .. } => body.matches(bytes, *reusable),
        }
    }

    fn rejects(&self, bytes: &[u8]) -> bool {
        match self {
            Self::Request(request) => request.decode(bytes).is_err(),
            Self::Reply { .. } => {
                decode_storage_rpc_response_payload_with_connection_disposition(bytes).is_err()
            }
        }
    }
}

fn record(target: bool, epoch: u64) -> BucketWriteReservationRecord {
    BucketWriteReservationRecord {
        bucket: BucketName::try_from("bucket").unwrap(),
        reservation_id: "reservation-1".into(),
        owner_token: "owner-2".into(),
        cluster_epoch: ClusterEpoch::new(epoch).unwrap(),
        bucket_execution_generation: 17,
        bucket_incarnation_generation: 23,
        operation_kind: if target {
            "put-object-stream-create"
        } else {
            "bucket-write-snapshot"
        }
        .into(),
        created_at: 1_000,
        lease_deadline: 8_000,
        target_context: target.then(|| "object-key".into()),
    }
}

fn deadline(bounded: bool) -> Option<StorageRpcAdmittedRouteEffectDeadline> {
    bounded.then_some(StorageRpcAdmittedRouteEffectDeadline {
        authority_valid_until_ms: 7_000,
        portable_wall_valid_until_ms: 5_000,
    })
}

fn response_cases(kind: StorageRpcMessageKind) -> Vec<(String, Reply)> {
    let mut cases = match kind {
        StorageRpcMessageKind::BucketWriteReservationAcquire
        | StorageRpcMessageKind::BucketWriteReservationHeartbeat => {
            let mut cases = Vec::new();
            for target in [false, true] {
                for epoch in [5, 9] {
                    if epoch == 5 && kind == StorageRpcMessageKind::BucketWriteReservationAcquire {
                        continue;
                    }
                    let mut row = record(target, epoch);
                    if kind == StorageRpcMessageKind::BucketWriteReservationHeartbeat {
                        row.lease_deadline = 12_000;
                    }
                    let outcome = StorageRpcBucketWriteReservationAcquireOutcome::Acquired(row);
                    cases.push((
                        format!("acquired-target-{target}-epoch-{epoch}"),
                        Reply::Record(StorageRpcBucketWriteReservationRecordResponse { outcome }),
                    ));
                }
            }
            // Both codecs have this closed outcome grammar; only acquisition
            // dispatch emits these two non-acquired success variants today.
            if kind == StorageRpcMessageKind::BucketWriteReservationAcquire {
                for outcome in [
                    StorageRpcBucketWriteReservationAcquireOutcome::Draining,
                    StorageRpcBucketWriteReservationAcquireOutcome::BucketNotFound {
                        name: BucketName::try_from("missing").unwrap(),
                    },
                ] {
                    let label = outcome_label(&outcome);
                    cases.push((
                        label.into(),
                        Reply::Record(StorageRpcBucketWriteReservationRecordResponse { outcome }),
                    ));
                }
            }
            cases
        }
        StorageRpcMessageKind::BucketWriteReservationsList => (0..=2)
            .map(|count| {
                let mut rows = vec![record(false, 5), record(true, 9)];
                rows[1].reservation_id = "reservation-2".into();
                rows.truncate(count);
                (
                    format!("list-{count}"),
                    Reply::List(StorageRpcBucketWriteReservationsListResponse { records: rows }),
                )
            })
            .collect(),
        StorageRpcMessageKind::BucketWriteReservationValidate
        | StorageRpcMessageKind::BucketWriteReservationRelease
        | StorageRpcMessageKind::ProofRelease
        | StorageRpcMessageKind::ObjectStreamUploadBucketWriteReservationUpdate => {
            vec![("success".into(), Reply::Empty)]
        }
        _ => panic!("not a reservation-family specimen"),
    };
    cases.push((
        "expired".into(),
        Reply::Error(StorageRpcErrorResponse {
            code: StorageRpcErrorCode::StaleShardLocation,
            message: "expired".into(),
        }),
    ));
    cases.push((
        "invalid".into(),
        Reply::Error(StorageRpcErrorResponse {
            code: StorageRpcErrorCode::PayloadDecode,
            message: "invalid reservation".into(),
        }),
    ));
    cases
}

fn outcome_label(outcome: &StorageRpcBucketWriteReservationAcquireOutcome) -> &'static str {
    // No wildcard: extending the production outcome enum requires reviewing
    // this corpus, not merely updating both sides of a round-trip test.
    match outcome {
        StorageRpcBucketWriteReservationAcquireOutcome::Acquired(_) => "acquired",
        StorageRpcBucketWriteReservationAcquireOutcome::Draining => "draining",
        StorageRpcBucketWriteReservationAcquireOutcome::BucketNotFound { .. } => "missing",
    }
}

fn specimens() -> BTreeMap<String, Specimen> {
    let mut cases = BTreeMap::new();
    let mut add = |name: String, specimen| {
        assert!(cases.insert(name, specimen).is_none());
    };
    for target in [false, true] {
        let row = record(target, 9);
        for bounded in [false, true] {
            add(
                format!("acquire-target-{target}-bounded-{bounded}"),
                Specimen::Request(Request::Acquire(
                    StorageRpcBucketWriteReservationAcquireRequest {
                        node_id: NodeId::new(7),
                        cluster_epoch: ClusterEpoch::new(9).unwrap(),
                        pg_id: PgId::new(3),
                        bucket: row.bucket.clone(),
                        reservation_id: row.reservation_id.clone(),
                        owner_token: row.owner_token.clone(),
                        operation_kind: row.operation_kind.clone(),
                        created_at: row.created_at,
                        lease_deadline: row.lease_deadline,
                        target_context: row.target_context.clone(),
                        effect_deadline: deadline(bounded),
                    },
                )),
            );
        }
        for epoch in [5, 9] {
            let row = record(target, epoch);
            let proof = BucketWriteReservationProof::from(&row);
            add(
                format!("validate-target-{target}-epoch-{epoch}"),
                Specimen::Request(Request::Validate(
                    StorageRpcBucketWriteReservationProofRequest {
                        node_id: NodeId::new(7),
                        route_cluster_epoch: ClusterEpoch::new(9).unwrap(),
                        pg_id: PgId::new(3),
                        proof: proof.clone(),
                    },
                )),
            );
            add(
                format!("release-target-{target}-epoch-{epoch}"),
                Specimen::Request(Request::Release(
                    StorageRpcBucketWriteReservationRecordRequest {
                        node_id: NodeId::new(7),
                        route_cluster_epoch: ClusterEpoch::new(9).unwrap(),
                        pg_id: PgId::new(3),
                        record: row,
                    },
                )),
            );
            for bounded in [false, true] {
                add(
                    format!("heartbeat-target-{target}-epoch-{epoch}-bounded-{bounded}"),
                    Specimen::Request(Request::Heartbeat(
                        StorageRpcBucketWriteReservationHeartbeatRequest {
                            node_id: NodeId::new(7),
                            route_cluster_epoch: ClusterEpoch::new(9).unwrap(),
                            pg_id: PgId::new(3),
                            proof: proof.clone(),
                            lease_deadline: 12_000,
                            effect_deadline: deadline(bounded),
                        },
                    )),
                );
                add(
                    format!("proof-release-target-{target}-epoch-{epoch}-bounded-{bounded}"),
                    Specimen::Request(Request::ProofRelease(StorageRpcProofReleaseRequest {
                        node_id: NodeId::new(7),
                        route_cluster_epoch: ClusterEpoch::new(9).unwrap(),
                        pg_id: PgId::new(3),
                        proof: proof.clone(),
                        operation_deadline: bounded.then_some(StorageRpcOperationDeadline {
                            portable_wall_valid_until_ms: 5_000,
                        }),
                    })),
                );
                if !target {
                    continue;
                } // Stream sessions always bind an exact object key.
                let mut renewed = proof.clone();
                renewed.lease_deadline = 12_000;
                add(
                    format!("update-target-{target}-epoch-{epoch}-bounded-{bounded}"),
                    Specimen::Request(Request::Update(Box::new(
                        StorageRpcStreamUploadBucketWriteReservationUpdateRequest {
                            object: StorageRpcObjectRequest {
                                node_id: NodeId::new(7),
                                cluster_epoch: ClusterEpoch::new(9).unwrap(),
                                pg_id: PgId::new(4),
                                bucket: proof.bucket.clone(),
                                key: ObjectKey::try_from("object-key").unwrap(),
                            },
                            session_id: SessionId::try_from("0123456789abcdef0123456789abcdef")
                                .unwrap(),
                            current: proof.clone(),
                            renewed,
                            effect_deadline: deadline(bounded),
                        },
                    ))),
                );
            }
        }
    }
    add(
        "list-request".into(),
        Specimen::Request(Request::List(StorageRpcBucketRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(9).unwrap(),
            pg_id: PgId::new(3),
            bucket: BucketName::try_from("bucket").unwrap(),
        })),
    );
    for (name, kind) in families() {
        for reusable in [false, true] {
            for (label, body) in response_cases(kind) {
                add(
                    format!("{name}-reply-{label}-reusable-{reusable}"),
                    Specimen::Reply {
                        kind,
                        body,
                        reusable,
                    },
                );
            }
        }
    }
    cases
}

fn families() -> [(&'static str, StorageRpcMessageKind); 7] {
    [
        (
            "acquire",
            StorageRpcMessageKind::BucketWriteReservationAcquire,
        ),
        (
            "validate",
            StorageRpcMessageKind::BucketWriteReservationValidate,
        ),
        (
            "heartbeat",
            StorageRpcMessageKind::BucketWriteReservationHeartbeat,
        ),
        (
            "release",
            StorageRpcMessageKind::BucketWriteReservationRelease,
        ),
        ("proof-release", StorageRpcMessageKind::ProofRelease),
        (
            "update",
            StorageRpcMessageKind::ObjectStreamUploadBucketWriteReservationUpdate,
        ),
        ("list", StorageRpcMessageKind::BucketWriteReservationsList),
    ]
}

fn frozen() -> BTreeMap<String, Vec<u8>> {
    let mut cases = BTreeMap::new();
    for line in include_str!("testdata/bucket_reservation_rpc_v34.hex")
        .lines()
        .filter(|line| !line.starts_with('#'))
    {
        let (name, hex) = line.split_once(' ').unwrap();
        assert!(cases
            .insert(name.to_owned(), crate::tests::frozen_hex_fixture(hex))
            .is_none());
    }
    cases
}

#[test]
fn bucket_reservation_v34_writer_and_reader_match_frozen_lifecycle() {
    assert_eq!(STORAGE_RPC_FRAME_ENCODING_VERSION, 34);
    assert_eq!(
        families().map(|(_, kind)| kind as u16),
        [38, 39, 123, 40, 11, 84, 83]
    );
    let expected = frozen();
    let cases = specimens();
    assert_eq!(cases.len(), 91);
    assert_eq!(
        cases.keys().collect::<Vec<_>>(),
        expected.keys().collect::<Vec<_>>()
    );
    for (name, case) in cases {
        assert_eq!(
            encode_storage_rpc_frame(REQUEST_ID, case.kind(), &case.payload()).unwrap(),
            expected[&name],
            "{name}"
        );
        let frame = decode_storage_rpc_frame(&expected[&name]).unwrap();
        assert_eq!(frame.kind, case.kind(), "{name}");
        assert_eq!(frame.request_id, REQUEST_ID, "{name}");
        assert!(case.matches(&frame.payload), "{name}");
        if let Specimen::Request(_) = case {
            let admitted =
                read_storage_rpc_request_frame_from(&mut std::io::Cursor::new(&expected[&name]))
                    .unwrap();
            assert_eq!(admitted, frame, "{name}");
        }
    }
}

#[test]
fn bucket_reservation_v34_rejects_truncation_trailing_data_and_other_versions() {
    for (name, case) in specimens() {
        let bytes = &frozen()[&name];
        let frame = decode_storage_rpc_frame(bytes).unwrap();
        for end in 0..frame.payload.len() {
            assert!(
                case.rejects(&frame.payload[..end]),
                "{name}: accepted truncation at {end}"
            );
        }
        let mut trailing = frame.payload.clone();
        trailing.push(0);
        assert!(case.rejects(&trailing), "{name}: accepted trailing data");
        for version in [33, 35] {
            let bytes = encode_storage_rpc_frame_with_version_for_test(
                REQUEST_ID,
                case.kind(),
                &frame.payload,
                version,
            );
            assert_eq!(
                decode_storage_rpc_frame(&bytes),
                Err(StorageRpcFrameError::UnsupportedVersion(version)),
                "{name}"
            );
            assert!(
                matches!(read_storage_rpc_request_frame_from(&mut std::io::Cursor::new(bytes)), Err(StorageRpcStreamError::Frame(StorageRpcFrameError::UnsupportedVersion(actual))) if actual == version),
                "{name}"
            );
        }
    }
}

#[test]
fn bucket_reservation_v34_rejects_unknown_optional_and_outcome_tags() {
    let frozen = frozen();
    for (name, case) in specimens() {
        let Specimen::Request(request) = &case else {
            continue;
        };
        let frame = decode_storage_rpc_frame(&frozen[&name]).unwrap();
        let (target, deadline) = match request {
            Request::Acquire(value) => (
                Some(&value.target_context),
                Some(if value.effect_deadline.is_some() {
                    17
                } else {
                    1
                }),
            ),
            Request::Validate(value) => (Some(&value.proof.target_context), None),
            Request::Release(value) => (Some(&value.record.target_context), None),
            Request::Heartbeat(value) => (
                None,
                Some(if value.effect_deadline.is_some() {
                    17
                } else {
                    1
                }),
            ),
            Request::ProofRelease(value) => (
                None,
                Some(if value.operation_deadline.is_some() {
                    9
                } else {
                    1
                }),
            ),
            Request::Update(value) => (
                None,
                Some(if value.effect_deadline.is_some() {
                    17
                } else {
                    1
                }),
            ),
            Request::List(_) => (None, None),
        };
        if let Some(length) = deadline {
            let mut invalid = frame.payload.clone();
            let offset = invalid.len() - length;
            invalid[offset] = 2;
            assert!(
                request.decode(&invalid).is_err(),
                "{name}: accepted unknown deadline tag"
            );
        }
        if let Some(target) = target {
            let mut invalid = frame.payload.clone();
            let target_len = 1 + target.as_ref().map_or(0, |value| 4 + value.len());
            let offset = invalid.len() - deadline.unwrap_or(0) - target_len;
            invalid[offset] = 2;
            assert!(
                request.decode(&invalid).is_err(),
                "{name}: accepted unknown target tag"
            );
        }
    }
    assert!(decode_bucket_write_reservation_record_response(&[3]).is_err());
    assert!(decode_bucket_write_reservations_list_response(&u32::MAX.to_le_bytes()).is_err());
}

#[test]
fn bucket_reservation_v34_max_fields_fit_frozen_request_caps() {
    let mut row = record(true, 9);
    row.bucket = BucketName::try_from("b".repeat(STORAGE_RPC_MAX_BUCKET_NAME_LEN)).unwrap();
    row.reservation_id = "r".repeat(STORAGE_RPC_MAX_BUCKET_WRITE_RESERVATION_ID_LEN);
    row.owner_token = "o".repeat(STORAGE_RPC_MAX_BUCKET_WRITE_OWNER_TOKEN_LEN);
    row.operation_kind = "p".repeat(STORAGE_RPC_MAX_BUCKET_WRITE_OPERATION_KIND_LEN);
    row.target_context = Some("k".repeat(STORAGE_RPC_MAX_BUCKET_WRITE_TARGET_CONTEXT_LEN));
    let proof = BucketWriteReservationProof::from(&row);
    let mut cases = BTreeMap::new();
    for case in specimens().into_values() {
        if let Specimen::Request(request) = case {
            cases.insert(request.kind() as u16, request);
        }
    }
    for ((_, kind), cap) in families()
        .into_iter()
        .zip([2566, 2572, 2597, 2572, 2581, 6276, 83])
    {
        let mut request = cases.remove(&(kind as u16)).unwrap();
        match &mut request {
            Request::Acquire(value) => {
                value.bucket = row.bucket.clone();
                value.reservation_id = row.reservation_id.clone();
                value.owner_token = row.owner_token.clone();
                value.operation_kind = row.operation_kind.clone();
                value.target_context = row.target_context.clone();
                value.effect_deadline = deadline(true);
            }
            Request::Validate(value) => value.proof = proof.clone(),
            Request::Release(value) => value.record = row.clone(),
            Request::Heartbeat(value) => {
                value.proof = proof.clone();
                value.effect_deadline = deadline(true);
            }
            Request::ProofRelease(value) => {
                value.proof = proof.clone();
                value.operation_deadline = Some(StorageRpcOperationDeadline {
                    portable_wall_valid_until_ms: 5_000,
                });
            }
            Request::Update(value) => {
                value.object.bucket = row.bucket.clone();
                value.object.key =
                    ObjectKey::try_from("k".repeat(STORAGE_RPC_MAX_OBJECT_KEY_LEN)).unwrap();
                value.current = proof.clone();
                value.renewed = proof.clone();
                value.renewed.lease_deadline = 12_000;
                value.effect_deadline = deadline(true);
            }
            Request::List(value) => value.bucket = row.bucket.clone(),
        }
        assert_eq!(
            message_kind_request_max_payload_len(kind, STORAGE_RPC_MAX_PAYLOAD_LEN),
            cap
        );
        let payload = request.encode();
        // Acquisition's existing limit includes one unused byte. Freeze this
        // allowance rather than changing production admission in evidence work.
        let slack = usize::from(kind == StorageRpcMessageKind::BucketWriteReservationAcquire);
        assert_eq!(payload.len(), cap - slack, "{kind:?}");
        let bytes = encode_storage_rpc_frame(REQUEST_ID, kind, &payload).unwrap();
        let admitted =
            read_storage_rpc_request_frame_from(&mut std::io::Cursor::new(bytes)).unwrap();
        assert_eq!(request.decode(&admitted.payload).unwrap(), request);
        for len in [cap, cap + 1] {
            let bytes = encode_storage_rpc_frame(REQUEST_ID, kind, &vec![0; len]).unwrap();
            let result = read_storage_rpc_request_frame_from(&mut std::io::Cursor::new(bytes));
            if len == cap {
                assert!(result.is_ok(), "{kind:?}");
            } else {
                assert!(
                    matches!(result, Err(StorageRpcStreamError::Frame(StorageRpcFrameError::PayloadTooLarge {len: actual, limit})) if actual == len && limit == cap),
                    "{kind:?}"
                );
            }
        }
    }
    assert!(cases.is_empty());
}
