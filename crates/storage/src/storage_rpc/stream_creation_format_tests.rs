// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Immutable evidence for stream creation's atomic generation ownership.
//! The raw expected-command carrier has no inner command marker; its layout is
//! owned by RPC v28, independently of the versioned build-response envelope.
//! The prior v27/v11 corpus remains literal, rejection-only evidence.
//! These are metadata-only specimens, not readable encrypted payload fixtures.

use super::*;
use crate::metadata_command::{MetadataCommandEnvelope, MetadataCommandId, MetadataCommandPayload};
use std::collections::BTreeMap;

const REQUEST_ID: u64 = 0x0102_0304_0506_0708;
const RETIRED: &str = include_str!("testdata/stream_creation_rpc_v27_command_v11_sse_s3_v5.hex");
const FROZEN: &str = include_str!("testdata/stream_creation_rpc_v28_command_v12_sse_s3_v5.hex");

enum Specimen {
    Match(Box<StorageRpcStreamUploadMatchRequest>),
    Build(Box<StorageRpcCreateStreamUploadCommandBuildRequest>),
    BuildResponse(StorageRpcObjectMetadataCommandBuildResponse),
    MatchResponse(StorageRpcStreamUploadMatchResponse),
    Error(StorageRpcMessageKind, StorageRpcErrorResponse),
}

impl Specimen {
    fn kind(&self) -> StorageRpcMessageKind {
        match self {
            Self::Match(_) | Self::MatchResponse(_) => {
                StorageRpcMessageKind::ObjectStreamUploadMatch
            }
            Self::Build(_) | Self::BuildResponse(_) => {
                StorageRpcMessageKind::ObjectStreamUploadCommandBuild
            }
            Self::Error(kind, _) => *kind,
        }
    }

    fn encode(&self) -> Vec<u8> {
        let payload = match self {
            Self::Match(request) => encode_stream_upload_match_request(request).unwrap(),
            Self::Build(request) => {
                encode_create_stream_upload_command_build_request(request).unwrap()
            }
            Self::BuildResponse(response) => encode_storage_rpc_success_response(
                &encode_object_metadata_command_build_response(response),
            ),
            Self::MatchResponse(response) => {
                encode_storage_rpc_success_response(&encode_stream_upload_match_response(response))
            }
            Self::Error(_, error) => encode_storage_rpc_error_response(error).unwrap(),
        };
        encode_storage_rpc_frame(REQUEST_ID, self.kind(), &payload).unwrap()
    }

    fn assert_decodes(&self, bytes: &[u8]) {
        let frame = decode_storage_rpc_frame(bytes).unwrap();
        assert_eq!(frame.request_id, REQUEST_ID);
        assert_eq!(frame.kind, self.kind());
        match self {
            Self::Match(expected) => assert_eq!(
                decode_stream_upload_match_request(&frame.payload).unwrap(),
                **expected
            ),
            Self::Build(expected) => assert_eq!(
                decode_create_stream_upload_command_build_request(&frame.payload).unwrap(),
                **expected
            ),
            Self::BuildResponse(expected) => {
                let payload = decode_storage_rpc_response_payload(&frame.payload)
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    decode_object_metadata_command_build_response(
                        &payload,
                        &MetadataCommandDecodeAuthority::new_for_test(),
                    )
                    .unwrap(),
                    *expected
                );
            }
            Self::MatchResponse(expected) => {
                let payload = decode_storage_rpc_response_payload(&frame.payload)
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    decode_stream_upload_match_response(&payload).unwrap(),
                    *expected
                );
            }
            Self::Error(_, expected) => assert_eq!(
                decode_storage_rpc_response_payload(&frame.payload).unwrap(),
                Err(expected.clone())
            ),
        }
    }
}

fn precondition_name(value: &StorageRpcCreateStreamUploadPrecondition) -> String {
    match value {
        StorageRpcCreateStreamUploadPrecondition::PutObjectNoCurrentCheck {} => {
            "put-unchecked".into()
        }
        StorageRpcCreateStreamUploadPrecondition::PutObject { expected_current } => {
            let current = match expected_current {
                None => "absent",
                Some(StoredObject::Live(_)) => "live",
                Some(StoredObject::DeleteMarker(_)) => "marker",
            };
            format!("put-{current}")
        }
        StorageRpcCreateStreamUploadPrecondition::UploadPart { .. } => "part".into(),
    }
}

fn specimens() -> BTreeMap<String, Specimen> {
    let bucket = BucketName::try_from("stream-format").unwrap();
    let key = ObjectKey::try_from("object").unwrap();
    let upload_id = UploadId::try_from("ab".repeat(UPLOAD_ID_LEN / 2)).unwrap();
    let encryption = ObjectEncryption::SseS3(crate::SseS3ObjectState::new(
        0x0102_0304,
        [0x11; crate::SSE_S3_WRAP_NONCE_LEN],
        [0x22; crate::SSE_S3_WRAPPED_DEK_LEN],
        [0x33; crate::SSE_S3_SEGMENT_NONCE_PREFIX_LEN],
    ));
    let owner = OwnerIdentity::from_principal("format-owner");
    let upload = MultipartUploadRecord {
        upload_id: upload_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        initiated_at: 29,
        state: UploadState::InProgress,
        tags: None,
        metadata_blob: SerializedMetadataBlob::default(),
        system_metadata_blob: SerializedSystemMetadataBlob::default(),
        initiator: owner.clone(),
        owner: owner.clone(),
        acl_grants: AclGrants::default(),
        public_read: false,
        object_generation_id: GenerationId::new(31).unwrap(),
        initiated_object_identity: None,
        object_lock: ObjectLockState::default(),
        checksum: None,
        encryption: encryption.clone(),
    };
    let live = StoredObject::Live(LiveObjectRecord {
        bucket: bucket.clone(),
        key: key.clone(),
        version_id: VersionId::Null,
        owner: owner.clone(),
        acl_grants: AclGrants::default(),
        public_read: false,
        generation_id: GenerationId::new(37).unwrap(),
        size: 41,
        etag: ObjectEtag::single_part(43),
        last_modified: 47,
        became_noncurrent_at: None,
        storage_class: StorageClass::Standard,
        ec: EcShape { k: 2, m: 1 },
        layout: ObjectLayout::Standard,
        tags: None,
        metadata_blob: None,
        system_metadata_blob: None,
        object_lock: ObjectLockState::default(),
        encryption: encryption.clone(),
    });
    let marker = StoredObject::DeleteMarker(DeleteMarkerRecord {
        bucket: bucket.clone(),
        key: key.clone(),
        version_id: VersionId::Null,
        owner,
        last_modified: 53,
    });
    let object = StorageRpcObjectRequest {
        node_id: NodeId::new(7),
        cluster_epoch: ClusterEpoch::new(9).unwrap(),
        pg_id: PgId::new(11),
        bucket: bucket.clone(),
        key: key.clone(),
    };
    let mut cases = BTreeMap::new();
    let mut add = |name: String, value| {
        assert!(
            cases.insert(name.clone(), value).is_none(),
            "duplicate specimen {name}"
        );
    };
    for target in [
        StreamUploadTarget::PutObject,
        StreamUploadTarget::UploadPart {
            upload_id,
            part_number: 17,
        },
    ] {
        let (label, operation) = match &target {
            StreamUploadTarget::PutObject => (
                "put",
                crate::metadata_command::PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
            ),
            StreamUploadTarget::UploadPart { .. } => (
                "part",
                crate::metadata_command::UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
            ),
        };
        let request = CreateStreamUploadReq {
            session_id: SessionId::try_from("0123456789abcdef0123456789abcdef").unwrap(),
            bucket: bucket.clone(),
            key: key.clone(),
            target: target.clone(),
            encryption: encryption.clone(),
        };
        let proof = BucketWriteReservationProof {
            bucket: bucket.clone(),
            reservation_id: "reservation".into(),
            owner_token: "owner-token".into(),
            cluster_epoch: object.cluster_epoch,
            bucket_execution_generation: 13,
            bucket_incarnation_generation: 19,
            operation_kind: operation.into(),
            created_at: 59,
            lease_deadline: 101,
            target_context: Some(key.as_str().into()),
        };
        add(
            format!("match-{label}-no-command"),
            Specimen::Match(Box::new(StorageRpcStreamUploadMatchRequest {
                object: object.clone(),
                request: request.clone(),
                expected_command: None,
            })),
        );
        for cleanup_after in [None, Some(67)] {
            let cleanup = if cleanup_after.is_some() {
                "deadline"
            } else {
                "no-deadline"
            };
            let command = CreateStreamUploadCommand::from_request_with_bucket_write_reservation_and_cleanup_deadline(
                request.clone(), GenerationId::new(61).unwrap(), 63, cleanup_after, proof.clone(),
            );
            add(
                format!("match-{label}-command-{cleanup}"),
                Specimen::Match(Box::new(StorageRpcStreamUploadMatchRequest {
                    object: object.clone(),
                    request: request.clone(),
                    expected_command: Some(command.clone()),
                })),
            );
            add(
                format!("build-response-{label}-{cleanup}"),
                Specimen::BuildResponse(StorageRpcObjectMetadataCommandBuildResponse {
                    outcome: StorageRpcObjectMetadataCommandBuildOutcome::Command(Box::new(
                        MetadataCommandEnvelope::new(
                            MetadataCommandId::new(
                                object.cluster_epoch,
                                object.pg_id,
                                MetadataCommandLogIndex::new(71).unwrap(),
                            ),
                            MetadataCommandPayload::CreateStreamUpload(Box::new(command)),
                        ),
                    )),
                }),
            );
            let preconditions = match &target {
                StreamUploadTarget::PutObject => std::iter::once(
                    StorageRpcCreateStreamUploadPrecondition::PutObjectNoCurrentCheck {},
                )
                .chain(
                    [None, Some(live.clone()), Some(marker.clone())]
                        .into_iter()
                        .map(move |expected_current| {
                            StorageRpcCreateStreamUploadPrecondition::PutObject { expected_current }
                        }),
                )
                .collect::<Vec<_>>(),
                StreamUploadTarget::UploadPart { .. } => {
                    vec![StorageRpcCreateStreamUploadPrecondition::UploadPart {
                        expected_upload: upload.clone(),
                    }]
                }
            };
            for precondition in preconditions {
                add(
                    format!("build-{}-{cleanup}", precondition_name(&precondition)),
                    Specimen::Build(Box::new(StorageRpcCreateStreamUploadCommandBuildRequest {
                        object: object.clone(),
                        request: request.clone(),
                        cleanup_after,
                        precondition,
                        bucket_write_reservation: proof.clone(),
                    })),
                );
            }
        }
    }
    for outcome in [
        StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot,
        StorageRpcObjectMetadataCommandBuildOutcome::Missing,
        StorageRpcObjectMetadataCommandBuildOutcome::LogConflict {
            node_id: 7,
            pg_id: 11,
            cluster_epoch: object.cluster_epoch,
            log_index: 73,
        },
    ] {
        let label = match &outcome {
            StorageRpcObjectMetadataCommandBuildOutcome::Command(_) => {
                unreachable!("command specimens are above")
            }
            StorageRpcObjectMetadataCommandBuildOutcome::StaleSnapshot => "stale",
            StorageRpcObjectMetadataCommandBuildOutcome::Missing => "missing",
            StorageRpcObjectMetadataCommandBuildOutcome::LogConflict { .. } => "conflict",
        };
        add(
            format!("build-response-{label}"),
            Specimen::BuildResponse(StorageRpcObjectMetadataCommandBuildResponse { outcome }),
        );
    }
    for exists in [false, true] {
        add(
            format!("match-response-{exists}"),
            Specimen::MatchResponse(StorageRpcStreamUploadMatchResponse { exists }),
        );
    }
    for (label, kind) in [
        (
            "build",
            StorageRpcMessageKind::ObjectStreamUploadCommandBuild,
        ),
        ("match", StorageRpcMessageKind::ObjectStreamUploadMatch),
    ] {
        add(
            format!("{label}-error"),
            Specimen::Error(
                kind,
                StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::PayloadDecode,
                    message: "invalid stream subject".into(),
                },
            ),
        );
    }
    cases
}

fn frozen_specimens(input: &str) -> BTreeMap<String, Vec<u8>> {
    let mut cases = BTreeMap::new();
    for line in input
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
    {
        let (name, hex) = line.split_once(' ').expect("name and exact hex frame");
        assert_eq!(hex.len() % 2, 0);
        let bytes = hex
            .as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| {
                let digit = |value: u8| char::from(value).to_digit(16).expect("hex digit") as u8;
                digit(pair[0]) * 16 + digit(pair[1])
            })
            .collect();
        assert!(
            cases.insert(name.to_owned(), bytes).is_none(),
            "duplicate frozen specimen {name}"
        );
    }
    cases
}

#[test]
fn stream_creation_rpc_v28_writers_match_frozen_command_v12_sse_s3_v5_evidence() {
    assert_eq!(STORAGE_RPC_FRAME_ENCODING_VERSION, 28);
    let current = specimens();
    let frozen = frozen_specimens(FROZEN);
    assert_eq!(current.len(), 27);
    assert_eq!(
        current.keys().collect::<Vec<_>>(),
        frozen.keys().collect::<Vec<_>>()
    );
    for (name, specimen) in current {
        assert_eq!(
            specimen.encode(),
            frozen[&name],
            "{name}: append evidence at a new containing version; do not rewrite the frozen bytes"
        );
    }
}

#[test]
fn stream_creation_rpc_v28_readers_decode_frozen_command_v12_sse_s3_v5_evidence() {
    let current = specimens();
    let frozen = frozen_specimens(FROZEN);
    assert_eq!(
        current.keys().collect::<Vec<_>>(),
        frozen.keys().collect::<Vec<_>>()
    );
    for (name, bytes) in frozen {
        current[&name].assert_decodes(&bytes);
    }
}

#[test]
fn retired_stream_creation_rpc_v27_frames_are_rejected() {
    let frozen = frozen_specimens(RETIRED);
    assert_eq!(frozen.len(), 35);
    for (name, bytes) in frozen {
        assert!(
            matches!(
                decode_storage_rpc_frame(&bytes),
                Err(StorageRpcFrameError::UnsupportedVersion(27))
            ),
            "{name}"
        );
    }
}
