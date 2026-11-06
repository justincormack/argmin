// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::StoreError;

fn cursor() -> ShardScavengerReferenceCursor {
    ShardScavengerReferenceCursor::StagingWindow {
        pg_id: PgId::new(3),
        topology_digest: [10; 32],
        attempt: SessionId::try_from("1".repeat(32)).unwrap(),
        introduction_epoch: ClusterEpoch::new(5).unwrap(),
        introduction_index: 9,
        window_index: 2,
    }
}

fn page() -> ShardScavengerReferencePage {
    ShardScavengerReferencePage {
        complete: true,
        items: vec![ShardScavengerReferencePageItem {
            cursor: cursor(),
            reference: ShardScavengerPayloadReference::StagingProtection(
                crate::types::ShardScavengerStagingProtection {
                    identity: ShardScavengerProtectedShardSetReference {
                        data_pg_id: 4,
                        okh: [8; 16],
                        generation_id: GenerationId::new(9).unwrap(),
                        ec: EcShape { k: 2, m: 1 },
                    },
                    placement_epoch: ClusterEpoch::new(5).unwrap(),
                    file_nodes: vec![NodeId::new(2), NodeId::new(0), NodeId::new(3)],
                    acknowledgement_primary: NodeId::new(1),
                },
            ),
        }],
    }
}

#[test]
fn staging_scavenger_v35_matches_frozen_request_and_response() {
    assert_eq!(STORAGE_RPC_FRAME_ENCODING_VERSION, 35);
    let request = StorageRpcShardScavengerReferencePageRequest {
        route: StorageRpcBucketPgRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(6).unwrap(),
            pg_id: PgId::new(3),
        },
        after: Some(cursor()),
        limit: NonZeroU16::new(64).unwrap(),
    };
    for (input, payload) in [
        (
            include_str!("testdata/staging_scavenger_rpc_v35_request.hex"),
            encode_shard_scavenger_reference_page_request(&request).unwrap(),
        ),
        (
            include_str!("testdata/staging_scavenger_rpc_v35_response.hex"),
            encode_storage_rpc_success_response(
                &encode_shard_scavenger_reference_page_response(&page()).unwrap(),
            ),
        ),
    ] {
        let bytes = crate::tests::frozen_hex_fixture(input);
        assert_eq!(
            encode_storage_rpc_frame(
                17,
                StorageRpcMessageKind::ShardScavengerReferencePage,
                &payload
            )
            .unwrap(),
            bytes
        );
        let decoded = decode_storage_rpc_frame(&bytes).unwrap();
        assert_eq!(decoded.payload, payload);
    }
    let request_frame = crate::tests::frozen_hex_fixture(include_str!(
        "testdata/staging_scavenger_rpc_v35_request.hex"
    ));
    let decoded =
        read_storage_rpc_request_frame_from(&mut std::io::Cursor::new(request_frame)).unwrap();
    assert_eq!(
        decode_shard_scavenger_reference_page_request(&decoded.payload).unwrap(),
        request
    );
    let response_frame = decode_storage_rpc_frame(&crate::tests::frozen_hex_fixture(include_str!(
        "testdata/staging_scavenger_rpc_v35_response.hex"
    )))
    .unwrap();
    let response = decode_storage_rpc_response_payload(&response_frame.payload)
        .unwrap()
        .unwrap();
    assert_eq!(
        decode_shard_scavenger_reference_page_response(&response).unwrap(),
        page()
    );
}

#[test]
fn staging_scavenger_v35_rejects_malformed_participants_and_crossed_cursor() {
    let original = page();
    let bytes = encode_shard_scavenger_reference_page_response(&original).unwrap();
    for end in 0..bytes.len() {
        assert!(decode_shard_scavenger_reference_page_response(&bytes[..end]).is_err());
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(decode_shard_scavenger_reference_page_response(&trailing).is_err());
    for case in 0..5 {
        let mut bad = original.clone();
        let ShardScavengerPayloadReference::StagingProtection(protection) =
            &mut bad.items[0].reference
        else {
            unreachable!()
        };
        match case {
            0 => {
                protection.file_nodes.pop();
            }
            1 => protection.file_nodes[1] = protection.file_nodes[0],
            2 => protection.identity.ec.k = 0,
            3 => {
                bad.items[0].cursor = ShardScavengerReferenceCursor::StreamUploadSegment {
                    session_id: SessionId::try_from("1".repeat(32)).unwrap(),
                    segment_index: 2,
                }
            }
            4 => {
                if let ShardScavengerReferenceCursor::StagingWindow {
                    introduction_index, ..
                } = &mut bad.items[0].cursor
                {
                    *introduction_index = 0;
                }
            }
            _ => unreachable!(),
        }
        assert!(
            encode_shard_scavenger_reference_page_response(&bad).is_err(),
            "case {case}"
        );
        // Bypass the checked writer to exercise the receiving boundary independently.
        let mut raw = vec![1, 1, 0];
        put_shard_scavenger_reference_cursor(&mut raw, &bad.items[0].cursor);
        put_scavenger_payload_reference(&mut raw, &bad.items[0].reference);
        assert!(
            decode_shard_scavenger_reference_page_response(&raw).is_err(),
            "case {case}"
        );
    }
    let request = StorageRpcShardScavengerReferencePageRequest {
        route: StorageRpcBucketPgRequest {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(6).unwrap(),
            pg_id: PgId::new(4),
        },
        after: Some(cursor()),
        limit: NonZeroU16::new(64).unwrap(),
    };
    assert!(encode_shard_scavenger_reference_page_request(&request).is_err());
}

#[test]
fn storage_rpc_v34_predecessor_corpora_remain_rejected() {
    for corpus in [
        include_str!("testdata/stream_creation_rpc_v34_command_v13_sse_s3_v5.hex"),
        include_str!("testdata/shard_ack_rpc_v34.hex"),
        include_str!("testdata/bucket_reservation_rpc_v34.hex"),
    ] {
        for line in corpus
            .lines()
            .filter(|line| !line.starts_with('#') && !line.is_empty())
        {
            let (_, hex) = line.split_once(' ').unwrap();
            let bytes = crate::tests::frozen_hex_fixture(hex);
            assert_eq!(
                decode_storage_rpc_frame(&bytes),
                Err(StorageRpcFrameError::UnsupportedVersion(34))
            );
        }
    }
    for input in [
        include_str!("testdata/checkpoint_transfer_rpc_v34_checkpoint_v7_install_request_canonical_v9_pg_v10.hex"),
        include_str!("testdata/checkpoint_transfer_rpc_v34_checkpoint_v7_export_response_canonical_v9_pg_v10.hex"),
        include_str!("testdata/checkpoint_transfer_rpc_v34_checkpoint_v8_install_request_canonical_v10_pg_v11.hex"),
        include_str!("testdata/checkpoint_transfer_rpc_v34_checkpoint_v8_export_response_canonical_v10_pg_v11.hex"),
    ] {
        assert_eq!(decode_storage_rpc_frame(&crate::tests::frozen_hex_fixture(input)), Err(StorageRpcFrameError::UnsupportedVersion(34)));
    }
}

#[test]
fn staging_scavenger_unix_rejects_crossed_and_nonprogressing_peer_pages() {
    use crate::node_runtime::client_facade::{ShardScavengerNodeClient, UnixStorageNodeClient};
    use std::os::unix::fs::PermissionsExt;
    for case in 0..6 {
        let tmp = test_util::tempdir();
        std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let socket = tmp.path().join("peer.sock");
        let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        let after = (case >= 3).then(cursor);
        let mut response = page();
        match case {
            0 => {
                if let ShardScavengerReferenceCursor::StagingWindow { pg_id, .. } =
                    &mut response.items[0].cursor
                {
                    *pg_id = PgId::new(4);
                }
            }
            1 => response.items.push(response.items[0].clone()),
            2 => {
                response.items.clear();
                response.complete = false;
            }
            3 => {
                // A valid item is not progress if the audit would install the
                // same resume position and issue this request indefinitely.
                response.complete = false;
                assert_eq!(Some(&response.items[0].cursor), after.as_ref());
            }
            4 => {
                response.complete = false;
                if let ShardScavengerReferenceCursor::StagingWindow { window_index, .. } =
                    &mut response.items[0].cursor
                {
                    *window_index += 1;
                }
            }
            5 => response.items.clear(), // Empty completion still advances the audit to the next PG.
            _ => unreachable!(),
        }
        let expected = response.clone();
        let expected_after = after.clone();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_storage_rpc_frame_from(&mut stream).unwrap();
            assert_eq!(
                request.kind,
                StorageRpcMessageKind::ShardScavengerReferencePage
            );
            let decoded = decode_shard_scavenger_reference_page_request(&request.payload).unwrap();
            assert_eq!(decoded.after, expected_after);
            let payload = encode_shard_scavenger_reference_page_response(&response).unwrap();
            write_storage_rpc_frame_to(
                &mut stream,
                &StorageRpcFrame {
                    request_id: request.request_id,
                    kind: request.kind,
                    payload: encode_storage_rpc_success_response(&payload),
                },
            )
            .unwrap();
        });
        let client =
            UnixStorageNodeClient::new(NodeId::new(7), ClusterEpoch::new(6).unwrap(), socket);
        let route = client
            .open_shard_scavenger_object_scan_route(
                ClusterEpoch::new(6).unwrap(),
                crate::ObjectMetadataScanPgId::new_for_test(PgId::new(3)),
            )
            .unwrap();
        let result =
            route.list_shard_scavenger_reference_page(after.as_ref(), NonZeroU16::new(1).unwrap());
        server.join().unwrap();
        if case < 4 {
            assert!(
                matches!(
                    result,
                    Err(StoreError::StorageRpc {
                        failure: StorageRpcErrorCode::PayloadDecode,
                        ..
                    })
                ),
                "case {case}: {result:?}"
            );
        } else {
            assert_eq!(result.unwrap(), expected, "case {case}");
        }
    }
}
