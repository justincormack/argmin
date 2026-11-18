// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Authenticated receiver-only claims. No assertion of physical file/ack effects
//! or sender-owned committed-encryption evidence is made by these fixtures.
use super::*;
use crate::storage_rpc::complete_direct_prepare as prepare;

#[path = "payload_staging_remote_direct_install_receiver_tests.rs"]
mod installation;

fn owner(f: &Mixed) -> StagingOwnerRecord {
    let node = f.node(NodeId::new(4));
    let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
    let read = pg.command_build_snapshot().unwrap();
    let owner = append::load(&pg, &fresh_capture::template(StagingTarget::DirectPut));
    read.finish().unwrap();
    owner
}

fn request<'a>(
    f: &Mixed,
    session: &UnixStorageNodeMetadataCommandSession,
    bytes: &'a [u8],
) -> prepare::Request<'a> {
    let reply = session
        .test_direct_snapshot_request(bytes.to_vec())
        .unwrap()
        .unwrap();
    let snapshot = wire::encode_request(&wire::decode_request(bytes).unwrap()).unwrap();
    let snapshot_sha256 =
        rpc::complete_stream_allocation::digest(snapshot.decode_response(&reply).unwrap());
    let owner = owner(f);
    let encrypted = crate::tests::sse_s3_segment_test_fixture(b"data", 0);
    let subject = crate::ObjectEncryptionSubject::standard_object(
        owner.subject.bucket_incarnation,
        &owner.subject.bucket,
        &owner.subject.key,
        owner.subject.payload_generation,
        &encrypted.encryption,
    );
    prepare::Request {
        snapshot_request: bytes,
        snapshot_sha256,
        segment_crc64: encrypted.storage_crc64,
        provisional: encrypted.encryption.clone(),
        commit: crate::StreamPutCommitInput {
            versioning: crate::BucketVersioningState::Suspended,
            version_id: crate::VersionId::Null,
            owner: crate::OwnerIdentity::from_principal("direct-owner"),
            acl_grants: Default::default(),
            public_read: true,
            etag_crc64: encrypted.payload_crc64,
            tags: None,
            metadata_blob: Default::default(),
            system_metadata_blob: Default::default(),
            object_lock: Default::default(),
            encryption: encrypted.committed_encryption(&subject),
        },
    }
}

fn require_snapshot_finished(f: &Mixed) {
    assert!(f
        .node(NodeId::new(4))
        .get_pg(f.context.id().pg_id().get())
        .unwrap()
        .conn
        .is_autocommit());
}

#[test]
fn staging_remote_direct_prepare_receiver_derives_terminal_without_effects_unix_tls() {
    for tls in [false, true] {
        with_request(tls, |f, session, bytes| {
            let mut request = request(f, session, bytes);
            for versioning in [
                crate::BucketVersioningState::Disabled,
                crate::BucketVersioningState::Suspended,
                crate::BucketVersioningState::Enabled,
            ] {
                request.commit.versioning = versioning;
                request.commit.version_id = if versioning == crate::BucketVersioningState::Enabled {
                    crate::VersionId::from_u64(9)
                } else {
                    crate::VersionId::Null
                };
                let encoded = prepare::encode_request(&request).unwrap();
                f.remote(NodeId::new(4))
                    .unwrap()
                    .server
                    .test_direct_prepare_admission(encoded.bytes());
                let before = f.observe();
                let shadows = f.shadows();
                for _ in 0..2 {
                    let reply = session
                        .test_direct_prepare_request(encoded.bytes().to_vec())
                        .unwrap()
                        .unwrap();
                    let reply = encoded.decode_response(&reply).unwrap();
                    let context = ContextualCommand::decode_command(
                        reply.command,
                        reply.checksum,
                        &topology(),
                    )
                    .unwrap();
                    assert_eq!(context.id().pg_id(), f.context.id().pg_id());
                    assert_eq!(context.id().cluster_epoch(), f.context.id().cluster_epoch());
                    assert_eq!(
                        context.id().log_index().get(),
                        f.context.id().log_index().get() + 1
                    );
                    context.assert_remote_direct_terminal_for_test(
                        &request.commit,
                        &owner(f),
                        request.segment_crc64,
                    );
                    assert_eq!(f.observe(), before);
                    assert_eq!(f.shadows(), shadows);
                    require_snapshot_finished(f);
                }
            }
        });
    }
}

#[test]
fn staging_remote_direct_prepare_receiver_rejects_crossed_snapshot_crc_lineage_and_receipts() {
    for tls in [false, true] {
        with_request(tls, |f, session, bytes| {
            let before = f.observe();
            for case in ["snapshot", "crc", "lineage", "receipts"] {
                let mut request = request(f, session, bytes);
                let mut snapshot = wire::decode_request(bytes).unwrap();
                if case == "receipts" {
                    snapshot.receipts.pop();
                }
                let snapshot = wire::encode_request(&snapshot).unwrap();
                request.snapshot_request = snapshot.bytes();
                match case {
                    "snapshot" => request.snapshot_sha256[0] ^= 1,
                    "crc" => request.segment_crc64 ^= 1,
                    "lineage" => {
                        let crate::ObjectEncryption::SseS3(state) = &request.provisional else {
                            unreachable!()
                        };
                        let mut prefix = *state.segment_nonce_prefix();
                        prefix[0] ^= 1;
                        // Same instance/wrap nonce, different immutable prefix.
                        request.provisional =
                            crate::ObjectEncryption::SseS3(crate::SseS3ObjectState::new(
                                state.wrapping_key_id(),
                                *state.wrap_nonce(),
                                *state.wrapped_dek(),
                                prefix,
                            ));
                    }
                    "receipts" => (),
                    _ => unreachable!(),
                }
                let encoded = prepare::encode_request(&request).unwrap();
                let error = session
                    .test_direct_prepare_request(encoded.bytes().to_vec())
                    .unwrap()
                    .unwrap_err();
                assert_eq!(
                    error.code,
                    Code::MetadataCommandContention,
                    "{case}: {error:?}"
                );
                let expected = match case {
                    "snapshot" => "snapshot changed",
                    "crc" => "integrity",
                    "lineage" => "encryption lineage",
                    "receipts" => "acknowledgement",
                    _ => unreachable!(),
                };
                assert!(error.message.contains(expected), "{case}: {error:?}");
                assert_eq!(f.observe(), before);
                require_snapshot_finished(f);
            }
        });
    }
}

#[test]
fn staging_remote_direct_prepare_receiver_revalidates_durable_owner_before_response() {
    for tls in [false, true] {
        for case in ["reservation", "window", "issuer", "current"] {
            with_request(tls, |f, session, bytes| {
                let request = prepare::encode_request(&request(f, session, bytes)).unwrap();
                let durable_owner = owner(f);
                let node = f.node(NodeId::new(4));
                {
                    let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
                    match case {
                        "reservation" => {
                            pg.conn
                                .execute_batch("DELETE FROM object_generation_reservations")
                                .unwrap();
                        }
                        "window" => {
                            pg.conn.execute_batch("UPDATE payload_staging_windows SET logical_bound = logical_bound - 1").unwrap();
                        }
                        "issuer" => {
                            pg.begin_allocation_lifetime_replacement()
                                .unwrap()
                                .complete()
                                .unwrap();
                            pg.recover_complete_command_state(
                                PgStoreRecoveryContext::for_node(NodeId::new(4)),
                                &topology(),
                            )
                            .unwrap();
                        }
                        "current" => {
                            let owner = &durable_owner;
                            let replacement_generation = crate::GenerationId::new(
                                owner.subject.payload_generation.get() + 1,
                            )
                            .unwrap();
                            pg.put_object_with_segments(
                                &crate::PutLiveObjectReq {
                                    bucket: owner.subject.bucket.clone(),
                                    key: owner.subject.key.clone(),
                                    version_id: crate::VersionId::Null,
                                    owner: crate::OwnerIdentity::from_principal("old"),
                                    acl_grants: Default::default(),
                                    public_read: false,
                                    generation_id: replacement_generation,
                                    size: 0,
                                    etag: crate::ObjectEtag::single_part(42),
                                    ec: owner.windows[0].geometry.ec,
                                    layout: crate::ObjectLayout::Standard,
                                    tags: None,
                                    metadata_blob: None,
                                    system_metadata_blob: None,
                                    object_lock: Default::default(),
                                    encryption:
                                        crate::tests::committed_metadata_only_test_encryption(
                                            owner.subject.bucket_incarnation,
                                            &owner.subject.bucket,
                                            &owner.subject.key,
                                            replacement_generation,
                                        ),
                                },
                                &[],
                            )
                            .unwrap();
                            complete_application::anchor(&pg, f.context.id());
                            pg.recover_complete_command_state(
                                PgStoreRecoveryContext::for_node(NodeId::new(4)),
                                &topology(),
                            )
                            .unwrap();
                        }
                        _ => unreachable!(),
                    }
                }
                let before = f.observe();
                let error = session
                    .test_direct_prepare_request(request.bytes().to_vec())
                    .unwrap()
                    .unwrap_err();
                assert_eq!(
                    error.code,
                    Code::MetadataCommandContention,
                    "{case}: {error:?}"
                );
                if case == "current" {
                    assert!(error.message.contains("snapshot changed"), "{error:?}");
                }
                assert_eq!(f.observe(), before);
                require_snapshot_finished(f);
            });
        }
    }
}

#[test]
fn staging_remote_direct_prepare_receiver_checks_both_cutoffs_after_snapshot_teardown() {
    for tls in [false, true] {
        for server_route in [false, true] {
            with_request(tls, |f, session, bytes| {
                let request = prepare::encode_request(&request(f, session, bytes)).unwrap();
                let server = &f.remote(NodeId::new(4)).unwrap().server;
                server.set_response_frame_test_hook(Arc::new(move |kind, _| {
                    if kind == Kind::StagingDirectPrepare {
                        StorageNodeServer::test_expire_direct_snapshot_after_capture(server_route);
                    }
                }));
                let before = f.observe();
                session
                    .test_direct_prepare_request(request.bytes().to_vec())
                    .unwrap()
                    .unwrap();
                let error = session
                    .test_direct_prepare_request(request.bytes().to_vec())
                    .unwrap()
                    .unwrap_err();
                assert_eq!(
                    error.code,
                    if server_route {
                        Code::StaleShardLocation
                    } else {
                        Code::TransportTimeout
                    },
                    "{error:?}"
                );
                assert_eq!(f.observe(), before);
                require_snapshot_finished(f);
            });
        }
    }
}
