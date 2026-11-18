// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Receiver-only evidence; receipt claims are not a substitute for the owning
//! sender's physical custody or committed-encryption capability.
use super::*;
use crate::storage_rpc::complete_direct_install as install;

fn prepared(session: &UnixStorageNodeMetadataCommandSession, bytes: &[u8]) -> Vec<u8> {
    let response = session
        .test_direct_prepare_request(bytes.to_vec())
        .unwrap()
        .unwrap();
    let preparation = prepare::encode_request(&prepare::decode_request(bytes).unwrap()).unwrap();
    let command = preparation.decode_response(&response).unwrap();
    install::encode_request(install::Request {
        preparation: bytes,
        command,
    })
    .unwrap()
}

fn crossed_timestamp(request: &[u8]) -> Vec<u8> {
    let original = install::decode_request(request).unwrap();
    let crossed = ContextualCommand::decode_command(
        original.command.command,
        original.command.checksum,
        &topology(),
    )
    .unwrap()
    .test_cross_direct_terminal_reply("timestamp");
    let encoded = crossed.encode_command().unwrap();
    install::encode_request(install::Request {
        preparation: original.preparation,
        command: rpc::complete_applied::BorrowedRequest {
            node_id: original.command.node_id,
            epoch: original.command.epoch,
            pg_id: original.command.pg_id,
            checksum: encoded.checksum_crc64(),
            command: encoded.bytes(),
        },
    })
    .unwrap()
}

fn seed_empty_predecessor(f: &Mixed) {
    // A metadata-only, payloadless predecessor with its exact published owner.
    // It produces a real displaced reclaim root without fabricated shard data.
    let current = owner(f);
    let mut old = fixture(StagingTarget::StreamPut);
    old.subject.payload_generation =
        GenerationId::new(current.subject.payload_generation.get() + 1).unwrap();
    old.subject.originating_epoch = ClusterEpoch::new(4).unwrap();
    old.subject.id = StagingOwnerId {
        attempt: crate::tests::stream_session_id("prior-terminal"),
        introduction: MetadataCommandId::new(
            old.subject.originating_epoch,
            old.subject.object_pg,
            crate::metadata_command::MetadataCommandLogIndex::new(1).unwrap(),
        ),
    };
    old.windows = vec![window(&old.subject, 0, old.subject.first_vid())];
    old.reserved_end = old.windows[0].end;
    old.maximum_cutoff_ms = old.windows[0].cutoff_ms;
    old.resources.generation = ResourceResponsibility::Live(StagingGenerationResource {
        reservation_id: old.subject.id.attempt.clone(),
        generation: old.subject.payload_generation,
    });
    old.resources.creation = ResourceResponsibility::Live(StagingBucketResource::capture(
        &proof(&old.subject, old.subject.creation_operation()),
        PgId::new(topology().bucket_pg_for(&old.subject.bucket)),
    ));
    let old = old.published_standard(&[]).unwrap();
    let topology = topology();
    let checked = old.validate(&topology).unwrap();
    let clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    for n in 0..5 {
        let node = f.node(NodeId::new(n));
        let pg = node.get_pg(old.subject.object_pg.get()).unwrap();
        pg.put_object_with_segments(
            &crate::PutLiveObjectReq {
                bucket: old.subject.bucket.clone(),
                key: old.subject.key.clone(),
                version_id: crate::VersionId::Null,
                owner: crate::OwnerIdentity::from_principal("prior-owner"),
                acl_grants: Default::default(),
                public_read: false,
                generation_id: old.subject.payload_generation,
                size: 0,
                etag: crate::ObjectEtag::single_part(42),
                ec: old.windows[0].geometry.ec,
                layout: crate::ObjectLayout::Standard,
                tags: None,
                metadata_blob: None,
                system_metadata_blob: None,
                object_lock: Default::default(),
                encryption: crate::tests::metadata_only_test_encryption(),
            },
            &[],
        )
        .unwrap();
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        // Raw metadata seeding is not an allocator: supply the predecessor's
        // durable non-recycling fence as part of this complete owner fixture.
        pg.conn.execute(
            "UPDATE object_write_counters SET max_committed_generation = max(max_committed_generation, ?3) WHERE bucket = ?1 AND key = ?2",
            rusqlite::params![old.subject.bucket, old.subject.key, old.subject.payload_generation.get()],
        ).unwrap();
        sql::insert(&pg, old.subject.object_pg, &topology, &checked).unwrap();
        pg.conn.execute_batch("COMMIT").unwrap();
        complete_application::anchor(&pg, f.context.id());
        pg.recover_complete_command_state(
            PgStoreRecoveryContext::for_node(NodeId::new(n)),
            &topology,
        )
        .unwrap();
    }
    drop(clock);
}

#[test]
fn staging_remote_direct_install_receiver_rejects_resealed_timestamps_with_and_without_displacement(
) {
    for tls in [false, true] {
        for displaced in [false, true] {
            with_request(tls, |f, session, bytes| {
                if displaced {
                    seed_empty_predecessor(f);
                }
                let preparation = prepare::encode_request(&request(f, session, bytes)).unwrap();
                let request = prepared(session, preparation.bytes());
                let crossed = crossed_timestamp(&request);
                let original = install::decode_request(&request).unwrap();
                let context = ContextualCommand::decode_command(
                    original.command.command,
                    original.command.checksum,
                    &topology(),
                )
                .unwrap();
                context.test_assert_direct_displacement(displaced);
                let before = f.observe();
                let error = session
                    .test_direct_install_request(crossed.clone())
                    .unwrap()
                    .unwrap_err();
                assert!(
                    error.message.contains("receiver-minted preparation"),
                    "{error:?}"
                );
                assert_eq!(f.observe(), before);
                let reply = session
                    .test_direct_install_request(request.clone())
                    .unwrap()
                    .unwrap();
                install::require_confirmation(&request, &reply).unwrap();
                let installed = f.observe();
                assert!(session
                    .test_direct_install_request(crossed)
                    .unwrap()
                    .is_err());
                assert_eq!(f.observe(), installed);
                let reply = session
                    .test_direct_install_request(request.clone())
                    .unwrap()
                    .unwrap();
                install::require_confirmation(&request, &reply).unwrap();
                assert_eq!(f.observe(), installed);
            });
        }
    }
}

#[test]
fn staging_remote_direct_install_receiver_fresh_install_requires_original_preparation_section() {
    for tls in [false, true] {
        with_request(tls, |f, session, bytes| {
            let preparation = prepare::encode_request(&request(f, session, bytes)).unwrap();
            let request = prepared(session, preparation.bytes());
            session.test_close_primary_section();
            let reopened = f
                .remote(NodeId::new(4))
                .unwrap()
                .client
                .open_metadata_command_critical_section_until(f.context.id().pg_id(), deadline())
                .unwrap();
            let before = f.observe();
            let error = reopened
                .test_direct_install_request(request)
                .unwrap()
                .unwrap_err();
            assert!(
                error.message.contains("receiver-minted preparation"),
                "{error:?}"
            );
            assert_eq!(f.observe(), before);
            // A rejected pending transaction conservatively invalidates the
            // optional prefix observation. Revalidate before new preparation;
            // this neither repairs state nor confers the old section identity.
            f.node(NodeId::new(4))
                .get_pg(f.context.id().pg_id().get())
                .unwrap()
                .recover_complete_command_state(
                    PgStoreRecoveryContext::for_node(NodeId::new(4)),
                    &topology(),
                )
                .unwrap();
            let request = prepared(&reopened, preparation.bytes());
            let reply = reopened
                .test_direct_install_request(request.clone())
                .unwrap()
                .unwrap();
            install::require_confirmation(&request, &reply).unwrap();
        });
    }
}

#[test]
fn staging_remote_direct_install_receiver_exact_retry_and_damaged_indexes_unix_tls() {
    for tls in [false, true] {
        for damage in ["staging", "routes", "scope-null", "scope-crossed"] {
            with_request(tls, |f, session, bytes| {
                let preparation = prepare::encode_request(&request(f, session, bytes)).unwrap();
                let request = prepared(session, preparation.bytes());
                let before = f.observe();
                let shadows = f.shadows();
                f.remote(NodeId::new(4))
                    .unwrap()
                    .server
                    .test_direct_install_admission(&request);
                assert_eq!(f.observe(), before);
                let reply = session
                    .test_direct_install_request(request.clone())
                    .unwrap()
                    .unwrap();
                install::require_confirmation(&request, &reply).unwrap();
                let after = f.observe();
                assert_ne!(after[4], before[4], "primary slot installed");
                assert_eq!(after[..4], before[..4]);
                assert_eq!(f.shadows(), shadows);
                let node = f.node(NodeId::new(4));
                let id = f.context.id();
                {
                    let pg = node.get_pg(id.pg_id().get()).unwrap();
                    let pending = pg
                        .inspect_complete_pending_command(4, &topology())
                        .unwrap()
                        .unwrap();
                    assert_eq!(pending.id.log_index().get(), id.log_index().get() + 1);
                    assert!(!pending.publication_started);
                }
                let reply = session
                    .test_direct_install_request(request.clone())
                    .unwrap()
                    .unwrap();
                install::require_confirmation(&request, &reply).unwrap();
                assert_eq!(f.observe(), after, "exact replay leaves all rows unchanged");
                node.get_pg(id.pg_id().get())
                    .unwrap()
                    .conn
                    .execute_batch(match damage {
                        "staging" => "DELETE FROM metadata_command_pending_staging_protection",
                        "routes" => "DELETE FROM metadata_command_pending_route_dependencies",
                        "scope-null" => {
                            "UPDATE metadata_command_pending_slot SET scope_bucket = NULL"
                        }
                        "scope-crossed" => {
                            "UPDATE metadata_command_pending_slot SET scope_bucket = 'crossed'"
                        }
                        _ => unreachable!(),
                    })
                    .unwrap();
                let damaged = f.observe();
                assert!(session
                    .test_direct_install_request(request)
                    .unwrap()
                    .is_err());
                assert_eq!(f.observe(), damaged, "exact retry must not repair {damage}");
            });
        }
    }
}

#[test]
fn staging_remote_direct_install_receiver_rejects_crossed_commands_without_mutation() {
    for tls in [false, true] {
        with_request(tls, |f, session, bytes| {
            let preparation = prepare::encode_request(&request(f, session, bytes)).unwrap();
            let request = prepared(session, preparation.bytes());
            let original = install::decode_request(&request).unwrap();
            let before = f.observe();
            for field in [
                "index",
                "future-index",
                "generation",
                "key",
                "owner",
                "etag",
                "encryption",
                "segment",
                "witness",
                "reservation",
                "sequence",
            ] {
                let crossed = ContextualCommand::decode_command(
                    original.command.command,
                    original.command.checksum,
                    &topology(),
                )
                .unwrap()
                .test_cross_direct_terminal_reply(field);
                let encoded = crossed.encode_command().unwrap();
                let request = install::encode_request(install::Request {
                    preparation: preparation.bytes(),
                    command: rpc::complete_applied::BorrowedRequest {
                        node_id: original.command.node_id,
                        epoch: original.command.epoch,
                        pg_id: original.command.pg_id,
                        checksum: encoded.checksum_crc64(),
                        command: encoded.bytes(),
                    },
                })
                .unwrap();
                let error = session
                    .test_direct_install_request(request)
                    .unwrap()
                    .unwrap_err();
                let expected = if field == "index" {
                    "already-applied log index"
                } else {
                    "remote direct installation"
                };
                assert!(error.message.contains(expected), "{field}: {error:?}");
                assert_eq!(f.observe(), before, "{field}");
            }
            let reply = session
                .test_direct_install_request(request.clone())
                .unwrap()
                .unwrap();
            install::require_confirmation(&request, &reply).unwrap();
        });
    }
}

#[test]
fn staging_remote_direct_install_receiver_revalidates_owner_snapshot_and_counter() {
    for tls in [false, true] {
        for installed in [false, true] {
            for change in ["reservation", "window", "sequence", "current", "issuer"] {
                with_request(tls, |f, session, bytes| {
                    let preparation = prepare::encode_request(&request(f, session, bytes)).unwrap();
                    let request = prepared(session, preparation.bytes());
                    if installed {
                        session
                            .test_direct_install_request(request.clone())
                            .unwrap()
                            .unwrap();
                    }
                    let durable_owner = owner(f);
                    let node = f.node(NodeId::new(4));
                    {
                        let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
                        if change == "current" {
                            let generation = crate::GenerationId::new(
                                durable_owner.subject.payload_generation.get() + 1,
                            )
                            .unwrap();
                            pg.put_object_with_segments(
                                &crate::PutLiveObjectReq {
                                    bucket: durable_owner.subject.bucket.clone(),
                                    key: durable_owner.subject.key.clone(),
                                    version_id: crate::VersionId::Null,
                                    owner: crate::OwnerIdentity::from_principal("replacement"),
                                    acl_grants: Default::default(),
                                    public_read: false,
                                    generation_id: generation,
                                    size: 0,
                                    etag: crate::ObjectEtag::single_part(42),
                                    ec: durable_owner.windows[0].geometry.ec,
                                    layout: crate::ObjectLayout::Standard,
                                    tags: None,
                                    metadata_blob: None,
                                    system_metadata_blob: None,
                                    object_lock: Default::default(),
                                    encryption:
                                        crate::tests::committed_metadata_only_test_encryption(
                                            durable_owner.subject.bucket_incarnation,
                                            &durable_owner.subject.bucket,
                                            &durable_owner.subject.key,
                                            generation,
                                        ),
                                },
                                &[],
                            )
                            .unwrap();
                        } else if change == "issuer" {
                            pg.begin_allocation_lifetime_replacement()
                                .unwrap()
                                .complete()
                                .unwrap();
                        } else {
                            pg.conn.execute_batch(match change {
                        "reservation" => "DELETE FROM object_generation_reservations",
                        "window" => "UPDATE payload_staging_windows SET logical_bound = logical_bound - 1",
                        "sequence" => "UPDATE object_write_counters SET next_write_sequence = next_write_sequence + 1",
                        _ => unreachable!(),
                    }).unwrap();
                        }
                    }
                    let before = f.observe();
                    let error = session
                        .test_direct_install_request(request)
                        .unwrap()
                        .unwrap_err();
                    assert!(
                        error.message.contains("remote direct installation"),
                        "{change}: {error:?}"
                    );
                    assert_eq!(f.observe(), before);
                    require_snapshot_finished(f);
                });
            }
        }
    }
}

#[test]
fn staging_remote_direct_install_receiver_fences_both_cutoffs_at_sql_effect() {
    for tls in [false, true] {
        for server_first in [false, true] {
            with_request(tls, |f, session, bytes| {
                let server = &f.remote(NodeId::new(4)).unwrap().server;
                let mut claims = wire::decode_request(bytes).unwrap();
                claims.receiver.deadline.portable_wall_valid_until_ms =
                    crate::clock::current_time_millis() + 10_000;
                let snapshot = wire::encode_request(&claims).unwrap();
                if server_first {
                    server
                        .test_receiver_route_validity(crate::clock::current_time_millis() + 5_000);
                }
                let preparation =
                    prepare::encode_request(&request(f, session, snapshot.bytes())).unwrap();
                server.set_response_frame_test_hook(Arc::new(move |kind, _| {
                    if kind == Kind::StagingDirectPrepare {
                        StorageNodeServer::test_expire_direct_at_install(server_first);
                    }
                }));
                let request = prepared(session, preparation.bytes());
                let before = f.observe();
                let error = session
                    .test_direct_install_request(request)
                    .unwrap()
                    .unwrap_err();
                assert!(
                    error.message.contains("expired") || error.message.contains("cutoff"),
                    "{error:?}"
                );
                assert_eq!(
                    f.observe(),
                    before,
                    "no slot, indexes, log or counter mutation"
                );
            });
        }
    }
}

#[test]
fn staging_remote_direct_install_receiver_lost_success_retains_exact_pending_custody() {
    for tls in [false, true] {
        with_request(tls, |f, session, bytes| {
            let preparation = prepare::encode_request(&request(f, session, bytes)).unwrap();
            let request = prepared(session, preparation.bytes());
            let primary = f.remote(NodeId::new(4)).unwrap();
            primary
                .server
                .set_response_frame_test_hook(Arc::new(|kind, _| {
                    if kind == Kind::StagingDirectInstall {
                        StorageNodeServer::test_lose_direct_snapshot_reply();
                    }
                }));
            assert!(session
                .test_direct_install_request(request.clone())
                .is_err());
            let after = f.observe();
            assert!(f
                .node(NodeId::new(4))
                .get_pg(f.context.id().pg_id().get())
                .unwrap()
                .inspect_complete_pending_command(4, &topology())
                .unwrap()
                .is_some());
            primary
                .server
                .set_response_frame_test_hook(Arc::new(|_, _| {}));
            // Receiver-only exact retry: no permission for the owning sender to
            // reconnect or discard its original session/custody is implied.
            let retry = primary
                .client
                .open_metadata_command_critical_section_until(f.context.id().pg_id(), deadline())
                .unwrap();
            assert!(retry
                .test_direct_install_request(crossed_timestamp(&request))
                .unwrap()
                .is_err());
            assert_eq!(
                f.observe(),
                after,
                "reconnect cannot substitute timestamps in an installed command"
            );
            let reply = retry
                .test_direct_install_request(request.clone())
                .unwrap()
                .unwrap();
            install::require_confirmation(&request, &reply).unwrap();
            assert_eq!(f.observe(), after);
        });
    }
}
