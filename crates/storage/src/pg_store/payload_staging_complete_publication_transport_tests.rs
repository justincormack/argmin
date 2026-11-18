// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Candidate payload -> admitted store -> exact confirmation. This deliberately
//! does not claim installed Unix/TLS dispatch or owning transport-error custody.

use super::*;
use crate::node_runtime::pg_store::CompletePublicationQuery;
use crate::storage_rpc::{self as rpc, complete_publication as wire, StorageRpcOperationDeadline};

pub(super) fn encode_at(context: &ContextualCommand, cutoff: u64) -> Vec<u8> {
    let encoded = context.encode_command().unwrap();
    wire::encode_request(wire::BorrowedRequest {
        command: rpc::complete_applied::BorrowedRequest {
            node_id: NodeId::new(4),
            epoch: context.id().cluster_epoch(),
            pg_id: context.id().pg_id(),
            checksum: encoded.checksum_crc64(),
            command: encoded.bytes(),
        },
        deadline: StorageRpcOperationDeadline {
            portable_wall_valid_until_ms: cutoff,
        },
    })
    .unwrap()
}

#[test]
fn staging_complete_publication_transport_all_contexts_confirm_exact_marker_and_lost_reply_retry() {
    for case in 0..4 {
        let dir = test_util::tempdir();
        let (server, _, config) = installed_config(&dir, config(case), true);
        let node = server.test_storage_node();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
        let context = context(&node.get_pg(pg_id.get()).unwrap(), case);
        install(&node.get_pg(pg_id.get()).unwrap(), &context);
        let pending = pending(&context);
        let request = pending
            .publication_request(config.node_id, &topology())
            .unwrap();
        let prepared = request.prepare_transport(deadline());
        let bytes = prepared.encode_request().unwrap();
        let query = CompletePublicationQuery::decode(&bytes, &topology()).unwrap();
        let receiver = query.request().unwrap();
        assert_eq!(receiver.id(), request.id());
        assert_eq!(receiver.command_sha256(), request.command_sha256());
        let before = without_marker(&node.get_pg(pg_id.get()).unwrap());
        // The first response is deliberately discarded. Retrying the identical
        // carrier confirms the same marker without changing any owner/log state.
        for attempt in 0..2 {
            let response = server
                .test_with_complete_publication_authority(&receiver, deadline(), |pg, authority| {
                    let response = receiver.start_response(pg, authority).unwrap();
                    assert!(started(pg));
                    assert_eq!(without_marker(pg), before);
                    response
                })
                .unwrap();
            if attempt == 0 {
                continue;
            }
            assert_eq!(
                prepared.decode_confirmation(&response).unwrap().id(),
                context.id()
            );
            for offset in [0, 2, 6, 14, 18, 26] {
                let mut crossed = response.clone();
                crossed[offset] ^= 1;
                assert!(
                    prepared.decode_confirmation(&crossed).is_err(),
                    "case {case}, offset {offset}"
                );
            }
        }
    }
}

#[test]
fn staging_complete_publication_transport_frozen_composite_rejects_malformed_and_current_only_inputs(
) {
    let _clock = crate::clock::test_time_override_guard(1_000);
    let (_dir, _pg, context) = complete_application::setup(5);
    let request = crate::tests::frozen_hex_fixture(include_str!(
        "testdata/complete_publication_payload_v1_command_v14_context_v1.hex"
    ));
    assert_eq!(encode_at(&context, 50_000), request);
    let query = CompletePublicationQuery::decode(&request, &topology()).unwrap();
    let bound = query.request().unwrap();
    assert_eq!(bound.id(), context.id());
    let response = crate::tests::frozen_hex_fixture(include_str!(
        "testdata/complete_publication_confirmation_v1_command_v14_context_v1.hex"
    ));
    assert_eq!(
        wire::encode_confirmation(wire::Confirmation {
            node_id: bound.node_id(),
            id: bound.id(),
            command_sha256: bound.command_sha256()
        }),
        response
    );
    let pending = pending(&context);
    let sender = pending
        .publication_request(NodeId::new(4), &topology())
        .unwrap();
    let prepared = sender.prepare_transport(deadline());
    assert_eq!(
        prepared.decode_confirmation(&response).unwrap().id(),
        context.id()
    );
    assert!(
        rpc::decode_metadata_command_request(
            &request,
            &crate::node_runtime::MetadataCommandDecodeAuthority::new()
        )
        .is_err(),
        "live publication decoder remains unchanged"
    );
    let predecessor = crate::tests::frozen_hex_fixture(include_str!(
        "testdata/complete_applied_rpc_v37_command_v14_context_v1.hex"
    ));
    assert!(
        CompletePublicationQuery::decode(&predecessor, &topology()).is_err(),
        "no missing-deadline fallback"
    );
    for length in 0..request.len() {
        assert!(
            CompletePublicationQuery::decode(&request[..length], &topology()).is_err(),
            "length {length}"
        );
    }
    let mut cases = Vec::new();
    for offset in [0, 14, 22, 26] {
        let mut bytes = request.clone();
        bytes[offset] ^= 1;
        cases.push(bytes);
    }
    let mut trailing = request.clone();
    trailing.push(0);
    cases.push(trailing);
    // Correctly checksummed retired command and unsupported context versions,
    // not merely corrupt frames. Complete payload starts at byte 38.
    for (offset, value) in [(65, 13), (request.len() - 3, 99)] {
        let mut bytes = request.clone();
        bytes[offset] = value;
        let checksum = checksum::crc64::checksum(&bytes[38..]);
        bytes[26..34].copy_from_slice(&checksum.to_le_bytes());
        cases.push(bytes);
    }
    for bytes in cases {
        assert!(CompletePublicationQuery::decode(&bytes, &topology()).is_err());
    }
    assert!(
        CompletePublicationQuery::decode(&request, &PgTopology::new(&[0, 1]).unwrap()).is_err()
    );
}

#[test]
fn staging_complete_publication_transport_freezes_cutoff_and_binds_receiver_deadline_once() {
    let clock = crate::clock::test_time_override_guard(50_000);
    let (_dir, _pg, context) = complete_application::setup(5);
    let pending = pending(&context);
    let request = pending
        .publication_request(NodeId::new(4), &topology())
        .unwrap();
    let sender_deadline = Instant::now() + Duration::from_secs(60);
    let prepared = request.prepare_transport(sender_deadline);
    let bytes = prepared.encode_request().unwrap();
    let cutoff = wire::decode_request(&bytes)
        .unwrap()
        .deadline
        .portable_wall_valid_until_ms;
    assert!(cutoff <= 110_000 - crate::control_plane_lease::CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS);
    assert!(cutoff > 50_000);
    clock.set(60_000);
    assert_eq!(
        prepared.encode_request().unwrap(),
        bytes,
        "retry must not refresh wire authority"
    );
    clock.set(cutoff - 5_000);
    let before = Instant::now();
    let query = CompletePublicationQuery::decode(&bytes, &topology()).unwrap();
    let after = Instant::now();
    let receiver = query.request().unwrap();
    let captured = receiver.bound_deadline(sender_deadline);
    // With a frozen wall clock, the five seconds starts at the receiver's own
    // monotonic sample, which lies between these two observations.
    assert!(captured >= before + Duration::from_secs(5));
    assert!(captured <= after + Duration::from_secs(5));
    for wall in [1, cutoff + 10_000] {
        clock.set(wall);
        assert_eq!(
            query.request().unwrap().bound_deadline(sender_deadline),
            captured
        );
        let forwarded = query
            .request()
            .unwrap()
            .prepare_transport(sender_deadline)
            .encode_request()
            .unwrap();
        assert!(
            wire::decode_request(&forwarded)
                .unwrap()
                .deadline
                .portable_wall_valid_until_ms
                <= cutoff,
            "forwarding after a wall-clock change must not renew original wire authority"
        );
    }
    let earlier = Instant::now();
    assert_eq!(
        receiver.bound_deadline(earlier),
        earlier,
        "owner may only shorten"
    );
    assert!(request.prepare_transport(earlier).encode_request().is_err());
}

#[test]
fn staging_complete_publication_repreparation_cannot_renew_holder_deadline() {
    let clock = crate::clock::test_time_override_guard(50_000);
    let (_dir, _pg, context) = complete_application::setup(5);
    let pending = pending(&context);
    let request = pending
        .publication_request(NodeId::new(4), &topology())
        .unwrap();
    let first_deadline = Instant::now() + Duration::from_secs(60);
    let first = request
        .prepare_transport(first_deadline)
        .encode_request()
        .unwrap();
    assert!(
        wire::decode_request(&first)
            .unwrap()
            .deadline
            .portable_wall_valid_until_ms
            > 50_000
    );

    // Exercise a new preparation, not a second encode of the same transport.
    // Both advancing and reversing wall time must preserve the first projection.
    for wall in [60_000, 1] {
        clock.set(wall);
        let later = first_deadline + Duration::from_secs(300);
        assert_eq!(
            request.prepare_transport(later).encode_request().unwrap(),
            first,
            "repreparing the same request renewed publication authority"
        );
        // Dropping request/transport views cannot reset the owning evidence.
        let reminted = pending
            .publication_request(NodeId::new(4), &topology())
            .unwrap();
        assert_eq!(reminted.bound_deadline(later), first_deadline);
        assert_eq!(
            reminted.prepare_transport(later).encode_request().unwrap(),
            first
        );
    }

    let shorter = first_deadline - Duration::from_secs(10);
    let shortened = request.prepare_transport(shorter).encode_request().unwrap();
    assert!(
        wire::decode_request(&shortened)
            .unwrap()
            .deadline
            .portable_wall_valid_until_ms
            <= wire::decode_request(&first)
                .unwrap()
                .deadline
                .portable_wall_valid_until_ms
    );
    assert_eq!(request.bound_deadline(shorter), shorter);
}

#[test]
fn staging_complete_publication_expired_holder_cannot_be_reprepared() {
    let clock = crate::clock::test_time_override_guard(50_000);
    let (_dir, _pg, context) = complete_application::setup(5);
    let pending = pending(&context);
    let expired = Instant::now();
    {
        let request = pending
            .publication_request(NodeId::new(4), &topology())
            .unwrap();
        assert!(request.prepare_transport(expired).encode_request().is_err());
    }
    clock.set(60_000);
    let request = pending
        .publication_request(NodeId::new(4), &topology())
        .unwrap();
    let later = Instant::now() + Duration::from_secs(60);
    assert_eq!(request.bound_deadline(later), expired);
    let error = request
        .prepare_transport(later)
        .encode_request()
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("encode complete publication request"),
        "{error}"
    );
}

#[test]
fn staging_complete_publication_transport_expiry_cannot_be_replaced_by_owner_deadline() {
    for in_transaction in [false, true] {
        let clock = crate::clock::test_time_override_guard(10_000);
        let dir = test_util::tempdir();
        let (server, _, _) = installed_config(&dir, config(0), true);
        let node = server.test_storage_node();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
        let context = context(&node.get_pg(pg_id.get()).unwrap(), 0);
        install(&node.get_pg(pg_id.get()).unwrap(), &context);
        let bytes = encode_at(&context, if in_transaction { 11_000 } else { 9_000 });
        let query = CompletePublicationQuery::decode(&bytes, &topology()).unwrap();
        let receiver = query.request().unwrap();
        let later_owner_deadline = Instant::now() + Duration::from_secs(60);
        let captured = receiver.bound_deadline(later_owner_deadline);
        let before = snapshot(&node.get_pg(pg_id.get()).unwrap());
        // Renew the raw wall view after rebinding: this may not extend the
        // retained receiver cutoff at admission or at the SQL effect boundary.
        clock.set(1);
        let result = server.test_with_complete_publication_authority(
            &receiver,
            later_owner_deadline,
            |pg, authority| {
                assert!(
                    in_transaction,
                    "already expired wire cutoff admitted authority"
                );
                let mut reached = false;
                let error = receiver
                    .test_start_with_hook(pg, authority, |stage| {
                        if stage == "before-marker" {
                            reached = true;
                            while let Some(remaining) =
                                captured.checked_duration_since(Instant::now())
                            {
                                std::thread::sleep(remaining);
                            }
                        }
                        Ok(())
                    })
                    .unwrap_err();
                assert!(reached);
                assert!(
                    error.to_string().contains("complete publication start"),
                    "{error}"
                );
            },
        );
        assert_eq!(result.is_ok(), in_transaction);
        let pg = node.get_pg(pg_id.get()).unwrap();
        assert_eq!(snapshot(&pg), before);
        assert!(!started(&pg));
    }
}

#[test]
fn staging_complete_publication_transport_stricter_request_deadline_shortens_existing_authority() {
    let _clock = crate::clock::test_time_override_guard(10_000);
    let dir = test_util::tempdir();
    let (server, _, config) = installed_config(&dir, config(0), true);
    let node = server.test_storage_node();
    let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
    let context = context(&node.get_pg(pg_id.get()).unwrap(), 0);
    install(&node.get_pg(pg_id.get()).unwrap(), &context);
    let pending = pending(&context);
    let local = pending
        .publication_request(config.node_id, &topology())
        .unwrap();
    let query = CompletePublicationQuery::decode(&encode_at(&context, 9_000), &topology()).unwrap();
    let expired = query.request().unwrap();
    assert_eq!(expired.id(), local.id());
    assert_eq!(expired.command_sha256(), local.command_sha256());
    server
        .test_with_complete_publication_authority(&local, deadline(), |pg, authority| {
            let before = snapshot(pg);
            let writes = pg.conn.total_changes();
            let error = expired.start_response(pg, authority).unwrap_err();
            assert!(
                error.to_string().contains("complete publication start"),
                "{error}"
            );
            assert_eq!(snapshot(pg), before);
            assert_eq!(pg.conn.total_changes(), writes);
            local.start_response(pg, authority).unwrap();
            assert!(started(pg));
        })
        .unwrap();
}

#[test]
fn staging_complete_publication_transport_rejects_crossed_complete_command_before_marker() {
    let dir = test_util::tempdir();
    let (server, _, config) = installed_config(&dir, config(0), true);
    let node = server.test_storage_node();
    let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
    let context = context(&node.get_pg(pg_id.get()).unwrap(), 0);
    install(&node.get_pg(pg_id.get()).unwrap(), &context);
    let crossed = crate::metadata_command::MetadataCommandEnvelope::new(
        context.id(),
        crate::metadata_command::MetadataCommandPayload::ReserveObjectVersion(
            crate::metadata_command::ReserveObjectVersionCommand::new(
                fixture(StagingTarget::DirectPut).subject.bucket,
                fixture(StagingTarget::DirectPut).subject.key,
                crate::VersionId::from_u64(99),
            ),
        ),
    );
    let crossed = ContextualCommand::decode_field(crossed, &[1, 0, 0], &topology()).unwrap();
    let cutoff = crate::clock::current_time_millis() + 60_000;
    let query =
        CompletePublicationQuery::decode(&encode_at(&crossed, cutoff), &topology()).unwrap();
    let receiver = query.request().unwrap();
    let before = snapshot(&node.get_pg(pg_id.get()).unwrap());
    server
        .test_with_complete_publication_authority(&receiver, deadline(), |pg, authority| {
            let writes = pg.conn.total_changes();
            assert!(receiver.start_response(pg, authority).is_err());
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(snapshot(pg), before);
            assert!(!started(pg));
        })
        .unwrap();
    // A same-ID response with another complete-command digest must not resolve
    // the sender's marker observation, independently of server-side rejection.
    let pending = pending(&context);
    let request = pending
        .publication_request(config.node_id, &topology())
        .unwrap();
    assert!(request
        .prepare_transport(deadline())
        .decode_confirmation(&wire::encode_confirmation(wire::Confirmation {
            node_id: config.node_id,
            id: context.id(),
            command_sha256: receiver.command_sha256(),
        }))
        .is_err());
}
