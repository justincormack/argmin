// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Real file and durable acknowledgement custody through a consuming read-only
//! handoff. Bad replies must not erase that custody or renew its authority.
use super::*;

macro_rules! ready {
    ($f:expr, $writer:ident, $owner:expr, $shards:expr) => {{
        write_all($f, &mut $writer, $owner, $shards);
        $writer
            .persist(
                &$f.remote($owner.windows[0].participants.acknowledgement_primary)
                    .unwrap()
                    .client,
            )
            .unwrap();
        let Writer::Direct(writer) = $writer else {
            unreachable!()
        };
        writer
            .into_acknowledged()
            .unwrap_or_else(|e| panic!("{}", e.error()))
    }};
}

#[path = "payload_staging_remote_direct_terminal_sender_tests.rs"]
mod terminal;

#[test]
fn staging_remote_direct_snapshot_sender_preserves_all_origins_and_physical_custody() {
    for tls in [false, true] {
        for origin in ["installed", "inspected", "uncertain"] {
            with_writer(
                tls,
                StagingTarget::DirectPut,
                false,
                origin,
                |f, mut writer, owner, shards| {
                    let mut ready = ready!(f, writer, owner, shards);
                    let history = ready.test_binding_state();
                    let id = ready.id();
                    let client = &f.remote(NodeId::new(4)).unwrap().client;
                    let session = ready.open_direct_primary_session(client).unwrap();
                    assert!(ready.open_direct_primary_session(client).is_err());
                    let before = f.observe();
                    let prepared = ready
                        .prepare_direct_remote(client, &session)
                        .unwrap_or_else(|e| panic!("{}", e.error()));
                    assert_eq!(prepared.id(), id);
                    assert_eq!(prepared.test_binding_state(), history);
                    assert!(prepared.auth_snapshot().existing_etag.is_none());
                    drop(prepared);
                    assert_eq!(f.observe(), before);
                    require_payload(f, owner, shards, true);
                },
            );
        }
    }
}

fn predecessor(owner: &StagingOwnerRecord) -> crate::StoredObject {
    use crate::*;
    StoredObject::Live(LiveObjectRecord {
        bucket: owner.subject.bucket.clone(),
        key: owner.subject.key.clone(),
        version_id: VersionId::Null,
        owner: OwnerIdentity::from_principal("old-owner"),
        acl_grants: AclGrants::default(),
        public_read: false,
        generation_id: GenerationId::MIN,
        size: 0,
        etag: ObjectEtag::single_part(42),
        last_modified: 100,
        became_noncurrent_at: None,
        storage_class: StorageClass::Standard,
        ec: owner.windows[0].geometry.ec,
        layout: ObjectLayout::Standard,
        tags: None,
        metadata_blob: None,
        system_metadata_blob: None,
        object_lock: ObjectLockState::default(),
        encryption: crate::tests::committed_metadata_only_test_encryption(
            1,
            &owner.subject.bucket,
            &owner.subject.key,
            GenerationId::MIN,
        ),
    })
}

#[test]
fn staging_remote_direct_snapshot_sender_rejects_crossed_replies_and_keeps_exact_retry() {
    for tls in [false, true] {
        with_writer(
            tls,
            StagingTarget::DirectPut,
            false,
            "uncertain",
            |f, mut writer, owner, shards| {
                let mut ready = ready!(f, writer, owner, shards);
                let history = ready.test_binding_state();
                let primary = f.remote(NodeId::new(4)).unwrap();
                let session = ready.open_direct_primary_session(&primary.client).unwrap();
                let before = f.observe();
                let correlation = Arc::new(std::sync::Mutex::new(None::<Vec<u8>>));
                for field in [
                    "correlation",
                    "etag",
                    "bucket",
                    "key",
                    "source",
                    "committed",
                    "generation",
                ] {
                    let correlation = Arc::clone(&correlation);
                    let mut record = predecessor(owner);
                    if field == "generation" {
                        let crate::StoredObject::Live(live) = &mut record else {
                            unreachable!()
                        };
                        live.generation_id = owner.subject.payload_generation;
                    }
                    primary.server.set_response_frame_test_hook(Arc::new(move |kind, frame| {
                    if kind != Kind::StagingDirectSnapshot { return; }
                    let reusable = rpc::decode_storage_rpc_response_payload_with_connection_disposition(&frame.payload).unwrap().connection_reusable;
                    let mut bytes = rpc::decode_storage_rpc_response_payload(&frame.payload).unwrap().unwrap();
                    let mut previous = correlation.lock().unwrap();
                    if let Some(previous) = previous.as_ref() { assert_eq!(&bytes[6..38], previous); }
                    else { *previous = Some(bytes[6..38].to_vec()); }
                    if field == "correlation" { bytes[6] ^= 1; }
                    else {
                        let mut snapshot = rpc::decode_direct_put_commit_snapshot_response(&bytes[42..]).unwrap();
                        if field == "etag" { snapshot.snapshot.auth_snapshot.existing_etag = Some("forged".into()); }
                        else {
                            let mut record = record.clone();
                            let crate::StoredObject::Live(live) = &mut record else { unreachable!() };
                            match field {
                                "bucket" => live.bucket = crate::tests::bucket_name("foreign-snapshot"),
                                "key" | "source" => live.key = crate::tests::object_key("foreign-snapshot"),
                                "committed" => snapshot.snapshot.committed_segments = Some(vec![]),
                                "generation" => {},
                                _ => unreachable!(),
                            }
                            if field == "source" { snapshot.snapshot.stale_payload_source = Some(record); }
                            else {
                                snapshot.snapshot.auth_snapshot.existing_etag = Some(live.etag.format());
                                snapshot.snapshot.current = Some(record);
                            }
                        }
                        let nested = rpc::encode_direct_put_commit_snapshot_response(&snapshot);
                        bytes.truncate(38);
                        bytes.extend_from_slice(&(nested.len() as u32).to_le_bytes());
                        bytes.extend_from_slice(&nested);
                    }
                    frame.payload = rpc::encode_storage_rpc_success_response(&bytes);
                    rpc::set_storage_rpc_response_connection_reusable(&mut frame.payload, reusable).unwrap();
                }));
                    let failure = ready
                        .prepare_direct_remote(&primary.client, &session)
                        .err()
                        .expect("crossed reply");
                    let expected = match field {
                        "correlation" => "acknowledged direct payload snapshot",
                        "etag" => "auth snapshot etag does not match current object",
                        "bucket" | "key" => "current object identity does not match request",
                        "source" => "stale payload source identity does not match request",
                        "committed" | "generation" => {
                            "active owner returned an already-committed generation"
                        }
                        _ => unreachable!(),
                    };
                    let diagnostic = match failure.error() {
                        Error::Store(StoreError::StorageRpc {
                            failure, detail, ..
                        }) => {
                            assert_eq!(*failure, rpc::StorageRpcErrorCode::PayloadDecode);
                            detail.as_str().to_string()
                        }
                        error => error.to_string(),
                    };
                    assert!(diagnostic.contains(expected), "{field}: {diagnostic}");
                    ready = failure.into_acknowledged();
                    assert_eq!(ready.test_binding_state(), history);
                    assert_eq!(f.observe(), before, "{field}");
                    require_payload(f, owner, shards, true);
                }
                primary
                    .server
                    .set_response_frame_test_hook(Arc::new(|_, _| {}));
                let prepared = ready
                    .prepare_direct_remote(&primary.client, &session)
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                assert_eq!(prepared.test_binding_state(), history);
                assert_eq!(f.observe(), before);
                require_payload(f, owner, shards, true);
            },
        );
    }
}

#[test]
fn staging_remote_direct_snapshot_sender_rejects_foreign_reopened_and_expired_sections() {
    for tls in [false, true] {
        with_writer(
            tls,
            StagingTarget::DirectPut,
            false,
            "installed",
            |f, mut writer, owner, shards| {
                let mut ready = ready!(f, writer, owner, shards);
                let history = ready.test_binding_state();
                let primary = f.remote(NodeId::new(4)).unwrap();
                let session = ready.open_direct_primary_session(&primary.client).unwrap();
                let before = f.observe();
                let count = Arc::new(AtomicUsize::new(0));
                let calls = Arc::clone(&count);
                primary
                    .server
                    .set_response_frame_test_hook(Arc::new(move |kind, _| {
                        if kind == Kind::StagingDirectSnapshot {
                            calls.fetch_add(1, Ordering::SeqCst);
                        }
                    }));
                let failure = ready
                    .prepare_direct_remote(&f.remote(NodeId::new(0)).unwrap().client, &session)
                    .err()
                    .unwrap();
                ready = failure.into_acknowledged();
                let failure = crate::clock::with_time_override(
                    crate::clock::current_time_millis() + 1_000_000,
                    || {
                        ready
                            .prepare_direct_remote(&primary.client, &session)
                            .err()
                            .unwrap()
                    },
                );
                ready = failure.into_acknowledged();
                drop(session);
                assert!(ready.open_direct_primary_session(&primary.client).is_err());
                let replacement = primary
                    .client
                    .open_metadata_command_critical_section_until(
                        f.context.id().pg_id(),
                        deadline(),
                    )
                    .unwrap();
                let failure = ready
                    .prepare_direct_remote(&primary.client, &replacement)
                    .err()
                    .expect("reopened section");
                let ready = failure.into_acknowledged();
                assert_eq!(count.load(Ordering::SeqCst), 0);
                assert_eq!(ready.test_binding_state(), history);
                assert_eq!(f.observe(), before);
                require_payload(f, owner, shards, true);
            },
        );
    }
}

#[test]
fn staging_remote_direct_snapshot_sender_lost_or_closed_reply_keeps_custody() {
    for tls in [false, true] {
        for lost in [false, true] {
            with_writer(
                tls,
                StagingTarget::DirectPut,
                false,
                "installed",
                |f, mut writer, owner, shards| {
                    let mut ready = ready!(f, writer, owner, shards);
                    let history = ready.test_binding_state();
                    let primary = f.remote(NodeId::new(4)).unwrap();
                    let session = ready.open_direct_primary_session(&primary.client).unwrap();
                    let before = f.observe();
                    primary
                        .server
                        .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                            if kind == Kind::StagingDirectSnapshot {
                                if lost {
                                    StorageNodeServer::test_lose_direct_snapshot_reply();
                                } else {
                                    rpc::set_storage_rpc_response_connection_reusable(
                                        &mut frame.payload,
                                        false,
                                    )
                                    .unwrap();
                                }
                            }
                        }));
                    let failure = ready
                        .prepare_direct_remote(&primary.client, &session)
                        .err()
                        .expect("unconfirmed read");
                    let mut ready = failure.into_acknowledged();
                    assert_eq!(ready.test_binding_state(), history);
                    assert!(ready.open_direct_primary_session(&primary.client).is_err());
                    drop(ready);
                    assert_eq!(f.observe(), before);
                    require_payload(f, owner, shards, true);
                },
            );
        }
    }
}
