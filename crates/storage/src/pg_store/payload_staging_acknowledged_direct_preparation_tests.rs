// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::node_runtime::traits::PgMetadataStore;

macro_rules! acknowledged {
    ($owner:expr, $writer:ident) => {{
        for (i, bytes) in files::shards($owner, b"data").iter().enumerate() {
            $writer.write(i as u8, bytes);
        }
        $writer.persist().unwrap();
        let Writer::Direct(writer) = $writer else {
            unreachable!()
        };
        writer
            .into_acknowledged()
            .unwrap_or_else(|e| panic!("{}", e.error()))
    }};
}

#[test]
fn staging_acknowledged_direct_preparation_preserves_payload_and_empty_snapshot() {
    with_writer_and_section(
        StagingTarget::DirectPut,
        false,
        |f, owner, mut writer, section| {
            let ready = acknowledged!(owner, writer);
            let id = ready.id();
            let history = ready.test_binding_state();
            let node = f.map.node(NodeId::new(4)).unwrap().test_node();
            let before = node.get_pg(id.pg_id().get()).unwrap().conn.total_changes();
            let prepared = ready
                .prepare_direct_embedded(section)
                .unwrap_or_else(|e| panic!("{}", e.error()));
            assert_eq!(prepared.id(), id);
            assert_eq!(prepared.test_binding_state(), history);
            assert!(prepared.auth_snapshot().existing_etag.is_none());
            assert!(prepared.test_snapshot().current.is_none());
            assert!(prepared.test_snapshot().stale_payload.is_none());
            drop(prepared);
            assert_eq!(
                node.get_pg(id.pg_id().get()).unwrap().conn.total_changes(),
                before
            );
            installation::assert_payload_retained(f, owner);
        },
    );
}

#[test]
fn staging_acknowledged_direct_preparation_captures_exact_existing_object() {
    let f = allocation::setup_live_with_floor(StagingTarget::DirectPut, false, Some(10));
    let template = fresh_capture::template(StagingTarget::DirectPut);
    let crossed = (0..1000)
        .map(|i| crate::tests::object_key(format!("direct-preparation-{i}")))
        .find(|key| {
            key != &template.subject.key
                && topology().object_pg_for(&template.subject.bucket, key)
                    == template.subject.object_pg.get()
        })
        .unwrap();
    // The raw fixture writer timestamps each row. Pin that timestamp across
    // replicas so confirmation sees identical predecessor state, not five
    // independently created objects.
    let seed_clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    for n in 0..5 {
        let node = f.map.node(NodeId::new(n)).unwrap().test_node();
        let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
        for (key, etag) in [(&template.subject.key, 42), (&crossed, 99)] {
            // Metadata-only predecessor: this test inspects displacement facts,
            // not predecessor payload reads or reclaim. New staged files below
            // use the real encrypted fixture and normal acknowledgement path.
            pg.put_object_with_segments(
                &crate::PutLiveObjectReq {
                    bucket: template.subject.bucket.clone(),
                    key: key.clone(),
                    version_id: crate::VersionId::Null,
                    owner: crate::OwnerIdentity::from_principal("prior-owner"),
                    acl_grants: crate::AclGrants::default(),
                    public_read: false,
                    generation_id: crate::GenerationId::MIN,
                    size: 0,
                    etag: crate::ObjectEtag::single_part(etag),
                    ec: template.windows[0].geometry.ec,
                    layout: crate::ObjectLayout::Standard,
                    tags: None,
                    metadata_blob: None,
                    system_metadata_blob: None,
                    object_lock: crate::ObjectLockState::default(),
                    encryption: crate::tests::metadata_only_test_encryption(),
                },
                &[],
            )
            .unwrap();
        }
        complete_application::anchor(&pg, f.context.id());
        pg.recover_complete_command_state(
            PgStoreRecoveryContext::for_node(NodeId::new(n)),
            &topology(),
        )
        .unwrap();
    }
    drop(seed_clock);
    allocation::with_confirmed_duration(
        &f,
        "installed",
        1,
        Duration::from_secs(5),
        |section, mut windows| {
            let owner = files::load_owner(&f);
            let window = windows.pop().unwrap();
            let mut writer = Writer::Direct(Box::new(
                window
                    .into_direct_file_writer_embedded(
                        section,
                        &crate::tests::sse_s3_segment_test_fixture(b"data", 0).encryption,
                    )
                    .unwrap_or_else(|e| panic!("{}", e.error())),
            ));
            let ready = acknowledged!(&owner, writer);
            let node = f.map.node(NodeId::new(4)).unwrap().test_node();
            let before = node
                .get_pg(f.context.id().pg_id().get())
                .unwrap()
                .conn
                .total_changes();
            let prepared = ready
                .prepare_direct_embedded(section)
                .unwrap_or_else(|e| panic!("{}", e.error()));
            assert_eq!(
                prepared.auth_snapshot().existing_etag,
                Some(crate::ObjectEtag::single_part(42).format())
            );
            let snapshot = prepared.test_snapshot();
            assert_eq!(snapshot.current.as_ref().unwrap().key(), &owner.subject.key);
            assert_eq!(snapshot.stale_payload_source, snapshot.current);
            assert!(snapshot.committed_segments.is_none());
            drop(prepared);
            assert_eq!(
                node.get_pg(f.context.id().pg_id().get())
                    .unwrap()
                    .conn
                    .total_changes(),
                before
            );
            installation::assert_payload_retained(&f, &owner);
        },
    );
}

#[test]
fn staging_acknowledged_direct_preparation_rejects_foreign_section_and_preserves_retry() {
    let foreign = allocation::setup_live(StagingTarget::DirectPut, false);
    with_writer_and_section(
        StagingTarget::DirectPut,
        false,
        |f, owner, mut writer, section| {
            let ready = acknowledged!(owner, writer);
            let id = ready.id();
            let history = ready.test_binding_state();
            let admission = foreign.handle.admit_current_route().unwrap();
            let route = admission
                .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
                .unwrap();
            let foreign_section = route.lock_until(deadline()).unwrap();
            let observe = |map: &crate::cluster::LocalClusterMap| {
                let pg = map
                    .node(NodeId::new(4))
                    .unwrap()
                    .test_node()
                    .get_pg(id.pg_id().get())
                    .unwrap();
                (complete_pending::observe(&pg), pg.conn.total_changes())
            };
            let before = [observe(&f.map), observe(&foreign.map)];
            let failure = ready
                .prepare_direct_embedded(&foreign_section)
                .err()
                .expect("a matching PG/node/epoch is not the original admitted runtime");
            assert!(matches!(
                failure.error(),
                StagingWindowConfirmationError::Store(
                    StoreError::RouteCapabilitySubjectMismatch { .. }
                )
            ));
            assert_eq!([observe(&f.map), observe(&foreign.map)], before);
            let ready = failure.into_acknowledged();
            assert_eq!(ready.test_binding_state(), history);
            let prepared = ready
                .prepare_direct_embedded(section)
                .unwrap_or_else(|e| panic!("{}", e.error()));
            assert_eq!(prepared.test_binding_state(), history);
            drop(prepared);
            installation::assert_payload_retained(f, owner);
        },
    );
}

#[test]
fn staging_acknowledged_direct_preparation_rechecks_reservation_and_keeps_failed_custody() {
    for damage in ["reservation", "window", "expiry"] {
        with_writer_and_section(
            StagingTarget::DirectPut,
            false,
            |f, owner, mut writer, section| {
                let ready = acknowledged!(owner, writer);
                let history = ready.test_binding_state();
                let node = f.map.node(NodeId::new(4)).unwrap().test_node();
                let pg_id = owner.subject.object_pg.get();
                let before_owner = custody(f, owner);
                let error = if damage == "expiry" {
                    crate::clock::with_time_override(owner.windows[0].cutoff_ms, || {
                        ready.prepare_direct_embedded(section)
                    })
                    .err()
                    .expect("expired authority")
                } else {
                    let pg = node.get_pg(pg_id).unwrap();
                    let sql = if damage == "reservation" {
                        "UPDATE object_generation_reservations SET generation_id = generation_id + 1"
                    } else {
                        "UPDATE payload_staging_windows SET logical_bound = logical_bound - 1"
                    };
                    pg.conn.execute_batch(sql).unwrap();
                    let writes = pg.conn.total_changes();
                    drop(pg);
                    let failure = ready
                        .prepare_direct_embedded(section)
                        .err()
                        .expect("changed durable authority");
                    assert_eq!(node.get_pg(pg_id).unwrap().conn.total_changes(), writes);
                    failure
                };
                let ready = error.into_acknowledged();
                assert_eq!(ready.test_binding_state(), history);
                drop(ready);
                installation::assert_files_retained(f, owner);
                if damage == "expiry" {
                    assert_eq!(custody(f, owner), before_owner);
                }
            },
        );
    }
}
