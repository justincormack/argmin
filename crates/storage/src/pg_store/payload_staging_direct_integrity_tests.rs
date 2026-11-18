// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn staging_direct_integrity_manifest_binds_actual_receipts_and_retains_custody() {
    for data in [b"".as_slice(), b"x", b"data"] {
        with_writer_and_section(
            StagingTarget::DirectPut,
            false,
            |f, owner, writer, section| {
                let Writer::Direct(mut writer) = writer else {
                    unreachable!()
                };
                let window = &owner.windows[0];
                let shards = files::shards(owner, data);
                let stored_size = window
                    .geometry
                    .codec
                    .stored_size(data.len() as u64)
                    .unwrap();
                assert_eq!(window.geometry.ec, EcShape { k: 2, m: 1 });
                assert!(
                    stored_size > data.len() as u64,
                    "encrypted fixture includes framing/tags"
                );
                if data == b"x" {
                    assert_ne!(
                        stored_size % u64::from(window.geometry.ec.k),
                        0,
                        "one-byte fixture must exercise EC padding removal"
                    );
                }
                let mut stored = shards[..usize::from(window.geometry.ec.k)].concat();
                stored.truncate(stored_size as usize);
                let crc = checksum::crc64::checksum(&stored);
                let acks: Vec<_> = shards
                    .iter()
                    .enumerate()
                    .map(|(index, bytes)| {
                        writer
                            .write_embedded(index as u8, data.len() as u64, bytes)
                            .unwrap()
                    })
                    .collect();
                writer.persist_acknowledgements_embedded().unwrap();
                let ready = writer
                    .into_acknowledged()
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                let history = ready.test_binding_state();
                let id = ready.id();
                let prepared = ready
                    .prepare_direct_embedded(section)
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                let changes = || {
                    (0..5)
                        .map(|n| {
                            f.map
                                .node(NodeId::new(n))
                                .unwrap()
                                .test_node()
                                .get_pg(id.pg_id().get())
                                .unwrap()
                                .conn
                                .total_changes()
                        })
                        .collect::<Vec<_>>()
                };
                let before = changes();
                let failure = prepared
                    .with_integrity(crc ^ 1)
                    .err()
                    .expect("CRC claim is not authority");
                assert!(matches!(failure.error(), E::Window(message)
                if message.contains("systematic shard acknowledgements do not compose")));
                let prepared = failure.into_prepared();
                assert_eq!(prepared.test_binding_state(), history);
                assert_eq!(changes(), before);
                let witnessed = prepared
                    .with_integrity(crc)
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                assert_eq!(witnessed.id(), id);
                assert_eq!(witnessed.test_binding_state(), history);
                assert!(witnessed.auth_snapshot().existing_etag.is_none());
                let manifest = witnessed.test_manifest();
                assert_eq!(manifest.bucket, owner.subject.bucket);
                assert_eq!(manifest.key, owner.subject.key);
                assert_eq!(manifest.generation, owner.subject.payload_generation);
                assert_eq!(manifest.segment_hash, window.segment_hash);
                assert_eq!(manifest.data_pg, window.participants.data_pg);
                assert_eq!(
                    manifest.placement_epoch,
                    window.participants.placement_epoch
                );
                assert_eq!(manifest.ec, window.geometry.ec);
                assert_eq!(manifest.codec, window.geometry.codec);
                assert_eq!(manifest.logical_size, data.len() as u64);
                assert_eq!(manifest.stored_size, stored_size);
                assert_eq!(manifest.crc64, crc);
                let record = manifest.integrity.durable_record();
                for (index, ack) in acks.iter().enumerate() {
                    assert_eq!(record.expected_ack(index), Some(*ack));
                }
                assert!(record.expected_ack(acks.len()).is_none());
                drop(witnessed);
                assert_eq!(changes(), before);
                assert_eq!(custody(f, owner), *owner);
                for (index, bytes) in shards.iter().enumerate() {
                    let p = &window.participants;
                    let key = ShardKey::new(&window.segment_hash, window.start.get(), index as u8);
                    assert_eq!(
                        f.map
                            .node(p.files[index])
                            .unwrap()
                            .test_node()
                            .read_shard_file(p.data_pg.get(), &key)
                            .unwrap(),
                        *bytes
                    );
                    f.map
                        .node(p.acknowledgement_primary)
                        .unwrap()
                        .test_node()
                        .get_pg(p.data_pg.get())
                        .unwrap()
                        .validate_written_shard_ack(&key, acks[index])
                        .unwrap();
                }
            },
        );
    }
}

#[test]
fn staging_direct_integrity_expiry_returns_original_prepared_custody() {
    with_writer_and_section(
        StagingTarget::DirectPut,
        false,
        |f, owner, mut writer, section| {
            let shards = files::shards(owner, b"data");
            let mut stored = shards[..usize::from(owner.windows[0].geometry.ec.k)].concat();
            stored.truncate(owner.windows[0].geometry.codec.stored_size(4).unwrap() as usize);
            for (i, bytes) in shards.iter().enumerate() {
                writer.write(i as u8, bytes);
            }
            writer.persist().unwrap();
            let Writer::Direct(writer) = writer else {
                unreachable!()
            };
            let prepared = writer
                .into_acknowledged()
                .unwrap_or_else(|e| panic!("{}", e.error()))
                .prepare_direct_embedded(section)
                .unwrap_or_else(|e| panic!("{}", e.error()));
            let history = prepared.test_binding_state();
            let failure = crate::clock::with_time_override(owner.windows[0].cutoff_ms, || {
                prepared.with_integrity(checksum::crc64::checksum(&stored))
            })
            .err()
            .expect("immutable cutoff");
            // CapturedWindowRead checks its rebound route fence before its
            // separate operation cutoff. Pin that earlier fail-closed boundary.
            assert!(
                matches!(
                    failure.error(),
                    E::Store(StoreError::RouteMapExpired { .. })
                ),
                "unexpected expiry diagnostic: {}",
                failure.error()
            );
            let prepared = failure.into_prepared();
            assert_eq!(prepared.test_binding_state(), history);
            drop(prepared);
            installation::assert_payload_retained(f, owner);
        },
    );
}
