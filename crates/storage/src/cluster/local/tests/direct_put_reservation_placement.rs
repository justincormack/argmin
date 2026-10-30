// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{ObjectPgActionError, SessionId};

struct Fixture {
    map: Arc<LocalClusterMap>,
    healthy: Arc<StorageCluster>,
    bucket: BucketName,
    key: ObjectKey,
    first_pg: u32,
    second_pg: u32,
    _tmp: test_util::TempDir,
}

impl Fixture {
    fn new() -> Self {
        let tmp = test_util::tempdir();
        let nodes = [0, 1, 2].map(NodeId::new);
        let map = Arc::new(
            LocalClusterMap::open(
                tmp.path(),
                &nodes,
                &(0..8).collect::<Vec<_>>(),
                EcShape { k: 2, m: 1 },
            )
            .unwrap(),
        );
        let bucket = crate::tests::bucket_name("reservation-placement");
        let key = (0..10_000)
            .map(|n| crate::tests::object_key(format!("key-{n}")))
            .find(|key| {
                let first = map
                    .object_generation_segment_data_pg(&bucket, key, GenerationId::MIN, 0)
                    .get();
                let second = map
                    .object_generation_segment_data_pg(
                        &bucket,
                        key,
                        GenerationId::new(2).unwrap(),
                        0,
                    )
                    .get();
                let metadata = map.pg_topology().object_pg_for(&bucket, key);
                let bucket_pg = map.pg_topology().bucket_pg_for(&bucket);
                first != second
                    && first != metadata
                    && second != metadata
                    && first != bucket_pg
                    && second != bucket_pg
            })
            .unwrap();
        let first_pg = map
            .object_generation_segment_data_pg(&bucket, &key, GenerationId::MIN, 0)
            .get();
        let second_pg = map
            .object_generation_segment_data_pg(&bucket, &key, GenerationId::new(2).unwrap(), 0)
            .get();
        assert_ne!(first_pg, second_pg);
        let healthy = StorageCluster::from_static_local_map(map.clone()).unwrap();
        create_test_bucket(&healthy, &bucket);
        Self {
            map,
            healthy,
            bucket,
            key,
            first_pg,
            second_pg,
            _tmp: tmp,
        }
    }

    fn unavailable(&self, pg: u32) -> Arc<StorageCluster> {
        let epoch = self.map.epoch();
        let routes = self.map.pg_routes.values().map(|route| {
            PgRouteSnapshot::reconstructed(
                epoch,
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                if route.pg_id().get() == pg {
                    PgState::Peering
                } else {
                    route.state()
                },
            )
        });
        // Clone process-local runtime ownership; never reopen the SQLite files.
        let map = self
            .map
            .test_clone_with_pg_routes(epoch, routes, [])
            .unwrap();
        StorageCluster::from_static_local_map(Arc::new(map)).unwrap()
    }
}

#[derive(Debug, PartialEq, Eq)]
struct ReservationObservation {
    replica: MetadataCommandReplicaState,
    log_index: u64,
    candidate: GenerationId,
    reservation: Option<GenerationId>,
}

fn observe(
    map: &LocalClusterMap,
    bucket: &BucketName,
    key: &ObjectKey,
    id: &SessionId,
) -> Vec<ReservationObservation> {
    let pg_id = map.pg_topology().object_pg_for(bucket, key);
    map.nodes
        .values()
        .map(|node| {
            let pg = node.storage_node().get_pg(pg_id).unwrap();
            assert!(pg
                .pending_metadata_command_envelope(node.node_id().as_u32(), map.epoch())
                .unwrap()
                .is_none());
            let reservation = match crate::PgMetadataStore::get_object_generation_reservation(
                &*pg, bucket, key, id,
            ) {
                Ok(generation) => Some(generation),
                Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. }) => None,
                Err(error) => panic!("unexpected reservation lookup: {error:?}"),
            };
            ReservationObservation {
                replica: pg.metadata_command_replica_state().unwrap(),
                log_index: pg.max_metadata_command_log_index(map.epoch()).unwrap(),
                candidate: crate::PgMetadataStore::next_generation_id(&*pg, bucket, key).unwrap(),
                reservation,
            }
        })
        .collect()
}

#[test]
fn direct_put_reservation_rejects_unavailable_selected_placement_before_publication() {
    let f = Fixture::new();
    let restricted = f.unavailable(f.first_pg);
    let id = crate::tests::stream_session_id("preflight");
    let before = observe(&f.map, &f.bucket, &f.key, &id);
    let reached_install = Arc::new(AtomicBool::new(false));
    let _hook = restricted.test_install_before_metadata_command_pending_install_hook({
        let reached = reached_install.clone();
        Arc::new(move || {
            reached.store(true, Ordering::SeqCst);
        })
    });
    assert!(
        matches!(restricted.reserve_put_object_generation(&f.bucket, &f.key, &id),
        Err(ObjectPgActionError::Store(StoreError::PgNotActive { pg_id, state: PgState::Peering, .. })) if pg_id == f.first_pg)
    );
    assert_eq!(observe(&f.map, &f.bucket, &f.key, &id), before);
    assert!(!reached_install.load(Ordering::SeqCst));
    let _healthy_hook = f
        .healthy
        .test_install_before_metadata_command_pending_install_hook({
            let reached = reached_install.clone();
            Arc::new(move || {
                reached.store(true, Ordering::SeqCst);
            })
        });
    assert_eq!(
        f.healthy
            .reserve_put_object_generation(&f.bucket, &f.key, &id)
            .unwrap(),
        GenerationId::MIN
    );
    assert!(
        reached_install.load(Ordering::SeqCst),
        "positive control did not reach installation"
    );
    // Re-observing an already committed reservation is not fresh issuance.
    assert_eq!(
        restricted
            .reserve_put_object_generation(&f.bucket, &f.key, &id)
            .unwrap(),
        GenerationId::MIN
    );
}

#[test]
fn direct_put_reservation_rechecks_new_candidate_placement_after_generation_race() {
    let f = Fixture::new();
    let restricted = f.unavailable(f.second_pg);
    let loser = crate::tests::stream_session_id("loser");
    let winner = crate::tests::stream_session_id("winner");
    let after_winner = Arc::new(Mutex::new(None));
    let ran = Arc::new(AtomicBool::new(false));
    let _hook = restricted.test_install_before_object_generation_command_id_hook({
        let healthy = f.healthy.clone();
        let map = f.map.clone();
        let (bucket, key, loser, winner) = (
            f.bucket.clone(),
            f.key.clone(),
            loser.clone(),
            winner.clone(),
        );
        let (ran, observed) = (ran.clone(), after_winner.clone());
        Arc::new(move || {
            if !ran.swap(true, Ordering::SeqCst) {
                assert_eq!(
                    healthy
                        .reserve_put_object_generation(&bucket, &key, &winner)
                        .unwrap(),
                    GenerationId::MIN
                );
                *observed.lock().unwrap() = Some(observe(&map, &bucket, &key, &loser));
            }
        })
    });
    assert!(
        matches!(restricted.reserve_put_object_generation(&f.bucket, &f.key, &loser),
        Err(ObjectPgActionError::Store(StoreError::PgNotActive { pg_id, .. })) if pg_id == f.second_pg)
    );
    assert!(ran.load(Ordering::SeqCst));
    assert_eq!(
        observe(&f.map, &f.bucket, &f.key, &loser),
        after_winner.lock().unwrap().take().unwrap()
    );
}

#[test]
fn direct_put_reservation_converges_exact_pending_command_despite_unavailable_data_route() {
    let f = Fixture::new();
    let restricted = f.unavailable(f.first_pg);
    let id = crate::tests::stream_session_id("pending");
    let pg = PgId::new(f.map.pg_topology().object_pg_for(&f.bucket, &f.key));
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            f.map.epoch(),
            pg,
            f.map.test_next_metadata_command_log_index(pg),
        ),
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            f.bucket.clone(),
            f.key.clone(),
            id.clone(),
            GenerationId::MIN,
            123,
        )),
    );
    insert_pending_metadata_command_for_test(&f.map, pg, &f.bucket, &command);
    assert_eq!(
        restricted
            .reserve_put_object_generation(&f.bucket, &f.key, &id)
            .unwrap(),
        GenerationId::MIN
    );
    for observed in observe(&f.map, &f.bucket, &f.key, &id) {
        assert_eq!(observed.reservation, Some(GenerationId::MIN));
        assert_eq!(observed.log_index, command.id().log_index().get());
        assert_eq!(
            observed.replica.applied_log_index,
            command.id().log_index().get()
        );
    }
}
