// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::cluster::{AllocatorCleanupFreshInstallOutcome, RequestWorkBudget};
use crate::ObjectPgActionError;

struct Fixture {
    map: Arc<LocalClusterMap>,
    cluster: Arc<StorageCluster>,
    bucket: BucketName,
    key: ObjectKey,
    pg_id: PgId,
    _tmp: test_util::TempDir,
}

impl Fixture {
    fn new() -> Self {
        let tmp = test_util::tempdir();
        let map = Arc::new(
            LocalClusterMap::open(
                tmp.path(),
                &[NodeId::new(0), NodeId::new(1), NodeId::new(2)],
                &[0, 1, 2, 3],
                EcShape { k: 2, m: 1 },
            )
            .unwrap(),
        );
        let bucket = bucket_for_pg(map.pg_topology(), 1, "allocator-install-");
        let key = key_for_object_pg(map.pg_topology(), &bucket, 2, "object-");
        let cluster = StorageCluster::from_static_local_map(map.clone()).unwrap();
        create_test_bucket(&cluster, &bucket);
        Self {
            map,
            cluster,
            bucket,
            key,
            pg_id: PgId::new(2),
            _tmp: tmp,
        }
    }

    fn command(&self, label: &str) -> MetadataCommandEnvelope {
        MetadataCommandEnvelope::new(
            self.cluster
                .next_object_metadata_command_id(self.pg_id)
                .unwrap(),
            MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
                self.bucket.clone(),
                self.key.clone(),
                crate::tests::stream_session_id(label),
                GenerationId::MIN,
                crate::clock::current_time_millis(),
            )),
        )
    }

    fn install(
        &self,
        budget: &mut RequestWorkBudget,
        build: impl FnOnce() -> Result<MetadataCommandEnvelope, ObjectPgActionError>,
    ) -> Result<AllocatorCleanupFreshInstallOutcome, ObjectPgActionError> {
        self.cluster
            .install_allocator_cleanup_built_metadata_command(
                crate::metadata_command::metadata_command_publisher!(ReservePutObjectGeneration),
                self.pg_id,
                &self.bucket,
                None,
                budget,
                build,
            )
    }

    fn positions(&self) -> Vec<(MetadataCommandReplicaState, u64)> {
        self.map
            .nodes
            .values()
            .map(|node| {
                let pg = node.storage_node().get_pg(self.pg_id.get()).unwrap();
                (
                    pg.metadata_command_replica_state().unwrap(),
                    pg.max_metadata_command_log_index(self.map.epoch()).unwrap(),
                )
            })
            .collect()
    }
}

fn budget() -> RequestWorkBudget {
    RequestWorkBudget::new(Duration::from_secs(5), None)
}

#[test]
fn allocator_built_install_propagates_builder_rejection_without_installation() {
    let f = Fixture::new();
    let before = f.positions();
    let error = f
        .install(&mut budget(), || {
            Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "test primary builder",
            }
            .into())
        })
        .err()
        .unwrap();
    assert!(matches!(
        error,
        ObjectPgActionError::Store(StoreError::RouteCapabilitySubjectMismatch {
            operation: "test primary builder"
        })
    ));
    assert_eq!(f.positions(), before);
    assert!(pending_metadata_command_for_test(&f.map, f.pg_id, &f.bucket).is_none());

    // The same integration point installs the exact successfully built command.
    let expected = f.command("successful");
    let outcome = f.install(&mut budget(), || Ok(expected.clone())).unwrap();
    let AllocatorCleanupFreshInstallOutcome::Installed(actual) = outcome else {
        panic!("successful builder did not install")
    };
    assert_eq!(*actual, expected);
    assert_eq!(
        pending_metadata_command_for_test(&f.map, f.pg_id, &f.bucket),
        Some(expected)
    );
}

#[test]
fn allocator_built_install_does_not_refresh_an_exhausted_caller_budget() {
    let f = Fixture::new();
    let before = f.positions();
    let mut expired = RequestWorkBudget::new(Duration::ZERO, None);
    let error = f
        .install(&mut expired, || panic!("expired budget reached builder"))
        .err()
        .unwrap();
    assert!(matches!(
        error,
        ObjectPgActionError::Store(StoreError::MetadataCommandContention { .. })
    ));
    assert_eq!(f.positions(), before);
    assert!(pending_metadata_command_for_test(&f.map, f.pg_id, &f.bucket).is_none());
}

#[test]
fn allocator_built_install_rejects_crossed_command_route_before_installation() {
    let f = Fixture::new();
    let valid = f.command("route-bound");
    let before = f.positions();
    for id in [
        MetadataCommandId::new(f.map.epoch(), PgId::new(3), valid.id().log_index()),
        MetadataCommandId::new(
            ClusterEpoch::new(f.map.epoch().get() + 1).unwrap(),
            f.pg_id,
            valid.id().log_index(),
        ),
    ] {
        let command = MetadataCommandEnvelope::new(id, valid.payload().clone());
        let error = f.install(&mut budget(), || Ok(command)).err().unwrap();
        assert!(matches!(
            error,
            ObjectPgActionError::Store(StoreError::RouteCapabilitySubjectMismatch {
                operation: "install built object metadata command"
            })
        ));
        assert_eq!(f.positions(), before);
        assert!(pending_metadata_command_for_test(&f.map, f.pg_id, &f.bucket).is_none());
    }
}

#[test]
fn allocator_built_install_returns_build_conflict_without_a_visible_contender() {
    let f = Fixture::new();
    let before = f.positions();
    let outcome = f
        .install(&mut budget(), || {
            Err(StoreError::MetadataCommandLogConflict {
                node_id: 0,
                pg_id: f.pg_id.get(),
                cluster_epoch: f.map.epoch(),
                log_index: 1,
            }
            .into())
        })
        .unwrap();
    assert!(matches!(
        outcome,
        AllocatorCleanupFreshInstallOutcome::LogConflictHandled
    ));
    assert_eq!(f.positions(), before);
    assert!(pending_metadata_command_for_test(&f.map, f.pg_id, &f.bucket).is_none());
}

#[test]
fn allocator_built_install_hands_off_the_exact_failed_contender() {
    let _serial = lock_metadata_command_apply_hook_test();
    let f = Fixture::new();
    let contender = f.command("handoff");
    insert_pending_metadata_command_for_test(&f.map, f.pg_id, &f.bucket, &contender);
    let attempts = Arc::new(AtomicUsize::new(0));
    let _hook = f.cluster.test_install_metadata_command_apply_attempt_hook({
        let attempts = attempts.clone();
        let id = contender.id();
        Arc::new(move |command| {
            assert_eq!(command.id(), id);
            attempts.fetch_add(1, Ordering::SeqCst);
            Err(StoreError::RouteMapExpired {
                cluster_epoch: id.cluster_epoch(),
                valid_until_ms: 1,
                now_ms: 2,
            })
        })
    });
    let outcome = f
        .install(&mut budget(), || {
            panic!("must not build across a pending contender")
        })
        .unwrap();
    let AllocatorCleanupFreshInstallOutcome::PendingContenderAwaitingRecovery { command } = outcome
    else {
        panic!("failed contender was not handed to recovery")
    };
    assert_eq!(*command, contender);
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(
        pending_metadata_command_for_test(&f.map, f.pg_id, &f.bucket),
        Some(contender)
    );
}

#[test]
fn allocator_built_install_drains_only_one_preexisting_or_build_race_contender() {
    let _serial = lock_metadata_command_apply_hook_test();
    for build_race in [false, true] {
        let f = Fixture::new();
        let first = f.command("first");
        if !build_race {
            insert_pending_metadata_command_for_test(&f.map, f.pg_id, &f.bucket, &first);
        }
        let next = Arc::new(Mutex::new(None));
        let _hook = f.cluster.test_install_after_metadata_command_drain_hook({
            let next = next.clone();
            let cluster = f.cluster.clone();
            let map = f.map.clone();
            let bucket = f.bucket.clone();
            let key = f.key.clone();
            let pg_id = f.pg_id;
            Arc::new(move || {
                let command = MetadataCommandEnvelope::new(
                    cluster.next_object_metadata_command_id(pg_id).unwrap(),
                    MetadataCommandPayload::ReserveObjectGeneration(
                        ReserveObjectGenerationCommand::new(
                            bucket.clone(),
                            key.clone(),
                            crate::tests::stream_session_id("second"),
                            GenerationId::new(2).unwrap(),
                            crate::clock::current_time_millis(),
                        ),
                    ),
                );
                insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);
                *next.lock().unwrap() = Some(command);
            })
        });
        let outcome = f
            .install(&mut budget(), || {
                assert!(
                    build_race,
                    "must not build past an existing pending command"
                );
                insert_pending_metadata_command_for_test(&f.map, f.pg_id, &f.bucket, &first);
                Err(StoreError::MetadataCommandLogConflict {
                    node_id: f
                        .map
                        .metadata_pg_primary_node(f.map.epoch(), f.pg_id)
                        .unwrap()
                        .node_id()
                        .as_u32(),
                    pg_id: f.pg_id.get(),
                    cluster_epoch: first.id().cluster_epoch(),
                    log_index: first.id().log_index().get(),
                }
                .into())
            })
            .unwrap();
        assert!(matches!(
            (build_race, outcome),
            (
                false,
                AllocatorCleanupFreshInstallOutcome::PendingContenderDrained
            ) | (
                true,
                AllocatorCleanupFreshInstallOutcome::LogConflictHandled
            )
        ));
        let next = next
            .lock()
            .unwrap()
            .clone()
            .expect("first drain must complete");
        assert_eq!(
            pending_metadata_command_for_test(&f.map, f.pg_id, &f.bucket),
            Some(next)
        );
        let primary = f
            .map
            .metadata_pg_primary_node(f.map.epoch(), f.pg_id)
            .unwrap();
        let pg = primary.storage_node().get_pg(f.pg_id.get()).unwrap();
        assert_eq!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &f.bucket,
                &f.key,
                &crate::tests::stream_session_id("first")
            )
            .unwrap(),
            GenerationId::MIN
        );
        assert!(matches!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &f.bucket,
                &f.key,
                &crate::tests::stream_session_id("second")
            ),
            Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
        ));
    }
}
