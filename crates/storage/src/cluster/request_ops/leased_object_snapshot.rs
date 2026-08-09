// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::super::{ObjectReadMetadataRoute, RequestWorkBudget, RetainedActiveRouteRepairFence};
use super::*;

pub struct LeasedObjectReadSnapshotOutcome<T> {
    value: T,
    leased_snapshot: LeasedObjectReadSnapshot,
}

impl<T> LeasedObjectReadSnapshotOutcome<T> {
    pub fn snapshot(&self) -> &ObjectReadSnapshot {
        self.leased_snapshot.snapshot.as_ref()
    }

    /// Split the authorization result into its value, shared immutable
    /// snapshot, and non-cloneable payload handoff. The snapshot and handoff
    /// refer to the same allocation rather than duplicating payload vectors.
    pub fn into_parts(self) -> (T, Arc<ObjectReadSnapshot>, LeasedObjectReadSnapshot) {
        let snapshot = Arc::clone(&self.leased_snapshot.snapshot);
        (self.value, snapshot, self.leased_snapshot)
    }
}

/// Non-cloneable proof that an exact object snapshot was loaded while its
/// payload generation was protected by a broad deletion-exclusion lease.
///
/// Its complete representation and broad-to-narrow consuming transition are
/// private to this module. Surrounding cluster code can inspect the immutable
/// snapshot and pass the token to the admitted route, but cannot extract or
/// retain the broad lease independently.
pub struct LeasedObjectReadSnapshot {
    cluster: Arc<StorageCluster>,
    bucket: BucketName,
    key: ObjectKey,
    version_id: Option<VersionId>,
    snapshot_mode: ObjectReadSnapshotMode,
    pg_id: ObjectMetadataPgId,
    snapshot: Arc<ObjectReadSnapshot>,
    payload_lease: Option<ObjectPayloadLease>,
    repair_fence: Option<RetainedActiveRouteRepairFence>,
}

impl LeasedObjectReadSnapshot {
    pub fn snapshot(&self) -> &ObjectReadSnapshot {
        self.snapshot.as_ref()
    }

    fn matches_route(
        &self,
        cluster: &Arc<StorageCluster>,
        route: &ObjectReadMetadataRoute<'_>,
    ) -> bool {
        Arc::ptr_eq(cluster, &self.cluster)
            && self.bucket == *route.bucket
            && self.key == *route.key
            && self.version_id == route.version_id
            && self.snapshot_mode == route.snapshot_mode
            && self.pg_id == route.pg_id
    }

    #[cfg(test)]
    pub(crate) fn test_shares_snapshot(&self, snapshot: &Arc<ObjectReadSnapshot>) -> bool {
        Arc::ptr_eq(snapshot, &self.snapshot)
    }

    #[cfg(test)]
    pub(crate) fn test_clear_object_segments(&mut self) {
        Arc::make_mut(&mut self.snapshot).object_segments.clear();
    }
}

/// Unforgeable authority for acquiring the broad deletion-exclusion lease
/// used only while loading one exact object snapshot.
///
/// The raw local-map acquisition requires this value, while construction is
/// private to the complete leased-snapshot workflow in this module. Sibling
/// request implementations can therefore neither acquire nor retain a broad
/// lease independently of the opaque snapshot handoff.
pub(in crate::cluster) struct BroadObjectPayloadLeaseAuthority<'a> {
    bucket: &'a BucketName,
    key: &'a ObjectKey,
    generation_id: GenerationId,
    _private: (),
}

impl<'a> BroadObjectPayloadLeaseAuthority<'a> {
    fn new(bucket: &'a BucketName, key: &'a ObjectKey, generation_id: GenerationId) -> Self {
        Self {
            bucket,
            key,
            generation_id,
            _private: (),
        }
    }

    pub(in crate::cluster) fn bucket(&self) -> &BucketName {
        self.bucket
    }

    pub(in crate::cluster) fn key(&self) -> &ObjectKey {
        self.key
    }

    pub(in crate::cluster) fn generation_id(&self) -> GenerationId {
        self.generation_id
    }
}

impl ActiveObjectReadRoute<'_> {
    pub fn load_leased_object_read_snapshot_if<T, E>(
        &self,
        action: impl FnMut(&StoredObject) -> Result<T, E>,
    ) -> Result<Result<LeasedObjectReadSnapshotOutcome<T>, E>, ObjectReadFailure> {
        let route = ObjectReadMetadataRoute {
            bucket: &self.bucket,
            key: &self.key,
            version_id: self.version_id,
            snapshot_mode: self.snapshot_mode,
            pg_id: self.pg_id,
        };
        let repair_fence = RetainedActiveRouteRepairFence {
            gate: self.admission._permit.gate.clone(),
            publication_generation: self.admission._permit.gate.publication_generation(),
            admitted_lease: self.admission.admitted_lease,
        };
        self.admission
            .cluster
            .load_leased_object_read_snapshot_if_on_route(
                &route,
                action,
                Some(repair_fence),
                || self.admission.require_valid_now_raw(),
            )
            .map_err(ObjectReadFailure::from_object_pg_action)
    }

    /// Retain narrowly scoped payload authority for the exact snapshot loaded
    /// through this route.
    ///
    /// The returned capability owns only deletion-exclusion leases and exact
    /// segment descriptors. It does not retain this request's publication
    /// admission and therefore cannot perform another object metadata lookup.
    pub fn retain_object_payload_read(
        &self,
        leased_snapshot: LeasedObjectReadSnapshot,
    ) -> Result<Option<RetainedObjectPayloadRead>, ObjectReadFailure> {
        let route = ObjectReadMetadataRoute {
            bucket: &self.bucket,
            key: &self.key,
            version_id: self.version_id,
            snapshot_mode: self.snapshot_mode,
            pg_id: self.pg_id,
        };
        self.admission
            .cluster
            .retain_object_payload_read_from_leased_snapshot(leased_snapshot, &route, || {
                self.admission.require_valid_now_raw()
            })
            .map_err(ObjectReadFailure::from_store)
    }
}

impl StorageCluster {
    #[cfg(test)]
    pub(crate) fn load_leased_object_read_snapshot_if<T, E>(
        self: &Arc<Self>,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        snapshot_mode: ObjectReadSnapshotMode,
        action: impl FnMut(&StoredObject) -> Result<T, E>,
    ) -> Result<Result<LeasedObjectReadSnapshotOutcome<T>, E>, ObjectPgActionError> {
        let route = ObjectReadMetadataRoute {
            bucket,
            key,
            version_id,
            snapshot_mode,
            pg_id: self.object_metadata_pg(bucket, key),
        };
        self.load_leased_object_read_snapshot_if_on_route(&route, action, None, || Ok(()))
    }

    fn load_leased_object_read_snapshot_if_on_route<T, E>(
        self: &Arc<Self>,
        route: &ObjectReadMetadataRoute<'_>,
        mut action: impl FnMut(&StoredObject) -> Result<T, E>,
        repair_fence: Option<RetainedActiveRouteRepairFence>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
    ) -> Result<Result<LeasedObjectReadSnapshotOutcome<T>, E>, ObjectPgActionError> {
        let object_pg_id = route.pg_id;
        let pg_id = object_pg_id.pg_id();
        require_valid_route()?;
        let read_node = self
            .local_map
            .metadata_pg_read_node(self.operation_epoch(), pg_id)?;
        let object_read_client = read_node.object_read_metadata_client();
        let object_read_route = object_read_client.open_object_read_metadata_route(
            self.operation_epoch(),
            object_pg_id,
            route.bucket,
            route.key,
            read_node.authorization(),
        )?;
        let mut work_budget = RequestWorkBudget::new(OBJECT_READ_SNAPSHOT_STALE_RETRY_BUDGET, None)
            .for_operation("load_leased_object_read_snapshot")
            .for_pg(pg_id);
        loop {
            work_budget
                .check("load leased object read snapshot stale retry budget exhausted")
                .map_err(ObjectPgActionError::Store)?;
            require_valid_route()?;
            let subject = object_read_route.load_object_read_auth_subject(route.version_id)?;
            let value = match action(&subject.stored) {
                Ok(value) => value,
                Err(error) => return Ok(Err(error)),
            };
            let payload_lease = if let Some(live) = subject.stored.as_live() {
                require_valid_route()?;
                let broad_lease_authority = BroadObjectPayloadLeaseAuthority::new(
                    route.bucket,
                    route.key,
                    live.generation_id,
                );
                match self.acquire_available_object_payload_lease(&broad_lease_authority) {
                    Ok(lease) => Some(lease),
                    Err(StoreError::NotFound) => {
                        work_budget
                            .sleep_after_contention(
                                "load leased object read snapshot stale retry budget exhausted",
                            )
                            .map_err(ObjectPgActionError::Store)?;
                        continue;
                    }
                    Err(error) => return Err(ObjectPgActionError::Store(error)),
                }
            } else {
                None
            };
            require_valid_route()?;
            match object_read_route.load_object_read_snapshot_for_subject(
                route.version_id,
                &subject.identity,
                route.snapshot_mode,
            ) {
                Ok(snapshot) => {
                    require_valid_route()?;
                    return Ok(Ok(LeasedObjectReadSnapshotOutcome {
                        value,
                        leased_snapshot: LeasedObjectReadSnapshot {
                            cluster: Arc::clone(self),
                            bucket: route.bucket.clone(),
                            key: route.key.clone(),
                            version_id: route.version_id,
                            snapshot_mode: route.snapshot_mode,
                            pg_id: route.pg_id,
                            snapshot: Arc::new(snapshot),
                            payload_lease,
                            repair_fence,
                        },
                    }));
                }
                Err(ObjectPgActionError::StaleObjectReadSubject) => {
                    work_budget
                        .sleep_after_contention(
                            "load leased object read snapshot stale retry budget exhausted",
                        )
                        .map_err(ObjectPgActionError::Store)?;
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn retain_object_payload_read_from_leased_snapshot(
        self: &Arc<Self>,
        leased_snapshot: LeasedObjectReadSnapshot,
        route: &ObjectReadMetadataRoute<'_>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
    ) -> Result<Option<RetainedObjectPayloadRead>, StoreError> {
        if !leased_snapshot.matches_route(self, route) {
            return Err(StoreError::PayloadShardSetMismatch {
                reason: "leased object snapshot provenance does not match active read route"
                    .to_string(),
            });
        }
        self.retain_object_payload_read_from_matching_leased_snapshot(
            leased_snapshot,
            &mut require_valid_route,
        )
    }

    fn retain_object_payload_read_from_matching_leased_snapshot(
        self: &Arc<Self>,
        leased_snapshot: LeasedObjectReadSnapshot,
        require_valid_route: &mut impl FnMut() -> Result<(), StoreError>,
    ) -> Result<Option<RetainedObjectPayloadRead>, StoreError> {
        let LeasedObjectReadSnapshot {
            cluster,
            bucket,
            key,
            version_id: _,
            snapshot_mode,
            pg_id: _,
            snapshot,
            payload_lease,
            repair_fence,
        } = leased_snapshot;
        if !Arc::ptr_eq(self, &cluster) {
            return Err(StoreError::PayloadShardSetMismatch {
                reason: "leased object snapshot belongs to another storage cluster".to_string(),
            });
        }
        if snapshot_mode != ObjectReadSnapshotMode::FullPayloadLayout {
            return Err(StoreError::PayloadShardSetMismatch {
                reason: "retained payload read requires a complete payload-layout snapshot"
                    .to_string(),
            });
        }
        let Some(live) = snapshot.stored.as_live() else {
            return Ok(None);
        };

        let mut segments = Vec::with_capacity(
            snapshot.object_segments.len() + snapshot.multipart_part_segments.len(),
        );
        for segment in &snapshot.object_segments {
            let Some(record) = segment.object_record() else {
                return Err(StoreError::PayloadShardSetMismatch {
                    reason: "object snapshot contains a multipart segment".to_string(),
                });
            };
            if record.bucket != bucket || record.key != key || record.version_id != live.version_id
            {
                return Err(StoreError::PayloadShardSetMismatch {
                    reason: "object segment does not match leased snapshot subject".to_string(),
                });
            }
            segments.push(segment.clone());
        }
        for segment in &snapshot.multipart_part_segments {
            let Some(record) = segment.multipart_record() else {
                return Err(StoreError::PayloadShardSetMismatch {
                    reason: "multipart snapshot contains an object segment".to_string(),
                });
            };
            if record.bucket != bucket
                || record.key != key
                || record.version_id != live.version_id.to_u64()
            {
                return Err(StoreError::PayloadShardSetMismatch {
                    reason: "multipart segment does not match leased snapshot subject".to_string(),
                });
            }
            segments.push(segment.clone());
        }
        let layout_matches = match live.layout {
            ObjectLayout::Standard => {
                snapshot.multipart_parts.is_empty()
                    && snapshot.multipart_part_segments.is_empty()
                    && (live.size == 0 || !snapshot.object_segments.is_empty())
            }
            ObjectLayout::MultipartManifest { parts_count } => {
                let mut part_sizes = HashMap::new();
                let unique_parts = snapshot
                    .multipart_parts
                    .iter()
                    .all(|part| part_sizes.insert(part.part_number(), part.size()).is_none());
                let mut segment_counts = HashMap::<u32, usize>::new();
                let segments_have_parts = snapshot.multipart_part_segments.iter().all(|segment| {
                    let Some(part_number) = segment.part_number() else {
                        return false;
                    };
                    if !part_sizes.contains_key(&part_number) {
                        return false;
                    }
                    *segment_counts.entry(part_number).or_default() += 1;
                    true
                });
                snapshot.object_segments.is_empty()
                    && unique_parts
                    && snapshot.multipart_parts.len() == parts_count.get() as usize
                    && segments_have_parts
                    && part_sizes.iter().all(|(part_number, size)| {
                        *size == 0 || segment_counts.contains_key(part_number)
                    })
            }
        };
        if !layout_matches {
            return Err(StoreError::PayloadShardSetMismatch {
                reason: "payload segments do not match the stored object layout".to_string(),
            });
        }

        let broad_lease = payload_lease.ok_or(StoreError::NotFound)?;
        let broad_leased_node_ids = broad_lease.leased_node_ids();
        let mut locations = Vec::new();
        let mut segment_locations = Vec::with_capacity(segments.len());
        for segment in &segments {
            let request = segment.stored_bytes_request();
            if request.stored_size == 0 {
                segment_locations.push(Vec::new());
                continue;
            }
            require_valid_route()?;
            let placed = self.segment_payload_shard_locations_at_placement_epoch(
                segment.placement_cluster_epoch(),
                &request,
            )?;
            locations.extend(
                placed
                    .iter()
                    .filter(|location| broad_leased_node_ids.contains(&location.node_id()))
                    .cloned(),
            );
            segment_locations.push(placed);
        }
        require_valid_route()?;
        let narrow_lease = self.acquire_available_object_payload_lease_for_shard_locations(
            &bucket,
            &key,
            live.generation_id,
            &locations,
        )?;
        require_valid_route()?;
        let leased_node_ids = narrow_lease.leased_node_ids().clone();
        for (segment, placed) in segments.iter().zip(&segment_locations) {
            if segment.stored_bytes_request().stored_size == 0 {
                continue;
            }
            let required = usize::from(segment.stored_bytes_request().ec.k);
            let available = placed
                .iter()
                .filter(|location| leased_node_ids.contains(&location.node_id()))
                .count();
            if available < required {
                return Err(StoreError::NotFound);
            }
        }

        let retained = RetainedObjectPayloadRead {
            cluster,
            bucket,
            key,
            generation_id: live.generation_id,
            segments,
            leased_node_ids,
            lease: Mutex::new(Some(narrow_lease)),
            repair_fence,
        };
        drop(broad_lease);
        Ok(Some(retained))
    }

    #[cfg(test)]
    pub(crate) fn retain_object_payload_read(
        self: &Arc<Self>,
        leased_snapshot: LeasedObjectReadSnapshot,
    ) -> Result<Option<RetainedObjectPayloadRead>, StoreError> {
        if !Arc::ptr_eq(self, &leased_snapshot.cluster) {
            return Err(StoreError::PayloadShardSetMismatch {
                reason: "leased object snapshot belongs to another storage cluster".to_string(),
            });
        }
        self.retain_object_payload_read_from_matching_leased_snapshot(leased_snapshot, &mut || {
            Ok(())
        })
    }

    fn acquire_available_object_payload_lease(
        self: &Arc<Self>,
        authority: &BroadObjectPayloadLeaseAuthority<'_>,
    ) -> Result<ObjectPayloadLease, StoreError> {
        let bucket = authority.bucket();
        let key = authority.key();
        let generation_id = authority.generation_id();
        let runtime_state = self.ensure_object_payload_lease_allowed(bucket, key, generation_id)?;
        let acquired = self
            .local_map
            .try_acquire_available_object_payload_lease(authority)?;
        if acquired.node_leases.is_empty() {
            return Err(StoreError::NotFound);
        }
        Ok(ObjectPayloadLease::new(
            Arc::downgrade(self),
            acquired,
            runtime_state,
            bucket.clone(),
            key.clone(),
            generation_id,
            self.object_metadata_pg_id(bucket, key),
        ))
    }
}
