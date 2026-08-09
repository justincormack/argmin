// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

pub struct StorageCluster {
    local_map: Arc<LocalClusterMap>,
    operation_epoch: ClusterEpoch,
    route_authority: StorageClusterRouteAuthority,
    bucket_write_owner_token: Arc<str>,
    rpc_auth: Option<crate::StorageRpcClientAuthConfig>,
    rpc_endpoints:
        Option<Arc<BTreeMap<NodeId, crate::storage_rpc_transport::StorageRpcClientEndpoint>>>,
    #[cfg(any(test, feature = "test-hooks"))]
    test_hooks: Arc<Mutex<StorageClusterTestHooks>>,
}

#[derive(Clone)]
pub struct StorageClusterRouteHandle {
    cluster: Arc<RwLock<Arc<StorageCluster>>>,
    same_epoch_generations: Arc<Mutex<Vec<Weak<StorageCluster>>>>,
    route_admission: StorageClusterRouteAdmissionGate,
}

/// Capability for publishing and renewing control-plane-authorized dynamic
/// route maps.
///
/// Request handling receives only [`StorageClusterRouteHandle`]. Constructing
/// this capability verifies that the initial generation has dynamic authority,
/// so static topology cannot acquire publication or refresh operations.
#[derive(Clone)]
pub struct StorageClusterRuntimeMapHandle {
    route_handle: StorageClusterRouteHandle,
}

#[derive(Clone, Default)]
struct StorageClusterRouteAdmissionGate {
    inner: Arc<StorageClusterRouteAdmissionGateInner>,
}

#[derive(Default)]
struct StorageClusterRouteAdmissionGateInner {
    state: Mutex<StorageClusterRouteAdmissionState>,
    changed: Condvar,
}

#[derive(Clone)]
pub(crate) struct StorageClusterRouteAdmissionDomain {
    gate: Weak<StorageClusterRouteAdmissionGateInner>,
}

impl StorageClusterRouteAdmissionDomain {
    pub(crate) fn matches(&self, handle: &StorageClusterRouteHandle) -> bool {
        self.gate
            .upgrade()
            .is_some_and(|gate| Arc::ptr_eq(&gate, &handle.route_admission.inner))
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum StorageClusterRouteTransitionState {
    #[default]
    Open,
    Draining,
    Publishing,
}

#[derive(Debug, Default)]
struct StorageClusterRouteAdmissionState {
    active_requests: usize,
    transition: StorageClusterRouteTransitionState,
    publication_generation: u64,
}

impl StorageClusterRouteAdmissionGate {
    fn acquire(&self) -> StorageClusterRouteAdmissionPermit {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while state.transition != StorageClusterRouteTransitionState::Open {
            state = self
                .inner
                .changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        state.active_requests = state
            .active_requests
            .checked_add(1)
            .expect("route admission permit count must not overflow");
        #[cfg(any(test, feature = "test-hooks"))]
        self.inner.changed.notify_all();
        StorageClusterRouteAdmissionPermit { gate: self.clone() }
    }

    fn begin_publication(&self) -> StorageClusterRoutePublicationGuard {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while state.transition != StorageClusterRouteTransitionState::Open {
            state = self
                .inner
                .changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        state.transition = StorageClusterRouteTransitionState::Draining;
        self.inner.changed.notify_all();
        while state.active_requests != 0 {
            state = self
                .inner
                .changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        state.transition = StorageClusterRouteTransitionState::Publishing;
        StorageClusterRoutePublicationGuard { gate: self.clone() }
    }

    fn acquire_for_publication_generation(
        &self,
        publication_generation: u64,
    ) -> Option<StorageClusterRouteAdmissionPermit> {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.transition != StorageClusterRouteTransitionState::Open
            || state.publication_generation != publication_generation
        {
            return None;
        }
        state.active_requests = state
            .active_requests
            .checked_add(1)
            .expect("route admission permit count must not overflow");
        Some(StorageClusterRouteAdmissionPermit { gate: self.clone() })
    }

    fn publication_generation(&self) -> u64 {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .publication_generation
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn wait_until_request_is_admitted(&self) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while state.active_requests == 0 {
            state = self
                .inner
                .changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn wait_until_publication_is_pending(&self) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while state.transition == StorageClusterRouteTransitionState::Open {
            state = self
                .inner
                .changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }
}

struct StorageClusterRouteAdmissionPermit {
    gate: StorageClusterRouteAdmissionGate,
}

impl Drop for StorageClusterRouteAdmissionPermit {
    fn drop(&mut self) {
        let mut state = self
            .gate
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.active_requests = state
            .active_requests
            .checked_sub(1)
            .expect("route admission permit count must not underflow");
        self.gate.inner.changed.notify_all();
    }
}

struct StorageClusterRoutePublicationGuard {
    gate: StorageClusterRouteAdmissionGate,
}

impl Drop for StorageClusterRoutePublicationGuard {
    fn drop(&mut self) {
        let mut state = self
            .gate
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.publication_generation = state
            .publication_generation
            .checked_add(1)
            .expect("route publication generation must not overflow");
        state.transition = StorageClusterRouteTransitionState::Open;
        self.gate.inner.changed.notify_all();
    }
}

#[derive(Clone)]
struct RetainedActiveRouteRepairFence {
    gate: StorageClusterRouteAdmissionGate,
    publication_generation: u64,
    admitted_lease: LocalRouteMapLeaseSnapshot,
}

/// Non-cloneable request admission for one installed frontend route-map
/// generation.
///
/// The guard prevents a replacement runtime map from being published while
/// the request is admitted. Its absolute deadline is captured at admission,
/// so a later lease renewal cannot extend authority already handed to a
/// long-running request. It deliberately does not dereference to
/// [`StorageCluster`]: storage operations must accept and validate an
/// admission explicitly as those operation boundaries are migrated.
///
/// ```compile_fail
/// use storage::StorageClusterRouteAdmission;
///
/// fn bypass_admission(admission: &StorageClusterRouteAdmission) {
///     let _ = admission.local_node_count();
/// }
/// ```
pub struct StorageClusterRouteAdmission {
    cluster: Arc<StorageCluster>,
    _permit: StorageClusterRouteAdmissionPermit,
    admitted_lease: LocalRouteMapLeaseSnapshot,
}

impl StorageClusterRouteAdmission {
    pub fn require_valid_now(&self) -> Result<(), crate::StoreFailure> {
        self.require_valid_now_raw().map_err(Into::into)
    }

    fn require_valid_now_raw(&self) -> Result<(), StoreError> {
        self.cluster.require_route_map_valid_now()?;
        let local_monotonic_ms = crate::clock::monotonic_time_millis();
        if self
            .cluster
            .local_map
            .route_map_lease_snapshot_is_valid_at(self.admitted_lease, local_monotonic_ms)
        {
            return Ok(());
        }
        Err(StoreError::RouteMapExpired {
            cluster_epoch: self.cluster.cluster_epoch(),
            valid_until_ms: self.admitted_lease.validity.valid_until_ms().unwrap_or(0),
            now_ms: crate::clock::current_time_millis(),
        })
    }

    /// Revalidate this admission immediately before an effect through its
    /// captured runtime-map generation.
    ///
    /// Callers which own a [`StorageClusterRouteHandle`] must first use
    /// [`StorageClusterRouteHandle::require_admission_valid_now`] so the
    /// frontend publication-admission domain is validated as well.
    pub fn require_valid_now_for(
        &self,
        storage_cluster: &Arc<StorageCluster>,
    ) -> Result<(), crate::StoreFailure> {
        self.require_valid_now_for_raw(storage_cluster)
            .map_err(Into::into)
    }

    fn require_valid_now_for_raw(
        &self,
        storage_cluster: &Arc<StorageCluster>,
    ) -> Result<(), StoreError> {
        if !Arc::ptr_eq(&self.cluster, storage_cluster) {
            return Err(StoreError::RouteAdmissionClusterMismatch {
                admitted_epoch: self.cluster.cluster_epoch(),
                operation_epoch: storage_cluster.cluster_epoch(),
            });
        }
        self.require_valid_now_raw()
    }

    /// Return the remaining lifetime of this admission's captured route
    /// authority. A bounded admission is never extended by a later route-map
    /// renewal; callers may use this to bound waits which otherwise perform no
    /// storage effect and therefore have no natural capability revalidation
    /// point.
    pub fn remaining_validity(&self) -> Result<Option<Duration>, crate::StoreFailure> {
        self.remaining_validity_raw().map_err(Into::into)
    }

    fn remaining_validity_raw(&self) -> Result<Option<Duration>, StoreError> {
        self.require_valid_now_raw()?;
        let Some(valid_until_monotonic_ms) = self.admitted_lease.local_valid_until_monotonic_ms
        else {
            return Ok(None);
        };
        let now_monotonic_ms = crate::clock::monotonic_time_millis();
        let Some(remaining_ms) = valid_until_monotonic_ms.checked_sub(now_monotonic_ms) else {
            return Err(StoreError::RouteMapExpired {
                cluster_epoch: self.cluster.cluster_epoch(),
                valid_until_ms: self.admitted_lease.validity.valid_until_ms().unwrap_or(0),
                now_ms: crate::clock::current_time_millis(),
            });
        };
        if remaining_ms == 0 {
            return Err(StoreError::RouteMapExpired {
                cluster_epoch: self.cluster.cluster_epoch(),
                valid_until_ms: self.admitted_lease.validity.valid_until_ms().unwrap_or(0),
                now_ms: crate::clock::current_time_millis(),
            });
        }
        Ok(Some(Duration::from_millis(remaining_ms)))
    }

    pub fn cluster_epoch(&self) -> ClusterEpoch {
        self.cluster.cluster_epoch()
    }

    /// Authority-clock deadline captured atomically with this admission.
    /// Stream-session creation persists it as an immutable cleanup handoff;
    /// unlike the process-monotonic deadline, it remains meaningful after a
    /// process restart and runtime-map publication.
    pub fn authority_valid_until_ms(&self) -> Option<u64> {
        self.admitted_lease.validity.valid_until_ms()
    }

    fn effect_fence(&self) -> AdmittedRouteEffectFence {
        match (
            self.authority_valid_until_ms(),
            self.admitted_lease.local_valid_until_monotonic_ms,
        ) {
            (Some(authority_valid_until_ms), Some(local_valid_until_monotonic_ms)) => {
                AdmittedRouteEffectFence::bounded(
                    self.cluster_epoch(),
                    authority_valid_until_ms,
                    local_valid_until_monotonic_ms,
                )
            }
            (None, None) => AdmittedRouteEffectFence::unbounded(self.cluster_epoch()),
            _ => unreachable!("route admission deadline representations must agree"),
        }
    }

    /// Derive active bucket-metadata authority for one bucket from this
    /// request's admitted runtime-map generation.
    pub fn active_bucket_route<'admission>(
        &'admission self,
        bucket: &BucketName,
    ) -> Result<ActiveBucketRoute<'admission>, crate::StoreFailure> {
        self.require_valid_now_raw()
            .map_err(crate::StoreFailure::from)?;
        Ok(ActiveBucketRoute {
            admission: self,
            bucket: bucket.clone(),
            pg_id: self.cluster.bucket_metadata_pg(bucket),
        })
    }

    /// Derive active authority for an account-scoped scan across every bucket
    /// metadata PG in this request's admitted runtime-map generation.
    pub fn active_bucket_metadata_scan(
        &self,
        owner_canonical_id: &CanonicalUserId,
    ) -> Result<ActiveBucketMetadataScan<'_>, crate::StoreFailure> {
        self.require_valid_now_raw()
            .map_err(crate::StoreFailure::from)?;
        Ok(ActiveBucketMetadataScan {
            admission: self,
            owner_canonical_id: owner_canonical_id.clone(),
        })
    }

    /// Derive active authority for a bucket-scoped scan across every object
    /// metadata PG in this request's admitted runtime-map generation.
    pub fn active_object_metadata_scan(
        &self,
        bucket: &BucketName,
    ) -> Result<ActiveObjectMetadataScan<'_>, crate::StoreFailure> {
        self.require_valid_now_raw()
            .map_err(crate::StoreFailure::from)?;
        Ok(ActiveObjectMetadataScan {
            admission: self,
            bucket: bucket.clone(),
        })
    }

    /// Derive active object-metadata read authority for one object from this
    /// request's admitted runtime-map generation.
    pub fn active_object_read_route<'admission>(
        &'admission self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        snapshot_mode: ObjectReadSnapshotMode,
    ) -> Result<ActiveObjectReadRoute<'admission>, crate::StoreFailure> {
        self.require_valid_now_raw()
            .map_err(crate::StoreFailure::from)?;
        Ok(ActiveObjectReadRoute {
            admission: self,
            bucket: bucket.clone(),
            key: key.clone(),
            version_id,
            snapshot_mode,
            pg_id: self.cluster.object_metadata_pg(bucket, key),
        })
    }

    /// Derive active object-metadata mutation authority for one object from
    /// this request's admitted runtime-map generation.
    pub fn active_object_metadata_mutation_route<'admission>(
        &'admission self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
    ) -> Result<ActiveObjectMetadataMutationRoute<'admission>, crate::StoreFailure> {
        self.require_valid_now_raw()
            .map_err(crate::StoreFailure::from)?;
        Ok(ActiveObjectMetadataMutationRoute {
            admission: self,
            bucket: bucket.clone(),
            key: key.clone(),
            version_id,
            pg_id: self.cluster.object_metadata_pg(bucket, key),
        })
    }

    /// Derive active authority for one PutObject workflow from this request's
    /// admitted runtime-map generation.
    pub fn active_put_object_route<'admission>(
        &'admission self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<ActivePutObjectRoute<'admission>, crate::StoreFailure> {
        self.require_valid_now_raw()
            .map_err(crate::StoreFailure::from)?;
        Ok(ActivePutObjectRoute {
            admission: self,
            bucket: bucket.clone(),
            key: key.clone(),
            bucket_pg_id: self.cluster.bucket_metadata_pg(bucket),
            object_pg_id: self.cluster.object_metadata_pg(bucket, key),
        })
    }

    /// Derive active multipart-control authority for one object from this
    /// request's admitted runtime-map generation.
    pub fn active_multipart_object_route<'admission>(
        &'admission self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<ActiveMultipartObjectRoute<'admission>, crate::StoreFailure> {
        self.require_valid_now_raw()
            .map_err(crate::StoreFailure::from)?;
        Ok(ActiveMultipartObjectRoute {
            admission: self,
            bucket: bucket.clone(),
            key: key.clone(),
            pg_id: self.cluster.object_metadata_pg(bucket, key),
        })
    }

    /// Narrow this admitted request to cleanup authority for one stream-upload
    /// object. The returned capability can only remove an abandoned session
    /// and its staged state from this admitted route generation; it cannot
    /// publish object data or acquire new reservations.
    pub fn retained_stream_upload_cleanup(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<RetainedStreamUploadCleanup, crate::StoreFailure> {
        #[cfg(any(test, feature = "test-hooks"))]
        self.cluster
            .maybe_run_before_retained_stream_cleanup_capability_hook();
        self.require_valid_now_raw()
            .map_err(crate::StoreFailure::from)?;
        let cleanup = RetainedStreamUploadCleanup {
            cluster: Arc::clone(&self.cluster),
            cluster_epoch: self.cluster.cluster_epoch(),
            object_pg_id: self.cluster.object_metadata_pg(bucket, key),
            bucket: bucket.clone(),
            key: key.clone(),
        };
        #[cfg(any(test, feature = "test-hooks"))]
        self.cluster
            .maybe_run_after_retained_stream_cleanup_capability_hook();
        Ok(cleanup)
    }
}
