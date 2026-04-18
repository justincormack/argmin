use super::*;

/// PG guards held while an object metadata snapshot is live.
///
/// Read-side payload access relies on generation-scoped leases, so current
/// object read paths only need the metadata PG guard.
pub(super) struct ObjectPgGuards<'a> {
    pub(super) meta: MutexGuard<'a, storage::PgStore>,
}

impl<'a> ObjectPgGuards<'a> {
    pub(super) fn new(meta: MutexGuard<'a, storage::PgStore>) -> Self {
        Self { meta }
    }

    pub(super) fn meta(&self) -> &storage::PgStore {
        &self.meta
    }
}

/// Ordered bucket/object PG guards for mixed metadata loads.
///
/// The underlying storage helper acquires the physical PG mutexes in ascending
/// PG ID order, but returns them in logical bucket/object order so call sites
/// cannot accidentally swap roles.
pub(super) struct BucketObjectPgGuards<'a> {
    bucket: MutexGuard<'a, storage::PgStore>,
    object: Option<MutexGuard<'a, storage::PgStore>>,
}

impl<'a> BucketObjectPgGuards<'a> {
    pub(super) fn new(
        bucket: MutexGuard<'a, storage::PgStore>,
        object: Option<MutexGuard<'a, storage::PgStore>>,
    ) -> Self {
        Self { bucket, object }
    }

    pub(super) fn bucket(&self) -> &storage::PgStore {
        &self.bucket
    }

    pub(super) fn object(&self) -> &storage::PgStore {
        match self.object.as_ref() {
            Some(object) => object,
            None => &self.bucket,
        }
    }

    pub(super) fn into_object_guards(self) -> ObjectPgGuards<'a> {
        let Self { bucket, object } = self;
        match object {
            Some(object) => ObjectPgGuards::new(object),
            None => ObjectPgGuards::new(bucket),
        }
    }
}

/// Ordered metadata/shard PG guards for object write publication.
///
/// The helper keeps logical meta/shard roles explicit even when both roles map
/// to the same physical PG.
pub(super) struct TwoPgGuards<'a> {
    meta: MutexGuard<'a, storage::PgStore>,
    shard: Option<MutexGuard<'a, storage::PgStore>>,
}

impl<'a> TwoPgGuards<'a> {
    pub(super) fn new(
        meta: MutexGuard<'a, storage::PgStore>,
        shard: Option<MutexGuard<'a, storage::PgStore>>,
    ) -> Self {
        Self { meta, shard }
    }

    pub(super) fn same_pg(&self) -> bool {
        self.shard.is_none()
    }

    pub(super) fn meta(&self) -> &storage::PgStore {
        &self.meta
    }

    pub(super) fn shard(&self) -> &storage::PgStore {
        match self.shard.as_ref() {
            Some(shard) => shard,
            None => &self.meta,
        }
    }
}

pub(super) struct LockedReadObject<'a> {
    pub(super) record: StoredObject,
    pub(super) pgs: ObjectPgGuards<'a>,
}

#[derive(Debug, Clone)]
pub(super) struct SnapshottedMultipartPart {
    pub(super) record: ObjectPartRecord,
    pub(super) object_offset_start: usize,
    pub(super) segments: Vec<SegmentPayloadRecord>,
}

#[derive(Debug, Clone)]
pub(super) enum StaleObjectPayload {
    Segments {
        generation_id: GenerationId,
        segments: Vec<ObjectSegmentRecord>,
    },
    Multipart {
        generation_id: GenerationId,
        parts: Vec<ObjectPartRecord>,
        streaming_segments: Vec<MultipartPartSegmentRecord>,
    },
}

/// The coordinator ties together EC, storage, and metadata.
pub(super) struct ReclaimSweeper {
    pub(super) storage_node: Arc<SharedStorageNode>,
    pub(super) stop: Arc<AtomicBool>,
    pub(super) handle: Option<JoinHandle<()>>,
}

pub(super) struct LifecycleSweeper {
    pub(super) stop: Arc<AtomicBool>,
    pub(super) handle: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Debug, Clone)]
pub(super) struct CachedBucketPolicy {
    pub(super) generation: u64,
    pub(super) policy: Arc<auth::BucketPolicy>,
}

#[derive(Debug, Clone)]
pub(super) struct CachedBucketLifecycle {
    pub(super) generation: u64,
    pub(super) config: Arc<BucketLifecycleConfiguration>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct LifecycleSweepStats {
    pub(super) scanned_buckets: u64,
    pub(super) expired_current_objects: u64,
    pub(super) expired_noncurrent_versions: u64,
    pub(super) expired_delete_markers: u64,
    pub(super) aborted_multipart_uploads: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DeletedLiveObjectKind {
    Segments,
    Multipart,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct DeletedLiveObjectReclaim {
    pub(super) generation_id: GenerationId,
    pub(super) kind: DeletedLiveObjectKind,
}

pub(super) enum ObjectAclAuthorization<'a> {
    ReadWithPolicy(auth::PolicyAction),
    WriteWithPolicy {
        action: auth::PolicyAction,
        policy_context: PutObjectPolicyContext<'a>,
    },
}
