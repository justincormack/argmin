use super::test_support::open_test_storage_cluster;
use super::*;
use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, TestCaseError, TestCaseResult};
use std::fmt::Write as _;
use std::path::Path;
use storage::{
    MultipartReclaimPartRecord, MultipartReclaimRecord, ObjectSegmentsReclaimRecord,
    ObjectSegmentsReclaimSegmentRecord, PgTopology, ReclaimWorkItem,
};

const TRACE_BUCKET: &str = "bucket";
const TRACE_KEY: &str = "key";
const TRACE_KEY_A: &str = "a";
const TRACE_KEY_B: &str = "b";
const TRACE_MAX_OPS: usize = 12;

fn trace_generation_id() -> GenerationId {
    GenerationId::new(1).expect("constant generation id must be valid")
}

fn trace_generation_id_new() -> GenerationId {
    GenerationId::new(2).expect("constant generation id must be valid")
}

fn make_test_read_runtime(dir: &Path) -> ReadRuntime {
    let storage_cluster = open_test_storage_cluster(dir, &[0]);
    let ec_shape = storage_cluster.default_payload_ec_shape();
    ReadRuntime {
        storage_node: storage_cluster,
        #[cfg(test)]
        pg_topology: PgTopology::new(&[0]).unwrap(),
        payload_buffer_pool: PayloadBufferPool::new(ec_shape),
        sse_c_validator: None,
        managed_key_provider: None,
    }
}

fn trace_object_segments_reclaim(
    runtime: &ReadRuntime,
    bucket: &str,
    key: &str,
    generation_id: GenerationId,
    created_at: u64,
) -> ObjectSegmentsReclaimRecord {
    let bucket_name = trusted_bucket_name(bucket);
    let object_key = trusted_object_key(key);
    let data_pg_id = runtime
        .pg_topology
        .object_generation_segment_data_pg(&bucket_name, &object_key, generation_id, 0)
        .get();

    ObjectSegmentsReclaimRecord {
        bucket: bucket_name,
        key: object_key,
        generation_id,
        created_at,
        segments: vec![ObjectSegmentsReclaimSegmentRecord {
            segment_index: 0,
            segment_okh: object_key_hash(bucket, key),
            segment_vid: generation_id,
            data_pg_id,
            ec: EcShape { k: 4, m: 2 },
        }],
    }
}

fn trace_multipart_reclaim(
    runtime: &ReadRuntime,
    bucket: &str,
    key: &str,
    generation_id: GenerationId,
    created_at: u64,
) -> MultipartReclaimRecord {
    let bucket_name = trusted_bucket_name(bucket);
    let object_key = trusted_object_key(key);
    let data_pg_id = runtime
        .pg_topology
        .object_generation_multipart_part_data_pg(&bucket_name, &object_key, generation_id, 1)
        .get();

    MultipartReclaimRecord {
        bucket: bucket_name,
        key: object_key,
        generation_id,
        created_at,
        parts: vec![MultipartReclaimPartRecord::ShardSet {
            part_number: 1,
            part_okh: object_key_hash(bucket, key),
            part_vid: generation_id,
            data_pg_id,
            ec: EcShape { k: 4, m: 2 },
        }],
    }
}

fn seed_deleting_bucket(runtime: &ReadRuntime) {
    runtime
        .storage_node
        .test_create_deleting_bucket(&trusted_bucket_name(TRACE_BUCKET))
        .unwrap();
}

#[derive(Debug, Clone)]
enum ReclaimTraceSeed {
    Choice(u8),
}

#[derive(Debug, Clone)]
enum ReclaimKindTraceSeed {
    Choice(u8),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TraceGeneration {
    Old,
    New,
}

impl TraceGeneration {
    fn generation_id(self) -> GenerationId {
        match self {
            Self::Old => trace_generation_id(),
            Self::New => trace_generation_id_new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TraceKey {
    A,
    B,
}

impl TraceKey {
    fn key(self) -> &'static str {
        match self {
            Self::A => TRACE_KEY_A,
            Self::B => TRACE_KEY_B,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TraceReclaimKind {
    Segments,
    Multipart,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ReclaimTraceOp {
    SeedMetadata,
    AcquireLease,
    ReleaseLease,
    EnqueueObjectReclaim,
    WorkerObjectStep,
    WorkerBucketDeleteStep,
    ExpectNoWork,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ReclaimKindTraceOp {
    SeedSegmentsMetadata,
    SeedMultipartMetadata,
    AcquireLease,
    ReleaseLease,
    EnqueueObjectReclaim,
    WorkerObjectStep,
    WorkerBucketDeleteStep,
    ExpectNoWork,
}

impl std::fmt::Display for ReclaimTraceOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SeedMetadata => write!(f, "seed-metadata"),
            Self::AcquireLease => write!(f, "acquire-lease"),
            Self::ReleaseLease => write!(f, "release-lease"),
            Self::EnqueueObjectReclaim => write!(f, "enqueue-object-reclaim"),
            Self::WorkerObjectStep => write!(f, "worker-object-step"),
            Self::WorkerBucketDeleteStep => write!(f, "worker-bucket-delete-step"),
            Self::ExpectNoWork => write!(f, "expect-no-work"),
        }
    }
}

impl std::fmt::Display for ReclaimKindTraceOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SeedSegmentsMetadata => write!(f, "seed-segments-metadata"),
            Self::SeedMultipartMetadata => write!(f, "seed-multipart-metadata"),
            Self::AcquireLease => write!(f, "acquire-lease"),
            Self::ReleaseLease => write!(f, "release-lease"),
            Self::EnqueueObjectReclaim => write!(f, "enqueue-object-reclaim"),
            Self::WorkerObjectStep => write!(f, "worker-object-step"),
            Self::WorkerBucketDeleteStep => write!(f, "worker-bucket-delete-step"),
            Self::ExpectNoWork => write!(f, "expect-no-work"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReclaimTraceModel {
    metadata_exists: bool,
    lease_held: bool,
    object_work_queued: bool,
    bucket_delete_queued: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReclaimKindTraceModel {
    reclaim_kind: Option<TraceReclaimKind>,
    lease_held: bool,
    object_work_queued: bool,
    bucket_delete_queued: bool,
}

impl ReclaimTraceModel {
    fn new() -> Self {
        Self {
            metadata_exists: false,
            lease_held: false,
            object_work_queued: false,
            bucket_delete_queued: false,
        }
    }

    fn legal_ops(&self) -> Vec<ReclaimTraceOp> {
        use ReclaimTraceOp::*;
        let mut ops = Vec::new();
        if !self.metadata_exists {
            ops.push(SeedMetadata);
        }
        if self.lease_held {
            ops.push(ReleaseLease);
        } else {
            ops.push(AcquireLease);
        }
        if self.metadata_exists {
            ops.push(EnqueueObjectReclaim);
        }
        if self.object_work_queued {
            ops.push(WorkerObjectStep);
        }
        if self.bucket_delete_queued && !self.object_work_queued {
            ops.push(WorkerBucketDeleteStep);
        }
        if !self.object_work_queued && !self.bucket_delete_queued {
            ops.push(ExpectNoWork);
        }
        ops
    }

    fn apply(&mut self, op: &ReclaimTraceOp) {
        use ReclaimTraceOp::*;
        match op {
            SeedMetadata => {
                self.metadata_exists = true;
            }
            AcquireLease => {
                self.lease_held = true;
            }
            ReleaseLease => {
                self.lease_held = false;
                if self.metadata_exists {
                    self.object_work_queued = true;
                }
            }
            EnqueueObjectReclaim => {
                self.object_work_queued = true;
            }
            WorkerObjectStep => {
                self.object_work_queued = false;
                if self.metadata_exists && !self.lease_held {
                    self.metadata_exists = false;
                    self.bucket_delete_queued = true;
                }
            }
            WorkerBucketDeleteStep => {
                self.bucket_delete_queued = false;
            }
            ExpectNoWork => {}
        }
    }
}

impl ReclaimKindTraceModel {
    fn new() -> Self {
        Self {
            reclaim_kind: None,
            lease_held: false,
            object_work_queued: false,
            bucket_delete_queued: false,
        }
    }

    fn legal_ops(&self) -> Vec<ReclaimKindTraceOp> {
        use ReclaimKindTraceOp::*;
        let mut ops = Vec::new();
        if self.reclaim_kind.is_none() {
            ops.push(SeedSegmentsMetadata);
            ops.push(SeedMultipartMetadata);
        }
        if self.lease_held {
            ops.push(ReleaseLease);
        } else {
            ops.push(AcquireLease);
        }
        if self.reclaim_kind.is_some() {
            ops.push(EnqueueObjectReclaim);
        }
        if self.object_work_queued {
            ops.push(WorkerObjectStep);
        }
        if self.bucket_delete_queued && !self.object_work_queued {
            ops.push(WorkerBucketDeleteStep);
        }
        if !self.object_work_queued && !self.bucket_delete_queued {
            ops.push(ExpectNoWork);
        }
        ops
    }

    fn apply(&mut self, op: &ReclaimKindTraceOp) {
        use ReclaimKindTraceOp::*;
        match op {
            SeedSegmentsMetadata => self.reclaim_kind = Some(TraceReclaimKind::Segments),
            SeedMultipartMetadata => self.reclaim_kind = Some(TraceReclaimKind::Multipart),
            AcquireLease => self.lease_held = true,
            ReleaseLease => {
                self.lease_held = false;
                if self.reclaim_kind.is_some() {
                    self.object_work_queued = true;
                }
            }
            EnqueueObjectReclaim => self.object_work_queued = true,
            WorkerObjectStep => {
                self.object_work_queued = false;
                if self.reclaim_kind.is_some() && !self.lease_held {
                    self.reclaim_kind = None;
                    self.bucket_delete_queued = true;
                }
            }
            WorkerBucketDeleteStep => self.bucket_delete_queued = false,
            ExpectNoWork => {}
        }
    }
}

fn reclaim_trace_strategy() -> BoxedStrategy<Vec<ReclaimTraceOp>> {
    proptest::collection::vec(
        any::<u8>().prop_map(ReclaimTraceSeed::Choice),
        0..=TRACE_MAX_OPS,
    )
    .prop_map(|seeds| {
        let mut model = ReclaimTraceModel::new();
        let mut ops = Vec::with_capacity(seeds.len());
        for seed in seeds {
            let ReclaimTraceSeed::Choice(choice) = seed;
            let legal = model.legal_ops();
            let op = legal[(choice as usize) % legal.len()].clone();
            model.apply(&op);
            ops.push(op);
        }
        ops
    })
    .boxed()
}

fn reclaim_kind_trace_strategy() -> BoxedStrategy<Vec<ReclaimKindTraceOp>> {
    proptest::collection::vec(
        any::<u8>().prop_map(ReclaimKindTraceSeed::Choice),
        0..=TRACE_MAX_OPS,
    )
    .prop_map(|seeds| {
        let mut model = ReclaimKindTraceModel::new();
        let mut ops = Vec::with_capacity(seeds.len());
        for seed in seeds {
            let ReclaimKindTraceSeed::Choice(choice) = seed;
            let legal = model.legal_ops();
            let op = legal[(choice as usize) % legal.len()].clone();
            model.apply(&op);
            ops.push(op);
        }
        ops
    })
    .boxed()
}

#[derive(Debug, Clone)]
enum TwoGenerationReclaimTraceSeed {
    Choice(u8),
}

#[derive(Debug, Clone)]
enum TwoKeyReclaimTraceSeed {
    Choice(u8),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TwoGenerationReclaimTraceOp {
    SeedBucketDelete,
    SeedOldMetadata,
    SeedNewMetadata,
    AcquireOldLease,
    ReleaseOldLease,
    AcquireNewLease,
    ReleaseNewLease,
    EnqueueOldReclaim,
    EnqueueNewReclaim,
    WorkerObjectStep,
    WorkerBucketDeleteStep,
    ExpectNoWork,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TwoKeyReclaimTraceOp {
    SeedBucketDelete,
    SeedKeyAMetadata,
    SeedKeyBMetadata,
    AcquireKeyALease,
    ReleaseKeyALease,
    AcquireKeyBLease,
    ReleaseKeyBLease,
    EnqueueKeyAReclaim,
    EnqueueKeyBReclaim,
    WorkerObjectStep,
    WorkerBucketDeleteStep,
    ExpectNoWork,
}

impl std::fmt::Display for TwoGenerationReclaimTraceOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SeedBucketDelete => write!(f, "seed-bucket-delete"),
            Self::SeedOldMetadata => write!(f, "seed-old-metadata"),
            Self::SeedNewMetadata => write!(f, "seed-new-metadata"),
            Self::AcquireOldLease => write!(f, "acquire-old-lease"),
            Self::ReleaseOldLease => write!(f, "release-old-lease"),
            Self::AcquireNewLease => write!(f, "acquire-new-lease"),
            Self::ReleaseNewLease => write!(f, "release-new-lease"),
            Self::EnqueueOldReclaim => write!(f, "enqueue-old-reclaim"),
            Self::EnqueueNewReclaim => write!(f, "enqueue-new-reclaim"),
            Self::WorkerObjectStep => write!(f, "worker-object-step"),
            Self::WorkerBucketDeleteStep => write!(f, "worker-bucket-delete-step"),
            Self::ExpectNoWork => write!(f, "expect-no-work"),
        }
    }
}

impl std::fmt::Display for TwoKeyReclaimTraceOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SeedBucketDelete => write!(f, "seed-bucket-delete"),
            Self::SeedKeyAMetadata => write!(f, "seed-key-a-metadata"),
            Self::SeedKeyBMetadata => write!(f, "seed-key-b-metadata"),
            Self::AcquireKeyALease => write!(f, "acquire-key-a-lease"),
            Self::ReleaseKeyALease => write!(f, "release-key-a-lease"),
            Self::AcquireKeyBLease => write!(f, "acquire-key-b-lease"),
            Self::ReleaseKeyBLease => write!(f, "release-key-b-lease"),
            Self::EnqueueKeyAReclaim => write!(f, "enqueue-key-a-reclaim"),
            Self::EnqueueKeyBReclaim => write!(f, "enqueue-key-b-reclaim"),
            Self::WorkerObjectStep => write!(f, "worker-object-step"),
            Self::WorkerBucketDeleteStep => write!(f, "worker-bucket-delete-step"),
            Self::ExpectNoWork => write!(f, "expect-no-work"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TwoGenerationReclaimTraceModel {
    bucket_exists: bool,
    bucket_deleting: bool,
    old_metadata_exists: bool,
    new_metadata_exists: bool,
    old_lease_held: bool,
    new_lease_held: bool,
    object_queue: Vec<TraceGeneration>,
    bucket_delete_queued: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TwoKeyReclaimTraceModel {
    bucket_exists: bool,
    bucket_deleting: bool,
    key_a_metadata_exists: bool,
    key_b_metadata_exists: bool,
    key_a_lease_held: bool,
    key_b_lease_held: bool,
    object_queue: Vec<TraceKey>,
    bucket_delete_queued: bool,
}

impl TwoGenerationReclaimTraceModel {
    fn new() -> Self {
        Self {
            bucket_exists: false,
            bucket_deleting: false,
            old_metadata_exists: false,
            new_metadata_exists: false,
            old_lease_held: false,
            new_lease_held: false,
            object_queue: Vec::new(),
            bucket_delete_queued: false,
        }
    }

    fn legal_ops(&self) -> Vec<TwoGenerationReclaimTraceOp> {
        use TwoGenerationReclaimTraceOp::*;
        let mut ops = Vec::new();
        if !self.bucket_exists && !self.bucket_delete_queued {
            ops.push(SeedBucketDelete);
        }
        if !self.old_metadata_exists {
            ops.push(SeedOldMetadata);
        }
        if !self.new_metadata_exists {
            ops.push(SeedNewMetadata);
        }
        if self.old_lease_held {
            ops.push(ReleaseOldLease);
        } else {
            ops.push(AcquireOldLease);
        }
        if self.new_lease_held {
            ops.push(ReleaseNewLease);
        } else {
            ops.push(AcquireNewLease);
        }
        if self.old_metadata_exists {
            ops.push(EnqueueOldReclaim);
        }
        if self.new_metadata_exists {
            ops.push(EnqueueNewReclaim);
        }
        if !self.object_queue.is_empty() {
            ops.push(WorkerObjectStep);
        }
        if self.bucket_delete_queued && self.object_queue.is_empty() {
            ops.push(WorkerBucketDeleteStep);
        }
        if self.object_queue.is_empty() && !self.bucket_delete_queued {
            ops.push(ExpectNoWork);
        }
        ops
    }

    fn enqueue_generation(&mut self, generation: TraceGeneration) {
        if !self.object_queue.contains(&generation) {
            self.object_queue.push(generation);
        }
    }

    fn current_bucket_root(&self) -> Option<TraceGeneration> {
        if self.old_metadata_exists {
            Some(TraceGeneration::Old)
        } else if self.new_metadata_exists {
            Some(TraceGeneration::New)
        } else {
            None
        }
    }

    fn apply(&mut self, op: &TwoGenerationReclaimTraceOp) {
        use TwoGenerationReclaimTraceOp::*;
        match op {
            SeedBucketDelete => {
                self.bucket_exists = true;
                self.bucket_deleting = true;
                self.bucket_delete_queued = true;
            }
            SeedOldMetadata => self.old_metadata_exists = true,
            SeedNewMetadata => self.new_metadata_exists = true,
            AcquireOldLease => self.old_lease_held = true,
            ReleaseOldLease => {
                self.old_lease_held = false;
                if self.old_metadata_exists {
                    self.enqueue_generation(TraceGeneration::Old);
                }
            }
            AcquireNewLease => self.new_lease_held = true,
            ReleaseNewLease => {
                self.new_lease_held = false;
                if self.new_metadata_exists {
                    self.enqueue_generation(TraceGeneration::New);
                }
            }
            EnqueueOldReclaim => self.enqueue_generation(TraceGeneration::Old),
            EnqueueNewReclaim => self.enqueue_generation(TraceGeneration::New),
            WorkerObjectStep => {
                let generation = self.object_queue.remove(0);
                match generation {
                    TraceGeneration::Old if self.old_metadata_exists && !self.old_lease_held => {
                        self.old_metadata_exists = false;
                        self.bucket_delete_queued = true;
                    }
                    TraceGeneration::New if self.new_metadata_exists && !self.new_lease_held => {
                        self.new_metadata_exists = false;
                        self.bucket_delete_queued = true;
                    }
                    _ => {}
                }
            }
            WorkerBucketDeleteStep => {
                self.bucket_delete_queued = false;
                if self.bucket_deleting {
                    match self.current_bucket_root() {
                        Some(TraceGeneration::Old) if !self.old_lease_held => {
                            self.enqueue_generation(TraceGeneration::Old);
                        }
                        Some(TraceGeneration::New) if !self.new_lease_held => {
                            self.enqueue_generation(TraceGeneration::New);
                        }
                        _ => {}
                    }
                    if !self.old_metadata_exists && !self.new_metadata_exists {
                        self.bucket_exists = false;
                        self.bucket_deleting = false;
                    }
                }
            }
            ExpectNoWork => {}
        }
    }
}

impl TwoKeyReclaimTraceModel {
    fn new() -> Self {
        Self {
            bucket_exists: false,
            bucket_deleting: false,
            key_a_metadata_exists: false,
            key_b_metadata_exists: false,
            key_a_lease_held: false,
            key_b_lease_held: false,
            object_queue: Vec::new(),
            bucket_delete_queued: false,
        }
    }

    fn legal_ops(&self) -> Vec<TwoKeyReclaimTraceOp> {
        use TwoKeyReclaimTraceOp::*;
        let mut ops = Vec::new();
        if !self.bucket_exists && !self.bucket_delete_queued {
            ops.push(SeedBucketDelete);
        }
        if !self.key_a_metadata_exists {
            ops.push(SeedKeyAMetadata);
        }
        if !self.key_b_metadata_exists {
            ops.push(SeedKeyBMetadata);
        }
        if self.key_a_lease_held {
            ops.push(ReleaseKeyALease);
        } else {
            ops.push(AcquireKeyALease);
        }
        if self.key_b_lease_held {
            ops.push(ReleaseKeyBLease);
        } else {
            ops.push(AcquireKeyBLease);
        }
        if self.key_a_metadata_exists {
            ops.push(EnqueueKeyAReclaim);
        }
        if self.key_b_metadata_exists {
            ops.push(EnqueueKeyBReclaim);
        }
        if !self.object_queue.is_empty() {
            ops.push(WorkerObjectStep);
        }
        if self.bucket_delete_queued && self.object_queue.is_empty() {
            ops.push(WorkerBucketDeleteStep);
        }
        if self.object_queue.is_empty() && !self.bucket_delete_queued {
            ops.push(ExpectNoWork);
        }
        ops
    }

    fn enqueue_key(&mut self, key: TraceKey) {
        if !self.object_queue.contains(&key) {
            self.object_queue.push(key);
        }
    }

    fn current_bucket_root(&self) -> Option<TraceKey> {
        if self.key_a_metadata_exists {
            Some(TraceKey::A)
        } else if self.key_b_metadata_exists {
            Some(TraceKey::B)
        } else {
            None
        }
    }

    fn apply(&mut self, op: &TwoKeyReclaimTraceOp) {
        use TwoKeyReclaimTraceOp::*;
        match op {
            SeedBucketDelete => {
                self.bucket_exists = true;
                self.bucket_deleting = true;
                self.bucket_delete_queued = true;
            }
            SeedKeyAMetadata => self.key_a_metadata_exists = true,
            SeedKeyBMetadata => self.key_b_metadata_exists = true,
            AcquireKeyALease => self.key_a_lease_held = true,
            ReleaseKeyALease => {
                self.key_a_lease_held = false;
                if self.key_a_metadata_exists {
                    self.enqueue_key(TraceKey::A);
                }
            }
            AcquireKeyBLease => self.key_b_lease_held = true,
            ReleaseKeyBLease => {
                self.key_b_lease_held = false;
                if self.key_b_metadata_exists {
                    self.enqueue_key(TraceKey::B);
                }
            }
            EnqueueKeyAReclaim => self.enqueue_key(TraceKey::A),
            EnqueueKeyBReclaim => self.enqueue_key(TraceKey::B),
            WorkerObjectStep => {
                let key = self.object_queue.remove(0);
                match key {
                    TraceKey::A if self.key_a_metadata_exists && !self.key_a_lease_held => {
                        self.key_a_metadata_exists = false;
                        self.bucket_delete_queued = true;
                    }
                    TraceKey::B if self.key_b_metadata_exists && !self.key_b_lease_held => {
                        self.key_b_metadata_exists = false;
                        self.bucket_delete_queued = true;
                    }
                    _ => {}
                }
            }
            WorkerBucketDeleteStep => {
                self.bucket_delete_queued = false;
                if self.bucket_deleting {
                    match self.current_bucket_root() {
                        Some(TraceKey::A) if !self.key_a_lease_held => {
                            self.enqueue_key(TraceKey::A)
                        }
                        Some(TraceKey::B) if !self.key_b_lease_held => {
                            self.enqueue_key(TraceKey::B)
                        }
                        _ => {}
                    }
                    if !self.key_a_metadata_exists && !self.key_b_metadata_exists {
                        self.bucket_exists = false;
                        self.bucket_deleting = false;
                    }
                }
            }
            ExpectNoWork => {}
        }
    }
}

fn two_generation_reclaim_trace_strategy() -> BoxedStrategy<Vec<TwoGenerationReclaimTraceOp>> {
    proptest::collection::vec(
        any::<u8>().prop_map(TwoGenerationReclaimTraceSeed::Choice),
        0..=TRACE_MAX_OPS,
    )
    .prop_map(|seeds| {
        let mut model = TwoGenerationReclaimTraceModel::new();
        let mut ops = Vec::with_capacity(seeds.len());
        for seed in seeds {
            let TwoGenerationReclaimTraceSeed::Choice(choice) = seed;
            let legal = model.legal_ops();
            let op = legal[(choice as usize) % legal.len()].clone();
            model.apply(&op);
            ops.push(op);
        }
        ops
    })
    .boxed()
}

fn two_key_reclaim_trace_strategy() -> BoxedStrategy<Vec<TwoKeyReclaimTraceOp>> {
    proptest::collection::vec(
        any::<u8>().prop_map(TwoKeyReclaimTraceSeed::Choice),
        0..=TRACE_MAX_OPS,
    )
    .prop_map(|seeds| {
        let mut model = TwoKeyReclaimTraceModel::new();
        let mut ops = Vec::with_capacity(seeds.len());
        for seed in seeds {
            let TwoKeyReclaimTraceSeed::Choice(choice) = seed;
            let legal = model.legal_ops();
            let op = legal[(choice as usize) % legal.len()].clone();
            model.apply(&op);
            ops.push(op);
        }
        ops
    })
    .boxed()
}

struct ReclaimTraceHarness {
    runtime: ReadRuntime,
    lease: Option<PayloadLease>,
}

struct ReclaimKindTraceHarness {
    runtime: ReadRuntime,
    lease: Option<PayloadLease>,
}

impl ReclaimTraceHarness {
    fn new(runtime: ReadRuntime) -> Self {
        Self {
            runtime,
            lease: None,
        }
    }

    fn execute(&mut self, op: &ReclaimTraceOp) -> TestCaseResult {
        use ReclaimTraceOp::*;
        match op {
            SeedMetadata => {
                self.seed_segments_metadata()?;
            }
            AcquireLease => {
                self.lease = Some(self.runtime.acquire_object_payload_lease(
                    TRACE_BUCKET,
                    TRACE_KEY,
                    trace_generation_id(),
                ));
            }
            ReleaseLease => {
                drop(self.lease.take().unwrap());
            }
            EnqueueObjectReclaim => {
                self.runtime.enqueue_object_payload_reclaim(
                    TRACE_BUCKET,
                    TRACE_KEY,
                    trace_generation_id(),
                );
            }
            WorkerObjectStep => {
                let work = self.take_next_work()?;
                match work {
                    Some(ReclaimWorkItem::ObjectPayload((bucket, key, generation_id)))
                        if bucket == trusted_bucket_name(TRACE_BUCKET)
                            && key == trusted_object_key(TRACE_KEY)
                            && generation_id == trace_generation_id() => {}
                    Some(ReclaimWorkItem::BucketDelete(_)) => {
                        return Err(TestCaseError::fail(
                            "expected object reclaim work item, got bucket delete",
                        ))
                    }
                    None => {
                        return Err(TestCaseError::fail(
                            "expected object reclaim work item, got none",
                        ))
                    }
                    Some(ReclaimWorkItem::ObjectPayload(_)) => {
                        return Err(TestCaseError::fail(
                            "expected object reclaim work item for the trace generation",
                        ))
                    }
                }
                self.runtime
                    .try_reclaim_object_payload(TRACE_BUCKET, TRACE_KEY, trace_generation_id())
                    .map_err(|err| {
                        TestCaseError::fail(format!(
                            "try_reclaim_object_payload failed unexpectedly: {err:?}"
                        ))
                    })?;
            }
            WorkerBucketDeleteStep => {
                let work = self.take_next_work()?;
                match work {
                    Some(ReclaimWorkItem::BucketDelete(bucket))
                        if bucket == trusted_bucket_name(TRACE_BUCKET) => {}
                    Some(ReclaimWorkItem::ObjectPayload(_)) => {
                        return Err(TestCaseError::fail(
                            "expected bucket delete finalize work item, got object reclaim",
                        ))
                    }
                    None => {
                        return Err(TestCaseError::fail(
                            "expected bucket delete finalize work item, got none",
                        ))
                    }
                    Some(ReclaimWorkItem::BucketDelete(_)) => {
                        return Err(TestCaseError::fail(
                            "expected bucket delete finalize work item for the trace bucket",
                        ))
                    }
                }
            }
            ExpectNoWork => {
                let work = self.take_next_work()?;
                if work.is_some() {
                    return Err(TestCaseError::fail("expected no queued reclaim work"));
                }
            }
        }
        Ok(())
    }

    fn seed_segments_metadata(&self) -> TestCaseResult {
        self.runtime
            .storage_node
            .test_put_object_segments_reclaim(
                &trusted_bucket_name(TRACE_BUCKET),
                &trusted_object_key(TRACE_KEY),
                &trace_object_segments_reclaim(
                    &self.runtime,
                    TRACE_BUCKET,
                    TRACE_KEY,
                    trace_generation_id(),
                    1,
                ),
            )
            .map_err(|err| {
                TestCaseError::fail(format!("test_put_object_segments_reclaim failed: {err:?}"))
            })?;
        Ok(())
    }

    fn metadata_exists(&self) -> bool {
        self.runtime
            .storage_node
            .test_payload_reclaim_exists(
                &trusted_bucket_name(TRACE_BUCKET),
                &trusted_object_key(TRACE_KEY),
                trace_generation_id(),
            )
            .unwrap()
    }

    fn lease_count(&self) -> usize {
        self.runtime.storage_node.object_payload_lease_count(
            &trusted_bucket_name(TRACE_BUCKET),
            &trusted_object_key(TRACE_KEY),
            trace_generation_id(),
        )
    }

    fn take_next_work(&self) -> Result<Option<ReclaimWorkItem>, TestCaseError> {
        Ok(self.runtime.storage_node.try_take_reclaim_work())
    }
}

impl ReclaimKindTraceHarness {
    fn new(runtime: ReadRuntime) -> Self {
        Self {
            runtime,
            lease: None,
        }
    }

    fn execute(&mut self, op: &ReclaimKindTraceOp) -> TestCaseResult {
        use ReclaimKindTraceOp::*;
        match op {
            SeedSegmentsMetadata => self.seed_segments_metadata()?,
            SeedMultipartMetadata => self.seed_multipart_metadata()?,
            AcquireLease => {
                self.lease = Some(self.runtime.acquire_object_payload_lease(
                    TRACE_BUCKET,
                    TRACE_KEY,
                    trace_generation_id(),
                ));
            }
            ReleaseLease => drop(self.lease.take().unwrap()),
            EnqueueObjectReclaim => {
                self.runtime.enqueue_object_payload_reclaim(
                    TRACE_BUCKET,
                    TRACE_KEY,
                    trace_generation_id(),
                );
            }
            WorkerObjectStep => {
                let work = self.take_next_work()?;
                match work {
                    Some(ReclaimWorkItem::ObjectPayload((bucket, key, generation_id)))
                        if bucket == trusted_bucket_name(TRACE_BUCKET)
                            && key == trusted_object_key(TRACE_KEY)
                            && generation_id == trace_generation_id() => {}
                    Some(ReclaimWorkItem::BucketDelete(_)) => {
                        return Err(TestCaseError::fail(
                            "expected object reclaim work item, got bucket delete",
                        ))
                    }
                    None => {
                        return Err(TestCaseError::fail(
                            "expected object reclaim work item, got none",
                        ))
                    }
                    Some(ReclaimWorkItem::ObjectPayload(_)) => {
                        return Err(TestCaseError::fail(
                            "expected object reclaim work item for the trace generation",
                        ))
                    }
                }
                self.runtime
                    .try_reclaim_object_payload(TRACE_BUCKET, TRACE_KEY, trace_generation_id())
                    .map_err(|err| {
                        TestCaseError::fail(format!(
                            "try_reclaim_object_payload failed unexpectedly: {err:?}"
                        ))
                    })?;
            }
            WorkerBucketDeleteStep => {
                let work = self.take_next_work()?;
                match work {
                    Some(ReclaimWorkItem::BucketDelete(bucket))
                        if bucket == trusted_bucket_name(TRACE_BUCKET) => {}
                    Some(ReclaimWorkItem::ObjectPayload(_)) => {
                        return Err(TestCaseError::fail(
                            "expected bucket delete finalize work item, got object reclaim",
                        ))
                    }
                    None => {
                        return Err(TestCaseError::fail(
                            "expected bucket delete finalize work item, got none",
                        ))
                    }
                    Some(ReclaimWorkItem::BucketDelete(_)) => {
                        return Err(TestCaseError::fail(
                            "expected bucket delete finalize work item for the trace bucket",
                        ))
                    }
                }
            }
            ExpectNoWork => {
                if self.take_next_work()?.is_some() {
                    return Err(TestCaseError::fail("expected no queued reclaim work"));
                }
            }
        }
        Ok(())
    }

    fn seed_segments_metadata(&self) -> TestCaseResult {
        self.runtime
            .storage_node
            .test_put_object_segments_reclaim(
                &trusted_bucket_name(TRACE_BUCKET),
                &trusted_object_key(TRACE_KEY),
                &trace_object_segments_reclaim(
                    &self.runtime,
                    TRACE_BUCKET,
                    TRACE_KEY,
                    trace_generation_id(),
                    1,
                ),
            )
            .map_err(|err| {
                TestCaseError::fail(format!("test_put_object_segments_reclaim failed: {err:?}"))
            })?;
        Ok(())
    }

    fn seed_multipart_metadata(&self) -> TestCaseResult {
        self.runtime
            .storage_node
            .test_put_multipart_reclaim(
                &trusted_bucket_name(TRACE_BUCKET),
                &trusted_object_key(TRACE_KEY),
                &trace_multipart_reclaim(
                    &self.runtime,
                    TRACE_BUCKET,
                    TRACE_KEY,
                    trace_generation_id(),
                    1,
                ),
            )
            .map_err(|err| {
                TestCaseError::fail(format!("test_put_multipart_reclaim failed: {err:?}"))
            })?;
        Ok(())
    }

    fn metadata_exists(&self) -> bool {
        self.runtime
            .storage_node
            .test_payload_reclaim_exists(
                &trusted_bucket_name(TRACE_BUCKET),
                &trusted_object_key(TRACE_KEY),
                trace_generation_id(),
            )
            .unwrap()
    }

    fn lease_count(&self) -> usize {
        self.runtime.storage_node.object_payload_lease_count(
            &trusted_bucket_name(TRACE_BUCKET),
            &trusted_object_key(TRACE_KEY),
            trace_generation_id(),
        )
    }

    fn take_next_work(&self) -> Result<Option<ReclaimWorkItem>, TestCaseError> {
        Ok(self.runtime.storage_node.try_take_reclaim_work())
    }
}

struct TwoGenerationReclaimTraceHarness {
    runtime: ReadRuntime,
    old_lease: Option<PayloadLease>,
    new_lease: Option<PayloadLease>,
}

struct TwoKeyReclaimTraceHarness {
    runtime: ReadRuntime,
    key_a_lease: Option<PayloadLease>,
    key_b_lease: Option<PayloadLease>,
}

impl TwoGenerationReclaimTraceHarness {
    fn new(runtime: ReadRuntime) -> Self {
        Self {
            runtime,
            old_lease: None,
            new_lease: None,
        }
    }

    fn execute(&mut self, op: &TwoGenerationReclaimTraceOp) -> TestCaseResult {
        use TwoGenerationReclaimTraceOp::*;
        match op {
            SeedBucketDelete => {
                seed_deleting_bucket(&self.runtime);
                self.runtime
                    .storage_node
                    .enqueue_bucket_delete_finalize(&trusted_bucket_name(TRACE_BUCKET));
            }
            SeedOldMetadata => self.seed_metadata_for(TraceGeneration::Old)?,
            SeedNewMetadata => self.seed_metadata_for(TraceGeneration::New)?,
            AcquireOldLease => {
                self.old_lease = Some(self.runtime.acquire_object_payload_lease(
                    TRACE_BUCKET,
                    TRACE_KEY,
                    trace_generation_id(),
                ));
            }
            ReleaseOldLease => drop(self.old_lease.take().unwrap()),
            AcquireNewLease => {
                self.new_lease = Some(self.runtime.acquire_object_payload_lease(
                    TRACE_BUCKET,
                    TRACE_KEY,
                    trace_generation_id_new(),
                ));
            }
            ReleaseNewLease => drop(self.new_lease.take().unwrap()),
            EnqueueOldReclaim => self.runtime.enqueue_object_payload_reclaim(
                TRACE_BUCKET,
                TRACE_KEY,
                trace_generation_id(),
            ),
            EnqueueNewReclaim => self.runtime.enqueue_object_payload_reclaim(
                TRACE_BUCKET,
                TRACE_KEY,
                trace_generation_id_new(),
            ),
            WorkerObjectStep => unreachable!("use execute_worker_object_step with model head"),
            WorkerBucketDeleteStep => {
                let work = self.take_next_work()?;
                match work {
                    Some(ReclaimWorkItem::BucketDelete(bucket))
                        if bucket == trusted_bucket_name(TRACE_BUCKET) => {}
                    Some(ReclaimWorkItem::ObjectPayload(_)) => {
                        return Err(TestCaseError::fail(
                            "expected bucket delete finalize work item, got object reclaim",
                        ))
                    }
                    None => {
                        return Err(TestCaseError::fail(
                            "expected bucket delete finalize work item, got none",
                        ))
                    }
                    Some(ReclaimWorkItem::BucketDelete(_)) => {
                        return Err(TestCaseError::fail(
                            "expected bucket delete finalize work item for the trace bucket",
                        ))
                    }
                }
                self.runtime
                    .try_finalize_bucket_delete_for(&trusted_bucket_name(TRACE_BUCKET))
                    .map_err(|err| {
                        TestCaseError::fail(format!(
                            "try_finalize_bucket_delete_for failed unexpectedly: {err:?}"
                        ))
                    })?;
            }
            ExpectNoWork => {
                if self.take_next_work()?.is_some() {
                    return Err(TestCaseError::fail("expected no queued reclaim work"));
                }
            }
        }
        Ok(())
    }

    fn execute_worker_object_step(&mut self, expected: TraceGeneration) -> TestCaseResult {
        let work = self.take_next_work()?;
        let actual = self.expected_generation_from_queue_step(work)?;
        prop_assert_eq!(
            actual,
            expected,
            "worker dequeued object reclaim generation out of FIFO order"
        );
        self.runtime
            .try_reclaim_object_payload(TRACE_BUCKET, TRACE_KEY, expected.generation_id())
            .map_err(|err| {
                TestCaseError::fail(format!(
                    "try_reclaim_object_payload failed unexpectedly: {err:?}"
                ))
            })?;
        Ok(())
    }

    fn seed_metadata_for(&self, generation: TraceGeneration) -> TestCaseResult {
        let created_at = match generation {
            TraceGeneration::Old => 1,
            TraceGeneration::New => 2,
        };
        self.runtime
            .storage_node
            .test_put_object_segments_reclaim(
                &trusted_bucket_name(TRACE_BUCKET),
                &trusted_object_key(TRACE_KEY),
                &trace_object_segments_reclaim(
                    &self.runtime,
                    TRACE_BUCKET,
                    TRACE_KEY,
                    generation.generation_id(),
                    created_at,
                ),
            )
            .map_err(|err| {
                TestCaseError::fail(format!("test_put_object_segments_reclaim failed: {err:?}"))
            })?;
        Ok(())
    }

    fn expected_generation_from_queue_step(
        &self,
        work: Option<ReclaimWorkItem>,
    ) -> Result<TraceGeneration, TestCaseError> {
        match work {
            Some(ReclaimWorkItem::ObjectPayload((bucket, key, generation_id)))
                if bucket == trusted_bucket_name(TRACE_BUCKET)
                    && key == trusted_object_key(TRACE_KEY)
                    && generation_id == trace_generation_id() =>
            {
                Ok(TraceGeneration::Old)
            }
            Some(ReclaimWorkItem::ObjectPayload((bucket, key, generation_id)))
                if bucket == trusted_bucket_name(TRACE_BUCKET)
                    && key == trusted_object_key(TRACE_KEY)
                    && generation_id == trace_generation_id_new() =>
            {
                Ok(TraceGeneration::New)
            }
            Some(ReclaimWorkItem::ObjectPayload(_)) => Err(TestCaseError::fail(
                "expected object reclaim work item for a traced generation",
            )),
            Some(ReclaimWorkItem::BucketDelete(_)) => Err(TestCaseError::fail(
                "expected object reclaim work item, got bucket delete",
            )),
            None => Err(TestCaseError::fail(
                "expected object reclaim work item, got none",
            )),
        }
    }

    fn metadata_exists(&self, generation: TraceGeneration) -> bool {
        self.runtime
            .storage_node
            .test_payload_reclaim_exists(
                &trusted_bucket_name(TRACE_BUCKET),
                &trusted_object_key(TRACE_KEY),
                generation.generation_id(),
            )
            .unwrap()
    }

    fn lease_count(&self, generation: TraceGeneration) -> usize {
        self.runtime.storage_node.object_payload_lease_count(
            &trusted_bucket_name(TRACE_BUCKET),
            &trusted_object_key(TRACE_KEY),
            generation.generation_id(),
        )
    }

    fn take_next_work(&self) -> Result<Option<ReclaimWorkItem>, TestCaseError> {
        Ok(self.runtime.storage_node.try_take_reclaim_work())
    }

    fn bucket_exists(&self) -> bool {
        self.runtime
            .storage_node
            .test_head_bucket_raw(&trusted_bucket_name(TRACE_BUCKET))
            .is_ok()
    }
}

impl TwoKeyReclaimTraceHarness {
    fn new(runtime: ReadRuntime) -> Self {
        Self {
            runtime,
            key_a_lease: None,
            key_b_lease: None,
        }
    }

    fn execute(&mut self, op: &TwoKeyReclaimTraceOp) -> TestCaseResult {
        use TwoKeyReclaimTraceOp::*;
        match op {
            SeedBucketDelete => {
                seed_deleting_bucket(&self.runtime);
                self.runtime
                    .storage_node
                    .enqueue_bucket_delete_finalize(&trusted_bucket_name(TRACE_BUCKET));
            }
            SeedKeyAMetadata => self.seed_metadata_for(TraceKey::A)?,
            SeedKeyBMetadata => self.seed_metadata_for(TraceKey::B)?,
            AcquireKeyALease => {
                self.key_a_lease = Some(self.runtime.acquire_object_payload_lease(
                    TRACE_BUCKET,
                    TRACE_KEY_A,
                    trace_generation_id(),
                ));
            }
            ReleaseKeyALease => drop(self.key_a_lease.take().unwrap()),
            AcquireKeyBLease => {
                self.key_b_lease = Some(self.runtime.acquire_object_payload_lease(
                    TRACE_BUCKET,
                    TRACE_KEY_B,
                    trace_generation_id(),
                ));
            }
            ReleaseKeyBLease => drop(self.key_b_lease.take().unwrap()),
            EnqueueKeyAReclaim => self.runtime.enqueue_object_payload_reclaim(
                TRACE_BUCKET,
                TRACE_KEY_A,
                trace_generation_id(),
            ),
            EnqueueKeyBReclaim => self.runtime.enqueue_object_payload_reclaim(
                TRACE_BUCKET,
                TRACE_KEY_B,
                trace_generation_id(),
            ),
            WorkerObjectStep => unreachable!("use execute_worker_object_step with model head"),
            WorkerBucketDeleteStep => {
                let work = self.take_next_work()?;
                match work {
                    Some(ReclaimWorkItem::BucketDelete(bucket))
                        if bucket == trusted_bucket_name(TRACE_BUCKET) => {}
                    Some(ReclaimWorkItem::ObjectPayload(_)) => {
                        return Err(TestCaseError::fail(
                            "expected bucket delete finalize work item, got object reclaim",
                        ))
                    }
                    None => {
                        return Err(TestCaseError::fail(
                            "expected bucket delete finalize work item, got none",
                        ))
                    }
                    Some(ReclaimWorkItem::BucketDelete(_)) => {
                        return Err(TestCaseError::fail(
                            "expected bucket delete finalize work item for the trace bucket",
                        ))
                    }
                }
                self.runtime
                    .try_finalize_bucket_delete_for(&trusted_bucket_name(TRACE_BUCKET))
                    .map_err(|err| {
                        TestCaseError::fail(format!(
                            "try_finalize_bucket_delete_for failed unexpectedly: {err:?}"
                        ))
                    })?;
            }
            ExpectNoWork => {
                if self.take_next_work()?.is_some() {
                    return Err(TestCaseError::fail("expected no queued reclaim work"));
                }
            }
        }
        Ok(())
    }

    fn execute_worker_object_step(&mut self, expected: TraceKey) -> TestCaseResult {
        let work = self.take_next_work()?;
        let actual = self.expected_key_from_queue_step(work)?;
        prop_assert_eq!(
            actual,
            expected,
            "worker dequeued object reclaim key out of bucket-root order"
        );
        self.runtime
            .try_reclaim_object_payload(TRACE_BUCKET, expected.key(), trace_generation_id())
            .map_err(|err| {
                TestCaseError::fail(format!(
                    "try_reclaim_object_payload failed unexpectedly: {err:?}"
                ))
            })?;
        Ok(())
    }

    fn seed_metadata_for(&self, key: TraceKey) -> TestCaseResult {
        let created_at = match key {
            TraceKey::A => 1,
            TraceKey::B => 2,
        };
        self.runtime
            .storage_node
            .test_put_object_segments_reclaim(
                &trusted_bucket_name(TRACE_BUCKET),
                &trusted_object_key(key.key()),
                &trace_object_segments_reclaim(
                    &self.runtime,
                    TRACE_BUCKET,
                    key.key(),
                    trace_generation_id(),
                    created_at,
                ),
            )
            .map_err(|err| {
                TestCaseError::fail(format!("test_put_object_segments_reclaim failed: {err:?}"))
            })?;
        Ok(())
    }

    fn expected_key_from_queue_step(
        &self,
        work: Option<ReclaimWorkItem>,
    ) -> Result<TraceKey, TestCaseError> {
        match work {
            Some(ReclaimWorkItem::ObjectPayload((bucket, key, generation_id)))
                if bucket == trusted_bucket_name(TRACE_BUCKET)
                    && key == trusted_object_key(TRACE_KEY_A)
                    && generation_id == trace_generation_id() =>
            {
                Ok(TraceKey::A)
            }
            Some(ReclaimWorkItem::ObjectPayload((bucket, key, generation_id)))
                if bucket == trusted_bucket_name(TRACE_BUCKET)
                    && key == trusted_object_key(TRACE_KEY_B)
                    && generation_id == trace_generation_id() =>
            {
                Ok(TraceKey::B)
            }
            Some(ReclaimWorkItem::ObjectPayload(_)) => Err(TestCaseError::fail(
                "expected object reclaim work item for a traced key",
            )),
            Some(ReclaimWorkItem::BucketDelete(_)) => Err(TestCaseError::fail(
                "expected object reclaim work item, got bucket delete",
            )),
            None => Err(TestCaseError::fail(
                "expected object reclaim work item, got none",
            )),
        }
    }

    fn metadata_exists(&self, key: TraceKey) -> bool {
        self.runtime
            .storage_node
            .test_payload_reclaim_exists(
                &trusted_bucket_name(TRACE_BUCKET),
                &trusted_object_key(key.key()),
                trace_generation_id(),
            )
            .unwrap()
    }

    fn lease_count(&self, key: TraceKey) -> usize {
        self.runtime.storage_node.object_payload_lease_count(
            &trusted_bucket_name(TRACE_BUCKET),
            &trusted_object_key(key.key()),
            trace_generation_id(),
        )
    }

    fn take_next_work(&self) -> Result<Option<ReclaimWorkItem>, TestCaseError> {
        Ok(self.runtime.storage_node.try_take_reclaim_work())
    }

    fn bucket_exists(&self) -> bool {
        self.runtime
            .storage_node
            .test_head_bucket_raw(&trusted_bucket_name(TRACE_BUCKET))
            .is_ok()
    }
}

fn render_reclaim_trace(ops: &[ReclaimTraceOp]) -> String {
    let mut rendered = String::new();
    for (index, op) in ops.iter().enumerate() {
        let _ = writeln!(&mut rendered, "{index}: {op}");
    }
    rendered
}

fn render_reclaim_kind_trace(ops: &[ReclaimKindTraceOp]) -> String {
    let mut rendered = String::new();
    for (index, op) in ops.iter().enumerate() {
        let _ = writeln!(&mut rendered, "{index}: {op}");
    }
    rendered
}

fn render_two_generation_reclaim_trace(ops: &[TwoGenerationReclaimTraceOp]) -> String {
    let mut rendered = String::new();
    for (index, op) in ops.iter().enumerate() {
        let _ = writeln!(&mut rendered, "{index}: {op}");
    }
    rendered
}

fn render_two_key_reclaim_trace(ops: &[TwoKeyReclaimTraceOp]) -> String {
    let mut rendered = String::new();
    for (index, op) in ops.iter().enumerate() {
        let _ = writeln!(&mut rendered, "{index}: {op}");
    }
    rendered
}

fn assert_reclaim_trace_matches_model(
    harness: &ReclaimTraceHarness,
    model: &ReclaimTraceModel,
    context: &str,
) -> TestCaseResult {
    prop_assert_eq!(
        harness.metadata_exists(),
        model.metadata_exists,
        "{}",
        context
    );
    prop_assert_eq!(
        harness.lease_count(),
        usize::from(model.lease_held),
        "{}",
        context
    );
    Ok(())
}

fn assert_reclaim_kind_trace_matches_model(
    harness: &ReclaimKindTraceHarness,
    model: &ReclaimKindTraceModel,
    context: &str,
) -> TestCaseResult {
    prop_assert_eq!(
        harness.metadata_exists(),
        model.reclaim_kind.is_some(),
        "{}",
        context
    );
    prop_assert_eq!(
        harness.lease_count(),
        usize::from(model.lease_held),
        "{}",
        context
    );
    Ok(())
}

fn assert_two_generation_reclaim_trace_matches_model(
    harness: &TwoGenerationReclaimTraceHarness,
    model: &TwoGenerationReclaimTraceModel,
    context: &str,
) -> TestCaseResult {
    prop_assert_eq!(harness.bucket_exists(), model.bucket_exists, "{}", context);
    prop_assert_eq!(
        harness.metadata_exists(TraceGeneration::Old),
        model.old_metadata_exists,
        "{}",
        context
    );
    prop_assert_eq!(
        harness.metadata_exists(TraceGeneration::New),
        model.new_metadata_exists,
        "{}",
        context
    );
    prop_assert_eq!(
        harness.lease_count(TraceGeneration::Old),
        usize::from(model.old_lease_held),
        "{}",
        context
    );
    prop_assert_eq!(
        harness.lease_count(TraceGeneration::New),
        usize::from(model.new_lease_held),
        "{}",
        context
    );
    Ok(())
}

fn assert_two_key_reclaim_trace_matches_model(
    harness: &TwoKeyReclaimTraceHarness,
    model: &TwoKeyReclaimTraceModel,
    context: &str,
) -> TestCaseResult {
    prop_assert_eq!(harness.bucket_exists(), model.bucket_exists, "{}", context);
    prop_assert_eq!(
        harness.metadata_exists(TraceKey::A),
        model.key_a_metadata_exists,
        "{}",
        context
    );
    prop_assert_eq!(
        harness.metadata_exists(TraceKey::B),
        model.key_b_metadata_exists,
        "{}",
        context
    );
    prop_assert_eq!(
        harness.lease_count(TraceKey::A),
        usize::from(model.key_a_lease_held),
        "{}",
        context
    );
    prop_assert_eq!(
        harness.lease_count(TraceKey::B),
        usize::from(model.key_b_lease_held),
        "{}",
        context
    );
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn prop_reclaim_queue_trace_matches_model(ops in reclaim_trace_strategy()) {
        let tmp = test_util::tempdir();
        let runtime = make_test_read_runtime(tmp.path());
        let mut harness = ReclaimTraceHarness::new(runtime);
        let mut model = ReclaimTraceModel::new();

        for (index, op) in ops.iter().enumerate() {
            let context = format!(
                "after step {index}: {op}\nfull trace:\n{}",
                render_reclaim_trace(&ops[..=index]),
            );
            harness.execute(op)?;
            model.apply(op);
            assert_reclaim_trace_matches_model(&harness, &model, &context)?;
        }
    }

    #[test]
    fn prop_reclaim_kind_trace_matches_model(ops in reclaim_kind_trace_strategy()) {
        let tmp = test_util::tempdir();
        let runtime = make_test_read_runtime(tmp.path());
        let mut harness = ReclaimKindTraceHarness::new(runtime);
        let mut model = ReclaimKindTraceModel::new();

        for (index, op) in ops.iter().enumerate() {
            let context = format!(
                "after step {index}: {op}\nfull trace:\n{}",
                render_reclaim_kind_trace(&ops[..=index]),
            );
            harness.execute(op)?;
            model.apply(op);
            assert_reclaim_kind_trace_matches_model(&harness, &model, &context)?;
        }
    }

    #[test]
    fn prop_two_generation_reclaim_queue_trace_matches_model(
        ops in two_generation_reclaim_trace_strategy()
    ) {
        let tmp = test_util::tempdir();
        let runtime = make_test_read_runtime(tmp.path());
        let mut harness = TwoGenerationReclaimTraceHarness::new(runtime);
        let mut model = TwoGenerationReclaimTraceModel::new();

        for (index, op) in ops.iter().enumerate() {
            let context = format!(
                "after step {index}: {op}\nfull trace:\n{}",
                render_two_generation_reclaim_trace(&ops[..=index]),
            );
            if matches!(op, TwoGenerationReclaimTraceOp::WorkerObjectStep) {
                let expected = *model
                    .object_queue
                    .first()
                    .expect("legal worker step must have queued object work");
                harness.execute_worker_object_step(expected)?;
            } else {
                harness.execute(op)?;
            }
            model.apply(op);
            assert_two_generation_reclaim_trace_matches_model(&harness, &model, &context)?;
        }
    }

    #[test]
    fn prop_two_key_reclaim_queue_trace_matches_model(
        ops in two_key_reclaim_trace_strategy()
    ) {
        let tmp = test_util::tempdir();
        let runtime = make_test_read_runtime(tmp.path());
        let mut harness = TwoKeyReclaimTraceHarness::new(runtime);
        let mut model = TwoKeyReclaimTraceModel::new();

        for (index, op) in ops.iter().enumerate() {
            let context = format!(
                "after step {index}: {op}\nfull trace:\n{}",
                render_two_key_reclaim_trace(&ops[..=index]),
            );
            if matches!(op, TwoKeyReclaimTraceOp::WorkerObjectStep) {
                let expected = *model
                    .object_queue
                    .first()
                    .expect("legal worker step must have queued object work");
                harness.execute_worker_object_step(expected)?;
            } else {
                harness.execute(op)?;
            }
            model.apply(op);
            assert_two_key_reclaim_trace_matches_model(&harness, &model, &context)?;
        }
    }
}

#[test]
fn object_reclaim_work_takes_priority_over_stale_bucket_delete_follow_on() {
    let tmp = test_util::tempdir();
    let runtime = make_test_read_runtime(tmp.path());
    let mut harness = ReclaimTraceHarness::new(runtime);

    harness.execute(&ReclaimTraceOp::SeedMetadata).unwrap();
    harness.execute(&ReclaimTraceOp::AcquireLease).unwrap();
    harness.execute(&ReclaimTraceOp::ReleaseLease).unwrap();
    harness.execute(&ReclaimTraceOp::WorkerObjectStep).unwrap();
    harness.execute(&ReclaimTraceOp::SeedMetadata).unwrap();
    harness
        .execute(&ReclaimTraceOp::EnqueueObjectReclaim)
        .unwrap();

    match harness.take_next_work().unwrap() {
        Some(ReclaimWorkItem::ObjectPayload((bucket, key, generation_id)))
            if bucket == trusted_bucket_name(TRACE_BUCKET)
                && key == trusted_object_key(TRACE_KEY)
                && generation_id == trace_generation_id() => {}
        Some(ReclaimWorkItem::BucketDelete(bucket)) => panic!(
            "new object reclaim work must outrank stale bucket-delete follow-on, got bucket delete for {bucket}"
        ),
        None => panic!("expected object reclaim work after reseeding metadata"),
        Some(ReclaimWorkItem::ObjectPayload(_)) => {
            panic!("expected object reclaim work for the traced generation")
        }
    }
}

#[test]
fn deleting_bucket_finalize_advances_from_old_generation_to_new_generation_root() {
    let tmp = test_util::tempdir();
    let runtime = make_test_read_runtime(tmp.path());
    let mut harness = TwoGenerationReclaimTraceHarness::new(runtime);

    harness
        .execute(&TwoGenerationReclaimTraceOp::SeedOldMetadata)
        .unwrap();
    harness
        .execute(&TwoGenerationReclaimTraceOp::SeedNewMetadata)
        .unwrap();
    harness
        .execute(&TwoGenerationReclaimTraceOp::SeedBucketDelete)
        .unwrap();
    harness
        .execute(&TwoGenerationReclaimTraceOp::EnqueueOldReclaim)
        .unwrap();
    harness
        .execute_worker_object_step(TraceGeneration::Old)
        .unwrap();

    assert!(
        !harness.metadata_exists(TraceGeneration::Old),
        "reclaiming the old generation should clear its durable reclaim metadata"
    );
    assert!(
        harness.metadata_exists(TraceGeneration::New),
        "the newer generation should still be present before bucket-delete finalize advances the root"
    );

    harness
        .execute(&TwoGenerationReclaimTraceOp::WorkerBucketDeleteStep)
        .unwrap();

    match harness.take_next_work().unwrap() {
        Some(ReclaimWorkItem::ObjectPayload((bucket, key, generation_id)))
            if bucket == trusted_bucket_name(TRACE_BUCKET)
                && key == trusted_object_key(TRACE_KEY)
                && generation_id == trace_generation_id_new() => {}
        Some(ReclaimWorkItem::ObjectPayload((_bucket, _key, generation_id))) => panic!(
            "bucket-delete finalize should advance to the new generation root, got generation {generation_id:?}"
        ),
        Some(ReclaimWorkItem::BucketDelete(bucket)) => panic!(
            "expected advanced object reclaim work for the new generation, got bucket delete for {bucket}"
        ),
        None => panic!("expected bucket-delete finalize to enqueue reclaim for the new generation root"),
    }

    harness
        .runtime
        .try_reclaim_object_payload(TRACE_BUCKET, TRACE_KEY, trace_generation_id_new())
        .unwrap();
    assert!(
        !harness.metadata_exists(TraceGeneration::New),
        "reclaiming the advanced new-generation root should clear its durable metadata"
    );
}

#[test]
fn bucket_delete_finalize_does_not_skip_a_lease_blocked_older_generation_root() {
    let tmp = test_util::tempdir();
    let runtime = make_test_read_runtime(tmp.path());
    let mut harness = TwoGenerationReclaimTraceHarness::new(runtime);

    harness
        .execute(&TwoGenerationReclaimTraceOp::SeedOldMetadata)
        .unwrap();
    harness
        .execute(&TwoGenerationReclaimTraceOp::SeedNewMetadata)
        .unwrap();
    harness
        .execute(&TwoGenerationReclaimTraceOp::AcquireOldLease)
        .unwrap();
    harness
        .execute(&TwoGenerationReclaimTraceOp::SeedBucketDelete)
        .unwrap();
    harness
        .execute(&TwoGenerationReclaimTraceOp::WorkerBucketDeleteStep)
        .unwrap();

    assert!(harness.bucket_exists());
    assert!(harness.metadata_exists(TraceGeneration::Old));
    assert!(harness.metadata_exists(TraceGeneration::New));
    assert_eq!(harness.lease_count(TraceGeneration::Old), 1);
    assert_eq!(harness.lease_count(TraceGeneration::New), 0);
    assert!(
        harness.take_next_work().unwrap().is_none(),
        "bucket delete finalize must not enqueue newer-generation reclaim while the current older root is still lease-blocked"
    );
}

#[test]
fn deleting_empty_bucket_finalize_removes_bucket_without_follow_on_reclaim() {
    let tmp = test_util::tempdir();
    let runtime = make_test_read_runtime(tmp.path());
    let mut harness = TwoGenerationReclaimTraceHarness::new(runtime);

    harness
        .execute(&TwoGenerationReclaimTraceOp::SeedBucketDelete)
        .unwrap();
    harness
        .execute(&TwoGenerationReclaimTraceOp::WorkerBucketDeleteStep)
        .unwrap();

    assert!(
        !harness.bucket_exists(),
        "finalizing delete for an otherwise empty deleting bucket should remove the bucket immediately"
    );
    assert!(
        harness.take_next_work().unwrap().is_none(),
        "empty deleting bucket finalize should not enqueue any follow-on reclaim work"
    );
}
