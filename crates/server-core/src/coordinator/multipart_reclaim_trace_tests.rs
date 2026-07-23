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

fn seed_deleting_bucket(runtime: &ReadRuntime) -> storage::BucketDeleteFinalizeRoot {
    let bucket = trusted_bucket_name(TRACE_BUCKET);
    runtime
        .storage_node
        .test_create_deleting_bucket(&bucket)
        .unwrap();
    let info = runtime.storage_node.test_head_bucket_raw(&bucket).unwrap();
    storage::BucketDeleteFinalizeRoot {
        bucket,
        bucket_incarnation_generation: info.bucket_incarnation_generation,
    }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReclaimHint {
    Object,
    BucketDelete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GenerationReclaimHint {
    Object(TraceGeneration),
    BucketDelete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyReclaimHint {
    Object(TraceKey),
    BucketDelete,
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
    queue: Vec<ReclaimHint>,
    deferred_object_reclaim: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReclaimKindTraceModel {
    reclaim_kind: Option<TraceReclaimKind>,
    lease_held: bool,
    queue: Vec<ReclaimHint>,
    deferred_object_reclaim: bool,
}

impl ReclaimTraceModel {
    fn new() -> Self {
        Self {
            metadata_exists: false,
            lease_held: false,
            queue: Vec::new(),
            deferred_object_reclaim: false,
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
        match self.queue.first() {
            Some(ReclaimHint::Object) => ops.push(WorkerObjectStep),
            Some(ReclaimHint::BucketDelete) => ops.push(WorkerBucketDeleteStep),
            None if self.deferred_object_reclaim => ops.push(WorkerObjectStep),
            None => ops.push(ExpectNoWork),
        }
        ops
    }

    fn enqueue_hint(&mut self, hint: ReclaimHint) {
        if hint == ReclaimHint::Object && self.deferred_object_reclaim {
            return;
        }
        if !self.queue.contains(&hint) {
            self.queue.push(hint);
        }
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
                    self.enqueue_hint(ReclaimHint::Object);
                }
            }
            EnqueueObjectReclaim => {
                self.enqueue_hint(ReclaimHint::Object);
            }
            WorkerObjectStep => {
                if self.queue.first() == Some(&ReclaimHint::Object) {
                    self.queue.remove(0);
                } else {
                    assert!(self.deferred_object_reclaim);
                    self.deferred_object_reclaim = false;
                }
                if self.metadata_exists && !self.lease_held {
                    self.metadata_exists = false;
                    self.enqueue_hint(ReclaimHint::BucketDelete);
                } else if self.metadata_exists && self.lease_held {
                    self.deferred_object_reclaim = true;
                }
            }
            WorkerBucketDeleteStep => {
                assert_eq!(self.queue.remove(0), ReclaimHint::BucketDelete);
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
            queue: Vec::new(),
            deferred_object_reclaim: false,
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
        match self.queue.first() {
            Some(ReclaimHint::Object) => ops.push(WorkerObjectStep),
            Some(ReclaimHint::BucketDelete) => ops.push(WorkerBucketDeleteStep),
            None if self.deferred_object_reclaim => ops.push(WorkerObjectStep),
            None => ops.push(ExpectNoWork),
        }
        ops
    }

    fn enqueue_hint(&mut self, hint: ReclaimHint) {
        if hint == ReclaimHint::Object && self.deferred_object_reclaim {
            return;
        }
        if !self.queue.contains(&hint) {
            self.queue.push(hint);
        }
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
                    self.enqueue_hint(ReclaimHint::Object);
                }
            }
            EnqueueObjectReclaim => self.enqueue_hint(ReclaimHint::Object),
            WorkerObjectStep => {
                if self.queue.first() == Some(&ReclaimHint::Object) {
                    self.queue.remove(0);
                } else {
                    assert!(self.deferred_object_reclaim);
                    self.deferred_object_reclaim = false;
                }
                if self.reclaim_kind.is_some() && !self.lease_held {
                    self.reclaim_kind = None;
                    self.enqueue_hint(ReclaimHint::BucketDelete);
                } else if self.reclaim_kind.is_some() && self.lease_held {
                    self.deferred_object_reclaim = true;
                }
            }
            WorkerBucketDeleteStep => {
                assert_eq!(self.queue.remove(0), ReclaimHint::BucketDelete);
            }
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
    queue: Vec<GenerationReclaimHint>,
    deferred: Vec<TraceGeneration>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TwoKeyReclaimTraceModel {
    bucket_exists: bool,
    bucket_deleting: bool,
    key_a_metadata_exists: bool,
    key_b_metadata_exists: bool,
    key_a_lease_held: bool,
    key_b_lease_held: bool,
    queue: Vec<KeyReclaimHint>,
    deferred: Vec<TraceKey>,
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
            queue: Vec::new(),
            deferred: Vec::new(),
        }
    }

    fn legal_ops(&self) -> Vec<TwoGenerationReclaimTraceOp> {
        use TwoGenerationReclaimTraceOp::*;
        let mut ops = Vec::new();
        if !self.bucket_exists && !self.queue.contains(&GenerationReclaimHint::BucketDelete) {
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
        match self.queue.first() {
            Some(GenerationReclaimHint::Object(_)) => ops.push(WorkerObjectStep),
            Some(GenerationReclaimHint::BucketDelete) => ops.push(WorkerBucketDeleteStep),
            None if !self.deferred.is_empty() => ops.push(WorkerObjectStep),
            None => ops.push(ExpectNoWork),
        }
        ops
    }

    fn enqueue_generation(&mut self, generation: TraceGeneration) {
        if self.deferred.contains(&generation) {
            return;
        }
        let hint = GenerationReclaimHint::Object(generation);
        if !self.queue.contains(&hint) {
            self.queue.push(hint);
        }
    }

    fn next_object_generation(&self) -> Option<TraceGeneration> {
        match self.queue.first() {
            Some(GenerationReclaimHint::Object(generation)) => Some(*generation),
            Some(GenerationReclaimHint::BucketDelete) => None,
            None => self.deferred.first().copied(),
        }
    }

    fn enqueue_bucket_delete(&mut self) {
        if !self.queue.contains(&GenerationReclaimHint::BucketDelete) {
            self.queue.push(GenerationReclaimHint::BucketDelete);
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
                self.enqueue_bucket_delete();
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
                let generation = match self.queue.first() {
                    Some(GenerationReclaimHint::Object(_)) => {
                        let GenerationReclaimHint::Object(generation) = self.queue.remove(0) else {
                            unreachable!("checked object hint")
                        };
                        generation
                    }
                    None => self.deferred.remove(0),
                    Some(GenerationReclaimHint::BucketDelete) => {
                        unreachable!("model only schedules worker-object-step for object hints")
                    }
                };
                match generation {
                    TraceGeneration::Old if self.old_metadata_exists && !self.old_lease_held => {
                        self.old_metadata_exists = false;
                        self.enqueue_bucket_delete();
                    }
                    TraceGeneration::New if self.new_metadata_exists && !self.new_lease_held => {
                        self.new_metadata_exists = false;
                        self.enqueue_bucket_delete();
                    }
                    TraceGeneration::Old
                        if self.old_metadata_exists
                            && self.old_lease_held
                            && !self.deferred.contains(&generation) =>
                    {
                        self.deferred.push(generation);
                    }
                    TraceGeneration::New
                        if self.new_metadata_exists
                            && self.new_lease_held
                            && !self.deferred.contains(&generation) =>
                    {
                        self.deferred.push(generation);
                    }
                    _ => {}
                }
            }
            WorkerBucketDeleteStep => {
                assert_eq!(self.queue.remove(0), GenerationReclaimHint::BucketDelete);
                if self.bucket_deleting {
                    let mut reclaimed_any = false;
                    loop {
                        match self.current_bucket_root() {
                            Some(TraceGeneration::Old) if !self.old_lease_held => {
                                self.old_metadata_exists = false;
                                reclaimed_any = true;
                                continue;
                            }
                            Some(TraceGeneration::New) if !self.new_lease_held => {
                                self.new_metadata_exists = false;
                                reclaimed_any = true;
                                continue;
                            }
                            _ => {}
                        }
                        break;
                    }
                    if !self.old_metadata_exists && !self.new_metadata_exists {
                        self.bucket_exists = false;
                        self.bucket_deleting = false;
                    } else if reclaimed_any {
                        self.enqueue_bucket_delete();
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
            queue: Vec::new(),
            deferred: Vec::new(),
        }
    }

    fn legal_ops(&self) -> Vec<TwoKeyReclaimTraceOp> {
        use TwoKeyReclaimTraceOp::*;
        let mut ops = Vec::new();
        if !self.bucket_exists && !self.queue.contains(&KeyReclaimHint::BucketDelete) {
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
        match self.queue.first() {
            Some(KeyReclaimHint::Object(_)) => ops.push(WorkerObjectStep),
            Some(KeyReclaimHint::BucketDelete) => ops.push(WorkerBucketDeleteStep),
            None if !self.deferred.is_empty() => ops.push(WorkerObjectStep),
            None => ops.push(ExpectNoWork),
        }
        ops
    }

    fn enqueue_key(&mut self, key: TraceKey) {
        if self.deferred.contains(&key) {
            return;
        }
        let hint = KeyReclaimHint::Object(key);
        if !self.queue.contains(&hint) {
            self.queue.push(hint);
        }
    }

    fn next_object_key(&self) -> Option<TraceKey> {
        match self.queue.first() {
            Some(KeyReclaimHint::Object(key)) => Some(*key),
            Some(KeyReclaimHint::BucketDelete) => None,
            None => self.deferred.first().copied(),
        }
    }

    fn enqueue_bucket_delete(&mut self) {
        if !self.queue.contains(&KeyReclaimHint::BucketDelete) {
            self.queue.push(KeyReclaimHint::BucketDelete);
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
                self.enqueue_bucket_delete();
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
                let key = match self.queue.first() {
                    Some(KeyReclaimHint::Object(_)) => {
                        let KeyReclaimHint::Object(key) = self.queue.remove(0) else {
                            unreachable!("checked object hint")
                        };
                        key
                    }
                    None => self.deferred.remove(0),
                    Some(KeyReclaimHint::BucketDelete) => {
                        unreachable!("model only schedules worker-object-step for object hints")
                    }
                };
                match key {
                    TraceKey::A if self.key_a_metadata_exists && !self.key_a_lease_held => {
                        self.key_a_metadata_exists = false;
                        self.enqueue_bucket_delete();
                    }
                    TraceKey::B if self.key_b_metadata_exists && !self.key_b_lease_held => {
                        self.key_b_metadata_exists = false;
                        self.enqueue_bucket_delete();
                    }
                    TraceKey::A
                        if self.key_a_metadata_exists
                            && self.key_a_lease_held
                            && !self.deferred.contains(&key) =>
                    {
                        self.deferred.push(key);
                    }
                    TraceKey::B
                        if self.key_b_metadata_exists
                            && self.key_b_lease_held
                            && !self.deferred.contains(&key) =>
                    {
                        self.deferred.push(key);
                    }
                    _ => {}
                }
            }
            WorkerBucketDeleteStep => {
                assert_eq!(self.queue.remove(0), KeyReclaimHint::BucketDelete);
                if self.bucket_deleting {
                    let mut reclaimed_any = false;
                    loop {
                        match self.current_bucket_root() {
                            Some(TraceKey::A) if !self.key_a_lease_held => {
                                self.key_a_metadata_exists = false;
                                reclaimed_any = true;
                                continue;
                            }
                            Some(TraceKey::B) if !self.key_b_lease_held => {
                                self.key_b_metadata_exists = false;
                                reclaimed_any = true;
                                continue;
                            }
                            _ => {}
                        }
                        break;
                    }
                    if !self.key_a_metadata_exists && !self.key_b_metadata_exists {
                        self.bucket_exists = false;
                        self.bucket_deleting = false;
                    } else if reclaimed_any {
                        self.enqueue_bucket_delete();
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
    deferred_object_reclaim: bool,
}

struct ReclaimKindTraceHarness {
    runtime: ReadRuntime,
    lease: Option<PayloadLease>,
    deferred_object_reclaim: bool,
}

impl ReclaimTraceHarness {
    fn new(runtime: ReadRuntime) -> Self {
        Self {
            runtime,
            lease: None,
            deferred_object_reclaim: false,
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
                self.expect_trace_object_work()?;
                let completed = self
                    .runtime
                    .try_reclaim_object_payload(TRACE_BUCKET, TRACE_KEY, trace_generation_id())
                    .map_err(|err| {
                        TestCaseError::fail(format!(
                            "try_reclaim_object_payload failed unexpectedly: {err:?}"
                        ))
                    })?;
                self.finish_or_defer_trace_object_work(completed);
            }
            WorkerBucketDeleteStep => {
                let work = self.take_next_work()?;
                match work {
                    Some(ReclaimWorkItem::BucketDelete(root))
                        if root.bucket == trusted_bucket_name(TRACE_BUCKET) => {}
                    Some(ReclaimWorkItem::ObjectPayload(_)) => {
                        return Err(TestCaseError::fail(
                            "expected bucket delete finalize work item, got object reclaim",
                        ))
                    }
                    Some(ReclaimWorkItem::BucketDeleteBegin(_)) => {
                        return Err(TestCaseError::fail(
                            "expected bucket delete finalize work item, got bucket delete begin",
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
                if self.deferred_object_reclaim {
                    return Err(TestCaseError::fail(
                        "expected no reclaim work but local deferred object reclaim is pending",
                    ));
                }
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

    fn expect_trace_object_work(&mut self) -> TestCaseResult {
        match self.take_next_work()? {
            Some(ReclaimWorkItem::ObjectPayload((bucket, key, generation_id)))
                if bucket == trusted_bucket_name(TRACE_BUCKET)
                    && key == trusted_object_key(TRACE_KEY)
                    && generation_id == trace_generation_id() =>
            {
                Ok(())
            }
            Some(ReclaimWorkItem::BucketDelete(_) | ReclaimWorkItem::BucketDeleteBegin(_)) => Err(
                TestCaseError::fail("expected object reclaim work item, got bucket delete"),
            ),
            Some(ReclaimWorkItem::ObjectPayload(_)) => Err(TestCaseError::fail(
                "expected object reclaim work item for the trace generation",
            )),
            None if self.deferred_object_reclaim => Ok(()),
            None => Err(TestCaseError::fail(
                "expected object reclaim work item, got none",
            )),
        }
    }

    fn finish_or_defer_trace_object_work(&mut self, completed: bool) {
        if completed {
            self.runtime
                .storage_node
                .finish_object_payload_reclaim_work(
                    &trusted_bucket_name(TRACE_BUCKET),
                    &trusted_object_key(TRACE_KEY),
                    trace_generation_id(),
                );
            self.deferred_object_reclaim = false;
        } else {
            self.deferred_object_reclaim = true;
        }
    }
}

impl ReclaimKindTraceHarness {
    fn new(runtime: ReadRuntime) -> Self {
        Self {
            runtime,
            lease: None,
            deferred_object_reclaim: false,
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
                self.expect_trace_object_work()?;
                let completed = self
                    .runtime
                    .try_reclaim_object_payload(TRACE_BUCKET, TRACE_KEY, trace_generation_id())
                    .map_err(|err| {
                        TestCaseError::fail(format!(
                            "try_reclaim_object_payload failed unexpectedly: {err:?}"
                        ))
                    })?;
                self.finish_or_defer_trace_object_work(completed);
            }
            WorkerBucketDeleteStep => {
                let work = self.take_next_work()?;
                match work {
                    Some(ReclaimWorkItem::BucketDelete(root))
                        if root.bucket == trusted_bucket_name(TRACE_BUCKET) => {}
                    Some(ReclaimWorkItem::ObjectPayload(_)) => {
                        return Err(TestCaseError::fail(
                            "expected bucket delete finalize work item, got object reclaim",
                        ))
                    }
                    Some(ReclaimWorkItem::BucketDeleteBegin(_)) => {
                        return Err(TestCaseError::fail(
                            "expected bucket delete finalize work item, got bucket delete begin",
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
                if self.deferred_object_reclaim {
                    return Err(TestCaseError::fail(
                        "expected no reclaim work but local deferred object reclaim is pending",
                    ));
                }
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

    fn expect_trace_object_work(&mut self) -> TestCaseResult {
        match self.take_next_work()? {
            Some(ReclaimWorkItem::ObjectPayload((bucket, key, generation_id)))
                if bucket == trusted_bucket_name(TRACE_BUCKET)
                    && key == trusted_object_key(TRACE_KEY)
                    && generation_id == trace_generation_id() =>
            {
                Ok(())
            }
            Some(ReclaimWorkItem::BucketDelete(_) | ReclaimWorkItem::BucketDeleteBegin(_)) => Err(
                TestCaseError::fail("expected object reclaim work item, got bucket delete"),
            ),
            Some(ReclaimWorkItem::ObjectPayload(_)) => Err(TestCaseError::fail(
                "expected object reclaim work item for the trace generation",
            )),
            None if self.deferred_object_reclaim => Ok(()),
            None => Err(TestCaseError::fail(
                "expected object reclaim work item, got none",
            )),
        }
    }

    fn finish_or_defer_trace_object_work(&mut self, completed: bool) {
        if completed {
            self.runtime
                .storage_node
                .finish_object_payload_reclaim_work(
                    &trusted_bucket_name(TRACE_BUCKET),
                    &trusted_object_key(TRACE_KEY),
                    trace_generation_id(),
                );
            self.deferred_object_reclaim = false;
        } else {
            self.deferred_object_reclaim = true;
        }
    }
}

struct TwoGenerationReclaimTraceHarness {
    runtime: ReadRuntime,
    old_lease: Option<PayloadLease>,
    new_lease: Option<PayloadLease>,
    deferred_object_reclaim: Vec<TraceGeneration>,
}

struct TwoKeyReclaimTraceHarness {
    runtime: ReadRuntime,
    key_a_lease: Option<PayloadLease>,
    key_b_lease: Option<PayloadLease>,
    deferred_object_reclaim: Vec<TraceKey>,
}

impl TwoGenerationReclaimTraceHarness {
    fn new(runtime: ReadRuntime) -> Self {
        Self {
            runtime,
            old_lease: None,
            new_lease: None,
            deferred_object_reclaim: Vec::new(),
        }
    }

    fn execute(&mut self, op: &TwoGenerationReclaimTraceOp) -> TestCaseResult {
        use TwoGenerationReclaimTraceOp::*;
        match op {
            SeedBucketDelete => {
                let root = seed_deleting_bucket(&self.runtime);
                self.runtime
                    .storage_node
                    .enqueue_bucket_delete_finalize(root);
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
                let root = match work {
                    Some(ReclaimWorkItem::BucketDelete(root))
                        if root.bucket == trusted_bucket_name(TRACE_BUCKET) =>
                    {
                        root
                    }
                    Some(ReclaimWorkItem::ObjectPayload(_)) => {
                        return Err(TestCaseError::fail(
                            "expected bucket delete finalize work item, got object reclaim",
                        ))
                    }
                    Some(ReclaimWorkItem::BucketDeleteBegin(_)) => {
                        return Err(TestCaseError::fail(
                            "expected bucket delete finalize work item, got bucket delete begin",
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
                };
                self.runtime
                    .try_finalize_bucket_delete_for_with_outcome(&root)
                    .map_err(|err| {
                        TestCaseError::fail(format!(
                            "try_finalize_bucket_delete_for_with_outcome failed unexpectedly: {err:?}"
                        ))
                    })?;
            }
            ExpectNoWork => {
                if let Some(work) = self.take_next_work()? {
                    return Err(TestCaseError::fail(format!(
                        "expected no queued reclaim work, got {work:?}"
                    )));
                }
            }
        }
        Ok(())
    }

    fn execute_worker_object_step(&mut self, expected: TraceGeneration) -> TestCaseResult {
        let actual = self.take_object_generation_work()?;
        prop_assert_eq!(
            actual,
            expected,
            "worker dequeued object reclaim generation out of FIFO order"
        );
        let completed = self
            .runtime
            .try_reclaim_object_payload(TRACE_BUCKET, TRACE_KEY, expected.generation_id())
            .map_err(|err| {
                TestCaseError::fail(format!(
                    "try_reclaim_object_payload failed unexpectedly: {err:?}"
                ))
            })?;
        self.finish_or_defer_generation_work(expected, completed);
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
            Some(ReclaimWorkItem::BucketDelete(_) | ReclaimWorkItem::BucketDeleteBegin(_)) => Err(
                TestCaseError::fail("expected object reclaim work item, got bucket delete"),
            ),
            None => Err(TestCaseError::fail(
                "expected object reclaim work item, got none",
            )),
        }
    }

    fn take_object_generation_work(&mut self) -> Result<TraceGeneration, TestCaseError> {
        match self.take_next_work()? {
            Some(work) => self.expected_generation_from_queue_step(Some(work)),
            None => {
                if self.deferred_object_reclaim.is_empty() {
                    Err(TestCaseError::fail(
                        "expected object reclaim work item, got none",
                    ))
                } else {
                    Ok(self.deferred_object_reclaim.remove(0))
                }
            }
        }
    }

    fn finish_or_defer_generation_work(&mut self, generation: TraceGeneration, completed: bool) {
        if completed {
            self.runtime
                .storage_node
                .finish_object_payload_reclaim_work(
                    &trusted_bucket_name(TRACE_BUCKET),
                    &trusted_object_key(TRACE_KEY),
                    generation.generation_id(),
                );
        } else if !self.deferred_object_reclaim.contains(&generation) {
            self.deferred_object_reclaim.push(generation);
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
            deferred_object_reclaim: Vec::new(),
        }
    }

    fn execute(&mut self, op: &TwoKeyReclaimTraceOp) -> TestCaseResult {
        use TwoKeyReclaimTraceOp::*;
        match op {
            SeedBucketDelete => {
                let root = seed_deleting_bucket(&self.runtime);
                self.runtime
                    .storage_node
                    .enqueue_bucket_delete_finalize(root);
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
                let root = match work {
                    Some(ReclaimWorkItem::BucketDelete(root))
                        if root.bucket == trusted_bucket_name(TRACE_BUCKET) =>
                    {
                        root
                    }
                    Some(ReclaimWorkItem::ObjectPayload(_)) => {
                        return Err(TestCaseError::fail(
                            "expected bucket delete finalize work item, got object reclaim",
                        ))
                    }
                    Some(ReclaimWorkItem::BucketDeleteBegin(_)) => {
                        return Err(TestCaseError::fail(
                            "expected bucket delete finalize work item, got bucket delete begin",
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
                };
                self.runtime
                    .try_finalize_bucket_delete_for_with_outcome(&root)
                    .map_err(|err| {
                        TestCaseError::fail(format!(
                            "try_finalize_bucket_delete_for_with_outcome failed unexpectedly: {err:?}"
                        ))
                    })?;
            }
            ExpectNoWork => {
                if let Some(work) = self.take_next_work()? {
                    return Err(TestCaseError::fail(format!(
                        "expected no queued reclaim work, got {work:?}"
                    )));
                }
            }
        }
        Ok(())
    }

    fn execute_worker_object_step(&mut self, expected: TraceKey) -> TestCaseResult {
        let actual = self.take_object_key_work()?;
        prop_assert_eq!(
            actual,
            expected,
            "worker dequeued object reclaim key out of bucket-root order"
        );
        let completed = self
            .runtime
            .try_reclaim_object_payload(TRACE_BUCKET, expected.key(), trace_generation_id())
            .map_err(|err| {
                TestCaseError::fail(format!(
                    "try_reclaim_object_payload failed unexpectedly: {err:?}"
                ))
            })?;
        self.finish_or_defer_key_work(expected, completed);
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
            Some(ReclaimWorkItem::BucketDelete(_) | ReclaimWorkItem::BucketDeleteBegin(_)) => Err(
                TestCaseError::fail("expected object reclaim work item, got bucket delete"),
            ),
            None => Err(TestCaseError::fail(
                "expected object reclaim work item, got none",
            )),
        }
    }

    fn take_object_key_work(&mut self) -> Result<TraceKey, TestCaseError> {
        match self.take_next_work()? {
            Some(work) => self.expected_key_from_queue_step(Some(work)),
            None => {
                if self.deferred_object_reclaim.is_empty() {
                    Err(TestCaseError::fail(
                        "expected object reclaim work item, got none",
                    ))
                } else {
                    Ok(self.deferred_object_reclaim.remove(0))
                }
            }
        }
    }

    fn finish_or_defer_key_work(&mut self, key: TraceKey, completed: bool) {
        if completed {
            self.runtime
                .storage_node
                .finish_object_payload_reclaim_work(
                    &trusted_bucket_name(TRACE_BUCKET),
                    &trusted_object_key(key.key()),
                    trace_generation_id(),
                );
        } else if !self.deferred_object_reclaim.contains(&key) {
            self.deferred_object_reclaim.push(key);
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
                let Some(expected) = model.next_object_generation() else {
                    panic!("legal worker step must have queued object work")
                };
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
                let Some(expected) = model.next_object_key() else {
                    panic!("legal worker step must have queued object work")
                };
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
fn stale_bucket_delete_follow_on_does_not_skip_new_object_reclaim() {
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
        Some(ReclaimWorkItem::BucketDelete(root))
            if root.bucket == trusted_bucket_name(TRACE_BUCKET) => {}
        Some(ReclaimWorkItem::BucketDelete(root)) => {
            panic!("expected bucket-delete follow-on for the trace bucket, got {root:?}")
        }
        Some(ReclaimWorkItem::BucketDeleteBegin(root)) => panic!(
            "expected bucket-delete follow-on for the trace bucket, got bucket delete begin for {}",
            root.bucket
        ),
        None => panic!("expected stale bucket-delete follow-on before new object reclaim work"),
        Some(ReclaimWorkItem::ObjectPayload(_)) => {
            panic!("FIFO local hints should not let object reclaim jump the stale bucket-delete follow-on")
        }
    }

    harness
        .runtime
        .try_finalize_bucket_delete_for(&trusted_bucket_name(TRACE_BUCKET))
        .unwrap();
    match harness.take_next_work().unwrap() {
        Some(ReclaimWorkItem::ObjectPayload((bucket, key, generation_id)))
            if bucket == trusted_bucket_name(TRACE_BUCKET)
                && key == trusted_object_key(TRACE_KEY)
                && generation_id == trace_generation_id() => {}
        Some(ReclaimWorkItem::BucketDelete(root)) => panic!(
            "bucket-delete follow-on must leave queued object reclaim visible, got {root:?}"
        ),
        Some(ReclaimWorkItem::BucketDeleteBegin(root)) => panic!(
            "bucket-delete follow-on must leave queued object reclaim visible, got bucket delete begin for {}",
            root.bucket
        ),
        None => panic!("expected object reclaim work after bucket-delete follow-on observes root"),
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
        .execute(&TwoGenerationReclaimTraceOp::WorkerBucketDeleteStep)
        .unwrap();

    assert!(
        !harness.metadata_exists(TraceGeneration::Old),
        "reclaiming the old generation should clear its durable reclaim metadata"
    );
    assert!(
        !harness.metadata_exists(TraceGeneration::New),
        "bucket-delete finalization should keep advancing through unleased durable roots"
    );

    match harness.take_next_work().unwrap() {
        Some(ReclaimWorkItem::ObjectPayload((bucket, key, generation_id)))
            if bucket == trusted_bucket_name(TRACE_BUCKET)
                && key == trusted_object_key(TRACE_KEY)
                && generation_id == trace_generation_id() => {}
        Some(ReclaimWorkItem::ObjectPayload((_bucket, _key, generation_id))) => panic!(
            "expected stale old-generation object hint after inline bucket-delete finalization, got generation {generation_id:?}"
        ),
        Some(ReclaimWorkItem::BucketDelete(root)) => panic!(
            "expected stale object reclaim hint after inline bucket-delete finalization, got {root:?}"
        ),
        Some(ReclaimWorkItem::BucketDeleteBegin(root)) => panic!(
            "expected stale object reclaim hint after inline bucket-delete finalization, got bucket delete begin for {}",
            root.bucket
        ),
        None => panic!("expected stale object reclaim hint from the pre-existing queue"),
    }

    assert_eq!(
        harness.take_next_work().unwrap(),
        None,
        "terminal bucket-delete finalization should purge redundant follow-on work"
    );
    assert!(
        !harness.bucket_exists(),
        "bucket-delete finalization should delete the bucket once all roots are drained"
    );
    assert!(
        !harness.metadata_exists(TraceGeneration::New),
        "stale new-generation queued work should be harmless after inline finalization"
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
