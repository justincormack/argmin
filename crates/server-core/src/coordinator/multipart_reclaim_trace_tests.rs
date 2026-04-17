use super::*;
use ec::EcConfig;
use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, TestCaseError, TestCaseResult};
use std::fmt::Write as _;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use storage::{PgMetadataStore, ReclaimWorkItem, SharedStorageNode, SimplePayloadReclaimRecord};

const TRACE_BUCKET: &str = "bucket";
const TRACE_KEY: &str = "key";
const TRACE_MAX_OPS: usize = 12;

fn trace_generation_id() -> GenerationId {
    GenerationId::new(1).expect("constant generation id must be valid")
}

fn trace_generation_id_new() -> GenerationId {
    GenerationId::new(2).expect("constant generation id must be valid")
}

fn make_test_read_runtime(dir: &Path) -> ReadRuntime {
    let ec_config = EcConfig::default();
    ReadRuntime {
        storage_node: Arc::new(SharedStorageNode::open(dir, &[0]).unwrap()),
        ec_codec: Arc::new(ErasureCodec::new(ec_config).unwrap()),
        ec_config,
        pg_topology: PgTopology::new(&[0]).unwrap(),
        payload_buffer_pool: PayloadBufferPool::new(ec_config),
        sse_c_validator: None,
        managed_key_provider: None,
    }
}

#[derive(Debug, Clone)]
enum ReclaimTraceSeed {
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReclaimTraceModel {
    metadata_exists: bool,
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

#[derive(Debug, Clone)]
enum TwoGenerationReclaimTraceSeed {
    Choice(u8),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TwoGenerationReclaimTraceOp {
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

impl std::fmt::Display for TwoGenerationReclaimTraceOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct TwoGenerationReclaimTraceModel {
    old_metadata_exists: bool,
    new_metadata_exists: bool,
    old_lease_held: bool,
    new_lease_held: bool,
    object_queue: Vec<TraceGeneration>,
    bucket_delete_queued: bool,
}

impl TwoGenerationReclaimTraceModel {
    fn new() -> Self {
        Self {
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

    fn apply(&mut self, op: &TwoGenerationReclaimTraceOp) {
        use TwoGenerationReclaimTraceOp::*;
        match op {
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
            WorkerBucketDeleteStep => self.bucket_delete_queued = false,
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

struct ReclaimTraceHarness {
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
                let meta_pg = self
                    .runtime
                    .storage_node
                    .get_pg(self.runtime.pg_topology.object_pg(TRACE_BUCKET, TRACE_KEY))
                    .map_err(|err| TestCaseError::fail(format!("get_pg failed: {err:?}")))?;
                meta_pg
                    .put_simple_payload_reclaim(&SimplePayloadReclaimRecord {
                        bucket: trusted_bucket_name(TRACE_BUCKET),
                        key: trusted_object_key(TRACE_KEY),
                        generation_id: trace_generation_id(),
                        ec: EcShape { k: 4, m: 2 },
                        created_at: 1,
                    })
                    .map_err(|err| {
                        TestCaseError::fail(format!("put_simple_payload_reclaim failed: {err:?}"))
                    })?;
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

    fn metadata_exists(&self) -> bool {
        let meta_pg = self
            .runtime
            .storage_node
            .get_pg(self.runtime.pg_topology.object_pg(TRACE_BUCKET, TRACE_KEY))
            .unwrap();
        PgMetadataStore::payload_reclaim_exists(
            &*meta_pg,
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
        let stop = Arc::new(AtomicBool::new(false));
        let waiter_stop = Arc::clone(&stop);
        let waiter_node = Arc::clone(&self.runtime.storage_node);
        let (tx, rx) = std::sync::mpsc::channel();
        let waiter = std::thread::spawn(move || {
            tx.send(waiter_node.wait_for_reclaim_work(&waiter_stop))
                .unwrap();
        });
        let result = match rx.recv_timeout(std::time::Duration::from_millis(25)) {
            Ok(work) => work,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => None,
            Err(err) => {
                return Err(TestCaseError::fail(format!(
                    "failed waiting for reclaim work: {err:?}"
                )))
            }
        };
        stop.store(true, Ordering::SeqCst);
        self.runtime.storage_node.wake_reclaim_workers();
        waiter.join().unwrap();
        Ok(result)
    }
}

struct TwoGenerationReclaimTraceHarness {
    runtime: ReadRuntime,
    old_lease: Option<PayloadLease>,
    new_lease: Option<PayloadLease>,
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
        let meta_pg = self
            .runtime
            .storage_node
            .get_pg(self.runtime.pg_topology.object_pg(TRACE_BUCKET, TRACE_KEY))
            .map_err(|err| TestCaseError::fail(format!("get_pg failed: {err:?}")))?;
        meta_pg
            .put_simple_payload_reclaim(&SimplePayloadReclaimRecord {
                bucket: trusted_bucket_name(TRACE_BUCKET),
                key: trusted_object_key(TRACE_KEY),
                generation_id: generation.generation_id(),
                ec: EcShape { k: 4, m: 2 },
                created_at: match generation {
                    TraceGeneration::Old => 1,
                    TraceGeneration::New => 2,
                },
            })
            .map_err(|err| {
                TestCaseError::fail(format!("put_simple_payload_reclaim failed: {err:?}"))
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
        let meta_pg = self
            .runtime
            .storage_node
            .get_pg(self.runtime.pg_topology.object_pg(TRACE_BUCKET, TRACE_KEY))
            .unwrap();
        PgMetadataStore::payload_reclaim_exists(
            &*meta_pg,
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
        let stop = Arc::new(AtomicBool::new(false));
        let waiter_stop = Arc::clone(&stop);
        let waiter_node = Arc::clone(&self.runtime.storage_node);
        let (tx, rx) = std::sync::mpsc::channel();
        let waiter = std::thread::spawn(move || {
            tx.send(waiter_node.wait_for_reclaim_work(&waiter_stop))
                .unwrap();
        });
        let result = match rx.recv_timeout(std::time::Duration::from_millis(25)) {
            Ok(work) => work,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => None,
            Err(err) => {
                return Err(TestCaseError::fail(format!(
                    "failed waiting for reclaim work: {err:?}"
                )))
            }
        };
        stop.store(true, Ordering::SeqCst);
        self.runtime.storage_node.wake_reclaim_workers();
        waiter.join().unwrap();
        Ok(result)
    }
}

fn render_reclaim_trace(ops: &[ReclaimTraceOp]) -> String {
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

fn assert_two_generation_reclaim_trace_matches_model(
    harness: &TwoGenerationReclaimTraceHarness,
    model: &TwoGenerationReclaimTraceModel,
    context: &str,
) -> TestCaseResult {
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
}
