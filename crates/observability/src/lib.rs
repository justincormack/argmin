use std::cell::RefCell;
use std::collections::VecDeque;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, TrySendError};
use std::sync::{Arc, Mutex, Once, OnceLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceContext {
    trace_id: Arc<str>,
    request_id: Arc<str>,
}

pub struct TraceContextIds {
    pub trace_id: String,
    pub request_id: String,
}

impl TraceContext {
    #[must_use]
    pub fn new_request() -> Self {
        static NEXT_TRACE_ID: AtomicU64 = AtomicU64::new(1);

        let now_micros = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros();
        let sequence = NEXT_TRACE_ID.fetch_add(1, Ordering::Relaxed);
        Self {
            trace_id: Arc::<str>::from(format!("{now_micros:016x}{sequence:016x}")),
            request_id: Arc::<str>::from(format!(
                "{:016X}",
                (now_micros as u64) ^ sequence.rotate_left(13)
            )),
        }
    }

    #[must_use]
    pub fn from_ids(ids: TraceContextIds) -> Self {
        Self {
            trace_id: Arc::<str>::from(ids.trace_id),
            request_id: Arc::<str>::from(ids.request_id),
        }
    }

    #[must_use]
    pub fn trace_id(&self) -> &str {
        &self.trace_id
    }

    #[must_use]
    pub fn request_id(&self) -> &str {
        &self.request_id
    }
}

#[derive(Clone)]
struct TraceState {
    context: TraceContext,
    depth: usize,
}

thread_local! {
    static TRACE_STATE: RefCell<Option<TraceState>> = const { RefCell::new(None) };
}

struct TraceConfig {
    enabled: bool,
    filters: Box<[Box<str>]>,
    file_path: Option<Box<str>>,
    sync_file: bool,
}

static TRACE_CONFIG: OnceLock<TraceConfig> = OnceLock::new();
static TRACE_CONFIG_OVERRIDE: OnceLock<TraceConfig> = OnceLock::new();

enum TraceSink {
    Stderr,
    AsyncFile(AsyncTraceSink),
    SyncFile(Mutex<BufWriter<File>>),
}

static TRACE_SINK: OnceLock<TraceSink> = OnceLock::new();
static TRACE_SINK_OVERRIDE: OnceLock<TraceSink> = OnceLock::new();
static REQUEST_START_TOTAL: AtomicU64 = AtomicU64::new(0);
static INFLIGHT_REQUESTS: AtomicU64 = AtomicU64::new(0);
static REQUEST_FINISH_TOTAL: AtomicU64 = AtomicU64::new(0);
static REQUEST_ERROR_TOTAL: AtomicU64 = AtomicU64::new(0);
static HTTP_500_RESPONSE_TOTAL: AtomicU64 = AtomicU64::new(0);
static OPERATION_ABORTED_RESPONSE_TOTAL: AtomicU64 = AtomicU64::new(0);
static SLOW_DOWN_RESPONSE_TOTAL: AtomicU64 = AtomicU64::new(0);
static SLOW_REQUEST_TOTAL: AtomicU64 = AtomicU64::new(0);
static REQUEST_ADMISSION_WAIT_TOTAL: AtomicU64 = AtomicU64::new(0);
static REQUEST_ADMISSION_WAIT_US_TOTAL: AtomicU64 = AtomicU64::new(0);
static REQUEST_ADMISSION_TIMEOUT_TOTAL: AtomicU64 = AtomicU64::new(0);
static STORAGE_RPC_ERROR_TOTAL: AtomicU64 = AtomicU64::new(0);
static STORAGE_RPC_ADMISSION_TOTAL: AtomicU64 = AtomicU64::new(0);
static STORAGE_RPC_ADMISSION_WAIT_TOTAL: AtomicU64 = AtomicU64::new(0);
static STORAGE_RPC_ADMISSION_WAIT_US_TOTAL: AtomicU64 = AtomicU64::new(0);
static STORAGE_RPC_ADMISSION_TIMEOUT_TOTAL: AtomicU64 = AtomicU64::new(0);
static STORAGE_RPC_ACTIVE_TOTAL: AtomicU64 = AtomicU64::new(0);
static STORAGE_RPC_ACTIVE_CONTROL: AtomicU64 = AtomicU64::new(0);
static STORAGE_RPC_ACTIVE_COMPLETION: AtomicU64 = AtomicU64::new(0);
static STORAGE_RPC_ACTIVE_PROGRESS: AtomicU64 = AtomicU64::new(0);
static STORAGE_RPC_ACTIVE_START_WRITE: AtomicU64 = AtomicU64::new(0);
static STORAGE_RPC_ACTIVE_READ: AtomicU64 = AtomicU64::new(0);
static STORAGE_RPC_ACTIVE_LIST: AtomicU64 = AtomicU64::new(0);
static STORAGE_RPC_PENDING_ENVELOPE_ACTIVE: AtomicU64 = AtomicU64::new(0);
static STORAGE_RPC_PENDING_ENVELOPE_STARTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static STORAGE_RPC_PENDING_ENVELOPE_COMPLETED_TOTAL: AtomicU64 = AtomicU64::new(0);
static STORAGE_RPC_PENDING_ENVELOPE_LONG_RUNNING_TOTAL: AtomicU64 = AtomicU64::new(0);
static STORAGE_RPC_PENDING_ENVELOPE_LONG_RUNNING_US_MAX: AtomicU64 = AtomicU64::new(0);
static STORAGE_RPC_PENDING_ENVELOPE_ACTIVE_SEQUENCE: AtomicU64 = AtomicU64::new(1);
static BUCKET_LOCK_WAIT_EXCEEDED_TOTAL: AtomicU64 = AtomicU64::new(0);
static SHARD_SCAVENGER_OBSERVATION_TOTAL: AtomicU64 = AtomicU64::new(0);
static SHARD_SCAVENGER_SCAN_INCOMPLETE_TOTAL: AtomicU64 = AtomicU64::new(0);
static METADATA_COMMAND_CONFLICT_TOTAL: AtomicU64 = AtomicU64::new(0);
static METADATA_COMMAND_PENDING_SLOT_ACTION_TOTAL: AtomicU64 = AtomicU64::new(0);
static METADATA_COMMAND_SESSION_WAIT_TOTAL: AtomicU64 = AtomicU64::new(0);
static METADATA_COMMAND_RECOVERY_LEADER_TOTAL: AtomicU64 = AtomicU64::new(0);
static METADATA_COMMAND_RECOVERY_WAIT_TOTAL: AtomicU64 = AtomicU64::new(0);
static METADATA_COMMAND_RECOVERY_WAIT_US_TOTAL: AtomicU64 = AtomicU64::new(0);
static METADATA_COMMAND_RECOVERY_WAIT_US_MAX: AtomicU64 = AtomicU64::new(0);
static METADATA_COMMAND_RECOVERY_TIMEOUT_TOTAL: AtomicU64 = AtomicU64::new(0);
static METADATA_COMMAND_RECOVERY_OUTCOME_TOTAL: AtomicU64 = AtomicU64::new(0);
static METADATA_COMMAND_BUDGET_EXHAUSTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static METADATA_COMMAND_BACKOFF_TOTAL: AtomicU64 = AtomicU64::new(0);
static METADATA_COMMAND_BACKOFF_US_TOTAL: AtomicU64 = AtomicU64::new(0);
static METADATA_COMMAND_BACKOFF_US_MAX: AtomicU64 = AtomicU64::new(0);
static RECLAIM_WORK_QUEUE_DEPTH: AtomicU64 = AtomicU64::new(0);
static OBJECT_PAYLOAD_RECLAIM_QUEUE_DEPTH: AtomicU64 = AtomicU64::new(0);
static OBJECT_PAYLOAD_RECLAIM_OUTSTANDING_DEPTH: AtomicU64 = AtomicU64::new(0);
static BUCKET_DELETE_BEGIN_QUEUE_DEPTH: AtomicU64 = AtomicU64::new(0);
static BUCKET_DELETE_FINALIZE_QUEUE_DEPTH: AtomicU64 = AtomicU64::new(0);
static BUCKET_DELETE_FINALIZE_OUTSTANDING_DEPTH: AtomicU64 = AtomicU64::new(0);
static RECLAIM_WORK_QUEUE_ACTION_TOTAL: AtomicU64 = AtomicU64::new(0);
static OBJECT_PAYLOAD_RECLAIM_EVENT_TOTAL: AtomicU64 = AtomicU64::new(0);
static OBJECT_PAYLOAD_RECLAIM_DURABLE_SCAN_TOTAL: AtomicU64 = AtomicU64::new(0);
static SHARD_REPAIR_QUEUE_DEPTH: AtomicU64 = AtomicU64::new(0);
static SHARD_REPAIR_EVENT_TOTAL: AtomicU64 = AtomicU64::new(0);
static SHARD_REPAIR_SHARDS_REWRITTEN_TOTAL: AtomicU64 = AtomicU64::new(0);
static SHARD_BACKFILL_QUEUE_DEPTH: AtomicU64 = AtomicU64::new(0);
static SHARD_BACKFILL_EVENT_TOTAL: AtomicU64 = AtomicU64::new(0);
static SHARD_BACKFILL_SHARDS_WRITTEN_TOTAL: AtomicU64 = AtomicU64::new(0);
static SHARD_BACKFILL_CANDIDATE_SCAN_TOTAL: AtomicU64 = AtomicU64::new(0);
static SHARD_BACKFILL_CANDIDATE_SCANNED_TOTAL: AtomicU64 = AtomicU64::new(0);
static SHARD_BACKFILL_CANDIDATE_CURRENT_EPOCH_TOTAL: AtomicU64 = AtomicU64::new(0);
static SHARD_BACKFILL_CANDIDATE_ALREADY_QUEUED_TOTAL: AtomicU64 = AtomicU64::new(0);
static SHARD_BACKFILL_CANDIDATE_ALREADY_COMPLETE_TOTAL: AtomicU64 = AtomicU64::new(0);
static SHARD_BACKFILL_CANDIDATE_ENQUEUED_TOTAL: AtomicU64 = AtomicU64::new(0);
static SHARD_BACKFILL_CANDIDATE_UNRECOVERABLE_TOTAL: AtomicU64 = AtomicU64::new(0);
static SHARD_BACKFILL_CANDIDATE_DEFERRED_TOTAL: AtomicU64 = AtomicU64::new(0);
static SHARD_BACKFILL_CANDIDATE_FAILED_TOTAL: AtomicU64 = AtomicU64::new(0);
static SHARD_BACKFILL_CANDIDATE_LIMIT_REACHED_TOTAL: AtomicU64 = AtomicU64::new(0);
static SHARD_BACKFILL_CANDIDATE_SCAN_ERROR_TOTAL: AtomicU64 = AtomicU64::new(0);
static METADATA_COMMAND_CHECKPOINT_RECORD_SCAN_TOTAL: AtomicU64 = AtomicU64::new(0);
static METADATA_COMMAND_CHECKPOINT_RECORD_SCANNED_TOTAL: AtomicU64 = AtomicU64::new(0);
static METADATA_COMMAND_CHECKPOINT_RECORD_RECORDED_TOTAL: AtomicU64 = AtomicU64::new(0);
static METADATA_COMMAND_CHECKPOINT_RECORD_ALREADY_CURRENT_TOTAL: AtomicU64 = AtomicU64::new(0);
static METADATA_COMMAND_CHECKPOINT_RECORD_SKIPPED_CADENCE_TOTAL: AtomicU64 = AtomicU64::new(0);
static METADATA_COMMAND_CHECKPOINT_RECORD_SKIPPED_INACTIVE_TOTAL: AtomicU64 = AtomicU64::new(0);
static METADATA_COMMAND_CHECKPOINT_RECORD_SKIPPED_EMPTY_TOTAL: AtomicU64 = AtomicU64::new(0);
static METADATA_COMMAND_CHECKPOINT_RECORD_SKIPPED_STALE_EPOCH_TOTAL: AtomicU64 = AtomicU64::new(0);
static METADATA_COMMAND_CHECKPOINT_RECORD_COMPACTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static METADATA_COMMAND_CHECKPOINT_RECORD_COMPACTION_DELETED_ENTRIES_TOTAL: AtomicU64 =
    AtomicU64::new(0);
static METADATA_COMMAND_CHECKPOINT_RECORD_COMPACTION_NOOP_TOTAL: AtomicU64 = AtomicU64::new(0);
static METADATA_COMMAND_CHECKPOINT_RECORD_COMPACTION_NO_CHECKPOINT_TOTAL: AtomicU64 =
    AtomicU64::new(0);
static METADATA_COMMAND_CHECKPOINT_RECORD_COMPACTION_PENDING_TOTAL: AtomicU64 = AtomicU64::new(0);
static METADATA_COMMAND_CHECKPOINT_RECORD_COMPACTION_FAILED_TOTAL: AtomicU64 = AtomicU64::new(0);
static METADATA_COMMAND_CHECKPOINT_RECORD_FAILED_TOTAL: AtomicU64 = AtomicU64::new(0);
static METADATA_COMMAND_CHECKPOINT_RECORD_LIMIT_REACHED_TOTAL: AtomicU64 = AtomicU64::new(0);
static METADATA_COMMAND_CHECKPOINT_RECORD_SCAN_ERROR_TOTAL: AtomicU64 = AtomicU64::new(0);
static METADATA_COMMAND_CHECKPOINT_RECORD_ERROR_DIMENSIONS: OnceLock<
    Mutex<Vec<MetadataCommandCheckpointRecordErrorDimensionCounter>>,
> = OnceLock::new();
static BACKGROUND_WORK_ADMISSION_EVENT_TOTAL: AtomicU64 = AtomicU64::new(0);
static BACKGROUND_WORK_ACTIVE_TOTAL: AtomicU64 = AtomicU64::new(0);
static BACKGROUND_WORK_FINISHED_TOTAL: AtomicU64 = AtomicU64::new(0);
static BACKGROUND_WORK_ELAPSED_US_TOTAL: AtomicU64 = AtomicU64::new(0);
static METADATA_COMMAND_CONFLICT_DIMENSIONS: OnceLock<Mutex<Vec<MetadataCommandDimensionCounter>>> =
    OnceLock::new();
static METADATA_COMMAND_PENDING_SLOT_ACTION_DIMENSIONS: OnceLock<
    Mutex<Vec<MetadataCommandDimensionCounter>>,
> = OnceLock::new();
static METADATA_COMMAND_RECOVERY_ADMISSION_DIMENSIONS: OnceLock<
    Mutex<Vec<MetadataCommandDimensionCounter>>,
> = OnceLock::new();
static METADATA_COMMAND_RECOVERY_OUTCOME_DIMENSIONS: OnceLock<
    Mutex<Vec<MetadataCommandDimensionCounter>>,
> = OnceLock::new();
static METADATA_COMMAND_BUDGET_DIMENSIONS: OnceLock<
    Mutex<Vec<MetadataCommandBudgetDimensionCounter>>,
> = OnceLock::new();
static METADATA_COMMAND_BACKOFF_DIMENSIONS: OnceLock<
    Mutex<Vec<MetadataCommandBackoffDimensionCounter>>,
> = OnceLock::new();
static RECLAIM_WORK_QUEUE_ACTION_DIMENSIONS: OnceLock<Mutex<Vec<ReclaimQueueDimensionCounter>>> =
    OnceLock::new();
static OBJECT_PAYLOAD_RECLAIM_EVENT_DIMENSIONS: OnceLock<
    Mutex<Vec<ObjectPayloadReclaimEventDimensionCounter>>,
> = OnceLock::new();
static OBJECT_PAYLOAD_RECLAIM_DURABLE_SCAN_DIMENSIONS: OnceLock<
    Mutex<Vec<ObjectPayloadReclaimEventDimensionCounter>>,
> = OnceLock::new();
static SHARD_REPAIR_EVENT_DIMENSIONS: OnceLock<Mutex<Vec<ShardRepairEventDimensionCounter>>> =
    OnceLock::new();
static SHARD_BACKFILL_EVENT_DIMENSIONS: OnceLock<Mutex<Vec<ShardBackfillEventDimensionCounter>>> =
    OnceLock::new();
static BACKGROUND_WORK_ADMISSION_DIMENSIONS: OnceLock<
    Mutex<Vec<BackgroundWorkAdmissionDimensionCounter>>,
> = OnceLock::new();
static STREAM_UPLOAD_ACTIVE_SESSIONS: AtomicU64 = AtomicU64::new(0);
static STREAM_UPLOAD_SESSION_CREATED_TOTAL: AtomicU64 = AtomicU64::new(0);
static STREAM_UPLOAD_SESSION_ABORTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static STREAM_UPLOAD_SESSION_FINALIZED_TOTAL: AtomicU64 = AtomicU64::new(0);
static STREAM_UPLOAD_BODY_STARTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static STREAM_UPLOAD_BODY_READ_COMPLETE_TOTAL: AtomicU64 = AtomicU64::new(0);
static STREAM_UPLOAD_SEGMENT_APPEND_STARTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static STREAM_UPLOAD_SEGMENT_APPEND_FINISHED_TOTAL: AtomicU64 = AtomicU64::new(0);
static STREAM_UPLOAD_SEGMENT_APPEND_ERROR_TOTAL: AtomicU64 = AtomicU64::new(0);
static STREAM_UPLOAD_FINALIZE_STARTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static STREAM_UPLOAD_FINALIZE_ERROR_TOTAL: AtomicU64 = AtomicU64::new(0);
static FLIGHT_RECORDER: OnceLock<Mutex<FlightRecorder>> = OnceLock::new();
static PANIC_FLIGHT_RECORDER_HOOK: Once = Once::new();
static FLIGHT_RECORD_SEQUENCE: AtomicU64 = AtomicU64::new(1);

const TRACE_FILE_QUEUE_CAPACITY: usize = 16_384;
const TRACE_FILE_IDLE_FLUSH_INTERVAL: Duration = Duration::from_millis(50);
const FLIGHT_RECORDER_CAPACITY: usize = 512;
const FLIGHT_RECORD_MAX_DETAIL_BYTES: usize = 1_024;
const METADATA_COMMAND_DIMENSION_CAPACITY: usize = 512;
const STORAGE_RPC_LONG_RUNNING_THRESHOLD: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FlightRecord {
    pub sequence: u64,
    pub ts_us: u128,
    pub trace_id: String,
    pub request_id: String,
    pub target: &'static str,
    pub event: &'static str,
    pub detail: String,
}

struct FlightRecorder {
    records: VecDeque<FlightRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataCommandDimensionSample {
    pub pg_id: u32,
    pub classifier: &'static str,
    pub command_kind: &'static str,
    pub count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataCommandBudgetDimensionSample {
    pub pg_id: Option<u32>,
    pub operation: &'static str,
    pub context: &'static str,
    pub count: u64,
    pub elapsed_us_total: u64,
    pub elapsed_us_max: u64,
    pub budget_us_max: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataCommandBackoffDimensionSample {
    pub pg_id: Option<u32>,
    pub operation: &'static str,
    pub context: &'static str,
    pub count: u64,
    pub sleep_us_total: u64,
    pub sleep_us_max: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReclaimQueueDimensionSample {
    pub work_kind: &'static str,
    pub action: &'static str,
    pub count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectPayloadReclaimEventDimensionSample {
    pub pg_id: u32,
    pub event: &'static str,
    pub count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardRepairEventDimensionSample {
    pub pg_id: Option<u32>,
    pub event: &'static str,
    pub count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardBackfillEventDimensionSample {
    pub pg_id: Option<u32>,
    pub event: &'static str,
    pub count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackgroundWorkAdmissionDimensionSample {
    pub class: &'static str,
    pub event: &'static str,
    pub count: u64,
    pub elapsed_us_total: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataCommandCheckpointRecordErrorDimensionSample {
    pub pg_id: u32,
    pub outcome: &'static str,
    pub error_kind: &'static str,
    pub count: u64,
}

struct MetadataCommandDimensionCounter {
    pg_id: u32,
    classifier: &'static str,
    command_kind: &'static str,
    count: u64,
}

struct MetadataCommandBudgetDimensionCounter {
    pg_id: Option<u32>,
    operation: &'static str,
    context: &'static str,
    count: u64,
    elapsed_us_total: u64,
    elapsed_us_max: u64,
    budget_us_max: u64,
}

struct MetadataCommandBackoffDimensionCounter {
    pg_id: Option<u32>,
    operation: &'static str,
    context: &'static str,
    count: u64,
    sleep_us_total: u64,
    sleep_us_max: u64,
}

struct ReclaimQueueDimensionCounter {
    work_kind: &'static str,
    action: &'static str,
    count: u64,
}

struct ObjectPayloadReclaimEventDimensionCounter {
    pg_id: u32,
    event: &'static str,
    count: u64,
}

struct ShardRepairEventDimensionCounter {
    pg_id: Option<u32>,
    event: &'static str,
    count: u64,
}

struct ShardBackfillEventDimensionCounter {
    pg_id: Option<u32>,
    event: &'static str,
    count: u64,
}

struct BackgroundWorkAdmissionDimensionCounter {
    class: &'static str,
    event: &'static str,
    count: u64,
    elapsed_us_total: u64,
}

struct MetadataCommandCheckpointRecordErrorDimensionCounter {
    pg_id: u32,
    outcome: &'static str,
    error_kind: &'static str,
    count: u64,
}

impl FlightRecorder {
    fn new() -> Self {
        Self {
            records: VecDeque::with_capacity(FLIGHT_RECORDER_CAPACITY),
        }
    }

    fn push(&mut self, record: FlightRecord) {
        if self.records.len() == FLIGHT_RECORDER_CAPACITY {
            self.records.pop_front();
        }
        self.records.push_back(record);
    }
}

struct AsyncTraceSink {
    sender: mpsc::SyncSender<Box<str>>,
    writer_failed: AtomicBool,
    dropped_lines: AtomicU64,
}

impl AsyncTraceSink {
    fn new(file: File) -> io::Result<Self> {
        let (sender, receiver) = mpsc::sync_channel::<Box<str>>(TRACE_FILE_QUEUE_CAPACITY);
        thread::Builder::new()
            .name("argmin-trace-writer".to_string())
            .spawn(move || trace_writer_loop(file, receiver))?;
        Ok(Self {
            sender,
            writer_failed: AtomicBool::new(false),
            dropped_lines: AtomicU64::new(0),
        })
    }

    fn write_line(&self, args: fmt::Arguments<'_>) {
        if self.writer_failed.load(Ordering::Relaxed) {
            write_stderr_line(args);
            return;
        }

        let line = fmt::format(args).into_boxed_str();
        match self.sender.try_send(line) {
            Ok(()) => {}
            Err(TrySendError::Full(_line)) => {
                self.dropped_lines.fetch_add(1, Ordering::Relaxed);
            }
            Err(TrySendError::Disconnected(line)) => {
                self.writer_failed.store(true, Ordering::Relaxed);
                write_stderr_str(&line);
            }
        }
    }
}

fn trace_writer_loop(file: File, receiver: mpsc::Receiver<Box<str>>) {
    let mut writer = BufWriter::new(file);
    loop {
        match receiver.recv_timeout(TRACE_FILE_IDLE_FLUSH_INTERVAL) {
            Ok(line) => {
                let _ = writer.write_all(line.as_bytes());
                let _ = writer.write_all(b"\n");
                while let Ok(line) = receiver.try_recv() {
                    let _ = writer.write_all(line.as_bytes());
                    let _ = writer.write_all(b"\n");
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                let _ = writer.flush();
            }
            Err(RecvTimeoutError::Disconnected) => {
                let _ = writer.flush();
                break;
            }
        }
    }
}

fn write_stderr_line(args: fmt::Arguments<'_>) {
    let mut stderr = io::stderr().lock();
    let _ = writeln!(stderr, "{args}");
}

fn write_stderr_str(line: &str) {
    let mut stderr = io::stderr().lock();
    let _ = writeln!(stderr, "{line}");
}

fn flight_recorder() -> &'static Mutex<FlightRecorder> {
    FLIGHT_RECORDER.get_or_init(|| Mutex::new(FlightRecorder::new()))
}

fn metadata_command_conflict_dimensions() -> &'static Mutex<Vec<MetadataCommandDimensionCounter>> {
    METADATA_COMMAND_CONFLICT_DIMENSIONS.get_or_init(|| Mutex::new(Vec::new()))
}

fn metadata_command_pending_slot_action_dimensions(
) -> &'static Mutex<Vec<MetadataCommandDimensionCounter>> {
    METADATA_COMMAND_PENDING_SLOT_ACTION_DIMENSIONS.get_or_init(|| Mutex::new(Vec::new()))
}

fn metadata_command_recovery_admission_dimensions(
) -> &'static Mutex<Vec<MetadataCommandDimensionCounter>> {
    METADATA_COMMAND_RECOVERY_ADMISSION_DIMENSIONS.get_or_init(|| Mutex::new(Vec::new()))
}

fn metadata_command_recovery_outcome_dimensions(
) -> &'static Mutex<Vec<MetadataCommandDimensionCounter>> {
    METADATA_COMMAND_RECOVERY_OUTCOME_DIMENSIONS.get_or_init(|| Mutex::new(Vec::new()))
}

fn metadata_command_budget_dimensions() -> &'static Mutex<Vec<MetadataCommandBudgetDimensionCounter>>
{
    METADATA_COMMAND_BUDGET_DIMENSIONS.get_or_init(|| Mutex::new(Vec::new()))
}

fn metadata_command_backoff_dimensions(
) -> &'static Mutex<Vec<MetadataCommandBackoffDimensionCounter>> {
    METADATA_COMMAND_BACKOFF_DIMENSIONS.get_or_init(|| Mutex::new(Vec::new()))
}

fn reclaim_work_queue_action_dimensions() -> &'static Mutex<Vec<ReclaimQueueDimensionCounter>> {
    RECLAIM_WORK_QUEUE_ACTION_DIMENSIONS.get_or_init(|| Mutex::new(Vec::new()))
}

fn object_payload_reclaim_event_dimensions(
) -> &'static Mutex<Vec<ObjectPayloadReclaimEventDimensionCounter>> {
    OBJECT_PAYLOAD_RECLAIM_EVENT_DIMENSIONS.get_or_init(|| Mutex::new(Vec::new()))
}

fn object_payload_reclaim_durable_scan_dimensions(
) -> &'static Mutex<Vec<ObjectPayloadReclaimEventDimensionCounter>> {
    OBJECT_PAYLOAD_RECLAIM_DURABLE_SCAN_DIMENSIONS.get_or_init(|| Mutex::new(Vec::new()))
}

fn shard_repair_event_dimensions() -> &'static Mutex<Vec<ShardRepairEventDimensionCounter>> {
    SHARD_REPAIR_EVENT_DIMENSIONS.get_or_init(|| Mutex::new(Vec::new()))
}

fn shard_backfill_event_dimensions() -> &'static Mutex<Vec<ShardBackfillEventDimensionCounter>> {
    SHARD_BACKFILL_EVENT_DIMENSIONS.get_or_init(|| Mutex::new(Vec::new()))
}

fn background_work_admission_dimensions(
) -> &'static Mutex<Vec<BackgroundWorkAdmissionDimensionCounter>> {
    BACKGROUND_WORK_ADMISSION_DIMENSIONS.get_or_init(|| Mutex::new(Vec::new()))
}

fn metadata_command_checkpoint_record_error_dimensions(
) -> &'static Mutex<Vec<MetadataCommandCheckpointRecordErrorDimensionCounter>> {
    METADATA_COMMAND_CHECKPOINT_RECORD_ERROR_DIMENSIONS.get_or_init(|| Mutex::new(Vec::new()))
}

fn fetch_max_atomic(counter: &AtomicU64, value: u64) {
    let mut current = counter.load(Ordering::Relaxed);
    while value > current {
        match counter.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(observed) => current = observed,
        }
    }
}

fn increment_metadata_command_dimension(
    counters: &Mutex<Vec<MetadataCommandDimensionCounter>>,
    pg_id: u32,
    classifier: &'static str,
    command_kind: &'static str,
) {
    let mut counters = counters.lock().unwrap_or_else(|err| err.into_inner());
    if let Some(counter) = counters.iter_mut().find(|counter| {
        counter.pg_id == pg_id
            && counter.classifier == classifier
            && counter.command_kind == command_kind
    }) {
        counter.count = counter.count.saturating_add(1);
        return;
    }
    if counters.len() < METADATA_COMMAND_DIMENSION_CAPACITY {
        counters.push(MetadataCommandDimensionCounter {
            pg_id,
            classifier,
            command_kind,
            count: 1,
        });
    }
}

fn metadata_command_dimension_snapshot(
    counters: &Mutex<Vec<MetadataCommandDimensionCounter>>,
) -> Vec<MetadataCommandDimensionSample> {
    counters
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .iter()
        .map(|counter| MetadataCommandDimensionSample {
            pg_id: counter.pg_id,
            classifier: counter.classifier,
            command_kind: counter.command_kind,
            count: counter.count,
        })
        .collect()
}

fn increment_metadata_command_budget_dimension(
    pg_id: Option<u32>,
    operation: &'static str,
    context: &'static str,
    elapsed_us: u64,
    budget_us: u64,
) {
    let mut counters = metadata_command_budget_dimensions()
        .lock()
        .unwrap_or_else(|err| err.into_inner());
    if let Some(counter) = counters.iter_mut().find(|counter| {
        counter.pg_id == pg_id && counter.operation == operation && counter.context == context
    }) {
        counter.count = counter.count.saturating_add(1);
        counter.elapsed_us_total = counter.elapsed_us_total.saturating_add(elapsed_us);
        counter.elapsed_us_max = counter.elapsed_us_max.max(elapsed_us);
        counter.budget_us_max = counter.budget_us_max.max(budget_us);
        return;
    }
    if counters.len() < METADATA_COMMAND_DIMENSION_CAPACITY {
        counters.push(MetadataCommandBudgetDimensionCounter {
            pg_id,
            operation,
            context,
            count: 1,
            elapsed_us_total: elapsed_us,
            elapsed_us_max: elapsed_us,
            budget_us_max: budget_us,
        });
    }
}

fn increment_metadata_command_backoff_dimension(
    pg_id: Option<u32>,
    operation: &'static str,
    context: &'static str,
    sleep_us: u64,
) {
    let mut counters = metadata_command_backoff_dimensions()
        .lock()
        .unwrap_or_else(|err| err.into_inner());
    if let Some(counter) = counters.iter_mut().find(|counter| {
        counter.pg_id == pg_id && counter.operation == operation && counter.context == context
    }) {
        counter.count = counter.count.saturating_add(1);
        counter.sleep_us_total = counter.sleep_us_total.saturating_add(sleep_us);
        counter.sleep_us_max = counter.sleep_us_max.max(sleep_us);
        return;
    }
    if counters.len() < METADATA_COMMAND_DIMENSION_CAPACITY {
        counters.push(MetadataCommandBackoffDimensionCounter {
            pg_id,
            operation,
            context,
            count: 1,
            sleep_us_total: sleep_us,
            sleep_us_max: sleep_us,
        });
    }
}

fn increment_reclaim_queue_dimension(work_kind: &'static str, action: &'static str) {
    let mut counters = reclaim_work_queue_action_dimensions()
        .lock()
        .unwrap_or_else(|err| err.into_inner());
    if let Some(counter) = counters
        .iter_mut()
        .find(|counter| counter.work_kind == work_kind && counter.action == action)
    {
        counter.count = counter.count.saturating_add(1);
        return;
    }
    if counters.len() < METADATA_COMMAND_DIMENSION_CAPACITY {
        counters.push(ReclaimQueueDimensionCounter {
            work_kind,
            action,
            count: 1,
        });
    }
}

fn increment_object_payload_reclaim_dimension(
    counters: &Mutex<Vec<ObjectPayloadReclaimEventDimensionCounter>>,
    pg_id: u32,
    event: &'static str,
) {
    let mut counters = counters.lock().unwrap_or_else(|err| err.into_inner());
    if let Some(counter) = counters
        .iter_mut()
        .find(|counter| counter.pg_id == pg_id && counter.event == event)
    {
        counter.count = counter.count.saturating_add(1);
        return;
    }
    if counters.len() < METADATA_COMMAND_DIMENSION_CAPACITY {
        counters.push(ObjectPayloadReclaimEventDimensionCounter {
            pg_id,
            event,
            count: 1,
        });
    }
}

fn increment_shard_repair_dimension(pg_id: Option<u32>, event: &'static str) {
    let mut counters = shard_repair_event_dimensions()
        .lock()
        .unwrap_or_else(|err| err.into_inner());
    if let Some(counter) = counters
        .iter_mut()
        .find(|counter| counter.pg_id == pg_id && counter.event == event)
    {
        counter.count = counter.count.saturating_add(1);
        return;
    }
    if counters.len() < METADATA_COMMAND_DIMENSION_CAPACITY {
        counters.push(ShardRepairEventDimensionCounter {
            pg_id,
            event,
            count: 1,
        });
    }
}

fn increment_shard_backfill_dimension(pg_id: Option<u32>, event: &'static str) {
    let mut counters = shard_backfill_event_dimensions()
        .lock()
        .unwrap_or_else(|err| err.into_inner());
    if let Some(counter) = counters
        .iter_mut()
        .find(|counter| counter.pg_id == pg_id && counter.event == event)
    {
        counter.count = counter.count.saturating_add(1);
        return;
    }
    if counters.len() < METADATA_COMMAND_DIMENSION_CAPACITY {
        counters.push(ShardBackfillEventDimensionCounter {
            pg_id,
            event,
            count: 1,
        });
    }
}

fn increment_background_work_admission_dimension(
    class: &'static str,
    event: &'static str,
    elapsed_us: Option<u64>,
) {
    let mut counters = background_work_admission_dimensions()
        .lock()
        .unwrap_or_else(|err| err.into_inner());
    if let Some(counter) = counters
        .iter_mut()
        .find(|counter| counter.class == class && counter.event == event)
    {
        counter.count = counter.count.saturating_add(1);
        if let Some(elapsed_us) = elapsed_us {
            counter.elapsed_us_total = counter.elapsed_us_total.saturating_add(elapsed_us);
        }
        return;
    }
    if counters.len() < METADATA_COMMAND_DIMENSION_CAPACITY {
        counters.push(BackgroundWorkAdmissionDimensionCounter {
            class,
            event,
            count: 1,
            elapsed_us_total: elapsed_us.unwrap_or(0),
        });
    }
}

fn increment_metadata_command_checkpoint_record_error_dimension(
    pg_id: u32,
    outcome: &'static str,
    error_kind: &'static str,
) {
    let mut counters = metadata_command_checkpoint_record_error_dimensions()
        .lock()
        .unwrap_or_else(|err| err.into_inner());
    if let Some(counter) = counters.iter_mut().find(|counter| {
        counter.pg_id == pg_id && counter.outcome == outcome && counter.error_kind == error_kind
    }) {
        counter.count = counter.count.saturating_add(1);
        return;
    }
    if counters.len() < METADATA_COMMAND_DIMENSION_CAPACITY {
        counters.push(MetadataCommandCheckpointRecordErrorDimensionCounter {
            pg_id,
            outcome,
            error_kind,
            count: 1,
        });
    }
}

#[must_use]
pub fn metadata_command_conflict_dimension_snapshot() -> Vec<MetadataCommandDimensionSample> {
    metadata_command_dimension_snapshot(metadata_command_conflict_dimensions())
}

#[must_use]
pub fn metadata_command_pending_slot_action_dimension_snapshot(
) -> Vec<MetadataCommandDimensionSample> {
    metadata_command_dimension_snapshot(metadata_command_pending_slot_action_dimensions())
}

#[must_use]
pub fn metadata_command_recovery_admission_dimension_snapshot(
) -> Vec<MetadataCommandDimensionSample> {
    metadata_command_dimension_snapshot(metadata_command_recovery_admission_dimensions())
}

#[must_use]
pub fn metadata_command_recovery_outcome_dimension_snapshot() -> Vec<MetadataCommandDimensionSample>
{
    metadata_command_dimension_snapshot(metadata_command_recovery_outcome_dimensions())
}

#[must_use]
pub fn metadata_command_budget_dimension_snapshot() -> Vec<MetadataCommandBudgetDimensionSample> {
    metadata_command_budget_dimensions()
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .iter()
        .map(|counter| MetadataCommandBudgetDimensionSample {
            pg_id: counter.pg_id,
            operation: counter.operation,
            context: counter.context,
            count: counter.count,
            elapsed_us_total: counter.elapsed_us_total,
            elapsed_us_max: counter.elapsed_us_max,
            budget_us_max: counter.budget_us_max,
        })
        .collect()
}

#[must_use]
pub fn metadata_command_backoff_dimension_snapshot() -> Vec<MetadataCommandBackoffDimensionSample> {
    metadata_command_backoff_dimensions()
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .iter()
        .map(|counter| MetadataCommandBackoffDimensionSample {
            pg_id: counter.pg_id,
            operation: counter.operation,
            context: counter.context,
            count: counter.count,
            sleep_us_total: counter.sleep_us_total,
            sleep_us_max: counter.sleep_us_max,
        })
        .collect()
}

#[must_use]
pub fn reclaim_work_queue_action_dimension_snapshot() -> Vec<ReclaimQueueDimensionSample> {
    reclaim_work_queue_action_dimensions()
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .iter()
        .map(|counter| ReclaimQueueDimensionSample {
            work_kind: counter.work_kind,
            action: counter.action,
            count: counter.count,
        })
        .collect()
}

#[must_use]
pub fn object_payload_reclaim_event_dimension_snapshot(
) -> Vec<ObjectPayloadReclaimEventDimensionSample> {
    object_payload_reclaim_event_dimensions()
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .iter()
        .map(|counter| ObjectPayloadReclaimEventDimensionSample {
            pg_id: counter.pg_id,
            event: counter.event,
            count: counter.count,
        })
        .collect()
}

#[must_use]
pub fn object_payload_reclaim_durable_scan_dimension_snapshot(
) -> Vec<ObjectPayloadReclaimEventDimensionSample> {
    object_payload_reclaim_durable_scan_dimensions()
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .iter()
        .map(|counter| ObjectPayloadReclaimEventDimensionSample {
            pg_id: counter.pg_id,
            event: counter.event,
            count: counter.count,
        })
        .collect()
}

#[must_use]
pub fn background_work_admission_dimension_snapshot() -> Vec<BackgroundWorkAdmissionDimensionSample>
{
    background_work_admission_dimensions()
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .iter()
        .map(|counter| BackgroundWorkAdmissionDimensionSample {
            class: counter.class,
            event: counter.event,
            count: counter.count,
            elapsed_us_total: counter.elapsed_us_total,
        })
        .collect()
}

#[must_use]
pub fn shard_repair_event_dimension_snapshot() -> Vec<ShardRepairEventDimensionSample> {
    shard_repair_event_dimensions()
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .iter()
        .map(|counter| ShardRepairEventDimensionSample {
            pg_id: counter.pg_id,
            event: counter.event,
            count: counter.count,
        })
        .collect()
}

#[must_use]
pub fn shard_backfill_event_dimension_snapshot() -> Vec<ShardBackfillEventDimensionSample> {
    shard_backfill_event_dimensions()
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .iter()
        .map(|counter| ShardBackfillEventDimensionSample {
            pg_id: counter.pg_id,
            event: counter.event,
            count: counter.count,
        })
        .collect()
}

#[must_use]
pub fn metadata_command_checkpoint_record_error_dimension_snapshot(
) -> Vec<MetadataCommandCheckpointRecordErrorDimensionSample> {
    metadata_command_checkpoint_record_error_dimensions()
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .iter()
        .map(
            |counter| MetadataCommandCheckpointRecordErrorDimensionSample {
                pg_id: counter.pg_id,
                outcome: counter.outcome,
                error_kind: counter.error_kind,
                count: counter.count,
            },
        )
        .collect()
}

fn truncate_detail(mut detail: String) -> String {
    if detail.len() <= FLIGHT_RECORD_MAX_DETAIL_BYTES {
        return detail;
    }

    let mut end = FLIGHT_RECORD_MAX_DETAIL_BYTES;
    while !detail.is_char_boundary(end) {
        end -= 1;
    }
    detail.truncate(end);
    detail.push_str("...");
    detail
}

fn stable_hash_hex(value: &str) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

fn request_flight_detail(summary: RequestSummary<'_>, suffix: fmt::Arguments<'_>) -> String {
    truncate_detail(format!(
        "status={} method={} path_hash={} has_query={} query_params={} sigv4_query={} streaming={} body_len={} bytes_sent={} lifetime_us={} {}",
        summary.status_code,
        summary.method,
        stable_hash_hex(summary.path),
        summary.query.has_query(),
        summary.query.param_count(),
        summary.query.has_sigv4_params(),
        summary.streaming,
        summary.body_len,
        summary.bytes_sent,
        summary.lifetime_us,
        suffix
    ))
}

pub fn record_flight_event(
    context: &TraceContext,
    target: &'static str,
    event: &'static str,
    detail: impl Into<String>,
) {
    let record = FlightRecord {
        sequence: FLIGHT_RECORD_SEQUENCE.fetch_add(1, Ordering::Relaxed),
        ts_us: unix_micros(),
        trace_id: context.trace_id().to_string(),
        request_id: context.request_id().to_string(),
        target,
        event,
        detail: truncate_detail(detail.into()),
    };
    let mut recorder = flight_recorder()
        .lock()
        .unwrap_or_else(|err| err.into_inner());
    recorder.push(record);
}

pub fn emit_flight_event(
    target: &'static str,
    event: &'static str,
    detail: impl Into<String>,
) -> bool {
    let Some(context) = current_context() else {
        return false;
    };
    let detail = detail.into();
    record_flight_event(&context, target, event, detail.clone());
    event_in_context(&context, target, event, Some(format_args!("{detail}")))
}

#[must_use]
pub fn flight_recorder_snapshot() -> Vec<FlightRecord> {
    flight_recorder()
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .records
        .iter()
        .cloned()
        .collect()
}

pub fn dump_flight_recorder_to_stderr(reason: &str) {
    let records = flight_recorder_snapshot();
    let mut stderr = io::stderr().lock();
    let _ = writeln!(
        stderr,
        "== argmin flight recorder dump reason={} records={} ==",
        escaped(reason),
        records.len()
    );
    for record in records {
        let _ = writeln!(
            stderr,
            "flight seq={} ts_us={} trace_id={} request_id={} target={} event={} {}",
            record.sequence,
            record.ts_us,
            record.trace_id,
            record.request_id,
            record.target,
            record.event,
            record.detail
        );
    }
}

pub fn install_panic_flight_recorder_hook() {
    PANIC_FLIGHT_RECORDER_HOOK.call_once(|| {
        let previous_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            dump_flight_recorder_to_stderr("panic");
            previous_hook(info);
        }));
    });
}

fn trace_config() -> &'static TraceConfig {
    TRACE_CONFIG_OVERRIDE
        .get()
        .unwrap_or_else(|| TRACE_CONFIG.get_or_init(parse_trace_config_from_env))
}

fn init_trace_sink(config: &TraceConfig) -> TraceSink {
    let Some(path) = config.file_path.as_deref() else {
        return TraceSink::Stderr;
    };

    let path = Path::new(path);
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        if let Err(err) = fs::create_dir_all(parent) {
            let _ = writeln!(
                io::stderr(),
                "observability: failed to create trace dir {}: {}",
                parent.display(),
                err
            );
            return TraceSink::Stderr;
        }
    }

    match OpenOptions::new().create(true).append(true).open(path) {
        Ok(file) if config.sync_file => TraceSink::SyncFile(Mutex::new(BufWriter::new(file))),
        Ok(file) => match AsyncTraceSink::new(file) {
            Ok(sink) => TraceSink::AsyncFile(sink),
            Err(err) => {
                let _ = writeln!(
                    io::stderr(),
                    "observability: failed to start trace writer thread for {}: {}",
                    path.display(),
                    err
                );
                match OpenOptions::new().create(true).append(true).open(path) {
                    Ok(file) => TraceSink::SyncFile(Mutex::new(BufWriter::new(file))),
                    Err(err) => {
                        let _ = writeln!(
                            io::stderr(),
                            "observability: failed to reopen trace file {}: {}",
                            path.display(),
                            err
                        );
                        TraceSink::Stderr
                    }
                }
            }
        },
        Err(err) => {
            let _ = writeln!(
                io::stderr(),
                "observability: failed to open trace file {}: {}",
                path.display(),
                err
            );
            TraceSink::Stderr
        }
    }
}

fn parse_trace_config_from_env() -> TraceConfig {
    let enabled = std::env::var("ARGMIN_TRACE")
        .ok()
        .is_some_and(|value| matches_enabled(value.trim()));
    let filters = parse_filters(std::env::var("ARGMIN_TRACE_FILTER").ok().as_deref());
    let file_path = normalize_file_path(std::env::var("ARGMIN_TRACE_FILE").ok().as_deref());
    TraceConfig {
        enabled,
        filters,
        file_path,
        sync_file: std::env::var("ARGMIN_TRACE_SYNC")
            .ok()
            .is_some_and(|value| matches_enabled(value.trim())),
    }
}

fn trace_sink() -> &'static TraceSink {
    if let Some(config) = TRACE_CONFIG_OVERRIDE.get() {
        return TRACE_SINK_OVERRIDE.get_or_init(|| init_trace_sink(config));
    }

    TRACE_SINK.get_or_init(|| init_trace_sink(trace_config()))
}

fn parse_filters(value: Option<&str>) -> Box<[Box<str>]> {
    value
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(Box::<str>::from)
                .collect::<Vec<_>>()
                .into_boxed_slice()
        })
        .unwrap_or_default()
}

fn normalize_file_path(value: Option<&str>) -> Option<Box<str>> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(Box::<str>::from)
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Escaped<'a> {
    value: &'a str,
}

#[must_use]
pub fn escaped(value: &str) -> Escaped<'_> {
    Escaped { value }
}

impl fmt::Display for Escaped<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.value, f)
    }
}

impl fmt::Debug for Escaped<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.value, f)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Redacted {
    label: &'static str,
}

#[must_use]
pub fn redacted(label: &'static str) -> Redacted {
    Redacted { label }
}

impl fmt::Display for Redacted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<redacted:{}>", self.label)
    }
}

impl fmt::Debug for Redacted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QuerySummary {
    has_query: bool,
    param_count: usize,
    has_sigv4_params: bool,
}

#[must_use]
pub fn query_summary(query: &str) -> QuerySummary {
    let has_query = !query.is_empty();
    let param_count = if has_query {
        query.split('&').filter(|part| !part.is_empty()).count()
    } else {
        0
    };
    let has_sigv4_params = query
        .split('&')
        .filter_map(|part| {
            let key = part.split_once('=').map_or(part, |(key, _)| key);
            (!key.is_empty()).then_some(key)
        })
        .any(|key| {
            key.eq_ignore_ascii_case("X-Amz-Algorithm")
                || key.eq_ignore_ascii_case("X-Amz-Credential")
                || key.eq_ignore_ascii_case("X-Amz-Signature")
                || key.eq_ignore_ascii_case("X-Amz-Security-Token")
                || key.eq_ignore_ascii_case("X-Amz-Date")
                || key.eq_ignore_ascii_case("X-Amz-Expires")
                || key.eq_ignore_ascii_case("X-Amz-SignedHeaders")
        });

    QuerySummary {
        has_query,
        param_count,
        has_sigv4_params,
    }
}

impl QuerySummary {
    #[must_use]
    pub fn has_query(self) -> bool {
        self.has_query
    }

    #[must_use]
    pub fn param_count(self) -> usize {
        self.param_count
    }

    #[must_use]
    pub fn has_sigv4_params(self) -> bool {
        self.has_sigv4_params
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequestSummary<'a> {
    pub method: &'a str,
    pub path: &'a str,
    pub query: QuerySummary,
    pub status_code: u16,
    pub streaming: bool,
    pub body_len: u64,
    pub bytes_sent: u64,
    pub lifetime_us: u128,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequestStartSummary<'a> {
    pub method: &'a str,
    pub path: &'a str,
    pub query: QuerySummary,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequestAdmissionWaitSummary<'a> {
    pub method: &'a str,
    pub path: &'a str,
    pub query: QuerySummary,
    pub wait_us: u128,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequestAdmissionTimeoutSummary<'a> {
    pub method: &'a str,
    pub path: &'a str,
    pub query: QuerySummary,
    pub wait_us: u128,
    pub timeout_us: u128,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamUploadPhase {
    SessionCreated,
    SessionAborted,
    SessionFinalized,
    BodyStarted,
    BodyReadComplete,
    SegmentAppendStarted,
    SegmentAppendFinished,
    SegmentAppendError,
    FinalizeStarted,
    FinalizeError,
}

impl StreamUploadPhase {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SessionCreated => "session_created",
            Self::SessionAborted => "session_aborted",
            Self::SessionFinalized => "session_finalized",
            Self::BodyStarted => "body_started",
            Self::BodyReadComplete => "body_read_complete",
            Self::SegmentAppendStarted => "segment_append_started",
            Self::SegmentAppendFinished => "segment_append_finished",
            Self::SegmentAppendError => "segment_append_error",
            Self::FinalizeStarted => "finalize_started",
            Self::FinalizeError => "finalize_error",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamUploadPhaseSummary<'a> {
    pub operation: &'static str,
    pub phase: StreamUploadPhase,
    pub bucket: &'a str,
    pub key: &'a str,
    pub upload_id: Option<&'a str>,
    pub part_number: Option<u32>,
    pub session_id: Option<&'a str>,
    pub segment_index: Option<u32>,
    pub body_bytes_received: Option<u64>,
    pub segment_bytes: Option<u64>,
    pub segment_count: Option<u32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShardScavengerObservationSummary<'a> {
    pub node_id: u32,
    pub data_pg_id: u32,
    pub shard_index: u8,
    pub shard_key_hex: &'a str,
    pub reason: &'static str,
    pub file_exists: bool,
    pub shard_row_exists: bool,
    pub last_error: Option<&'a str>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MetadataCommandConflictSummary {
    pub node_id: Option<u32>,
    pub pg_id: u32,
    pub cluster_epoch: u64,
    pub log_index: Option<u64>,
    pub kind: &'static str,
    pub command_kind: Option<&'static str>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MetadataCommandPendingSlotActionSummary {
    pub node_id: Option<u32>,
    pub pg_id: u32,
    pub cluster_epoch: u64,
    pub log_index: Option<u64>,
    pub action: &'static str,
    pub command_kind: Option<&'static str>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MetadataCommandSessionWaitSummary {
    pub node_id: u32,
    pub pg_id: u32,
    pub wait_us: u128,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MetadataCommandRecoveryAdmissionSummary {
    pub node_id: Option<u32>,
    pub pg_id: u32,
    pub cluster_epoch: u64,
    pub log_index: Option<u64>,
    pub admission: &'static str,
    pub command_kind: Option<&'static str>,
    pub wait_us: u128,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MetadataCommandRecoveryOutcomeSummary {
    pub node_id: Option<u32>,
    pub pg_id: u32,
    pub cluster_epoch: u64,
    pub log_index: Option<u64>,
    pub outcome: &'static str,
    pub command_kind: Option<&'static str>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MetadataCommandBudgetExhaustedSummary {
    pub pg_id: Option<u32>,
    pub operation: &'static str,
    pub context: &'static str,
    pub elapsed_us: u128,
    pub budget_us: u128,
    pub attempts: usize,
    pub max_attempts: Option<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MetadataCommandBackoffSummary {
    pub pg_id: Option<u32>,
    pub operation: &'static str,
    pub context: &'static str,
    pub sleep_us: u128,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReclaimQueueSummary {
    pub work_kind: &'static str,
    pub action: &'static str,
    pub queue_depth: usize,
    pub object_payload_depth: usize,
    pub object_payload_outstanding_depth: usize,
    pub bucket_delete_begin_depth: usize,
    pub bucket_delete_finalize_depth: usize,
    pub bucket_delete_finalize_outstanding_depth: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObjectPayloadReclaimEventSummary {
    pub pg_id: u32,
    pub event: &'static str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShardRepairEventSummary {
    pub pg_id: Option<u32>,
    pub event: &'static str,
    pub queue_depth: Option<usize>,
    pub shards_rewritten: Option<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShardBackfillEventSummary {
    pub pg_id: Option<u32>,
    pub event: &'static str,
    pub queue_depth: Option<usize>,
    pub shards_written: Option<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShardBackfillCandidateScanSummary {
    pub scanned: usize,
    pub current_epoch: usize,
    pub already_queued: usize,
    pub already_complete: usize,
    pub enqueued: usize,
    pub unrecoverable: usize,
    pub deferred: usize,
    pub failed: usize,
    pub limit_reached: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MetadataCommandCheckpointRecordSummary {
    pub scanned: usize,
    pub recorded: usize,
    pub already_current: usize,
    pub skipped_cadence: usize,
    pub skipped_inactive: usize,
    pub skipped_empty: usize,
    pub skipped_stale_epoch: usize,
    pub compacted: usize,
    pub compaction_deleted_entries: u64,
    pub compaction_noop: usize,
    pub compaction_no_checkpoint: usize,
    pub compaction_pending: usize,
    pub compaction_failed: usize,
    pub failed: usize,
    pub limit_reached: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MetadataCommandCheckpointRecordErrorSummary {
    pub pg_id: u32,
    pub outcome: &'static str,
    pub error_kind: &'static str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackgroundWorkClass {
    KnownDamageRepair,
    BackfillCandidateScan,
    RoutineBackfill,
    ReclaimCleanup,
    LifecycleCleanup,
    StreamSessionCleanup,
    OpportunisticScan,
    RoutineMetadataCheckpoint,
}

impl BackgroundWorkClass {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::KnownDamageRepair => "known_damage_repair",
            Self::BackfillCandidateScan => "backfill_candidate_scan",
            Self::RoutineBackfill => "routine_backfill",
            Self::ReclaimCleanup => "reclaim_cleanup",
            Self::LifecycleCleanup => "lifecycle_cleanup",
            Self::StreamSessionCleanup => "stream_session_cleanup",
            Self::OpportunisticScan => "opportunistic_scan",
            Self::RoutineMetadataCheckpoint => "routine_metadata_checkpoint",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackgroundWorkAdmissionEvent {
    Admitted,
    Finished,
    DeniedLimit,
    DeniedForegroundPressure,
    DeniedKnownDamageActive,
    DeniedBacklogPressure,
}

impl BackgroundWorkAdmissionEvent {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Admitted => "admitted",
            Self::Finished => "finished",
            Self::DeniedLimit => "denied_limit",
            Self::DeniedForegroundPressure => "denied_foreground_pressure",
            Self::DeniedKnownDamageActive => "denied_known_damage_active",
            Self::DeniedBacklogPressure => "denied_backlog_pressure",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BackgroundWorkAdmissionSummary {
    pub class: BackgroundWorkClass,
    pub event: BackgroundWorkAdmissionEvent,
    pub active_total: usize,
    pub elapsed_us: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageRpcAdmissionClass {
    Control,
    Completion,
    Progress,
    StartWrite,
    Read,
    List,
}

impl StorageRpcAdmissionClass {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Control => "control",
            Self::Completion => "completion",
            Self::Progress => "progress",
            Self::StartWrite => "start_write",
            Self::Read => "read",
            Self::List => "list",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageRpcErrorSummary<'a> {
    pub node_id: u32,
    pub rpc_kind: &'a str,
    pub error_code: &'a str,
    pub message: &'a str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageRpcAdmissionWaitSummary<'a> {
    pub node_id: u32,
    pub rpc_kind: &'a str,
    pub admission_class: StorageRpcAdmissionClass,
    pub wait_us: u128,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageRpcAdmissionTimeoutSummary<'a> {
    pub node_id: u32,
    pub rpc_kind: &'a str,
    pub admission_class: StorageRpcAdmissionClass,
    pub wait_us: u128,
    pub timeout_us: u128,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageRpcActiveSummary<'a> {
    pub node_id: u32,
    pub rpc_request_id: u64,
    pub rpc_kind: &'a str,
    pub admission_class: &'a str,
}

#[derive(Clone, Copy, Debug)]
struct StorageRpcPendingEnvelopeActiveRecord {
    sequence: u64,
    node_id: u32,
    rpc_request_id: u64,
    started_at: Instant,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub request_start_total: u64,
    pub inflight_requests: u64,
    pub request_finish_total: u64,
    pub request_error_total: u64,
    pub http_500_response_total: u64,
    pub operation_aborted_response_total: u64,
    pub slow_down_response_total: u64,
    pub slow_request_total: u64,
    pub request_admission_wait_total: u64,
    pub request_admission_wait_us_total: u64,
    pub request_admission_timeout_total: u64,
    pub storage_rpc_error_total: u64,
    pub storage_rpc_admission_total: u64,
    pub storage_rpc_admission_wait_total: u64,
    pub storage_rpc_admission_wait_us_total: u64,
    pub storage_rpc_admission_timeout_total: u64,
    pub storage_rpc_active_total: u64,
    pub storage_rpc_active_control: u64,
    pub storage_rpc_active_completion: u64,
    pub storage_rpc_active_progress: u64,
    pub storage_rpc_active_start_write: u64,
    pub storage_rpc_active_read: u64,
    pub storage_rpc_active_list: u64,
    pub storage_rpc_pending_envelope_active: u64,
    pub storage_rpc_pending_envelope_started_total: u64,
    pub storage_rpc_pending_envelope_completed_total: u64,
    pub storage_rpc_pending_envelope_long_running_total: u64,
    pub storage_rpc_pending_envelope_long_running_us_max: u64,
    pub storage_rpc_pending_envelope_oldest_active_us: u64,
    pub storage_rpc_pending_envelope_oldest_active_node_id: u64,
    pub storage_rpc_pending_envelope_oldest_active_request_id: u64,
    pub bucket_lock_wait_exceeded_total: u64,
    pub shard_scavenger_observation_total: u64,
    pub shard_scavenger_scan_incomplete_total: u64,
    pub metadata_command_conflict_total: u64,
    pub metadata_command_pending_slot_action_total: u64,
    pub metadata_command_session_wait_total: u64,
    pub metadata_command_recovery_leader_total: u64,
    pub metadata_command_recovery_wait_total: u64,
    pub metadata_command_recovery_wait_us_total: u64,
    pub metadata_command_recovery_wait_us_max: u64,
    pub metadata_command_recovery_timeout_total: u64,
    pub metadata_command_recovery_outcome_total: u64,
    pub metadata_command_budget_exhausted_total: u64,
    pub metadata_command_backoff_total: u64,
    pub metadata_command_backoff_us_total: u64,
    pub metadata_command_backoff_us_max: u64,
    pub reclaim_work_queue_depth: u64,
    pub object_payload_reclaim_queue_depth: u64,
    pub object_payload_reclaim_outstanding_depth: u64,
    pub bucket_delete_begin_queue_depth: u64,
    pub bucket_delete_finalize_queue_depth: u64,
    pub bucket_delete_finalize_outstanding_depth: u64,
    pub reclaim_work_queue_action_total: u64,
    pub object_payload_reclaim_event_total: u64,
    pub object_payload_reclaim_durable_scan_total: u64,
    pub shard_repair_queue_depth: u64,
    pub shard_repair_event_total: u64,
    pub shard_repair_shards_rewritten_total: u64,
    pub shard_backfill_queue_depth: u64,
    pub shard_backfill_event_total: u64,
    pub shard_backfill_shards_written_total: u64,
    pub shard_backfill_candidate_scan_total: u64,
    pub shard_backfill_candidate_scanned_total: u64,
    pub shard_backfill_candidate_current_epoch_total: u64,
    pub shard_backfill_candidate_already_queued_total: u64,
    pub shard_backfill_candidate_already_complete_total: u64,
    pub shard_backfill_candidate_enqueued_total: u64,
    pub shard_backfill_candidate_unrecoverable_total: u64,
    pub shard_backfill_candidate_deferred_total: u64,
    pub shard_backfill_candidate_failed_total: u64,
    pub shard_backfill_candidate_limit_reached_total: u64,
    pub shard_backfill_candidate_scan_error_total: u64,
    pub metadata_command_checkpoint_record_scan_total: u64,
    pub metadata_command_checkpoint_record_scanned_total: u64,
    pub metadata_command_checkpoint_record_recorded_total: u64,
    pub metadata_command_checkpoint_record_already_current_total: u64,
    pub metadata_command_checkpoint_record_skipped_cadence_total: u64,
    pub metadata_command_checkpoint_record_skipped_inactive_total: u64,
    pub metadata_command_checkpoint_record_skipped_empty_total: u64,
    pub metadata_command_checkpoint_record_skipped_stale_epoch_total: u64,
    pub metadata_command_checkpoint_record_compacted_total: u64,
    pub metadata_command_checkpoint_record_compaction_deleted_entries_total: u64,
    pub metadata_command_checkpoint_record_compaction_noop_total: u64,
    pub metadata_command_checkpoint_record_compaction_no_checkpoint_total: u64,
    pub metadata_command_checkpoint_record_compaction_pending_total: u64,
    pub metadata_command_checkpoint_record_compaction_failed_total: u64,
    pub metadata_command_checkpoint_record_failed_total: u64,
    pub metadata_command_checkpoint_record_limit_reached_total: u64,
    pub metadata_command_checkpoint_record_scan_error_total: u64,
    pub background_work_admission_event_total: u64,
    pub background_work_active_total: u64,
    pub background_work_finished_total: u64,
    pub background_work_elapsed_us_total: u64,
    pub stream_upload_active_sessions: u64,
    pub stream_upload_session_created_total: u64,
    pub stream_upload_session_aborted_total: u64,
    pub stream_upload_session_finalized_total: u64,
    pub stream_upload_body_started_total: u64,
    pub stream_upload_body_read_complete_total: u64,
    pub stream_upload_segment_append_started_total: u64,
    pub stream_upload_segment_append_finished_total: u64,
    pub stream_upload_segment_append_error_total: u64,
    pub stream_upload_finalize_started_total: u64,
    pub stream_upload_finalize_error_total: u64,
}

impl MetricsSnapshot {
    pub fn iter_named(&self) -> impl Iterator<Item = (&'static str, u64)> {
        [
            ("request_start_total", self.request_start_total),
            ("inflight_requests", self.inflight_requests),
            ("request_finish_total", self.request_finish_total),
            ("request_error_total", self.request_error_total),
            ("http_500_response_total", self.http_500_response_total),
            (
                "operation_aborted_response_total",
                self.operation_aborted_response_total,
            ),
            ("slow_down_response_total", self.slow_down_response_total),
            ("slow_request_total", self.slow_request_total),
            (
                "request_admission_wait_total",
                self.request_admission_wait_total,
            ),
            (
                "request_admission_wait_us_total",
                self.request_admission_wait_us_total,
            ),
            (
                "request_admission_timeout_total",
                self.request_admission_timeout_total,
            ),
            ("storage_rpc_error_total", self.storage_rpc_error_total),
            (
                "storage_rpc_admission_total",
                self.storage_rpc_admission_total,
            ),
            (
                "storage_rpc_admission_wait_total",
                self.storage_rpc_admission_wait_total,
            ),
            (
                "storage_rpc_admission_wait_us_total",
                self.storage_rpc_admission_wait_us_total,
            ),
            (
                "storage_rpc_admission_timeout_total",
                self.storage_rpc_admission_timeout_total,
            ),
            ("storage_rpc_active_total", self.storage_rpc_active_total),
            (
                "storage_rpc_active_control",
                self.storage_rpc_active_control,
            ),
            (
                "storage_rpc_active_completion",
                self.storage_rpc_active_completion,
            ),
            (
                "storage_rpc_active_progress",
                self.storage_rpc_active_progress,
            ),
            (
                "storage_rpc_active_start_write",
                self.storage_rpc_active_start_write,
            ),
            ("storage_rpc_active_read", self.storage_rpc_active_read),
            ("storage_rpc_active_list", self.storage_rpc_active_list),
            (
                "storage_rpc_pending_envelope_active",
                self.storage_rpc_pending_envelope_active,
            ),
            (
                "storage_rpc_pending_envelope_started_total",
                self.storage_rpc_pending_envelope_started_total,
            ),
            (
                "storage_rpc_pending_envelope_completed_total",
                self.storage_rpc_pending_envelope_completed_total,
            ),
            (
                "storage_rpc_pending_envelope_long_running_total",
                self.storage_rpc_pending_envelope_long_running_total,
            ),
            (
                "storage_rpc_pending_envelope_long_running_us_max",
                self.storage_rpc_pending_envelope_long_running_us_max,
            ),
            (
                "storage_rpc_pending_envelope_oldest_active_us",
                self.storage_rpc_pending_envelope_oldest_active_us,
            ),
            (
                "storage_rpc_pending_envelope_oldest_active_node_id",
                self.storage_rpc_pending_envelope_oldest_active_node_id,
            ),
            (
                "storage_rpc_pending_envelope_oldest_active_request_id",
                self.storage_rpc_pending_envelope_oldest_active_request_id,
            ),
            (
                "bucket_lock_wait_exceeded_total",
                self.bucket_lock_wait_exceeded_total,
            ),
            (
                "shard_scavenger_observation_total",
                self.shard_scavenger_observation_total,
            ),
            (
                "shard_scavenger_scan_incomplete_total",
                self.shard_scavenger_scan_incomplete_total,
            ),
            (
                "metadata_command_conflict_total",
                self.metadata_command_conflict_total,
            ),
            (
                "metadata_command_pending_slot_action_total",
                self.metadata_command_pending_slot_action_total,
            ),
            (
                "metadata_command_session_wait_total",
                self.metadata_command_session_wait_total,
            ),
            (
                "metadata_command_recovery_leader_total",
                self.metadata_command_recovery_leader_total,
            ),
            (
                "metadata_command_recovery_wait_total",
                self.metadata_command_recovery_wait_total,
            ),
            (
                "metadata_command_recovery_wait_us_total",
                self.metadata_command_recovery_wait_us_total,
            ),
            (
                "metadata_command_recovery_wait_us_max",
                self.metadata_command_recovery_wait_us_max,
            ),
            (
                "metadata_command_recovery_timeout_total",
                self.metadata_command_recovery_timeout_total,
            ),
            (
                "metadata_command_recovery_outcome_total",
                self.metadata_command_recovery_outcome_total,
            ),
            (
                "metadata_command_budget_exhausted_total",
                self.metadata_command_budget_exhausted_total,
            ),
            (
                "metadata_command_backoff_total",
                self.metadata_command_backoff_total,
            ),
            (
                "metadata_command_backoff_us_total",
                self.metadata_command_backoff_us_total,
            ),
            (
                "metadata_command_backoff_us_max",
                self.metadata_command_backoff_us_max,
            ),
            ("reclaim_work_queue_depth", self.reclaim_work_queue_depth),
            (
                "object_payload_reclaim_queue_depth",
                self.object_payload_reclaim_queue_depth,
            ),
            (
                "object_payload_reclaim_outstanding_depth",
                self.object_payload_reclaim_outstanding_depth,
            ),
            (
                "bucket_delete_begin_queue_depth",
                self.bucket_delete_begin_queue_depth,
            ),
            (
                "bucket_delete_finalize_queue_depth",
                self.bucket_delete_finalize_queue_depth,
            ),
            (
                "bucket_delete_finalize_outstanding_depth",
                self.bucket_delete_finalize_outstanding_depth,
            ),
            (
                "reclaim_work_queue_action_total",
                self.reclaim_work_queue_action_total,
            ),
            (
                "object_payload_reclaim_event_total",
                self.object_payload_reclaim_event_total,
            ),
            (
                "object_payload_reclaim_durable_scan_total",
                self.object_payload_reclaim_durable_scan_total,
            ),
            ("shard_repair_queue_depth", self.shard_repair_queue_depth),
            ("shard_repair_event_total", self.shard_repair_event_total),
            (
                "shard_repair_shards_rewritten_total",
                self.shard_repair_shards_rewritten_total,
            ),
            (
                "shard_backfill_queue_depth",
                self.shard_backfill_queue_depth,
            ),
            (
                "shard_backfill_event_total",
                self.shard_backfill_event_total,
            ),
            (
                "shard_backfill_shards_written_total",
                self.shard_backfill_shards_written_total,
            ),
            (
                "shard_backfill_candidate_scan_total",
                self.shard_backfill_candidate_scan_total,
            ),
            (
                "shard_backfill_candidate_scanned_total",
                self.shard_backfill_candidate_scanned_total,
            ),
            (
                "shard_backfill_candidate_current_epoch_total",
                self.shard_backfill_candidate_current_epoch_total,
            ),
            (
                "shard_backfill_candidate_already_queued_total",
                self.shard_backfill_candidate_already_queued_total,
            ),
            (
                "shard_backfill_candidate_already_complete_total",
                self.shard_backfill_candidate_already_complete_total,
            ),
            (
                "shard_backfill_candidate_enqueued_total",
                self.shard_backfill_candidate_enqueued_total,
            ),
            (
                "shard_backfill_candidate_unrecoverable_total",
                self.shard_backfill_candidate_unrecoverable_total,
            ),
            (
                "shard_backfill_candidate_deferred_total",
                self.shard_backfill_candidate_deferred_total,
            ),
            (
                "shard_backfill_candidate_failed_total",
                self.shard_backfill_candidate_failed_total,
            ),
            (
                "shard_backfill_candidate_limit_reached_total",
                self.shard_backfill_candidate_limit_reached_total,
            ),
            (
                "shard_backfill_candidate_scan_error_total",
                self.shard_backfill_candidate_scan_error_total,
            ),
            (
                "metadata_command_checkpoint_record_scan_total",
                self.metadata_command_checkpoint_record_scan_total,
            ),
            (
                "metadata_command_checkpoint_record_scanned_total",
                self.metadata_command_checkpoint_record_scanned_total,
            ),
            (
                "metadata_command_checkpoint_record_recorded_total",
                self.metadata_command_checkpoint_record_recorded_total,
            ),
            (
                "metadata_command_checkpoint_record_already_current_total",
                self.metadata_command_checkpoint_record_already_current_total,
            ),
            (
                "metadata_command_checkpoint_record_skipped_cadence_total",
                self.metadata_command_checkpoint_record_skipped_cadence_total,
            ),
            (
                "metadata_command_checkpoint_record_skipped_inactive_total",
                self.metadata_command_checkpoint_record_skipped_inactive_total,
            ),
            (
                "metadata_command_checkpoint_record_skipped_empty_total",
                self.metadata_command_checkpoint_record_skipped_empty_total,
            ),
            (
                "metadata_command_checkpoint_record_skipped_stale_epoch_total",
                self.metadata_command_checkpoint_record_skipped_stale_epoch_total,
            ),
            (
                "metadata_command_checkpoint_record_compacted_total",
                self.metadata_command_checkpoint_record_compacted_total,
            ),
            (
                "metadata_command_checkpoint_record_compaction_deleted_entries_total",
                self.metadata_command_checkpoint_record_compaction_deleted_entries_total,
            ),
            (
                "metadata_command_checkpoint_record_compaction_noop_total",
                self.metadata_command_checkpoint_record_compaction_noop_total,
            ),
            (
                "metadata_command_checkpoint_record_compaction_no_checkpoint_total",
                self.metadata_command_checkpoint_record_compaction_no_checkpoint_total,
            ),
            (
                "metadata_command_checkpoint_record_compaction_pending_total",
                self.metadata_command_checkpoint_record_compaction_pending_total,
            ),
            (
                "metadata_command_checkpoint_record_compaction_failed_total",
                self.metadata_command_checkpoint_record_compaction_failed_total,
            ),
            (
                "metadata_command_checkpoint_record_failed_total",
                self.metadata_command_checkpoint_record_failed_total,
            ),
            (
                "metadata_command_checkpoint_record_limit_reached_total",
                self.metadata_command_checkpoint_record_limit_reached_total,
            ),
            (
                "metadata_command_checkpoint_record_scan_error_total",
                self.metadata_command_checkpoint_record_scan_error_total,
            ),
            (
                "background_work_admission_event_total",
                self.background_work_admission_event_total,
            ),
            (
                "background_work_active_total",
                self.background_work_active_total,
            ),
            (
                "background_work_finished_total",
                self.background_work_finished_total,
            ),
            (
                "background_work_elapsed_us_total",
                self.background_work_elapsed_us_total,
            ),
            (
                "stream_upload_active_sessions",
                self.stream_upload_active_sessions,
            ),
            (
                "stream_upload_session_created_total",
                self.stream_upload_session_created_total,
            ),
            (
                "stream_upload_session_aborted_total",
                self.stream_upload_session_aborted_total,
            ),
            (
                "stream_upload_session_finalized_total",
                self.stream_upload_session_finalized_total,
            ),
            (
                "stream_upload_body_started_total",
                self.stream_upload_body_started_total,
            ),
            (
                "stream_upload_body_read_complete_total",
                self.stream_upload_body_read_complete_total,
            ),
            (
                "stream_upload_segment_append_started_total",
                self.stream_upload_segment_append_started_total,
            ),
            (
                "stream_upload_segment_append_finished_total",
                self.stream_upload_segment_append_finished_total,
            ),
            (
                "stream_upload_segment_append_error_total",
                self.stream_upload_segment_append_error_total,
            ),
            (
                "stream_upload_finalize_started_total",
                self.stream_upload_finalize_started_total,
            ),
            (
                "stream_upload_finalize_error_total",
                self.stream_upload_finalize_error_total,
            ),
        ]
        .into_iter()
    }
}

pub struct InflightRequestsGuard {
    active: bool,
}

pub struct StreamUploadActiveSessionGuard {
    active: bool,
}

pub struct StorageRpcActiveGuard {
    started_at: Instant,
    summary: StorageRpcActiveSummary<'static>,
    sequence: u64,
    active: bool,
}

impl Drop for InflightRequestsGuard {
    fn drop(&mut self) {
        if self.active {
            INFLIGHT_REQUESTS.fetch_sub(1, Ordering::Relaxed);
            self.active = false;
        }
    }
}

impl Drop for StorageRpcActiveGuard {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        STORAGE_RPC_PENDING_ENVELOPE_ACTIVE.fetch_sub(1, Ordering::Relaxed);
        STORAGE_RPC_PENDING_ENVELOPE_COMPLETED_TOTAL.fetch_add(1, Ordering::Relaxed);
        storage_rpc_pending_envelope_active_records()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|record| record.sequence != self.sequence);
        let elapsed = self.started_at.elapsed();
        if elapsed >= STORAGE_RPC_LONG_RUNNING_THRESHOLD {
            let elapsed_us = saturating_u128_to_u64(elapsed.as_micros());
            STORAGE_RPC_PENDING_ENVELOPE_LONG_RUNNING_TOTAL.fetch_add(1, Ordering::Relaxed);
            fetch_max_atomic(
                &STORAGE_RPC_PENDING_ENVELOPE_LONG_RUNNING_US_MAX,
                elapsed_us,
            );
            if let Some(context) = current_context() {
                record_flight_event(
                    &context,
                    "storage_rpc_client",
                    "storage_rpc_pending_envelope_long_running",
                    format!(
                        "node_id={} rpc_kind={} admission_class={} elapsed_us={}",
                        self.summary.node_id,
                        self.summary.rpc_kind,
                        self.summary.admission_class,
                        elapsed_us
                    ),
                );
            }
        }
        self.active = false;
    }
}

impl Drop for StreamUploadActiveSessionGuard {
    fn drop(&mut self) {
        if self.active {
            STREAM_UPLOAD_ACTIVE_SESSIONS.fetch_sub(1, Ordering::Relaxed);
            self.active = false;
        }
    }
}

#[must_use]
pub fn inflight_requests_guard() -> InflightRequestsGuard {
    INFLIGHT_REQUESTS.fetch_add(1, Ordering::Relaxed);
    InflightRequestsGuard { active: true }
}

#[must_use]
pub fn stream_upload_active_session_guard() -> StreamUploadActiveSessionGuard {
    STREAM_UPLOAD_ACTIVE_SESSIONS.fetch_add(1, Ordering::Relaxed);
    StreamUploadActiveSessionGuard { active: true }
}

pub fn storage_rpc_admission_class_acquired(admission_class: StorageRpcAdmissionClass) {
    STORAGE_RPC_ACTIVE_TOTAL.fetch_add(1, Ordering::Relaxed);
    storage_rpc_admission_class_counter(admission_class).fetch_add(1, Ordering::Relaxed);
}

pub fn storage_rpc_admission_class_released(admission_class: StorageRpcAdmissionClass) {
    STORAGE_RPC_ACTIVE_TOTAL.fetch_sub(1, Ordering::Relaxed);
    storage_rpc_admission_class_counter(admission_class).fetch_sub(1, Ordering::Relaxed);
}

#[must_use]
pub fn storage_rpc_pending_envelope_guard(
    summary: StorageRpcActiveSummary<'static>,
) -> StorageRpcActiveGuard {
    let started_at = Instant::now();
    let sequence = STORAGE_RPC_PENDING_ENVELOPE_ACTIVE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    STORAGE_RPC_PENDING_ENVELOPE_ACTIVE.fetch_add(1, Ordering::Relaxed);
    STORAGE_RPC_PENDING_ENVELOPE_STARTED_TOTAL.fetch_add(1, Ordering::Relaxed);
    storage_rpc_pending_envelope_active_records()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push(StorageRpcPendingEnvelopeActiveRecord {
            sequence,
            node_id: summary.node_id,
            rpc_request_id: summary.rpc_request_id,
            started_at,
        });
    StorageRpcActiveGuard {
        started_at,
        summary,
        sequence,
        active: true,
    }
}

fn storage_rpc_pending_envelope_active_records(
) -> &'static Mutex<Vec<StorageRpcPendingEnvelopeActiveRecord>> {
    static ACTIVE: OnceLock<Mutex<Vec<StorageRpcPendingEnvelopeActiveRecord>>> = OnceLock::new();
    ACTIVE.get_or_init(|| Mutex::new(Vec::new()))
}

fn storage_rpc_admission_class_counter(
    admission_class: StorageRpcAdmissionClass,
) -> &'static AtomicU64 {
    match admission_class {
        StorageRpcAdmissionClass::Control => &STORAGE_RPC_ACTIVE_CONTROL,
        StorageRpcAdmissionClass::Completion => &STORAGE_RPC_ACTIVE_COMPLETION,
        StorageRpcAdmissionClass::Progress => &STORAGE_RPC_ACTIVE_PROGRESS,
        StorageRpcAdmissionClass::StartWrite => &STORAGE_RPC_ACTIVE_START_WRITE,
        StorageRpcAdmissionClass::Read => &STORAGE_RPC_ACTIVE_READ,
        StorageRpcAdmissionClass::List => &STORAGE_RPC_ACTIVE_LIST,
    }
}

#[must_use]
pub fn metrics_snapshot() -> MetricsSnapshot {
    let pending_envelope_oldest_active = {
        let records = storage_rpc_pending_envelope_active_records()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        records
            .iter()
            .max_by_key(|record| record.started_at.elapsed())
            .map(|record| {
                (
                    saturating_u128_to_u64(record.started_at.elapsed().as_micros()),
                    u64::from(record.node_id),
                    record.rpc_request_id,
                )
            })
            .unwrap_or((0, 0, 0))
    };
    MetricsSnapshot {
        request_start_total: REQUEST_START_TOTAL.load(Ordering::Relaxed),
        inflight_requests: INFLIGHT_REQUESTS.load(Ordering::Relaxed),
        request_finish_total: REQUEST_FINISH_TOTAL.load(Ordering::Relaxed),
        request_error_total: REQUEST_ERROR_TOTAL.load(Ordering::Relaxed),
        http_500_response_total: HTTP_500_RESPONSE_TOTAL.load(Ordering::Relaxed),
        operation_aborted_response_total: OPERATION_ABORTED_RESPONSE_TOTAL.load(Ordering::Relaxed),
        slow_down_response_total: SLOW_DOWN_RESPONSE_TOTAL.load(Ordering::Relaxed),
        slow_request_total: SLOW_REQUEST_TOTAL.load(Ordering::Relaxed),
        request_admission_wait_total: REQUEST_ADMISSION_WAIT_TOTAL.load(Ordering::Relaxed),
        request_admission_wait_us_total: REQUEST_ADMISSION_WAIT_US_TOTAL.load(Ordering::Relaxed),
        request_admission_timeout_total: REQUEST_ADMISSION_TIMEOUT_TOTAL.load(Ordering::Relaxed),
        storage_rpc_error_total: STORAGE_RPC_ERROR_TOTAL.load(Ordering::Relaxed),
        storage_rpc_admission_total: STORAGE_RPC_ADMISSION_TOTAL.load(Ordering::Relaxed),
        storage_rpc_admission_wait_total: STORAGE_RPC_ADMISSION_WAIT_TOTAL.load(Ordering::Relaxed),
        storage_rpc_admission_wait_us_total: STORAGE_RPC_ADMISSION_WAIT_US_TOTAL
            .load(Ordering::Relaxed),
        storage_rpc_admission_timeout_total: STORAGE_RPC_ADMISSION_TIMEOUT_TOTAL
            .load(Ordering::Relaxed),
        storage_rpc_active_total: STORAGE_RPC_ACTIVE_TOTAL.load(Ordering::Relaxed),
        storage_rpc_active_control: STORAGE_RPC_ACTIVE_CONTROL.load(Ordering::Relaxed),
        storage_rpc_active_completion: STORAGE_RPC_ACTIVE_COMPLETION.load(Ordering::Relaxed),
        storage_rpc_active_progress: STORAGE_RPC_ACTIVE_PROGRESS.load(Ordering::Relaxed),
        storage_rpc_active_start_write: STORAGE_RPC_ACTIVE_START_WRITE.load(Ordering::Relaxed),
        storage_rpc_active_read: STORAGE_RPC_ACTIVE_READ.load(Ordering::Relaxed),
        storage_rpc_active_list: STORAGE_RPC_ACTIVE_LIST.load(Ordering::Relaxed),
        storage_rpc_pending_envelope_active: STORAGE_RPC_PENDING_ENVELOPE_ACTIVE
            .load(Ordering::Relaxed),
        storage_rpc_pending_envelope_started_total: STORAGE_RPC_PENDING_ENVELOPE_STARTED_TOTAL
            .load(Ordering::Relaxed),
        storage_rpc_pending_envelope_completed_total: STORAGE_RPC_PENDING_ENVELOPE_COMPLETED_TOTAL
            .load(Ordering::Relaxed),
        storage_rpc_pending_envelope_long_running_total:
            STORAGE_RPC_PENDING_ENVELOPE_LONG_RUNNING_TOTAL.load(Ordering::Relaxed),
        storage_rpc_pending_envelope_long_running_us_max:
            STORAGE_RPC_PENDING_ENVELOPE_LONG_RUNNING_US_MAX.load(Ordering::Relaxed),
        storage_rpc_pending_envelope_oldest_active_us: pending_envelope_oldest_active.0,
        storage_rpc_pending_envelope_oldest_active_node_id: pending_envelope_oldest_active.1,
        storage_rpc_pending_envelope_oldest_active_request_id: pending_envelope_oldest_active.2,
        bucket_lock_wait_exceeded_total: BUCKET_LOCK_WAIT_EXCEEDED_TOTAL.load(Ordering::Relaxed),
        shard_scavenger_observation_total: SHARD_SCAVENGER_OBSERVATION_TOTAL
            .load(Ordering::Relaxed),
        shard_scavenger_scan_incomplete_total: SHARD_SCAVENGER_SCAN_INCOMPLETE_TOTAL
            .load(Ordering::Relaxed),
        metadata_command_conflict_total: METADATA_COMMAND_CONFLICT_TOTAL.load(Ordering::Relaxed),
        metadata_command_pending_slot_action_total: METADATA_COMMAND_PENDING_SLOT_ACTION_TOTAL
            .load(Ordering::Relaxed),
        metadata_command_session_wait_total: METADATA_COMMAND_SESSION_WAIT_TOTAL
            .load(Ordering::Relaxed),
        metadata_command_recovery_leader_total: METADATA_COMMAND_RECOVERY_LEADER_TOTAL
            .load(Ordering::Relaxed),
        metadata_command_recovery_wait_total: METADATA_COMMAND_RECOVERY_WAIT_TOTAL
            .load(Ordering::Relaxed),
        metadata_command_recovery_wait_us_total: METADATA_COMMAND_RECOVERY_WAIT_US_TOTAL
            .load(Ordering::Relaxed),
        metadata_command_recovery_wait_us_max: METADATA_COMMAND_RECOVERY_WAIT_US_MAX
            .load(Ordering::Relaxed),
        metadata_command_recovery_timeout_total: METADATA_COMMAND_RECOVERY_TIMEOUT_TOTAL
            .load(Ordering::Relaxed),
        metadata_command_recovery_outcome_total: METADATA_COMMAND_RECOVERY_OUTCOME_TOTAL
            .load(Ordering::Relaxed),
        metadata_command_budget_exhausted_total: METADATA_COMMAND_BUDGET_EXHAUSTED_TOTAL
            .load(Ordering::Relaxed),
        metadata_command_backoff_total: METADATA_COMMAND_BACKOFF_TOTAL.load(Ordering::Relaxed),
        metadata_command_backoff_us_total: METADATA_COMMAND_BACKOFF_US_TOTAL
            .load(Ordering::Relaxed),
        metadata_command_backoff_us_max: METADATA_COMMAND_BACKOFF_US_MAX.load(Ordering::Relaxed),
        reclaim_work_queue_depth: RECLAIM_WORK_QUEUE_DEPTH.load(Ordering::Relaxed),
        object_payload_reclaim_queue_depth: OBJECT_PAYLOAD_RECLAIM_QUEUE_DEPTH
            .load(Ordering::Relaxed),
        object_payload_reclaim_outstanding_depth: OBJECT_PAYLOAD_RECLAIM_OUTSTANDING_DEPTH
            .load(Ordering::Relaxed),
        bucket_delete_begin_queue_depth: BUCKET_DELETE_BEGIN_QUEUE_DEPTH.load(Ordering::Relaxed),
        bucket_delete_finalize_queue_depth: BUCKET_DELETE_FINALIZE_QUEUE_DEPTH
            .load(Ordering::Relaxed),
        bucket_delete_finalize_outstanding_depth: BUCKET_DELETE_FINALIZE_OUTSTANDING_DEPTH
            .load(Ordering::Relaxed),
        reclaim_work_queue_action_total: RECLAIM_WORK_QUEUE_ACTION_TOTAL.load(Ordering::Relaxed),
        object_payload_reclaim_event_total: OBJECT_PAYLOAD_RECLAIM_EVENT_TOTAL
            .load(Ordering::Relaxed),
        object_payload_reclaim_durable_scan_total: OBJECT_PAYLOAD_RECLAIM_DURABLE_SCAN_TOTAL
            .load(Ordering::Relaxed),
        shard_repair_queue_depth: SHARD_REPAIR_QUEUE_DEPTH.load(Ordering::Relaxed),
        shard_repair_event_total: SHARD_REPAIR_EVENT_TOTAL.load(Ordering::Relaxed),
        shard_repair_shards_rewritten_total: SHARD_REPAIR_SHARDS_REWRITTEN_TOTAL
            .load(Ordering::Relaxed),
        shard_backfill_queue_depth: SHARD_BACKFILL_QUEUE_DEPTH.load(Ordering::Relaxed),
        shard_backfill_event_total: SHARD_BACKFILL_EVENT_TOTAL.load(Ordering::Relaxed),
        shard_backfill_shards_written_total: SHARD_BACKFILL_SHARDS_WRITTEN_TOTAL
            .load(Ordering::Relaxed),
        shard_backfill_candidate_scan_total: SHARD_BACKFILL_CANDIDATE_SCAN_TOTAL
            .load(Ordering::Relaxed),
        shard_backfill_candidate_scanned_total: SHARD_BACKFILL_CANDIDATE_SCANNED_TOTAL
            .load(Ordering::Relaxed),
        shard_backfill_candidate_current_epoch_total: SHARD_BACKFILL_CANDIDATE_CURRENT_EPOCH_TOTAL
            .load(Ordering::Relaxed),
        shard_backfill_candidate_already_queued_total:
            SHARD_BACKFILL_CANDIDATE_ALREADY_QUEUED_TOTAL.load(Ordering::Relaxed),
        shard_backfill_candidate_already_complete_total:
            SHARD_BACKFILL_CANDIDATE_ALREADY_COMPLETE_TOTAL.load(Ordering::Relaxed),
        shard_backfill_candidate_enqueued_total: SHARD_BACKFILL_CANDIDATE_ENQUEUED_TOTAL
            .load(Ordering::Relaxed),
        shard_backfill_candidate_unrecoverable_total: SHARD_BACKFILL_CANDIDATE_UNRECOVERABLE_TOTAL
            .load(Ordering::Relaxed),
        shard_backfill_candidate_deferred_total: SHARD_BACKFILL_CANDIDATE_DEFERRED_TOTAL
            .load(Ordering::Relaxed),
        shard_backfill_candidate_failed_total: SHARD_BACKFILL_CANDIDATE_FAILED_TOTAL
            .load(Ordering::Relaxed),
        shard_backfill_candidate_limit_reached_total: SHARD_BACKFILL_CANDIDATE_LIMIT_REACHED_TOTAL
            .load(Ordering::Relaxed),
        shard_backfill_candidate_scan_error_total: SHARD_BACKFILL_CANDIDATE_SCAN_ERROR_TOTAL
            .load(Ordering::Relaxed),
        metadata_command_checkpoint_record_scan_total:
            METADATA_COMMAND_CHECKPOINT_RECORD_SCAN_TOTAL.load(Ordering::Relaxed),
        metadata_command_checkpoint_record_scanned_total:
            METADATA_COMMAND_CHECKPOINT_RECORD_SCANNED_TOTAL.load(Ordering::Relaxed),
        metadata_command_checkpoint_record_recorded_total:
            METADATA_COMMAND_CHECKPOINT_RECORD_RECORDED_TOTAL.load(Ordering::Relaxed),
        metadata_command_checkpoint_record_already_current_total:
            METADATA_COMMAND_CHECKPOINT_RECORD_ALREADY_CURRENT_TOTAL.load(Ordering::Relaxed),
        metadata_command_checkpoint_record_skipped_cadence_total:
            METADATA_COMMAND_CHECKPOINT_RECORD_SKIPPED_CADENCE_TOTAL.load(Ordering::Relaxed),
        metadata_command_checkpoint_record_skipped_inactive_total:
            METADATA_COMMAND_CHECKPOINT_RECORD_SKIPPED_INACTIVE_TOTAL.load(Ordering::Relaxed),
        metadata_command_checkpoint_record_skipped_empty_total:
            METADATA_COMMAND_CHECKPOINT_RECORD_SKIPPED_EMPTY_TOTAL.load(Ordering::Relaxed),
        metadata_command_checkpoint_record_skipped_stale_epoch_total:
            METADATA_COMMAND_CHECKPOINT_RECORD_SKIPPED_STALE_EPOCH_TOTAL.load(Ordering::Relaxed),
        metadata_command_checkpoint_record_compacted_total:
            METADATA_COMMAND_CHECKPOINT_RECORD_COMPACTED_TOTAL.load(Ordering::Relaxed),
        metadata_command_checkpoint_record_compaction_deleted_entries_total:
            METADATA_COMMAND_CHECKPOINT_RECORD_COMPACTION_DELETED_ENTRIES_TOTAL
                .load(Ordering::Relaxed),
        metadata_command_checkpoint_record_compaction_noop_total:
            METADATA_COMMAND_CHECKPOINT_RECORD_COMPACTION_NOOP_TOTAL.load(Ordering::Relaxed),
        metadata_command_checkpoint_record_compaction_no_checkpoint_total:
            METADATA_COMMAND_CHECKPOINT_RECORD_COMPACTION_NO_CHECKPOINT_TOTAL
                .load(Ordering::Relaxed),
        metadata_command_checkpoint_record_compaction_pending_total:
            METADATA_COMMAND_CHECKPOINT_RECORD_COMPACTION_PENDING_TOTAL.load(Ordering::Relaxed),
        metadata_command_checkpoint_record_compaction_failed_total:
            METADATA_COMMAND_CHECKPOINT_RECORD_COMPACTION_FAILED_TOTAL.load(Ordering::Relaxed),
        metadata_command_checkpoint_record_failed_total:
            METADATA_COMMAND_CHECKPOINT_RECORD_FAILED_TOTAL.load(Ordering::Relaxed),
        metadata_command_checkpoint_record_limit_reached_total:
            METADATA_COMMAND_CHECKPOINT_RECORD_LIMIT_REACHED_TOTAL.load(Ordering::Relaxed),
        metadata_command_checkpoint_record_scan_error_total:
            METADATA_COMMAND_CHECKPOINT_RECORD_SCAN_ERROR_TOTAL.load(Ordering::Relaxed),
        background_work_admission_event_total: BACKGROUND_WORK_ADMISSION_EVENT_TOTAL
            .load(Ordering::Relaxed),
        background_work_active_total: BACKGROUND_WORK_ACTIVE_TOTAL.load(Ordering::Relaxed),
        background_work_finished_total: BACKGROUND_WORK_FINISHED_TOTAL.load(Ordering::Relaxed),
        background_work_elapsed_us_total: BACKGROUND_WORK_ELAPSED_US_TOTAL.load(Ordering::Relaxed),
        stream_upload_active_sessions: STREAM_UPLOAD_ACTIVE_SESSIONS.load(Ordering::Relaxed),
        stream_upload_session_created_total: STREAM_UPLOAD_SESSION_CREATED_TOTAL
            .load(Ordering::Relaxed),
        stream_upload_session_aborted_total: STREAM_UPLOAD_SESSION_ABORTED_TOTAL
            .load(Ordering::Relaxed),
        stream_upload_session_finalized_total: STREAM_UPLOAD_SESSION_FINALIZED_TOTAL
            .load(Ordering::Relaxed),
        stream_upload_body_started_total: STREAM_UPLOAD_BODY_STARTED_TOTAL.load(Ordering::Relaxed),
        stream_upload_body_read_complete_total: STREAM_UPLOAD_BODY_READ_COMPLETE_TOTAL
            .load(Ordering::Relaxed),
        stream_upload_segment_append_started_total: STREAM_UPLOAD_SEGMENT_APPEND_STARTED_TOTAL
            .load(Ordering::Relaxed),
        stream_upload_segment_append_finished_total: STREAM_UPLOAD_SEGMENT_APPEND_FINISHED_TOTAL
            .load(Ordering::Relaxed),
        stream_upload_segment_append_error_total: STREAM_UPLOAD_SEGMENT_APPEND_ERROR_TOTAL
            .load(Ordering::Relaxed),
        stream_upload_finalize_started_total: STREAM_UPLOAD_FINALIZE_STARTED_TOTAL
            .load(Ordering::Relaxed),
        stream_upload_finalize_error_total: STREAM_UPLOAD_FINALIZE_ERROR_TOTAL
            .load(Ordering::Relaxed),
    }
}

pub fn emit_request_start(
    context: &TraceContext,
    target: &'static str,
    summary: RequestStartSummary<'_>,
) -> bool {
    REQUEST_START_TOTAL.fetch_add(1, Ordering::Relaxed);
    record_flight_event(
        context,
        target,
        "request_start",
        truncate_detail(format!(
            "method={} path_hash={} has_query={} query_params={} sigv4_query={}",
            summary.method,
            stable_hash_hex(summary.path),
            summary.query.has_query(),
            summary.query.param_count(),
            summary.query.has_sigv4_params()
        )),
    );
    event_in_context(
        context,
        target,
        "request_start",
        Some(format_args!(
            "method={} path={:?} has_query={} query_params={} sigv4_query={}",
            summary.method,
            summary.path,
            summary.query.has_query(),
            summary.query.param_count(),
            summary.query.has_sigv4_params()
        )),
    )
}

pub fn emit_request_finish(
    context: &TraceContext,
    target: &'static str,
    summary: RequestSummary<'_>,
    outcome: &'static str,
) -> bool {
    REQUEST_FINISH_TOTAL.fetch_add(1, Ordering::Relaxed);
    record_flight_event(
        context,
        target,
        "request_finish",
        request_flight_detail(summary, format_args!("outcome={outcome}")),
    );
    event_in_context(
        context,
        target,
        "request_finish",
        Some(format_args!(
            "status={} method={} path={:?} has_query={} query_params={} sigv4_query={} streaming={} body_len={} bytes_sent={} lifetime_us={} outcome={}",
            summary.status_code,
            summary.method,
            summary.path,
            summary.query.has_query(),
            summary.query.param_count(),
            summary.query.has_sigv4_params(),
            summary.streaming,
            summary.body_len,
            summary.bytes_sent,
            summary.lifetime_us,
            outcome
        )),
    )
}

pub fn emit_request_error(
    context: &TraceContext,
    target: &'static str,
    summary: RequestSummary<'_>,
    stage: &'static str,
    error_code: &str,
    cause_label: &'static str,
) -> bool {
    REQUEST_ERROR_TOTAL.fetch_add(1, Ordering::Relaxed);
    if summary.status_code == 500 {
        HTTP_500_RESPONSE_TOTAL.fetch_add(1, Ordering::Relaxed);
    }
    match (summary.status_code, error_code) {
        (409, "OperationAborted") => {
            OPERATION_ABORTED_RESPONSE_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        (503, "SlowDown") => {
            SLOW_DOWN_RESPONSE_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        _ => {}
    }
    record_flight_event(
        context,
        target,
        "request_error",
        request_flight_detail(
            summary,
            format_args!(
                "stage={} error_code={} cause_label={}",
                stage, error_code, cause_label
            ),
        ),
    );
    event_in_context(
        context,
        target,
        "request_error",
        Some(format_args!(
            "status={} method={} path={:?} has_query={} query_params={} sigv4_query={} streaming={} body_len={} bytes_sent={} lifetime_us={} stage={} error_code={} cause_label={}",
            summary.status_code,
            summary.method,
            summary.path,
            summary.query.has_query(),
            summary.query.param_count(),
            summary.query.has_sigv4_params(),
            summary.streaming,
            summary.body_len,
            summary.bytes_sent,
            summary.lifetime_us,
            stage,
            error_code,
            cause_label
        )),
    )
}

pub fn emit_http_500_cause_chain(
    context: &TraceContext,
    target: &'static str,
    summary: RequestSummary<'_>,
    cause_label: &'static str,
    cause_chain: &str,
) -> bool {
    record_flight_event(
        context,
        target,
        "request_500_cause_chain",
        request_flight_detail(
            summary,
            format_args!(
                "cause_label={} cause_chain={}",
                cause_label,
                escaped(cause_chain)
            ),
        ),
    );
    event_in_context(
        context,
        target,
        "request_500_cause_chain",
        Some(format_args!(
            "status={} method={} path={:?} has_query={} query_params={} sigv4_query={} streaming={} body_len={} bytes_sent={} lifetime_us={} cause_label={} cause_chain={}",
            summary.status_code,
            summary.method,
            summary.path,
            summary.query.has_query(),
            summary.query.param_count(),
            summary.query.has_sigv4_params(),
            summary.streaming,
            summary.body_len,
            summary.bytes_sent,
            summary.lifetime_us,
            cause_label,
            escaped(cause_chain)
        )),
    )
}

pub fn emit_http_500_server_detail(
    context: &TraceContext,
    target: &'static str,
    summary: RequestSummary<'_>,
    server_detail: &str,
) -> bool {
    record_flight_event(
        context,
        target,
        "request_500_server_detail",
        request_flight_detail(
            summary,
            format_args!("server_detail={}", escaped(server_detail)),
        ),
    );
    event_in_context(
        context,
        target,
        "request_500_server_detail",
        Some(format_args!(
            "status={} method={} path={:?} has_query={} query_params={} sigv4_query={} streaming={} body_len={} bytes_sent={} lifetime_us={} server_detail={}",
            summary.status_code,
            summary.method,
            summary.path,
            summary.query.has_query(),
            summary.query.param_count(),
            summary.query.has_sigv4_params(),
            summary.streaming,
            summary.body_len,
            summary.bytes_sent,
            summary.lifetime_us,
            escaped(server_detail)
        )),
    )
}

pub fn emit_slow_request(
    context: &TraceContext,
    target: &'static str,
    summary: RequestSummary<'_>,
    outcome: &'static str,
    error_code: Option<&str>,
) -> bool {
    SLOW_REQUEST_TOTAL.fetch_add(1, Ordering::Relaxed);
    record_flight_event(
        context,
        target,
        "slow_request",
        request_flight_detail(
            summary,
            format_args!(
                "outcome={}{}",
                outcome,
                error_code
                    .map(|code| format!(" error_code={code}"))
                    .unwrap_or_default()
            ),
        ),
    );
    let error_suffix = error_code
        .map(|code| format!(" error_code={code}"))
        .unwrap_or_default();
    event_in_context(
        context,
        target,
        "slow_request",
        Some(format_args!(
            "status={} method={} path={:?} has_query={} query_params={} sigv4_query={} streaming={} body_len={} bytes_sent={} lifetime_us={} outcome={}{}",
            summary.status_code,
            summary.method,
            summary.path,
            summary.query.has_query(),
            summary.query.param_count(),
            summary.query.has_sigv4_params(),
            summary.streaming,
            summary.body_len,
            summary.bytes_sent,
            summary.lifetime_us,
            outcome,
            error_suffix
        )),
    )
}

fn saturating_u128_to_u64(value: u128) -> u64 {
    value.try_into().unwrap_or(u64::MAX)
}

pub fn emit_request_admission_wait(
    context: &TraceContext,
    target: &'static str,
    summary: RequestAdmissionWaitSummary<'_>,
) -> bool {
    REQUEST_ADMISSION_WAIT_TOTAL.fetch_add(1, Ordering::Relaxed);
    REQUEST_ADMISSION_WAIT_US_TOTAL
        .fetch_add(saturating_u128_to_u64(summary.wait_us), Ordering::Relaxed);
    record_flight_event(
        context,
        target,
        "request_admission_wait",
        truncate_detail(format!(
            "method={} path_hash={} has_query={} query_params={} sigv4_query={} wait_us={}",
            summary.method,
            stable_hash_hex(summary.path),
            summary.query.has_query(),
            summary.query.param_count(),
            summary.query.has_sigv4_params(),
            summary.wait_us
        )),
    );
    event_in_context(
        context,
        target,
        "request_admission_wait",
        Some(format_args!(
            "method={} path_hash={} has_query={} query_params={} sigv4_query={} wait_us={}",
            summary.method,
            stable_hash_hex(summary.path),
            summary.query.has_query(),
            summary.query.param_count(),
            summary.query.has_sigv4_params(),
            summary.wait_us
        )),
    )
}

pub fn emit_request_admission_timeout(
    context: &TraceContext,
    target: &'static str,
    summary: RequestAdmissionTimeoutSummary<'_>,
) -> bool {
    REQUEST_ADMISSION_TIMEOUT_TOTAL.fetch_add(1, Ordering::Relaxed);
    record_flight_event(
        context,
        target,
        "request_admission_timeout",
        truncate_detail(format!(
            "method={} path_hash={} has_query={} query_params={} sigv4_query={} wait_us={} timeout_us={}",
            summary.method,
            stable_hash_hex(summary.path),
            summary.query.has_query(),
            summary.query.param_count(),
            summary.query.has_sigv4_params(),
            summary.wait_us,
            summary.timeout_us
        )),
    );
    event_in_context(
        context,
        target,
        "request_admission_timeout",
        Some(format_args!(
            "method={} path_hash={} has_query={} query_params={} sigv4_query={} wait_us={} timeout_us={}",
            summary.method,
            stable_hash_hex(summary.path),
            summary.query.has_query(),
            summary.query.param_count(),
            summary.query.has_sigv4_params(),
            summary.wait_us,
            summary.timeout_us
        )),
    )
}

fn update_stream_upload_phase_metrics(phase: StreamUploadPhase) {
    match phase {
        StreamUploadPhase::SessionCreated => {
            STREAM_UPLOAD_SESSION_CREATED_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        StreamUploadPhase::SessionAborted => {
            STREAM_UPLOAD_SESSION_ABORTED_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        StreamUploadPhase::SessionFinalized => {
            STREAM_UPLOAD_SESSION_FINALIZED_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        StreamUploadPhase::BodyStarted => {
            STREAM_UPLOAD_BODY_STARTED_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        StreamUploadPhase::BodyReadComplete => {
            STREAM_UPLOAD_BODY_READ_COMPLETE_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        StreamUploadPhase::SegmentAppendStarted => {
            STREAM_UPLOAD_SEGMENT_APPEND_STARTED_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        StreamUploadPhase::SegmentAppendFinished => {
            STREAM_UPLOAD_SEGMENT_APPEND_FINISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        StreamUploadPhase::SegmentAppendError => {
            STREAM_UPLOAD_SEGMENT_APPEND_ERROR_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        StreamUploadPhase::FinalizeStarted => {
            STREAM_UPLOAD_FINALIZE_STARTED_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        StreamUploadPhase::FinalizeError => {
            STREAM_UPLOAD_FINALIZE_ERROR_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub fn emit_stream_upload_phase(
    context: &TraceContext,
    target: &'static str,
    summary: StreamUploadPhaseSummary<'_>,
) -> bool {
    update_stream_upload_phase_metrics(summary.phase);
    let phase = summary.phase.as_str();
    let upload_id_hash = summary
        .upload_id
        .map(stable_hash_hex)
        .unwrap_or_else(|| "none".to_string());
    let session_id_hash = summary
        .session_id
        .map(stable_hash_hex)
        .unwrap_or_else(|| "none".to_string());
    let part_number = summary
        .part_number
        .map(|part_number| part_number.to_string())
        .unwrap_or_else(|| "none".to_string());
    let segment_index = summary
        .segment_index
        .map(|segment_index| segment_index.to_string())
        .unwrap_or_else(|| "none".to_string());
    let body_bytes_received = summary
        .body_bytes_received
        .map(|bytes| bytes.to_string())
        .unwrap_or_else(|| "none".to_string());
    let segment_bytes = summary
        .segment_bytes
        .map(|bytes| bytes.to_string())
        .unwrap_or_else(|| "none".to_string());
    let segment_count = summary
        .segment_count
        .map(|count| count.to_string())
        .unwrap_or_else(|| "none".to_string());
    let detail = truncate_detail(format!(
        "operation={} phase={} bucket_hash={} key_hash={} upload_id_hash={} part_number={} session_id_hash={} segment_index={} body_bytes_received={} segment_bytes={} segment_count={}",
        summary.operation,
        phase,
        stable_hash_hex(summary.bucket),
        stable_hash_hex(summary.key),
        upload_id_hash,
        part_number,
        session_id_hash,
        segment_index,
        body_bytes_received,
        segment_bytes,
        segment_count
    ));
    record_flight_event(context, target, "stream_upload_phase", detail);
    event_in_context(
        context,
        target,
        "stream_upload_phase",
        Some(format_args!(
            "operation={} phase={} bucket_hash={} key_hash={} upload_id_hash={} part_number={} session_id_hash={} segment_index={} body_bytes_received={} segment_bytes={} segment_count={}",
            summary.operation,
            phase,
            stable_hash_hex(summary.bucket),
            stable_hash_hex(summary.key),
            upload_id_hash,
            part_number,
            session_id_hash,
            segment_index,
            body_bytes_received,
            segment_bytes,
            segment_count
        )),
    )
}

pub fn emit_bucket_lock_wait_exceeded<T: fmt::Debug>(
    context: &TraceContext,
    target: &'static str,
    bucket: &T,
    stripe: usize,
    wait_us: u128,
) -> bool {
    BUCKET_LOCK_WAIT_EXCEEDED_TOTAL.fetch_add(1, Ordering::Relaxed);
    event_in_context(
        context,
        target,
        "bucket_lock_wait_exceeded",
        Some(format_args!(
            "bucket={:?} stripe={} wait_us={}",
            bucket, stripe, wait_us
        )),
    )
}

pub fn emit_metadata_command_conflict(
    target: &'static str,
    summary: MetadataCommandConflictSummary,
) -> bool {
    METADATA_COMMAND_CONFLICT_TOTAL.fetch_add(1, Ordering::Relaxed);
    increment_metadata_command_dimension(
        metadata_command_conflict_dimensions(),
        summary.pg_id,
        summary.kind,
        summary.command_kind.unwrap_or("unknown"),
    );
    let Some(context) = current_context() else {
        return false;
    };
    let node_id = summary
        .node_id
        .map(|node_id| node_id.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let log_index = summary
        .log_index
        .map(|log_index| log_index.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let command_kind = summary.command_kind.unwrap_or("unknown");
    let detail = format!(
        "node_id={} pg_id={} cluster_epoch={} log_index={} kind={} command_kind={}",
        node_id, summary.pg_id, summary.cluster_epoch, log_index, summary.kind, command_kind
    );
    record_flight_event(&context, target, "metadata_command_conflict", detail);
    event_in_context(
        &context,
        target,
        "metadata_command_conflict",
        Some(format_args!(
            "node_id={} pg_id={} cluster_epoch={} log_index={} kind={} command_kind={}",
            node_id, summary.pg_id, summary.cluster_epoch, log_index, summary.kind, command_kind
        )),
    )
}

pub fn emit_metadata_command_pending_slot_action(
    target: &'static str,
    summary: MetadataCommandPendingSlotActionSummary,
) -> bool {
    METADATA_COMMAND_PENDING_SLOT_ACTION_TOTAL.fetch_add(1, Ordering::Relaxed);
    increment_metadata_command_dimension(
        metadata_command_pending_slot_action_dimensions(),
        summary.pg_id,
        summary.action,
        summary.command_kind.unwrap_or("unknown"),
    );
    let Some(context) = current_context() else {
        return false;
    };
    let node_id = summary
        .node_id
        .map(|node_id| node_id.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let log_index = summary
        .log_index
        .map(|log_index| log_index.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let command_kind = summary.command_kind.unwrap_or("unknown");
    let detail = format!(
        "node_id={} pg_id={} cluster_epoch={} log_index={} action={} command_kind={}",
        node_id, summary.pg_id, summary.cluster_epoch, log_index, summary.action, command_kind
    );
    record_flight_event(
        &context,
        target,
        "metadata_command_pending_slot_action",
        detail,
    );
    event_in_context(
        &context,
        target,
        "metadata_command_pending_slot_action",
        Some(format_args!(
            "node_id={} pg_id={} cluster_epoch={} log_index={} action={} command_kind={}",
            node_id, summary.pg_id, summary.cluster_epoch, log_index, summary.action, command_kind
        )),
    )
}

pub fn emit_metadata_command_session_wait(
    target: &'static str,
    summary: MetadataCommandSessionWaitSummary,
) -> bool {
    METADATA_COMMAND_SESSION_WAIT_TOTAL.fetch_add(1, Ordering::Relaxed);
    let Some(context) = current_context() else {
        return false;
    };
    record_flight_event(
        &context,
        target,
        "metadata_command_session_wait",
        format!(
            "node_id={} pg_id={} wait_us={}",
            summary.node_id, summary.pg_id, summary.wait_us
        ),
    );
    event_in_context(
        &context,
        target,
        "metadata_command_session_wait",
        Some(format_args!(
            "node_id={} pg_id={} wait_us={}",
            summary.node_id, summary.pg_id, summary.wait_us
        )),
    )
}

pub fn emit_metadata_command_recovery_admission(
    target: &'static str,
    summary: MetadataCommandRecoveryAdmissionSummary,
) -> bool {
    match summary.admission {
        "leader" => {
            METADATA_COMMAND_RECOVERY_LEADER_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        "waited" => {
            METADATA_COMMAND_RECOVERY_WAIT_TOTAL.fetch_add(1, Ordering::Relaxed);
            let wait_us = saturating_u128_to_u64(summary.wait_us);
            METADATA_COMMAND_RECOVERY_WAIT_US_TOTAL.fetch_add(wait_us, Ordering::Relaxed);
            fetch_max_atomic(&METADATA_COMMAND_RECOVERY_WAIT_US_MAX, wait_us);
        }
        "timed_out" => {
            METADATA_COMMAND_RECOVERY_WAIT_TOTAL.fetch_add(1, Ordering::Relaxed);
            METADATA_COMMAND_RECOVERY_TIMEOUT_TOTAL.fetch_add(1, Ordering::Relaxed);
            let wait_us = saturating_u128_to_u64(summary.wait_us);
            METADATA_COMMAND_RECOVERY_WAIT_US_TOTAL.fetch_add(wait_us, Ordering::Relaxed);
            fetch_max_atomic(&METADATA_COMMAND_RECOVERY_WAIT_US_MAX, wait_us);
        }
        _ => {}
    }
    increment_metadata_command_dimension(
        metadata_command_recovery_admission_dimensions(),
        summary.pg_id,
        summary.admission,
        summary.command_kind.unwrap_or("unknown"),
    );
    let Some(context) = current_context() else {
        return false;
    };
    let node_id = summary
        .node_id
        .map(|node_id| node_id.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let log_index = summary
        .log_index
        .map(|log_index| log_index.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let command_kind = summary.command_kind.unwrap_or("unknown");
    let detail = format!(
        "node_id={} pg_id={} cluster_epoch={} log_index={} admission={} command_kind={} wait_us={}",
        node_id,
        summary.pg_id,
        summary.cluster_epoch,
        log_index,
        summary.admission,
        command_kind,
        summary.wait_us
    );
    record_flight_event(
        &context,
        target,
        "metadata_command_recovery_admission",
        detail,
    );
    event_in_context(
        &context,
        target,
        "metadata_command_recovery_admission",
        Some(format_args!(
            "node_id={} pg_id={} cluster_epoch={} log_index={} admission={} command_kind={} wait_us={}",
            node_id,
            summary.pg_id,
            summary.cluster_epoch,
            log_index,
            summary.admission,
            command_kind,
            summary.wait_us
        )),
    )
}

pub fn emit_metadata_command_recovery_outcome(
    target: &'static str,
    summary: MetadataCommandRecoveryOutcomeSummary,
) -> bool {
    METADATA_COMMAND_RECOVERY_OUTCOME_TOTAL.fetch_add(1, Ordering::Relaxed);
    increment_metadata_command_dimension(
        metadata_command_recovery_outcome_dimensions(),
        summary.pg_id,
        summary.outcome,
        summary.command_kind.unwrap_or("unknown"),
    );
    let Some(context) = current_context() else {
        return false;
    };
    let node_id = summary
        .node_id
        .map(|node_id| node_id.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let log_index = summary
        .log_index
        .map(|log_index| log_index.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let command_kind = summary.command_kind.unwrap_or("unknown");
    let detail = format!(
        "node_id={} pg_id={} cluster_epoch={} log_index={} outcome={} command_kind={}",
        node_id, summary.pg_id, summary.cluster_epoch, log_index, summary.outcome, command_kind
    );
    record_flight_event(
        &context,
        target,
        "metadata_command_recovery_outcome",
        detail,
    );
    event_in_context(
        &context,
        target,
        "metadata_command_recovery_outcome",
        Some(format_args!(
            "node_id={} pg_id={} cluster_epoch={} log_index={} outcome={} command_kind={}",
            node_id, summary.pg_id, summary.cluster_epoch, log_index, summary.outcome, command_kind
        )),
    )
}

pub fn emit_metadata_command_budget_exhausted(
    target: &'static str,
    summary: MetadataCommandBudgetExhaustedSummary,
) -> bool {
    METADATA_COMMAND_BUDGET_EXHAUSTED_TOTAL.fetch_add(1, Ordering::Relaxed);
    let elapsed_us = saturating_u128_to_u64(summary.elapsed_us);
    let budget_us = saturating_u128_to_u64(summary.budget_us);
    increment_metadata_command_budget_dimension(
        summary.pg_id,
        summary.operation,
        summary.context,
        elapsed_us,
        budget_us,
    );
    let Some(context) = current_context() else {
        return false;
    };
    let pg_id = summary
        .pg_id
        .map(|pg_id| pg_id.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let max_attempts = summary
        .max_attempts
        .map(|max_attempts| max_attempts.to_string())
        .unwrap_or_else(|| "none".to_string());
    let detail = format!(
        "pg_id={} operation={} context={:?} elapsed_us={} budget_us={} attempts={} max_attempts={}",
        pg_id,
        summary.operation,
        summary.context,
        summary.elapsed_us,
        summary.budget_us,
        summary.attempts,
        max_attempts
    );
    record_flight_event(
        &context,
        target,
        "metadata_command_budget_exhausted",
        detail,
    );
    event_in_context(
        &context,
        target,
        "metadata_command_budget_exhausted",
        Some(format_args!(
            "pg_id={} operation={} context={:?} elapsed_us={} budget_us={} attempts={} max_attempts={}",
            pg_id,
            summary.operation,
            summary.context,
            summary.elapsed_us,
            summary.budget_us,
            summary.attempts,
            max_attempts
        )),
    )
}

pub fn emit_metadata_command_backoff(
    target: &'static str,
    summary: MetadataCommandBackoffSummary,
) -> bool {
    METADATA_COMMAND_BACKOFF_TOTAL.fetch_add(1, Ordering::Relaxed);
    let sleep_us = saturating_u128_to_u64(summary.sleep_us);
    METADATA_COMMAND_BACKOFF_US_TOTAL.fetch_add(sleep_us, Ordering::Relaxed);
    fetch_max_atomic(&METADATA_COMMAND_BACKOFF_US_MAX, sleep_us);
    increment_metadata_command_backoff_dimension(
        summary.pg_id,
        summary.operation,
        summary.context,
        sleep_us,
    );
    let Some(context) = current_context() else {
        return false;
    };
    let pg_id = summary
        .pg_id
        .map(|pg_id| pg_id.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let detail = format!(
        "pg_id={} operation={} context={:?} sleep_us={}",
        pg_id, summary.operation, summary.context, summary.sleep_us
    );
    record_flight_event(&context, target, "metadata_command_backoff", detail);
    event_in_context(
        &context,
        target,
        "metadata_command_backoff",
        Some(format_args!(
            "pg_id={} operation={} context={:?} sleep_us={}",
            pg_id, summary.operation, summary.context, summary.sleep_us
        )),
    )
}

pub fn emit_reclaim_queue_action(target: &'static str, summary: ReclaimQueueSummary) -> bool {
    RECLAIM_WORK_QUEUE_ACTION_TOTAL.fetch_add(1, Ordering::Relaxed);
    RECLAIM_WORK_QUEUE_DEPTH.store(summary.queue_depth as u64, Ordering::Relaxed);
    OBJECT_PAYLOAD_RECLAIM_QUEUE_DEPTH
        .store(summary.object_payload_depth as u64, Ordering::Relaxed);
    OBJECT_PAYLOAD_RECLAIM_OUTSTANDING_DEPTH.store(
        summary.object_payload_outstanding_depth as u64,
        Ordering::Relaxed,
    );
    BUCKET_DELETE_BEGIN_QUEUE_DEPTH
        .store(summary.bucket_delete_begin_depth as u64, Ordering::Relaxed);
    BUCKET_DELETE_FINALIZE_QUEUE_DEPTH.store(
        summary.bucket_delete_finalize_depth as u64,
        Ordering::Relaxed,
    );
    BUCKET_DELETE_FINALIZE_OUTSTANDING_DEPTH.store(
        summary.bucket_delete_finalize_outstanding_depth as u64,
        Ordering::Relaxed,
    );
    increment_reclaim_queue_dimension(summary.work_kind, summary.action);
    let Some(context) = current_context() else {
        return false;
    };
    let detail = format!(
        "work_kind={} action={} queue_depth={} object_payload_depth={} object_payload_outstanding_depth={} bucket_delete_begin_depth={} bucket_delete_finalize_depth={} bucket_delete_finalize_outstanding_depth={}",
        summary.work_kind,
        summary.action,
        summary.queue_depth,
        summary.object_payload_depth,
        summary.object_payload_outstanding_depth,
        summary.bucket_delete_begin_depth,
        summary.bucket_delete_finalize_depth,
        summary.bucket_delete_finalize_outstanding_depth
    );
    record_flight_event(&context, target, "reclaim_queue_action", detail);
    event_in_context(
        &context,
        target,
        "reclaim_queue_action",
        Some(format_args!(
            "work_kind={} action={} queue_depth={} object_payload_depth={} object_payload_outstanding_depth={} bucket_delete_begin_depth={} bucket_delete_finalize_depth={}",
            summary.work_kind,
            summary.action,
            summary.queue_depth,
            summary.object_payload_depth,
            summary.object_payload_outstanding_depth,
            summary.bucket_delete_begin_depth,
            summary.bucket_delete_finalize_depth
        )),
    )
}

pub fn emit_object_payload_reclaim_event(
    target: &'static str,
    summary: ObjectPayloadReclaimEventSummary,
) -> bool {
    OBJECT_PAYLOAD_RECLAIM_EVENT_TOTAL.fetch_add(1, Ordering::Relaxed);
    increment_object_payload_reclaim_dimension(
        object_payload_reclaim_event_dimensions(),
        summary.pg_id,
        summary.event,
    );
    let Some(context) = current_context() else {
        return false;
    };
    let detail = format!("pg_id={} event={}", summary.pg_id, summary.event);
    record_flight_event(&context, target, "object_payload_reclaim_event", detail);
    event_in_context(
        &context,
        target,
        "object_payload_reclaim_event",
        Some(format_args!(
            "pg_id={} event={}",
            summary.pg_id, summary.event
        )),
    )
}

pub fn emit_object_payload_reclaim_durable_scan(
    target: &'static str,
    summary: ObjectPayloadReclaimEventSummary,
) -> bool {
    OBJECT_PAYLOAD_RECLAIM_DURABLE_SCAN_TOTAL.fetch_add(1, Ordering::Relaxed);
    increment_object_payload_reclaim_dimension(
        object_payload_reclaim_durable_scan_dimensions(),
        summary.pg_id,
        summary.event,
    );
    let Some(context) = current_context() else {
        return false;
    };
    let detail = format!("pg_id={} outcome={}", summary.pg_id, summary.event);
    record_flight_event(
        &context,
        target,
        "object_payload_reclaim_durable_scan",
        detail,
    );
    event_in_context(
        &context,
        target,
        "object_payload_reclaim_durable_scan",
        Some(format_args!(
            "pg_id={} outcome={}",
            summary.pg_id, summary.event
        )),
    )
}

pub fn emit_shard_repair_event(target: &'static str, summary: ShardRepairEventSummary) -> bool {
    SHARD_REPAIR_EVENT_TOTAL.fetch_add(1, Ordering::Relaxed);
    if let Some(queue_depth) = summary.queue_depth {
        SHARD_REPAIR_QUEUE_DEPTH.store(queue_depth as u64, Ordering::Relaxed);
    }
    if let Some(shards_rewritten) = summary.shards_rewritten {
        SHARD_REPAIR_SHARDS_REWRITTEN_TOTAL.fetch_add(shards_rewritten as u64, Ordering::Relaxed);
    }
    increment_shard_repair_dimension(summary.pg_id, summary.event);
    let Some(context) = current_context() else {
        return false;
    };
    let pg_id = summary
        .pg_id
        .map(|pg_id| pg_id.to_string())
        .unwrap_or_else(|| "none".to_string());
    let mut detail = format!("pg_id={} event={}", pg_id, summary.event);
    if let Some(queue_depth) = summary.queue_depth {
        detail.push_str(&format!(" queue_depth={queue_depth}"));
    }
    if let Some(shards_rewritten) = summary.shards_rewritten {
        detail.push_str(&format!(" shards_rewritten={shards_rewritten}"));
    }
    record_flight_event(&context, target, "shard_repair_event", detail.clone());
    event_in_context(
        &context,
        target,
        "shard_repair_event",
        Some(format_args!("{detail}")),
    )
}

pub fn emit_shard_backfill_event(target: &'static str, summary: ShardBackfillEventSummary) -> bool {
    SHARD_BACKFILL_EVENT_TOTAL.fetch_add(1, Ordering::Relaxed);
    if let Some(queue_depth) = summary.queue_depth {
        SHARD_BACKFILL_QUEUE_DEPTH.store(queue_depth as u64, Ordering::Relaxed);
    }
    if let Some(shards_written) = summary.shards_written {
        SHARD_BACKFILL_SHARDS_WRITTEN_TOTAL.fetch_add(shards_written as u64, Ordering::Relaxed);
    }
    increment_shard_backfill_dimension(summary.pg_id, summary.event);
    let Some(context) = current_context() else {
        return false;
    };
    let pg_id = summary
        .pg_id
        .map(|pg_id| pg_id.to_string())
        .unwrap_or_else(|| "none".to_string());
    let mut detail = format!("pg_id={} event={}", pg_id, summary.event);
    if let Some(queue_depth) = summary.queue_depth {
        detail.push_str(&format!(" queue_depth={queue_depth}"));
    }
    if let Some(shards_written) = summary.shards_written {
        detail.push_str(&format!(" shards_written={shards_written}"));
    }
    record_flight_event(&context, target, "shard_backfill_event", detail.clone());
    event_in_context(
        &context,
        target,
        "shard_backfill_event",
        Some(format_args!("{detail}")),
    )
}

pub fn emit_shard_backfill_candidate_scan(
    target: &'static str,
    summary: ShardBackfillCandidateScanSummary,
) -> bool {
    SHARD_BACKFILL_CANDIDATE_SCAN_TOTAL.fetch_add(1, Ordering::Relaxed);
    SHARD_BACKFILL_CANDIDATE_SCANNED_TOTAL.fetch_add(summary.scanned as u64, Ordering::Relaxed);
    SHARD_BACKFILL_CANDIDATE_CURRENT_EPOCH_TOTAL
        .fetch_add(summary.current_epoch as u64, Ordering::Relaxed);
    SHARD_BACKFILL_CANDIDATE_ALREADY_QUEUED_TOTAL
        .fetch_add(summary.already_queued as u64, Ordering::Relaxed);
    SHARD_BACKFILL_CANDIDATE_ALREADY_COMPLETE_TOTAL
        .fetch_add(summary.already_complete as u64, Ordering::Relaxed);
    SHARD_BACKFILL_CANDIDATE_ENQUEUED_TOTAL.fetch_add(summary.enqueued as u64, Ordering::Relaxed);
    SHARD_BACKFILL_CANDIDATE_UNRECOVERABLE_TOTAL
        .fetch_add(summary.unrecoverable as u64, Ordering::Relaxed);
    SHARD_BACKFILL_CANDIDATE_DEFERRED_TOTAL.fetch_add(summary.deferred as u64, Ordering::Relaxed);
    SHARD_BACKFILL_CANDIDATE_FAILED_TOTAL.fetch_add(summary.failed as u64, Ordering::Relaxed);
    if summary.limit_reached {
        SHARD_BACKFILL_CANDIDATE_LIMIT_REACHED_TOTAL.fetch_add(1, Ordering::Relaxed);
    }
    let Some(context) = current_context() else {
        return false;
    };
    let detail = format!(
        "scanned={} current_epoch={} already_queued={} already_complete={} enqueued={} unrecoverable={} deferred={} failed={} limit_reached={}",
        summary.scanned,
        summary.current_epoch,
        summary.already_queued,
        summary.already_complete,
        summary.enqueued,
        summary.unrecoverable,
        summary.deferred,
        summary.failed,
        summary.limit_reached,
    );
    record_flight_event(
        &context,
        target,
        "shard_backfill_candidate_scan",
        detail.clone(),
    );
    event_in_context(
        &context,
        target,
        "shard_backfill_candidate_scan",
        Some(format_args!("{detail}")),
    )
}

pub fn emit_shard_backfill_candidate_scan_error(
    target: &'static str,
    error: &impl fmt::Display,
) -> bool {
    SHARD_BACKFILL_CANDIDATE_SCAN_TOTAL.fetch_add(1, Ordering::Relaxed);
    SHARD_BACKFILL_CANDIDATE_SCAN_ERROR_TOTAL.fetch_add(1, Ordering::Relaxed);
    let Some(context) = current_context() else {
        return false;
    };
    let detail = truncate_detail(format!("error={error}"));
    record_flight_event(
        &context,
        target,
        "shard_backfill_candidate_scan_error",
        detail.clone(),
    );
    event_in_context(
        &context,
        target,
        "shard_backfill_candidate_scan_error",
        Some(format_args!("{detail}")),
    )
}

pub fn emit_metadata_command_checkpoint_record_scan(
    target: &'static str,
    summary: MetadataCommandCheckpointRecordSummary,
) -> bool {
    METADATA_COMMAND_CHECKPOINT_RECORD_SCAN_TOTAL.fetch_add(1, Ordering::Relaxed);
    METADATA_COMMAND_CHECKPOINT_RECORD_SCANNED_TOTAL
        .fetch_add(summary.scanned as u64, Ordering::Relaxed);
    METADATA_COMMAND_CHECKPOINT_RECORD_RECORDED_TOTAL
        .fetch_add(summary.recorded as u64, Ordering::Relaxed);
    METADATA_COMMAND_CHECKPOINT_RECORD_ALREADY_CURRENT_TOTAL
        .fetch_add(summary.already_current as u64, Ordering::Relaxed);
    METADATA_COMMAND_CHECKPOINT_RECORD_SKIPPED_CADENCE_TOTAL
        .fetch_add(summary.skipped_cadence as u64, Ordering::Relaxed);
    METADATA_COMMAND_CHECKPOINT_RECORD_SKIPPED_INACTIVE_TOTAL
        .fetch_add(summary.skipped_inactive as u64, Ordering::Relaxed);
    METADATA_COMMAND_CHECKPOINT_RECORD_SKIPPED_EMPTY_TOTAL
        .fetch_add(summary.skipped_empty as u64, Ordering::Relaxed);
    METADATA_COMMAND_CHECKPOINT_RECORD_SKIPPED_STALE_EPOCH_TOTAL
        .fetch_add(summary.skipped_stale_epoch as u64, Ordering::Relaxed);
    METADATA_COMMAND_CHECKPOINT_RECORD_COMPACTED_TOTAL
        .fetch_add(summary.compacted as u64, Ordering::Relaxed);
    METADATA_COMMAND_CHECKPOINT_RECORD_COMPACTION_DELETED_ENTRIES_TOTAL
        .fetch_add(summary.compaction_deleted_entries, Ordering::Relaxed);
    METADATA_COMMAND_CHECKPOINT_RECORD_COMPACTION_NOOP_TOTAL
        .fetch_add(summary.compaction_noop as u64, Ordering::Relaxed);
    METADATA_COMMAND_CHECKPOINT_RECORD_COMPACTION_NO_CHECKPOINT_TOTAL
        .fetch_add(summary.compaction_no_checkpoint as u64, Ordering::Relaxed);
    METADATA_COMMAND_CHECKPOINT_RECORD_COMPACTION_PENDING_TOTAL
        .fetch_add(summary.compaction_pending as u64, Ordering::Relaxed);
    METADATA_COMMAND_CHECKPOINT_RECORD_COMPACTION_FAILED_TOTAL
        .fetch_add(summary.compaction_failed as u64, Ordering::Relaxed);
    METADATA_COMMAND_CHECKPOINT_RECORD_FAILED_TOTAL
        .fetch_add(summary.failed as u64, Ordering::Relaxed);
    if summary.limit_reached {
        METADATA_COMMAND_CHECKPOINT_RECORD_LIMIT_REACHED_TOTAL.fetch_add(1, Ordering::Relaxed);
    }
    let Some(context) = current_context() else {
        return false;
    };
    let detail = format!(
        "scanned={} recorded={} already_current={} skipped_cadence={} skipped_inactive={} skipped_empty={} skipped_stale_epoch={} compacted={} compaction_deleted_entries={} compaction_noop={} compaction_no_checkpoint={} compaction_pending={} compaction_failed={} failed={} limit_reached={}",
        summary.scanned,
        summary.recorded,
        summary.already_current,
        summary.skipped_cadence,
        summary.skipped_inactive,
        summary.skipped_empty,
        summary.skipped_stale_epoch,
        summary.compacted,
        summary.compaction_deleted_entries,
        summary.compaction_noop,
        summary.compaction_no_checkpoint,
        summary.compaction_pending,
        summary.compaction_failed,
        summary.failed,
        summary.limit_reached,
    );
    record_flight_event(
        &context,
        target,
        "metadata_command_checkpoint_record_scan",
        detail.clone(),
    );
    event_in_context(
        &context,
        target,
        "metadata_command_checkpoint_record_scan",
        Some(format_args!("{detail}")),
    )
}

pub fn emit_metadata_command_checkpoint_record_scan_error(
    target: &'static str,
    error: &impl fmt::Display,
) -> bool {
    METADATA_COMMAND_CHECKPOINT_RECORD_SCAN_TOTAL.fetch_add(1, Ordering::Relaxed);
    METADATA_COMMAND_CHECKPOINT_RECORD_SCAN_ERROR_TOTAL.fetch_add(1, Ordering::Relaxed);
    let Some(context) = current_context() else {
        return false;
    };
    let detail = truncate_detail(format!("error={error}"));
    record_flight_event(
        &context,
        target,
        "metadata_command_checkpoint_record_scan_error",
        detail.clone(),
    );
    event_in_context(
        &context,
        target,
        "metadata_command_checkpoint_record_scan_error",
        Some(format_args!("{detail}")),
    )
}

pub fn emit_metadata_command_checkpoint_record_error(
    target: &'static str,
    summary: MetadataCommandCheckpointRecordErrorSummary,
) -> bool {
    increment_metadata_command_checkpoint_record_error_dimension(
        summary.pg_id,
        summary.outcome,
        summary.error_kind,
    );
    event(
        target,
        "metadata_command_checkpoint_record_error",
        Some(format_args!(
            "pg_id={} outcome={} error_kind={}",
            summary.pg_id, summary.outcome, summary.error_kind
        )),
    )
}

pub fn emit_background_work_admission_event(
    target: &'static str,
    summary: BackgroundWorkAdmissionSummary,
) -> bool {
    BACKGROUND_WORK_ADMISSION_EVENT_TOTAL.fetch_add(1, Ordering::Relaxed);
    let global_active_total = match summary.event {
        BackgroundWorkAdmissionEvent::Admitted => BACKGROUND_WORK_ACTIVE_TOTAL
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1),
        BackgroundWorkAdmissionEvent::Finished => {
            BACKGROUND_WORK_FINISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
            if let Some(elapsed_us) = summary.elapsed_us {
                BACKGROUND_WORK_ELAPSED_US_TOTAL.fetch_add(elapsed_us, Ordering::Relaxed);
            }
            BACKGROUND_WORK_ACTIVE_TOTAL
                .fetch_sub(1, Ordering::Relaxed)
                .saturating_sub(1)
        }
        BackgroundWorkAdmissionEvent::DeniedLimit
        | BackgroundWorkAdmissionEvent::DeniedForegroundPressure
        | BackgroundWorkAdmissionEvent::DeniedKnownDamageActive
        | BackgroundWorkAdmissionEvent::DeniedBacklogPressure => {
            BACKGROUND_WORK_ACTIVE_TOTAL.load(Ordering::Relaxed)
        }
    };
    let class = summary.class.as_str();
    let event = summary.event.as_str();
    increment_background_work_admission_dimension(class, event, summary.elapsed_us);
    let Some(context) = current_context() else {
        return false;
    };
    let detail = match summary.elapsed_us {
        Some(elapsed_us) => format!(
            "class={} event={} global_active_total={} local_active_total={} elapsed_us={}",
            class, event, global_active_total, summary.active_total, elapsed_us
        ),
        None => format!(
            "class={} event={} global_active_total={} local_active_total={}",
            class, event, global_active_total, summary.active_total
        ),
    };
    record_flight_event(&context, target, "background_work_admission_event", detail);
    event_in_context(
        &context,
        target,
        "background_work_admission_event",
        Some(format_args!(
            "class={} event={} global_active_total={} local_active_total={} elapsed_us={}",
            class,
            event,
            global_active_total,
            summary.active_total,
            summary.elapsed_us.unwrap_or(0)
        )),
    )
}

pub fn emit_storage_rpc_error(target: &'static str, summary: StorageRpcErrorSummary<'_>) -> bool {
    STORAGE_RPC_ERROR_TOTAL.fetch_add(1, Ordering::Relaxed);
    let Some(context) = current_context() else {
        return false;
    };
    let message_hash = stable_hash_hex(summary.message);
    let detail = format!(
        "node_id={} rpc_kind={} error_code={} message_len={} message_hash={}",
        summary.node_id,
        summary.rpc_kind,
        summary.error_code,
        summary.message.len(),
        message_hash
    );
    record_flight_event(&context, target, "storage_rpc_error", detail);
    event_in_context(
        &context,
        target,
        "storage_rpc_error",
        Some(format_args!(
            "node_id={} rpc_kind={} error_code={} message_len={} message_hash={}",
            summary.node_id,
            summary.rpc_kind,
            summary.error_code,
            summary.message.len(),
            message_hash
        )),
    )
}

pub fn emit_storage_rpc_admission_attempt() {
    STORAGE_RPC_ADMISSION_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn emit_storage_rpc_admission_wait(
    target: &'static str,
    summary: StorageRpcAdmissionWaitSummary<'_>,
) -> bool {
    STORAGE_RPC_ADMISSION_WAIT_TOTAL.fetch_add(1, Ordering::Relaxed);
    STORAGE_RPC_ADMISSION_WAIT_US_TOTAL
        .fetch_add(saturating_u128_to_u64(summary.wait_us), Ordering::Relaxed);
    let admission_class = summary.admission_class.as_str();
    let Some(context) = current_context() else {
        return false;
    };
    record_flight_event(
        &context,
        target,
        "storage_rpc_admission_wait",
        format!(
            "node_id={} rpc_kind={} admission_class={} wait_us={}",
            summary.node_id, summary.rpc_kind, admission_class, summary.wait_us
        ),
    );
    event_in_context(
        &context,
        target,
        "storage_rpc_admission_wait",
        Some(format_args!(
            "node_id={} rpc_kind={} admission_class={} wait_us={}",
            summary.node_id, summary.rpc_kind, admission_class, summary.wait_us
        )),
    )
}

pub fn emit_storage_rpc_admission_timeout(
    target: &'static str,
    summary: StorageRpcAdmissionTimeoutSummary<'_>,
) -> bool {
    STORAGE_RPC_ADMISSION_TIMEOUT_TOTAL.fetch_add(1, Ordering::Relaxed);
    let admission_class = summary.admission_class.as_str();
    let Some(context) = current_context() else {
        return false;
    };
    record_flight_event(
        &context,
        target,
        "storage_rpc_admission_timeout",
        format!(
            "node_id={} rpc_kind={} admission_class={} wait_us={} timeout_us={}",
            summary.node_id, summary.rpc_kind, admission_class, summary.wait_us, summary.timeout_us
        ),
    );
    event_in_context(
        &context,
        target,
        "storage_rpc_admission_timeout",
        Some(format_args!(
            "node_id={} rpc_kind={} admission_class={} wait_us={} timeout_us={}",
            summary.node_id, summary.rpc_kind, admission_class, summary.wait_us, summary.timeout_us
        )),
    )
}

pub fn emit_shard_scavenger_observation(
    target: &'static str,
    summary: ShardScavengerObservationSummary<'_>,
) -> bool {
    SHARD_SCAVENGER_OBSERVATION_TOTAL.fetch_add(1, Ordering::Relaxed);
    if summary.reason == "scan_incomplete" {
        SHARD_SCAVENGER_SCAN_INCOMPLETE_TOTAL.fetch_add(1, Ordering::Relaxed);
    }
    event(
        target,
        "shard_scavenger_observation",
        Some(format_args!(
            "node_id={} data_pg_id={} shard_index={} shard_key={} reason={} file_exists={} shard_row_exists={} last_error={}",
            summary.node_id,
            summary.data_pg_id,
            summary.shard_index,
            summary.shard_key_hex,
            summary.reason,
            summary.file_exists,
            summary.shard_row_exists,
            summary.last_error.map_or("<none>".to_string(), |error| escaped(error).to_string())
        )),
    )
}

pub fn configure(enabled: bool, filter: Option<&str>, file_path: Option<&str>) -> bool {
    configure_with_options(enabled, filter, file_path, false)
}

pub fn configure_with_options(
    enabled: bool,
    filter: Option<&str>,
    file_path: Option<&str>,
    sync_file: bool,
) -> bool {
    TRACE_CONFIG_OVERRIDE
        .set(TraceConfig {
            enabled,
            filters: parse_filters(filter),
            file_path: normalize_file_path(file_path),
            sync_file,
        })
        .is_ok()
}

fn write_trace_line(args: fmt::Arguments<'_>) {
    match trace_sink() {
        TraceSink::Stderr => write_stderr_line(args),
        TraceSink::AsyncFile(file) => file.write_line(args),
        TraceSink::SyncFile(file) => {
            let mut file = file.lock().unwrap_or_else(|err| err.into_inner());
            let _ = writeln!(file, "{args}");
            let _ = file.flush();
        }
    }
}

fn matches_enabled(value: &str) -> bool {
    !matches!(
        value,
        "" | "0" | "false" | "False" | "FALSE" | "off" | "OFF"
    )
}

fn tracing_enabled_for(target: &str) -> bool {
    let config = trace_config();
    config.enabled
        && (config.filters.is_empty()
            || config
                .filters
                .iter()
                .any(|filter| target == filter.as_ref() || target.starts_with(filter.as_ref())))
}

fn unix_micros() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
}

pub struct AttachedTrace {
    previous: Option<TraceState>,
}

impl AttachedTrace {
    #[must_use]
    pub fn new(context: TraceContext) -> Self {
        let previous = TRACE_STATE.with(|slot| {
            let mut slot = slot.borrow_mut();
            let next_depth = slot.as_ref().map_or(0, |state| state.depth);
            slot.replace(TraceState {
                context,
                depth: next_depth,
            })
        });
        Self { previous }
    }
}

impl Drop for AttachedTrace {
    fn drop(&mut self) {
        TRACE_STATE.with(|slot| {
            *slot.borrow_mut() = self.previous.take();
        });
    }
}

#[must_use]
pub fn current_context() -> Option<TraceContext> {
    TRACE_STATE.with(|slot| slot.borrow().as_ref().map(|state| state.context.clone()))
}

#[cfg(feature = "deep-tracing")]
pub struct TraceScope {
    target: &'static str,
    name: &'static str,
    start: Instant,
    entered_depth: usize,
    context: Option<TraceContext>,
}

#[cfg(not(feature = "deep-tracing"))]
pub struct TraceScope;

#[cfg(feature = "deep-tracing")]
impl TraceScope {
    #[must_use]
    pub fn new(
        target: &'static str,
        name: &'static str,
        fields: Option<fmt::Arguments<'_>>,
    ) -> Self {
        if !tracing_enabled_for(target) {
            return Self {
                target,
                name,
                start: Instant::now(),
                entered_depth: 0,
                context: None,
            };
        }

        let context = TRACE_STATE.with(|slot| {
            let mut slot = slot.borrow_mut();
            let state = slot.as_mut()?;
            let depth = state.depth;
            let context = state.context.clone();
            state.depth += 1;
            Some((context, depth))
        });

        let Some((context, depth)) = context else {
            return Self {
                target,
                name,
                start: Instant::now(),
                entered_depth: 0,
                context: None,
            };
        };

        if let Some(fields) = fields {
            write_trace_line(format_args!(
                "trace ts_us={} trace_id={} depth={} event=enter target={} span={} {}",
                unix_micros(),
                context.trace_id(),
                depth,
                target,
                name,
                fields
            ));
        } else {
            write_trace_line(format_args!(
                "trace ts_us={} trace_id={} depth={} event=enter target={} span={}",
                unix_micros(),
                context.trace_id(),
                depth,
                target,
                name
            ));
        }

        Self {
            target,
            name,
            start: Instant::now(),
            entered_depth: depth,
            context: Some(context),
        }
    }
}

#[cfg(not(feature = "deep-tracing"))]
impl TraceScope {
    #[must_use]
    #[inline]
    pub fn new(
        target: &'static str,
        name: &'static str,
        fields: Option<fmt::Arguments<'_>>,
    ) -> Self {
        let _ = (target, name, fields);
        Self
    }
}

#[cfg(feature = "deep-tracing")]
impl Drop for TraceScope {
    fn drop(&mut self) {
        let Some(context) = self.context.as_ref() else {
            return;
        };

        TRACE_STATE.with(|slot| {
            if let Some(state) = slot.borrow_mut().as_mut() {
                state.depth = state.depth.saturating_sub(1);
            }
        });

        write_trace_line(format_args!(
            "trace ts_us={} trace_id={} depth={} event=exit target={} span={} dur_us={}",
            unix_micros(),
            context.trace_id(),
            self.entered_depth,
            self.target,
            self.name,
            self.start.elapsed().as_micros()
        ));
    }
}

#[must_use]
pub fn event(target: &'static str, name: &'static str, fields: Option<fmt::Arguments<'_>>) -> bool {
    if !tracing_enabled_for(target) {
        return false;
    }

    let current = TRACE_STATE.with(|slot| {
        slot.borrow()
            .as_ref()
            .map(|state| (state.context.trace_id().to_owned(), state.depth))
    });
    let (trace_id, depth) = current.unwrap_or_else(|| ("background".to_string(), 0));
    if let Some(fields) = fields {
        write_trace_line(format_args!(
            "trace ts_us={} trace_id={} depth={} event={} target={} {}",
            unix_micros(),
            trace_id,
            depth,
            name,
            target,
            fields
        ));
    } else {
        write_trace_line(format_args!(
            "trace ts_us={} trace_id={} depth={} event={} target={}",
            unix_micros(),
            trace_id,
            depth,
            name,
            target
        ));
    }
    true
}

#[must_use]
pub fn event_in_context(
    context: &TraceContext,
    target: &'static str,
    name: &'static str,
    fields: Option<fmt::Arguments<'_>>,
) -> bool {
    if !tracing_enabled_for(target) {
        return false;
    }

    let depth = TRACE_STATE.with(|slot| slot.borrow().as_ref().map_or(0, |state| state.depth));
    if let Some(fields) = fields {
        write_trace_line(format_args!(
            "trace ts_us={} trace_id={} depth={} event={} target={} {}",
            unix_micros(),
            context.trace_id(),
            depth,
            name,
            target,
            fields
        ));
    } else {
        write_trace_line(format_args!(
            "trace ts_us={} trace_id={} depth={} event={} target={}",
            unix_micros(),
            context.trace_id(),
            depth,
            name,
            target
        ));
    }
    true
}

#[macro_export]
macro_rules! trace_scope {
    ($target:expr, $name:expr) => {
        let _argmin_trace_scope = $crate::TraceScope::new($target, $name, None);
    };
    ($target:expr, $name:expr, $($arg:tt)*) => {
        let _argmin_trace_scope =
            $crate::TraceScope::new($target, $name, Some(format_args!($($arg)*)));
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static METRICS_TEST_MUTEX: Mutex<()> = Mutex::new(());

    #[test]
    fn escaped_renders_debug_style_one_line_output() {
        assert_eq!(
            format!("{}", escaped("line1\r\nline2\t\u{1b}[31m")),
            r#""line1\r\nline2\t\u{1b}[31m""#
        );
    }

    #[test]
    fn redacted_never_formats_secret_value() {
        assert_eq!(
            format!("{}", redacted("sigv4_credential")),
            "<redacted:sigv4_credential>"
        );
        assert_eq!(
            format!("{:?}", redacted("sse_c_key")),
            "<redacted:sse_c_key>"
        );
    }

    #[test]
    fn query_summary_detects_sigv4_queries() {
        let summary = query_summary(
            "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=abc&X-Amz-Signature=deadbeef",
        );
        assert!(summary.has_query());
        assert_eq!(summary.param_count(), 3);
        assert!(summary.has_sigv4_params());
    }

    #[test]
    fn query_summary_handles_empty_and_non_sigv4_queries() {
        let empty = query_summary("");
        assert!(!empty.has_query());
        assert_eq!(empty.param_count(), 0);
        assert!(!empty.has_sigv4_params());

        let ordinary = query_summary("prefix=a&delimiter=/");
        assert!(ordinary.has_query());
        assert_eq!(ordinary.param_count(), 2);
        assert!(!ordinary.has_sigv4_params());
    }

    #[test]
    fn metrics_snapshot_iter_named_binds_names_to_fields() {
        let snapshot = MetricsSnapshot {
            request_start_total: 11,
            storage_rpc_error_total: 22,
            bucket_lock_wait_exceeded_total: 33,
            metadata_command_checkpoint_record_compaction_failed_total: 44,
            stream_upload_finalize_error_total: 55,
            stream_upload_finalize_started_total: 66,
            ..MetricsSnapshot::default()
        };
        let entries: Vec<_> = snapshot.iter_named().collect();
        let map: std::collections::HashMap<_, _> = entries.iter().copied().collect();

        assert_eq!(
            entries.len(),
            map.len(),
            "fixed metric names must be unique"
        );
        assert_eq!(map.get("request_start_total").copied(), Some(11));
        assert_eq!(map.get("storage_rpc_error_total").copied(), Some(22));
        assert_eq!(
            map.get("bucket_lock_wait_exceeded_total").copied(),
            Some(33)
        );
        assert_eq!(
            map.get("metadata_command_checkpoint_record_compaction_failed_total")
                .copied(),
            Some(44)
        );
        assert_eq!(
            map.get("stream_upload_finalize_error_total").copied(),
            Some(55)
        );
        assert_eq!(
            map.get("stream_upload_finalize_started_total").copied(),
            Some(66)
        );
    }

    #[test]
    fn inflight_requests_guard_updates_snapshot() {
        let _guard = METRICS_TEST_MUTEX.lock().unwrap();
        let before = metrics_snapshot();
        {
            let inflight_guard = inflight_requests_guard();
            let during = metrics_snapshot();
            assert_eq!(during.inflight_requests, before.inflight_requests + 1);
            drop(inflight_guard);
        }
        let after = metrics_snapshot();
        assert_eq!(after.inflight_requests, before.inflight_requests);
    }

    #[test]
    fn helper_emitters_increment_metrics_even_when_tracing_is_disabled() {
        let _guard = METRICS_TEST_MUTEX.lock().unwrap();
        let before = metrics_snapshot();
        let ctx = TraceContext::new_request();
        let summary = RequestSummary {
            method: "GET",
            path: "/bucket/key",
            query: query_summary("partNumber=1"),
            status_code: 206,
            streaming: true,
            body_len: 123,
            bytes_sent: 64,
            lifetime_us: 42,
        };

        emit_request_start(
            &ctx,
            "server_http",
            RequestStartSummary {
                method: "GET",
                path: "/bucket/key",
                query: query_summary("partNumber=1"),
            },
        );
        emit_request_finish(&ctx, "server_http", summary, "complete");
        emit_request_error(
            &ctx,
            "server_http",
            summary,
            "response_body",
            "InternalError",
            "storage_rpc_resource_exhausted",
        );
        emit_slow_request(&ctx, "server_http", summary, "error", Some("InternalError"));
        emit_request_admission_wait(
            &ctx,
            "server_http",
            RequestAdmissionWaitSummary {
                method: "PUT",
                path: "/bucket/key",
                query: query_summary("X-Amz-Signature=secret"),
                wait_us: 123,
            },
        );
        emit_request_admission_timeout(
            &ctx,
            "server_http",
            RequestAdmissionTimeoutSummary {
                method: "PUT",
                path: "/bucket/key",
                query: query_summary("X-Amz-Signature=secret"),
                wait_us: 5_000_000,
                timeout_us: 5_000_000,
            },
        );
        emit_storage_rpc_admission_attempt();
        emit_storage_rpc_admission_wait(
            "storage_node_client",
            StorageRpcAdmissionWaitSummary {
                node_id: 7,
                rpc_kind: "shard write",
                admission_class: StorageRpcAdmissionClass::Progress,
                wait_us: 456,
            },
        );
        emit_storage_rpc_admission_timeout(
            "storage_node_client",
            StorageRpcAdmissionTimeoutSummary {
                node_id: 7,
                rpc_kind: "shard write",
                admission_class: StorageRpcAdmissionClass::Progress,
                wait_us: 5_000_000,
                timeout_us: 5_000_000,
            },
        );
        emit_bucket_lock_wait_exceeded(&ctx, "storage", &"bucket", 3, 1_500);
        emit_metadata_command_conflict(
            "storage",
            MetadataCommandConflictSummary {
                node_id: Some(7),
                pg_id: 11,
                cluster_epoch: 1,
                log_index: Some(13),
                kind: "log_conflict",
                command_kind: Some("CommitDirectPutObject"),
            },
        );
        emit_metadata_command_pending_slot_action(
            "storage",
            MetadataCommandPendingSlotActionSummary {
                node_id: Some(7),
                pg_id: 11,
                cluster_epoch: 1,
                log_index: Some(14),
                action: "drain_attempt",
                command_kind: Some("AppendStreamSegment"),
            },
        );
        emit_metadata_command_session_wait(
            "storage",
            MetadataCommandSessionWaitSummary {
                node_id: 7,
                pg_id: 11,
                wait_us: 55,
            },
        );
        emit_metadata_command_recovery_admission(
            "storage",
            MetadataCommandRecoveryAdmissionSummary {
                node_id: Some(7),
                pg_id: 11,
                cluster_epoch: 1,
                log_index: Some(15),
                admission: "leader",
                command_kind: Some("ReserveObjectVersion"),
                wait_us: 0,
            },
        );
        emit_metadata_command_recovery_admission(
            "storage",
            MetadataCommandRecoveryAdmissionSummary {
                node_id: Some(7),
                pg_id: 11,
                cluster_epoch: 1,
                log_index: Some(15),
                admission: "waited",
                command_kind: Some("ReserveObjectVersion"),
                wait_us: 99,
            },
        );
        emit_metadata_command_recovery_admission(
            "storage",
            MetadataCommandRecoveryAdmissionSummary {
                node_id: Some(7),
                pg_id: 11,
                cluster_epoch: 1,
                log_index: Some(15),
                admission: "timed_out",
                command_kind: Some("ReserveObjectVersion"),
                wait_us: 101,
            },
        );
        emit_metadata_command_recovery_outcome(
            "storage",
            MetadataCommandRecoveryOutcomeSummary {
                node_id: Some(7),
                pg_id: 11,
                cluster_epoch: 1,
                log_index: Some(15),
                outcome: "retry_partial_exact_conflict",
                command_kind: Some("ReserveObjectVersion"),
            },
        );
        emit_metadata_command_budget_exhausted(
            "storage",
            MetadataCommandBudgetExhaustedSummary {
                pg_id: Some(11),
                operation: "bucket_delete_begin",
                context: "bucket delete begin metadata convergence budget exhausted",
                elapsed_us: 10_123_456,
                budget_us: 10_000_000,
                attempts: 37,
                max_attempts: None,
            },
        );
        emit_metadata_command_backoff(
            "storage",
            MetadataCommandBackoffSummary {
                pg_id: Some(11),
                operation: "bucket_delete_begin",
                context: "bucket delete drain bucket pg command",
                sleep_us: 12_345,
            },
        );
        emit_reclaim_queue_action(
            "storage",
            ReclaimQueueSummary {
                work_kind: "object_payload",
                action: "enqueue",
                queue_depth: 3,
                object_payload_depth: 2,
                object_payload_outstanding_depth: 2,
                bucket_delete_begin_depth: 1,
                bucket_delete_finalize_depth: 1,
                bucket_delete_finalize_outstanding_depth: 1,
            },
        );
        emit_object_payload_reclaim_event(
            "storage",
            ObjectPayloadReclaimEventSummary {
                pg_id: 11,
                event: "deferred_pending_command",
            },
        );
        emit_object_payload_reclaim_durable_scan(
            "storage",
            ObjectPayloadReclaimEventSummary {
                pg_id: 11,
                event: "queued",
            },
        );
        emit_shard_repair_event(
            "storage",
            ShardRepairEventSummary {
                pg_id: Some(11),
                event: "queued",
                queue_depth: Some(4),
                shards_rewritten: None,
            },
        );
        emit_shard_repair_event(
            "storage",
            ShardRepairEventSummary {
                pg_id: Some(11),
                event: "repaired",
                queue_depth: None,
                shards_rewritten: Some(2),
            },
        );
        emit_shard_backfill_event(
            "storage",
            ShardBackfillEventSummary {
                pg_id: Some(11),
                event: "claim_started",
                queue_depth: Some(3),
                shards_written: None,
            },
        );
        emit_shard_backfill_event(
            "storage",
            ShardBackfillEventSummary {
                pg_id: Some(11),
                event: "backfilled",
                queue_depth: None,
                shards_written: Some(2),
            },
        );
        emit_shard_backfill_candidate_scan(
            "storage",
            ShardBackfillCandidateScanSummary {
                scanned: 7,
                current_epoch: 2,
                already_queued: 5,
                already_complete: 1,
                enqueued: 3,
                unrecoverable: 1,
                deferred: 6,
                failed: 4,
                limit_reached: true,
            },
        );
        emit_shard_backfill_candidate_scan_error("storage", &"scanner unavailable");
        emit_metadata_command_checkpoint_record_scan(
            "storage",
            MetadataCommandCheckpointRecordSummary {
                scanned: 11,
                recorded: 3,
                already_current: 4,
                skipped_cadence: 5,
                skipped_inactive: 2,
                skipped_empty: 12,
                skipped_stale_epoch: 11,
                compacted: 6,
                compaction_deleted_entries: 13,
                compaction_noop: 7,
                compaction_no_checkpoint: 8,
                compaction_pending: 9,
                compaction_failed: 10,
                failed: 1,
                limit_reached: true,
            },
        );
        emit_metadata_command_checkpoint_record_scan_error("storage", &"checkpoint unavailable");
        emit_metadata_command_checkpoint_record_error(
            "storage",
            MetadataCommandCheckpointRecordErrorSummary {
                pg_id: 11,
                outcome: "skipped_stale",
                error_kind: "storage_rpc_unknown_pg",
            },
        );
        emit_background_work_admission_event(
            "storage",
            BackgroundWorkAdmissionSummary {
                class: BackgroundWorkClass::KnownDamageRepair,
                event: BackgroundWorkAdmissionEvent::Admitted,
                active_total: 1,
                elapsed_us: None,
            },
        );
        emit_background_work_admission_event(
            "storage",
            BackgroundWorkAdmissionSummary {
                class: BackgroundWorkClass::KnownDamageRepair,
                event: BackgroundWorkAdmissionEvent::Finished,
                active_total: 0,
                elapsed_us: Some(321),
            },
        );
        emit_storage_rpc_error(
            "storage",
            StorageRpcErrorSummary {
                node_id: 7,
                rpc_kind: "ShardReadRange",
                error_code: "ResourceExhausted",
                message: "read handle limit exceeded for secret bucket",
            },
        );
        emit_shard_scavenger_observation(
            "storage",
            ShardScavengerObservationSummary {
                node_id: 1,
                data_pg_id: 2,
                shard_index: 3,
                shard_key_hex: "0000000000000000000000000000000000000000000000000000000000000000",
                reason: "scan_incomplete",
                file_exists: false,
                shard_row_exists: false,
                last_error: Some("scan failed"),
            },
        );
        {
            let _active = stream_upload_active_session_guard();
            emit_stream_upload_phase(
                &ctx,
                "server_http",
                StreamUploadPhaseSummary {
                    operation: "UploadPart",
                    phase: StreamUploadPhase::SessionCreated,
                    bucket: "bucket",
                    key: "key",
                    upload_id: Some("upload"),
                    part_number: Some(1),
                    session_id: Some("session"),
                    segment_index: None,
                    body_bytes_received: None,
                    segment_bytes: None,
                    segment_count: None,
                },
            );
            assert_eq!(
                metrics_snapshot().stream_upload_active_sessions,
                before.stream_upload_active_sessions + 1
            );
        }
        emit_stream_upload_phase(
            &ctx,
            "server_http",
            StreamUploadPhaseSummary {
                operation: "UploadPart",
                phase: StreamUploadPhase::BodyStarted,
                bucket: "bucket",
                key: "key",
                upload_id: Some("upload"),
                part_number: Some(1),
                session_id: Some("session"),
                segment_index: None,
                body_bytes_received: Some(8),
                segment_bytes: Some(8),
                segment_count: None,
            },
        );
        emit_stream_upload_phase(
            &ctx,
            "server_http",
            StreamUploadPhaseSummary {
                operation: "UploadPart",
                phase: StreamUploadPhase::SegmentAppendStarted,
                bucket: "bucket",
                key: "key",
                upload_id: Some("upload"),
                part_number: Some(1),
                session_id: Some("session"),
                segment_index: Some(0),
                body_bytes_received: Some(8),
                segment_bytes: Some(8),
                segment_count: None,
            },
        );
        emit_stream_upload_phase(
            &ctx,
            "server_http",
            StreamUploadPhaseSummary {
                operation: "UploadPart",
                phase: StreamUploadPhase::SegmentAppendFinished,
                bucket: "bucket",
                key: "key",
                upload_id: Some("upload"),
                part_number: Some(1),
                session_id: Some("session"),
                segment_index: Some(0),
                body_bytes_received: Some(8),
                segment_bytes: Some(8),
                segment_count: None,
            },
        );
        emit_stream_upload_phase(
            &ctx,
            "server_http",
            StreamUploadPhaseSummary {
                operation: "UploadPart",
                phase: StreamUploadPhase::BodyReadComplete,
                bucket: "bucket",
                key: "key",
                upload_id: Some("upload"),
                part_number: Some(1),
                session_id: Some("session"),
                segment_index: None,
                body_bytes_received: Some(8),
                segment_bytes: None,
                segment_count: Some(1),
            },
        );
        emit_stream_upload_phase(
            &ctx,
            "server_http",
            StreamUploadPhaseSummary {
                operation: "UploadPart",
                phase: StreamUploadPhase::FinalizeStarted,
                bucket: "bucket",
                key: "key",
                upload_id: Some("upload"),
                part_number: Some(1),
                session_id: Some("session"),
                segment_index: None,
                body_bytes_received: Some(8),
                segment_bytes: None,
                segment_count: Some(1),
            },
        );
        emit_stream_upload_phase(
            &ctx,
            "server_http",
            StreamUploadPhaseSummary {
                operation: "UploadPart",
                phase: StreamUploadPhase::SessionFinalized,
                bucket: "bucket",
                key: "key",
                upload_id: Some("upload"),
                part_number: Some(1),
                session_id: Some("session"),
                segment_index: None,
                body_bytes_received: Some(8),
                segment_bytes: None,
                segment_count: Some(1),
            },
        );

        let after = metrics_snapshot();
        assert_eq!(after.request_start_total, before.request_start_total + 1);
        assert_eq!(after.request_finish_total, before.request_finish_total + 1);
        assert_eq!(after.request_error_total, before.request_error_total + 1);
        assert_eq!(
            after.http_500_response_total,
            before.http_500_response_total
        );
        assert_eq!(
            after.operation_aborted_response_total,
            before.operation_aborted_response_total
        );
        assert_eq!(
            after.slow_down_response_total,
            before.slow_down_response_total
        );
        assert_eq!(after.slow_request_total, before.slow_request_total + 1);
        assert_eq!(
            after.request_admission_wait_total,
            before.request_admission_wait_total + 1
        );
        assert_eq!(
            after.request_admission_wait_us_total,
            before.request_admission_wait_us_total + 123
        );
        assert_eq!(
            after.request_admission_timeout_total,
            before.request_admission_timeout_total + 1
        );
        assert_eq!(
            after.bucket_lock_wait_exceeded_total,
            before.bucket_lock_wait_exceeded_total + 1
        );
        assert_eq!(
            after.metadata_command_conflict_total,
            before.metadata_command_conflict_total + 1
        );
        assert_eq!(
            after.metadata_command_pending_slot_action_total,
            before.metadata_command_pending_slot_action_total + 1
        );
        assert_eq!(
            after.metadata_command_session_wait_total,
            before.metadata_command_session_wait_total + 1
        );
        assert_eq!(
            after.metadata_command_recovery_leader_total,
            before.metadata_command_recovery_leader_total + 1
        );
        assert_eq!(
            after.metadata_command_recovery_wait_total,
            before.metadata_command_recovery_wait_total + 2
        );
        assert_eq!(
            after.metadata_command_recovery_wait_us_total,
            before.metadata_command_recovery_wait_us_total + 200
        );
        assert!(after.metadata_command_recovery_wait_us_max >= 101);
        assert_eq!(
            after.metadata_command_recovery_timeout_total,
            before.metadata_command_recovery_timeout_total + 1
        );
        assert_eq!(
            after.metadata_command_recovery_outcome_total,
            before.metadata_command_recovery_outcome_total + 1
        );
        assert_eq!(
            after.metadata_command_budget_exhausted_total,
            before.metadata_command_budget_exhausted_total + 1
        );
        assert_eq!(
            after.metadata_command_backoff_total,
            before.metadata_command_backoff_total + 1
        );
        assert_eq!(
            after.metadata_command_backoff_us_total,
            before.metadata_command_backoff_us_total + 12_345
        );
        assert!(after.metadata_command_backoff_us_max >= 12_345);
        assert!(metadata_command_budget_dimension_snapshot()
            .iter()
            .any(|sample| sample.pg_id == Some(11)
                && sample.operation == "bucket_delete_begin"
                && sample.context == "bucket delete begin metadata convergence budget exhausted"
                && sample.count >= 1
                && sample.elapsed_us_max >= 10_123_456
                && sample.budget_us_max >= 10_000_000));
        assert!(metadata_command_backoff_dimension_snapshot()
            .iter()
            .any(|sample| sample.pg_id == Some(11)
                && sample.operation == "bucket_delete_begin"
                && sample.context == "bucket delete drain bucket pg command"
                && sample.count >= 1
                && sample.sleep_us_total >= 12_345
                && sample.sleep_us_max >= 12_345));
        assert_eq!(after.reclaim_work_queue_depth, 3);
        assert_eq!(after.object_payload_reclaim_queue_depth, 2);
        assert_eq!(after.object_payload_reclaim_outstanding_depth, 2);
        assert_eq!(after.bucket_delete_begin_queue_depth, 1);
        assert_eq!(after.bucket_delete_finalize_queue_depth, 1);
        assert_eq!(after.bucket_delete_finalize_outstanding_depth, 1);
        assert_eq!(
            after.reclaim_work_queue_action_total,
            before.reclaim_work_queue_action_total + 1
        );
        assert_eq!(
            after.object_payload_reclaim_event_total,
            before.object_payload_reclaim_event_total + 1
        );
        assert_eq!(
            after.object_payload_reclaim_durable_scan_total,
            before.object_payload_reclaim_durable_scan_total + 1
        );
        assert!(reclaim_work_queue_action_dimension_snapshot()
            .iter()
            .any(|sample| sample.work_kind == "object_payload"
                && sample.action == "enqueue"
                && sample.count >= 1));
        assert!(object_payload_reclaim_event_dimension_snapshot()
            .iter()
            .any(|sample| sample.pg_id == 11
                && sample.event == "deferred_pending_command"
                && sample.count >= 1));
        assert!(object_payload_reclaim_durable_scan_dimension_snapshot()
            .iter()
            .any(|sample| sample.pg_id == 11 && sample.event == "queued" && sample.count >= 1));
        assert_eq!(after.shard_repair_queue_depth, 4);
        assert_eq!(
            after.shard_repair_event_total,
            before.shard_repair_event_total + 2
        );
        assert_eq!(
            after.shard_repair_shards_rewritten_total,
            before.shard_repair_shards_rewritten_total + 2
        );
        assert!(shard_repair_event_dimension_snapshot()
            .iter()
            .any(|sample| sample.pg_id == Some(11)
                && sample.event == "queued"
                && sample.count >= 1));
        assert!(shard_repair_event_dimension_snapshot()
            .iter()
            .any(|sample| sample.pg_id == Some(11)
                && sample.event == "repaired"
                && sample.count >= 1));
        assert_eq!(after.shard_backfill_queue_depth, 3);
        assert_eq!(
            after.shard_backfill_event_total,
            before.shard_backfill_event_total + 2
        );
        assert_eq!(
            after.shard_backfill_shards_written_total,
            before.shard_backfill_shards_written_total + 2
        );
        assert!(shard_backfill_event_dimension_snapshot()
            .iter()
            .any(|sample| sample.pg_id == Some(11)
                && sample.event == "claim_started"
                && sample.count >= 1));
        assert!(shard_backfill_event_dimension_snapshot()
            .iter()
            .any(|sample| sample.pg_id == Some(11)
                && sample.event == "backfilled"
                && sample.count >= 1));
        assert_eq!(
            after.shard_backfill_candidate_scan_total,
            before.shard_backfill_candidate_scan_total + 2
        );
        assert_eq!(
            after.shard_backfill_candidate_scanned_total,
            before.shard_backfill_candidate_scanned_total + 7
        );
        assert_eq!(
            after.shard_backfill_candidate_current_epoch_total,
            before.shard_backfill_candidate_current_epoch_total + 2
        );
        assert_eq!(
            after.shard_backfill_candidate_already_queued_total,
            before.shard_backfill_candidate_already_queued_total + 5
        );
        assert_eq!(
            after.shard_backfill_candidate_already_complete_total,
            before.shard_backfill_candidate_already_complete_total + 1
        );
        assert_eq!(
            after.shard_backfill_candidate_enqueued_total,
            before.shard_backfill_candidate_enqueued_total + 3
        );
        assert_eq!(
            after.shard_backfill_candidate_unrecoverable_total,
            before.shard_backfill_candidate_unrecoverable_total + 1
        );
        assert_eq!(
            after.shard_backfill_candidate_deferred_total,
            before.shard_backfill_candidate_deferred_total + 6
        );
        assert_eq!(
            after.shard_backfill_candidate_failed_total,
            before.shard_backfill_candidate_failed_total + 4
        );
        assert_eq!(
            after.shard_backfill_candidate_limit_reached_total,
            before.shard_backfill_candidate_limit_reached_total + 1
        );
        assert_eq!(
            after.shard_backfill_candidate_scan_error_total,
            before.shard_backfill_candidate_scan_error_total + 1
        );
        assert_eq!(
            after.metadata_command_checkpoint_record_scan_total,
            before.metadata_command_checkpoint_record_scan_total + 2
        );
        assert_eq!(
            after.metadata_command_checkpoint_record_scanned_total,
            before.metadata_command_checkpoint_record_scanned_total + 11
        );
        assert_eq!(
            after.metadata_command_checkpoint_record_recorded_total,
            before.metadata_command_checkpoint_record_recorded_total + 3
        );
        assert_eq!(
            after.metadata_command_checkpoint_record_already_current_total,
            before.metadata_command_checkpoint_record_already_current_total + 4
        );
        assert_eq!(
            after.metadata_command_checkpoint_record_skipped_cadence_total,
            before.metadata_command_checkpoint_record_skipped_cadence_total + 5
        );
        assert_eq!(
            after.metadata_command_checkpoint_record_skipped_inactive_total,
            before.metadata_command_checkpoint_record_skipped_inactive_total + 2
        );
        assert_eq!(
            after.metadata_command_checkpoint_record_skipped_empty_total,
            before.metadata_command_checkpoint_record_skipped_empty_total + 12
        );
        assert_eq!(
            after.metadata_command_checkpoint_record_skipped_stale_epoch_total,
            before.metadata_command_checkpoint_record_skipped_stale_epoch_total + 11
        );
        assert_eq!(
            after.metadata_command_checkpoint_record_compacted_total,
            before.metadata_command_checkpoint_record_compacted_total + 6
        );
        assert_eq!(
            after.metadata_command_checkpoint_record_compaction_deleted_entries_total,
            before.metadata_command_checkpoint_record_compaction_deleted_entries_total + 13
        );
        assert_eq!(
            after.metadata_command_checkpoint_record_compaction_noop_total,
            before.metadata_command_checkpoint_record_compaction_noop_total + 7
        );
        assert_eq!(
            after.metadata_command_checkpoint_record_compaction_no_checkpoint_total,
            before.metadata_command_checkpoint_record_compaction_no_checkpoint_total + 8
        );
        assert_eq!(
            after.metadata_command_checkpoint_record_compaction_pending_total,
            before.metadata_command_checkpoint_record_compaction_pending_total + 9
        );
        assert_eq!(
            after.metadata_command_checkpoint_record_compaction_failed_total,
            before.metadata_command_checkpoint_record_compaction_failed_total + 10
        );
        assert_eq!(
            after.metadata_command_checkpoint_record_failed_total,
            before.metadata_command_checkpoint_record_failed_total + 1
        );
        assert_eq!(
            after.metadata_command_checkpoint_record_limit_reached_total,
            before.metadata_command_checkpoint_record_limit_reached_total + 1
        );
        assert_eq!(
            after.metadata_command_checkpoint_record_scan_error_total,
            before.metadata_command_checkpoint_record_scan_error_total + 1
        );
        assert!(
            metadata_command_checkpoint_record_error_dimension_snapshot()
                .iter()
                .any(|sample| sample.pg_id == 11
                    && sample.outcome == "skipped_stale"
                    && sample.error_kind == "storage_rpc_unknown_pg"
                    && sample.count >= 1)
        );
        assert_eq!(
            after.background_work_admission_event_total,
            before.background_work_admission_event_total + 2
        );
        assert_eq!(after.background_work_active_total, 0);
        assert_eq!(
            after.background_work_finished_total,
            before.background_work_finished_total + 1
        );
        assert_eq!(
            after.background_work_elapsed_us_total,
            before.background_work_elapsed_us_total + 321
        );
        assert!(background_work_admission_dimension_snapshot()
            .iter()
            .any(|sample| sample.class == "known_damage_repair"
                && sample.event == "finished"
                && sample.count >= 1
                && sample.elapsed_us_total >= 321));
        assert_eq!(
            after.storage_rpc_error_total,
            before.storage_rpc_error_total + 1
        );
        assert_eq!(
            after.storage_rpc_admission_total,
            before.storage_rpc_admission_total + 1
        );
        assert_eq!(
            after.storage_rpc_admission_wait_total,
            before.storage_rpc_admission_wait_total + 1
        );
        assert_eq!(
            after.storage_rpc_admission_wait_us_total,
            before.storage_rpc_admission_wait_us_total + 456
        );
        assert_eq!(
            after.storage_rpc_admission_timeout_total,
            before.storage_rpc_admission_timeout_total + 1
        );
        assert_eq!(
            after.shard_scavenger_observation_total,
            before.shard_scavenger_observation_total + 1
        );
        assert_eq!(
            after.shard_scavenger_scan_incomplete_total,
            before.shard_scavenger_scan_incomplete_total + 1
        );
        assert_eq!(
            after.stream_upload_active_sessions,
            before.stream_upload_active_sessions
        );
        assert_eq!(
            after.stream_upload_session_created_total,
            before.stream_upload_session_created_total + 1
        );
        assert_eq!(
            after.stream_upload_body_started_total,
            before.stream_upload_body_started_total + 1
        );
        assert_eq!(
            after.stream_upload_body_read_complete_total,
            before.stream_upload_body_read_complete_total + 1
        );
        assert_eq!(
            after.stream_upload_segment_append_started_total,
            before.stream_upload_segment_append_started_total + 1
        );
        assert_eq!(
            after.stream_upload_segment_append_finished_total,
            before.stream_upload_segment_append_finished_total + 1
        );
        assert_eq!(
            after.stream_upload_finalize_started_total,
            before.stream_upload_finalize_started_total + 1
        );
        assert_eq!(
            after.stream_upload_session_finalized_total,
            before.stream_upload_session_finalized_total + 1
        );
    }

    #[test]
    fn request_error_metrics_classify_500_operation_aborted_and_slow_down() {
        let _guard = METRICS_TEST_MUTEX.lock().unwrap();
        let before = metrics_snapshot();
        let ctx = TraceContext::new_request();
        let mut summary = RequestSummary {
            method: "PUT",
            path: "/bucket/key",
            query: query_summary(""),
            status_code: 500,
            streaming: false,
            body_len: 0,
            bytes_sent: 0,
            lifetime_us: 7,
        };

        emit_request_error(
            &ctx,
            "server_http",
            summary,
            "response_body",
            "InternalError",
            "metadata_command_log_conflict",
        );
        summary.status_code = 409;
        emit_request_error(
            &ctx,
            "server_http",
            summary,
            "response_body",
            "OperationAborted",
            "operation_aborted",
        );
        summary.status_code = 503;
        emit_request_error(
            &ctx,
            "server_http",
            summary,
            "response_body",
            "SlowDown",
            "slow_down",
        );
        summary.status_code = 206;
        emit_request_error(
            &ctx,
            "server_http",
            summary,
            "response_body",
            "InternalError",
            "shard_store_storage_rpc_resource_exhausted",
        );
        emit_request_error(
            &ctx,
            "server_http",
            summary,
            "response_body",
            "SlowDown",
            "slow_down",
        );

        let after = metrics_snapshot();
        assert_eq!(after.request_error_total, before.request_error_total + 5);
        assert_eq!(
            after.http_500_response_total,
            before.http_500_response_total + 1
        );
        assert_eq!(
            after.operation_aborted_response_total,
            before.operation_aborted_response_total + 1
        );
        assert_eq!(
            after.slow_down_response_total,
            before.slow_down_response_total + 1
        );
    }

    #[test]
    fn request_error_records_redacted_bounded_flight_record() {
        let _guard = METRICS_TEST_MUTEX.lock().unwrap();
        let ctx = TraceContext::from_ids(TraceContextIds {
            trace_id: "trace-flight-redaction".to_string(),
            request_id: "request-flight-redaction".to_string(),
        });
        let summary = RequestSummary {
            method: "PUT",
            path: "/secret-bucket/secret-key",
            query: query_summary("X-Amz-Signature=secret&partNumber=1"),
            status_code: 500,
            streaming: false,
            body_len: 17,
            bytes_sent: 0,
            lifetime_us: 123,
        };

        emit_request_error(
            &ctx,
            "server_http",
            summary,
            "response_body",
            "InternalError",
            "metadata_command_contention",
        );
        emit_http_500_cause_chain(
            &ctx,
            "server_http",
            summary,
            "metadata_command_contention",
            "server_error>store_error>metadata_command_contention",
        );

        let records = flight_recorder_snapshot();
        let record = records
            .iter()
            .rev()
            .find(|record| {
                record.request_id == "request-flight-redaction" && record.event == "request_error"
            })
            .expect("request error should be recorded in flight recorder");
        assert_eq!(record.event, "request_error");
        assert!(record.detail.contains("path_hash="));
        assert!(record.detail.contains("sigv4_query=true"));
        assert!(record
            .detail
            .contains("cause_label=metadata_command_contention"));
        assert!(!record.detail.contains("secret-bucket"));
        assert!(!record.detail.contains("secret-key"));
        assert!(!record.detail.contains("secret"));
        assert!(record.detail.len() <= FLIGHT_RECORD_MAX_DETAIL_BYTES + 3);

        let cause_chain_record = records
            .iter()
            .rev()
            .find(|record| {
                record.request_id == "request-flight-redaction"
                    && record.event == "request_500_cause_chain"
            })
            .expect("500 cause chain should be recorded in flight recorder");
        assert!(cause_chain_record.detail.contains("path_hash="));
        assert!(cause_chain_record.detail.contains("sigv4_query=true"));
        assert!(cause_chain_record
            .detail
            .contains("cause_label=metadata_command_contention"));
        assert!(cause_chain_record
            .detail
            .contains("cause_chain=\"server_error>store_error>metadata_command_contention\""));
        assert!(!cause_chain_record.detail.contains("secret-bucket"));
        assert!(!cause_chain_record.detail.contains("secret-key"));
        assert!(!cause_chain_record.detail.contains("secret"));
        assert!(cause_chain_record.detail.len() <= FLIGHT_RECORD_MAX_DETAIL_BYTES + 3);
    }

    #[test]
    fn request_admission_records_redacted_bounded_flight_records() {
        let _guard = METRICS_TEST_MUTEX.lock().unwrap();
        let ctx = TraceContext::from_ids(TraceContextIds {
            trace_id: "trace-admission-redaction".to_string(),
            request_id: "request-admission-redaction".to_string(),
        });
        let wait_summary = RequestAdmissionWaitSummary {
            method: "PUT",
            path: "/secret-bucket/secret-key",
            query: query_summary("X-Amz-Signature=secret&partNumber=1"),
            wait_us: 42_000,
        };
        let timeout_summary = RequestAdmissionTimeoutSummary {
            method: "PUT",
            path: "/secret-bucket/secret-key",
            query: query_summary("X-Amz-Signature=secret&partNumber=1"),
            wait_us: 42_000,
            timeout_us: 5_000_000,
        };

        emit_request_admission_wait(&ctx, "server_http", wait_summary);
        emit_request_admission_timeout(&ctx, "server_http", timeout_summary);

        let records = flight_recorder_snapshot();
        for event in ["request_admission_wait", "request_admission_timeout"] {
            let record = records
                .iter()
                .rev()
                .find(|record| {
                    record.request_id == "request-admission-redaction" && record.event == event
                })
                .expect("admission event should be recorded in flight recorder");
            assert!(record.detail.contains("path_hash="));
            assert!(record.detail.contains("sigv4_query=true"));
            assert!(record.detail.contains("wait_us=42000"));
            assert!(!record.detail.contains("secret-bucket"));
            assert!(!record.detail.contains("secret-key"));
            assert!(!record.detail.contains("secret"));
            assert!(record.detail.len() <= FLIGHT_RECORD_MAX_DETAIL_BYTES + 3);
        }
    }

    #[test]
    fn metadata_command_diagnostics_record_bounded_pg_context() {
        let _guard = METRICS_TEST_MUTEX.lock().unwrap();
        let ctx = TraceContext::from_ids(TraceContextIds {
            trace_id: "trace-metadata-command".to_string(),
            request_id: "request-metadata-command".to_string(),
        });
        let _attached = AttachedTrace::new(ctx);
        let before = metrics_snapshot();

        emit_metadata_command_conflict(
            "storage",
            MetadataCommandConflictSummary {
                node_id: Some(7),
                pg_id: 3,
                cluster_epoch: 1,
                log_index: Some(9),
                kind: "pending_slot_conflict",
                command_kind: Some("CreateStreamUpload"),
            },
        );
        emit_metadata_command_pending_slot_action(
            "storage",
            MetadataCommandPendingSlotActionSummary {
                node_id: Some(7),
                pg_id: 3,
                cluster_epoch: 1,
                log_index: Some(10),
                action: "drain_attempt",
                command_kind: Some("CommitDirectPutObject"),
            },
        );
        emit_metadata_command_session_wait(
            "storage",
            MetadataCommandSessionWaitSummary {
                node_id: 7,
                pg_id: 3,
                wait_us: 1234,
            },
        );
        emit_metadata_command_recovery_admission(
            "storage",
            MetadataCommandRecoveryAdmissionSummary {
                node_id: Some(7),
                pg_id: 3,
                cluster_epoch: 1,
                log_index: Some(11),
                admission: "waited",
                command_kind: Some("ReserveObjectVersion"),
                wait_us: 4321,
            },
        );
        emit_metadata_command_recovery_outcome(
            "storage",
            MetadataCommandRecoveryOutcomeSummary {
                node_id: Some(7),
                pg_id: 3,
                cluster_epoch: 1,
                log_index: Some(11),
                outcome: "applied",
                command_kind: Some("ReserveObjectVersion"),
            },
        );

        let after = metrics_snapshot();
        assert_eq!(
            after.metadata_command_conflict_total,
            before.metadata_command_conflict_total + 1
        );
        assert_eq!(
            after.metadata_command_pending_slot_action_total,
            before.metadata_command_pending_slot_action_total + 1
        );
        assert_eq!(
            after.metadata_command_session_wait_total,
            before.metadata_command_session_wait_total + 1
        );
        assert_eq!(
            after.metadata_command_recovery_wait_total,
            before.metadata_command_recovery_wait_total + 1
        );
        assert_eq!(
            after.metadata_command_recovery_wait_us_total,
            before.metadata_command_recovery_wait_us_total + 4321
        );
        assert!(after.metadata_command_recovery_wait_us_max >= 4321);
        assert_eq!(
            after.metadata_command_recovery_outcome_total,
            before.metadata_command_recovery_outcome_total + 1
        );

        let records = flight_recorder_snapshot();
        let conflict = records
            .iter()
            .rev()
            .find(|record| {
                record.request_id == "request-metadata-command"
                    && record.event == "metadata_command_conflict"
            })
            .expect("metadata command conflict should be recorded");
        assert!(conflict.detail.contains("node_id=7"));
        assert!(conflict.detail.contains("pg_id=3"));
        assert!(conflict.detail.contains("log_index=9"));
        assert!(conflict.detail.contains("kind=pending_slot_conflict"));
        assert!(conflict.detail.contains("command_kind=CreateStreamUpload"));

        let pending_slot = records
            .iter()
            .rev()
            .find(|record| {
                record.request_id == "request-metadata-command"
                    && record.event == "metadata_command_pending_slot_action"
            })
            .expect("metadata command pending-slot action should be recorded");
        assert!(pending_slot.detail.contains("node_id=7"));
        assert!(pending_slot.detail.contains("pg_id=3"));
        assert!(pending_slot.detail.contains("log_index=10"));
        assert!(pending_slot.detail.contains("action=drain_attempt"));
        assert!(pending_slot
            .detail
            .contains("command_kind=CommitDirectPutObject"));

        let wait = records
            .iter()
            .rev()
            .find(|record| {
                record.request_id == "request-metadata-command"
                    && record.event == "metadata_command_session_wait"
            })
            .expect("metadata command session wait should be recorded");
        assert!(wait.detail.contains("node_id=7"));
        assert!(wait.detail.contains("pg_id=3"));
        assert!(wait.detail.contains("wait_us=1234"));

        let recovery_admission = records
            .iter()
            .rev()
            .find(|record| {
                record.request_id == "request-metadata-command"
                    && record.event == "metadata_command_recovery_admission"
            })
            .expect("metadata command recovery admission should be recorded");
        assert!(recovery_admission.detail.contains("node_id=7"));
        assert!(recovery_admission.detail.contains("pg_id=3"));
        assert!(recovery_admission.detail.contains("log_index=11"));
        assert!(recovery_admission.detail.contains("admission=waited"));
        assert!(recovery_admission
            .detail
            .contains("command_kind=ReserveObjectVersion"));
        assert!(recovery_admission.detail.contains("wait_us=4321"));

        let recovery_outcome = records
            .iter()
            .rev()
            .find(|record| {
                record.request_id == "request-metadata-command"
                    && record.event == "metadata_command_recovery_outcome"
            })
            .expect("metadata command recovery outcome should be recorded");
        assert!(recovery_outcome.detail.contains("node_id=7"));
        assert!(recovery_outcome.detail.contains("pg_id=3"));
        assert!(recovery_outcome.detail.contains("log_index=11"));
        assert!(recovery_outcome.detail.contains("outcome=applied"));
        assert!(recovery_outcome
            .detail
            .contains("command_kind=ReserveObjectVersion"));
    }

    #[test]
    fn storage_rpc_error_records_bounded_redacted_context() {
        let _guard = METRICS_TEST_MUTEX.lock().unwrap();
        let ctx = TraceContext::from_ids(TraceContextIds {
            trace_id: "trace-storage-rpc-error".to_string(),
            request_id: "request-storage-rpc-error".to_string(),
        });
        let _attached = AttachedTrace::new(ctx);
        let before = metrics_snapshot();

        emit_storage_rpc_error(
            "storage",
            StorageRpcErrorSummary {
                node_id: 7,
                rpc_kind: "ObjectStreamSegmentAppendPrepare",
                error_code: "PayloadDecode",
                message: "bad payload for /secret-bucket/secret-key",
            },
        );

        let after = metrics_snapshot();
        assert_eq!(
            after.storage_rpc_error_total,
            before.storage_rpc_error_total + 1
        );
        let records = flight_recorder_snapshot();
        let record = records
            .iter()
            .rev()
            .find(|record| {
                record.request_id == "request-storage-rpc-error"
                    && record.event == "storage_rpc_error"
            })
            .expect("storage RPC error should be recorded");
        assert!(record.detail.contains("node_id=7"));
        assert!(record
            .detail
            .contains("rpc_kind=ObjectStreamSegmentAppendPrepare"));
        assert!(record.detail.contains("error_code=PayloadDecode"));
        assert!(record.detail.contains("message_len="));
        assert!(record.detail.contains("message_hash="));
        assert!(!record.detail.contains("secret-bucket"));
        assert!(!record.detail.contains("secret-key"));
        assert!(record.detail.len() <= FLIGHT_RECORD_MAX_DETAIL_BYTES + 3);
    }

    #[test]
    fn flight_recorder_is_bounded() {
        let _guard = METRICS_TEST_MUTEX.lock().unwrap();
        let ctx = TraceContext::from_ids(TraceContextIds {
            trace_id: "trace-flight-bounds".to_string(),
            request_id: "request-flight-bounds".to_string(),
        });

        for index in 0..(FLIGHT_RECORDER_CAPACITY + 8) {
            record_flight_event(&ctx, "test", "bounded", format!("index={index}"));
        }

        let records = flight_recorder_snapshot();
        assert!(records.len() <= FLIGHT_RECORDER_CAPACITY);
        assert!(!records.iter().any(|record| record.detail == "index=0"));
        assert!(records
            .iter()
            .any(|record| record.detail == format!("index={}", FLIGHT_RECORDER_CAPACITY + 7)));
    }

    #[test]
    fn attached_trace_sets_current_context_even_when_tracing_is_disabled() {
        let ctx = TraceContext::from_ids(TraceContextIds {
            trace_id: "0123456789abcdef0123456789abcdef".to_string(),
            request_id: "2VG1X5NNMZ52HKC0".to_string(),
        });

        let guard = AttachedTrace::new(ctx.clone());
        assert_eq!(current_context(), Some(ctx));
        drop(guard);
        assert_eq!(current_context(), None);
    }
}
