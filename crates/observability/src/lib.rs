use std::cell::RefCell;
use std::collections::VecDeque;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, TrySendError};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
#[cfg(feature = "deep-tracing")]
use std::time::Instant;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceContext {
    trace_id: Arc<str>,
    request_id: Arc<str>,
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
    pub fn from_ids(trace_id: String, request_id: String) -> Self {
        Self {
            trace_id: Arc::<str>::from(trace_id),
            request_id: Arc::<str>::from(request_id),
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
static INFLIGHT_REQUESTS: AtomicU64 = AtomicU64::new(0);
static REQUEST_FINISH_TOTAL: AtomicU64 = AtomicU64::new(0);
static REQUEST_ERROR_TOTAL: AtomicU64 = AtomicU64::new(0);
static HTTP_500_RESPONSE_TOTAL: AtomicU64 = AtomicU64::new(0);
static OPERATION_ABORTED_RESPONSE_TOTAL: AtomicU64 = AtomicU64::new(0);
static SLOW_DOWN_RESPONSE_TOTAL: AtomicU64 = AtomicU64::new(0);
static SLOW_REQUEST_TOTAL: AtomicU64 = AtomicU64::new(0);
static STORAGE_RPC_ERROR_TOTAL: AtomicU64 = AtomicU64::new(0);
static BUCKET_LOCK_WAIT_EXCEEDED_TOTAL: AtomicU64 = AtomicU64::new(0);
static SHARD_SCAVENGER_OBSERVATION_TOTAL: AtomicU64 = AtomicU64::new(0);
static SHARD_SCAVENGER_SCAN_INCOMPLETE_TOTAL: AtomicU64 = AtomicU64::new(0);
static METADATA_COMMAND_CONFLICT_TOTAL: AtomicU64 = AtomicU64::new(0);
static METADATA_COMMAND_PENDING_SLOT_ACTION_TOTAL: AtomicU64 = AtomicU64::new(0);
static METADATA_COMMAND_SESSION_WAIT_TOTAL: AtomicU64 = AtomicU64::new(0);
static FLIGHT_RECORDER: OnceLock<Mutex<FlightRecorder>> = OnceLock::new();
static FLIGHT_RECORD_SEQUENCE: AtomicU64 = AtomicU64::new(1);

const TRACE_FILE_QUEUE_CAPACITY: usize = 16_384;
const TRACE_FILE_IDLE_FLUSH_INTERVAL: Duration = Duration::from_millis(50);
const FLIGHT_RECORDER_CAPACITY: usize = 512;
const FLIGHT_RECORD_MAX_DETAIL_BYTES: usize = 1_024;

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
pub struct StorageRpcErrorSummary<'a> {
    pub node_id: u32,
    pub rpc_kind: &'a str,
    pub error_code: &'a str,
    pub message: &'a str,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub inflight_requests: u64,
    pub request_finish_total: u64,
    pub request_error_total: u64,
    pub http_500_response_total: u64,
    pub operation_aborted_response_total: u64,
    pub slow_down_response_total: u64,
    pub slow_request_total: u64,
    pub storage_rpc_error_total: u64,
    pub bucket_lock_wait_exceeded_total: u64,
    pub shard_scavenger_observation_total: u64,
    pub shard_scavenger_scan_incomplete_total: u64,
    pub metadata_command_conflict_total: u64,
    pub metadata_command_pending_slot_action_total: u64,
    pub metadata_command_session_wait_total: u64,
}

pub struct InflightRequestsGuard {
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

#[must_use]
pub fn inflight_requests_guard() -> InflightRequestsGuard {
    INFLIGHT_REQUESTS.fetch_add(1, Ordering::Relaxed);
    InflightRequestsGuard { active: true }
}

#[must_use]
pub fn metrics_snapshot() -> MetricsSnapshot {
    MetricsSnapshot {
        inflight_requests: INFLIGHT_REQUESTS.load(Ordering::Relaxed),
        request_finish_total: REQUEST_FINISH_TOTAL.load(Ordering::Relaxed),
        request_error_total: REQUEST_ERROR_TOTAL.load(Ordering::Relaxed),
        http_500_response_total: HTTP_500_RESPONSE_TOTAL.load(Ordering::Relaxed),
        operation_aborted_response_total: OPERATION_ABORTED_RESPONSE_TOTAL.load(Ordering::Relaxed),
        slow_down_response_total: SLOW_DOWN_RESPONSE_TOTAL.load(Ordering::Relaxed),
        slow_request_total: SLOW_REQUEST_TOTAL.load(Ordering::Relaxed),
        storage_rpc_error_total: STORAGE_RPC_ERROR_TOTAL.load(Ordering::Relaxed),
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
    }
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
        emit_metadata_command_session_wait(
            "storage",
            MetadataCommandSessionWaitSummary {
                node_id: 7,
                pg_id: 11,
                wait_us: 55,
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

        let after = metrics_snapshot();
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
            after.bucket_lock_wait_exceeded_total,
            before.bucket_lock_wait_exceeded_total + 1
        );
        assert_eq!(
            after.metadata_command_conflict_total,
            before.metadata_command_conflict_total + 1
        );
        assert_eq!(
            after.metadata_command_session_wait_total,
            before.metadata_command_session_wait_total + 1
        );
        assert_eq!(
            after.storage_rpc_error_total,
            before.storage_rpc_error_total + 1
        );
        assert_eq!(
            after.shard_scavenger_observation_total,
            before.shard_scavenger_observation_total + 1
        );
        assert_eq!(
            after.shard_scavenger_scan_incomplete_total,
            before.shard_scavenger_scan_incomplete_total + 1
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
        let ctx = TraceContext::from_ids(
            "trace-flight-redaction".to_string(),
            "request-flight-redaction".to_string(),
        );
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

        let records = flight_recorder_snapshot();
        let record = records
            .iter()
            .rev()
            .find(|record| record.request_id == "request-flight-redaction")
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
    }

    #[test]
    fn metadata_command_diagnostics_record_bounded_pg_context() {
        let _guard = METRICS_TEST_MUTEX.lock().unwrap();
        let ctx = TraceContext::from_ids(
            "trace-metadata-command".to_string(),
            "request-metadata-command".to_string(),
        );
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
    }

    #[test]
    fn storage_rpc_error_records_bounded_redacted_context() {
        let _guard = METRICS_TEST_MUTEX.lock().unwrap();
        let ctx = TraceContext::from_ids(
            "trace-storage-rpc-error".to_string(),
            "request-storage-rpc-error".to_string(),
        );
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
        let ctx = TraceContext::from_ids(
            "trace-flight-bounds".to_string(),
            "request-flight-bounds".to_string(),
        );

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
        let ctx = TraceContext::from_ids(
            "0123456789abcdef0123456789abcdef".to_string(),
            "2VG1X5NNMZ52HKC0".to_string(),
        );

        let guard = AttachedTrace::new(ctx.clone());
        assert_eq!(current_context(), Some(ctx));
        drop(guard);
        assert_eq!(current_context(), None);
    }
}
