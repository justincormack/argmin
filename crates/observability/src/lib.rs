use std::cell::RefCell;
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
        }
    }

    #[must_use]
    pub fn trace_id(&self) -> &str {
        &self.trace_id
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
static SLOW_REQUEST_TOTAL: AtomicU64 = AtomicU64::new(0);
static BUCKET_LOCK_WAIT_EXCEEDED_TOTAL: AtomicU64 = AtomicU64::new(0);
static MULTIPART_COMPLETION_BUCKET_LOCK_WAIT_EXCEEDED_TOTAL: AtomicU64 = AtomicU64::new(0);

const TRACE_FILE_QUEUE_CAPACITY: usize = 16_384;
const TRACE_FILE_IDLE_FLUSH_INTERVAL: Duration = Duration::from_millis(50);

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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub inflight_requests: u64,
    pub request_finish_total: u64,
    pub request_error_total: u64,
    pub slow_request_total: u64,
    pub bucket_lock_wait_exceeded_total: u64,
    pub multipart_completion_bucket_lock_wait_exceeded_total: u64,
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
        slow_request_total: SLOW_REQUEST_TOTAL.load(Ordering::Relaxed),
        bucket_lock_wait_exceeded_total: BUCKET_LOCK_WAIT_EXCEEDED_TOTAL.load(Ordering::Relaxed),
        multipart_completion_bucket_lock_wait_exceeded_total:
            MULTIPART_COMPLETION_BUCKET_LOCK_WAIT_EXCEEDED_TOTAL.load(Ordering::Relaxed),
    }
}

pub fn emit_request_finish(
    context: &TraceContext,
    target: &'static str,
    summary: RequestSummary<'_>,
    outcome: &'static str,
) -> bool {
    REQUEST_FINISH_TOTAL.fetch_add(1, Ordering::Relaxed);
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
) -> bool {
    REQUEST_ERROR_TOTAL.fetch_add(1, Ordering::Relaxed);
    event_in_context(
        context,
        target,
        "request_error",
        Some(format_args!(
            "status={} method={} path={:?} has_query={} query_params={} sigv4_query={} streaming={} body_len={} bytes_sent={} lifetime_us={} stage={} error_code={}",
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
            error_code
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

pub fn emit_multipart_completion_bucket_lock_wait_exceeded<T: fmt::Debug>(
    context: &TraceContext,
    target: &'static str,
    bucket: &T,
    stripe: usize,
    wait_us: u128,
) -> bool {
    MULTIPART_COMPLETION_BUCKET_LOCK_WAIT_EXCEEDED_TOTAL.fetch_add(1, Ordering::Relaxed);
    event_in_context(
        context,
        target,
        "multipart_completion_bucket_lock_wait_exceeded",
        Some(format_args!(
            "bucket={:?} stripe={} wait_us={}",
            bucket, stripe, wait_us
        )),
    )
}

pub fn configure(enabled: bool, filter: Option<&str>, file_path: Option<&str>) -> bool {
    TRACE_CONFIG_OVERRIDE
        .set(TraceConfig {
            enabled,
            filters: parse_filters(filter),
            file_path: normalize_file_path(file_path),
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
        if !trace_config().enabled {
            return Self { previous: None };
        }

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
        if !trace_config().enabled {
            return;
        }

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
        );
        emit_slow_request(&ctx, "server_http", summary, "error", Some("InternalError"));
        emit_bucket_lock_wait_exceeded(&ctx, "storage", &"bucket", 3, 1_500);
        emit_multipart_completion_bucket_lock_wait_exceeded(&ctx, "storage", &"bucket", 7, 2_500);

        let after = metrics_snapshot();
        assert_eq!(after.request_finish_total, before.request_finish_total + 1);
        assert_eq!(after.request_error_total, before.request_error_total + 1);
        assert_eq!(after.slow_request_total, before.slow_request_total + 1);
        assert_eq!(
            after.bucket_lock_wait_exceeded_total,
            before.bucket_lock_wait_exceeded_total + 1
        );
        assert_eq!(
            after.multipart_completion_bucket_lock_wait_exceeded_total,
            before.multipart_completion_bucket_lock_wait_exceeded_total + 1
        );
    }
}
