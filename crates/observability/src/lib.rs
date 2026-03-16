use std::cell::RefCell;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

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
    File(Mutex<File>),
}

static TRACE_SINK: OnceLock<TraceSink> = OnceLock::new();
static TRACE_SINK_OVERRIDE: OnceLock<TraceSink> = OnceLock::new();

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
        Ok(file) => TraceSink::File(Mutex::new(file)),
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
        TraceSink::Stderr => {
            let mut stderr = io::stderr().lock();
            let _ = writeln!(stderr, "{args}");
        }
        TraceSink::File(file) => {
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

pub struct TraceScope {
    target: &'static str,
    name: &'static str,
    start: Instant,
    entered_depth: usize,
    context: Option<TraceContext>,
}

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
