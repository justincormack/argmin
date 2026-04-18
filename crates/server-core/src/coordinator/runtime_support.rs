use super::*;

#[cfg(test)]
#[derive(Default, Clone)]
pub(super) struct ReclamationTestHooks {
    pub(super) target: Option<(String, String)>,
    pub(super) after_multipart_snapshot: Option<Arc<dyn Fn() + Send + Sync>>,
    pub(super) after_multipart_delete_metadata: Option<Arc<dyn Fn() + Send + Sync>>,
    pub(super) after_multipart_complete_pre_commit: Option<Arc<dyn Fn() + Send + Sync>>,
    pub(super) after_object_segments_first_segment: Option<Arc<dyn Fn() + Send + Sync>>,
    pub(super) after_object_segments_delete_metadata: Option<Arc<dyn Fn() + Send + Sync>>,
}

#[cfg(test)]
pub(super) static RECLAMATION_TEST_HOOKS: OnceLock<Mutex<ReclamationTestHooks>> = OnceLock::new();
#[cfg(test)]
pub(super) static RECLAMATION_TEST_SERIAL: OnceLock<Mutex<()>> = OnceLock::new();

#[cfg(test)]
#[derive(Default, Clone)]
pub(super) struct StreamAppendTestHooks {
    pub(super) target: Option<(String, u32)>,
    pub(super) after_prepare: Option<Arc<dyn Fn() + Send + Sync>>,
}

#[cfg(test)]
pub(super) static STREAM_APPEND_TEST_HOOKS: OnceLock<Mutex<StreamAppendTestHooks>> =
    OnceLock::new();
#[cfg(test)]
pub(super) static STREAM_APPEND_TEST_SERIAL: OnceLock<Mutex<()>> = OnceLock::new();

pub(super) static LIFECYCLE_SWEEPER_REGISTRY: OnceLock<
    Mutex<HashMap<usize, Weak<LifecycleSweeper>>>,
> = OnceLock::new();

#[cfg(test)]
pub(super) struct ReclamationTestHookGuard;

#[cfg(test)]
impl Drop for ReclamationTestHookGuard {
    fn drop(&mut self) {
        let hooks =
            RECLAMATION_TEST_HOOKS.get_or_init(|| Mutex::new(ReclamationTestHooks::default()));
        *hooks.lock().unwrap() = ReclamationTestHooks::default();
    }
}

#[cfg(test)]
pub(super) struct StreamAppendTestHookGuard;

#[cfg(test)]
impl Drop for StreamAppendTestHookGuard {
    fn drop(&mut self) {
        let hooks =
            STREAM_APPEND_TEST_HOOKS.get_or_init(|| Mutex::new(StreamAppendTestHooks::default()));
        *hooks.lock().unwrap() = StreamAppendTestHooks::default();
    }
}

#[cfg(test)]
pub(super) fn install_reclamation_test_hooks(
    hooks: ReclamationTestHooks,
) -> ReclamationTestHookGuard {
    let slot = RECLAMATION_TEST_HOOKS.get_or_init(|| Mutex::new(ReclamationTestHooks::default()));
    *slot.lock().unwrap() = hooks;
    ReclamationTestHookGuard
}

#[cfg(test)]
pub(super) fn install_stream_append_test_hooks(
    hooks: StreamAppendTestHooks,
) -> StreamAppendTestHookGuard {
    let slot =
        STREAM_APPEND_TEST_HOOKS.get_or_init(|| Mutex::new(StreamAppendTestHooks::default()));
    *slot.lock().unwrap() = hooks;
    StreamAppendTestHookGuard
}

#[cfg(test)]
pub(super) fn maybe_run_multipart_snapshot_hook(bucket: &str, key: &str) {
    let hooks = RECLAMATION_TEST_HOOKS
        .get_or_init(|| Mutex::new(ReclamationTestHooks::default()))
        .lock()
        .unwrap()
        .clone();
    if hooks
        .target
        .as_ref()
        .is_some_and(|(b, k)| b == bucket && k == key)
    {
        if let Some(hook) = hooks.after_multipart_snapshot {
            hook();
        }
    }
}

#[cfg(test)]
pub(super) fn maybe_run_multipart_delete_metadata_hook(bucket: &str, key: &str) {
    let hooks = RECLAMATION_TEST_HOOKS
        .get_or_init(|| Mutex::new(ReclamationTestHooks::default()))
        .lock()
        .unwrap()
        .clone();
    if hooks
        .target
        .as_ref()
        .is_some_and(|(b, k)| b == bucket && k == key)
    {
        if let Some(hook) = hooks.after_multipart_delete_metadata {
            hook();
        }
    }
}

#[cfg(test)]
pub(super) fn maybe_run_multipart_complete_pre_commit_hook(bucket: &str, key: &str) {
    let hooks = RECLAMATION_TEST_HOOKS
        .get_or_init(|| Mutex::new(ReclamationTestHooks::default()))
        .lock()
        .unwrap()
        .clone();
    if hooks
        .target
        .as_ref()
        .is_some_and(|(b, k)| b == bucket && k == key)
    {
        if let Some(hook) = hooks.after_multipart_complete_pre_commit {
            hook();
        }
    }
}

#[cfg(test)]
pub(super) fn maybe_run_stream_append_prepare_hook(session_id: &SessionId, segment_index: u32) {
    let hooks = STREAM_APPEND_TEST_HOOKS
        .get_or_init(|| Mutex::new(StreamAppendTestHooks::default()))
        .lock()
        .unwrap()
        .clone();
    if hooks
        .target
        .as_ref()
        .is_some_and(|(session, index)| session == session_id && *index == segment_index)
    {
        if let Some(hook) = hooks.after_prepare {
            hook();
        }
    }
}

#[cfg(test)]
pub(super) fn maybe_run_object_segments_first_segment_hook(bucket: &str, key: &str) {
    let hooks = RECLAMATION_TEST_HOOKS
        .get_or_init(|| Mutex::new(ReclamationTestHooks::default()))
        .lock()
        .unwrap()
        .clone();
    if hooks
        .target
        .as_ref()
        .is_some_and(|(b, k)| b == bucket && k == key)
    {
        if let Some(hook) = hooks.after_object_segments_first_segment {
            hook();
        }
    }
}

#[cfg(test)]
pub(super) fn maybe_run_object_segments_delete_metadata_hook(bucket: &str, key: &str) {
    let hooks = RECLAMATION_TEST_HOOKS
        .get_or_init(|| Mutex::new(ReclamationTestHooks::default()))
        .lock()
        .unwrap()
        .clone();
    if hooks
        .target
        .as_ref()
        .is_some_and(|(b, k)| b == bucket && k == key)
    {
        if let Some(hook) = hooks.after_object_segments_delete_metadata {
            hook();
        }
    }
}
