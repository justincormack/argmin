use std::sync::{Arc, Mutex, OnceLock};

use storage::SessionId;

use super::Coordinator;

#[derive(Default, Clone)]
pub(super) struct ReclamationTestHooks {
    pub(super) target: Option<(String, String)>,
    pub(super) probe_multipart_complete_auth_lookup: bool,
    pub(super) after_multipart_snapshot: Option<Arc<dyn Fn() + Send + Sync>>,
    pub(super) after_multipart_delete_metadata: Option<Arc<dyn Fn() + Send + Sync>>,
    pub(super) after_multipart_complete_pre_commit: Option<Arc<dyn Fn() + Send + Sync>>,
    pub(super) after_object_segments_first_segment: Option<Arc<dyn Fn() + Send + Sync>>,
    pub(super) after_object_segments_delete_metadata: Option<Arc<dyn Fn() + Send + Sync>>,
}

pub(super) static RECLAMATION_TEST_HOOKS: OnceLock<Mutex<ReclamationTestHooks>> = OnceLock::new();
pub(super) static RECLAMATION_TEST_SERIAL: OnceLock<Mutex<()>> = OnceLock::new();
pub(super) static STORAGE_TEST_HOOK_SERIAL: OnceLock<Mutex<()>> = OnceLock::new();

#[derive(Default, Clone)]
pub(super) struct BucketPolicyLoadTestHooks {
    pub(super) bucket: Option<String>,
    pub(super) before_storage_load: Option<Arc<dyn Fn() + Send + Sync>>,
    pub(super) after_policy_fast_path_hit: Option<Arc<dyn Fn() + Send + Sync>>,
}

pub(super) static BUCKET_POLICY_LOAD_TEST_HOOKS: OnceLock<Mutex<BucketPolicyLoadTestHooks>> =
    OnceLock::new();
pub(super) static BUCKET_POLICY_LOAD_TEST_SERIAL: OnceLock<Mutex<()>> = OnceLock::new();
pub(super) static BUCKET_FAST_PATH_IDENTITY_LOAD_ERROR_TEST_BUCKET: OnceLock<
    Mutex<Option<String>>,
> = OnceLock::new();

#[derive(Default, Clone)]
pub(super) struct BucketWriteHandleTestHooks {
    pub(super) bucket: Option<String>,
    pub(super) after_loaded: Option<Arc<dyn Fn() + Send + Sync>>,
    pub(super) probe_direct_put_commit: bool,
    pub(super) probe_begin_stream_part_session: bool,
    pub(super) probe_finalize_stream_part_commit: bool,
    pub(super) probe_finalize_stream_put_commit: bool,
    pub(super) probe_bucket_mutation_write: bool,
    pub(super) probe_object_read_snapshot: bool,
    pub(super) probe_object_metadata_access: bool,
    pub(super) probe_delete_object_lookup: bool,
}

pub(super) static BUCKET_WRITE_HANDLE_TEST_HOOKS: OnceLock<Mutex<BucketWriteHandleTestHooks>> =
    OnceLock::new();
pub(super) static BUCKET_WRITE_HANDLE_TEST_SERIAL: OnceLock<Mutex<()>> = OnceLock::new();

#[derive(Default, Clone)]
pub(super) struct StreamAppendTestHooks {
    pub(super) target: Option<(String, u32)>,
    pub(super) after_prepare: Option<Arc<dyn Fn() + Send + Sync>>,
}

pub(super) static STREAM_APPEND_TEST_SERIAL: OnceLock<Mutex<()>> = OnceLock::new();

pub(super) struct ReclamationTestHookGuard;

impl Drop for ReclamationTestHookGuard {
    fn drop(&mut self) {
        let hooks =
            RECLAMATION_TEST_HOOKS.get_or_init(|| Mutex::new(ReclamationTestHooks::default()));
        *hooks.lock().unwrap() = ReclamationTestHooks::default();
    }
}

pub(super) struct StreamAppendTestHookGuard {
    hooks: Arc<Mutex<StreamAppendTestHooks>>,
}

pub(super) struct BucketPolicyLoadTestHookGuard;

pub(super) struct BucketFastPathIdentityLoadErrorTestHookGuard;

pub(super) struct BucketWriteHandleTestHookGuard;

impl Drop for StreamAppendTestHookGuard {
    fn drop(&mut self) {
        *self.hooks.lock().unwrap() = StreamAppendTestHooks::default();
    }
}

impl Drop for BucketPolicyLoadTestHookGuard {
    fn drop(&mut self) {
        let hooks = BUCKET_POLICY_LOAD_TEST_HOOKS
            .get_or_init(|| Mutex::new(BucketPolicyLoadTestHooks::default()));
        *hooks.lock().unwrap() = BucketPolicyLoadTestHooks::default();
    }
}

impl Drop for BucketWriteHandleTestHookGuard {
    fn drop(&mut self) {
        let hooks = BUCKET_WRITE_HANDLE_TEST_HOOKS
            .get_or_init(|| Mutex::new(BucketWriteHandleTestHooks::default()));
        *hooks.lock().unwrap() = BucketWriteHandleTestHooks::default();
    }
}

impl Drop for BucketFastPathIdentityLoadErrorTestHookGuard {
    fn drop(&mut self) {
        let bucket =
            BUCKET_FAST_PATH_IDENTITY_LOAD_ERROR_TEST_BUCKET.get_or_init(|| Mutex::new(None));
        *bucket.lock().unwrap() = None;
    }
}

pub(super) fn install_reclamation_test_hooks(
    hooks: ReclamationTestHooks,
) -> ReclamationTestHookGuard {
    let slot = RECLAMATION_TEST_HOOKS.get_or_init(|| Mutex::new(ReclamationTestHooks::default()));
    *slot.lock().unwrap() = hooks;
    ReclamationTestHookGuard
}

impl Coordinator {
    pub(super) fn install_stream_append_test_hooks(
        &self,
        hooks: StreamAppendTestHooks,
    ) -> StreamAppendTestHookGuard {
        *self.shared_caches.stream_append_test_hooks.lock().unwrap() = hooks;
        StreamAppendTestHookGuard {
            hooks: Arc::clone(&self.shared_caches.stream_append_test_hooks),
        }
    }
}

pub(super) fn install_bucket_policy_load_test_hooks(
    hooks: BucketPolicyLoadTestHooks,
) -> BucketPolicyLoadTestHookGuard {
    let slot = BUCKET_POLICY_LOAD_TEST_HOOKS
        .get_or_init(|| Mutex::new(BucketPolicyLoadTestHooks::default()));
    *slot.lock().unwrap() = hooks;
    BucketPolicyLoadTestHookGuard
}

pub(super) fn install_bucket_fast_path_identity_load_error_test_hook(
    bucket: String,
) -> BucketFastPathIdentityLoadErrorTestHookGuard {
    let slot = BUCKET_FAST_PATH_IDENTITY_LOAD_ERROR_TEST_BUCKET.get_or_init(|| Mutex::new(None));
    *slot.lock().unwrap() = Some(bucket);
    BucketFastPathIdentityLoadErrorTestHookGuard
}

pub(super) fn install_bucket_write_handle_test_hooks(
    hooks: BucketWriteHandleTestHooks,
) -> BucketWriteHandleTestHookGuard {
    let slot = BUCKET_WRITE_HANDLE_TEST_HOOKS
        .get_or_init(|| Mutex::new(BucketWriteHandleTestHooks::default()));
    *slot.lock().unwrap() = hooks;
    BucketWriteHandleTestHookGuard
}

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

impl Coordinator {
    pub(super) fn maybe_run_stream_append_prepare_hook(
        &self,
        session_id: &SessionId,
        segment_index: u32,
    ) {
        let hooks = self
            .shared_caches
            .stream_append_test_hooks
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
}

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

pub(super) fn maybe_run_bucket_policy_storage_load_hook(bucket: &str) {
    let hooks = BUCKET_POLICY_LOAD_TEST_HOOKS
        .get_or_init(|| Mutex::new(BucketPolicyLoadTestHooks::default()))
        .lock()
        .unwrap()
        .clone();
    if hooks.bucket.as_ref().is_some_and(|target| target == bucket) {
        if let Some(hook) = hooks.before_storage_load {
            hook();
        }
    }
}

pub(super) fn maybe_run_bucket_policy_fast_path_hook(bucket: &str) {
    let hooks = BUCKET_POLICY_LOAD_TEST_HOOKS
        .get_or_init(|| Mutex::new(BucketPolicyLoadTestHooks::default()))
        .lock()
        .unwrap()
        .clone();
    if hooks.bucket.as_ref().is_some_and(|target| target == bucket) {
        if let Some(hook) = hooks.after_policy_fast_path_hit {
            hook();
        }
    }
}

pub(super) fn should_fail_bucket_fast_path_identity_load(bucket: &str) -> bool {
    BUCKET_FAST_PATH_IDENTITY_LOAD_ERROR_TEST_BUCKET
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap()
        .as_ref()
        .is_some_and(|target| target == bucket)
}

pub(super) fn maybe_run_bucket_write_handle_loaded_hook(bucket: &str) {
    let hooks = BUCKET_WRITE_HANDLE_TEST_HOOKS
        .get_or_init(|| Mutex::new(BucketWriteHandleTestHooks::default()))
        .lock()
        .unwrap()
        .clone();
    if hooks.bucket.as_ref().is_some_and(|target| target == bucket) {
        if let Some(hook) = hooks.after_loaded {
            hook();
        }
    }
}

fn bucket_write_handle_test_hooks_for(bucket: &str) -> BucketWriteHandleTestHooks {
    let hooks = BUCKET_WRITE_HANDLE_TEST_HOOKS
        .get_or_init(|| Mutex::new(BucketWriteHandleTestHooks::default()))
        .lock()
        .unwrap()
        .clone();
    if hooks.bucket.as_ref().is_some_and(|target| target == bucket) {
        hooks
    } else {
        BucketWriteHandleTestHooks::default()
    }
}

pub(super) fn should_probe_direct_put_commit(bucket: &str) -> bool {
    bucket_write_handle_test_hooks_for(bucket).probe_direct_put_commit
}

pub(super) fn should_probe_begin_stream_part_session(bucket: &str) -> bool {
    bucket_write_handle_test_hooks_for(bucket).probe_begin_stream_part_session
}

pub(super) fn should_probe_finalize_stream_put_commit(bucket: &str) -> bool {
    bucket_write_handle_test_hooks_for(bucket).probe_finalize_stream_put_commit
}

pub(super) fn should_probe_finalize_stream_part_commit(bucket: &str) -> bool {
    bucket_write_handle_test_hooks_for(bucket).probe_finalize_stream_part_commit
}

pub(super) fn should_probe_bucket_mutation_write(bucket: &str) -> bool {
    bucket_write_handle_test_hooks_for(bucket).probe_bucket_mutation_write
}

pub(super) fn should_probe_object_read_snapshot(bucket: &str) -> bool {
    bucket_write_handle_test_hooks_for(bucket).probe_object_read_snapshot
}

pub(super) fn should_probe_object_metadata_access(bucket: &str) -> bool {
    bucket_write_handle_test_hooks_for(bucket).probe_object_metadata_access
}

pub(super) fn should_probe_delete_object_lookup(bucket: &str) -> bool {
    bucket_write_handle_test_hooks_for(bucket).probe_delete_object_lookup
}

pub(super) fn should_probe_multipart_complete_auth_lookup(bucket: &str, key: &str) -> bool {
    let hooks = RECLAMATION_TEST_HOOKS
        .get_or_init(|| Mutex::new(ReclamationTestHooks::default()))
        .lock()
        .unwrap()
        .clone();
    hooks.probe_multipart_complete_auth_lookup
        && hooks
            .target
            .as_ref()
            .is_some_and(|(b, k)| b == bucket && k == key)
}
