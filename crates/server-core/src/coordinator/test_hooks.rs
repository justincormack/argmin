use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::Duration;

use storage::{ProcessLocalRegistryKey, SessionId, StorageCluster};

use super::Coordinator;

#[derive(Default, Clone)]
pub(super) struct ReclamationTestHooks {
    pub(super) target: Option<(String, String)>,
    pub(super) target_reclaim_worker_registry_key: Option<ProcessLocalRegistryKey>,
    pub(super) probe_multipart_complete_auth_lookup: bool,
    pub(super) reclaim_worker_durable_scan_delay_override: Option<Duration>,
    pub(super) after_reclaim_worker_idle_return: Option<Arc<dyn Fn() + Send + Sync>>,
    pub(super) after_reclaim_work_dequeued: Option<Arc<dyn Fn() + Send + Sync>>,
    pub(super) before_reclaim_work_execute: Option<Arc<dyn Fn(Arc<StorageCluster>) + Send + Sync>>,
    pub(super) after_multipart_snapshot: Option<Arc<dyn Fn() + Send + Sync>>,
    pub(super) after_multipart_delete_metadata: Option<Arc<dyn Fn() + Send + Sync>>,
    pub(super) after_abort_multipart_bucket_summary: Option<Arc<dyn Fn() + Send + Sync>>,
    pub(super) after_abort_multipart_auth_lookup: Option<Arc<dyn Fn() + Send + Sync>>,
    pub(super) after_list_parts_authorized: Option<Arc<dyn Fn() + Send + Sync>>,
    pub(super) after_list_parts_storage_list: Option<Arc<dyn Fn() + Send + Sync>>,
    pub(super) before_multipart_complete_snapshot: Option<Arc<dyn Fn() + Send + Sync>>,
    pub(super) after_multipart_complete_pre_commit: Option<Arc<dyn Fn() + Send + Sync>>,
    pub(super) after_multipart_complete_commit: Option<Arc<dyn Fn() + Send + Sync>>,
    pub(super) after_object_read_snapshot: Option<Arc<dyn Fn() + Send + Sync>>,
    pub(super) after_upload_part_copy_stream_session: Option<Arc<dyn Fn() + Send + Sync>>,
    pub(super) after_object_segments_first_segment: Option<Arc<dyn Fn() + Send + Sync>>,
    pub(super) after_object_segments_delete_metadata: Option<Arc<dyn Fn() + Send + Sync>>,
}

pub(super) static RECLAMATION_TEST_HOOKS: OnceLock<Mutex<ReclamationTestHooks>> = OnceLock::new();
pub(super) static RECLAMATION_TEST_SERIAL: OnceLock<Mutex<()>> = OnceLock::new();
pub(super) static STORAGE_TEST_HOOK_SERIAL: OnceLock<Mutex<()>> = OnceLock::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct DeterministicFaultToken(&'static str);

impl DeterministicFaultToken {
    pub(super) const fn new(name: &'static str) -> Self {
        Self(name)
    }
}

#[derive(Debug, Default)]
struct DeterministicFaultGateState {
    arrived: bool,
    released: bool,
}

#[derive(Debug)]
pub(super) struct DeterministicFaultGate {
    token: DeterministicFaultToken,
    state: Mutex<DeterministicFaultGateState>,
    changed: Condvar,
}

pub(super) struct DeterministicFaultGateReleaseGuard {
    gate: Arc<DeterministicFaultGate>,
}

impl Drop for DeterministicFaultGateReleaseGuard {
    fn drop(&mut self) {
        self.gate.release();
    }
}

impl DeterministicFaultGate {
    pub(super) fn new(token: DeterministicFaultToken) -> Arc<Self> {
        Arc::new(Self {
            token,
            state: Mutex::new(DeterministicFaultGateState::default()),
            changed: Condvar::new(),
        })
    }

    pub(super) fn wait_at(&self, token: DeterministicFaultToken) {
        if token != self.token {
            return;
        }
        let mut state = self.state.lock().unwrap();
        state.arrived = true;
        self.changed.notify_all();
        while !state.released {
            state = self.changed.wait(state).unwrap();
        }
    }

    pub(super) fn wait_until_arrived(&self, timeout: Duration) {
        let state = self.state.lock().unwrap();
        let (state, _) = self
            .changed
            .wait_timeout_while(state, timeout, |state| !state.arrived)
            .unwrap();
        assert!(
            state.arrived,
            "timed out waiting for deterministic fault gate {:?}",
            self.token
        );
    }

    pub(super) fn release(&self) {
        let mut state = self.state.lock().unwrap();
        state.released = true;
        self.changed.notify_all();
    }

    pub(super) fn release_on_drop(self: &Arc<Self>) -> DeterministicFaultGateReleaseGuard {
        DeterministicFaultGateReleaseGuard {
            gate: Arc::clone(self),
        }
    }
}

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
    pub(super) after_bucket_mutation_storage_node_capture: Option<Arc<dyn Fn() + Send + Sync>>,
    pub(super) after_loaded: Option<Arc<dyn Fn() + Send + Sync>>,
    pub(super) after_object_metadata_policy_context: Option<Arc<dyn Fn() + Send + Sync>>,
    pub(super) after_multipart_create_commit: Option<Arc<dyn Fn() + Send + Sync>>,
    pub(super) probe_direct_put_commit: bool,
    pub(super) probe_begin_stream_part_session: bool,
    pub(super) probe_finalize_stream_part_commit: bool,
    pub(super) probe_finalize_stream_put_commit: bool,
    pub(super) probe_bucket_mutation_write: bool,
    pub(super) probe_object_read_snapshot: bool,
    pub(super) probe_object_metadata_access: bool,
    pub(super) probe_delete_object_lookup: bool,
}

pub(super) static BUCKET_WRITE_HANDLE_TEST_SERIAL: OnceLock<Mutex<()>> = OnceLock::new();

#[derive(Default, Clone)]
pub(super) struct ListObjectsTestHooks {
    pub(super) bucket: Option<String>,
    pub(super) before_storage_list: Option<Arc<dyn Fn() + Send + Sync>>,
}

pub(super) static LIST_OBJECTS_TEST_HOOKS: OnceLock<Mutex<ListObjectsTestHooks>> = OnceLock::new();
pub(super) static LIST_OBJECTS_TEST_SERIAL: OnceLock<Mutex<()>> = OnceLock::new();

#[derive(Default, Clone)]
pub(super) struct StreamAppendTestHooks {
    pub(super) target: Option<(String, u32)>,
    pub(super) after_prepare: Option<Arc<dyn Fn() + Send + Sync>>,
}

pub(super) static STREAM_APPEND_TEST_SERIAL: OnceLock<Mutex<()>> = OnceLock::new();

pub(super) struct ReclamationTestHookGuard {
    _storage_reclaim_guard: storage::StorageReclaimWorkerTestHookGuard,
}

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

pub(super) struct BucketWriteHandleTestHookGuard {
    hooks: Arc<Mutex<BucketWriteHandleTestHooks>>,
}

pub(super) struct ListObjectsTestHookGuard;

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
        *self.hooks.lock().unwrap() = BucketWriteHandleTestHooks::default();
    }
}

impl Drop for ListObjectsTestHookGuard {
    fn drop(&mut self) {
        let hooks =
            LIST_OBJECTS_TEST_HOOKS.get_or_init(|| Mutex::new(ListObjectsTestHooks::default()));
        *hooks.lock().unwrap() = ListObjectsTestHooks::default();
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
    let storage_reclaim_guard =
        storage::install_reclaim_worker_test_hooks(storage::StorageReclaimWorkerTestHooks {
            target_registry_key: hooks.target_reclaim_worker_registry_key,
            durable_scan_delay_override: hooks.reclaim_worker_durable_scan_delay_override,
            after_idle_return: hooks.after_reclaim_worker_idle_return.clone(),
            after_work_dequeued: hooks.after_reclaim_work_dequeued.clone(),
            before_work_execute: hooks.before_reclaim_work_execute.clone(),
        });
    let slot = RECLAMATION_TEST_HOOKS.get_or_init(|| Mutex::new(ReclamationTestHooks::default()));
    *slot.lock().unwrap() = hooks;
    ReclamationTestHookGuard {
        _storage_reclaim_guard: storage_reclaim_guard,
    }
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

impl Coordinator {
    pub(super) fn install_bucket_write_handle_test_hooks(
        &self,
        hooks: BucketWriteHandleTestHooks,
    ) -> BucketWriteHandleTestHookGuard {
        *self
            .shared_caches
            .bucket_write_handle_test_hooks
            .lock()
            .unwrap() = hooks;
        BucketWriteHandleTestHookGuard {
            hooks: Arc::clone(&self.shared_caches.bucket_write_handle_test_hooks),
        }
    }
}

pub(super) fn install_list_objects_test_hooks(
    hooks: ListObjectsTestHooks,
) -> ListObjectsTestHookGuard {
    let slot = LIST_OBJECTS_TEST_HOOKS.get_or_init(|| Mutex::new(ListObjectsTestHooks::default()));
    *slot.lock().unwrap() = hooks;
    ListObjectsTestHookGuard
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

pub(super) fn maybe_run_object_read_snapshot_hook(bucket: &str, key: &str) {
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
        if let Some(hook) = hooks.after_object_read_snapshot {
            hook();
        }
    }
}

pub(super) fn maybe_run_upload_part_copy_stream_session_hook(bucket: &str, key: &str) {
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
        if let Some(hook) = hooks.after_upload_part_copy_stream_session {
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

pub(super) fn maybe_run_abort_multipart_bucket_summary_hook(bucket: &str, key: &str) {
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
        if let Some(hook) = hooks.after_abort_multipart_bucket_summary {
            hook();
        }
    }
}

pub(super) fn maybe_run_abort_multipart_auth_lookup_hook(bucket: &str, key: &str) {
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
        if let Some(hook) = hooks.after_abort_multipart_auth_lookup {
            hook();
        }
    }
}

pub(super) fn maybe_run_list_parts_authorized_hook(bucket: &str, key: &str) {
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
        if let Some(hook) = hooks.after_list_parts_authorized {
            hook();
        }
    }
}

pub(super) fn maybe_run_list_parts_storage_list_hook(bucket: &str, key: &str) {
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
        if let Some(hook) = hooks.after_list_parts_storage_list {
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

pub(super) fn maybe_run_multipart_complete_snapshot_hook(bucket: &str, key: &str) {
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
        if let Some(hook) = hooks.before_multipart_complete_snapshot {
            hook();
        }
    }
}

pub(super) fn maybe_run_multipart_complete_commit_hook(bucket: &str, key: &str) {
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
        if let Some(hook) = hooks.after_multipart_complete_commit {
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

impl Coordinator {
    pub(super) fn maybe_run_bucket_write_handle_loaded_hook(&self, bucket: &str) {
        let hooks = self
            .shared_caches
            .bucket_write_handle_test_hooks
            .lock()
            .unwrap()
            .clone();
        if hooks.bucket.as_ref().is_some_and(|target| target == bucket) {
            if let Some(hook) = hooks.after_loaded {
                hook();
            }
        }
    }

    pub(super) fn maybe_run_multipart_create_committed_hook(&self, bucket: &str) {
        let hooks = self
            .shared_caches
            .bucket_write_handle_test_hooks
            .lock()
            .unwrap()
            .clone();
        if hooks.bucket.as_ref().is_some_and(|target| target == bucket) {
            if let Some(hook) = hooks.after_multipart_create_commit {
                hook();
            }
        }
    }

    pub(super) fn maybe_run_bucket_mutation_storage_node_capture_hook(&self, bucket: &str) {
        let hooks = self
            .shared_caches
            .bucket_write_handle_test_hooks
            .lock()
            .unwrap()
            .clone();
        if hooks.bucket.as_ref().is_some_and(|target| target == bucket) {
            if let Some(hook) = hooks.after_bucket_mutation_storage_node_capture {
                hook();
            }
        }
    }

    pub(super) fn maybe_run_object_metadata_policy_context_hook(&self, bucket: &str) {
        let hooks = self
            .shared_caches
            .bucket_write_handle_test_hooks
            .lock()
            .unwrap()
            .clone();
        if hooks.bucket.as_ref().is_some_and(|target| target == bucket) {
            if let Some(hook) = hooks.after_object_metadata_policy_context {
                hook();
            }
        }
    }
}

pub(super) fn maybe_run_list_objects_before_storage_hook(bucket: &str) {
    let hooks = LIST_OBJECTS_TEST_HOOKS
        .get_or_init(|| Mutex::new(ListObjectsTestHooks::default()))
        .lock()
        .unwrap()
        .clone();
    if hooks.bucket.as_ref().is_some_and(|target| target == bucket) {
        if let Some(hook) = hooks.before_storage_list {
            hook();
        }
    }
}

impl Coordinator {
    fn bucket_write_handle_test_hooks_for(&self, bucket: &str) -> BucketWriteHandleTestHooks {
        let hooks = self
            .shared_caches
            .bucket_write_handle_test_hooks
            .lock()
            .unwrap()
            .clone();
        if hooks.bucket.as_ref().is_some_and(|target| target == bucket) {
            hooks
        } else {
            BucketWriteHandleTestHooks::default()
        }
    }

    pub(super) fn should_probe_direct_put_commit(&self, bucket: &str) -> bool {
        self.bucket_write_handle_test_hooks_for(bucket)
            .probe_direct_put_commit
    }

    pub(super) fn should_probe_begin_stream_part_session(&self, bucket: &str) -> bool {
        self.bucket_write_handle_test_hooks_for(bucket)
            .probe_begin_stream_part_session
    }

    pub(super) fn should_probe_finalize_stream_put_commit(&self, bucket: &str) -> bool {
        self.bucket_write_handle_test_hooks_for(bucket)
            .probe_finalize_stream_put_commit
    }

    pub(super) fn should_probe_finalize_stream_part_commit(&self, bucket: &str) -> bool {
        self.bucket_write_handle_test_hooks_for(bucket)
            .probe_finalize_stream_part_commit
    }

    pub(super) fn should_probe_bucket_mutation_write(&self, bucket: &str) -> bool {
        self.bucket_write_handle_test_hooks_for(bucket)
            .probe_bucket_mutation_write
    }

    pub(super) fn should_probe_object_read_snapshot(&self, bucket: &str) -> bool {
        self.bucket_write_handle_test_hooks_for(bucket)
            .probe_object_read_snapshot
    }

    pub(super) fn should_probe_object_metadata_access(&self, bucket: &str) -> bool {
        self.bucket_write_handle_test_hooks_for(bucket)
            .probe_object_metadata_access
    }

    pub(super) fn should_probe_delete_object_lookup(&self, bucket: &str) -> bool {
        self.bucket_write_handle_test_hooks_for(bucket)
            .probe_delete_object_lookup
    }
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
