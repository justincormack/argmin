// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::test_helpers;
use super::test_support::{setup_coordinator_without_reclaim_sweeper, NO_WRITE};
use super::test_topology::*;
use super::*;
use crate::conditional::{ReadCondition, SpecificEtag};
use crate::metadata_blob::MetadataBlob;
use crate::sse::ManagedWrappingKeyConfig;
use crate::system_metadata::SystemMetadata;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use storage::test_support::{
    StorageClusterLifecycleTestSupport as _, StorageClusterPayloadTestSupport as _,
};
use storage::test_support::{TestMultipartPartPayloadSnapshot, TestStreamUploadPayloadSnapshot};

const NO_READ: &ReadCondition = &ReadCondition {
    if_match: None,
    if_none_match: None,
    if_modified_since: None,
    if_unmodified_since: None,
};
const NO_PUT_OBJECT_ACL: PutObjectAcl<'static> = PutObjectAcl::None;
const TEST_SSE_S3_WRAPPING_KEY_B64: &str = "YWJjZGVmMDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODk=";
const MULTIPART_COMPLETE_RACE_TIMEOUT: Duration = Duration::from_secs(2);
const MULTIPART_COMPLETE_TERMINAL_REAUTHORIZATION_TOKEN: DeterministicFaultToken =
    DeterministicFaultToken::new("multipart-complete-terminal-reauthorization");

fn test_sse_s3_provider() -> StaticManagedKeyProvider {
    StaticManagedKeyProvider::single(
        ManagedWrappingKeyConfig::from_base64(1, TEST_SSE_S3_WRAPPING_KEY_B64).unwrap(),
    )
}

fn setup_coordinator(dir: &Path) -> Coordinator {
    let pg_ids: Vec<u32> = (0..1).collect();
    let storage_cluster = open_test_storage_cluster(dir, &pg_ids);
    Coordinator::new_with_managed_key_provider_for_storage_cluster(
        storage_cluster,
        "us-east-1".to_string(),
        None,
        test_sse_s3_provider(),
    )
    .unwrap()
}

fn open_test_storage_cluster(dir: &Path, pg_ids: &[u32]) -> Arc<StorageCluster> {
    let ec_config = ec::EcConfig::default();
    let ec_shape = storage::EcShape {
        k: ec_config.data_shards(),
        m: ec_config.parity_shards(),
    };
    let node_count = u32::from(ec_shape.k) + u32::from(ec_shape.m);
    let node_ids: Vec<_> = (0..node_count).map(storage::NodeId::new).collect();
    StorageCluster::open_static_local_nodes(dir, &node_ids, pg_ids, ec_shape)
        .expect("open local storage cluster")
}

fn setup_same_process_coordinator_with_storage_cluster(
    storage_cluster: Arc<StorageCluster>,
) -> Coordinator {
    let shared_caches = shared_caches_for_storage_cluster(&storage_cluster);
    Coordinator::new_with_shared_caches_and_lifecycle_sweeper_factory(
        storage_cluster,
        shared_caches,
        "us-east-1".to_string(),
        None,
        Some(test_sse_s3_provider()),
        LifecycleSweeper::acquire_shared,
    )
    .unwrap()
}

fn test_requester() -> Requester {
    test_helpers::requester("default-owner")
}

fn object_request<'a>(bucket: &'a str, key: &'a str, requester: Requester) -> ObjectRequest<'a> {
    ObjectRequest::new(
        trusted_bucket_name(bucket),
        trusted_object_key(key),
        requester,
        None,
    )
}

fn object_version_request<'a>(
    bucket: &'a str,
    key: &'a str,
    version_id: Option<VersionId>,
    requester: Requester,
) -> ObjectVersionRequest<'a> {
    ObjectVersionRequest::new(
        trusted_bucket_name(bucket),
        trusted_object_key(key),
        version_id,
        requester,
        None,
    )
}

trait MultipartUploadIdArg {
    fn into_test_upload_id(self) -> UploadId;
}

impl MultipartUploadIdArg for &str {
    fn into_test_upload_id(self) -> UploadId {
        UploadId::try_from(self).unwrap_or_else(|_| trusted_upload_id(self))
    }
}

impl MultipartUploadIdArg for &UploadId {
    fn into_test_upload_id(self) -> UploadId {
        self.clone()
    }
}

fn multipart_object_request<'a, I: MultipartUploadIdArg>(
    bucket: &'a str,
    key: &'a str,
    upload_id: I,
    requester: Requester,
) -> MultipartObjectRequest<'a> {
    MultipartObjectRequest::new(
        trusted_bucket_name(bucket),
        trusted_object_key(key),
        upload_id.into_test_upload_id(),
        requester,
        None,
    )
}

fn begin_stream_put_test(
    coord: &Coordinator,
    bucket: &str,
    key: &str,
) -> Result<SessionId, ServerError> {
    let authorized = coord.authorize_put_object_write(&AuthorizePutObjectRequest {
        object: object_request(bucket, key, test_requester()),
        acl: NO_PUT_OBJECT_ACL.into(),
        policy_context: PutObjectPolicyContext::default(),
        object_lock: ObjectLockState::default(),
        tags: None,
        encryption: WriteEncryptionRequest::none(),
    })?;
    coord.begin_stream_put_session(&authorized)
}

fn begin_stream_part_test<I: MultipartUploadIdArg>(
    coord: &Coordinator,
    bucket: &str,
    key: &str,
    upload_id: I,
    part_number: u32,
) -> Result<BeginStreamPartResult, ServerError> {
    coord.begin_stream_part(&BeginStreamPartRequest {
        upload: multipart_object_request(bucket, key, upload_id, test_requester()),
        part_number,
        policy_context: PutObjectPolicyContext::default(),
        sse_customer: None,
    })
}

fn create_basic_multipart_upload(
    coord: &Coordinator,
    bucket: &str,
    key: &str,
) -> CreateMultipartUploadResult {
    coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request(bucket, key, test_requester()),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap()
}

fn create_upload_with_parts(
    coord: &Coordinator,
    bucket: &str,
    key: &str,
    parts: &[(u32, &[u8])],
) -> (UploadId, Vec<CompletePart>) {
    let create = create_basic_multipart_upload(coord, bucket, key);
    let complete_parts = parts
        .iter()
        .map(|&(part_number, data)| {
            let result = test_helpers::upload_part(
                coord,
                &test_helpers::UploadPartRequest {
                    upload: multipart_object_request(
                        bucket,
                        key,
                        &create.upload_id,
                        test_requester(),
                    ),
                    part_number,
                    data,
                    claimed_checksum: None,
                    sse_customer: None,
                },
            )
            .unwrap();
            CompletePart {
                part_number,
                etag: result.etag,
                checksum: None,
            }
        })
        .collect();
    (create.upload_id, complete_parts)
}

fn make_test_read_runtime(dir: &Path) -> ReadRuntime {
    let storage_cluster = open_test_storage_cluster(dir, &[0]);
    let ec_shape = storage_cluster.default_payload_ec_shape();
    ReadRuntime {
        storage: super::read_core::ReadStorage::Cluster(storage_cluster),
        payload_buffer_pool: PayloadBufferPool::new(ec_shape),
        sse_c_validator: None,
        managed_key_provider: None,
    }
}

fn read_all_body(mut body: ReadHandle) -> Result<Vec<u8>, ServerError> {
    let mut out = Vec::new();
    while let Some(chunk) = body.next_chunk(INTERNAL_SEGMENT_SIZE)? {
        out.extend_from_slice(&chunk);
    }
    Ok(out)
}

struct InvariantHarness<'a> {
    coord: &'a Coordinator,
}

impl<'a> InvariantHarness<'a> {
    fn new(coord: &'a Coordinator) -> Self {
        Self { coord }
    }

    fn active_stream_session_count_for(&self, bucket: &str, key: &str) -> usize {
        let bucket_name = trusted_bucket_name(bucket);
        let key_name = trusted_object_key(key);
        storage::test_support::stream_upload_session_count_for_object(
            &self.coord.storage_node(),
            &bucket_name,
            &key_name,
        )
        .unwrap()
    }

    fn pending_multipart_upload_count_for(&self, bucket: &str, key: &str) -> usize {
        let bucket_name = trusted_bucket_name(bucket);
        let key_name = trusted_object_key(key);
        storage::test_support::multipart_upload_count_for_object(
            &self.coord.storage_node(),
            &bucket_name,
            &key_name,
        )
        .unwrap()
    }

    fn pending_reclaim_root_count_for(&self, bucket: &str, key: &str) -> usize {
        let bucket_name = trusted_bucket_name(bucket);
        let key_name = trusted_object_key(key);
        storage::test_support::object_payload_reclaim_root_count_for(
            &self.coord.storage_node(),
            &bucket_name,
            &key_name,
        )
        .unwrap()
    }

    fn multipart_part_payload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &UploadId,
    ) -> TestMultipartPartPayloadSnapshot {
        self.coord
            .storage_node()
            .test_capture_multipart_upload_payload(
                &trusted_bucket_name(bucket),
                &trusted_object_key(key),
                upload_id,
            )
            .unwrap()
    }

    fn multipart_upload_state(&self, bucket: &str, key: &str, upload_id: &UploadId) -> UploadState {
        storage::test_support::multipart_upload_state(
            &self.coord.storage_node(),
            &trusted_bucket_name(bucket),
            &trusted_object_key(key),
            upload_id,
        )
        .unwrap()
    }

    fn stream_payload(
        &self,
        bucket: &str,
        key: &str,
        session_id: &SessionId,
    ) -> TestStreamUploadPayloadSnapshot {
        self.coord
            .storage_node()
            .test_capture_stream_upload_payload(
                &trusted_bucket_name(bucket),
                &trusted_object_key(key),
                session_id,
            )
            .unwrap()
    }

    fn mark_stream_session_stale(&self, bucket: &str, key: &str, session_id: &SessionId) {
        self.coord
            .storage_node()
            .test_mark_stream_upload_stale(
                &trusted_bucket_name(bucket),
                &trusted_object_key(key),
                session_id,
            )
            .unwrap();
    }

    fn assert_no_active_stream_sessions_for(&self, bucket: &str, key: &str, invariant: &str) {
        let session_count = self.active_stream_session_count_for(bucket, key);
        assert_eq!(
            session_count, 0,
            "{invariant}: expected no active stream sessions for {bucket}/{key}"
        );
    }

    fn assert_no_pending_multipart_uploads_for(&self, bucket: &str, key: &str, invariant: &str) {
        let upload_count = self.pending_multipart_upload_count_for(bucket, key);
        assert_eq!(
            upload_count, 0,
            "{invariant}: expected no pending multipart uploads for {bucket}/{key}"
        );
    }

    fn assert_no_pending_reclaim_roots_for(&self, bucket: &str, key: &str, invariant: &str) {
        let root_count = self.pending_reclaim_root_count_for(bucket, key);
        assert_eq!(
            root_count, 0,
            "{invariant}: expected no pending reclaim roots for {bucket}/{key}"
        );
    }
}

struct StreamAppendRaceSync {
    prepared_barrier: Arc<Barrier>,
    _serial_guard: MutexGuard<'static, ()>,
    _guard: StreamAppendTestHookGuard,
}

fn install_stream_append_race_hooks(
    coord: &Coordinator,
    session_id: &SessionId,
    segment_index: u32,
) -> StreamAppendRaceSync {
    let serial = STREAM_APPEND_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let prepared_barrier = Arc::new(Barrier::new(3));
    let prepared_barrier_hook = Arc::clone(&prepared_barrier);
    let guard = coord.install_stream_append_test_hooks(StreamAppendTestHooks {
        target: Some((session_id.as_str().to_owned(), segment_index)),
        after_prepare: Some(Arc::new(move || {
            prepared_barrier_hook.wait();
        })),
    });
    StreamAppendRaceSync {
        prepared_barrier,
        _serial_guard: serial,
        _guard: guard,
    }
}

struct MultipartCompletePreCommitRaceSync {
    reached: Arc<Barrier>,
    resume: Arc<Barrier>,
    retry_clock: Option<MultipartCompleteRetryClock>,
    _guard: ReclamationTestHookGuard,
    _serial_guard: MutexGuard<'static, ()>,
}

struct MultipartCompleteStaleDeadlineRaceSync {
    pre_commit_reached: Arc<Barrier>,
    pre_commit_resume: Arc<Barrier>,
    terminal_reauthorization_gate: Arc<DeterministicFaultGate>,
    retry_clock: MultipartCompleteRetryClock,
    _guard: ReclamationTestHookGuard,
    _serial_guard: MutexGuard<'static, ()>,
}

struct MultipartCompleteContentionRaceSync {
    terminal_reauthorization_gate: Arc<DeterministicFaultGate>,
    contention_injections: Arc<AtomicUsize>,
    _guard: ReclamationTestHookGuard,
    _serial_guard: MutexGuard<'static, ()>,
}

#[derive(Clone)]
struct MultipartCompleteRetryClock {
    now: Arc<Mutex<Instant>>,
}

impl MultipartCompleteRetryClock {
    fn frozen() -> Self {
        Self {
            now: Arc::new(Mutex::new(Instant::now())),
        }
    }

    fn now(&self) -> Instant {
        *self.now.lock().unwrap()
    }

    fn advance(&self, elapsed: Duration) {
        let mut now = self.now.lock().unwrap();
        *now += elapsed;
    }
}

impl MultipartCompletePreCommitRaceSync {
    fn advance_retry_clock(&self, elapsed: Duration) {
        self.retry_clock
            .as_ref()
            .expect("multipart completion race hook must install a retry clock")
            .advance(elapsed);
    }
}

impl MultipartCompleteStaleDeadlineRaceSync {
    fn advance_retry_clock(&self, elapsed: Duration) {
        self.retry_clock.advance(elapsed);
    }
}

fn install_multipart_complete_stale_deadline_race_hooks(
    bucket: &str,
    key: &str,
) -> MultipartCompleteStaleDeadlineRaceSync {
    let serial = RECLAMATION_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let pre_commit_reached = Arc::new(Barrier::new(2));
    let pre_commit_resume = Arc::new(Barrier::new(2));
    let terminal_reauthorization_gate =
        DeterministicFaultGate::new(MULTIPART_COMPLETE_TERMINAL_REAUTHORIZATION_TOKEN);
    let pre_commit_reached_hook = Arc::clone(&pre_commit_reached);
    let pre_commit_resume_hook = Arc::clone(&pre_commit_resume);
    let terminal_reauthorization_gate_hook = Arc::clone(&terminal_reauthorization_gate);
    let remaining_pre_commit_pauses = Arc::new(AtomicUsize::new(2));
    let remaining_pre_commit_pauses_hook = Arc::clone(&remaining_pre_commit_pauses);
    let retry_clock = MultipartCompleteRetryClock::frozen();
    let retry_clock_hook = retry_clock.clone();
    let guard = install_reclamation_test_hooks(ReclamationTestHooks {
        target: Some((bucket.to_string(), key.to_string())),
        after_multipart_complete_pre_commit: Some(Arc::new(move || {
            if remaining_pre_commit_pauses_hook
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                pre_commit_reached_hook.wait();
                pre_commit_resume_hook.wait();
            }
        })),
        multipart_complete_stale_snapshot_retry_now: Some(Arc::new(move || retry_clock_hook.now())),
        before_multipart_complete_terminal_reauthorization: Some(Arc::new(move || {
            terminal_reauthorization_gate_hook
                .wait_at(MULTIPART_COMPLETE_TERMINAL_REAUTHORIZATION_TOKEN);
        })),
        ..ReclamationTestHooks::default()
    });
    MultipartCompleteStaleDeadlineRaceSync {
        pre_commit_reached,
        pre_commit_resume,
        terminal_reauthorization_gate,
        retry_clock,
        _guard: guard,
        _serial_guard: serial,
    }
}

fn install_multipart_complete_contention_race_hooks(
    bucket: &str,
    key: &str,
) -> MultipartCompleteContentionRaceSync {
    let serial = RECLAMATION_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let terminal_reauthorization_gate =
        DeterministicFaultGate::new(MULTIPART_COMPLETE_TERMINAL_REAUTHORIZATION_TOKEN);
    let inject_contention = Arc::new(AtomicBool::new(true));
    let contention_injections = Arc::new(AtomicUsize::new(0));
    let inject_contention_hook = Arc::clone(&inject_contention);
    let contention_injections_hook = Arc::clone(&contention_injections);
    let terminal_reauthorization_gate_hook = Arc::clone(&terminal_reauthorization_gate);
    let guard = install_reclamation_test_hooks(ReclamationTestHooks {
        target: Some((bucket.to_string(), key.to_string())),
        multipart_complete_commit_failure: Some(Arc::new(move || {
            inject_contention_hook
                .swap(false, Ordering::SeqCst)
                .then(|| {
                    contention_injections_hook.fetch_add(1, Ordering::SeqCst);
                    storage::MultipartCompletionFailureKind::MetadataCommandContention
                })
        })),
        before_multipart_complete_terminal_reauthorization: Some(Arc::new(move || {
            terminal_reauthorization_gate_hook
                .wait_at(MULTIPART_COMPLETE_TERMINAL_REAUTHORIZATION_TOKEN);
        })),
        ..ReclamationTestHooks::default()
    });
    MultipartCompleteContentionRaceSync {
        terminal_reauthorization_gate,
        contention_injections,
        _guard: guard,
        _serial_guard: serial,
    }
}

fn install_multipart_complete_pre_commit_race_hooks(
    bucket: &str,
    key: &str,
) -> MultipartCompletePreCommitRaceSync {
    let serial = RECLAMATION_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let reached = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    let reached_hook = Arc::clone(&reached);
    let resume_hook = Arc::clone(&resume);
    let retry_clock = MultipartCompleteRetryClock::frozen();
    let retry_clock_hook = retry_clock.clone();
    let guard = install_reclamation_test_hooks(ReclamationTestHooks {
        target: Some((bucket.to_string(), key.to_string())),
        after_multipart_complete_pre_commit: Some(Arc::new(move || {
            reached_hook.wait();
            resume_hook.wait();
        })),
        multipart_complete_stale_snapshot_retry_now: Some(Arc::new(move || retry_clock_hook.now())),
        ..ReclamationTestHooks::default()
    });
    MultipartCompletePreCommitRaceSync {
        reached,
        resume,
        retry_clock: Some(retry_clock),
        _guard: guard,
        _serial_guard: serial,
    }
}

fn install_counted_multipart_complete_pre_commit_race_hooks(
    bucket: &str,
    key: &str,
    pauses: usize,
) -> MultipartCompletePreCommitRaceSync {
    let serial = RECLAMATION_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let reached = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    let reached_hook = Arc::clone(&reached);
    let resume_hook = Arc::clone(&resume);
    let remaining = Arc::new(AtomicUsize::new(pauses));
    let remaining_hook = Arc::clone(&remaining);
    let retry_clock = MultipartCompleteRetryClock::frozen();
    let retry_clock_hook = retry_clock.clone();
    let guard = install_reclamation_test_hooks(ReclamationTestHooks {
        target: Some((bucket.to_string(), key.to_string())),
        after_multipart_complete_pre_commit: Some(Arc::new(move || {
            if remaining_hook
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                reached_hook.wait();
                resume_hook.wait();
            }
        })),
        multipart_complete_stale_snapshot_retry_now: Some(Arc::new(move || retry_clock_hook.now())),
        ..ReclamationTestHooks::default()
    });
    MultipartCompletePreCommitRaceSync {
        reached,
        resume,
        retry_clock: Some(retry_clock),
        _guard: guard,
        _serial_guard: serial,
    }
}

fn install_one_shot_multipart_complete_pre_commit_race_hooks(
    bucket: &str,
    key: &str,
) -> MultipartCompletePreCommitRaceSync {
    let serial = RECLAMATION_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let reached = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    let reached_hook = Arc::clone(&reached);
    let resume_hook = Arc::clone(&resume);
    let first = Arc::new(AtomicBool::new(true));
    let first_hook = Arc::clone(&first);
    let guard = install_reclamation_test_hooks(ReclamationTestHooks {
        target: Some((bucket.to_string(), key.to_string())),
        after_multipart_complete_pre_commit: Some(Arc::new(move || {
            if first_hook.swap(false, Ordering::SeqCst) {
                reached_hook.wait();
                resume_hook.wait();
            }
        })),
        ..ReclamationTestHooks::default()
    });
    MultipartCompletePreCommitRaceSync {
        reached,
        resume,
        retry_clock: None,
        _guard: guard,
        _serial_guard: serial,
    }
}

fn install_multipart_complete_snapshot_race_hooks(
    bucket: &str,
    key: &str,
) -> MultipartCompletePreCommitRaceSync {
    let serial = RECLAMATION_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let reached = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    let reached_hook = Arc::clone(&reached);
    let resume_hook = Arc::clone(&resume);
    let guard = install_reclamation_test_hooks(ReclamationTestHooks {
        target: Some((bucket.to_string(), key.to_string())),
        before_multipart_complete_snapshot: Some(Arc::new(move || {
            reached_hook.wait();
            resume_hook.wait();
        })),
        ..ReclamationTestHooks::default()
    });
    MultipartCompletePreCommitRaceSync {
        reached,
        resume,
        retry_clock: None,
        _guard: guard,
        _serial_guard: serial,
    }
}

fn install_one_shot_multipart_complete_snapshot_race_hooks(
    bucket: &str,
    key: &str,
) -> MultipartCompletePreCommitRaceSync {
    let serial = RECLAMATION_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let reached = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    let reached_hook = Arc::clone(&reached);
    let resume_hook = Arc::clone(&resume);
    let first = Arc::new(AtomicBool::new(true));
    let first_hook = Arc::clone(&first);
    let guard = install_reclamation_test_hooks(ReclamationTestHooks {
        target: Some((bucket.to_string(), key.to_string())),
        before_multipart_complete_snapshot: Some(Arc::new(move || {
            if first_hook.swap(false, Ordering::SeqCst) {
                reached_hook.wait();
                resume_hook.wait();
            }
        })),
        ..ReclamationTestHooks::default()
    });
    MultipartCompletePreCommitRaceSync {
        reached,
        resume,
        retry_clock: None,
        _guard: guard,
        _serial_guard: serial,
    }
}

fn begin_stream_put_with_segment_path(
    coord: &Coordinator,
    bucket: &str,
    key_prefix: &str,
    require_cross_pg: bool,
) -> (String, SessionId) {
    for suffix in 0..256 {
        let key = format!("{key_prefix}-{suffix}");
        let session_id = begin_stream_put_test(coord, bucket, &key).unwrap();
        let has_cross_pg =
            stream_put_session_has_cross_pg_segments(coord, bucket, &key, &session_id);
        if has_cross_pg == require_cross_pg {
            return (key, session_id);
        }
        coord.abort_stream_put(bucket, &key, &session_id).unwrap();
    }
    panic!(
        "failed to find stream session for require_cross_pg={require_cross_pg} after 256 attempts"
    );
}

fn run_stream_duplicate_segment_race_invariant_test(pg_count: u32, require_cross_pg: bool) {
    let dir = test_util::tempdir();
    let pg_ids: Vec<u32> = (0..pg_count).collect();
    let storage_cluster = open_test_storage_cluster(dir.path(), &pg_ids);
    let admin = setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let writer_a =
        setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let writer_b = setup_same_process_coordinator_with_storage_cluster(storage_cluster);
    let invariant =
        "duplicate stream appends at the same segment index must leave exactly one staged winner";
    let state = InvariantHarness::new(&admin);

    admin
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let (key, session_id) =
        begin_stream_put_with_segment_path(&admin, "bucket", "stream-race", require_cross_pg);
    if require_cross_pg {
        assert!(
            stream_put_session_has_cross_pg_segments(&admin, "bucket", &key, &session_id),
            "{invariant}: expected the staged payloads to span PGs for the cross-PG case"
        );
    } else {
        assert!(
            !stream_put_session_has_cross_pg_segments(&admin, "bucket", &key, &session_id),
            "{invariant}: expected same-PG case to stage both payloads in the metadata PG"
        );
    }

    let sync = install_stream_append_race_hooks(&admin, &session_id, 0);
    let data_a = b"first-segment".to_vec();
    let data_b = b"second-segment".to_vec();
    let key_a = key.clone();
    let key_b = key.clone();
    let session_a = session_id.clone();
    let session_b = session_id.clone();
    let t_a = std::thread::spawn(move || {
        writer_a.append_plaintext_stream_segment_for_test("bucket", &key_a, &session_a, 0, &data_a)
    });
    let t_b = std::thread::spawn(move || {
        writer_b.append_plaintext_stream_segment_for_test("bucket", &key_b, &session_b, 0, &data_b)
    });

    sync.prepared_barrier.wait();

    let result_a = t_a.join().unwrap();
    let result_b = t_b.join().unwrap();
    let winner = match (&result_a, &result_b) {
        (Ok(()), Err(ServerError::InvalidRequest { .. })) => b"first-segment".as_slice(),
        (Err(ServerError::InvalidRequest { .. }), Ok(())) => b"second-segment".as_slice(),
        _ => panic!(
            "{invariant}: expected exactly one successful append and one duplicate rejection, got {result_a:?} and {result_b:?}"
        ),
    };

    let staged = admin
        .storage_node()
        .test_capture_stream_upload_payload(
            &trusted_bucket_name("bucket"),
            &trusted_object_key(&key),
            &session_id,
        )
        .unwrap();
    let staged_layout = staged.layout();
    assert_eq!(
        staged_layout.len(),
        1,
        "{invariant}: expected exactly one staged segment after duplicate append race"
    );
    assert_eq!(
        staged_layout[0].segment_index, 0,
        "{invariant}: expected the winner to occupy segment index 0"
    );
    admin
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", &key, test_requester()),
            session_id: &session_id,
            crc64: checksum::crc64::checksum(winner),
            total_size: winner.len() as u64,
            metadata_blob: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            write_encryption: ActiveWriteEncryptionRef::None,
            tags: None,
            cond: &WriteCondition::default(),
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            requested_object_lock: ObjectLockState::default(),
        })
        .unwrap();

    let object = admin
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request("bucket", &key, None, test_requester()),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(
        read_all_body(object.body).unwrap(),
        winner,
        "{invariant}: finalized object body did not match the winning duplicate append"
    );
    state.assert_no_active_stream_sessions_for("bucket", &key, invariant);
}

#[test]
fn streamed_part_reupload_replaces_displaced_shards_without_orphans() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    let invariant = "reuploading a streamed multipart part does not orphan displaced part shards";
    let state = InvariantHarness::new(&coord);

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let mpu = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request("bucket", "key", test_requester()),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let session_a = begin_stream_part_test(&coord, "bucket", "key", &mpu.upload_id, 1)
        .unwrap()
        .session_id;
    let data_a = b"streamed-reupload-a";
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_a, 0, data_a)
        .unwrap();
    let result_a = coord
        .finalize_stream_part(FinalizeStreamPartRequest {
            upload: multipart_object_request("bucket", "key", &mpu.upload_id, test_requester()),
            session_id: &session_a,
            part_number: 1,
            crc64: checksum::crc64::checksum(data_a),
            total_size: data_a.len() as u64,
            claimed_checksum: None,
            computed_checksum: None,
        })
        .unwrap();

    let payload_before = state.multipart_part_payload("bucket", "key", &mpu.upload_id);
    assert_eq!(
        payload_before.segment_count(),
        1,
        "{invariant}: expected exactly one committed segment set before reupload"
    );
    assert!(coord
        .storage_node()
        .test_multipart_part_payload_snapshot_is_fully_present(&payload_before)
        .unwrap());

    let session_b = begin_stream_part_test(&coord, "bucket", "key", &mpu.upload_id, 1)
        .unwrap()
        .session_id;
    let data_b = b"streamed-reupload-b";
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_b, 0, data_b)
        .unwrap();
    let result_b = coord
        .finalize_stream_part(FinalizeStreamPartRequest {
            upload: multipart_object_request("bucket", "key", &mpu.upload_id, test_requester()),
            session_id: &session_b,
            part_number: 1,
            crc64: checksum::crc64::checksum(data_b),
            total_size: data_b.len() as u64,
            claimed_checksum: None,
            computed_checksum: None,
        })
        .unwrap();
    assert_ne!(result_a.etag, result_b.etag);

    let payload_after = state.multipart_part_payload("bucket", "key", &mpu.upload_id);
    assert_eq!(
        payload_after.segment_count(),
        1,
        "{invariant}: expected exactly one current committed segment set after reupload"
    );
    assert!(
        payload_before.part_payload_identity_differs_from(&payload_after, 1),
        "{invariant}: reupload should replace the committed segment generation"
    );
    assert!(coord
        .storage_node()
        .test_multipart_part_payload_snapshot_is_fully_absent(&payload_before)
        .unwrap());
    assert!(coord
        .storage_node()
        .test_multipart_part_payload_snapshot_is_fully_present(&payload_after)
        .unwrap());
    state.assert_no_active_stream_sessions_for("bucket", "key", invariant);
}

#[test]
fn aborting_streamed_multipart_upload_cleans_committed_segments_and_shards() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    let invariant =
        "aborting a streamed multipart upload removes committed multipart segment rows and shards";
    let state = InvariantHarness::new(&coord);

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let mpu = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request("bucket", "key", test_requester()),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let session_id = begin_stream_part_test(&coord, "bucket", "key", &mpu.upload_id, 1)
        .unwrap()
        .session_id;
    let data = b"streamed-part-data-for-abort-test";
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 0, data)
        .unwrap();
    coord
        .finalize_stream_part(FinalizeStreamPartRequest {
            upload: multipart_object_request("bucket", "key", &mpu.upload_id, test_requester()),
            session_id: &session_id,
            part_number: 1,
            crc64: checksum::crc64::checksum(data),
            total_size: data.len() as u64,
            claimed_checksum: None,
            computed_checksum: None,
        })
        .unwrap();

    let payload_before = state.multipart_part_payload("bucket", "key", &mpu.upload_id);
    assert!(
        !payload_before.is_empty(),
        "{invariant}: expected committed multipart segments before abort"
    );
    assert!(coord
        .storage_node()
        .test_multipart_part_payload_snapshot_is_fully_present(&payload_before)
        .unwrap());

    coord
        .abort_multipart_upload(&multipart_object_request(
            "bucket",
            "key",
            &mpu.upload_id,
            test_requester(),
        ))
        .unwrap();

    assert!(
        state
            .multipart_part_payload("bucket", "key", &mpu.upload_id)
            .is_empty(),
        "{invariant}: expected no multipart segment rows after abort"
    );
    state.assert_no_pending_multipart_uploads_for("bucket", "key", invariant);
    state.assert_no_active_stream_sessions_for("bucket", "key", invariant);

    assert!(coord
        .storage_node()
        .test_multipart_part_payload_snapshot_is_fully_absent(&payload_before)
        .unwrap());
}

#[test]
fn scavenging_stale_sessions_removes_abandoned_streaming_state() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    let invariant = "scavenging a stale stream session removes the session and its staged writes";
    let state = InvariantHarness::new(&coord);

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 0, b"data")
        .unwrap();

    state.mark_stream_session_stale("bucket", "key", &session_id);
    let scavenge_time = storage::clock::current_time_millis().saturating_add(61_000);
    let count =
        storage::clock::with_time_override(scavenge_time, || coord.scavenge_stale_sessions(1));
    assert_eq!(
        count, 1,
        "{invariant}: expected exactly one stale session scavenged"
    );

    let err = coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 1, b"more")
        .unwrap_err();
    assert!(
        matches!(
            err,
            ServerError::StreamUpload(ref error)
                if error.kind() == storage::StreamUploadFailureKind::SessionNotFound
        ),
        "{invariant}: expected session-not-found after scavenging, got {err:?}"
    );
    state.assert_no_active_stream_sessions_for("bucket", "key", invariant);
    state.assert_no_pending_reclaim_roots_for("bucket", "key", invariant);
}

#[test]
fn scavenging_skips_committed_objects_and_their_payloads() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    let invariant =
        "scavenging stale sessions does not disturb committed objects or durable payloads";
    let state = InvariantHarness::new(&coord);

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 0, b"safe-data")
        .unwrap();
    coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "key", test_requester()),
            session_id: &session_id,
            crc64: checksum::crc64::checksum(b"safe-data"),
            total_size: 9,
            metadata_blob: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            write_encryption: ActiveWriteEncryptionRef::None,
            tags: None,
            cond: &WriteCondition::default(),
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            requested_object_lock: ObjectLockState::default(),
        })
        .unwrap();

    let count = coord.scavenge_stale_sessions(0);
    assert_eq!(
        count, 0,
        "{invariant}: expected no stale sessions after finalize"
    );

    let result = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request("bucket", "key", None, test_requester()),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(
        read_all_body(result.body).unwrap(),
        b"safe-data",
        "{invariant}: committed object body changed after scavenging"
    );
    state.assert_no_active_stream_sessions_for("bucket", "key", invariant);
    state.assert_no_pending_reclaim_roots_for("bucket", "key", invariant);
}

#[test]
fn staged_stream_object_is_not_visible_before_finalize() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    let invariant = "a staged streaming object is not externally visible before finalize succeeds";
    let state = InvariantHarness::new(&coord);

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "new-key").unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "new-key", &session_id, 0, b"pending")
        .unwrap();

    let err = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request("bucket", "new-key", None, test_requester()),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::ObjectNotFound { .. }),
        "{invariant}: staged object became readable before finalize, got {err:?}"
    );

    let err = coord
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request("bucket", "new-key", None, test_requester()),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::ObjectNotFound { .. }),
        "{invariant}: staged object became head-visible before finalize, got {err:?}"
    );

    let session_count = state.active_stream_session_count_for("bucket", "new-key");
    assert_eq!(
        session_count, 1,
        "{invariant}: expected exactly one active session to hold the staged object state"
    );
    state.assert_no_pending_reclaim_roots_for("bucket", "new-key", invariant);
}

#[test]
fn duplicate_stream_append_race_same_pg_preserves_one_winner() {
    run_stream_duplicate_segment_race_invariant_test(1, false);
}

#[test]
fn duplicate_stream_append_race_cross_pg_preserves_one_winner() {
    run_stream_duplicate_segment_race_invariant_test(4, true);
}

#[test]
fn aborting_multipart_upload_rejects_late_list_parts_without_state_loss() {
    let dir = test_util::tempdir();
    let coord = super::test_support::setup_coordinator_without_lifecycle_sweeper(dir.path());
    let invariant = "once a multipart upload is aborting, later list-parts operations fail predictably without losing the upload state";
    let state = InvariantHarness::new(&coord);

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let create = create_basic_multipart_upload(&coord, "bucket", "key");
    test_helpers::upload_part(
        &coord,
        &test_helpers::UploadPartRequest {
            upload: multipart_object_request("bucket", "key", &create.upload_id, test_requester()),
            part_number: 1,
            data: b"data",
            claimed_checksum: None,
            sse_customer: None,
        },
    )
    .unwrap();

    coord
        .storage_node()
        .test_mark_multipart_upload_aborting(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("key"),
            &create.upload_id,
        )
        .unwrap();

    let err = coord
        .list_parts(&ListPartsRequest {
            upload: multipart_object_request("bucket", "key", &create.upload_id, test_requester()),
            part_number_marker: None,
            max_parts: 100,
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::NoSuchUpload { .. }),
        "{invariant}: expected NoSuchUpload once the upload is aborting, got {err:?}"
    );

    assert_eq!(
        state.multipart_upload_state("bucket", "key", &create.upload_id),
        UploadState::Aborting,
        "{invariant}: late list-parts should not change the aborting terminal state"
    );
}

#[test]
fn completing_multipart_upload_rejects_late_abort_without_state_loss() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    let invariant = "once a multipart upload is completing, later abort attempts fail predictably without losing the upload state";
    let state = InvariantHarness::new(&coord);

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let create = create_basic_multipart_upload(&coord, "bucket", "key");

    coord
        .storage_node()
        .test_mark_multipart_upload_completing(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("key"),
            &create.upload_id,
        )
        .unwrap();

    let err = coord
        .abort_multipart_upload(&multipart_object_request(
            "bucket",
            "key",
            &create.upload_id,
            test_requester(),
        ))
        .unwrap_err();
    assert!(
        matches!(err, ServerError::NoSuchUpload { .. }),
        "{invariant}: expected NoSuchUpload once the upload is completing, got {err:?}"
    );

    assert_eq!(
        state.multipart_upload_state("bucket", "key", &create.upload_id),
        UploadState::Completing,
        "{invariant}: late abort should not change the completing terminal state"
    );
}

fn assert_identical_completion_race_replays_success(
    bucket: &'static str,
    key: &'static str,
    install_hooks: fn(&str, &str) -> MultipartCompletePreCommitRaceSync,
    invariant: &str,
) {
    let dir = test_util::tempdir();
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(dir.path(), &pg_ids);
    let admin = setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let delayed = setup_same_process_coordinator_with_storage_cluster(storage_cluster);
    let state = InvariantHarness::new(&admin);

    admin
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    let (upload_id, parts) = create_upload_with_parts(&admin, bucket, key, &[(1, b"part")]);

    let sync = install_hooks(bucket, key);
    let upload_id_for_delayed = upload_id.clone();
    let parts_for_delayed = parts.clone();
    let delayed_completion = std::thread::spawn(move || {
        delayed.complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request(bucket, key, &upload_id_for_delayed, test_requester()),
            parts: &parts_for_delayed,
            claimed_checksum: None,
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        })
    });

    sync.reached.wait();
    let winning = admin
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request(bucket, key, &upload_id, test_requester()),
            parts: &parts,
            claimed_checksum: None,
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        })
        .unwrap();
    sync.resume.wait();

    let replay = delayed_completion.join().unwrap().unwrap();
    assert_eq!(replay.etag, winning.etag, "{invariant}");
    assert_eq!(replay.version_id, winning.version_id, "{invariant}");

    let object = admin
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request(bucket, key, None, test_requester()),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(object.etag, winning.etag, "{invariant}");
    let err = admin
        .list_parts(&ListPartsRequest {
            upload: multipart_object_request(bucket, key, &upload_id, test_requester()),
            part_number_marker: None,
            max_parts: 100,
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::NoSuchUpload { .. }),
        "{invariant}: completed upload must be terminal, got {err:?}"
    );
    state.assert_no_pending_multipart_uploads_for(bucket, key, invariant);
}

#[test]
fn identical_completion_after_snapshot_lookup_race_replays_success() {
    assert_identical_completion_race_replays_success(
        "race-identical-snapshot-bucket",
        "race-identical-snapshot-key",
        install_one_shot_multipart_complete_snapshot_race_hooks,
        "an identical completion authorized before another completion publishes must restart after its snapshot lookup loses the race and resolve through terminal replay",
    );
}

#[test]
fn identical_completion_after_pre_commit_race_replays_success() {
    assert_identical_completion_race_replays_success(
        "race-identical-commit-bucket",
        "race-identical-commit-key",
        install_one_shot_multipart_complete_pre_commit_race_hooks,
        "an identical completion with a validated snapshot must restart when another completion publishes before its commit and resolve through terminal replay",
    );
}

#[test]
fn identical_completion_published_at_stale_deadline_reauthorizes_replay() {
    let dir = test_util::tempdir();
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(dir.path(), &pg_ids);
    let admin = setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let delayed = setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let uploader = setup_same_process_coordinator_with_storage_cluster(storage_cluster);
    let bucket = "race-identical-stale-deadline-bucket";
    let key = "race-identical-stale-deadline-key";
    let invariant = "an identical winner published at stale-snapshot budget expiry must be observed by final reauthorization";

    admin
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    let part_bytes = b"same part bytes";
    let (upload_id, parts) = create_upload_with_parts(&admin, bucket, key, &[(1, part_bytes)]);

    let sync = install_multipart_complete_stale_deadline_race_hooks(bucket, key);
    let upload_id_for_delayed = upload_id.clone();
    let parts_for_delayed = parts.clone();
    let delayed_completion = std::thread::spawn(move || {
        delayed.complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request(bucket, key, &upload_id_for_delayed, test_requester()),
            parts: &parts_for_delayed,
            claimed_checksum: None,
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        })
    });

    sync.pre_commit_reached.wait();
    let replacement = test_helpers::upload_part(
        &uploader,
        &test_helpers::UploadPartRequest {
            upload: multipart_object_request(bucket, key, &upload_id, test_requester()),
            part_number: 1,
            data: part_bytes,
            claimed_checksum: None,
            sse_customer: None,
        },
    )
    .unwrap();
    assert_eq!(replacement.etag, parts[0].etag, "{invariant}");
    sync.pre_commit_resume.wait();

    sync.pre_commit_reached.wait();
    let second_replacement = test_helpers::upload_part(
        &uploader,
        &test_helpers::UploadPartRequest {
            upload: multipart_object_request(bucket, key, &upload_id, test_requester()),
            part_number: 1,
            data: part_bytes,
            claimed_checksum: None,
            sse_customer: None,
        },
    )
    .unwrap();
    assert_eq!(second_replacement.etag, parts[0].etag, "{invariant}");
    sync.advance_retry_clock(multipart::COMPLETE_MULTIPART_STALE_SNAPSHOT_RETRY_BUDGET);
    sync.pre_commit_resume.wait();

    let _terminal_reauthorization_release_guard =
        sync.terminal_reauthorization_gate.release_on_drop();
    sync.terminal_reauthorization_gate
        .wait_until_arrived(MULTIPART_COMPLETE_RACE_TIMEOUT);
    let winning = admin
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request(bucket, key, &upload_id, test_requester()),
            parts: &parts,
            claimed_checksum: None,
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        })
        .unwrap();
    sync.terminal_reauthorization_gate.release();

    let replay = delayed_completion.join().unwrap().unwrap();
    assert_eq!(replay.etag, winning.etag, "{invariant}");
    assert_eq!(replay.version_id, winning.version_id, "{invariant}");
}

#[test]
fn different_completion_contention_after_winner_reauthorizes_no_such_upload() {
    let dir = test_util::tempdir();
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(dir.path(), &pg_ids);
    let winner = setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let delayed = setup_same_process_coordinator_with_storage_cluster(storage_cluster);
    let bucket = "race-different-contention-bucket";
    let key = "race-different-contention-key";
    let invariant = "commit-time contention after a different completion consumes the upload must reauthorize as NoSuchUpload";

    winner
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    let (upload_id, parts) =
        create_upload_with_parts(&winner, bucket, key, &[(1, b"winner"), (2, b"loser")]);
    let winning_parts = &parts[..1];
    let delayed_parts = parts[1..].to_vec();

    let sync = install_multipart_complete_contention_race_hooks(bucket, key);
    let upload_id_for_delayed = upload_id.clone();
    let delayed_completion = std::thread::spawn(move || {
        delayed.complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request(bucket, key, &upload_id_for_delayed, test_requester()),
            parts: &delayed_parts,
            claimed_checksum: None,
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        })
    });

    let _terminal_reauthorization_release_guard =
        sync.terminal_reauthorization_gate.release_on_drop();
    sync.terminal_reauthorization_gate
        .wait_until_arrived(MULTIPART_COMPLETE_RACE_TIMEOUT);
    winner
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request(bucket, key, &upload_id, test_requester()),
            parts: winning_parts,
            claimed_checksum: None,
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        })
        .unwrap();
    sync.terminal_reauthorization_gate.release();

    let error = delayed_completion.join().unwrap().unwrap_err();
    assert!(
        matches!(error, ServerError::NoSuchUpload { .. }),
        "{invariant}: got {error:?}"
    );
    assert_eq!(
        sync.contention_injections.load(Ordering::SeqCst),
        1,
        "{invariant}: the delayed commit must exercise the contention branch exactly once"
    );
}

#[test]
fn abort_wins_over_complete_after_snapshot_without_leaking_multipart_state() {
    let dir = test_util::tempdir();
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(dir.path(), &pg_ids);
    let admin = setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let completer =
        setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let aborter = setup_same_process_coordinator_with_storage_cluster(storage_cluster);
    let bucket = "race-complete-bucket";
    let key = "race-complete-key";
    let invariant =
        "if abort wins after complete has snapshotted multipart state, the upload is removed without exposing a committed object or leaking multipart state";
    let state = InvariantHarness::new(&admin);

    admin
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    let (upload_id, parts) = create_upload_with_parts(&admin, bucket, key, &[(1, b"part")]);

    let sync = install_multipart_complete_pre_commit_race_hooks(bucket, key);
    let upload_id_for_complete = upload_id.clone();
    let parts_for_complete = parts.clone();
    let t_complete = std::thread::spawn(move || {
        completer.complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request(
                bucket,
                key,
                &upload_id_for_complete,
                test_requester(),
            ),
            parts: &parts_for_complete,
            claimed_checksum: None,
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        })
    });

    sync.reached.wait();

    aborter
        .abort_multipart_upload(&multipart_object_request(
            bucket,
            key,
            &upload_id,
            test_requester(),
        ))
        .unwrap();

    sync.resume.wait();
    let complete_res = t_complete.join().unwrap();
    assert!(
        complete_res.is_err(),
        "{invariant}: complete should lose once abort deletes the upload, got {complete_res:?}"
    );

    let err = admin
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request(bucket, key, None, test_requester()),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::ObjectNotFound { .. }),
        "{invariant}: aborted completion race should not expose a visible object, got {err:?}"
    );

    let err = admin
        .list_parts(&ListPartsRequest {
            upload: multipart_object_request(bucket, key, &upload_id, test_requester()),
            part_number_marker: None,
            max_parts: 100,
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::NoSuchUpload { .. }),
        "{invariant}: multipart upload should be gone after abort wins, got {err:?}"
    );

    state.assert_no_pending_multipart_uploads_for(bucket, key, invariant);
    state.assert_no_pending_reclaim_roots_for(bucket, key, invariant);
    assert!(
        state
            .multipart_part_payload(bucket, key, &upload_id)
            .is_empty(),
        "{invariant}: abort winner should leave no committed multipart segment rows"
    );
}

#[test]
fn upload_part_replace_after_complete_snapshot_is_revalidated_before_publish() {
    let dir = test_util::tempdir();
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(dir.path(), &pg_ids);
    let admin = setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let completer =
        setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let uploader = setup_same_process_coordinator_with_storage_cluster(storage_cluster);
    let bucket = "race-complete-part-replace";
    let key = "race-complete-part-key";
    let invariant =
        "CompleteMultipartUpload must not publish a part row that was replaced after snapshot";

    admin
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    let (upload_id, parts) = create_upload_with_parts(&admin, bucket, key, &[(1, b"old part")]);

    let sync = install_one_shot_multipart_complete_pre_commit_race_hooks(bucket, key);
    let upload_id_for_complete = upload_id.clone();
    let parts_for_complete = parts.clone();
    let t_complete = std::thread::spawn(move || {
        completer.complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request(
                bucket,
                key,
                &upload_id_for_complete,
                test_requester(),
            ),
            parts: &parts_for_complete,
            claimed_checksum: None,
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        })
    });

    sync.reached.wait();
    let replacement = test_helpers::upload_part(
        &uploader,
        &test_helpers::UploadPartRequest {
            upload: multipart_object_request(bucket, key, &upload_id, test_requester()),
            part_number: 1,
            data: b"replacement part",
            claimed_checksum: None,
            sse_customer: None,
        },
    )
    .unwrap();
    assert_ne!(
        replacement.etag, parts[0].etag,
        "{invariant}: replacement part must change the selected row identity"
    );
    sync.resume.wait();

    let err = t_complete.join().unwrap().unwrap_err();
    assert!(
        matches!(err, ServerError::InvalidPart { part_number: 1 }),
        "{invariant}: stale completion should retry and return InvalidPart, got {err:?}"
    );

    let err = admin
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request(bucket, key, None, test_requester()),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::ObjectNotFound { .. }),
        "{invariant}: stale completion must not expose a visible object, got {err:?}"
    );

    let listed = admin
        .list_parts(&ListPartsRequest {
            upload: multipart_object_request(bucket, key, &upload_id, test_requester()),
            part_number_marker: None,
            max_parts: 100,
        })
        .unwrap();
    assert_eq!(listed.parts.len(), 1, "{invariant}");
    assert_eq!(listed.parts[0].etag, replacement.etag, "{invariant}");
}

#[test]
fn same_etag_part_replacement_can_retry_beyond_old_attempt_limit() {
    const STALE_SNAPSHOTS: usize = 9;

    let dir = test_util::tempdir();
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(dir.path(), &pg_ids);
    let admin = setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let completer =
        setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let uploader = setup_same_process_coordinator_with_storage_cluster(storage_cluster);
    let bucket = "race-complete-part-time-budget";
    let key = "race-complete-part-time-budget-key";
    let invariant =
        "fast same-ETag replacements must use the elapsed-time retry budget, not an attempt limit";

    admin
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    let part_bytes = b"same part bytes";
    let (upload_id, parts) = create_upload_with_parts(&admin, bucket, key, &[(1, part_bytes)]);

    let sync =
        install_counted_multipart_complete_pre_commit_race_hooks(bucket, key, STALE_SNAPSHOTS);
    let upload_id_for_complete = upload_id.clone();
    let parts_for_complete = parts.clone();
    let t_complete = std::thread::spawn(move || {
        completer.complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request(
                bucket,
                key,
                &upload_id_for_complete,
                test_requester(),
            ),
            parts: &parts_for_complete,
            claimed_checksum: None,
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        })
    });

    for replacement_index in 0..STALE_SNAPSHOTS {
        sync.reached.wait();
        let replacement = test_helpers::upload_part(
            &uploader,
            &test_helpers::UploadPartRequest {
                upload: multipart_object_request(bucket, key, &upload_id, test_requester()),
                part_number: 1,
                data: part_bytes,
                claimed_checksum: None,
                sse_customer: None,
            },
        )
        .unwrap();
        assert_eq!(
            replacement.etag, parts[0].etag,
            "{invariant}: replacement {replacement_index} must preserve the selected ETag"
        );
        sync.resume.wait();
    }

    let completed = t_complete.join().unwrap().unwrap();
    assert!(!completed.etag.is_empty(), "{invariant}");
    let object = admin
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request(bucket, key, None, test_requester()),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(
        read_all_body(object.body).unwrap(),
        part_bytes,
        "{invariant}"
    );
}

#[test]
fn stale_snapshot_time_budget_exhaustion_returns_operation_aborted_without_publication() {
    let dir = test_util::tempdir();
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(dir.path(), &pg_ids);
    let admin = setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let completer =
        setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let uploader = setup_same_process_coordinator_with_storage_cluster(storage_cluster);
    let bucket = "race-complete-part-starvation";
    let key = "race-complete-part-starvation-key";
    let invariant = "valid same-ETag part replacement after the completion stale-snapshot deadline must return OperationAborted without publishing an object or damaging the upload";
    let state = InvariantHarness::new(&admin);

    admin
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    let part_bytes = b"same part bytes";
    let (upload_id, parts) = create_upload_with_parts(&admin, bucket, key, &[(1, part_bytes)]);

    let sync = install_multipart_complete_pre_commit_race_hooks(bucket, key);
    let upload_id_for_complete = upload_id.clone();
    let parts_for_complete = parts.clone();
    let t_complete = std::thread::spawn(move || {
        completer.complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request(
                bucket,
                key,
                &upload_id_for_complete,
                test_requester(),
            ),
            parts: &parts_for_complete,
            claimed_checksum: None,
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        })
    });

    sync.reached.wait();
    let replacement = test_helpers::upload_part(
        &uploader,
        &test_helpers::UploadPartRequest {
            upload: multipart_object_request(bucket, key, &upload_id, test_requester()),
            part_number: 1,
            data: part_bytes,
            claimed_checksum: None,
            sse_customer: None,
        },
    )
    .unwrap();
    assert_eq!(
        replacement.etag, parts[0].etag,
        "{invariant}: replacement must preserve the requested ETag"
    );
    sync.resume.wait();

    sync.reached.wait();
    let replacement = test_helpers::upload_part(
        &uploader,
        &test_helpers::UploadPartRequest {
            upload: multipart_object_request(bucket, key, &upload_id, test_requester()),
            part_number: 1,
            data: part_bytes,
            claimed_checksum: None,
            sse_customer: None,
        },
    )
    .unwrap();
    assert_eq!(
        replacement.etag, parts[0].etag,
        "{invariant}: second replacement must preserve the requested ETag"
    );
    sync.advance_retry_clock(multipart::COMPLETE_MULTIPART_STALE_SNAPSHOT_RETRY_BUDGET);
    sync.resume.wait();

    let err = t_complete.join().unwrap().unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "{invariant}: got {err:?}"
    );

    let err = admin
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request(bucket, key, None, test_requester()),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::ObjectNotFound { .. }),
        "{invariant}: got published object {err:?}"
    );

    let listed = admin
        .list_parts(&ListPartsRequest {
            upload: multipart_object_request(bucket, key, &upload_id, test_requester()),
            part_number_marker: None,
            max_parts: 100,
        })
        .unwrap();
    assert_eq!(listed.parts.len(), 1, "{invariant}");
    assert_eq!(listed.parts[0].part_number, 1, "{invariant}");
    assert_eq!(listed.parts[0].etag, parts[0].etag, "{invariant}");
    assert_eq!(
        state.multipart_upload_state(bucket, key, &upload_id),
        UploadState::InProgress,
        "{invariant}"
    );
}

#[test]
fn complete_multipart_if_match_uses_identity_validated_snapshot_etag() {
    let dir = test_util::tempdir();
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(dir.path(), &pg_ids);
    let admin = setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let completer =
        setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let writer = setup_same_process_coordinator_with_storage_cluster(storage_cluster);
    let bucket = "race-complete-if-match-bucket";
    let key = "race-complete-if-match-key";
    let invariant =
        "CompleteMultipartUpload If-Match must be checked against the same snapshot used to build the command";

    admin
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    let initial = test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            object: object_request(bucket, key, test_requester()),
            data: b"initial object",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: &WriteCondition::default(),
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
        },
    )
    .unwrap();
    let (upload_id, parts) = create_upload_with_parts(&admin, bucket, key, &[(1, b"mpu object")]);

    let sync = install_multipart_complete_snapshot_race_hooks(bucket, key);
    let upload_id_for_complete = upload_id.clone();
    let parts_for_complete = parts.clone();
    let initial_etag = initial.etag.clone();
    let t_complete = std::thread::spawn(move || {
        let cond = WriteCondition::IfMatch(SpecificEtag::new(initial_etag).unwrap());
        completer.complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request(
                bucket,
                key,
                &upload_id_for_complete,
                test_requester(),
            ),
            parts: &parts_for_complete,
            claimed_checksum: None,
            expected_object_size: None,
            cond: &cond,
            sse_customer: None,
        })
    });

    sync.reached.wait();
    let intervening = test_helpers::put_object(
        &writer,
        &PutObjectRequest {
            object: object_request(bucket, key, test_requester()),
            data: b"intervening object",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: &WriteCondition::default(),
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
        },
    )
    .unwrap();
    assert_ne!(
        intervening.etag, initial.etag,
        "{invariant}: intervening write must change the current object etag"
    );
    sync.resume.wait();

    let err = t_complete.join().unwrap().unwrap_err();
    assert!(
        matches!(err, ServerError::PreconditionFailed { .. }),
        "{invariant}: completion should fail its If-Match against the updated snapshot, got {err:?}"
    );

    let current = admin
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request(bucket, key, None, test_requester()),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(
        current.etag, intervening.etag,
        "{invariant}: failed completion must not replace the intervening object"
    );
}

#[test]
fn failed_stream_put_finalize_is_scavenged_without_visibility_or_orphans() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    let invariant =
        "a failed stream-put finalize leaves no visible object, and stale-session scavenging removes the abandoned staged shards";
    let state = InvariantHarness::new(&coord);

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 0, b"hello")
        .unwrap();

    let staged_payload = state.stream_payload("bucket", "key", &session_id);
    assert_eq!(
        staged_payload.segment_count(),
        1,
        "{invariant}: expected one staged segment before finalize failure"
    );
    assert!(coord
        .storage_node()
        .test_stream_upload_payload_snapshot_is_fully_present(&staged_payload)
        .unwrap());

    let err = coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "key", test_requester()),
            session_id: &session_id,
            crc64: checksum::crc64::checksum(b"hello"),
            total_size: 999,
            metadata_blob: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            write_encryption: ActiveWriteEncryptionRef::None,
            tags: None,
            cond: &WriteCondition::default(),
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            requested_object_lock: ObjectLockState::default(),
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::InvalidRequest { .. }),
        "{invariant}: expected InvalidRequest from failed finalize, got {err:?}"
    );

    let err = coord
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request("bucket", "key", None, test_requester()),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::ObjectNotFound { .. }),
        "{invariant}: failed finalize should not make the object visible, got {err:?}"
    );

    let session_count = state.active_stream_session_count_for("bucket", "key");
    assert_eq!(
        session_count, 1,
        "{invariant}: failed finalize should leave exactly one stale session to scavenge"
    );
    state.assert_no_pending_reclaim_roots_for("bucket", "key", invariant);

    state.mark_stream_session_stale("bucket", "key", &session_id);
    let scavenge_time = storage::clock::current_time_millis().saturating_add(61_000);
    let count =
        storage::clock::with_time_override(scavenge_time, || coord.scavenge_stale_sessions(1));
    assert_eq!(
        count, 1,
        "{invariant}: expected stale-session scavenging to clean the failed finalize session"
    );

    state.assert_no_active_stream_sessions_for("bucket", "key", invariant);
    state.assert_no_pending_reclaim_roots_for("bucket", "key", invariant);
    assert!(coord
        .storage_node()
        .test_stream_upload_payload_snapshot_is_fully_absent(&staged_payload)
        .unwrap());
}

#[test]
fn dropping_a_read_only_payload_lease_does_not_enqueue_reclaim_work() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator_without_reclaim_sweeper(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request("bucket", "key", test_requester()),
            data: b"leased live payload",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let subject = storage::test_support::capture_object_payload_reclaim_subject(
        &coord.storage_node(),
        &trusted_bucket_name("bucket"),
        &trusted_object_key("key"),
        VersionId::Null,
    )
    .unwrap();

    drop(coord.read_runtime().acquire_object_payload_lease(&subject));

    assert_eq!(
        coord
            .storage_node()
            .test_object_payload_reclaim_outstanding_depth(),
        0,
        "dropping a read-only lease without durable reclaim metadata must not schedule work"
    );
}

#[test]
fn final_payload_lease_drop_enqueues_only_while_reclaim_metadata_exists() {
    let dir = test_util::tempdir();
    let runtime = make_test_read_runtime(dir.path());
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let subject = storage::test_support::seed_segmented_object_payload_reclaim(
        runtime.storage_node(),
        &bucket,
        &key,
        1,
    )
    .unwrap();

    let lease = runtime.acquire_object_payload_lease(&subject);
    drop(lease);

    assert_eq!(
        runtime
            .storage_node()
            .test_object_payload_reclaim_outstanding_depth(),
        1,
        "dropping the final lease must schedule the extant durable reclaim root"
    );
    assert!(storage::test_support::object_payload_has_reclaim_root(
        runtime.storage_node(),
        &subject,
    )
    .unwrap());
}

#[test]
fn failed_stream_part_finalize_abort_cleanup_leaves_no_visible_part_or_orphans() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    let invariant =
        "a failed stream-part finalize leaves no visible multipart part, and abort cleanup removes the staged shards";
    let state = InvariantHarness::new(&coord);

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let mpu = create_basic_multipart_upload(&coord, "bucket", "key");
    let session_id = begin_stream_part_test(&coord, "bucket", "key", &mpu.upload_id, 1)
        .unwrap()
        .session_id;
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 0, b"part-data")
        .unwrap();

    let staged_payload = state.stream_payload("bucket", "key", &session_id);
    assert_eq!(
        staged_payload.segment_count(),
        1,
        "{invariant}: expected one staged multipart segment before finalize failure"
    );
    assert!(coord
        .storage_node()
        .test_stream_upload_payload_snapshot_is_fully_present(&staged_payload)
        .unwrap());

    let err = coord
        .finalize_stream_part(FinalizeStreamPartRequest {
            upload: multipart_object_request("bucket", "key", &mpu.upload_id, test_requester()),
            session_id: &session_id,
            part_number: 1,
            crc64: checksum::crc64::checksum(b"part-data"),
            total_size: 999,
            claimed_checksum: None,
            computed_checksum: None,
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::InvalidRequest { .. }),
        "{invariant}: expected InvalidRequest from failed stream-part finalize, got {err:?}"
    );

    let parts = coord
        .list_parts(&ListPartsRequest {
            upload: multipart_object_request("bucket", "key", &mpu.upload_id, test_requester()),
            part_number_marker: None,
            max_parts: 100,
        })
        .unwrap();
    assert!(
        parts.parts.is_empty(),
        "{invariant}: failed finalize should not expose a committed multipart part"
    );

    assert_eq!(
        state.multipart_upload_state("bucket", "key", &mpu.upload_id),
        UploadState::InProgress,
        "{invariant}: failed part finalize should not change the multipart upload state"
    );

    let session_count = state.active_stream_session_count_for("bucket", "key");
    assert_eq!(
        session_count,
        1,
        "{invariant}: failed part finalize should leave exactly one active session for the request abort guard"
    );

    coord
        .abort_stream_part_session(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("key"),
            &session_id,
        )
        .expect("abort failed stream-part session");

    state.assert_no_active_stream_sessions_for("bucket", "key", invariant);
    assert!(
        state
            .multipart_part_payload("bucket", "key", &mpu.upload_id)
            .is_empty(),
        "{invariant}: failed finalize should leave no committed multipart part segments"
    );
    assert!(coord
        .storage_node()
        .test_stream_upload_payload_snapshot_is_fully_absent(&staged_payload)
        .unwrap());
}
