use super::test_helpers;
use super::*;
use crate::conditional::ReadCondition;
use crate::metadata_blob::MetadataBlob;
use crate::sse::ManagedWrappingKeyConfig;
use crate::system_metadata::SystemMetadata;
use ec::EcConfig;
use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, TestCaseError, TestCaseResult};
use std::fmt::Write as _;
use std::path::Path;
use std::sync::Arc;

const TRACE_BUCKET: &str = "bucket";
const TRACE_KEY: &str = "key";
const TRACE_PART_DATA: &[u8] = b"trace-part";
const TRACE_MAX_OPS: usize = 10;
const NO_READ: &ReadCondition = &ReadCondition {
    if_match: None,
    if_none_match: None,
    if_modified_since: None,
    if_unmodified_since: None,
};
const NO_PUT_OBJECT_ACL: PutObjectAcl<'static> = PutObjectAcl::None;
const TEST_SSE_S3_WRAPPING_KEY_B64: &str = "YWJjZGVmMDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODk=";

fn test_sse_s3_provider() -> StaticManagedKeyProvider {
    StaticManagedKeyProvider::single(
        ManagedWrappingKeyConfig::from_base64(1, TEST_SSE_S3_WRAPPING_KEY_B64).unwrap(),
    )
}

fn setup_coordinator(dir: &Path) -> Coordinator {
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_node = Arc::new(SharedStorageNode::open(dir, &pg_ids).unwrap());
    let ec_config = EcConfig::default();
    Coordinator::new_with_managed_key_provider(
        storage_node,
        ec_config,
        "us-east-1".to_string(),
        None,
        test_sse_s3_provider(),
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

trait MultipartUploadIdArg {
    fn into_test_upload_id(self) -> UploadId;
}

impl MultipartUploadIdArg for &UploadId {
    fn into_test_upload_id(self) -> UploadId {
        self.clone()
    }
}

impl MultipartUploadIdArg for &str {
    fn into_test_upload_id(self) -> UploadId {
        UploadId::try_from(self).unwrap_or_else(|_| trusted_upload_id(self))
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

fn read_all_body(mut body: ReadHandle) -> Result<Vec<u8>, ServerError> {
    let mut out = Vec::new();
    while let Some(chunk) = body.next_chunk(INTERNAL_SEGMENT_SIZE)? {
        out.extend_from_slice(&chunk);
    }
    Ok(out)
}

#[derive(Debug, Clone)]
enum MultipartTraceSeed {
    Choice(u8),
}

#[derive(Debug, Clone)]
enum MultipartTraceOp {
    CreateUpload,
    BeginStreamPart,
    AppendData,
    FinalizePart,
    CompleteActive,
    AbortActive,
    AbortLast,
    CompleteLast,
    AbortSession,
}

impl std::fmt::Display for MultipartTraceOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CreateUpload => write!(f, "create-upload"),
            Self::BeginStreamPart => write!(f, "begin-stream-part"),
            Self::AppendData => write!(f, "append-data"),
            Self::FinalizePart => write!(f, "finalize-part"),
            Self::CompleteActive => write!(f, "complete-active"),
            Self::AbortActive => write!(f, "abort-active"),
            Self::AbortLast => write!(f, "abort-last"),
            Self::CompleteLast => write!(f, "complete-last"),
            Self::AbortSession => write!(f, "abort-session"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionModelState {
    Empty,
    HasData,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MultipartTraceModel {
    object_visible: bool,
    active_upload: bool,
    active_session: Option<SessionModelState>,
    part_committed: bool,
    last_upload_available: bool,
    last_completed_upload_available: bool,
}

impl MultipartTraceModel {
    fn new() -> Self {
        Self {
            object_visible: false,
            active_upload: false,
            active_session: None,
            part_committed: false,
            last_upload_available: false,
            last_completed_upload_available: false,
        }
    }

    fn legal_ops(&self) -> &'static [MultipartTraceOp] {
        use MultipartTraceOp::*;
        use SessionModelState::*;
        match (
            self.active_upload,
            self.active_session,
            self.part_committed,
            self.last_upload_available,
            self.last_completed_upload_available,
        ) {
            (false, None, false, false, false) => &[CreateUpload],
            (false, None, false, true, false) => &[CreateUpload, AbortLast],
            (false, None, false, true, true) => &[CreateUpload, AbortLast, CompleteLast],
            (false, Some(_), false, _, _) => &[AbortSession],
            (true, None, false, _, _) => &[BeginStreamPart, AbortActive],
            (true, Some(Empty), false, _, _) => &[AppendData, AbortSession],
            (true, Some(HasData), false, _, _) => &[AppendData, FinalizePart, AbortSession],
            (true, Some(Empty), true, _, _) => &[AppendData, AbortSession, AbortActive],
            (true, Some(HasData), true, _, _) => {
                &[AppendData, FinalizePart, AbortSession, AbortActive]
            }
            (true, None, true, _, _) => &[CompleteActive, AbortActive, BeginStreamPart],
            _ => &[CreateUpload],
        }
    }

    fn apply(&mut self, op: &MultipartTraceOp) {
        use MultipartTraceOp::*;
        use SessionModelState::*;
        match op {
            CreateUpload => {
                self.active_upload = true;
                self.active_session = None;
                self.part_committed = false;
                self.last_upload_available = true;
            }
            BeginStreamPart => {
                self.active_session = Some(Empty);
            }
            AppendData => {
                self.active_session = Some(HasData);
            }
            FinalizePart => {
                self.active_session = None;
                self.part_committed = true;
            }
            CompleteActive => {
                self.active_upload = false;
                self.active_session = None;
                self.part_committed = false;
                self.object_visible = true;
                self.last_upload_available = true;
                self.last_completed_upload_available = true;
            }
            AbortActive => {
                self.active_upload = false;
                self.part_committed = false;
                self.last_upload_available = true;
            }
            AbortLast | CompleteLast => {}
            AbortSession => {
                self.active_session = None;
            }
        }
    }
}

fn render_trace(ops: &[MultipartTraceOp]) -> String {
    let mut rendered = String::new();
    for (index, op) in ops.iter().enumerate() {
        let _ = writeln!(&mut rendered, "{index}: {op}");
    }
    rendered
}

fn multipart_trace_strategy() -> BoxedStrategy<Vec<MultipartTraceOp>> {
    proptest::collection::vec(
        any::<u8>().prop_map(MultipartTraceSeed::Choice),
        0..=TRACE_MAX_OPS,
    )
    .prop_map(|seeds| {
        let mut model = MultipartTraceModel::new();
        let mut ops = Vec::with_capacity(seeds.len());
        for seed in seeds {
            let MultipartTraceSeed::Choice(choice) = seed;
            let legal = model.legal_ops();
            let op = legal[(choice as usize) % legal.len()].clone();
            model.apply(&op);
            ops.push(op);
        }
        ops
    })
    .boxed()
}

struct MultipartTraceHarness {
    coord: Coordinator,
    current_upload_id: Option<UploadId>,
    last_upload_id: Option<UploadId>,
    last_completed_upload_id: Option<UploadId>,
    current_session_id: Option<SessionId>,
    last_part_etag: Option<String>,
    last_completed_part_etag: Option<String>,
}

impl MultipartTraceHarness {
    fn new(coord: Coordinator) -> Self {
        Self {
            coord,
            current_upload_id: None,
            last_upload_id: None,
            last_completed_upload_id: None,
            current_session_id: None,
            last_part_etag: None,
            last_completed_part_etag: None,
        }
    }

    fn execute(&mut self, op: &MultipartTraceOp) {
        match op {
            MultipartTraceOp::CreateUpload => {
                let create = create_basic_multipart_upload(&self.coord, TRACE_BUCKET, TRACE_KEY);
                self.last_upload_id = Some(create.upload_id.clone());
                self.current_upload_id = Some(create.upload_id);
                self.current_session_id = None;
                self.last_part_etag = None;
            }
            MultipartTraceOp::BeginStreamPart => {
                let upload_id = self.current_upload_id.as_ref().unwrap();
                let session =
                    begin_stream_part_test(&self.coord, TRACE_BUCKET, TRACE_KEY, upload_id, 1)
                        .unwrap();
                self.current_session_id = Some(session.session_id);
            }
            MultipartTraceOp::AppendData => {
                let session_id = self.current_session_id.as_ref().unwrap();
                let _ = self.coord.append_plaintext_stream_segment_for_test(
                    TRACE_BUCKET,
                    TRACE_KEY,
                    session_id,
                    0,
                    TRACE_PART_DATA,
                );
            }
            MultipartTraceOp::FinalizePart => {
                let upload_id = self.current_upload_id.as_ref().unwrap();
                let session_id = self.current_session_id.as_ref().unwrap();
                let result = self
                    .coord
                    .finalize_stream_part(FinalizeStreamPartRequest {
                        upload: multipart_object_request(
                            TRACE_BUCKET,
                            TRACE_KEY,
                            upload_id,
                            test_requester(),
                        ),
                        session_id,
                        part_number: 1,
                        crc64: checksum::crc64::checksum(TRACE_PART_DATA),
                        total_size: TRACE_PART_DATA.len() as u64,
                        claimed_checksum: None,
                        computed_checksum: None,
                    })
                    .unwrap();
                self.last_part_etag = Some(result.etag);
                self.current_session_id = None;
            }
            MultipartTraceOp::CompleteActive => {
                let upload_id = self.current_upload_id.as_ref().unwrap();
                let etag = self.last_part_etag.as_ref().unwrap();
                let _ = self
                    .coord
                    .complete_multipart_upload(&CompleteMultipartUploadRequest {
                        upload: multipart_object_request(
                            TRACE_BUCKET,
                            TRACE_KEY,
                            upload_id,
                            test_requester(),
                        ),
                        parts: &[CompletePart {
                            part_number: 1,
                            etag: etag.clone(),
                            checksum: None,
                        }],
                        claimed_checksum: None,
                        expected_object_size: None,
                        cond: &WriteCondition::default(),
                        sse_customer: None,
                    });
                self.last_completed_upload_id = Some(upload_id.clone());
                self.last_completed_part_etag = Some(etag.clone());
                self.current_upload_id = None;
                self.current_session_id = None;
                self.last_part_etag = None;
            }
            MultipartTraceOp::AbortActive => {
                let upload_id = self.current_upload_id.as_ref().unwrap();
                let session_id = self.current_session_id.clone();
                let _ = self.coord.abort_multipart_upload(&multipart_object_request(
                    TRACE_BUCKET,
                    TRACE_KEY,
                    upload_id,
                    test_requester(),
                ));
                self.current_upload_id = None;
                self.current_session_id = session_id;
                self.last_part_etag = None;
            }
            MultipartTraceOp::AbortLast => {
                let upload_id = self.last_upload_id.as_ref().unwrap();
                let _ = self.coord.abort_multipart_upload(&multipart_object_request(
                    TRACE_BUCKET,
                    TRACE_KEY,
                    upload_id,
                    test_requester(),
                ));
            }
            MultipartTraceOp::CompleteLast => {
                let upload_id = self.last_completed_upload_id.as_ref().unwrap();
                let etag = self.last_completed_part_etag.as_ref().unwrap();
                let _ = self
                    .coord
                    .complete_multipart_upload(&CompleteMultipartUploadRequest {
                        upload: multipart_object_request(
                            TRACE_BUCKET,
                            TRACE_KEY,
                            upload_id,
                            test_requester(),
                        ),
                        parts: &[CompletePart {
                            part_number: 1,
                            etag: etag.clone(),
                            checksum: None,
                        }],
                        claimed_checksum: None,
                        expected_object_size: None,
                        cond: &WriteCondition::default(),
                        sse_customer: None,
                    });
            }
            MultipartTraceOp::AbortSession => {
                let session_id = self.current_session_id.as_ref().unwrap();
                let _ = self.coord.abort_stream_part_session(
                    &trusted_bucket_name(TRACE_BUCKET),
                    &trusted_object_key(TRACE_KEY),
                    session_id,
                );
                self.current_session_id = None;
            }
        }
    }

    fn active_session_count(&self) -> usize {
        let mut count = 0usize;
        self.coord
            .pg_topology
            .for_each_pg(|pg_id| {
                let pg = self.coord.storage_node.get_pg(pg_id)?;
                count += pg
                    .list_all_stream_uploads()?
                    .into_iter()
                    .filter(|session| {
                        session.bucket.as_str() == TRACE_BUCKET && session.key.as_str() == TRACE_KEY
                    })
                    .count();
                Ok::<(), ServerError>(())
            })
            .unwrap();
        count
    }

    fn pending_upload_count(&self) -> usize {
        let mut count = 0usize;
        self.coord
            .pg_topology
            .for_each_pg(|pg_id| {
                let pg = self.coord.storage_node.get_pg(pg_id)?;
                count += pg
                    .list_multipart_uploads(&storage::ListMultipartUploadsReq {
                        bucket: trusted_bucket_name(TRACE_BUCKET),
                        prefix: None,
                        key_marker: None,
                        upload_id_marker: None,
                        max_uploads: u32::MAX,
                    })?
                    .uploads
                    .into_iter()
                    .filter(|upload| upload.key.as_str() == TRACE_KEY)
                    .count();
                Ok::<(), ServerError>(())
            })
            .unwrap();
        count
    }
}

fn assert_trace_matches_model(
    harness: &MultipartTraceHarness,
    model: &MultipartTraceModel,
    context: &str,
) -> TestCaseResult {
    prop_assert_eq!(
        harness.pending_upload_count(),
        usize::from(model.active_upload),
        "{}",
        context
    );
    prop_assert_eq!(
        harness.active_session_count(),
        usize::from(model.active_session.is_some()),
        "{}",
        context
    );

    if let Some(upload_id) = harness
        .current_upload_id
        .as_ref()
        .filter(|_| model.active_upload)
    {
        let result = harness.coord.list_parts(&ListPartsRequest {
            upload: multipart_object_request(TRACE_BUCKET, TRACE_KEY, upload_id, test_requester()),
            part_number_marker: None,
            max_parts: 100,
        });
        match (model.part_committed, result) {
            (true, Ok(parts)) => {
                prop_assert_eq!(parts.parts.len(), 1, "{}", context);
            }
            (false, Ok(parts)) => {
                prop_assert!(parts.parts.is_empty(), "{}", context);
            }
            (_, Err(err)) => {
                return Err(TestCaseError::fail(format!(
                    "{context}\nlist_parts unexpectedly failed: {err:?}"
                )));
            }
        }
    } else if let Some(upload_id) = harness.last_upload_id.as_ref() {
        let err = harness
            .coord
            .list_parts(&ListPartsRequest {
                upload: multipart_object_request(
                    TRACE_BUCKET,
                    TRACE_KEY,
                    upload_id,
                    test_requester(),
                ),
                part_number_marker: None,
                max_parts: 100,
            })
            .unwrap_err();
        prop_assert!(
            matches!(err, ServerError::NoSuchUpload { .. }),
            "{}",
            context
        );
    }

    if model.object_visible {
        let result = harness
            .coord
            .get_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request(TRACE_BUCKET, TRACE_KEY, None, test_requester()),
                cond: NO_READ,
            })
            .map_err(|err| {
                TestCaseError::fail(format!("{context}\nexpected visible object, got {err:?}"))
            })?;
        let body = read_all_body(result.body).map_err(|err| {
            TestCaseError::fail(format!("{context}\nfailed reading body: {err:?}"))
        })?;
        prop_assert_eq!(body, TRACE_PART_DATA, "{}", context);
    } else {
        let err = harness
            .coord
            .head_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request(TRACE_BUCKET, TRACE_KEY, None, test_requester()),
                cond: NO_READ,
            })
            .unwrap_err();
        prop_assert!(
            matches!(err, ServerError::ObjectNotFound { .. }),
            "{}",
            context
        );
    }

    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn prop_multipart_session_trace_matches_model(ops in multipart_trace_strategy()) {
        let trace = render_trace(&ops);
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord
            .create_bucket_for_owner("default-owner", TRACE_BUCKET, false)
            .unwrap();
        let mut harness = MultipartTraceHarness::new(coord);
        let mut model = MultipartTraceModel::new();

        let initial_context = format!("initial state\nfull trace:\n{trace}");
        assert_trace_matches_model(&harness, &model, &initial_context)?;

        for (index, op) in ops.iter().enumerate() {
            harness.execute(op);
            model.apply(op);
            let step_context = format!("after step {index}: {op}\nfull trace:\n{trace}");
            assert_trace_matches_model(&harness, &model, &step_context)?;
        }
    }
}

#[test]
fn abort_active_upload_with_live_stream_part_session_removes_upload_and_requires_session_cleanup() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", TRACE_BUCKET, false)
        .unwrap();

    let mut harness = MultipartTraceHarness::new(coord);
    harness.execute(&MultipartTraceOp::CreateUpload);
    harness.execute(&MultipartTraceOp::BeginStreamPart);

    let upload_id = harness.current_upload_id.as_ref().unwrap();
    let session_id = harness.current_session_id.as_ref().unwrap();
    harness
        .coord
        .abort_multipart_upload(&multipart_object_request(
            TRACE_BUCKET,
            TRACE_KEY,
            upload_id,
            test_requester(),
        ))
        .unwrap();
    assert_eq!(harness.pending_upload_count(), 0);
    assert_eq!(harness.active_session_count(), 1);

    let err = harness
        .coord
        .list_parts(&ListPartsRequest {
            upload: multipart_object_request(TRACE_BUCKET, TRACE_KEY, upload_id, test_requester()),
            part_number_marker: None,
            max_parts: 100,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::NoSuchUpload { .. }));

    let err = harness
        .coord
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request(TRACE_BUCKET, TRACE_KEY, None, test_requester()),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::ObjectNotFound { .. }));

    harness
        .coord
        .abort_stream_part_session(
            &trusted_bucket_name(TRACE_BUCKET),
            &trusted_object_key(TRACE_KEY),
            session_id,
        )
        .unwrap();
    assert_eq!(harness.active_session_count(), 0);
}
