// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::node_client::{ObjectGenerationMetadataNodeClient, ObjectGenerationMetadataRoute};
use crate::node_runtime::payload_placement::PayloadPlacementInput;
use crate::types::AdmittedRouteEffectFence;
use crate::PgMetadataStore as _;
use crate::{ObjectMetadataPgId, ObjectPgActionError};

type BeforeGenerationRead = Mutex<Option<Box<dyn FnOnce() + Send>>>;

struct InterleavedGenerationClient {
    inner: Arc<dyn ObjectGenerationMetadataNodeClient>,
    before_read: Arc<BeforeGenerationRead>,
}

struct InterleavedGenerationRoute<'a> {
    inner: Box<dyn ObjectGenerationMetadataRoute + 'a>,
    before_read: &'a BeforeGenerationRead,
}

impl ObjectGenerationMetadataNodeClient for InterleavedGenerationClient {
    fn open_object_generation_metadata_route(
        &self,
        epoch: ClusterEpoch,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Box<dyn ObjectGenerationMetadataRoute + '_>, ObjectPgActionError> {
        Ok(Box::new(InterleavedGenerationRoute {
            inner: self
                .inner
                .open_object_generation_metadata_route(epoch, pg_id, bucket, key)?,
            before_read: &self.before_read,
        }))
    }
}

impl ObjectGenerationMetadataRoute for InterleavedGenerationRoute<'_> {
    fn build_reserve_put_object_generation_command(
        &self,
        reservation_id: &crate::SessionId,
        expected_generation: crate::GenerationId,
        proof: &crate::BucketWriteReservationProof,
        placement: PayloadPlacementInput<'_>,
        fence: AdmittedRouteEffectFence,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        self.inner.build_reserve_put_object_generation_command(
            reservation_id,
            expected_generation,
            proof,
            placement,
            fence,
        )
    }

    fn object_generation_reservation(
        &self,
        reservation_id: &crate::SessionId,
    ) -> Result<crate::GenerationId, ObjectPgActionError> {
        // Run after the earlier session/upload reads, before the real durable
        // reservation read. No fake error or production hook is involved.
        let action = self.before_read.lock().unwrap().take();
        if let Some(action) = action {
            action();
        }
        self.inner.object_generation_reservation(reservation_id)
    }

    fn next_object_generation_id(&self) -> Result<crate::GenerationId, ObjectPgActionError> {
        self.inner.next_object_generation_id()
    }
}

#[test]
fn multipart_completion_between_authority_reads_returns_no_such_upload() {
    for during_append in [false, true] {
        check_missing_part_generation(during_append, true);
    }
}

#[test]
fn live_multipart_missing_generation_remains_an_internal_error() {
    for during_append in [false, true] {
        check_missing_part_generation(during_append, false);
    }
}

fn check_missing_part_generation(during_append: bool, complete: bool) {
    let tmp = test_util::tempdir();
    let mut map = LocalClusterMap::open(
        tmp.path(),
        &[NodeId::new(0), NodeId::new(1), NodeId::new(2)],
        &[0],
        EcShape { k: 2, m: 1 },
    )
    .unwrap();
    let before_read: Arc<BeforeGenerationRead> = Arc::new(Mutex::new(None));
    let primary = map.nodes.get_mut(&NodeId::new(0)).unwrap();
    primary.object_generation_metadata_client = Arc::new(InterleavedGenerationClient {
        inner: Arc::clone(&primary.object_generation_metadata_client),
        before_read: Arc::clone(&before_read),
    });
    let map = Arc::new(map);
    let cluster = StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let bucket = crate::tests::bucket_name("part-authority-race");
    let key = crate::tests::object_key("object");
    create_test_bucket(&cluster, &bucket);
    let (mut request, _) =
        seed_streamed_multipart_completion(&cluster, &bucket, &key, "authorityrace");
    let session_id = crate::tests::stream_session_id("replacement");
    let upload = cluster
        .load_in_progress_multipart_upload(&bucket, &key, &request.upload_id)
        .unwrap();
    cluster
        .create_upload_part_stream_session(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload.clone()),
            1,
            &session_id,
        )
        .unwrap();
    let snapshot = cluster
        .load_multipart_completion_snapshot(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
            &[1],
        )
        .unwrap();
    request.expected_stale_payload_source = snapshot.stale_payload_source;
    request.selected_streaming_segments = snapshot.selected_streaming_segments;
    request.expected_cleanup = snapshot.cleanup;
    let upload_id = request.upload_id.clone();
    let handle = crate::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&cluster));
    let admission = handle.admit_current_route().unwrap();
    let route = admission
        .active_multipart_object_route(&bucket, &key)
        .unwrap();
    let encrypted = crate::tests::sse_s3_multipart_segment_test_fixture(b"replacement part", 1, 0);
    let authorize = || {
        route.authorize_stream_part_segment(
            &upload_id,
            &session_id,
            1,
            0,
            b"replacement part".len() as u64,
        )
    };
    let authority = during_append.then(|| authorize().unwrap());
    let initial_session = route.load_stream_session(&session_id).unwrap();
    let ran = Arc::new(AtomicBool::new(false));
    let observed_prefix = Arc::new(Mutex::new(None));
    let hook_ran = Arc::clone(&ran);
    let hook_prefix = Arc::clone(&observed_prefix);
    let hook_cluster = Arc::clone(&cluster);
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_session = session_id.clone();
    *before_read.lock().unwrap() = Some(Box::new(move || {
        if complete {
            hook_cluster
                .complete_multipart_upload_commit_serialized(request)
                .unwrap();
        } else {
            // Owner-local corruption canary: a live upload with no reservation
            // is not evidence that completion won and must not become a 404.
            for node in hook_map.nodes.values() {
                node.storage_node()
                    .get_pg(0)
                    .unwrap()
                    .delete_object_generation_reservation(&hook_bucket, &hook_key, &hook_session)
                    .unwrap();
            }
        }
        let primary = hook_map.node(NodeId::new(0)).unwrap();
        let pg = primary.storage_node().get_pg(0).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &hook_bucket,
                &hook_key,
                &hook_session
            ),
            Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
        ));
        let state = pg.metadata_command_replica_state().unwrap();
        *hook_prefix.lock().unwrap() = Some((state.applied_log_index, state.applied_log_hash));
        hook_ran.store(true, Ordering::SeqCst);
    }));
    let shard_writes = crate::test_support::install_payload_shard_write_attempt_hook(
        &cluster,
        Arc::new(|_| Ok(())),
    );
    let error = if let Some(authority) = authority {
        route
            .append_authorized_stream_part_segment(
                authority,
                encrypted.payload_crc64,
                &encrypted.storage_bytes,
            )
            .unwrap_err()
    } else {
        match authorize() {
            Ok(_) => panic!("missing generation must not mint authority"),
            Err(error) => error,
        }
    };
    assert!(
        ran.load(Ordering::SeqCst),
        "the exact read interleaving must run"
    );
    assert_eq!(
        error.kind(),
        if complete {
            crate::StreamUploadFailureKind::NoSuchUpload
        } else {
            crate::StreamUploadFailureKind::InternalError
        }
    );
    assert_eq!(shard_writes.finish().count(), 0);
    assert!(pending_metadata_command_for_test(&map, PgId::new(0), &bucket).is_none());
    let pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(0)
        .unwrap();
    let state = pg.metadata_command_replica_state().unwrap();
    assert_eq!(
        Some((state.applied_log_index, state.applied_log_hash)),
        *observed_prefix.lock().unwrap()
    );
    if !complete {
        let session = crate::PgMetadataStore::get_stream_upload(&*pg, &session_id).unwrap();
        assert_eq!(session, initial_session);
        assert!(
            crate::PgMetadataStore::list_stream_segments(&*pg, &session_id)
                .unwrap()
                .is_empty()
        );
    }
}
