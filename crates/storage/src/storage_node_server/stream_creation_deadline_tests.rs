// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::node_client::BuildCreateStreamUploadCommandReq;
use crate::node_runtime::client_facade::LocalStorageNodeClient;
use crate::node_runtime::payload_placement::PayloadPlacementInput;

#[path = "stream_preparation_deadline_tests.rs"]
mod preparation;

struct CreationFixture {
    request: CreateStreamUploadReq,
    upload: MultipartUploadRecord,
    proof: BucketWriteReservationProof,
}

impl CreationFixture {
    fn build_command(
        &self,
        client: &dyn ObjectMutationMetadataNodeClient,
        config: &StorageNodeProcessConfig,
        fence: AdmittedRouteEffectFence,
        append: Option<&StreamUploadSegmentRecord>,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        let pg_id = ObjectMetadataPgId::new_for_test(PgId::new(0));
        if let Some(segment) = append {
            client
                .open_stream_upload_session_metadata_route(
                    config.cluster_epoch,
                    pg_id,
                    &self.request.bucket,
                    &self.request.key,
                    &self.request.session_id,
                )?
                .build_append_command(&self.request.target, segment, fence,
                    PayloadPlacementInput::from_server_config(config))
        } else {
            client
                .open_stream_upload_creation_metadata_route(
                    config.cluster_epoch,
                    pg_id,
                    &self.request.bucket,
                    &self.request.key,
                )?
                .build_create_stream_upload_command(self.build(config, fence))
        }
    }

    fn seed_session(
        &self,
        node: &Arc<SharedStorageNode>,
        config: &StorageNodeProcessConfig,
    ) {
        let client = LocalStorageNodeClient::new(config.node_id, Arc::clone(node));
        let command = self
            .build_command(
                &client,
                config,
                AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
                None,
            )
            .unwrap();
        node.get_pg(0)
            .unwrap()
            .apply_metadata_command_and_record(config.node_id.as_u32(), &command)
            .unwrap();
    }

    fn seed_append(
        &self,
        node: &Arc<SharedStorageNode>,
        config: &StorageNodeProcessConfig,
    ) -> StreamUploadSegmentRecord {
        self.seed_session(node, config);
        let client = LocalStorageNodeClient::new(config.node_id, Arc::clone(node));
        // Metadata-only preflight fixture: no ciphertext is exposed or written.
        let (crc, witness) = crate::types::SegmentShardIntegrityRecord::for_zero_filled_segment(
            EcShape { k: 1, m: 0 },
            self.request.encryption.segment_stored_size(1).unwrap(),
        );
        let expected = {
            let pg = node.get_pg(0).unwrap();
            let session = pg.get_stream_upload(&self.request.session_id).unwrap();
            let generation = pg
                .get_object_generation_reservation(
                    &self.request.bucket,
                    &self.request.key,
                    &self.request.session_id,
                )
                .unwrap();
            match &self.request.target {
                StreamUploadTarget::PutObject => crate::ExpectedStreamAppendState::PutObject {
                    session,
                    generation_id: generation,
                },
                StreamUploadTarget::UploadPart { .. } => crate::ExpectedStreamAppendState::UploadPart {
                    session,
                    upload: Box::new(self.upload.clone()),
                    part_generation_id: generation,
                },
            }
        };
        let (_, mut segment) = client
            .open_stream_upload_session_metadata_route(
                config.cluster_epoch,
                ObjectMetadataPgId::new_for_test(PgId::new(0)),
                &self.request.bucket,
                &self.request.key,
                &self.request.session_id,
            )
            .unwrap()
            .prepare_segment_append_with_expected_append_state(
                &PrepareStreamUploadSegmentAppendReq {
                    session_id: self.request.session_id.clone(),
                    segment_index: 0,
                    size: 1,
                    segment_crc64: crc,
                    payload_crc64: 0,
                },
                Some(&expected),
                AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
                PayloadPlacementInput::from_server_config(config),
            )
            .unwrap();
        segment.shard_integrity = Some(witness);
        segment
    }

    fn new(node: &SharedStorageNode, part: bool) -> Self {
        let bucket = crate::tests::bucket_name("creation-deadline");
        let key = crate::tests::object_key("key");
        let upload_id = crate::tests::multipart_upload_id("deadline");
        let pg = node.get_pg(0).unwrap();
        // Metadata-only builder fixture; no payload is written.
        PgMetadataStore::create_multipart_upload(
            &*pg,
            &CreateMultipartUploadReq {
                upload_id: upload_id.clone(),
                bucket: bucket.clone(),
                key: key.clone(),
                tags: None,
                metadata_blob: crate::SerializedMetadataBlob::default(),
                system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
                initiator: crate::OwnerIdentity::from_principal("owner"),
                owner: crate::OwnerIdentity::from_principal("owner"),
                acl_grants: AclGrants::default(),
                public_read: false,
                object_lock: crate::ObjectLockState::default(),
                checksum: None,
                encryption: crate::tests::metadata_only_test_encryption(),
            },
        )
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
        let upload = pg.get_multipart_upload(&upload_id).unwrap();
        let request = CreateStreamUploadReq {
            session_id: crate::tests::stream_session_id("deadline"),
            bucket: bucket.clone(),
            key: key.clone(),
            target: if part {
                StreamUploadTarget::UploadPart {
                    upload_id,
                    part_number: 1,
                }
            } else {
                StreamUploadTarget::PutObject
            },
            encryption: upload.encryption.clone(),
        };
        let mut proof = test_bucket_write_reservation_proof(bucket, &key);
        proof.operation_kind = if part {
            UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND
        } else {
            PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND
        }
        .to_string();
        Self {
            request,
            upload,
            proof,
        }
    }

    fn build<'a>(
        &'a self,
        config: &'a StorageNodeProcessConfig,
        effect_fence: AdmittedRouteEffectFence,
    ) -> BuildCreateStreamUploadCommandReq<'a> {
        BuildCreateStreamUploadCommandReq {
            effect_fence,
            placement: PayloadPlacementInput::from_server_config(config),
            request: &self.request,
            // A long cleanup horizon must not extend request authority.
            cleanup_after: Some(100_000),
            bucket_write_reservation: &self.proof,
            precondition: if matches!(self.request.target, StreamUploadTarget::PutObject) {
                CreateStreamUploadPrecondition::PutObject {
                    expected_current: None,
                }
            } else {
                CreateStreamUploadPrecondition::UploadPart {
                    expected_upload: &self.upload,
                }
            },
        }
    }
}

#[test]
fn stream_creation_rechecks_captured_deadline_after_pg_lock_gap() {
    stream_command_deadline_matrix(false);
}

#[test]
fn stream_append_builder_rechecks_captured_deadline_after_pg_lock_gap() {
    stream_command_deadline_matrix(true);
}

fn stream_command_deadline_matrix(append: bool) {
    for (remote, server_first, absent_client) in [
        (false, false, false),
        (true, false, false),
        (true, true, false),
        (true, true, true),
    ] {
        for part in [false, true] {
            let time = crate::clock::test_time_override_guard(1_000);
            let tmp = test_util::tempdir();
            let mut config = test_config(&tmp);
            config.default_ec_shape = EcShape { k: 1, m: 0 };
            config.route_map_validity =
                RouteMapValidity::until_ms(if server_first { 5_000 } else { 10_000 }).unwrap();
            private_socket_dir(config.socket_path.parent().unwrap());
            let server = StorageNodeServer::bind(config.clone()).unwrap();
            let node = server.test_storage_node();
            let fixture = CreationFixture::new(&node, part);
            let segment = append.then(|| fixture.seed_append(&node, &config));
            let observe = || {
                let pg = node.get_pg(0).unwrap();
                (
                    pg.metadata_command_checkpoint(config.node_id.as_u32(), config.cluster_epoch)
                        .unwrap(),
                    pg.pending_metadata_command_envelope(
                        config.node_id.as_u32(),
                        config.cluster_epoch,
                    )
                    .unwrap(),
                    pg.max_metadata_command_log_index(config.cluster_epoch)
                        .unwrap(),
                    PgMetadataStore::next_generation_id(
                        &*pg,
                        &fixture.request.bucket,
                        &fixture.request.key,
                    )
                    .unwrap(),
                    pg.list_all_stream_uploads().unwrap(),
                    pg.list_stream_segments(&fixture.request.session_id)
                        .unwrap(),
                )
            };
            let before = observe();
            let fence = if absent_client {
                AdmittedRouteEffectFence::unbounded(config.cluster_epoch)
            } else {
                AdmittedRouteEffectFence::bounded(
                    config.cluster_epoch,
                    if server_first { 10_000 } else { 5_000 },
                    if server_first { 9_000 } else { 4_000 },
                )
            };
            let hook_ran = Arc::new(AtomicBool::new(false));
            let ran = Arc::clone(&hook_ran);
            let serving = if remote {
                Some(thread::spawn(move || {
                    let time = crate::clock::test_time_override_guard(1_000);
                    server.accept_one().unwrap();
                    let control = time.control();
                    LocalStorageNodeClient::test_with_before_stream_command_lock_hook(
                        move || {
                            ran.store(true, Ordering::SeqCst);
                            control.set(4_500);
                        },
                        || server.accept_one().unwrap(),
                    );
                }))
            } else {
                None
            };
            let unix = UnixStorageNodeClient::new(
                config.node_id,
                config.cluster_epoch,
                config.socket_path.clone(),
            );
            let local = LocalStorageNodeClient::new(config.node_id, Arc::clone(&node));
            let client: &dyn ObjectMutationMetadataNodeClient = if remote { &unix } else { &local };
            let build = || fixture.build_command(client, &config, fence, segment.as_ref());
            let positive = build().unwrap();
            assert_eq!(positive.id().log_index().get(), before.2 + 1);
            let error = if remote {
                build().unwrap_err()
            } else {
                let control = time.control();
                let ran = Arc::clone(&hook_ran);
                LocalStorageNodeClient::test_with_before_stream_command_lock_hook(
                    move || {
                        ran.store(true, Ordering::SeqCst);
                        control.set(4_500);
                    },
                    build,
                )
                .unwrap_err()
            };
            assert!(
                hook_ran.load(Ordering::SeqCst),
                "must pass the initial deadline check"
            );
            assert!(
                if remote {
                    matches!(
                        &error,
                        ObjectPgActionError::Store(StoreError::StorageRpc {
                            failure: StorageRpcErrorCode::StaleShardLocation,
                            ..
                        })
                    )
                } else {
                    matches!(
                        &error,
                        ObjectPgActionError::Store(StoreError::RouteMapExpired {
                            valid_until_ms: 5_000,
                            now_ms: 4_500,
                            ..
                        })
                    )
                },
                "remote={remote} part={part} server_first={server_first}: {error:?}"
            );
            if let Some(serving) = serving {
                serving.join().unwrap();
            }
            assert_eq!(observe(), before, "expired creation changed durable state");
            if !append {
                assert!(matches!(
                    node.get_pg(0).unwrap().get_object_generation_reservation(
                        &fixture.request.bucket,
                        &fixture.request.key,
                        &fixture.request.session_id
                    ),
                    Err(MetadataError::ObjectGenerationReservationNotFound { .. })
                ));
            }
        }
    }
}

#[test]
fn tls_stream_creation_rebinds_sender_deadline_across_monotonic_origins() {
    tls_stream_command_deadline_matrix(false);
}

#[test]
fn tls_stream_append_builder_rebinds_sender_deadline_across_monotonic_origins() {
    tls_stream_command_deadline_matrix(true);
}

fn tls_stream_command_deadline_matrix(append: bool) {
    tls_command_deadline_matrix(if append { TlsBuilder::Append } else { TlsBuilder::Create });
}

#[derive(Clone, Copy)]
enum TlsBuilder {
    Create,
    Append,
    DirectGeneration,
}

#[test]
fn tls_generation_builder_carries_proof_and_rebinds_sender_deadline() {
    tls_command_deadline_matrix(TlsBuilder::DirectGeneration);
}

fn tls_command_deadline_matrix(kind: TlsBuilder) {
    let append = matches!(kind, TlsBuilder::Append);
    for part in [false, true] {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.default_ec_shape = EcShape { k: 1, m: 0 };
        config.route_map_validity = RouteMapValidity::until_ms(10_000).unwrap();
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let server = crate::clock::with_time_and_monotonic_override(1_000, 900_000, || {
            PreparedStorageNodeServer::new(config.clone())
                .with_rpc_auth(storage_rpc_server_auth(&credential))
                .with_rpc_listeners(vec![StorageNodeRpcListenerConfig::tls_tcp_with_config(
                    "127.0.0.1:0".parse().unwrap(),
                    storage_rpc_tls_server_config(),
                )])
                .bind()
                .unwrap()
        });
        let node = server.test_storage_node();
        let fixture = CreationFixture::new(&node, part);
        let candidate = PgMetadataStore::next_generation_id(
            &*node.get_pg(0).unwrap(), &fixture.request.bucket, &fixture.request.key,
        ).unwrap();
        let mut direct_proof = fixture.proof.clone();
        direct_proof.operation_kind = PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND.into();
        let segment = append.then(|| fixture.seed_append(&node, &config));
        let before = node
            .get_pg(0)
            .unwrap()
            .metadata_command_checkpoint(config.node_id.as_u32(), config.cluster_epoch)
            .unwrap();
        let address = server.tcp_listener_addr_for_test();
        let serving = thread::spawn(move || {
            for (wall, monotonic) in [(3_500, 902_500), (4_500, 903_500)] {
                crate::clock::with_time_and_monotonic_override(wall, monotonic, || {
                    server.accept_one().unwrap()
                });
            }
        });
        // Sender projects wall cutoff 4,000 from local monotonic deadline
        // 11,500. Server must rebind, not compare against its origin 900,000.
        let build = |late: bool| {
            // Each case owns one connection so accept_one returns before the
            // next receiver clock scope begins; no pooled session spans cases.
            let endpoint = StorageRpcClientEndpoint::tcp_with_config(
                format!("tcp://localhost:{}", address.port()),
                vec![address],
                "localhost",
                storage_rpc_tls_client_config(),
            )
            .unwrap();
            let client = UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
                config.node_id,
                config.cluster_epoch,
                endpoint,
                LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
                Some(storage_rpc_client_auth(credential.clone(), 9)),
            );
            // Keep the authenticated response within the allowed 1,000 ms
            // future skew while both sender samples still project cutoff 4,000.
            crate::clock::with_time_and_monotonic_override(
                if late { 3_500 } else { 2_500 },
                if late { 11_000 } else { 10_000 },
                || {
                    if matches!(kind, TlsBuilder::DirectGeneration) {
                        return client.open_object_generation_metadata_route(
                            config.cluster_epoch, ObjectMetadataPgId::new_for_test(PgId::new(0)),
                            &fixture.request.bucket, &fixture.request.key,
                        )?.build_reserve_put_object_generation_command(
                            &fixture.request.session_id, candidate, &direct_proof,
                            PayloadPlacementInput::from_server_config(&config),
                            AdmittedRouteEffectFence::bounded(config.cluster_epoch, 5_000, 11_500),
                        );
                    }
                    fixture.build_command(
                        &client,
                        &config,
                        AdmittedRouteEffectFence::bounded(config.cluster_epoch, 5_000, 11_500),
                        segment.as_ref(),
                    )
                },
            )
        };
        assert_eq!(
            build(false).unwrap().id().log_index().get(),
            if append { 2 } else { 1 }
        );
        let error = build(true).unwrap_err();
        assert!(
            matches!(
                &error,
                ObjectPgActionError::Store(StoreError::StorageRpc {
                    failure: StorageRpcErrorCode::StaleShardLocation,
                    ..
                })
            ),
            "{error:?}; storage detail: {}",
            match &error {
                ObjectPgActionError::Store(StoreError::StorageRpc { detail, .. }) =>
                    detail.as_str(),
                _ => "not an RPC error",
            }
        );
        serving.join().unwrap();
        assert_eq!(
            node.get_pg(0)
                .unwrap()
                .metadata_command_checkpoint(config.node_id.as_u32(), config.cluster_epoch)
                .unwrap(),
            before
        );
        assert_eq!(
            node.get_pg(0)
                .unwrap()
                .max_metadata_command_log_index(config.cluster_epoch)
                .unwrap(),
            if append { 1 } else { 0 }
        );
    }
}
