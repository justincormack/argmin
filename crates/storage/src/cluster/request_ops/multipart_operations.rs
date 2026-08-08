impl super::StorageCluster {
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn create_put_object_stream_session<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        request: BucketSnapshotRequest,
        action: impl FnMut(
            BucketSnapshot,
            Option<StoredObject>,
        ) -> Result<(T, CreateStreamUploadReq), E>,
    ) -> Result<Result<T, E>, crate::BucketSnapshotLoadFailure> {
        self.create_put_object_stream_session_raw(bucket, key, request, action)
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn create_put_object_stream_session_raw<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        request: BucketSnapshotRequest,
        action: impl FnMut(
            BucketSnapshot,
            Option<StoredObject>,
        ) -> Result<(T, CreateStreamUploadReq), E>,
    ) -> Result<Result<T, E>, BucketSnapshotLoadError> {
        self.create_put_object_stream_session_with_cleanup_deadline_raw(
            bucket, key, request, None, action,
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn create_put_object_stream_session_with_cleanup_deadline_raw<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        request: BucketSnapshotRequest,
        cleanup_after: Option<u64>,
        action: impl FnMut(
            BucketSnapshot,
            Option<StoredObject>,
        ) -> Result<(T, CreateStreamUploadReq), E>,
    ) -> Result<Result<T, E>, BucketSnapshotLoadError> {
        self.create_put_object_stream_session_with_route_validation(
            super::PutObjectMutationEffectRoute {
                bucket_pg_id: self.bucket_metadata_pg(bucket),
                object_pg_id: self.object_metadata_pg(bucket, key),
                bucket,
                key,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            || Ok(()),
            request,
            cleanup_after,
            action,
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn create_put_object_stream_session_with_cleanup_deadline<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        request: BucketSnapshotRequest,
        cleanup_after: Option<u64>,
        action: impl FnMut(
            BucketSnapshot,
            Option<StoredObject>,
        ) -> Result<(T, CreateStreamUploadReq), E>,
    ) -> Result<Result<T, E>, crate::BucketSnapshotLoadFailure> {
        self.create_put_object_stream_session_with_cleanup_deadline_raw(
            bucket,
            key,
            request,
            cleanup_after,
            action,
        )
        .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    pub(super) fn create_put_object_stream_session_with_route_validation<T, E>(
        &self,
        route: super::PutObjectMutationEffectRoute<'_>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        request: BucketSnapshotRequest,
        cleanup_after: Option<u64>,
        mut action: impl FnMut(
            BucketSnapshot,
            Option<StoredObject>,
        ) -> Result<(T, CreateStreamUploadReq), E>,
    ) -> Result<Result<T, E>, BucketSnapshotLoadError> {
        let publisher =
            crate::metadata_command::metadata_command_publisher!(CreatePutObjectStreamSession);
        enum Attempt<T> {
            Complete(T),
            Retry,
        }

        let super::PutObjectMutationEffectRoute {
            bucket_pg_id,
            object_pg_id,
            bucket,
            key,
            effect_fence,
        } = route;
        let pg_id = object_pg_id.pg_id();
        loop {
            require_valid_route()?;
            let applied_commands = self
                .drain_pending_object_metadata_commands_for_publisher_collect(
                    publisher, pg_id, bucket,
                )
                .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
            require_valid_route()?;
            let reservation = match self.acquire_durable_bucket_write_reservation_with_effect_fence(
                bucket,
                PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
                Some(key.as_str()),
                Some(effect_fence),
            ) {
                Ok(reservation) => reservation,
                Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteDraining)) => {
                    self.wait_for_durable_bucket_write_drain(bucket)?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let proof = BucketWriteReservationProof::from(&reservation.record);
            let mut disposition = super::BucketWriteReservationDisposition::ReleaseByCaller;
            let result = (|| {
                require_valid_route()?;
                let metadata_route = reservation.node.open_bucket_metadata_route(
                    self.operation_epoch(),
                    bucket_pg_id,
                    bucket,
                )?;
                let snapshot = metadata_route.load_bucket_snapshot(request)?;

                let mutation_client = self.object_mutation_metadata_primary_client(bucket, key)?;
                let stream_creation_route = mutation_client
                    .open_stream_upload_creation_metadata_route(
                        self.operation_epoch(),
                        object_pg_id,
                        bucket,
                        key,
                    )
                    .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
                require_valid_route()?;
                let current_object = mutation_client
                    .open_object_delete_metadata_route(
                        self.operation_epoch(),
                        object_pg_id,
                        bucket,
                        key,
                    )
                    .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?
                    .load_current_object_delete_snapshot()
                    .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
                let existing_object = match current_object.stored.as_ref() {
                    Some(StoredObject::Live(record)) => Some(StoredObject::Live(record.clone())),
                    Some(StoredObject::DeleteMarker(_)) | None => None,
                };

                let (value, create) = match action(snapshot, existing_object) {
                    Ok(prepared) => prepared,
                    Err(error) => return Ok(Err(error)),
                };
                if create.bucket != *bucket || create.key != *key {
                    return Err(BucketSnapshotLoadError::Store(
                        StoreError::RouteCapabilitySubjectMismatch {
                            operation: "create put object stream session",
                        },
                    ));
                }
                require_valid_route()?;
                if stream_creation_route
                    .matching_stream_upload_exists(
                        &create,
                        super::applied_stream_create_command(
                            &applied_commands,
                            &create,
                            cleanup_after,
                        ),
                    )
                    .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?
                {
                    return Ok(Ok(Attempt::Complete(value)));
                }
                self.reserve_put_object_generation_with_route_validation(
                    route,
                    &create.session_id,
                    &mut require_valid_route,
                )
                .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;

                #[cfg(test)]
                maybe_run_before_stream_put_create_command_id_hook(
                    self.metadata_command_apply_test_hook_scope_id(),
                );
                if let Err(error) = require_valid_route() {
                    let _ =
                        self.release_object_generation_reservation(bucket, key, &create.session_id);
                    return Err(error.into());
                }
                let command = match stream_creation_route.build_create_stream_upload_command(
                    BuildCreateStreamUploadCommandReq {
                        request: &create,
                        cleanup_after,
                        precondition: CreateStreamUploadPrecondition::PutObject {
                            expected_current: current_object.stored.as_ref(),
                            require_generation_reservation: true,
                        },
                        bucket_write_reservation: &proof,
                    },
                ) {
                    Ok(command) => command,
                    Err(ObjectPgActionError::StaleObjectReadSubject) => {
                        let cleanup = self.release_object_generation_reservation(
                            bucket,
                            key,
                            &create.session_id,
                        );
                        if let Err(cleanup_error) = cleanup {
                            return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                                cleanup_error,
                            ));
                        }
                        return Ok(Ok(Attempt::Retry));
                    }
                    Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                        ..
                    })) => {
                        let cleanup = self
                            .drain_one_pending_object_metadata_command(publisher, pg_id, bucket)
                            .and_then(|_| {
                                self.release_object_generation_reservation(
                                    bucket,
                                    key,
                                    &create.session_id,
                                )
                            });
                        if let Err(cleanup_error) = cleanup {
                            return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                                cleanup_error,
                            ));
                        }
                        return Ok(Ok(Attempt::Retry));
                    }
                    Err(error) => {
                        let _ = self.release_object_generation_reservation(
                            bucket,
                            key,
                            &create.session_id,
                        );
                        return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                            error,
                        ));
                    }
                };
                #[cfg(any(test, feature = "test-hooks"))]
                maybe_run_before_stream_put_create_pending_install_hook(
                    self.metadata_command_apply_test_hook_scope_id(),
                );
                if let Err(error) = require_valid_route() {
                    let _ =
                        self.release_object_generation_reservation(bucket, key, &create.session_id);
                    return Err(error.into());
                }
                self.maybe_run_before_metadata_command_pending_install_hook();
                let install = match self.install_snapshot_sensitive_metadata_command_or_drain(
                    publisher,
                    pg_id,
                    bucket,
                    &command,
                    Some(effect_fence),
                ) {
                    Ok(install) => install,
                    Err(error) => {
                        let _ = self.release_object_generation_reservation(
                            bucket,
                            key,
                            &create.session_id,
                        );
                        return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                            error,
                        ));
                    }
                };
                match install {
                    super::SnapshotSensitiveInstallOutcome::Installed => {}
                    super::SnapshotSensitiveInstallOutcome::ContenderDrained => {
                        if let Err(cleanup_error) = self.release_object_generation_reservation(
                            bucket,
                            key,
                            &create.session_id,
                        ) {
                            return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                                cleanup_error,
                            ));
                        }
                        return Ok(Ok(Attempt::Retry));
                    }
                }
                if let Err(error) =
                    self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)
                {
                    match self.pending_metadata_command_uses_bucket_write_reservation(
                        pg_id, bucket, &proof,
                    ) {
                        Ok(true) => {
                            disposition =
                                super::BucketWriteReservationDisposition::TransferredToCommand;
                        }
                        Ok(false) => {
                            let _ = self.release_object_generation_reservation(
                                bucket,
                                key,
                                &create.session_id,
                            );
                        }
                        Err(lookup_error) => {
                            disposition = super::BucketWriteReservationDisposition::PreserveForOwnershipCheckFailure;
                            return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                                lookup_error,
                            ));
                        }
                    }
                    return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                        error,
                    ));
                }

                disposition = super::BucketWriteReservationDisposition::TransferredToCommand;
                Ok(Ok(Attempt::Complete(value)))
            })();
            let release_result = match disposition {
                super::BucketWriteReservationDisposition::TransferredToCommand => Ok(()),
                super::BucketWriteReservationDisposition::PreserveForOwnershipCheckFailure => {
                    Ok(())
                }
                super::BucketWriteReservationDisposition::ReleaseByCaller => {
                    self.release_durable_bucket_write_reservation(reservation)
                }
            };
            let attempt = Self::finish_bucket_write_snapshot_operation(result, release_result)?;
            match attempt {
                Ok(Attempt::Complete(value)) => return Ok(Ok(value)),
                Ok(Attempt::Retry) => continue,
                Err(error) => return Ok(Err(error)),
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn finalize_put_object_stream<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
        total_size: u64,
        action: impl FnMut(StreamPutFinalizeSnapshot) -> Result<PreparedStreamPutCommit<T>, E>,
    ) -> Result<Result<FinalizeStreamPutOutcome<T>, E>, ObjectPgActionError> {
        self.finalize_put_object_stream_with_route_validation(
            super::PutObjectMutationEffectRoute {
                bucket_pg_id: self.bucket_metadata_pg(bucket),
                object_pg_id: self.object_metadata_pg(bucket, key),
                bucket,
                key,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            session_id,
            total_size,
            || Ok(()),
            action,
        )
    }

    pub(super) fn finalize_put_object_stream_with_route_validation<T, E>(
        &self,
        route: super::PutObjectMutationEffectRoute<'_>,
        session_id: &SessionId,
        total_size: u64,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        mut action: impl FnMut(StreamPutFinalizeSnapshot) -> Result<PreparedStreamPutCommit<T>, E>,
    ) -> Result<Result<FinalizeStreamPutOutcome<T>, E>, ObjectPgActionError> {
        let publisher =
            crate::metadata_command::metadata_command_publisher!(FinalizePutObjectStream);
        let super::PutObjectMutationEffectRoute {
            object_pg_id,
            bucket,
            key,
            effect_fence,
            ..
        } = route;
        let pg_id = object_pg_id.pg_id();
        let mutation_client = self.object_mutation_metadata_primary_client(bucket, key)?;
        let finalization_route = mutation_client.open_stream_put_finalization_metadata_route(
            self.operation_epoch(),
            object_pg_id,
            bucket,
            key,
            session_id,
        )?;
        let mut stale_snapshot_work_budget =
            super::RequestWorkBudget::new(super::STREAM_PUT_STALE_COMMIT_RETRY_BUDGET, None)
                .for_operation("finalize_stream_put")
                .for_pg(pg_id);
        let mut pending_work_budget =
            super::RequestWorkBudget::new(super::BUCKET_WRITE_DRAIN_RETRY_BUDGET, None)
                .for_operation("finalize_stream_put_pending")
                .for_pg(pg_id);

        let (command, new_pending_command, prepared) = loop {
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            pending_work_budget
                .check("stream PUT finalization pending retry budget exhausted")
                .map_err(ObjectPgActionError::Store)?;
            if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                let is_matching_stream_commit = matches!(
                    command.payload(),
                    MetadataCommandPayload::CommitDirectPutObject(commit)
                        if commit.matches_stream_session(bucket, key, session_id)
                );
                if !is_matching_stream_commit {
                    self.drain_pending_object_metadata_command(publisher, pg_id, &command)?;
                    pending_work_budget
                        .sleep_after_contention(
                            "stream PUT finalization pending drain retry budget exhausted",
                        )
                        .map_err(ObjectPgActionError::Store)?;
                    continue;
                }
            }

            let pending_command = self
                .pending_metadata_command_for_bucket(pg_id, bucket)?
                .filter(|command| {
                    matches!(
                        command.payload(),
                        MetadataCommandPayload::CommitDirectPutObject(commit)
                            if commit.matches_stream_session(bucket, key, session_id)
                    )
                });

            let storage_snapshot = match finalization_route.load_snapshot() {
                Ok(snapshot) => snapshot,
                Err(
                    error @ ObjectPgActionError::Metadata(MetadataError::StreamSessionNotFound {
                        ..
                    }),
                ) => {
                    if let Some(command) = pending_command.clone() {
                        self.apply_exact_pending_object_metadata_command(
                            pg_id,
                            super::ExactPendingObjectMetadataCommand::for_checked_request(&command),
                        )?;
                        continue;
                    }
                    return Err(error);
                }
                Err(error) => return Err(error),
            };
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            let Some(effective_bucket_write_reservation) =
                storage_snapshot.session.bucket_write_reservation.as_ref()
            else {
                return Err(ObjectPgActionError::InvalidRequest {
                    reason: "PutObject stream session is missing bucket write proof".to_string(),
                });
            };
            if pending_command.as_ref().is_some_and(|command| {
                matches!(
                    command.payload(),
                    MetadataCommandPayload::CommitDirectPutObject(commit)
                        if commit.bucket_write_reservation != *effective_bucket_write_reservation
                )
            }) {
                return Err(ObjectPgActionError::InvalidRequest {
                    reason: "pending stream PUT commit reservation proof does not match the durable stream session"
                        .to_string(),
                });
            }
            let prepared = match action(StreamPutFinalizeSnapshot {
                session: storage_snapshot.session.clone(),
                existing_etag: storage_snapshot.existing_etag.clone(),
            }) {
                Ok(prepared) => prepared,
                Err(error) => return Ok(Err(error)),
            };

            let (command, new_pending_command) = match pending_command {
                Some(command) => (command, false),
                None => {
                    let version_id = if prepared.versioning == BucketVersioningState::Enabled {
                        self.reserve_next_object_version_for_completion_with_effect_fence(
                            pg_id,
                            bucket,
                            key,
                            effect_fence,
                            &mut require_valid_route,
                        )?
                    } else {
                        VersionId::Null
                    };
                    let commit = StreamPutCommitInput {
                        versioning: prepared.versioning,
                        version_id,
                        owner: prepared.owner.clone(),
                        acl_grants: prepared.acl_grants.clone(),
                        public_read: prepared.public_read,
                        etag_crc64: prepared.etag_crc64,
                        tags: prepared.tags.clone(),
                        metadata_blob: prepared.metadata_blob.clone(),
                        system_metadata_blob: prepared.system_metadata_blob.clone(),
                        object_lock: prepared.object_lock,
                        encryption: prepared.encryption.clone(),
                    };
                    #[cfg(any(test, feature = "test-hooks"))]
                    maybe_run_before_stream_put_finalize_command_id_hook(
                        self.metadata_command_apply_test_hook_scope_id(),
                    );
                    require_valid_route().map_err(ObjectPgActionError::Store)?;
                    let command = match finalization_route.build_commit_command(
                        BuildStreamPutCommitCommandReq {
                            total_size,
                            expected_snapshot: &storage_snapshot,
                            commit: &commit,
                            bucket_write_reservation: effective_bucket_write_reservation,
                        },
                        effect_fence,
                    ) {
                        Ok(command) => command,
                        Err(ObjectPgActionError::StaleStreamFinalizeSnapshot) => {
                            if let Err(error) = stale_snapshot_work_budget.sleep_after_contention(
                                "stream PUT stale commit snapshot retry budget exhausted",
                            ) {
                                return Err(ObjectPgActionError::Store(error));
                            }
                            continue;
                        }
                        Err(ObjectPgActionError::Store(
                            StoreError::MetadataCommandLogConflict { .. },
                        )) => {
                            self.drain_one_pending_object_metadata_command(
                                publisher, pg_id, bucket,
                            )?;
                            continue;
                        }
                        Err(error) => return Err(error),
                    };
                    require_valid_route().map_err(ObjectPgActionError::Store)?;
                    match self.install_terminal_session_retry_metadata_command(
                        publisher,
                        pg_id,
                        bucket,
                        &command,
                        Some(effect_fence),
                        |pending| {
                            matches!(
                                pending.payload(),
                                MetadataCommandPayload::CommitDirectPutObject(commit)
                                    if commit.matches_stream_session(bucket, key, session_id)
                            )
                        },
                    )? {
                        super::TerminalSessionRetryInstallOutcome::Installed => {}
                        super::TerminalSessionRetryInstallOutcome::MatchingContenderVisible(_)
                        | super::TerminalSessionRetryInstallOutcome::UnrelatedContenderVisible
                        | super::TerminalSessionRetryInstallOutcome::ContentionWithoutVisibleCommand => {
                            continue;
                        }
                    }
                    (command, true)
                }
            };
            break (command, new_pending_command, prepared);
        };

        if new_pending_command {
            self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
        } else {
            self.apply_exact_pending_object_metadata_command(
                pg_id,
                super::ExactPendingObjectMetadataCommand::for_checked_request(&command),
            )?;
        }

        let MetadataCommandPayload::CommitDirectPutObject(commit) = command.payload() else {
            unreachable!("stream put commit pending command kind changed");
        };
        Ok(Ok(FinalizeStreamPutOutcome {
            value: prepared.value,
            version_id: commit.object.version_id,
            encryption: commit.object.encryption.clone(),
            live_tags: commit.object.tags.clone(),
            live_size: commit.object.size,
            live_last_modified: commit.last_modified_millis,
            stale_generation_id: super::object_payload_reclaim_generation(&commit.stale_payload),
        }))
    }

    #[cfg(test)]
    pub(crate) fn create_multipart_upload<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        request: BucketSnapshotRequest,
        mut action: impl FnMut(
            BucketSnapshot,
            Option<StoredObject>,
        ) -> Result<(T, CreateMultipartUploadReq), E>,
    ) -> Result<Result<CreateMultipartUploadOutcome<T>, E>, BucketSnapshotLoadError> {
        self.create_multipart_upload_inner_with_route_validation(
            super::MultipartObjectMutationEffectRoute {
                pg_id: self.object_metadata_pg(bucket, key),
                bucket,
                key,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            || Ok(()),
            request,
            |snapshot, existing| {
                action(snapshot, existing).map(|(value, create)| {
                    (
                        value,
                        MultipartUploadCreatePreparation::Durable {
                            request: create,
                            ordered_id_key: None,
                        },
                    )
                })
            },
        )
    }

    #[cfg(test)]
    pub(crate) fn create_multipart_upload_with_ordered_id<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        request: BucketSnapshotRequest,
        mut action: impl FnMut(
            BucketSnapshot,
            Option<StoredObject>,
        )
            -> Result<(T, CreateMultipartUploadReq, MultipartUploadIdKey), E>,
    ) -> Result<Result<CreateMultipartUploadOutcome<T>, E>, BucketSnapshotLoadError> {
        self.create_multipart_upload_inner_with_route_validation(
            super::MultipartObjectMutationEffectRoute {
                pg_id: self.object_metadata_pg(bucket, key),
                bucket,
                key,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            || Ok(()),
            request,
            |snapshot, existing| {
                action(snapshot, existing).map(|(value, create, upload_id_key)| {
                    (
                        value,
                        MultipartUploadCreatePreparation::Durable {
                            request: create,
                            ordered_id_key: Some(upload_id_key),
                        },
                    )
                })
            },
        )
    }

    pub(super) fn create_multipart_upload_with_ordered_id_with_route_validation<T, E>(
        &self,
        route: super::MultipartObjectMutationEffectRoute<'_>,
        require_valid_route: impl FnMut() -> Result<(), StoreError>,
        request: BucketSnapshotRequest,
        mut action: impl FnMut(
            BucketSnapshot,
            Option<StoredObject>,
        ) -> Result<(T, CreateMultipartUploadInput), E>,
    ) -> Result<Result<CreateMultipartUploadOutcome<T>, E>, BucketSnapshotLoadError> {
        self.create_multipart_upload_inner_with_route_validation(
            route,
            require_valid_route,
            request,
            |snapshot, existing| {
                action(snapshot, existing).map(|(value, input)| {
                    (value, MultipartUploadCreatePreparation::Authorized(input))
                })
            },
        )
    }

    fn create_multipart_upload_inner_with_route_validation<T, E>(
        &self,
        route: super::MultipartObjectMutationEffectRoute<'_>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        request: BucketSnapshotRequest,
        mut action: impl FnMut(
            BucketSnapshot,
            Option<StoredObject>,
        ) -> Result<(T, MultipartUploadCreatePreparation), E>,
    ) -> Result<Result<CreateMultipartUploadOutcome<T>, E>, BucketSnapshotLoadError> {
        let publisher = crate::metadata_command::metadata_command_publisher!(CreateMultipartUpload);
        enum Attempt<T> {
            Complete(CreateMultipartUploadOutcome<T>),
            Retry,
        }

        let super::MultipartObjectMutationEffectRoute {
            pg_id: object_pg_id,
            bucket,
            key,
            effect_fence,
        } = route;
        let pg_id = object_pg_id.pg_id();
        let mut authorized_issuance = AuthorizedMultipartUploadCreateIssuance::default();
        loop {
            require_valid_route()?;
            let applied_commands = self
                .drain_pending_object_metadata_commands_for_publisher_collect(
                    publisher, pg_id, bucket,
                )
                .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
            require_valid_route()?;
            let reservation = match self.acquire_durable_bucket_write_reservation_with_effect_fence(
                bucket,
                CREATE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
                Some(key.as_str()),
                Some(effect_fence),
            ) {
                Ok(reservation) => reservation,
                Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteDraining)) => {
                    self.wait_for_durable_bucket_write_drain(bucket)?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let proof = BucketWriteReservationProof::from(&reservation.record);
            let mut disposition = super::BucketWriteReservationDisposition::ReleaseByCaller;
            let result = (|| {
                require_valid_route()?;
                let metadata_route = reservation.node.open_bucket_metadata_route(
                    self.operation_epoch(),
                    self.validated_bucket_metadata_pg(PgId::new(reservation.pg_id)),
                    bucket,
                )?;
                let snapshot = metadata_route.load_bucket_snapshot(request)?;
                let upload_id_key = snapshot.bucket.multipart_upload_id_key.clone();

                let mutation_client = self.object_mutation_metadata_primary_client(bucket, key)?;
                let multipart_creation_route = mutation_client
                    .open_multipart_upload_creation_metadata_route(
                        self.operation_epoch(),
                        object_pg_id,
                        bucket,
                        key,
                    )
                    .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
                require_valid_route()?;
                let current_object = mutation_client
                    .open_object_delete_metadata_route(
                        self.operation_epoch(),
                        object_pg_id,
                        bucket,
                        key,
                    )
                    .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?
                    .load_current_object_delete_snapshot()
                    .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
                let existing_object = match current_object.stored.as_ref() {
                    Some(StoredObject::Live(record)) => Some(StoredObject::Live(record.clone())),
                    Some(StoredObject::DeleteMarker(_)) | None => None,
                };

                let (value, preparation) = match action(snapshot, existing_object) {
                    Ok(prepared) => prepared,
                    Err(error) => return Ok(Err(error)),
                };
                let (create, ordered_id_key) = match preparation {
                    #[cfg(test)]
                    MultipartUploadCreatePreparation::Durable {
                        request,
                        ordered_id_key,
                    } => (request, ordered_id_key),
                    MultipartUploadCreatePreparation::Authorized(input) => {
                        let create = authorized_issuance
                            .prepare(&upload_id_key, bucket, key, input)
                            .map_err(BucketSnapshotLoadError::Store)?;
                        self.maybe_run_after_multipart_create_upload_id_prepared_hook(
                            &create.upload_id,
                        );
                        (create, Some(upload_id_key))
                    }
                };
                if create.bucket != *bucket || create.key != *key {
                    return Err(BucketSnapshotLoadError::Store(
                        StoreError::RouteCapabilitySubjectMismatch {
                            operation: "create multipart upload",
                        },
                    ));
                }
                require_valid_route()?;
                let applied_create =
                    super::applied_multipart_create_command(&applied_commands, &create);
                if let Some(initiated_at) = multipart_creation_route
                    .matching_multipart_upload_initiated_at(&create, applied_create)
                    .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?
                {
                    return Ok(Ok(Attempt::Complete(CreateMultipartUploadOutcome {
                        value,
                        upload_id: applied_create.map_or_else(
                            || create.upload_id.clone(),
                            |command| command.upload.upload_id.clone(),
                        ),
                        initiated_at,
                    })));
                }

                let mut command = match multipart_creation_route
                    .build_create_multipart_upload_command(BuildCreateMultipartUploadCommandReq {
                        request: &create,
                        expected_current: current_object.stored.as_ref(),
                        bucket_write_reservation: &proof,
                    }) {
                    Ok(command) => command,
                    Err(ObjectPgActionError::StaleObjectReadSubject) => {
                        return Ok(Ok(Attempt::Retry));
                    }
                    Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                        ..
                    })) => {
                        self.drain_one_pending_object_metadata_command(publisher, pg_id, bucket)
                            .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
                        return Ok(Ok(Attempt::Retry));
                    }
                    Err(error) => {
                        return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                            error,
                        ));
                    }
                };
                if let Some(upload_id_key) = ordered_id_key {
                    let MetadataCommandPayload::CreateMultipartUpload(provisional_command) =
                        command.payload()
                    else {
                        unreachable!("multipart create command changed payload kind");
                    };
                    let ordered_upload_id = upload_id_key.with_listing_position(
                        &create.bucket,
                        &create.key,
                        &create.upload_id,
                        command.id().cluster_epoch().get(),
                        command.id().log_index().get(),
                    );
                    let mut ordered_command = provisional_command.as_ref().clone();
                    ordered_command.upload.upload_id = ordered_upload_id;
                    command = MetadataCommandEnvelope::new(
                        command.id(),
                        MetadataCommandPayload::CreateMultipartUpload(Box::new(ordered_command)),
                    );
                }
                self.maybe_run_before_multipart_create_command_install_hook(&command);
                require_valid_route()?;
                match self
                    .install_snapshot_sensitive_metadata_command_or_drain(
                        publisher,
                        pg_id,
                        bucket,
                        &command,
                        Some(effect_fence),
                    )
                    .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?
                {
                    super::SnapshotSensitiveInstallOutcome::Installed => {}
                    super::SnapshotSensitiveInstallOutcome::ContenderDrained => {
                        return Ok(Ok(Attempt::Retry));
                    }
                }
                if let Err(error) =
                    self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)
                {
                    match self.pending_metadata_command_uses_bucket_write_reservation(
                        pg_id, bucket, &proof,
                    ) {
                        Ok(true) => {
                            disposition =
                                super::BucketWriteReservationDisposition::TransferredToCommand;
                        }
                        Ok(false) => {}
                        Err(lookup_error) => {
                            disposition = super::BucketWriteReservationDisposition::PreserveForOwnershipCheckFailure;
                            return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                                lookup_error,
                            ));
                        }
                    }
                    return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                        error,
                    ));
                }
                disposition = super::BucketWriteReservationDisposition::TransferredToCommand;

                let MetadataCommandPayload::CreateMultipartUpload(create_command) =
                    command.payload()
                else {
                    unreachable!("multipart create command changed payload kind");
                };
                let initiated_at = create_command.upload.initiated_at;
                Ok(Ok(Attempt::Complete(CreateMultipartUploadOutcome {
                    value,
                    upload_id: create_command.upload.upload_id.clone(),
                    initiated_at,
                })))
            })();
            let release_result = match disposition {
                super::BucketWriteReservationDisposition::TransferredToCommand => Ok(()),
                super::BucketWriteReservationDisposition::PreserveForOwnershipCheckFailure => {
                    Ok(())
                }
                super::BucketWriteReservationDisposition::ReleaseByCaller => {
                    self.release_durable_bucket_write_reservation(reservation)
                }
            };
            let attempt = Self::finish_bucket_write_snapshot_operation(result, release_result)?;
            match attempt {
                Ok(Attempt::Complete(outcome)) => return Ok(Ok(outcome)),
                Ok(Attempt::Retry) => continue,
                Err(error) => return Ok(Err(error)),
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn load_multipart_upload(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, BucketSnapshotLoadError> {
        let pg_id = self.object_metadata_pg(bucket, key);
        let mutation_client = self.object_mutation_metadata_primary_client(bucket, key)?;
        mutation_client
            .open_multipart_upload_lookup_metadata_route(self.operation_epoch(), pg_id, bucket, key)
            .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?
            .load_multipart_upload(upload_id)
    }

    #[cfg(test)]
    pub(crate) fn begin_upload_part_stream_session<T, E>(
        &self,
        req: BeginUploadPartStreamSessionReq,
        action: impl FnMut(&MultipartUploadRecord) -> Result<(AuthorizedMultipartUploadRecord, T), E>,
    ) -> Result<Result<T, E>, BucketSnapshotLoadError> {
        self.begin_upload_part_stream_session_with_cleanup_deadline(req, None, action)
    }

    #[cfg(test)]
    pub(crate) fn begin_upload_part_stream_session_with_cleanup_deadline<T, E>(
        &self,
        req: BeginUploadPartStreamSessionReq,
        cleanup_after: Option<u64>,
        mut action: impl FnMut(
            &MultipartUploadRecord,
        ) -> Result<(AuthorizedMultipartUploadRecord, T), E>,
    ) -> Result<Result<T, E>, BucketSnapshotLoadError> {
        let publisher =
            crate::metadata_command::metadata_command_publisher!(CreateUploadPartStreamSession);
        let BeginUploadPartStreamSessionReq {
            bucket,
            key,
            upload_id,
            part_number,
            session_id,
            bucket_write_reservation,
        } = req;
        let object_pg_id = self.object_metadata_pg(&bucket, &key);
        let pg_id = object_pg_id.pg_id();
        let mutation_client = self.object_mutation_metadata_primary_client(&bucket, &key)?;
        let multipart_lookup_route = mutation_client
            .open_multipart_upload_lookup_metadata_route(
                self.operation_epoch(),
                object_pg_id,
                &bucket,
                &key,
            )
            .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
        let stream_creation_route = mutation_client
            .open_stream_upload_creation_metadata_route(
                self.operation_epoch(),
                object_pg_id,
                &bucket,
                &key,
            )
            .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
        macro_rules! release_caller_bucket_write_proof {
            () => {{
                self.release_bucket_write_reservation_proof(&bucket_write_reservation)
            }};
        }
        loop {
            let applied_commands = match self
                .drain_pending_object_metadata_commands_for_publisher_collect(
                    publisher, pg_id, &bucket,
                ) {
                Ok(applied_commands) => applied_commands,
                Err(error) => {
                    release_caller_bucket_write_proof!()?;
                    return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                        error,
                    ));
                }
            };
            let upload = match multipart_lookup_route.load_in_progress_multipart_upload(&upload_id)
            {
                Ok(upload) => upload,
                Err(error) => {
                    release_caller_bucket_write_proof!()?;
                    return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                        error,
                    ));
                }
            };
            let (authorized_upload, result) = match action(&upload) {
                Ok((authorized_upload, result)) => (authorized_upload, result),
                Err(error) => {
                    release_caller_bucket_write_proof!()?;
                    return Ok(Err(error));
                }
            };
            if authorized_upload.record() != &upload {
                release_caller_bucket_write_proof!()?;
                return Err(BucketSnapshotLoadError::Metadata(
                    MetadataError::NoSuchUpload {
                        upload_id: upload_id.to_string(),
                    },
                ));
            }
            let create = CreateStreamUploadReq {
                session_id: session_id.clone(),
                bucket: bucket.clone(),
                key: key.clone(),
                target: StreamUploadTarget::UploadPart {
                    upload_id: upload_id.clone(),
                    part_number,
                },
                encryption: upload.encryption.clone(),
            };
            match stream_creation_route.matching_stream_upload_exists(
                &create,
                super::applied_stream_create_command(&applied_commands, &create, cleanup_after),
            ) {
                Ok(true) => {
                    release_caller_bucket_write_proof!()?;
                    return Ok(Ok(result));
                }
                Ok(false) => {}
                Err(error) => {
                    release_caller_bucket_write_proof!()?;
                    return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                        error,
                    ));
                }
            }
            let command = match stream_creation_route.build_create_stream_upload_command(
                BuildCreateStreamUploadCommandReq {
                    request: &create,
                    cleanup_after,
                    precondition: CreateStreamUploadPrecondition::UploadPart {
                        expected_upload: &upload,
                    },
                    bucket_write_reservation: &bucket_write_reservation,
                },
            ) {
                Ok(command) => command,
                Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                    ..
                })) => {
                    if let Err(error) =
                        self.drain_one_pending_object_metadata_command(publisher, pg_id, &bucket)
                    {
                        release_caller_bucket_write_proof!()?;
                        return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                            error,
                        ));
                    }
                    continue;
                }
                Err(error) => {
                    release_caller_bucket_write_proof!()?;
                    return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                        error,
                    ));
                }
            };
            let install_result = self.install_snapshot_sensitive_metadata_command_or_drain(
                publisher, pg_id, &bucket, &command, None,
            );
            match install_result {
                Ok(super::SnapshotSensitiveInstallOutcome::Installed) => {}
                Ok(super::SnapshotSensitiveInstallOutcome::ContenderDrained) => continue,
                Err(error) => {
                    release_caller_bucket_write_proof!()?;
                    return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                        error,
                    ));
                }
            }
            self.apply_new_object_metadata_command_for_bucket(pg_id, &bucket, &command)
                .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
            return Ok(Ok(result));
        }
    }

    #[cfg(test)]
    pub(crate) fn create_upload_part_stream_session(
        &self,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        part_number: u32,
        session_id: &SessionId,
    ) -> Result<SessionId, ObjectPgActionError> {
        self.create_upload_part_stream_session_with_cleanup_deadline(
            authorized_upload,
            part_number,
            session_id,
            None,
        )
    }

    #[cfg(test)]
    pub(crate) fn create_upload_part_stream_session_with_cleanup_deadline(
        &self,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        part_number: u32,
        session_id: &SessionId,
        cleanup_after: Option<u64>,
    ) -> Result<SessionId, ObjectPgActionError> {
        let bucket = &authorized_upload.record().bucket;
        let key = &authorized_upload.record().key;
        self.create_upload_part_stream_session_with_route_validation(
            super::MultipartObjectMutationEffectRoute {
                pg_id: self.object_metadata_pg(bucket, key),
                bucket,
                key,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            authorized_upload,
            part_number,
            session_id,
            cleanup_after,
            || Ok(()),
        )
    }

    pub(super) fn create_upload_part_stream_session_with_route_validation(
        &self,
        route: super::MultipartObjectMutationEffectRoute<'_>,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        part_number: u32,
        session_id: &SessionId,
        cleanup_after: Option<u64>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
    ) -> Result<SessionId, ObjectPgActionError> {
        let publisher =
            crate::metadata_command::metadata_command_publisher!(CreateUploadPartStreamSession);
        let super::MultipartObjectMutationEffectRoute {
            pg_id: object_pg_id,
            bucket,
            key,
            effect_fence,
        } = route;
        if authorized_upload.record().bucket != *bucket || authorized_upload.record().key != *key {
            return Err(ObjectPgActionError::Store(
                StoreError::RouteCapabilitySubjectMismatch {
                    operation: "create UploadPart stream session",
                },
            ));
        }
        let upload_id = &authorized_upload.record().upload_id;
        let pg_id = object_pg_id.pg_id();
        let mutation_client = self.object_mutation_metadata_primary_client(bucket, key)?;
        let multipart_lookup_route = mutation_client.open_multipart_upload_lookup_metadata_route(
            self.operation_epoch(),
            object_pg_id,
            bucket,
            key,
        )?;
        let stream_creation_route = mutation_client.open_stream_upload_creation_metadata_route(
            self.operation_epoch(),
            object_pg_id,
            bucket,
            key,
        )?;
        loop {
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            let reservation = match self.acquire_durable_bucket_write_reservation_with_effect_fence(
                bucket,
                UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
                Some(key.as_str()),
                Some(effect_fence),
            ) {
                Ok(reservation) => reservation,
                Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteDraining)) => {
                    self.wait_for_durable_bucket_write_drain(bucket)
                        .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
                    continue;
                }
                Err(error) => {
                    return Err(super::bucket_snapshot_error_to_object_pg_action_error(
                        error,
                    ))
                }
            };
            let bucket_write_reservation = BucketWriteReservationProof::from(&reservation.record);
            macro_rules! release_caller_bucket_write_proof {
                () => {{
                    self.release_bucket_write_reservation_proof(&bucket_write_reservation)
                        .map_err(super::bucket_snapshot_error_to_object_pg_action_error)
                }};
            }
            if let Err(error) = require_valid_route() {
                release_caller_bucket_write_proof!()?;
                return Err(ObjectPgActionError::Store(error));
            }
            let applied_commands = match self
                .drain_pending_object_metadata_commands_for_publisher_collect(
                    publisher, pg_id, bucket,
                ) {
                Ok(applied_commands) => applied_commands,
                Err(error) => {
                    release_caller_bucket_write_proof!()?;
                    return Err(error);
                }
            };
            if let Err(error) = require_valid_route() {
                release_caller_bucket_write_proof!()?;
                return Err(ObjectPgActionError::Store(error));
            }
            let upload = match multipart_lookup_route.load_in_progress_multipart_upload(upload_id) {
                Ok(upload) => upload,
                Err(error) => {
                    release_caller_bucket_write_proof!()?;
                    return Err(error);
                }
            };
            if upload != *authorized_upload.record() {
                release_caller_bucket_write_proof!()?;
                return Err(MetadataError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                }
                .into());
            }
            let create = CreateStreamUploadReq {
                session_id: session_id.clone(),
                bucket: bucket.clone(),
                key: key.clone(),
                target: StreamUploadTarget::UploadPart {
                    upload_id: upload_id.clone(),
                    part_number,
                },
                encryption: upload.encryption.clone(),
            };
            if let Err(error) = require_valid_route() {
                release_caller_bucket_write_proof!()?;
                return Err(ObjectPgActionError::Store(error));
            }
            match stream_creation_route.matching_stream_upload_exists(
                &create,
                super::applied_stream_create_command(&applied_commands, &create, cleanup_after),
            ) {
                Ok(true) => {
                    release_caller_bucket_write_proof!()?;
                    return Ok(session_id.clone());
                }
                Ok(false) => {}
                Err(error) => {
                    release_caller_bucket_write_proof!()?;
                    return Err(error);
                }
            }
            if let Err(error) = require_valid_route() {
                release_caller_bucket_write_proof!()?;
                return Err(ObjectPgActionError::Store(error));
            }
            let command = match stream_creation_route.build_create_stream_upload_command(
                BuildCreateStreamUploadCommandReq {
                    request: &create,
                    cleanup_after,
                    precondition: CreateStreamUploadPrecondition::UploadPart {
                        expected_upload: &upload,
                    },
                    bucket_write_reservation: &bucket_write_reservation,
                },
            ) {
                Ok(command) => command,
                Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                    ..
                })) => {
                    if let Err(error) =
                        self.drain_one_pending_object_metadata_command(publisher, pg_id, bucket)
                    {
                        release_caller_bucket_write_proof!()?;
                        return Err(error);
                    }
                    release_caller_bucket_write_proof!()?;
                    continue;
                }
                Err(error) => {
                    release_caller_bucket_write_proof!()?;
                    return Err(error);
                }
            };
            if let Err(error) = require_valid_route() {
                release_caller_bucket_write_proof!()?;
                return Err(ObjectPgActionError::Store(error));
            }
            match self.install_snapshot_sensitive_metadata_command_or_drain(
                publisher,
                pg_id,
                bucket,
                &command,
                Some(effect_fence),
            ) {
                Ok(super::SnapshotSensitiveInstallOutcome::Installed) => {}
                Ok(super::SnapshotSensitiveInstallOutcome::ContenderDrained) => {
                    release_caller_bucket_write_proof!()?;
                    continue;
                }
                Err(error) => {
                    release_caller_bucket_write_proof!()?;
                    return Err(error);
                }
            }
            self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
            return Ok(session_id.clone());
        }
    }

    /// Owner-local test access to the complete durable multipart record.
    #[cfg(test)]
    pub(crate) fn load_in_progress_multipart_upload(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError> {
        self.load_in_progress_multipart_upload_with_route_validation(
            super::MultipartObjectMutationEffectRoute {
                pg_id: self.object_metadata_pg(bucket, key),
                bucket,
                key,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            upload_id,
            || Ok(()),
        )
    }

    pub(super) fn load_in_progress_multipart_upload_with_route_validation(
        &self,
        route: super::MultipartObjectMutationEffectRoute<'_>,
        upload_id: &UploadId,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError> {
        let super::MultipartObjectMutationEffectRoute {
            pg_id,
            bucket,
            key,
            effect_fence: _,
        } = route;
        require_valid_route().map_err(ObjectPgActionError::Store)?;
        let mutation_client = self.object_mutation_metadata_primary_client(bucket, key)?;
        mutation_client
            .open_multipart_upload_lookup_metadata_route(
                self.operation_epoch(),
                pg_id,
                bucket,
                key,
            )?
            .load_in_progress_multipart_upload(upload_id)
    }

    #[cfg(feature = "test-hooks")]
    pub(crate) fn try_load_in_progress_multipart_upload(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<Option<MultipartUploadRecord>, ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        self.local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?
            .test_node()
            .try_load_in_progress_multipart_upload(bucket, key, upload_id)
    }

    #[cfg(test)]
    pub(crate) fn load_multipart_completion_snapshot(
        &self,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        requested_part_numbers: &[u32],
    ) -> Result<MultipartCompletionSnapshot, ObjectPgActionError> {
        let bucket = &authorized_upload.record().bucket;
        let key = &authorized_upload.record().key;
        self.load_multipart_completion_snapshot_with_route_validation(
            super::MultipartObjectMutationEffectRoute {
                pg_id: self.object_metadata_pg(bucket, key),
                bucket,
                key,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            authorized_upload,
            requested_part_numbers,
            || Ok(()),
        )
    }

    pub(super) fn load_multipart_completion_snapshot_with_route_validation(
        &self,
        route: super::MultipartObjectMutationEffectRoute<'_>,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        requested_part_numbers: &[u32],
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
    ) -> Result<MultipartCompletionSnapshot, ObjectPgActionError> {
        let super::MultipartObjectMutationEffectRoute {
            pg_id,
            bucket,
            key,
            effect_fence: _,
        } = route;
        if authorized_upload.record().bucket != *bucket || authorized_upload.record().key != *key {
            return Err(ObjectPgActionError::Store(
                StoreError::RouteCapabilitySubjectMismatch {
                    operation: "load multipart completion snapshot",
                },
            ));
        }
        require_valid_route().map_err(ObjectPgActionError::Store)?;
        self.object_mutation_metadata_primary_client(bucket, key)?
            .open_authorized_multipart_upload_metadata_route(
                self.operation_epoch(),
                pg_id,
                authorized_upload,
            )?
            .load_multipart_completion_snapshot(requested_part_numbers)
    }

    fn complete_multipart_outcome_from_command(
        command: &CommitMultipartObjectCommand,
    ) -> CompleteMultipartCommitOutcome {
        CompleteMultipartCommitOutcome {
            version_id: command.object.version_id,
            stale_payload_generation_id: super::object_payload_reclaim_generation(
                &command.stale_payload,
            ),
            live_tags: command.object.tags.clone(),
            live_size: command.object.size,
            live_last_modified: command.last_modified_millis,
        }
    }

    pub(super) fn complete_multipart_command_cleanup(
        command: &CommitMultipartObjectCommand,
    ) -> CompleteMultipartCommitCleanup {
        CompleteMultipartCommitCleanup {
            omitted_parts: command.omitted_parts.clone(),
            omitted_streaming_segments: command.omitted_streaming_segments.clone(),
            stream_uploads: command.stream_uploads.clone(),
            stream_upload_segments: command.stream_upload_segments.clone(),
        }
    }

    /// Replicate the bucket-write dependency before publishing completion on the object PG.
    ///
    /// The returned sequence is only an idempotence token for the bucket-PG command; replay
    /// semantics are stored with the completed object version. A barrier already pending on
    /// entry is drained as contention and never satisfies the current reservation, because the
    /// command intentionally carries no reservation identity.
    fn establish_multipart_completion_barrier(
        &self,
        bucket: &BucketName,
        completion_target_context: &str,
        bucket_write_reservation: &BucketWriteReservationProof,
        effect_fence: Option<AdmittedRouteEffectFence>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<u64, ObjectPgActionError> {
        let publisher = crate::metadata_command::metadata_command_publisher!(
            EstablishMultipartCompletionBarrier
        );
        let pg_id = PgId::new(self.bucket_metadata_pg_id(bucket));
        let bucket_metadata_client = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?
            .bucket_metadata_client()
            .clone();
        let metadata_route = bucket_metadata_client
            .open_bucket_metadata_route(
                self.operation_epoch(),
                self.validated_bucket_metadata_pg(pg_id),
                bucket,
            )
            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
        loop {
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            work_budget.check("multipart completion barrier reservation budget exhausted")?;
            if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                if self
                    .drain_unrelated_pending_metadata_command_for_bucket_with_work_budget(
                        pg_id,
                        bucket,
                        &command,
                        work_budget,
                    )
                    .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?
                {
                    continue;
                }
                if let MetadataCommandPayload::AdvanceMultipartCompletionBarrier(_) =
                    command.payload()
                {
                    match self
                        .drain_bucket_pg_pending_metadata_command_with_work_budget(
                            pg_id,
                            &command,
                            false,
                            work_budget,
                        )
                        .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?
                    {
                        super::PendingMetadataCommandOutcome::Applied => continue,
                        super::PendingMetadataCommandOutcome::RetryPartialExactConflict => {
                            return Err(super::conflicting_pending_object_metadata_command(
                                "retryable partial pending multipart completion barrier command",
                            ));
                        }
                        super::PendingMetadataCommandOutcome::Abandoned => continue,
                    }
                }
                match self.finish_pending_command_for_multipart_completion_barrier(
                    pg_id,
                    &command,
                    work_budget,
                )? {
                    super::PendingMetadataCommandOutcome::Applied => continue,
                    super::PendingMetadataCommandOutcome::RetryPartialExactConflict => {
                        return Err(super::conflicting_pending_object_metadata_command(
                            "retryable partial pending multipart completion barrier dependency command",
                        ));
                    }
                    super::PendingMetadataCommandOutcome::Abandoned => continue,
                }
            }

            #[cfg(test)]
            maybe_run_before_multipart_completion_barrier_command_id_hook(
                self.metadata_command_apply_test_hook_scope_id(),
            );
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            let Some(command_id) = self
                .next_completion_bucket_metadata_command_id_or_drain_with_work_budget(
                    pg_id,
                    bucket,
                    work_budget,
                )
                .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?
            else {
                continue;
            };
            let (barrier_sequence, command) = metadata_route
                .build_advance_multipart_completion_barrier_command(
                    command_id,
                    completion_target_context,
                    bucket_write_reservation,
                )
                .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
            match self
                .install_allocator_cleanup_bucket_pg_command_or_retry(
                    publisher,
                    pg_id,
                    bucket,
                    &command,
                    effect_fence,
                    work_budget,
                )
                .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?
            {
                super::AllocatorCleanupPendingInstallOutcome::Installed => {}
                super::AllocatorCleanupPendingInstallOutcome::RetryAfterContention => continue,
            }
            match self.finish_pending_metadata_command_to_acting_set_with_work_budget(
                pg_id,
                &command,
                true,
                work_budget,
            ) {
                Ok(super::PendingMetadataCommandOutcome::Applied) => return Ok(barrier_sequence),
                Ok(
                    super::PendingMetadataCommandOutcome::Abandoned
                    | super::PendingMetadataCommandOutcome::RetryPartialExactConflict,
                ) => continue,
                Err(error) => {
                    return Err(super::bucket_snapshot_error_to_object_pg_action_error(
                        error,
                    ))
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn test_establish_multipart_completion_barrier(
        &self,
        bucket: &BucketName,
    ) -> Result<u64, ObjectPgActionError> {
        let mut work_budget = super::RequestWorkBudget::new(
            std::time::Duration::from_millis(METADATA_COMMAND_APPLY_RETRY_BUDGET_MILLIS),
            None,
        )
        .for_operation("test_multipart_completion_barrier")
        .for_pg(PgId::new(self.bucket_metadata_pg_id(bucket)));
        let reservation = self
            .acquire_completion_durable_bucket_write_reservation(
                bucket,
                COMPLETE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
                Some("test-completed-multipart-order"),
            )
            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
        let proof = BucketWriteReservationProof::from(&reservation.record);
        let result = self.establish_multipart_completion_barrier(
            bucket,
            "test-completed-multipart-order",
            &proof,
            None,
            || Ok(()),
            &mut work_budget,
        );
        let release = self
            .release_durable_bucket_write_reservation(reservation)
            .map_err(super::bucket_snapshot_error_to_object_pg_action_error);
        match (result, release) {
            (Ok(order), Ok(())) => Ok(order),
            (Ok(_), Err(error)) | (Err(error), Ok(())) | (Err(error), Err(_)) => Err(error),
        }
    }

    fn apply_multipart_completion_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), ObjectPgActionError> {
        let mut command = command.clone();
        loop {
            match self.apply_metadata_command_to_acting_set(&command) {
                Ok(()) => {
                    self.release_metadata_command_bucket_write_reservation(&command)
                        .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
                    self.remove_pending_metadata_command_for_bucket(pg_id, bucket, &command)
                        .map_err(ObjectPgActionError::from)?;
                    self.after_object_metadata_command_applied(&command);
                    return Ok(());
                }
                Err(error)
                    if matches!(
                        self.retryable_partial_exact_metadata_command_conflict_applied_on_all_nodes(
                            pg_id,
                            &command,
                            error.applied_nodes,
                            &error.source,
                        )
                        .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?,
                        Some(true)
                    ) =>
                {
                    self.release_metadata_command_bucket_write_reservation(&command)
                        .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
                    self.remove_pending_metadata_command_for_bucket(pg_id, bucket, &command)
                        .map_err(ObjectPgActionError::from)?;
                    self.after_object_metadata_command_applied(&command);
                    return Ok(());
                }
                Err(error)
                    if matches!(
                        self.retryable_partial_exact_metadata_command_conflict_applied_on_all_nodes(
                            pg_id,
                            &command,
                            error.applied_nodes,
                            &error.source,
                        )
                        .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?,
                        Some(false)
                    ) =>
                {
                    return Err(super::conflicting_pending_object_metadata_command(
                        "retryable partial multipart completion command conflict",
                    ));
                }
                Err(error)
                    if error.applied_nodes == 0
                        && super::StorageCluster::metadata_command_log_conflict_matches(
                            &command,
                            &error.source,
                        ) =>
                {
                    let Some(reissued) = self
                        .reissue_pending_metadata_command(pg_id, &command)
                        .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?
                    else {
                        return Err(super::conflicting_pending_object_metadata_command(
                            "pending multipart completion command was displaced during reissue",
                        ));
                    };
                    command = reissued;
                }
                Err(error) => {
                    return Err(super::bucket_snapshot_error_to_object_pg_action_error(
                        error.source,
                    ))
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn complete_multipart_upload_commit_serialized(
        &self,
        req: CompleteMultipartCommitRequest,
    ) -> Result<CompleteMultipartCommitOutcome, ObjectPgActionError> {
        let bucket = req.bucket.clone();
        let key = req.key.clone();
        self.complete_multipart_upload_commit_serialized_with_route_validation(
            super::MultipartObjectMutationEffectRoute {
                pg_id: self.object_metadata_pg(&bucket, &key),
                bucket: &bucket,
                key: &key,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            req,
            || Ok(()),
        )
    }

    pub(super) fn complete_multipart_upload_commit_serialized_with_route_validation(
        &self,
        route: super::MultipartObjectMutationEffectRoute<'_>,
        mut req: CompleteMultipartCommitRequest,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
    ) -> Result<CompleteMultipartCommitOutcome, ObjectPgActionError> {
        let publisher = crate::metadata_command::metadata_command_publisher!(
            CompleteMultipartUploadCommitSerialized
        );
        let super::MultipartObjectMutationEffectRoute {
            pg_id: object_pg_id,
            bucket: route_bucket,
            key: route_key,
            effect_fence,
        } = route;
        if req.bucket != *route_bucket || req.key != *route_key {
            return Err(ObjectPgActionError::Store(
                StoreError::RouteCapabilitySubjectMismatch {
                    operation: "complete multipart upload",
                },
            ));
        }
        let bucket = req.bucket.clone();
        let key = req.key.clone();
        let upload_id = req.upload_id.clone();
        let generation_id = req.generation_id;
        let pg_id = object_pg_id.pg_id();
        let mutation_client = self.object_mutation_metadata_primary_client(&bucket, &key)?;
        let multipart_lookup_route = mutation_client.open_multipart_upload_lookup_metadata_route(
            self.operation_epoch(),
            object_pg_id,
            &bucket,
            &key,
        )?;
        let multipart_completion_route = mutation_client
            .open_multipart_completion_mutation_metadata_route(
                self.operation_epoch(),
                object_pg_id,
                &bucket,
                &key,
            )?;
        let mut work_budget = super::RequestWorkBudget::new(
            std::time::Duration::from_millis(METADATA_COMMAND_APPLY_RETRY_BUDGET_MILLIS),
            None,
        )
        .for_operation("complete_multipart_upload_commit")
        .for_pg(pg_id);

        'retry_after_pending_conflict: loop {
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            work_budget.check("complete multipart commit budget exhausted")?;
            let reservation = match self
                .acquire_completion_durable_bucket_write_reservation_with_effect_fence(
                    &bucket,
                    COMPLETE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
                    Some(key.as_str()),
                    effect_fence,
                ) {
                Ok(reservation) => reservation,
                Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteDraining)) => {
                    self.wait_for_durable_bucket_write_drain(&bucket)
                        .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
                    continue;
                }
                Err(error) => {
                    return Err(super::bucket_snapshot_error_to_object_pg_action_error(
                        error,
                    ));
                }
            };
            let bucket_write_reservation = BucketWriteReservationProof::from(&reservation.record);
            macro_rules! release_bucket_write_proof {
                () => {{
                    self.release_bucket_write_reservation_proof(&bucket_write_reservation)
                        .map_err(super::bucket_snapshot_error_to_object_pg_action_error)
                }};
            }

            loop {
                if let Err(error) = require_valid_route() {
                    release_bucket_write_proof!()?;
                    return Err(ObjectPgActionError::Store(error));
                }
                let command = match self.pending_metadata_command_for_bucket(pg_id, &bucket) {
                    Ok(Some(command)) => command,
                    Ok(None) => break,
                    Err(error) => {
                        release_bucket_write_proof!()?;
                        return Err(error.into());
                    }
                };
                if let MetadataCommandPayload::CommitMultipartObject(commit) = command.payload() {
                    if commit.matches_request(
                        &bucket,
                        &key,
                        &upload_id,
                        generation_id,
                        req.completion_fingerprint,
                        &req.part_records,
                    ) {
                        let outcome = Self::complete_multipart_outcome_from_command(commit);
                        release_bucket_write_proof!()?;
                        self.apply_multipart_completion_command(pg_id, &bucket, &command)?;
                        return Ok(outcome);
                    }
                }
                if let Err(error) =
                    self.drain_pending_object_metadata_command(publisher, pg_id, &command)
                {
                    release_bucket_write_proof!()?;
                    return Err(error);
                }
            }

            if req.part_records.is_empty() {
                release_bucket_write_proof!()?;
                return Err(MetadataError::InvariantViolation {
                    context: "complete multipart command empty parts",
                    reason: "multipart completion requires at least one part".into(),
                }
                .into());
            }

            if req.conditional_completion {
                if let Err(error) = require_valid_route() {
                    release_bucket_write_proof!()?;
                    return Err(ObjectPgActionError::Store(error));
                }
                let upload =
                    match multipart_lookup_route.load_in_progress_multipart_upload(&upload_id) {
                        Ok(upload) => upload,
                        Err(error) => {
                            release_bucket_write_proof!()?;
                            return Err(error);
                        }
                    };
                if req.expected_current_object_identity != upload.initiated_object_identity {
                    release_bucket_write_proof!()?;
                    return Err(ObjectPgActionError::MultipartConditionalRequestConflict);
                }
            }

            if req.versioning != BucketVersioningState::Enabled {
                if let Err(error) = require_valid_route() {
                    release_bucket_write_proof!()?;
                    return Err(ObjectPgActionError::Store(error));
                }
                #[cfg(test)]
                maybe_run_multipart_completion_stale_retry_hook(
                    self.metadata_command_apply_test_hook_scope_id(),
                    MultipartCompletionStaleRetryTestEvent::BeforeStalePayloadSourceLoad,
                    &upload_id,
                );
                match multipart_completion_route.load_stale_payload_source() {
                    Ok(current_stale_payload_source) => {
                        req.expected_stale_payload_source = current_stale_payload_source;
                    }
                    Err(error) => {
                        release_bucket_write_proof!()?;
                        return Err(error);
                    }
                }
            }

            let version_id = if req.versioning == BucketVersioningState::Enabled {
                match self.reserve_next_object_version_for_completion_with_effect_fence(
                    pg_id,
                    &bucket,
                    &key,
                    effect_fence,
                    &mut require_valid_route,
                ) {
                    Ok(version_id) => version_id,
                    Err(error) => {
                        release_bucket_write_proof!()?;
                        return Err(error);
                    }
                }
            } else {
                VersionId::Null
            };
            // Replicate the bucket-write dependency before the object-PG commit. This advances
            // one fixed-size scalar and never allocates or retains per-upload terminal records.
            if let Err(error) = self.establish_multipart_completion_barrier(
                &bucket,
                key.as_str(),
                &bucket_write_reservation,
                Some(effect_fence),
                &mut require_valid_route,
                &mut work_budget,
            ) {
                release_bucket_write_proof!()?;
                return Err(error);
            }
            let expected_object_parts = complete_multipart_expected_object_parts(
                &req,
                version_id,
                self.local_map.pg_topology(),
            );
            if let Err(error) = require_valid_route() {
                release_bucket_write_proof!()?;
                return Err(ObjectPgActionError::Store(error));
            }
            let command = match multipart_completion_route.build_complete_multipart_object_command(
                BuildCompleteMultipartObjectCommandReq {
                    request: &req,
                    version_id,
                    expected_object_parts: &expected_object_parts,
                    bucket_write_reservation: &bucket_write_reservation,
                },
            ) {
                Ok(command) => command,
                Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                    ..
                })) => {
                    if let Err(error) =
                        self.drain_one_pending_object_metadata_command(publisher, pg_id, &bucket)
                    {
                        release_bucket_write_proof!()?;
                        return Err(error);
                    }
                    release_bucket_write_proof!()?;
                    continue 'retry_after_pending_conflict;
                }
                Err(ObjectPgActionError::StaleMultipartCompletionSnapshot)
                    if version_id.is_null() =>
                {
                    #[cfg(test)]
                    maybe_run_multipart_completion_stale_retry_hook(
                        self.metadata_command_apply_test_hook_scope_id(),
                        MultipartCompletionStaleRetryTestEvent::AfterStaleCommandBuild,
                        &upload_id,
                    );
                    if let Err(error) = require_valid_route() {
                        release_bucket_write_proof!()?;
                        return Err(ObjectPgActionError::Store(error));
                    }
                    #[cfg(test)]
                    maybe_run_multipart_completion_stale_retry_hook(
                        self.metadata_command_apply_test_hook_scope_id(),
                        MultipartCompletionStaleRetryTestEvent::BeforeStalePayloadSourceLoad,
                        &upload_id,
                    );
                    let current_stale_payload_source =
                        match multipart_completion_route.load_stale_payload_source() {
                            Ok(source) => source,
                            Err(error) => {
                                release_bucket_write_proof!()?;
                                return Err(error);
                            }
                        };
                    release_bucket_write_proof!()?;
                    if current_stale_payload_source == req.expected_stale_payload_source {
                        return Err(ObjectPgActionError::StaleMultipartCompletionSnapshot);
                    }
                    req.expected_stale_payload_source = current_stale_payload_source;
                    continue 'retry_after_pending_conflict;
                }
                Err(error) => {
                    release_bucket_write_proof!()?;
                    return Err(error);
                }
            };
            // A matching completion contender carries the exact outcome this caller must return.
            // Preserve it for the retry loop instead of draining it generically and losing that
            // request-shaped result.
            match self.install_matching_outcome_retry_metadata_command(
                publisher,
                pg_id,
                &bucket,
                &command,
                Some(effect_fence),
                |pending| {
                    matches!(
                        pending.payload(),
                        MetadataCommandPayload::CommitMultipartObject(commit)
                            if commit.matches_request(
                                &bucket,
                                &key,
                                &upload_id,
                                generation_id,
                                req.completion_fingerprint,
                                &req.part_records,
                            )
                    )
                },
            ) {
                Ok(super::MatchingOutcomeRetryInstallOutcome::Installed) => {}
                Ok(super::MatchingOutcomeRetryInstallOutcome::MatchingContenderVisible(
                    pending,
                )) => {
                    let pending_owns_proof = matches!(
                        pending.payload(),
                        MetadataCommandPayload::CommitMultipartObject(commit)
                            if commit.bucket_write_reservation == bucket_write_reservation
                    );
                    if !pending_owns_proof {
                        release_bucket_write_proof!()?;
                    }
                    continue 'retry_after_pending_conflict;
                }
                Ok(
                    super::MatchingOutcomeRetryInstallOutcome::UnrelatedContenderVisible
                    | super::MatchingOutcomeRetryInstallOutcome::ContentionWithoutVisibleCommand,
                ) => {
                    release_bucket_write_proof!()?;
                    continue 'retry_after_pending_conflict;
                }
                Err(error) => {
                    release_bucket_write_proof!()?;
                    return Err(error);
                }
            }
            self.apply_multipart_completion_command(pg_id, &bucket, &command)?;

            let MetadataCommandPayload::CommitMultipartObject(commit) = command.payload() else {
                unreachable!("new complete multipart command changed payload kind");
            };
            return Ok(Self::complete_multipart_outcome_from_command(commit));
        }
    }

    fn commit_stream_part_commands_match_retry(
        pending: &CommitStreamPartCommand,
        candidate: &CommitStreamPartCommand,
    ) -> bool {
        let mut adjusted = candidate.clone();
        adjusted.part.last_modified = pending.part.last_modified;
        pending == &adjusted
    }

    #[cfg(test)]
    pub(crate) fn finalize_upload_part_stream<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        input: StreamPartFinalizeInput<'_>,
        action: impl FnMut(StreamPartFinalizeSnapshot) -> Result<PreparedStreamPartCommit<T>, E>,
    ) -> Result<Result<FinalizeStreamPartOutcome<T>, E>, ObjectPgActionError> {
        self.finalize_upload_part_stream_with_route_validation(
            super::MultipartObjectMutationEffectRoute {
                pg_id: self.object_metadata_pg(bucket, key),
                bucket,
                key,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            input,
            || Ok(()),
            action,
        )
    }

    pub(super) fn finalize_upload_part_stream_with_route_validation<T, E>(
        &self,
        route: super::MultipartObjectMutationEffectRoute<'_>,
        input: StreamPartFinalizeInput<'_>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        mut action: impl FnMut(StreamPartFinalizeSnapshot) -> Result<PreparedStreamPartCommit<T>, E>,
    ) -> Result<Result<FinalizeStreamPartOutcome<T>, E>, ObjectPgActionError> {
        let publisher =
            crate::metadata_command::metadata_command_publisher!(FinalizeUploadPartStream);
        let super::MultipartObjectMutationEffectRoute {
            pg_id: object_pg_id,
            bucket,
            key,
            effect_fence,
        } = route;
        let StreamPartFinalizeInput {
            upload_id,
            session_id,
            part_number,
            total_size,
            payload_crc64,
        } = input;
        let pg_id = object_pg_id.pg_id();
        let mutation_client = self.object_mutation_metadata_primary_client(bucket, key)?;
        let finalization_route = mutation_client.open_stream_part_finalization_metadata_route(
            self.operation_epoch(),
            object_pg_id,
            bucket,
            key,
            upload_id,
            session_id,
            part_number,
        )?;
        let mut pending_work_budget =
            super::RequestWorkBudget::new(super::BUCKET_WRITE_DRAIN_RETRY_BUDGET, None)
                .for_operation("finalize_stream_part_pending")
                .for_pg(pg_id);

        loop {
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            pending_work_budget
                .check("stream part finalization pending retry budget exhausted")
                .map_err(ObjectPgActionError::Store)?;
            let mut pending_command = None;
            if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                let is_matching_stream_part_commit = matches!(
                    command.payload(),
                    MetadataCommandPayload::CommitStreamPart(commit)
                        if commit.matches_request(bucket, key, upload_id, session_id, part_number)
                );
                if is_matching_stream_part_commit {
                    pending_command = Some(command);
                } else {
                    self.drain_pending_object_metadata_command(publisher, pg_id, &command)?;
                    pending_work_budget
                        .sleep_after_contention(
                            "stream part finalization pending drain retry budget exhausted",
                        )
                        .map_err(ObjectPgActionError::Store)?;
                    continue;
                }
            }

            let mut bucket_write_proof = None;
            if pending_command.is_none() {
                let reservation = match self
                    .acquire_durable_bucket_write_reservation_with_effect_fence(
                        bucket,
                        UPLOAD_PART_STREAM_FINALIZE_BUCKET_WRITE_OPERATION_KIND,
                        Some(key.as_str()),
                        Some(effect_fence),
                    ) {
                    Ok(reservation) => reservation,
                    Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteDraining)) => {
                        self.wait_for_durable_bucket_write_drain(bucket)
                            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
                        continue;
                    }
                    Err(error) => {
                        return Err(super::bucket_snapshot_error_to_object_pg_action_error(
                            error,
                        ));
                    }
                };
                bucket_write_proof = Some(BucketWriteReservationProof::from(&reservation.record));
            }
            macro_rules! release_bucket_write_proof_if_unowned {
                () => {{
                    if let Some(proof) = &bucket_write_proof {
                        self.release_bucket_write_reservation_proof(proof)
                            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)
                    } else {
                        Ok(())
                    }
                }};
            }

            if let Err(error) = require_valid_route() {
                release_bucket_write_proof_if_unowned!()?;
                return Err(ObjectPgActionError::Store(error));
            }
            let storage_snapshot = match finalization_route.load_snapshot() {
                Ok(snapshot) => snapshot,
                Err(
                    error @ ObjectPgActionError::Metadata(MetadataError::StreamSessionNotFound {
                        ..
                    }),
                ) => {
                    if let Some(command) = pending_command.clone() {
                        self.apply_exact_pending_object_metadata_command(
                            pg_id,
                            super::ExactPendingObjectMetadataCommand::for_checked_request(&command),
                        )?;
                        continue;
                    }
                    release_bucket_write_proof_if_unowned!()?;
                    return Err(error);
                }
                Err(error) => {
                    release_bucket_write_proof_if_unowned!()?;
                    return Err(error);
                }
            };

            let staging_segments = &storage_snapshot.auth_snapshot.staging_segments;
            let Some(staged_size) = staging_segments
                .iter()
                .try_fold(0u64, |total, segment| total.checked_add(segment.size))
            else {
                release_bucket_write_proof_if_unowned!()?;
                return Err(ObjectPgActionError::InvalidRequest {
                    reason: "stream UploadPart staged size exceeds u64".to_string(),
                });
            };
            let staged_payload_crc64 =
                staging_segments
                    .iter()
                    .fold(checksum::crc64::checksum(&[]), |crc64, segment| {
                        checksum::crc64::combine(crc64, segment.payload_crc64, segment.size)
                    });
            let upload = &storage_snapshot.auth_snapshot.upload;
            let prepared = match action(StreamPartFinalizeSnapshot {
                upload_checksum: upload.checksum,
                managed_encryption: upload.encryption.managed_encryption_algorithm(),
                staged_size,
                staged_payload_crc64,
            }) {
                Ok(prepared) => prepared,
                Err(error) => {
                    release_bucket_write_proof_if_unowned!()?;
                    return Ok(Err(error));
                }
            };
            if staged_size != total_size {
                release_bucket_write_proof_if_unowned!()?;
                return Err(ObjectPgActionError::InvalidRequest {
                    reason: format!(
                        "stream UploadPart size mismatch: caller passed {total_size} but staged segments sum to {staged_size}"
                    ),
                });
            }
            if staged_payload_crc64 != payload_crc64 {
                release_bucket_write_proof_if_unowned!()?;
                return Err(ObjectPgActionError::InvalidRequest {
                    reason: format!(
                        "stream UploadPart etag CRC64 mismatch: caller passed {payload_crc64} but staged payload segments combine to {staged_payload_crc64}"
                    ),
                });
            }

            let Some(generation) = storage_snapshot
                .existing_part
                .as_ref()
                .map_or(Some(0), |part| part.generation.checked_add(1))
            else {
                release_bucket_write_proof_if_unowned!()?;
                return Err(ObjectPgActionError::InvalidRequest {
                    reason: "multipart part generation exhausted".to_string(),
                });
            };
            let ec = staging_segments.first().map_or_else(
                || self.default_payload_ec_shape(),
                |segment| EcShape {
                    k: segment.ec_k,
                    m: segment.ec_m,
                },
            );
            let placement_cluster_epoch = staging_segments
                .first()
                .map_or(self.operation_epoch(), |segment| {
                    segment.placement_cluster_epoch
                });
            let part = MultipartPartRecord {
                upload_id: upload_id.clone(),
                part_number,
                generation,
                size: total_size,
                payload_crc64,
                etag: payload_crc64.to_be_bytes().to_vec(),
                etag_kind: EtagKind::Crc64,
                part_vid: GenerationId::new(u64::from(generation) + 1)
                    .expect("multipart part generation must be nonzero"),
                placement_cluster_epoch,
                ec_k: ec.k,
                ec_m: ec.m,
                last_modified: prepared.last_modified,
                checksum: prepared.checksum.clone(),
            };
            let segments = staging_segments
                .iter()
                .map(|segment| MultipartPartSegmentRecord {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    upload_id: upload_id.clone(),
                    version_id: MULTIPART_PART_SEGMENT_STAGING_VERSION_ID.to_u64(),
                    part_number,
                    segment_index: segment.segment_index,
                    size: segment.size,
                    segment_crc64: segment.segment_crc64,
                    segment_okh: segment.segment_okh,
                    segment_vid: segment.segment_vid,
                    data_pg_id: segment.data_pg_id,
                    placement_cluster_epoch: segment.placement_cluster_epoch,
                    ec_k: segment.ec_k,
                    ec_m: segment.ec_m,
                })
                .collect::<Vec<_>>();

            let command_bucket_write_reservation = pending_command
                .as_ref()
                .and_then(|command| match command.payload() {
                    MetadataCommandPayload::CommitStreamPart(commit) => {
                        Some(commit.bucket_write_reservation.clone())
                    }
                    _ => None,
                })
                .or_else(|| bucket_write_proof.clone())
                .expect("stream part commit command must carry a bucket-write proof");
            let expected_command_bucket_write_reservation =
                command_bucket_write_reservation.clone();
            let command_payload = CommitStreamPartCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                session_id: session_id.clone(),
                upload: storage_snapshot.auth_snapshot.upload.clone(),
                part: part.clone(),
                segments: segments.clone(),
                existing_part: storage_snapshot.existing_part.clone(),
                displaced_segments: storage_snapshot.displaced_segments.clone(),
                bucket_write_reservation: command_bucket_write_reservation,
            };
            let command_is_pending = pending_command.is_some();
            let command = if let Some(command) = pending_command {
                let MetadataCommandPayload::CommitStreamPart(pending) = command.payload() else {
                    unreachable!("filtered pending command changed kind");
                };
                if !Self::commit_stream_part_commands_match_retry(pending, &command_payload) {
                    release_bucket_write_proof_if_unowned!()?;
                    return Err(ObjectPgActionError::InvalidRequest {
                        reason: "pending stream part commit does not match retry".to_string(),
                    });
                }
                command
            } else {
                if let Err(error) = require_valid_route() {
                    release_bucket_write_proof_if_unowned!()?;
                    return Err(ObjectPgActionError::Store(error));
                }
                let command = match finalization_route.build_commit_command(
                    BuildStreamPartCommitCommandReq {
                        expected_snapshot: &storage_snapshot,
                        part: &part,
                        segments: &segments,
                        bucket_write_reservation: &expected_command_bucket_write_reservation,
                    },
                    effect_fence,
                ) {
                    Ok(command) => command,
                    Err(ObjectPgActionError::StaleStreamFinalizeSnapshot) => {
                        release_bucket_write_proof_if_unowned!()?;
                        continue;
                    }
                    Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                        ..
                    })) => {
                        if let Err(error) =
                            self.drain_one_pending_object_metadata_command(publisher, pg_id, bucket)
                        {
                            release_bucket_write_proof_if_unowned!()?;
                            return Err(error);
                        }
                        release_bucket_write_proof_if_unowned!()?;
                        continue;
                    }
                    Err(error) => {
                        release_bucket_write_proof_if_unowned!()?;
                        return Err(error);
                    }
                };
                if let Err(error) = require_valid_route() {
                    release_bucket_write_proof_if_unowned!()?;
                    return Err(ObjectPgActionError::Store(error));
                }
                match self.install_terminal_session_retry_metadata_command(
                    publisher,
                    pg_id,
                    bucket,
                    &command,
                    Some(effect_fence),
                    |pending| {
                        matches!(
                            pending.payload(),
                            MetadataCommandPayload::CommitStreamPart(commit)
                                if commit.matches_request(
                                    bucket,
                                    key,
                                    upload_id,
                                    session_id,
                                    part_number,
                                )
                        )
                    },
                ) {
                    Ok(super::TerminalSessionRetryInstallOutcome::Installed) => {}
                    Ok(super::TerminalSessionRetryInstallOutcome::MatchingContenderVisible(
                        pending,
                    )) => {
                        let pending_owns_proof = matches!(
                            pending.payload(),
                            MetadataCommandPayload::CommitStreamPart(commit)
                                if commit.bucket_write_reservation
                                    == expected_command_bucket_write_reservation
                        );
                        if !pending_owns_proof {
                            release_bucket_write_proof_if_unowned!()?;
                        }
                        continue;
                    }
                    Ok(
                        super::TerminalSessionRetryInstallOutcome::UnrelatedContenderVisible
                        | super::TerminalSessionRetryInstallOutcome::ContentionWithoutVisibleCommand,
                    ) => {
                        release_bucket_write_proof_if_unowned!()?;
                        continue;
                    }
                    Err(error) => {
                        release_bucket_write_proof_if_unowned!()?;
                        return Err(error);
                    }
                }
                command
            };

            let MetadataCommandPayload::CommitStreamPart(commit) = command.payload() else {
                unreachable!("stream part pending command kind changed");
            };
            let last_modified = commit.part.last_modified;
            if command_is_pending {
                self.apply_exact_pending_object_metadata_command(
                    pg_id,
                    super::ExactPendingObjectMetadataCommand::for_checked_request(&command),
                )?;
            } else {
                self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
            }
            return Ok(Ok(FinalizeStreamPartOutcome {
                value: prepared.value,
                last_modified,
            }));
        }
    }

    pub(super) fn list_multipart_uploads_for_bucket_with_route_validation(
        &self,
        scan: &super::ObjectMetadataScanRoute<'_>,
        prefix: Option<&ObjectKey>,
        delimiter: Option<&str>,
        key_marker: Option<&ObjectKey>,
        upload_id_marker: Option<&UploadId>,
        max_uploads: u32,
    ) -> Result<ListedBucketMultipartUploads, ObjectPgActionError> {
        let bucket = scan.bucket;
        if max_uploads == 0 {
            return Ok(ListedBucketMultipartUploads::from_storage(
                Vec::new(),
                Vec::new(),
                false,
                None,
            ));
        }

        let fetch_limit = max_uploads.saturating_add(1);
        let prefix = prefix.cloned();
        let key_marker = key_marker.cloned();
        let upload_id_marker = upload_id_marker.cloned();
        let delimiter = delimiter.filter(|delimiter| !delimiter.is_empty());

        if delimiter.is_none() {
            let max = max_uploads as usize;
            let mut smallest = BoundedSmallestRecords::new(max.saturating_add(1));
            for pg_id in self.metadata_pg_ids() {
                scan.require_valid().map_err(ObjectPgActionError::Store)?;
                let resp = self.list_multipart_uploads_page(
                    pg_id,
                    &ListMultipartUploadsReq {
                        bucket: bucket.clone(),
                        prefix: prefix.clone(),
                        page_start: key_marker.clone().map(|key_marker| {
                            ListMultipartUploadsPageStart::After {
                                key_marker,
                                upload_id_marker: upload_id_marker.clone(),
                            }
                        }),
                        max_uploads: fetch_limit,
                    },
                )?;
                #[cfg(any(test, feature = "test-hooks"))]
                self.maybe_run_after_metadata_listing_pg_complete_hook(pg_id);
                for upload in resp.uploads {
                    let order = (
                        upload.key.clone(),
                        multipart_upload_listing_position(&upload),
                        upload.upload_id.clone(),
                    );
                    smallest.insert(order, upload);
                }
            }
            let mut uploads = smallest.into_values();
            let is_truncated = uploads.len() > max;
            uploads.truncate(max);
            let next_marker = uploads
                .last()
                .map(|upload| MultipartUploadListMarker::Upload {
                    key: upload.key.clone(),
                    upload_id: upload.upload_id.clone(),
                });
            return Ok(ListedBucketMultipartUploads::from_storage(
                uploads,
                Vec::new(),
                is_truncated,
                next_marker,
            ));
        }

        let prefix_str = prefix.as_ref().map_or("", ObjectKey::as_str);
        let delimiter = delimiter.expect("checked above");
        let fetch_uploads_page = |cursor: &mut MultipartUploadCursor,
                                  start: Option<ListMultipartUploadsPageStart>|
         -> Result<(), ObjectPgActionError> {
            scan.require_valid().map_err(ObjectPgActionError::Store)?;
            let resp = self.list_multipart_uploads_page(
                cursor.pg_id,
                &ListMultipartUploadsReq {
                    bucket: bucket.clone(),
                    prefix: prefix.clone(),
                    page_start: start,
                    max_uploads: fetch_limit,
                },
            )?;
            #[cfg(any(test, feature = "test-hooks"))]
            self.maybe_run_after_metadata_listing_pg_complete_hook(cursor.pg_id);
            cursor.uploads = resp.uploads;
            cursor.next_index = 0;
            cursor.next_page_start = if resp.is_truncated {
                resp.next_key_marker
                    .map(|key_marker| ListMultipartUploadsPageStart::After {
                        key_marker,
                        upload_id_marker: resp.next_upload_id_marker,
                    })
            } else {
                None
            };
            Ok(())
        };

        let refill_cursor =
            |cursor: &mut MultipartUploadCursor| -> Result<(), ObjectPgActionError> {
                while cursor.current().is_none() {
                    let Some(next_start) = cursor.next_page_start.clone() else {
                        break;
                    };
                    fetch_uploads_page(cursor, Some(next_start))?;
                }
                Ok(())
            };

        let jump_cursor_to = |cursor: &mut MultipartUploadCursor,
                              start: ListMultipartUploadsPageStart|
         -> Result<(), ObjectPgActionError> {
            cursor.uploads.clear();
            cursor.next_index = 0;
            cursor.next_page_start = Some(start);
            refill_cursor(cursor)
        };

        let skip_cursor_prefix = |cursor: &mut MultipartUploadCursor,
                                  common_prefix: &str|
         -> Result<(), ObjectPgActionError> {
            while cursor
                .current()
                .is_some_and(|upload| upload.key.as_str().starts_with(common_prefix))
            {
                cursor.next_index += 1;
                refill_cursor(cursor)?;
            }
            Ok(())
        };

        let initial_start =
            key_marker
                .clone()
                .map(|key_marker| ListMultipartUploadsPageStart::After {
                    key_marker,
                    upload_id_marker,
                });
        let mut cursors = Vec::new();
        for pg_id in self.metadata_pg_ids() {
            let mut cursor = MultipartUploadCursor {
                pg_id,
                uploads: Vec::new(),
                next_index: 0,
                next_page_start: None,
            };
            fetch_uploads_page(&mut cursor, initial_start.clone())?;
            cursors.push(cursor);
        }

        let max = max_uploads as usize;
        let mut uploads = Vec::new();
        let mut common_prefixes = Vec::new();
        let mut is_truncated = false;
        let mut next_marker = None;
        let mut active_common_prefix = key_marker.as_ref().and_then(|marker| {
            let after_prefix = marker.as_str().strip_prefix(prefix_str)?;
            after_prefix
                .ends_with(delimiter)
                .then(|| (marker.clone(), crate::object_key_prefix_upper_bound(marker)))
        });

        while let Some((cursor_index, _)) = cursors
            .iter()
            .enumerate()
            .filter_map(|(cursor_index, cursor)| {
                cursor.current().map(|upload| (cursor_index, upload))
            })
            .min_by(|(left_index, left), (right_index, right)| {
                left.key
                    .cmp(&right.key)
                    .then_with(|| {
                        multipart_upload_listing_position(left)
                            .cmp(&multipart_upload_listing_position(right))
                    })
                    .then_with(|| left.upload_id.cmp(&right.upload_id))
                    .then_with(|| left_index.cmp(right_index))
            })
        {
            let current_key = cursors[cursor_index]
                .current()
                .expect("selected cursor should have a current upload")
                .key
                .clone();
            if let Some((ref common_prefix, ref upper_bound)) = active_common_prefix {
                if current_key.as_str().starts_with(common_prefix.as_str()) {
                    if let Some(upper_bound) = upper_bound.clone() {
                        jump_cursor_to(
                            &mut cursors[cursor_index],
                            ListMultipartUploadsPageStart::At(upper_bound),
                        )?;
                    } else {
                        skip_cursor_prefix(&mut cursors[cursor_index], common_prefix.as_str())?;
                    }
                    continue;
                }
                active_common_prefix = None;
            }

            if let Some(common_prefix) =
                crate::object_key_common_prefix(&current_key, prefix_str, delimiter)
            {
                let upper_bound = crate::object_key_prefix_upper_bound(&common_prefix);
                active_common_prefix = Some((common_prefix.clone(), upper_bound.clone()));
                if key_marker
                    .as_ref()
                    .is_some_and(|marker| common_prefix.as_str() <= marker.as_str())
                {
                    if let Some(upper_bound) = upper_bound {
                        jump_cursor_to(
                            &mut cursors[cursor_index],
                            ListMultipartUploadsPageStart::At(upper_bound),
                        )?;
                    } else {
                        skip_cursor_prefix(&mut cursors[cursor_index], common_prefix.as_str())?;
                    }
                    continue;
                }
                if uploads.len() + common_prefixes.len() >= max {
                    is_truncated = true;
                    break;
                }
                next_marker = Some(MultipartUploadListMarker::CommonPrefix(
                    common_prefix.clone(),
                ));
                common_prefixes.push(common_prefix);
                continue;
            }

            if uploads.len() + common_prefixes.len() >= max {
                is_truncated = true;
                break;
            }
            let current = cursors[cursor_index]
                .current()
                .expect("selected cursor should have a current upload")
                .clone();
            next_marker = Some(MultipartUploadListMarker::Upload {
                key: current.key.clone(),
                upload_id: current.upload_id.clone(),
            });
            uploads.push(current);
            cursors[cursor_index].next_index += 1;
            refill_cursor(&mut cursors[cursor_index])?;
        }

        Ok(ListedBucketMultipartUploads::from_storage(
            uploads,
            common_prefixes,
            is_truncated,
            next_marker,
        ))
    }

    #[cfg(test)]
    pub(crate) fn list_multipart_uploads_for_bucket(
        &self,
        bucket: &BucketName,
        prefix: Option<&ObjectKey>,
        delimiter: Option<&str>,
        key_marker: Option<&ObjectKey>,
        upload_id_marker: Option<&UploadId>,
        max_uploads: u32,
    ) -> Result<ListedBucketMultipartUploads, ObjectPgActionError> {
        let require_valid_route = || Ok(());
        let scan = super::ObjectMetadataScanRoute {
            bucket,
            require_valid_route: &require_valid_route,
        };
        self.list_multipart_uploads_for_bucket_with_route_validation(
            &scan,
            prefix,
            delimiter,
            key_marker,
            upload_id_marker,
            max_uploads,
        )
    }

    pub(super) fn list_multipart_parts_for_authorized_upload_with_route_validation(
        &self,
        route: super::MultipartObjectMutationEffectRoute<'_>,
        authorized_upload: &crate::AuthorizedMultipartUploadListParts,
        part_number_marker: Option<u32>,
        max_parts: u32,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
    ) -> Result<ListedMultipartParts, ObjectPgActionError> {
        let super::MultipartObjectMutationEffectRoute {
            pg_id,
            bucket,
            key,
            effect_fence: _,
        } = route;
        if authorized_upload.record().bucket != *bucket || authorized_upload.record().key != *key {
            return Err(ObjectPgActionError::Store(
                StoreError::RouteCapabilitySubjectMismatch {
                    operation: "list multipart parts",
                },
            ));
        }
        require_valid_route().map_err(ObjectPgActionError::Store)?;
        let internal_authorized_upload =
            AuthorizedMultipartUploadRecord::assume_authorized(authorized_upload.record().clone());
        let read_node = self
            .local_map
            .metadata_pg_read_node(self.operation_epoch(), pg_id.pg_id())?;
        read_node
            .object_read_metadata_client()
            .open_authorized_multipart_upload_read_route(
                self.operation_epoch(),
                pg_id,
                &internal_authorized_upload,
                read_node.authorization(),
            )?
            .list_multipart_parts(part_number_marker, max_parts)
    }

    pub(super) fn lookup_multipart_upload_management_with_route_validation(
        &self,
        route: super::MultipartObjectMutationEffectRoute<'_>,
        upload_id: &UploadId,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
    ) -> Result<MultipartUploadManagementLookup, ObjectPgActionError> {
        let super::MultipartObjectMutationEffectRoute {
            pg_id: object_pg_id,
            bucket,
            key,
            effect_fence: _,
        } = route;
        let pg_id = object_pg_id.pg_id();
        require_valid_route().map_err(ObjectPgActionError::Store)?;
        // A concurrent terminal command may have removed the active upload on
        // part of the acting set before its object-scoped completion replay is
        // visible everywhere. Finish the durable command before classifying
        // the upload for CompleteMultipartUpload or AbortMultipartUpload.
        let route_state = self
            .local_map
            .pg_route(pg_id)
            .expect("validated multipart metadata read route must exist")
            .state();
        if route_state == PgState::Active {
            let mut work_budget =
                super::RequestWorkBudget::new(super::BUCKET_WRITE_DRAIN_RETRY_BUDGET, None)
                    .for_operation("multipart_management_lookup_recovery")
                    .for_pg(pg_id);
            let mut recovery_authority =
                super::MetadataCommandRecoveryDrainAuthority::new(&mut work_budget);
            loop {
                require_valid_route().map_err(ObjectPgActionError::Store)?;
                let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? else {
                    break;
                };
                match self.drain_pending_metadata_command_with_recovery_authority(
                    &mut recovery_authority,
                    pg_id,
                    &command,
                )? {
                    super::PendingMetadataCommandOutcome::Applied
                    | super::PendingMetadataCommandOutcome::Abandoned => {}
                    super::PendingMetadataCommandOutcome::RetryPartialExactConflict => {
                        return Err(super::conflicting_pending_object_metadata_command(
                            "retryable partial pending multipart management lookup command",
                        ));
                    }
                }
            }
        }
        require_valid_route().map_err(ObjectPgActionError::Store)?;
        let read_node = self
            .local_map
            .metadata_pg_read_node(self.operation_epoch(), pg_id)?;
        read_node
            .object_read_metadata_client()
            .open_multipart_upload_read_route(
                self.operation_epoch(),
                object_pg_id,
                bucket,
                key,
                read_node.authorization(),
            )?
            .lookup_multipart_upload_management(upload_id)
    }

    #[cfg(test)]
    pub(crate) fn abort_multipart_upload(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<bool, ObjectPgActionError> {
        let pg_id = self.object_metadata_pg(bucket, key);
        self.abort_multipart_upload_locked(
            pg_id,
            bucket,
            key,
            upload_id,
            AbortMultipartUploadDrainMode::Wait,
            None,
        )
    }

    pub fn abort_multipart_upload_for_lifecycle_sweep(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        expected_bucket_incarnation_generation: u64,
    ) -> Result<bool, crate::LifecycleMutationFailure> {
        self.abort_multipart_upload_for_lifecycle_sweep_raw(
            bucket,
            key,
            upload_id,
            expected_bucket_incarnation_generation,
        )
        .map_err(crate::LifecycleMutationFailure::from_object_pg_action)
    }

    fn abort_multipart_upload_for_lifecycle_sweep_raw(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        expected_bucket_incarnation_generation: u64,
    ) -> Result<bool, ObjectPgActionError> {
        let pg_id = self.object_metadata_pg(bucket, key);
        self.abort_multipart_upload_locked(
            pg_id,
            bucket,
            key,
            upload_id,
            AbortMultipartUploadDrainMode::Stop,
            Some(expected_bucket_incarnation_generation),
        )
    }

    fn abort_multipart_upload_locked(
        &self,
        object_pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        drain_mode: AbortMultipartUploadDrainMode,
        expected_bucket_incarnation_generation: Option<u64>,
    ) -> Result<bool, ObjectPgActionError> {
        let publisher =
            crate::metadata_command::metadata_command_publisher!(AbortMultipartUploadLocked);
        let pg_id = object_pg_id.pg_id();
        let mut work_budget =
            super::RequestWorkBudget::new(super::BUCKET_WRITE_DRAIN_RETRY_BUDGET, None)
                .for_operation("abort_multipart_upload")
                .for_pg(pg_id);
        'retry_after_pending_conflict: loop {
            work_budget
                .check("multipart abort retry budget exhausted")
                .map_err(ObjectPgActionError::Store)?;
            if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                if metadata_command_is_matching_multipart_abort(&command, bucket, key, upload_id) {
                    if expected_bucket_incarnation_generation.is_some_and(|expected| {
                        !metadata_command_matches_bucket_incarnation(&command, expected)
                    }) {
                        return Ok(false);
                    }
                    self.apply_exact_pending_object_metadata_command(
                        pg_id,
                        super::ExactPendingObjectMetadataCommand::for_checked_request(&command),
                    )?;
                    return Ok(true);
                }
                self.drain_pending_object_metadata_command(publisher, pg_id, &command)?;
                work_budget
                    .sleep_after_contention("multipart abort pending drain retry budget exhausted")
                    .map_err(ObjectPgActionError::Store)?;
                continue 'retry_after_pending_conflict;
            }

            let proof = match match expected_bucket_incarnation_generation {
                Some(expected) => self
                    .try_acquire_lifecycle_bucket_write_proof_for_object_metadata_command(
                        bucket,
                        key,
                        ABORT_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
                        expected,
                    )?,
                None => self.try_acquire_bucket_write_proof_for_object_metadata_command(
                    bucket,
                    key,
                    ABORT_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
                    drain_mode == AbortMultipartUploadDrainMode::Wait,
                )?,
            } {
                Some(proof) => proof,
                None if drain_mode == AbortMultipartUploadDrainMode::Wait => {
                    continue 'retry_after_pending_conflict;
                }
                None => return Ok(false),
            };
            let command = match self.prepare_abort_multipart_upload_command(
                object_pg_id,
                bucket,
                key,
                upload_id,
                proof.clone(),
                AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            ) {
                Ok(Some(command)) => command,
                Ok(None) => {
                    self.release_bucket_write_proof_for_object_metadata_command(&proof)?;
                    return Ok(false);
                }
                Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                    ..
                })) => {
                    self.release_bucket_write_proof_for_object_metadata_command(&proof)?;
                    self.drain_one_pending_object_metadata_command(publisher, pg_id, bucket)?;
                    continue 'retry_after_pending_conflict;
                }
                Err(ObjectPgActionError::StaleObjectReadSubject) => {
                    self.release_bucket_write_proof_for_object_metadata_command(&proof)?;
                    continue 'retry_after_pending_conflict;
                }
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(&proof)?;
                    return Err(error);
                }
            };
            let MetadataCommandPayload::AbortMultipartUpload(_) = command.payload() else {
                unreachable!("prepared abort multipart command changed payload kind");
            };
            #[cfg(test)]
            maybe_run_before_abort_multipart_pending_install_hook(
                self.metadata_command_apply_test_hook_scope_id(),
            );
            self.maybe_run_before_metadata_command_pending_install_hook();
            match self.install_terminal_session_retry_metadata_command(
                publisher,
                pg_id,
                bucket,
                &command,
                None,
                |pending| {
                    metadata_command_is_matching_multipart_abort(pending, bucket, key, upload_id)
                },
            ) {
                Ok(super::TerminalSessionRetryInstallOutcome::Installed) => {}
                Ok(super::TerminalSessionRetryInstallOutcome::MatchingContenderVisible(
                    pending,
                )) => {
                    self.release_bucket_write_proof_for_object_metadata_command(&proof)?;
                    if expected_bucket_incarnation_generation.is_some_and(|expected| {
                        !metadata_command_matches_bucket_incarnation(&pending, expected)
                    }) {
                        return Ok(false);
                    }
                    self.apply_exact_pending_object_metadata_command(
                        pg_id,
                        super::ExactPendingObjectMetadataCommand::for_checked_request(&pending),
                    )?;
                    return Ok(true);
                }
                Ok(
                    super::TerminalSessionRetryInstallOutcome::UnrelatedContenderVisible
                    | super::TerminalSessionRetryInstallOutcome::ContentionWithoutVisibleCommand,
                ) => {
                    self.release_bucket_write_proof_for_object_metadata_command(&proof)?;
                    continue 'retry_after_pending_conflict;
                }
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(&proof)?;
                    return Err(error);
                }
            }
            if let Err(error) =
                self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)
            {
                match self
                    .pending_metadata_command_uses_bucket_write_reservation(pg_id, bucket, &proof)
                {
                    Ok(true) => {}
                    Ok(false) => {
                        self.release_bucket_write_proof_for_object_metadata_command(&proof)?;
                    }
                    Err(lookup_error) => return Err(lookup_error),
                }
                return Err(error);
            }
            return Ok(true);
        }
    }

    #[cfg(test)]
    pub(crate) fn abort_authorized_multipart_upload(
        &self,
        authorized_upload: &crate::AuthorizedMultipartUploadAbort,
    ) -> Result<bool, ObjectPgActionError> {
        let bucket = &authorized_upload.record().bucket;
        let key = &authorized_upload.record().key;
        self.abort_authorized_multipart_upload_locked(
            super::MultipartObjectMutationEffectRoute {
                pg_id: self.object_metadata_pg(bucket, key),
                bucket,
                key,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            authorized_upload,
            || Ok(()),
        )
    }

    pub(super) fn abort_authorized_multipart_upload_locked(
        &self,
        route: super::MultipartObjectMutationEffectRoute<'_>,
        authorized_upload: &crate::AuthorizedMultipartUploadAbort,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
    ) -> Result<bool, ObjectPgActionError> {
        let publisher = crate::metadata_command::metadata_command_publisher!(
            AbortAuthorizedMultipartUploadLocked
        );
        let super::MultipartObjectMutationEffectRoute {
            pg_id: object_pg_id,
            bucket,
            key,
            effect_fence,
        } = route;
        if authorized_upload.record().bucket != *bucket || authorized_upload.record().key != *key {
            return Err(ObjectPgActionError::Store(
                StoreError::RouteCapabilitySubjectMismatch {
                    operation: "abort multipart upload",
                },
            ));
        }
        let pg_id = object_pg_id.pg_id();
        let upload_id = &authorized_upload.record().upload_id;
        let mut work_budget =
            super::RequestWorkBudget::new(super::BUCKET_WRITE_DRAIN_RETRY_BUDGET, None)
                .for_operation("abort_authorized_multipart_upload")
                .for_pg(pg_id);
        'retry_after_pending_conflict: loop {
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            work_budget
                .check("authorized multipart abort retry budget exhausted")
                .map_err(ObjectPgActionError::Store)?;
            if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                if metadata_command_is_matching_multipart_abort(&command, bucket, key, upload_id) {
                    self.apply_exact_pending_object_metadata_command(
                        pg_id,
                        super::ExactPendingObjectMetadataCommand::for_checked_request(&command),
                    )?;
                    return Ok(true);
                }
                self.drain_pending_object_metadata_command(publisher, pg_id, &command)?;
                work_budget
                    .sleep_after_contention(
                        "authorized multipart abort pending drain retry budget exhausted",
                    )
                    .map_err(ObjectPgActionError::Store)?;
                continue 'retry_after_pending_conflict;
            }

            require_valid_route().map_err(ObjectPgActionError::Store)?;
            let proof = match self
                .acquire_bucket_write_proof_for_object_metadata_command_with_effect_fence(
                    bucket,
                    key,
                    ABORT_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
                    effect_fence,
                )? {
                Some(proof) => proof,
                None => continue 'retry_after_pending_conflict,
            };
            if let Err(error) = require_valid_route() {
                self.release_bucket_write_proof_for_object_metadata_command(&proof)?;
                return Err(ObjectPgActionError::Store(error));
            }
            let command = match self.prepare_authorized_abort_multipart_upload_command(
                object_pg_id,
                authorized_upload,
                proof.clone(),
                effect_fence,
            ) {
                Ok(Some(command)) => command,
                Ok(None) => {
                    self.release_bucket_write_proof_for_object_metadata_command(&proof)?;
                    return Ok(false);
                }
                Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                    ..
                })) => {
                    self.release_bucket_write_proof_for_object_metadata_command(&proof)?;
                    self.drain_one_pending_object_metadata_command(publisher, pg_id, bucket)?;
                    continue 'retry_after_pending_conflict;
                }
                Err(ObjectPgActionError::StaleObjectReadSubject) => {
                    self.release_bucket_write_proof_for_object_metadata_command(&proof)?;
                    continue 'retry_after_pending_conflict;
                }
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(&proof)?;
                    return Err(error);
                }
            };
            let MetadataCommandPayload::AbortMultipartUpload(_) = command.payload() else {
                unreachable!("prepared abort multipart command changed payload kind");
            };
            #[cfg(test)]
            maybe_run_before_abort_multipart_pending_install_hook(
                self.metadata_command_apply_test_hook_scope_id(),
            );
            self.maybe_run_before_metadata_command_pending_install_hook();
            match self.install_terminal_session_retry_metadata_command(
                publisher,
                pg_id,
                bucket,
                &command,
                Some(effect_fence),
                |pending| {
                    metadata_command_is_matching_multipart_abort(pending, bucket, key, upload_id)
                },
            ) {
                Ok(super::TerminalSessionRetryInstallOutcome::Installed) => {}
                Ok(super::TerminalSessionRetryInstallOutcome::MatchingContenderVisible(
                    pending,
                )) => {
                    self.release_bucket_write_proof_for_object_metadata_command(&proof)?;
                    self.apply_exact_pending_object_metadata_command(
                        pg_id,
                        super::ExactPendingObjectMetadataCommand::for_checked_request(&pending),
                    )?;
                    return Ok(true);
                }
                Ok(
                    super::TerminalSessionRetryInstallOutcome::UnrelatedContenderVisible
                    | super::TerminalSessionRetryInstallOutcome::ContentionWithoutVisibleCommand,
                ) => {
                    self.release_bucket_write_proof_for_object_metadata_command(&proof)?;
                    continue 'retry_after_pending_conflict;
                }
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(&proof)?;
                    return Err(error);
                }
            }
            if let Err(error) =
                self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)
            {
                match self
                    .pending_metadata_command_uses_bucket_write_reservation(pg_id, bucket, &proof)
                {
                    Ok(true) => {}
                    Ok(false) => {
                        self.release_bucket_write_proof_for_object_metadata_command(&proof)?;
                    }
                    Err(lookup_error) => return Err(lookup_error),
                }
                return Err(error);
            }
            return Ok(true);
        }
    }

    fn prepare_abort_multipart_upload_command(
        &self,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        bucket_write_reservation: BucketWriteReservationProof,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError> {
        let mutation_client = self.object_mutation_metadata_primary_client(bucket, key)?;
        let multipart_abort_route = mutation_client.open_multipart_abort_mutation_metadata_route(
            self.operation_epoch(),
            pg_id,
            bucket,
            key,
            upload_id,
        )?;
        let expected_cleanup = multipart_abort_route.load_cleanup()?;
        multipart_abort_route.build_abort_multipart_upload_command(
            crate::node_client::BuildAbortMultipartUploadCommandReq {
                expected_cleanup: expected_cleanup.as_ref(),
                bucket_write_reservation: &bucket_write_reservation,
            },
            effect_fence,
        )
    }

    fn prepare_authorized_abort_multipart_upload_command(
        &self,
        pg_id: ObjectMetadataPgId,
        authorized_upload: &crate::AuthorizedMultipartUploadAbort,
        bucket_write_reservation: BucketWriteReservationProof,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError> {
        let mutation_client = self.object_mutation_metadata_primary_client(
            &authorized_upload.record().bucket,
            &authorized_upload.record().key,
        )?;
        let multipart_abort_route = mutation_client.open_multipart_abort_mutation_metadata_route(
            self.operation_epoch(),
            pg_id,
            &authorized_upload.record().bucket,
            &authorized_upload.record().key,
            &authorized_upload.record().upload_id,
        )?;
        let expected_cleanup = multipart_abort_route.load_cleanup()?;
        if expected_cleanup
            .as_ref()
            .is_some_and(|cleanup| cleanup.upload != *authorized_upload.record())
        {
            return Ok(None);
        }
        multipart_abort_route.build_authorized_abort_multipart_upload_command(
            crate::node_client::BuildAuthorizedAbortMultipartUploadCommandReq {
                authorized_upload,
                expected_cleanup: expected_cleanup.as_ref(),
                bucket_write_reservation: &bucket_write_reservation,
            },
            effect_fence,
        )
    }

    pub fn abort_multipart_upload_if_due<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        expected_bucket_incarnation_generation: u64,
        should_abort: impl FnOnce(Option<&str>, u64) -> Result<bool, E>,
    ) -> Result<Result<bool, E>, crate::LifecycleMutationFailure> {
        self.abort_multipart_upload_if_due_raw(
            bucket,
            key,
            upload_id,
            expected_bucket_incarnation_generation,
            should_abort,
        )
        .map_err(crate::LifecycleMutationFailure::from_object_pg_action)
    }

    pub(crate) fn abort_multipart_upload_if_due_raw<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        expected_bucket_incarnation_generation: u64,
        should_abort: impl FnOnce(Option<&str>, u64) -> Result<bool, E>,
    ) -> Result<Result<bool, E>, ObjectPgActionError> {
        let Some(lifecycle_context) =
            self.load_bucket_lifecycle_context(bucket, expected_bucket_incarnation_generation)?
        else {
            return Ok(Ok(false));
        };
        let BucketLifecycleContext {
            bucket_incarnation_generation,
            raw_lifecycle,
            ..
        } = lifecycle_context;

        let object_pg_id = self.object_metadata_pg(bucket, key);
        let pg_id = object_pg_id.pg_id();
        let mut recovery_work_budget =
            super::RequestWorkBudget::new(super::BUCKET_WRITE_DRAIN_RETRY_BUDGET, None)
                .for_operation("lifecycle_multipart_abort_recovery")
                .for_pg(pg_id);
        let mut recovery_authority =
            super::MetadataCommandRecoveryDrainAuthority::new(&mut recovery_work_budget);
        while let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
            let matching_abort = matches!(
                command.payload(),
                MetadataCommandPayload::AbortMultipartUpload(abort)
                    if abort.bucket == *bucket
                        && abort.key == *key
                        && abort.upload_id == *upload_id
            );
            if matching_abort {
                if !metadata_command_matches_bucket_incarnation(
                    &command,
                    bucket_incarnation_generation,
                ) {
                    return Ok(Ok(false));
                }
                self.apply_exact_pending_object_metadata_command(
                    pg_id,
                    super::ExactPendingObjectMetadataCommand::for_checked_request(&command),
                )?;
                return Ok(Ok(true));
            }
            match self.drain_pending_metadata_command_with_recovery_authority(
                &mut recovery_authority,
                pg_id,
                &command,
            )? {
                super::PendingMetadataCommandOutcome::Applied
                | super::PendingMetadataCommandOutcome::Abandoned => {}
                super::PendingMetadataCommandOutcome::RetryPartialExactConflict => {
                    return Err(super::conflicting_pending_object_metadata_command(
                        "retryable partial pending lifecycle multipart abort command",
                    ));
                }
            }
        }

        let mutation_client = self.object_mutation_metadata_primary_client(bucket, key)?;
        let multipart_lookup_route = mutation_client.open_multipart_upload_lookup_metadata_route(
            self.operation_epoch(),
            object_pg_id,
            bucket,
            key,
        )?;
        let upload = match multipart_lookup_route.load_multipart_upload(upload_id) {
            Ok(upload) => upload,
            Err(BucketSnapshotLoadError::Metadata(MetadataError::NoSuchUpload { .. })) => {
                return Ok(Ok(false));
            }
            Err(error) => {
                return Err(super::bucket_snapshot_error_to_object_pg_action_error(
                    error,
                ))
            }
        };

        if upload.state == UploadState::Aborting {
            return self
                .abort_multipart_upload_locked(
                    object_pg_id,
                    bucket,
                    key,
                    upload_id,
                    AbortMultipartUploadDrainMode::Stop,
                    Some(bucket_incarnation_generation),
                )
                .map(Ok);
        }
        if upload.state != UploadState::InProgress || raw_lifecycle.is_none() {
            return Ok(Ok(false));
        }

        let should_abort = match should_abort(raw_lifecycle.as_deref(), upload.initiated_at) {
            Ok(should_abort) => should_abort,
            Err(error) => return Ok(Err(error)),
        };
        if !should_abort {
            return Ok(Ok(false));
        }

        self.abort_multipart_upload_locked(
            object_pg_id,
            bucket,
            key,
            upload_id,
            AbortMultipartUploadDrainMode::Stop,
            Some(bucket_incarnation_generation),
        )
        .map(Ok)
    }

}
