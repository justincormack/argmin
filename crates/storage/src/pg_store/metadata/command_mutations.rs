// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

impl PgStore {
    fn apply_put_object_metadata_command(
        &self,
        command: &PutObjectMetadataCommand,
    ) -> Result<(), MetadataError> {
        if self.put_object_metadata_command_already_applied(command)? {
            return Ok(());
        }
        self.with_immediate_txn(
            "put object metadata command (begin txn)",
            "put object metadata command (commit txn)",
            |store| store.put_object_metadata_explicit_in_open_txn(&command.object),
        )
    }

    fn put_object_metadata_command_already_applied(
        &self,
        command: &PutObjectMetadataCommand,
    ) -> Result<bool, MetadataError> {
        let stored = match self.get_object_version(
            &command.object.bucket,
            &command.object.key,
            command.object.version_id,
        ) {
            Ok(StoredObject::Live(record)) => record,
            Ok(StoredObject::DeleteMarker(_)) | Err(MetadataError::ObjectNotFound) => {
                return Ok(false);
            }
            Err(error) => return Err(error),
        };
        Ok(command.matches_object(&stored))
    }

    fn put_object_metadata_explicit_in_open_txn(
        &self,
        object: &LiveObjectRecord,
    ) -> Result<(), MetadataError> {
        let stored = match self.get_object_version(&object.bucket, &object.key, object.version_id) {
            Ok(StoredObject::Live(record)) => record,
            Ok(StoredObject::DeleteMarker(_)) => {
                return Err(MetadataError::MethodNotAllowedOnDeleteMarker);
            }
            Err(error) => return Err(error),
        };
        if stored == *object {
            return Ok(());
        }
        if !Self::put_object_metadata_preimage_matches(&stored, object) {
            return Err(MetadataError::InvariantViolation {
                context: "put object metadata command preimage mismatch",
                reason: "metadata state does not satisfy the operation invariant".into(),
            });
        }

        let tags = object.tags.as_ref().map(SerializedTagSet::as_str);
        let (object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) =
            Self::object_lock_sql_values(object.object_lock).map_err(|e| MetadataError::Db {
                context: "put object metadata command (encode object lock)",
                source: e.into(),
            })?;
        let updated = self.execute_cached_metadata(
            "UPDATE objects \
             SET tags = ?1, acl_grants = ?2, public_read = ?3, \
                 object_lock_retention_mode = ?4, object_lock_retain_until = ?5, \
                 object_lock_legal_hold = ?6 \
             WHERE bucket = ?7 AND key = ?8 AND version_id = ?9 AND status = ?10",
            params![
                tags,
                Self::serialize_acl_grants(&object.acl_grants),
                i32::from(object.public_read),
                object_lock_retention_mode,
                object_lock_retain_until,
                object_lock_legal_hold,
                &object.bucket,
                &object.key,
                object.version_id.to_u64() as i64,
                ObjectState::Live as u8,
            ],
            "put object metadata command",
        )?;
        if updated == 0 {
            return Err(MetadataError::ObjectNotFound);
        }
        Ok(())
    }

    fn put_object_metadata_preimage_matches(
        stored: &LiveObjectRecord,
        object: &LiveObjectRecord,
    ) -> bool {
        stored.bucket == object.bucket
            && stored.key == object.key
            && stored.version_id == object.version_id
            && stored.owner == object.owner
            && stored.generation_id == object.generation_id
            && stored.size == object.size
            && stored.etag == object.etag
            && stored.last_modified == object.last_modified
            && stored.became_noncurrent_at == object.became_noncurrent_at
            && stored.storage_class == object.storage_class
            && stored.ec == object.ec
            && stored.layout == object.layout
            && stored.metadata_blob == object.metadata_blob
            && stored.system_metadata_blob == object.system_metadata_blob
            && stored.encryption == object.encryption
    }

    fn create_stream_upload_explicit(
        &self,
        session: &StreamUploadCommandRecord,
        initial_next_segment_vid: GenerationId,
        cleanup_after: Option<u64>,
        bucket_write_reservation: Option<&BucketWriteReservationProof>,
    ) -> Result<(), MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::create_stream_upload_explicit",
            "pg_id={} session_id={:?} bucket={:?} key={:?}",
            self.pg_id,
            session.session_id.as_str(),
            session.bucket.as_str(),
            session.key.as_str()
        );
        let op_kind = session.target.op_kind() as u8;
        let upload_id = session.target.upload_id();
        let part_number = session.target.part_number().map(|n| n as i64);
        let encryption_type = session.encryption.encryption_type() as u8;
        let encryption_state = session.encryption.encode_state();
        let reservation_id = bucket_write_reservation.map(|proof| proof.reservation_id.as_str());
        let owner_token = bucket_write_reservation.map(|proof| proof.owner_token.as_str());
        let cluster_epoch = bucket_write_reservation.map(|proof| proof.cluster_epoch.get() as i64);
        let execution_generation =
            bucket_write_reservation.map(|proof| proof.bucket_execution_generation as i64);
        let incarnation_generation =
            bucket_write_reservation.map(|proof| proof.bucket_incarnation_generation as i64);
        let operation_kind = bucket_write_reservation.map(|proof| proof.operation_kind.as_str());
        let created_at = bucket_write_reservation.map(|proof| proof.created_at as i64);
        let lease_deadline = bucket_write_reservation.map(|proof| proof.lease_deadline as i64);
        let cleanup_after = cleanup_after
            .map(i64::try_from)
            .transpose()
            .map_err(|source| MetadataError::Db {
                context: "create stream upload cleanup deadline",
                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
            })?;
        let target_context =
            bucket_write_reservation.and_then(|proof| proof.target_context.as_deref());
        self.conn
            .execute(
                "INSERT INTO stream_uploads \
                 (session_id, bucket, key, op_kind, upload_id, part_number, state, created_at, cleanup_after, encryption_type, encryption_state, next_segment_vid, \
                  bucket_write_reservation_id, bucket_write_owner_token, bucket_write_cluster_epoch, bucket_write_execution_generation, \
                  bucket_write_incarnation_generation, bucket_write_operation_kind, bucket_write_created_at, bucket_write_lease_deadline, bucket_write_target_context) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21)",
                params![
                    session.session_id,
                    session.bucket,
                    session.key,
                    op_kind,
                    upload_id,
                    part_number,
                    session.state as u8,
                    session.created_at as i64,
                    cleanup_after,
                    encryption_type,
                    encryption_state,
                    initial_next_segment_vid.get() as i64,
                    reservation_id,
                    owner_token,
                    cluster_epoch,
                    execution_generation,
                    incarnation_generation,
                    operation_kind,
                    created_at,
                    lease_deadline,
                    target_context,
                ],
            )
            .map_err(|e| MetadataError::Db {
                context: "create stream upload explicit",
                source: e.into(),
            })?;
        Ok(())
    }

    fn apply_create_stream_upload_command(
        &self,
        command: &CreateStreamUploadCommand,
    ) -> Result<(), MetadataError> {
        self.validate_create_stream_upload_command_target(command)?;
        match self.get_stream_upload(&command.session.session_id) {
            Ok(existing)
                if StreamUploadCommandRecord::from(&existing) == command.session
                    && existing.next_segment_vid == command.initial_next_segment_vid
                    && existing.cleanup_after == command.cleanup_after
                    && stream_upload_bucket_write_reservation_matches_command(
                        &existing, command,
                    ) =>
            {
                Ok(())
            }
            Ok(_) => Err(MetadataError::InvariantViolation {
                context: "create stream upload command existing session mismatch",
                reason: "metadata state does not satisfy the operation invariant".into(),
            }),
            Err(MetadataError::StreamSessionNotFound { .. }) => {
                let bucket_write_reservation = (command.session.target
                    == StreamUploadTarget::PutObject)
                    .then_some(&command.bucket_write_reservation);
                self.create_stream_upload_explicit(
                    &command.session,
                    command.initial_next_segment_vid,
                    command.cleanup_after,
                    bucket_write_reservation,
                )
            }
            Err(error) => Err(error),
        }
    }

    fn validate_create_stream_upload_command_target(
        &self,
        command: &CreateStreamUploadCommand,
    ) -> Result<(), MetadataError> {
        match &command.session.target {
            StreamUploadTarget::PutObject => {
                if command.session.state == StreamUploadState::InProgress {
                    Ok(())
                } else {
                    Err(MetadataError::InvariantViolation {
                        context: "create stream upload command state mismatch",
                        reason: "metadata state does not satisfy the operation invariant".into(),
                    })
                }
            }
            StreamUploadTarget::UploadPart { upload_id, .. } => {
                let upload = self.get_multipart_upload(upload_id)?;
                if upload.bucket != command.session.bucket
                    || upload.key != command.session.key
                    || upload.state != UploadState::InProgress
                {
                    return Err(MetadataError::NoSuchUpload {
                        upload_id: upload_id.to_string(),
                    });
                }
                if upload.encryption != command.session.encryption {
                    return Err(MetadataError::InvariantViolation {
                        context: "create stream upload command upload encryption mismatch",
                        reason: "metadata state does not satisfy the operation invariant".into(),
                    });
                }
                if command.session.state == StreamUploadState::InProgress {
                    Ok(())
                } else {
                    Err(MetadataError::InvariantViolation {
                        context: "create stream upload command state mismatch",
                        reason: "metadata state does not satisfy the operation invariant".into(),
                    })
                }
            }
        }
    }

    fn apply_append_stream_segment_command(
        &self,
        command: &AppendStreamSegmentCommand,
    ) -> Result<(), MetadataError> {
        self.with_immediate_txn(
            "append stream segment command (begin txn)",
            "append stream segment command (commit txn)",
            |store| {
                let session = store.get_stream_upload(&command.segment.session_id)?;
                if session.bucket != command.bucket || session.key != command.key {
                    return Err(MetadataError::InvariantViolation {
                        context: "append stream segment command session binding mismatch",
                        reason: "metadata state does not satisfy the operation invariant".into(),
                    });
                }
                if session.target != command.target {
                    return Err(MetadataError::InvariantViolation {
                        context: "append stream segment command target binding mismatch",
                        reason: "metadata state does not satisfy the operation invariant".into(),
                    });
                }
                if session.state != StreamUploadState::InProgress {
                    return Err(MetadataError::StreamSessionNotInProgress {
                        state: session.state as u8,
                    });
                }
                if let StreamUploadTarget::UploadPart { upload_id, .. } = &session.target {
                    let upload = store.get_multipart_upload(upload_id)?;
                    if upload.bucket != command.bucket
                        || upload.key != command.key
                        || upload.state != UploadState::InProgress
                    {
                        return Err(MetadataError::NoSuchUpload {
                            upload_id: upload_id.to_string(),
                        });
                    }
                }

                let existing = store
                    .list_stream_segments(&command.segment.session_id)?
                    .into_iter()
                    .find(|segment| segment.segment_index == command.segment.segment_index);
                match existing {
                    Some(existing) if existing == command.segment => store
                        .advance_stream_segment_vid_floor(
                            &command.segment.session_id,
                            command.segment.segment_vid,
                        ),
                    Some(_) => Err(MetadataError::StreamSegmentConflict {
                        segment_index: command.segment.segment_index,
                    }),
                    None => {
                        store.append_stream_segment_direct(&command.segment)?;
                        store.advance_stream_segment_vid_floor(
                            &command.segment.session_id,
                            command.segment.segment_vid,
                        )
                    }
                }
            },
        )
    }

    fn apply_abort_stream_upload_command(
        &self,
        command: &AbortStreamUploadCommand,
    ) -> Result<(), MetadataError> {
        self.with_immediate_txn(
            "abort stream upload command (begin txn)",
            "abort stream upload command (commit txn)",
            |store| {
                let session = store.get_stream_upload(&command.session_id)?;
                if session.bucket != command.bucket || session.key != command.key {
                    return Err(MetadataError::InvariantViolation {
                        context: "abort stream upload command session binding mismatch",
                        reason: "metadata state does not satisfy the operation invariant".into(),
                    });
                }
                if session.state != StreamUploadState::InProgress {
                    return Err(MetadataError::StreamSessionNotInProgress {
                        state: session.state as u8,
                    });
                }
                let reservation_matches = match (
                    session.bucket_write_reservation.as_ref(),
                    command.stream_create_bucket_write_reservation.as_ref(),
                ) {
                    (None, None) => true,
                    (Some(session_proof), Some(command_proof)) => {
                        session_proof.has_same_stable_identity(command_proof)
                    }
                    (None, Some(_)) | (Some(_), None) => false,
                };
                if !reservation_matches {
                    return Err(MetadataError::InvariantViolation {
                        context: "abort stream upload command reservation mismatch",
                        reason: "metadata state does not satisfy the operation invariant".into(),
                    });
                }
                let staged_segments = store.list_stream_segments(&command.session_id)?;
                if staged_segments != command.staged_segments {
                    return Err(MetadataError::InvariantViolation {
                        context: "abort stream upload command staged segment mismatch",
                        reason: "metadata state does not satisfy the operation invariant".into(),
                    });
                }
                store.set_stream_upload_state_direct(
                    &command.session_id,
                    StreamUploadState::Aborted,
                )?;
                store.delete_stream_upload_in_open_txn(&command.session_id)
            },
        )
    }

    fn apply_commit_stream_part_command(
        &self,
        command: &CommitStreamPartCommand,
    ) -> Result<(), MetadataError> {
        if self.commit_stream_part_command_already_applied(command)? {
            return Ok(());
        }

        self.with_immediate_txn(
            "commit stream part command (begin txn)",
            "commit stream part command (commit txn)",
            |store| {
                store.validate_commit_stream_part_command(command)?;

                store.set_stream_upload_state_direct(
                    &command.session_id,
                    StreamUploadState::Completing,
                )?;
                store.insert_multipart_part_explicit(&command.part)?;
                store.delete_multipart_part_segments_for_upload_part(
                    &command.bucket,
                    &command.key,
                    &command.upload.upload_id,
                    command.part.part_number,
                )?;
                store.insert_multipart_part_segments_explicit(&command.segments)?;
                store.delete_stream_upload_in_open_txn(&command.session_id)
            },
        )
    }

    fn commit_stream_part_command_already_applied(
        &self,
        command: &CommitStreamPartCommand,
    ) -> Result<bool, MetadataError> {
        match self.get_stream_upload(&command.session_id) {
            Ok(_) => Ok(false),
            Err(MetadataError::StreamSessionNotFound { .. }) => {
                let part = match self
                    .get_multipart_part(&command.part.upload_id, command.part.part_number)
                {
                    Ok(part) => part,
                    Err(MetadataError::PartNotFound { .. }) => return Ok(false),
                    Err(error) => return Err(error),
                };
                let segments = self.get_multipart_part_segments_for_upload_part(
                    &command.bucket,
                    &command.key,
                    &command.upload.upload_id,
                    command.part.part_number,
                )?;
                if part == command.part && segments == command.segments {
                    Ok(true)
                } else {
                    Err(MetadataError::InvariantViolation {
                        context: "commit stream part command applied result mismatch",
                        reason: "metadata state does not satisfy the operation invariant".into(),
                    })
                }
            }
            Err(error) => Err(error),
        }
    }

    fn validate_commit_stream_part_command(
        &self,
        command: &CommitStreamPartCommand,
    ) -> Result<(), MetadataError> {
        if !command.has_consistent_subject() {
            return Err(MetadataError::InvariantViolation {
                context: "commit stream part command upload binding mismatch",
                reason: "metadata state does not satisfy the operation invariant".into(),
            });
        }
        let expected_generation = match command.existing_part.as_ref() {
            Some(existing) => {
                if existing.upload_id != command.upload.upload_id
                    || existing.part_number != command.part.part_number
                {
                    return Err(MetadataError::InvariantViolation {
                        context: "commit stream part command existing part binding mismatch",
                        reason: "metadata state does not satisfy the operation invariant".into(),
                    });
                }
                existing
                    .generation
                    .checked_add(1)
                    .ok_or(MetadataError::InvariantViolation {
                        context: "commit stream part command generation overflow",
                        reason: "metadata state does not satisfy the operation invariant".into(),
                    })?
            }
            None => 0,
        };
        if command.part.generation != expected_generation {
            return Err(MetadataError::InvariantViolation {
                context: "commit stream part command generation mismatch",
                reason: "metadata state does not satisfy the operation invariant".into(),
            });
        }

        let session = self.get_stream_upload(&command.session_id)?;
        if session.state != StreamUploadState::InProgress {
            return Err(MetadataError::StreamSessionNotInProgress {
                state: session.state as u8,
            });
        }
        match &session.target {
            StreamUploadTarget::UploadPart {
                upload_id,
                part_number,
            } if upload_id == &command.upload.upload_id
                && *part_number == command.part.part_number => {}
            _ => {
                return Err(MetadataError::StreamSessionNotFound {
                    session_id: command.session_id.as_str().to_owned(),
                })
            }
        }
        if session.bucket != command.bucket || session.key != command.key {
            return Err(MetadataError::InvariantViolation {
                context: "commit stream part command session binding mismatch",
                reason: "metadata state does not satisfy the operation invariant".into(),
            });
        }

        let upload = self.get_multipart_upload(&command.upload.upload_id)?;
        if upload != command.upload {
            return Err(MetadataError::InvariantViolation {
                context: "commit stream part command upload mismatch",
                reason: "metadata state does not satisfy the operation invariant".into(),
            });
        }

        let existing_part =
            match self.get_multipart_part(&command.part.upload_id, command.part.part_number) {
                Ok(part) => Some(part),
                Err(MetadataError::PartNotFound { .. }) => None,
                Err(error) => return Err(error),
            };
        if existing_part != command.existing_part {
            return Err(MetadataError::InvariantViolation {
                context: "commit stream part command existing part mismatch",
                reason: "metadata state does not satisfy the operation invariant".into(),
            });
        }

        let displaced_segments = self.get_multipart_part_segments_for_upload_part(
            &command.bucket,
            &command.key,
            &command.upload.upload_id,
            command.part.part_number,
        )?;
        if displaced_segments != command.displaced_segments {
            return Err(MetadataError::InvariantViolation {
                context: "commit stream part command displaced segments mismatch",
                reason: "metadata state does not satisfy the operation invariant".into(),
            });
        }

        let staged_segments = self.list_stream_segments(&command.session_id)?;
        if staged_segments.len() != command.segments.len()
            || staged_segments
                .iter()
                .zip(command.segments.iter())
                .any(|(staged, segment)| {
                    staged.segment_index != segment.segment_index
                        || staged.size != segment.size
                        || staged.segment_crc64 != segment.segment_crc64
                        || staged.segment_okh != segment.segment_okh
                        || staged.segment_vid != segment.segment_vid
                        || staged.data_pg_id != segment.data_pg_id
                        || staged.placement_cluster_epoch != segment.placement_cluster_epoch
                        || staged.ec_k != segment.ec_k
                        || staged.ec_m != segment.ec_m
                })
        {
            return Err(MetadataError::InvariantViolation {
                context: "commit stream part command staged segments mismatch",
                reason: "metadata state does not satisfy the operation invariant".into(),
            });
        }
        let staged_segments_total: u64 = staged_segments.iter().map(|segment| segment.size).sum();
        if staged_segments_total != command.part.size {
            return Err(MetadataError::InvariantViolation {
                context: "commit stream part command staged payload size mismatch",
                reason: "metadata state does not satisfy the operation invariant".into(),
            });
        }
        let staged_crc64 = combined_stream_segment_payload_crc64(&staged_segments);
        if staged_crc64 != command.part.payload_crc64 {
            return Err(MetadataError::InvariantViolation {
                context: "commit stream part command staged payload CRC64 mismatch",
                reason: "metadata state does not satisfy the operation invariant".into(),
            });
        }
        for segment in &command.segments {
            if segment.bucket != command.bucket
                || segment.key != command.key
                || segment.upload_id != command.upload.upload_id
                || segment.version_id != PART_SEGMENT_STAGING_VERSION_ID.to_u64()
                || segment.part_number != command.part.part_number
            {
                return Err(MetadataError::InvariantViolation {
                    context: "commit stream part command segment binding mismatch",
                    reason: "metadata state does not satisfy the operation invariant".into(),
                });
            }
        }
        Ok(())
    }

    fn insert_multipart_part_explicit(
        &self,
        part: &MultipartPartRecord,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "INSERT OR REPLACE INTO multipart_parts \
                 (upload_id, part_number, generation, size, payload_crc64, etag, etag_kind, \
                  part_vid, placement_cluster_epoch, ec_k, ec_m, last_modified, checksum) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                params![
                    part.upload_id,
                    part.part_number,
                    part.generation,
                    part.size as i64,
                    part.payload_crc64 as i64,
                    part.etag,
                    part.etag_kind as u8,
                    part.part_vid.get() as i64,
                    part.placement_cluster_epoch.get() as i64,
                    part.ec_k,
                    part.ec_m,
                    part.last_modified as i64,
                    part.checksum.as_ref().map(|checksum| checksum.as_slice()),
                ],
            )
            .map_err(|e| MetadataError::Db {
                context: "insert multipart part explicit",
                source: e.into(),
            })?;
        Ok(())
    }

    fn delete_multipart_part_segments_for_upload_part(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        part_number: u32,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM multipart_part_segments \
                 WHERE bucket = ?1 AND key = ?2 AND upload_id = ?3 AND part_number = ?4",
                params![bucket, key, upload_id, part_number],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete multipart part segments for upload part",
                source: e.into(),
            })?;
        Ok(())
    }

    fn insert_multipart_part_segments_explicit(
        &self,
        segments: &[MultipartPartSegmentRecord],
    ) -> Result<(), MetadataError> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "INSERT INTO multipart_part_segments \
                 (bucket, key, upload_id, version_id, part_number, segment_index, size, \
                  segment_crc64, segment_okh, segment_vid, data_pg_id, placement_cluster_epoch, ec_k, ec_m) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            )
            .map_err(|e| MetadataError::Db {
                context: "prepare insert multipart part segments explicit",
                source: e.into(),
            })?;
        for segment in segments {
            stmt.execute(params![
                segment.bucket,
                segment.key,
                segment.upload_id,
                segment.version_id as i64,
                segment.part_number,
                segment.segment_index,
                segment.size as i64,
                segment.segment_crc64 as i64,
                segment.segment_okh.as_slice(),
                segment.segment_vid.get() as i64,
                segment.data_pg_id,
                segment.placement_cluster_epoch.get() as i64,
                segment.ec_k,
                segment.ec_m,
            ])
            .map_err(|e| MetadataError::Db {
                context: "insert multipart part segment explicit",
                source: e.into(),
            })?;
        }
        Ok(())
    }

    fn apply_create_multipart_upload_command(
        &self,
        command: &CreateMultipartUploadCommand,
    ) -> Result<(), MetadataError> {
        if command.upload().state != UploadState::InProgress {
            return Err(MetadataError::InvariantViolation {
                context: "create multipart upload command state mismatch",
                reason: "metadata state does not satisfy the operation invariant".into(),
            });
        }
        self.create_multipart_upload_explicit(command.upload())
    }

    fn parse_multipart_object_identity(
        row: &rusqlite::Row<'_>,
        kind_index: usize,
        version_index: usize,
        value_index: usize,
    ) -> rusqlite::Result<Option<MultipartObjectIdentity>> {
        let kind = row.get::<_, u8>(kind_index)?;
        let version = row.get::<_, Option<i64>>(version_index)?;
        let value = row.get::<_, Option<i64>>(value_index)?;
        match (kind, version, value) {
            (0, None, None) => Ok(None),
            (1, Some(version), Some(generation)) => {
                let generation = u64::try_from(generation).map_err(|_| {
                    rusqlite::Error::FromSqlConversionFailure(
                        value_index,
                        rusqlite::types::Type::Integer,
                        Box::from("negative multipart initiation live generation"),
                    )
                })?;
                Ok(Some(MultipartObjectIdentity::Live {
                    version_id: Self::parse_version_id(version, version_index)?,
                    generation_id: GenerationId::new(generation).ok_or_else(|| {
                        rusqlite::Error::FromSqlConversionFailure(
                            value_index,
                            rusqlite::types::Type::Integer,
                            Box::from("invalid multipart initiation live generation"),
                        )
                    })?,
                }))
            }
            (2, Some(version), Some(write_sequence)) => {
                let write_sequence = u64::try_from(write_sequence).map_err(|_| {
                    rusqlite::Error::FromSqlConversionFailure(
                        value_index,
                        rusqlite::types::Type::Integer,
                        Box::from("negative multipart initiation delete-marker write sequence"),
                    )
                })?;
                if write_sequence == 0 {
                    return Err(rusqlite::Error::FromSqlConversionFailure(
                        value_index,
                        rusqlite::types::Type::Integer,
                        Box::from("zero multipart initiation delete-marker write sequence"),
                    ));
                }
                Ok(Some(MultipartObjectIdentity::DeleteMarker {
                    version_id: Self::parse_version_id(version, version_index)?,
                    write_sequence,
                }))
            }
            _ => Err(rusqlite::Error::FromSqlConversionFailure(
                kind_index,
                rusqlite::types::Type::Integer,
                Box::from("invalid multipart initiation object identity"),
            )),
        }
    }

    fn create_multipart_upload_explicit(
        &self,
        upload: &MultipartUploadRecord,
    ) -> Result<(), MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::create_multipart_upload_explicit",
            "pg_id={} upload_id={:?} bucket={:?} key={:?}",
            self.pg_id,
            upload.upload_id.as_str(),
            upload.bucket.as_str(),
            upload.key.as_str()
        );
        let algo = upload.checksum.map(|c| c.algorithm().wire_tag());
        let ctype = upload.checksum.map(|c| c.checksum_type().wire_tag());
        let tags = upload.tags.as_ref().map(SerializedTagSet::as_str);
        let (object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) =
            Self::object_lock_sql_values(upload.object_lock).map_err(|e| MetadataError::Db {
                context: "create multipart upload (encode object lock)",
                source: e.into(),
            })?;
        let encryption_type = upload.encryption.encryption_type() as u8;
        let encryption_state = upload.encryption.encode_state();
        let system_metadata_blob = upload.system_metadata_blob.as_slice();
        let initiator = &upload.initiator;
        let identity_sql_value = |value: u64| {
            i64::try_from(value).map_err(|_| MetadataError::Db {
                context: "create multipart upload identity exceeds SQLite integer range",
                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::from(
                    "multipart upload identity exceeds SQLite integer range",
                )),
            })
        };
        let (initiated_object_kind, initiated_object_version_id, initiated_object_value) =
            match upload.initiated_object_identity {
                None => (0_i64, None, None),
                Some(MultipartObjectIdentity::Live {
                    version_id,
                    generation_id,
                }) => (
                    1_i64,
                    Some(identity_sql_value(version_id.to_u64())?),
                    Some(identity_sql_value(generation_id.get())?),
                ),
                Some(MultipartObjectIdentity::DeleteMarker {
                    version_id,
                    write_sequence,
                }) => (
                    2_i64,
                    Some(identity_sql_value(version_id.to_u64())?),
                    Some(identity_sql_value(write_sequence)?),
                ),
            };
        let (listing_cluster_epoch, listing_log_index) =
            MultipartUploadIdKey::listing_position(&upload.upload_id)
                .filter(|position| *position != (0, 0))
                .unwrap_or_else(|| (1, upload.object_generation_id.get()));
        let listing_cluster_epoch =
            i64::try_from(listing_cluster_epoch).map_err(|_| MetadataError::Db {
                context: "create multipart upload listing cluster epoch",
                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::from(
                    "multipart upload listing cluster epoch exceeds SQLite integer range",
                )),
            })?;
        let listing_log_index =
            i64::try_from(listing_log_index).map_err(|_| MetadataError::Db {
                context: "create multipart upload listing log index",
                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::from(
                    "multipart upload listing log index exceeds SQLite integer range",
                )),
            })?;
        self.with_immediate_txn(
            "create multipart upload (begin txn)",
            "create multipart upload (commit txn)",
            |store| {
                match store.conn.execute(
                    "INSERT INTO object_generation_reservations \
                     (reservation_id, bucket, key, generation_id, created_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        upload.upload_id.as_str(),
                        upload.bucket,
                        upload.key,
                        upload.object_generation_id.get() as i64,
                        upload.initiated_at as i64,
                    ],
                ) {
                    Ok(_) => {}
                    Err(rusqlite::Error::SqliteFailure(_, _)) => {
                        let existing_generation = store
                            .conn
                            .query_row(
                                "SELECT generation_id FROM object_generation_reservations \
                                 WHERE reservation_id = ?1 AND bucket = ?2 AND key = ?3",
                                params![upload.upload_id.as_str(), upload.bucket, upload.key],
                                |row| {
                                    let raw: i64 = row.get(0)?;
                                    Self::parse_generation_id(raw, 0, "generation_id")
                                },
                            )
                            .optional()
                            .map_err(|e| MetadataError::Db {
                                context: "create multipart upload explicit reservation lookup",
                                source: e.into(),
                            })?;
                        if !matches!(existing_generation, Some(existing) if existing == upload.object_generation_id)
                        {
                            return Err(MetadataError::InvariantViolation {
                                context: "create multipart upload explicit reservation mismatch", reason: "metadata state does not satisfy the operation invariant".into() });
                        }
                    }
                    Err(e) => {
                        return Err(MetadataError::Db { context: "create multipart upload (reserve generation)",
                            source: e.into(),
                        });
                    }
                }
                match store.conn.execute(
                    "INSERT INTO multipart_uploads \
                     (upload_id, bucket, key, initiated_at, state, tags, metadata_blob, system_metadata_blob, owner_principal, owner_canonical_id, \
                      initiator_principal, initiator_canonical_id, checksum_algorithm, checksum_type, encryption_type, encryption_state, acl_grants, public_read, object_generation_id, listing_cluster_epoch, listing_log_index, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold, initiated_object_kind, initiated_object_version_id, initiated_object_generation_or_write_sequence) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27)",
                    params![
                        upload.upload_id,
                        upload.bucket,
                        upload.key,
                        upload.initiated_at as i64,
                        upload.state as u8,
                        tags,
                        upload.metadata_blob.as_slice(),
                        system_metadata_blob,
                        upload.owner.principal,
                        upload.owner.canonical_id.as_str(),
                        initiator.principal.as_str(),
                        initiator.canonical_id.as_str(),
                        algo,
                        ctype,
                        encryption_type,
                        encryption_state,
                        Self::serialize_acl_grants(&upload.acl_grants),
                        i32::from(upload.public_read),
                        upload.object_generation_id.get() as i64,
                        listing_cluster_epoch,
                        listing_log_index,
                        object_lock_retention_mode,
                        object_lock_retain_until,
                        object_lock_legal_hold,
                        initiated_object_kind,
                        initiated_object_version_id,
                        initiated_object_value,
                    ],
                ) {
                    Ok(_) => {}
                    Err(rusqlite::Error::SqliteFailure(_, _)) => {
                        let existing = store.get_multipart_upload(&upload.upload_id)?;
                        if existing != *upload {
                            return Err(MetadataError::InvariantViolation {
                                context: "create multipart upload explicit existing upload mismatch", reason: "metadata state does not satisfy the operation invariant".into() });
                        }
                    }
                    Err(e) => {
                        return Err(MetadataError::Db { context: "create multipart upload",
                            source: e.into(),
                        });
                    }
                }
                Ok(())
            },
        )
    }

    fn list_stream_uploads_for_multipart_upload(
        &self,
        upload_id: &UploadId,
    ) -> Result<Vec<StreamUploadRecord>, MetadataError> {
        let sql = format!(
            "{STREAM_UPLOAD_SELECT} WHERE op_kind = ?1 AND upload_id = ?2 ORDER BY session_id ASC"
        );
        let mut stmt = self
            .conn
            .prepare_cached(&sql)
            .map_err(|e| MetadataError::Db {
                context: "list stream uploads for multipart upload (prepare)",
                source: e.into(),
            })?;
        let rows = stmt
            .query_map(
                params![StreamUploadKind::UploadPart as u8, upload_id.as_str()],
                parse_stream_upload_record,
            )
            .map_err(|e| MetadataError::Db {
                context: "list stream uploads for multipart upload (query)",
                source: e.into(),
            })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| MetadataError::Db {
                context: "list stream uploads for multipart upload (collect)",
                source: e.into(),
            })
    }

    fn list_stream_segments_for_sessions(
        &self,
        sessions: &[StreamUploadRecord],
    ) -> Result<Vec<StreamUploadSegmentRecord>, MetadataError> {
        let mut segments = Vec::new();
        for session in sessions {
            segments.extend(self.list_stream_segments(&session.session_id)?);
        }
        Ok(segments)
    }

    pub(crate) fn prepare_abort_multipart_upload_cleanup(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<Option<AbortMultipartUploadCleanup>, MetadataError> {
        self.with_immediate_txn(
            "prepare abort multipart upload cleanup (begin txn)",
            "prepare abort multipart upload cleanup (commit txn)",
            |store| {
                let upload = match store.get_multipart_upload(upload_id) {
                    Ok(upload) => {
                        if upload.bucket != *bucket || upload.key != *key {
                            return Err(MetadataError::NoSuchUpload {
                                upload_id: upload_id.to_string(),
                            });
                        }
                        upload
                    }
                    Err(MetadataError::NoSuchUpload { .. }) => return Ok(None),
                    Err(error) => return Err(error),
                };

                match upload.state {
                    UploadState::InProgress => {}
                    UploadState::Aborting => {}
                    UploadState::Completing => return Ok(None),
                }

                let parts = store
                    .list_multipart_parts(&ListPartsReq {
                        upload_id: upload_id.clone(),
                        part_number_marker: None,
                        max_parts: u32::MAX,
                    })?
                    .parts;
                let streaming_segments =
                    store.get_all_multipart_part_segments_for_upload(upload_id)?;
                let stream_uploads = store.list_stream_uploads_for_multipart_upload(upload_id)?;
                let stream_upload_segments =
                    store.list_stream_segments_for_sessions(&stream_uploads)?;
                let stream_uploads = stream_uploads
                    .iter()
                    .map(TerminalStreamCleanupRecord::from)
                    .collect();

                Ok(Some(AbortMultipartUploadCleanup {
                    upload,
                    parts,
                    streaming_segments,
                    stream_uploads,
                    stream_upload_segments,
                }))
            },
        )
    }

    pub(crate) fn prepare_authorized_abort_multipart_upload_cleanup(
        &self,
        authorized_upload: &crate::AuthorizedMultipartUploadAbort,
    ) -> Result<Option<AbortMultipartUploadCleanup>, MetadataError> {
        self.with_immediate_txn(
            "prepare authorized abort multipart upload cleanup (begin txn)",
            "prepare authorized abort multipart upload cleanup (commit txn)",
            |store| {
                let upload = match store.get_multipart_upload(&authorized_upload.record().upload_id)
                {
                    Ok(upload) => upload,
                    Err(MetadataError::NoSuchUpload { .. }) => return Ok(None),
                    Err(error) => return Err(error),
                };
                if upload != *authorized_upload.record() {
                    return Ok(None);
                }

                match upload.state {
                    UploadState::InProgress => {}
                    UploadState::Aborting => {}
                    UploadState::Completing => return Ok(None),
                }

                let parts = store
                    .list_multipart_parts(&ListPartsReq {
                        upload_id: upload.upload_id.clone(),
                        part_number_marker: None,
                        max_parts: u32::MAX,
                    })?
                    .parts;
                let streaming_segments =
                    store.get_all_multipart_part_segments_for_upload(&upload.upload_id)?;
                let stream_uploads =
                    store.list_stream_uploads_for_multipart_upload(&upload.upload_id)?;
                let stream_upload_segments =
                    store.list_stream_segments_for_sessions(&stream_uploads)?;
                let stream_uploads = stream_uploads
                    .iter()
                    .map(TerminalStreamCleanupRecord::from)
                    .collect();

                Ok(Some(AbortMultipartUploadCleanup {
                    upload,
                    parts,
                    streaming_segments,
                    stream_uploads,
                    stream_upload_segments,
                }))
            },
        )
    }

    fn apply_abort_multipart_upload_command(
        &self,
        command: &AbortMultipartUploadCommand,
    ) -> Result<(), MetadataError> {
        if !command.has_consistent_subject() {
            return Err(MetadataError::InvariantViolation {
                context: "abort multipart upload command (subject mismatch)",
                reason: "metadata state does not satisfy the operation invariant".into(),
            });
        }
        self.with_immediate_txn(
            "abort multipart upload command (begin txn)",
            "abort multipart upload command (commit txn)",
            |store| {
                let upload = match store.get_multipart_upload(&command.upload_id) {
                    Ok(upload) => {
                        if upload.bucket != command.bucket || upload.key != command.key {
                            return Err(MetadataError::InvariantViolation {
                                context: "abort multipart upload command (upload mismatch)",
                                reason: "metadata state does not satisfy the operation invariant"
                                    .into(),
                            });
                        }
                        if upload != command.cleanup.upload {
                            return Err(MetadataError::InvariantViolation {
                                context: "abort multipart upload command (cleanup mismatch)",
                                reason: "metadata state does not satisfy the operation invariant"
                                    .into(),
                            });
                        }
                        upload
                    }
                    Err(MetadataError::NoSuchUpload { .. }) => {
                        return Err(MetadataError::InvariantViolation {
                            context: "abort multipart upload command (upload missing)",
                            reason: "metadata state does not satisfy the operation invariant"
                                .into(),
                        });
                    }
                    Err(error) => return Err(error),
                };
                debug_assert_eq!(upload, command.cleanup.upload);
                let parts = store
                    .list_multipart_parts(&ListPartsReq {
                        upload_id: command.upload_id.clone(),
                        part_number_marker: None,
                        max_parts: u32::MAX,
                    })?
                    .parts;
                if parts != command.cleanup.parts {
                    return Err(MetadataError::InvariantViolation {
                        context: "abort multipart upload command (parts mismatch)",
                        reason: "metadata state does not satisfy the operation invariant".into(),
                    });
                }
                let streaming_segments =
                    store.get_all_multipart_part_segments_for_upload(&command.upload_id)?;
                if streaming_segments != command.cleanup.streaming_segments {
                    return Err(MetadataError::InvariantViolation {
                        context: "abort multipart upload command (part segments mismatch)",
                        reason: "metadata state does not satisfy the operation invariant".into(),
                    });
                }
                let stream_uploads =
                    store.list_stream_uploads_for_multipart_upload(&command.upload_id)?;
                if !Self::stream_upload_cleanup_records_match(
                    &stream_uploads,
                    &command.cleanup.stream_uploads,
                ) {
                    return Err(MetadataError::InvariantViolation {
                        context: "abort multipart upload command (stream uploads mismatch)",
                        reason: "metadata state does not satisfy the operation invariant".into(),
                    });
                }
                let stream_upload_segments =
                    store.list_stream_segments_for_sessions(&stream_uploads)?;
                if stream_upload_segments != command.cleanup.stream_upload_segments {
                    return Err(MetadataError::InvariantViolation {
                        context: "abort multipart upload command (stream upload segments mismatch)",
                        reason: "metadata state does not satisfy the operation invariant".into(),
                    });
                }
                for session in &command.cleanup.stream_uploads {
                    store.delete_stream_upload_in_open_txn(&session.session_id)?;
                }
                store.delete_multipart_part_segments_by_upload_id_direct(&command.upload_id)?;
                store
                    .conn
                    .execute(
                        "DELETE FROM object_generation_reservations WHERE reservation_id = ?1",
                        params![command.upload_id.as_str()],
                    )
                    .map_err(|e| MetadataError::Db {
                        context: "abort multipart upload command (delete generation reservation)",
                        source: e.into(),
                    })?;
                store
                    .conn
                    .execute(
                        "DELETE FROM multipart_uploads WHERE upload_id = ?1",
                        params![command.upload_id.as_str()],
                    )
                    .map_err(|e| MetadataError::Db {
                        context: "abort multipart upload command (delete upload)",
                        source: e.into(),
                    })?;
                Ok(())
            },
        )
    }

    fn put_delete_marker_explicit_in_open_txn(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        owner: &OwnerIdentity,
        write_sequence: u64,
        last_modified_millis: u64,
    ) -> Result<(), MetadataError> {
        self.mark_current_live_noncurrent(
            bucket.as_str(),
            key.as_str(),
            version_id,
            last_modified_millis,
        )
        .map_err(|e| MetadataError::Db {
            context: "put object meta (mark noncurrent delete marker)",
            source: e.into(),
        })?;
        self.advance_object_version_counter_in_open_txn(bucket, key, version_id)?;
        self.advance_object_write_counter_in_open_txn(bucket, key, write_sequence, None)?;
        let sql = if version_id.is_null() {
            "INSERT OR REPLACE INTO objects \
             (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, last_modified, \
              storage_class, ec_k, ec_m, status, data_layout, parts_count, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read) \
             VALUES (?1, ?2, ?3, ?4, NULL, 0, zeroblob(0), 0, ?5, 0, 0, 0, 1, 0, NULL, NULL, NULL, 0, NULL, ?6, ?7, ?8, 0)"
        } else {
            "INSERT INTO objects \
             (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, last_modified, \
              storage_class, ec_k, ec_m, status, data_layout, parts_count, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read) \
             VALUES (?1, ?2, ?3, ?4, NULL, 0, zeroblob(0), 0, ?5, 0, 0, 0, 1, 0, NULL, NULL, NULL, 0, NULL, ?6, ?7, ?8, 0)"
        };
        self.execute_cached_metadata(
            sql,
            params![
                bucket,
                key,
                version_id.to_u64() as i64,
                write_sequence as i64,
                last_modified_millis as i64,
                owner.principal,
                owner.canonical_id.as_str(),
                Self::serialize_acl_grants(&AclGrants::default()),
            ],
            "put object meta (delete marker)",
        )?;
        Ok(())
    }

    fn delete_object_version_in_open_txn(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<(), MetadataError> {
        let write_sequence = self.next_object_write_sequence(bucket.as_str(), key.as_str())?;
        self.advance_object_write_counter_in_open_txn(bucket, key, write_sequence, None)?;
        let deleted_was_current: bool = self.query_row_cached_metadata(
            "SELECT EXISTS( \
                 SELECT 1 FROM objects \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3 \
                   AND version_id = ( \
                       SELECT version_id FROM objects \
                       WHERE bucket = ?1 AND key = ?2 \
                       ORDER BY write_sequence DESC LIMIT 1 \
                   ) \
             )",
            params![bucket, key, version_id.to_u64() as i64],
            "delete object version (lookup current)",
            |row| row.get::<_, i64>(0).map(|value| value != 0),
        )?;

        self.execute_cached_metadata(
            "DELETE FROM objects WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
            params![bucket, key, version_id.to_u64() as i64],
            "delete object version",
        )?;

        if deleted_was_current {
            self.clear_current_live_noncurrent(bucket.as_str(), key.as_str())
                .map_err(|e| MetadataError::Db {
                    context: "delete object version (restore current)",
                    source: e.into(),
                })?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn put_bucket_versioning_inner(
        &self,
        name: &BucketName,
        state: BucketVersioningState,
        generation: BucketExecutionGeneration,
    ) -> Result<(), MetadataError> {
        let (current, current_generation): (BucketVersioningState, u64) = self
            .query_row_cached_optional_metadata(
                "SELECT versioning, bucket_execution_generation FROM buckets WHERE name = ?1",
                params![name.as_str()],
                "get bucket versioning",
                |row| {
                    let raw_versioning = row.get::<_, u8>(0)?;
                    let raw_generation = row.get::<_, i64>(1)?;
                    Ok((raw_versioning, raw_generation))
                },
            )?
            .ok_or_else(|| bucket_not_found(name.as_str()))
            .and_then(|(raw_versioning, raw_generation)| {
                let versioning =
                    BucketVersioningState::from_u8(raw_versioning).ok_or_else(|| {
                        MetadataError::Db {
                            context: "invalid versioning state in database",
                            source: crate::error::DatabaseError::from_sql_conversion_failure(
                                0,
                                rusqlite::types::Type::Integer,
                                Box::from(format!("invalid versioning: {raw_versioning}")),
                            ),
                        }
                    })?;
                let generation = raw_generation.try_into().map_err(|_| MetadataError::Db {
                    context: "decode bucket execution generation",
                    source: crate::error::DatabaseError::from_sql_conversion_failure(
                        1,
                        rusqlite::types::Type::Integer,
                        Box::from("negative bucket_execution_generation"),
                    ),
                })?;
                Ok((versioning, generation))
            })?;
        if let BucketExecutionGeneration::Explicit(explicit) = generation {
            if current_generation == explicit {
                if current == state {
                    return Ok(());
                }
                return Err(MetadataError::InvariantViolation {
                    context: "apply conflicting bucket versioning command",
                    reason: "metadata state does not satisfy the operation invariant".into(),
                });
            }
            if current_generation > explicit {
                return Err(MetadataError::InvariantViolation {
                    context: "apply stale bucket versioning command",
                    reason: "metadata state does not satisfy the operation invariant".into(),
                });
            }
        }
        if state == BucketVersioningState::Disabled && current != BucketVersioningState::Disabled {
            return Err(MetadataError::InvalidVersioningTransition {
                from: current,
                to: state,
            });
        }
        let current_object_lock = self.head_bucket_raw(name)?.object_lock;
        Self::validate_bucket_object_lock_transition(
            state,
            current_object_lock,
            current_object_lock,
            "put bucket versioning",
        )?;

        self.with_immediate_txn(
            "put bucket versioning (begin txn)",
            "put bucket versioning (commit txn)",
            |store| {
                let generation = match generation {
                    #[cfg(test)]
                    BucketExecutionGeneration::Allocate => store
                        .next_bucket_execution_generation_in_txn(
                            "put bucket versioning (allocate execution generation)",
                        )?,
                    BucketExecutionGeneration::Explicit(generation) => {
                        store.advance_bucket_execution_generation_in_txn(
                            generation,
                            "put bucket versioning (advance execution generation)",
                        )?;
                        generation
                    }
                };
                store.execute_cached_metadata(
                    "UPDATE buckets \
                     SET versioning = ?1, \
                         bucket_execution_generation = ?2 \
                     WHERE name = ?3",
                    params![state as u8 as i64, generation as i64, name.as_str()],
                    "put bucket versioning",
                )?;
                Ok(())
            },
        )
    }

    #[cfg(test)]
    fn put_bucket_acl_inner(
        &self,
        name: &BucketName,
        acl_grants: &AclGrants,
        summary: BucketAclSummary,
        generation: BucketExecutionGeneration,
    ) -> Result<(), MetadataError> {
        let (current_acl_grants, current_public_read, current_public_write, current_generation): (
            AclGrants,
            bool,
            bool,
            u64,
        ) = self
            .query_row_cached_optional_metadata(
                "SELECT acl_grants, public_read, public_write, bucket_execution_generation \
                 FROM buckets \
                 WHERE name = ?1",
                params![name.as_str()],
                "get bucket acl",
                |row| {
                    let raw_acl_grants = row.get::<_, String>(0)?;
                    let public_read = row.get::<_, i64>(1)? != 0;
                    let public_write = row.get::<_, i64>(2)? != 0;
                    let raw_generation = row.get::<_, i64>(3)?;
                    Ok((raw_acl_grants, public_read, public_write, raw_generation))
                },
            )?
            .ok_or_else(|| bucket_not_found(name.as_str()))
            .and_then(
                |(raw_acl_grants, public_read, public_write, raw_generation)| {
                    let acl_grants = Self::parse_acl_grants(raw_acl_grants, 0, "bucket acl")
                        .map_err(|source| MetadataError::Db {
                            context: "decode bucket acl",
                            source: source.into(),
                        })?;
                    let generation = raw_generation.try_into().map_err(|_| MetadataError::Db {
                        context: "decode bucket execution generation",
                        source: crate::error::DatabaseError::from_sql_conversion_failure(
                            3,
                            rusqlite::types::Type::Integer,
                            Box::from("negative bucket_execution_generation"),
                        ),
                    })?;
                    Ok((acl_grants, public_read, public_write, generation))
                },
            )?;
        if let BucketExecutionGeneration::Explicit(explicit) = generation {
            if current_generation == explicit {
                if current_acl_grants == *acl_grants
                    && current_public_read == summary.public_read
                    && current_public_write == summary.public_write
                {
                    return Ok(());
                }
                return Err(MetadataError::InvariantViolation {
                    context: "apply conflicting bucket acl command",
                    reason: "metadata state does not satisfy the operation invariant".into(),
                });
            }
            if current_generation > explicit {
                return Err(MetadataError::InvariantViolation {
                    context: "apply stale bucket acl command",
                    reason: "metadata state does not satisfy the operation invariant".into(),
                });
            }
        }

        self.with_immediate_txn(
            "put bucket acl (begin txn)",
            "put bucket acl (commit txn)",
            |store| {
                let generation = match generation {
                    #[cfg(test)]
                    BucketExecutionGeneration::Allocate => store
                        .next_bucket_execution_generation_in_txn(
                            "put bucket acl (allocate execution generation)",
                        )?,
                    BucketExecutionGeneration::Explicit(generation) => {
                        store.advance_bucket_execution_generation_in_txn(
                            generation,
                            "put bucket acl (advance execution generation)",
                        )?;
                        generation
                    }
                };
                let updated = store.execute_cached_metadata(
                    "UPDATE buckets \
                     SET acl_grants = ?1, \
                         public_read = ?2, \
                         public_write = ?3, \
                         bucket_execution_generation = ?4 \
                    WHERE name = ?5",
                    params![
                        Self::serialize_acl_grants(acl_grants),
                        i32::from(summary.public_read),
                        i32::from(summary.public_write),
                        generation as i64,
                        name.as_str()
                    ],
                    "put bucket acl",
                )?;
                if updated == 0 {
                    return Err(bucket_not_found(name.as_str()));
                }
                Ok(())
            },
        )
    }

    #[cfg(test)]
    fn bucket_property_matches(
        &self,
        info: &BucketInfo,
        mutation: &BucketPropertyMutation,
    ) -> Result<bool, MetadataError> {
        match mutation {
            BucketPropertyMutation::ObjectLock(config) => Ok(info.object_lock == *config),
            BucketPropertyMutation::Encryption(config) => {
                Ok(PgMetadataStore::get_bucket_encryption(self, &info.name)? == *config)
            }
            BucketPropertyMutation::PublicAccessBlock(config) => {
                Ok(info.public_access_block == *config)
            }
            BucketPropertyMutation::OwnershipControls(config) => {
                Ok(info.ownership_controls == *config)
            }
            BucketPropertyMutation::AbacEnabled(enabled) => {
                Ok(info.bucket_abac_enabled == *enabled)
            }
        }
    }

    #[cfg(test)]
    fn put_bucket_property_inner(
        &self,
        name: &BucketName,
        mutation: &BucketPropertyMutation,
        generation: BucketExecutionGeneration,
    ) -> Result<(), MetadataError> {
        let info = self.head_bucket_raw(name)?;
        if let BucketExecutionGeneration::Explicit(explicit) = generation {
            if info.bucket_execution_generation == explicit {
                if self.bucket_property_matches(&info, mutation)? {
                    return Ok(());
                }
                return Err(MetadataError::InvariantViolation {
                    context: bucket_property_conflict_context(mutation.effect()),
                    reason: "metadata state does not satisfy the operation invariant".into(),
                });
            }
            if info.bucket_execution_generation > explicit {
                return Err(MetadataError::InvariantViolation {
                    context: bucket_property_stale_context(mutation.effect()),
                    reason: "metadata state does not satisfy the operation invariant".into(),
                });
            }
        }
        if let BucketPropertyMutation::ObjectLock(config) = mutation {
            Self::validate_bucket_object_lock_transition(
                info.versioning,
                info.object_lock,
                *config,
                "put bucket object lock",
            )?;
        }

        self.with_immediate_txn(
            "put bucket property (begin txn)",
            "put bucket property (commit txn)",
            |store| {
                let generation = match generation {
                    #[cfg(test)]
                    BucketExecutionGeneration::Allocate => store
                        .next_bucket_execution_generation_in_txn(
                            "put bucket property (allocate execution generation)",
                        )?,
                    BucketExecutionGeneration::Explicit(generation) => {
                        store.advance_bucket_execution_generation_in_txn(
                            generation,
                            "put bucket property (advance execution generation)",
                        )?;
                        generation
                    }
                };
                let updated = match mutation {
                    BucketPropertyMutation::ObjectLock(config) => {
                        let (enabled, default_mode, default_days, default_years) =
                            Self::bucket_object_lock_sql_values(*config).map_err(|e| {
                                MetadataError::Db {
                                    context: "put bucket object lock (encode)",
                                    source: e.into(),
                                }
                            })?;
                        store.execute_cached_metadata(
                            "UPDATE buckets \
                             SET object_lock_enabled = ?1, \
                                 object_lock_default_mode = ?2, \
                                 object_lock_default_days = ?3, \
                                 object_lock_default_years = ?4, \
                                 bucket_execution_generation = ?5 \
                             WHERE name = ?6",
                            params![
                                enabled,
                                default_mode,
                                default_days,
                                default_years,
                                generation as i64,
                                name.as_str()
                            ],
                            "put bucket object lock",
                        )?
                    }
                    BucketPropertyMutation::Encryption(config) => store.execute_cached_metadata(
                        "UPDATE buckets \
                             SET default_encryption_type = ?1, \
                                 sse_c_blocked = ?2, \
                                 bucket_execution_generation = ?3 \
                             WHERE name = ?4",
                        params![
                            config.default_encryption.map(|value| value as u8),
                            i32::from(config.sse_c_blocked),
                            generation as i64,
                            name.as_str()
                        ],
                        "put bucket encryption",
                    )?,
                    BucketPropertyMutation::PublicAccessBlock(config) => {
                        let (
                            present,
                            block_public_acls,
                            ignore_public_acls,
                            block_public_policy,
                            restrict_public_buckets,
                        ) = Self::public_access_block_sql_values(*config);
                        store.execute_cached_metadata(
                            "UPDATE buckets SET \
                                 public_access_block_present = ?1, \
                                 public_access_block_block_public_acls = ?2, \
                                 public_access_block_ignore_public_acls = ?3, \
                                 public_access_block_block_public_policy = ?4, \
                                 public_access_block_restrict_public_buckets = ?5, \
                                 bucket_execution_generation = ?6 \
                             WHERE name = ?7",
                            params![
                                present,
                                block_public_acls,
                                ignore_public_acls,
                                block_public_policy,
                                restrict_public_buckets,
                                generation as i64,
                                name.as_str(),
                            ],
                            "put bucket public access block",
                        )?
                    }
                    BucketPropertyMutation::OwnershipControls(config) => store
                        .execute_cached_metadata(
                            "UPDATE buckets \
                             SET ownership_controls_mode = ?1, \
                                 bucket_execution_generation = ?2 \
                             WHERE name = ?3",
                            params![
                                Self::ownership_controls_sql_value(*config),
                                generation as i64,
                                name.as_str()
                            ],
                            "put bucket ownership controls",
                        )?,
                    BucketPropertyMutation::AbacEnabled(enabled) => store.execute_cached_metadata(
                        "UPDATE buckets \
                             SET bucket_abac_enabled = ?1, \
                                 bucket_execution_generation = ?2 \
                             WHERE name = ?3",
                        params![
                            if *enabled { 1 } else { 0 },
                            generation as i64,
                            name.as_str()
                        ],
                        "put bucket abac enabled",
                    )?,
                };
                if updated == 0 {
                    return Err(bucket_not_found(name.as_str()));
                }
                Ok(())
            },
        )
    }

}
