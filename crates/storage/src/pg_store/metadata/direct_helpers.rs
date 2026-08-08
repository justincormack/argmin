impl PgStore {
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_insert_lifecycle_sweep_claim(
        &self,
        bucket: &BucketName,
        bucket_incarnation_generation: u64,
        claim_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        lease_deadline: Option<u64>,
    ) -> Result<(), MetadataError> {
        let bucket_incarnation_generation =
            i64::try_from(bucket_incarnation_generation).map_err(|source| MetadataError::Db {
                context: "test insert lifecycle sweep claim incarnation",
                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
            })?;
        let lease_deadline = lease_deadline
            .map(i64::try_from)
            .transpose()
            .map_err(|source| MetadataError::Db {
                context: "test insert lifecycle sweep claim lease deadline",
                source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
            })?;
        self.conn
            .execute(
                "INSERT INTO lifecycle_sweep_claims \
                 (bucket, bucket_incarnation_generation, claim_id, owner_token, cluster_epoch, \
                  pg_id, claimed_at, heartbeat_at, lease_deadline, attempt_count, last_error) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, 0, ?7, 1, NULL)",
                params![
                    bucket,
                    bucket_incarnation_generation,
                    claim_id,
                    owner_token,
                    cluster_epoch.get(),
                    self.pg_id,
                    lease_deadline,
                ],
            )
            .map_err(|source| MetadataError::Db {
                context: "test insert lifecycle sweep claim",
                source: source.into(),
            })?;
        Ok(())
    }

    fn delete_object_generation_reservation_direct(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM object_generation_reservations \
                 WHERE reservation_id = ?1 AND bucket = ?2 AND key = ?3",
                params![reservation_id.as_str(), bucket, key],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete object generation reservation",
                source: e.into(),
            })?;
        Ok(())
    }

    fn delete_object_segments_reclaim_direct(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM object_segments_reclaims \
                 WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3",
                params![bucket, key, generation_id.get() as i64],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete object segments reclaim",
                source: e.into(),
            })?;
        Ok(())
    }

    fn delete_multipart_reclaim_direct(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM multipart_reclaims \
                 WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3",
                params![bucket, key, generation_id.get() as i64],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete multipart reclaim",
                source: e.into(),
            })?;
        Ok(())
    }

    fn delete_object_parts_direct(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM object_parts \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![bucket, key, version_id.to_u64() as i64],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete object parts",
                source: e.into(),
            })?;
        Ok(())
    }

    fn set_stream_upload_state_direct(
        &self,
        session_id: &SessionId,
        new_state: StreamUploadState,
    ) -> Result<(), MetadataError> {
        let current: u8 = self
            .conn
            .query_row(
                "SELECT state FROM stream_uploads WHERE session_id = ?1",
                params![session_id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get stream upload state",
                source: e.into(),
            })?
            .ok_or_else(|| MetadataError::StreamSessionNotFound {
                session_id: session_id.as_str().to_owned(),
            })?;

        if current != StreamUploadState::InProgress as u8 {
            return Err(MetadataError::StreamSessionNotInProgress { state: current });
        }

        self.conn
            .execute(
                "UPDATE stream_uploads SET state = ?1 WHERE session_id = ?2",
                params![new_state as u8, session_id.as_str()],
            )
            .map_err(|e| MetadataError::Db {
                context: "set stream upload state",
                source: e.into(),
            })?;
        Ok(())
    }

    fn delete_stream_upload_in_open_txn(
        &self,
        session_id: &SessionId,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM stream_uploads WHERE session_id = ?1",
                params![session_id.as_str()],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete stream upload",
                source: e.into(),
            })?;
        self.conn
            .execute(
                "DELETE FROM object_generation_reservations WHERE reservation_id = ?1",
                params![session_id.as_str()],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete stream upload generation reservation",
                source: e.into(),
            })?;
        Ok(())
    }

    fn append_stream_segment_direct(
        &self,
        segment: &StreamUploadSegmentRecord,
    ) -> Result<(), MetadataError> {
        self.conn
                .execute(
                    "INSERT INTO stream_upload_segments \
                 (session_id, segment_index, size, segment_crc64, payload_crc64, segment_okh, segment_vid, data_pg_id, placement_cluster_epoch, ec_k, ec_m) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                params![
                    segment.session_id,
                    segment.segment_index,
                    segment.size as i64,
                    segment.segment_crc64 as i64,
                    segment.payload_crc64 as i64,
                    segment.segment_okh.as_slice(),
                    segment.segment_vid.get() as i64,
                    segment.data_pg_id,
                    segment.placement_cluster_epoch.get() as i64,
                    segment.ec_k,
                    segment.ec_m,
                ],
            )
            .map_err(|e| MetadataError::Db {
                context: "append stream segment",
                source: e.into(),
            })?;
        Ok(())
    }

    fn advance_stream_segment_vid_floor(
        &self,
        session_id: &SessionId,
        segment_vid: GenerationId,
    ) -> Result<(), MetadataError> {
        let next_vid =
            segment_vid
                .get()
                .checked_add(1)
                .ok_or_else(|| MetadataError::InvariantViolation {
                    context: "advance stream segment VID floor overflow",
                    reason: "metadata state does not satisfy the operation invariant".into(),
                })?;
        self.conn
            .execute(
                "UPDATE stream_uploads \
                 SET next_segment_vid = CASE \
                     WHEN next_segment_vid < ?1 THEN ?1 \
                     ELSE next_segment_vid \
                 END \
                 WHERE session_id = ?2",
                params![next_vid as i64, session_id.as_str()],
            )
            .map_err(|e| MetadataError::Db {
                context: "advance stream segment VID floor",
                source: e.into(),
            })?;
        Ok(())
    }

    fn delete_object_segments_direct(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM object_segments \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![bucket, key, version_id.to_u64() as i64],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete stream object segments",
                source: e.into(),
            })?;
        Ok(())
    }

    fn delete_multipart_part_segments_direct(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM multipart_part_segments \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![bucket, key, version_id.to_u64() as i64],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete multipart part segments",
                source: e.into(),
            })?;
        Ok(())
    }

    fn delete_multipart_part_segments_by_upload_id_direct(
        &self,
        upload_id: &UploadId,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM multipart_part_segments WHERE upload_id = ?1",
                params![upload_id.as_str()],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete multipart part segments by upload_id",
                source: e.into(),
            })?;
        Ok(())
    }
}
