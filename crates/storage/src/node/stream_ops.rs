use super::*;
use crate::{EcShape, WrittenShardAck};
#[cfg(test)]
use crate::{PrepareStreamUploadSegmentAppendReq, StreamUploadRecord, StreamUploadSegmentRecord};

impl SharedStorageNode {
    pub(crate) fn write_erasure_coded_segment_shards_with<F>(
        &self,
        segment_okh: &[u8; 16],
        segment_vid: GenerationId,
        data: &[u8],
        ec: EcShape,
        write_shards: F,
    ) -> Result<Vec<WrittenShardAck>, StoreError>
    where
        F: FnOnce(&[(ShardKey, &[u8])]) -> Result<Vec<(ShardKey, WriteAck)>, StoreError>,
    {
        let state = self.ec_write_state(ec)?;
        let k = ec.k as usize;
        let m = ec.m as usize;
        let remainder = data.len() % k;
        let mut padded = Vec::new();
        let shard_source: &[u8] = if remainder == 0 {
            data
        } else {
            padded.reserve_exact(data.len() + (k - remainder));
            padded.extend_from_slice(data);
            padded.resize(data.len() + (k - remainder), 0);
            &padded
        };

        let shard_size = shard_source.len() / k;
        let data_shards: Vec<&[u8]> = (0..k)
            .map(|i| &shard_source[i * shard_size..(i + 1) * shard_size])
            .collect();
        let written_shards = if shard_size == 0 {
            let mut parity_bufs: Vec<Vec<u8>> = (0..m).map(|_| Vec::new()).collect();
            let mut parity_refs: Vec<&mut [u8]> = parity_bufs
                .iter_mut()
                .map(std::vec::Vec::as_mut_slice)
                .collect();
            state
                .codec
                .encode(&data_shards, &mut parity_refs)
                .map_err(|error| StoreError::ErasureCoding {
                    context: "encode segment parity",
                    reason: error.to_string(),
                })?;

            let mut shard_batch: Vec<(ShardKey, &[u8])> = Vec::with_capacity(k + m);
            for (shard_index, shard_payload) in data_shards.iter().enumerate() {
                shard_batch.push((
                    ShardKey::new(segment_okh, segment_vid.get(), shard_index as u8),
                    *shard_payload,
                ));
            }
            for (parity_index, shard_payload) in parity_bufs.iter().enumerate() {
                shard_batch.push((
                    ShardKey::new(segment_okh, segment_vid.get(), (k + parity_index) as u8),
                    shard_payload.as_slice(),
                ));
            }
            write_shards(&shard_batch)?
        } else {
            let parity_len = m.checked_mul(shard_size).ok_or(StoreError::ErasureCoding {
                context: "size parity scratch",
                reason: "parity scratch length overflow".to_string(),
            })?;
            let mut scratch = state.scratch.checkout(parity_len);
            {
                let parity = scratch.as_mut_slice(parity_len);
                let mut parity_refs: Vec<&mut [u8]> = parity.chunks_exact_mut(shard_size).collect();
                state
                    .codec
                    .encode(&data_shards, &mut parity_refs)
                    .map_err(|error| StoreError::ErasureCoding {
                        context: "encode segment parity",
                        reason: error.to_string(),
                    })?;
            }
            let parity = scratch.as_slice(parity_len);

            let mut shard_batch: Vec<(ShardKey, &[u8])> = Vec::with_capacity(k + m);
            for (shard_index, shard_payload) in data_shards.iter().enumerate() {
                shard_batch.push((
                    ShardKey::new(segment_okh, segment_vid.get(), shard_index as u8),
                    *shard_payload,
                ));
            }
            for (parity_index, shard_payload) in parity.chunks_exact(shard_size).enumerate() {
                shard_batch.push((
                    ShardKey::new(segment_okh, segment_vid.get(), (k + parity_index) as u8),
                    shard_payload,
                ));
            }
            write_shards(&shard_batch)?
        };

        Ok(written_shards
            .into_iter()
            .map(|(key, ack)| WrittenShardAck { key, ack })
            .collect())
    }

    #[cfg(test)]
    fn validate_stream_upload_session_binding(
        session: &StreamUploadRecord,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<(), ObjectPgActionError> {
        if session.state != StreamUploadState::InProgress {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "stream session is not in progress".to_string(),
            });
        }
        if session.bucket != bucket.as_str() || session.key != key.as_str() {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "session bucket/key mismatch".to_string(),
            });
        }
        Ok(())
    }

    #[cfg(test)]
    fn reject_duplicate_stream_segment_index(
        pg: &PgStore,
        session_id: &SessionId,
        segment_index: u32,
    ) -> Result<(), ObjectPgActionError> {
        let existing_segments = pg.list_stream_segments(session_id)?;
        if existing_segments
            .iter()
            .any(|segment| segment.segment_index == segment_index)
        {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: format!("duplicate segment_index {segment_index}"),
            });
        }
        Ok(())
    }

    #[cfg(test)]
    pub fn load_stream_upload_session(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<StreamUploadRecord, ObjectPgActionError> {
        let pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
        let session = pg.get_stream_upload(session_id)?;
        Self::validate_stream_upload_session_binding(&session, bucket, key)?;
        Ok(session)
    }

    #[cfg(test)]
    pub fn prepare_stream_segment_append(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        request: &PrepareStreamUploadSegmentAppendReq,
    ) -> Result<(StreamUploadTarget, StreamUploadSegmentRecord), ObjectPgActionError> {
        let meta_pg_id = self.pg_topology.object_pg_for(bucket, key);
        let pg = self.get_pg(meta_pg_id)?;
        let session = pg.get_stream_upload(&request.session_id)?;
        Self::validate_stream_upload_session_binding(&session, bucket, key)?;
        Self::reject_duplicate_stream_segment_index(
            &pg,
            &request.session_id,
            request.segment_index,
        )?;
        let (segment_okh, segment_vid, data_pg_id) = match session.target {
            StreamUploadTarget::PutObject => {
                let generation_id =
                    pg.get_object_generation_reservation(bucket, key, &request.session_id)?;
                let segment_vid = pg.allocate_stream_segment_vid(&request.session_id)?;
                (
                    crate::segment_key_hash(
                        bucket.as_str(),
                        key.as_str(),
                        generation_id,
                        request.segment_index,
                    ),
                    segment_vid,
                    self.pg_topology
                        .object_generation_segment_data_pg(
                            bucket,
                            key,
                            generation_id,
                            request.segment_index,
                        )
                        .get(),
                )
            }
            StreamUploadTarget::UploadPart {
                ref upload_id,
                part_number,
            } => {
                let upload = Self::load_in_progress_multipart_upload_from_object_pg(
                    &pg, bucket, key, upload_id,
                )?;
                let segment_vid = pg.allocate_stream_segment_vid(&request.session_id)?;
                (
                    request.segment_okh,
                    segment_vid,
                    self.pg_topology
                        .object_generation_multipart_part_segment_data_pg(
                            bucket,
                            key,
                            upload.object_generation_id,
                            part_number,
                            request.segment_index,
                        )
                        .get(),
                )
            }
        };
        let segment_record = StreamUploadSegmentRecord {
            session_id: request.session_id.clone(),
            segment_index: request.segment_index,
            size: request.size,
            segment_crc64: request.segment_crc64,
            segment_okh,
            segment_vid,
            data_pg_id,
            ec_k: self.default_ec_shape.k,
            ec_m: self.default_ec_shape.m,
        };
        Ok((session.target, segment_record))
    }

    #[cfg(test)]
    pub fn list_all_stream_uploads_best_effort(&self) -> Vec<StreamUploadRecord> {
        let mut sessions = Vec::new();
        for &pg_id in &self.pg_id_list {
            let Ok(pg) = self.get_pg(pg_id) else {
                continue;
            };
            let Ok(mut local) = pg.list_all_stream_uploads() else {
                continue;
            };
            sessions.append(&mut local);
        }
        sessions
    }
}
