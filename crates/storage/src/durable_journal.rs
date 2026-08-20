// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::control_plane::ControlPlaneError;

const FRAME_LENGTH_LEN: usize = std::mem::size_of::<u32>();
const FRAME_PREFIX_LEN: usize = FRAME_LENGTH_LEN * 2;
const CHECKSUM_LEN: usize = std::mem::size_of::<u64>();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DurableJournalFileHeaderFormatError {
    Truncated,
    ChecksumMismatch { expected: u64, actual: u64 },
    UnknownMagic,
    UnsupportedVersion(u16),
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct DurableJournalFormat {
    pub file_magic: &'static [u8],
    pub file_version: u16,
    pub label: &'static str,
}

impl DurableJournalFormat {
    pub(crate) const fn header_len(self) -> usize {
        self.file_magic.len()
            + std::mem::size_of::<u16>()
            + std::mem::size_of::<u64>()
            + CHECKSUM_LEN
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct DurableJournalIoContexts {
    pub create_directory: &'static str,
    pub open_for_append: &'static str,
    pub write_frame_length: &'static str,
    pub write_frame: &'static str,
    pub sync_file: &'static str,
    pub stat_for_status: &'static str,
    pub open_for_replay: &'static str,
    pub read_for_replay: &'static str,
    pub open_for_tail_truncation: &'static str,
    pub truncate_torn_tail: &'static str,
    pub sync_truncated_tail: &'static str,
    pub read_for_compaction: &'static str,
    pub create_compacted_temp: &'static str,
    pub write_compacted_temp: &'static str,
    pub sync_compacted_temp: &'static str,
    pub commit_compacted: &'static str,
    pub stat_before_append: &'static str,
    pub write_file_header: &'static str,
    pub open_file_header: &'static str,
    pub read_file_header: &'static str,
}

pub(crate) trait DurableJournalObserver: std::fmt::Debug + Send + Sync + 'static {
    fn record_append(&self, _elapsed: Duration, _succeeded: bool) {}

    fn record_lock_wait(&self, _elapsed: Duration) {}

    fn record_frame_bytes(&self, _bytes: usize) {}

    fn record_file_sync(&self, _elapsed: Duration) {}

    fn record_directory_sync(&self, _elapsed: Duration) {}

    fn record_compaction(&self, _elapsed: Duration, _succeeded: bool) {}

    fn record_compaction_lock_wait(&self, _elapsed: Duration) {}

    fn record_compaction_bytes(&self, _bytes: usize) {}

    fn record_compaction_file_sync(&self, _elapsed: Duration) {}

    fn record_compaction_directory_sync(&self, _elapsed: Duration) {}

    fn before_file_sync(&self, _path: &Path) -> Result<(), ControlPlaneError> {
        Ok(())
    }

    fn sync_parent(&self, path: &Path) -> Result<(), ControlPlaneError>;
}

#[derive(Debug)]
pub(crate) enum DurableJournalAppendError {
    BeforeReplayableRecord(ControlPlaneError),
    AmbiguousRecordMayExist(ControlPlaneError),
    ReplayableRecordMayExist(ControlPlaneError),
}

impl DurableJournalAppendError {
    pub(crate) fn into_control_plane_error(self) -> ControlPlaneError {
        match self {
            Self::BeforeReplayableRecord(error)
            | Self::AmbiguousRecordMayExist(error)
            | Self::ReplayableRecordMayExist(error) => error,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DurableJournalOffsets {
    pub base_offset: u64,
    pub clean_len: u64,
}

#[derive(Debug)]
pub(crate) struct DurableJournalFrames {
    pub frames: Vec<Vec<u8>>,
    pub clean_len: u64,
    pub truncated_tail: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct DurableJournalFile<O: DurableJournalObserver> {
    path: PathBuf,
    format: DurableJournalFormat,
    contexts: DurableJournalIoContexts,
    io_lock: Arc<Mutex<()>>,
    observer: Arc<O>,
}

impl<O: DurableJournalObserver> DurableJournalFile<O> {
    pub(crate) fn new(
        path: PathBuf,
        format: DurableJournalFormat,
        contexts: DurableJournalIoContexts,
        observer: Arc<O>,
    ) -> Self {
        Self {
            path,
            format,
            contexts,
            io_lock: Arc::new(Mutex::new(())),
            observer,
        }
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn framed_len(frame_len: usize) -> u64 {
        u64::try_from(frame_len.saturating_add(FRAME_PREFIX_LEN)).unwrap_or(u64::MAX)
    }

    pub(crate) fn append_frame(&self, frame: &[u8]) -> Result<(), DurableJournalAppendError> {
        let append_started = Instant::now();
        let result = self.append_frame_inner(frame);
        self.observer
            .record_append(append_started.elapsed(), result.is_ok());
        result
    }

    fn append_frame_inner(&self, frame: &[u8]) -> Result<(), DurableJournalAppendError> {
        let lock_started = Instant::now();
        let guard = self
            .io_lock
            .lock()
            .map_err(|_| self.protocol_error(format!("{} lock poisoned", self.format.label)))
            .map_err(DurableJournalAppendError::BeforeReplayableRecord);
        self.observer.record_lock_wait(lock_started.elapsed());
        let _guard = guard?;

        let frame_len = u32::try_from(frame.len())
            .map_err(|_| {
                self.protocol_error(format!(
                    "{} frame length {} exceeds u32::MAX",
                    self.format.label,
                    frame.len()
                ))
            })
            .map_err(DurableJournalAppendError::BeforeReplayableRecord)?;
        if frame_len == 0 {
            return Err(DurableJournalAppendError::BeforeReplayableRecord(
                self.protocol_error(format!("zero-length {} frame", self.format.label)),
            ));
        }
        let frame_bytes = frame.len().saturating_add(FRAME_PREFIX_LEN);

        if let Some(parent) = self
            .path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)
                .map_err(|source| ControlPlaneError::io(self.contexts.create_directory, source))
                .map_err(DurableJournalAppendError::BeforeReplayableRecord)?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&self.path)
            .map_err(|source| ControlPlaneError::io(self.contexts.open_for_append, source))
            .map_err(DurableJournalAppendError::BeforeReplayableRecord)?;
        self.ensure_file_header(&mut file)?;
        let mut frame_prefix = [0u8; FRAME_PREFIX_LEN];
        frame_prefix[..FRAME_LENGTH_LEN].copy_from_slice(&frame_len.to_be_bytes());
        frame_prefix[FRAME_LENGTH_LEN..].copy_from_slice(&(!frame_len).to_be_bytes());
        self.write_replayable_bytes(&mut file, &frame_prefix, self.contexts.write_frame_length)?;
        self.write_replayable_bytes(&mut file, frame, self.contexts.write_frame)?;

        let file_sync_started = Instant::now();
        let file_sync_result = self.observer.before_file_sync(&self.path).and_then(|()| {
            file.sync_all()
                .map_err(|source| ControlPlaneError::io(self.contexts.sync_file, source))
        });
        self.observer.record_file_sync(file_sync_started.elapsed());
        file_sync_result.map_err(DurableJournalAppendError::AmbiguousRecordMayExist)?;
        self.observer.record_frame_bytes(frame_bytes);

        let directory_sync_started = Instant::now();
        let directory_sync_result = self.observer.sync_parent(&self.path);
        self.observer
            .record_directory_sync(directory_sync_started.elapsed());
        directory_sync_result.map_err(DurableJournalAppendError::ReplayableRecordMayExist)?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn clean_len(&self) -> Result<u64, ControlPlaneError> {
        let _guard = self.lock()?;
        let replay_offset = match self.read_file_base_offset_unlocked() {
            Ok(base_offset) => base_offset,
            Err(ControlPlaneError::Io { diagnostic: source })
                if source.kind() == io::ErrorKind::NotFound =>
            {
                return Ok(0);
            }
            Err(error) => return Err(error),
        };
        Ok(self.read_frames_from_unlocked(replay_offset)?.clean_len)
    }

    pub(crate) fn status_offsets(&self) -> Result<DurableJournalOffsets, ControlPlaneError> {
        let _guard = self.lock()?;
        let file_len = match fs::metadata(&self.path) {
            Ok(metadata) => metadata.len(),
            Err(source) if source.kind() == io::ErrorKind::NotFound => {
                return Ok(DurableJournalOffsets {
                    base_offset: 0,
                    clean_len: 0,
                });
            }
            Err(source) => {
                return Err(ControlPlaneError::io(self.contexts.stat_for_status, source));
            }
        };
        if file_len == 0 {
            return Ok(DurableJournalOffsets {
                base_offset: 0,
                clean_len: 0,
            });
        }
        let base_offset = self.read_file_base_offset_unlocked()?;
        let header_len = u64::try_from(self.format.header_len()).expect("header length fits u64");
        if file_len < header_len {
            return Err(self.protocol_error(format!("truncated {} file header", self.format.label)));
        }
        let clean_len = base_offset
            .checked_add(file_len - header_len)
            .ok_or_else(|| {
                self.protocol_error(format!("{} status length overflows", self.format.label))
            })?;
        Ok(DurableJournalOffsets {
            base_offset,
            clean_len,
        })
    }

    pub(crate) fn read_frames_from(
        &self,
        replay_offset: u64,
    ) -> Result<DurableJournalFrames, ControlPlaneError> {
        let _guard = self.lock()?;
        self.read_frames_from_unlocked(replay_offset)
    }

    fn read_frames_from_unlocked(
        &self,
        replay_offset: u64,
    ) -> Result<DurableJournalFrames, ControlPlaneError> {
        let mut file = match File::open(&self.path) {
            Ok(file) => file,
            Err(source) if source.kind() == io::ErrorKind::NotFound => {
                if replay_offset != 0 {
                    return Err(self.protocol_error(format!(
                        "{} replay offset {replay_offset} has no journal file",
                        self.format.label
                    )));
                }
                return Ok(DurableJournalFrames {
                    frames: Vec::new(),
                    clean_len: 0,
                    truncated_tail: false,
                });
            }
            Err(source) => {
                return Err(ControlPlaneError::io(self.contexts.open_for_replay, source));
            }
        };
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .map_err(|source| ControlPlaneError::io(self.contexts.read_for_replay, source))?;
        if bytes.is_empty() {
            if replay_offset != 0 {
                return Err(self.protocol_error(format!(
                    "{} replay offset {replay_offset} has an empty journal file",
                    self.format.label
                )));
            }
            return Ok(DurableJournalFrames {
                frames: Vec::new(),
                clean_len: 0,
                truncated_tail: false,
            });
        }

        let (base_offset, header_len) = self.decode_file_header(&bytes)?;
        let replay_offset = usize::try_from(replay_offset).map_err(|_| {
            self.protocol_error(format!("{} replay offset exceeds usize", self.format.label))
        })?;
        let base_offset = usize::try_from(base_offset).map_err(|_| {
            self.protocol_error(format!("{} base offset exceeds usize", self.format.label))
        })?;
        if replay_offset < base_offset {
            return Err(self.protocol_error(format!(
                "{} replay offset {replay_offset} is before compacted base offset {base_offset}",
                self.format.label
            )));
        }
        let replay_offset = header_len
            .checked_add(replay_offset - base_offset)
            .ok_or_else(|| {
                self.protocol_error(format!(
                    "{} replay offset overflows physical offset",
                    self.format.label
                ))
            })?;
        if replay_offset > bytes.len() {
            return Err(self.protocol_error(format!(
                "{} replay offset {replay_offset} exceeds journal length {}",
                self.format.label,
                bytes.len()
            )));
        }

        let mut frames = Vec::new();
        let mut offset = replay_offset;
        let mut clean_len = base_offset + (replay_offset - header_len);
        while offset < bytes.len() {
            let frame_start = offset;
            let logical_frame_start = base_offset + (frame_start - header_len);
            if bytes.len() - offset < FRAME_PREFIX_LEN {
                return Ok(DurableJournalFrames {
                    frames,
                    clean_len: clean_len as u64,
                    truncated_tail: true,
                });
            }
            let frame_len = u32::from_be_bytes(
                bytes[offset..offset + FRAME_LENGTH_LEN]
                    .try_into()
                    .expect("journal frame length prefix has fixed width"),
            );
            let frame_len_check = u32::from_be_bytes(
                bytes[offset + FRAME_LENGTH_LEN..offset + FRAME_PREFIX_LEN]
                    .try_into()
                    .expect("journal frame length check has fixed width"),
            );
            if frame_len_check != !frame_len {
                return Err(self
                    .protocol_error(format!("{} frame length check mismatch", self.format.label)));
            }
            offset += FRAME_PREFIX_LEN;
            if frame_len == 0 {
                return Err(self.protocol_error(format!("zero-length {} frame", self.format.label)));
            }
            let remaining = bytes.len() - offset;
            if u64::from(frame_len) > u64::try_from(remaining).unwrap_or(u64::MAX) {
                return Ok(DurableJournalFrames {
                    frames,
                    clean_len: logical_frame_start as u64,
                    truncated_tail: true,
                });
            }
            let frame_len = usize::try_from(frame_len)
                .expect("frame length bounded by remaining addressable bytes");
            let frame_end = offset + frame_len;
            frames.push(bytes[offset..frame_end].to_vec());
            offset = frame_end;
            clean_len = base_offset + (offset - header_len);
        }
        Ok(DurableJournalFrames {
            frames,
            clean_len: clean_len as u64,
            truncated_tail: false,
        })
    }

    pub(crate) fn truncate_to_clean_len(&self, clean_len: u64) -> Result<(), ControlPlaneError> {
        let _guard = self.lock()?;
        let base_offset = self.read_file_base_offset_unlocked()?;
        if clean_len < base_offset {
            return Err(self.protocol_error(format!(
                "{} clean length {clean_len} is before base offset {base_offset}",
                self.format.label
            )));
        }
        let file = OpenOptions::new()
            .write(true)
            .open(&self.path)
            .map_err(|source| {
                ControlPlaneError::io(self.contexts.open_for_tail_truncation, source)
            })?;
        let physical_len = u64::try_from(self.format.header_len())
            .expect("header length fits u64")
            .checked_add(clean_len - base_offset)
            .ok_or_else(|| {
                self.protocol_error(format!("{} physical length overflows", self.format.label))
            })?;
        file.set_len(physical_len)
            .map_err(|source| ControlPlaneError::io(self.contexts.truncate_torn_tail, source))?;
        file.sync_all()
            .map_err(|source| ControlPlaneError::io(self.contexts.sync_truncated_tail, source))?;
        self.observer.sync_parent(&self.path)
    }

    pub(crate) fn compact_through(&self, replay_offset: u64) -> Result<(), ControlPlaneError> {
        let compact_started = Instant::now();
        let result = self.compact_through_inner(replay_offset);
        self.observer
            .record_compaction(compact_started.elapsed(), result.is_ok());
        result
    }

    pub(crate) fn replace_from(
        &self,
        replay_offset: u64,
        expected_clean_len: u64,
        frames: &[Vec<u8>],
    ) -> Result<(), ControlPlaneError> {
        let compact_started = Instant::now();
        let result = self.replace_from_inner(replay_offset, expected_clean_len, frames);
        self.observer
            .record_compaction(compact_started.elapsed(), result.is_ok());
        result
    }

    fn replace_from_inner(
        &self,
        replay_offset: u64,
        expected_clean_len: u64,
        frames: &[Vec<u8>],
    ) -> Result<(), ControlPlaneError> {
        let lock_started = Instant::now();
        let guard = self.lock();
        self.observer
            .record_compaction_lock_wait(lock_started.elapsed());
        let _guard = guard?;

        let bytes = fs::read(&self.path)
            .map_err(|source| ControlPlaneError::io(self.contexts.read_for_compaction, source))?;
        let (base_offset, header_len) = self.decode_file_header(&bytes)?;
        if replay_offset < base_offset {
            return Err(self.protocol_error(format!(
                "{} replacement offset {replay_offset} is before base offset {base_offset}",
                self.format.label
            )));
        }
        let physical_payload_len = bytes.len().checked_sub(header_len).ok_or_else(|| {
            self.protocol_error(format!("truncated {} file header", self.format.label))
        })?;
        let clean_len = base_offset
            .checked_add(u64::try_from(physical_payload_len).unwrap_or(u64::MAX))
            .ok_or_else(|| {
                self.protocol_error(format!("{} clean length overflows", self.format.label))
            })?;
        if clean_len != expected_clean_len {
            return Err(self.protocol_error(format!(
                "{} changed during checkpoint replacement: expected clean length {expected_clean_len}, actual {clean_len}",
                self.format.label
            )));
        }
        if replay_offset > clean_len {
            return Err(self.protocol_error(format!(
                "{} replacement offset {replay_offset} exceeds clean length {clean_len}",
                self.format.label
            )));
        }

        let mut replacement = Vec::new();
        for frame in frames {
            let frame_len = u32::try_from(frame.len()).map_err(|_| {
                self.protocol_error(format!(
                    "{} replacement frame length {} exceeds u32::MAX",
                    self.format.label,
                    frame.len()
                ))
            })?;
            if frame_len == 0 {
                return Err(self.protocol_error(format!("zero-length {} frame", self.format.label)));
            }
            replacement.extend_from_slice(&frame_len.to_be_bytes());
            replacement.extend_from_slice(&(!frame_len).to_be_bytes());
            replacement.extend_from_slice(frame);
        }
        let compacted = self.encode_file_bytes(replay_offset, &replacement);
        let tmp_path = self.tmp_path();
        {
            let mut file = File::create(&tmp_path).map_err(|source| {
                ControlPlaneError::io(self.contexts.create_compacted_temp, source)
            })?;
            file.write_all(&compacted).map_err(|source| {
                ControlPlaneError::io(self.contexts.write_compacted_temp, source)
            })?;
            self.observer.record_compaction_bytes(compacted.len());
            let file_sync_started = Instant::now();
            let file_sync_result = file
                .sync_all()
                .map_err(|source| ControlPlaneError::io(self.contexts.sync_compacted_temp, source));
            self.observer
                .record_compaction_file_sync(file_sync_started.elapsed());
            file_sync_result?;
        }
        fs::rename(&tmp_path, &self.path)
            .map_err(|source| ControlPlaneError::io(self.contexts.commit_compacted, source))?;
        let directory_sync_started = Instant::now();
        let directory_sync_result = self.observer.sync_parent(&self.path);
        self.observer
            .record_compaction_directory_sync(directory_sync_started.elapsed());
        directory_sync_result
    }

    fn compact_through_inner(&self, replay_offset: u64) -> Result<(), ControlPlaneError> {
        let lock_started = Instant::now();
        let guard = self.lock();
        self.observer
            .record_compaction_lock_wait(lock_started.elapsed());
        let _guard = guard?;
        let mut bytes = match fs::read(&self.path) {
            Ok(bytes) => bytes,
            Err(source) if source.kind() == io::ErrorKind::NotFound => {
                if replay_offset == 0 {
                    return Ok(());
                }
                return Err(self.protocol_error(format!(
                    "{} compaction offset {replay_offset} has no journal file",
                    self.format.label
                )));
            }
            Err(source) => {
                return Err(ControlPlaneError::io(
                    self.contexts.read_for_compaction,
                    source,
                ));
            }
        };
        if bytes.is_empty() {
            if replay_offset == 0 {
                return Ok(());
            }
            return Err(self.protocol_error(format!(
                "{} compaction offset {replay_offset} has an empty journal file",
                self.format.label
            )));
        }
        let (base_offset, header_len) = self.decode_file_header(&bytes)?;
        if replay_offset < base_offset {
            return Err(self.protocol_error(format!(
                "{} compaction offset {replay_offset} is before base offset {base_offset}",
                self.format.label
            )));
        }
        let relative_offset = usize::try_from(replay_offset - base_offset).map_err(|_| {
            self.protocol_error(format!(
                "{} compaction offset exceeds usize",
                self.format.label
            ))
        })?;
        let suffix_start = header_len.checked_add(relative_offset).ok_or_else(|| {
            self.protocol_error(format!("{} compaction offset overflows", self.format.label))
        })?;
        if suffix_start > bytes.len() {
            return Err(self.protocol_error(format!(
                "{} compaction offset {replay_offset} exceeds journal length {} from base offset {base_offset}",
                self.format.label,
                bytes.len() - header_len
            )));
        }
        if replay_offset == base_offset {
            return Ok(());
        }

        let suffix = bytes.split_off(suffix_start);
        let compacted = self.encode_file_bytes(replay_offset, &suffix);
        let tmp_path = self.tmp_path();
        {
            let mut file = File::create(&tmp_path).map_err(|source| {
                ControlPlaneError::io(self.contexts.create_compacted_temp, source)
            })?;
            file.write_all(&compacted).map_err(|source| {
                ControlPlaneError::io(self.contexts.write_compacted_temp, source)
            })?;
            self.observer.record_compaction_bytes(compacted.len());
            let file_sync_started = Instant::now();
            let file_sync_result = file
                .sync_all()
                .map_err(|source| ControlPlaneError::io(self.contexts.sync_compacted_temp, source));
            self.observer
                .record_compaction_file_sync(file_sync_started.elapsed());
            file_sync_result?;
        }
        fs::rename(&tmp_path, &self.path)
            .map_err(|source| ControlPlaneError::io(self.contexts.commit_compacted, source))?;
        let directory_sync_started = Instant::now();
        let directory_sync_result = self.observer.sync_parent(&self.path);
        self.observer
            .record_compaction_directory_sync(directory_sync_started.elapsed());
        directory_sync_result
    }

    pub(crate) fn encode_file_header(&self, base_offset: u64) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.format.header_len());
        out.extend_from_slice(self.format.file_magic);
        out.extend_from_slice(&self.format.file_version.to_be_bytes());
        out.extend_from_slice(&base_offset.to_be_bytes());
        let checksum = checksum::crc64::checksum(&out);
        out.extend_from_slice(&checksum.to_be_bytes());
        out
    }

    pub(crate) fn decode_file_header(
        &self,
        bytes: &[u8],
    ) -> Result<(u64, usize), ControlPlaneError> {
        self.decode_file_header_classified(bytes)
            .map_err(|error| self.file_header_format_error(error))
    }

    pub(crate) fn decode_file_header_classified(
        &self,
        bytes: &[u8],
    ) -> Result<(u64, usize), DurableJournalFileHeaderFormatError> {
        let header_len = self.format.header_len();
        if bytes.is_empty() {
            return Ok((0, header_len));
        }
        if bytes.len() < header_len {
            return Err(DurableJournalFileHeaderFormatError::Truncated);
        }
        let (header, checksum_bytes) = bytes[..header_len].split_at(header_len - CHECKSUM_LEN);
        let expected_checksum = u64::from_be_bytes(
            checksum_bytes
                .try_into()
                .expect("journal header checksum has fixed length"),
        );
        let actual_checksum = checksum::crc64::checksum(header);
        if actual_checksum != expected_checksum {
            return Err(DurableJournalFileHeaderFormatError::ChecksumMismatch {
                expected: expected_checksum,
                actual: actual_checksum,
            });
        }
        if &header[..self.format.file_magic.len()] != self.format.file_magic {
            return Err(DurableJournalFileHeaderFormatError::UnknownMagic);
        }
        let version_start = self.format.file_magic.len();
        let version = u16::from_be_bytes(
            header[version_start..version_start + std::mem::size_of::<u16>()]
                .try_into()
                .expect("journal header version has fixed width"),
        );
        if version != self.format.file_version {
            return Err(DurableJournalFileHeaderFormatError::UnsupportedVersion(
                version,
            ));
        }
        let offset_start = version_start + std::mem::size_of::<u16>();
        let base_offset = u64::from_be_bytes(
            header[offset_start..offset_start + std::mem::size_of::<u64>()]
                .try_into()
                .expect("journal header base offset has fixed width"),
        );
        Ok((base_offset, header_len))
    }

    fn file_header_format_error(
        &self,
        error: DurableJournalFileHeaderFormatError,
    ) -> ControlPlaneError {
        let message = match error {
            DurableJournalFileHeaderFormatError::Truncated => {
                format!("truncated {} file header", self.format.label)
            }
            DurableJournalFileHeaderFormatError::ChecksumMismatch { expected, actual } => {
                format!(
                    "{} file header checksum mismatch: expected {expected:#x}, actual {actual:#x}",
                    self.format.label
                )
            }
            DurableJournalFileHeaderFormatError::UnknownMagic => {
                format!("invalid {} file header magic", self.format.label)
            }
            DurableJournalFileHeaderFormatError::UnsupportedVersion(version) => {
                format!(
                    "unsupported {} file header version {version}",
                    self.format.label
                )
            }
        };
        self.protocol_error(message)
    }

    fn ensure_file_header(&self, file: &mut File) -> Result<(), DurableJournalAppendError> {
        if file
            .metadata()
            .map_err(|source| ControlPlaneError::io(self.contexts.stat_before_append, source))
            .map_err(DurableJournalAppendError::BeforeReplayableRecord)?
            .len()
            != 0
        {
            self.read_file_base_offset_unlocked()
                .map_err(DurableJournalAppendError::BeforeReplayableRecord)?;
            return Ok(());
        }
        self.write_replayable_bytes(
            file,
            &self.encode_file_header(0),
            self.contexts.write_file_header,
        )
    }

    fn read_file_base_offset_unlocked(&self) -> Result<u64, ControlPlaneError> {
        let header_len = self.format.header_len();
        let file = File::open(&self.path)
            .map_err(|source| ControlPlaneError::io(self.contexts.open_file_header, source))?;
        let mut bytes = Vec::with_capacity(header_len);
        file.take(header_len as u64)
            .read_to_end(&mut bytes)
            .map_err(|source| ControlPlaneError::io(self.contexts.read_file_header, source))?;
        Ok(self.decode_file_header(&bytes)?.0)
    }

    fn encode_file_bytes(&self, base_offset: u64, frames: &[u8]) -> Vec<u8> {
        let mut out = self.encode_file_header(base_offset);
        out.extend_from_slice(frames);
        out
    }

    fn write_replayable_bytes(
        &self,
        writer: &mut impl Write,
        bytes: &[u8],
        context: &'static str,
    ) -> Result<(), DurableJournalAppendError> {
        writer.write_all(bytes).map_err(|source| {
            DurableJournalAppendError::AmbiguousRecordMayExist(ControlPlaneError::io(
                context, source,
            ))
        })
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, ()>, ControlPlaneError> {
        self.io_lock
            .lock()
            .map_err(|_| self.protocol_error(format!("{} lock poisoned", self.format.label)))
    }

    fn protocol_error(&self, message: impl Into<String>) -> ControlPlaneError {
        ControlPlaneError::CommandDecode {
            message: message.into(),
        }
    }

    fn tmp_path(&self) -> PathBuf {
        let file_name = self
            .path
            .file_name()
            .and_then(|file_name| file_name.to_str())
            .unwrap_or("durable-journal");
        self.path
            .with_file_name(format!("{file_name}.tmp.{}", std::process::id()))
    }
}
