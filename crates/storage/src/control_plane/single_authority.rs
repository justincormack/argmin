// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

pub trait ControlPlaneStore {
    fn load(&self) -> Result<Option<ClusterControlSnapshot>, ControlPlaneError>;
    fn checkpoint(
        &self,
        previous_snapshot: Option<&ClusterControlSnapshot>,
        next_snapshot: &ClusterControlSnapshot,
    ) -> Result<(), ControlPlaneError>;

    fn commit_command(
        &self,
        previous_snapshot: &ClusterControlSnapshot,
        _command: &ControlPlaneCommand,
        next_snapshot: &ClusterControlSnapshot,
    ) -> Result<(), ControlPlaneError> {
        self.checkpoint(Some(previous_snapshot), next_snapshot)
    }

    fn ensure_healthy(&self) -> Result<(), ControlPlaneError> {
        Ok(())
    }

    #[cfg(test)]
    fn checkpoint_manually_modified_snapshot_for_test(
        &self,
        previous_snapshot: &ClusterControlSnapshot,
        next_snapshot: &ClusterControlSnapshot,
    ) -> Result<(), ControlPlaneError> {
        self.checkpoint(Some(previous_snapshot), next_snapshot)
    }
}

#[derive(Debug, Default)]
struct FileControlPlaneStoreDurability {
    initialized: bool,
    initial_identity_created: bool,
    poisoned: Option<String>,
    journal_clean_offset: u64,
    commands_since_checkpoint: u64,
    bytes_since_checkpoint: u64,
    first_uncheckpointed_at: Option<Instant>,
    checkpoint_generation: u64,
    published_snapshot_digest: Option<u64>,
    published_chain_digest: Option<u64>,
}

#[derive(Debug, Clone, Default)]
struct SingleAuthorityJournalObserver {
    #[cfg(test)]
    fail_next_file_sync: Arc<std::sync::atomic::AtomicBool>,
    #[cfg(test)]
    fail_next_directory_sync: Arc<std::sync::atomic::AtomicBool>,
}

impl DurableJournalObserver for SingleAuthorityJournalObserver {
    fn record_append(&self, elapsed: Duration, succeeded: bool) {
        observability::record_control_plane_journal_append(elapsed, succeeded);
    }

    fn record_lock_wait(&self, elapsed: Duration) {
        observability::record_control_plane_journal_lock_wait(elapsed);
    }

    fn record_frame_bytes(&self, bytes: usize) {
        observability::record_control_plane_journal_frame_bytes(bytes);
    }

    fn record_file_sync(&self, elapsed: Duration) {
        observability::record_control_plane_journal_file_sync(elapsed);
    }

    fn record_directory_sync(&self, elapsed: Duration) {
        observability::record_control_plane_journal_directory_sync(elapsed);
    }

    fn record_compaction(&self, elapsed: Duration, succeeded: bool) {
        observability::record_control_plane_journal_compaction(elapsed, succeeded);
    }

    fn record_compaction_lock_wait(&self, elapsed: Duration) {
        observability::record_control_plane_journal_compaction_lock_wait(elapsed);
    }

    fn record_compaction_bytes(&self, bytes: usize) {
        observability::record_control_plane_journal_compaction_bytes(bytes);
    }

    fn record_compaction_file_sync(&self, elapsed: Duration) {
        observability::record_control_plane_journal_compaction_file_sync(elapsed);
    }

    fn record_compaction_directory_sync(&self, elapsed: Duration) {
        observability::record_control_plane_journal_compaction_directory_sync(elapsed);
    }

    fn before_file_sync(&self, _path: &Path) -> Result<(), ControlPlaneError> {
        #[cfg(test)]
        if self
            .fail_next_file_sync
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(ControlPlaneError::io(
                "sync single-authority control-plane journal",
                std::io::Error::other(
                    "injected single-authority control-plane journal sync failure",
                ),
            ));
        }
        Ok(())
    }

    fn sync_parent(&self, path: &Path) -> Result<(), ControlPlaneError> {
        #[cfg(test)]
        if self
            .fail_next_directory_sync
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(ControlPlaneError::io(
                "sync single-authority control-plane journal directory",
                std::io::Error::other(
                    "injected single-authority control-plane journal directory sync failure",
                ),
            ));
        }
        let parent = state_parent(path);
        std::fs::File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|source| {
                ControlPlaneError::io(
                    "sync single-authority control-plane journal directory",
                    source,
                )
            })
    }
}

#[derive(Debug, Clone)]
pub struct FileControlPlaneStore {
    path: PathBuf,
    journal: DurableJournalFile<SingleAuthorityJournalObserver>,
    durability: Arc<Mutex<FileControlPlaneStoreDurability>>,
    checkpoint_publication: Arc<Mutex<()>>,
    checkpoint_command_limit: u64,
    checkpoint_byte_limit: u64,
    checkpoint_interval: Duration,
    #[cfg(test)]
    journal_observer: Arc<SingleAuthorityJournalObserver>,
    #[cfg(test)]
    fail_checkpoint_after_anchor: Arc<std::sync::atomic::AtomicBool>,
    #[cfg(test)]
    fail_initial_checkpoint_after_identity: Arc<std::sync::atomic::AtomicBool>,
    #[cfg(test)]
    fail_checkpoint_after_prepared_snapshot_sync: Arc<std::sync::atomic::AtomicBool>,
    #[cfg(test)]
    checkpoint_after_journal_replacement_gate: Arc<(
        Mutex<CheckpointAfterJournalReplacementGate>,
        std::sync::Condvar,
    )>,
    #[cfg(test)]
    commit_before_durability_lock_signal:
        Arc<(Mutex<CommitBeforeDurabilityLockSignal>, std::sync::Condvar)>,
}

struct FileControlPlaneCheckpointCapture {
    store_instance: Arc<Mutex<FileControlPlaneStoreDurability>>,
    checkpoint_generation: u64,
    journal_offset: u64,
    chain_digest: u64,
    captured_at: Instant,
}

#[cfg(test)]
#[derive(Debug, Default)]
struct CheckpointAfterJournalReplacementGate {
    pause: bool,
    reached: bool,
}

#[cfg(test)]
#[derive(Debug, Default)]
struct CommitBeforeDurabilityLockSignal {
    armed: bool,
    reached: bool,
}

pub struct SingleAuthorityDurableCheckpoint {
    store: FileControlPlaneStore,
    capture: FileControlPlaneCheckpointCapture,
    snapshot: ClusterControlSnapshot,
}

impl SingleAuthorityDurableCheckpoint {
    pub fn persist(self) -> Result<(), ControlPlaneError> {
        self.store
            .persist_captured_checkpoint(self.capture, &self.snapshot)
    }
}

impl FileControlPlaneStore {
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self::with_checkpoint_policy(
            path.into(),
            SINGLE_AUTHORITY_JOURNAL_CHECKPOINT_COMMAND_LIMIT,
            SINGLE_AUTHORITY_JOURNAL_CHECKPOINT_BYTE_LIMIT,
            SINGLE_AUTHORITY_JOURNAL_CHECKPOINT_INTERVAL,
        )
    }

    #[cfg(test)]
    fn with_checkpoint_limits(
        path: PathBuf,
        checkpoint_command_limit: u64,
        checkpoint_byte_limit: u64,
    ) -> Self {
        Self::with_checkpoint_policy(
            path,
            checkpoint_command_limit,
            checkpoint_byte_limit,
            SINGLE_AUTHORITY_JOURNAL_CHECKPOINT_INTERVAL,
        )
    }

    fn with_checkpoint_policy(
        path: PathBuf,
        checkpoint_command_limit: u64,
        checkpoint_byte_limit: u64,
        checkpoint_interval: Duration,
    ) -> Self {
        let journal_observer = Arc::new(SingleAuthorityJournalObserver::default());
        let journal = DurableJournalFile::new(
            single_authority_journal_path(&path),
            DurableJournalFormat {
                file_magic: SINGLE_AUTHORITY_JOURNAL_FILE_MAGIC,
                file_version: SINGLE_AUTHORITY_JOURNAL_FILE_VERSION,
                label: "single-authority control-plane journal",
            },
            DurableJournalIoContexts {
                create_directory: "create single-authority control-plane journal directory",
                open_for_append: "open single-authority control-plane journal for append",
                write_frame_length: "write single-authority control-plane journal frame length",
                write_frame: "write single-authority control-plane journal frame",
                sync_file: "sync single-authority control-plane journal",
                stat_for_status: "stat single-authority control-plane journal",
                open_for_replay: "open single-authority control-plane journal for replay",
                read_for_replay: "read single-authority control-plane journal",
                open_for_tail_truncation:
                    "open single-authority control-plane journal for tail truncation",
                truncate_torn_tail: "truncate torn single-authority control-plane journal tail",
                sync_truncated_tail: "sync truncated single-authority control-plane journal tail",
                read_for_compaction: "read single-authority control-plane journal for compaction",
                create_compacted_temp: "create compacted single-authority control-plane journal",
                write_compacted_temp: "write compacted single-authority control-plane journal",
                sync_compacted_temp: "sync compacted single-authority control-plane journal",
                commit_compacted: "commit compacted single-authority control-plane journal",
                stat_before_append: "stat single-authority control-plane journal before append",
                write_file_header: "write single-authority control-plane journal header",
                open_file_header: "open single-authority control-plane journal header",
                read_file_header: "read single-authority control-plane journal header",
            },
            Arc::clone(&journal_observer),
        );
        Self {
            path,
            journal,
            durability: Arc::new(Mutex::new(FileControlPlaneStoreDurability::default())),
            checkpoint_publication: Arc::new(Mutex::new(())),
            checkpoint_command_limit,
            checkpoint_byte_limit,
            checkpoint_interval,
            #[cfg(test)]
            journal_observer,
            #[cfg(test)]
            fail_checkpoint_after_anchor: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(test)]
            fail_initial_checkpoint_after_identity: Arc::new(std::sync::atomic::AtomicBool::new(
                false,
            )),
            #[cfg(test)]
            fail_checkpoint_after_prepared_snapshot_sync: Arc::new(
                std::sync::atomic::AtomicBool::new(false),
            ),
            #[cfg(test)]
            checkpoint_after_journal_replacement_gate: Arc::new((
                Mutex::new(CheckpointAfterJournalReplacementGate::default()),
                std::sync::Condvar::new(),
            )),
            #[cfg(test)]
            commit_before_durability_lock_signal: Arc::new((
                Mutex::new(CommitBeforeDurabilityLockSignal::default()),
                std::sync::Condvar::new(),
            )),
        }
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[must_use]
    pub fn journal_path(&self) -> &Path {
        self.journal.path()
    }

    #[cfg(test)]
    fn fail_next_journal_file_sync(&self) {
        self.journal_observer
            .fail_next_file_sync
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    fn fail_next_journal_directory_sync(&self) {
        self.journal_observer
            .fail_next_directory_sync
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    fn fail_next_checkpoint_after_anchor(&self) {
        self.fail_checkpoint_after_anchor
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    fn fail_next_initial_checkpoint_after_identity(&self) {
        self.fail_initial_checkpoint_after_identity
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    fn fail_next_checkpoint_after_prepared_snapshot_sync(&self) {
        self.fail_checkpoint_after_prepared_snapshot_sync
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    fn pause_next_checkpoint_after_journal_replacement(&self) {
        let (state, _) = &*self.checkpoint_after_journal_replacement_gate;
        let mut state = state.lock().unwrap();
        state.pause = true;
        state.reached = false;
    }

    #[cfg(test)]
    fn wait_for_checkpoint_journal_replacement(&self, timeout: Duration) -> bool {
        let (state, reached) = &*self.checkpoint_after_journal_replacement_gate;
        let state = state.lock().unwrap();
        let (state, _) = reached
            .wait_timeout_while(state, timeout, |state| !state.reached)
            .unwrap();
        state.reached
    }

    #[cfg(test)]
    fn release_checkpoint_after_journal_replacement(&self) {
        let (state, released) = &*self.checkpoint_after_journal_replacement_gate;
        let mut state = state.lock().unwrap();
        state.pause = false;
        released.notify_all();
    }

    #[cfg(test)]
    fn wait_after_checkpoint_journal_replacement_if_requested(&self) {
        let (state, released) = &*self.checkpoint_after_journal_replacement_gate;
        let mut state = state.lock().unwrap();
        if !state.pause {
            return;
        }
        state.reached = true;
        released.notify_all();
        while state.pause {
            state = released.wait(state).unwrap();
        }
    }

    #[cfg(test)]
    fn arm_commit_before_durability_lock_signal(&self) {
        let (state, _) = &*self.commit_before_durability_lock_signal;
        let mut state = state.lock().unwrap();
        state.armed = true;
        state.reached = false;
    }

    #[cfg(test)]
    fn signal_commit_before_durability_lock_if_armed(&self) {
        let (state, reached) = &*self.commit_before_durability_lock_signal;
        let mut state = state.lock().unwrap();
        if !state.armed {
            return;
        }
        state.armed = false;
        state.reached = true;
        reached.notify_all();
    }

    #[cfg(test)]
    fn wait_for_commit_before_durability_lock(&self, timeout: Duration) -> bool {
        let (state, reached) = &*self.commit_before_durability_lock_signal;
        let state = state.lock().unwrap();
        let (state, _) = reached
            .wait_timeout_while(state, timeout, |state| !state.reached)
            .unwrap();
        state.reached
    }

    pub fn load_authority_clock_restart_checkpoint(
        &self,
        binding: ControlPlaneAuthorityClockCheckpointBinding,
    ) -> Result<Option<ControlPlaneAuthorityClockRestartCheckpoint>, ControlPlaneError> {
        load_authority_clock_restart_checkpoint(&self.path, binding)
    }

    pub fn load_or_create_authority_clock_checkpoint_binding(
        &self,
    ) -> Result<ControlPlaneAuthorityClockCheckpointBinding, ControlPlaneError> {
        let (binding, created) =
            self.load_or_create_authority_clock_checkpoint_binding_untracked()?;
        if created {
            self.lock_durability()?.initial_identity_created = true;
        }
        Ok(binding)
    }

    fn load_or_create_authority_clock_checkpoint_binding_untracked(
        &self,
    ) -> Result<(ControlPlaneAuthorityClockCheckpointBinding, bool), ControlPlaneError> {
        match load_single_authority_clock_checkpoint_binding(&self.path)? {
            Some(binding) => Ok((binding, false)),
            None if self.path.exists() => Err(ControlPlaneError::AuthorityClockCheckpoint {
                message: "existing single-authority state is missing its durable identity"
                    .to_owned(),
            }),
            None => {
                let binding =
                    ControlPlaneAuthorityClockCheckpointBinding::generate_single_authority()?;
                store_single_authority_clock_checkpoint_binding(&self.path, binding)?;
                Ok((binding, true))
            }
        }
    }
}

fn authority_clock_restart_checkpoint_path(path: &Path) -> PathBuf {
    let mut checkpoint_path = path.as_os_str().to_os_string();
    checkpoint_path.push(".clock");
    PathBuf::from(checkpoint_path)
}

fn authority_clock_restart_checkpoint_tmp_path(path: &Path) -> PathBuf {
    let mut tmp_path = authority_clock_restart_checkpoint_path(path).into_os_string();
    tmp_path.push(".tmp");
    PathBuf::from(tmp_path)
}

fn single_authority_identity_path(path: &Path) -> PathBuf {
    let mut identity_path = path.as_os_str().to_os_string();
    identity_path.push(".identity");
    PathBuf::from(identity_path)
}

fn single_authority_identity_tmp_path(path: &Path) -> PathBuf {
    let mut tmp_path = single_authority_identity_path(path).into_os_string();
    tmp_path.push(".tmp");
    PathBuf::from(tmp_path)
}

fn single_authority_journal_path(path: &Path) -> PathBuf {
    let mut journal_path = path.as_os_str().to_os_string();
    journal_path.push(".journal");
    PathBuf::from(journal_path)
}

fn single_authority_snapshot_tmp_path(path: &Path) -> PathBuf {
    path.with_extension("tmp")
}

fn single_authority_initialized_path(path: &Path) -> PathBuf {
    let mut initialized_path = path.as_os_str().to_os_string();
    initialized_path.push(".initialized");
    PathBuf::from(initialized_path)
}

fn single_authority_initialized_tmp_path(path: &Path) -> PathBuf {
    let mut tmp_path = single_authority_initialized_path(path).into_os_string();
    tmp_path.push(".tmp");
    PathBuf::from(tmp_path)
}

enum FixedControlPlaneSidecarReadError {
    Truncated,
    InvalidLength(ControlPlaneError),
    Invalid(ControlPlaneError),
}

fn read_fixed_control_plane_sidecar<const N: usize>(
    path: &Path,
    context: &'static str,
) -> Result<Option<[u8; N]>, FixedControlPlaneSidecarReadError> {
    let metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(FixedControlPlaneSidecarReadError::Invalid(
                ControlPlaneError::io(context, source),
            ));
        }
    };
    if metadata.len() < N as u64 {
        return Err(FixedControlPlaneSidecarReadError::Truncated);
    }
    if metadata.len() > N as u64 {
        return Err(FixedControlPlaneSidecarReadError::InvalidLength(
            ControlPlaneError::AuthorityClockCheckpoint {
                message: format!(
                    "{} length {} does not match required fixed length {N}",
                    path.display(),
                    metadata.len()
                ),
            },
        ));
    }
    let mut file = std::fs::File::open(path).map_err(|source| {
        FixedControlPlaneSidecarReadError::Invalid(ControlPlaneError::io(context, source))
    })?;
    let mut bytes = [0u8; N];
    if let Err(source) = file.read_exact(&mut bytes) {
        return if source.kind() == ErrorKind::UnexpectedEof {
            Err(FixedControlPlaneSidecarReadError::Truncated)
        } else {
            Err(FixedControlPlaneSidecarReadError::Invalid(
                ControlPlaneError::io(context, source),
            ))
        };
    }
    let mut trailing = [0u8; 1];
    if file
        .read(&mut trailing)
        .map_err(|source| {
            FixedControlPlaneSidecarReadError::Invalid(ControlPlaneError::io(context, source))
        })?
        != 0
    {
        return Err(FixedControlPlaneSidecarReadError::InvalidLength(
            ControlPlaneError::AuthorityClockCheckpoint {
                message: format!("{} grew while it was being read", path.display()),
            },
        ));
    }
    Ok(Some(bytes))
}

struct ControlPlaneSidecarIoContexts {
    create: &'static str,
    write: &'static str,
    sync: &'static str,
    rename: &'static str,
    directory: &'static str,
}

fn store_control_plane_sidecar(
    path: &Path,
    tmp_path: &Path,
    bytes: &[u8],
    contexts: ControlPlaneSidecarIoContexts,
) -> Result<(), ControlPlaneError> {
    let parent = state_parent(path);
    create_control_plane_directory_all_durable(parent)?;
    {
        let mut file = std::fs::File::create(tmp_path)
            .map_err(|source| ControlPlaneError::io(contexts.create, source))?;
        file.write_all(bytes)
            .map_err(|source| ControlPlaneError::io(contexts.write, source))?;
        file.sync_all()
            .map_err(|source| ControlPlaneError::io(contexts.sync, source))?;
    }
    std::fs::rename(tmp_path, path)
        .map_err(|source| ControlPlaneError::io(contexts.rename, source))?;
    std::fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| ControlPlaneError::io(contexts.directory, source))?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SingleAuthorityIdentityFormatError {
    Truncated,
    UnknownMagic,
    UnsupportedVersion(u16),
}

impl std::fmt::Display for SingleAuthorityIdentityFormatError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated => formatter.write_str("single-authority durable identity is truncated"),
            Self::UnknownMagic => {
                formatter.write_str("single-authority durable identity magic mismatch")
            }
            Self::UnsupportedVersion(version) => write!(
                formatter,
                "unsupported single-authority durable identity version {version}"
            ),
        }
    }
}

#[derive(Debug)]
enum SingleAuthorityIdentityDecodeError {
    Format(SingleAuthorityIdentityFormatError),
    Invalid(ControlPlaneError),
}

impl SingleAuthorityIdentityDecodeError {
    fn into_control_plane_error(self) -> ControlPlaneError {
        match self {
            Self::Format(error) => ControlPlaneError::AuthorityClockCheckpoint {
                message: error.to_string(),
            },
            Self::Invalid(error) => error,
        }
    }
}

fn decode_single_authority_clock_checkpoint_binding(
    bytes: &[u8],
) -> Result<ControlPlaneAuthorityClockCheckpointBinding, SingleAuthorityIdentityDecodeError> {
    if bytes.len() < CONTROL_PLANE_STATE_IDENTITY_LEN {
        return Err(SingleAuthorityIdentityDecodeError::Format(
            SingleAuthorityIdentityFormatError::Truncated,
        ));
    }
    if bytes.len() != CONTROL_PLANE_STATE_IDENTITY_LEN {
        return Err(SingleAuthorityIdentityDecodeError::Invalid(
            ControlPlaneError::AuthorityClockCheckpoint {
                message: format!(
                    "single-authority durable identity length {} does not match required fixed length {CONTROL_PLANE_STATE_IDENTITY_LEN}",
                    bytes.len()
                ),
            },
        ));
    }
    let (body, checksum_bytes) = bytes.split_at(CONTROL_PLANE_STATE_IDENTITY_LEN - 8);
    if checksum::crc64::checksum(body)
        != u64::from_be_bytes(
            checksum_bytes
                .try_into()
                .expect("identity checksum has fixed length"),
        )
    {
        return Err(SingleAuthorityIdentityDecodeError::Invalid(
            ControlPlaneError::AuthorityClockCheckpoint {
                message: "single-authority durable identity checksum mismatch".to_owned(),
            },
        ));
    }
    if &body[..CONTROL_PLANE_STATE_IDENTITY_MAGIC.len()] != CONTROL_PLANE_STATE_IDENTITY_MAGIC {
        return Err(SingleAuthorityIdentityDecodeError::Format(
            SingleAuthorityIdentityFormatError::UnknownMagic,
        ));
    }
    let version_offset = CONTROL_PLANE_STATE_IDENTITY_MAGIC.len();
    let version = u16::from_be_bytes(
        body[version_offset..version_offset + 2]
            .try_into()
            .expect("identity version has fixed length"),
    );
    if version != CONTROL_PLANE_STATE_IDENTITY_VERSION {
        return Err(SingleAuthorityIdentityDecodeError::Format(
            SingleAuthorityIdentityFormatError::UnsupportedVersion(version),
        ));
    }
    let mut binding = [0u8; CONTROL_PLANE_CLOCK_CHECKPOINT_BINDING_LEN];
    binding.copy_from_slice(&body[version_offset + 2..]);
    Ok(ControlPlaneAuthorityClockCheckpointBinding(binding))
}

fn load_single_authority_clock_checkpoint_binding_classified(
    durable_state_path: &Path,
) -> Result<Option<ControlPlaneAuthorityClockCheckpointBinding>, SingleAuthorityIdentityDecodeError>
{
    let identity_path = single_authority_identity_path(durable_state_path);
    let Some(bytes) = read_fixed_control_plane_sidecar::<CONTROL_PLANE_STATE_IDENTITY_LEN>(
        &identity_path,
        "load single-authority control-plane durable identity",
    )
    .map_err(|error| match error {
        FixedControlPlaneSidecarReadError::Truncated => SingleAuthorityIdentityDecodeError::Format(
            SingleAuthorityIdentityFormatError::Truncated,
        ),
        FixedControlPlaneSidecarReadError::InvalidLength(error)
        | FixedControlPlaneSidecarReadError::Invalid(error) => {
            SingleAuthorityIdentityDecodeError::Invalid(error)
        }
    })?
    else {
        return Ok(None);
    };
    decode_single_authority_clock_checkpoint_binding(&bytes).map(Some)
}

fn load_single_authority_clock_checkpoint_binding(
    durable_state_path: &Path,
) -> Result<Option<ControlPlaneAuthorityClockCheckpointBinding>, ControlPlaneError> {
    load_single_authority_clock_checkpoint_binding_classified(durable_state_path)
        .map_err(SingleAuthorityIdentityDecodeError::into_control_plane_error)
}

fn encode_single_authority_clock_checkpoint_binding(
    binding: ControlPlaneAuthorityClockCheckpointBinding,
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(CONTROL_PLANE_STATE_IDENTITY_LEN);
    bytes.extend_from_slice(CONTROL_PLANE_STATE_IDENTITY_MAGIC);
    bytes.extend_from_slice(&CONTROL_PLANE_STATE_IDENTITY_VERSION.to_be_bytes());
    bytes.extend_from_slice(&binding.0);
    bytes.extend_from_slice(&checksum::crc64::checksum(&bytes).to_be_bytes());
    bytes
}

fn store_single_authority_clock_checkpoint_binding(
    durable_state_path: &Path,
    binding: ControlPlaneAuthorityClockCheckpointBinding,
) -> Result<(), ControlPlaneError> {
    let bytes = encode_single_authority_clock_checkpoint_binding(binding);
    store_control_plane_sidecar(
        &single_authority_identity_path(durable_state_path),
        &single_authority_identity_tmp_path(durable_state_path),
        &bytes,
        ControlPlaneSidecarIoContexts {
            create: "create single-authority control-plane durable identity",
            write: "write single-authority control-plane durable identity",
            sync: "sync single-authority control-plane durable identity",
            rename: "commit single-authority control-plane durable identity",
            directory: "sync single-authority control-plane durable identity directory",
        },
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SingleAuthorityInitializationMarkerFormatError {
    Truncated,
    InvalidLength,
    UnknownMagic,
    UnsupportedVersion(u16),
}

impl std::fmt::Display for SingleAuthorityInitializationMarkerFormatError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated => {
                formatter.write_str("single-authority initialization marker is truncated")
            }
            Self::InvalidLength => formatter.write_str(
                "single-authority initialization marker length does not match required fixed length",
            ),
            Self::UnknownMagic => {
                formatter.write_str("single-authority initialization marker magic mismatch")
            }
            Self::UnsupportedVersion(version) => write!(
                formatter,
                "unsupported single-authority initialization marker version {version}"
            ),
        }
    }
}

#[derive(Debug)]
enum SingleAuthorityInitializationMarkerDecodeError {
    Format(SingleAuthorityInitializationMarkerFormatError),
    Invalid(ControlPlaneError),
}

impl SingleAuthorityInitializationMarkerDecodeError {
    fn into_control_plane_error(self) -> ControlPlaneError {
        match self {
            Self::Format(error) => ControlPlaneError::CommandDecode {
                message: error.to_string(),
            },
            Self::Invalid(error) => error,
        }
    }
}

fn decode_single_authority_initialized_binding(
    bytes: &[u8],
) -> Result<ControlPlaneAuthorityClockCheckpointBinding, SingleAuthorityInitializationMarkerDecodeError>
{
    if bytes.len() < SINGLE_AUTHORITY_INITIALIZED_LEN {
        return Err(SingleAuthorityInitializationMarkerDecodeError::Format(
            SingleAuthorityInitializationMarkerFormatError::Truncated,
        ));
    }
    if bytes.len() != SINGLE_AUTHORITY_INITIALIZED_LEN {
        return Err(SingleAuthorityInitializationMarkerDecodeError::Format(
            SingleAuthorityInitializationMarkerFormatError::InvalidLength,
        ));
    }
    let (body, checksum_bytes) = bytes.split_at(SINGLE_AUTHORITY_INITIALIZED_LEN - 8);
    if checksum::crc64::checksum(body)
        != u64::from_be_bytes(
            checksum_bytes
                .try_into()
                .expect("initialization marker checksum has fixed length"),
        )
    {
        return Err(SingleAuthorityInitializationMarkerDecodeError::Invalid(
            ControlPlaneError::CommandDecode {
                message: "single-authority initialization marker checksum mismatch".to_owned(),
            },
        ));
    }
    if &body[..SINGLE_AUTHORITY_INITIALIZED_MAGIC.len()] != SINGLE_AUTHORITY_INITIALIZED_MAGIC {
        return Err(SingleAuthorityInitializationMarkerDecodeError::Format(
            SingleAuthorityInitializationMarkerFormatError::UnknownMagic,
        ));
    }
    let version_offset = SINGLE_AUTHORITY_INITIALIZED_MAGIC.len();
    let version = u16::from_be_bytes(
        body[version_offset..version_offset + 2]
            .try_into()
            .expect("initialization marker version has fixed length"),
    );
    if version != SINGLE_AUTHORITY_INITIALIZED_VERSION {
        return Err(SingleAuthorityInitializationMarkerDecodeError::Format(
            SingleAuthorityInitializationMarkerFormatError::UnsupportedVersion(version),
        ));
    }
    let mut binding = [0; CONTROL_PLANE_CLOCK_CHECKPOINT_BINDING_LEN];
    binding.copy_from_slice(&body[version_offset + 2..]);
    Ok(ControlPlaneAuthorityClockCheckpointBinding(binding))
}

fn load_single_authority_initialized_binding_classified(
    durable_state_path: &Path,
) -> Result<
    Option<ControlPlaneAuthorityClockCheckpointBinding>,
    SingleAuthorityInitializationMarkerDecodeError,
> {
    let initialized_path = single_authority_initialized_path(durable_state_path);
    let Some(bytes) = read_fixed_control_plane_sidecar::<SINGLE_AUTHORITY_INITIALIZED_LEN>(
        &initialized_path,
        "load single-authority control-plane initialization marker",
    )
    .map_err(|error| match error {
        FixedControlPlaneSidecarReadError::Truncated => {
            SingleAuthorityInitializationMarkerDecodeError::Format(
                SingleAuthorityInitializationMarkerFormatError::Truncated,
            )
        }
        FixedControlPlaneSidecarReadError::InvalidLength(_) => {
            SingleAuthorityInitializationMarkerDecodeError::Format(
                SingleAuthorityInitializationMarkerFormatError::InvalidLength,
            )
        }
        FixedControlPlaneSidecarReadError::Invalid(error) => {
            SingleAuthorityInitializationMarkerDecodeError::Invalid(error)
        }
    })?
    else {
        return Ok(None);
    };
    decode_single_authority_initialized_binding(&bytes).map(Some)
}

fn load_single_authority_initialized_binding(
    durable_state_path: &Path,
) -> Result<Option<ControlPlaneAuthorityClockCheckpointBinding>, ControlPlaneError> {
    load_single_authority_initialized_binding_classified(durable_state_path)
        .map_err(SingleAuthorityInitializationMarkerDecodeError::into_control_plane_error)
}

fn encode_single_authority_initialized_binding(
    binding: ControlPlaneAuthorityClockCheckpointBinding,
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(SINGLE_AUTHORITY_INITIALIZED_LEN);
    bytes.extend_from_slice(SINGLE_AUTHORITY_INITIALIZED_MAGIC);
    bytes.extend_from_slice(&SINGLE_AUTHORITY_INITIALIZED_VERSION.to_be_bytes());
    bytes.extend_from_slice(&binding.0);
    bytes.extend_from_slice(&checksum::crc64::checksum(&bytes).to_be_bytes());
    bytes
}

fn store_single_authority_initialized_binding(
    durable_state_path: &Path,
    binding: ControlPlaneAuthorityClockCheckpointBinding,
) -> Result<(), ControlPlaneError> {
    let bytes = encode_single_authority_initialized_binding(binding);
    store_control_plane_sidecar(
        &single_authority_initialized_path(durable_state_path),
        &single_authority_initialized_tmp_path(durable_state_path),
        &bytes,
        ControlPlaneSidecarIoContexts {
            create: "create single-authority control-plane initialization marker",
            write: "write single-authority control-plane initialization marker",
            sync: "sync single-authority control-plane initialization marker",
            rename: "commit single-authority control-plane initialization marker",
            directory: "sync single-authority control-plane initialization marker directory",
        },
    )
}

pub fn load_authority_clock_restart_checkpoint(
    durable_state_path: &Path,
    expected_binding: ControlPlaneAuthorityClockCheckpointBinding,
) -> Result<Option<ControlPlaneAuthorityClockRestartCheckpoint>, ControlPlaneError> {
    load_authority_clock_restart_checkpoint_classified(durable_state_path, expected_binding)
        .map_err(ControlPlaneAuthorityClockRestartCheckpointDecodeError::into_control_plane_error)
}

fn load_authority_clock_restart_checkpoint_classified(
    durable_state_path: &Path,
    expected_binding: ControlPlaneAuthorityClockCheckpointBinding,
) -> Result<
    Option<ControlPlaneAuthorityClockRestartCheckpoint>,
    ControlPlaneAuthorityClockRestartCheckpointDecodeError,
> {
    let checkpoint_path = authority_clock_restart_checkpoint_path(durable_state_path);
    let Some(bytes) = read_fixed_control_plane_sidecar::<CONTROL_PLANE_CLOCK_CHECKPOINT_LEN>(
        &checkpoint_path,
        "load control-plane authority clock checkpoint",
    )
    .map_err(|error| match error {
        FixedControlPlaneSidecarReadError::Truncated => {
            ControlPlaneAuthorityClockRestartCheckpointDecodeError::Format(
                ControlPlaneAuthorityClockRestartCheckpointFormatError::Truncated,
            )
        }
        FixedControlPlaneSidecarReadError::InvalidLength(error)
        | FixedControlPlaneSidecarReadError::Invalid(error) => {
            ControlPlaneAuthorityClockRestartCheckpointDecodeError::Invalid(error)
        }
    })?
    else {
        return Ok(None);
    };
    ControlPlaneAuthorityClockRestartCheckpoint::decode_classified(&bytes, expected_binding)
        .map(Some)
}

pub fn load_authority_clock_restart_checkpoint_for_startup(
    durable_state_path: &Path,
    expected_binding: ControlPlaneAuthorityClockCheckpointBinding,
) -> Result<Option<ControlPlaneAuthorityClockRestartCheckpoint>, ControlPlaneError> {
    match load_authority_clock_restart_checkpoint_classified(
        durable_state_path,
        expected_binding,
    ) {
        Ok(checkpoint) => Ok(checkpoint),
        Err(ControlPlaneAuthorityClockRestartCheckpointDecodeError::Format(
            error @ (ControlPlaneAuthorityClockRestartCheckpointFormatError::UnknownMagic
            | ControlPlaneAuthorityClockRestartCheckpointFormatError::UnsupportedVersion(_)),
        )) => Err(ControlPlaneAuthorityClockRestartCheckpointDecodeError::Format(error)
            .into_control_plane_error()),
        Err(
            error @ (ControlPlaneAuthorityClockRestartCheckpointDecodeError::Format(
                ControlPlaneAuthorityClockRestartCheckpointFormatError::Truncated,
            )
            | ControlPlaneAuthorityClockRestartCheckpointDecodeError::Invalid(
                ControlPlaneError::AuthorityClockCheckpoint { .. },
            )),
        ) => {
            eprintln!(
                "control-plane authority clock checkpoint is invalid; treating restart continuity evidence as absent: {}",
                error.into_control_plane_error()
            );
            Ok(None)
        }
        Err(ControlPlaneAuthorityClockRestartCheckpointDecodeError::Invalid(error)) => Err(error),
    }
}

pub fn invalidate_authority_clock_restart_checkpoint(
    durable_state_path: &Path,
) -> Result<(), ControlPlaneError> {
    let checkpoint_path = authority_clock_restart_checkpoint_path(durable_state_path);
    let removed = match std::fs::remove_file(&checkpoint_path) {
        Ok(()) => true,
        Err(error) if error.kind() == ErrorKind::NotFound => false,
        Err(source) => {
            return Err(ControlPlaneError::io(
                "invalidate control-plane authority clock checkpoint",
                source,
            ));
        }
    };
    if !removed {
        return Ok(());
    }
    std::fs::File::open(state_parent(&checkpoint_path))
        .and_then(|directory| directory.sync_all())
        .map_err(|source| {
            ControlPlaneError::io(
                "sync invalidated control-plane authority clock checkpoint directory",
                source,
            )
        })?;
    Ok(())
}

pub fn store_authority_clock_restart_checkpoint(
    durable_state_path: &Path,
    binding: ControlPlaneAuthorityClockCheckpointBinding,
    authority_generation: u64,
    committed_timestamp_high_water_ms: Option<u64>,
) -> Result<ControlPlaneAuthorityClockRestartCheckpoint, ControlPlaneError> {
    let checkpoint = ControlPlaneAuthorityClockRestartCheckpoint::from_process_clock(
        binding,
        authority_generation,
        committed_timestamp_high_water_ms,
    )?;
    store_authority_clock_restart_checkpoint_value(durable_state_path, checkpoint)?;
    Ok(checkpoint)
}

pub fn store_validated_authority_clock_restart_checkpoint(
    durable_state_path: &Path,
    binding: ControlPlaneAuthorityClockCheckpointBinding,
    committed_timestamp_high_water_ms: Option<u64>,
    authority_clock: &mut ControlPlaneAuthorityClock,
) -> Result<ControlPlaneAuthorityClockRestartCheckpoint, ControlPlaneError> {
    let sample = control_plane_process_clock_sample()?;
    let checkpoint = validated_authority_clock_restart_checkpoint(
        binding,
        committed_timestamp_high_water_ms,
        authority_clock,
        sample.wall_time_ms(),
        sample.health_time_ms(),
    )?;
    store_authority_clock_restart_checkpoint_value(durable_state_path, checkpoint)?;
    Ok(checkpoint)
}

fn validated_authority_clock_restart_checkpoint(
    binding: ControlPlaneAuthorityClockCheckpointBinding,
    committed_timestamp_high_water_ms: Option<u64>,
    authority_clock: &mut ControlPlaneAuthorityClock,
    wall_time_ms: u64,
    health_time_ms: Option<u64>,
) -> Result<ControlPlaneAuthorityClockRestartCheckpoint, ControlPlaneError> {
    authority_clock.effective_now_ms(wall_time_ms, health_time_ms)?;
    let health_time_ms =
        health_time_ms.ok_or(ControlPlaneError::AuthorityClockSourceUnavailable)?;
    Ok(ControlPlaneAuthorityClockRestartCheckpoint::new(
        binding,
        authority_clock.generation,
        committed_timestamp_high_water_ms,
        wall_time_ms,
        health_time_ms,
    ))
}

fn store_authority_clock_restart_checkpoint_value(
    durable_state_path: &Path,
    checkpoint: ControlPlaneAuthorityClockRestartCheckpoint,
) -> Result<(), ControlPlaneError> {
    let checkpoint_path = authority_clock_restart_checkpoint_path(durable_state_path);
    let tmp_path = authority_clock_restart_checkpoint_tmp_path(durable_state_path);
    store_control_plane_sidecar(
        &checkpoint_path,
        &tmp_path,
        &checkpoint.encode(),
        ControlPlaneSidecarIoContexts {
            create: "create control-plane authority clock checkpoint",
            write: "write control-plane authority clock checkpoint",
            sync: "sync control-plane authority clock checkpoint",
            rename: "commit control-plane authority clock checkpoint",
            directory: "sync control-plane authority clock checkpoint directory",
        },
    )?;
    Ok(())
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn prepare_unsupported_authority_clock_checkpoint_restart_for_test(
    durable_state_path: &Path,
    version: u16,
) -> Result<(), ControlPlaneError> {
    if !matches!(version, 1 | 3) {
        return Err(ControlPlaneError::CommandDecode {
            message: "test checkpoint version must be one of the retained adjacent fixtures"
                .to_owned(),
        });
    }

    let checkpoint_path = authority_clock_restart_checkpoint_path(durable_state_path);
    let mut checkpoint = std::fs::read(&checkpoint_path).map_err(|source| {
        ControlPlaneError::io(
            "read authority-clock checkpoint for unsupported-version test",
            source,
        )
    })?;
    if checkpoint.len() != CONTROL_PLANE_CLOCK_CHECKPOINT_LEN {
        return Err(ControlPlaneError::AuthorityClockCheckpoint {
            message: format!(
                "test checkpoint length {} does not match required fixed length {CONTROL_PLANE_CLOCK_CHECKPOINT_LEN}",
                checkpoint.len()
            ),
        });
    }
    let version_offset = CONTROL_PLANE_CLOCK_CHECKPOINT_MAGIC.len();
    checkpoint[version_offset..version_offset + 2].copy_from_slice(&version.to_be_bytes());
    let checksum_offset = checkpoint.len() - CONTROL_PLANE_CLOCK_CHECKPOINT_CHECKSUM_LEN;
    let checksum = checksum::crc64::checksum(&checkpoint[..checksum_offset]);
    checkpoint[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
    {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&checkpoint_path)
            .map_err(|source| {
                ControlPlaneError::io(
                    "open authority-clock checkpoint for unsupported-version test",
                    source,
                )
            })?;
        file.write_all(&checkpoint).map_err(|source| {
            ControlPlaneError::io(
                "write authority-clock checkpoint for unsupported-version test",
                source,
            )
        })?;
        file.sync_all().map_err(|source| {
            ControlPlaneError::io(
                "sync authority-clock checkpoint for unsupported-version test",
                source,
            )
        })?;
    }

    let journal_path = single_authority_journal_path(durable_state_path);
    let mut journal = std::fs::OpenOptions::new()
        .append(true)
        .open(&journal_path)
        .map_err(|source| {
            ControlPlaneError::io(
                "open single-authority journal for checkpoint-ordering test",
                source,
            )
        })?;
    journal.write_all(&[0xa3, 0xc1]).map_err(|source| {
        ControlPlaneError::io(
            "append recoverable single-authority journal tail for checkpoint-ordering test",
            source,
        )
    })?;
    journal.sync_all().map_err(|source| {
        ControlPlaneError::io(
            "sync recoverable single-authority journal tail for checkpoint-ordering test",
            source,
        )
    })?;
    Ok(())
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn prepare_unsupported_single_authority_identity_restart_for_test(
    durable_state_path: &Path,
    version: u16,
) -> Result<(), ControlPlaneError> {
    if !matches!(version, 0 | 2) {
        return Err(ControlPlaneError::CommandDecode {
            message: "test durable-identity version must be one of the retained adjacent fixtures"
                .to_owned(),
        });
    }

    let binding = ControlPlaneAuthorityClockCheckpointBinding([0x42; 32]);
    store_single_authority_clock_checkpoint_binding(durable_state_path, binding)?;
    let identity_path = single_authority_identity_path(durable_state_path);
    let mut identity = std::fs::read(&identity_path).map_err(|source| {
        ControlPlaneError::io(
            "read single-authority identity for unsupported-version test",
            source,
        )
    })?;
    let version_offset = CONTROL_PLANE_STATE_IDENTITY_MAGIC.len();
    identity[version_offset..version_offset + 2].copy_from_slice(&version.to_be_bytes());
    let checksum_offset = identity.len() - std::mem::size_of::<u64>();
    let checksum = checksum::crc64::checksum(&identity[..checksum_offset]);
    identity[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&identity_path)
        .map_err(|source| {
            ControlPlaneError::io(
                "open single-authority identity for unsupported-version test",
                source,
            )
        })?;
    file.write_all(&identity).map_err(|source| {
        ControlPlaneError::io(
            "write single-authority identity for unsupported-version test",
            source,
        )
    })?;
    file.sync_all().map_err(|source| {
        ControlPlaneError::io(
            "sync single-authority identity for unsupported-version test",
            source,
        )
    })?;
    Ok(())
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn prepare_unsupported_single_authority_initialization_restart_for_test(
    durable_state_path: &Path,
    version: u16,
) -> Result<(), ControlPlaneError> {
    if !matches!(version, 0 | 2) {
        return Err(ControlPlaneError::CommandDecode {
            message:
                "test initialization-marker version must be one of the retained adjacent fixtures"
                    .to_owned(),
        });
    }

    let initialized_path = single_authority_initialized_path(durable_state_path);
    let mut marker = std::fs::read(&initialized_path).map_err(|source| {
        ControlPlaneError::io(
            "read single-authority initialization marker for unsupported-version test",
            source,
        )
    })?;
    if marker.len() != SINGLE_AUTHORITY_INITIALIZED_LEN {
        return Err(ControlPlaneError::AuthorityClockCheckpoint {
            message: format!(
                "test initialization-marker length {} does not match required fixed length {SINGLE_AUTHORITY_INITIALIZED_LEN}",
                marker.len()
            ),
        });
    }
    let version_offset = SINGLE_AUTHORITY_INITIALIZED_MAGIC.len();
    marker[version_offset..version_offset + 2].copy_from_slice(&version.to_be_bytes());
    let checksum_offset = marker.len() - std::mem::size_of::<u64>();
    let checksum = checksum::crc64::checksum(&marker[..checksum_offset]);
    marker[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
    {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&initialized_path)
            .map_err(|source| {
                ControlPlaneError::io(
                    "open single-authority initialization marker for unsupported-version test",
                    source,
                )
            })?;
        file.write_all(&marker).map_err(|source| {
            ControlPlaneError::io(
                "write single-authority initialization marker for unsupported-version test",
                source,
            )
        })?;
        file.sync_all().map_err(|source| {
            ControlPlaneError::io(
                "sync single-authority initialization marker for unsupported-version test",
                source,
            )
        })?;
    }

    let journal_path = single_authority_journal_path(durable_state_path);
    let mut journal = std::fs::OpenOptions::new()
        .append(true)
        .open(&journal_path)
        .map_err(|source| {
            ControlPlaneError::io(
                "open single-authority journal for initialization-marker ordering test",
                source,
            )
        })?;
    journal.write_all(&[0xa3, 0xc1]).map_err(|source| {
        ControlPlaneError::io(
            "append recoverable single-authority journal tail for initialization-marker ordering test",
            source,
        )
    })?;
    journal.sync_all().map_err(|source| {
        ControlPlaneError::io(
            "sync recoverable single-authority journal tail for initialization-marker ordering test",
            source,
        )
    })?;
    Ok(())
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn prepare_unsupported_single_authority_journal_file_restart_for_test(
    durable_state_path: &Path,
    version: u16,
) -> Result<(), ControlPlaneError> {
    if !matches!(version, 1 | 3) {
        return Err(ControlPlaneError::CommandDecode {
            message: "test journal-file version must be one of the retained adjacent fixtures"
                .to_owned(),
        });
    }

    let journal_path = single_authority_journal_path(durable_state_path);
    let mut journal = std::fs::read(&journal_path).map_err(|source| {
        ControlPlaneError::io(
            "read single-authority journal for unsupported-file-version test",
            source,
        )
    })?;
    let header_len = SINGLE_AUTHORITY_JOURNAL_FILE_MAGIC.len()
        + std::mem::size_of::<u16>()
        + std::mem::size_of::<u64>()
        + std::mem::size_of::<u64>();
    if journal.len() < header_len {
        return Err(ControlPlaneError::CommandDecode {
            message: "test single-authority journal does not contain a complete file header"
                .to_owned(),
        });
    }
    let version_offset = SINGLE_AUTHORITY_JOURNAL_FILE_MAGIC.len();
    journal[version_offset..version_offset + 2].copy_from_slice(&version.to_be_bytes());
    let checksum_offset = header_len - std::mem::size_of::<u64>();
    let checksum = checksum::crc64::checksum(&journal[..checksum_offset]);
    journal[checksum_offset..header_len].copy_from_slice(&checksum.to_be_bytes());
    journal.extend_from_slice(&[0xa3, 0xc1]);

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&journal_path)
        .map_err(|source| {
            ControlPlaneError::io(
                "open single-authority journal for unsupported-file-version test",
                source,
            )
        })?;
    file.write_all(&journal).map_err(|source| {
        ControlPlaneError::io(
            "write single-authority journal for unsupported-file-version test",
            source,
        )
    })?;
    file.sync_all().map_err(|source| {
        ControlPlaneError::io(
            "sync single-authority journal for unsupported-file-version test",
            source,
        )
    })?;
    Ok(())
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn prepare_unsupported_single_authority_journal_record_restart_for_test(
    durable_state_path: &Path,
    version: u16,
) -> Result<(), ControlPlaneError> {
    if !matches!(version, 1 | 3) {
        return Err(ControlPlaneError::CommandDecode {
            message: "test journal-record version must be one of the retained adjacent fixtures"
                .to_owned(),
        });
    }

    let mut authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
        durable_state_path.to_path_buf(),
    ))?;
    authority.set_node_membership(NodeId::new(0x7f), NodeMembershipState::Active)?;
    drop(authority);

    let journal_path = single_authority_journal_path(durable_state_path);
    let mut journal = std::fs::read(&journal_path).map_err(|source| {
        ControlPlaneError::io(
            "read single-authority journal for unsupported-record-version test",
            source,
        )
    })?;
    let header_len = FileControlPlaneStore::new(durable_state_path.to_path_buf())
        .journal
        .encode_file_header(0)
        .len();
    let mut frame_offset = header_len;
    let mut last_record = None;
    while frame_offset < journal.len() {
        if journal.len() - frame_offset < 2 * std::mem::size_of::<u32>() {
            return Err(ControlPlaneError::CommandDecode {
                message: "test single-authority journal contains a truncated frame prefix"
                    .to_owned(),
            });
        }
        let frame_len = u32::from_be_bytes(
            journal[frame_offset..frame_offset + std::mem::size_of::<u32>()]
                .try_into()
                .expect("test journal frame length has fixed width"),
        );
        let frame_len_check = u32::from_be_bytes(
            journal[frame_offset + std::mem::size_of::<u32>()
                ..frame_offset + 2 * std::mem::size_of::<u32>()]
                .try_into()
                .expect("test journal frame length check has fixed width"),
        );
        if frame_len_check != !frame_len {
            return Err(ControlPlaneError::CommandDecode {
                message: "test single-authority journal frame length check mismatch".to_owned(),
            });
        }
        let record_start = frame_offset + 2 * std::mem::size_of::<u32>();
        let record_end = record_start
            .checked_add(usize::try_from(frame_len).map_err(|_| {
                ControlPlaneError::CommandDecode {
                    message: "test single-authority journal frame length exceeds usize".to_owned(),
                }
            })?)
            .ok_or_else(|| ControlPlaneError::CommandDecode {
                message: "test single-authority journal frame length overflows usize".to_owned(),
            })?;
        if record_end > journal.len() {
            return Err(ControlPlaneError::CommandDecode {
                message: "test single-authority journal contains a truncated frame".to_owned(),
            });
        }
        last_record = Some((record_start, record_end));
        frame_offset = record_end;
    }
    let (record_start, record_end) = last_record.ok_or_else(|| {
        ControlPlaneError::CommandDecode {
            message: "test single-authority journal contains no records".to_owned(),
        }
    })?;
    let kind_offset = record_start
        + SINGLE_AUTHORITY_JOURNAL_RECORD_MAGIC.len()
        + std::mem::size_of::<u16>()
        + CONTROL_PLANE_CLOCK_CHECKPOINT_BINDING_LEN
        + 2 * std::mem::size_of::<u64>();
    if journal.get(kind_offset).copied() != Some(SINGLE_AUTHORITY_JOURNAL_RECORD_COMMAND) {
        return Err(ControlPlaneError::CommandDecode {
            message: "test single-authority journal final record is not a command".to_owned(),
        });
    }
    let command_offset = kind_offset + std::mem::size_of::<u8>() + std::mem::size_of::<u32>();
    if command_offset >= record_end - SINGLE_AUTHORITY_JOURNAL_RECORD_CHECKSUM_LEN {
        return Err(ControlPlaneError::CommandDecode {
            message: "test single-authority journal command payload is empty".to_owned(),
        });
    }
    journal[command_offset] ^= 0xff;
    let previous_chain_digest_offset = record_start
        + SINGLE_AUTHORITY_JOURNAL_RECORD_MAGIC.len()
        + std::mem::size_of::<u16>()
        + CONTROL_PLANE_CLOCK_CHECKPOINT_BINDING_LEN;
    let previous_chain_digest = u64::from_be_bytes(
        journal[previous_chain_digest_offset
            ..previous_chain_digest_offset + std::mem::size_of::<u64>()]
            .try_into()
            .expect("test journal previous chain digest has fixed width"),
    );
    let record_checksum_offset = record_end - SINGLE_AUTHORITY_JOURNAL_RECORD_CHECKSUM_LEN;
    let resulting_chain_digest = single_authority_command_chain_digest(
        previous_chain_digest,
        &journal[command_offset..record_checksum_offset],
    );
    let resulting_chain_digest_offset = previous_chain_digest_offset + std::mem::size_of::<u64>();
    journal[resulting_chain_digest_offset
        ..resulting_chain_digest_offset + std::mem::size_of::<u64>()]
        .copy_from_slice(&resulting_chain_digest.to_be_bytes());
    let record_version_offset = record_start + SINGLE_AUTHORITY_JOURNAL_RECORD_MAGIC.len();
    journal[record_version_offset..record_version_offset + 2]
        .copy_from_slice(&version.to_be_bytes());
    let record_checksum = checksum::crc64::checksum(&journal[record_start..record_checksum_offset]);
    journal[record_checksum_offset..record_end].copy_from_slice(&record_checksum.to_be_bytes());
    journal.extend_from_slice(&[0xa3, 0xc1]);

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&journal_path)
        .map_err(|source| {
            ControlPlaneError::io(
                "open single-authority journal for unsupported-record-version test",
                source,
            )
        })?;
    file.write_all(&journal).map_err(|source| {
        ControlPlaneError::io(
            "write single-authority journal for unsupported-record-version test",
            source,
        )
    })?;
    file.sync_all().map_err(|source| {
        ControlPlaneError::io(
            "sync single-authority journal for unsupported-record-version test",
            source,
        )
    })?;
    Ok(())
}

#[derive(Debug)]
struct SingleAuthorityJournalRecord {
    binding: ControlPlaneAuthorityClockCheckpointBinding,
    previous_chain_digest: u64,
    resulting_chain_digest: u64,
    command: Option<ControlPlaneCommand>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SingleAuthorityJournalRecordFormatError {
    Truncated,
    UnknownMagic,
    UnsupportedVersion(u16),
}

impl std::fmt::Display for SingleAuthorityJournalRecordFormatError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated => formatter
                .write_str("truncated single-authority control-plane journal record"),
            Self::UnknownMagic => formatter
                .write_str("invalid single-authority control-plane journal record magic"),
            Self::UnsupportedVersion(version) => write!(
                formatter,
                "unsupported single-authority control-plane journal record version {version}"
            ),
        }
    }
}

#[derive(Debug)]
enum SingleAuthorityJournalRecordDecodeError {
    Format(SingleAuthorityJournalRecordFormatError),
    Invalid(ControlPlaneError),
}

impl SingleAuthorityJournalRecordDecodeError {
    fn into_control_plane_error(self) -> ControlPlaneError {
        match self {
            Self::Format(error) => ControlPlaneError::CommandDecode {
                message: error.to_string(),
            },
            Self::Invalid(error) => error,
        }
    }
}

impl SingleAuthorityJournalRecord {
    fn encode(&self) -> Result<Vec<u8>, ControlPlaneError> {
        let (kind, command) = match &self.command {
            Some(command) => (
                SINGLE_AUTHORITY_JOURNAL_RECORD_COMMAND,
                encode_control_plane_command(command)?,
            ),
            None => (SINGLE_AUTHORITY_JOURNAL_RECORD_CHECKPOINT, Vec::new()),
        };
        self.encode_parts(kind, &command)
    }

    fn encode_parts(&self, kind: u8, command: &[u8]) -> Result<Vec<u8>, ControlPlaneError> {
        let command_len =
            u32::try_from(command.len()).map_err(|_| ControlPlaneError::CommandDecode {
                message: format!(
                    "single-authority control-plane journal command length {} exceeds u32::MAX",
                    command.len()
                ),
            })?;
        let mut out = Vec::with_capacity(
            SINGLE_AUTHORITY_JOURNAL_RECORD_MAGIC.len()
                + std::mem::size_of::<u16>()
                + CONTROL_PLANE_CLOCK_CHECKPOINT_BINDING_LEN
                + 2 * std::mem::size_of::<u64>()
                + std::mem::size_of::<u8>()
                + std::mem::size_of::<u32>()
                + command.len()
                + SINGLE_AUTHORITY_JOURNAL_RECORD_CHECKSUM_LEN,
        );
        out.extend_from_slice(SINGLE_AUTHORITY_JOURNAL_RECORD_MAGIC);
        out.extend_from_slice(&SINGLE_AUTHORITY_JOURNAL_RECORD_VERSION.to_be_bytes());
        out.extend_from_slice(&self.binding.0);
        out.extend_from_slice(&self.previous_chain_digest.to_be_bytes());
        out.extend_from_slice(&self.resulting_chain_digest.to_be_bytes());
        out.push(kind);
        out.extend_from_slice(&command_len.to_be_bytes());
        out.extend_from_slice(command);
        out.extend_from_slice(&checksum::crc64::checksum(&out).to_be_bytes());
        Ok(out)
    }

    #[cfg(test)]
    fn encode_command_bytes_for_test(
        binding: ControlPlaneAuthorityClockCheckpointBinding,
        previous_chain_digest: u64,
        command: &[u8],
    ) -> Result<Vec<u8>, ControlPlaneError> {
        let record = Self {
            binding,
            previous_chain_digest,
            resulting_chain_digest: single_authority_command_chain_digest(
                previous_chain_digest,
                command,
            ),
            command: None,
        };
        record.encode_parts(SINGLE_AUTHORITY_JOURNAL_RECORD_COMMAND, command)
    }

    fn decode(bytes: &[u8]) -> Result<Self, ControlPlaneError> {
        Self::decode_classified(bytes)
            .map_err(SingleAuthorityJournalRecordDecodeError::into_control_plane_error)
    }

    fn decode_classified(bytes: &[u8]) -> Result<Self, SingleAuthorityJournalRecordDecodeError> {
        let fixed_len = SINGLE_AUTHORITY_JOURNAL_RECORD_MAGIC.len()
            + std::mem::size_of::<u16>()
            + CONTROL_PLANE_CLOCK_CHECKPOINT_BINDING_LEN
            + 2 * std::mem::size_of::<u64>()
            + std::mem::size_of::<u8>()
            + std::mem::size_of::<u32>()
            + SINGLE_AUTHORITY_JOURNAL_RECORD_CHECKSUM_LEN;
        if bytes.len() < fixed_len {
            return Err(SingleAuthorityJournalRecordDecodeError::Format(
                SingleAuthorityJournalRecordFormatError::Truncated,
            ));
        }
        let (body, checksum_bytes) =
            bytes.split_at(bytes.len() - SINGLE_AUTHORITY_JOURNAL_RECORD_CHECKSUM_LEN);
        let expected_checksum = u64::from_be_bytes(
            checksum_bytes
                .try_into()
                .expect("journal record checksum has fixed length"),
        );
        let actual_checksum = checksum::crc64::checksum(body);
        if actual_checksum != expected_checksum {
            return Err(SingleAuthorityJournalRecordDecodeError::Invalid(
                ControlPlaneError::CommandDecode {
                    message: format!(
                        "single-authority control-plane journal record checksum mismatch: expected {expected_checksum:#x}, actual {actual_checksum:#x}"
                    ),
                },
            ));
        }
        let mut offset = 0;
        if &body[..SINGLE_AUTHORITY_JOURNAL_RECORD_MAGIC.len()]
            != SINGLE_AUTHORITY_JOURNAL_RECORD_MAGIC
        {
            return Err(SingleAuthorityJournalRecordDecodeError::Format(
                SingleAuthorityJournalRecordFormatError::UnknownMagic,
            ));
        }
        offset += SINGLE_AUTHORITY_JOURNAL_RECORD_MAGIC.len();
        let version = u16::from_be_bytes(
            body[offset..offset + std::mem::size_of::<u16>()]
                .try_into()
                .expect("journal record version has fixed length"),
        );
        offset += std::mem::size_of::<u16>();
        if version != SINGLE_AUTHORITY_JOURNAL_RECORD_VERSION {
            return Err(SingleAuthorityJournalRecordDecodeError::Format(
                SingleAuthorityJournalRecordFormatError::UnsupportedVersion(version),
            ));
        }
        let mut binding = [0; CONTROL_PLANE_CLOCK_CHECKPOINT_BINDING_LEN];
        binding.copy_from_slice(&body[offset..offset + CONTROL_PLANE_CLOCK_CHECKPOINT_BINDING_LEN]);
        offset += CONTROL_PLANE_CLOCK_CHECKPOINT_BINDING_LEN;
        let previous_chain_digest = u64::from_be_bytes(
            body[offset..offset + std::mem::size_of::<u64>()]
                .try_into()
                .expect("previous chain digest has fixed length"),
        );
        offset += std::mem::size_of::<u64>();
        let resulting_chain_digest = u64::from_be_bytes(
            body[offset..offset + std::mem::size_of::<u64>()]
                .try_into()
                .expect("resulting chain digest has fixed length"),
        );
        offset += std::mem::size_of::<u64>();
        let kind = body[offset];
        offset += std::mem::size_of::<u8>();
        let command_len = u32::from_be_bytes(
            body[offset..offset + std::mem::size_of::<u32>()]
                .try_into()
                .expect("journal command length has fixed length"),
        );
        offset += std::mem::size_of::<u32>();
        let remaining = body.len() - offset;
        if u64::from(command_len) != u64::try_from(remaining).unwrap_or(u64::MAX) {
            return Err(SingleAuthorityJournalRecordDecodeError::Invalid(
                ControlPlaneError::CommandDecode {
                    message: "single-authority control-plane journal command length mismatch"
                        .to_owned(),
                },
            ));
        }
        let command_len = usize::try_from(command_len)
            .expect("command length equals remaining addressable bytes");
        let command_end = offset + command_len;
        let command = match kind {
            SINGLE_AUTHORITY_JOURNAL_RECORD_CHECKPOINT if command_len == 0 => None,
            SINGLE_AUTHORITY_JOURNAL_RECORD_COMMAND if command_len != 0 => {
                Some(
                    decode_control_plane_command(&body[offset..command_end])
                        .map_err(SingleAuthorityJournalRecordDecodeError::Invalid)?,
                )
            }
            SINGLE_AUTHORITY_JOURNAL_RECORD_CHECKPOINT
            | SINGLE_AUTHORITY_JOURNAL_RECORD_COMMAND => {
                return Err(SingleAuthorityJournalRecordDecodeError::Invalid(
                    ControlPlaneError::CommandDecode {
                        message:
                            "single-authority control-plane journal record kind has invalid command length"
                                .to_owned(),
                    },
                ));
            }
            _ => {
                return Err(SingleAuthorityJournalRecordDecodeError::Invalid(
                    ControlPlaneError::CommandDecode {
                        message: format!(
                            "invalid single-authority control-plane journal record kind {kind}"
                        ),
                    },
                ));
            }
        };
        Ok(Self {
            binding: ControlPlaneAuthorityClockCheckpointBinding(binding),
            previous_chain_digest,
            resulting_chain_digest,
            command,
        })
    }
}

#[cfg(test)]
thread_local! {
    static SINGLE_AUTHORITY_SNAPSHOT_DIGEST_COMPUTATIONS: std::cell::Cell<u64> =
        const { std::cell::Cell::new(0) };
}

fn single_authority_snapshot_digest(snapshot: &ClusterControlSnapshot) -> u64 {
    #[cfg(test)]
    SINGLE_AUTHORITY_SNAPSHOT_DIGEST_COMPUTATIONS
        .with(|count| count.set(count.get().saturating_add(1)));
    checksum::crc64::checksum(format_snapshot(snapshot).as_bytes())
}

#[cfg(test)]
fn single_authority_snapshot_digest_computations() -> u64 {
    SINGLE_AUTHORITY_SNAPSHOT_DIGEST_COMPUTATIONS.with(std::cell::Cell::get)
}

fn single_authority_command_chain_digest(
    previous_chain_digest: u64,
    encoded_command: &[u8],
) -> u64 {
    let mut bytes =
        Vec::with_capacity(std::mem::size_of::<u64>().saturating_add(encoded_command.len()));
    bytes.extend_from_slice(&previous_chain_digest.to_be_bytes());
    bytes.extend_from_slice(encoded_command);
    checksum::crc64::checksum(&bytes)
}

impl ControlPlaneStore for FileControlPlaneStore {
    fn load(&self) -> Result<Option<ClusterControlSnapshot>, ControlPlaneError> {
        let mut durability = self.lock_durability()?;
        self.ensure_healthy_locked(&durability)?;
        let read_snapshot_candidate = |path: &Path| -> Result<Option<String>, ControlPlaneError> {
            match std::fs::read_to_string(path) {
                Ok(contents) => Ok(Some(contents)),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(source) => Err(ControlPlaneError::io("load control-plane state", source)),
            }
        };
        let published_contents = read_snapshot_candidate(&self.path)?;
        let prepared_path = single_authority_snapshot_tmp_path(&self.path);
        let prepared_contents = read_snapshot_candidate(&prepared_path)?;
        if published_contents.is_none() && prepared_contents.is_none() {
            if single_authority_initialized_path(&self.path).exists() {
                return Err(ControlPlaneError::CommandDecode {
                    message:
                        "single-authority durable identity or journal exists without its control-plane checkpoint"
                            .to_owned(),
                });
            }
            if !self.journal.path().exists() {
                durability.initialized = true;
                durability.initial_identity_created =
                    single_authority_identity_path(&self.path).exists();
                durability.journal_clean_offset = 0;
                durability.commands_since_checkpoint = 0;
                durability.bytes_since_checkpoint = 0;
                durability.first_uncheckpointed_at = None;
                durability.published_snapshot_digest = None;
                durability.published_chain_digest = None;
                return Ok(None);
            }
        };
        if let Some(contents) = published_contents.as_ref() {
            parse_snapshot(contents)?;
        }
        let binding =
            load_single_authority_clock_checkpoint_binding(&self.path)?.ok_or_else(|| {
                ControlPlaneError::AuthorityClockCheckpoint {
                    message: "existing single-authority state is missing its durable identity"
                        .to_owned(),
                }
            })?;
        let initialized_binding = load_single_authority_initialized_binding(&self.path)?;
        if initialized_binding.is_some_and(|initialized_binding| initialized_binding != binding) {
            return Err(ControlPlaneError::CommandDecode {
                message:
                    "single-authority initialization marker identity does not match durable state"
                        .to_owned(),
            });
        }
        if published_contents.is_none()
            && !self.journal.path().exists()
            && initialized_binding.is_none()
        {
            durability.initialized = true;
            durability.initial_identity_created = true;
            durability.journal_clean_offset = 0;
            durability.commands_since_checkpoint = 0;
            durability.bytes_since_checkpoint = 0;
            durability.first_uncheckpointed_at = None;
            durability.published_snapshot_digest = None;
            durability.published_chain_digest = None;
            return Ok(None);
        }
        let incomplete_initialization =
            published_contents.is_none() && initialized_binding.is_none();
        let frames = match self
            .journal
            .status_offsets()
            .and_then(|offsets| self.journal.read_frames_from(offsets.base_offset))
        {
            Ok(frames) => frames,
            Err(_) if incomplete_initialization => {
                self.discard_incomplete_initialization_locked(&mut durability)?;
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        let mut records = Vec::with_capacity(frames.frames.len());
        for frame in frames.frames {
            let record = match SingleAuthorityJournalRecord::decode(&frame) {
                Ok(record) => record,
                Err(_) if incomplete_initialization => {
                    self.discard_incomplete_initialization_locked(&mut durability)?;
                    return Ok(None);
                }
                Err(error) => return Err(error),
            };
            if record.binding != binding {
                return Err(ControlPlaneError::CommandDecode {
                    message:
                        "single-authority control-plane journal identity does not match durable state"
                            .to_owned(),
                });
            }
            records.push(record);
        }
        if incomplete_initialization && !records.iter().any(|record| record.command.is_none()) {
            self.discard_incomplete_initialization_locked(&mut durability)?;
            return Ok(None);
        }
        let (checkpoint_contents, checkpoint_digest, replay_start, prepared_checkpoint) = [
            (published_contents.as_ref(), false),
            (prepared_contents.as_ref(), true),
        ]
        .into_iter()
        .filter_map(|(contents, prepared)| {
            let contents = contents?;
            let digest = checksum::crc64::checksum(contents.as_bytes());
            records
                .iter()
                .enumerate()
                .filter_map(|(index, record)| {
                    (record.command.is_none() && record.resulting_chain_digest == digest)
                        .then_some(index + 1)
                })
                .next_back()
                .map(|replay_start| (contents, digest, replay_start, prepared))
        })
        .max_by_key(|(_, _, replay_start, _)| *replay_start)
            .ok_or_else(|| ControlPlaneError::CommandDecode {
                message:
                    "single-authority control-plane journal has no identity-bound checkpoint anchor for the durable snapshot"
                        .to_owned(),
            })?;
        let mut snapshot = parse_snapshot(checkpoint_contents)?;
        let mut chain_digest = checkpoint_digest;
        for record in &records[replay_start..] {
            let Some(command) = &record.command else {
                return Err(ControlPlaneError::CommandDecode {
                    message:
                        "single-authority control-plane journal has an unexpected checkpoint anchor in its replay suffix"
                            .to_owned(),
                });
            };
            if chain_digest != record.previous_chain_digest {
                return Err(ControlPlaneError::CommandDecode {
                    message:
                        "single-authority control-plane journal command chain is discontinuous"
                            .to_owned(),
                });
            }
            let encoded_command = encode_control_plane_command(command)?;
            let expected_resulting_chain_digest =
                single_authority_command_chain_digest(chain_digest, &encoded_command);
            if record.resulting_chain_digest != expected_resulting_chain_digest {
                return Err(ControlPlaneError::CommandDecode {
                    message: "single-authority control-plane journal command chain digest mismatch"
                        .to_owned(),
                });
            }
            let applied = snapshot
                .apply_control_plane_command(command.clone())
                .map_err(|error| ControlPlaneError::CommandDecode {
                    message: format!(
                        "single-authority control-plane journal command was rejected during replay: {error}"
                    ),
                })?;
            if !applied.changed() {
                return Err(ControlPlaneError::CommandDecode {
                    message:
                        "single-authority control-plane journal contains a non-mutating command"
                            .to_owned(),
                });
            }
            snapshot = applied.into_snapshot();
            // Command application records history and validates the resulting publication.
            // Repeating either operation here is both redundant and O(retained history).
            chain_digest = record.resulting_chain_digest;
        }
        if frames.truncated_tail {
            self.journal.truncate_to_clean_len(frames.clean_len)?;
        }
        if prepared_checkpoint {
            self.publish_prepared_snapshot_file(&prepared_path)?;
        }
        durability.initialized = true;
        durability.journal_clean_offset = frames.clean_len;
        durability.commands_since_checkpoint = u64::try_from(
            records[replay_start..]
                .iter()
                .filter(|record| record.command.is_some())
                .count(),
        )
        .unwrap_or(u64::MAX);
        durability.bytes_since_checkpoint = records[replay_start..]
            .iter()
            .filter(|record| record.command.is_some())
            .map(|record| {
                record.encode().map(|frame| {
                    DurableJournalFile::<SingleAuthorityJournalObserver>::framed_len(frame.len())
                })
            })
            .try_fold(0u64, |total, frame_len| {
                frame_len.map(|frame_len| total.saturating_add(frame_len))
            })?;
        durability.first_uncheckpointed_at =
            (durability.commands_since_checkpoint != 0).then(Instant::now);
        durability.published_snapshot_digest = Some(single_authority_snapshot_digest(&snapshot));
        durability.published_chain_digest = Some(chain_digest);
        Ok(Some(snapshot))
    }

    fn checkpoint(
        &self,
        previous_snapshot: Option<&ClusterControlSnapshot>,
        next_snapshot: &ClusterControlSnapshot,
    ) -> Result<(), ControlPlaneError> {
        let _publication =
            self.checkpoint_publication
                .lock()
                .map_err(|_| ControlPlaneError::CommandDecode {
                    message: "single-authority checkpoint publication lock poisoned".to_owned(),
                })?;
        let save_started = Instant::now();
        let result = (|| {
            let mut durability = self.lock_durability()?;
            self.ensure_healthy_locked(&durability)?;
            let expected_digest = previous_snapshot.map(single_authority_snapshot_digest);
            if durability.published_snapshot_digest != expected_digest {
                return Err(ControlPlaneError::CommandDecode {
                    message:
                        "single-authority control-plane checkpoint base does not match the latest durable snapshot"
                            .to_owned(),
                });
            }
            self.checkpoint_snapshot_locked(next_snapshot, &mut durability)
        })();
        observability::record_control_plane_snapshot_save(save_started.elapsed(), result.is_ok());
        result
    }

    fn commit_command(
        &self,
        previous_snapshot: &ClusterControlSnapshot,
        command: &ControlPlaneCommand,
        next_snapshot: &ClusterControlSnapshot,
    ) -> Result<(), ControlPlaneError> {
        #[cfg(test)]
        self.signal_commit_before_durability_lock_if_armed();
        let mut durability = self.lock_durability()?;
        self.ensure_healthy_locked(&durability)?;
        if !durability.initialized {
            return Err(ControlPlaneError::CommandDecode {
                message: "single-authority control-plane store was not initialized before commit"
                    .to_owned(),
            });
        }
        if previous_snapshot == next_snapshot {
            return Err(ControlPlaneError::CommandDecode {
                message: "single-authority control-plane journal refused a non-mutating command"
                    .to_owned(),
            });
        }
        let previous_chain_digest =
            durability
                .published_chain_digest
                .ok_or_else(|| ControlPlaneError::CommandDecode {
                    message:
                        "single-authority control-plane command has no published journal chain"
                            .to_owned(),
                })?;
        let encoded_command = encode_control_plane_command(command)?;
        let resulting_chain_digest =
            single_authority_command_chain_digest(previous_chain_digest, &encoded_command);
        let binding =
            load_single_authority_clock_checkpoint_binding(&self.path)?.ok_or_else(|| {
                ControlPlaneError::AuthorityClockCheckpoint {
                    message: "single-authority durable identity is missing before journal append"
                        .to_owned(),
                }
            })?;
        let record = SingleAuthorityJournalRecord {
            binding,
            previous_chain_digest,
            resulting_chain_digest,
            command: Some(command.clone()),
        };
        let encoded = record.encode()?;
        let encoded_frame_len =
            DurableJournalFile::<SingleAuthorityJournalObserver>::framed_len(encoded.len());
        let next_journal_clean_offset = durability
            .journal_clean_offset
            .checked_add(encoded_frame_len)
            .ok_or_else(|| ControlPlaneError::CommandDecode {
                message: "single-authority control-plane journal offset overflows".to_owned(),
            })?;
        if let Err(error) = self.journal.append_frame(&encoded) {
            return match error {
                DurableJournalAppendError::BeforeReplayableRecord(error) => Err(error),
                DurableJournalAppendError::AmbiguousRecordMayExist(error)
                | DurableJournalAppendError::ReplayableRecordMayExist(error) => {
                    Self::latch_durability_failure(&mut durability, &error);
                    let result = Err(ControlPlaneError::CommandDecode {
                        message: format!(
                            "single-authority control-plane durability poisoned after ambiguous journal append: {error}"
                        ),
                    });
                    drop(durability);
                    Self::log_durability_failure("journal_append", &error);
                    result
                }
            };
        }
        durability.published_snapshot_digest = None;
        durability.published_chain_digest = Some(resulting_chain_digest);
        durability.journal_clean_offset = next_journal_clean_offset;
        durability.commands_since_checkpoint =
            durability.commands_since_checkpoint.saturating_add(1);
        durability.bytes_since_checkpoint = durability
            .bytes_since_checkpoint
            .saturating_add(encoded_frame_len);
        if durability.first_uncheckpointed_at.is_none() {
            durability.first_uncheckpointed_at = Some(Instant::now());
        }
        Ok(())
    }

    fn ensure_healthy(&self) -> Result<(), ControlPlaneError> {
        let durability = self.lock_durability()?;
        self.ensure_healthy_locked(&durability)
    }

    #[cfg(test)]
    fn checkpoint_manually_modified_snapshot_for_test(
        &self,
        _previous_snapshot: &ClusterControlSnapshot,
        next_snapshot: &ClusterControlSnapshot,
    ) -> Result<(), ControlPlaneError> {
        let _publication =
            self.checkpoint_publication
                .lock()
                .map_err(|_| ControlPlaneError::CommandDecode {
                    message: "single-authority checkpoint publication lock poisoned".to_owned(),
                })?;
        let mut durability = self.lock_durability()?;
        self.ensure_healthy_locked(&durability)?;
        self.checkpoint_snapshot_locked(next_snapshot, &mut durability)
    }
}

impl FileControlPlaneStore {
    fn durability_failure_log_message(stage: &'static str, error: &ControlPlaneError) -> String {
        format!(
            "single-authority control-plane durability failure stage={stage}: {}",
            error.retained_diagnostic_message()
        )
    }

    fn latch_durability_failure(
        durability: &mut FileControlPlaneStoreDurability,
        error: &ControlPlaneError,
    ) {
        durability.poisoned.get_or_insert_with(|| error.to_string());
    }

    fn write_durability_failure(
        stage: &'static str,
        error: &ControlPlaneError,
        output: &mut dyn std::io::Write,
    ) -> std::io::Result<()> {
        writeln!(
            output,
            "{}",
            Self::durability_failure_log_message(stage, error)
        )
    }

    fn log_durability_failure(stage: &'static str, error: &ControlPlaneError) {
        let stderr = std::io::stderr();
        let mut stderr = stderr.lock();
        let _ = Self::write_durability_failure(stage, error, &mut stderr);
    }

    fn lock_durability(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, FileControlPlaneStoreDurability>, ControlPlaneError> {
        self.durability
            .lock()
            .map_err(|_| ControlPlaneError::CommandDecode {
                message: "single-authority control-plane durability lock poisoned".to_owned(),
            })
    }

    fn ensure_healthy_locked(
        &self,
        durability: &FileControlPlaneStoreDurability,
    ) -> Result<(), ControlPlaneError> {
        if let Some(reason) = &durability.poisoned {
            return Err(ControlPlaneError::CommandDecode {
                message: format!("single-authority control-plane durability is poisoned: {reason}"),
            });
        }
        Ok(())
    }

    fn capture_checkpoint_if_due(
        &self,
        now: Instant,
    ) -> Result<Option<FileControlPlaneCheckpointCapture>, ControlPlaneError> {
        let durability = self.lock_durability()?;
        self.ensure_healthy_locked(&durability)?;
        let Some(first_uncheckpointed_at) = durability.first_uncheckpointed_at else {
            return Ok(None);
        };
        if durability.commands_since_checkpoint < self.checkpoint_command_limit
            && durability.bytes_since_checkpoint < self.checkpoint_byte_limit
            && now.saturating_duration_since(first_uncheckpointed_at) < self.checkpoint_interval
        {
            return Ok(None);
        }
        let chain_digest =
            durability
                .published_chain_digest
                .ok_or_else(|| ControlPlaneError::CommandDecode {
                    message: "single-authority checkpoint capture has no published journal chain"
                        .to_owned(),
                })?;
        Ok(Some(FileControlPlaneCheckpointCapture {
            store_instance: Arc::clone(&self.durability),
            checkpoint_generation: durability.checkpoint_generation,
            journal_offset: durability.journal_clean_offset,
            chain_digest,
            captured_at: now,
        }))
    }

    fn persist_captured_checkpoint(
        &self,
        capture: FileControlPlaneCheckpointCapture,
        snapshot: &ClusterControlSnapshot,
    ) -> Result<(), ControlPlaneError> {
        let _publication =
            self.checkpoint_publication
                .lock()
                .map_err(|_| ControlPlaneError::CommandDecode {
                    message: "single-authority checkpoint publication lock poisoned".to_owned(),
                })?;
        self.validate_checkpoint_capture(&capture)?;
        let save_started = Instant::now();
        let result = self.persist_captured_checkpoint_inner(capture, snapshot);
        observability::record_control_plane_snapshot_save(save_started.elapsed(), result.is_ok());
        if let Err(error) = &result {
            if let Ok(mut durability) = self.lock_durability() {
                Self::latch_durability_failure(&mut durability, error);
            }
        }
        drop(_publication);
        if let Err(error) = &result {
            Self::log_durability_failure("checkpoint_persistence", error);
        }
        result
    }

    fn validate_checkpoint_capture(
        &self,
        capture: &FileControlPlaneCheckpointCapture,
    ) -> Result<(), ControlPlaneError> {
        let durability = self.lock_durability()?;
        self.ensure_healthy_locked(&durability)?;
        if !Arc::ptr_eq(&capture.store_instance, &self.durability) {
            return Err(ControlPlaneError::CommandDecode {
                message: "single-authority checkpoint capture belongs to another store instance"
                    .to_owned(),
            });
        }
        if capture.checkpoint_generation != durability.checkpoint_generation {
            return Err(ControlPlaneError::CommandDecode {
                message: "single-authority checkpoint capture is stale".to_owned(),
            });
        }
        if capture.journal_offset > durability.journal_clean_offset {
            return Err(ControlPlaneError::CommandDecode {
                message: "single-authority checkpoint capture is beyond the published journal"
                    .to_owned(),
            });
        }
        let offsets = self.journal.status_offsets()?;
        if capture.journal_offset < offsets.base_offset
            || capture.journal_offset > offsets.clean_len
        {
            return Err(ControlPlaneError::CommandDecode {
                message: "single-authority checkpoint capture is outside the retained journal"
                    .to_owned(),
            });
        }
        Ok(())
    }

    fn persist_captured_checkpoint_inner(
        &self,
        capture: FileControlPlaneCheckpointCapture,
        snapshot: &ClusterControlSnapshot,
    ) -> Result<(), ControlPlaneError> {
        let (prepared_path, snapshot_digest) = self.write_prepared_snapshot_file(snapshot)?;

        let mut durability = self.lock_durability()?;
        self.ensure_healthy_locked(&durability)?;
        let publication_result = (|| {
            debug_assert_eq!(
                capture.checkpoint_generation, durability.checkpoint_generation,
                "checkpoint publication lock prevents another checkpoint"
            );
            let suffix = self.journal.read_frames_from(capture.journal_offset)?;
            if suffix.truncated_tail {
                return Err(ControlPlaneError::CommandDecode {
                    message: "single-authority checkpoint found a torn live journal suffix"
                        .to_owned(),
                });
            }

            let binding =
                load_single_authority_clock_checkpoint_binding(&self.path)?.ok_or_else(|| {
                    ControlPlaneError::AuthorityClockCheckpoint {
                    message:
                        "single-authority durable identity is missing before checkpoint anchoring"
                            .to_owned(),
                }
                })?;
            let anchor = SingleAuthorityJournalRecord {
                binding,
                previous_chain_digest: capture.chain_digest,
                resulting_chain_digest: snapshot_digest,
                command: None,
            }
            .encode()?;
            let mut replacement = vec![anchor];
            let mut original_chain_digest = capture.chain_digest;
            let mut resulting_chain_digest = snapshot_digest;
            let mut suffix_bytes = 0u64;
            for frame in suffix.frames {
                let record = SingleAuthorityJournalRecord::decode(&frame)?;
                if record.binding != binding {
                    return Err(ControlPlaneError::CommandDecode {
                        message:
                            "single-authority checkpoint suffix belongs to another durable identity"
                                .to_owned(),
                    });
                }
                let command =
                    record.command.ok_or_else(|| {
                        ControlPlaneError::CommandDecode {
                    message:
                        "single-authority checkpoint suffix contains an unexpected checkpoint anchor"
                            .to_owned(),
                }
                    })?;
                if record.previous_chain_digest != original_chain_digest {
                    return Err(ControlPlaneError::CommandDecode {
                        message:
                            "single-authority checkpoint suffix command chain is discontinuous"
                                .to_owned(),
                    });
                }
                let encoded_command = encode_control_plane_command(&command)?;
                let expected_original_digest =
                    single_authority_command_chain_digest(original_chain_digest, &encoded_command);
                if record.resulting_chain_digest != expected_original_digest {
                    return Err(ControlPlaneError::CommandDecode {
                        message: "single-authority checkpoint suffix command chain digest mismatch"
                            .to_owned(),
                    });
                }
                original_chain_digest = record.resulting_chain_digest;
                let rebased_digest =
                    single_authority_command_chain_digest(resulting_chain_digest, &encoded_command);
                let rebased = SingleAuthorityJournalRecord {
                    binding,
                    previous_chain_digest: resulting_chain_digest,
                    resulting_chain_digest: rebased_digest,
                    command: Some(command),
                }
                .encode()?;
                suffix_bytes = suffix_bytes.saturating_add(DurableJournalFile::<
                    SingleAuthorityJournalObserver,
                >::framed_len(
                    rebased.len()
                ));
                replacement.push(rebased);
                resulting_chain_digest = rebased_digest;
            }
            if durability.published_chain_digest != Some(original_chain_digest) {
                return Err(ControlPlaneError::CommandDecode {
                    message:
                        "single-authority checkpoint suffix does not reach the published journal chain"
                            .to_owned(),
                });
            }
            let replacement_bytes = replacement.iter().try_fold(0u64, |total, frame| {
                total
                    .checked_add(
                        DurableJournalFile::<SingleAuthorityJournalObserver>::framed_len(
                            frame.len(),
                        ),
                    )
                    .ok_or_else(|| ControlPlaneError::CommandDecode {
                        message: "single-authority checkpoint replacement length overflows"
                            .to_owned(),
                    })
            })?;
            let replaced_clean_offset = capture
                .journal_offset
                .checked_add(replacement_bytes)
                .ok_or_else(|| ControlPlaneError::CommandDecode {
                    message: "single-authority checkpoint journal offset overflows".to_owned(),
                })?;

            self.journal
                .replace_from(capture.journal_offset, suffix.clean_len, &replacement)?;
            #[cfg(test)]
            self.wait_after_checkpoint_journal_replacement_if_requested();
            #[cfg(test)]
            if self
                .fail_checkpoint_after_anchor
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(ControlPlaneError::io(
                    "publish prepared single-authority control-plane checkpoint",
                    std::io::Error::other(
                        "injected failure after single-authority checkpoint anchor",
                    ),
                ));
            }
            self.publish_prepared_snapshot_file(&prepared_path)?;
            if load_single_authority_initialized_binding(&self.path)?.is_none() {
                store_single_authority_initialized_binding(&self.path, binding)?;
            }

            let suffix_commands =
                u64::try_from(replacement.len().saturating_sub(1)).unwrap_or(u64::MAX);
            durability.initialized = true;
            durability.initial_identity_created = false;
            durability.journal_clean_offset = replaced_clean_offset;
            durability.commands_since_checkpoint = suffix_commands;
            durability.bytes_since_checkpoint = suffix_bytes;
            durability.first_uncheckpointed_at =
                (suffix_commands != 0).then_some(capture.captured_at);
            durability.checkpoint_generation = durability.checkpoint_generation.saturating_add(1);
            durability.published_snapshot_digest =
                (suffix_commands == 0).then_some(snapshot_digest);
            durability.published_chain_digest = Some(resulting_chain_digest);
            Ok(())
        })();
        if let Err(error) = &publication_result {
            durability.poisoned = Some(error.to_string());
        }
        publication_result
    }

    fn discard_incomplete_initialization_locked(
        &self,
        durability: &mut FileControlPlaneStoreDurability,
    ) -> Result<(), ControlPlaneError> {
        let mut removed = false;
        let prepared_path = single_authority_snapshot_tmp_path(&self.path);
        for path in [self.journal.path(), prepared_path.as_path()] {
            match std::fs::remove_file(path) {
                Ok(()) => removed = true,
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(source) => {
                    return Err(ControlPlaneError::io(
                        "discard incomplete single-authority initialization",
                        source,
                    ));
                }
            }
        }
        if removed {
            std::fs::File::open(state_parent(&self.path))
                .and_then(|directory| directory.sync_all())
                .map_err(|source| {
                    ControlPlaneError::io(
                        "sync discarded single-authority initialization directory",
                        source,
                    )
                })?;
        }
        durability.initialized = true;
        durability.initial_identity_created = true;
        durability.journal_clean_offset = 0;
        durability.commands_since_checkpoint = 0;
        durability.bytes_since_checkpoint = 0;
        durability.first_uncheckpointed_at = None;
        durability.published_snapshot_digest = None;
        durability.published_chain_digest = None;
        Ok(())
    }

    fn checkpoint_snapshot_locked(
        &self,
        snapshot: &ClusterControlSnapshot,
        durability: &mut FileControlPlaneStoreDurability,
    ) -> Result<(), ControlPlaneError> {
        let compact_through = durability.journal_clean_offset;
        let (prepared_path, snapshot_digest) = self.prepare_snapshot_file(snapshot, durability)?;
        let binding =
            load_single_authority_clock_checkpoint_binding(&self.path)?.ok_or_else(|| {
                ControlPlaneError::AuthorityClockCheckpoint {
                    message:
                        "single-authority durable identity is missing before checkpoint anchoring"
                            .to_owned(),
                }
            })?;
        let anchor = SingleAuthorityJournalRecord {
            binding,
            previous_chain_digest: durability.published_chain_digest.unwrap_or(snapshot_digest),
            resulting_chain_digest: snapshot_digest,
            command: None,
        }
        .encode()?;
        let checkpoint_clean_offset = compact_through
            .checked_add(
                DurableJournalFile::<SingleAuthorityJournalObserver>::framed_len(anchor.len()),
            )
            .ok_or_else(|| ControlPlaneError::CommandDecode {
                message: "single-authority checkpoint journal offset overflows".to_owned(),
            })?;
        self.journal
            .append_frame(&anchor)
            .map_err(DurableJournalAppendError::into_control_plane_error)?;
        #[cfg(test)]
        if self
            .fail_checkpoint_after_anchor
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(ControlPlaneError::io(
                "publish prepared single-authority control-plane checkpoint",
                std::io::Error::other("injected failure after single-authority checkpoint anchor"),
            ));
        }
        self.publish_prepared_snapshot_file(&prepared_path)?;
        self.journal.compact_through(compact_through)?;
        if load_single_authority_initialized_binding(&self.path)?.is_none() {
            store_single_authority_initialized_binding(&self.path, binding)?;
        }
        durability.initialized = true;
        durability.initial_identity_created = false;
        durability.journal_clean_offset = checkpoint_clean_offset;
        durability.commands_since_checkpoint = 0;
        durability.bytes_since_checkpoint = 0;
        durability.first_uncheckpointed_at = None;
        durability.checkpoint_generation = durability.checkpoint_generation.saturating_add(1);
        durability.published_snapshot_digest = Some(snapshot_digest);
        durability.published_chain_digest = Some(snapshot_digest);
        Ok(())
    }

    fn prepare_snapshot_file(
        &self,
        snapshot: &ClusterControlSnapshot,
        durability: &mut FileControlPlaneStoreDurability,
    ) -> Result<(PathBuf, u64), ControlPlaneError> {
        let state_existed = self.path.exists();
        ensure_control_plane_state_parent_directory(&self.path)?;
        if !state_existed {
            let (binding, identity_created) =
                self.load_or_create_authority_clock_checkpoint_binding_untracked()?;
            durability.initial_identity_created |= identity_created;
            #[cfg(test)]
            if identity_created
                && self
                    .fail_initial_checkpoint_after_identity
                    .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(ControlPlaneError::io(
                    "initialize single-authority control-plane checkpoint",
                    std::io::Error::other(
                        "injected failure after single-authority durable identity creation",
                    ),
                ));
            }
            if self
                .load_authority_clock_restart_checkpoint(binding)?
                .is_none()
            {
                store_authority_clock_restart_checkpoint(
                    &self.path,
                    binding,
                    1,
                    snapshot.max_committed_timestamp_ms(),
                )?;
            }
        }
        self.write_prepared_snapshot_file(snapshot)
    }

    fn write_prepared_snapshot_file(
        &self,
        snapshot: &ClusterControlSnapshot,
    ) -> Result<(PathBuf, u64), ControlPlaneError> {
        ensure_control_plane_state_parent_directory(&self.path)?;
        let serialize_started = Instant::now();
        let formatted_snapshot = format_snapshot(snapshot);
        let snapshot_digest = checksum::crc64::checksum(formatted_snapshot.as_bytes());
        observability::record_control_plane_snapshot_serialization(
            serialize_started.elapsed(),
            formatted_snapshot.len(),
        );
        let tmp_path = single_authority_snapshot_tmp_path(&self.path);
        {
            let mut tmp_file = std::fs::File::create(&tmp_path)
                .map_err(|source| ControlPlaneError::io("create control-plane state", source))?;
            tmp_file
                .write_all(formatted_snapshot.as_bytes())
                .map_err(|source| ControlPlaneError::io("write control-plane state", source))?;
            let sync_started = Instant::now();
            let sync_result = tmp_file.sync_all();
            observability::record_control_plane_snapshot_sync(sync_started.elapsed());
            sync_result
                .map_err(|source| ControlPlaneError::io("sync control-plane state", source))?;
        }
        let sync_started = Instant::now();
        let sync_result =
            std::fs::File::open(state_parent(&tmp_path)).and_then(|directory| directory.sync_all());
        observability::record_control_plane_snapshot_sync(sync_started.elapsed());
        sync_result.map_err(|source| {
            ControlPlaneError::io("sync prepared control-plane state directory", source)
        })?;
        #[cfg(test)]
        if self
            .fail_checkpoint_after_prepared_snapshot_sync
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(ControlPlaneError::io(
                "anchor prepared single-authority control-plane checkpoint",
                std::io::Error::other(
                    "injected failure after prepared single-authority checkpoint sync",
                ),
            ));
        }
        Ok((tmp_path, snapshot_digest))
    }

    fn publish_prepared_snapshot_file(
        &self,
        prepared_path: &Path,
    ) -> Result<(), ControlPlaneError> {
        std::fs::rename(prepared_path, &self.path)
            .map_err(|source| ControlPlaneError::io("commit control-plane state", source))?;
        let sync_started = Instant::now();
        let sync_result = std::fs::File::open(state_parent(&self.path))
            .and_then(|directory| directory.sync_all());
        observability::record_control_plane_snapshot_sync(sync_started.elapsed());
        sync_result.map_err(|source| {
            ControlPlaneError::io("sync control-plane state directory", source)
        })?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct SingleAuthorityControlPlane<S> {
    store: S,
    durable_snapshot: ClusterControlSnapshot,
    snapshot: ClusterControlSnapshot,
    runtime_map_content_certificate: Mutex<Option<RuntimeMapContentCertificate>>,
}

impl SingleAuthorityControlPlane<FileControlPlaneStore> {
    pub fn capture_durable_checkpoint_if_due(
        &self,
        now: Instant,
    ) -> Result<Option<SingleAuthorityDurableCheckpoint>, ControlPlaneError> {
        let Some(capture) = self.store.capture_checkpoint_if_due(now)? else {
            return Ok(None);
        };
        Ok(Some(SingleAuthorityDurableCheckpoint {
            store: self.store.clone(),
            capture,
            snapshot: self.durable_snapshot.clone(),
        }))
    }
}

impl<S: ControlPlaneStore> SingleAuthorityControlPlane<S> {
    pub fn open(store: S) -> Result<Self, ControlPlaneError> {
        let loaded = store.load()?;
        let (previous_snapshot, snapshot) = match loaded {
            Some(mut snapshot) => {
                let previous_snapshot = snapshot.clone();
                snapshot.bump_authority_after_restart()?;
                snapshot.record_history_from(&previous_snapshot);
                (Some(previous_snapshot), snapshot)
            }
            None => (None, ClusterControlSnapshot::empty()),
        };
        if let Some(previous_snapshot) = previous_snapshot.as_ref() {
            debug_assert_ne!(
                single_authority_snapshot_digest(previous_snapshot),
                single_authority_snapshot_digest(&snapshot)
            );
        }
        validate_control_plane_snapshot(
            "attempted to open invalid control-plane snapshot",
            &snapshot,
        )?;
        store.checkpoint(previous_snapshot.as_ref(), &snapshot)?;
        Ok(Self {
            store,
            durable_snapshot: snapshot.clone(),
            snapshot,
            runtime_map_content_certificate: Mutex::new(None),
        })
    }

    #[must_use]
    pub fn snapshot(&self) -> &ClusterControlSnapshot {
        &self.snapshot
    }

    fn apply_and_commit_command(
        &mut self,
        mut command: ControlPlaneCommand,
    ) -> Result<AppliedControlPlaneCommand, ControlPlaneError> {
        self.store.ensure_healthy()?;
        self.promote_volatile_heartbeat_leases_if_needed()?;
        command = self
            .snapshot
            .bind_metadata_transfer_fence_command(&self.durable_snapshot, command)?;
        let live_applied = self.snapshot.apply_control_plane_command(command.clone())?;
        let durable_applied = self
            .durable_snapshot
            .apply_control_plane_command(command.clone())?;
        if live_applied.changed() != durable_applied.changed() {
            return Err(ControlPlaneError::SnapshotInvariantViolation {
                context: "single-authority volatile heartbeat command rebase",
                message: "command mutation outcome differs between live and durable state"
                    .to_owned(),
            });
        }
        self.commit_rebased_command(command, live_applied, durable_applied)
    }

    fn apply_heartbeat_command(
        &mut self,
        command: ControlPlaneCommand,
    ) -> Result<(), ControlPlaneError> {
        self.store.ensure_healthy()?;
        if let Some(next_snapshot) = self
            .snapshot
            .apply_covered_volatile_heartbeat(command.clone())?
        {
            self.snapshot = next_snapshot;
            return Ok(());
        }
        self.promote_volatile_heartbeat_leases_if_needed()?;
        let applied = self.snapshot.apply_control_plane_command(command.clone())?;
        if applied.changed() {
            let durable_applied = self
                .durable_snapshot
                .apply_control_plane_command(command.clone())?;
            self.commit_rebased_command(command, applied, durable_applied)?;
        }
        Ok(())
    }

    pub fn set_node_membership(
        &mut self,
        node_id: NodeId,
        membership: NodeMembershipState,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        self.apply_and_commit_command(ControlPlaneCommand::SetNodeMembership {
            node_id,
            membership,
        })?;
        Ok(self.snapshot.clone())
    }

    pub fn mark_node_availability(
        &mut self,
        node_id: NodeId,
        availability: NodeAvailabilityState,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        self.apply_and_commit_command(ControlPlaneCommand::MarkNodeAvailability {
            node_id,
            availability,
        })?;
        Ok(self.snapshot.clone())
    }

    pub fn bootstrap_initial_cluster_map(
        &mut self,
        nodes: Vec<(NodeId, String)>,
        pg_ids: Vec<PgId>,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        self.apply_and_commit_command(ControlPlaneCommand::BootstrapInitialClusterMap {
            nodes,
            pg_ids,
        })?;
        Ok(self.snapshot.clone())
    }

    /// Establish the environment-configured uncertified initial topology if
    /// this authority has no control-plane state yet.
    ///
    /// `Ok(None)` means either no storage nodes were configured or an initial
    /// topology was already present. `Ok(Some(epoch))` means this call
    /// durably established the supplied topology at the returned logical
    /// cluster epoch.
    pub fn establish_uncertified_initial_control_plane_topology(
        &mut self,
        topology: &UncertifiedInitialControlPlaneTopology,
    ) -> Result<Option<u64>, ControlPlaneError> {
        if topology.initialized_epoch(&self.snapshot).is_some() {
            return Ok(None);
        }
        let Some(command) = topology.bootstrap_command() else {
            return Ok(None);
        };
        self.apply_and_commit_command(command)?;
        Ok(Some(self.snapshot.cluster_epoch().get()))
    }

    pub fn set_pg_acting_set(
        &mut self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        self.apply_and_commit_command(ControlPlaneCommand::SetPgActingSet { pg_id, acting_set })?;
        Ok(self.snapshot.clone())
    }

    pub fn begin_unavailable_pg_placement_transition(
        &mut self,
        pg_id: PgId,
        unavailable_node_id: NodeId,
        begin_at_ms: u64,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        self.begin_unavailable_pg_placement_transition_batch(
            &[(pg_id, unavailable_node_id)],
            begin_at_ms,
        )
    }

    pub(crate) fn begin_unavailable_pg_placement_transition_batch(
        &mut self,
        candidates: &[(PgId, NodeId)],
        begin_at_ms: u64,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        let command = self
            .snapshot
            .begin_unavailable_pg_placement_transition_batch_command(candidates, begin_at_ms)?;
        self.apply_and_commit_command(command)?;
        Ok(self.snapshot.clone())
    }

    pub fn authorize_unavailable_pg_staging_intents_batch(
        &mut self,
        authorizations: &[UnavailablePgStagingIntentAuthorizationRequest],
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        let command = self
            .snapshot
            .authorize_unavailable_pg_staging_intents_batch_command(authorizations)?;
        self.apply_and_commit_command(command)?;
        Ok(self.snapshot.clone())
    }

    pub fn install_unavailable_pg_placement_transitions_batch(
        &mut self,
        transitions: &[UnavailablePgTransitionInstallRequest],
        expected_destination_epoch: ClusterEpoch,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        let command = self
            .snapshot
            .install_unavailable_pg_placement_transitions_batch_command(
                transitions,
                expected_destination_epoch,
            )?;
        self.apply_and_commit_command(command)?;
        Ok(self.snapshot.clone())
    }

    pub fn checkpoint_metadata_transfer_staging_evidence_pages(
        &mut self,
        actor_node_id: NodeId,
        actor_node_incarnation: u64,
        first_generation: u64,
        last_generation: u64,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        let command = self
            .snapshot
            .checkpoint_metadata_transfer_staging_evidence_pages_command(
                actor_node_id,
                actor_node_incarnation,
                first_generation,
                last_generation,
            )?;
        self.apply_and_commit_command(command)?;
        Ok(self.snapshot.clone())
    }

    pub fn finalize_metadata_transfer_staging_generation(
        &mut self,
        cleanup: FinalizeMetadataTransferStagingGenerationRequest,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        let command = self
            .snapshot
            .finalize_metadata_transfer_staging_generation_command(cleanup)?;
        self.apply_and_commit_command(command)?;
        Ok(self.snapshot.clone())
    }

    pub fn complete_unavailable_pg_placement_transition(
        &mut self,
        work: &UnavailablePgReconciliationWork,
        ready_at_ms: u64,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        self.complete_unavailable_pg_placement_transition_batch(
            std::slice::from_ref(work),
            ready_at_ms,
        )
    }

    pub(crate) fn complete_unavailable_pg_placement_transition_batch(
        &mut self,
        work: &[UnavailablePgReconciliationWork],
        ready_at_ms: u64,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        let command = self
            .snapshot
            .complete_unavailable_pg_placement_transition_batch_command(work, ready_at_ms)?;
        self.apply_and_commit_command(command)?;
        Ok(self.snapshot.clone())
    }

    pub fn poll_unavailable_pg_reconciliation(
        &mut self,
        cursor: &mut UnavailablePgReconciliationCursor,
        now_ms: u64,
    ) -> Result<Option<UnavailablePgReconciliationWork>, ControlPlaneError> {
        let scan = self
            .snapshot
            .scan_unavailable_pg_reconciliation(*cursor, now_ms);
        *cursor = scan.next_cursor;
        match scan.candidate {
            None => Ok(None),
            Some(UnavailablePgReconciliationCandidate::Resume(work)) => Ok(Some(work)),
            Some(UnavailablePgReconciliationCandidate::Begin {
                pg_id,
                unavailable_node_id,
            }) => {
                let command = self
                    .snapshot
                    .begin_unavailable_pg_placement_transition_command(
                        pg_id,
                        unavailable_node_id,
                        now_ms,
                    )?;
                self.apply_and_commit_command(command)?;
                let transition = self
                    .snapshot
                    .unavailable_pg_placement_transition(pg_id)
                    .ok_or_else(|| {
                        ControlPlaneError::invariant_failure(
                            "committed unavailable PG transition is absent from current state",
                        )
                })?;
                Ok(Some(UnavailablePgReconciliationWork::from_transition(
                    transition,
                    UnavailablePgReconciliationStage::MetadataTransfer,
                )))
            }
        }
    }

    pub(crate) fn poll_unavailable_pg_reconciliation_batch(
        &mut self,
        cursor: &mut UnavailablePgReconciliationCursor,
        now_ms: u64,
    ) -> Result<UnavailablePgReconciliationPollBatch, ControlPlaneError> {
        let scan = self
            .snapshot
            .scan_unavailable_pg_reconciliation_batch(*cursor, now_ms);
        *cursor = scan.next_cursor;
        let begin_candidates = scan
            .candidates
            .iter()
            .filter_map(|candidate| match candidate {
                UnavailablePgReconciliationCandidate::Begin {
                    pg_id,
                    unavailable_node_id,
                } => Some((*pg_id, *unavailable_node_id)),
                UnavailablePgReconciliationCandidate::Resume(_) => None,
            })
            .collect::<Vec<_>>();
        let mut begun = Vec::new();
        let mut rejected = Vec::new();
        if !begin_candidates.is_empty() {
            let prepared = self
                .snapshot
                .prepare_unavailable_pg_placement_transition_batch(
                    &begin_candidates,
                    now_ms,
                )?;
            let included = prepared.included;
            rejected.extend(prepared.rejected);
            if let Some(command) = prepared.command {
                self.apply_and_commit_command(command)?;
                begun.extend(included.into_iter().map(|(pg_id, _)| pg_id));
            }
        }
        let mut work = scan
            .candidates
            .into_iter()
            .filter_map(|candidate| match candidate {
                UnavailablePgReconciliationCandidate::Resume(work) => Some(work),
                UnavailablePgReconciliationCandidate::Begin { .. } => None,
            })
            .collect::<Vec<_>>();
        for pg_id in begun {
            let transition = self
                .snapshot
                .unavailable_pg_placement_transition(pg_id)
                .ok_or_else(|| {
                    ControlPlaneError::invariant_failure(
                        "committed unavailable PG transition is absent from current state",
                    )
                })?;
            work.push(UnavailablePgReconciliationWork::from_transition(
                transition,
                UnavailablePgReconciliationStage::MetadataTransfer,
            ));
        }
        work.sort_by_key(UnavailablePgReconciliationWork::pg_id);
        Ok(UnavailablePgReconciliationPollBatch { work, rejected })
    }

    pub fn complete_unavailable_pg_reconciliation(
        &mut self,
        work: &UnavailablePgReconciliationWork,
        now_ms: u64,
    ) -> Result<bool, ControlPlaneError> {
        let Some(transition) = self
            .snapshot
            .unavailable_pg_placement_transition(work.pg_id())
        else {
            return Ok(false);
        };
        if !work.mutation_binding().matches_transition(transition) {
            return Ok(false);
        }
        let command = self
            .snapshot
            .complete_unavailable_pg_placement_transition_command(work, now_ms)?;
        self.apply_and_commit_command(command)?;
        Ok(true)
    }

    pub(crate) fn complete_unavailable_pg_reconciliation_batch(
        &mut self,
        work: &[UnavailablePgReconciliationWork],
        now_ms: u64,
    ) -> Result<UnavailablePgReconciliationCompletionBatch, ControlPlaneError> {
        let prepared = self
            .snapshot
            .prepare_unavailable_pg_placement_completion_batch(work, now_ms)?;
        if let Some(command) = prepared.command {
            self.apply_and_commit_command(command)?;
        }
        let included_pg_ids = prepared
            .included
            .iter()
            .map(UnavailablePgReconciliationWork::pg_id)
            .collect::<BTreeSet<_>>();
        let rejected_pg_ids = prepared
            .rejected
            .iter()
            .map(|(work, _)| work.pg_id())
            .collect::<BTreeSet<_>>();
        let rederive = work
            .iter()
            .filter(|work| {
                !included_pg_ids.contains(&work.pg_id())
                    && !rejected_pg_ids.contains(&work.pg_id())
            })
            .cloned()
            .collect();
        Ok(UnavailablePgReconciliationCompletionBatch {
            completed: prepared.included,
            rejected: prepared.rejected,
            rederive,
        })
    }

    pub fn set_pg_acting_set_with_metadata_transfer(
        &mut self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
        transfer: PgMetadataTransferProof,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        let expected_destination_epoch = next_epoch(self.snapshot.cluster_epoch())?;
        self.set_pg_acting_set_with_metadata_transfer_at_epoch(
            pg_id,
            acting_set,
            transfer,
            expected_destination_epoch,
        )
    }

    pub fn set_pg_acting_set_with_metadata_transfer_at_epoch(
        &mut self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
        transfer: PgMetadataTransferProof,
        expected_destination_epoch: ClusterEpoch,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        self.apply_and_commit_command(ControlPlaneCommand::SetPgActingSetWithMetadataTransfer {
            pg_id,
            acting_set,
            transfer,
            expected_destination_epoch,
            unavailable_transition: None,
        })?;
        Ok(self.snapshot.clone())
    }

    pub fn fence_pg_for_metadata_transfer(
        &mut self,
        pg_id: PgId,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        Ok(self
            .fence_pg_for_metadata_transfer_with_source_lease(pg_id)?
            .into_parts()
            .0)
    }

    pub fn fence_pg_for_metadata_transfer_with_source_lease(
        &mut self,
        pg_id: PgId,
    ) -> Result<FencedPgMetadataTransferSnapshot, ControlPlaneError> {
        let applied =
            self.apply_and_commit_command(ControlPlaneCommand::FencePgForMetadataTransfer {
                pg_id,
                source_primary_lease_deadline_ms: None,
                lease_horizon_authority: None,
                unavailable_transition: None,
            })?;
        let source_primary_lease_deadline_ms = match applied.response() {
            ControlPlaneCommandResponse::FencePgForMetadataTransfer {
                source_primary_lease_deadline_ms,
            } => *source_primary_lease_deadline_ms,
            _ => unreachable!("metadata transfer fence command returned the wrong response"),
        };
        Ok(FencedPgMetadataTransferSnapshot::new(
            self.snapshot.clone(),
            source_primary_lease_deadline_ms,
        ))
    }

    pub fn fence_unavailable_pg_transition_with_source_lease(
        &mut self,
        binding: UnavailablePgTransitionMutationBinding,
    ) -> Result<FencedPgMetadataTransferSnapshot, ControlPlaneError> {
        let pg_id = binding.pg_id();
        let applied =
            self.apply_and_commit_command(ControlPlaneCommand::FencePgForMetadataTransfer {
                pg_id,
                source_primary_lease_deadline_ms: None,
                lease_horizon_authority: None,
                unavailable_transition: Some(binding),
            })?;
        let source_primary_lease_deadline_ms = match applied.response() {
            ControlPlaneCommandResponse::FencePgForMetadataTransfer {
                source_primary_lease_deadline_ms,
            } => *source_primary_lease_deadline_ms,
            _ => unreachable!("metadata transfer fence command returned the wrong response"),
        };
        Ok(FencedPgMetadataTransferSnapshot::new(
            self.snapshot.clone(),
            source_primary_lease_deadline_ms,
        ))
    }

    pub fn install_unavailable_pg_transition_metadata_transfer(
        &mut self,
        binding: UnavailablePgTransitionMutationBinding,
        transfer: PgMetadataTransferProof,
        expected_destination_epoch: ClusterEpoch,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        self.apply_and_commit_command(
            ControlPlaneCommand::SetPgActingSetWithMetadataTransfer {
                pg_id: binding.pg_id(),
                acting_set: binding.destination_acting_set().to_vec(),
                transfer,
                expected_destination_epoch,
                unavailable_transition: Some(binding),
            },
        )?;
        Ok(self.snapshot.clone())
    }

    pub fn set_pg_state(
        &mut self,
        pg_id: PgId,
        state: PgState,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        self.apply_and_commit_command(ControlPlaneCommand::SetPgState { pg_id, state })?;
        Ok(self.snapshot.clone())
    }

    pub fn complete_pg_peering(
        &mut self,
        pg_id: PgId,
        primary: NodeId,
        node_incarnation: u64,
        now_ms: u64,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        self.apply_and_commit_command(ControlPlaneCommand::CompletePgPeering {
            pg_id,
            primary,
            node_incarnation,
            complete_at_ms: now_ms,
        })?;
        Ok(self.snapshot.clone())
    }

    pub fn complete_ready_pg_peerings(
        &mut self,
        now_ms: u64,
    ) -> Result<Vec<PgId>, ControlPlaneError> {
        let ready = self.snapshot.ready_pg_peering_completions(now_ms)?;
        if ready.is_empty() {
            return Ok(Vec::new());
        }

        let pg_ids = ready.iter().map(|completion| completion.pg_id).collect();
        self.apply_and_commit_command(ControlPlaneCommand::CompleteReadyPgPeerings {
            ready_at_ms: now_ms,
            ready,
        })?;
        Ok(pg_ids)
    }

    pub fn heartbeat(
        &mut self,
        heartbeat: NodeHeartbeat,
        authority_now_ms: u64,
    ) -> Result<HeartbeatLease, ControlPlaneError> {
        self.heartbeat_with_lease_horizon_authority(heartbeat, authority_now_ms, None)
    }

    fn heartbeat_with_lease_horizon_authority(
        &mut self,
        heartbeat: NodeHeartbeat,
        authority_now_ms: u64,
        lease_horizon_authority: Option<LeaseHorizonAuthorityBinding>,
    ) -> Result<HeartbeatLease, ControlPlaneError> {
        if heartbeat.requested_lease_duration_ms == 0 {
            return Err(ControlPlaneError::InvalidLeaseDuration);
        }
        if heartbeat.requested_lease_duration_ms > MAX_HEARTBEAT_LEASE_MS {
            return Err(ControlPlaneError::LeaseDurationTooLong {
                requested_ms: heartbeat.requested_lease_duration_ms,
                max_ms: MAX_HEARTBEAT_LEASE_MS,
            });
        }
        let lease_deadline_ms = self.snapshot.heartbeat_lease_deadline(
            heartbeat.node_id,
            authority_now_ms,
            heartbeat.requested_lease_duration_ms,
        )?;
        let observed_epoch = heartbeat.observed_epoch;
        let current_epoch = self.snapshot.cluster_epoch;
        let node_id = heartbeat.node_id;
        self.apply_heartbeat_command(ControlPlaneCommand::RecordNodeHeartbeat {
            heartbeat,
            heartbeat_at_ms: authority_now_ms,
            lease_deadline_ms,
            lease_horizon_authority,
        })?;
        let serving = self.snapshot.node(node_id).is_some_and(|record| {
            observed_epoch == current_epoch
                && record.can_serve_primary(self.snapshot.cluster_epoch, authority_now_ms)
        });
        Ok(HeartbeatLease {
            authority_incarnation: self.snapshot.authority_incarnation,
            cluster_epoch: self.snapshot.cluster_epoch,
            node_id,
            lease_deadline_ms,
            serving,
            snapshot: self.snapshot.clone(),
        })
    }

    fn current_heartbeat_lease_for_node(
        &self,
        node_id: NodeId,
        now_ms: u64,
    ) -> Result<HeartbeatLease, ControlPlaneError> {
        self.snapshot
            .current_heartbeat_lease_for_node(node_id, now_ms)
    }

    fn refresh_node_heartbeat_internal(
        &mut self,
        heartbeat: NodeHeartbeat,
        authority_now_ms: u64,
        lease_horizon_authority: Option<LeaseHorizonAuthorityBinding>,
    ) -> Result<ControlPlaneHeartbeatRefresh, ControlPlaneError> {
        let history_reference_validation_epoch = self.snapshot.cluster_epoch();
        let node_id = heartbeat.node_id;
        let requested_observed_epoch = heartbeat.observed_epoch;
        let previous_observed_epoch = self
            .snapshot
            .node(node_id)
            .and_then(NodeControlRecord::last_observed_epoch);
        let mut lease = self.heartbeat_with_lease_horizon_authority(
            heartbeat,
            authority_now_ms,
            lease_horizon_authority,
        )?;
        let just_activated_pgs: BTreeSet<PgId> = self
            .complete_ready_pg_peerings(authority_now_ms)?
            .into_iter()
            .collect();
        if !just_activated_pgs.is_empty() {
            lease = self.current_heartbeat_lease_for_node(node_id, authority_now_ms)?;
        }
        let current_epoch = self.snapshot.cluster_epoch();
        let observed_epoch = [Some(requested_observed_epoch), previous_observed_epoch]
            .into_iter()
            .flatten()
            .filter(|observed_epoch| *observed_epoch <= current_epoch)
            .max()
            .unwrap_or(requested_observed_epoch);
        let runtime_map = self.snapshot.runtime_map_for_storage_node_refresh(
            authority_now_ms,
            node_id,
            observed_epoch,
        )?;
        Ok(ControlPlaneHeartbeatRefresh {
            lease,
            runtime_map,
            history_reference_validation_epoch,
        })
    }

    pub fn expire_heartbeat_leases(
        &mut self,
        now_ms: u64,
    ) -> Result<HeartbeatLeaseExpiry, ControlPlaneError> {
        self.store.ensure_healthy()?;
        self.promote_volatile_heartbeat_leases_if_needed()?;
        let expire_at_ms = self.snapshot.heartbeat_lease_expiry_timestamp(now_ms);
        let command = ControlPlaneCommand::ExpireHeartbeatLeases { expire_at_ms };
        let applied = self.snapshot.apply_control_plane_command(command.clone())?;
        let durable_applied = self
            .durable_snapshot
            .apply_control_plane_command(command.clone())?;
        let ControlPlaneCommandResponse::ExpireHeartbeatLeases {
            expired_nodes,
            peering_pgs,
        } = applied.response()
        else {
            unreachable!("heartbeat lease expiry command returned the wrong response");
        };
        let expired_nodes = expired_nodes.clone();
        let peering_pgs = peering_pgs.clone();
        if !expired_nodes.is_empty() && applied.changed() {
            self.commit_rebased_command(command, applied, durable_applied)?;
        }
        Ok(HeartbeatLeaseExpiry {
            cluster_epoch: self.snapshot.cluster_epoch,
            expired_nodes,
            peering_pgs,
            snapshot: self.snapshot.clone(),
        })
    }

    pub fn authorize_node_service(
        &self,
        node_id: NodeId,
        node_incarnation: u64,
        observed_epoch: ClusterEpoch,
        now_ms: u64,
    ) -> Result<NodeServiceAuthorization, ControlPlaneError> {
        self.store.ensure_healthy()?;
        let record = self
            .snapshot
            .nodes
            .get(&node_id)
            .ok_or(ControlPlaneError::UnknownNode {
                node_id: node_id.as_u32(),
            })?;
        if matches!(
            record.membership,
            NodeMembershipState::Out | NodeMembershipState::Removed
        ) {
            return Err(ControlPlaneError::NodeCannotReceiveLease {
                node_id: node_id.as_u32(),
                membership: record.membership,
            });
        }
        if node_incarnation != record.node_incarnation {
            return Err(ControlPlaneError::NodeIncarnationMismatch {
                node_id: node_id.as_u32(),
                sender_incarnation: node_incarnation,
                current_incarnation: record.node_incarnation,
            });
        }
        if observed_epoch != self.snapshot.cluster_epoch {
            return Err(ControlPlaneError::StaleNodeObservedEpoch {
                node_id: node_id.as_u32(),
                observed_epoch,
                current_epoch: self.snapshot.cluster_epoch,
            });
        }
        let lease_deadline_ms =
            record
                .lease_deadline_ms
                .ok_or(ControlPlaneError::NodeLeaseExpired {
                    node_id: node_id.as_u32(),
                    now_ms,
                    lease_deadline_ms: None,
                })?;
        if lease_deadline_ms <= now_ms {
            return Err(ControlPlaneError::NodeLeaseExpired {
                node_id: node_id.as_u32(),
                now_ms,
                lease_deadline_ms: Some(lease_deadline_ms),
            });
        }
        if !record.can_serve_primary(self.snapshot.cluster_epoch, now_ms) {
            return Err(ControlPlaneError::NodeNotServingCurrentEpoch {
                node_id: node_id.as_u32(),
                cluster_epoch: self.snapshot.cluster_epoch,
            });
        }
        Ok(NodeServiceAuthorization {
            authority_incarnation: self.snapshot.authority_incarnation,
            cluster_epoch: self.snapshot.cluster_epoch,
            node_id,
            node_incarnation,
            lease_deadline_ms,
        })
    }

    pub fn validate_node_service_authorization(
        &self,
        authorization: &NodeServiceAuthorization,
        now_ms: u64,
    ) -> Result<(), ControlPlaneError> {
        self.store.ensure_healthy()?;
        if authorization.authority_incarnation() != self.snapshot.authority_incarnation {
            return Err(ControlPlaneError::StaleAuthorityIncarnation {
                authority_incarnation: authorization.authority_incarnation(),
                current_authority_incarnation: self.snapshot.authority_incarnation,
            });
        }
        if authorization.cluster_epoch() != self.snapshot.cluster_epoch {
            return Err(ControlPlaneError::StaleAuthorizationEpoch {
                cluster_epoch: authorization.cluster_epoch(),
                current_epoch: self.snapshot.cluster_epoch,
            });
        }
        if authorization.lease_deadline_ms() <= now_ms {
            return Err(ControlPlaneError::NodeLeaseExpired {
                node_id: authorization.node_id().as_u32(),
                now_ms,
                lease_deadline_ms: Some(authorization.lease_deadline_ms()),
            });
        }

        let node_id = authorization.node_id();
        let record = self
            .snapshot
            .nodes
            .get(&node_id)
            .ok_or(ControlPlaneError::UnknownNode {
                node_id: node_id.as_u32(),
            })?;
        if record.node_incarnation != authorization.node_incarnation() {
            return Err(ControlPlaneError::NodeIncarnationMismatch {
                node_id: node_id.as_u32(),
                sender_incarnation: authorization.node_incarnation(),
                current_incarnation: record.node_incarnation,
            });
        }
        let current_lease_deadline_ms =
            record
                .lease_deadline_ms
                .ok_or(ControlPlaneError::NodeLeaseExpired {
                    node_id: node_id.as_u32(),
                    now_ms,
                    lease_deadline_ms: None,
                })?;
        if current_lease_deadline_ms <= now_ms {
            return Err(ControlPlaneError::NodeLeaseExpired {
                node_id: node_id.as_u32(),
                now_ms,
                lease_deadline_ms: Some(current_lease_deadline_ms),
            });
        }
        if !record.can_serve_primary(self.snapshot.cluster_epoch, now_ms) {
            return Err(ControlPlaneError::NodeNotServingCurrentEpoch {
                node_id: node_id.as_u32(),
                cluster_epoch: self.snapshot.cluster_epoch,
            });
        }
        Ok(())
    }

    pub fn authorize_pg_primary_service(
        &self,
        pg_id: PgId,
        primary_node_id: NodeId,
        node_incarnation: u64,
        observed_epoch: ClusterEpoch,
        now_ms: u64,
    ) -> Result<PgPrimaryAuthorization, ControlPlaneError> {
        let node_authorization =
            self.authorize_node_service(primary_node_id, node_incarnation, observed_epoch, now_ms)?;
        let record = self
            .snapshot
            .pgs
            .get(&pg_id)
            .ok_or(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })?;
        if record.state != PgState::Active {
            return Err(ControlPlaneError::PgNotActive {
                pg_id: pg_id.get(),
                cluster_epoch: self.snapshot.cluster_epoch,
                state: record.state,
            });
        }
        let serving_primary = record
            .active_primary
            .filter(|primary| {
                self.snapshot
                    .node(*primary)
                    .is_some_and(|node| node.can_serve_primary(self.snapshot.cluster_epoch, now_ms))
            })
            .ok_or(ControlPlaneError::PgHasNoServingPrimary {
                pg_id: pg_id.get(),
                cluster_epoch: self.snapshot.cluster_epoch,
            })?;
        if serving_primary != primary_node_id {
            return Err(ControlPlaneError::NodeNotPgPrimary {
                pg_id: pg_id.get(),
                node_id: primary_node_id.as_u32(),
                primary_node_id: serving_primary.as_u32(),
                cluster_epoch: self.snapshot.cluster_epoch,
            });
        }
        validate_pg_primary_active_observation(&self.snapshot, pg_id, primary_node_id)?;
        Ok(PgPrimaryAuthorization {
            authority_incarnation: self.snapshot.authority_incarnation,
            cluster_epoch: self.snapshot.cluster_epoch,
            pg_id,
            primary_node_id,
            primary_node_incarnation: node_authorization.node_incarnation(),
            lease_deadline_ms: node_authorization.lease_deadline_ms(),
        })
    }

    pub fn authorize_pg_operation(
        &self,
        operation: PgServiceOperation,
        pg_id: PgId,
        primary_node_id: NodeId,
        node_incarnation: u64,
        observed_epoch: ClusterEpoch,
        now_ms: u64,
    ) -> Result<PgOperationAuthorization, ControlPlaneError> {
        let primary = self.authorize_pg_primary_service(
            pg_id,
            primary_node_id,
            node_incarnation,
            observed_epoch,
            now_ms,
        )?;
        Ok(PgOperationAuthorization { operation, primary })
    }

    pub fn validate_pg_operation_authorization(
        &self,
        authorization: &PgOperationAuthorization,
        now_ms: u64,
    ) -> Result<(), ControlPlaneError> {
        self.validate_pg_operation_authorization_for(
            authorization,
            authorization.operation(),
            now_ms,
        )
    }

    pub fn validate_pg_operation_authorization_for(
        &self,
        authorization: &PgOperationAuthorization,
        operation: PgServiceOperation,
        now_ms: u64,
    ) -> Result<(), ControlPlaneError> {
        if authorization.operation() != operation {
            return Err(ControlPlaneError::PgOperationAuthorizationMismatch {
                expected: operation,
                actual: authorization.operation(),
            });
        }
        self.validate_node_service_authorization(&authorization.primary().into(), now_ms)?;

        let primary_node_id = authorization.primary_node_id();
        let pg_id = authorization.pg_id();
        let pg = self
            .snapshot
            .pgs
            .get(&pg_id)
            .ok_or(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })?;
        if pg.state != PgState::Active {
            return Err(ControlPlaneError::PgNotActive {
                pg_id: pg_id.get(),
                cluster_epoch: self.snapshot.cluster_epoch,
                state: pg.state,
            });
        }
        let serving_primary = pg
            .active_primary
            .filter(|primary| {
                self.snapshot
                    .node(*primary)
                    .is_some_and(|node| node.can_serve_primary(self.snapshot.cluster_epoch, now_ms))
            })
            .ok_or(ControlPlaneError::PgHasNoServingPrimary {
                pg_id: pg_id.get(),
                cluster_epoch: self.snapshot.cluster_epoch,
            })?;
        if serving_primary != primary_node_id {
            return Err(ControlPlaneError::NodeNotPgPrimary {
                pg_id: pg_id.get(),
                node_id: primary_node_id.as_u32(),
                primary_node_id: serving_primary.as_u32(),
                cluster_epoch: self.snapshot.cluster_epoch,
            });
        }
        validate_pg_primary_active_observation(&self.snapshot, pg_id, primary_node_id)
    }

    #[must_use]
    pub fn serving_pg_primary(&self, pg_id: PgId, now_ms: u64) -> Option<NodeId> {
        let record = self.snapshot.pgs.get(&pg_id)?;
        if record.state != PgState::Active {
            return None;
        }
        let primary = record.active_primary?;
        self.snapshot
            .node(primary)
            .is_some_and(|node| node.can_serve_primary(self.snapshot.cluster_epoch, now_ms))
            .then_some(())?;
        primary_has_current_pg_state(&self.snapshot, pg_id, primary, PgState::Active)
            .then_some(primary)
    }

    #[must_use]
    pub fn deterministic_pg_primary(
        &self,
        _pg_id: PgId,
        acting_set: &[NodeId],
        now_ms: u64,
    ) -> Option<NodeId> {
        acting_set.iter().copied().find(|node_id| {
            self.snapshot
                .nodes
                .get(node_id)
                .is_some_and(|record| record.can_serve_primary(self.snapshot.cluster_epoch, now_ms))
        })
    }

    #[cfg(test)]
    fn commit_snapshot(
        &mut self,
        mut next_snapshot: ClusterControlSnapshot,
    ) -> Result<(), ControlPlaneError> {
        next_snapshot.record_history_from(&self.snapshot);
        validate_control_plane_snapshot(
            "attempted to commit invalid control-plane snapshot",
            &next_snapshot,
        )?;
        self.store.checkpoint_manually_modified_snapshot_for_test(
            &self.durable_snapshot,
            &next_snapshot,
        )?;
        self.durable_snapshot = next_snapshot.clone();
        self.snapshot = next_snapshot;
        *self.runtime_map_content_certificate.lock().map_err(|_| {
            ControlPlaneError::rpc_protocol(
                "control-plane runtime-map content certificate lock poisoned".to_owned(),
            )
        })? = None;
        Ok(())
    }

    fn promote_volatile_heartbeat_leases_if_needed(&mut self) -> Result<(), ControlPlaneError> {
        let Some(command) = self
            .snapshot
            .promote_volatile_heartbeat_leases_command(&self.durable_snapshot)?
        else {
            return Ok(());
        };
        let live_applied = self.snapshot.apply_control_plane_command(command.clone())?;
        let durable_applied = self
            .durable_snapshot
            .apply_control_plane_command(command.clone())?;
        self.commit_rebased_command(command, live_applied, durable_applied)?;
        Ok(())
    }

    fn commit_rebased_command(
        &mut self,
        command: ControlPlaneCommand,
        live_applied: AppliedControlPlaneCommand,
        durable_applied: AppliedControlPlaneCommand,
    ) -> Result<AppliedControlPlaneCommand, ControlPlaneError> {
        if !durable_applied.changed() {
            return Ok(live_applied);
        }
        let response = live_applied.response().clone();
        let changed = live_applied.changed();
        // Both applications already record history and validate their resulting snapshots.
        let next_live_snapshot = live_applied.into_snapshot();
        let next_durable_snapshot = durable_applied.into_snapshot();
        self.store
            .commit_command(&self.durable_snapshot, &command, &next_durable_snapshot)?;
        self.durable_snapshot = next_durable_snapshot;
        self.snapshot = next_live_snapshot.clone();
        *self.runtime_map_content_certificate.lock().map_err(|_| {
            ControlPlaneError::rpc_protocol(
                "control-plane runtime-map content certificate lock poisoned".to_owned(),
            )
        })? = None;
        Ok(AppliedControlPlaneCommand::new(
            next_live_snapshot,
            response,
            changed,
        ))
    }
}

impl<S: ControlPlaneStore> ControlPlaneHeartbeatSink for SingleAuthorityControlPlane<S> {
    fn submit_node_heartbeat(
        &mut self,
        heartbeat: NodeHeartbeat,
        authority_now_ms: u64,
    ) -> Result<HeartbeatLease, ControlPlaneError> {
        self.heartbeat(heartbeat, authority_now_ms)
    }
}

impl<S: ControlPlaneStore> ControlPlaneHeartbeatRuntimeMapSource
    for SingleAuthorityControlPlane<S>
{
    fn refresh_node_heartbeat(
        &mut self,
        heartbeat: NodeHeartbeat,
        authority_now_ms: u64,
    ) -> Result<ControlPlaneHeartbeatRefresh, ControlPlaneError> {
        self.refresh_node_heartbeat_internal(heartbeat, authority_now_ms, None)
    }

    fn refresh_node_heartbeat_with_lease_horizon_authority(
        &mut self,
        heartbeat: NodeHeartbeat,
        authority_now_ms: u64,
        lease_horizon_authority: LeaseHorizonAuthorityBinding,
    ) -> Result<ControlPlaneHeartbeatRefresh, ControlPlaneError> {
        self.refresh_node_heartbeat_internal(
            heartbeat,
            authority_now_ms,
            Some(lease_horizon_authority),
        )
    }
}

impl<S: ControlPlaneStore> ControlPlaneRuntimeMapSource for SingleAuthorityControlPlane<S> {
    fn runtime_map_snapshot(
        &self,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        self.store.ensure_healthy()?;
        self.snapshot.runtime_map(authority_now_ms)
    }

    fn runtime_map_status(
        &self,
        authority_now_ms: u64,
    ) -> Result<ControlPlaneRuntimeMapStatus, ControlPlaneError> {
        self.store.ensure_healthy()?;
        let mut cached = self.runtime_map_content_certificate.lock().map_err(|_| {
            ControlPlaneError::rpc_protocol(
                "control-plane runtime-map content certificate lock poisoned".to_owned(),
            )
        })?;
        if let Some(certificate) = *cached {
            if let Some(status) =
                ControlPlaneRuntimeMapStatus::from_snapshot_with_content_certificate(
                    &self.snapshot,
                    authority_now_ms,
                    RuntimeMapFreshnessProof::SingleAuthority {
                        authority_incarnation: self.snapshot.authority_incarnation(),
                        issued_at_ms: authority_now_ms,
                    },
                    certificate,
                )?
            {
                return Ok(status);
            }
        }
        let runtime_map = self.snapshot.runtime_map(authority_now_ms)?;
        *cached = Some(RuntimeMapContentCertificate::from_snapshot_and_runtime_map(
            &self.snapshot,
            &runtime_map,
        ));
        Ok(ControlPlaneRuntimeMapStatus::from_runtime_map(&runtime_map))
    }

    fn runtime_map_diagnostics_snapshot(
        &self,
        authority_now_ms: u64,
    ) -> Result<ControlPlaneRuntimeMapDiagnosticSnapshot, ControlPlaneError> {
        self.store.ensure_healthy()?;
        let runtime_map = self.snapshot.runtime_map(authority_now_ms)?;
        let node_leases = self
            .snapshot
            .nodes()
            .map(|node| ControlPlaneRuntimeMapNodeLeaseDiagnostic {
                node_id: node.node_id(),
                lease_deadline_ms: node.lease_deadline_ms(),
            })
            .collect();
        ControlPlaneRuntimeMapDiagnosticSnapshot::new(runtime_map, node_leases)
    }

    fn pending_metadata_command_recoveries(
        &self,
        _authority_now_ms: u64,
    ) -> Result<PendingMetadataCommandRecoveryListing, ControlPlaneError> {
        self.store.ensure_healthy()?;
        Ok(self.snapshot.pending_metadata_command_recoveries())
    }

    fn pg_runtime_map_snapshot(
        &self,
        pg_id: PgId,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        self.store.ensure_healthy()?;
        self.snapshot
            .reconstructed_runtime_map_for_pg_with_fallback_validity(
                pg_id,
                non_serving_runtime_map_validity(authority_now_ms),
            )
    }

    fn serving_pg_runtime_map_snapshot(
        &self,
        pg_id: PgId,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        self.store.ensure_healthy()?;
        self.snapshot
            .serving_runtime_map_for_pg_with_freshness_proof(
                pg_id,
                authority_now_ms,
                RuntimeMapFreshnessProof::SingleAuthority {
                    authority_incarnation: self.snapshot.authority_incarnation(),
                    issued_at_ms: authority_now_ms,
                },
            )
    }
}

impl<S: ControlPlaneStore> ControlPlaneLinearizedCommandSink for SingleAuthorityControlPlane<S> {
    fn submit_control_plane_command(
        &mut self,
        command: ControlPlaneCommand,
    ) -> Result<AppliedControlPlaneCommand, ControlPlaneError> {
        self.apply_and_commit_command(command)
    }
}

impl<S: ControlPlaneStore> ControlPlaneLinearizedRuntimeMapSource
    for SingleAuthorityControlPlane<S>
{
    fn linearized_runtime_map_snapshot(
        &self,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        self.runtime_map_snapshot(authority_now_ms)
    }
}

impl<S: ControlPlaneStore> ControlPlaneAdmin for SingleAuthorityControlPlane<S> {
    fn authority_clock_context(
        &self,
    ) -> Result<ControlPlaneAuthorityClockContext, ControlPlaneError> {
        self.store.ensure_healthy()?;
        Ok(ControlPlaneAuthorityClockContext::new(
            self.snapshot().max_committed_timestamp_ms(),
            None,
            true,
            true,
        ))
    }

    fn set_pg_acting_set(
        &mut self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        SingleAuthorityControlPlane::set_pg_acting_set(self, pg_id, acting_set)
    }

    fn begin_unavailable_pg_placement_transition(
        &mut self,
        pg_id: PgId,
        unavailable_node_id: NodeId,
        begin_at_ms: u64,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        SingleAuthorityControlPlane::begin_unavailable_pg_placement_transition(
            self,
            pg_id,
            unavailable_node_id,
            begin_at_ms,
        )
    }

    fn complete_unavailable_pg_placement_transition(
        &mut self,
        work: &UnavailablePgReconciliationWork,
        ready_at_ms: u64,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        SingleAuthorityControlPlane::complete_unavailable_pg_placement_transition(
            self,
            work,
            ready_at_ms,
        )
    }

    fn authorize_unavailable_pg_staging_intents_batch(
        &mut self,
        authorizations: &[UnavailablePgStagingIntentAuthorizationRequest],
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        SingleAuthorityControlPlane::authorize_unavailable_pg_staging_intents_batch(
            self,
            authorizations,
        )
    }

    fn install_unavailable_pg_placement_transitions_batch(
        &mut self,
        transitions: &[UnavailablePgTransitionInstallRequest],
        expected_destination_epoch: ClusterEpoch,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        SingleAuthorityControlPlane::install_unavailable_pg_placement_transitions_batch(
            self,
            transitions,
            expected_destination_epoch,
        )
    }

    fn apply_metadata_transfer_staging_evidence_page(
        &mut self,
        operation_payload: Vec<u8>,
        page_digest: [u8; 32],
    ) -> Result<Vec<u8>, ControlPlaneError> {
        let applied = self.apply_and_commit_command(
            ControlPlaneCommand::ApplyMetadataTransferStagingEvidencePage {
                operation_payload,
                page_digest,
            },
        )?;
        let ControlPlaneCommandResponse::ApplyMetadataTransferStagingEvidencePage {
            apply_receipt,
        } = applied.response()
        else {
            unreachable!("staging evidence command returned the wrong response");
        };
        Ok(apply_receipt.clone())
    }

    fn checkpoint_metadata_transfer_staging_evidence_pages(
        &mut self,
        actor_node_id: NodeId,
        actor_node_incarnation: u64,
        first_generation: u64,
        last_generation: u64,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        SingleAuthorityControlPlane::checkpoint_metadata_transfer_staging_evidence_pages(
            self,
            actor_node_id,
            actor_node_incarnation,
            first_generation,
            last_generation,
        )
    }

    fn finalize_metadata_transfer_staging_generation(
        &mut self,
        cleanup: FinalizeMetadataTransferStagingGenerationRequest,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        SingleAuthorityControlPlane::finalize_metadata_transfer_staging_generation(self, cleanup)
    }

    fn fence_pg_for_metadata_transfer(
        &mut self,
        pg_id: PgId,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        SingleAuthorityControlPlane::fence_pg_for_metadata_transfer(self, pg_id)
    }

    fn fence_pg_for_metadata_transfer_with_source_lease(
        &mut self,
        pg_id: PgId,
    ) -> Result<FencedPgMetadataTransferSnapshot, ControlPlaneError> {
        SingleAuthorityControlPlane::fence_pg_for_metadata_transfer_with_source_lease(self, pg_id)
    }

    fn fence_unavailable_pg_transition_with_source_lease(
        &mut self,
        binding: UnavailablePgTransitionMutationBinding,
    ) -> Result<FencedPgMetadataTransferSnapshot, ControlPlaneError> {
        SingleAuthorityControlPlane::fence_unavailable_pg_transition_with_source_lease(
            self, binding,
        )
    }

    fn set_pg_acting_set_with_metadata_transfer(
        &mut self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
        transfer: PgMetadataTransferProof,
        expected_destination_epoch: ClusterEpoch,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        SingleAuthorityControlPlane::set_pg_acting_set_with_metadata_transfer_at_epoch(
            self,
            pg_id,
            acting_set,
            transfer,
            expected_destination_epoch,
        )
    }

    fn install_unavailable_pg_transition_metadata_transfer(
        &mut self,
        binding: UnavailablePgTransitionMutationBinding,
        transfer: PgMetadataTransferProof,
        expected_destination_epoch: ClusterEpoch,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        SingleAuthorityControlPlane::install_unavailable_pg_transition_metadata_transfer(
            self,
            binding,
            transfer,
            expected_destination_epoch,
        )
    }
}
