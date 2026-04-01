use crate::types::*;
use proptest::prelude::*;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::num::NonZeroU64;

pub(super) const PROP_TEST_BUCKET: &str = "bucket";

const STATEFUL_MAX_KEYS: usize = 4;
const STATEFUL_MAX_OPS: usize = 20;
const STATEFUL_MAX_PAGE_SIZE: u32 = 5;
const PAGINATION_MAX_INPUT_KEYS: usize = 40;
const PAGINATION_MAX_PAGE_SIZE: u32 = 10;

const DISABLED_VERSIONING_TARGETS: [BucketVersioningState; 2] = [
    BucketVersioningState::Disabled,
    BucketVersioningState::Enabled,
];
const ENABLED_VERSIONING_TARGETS: [BucketVersioningState; 2] = [
    BucketVersioningState::Enabled,
    BucketVersioningState::Suspended,
];
const SUSPENDED_VERSIONING_TARGETS: [BucketVersioningState; 2] = [
    BucketVersioningState::Enabled,
    BucketVersioningState::Suspended,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ModelObjectKind {
    Live,
    DeleteMarker,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ObjectSnapshot {
    pub key: ObjectKey,
    pub version_id: VersionId,
    pub kind: ModelObjectKind,
    pub size: Option<u64>,
}

impl ObjectSnapshot {
    pub(super) fn is_live(&self) -> bool {
        self.kind == ModelObjectKind::Live
    }
}

impl From<&StoredObject> for ObjectSnapshot {
    fn from(value: &StoredObject) -> Self {
        Self {
            key: value.key().clone(),
            version_id: value.version_id(),
            kind: if value.is_delete_marker() {
                ModelObjectKind::DeleteMarker
            } else {
                ModelObjectKind::Live
            },
            size: value.as_live().map(|live| live.size),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct VersionSnapshot {
    pub object: ObjectSnapshot,
    pub is_latest: bool,
}

pub(super) fn object_snapshots_from_store(objects: &[StoredObject]) -> Vec<ObjectSnapshot> {
    objects.iter().map(ObjectSnapshot::from).collect()
}

pub(super) fn version_snapshots_from_store(objects: &[StoredObject]) -> Vec<VersionSnapshot> {
    let mut previous_key: Option<&ObjectKey> = None;
    let mut snapshots = Vec::with_capacity(objects.len());

    for object in objects {
        let is_latest = previous_key != Some(object.key());
        previous_key = Some(object.key());
        snapshots.push(VersionSnapshot {
            object: ObjectSnapshot::from(object),
            is_latest,
        });
    }

    snapshots
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ModelVersion {
    pub key: ObjectKey,
    pub version_id: VersionId,
    pub kind: ModelObjectKind,
    pub size: Option<u64>,
    pub logical_write_order: u64,
    pub became_noncurrent_at: Option<u64>,
}

impl ModelVersion {
    fn snapshot(&self) -> ObjectSnapshot {
        ObjectSnapshot {
            key: self.key.clone(),
            version_id: self.version_id,
            kind: self.kind,
            size: self.size,
        }
    }

    fn is_live(&self) -> bool {
        self.kind == ModelObjectKind::Live
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ModelOp {
    SetVersioning(BucketVersioningState),
    PutLive {
        key: ObjectKey,
        version_id: VersionId,
        size: u64,
    },
    PutDeleteMarker {
        key: ObjectKey,
        version_id: VersionId,
    },
    DeleteVersion {
        key: ObjectKey,
        version_id: VersionId,
    },
}

impl std::fmt::Display for ModelOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SetVersioning(state) => write!(f, "set-versioning({state:?})"),
            Self::PutLive {
                key,
                version_id,
                size,
            } => {
                write!(
                    f,
                    "put-live(key={key}, version_id={version_id}, size={size})"
                )
            }
            Self::PutDeleteMarker { key, version_id } => {
                write!(f, "put-delete-marker(key={key}, version_id={version_id})")
            }
            Self::DeleteVersion { key, version_id } => {
                write!(f, "delete-version(key={key}, version_id={version_id})")
            }
        }
    }
}

pub(super) fn render_trace(ops: &[ModelOp]) -> String {
    let mut rendered = String::new();
    for (index, op) in ops.iter().enumerate() {
        let _ = writeln!(&mut rendered, "{index}: {op}");
    }
    rendered
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ModelApplyError {
    DuplicateVersionId {
        key: ObjectKey,
        version_id: VersionId,
    },
    InvalidVersioningTransition {
        from: BucketVersioningState,
        to: BucketVersioningState,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct VersionStateModel {
    versioning: BucketVersioningState,
    next_logical_write_order: u64,
    objects: BTreeMap<ObjectKey, Vec<ModelVersion>>,
}

impl Default for VersionStateModel {
    fn default() -> Self {
        Self::new()
    }
}

impl VersionStateModel {
    pub(super) fn new() -> Self {
        Self::with_versioning(BucketVersioningState::Disabled)
    }

    pub(super) fn with_versioning(versioning: BucketVersioningState) -> Self {
        Self {
            versioning,
            next_logical_write_order: 1,
            objects: BTreeMap::new(),
        }
    }

    pub(super) fn versioning(&self) -> BucketVersioningState {
        self.versioning
    }

    pub(super) fn legal_versioning_targets(&self) -> &'static [BucketVersioningState] {
        match self.versioning {
            BucketVersioningState::Disabled => &DISABLED_VERSIONING_TARGETS,
            BucketVersioningState::Enabled => &ENABLED_VERSIONING_TARGETS,
            BucketVersioningState::Suspended => &SUSPENDED_VERSIONING_TARGETS,
        }
    }

    pub(super) fn apply_all(&mut self, ops: &[ModelOp]) -> Result<(), ModelApplyError> {
        for op in ops {
            self.apply(op)?;
        }
        Ok(())
    }

    pub(super) fn apply(&mut self, op: &ModelOp) -> Result<(), ModelApplyError> {
        match op {
            ModelOp::SetVersioning(state) => self.apply_versioning(*state),
            ModelOp::PutLive {
                key,
                version_id,
                size,
            } => self.insert_version(key.clone(), *version_id, ModelObjectKind::Live, Some(*size)),
            ModelOp::PutDeleteMarker { key, version_id } => self.insert_version(
                key.clone(),
                *version_id,
                ModelObjectKind::DeleteMarker,
                None,
            ),
            ModelOp::DeleteVersion { key, version_id } => {
                self.delete_version(key, *version_id);
                Ok(())
            }
        }
    }

    pub(super) fn current_snapshot(&self, key: &ObjectKey) -> Option<ObjectSnapshot> {
        self.current(key).map(ModelVersion::snapshot)
    }

    pub(super) fn live_listing(&self) -> Vec<ObjectSnapshot> {
        self.objects
            .values()
            .filter_map(|versions| versions.last())
            .filter(|version| version.is_live())
            .map(ModelVersion::snapshot)
            .collect()
    }

    pub(super) fn version_listing(&self) -> Vec<VersionSnapshot> {
        let mut versions = Vec::new();

        for per_key_versions in self.objects.values() {
            for (index, version) in per_key_versions.iter().rev().enumerate() {
                versions.push(VersionSnapshot {
                    object: version.snapshot(),
                    is_latest: index == 0,
                });
            }
        }

        versions
    }

    pub(super) fn versions_for_key(&self, key: &ObjectKey) -> &[ModelVersion] {
        self.objects.get(key).map_or(&[], Vec::as_slice)
    }

    fn current(&self, key: &ObjectKey) -> Option<&ModelVersion> {
        self.objects.get(key).and_then(|versions| versions.last())
    }

    fn apply_versioning(&mut self, next: BucketVersioningState) -> Result<(), ModelApplyError> {
        if !self.legal_versioning_targets().contains(&next) {
            return Err(ModelApplyError::InvalidVersioningTransition {
                from: self.versioning,
                to: next,
            });
        }
        self.versioning = next;
        Ok(())
    }

    fn insert_version(
        &mut self,
        key: ObjectKey,
        version_id: VersionId,
        kind: ModelObjectKind,
        size: Option<u64>,
    ) -> Result<(), ModelApplyError> {
        let versions = self.objects.entry(key.clone()).or_default();

        if version_id.is_versioned()
            && versions
                .iter()
                .any(|version| version.version_id == version_id)
        {
            return Err(ModelApplyError::DuplicateVersionId { key, version_id });
        }

        let logical_write_order = self.next_logical_write_order;
        self.next_logical_write_order += 1;

        if let Some(current) = versions.last_mut() {
            if current.is_live() && current.version_id != version_id {
                current.became_noncurrent_at = Some(logical_write_order);
            }
        }

        if version_id.is_null() {
            versions.retain(|version| version.version_id != VersionId::Null);
        }

        versions.push(ModelVersion {
            key,
            version_id,
            kind,
            size,
            logical_write_order,
            became_noncurrent_at: None,
        });

        Ok(())
    }

    fn delete_version(&mut self, key: &ObjectKey, version_id: VersionId) {
        let mut remove_key = false;

        if let Some(versions) = self.objects.get_mut(key) {
            let deleted_was_current = versions
                .last()
                .is_some_and(|current| current.version_id == version_id);

            versions.retain(|version| version.version_id != version_id);

            if deleted_was_current {
                if let Some(current) = versions.last_mut() {
                    if current.is_live() {
                        current.became_noncurrent_at = None;
                    }
                }
            }

            remove_key = versions.is_empty();
        }

        if remove_key {
            self.objects.remove(key);
        }
    }
}

pub(super) fn key_strategy() -> BoxedStrategy<ObjectKey> {
    proptest::string::string_regex(r"[A-Za-z0-9._/-]{1,16}")
        .expect("static key regex should compile")
        .prop_map(ObjectKey::from)
        .boxed()
}

pub(super) fn stateful_key_set_strategy() -> BoxedStrategy<Vec<ObjectKey>> {
    proptest::collection::btree_set(key_strategy(), 1..=STATEFUL_MAX_KEYS)
        .prop_map(|keys| keys.into_iter().collect())
        .boxed()
}

pub(super) fn versioning_state_strategy() -> BoxedStrategy<BucketVersioningState> {
    prop_oneof![
        1 => Just(BucketVersioningState::Disabled),
        2 => Just(BucketVersioningState::Enabled),
        3 => Just(BucketVersioningState::Suspended),
    ]
    .boxed()
}

pub(super) fn stateful_page_size_strategy() -> BoxedStrategy<u32> {
    (1u32..=STATEFUL_MAX_PAGE_SIZE).boxed()
}

pub(super) fn pagination_keys_strategy() -> BoxedStrategy<Vec<ObjectKey>> {
    proptest::collection::vec(key_strategy(), 0..=PAGINATION_MAX_INPUT_KEYS).boxed()
}

pub(super) fn pagination_page_size_strategy() -> BoxedStrategy<u32> {
    (1u32..=PAGINATION_MAX_PAGE_SIZE).boxed()
}

fn size_strategy() -> BoxedStrategy<u64> {
    (0u64..=4096).boxed()
}

#[derive(Debug, Clone)]
enum TraceSeed {
    Transition {
        choice: u8,
    },
    PutLive {
        key_index: usize,
        size: u64,
    },
    PutDeleteMarker {
        key_index: usize,
    },
    DeleteVersion {
        key_index: usize,
        choice: u8,
        size: u64,
    },
}

fn trace_seed_strategy(key_count: usize) -> BoxedStrategy<TraceSeed> {
    assert!(
        key_count > 0,
        "trace seed strategy requires at least one key"
    );

    prop_oneof![
        2 => any::<u8>().prop_map(|choice| TraceSeed::Transition { choice }),
        5 => (0usize..key_count, size_strategy()).prop_map(|(key_index, size)| TraceSeed::PutLive {
            key_index,
            size,
        }),
        3 => (0usize..key_count).prop_map(|key_index| TraceSeed::PutDeleteMarker { key_index }),
        2 => (0usize..key_count, any::<u8>(), size_strategy()).prop_map(
            |(key_index, choice, size)| TraceSeed::DeleteVersion {
                key_index,
                choice,
                size,
            }
        ),
    ]
    .boxed()
}

fn numbered_version_id(raw: u64) -> VersionId {
    VersionId::Versioned(
        NonZeroU64::new(raw).expect("numbered version ids produced by the helper are non-zero"),
    )
}

fn next_numbered_version_id(model: &VersionStateModel, key: &ObjectKey) -> VersionId {
    let next = model
        .versions_for_key(key)
        .iter()
        .filter_map(|version| match version.version_id {
            VersionId::Null => None,
            VersionId::Versioned(value) => Some(value.get()),
        })
        .max()
        .unwrap_or(0)
        + 1;
    numbered_version_id(next)
}

fn existing_version_for_choice(
    model: &VersionStateModel,
    key: &ObjectKey,
    choice: u8,
) -> Option<VersionId> {
    let versions = model.versions_for_key(key);
    if versions.is_empty() {
        None
    } else {
        Some(versions[(choice as usize) % versions.len()].version_id)
    }
}

fn reachable_op_from_seed(
    model: &VersionStateModel,
    keys: &[ObjectKey],
    seed: TraceSeed,
) -> ModelOp {
    match seed {
        TraceSeed::Transition { choice } => {
            let legal_targets = model.legal_versioning_targets();
            ModelOp::SetVersioning(legal_targets[(choice as usize) % legal_targets.len()])
        }
        TraceSeed::PutLive { key_index, size } => {
            let key = keys[key_index].clone();
            let version_id = match model.versioning() {
                BucketVersioningState::Enabled => next_numbered_version_id(model, &key),
                BucketVersioningState::Disabled | BucketVersioningState::Suspended => {
                    VersionId::Null
                }
            };
            ModelOp::PutLive {
                key,
                version_id,
                size,
            }
        }
        TraceSeed::PutDeleteMarker { key_index } => {
            let key = keys[key_index].clone();
            match model.versioning() {
                BucketVersioningState::Enabled => ModelOp::PutDeleteMarker {
                    key: key.clone(),
                    version_id: next_numbered_version_id(model, &key),
                },
                BucketVersioningState::Suspended => ModelOp::PutDeleteMarker {
                    key,
                    version_id: VersionId::Null,
                },
                BucketVersioningState::Disabled => ModelOp::DeleteVersion {
                    key,
                    version_id: VersionId::Null,
                },
            }
        }
        TraceSeed::DeleteVersion {
            key_index,
            choice,
            size,
        } => {
            let key = keys[key_index].clone();
            if let Some(version_id) = existing_version_for_choice(model, &key, choice) {
                ModelOp::DeleteVersion { key, version_id }
            } else {
                let version_id = match model.versioning() {
                    BucketVersioningState::Enabled => next_numbered_version_id(model, &key),
                    BucketVersioningState::Disabled | BucketVersioningState::Suspended => {
                        VersionId::Null
                    }
                };
                ModelOp::PutLive {
                    key,
                    version_id,
                    size,
                }
            }
        }
    }
}

pub(super) fn operation_trace_strategy(keys: Vec<ObjectKey>) -> BoxedStrategy<Vec<ModelOp>> {
    assert!(
        !keys.is_empty(),
        "operation trace strategy requires at least one generated key"
    );

    let key_count = keys.len();
    proptest::collection::vec(trace_seed_strategy(key_count), 1..=STATEFUL_MAX_OPS)
        .prop_map(move |seeds| {
            let mut model = VersionStateModel::new();
            let mut ops = Vec::with_capacity(seeds.len());

            for seed in seeds {
                let op = reachable_op_from_seed(&model, &keys, seed);
                model
                    .apply(&op)
                    .expect("reachable op construction must stay within model invariants");
                ops.push(op);
            }

            ops
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::strategy::ValueTree;
    use proptest::test_runner::TestRunner;

    fn live_object(key: &str, version_id: VersionId, size: u64) -> StoredObject {
        StoredObject::Live(LiveObjectRecord {
            bucket: PROP_TEST_BUCKET.into(),
            key: key.into(),
            version_id,
            owner: OwnerIdentity::from_principal("owner"),
            acl_grants: AclGrants::default(),
            public_read: false,
            generation_id: GenerationId::MIN,
            size,
            etag: ObjectEtag::SinglePart([size as u8, 0, 0, 0, 0, 0, 0, 0]),
            last_modified: version_id.to_u64(),
            became_noncurrent_at: None,
            storage_class: StorageClass::Standard,
            ec: EcShape { k: 4, m: 2 },
            layout: ObjectLayout::Standard,
            tags: None,
            metadata_blob: None,
            system_metadata_blob: None,
            object_lock: ObjectLockState::default(),
            encryption: ObjectEncryption::None,
        })
    }

    fn delete_marker(key: &str, version_id: VersionId) -> StoredObject {
        StoredObject::DeleteMarker(DeleteMarkerRecord {
            bucket: PROP_TEST_BUCKET.into(),
            key: key.into(),
            version_id,
            owner: OwnerIdentity::from_principal("owner"),
            last_modified: version_id.to_u64(),
        })
    }

    #[test]
    fn model_rejects_invalid_disabled_to_suspended_transition() {
        let mut model = VersionStateModel::new();

        assert_eq!(
            model.legal_versioning_targets(),
            &[
                BucketVersioningState::Disabled,
                BucketVersioningState::Enabled,
            ]
        );

        let err = model
            .apply(&ModelOp::SetVersioning(BucketVersioningState::Suspended))
            .unwrap_err();
        assert_eq!(
            err,
            ModelApplyError::InvalidVersioningTransition {
                from: BucketVersioningState::Disabled,
                to: BucketVersioningState::Suspended,
            }
        );
    }

    #[test]
    fn model_null_live_version_becomes_current_over_older_numbered_version() {
        let key = ObjectKey::from("k");
        let numbered = numbered_version_id(1);
        let mut model = VersionStateModel::new();

        model
            .apply_all(&[
                ModelOp::SetVersioning(BucketVersioningState::Enabled),
                ModelOp::PutLive {
                    key: key.clone(),
                    version_id: numbered,
                    size: 100,
                },
                ModelOp::SetVersioning(BucketVersioningState::Suspended),
                ModelOp::PutLive {
                    key: key.clone(),
                    version_id: VersionId::Null,
                    size: 200,
                },
            ])
            .unwrap();

        assert_eq!(model.versioning(), BucketVersioningState::Suspended);
        assert_eq!(
            model.current_snapshot(&key),
            Some(ObjectSnapshot {
                key: key.clone(),
                version_id: VersionId::Null,
                kind: ModelObjectKind::Live,
                size: Some(200),
            })
        );
        assert_eq!(
            model.live_listing(),
            vec![ObjectSnapshot {
                key: key.clone(),
                version_id: VersionId::Null,
                kind: ModelObjectKind::Live,
                size: Some(200),
            }]
        );

        let versions = model.version_listing();
        assert_eq!(versions.len(), 2);
        assert!(versions[0].is_latest);
        assert!(versions[0].object.is_live());
        assert_eq!(versions[0].object.version_id, VersionId::Null);
        assert_eq!(versions[1].object.version_id, numbered);
        assert!(!versions[1].is_latest);
        assert_eq!(
            model.versions_for_key(&key)[0].became_noncurrent_at,
            Some(2)
        );
    }

    #[test]
    fn deleting_current_version_restores_revealed_live_version_to_current() {
        let key = ObjectKey::from("k");
        let older = numbered_version_id(1);
        let current = numbered_version_id(2);
        let mut model = VersionStateModel::with_versioning(BucketVersioningState::Enabled);

        model
            .apply_all(&[
                ModelOp::PutLive {
                    key: key.clone(),
                    version_id: older,
                    size: 100,
                },
                ModelOp::PutLive {
                    key: key.clone(),
                    version_id: current,
                    size: 200,
                },
            ])
            .unwrap();
        assert_eq!(
            model.versions_for_key(&key)[0].became_noncurrent_at,
            Some(2)
        );

        model
            .apply(&ModelOp::DeleteVersion {
                key: key.clone(),
                version_id: current,
            })
            .unwrap();

        assert_eq!(
            model.current_snapshot(&key),
            Some(ObjectSnapshot {
                key: key.clone(),
                version_id: older,
                kind: ModelObjectKind::Live,
                size: Some(100),
            })
        );
        assert_eq!(model.versions_for_key(&key)[0].became_noncurrent_at, None);
    }

    #[test]
    fn store_version_snapshots_mark_only_first_version_per_key_latest() {
        let objects = vec![
            live_object("a", numbered_version_id(2), 20),
            live_object("a", numbered_version_id(1), 10),
            delete_marker("b", VersionId::Null),
        ];

        assert_eq!(
            object_snapshots_from_store(&objects),
            vec![
                ObjectSnapshot {
                    key: ObjectKey::from("a"),
                    version_id: numbered_version_id(2),
                    kind: ModelObjectKind::Live,
                    size: Some(20),
                },
                ObjectSnapshot {
                    key: ObjectKey::from("a"),
                    version_id: numbered_version_id(1),
                    kind: ModelObjectKind::Live,
                    size: Some(10),
                },
                ObjectSnapshot {
                    key: ObjectKey::from("b"),
                    version_id: VersionId::Null,
                    kind: ModelObjectKind::DeleteMarker,
                    size: None,
                },
            ]
        );

        let versions = version_snapshots_from_store(&objects);
        assert_eq!(versions.len(), 3);
        assert!(versions[0].is_latest);
        assert!(!versions[1].is_latest);
        assert!(versions[2].is_latest);
        assert_eq!(versions[2].object.kind, ModelObjectKind::DeleteMarker);
    }

    #[test]
    fn strategies_respect_phase_one_bounds() {
        let mut runner = TestRunner::default();

        let keys = stateful_key_set_strategy()
            .new_tree(&mut runner)
            .expect("key set strategy should produce a value")
            .current();
        assert!((1..=STATEFUL_MAX_KEYS).contains(&keys.len()));

        let page_size = stateful_page_size_strategy()
            .new_tree(&mut runner)
            .expect("page size strategy should produce a value")
            .current();
        assert!((1..=STATEFUL_MAX_PAGE_SIZE).contains(&page_size));

        let pagination_keys = pagination_keys_strategy()
            .new_tree(&mut runner)
            .expect("pagination key strategy should produce a value")
            .current();
        assert!(pagination_keys.len() <= PAGINATION_MAX_INPUT_KEYS);

        let pagination_page_size = pagination_page_size_strategy()
            .new_tree(&mut runner)
            .expect("pagination page size strategy should produce a value")
            .current();
        assert!((1..=PAGINATION_MAX_PAGE_SIZE).contains(&pagination_page_size));

        let versioning = versioning_state_strategy()
            .new_tree(&mut runner)
            .expect("versioning strategy should produce a value")
            .current();
        assert!(matches!(
            versioning,
            BucketVersioningState::Disabled
                | BucketVersioningState::Enabled
                | BucketVersioningState::Suspended
        ));

        let ops = operation_trace_strategy(keys)
            .new_tree(&mut runner)
            .expect("operation trace strategy should produce a value")
            .current();
        assert!((1..=STATEFUL_MAX_OPS).contains(&ops.len()));
        assert!(!render_trace(&ops).is_empty());
    }

    #[test]
    fn generated_operation_traces_respect_reachable_versioning_surface() {
        let mut runner = TestRunner::default();
        let keys = stateful_key_set_strategy()
            .new_tree(&mut runner)
            .expect("key set strategy should produce a value")
            .current();
        let ops = operation_trace_strategy(keys)
            .new_tree(&mut runner)
            .expect("operation trace strategy should produce a value")
            .current();

        let mut model = VersionStateModel::new();
        for op in &ops {
            match op {
                ModelOp::SetVersioning(target) => {
                    assert!(model.legal_versioning_targets().contains(target));
                }
                ModelOp::PutLive {
                    key, version_id, ..
                } => match model.versioning() {
                    BucketVersioningState::Enabled => {
                        assert!(version_id.is_versioned());
                        assert_eq!(*version_id, next_numbered_version_id(&model, key));
                    }
                    BucketVersioningState::Disabled | BucketVersioningState::Suspended => {
                        assert_eq!(*version_id, VersionId::Null);
                    }
                },
                ModelOp::PutDeleteMarker { key, version_id } => match model.versioning() {
                    BucketVersioningState::Enabled => {
                        assert!(version_id.is_versioned());
                        assert_eq!(*version_id, next_numbered_version_id(&model, key));
                    }
                    BucketVersioningState::Suspended => {
                        assert_eq!(*version_id, VersionId::Null);
                    }
                    BucketVersioningState::Disabled => {
                        panic!("disabled buckets should not emit delete marker writes");
                    }
                },
                ModelOp::DeleteVersion { key, version_id } => {
                    if model.versioning() == BucketVersioningState::Disabled {
                        assert_eq!(*version_id, VersionId::Null);
                    } else {
                        assert!(
                            model
                                .versions_for_key(key)
                                .iter()
                                .any(|version| version.version_id == *version_id),
                            "delete-version should target an existing version outside disabled mode"
                        );
                    }
                }
            }

            model.apply(op).unwrap();
        }
    }
}
