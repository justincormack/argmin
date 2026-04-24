#![cfg_attr(test, allow(dead_code))]

#[cfg(test)]
use std::sync::MutexGuard;

#[cfg(test)]
use storage::StoredObject;

#[cfg(test)]
/// PG guards held while an object metadata snapshot is live.
///
/// Read-side payload access relies on generation-scoped leases, so current
/// object read paths only need the metadata PG guard.
pub(super) struct ObjectPgGuards<'a> {
    pub(super) meta: MutexGuard<'a, storage::PgStore>,
}

#[cfg(test)]
impl<'a> ObjectPgGuards<'a> {
    pub(super) fn new(meta: MutexGuard<'a, storage::PgStore>) -> Self {
        Self { meta }
    }

    pub(super) fn meta(&self) -> &storage::PgStore {
        &self.meta
    }
}

#[cfg(test)]
pub(super) struct LockedReadObject<'a> {
    pub(super) record: StoredObject,
    pub(super) pgs: ObjectPgGuards<'a>,
}
