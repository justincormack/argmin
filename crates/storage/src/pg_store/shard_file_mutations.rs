// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Process-local exclusion between final-file publication and deletion.
//!
//! One registry belongs to an opened PG and is shared by its locked store and
//! unlocked file-IO facade. It is not durable write authority: a delayed request
//! must still pass its effect fence after acquiring access. No registry lock is
//! held across IO, and unrelated shard keys do not exclude each other.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock, TryLockError};

use crate::ShardKey;

#[cfg(test)]
type DeleteWaitHook = Box<dyn FnOnce(&ShardKey) + Send>;

#[derive(Default)]
pub(in crate::node_runtime) struct ShardFileMutations {
    entries: Mutex<HashMap<ShardKey, Arc<RwLock<()>>>>,
    #[cfg(test)]
    before_delete_wait: Mutex<Option<DeleteWaitHook>>,
    #[cfg(test)]
    before_publish_wait: Mutex<Option<DeleteWaitHook>>,
}

struct Entry<'a> {
    registry: &'a ShardFileMutations,
    key: ShardKey,
    lock: Arc<RwLock<()>>,
}

impl ShardFileMutations {
    fn entry(&self, key: &ShardKey) -> Entry<'_> {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        Entry {
            registry: self,
            key: key.clone(),
            lock: Arc::clone(entries.entry(key.clone()).or_default()),
        }
    }

    pub(in crate::node_runtime) fn publish<T>(
        &self,
        key: &ShardKey,
        action: impl FnOnce() -> T,
    ) -> T {
        let entry = self.entry(key);
        // Concurrent exact publications keep the existing hard-link semantics.
        // Poison carries no protected data here: unwind has ended that IO task.
        let _guard = match entry.lock.try_read() {
            Ok(guard) => guard,
            Err(TryLockError::Poisoned(error)) => error.into_inner(),
            Err(TryLockError::WouldBlock) => {
                #[cfg(test)]
                {
                    let hook = self.before_publish_wait.lock().unwrap().take();
                    if let Some(hook) = hook {
                        hook(key);
                    }
                }
                entry.lock.read().unwrap_or_else(|error| error.into_inner())
            }
        };
        action()
    }

    pub(in crate::node_runtime) fn delete<T>(
        &self,
        key: &ShardKey,
        action: impl FnOnce() -> T,
    ) -> T {
        let entry = self.entry(key);
        let _guard = match entry.lock.try_write() {
            Ok(guard) => guard,
            Err(TryLockError::Poisoned(error)) => error.into_inner(),
            Err(TryLockError::WouldBlock) => {
                #[cfg(test)]
                {
                    let hook = self.before_delete_wait.lock().unwrap().take();
                    if let Some(hook) = hook {
                        hook(key);
                    }
                }
                entry
                    .lock
                    .write()
                    .unwrap_or_else(|error| error.into_inner())
            }
        };
        action()
    }
}

impl Drop for Entry<'_> {
    fn drop(&mut self) {
        // The read/write guard drops before this entry. Hold the registry mutex
        // while deciding whether a waiter/user still owns another strong ref;
        // acquisition cannot race removal and create a second lock for the key.
        let mut entries = self
            .registry
            .entries
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if Arc::strong_count(&self.lock) == 2 {
            entries.remove(&self.key);
        }
    }
}

#[cfg(test)]
#[path = "shard_file_mutations_tests.rs"]
mod tests;
