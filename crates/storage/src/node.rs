/// LocalStorageNode and SharedStorageNode — manage multiple PgStores on a single node.
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use crate::error::StoreError;
use crate::pg_store::PgStore;
use crate::traits::{ShardStore, StorageNode};

/// A local storage node managing multiple PG stores.
///
/// On construction, takes a data directory and a list of PG IDs.
/// Creates PG directories if they don't exist. Routes operations
/// to the appropriate PgStore.
pub struct LocalStorageNode {
    stores: HashMap<u32, PgStore>,
    pg_id_list: Vec<u32>,
    data_dir: PathBuf,
}

impl LocalStorageNode {
    /// Open a storage node, creating PG directories as needed.
    pub fn open(data_dir: &Path, pg_ids: &[u32]) -> Result<Self, StoreError> {
        std::fs::create_dir_all(data_dir).map_err(|e| StoreError::Io {
            context: "create data dir",
            source: e,
        })?;

        let mut stores = HashMap::with_capacity(pg_ids.len());
        let mut pg_id_list = Vec::with_capacity(pg_ids.len());

        for &pg_id in pg_ids {
            let pg_dir = data_dir.join(format!("pg-{pg_id:04}"));
            let store = PgStore::open(&pg_dir, pg_id)?;
            stores.insert(pg_id, store);
            pg_id_list.push(pg_id);
        }

        pg_id_list.sort_unstable();

        Ok(Self {
            stores,
            pg_id_list,
            data_dir: data_dir.to_path_buf(),
        })
    }

    /// Return the data directory path.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Get a reference to the PgStore for the given PG ID.
    ///
    /// This returns the full PgStore which implements both ShardStore
    /// and PgMetadataStore.
    pub fn get_pg(&self, pg_id: u32) -> Result<&PgStore, StoreError> {
        self.stores
            .get(&pg_id)
            .ok_or(StoreError::PgNotFound { pg_id })
    }
}

impl StorageNode for LocalStorageNode {
    fn get_pg_store(&self, pg_id: u32) -> Result<&dyn ShardStore, StoreError> {
        self.stores
            .get(&pg_id)
            .map(|s| s as &dyn ShardStore)
            .ok_or(StoreError::PgNotFound { pg_id })
    }

    fn pg_ids(&self) -> &[u32] {
        &self.pg_id_list
    }
}

/// A shared storage node managing multiple PG stores behind mutexes.
///
/// All frontends share one `Arc<SharedStorageNode>`. Each PG's `PgStore`
/// is wrapped in a `std::sync::Mutex`, serializing all operations within
/// a PG while allowing parallelism across PGs.
pub struct SharedStorageNode {
    stores: HashMap<u32, Mutex<PgStore>>,
    pg_id_list: Vec<u32>,
    data_dir: PathBuf,
}

impl SharedStorageNode {
    /// Open a shared storage node, creating PG directories as needed.
    pub fn open(data_dir: &Path, pg_ids: &[u32]) -> Result<Self, StoreError> {
        std::fs::create_dir_all(data_dir).map_err(|e| StoreError::Io {
            context: "create data dir",
            source: e,
        })?;

        let mut stores = HashMap::with_capacity(pg_ids.len());
        let mut pg_id_list = Vec::with_capacity(pg_ids.len());

        for &pg_id in pg_ids {
            let pg_dir = data_dir.join(format!("pg-{pg_id:04}"));
            let store = PgStore::open(&pg_dir, pg_id)?;
            stores.insert(pg_id, Mutex::new(store));
            pg_id_list.push(pg_id);
        }

        pg_id_list.sort_unstable();

        Ok(Self {
            stores,
            pg_id_list,
            data_dir: data_dir.to_path_buf(),
        })
    }

    /// Return the data directory path.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Return the sorted list of PG IDs.
    pub fn pg_ids(&self) -> &[u32] {
        &self.pg_id_list
    }

    /// Lock and return a guard for the given PG.
    pub fn get_pg(&self, pg_id: u32) -> Result<MutexGuard<'_, PgStore>, StoreError> {
        let mutex = self
            .stores
            .get(&pg_id)
            .ok_or(StoreError::PgNotFound { pg_id })?;
        Ok(mutex.lock().unwrap_or_else(|e| e.into_inner()))
    }

    /// Lock two PGs for operations that span a metadata PG and a shard PG.
    ///
    /// When both IDs are the same, returns `(guard, None)` — the caller uses
    /// the single guard for both roles. When different, locks in ascending
    /// ID order to prevent deadlocks and returns `(lower_guard, Some(higher_guard))`.
    ///
    /// The first returned guard corresponds to `pg_a`, the second to `pg_b`.
    pub fn lock_two_pgs(
        &self,
        pg_a: u32,
        pg_b: u32,
    ) -> Result<(MutexGuard<'_, PgStore>, Option<MutexGuard<'_, PgStore>>), StoreError> {
        if pg_a == pg_b {
            let guard = self.get_pg(pg_a)?;
            return Ok((guard, None));
        }

        // Lock in ascending order to prevent deadlocks.
        if pg_a < pg_b {
            let guard_a = self.get_pg(pg_a)?;
            let guard_b = self.get_pg(pg_b)?;
            Ok((guard_a, Some(guard_b)))
        } else {
            let guard_b = self.get_pg(pg_b)?;
            let guard_a = self.get_pg(pg_a)?;
            Ok((guard_a, Some(guard_b)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_dir_accessor() {
        let tmp = tempfile::tempdir().unwrap();
        let node = LocalStorageNode::open(tmp.path(), &[0, 1]).unwrap();
        assert_eq!(node.data_dir(), tmp.path());
    }

    #[test]
    fn get_pg_store_not_found() {
        let tmp = tempfile::tempdir().unwrap();
        let node = LocalStorageNode::open(tmp.path(), &[0, 1]).unwrap();
        let result = node.get_pg_store(999);
        assert!(result.is_err());
    }

    #[test]
    fn get_pg_valid() {
        let tmp = tempfile::tempdir().unwrap();
        let node = LocalStorageNode::open(tmp.path(), &[0, 1, 2]).unwrap();
        assert!(node.get_pg(0).is_ok());
        assert!(node.get_pg(1).is_ok());
        assert!(node.get_pg(2).is_ok());
        assert!(node.get_pg(3).is_err());
    }

    #[test]
    fn pg_ids_sorted() {
        let tmp = tempfile::tempdir().unwrap();
        let node = LocalStorageNode::open(tmp.path(), &[5, 2, 8, 1]).unwrap();
        assert_eq!(node.pg_ids(), &[1, 2, 5, 8]);
    }

    // ── SharedStorageNode tests ──────────────────────────────────────

    #[test]
    fn shared_node_open_and_get_pg() {
        let tmp = tempfile::tempdir().unwrap();
        let node = SharedStorageNode::open(tmp.path(), &[0, 1, 2]).unwrap();
        assert!(node.get_pg(0).is_ok());
        assert!(node.get_pg(1).is_ok());
        assert!(node.get_pg(2).is_ok());
        assert!(node.get_pg(3).is_err());
    }

    #[test]
    fn shared_node_pg_ids_sorted() {
        let tmp = tempfile::tempdir().unwrap();
        let node = SharedStorageNode::open(tmp.path(), &[5, 2, 8, 1]).unwrap();
        assert_eq!(node.pg_ids(), &[1, 2, 5, 8]);
    }

    #[test]
    fn shared_node_data_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let node = SharedStorageNode::open(tmp.path(), &[0]).unwrap();
        assert_eq!(node.data_dir(), tmp.path());
    }

    #[test]
    fn shared_node_lock_two_pgs_same() {
        let tmp = tempfile::tempdir().unwrap();
        let node = SharedStorageNode::open(tmp.path(), &[0, 1]).unwrap();
        let (guard, opt) = node.lock_two_pgs(0, 0).unwrap();
        assert!(opt.is_none());
        assert_eq!(guard.pg_id(), 0);
    }

    #[test]
    fn shared_node_lock_two_pgs_different() {
        let tmp = tempfile::tempdir().unwrap();
        let node = SharedStorageNode::open(tmp.path(), &[0, 1]).unwrap();
        let (guard_a, opt_b) = node.lock_two_pgs(0, 1).unwrap();
        assert_eq!(guard_a.pg_id(), 0);
        let guard_b = opt_b.unwrap();
        assert_eq!(guard_b.pg_id(), 1);
    }

    #[test]
    fn shared_node_lock_two_pgs_reversed_order() {
        let tmp = tempfile::tempdir().unwrap();
        let node = SharedStorageNode::open(tmp.path(), &[0, 1]).unwrap();
        // Request pg_b=0, pg_a=1 — should still lock 0 first internally,
        // but return guards in the requested order.
        let (guard_a, opt_b) = node.lock_two_pgs(1, 0).unwrap();
        assert_eq!(guard_a.pg_id(), 1);
        let guard_b = opt_b.unwrap();
        assert_eq!(guard_b.pg_id(), 0);
    }
}
