use std::ops::Deref;
use std::sync::Arc;

use crate::node::SharedStorageNode;

/// Cluster-shaped storage handle.
///
/// Phase 1 keeps this backed by one local shared node so existing storage
/// behavior remains unchanged while coordinator code stops owning the local
/// implementation type directly. The single-node compatibility helpers and
/// `Deref` implementation are transitional; new cluster request paths should
/// grow explicit `StorageCluster` APIs instead of depending on local-node
/// delegation.
#[derive(Clone)]
pub struct StorageCluster {
    single_node: Arc<SharedStorageNode>,
}

impl StorageCluster {
    pub fn single_node(single_node: Arc<SharedStorageNode>) -> Self {
        Self { single_node }
    }

    pub fn shared_single_node(single_node: Arc<SharedStorageNode>) -> Arc<Self> {
        Arc::new(Self::single_node(single_node))
    }

    pub fn single_node_compat_handle(&self) -> &Arc<SharedStorageNode> {
        &self.single_node
    }

    pub fn single_node_compat_key(&self) -> usize {
        Arc::as_ptr(&self.single_node) as usize
    }
}

impl From<Arc<SharedStorageNode>> for StorageCluster {
    fn from(single_node: Arc<SharedStorageNode>) -> Self {
        Self::single_node(single_node)
    }
}

/// Transitional delegation for the first Phase 1 slice.
impl Deref for StorageCluster {
    type Target = SharedStorageNode;

    fn deref(&self) -> &Self::Target {
        self.single_node.as_ref()
    }
}
