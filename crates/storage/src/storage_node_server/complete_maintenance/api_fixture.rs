// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

// Standalone compiler harness. Dependencies are stubs, but the module included
// at AUTHORITY_PATH is the unmodified production authority implementation.
#![allow(dead_code, unused_imports)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct PgId(u32);
impl PgId {
    pub fn get(self) -> u32 {
        self.0
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ClusterEpoch(u64);
#[derive(Clone, Copy)]
pub struct NodeId(u32);
impl NodeId {
    pub fn as_u32(self) -> u32 {
        self.0
    }
}
#[derive(PartialEq, Eq)]
pub struct PgTopology;
pub enum StoreError {
    RouteCapabilitySubjectMismatch {
        operation: &'static str,
    },
    RouteMapExpired {
        cluster_epoch: ClusterEpoch,
        valid_until_ms: u64,
        now_ms: u64,
    },
}
mod clock {
    pub fn current_time_millis() -> u64 {
        0
    }
}
mod node_runtime {
    use super::*;
    pub mod pg_store {
        pub struct PgStore;
        impl PgStore {
            pub fn pg_id(&self) -> u32 {
                0
            }
        }
    }
    pub mod server {
        use super::*;
        pub struct Config {
            pub node_id: NodeId,
            pub cluster_epoch: ClusterEpoch,
        }
        impl Config {
            pub fn route_map_valid_until_ms(&self) -> Option<u64> {
                None
            }
        }
        pub struct Node;
        impl Node {
            pub fn pg_topology(&self) -> &PgTopology {
                &PgTopology
            }
        }
        pub struct StorageNodeConnectionHandler {
            pub config: Config,
            pub node: Node,
        }
        impl StorageNodeConnectionHandler {
            pub fn validate_pg_route(
                &self,
                _: NodeId,
                _: ClusterEpoch,
                _: PgId,
            ) -> Result<(), StorageRpcErrorResponse> {
                Ok(())
            }
            pub fn validate_primary_pg(
                &self,
                _: PgId,
                _: &str,
            ) -> Result<(), StorageRpcErrorResponse> {
                Ok(())
            }
            pub fn validate_metadata_mutation_route_not_expired(
                &self,
            ) -> Result<(), StorageRpcErrorResponse> {
                Ok(())
            }
            pub fn current_route_map_lease_is_valid(&self) -> bool {
                true
            }
        }
        pub struct StorageNodeSession;
        impl StorageNodeSession {
            pub fn holds_metadata_command_pg_lock(&self, _: PgId) -> bool {
                true
            }
        }
        pub struct StorageNodeMetadataCommandGuard {
            pub released: bool,
            pub pg_id: PgId,
        }
        pub struct StorageRpcMetadataCommandStateRequest {
            pub node_id: NodeId,
            pub cluster_epoch: ClusterEpoch,
            pub pg_id: PgId,
        }
        pub struct StorageRpcErrorResponse {
            pub code: StorageRpcErrorCode,
            pub message: String,
        }
        pub enum StorageRpcErrorCode {
            Internal,
        }
        mod complete_maintenance {
            use super::*;
            #[path = "AUTHORITY_PATH"]
            mod authority;
            pub(in crate::node_runtime) use authority::{CompleteLogMutationAuthority, Mutation};
            fn owner_mints<'a>(
                handler: &'a StorageNodeConnectionHandler,
                session: &'a StorageNodeSession,
                serialization: &'a Option<StorageNodeMetadataCommandGuard>,
                pg: &'a pg_store::PgStore,
                request: &StorageRpcMetadataCommandStateRequest,
            ) -> Result<CompleteLogMutationAuthority<'a>, StorageRpcErrorResponse> {
                CompleteLogMutationAuthority::admit(
                    handler,
                    session,
                    serialization,
                    pg,
                    request,
                    Mutation::Record,
                )
            }
        }
        pub(in crate::node_runtime) use complete_maintenance::{
            CompleteLogMutationAuthority, Mutation,
        };
    }
    mod sibling {
        use super::*;
        use server::*;
        fn probe<'a>(
            handler: &'a StorageNodeConnectionHandler,
            session: &'a StorageNodeSession,
            serialization: &'a Option<StorageNodeMetadataCommandGuard>,
            pg: &'a pg_store::PgStore,
            request: &StorageRpcMetadataCommandStateRequest,
            authority: &CompleteLogMutationAuthority<'a>,
        ) {
            PROBE
        }
    }
}
