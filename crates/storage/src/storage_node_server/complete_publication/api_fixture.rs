// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

// Standalone compiler harness: the included authority is the real source.
#![allow(dead_code, unused_imports)]
use std::time::Instant;
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct PgId(u32);
impl PgId { pub fn get(self) -> u32 { self.0 } }
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ClusterEpoch;
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct NodeId;
#[derive(PartialEq, Eq)]
pub struct PgTopology;
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct MetadataCommandId;
impl MetadataCommandId {
    fn pg_id(self) -> PgId { PgId(0) }
    fn cluster_epoch(self) -> ClusterEpoch { ClusterEpoch }
}
pub enum StoreError {
    RouteCapabilitySubjectMismatch { operation: &'static str },
    RouteMapExpired { cluster_epoch: ClusterEpoch, valid_until_ms: u64, now_ms: u64 },
    OperationDeadlineExceeded { context: &'static str },
}
mod clock {
    pub fn current_time_millis() -> u64 { 0 }
    pub fn monotonic_time_millis() -> u64 { 0 }
}
mod node_runtime {
    use super::*;
    pub mod pg_store {
        use super::*;
        pub struct PgStore;
        impl PgStore { pub fn pg_id(&self) -> u32 { 0 } }
        pub struct CompletePublicationRequest<'a>(&'a ());
        impl CompletePublicationRequest<'_> {
            pub fn bound_deadline(&self, deadline: Instant) -> Instant { deadline }
            pub fn id(&self) -> MetadataCommandId { MetadataCommandId }
            pub fn node_id(&self) -> NodeId { NodeId }
            pub fn topology(&self) -> &PgTopology { &PgTopology }
            pub fn command_sha256(&self) -> [u8; 32] { [0; 32] }
        }
    }
    pub mod server {
        use super::*;
        use pg_store::{PgStore, CompletePublicationRequest};
        pub struct Config { pub node_id: NodeId, pub cluster_epoch: ClusterEpoch }
        impl Config { pub fn route_map_valid_until_ms(&self) -> Option<u64> { None } }
        pub struct Node;
        impl Node { pub fn pg_topology(&self) -> &PgTopology { &PgTopology } }
        pub struct StorageNodeConnectionHandler { pub config: Config, pub node: Node }
        impl StorageNodeConnectionHandler {
            pub fn validate_active_admission(&self, _: &StorageNodeRouteAdmissionPermit, _: &'static str) -> Result<(), StorageRpcErrorResponse> { Ok(()) }
            pub fn validate_pg_route(&self, _: NodeId, _: ClusterEpoch, _: PgId) -> Result<(), StorageRpcErrorResponse> { Ok(()) }
            pub fn validate_primary_pg(&self, _: PgId, _: &str) -> Result<(), StorageRpcErrorResponse> { Ok(()) }
            pub fn current_route_map_lease_is_valid(&self) -> bool { true }
            pub fn complete_publication_route(&self, _: &CompletePublicationRequest<'_>) -> Result<(StorageNodeMetadataCommandLockBinding, StorageNodeRouteFence), StorageRpcErrorResponse> {
                Ok((StorageNodeMetadataCommandLockBinding { pg_id: PgId(0), cluster_epoch: ClusterEpoch, authority: StorageNodeMetadataCommandLockAuthority::CurrentPrimary }, StorageNodeRouteFence))
            }
        }
        pub struct StorageNodeSession;
        impl StorageNodeSession {
            pub fn holds_metadata_command_pg_lock_with_binding(&self, _: StorageNodeMetadataCommandLockBinding) -> bool { true }
        }
        pub struct StorageNodeRouteAdmissionPermit;
        #[derive(Clone, Copy)]
        pub struct StorageNodeMetadataCommandLockBinding { pub pg_id: PgId, pub cluster_epoch: ClusterEpoch, pub authority: StorageNodeMetadataCommandLockAuthority }
        #[derive(Clone, Copy)]
        pub enum StorageNodeMetadataCommandLockAuthority { CurrentPrimary }
        #[derive(Clone, Copy)]
        pub struct StorageNodeRouteFence;
        impl StorageNodeRouteFence {
            pub fn validate_store_at(self, _: u64, _: u64) -> Result<(), StoreError> { Ok(()) }
        }
        pub struct StorageRpcErrorResponse;
        fn store_error_response(_: StoreError) -> StorageRpcErrorResponse { StorageRpcErrorResponse }
        mod complete_publication {
            use super::*;
            #[path = "AUTHORITY_PATH"]
            mod authority;
            pub(in crate::node_runtime) use authority::CompletePublicationAuthority;
            fn owner_mints<'a>(handler: &'a StorageNodeConnectionHandler, session: &'a StorageNodeSession, route: &'a StorageNodeRouteAdmissionPermit, pg: &'a PgStore, request: &'a CompletePublicationRequest<'a>, deadline: Instant) -> Result<CompletePublicationAuthority<'a>, StorageRpcErrorResponse> {
                CompletePublicationAuthority::admit(handler, session, route, pg, request, deadline)
            }
        }
        pub(in crate::node_runtime) use complete_publication::CompletePublicationAuthority;
    }
    mod sibling {
        use super::*;
        use server::*;
        fn probe<'a>(handler: &'a StorageNodeConnectionHandler, session: &'a StorageNodeSession, route: &'a StorageNodeRouteAdmissionPermit, pg: &'a pg_store::PgStore, request: &'a pg_store::CompletePublicationRequest<'a>, authority: &CompletePublicationAuthority<'a>) {
            PROBE
        }
    }
}
