// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

// Compile the real receiver and its private admitted factory, stubbing only dependencies.
#![allow(dead_code, unused_imports)]
use std::{time::{Duration, Instant}, sync::Arc};
#[derive(Clone, Copy, PartialEq, Eq)] pub struct PgId(u32);
impl PgId { fn get(self) -> u32 { self.0 } }
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)] pub struct ClusterEpoch;
#[derive(Clone, Copy, PartialEq, Eq)] pub struct NodeId;
#[derive(PartialEq, Eq)] pub struct PgTopology;
#[derive(Clone, Copy, PartialEq, Eq)] pub struct MetadataCommandId;
impl MetadataCommandId {
    fn pg_id(self) -> PgId { PgId(0) }
    fn cluster_epoch(self) -> ClusterEpoch { ClusterEpoch }
    fn log_index(self) -> PgId { PgId(0) }
}
pub enum StoreError { RouteCapabilitySubjectMismatch { operation: &'static str }, OperationDeadlineExceeded { context: &'static str } }
mod clock { pub fn current_time_millis() -> u64 { 0 } pub fn monotonic_time_millis() -> u64 { 0 } }
mod storage_rpc { pub type StorageRpcPayloadError = std::io::Error; }
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
            pub fn command_sha256(&self) -> [u8;32] { [0;32] }
            pub fn checksum_crc64(&self) -> u64 { 0 }
        }
        pub struct CompleteReplicaAbandonmentRequest<'a>(CompletePublicationRequest<'a>);
        impl CompleteReplicaAbandonmentRequest<'_> {
            pub fn primary(&self) -> NodeId { NodeId }
            pub fn publication_request(&self) -> &CompletePublicationRequest<'_> { &self.0 }
            pub fn abandon_response(&self, _: &PgStore, _: &server::RemoteReplicaAbandonmentAuthority<'_>) -> Result<Vec<u8>, Error> { Ok(vec![]) }
        }
        pub struct Error;
        impl Error { pub fn into_store_error(self) -> Result<StoreError, Self> { Err(self) } }
        impl std::fmt::Display for Error { fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f,"error") } }
        pub struct CompleteReplicaAbandonmentQuery;
        impl CompleteReplicaAbandonmentQuery {
            pub fn decode(_: &[u8], _: &PgTopology) -> Result<Self, storage_rpc::StorageRpcPayloadError> { Ok(Self) }
            pub fn request(&self) -> Result<CompleteReplicaAbandonmentRequest<'_>, storage_rpc::StorageRpcPayloadError> { Ok(CompleteReplicaAbandonmentRequest(CompletePublicationRequest(&()))) }
        }
    }
    pub mod server {
        use super::*;
        use pg_store::*;
        pub struct Config { pub node_id: NodeId, pub cluster_epoch: ClusterEpoch, pub pg_routes: Vec<Route>, pub historical_pg_routes: Vec<Route>, pub pending_metadata_command_recoveries: Vec<(PgId, Recovery)> }
        pub struct Route { pub pg_id: u32, pub cluster_epoch: ClusterEpoch, pub primary_node_id: NodeId, pub acting_set: Vec<NodeId> }
        pub struct Recovery;
        impl Recovery {
            pub fn reporting_node_id(&self) -> NodeId { NodeId }
            pub fn pending(&self) -> &Self { self }
            pub fn cluster_epoch(&self) -> ClusterEpoch { ClusterEpoch }
            pub fn log_index(&self) -> u32 { 0 }
            pub fn command_checksum(&self) -> u64 { 0 }
            pub fn command_sha256(&self) -> [u8;32] { [0;32] }
        }
        pub struct Node;
        impl Node {
            pub fn pg_topology(&self) -> &PgTopology { &PgTopology }
            pub fn get_pg_until(&self, _: u32, _: Instant) -> Result<PgStore, StoreError> { Ok(PgStore) }
        }
        pub struct StorageNodeConnectionHandler { pub config: Config, pub node: Node, pub metadata_command_locks: Locks, pub rpc_auth: Option<Arc<()>> }
        impl StorageNodeConnectionHandler {
            pub fn validate_active_admission(&self, _: &StorageNodeRouteAdmissionPermit, _: &'static str) -> Result<(), StorageRpcErrorResponse> { Ok(()) }
            pub fn validate_pg_route(&self, _: NodeId, _: ClusterEpoch, _: PgId) -> Result<(), StorageRpcErrorResponse> { Ok(()) }
            pub fn validate_historical_active_pg_route_for_metadata_command_recovery(&self, _: NodeId, _: ClusterEpoch, _: PgId) -> Result<(), StorageRpcErrorResponse> { Ok(()) }
            pub fn bounded_historical_metadata_command_fence(&self, _: ClusterEpoch, _: PgId) -> Result<StorageNodeRouteFence, StorageRpcErrorResponse> { Ok(StorageNodeRouteFence { local_valid_until_monotonic_ms: None }) }
            pub fn current_route_map_lease(&self) -> Option<()> { None }
            pub fn metadata_command_pg_guard_until(&self, _: &StorageNodeSession, _: PgId, _: Option<Instant>) -> Result<Option<StorageNodeMetadataCommandGuard>, StorageRpcErrorResponse> { Ok(None) }
        }
        pub struct StorageNodeSession;
        pub struct StorageNodeRouteAdmissionPermit;
        pub struct StorageNodeMetadataCommandGuard { pub released: bool, pub pg_id: PgId, pub locks: Locks }
        pub struct Locks { pub state: Arc<()> }
        #[derive(Clone, Copy)] pub struct StorageNodeRouteFence { pub local_valid_until_monotonic_ms: Option<u64> }
        impl StorageNodeRouteFence {
            pub fn current(_: &Config, _: Option<()>) -> Self { Self { local_valid_until_monotonic_ms: None } }
            pub fn validate_store_at(self, _: u64, _: u64) -> Result<(), StoreError> { Ok(()) }
        }
        pub struct StorageRpcErrorResponse { pub code: StorageRpcErrorCode, pub message: String }
        pub enum StorageRpcErrorCode { Internal, StaleShardLocation, PayloadDecode }
        fn store_error_response(_: StoreError) -> StorageRpcErrorResponse { StorageRpcErrorResponse { code: StorageRpcErrorCode::Internal, message: String::new() } }
        fn storage_node_rpc_io_timeout(_: Option<&()>) -> Duration { Duration::from_secs(1) }
        fn encode_storage_rpc_error_response(_: &StorageRpcErrorResponse) -> Result<Vec<u8>, storage_rpc::StorageRpcPayloadError> { Ok(vec![]) }
        fn encode_storage_rpc_success_response(_: &[u8]) -> Vec<u8> { vec![] }
        #[path = "OWNER_PATH"] mod owner;
        pub(in crate::node_runtime) use owner::RemoteReplicaAbandonmentAuthority;
        mod sibling {
            use super::*;
            fn probe<'a>(handler: &'a StorageNodeConnectionHandler, session: &'a StorageNodeSession, route: &'a StorageNodeRouteAdmissionPermit, guard: &'a StorageNodeMetadataCommandGuard, pg: &'a PgStore, request: &'a CompleteReplicaAbandonmentRequest<'a>, authority: &RemoteReplicaAbandonmentAuthority<'a>) { PROBE }
        }
    }
}
