// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

// Actual target and bridge sources, with minimal non-authority dependencies.
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
pub struct MetadataCommandReplicaState;
mod metadata_command { pub use crate::MetadataCommandReplicaState; }
impl MetadataCommandId {
    fn pg_id(self) -> PgId { PgId(0) }
    fn cluster_epoch(self) -> ClusterEpoch { ClusterEpoch }
    fn log_index(self) -> std::num::NonZeroU64 { std::num::NonZeroU64::new(1).unwrap() }
}
struct AdmittedRouteEffectFence;
impl AdmittedRouteEffectFence {
    fn deadline(&self) -> Option<Deadline> { Some(Deadline) }
    fn require_valid_for(&self, _: ClusterEpoch) -> Result<(), StoreError> { Ok(()) }
}
struct Deadline;
impl Deadline { fn portable_wall_valid_until_ms(self) -> u64 { 1 } }
mod storage_rpc {
    use super::*;
    pub struct StorageRpcOperationDeadline { pub portable_wall_valid_until_ms: u64 }
    impl StorageRpcOperationDeadline { pub fn local_deadline(self) -> Instant { Instant::now() } }
}
pub enum StoreError {
    RouteCapabilitySubjectMismatch { operation: &'static str },
    OperationDeadlineExceeded { context: &'static str },
}
mod node { pub use crate::node_runtime::engine::LocalNodeRuntime; }
mod node_client {
    use super::*;
    pub struct UnixStorageNodeClient;
    impl UnixStorageNodeClient {
        pub fn node_id(&self) -> NodeId { NodeId }
        pub fn open_complete_historical_primary(&self, _: &crate::cluster::HistoricalRemoteApplicationRoute<'_>, _: Instant) -> Result<UnixStorageNodeMetadataCommandSession, StoreError> { unimplemented!() }
    }
    pub struct UnixStorageNodeMetadataCommandSession;
    pub struct MetadataCommandSessionIdentity;
    pub struct CompletePrimarySessionBorrow<'a>(&'a UnixStorageNodeMetadataCommandSession);
    impl CompletePrimarySessionBorrow<'_> {
        pub fn require_identity(&self, _: &MetadataCommandSessionIdentity) -> Result<(), StoreError> { Ok(()) }
    }
    impl UnixStorageNodeMetadataCommandSession {
        pub fn borrow_complete_primary(&self, _: &UnixStorageNodeClient, _: PgId, _: ClusterEpoch, _: Instant) -> Result<CompletePrimarySessionBorrow<'_>, StoreError> { Ok(CompletePrimarySessionBorrow(self)) }
    }
}
mod cluster {
    use super::*;
    use node_runtime::engine::LocalNodeRuntime;
    enum PublicationSubject { Current, Historical }
    struct Member(LocalNodeRuntime);
    impl Member {
        fn node_id(&self) -> NodeId { NodeId }
        fn embedded_publication_runtime(&self) -> Result<&LocalNodeRuntime, StoreError> { Ok(&self.0) }
        fn is_complete_fanout_client(&self, _: &crate::node_client::UnixStorageNodeClient) -> bool { true }
    }
    mod local { pub(super) use super::Member as LocalNodeStore; }
    mod complete_fanout_route {
        use super::*;
        use crate::node_client::UnixStorageNodeClient;
        pub struct CompleteFanoutRoute<'a> { peers: Vec<&'a Member>, primary: NodeId, pg_id: PgId, cutoff: Option<(Instant, ())> }
        impl CompleteFanoutRoute<'_> {
            pub fn require_client(&self, _: &UnixStorageNodeClient) -> Result<(), StoreError> { Ok(()) }
            pub fn require_valid(&self) -> Result<(), StoreError> { Ok(()) }
            pub fn require_command(&self, _: MetadataCommandId, _: u64, _: [u8; 32]) -> Result<(), StoreError> { Ok(()) }
            pub fn pg_id(&self) -> PgId { self.pg_id }
            pub fn epoch(&self) -> ClusterEpoch { ClusterEpoch }
        }
        #[path = "REMOTE_TARGET_PATH"]
        mod embedded_replica;
        pub(crate) use embedded_replica::RemotePrimaryEmbeddedTarget;
    }
    pub(crate) use complete_fanout_route::{CompleteFanoutRoute, RemotePrimaryEmbeddedTarget};
    struct Map(Member);
    impl Map {
        fn metadata_pg_acting_nodes(&self, _: ClusterEpoch, _: PgId) -> Result<Vec<&Member>, StoreError> { Ok(vec![&self.0]) }
        fn node(&self, _: NodeId) -> Option<&Member> { Some(&self.0) }
    }
    struct Cluster { local_map: Map }
    struct Admission { cluster: Cluster }
    type StorageClusterRouteAdmission = Admission;
    struct ClusterRuntimeMapSnapshot;
    struct HistoricalApplicationSubject { pg_id: PgId, primary: NodeId, pending: Pending, members: Vec<NodeId> }
    #[derive(Clone, Copy)]
    struct Pending;
    impl Pending {
        fn cluster_epoch(self) -> ClusterEpoch { ClusterEpoch }
        fn log_index(self) -> u64 { 1 }
        fn command_checksum(self) -> u64 { 0 }
        fn command_sha256(self) -> [u8; 32] { [0; 32] }
    }
    impl Admission {
        fn historical_application_subject(&self, _: &ClusterRuntimeMapSnapshot, _: PgId) -> Result<HistoricalApplicationSubject, StoreError> { unimplemented!() }
        fn effect_fence(&self) -> AdmittedRouteEffectFence { AdmittedRouteEffectFence }
        fn cluster_epoch(&self) -> ClusterEpoch { ClusterEpoch }
        fn require_valid_now_raw(&self) -> Result<(), StoreError> { Ok(()) }
    }
    #[path = "HISTORICAL_TARGET_PATH"]
    mod historical_remote;
    pub(crate) use historical_remote::HistoricalRemoteApplicationRoute;
    struct Route { subject: PublicationSubject, admission: Admission }
    pub struct EmbeddedPublicationSection<'a> { route: &'a Route }
    impl EmbeddedPublicationSection<'_> {
        fn application_member(&self, _: NodeId) -> Result<&Member, StoreError> { Ok(&self.route.admission.cluster.local_map.0) }
        pub fn require_command(&self, _: MetadataCommandId, _: u64, _: [u8; 32]) -> Result<(), StoreError> { Ok(()) }
        pub fn require_effect(&self) -> Result<(), StoreError> { Ok(()) }
        pub fn pg_id(&self) -> PgId { PgId(0) }
        pub fn epoch(&self) -> ClusterEpoch { ClusterEpoch }
        pub fn deadline(&self) -> Instant { Instant::now() }
    }
    #[path = "TARGET_PATH"]
    mod application_target;
    pub(crate) use application_target::EmbeddedApplicationTarget;
}
mod node_runtime {
    use super::*;
    pub mod pg_store {
        use super::*;
        pub struct PgStore;
        impl PgStore {
            pub fn pg_id(&self) -> u32 { 0 }
            pub fn apply_embedded_complete_metadata_command(&self, _: &engine::EmbeddedApplicationAuthority<'_>) -> Result<MetadataCommandReplicaState, CompleteApplicationError> { Ok(MetadataCommandReplicaState) }
        }
        pub struct CompleteApplicationError;
        impl From<StoreError> for CompleteApplicationError { fn from(_: StoreError) -> Self { Self } }
        pub struct CompleteApplicationRequest<'a>(&'a ());
        impl CompleteApplicationRequest<'_> {
            pub fn deadline(&self) -> Instant { Instant::now() }
            pub fn id(&self) -> MetadataCommandId { MetadataCommandId }
            pub fn node_id(&self) -> NodeId { NodeId }
            pub fn topology(&self) -> &PgTopology { &PgTopology }
            pub fn command_sha256(&self) -> [u8; 32] { [0; 32] }
            pub fn checksum_crc64(&self) -> u64 { 0 }
        }
    }
    pub mod engine {
        use super::*;
        use pg_store::PgStore;
        pub struct Node;
        impl Node {
            fn get_pg_until(&self, _: u32, _: Instant) -> Result<PgStore, StoreError> { Ok(PgStore) }
        }
        pub struct LocalNodeRuntime { node: Node }
        impl LocalNodeRuntime { pub fn pg_topology(&self) -> &PgTopology { &PgTopology } }
        #[path = "OWNER_PATH"]
        mod owner;
        pub(in crate::node_runtime) use owner::EmbeddedApplicationAuthority;
    }
    mod sibling {
        use super::*;
        use engine::{EmbeddedApplicationAuthority, LocalNodeRuntime};
        use cluster::{CompleteFanoutRoute, RemotePrimaryEmbeddedTarget, EmbeddedApplicationTarget, EmbeddedPublicationSection, HistoricalRemoteApplicationRoute};
        use node_client::{UnixStorageNodeClient, UnixStorageNodeMetadataCommandSession};
        use pg_store::{PgStore, CompleteApplicationRequest};
        fn probe<'a>(section: EmbeddedPublicationSection<'a>, target: &'a EmbeddedApplicationTarget<'a>, runtime: &'a LocalNodeRuntime, pg: &'a PgStore, request: &'a CompleteApplicationRequest<'a>, authority: &EmbeddedApplicationAuthority<'a>, route: CompleteFanoutRoute<'a>, client: &UnixStorageNodeClient, session: UnixStorageNodeMetadataCommandSession, historical: HistoricalRemoteApplicationRoute<'a>) {
            PROBE
        }
    }
}
