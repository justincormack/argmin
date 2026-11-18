// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

// Compile both actual owners. Stubs model unrelated dependencies, not authority visibility.
#![allow(dead_code, unused_imports)]
use std::time::Instant;
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct NodeId;
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct PgId;
impl PgId { fn get(self) -> u32 { 0 } }
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct MetadataCommandId;
#[derive(PartialEq, Eq)]
pub struct PgTopology;
pub enum StoreError {
    RouteCapabilitySubjectMismatch { operation: &'static str },
    OperationDeadlineExceeded { context: &'static str },
}
mod cluster {
    use super::*;
    pub struct EmbeddedApplicationTarget<'a>(&'a node_runtime::engine::LocalNodeRuntime);
    impl EmbeddedApplicationTarget<'_> {
        pub fn node_id(&self) -> NodeId { NodeId }
        pub fn pg_id(&self) -> PgId { PgId }
        pub fn deadline(&self) -> Instant { Instant::now() }
        pub fn runtime(&self) -> Result<&node_runtime::engine::LocalNodeRuntime, StoreError> { Ok(self.0) }
        pub fn require_command(&self, _: MetadataCommandId, _: u64, _: [u8; 32]) -> Result<(), StoreError> { Ok(()) }
    }
    pub struct EmbeddedPublicationSection<'a>(&'a node_runtime::engine::LocalNodeRuntime);
    impl EmbeddedPublicationSection<'_> {
        pub fn application_target(&self, _: NodeId) -> Result<EmbeddedApplicationTarget<'_>, StoreError> { Ok(EmbeddedApplicationTarget(self.0)) }
    }
    pub struct CompleteFanoutRoute<'a>(&'a node_runtime::engine::LocalNodeRuntime);
    impl CompleteFanoutRoute<'_> {
        pub fn primary(&self) -> NodeId { NodeId }
        pub fn nodes(&self) -> impl Iterator<Item=NodeId> { [NodeId].into_iter() }
        pub fn require_embedded_section(&self, _: &EmbeddedPublicationSection<'_>) -> Result<(), StoreError> { Ok(()) }
        pub fn embedded_replica_target<'a>(&'a self, _: &node_runtime::clients::UnixStorageNodeClient, _: &'a node_runtime::clients::UnixStorageNodeMetadataCommandSession, _: NodeId, _: Instant) -> Result<EmbeddedApplicationTarget<'a>, StoreError> { Ok(EmbeddedApplicationTarget(self.0)) }
    }
}
mod node_runtime {
    use super::*;
    pub mod clients { pub struct UnixStorageNodeClient; pub struct UnixStorageNodeMetadataCommandSession; }
    pub mod pg_store {
        use super::*;
        use cluster::{CompleteFanoutRoute, EmbeddedPublicationSection};
        pub struct PgStore;
        impl PgStore {
            pub fn pg_id(&self) -> u32 { 0 }
            pub fn record_complete_replica_abandonment(&self, _: &engine::EmbeddedReplicaAbandonmentAuthority<'_>) -> Result<(), CompletePublicationError> { Ok(()) }
        }
        pub struct CompletePublicationError;
        impl From<StoreError> for CompletePublicationError { fn from(_: StoreError) -> Self { Self } }
        pub struct CompletePublicationRequest<'a>(&'a ());
        impl CompletePublicationRequest<'_> {
            pub fn id(&self) -> MetadataCommandId { MetadataCommandId }
            pub fn node_id(&self) -> NodeId { NodeId }
            pub fn topology(&self) -> &PgTopology { &PgTopology }
            pub fn command_sha256(&self) -> [u8;32] { [0;32] }
            pub fn checksum_crc64(&self) -> u64 { 0 }
            pub fn bound_deadline(&self, deadline: Instant) -> Instant { deadline }
        }
        pub struct Source;
        impl Source { fn request(&self, _: NodeId) -> Result<CompletePublicationRequest<'_>, StoreError> { Ok(CompletePublicationRequest(&())) } }
        pub struct Sender { source: Source }
        pub struct CompleteFanoutSender<'a> { route: CompleteFanoutRoute<'a>, sender: Sender }
        impl CompleteFanoutSender<'_> {
            fn require_authority(&self) -> Result<(), StoreError> { Ok(()) }
            fn require_primary(&self, _: &clients::UnixStorageNodeClient, _: &clients::UnixStorageNodeMetadataCommandSession) -> Result<(), StoreError> { Ok(()) }
            fn work_deadline(&self) -> Instant { Instant::now() }
        }
        #[derive(PartialEq, Eq)]
        pub enum UnstartedAbandonmentObservation { NoAttemptObserved, PrimaryTombstoneConfirmed }
        pub struct CompleteUnstartedAbandonment<'a> {
            owner: CompleteFanoutSender<'a>, observation: UnstartedAbandonmentObservation,
            replicas: std::collections::BTreeMap<NodeId, ReplicaAbandonmentObservation>,
        }
        #[path = "CUSTODY_PATH"]
        mod replicas;
        pub(in crate::node_runtime) use replicas::{ReplicaAbandonmentPermit, ReplicaAbandonmentObservation};
    }
    pub mod engine {
        use super::*;
        use pg_store::PgStore;
        pub struct Node;
        impl Node { fn get_pg_until(&self, _: u32, _: Instant) -> Result<PgStore, StoreError> { Ok(PgStore) } }
        pub struct LocalNodeRuntime { node: Node }
        impl LocalNodeRuntime { pub fn pg_topology(&self) -> &PgTopology { &PgTopology } }
        #[path = "OWNER_PATH"]
        mod owner;
        pub(in crate::node_runtime) use owner::EmbeddedReplicaAbandonmentAuthority;
    }
    mod sibling {
        use super::*;
        use pg_store::{PgStore, CompletePublicationRequest, CompleteFanoutSender, CompleteUnstartedAbandonment, ReplicaAbandonmentPermit};
        use engine::EmbeddedReplicaAbandonmentAuthority;
        use cluster::EmbeddedApplicationTarget;
        fn probe<'a>(permit: &'a ReplicaAbandonmentPermit<'a, 'a>, pg: &'a PgStore, request: &'a CompletePublicationRequest<'a>, authority: &EmbeddedReplicaAbandonmentAuthority<'a>, owner: &'a CompleteFanoutSender<'a>, target: &'a EmbeddedApplicationTarget<'a>) {
            PROBE
        }
    }
}
