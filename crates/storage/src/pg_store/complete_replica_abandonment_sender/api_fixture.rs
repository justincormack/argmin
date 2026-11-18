// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

// Compile the actual sender owner; stub unrelated dependencies only.
#![allow(dead_code, unused_imports)]
use std::time::Instant;
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)] pub struct NodeId;
impl NodeId { pub fn as_u32(self) -> u32 { 0 } }
#[derive(Clone, Copy, PartialEq, Eq)] pub struct PgId;
#[derive(Clone, Copy, PartialEq, Eq)] pub struct ClusterEpoch;
#[derive(Clone, Copy, PartialEq, Eq)] pub struct MetadataCommandId;
#[derive(PartialEq, Eq)] pub struct PgTopology;
pub struct StorageNodeFailureDetail;
impl StorageNodeFailureDetail { pub fn new(_: String) -> Self { Self } }
pub enum StoreError { RouteCapabilitySubjectMismatch { operation: &'static str }, OperationDeadlineExceeded { context: &'static str }, StorageRpc { node_id: u32, operation: &'static str, failure: storage_rpc::StorageRpcErrorCode, detail: StorageNodeFailureDetail } }
mod storage_rpc { pub enum StorageRpcErrorCode { PayloadDecode } }
mod cluster {
    use super::*;
    pub struct EmbeddedPublicationSection<'a>(&'a ());
    pub struct CompleteFanoutRoute<'a>(&'a ());
    impl CompleteFanoutRoute<'_> {
        pub fn primary(&self) -> NodeId { NodeId }
        pub fn nodes(&self) -> impl Iterator<Item=NodeId> { [NodeId].into_iter() }
        pub fn pg_id(&self) -> PgId { PgId }
        pub fn epoch(&self) -> ClusterEpoch { ClusterEpoch }
        pub fn require_embedded_section(&self, _: &EmbeddedPublicationSection<'_>) -> Result<(), StoreError> { Ok(()) }
        pub fn require_client(&self, _: &node_runtime::clients::UnixStorageNodeClient) -> Result<(), StoreError> { Ok(()) }
        pub fn require_command(&self, _: MetadataCommandId, _: u64, _: [u8;32]) -> Result<(), StoreError> { Ok(()) }
    }
}
mod node_runtime {
    use super::*;
    pub mod clients {
        use super::*;
        pub struct CompletePrimarySessionBorrow<'a>(&'a ());
        pub struct UnixStorageNodeClient;
        impl UnixStorageNodeClient {
            pub fn node_id(&self) -> NodeId { NodeId }
            pub fn send_complete_replica_abandonment(&self, _: pg_store::CompleteReplicaAbandonmentAttempt<'_>) -> Result<(), pg_store::CompleteAbandonmentSendError> { Ok(()) }
        }
        pub struct UnixStorageNodeMetadataCommandSession;
        impl UnixStorageNodeMetadataCommandSession {
            pub fn borrow_complete_primary(&self, _: &UnixStorageNodeClient, _: PgId, _: ClusterEpoch, _: Instant) -> Result<CompletePrimarySessionBorrow<'_>, StoreError> { Ok(CompletePrimarySessionBorrow(&())) }
        }
    }
    pub mod pg_store {
        use super::*;
        use cluster::{CompleteFanoutRoute, EmbeddedPublicationSection};
        pub struct CompletePublicationRequest<'a>(&'a ());
        impl CompletePublicationRequest<'_> {
            pub fn id(&self) -> MetadataCommandId { MetadataCommandId }
            pub fn node_id(&self) -> NodeId { NodeId }
            pub fn topology(&self) -> &PgTopology { &PgTopology }
            pub fn command_sha256(&self) -> [u8;32] { [0;32] }
            pub fn checksum_crc64(&self) -> u64 { 0 }
            pub fn prepare_transport(&self, _: Instant) -> CompletePublicationTransport<'_> { CompletePublicationTransport(self) }
        }
        pub struct CompletePublicationTransport<'a>(&'a CompletePublicationRequest<'a>);
        impl CompletePublicationTransport<'_> {
            pub fn node_id(&self) -> NodeId { NodeId }
            pub fn deadline(&self) -> Instant { Instant::now() }
            pub fn require_fanout_route(&self, _: &CompleteFanoutRoute<'_>) -> Result<(), StoreError> { Ok(()) }
            pub fn encode_replica_abandonment_request(&self, _: &CompleteReplicaAbandonmentAttempt<'_>) -> Result<Vec<u8>, CompletePendingInspectionError> { Ok(vec![]) }
            pub fn decode_replica_abandonment_confirmation(&self, _: NodeId, _: &[u8]) -> Result<(), std::io::Error> { Ok(()) }
        }
        pub enum CompletePendingInspectionError { Store(StoreError), Other }
        impl std::fmt::Display for CompletePendingInspectionError { fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f,"error") } }
        pub enum CompleteAbandonmentSendError { NotSent(StoreError), Unconfirmed(StoreError) }
        pub struct Source;
        impl Source { fn request(&self, _: NodeId) -> Result<CompletePublicationRequest<'_>, StoreError> { Ok(CompletePublicationRequest(&())) } }
        pub struct Sender { source: Source }
        pub struct CompleteFanoutSender<'a> { route: CompleteFanoutRoute<'a>, sender: Sender }
        impl CompleteFanoutSender<'_> {
            fn require_authority(&self) -> Result<(), StoreError> { Ok(()) }
            fn require_primary(&self, _: &clients::UnixStorageNodeClient, _: &clients::UnixStorageNodeMetadataCommandSession) -> Result<(), StoreError> { Ok(()) }
            fn work_deadline(&self) -> Instant { Instant::now() }
        }
        #[derive(Clone, Copy, PartialEq, Eq)] pub enum ReplicaAbandonmentObservation { NoAttemptObserved, Unconfirmed, TombstoneConfirmed }
        pub struct CompleteUnstartedAbandonment<'a> { owner: CompleteFanoutSender<'a>, replicas: std::collections::BTreeMap<NodeId, ReplicaAbandonmentObservation> }
        impl CompleteUnstartedAbandonment<'_> {
            fn require_replica(&self, _: NodeId) -> Result<(), StoreError> { Ok(()) }
            fn replica_observation(&self, _: NodeId) -> ReplicaAbandonmentObservation { ReplicaAbandonmentObservation::NoAttemptObserved }
        }
        #[path = "OWNER_PATH"] mod owner;
        pub(in crate::node_runtime) use owner::CompleteReplicaAbandonmentAttempt;
        mod sibling {
            use super::*;
            fn probe<'a>(holder: &mut CompleteUnstartedAbandonment<'a>, section: &EmbeddedPublicationSection<'_>, client: &clients::UnixStorageNodeClient, attempt: CompleteReplicaAbandonmentAttempt<'a>, transport: &'a CompletePublicationTransport<'a>, owner: &'a CompleteFanoutSender<'a>, observation: &'a mut ReplicaAbandonmentObservation) {
                PROBE
            }
        }
    }
}
