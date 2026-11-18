// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

// Compile the real bridge source, not a duplicate of its visibility declarations.
#![allow(dead_code, unused_imports)]
use std::time::Instant;
mod types { pub struct AdmittedRouteEffectFence; }
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct PgId(u32);
impl PgId { pub fn get(self) -> u32 { self.0 } }
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ClusterEpoch;
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct NodeId;
impl NodeId { pub fn as_u32(self) -> u32 { 0 } }
pub struct WriteAck;
pub struct ShardKey;
#[derive(PartialEq, Eq)]
pub struct PgTopology;
pub struct StreamUploadSegmentRecord;
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct MetadataCommandId;
impl MetadataCommandId {
    fn pg_id(self) -> PgId { PgId(0) }
    fn cluster_epoch(self) -> ClusterEpoch { ClusterEpoch }
}
pub enum StoreError {
    RouteCapabilitySubjectMismatch { operation: &'static str },
    OperationDeadlineExceeded { context: &'static str },
}
mod cluster {
    use super::*;
    pub struct ShardLocation;
    impl ShardLocation { pub fn data_pg_id(&self) -> PgId { PgId(0) } }
    pub struct EmbeddedPublicationSection<'a>(&'a node_runtime::engine::LocalNodeRuntime);
    impl EmbeddedPublicationSection<'_> {
        pub fn staging_capture_fence(&self) -> Result<crate::types::AdmittedRouteEffectFence, StoreError> { unimplemented!() }
        pub fn require_effect(&self) -> Result<(), StoreError> { Ok(()) }
        pub fn pg_id(&self) -> PgId { PgId(0) }
        pub fn node_id(&self) -> NodeId { NodeId }
        pub fn epoch(&self) -> ClusterEpoch { ClusterEpoch }
        pub fn require_command(&self, _: MetadataCommandId, _: u64, _: [u8; 32]) -> Result<(), StoreError> { Ok(()) }
        pub fn deadline(&self) -> Instant { Instant::now() }
        pub fn runtime(&self) -> Result<&node_runtime::engine::LocalNodeRuntime, StoreError> { Ok(self.0) }
        pub fn staging_placement(&self) -> Result<node_runtime::payload_placement::PayloadPlacementInput<'_>, StoreError> { unimplemented!() }
    }
}
mod node_runtime {
    use super::*;
    pub mod payload_placement { pub struct PayloadPlacementInput<'a>(pub &'a ()); }
    pub mod pg_store {
        use super::*;
        pub struct PgStore;
        pub struct StagingAcknowledgementBatch<'a, 'b>(&'a &'b ());
        impl StagingAcknowledgementBatch<'_, '_> {
            pub fn require_runtime(&self, _: &engine::LocalNodeRuntime) -> Result<(PgId, Instant), StagingWindowConfirmationError> { Ok((PgId(0), Instant::now())) }
            pub fn apply_in_store(&self, _: &PgStore, _: &engine::EmbeddedStagingAckAuthority<'_, '_, '_>) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
        }
        pub struct AllocatedStreamWindow<'a>(&'a ());
        impl AllocatedStreamWindow<'_> {
            pub fn file_deadline(&self, _: &cluster::EmbeddedPublicationSection<'_>) -> Result<Instant, StagingWindowConfirmationError> { Ok(Instant::now()) }
            pub fn require_file_store(&self, _: &cluster::EmbeddedPublicationSection<'_>, _: &PgStore) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
        }
        pub struct StreamStagingFileWriter<'a>(&'a ());
        impl StreamStagingFileWriter<'_> {
            pub fn require_file_runtime(&self, _: &engine::LocalNodeRuntime, _: u8, _: usize) -> Result<(cluster::ShardLocation, ShardKey, ()), StagingWindowConfirmationError> { unimplemented!() }
        }
        pub type StagingWindowConfirmationError = CompletePublicationError;
        pub struct ConfirmedStagingWindow<'a>(&'a ());
        impl ConfirmedStagingWindow<'_> {
            pub fn file_deadline(&self, _: &cluster::EmbeddedPublicationSection<'_>) -> Result<Instant, StagingWindowConfirmationError> { Ok(Instant::now()) }
            pub fn require_direct_file_store(&self, _: &cluster::EmbeddedPublicationSection<'_>, _: &PgStore) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
        }
        pub struct DirectStagingFileWriter<'a>(&'a ());
        impl DirectStagingFileWriter<'_> {
            pub fn require_file_runtime(&self, _: &engine::LocalNodeRuntime, _: u8, _: u64, _: usize) -> Result<(cluster::ShardLocation, ShardKey, ()), StagingWindowConfirmationError> { unimplemented!() }
        }
        pub struct Snapshot;
        impl Snapshot { pub fn finish(self) -> Result<(), StagingWindowConfirmationError> { Ok(()) } }
        impl PgStore {
            pub fn inspect_complete_settled_prefix(&self, _: u32, _: ClusterEpoch, _: &PgTopology) -> Result<Snapshot, StagingWindowConfirmationError> { Ok(Snapshot) }
        }
        pub struct StreamWindowAllocationPermit<'a, 'b>(&'a &'b ());
        pub struct CapturedStreamAppend;
        pub struct DirectStagingSnapshot;
        pub struct CapturedDirectTerminal;
        pub struct DirectTerminalInstallationProof<'a, 'b>(&'a &'b ());
        impl DirectTerminalInstallationProof<'_, '_> {
            pub fn require_section(&self, _: &crate::cluster::EmbeddedPublicationSection<'_>) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
        }
        pub struct DirectTerminalInstallation<'a, 'b>(&'a &'b ());
        impl DirectTerminalInstallation<'_, '_> {
            pub fn deadline(&self) -> Instant { Instant::now() }
            pub fn require_section(&self, _: &crate::cluster::EmbeddedPublicationSection<'_>) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
            pub fn execute_in_store(self, _: &PgStore, _: &engine::EmbeddedPublicationAuthority<'_>, _: &CompletePublicationRequest<'_>) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
        }
        pub struct DirectTerminalPreparation<'a, 'b>(&'a &'b ());
        impl DirectTerminalPreparation<'_, '_> {
            pub fn require_section(&self, _: &cluster::EmbeddedPublicationSection<'_>) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
            pub fn deadline(&self) -> Instant { Instant::now() }
            pub fn publication_request(&self) -> Result<CompletePublicationRequest<'_>, StagingWindowConfirmationError> { unimplemented!() }
            pub fn capture_in_store(&self, _: &PgStore, _: &engine::EmbeddedPublicationAuthority<'_>, _: &CompletePublicationRequest<'_>) -> Result<CapturedDirectTerminal, StagingWindowConfirmationError> { unimplemented!() }
        }
        pub struct DirectStagingPreparation<'a, 'b>(&'a &'b ());
        pub struct CompletePendingEvidence;
        impl DirectStagingPreparation<'_, '_> {
            pub fn capture_contender_in_store(&self, _: &PgStore, _: &engine::EmbeddedPublicationAuthority<'_>, _: &CompletePublicationRequest<'_>) -> Result<CompletePendingEvidence, StagingWindowConfirmationError> { unimplemented!() }
            pub fn require_section(&self, _: &cluster::EmbeddedPublicationSection<'_>) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
            pub fn deadline(&self) -> Instant { Instant::now() }
            pub fn publication_request(&self) -> Result<CompletePublicationRequest<'_>, StagingWindowConfirmationError> { unimplemented!() }
            pub fn capture_in_store(&self, _: &PgStore, _: &engine::EmbeddedPublicationAuthority<'_>, _: &CompletePublicationRequest<'_>) -> Result<DirectStagingSnapshot, StagingWindowConfirmationError> { unimplemented!() }
        }
        pub struct StreamAppendInstallationProof<'a, 'b>(&'a &'b ());
        impl StreamAppendInstallationProof<'_, '_> {
            pub fn require_section(&self, _: &cluster::EmbeddedPublicationSection<'_>) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
        }
        pub struct StreamAppendInstallation<'a, 'b>(&'a &'b ());
        impl StreamAppendInstallation<'_, '_> {
            pub fn require_section(&self, _: &cluster::EmbeddedPublicationSection<'_>) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
            pub fn deadline(&self) -> Instant { Instant::now() }
            pub fn execute_in_store(self, _: &PgStore, _: &engine::EmbeddedPublicationAuthority<'_>, _: &CompletePublicationRequest<'_>) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
        }
        pub struct StreamAppendPreparation<'a, 'b>(&'a &'b ());
        impl StreamAppendPreparation<'_, '_> {
            pub fn require_section(&self, _: &cluster::EmbeddedPublicationSection<'_>) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
            pub fn deadline(&self) -> Instant { Instant::now() }
            pub fn publication_request(&self) -> Result<CompletePublicationRequest<'_>, StagingWindowConfirmationError> { unimplemented!() }
            pub fn prepare_in_store(&self, _: &PgStore, _: &engine::EmbeddedPublicationAuthority<'_>, _: &CompletePublicationRequest<'_>, _: payload_placement::PayloadPlacementInput<'_>) -> Result<CapturedStreamAppend, StagingWindowConfirmationError> { unimplemented!() }
        }
        impl StreamWindowAllocationPermit<'_, '_> {
            pub fn require_section(&self, _: &cluster::EmbeddedPublicationSection<'_>) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
            pub fn deadline(&self) -> Instant { Instant::now() }
            pub fn publication_request(&self) -> Result<CompletePublicationRequest<'_>, StagingWindowConfirmationError> { unimplemented!() }
        }
        impl PgStore {
            pub fn allocate_confirmed_stream_window(&self, _: &StreamWindowAllocationPermit<'_, '_>, _: &engine::EmbeddedPublicationAuthority<'_>, _: &CompletePublicationRequest<'_>, _: payload_placement::PayloadPlacementInput<'_>) -> Result<crate::StreamUploadSegmentRecord, StagingWindowConfirmationError> { unimplemented!() }
        }
        impl PgStore { pub fn pg_id(&self) -> u32 { 0 } }
        pub struct CompletePublicationError;
        pub struct AbandonedPartCustodyObservation;
        pub type AbandonedPartCustodyError = CompletePublicationError;
        pub struct AbandonedPartPublication<'a, 'b>(&'a &'b ());
        impl AbandonedPartPublication<'_, '_> {
            pub fn inspect_custody_in_store(&self, _: &PgStore, _: &engine::EmbeddedPublicationAuthority<'_>, _: &CompletePublicationRequest<'_>) -> Result<AbandonedPartCustodyObservation, AbandonedPartCustodyError> { unimplemented!() }
            pub fn require_embedded_section(&self, _: &cluster::EmbeddedPublicationSection<'_>) -> Result<(), StoreError> { Ok(()) }
            pub fn publish_in_store(&self, _: &PgStore, _: &engine::EmbeddedPublicationAuthority<'_>, _: &CompletePublicationRequest<'_>) -> Result<(), CompletePublicationError> { Ok(()) }
        }
        pub struct AbandonedPartInstallation<'a, 'b>(&'a &'b ());
        impl AbandonedPartInstallation<'_, '_> {
            pub fn require_embedded_section(&self, _: &cluster::EmbeddedPublicationSection<'_>) -> Result<(), StoreError> { Ok(()) }
            pub fn require_request(&self, _: &CompletePublicationRequest<'_>) -> Result<(), StoreError> { Ok(()) }
            pub fn execute_in_store(self, _: &PgStore, _: &engine::EmbeddedPublicationAuthority<'_>, _: &CompletePublicationRequest<'_>) -> Result<(), CompletePublicationError> { Ok(()) }
        }
        pub type AbandonedPartPreparationError = CompletePublicationError;
        pub struct AbandonedPartPreparation<'a, 'b>(&'a &'b ());
        pub struct PreparedAbandonedPartBorrow<'a>(&'a ());
        impl AbandonedPartPreparation<'_, '_> {
            pub fn require_embedded_section(&self, _: &cluster::EmbeddedPublicationSection<'_>) -> Result<(), StoreError> { Ok(()) }
            pub fn prepare_in_store<'a>(&'a self, _: &PgStore, _: &engine::EmbeddedPublicationAuthority<'_>, _: &CompletePublicationRequest<'_>) -> Result<PreparedAbandonedPartBorrow<'a>, CompletePublicationError> { unimplemented!() }
        }
        impl From<StoreError> for CompletePublicationError { fn from(_: StoreError) -> Self { Self } }
        pub struct CompletePublicationRequest<'a>(&'a ());
        pub struct TerminalSlotRetirementAuthority<'a>(&'a ());
        pub struct UnstartedAbandonmentAuthority<'a, 'b>(&'a &'b ());
        impl UnstartedAbandonmentAuthority<'_, '_> {
            pub fn require_embedded_section(&self, _: &cluster::EmbeddedPublicationSection<'_>) -> Result<(), StoreError> { Ok(()) }
        }
        impl TerminalSlotRetirementAuthority<'_> {
            pub fn require_embedded_section(&self, _: &cluster::EmbeddedPublicationSection<'_>) -> Result<(), StoreError> { Ok(()) }
        }
        impl CompletePublicationRequest<'_> {
            pub fn abandoned_part_cleanup_request<'a>(_: &'a AbandonedPartPublication<'_, '_>) -> Result<CompletePublicationRequest<'a>, StoreError> { Ok(CompletePublicationRequest(&())) }
            pub fn bound_deadline(&self, deadline: Instant) -> Instant { deadline }
            pub fn id(&self) -> MetadataCommandId { MetadataCommandId }
            pub fn node_id(&self) -> NodeId { NodeId }
            pub fn topology(&self) -> &PgTopology { &PgTopology }
            pub fn command_sha256(&self) -> [u8; 32] { [0; 32] }
            pub fn checksum_crc64(&self) -> u64 { 0 }
            pub fn start_embedded(&self, _: &PgStore, _: &engine::EmbeddedPublicationAuthority<'_>) -> Result<(), CompletePublicationError> { Ok(()) }
            pub fn abandon_unstarted_embedded(&self, _: &PgStore, _: &engine::EmbeddedPublicationAuthority<'_>, _: &UnstartedAbandonmentAuthority<'_, '_>) -> Result<(), CompletePublicationError> { Ok(()) }
            pub fn retire_embedded(&self, _: &PgStore, _: &engine::EmbeddedPublicationAuthority<'_>, _: &TerminalSlotRetirementAuthority<'_>) -> Result<(), CompletePublicationError> { Ok(()) }
        }
    }
    pub mod engine {
        use super::*;
        use pg_store::PgStore;
        pub struct Node;
        impl Node {
            fn get_pg_until(&self, _: u32, _: Instant) -> Result<PgStore, StoreError> { Ok(PgStore) }
            fn write_shard_file_if_absent_with_effect_fence(&self, _: u32, _: &ShardKey, _: &[u8], _: ()) -> Result<WriteAck, StoreError> { Ok(WriteAck) }
        }
        pub struct LocalNodeRuntime { node: Node }
        impl LocalNodeRuntime { pub fn pg_topology(&self) -> &PgTopology { &PgTopology } }
        #[path = "OWNER_PATH"]
        mod owner;
        pub(in crate::node_runtime) use owner::{EmbeddedPublicationAuthority, EmbeddedStagingAckAuthority};
    }
    mod sibling {
        use super::*;
        use engine::EmbeddedPublicationAuthority;
        use cluster::EmbeddedPublicationSection;
        use pg_store::{PgStore, CompletePublicationRequest};
        fn probe<'a>(section: &'a EmbeddedPublicationSection<'a>, pg: &'a PgStore, request: &'a CompletePublicationRequest<'a>, authority: &EmbeddedPublicationAuthority<'a>) {
            PROBE
        }
    }
}
