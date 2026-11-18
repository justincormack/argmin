// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0
#![allow(dead_code, unused_imports)]
extern crate self as checksum;
pub mod crc64 { pub fn checksum(_: &[u8]) -> u64 { 0 } }
pub use types::CommittedObjectEncryption;
pub type GenerationId = u64;
#[derive(Clone)] pub struct DirectPutCommitSnapshot;
#[derive(Clone)] pub struct StreamPutCommitInput { pub encryption: ObjectEncryption }
pub struct ObjectEncryptionSubject;
pub struct SegmentEncryptionSubject(ObjectEncryptionSubject);
impl SegmentEncryptionSubject {
    pub fn standard_object(subject: ObjectEncryptionSubject, _: u32, _: u64) -> Self { Self(subject) }
    pub fn object_subject(&self) -> &ObjectEncryptionSubject { &self.0 }
    pub fn logical_plaintext_len(&self) -> u64 { 4 }
}
#[derive(Clone)] pub struct DirectPutCommitStorageSnapshot { pub auth_snapshot: DirectPutCommitSnapshot }
mod node_runtime {
    pub mod payload_placement { pub struct PayloadPlacementInput<'a>(pub &'a ()); }
    pub mod engine {
        use super::pg_store::*;
        pub struct LocalNodeRuntime;
        impl LocalNodeRuntime {
            pub fn persist_staging_acknowledgements(&self, _: &StagingAcknowledgementBatch<'_, '_>) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
            pub fn write_stream_staging_file(&self, _: &StreamStagingFileWriter<'_>, _: u8, _: &[u8]) -> Result<crate::WriteAck, StagingWindowConfirmationError> { unimplemented!() }
            pub fn pg_topology(&self) -> &() { &() }
            pub fn write_direct_staging_file(&self, _: &DirectStagingFileWriter<'_>, _: u8, _: u64, _: &[u8]) -> Result<crate::WriteAck, StagingWindowConfirmationError> { unimplemented!() }
        }
        pub struct EmbeddedPublicationAuthority<'a>(pub &'a ());
        pub struct EmbeddedStagingAckAuthority<'a, 'b, 'c>(pub &'a &'b &'c ());
        impl EmbeddedStagingAckAuthority<'_, '_, '_> {
            pub fn require(&self, _: &StagingAcknowledgementBatch<'_, '_>, _: &PgStore) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
        }
        impl EmbeddedPublicationAuthority<'_> {
            pub fn direct_reinspection_bounds(&self, _: &DirectStagingPreparation<'_, '_>) -> Result<(std::time::Instant, crate::types::AdmittedRouteEffectFence), StagingWindowConfirmationError> { unimplemented!() }
            pub fn require_direct_terminal(&self, _: &DirectTerminalPreparation<'_, '_>) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
            pub fn require_direct_preparation(&self, _: &DirectStagingPreparation<'_, '_>) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
            pub fn require_stream_append_preparation(&self, _: &StreamAppendPreparation<'_, '_>) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
            pub fn require_stream_window_allocation(&self, _: &StreamWindowAllocationPermit<'_, '_>) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
            pub fn require_effect(&self, _: &PgStore, _: &CompletePublicationRequest<'_>) -> Result<(), StoreError> { Ok(()) }
        }
    }
    pub mod pg_store {
        use std::time::Instant;
        type NodeId = u32;
        type MetadataCommandReplicaState = ();
        #[derive(PartialEq)] pub enum PublicationObservation { NoAttemptObserved, Unconfirmed, MarkerConfirmed }
        #[derive(PartialEq)] pub enum ApplicationObservation { NoAttemptObserved, Unconfirmed, Applied }
        pub struct FanoutError;
        impl std::fmt::Display for FanoutError { fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str("fanout error") } }
        impl From<StoreError> for FanoutError { fn from(_: StoreError) -> Self { Self } }
        impl From<StagingWindowConfirmationError> for FanoutError { fn from(_: StagingWindowConfirmationError) -> Self { Self } }
        fn require_cohort_application_order(_: &Route, _: &Sender, _: u32) -> Result<(), FanoutError> { Ok(()) }
        fn require_cohort_completion(_: &Route, _: &Sender) -> Result<(), FanoutError> { Ok(()) }
        fn terminal_work_for_handoff(_: &Route, _: &Sender) -> Result<((), ()), FanoutError> { Ok(((), ())) }
        #[derive(PartialEq)] pub struct MetadataCommandId;
        impl MetadataCommandId { pub fn pg_id(&self) -> u32 { 0 } pub fn cluster_epoch(&self) -> u64 { 1 } }
        pub struct PgStore;
        pub struct CompletePendingEvidence { pending: Option<(Context, bool)>, publication_deadline: PublicationDeadline }
        impl CompletePendingEvidence {
            pub fn id(&self) -> Option<MetadataCommandId> { None }
            pub fn publication_started(&self) -> bool { false }
            pub fn into_publication_sender(self, _: NodeId, _: Instant) -> CompletePublicationSender { Sender { source: PublicationSource::Inspected(self) } }
        }
        pub struct CompletePendingRequest;
        impl CompletePendingRequest {
            pub fn new(_: u32, _: u64, _: &()) -> Result<Self, String> { Ok(Self) }
            pub fn read_current_slot(&self, _: &PgStore, _: u32) -> Result<CompletePendingEvidence, String> { unimplemented!() }
        }
        pub struct Snapshot;
        impl Snapshot { pub fn finish(self) -> Result<(), String> { Ok(()) } }
        impl PgStore {
            pub(in crate::node_runtime::pg_store) fn validate_complete_direct_reinspection_prefix(&self, _: NodeId, _: u64, _: &(), _: Instant, _: crate::types::AdmittedRouteEffectFence) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
            pub fn command_build_snapshot(&self) -> Result<Snapshot, String> { Ok(Snapshot) }
            pub fn require_live_allocation_lifetime(&self) -> Result<(), StoreError> { Ok(()) }
        }
        impl PgStore { pub fn register_written_shards_batch_exact_with_effect_fence(&self, _: &[(&crate::ShardKey, crate::WriteAck)], _: crate::types::AdmittedRouteEffectFence) -> Result<(), StoreError> { Ok(()) } }
        pub struct CompletePublicationRequest<'a>(&'a ());
        impl CompletePublicationRequest<'_> {
            pub fn id(&self) -> MetadataCommandId { MetadataCommandId }
            pub fn command_sha256(&self) -> [u8;32] { [0;32] }
            pub fn node_id(&self) -> u32 { 0 }
            pub fn topology(&self) -> &() { &() }
        }
        #[derive(Debug)]
        pub enum StoreError {
            OperationDeadlineExceeded { context: &'static str },
            RouteCapabilitySubjectMismatch { operation: &'static str },
        }
        #[derive(Debug)] pub enum StagingWindowConfirmationError { Window(String) }
        impl From<StoreError> for StagingWindowConfirmationError { fn from(_: StoreError) -> Self { Self::Window(String::new()) } }
        pub struct StagingWindowConfirmationObservation;
        pub struct StagingWindowInspectionDescription;
        impl StagingWindowInspectionDescription {
            pub fn decode(_: &[u8]) -> Result<Self, StagingWindowConfirmationError> { Ok(Self) }
            pub fn request(&self, _: &()) -> Result<&Self, StagingWindowConfirmationError> { Ok(self) }
            pub fn require_allocated_segment(&self, _: &crate::PrepareStreamUploadSegmentAppendReq, _: &crate::StreamUploadSegmentRecord) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
        }
        pub mod payload_staging {
            use super::*;
            pub struct DirectIntroductionWindowScope<'a, 'section, 'route>(std::marker::PhantomData<(&'a (), &'section (), &'route ())>);
            pub struct RemoteIntroductionWindowScope<'a, 'route>(std::marker::PhantomData<(&'a (), &'route ())>);
            impl<'a, 'route> RemoteIntroductionWindowScope<'a, 'route> {
                pub(in crate::node_runtime::pg_store) fn into_parts(self) -> (&'a CompletePublicationSender, &'a Route, &'a UnixStorageNodeClient, &'a UnixStorageNodeMetadataCommandSession, Instant) { unimplemented!() }
            }
            pub struct DirectIntroductionFileScope<'section, 'route>(std::marker::PhantomData<(&'section (), &'route ())>);
            pub struct RemoteIntroductionFileScope<'route>(std::marker::PhantomData<&'route ()>);
            impl<'route> RemoteIntroductionFileScope<'route> {
                pub(in crate::node_runtime::pg_store) fn into_parts(self) -> (Route, CompletePublicationSender, CapturedWindowRead, &'route UnixStorageNodeClient, &'route UnixStorageNodeMetadataCommandSession) { unimplemented!() }
            }
            impl<'section, 'route> DirectIntroductionFileScope<'section, 'route> {
                pub(in crate::node_runtime::pg_store) fn into_parts(self) -> (Route, CompletePublicationSender, CapturedWindowRead, &'section EmbeddedPublicationSection<'route>, Result<(), StagingWindowConfirmationError>) { unimplemented!() }
            }
            impl<'a, 'section, 'route> DirectIntroductionWindowScope<'a, 'section, 'route> {
                pub(in crate::node_runtime::pg_store) fn into_parts(self) -> (&'a CompletePublicationSender, &'section EmbeddedPublicationSection<'route>, Instant) { unimplemented!() }
            }
            pub struct CapturedDirectTerminal;
            impl CapturedDirectTerminal {
                pub fn remote_preparation_digest(&self) -> Result<[u8;32], StagingWindowConfirmationError> { Ok([0;32]) }
                pub fn install_remote(&mut self, _: &DirectTerminalInstallationProof<'_, '_>, _: &mut CompletePrimarySessionBorrow<'_>) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
                pub fn into_publication_sender(self, _: u32, _: Instant) -> CompletePublicationSender { unimplemented!() }
                pub fn digest(&self) -> Result<[u8; 32], StagingWindowConfirmationError> { Ok([0; 32]) }
                pub fn installation(&self) -> DirectTerminalInstallationObservation { DirectTerminalInstallationObservation::NoAcceptanceObserved }
                pub fn install_in_store(&mut self, _: &DirectTerminalInstallationProof<'_, '_>, _: &PgStore, _: &super::super::engine::EmbeddedPublicationAuthority<'_>, _: &CompletePublicationRequest<'_>) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
                pub fn id(&self) -> MetadataCommandId { MetadataCommandId }
                pub fn capture(_: &DirectTerminalPreparation<'_, '_>, _: &PgStore, _: &super::super::engine::EmbeddedPublicationAuthority<'_>, _: &CompletePublicationRequest<'_>) -> Result<Self, StagingWindowConfirmationError> { unimplemented!() }
            }
            pub struct DirectStagingManifest;
            impl DirectStagingManifest { pub fn crc64(&self) -> u64 { 0 } }
            pub struct ContextualCommand;
            pub struct CapturedStreamAppend;
            impl CapturedStreamAppend {
                pub fn install_remote(&mut self, _: &StreamAppendInstallationProof<'_, '_>, _: &mut CompletePrimarySessionBorrow<'_>) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
                pub fn into_publication_sender(self, _: u32, _: Instant) -> CompletePublicationSender { unimplemented!() }
                pub fn id(&self) -> MetadataCommandId { MetadataCommandId }
                pub fn digest(&self) -> Result<[u8;32], StagingWindowConfirmationError> { Ok([0;32]) }
                pub fn installation(&self) -> StreamAppendInstallationObservation { StreamAppendInstallationObservation::NoAcceptanceObserved }
                pub fn install_in_store(&mut self, _: &StreamAppendInstallationProof<'_, '_>, _: &PgStore, _: &super::super::engine::EmbeddedPublicationAuthority<'_>, _: &CompletePublicationRequest<'_>) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
            }
            pub fn capture_acknowledged_append(_: &StreamAppendPreparation<'_, '_>, _: &PgStore, _: &super::super::engine::EmbeddedPublicationAuthority<'_>, _: &CompletePublicationRequest<'_>, _: super::super::payload_placement::PayloadPlacementInput<'_>) -> Result<CapturedStreamAppend, StagingWindowConfirmationError> { unimplemented!() }
            pub struct CapturedWindowRead;
            pub struct StagingAcknowledgementRecords { pub pg: crate::PgId, pub primary: crate::NodeId, pub epoch: crate::ClusterEpoch, pub rows: Vec<(crate::ShardKey, crate::WriteAck)> }
            impl CapturedWindowRead {
                pub fn reinspect_direct_prefix_embedded(&mut self, _: &EmbeddedPublicationSection<'_>, _: &(), _: Instant) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
                pub fn direct_encoding_geometry(&self, _: &(), _: u64) -> Result<([u8;16], crate::GenerationId, crate::EcShape), StagingWindowConfirmationError> { unimplemented!() }
                pub fn direct_ciphertext_size(&self, _: &(), _: u64) -> Result<u64, StagingWindowConfirmationError> { unimplemented!() }
                pub fn require_direct_terminal_response(&self, _: &(), _: &DirectStagingManifest, _: &crate::StreamPutCommitInput, _: &crate::DirectPutCommitStorageSnapshot, _: &crate::metadata_command::CommitDirectPutObjectCommand) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
                pub fn require_direct_snapshot_response(&self, _: &(), _: &UnixStorageNodeClient, _: &crate::DirectPutCommitStorageSnapshot) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
                pub fn require_direct_terminal_snapshot(&self, _: &PgStore, _: u32, _: &(), _: &crate::DirectPutCommitStorageSnapshot) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
                pub fn direct_encryption_subject(&self, _: &(), _: &crate::ObjectEncryption) -> Result<crate::ObjectEncryptionSubject, StagingWindowConfirmationError> { unimplemented!() }
                pub fn direct_terminal_command(&self, _: &PgStore, _: &CompletePublicationRequest<'_>, _: &DirectStagingManifest, _: &crate::StreamPutCommitInput, _: &crate::DirectPutCommitStorageSnapshot) -> Result<crate::metadata_command::CommitDirectPutObjectCommand, StagingWindowConfirmationError> { unimplemented!() }
                pub fn direct_manifest(&self, _: &(), _: u64, _: u64, _: &std::collections::BTreeMap<u8, crate::WriteAck>) -> Result<DirectStagingManifest, StagingWindowConfirmationError> { unimplemented!() }
                pub fn direct_commit_snapshot(&self, _: &PgStore, _: u32, _: &()) -> Result<crate::DirectPutCommitStorageSnapshot, StagingWindowConfirmationError> { unimplemented!() }
                pub fn require_append_response(&self, _: &ContextualCommand, _: &crate::metadata_command::AppendStreamSegmentCommand, _: &(), _: super::super::payload_placement::PayloadPlacementInput<'_>, _: u64) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
                pub fn file_descriptor(&self, _: &()) -> Result<Vec<u8>, StagingWindowConfirmationError> { Ok(Vec::new()) }
                pub fn require_remote_session(&self, _: &CompletePrimarySessionBorrow<'_>) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
                pub fn inspect_direct_files_remote(&self, _: &mut CompletePrimarySessionBorrow<'_>, _: u32, _: u32, _: &()) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
                pub fn acknowledgement_batch(&self, _: &(), _: u64, _: &std::collections::BTreeMap<u8, crate::WriteAck>) -> Result<StagingAcknowledgementRecords, StagingWindowConfirmationError> { unimplemented!() }
                pub fn require_stream_files(&self, _: &PgStore, _: u32, _: &(), _: &crate::StreamUploadSegmentRecord, _: &crate::types::ExpectedStreamAppendState) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
                pub fn stream_file_location(&self, _: &(), _: super::super::payload_placement::PayloadPlacementInput<'_>, _: &crate::StreamUploadSegmentRecord, _: u8, _: usize) -> Result<(crate::cluster::ShardLocation, crate::ShardKey), StagingWindowConfirmationError> { unimplemented!() }
                pub fn require_direct_files(&self, _: &PgStore, _: u32, _: &()) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
                pub fn file_effect_fence(&self) -> Result<crate::types::AdmittedRouteEffectFence, StagingWindowConfirmationError> { Ok(crate::types::AdmittedRouteEffectFence) }
                pub fn direct_file_location(&self, _: &(), _: super::super::payload_placement::PayloadPlacementInput<'_>, _: u8, _: u64, _: usize) -> Result<(crate::cluster::ShardLocation, crate::ShardKey), StagingWindowConfirmationError> { unimplemented!() }
                pub fn deadline(&self) -> Instant { Instant::now() }
                pub fn effect_fence(&self) -> crate::types::AdmittedRouteEffectFence { crate::types::AdmittedRouteEffectFence }
                pub fn require_store_prefix(&self, _: &PgStore, _: u32, _: &()) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
                pub fn require_valid(&self) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
                pub fn observation(&self) -> &StagingWindowConfirmationObservation { &StagingWindowConfirmationObservation }
            }
            pub fn prepare_confirmed_stream_window(_: &PgStore, _: super::super::payload_placement::PayloadPlacementInput<'_>, _: &StreamWindowAllocationPermit<'_, '_>, _: &super::super::engine::EmbeddedPublicationAuthority<'_>, _: &CompletePublicationRequest<'_>) -> Result<crate::StreamUploadSegmentRecord, StagingWindowConfirmationError> { unimplemented!() }
        }
        pub struct EmbeddedPublicationSection<'a>(&'a ());
        impl EmbeddedPublicationSection<'_> { pub fn deadline(&self) -> Instant { Instant::now() } }
        impl EmbeddedPublicationSection<'_> {
            pub fn introduction_identity(&self) -> crate::cluster::EmbeddedPublicationSectionIdentity { unimplemented!() }
            pub fn install_direct_terminal(&self, _: &CompletePublicationRequest<'_>, _: DirectTerminalInstallation<'_, '_>) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
            pub fn prepare_direct_terminal(&self, _: &DirectTerminalPreparation<'_, '_>) -> Result<payload_staging::CapturedDirectTerminal, StagingWindowConfirmationError> { unimplemented!() }
            pub fn inspect_direct_contender(&self, _: &DirectStagingPreparation<'_, '_>) -> Result<CompletePendingEvidence, StagingWindowConfirmationError> { unimplemented!() }
            pub fn prepare_acknowledged_direct_payload(&self, _: &DirectStagingPreparation<'_, '_>) -> Result<DirectStagingSnapshot, StagingWindowConfirmationError> { unimplemented!() }
            pub fn application_target(&self, _: u32) -> Result<(), StoreError> { Ok(()) }
            pub fn install_acknowledged_stream_append(&self, _: &CompletePublicationRequest<'_>, _: StreamAppendInstallation<'_, '_>) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
            pub fn prepare_acknowledged_stream_append(&self, _: &StreamAppendPreparation<'_, '_>) -> Result<payload_staging::CapturedStreamAppend, StagingWindowConfirmationError> { unimplemented!() }
            pub fn require_stream_file_window(&self, _: &AllocatedStreamWindow<'_>) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
            pub fn require_direct_file_window(&self, _: &ConfirmedStagingWindow<'_>) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
            pub fn runtime(&self) -> Result<&crate::node::LocalNodeRuntime, StoreError> { unimplemented!() }
            pub fn require_effect(&self) -> Result<(), StoreError> { Ok(()) }
            pub fn node_id(&self) -> u32 { 0 }
        }
        impl EmbeddedPublicationSection<'_> { pub fn allocate_confirmed_stream_window(&self, _: &StreamWindowAllocationPermit<'_, '_>) -> Result<crate::StreamUploadSegmentRecord, StagingWindowConfirmationError> { unimplemented!() } }
        pub struct UnixStorageNodeClient;
        impl UnixStorageNodeClient {
            pub fn node_id(&self) -> u32 { 0 }
            pub fn send_staging_acknowledgements(&self, _: &RemoteStagingAcknowledgementAttempt<'_, '_>) -> Result<(), StagingWindowConfirmationError> { unimplemented!() }
            pub fn send_direct_staging_file(&self, _: &RemoteDirectFileAttempt<'_, '_>) -> Result<crate::WriteAck, StagingWindowConfirmationError> { unimplemented!() }
            pub fn send_stream_staging_file(&self, _: &RemoteStreamFileAttempt<'_, '_>) -> Result<crate::WriteAck, StagingWindowConfirmationError> { unimplemented!() }
        }
        pub struct UnixStorageNodeMetadataCommandSession;
        pub struct MetadataCommandSessionIdentity;
        pub struct CompletePrimarySessionBorrow<'a>(&'a ());
        impl CompletePrimarySessionBorrow<'_> {
            pub fn prepare_direct_terminal(&mut self, _: &UnixStorageNodeClient, _: &DirectTerminalPreparation<'_, '_>) -> Result<payload_staging::CapturedDirectTerminal, StagingWindowConfirmationError> { unimplemented!() }
            pub fn prepare_direct_snapshot(&mut self, _: &UnixStorageNodeClient, _: &DirectStagingPreparation<'_, '_>) -> Result<DirectStagingSnapshot, StagingWindowConfirmationError> { unimplemented!() }
            pub fn require_identity(&self, _: &MetadataCommandSessionIdentity) -> Result<(), StoreError> { Ok(()) }
            pub fn prepare_stream_append(&mut self, _: &StreamAppendPreparation<'_, '_>) -> Result<payload_staging::CapturedStreamAppend, StagingWindowConfirmationError> { unimplemented!() }
            pub fn release_until(self) -> Result<(), StoreError> { Ok(()) }
            pub fn inspect_stream_file_primary(&mut self, _: &[u8]) -> Result<(), StoreError> { Ok(()) }
        }
        impl UnixStorageNodeMetadataCommandSession {
            pub fn require_identity(&self, _: &MetadataCommandSessionIdentity) -> Result<(), StoreError> { Ok(()) }
            pub fn require_origin(&self, _: &UnixStorageNodeClient) -> Result<(), StoreError> { Ok(()) }
            pub fn identity(&self) -> MetadataCommandSessionIdentity { MetadataCommandSessionIdentity }
            pub fn borrow_complete_primary(&self, _: &UnixStorageNodeClient, _: u32, _: u64, _: Instant) -> Result<CompletePrimarySessionBorrow<'_>, StoreError> { unimplemented!() }
            pub fn send_stream_allocation(&self, _: &UnixStorageNodeClient, _: RemoteStreamAllocationAttempt<'_, '_>) -> Result<crate::StreamUploadSegmentRecord, StagingWindowConfirmationError> { unimplemented!() }
        }
        pub struct Route;
        pub struct Encoder;
        impl Encoder {
            pub fn encode<E: From<StoreError>>(&self, _: &[u8;16], _: crate::GenerationId, _: &[u8], _: crate::EcShape, _: impl FnOnce(&[(crate::ShardKey, &[u8])]) -> Result<Vec<(crate::ShardKey, crate::WriteAck)>, E>) -> Result<(), E> { unimplemented!() }
        }
        impl Route {
            pub fn retain_current(&self) -> Result<Self, StoreError> { Ok(Self) }
            pub fn begin_direct_put_terminal_work(&self, _: Instant, _: crate::storage_rpc::StorageRpcOperationDeadline) -> crate::cluster::DirectPutTerminalWork { unimplemented!() }
            pub fn staging_payload_encoder(&self) -> Result<Encoder, StoreError> { Ok(Encoder) }
            pub fn embedded_replica_target_in_session(&self, _: &UnixStorageNodeClient, _: &UnixStorageNodeMetadataCommandSession, _: &MetadataCommandSessionIdentity, _: u32, _: Instant) -> Result<(), StoreError> { Ok(()) }
            pub fn require_client(&self, _: &UnixStorageNodeClient) -> Result<(), StoreError> { Ok(()) }
            pub fn require_staging_ack_client(&self, _: crate::PgId, _: crate::NodeId, _: &UnixStorageNodeClient) -> Result<(), StoreError> { Ok(()) }
            pub fn require_staging_file_client(&self, _: &crate::cluster::ShardLocation, _: &UnixStorageNodeClient) -> Result<(), StoreError> { Ok(()) }
            pub fn embedded_staging_ack_runtime(&self, _: crate::PgId, _: crate::NodeId, _: crate::ClusterEpoch) -> Result<&crate::node::LocalNodeRuntime, StoreError> { unimplemented!() }
            pub fn epoch(&self) -> u64 { 1 }
            pub fn topology(&self) -> &() { &() }
            pub fn staging_file_placement(&self) -> Result<(super::payload_placement::PayloadPlacementInput<'_>, crate::types::AdmittedRouteEffectFence), StoreError> { unimplemented!() }
            pub fn embedded_staging_file_runtime(&self, _: &crate::cluster::ShardLocation) -> Result<&crate::node::LocalNodeRuntime, StoreError> { unimplemented!() }
            pub fn primary(&self) -> u32 { 0 }
            pub fn nodes(&self) -> impl Iterator<Item = u32> { [0, 1, 2].into_iter() }
            pub fn require_embedded_section(&self, _: &EmbeddedPublicationSection<'_>) -> Result<(), StoreError> { Ok(()) }
            pub fn window_confirmation_deadline(&self, _: &UnixStorageNodeClient, d: Instant) -> Result<Instant, StoreError> { Ok(d) }
            pub fn is_historical(&self) -> bool { false }
        }
        pub struct Context;
        trait Quiescence { fn is_quiescent(&self) -> bool; }
        impl Quiescence for () { fn is_quiescent(&self) -> bool { true } }
        impl Context {
            pub fn terminal_work(&self) {}
            pub fn terminal_slot_work(&self) {}
            pub fn encode_stream_allocation(&self, _: u32, _: &payload_staging::CapturedWindowRead, _: &crate::PrepareStreamUploadSegmentAppendReq, _: &crate::types::ExpectedStreamAppendState) -> Result<Vec<u8>, StagingWindowConfirmationError> { unimplemented!() }
            pub fn require_stream_allocation_window(&self, _: &PgStore, _: &crate::PrepareStreamUploadSegmentAppendReq) -> Result<(), StagingWindowConfirmationError> { Ok(()) }
            pub fn capture_window_read_embedded(&self, _: &EmbeddedPublicationSection<'_>, _: Instant) -> Result<payload_staging::CapturedWindowRead, StagingWindowConfirmationError> { unimplemented!() }
            pub fn capture_window_read_remote(&self, _: &Route, _: &UnixStorageNodeClient, _: &UnixStorageNodeMetadataCommandSession, _: Instant) -> Result<payload_staging::CapturedWindowRead, StagingWindowConfirmationError> { unimplemented!() }
        }
        type Inspected = CompletePendingEvidence;
        pub enum PublicationSource { Inspected(Inspected), Submitted { evidence: Context, deadline: PublicationDeadline } }
        impl PublicationSource { pub fn request(&self, _: u32) -> Result<CompletePublicationRequest<'_>, String> { unimplemented!() } }
        pub struct Sender { source: PublicationSource }
        type CompletePublicationSender = Sender;
        impl Sender {
            pub fn require_cohort_subject(&self, _: &Route) -> Result<(), StoreError> { Ok(()) }
            pub fn apply_embedded_in_cohort(&mut self, _: &Route, _: &EmbeddedPublicationSection<'_>, _: NodeId, _: Instant) -> Result<(), FanoutError> { Ok(()) }
            pub fn id(&self) -> Option<MetadataCommandId> { Some(MetadataCommandId) }
            pub fn observation(&self) -> PublicationObservation { PublicationObservation::NoAttemptObserved }
            pub fn application_observation(&self, _: u32) -> ApplicationObservation { ApplicationObservation::NoAttemptObserved }
            pub fn send_embedded(&mut self, _: &EmbeddedPublicationSection<'_>, _: Instant) -> Result<(), FanoutError> { Ok(()) }
            pub fn send_remote_held(&mut self, _: &mut crate::node_client::CompletePrimarySessionBorrow<'_>, _: Instant) -> Result<(), FanoutError> { Ok(()) }
            pub fn apply_remote_held(&mut self, _: &mut crate::node_client::CompletePrimarySessionBorrow<'_>, _: Instant) -> Result<(), FanoutError> { Ok(()) }
            pub fn apply_remote_in_fanout(&mut self, _: &UnixStorageNodeClient, _: &Route, _: Instant) -> Result<(), FanoutError> { Ok(()) }
            pub fn apply_embedded(&mut self, _: &(), _: Instant) -> Result<(), FanoutError> { Ok(()) }
        }
        pub struct Owner { route: Route, sender: Sender }
        type CompleteFanoutSender<'a> = Owner;
        pub struct PublicationDeadline(std::sync::OnceLock<Cutoff>);
        pub struct Cutoff { local: Instant, portable: Portable }
        type Portable = crate::storage_rpc::StorageRpcOperationDeadline;
        type CapturedPublicationDeadline = Cutoff;
        impl Owner {
            pub fn open_primary_session(&self, _: &UnixStorageNodeClient, _: Instant) -> Result<UnixStorageNodeMetadataCommandSession, StoreError> { unimplemented!() }
            pub fn work_deadline(&self) -> Instant { Instant::now() }
            pub fn captured_deadline(&self) -> Cutoff { Cutoff { local: Instant::now(), portable: Portable { portable_wall_valid_until_ms: 0 } } }
            pub fn require_primary(&self, _: &UnixStorageNodeClient, _: &UnixStorageNodeMetadataCommandSession) -> Result<(), StoreError> { Ok(()) }
        }
        #[derive(PartialEq)] pub enum TerminalSlotRetirementObservation { Retired }
        pub enum CompleteRetirementSendError { NotSent(StoreError) }
        impl std::fmt::Display for CompleteRetirementSendError { fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str("retirement error") } }
        pub struct CompleteTerminalHandoff<'a> { owner: Owner, retirement: TerminalSlotRetirementObservation, lifetime: &'a () }
        pub type TerminalWorkObservation = ();
        pub type TerminalReservationObservation = ();
        impl<'a> CompleteTerminalHandoff<'a> {
            pub fn work(&self) -> TerminalWorkObservation {}
            pub fn reservation_observation(&self) -> TerminalReservationObservation {}
            pub fn release_reservation(&mut self) -> Result<(), crate::BucketSnapshotLoadError> { Ok(()) }
            pub fn application_observation(&self, _: u32) -> ApplicationObservation { ApplicationObservation::Applied }
            pub fn slot_retirement_observation(&self) -> TerminalSlotRetirementObservation { TerminalSlotRetirementObservation::Retired }
            pub fn retire_embedded(&mut self, _: &EmbeddedPublicationSection<'_>, _: Instant) -> Result<(), CompleteRetirementSendError> { Ok(()) }
            pub fn retire_remote(&mut self, _: &UnixStorageNodeClient, _: &UnixStorageNodeMetadataCommandSession, _: Instant) -> Result<(), CompleteRetirementSendError> { Ok(()) }
            fn from_confirmed_owner(owner: CompleteFanoutSender<'a>, _: (), _: ()) -> Self { Self { owner, retirement: TerminalSlotRetirementObservation::Retired, lifetime: &() } }
            pub fn id(&self) -> MetadataCommandId { MetadataCommandId }
            pub fn require_valid(&self) -> Result<(), StoreError> { Ok(()) }
        }
        #[path = "OWNER_PATH"] mod window;
        pub(in crate::node_runtime) use window::{DirectStagingPreparation, DirectStagingSnapshot, DirectTerminalPreparation, DirectTerminalInstallation, DirectTerminalInstallationObservation, DirectTerminalInstallationProof};
        pub(in crate::node_runtime) use window::{StreamAppendInstallation, StreamAppendInstallationProof, StreamAppendInstallationObservation, StreamAppendPreparation, RemoteStagingAcknowledgementAttempt, StagingAcknowledgementBatch, StreamStagingFileWriter, DirectStagingFileWriter, RemoteDirectFileAttempt, RemoteStreamFileAttempt, RemoteStreamAllocationAttempt, AllocatedStreamWindow, StreamWindowAllocationFailure, StreamWindowAllocationPermit, ConfirmedStagingWindow, StagingWindowHandoffFailure};
        mod sibling {
            use super::*;
            fn probe<'a>(terminal: CompleteTerminalHandoff<'a>, confirmed: ConfirmedStagingWindow<'a>, failure: StagingWindowHandoffFailure<'a>, section: &EmbeddedPublicationSection<'_>, owned_section: EmbeddedPublicationSection<'_>, client: &UnixStorageNodeClient, session: &UnixStorageNodeMetadataCommandSession, allocated: AllocatedStreamWindow<'a>, allocation_failure: StreamWindowAllocationFailure<'a>, request: &crate::PrepareStreamUploadSegmentAppendReq, expected: &crate::types::ExpectedStreamAppendState, prepared_intro: window::DirectIntroductionFilePreparation<'a>) {
                PROBE
            }
        }
    }
}
mod clock { pub fn current_time_millis() -> u64 { 0 } }
mod node_client { pub use crate::node_runtime::pg_store::{CompletePrimarySessionBorrow, MetadataCommandSessionIdentity}; }
pub struct PrepareStreamUploadSegmentAppendReq { pub session_id: String, pub segment_index: u32, pub size: u64, pub segment_crc64: u64, pub payload_crc64: u64 }
#[derive(Clone)] pub struct StreamUploadSegmentRecord { pub session_id: String, pub segment_index: u32, pub size: u64, pub segment_crc64: u64, pub payload_crc64: u64, pub data_pg_id: u32, pub ec_k: u8, pub ec_m: u8, pub segment_okh: [u8;16], pub segment_vid: u64, pub shard_integrity: Option<()> }
#[derive(Clone)] pub struct ObjectEncryption;
impl ObjectEncryption { pub fn segment_stored_size(&self, _: u64) -> Option<u64> { Some(20) } }
pub struct EcShape { pub k: u8, pub m: u8 }
#[derive(Clone)] pub struct StreamUploadRecord { pub bucket: String, pub key: String, pub target: (), pub encryption: ObjectEncryption }
mod metadata_command { pub struct CommitDirectPutObjectCommand; pub struct AppendStreamSegmentCommand { pub bucket: String, pub key: String, pub target: (), pub segment: crate::StreamUploadSegmentRecord } }
pub type NodeId = u32;
pub struct BucketSnapshotLoadError;
impl From<node_runtime::pg_store::StoreError> for BucketSnapshotLoadError { fn from(_: node_runtime::pg_store::StoreError) -> Self { Self } }
pub type PgTopology = ();
pub type PgId = u32;
pub type ClusterEpoch = u64;
mod storage_rpc {
    pub struct StorageRpcDirectPutCommitSnapshotResponse { pub snapshot: crate::DirectPutCommitStorageSnapshot }
    pub fn encode_direct_put_commit_snapshot_response(_: &StorageRpcDirectPutCommitSnapshotResponse) -> Vec<u8> { Vec::new() }
    pub mod complete_direct_prepare {
        pub struct EncodedRequest;
        pub struct Request<'a> { pub snapshot_request: &'a [u8], pub snapshot_sha256: [u8;32], pub segment_crc64: u64, pub provisional: crate::ObjectEncryption, pub commit: crate::StreamPutCommitInput }
        pub fn encode_request(_: &Request<'_>) -> Result<EncodedRequest, String> { unimplemented!() }
    }
    pub fn decode_direct_put_commit_snapshot_response(_: &[u8]) -> Result<StorageRpcDirectPutCommitSnapshotResponse, String> { unimplemented!() }
    pub mod complete_direct_snapshot {
        pub struct EncodedRequest;
        pub fn encode_request(_: &super::complete_staging_ack::Request<'_>) -> Result<EncodedRequest, String> { Ok(EncodedRequest) }
        impl EncodedRequest {
            pub fn bytes(&self) -> &[u8] { &[] }
            pub fn decode_response<'a>(&self, bytes: &'a [u8]) -> Result<&'a [u8], String> { Ok(bytes) }
        }
    }
    pub mod complete_stream_append_prepare {
        pub struct EncodedRequest;
        pub fn encode_request(_: &super::complete_staging_ack::Request<'_>) -> Result<EncodedRequest, String> { Ok(EncodedRequest) }
    }
    pub mod complete_staging_ack {
        pub use super::complete_staging_file::Provenance;
        pub struct Receipt { pub shard_index: u8, pub ack: crate::WriteAck }
        pub struct Request<'a> { pub receiver: super::complete_settled_prefix::Request, pub provenance: Provenance<'a>, pub receipts: Vec<Receipt> }
        pub struct EncodedRequest;
        pub fn encode_request(_: &Request<'_>) -> Result<EncodedRequest, String> { Ok(EncodedRequest) }
        impl EncodedRequest {
            pub fn bytes(&self) -> &[u8] { &[] }
            pub fn decode_response(&self, _: &[u8]) -> Result<(), String> { Ok(()) }
        }
    }
    pub mod complete_settled_prefix {
        pub struct Request { pub node: u32, pub pg: u32, pub epoch: u64, pub topology: [u8;32], pub deadline: super::StorageRpcOperationDeadline }
        pub fn topology_digest(_: &()) -> [u8;32] { [0;32] }
    }
    pub mod complete_staging_file {
        pub enum Provenance<'a> { Direct { descriptor: &'a [u8], logical_size: u64 }, Stream { inspection: &'a [u8] } }
        pub struct Request<'a> { pub receiver: super::complete_settled_prefix::Request, pub provenance: Provenance<'a>, pub shard_index: u8, pub bytes: &'a [u8] }
        pub struct EncodedRequest;
        pub fn encode_request(_: Request<'_>) -> Result<EncodedRequest, String> { Ok(EncodedRequest) }
        impl EncodedRequest {
            pub fn digest(&self) -> [u8;32] { [0;32] }
            pub fn bytes(&self) -> &[u8] { &[] }
            pub fn decode_response(&self, _: &[u8]) -> Result<crate::WriteAck, String> { Ok(crate::WriteAck) }
        }
    }
    pub mod complete_stream_file_primary {
        pub fn encode_request(_: &[u8], _: &crate::StreamUploadSegmentRecord) -> Result<Vec<u8>, String> { unimplemented!() }
    }
    #[derive(Clone, Copy)] pub struct StorageRpcOperationDeadline { pub portable_wall_valid_until_ms: u64 }
    impl StorageRpcOperationDeadline { pub fn from_instant(_: std::time::Instant) -> Self { Self { portable_wall_valid_until_ms: 0 } } }
    pub mod complete_stream_allocation {
    pub struct Window<'a> { pub descriptor: &'a [u8] }
    pub struct Preparation { pub request: crate::PrepareStreamUploadSegmentAppendReq }
    pub struct Request<'a> { pub window: Window<'a>, pub preparation: Preparation }
    pub fn digest(_: &[u8]) -> [u8;32] { [0;32] }
    pub fn decode_request(_: &[u8]) -> Result<Request<'_>, String> { unimplemented!() }
    pub fn decode_response(_: &[u8], _: [u8;32]) -> Result<crate::StreamUploadSegmentRecord, String> { unimplemented!() }
} }
mod types {
    pub struct CommittedObjectEncryption;
    impl CommittedObjectEncryption {
        pub fn matches(&self, _: &crate::ObjectEncryptionSubject, _: &crate::ObjectEncryption) -> bool { true }
        pub fn encryption(&self) -> &crate::ObjectEncryption { &crate::ObjectEncryption }
    }
    #[derive(Clone)] pub enum ExpectedStreamAppendState { PutObject { session: crate::StreamUploadRecord }, UploadPart { session: crate::StreamUploadRecord } }
    pub struct SegmentShardIntegrityWitness;
    impl SegmentShardIntegrityWitness {
        pub fn durable_record(&self) {}
        pub fn from_ordered_shard_acks(_: u32, _: crate::EcShape, _: &[u8;16], _: u64, _: u64, _: u64, _: &[(&crate::ShardKey, crate::WriteAck)]) -> Result<Self, String> { unimplemented!() }
    }
    #[derive(Clone, Copy)] pub struct AdmittedRouteEffectFence;
    impl AdmittedRouteEffectFence {
        pub fn cluster_epoch(&self) -> u64 { 1 }
        pub fn deadline(&self) -> Option<Self> { Some(Self) }
        pub fn portable_wall_valid_until_ms(&self) -> u64 { 1 }
        pub fn authority_valid_until_ms(&self) -> u64 { 1 }
        pub fn intersect(self, _: Self) -> Result<Self, crate::node_runtime::pg_store::StoreError> { Ok(self) }
        pub fn bind_portable(_: u64, _: u64, _: u64) -> Self { Self }
        pub fn require_valid_for(self, _: u64) -> Result<(), crate::node_runtime::pg_store::StoreError> { Ok(()) }
    }
}
#[derive(Clone, Copy, PartialEq)] pub struct WriteAck;
#[derive(Clone)] pub struct ShardKey;
mod cluster {
    pub struct DirectPutTerminalWork;
    impl DirectPutTerminalWork {
        pub fn bounds(&self) -> (std::time::Instant, crate::storage_rpc::StorageRpcOperationDeadline) { unimplemented!() }
        pub fn require_deadline(&self) -> Result<(), crate::node_runtime::pg_store::StoreError> { Ok(()) }
        pub fn begin_attempt(&mut self) -> Result<(), crate::node_runtime::pg_store::StoreError> { Ok(()) }
    }
    pub struct EmbeddedPublicationSectionIdentity(());
    impl EmbeddedPublicationSectionIdentity {
        pub fn require_section(&self, _: &crate::node_runtime::pg_store::EmbeddedPublicationSection<'_>) -> Result<(), crate::node_runtime::pg_store::StoreError> { Ok(()) }
    }
    pub struct ShardLocation; impl ShardLocation { pub fn node_id(&self) -> u32 { 0 } }
}
mod node {
    pub use crate::node_runtime::engine::LocalNodeRuntime;
}
