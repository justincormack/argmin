// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

// Compile the actual handoff; unrelated dependencies are stubs.
#![allow(dead_code, unused_imports)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)] pub struct PgTopology;
#[derive(Clone, Copy, PartialEq, Eq, Debug)] pub struct ClusterEpoch;
#[derive(Clone, Copy, PartialEq, Eq, Debug)] pub struct PgId;
pub struct BucketSnapshotLoadError;
mod storage_rpc { #[derive(Clone, Copy)] pub struct StorageRpcOperationDeadline; }
impl From<node_runtime::pg_store::StoreError> for BucketSnapshotLoadError {
    fn from(_: node_runtime::pg_store::StoreError) -> Self { Self }
}
pub mod metadata_command {
    use super::*;
    #[derive(Clone, Copy, PartialEq, Eq, Debug)] pub struct MetadataCommandId;
    impl MetadataCommandId {
        pub fn pg_id(self) -> PgId { PgId }
        pub fn cluster_epoch(self) -> ClusterEpoch { ClusterEpoch }
        pub fn new(_: ClusterEpoch, _: PgId, _: MetadataCommandLogIndex) -> Self { Self }
    }
    pub struct MetadataCommandLogIndex;
    impl MetadataCommandLogIndex { pub fn new(_: u64) -> Option<Self> { Some(Self) } }
    #[derive(Clone, Copy)] pub struct MetadataCommandReplicaState { pub cluster_epoch: ClusterEpoch, pub applied_log_index: u64 }
}
mod cluster {
    use super::*;
    use node_runtime::pg_store::*;
    pub use node_runtime::pg_store::Route as CompleteFanoutRoute;
    pub struct EmbeddedPublicationSection<'a>(&'a ());
    pub struct EmbeddedApplicationTarget;
    impl EmbeddedPublicationSection<'_> {
        pub fn application_target(&self, _: NodeId) -> Result<EmbeddedApplicationTarget, StoreError> { Ok(EmbeddedApplicationTarget) }
    }
    impl EmbeddedApplicationTarget {
        pub fn apply_complete_command(&self, _: &CompleteApplicationRequest<'_>) -> Result<metadata_command::MetadataCommandReplicaState, CompleteApplicationError> { unimplemented!() }
    }
}
mod node_runtime {
    pub mod clients {
        use super::pg_store::NodeId;
        pub struct UnixStorageNodeClient;
        impl UnixStorageNodeClient { pub fn node_id(&self) -> NodeId { NodeId } }
    }
    pub mod engine {
        use super::pg_store::*;
        pub struct EmbeddedPublicationAuthority<'a>(&'a ());
        impl EmbeddedPublicationAuthority<'_> {
            pub fn require_abandoned_part_publication(&self, _: &AbandonedPartPublication<'_, '_>) -> Result<(), StoreError> { Ok(()) }
            pub fn require_abandoned_part_installation(&self, _: &AbandonedPartInstallation<'_, '_>) -> Result<(), StoreError> { Ok(()) }
            pub fn require_abandoned_part_preparation(&self, _: &AbandonedPartPreparation<'_, '_>) -> Result<(), StoreError> { Ok(()) }
            pub fn require_effect(&self, _: &PgStore, _: &CompletePublicationRequest<'_>) -> Result<(), StoreError> { Ok(()) }
        }
    }
    pub mod pg_store {
        use crate::*;
        use crate::node_runtime::engine;
        use metadata_command::{MetadataCommandId, MetadataCommandReplicaState};
        use payload_staging::ContextualCommand;
        impl cluster::EmbeddedPublicationSection<'_> {
            pub(in crate::node_runtime) fn inspect_abandoned_part_custody(&self, _: &AbandonedPartPublication<'_, '_>, _: std::time::Instant) -> Result<AbandonedPartCustodyObservation, AbandonedPartCustodyError> { unimplemented!() }
            pub(in crate::node_runtime) fn publish_abandoned_part_cleanup(&self, _: &AbandonedPartPublication<'_, '_>, _: std::time::Instant) -> Result<(), CompletePublicationError> { Ok(()) }
            pub(in crate::node_runtime) fn install_abandoned_part_cleanup(&self, _: &CompletePublicationRequest<'_>, _: AbandonedPartInstallation<'_, '_>, _: std::time::Instant) -> Result<(), CompletePublicationError> { Ok(()) }
            pub(in crate::node_runtime) fn prepare_abandoned_part_cleanup<'a>(&self, _: &CompletePublicationRequest<'_>, _: &'a AbandonedPartPreparation<'_, '_>, _: std::time::Instant) -> Result<PreparedAbandonedPartBorrow<'a>, AbandonedPartPreparationError> { unimplemented!() }
        }
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)] pub struct NodeId;
        impl NodeId { pub fn as_u32(self) -> u32 { 0 } }
        #[derive(Debug)] pub enum StoreError { RouteCapabilitySubjectMismatch { operation: &'static str } }
        pub struct CompletePublicationError;
        pub struct AbandonedPartCustodyObservation;
        pub struct AbandonedPartCustodyError;
        impl From<StoreError> for AbandonedPartCustodyError { fn from(_: StoreError) -> Self { Self } }
        pub struct CompleteApplicationError;
        #[derive(Clone, Copy, PartialEq, Eq)] pub enum ApplicationObservation { NoAttemptObserved, Unconfirmed, Applied }
        pub struct CompleteApplicationSendError;
        pub enum ApplicationAttemptError { PublicationNotConfirmed, Embedded(CompleteApplicationError), Remote(CompleteApplicationSendError) }
        pub enum FanoutError { Scope(StoreError), Application(ApplicationAttemptError), WitnessNotConfirmed, PrimaryNotConfirmed, Incomplete { confirmed: usize, required: usize } }
        impl From<StoreError> for FanoutError { fn from(e: StoreError) -> Self { Self::Scope(e) } }
        impl From<ApplicationAttemptError> for FanoutError { fn from(e: ApplicationAttemptError) -> Self { Self::Application(e) } }
        #[derive(Clone, Copy, PartialEq, Eq)] pub enum PublicationObservation { NoAttemptObserved, Unconfirmed, MarkerConfirmed }
        impl From<StoreError> for CompletePublicationError { fn from(_: StoreError) -> Self { Self } }
        pub enum AbandonmentHandoffError { Scope(StoreError), Incomplete { confirmed: usize, required: usize } }
        impl From<StoreError> for AbandonmentHandoffError {
            fn from(e: StoreError) -> Self { Self::Scope(e) }
        }
        #[derive(PartialEq, Eq)] pub enum UnstartedAbandonmentObservation { PrimaryTombstoneConfirmed }
        #[derive(PartialEq, Eq)] pub enum ReplicaAbandonmentObservation { TombstoneConfirmed }
        pub struct Route<'a>(&'a ());
        impl Route<'_> {
            fn nodes(&self) -> impl Iterator<Item = NodeId> { [NodeId].into_iter() }
            fn primary(&self) -> NodeId { NodeId }
            fn is_historical(&self) -> bool { false }
            fn require_embedded_section(&self, _: &cluster::EmbeddedPublicationSection<'_>) -> Result<(), StoreError> { Ok(()) }
            fn require_client(&self, _: &crate::node_runtime::clients::UnixStorageNodeClient) -> Result<(), StoreError> { Ok(()) }
        }
        pub struct Source;
        impl Source { fn request(&self, _: NodeId) -> Result<CompletePublicationRequest<'_>, StoreError> { unimplemented!() } }
        pub struct Sender { source: Source }
        type CompleteFanoutRoute<'a> = Route<'a>;
        #[derive(Clone, Copy)] pub struct CapturedPublicationDeadline { local: std::time::Instant, portable: storage_rpc::StorageRpcOperationDeadline }
        #[derive(Clone, Copy, PartialEq, Eq)] pub enum TerminalReservationObservation { NoAttemptObserved, Unconfirmed, Released }
        pub struct TerminalReservationAuthority<'a>(&'a ());
        impl TerminalReservationAuthority<'_> {
            fn release_abandoned_part_finalizer(_: handoff::AbandonedPartFinalizerRelease<'_>) -> Result<(), BucketSnapshotLoadError> { Ok(()) }
            fn require_context(&self, _: &ContextualCommand) -> Result<(), StoreError> { Ok(()) }
        }
        pub struct Fanout<'a> { route: Route<'a>, sender: Sender }
        impl Fanout<'_> {
            fn require_authority(&self) -> Result<(), StoreError> { Ok(()) }
            fn id(&self) -> MetadataCommandId { MetadataCommandId }
            fn work_deadline(&self) -> std::time::Instant { std::time::Instant::now() }
            fn captured_deadline(&self) -> CapturedPublicationDeadline { unimplemented!() }
            fn witness(&self) -> Option<NodeId> { None }
        }
        pub struct CompleteUnstartedAbandonment<'a> { owner: Fanout<'a>, observation: UnstartedAbandonmentObservation }
        impl CompleteUnstartedAbandonment<'_> {
            fn replica_observation(&self, _: NodeId) -> ReplicaAbandonmentObservation { ReplicaAbandonmentObservation::TombstoneConfirmed }
        }
        #[path = "OWNER_PATH"] mod handoff;
        pub(in crate::node_runtime) use handoff::{AbandonedPartInstallation, AbandonedPartInstallationObservation, AbandonedPartPublication};
        pub(in crate::node_runtime) use handoff::{AbandonedPartTerminalHandoff, AbandonedPartTerminalFailure};
        use handoff::AbandonedPartFinalizerRelease;
        pub(in crate::node_runtime) use handoff::{CompleteAbandonmentHandoff, AbandonmentHandoffFailure, AbandonedPartPreparation, AbandonedPartPreparationError, PreparedAbandonedPart, PreparedAbandonedPartBorrow, AbandonedPartPreparationFailure};
        pub struct CompletePublicationRequest<'a>(&'a ());
        pub struct CompletePublicationTransport<'a>(&'a ());
        impl CompletePublicationTransport<'_> {
            pub fn send_application_in_fanout(&self, _: &Route<'_>, _: &crate::node_runtime::clients::UnixStorageNodeClient, _: &mut ApplicationObservation) -> Result<MetadataCommandReplicaState, CompleteApplicationSendError> { unimplemented!() }
        }
        pub struct CompleteApplicationRequest<'a>(&'a ());
        impl<'a> CompleteApplicationRequest<'a> {
            pub fn from_publication_transport(_: &'a CompletePublicationTransport<'a>) -> Self { unimplemented!() }
        }
        impl CompletePublicationRequest<'_> {
            pub fn abandoned_part_member_request<'a>(_: &'a AbandonedPartPublication<'_, '_>, _: NodeId) -> Result<CompletePublicationRequest<'a>, StoreError> { unimplemented!() }
            pub fn prepare_transport(&self, _: std::time::Instant) -> CompletePublicationTransport<'_> { unimplemented!() }
            pub fn require_abandoned_part_binding(&self, _: &AbandonedPartPublication<'_, '_>) -> Result<(), StoreError> { Ok(()) }
            pub fn start_embedded(&self, _: &PgStore, _: &engine::EmbeddedPublicationAuthority<'_>) -> Result<(), CompletePublicationError> { Ok(()) }
            pub fn install_abandoned_part(&self, _: &PgStore, _: &engine::EmbeddedPublicationAuthority<'_>, _: &AbandonedPartInstallation<'_, '_>) -> Result<(), CompletePublicationError> { Ok(()) }
            pub fn node_id(&self) -> NodeId { NodeId }
            pub fn id(&self) -> MetadataCommandId { MetadataCommandId }
            pub fn command_sha256(&self) -> [u8; 32] { [0; 32] }
            pub fn topology(&self) -> &PgTopology { &PgTopology }
        }
        pub enum MetadataCommandStartupDisposition { Abandoned {} }
        pub struct Observation { publication_started: bool, terminal: MetadataCommandStartupDisposition }
        pub struct Retained { context: ContextualCommand, observation: Observation }
        pub struct Snapshot;
        impl Snapshot { pub fn finish(self) -> Result<(), StoreError> { Ok(()) } }
        pub struct PgStore;
        impl PgStore {
            fn command_build_snapshot(&self) -> Result<Snapshot, StoreError> { Ok(Snapshot) }
            fn load_complete_pending_in_open_transaction(&self, _: u32, _: &PgTopology) -> Result<Option<Retained>, StoreError> { unimplemented!() }
            fn metadata_command_replica_state(&self) -> Result<MetadataCommandReplicaState, StoreError> { unimplemented!() }
        }
        pub mod payload_staging {
            use super::*;
            #[derive(Debug)] pub enum ContextError { Encoding(&'static str), ReissuePosition }
            impl From<StoreError> for ContextError { fn from(_: StoreError) -> Self { Self::ReissuePosition } }
            impl std::fmt::Display for ContextError { fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "error") } }
            impl std::error::Error for ContextError {}
            pub struct ContextualCommand;
            #[derive(Clone, Copy)] pub struct TerminalWorkObservation;
            impl ContextualCommand {
                pub fn inspect_abandoned_part_custody(&self, _: &PgStore, _: u32) -> Result<AbandonedPartCustodyObservation, AbandonedPartCustodyError> { unimplemented!() }
                pub fn release_terminal_reservation(&self, _: TerminalReservationAuthority<'_>) -> Result<(), BucketSnapshotLoadError> { Ok(()) }
                pub fn terminal_work(&self) -> TerminalWorkObservation { TerminalWorkObservation }
                pub fn id(&self) -> MetadataCommandId { MetadataCommandId }
                pub fn encode_command(&self) -> Result<EncodedContextualCommand<'_>, StoreError> { Ok(EncodedContextualCommand(self)) }
            }
            pub struct EncodedContextualCommand<'a>(&'a ContextualCommand);
            impl EncodedContextualCommand<'_> {
                pub fn bytes(&self) -> &[u8] { &[] }
                pub fn id(&self) -> MetadataCommandId { MetadataCommandId }
                pub fn command_sha256(&self) -> [u8; 32] { [0; 32] }
                pub fn topology(&self) -> &PgTopology { &PgTopology }
                pub fn capture_admitted_part_cleanup(&self, _: &AbandonedPartPreparation<'_, '_>, _: &PgStore, _: u32, _: MetadataCommandId) -> Result<ContextualCommand, ContextError> { Ok(ContextualCommand) }
            }
        }
        mod sibling {
            use super::*;
            fn probe<'a>(owner: CompleteUnstartedAbandonment<'a>, complete: CompleteAbandonmentHandoff<'a>, failure: AbandonmentHandoffFailure<'a>, mut prepared: PreparedAbandonedPart<'a>, failed: AbandonedPartPreparationFailure<'a>, mut terminal: AbandonedPartTerminalHandoff<'a>, terminal_failure: AbandonedPartTerminalFailure<'a>, section: &cluster::EmbeddedPublicationSection<'_>, replica: &crate::node_runtime::clients::UnixStorageNodeClient) {
                PROBE
            }
        }
    }
}
