use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::{self, File};
use std::future::Future;
use std::io::{self, Cursor, Read, Write};
use std::ops::{Bound, RangeBounds};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use futures_util::{Stream, StreamExt};
use openraft::errors::{NetworkError, RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::impls::leader_id_adv::LeaderId;
use openraft::impls::BasicNode;
use openraft::impls::Entry;
use openraft::impls::Vote;
use openraft::network::{RPCOption, RaftNetworkFactory, RaftNetworkV2};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, TransferLeaderError,
    TransferLeaderRequest, TransferLeaderResponse, VoteRequest, VoteResponse,
};
use openraft::storage::Snapshot;
use openraft::storage::SnapshotMeta;
use openraft::storage::{EntryResponder, IOFlushed, LogState, RaftLogStorage, RaftStateMachine};
use openraft::type_config::alias::{
    LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf, VoteOf,
};
use openraft::type_config::TypeConfigExt;
use openraft::EntryPayload;
use openraft::LogId;
use openraft::Membership;
use openraft::OptionalSend;
use openraft::Raft;
use openraft::RaftLogReader;
use openraft::RaftSnapshotBuilder;
use openraft::RaftTypeConfig;
use openraft::ReadPolicy;
use openraft::ServerState;
use openraft::StoredMembership;
use openraft::{AnyError, Config};
use placement::NodeId;

use crate::control_plane::{
    AuthorityIncarnation, ClusterControlSnapshot, ClusterRuntimeMapSnapshot, ControlPlaneError,
    NodeAvailabilityState, NodeMembershipState,
};
use crate::control_plane_command::{
    decode_control_plane_command, encode_control_plane_command, ControlPlaneCommand,
    ControlPlaneCommandResponse, ControlPlaneLogId, ControlPlaneSnapshotArtifact,
    ReplicatedControlPlaneStateMachine,
};
use crate::{ClusterEpoch, PgState};

pub type ControlPlaneRaftNodeId = u64;
pub type ControlPlaneRaftTerm = u64;
pub type ControlPlaneRaftLeaderId = LeaderId<ControlPlaneRaftTerm, ControlPlaneRaftNodeId>;
pub type ControlPlaneRaftEntry =
    Entry<ControlPlaneRaftLeaderId, ControlPlaneCommand, ControlPlaneRaftNodeId, BasicNode>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlPlaneRaftPeerFrameIdentity {
    pub cluster_name: String,
    pub source: ControlPlaneRaftNodeId,
    pub target: ControlPlaneRaftNodeId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlPlaneRaftPeerFrameKind {
    OrdinaryRpc,
    Snapshot,
}

impl ControlPlaneRaftPeerFrameIdentity {
    #[must_use]
    pub fn new(
        cluster_name: impl Into<String>,
        source: ControlPlaneRaftNodeId,
        target: ControlPlaneRaftNodeId,
    ) -> Self {
        Self {
            cluster_name: cluster_name.into(),
            source,
            target,
        }
    }
}

#[derive(Debug, Clone)]
pub enum ControlPlaneRaftPeerRpcRequest {
    AppendEntries(AppendEntriesRequest<ControlPlaneRaftTypeConfig>),
    Vote(VoteRequest<ControlPlaneRaftTypeConfig>),
    PreVote(VoteRequest<ControlPlaneRaftTypeConfig>),
    TransferLeader(TransferLeaderRequest<ControlPlaneRaftTypeConfig>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlPlaneRaftPeerRpcResponse {
    AppendEntries(AppendEntriesResponse<ControlPlaneRaftTypeConfig>),
    Vote(VoteResponse<ControlPlaneRaftTypeConfig>),
    TransferLeader(TransferLeaderResponse<ControlPlaneRaftTypeConfig>),
}

#[derive(Debug, Clone)]
pub struct ControlPlaneRaftPeerSnapshotRequest {
    pub vote: VoteOf<ControlPlaneRaftTypeConfig>,
    pub snapshot: SnapshotOf<ControlPlaneRaftTypeConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlPlaneRaftPeerSnapshotResponse {
    pub response: SnapshotResponse<ControlPlaneRaftTypeConfig>,
}

impl ControlPlaneRaftPeerRpcRequest {
    pub fn encode_frame(&self) -> Result<Vec<u8>, ControlPlaneError> {
        self.encode_frame_with_identity(None)
    }

    pub fn encode_frame_for_peer(
        &self,
        identity: &ControlPlaneRaftPeerFrameIdentity,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        self.encode_frame_with_identity(Some(identity))
    }

    fn encode_frame_with_identity(
        &self,
        identity: Option<&ControlPlaneRaftPeerFrameIdentity>,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        let mut out = Vec::new();
        out.extend_from_slice(CONTROL_PLANE_RAFT_PEER_RPC_MAGIC);
        write_raft_u16(&mut out, CONTROL_PLANE_RAFT_PEER_RPC_VERSION);
        write_raft_u8(&mut out, CONTROL_PLANE_RAFT_PEER_RPC_KIND_REQUEST);
        write_raft_peer_frame_identity(&mut out, identity)?;
        match self {
            Self::AppendEntries(request) => {
                write_raft_u8(&mut out, CONTROL_PLANE_RAFT_PEER_RPC_REQUEST_APPEND_ENTRIES);
                write_raft_append_entries_request(&mut out, request)?;
            }
            Self::Vote(request) => {
                write_raft_u8(&mut out, CONTROL_PLANE_RAFT_PEER_RPC_REQUEST_VOTE);
                write_raft_vote_request(&mut out, request);
            }
            Self::PreVote(request) => {
                write_raft_u8(&mut out, CONTROL_PLANE_RAFT_PEER_RPC_REQUEST_PRE_VOTE);
                write_raft_vote_request(&mut out, request);
            }
            Self::TransferLeader(request) => {
                write_raft_u8(
                    &mut out,
                    CONTROL_PLANE_RAFT_PEER_RPC_REQUEST_TRANSFER_LEADER,
                );
                write_raft_transfer_leader_request(&mut out, request);
            }
        }
        append_raft_artifact_checksum(&mut out);
        Ok(out)
    }

    pub fn decode_frame(bytes: &[u8]) -> Result<Self, ControlPlaneError> {
        Self::decode_frame_with_identity(bytes, None)
    }

    pub fn decode_frame_for_peer(
        bytes: &[u8],
        expected_identity: &ControlPlaneRaftPeerFrameIdentity,
    ) -> Result<Self, ControlPlaneError> {
        Self::decode_frame_with_identity(bytes, Some(expected_identity))
    }

    fn decode_frame_with_identity(
        bytes: &[u8],
        expected_identity: Option<&ControlPlaneRaftPeerFrameIdentity>,
    ) -> Result<Self, ControlPlaneError> {
        decode_raft_peer_rpc_frame(
            bytes,
            CONTROL_PLANE_RAFT_PEER_RPC_KIND_REQUEST,
            expected_identity,
            |reader| match reader.read_u8()? {
                CONTROL_PLANE_RAFT_PEER_RPC_REQUEST_APPEND_ENTRIES => {
                    Ok(Self::AppendEntries(reader.read_append_entries_request()?))
                }
                CONTROL_PLANE_RAFT_PEER_RPC_REQUEST_VOTE => {
                    Ok(Self::Vote(reader.read_vote_request()?))
                }
                CONTROL_PLANE_RAFT_PEER_RPC_REQUEST_PRE_VOTE => {
                    Ok(Self::PreVote(reader.read_vote_request()?))
                }
                CONTROL_PLANE_RAFT_PEER_RPC_REQUEST_TRANSFER_LEADER => {
                    Ok(Self::TransferLeader(reader.read_transfer_leader_request()?))
                }
                value => Err(raft_artifact_protocol_error(format!(
                    "unknown control-plane OpenRaft peer RPC request tag {value}"
                ))),
            },
        )
    }
}

impl ControlPlaneRaftPeerRpcResponse {
    pub fn encode_frame(&self) -> Result<Vec<u8>, ControlPlaneError> {
        self.encode_frame_with_identity(None)
    }

    pub fn encode_frame_for_peer(
        &self,
        identity: &ControlPlaneRaftPeerFrameIdentity,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        self.encode_frame_with_identity(Some(identity))
    }

    fn encode_frame_with_identity(
        &self,
        identity: Option<&ControlPlaneRaftPeerFrameIdentity>,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        let mut out = Vec::new();
        out.extend_from_slice(CONTROL_PLANE_RAFT_PEER_RPC_MAGIC);
        write_raft_u16(&mut out, CONTROL_PLANE_RAFT_PEER_RPC_VERSION);
        write_raft_u8(&mut out, CONTROL_PLANE_RAFT_PEER_RPC_KIND_RESPONSE);
        write_raft_peer_frame_identity(&mut out, identity)?;
        match self {
            Self::AppendEntries(response) => {
                write_raft_u8(
                    &mut out,
                    CONTROL_PLANE_RAFT_PEER_RPC_RESPONSE_APPEND_ENTRIES,
                );
                write_raft_append_entries_response(&mut out, response);
            }
            Self::Vote(response) => {
                write_raft_u8(&mut out, CONTROL_PLANE_RAFT_PEER_RPC_RESPONSE_VOTE);
                write_raft_vote_response(&mut out, response);
            }
            Self::TransferLeader(response) => {
                write_raft_u8(
                    &mut out,
                    CONTROL_PLANE_RAFT_PEER_RPC_RESPONSE_TRANSFER_LEADER,
                );
                write_raft_transfer_leader_response(&mut out, response);
            }
        }
        append_raft_artifact_checksum(&mut out);
        Ok(out)
    }

    pub fn decode_frame(bytes: &[u8]) -> Result<Self, ControlPlaneError> {
        Self::decode_frame_with_identity(bytes, None)
    }

    pub fn decode_frame_for_peer(
        bytes: &[u8],
        expected_identity: &ControlPlaneRaftPeerFrameIdentity,
    ) -> Result<Self, ControlPlaneError> {
        Self::decode_frame_with_identity(bytes, Some(expected_identity))
    }

    fn decode_frame_with_identity(
        bytes: &[u8],
        expected_identity: Option<&ControlPlaneRaftPeerFrameIdentity>,
    ) -> Result<Self, ControlPlaneError> {
        decode_raft_peer_rpc_frame(
            bytes,
            CONTROL_PLANE_RAFT_PEER_RPC_KIND_RESPONSE,
            expected_identity,
            |reader| match reader.read_u8()? {
                CONTROL_PLANE_RAFT_PEER_RPC_RESPONSE_APPEND_ENTRIES => {
                    Ok(Self::AppendEntries(reader.read_append_entries_response()?))
                }
                CONTROL_PLANE_RAFT_PEER_RPC_RESPONSE_VOTE => {
                    Ok(Self::Vote(reader.read_vote_response()?))
                }
                CONTROL_PLANE_RAFT_PEER_RPC_RESPONSE_TRANSFER_LEADER => Ok(Self::TransferLeader(
                    reader.read_transfer_leader_response()?,
                )),
                value => Err(raft_artifact_protocol_error(format!(
                    "unknown control-plane OpenRaft peer RPC response tag {value}"
                ))),
            },
        )
    }
}

impl ControlPlaneRaftPeerSnapshotRequest {
    pub fn encode_frame(&self) -> Result<Vec<u8>, ControlPlaneError> {
        self.encode_frame_with_identity(None)
    }

    pub fn encode_frame_for_peer(
        &self,
        identity: &ControlPlaneRaftPeerFrameIdentity,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        self.encode_frame_with_identity(Some(identity))
    }

    fn encode_frame_with_identity(
        &self,
        identity: Option<&ControlPlaneRaftPeerFrameIdentity>,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        let mut out = Vec::new();
        out.extend_from_slice(CONTROL_PLANE_RAFT_PEER_RPC_MAGIC);
        write_raft_u16(&mut out, CONTROL_PLANE_RAFT_PEER_RPC_VERSION);
        write_raft_u8(&mut out, CONTROL_PLANE_RAFT_PEER_RPC_KIND_SNAPSHOT_REQUEST);
        write_raft_peer_frame_identity(&mut out, identity)?;
        write_raft_vote(&mut out, self.vote);
        write_raft_snapshot(&mut out, &self.snapshot)?;
        append_raft_artifact_checksum(&mut out);
        Ok(out)
    }

    pub fn decode_frame(
        bytes: &[u8],
        max_frame_bytes: usize,
        max_snapshot_bytes: usize,
    ) -> Result<Self, ControlPlaneError> {
        Self::decode_frame_with_identity(bytes, max_frame_bytes, max_snapshot_bytes, None)
    }

    pub fn decode_frame_for_peer(
        bytes: &[u8],
        max_frame_bytes: usize,
        max_snapshot_bytes: usize,
        expected_identity: &ControlPlaneRaftPeerFrameIdentity,
    ) -> Result<Self, ControlPlaneError> {
        Self::decode_frame_with_identity(
            bytes,
            max_frame_bytes,
            max_snapshot_bytes,
            Some(expected_identity),
        )
    }

    fn decode_frame_with_identity(
        bytes: &[u8],
        max_frame_bytes: usize,
        max_snapshot_bytes: usize,
        expected_identity: Option<&ControlPlaneRaftPeerFrameIdentity>,
    ) -> Result<Self, ControlPlaneError> {
        if bytes.len() > max_frame_bytes {
            return Err(raft_artifact_protocol_error(format!(
                "control-plane OpenRaft peer snapshot request frame size {} bytes exceeds limit {}",
                bytes.len(),
                max_frame_bytes
            )));
        }
        decode_raft_peer_rpc_frame(
            bytes,
            CONTROL_PLANE_RAFT_PEER_RPC_KIND_SNAPSHOT_REQUEST,
            expected_identity,
            |reader| {
                let vote = reader.read_vote()?;
                let snapshot = reader
                    .read_snapshot_limited("raft peer snapshot payload", max_snapshot_bytes)?;
                Ok(Self { vote, snapshot })
            },
        )
    }
}

impl ControlPlaneRaftPeerSnapshotResponse {
    pub fn encode_frame(&self) -> Result<Vec<u8>, ControlPlaneError> {
        self.encode_frame_with_identity(None)
    }

    pub fn encode_frame_for_peer(
        &self,
        identity: &ControlPlaneRaftPeerFrameIdentity,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        self.encode_frame_with_identity(Some(identity))
    }

    fn encode_frame_with_identity(
        &self,
        identity: Option<&ControlPlaneRaftPeerFrameIdentity>,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        let mut out = Vec::new();
        out.extend_from_slice(CONTROL_PLANE_RAFT_PEER_RPC_MAGIC);
        write_raft_u16(&mut out, CONTROL_PLANE_RAFT_PEER_RPC_VERSION);
        write_raft_u8(&mut out, CONTROL_PLANE_RAFT_PEER_RPC_KIND_SNAPSHOT_RESPONSE);
        write_raft_peer_frame_identity(&mut out, identity)?;
        write_raft_vote(&mut out, self.response.vote);
        append_raft_artifact_checksum(&mut out);
        Ok(out)
    }

    pub fn decode_frame(bytes: &[u8]) -> Result<Self, ControlPlaneError> {
        Self::decode_frame_with_identity(bytes, None)
    }

    pub fn decode_frame_for_peer(
        bytes: &[u8],
        expected_identity: &ControlPlaneRaftPeerFrameIdentity,
    ) -> Result<Self, ControlPlaneError> {
        Self::decode_frame_with_identity(bytes, Some(expected_identity))
    }

    fn decode_frame_with_identity(
        bytes: &[u8],
        expected_identity: Option<&ControlPlaneRaftPeerFrameIdentity>,
    ) -> Result<Self, ControlPlaneError> {
        decode_raft_peer_rpc_frame(
            bytes,
            CONTROL_PLANE_RAFT_PEER_RPC_KIND_SNAPSHOT_RESPONSE,
            expected_identity,
            |reader| {
                Ok(Self {
                    response: SnapshotResponse {
                        vote: reader.read_vote()?,
                    },
                })
            },
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlPlaneRaftPeerTransportLimits {
    pub max_frame_bytes: usize,
    pub max_append_entries: usize,
    pub max_append_entries_bytes: usize,
    pub max_snapshot_bytes: usize,
}

#[derive(Debug, Clone)]
pub struct ControlPlaneRaftPeerTransportPolicy {
    cluster_name: String,
    peers: BTreeMap<ControlPlaneRaftNodeId, BasicNode>,
    limits: ControlPlaneRaftPeerTransportLimits,
}

impl ControlPlaneRaftPeerTransportPolicy {
    #[must_use]
    pub fn new(
        cluster_name: impl Into<String>,
        peers: BTreeMap<ControlPlaneRaftNodeId, BasicNode>,
        limits: ControlPlaneRaftPeerTransportLimits,
    ) -> Self {
        Self {
            cluster_name: cluster_name.into(),
            peers,
            limits,
        }
    }

    pub fn validate_target_node(
        &self,
        target: ControlPlaneRaftNodeId,
        node: &BasicNode,
        rpc_name: &'static str,
    ) -> Result<(), ControlPlaneRaftPeerTransportRejection> {
        let expected = self.peers.get(&target).ok_or_else(|| {
            ControlPlaneRaftPeerTransportRejection::UnknownTarget {
                cluster_name: self.cluster_name.clone(),
                target,
                rpc_name,
            }
        })?;
        if expected == node {
            Ok(())
        } else {
            Err(ControlPlaneRaftPeerTransportRejection::EndpointMismatch {
                cluster_name: self.cluster_name.clone(),
                target,
                rpc_name,
                expected: expected.addr.clone(),
                actual: node.addr.clone(),
            })
        }
    }

    pub fn frame_identity(
        &self,
        source: ControlPlaneRaftNodeId,
        target: ControlPlaneRaftNodeId,
    ) -> Result<ControlPlaneRaftPeerFrameIdentity, ControlPlaneRaftPeerTransportRejection> {
        if !self.peers.contains_key(&source) {
            return Err(ControlPlaneRaftPeerTransportRejection::UnknownSource {
                cluster_name: self.cluster_name.clone(),
                source,
            });
        }
        if !self.peers.contains_key(&target) {
            return Err(ControlPlaneRaftPeerTransportRejection::UnknownTarget {
                cluster_name: self.cluster_name.clone(),
                target,
                rpc_name: "peer_frame",
            });
        }
        Ok(ControlPlaneRaftPeerFrameIdentity::new(
            self.cluster_name.clone(),
            source,
            target,
        ))
    }

    pub fn validate_append_entries(
        &self,
        target: ControlPlaneRaftNodeId,
        entries_len: usize,
        entries_bytes: usize,
    ) -> Result<(), ControlPlaneRaftPeerTransportRejection> {
        if entries_len > self.limits.max_append_entries {
            return Err(
                ControlPlaneRaftPeerTransportRejection::AppendEntriesBatchTooLarge {
                    cluster_name: self.cluster_name.clone(),
                    target,
                    entries_len,
                    max_append_entries: self.limits.max_append_entries,
                },
            );
        }
        if entries_bytes > self.limits.max_append_entries_bytes {
            return Err(
                ControlPlaneRaftPeerTransportRejection::AppendEntriesPayloadTooLarge {
                    cluster_name: self.cluster_name.clone(),
                    target,
                    entries_bytes,
                    max_append_entries_bytes: self.limits.max_append_entries_bytes,
                },
            );
        }
        Ok(())
    }

    pub fn validate_snapshot(
        &self,
        target: ControlPlaneRaftNodeId,
        snapshot_bytes: usize,
    ) -> Result<(), ControlPlaneRaftPeerTransportRejection> {
        if snapshot_bytes <= self.limits.max_snapshot_bytes {
            Ok(())
        } else {
            Err(ControlPlaneRaftPeerTransportRejection::SnapshotTooLarge {
                cluster_name: self.cluster_name.clone(),
                target,
                snapshot_bytes,
                max_snapshot_bytes: self.limits.max_snapshot_bytes,
            })
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlPlaneRaftPeerTransportRejection {
    UnknownSource {
        cluster_name: String,
        source: ControlPlaneRaftNodeId,
    },
    UnknownTarget {
        cluster_name: String,
        target: ControlPlaneRaftNodeId,
        rpc_name: &'static str,
    },
    EndpointMismatch {
        cluster_name: String,
        target: ControlPlaneRaftNodeId,
        rpc_name: &'static str,
        expected: String,
        actual: String,
    },
    AppendEntriesBatchTooLarge {
        cluster_name: String,
        target: ControlPlaneRaftNodeId,
        entries_len: usize,
        max_append_entries: usize,
    },
    AppendEntriesPayloadTooLarge {
        cluster_name: String,
        target: ControlPlaneRaftNodeId,
        entries_bytes: usize,
        max_append_entries_bytes: usize,
    },
    SnapshotTooLarge {
        cluster_name: String,
        target: ControlPlaneRaftNodeId,
        snapshot_bytes: usize,
        max_snapshot_bytes: usize,
    },
}

impl fmt::Display for ControlPlaneRaftPeerTransportRejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownSource {
                cluster_name,
                source,
            } => write!(
                f,
                "control-plane raft peer transport cluster {cluster_name} has no configured source node {source}",
            ),
            Self::UnknownTarget {
                cluster_name,
                target,
                rpc_name,
            } => write!(
                f,
                "control-plane raft peer transport cluster {cluster_name} has no configured target node {target} for {rpc_name}",
            ),
            Self::EndpointMismatch {
                cluster_name,
                target,
                rpc_name,
                expected,
                actual,
            } => write!(
                f,
                "control-plane raft peer transport cluster {cluster_name} rejected {rpc_name} to node {target}: endpoint mismatch, expected {expected}, got {actual}",
            ),
            Self::AppendEntriesBatchTooLarge {
                cluster_name,
                target,
                entries_len,
                max_append_entries,
            } => write!(
                f,
                "control-plane raft peer transport cluster {cluster_name} rejected append_entries to node {target}: {entries_len} entries exceeds limit {max_append_entries}",
            ),
            Self::AppendEntriesPayloadTooLarge {
                cluster_name,
                target,
                entries_bytes,
                max_append_entries_bytes,
            } => write!(
                f,
                "control-plane raft peer transport cluster {cluster_name} rejected append_entries to node {target}: encoded entries payload {entries_bytes} bytes exceeds limit {max_append_entries_bytes}",
            ),
            Self::SnapshotTooLarge {
                cluster_name,
                target,
                snapshot_bytes,
                max_snapshot_bytes,
            } => write!(
                f,
                "control-plane raft peer transport cluster {cluster_name} rejected full_snapshot to node {target}: {snapshot_bytes} bytes exceeds limit {max_snapshot_bytes}",
            ),
        }
    }
}

impl std::error::Error for ControlPlaneRaftPeerTransportRejection {}

fn raft_rpc_error_from_transport_rejection(
    rejection: ControlPlaneRaftPeerTransportRejection,
) -> RPCError<ControlPlaneRaftTypeConfig> {
    let message = rejection.to_string();
    match rejection {
        ControlPlaneRaftPeerTransportRejection::UnknownTarget { .. } => {
            RPCError::Unreachable(Unreachable::new(&AnyError::error(message)))
        }
        _ => RPCError::Network(NetworkError::from_string(message)),
    }
}

fn raft_streaming_error_from_transport_rejection(
    rejection: ControlPlaneRaftPeerTransportRejection,
) -> StreamingError<ControlPlaneRaftTypeConfig> {
    let message = rejection.to_string();
    match rejection {
        ControlPlaneRaftPeerTransportRejection::UnknownTarget { .. } => {
            StreamingError::Unreachable(Unreachable::new(&AnyError::error(message)))
        }
        _ => StreamingError::Network(NetworkError::from_string(message)),
    }
}

fn raft_streaming_error_from_rpc_error(
    error: RPCError<ControlPlaneRaftTypeConfig>,
) -> StreamingError<ControlPlaneRaftTypeConfig> {
    match error {
        RPCError::Timeout(error) => StreamingError::Network(NetworkError::from_string(format!(
            "control-plane OpenRaft peer validation timed out: {error}"
        ))),
        RPCError::Unreachable(error) => StreamingError::Unreachable(error),
        RPCError::Network(error) => StreamingError::Network(error),
        RPCError::RemoteError(error) => StreamingError::Network(NetworkError::from_string(
            format!("control-plane OpenRaft peer validation remote error: {error}"),
        )),
    }
}

fn raft_rpc_protocol_error(
    context: &'static str,
    error: ControlPlaneError,
) -> RPCError<ControlPlaneRaftTypeConfig> {
    RPCError::Network(NetworkError::from_string(format!(
        "control-plane OpenRaft {context} peer frame failed: {error:?}"
    )))
}

fn raft_streaming_protocol_error(
    context: &'static str,
    error: ControlPlaneError,
) -> StreamingError<ControlPlaneRaftTypeConfig> {
    StreamingError::Network(NetworkError::from_string(format!(
        "control-plane OpenRaft {context} peer frame failed: {error:?}"
    )))
}

fn raft_unix_io_rpc_error(
    context: &'static str,
    target: ControlPlaneRaftNodeId,
    source: io::Error,
) -> RPCError<ControlPlaneRaftTypeConfig> {
    let message = format!(
        "control-plane OpenRaft Unix peer transport {context} for node {target} failed: {source}"
    );
    match source.kind() {
        io::ErrorKind::NotFound
        | io::ErrorKind::ConnectionRefused
        | io::ErrorKind::ConnectionAborted
        | io::ErrorKind::ConnectionReset
        | io::ErrorKind::NotConnected
        | io::ErrorKind::BrokenPipe
        | io::ErrorKind::UnexpectedEof
        | io::ErrorKind::TimedOut => {
            RPCError::Unreachable(Unreachable::new(&AnyError::error(message)))
        }
        _ => RPCError::Network(NetworkError::from_string(message)),
    }
}

fn raft_unix_transport_rpc_error(
    context: &'static str,
    target: ControlPlaneRaftNodeId,
    error: ControlPlaneError,
) -> RPCError<ControlPlaneRaftTypeConfig> {
    match error {
        ControlPlaneError::Io { source, .. } => raft_unix_io_rpc_error(context, target, source),
        error => raft_rpc_protocol_error(context, error),
    }
}

fn raft_unix_transport_streaming_error(
    context: &'static str,
    target: ControlPlaneRaftNodeId,
    error: ControlPlaneError,
) -> StreamingError<ControlPlaneRaftTypeConfig> {
    match raft_unix_transport_rpc_error(context, target, error) {
        RPCError::Unreachable(error) => StreamingError::Unreachable(error),
        RPCError::Network(error) => StreamingError::Network(error),
        RPCError::Timeout(error) => StreamingError::Network(NetworkError::from_string(format!(
            "control-plane OpenRaft Unix peer transport {context} timed out for node {target}: {error}"
        ))),
        RPCError::RemoteError(error) => StreamingError::Network(NetworkError::from_string(
            format!(
                "control-plane OpenRaft Unix peer transport {context} remote error for node {target}: {error}"
            ),
        )),
    }
}

#[derive(Debug, Clone)]
pub struct ControlPlaneRaftUnixPeerNetworkFactory {
    local_node_id: ControlPlaneRaftNodeId,
    policy: Arc<ControlPlaneRaftPeerTransportPolicy>,
    rpc_timeout: Duration,
}

impl ControlPlaneRaftUnixPeerNetworkFactory {
    #[must_use]
    pub fn new(
        local_node_id: ControlPlaneRaftNodeId,
        policy: ControlPlaneRaftPeerTransportPolicy,
        rpc_timeout: Duration,
    ) -> Self {
        Self {
            local_node_id,
            policy: Arc::new(policy),
            rpc_timeout,
        }
    }
}

impl RaftNetworkFactory<ControlPlaneRaftTypeConfig> for ControlPlaneRaftUnixPeerNetworkFactory {
    type Network = ControlPlaneRaftUnixPeerNetwork;

    async fn new_client(
        &mut self,
        target: ControlPlaneRaftNodeId,
        node: &BasicNode,
    ) -> Self::Network {
        ControlPlaneRaftUnixPeerNetwork {
            local_node_id: self.local_node_id,
            target,
            node: node.clone(),
            policy: Arc::clone(&self.policy),
            rpc_timeout: self.rpc_timeout,
        }
    }
}

#[derive(Clone)]
pub struct ControlPlaneRaftUnixPeerNetwork {
    local_node_id: ControlPlaneRaftNodeId,
    target: ControlPlaneRaftNodeId,
    node: BasicNode,
    policy: Arc<ControlPlaneRaftPeerTransportPolicy>,
    rpc_timeout: Duration,
}

impl ControlPlaneRaftUnixPeerNetwork {
    fn encoded_append_entries_payload_len(
        entries: &[ControlPlaneRaftEntry],
    ) -> Result<usize, RPCError<ControlPlaneRaftTypeConfig>> {
        let mut encoded = Vec::new();
        write_raft_u32(
            &mut encoded,
            raft_len_as_u32(entries.len(), "raft append entries")
                .map_err(|error| raft_rpc_protocol_error("append_entries encode", error))?,
        );
        for entry in entries {
            write_raft_entry(&mut encoded, entry)
                .map_err(|error| raft_rpc_protocol_error("append_entries encode", error))?;
        }
        Ok(encoded.len())
    }

    fn request_identity(
        &self,
    ) -> Result<ControlPlaneRaftPeerFrameIdentity, RPCError<ControlPlaneRaftTypeConfig>> {
        self.policy
            .frame_identity(self.local_node_id, self.target)
            .map_err(raft_rpc_error_from_transport_rejection)
    }

    fn response_identity(
        identity: &ControlPlaneRaftPeerFrameIdentity,
    ) -> ControlPlaneRaftPeerFrameIdentity {
        reverse_raft_peer_frame_identity(identity)
    }

    fn connect(
        &self,
        rpc_name: &'static str,
    ) -> Result<UnixStream, RPCError<ControlPlaneRaftTypeConfig>> {
        self.policy
            .validate_target_node(self.target, &self.node, rpc_name)
            .map_err(raft_rpc_error_from_transport_rejection)?;
        let stream = UnixStream::connect(&self.node.addr)
            .map_err(|source| raft_unix_io_rpc_error("connect", self.target, source))?;
        stream
            .set_read_timeout(Some(self.rpc_timeout))
            .map_err(|source| raft_unix_io_rpc_error("set read timeout", self.target, source))?;
        stream
            .set_write_timeout(Some(self.rpc_timeout))
            .map_err(|source| raft_unix_io_rpc_error("set write timeout", self.target, source))?;
        Ok(stream)
    }

    fn send_rpc_frame(
        &self,
        rpc_name: &'static str,
        request: ControlPlaneRaftPeerRpcRequest,
    ) -> Result<ControlPlaneRaftPeerRpcResponse, RPCError<ControlPlaneRaftTypeConfig>> {
        let identity = self.request_identity()?;
        let encoded = request
            .encode_frame_for_peer(&identity)
            .map_err(|error| raft_rpc_protocol_error("encode", error))?;
        let mut stream = self.connect(rpc_name)?;
        write_control_plane_raft_peer_transport_frame(&mut stream, &encoded).map_err(|error| {
            raft_unix_transport_rpc_error("write transport", self.target, error)
        })?;
        let response_frame = read_control_plane_raft_peer_transport_frame(
            &mut stream,
            self.policy.limits.max_frame_bytes,
        )
        .map_err(|error| raft_unix_transport_rpc_error("read transport", self.target, error))?;
        ControlPlaneRaftPeerRpcResponse::decode_frame_for_peer(
            &response_frame,
            &Self::response_identity(&identity),
        )
        .map_err(|error| raft_rpc_protocol_error("response decode", error))
    }

    fn send_snapshot_frame(
        &self,
        vote: VoteOf<ControlPlaneRaftTypeConfig>,
        snapshot: SnapshotOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<
        SnapshotResponse<ControlPlaneRaftTypeConfig>,
        StreamingError<ControlPlaneRaftTypeConfig>,
    > {
        self.policy
            .validate_target_node(self.target, &self.node, "full_snapshot")
            .map_err(raft_streaming_error_from_transport_rejection)?;
        self.policy
            .validate_snapshot(self.target, snapshot.snapshot.get_ref().len())
            .map_err(raft_streaming_error_from_transport_rejection)?;
        let identity = self
            .request_identity()
            .map_err(raft_streaming_error_from_rpc_error)?;
        let encoded = ControlPlaneRaftPeerSnapshotRequest { vote, snapshot }
            .encode_frame_for_peer(&identity)
            .map_err(|error| raft_streaming_protocol_error("full_snapshot encode", error))?;
        let mut stream = self
            .connect("full_snapshot")
            .map_err(raft_streaming_error_from_rpc_error)?;
        write_control_plane_raft_peer_transport_frame(&mut stream, &encoded).map_err(|error| {
            raft_unix_transport_streaming_error("full_snapshot write transport", self.target, error)
        })?;
        let response_frame = read_control_plane_raft_peer_transport_frame(
            &mut stream,
            self.policy.limits.max_frame_bytes,
        )
        .map_err(|error| {
            raft_unix_transport_streaming_error("full_snapshot read transport", self.target, error)
        })?;
        let response = ControlPlaneRaftPeerSnapshotResponse::decode_frame_for_peer(
            &response_frame,
            &Self::response_identity(&identity),
        )
        .map_err(|error| raft_streaming_protocol_error("full_snapshot response decode", error))?;
        Ok(response.response)
    }
}

impl fmt::Debug for ControlPlaneRaftUnixPeerNetwork {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlPlaneRaftUnixPeerNetwork")
            .field("local_node_id", &self.local_node_id)
            .field("target", &self.target)
            .finish_non_exhaustive()
    }
}

impl RaftNetworkV2<ControlPlaneRaftTypeConfig> for ControlPlaneRaftUnixPeerNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<ControlPlaneRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<ControlPlaneRaftTypeConfig>,
        RPCError<ControlPlaneRaftTypeConfig>,
    > {
        self.policy
            .validate_append_entries(
                self.target,
                rpc.entries.len(),
                Self::encoded_append_entries_payload_len(&rpc.entries)?,
            )
            .map_err(raft_rpc_error_from_transport_rejection)?;
        let ControlPlaneRaftPeerRpcResponse::AppendEntries(response) = self.send_rpc_frame(
            "append_entries",
            ControlPlaneRaftPeerRpcRequest::AppendEntries(rpc),
        )?
        else {
            return Err(raft_rpc_protocol_error(
                "append_entries response decode",
                raft_artifact_protocol_error("decoded non-append_entries response frame"),
            ));
        };
        Ok(response)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<ControlPlaneRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<ControlPlaneRaftTypeConfig>, RPCError<ControlPlaneRaftTypeConfig>>
    {
        let ControlPlaneRaftPeerRpcResponse::Vote(response) =
            self.send_rpc_frame("vote", ControlPlaneRaftPeerRpcRequest::Vote(rpc))?
        else {
            return Err(raft_rpc_protocol_error(
                "vote response decode",
                raft_artifact_protocol_error("decoded non-vote response frame"),
            ));
        };
        Ok(response)
    }

    async fn pre_vote(
        &mut self,
        rpc: VoteRequest<ControlPlaneRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<ControlPlaneRaftTypeConfig>, RPCError<ControlPlaneRaftTypeConfig>>
    {
        let ControlPlaneRaftPeerRpcResponse::Vote(response) =
            self.send_rpc_frame("pre_vote", ControlPlaneRaftPeerRpcRequest::PreVote(rpc))?
        else {
            return Err(raft_rpc_protocol_error(
                "pre_vote response decode",
                raft_artifact_protocol_error("decoded non-vote response frame"),
            ));
        };
        Ok(response)
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<ControlPlaneRaftTypeConfig>,
        snapshot: SnapshotOf<ControlPlaneRaftTypeConfig>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<
        SnapshotResponse<ControlPlaneRaftTypeConfig>,
        StreamingError<ControlPlaneRaftTypeConfig>,
    > {
        self.send_snapshot_frame(vote, snapshot)
    }

    async fn transfer_leader(
        &mut self,
        req: TransferLeaderRequest<ControlPlaneRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        TransferLeaderResponse<ControlPlaneRaftTypeConfig>,
        RPCError<ControlPlaneRaftTypeConfig>,
    > {
        let ControlPlaneRaftPeerRpcResponse::TransferLeader(response) = self.send_rpc_frame(
            "transfer_leader",
            ControlPlaneRaftPeerRpcRequest::TransferLeader(req),
        )?
        else {
            return Err(raft_rpc_protocol_error(
                "transfer_leader response decode",
                raft_artifact_protocol_error("decoded non-transfer_leader response frame"),
            ));
        };
        Ok(response)
    }
}

#[derive(Debug)]
pub enum ControlPlaneRaftApplyResponse {
    Blank,
    Membership,
    Applied(ControlPlaneCommandResponse),
    Rejected(ControlPlaneError),
}

#[derive(Debug)]
pub enum ControlPlaneRaftCommandOutcome {
    Applied(ControlPlaneCommandResponse),
    Rejected(ControlPlaneError),
}

#[derive(Debug)]
pub struct SubmittedControlPlaneRaftCommand {
    log_id: LogIdOf<ControlPlaneRaftTypeConfig>,
    outcome: ControlPlaneRaftCommandOutcome,
}

impl SubmittedControlPlaneRaftCommand {
    #[must_use]
    pub fn log_id(&self) -> LogIdOf<ControlPlaneRaftTypeConfig> {
        self.log_id
    }

    #[must_use]
    pub fn outcome(&self) -> &ControlPlaneRaftCommandOutcome {
        &self.outcome
    }

    #[must_use]
    pub fn into_outcome(self) -> ControlPlaneRaftCommandOutcome {
        self.outcome
    }
}

pub struct ControlPlaneRaftAuthority {
    cluster_name: String,
    raft: Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
    log_store: Option<ControlPlaneRaftLogStore>,
}

pub type ControlPlaneRaftFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait ControlPlaneRaftLinearizedCommandSink {
    fn submit_control_plane_command(
        &self,
        command: ControlPlaneCommand,
    ) -> ControlPlaneRaftFuture<'_, Result<SubmittedControlPlaneRaftCommand, ControlPlaneError>>;
}

pub trait ControlPlaneRaftLinearizedRuntimeMapSource {
    fn linearized_runtime_map_snapshot(
        &self,
        issued_at_ms: u64,
    ) -> ControlPlaneRaftFuture<'_, Result<ClusterRuntimeMapSnapshot, ControlPlaneError>>;
}

pub trait ControlPlaneRaftAuthorityStatusSource {
    fn status(
        &self,
    ) -> ControlPlaneRaftFuture<'_, Result<ControlPlaneRaftAuthorityStatus, ControlPlaneError>>;
}

#[derive(Clone)]
pub struct ControlPlaneRaftAuthorityStatusHandle {
    inner: Arc<dyn ControlPlaneRaftAuthorityStatusSource + Send + Sync>,
}

impl ControlPlaneRaftAuthorityStatusHandle {
    pub fn new<T>(status_source: Arc<T>) -> Self
    where
        T: ControlPlaneRaftAuthorityStatusSource + Send + Sync + 'static,
    {
        Self {
            inner: status_source,
        }
    }

    pub fn from_status_source(
        status_source: Arc<dyn ControlPlaneRaftAuthorityStatusSource + Send + Sync>,
    ) -> Self {
        Self {
            inner: status_source,
        }
    }

    #[must_use]
    pub fn as_status_source(
        &self,
    ) -> &(dyn ControlPlaneRaftAuthorityStatusSource + Send + Sync + 'static) {
        &*self.inner
    }

    pub async fn status(&self) -> Result<ControlPlaneRaftAuthorityStatus, ControlPlaneError> {
        self.inner.status().await
    }
}

impl fmt::Debug for ControlPlaneRaftAuthorityStatusHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlPlaneRaftAuthorityStatusHandle")
            .finish_non_exhaustive()
    }
}

impl ControlPlaneRaftAuthorityStatusSource for ControlPlaneRaftAuthorityStatusHandle {
    fn status(
        &self,
    ) -> ControlPlaneRaftFuture<'_, Result<ControlPlaneRaftAuthorityStatus, ControlPlaneError>>
    {
        self.inner.status()
    }
}

pub trait ControlPlaneRaftLeaderRoutedAdmin {
    fn replace_voters(
        &self,
        voters: BTreeSet<ControlPlaneRaftNodeId>,
        retain_removed_voters_as_learners: bool,
    ) -> ControlPlaneRaftFuture<'_, Result<LogIdOf<ControlPlaneRaftTypeConfig>, ControlPlaneError>>;

    fn add_learner(
        &self,
        node_id: ControlPlaneRaftNodeId,
        node: BasicNode,
        wait_for_catch_up: bool,
    ) -> ControlPlaneRaftFuture<'_, Result<LogIdOf<ControlPlaneRaftTypeConfig>, ControlPlaneError>>;

    fn transfer_leadership_to(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> ControlPlaneRaftFuture<'_, Result<(), ControlPlaneError>>;
}

pub trait ControlPlaneRaftAuthorityBootstrap {
    fn initialize_membership(
        &self,
        nodes: BTreeMap<ControlPlaneRaftNodeId, BasicNode>,
    ) -> ControlPlaneRaftFuture<'_, Result<(), ControlPlaneError>>;

    fn is_initialized(&self) -> ControlPlaneRaftFuture<'_, Result<bool, ControlPlaneError>>;
}

pub trait ControlPlaneRaftAuthorityNodeLifecycle {
    fn wait_for_applied_index_at_least(
        &self,
        index: u64,
        timeout: Duration,
        message: &'static str,
    ) -> ControlPlaneRaftFuture<'_, Result<(), ControlPlaneError>>;

    fn wait_for_applied_log_id(
        &self,
        log_id: LogIdOf<ControlPlaneRaftTypeConfig>,
        timeout: Duration,
        message: &'static str,
    ) -> ControlPlaneRaftFuture<'_, Result<(), ControlPlaneError>>;

    fn wait_for_current_leader(
        &self,
        leader_id: ControlPlaneRaftNodeId,
        timeout: Duration,
        message: &'static str,
    ) -> ControlPlaneRaftFuture<'_, Result<(), ControlPlaneError>>;

    fn shutdown(&self) -> ControlPlaneRaftFuture<'_, Result<(), ControlPlaneError>>;
}

pub trait ControlPlaneRaftLinearizedAuthority:
    ControlPlaneRaftLinearizedCommandSink
    + ControlPlaneRaftLinearizedRuntimeMapSource
    + ControlPlaneRaftAuthorityStatusSource
{
}

impl<T> ControlPlaneRaftLinearizedAuthority for T where
    T: ControlPlaneRaftLinearizedCommandSink
        + ControlPlaneRaftLinearizedRuntimeMapSource
        + ControlPlaneRaftAuthorityStatusSource
{
}

pub trait ControlPlaneRaftAuthorityStatusListSource {
    fn authority_statuses(
        &self,
    ) -> ControlPlaneRaftFuture<
        '_,
        Result<
            BTreeMap<ControlPlaneRaftNodeId, ControlPlaneRaftAuthorityStatus>,
            ControlPlaneError,
        >,
    >;
}

pub trait ControlPlaneRaftAuthorityBootstrapDirectory {
    fn authority_bootstrap_for_node(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> ControlPlaneRaftFuture<
        '_,
        Result<ControlPlaneRaftAuthorityBootstrapHandle, ControlPlaneError>,
    >;
}

pub trait ControlPlaneRaftAuthorityNodeLifecycleDirectory {
    fn authority_node_lifecycle_for_node(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> ControlPlaneRaftFuture<
        '_,
        Result<ControlPlaneRaftAuthorityNodeLifecycleHandle, ControlPlaneError>,
    >;
}

pub trait ControlPlaneRaftLinearizedAuthorityDirectory {
    fn linearized_authority_for_node(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> ControlPlaneRaftFuture<'_, Result<ControlPlaneRaftAuthorityHandle, ControlPlaneError>>;
}

pub trait ControlPlaneRaftLeaderRoutedAdminDirectory {
    fn leader_routed_admin_for_node(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> ControlPlaneRaftFuture<
        '_,
        Result<ControlPlaneRaftLeaderRoutedAdminHandle, ControlPlaneError>,
    >;
}

#[derive(Clone)]
pub struct ControlPlaneRaftAuthorityStatusListHandle {
    inner: Arc<dyn ControlPlaneRaftAuthorityStatusListSource + Send + Sync>,
}

impl ControlPlaneRaftAuthorityStatusListHandle {
    pub fn new<T>(status_list: Arc<T>) -> Self
    where
        T: ControlPlaneRaftAuthorityStatusListSource + Send + Sync + 'static,
    {
        Self { inner: status_list }
    }

    pub fn from_status_list(
        status_list: Arc<dyn ControlPlaneRaftAuthorityStatusListSource + Send + Sync>,
    ) -> Self {
        Self { inner: status_list }
    }

    #[must_use]
    pub fn as_status_list(
        &self,
    ) -> &(dyn ControlPlaneRaftAuthorityStatusListSource + Send + Sync + 'static) {
        &*self.inner
    }

    pub async fn authority_statuses(
        &self,
    ) -> Result<BTreeMap<ControlPlaneRaftNodeId, ControlPlaneRaftAuthorityStatus>, ControlPlaneError>
    {
        self.inner.authority_statuses().await
    }
}

impl fmt::Debug for ControlPlaneRaftAuthorityStatusListHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlPlaneRaftAuthorityStatusListHandle")
            .finish_non_exhaustive()
    }
}

impl ControlPlaneRaftAuthorityStatusListSource for ControlPlaneRaftAuthorityStatusListHandle {
    fn authority_statuses(
        &self,
    ) -> ControlPlaneRaftFuture<
        '_,
        Result<
            BTreeMap<ControlPlaneRaftNodeId, ControlPlaneRaftAuthorityStatus>,
            ControlPlaneError,
        >,
    > {
        self.inner.authority_statuses()
    }
}

fn current_serving_authority_node_id(
    statuses: &BTreeMap<ControlPlaneRaftNodeId, ControlPlaneRaftAuthorityStatus>,
) -> Result<ControlPlaneRaftNodeId, ControlPlaneError> {
    let mut serving_node_id = None;
    for (directory_node_id, status) in statuses {
        if *directory_node_id != status.node_id() {
            return Err(ControlPlaneError::RpcRemote {
                message: format!(
                    "raft authority directory status key {} disagrees with reported node {}",
                    directory_node_id,
                    status.node_id()
                ),
            });
        }
        if !status.linearized_authority_serving() {
            continue;
        }
        if let Some(existing_node_id) = serving_node_id {
            return Err(ControlPlaneError::RpcRemote {
                message: format!(
                    "raft authority directory found multiple serving raft authorities: {existing_node_id} and {}",
                    status.node_id()
                ),
            });
        }
        serving_node_id = Some(status.node_id());
    }
    serving_node_id.ok_or_else(|| ControlPlaneError::RpcRemote {
        message: "raft authority directory found no serving raft authority".to_string(),
    })
}

fn validate_selected_linearized_authority_status(
    selected_node_id: ControlPlaneRaftNodeId,
    status: &ControlPlaneRaftAuthorityStatus,
) -> Result<(), ControlPlaneError> {
    if status.node_id() != selected_node_id {
        return Err(ControlPlaneError::RpcRemote {
            message: format!(
                "raft linearized authority directory returned node {} for selected serving node {selected_node_id}",
                status.node_id()
            ),
        });
    }
    if !status.linearized_authority_serving() {
        return Err(ControlPlaneError::RpcRemote {
            message: format!(
                "raft linearized authority directory selected node {selected_node_id}, but it is no longer serving: {:?}",
                status.linearized_authority_readiness()
            ),
        });
    }
    Ok(())
}

#[derive(Clone)]
pub struct ControlPlaneRaftAuthorityBootstrapDirectoryHandle {
    inner: Arc<dyn ControlPlaneRaftAuthorityBootstrapDirectory + Send + Sync>,
}

impl ControlPlaneRaftAuthorityBootstrapDirectoryHandle {
    pub fn new<T>(directory: Arc<T>) -> Self
    where
        T: ControlPlaneRaftAuthorityBootstrapDirectory + Send + Sync + 'static,
    {
        Self { inner: directory }
    }

    pub fn from_bootstrap_directory(
        directory: Arc<dyn ControlPlaneRaftAuthorityBootstrapDirectory + Send + Sync>,
    ) -> Self {
        Self { inner: directory }
    }

    #[must_use]
    pub fn as_bootstrap_directory(
        &self,
    ) -> &(dyn ControlPlaneRaftAuthorityBootstrapDirectory + Send + Sync + 'static) {
        &*self.inner
    }

    pub async fn authority_bootstrap_for_node(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> Result<ControlPlaneRaftAuthorityBootstrapHandle, ControlPlaneError> {
        self.inner.authority_bootstrap_for_node(node_id).await
    }
}

impl fmt::Debug for ControlPlaneRaftAuthorityBootstrapDirectoryHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlPlaneRaftAuthorityBootstrapDirectoryHandle")
            .finish_non_exhaustive()
    }
}

impl ControlPlaneRaftAuthorityBootstrapDirectory
    for ControlPlaneRaftAuthorityBootstrapDirectoryHandle
{
    fn authority_bootstrap_for_node(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> ControlPlaneRaftFuture<
        '_,
        Result<ControlPlaneRaftAuthorityBootstrapHandle, ControlPlaneError>,
    > {
        self.inner.authority_bootstrap_for_node(node_id)
    }
}

#[derive(Clone)]
pub struct ControlPlaneRaftAuthorityNodeLifecycleDirectoryHandle {
    inner: Arc<dyn ControlPlaneRaftAuthorityNodeLifecycleDirectory + Send + Sync>,
}

impl ControlPlaneRaftAuthorityNodeLifecycleDirectoryHandle {
    pub fn new<T>(directory: Arc<T>) -> Self
    where
        T: ControlPlaneRaftAuthorityNodeLifecycleDirectory + Send + Sync + 'static,
    {
        Self { inner: directory }
    }

    pub fn from_node_lifecycle_directory(
        directory: Arc<dyn ControlPlaneRaftAuthorityNodeLifecycleDirectory + Send + Sync>,
    ) -> Self {
        Self { inner: directory }
    }

    #[must_use]
    pub fn as_node_lifecycle_directory(
        &self,
    ) -> &(dyn ControlPlaneRaftAuthorityNodeLifecycleDirectory + Send + Sync + 'static) {
        &*self.inner
    }

    pub async fn authority_node_lifecycle_for_node(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> Result<ControlPlaneRaftAuthorityNodeLifecycleHandle, ControlPlaneError> {
        self.inner.authority_node_lifecycle_for_node(node_id).await
    }
}

impl fmt::Debug for ControlPlaneRaftAuthorityNodeLifecycleDirectoryHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlPlaneRaftAuthorityNodeLifecycleDirectoryHandle")
            .finish_non_exhaustive()
    }
}

impl ControlPlaneRaftAuthorityNodeLifecycleDirectory
    for ControlPlaneRaftAuthorityNodeLifecycleDirectoryHandle
{
    fn authority_node_lifecycle_for_node(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> ControlPlaneRaftFuture<
        '_,
        Result<ControlPlaneRaftAuthorityNodeLifecycleHandle, ControlPlaneError>,
    > {
        self.inner.authority_node_lifecycle_for_node(node_id)
    }
}

#[derive(Clone)]
pub struct ControlPlaneRaftLinearizedAuthorityDirectoryHandle {
    inner: Arc<dyn ControlPlaneRaftLinearizedAuthorityDirectory + Send + Sync>,
}

impl ControlPlaneRaftLinearizedAuthorityDirectoryHandle {
    pub fn new<T>(directory: Arc<T>) -> Self
    where
        T: ControlPlaneRaftLinearizedAuthorityDirectory + Send + Sync + 'static,
    {
        Self { inner: directory }
    }

    pub fn from_linearized_authority_directory(
        directory: Arc<dyn ControlPlaneRaftLinearizedAuthorityDirectory + Send + Sync>,
    ) -> Self {
        Self { inner: directory }
    }

    #[must_use]
    pub fn as_linearized_authority_directory(
        &self,
    ) -> &(dyn ControlPlaneRaftLinearizedAuthorityDirectory + Send + Sync + 'static) {
        &*self.inner
    }

    pub async fn linearized_authority_for_node(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> Result<ControlPlaneRaftAuthorityHandle, ControlPlaneError> {
        self.inner.linearized_authority_for_node(node_id).await
    }
}

impl fmt::Debug for ControlPlaneRaftLinearizedAuthorityDirectoryHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlPlaneRaftLinearizedAuthorityDirectoryHandle")
            .finish_non_exhaustive()
    }
}

impl ControlPlaneRaftLinearizedAuthorityDirectory
    for ControlPlaneRaftLinearizedAuthorityDirectoryHandle
{
    fn linearized_authority_for_node(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> ControlPlaneRaftFuture<'_, Result<ControlPlaneRaftAuthorityHandle, ControlPlaneError>>
    {
        self.inner.linearized_authority_for_node(node_id)
    }
}

#[derive(Clone)]
pub struct ControlPlaneRaftLeaderRoutedAdminDirectoryHandle {
    inner: Arc<dyn ControlPlaneRaftLeaderRoutedAdminDirectory + Send + Sync>,
}

impl ControlPlaneRaftLeaderRoutedAdminDirectoryHandle {
    pub fn new<T>(directory: Arc<T>) -> Self
    where
        T: ControlPlaneRaftLeaderRoutedAdminDirectory + Send + Sync + 'static,
    {
        Self { inner: directory }
    }

    pub fn from_leader_routed_admin_directory(
        directory: Arc<dyn ControlPlaneRaftLeaderRoutedAdminDirectory + Send + Sync>,
    ) -> Self {
        Self { inner: directory }
    }

    #[must_use]
    pub fn as_leader_routed_admin_directory(
        &self,
    ) -> &(dyn ControlPlaneRaftLeaderRoutedAdminDirectory + Send + Sync + 'static) {
        &*self.inner
    }

    pub async fn leader_routed_admin_for_node(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> Result<ControlPlaneRaftLeaderRoutedAdminHandle, ControlPlaneError> {
        self.inner.leader_routed_admin_for_node(node_id).await
    }
}

impl fmt::Debug for ControlPlaneRaftLeaderRoutedAdminDirectoryHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlPlaneRaftLeaderRoutedAdminDirectoryHandle")
            .finish_non_exhaustive()
    }
}

impl ControlPlaneRaftLeaderRoutedAdminDirectory
    for ControlPlaneRaftLeaderRoutedAdminDirectoryHandle
{
    fn leader_routed_admin_for_node(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> ControlPlaneRaftFuture<
        '_,
        Result<ControlPlaneRaftLeaderRoutedAdminHandle, ControlPlaneError>,
    > {
        self.inner.leader_routed_admin_for_node(node_id)
    }
}

#[derive(Clone)]
pub struct ControlPlaneRaftAuthorityHandle {
    inner: Arc<dyn ControlPlaneRaftLinearizedAuthority + Send + Sync>,
}

impl ControlPlaneRaftAuthorityHandle {
    pub fn new<T>(authority: Arc<T>) -> Self
    where
        T: ControlPlaneRaftLinearizedAuthority + Send + Sync + 'static,
    {
        Self { inner: authority }
    }

    pub fn from_linearized_authority(
        authority: Arc<dyn ControlPlaneRaftLinearizedAuthority + Send + Sync>,
    ) -> Self {
        Self { inner: authority }
    }

    #[must_use]
    pub fn as_linearized_authority(
        &self,
    ) -> &(dyn ControlPlaneRaftLinearizedAuthority + Send + Sync + 'static) {
        &*self.inner
    }
}

impl fmt::Debug for ControlPlaneRaftAuthorityHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlPlaneRaftAuthorityHandle")
            .finish_non_exhaustive()
    }
}

impl ControlPlaneRaftLinearizedCommandSink for ControlPlaneRaftAuthorityHandle {
    fn submit_control_plane_command(
        &self,
        command: ControlPlaneCommand,
    ) -> ControlPlaneRaftFuture<'_, Result<SubmittedControlPlaneRaftCommand, ControlPlaneError>>
    {
        self.inner.submit_control_plane_command(command)
    }
}

impl ControlPlaneRaftLinearizedRuntimeMapSource for ControlPlaneRaftAuthorityHandle {
    fn linearized_runtime_map_snapshot(
        &self,
        issued_at_ms: u64,
    ) -> ControlPlaneRaftFuture<'_, Result<ClusterRuntimeMapSnapshot, ControlPlaneError>> {
        self.inner.linearized_runtime_map_snapshot(issued_at_ms)
    }
}

impl ControlPlaneRaftAuthorityStatusSource for ControlPlaneRaftAuthorityHandle {
    fn status(
        &self,
    ) -> ControlPlaneRaftFuture<'_, Result<ControlPlaneRaftAuthorityStatus, ControlPlaneError>>
    {
        self.inner.status()
    }
}

#[derive(Clone)]
pub struct ControlPlaneRaftLeaderRoutedAdminHandle {
    inner: Arc<dyn ControlPlaneRaftLeaderRoutedAdmin + Send + Sync>,
}

impl ControlPlaneRaftLeaderRoutedAdminHandle {
    pub fn new<T>(authority: Arc<T>) -> Self
    where
        T: ControlPlaneRaftLeaderRoutedAdmin + Send + Sync + 'static,
    {
        Self { inner: authority }
    }

    pub fn from_leader_routed_admin(
        authority: Arc<dyn ControlPlaneRaftLeaderRoutedAdmin + Send + Sync>,
    ) -> Self {
        Self { inner: authority }
    }

    #[must_use]
    pub fn as_leader_routed_admin(
        &self,
    ) -> &(dyn ControlPlaneRaftLeaderRoutedAdmin + Send + Sync + 'static) {
        &*self.inner
    }
}

impl fmt::Debug for ControlPlaneRaftLeaderRoutedAdminHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlPlaneRaftLeaderRoutedAdminHandle")
            .finish_non_exhaustive()
    }
}

impl ControlPlaneRaftLeaderRoutedAdmin for ControlPlaneRaftLeaderRoutedAdminHandle {
    fn replace_voters(
        &self,
        voters: BTreeSet<ControlPlaneRaftNodeId>,
        retain_removed_voters_as_learners: bool,
    ) -> ControlPlaneRaftFuture<'_, Result<LogIdOf<ControlPlaneRaftTypeConfig>, ControlPlaneError>>
    {
        self.inner
            .replace_voters(voters, retain_removed_voters_as_learners)
    }

    fn add_learner(
        &self,
        node_id: ControlPlaneRaftNodeId,
        node: BasicNode,
        wait_for_catch_up: bool,
    ) -> ControlPlaneRaftFuture<'_, Result<LogIdOf<ControlPlaneRaftTypeConfig>, ControlPlaneError>>
    {
        self.inner.add_learner(node_id, node, wait_for_catch_up)
    }

    fn transfer_leadership_to(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> ControlPlaneRaftFuture<'_, Result<(), ControlPlaneError>> {
        self.inner.transfer_leadership_to(node_id)
    }
}

#[derive(Clone)]
pub struct ControlPlaneRaftAuthorityBootstrapHandle {
    inner: Arc<dyn ControlPlaneRaftAuthorityBootstrap + Send + Sync>,
}

impl ControlPlaneRaftAuthorityBootstrapHandle {
    pub fn new<T>(authority: Arc<T>) -> Self
    where
        T: ControlPlaneRaftAuthorityBootstrap + Send + Sync + 'static,
    {
        Self { inner: authority }
    }

    pub fn from_bootstrap_authority(
        authority: Arc<dyn ControlPlaneRaftAuthorityBootstrap + Send + Sync>,
    ) -> Self {
        Self { inner: authority }
    }

    #[must_use]
    pub fn as_bootstrap_authority(
        &self,
    ) -> &(dyn ControlPlaneRaftAuthorityBootstrap + Send + Sync + 'static) {
        &*self.inner
    }
}

impl fmt::Debug for ControlPlaneRaftAuthorityBootstrapHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlPlaneRaftAuthorityBootstrapHandle")
            .finish_non_exhaustive()
    }
}

impl ControlPlaneRaftAuthorityBootstrap for ControlPlaneRaftAuthorityBootstrapHandle {
    fn initialize_membership(
        &self,
        nodes: BTreeMap<ControlPlaneRaftNodeId, BasicNode>,
    ) -> ControlPlaneRaftFuture<'_, Result<(), ControlPlaneError>> {
        self.inner.initialize_membership(nodes)
    }

    fn is_initialized(&self) -> ControlPlaneRaftFuture<'_, Result<bool, ControlPlaneError>> {
        self.inner.is_initialized()
    }
}

#[derive(Clone)]
pub struct ControlPlaneRaftAuthorityNodeLifecycleHandle {
    inner: Arc<dyn ControlPlaneRaftAuthorityNodeLifecycle + Send + Sync>,
}

impl ControlPlaneRaftAuthorityNodeLifecycleHandle {
    pub fn new<T>(authority: Arc<T>) -> Self
    where
        T: ControlPlaneRaftAuthorityNodeLifecycle + Send + Sync + 'static,
    {
        Self { inner: authority }
    }

    pub fn from_node_lifecycle_authority(
        authority: Arc<dyn ControlPlaneRaftAuthorityNodeLifecycle + Send + Sync>,
    ) -> Self {
        Self { inner: authority }
    }

    #[must_use]
    pub fn as_node_lifecycle_authority(
        &self,
    ) -> &(dyn ControlPlaneRaftAuthorityNodeLifecycle + Send + Sync + 'static) {
        &*self.inner
    }
}

impl fmt::Debug for ControlPlaneRaftAuthorityNodeLifecycleHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlPlaneRaftAuthorityNodeLifecycleHandle")
            .finish_non_exhaustive()
    }
}

impl ControlPlaneRaftAuthorityNodeLifecycle for ControlPlaneRaftAuthorityNodeLifecycleHandle {
    fn wait_for_applied_index_at_least(
        &self,
        index: u64,
        timeout: Duration,
        message: &'static str,
    ) -> ControlPlaneRaftFuture<'_, Result<(), ControlPlaneError>> {
        self.inner
            .wait_for_applied_index_at_least(index, timeout, message)
    }

    fn wait_for_applied_log_id(
        &self,
        log_id: LogIdOf<ControlPlaneRaftTypeConfig>,
        timeout: Duration,
        message: &'static str,
    ) -> ControlPlaneRaftFuture<'_, Result<(), ControlPlaneError>> {
        self.inner.wait_for_applied_log_id(log_id, timeout, message)
    }

    fn wait_for_current_leader(
        &self,
        leader_id: ControlPlaneRaftNodeId,
        timeout: Duration,
        message: &'static str,
    ) -> ControlPlaneRaftFuture<'_, Result<(), ControlPlaneError>> {
        self.inner
            .wait_for_current_leader(leader_id, timeout, message)
    }

    fn shutdown(&self) -> ControlPlaneRaftFuture<'_, Result<(), ControlPlaneError>> {
        self.inner.shutdown()
    }
}

#[derive(Clone)]
pub struct ControlPlaneRaftAuthorityRoutingHandle {
    observer: ControlPlaneRaftAuthorityStatusHandle,
    status_list: ControlPlaneRaftAuthorityStatusListHandle,
    directory: ControlPlaneRaftLinearizedAuthorityDirectoryHandle,
}

impl ControlPlaneRaftAuthorityRoutingHandle {
    #[must_use]
    pub fn new(
        observer: ControlPlaneRaftAuthorityStatusHandle,
        status_list: ControlPlaneRaftAuthorityStatusListHandle,
        directory: ControlPlaneRaftLinearizedAuthorityDirectoryHandle,
    ) -> Self {
        Self {
            observer,
            status_list,
            directory,
        }
    }

    pub async fn current_serving_linearized_authority(
        &self,
    ) -> Result<ControlPlaneRaftAuthorityHandle, ControlPlaneError> {
        let statuses = self.status_list.authority_statuses().await?;
        let node_id = current_serving_authority_node_id(&statuses)?;
        let authority = self
            .directory
            .linearized_authority_for_node(node_id)
            .await?;
        let status = authority.status().await?;
        validate_selected_linearized_authority_status(node_id, &status)?;
        Ok(authority)
    }

    pub async fn submit_control_plane_command(
        &self,
        command: ControlPlaneCommand,
    ) -> Result<SubmittedControlPlaneRaftCommand, ControlPlaneError> {
        self.current_serving_linearized_authority()
            .await?
            .submit_control_plane_command(command)
            .await
    }

    pub async fn linearized_runtime_map_snapshot(
        &self,
        issued_at_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        self.current_serving_linearized_authority()
            .await?
            .linearized_runtime_map_snapshot(issued_at_ms)
            .await
    }

    pub async fn observer_status(
        &self,
    ) -> Result<ControlPlaneRaftAuthorityStatus, ControlPlaneError> {
        self.observer.status().await
    }

    pub async fn status(&self) -> Result<ControlPlaneRaftAuthorityStatus, ControlPlaneError> {
        self.current_serving_linearized_authority()
            .await?
            .status()
            .await
    }
}

impl fmt::Debug for ControlPlaneRaftAuthorityRoutingHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlPlaneRaftAuthorityRoutingHandle")
            .finish_non_exhaustive()
    }
}

impl ControlPlaneRaftLinearizedCommandSink for ControlPlaneRaftAuthorityRoutingHandle {
    fn submit_control_plane_command(
        &self,
        command: ControlPlaneCommand,
    ) -> ControlPlaneRaftFuture<'_, Result<SubmittedControlPlaneRaftCommand, ControlPlaneError>>
    {
        Box::pin(
            ControlPlaneRaftAuthorityRoutingHandle::submit_control_plane_command(self, command),
        )
    }
}

impl ControlPlaneRaftLinearizedRuntimeMapSource for ControlPlaneRaftAuthorityRoutingHandle {
    fn linearized_runtime_map_snapshot(
        &self,
        issued_at_ms: u64,
    ) -> ControlPlaneRaftFuture<'_, Result<ClusterRuntimeMapSnapshot, ControlPlaneError>> {
        Box::pin(
            ControlPlaneRaftAuthorityRoutingHandle::linearized_runtime_map_snapshot(
                self,
                issued_at_ms,
            ),
        )
    }
}

impl ControlPlaneRaftAuthorityStatusSource for ControlPlaneRaftAuthorityRoutingHandle {
    fn status(
        &self,
    ) -> ControlPlaneRaftFuture<'_, Result<ControlPlaneRaftAuthorityStatus, ControlPlaneError>>
    {
        Box::pin(ControlPlaneRaftAuthorityRoutingHandle::status(self))
    }
}

#[derive(Clone)]
pub struct ControlPlaneRaftLeaderRoutedAdminRoutingHandle {
    status_list: ControlPlaneRaftAuthorityStatusListHandle,
    directory: ControlPlaneRaftLeaderRoutedAdminDirectoryHandle,
}

impl ControlPlaneRaftLeaderRoutedAdminRoutingHandle {
    #[must_use]
    pub fn new(
        status_list: ControlPlaneRaftAuthorityStatusListHandle,
        directory: ControlPlaneRaftLeaderRoutedAdminDirectoryHandle,
    ) -> Self {
        Self {
            status_list,
            directory,
        }
    }

    pub async fn current_serving_leader_routed_admin(
        &self,
    ) -> Result<ControlPlaneRaftLeaderRoutedAdminHandle, ControlPlaneError> {
        let statuses = self.status_list.authority_statuses().await?;
        let node_id = current_serving_authority_node_id(&statuses)?;
        self.directory.leader_routed_admin_for_node(node_id).await
    }

    pub async fn replace_voters(
        &self,
        voters: BTreeSet<ControlPlaneRaftNodeId>,
        retain_removed_voters_as_learners: bool,
    ) -> Result<LogIdOf<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
        self.current_serving_leader_routed_admin()
            .await?
            .replace_voters(voters, retain_removed_voters_as_learners)
            .await
    }

    pub async fn add_learner(
        &self,
        node_id: ControlPlaneRaftNodeId,
        node: BasicNode,
        wait_for_catch_up: bool,
    ) -> Result<LogIdOf<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
        self.current_serving_leader_routed_admin()
            .await?
            .add_learner(node_id, node, wait_for_catch_up)
            .await
    }

    pub async fn transfer_leadership_to(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> Result<(), ControlPlaneError> {
        self.current_serving_leader_routed_admin()
            .await?
            .transfer_leadership_to(node_id)
            .await
    }
}

impl fmt::Debug for ControlPlaneRaftLeaderRoutedAdminRoutingHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlPlaneRaftLeaderRoutedAdminRoutingHandle")
            .finish_non_exhaustive()
    }
}

impl ControlPlaneRaftLeaderRoutedAdmin for ControlPlaneRaftLeaderRoutedAdminRoutingHandle {
    fn replace_voters(
        &self,
        voters: BTreeSet<ControlPlaneRaftNodeId>,
        retain_removed_voters_as_learners: bool,
    ) -> ControlPlaneRaftFuture<'_, Result<LogIdOf<ControlPlaneRaftTypeConfig>, ControlPlaneError>>
    {
        Box::pin(
            ControlPlaneRaftLeaderRoutedAdminRoutingHandle::replace_voters(
                self,
                voters,
                retain_removed_voters_as_learners,
            ),
        )
    }

    fn add_learner(
        &self,
        node_id: ControlPlaneRaftNodeId,
        node: BasicNode,
        wait_for_catch_up: bool,
    ) -> ControlPlaneRaftFuture<'_, Result<LogIdOf<ControlPlaneRaftTypeConfig>, ControlPlaneError>>
    {
        Box::pin(ControlPlaneRaftLeaderRoutedAdminRoutingHandle::add_learner(
            self,
            node_id,
            node,
            wait_for_catch_up,
        ))
    }

    fn transfer_leadership_to(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> ControlPlaneRaftFuture<'_, Result<(), ControlPlaneError>> {
        Box::pin(
            ControlPlaneRaftLeaderRoutedAdminRoutingHandle::transfer_leadership_to(self, node_id),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlPlaneRaftAuthorityStatus {
    node_id: ControlPlaneRaftNodeId,
    current_leader: Option<ControlPlaneRaftNodeId>,
    server_state: ServerState,
    local_leader: bool,
    effective_voter: bool,
    effective_learner: bool,
    applied_voter: bool,
    applied_learner: bool,
    persisted_vote: Option<VoteOf<ControlPlaneRaftTypeConfig>>,
    current_term: Option<ControlPlaneRaftTerm>,
    last_log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    last_purged_log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    committed: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    applied: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    current_snapshot: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    authority_incarnation: AuthorityIncarnation,
    current_cluster_epoch: ClusterEpoch,
    retained_history_count: usize,
    oldest_retained_history_epoch: Option<ClusterEpoch>,
    newest_retained_history_epoch: Option<ClusterEpoch>,
    oldest_storage_history_floor_epoch: Option<ClusterEpoch>,
    storage_node_lease_deadline_count: usize,
    earliest_storage_node_lease_deadline_ms: Option<u64>,
    latest_storage_node_lease_deadline_ms: Option<u64>,
    storage_node_count: usize,
    joining_storage_node_count: usize,
    active_storage_node_count: usize,
    draining_storage_node_count: usize,
    out_storage_node_count: usize,
    removed_storage_node_count: usize,
    healthy_storage_node_count: usize,
    suspect_storage_node_count: usize,
    unavailable_storage_node_count: usize,
    pg_count: usize,
    active_pg_count: usize,
    peering_pg_count: usize,
    degraded_pg_count: usize,
    backfilling_pg_count: usize,
    inconsistent_pg_count: usize,
    active_primary_pg_count: usize,
    peering_metadata_transfer_pg_count: usize,
    metadata_transfer_fenced_pg_count: usize,
    metadata_transfer_fence_source_lease_deadline_count: usize,
    earliest_metadata_transfer_fence_source_lease_deadline_ms: Option<u64>,
    latest_metadata_transfer_fence_source_lease_deadline_ms: Option<u64>,
    effective_membership_log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    effective_voters: BTreeSet<ControlPlaneRaftNodeId>,
    effective_learners: BTreeSet<ControlPlaneRaftNodeId>,
    applied_membership_log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    applied_voters: BTreeSet<ControlPlaneRaftNodeId>,
    applied_learners: BTreeSet<ControlPlaneRaftNodeId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlPlaneRaftLinearizedAuthorityReadiness {
    Serving,
    NotLocalLeader,
    NotEffectiveVoter,
    NotAppliedToCommitted,
}

impl ControlPlaneRaftLinearizedAuthorityReadiness {
    #[must_use]
    pub fn serving(self) -> bool {
        matches!(self, Self::Serving)
    }
}

#[must_use]
fn linearized_authority_readiness_from_flags(
    local_leader: bool,
    effective_voter: bool,
    applied_caught_up_to_committed: bool,
) -> ControlPlaneRaftLinearizedAuthorityReadiness {
    if !local_leader {
        ControlPlaneRaftLinearizedAuthorityReadiness::NotLocalLeader
    } else if !effective_voter {
        ControlPlaneRaftLinearizedAuthorityReadiness::NotEffectiveVoter
    } else if !applied_caught_up_to_committed {
        ControlPlaneRaftLinearizedAuthorityReadiness::NotAppliedToCommitted
    } else {
        ControlPlaneRaftLinearizedAuthorityReadiness::Serving
    }
}

impl ControlPlaneRaftAuthorityStatus {
    #[must_use]
    pub fn node_id(&self) -> ControlPlaneRaftNodeId {
        self.node_id
    }

    #[must_use]
    pub fn current_leader(&self) -> Option<ControlPlaneRaftNodeId> {
        self.current_leader
    }

    #[must_use]
    pub fn server_state(&self) -> ServerState {
        self.server_state
    }

    #[must_use]
    pub fn local_leader(&self) -> bool {
        self.local_leader
    }

    #[must_use]
    pub fn effective_voter(&self) -> bool {
        self.effective_voter
    }

    #[must_use]
    pub fn effective_learner(&self) -> bool {
        self.effective_learner
    }

    #[must_use]
    pub fn applied_voter(&self) -> bool {
        self.applied_voter
    }

    #[must_use]
    pub fn applied_learner(&self) -> bool {
        self.applied_learner
    }

    #[must_use]
    pub fn linearized_authority_serving(&self) -> bool {
        self.linearized_authority_readiness().serving()
    }

    #[must_use]
    pub fn linearized_authority_readiness(&self) -> ControlPlaneRaftLinearizedAuthorityReadiness {
        linearized_authority_readiness_from_flags(
            self.local_leader,
            self.effective_voter,
            self.applied_caught_up_to_committed(),
        )
    }

    #[must_use]
    pub fn persisted_vote(&self) -> Option<VoteOf<ControlPlaneRaftTypeConfig>> {
        self.persisted_vote
    }

    #[must_use]
    pub fn current_term(&self) -> Option<ControlPlaneRaftTerm> {
        self.current_term
    }

    #[must_use]
    pub fn last_log_id(&self) -> Option<LogIdOf<ControlPlaneRaftTypeConfig>> {
        self.last_log_id
    }

    #[must_use]
    pub fn last_log_index(&self) -> Option<u64> {
        self.last_log_id.map(|log_id| log_id.index())
    }

    #[must_use]
    pub fn last_purged_log_id(&self) -> Option<LogIdOf<ControlPlaneRaftTypeConfig>> {
        self.last_purged_log_id
    }

    #[must_use]
    pub fn last_purged_index(&self) -> Option<u64> {
        self.last_purged_log_id.map(|log_id| log_id.index())
    }

    #[must_use]
    pub fn committed(&self) -> Option<LogIdOf<ControlPlaneRaftTypeConfig>> {
        self.committed
    }

    #[must_use]
    pub fn committed_index(&self) -> Option<u64> {
        self.committed.map(|log_id| log_id.index())
    }

    #[must_use]
    pub fn applied(&self) -> Option<LogIdOf<ControlPlaneRaftTypeConfig>> {
        self.applied
    }

    #[must_use]
    pub fn applied_index(&self) -> Option<u64> {
        self.applied.map(|log_id| log_id.index())
    }

    #[must_use]
    pub fn current_snapshot(&self) -> Option<LogIdOf<ControlPlaneRaftTypeConfig>> {
        self.current_snapshot
    }

    #[must_use]
    pub fn current_snapshot_index(&self) -> Option<u64> {
        self.current_snapshot.map(|log_id| log_id.index())
    }

    #[must_use]
    pub fn committed_to_applied_index_gap(&self) -> Option<i128> {
        Some(i128::from(self.committed?.index()) - i128::from(self.applied?.index()))
    }

    #[must_use]
    pub fn last_log_to_committed_index_gap(&self) -> Option<i128> {
        Some(i128::from(self.last_log_id?.index()) - i128::from(self.committed?.index()))
    }

    #[must_use]
    pub fn applied_caught_up_to_committed(&self) -> bool {
        self.committed.is_some() && self.committed == self.applied
    }

    #[must_use]
    pub fn committed_caught_up_to_last_log(&self) -> bool {
        self.last_log_to_committed_index_gap() == Some(0)
    }

    #[must_use]
    pub fn authority_incarnation(&self) -> AuthorityIncarnation {
        self.authority_incarnation
    }

    #[must_use]
    pub fn current_cluster_epoch(&self) -> ClusterEpoch {
        self.current_cluster_epoch
    }

    #[must_use]
    pub fn retained_history_count(&self) -> usize {
        self.retained_history_count
    }

    #[must_use]
    pub fn oldest_retained_history_epoch(&self) -> Option<ClusterEpoch> {
        self.oldest_retained_history_epoch
    }

    #[must_use]
    pub fn newest_retained_history_epoch(&self) -> Option<ClusterEpoch> {
        self.newest_retained_history_epoch
    }

    #[must_use]
    pub fn oldest_storage_history_floor_epoch(&self) -> Option<ClusterEpoch> {
        self.oldest_storage_history_floor_epoch
    }

    #[must_use]
    pub fn storage_node_lease_deadline_count(&self) -> usize {
        self.storage_node_lease_deadline_count
    }

    #[must_use]
    pub fn earliest_storage_node_lease_deadline_ms(&self) -> Option<u64> {
        self.earliest_storage_node_lease_deadline_ms
    }

    #[must_use]
    pub fn latest_storage_node_lease_deadline_ms(&self) -> Option<u64> {
        self.latest_storage_node_lease_deadline_ms
    }

    #[must_use]
    pub fn storage_node_count(&self) -> usize {
        self.storage_node_count
    }

    #[must_use]
    pub fn joining_storage_node_count(&self) -> usize {
        self.joining_storage_node_count
    }

    #[must_use]
    pub fn active_storage_node_count(&self) -> usize {
        self.active_storage_node_count
    }

    #[must_use]
    pub fn draining_storage_node_count(&self) -> usize {
        self.draining_storage_node_count
    }

    #[must_use]
    pub fn out_storage_node_count(&self) -> usize {
        self.out_storage_node_count
    }

    #[must_use]
    pub fn removed_storage_node_count(&self) -> usize {
        self.removed_storage_node_count
    }

    #[must_use]
    pub fn healthy_storage_node_count(&self) -> usize {
        self.healthy_storage_node_count
    }

    #[must_use]
    pub fn suspect_storage_node_count(&self) -> usize {
        self.suspect_storage_node_count
    }

    #[must_use]
    pub fn unavailable_storage_node_count(&self) -> usize {
        self.unavailable_storage_node_count
    }

    #[must_use]
    pub fn pg_count(&self) -> usize {
        self.pg_count
    }

    #[must_use]
    pub fn active_pg_count(&self) -> usize {
        self.active_pg_count
    }

    #[must_use]
    pub fn peering_pg_count(&self) -> usize {
        self.peering_pg_count
    }

    #[must_use]
    pub fn degraded_pg_count(&self) -> usize {
        self.degraded_pg_count
    }

    #[must_use]
    pub fn backfilling_pg_count(&self) -> usize {
        self.backfilling_pg_count
    }

    #[must_use]
    pub fn inconsistent_pg_count(&self) -> usize {
        self.inconsistent_pg_count
    }

    #[must_use]
    pub fn active_primary_pg_count(&self) -> usize {
        self.active_primary_pg_count
    }

    #[must_use]
    pub fn peering_metadata_transfer_pg_count(&self) -> usize {
        self.peering_metadata_transfer_pg_count
    }

    #[must_use]
    pub fn metadata_transfer_fenced_pg_count(&self) -> usize {
        self.metadata_transfer_fenced_pg_count
    }

    #[must_use]
    pub fn metadata_transfer_fence_source_lease_deadline_count(&self) -> usize {
        self.metadata_transfer_fence_source_lease_deadline_count
    }

    #[must_use]
    pub fn earliest_metadata_transfer_fence_source_lease_deadline_ms(&self) -> Option<u64> {
        self.earliest_metadata_transfer_fence_source_lease_deadline_ms
    }

    #[must_use]
    pub fn latest_metadata_transfer_fence_source_lease_deadline_ms(&self) -> Option<u64> {
        self.latest_metadata_transfer_fence_source_lease_deadline_ms
    }

    #[must_use]
    pub fn effective_membership_log_id(&self) -> Option<LogIdOf<ControlPlaneRaftTypeConfig>> {
        self.effective_membership_log_id
    }

    #[must_use]
    pub fn effective_voters(&self) -> &BTreeSet<ControlPlaneRaftNodeId> {
        &self.effective_voters
    }

    #[must_use]
    pub fn effective_learners(&self) -> &BTreeSet<ControlPlaneRaftNodeId> {
        &self.effective_learners
    }

    #[must_use]
    pub fn applied_membership_log_id(&self) -> Option<LogIdOf<ControlPlaneRaftTypeConfig>> {
        self.applied_membership_log_id
    }

    #[must_use]
    pub fn applied_voters(&self) -> &BTreeSet<ControlPlaneRaftNodeId> {
        &self.applied_voters
    }

    #[must_use]
    pub fn applied_learners(&self) -> &BTreeSet<ControlPlaneRaftNodeId> {
        &self.applied_learners
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct ExperimentalSingleNodeRaftNetworkFactory;

impl RaftNetworkFactory<ControlPlaneRaftTypeConfig> for ExperimentalSingleNodeRaftNetworkFactory {
    type Network = ExperimentalSingleNodeRaftNetwork;

    async fn new_client(
        &mut self,
        target: ControlPlaneRaftNodeId,
        _node: &BasicNode,
    ) -> Self::Network {
        ExperimentalSingleNodeRaftNetwork { target }
    }
}

#[derive(Debug, Clone, Copy)]
struct ExperimentalSingleNodeRaftNetwork {
    target: ControlPlaneRaftNodeId,
}

impl ExperimentalSingleNodeRaftNetwork {
    fn unreachable(&self, rpc_name: &'static str) -> Unreachable<ControlPlaneRaftTypeConfig> {
        Unreachable::new(&AnyError::error(format!(
            "experimental single-node control-plane raft network has no remote target {} for {rpc_name}",
            self.target
        )))
    }
}

impl RaftNetworkV2<ControlPlaneRaftTypeConfig> for ExperimentalSingleNodeRaftNetwork {
    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<ControlPlaneRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<ControlPlaneRaftTypeConfig>,
        RPCError<ControlPlaneRaftTypeConfig>,
    > {
        Err(RPCError::Unreachable(self.unreachable("append_entries")))
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<ControlPlaneRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<ControlPlaneRaftTypeConfig>, RPCError<ControlPlaneRaftTypeConfig>>
    {
        Err(RPCError::Unreachable(self.unreachable("vote")))
    }

    async fn full_snapshot(
        &mut self,
        _vote: VoteOf<ControlPlaneRaftTypeConfig>,
        _snapshot: SnapshotOf<ControlPlaneRaftTypeConfig>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<
        SnapshotResponse<ControlPlaneRaftTypeConfig>,
        StreamingError<ControlPlaneRaftTypeConfig>,
    > {
        Err(StreamingError::Unreachable(
            self.unreachable("full_snapshot"),
        ))
    }
}

fn experimental_single_node_raft_config(
    cluster_name: impl Into<String>,
) -> Result<Arc<Config>, ControlPlaneError> {
    Ok(Arc::new(
        Config {
            cluster_name: cluster_name.into(),
            heartbeat_interval: 50,
            election_timeout_min: 150,
            election_timeout_max: 300,
            enable_tick: false,
            enable_heartbeat: false,
            enable_elect: false,
            ..Default::default()
        }
        .validate()
        .map_err(|error| ControlPlaneError::RpcRemote {
            message: format!("OpenRaft experimental single-node config failed: {error}"),
        })?,
    ))
}

impl ControlPlaneRaftAuthority {
    pub async fn new_experimental_single_node_in_memory(
        cluster_name: impl Into<String>,
        node_id: ControlPlaneRaftNodeId,
    ) -> Result<Self, ControlPlaneError> {
        let cluster_name = cluster_name.into();
        let config = experimental_single_node_raft_config(cluster_name.clone())?;
        let log_store = ControlPlaneRaftLogStore::empty();
        let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            node_id,
            config,
            ExperimentalSingleNodeRaftNetworkFactory,
            log_store.clone(),
            ControlPlaneRaftStateMachine::empty(),
        )
        .await
        .map_err(|error| openraft_remote_error("new experimental single-node authority", error))?;
        Ok(Self::new_with_log_store(raft, log_store, cluster_name))
    }

    pub async fn new_experimental_single_node_durable(
        cluster_name: impl Into<String>,
        node_id: ControlPlaneRaftNodeId,
        artifact_path: &Path,
    ) -> Result<Self, ControlPlaneError> {
        let cluster_name = cluster_name.into();
        let config = experimental_single_node_raft_config(cluster_name.clone())?;
        let (log_store, state_machine) =
            match ControlPlaneRaftRestartArtifact::load_durable_artifact(artifact_path) {
                Ok(artifact) => {
                    artifact.validate_cluster_identity(&cluster_name)?;
                    artifact.validate_single_node_local_identity(node_id)?;
                    artifact.restore().map_err(|source| ControlPlaneError::Io {
                        context: "restore control-plane OpenRaft durable restart artifact",
                        source,
                    })?
                }
                Err(ControlPlaneError::Io { source, .. })
                    if source.kind() == io::ErrorKind::NotFound =>
                {
                    (
                        ControlPlaneRaftLogStore::empty(),
                        ControlPlaneRaftStateMachine::empty(),
                    )
                }
                Err(error) => return Err(error),
            };
        let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            node_id,
            config,
            ExperimentalSingleNodeRaftNetworkFactory,
            log_store.clone(),
            state_machine,
        )
        .await
        .map_err(|error| {
            openraft_remote_error("new experimental durable single-node authority", error)
        })?;
        Ok(Self::new_with_log_store(raft, log_store, cluster_name))
    }

    #[must_use]
    pub fn new(raft: Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>) -> Self {
        Self {
            cluster_name: String::new(),
            raft,
            log_store: None,
        }
    }

    #[must_use]
    pub fn new_with_log_store(
        raft: Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
        log_store: ControlPlaneRaftLogStore,
        cluster_name: impl Into<String>,
    ) -> Self {
        Self {
            cluster_name: cluster_name.into(),
            raft,
            log_store: Some(log_store),
        }
    }

    #[must_use]
    pub fn raft(&self) -> &Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine> {
        &self.raft
    }

    pub async fn initialize_membership(
        &self,
        nodes: BTreeMap<ControlPlaneRaftNodeId, BasicNode>,
    ) -> Result<(), ControlPlaneError> {
        self.raft
            .initialize(nodes)
            .await
            .map_err(|error| openraft_remote_error("initialize", error))?;
        Ok(())
    }

    pub async fn initialize_single_node_membership(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> Result<(), ControlPlaneError> {
        let mut nodes = BTreeMap::new();
        nodes.insert(node_id, BasicNode::default());
        self.initialize_membership(nodes).await
    }

    pub async fn is_initialized(&self) -> Result<bool, ControlPlaneError> {
        self.raft
            .is_initialized()
            .await
            .map_err(|error| openraft_remote_error("is-initialized", error))
    }

    pub async fn replace_voters(
        &self,
        voters: BTreeSet<ControlPlaneRaftNodeId>,
        retain_removed_voters_as_learners: bool,
    ) -> Result<LogIdOf<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
        let response = self
            .raft
            .change_membership(voters, retain_removed_voters_as_learners)
            .await
            .map_err(|error| openraft_remote_error("change-membership", error))?;
        Ok(response.log_id)
    }

    pub async fn add_learner(
        &self,
        node_id: ControlPlaneRaftNodeId,
        node: BasicNode,
        wait_for_catch_up: bool,
    ) -> Result<LogIdOf<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
        let response = self
            .raft
            .add_learner(node_id, node, wait_for_catch_up)
            .await
            .map_err(|error| openraft_remote_error("add-learner", error))?;
        Ok(response.log_id)
    }

    pub async fn transfer_leadership_to(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> Result<(), ControlPlaneError> {
        self.raft
            .trigger()
            .transfer_leader(node_id)
            .await
            .map_err(|error| openraft_remote_error("transfer-leader", error))
    }

    pub async fn wait_for_applied_index_at_least(
        &self,
        index: u64,
        timeout: Duration,
        message: &'static str,
    ) -> Result<(), ControlPlaneError> {
        self.raft
            .wait(Some(timeout))
            .applied_index_at_least(Some(index), message)
            .await
            .map(|_| ())
            .map_err(|error| openraft_remote_error("wait-applied-index", error))
    }

    pub async fn wait_for_applied_log_id(
        &self,
        log_id: LogIdOf<ControlPlaneRaftTypeConfig>,
        timeout: Duration,
        message: &'static str,
    ) -> Result<(), ControlPlaneError> {
        ControlPlaneRaftTypeConfig::timeout(timeout, async {
            loop {
                let applied = self
                    .raft
                    .with_state_machine(|state_machine| {
                        let applied = state_machine.last_applied();
                        Box::pin(async move { applied })
                    })
                    .await
                    .map_err(|error| openraft_remote_error("wait-applied-log-id", error))?;
                if let Some(applied) = applied {
                    if applied.index() > log_id.index() {
                        return Ok(());
                    }
                    if applied.index() == log_id.index() {
                        if applied == log_id {
                            return Ok(());
                        }
                        return Err(ControlPlaneError::RpcRemote {
                            message: format!(
                                "OpenRaft wait-applied-log-id observed mismatched log id: \
                                 applied={applied:?}, expected={log_id:?}: {message}"
                            ),
                        });
                    }
                }
                ControlPlaneRaftTypeConfig::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| ControlPlaneError::RpcRemote {
            message: format!("OpenRaft wait-applied-log-id timed out after {timeout:?}: {message}"),
        })?
    }

    pub async fn wait_for_current_leader(
        &self,
        leader_id: ControlPlaneRaftNodeId,
        timeout: Duration,
        message: &'static str,
    ) -> Result<(), ControlPlaneError> {
        self.raft
            .wait(Some(timeout))
            .current_leader(leader_id, message)
            .await
            .map(|_| ())
            .map_err(|error| openraft_remote_error("wait-current-leader", error))
    }

    pub async fn submit_control_plane_command(
        &self,
        command: ControlPlaneCommand,
    ) -> Result<SubmittedControlPlaneRaftCommand, ControlPlaneError> {
        submit_control_plane_command_via_openraft(&self.raft, command).await
    }

    pub async fn linearized_runtime_map_snapshot(
        &self,
        issued_at_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        runtime_map_via_openraft_read_index(&self.raft, issued_at_ms).await
    }

    pub async fn current_control_plane_snapshot(
        &self,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        self.raft
            .with_state_machine(|state_machine| {
                let snapshot = state_machine.inner().snapshot().clone();
                Box::pin(async move { snapshot })
            })
            .await
            .map_err(|error| openraft_remote_error("state-machine snapshot read", error))
    }

    pub async fn store_durable_restart_artifact(
        &self,
        path: &Path,
    ) -> Result<(), ControlPlaneError> {
        let log_store = self
            .log_store
            .as_ref()
            .ok_or_else(|| ControlPlaneError::RpcRemote {
                message: "OpenRaft durable restart artifact requested without retained log store"
                    .to_string(),
            })?;
        let log_store =
            log_store
                .export_restart_artifact()
                .map_err(|source| ControlPlaneError::Io {
                    context: "export control-plane OpenRaft durable log-store restart artifact",
                    source,
                })?;
        let state_machine = self
            .raft
            .with_state_machine(|state_machine| {
                let artifact = state_machine.export_restart_artifact();
                Box::pin(async move { artifact })
            })
            .await
            .map_err(|error| openraft_remote_error("state-machine restart artifact read", error))?;
        ControlPlaneRaftRestartArtifact {
            cluster_name: self.cluster_name.clone(),
            log_store,
            state_machine,
        }
        .store_durable_artifact(path)
    }

    pub async fn status(&self) -> Result<ControlPlaneRaftAuthorityStatus, ControlPlaneError> {
        let node_id = *self.raft.node_id();
        let current_leader = self.raft.current_leader().await;
        let persisted_vote = self
            .log_store
            .as_ref()
            .map(ControlPlaneRaftLogStore::persisted_vote)
            .transpose()
            .map_err(|error| openraft_remote_error("status log-store vote read", error))?
            .flatten();
        let current_term = persisted_vote.map(|vote| vote.leader_id.term);
        let last_purged_log_id = self
            .log_store
            .as_ref()
            .map(ControlPlaneRaftLogStore::last_purged_log_id)
            .transpose()
            .map_err(|error| openraft_remote_error("status log-store read", error))?
            .flatten();
        let (
            last_log_id,
            committed,
            server_state,
            effective_membership_log_id,
            effective_voters,
            effective_learners,
        ) = self
            .raft
            .with_raft_state(|state| {
                let effective_membership = state.membership_state.effective();
                (
                    state.log_ids.last().cloned(),
                    state.local_committed().cloned(),
                    state.server_state,
                    *effective_membership.log_id(),
                    effective_membership
                        .membership()
                        .voter_ids()
                        .collect::<BTreeSet<_>>(),
                    effective_membership
                        .membership()
                        .learner_ids()
                        .collect::<BTreeSet<_>>(),
                )
            })
            .await
            .map_err(|error| openraft_remote_error("status raft-state read", error))?;
        let (
            applied,
            current_snapshot,
            authority_incarnation,
            current_cluster_epoch,
            retained_history_count,
            oldest_retained_history_epoch,
            newest_retained_history_epoch,
            oldest_storage_history_floor_epoch,
            storage_node_lease_deadline_count,
            earliest_storage_node_lease_deadline_ms,
            latest_storage_node_lease_deadline_ms,
            storage_node_count,
            joining_storage_node_count,
            active_storage_node_count,
            draining_storage_node_count,
            out_storage_node_count,
            removed_storage_node_count,
            healthy_storage_node_count,
            suspect_storage_node_count,
            unavailable_storage_node_count,
            pg_count,
            active_pg_count,
            peering_pg_count,
            degraded_pg_count,
            backfilling_pg_count,
            inconsistent_pg_count,
            active_primary_pg_count,
            peering_metadata_transfer_pg_count,
            metadata_transfer_fenced_pg_count,
            metadata_transfer_fence_source_lease_deadline_count,
            earliest_metadata_transfer_fence_source_lease_deadline_ms,
            latest_metadata_transfer_fence_source_lease_deadline_ms,
            applied_membership_log_id,
            applied_voters,
            applied_learners,
        ) = self
            .raft
            .with_state_machine(|state_machine| {
                let last_applied = state_machine.last_applied();
                let current_snapshot = state_machine
                    .current_snapshot()
                    .and_then(|snapshot| snapshot.meta.last_log_id);
                let snapshot = state_machine.inner().snapshot();
                let authority_incarnation = snapshot.authority_incarnation();
                let current_cluster_epoch = snapshot.cluster_epoch();
                let retained_history_count = snapshot.cluster_map_history().len();
                let oldest_retained_history_epoch = snapshot
                    .cluster_map_history()
                    .first()
                    .map(|record| record.cluster_epoch());
                let newest_retained_history_epoch = snapshot
                    .cluster_map_history()
                    .last()
                    .map(|record| record.cluster_epoch());
                let oldest_storage_history_floor_epoch = snapshot
                    .nodes()
                    .filter_map(|node| node.cluster_map_history_floor_epoch())
                    .min();
                let mut storage_node_lease_deadline_count = 0;
                let mut earliest_storage_node_lease_deadline_ms = None;
                let mut latest_storage_node_lease_deadline_ms = None;
                let mut storage_node_count = 0;
                let mut joining_storage_node_count = 0;
                let mut active_storage_node_count = 0;
                let mut draining_storage_node_count = 0;
                let mut out_storage_node_count = 0;
                let mut removed_storage_node_count = 0;
                let mut healthy_storage_node_count = 0;
                let mut suspect_storage_node_count = 0;
                let mut unavailable_storage_node_count = 0;
                for node in snapshot.nodes() {
                    storage_node_count += 1;
                    match node.membership() {
                        NodeMembershipState::Joining => joining_storage_node_count += 1,
                        NodeMembershipState::Active => active_storage_node_count += 1,
                        NodeMembershipState::Draining => draining_storage_node_count += 1,
                        NodeMembershipState::Out => out_storage_node_count += 1,
                        NodeMembershipState::Removed => removed_storage_node_count += 1,
                    }
                    match node.availability() {
                        NodeAvailabilityState::Healthy => healthy_storage_node_count += 1,
                        NodeAvailabilityState::Suspect => suspect_storage_node_count += 1,
                        NodeAvailabilityState::Unavailable => unavailable_storage_node_count += 1,
                    }
                    if let Some(deadline_ms) = node.lease_deadline_ms() {
                        observe_deadline_range(
                            deadline_ms,
                            &mut storage_node_lease_deadline_count,
                            &mut earliest_storage_node_lease_deadline_ms,
                            &mut latest_storage_node_lease_deadline_ms,
                        );
                    }
                }
                let mut pg_count = 0;
                let mut active_pg_count = 0;
                let mut peering_pg_count = 0;
                let mut degraded_pg_count = 0;
                let mut backfilling_pg_count = 0;
                let mut inconsistent_pg_count = 0;
                let mut active_primary_pg_count = 0;
                let mut peering_metadata_transfer_pg_count = 0;
                let mut metadata_transfer_fenced_pg_count = 0;
                let mut metadata_transfer_fence_source_lease_deadline_count = 0;
                let mut earliest_metadata_transfer_fence_source_lease_deadline_ms = None;
                let mut latest_metadata_transfer_fence_source_lease_deadline_ms = None;
                for pg in snapshot.pgs() {
                    pg_count += 1;
                    match pg.state() {
                        PgState::Active => active_pg_count += 1,
                        PgState::Peering => peering_pg_count += 1,
                        PgState::Degraded => degraded_pg_count += 1,
                        PgState::Backfilling => backfilling_pg_count += 1,
                        PgState::Inconsistent => inconsistent_pg_count += 1,
                    }
                    if pg.active_primary().is_some() {
                        active_primary_pg_count += 1;
                    }
                    if pg.peering_metadata_transfer().is_some() {
                        peering_metadata_transfer_pg_count += 1;
                    }
                    if pg.metadata_transfer_fenced() {
                        metadata_transfer_fenced_pg_count += 1;
                    }
                    if let Some(deadline_ms) = pg.metadata_transfer_fence_source_lease_deadline_ms()
                    {
                        observe_deadline_range(
                            deadline_ms,
                            &mut metadata_transfer_fence_source_lease_deadline_count,
                            &mut earliest_metadata_transfer_fence_source_lease_deadline_ms,
                            &mut latest_metadata_transfer_fence_source_lease_deadline_ms,
                        );
                    }
                }
                let membership = state_machine.last_membership();
                let membership_log_id = *membership.log_id();
                let voters = membership.membership().voter_ids().collect::<BTreeSet<_>>();
                let learners = membership
                    .membership()
                    .learner_ids()
                    .collect::<BTreeSet<_>>();
                Box::pin(async move {
                    (
                        last_applied,
                        current_snapshot,
                        authority_incarnation,
                        current_cluster_epoch,
                        retained_history_count,
                        oldest_retained_history_epoch,
                        newest_retained_history_epoch,
                        oldest_storage_history_floor_epoch,
                        storage_node_lease_deadline_count,
                        earliest_storage_node_lease_deadline_ms,
                        latest_storage_node_lease_deadline_ms,
                        storage_node_count,
                        joining_storage_node_count,
                        active_storage_node_count,
                        draining_storage_node_count,
                        out_storage_node_count,
                        removed_storage_node_count,
                        healthy_storage_node_count,
                        suspect_storage_node_count,
                        unavailable_storage_node_count,
                        pg_count,
                        active_pg_count,
                        peering_pg_count,
                        degraded_pg_count,
                        backfilling_pg_count,
                        inconsistent_pg_count,
                        active_primary_pg_count,
                        peering_metadata_transfer_pg_count,
                        metadata_transfer_fenced_pg_count,
                        metadata_transfer_fence_source_lease_deadline_count,
                        earliest_metadata_transfer_fence_source_lease_deadline_ms,
                        latest_metadata_transfer_fence_source_lease_deadline_ms,
                        membership_log_id,
                        voters,
                        learners,
                    )
                })
            })
            .await
            .map_err(|error| openraft_remote_error("status state-machine read", error))?;
        let local_leader = server_state == ServerState::Leader && current_leader == Some(node_id);
        let effective_voter = effective_voters.contains(&node_id);
        let effective_learner = effective_learners.contains(&node_id);
        let applied_voter = applied_voters.contains(&node_id);
        let applied_learner = applied_learners.contains(&node_id);
        Ok(ControlPlaneRaftAuthorityStatus {
            node_id,
            current_leader,
            server_state,
            local_leader,
            effective_voter,
            effective_learner,
            applied_voter,
            applied_learner,
            persisted_vote,
            current_term,
            last_log_id,
            last_purged_log_id,
            committed,
            applied,
            current_snapshot,
            authority_incarnation,
            current_cluster_epoch,
            retained_history_count,
            oldest_retained_history_epoch,
            newest_retained_history_epoch,
            oldest_storage_history_floor_epoch,
            storage_node_lease_deadline_count,
            earliest_storage_node_lease_deadline_ms,
            latest_storage_node_lease_deadline_ms,
            storage_node_count,
            joining_storage_node_count,
            active_storage_node_count,
            draining_storage_node_count,
            out_storage_node_count,
            removed_storage_node_count,
            healthy_storage_node_count,
            suspect_storage_node_count,
            unavailable_storage_node_count,
            pg_count,
            active_pg_count,
            peering_pg_count,
            degraded_pg_count,
            backfilling_pg_count,
            inconsistent_pg_count,
            active_primary_pg_count,
            peering_metadata_transfer_pg_count,
            metadata_transfer_fenced_pg_count,
            metadata_transfer_fence_source_lease_deadline_count,
            earliest_metadata_transfer_fence_source_lease_deadline_ms,
            latest_metadata_transfer_fence_source_lease_deadline_ms,
            effective_membership_log_id,
            effective_voters,
            effective_learners,
            applied_membership_log_id,
            applied_voters,
            applied_learners,
        })
    }

    pub async fn shutdown(&self) -> Result<(), ControlPlaneError> {
        self.raft
            .shutdown()
            .await
            .map_err(|error| openraft_remote_error("shutdown", error))
    }
}

impl ControlPlaneRaftLinearizedCommandSink for ControlPlaneRaftAuthority {
    fn submit_control_plane_command(
        &self,
        command: ControlPlaneCommand,
    ) -> ControlPlaneRaftFuture<'_, Result<SubmittedControlPlaneRaftCommand, ControlPlaneError>>
    {
        Box::pin(async move {
            ControlPlaneRaftAuthority::submit_control_plane_command(self, command).await
        })
    }
}

impl ControlPlaneRaftLinearizedRuntimeMapSource for ControlPlaneRaftAuthority {
    fn linearized_runtime_map_snapshot(
        &self,
        issued_at_ms: u64,
    ) -> ControlPlaneRaftFuture<'_, Result<ClusterRuntimeMapSnapshot, ControlPlaneError>> {
        Box::pin(async move {
            ControlPlaneRaftAuthority::linearized_runtime_map_snapshot(self, issued_at_ms).await
        })
    }
}

impl ControlPlaneRaftAuthorityStatusSource for ControlPlaneRaftAuthority {
    fn status(
        &self,
    ) -> ControlPlaneRaftFuture<'_, Result<ControlPlaneRaftAuthorityStatus, ControlPlaneError>>
    {
        Box::pin(async move { ControlPlaneRaftAuthority::status(self).await })
    }
}

impl ControlPlaneRaftLeaderRoutedAdmin for ControlPlaneRaftAuthority {
    fn replace_voters(
        &self,
        voters: BTreeSet<ControlPlaneRaftNodeId>,
        retain_removed_voters_as_learners: bool,
    ) -> ControlPlaneRaftFuture<'_, Result<LogIdOf<ControlPlaneRaftTypeConfig>, ControlPlaneError>>
    {
        Box::pin(async move {
            ControlPlaneRaftAuthority::replace_voters(
                self,
                voters,
                retain_removed_voters_as_learners,
            )
            .await
        })
    }

    fn add_learner(
        &self,
        node_id: ControlPlaneRaftNodeId,
        node: BasicNode,
        wait_for_catch_up: bool,
    ) -> ControlPlaneRaftFuture<'_, Result<LogIdOf<ControlPlaneRaftTypeConfig>, ControlPlaneError>>
    {
        Box::pin(async move {
            ControlPlaneRaftAuthority::add_learner(self, node_id, node, wait_for_catch_up).await
        })
    }

    fn transfer_leadership_to(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> ControlPlaneRaftFuture<'_, Result<(), ControlPlaneError>> {
        Box::pin(
            async move { ControlPlaneRaftAuthority::transfer_leadership_to(self, node_id).await },
        )
    }
}

impl ControlPlaneRaftAuthorityBootstrap for ControlPlaneRaftAuthority {
    fn initialize_membership(
        &self,
        nodes: BTreeMap<ControlPlaneRaftNodeId, BasicNode>,
    ) -> ControlPlaneRaftFuture<'_, Result<(), ControlPlaneError>> {
        Box::pin(async move { ControlPlaneRaftAuthority::initialize_membership(self, nodes).await })
    }

    fn is_initialized(&self) -> ControlPlaneRaftFuture<'_, Result<bool, ControlPlaneError>> {
        Box::pin(async move { ControlPlaneRaftAuthority::is_initialized(self).await })
    }
}

impl ControlPlaneRaftAuthorityNodeLifecycle for ControlPlaneRaftAuthority {
    fn wait_for_applied_index_at_least(
        &self,
        index: u64,
        timeout: Duration,
        message: &'static str,
    ) -> ControlPlaneRaftFuture<'_, Result<(), ControlPlaneError>> {
        Box::pin(async move {
            ControlPlaneRaftAuthority::wait_for_applied_index_at_least(
                self, index, timeout, message,
            )
            .await
        })
    }

    fn wait_for_applied_log_id(
        &self,
        log_id: LogIdOf<ControlPlaneRaftTypeConfig>,
        timeout: Duration,
        message: &'static str,
    ) -> ControlPlaneRaftFuture<'_, Result<(), ControlPlaneError>> {
        Box::pin(async move {
            ControlPlaneRaftAuthority::wait_for_applied_log_id(self, log_id, timeout, message).await
        })
    }

    fn wait_for_current_leader(
        &self,
        leader_id: ControlPlaneRaftNodeId,
        timeout: Duration,
        message: &'static str,
    ) -> ControlPlaneRaftFuture<'_, Result<(), ControlPlaneError>> {
        Box::pin(async move {
            ControlPlaneRaftAuthority::wait_for_current_leader(self, leader_id, timeout, message)
                .await
        })
    }

    fn shutdown(&self) -> ControlPlaneRaftFuture<'_, Result<(), ControlPlaneError>> {
        Box::pin(async move { ControlPlaneRaftAuthority::shutdown(self).await })
    }
}

openraft::declare_raft_types!(
    pub ControlPlaneRaftTypeConfig:
        D = ControlPlaneCommand,
        R = ControlPlaneRaftApplyResponse,
        NodeId = ControlPlaneRaftNodeId,
        Node = BasicNode,
        Term = ControlPlaneRaftTerm,
        LeaderId = ControlPlaneRaftLeaderId,
        Vote = Vote<ControlPlaneRaftLeaderId>,
        Entry = ControlPlaneRaftEntry,
        SnapshotData = Cursor<Vec<u8>>,
);

#[must_use]
pub fn raft_node_id_from_storage_node_id(node_id: NodeId) -> ControlPlaneRaftNodeId {
    u64::from(node_id.as_u32())
}

#[must_use]
pub fn storage_node_id_from_raft_node_id(node_id: ControlPlaneRaftNodeId) -> Option<NodeId> {
    let node_id = u32::try_from(node_id).ok()?;
    Some(NodeId::new(node_id))
}

#[must_use]
pub fn control_plane_log_id_from_raft(
    log_id: LogIdOf<ControlPlaneRaftTypeConfig>,
) -> Option<ControlPlaneLogId> {
    ControlPlaneLogId::new(log_id.committed_leader_id().term, log_id.index())
}

#[must_use]
pub fn raft_log_id_from_control_plane(
    leader_node_id: ControlPlaneRaftNodeId,
    log_id: ControlPlaneLogId,
) -> LogIdOf<ControlPlaneRaftTypeConfig> {
    LogId::new(
        LeaderId {
            term: log_id.term(),
            node_id: leader_node_id,
        },
        log_id.index(),
    )
}

pub fn assert_openraft_type_config() {
    fn assert_config<C: RaftTypeConfig>() {}
    assert_config::<ControlPlaneRaftTypeConfig>();
}

pub async fn submit_control_plane_command_via_openraft(
    raft: &Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
    command: ControlPlaneCommand,
) -> Result<SubmittedControlPlaneRaftCommand, ControlPlaneError> {
    let response = raft
        .client_write(command)
        .await
        .map_err(|error| openraft_remote_error("client-write", error))?;
    let outcome = match response.data {
        ControlPlaneRaftApplyResponse::Applied(response) => {
            ControlPlaneRaftCommandOutcome::Applied(response)
        }
        ControlPlaneRaftApplyResponse::Rejected(error) => {
            ControlPlaneRaftCommandOutcome::Rejected(error)
        }
        ControlPlaneRaftApplyResponse::Blank | ControlPlaneRaftApplyResponse::Membership => {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "OpenRaft client-write for control-plane command returned non-command response at {}",
                    response.log_id
                ),
            });
        }
    };
    Ok(SubmittedControlPlaneRaftCommand {
        log_id: response.log_id,
        outcome,
    })
}

pub async fn runtime_map_via_openraft_read_index(
    raft: &Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
    issued_at_ms: u64,
) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
    let read_log_id = raft
        .ensure_linearizable(ReadPolicy::ReadIndex)
        .await
        .map_err(|error| openraft_remote_error("read-index", error))?
        .ok_or_else(|| ControlPlaneError::CommandDecode {
            message: "OpenRaft read-index returned no applied log id".to_string(),
        })?;
    let read_index = control_plane_log_id_from_raft(read_log_id).ok_or_else(|| {
        ControlPlaneError::CommandDecode {
            message: format!("invalid OpenRaft read-index log id for runtime map: {read_log_id}"),
        }
    })?;

    raft.with_state_machine(move |state_machine| {
        Box::pin(async move {
            let Some(last_applied) = state_machine.last_applied() else {
                return Err(ControlPlaneError::ControlPlaneReadIndexNotApplied {
                    read_index,
                    last_applied: state_machine.inner().last_applied(),
                });
            };
            if last_applied.index() < read_log_id.index() {
                return Err(ControlPlaneError::ControlPlaneReadIndexNotApplied {
                    read_index,
                    last_applied: state_machine.inner().last_applied(),
                });
            }
            state_machine.runtime_map_for_current_applied_read_index(issued_at_ms)
        })
    })
    .await
    .map_err(|error| openraft_remote_error("state-machine read", error))?
}

fn control_plane_error_to_io_error(context: &'static str, error: ControlPlaneError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("{context}: {error}"))
}

fn openraft_remote_error(context: &'static str, error: impl fmt::Display) -> ControlPlaneError {
    ControlPlaneError::RpcRemote {
        message: format!("OpenRaft {context} failed: {error}"),
    }
}

fn raft_log_store_error(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn observe_deadline_range(
    deadline_ms: u64,
    count: &mut usize,
    earliest_ms: &mut Option<u64>,
    latest_ms: &mut Option<u64>,
) {
    *count += 1;
    *earliest_ms = Some(earliest_ms.map_or(deadline_ms, |existing| existing.min(deadline_ms)));
    *latest_ms = Some(latest_ms.map_or(deadline_ms, |existing| existing.max(deadline_ms)));
}

#[derive(Debug, Clone, Default)]
pub struct ControlPlaneRaftLogStore {
    inner: Arc<Mutex<ControlPlaneRaftLogStoreInner>>,
}

#[derive(Debug, Clone, Default)]
pub struct ControlPlaneRaftLogStoreRestartArtifact {
    vote: Option<VoteOf<ControlPlaneRaftTypeConfig>>,
    committed: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    last_purged_log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    entries: Vec<ControlPlaneRaftEntry>,
}

#[derive(Debug, Clone)]
pub struct ControlPlaneRaftRestartArtifact {
    cluster_name: String,
    log_store: ControlPlaneRaftLogStoreRestartArtifact,
    state_machine: ControlPlaneRaftStateMachineRestartArtifact,
}

const CONTROL_PLANE_RAFT_RESTART_MAGIC: &[u8] = b"ARGMINCPRAFT";
const CONTROL_PLANE_RAFT_RESTART_VERSION: u16 = 2;
const CONTROL_PLANE_RAFT_RESTART_CHECKSUM_LEN: usize = 8;
const CONTROL_PLANE_RAFT_PEER_RPC_MAGIC: &[u8] = b"ARGMINCPRAFTPEER";
const CONTROL_PLANE_RAFT_PEER_RPC_VERSION: u16 = 1;
const CONTROL_PLANE_RAFT_PEER_RPC_CHECKSUM_LEN: usize = 8;
const CONTROL_PLANE_RAFT_PEER_RPC_KIND_REQUEST: u8 = 1;
const CONTROL_PLANE_RAFT_PEER_RPC_KIND_RESPONSE: u8 = 2;
const CONTROL_PLANE_RAFT_PEER_RPC_KIND_SNAPSHOT_REQUEST: u8 = 3;
const CONTROL_PLANE_RAFT_PEER_RPC_KIND_SNAPSHOT_RESPONSE: u8 = 4;
const CONTROL_PLANE_RAFT_PEER_RPC_REQUEST_APPEND_ENTRIES: u8 = 1;
const CONTROL_PLANE_RAFT_PEER_RPC_REQUEST_VOTE: u8 = 2;
const CONTROL_PLANE_RAFT_PEER_RPC_REQUEST_PRE_VOTE: u8 = 3;
const CONTROL_PLANE_RAFT_PEER_RPC_REQUEST_TRANSFER_LEADER: u8 = 4;
const CONTROL_PLANE_RAFT_PEER_RPC_RESPONSE_APPEND_ENTRIES: u8 = 1;
const CONTROL_PLANE_RAFT_PEER_RPC_RESPONSE_VOTE: u8 = 2;
const CONTROL_PLANE_RAFT_PEER_RPC_RESPONSE_TRANSFER_LEADER: u8 = 3;
const CONTROL_PLANE_RAFT_APPEND_ENTRIES_RESPONSE_SUCCESS: u8 = 1;
const CONTROL_PLANE_RAFT_APPEND_ENTRIES_RESPONSE_PARTIAL_SUCCESS: u8 = 2;
const CONTROL_PLANE_RAFT_APPEND_ENTRIES_RESPONSE_CONFLICT: u8 = 3;
const CONTROL_PLANE_RAFT_APPEND_ENTRIES_RESPONSE_HIGHER_VOTE: u8 = 4;
const CONTROL_PLANE_RAFT_TRANSFER_LEADER_RESPONSE_SUCCESS: u8 = 1;
const CONTROL_PLANE_RAFT_TRANSFER_LEADER_RESPONSE_VOTE_CHANGED: u8 = 2;
const CONTROL_PLANE_RAFT_TRANSFER_LEADER_RESPONSE_LOG_NOT_FLUSHED: u8 = 3;
const RAFT_ENTRY_MIN_LEN: usize = 8 + 8 + 8 + 1;
const RAFT_MEMBERSHIP_CONFIG_MIN_LEN: usize = 4;
const RAFT_MEMBERSHIP_NODE_MIN_LEN: usize = 8 + 4;

#[derive(Debug, Default)]
struct ControlPlaneRaftLogStoreInner {
    vote: Option<VoteOf<ControlPlaneRaftTypeConfig>>,
    committed: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    last_purged_log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    entries: BTreeMap<u64, ControlPlaneRaftEntry>,
}

impl ControlPlaneRaftLogStore {
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn export_restart_artifact(
        &self,
    ) -> Result<ControlPlaneRaftLogStoreRestartArtifact, io::Error> {
        let inner = self.lock()?;
        Ok(ControlPlaneRaftLogStoreRestartArtifact {
            vote: inner.vote,
            committed: inner.committed,
            last_purged_log_id: inner.last_purged_log_id,
            entries: inner.entries.values().cloned().collect(),
        })
    }

    pub fn last_purged_log_id(
        &self,
    ) -> Result<Option<LogIdOf<ControlPlaneRaftTypeConfig>>, io::Error> {
        Ok(self.lock()?.last_purged_log_id)
    }

    pub fn persisted_vote(&self) -> Result<Option<VoteOf<ControlPlaneRaftTypeConfig>>, io::Error> {
        Ok(self.lock()?.vote)
    }

    pub fn from_restart_artifact(
        artifact: ControlPlaneRaftLogStoreRestartArtifact,
    ) -> Result<Self, io::Error> {
        let mut inner = ControlPlaneRaftLogStoreInner {
            vote: artifact.vote,
            committed: None,
            last_purged_log_id: artifact.last_purged_log_id,
            entries: BTreeMap::new(),
        };
        Self::validate_contiguous_append(&inner, &artifact.entries)?;
        for entry in artifact.entries {
            inner.entries.insert(entry.log_id.index(), entry);
        }
        Self::validate_committed_update(&inner, artifact.committed)?;
        inner.committed = artifact.committed;
        Self::validate_purged_boundary_has_committed(&inner)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    fn lock(&self) -> Result<MutexGuard<'_, ControlPlaneRaftLogStoreInner>, io::Error> {
        self.inner
            .lock()
            .map_err(|_| io::Error::other("control-plane OpenRaft log store lock poisoned"))
    }

    fn validate_contiguous_append(
        inner: &ControlPlaneRaftLogStoreInner,
        entries: &[ControlPlaneRaftEntry],
    ) -> Result<(), io::Error> {
        let Some(first) = entries.first() else {
            return Ok(());
        };
        let current_last_log_id = inner.last_log_id();
        let expected_first_index = match current_last_log_id {
            Some(log_id) => log_id.index().checked_add(1).ok_or_else(|| {
                raft_log_store_error("cannot append after u64::MAX OpenRaft log index")
            })?,
            None => 0,
        };
        if first.log_id.index() != expected_first_index {
            return Err(raft_log_store_error(format!(
                "control-plane OpenRaft append starts at index {}, expected {}",
                first.log_id.index(),
                expected_first_index
            )));
        }
        if expected_first_index == 0 {
            Self::validate_bootstrap_entry_shape(first)?;
        }

        let mut expected_index = expected_first_index;
        for entry in entries {
            if entry.log_id.index() != expected_index {
                return Err(raft_log_store_error(format!(
                    "control-plane OpenRaft append leaves a log hole at index {expected_index}; next entry is {}",
                    entry.log_id.index()
                )));
            }
            expected_index = expected_index.checked_add(1).ok_or_else(|| {
                raft_log_store_error("control-plane OpenRaft append range overflows u64")
            })?;
        }
        Ok(())
    }

    fn validate_bootstrap_entry_shape(entry: &ControlPlaneRaftEntry) -> Result<(), io::Error> {
        if is_openraft_bootstrap_log_id(entry.log_id)
            && matches!(entry.payload, EntryPayload::Membership(_))
        {
            return Ok(());
        }
        Err(raft_log_store_error(format!(
            "control-plane OpenRaft log index 0 entry must be bootstrap membership at term 0; got log id {} with payload {}",
            entry.log_id,
            raft_entry_payload_name(entry)
        )))
    }

    fn range_start<RB>(range: &RB) -> Result<Option<u64>, io::Error>
    where
        RB: RangeBounds<u64>,
    {
        match range.start_bound() {
            Bound::Included(start) => Ok(Some(*start)),
            Bound::Excluded(start) => Ok(start.checked_add(1)),
            Bound::Unbounded => Ok(Some(0)),
        }
    }

    fn range_end_exclusive<RB>(range: &RB) -> Option<u64>
    where
        RB: RangeBounds<u64>,
    {
        match range.end_bound() {
            Bound::Included(end) => end.checked_add(1),
            Bound::Excluded(end) => Some(*end),
            Bound::Unbounded => None,
        }
    }

    fn before_range_end(index: u64, end_exclusive: Option<u64>) -> bool {
        end_exclusive.is_none_or(|end_exclusive| index < end_exclusive)
    }

    fn validate_known_log_id(
        inner: &ControlPlaneRaftLogStoreInner,
        context: &'static str,
        log_id: LogIdOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<(), io::Error> {
        let Some(current_last_log_id) = inner.last_log_id() else {
            return Err(raft_log_store_error(format!(
                "cannot {context} {log_id}; control-plane OpenRaft log is empty"
            )));
        };
        if log_id.index() > current_last_log_id.index() {
            return Err(raft_log_store_error(format!(
                "cannot {context} {log_id}; current last log id is {current_last_log_id}"
            )));
        }
        if let Some(last_purged_log_id) = inner.last_purged_log_id {
            if log_id.index() < last_purged_log_id.index() {
                return Err(raft_log_store_error(format!(
                    "cannot {context} {log_id}; it is before purged boundary {last_purged_log_id}"
                )));
            }
            if log_id.index() == last_purged_log_id.index() {
                if log_id == last_purged_log_id {
                    return Ok(());
                }
                return Err(raft_log_store_error(format!(
                    "cannot {context} mismatched purged log id {log_id}; purged boundary is {last_purged_log_id}"
                )));
            }
        }
        let Some(entry) = inner.entries.get(&log_id.index()) else {
            return Err(raft_log_store_error(format!(
                "cannot {context} {log_id}; control-plane OpenRaft log has no entry at that index"
            )));
        };
        if entry.log_id != log_id {
            return Err(raft_log_store_error(format!(
                "cannot {context} mismatched log id {log_id}; stored {}",
                entry.log_id
            )));
        }
        Ok(())
    }

    fn validate_committed_update(
        inner: &ControlPlaneRaftLogStoreInner,
        committed: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    ) -> Result<(), io::Error> {
        let Some(committed) = committed else {
            if inner.committed.is_some() {
                return Err(raft_log_store_error(
                    "cannot clear control-plane OpenRaft committed log id",
                ));
            }
            return Ok(());
        };
        if let Some(previous_committed) = inner.committed {
            if committed.index() < previous_committed.index() {
                return Err(raft_log_store_error(format!(
                    "cannot regress control-plane OpenRaft committed log id from {previous_committed} to {committed}"
                )));
            }
            if committed.index() == previous_committed.index() && committed != previous_committed {
                return Err(raft_log_store_error(format!(
                    "cannot change control-plane OpenRaft committed log id at index {} from {previous_committed} to {committed}",
                    committed.index()
                )));
            }
        }
        Self::validate_known_log_id(inner, "commit", committed)?;
        Self::validate_vote_covers_committed(inner.vote, committed)
    }

    fn validate_vote_update(
        inner: &ControlPlaneRaftLogStoreInner,
        vote: VoteOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<(), io::Error> {
        let Some(previous_vote) = inner.vote else {
            return Ok(());
        };
        if matches!(
            vote.partial_cmp(&previous_vote),
            Some(std::cmp::Ordering::Equal | std::cmp::Ordering::Greater)
        ) {
            return Ok(());
        }
        Err(raft_log_store_error(format!(
            "cannot regress control-plane OpenRaft vote from {previous_vote} to {vote}"
        )))
    }

    fn validate_vote_covers_committed(
        vote: Option<VoteOf<ControlPlaneRaftTypeConfig>>,
        committed: LogIdOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<(), io::Error> {
        let Some(vote) = vote else {
            return Err(raft_log_store_error(format!(
                "control-plane OpenRaft log store is missing vote state for committed log id {committed}"
            )));
        };
        if vote.leader_id >= *committed.committed_leader_id() {
            return Ok(());
        }
        Err(raft_log_store_error(format!(
            "control-plane OpenRaft log store vote {vote} does not cover committed log id {committed}"
        )))
    }

    fn validate_purged_boundary_has_committed(
        inner: &ControlPlaneRaftLogStoreInner,
    ) -> Result<(), io::Error> {
        let Some(last_purged_log_id) = inner.last_purged_log_id else {
            return Ok(());
        };
        let Some(committed) = inner.committed else {
            return Err(raft_log_store_error(format!(
                "control-plane OpenRaft log store has purged boundary {last_purged_log_id} without a committed restart gate"
            )));
        };
        if last_purged_log_id.index() <= committed.index() {
            return Ok(());
        }
        Err(raft_log_store_error(format!(
            "control-plane OpenRaft purged boundary {last_purged_log_id} is after committed log id {committed}"
        )))
    }
}

impl ControlPlaneRaftLogStoreInner {
    fn last_log_id(&self) -> Option<LogIdOf<ControlPlaneRaftTypeConfig>> {
        self.entries
            .last_key_value()
            .map(|(_, entry)| entry.log_id)
            .or(self.last_purged_log_id)
    }
}

impl ControlPlaneRaftRestartArtifact {
    pub fn capture(
        cluster_name: impl Into<String>,
        log_store: &ControlPlaneRaftLogStore,
        state_machine: &ControlPlaneRaftStateMachine,
    ) -> Result<Self, io::Error> {
        Ok(Self {
            cluster_name: cluster_name.into(),
            log_store: log_store.export_restart_artifact()?,
            state_machine: state_machine.export_restart_artifact(),
        })
    }

    pub fn encode_durable_artifact(&self) -> Result<Vec<u8>, ControlPlaneError> {
        let mut out = Vec::new();
        out.extend_from_slice(CONTROL_PLANE_RAFT_RESTART_MAGIC);
        write_raft_u16(&mut out, CONTROL_PLANE_RAFT_RESTART_VERSION);
        write_raft_string(&mut out, &self.cluster_name)?;
        write_raft_log_store_artifact(&mut out, &self.log_store)?;
        write_raft_state_machine_artifact(&mut out, &self.state_machine)?;
        append_raft_artifact_checksum(&mut out);
        Ok(out)
    }

    pub fn decode_durable_artifact(bytes: &[u8]) -> Result<Self, ControlPlaneError> {
        let min_len =
            CONTROL_PLANE_RAFT_RESTART_MAGIC.len() + 2 + CONTROL_PLANE_RAFT_RESTART_CHECKSUM_LEN;
        if bytes.len() < min_len {
            return Err(raft_artifact_protocol_error(
                "truncated control-plane OpenRaft durable restart artifact",
            ));
        }
        let (body, checksum_bytes) =
            bytes.split_at(bytes.len() - CONTROL_PLANE_RAFT_RESTART_CHECKSUM_LEN);
        let expected_checksum = u64::from_be_bytes(
            checksum_bytes
                .try_into()
                .expect("checksum split length is fixed"),
        );
        let actual_checksum = raft_artifact_checksum(body);
        if actual_checksum != expected_checksum {
            return Err(raft_artifact_protocol_error(format!(
                "control-plane OpenRaft durable restart artifact checksum mismatch: expected {expected_checksum:#x}, actual {actual_checksum:#x}"
            )));
        }

        let mut reader = RaftArtifactReader::new(body);
        let magic = reader.read_exact(CONTROL_PLANE_RAFT_RESTART_MAGIC.len())?;
        if magic != CONTROL_PLANE_RAFT_RESTART_MAGIC {
            return Err(raft_artifact_protocol_error(
                "invalid control-plane OpenRaft durable restart artifact magic",
            ));
        }
        let version = reader.read_u16()?;
        if version != CONTROL_PLANE_RAFT_RESTART_VERSION {
            return Err(raft_artifact_protocol_error(format!(
                "unsupported control-plane OpenRaft durable restart artifact version {version}"
            )));
        }
        let artifact = Self {
            cluster_name: reader.read_string()?,
            log_store: read_raft_log_store_artifact(&mut reader)?,
            state_machine: read_raft_state_machine_artifact(&mut reader)?,
        };
        reader.finish()?;
        artifact
            .clone()
            .restore()
            .map_err(|error| raft_artifact_protocol_error(error.to_string()))?;
        Ok(artifact)
    }

    pub fn load_durable_artifact(path: &Path) -> Result<Self, ControlPlaneError> {
        let mut file = File::open(path).map_err(|source| ControlPlaneError::Io {
            context: "open control-plane OpenRaft durable restart artifact",
            source,
        })?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .map_err(|source| ControlPlaneError::Io {
                context: "read control-plane OpenRaft durable restart artifact",
                source,
            })?;
        Self::decode_durable_artifact(&bytes)
    }

    pub fn store_durable_artifact(&self, path: &Path) -> Result<(), ControlPlaneError> {
        let bytes = self.encode_durable_artifact()?;
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent).map_err(|source| ControlPlaneError::Io {
                context: "create control-plane OpenRaft durable restart artifact directory",
                source,
            })?;
        }
        let tmp_path = durable_artifact_tmp_path(path);
        {
            let mut file = File::create(&tmp_path).map_err(|source| ControlPlaneError::Io {
                context: "create control-plane OpenRaft durable restart artifact temp file",
                source,
            })?;
            file.write_all(&bytes)
                .map_err(|source| ControlPlaneError::Io {
                    context: "write control-plane OpenRaft durable restart artifact temp file",
                    source,
                })?;
            file.sync_all().map_err(|source| ControlPlaneError::Io {
                context: "sync control-plane OpenRaft durable restart artifact temp file",
                source,
            })?;
        }
        fs::rename(&tmp_path, path).map_err(|source| ControlPlaneError::Io {
            context: "commit control-plane OpenRaft durable restart artifact",
            source,
        })?;
        sync_durable_artifact_parent(path)?;
        Ok(())
    }

    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub fn store_single_node_committed_ahead_bootstrap_artifact_for_test(
        path: &Path,
        cluster_name: impl Into<String>,
        node_id: ControlPlaneRaftNodeId,
        nodes: Vec<(NodeId, String)>,
        pg_ids: Vec<crate::PgId>,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        let log_id = |term, index| LogId::new(LeaderId { term, node_id }, index);
        let bootstrap_membership = ControlPlaneRaftEntry {
            log_id: log_id(0, 0),
            payload: EntryPayload::Membership(Membership::new_with_defaults(
                vec![BTreeSet::from([node_id])],
                [],
            )),
        };
        let blank = ControlPlaneRaftEntry {
            log_id: log_id(3, 1),
            payload: EntryPayload::Blank,
        };
        let bootstrap_command = ControlPlaneRaftEntry {
            log_id: log_id(3, 2),
            payload: EntryPayload::Normal(ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes,
                pg_ids,
            }),
        };

        let mut state_machine = ControlPlaneRaftStateMachine::empty();
        state_machine.apply_entry(bootstrap_membership.clone())?;
        state_machine.apply_entry(blank.clone())?;

        let mut expected_state_machine = state_machine.clone();
        expected_state_machine.apply_entry(bootstrap_command.clone())?;

        ControlPlaneRaftRestartArtifact {
            cluster_name: cluster_name.into(),
            log_store: ControlPlaneRaftLogStoreRestartArtifact {
                vote: Some(Vote::new_committed(3, node_id)),
                committed: Some(bootstrap_command.log_id),
                last_purged_log_id: None,
                entries: vec![bootstrap_membership, blank, bootstrap_command],
            },
            state_machine: state_machine.export_restart_artifact(),
        }
        .store_durable_artifact(path)?;

        Ok(expected_state_machine.inner().snapshot().clone())
    }

    pub fn restore(
        self,
    ) -> Result<(ControlPlaneRaftLogStore, ControlPlaneRaftStateMachine), io::Error> {
        let log_store = ControlPlaneRaftLogStore::from_restart_artifact(self.log_store.clone())?;
        let state_machine =
            ControlPlaneRaftStateMachine::from_restart_artifact(self.state_machine.clone())
                .map_err(|error| {
                    control_plane_error_to_io_error("OpenRaft state-machine restart", error)
                })?;
        Self::validate_log_store_state_machine_pair(&self.log_store, &self.state_machine)?;
        Self::validate_cached_snapshot_replays_to_state_machine(
            &self.log_store,
            &self.state_machine,
        )?;
        Ok((log_store, state_machine))
    }

    fn validate_cluster_identity(
        &self,
        expected_cluster_name: &str,
    ) -> Result<(), ControlPlaneError> {
        if self.cluster_name == expected_cluster_name {
            return Ok(());
        }
        Err(raft_artifact_protocol_error(format!(
            "control-plane OpenRaft durable restart artifact belongs to cluster {:?}, not configured cluster {:?}",
            self.cluster_name, expected_cluster_name
        )))
    }

    fn validate_single_node_local_identity(
        &self,
        local_node_id: ControlPlaneRaftNodeId,
    ) -> Result<(), ControlPlaneError> {
        if let Some(vote) = self.log_store.vote {
            Self::validate_single_node_leader_id("persisted vote", vote.leader_id, local_node_id)?;
        }
        if let Some(committed) = self.log_store.committed {
            Self::validate_single_node_log_id("committed log id", committed, local_node_id)?;
        }
        if let Some(last_purged_log_id) = self.log_store.last_purged_log_id {
            Self::validate_single_node_log_id(
                "purged boundary log id",
                last_purged_log_id,
                local_node_id,
            )?;
        }
        for entry in &self.log_store.entries {
            Self::validate_single_node_log_id("retained log entry", entry.log_id, local_node_id)?;
            if let EntryPayload::Membership(membership) = &entry.payload {
                Self::validate_single_node_membership(
                    "retained log entry membership",
                    membership,
                    local_node_id,
                )?;
            }
        }
        if let Some(last_applied) = self.state_machine.last_applied {
            Self::validate_single_node_log_id(
                "state-machine applied log id",
                last_applied,
                local_node_id,
            )?;
        }
        match self.state_machine.last_membership.log_id() {
            Some(last_membership_log_id) => {
                Self::validate_single_node_log_id(
                    "state-machine membership log id",
                    *last_membership_log_id,
                    local_node_id,
                )?;
                Self::validate_single_node_membership(
                    "state-machine membership",
                    self.state_machine.last_membership.membership(),
                    local_node_id,
                )?;
            }
            None => {
                Self::validate_uninitialized_membership(
                    "state-machine membership",
                    self.state_machine.last_membership.membership(),
                )?;
            }
        }
        Ok(())
    }

    fn validate_single_node_log_id(
        context: &'static str,
        log_id: LogIdOf<ControlPlaneRaftTypeConfig>,
        local_node_id: ControlPlaneRaftNodeId,
    ) -> Result<(), ControlPlaneError> {
        Self::validate_single_node_leader_id(context, *log_id.committed_leader_id(), local_node_id)
    }

    fn validate_single_node_leader_id(
        context: &'static str,
        leader_id: ControlPlaneRaftLeaderId,
        local_node_id: ControlPlaneRaftNodeId,
    ) -> Result<(), ControlPlaneError> {
        if leader_id.node_id == local_node_id {
            return Ok(());
        }
        Err(raft_artifact_protocol_error(format!(
            "control-plane OpenRaft durable restart artifact {context} belongs to OpenRaft node {}, not local node {local_node_id}",
            leader_id.node_id
        )))
    }

    fn validate_single_node_membership(
        context: &'static str,
        membership: &Membership<ControlPlaneRaftNodeId, BasicNode>,
        local_node_id: ControlPlaneRaftNodeId,
    ) -> Result<(), ControlPlaneError> {
        let expected_voters = BTreeSet::from([local_node_id]);
        let configs = membership.get_joint_config();
        let learners = membership.learner_ids().collect::<BTreeSet<_>>();
        if configs.len() == 1 && configs.first() == Some(&expected_voters) && learners.is_empty() {
            return Ok(());
        }
        let voters = membership.voter_ids().collect::<BTreeSet<_>>();
        Err(raft_artifact_protocol_error(format!(
            "control-plane OpenRaft durable restart artifact {context} must be single-node membership for local node {local_node_id}; voters={voters:?} learners={learners:?}"
        )))
    }

    fn validate_uninitialized_membership(
        context: &'static str,
        membership: &Membership<ControlPlaneRaftNodeId, BasicNode>,
    ) -> Result<(), ControlPlaneError> {
        let configs = membership.get_joint_config();
        let learners = membership.learner_ids().collect::<BTreeSet<_>>();
        if configs.is_empty() && learners.is_empty() {
            return Ok(());
        }
        let voters = membership.voter_ids().collect::<BTreeSet<_>>();
        Err(raft_artifact_protocol_error(format!(
            "control-plane OpenRaft durable restart artifact {context} without log id must be empty uninitialized membership; voters={voters:?} learners={learners:?}"
        )))
    }

    fn validate_log_store_state_machine_pair(
        log_store: &ControlPlaneRaftLogStoreRestartArtifact,
        state_machine: &ControlPlaneRaftStateMachineRestartArtifact,
    ) -> Result<(), io::Error> {
        if let Some(last_purged_log_id) = log_store.last_purged_log_id {
            match state_machine.last_applied {
                Some(last_applied) if last_applied.index() > last_purged_log_id.index() => {}
                Some(last_applied) if last_applied == last_purged_log_id => {}
                Some(last_applied) => {
                    return Err(raft_log_store_error(format!(
                        "control-plane OpenRaft state-machine applied log id {last_applied} is behind purged boundary {last_purged_log_id}"
                    )));
                }
                None => {
                    return Err(raft_log_store_error(format!(
                        "control-plane OpenRaft state-machine has no applied log id but log is purged through {last_purged_log_id}"
                    )));
                }
            }
        }

        let Some(last_applied) = state_machine.last_applied else {
            return Ok(());
        };
        let Some(known_applied) =
            Self::log_store_artifact_log_id_at(log_store, last_applied.index())
        else {
            return Err(raft_log_store_error(format!(
                "control-plane OpenRaft state-machine applied log id {last_applied} is not retained or purged in the log store"
            )));
        };
        if known_applied != last_applied {
            return Err(raft_log_store_error(format!(
                "control-plane OpenRaft state-machine applied log id {last_applied} does not match log-store log id {known_applied}"
            )));
        }

        if is_openraft_bootstrap_log_id(last_applied) {
            return Ok(());
        }
        let Some(committed) = log_store.committed else {
            return Err(raft_log_store_error(format!(
                "control-plane OpenRaft state-machine applied log id {last_applied} has no committed restart gate"
            )));
        };
        if last_applied.index() > committed.index() {
            return Err(raft_log_store_error(format!(
                "control-plane OpenRaft state-machine applied log id {last_applied} is after committed restart gate {committed}"
            )));
        }
        if last_applied.index() == committed.index() && last_applied != committed {
            return Err(raft_log_store_error(format!(
                "control-plane OpenRaft state-machine applied log id {last_applied} conflicts with committed restart gate {committed}"
            )));
        }
        Ok(())
    }

    fn validate_cached_snapshot_replays_to_state_machine(
        log_store: &ControlPlaneRaftLogStoreRestartArtifact,
        state_machine: &ControlPlaneRaftStateMachineRestartArtifact,
    ) -> Result<(), io::Error> {
        let Some(snapshot) = state_machine.current_snapshot.as_ref() else {
            return Ok(());
        };
        if snapshot.meta.last_log_id == state_machine.last_applied {
            return Ok(());
        }
        let Some(target_last_applied) = state_machine.last_applied else {
            return Err(raft_log_store_error(
                "cached OpenRaft snapshot is present but state-machine has no applied log id",
            ));
        };
        Self::validate_cached_snapshot_membership_at_boundary(
            log_store,
            snapshot,
            target_last_applied,
        )?;

        let control_plane_snapshot_log_id = match snapshot.meta.last_log_id {
            Some(log_id) if is_openraft_bootstrap_log_id(log_id) => None,
            Some(log_id) => control_plane_log_id_from_raft(log_id)
                .ok_or_else(|| {
                    raft_log_store_error(format!(
                        "invalid cached OpenRaft snapshot last_log_id: {log_id}"
                    ))
                })
                .map(Some)?,
            None => None,
        };
        let mut snapshot_inner = ReplicatedControlPlaneStateMachine::empty();
        snapshot_inner
            .install_snapshot_artifact(ControlPlaneSnapshotArtifact::new(
                control_plane_snapshot_log_id,
                snapshot.snapshot.get_ref().clone(),
            ))
            .map_err(|error| {
                control_plane_error_to_io_error("OpenRaft cached snapshot replay base", error)
            })?;
        let mut replayed = ControlPlaneRaftStateMachine::new(
            snapshot_inner,
            snapshot.meta.last_log_id,
            snapshot.meta.last_membership.clone(),
        )
        .map_err(|error| {
            control_plane_error_to_io_error("OpenRaft cached snapshot replay state", error)
        })?;

        let next_index = snapshot
            .meta
            .last_log_id
            .map_or(Some(0), |log_id| log_id.index().checked_add(1))
            .ok_or_else(|| {
                raft_log_store_error(
                    "cached OpenRaft snapshot last_log_id cannot be followed by a suffix",
                )
            })?;
        for index in next_index..=target_last_applied.index() {
            let entry = Self::log_store_artifact_entry_at(log_store, index).ok_or_else(|| {
                raft_log_store_error(format!(
                    "cached OpenRaft snapshot cannot replay missing retained suffix entry at index {index}"
                ))
            })?;
            replayed.apply_entry(entry.clone()).map_err(|error| {
                control_plane_error_to_io_error("OpenRaft cached snapshot suffix replay", error)
            })?;
        }
        if replayed.last_applied != state_machine.last_applied
            || replayed.last_membership != state_machine.last_membership
            || replayed.inner.snapshot() != state_machine.inner.snapshot()
            || replayed.inner.last_applied() != state_machine.inner.last_applied()
        {
            return Err(raft_log_store_error(
                "cached OpenRaft snapshot plus retained suffix does not match state-machine restart payload",
            ));
        }
        Ok(())
    }

    fn validate_cached_snapshot_membership_at_boundary(
        log_store: &ControlPlaneRaftLogStoreRestartArtifact,
        snapshot: &SnapshotOf<ControlPlaneRaftTypeConfig>,
        target_last_applied: LogIdOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<(), io::Error> {
        let Some(snapshot_log_id) = snapshot.meta.last_log_id else {
            return Ok(());
        };
        let suffix_has_membership = log_store.entries.iter().any(|entry| {
            entry.log_id.index() > snapshot_log_id.index()
                && entry.log_id.index() <= target_last_applied.index()
                && matches!(&entry.payload, EntryPayload::Membership(_))
        });
        if !suffix_has_membership {
            return Ok(());
        }

        let Some(expected_membership) =
            Self::log_store_artifact_membership_at(log_store, snapshot_log_id.index())
        else {
            return Err(raft_log_store_error(format!(
                "cached OpenRaft snapshot membership at {snapshot_log_id} cannot be validated from retained log prefix before membership-changing suffix"
            )));
        };
        if expected_membership.log_id() != snapshot.meta.last_membership.log_id()
            || expected_membership.membership() != snapshot.meta.last_membership.membership()
        {
            return Err(raft_log_store_error(format!(
                "cached OpenRaft snapshot membership at {snapshot_log_id} does not match retained log prefix"
            )));
        }
        Ok(())
    }

    fn log_store_artifact_log_id_at(
        log_store: &ControlPlaneRaftLogStoreRestartArtifact,
        index: u64,
    ) -> Option<LogIdOf<ControlPlaneRaftTypeConfig>> {
        if let Some(last_purged_log_id) = log_store.last_purged_log_id {
            if index < last_purged_log_id.index() {
                return None;
            }
            if index == last_purged_log_id.index() {
                return Some(last_purged_log_id);
            }
        }
        log_store
            .entries
            .iter()
            .find(|entry| entry.log_id.index() == index)
            .map(|entry| entry.log_id)
    }

    fn log_store_artifact_entry_at(
        log_store: &ControlPlaneRaftLogStoreRestartArtifact,
        index: u64,
    ) -> Option<&ControlPlaneRaftEntry> {
        log_store
            .entries
            .iter()
            .find(|entry| entry.log_id.index() == index)
    }

    fn log_store_artifact_membership_at(
        log_store: &ControlPlaneRaftLogStoreRestartArtifact,
        index: u64,
    ) -> Option<StoredMembershipOf<ControlPlaneRaftTypeConfig>> {
        if log_store.last_purged_log_id.is_some() {
            return None;
        }
        let mut membership = StoredMembership::default();
        let mut expected_index = 0;
        for entry in &log_store.entries {
            if entry.log_id.index() != expected_index {
                return None;
            }
            if entry.log_id.index() > index {
                break;
            }
            if let EntryPayload::Membership(entry_membership) = &entry.payload {
                membership = StoredMembership::new(Some(entry.log_id), entry_membership.clone());
            }
            expected_index = expected_index.checked_add(1)?;
        }
        if expected_index > index {
            Some(membership)
        } else {
            None
        }
    }
}

fn decode_raft_peer_rpc_frame<T>(
    bytes: &[u8],
    expected_kind: u8,
    expected_identity: Option<&ControlPlaneRaftPeerFrameIdentity>,
    decode_body: impl FnOnce(&mut RaftArtifactReader<'_>) -> Result<T, ControlPlaneError>,
) -> Result<T, ControlPlaneError> {
    let mut reader = raft_peer_rpc_frame_reader(bytes)?;
    let kind = reader.read_u8()?;
    if kind != expected_kind {
        return Err(raft_artifact_protocol_error(format!(
            "control-plane OpenRaft peer RPC frame kind {kind} does not match expected kind {expected_kind}"
        )));
    }
    let identity = reader.read_peer_frame_identity()?;
    if let Some(expected_identity) = expected_identity {
        validate_raft_peer_frame_identity(&identity, expected_identity)?;
    }
    let decoded = decode_body(&mut reader)?;
    reader.finish()?;
    Ok(decoded)
}

fn raft_peer_rpc_frame_reader(bytes: &[u8]) -> Result<RaftArtifactReader<'_>, ControlPlaneError> {
    let min_len =
        CONTROL_PLANE_RAFT_PEER_RPC_MAGIC.len() + 2 + 1 + CONTROL_PLANE_RAFT_PEER_RPC_CHECKSUM_LEN;
    if bytes.len() < min_len {
        return Err(raft_artifact_protocol_error(
            "truncated control-plane OpenRaft peer RPC frame",
        ));
    }
    let (body, checksum_bytes) =
        bytes.split_at(bytes.len() - CONTROL_PLANE_RAFT_PEER_RPC_CHECKSUM_LEN);
    let expected_checksum = u64::from_be_bytes(
        checksum_bytes
            .try_into()
            .expect("checksum split length is fixed"),
    );
    let actual_checksum = raft_artifact_checksum(body);
    if actual_checksum != expected_checksum {
        return Err(raft_artifact_protocol_error(format!(
            "control-plane OpenRaft peer RPC frame checksum mismatch: expected {expected_checksum:#x}, actual {actual_checksum:#x}"
        )));
    }

    let mut reader =
        RaftArtifactReader::with_context(body, "control-plane OpenRaft peer RPC frame");
    let magic = reader.read_exact(CONTROL_PLANE_RAFT_PEER_RPC_MAGIC.len())?;
    if magic != CONTROL_PLANE_RAFT_PEER_RPC_MAGIC {
        return Err(raft_artifact_protocol_error(
            "invalid control-plane OpenRaft peer RPC frame magic",
        ));
    }
    let version = reader.read_u16()?;
    if version != CONTROL_PLANE_RAFT_PEER_RPC_VERSION {
        return Err(raft_artifact_protocol_error(format!(
            "unsupported control-plane OpenRaft peer RPC frame version {version}"
        )));
    }
    Ok(reader)
}

pub fn decode_control_plane_raft_peer_request_frame_kind(
    bytes: &[u8],
) -> Result<ControlPlaneRaftPeerFrameKind, ControlPlaneError> {
    let mut reader = raft_peer_rpc_frame_reader(bytes)?;
    match reader.read_u8()? {
        CONTROL_PLANE_RAFT_PEER_RPC_KIND_REQUEST => Ok(ControlPlaneRaftPeerFrameKind::OrdinaryRpc),
        CONTROL_PLANE_RAFT_PEER_RPC_KIND_SNAPSHOT_REQUEST => {
            Ok(ControlPlaneRaftPeerFrameKind::Snapshot)
        }
        CONTROL_PLANE_RAFT_PEER_RPC_KIND_RESPONSE => Err(raft_artifact_protocol_error(
            "control-plane OpenRaft peer RPC response frame cannot be handled as a request",
        )),
        CONTROL_PLANE_RAFT_PEER_RPC_KIND_SNAPSHOT_RESPONSE => Err(raft_artifact_protocol_error(
            "control-plane OpenRaft peer snapshot response frame cannot be handled as a request",
        )),
        kind => Err(raft_artifact_protocol_error(format!(
            "unknown control-plane OpenRaft peer RPC frame kind {kind}"
        ))),
    }
}

fn write_raft_peer_frame_identity(
    out: &mut Vec<u8>,
    identity: Option<&ControlPlaneRaftPeerFrameIdentity>,
) -> Result<(), ControlPlaneError> {
    match identity {
        None => write_raft_u8(out, 0),
        Some(identity) => {
            write_raft_u8(out, 1);
            write_raft_string(out, &identity.cluster_name)?;
            write_raft_u64(out, identity.source);
            write_raft_u64(out, identity.target);
        }
    }
    Ok(())
}

fn validate_raft_peer_frame_identity(
    actual: &Option<ControlPlaneRaftPeerFrameIdentity>,
    expected: &ControlPlaneRaftPeerFrameIdentity,
) -> Result<(), ControlPlaneError> {
    let Some(actual) = actual else {
        return Err(raft_artifact_protocol_error(
            "control-plane OpenRaft peer RPC frame is missing peer identity",
        ));
    };
    if actual.cluster_name != expected.cluster_name {
        return Err(raft_artifact_protocol_error(format!(
            "control-plane OpenRaft peer RPC frame cluster identity mismatch: expected {}, got {}",
            expected.cluster_name, actual.cluster_name
        )));
    }
    if actual.source != expected.source {
        return Err(raft_artifact_protocol_error(format!(
            "control-plane OpenRaft peer RPC frame source identity mismatch: expected {}, got {}",
            expected.source, actual.source
        )));
    }
    if actual.target != expected.target {
        return Err(raft_artifact_protocol_error(format!(
            "control-plane OpenRaft peer RPC frame target identity mismatch: expected {}, got {}",
            expected.target, actual.target
        )));
    }
    Ok(())
}

pub fn write_control_plane_raft_peer_transport_frame(
    writer: &mut impl Write,
    frame: &[u8],
) -> Result<(), ControlPlaneError> {
    let frame_len = u32::try_from(frame.len()).map_err(|_| ControlPlaneError::RpcProtocol {
        message: format!(
            "control-plane OpenRaft peer transport frame too large: {} bytes",
            frame.len()
        ),
    })?;
    let mut header = Vec::with_capacity(std::mem::size_of::<u32>());
    write_raft_u32(&mut header, frame_len);
    writer
        .write_all(&header)
        .and_then(|()| writer.write_all(frame))
        .map_err(|source| ControlPlaneError::Io {
            context: "write control-plane OpenRaft peer transport frame",
            source,
        })
}

pub fn read_control_plane_raft_peer_transport_frame(
    reader: &mut impl Read,
    max_frame_bytes: usize,
) -> Result<Vec<u8>, ControlPlaneError> {
    let mut header = [0; std::mem::size_of::<u32>()];
    reader
        .read_exact(&mut header)
        .map_err(|source| ControlPlaneError::Io {
            context: "read control-plane OpenRaft peer transport frame header",
            source,
        })?;
    let frame_len = usize::try_from(u32::from_be_bytes(header)).map_err(|_| {
        ControlPlaneError::RpcProtocol {
            message: "control-plane OpenRaft peer transport frame length does not fit usize"
                .to_string(),
        }
    })?;
    if frame_len > max_frame_bytes {
        return Err(ControlPlaneError::RpcProtocol {
            message: format!(
                "control-plane OpenRaft peer transport frame size {frame_len} bytes exceeds limit {max_frame_bytes}"
            ),
        });
    }
    let mut frame = vec![0; frame_len];
    reader
        .read_exact(&mut frame)
        .map_err(|source| ControlPlaneError::Io {
            context: "read control-plane OpenRaft peer transport frame payload",
            source,
        })?;
    Ok(frame)
}

pub async fn handle_control_plane_raft_peer_rpc_frame(
    raft: &Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
    frame: &[u8],
    expected_identity: &ControlPlaneRaftPeerFrameIdentity,
) -> Result<Vec<u8>, ControlPlaneError> {
    handle_control_plane_raft_peer_rpc_frame_with_identity(raft, frame, Some(expected_identity))
        .await
}

async fn handle_control_plane_raft_peer_rpc_frame_with_identity(
    raft: &Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
    frame: &[u8],
    expected_identity: Option<&ControlPlaneRaftPeerFrameIdentity>,
) -> Result<Vec<u8>, ControlPlaneError> {
    let request =
        ControlPlaneRaftPeerRpcRequest::decode_frame_with_identity(frame, expected_identity)?;
    let response = match request {
        ControlPlaneRaftPeerRpcRequest::AppendEntries(request) => {
            ControlPlaneRaftPeerRpcResponse::AppendEntries(
                raft.append_entries(request)
                    .await
                    .map_err(|error| openraft_remote_error("peer append_entries", error))?,
            )
        }
        ControlPlaneRaftPeerRpcRequest::Vote(request) => ControlPlaneRaftPeerRpcResponse::Vote(
            raft.vote(request)
                .await
                .map_err(|error| openraft_remote_error("peer vote", error))?,
        ),
        ControlPlaneRaftPeerRpcRequest::PreVote(request) => ControlPlaneRaftPeerRpcResponse::Vote(
            raft.pre_vote(request)
                .await
                .map_err(|error| openraft_remote_error("peer pre_vote", error))?,
        ),
        ControlPlaneRaftPeerRpcRequest::TransferLeader(request) => {
            ControlPlaneRaftPeerRpcResponse::TransferLeader(
                raft.handle_transfer_leader(request)
                    .await
                    .map_err(|error| openraft_remote_error("peer transfer_leader", error))?,
            )
        }
    };
    response.encode_frame_with_identity(
        expected_identity
            .as_ref()
            .map(|identity| reverse_raft_peer_frame_identity(identity))
            .as_ref(),
    )
}

pub async fn handle_control_plane_raft_peer_snapshot_frame(
    raft: &Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
    frame: &[u8],
    max_frame_bytes: usize,
    max_snapshot_bytes: usize,
    expected_identity: &ControlPlaneRaftPeerFrameIdentity,
) -> Result<Vec<u8>, ControlPlaneError> {
    handle_control_plane_raft_peer_snapshot_frame_with_identity(
        raft,
        frame,
        max_frame_bytes,
        max_snapshot_bytes,
        Some(expected_identity),
    )
    .await
}

pub async fn handle_control_plane_raft_peer_unix_stream(
    raft: &Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
    stream: &mut UnixStream,
    frame_kind: ControlPlaneRaftPeerFrameKind,
    limits: ControlPlaneRaftPeerTransportLimits,
    expected_identity: &ControlPlaneRaftPeerFrameIdentity,
    io_timeout: Duration,
) -> Result<(), ControlPlaneError> {
    stream
        .set_read_timeout(Some(io_timeout))
        .map_err(|source| ControlPlaneError::Io {
            context: "set control-plane OpenRaft peer stream read timeout",
            source,
        })?;
    stream
        .set_write_timeout(Some(io_timeout))
        .map_err(|source| ControlPlaneError::Io {
            context: "set control-plane OpenRaft peer stream write timeout",
            source,
        })?;
    let request_frame =
        read_control_plane_raft_peer_transport_frame(stream, limits.max_frame_bytes)?;
    handle_control_plane_raft_peer_unix_request_frame(
        raft,
        stream,
        &request_frame,
        frame_kind,
        limits,
        expected_identity,
    )
    .await
}

pub async fn handle_control_plane_raft_peer_unix_stream_detecting_frame_kind(
    raft: &Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
    stream: &mut UnixStream,
    limits: ControlPlaneRaftPeerTransportLimits,
    expected_identity: &ControlPlaneRaftPeerFrameIdentity,
    io_timeout: Duration,
) -> Result<(), ControlPlaneError> {
    stream
        .set_read_timeout(Some(io_timeout))
        .map_err(|source| ControlPlaneError::Io {
            context: "set control-plane OpenRaft peer stream read timeout",
            source,
        })?;
    stream
        .set_write_timeout(Some(io_timeout))
        .map_err(|source| ControlPlaneError::Io {
            context: "set control-plane OpenRaft peer stream write timeout",
            source,
        })?;
    let request_frame =
        read_control_plane_raft_peer_transport_frame(stream, limits.max_frame_bytes)?;
    let frame_kind = decode_control_plane_raft_peer_request_frame_kind(&request_frame)?;
    handle_control_plane_raft_peer_unix_request_frame(
        raft,
        stream,
        &request_frame,
        frame_kind,
        limits,
        expected_identity,
    )
    .await
}

async fn handle_control_plane_raft_peer_unix_request_frame(
    raft: &Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
    stream: &mut UnixStream,
    request_frame: &[u8],
    frame_kind: ControlPlaneRaftPeerFrameKind,
    limits: ControlPlaneRaftPeerTransportLimits,
    expected_identity: &ControlPlaneRaftPeerFrameIdentity,
) -> Result<(), ControlPlaneError> {
    let response_frame = match frame_kind {
        ControlPlaneRaftPeerFrameKind::OrdinaryRpc => {
            handle_control_plane_raft_peer_rpc_frame(raft, request_frame, expected_identity).await?
        }
        ControlPlaneRaftPeerFrameKind::Snapshot => {
            handle_control_plane_raft_peer_snapshot_frame(
                raft,
                request_frame,
                limits.max_frame_bytes,
                limits.max_snapshot_bytes,
                expected_identity,
            )
            .await?
        }
    };
    write_control_plane_raft_peer_transport_frame(stream, &response_frame)
}

async fn handle_control_plane_raft_peer_snapshot_frame_with_identity(
    raft: &Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
    frame: &[u8],
    max_frame_bytes: usize,
    max_snapshot_bytes: usize,
    expected_identity: Option<&ControlPlaneRaftPeerFrameIdentity>,
) -> Result<Vec<u8>, ControlPlaneError> {
    let request = ControlPlaneRaftPeerSnapshotRequest::decode_frame_with_identity(
        frame,
        max_frame_bytes,
        max_snapshot_bytes,
        expected_identity,
    )?;
    let response = raft
        .install_full_snapshot(request.vote, request.snapshot)
        .await
        .map_err(|error| openraft_remote_error("peer full_snapshot", error))?;
    ControlPlaneRaftPeerSnapshotResponse { response }.encode_frame_with_identity(
        expected_identity
            .as_ref()
            .map(|identity| reverse_raft_peer_frame_identity(identity))
            .as_ref(),
    )
}

fn reverse_raft_peer_frame_identity(
    identity: &ControlPlaneRaftPeerFrameIdentity,
) -> ControlPlaneRaftPeerFrameIdentity {
    ControlPlaneRaftPeerFrameIdentity::new(
        identity.cluster_name.clone(),
        identity.target,
        identity.source,
    )
}

fn durable_artifact_tmp_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|file_name| file_name.to_str())
        .unwrap_or("control-plane-raft.state");
    path.with_file_name(format!("{file_name}.tmp.{}", std::process::id()))
}

fn sync_durable_artifact_parent(path: &Path) -> Result<(), ControlPlaneError> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| ControlPlaneError::Io {
            context: "sync control-plane OpenRaft durable restart artifact directory",
            source,
        })
}

fn append_raft_artifact_checksum(out: &mut Vec<u8>) {
    let checksum = raft_artifact_checksum(out);
    write_raft_u64(out, checksum);
}

fn raft_artifact_checksum(body: &[u8]) -> u64 {
    checksum::crc64::checksum(body)
}

fn write_raft_log_store_artifact(
    out: &mut Vec<u8>,
    artifact: &ControlPlaneRaftLogStoreRestartArtifact,
) -> Result<(), ControlPlaneError> {
    write_raft_option_vote(out, artifact.vote);
    write_raft_option_log_id(out, artifact.committed);
    write_raft_option_log_id(out, artifact.last_purged_log_id);
    write_raft_u32(
        out,
        raft_len_as_u32(artifact.entries.len(), "raft log entries")?,
    );
    for entry in &artifact.entries {
        write_raft_entry(out, entry)?;
    }
    Ok(())
}

fn read_raft_log_store_artifact(
    reader: &mut RaftArtifactReader<'_>,
) -> Result<ControlPlaneRaftLogStoreRestartArtifact, ControlPlaneError> {
    let vote = reader.read_option_vote()?;
    let committed = reader.read_option_log_id()?;
    let last_purged_log_id = reader.read_option_log_id()?;
    let entry_count = reader.read_collection_len("raft log entries", RAFT_ENTRY_MIN_LEN)?;
    let mut entries = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        entries.push(reader.read_entry()?);
    }
    Ok(ControlPlaneRaftLogStoreRestartArtifact {
        vote,
        committed,
        last_purged_log_id,
        entries,
    })
}

fn write_raft_state_machine_artifact(
    out: &mut Vec<u8>,
    artifact: &ControlPlaneRaftStateMachineRestartArtifact,
) -> Result<(), ControlPlaneError> {
    write_raft_option_log_id(out, artifact.last_applied);
    write_raft_stored_membership(out, &artifact.last_membership)?;

    let mut inner = artifact.inner.clone();
    let snapshot_artifact = inner.build_snapshot_artifact()?;
    write_raft_bytes(out, snapshot_artifact.payload())?;
    write_raft_option_snapshot(out, artifact.current_snapshot.as_ref())?;
    Ok(())
}

fn read_raft_state_machine_artifact(
    reader: &mut RaftArtifactReader<'_>,
) -> Result<ControlPlaneRaftStateMachineRestartArtifact, ControlPlaneError> {
    let last_applied = reader.read_option_log_id()?;
    let last_membership = reader.read_stored_membership()?;
    let snapshot_payload = reader.read_bytes("raft state-machine snapshot")?.to_vec();
    let control_plane_last_applied = match last_applied {
        Some(log_id) if is_openraft_bootstrap_log_id(log_id) => None,
        Some(log_id) => Some(control_plane_log_id_from_raft(log_id).ok_or_else(|| {
            raft_artifact_protocol_error(format!(
                "invalid OpenRaft state-machine durable last-applied log id: {log_id}"
            ))
        })?),
        None => None,
    };
    let mut inner = ReplicatedControlPlaneStateMachine::empty();
    inner.install_snapshot_artifact(ControlPlaneSnapshotArtifact::new(
        control_plane_last_applied,
        snapshot_payload,
    ))?;
    let current_snapshot = reader.read_option_snapshot()?;
    Ok(ControlPlaneRaftStateMachineRestartArtifact {
        inner,
        last_applied,
        last_membership,
        current_snapshot,
    })
}

fn write_raft_option_snapshot(
    out: &mut Vec<u8>,
    snapshot: Option<&SnapshotOf<ControlPlaneRaftTypeConfig>>,
) -> Result<(), ControlPlaneError> {
    match snapshot {
        None => write_raft_u8(out, 0),
        Some(snapshot) => {
            write_raft_u8(out, 1);
            write_raft_snapshot(out, snapshot)?;
        }
    }
    Ok(())
}

fn write_raft_snapshot(
    out: &mut Vec<u8>,
    snapshot: &SnapshotOf<ControlPlaneRaftTypeConfig>,
) -> Result<(), ControlPlaneError> {
    write_raft_snapshot_meta(out, &snapshot.meta)?;
    write_raft_bytes(out, snapshot.snapshot.get_ref())?;
    Ok(())
}

fn write_raft_snapshot_meta(
    out: &mut Vec<u8>,
    meta: &SnapshotMetaOf<ControlPlaneRaftTypeConfig>,
) -> Result<(), ControlPlaneError> {
    write_raft_option_log_id(out, meta.last_log_id);
    write_raft_stored_membership(out, &meta.last_membership)?;
    write_raft_string(out, &meta.snapshot_id)?;
    Ok(())
}

fn write_raft_append_entries_request(
    out: &mut Vec<u8>,
    request: &AppendEntriesRequest<ControlPlaneRaftTypeConfig>,
) -> Result<(), ControlPlaneError> {
    write_raft_vote(out, request.vote);
    write_raft_option_log_id(out, request.prev_log_id);
    write_raft_u32(
        out,
        raft_len_as_u32(request.entries.len(), "raft append entries")?,
    );
    for entry in &request.entries {
        write_raft_entry(out, entry)?;
    }
    write_raft_option_log_id(out, request.leader_commit);
    Ok(())
}

fn write_raft_append_entries_response(
    out: &mut Vec<u8>,
    response: &AppendEntriesResponse<ControlPlaneRaftTypeConfig>,
) {
    match response {
        AppendEntriesResponse::Success => {
            write_raft_u8(out, CONTROL_PLANE_RAFT_APPEND_ENTRIES_RESPONSE_SUCCESS);
        }
        AppendEntriesResponse::PartialSuccess(log_id) => {
            write_raft_u8(
                out,
                CONTROL_PLANE_RAFT_APPEND_ENTRIES_RESPONSE_PARTIAL_SUCCESS,
            );
            write_raft_option_log_id(out, *log_id);
        }
        AppendEntriesResponse::Conflict => {
            write_raft_u8(out, CONTROL_PLANE_RAFT_APPEND_ENTRIES_RESPONSE_CONFLICT);
        }
        AppendEntriesResponse::HigherVote(vote) => {
            write_raft_u8(out, CONTROL_PLANE_RAFT_APPEND_ENTRIES_RESPONSE_HIGHER_VOTE);
            write_raft_vote(out, *vote);
        }
    }
}

fn write_raft_vote_request(out: &mut Vec<u8>, request: &VoteRequest<ControlPlaneRaftTypeConfig>) {
    write_raft_vote(out, request.vote);
    write_raft_option_log_id(out, request.last_log_id);
    write_raft_bool(out, request.leadership_transfer);
}

fn write_raft_vote_response(
    out: &mut Vec<u8>,
    response: &VoteResponse<ControlPlaneRaftTypeConfig>,
) {
    write_raft_vote(out, response.vote);
    write_raft_bool(out, response.vote_granted);
    write_raft_option_log_id(out, response.last_log_id);
}

fn write_raft_transfer_leader_request(
    out: &mut Vec<u8>,
    request: &TransferLeaderRequest<ControlPlaneRaftTypeConfig>,
) {
    write_raft_vote(out, *request.from_leader());
    write_raft_u64(out, *request.to_node_id());
    write_raft_option_log_id(out, request.last_log_id().copied());
}

fn write_raft_transfer_leader_response(
    out: &mut Vec<u8>,
    response: &TransferLeaderResponse<ControlPlaneRaftTypeConfig>,
) {
    match response {
        Ok(()) => write_raft_u8(out, CONTROL_PLANE_RAFT_TRANSFER_LEADER_RESPONSE_SUCCESS),
        Err(TransferLeaderError::VoteChanged { expected, actual }) => {
            write_raft_u8(
                out,
                CONTROL_PLANE_RAFT_TRANSFER_LEADER_RESPONSE_VOTE_CHANGED,
            );
            write_raft_vote(out, *expected);
            write_raft_vote(out, *actual);
        }
        Err(TransferLeaderError::LogNotFlushed { expected, actual }) => {
            write_raft_u8(
                out,
                CONTROL_PLANE_RAFT_TRANSFER_LEADER_RESPONSE_LOG_NOT_FLUSHED,
            );
            write_raft_option_log_id(out, *expected);
            write_raft_option_log_id(out, *actual);
        }
    }
}

fn write_raft_entry(
    out: &mut Vec<u8>,
    entry: &ControlPlaneRaftEntry,
) -> Result<(), ControlPlaneError> {
    write_raft_log_id(out, entry.log_id);
    match &entry.payload {
        EntryPayload::Blank => write_raft_u8(out, 0),
        EntryPayload::Membership(membership) => {
            write_raft_u8(out, 1);
            write_raft_membership(out, membership)?;
        }
        EntryPayload::Normal(command) => {
            write_raft_u8(out, 2);
            let encoded = encode_control_plane_command(command)?;
            write_raft_bytes(out, &encoded)?;
        }
    }
    Ok(())
}

fn write_raft_vote(out: &mut Vec<u8>, vote: VoteOf<ControlPlaneRaftTypeConfig>) {
    write_raft_leader_id(out, vote.leader_id);
    write_raft_bool(out, vote.committed);
}

fn write_raft_stored_membership(
    out: &mut Vec<u8>,
    membership: &StoredMembershipOf<ControlPlaneRaftTypeConfig>,
) -> Result<(), ControlPlaneError> {
    write_raft_option_log_id(out, *membership.log_id());
    write_raft_membership(out, membership.membership())
}

fn write_raft_membership(
    out: &mut Vec<u8>,
    membership: &Membership<ControlPlaneRaftNodeId, BasicNode>,
) -> Result<(), ControlPlaneError> {
    let configs = membership.get_joint_config();
    write_raft_u32(
        out,
        raft_len_as_u32(configs.len(), "raft membership configs")?,
    );
    for config in configs {
        write_raft_u32(
            out,
            raft_len_as_u32(config.len(), "raft membership config voters")?,
        );
        for node_id in config {
            write_raft_u64(out, *node_id);
        }
    }
    let nodes = membership.nodes().collect::<Vec<_>>();
    write_raft_u32(out, raft_len_as_u32(nodes.len(), "raft membership nodes")?);
    for (node_id, node) in nodes {
        write_raft_u64(out, *node_id);
        write_raft_string(out, &node.addr)?;
    }
    Ok(())
}

fn write_raft_option_vote(out: &mut Vec<u8>, vote: Option<VoteOf<ControlPlaneRaftTypeConfig>>) {
    match vote {
        None => write_raft_u8(out, 0),
        Some(vote) => {
            write_raft_u8(out, 1);
            write_raft_vote(out, vote);
        }
    }
}

fn write_raft_option_log_id(
    out: &mut Vec<u8>,
    log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
) {
    match log_id {
        None => write_raft_u8(out, 0),
        Some(log_id) => {
            write_raft_u8(out, 1);
            write_raft_log_id(out, log_id);
        }
    }
}

fn write_raft_log_id(out: &mut Vec<u8>, log_id: LogIdOf<ControlPlaneRaftTypeConfig>) {
    write_raft_leader_id(out, *log_id.committed_leader_id());
    write_raft_u64(out, log_id.index());
}

fn write_raft_leader_id(out: &mut Vec<u8>, leader_id: ControlPlaneRaftLeaderId) {
    write_raft_u64(out, leader_id.term);
    write_raft_u64(out, leader_id.node_id);
}

fn write_raft_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), ControlPlaneError> {
    write_raft_u32(out, raft_len_as_u32(bytes.len(), "raft byte payload")?);
    out.extend_from_slice(bytes);
    Ok(())
}

fn write_raft_string(out: &mut Vec<u8>, value: &str) -> Result<(), ControlPlaneError> {
    write_raft_u32(out, raft_len_as_u32(value.len(), "raft string")?);
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn write_raft_bool(out: &mut Vec<u8>, value: bool) {
    write_raft_u8(out, u8::from(value));
}

fn write_raft_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

fn write_raft_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_raft_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_raft_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn raft_len_as_u32(len: usize, field: &'static str) -> Result<u32, ControlPlaneError> {
    u32::try_from(len)
        .map_err(|_| raft_artifact_protocol_error(format!("{field} length {len} exceeds u32::MAX")))
}

fn raft_artifact_protocol_error(message: impl Into<String>) -> ControlPlaneError {
    ControlPlaneError::CommandDecode {
        message: message.into(),
    }
}

struct RaftArtifactReader<'a> {
    payload: &'a [u8],
    offset: usize,
    context: &'static str,
}

impl<'a> RaftArtifactReader<'a> {
    fn new(payload: &'a [u8]) -> Self {
        Self::with_context(payload, "control-plane OpenRaft durable restart artifact")
    }

    fn with_context(payload: &'a [u8], context: &'static str) -> Self {
        Self {
            payload,
            offset: 0,
            context,
        }
    }

    fn finish(&self) -> Result<(), ControlPlaneError> {
        if self.offset == self.payload.len() {
            Ok(())
        } else {
            Err(raft_artifact_protocol_error(format!(
                "{} has {} trailing bytes",
                self.context,
                self.payload.len() - self.offset
            )))
        }
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], ControlPlaneError> {
        let end = self.offset.checked_add(len).ok_or_else(|| {
            raft_artifact_protocol_error(format!("{} offset overflow", self.context))
        })?;
        let bytes = self
            .payload
            .get(self.offset..end)
            .ok_or_else(|| raft_artifact_protocol_error(format!("truncated {}", self.context)))?;
        self.offset = end;
        Ok(bytes)
    }

    fn read_u8(&mut self) -> Result<u8, ControlPlaneError> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u16(&mut self) -> Result<u16, ControlPlaneError> {
        let bytes = self.read_exact(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn read_u32(&mut self) -> Result<u32, ControlPlaneError> {
        let bytes = self.read_exact(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_u64(&mut self) -> Result<u64, ControlPlaneError> {
        let bytes = self.read_exact(8)?;
        Ok(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn read_bool(&mut self) -> Result<bool, ControlPlaneError> {
        match self.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(raft_artifact_protocol_error(format!(
                "invalid {} boolean value {value}",
                self.context
            ))),
        }
    }

    fn read_len(&mut self, field: &'static str) -> Result<usize, ControlPlaneError> {
        usize::try_from(self.read_u32()?)
            .map_err(|_| raft_artifact_protocol_error(format!("{field} length does not fit usize")))
    }

    fn read_collection_len(
        &mut self,
        field: &'static str,
        min_item_len: usize,
    ) -> Result<usize, ControlPlaneError> {
        assert!(min_item_len > 0);
        let len = self.read_len(field)?;
        let max_items = self.remaining_len() / min_item_len;
        if len > max_items {
            return Err(raft_artifact_protocol_error(format!(
                "{field} count {len} exceeds remaining control-plane OpenRaft durable payload capacity {max_items}",
            )));
        }
        Ok(len)
    }

    fn read_bytes(&mut self, field: &'static str) -> Result<&'a [u8], ControlPlaneError> {
        let len = self.read_len(field)?;
        self.read_exact(len)
    }

    fn read_limited_bytes(
        &mut self,
        field: &'static str,
        max_len: usize,
    ) -> Result<&'a [u8], ControlPlaneError> {
        let len = self.read_len(field)?;
        if len > max_len {
            return Err(raft_artifact_protocol_error(format!(
                "{field} length {len} exceeds limit {max_len}"
            )));
        }
        self.read_exact(len)
    }

    fn read_string(&mut self) -> Result<String, ControlPlaneError> {
        let bytes = self.read_bytes("raft string")?;
        std::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|source| {
                raft_artifact_protocol_error(format!(
                    "control-plane OpenRaft durable string is not UTF-8: {source}"
                ))
            })
    }

    fn read_option_vote(
        &mut self,
    ) -> Result<Option<VoteOf<ControlPlaneRaftTypeConfig>>, ControlPlaneError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_vote()?)),
            value => Err(raft_artifact_protocol_error(format!(
                "invalid control-plane OpenRaft durable optional vote tag {value}"
            ))),
        }
    }

    fn read_option_log_id(
        &mut self,
    ) -> Result<Option<LogIdOf<ControlPlaneRaftTypeConfig>>, ControlPlaneError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_log_id()?)),
            value => Err(raft_artifact_protocol_error(format!(
                "invalid control-plane OpenRaft durable optional log-id tag {value}"
            ))),
        }
    }

    fn read_append_entries_request(
        &mut self,
    ) -> Result<AppendEntriesRequest<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
        let vote = self.read_vote()?;
        let prev_log_id = self.read_option_log_id()?;
        let entry_count = self.read_collection_len("raft append entries", RAFT_ENTRY_MIN_LEN)?;
        let mut entries = Vec::with_capacity(entry_count);
        for _ in 0..entry_count {
            entries.push(self.read_entry()?);
        }
        let leader_commit = self.read_option_log_id()?;
        Ok(AppendEntriesRequest {
            vote,
            prev_log_id,
            entries,
            leader_commit,
        })
    }

    fn read_append_entries_response(
        &mut self,
    ) -> Result<AppendEntriesResponse<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
        match self.read_u8()? {
            CONTROL_PLANE_RAFT_APPEND_ENTRIES_RESPONSE_SUCCESS => {
                Ok(AppendEntriesResponse::Success)
            }
            CONTROL_PLANE_RAFT_APPEND_ENTRIES_RESPONSE_PARTIAL_SUCCESS => Ok(
                AppendEntriesResponse::PartialSuccess(self.read_option_log_id()?),
            ),
            CONTROL_PLANE_RAFT_APPEND_ENTRIES_RESPONSE_CONFLICT => {
                Ok(AppendEntriesResponse::Conflict)
            }
            CONTROL_PLANE_RAFT_APPEND_ENTRIES_RESPONSE_HIGHER_VOTE => {
                Ok(AppendEntriesResponse::HigherVote(self.read_vote()?))
            }
            value => Err(raft_artifact_protocol_error(format!(
                "unknown control-plane OpenRaft peer RPC append_entries response tag {value}"
            ))),
        }
    }

    fn read_vote_request(
        &mut self,
    ) -> Result<VoteRequest<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
        let vote = self.read_vote()?;
        let last_log_id = self.read_option_log_id()?;
        let leadership_transfer = self.read_bool()?;
        Ok(VoteRequest {
            vote,
            last_log_id,
            leadership_transfer,
        })
    }

    fn read_vote_response(
        &mut self,
    ) -> Result<VoteResponse<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
        let vote = self.read_vote()?;
        let vote_granted = self.read_bool()?;
        let last_log_id = self.read_option_log_id()?;
        Ok(VoteResponse {
            vote,
            vote_granted,
            last_log_id,
        })
    }

    fn read_transfer_leader_request(
        &mut self,
    ) -> Result<TransferLeaderRequest<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
        let from_leader = self.read_vote()?;
        let to_node_id = self.read_u64()?;
        let last_log_id = self.read_option_log_id()?;
        Ok(TransferLeaderRequest::new(
            from_leader,
            to_node_id,
            last_log_id,
        ))
    }

    fn read_transfer_leader_response(
        &mut self,
    ) -> Result<TransferLeaderResponse<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
        match self.read_u8()? {
            CONTROL_PLANE_RAFT_TRANSFER_LEADER_RESPONSE_SUCCESS => Ok(Ok(())),
            CONTROL_PLANE_RAFT_TRANSFER_LEADER_RESPONSE_VOTE_CHANGED => {
                Ok(Err(TransferLeaderError::VoteChanged {
                    expected: self.read_vote()?,
                    actual: self.read_vote()?,
                }))
            }
            CONTROL_PLANE_RAFT_TRANSFER_LEADER_RESPONSE_LOG_NOT_FLUSHED => {
                Ok(Err(TransferLeaderError::LogNotFlushed {
                    expected: self.read_option_log_id()?,
                    actual: self.read_option_log_id()?,
                }))
            }
            value => Err(raft_artifact_protocol_error(format!(
                "unknown control-plane OpenRaft peer RPC transfer_leader response tag {value}"
            ))),
        }
    }

    fn read_entry(&mut self) -> Result<ControlPlaneRaftEntry, ControlPlaneError> {
        let log_id = self.read_log_id()?;
        let payload = match self.read_u8()? {
            0 => EntryPayload::Blank,
            1 => EntryPayload::Membership(self.read_membership()?),
            2 => EntryPayload::Normal(decode_control_plane_command(
                self.read_bytes("raft command payload")?,
            )?),
            value => {
                return Err(raft_artifact_protocol_error(format!(
                    "unknown control-plane OpenRaft durable entry payload tag {value}"
                )));
            }
        };
        Ok(Entry { log_id, payload })
    }

    fn read_stored_membership(
        &mut self,
    ) -> Result<StoredMembershipOf<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
        let log_id = self.read_option_log_id()?;
        let membership = self.read_membership()?;
        Ok(StoredMembership::new(log_id, membership))
    }

    fn read_option_snapshot(
        &mut self,
    ) -> Result<Option<SnapshotOf<ControlPlaneRaftTypeConfig>>, ControlPlaneError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_snapshot("raft cached snapshot payload")?)),
            value => Err(raft_artifact_protocol_error(format!(
                "invalid control-plane OpenRaft durable optional snapshot tag {value}"
            ))),
        }
    }

    fn read_snapshot(
        &mut self,
        payload_field: &'static str,
    ) -> Result<SnapshotOf<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
        let meta = self.read_snapshot_meta()?;
        let payload = self.read_bytes(payload_field)?.to_vec();
        Ok(Snapshot {
            meta,
            snapshot: Cursor::new(payload),
        })
    }

    fn read_snapshot_limited(
        &mut self,
        payload_field: &'static str,
        max_payload_bytes: usize,
    ) -> Result<SnapshotOf<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
        let meta = self.read_snapshot_meta()?;
        let payload = self.read_limited_bytes(payload_field, max_payload_bytes)?;
        Ok(Snapshot {
            meta,
            snapshot: Cursor::new(payload.to_vec()),
        })
    }

    fn read_snapshot_meta(
        &mut self,
    ) -> Result<SnapshotMetaOf<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
        let last_log_id = self.read_option_log_id()?;
        let last_membership = self.read_stored_membership()?;
        let snapshot_id = self.read_string()?;
        Ok(SnapshotMeta {
            last_log_id,
            last_membership,
            snapshot_id,
        })
    }

    fn read_membership(
        &mut self,
    ) -> Result<Membership<ControlPlaneRaftNodeId, BasicNode>, ControlPlaneError> {
        let config_count =
            self.read_collection_len("raft membership configs", RAFT_MEMBERSHIP_CONFIG_MIN_LEN)?;
        let mut configs = Vec::with_capacity(config_count);
        for _ in 0..config_count {
            let voter_count = self
                .read_collection_len("raft membership config voters", std::mem::size_of::<u64>())?;
            let mut voters = BTreeSet::new();
            for _ in 0..voter_count {
                voters.insert(self.read_u64()?);
            }
            configs.push(voters);
        }

        let node_count =
            self.read_collection_len("raft membership nodes", RAFT_MEMBERSHIP_NODE_MIN_LEN)?;
        let mut nodes = BTreeMap::new();
        for _ in 0..node_count {
            let node_id = self.read_u64()?;
            let node = BasicNode::new(self.read_string()?);
            if nodes.insert(node_id, node).is_some() {
                return Err(raft_artifact_protocol_error(format!(
                    "duplicate control-plane OpenRaft durable membership node {node_id}"
                )));
            }
        }
        Membership::new(configs, nodes).map_err(|error| {
            raft_artifact_protocol_error(format!(
                "invalid control-plane OpenRaft durable membership: {error}"
            ))
        })
    }

    fn read_log_id(&mut self) -> Result<LogIdOf<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
        let leader_id = self.read_leader_id()?;
        let index = self.read_u64()?;
        Ok(LogId::new(leader_id, index))
    }

    fn read_leader_id(&mut self) -> Result<ControlPlaneRaftLeaderId, ControlPlaneError> {
        Ok(LeaderId {
            term: self.read_u64()?,
            node_id: self.read_u64()?,
        })
    }

    fn read_vote(&mut self) -> Result<VoteOf<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
        let leader_id = self.read_leader_id()?;
        let committed = self.read_bool()?;
        Ok(Vote {
            leader_id,
            committed,
        })
    }

    fn read_peer_frame_identity(
        &mut self,
    ) -> Result<Option<ControlPlaneRaftPeerFrameIdentity>, ControlPlaneError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => {
                let cluster_name = self.read_string()?;
                let source = self.read_u64()?;
                let target = self.read_u64()?;
                Ok(Some(ControlPlaneRaftPeerFrameIdentity {
                    cluster_name,
                    source,
                    target,
                }))
            }
            value => Err(raft_artifact_protocol_error(format!(
                "invalid control-plane OpenRaft peer RPC frame identity tag {value}"
            ))),
        }
    }

    fn remaining_len(&self) -> usize {
        self.payload.len() - self.offset
    }
}

impl RaftLogReader<ControlPlaneRaftTypeConfig> for ControlPlaneRaftLogStore {
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<ControlPlaneRaftEntry>, io::Error>
    where
        RB: RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend,
    {
        let Some(start) = Self::range_start(&range)? else {
            return Ok(Vec::new());
        };
        let end_exclusive = Self::range_end_exclusive(&range);
        if end_exclusive.is_some_and(|end_exclusive| start >= end_exclusive) {
            return Ok(Vec::new());
        }

        let inner = self.lock()?;
        let Some((&first_present, _)) = inner.entries.first_key_value() else {
            return Ok(Vec::new());
        };
        let Some((&last_present, _)) = inner.entries.last_key_value() else {
            return Ok(Vec::new());
        };

        let mut entries = Vec::new();
        let mut index = start.max(first_present);
        while index <= last_present && Self::before_range_end(index, end_exclusive) {
            let entry = inner.entries.get(&index).ok_or_else(|| {
                raft_log_store_error(format!(
                    "control-plane OpenRaft log hole at readable index {index}"
                ))
            })?;
            entries.push(entry.clone());
            let Some(next_index) = index.checked_add(1) else {
                break;
            };
            index = next_index;
        }
        Ok(entries)
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<ControlPlaneRaftTypeConfig>>, io::Error> {
        Ok(self.lock()?.vote)
    }
}

impl RaftLogStorage<ControlPlaneRaftTypeConfig> for ControlPlaneRaftLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<ControlPlaneRaftTypeConfig>, io::Error> {
        let inner = self.lock()?;
        Ok(LogState {
            last_purged_log_id: inner.last_purged_log_id,
            last_log_id: inner.last_log_id(),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(
        &mut self,
        vote: &VoteOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<(), io::Error> {
        let mut inner = self.lock()?;
        Self::validate_vote_update(&inner, *vote)?;
        inner.vote = Some(*vote);
        Ok(())
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    ) -> Result<(), io::Error> {
        let mut inner = self.lock()?;
        Self::validate_committed_update(&inner, committed)?;
        inner.committed = committed;
        Ok(())
    }

    async fn read_committed(
        &mut self,
    ) -> Result<Option<LogIdOf<ControlPlaneRaftTypeConfig>>, io::Error> {
        Ok(self.lock()?.committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<ControlPlaneRaftTypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = ControlPlaneRaftEntry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries = entries.into_iter().collect::<Vec<_>>();
        {
            let mut inner = self.lock()?;
            if let Err(error) = Self::validate_contiguous_append(&inner, &entries) {
                let message = error.to_string();
                callback.io_completed(Err(raft_log_store_error(message.clone())));
                return Err(raft_log_store_error(message));
            }
            for entry in entries {
                inner.entries.insert(entry.log_id.index(), entry);
            }
        }
        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate_after(
        &mut self,
        last_log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    ) -> Result<(), io::Error> {
        let mut inner = self.lock()?;
        if let Some(committed) = inner.committed {
            match last_log_id {
                Some(last_log_id) if last_log_id.index() >= committed.index() => {}
                Some(last_log_id) => {
                    return Err(raft_log_store_error(format!(
                        "cannot truncate control-plane OpenRaft log after {last_log_id}; committed log id is {committed}"
                    )));
                }
                None => {
                    return Err(raft_log_store_error(format!(
                        "cannot clear control-plane OpenRaft log; committed log id is {committed}"
                    )));
                }
            }
        }
        let Some(last_log_id) = last_log_id else {
            inner.entries.clear();
            return Ok(());
        };

        Self::validate_known_log_id(&inner, "truncate after", last_log_id)?;
        inner
            .entries
            .retain(|index, _| *index <= last_log_id.index());
        Ok(())
    }

    async fn purge(
        &mut self,
        log_id: LogIdOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<(), io::Error> {
        let mut inner = self.lock()?;
        if let Some(last_purged_log_id) = inner.last_purged_log_id {
            if log_id.index() <= last_purged_log_id.index() {
                if log_id == last_purged_log_id {
                    return Ok(());
                }
                return Err(raft_log_store_error(format!(
                    "cannot repurge control-plane OpenRaft log to {log_id}; current purged boundary is {last_purged_log_id}"
                )));
            }
        }
        if inner.committed.is_none() {
            return Err(raft_log_store_error(format!(
                "cannot purge control-plane OpenRaft log to {log_id}; no committed restart gate"
            )));
        }
        Self::validate_vote_covers_committed(inner.vote, log_id)?;
        inner.entries.retain(|index, _| *index > log_id.index());
        inner.last_purged_log_id = Some(log_id);
        if inner
            .committed
            .is_some_and(|committed| committed.index() < log_id.index())
        {
            inner.committed = Some(log_id);
        }
        Ok(())
    }
}

fn is_openraft_bootstrap_log_id(log_id: LogIdOf<ControlPlaneRaftTypeConfig>) -> bool {
    log_id.index() == 0 && log_id.committed_leader_id().term == 0
}

fn raft_entry_payload_name(entry: &ControlPlaneRaftEntry) -> &'static str {
    match &entry.payload {
        EntryPayload::Blank => "blank",
        EntryPayload::Membership(_) => "membership",
        EntryPayload::Normal(_) => "normal",
    }
}

#[derive(Debug, Clone)]
pub struct ControlPlaneRaftSnapshotBuilder {
    snapshot: Result<SnapshotOf<ControlPlaneRaftTypeConfig>, String>,
}

impl ControlPlaneRaftSnapshotBuilder {
    #[must_use]
    pub fn new(snapshot: SnapshotOf<ControlPlaneRaftTypeConfig>) -> Self {
        Self {
            snapshot: Ok(snapshot),
        }
    }

    #[must_use]
    pub fn from_error(error: ControlPlaneError) -> Self {
        Self {
            snapshot: Err(error.to_string()),
        }
    }
}

impl RaftSnapshotBuilder<ControlPlaneRaftTypeConfig> for ControlPlaneRaftSnapshotBuilder {
    async fn build_snapshot(
        &mut self,
    ) -> Result<SnapshotOf<ControlPlaneRaftTypeConfig>, io::Error> {
        self.snapshot.clone().map_err(|message| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("control-plane OpenRaft snapshot build failed: {message}"),
            )
        })
    }
}

#[derive(Debug, Clone)]
pub struct ControlPlaneRaftStateMachine {
    inner: ReplicatedControlPlaneStateMachine,
    last_applied: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    last_membership: StoredMembershipOf<ControlPlaneRaftTypeConfig>,
    current_snapshot: Option<SnapshotOf<ControlPlaneRaftTypeConfig>>,
}

#[derive(Debug, Clone)]
pub struct ControlPlaneRaftStateMachineRestartArtifact {
    inner: ReplicatedControlPlaneStateMachine,
    last_applied: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    last_membership: StoredMembershipOf<ControlPlaneRaftTypeConfig>,
    current_snapshot: Option<SnapshotOf<ControlPlaneRaftTypeConfig>>,
}

impl ControlPlaneRaftStateMachine {
    #[must_use]
    pub fn empty() -> Self {
        Self::from_parts_unchecked(
            ReplicatedControlPlaneStateMachine::empty(),
            None,
            StoredMembership::default(),
        )
    }

    pub fn new(
        inner: ReplicatedControlPlaneStateMachine,
        last_applied: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
        last_membership: StoredMembershipOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<Self, ControlPlaneError> {
        Self::validate_restart_log_id_consistency(&inner, last_applied)?;
        Self::validate_snapshot_membership_position(last_applied, &last_membership)?;
        Ok(Self::from_parts_unchecked(
            inner,
            last_applied,
            last_membership,
        ))
    }

    #[must_use]
    pub fn export_restart_artifact(&self) -> ControlPlaneRaftStateMachineRestartArtifact {
        let current_snapshot = self
            .current_snapshot
            .as_ref()
            .filter(|snapshot| {
                Self::snapshot_at_or_before(snapshot.meta.last_log_id, self.last_applied)
            })
            .cloned();
        ControlPlaneRaftStateMachineRestartArtifact {
            inner: self.inner.clone(),
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
            current_snapshot,
        }
    }

    pub fn from_restart_artifact(
        artifact: ControlPlaneRaftStateMachineRestartArtifact,
    ) -> Result<Self, ControlPlaneError> {
        let mut state_machine = Self::new(
            artifact.inner,
            artifact.last_applied,
            artifact.last_membership,
        )?;
        if let Some(snapshot) = artifact.current_snapshot {
            state_machine.validate_cached_snapshot(&snapshot)?;
            state_machine.current_snapshot = Some(snapshot);
        }
        Ok(state_machine)
    }

    fn from_parts_unchecked(
        inner: ReplicatedControlPlaneStateMachine,
        last_applied: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
        last_membership: StoredMembershipOf<ControlPlaneRaftTypeConfig>,
    ) -> Self {
        Self {
            inner,
            last_applied,
            last_membership,
            current_snapshot: None,
        }
    }

    #[must_use]
    pub fn inner(&self) -> &ReplicatedControlPlaneStateMachine {
        &self.inner
    }

    #[must_use]
    pub fn last_applied(&self) -> Option<LogIdOf<ControlPlaneRaftTypeConfig>> {
        self.last_applied
    }

    #[must_use]
    pub fn last_membership(&self) -> &StoredMembershipOf<ControlPlaneRaftTypeConfig> {
        &self.last_membership
    }

    #[must_use]
    pub fn current_snapshot(&self) -> Option<&SnapshotOf<ControlPlaneRaftTypeConfig>> {
        self.current_snapshot.as_ref()
    }

    fn validate_cached_snapshot(
        &self,
        snapshot: &SnapshotOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<(), ControlPlaneError> {
        let control_plane_snapshot_log_id = match snapshot.meta.last_log_id {
            Some(log_id) if is_openraft_bootstrap_log_id(log_id) => None,
            Some(log_id) => Some(Self::validate_snapshot_log_id_shape(
                "cached snapshot last_log_id",
                log_id,
            )?),
            None => None,
        };
        Self::validate_snapshot_membership_position(
            snapshot.meta.last_log_id,
            &snapshot.meta.last_membership,
        )?;
        let expected_snapshot_id = Self::snapshot_id_for_log_id(snapshot.meta.last_log_id);
        if snapshot.meta.snapshot_id != expected_snapshot_id {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "cached OpenRaft snapshot id {} does not match expected {} for last_log_id {:?}",
                    snapshot.meta.snapshot_id, expected_snapshot_id, snapshot.meta.last_log_id
                ),
            });
        }
        if !Self::snapshot_at_or_before(snapshot.meta.last_log_id, self.last_applied) {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "cached OpenRaft snapshot last_log_id {:?} is after state-machine applied log id {:?}",
                    snapshot.meta.last_log_id, self.last_applied
                ),
            });
        }

        let mut snapshot_inner = ReplicatedControlPlaneStateMachine::empty();
        snapshot_inner.install_snapshot_artifact(ControlPlaneSnapshotArtifact::new(
            control_plane_snapshot_log_id,
            snapshot.snapshot.get_ref().clone(),
        ))?;
        if snapshot_inner.last_applied() != control_plane_snapshot_log_id {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "cached OpenRaft snapshot payload last-applied {:?} does not match snapshot metadata {:?}",
                    snapshot_inner.last_applied(),
                    control_plane_snapshot_log_id
                ),
            });
        }
        if snapshot.meta.last_log_id == self.last_applied {
            if snapshot.meta.last_membership.log_id() != self.last_membership.log_id()
                || snapshot.meta.last_membership.membership() != self.last_membership.membership()
            {
                return Err(ControlPlaneError::CommandDecode {
                    message:
                        "cached OpenRaft snapshot membership does not match state-machine membership"
                            .to_string(),
                });
            }
            if snapshot_inner.snapshot() != self.inner.snapshot()
                || snapshot_inner.last_applied() != self.inner.last_applied()
            {
                return Err(ControlPlaneError::CommandDecode {
                    message:
                        "cached OpenRaft snapshot payload does not match state-machine restart payload"
                            .to_string(),
                });
            }
        }
        Ok(())
    }

    fn snapshot_at_or_before(
        snapshot_log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
        last_applied: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    ) -> bool {
        match (snapshot_log_id, last_applied) {
            (None, _) => true,
            (Some(_), None) => false,
            (Some(snapshot_log_id), Some(last_applied)) => {
                snapshot_log_id.index() < last_applied.index() || snapshot_log_id == last_applied
            }
        }
    }

    #[must_use]
    pub fn applied_state(
        &self,
    ) -> (
        Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
        StoredMembershipOf<ControlPlaneRaftTypeConfig>,
    ) {
        (self.last_applied, self.last_membership.clone())
    }

    pub fn runtime_map_for_applied_read_index(
        &self,
        read_index: LogIdOf<ControlPlaneRaftTypeConfig>,
        issued_at_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let control_plane_read_index = control_plane_log_id_from_raft(read_index).ok_or_else(|| {
            ControlPlaneError::CommandDecode {
                message: format!(
                    "invalid OpenRaft read-index log id for control-plane runtime map proof: {read_index}"
                ),
            }
        })?;
        if self.last_applied != Some(read_index) {
            if let Some(last_applied) = self.last_applied {
                if last_applied.committed_leader_id().term == read_index.committed_leader_id().term
                    && last_applied.index() == read_index.index()
                {
                    return Err(ControlPlaneError::CommandDecode {
                        message: format!(
                            "OpenRaft read-index log id {read_index} does not match applied log id {last_applied}"
                        ),
                    });
                }
            }
            return Err(ControlPlaneError::ControlPlaneReadIndexNotApplied {
                read_index: control_plane_read_index,
                last_applied: self.inner.last_applied(),
            });
        }
        self.inner
            .runtime_map_for_read_index(control_plane_read_index, issued_at_ms)
    }

    pub fn runtime_map_for_current_applied_read_index(
        &self,
        issued_at_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let last_applied = self
            .last_applied
            .ok_or_else(|| ControlPlaneError::CommandDecode {
                message: "cannot build OpenRaft read-index runtime map before any log is applied"
                    .to_string(),
            })?;
        self.runtime_map_for_applied_read_index(last_applied, issued_at_ms)
    }

    pub fn apply_entry(
        &mut self,
        entry: ControlPlaneRaftEntry,
    ) -> Result<ControlPlaneRaftApplyResponse, ControlPlaneError> {
        let raft_log_id = entry.log_id;
        self.validate_apply_position(raft_log_id)?;
        match entry.payload {
            EntryPayload::Blank => {
                let control_plane_log_id = Self::control_plane_log_id_for_entry(raft_log_id)?;
                self.inner.apply_committed_noop(control_plane_log_id)?;
                self.last_applied = Some(raft_log_id);
                Ok(ControlPlaneRaftApplyResponse::Blank)
            }
            EntryPayload::Membership(membership) => {
                if !is_openraft_bootstrap_log_id(raft_log_id) {
                    let control_plane_log_id = Self::control_plane_log_id_for_entry(raft_log_id)?;
                    self.inner.apply_committed_noop(control_plane_log_id)?;
                }
                self.last_membership = StoredMembership::new(Some(raft_log_id), membership);
                self.last_applied = Some(raft_log_id);
                Ok(ControlPlaneRaftApplyResponse::Membership)
            }
            EntryPayload::Normal(command) => {
                let control_plane_log_id = Self::control_plane_log_id_for_entry(raft_log_id)?;
                let applied = self
                    .inner
                    .apply_committed_command(control_plane_log_id, command)?;
                self.last_applied = Some(raft_log_id);
                match applied.into_outcome() {
                    crate::control_plane_command::CommittedControlPlaneCommandOutcome::Applied(
                        applied,
                    ) => Ok(ControlPlaneRaftApplyResponse::Applied(
                        applied.response().clone(),
                    )),
                    crate::control_plane_command::CommittedControlPlaneCommandOutcome::Rejected(
                        error,
                    ) => Ok(ControlPlaneRaftApplyResponse::Rejected(error)),
                }
            }
        }
    }

    fn control_plane_log_id_for_entry(
        raft_log_id: LogIdOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<ControlPlaneLogId, ControlPlaneError> {
        control_plane_log_id_from_raft(raft_log_id).ok_or_else(|| {
            ControlPlaneError::CommandDecode {
                message: format!("invalid OpenRaft log id for control-plane entry: {raft_log_id}"),
            }
        })
    }

    fn validate_apply_position(
        &self,
        raft_log_id: LogIdOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<(), ControlPlaneError> {
        let Some(last_applied) = self.last_applied else {
            if is_openraft_bootstrap_log_id(raft_log_id) {
                return Ok(());
            }
            if raft_log_id.index() == 0 {
                return Err(ControlPlaneError::CommandDecode {
                    message: format!(
                        "invalid OpenRaft log id for first control-plane entry: {raft_log_id}"
                    ),
                });
            }
            if raft_log_id.index() != 1 {
                return Err(ControlPlaneError::ControlPlaneLogIndexMismatch {
                    expected_index: 1,
                    actual_index: raft_log_id.index(),
                });
            }
            return Ok(());
        };

        let expected_index = last_applied.index().checked_add(1).ok_or(
            ControlPlaneError::ControlPlaneLogIndexOverflow {
                index: last_applied.index(),
            },
        )?;
        if raft_log_id.index() != expected_index {
            return Err(ControlPlaneError::ControlPlaneLogIndexMismatch {
                expected_index,
                actual_index: raft_log_id.index(),
            });
        }
        if raft_log_id.committed_leader_id().term < last_applied.committed_leader_id().term {
            return Err(ControlPlaneError::ControlPlaneLogTermRegression {
                previous_term: last_applied.committed_leader_id().term,
                actual_term: raft_log_id.committed_leader_id().term,
                index: raft_log_id.index(),
            });
        }
        if raft_log_id.committed_leader_id().term == last_applied.committed_leader_id().term
            && raft_log_id.committed_leader_id().node_id
                < last_applied.committed_leader_id().node_id
        {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "OpenRaft log id {raft_log_id} is not after last applied log id {last_applied}"
                ),
            });
        }
        Ok(())
    }

    pub fn build_snapshot(
        &mut self,
    ) -> Result<SnapshotOf<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
        let artifact = self.inner.build_snapshot_artifact()?;
        let meta = self.snapshot_meta_for_artifact(&artifact)?;
        let snapshot = Snapshot {
            meta,
            snapshot: Cursor::new(artifact.into_payload()),
        };
        self.current_snapshot = Some(snapshot.clone());
        Ok(snapshot)
    }

    pub fn create_snapshot_builder(
        &mut self,
    ) -> Result<ControlPlaneRaftSnapshotBuilder, ControlPlaneError> {
        Ok(ControlPlaneRaftSnapshotBuilder::new(self.build_snapshot()?))
    }

    pub fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<ControlPlaneRaftTypeConfig>,
        snapshot: Cursor<Vec<u8>>,
    ) -> Result<(), ControlPlaneError> {
        let last_applied = self.validate_snapshot_meta(meta)?;
        let payload = snapshot.into_inner();
        self.inner
            .install_snapshot_artifact(ControlPlaneSnapshotArtifact::new(
                last_applied,
                payload.clone(),
            ))?;
        self.last_applied = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();
        self.current_snapshot = Some(Snapshot {
            meta: meta.clone(),
            snapshot: Cursor::new(payload),
        });
        Ok(())
    }

    fn snapshot_meta_for_artifact(
        &self,
        artifact: &ControlPlaneSnapshotArtifact,
    ) -> Result<SnapshotMetaOf<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
        let last_log_id = match artifact.last_applied() {
            None => match self.last_applied {
                Some(last_applied) if is_openraft_bootstrap_log_id(last_applied) => {
                    Some(last_applied)
                }
                Some(last_applied) => {
                    return Err(ControlPlaneError::SnapshotDecode {
                        message: format!(
                            "snapshot artifact has no control-plane last-applied log id but OpenRaft log id is {last_applied}"
                        ),
                    });
                }
                None => None,
            },
            Some(artifact_log_id) => {
                let last_applied = self.last_applied.ok_or_else(|| {
                    ControlPlaneError::SnapshotDecode {
                        message: format!(
                            "snapshot artifact has last-applied {artifact_log_id:?} but OpenRaft log id is absent"
                        ),
                    }
                })?;
                if control_plane_log_id_from_raft(last_applied) != Some(artifact_log_id) {
                    return Err(ControlPlaneError::SnapshotDecode {
                        message: format!(
                            "snapshot artifact last-applied {artifact_log_id:?} does not match OpenRaft log id {last_applied}"
                        ),
                    });
                }
                Some(last_applied)
            }
        };
        Ok(SnapshotMeta {
            last_log_id,
            last_membership: self.last_membership.clone(),
            snapshot_id: Self::snapshot_id_for_log_id(last_log_id),
        })
    }

    fn snapshot_id_for_log_id(log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>) -> String {
        match log_id {
            Some(log_id) => format!(
                "control-plane-T{}-N{}-I{}",
                log_id.committed_leader_id().term,
                log_id.committed_leader_id().node_id,
                log_id.index()
            ),
            None => "control-plane-empty".to_string(),
        }
    }

    fn validate_snapshot_meta(
        &self,
        meta: &SnapshotMetaOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<Option<ControlPlaneLogId>, ControlPlaneError> {
        let snapshot_log_id = match meta.last_log_id {
            Some(log_id) if is_openraft_bootstrap_log_id(log_id) => None,
            Some(log_id) => Some(Self::validate_snapshot_log_id_shape("last_log_id", log_id)?),
            None => None,
        };
        self.validate_snapshot_install_position(meta.last_log_id)?;
        Self::validate_snapshot_membership_position(meta.last_log_id, &meta.last_membership)?;
        let expected_snapshot_id = Self::snapshot_id_for_log_id(meta.last_log_id);
        if meta.snapshot_id != expected_snapshot_id {
            return Err(ControlPlaneError::SnapshotDecode {
                message: format!(
                    "OpenRaft snapshot id {} does not match expected {} for last_log_id {:?}",
                    meta.snapshot_id, expected_snapshot_id, meta.last_log_id
                ),
            });
        }
        Ok(snapshot_log_id)
    }

    fn validate_restart_log_id_consistency(
        inner: &ReplicatedControlPlaneStateMachine,
        last_applied: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    ) -> Result<(), ControlPlaneError> {
        match (inner.last_applied(), last_applied) {
            (None, None) => Ok(()),
            (None, Some(log_id)) if is_openraft_bootstrap_log_id(log_id) => Ok(()),
            (None, Some(log_id)) => Err(ControlPlaneError::SnapshotDecode {
                message: format!(
                    "OpenRaft state machine restart has log id {log_id} but no control-plane last-applied log id"
                ),
            }),
            (Some(control_plane_log_id), Some(log_id))
                if control_plane_log_id_from_raft(log_id) == Some(control_plane_log_id) =>
            {
                Ok(())
            }
            (Some(control_plane_log_id), Some(log_id)) => Err(ControlPlaneError::SnapshotDecode {
                message: format!(
                    "OpenRaft state machine restart log id {log_id} does not match control-plane last-applied {control_plane_log_id:?}"
                ),
            }),
            (Some(control_plane_log_id), None) => Err(ControlPlaneError::SnapshotDecode {
                message: format!(
                    "OpenRaft state machine restart is missing log id for control-plane last-applied {control_plane_log_id:?}"
                ),
            }),
        }
    }

    fn validate_snapshot_log_id_shape(
        field: &'static str,
        log_id: LogIdOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<ControlPlaneLogId, ControlPlaneError> {
        control_plane_log_id_from_raft(log_id).ok_or_else(|| ControlPlaneError::SnapshotDecode {
            message: format!("invalid OpenRaft snapshot {field}: {log_id}"),
        })
    }

    fn validate_snapshot_install_position(
        &self,
        snapshot_last_log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    ) -> Result<(), ControlPlaneError> {
        let Some(current_last_applied) = self.last_applied else {
            return Ok(());
        };
        let Some(snapshot_last_log_id) = snapshot_last_log_id else {
            return Err(ControlPlaneError::ControlPlaneSnapshotMissingLogId {
                current_index: current_last_applied.index(),
            });
        };
        if snapshot_last_log_id.index() < current_last_applied.index() {
            return Err(ControlPlaneError::ControlPlaneSnapshotLogIndexRegression {
                current_index: current_last_applied.index(),
                artifact_index: snapshot_last_log_id.index(),
            });
        }
        if snapshot_last_log_id.index() == current_last_applied.index()
            && snapshot_last_log_id != current_last_applied
        {
            return Err(ControlPlaneError::SnapshotDecode {
                message: format!(
                    "OpenRaft snapshot last_log_id {snapshot_last_log_id} does not match current applied log id {current_last_applied}"
                ),
            });
        }
        if snapshot_last_log_id.committed_leader_id().term
            < current_last_applied.committed_leader_id().term
        {
            return Err(ControlPlaneError::ControlPlaneLogTermRegression {
                previous_term: current_last_applied.committed_leader_id().term,
                actual_term: snapshot_last_log_id.committed_leader_id().term,
                index: snapshot_last_log_id.index(),
            });
        }
        if snapshot_last_log_id.committed_leader_id().term
            == current_last_applied.committed_leader_id().term
            && snapshot_last_log_id.committed_leader_id().node_id
                != current_last_applied.committed_leader_id().node_id
        {
            return Err(ControlPlaneError::SnapshotDecode {
                message: format!(
                    "OpenRaft snapshot last_log_id {snapshot_last_log_id} conflicts with current applied leader id {current_last_applied}"
                ),
            });
        }
        Ok(())
    }

    fn validate_snapshot_membership_position(
        snapshot_last_log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
        membership: &StoredMembershipOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<(), ControlPlaneError> {
        let Some(membership_log_id) = membership.log_id().as_ref().copied() else {
            return Ok(());
        };
        if !is_openraft_bootstrap_log_id(membership_log_id) {
            Self::validate_snapshot_log_id_shape("last_membership.log_id", membership_log_id)?;
        }
        let Some(snapshot_last_log_id) = snapshot_last_log_id else {
            return Err(ControlPlaneError::SnapshotDecode {
                message: format!(
                    "OpenRaft snapshot membership log id {membership_log_id} is present without snapshot last_log_id"
                ),
            });
        };
        if membership_log_id.index() > snapshot_last_log_id.index() {
            return Err(ControlPlaneError::SnapshotDecode {
                message: format!(
                    "OpenRaft snapshot membership log id {membership_log_id} is after snapshot last_log_id {snapshot_last_log_id}"
                ),
            });
        }
        if membership_log_id.index() == snapshot_last_log_id.index()
            && membership_log_id != snapshot_last_log_id
        {
            return Err(ControlPlaneError::SnapshotDecode {
                message: format!(
                    "OpenRaft snapshot membership log id {membership_log_id} does not match snapshot last_log_id {snapshot_last_log_id} at the same index"
                ),
            });
        }
        if membership_log_id.committed_leader_id().term
            > snapshot_last_log_id.committed_leader_id().term
        {
            return Err(ControlPlaneError::SnapshotDecode {
                message: format!(
                    "OpenRaft snapshot membership log id {membership_log_id} has a future term relative to snapshot last_log_id {snapshot_last_log_id}"
                ),
            });
        }
        if membership_log_id.committed_leader_id().term
            == snapshot_last_log_id.committed_leader_id().term
            && membership_log_id.committed_leader_id().node_id
                != snapshot_last_log_id.committed_leader_id().node_id
        {
            return Err(ControlPlaneError::SnapshotDecode {
                message: format!(
                    "OpenRaft snapshot membership log id {membership_log_id} conflicts with snapshot leader id {snapshot_last_log_id}"
                ),
            });
        }
        Ok(())
    }
}

impl RaftStateMachine<ControlPlaneRaftTypeConfig> for ControlPlaneRaftStateMachine {
    type SnapshotBuilder = ControlPlaneRaftSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
            StoredMembershipOf<ControlPlaneRaftTypeConfig>,
        ),
        io::Error,
    > {
        Ok(ControlPlaneRaftStateMachine::applied_state(self))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<ControlPlaneRaftTypeConfig>, io::Error>>
            + Unpin
            + OptionalSend,
    {
        while let Some(entry) = entries.next().await {
            let (entry, responder) = entry?;
            let response = self
                .apply_entry(entry)
                .map_err(|error| control_plane_error_to_io_error("OpenRaft apply", error))?;
            if let Some(responder) = responder {
                responder.send(response);
            }
        }
        Ok(())
    }

    async fn try_create_snapshot_builder(&mut self, _force: bool) -> Option<Self::SnapshotBuilder> {
        Some(self.get_snapshot_builder().await)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        match self.create_snapshot_builder() {
            Ok(builder) => builder,
            Err(error) => ControlPlaneRaftSnapshotBuilder::from_error(error),
        }
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Cursor<Vec<u8>>, io::Error> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<ControlPlaneRaftTypeConfig>,
        snapshot: Cursor<Vec<u8>>,
    ) -> Result<(), io::Error> {
        ControlPlaneRaftStateMachine::install_snapshot(self, meta, snapshot)
            .map_err(|error| control_plane_error_to_io_error("OpenRaft install snapshot", error))
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<SnapshotOf<ControlPlaneRaftTypeConfig>>, io::Error> {
        Ok(self.current_snapshot.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::future::Future;
    use std::os::unix::net::{UnixListener, UnixStream};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use std::thread;
    use std::time::Duration;

    use super::*;
    use futures_util::stream;
    use openraft::errors::{
        NetworkError, RPCError, ReplicationClosed, StreamingError, Unreachable,
    };
    use openraft::network::{RPCOption, RaftNetworkFactory, RaftNetworkV2};
    use openraft::raft::{
        AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, TransferLeaderRequest,
        TransferLeaderResponse, VoteRequest, VoteResponse,
    };
    use openraft::type_config::TypeConfigExt;
    use openraft::{AnyError, Config, Membership, Raft, ReadPolicy};

    use crate::control_plane::{
        ClusterControlSnapshot, NodeAvailabilityState, NodeHeartbeat, RuntimeMapFreshnessProof,
    };
    use crate::types::PgId;
    use crate::PgClusterMapHistoryReferenceSummary;

    #[derive(Debug, Clone, Copy, Default)]
    struct UnreachableRaftNetworkFactory;

    impl RaftNetworkFactory<ControlPlaneRaftTypeConfig> for UnreachableRaftNetworkFactory {
        type Network = UnreachableRaftNetwork;

        async fn new_client(
            &mut self,
            target: ControlPlaneRaftNodeId,
            _node: &BasicNode,
        ) -> Self::Network {
            UnreachableRaftNetwork { target }
        }
    }

    #[derive(Debug, Clone, Copy)]
    struct UnreachableRaftNetwork {
        target: ControlPlaneRaftNodeId,
    }

    impl UnreachableRaftNetwork {
        fn unreachable(&self, rpc_name: &'static str) -> Unreachable<ControlPlaneRaftTypeConfig> {
            Unreachable::new(&AnyError::error(format!(
                "test network should not send {rpc_name} to node {}",
                self.target
            )))
        }
    }

    impl RaftNetworkV2<ControlPlaneRaftTypeConfig> for UnreachableRaftNetwork {
        async fn append_entries(
            &mut self,
            _rpc: AppendEntriesRequest<ControlPlaneRaftTypeConfig>,
            _option: RPCOption,
        ) -> Result<
            AppendEntriesResponse<ControlPlaneRaftTypeConfig>,
            RPCError<ControlPlaneRaftTypeConfig>,
        > {
            Err(RPCError::Unreachable(self.unreachable("append_entries")))
        }

        async fn vote(
            &mut self,
            _rpc: VoteRequest<ControlPlaneRaftTypeConfig>,
            _option: RPCOption,
        ) -> Result<VoteResponse<ControlPlaneRaftTypeConfig>, RPCError<ControlPlaneRaftTypeConfig>>
        {
            Err(RPCError::Unreachable(self.unreachable("vote")))
        }

        async fn full_snapshot(
            &mut self,
            _vote: VoteOf<ControlPlaneRaftTypeConfig>,
            _snapshot: SnapshotOf<ControlPlaneRaftTypeConfig>,
            _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
            _option: RPCOption,
        ) -> Result<
            SnapshotResponse<ControlPlaneRaftTypeConfig>,
            StreamingError<ControlPlaneRaftTypeConfig>,
        > {
            Err(StreamingError::Unreachable(
                self.unreachable("full_snapshot"),
            ))
        }
    }

    #[derive(Debug, Clone, Default)]
    struct InMemoryRaftNetworkFactory {
        peers: Arc<
            Mutex<
                BTreeMap<
                    ControlPlaneRaftNodeId,
                    Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
                >,
            >,
        >,
        policy: Option<Arc<ControlPlaneRaftPeerTransportPolicy>>,
        local_node_id: Option<ControlPlaneRaftNodeId>,
    }

    impl InMemoryRaftNetworkFactory {
        fn with_transport_policy(policy: ControlPlaneRaftPeerTransportPolicy) -> Self {
            Self {
                peers: Arc::default(),
                policy: Some(Arc::new(policy)),
                local_node_id: None,
            }
        }

        fn for_local_node(&self, local_node_id: ControlPlaneRaftNodeId) -> Self {
            Self {
                peers: self.peers.clone(),
                policy: self.policy.clone(),
                local_node_id: Some(local_node_id),
            }
        }

        fn register(
            &self,
            node_id: ControlPlaneRaftNodeId,
            raft: Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
        ) {
            self.peers.lock().unwrap().insert(node_id, raft);
        }

        fn unregister(&self, node_id: ControlPlaneRaftNodeId) {
            self.peers.lock().unwrap().remove(&node_id);
        }
    }

    #[derive(Debug, Clone, Default)]
    struct InMemoryAuthorityCapabilityDirectory {
        entries: Arc<Mutex<BTreeMap<ControlPlaneRaftNodeId, InMemoryAuthorityCapabilityEntry>>>,
    }

    #[derive(Debug, Clone)]
    struct InMemoryAuthorityCapabilityEntry {
        status: ControlPlaneRaftAuthorityStatusHandle,
        bootstrap: ControlPlaneRaftAuthorityBootstrapHandle,
        node_lifecycle: ControlPlaneRaftAuthorityNodeLifecycleHandle,
        linearized_authority: ControlPlaneRaftAuthorityHandle,
        leader_routed_admin: ControlPlaneRaftLeaderRoutedAdminHandle,
    }

    impl InMemoryAuthorityCapabilityDirectory {
        fn register<T>(&self, node_id: ControlPlaneRaftNodeId, authority: Arc<T>)
        where
            T: ControlPlaneRaftAuthorityStatusSource
                + ControlPlaneRaftAuthorityBootstrap
                + ControlPlaneRaftAuthorityNodeLifecycle
                + ControlPlaneRaftLinearizedAuthority
                + ControlPlaneRaftLeaderRoutedAdmin
                + Send
                + Sync
                + 'static,
        {
            let status = ControlPlaneRaftAuthorityStatusHandle::new(Arc::clone(&authority));
            let bootstrap = ControlPlaneRaftAuthorityBootstrapHandle::new(Arc::clone(&authority));
            let node_lifecycle =
                ControlPlaneRaftAuthorityNodeLifecycleHandle::new(Arc::clone(&authority));
            let linearized_authority = ControlPlaneRaftAuthorityHandle::new(Arc::clone(&authority));
            let leader_routed_admin = ControlPlaneRaftLeaderRoutedAdminHandle::new(authority);
            self.entries.lock().unwrap().insert(
                node_id,
                InMemoryAuthorityCapabilityEntry {
                    status,
                    bootstrap,
                    node_lifecycle,
                    linearized_authority,
                    leader_routed_admin,
                },
            );
        }
    }

    impl ControlPlaneRaftAuthorityStatusListSource for InMemoryAuthorityCapabilityDirectory {
        fn authority_statuses(
            &self,
        ) -> ControlPlaneRaftFuture<
            '_,
            Result<
                BTreeMap<ControlPlaneRaftNodeId, ControlPlaneRaftAuthorityStatus>,
                ControlPlaneError,
            >,
        > {
            Box::pin(async move {
                let entries = {
                    let entries =
                        self.entries
                            .lock()
                            .map_err(|_| ControlPlaneError::RpcRemote {
                                message:
                                    "in-memory test authority capability directory lock poisoned"
                                        .to_string(),
                            })?;
                    entries.clone()
                };
                let mut statuses = BTreeMap::new();
                for (node_id, entry) in entries {
                    let status = entry.status.status().await.map_err(|error| {
                        ControlPlaneError::RpcRemote {
                            message: format!(
                                "in-memory test authority capability directory status for node {node_id} failed: {error:?}"
                            ),
                        }
                    })?;
                    statuses.insert(node_id, status);
                }
                Ok(statuses)
            })
        }
    }

    impl ControlPlaneRaftAuthorityBootstrapDirectory for InMemoryAuthorityCapabilityDirectory {
        fn authority_bootstrap_for_node(
            &self,
            node_id: ControlPlaneRaftNodeId,
        ) -> ControlPlaneRaftFuture<
            '_,
            Result<ControlPlaneRaftAuthorityBootstrapHandle, ControlPlaneError>,
        > {
            let result = (|| {
                let entries = self
                    .entries
                    .lock()
                    .map_err(|_| ControlPlaneError::RpcRemote {
                        message: "in-memory test authority capability directory lock poisoned"
                            .to_string(),
                    })?;
                entries
                    .get(&node_id)
                    .map(|entry| entry.bootstrap.clone())
                    .ok_or_else(|| ControlPlaneError::RpcRemote {
                        message: format!(
                            "in-memory test authority capability directory has no bootstrap node {node_id}"
                        ),
                    })
            })();
            Box::pin(std::future::ready(result))
        }
    }

    impl ControlPlaneRaftAuthorityNodeLifecycleDirectory for InMemoryAuthorityCapabilityDirectory {
        fn authority_node_lifecycle_for_node(
            &self,
            node_id: ControlPlaneRaftNodeId,
        ) -> ControlPlaneRaftFuture<
            '_,
            Result<ControlPlaneRaftAuthorityNodeLifecycleHandle, ControlPlaneError>,
        > {
            let result = (|| {
                let entries = self
                    .entries
                    .lock()
                    .map_err(|_| ControlPlaneError::RpcRemote {
                        message: "in-memory test authority capability directory lock poisoned"
                            .to_string(),
                    })?;
                entries
                    .get(&node_id)
                    .map(|entry| entry.node_lifecycle.clone())
                    .ok_or_else(|| ControlPlaneError::RpcRemote {
                        message: format!(
                            "in-memory test authority capability directory has no node-lifecycle node {node_id}"
                        ),
                    })
            })();
            Box::pin(std::future::ready(result))
        }
    }

    impl ControlPlaneRaftLinearizedAuthorityDirectory for InMemoryAuthorityCapabilityDirectory {
        fn linearized_authority_for_node(
            &self,
            node_id: ControlPlaneRaftNodeId,
        ) -> ControlPlaneRaftFuture<'_, Result<ControlPlaneRaftAuthorityHandle, ControlPlaneError>>
        {
            let result = (|| {
                let entries = self
                    .entries
                    .lock()
                    .map_err(|_| ControlPlaneError::RpcRemote {
                        message: "in-memory test authority capability directory lock poisoned"
                            .to_string(),
                    })?;
                entries
                    .get(&node_id)
                    .map(|entry| entry.linearized_authority.clone())
                    .ok_or_else(|| ControlPlaneError::RpcRemote {
                        message: format!(
                            "in-memory test authority capability directory has no linearized node {node_id}"
                        ),
                    })
            })();
            Box::pin(std::future::ready(result))
        }
    }

    impl ControlPlaneRaftLeaderRoutedAdminDirectory for InMemoryAuthorityCapabilityDirectory {
        fn leader_routed_admin_for_node(
            &self,
            node_id: ControlPlaneRaftNodeId,
        ) -> ControlPlaneRaftFuture<
            '_,
            Result<ControlPlaneRaftLeaderRoutedAdminHandle, ControlPlaneError>,
        > {
            let result = (|| {
                let entries = self
                    .entries
                    .lock()
                    .map_err(|_| ControlPlaneError::RpcRemote {
                        message: "in-memory test authority capability directory lock poisoned"
                            .to_string(),
                    })?;
                entries
                    .get(&node_id)
                    .map(|entry| entry.leader_routed_admin.clone())
                    .ok_or_else(|| ControlPlaneError::RpcRemote {
                        message: format!(
                            "in-memory test authority capability directory has no leader-routed admin node {node_id}"
                        ),
                    })
            })();
            Box::pin(std::future::ready(result))
        }
    }

    #[derive(Clone)]
    struct FixedLinearizedAuthorityDirectory {
        authority: ControlPlaneRaftAuthorityHandle,
    }

    impl ControlPlaneRaftLinearizedAuthorityDirectory for FixedLinearizedAuthorityDirectory {
        fn linearized_authority_for_node(
            &self,
            _node_id: ControlPlaneRaftNodeId,
        ) -> ControlPlaneRaftFuture<'_, Result<ControlPlaneRaftAuthorityHandle, ControlPlaneError>>
        {
            Box::pin(std::future::ready(Ok(self.authority.clone())))
        }
    }

    impl RaftNetworkFactory<ControlPlaneRaftTypeConfig> for InMemoryRaftNetworkFactory {
        type Network = InMemoryRaftNetwork;

        async fn new_client(
            &mut self,
            target: ControlPlaneRaftNodeId,
            node: &BasicNode,
        ) -> Self::Network {
            InMemoryRaftNetwork {
                peers: self.peers.clone(),
                policy: self.policy.clone(),
                source: self.local_node_id,
                target,
                node: node.clone(),
            }
        }
    }

    #[derive(Clone)]
    struct InMemoryRaftNetwork {
        peers: Arc<
            Mutex<
                BTreeMap<
                    ControlPlaneRaftNodeId,
                    Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
                >,
            >,
        >,
        policy: Option<Arc<ControlPlaneRaftPeerTransportPolicy>>,
        source: Option<ControlPlaneRaftNodeId>,
        target: ControlPlaneRaftNodeId,
        node: BasicNode,
    }

    impl InMemoryRaftNetwork {
        fn encoded_append_entries_payload_len(
            entries: &[ControlPlaneRaftEntry],
        ) -> Result<usize, RPCError<ControlPlaneRaftTypeConfig>> {
            let mut encoded = Vec::new();
            write_raft_u32(
                &mut encoded,
                raft_len_as_u32(entries.len(), "raft append entries").map_err(|error| {
                    RPCError::Network(NetworkError::from_string(format!(
                        "in-memory test raft network append_entries encode failed: {error:?}"
                    )))
                })?,
            );
            for entry in entries {
                write_raft_entry(&mut encoded, entry).map_err(|error| {
                    RPCError::Network(NetworkError::from_string(format!(
                        "in-memory test raft network append_entries encode failed: {error:?}"
                    )))
                })?;
            }
            Ok(encoded.len())
        }

        fn validate_peer(
            &self,
            rpc_name: &'static str,
        ) -> Result<(), RPCError<ControlPlaneRaftTypeConfig>> {
            if let Some(policy) = &self.policy {
                policy
                    .validate_target_node(self.target, &self.node, rpc_name)
                    .map_err(Self::rpc_error_from_transport_rejection)?;
            }
            Ok(())
        }

        fn request_identity(
            &self,
        ) -> Result<Option<ControlPlaneRaftPeerFrameIdentity>, RPCError<ControlPlaneRaftTypeConfig>>
        {
            let Some(policy) = &self.policy else {
                return Ok(None);
            };
            let source = self.source.ok_or_else(|| {
                RPCError::Network(NetworkError::from_string(
                    "in-memory test raft network has no local source node for peer frame identity",
                ))
            })?;
            policy
                .frame_identity(source, self.target)
                .map(Some)
                .map_err(Self::rpc_error_from_transport_rejection)
        }

        fn response_identity(
            &self,
        ) -> Result<Option<ControlPlaneRaftPeerFrameIdentity>, RPCError<ControlPlaneRaftTypeConfig>>
        {
            let Some(policy) = &self.policy else {
                return Ok(None);
            };
            let source = self.source.ok_or_else(|| {
                RPCError::Network(NetworkError::from_string(
                    "in-memory test raft network has no local source node for peer frame identity",
                ))
            })?;
            policy
                .frame_identity(self.target, source)
                .map(Some)
                .map_err(Self::rpc_error_from_transport_rejection)
        }

        fn snapshot_request_identity(
            &self,
        ) -> Result<
            Option<ControlPlaneRaftPeerFrameIdentity>,
            StreamingError<ControlPlaneRaftTypeConfig>,
        > {
            self.request_identity()
                .map_err(Self::streaming_error_from_rpc_error)
        }

        fn snapshot_response_identity(
            &self,
        ) -> Result<
            Option<ControlPlaneRaftPeerFrameIdentity>,
            StreamingError<ControlPlaneRaftTypeConfig>,
        > {
            self.response_identity()
                .map_err(Self::streaming_error_from_rpc_error)
        }

        fn streaming_error_from_rpc_error(
            error: RPCError<ControlPlaneRaftTypeConfig>,
        ) -> StreamingError<ControlPlaneRaftTypeConfig> {
            match error {
                RPCError::Timeout(error) => StreamingError::Network(NetworkError::from_string(
                    format!("peer identity validation timed out: {error}"),
                )),
                RPCError::Unreachable(error) => StreamingError::Unreachable(error),
                RPCError::Network(error) => StreamingError::Network(error),
                RPCError::RemoteError(error) => StreamingError::Network(NetworkError::from_string(
                    format!("peer identity validation remote error: {error}"),
                )),
            }
        }

        fn rpc_error_from_transport_rejection(
            rejection: ControlPlaneRaftPeerTransportRejection,
        ) -> RPCError<ControlPlaneRaftTypeConfig> {
            let message = rejection.to_string();
            match rejection {
                ControlPlaneRaftPeerTransportRejection::UnknownTarget { .. } => {
                    RPCError::Unreachable(Unreachable::new(&AnyError::error(message)))
                }
                _ => RPCError::Network(NetworkError::from_string(message)),
            }
        }

        fn streaming_error_from_transport_rejection(
            rejection: ControlPlaneRaftPeerTransportRejection,
        ) -> StreamingError<ControlPlaneRaftTypeConfig> {
            let message = rejection.to_string();
            match rejection {
                ControlPlaneRaftPeerTransportRejection::UnknownTarget { .. } => {
                    StreamingError::Unreachable(Unreachable::new(&AnyError::error(message)))
                }
                _ => StreamingError::Network(NetworkError::from_string(message)),
            }
        }

        fn rpc_protocol_error(
            context: &'static str,
            error: ControlPlaneError,
        ) -> RPCError<ControlPlaneRaftTypeConfig> {
            RPCError::Network(NetworkError::from_string(format!(
                "in-memory test raft network {context} peer frame failed: {error:?}"
            )))
        }

        fn streaming_protocol_error(
            context: &'static str,
            error: ControlPlaneError,
        ) -> StreamingError<ControlPlaneRaftTypeConfig> {
            StreamingError::Network(NetworkError::from_string(format!(
                "in-memory test raft network {context} peer frame failed: {error:?}"
            )))
        }

        fn target_raft(
            &self,
            rpc_name: &'static str,
        ) -> Result<
            Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
            RPCError<ControlPlaneRaftTypeConfig>,
        > {
            let peers = self.peers.lock().map_err(|_| {
                RPCError::Network(NetworkError::from_string(
                    "in-memory test raft network registry lock poisoned",
                ))
            })?;
            peers.get(&self.target).cloned().ok_or_else(|| {
                RPCError::Unreachable(Unreachable::new(&AnyError::error(format!(
                    "in-memory test raft network has no target {} for {rpc_name}",
                    self.target
                ))))
            })
        }
    }

    impl fmt::Debug for InMemoryRaftNetwork {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("InMemoryRaftNetwork")
                .field("target", &self.target)
                .finish_non_exhaustive()
        }
    }

    impl RaftNetworkV2<ControlPlaneRaftTypeConfig> for InMemoryRaftNetwork {
        async fn append_entries(
            &mut self,
            rpc: AppendEntriesRequest<ControlPlaneRaftTypeConfig>,
            _option: RPCOption,
        ) -> Result<
            AppendEntriesResponse<ControlPlaneRaftTypeConfig>,
            RPCError<ControlPlaneRaftTypeConfig>,
        > {
            self.validate_peer("append_entries")?;
            if let Some(policy) = &self.policy {
                policy
                    .validate_append_entries(
                        self.target,
                        rpc.entries.len(),
                        Self::encoded_append_entries_payload_len(&rpc.entries)?,
                    )
                    .map_err(Self::rpc_error_from_transport_rejection)?;
            }
            let request_identity = self.request_identity()?;
            let encoded = ControlPlaneRaftPeerRpcRequest::AppendEntries(rpc)
                .encode_frame_with_identity(request_identity.as_ref())
                .map_err(|error| Self::rpc_protocol_error("append_entries encode", error))?;
            let response_frame = handle_control_plane_raft_peer_rpc_frame_with_identity(
                &self.target_raft("append_entries")?,
                &encoded,
                request_identity.as_ref(),
            )
            .await
            .map_err(|error| Self::rpc_protocol_error("append_entries dispatch", error))?;
            let response_identity = self.response_identity()?;
            let ControlPlaneRaftPeerRpcResponse::AppendEntries(response) =
                ControlPlaneRaftPeerRpcResponse::decode_frame_with_identity(
                    &response_frame,
                    response_identity.as_ref(),
                )
                .map_err(|error| {
                    Self::rpc_protocol_error("append_entries response decode", error)
                })?
            else {
                return Err(Self::rpc_protocol_error(
                    "append_entries response decode",
                    raft_artifact_protocol_error("decoded non-append_entries response frame"),
                ));
            };
            Ok(response)
        }

        async fn vote(
            &mut self,
            rpc: VoteRequest<ControlPlaneRaftTypeConfig>,
            _option: RPCOption,
        ) -> Result<VoteResponse<ControlPlaneRaftTypeConfig>, RPCError<ControlPlaneRaftTypeConfig>>
        {
            self.validate_peer("vote")?;
            let request_identity = self.request_identity()?;
            let encoded = ControlPlaneRaftPeerRpcRequest::Vote(rpc)
                .encode_frame_with_identity(request_identity.as_ref())
                .map_err(|error| Self::rpc_protocol_error("vote encode", error))?;
            let response_frame = handle_control_plane_raft_peer_rpc_frame_with_identity(
                &self.target_raft("vote")?,
                &encoded,
                request_identity.as_ref(),
            )
            .await
            .map_err(|error| Self::rpc_protocol_error("vote dispatch", error))?;
            let response_identity = self.response_identity()?;
            let ControlPlaneRaftPeerRpcResponse::Vote(response) =
                ControlPlaneRaftPeerRpcResponse::decode_frame_with_identity(
                    &response_frame,
                    response_identity.as_ref(),
                )
                .map_err(|error| Self::rpc_protocol_error("vote response decode", error))?
            else {
                return Err(Self::rpc_protocol_error(
                    "vote response decode",
                    raft_artifact_protocol_error("decoded non-vote response frame"),
                ));
            };
            Ok(response)
        }

        async fn pre_vote(
            &mut self,
            rpc: VoteRequest<ControlPlaneRaftTypeConfig>,
            _option: RPCOption,
        ) -> Result<VoteResponse<ControlPlaneRaftTypeConfig>, RPCError<ControlPlaneRaftTypeConfig>>
        {
            self.validate_peer("pre_vote")?;
            let request_identity = self.request_identity()?;
            let encoded = ControlPlaneRaftPeerRpcRequest::PreVote(rpc)
                .encode_frame_with_identity(request_identity.as_ref())
                .map_err(|error| Self::rpc_protocol_error("pre_vote encode", error))?;
            let response_frame = handle_control_plane_raft_peer_rpc_frame_with_identity(
                &self.target_raft("pre_vote")?,
                &encoded,
                request_identity.as_ref(),
            )
            .await
            .map_err(|error| Self::rpc_protocol_error("pre_vote dispatch", error))?;
            let response_identity = self.response_identity()?;
            let ControlPlaneRaftPeerRpcResponse::Vote(response) =
                ControlPlaneRaftPeerRpcResponse::decode_frame_with_identity(
                    &response_frame,
                    response_identity.as_ref(),
                )
                .map_err(|error| Self::rpc_protocol_error("pre_vote response decode", error))?
            else {
                return Err(Self::rpc_protocol_error(
                    "pre_vote response decode",
                    raft_artifact_protocol_error("decoded non-vote response frame"),
                ));
            };
            Ok(response)
        }

        async fn full_snapshot(
            &mut self,
            vote: VoteOf<ControlPlaneRaftTypeConfig>,
            snapshot: SnapshotOf<ControlPlaneRaftTypeConfig>,
            _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
            _option: RPCOption,
        ) -> Result<
            SnapshotResponse<ControlPlaneRaftTypeConfig>,
            StreamingError<ControlPlaneRaftTypeConfig>,
        > {
            self.validate_peer("full_snapshot")
                .map_err(|error| match error {
                    RPCError::Timeout(error) => StreamingError::Network(NetworkError::from_string(
                        format!("full_snapshot peer validation timed out: {error}"),
                    )),
                    RPCError::Unreachable(error) => StreamingError::Unreachable(error),
                    RPCError::Network(error) => StreamingError::Network(error),
                    RPCError::RemoteError(error) => {
                        StreamingError::Network(NetworkError::from_string(format!(
                            "full_snapshot peer validation remote error: {error}"
                        )))
                    }
                })?;
            if let Some(policy) = &self.policy {
                policy
                    .validate_snapshot(self.target, snapshot.snapshot.get_ref().len())
                    .map_err(Self::streaming_error_from_transport_rejection)?;
            }
            let max_snapshot_bytes = self
                .policy
                .as_ref()
                .map_or(usize::MAX, |policy| policy.limits.max_snapshot_bytes);
            let request = ControlPlaneRaftPeerSnapshotRequest { vote, snapshot };
            let request_identity = self.snapshot_request_identity()?;
            let encoded = request
                .encode_frame_with_identity(request_identity.as_ref())
                .map_err(|error| Self::streaming_protocol_error("full_snapshot encode", error))?;
            let response_frame = handle_control_plane_raft_peer_snapshot_frame_with_identity(
                &self.target_raft("full_snapshot")?,
                &encoded,
                usize::MAX,
                max_snapshot_bytes,
                request_identity.as_ref(),
            )
            .await
            .map_err(|error| Self::streaming_protocol_error("full_snapshot dispatch", error))?;
            let response_identity = self.snapshot_response_identity()?;
            let response = ControlPlaneRaftPeerSnapshotResponse::decode_frame_with_identity(
                &response_frame,
                response_identity.as_ref(),
            )
            .map_err(|error| {
                Self::streaming_protocol_error("full_snapshot response decode", error)
            })?
            .response;
            Ok(response)
        }

        async fn transfer_leader(
            &mut self,
            req: TransferLeaderRequest<ControlPlaneRaftTypeConfig>,
            _option: RPCOption,
        ) -> Result<
            TransferLeaderResponse<ControlPlaneRaftTypeConfig>,
            RPCError<ControlPlaneRaftTypeConfig>,
        > {
            self.validate_peer("transfer_leader")?;
            let request_identity = self.request_identity()?;
            let encoded = ControlPlaneRaftPeerRpcRequest::TransferLeader(req)
                .encode_frame_with_identity(request_identity.as_ref())
                .map_err(|error| Self::rpc_protocol_error("transfer_leader encode", error))?;
            let response_frame = handle_control_plane_raft_peer_rpc_frame_with_identity(
                &self.target_raft("transfer_leader")?,
                &encoded,
                request_identity.as_ref(),
            )
            .await
            .map_err(|error| Self::rpc_protocol_error("transfer_leader dispatch", error))?;
            let response_identity = self.response_identity()?;
            let ControlPlaneRaftPeerRpcResponse::TransferLeader(response) =
                ControlPlaneRaftPeerRpcResponse::decode_frame_with_identity(
                    &response_frame,
                    response_identity.as_ref(),
                )
                .map_err(|error| {
                    Self::rpc_protocol_error("transfer_leader response decode", error)
                })?
            else {
                return Err(Self::rpc_protocol_error(
                    "transfer_leader response decode",
                    raft_artifact_protocol_error("decoded non-transfer_leader response frame"),
                ));
            };
            Ok(response)
        }
    }

    fn test_peer_transport_policy() -> ControlPlaneRaftPeerTransportPolicy {
        ControlPlaneRaftPeerTransportPolicy::new(
            "control-plane-raft-peer-transport-test",
            BTreeMap::from([(1, BasicNode::new("node-1")), (2, BasicNode::new("node-2"))]),
            ControlPlaneRaftPeerTransportLimits {
                max_frame_bytes: 4096,
                max_append_entries: 1,
                max_append_entries_bytes: 128,
                max_snapshot_bytes: 0,
            },
        )
    }

    async fn test_policy_network_client(
        target: ControlPlaneRaftNodeId,
        node: &BasicNode,
    ) -> InMemoryRaftNetwork {
        let mut factory =
            InMemoryRaftNetworkFactory::with_transport_policy(test_peer_transport_policy())
                .for_local_node(1);
        factory.new_client(target, node).await
    }

    fn refresh_raft_peer_frame_checksum(frame: &mut Vec<u8>) {
        let checksum_start = frame.len() - CONTROL_PLANE_RAFT_PEER_RPC_CHECKSUM_LEN;
        frame.truncate(checksum_start);
        append_raft_artifact_checksum(frame);
    }

    static RAFT_UNIX_SOCKET_COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn raft_unix_socket_path(test_name: &str) -> PathBuf {
        let sequence = RAFT_UNIX_SOCKET_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "argmin-control-plane-raft-{test_name}-{}-{sequence}.sock",
            std::process::id()
        ))
    }

    #[test]
    fn control_plane_raft_peer_rpc_append_entries_request_frame_round_trips() {
        let request = AppendEntriesRequest {
            vote: Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 7),
            prev_log_id: Some(raft_log_id(3, 7, 4)),
            entries: vec![normal_entry(
                3,
                7,
                5,
                ControlPlaneCommand::SetNodeMembership {
                    node_id: NodeId::new(11),
                    membership: NodeMembershipState::Active,
                },
            )],
            leader_commit: Some(raft_log_id(3, 7, 5)),
        };
        let encoded = ControlPlaneRaftPeerRpcRequest::AppendEntries(request.clone())
            .encode_frame()
            .unwrap();
        let decoded = ControlPlaneRaftPeerRpcRequest::decode_frame(&encoded).unwrap();

        let ControlPlaneRaftPeerRpcRequest::AppendEntries(decoded) = decoded else {
            panic!("decoded wrong peer RPC request variant");
        };
        assert_eq!(decoded.vote, request.vote);
        assert_eq!(decoded.prev_log_id, request.prev_log_id);
        assert_eq!(decoded.entries, request.entries);
        assert_eq!(decoded.leader_commit, request.leader_commit);
    }

    #[test]
    fn control_plane_raft_peer_rpc_vote_request_frames_round_trip() {
        let request = VoteRequest {
            vote: Vote::<ControlPlaneRaftLeaderId>::new(4, 9),
            last_log_id: Some(raft_log_id(3, 7, 8)),
            leadership_transfer: true,
        };
        let encoded = ControlPlaneRaftPeerRpcRequest::Vote(request.clone())
            .encode_frame()
            .unwrap();
        let decoded = ControlPlaneRaftPeerRpcRequest::decode_frame(&encoded).unwrap();
        let ControlPlaneRaftPeerRpcRequest::Vote(decoded) = decoded else {
            panic!("decoded wrong vote peer RPC request variant");
        };
        assert_eq!(decoded, request);

        let encoded = ControlPlaneRaftPeerRpcRequest::PreVote(request.clone())
            .encode_frame()
            .unwrap();
        let decoded = ControlPlaneRaftPeerRpcRequest::decode_frame(&encoded).unwrap();
        let ControlPlaneRaftPeerRpcRequest::PreVote(decoded) = decoded else {
            panic!("decoded wrong pre-vote peer RPC request variant");
        };
        assert_eq!(decoded, request);
    }

    #[test]
    fn control_plane_raft_peer_rpc_response_frames_round_trip() {
        let append = AppendEntriesResponse::HigherVote(Vote::<ControlPlaneRaftLeaderId>::new(5, 9));
        let encoded = ControlPlaneRaftPeerRpcResponse::AppendEntries(append.clone())
            .encode_frame()
            .unwrap();
        let decoded = ControlPlaneRaftPeerRpcResponse::decode_frame(&encoded).unwrap();
        assert_eq!(
            decoded,
            ControlPlaneRaftPeerRpcResponse::AppendEntries(append)
        );

        let vote = VoteResponse {
            vote: Vote::<ControlPlaneRaftLeaderId>::new_committed(5, 9),
            vote_granted: true,
            last_log_id: Some(raft_log_id(5, 9, 12)),
        };
        let encoded = ControlPlaneRaftPeerRpcResponse::Vote(vote.clone())
            .encode_frame()
            .unwrap();
        let decoded = ControlPlaneRaftPeerRpcResponse::decode_frame(&encoded).unwrap();
        assert_eq!(decoded, ControlPlaneRaftPeerRpcResponse::Vote(vote));
    }

    #[test]
    fn control_plane_raft_peer_rpc_transfer_leader_frames_round_trip() {
        let request = TransferLeaderRequest::new(
            Vote::<ControlPlaneRaftLeaderId>::new_committed(6, 1),
            2,
            Some(raft_log_id(6, 1, 14)),
        );
        let identity =
            ControlPlaneRaftPeerFrameIdentity::new("control-plane-transfer-leader-frame", 1, 2);
        let encoded = ControlPlaneRaftPeerRpcRequest::TransferLeader(request.clone())
            .encode_frame_for_peer(&identity)
            .unwrap();
        let decoded =
            ControlPlaneRaftPeerRpcRequest::decode_frame_for_peer(&encoded, &identity).unwrap();
        let ControlPlaneRaftPeerRpcRequest::TransferLeader(decoded) = decoded else {
            panic!("decoded wrong transfer_leader request variant");
        };
        assert_eq!(decoded, request);

        let success = ControlPlaneRaftPeerRpcResponse::TransferLeader(Ok(()));
        let encoded = success.encode_frame_for_peer(&identity).unwrap();
        assert_eq!(
            ControlPlaneRaftPeerRpcResponse::decode_frame_for_peer(&encoded, &identity).unwrap(),
            success
        );

        let vote_changed = ControlPlaneRaftPeerRpcResponse::TransferLeader(Err(
            TransferLeaderError::VoteChanged {
                expected: Vote::<ControlPlaneRaftLeaderId>::new_committed(6, 1),
                actual: Vote::<ControlPlaneRaftLeaderId>::new(7, 2),
            },
        ));
        let encoded = vote_changed.encode_frame_for_peer(&identity).unwrap();
        assert_eq!(
            ControlPlaneRaftPeerRpcResponse::decode_frame_for_peer(&encoded, &identity).unwrap(),
            vote_changed
        );

        let log_not_flushed = ControlPlaneRaftPeerRpcResponse::TransferLeader(Err(
            TransferLeaderError::LogNotFlushed {
                expected: Some(raft_log_id(6, 1, 14)),
                actual: Some(raft_log_id(6, 2, 12)),
            },
        ));
        let encoded = log_not_flushed.encode_frame_for_peer(&identity).unwrap();
        assert_eq!(
            ControlPlaneRaftPeerRpcResponse::decode_frame_for_peer(&encoded, &identity).unwrap(),
            log_not_flushed
        );
    }

    #[test]
    fn control_plane_raft_peer_snapshot_frames_round_trip() {
        let mut state_machine = ControlPlaneRaftStateMachine::empty();
        state_machine
            .apply_entry(bootstrap_membership_entry(1))
            .unwrap();
        let snapshot = state_machine.build_snapshot().unwrap();
        assert!(!snapshot.snapshot.get_ref().is_empty());

        let request = ControlPlaneRaftPeerSnapshotRequest {
            vote: Vote::<ControlPlaneRaftLeaderId>::new_committed(1, 1),
            snapshot: snapshot.clone(),
        };
        let encoded = request.encode_frame().unwrap();
        let decoded =
            ControlPlaneRaftPeerSnapshotRequest::decode_frame(&encoded, usize::MAX, usize::MAX)
                .unwrap();
        assert_eq!(decoded.vote, request.vote);
        assert_eq!(decoded.snapshot.meta, request.snapshot.meta);
        assert_eq!(
            decoded.snapshot.snapshot.get_ref(),
            request.snapshot.snapshot.get_ref()
        );

        let response = ControlPlaneRaftPeerSnapshotResponse {
            response: SnapshotResponse::new(Vote::<ControlPlaneRaftLeaderId>::new_committed(2, 1)),
        };
        let encoded = response.encode_frame().unwrap();
        let decoded = ControlPlaneRaftPeerSnapshotResponse::decode_frame(&encoded).unwrap();
        assert_eq!(decoded, response);
    }

    #[test]
    fn control_plane_raft_peer_snapshot_frames_fail_closed_across_direction() {
        let mut state_machine = ControlPlaneRaftStateMachine::empty();
        state_machine
            .apply_entry(bootstrap_membership_entry(1))
            .unwrap();
        let snapshot = state_machine.build_snapshot().unwrap();

        let request = ControlPlaneRaftPeerSnapshotRequest {
            vote: Vote::<ControlPlaneRaftLeaderId>::new_committed(1, 1),
            snapshot,
        };
        let encoded_request = request.encode_frame().unwrap();
        let err = ControlPlaneRaftPeerSnapshotResponse::decode_frame(&encoded_request).unwrap_err();
        assert!(matches!(
            err,
            ControlPlaneError::CommandDecode { message }
                if message.contains("does not match expected kind")
        ));

        let response = ControlPlaneRaftPeerSnapshotResponse {
            response: SnapshotResponse::new(Vote::<ControlPlaneRaftLeaderId>::new(3, 1)),
        };
        let encoded_response = response.encode_frame().unwrap();
        let err = ControlPlaneRaftPeerSnapshotRequest::decode_frame(
            &encoded_response,
            usize::MAX,
            usize::MAX,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            ControlPlaneError::CommandDecode { message }
                if message.contains("does not match expected kind")
        ));
    }

    #[test]
    fn control_plane_raft_peer_snapshot_request_decode_enforces_size_limits() {
        let mut state_machine = ControlPlaneRaftStateMachine::empty();
        state_machine
            .apply_entry(bootstrap_membership_entry(1))
            .unwrap();
        let snapshot = state_machine.build_snapshot().unwrap();
        let payload_len = snapshot.snapshot.get_ref().len();
        assert!(payload_len > 0);

        let request = ControlPlaneRaftPeerSnapshotRequest {
            vote: Vote::<ControlPlaneRaftLeaderId>::new_committed(1, 1),
            snapshot,
        };
        let encoded = request.encode_frame().unwrap();
        let err = ControlPlaneRaftPeerSnapshotRequest::decode_frame(
            &encoded,
            encoded.len() - 1,
            usize::MAX,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            ControlPlaneError::CommandDecode { message }
                if message.contains("peer snapshot request frame size")
                    && message.contains("exceeds limit")
        ));

        let err = ControlPlaneRaftPeerSnapshotRequest::decode_frame(
            &encoded,
            usize::MAX,
            payload_len - 1,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            ControlPlaneError::CommandDecode { message }
                if message.contains("raft peer snapshot payload length")
                    && message.contains("exceeds limit")
        ));
    }

    #[test]
    fn control_plane_raft_peer_rpc_frames_fail_closed_across_direction() {
        let request = VoteRequest {
            vote: Vote::<ControlPlaneRaftLeaderId>::new(4, 9),
            last_log_id: None,
            leadership_transfer: false,
        };
        let encoded_request = ControlPlaneRaftPeerRpcRequest::Vote(request)
            .encode_frame()
            .unwrap();
        let err = ControlPlaneRaftPeerRpcResponse::decode_frame(&encoded_request).unwrap_err();
        assert!(matches!(
            err,
            ControlPlaneError::CommandDecode { message }
                if message.contains("does not match expected kind")
        ));

        let response = VoteResponse {
            vote: Vote::<ControlPlaneRaftLeaderId>::new(4, 9),
            vote_granted: false,
            last_log_id: None,
        };
        let encoded_response = ControlPlaneRaftPeerRpcResponse::Vote(response)
            .encode_frame()
            .unwrap();
        let err = ControlPlaneRaftPeerRpcRequest::decode_frame(&encoded_response).unwrap_err();
        assert!(matches!(
            err,
            ControlPlaneError::CommandDecode { message }
                if message.contains("does not match expected kind")
        ));
    }

    #[test]
    fn control_plane_raft_peer_rpc_frame_decode_fails_closed() {
        let request = VoteRequest {
            vote: Vote::<ControlPlaneRaftLeaderId>::new(4, 9),
            last_log_id: Some(raft_log_id(3, 7, 8)),
            leadership_transfer: false,
        };
        let encoded = ControlPlaneRaftPeerRpcRequest::Vote(request)
            .encode_frame()
            .unwrap();

        let err = ControlPlaneRaftPeerRpcRequest::decode_frame(&encoded[..4]).unwrap_err();
        assert!(matches!(
            err,
            ControlPlaneError::CommandDecode { message }
                if message.contains("truncated control-plane OpenRaft peer RPC frame")
        ));

        let mut bad_magic = encoded.clone();
        bad_magic[0] ^= 1;
        refresh_raft_peer_frame_checksum(&mut bad_magic);
        let err = ControlPlaneRaftPeerRpcRequest::decode_frame(&bad_magic).unwrap_err();
        assert!(matches!(
            err,
            ControlPlaneError::CommandDecode { message }
                if message.contains("invalid control-plane OpenRaft peer RPC frame magic")
        ));

        let mut unsupported_version = encoded.clone();
        unsupported_version[CONTROL_PLANE_RAFT_PEER_RPC_MAGIC.len() + 1] =
            CONTROL_PLANE_RAFT_PEER_RPC_VERSION as u8 + 1;
        refresh_raft_peer_frame_checksum(&mut unsupported_version);
        let err = ControlPlaneRaftPeerRpcRequest::decode_frame(&unsupported_version).unwrap_err();
        assert!(matches!(
            err,
            ControlPlaneError::CommandDecode { message }
                if message.contains("unsupported control-plane OpenRaft peer RPC frame version")
        ));

        let mut bad_checksum = encoded.clone();
        bad_checksum[CONTROL_PLANE_RAFT_PEER_RPC_MAGIC.len() + 2] ^= 1;
        let err = ControlPlaneRaftPeerRpcRequest::decode_frame(&bad_checksum).unwrap_err();
        assert!(matches!(
            err,
            ControlPlaneError::CommandDecode { message }
                if message.contains("control-plane OpenRaft peer RPC frame checksum mismatch")
        ));

        let mut trailing = encoded.clone();
        let checksum_start = trailing.len() - CONTROL_PLANE_RAFT_PEER_RPC_CHECKSUM_LEN;
        trailing.insert(checksum_start, 0);
        refresh_raft_peer_frame_checksum(&mut trailing);
        let err = ControlPlaneRaftPeerRpcRequest::decode_frame(&trailing).unwrap_err();
        assert!(matches!(
            err,
            ControlPlaneError::CommandDecode { message }
                if message.contains("control-plane OpenRaft peer RPC frame has 1 trailing bytes")
        ));

        let mut unknown_tag = Vec::new();
        unknown_tag.extend_from_slice(CONTROL_PLANE_RAFT_PEER_RPC_MAGIC);
        write_raft_u16(&mut unknown_tag, CONTROL_PLANE_RAFT_PEER_RPC_VERSION);
        write_raft_u8(&mut unknown_tag, CONTROL_PLANE_RAFT_PEER_RPC_KIND_REQUEST);
        write_raft_u8(&mut unknown_tag, 0);
        write_raft_u8(&mut unknown_tag, 99);
        append_raft_artifact_checksum(&mut unknown_tag);
        let err = ControlPlaneRaftPeerRpcRequest::decode_frame(&unknown_tag).unwrap_err();
        assert!(matches!(
            err,
            ControlPlaneError::CommandDecode { message }
                if message.contains("unknown control-plane OpenRaft peer RPC request tag 99")
        ));
    }

    #[test]
    fn control_plane_raft_peer_rpc_frame_identity_fails_closed() {
        let request = VoteRequest {
            vote: Vote::<ControlPlaneRaftLeaderId>::new(4, 1),
            last_log_id: None,
            leadership_transfer: false,
        };
        let expected =
            ControlPlaneRaftPeerFrameIdentity::new("control-plane-raft-peer-identity", 1, 2);
        let encoded = ControlPlaneRaftPeerRpcRequest::Vote(request)
            .encode_frame_for_peer(&expected)
            .unwrap();

        let err = ControlPlaneRaftPeerRpcRequest::decode_frame_for_peer(
            &encoded,
            &ControlPlaneRaftPeerFrameIdentity::new("wrong-cluster", 1, 2),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            ControlPlaneError::CommandDecode { message }
                if message.contains("cluster identity mismatch")
        ));

        let err = ControlPlaneRaftPeerRpcRequest::decode_frame_for_peer(
            &encoded,
            &ControlPlaneRaftPeerFrameIdentity::new("control-plane-raft-peer-identity", 9, 2),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            ControlPlaneError::CommandDecode { message }
                if message.contains("source identity mismatch")
        ));

        let err = ControlPlaneRaftPeerRpcRequest::decode_frame_for_peer(
            &encoded,
            &ControlPlaneRaftPeerFrameIdentity::new("control-plane-raft-peer-identity", 1, 9),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            ControlPlaneError::CommandDecode { message }
                if message.contains("target identity mismatch")
        ));

        let missing_identity = ControlPlaneRaftPeerRpcResponse::Vote(VoteResponse {
            vote: Vote::<ControlPlaneRaftLeaderId>::new(4, 1),
            vote_granted: false,
            last_log_id: None,
        })
        .encode_frame()
        .unwrap();
        let err = ControlPlaneRaftPeerRpcResponse::decode_frame_for_peer(
            &missing_identity,
            &ControlPlaneRaftPeerFrameIdentity::new("control-plane-raft-peer-identity", 2, 1),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            ControlPlaneError::CommandDecode { message }
                if message.contains("missing peer identity")
        ));
    }

    #[test]
    fn control_plane_raft_peer_transport_frame_round_trips() {
        let identity =
            ControlPlaneRaftPeerFrameIdentity::new("control-plane-peer-transport-frame", 1, 2);
        let request = ControlPlaneRaftPeerRpcRequest::Vote(VoteRequest {
            vote: Vote::<ControlPlaneRaftLeaderId>::new(4, 1),
            last_log_id: Some(raft_log_id(3, 1, 7)),
            leadership_transfer: false,
        });
        let frame = request.encode_frame_for_peer(&identity).unwrap();
        let mut transport = Vec::new();
        write_control_plane_raft_peer_transport_frame(&mut transport, &frame).unwrap();

        let mut cursor = Cursor::new(transport);
        let decoded_frame =
            read_control_plane_raft_peer_transport_frame(&mut cursor, frame.len()).unwrap();
        assert_eq!(decoded_frame, frame);
        let decoded =
            ControlPlaneRaftPeerRpcRequest::decode_frame_for_peer(&decoded_frame, &identity)
                .unwrap();
        assert!(matches!(decoded, ControlPlaneRaftPeerRpcRequest::Vote(_)));
    }

    #[test]
    fn control_plane_raft_peer_transport_frame_rejects_oversized_prefix_before_payload_read() {
        let max_frame_bytes = 8usize;
        let mut transport = Vec::new();
        write_raft_u32(&mut transport, u32::try_from(max_frame_bytes + 1).unwrap());
        let mut cursor = Cursor::new(transport);

        let err =
            read_control_plane_raft_peer_transport_frame(&mut cursor, max_frame_bytes).unwrap_err();
        assert!(matches!(
            err,
            ControlPlaneError::RpcProtocol { message }
                if message.contains("peer transport frame size 9 bytes exceeds limit 8")
        ));
    }

    #[test]
    fn control_plane_raft_peer_transport_rejects_endpoint_mismatch() {
        ControlPlaneRaftTypeConfig::run(async {
            let mut network = test_policy_network_client(2, &BasicNode::new("wrong-node-2")).await;
            let err = network
                .vote(
                    VoteRequest {
                        vote: Vote::<ControlPlaneRaftLeaderId>::new(1, 1),
                        last_log_id: None,
                        leadership_transfer: false,
                    },
                    RPCOption::new(Duration::from_millis(10)),
                )
                .await
                .unwrap_err();

            assert!(matches!(
                err,
                RPCError::Network(error)
                    if error.to_string().contains("endpoint mismatch")
                        && error.to_string().contains("expected node-2")
                        && error.to_string().contains("got wrong-node-2")
            ));
        });
    }

    #[test]
    fn control_plane_raft_peer_transport_rejects_unconfigured_target() {
        ControlPlaneRaftTypeConfig::run(async {
            let mut network = test_policy_network_client(3, &BasicNode::new("node-3")).await;
            let err = network
                .vote(
                    VoteRequest {
                        vote: Vote::<ControlPlaneRaftLeaderId>::new(1, 1),
                        last_log_id: None,
                        leadership_transfer: false,
                    },
                    RPCOption::new(Duration::from_millis(10)),
                )
                .await
                .unwrap_err();

            assert!(matches!(
                err,
                RPCError::Unreachable(error)
                    if error.to_string().contains("no configured target node 3")
            ));
        });
    }

    #[test]
    fn control_plane_raft_peer_transport_rejects_oversized_append_entries_batch() {
        ControlPlaneRaftTypeConfig::run(async {
            let mut network = test_policy_network_client(2, &BasicNode::new("node-2")).await;
            let err = network
                .append_entries(
                    AppendEntriesRequest {
                        vote: Vote::<ControlPlaneRaftLeaderId>::new_committed(1, 1),
                        prev_log_id: None,
                        entries: vec![blank_entry(1, 1, 1), blank_entry(1, 1, 2)],
                        leader_commit: None,
                    },
                    RPCOption::new(Duration::from_millis(10)),
                )
                .await
                .unwrap_err();

            assert!(matches!(
                err,
                RPCError::Network(error)
                    if error.to_string().contains("2 entries exceeds limit 1")
            ));
        });
    }

    #[test]
    fn control_plane_raft_peer_transport_rejects_oversized_append_entries_payload() {
        ControlPlaneRaftTypeConfig::run(async {
            let mut network = test_policy_network_client(2, &BasicNode::new("node-2")).await;
            let err = network
                .append_entries(
                    AppendEntriesRequest {
                        vote: Vote::<ControlPlaneRaftLeaderId>::new_committed(1, 1),
                        prev_log_id: None,
                        entries: vec![normal_entry(
                            1,
                            1,
                            1,
                            ControlPlaneCommand::BootstrapInitialClusterMap {
                                nodes: vec![(NodeId::new(1), "x".repeat(1024))],
                                pg_ids: vec![],
                            },
                        )],
                        leader_commit: None,
                    },
                    RPCOption::new(Duration::from_millis(10)),
                )
                .await
                .unwrap_err();

            assert!(matches!(
                err,
                RPCError::Network(error)
                    if error.to_string().contains("encoded entries payload")
                        && error.to_string().contains("exceeds limit 128")
            ));
        });
    }

    #[test]
    fn control_plane_raft_peer_transport_rejects_oversized_snapshot() {
        ControlPlaneRaftTypeConfig::run(async {
            let mut network = test_policy_network_client(2, &BasicNode::new("node-2")).await;
            let mut state_machine = ControlPlaneRaftStateMachine::empty();
            state_machine
                .apply_entry(bootstrap_membership_entry(1))
                .unwrap();
            let snapshot = state_machine.build_snapshot().unwrap();
            assert!(!snapshot.snapshot.get_ref().is_empty());

            let err = network
                .full_snapshot(
                    Vote::<ControlPlaneRaftLeaderId>::new_committed(1, 1),
                    snapshot,
                    std::future::pending::<ReplicationClosed>(),
                    RPCOption::new(Duration::from_millis(10)),
                )
                .await
                .unwrap_err();

            assert!(matches!(
                err,
                StreamingError::Network(error)
                    if error.to_string().contains("bytes exceeds limit 0")
            ));
        });
    }

    #[test]
    fn control_plane_raft_unix_peer_network_vote_round_trips_framed_identity() {
        ControlPlaneRaftTypeConfig::run(async {
            let socket_path = raft_unix_socket_path("vote-round-trip");
            let listener = UnixListener::bind(&socket_path).unwrap();
            let limits = ControlPlaneRaftPeerTransportLimits {
                max_frame_bytes: 4096,
                max_append_entries: 8,
                max_append_entries_bytes: 4096,
                max_snapshot_bytes: 4096,
            };
            let expected_request_identity =
                ControlPlaneRaftPeerFrameIdentity::new("control-plane-raft-unix-peer-test", 1, 2);
            let server_identity = expected_request_identity.clone();
            let server_path = socket_path.clone();
            let server = thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(1)))
                    .unwrap();
                stream
                    .set_write_timeout(Some(Duration::from_secs(1)))
                    .unwrap();

                let request_frame = read_control_plane_raft_peer_transport_frame(
                    &mut stream,
                    limits.max_frame_bytes,
                )
                .unwrap();
                let request = ControlPlaneRaftPeerRpcRequest::decode_frame_for_peer(
                    &request_frame,
                    &server_identity,
                )
                .unwrap();
                let ControlPlaneRaftPeerRpcRequest::Vote(request) = request else {
                    panic!("decoded wrong Unix peer request variant");
                };
                assert_eq!(request.vote, Vote::<ControlPlaneRaftLeaderId>::new(7, 1));
                assert_eq!(request.last_log_id, Some(raft_log_id(6, 1, 10)));
                assert!(!request.leadership_transfer);

                let response = ControlPlaneRaftPeerRpcResponse::Vote(VoteResponse {
                    vote: Vote::<ControlPlaneRaftLeaderId>::new_committed(8, 2),
                    vote_granted: true,
                    last_log_id: Some(raft_log_id(7, 2, 11)),
                });
                let response_frame = response
                    .encode_frame_for_peer(&reverse_raft_peer_frame_identity(&server_identity))
                    .unwrap();
                write_control_plane_raft_peer_transport_frame(&mut stream, &response_frame)
                    .unwrap();
                let _ = std::fs::remove_file(server_path);
            });

            let node = BasicNode::new(socket_path.display().to_string());
            let policy = ControlPlaneRaftPeerTransportPolicy::new(
                "control-plane-raft-unix-peer-test",
                BTreeMap::from([(1, BasicNode::new("node-1")), (2, node.clone())]),
                limits,
            );
            let mut factory =
                ControlPlaneRaftUnixPeerNetworkFactory::new(1, policy, Duration::from_secs(1));
            let mut network = factory.new_client(2, &node).await;
            let response = network
                .vote(
                    VoteRequest {
                        vote: Vote::<ControlPlaneRaftLeaderId>::new(7, 1),
                        last_log_id: Some(raft_log_id(6, 1, 10)),
                        leadership_transfer: false,
                    },
                    RPCOption::new(Duration::from_secs(1)),
                )
                .await
                .unwrap();

            assert_eq!(
                response,
                VoteResponse {
                    vote: Vote::<ControlPlaneRaftLeaderId>::new_committed(8, 2),
                    vote_granted: true,
                    last_log_id: Some(raft_log_id(7, 2, 11)),
                }
            );
            server.join().unwrap();
            let _ = std::fs::remove_file(socket_path);
        });
    }

    #[test]
    fn control_plane_raft_unix_peer_network_read_eof_is_unreachable() {
        ControlPlaneRaftTypeConfig::run(async {
            let socket_path = raft_unix_socket_path("vote-eof");
            let listener = UnixListener::bind(&socket_path).unwrap();
            let limits = ControlPlaneRaftPeerTransportLimits {
                max_frame_bytes: 4096,
                max_append_entries: 8,
                max_append_entries_bytes: 4096,
                max_snapshot_bytes: 4096,
            };
            let server_path = socket_path.clone();
            let server = thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                let _ = read_control_plane_raft_peer_transport_frame(
                    &mut stream,
                    limits.max_frame_bytes,
                )
                .unwrap();
                drop(stream);
                let _ = std::fs::remove_file(server_path);
            });

            let node = BasicNode::new(socket_path.display().to_string());
            let policy = ControlPlaneRaftPeerTransportPolicy::new(
                "control-plane-raft-unix-peer-eof-test",
                BTreeMap::from([(1, BasicNode::new("node-1")), (2, node.clone())]),
                limits,
            );
            let mut factory =
                ControlPlaneRaftUnixPeerNetworkFactory::new(1, policy, Duration::from_secs(1));
            let mut network = factory.new_client(2, &node).await;
            let err = network
                .vote(
                    VoteRequest {
                        vote: Vote::<ControlPlaneRaftLeaderId>::new(7, 1),
                        last_log_id: None,
                        leadership_transfer: false,
                    },
                    RPCOption::new(Duration::from_secs(1)),
                )
                .await
                .unwrap_err();

            assert!(matches!(
                err,
                RPCError::Unreachable(error)
                    if error.to_string().contains("read transport")
            ));
            server.join().unwrap();
            let _ = std::fs::remove_file(socket_path);
        });
    }

    #[test]
    fn control_plane_raft_unix_peer_snapshot_read_eof_is_unreachable() {
        ControlPlaneRaftTypeConfig::run(async {
            let socket_path = raft_unix_socket_path("snapshot-eof");
            let listener = UnixListener::bind(&socket_path).unwrap();
            let limits = ControlPlaneRaftPeerTransportLimits {
                max_frame_bytes: 8192,
                max_append_entries: 8,
                max_append_entries_bytes: 4096,
                max_snapshot_bytes: 8192,
            };
            let server_path = socket_path.clone();
            let server = thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                let _ = read_control_plane_raft_peer_transport_frame(
                    &mut stream,
                    limits.max_frame_bytes,
                )
                .unwrap();
                drop(stream);
                let _ = std::fs::remove_file(server_path);
            });

            let node = BasicNode::new(socket_path.display().to_string());
            let policy = ControlPlaneRaftPeerTransportPolicy::new(
                "control-plane-raft-unix-peer-snapshot-eof-test",
                BTreeMap::from([(1, BasicNode::new("node-1")), (2, node.clone())]),
                limits,
            );
            let mut state_machine = ControlPlaneRaftStateMachine::empty();
            state_machine
                .apply_entry(bootstrap_membership_entry(1))
                .unwrap();
            let snapshot = state_machine.build_snapshot().unwrap();

            let mut factory =
                ControlPlaneRaftUnixPeerNetworkFactory::new(1, policy, Duration::from_secs(1));
            let mut network = factory.new_client(2, &node).await;
            let err = network
                .full_snapshot(
                    Vote::<ControlPlaneRaftLeaderId>::new_committed(7, 1),
                    snapshot,
                    std::future::pending::<ReplicationClosed>(),
                    RPCOption::new(Duration::from_secs(1)),
                )
                .await
                .unwrap_err();

            assert!(matches!(
                err,
                StreamingError::Unreachable(error)
                    if error.to_string().contains("full_snapshot read transport")
            ));
            server.join().unwrap();
            let _ = std::fs::remove_file(socket_path);
        });
    }

    #[test]
    fn control_plane_raft_peer_unix_stream_handler_dispatches_vote() {
        ControlPlaneRaftTypeConfig::run(async {
            let source_node_id = 7_001;
            let target_node_id = 7_002;
            let authority = ControlPlaneRaftAuthority::new_experimental_single_node_in_memory(
                "control-plane-raft-unix-peer-handler-vote-test",
                target_node_id,
            )
            .await
            .unwrap();
            authority
                .initialize_single_node_membership(target_node_id)
                .await
                .unwrap();
            let (mut server_stream, mut client_stream) = UnixStream::pair().unwrap();
            let limits = ControlPlaneRaftPeerTransportLimits {
                max_frame_bytes: 4096,
                max_append_entries: 8,
                max_append_entries_bytes: 4096,
                max_snapshot_bytes: 4096,
            };
            let request_identity = ControlPlaneRaftPeerFrameIdentity::new(
                "control-plane-raft-unix-handler",
                source_node_id,
                target_node_id,
            );
            let client_identity = request_identity.clone();
            let client = thread::spawn(move || {
                let request = ControlPlaneRaftPeerRpcRequest::Vote(VoteRequest {
                    vote: Vote::<ControlPlaneRaftLeaderId>::new(3, source_node_id),
                    last_log_id: None,
                    leadership_transfer: false,
                });
                let request_frame = request.encode_frame_for_peer(&client_identity).unwrap();
                write_control_plane_raft_peer_transport_frame(&mut client_stream, &request_frame)
                    .unwrap();
                let response_frame = read_control_plane_raft_peer_transport_frame(
                    &mut client_stream,
                    limits.max_frame_bytes,
                )
                .unwrap();
                ControlPlaneRaftPeerRpcResponse::decode_frame_for_peer(
                    &response_frame,
                    &reverse_raft_peer_frame_identity(&client_identity),
                )
                .unwrap()
            });

            handle_control_plane_raft_peer_unix_stream(
                authority.raft(),
                &mut server_stream,
                ControlPlaneRaftPeerFrameKind::OrdinaryRpc,
                limits,
                &request_identity,
                Duration::from_secs(1),
            )
            .await
            .unwrap();

            let response = client.join().unwrap();
            assert!(matches!(response, ControlPlaneRaftPeerRpcResponse::Vote(_)));
        });
    }

    #[test]
    fn control_plane_raft_peer_unix_stream_handler_dispatches_snapshot() {
        ControlPlaneRaftTypeConfig::run(async {
            let source_node_id = 7_011;
            let target_node_id = 7_012;
            let authority = ControlPlaneRaftAuthority::new_experimental_single_node_in_memory(
                "control-plane-raft-unix-peer-handler-snapshot-test",
                target_node_id,
            )
            .await
            .unwrap();
            let mut state_machine = ControlPlaneRaftStateMachine::empty();
            let snapshot = state_machine.build_snapshot().unwrap();
            let (mut server_stream, mut client_stream) = UnixStream::pair().unwrap();
            let limits = ControlPlaneRaftPeerTransportLimits {
                max_frame_bytes: 8192,
                max_append_entries: 8,
                max_append_entries_bytes: 4096,
                max_snapshot_bytes: 8192,
            };
            let request_identity = ControlPlaneRaftPeerFrameIdentity::new(
                "control-plane-raft-unix-handler",
                source_node_id,
                target_node_id,
            );
            let client_identity = request_identity.clone();
            let client = thread::spawn(move || {
                let request = ControlPlaneRaftPeerSnapshotRequest {
                    vote: Vote::<ControlPlaneRaftLeaderId>::new_committed(3, source_node_id),
                    snapshot,
                };
                let request_frame = request.encode_frame_for_peer(&client_identity).unwrap();
                write_control_plane_raft_peer_transport_frame(&mut client_stream, &request_frame)
                    .unwrap();
                let response_frame = read_control_plane_raft_peer_transport_frame(
                    &mut client_stream,
                    limits.max_frame_bytes,
                )
                .unwrap();
                ControlPlaneRaftPeerSnapshotResponse::decode_frame_for_peer(
                    &response_frame,
                    &reverse_raft_peer_frame_identity(&client_identity),
                )
                .unwrap()
            });

            handle_control_plane_raft_peer_unix_stream(
                authority.raft(),
                &mut server_stream,
                ControlPlaneRaftPeerFrameKind::Snapshot,
                limits,
                &request_identity,
                Duration::from_secs(1),
            )
            .await
            .unwrap();

            let response = client.join().unwrap();
            assert_eq!(
                response.response.vote,
                Vote::<ControlPlaneRaftLeaderId>::new_committed(3, source_node_id)
            );
        });
    }

    #[test]
    fn control_plane_raft_peer_request_frame_kind_rejects_response_frames() {
        let identity =
            ControlPlaneRaftPeerFrameIdentity::new("control-plane-raft-kind-detect", 1, 2);
        let response = ControlPlaneRaftPeerRpcResponse::Vote(VoteResponse {
            vote: Vote::<ControlPlaneRaftLeaderId>::new(3, 2),
            vote_granted: true,
            last_log_id: None,
        });
        let encoded_response = response.encode_frame_for_peer(&identity).unwrap();
        let err = decode_control_plane_raft_peer_request_frame_kind(&encoded_response).unwrap_err();
        assert!(matches!(
            err,
            ControlPlaneError::CommandDecode { message }
                if message.contains("response frame cannot be handled as a request")
        ));

        let snapshot_response = ControlPlaneRaftPeerSnapshotResponse {
            response: SnapshotResponse::new(Vote::<ControlPlaneRaftLeaderId>::new(3, 2)),
        };
        let encoded_snapshot_response = snapshot_response.encode_frame_for_peer(&identity).unwrap();
        let err = decode_control_plane_raft_peer_request_frame_kind(&encoded_snapshot_response)
            .unwrap_err();
        assert!(matches!(
            err,
            ControlPlaneError::CommandDecode { message }
                if message.contains("snapshot response frame cannot be handled as a request")
        ));
    }

    #[test]
    fn control_plane_raft_peer_unix_stream_handler_auto_dispatches_vote() {
        ControlPlaneRaftTypeConfig::run(async {
            let source_node_id = 7_021;
            let target_node_id = 7_022;
            let authority = ControlPlaneRaftAuthority::new_experimental_single_node_in_memory(
                "control-plane-raft-unix-peer-auto-handler-vote-test",
                target_node_id,
            )
            .await
            .unwrap();
            authority
                .initialize_single_node_membership(target_node_id)
                .await
                .unwrap();
            let (mut server_stream, mut client_stream) = UnixStream::pair().unwrap();
            let limits = ControlPlaneRaftPeerTransportLimits {
                max_frame_bytes: 4096,
                max_append_entries: 8,
                max_append_entries_bytes: 4096,
                max_snapshot_bytes: 4096,
            };
            let request_identity = ControlPlaneRaftPeerFrameIdentity::new(
                "control-plane-raft-unix-auto-handler",
                source_node_id,
                target_node_id,
            );
            let client_identity = request_identity.clone();
            let client = thread::spawn(move || {
                let request = ControlPlaneRaftPeerRpcRequest::Vote(VoteRequest {
                    vote: Vote::<ControlPlaneRaftLeaderId>::new(3, source_node_id),
                    last_log_id: None,
                    leadership_transfer: false,
                });
                let request_frame = request.encode_frame_for_peer(&client_identity).unwrap();
                write_control_plane_raft_peer_transport_frame(&mut client_stream, &request_frame)
                    .unwrap();
                let response_frame = read_control_plane_raft_peer_transport_frame(
                    &mut client_stream,
                    limits.max_frame_bytes,
                )
                .unwrap();
                ControlPlaneRaftPeerRpcResponse::decode_frame_for_peer(
                    &response_frame,
                    &reverse_raft_peer_frame_identity(&client_identity),
                )
                .unwrap()
            });

            handle_control_plane_raft_peer_unix_stream_detecting_frame_kind(
                authority.raft(),
                &mut server_stream,
                limits,
                &request_identity,
                Duration::from_secs(1),
            )
            .await
            .unwrap();

            let response = client.join().unwrap();
            assert!(matches!(response, ControlPlaneRaftPeerRpcResponse::Vote(_)));
        });
    }

    #[test]
    fn control_plane_raft_peer_unix_stream_handler_auto_dispatches_snapshot() {
        ControlPlaneRaftTypeConfig::run(async {
            let source_node_id = 7_031;
            let target_node_id = 7_032;
            let authority = ControlPlaneRaftAuthority::new_experimental_single_node_in_memory(
                "control-plane-raft-unix-peer-auto-handler-snapshot-test",
                target_node_id,
            )
            .await
            .unwrap();
            let mut state_machine = ControlPlaneRaftStateMachine::empty();
            let snapshot = state_machine.build_snapshot().unwrap();
            let (mut server_stream, mut client_stream) = UnixStream::pair().unwrap();
            let limits = ControlPlaneRaftPeerTransportLimits {
                max_frame_bytes: 8192,
                max_append_entries: 8,
                max_append_entries_bytes: 4096,
                max_snapshot_bytes: 8192,
            };
            let request_identity = ControlPlaneRaftPeerFrameIdentity::new(
                "control-plane-raft-unix-auto-handler",
                source_node_id,
                target_node_id,
            );
            let client_identity = request_identity.clone();
            let client = thread::spawn(move || {
                let request = ControlPlaneRaftPeerSnapshotRequest {
                    vote: Vote::<ControlPlaneRaftLeaderId>::new_committed(3, source_node_id),
                    snapshot,
                };
                let request_frame = request.encode_frame_for_peer(&client_identity).unwrap();
                write_control_plane_raft_peer_transport_frame(&mut client_stream, &request_frame)
                    .unwrap();
                let response_frame = read_control_plane_raft_peer_transport_frame(
                    &mut client_stream,
                    limits.max_frame_bytes,
                )
                .unwrap();
                ControlPlaneRaftPeerSnapshotResponse::decode_frame_for_peer(
                    &response_frame,
                    &reverse_raft_peer_frame_identity(&client_identity),
                )
                .unwrap()
            });

            handle_control_plane_raft_peer_unix_stream_detecting_frame_kind(
                authority.raft(),
                &mut server_stream,
                limits,
                &request_identity,
                Duration::from_secs(1),
            )
            .await
            .unwrap();

            let response = client.join().unwrap();
            assert_eq!(
                response.response.vote,
                Vote::<ControlPlaneRaftLeaderId>::new_committed(3, source_node_id)
            );
        });
    }

    fn test_raft_config(cluster_name: &'static str) -> Arc<Config> {
        test_raft_config_with_log_reversion(cluster_name, None)
    }

    fn test_raft_config_with_log_reversion(
        cluster_name: &'static str,
        allow_log_reversion: Option<bool>,
    ) -> Arc<Config> {
        Arc::new(
            Config {
                cluster_name: cluster_name.to_string(),
                heartbeat_interval: 50,
                election_timeout_min: 150,
                election_timeout_max: 300,
                enable_tick: false,
                enable_heartbeat: false,
                enable_elect: false,
                allow_log_reversion,
                ..Default::default()
            }
            .validate()
            .unwrap(),
        )
    }

    async fn wait_for_local_leader(
        raft: &Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
        message: &'static str,
    ) {
        raft.wait(Some(Duration::from_secs(1)))
            .state(ServerState::Leader, message)
            .await
            .unwrap();
        raft.as_leader()
            .expect("local single-node raft should have a committed leader vote");
    }

    async fn initialized_two_node_authorities(
        cluster_name: &'static str,
        node1: ControlPlaneRaftNodeId,
        node2: ControlPlaneRaftNodeId,
    ) -> (ControlPlaneRaftAuthority, ControlPlaneRaftAuthority) {
        let network = InMemoryRaftNetworkFactory::default();
        let config = test_raft_config(cluster_name);
        let raft1 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            node1,
            config.clone(),
            network.clone(),
            ControlPlaneRaftLogStore::empty(),
            ControlPlaneRaftStateMachine::empty(),
        )
        .await
        .unwrap();
        let raft2 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            node2,
            config,
            network.clone(),
            ControlPlaneRaftLogStore::empty(),
            ControlPlaneRaftStateMachine::empty(),
        )
        .await
        .unwrap();
        network.register(node1, raft1.clone());
        network.register(node2, raft2.clone());
        let authority1 = ControlPlaneRaftAuthority::new(raft1);
        let authority2 = ControlPlaneRaftAuthority::new(raft2);

        authority1
            .initialize_membership(BTreeMap::from([
                (node1, BasicNode::new(format!("node-{node1}"))),
                (node2, BasicNode::new(format!("node-{node2}"))),
            ]))
            .await
            .unwrap();
        wait_for_local_leader(authority1.raft(), "two-node initialized leadership").await;

        (authority1, authority2)
    }

    async fn initialized_three_node_cluster_with_two_voters(
        cluster_name: &'static str,
        node1: ControlPlaneRaftNodeId,
        node2: ControlPlaneRaftNodeId,
        node3: ControlPlaneRaftNodeId,
    ) -> (
        ControlPlaneRaftAuthority,
        ControlPlaneRaftAuthority,
        ControlPlaneRaftAuthority,
    ) {
        let network = InMemoryRaftNetworkFactory::default();
        let config = test_raft_config(cluster_name);
        let raft1 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            node1,
            config.clone(),
            network.clone(),
            ControlPlaneRaftLogStore::empty(),
            ControlPlaneRaftStateMachine::empty(),
        )
        .await
        .unwrap();
        let raft2 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            node2,
            config.clone(),
            network.clone(),
            ControlPlaneRaftLogStore::empty(),
            ControlPlaneRaftStateMachine::empty(),
        )
        .await
        .unwrap();
        let raft3 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            node3,
            config,
            network.clone(),
            ControlPlaneRaftLogStore::empty(),
            ControlPlaneRaftStateMachine::empty(),
        )
        .await
        .unwrap();
        network.register(node1, raft1.clone());
        network.register(node2, raft2.clone());
        network.register(node3, raft3.clone());
        let authority1 = ControlPlaneRaftAuthority::new(raft1);
        let authority2 = ControlPlaneRaftAuthority::new(raft2);
        let authority3 = ControlPlaneRaftAuthority::new(raft3);

        authority1
            .initialize_membership(BTreeMap::from([
                (node1, BasicNode::new(format!("node-{node1}"))),
                (node2, BasicNode::new(format!("node-{node2}"))),
            ]))
            .await
            .unwrap();
        wait_for_local_leader(authority1.raft(), "three-node initialized leadership").await;

        (authority1, authority2, authority3)
    }

    struct ThreeVoterAuthorityFixture {
        network: InMemoryRaftNetworkFactory,
        config: Arc<Config>,
        leader_log_store: ControlPlaneRaftLogStore,
        third_log_store: ControlPlaneRaftLogStore,
        authority1: ControlPlaneRaftAuthority,
        authority2: ControlPlaneRaftAuthority,
        authority3: ControlPlaneRaftAuthority,
    }

    async fn initialized_three_node_voter_authorities(
        cluster_name: &'static str,
        node1: ControlPlaneRaftNodeId,
        node2: ControlPlaneRaftNodeId,
        node3: ControlPlaneRaftNodeId,
    ) -> ThreeVoterAuthorityFixture {
        initialized_three_node_voter_authorities_with_config(
            test_raft_config(cluster_name),
            node1,
            node2,
            node3,
        )
        .await
    }

    async fn initialized_three_node_voter_authorities_with_config(
        config: Arc<Config>,
        node1: ControlPlaneRaftNodeId,
        node2: ControlPlaneRaftNodeId,
        node3: ControlPlaneRaftNodeId,
    ) -> ThreeVoterAuthorityFixture {
        let network = InMemoryRaftNetworkFactory::default();
        let leader_log_store = ControlPlaneRaftLogStore::empty();
        let raft1 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            node1,
            config.clone(),
            network.clone(),
            leader_log_store.clone(),
            ControlPlaneRaftStateMachine::empty(),
        )
        .await
        .unwrap();
        let raft2 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            node2,
            config.clone(),
            network.clone(),
            ControlPlaneRaftLogStore::empty(),
            ControlPlaneRaftStateMachine::empty(),
        )
        .await
        .unwrap();
        let log_store3 = ControlPlaneRaftLogStore::empty();
        let raft3 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            node3,
            config.clone(),
            network.clone(),
            log_store3.clone(),
            ControlPlaneRaftStateMachine::empty(),
        )
        .await
        .unwrap();
        network.register(node1, raft1.clone());
        network.register(node2, raft2.clone());
        network.register(node3, raft3.clone());
        let authority1 = ControlPlaneRaftAuthority::new_with_log_store(
            raft1,
            leader_log_store.clone(),
            "test-cluster",
        );
        let authority2 = ControlPlaneRaftAuthority::new(raft2);
        let authority3 = ControlPlaneRaftAuthority::new_with_log_store(
            raft3,
            log_store3.clone(),
            "test-cluster",
        );

        authority1
            .initialize_membership(BTreeMap::from([
                (node1, BasicNode::new(format!("node-{node1}"))),
                (node2, BasicNode::new(format!("node-{node2}"))),
                (node3, BasicNode::new(format!("node-{node3}"))),
            ]))
            .await
            .unwrap();
        wait_for_local_leader(authority1.raft(), "three-voter initialized leadership").await;

        ThreeVoterAuthorityFixture {
            network,
            config,
            leader_log_store,
            third_log_store: log_store3,
            authority1,
            authority2,
            authority3,
        }
    }

    async fn capture_openraft_restart_artifact(
        log_store: &ControlPlaneRaftLogStore,
        authority: &ControlPlaneRaftAuthority,
    ) -> ControlPlaneRaftRestartArtifact {
        let log_store = log_store.export_restart_artifact().unwrap();
        let state_machine = authority
            .raft()
            .with_state_machine(|state_machine| {
                let artifact = state_machine.export_restart_artifact();
                Box::pin(async move { artifact })
            })
            .await
            .unwrap();
        ControlPlaneRaftRestartArtifact {
            cluster_name: authority.cluster_name.clone(),
            log_store,
            state_machine,
        }
    }

    #[test]
    fn control_plane_raft_linearized_authority_readiness_from_flags_is_ordered() {
        assert_eq!(
            linearized_authority_readiness_from_flags(false, false, false),
            ControlPlaneRaftLinearizedAuthorityReadiness::NotLocalLeader
        );
        assert_eq!(
            linearized_authority_readiness_from_flags(false, true, true),
            ControlPlaneRaftLinearizedAuthorityReadiness::NotLocalLeader
        );
        assert_eq!(
            linearized_authority_readiness_from_flags(true, false, true),
            ControlPlaneRaftLinearizedAuthorityReadiness::NotEffectiveVoter
        );
        assert_eq!(
            linearized_authority_readiness_from_flags(true, true, false),
            ControlPlaneRaftLinearizedAuthorityReadiness::NotAppliedToCommitted
        );
        assert_eq!(
            linearized_authority_readiness_from_flags(true, true, true),
            ControlPlaneRaftLinearizedAuthorityReadiness::Serving
        );
    }

    fn test_authority_status(
        node_id: ControlPlaneRaftNodeId,
        linearized_authority_serving: bool,
    ) -> ControlPlaneRaftAuthorityStatus {
        let caught_up_log_id = linearized_authority_serving.then(|| raft_log_id(1, node_id, 1));
        ControlPlaneRaftAuthorityStatus {
            node_id,
            current_leader: linearized_authority_serving.then_some(node_id),
            server_state: if linearized_authority_serving {
                ServerState::Leader
            } else {
                ServerState::Follower
            },
            local_leader: linearized_authority_serving,
            effective_voter: linearized_authority_serving,
            effective_learner: false,
            applied_voter: linearized_authority_serving,
            applied_learner: false,
            persisted_vote: None,
            current_term: None,
            last_log_id: None,
            last_purged_log_id: None,
            committed: caught_up_log_id,
            applied: caught_up_log_id,
            current_snapshot: None,
            authority_incarnation: AuthorityIncarnation::INITIAL,
            current_cluster_epoch: ClusterEpoch::INITIAL,
            retained_history_count: 0,
            oldest_retained_history_epoch: None,
            newest_retained_history_epoch: None,
            oldest_storage_history_floor_epoch: None,
            storage_node_lease_deadline_count: 0,
            earliest_storage_node_lease_deadline_ms: None,
            latest_storage_node_lease_deadline_ms: None,
            storage_node_count: 0,
            joining_storage_node_count: 0,
            active_storage_node_count: 0,
            draining_storage_node_count: 0,
            out_storage_node_count: 0,
            removed_storage_node_count: 0,
            healthy_storage_node_count: 0,
            suspect_storage_node_count: 0,
            unavailable_storage_node_count: 0,
            pg_count: 0,
            active_pg_count: 0,
            peering_pg_count: 0,
            degraded_pg_count: 0,
            backfilling_pg_count: 0,
            inconsistent_pg_count: 0,
            active_primary_pg_count: 0,
            peering_metadata_transfer_pg_count: 0,
            metadata_transfer_fenced_pg_count: 0,
            metadata_transfer_fence_source_lease_deadline_count: 0,
            earliest_metadata_transfer_fence_source_lease_deadline_ms: None,
            latest_metadata_transfer_fence_source_lease_deadline_ms: None,
            effective_membership_log_id: None,
            effective_voters: linearized_authority_serving
                .then_some(node_id)
                .into_iter()
                .collect(),
            effective_learners: BTreeSet::new(),
            applied_membership_log_id: None,
            applied_voters: linearized_authority_serving
                .then_some(node_id)
                .into_iter()
                .collect(),
            applied_learners: BTreeSet::new(),
        }
    }

    #[test]
    fn control_plane_raft_current_serving_authority_node_id_fails_closed() {
        let statuses = BTreeMap::from([
            (431, test_authority_status(431, false)),
            (432, test_authority_status(432, true)),
        ]);
        assert_eq!(current_serving_authority_node_id(&statuses).unwrap(), 432);

        let no_serving = BTreeMap::from([
            (431, test_authority_status(431, false)),
            (432, test_authority_status(432, false)),
        ]);
        assert!(matches!(
            current_serving_authority_node_id(&no_serving),
            Err(ControlPlaneError::RpcRemote { message })
                if message.contains("no serving raft authority")
        ));

        let key_mismatch = BTreeMap::from([(431, test_authority_status(432, false))]);
        assert!(matches!(
            current_serving_authority_node_id(&key_mismatch),
            Err(ControlPlaneError::RpcRemote { message })
                if message.contains("status key 431 disagrees with reported node 432")
        ));

        let mut lagging_leader = test_authority_status(433, true);
        lagging_leader.committed = Some(raft_log_id(1, 433, 2));
        lagging_leader.applied = Some(raft_log_id(1, 433, 1));
        assert_eq!(
            lagging_leader.linearized_authority_readiness(),
            ControlPlaneRaftLinearizedAuthorityReadiness::NotAppliedToCommitted
        );
        assert!(!lagging_leader.linearized_authority_serving());
        let lagging = BTreeMap::from([(433, lagging_leader)]);
        assert!(matches!(
            current_serving_authority_node_id(&lagging),
            Err(ControlPlaneError::RpcRemote { message })
                if message.contains("no serving raft authority")
        ));

        let mut same_index_different_term = test_authority_status(434, true);
        same_index_different_term.committed = Some(raft_log_id(2, 434, 3));
        same_index_different_term.applied = Some(raft_log_id(1, 434, 3));
        assert_eq!(
            same_index_different_term.linearized_authority_readiness(),
            ControlPlaneRaftLinearizedAuthorityReadiness::NotAppliedToCommitted
        );
        assert!(!same_index_different_term.linearized_authority_serving());
        let same_index_mismatch = BTreeMap::from([(434, same_index_different_term)]);
        assert!(matches!(
            current_serving_authority_node_id(&same_index_mismatch),
            Err(ControlPlaneError::RpcRemote { message })
                if message.contains("no serving raft authority")
        ));
    }

    async fn wait_for_log_purged_to(
        log_store: &ControlPlaneRaftLogStore,
        log_id: LogIdOf<ControlPlaneRaftTypeConfig>,
        message: &'static str,
    ) {
        for _ in 0..100 {
            if log_store.last_purged_log_id().unwrap() == Some(log_id) {
                return;
            }
            ControlPlaneRaftTypeConfig::sleep(Duration::from_millis(10)).await;
        }
        panic!("{message}: log store did not purge through {log_id}");
    }

    async fn expect_bounded_control_plane_raft<T, Fut>(
        future: Fut,
        timeout: Duration,
        message: &'static str,
    ) -> T
    where
        Fut: Future<Output = Result<T, ControlPlaneError>> + OptionalSend,
    {
        match ControlPlaneRaftTypeConfig::timeout(timeout, future).await {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => panic!("{message}: {error:?}"),
            Err(_) => panic!("{message}: timed out after {timeout:?}"),
        }
    }

    async fn expect_bounded_control_plane_raft_error<T, Fut>(
        future: Fut,
        timeout: Duration,
        message: &'static str,
    ) -> ControlPlaneError
    where
        Fut: Future<Output = Result<T, ControlPlaneError>> + OptionalSend,
    {
        match ControlPlaneRaftTypeConfig::timeout(timeout, future).await {
            Ok(Ok(_)) => panic!("{message}: unexpectedly succeeded"),
            Ok(Err(error)) => error,
            Err(_) => panic!("{message}: timed out after {timeout:?}"),
        }
    }

    fn assert_error_contains<T>(result: Result<T, ControlPlaneError>, expected: &str) {
        match result {
            Ok(_) => panic!("expected error containing {expected:?}, got success"),
            Err(error) => assert!(
                error.to_string().contains(expected),
                "expected error {error:?} to contain {expected:?}"
            ),
        }
    }

    async fn wait_for_authority_status_matching(
        authority: &ControlPlaneRaftAuthority,
        timeout: Duration,
        message: &'static str,
        predicate: impl Fn(&ControlPlaneRaftAuthorityStatus) -> bool + Sync,
    ) -> ControlPlaneRaftAuthorityStatus {
        match ControlPlaneRaftTypeConfig::timeout(timeout, async {
            loop {
                let status = authority
                    .status()
                    .await
                    .unwrap_or_else(|error| panic!("{message}: failed to read status: {error:?}"));
                if predicate(&status) {
                    return status;
                }
                ControlPlaneRaftTypeConfig::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        {
            Ok(status) => status,
            Err(_) => panic!("{message}: timed out after {timeout:?}"),
        }
    }

    fn raft_log_id(term: u64, node_id: u64, index: u64) -> LogIdOf<ControlPlaneRaftTypeConfig> {
        LogId::new(LeaderId { term, node_id }, index)
    }

    fn blank_entry(term: u64, node_id: u64, index: u64) -> ControlPlaneRaftEntry {
        Entry {
            log_id: raft_log_id(term, node_id, index),
            payload: EntryPayload::Blank,
        }
    }

    fn membership_entry(term: u64, node_id: u64, index: u64) -> ControlPlaneRaftEntry {
        Entry {
            log_id: raft_log_id(term, node_id, index),
            payload: EntryPayload::Membership(Membership::new_with_defaults(
                vec![BTreeSet::from([1, 2])],
                [],
            )),
        }
    }

    fn single_node_membership_entry(term: u64, node_id: u64, index: u64) -> ControlPlaneRaftEntry {
        Entry {
            log_id: raft_log_id(term, node_id, index),
            payload: EntryPayload::Membership(Membership::new_with_defaults(
                vec![BTreeSet::from([node_id])],
                [],
            )),
        }
    }

    fn bootstrap_membership_entry(node_id: u64) -> ControlPlaneRaftEntry {
        membership_entry(0, node_id, 0)
    }

    fn single_node_bootstrap_membership_entry(node_id: u64) -> ControlPlaneRaftEntry {
        single_node_membership_entry(0, node_id, 0)
    }

    fn normal_entry(
        term: u64,
        node_id: u64,
        index: u64,
        command: ControlPlaneCommand,
    ) -> ControlPlaneRaftEntry {
        Entry {
            log_id: raft_log_id(term, node_id, index),
            payload: EntryPayload::Normal(command),
        }
    }

    fn test_membership() -> Membership<ControlPlaneRaftNodeId, BasicNode> {
        Membership::new_with_defaults(vec![BTreeSet::from([1, 2])], [])
    }

    fn replicated_state_machine_with_noops(
        term: u64,
        through_index: u64,
    ) -> ReplicatedControlPlaneStateMachine {
        let mut state_machine = ReplicatedControlPlaneStateMachine::empty();
        for index in 1..=through_index {
            state_machine
                .apply_committed_noop(ControlPlaneLogId::new(term, index).unwrap())
                .unwrap();
        }
        state_machine
    }

    fn state_machine_restart_artifact_with_noops(
        term: u64,
        node_id: u64,
        through_index: u64,
    ) -> ControlPlaneRaftStateMachineRestartArtifact {
        ControlPlaneRaftStateMachineRestartArtifact {
            inner: replicated_state_machine_with_noops(term, through_index),
            last_applied: Some(raft_log_id(term, node_id, through_index)),
            last_membership: StoredMembership::default(),
            current_snapshot: None,
        }
    }

    #[test]
    fn control_plane_raft_log_id_round_trips_term_and_index() {
        let control_plane_log_id = ControlPlaneLogId::new(7, 42).unwrap();
        let raft_log_id = raft_log_id_from_control_plane(3, control_plane_log_id);

        assert_eq!(raft_log_id.committed_leader_id().term, 7);
        assert_eq!(raft_log_id.committed_leader_id().node_id, 3);
        assert_eq!(raft_log_id.index(), 42);
        assert_eq!(
            control_plane_log_id_from_raft(raft_log_id),
            Some(control_plane_log_id)
        );
    }

    #[test]
    fn control_plane_raft_log_id_rejects_reserved_values() {
        let zero_term = LogId::new(
            LeaderId {
                term: 0,
                node_id: 1,
            },
            1,
        );
        let zero_index = LogId::new(
            LeaderId {
                term: 1,
                node_id: 1,
            },
            0,
        );

        assert_eq!(control_plane_log_id_from_raft(zero_term), None);
        assert_eq!(control_plane_log_id_from_raft(zero_index), None);
    }

    #[test]
    fn control_plane_raft_node_id_conversion_is_bounded_by_storage_node_id() {
        let storage_id = NodeId::new(17);

        assert_eq!(raft_node_id_from_storage_node_id(storage_id), 17);
        assert_eq!(storage_node_id_from_raft_node_id(17), Some(storage_id));
        assert_eq!(
            storage_node_id_from_raft_node_id(u64::from(u32::MAX) + 1),
            None
        );
    }

    #[test]
    fn control_plane_raft_state_machine_applies_blank_and_membership_entries() {
        let mut state_machine = ControlPlaneRaftStateMachine::empty();

        assert!(matches!(
            state_machine.apply_entry(blank_entry(1, 1, 1)).unwrap(),
            ControlPlaneRaftApplyResponse::Blank
        ));
        assert_eq!(state_machine.last_applied(), Some(raft_log_id(1, 1, 1)));

        assert!(matches!(
            state_machine
                .apply_entry(membership_entry(1, 1, 2))
                .unwrap(),
            ControlPlaneRaftApplyResponse::Membership
        ));
        assert_eq!(state_machine.last_applied(), Some(raft_log_id(1, 1, 2)));
        assert_eq!(
            state_machine.last_membership().log_id(),
            &Some(raft_log_id(1, 1, 2))
        );
        assert_eq!(
            state_machine
                .inner()
                .last_applied()
                .map(|log_id| (log_id.term(), log_id.index())),
            Some((1, 2))
        );
    }

    #[test]
    fn control_plane_raft_state_machine_applies_openraft_bootstrap_membership() {
        let mut empty_state_machine = ControlPlaneRaftStateMachine::empty();
        let empty_snapshot = empty_state_machine.build_snapshot().unwrap();
        assert_eq!(empty_snapshot.meta.last_log_id, None);
        assert_eq!(empty_snapshot.meta.snapshot_id, "control-plane-empty");

        let mut state_machine = ControlPlaneRaftStateMachine::empty();

        assert!(matches!(
            state_machine
                .apply_entry(bootstrap_membership_entry(7))
                .unwrap(),
            ControlPlaneRaftApplyResponse::Membership
        ));
        assert_eq!(state_machine.last_applied(), Some(raft_log_id(0, 7, 0)));
        assert_eq!(
            state_machine.last_membership().log_id(),
            &Some(raft_log_id(0, 7, 0))
        );
        assert_eq!(state_machine.inner().last_applied(), None);

        let snapshot = state_machine.build_snapshot().unwrap();
        assert_eq!(snapshot.meta.last_log_id, Some(raft_log_id(0, 7, 0)));
        assert_eq!(snapshot.meta.snapshot_id, "control-plane-T0-N7-I0");
        assert_ne!(snapshot.meta.snapshot_id, empty_snapshot.meta.snapshot_id);
        assert_eq!(
            snapshot.meta.last_membership.log_id(),
            &Some(raft_log_id(0, 7, 0))
        );

        let mut target = ControlPlaneRaftStateMachine::empty();
        target
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .unwrap();
        assert_eq!(target.last_applied(), Some(raft_log_id(0, 7, 0)));
        assert_eq!(target.inner().last_applied(), None);
        assert_eq!(
            target.last_membership().log_id(),
            &Some(raft_log_id(0, 7, 0))
        );
    }

    #[test]
    fn control_plane_raft_state_machine_new_validates_restart_state() {
        let empty = ControlPlaneRaftStateMachine::new(
            ReplicatedControlPlaneStateMachine::empty(),
            None,
            StoredMembership::default(),
        )
        .unwrap();
        assert_eq!(empty.last_applied(), None);

        let bootstrap = ControlPlaneRaftStateMachine::new(
            ReplicatedControlPlaneStateMachine::empty(),
            Some(raft_log_id(0, 7, 0)),
            StoredMembership::new(Some(raft_log_id(0, 7, 0)), test_membership()),
        )
        .unwrap();
        assert_eq!(bootstrap.last_applied(), Some(raft_log_id(0, 7, 0)));
        assert_eq!(bootstrap.inner().last_applied(), None);

        let applied = ControlPlaneRaftStateMachine::new(
            replicated_state_machine_with_noops(2, 3),
            Some(raft_log_id(2, 7, 3)),
            StoredMembership::new(Some(raft_log_id(2, 7, 2)), test_membership()),
        )
        .unwrap();
        assert_eq!(applied.last_applied(), Some(raft_log_id(2, 7, 3)));
        assert_eq!(
            applied
                .inner()
                .last_applied()
                .map(|log_id| (log_id.term(), log_id.index())),
            Some((2, 3))
        );
    }

    #[test]
    fn control_plane_raft_state_machine_new_rejects_inconsistent_restart_state() {
        let err = ControlPlaneRaftStateMachine::new(
            replicated_state_machine_with_noops(2, 3),
            None,
            StoredMembership::default(),
        )
        .unwrap_err();
        assert!(matches!(err, ControlPlaneError::SnapshotDecode { .. }));

        let err = ControlPlaneRaftStateMachine::new(
            ReplicatedControlPlaneStateMachine::empty(),
            Some(raft_log_id(2, 7, 3)),
            StoredMembership::default(),
        )
        .unwrap_err();
        assert!(matches!(err, ControlPlaneError::SnapshotDecode { .. }));

        let err = ControlPlaneRaftStateMachine::new(
            replicated_state_machine_with_noops(2, 3),
            Some(raft_log_id(3, 7, 3)),
            StoredMembership::default(),
        )
        .unwrap_err();
        assert!(matches!(err, ControlPlaneError::SnapshotDecode { .. }));

        let err = ControlPlaneRaftStateMachine::new(
            replicated_state_machine_with_noops(2, 3),
            Some(raft_log_id(2, 7, 3)),
            StoredMembership::new(Some(raft_log_id(2, 7, 4)), test_membership()),
        )
        .unwrap_err();
        assert!(matches!(err, ControlPlaneError::SnapshotDecode { .. }));

        let mut state_machine = ControlPlaneRaftStateMachine::new(
            replicated_state_machine_with_noops(3, 1),
            Some(raft_log_id(3, 7, 1)),
            StoredMembership::default(),
        )
        .unwrap();
        state_machine.build_snapshot().unwrap();
        let mut artifact = state_machine.export_restart_artifact();
        artifact
            .current_snapshot
            .as_mut()
            .expect("tip snapshot should be exported")
            .meta
            .snapshot_id = "control-plane-wrong".to_string();
        let err = ControlPlaneRaftStateMachine::from_restart_artifact(artifact).unwrap_err();
        assert!(matches!(
            err,
            ControlPlaneError::CommandDecode { message }
                if message.contains("cached OpenRaft snapshot id")
        ));
    }

    #[test]
    fn control_plane_raft_state_machine_restores_restart_artifact() {
        let mut source = ControlPlaneRaftStateMachine::empty();
        source
            .apply_entry(normal_entry(
                2,
                7,
                1,
                ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![(NodeId::new(1), "node-1".to_string())],
                    pg_ids: vec![PgId::new(0)],
                },
            ))
            .unwrap();
        source.apply_entry(membership_entry(2, 7, 2)).unwrap();
        assert!(matches!(
            source
                .apply_entry(normal_entry(
                    2,
                    7,
                    3,
                    ControlPlaneCommand::MarkNodeAvailability {
                        node_id: NodeId::new(99),
                        availability: NodeAvailabilityState::Healthy,
                    },
                ))
                .unwrap(),
            ControlPlaneRaftApplyResponse::Rejected(ControlPlaneError::UnknownNode { node_id: 99 })
        ));

        let artifact = source.export_restart_artifact();
        let mut restored = ControlPlaneRaftStateMachine::from_restart_artifact(artifact).unwrap();

        assert_eq!(restored.last_applied(), Some(raft_log_id(2, 7, 3)));
        assert_eq!(
            restored.last_membership().log_id(),
            &Some(raft_log_id(2, 7, 2))
        );
        assert_eq!(restored.inner().snapshot(), source.inner().snapshot());
        assert!(restored.current_snapshot().is_none());

        restored.apply_entry(blank_entry(2, 7, 4)).unwrap();
        let runtime_map = restored
            .runtime_map_for_applied_read_index(raft_log_id(2, 7, 4), 12_345)
            .unwrap();
        assert_eq!(
            runtime_map.freshness_proof().read_index(),
            Some(ControlPlaneLogId::new(2, 4).unwrap())
        );
    }

    #[test]
    fn control_plane_raft_state_machine_rejects_invalid_restart_artifacts() {
        let inner_with_applied = replicated_state_machine_with_noops(2, 3);
        let err = ControlPlaneRaftStateMachine::from_restart_artifact(
            ControlPlaneRaftStateMachineRestartArtifact {
                inner: inner_with_applied.clone(),
                last_applied: None,
                last_membership: StoredMembership::default(),
                current_snapshot: None,
            },
        )
        .unwrap_err();
        assert!(matches!(err, ControlPlaneError::SnapshotDecode { .. }));

        let err = ControlPlaneRaftStateMachine::from_restart_artifact(
            ControlPlaneRaftStateMachineRestartArtifact {
                inner: ReplicatedControlPlaneStateMachine::empty(),
                last_applied: Some(raft_log_id(2, 7, 3)),
                last_membership: StoredMembership::default(),
                current_snapshot: None,
            },
        )
        .unwrap_err();
        assert!(matches!(err, ControlPlaneError::SnapshotDecode { .. }));

        let err = ControlPlaneRaftStateMachine::from_restart_artifact(
            ControlPlaneRaftStateMachineRestartArtifact {
                inner: inner_with_applied,
                last_applied: Some(raft_log_id(2, 7, 3)),
                last_membership: StoredMembership::new(
                    Some(raft_log_id(2, 7, 4)),
                    test_membership(),
                ),
                current_snapshot: None,
            },
        )
        .unwrap_err();
        assert!(matches!(err, ControlPlaneError::SnapshotDecode { .. }));
    }

    #[test]
    fn control_plane_raft_state_machine_rejects_nonzero_term_index_zero_membership() {
        let mut state_machine = ControlPlaneRaftStateMachine::empty();

        let err = state_machine
            .apply_entry(membership_entry(1, 7, 0))
            .unwrap_err();

        assert!(matches!(err, ControlPlaneError::CommandDecode { .. }));
        assert_eq!(state_machine.last_applied(), None);
        assert_eq!(state_machine.last_membership().log_id(), &None);
        assert_eq!(state_machine.inner().last_applied(), None);
    }

    #[test]
    fn control_plane_raft_state_machine_rejects_out_of_order_apply() {
        let mut state_machine = ControlPlaneRaftStateMachine::empty();

        let err = state_machine.apply_entry(blank_entry(3, 1, 2)).unwrap_err();
        assert!(matches!(
            err,
            ControlPlaneError::ControlPlaneLogIndexMismatch {
                expected_index: 1,
                actual_index: 2,
            }
        ));
        assert_eq!(state_machine.last_applied(), None);

        state_machine
            .apply_entry(bootstrap_membership_entry(1))
            .unwrap();
        let err = state_machine
            .apply_entry(bootstrap_membership_entry(1))
            .unwrap_err();
        assert!(matches!(
            err,
            ControlPlaneError::ControlPlaneLogIndexMismatch {
                expected_index: 1,
                actual_index: 0,
            }
        ));
        assert_eq!(state_machine.last_applied(), Some(raft_log_id(0, 1, 0)));

        state_machine.apply_entry(blank_entry(3, 2, 1)).unwrap();
        let err = state_machine
            .apply_entry(blank_entry(2, 99, 2))
            .unwrap_err();
        assert!(matches!(
            err,
            ControlPlaneError::ControlPlaneLogTermRegression {
                previous_term: 3,
                actual_term: 2,
                index: 2,
            }
        ));
        assert_eq!(state_machine.last_applied(), Some(raft_log_id(3, 2, 1)));

        let err = state_machine.apply_entry(blank_entry(3, 1, 2)).unwrap_err();
        assert!(matches!(err, ControlPlaneError::CommandDecode { .. }));
        assert_eq!(state_machine.last_applied(), Some(raft_log_id(3, 2, 1)));
    }

    #[test]
    fn control_plane_raft_state_machine_rejects_apply_after_max_index() {
        let inner = ReplicatedControlPlaneStateMachine::new(
            ClusterControlSnapshot::empty(),
            Some(ControlPlaneLogId::new(1, u64::MAX).unwrap()),
        );
        let mut state_machine = ControlPlaneRaftStateMachine::new(
            inner,
            Some(raft_log_id(1, 7, u64::MAX)),
            StoredMembership::default(),
        )
        .unwrap();

        let err = state_machine
            .apply_entry(blank_entry(1, 7, u64::MAX))
            .unwrap_err();

        assert!(matches!(
            err,
            ControlPlaneError::ControlPlaneLogIndexOverflow { index: u64::MAX }
        ));
        assert_eq!(
            state_machine.last_applied(),
            Some(raft_log_id(1, 7, u64::MAX))
        );
    }

    #[test]
    fn control_plane_raft_state_machine_builds_and_installs_snapshot() {
        let mut source = ControlPlaneRaftStateMachine::empty();
        source.apply_entry(blank_entry(2, 7, 1)).unwrap();
        let snapshot = source.build_snapshot().unwrap();

        assert_eq!(snapshot.meta.last_log_id, Some(raft_log_id(2, 7, 1)));
        assert_eq!(snapshot.meta.snapshot_id, "control-plane-T2-N7-I1");

        let mut target = ControlPlaneRaftStateMachine::empty();
        target
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .unwrap();

        assert_eq!(target.last_applied(), Some(raft_log_id(2, 7, 1)));
        assert_eq!(
            target
                .inner()
                .last_applied()
                .map(|log_id| (log_id.term(), log_id.index())),
            Some((2, 1))
        );
    }

    #[test]
    fn control_plane_raft_snapshot_builder_returns_stable_snapshot_view() {
        let mut state_machine = ControlPlaneRaftStateMachine::empty();
        state_machine.apply_entry(blank_entry(2, 7, 1)).unwrap();

        let mut builder = state_machine.create_snapshot_builder().unwrap();
        state_machine.apply_entry(blank_entry(2, 7, 2)).unwrap();

        let snapshot = ControlPlaneRaftTypeConfig::run(builder.build_snapshot()).unwrap();

        assert_eq!(snapshot.meta.last_log_id, Some(raft_log_id(2, 7, 1)));
        assert_eq!(snapshot.meta.snapshot_id, "control-plane-T2-N7-I1");
        assert_eq!(state_machine.last_applied(), Some(raft_log_id(2, 7, 2)));
        assert_eq!(
            state_machine
                .current_snapshot()
                .map(|snapshot| snapshot.meta.last_log_id),
            Some(Some(raft_log_id(2, 7, 1)))
        );
    }

    #[test]
    fn control_plane_raft_snapshot_install_rejects_same_position_different_leader() {
        let mut source = ControlPlaneRaftStateMachine::empty();
        source.apply_entry(blank_entry(2, 7, 1)).unwrap();
        let snapshot = source.build_snapshot().unwrap();
        let snapshot_meta = snapshot.meta.clone();
        let snapshot_payload = snapshot.snapshot.clone();

        let mut target = ControlPlaneRaftStateMachine::empty();
        target
            .install_snapshot(&snapshot_meta, snapshot_payload.clone())
            .unwrap();

        let mut bad_meta = snapshot_meta;
        bad_meta.last_log_id = Some(raft_log_id(2, 8, 1));
        let err = target
            .install_snapshot(&bad_meta, snapshot_payload)
            .unwrap_err();

        assert!(matches!(err, ControlPlaneError::SnapshotDecode { .. }));
        assert_eq!(target.last_applied(), Some(raft_log_id(2, 7, 1)));
    }

    #[test]
    fn control_plane_raft_snapshot_install_rejects_mismatched_snapshot_id() {
        let mut source = ControlPlaneRaftStateMachine::empty();
        source.apply_entry(blank_entry(2, 7, 1)).unwrap();
        let snapshot = source.build_snapshot().unwrap();
        let mut bad_meta = snapshot.meta.clone();
        bad_meta.snapshot_id = "control-plane-1".to_string();

        let mut target = ControlPlaneRaftStateMachine::empty();
        let err = target
            .install_snapshot(&bad_meta, snapshot.snapshot)
            .unwrap_err();

        assert!(matches!(err, ControlPlaneError::SnapshotDecode { .. }));
        assert_eq!(target.last_applied(), None);
        assert!(target.current_snapshot().is_none());
    }

    #[test]
    fn control_plane_raft_snapshot_install_rejects_unapplied_membership_log_id() {
        let mut source = ControlPlaneRaftStateMachine::empty();
        source.apply_entry(blank_entry(2, 7, 1)).unwrap();
        let snapshot = source.build_snapshot().unwrap();
        let mut bad_meta = snapshot.meta.clone();
        bad_meta.last_membership =
            StoredMembership::new(Some(raft_log_id(2, 7, 2)), test_membership());

        let mut target = ControlPlaneRaftStateMachine::empty();
        let err = target
            .install_snapshot(&bad_meta, snapshot.snapshot)
            .unwrap_err();

        assert!(matches!(err, ControlPlaneError::SnapshotDecode { .. }));
        assert_eq!(target.last_applied(), None);
    }

    #[test]
    fn control_plane_raft_snapshot_install_rejects_invalid_membership_log_id() {
        let mut source = ControlPlaneRaftStateMachine::empty();
        source.apply_entry(blank_entry(2, 7, 1)).unwrap();
        let snapshot = source.build_snapshot().unwrap();
        let mut bad_meta = snapshot.meta.clone();
        bad_meta.last_membership =
            StoredMembership::new(Some(raft_log_id(0, 7, 1)), test_membership());

        let mut target = ControlPlaneRaftStateMachine::empty();
        let err = target
            .install_snapshot(&bad_meta, snapshot.snapshot)
            .unwrap_err();

        assert!(matches!(err, ControlPlaneError::SnapshotDecode { .. }));
        assert_eq!(target.last_applied(), None);
    }

    #[test]
    fn control_plane_raft_snapshot_install_rejects_nonzero_term_index_zero_log_id() {
        let mut source = ControlPlaneRaftStateMachine::empty();
        source.apply_entry(bootstrap_membership_entry(7)).unwrap();
        let snapshot = source.build_snapshot().unwrap();

        let mut bad_meta = snapshot.meta.clone();
        bad_meta.last_log_id = Some(raft_log_id(1, 7, 0));
        bad_meta.last_membership =
            StoredMembership::new(Some(raft_log_id(1, 7, 0)), test_membership());

        let mut target = ControlPlaneRaftStateMachine::empty();
        let err = target
            .install_snapshot(&bad_meta, snapshot.snapshot)
            .unwrap_err();

        assert!(matches!(err, ControlPlaneError::SnapshotDecode { .. }));
        assert_eq!(target.last_applied(), None);
        assert_eq!(target.last_membership().log_id(), &None);
        assert_eq!(target.inner().last_applied(), None);
    }

    #[test]
    fn control_plane_raft_state_machine_maps_normal_outcomes_to_application_responses() {
        let mut state_machine = ControlPlaneRaftStateMachine::empty();

        let applied = state_machine
            .apply_entry(normal_entry(
                1,
                1,
                1,
                ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![(NodeId::new(1), "node-1".to_string())],
                    pg_ids: vec![PgId::new(0)],
                },
            ))
            .unwrap();
        assert!(matches!(
            applied,
            ControlPlaneRaftApplyResponse::Applied(
                ControlPlaneCommandResponse::BootstrapInitialClusterMap
            )
        ));

        let rejected = state_machine
            .apply_entry(normal_entry(
                1,
                1,
                2,
                ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(99),
                    availability: NodeAvailabilityState::Healthy,
                },
            ))
            .unwrap();
        assert!(matches!(
            rejected,
            ControlPlaneRaftApplyResponse::Rejected(ControlPlaneError::UnknownNode { node_id: 99 })
        ));
        assert_eq!(state_machine.last_applied(), Some(raft_log_id(1, 1, 2)));
        assert_eq!(
            state_machine
                .inner()
                .last_applied()
                .map(|log_id| (log_id.term(), log_id.index())),
            Some((1, 2))
        );
    }

    #[test]
    fn control_plane_raft_state_machine_runtime_map_read_index_uses_applied_log_id() {
        let mut state_machine = ControlPlaneRaftStateMachine::empty();
        state_machine
            .apply_entry(normal_entry(
                2,
                7,
                1,
                ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![(NodeId::new(1), "node-1".to_string())],
                    pg_ids: vec![PgId::new(0)],
                },
            ))
            .unwrap();

        let runtime_map = state_machine
            .runtime_map_for_applied_read_index(raft_log_id(2, 7, 1), 12_345)
            .unwrap();

        assert_eq!(
            runtime_map.freshness_proof(),
            &RuntimeMapFreshnessProof::ReadIndex {
                authority_incarnation: runtime_map.freshness_proof().authority_incarnation(),
                read_index: ControlPlaneLogId::new(2, 1).unwrap(),
                issued_at_ms: 12_345,
            }
        );
        assert_eq!(
            runtime_map.freshness_proof().read_index(),
            Some(ControlPlaneLogId::new(2, 1).unwrap())
        );
        assert!(runtime_map.freshness_proof().is_serving_authority_read());
    }

    #[test]
    fn control_plane_raft_state_machine_runtime_map_current_read_index_uses_tip() {
        let mut state_machine = ControlPlaneRaftStateMachine::empty();
        state_machine
            .apply_entry(normal_entry(
                2,
                7,
                1,
                ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![(NodeId::new(1), "node-1".to_string())],
                    pg_ids: vec![PgId::new(0)],
                },
            ))
            .unwrap();
        state_machine.apply_entry(blank_entry(2, 7, 2)).unwrap();

        let stale_captured_read_index = state_machine
            .runtime_map_for_applied_read_index(raft_log_id(2, 7, 1), 12_345)
            .unwrap_err();
        assert!(matches!(
            stale_captured_read_index,
            ControlPlaneError::ControlPlaneReadIndexNotApplied { .. }
        ));

        let runtime_map = state_machine
            .runtime_map_for_current_applied_read_index(12_346)
            .unwrap();
        assert_eq!(
            runtime_map.freshness_proof(),
            &RuntimeMapFreshnessProof::ReadIndex {
                authority_incarnation: runtime_map.freshness_proof().authority_incarnation(),
                read_index: ControlPlaneLogId::new(2, 2).unwrap(),
                issued_at_ms: 12_346,
            }
        );
    }

    #[test]
    fn control_plane_raft_state_machine_runtime_map_rejects_unapplied_read_index() {
        let mut state_machine = ControlPlaneRaftStateMachine::empty();
        state_machine.apply_entry(blank_entry(3, 7, 1)).unwrap();

        let future_index = state_machine
            .runtime_map_for_applied_read_index(raft_log_id(3, 7, 2), 12_345)
            .unwrap_err();
        assert!(matches!(
            future_index,
            ControlPlaneError::ControlPlaneReadIndexNotApplied { .. }
        ));

        let lower_term_higher_index = state_machine
            .runtime_map_for_applied_read_index(raft_log_id(2, 7, 2), 12_345)
            .unwrap_err();
        assert!(matches!(
            lower_term_higher_index,
            ControlPlaneError::ControlPlaneReadIndexNotApplied { .. }
        ));

        let same_position_different_leader = state_machine
            .runtime_map_for_applied_read_index(raft_log_id(3, 8, 1), 12_345)
            .unwrap_err();
        assert!(matches!(
            same_position_different_leader,
            ControlPlaneError::CommandDecode { .. }
        ));

        let invalid_bootstrap_read_index = state_machine
            .runtime_map_for_applied_read_index(raft_log_id(0, 7, 0), 12_345)
            .unwrap_err();
        assert!(matches!(
            invalid_bootstrap_read_index,
            ControlPlaneError::CommandDecode { .. }
        ));
    }

    #[test]
    fn control_plane_raft_state_machine_trait_apply_drains_entry_responder_stream() {
        let mut state_machine = ControlPlaneRaftStateMachine::empty();
        let entries = stream::iter(vec![
            Ok((blank_entry(1, 1, 1), None)),
            Ok((
                normal_entry(
                    1,
                    1,
                    2,
                    ControlPlaneCommand::BootstrapInitialClusterMap {
                        nodes: vec![(NodeId::new(1), "node-1".to_string())],
                        pg_ids: vec![PgId::new(0)],
                    },
                ),
                None,
            )),
            Ok((
                normal_entry(
                    1,
                    1,
                    3,
                    ControlPlaneCommand::MarkNodeAvailability {
                        node_id: NodeId::new(99),
                        availability: NodeAvailabilityState::Healthy,
                    },
                ),
                None,
            )),
        ]);

        ControlPlaneRaftTypeConfig::run(RaftStateMachine::apply(&mut state_machine, entries))
            .unwrap();

        assert_eq!(state_machine.last_applied(), Some(raft_log_id(1, 1, 3)));
        assert_eq!(
            state_machine
                .inner()
                .last_applied()
                .map(|log_id| (log_id.term(), log_id.index())),
            Some((1, 3))
        );
    }

    #[test]
    fn control_plane_raft_log_store_tracks_vote_committed_and_visible_entries() {
        ControlPlaneRaftTypeConfig::run(async {
            let mut store = ControlPlaneRaftLogStore::empty();
            let vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1);

            RaftLogStorage::save_vote(&mut store, &vote).await.unwrap();
            assert_eq!(
                RaftLogReader::read_vote(&mut store).await.unwrap(),
                Some(vote)
            );

            RaftLogStorage::append(
                &mut store,
                vec![
                    bootstrap_membership_entry(1),
                    blank_entry(3, 1, 1),
                    blank_entry(3, 1, 2),
                ],
                IOFlushed::noop(),
            )
            .await
            .unwrap();

            let mut reader = RaftLogStorage::get_log_reader(&mut store).await;
            RaftLogStorage::append(&mut store, vec![blank_entry(3, 1, 3)], IOFlushed::noop())
                .await
                .unwrap();

            let entries = RaftLogReader::try_get_log_entries(&mut reader, 0..4)
                .await
                .unwrap();
            assert_eq!(
                entries.iter().map(|entry| entry.log_id).collect::<Vec<_>>(),
                vec![
                    raft_log_id(0, 1, 0),
                    raft_log_id(3, 1, 1),
                    raft_log_id(3, 1, 2),
                    raft_log_id(3, 1, 3),
                ]
            );

            RaftLogStorage::save_committed(&mut store, Some(raft_log_id(3, 1, 2)))
                .await
                .unwrap();
            assert_eq!(
                RaftLogStorage::read_committed(&mut store).await.unwrap(),
                Some(raft_log_id(3, 1, 2))
            );

            let log_state = RaftLogStorage::get_log_state(&mut store).await.unwrap();
            assert_eq!(log_state.last_purged_log_id, None);
            assert_eq!(log_state.last_log_id, Some(raft_log_id(3, 1, 3)));
        });
    }

    #[test]
    fn control_plane_raft_log_store_rejects_vote_regression() {
        ControlPlaneRaftTypeConfig::run(async {
            let mut store = ControlPlaneRaftLogStore::empty();
            let vote = Vote::<ControlPlaneRaftLeaderId>::new(3, 2);

            RaftLogStorage::save_vote(&mut store, &vote).await.unwrap();

            let lower_term = Vote::<ControlPlaneRaftLeaderId>::new(2, 99);
            let err = RaftLogStorage::save_vote(&mut store, &lower_term)
                .await
                .unwrap_err();
            assert!(err.to_string().contains("regress"));
            assert_eq!(
                RaftLogReader::read_vote(&mut store).await.unwrap(),
                Some(vote)
            );

            let lower_node_same_term = Vote::<ControlPlaneRaftLeaderId>::new(3, 1);
            let err = RaftLogStorage::save_vote(&mut store, &lower_node_same_term)
                .await
                .unwrap_err();
            assert!(err.to_string().contains("regress"));

            let committed = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 2);
            RaftLogStorage::save_vote(&mut store, &committed)
                .await
                .unwrap();

            let uncommitted_same_leader = Vote::<ControlPlaneRaftLeaderId>::new(3, 2);
            let err = RaftLogStorage::save_vote(&mut store, &uncommitted_same_leader)
                .await
                .unwrap_err();
            assert!(err.to_string().contains("regress"));
            assert_eq!(
                RaftLogReader::read_vote(&mut store).await.unwrap(),
                Some(committed)
            );

            let higher = Vote::<ControlPlaneRaftLeaderId>::new(4, 1);
            RaftLogStorage::save_vote(&mut store, &higher)
                .await
                .unwrap();
            assert_eq!(
                RaftLogReader::read_vote(&mut store).await.unwrap(),
                Some(higher)
            );
        });
    }

    #[test]
    fn control_plane_raft_log_store_rejects_invalid_committed_watermarks() {
        ControlPlaneRaftTypeConfig::run(async {
            let mut store = ControlPlaneRaftLogStore::empty();

            let err = RaftLogStorage::save_committed(&mut store, Some(raft_log_id(3, 1, 1)))
                .await
                .unwrap_err();
            assert!(err.to_string().contains("log is empty"));
            assert_eq!(
                RaftLogStorage::read_committed(&mut store).await.unwrap(),
                None
            );

            RaftLogStorage::append(
                &mut store,
                vec![
                    bootstrap_membership_entry(1),
                    blank_entry(3, 1, 1),
                    blank_entry(3, 1, 2),
                ],
                IOFlushed::noop(),
            )
            .await
            .unwrap();
            let err = RaftLogStorage::save_committed(&mut store, Some(raft_log_id(3, 1, 2)))
                .await
                .unwrap_err();
            assert!(err.to_string().contains("missing vote state"));

            let lower_node_same_term_vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 0);
            RaftLogStorage::save_vote(&mut store, &lower_node_same_term_vote)
                .await
                .unwrap();
            let err = RaftLogStorage::save_committed(&mut store, Some(raft_log_id(3, 1, 2)))
                .await
                .unwrap_err();
            assert!(err.to_string().contains("does not cover"));

            let vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1);
            RaftLogStorage::save_vote(&mut store, &vote).await.unwrap();
            RaftLogStorage::save_committed(&mut store, Some(raft_log_id(3, 1, 2)))
                .await
                .unwrap();

            let err = RaftLogStorage::save_committed(&mut store, Some(raft_log_id(3, 1, 1)))
                .await
                .unwrap_err();
            assert!(err.to_string().contains("regress"));

            let err = RaftLogStorage::save_committed(&mut store, Some(raft_log_id(4, 1, 2)))
                .await
                .unwrap_err();
            assert!(err.to_string().contains("cannot change"));

            let err = RaftLogStorage::save_committed(&mut store, Some(raft_log_id(3, 1, 3)))
                .await
                .unwrap_err();
            assert!(err.to_string().contains("current last log id"));

            let err = RaftLogStorage::save_committed(&mut store, None)
                .await
                .unwrap_err();
            assert!(err.to_string().contains("cannot clear"));

            assert_eq!(
                RaftLogStorage::read_committed(&mut store).await.unwrap(),
                Some(raft_log_id(3, 1, 2))
            );
        });
    }

    #[test]
    fn control_plane_raft_log_store_rejects_truncating_committed_entries() {
        ControlPlaneRaftTypeConfig::run(async {
            let mut store = ControlPlaneRaftLogStore::empty();
            RaftLogStorage::append(
                &mut store,
                vec![
                    bootstrap_membership_entry(1),
                    blank_entry(3, 1, 1),
                    blank_entry(3, 1, 2),
                    blank_entry(3, 1, 3),
                ],
                IOFlushed::noop(),
            )
            .await
            .unwrap();
            let vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1);
            RaftLogStorage::save_vote(&mut store, &vote).await.unwrap();
            RaftLogStorage::save_committed(&mut store, Some(raft_log_id(3, 1, 2)))
                .await
                .unwrap();

            let err = RaftLogStorage::truncate_after(&mut store, Some(raft_log_id(3, 1, 1)))
                .await
                .unwrap_err();
            assert!(err.to_string().contains("committed log id"));

            let err = RaftLogStorage::truncate_after(&mut store, None)
                .await
                .unwrap_err();
            assert!(err.to_string().contains("committed log id"));

            RaftLogStorage::truncate_after(&mut store, Some(raft_log_id(3, 1, 2)))
                .await
                .unwrap();
            let log_state = RaftLogStorage::get_log_state(&mut store).await.unwrap();
            assert_eq!(log_state.last_log_id, Some(raft_log_id(3, 1, 2)));
            assert_eq!(
                RaftLogStorage::read_committed(&mut store).await.unwrap(),
                Some(raft_log_id(3, 1, 2))
            );
        });
    }

    #[test]
    fn control_plane_raft_log_store_rejects_unknown_truncate_boundaries() {
        ControlPlaneRaftTypeConfig::run(async {
            let mut store = ControlPlaneRaftLogStore::empty();
            RaftLogStorage::append(
                &mut store,
                vec![bootstrap_membership_entry(1), blank_entry(3, 1, 1)],
                IOFlushed::noop(),
            )
            .await
            .unwrap();

            let err = RaftLogStorage::truncate_after(&mut store, Some(raft_log_id(3, 1, 2)))
                .await
                .unwrap_err();
            assert!(err.to_string().contains("current last log id"));

            let err = RaftLogStorage::truncate_after(&mut store, Some(raft_log_id(4, 1, 1)))
                .await
                .unwrap_err();
            assert!(err.to_string().contains("mismatched log id"));

            let vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1);
            RaftLogStorage::save_vote(&mut store, &vote).await.unwrap();
            RaftLogStorage::save_committed(&mut store, Some(raft_log_id(3, 1, 1)))
                .await
                .unwrap();
            RaftLogStorage::purge(&mut store, raft_log_id(3, 1, 1))
                .await
                .unwrap();
            let err = RaftLogStorage::truncate_after(&mut store, Some(raft_log_id(4, 1, 1)))
                .await
                .unwrap_err();
            assert!(err.to_string().contains("mismatched purged log id"));

            RaftLogStorage::truncate_after(&mut store, Some(raft_log_id(3, 1, 1)))
                .await
                .unwrap();
            let log_state = RaftLogStorage::get_log_state(&mut store).await.unwrap();
            assert_eq!(log_state.last_log_id, Some(raft_log_id(3, 1, 1)));
        });
    }

    #[test]
    fn control_plane_raft_log_store_promotes_committed_gate_when_snapshot_purge_passes_it() {
        ControlPlaneRaftTypeConfig::run(async {
            let mut store = ControlPlaneRaftLogStore::empty();
            RaftLogStorage::append(
                &mut store,
                vec![
                    bootstrap_membership_entry(1),
                    blank_entry(3, 1, 1),
                    blank_entry(3, 1, 2),
                    blank_entry(3, 1, 3),
                ],
                IOFlushed::noop(),
            )
            .await
            .unwrap();
            let vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1);
            RaftLogStorage::save_vote(&mut store, &vote).await.unwrap();
            RaftLogStorage::save_committed(&mut store, Some(raft_log_id(3, 1, 2)))
                .await
                .unwrap();

            RaftLogStorage::purge(&mut store, raft_log_id(3, 1, 3))
                .await
                .unwrap();
            let log_state = RaftLogStorage::get_log_state(&mut store).await.unwrap();
            assert_eq!(log_state.last_purged_log_id, Some(raft_log_id(3, 1, 3)));
            assert_eq!(
                RaftLogStorage::read_committed(&mut store).await.unwrap(),
                Some(raft_log_id(3, 1, 3))
            );
            let entries = RaftLogReader::try_get_log_entries(&mut store, 0..4)
                .await
                .unwrap();
            assert!(entries.is_empty());
        });
    }

    #[test]
    fn control_plane_openraft_single_node_initialize_uses_bootstrap_membership() {
        ControlPlaneRaftTypeConfig::run(async {
            let mut log_store = ControlPlaneRaftLogStore::empty();
            let state_machine = ControlPlaneRaftStateMachine::empty();
            let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
                1,
                test_raft_config("control-plane-raft-bootstrap-membership-test"),
                UnreachableRaftNetworkFactory,
                log_store.clone(),
                state_machine,
            )
            .await
            .unwrap();

            let authority = ControlPlaneRaftAuthority::new_with_log_store(
                raft,
                log_store.clone(),
                "test-cluster",
            );
            authority
                .initialize_membership(BTreeMap::from([(1, BasicNode::new("node-1"))]))
                .await
                .unwrap();
            assert!(authority.is_initialized().await.unwrap());

            let bootstrap_log_id = raft_log_id(0, 1, 0);
            let status = authority.status().await.unwrap();
            assert_eq!(status.effective_membership_log_id(), Some(bootstrap_log_id));
            assert_eq!(status.effective_voters(), &BTreeSet::from([1]));

            let entries = RaftLogReader::try_get_log_entries(&mut log_store, 0..1)
                .await
                .unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].log_id, bootstrap_log_id);
            assert!(matches!(entries[0].payload, EntryPayload::Membership(_)));

            authority.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_triggered_single_node_client_write_applies_and_rejects() {
        ControlPlaneRaftTypeConfig::run(async {
            let log_store = ControlPlaneRaftLogStore::empty();
            let state_machine = ControlPlaneRaftStateMachine::empty();
            let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
                1,
                test_raft_config("control-plane-raft-client-write-test"),
                UnreachableRaftNetworkFactory,
                log_store.clone(),
                state_machine,
            )
            .await
            .unwrap();

            let authority =
                ControlPlaneRaftAuthority::new_with_log_store(raft, log_store, "test-cluster");
            authority
                .initialize_membership(BTreeMap::from([(1, BasicNode::new("node-1"))]))
                .await
                .unwrap();
            wait_for_local_leader(authority.raft(), "single-node initialization leadership").await;

            let bootstrap = authority
                .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![(NodeId::new(1), "node-1".to_string())],
                    pg_ids: vec![PgId::new(0)],
                })
                .await
                .unwrap();
            let bootstrap_log_id = bootstrap.log_id();
            assert!(matches!(
                bootstrap.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::BootstrapInitialClusterMap
                )
            ));
            let bootstrap_epoch = authority.status().await.unwrap().current_cluster_epoch();

            let heartbeat = authority
                .submit_control_plane_command(ControlPlaneCommand::RecordNodeHeartbeat {
                    heartbeat: NodeHeartbeat {
                        node_id: NodeId::new(1),
                        node_incarnation: 1,
                        endpoint: "node-1".to_string(),
                        observed_epoch: bootstrap_epoch,
                        requested_lease_duration_ms: 345,
                        cluster_map_history_reference_summary:
                            PgClusterMapHistoryReferenceSummary {
                                oldest_live_placement_epoch: None,
                                oldest_durable_backfill_epoch: None,
                            },
                        pg_observations: Vec::new(),
                    },
                    heartbeat_at_ms: 12_000,
                    lease_deadline_ms: 12_345,
                })
                .await
                .unwrap();
            assert_eq!(heartbeat.log_id().index(), bootstrap_log_id.index() + 1);
            assert!(matches!(
                heartbeat.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::RecordNodeHeartbeat
                )
            ));

            let rejected = authority
                .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(99),
                    availability: NodeAvailabilityState::Healthy,
                })
                .await
                .unwrap();
            assert_eq!(rejected.log_id().index(), heartbeat.log_id().index() + 1);
            let rejected_log_id = rejected.log_id();
            assert!(matches!(
                rejected.outcome(),
                ControlPlaneRaftCommandOutcome::Rejected(ControlPlaneError::UnknownNode {
                    node_id
                }) if *node_id == 99
            ));
            authority
                .wait_for_applied_log_id(
                    bootstrap_log_id,
                    Duration::from_secs(1),
                    "already-applied earlier entry remains reflected",
                )
                .await
                .unwrap();

            let (applied_snapshot, (applied_log_id, _applied_membership)) = authority
                .raft()
                .with_state_machine(|state_machine| {
                    let snapshot = state_machine.inner().snapshot().clone();
                    let applied_state = ControlPlaneRaftStateMachine::applied_state(state_machine);
                    Box::pin(async move { (snapshot, applied_state) })
                })
                .await
                .unwrap();
            assert!(applied_snapshot.node(NodeId::new(1)).is_some());
            assert!(applied_snapshot.node(NodeId::new(99)).is_none());
            assert_eq!(applied_log_id, Some(rejected_log_id));

            let status = authority.status().await.unwrap();
            assert_eq!(status.node_id(), 1);
            assert_eq!(status.current_leader(), Some(1));
            assert_eq!(status.server_state(), ServerState::Leader);
            assert!(status.local_leader());
            assert!(status.effective_voter());
            assert!(!status.effective_learner());
            assert!(status.applied_voter());
            assert!(!status.applied_learner());
            assert_eq!(
                status.linearized_authority_readiness(),
                ControlPlaneRaftLinearizedAuthorityReadiness::Serving
            );
            assert!(status.linearized_authority_serving());
            let persisted_vote = status
                .persisted_vote()
                .expect("single-node leader should persist a vote");
            assert!(persisted_vote.committed);
            assert_eq!(persisted_vote.leader_id.node_id, 1);
            assert_eq!(
                status.current_term(),
                Some(rejected_log_id.committed_leader_id().term)
            );
            assert_eq!(
                persisted_vote.leader_id.term,
                rejected_log_id.committed_leader_id().term
            );
            assert_eq!(status.last_log_id(), Some(rejected_log_id));
            assert_eq!(status.last_log_index(), Some(rejected_log_id.index()));
            assert_eq!(status.last_purged_log_id(), None);
            assert_eq!(status.last_purged_index(), None);
            assert_eq!(status.committed(), Some(rejected_log_id));
            assert_eq!(status.committed_index(), Some(rejected_log_id.index()));
            assert_eq!(status.applied(), Some(rejected_log_id));
            assert_eq!(status.applied_index(), Some(rejected_log_id.index()));
            assert_eq!(status.current_snapshot(), None);
            assert_eq!(status.current_snapshot_index(), None);
            assert_eq!(status.committed_to_applied_index_gap(), Some(0));
            assert_eq!(status.last_log_to_committed_index_gap(), Some(0));
            assert!(status.applied_caught_up_to_committed());
            assert!(status.committed_caught_up_to_last_log());
            assert_eq!(status.effective_voters(), &BTreeSet::from([1]));
            assert_eq!(status.applied_voters(), &BTreeSet::from([1]));
            assert_eq!(status.storage_node_lease_deadline_count(), 1);
            assert_eq!(
                status.earliest_storage_node_lease_deadline_ms(),
                Some(12_345)
            );
            assert_eq!(status.latest_storage_node_lease_deadline_ms(), Some(12_345));
            assert_eq!(
                status.metadata_transfer_fence_source_lease_deadline_count(),
                0
            );
            assert_eq!(
                status.earliest_metadata_transfer_fence_source_lease_deadline_ms(),
                None
            );
            assert_eq!(
                status.latest_metadata_transfer_fence_source_lease_deadline_ms(),
                None
            );

            authority.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_read_index_runtime_map_uses_applied_tip() {
        ControlPlaneRaftTypeConfig::run(async {
            let log_store = ControlPlaneRaftLogStore::empty();
            let state_machine = ControlPlaneRaftStateMachine::empty();
            let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
                1,
                test_raft_config("control-plane-raft-read-index-runtime-map-test"),
                UnreachableRaftNetworkFactory,
                log_store,
                state_machine,
            )
            .await
            .unwrap();

            let authority = ControlPlaneRaftAuthority::new(raft);
            authority
                .initialize_membership(BTreeMap::from([(1, BasicNode::new("node-1"))]))
                .await
                .unwrap();
            wait_for_local_leader(authority.raft(), "single-node read-index leadership").await;

            let write = authority
                .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![(NodeId::new(1), "node-1".to_string())],
                    pg_ids: vec![PgId::new(0)],
                })
                .await
                .unwrap();
            assert!(matches!(
                write.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::BootstrapInitialClusterMap
                )
            ));

            let runtime_map = authority
                .linearized_runtime_map_snapshot(44_000)
                .await
                .unwrap();
            let applied_log_id = authority
                .status()
                .await
                .unwrap()
                .applied()
                .expect("read-index should have an applied tip");
            let expected_control_plane_read_index = control_plane_log_id_from_raft(applied_log_id)
                .expect("read-index should be non-bootstrap");

            assert_eq!(
                runtime_map.freshness_proof().read_index(),
                Some(expected_control_plane_read_index)
            );
            assert_eq!(runtime_map.freshness_proof().issued_at_ms(), Some(44_000));
            assert!(runtime_map.freshness_proof().is_serving_authority_read());
            assert!(runtime_map
                .nodes()
                .iter()
                .any(|node| node.node_id() == NodeId::new(1)));

            authority.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_linearized_authority_handle_submits_reads_and_reports_status() {
        ControlPlaneRaftTypeConfig::run(async {
            let log_store = ControlPlaneRaftLogStore::empty();
            let state_machine = ControlPlaneRaftStateMachine::empty();
            let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
                301,
                test_raft_config("control-plane-raft-linearized-authority-trait-test"),
                UnreachableRaftNetworkFactory,
                log_store,
                state_machine,
            )
            .await
            .unwrap();

            let authority = Arc::new(ControlPlaneRaftAuthority::new(raft));
            authority
                .initialize_membership(BTreeMap::from([(301, BasicNode::new("node-301"))]))
                .await
                .unwrap();
            wait_for_local_leader(authority.raft(), "linearized authority trait leadership").await;

            let handle = ControlPlaneRaftAuthorityHandle::new(Arc::clone(&authority));
            let linearized_authority = handle.as_linearized_authority();
            let write = linearized_authority
                .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![(NodeId::new(301), "node-301".to_string())],
                    pg_ids: vec![PgId::new(0)],
                })
                .await
                .unwrap();
            assert!(matches!(
                write.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::BootstrapInitialClusterMap
                )
            ));

            let runtime_map = linearized_authority
                .linearized_runtime_map_snapshot(66_000)
                .await
                .unwrap();
            let status = linearized_authority.status().await.unwrap();
            assert_eq!(
                status.linearized_authority_readiness(),
                ControlPlaneRaftLinearizedAuthorityReadiness::Serving
            );
            assert_eq!(status.current_leader(), Some(301));
            assert_eq!(status.applied(), Some(write.log_id()));
            let expected_read_index = control_plane_log_id_from_raft(
                status
                    .applied()
                    .expect("trait read should have an applied tip"),
            )
            .expect("trait read should be non-bootstrap");

            assert_eq!(
                runtime_map.freshness_proof().read_index(),
                Some(expected_read_index)
            );
            assert_eq!(runtime_map.freshness_proof().issued_at_ms(), Some(66_000));
            assert!(runtime_map
                .nodes()
                .iter()
                .any(|node| node.node_id() == NodeId::new(301)));

            authority.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_explicit_handles_manage_membership_and_leadership() {
        ControlPlaneRaftTypeConfig::run(async {
            let operation_timeout = Duration::from_secs(2);
            let (authority1, authority2) = initialized_two_node_authorities(
                "control-plane-raft-admin-authority-handle-test",
                401,
                402,
            )
            .await;
            let authority1 = Arc::new(authority1);
            let authority2 = Arc::new(authority2);
            let linearized1 = ControlPlaneRaftAuthorityHandle::new(Arc::clone(&authority1));
            let admin1 = ControlPlaneRaftLeaderRoutedAdminHandle::new(Arc::clone(&authority1));
            let admin2 = ControlPlaneRaftLeaderRoutedAdminHandle::new(Arc::clone(&authority2));
            let status2 = ControlPlaneRaftAuthorityStatusHandle::new(Arc::clone(&authority2));
            let bootstrap1 = ControlPlaneRaftAuthorityBootstrapHandle::new(Arc::clone(&authority1));
            let lifecycle1 =
                ControlPlaneRaftAuthorityNodeLifecycleHandle::new(Arc::clone(&authority1));
            let lifecycle2 =
                ControlPlaneRaftAuthorityNodeLifecycleHandle::new(Arc::clone(&authority2));
            let command_sink = linearized1.as_linearized_authority();
            let leader_admin = admin1.as_leader_routed_admin();

            assert!(bootstrap1.is_initialized().await.unwrap());
            let bootstrap = expect_bounded_control_plane_raft(
                command_sink.submit_control_plane_command(
                    ControlPlaneCommand::BootstrapInitialClusterMap {
                        nodes: vec![
                            (NodeId::new(401), "node-401".to_string()),
                            (NodeId::new(402), "node-402".to_string()),
                        ],
                        pg_ids: vec![PgId::new(0)],
                    },
                ),
                operation_timeout,
                "explicit handles bootstrap command",
            )
            .await;
            assert!(matches!(
                bootstrap.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::BootstrapInitialClusterMap
                )
            ));
            expect_bounded_control_plane_raft(
                lifecycle2.wait_for_applied_log_id(
                    bootstrap.log_id(),
                    Duration::from_secs(1),
                    "explicit handles follower applied bootstrap",
                ),
                operation_timeout,
                "explicit handles wait for follower bootstrap",
            )
            .await;

            expect_bounded_control_plane_raft(
                leader_admin.transfer_leadership_to(402),
                operation_timeout,
                "explicit handles transfer leadership",
            )
            .await;
            expect_bounded_control_plane_raft(
                lifecycle2.wait_for_current_leader(
                    402,
                    Duration::from_secs(1),
                    "explicit handles observed transferred leader",
                ),
                operation_timeout,
                "explicit handles wait for transferred leader",
            )
            .await;

            let membership_log_id = expect_bounded_control_plane_raft(
                admin2.replace_voters(BTreeSet::from([402]), false),
                operation_timeout,
                "explicit handles replace voters",
            )
            .await;
            expect_bounded_control_plane_raft(
                lifecycle2.wait_for_applied_log_id(
                    membership_log_id,
                    Duration::from_secs(1),
                    "explicit handles applied voter replacement",
                ),
                operation_timeout,
                "explicit handles wait for voter replacement",
            )
            .await;

            let status = status2.status().await.unwrap();
            assert_eq!(status.current_leader(), Some(402));
            assert_eq!(
                status.effective_membership_log_id(),
                Some(membership_log_id)
            );
            assert_eq!(status.effective_voters(), &BTreeSet::from([402]));
            assert_eq!(
                status.linearized_authority_readiness(),
                ControlPlaneRaftLinearizedAuthorityReadiness::Serving
            );

            expect_bounded_control_plane_raft(
                lifecycle1.shutdown(),
                operation_timeout,
                "explicit handles shutdown removed voter",
            )
            .await;
            expect_bounded_control_plane_raft(
                lifecycle2.shutdown(),
                operation_timeout,
                "explicit handles shutdown surviving voter",
            )
            .await;
        });
    }

    #[test]
    fn control_plane_openraft_authority_directories_route_by_node_id() {
        ControlPlaneRaftTypeConfig::run(async {
            let operation_timeout = Duration::from_secs(2);
            let (authority1, authority2) = initialized_two_node_authorities(
                "control-plane-raft-authority-capability-directory-test",
                411,
                412,
            )
            .await;
            let authority1 = Arc::new(authority1);
            let authority2 = Arc::new(authority2);
            let directory = InMemoryAuthorityCapabilityDirectory::default();
            directory.register(411, Arc::clone(&authority1));
            directory.register(412, Arc::clone(&authority2));
            let bootstrap_directory =
                ControlPlaneRaftAuthorityBootstrapDirectoryHandle::new(Arc::new(directory.clone()));
            let linearized_directory = ControlPlaneRaftLinearizedAuthorityDirectoryHandle::new(
                Arc::new(directory.clone()),
            );
            let admin_directory =
                ControlPlaneRaftLeaderRoutedAdminDirectoryHandle::new(Arc::new(directory.clone()));
            let node_lifecycle_directory =
                ControlPlaneRaftAuthorityNodeLifecycleDirectoryHandle::new(Arc::new(
                    directory.clone(),
                ));
            let status_list_handle =
                ControlPlaneRaftAuthorityStatusListHandle::new(Arc::new(directory.clone()));
            let observer_status =
                ControlPlaneRaftAuthorityStatusHandle::new(Arc::clone(&authority1));

            let missing_bootstrap = bootstrap_directory.authority_bootstrap_for_node(499).await;
            assert!(matches!(
                missing_bootstrap,
                Err(ControlPlaneError::RpcRemote { message })
                    if message.contains("no bootstrap node 499")
            ));
            let missing_node_lifecycle = node_lifecycle_directory
                .authority_node_lifecycle_for_node(499)
                .await;
            assert!(matches!(
                missing_node_lifecycle,
                Err(ControlPlaneError::RpcRemote { message })
                    if message.contains("no node-lifecycle node 499")
            ));
            let missing_linearized = linearized_directory
                .linearized_authority_for_node(499)
                .await;
            assert!(matches!(
                missing_linearized,
                Err(ControlPlaneError::RpcRemote { message })
                    if message.contains("no linearized node 499")
            ));
            let missing_admin = admin_directory.leader_routed_admin_for_node(499).await;
            assert!(matches!(
                missing_admin,
                Err(ControlPlaneError::RpcRemote { message })
                    if message.contains("no leader-routed admin node 499")
            ));

            let leader_bootstrap = expect_bounded_control_plane_raft(
                bootstrap_directory.authority_bootstrap_for_node(411),
                operation_timeout,
                "authority bootstrap directory lookup leader",
            )
            .await;
            assert!(
                expect_bounded_control_plane_raft(
                    leader_bootstrap.is_initialized(),
                    operation_timeout,
                    "authority bootstrap directory is initialized check",
                )
                .await
            );
            let leader_node_lifecycle = expect_bounded_control_plane_raft(
                node_lifecycle_directory.authority_node_lifecycle_for_node(411),
                operation_timeout,
                "authority node-lifecycle directory lookup leader",
            )
            .await;
            let follower_node_lifecycle = expect_bounded_control_plane_raft(
                node_lifecycle_directory.authority_node_lifecycle_for_node(412),
                operation_timeout,
                "authority node-lifecycle directory lookup follower",
            )
            .await;
            let routed_client = ControlPlaneRaftAuthorityRoutingHandle::new(
                observer_status,
                status_list_handle.clone(),
                linearized_directory.clone(),
            );
            let routed_admin = ControlPlaneRaftLeaderRoutedAdminRoutingHandle::new(
                status_list_handle.clone(),
                admin_directory.clone(),
            );
            let routed_admin = ControlPlaneRaftLeaderRoutedAdminHandle::new(Arc::new(routed_admin));
            let leader_routed_admin = routed_admin.as_leader_routed_admin();
            let routed_linearized_handle =
                ControlPlaneRaftAuthorityHandle::new(Arc::new(routed_client.clone()));
            let routed_linearized_authority = routed_linearized_handle.as_linearized_authority();
            wait_for_authority_status_matching(
                &authority1,
                operation_timeout,
                "authority capability directory leader serving before bootstrap command",
                ControlPlaneRaftAuthorityStatus::linearized_authority_serving,
            )
            .await;
            let bootstrap = expect_bounded_control_plane_raft(
                routed_client.submit_control_plane_command(
                    ControlPlaneCommand::BootstrapInitialClusterMap {
                        nodes: vec![
                            (NodeId::new(411), "node-411".to_string()),
                            (NodeId::new(412), "node-412".to_string()),
                        ],
                        pg_ids: vec![PgId::new(0)],
                    },
                ),
                operation_timeout,
                "authority capability directory bootstrap command",
            )
            .await;
            assert!(matches!(
                bootstrap.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::BootstrapInitialClusterMap
                )
            ));
            expect_bounded_control_plane_raft(
                follower_node_lifecycle.wait_for_applied_log_id(
                    bootstrap.log_id(),
                    Duration::from_secs(1),
                    "authority capability directory follower applied bootstrap",
                ),
                operation_timeout,
                "authority capability directory follower wait",
            )
            .await;

            expect_bounded_control_plane_raft(
                leader_routed_admin.transfer_leadership_to(412),
                operation_timeout,
                "authority capability directory routed transfer leadership",
            )
            .await;
            expect_bounded_control_plane_raft(
                follower_node_lifecycle.wait_for_current_leader(
                    412,
                    Duration::from_secs(1),
                    "authority capability directory observed transferred leader",
                ),
                operation_timeout,
                "authority capability directory wait for transferred leader",
            )
            .await;
            expect_bounded_control_plane_raft(
                leader_node_lifecycle.wait_for_current_leader(
                    412,
                    Duration::from_secs(1),
                    "authority capability directory observer saw transferred leader",
                ),
                operation_timeout,
                "authority capability directory wait for observer transfer view",
            )
            .await;
            let transferred_statuses = expect_bounded_control_plane_raft(
                async {
                    for _ in 0..100 {
                        let statuses = status_list_handle.authority_statuses().await?;
                        if statuses.get(&412).is_some_and(|status| {
                            status.current_leader() == Some(412)
                                && status.local_leader()
                                && status.applied_caught_up_to_committed()
                        }) {
                            return Ok(statuses);
                        }
                        ControlPlaneRaftTypeConfig::sleep(Duration::from_millis(10)).await;
                    }
                    Err(ControlPlaneError::RpcRemote {
                        message:
                            "authority status-list directory transferred leader did not catch up"
                                .to_string(),
                    })
                },
                operation_timeout,
                "authority status-list directory statuses after transfer",
            )
            .await;
            let status_list = status_list_handle.as_status_list();
            let listed_statuses = expect_bounded_control_plane_raft(
                status_list.authority_statuses(),
                operation_timeout,
                "authority status-list handle statuses after transfer",
            )
            .await;
            assert_eq!(
                listed_statuses.keys().copied().collect::<BTreeSet<_>>(),
                BTreeSet::from([411, 412])
            );
            assert_eq!(
                transferred_statuses
                    .keys()
                    .copied()
                    .collect::<BTreeSet<_>>(),
                BTreeSet::from([411, 412])
            );
            let old_leader_status = transferred_statuses
                .get(&411)
                .expect("directory should report old leader");
            assert_eq!(old_leader_status.current_leader(), Some(412));
            assert!(!old_leader_status.local_leader());
            let new_leader_status = transferred_statuses
                .get(&412)
                .expect("directory should report new leader");
            assert_eq!(new_leader_status.current_leader(), Some(412));
            assert!(new_leader_status.local_leader());
            assert!(new_leader_status.linearized_authority_serving());
            let linearized_directory_authority = expect_bounded_control_plane_raft(
                linearized_directory.linearized_authority_for_node(412),
                operation_timeout,
                "linearized authority directory lookup transferred leader",
            )
            .await;
            let linearized_directory_status = expect_bounded_control_plane_raft(
                linearized_directory_authority.status(),
                operation_timeout,
                "linearized authority directory transferred leader status",
            )
            .await;
            assert_eq!(linearized_directory_status.node_id(), 412);
            assert!(linearized_directory_status.linearized_authority_serving());
            let routed_client_serving_authority = expect_bounded_control_plane_raft(
                routed_client.current_serving_linearized_authority(),
                operation_timeout,
                "authority routing handle current serving linearized authority",
            )
            .await;
            let routed_client_serving_status = expect_bounded_control_plane_raft(
                async {
                    for _ in 0..100 {
                        let status = routed_client_serving_authority.status().await?;
                        if status.linearized_authority_serving() {
                            return Ok(status);
                        }
                        ControlPlaneRaftTypeConfig::sleep(Duration::from_millis(10)).await;
                    }
                    Err(ControlPlaneError::RpcRemote {
                        message:
                            "authority routing handle selected authority did not report serving"
                                .to_string(),
                    })
                },
                operation_timeout,
                "authority routing handle current serving routed authority status",
            )
            .await;
            assert_eq!(routed_client_serving_status.node_id(), 412);
            assert!(routed_client_serving_status.linearized_authority_serving());
            let routed_write = expect_bounded_control_plane_raft(
                routed_linearized_authority.submit_control_plane_command(
                    ControlPlaneCommand::MarkNodeAvailability {
                        node_id: NodeId::new(411),
                        availability: NodeAvailabilityState::Unavailable,
                    },
                ),
                operation_timeout,
                "authority capability directory routed command after transfer",
            )
            .await;
            assert!(matches!(
                routed_write.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::MarkNodeAvailability
                )
            ));
            let routed_status = expect_bounded_control_plane_raft(
                routed_linearized_authority.status(),
                operation_timeout,
                "authority capability directory routed status after transfer",
            )
            .await;
            assert_eq!(routed_status.node_id(), 412);
            assert_eq!(routed_status.current_leader(), Some(412));
            assert!(routed_status.local_leader());
            assert!(routed_status.linearized_authority_serving());
            let observer_status = expect_bounded_control_plane_raft(
                routed_client.observer_status(),
                operation_timeout,
                "authority capability directory observer status after transfer",
            )
            .await;
            assert_eq!(observer_status.node_id(), 411);
            assert_eq!(observer_status.current_leader(), Some(412));
            assert!(!observer_status.local_leader());
            let runtime_map = expect_bounded_control_plane_raft(
                routed_linearized_authority.linearized_runtime_map_snapshot(91_000),
                operation_timeout,
                "authority capability directory routed runtime map read",
            )
            .await;
            assert_eq!(runtime_map.freshness_proof().issued_at_ms(), Some(91_000));
            assert!(runtime_map
                .nodes()
                .iter()
                .any(|node| node.node_id() == NodeId::new(411)));
            assert!(runtime_map
                .nodes()
                .iter()
                .any(|node| node.node_id() == NodeId::new(412)));
            let direct_runtime_map = expect_bounded_control_plane_raft(
                routed_client_serving_authority.linearized_runtime_map_snapshot(91_001),
                operation_timeout,
                "authority capability directory current serving routed runtime map read",
            )
            .await;
            assert_eq!(
                direct_runtime_map.freshness_proof().issued_at_ms(),
                Some(91_001)
            );
            let routed_membership_log_id = expect_bounded_control_plane_raft(
                leader_routed_admin.replace_voters(BTreeSet::from([412]), false),
                operation_timeout,
                "authority capability directory routed voter replacement",
            )
            .await;
            expect_bounded_control_plane_raft(
                follower_node_lifecycle.wait_for_applied_log_id(
                    routed_membership_log_id,
                    Duration::from_secs(1),
                    "authority capability directory routed voter replacement applied",
                ),
                operation_timeout,
                "authority capability directory wait for routed voter replacement",
            )
            .await;
            let routed_membership_status = expect_bounded_control_plane_raft(
                routed_client.status(),
                operation_timeout,
                "authority capability directory routed status after voter replacement",
            )
            .await;
            assert_eq!(routed_membership_status.node_id(), 412);
            assert_eq!(
                routed_membership_status.effective_membership_log_id(),
                Some(routed_membership_log_id)
            );
            assert_eq!(
                routed_membership_status.effective_voters(),
                &BTreeSet::from([412])
            );
            assert!(routed_membership_status.local_leader());
            assert!(routed_membership_status.linearized_authority_serving());

            expect_bounded_control_plane_raft(
                leader_node_lifecycle.shutdown(),
                operation_timeout,
                "authority capability directory shutdown old leader",
            )
            .await;
            expect_bounded_control_plane_raft(
                follower_node_lifecycle.shutdown(),
                operation_timeout,
                "authority capability directory shutdown transferred leader",
            )
            .await;
        });
    }

    #[test]
    fn control_plane_openraft_authority_capability_directory_rejects_multiple_serving_authorities()
    {
        ControlPlaneRaftTypeConfig::run(async {
            let operation_timeout = Duration::from_secs(2);
            let network = InMemoryRaftNetworkFactory::default();
            let raft1 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
                421,
                test_raft_config("control-plane-raft-directory-multiple-serving-test-421"),
                network.clone(),
                ControlPlaneRaftLogStore::empty(),
                ControlPlaneRaftStateMachine::empty(),
            )
            .await
            .unwrap();
            let raft2 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
                422,
                test_raft_config("control-plane-raft-directory-multiple-serving-test-422"),
                network,
                ControlPlaneRaftLogStore::empty(),
                ControlPlaneRaftStateMachine::empty(),
            )
            .await
            .unwrap();
            let authority1 = Arc::new(ControlPlaneRaftAuthority::new(raft1));
            let authority2 = Arc::new(ControlPlaneRaftAuthority::new(raft2));
            authority1
                .initialize_membership(BTreeMap::from([(421, BasicNode::new("node-421"))]))
                .await
                .unwrap();
            authority2
                .initialize_membership(BTreeMap::from([(422, BasicNode::new("node-422"))]))
                .await
                .unwrap();
            wait_for_local_leader(authority1.raft(), "directory first independent leader").await;
            wait_for_local_leader(authority2.raft(), "directory second independent leader").await;
            wait_for_authority_status_matching(
                &authority1,
                operation_timeout,
                "directory first independent leader serving",
                ControlPlaneRaftAuthorityStatus::linearized_authority_serving,
            )
            .await;
            wait_for_authority_status_matching(
                &authority2,
                operation_timeout,
                "directory second independent leader serving",
                ControlPlaneRaftAuthorityStatus::linearized_authority_serving,
            )
            .await;

            let directory = InMemoryAuthorityCapabilityDirectory::default();
            directory.register(421, Arc::clone(&authority1));
            directory.register(422, Arc::clone(&authority2));
            let linearized_directory = ControlPlaneRaftLinearizedAuthorityDirectoryHandle::new(
                Arc::new(directory.clone()),
            );
            let status_list =
                ControlPlaneRaftAuthorityStatusListHandle::new(Arc::new(directory.clone()));
            let observer_status =
                ControlPlaneRaftAuthorityStatusHandle::new(Arc::clone(&authority1));
            let routed_client = ControlPlaneRaftAuthorityRoutingHandle::new(
                observer_status,
                status_list,
                linearized_directory,
            );

            let err = expect_bounded_control_plane_raft_error(
                routed_client.current_serving_linearized_authority(),
                operation_timeout,
                "routing handle rejects multiple serving authorities",
            )
            .await;
            assert!(matches!(
                err,
                ControlPlaneError::RpcRemote { message }
                    if message.contains("multiple serving raft authorities")
            ));

            authority1.shutdown().await.unwrap();
            authority2.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_routing_rejects_mismatched_linearized_directory_handle() {
        ControlPlaneRaftTypeConfig::run(async {
            let operation_timeout = Duration::from_secs(2);
            let (authority1, authority2) = initialized_two_node_authorities(
                "control-plane-raft-routing-mismatched-directory-test",
                451,
                452,
            )
            .await;
            let authority1 = Arc::new(authority1);
            let authority2 = Arc::new(authority2);
            wait_for_authority_status_matching(
                &authority1,
                operation_timeout,
                "mismatched directory selected leader serving",
                ControlPlaneRaftAuthorityStatus::linearized_authority_serving,
            )
            .await;

            let status_directory = InMemoryAuthorityCapabilityDirectory::default();
            status_directory.register(451, Arc::clone(&authority1));
            status_directory.register(452, Arc::clone(&authority2));
            let status_list =
                ControlPlaneRaftAuthorityStatusListHandle::new(Arc::new(status_directory));
            let observer_status =
                ControlPlaneRaftAuthorityStatusHandle::new(Arc::clone(&authority1));
            let mismatched_directory = FixedLinearizedAuthorityDirectory {
                authority: ControlPlaneRaftAuthorityHandle::new(Arc::clone(&authority2)),
            };
            let routed_client = ControlPlaneRaftAuthorityRoutingHandle::new(
                observer_status,
                status_list,
                ControlPlaneRaftLinearizedAuthorityDirectoryHandle::new(Arc::new(
                    mismatched_directory,
                )),
            );

            let err = expect_bounded_control_plane_raft_error(
                routed_client.current_serving_linearized_authority(),
                operation_timeout,
                "routing handle rejects mismatched linearized directory handle",
            )
            .await;
            assert!(matches!(
                err,
                ControlPlaneError::RpcRemote { message }
                    if message.contains(
                        "linearized authority directory returned node 452 for selected serving node 451"
                    )
            ));

            authority1.shutdown().await.unwrap();
            authority2.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_read_index_requires_leader() {
        ControlPlaneRaftTypeConfig::run(async {
            let log_store = ControlPlaneRaftLogStore::empty();
            let state_machine = ControlPlaneRaftStateMachine::empty();
            let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
                1,
                test_raft_config("control-plane-raft-read-index-non-leader-test"),
                UnreachableRaftNetworkFactory,
                log_store.clone(),
                state_machine,
            )
            .await
            .unwrap();

            let authority = ControlPlaneRaftAuthority::new(raft);
            authority
                .initialize_membership(BTreeMap::from([
                    (1, BasicNode::new("node-1")),
                    (2, BasicNode::new("node-2")),
                ]))
                .await
                .unwrap();

            let err = authority
                .raft()
                .ensure_linearizable(ReadPolicy::ReadIndex)
                .await
                .unwrap_err();
            let forward_to_leader = err.forward_to_leader().expect("read should need leader");
            assert_eq!(forward_to_leader.leader_id, None);
            assert_eq!(forward_to_leader.leader_node, None);

            let err = authority
                .linearized_runtime_map_snapshot(44_000)
                .await
                .unwrap_err();
            assert!(matches!(
                err,
                ControlPlaneError::RpcRemote { message }
                    if message.contains("OpenRaft read-index failed")
            ));

            let err = authority
                .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![(NodeId::new(1), "node-1".to_string())],
                    pg_ids: vec![PgId::new(0)],
                })
                .await
                .unwrap_err();
            assert!(matches!(
                err,
                ControlPlaneError::RpcRemote { message }
                    if message.contains("OpenRaft client-write failed")
            ));

            authority.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_two_node_client_write_replicates_to_follower() {
        ControlPlaneRaftTypeConfig::run(async {
            let (authority1, authority2) = initialized_two_node_authorities(
                "control-plane-raft-two-node-replication-test",
                101,
                102,
            )
            .await;

            let write = authority1
                .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![
                        (NodeId::new(101), "node-101".to_string()),
                        (NodeId::new(102), "node-102".to_string()),
                    ],
                    pg_ids: vec![PgId::new(0)],
                })
                .await
                .unwrap();
            assert!(matches!(
                write.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::BootstrapInitialClusterMap
                )
            ));

            authority2
                .wait_for_applied_log_id(
                    write.log_id(),
                    Duration::from_secs(1),
                    "two-node follower applied client write",
                )
                .await
                .unwrap();
            let follower_state = authority2
                .raft()
                .with_state_machine(|state_machine| {
                    let last_applied = state_machine.last_applied();
                    let node_ids = state_machine
                        .inner()
                        .snapshot()
                        .nodes()
                        .map(|node| node.node_id())
                        .collect::<Vec<_>>();
                    Box::pin(async move { (last_applied, node_ids) })
                })
                .await
                .unwrap();
            assert_eq!(follower_state.0, Some(write.log_id()));
            assert_eq!(follower_state.1, vec![NodeId::new(101), NodeId::new(102)]);

            authority1.shutdown().await.unwrap();
            authority2.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_two_node_rejected_command_replicates_without_mutation() {
        ControlPlaneRaftTypeConfig::run(async {
            let (authority1, authority2) = initialized_two_node_authorities(
                "control-plane-raft-two-node-rejected-command-test",
                401,
                402,
            )
            .await;

            let bootstrap = authority1
                .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![
                        (NodeId::new(401), "node-401".to_string()),
                        (NodeId::new(402), "node-402".to_string()),
                    ],
                    pg_ids: vec![PgId::new(0)],
                })
                .await
                .unwrap();
            assert!(matches!(
                bootstrap.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::BootstrapInitialClusterMap
                )
            ));

            let rejected = authority1
                .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(499),
                    availability: NodeAvailabilityState::Healthy,
                })
                .await
                .unwrap();
            assert_eq!(rejected.log_id().index(), bootstrap.log_id().index() + 1);
            assert!(matches!(
                rejected.outcome(),
                ControlPlaneRaftCommandOutcome::Rejected(ControlPlaneError::UnknownNode {
                    node_id
                }) if *node_id == 499
            ));

            authority2
                .wait_for_applied_log_id(
                    rejected.log_id(),
                    Duration::from_secs(1),
                    "two-node follower applied rejected command",
                )
                .await
                .unwrap();
            let follower_state = authority2
                .raft()
                .with_state_machine(|state_machine| {
                    let last_applied = state_machine.last_applied();
                    let snapshot = state_machine.inner().snapshot().clone();
                    Box::pin(async move { (last_applied, snapshot) })
                })
                .await
                .unwrap();
            assert_eq!(follower_state.0, Some(rejected.log_id()));
            assert!(follower_state.1.node(NodeId::new(401)).is_some());
            assert!(follower_state.1.node(NodeId::new(402)).is_some());
            assert!(follower_state.1.node(NodeId::new(499)).is_none());

            authority1.shutdown().await.unwrap();
            authority2.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_leader_transfer_fences_old_leader() {
        ControlPlaneRaftTypeConfig::run(async {
            let (authority1, authority2) = initialized_two_node_authorities(
                "control-plane-raft-leader-transfer-test",
                701,
                702,
            )
            .await;

            let bootstrap = authority1
                .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![
                        (NodeId::new(701), "node-701".to_string()),
                        (NodeId::new(702), "node-702".to_string()),
                    ],
                    pg_ids: vec![PgId::new(0)],
                })
                .await
                .unwrap();
            assert!(matches!(
                bootstrap.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::BootstrapInitialClusterMap
                )
            ));
            authority2
                .wait_for_applied_log_id(
                    bootstrap.log_id(),
                    Duration::from_secs(1),
                    "new leader candidate applied bootstrap before transfer",
                )
                .await
                .unwrap();

            authority1.transfer_leadership_to(702).await.unwrap();
            authority1
                .wait_for_current_leader(
                    702,
                    Duration::from_secs(1),
                    "old leader observed transferred leader",
                )
                .await
                .unwrap();
            authority2
                .wait_for_current_leader(
                    702,
                    Duration::from_secs(1),
                    "new leader observed transferred leadership",
                )
                .await
                .unwrap();

            let old_leader_read_err = authority1
                .linearized_runtime_map_snapshot(77_000)
                .await
                .unwrap_err();
            assert!(matches!(
                old_leader_read_err,
                ControlPlaneError::RpcRemote { message }
                    if message.contains("OpenRaft read-index failed")
            ));

            let old_leader_err = authority1
                .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(701),
                    availability: NodeAvailabilityState::Unavailable,
                })
                .await
                .unwrap_err();
            assert!(matches!(
                old_leader_err,
                ControlPlaneError::RpcRemote { message }
                    if message.contains("OpenRaft client-write failed")
            ));
            let old_leader_replace_voters_err = authority1
                .replace_voters(BTreeSet::from([701]), false)
                .await
                .unwrap_err();
            assert!(matches!(
                old_leader_replace_voters_err,
                ControlPlaneError::RpcRemote { message }
                    if message.contains("OpenRaft change-membership failed")
            ));
            let old_leader_add_learner_err = authority1
                .add_learner(703, BasicNode::new("node-703"), false)
                .await
                .unwrap_err();
            assert!(matches!(
                old_leader_add_learner_err,
                ControlPlaneError::RpcRemote { message }
                    if message.contains("OpenRaft add-learner failed")
            ));

            let follow_up = authority2
                .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(702),
                    availability: NodeAvailabilityState::Unavailable,
                })
                .await
                .unwrap();
            assert!(matches!(
                follow_up.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::MarkNodeAvailability
                )
            ));
            assert!(follow_up.log_id().index() > bootstrap.log_id().index());

            authority1
                .wait_for_applied_log_id(
                    follow_up.log_id(),
                    Duration::from_secs(1),
                    "old leader follower applied post-transfer command",
                )
                .await
                .unwrap();
            let old_status = authority1.status().await.unwrap();
            let new_status = authority2.status().await.unwrap();
            assert_eq!(old_status.current_leader(), Some(702));
            assert_eq!(new_status.current_leader(), Some(702));
            assert_eq!(old_status.server_state(), ServerState::Follower);
            assert_eq!(new_status.server_state(), ServerState::Leader);
            assert!(!old_status.local_leader());
            assert!(old_status.effective_voter());
            assert_eq!(
                old_status.linearized_authority_readiness(),
                ControlPlaneRaftLinearizedAuthorityReadiness::NotLocalLeader
            );
            assert!(!old_status.linearized_authority_serving());
            assert!(new_status.local_leader());
            assert!(new_status.effective_voter());
            assert_eq!(
                new_status.linearized_authority_readiness(),
                ControlPlaneRaftLinearizedAuthorityReadiness::Serving
            );
            assert!(new_status.linearized_authority_serving());
            assert_eq!(old_status.applied(), Some(follow_up.log_id()));
            assert_eq!(new_status.applied(), Some(follow_up.log_id()));

            let runtime_map = authority2
                .linearized_runtime_map_snapshot(78_000)
                .await
                .unwrap();
            let expected_read_index = control_plane_log_id_from_raft(follow_up.log_id())
                .expect("post-transfer command log id should be non-bootstrap");
            assert_eq!(
                runtime_map.freshness_proof().read_index(),
                Some(expected_read_index)
            );
            assert_eq!(runtime_map.freshness_proof().issued_at_ms(), Some(78_000));
            assert!(runtime_map.freshness_proof().is_serving_authority_read());

            authority1.shutdown().await.unwrap();
            authority2.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_two_node_membership_change_updates_state_machine() {
        ControlPlaneRaftTypeConfig::run(async {
            let operation_timeout = Duration::from_secs(2);
            let (authority1, authority2) = initialized_two_node_authorities(
                "control-plane-raft-two-node-membership-change-test",
                501,
                502,
            )
            .await;

            let bootstrap = expect_bounded_control_plane_raft(
                authority1.submit_control_plane_command(
                    ControlPlaneCommand::BootstrapInitialClusterMap {
                        nodes: vec![
                            (NodeId::new(501), "node-501".to_string()),
                            (NodeId::new(502), "node-502".to_string()),
                        ],
                        pg_ids: vec![PgId::new(0)],
                    },
                ),
                operation_timeout,
                "two-node membership test bootstrap command",
            )
            .await;
            assert!(matches!(
                bootstrap.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::BootstrapInitialClusterMap
                )
            ));
            authority2
                .wait_for_applied_log_id(
                    bootstrap.log_id(),
                    Duration::from_secs(1),
                    "removed voter candidate applied bootstrap before serving",
                )
                .await
                .unwrap();

            expect_bounded_control_plane_raft(
                authority1.transfer_leadership_to(502),
                operation_timeout,
                "two-node membership test transfer leadership to removed voter",
            )
            .await;
            authority1
                .wait_for_current_leader(
                    502,
                    Duration::from_secs(1),
                    "old leader observed removed voter leadership",
                )
                .await
                .unwrap();
            authority2
                .wait_for_current_leader(
                    502,
                    Duration::from_secs(1),
                    "removed voter became serving leader before removal",
                )
                .await
                .unwrap();

            let pre_removal_runtime_map = expect_bounded_control_plane_raft(
                authority2.linearized_runtime_map_snapshot(50_000),
                operation_timeout,
                "two-node membership test pre-removal read-index runtime map",
            )
            .await;
            assert!(pre_removal_runtime_map
                .freshness_proof()
                .is_serving_authority_read());
            assert_eq!(
                pre_removal_runtime_map.freshness_proof().issued_at_ms(),
                Some(50_000)
            );
            let pre_removal_write = expect_bounded_control_plane_raft(
                authority2.submit_control_plane_command(
                    ControlPlaneCommand::MarkNodeAvailability {
                        node_id: NodeId::new(502),
                        availability: NodeAvailabilityState::Unavailable,
                    },
                ),
                operation_timeout,
                "two-node membership test pre-removal write through removed voter",
            )
            .await;
            assert!(matches!(
                pre_removal_write.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::MarkNodeAvailability
                )
            ));

            expect_bounded_control_plane_raft(
                authority2.transfer_leadership_to(501),
                operation_timeout,
                "two-node membership test transfer leadership back before removal",
            )
            .await;
            authority1
                .wait_for_current_leader(
                    501,
                    Duration::from_secs(1),
                    "surviving voter became leader before membership removal",
                )
                .await
                .unwrap();
            authority2
                .wait_for_current_leader(
                    501,
                    Duration::from_secs(1),
                    "removed voter observed surviving leader before removal",
                )
                .await
                .unwrap();

            let membership_log_id = expect_bounded_control_plane_raft(
                authority1.replace_voters(BTreeSet::from([501]), false),
                operation_timeout,
                "two-node membership test remove previously serving voter from membership",
            )
            .await;
            authority1
                .wait_for_applied_log_id(
                    membership_log_id,
                    Duration::from_secs(1),
                    "two-node leader applied membership change",
                )
                .await
                .unwrap();
            authority1
                .wait_for_current_leader(
                    501,
                    Duration::from_secs(1),
                    "remaining voter stayed leader after membership removal",
                )
                .await
                .unwrap();
            let status = expect_bounded_control_plane_raft(
                authority1.status(),
                operation_timeout,
                "two-node membership test status after membership removal",
            )
            .await;
            assert_eq!(status.current_leader(), Some(501));
            assert_eq!(
                status.effective_membership_log_id(),
                Some(membership_log_id)
            );
            assert_eq!(status.effective_voters(), &BTreeSet::from([501]));
            assert_eq!(
                status.linearized_authority_readiness(),
                ControlPlaneRaftLinearizedAuthorityReadiness::Serving
            );
            assert_eq!(status.applied_membership_log_id(), Some(membership_log_id));
            assert_eq!(status.applied_voters(), &BTreeSet::from([501]));

            let removed_read_err = expect_bounded_control_plane_raft_error(
                authority2.linearized_runtime_map_snapshot(50_100),
                operation_timeout,
                "two-node membership test removed voter read-index runtime map",
            )
            .await;
            assert!(matches!(
                removed_read_err,
                ControlPlaneError::RpcRemote { message }
                    if message.contains("OpenRaft read-index failed")
            ));
            let removed_write_err = expect_bounded_control_plane_raft_error(
                authority2.submit_control_plane_command(
                    ControlPlaneCommand::MarkNodeAvailability {
                        node_id: NodeId::new(502),
                        availability: NodeAvailabilityState::Unavailable,
                    },
                ),
                operation_timeout,
                "two-node membership test removed voter write",
            )
            .await;
            assert!(matches!(
                removed_write_err,
                ControlPlaneError::RpcRemote { message }
                    if message.contains("OpenRaft client-write failed")
            ));

            let follow_up = expect_bounded_control_plane_raft(
                authority1.submit_control_plane_command(
                    ControlPlaneCommand::MarkNodeAvailability {
                        node_id: NodeId::new(501),
                        availability: NodeAvailabilityState::Unavailable,
                    },
                ),
                operation_timeout,
                "two-node membership test follow-up write through surviving voter",
            )
            .await;
            assert!(matches!(
                follow_up.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::MarkNodeAvailability
                )
            ));
            assert!(follow_up.log_id().index() > membership_log_id.index());
            let follow_up_status = expect_bounded_control_plane_raft(
                authority1.status(),
                operation_timeout,
                "two-node membership test follow-up status",
            )
            .await;
            assert_eq!(follow_up_status.applied(), Some(follow_up.log_id()));
            assert_eq!(
                follow_up_status.effective_membership_log_id(),
                Some(membership_log_id)
            );
            assert_eq!(follow_up_status.effective_voters(), &BTreeSet::from([501]));
            assert_eq!(
                follow_up_status.applied_membership_log_id(),
                Some(membership_log_id)
            );
            assert_eq!(follow_up_status.applied_voters(), &BTreeSet::from([501]));

            authority1.shutdown().await.unwrap();
            authority2.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_adds_learner_then_promotes_to_voter() {
        ControlPlaneRaftTypeConfig::run(async {
            let (authority1, authority2, authority3) =
                initialized_three_node_cluster_with_two_voters(
                    "control-plane-raft-add-learner-promote-test",
                    601,
                    602,
                    603,
                )
                .await;

            let bootstrap = authority1
                .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![
                        (NodeId::new(601), "node-601".to_string()),
                        (NodeId::new(602), "node-602".to_string()),
                        (NodeId::new(603), "node-603".to_string()),
                    ],
                    pg_ids: vec![PgId::new(0)],
                })
                .await
                .unwrap();
            assert!(matches!(
                bootstrap.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::BootstrapInitialClusterMap
                )
            ));

            let learner_log_id = authority1
                .add_learner(603, BasicNode::new("node-603"), false)
                .await
                .unwrap();
            authority3
                .wait_for_applied_log_id(
                    learner_log_id,
                    Duration::from_secs(1),
                    "new learner applied learner membership",
                )
                .await
                .unwrap();
            let learner_status = authority3.status().await.unwrap();
            assert_eq!(learner_status.applied(), Some(learner_log_id));
            assert_eq!(
                learner_status.effective_membership_log_id(),
                Some(learner_log_id)
            );
            assert_eq!(
                learner_status.effective_voters(),
                &BTreeSet::from([601, 602])
            );
            assert_eq!(learner_status.effective_learners(), &BTreeSet::from([603]));
            assert!(!learner_status.effective_voter());
            assert!(learner_status.effective_learner());
            assert_eq!(
                learner_status.linearized_authority_readiness(),
                ControlPlaneRaftLinearizedAuthorityReadiness::NotLocalLeader
            );
            assert!(!learner_status.linearized_authority_serving());
            assert_eq!(
                learner_status.applied_membership_log_id(),
                Some(learner_log_id)
            );
            assert_eq!(learner_status.applied_voters(), &BTreeSet::from([601, 602]));
            assert_eq!(learner_status.applied_learners(), &BTreeSet::from([603]));
            assert!(!learner_status.applied_voter());
            assert!(learner_status.applied_learner());

            let promote_log_id = authority1
                .replace_voters(BTreeSet::from([601, 602, 603]), true)
                .await
                .unwrap();
            authority1
                .wait_for_applied_log_id(
                    promote_log_id,
                    Duration::from_secs(1),
                    "leader applied learner promotion",
                )
                .await
                .unwrap();
            authority3
                .wait_for_applied_log_id(
                    promote_log_id,
                    Duration::from_secs(1),
                    "promoted learner applied voter membership",
                )
                .await
                .unwrap();

            let leader_status = authority1.status().await.unwrap();
            assert_eq!(
                leader_status.effective_membership_log_id(),
                Some(promote_log_id)
            );
            assert_eq!(
                leader_status.effective_voters(),
                &BTreeSet::from([601, 602, 603])
            );
            assert_eq!(leader_status.effective_learners(), &BTreeSet::new());
            assert!(leader_status.local_leader());
            assert!(leader_status.effective_voter());
            assert!(!leader_status.effective_learner());
            assert_eq!(
                leader_status.linearized_authority_readiness(),
                ControlPlaneRaftLinearizedAuthorityReadiness::Serving
            );
            assert!(leader_status.linearized_authority_serving());
            assert_eq!(
                leader_status.applied_membership_log_id(),
                Some(promote_log_id)
            );
            assert_eq!(
                leader_status.applied_voters(),
                &BTreeSet::from([601, 602, 603])
            );
            assert_eq!(leader_status.applied_learners(), &BTreeSet::new());
            assert!(leader_status.applied_voter());
            assert!(!leader_status.applied_learner());

            let promoted_status = authority3.status().await.unwrap();
            assert_eq!(promoted_status.applied(), Some(promote_log_id));
            assert_eq!(
                promoted_status.effective_membership_log_id(),
                Some(promote_log_id)
            );
            assert_eq!(
                promoted_status.effective_voters(),
                &BTreeSet::from([601, 602, 603])
            );
            assert_eq!(promoted_status.effective_learners(), &BTreeSet::new());
            assert!(promoted_status.effective_voter());
            assert!(!promoted_status.effective_learner());
            assert_eq!(
                promoted_status.linearized_authority_readiness(),
                ControlPlaneRaftLinearizedAuthorityReadiness::NotLocalLeader
            );
            assert!(!promoted_status.linearized_authority_serving());
            assert_eq!(
                promoted_status.applied_membership_log_id(),
                Some(promote_log_id)
            );
            assert_eq!(
                promoted_status.applied_voters(),
                &BTreeSet::from([601, 602, 603])
            );
            assert_eq!(promoted_status.applied_learners(), &BTreeSet::new());
            assert!(promoted_status.applied_voter());
            assert!(!promoted_status.applied_learner());

            authority1.shutdown().await.unwrap();
            authority2.shutdown().await.unwrap();
            authority3.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_promoted_voter_restart_preserves_membership() {
        ControlPlaneRaftTypeConfig::run(async {
            let network = InMemoryRaftNetworkFactory::default();
            let config = test_raft_config("control-plane-raft-promoted-voter-restart-test");
            let log_store3 = ControlPlaneRaftLogStore::empty();
            let raft1 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
                621,
                config.clone(),
                network.clone(),
                ControlPlaneRaftLogStore::empty(),
                ControlPlaneRaftStateMachine::empty(),
            )
            .await
            .unwrap();
            let raft2 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
                622,
                config.clone(),
                network.clone(),
                ControlPlaneRaftLogStore::empty(),
                ControlPlaneRaftStateMachine::empty(),
            )
            .await
            .unwrap();
            let raft3 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
                623,
                config.clone(),
                network.clone(),
                log_store3.clone(),
                ControlPlaneRaftStateMachine::empty(),
            )
            .await
            .unwrap();
            network.register(621, raft1.clone());
            network.register(622, raft2.clone());
            network.register(623, raft3.clone());
            let authority1 = ControlPlaneRaftAuthority::new(raft1);
            let authority2 = ControlPlaneRaftAuthority::new(raft2);
            let authority3 = ControlPlaneRaftAuthority::new_with_log_store(
                raft3,
                log_store3.clone(),
                "test-cluster",
            );

            authority1
                .initialize_membership(BTreeMap::from([
                    (621, BasicNode::new("node-621")),
                    (622, BasicNode::new("node-622")),
                ]))
                .await
                .unwrap();
            wait_for_local_leader(
                authority1.raft(),
                "two-voter cluster initialized before promoted-voter restart",
            )
            .await;

            let bootstrap = authority1
                .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![
                        (NodeId::new(621), "node-621".to_string()),
                        (NodeId::new(622), "node-622".to_string()),
                        (NodeId::new(623), "node-623".to_string()),
                    ],
                    pg_ids: vec![PgId::new(0)],
                })
                .await
                .unwrap();
            assert!(matches!(
                bootstrap.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::BootstrapInitialClusterMap
                )
            ));

            let learner_log_id = authority1
                .add_learner(623, BasicNode::new("node-623"), false)
                .await
                .unwrap();
            authority3
                .wait_for_applied_index_at_least(
                    learner_log_id.index(),
                    Duration::from_secs(1),
                    "restart candidate applied learner membership",
                )
                .await
                .unwrap();

            let promote_log_id = authority1
                .replace_voters(BTreeSet::from([621, 622, 623]), true)
                .await
                .unwrap();
            authority3
                .wait_for_applied_log_id(
                    promote_log_id,
                    Duration::from_secs(1),
                    "restart candidate applied voter promotion",
                )
                .await
                .unwrap();
            let pre_restart_status = authority3.status().await.unwrap();
            assert_eq!(pre_restart_status.applied(), Some(promote_log_id));
            assert_eq!(
                pre_restart_status.effective_membership_log_id(),
                Some(promote_log_id)
            );
            assert_eq!(
                pre_restart_status.effective_voters(),
                &BTreeSet::from([621, 622, 623])
            );
            assert_eq!(
                pre_restart_status.applied_membership_log_id(),
                Some(promote_log_id)
            );
            assert_eq!(
                pre_restart_status.applied_voters(),
                &BTreeSet::from([621, 622, 623])
            );

            let restart_artifact =
                capture_openraft_restart_artifact(&log_store3, &authority3).await;
            authority3.shutdown().await.unwrap();
            network.unregister(623);

            let (restored_log_store, restored_state_machine) = restart_artifact.restore().unwrap();
            let restored_log_store_for_status = restored_log_store.clone();
            let restarted_raft =
                Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
                    623,
                    config,
                    network.clone(),
                    restored_log_store,
                    restored_state_machine,
                )
                .await
                .unwrap();
            network.register(623, restarted_raft.clone());
            let restarted_authority = ControlPlaneRaftAuthority::new_with_log_store(
                restarted_raft,
                restored_log_store_for_status,
                "test-cluster",
            );

            let restarted_status = restarted_authority.status().await.unwrap();
            assert_eq!(restarted_status.applied(), Some(promote_log_id));
            assert_eq!(
                restarted_status.effective_membership_log_id(),
                Some(promote_log_id)
            );
            assert_eq!(
                restarted_status.effective_voters(),
                &BTreeSet::from([621, 622, 623])
            );
            assert_eq!(
                restarted_status.applied_membership_log_id(),
                Some(promote_log_id)
            );
            assert_eq!(
                restarted_status.applied_voters(),
                &BTreeSet::from([621, 622, 623])
            );

            let post_restart_write = authority1
                .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(623),
                    availability: NodeAvailabilityState::Unavailable,
                })
                .await
                .unwrap();
            assert!(matches!(
                post_restart_write.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::MarkNodeAvailability
                )
            ));
            assert!(post_restart_write.log_id().index() > promote_log_id.index());
            restarted_authority
                .wait_for_applied_index_at_least(
                    post_restart_write.log_id().index(),
                    Duration::from_secs(1),
                    "restarted promoted voter applied post-restart command",
                )
                .await
                .unwrap();
            let caught_up_status = restarted_authority.status().await.unwrap();
            assert_eq!(
                caught_up_status.applied(),
                Some(post_restart_write.log_id())
            );
            assert_eq!(
                caught_up_status.effective_membership_log_id(),
                Some(promote_log_id)
            );
            assert_eq!(
                caught_up_status.effective_voters(),
                &BTreeSet::from([621, 622, 623])
            );
            assert_eq!(
                caught_up_status.applied_membership_log_id(),
                Some(promote_log_id)
            );
            assert_eq!(
                caught_up_status.applied_voters(),
                &BTreeSet::from([621, 622, 623])
            );
            let node623_availability = restarted_authority
                .raft()
                .with_state_machine(|state_machine| {
                    let availability = state_machine
                        .inner()
                        .snapshot()
                        .node(NodeId::new(623))
                        .map(|node| node.availability());
                    Box::pin(async move { availability })
                })
                .await
                .unwrap();
            assert_eq!(
                node623_availability,
                Some(NodeAvailabilityState::Unavailable)
            );

            authority1.shutdown().await.unwrap();
            authority2.shutdown().await.unwrap();
            restarted_authority.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_two_node_read_index_runtime_map_uses_quorum_applied_tip() {
        ControlPlaneRaftTypeConfig::run(async {
            let (authority1, authority2) = initialized_two_node_authorities(
                "control-plane-raft-two-node-read-index-runtime-map-test",
                201,
                202,
            )
            .await;

            let write = authority1
                .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![
                        (NodeId::new(201), "node-201".to_string()),
                        (NodeId::new(202), "node-202".to_string()),
                    ],
                    pg_ids: vec![PgId::new(0)],
                })
                .await
                .unwrap();
            assert!(matches!(
                write.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::BootstrapInitialClusterMap
                )
            ));
            authority2
                .wait_for_applied_log_id(
                    write.log_id(),
                    Duration::from_secs(1),
                    "two-node follower applied before read-index",
                )
                .await
                .unwrap();

            let runtime_map = authority1
                .linearized_runtime_map_snapshot(55_000)
                .await
                .unwrap();
            let applied_log_id = authority1
                .status()
                .await
                .unwrap()
                .applied()
                .expect("read-index should have an applied tip");
            assert!(applied_log_id.index() >= write.log_id().index());
            let expected_read_index = control_plane_log_id_from_raft(applied_log_id)
                .expect("read-index should be non-bootstrap");

            assert_eq!(
                runtime_map.freshness_proof().read_index(),
                Some(expected_read_index)
            );
            assert_eq!(runtime_map.freshness_proof().issued_at_ms(), Some(55_000));
            assert!(runtime_map.freshness_proof().is_serving_authority_read());
            assert!(runtime_map
                .nodes()
                .iter()
                .any(|node| node.node_id() == NodeId::new(201)));
            assert!(runtime_map
                .nodes()
                .iter()
                .any(|node| node.node_id() == NodeId::new(202)));

            authority1.shutdown().await.unwrap();
            authority2.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_restarted_follower_catches_up_committed_prefix() {
        ControlPlaneRaftTypeConfig::run(async {
            let ThreeVoterAuthorityFixture {
                network,
                config,
                leader_log_store: _,
                third_log_store,
                authority1,
                authority2,
                authority3,
            } = initialized_three_node_voter_authorities(
                "control-plane-raft-follower-restart-catch-up-test",
                801,
                802,
                803,
            )
            .await;

            let bootstrap = authority1
                .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![
                        (NodeId::new(801), "node-801".to_string()),
                        (NodeId::new(802), "node-802".to_string()),
                        (NodeId::new(803), "node-803".to_string()),
                    ],
                    pg_ids: vec![PgId::new(0)],
                })
                .await
                .unwrap();
            assert!(matches!(
                bootstrap.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::BootstrapInitialClusterMap
                )
            ));
            authority3
                .wait_for_applied_log_id(
                    bootstrap.log_id(),
                    Duration::from_secs(1),
                    "third voter applied bootstrap before restart",
                )
                .await
                .unwrap();

            let restart_artifact =
                capture_openraft_restart_artifact(&third_log_store, &authority3).await;
            authority3.shutdown().await.unwrap();
            network.unregister(803);

            let offline_write = authority1
                .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(802),
                    availability: NodeAvailabilityState::Unavailable,
                })
                .await
                .unwrap();
            assert!(matches!(
                offline_write.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::MarkNodeAvailability
                )
            ));
            authority2
                .wait_for_applied_index_at_least(
                    offline_write.log_id().index(),
                    Duration::from_secs(1),
                    "second voter applied command committed while third voter was down",
                )
                .await
                .unwrap();

            let (restored_log_store, restored_state_machine) = restart_artifact.restore().unwrap();
            let restored_log_store_for_status = restored_log_store.clone();
            let restarted_raft =
                Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
                    803,
                    config,
                    network.clone(),
                    restored_log_store,
                    restored_state_machine,
                )
                .await
                .unwrap();
            network.register(803, restarted_raft.clone());
            let restarted_authority = ControlPlaneRaftAuthority::new_with_log_store(
                restarted_raft,
                restored_log_store_for_status,
                "test-cluster",
            );

            let catch_up_trigger = authority1
                .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(803),
                    availability: NodeAvailabilityState::Unavailable,
                })
                .await
                .unwrap();
            assert!(matches!(
                catch_up_trigger.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::MarkNodeAvailability
                )
            ));
            assert!(catch_up_trigger.log_id().index() > offline_write.log_id().index());

            restarted_authority
                .wait_for_applied_index_at_least(
                    catch_up_trigger.log_id().index(),
                    Duration::from_secs(1),
                    "restarted third voter caught up missing committed prefix",
                )
                .await
                .unwrap();
            let restarted_status = restarted_authority.status().await.unwrap();
            assert_eq!(restarted_status.current_leader(), Some(801));
            assert_eq!(restarted_status.applied(), Some(catch_up_trigger.log_id()));
            assert_eq!(
                restarted_status.effective_voters(),
                &BTreeSet::from([801, 802, 803])
            );

            let restarted_state = restarted_authority
                .raft()
                .with_state_machine(|state_machine| {
                    let snapshot = state_machine.inner().snapshot();
                    let node_ids = snapshot
                        .nodes()
                        .map(|node| node.node_id())
                        .collect::<Vec<_>>();
                    let node802_availability = snapshot
                        .node(NodeId::new(802))
                        .map(|node| node.availability());
                    let node803_availability = snapshot
                        .node(NodeId::new(803))
                        .map(|node| node.availability());
                    Box::pin(async move { (node_ids, node802_availability, node803_availability) })
                })
                .await
                .unwrap();
            assert_eq!(
                restarted_state.0,
                vec![NodeId::new(801), NodeId::new(802), NodeId::new(803)]
            );
            assert_eq!(restarted_state.1, Some(NodeAvailabilityState::Unavailable));
            assert_eq!(restarted_state.2, Some(NodeAvailabilityState::Unavailable));

            authority1.shutdown().await.unwrap();
            authority2.shutdown().await.unwrap();
            restarted_authority.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_restarted_follower_catches_up_from_leader_snapshot() {
        ControlPlaneRaftTypeConfig::run(async {
            let ThreeVoterAuthorityFixture {
                network,
                config,
                leader_log_store,
                third_log_store,
                authority1,
                authority2,
                authority3,
            } = initialized_three_node_voter_authorities_with_config(
                test_raft_config_with_log_reversion(
                    "control-plane-raft-follower-snapshot-catch-up-test",
                    Some(true),
                ),
                1101,
                1102,
                1103,
            )
            .await;

            let bootstrap = authority1
                .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![
                        (NodeId::new(1101), "node-1101".to_string()),
                        (NodeId::new(1102), "node-1102".to_string()),
                        (NodeId::new(1103), "node-1103".to_string()),
                    ],
                    pg_ids: vec![PgId::new(0)],
                })
                .await
                .unwrap();
            assert!(matches!(
                bootstrap.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::BootstrapInitialClusterMap
                )
            ));
            authority3
                .wait_for_applied_log_id(
                    bootstrap.log_id(),
                    Duration::from_secs(1),
                    "third voter applied bootstrap before snapshot catch-up restart",
                )
                .await
                .unwrap();

            let restart_artifact =
                capture_openraft_restart_artifact(&third_log_store, &authority3).await;

            let offline_write = authority1
                .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(1102),
                    availability: NodeAvailabilityState::Unavailable,
                })
                .await
                .unwrap();
            assert!(matches!(
                offline_write.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::MarkNodeAvailability
                )
            ));
            authority2
                .wait_for_applied_index_at_least(
                    offline_write.log_id().index(),
                    Duration::from_secs(1),
                    "second voter applied command before leader snapshot purge",
                )
                .await
                .unwrap();
            authority3
                .wait_for_applied_index_at_least(
                    offline_write.log_id().index(),
                    Duration::from_secs(1),
                    "third voter applied command before leader snapshot purge",
                )
                .await
                .unwrap();

            let mut snapshot_progress = authority1.raft().watch_snapshot_progress();
            authority1.raft().trigger().snapshot().await.unwrap();
            snapshot_progress
                .wait_until_ge(&Some(offline_write.log_id()))
                .await
                .unwrap();
            let leader_snapshot = authority1.raft().get_snapshot().await.unwrap().unwrap();
            assert_eq!(
                leader_snapshot.meta.last_log_id,
                Some(offline_write.log_id())
            );

            authority1
                .raft()
                .trigger()
                .purge_log(offline_write.log_id().index())
                .await
                .unwrap();
            wait_for_log_purged_to(
                &leader_log_store,
                offline_write.log_id(),
                "leader purged log prefix covered by snapshot",
            )
            .await;
            let leader_status = authority1.status().await.unwrap();
            let leader_vote = leader_status
                .persisted_vote()
                .expect("leader should report its persisted vote");
            assert!(leader_vote.committed);
            assert_eq!(leader_vote.leader_id.node_id, 1101);
            assert_eq!(
                leader_status.current_term(),
                Some(offline_write.log_id().committed_leader_id().term)
            );
            assert_eq!(
                leader_status.last_purged_log_id(),
                Some(offline_write.log_id())
            );

            authority3.shutdown().await.unwrap();
            network.unregister(1103);

            let (restored_log_store, restored_state_machine) = restart_artifact.restore().unwrap();
            let restored_log_store_for_status = restored_log_store.clone();
            let restarted_raft =
                Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
                    1103,
                    config,
                    network.clone(),
                    restored_log_store,
                    restored_state_machine,
                )
                .await
                .unwrap();
            network.register(1103, restarted_raft.clone());
            let restarted_authority = ControlPlaneRaftAuthority::new_with_log_store(
                restarted_raft,
                restored_log_store_for_status,
                "test-cluster",
            );
            authority1
                .raft()
                .trigger()
                .allow_next_revert(&1103, true)
                .await
                .unwrap()
                .unwrap();

            let catch_up_trigger = authority1
                .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(1103),
                    availability: NodeAvailabilityState::Unavailable,
                })
                .await
                .unwrap();
            assert!(matches!(
                catch_up_trigger.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::MarkNodeAvailability
                )
            ));

            restarted_authority
                .wait_for_applied_index_at_least(
                    catch_up_trigger.log_id().index(),
                    Duration::from_secs(1),
                    "restarted third voter caught up through leader snapshot",
                )
                .await
                .unwrap();
            let restarted_status = restarted_authority.status().await.unwrap();
            assert_eq!(restarted_status.applied(), Some(catch_up_trigger.log_id()));
            let restarted_vote = restarted_status
                .persisted_vote()
                .expect("restarted follower should retain its persisted vote");
            assert_eq!(restarted_vote.leader_id.node_id, 1101);
            assert_eq!(
                restarted_status.current_term(),
                Some(catch_up_trigger.log_id().committed_leader_id().term)
            );
            assert_eq!(
                restarted_status.last_purged_log_id(),
                Some(offline_write.log_id())
            );
            assert_eq!(
                restarted_status.current_snapshot(),
                Some(offline_write.log_id())
            );

            let restarted_state = restarted_authority
                .raft()
                .with_state_machine(|state_machine| {
                    let snapshot_log_id = state_machine
                        .current_snapshot()
                        .and_then(|snapshot| snapshot.meta.last_log_id);
                    let snapshot = state_machine.inner().snapshot();
                    let node1102_availability = snapshot
                        .node(NodeId::new(1102))
                        .map(|node| node.availability());
                    let node1103_availability = snapshot
                        .node(NodeId::new(1103))
                        .map(|node| node.availability());
                    Box::pin(async move {
                        (
                            snapshot_log_id,
                            node1102_availability,
                            node1103_availability,
                        )
                    })
                })
                .await
                .unwrap();
            assert_eq!(restarted_state.0, Some(offline_write.log_id()));
            assert_eq!(restarted_state.1, Some(NodeAvailabilityState::Unavailable));
            assert_eq!(restarted_state.2, Some(NodeAvailabilityState::Unavailable));

            authority1.shutdown().await.unwrap();
            authority2.shutdown().await.unwrap();
            restarted_authority.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_restarted_leader_resumes_writes_and_reads() {
        ControlPlaneRaftTypeConfig::run(async {
            let ThreeVoterAuthorityFixture {
                network,
                config,
                leader_log_store,
                third_log_store: _,
                authority1,
                authority2,
                authority3,
            } = initialized_three_node_voter_authorities(
                "control-plane-raft-leader-restart-resume-test",
                901,
                902,
                903,
            )
            .await;

            let bootstrap = authority1
                .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![
                        (NodeId::new(901), "node-901".to_string()),
                        (NodeId::new(902), "node-902".to_string()),
                        (NodeId::new(903), "node-903".to_string()),
                    ],
                    pg_ids: vec![PgId::new(0)],
                })
                .await
                .unwrap();
            assert!(matches!(
                bootstrap.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::BootstrapInitialClusterMap
                )
            ));
            authority2
                .wait_for_applied_log_id(
                    bootstrap.log_id(),
                    Duration::from_secs(1),
                    "second voter applied bootstrap before leader restart",
                )
                .await
                .unwrap();
            authority3
                .wait_for_applied_log_id(
                    bootstrap.log_id(),
                    Duration::from_secs(1),
                    "third voter applied bootstrap before leader restart",
                )
                .await
                .unwrap();

            let restart_artifact =
                capture_openraft_restart_artifact(&leader_log_store, &authority1).await;
            authority1.shutdown().await.unwrap();
            network.unregister(901);

            let follower_write_err = authority2
                .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(902),
                    availability: NodeAvailabilityState::Unavailable,
                })
                .await
                .unwrap_err();
            assert!(matches!(
                follower_write_err,
                ControlPlaneError::RpcRemote { message }
                    if message.contains("OpenRaft client-write failed")
            ));

            let (restored_log_store, restored_state_machine) = restart_artifact.restore().unwrap();
            let restored_log_store_for_status = restored_log_store.clone();
            let restarted_raft =
                Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
                    901,
                    config,
                    network.clone(),
                    restored_log_store,
                    restored_state_machine,
                )
                .await
                .unwrap();
            network.register(901, restarted_raft.clone());
            let restarted_authority = ControlPlaneRaftAuthority::new_with_log_store(
                restarted_raft,
                restored_log_store_for_status,
                "test-cluster",
            );
            restarted_authority
                .wait_for_current_leader(
                    901,
                    Duration::from_secs(1),
                    "restarted leader recovered current leadership",
                )
                .await
                .unwrap();

            let resumed_write = restarted_authority
                .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(903),
                    availability: NodeAvailabilityState::Unavailable,
                })
                .await
                .unwrap();
            assert!(matches!(
                resumed_write.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::MarkNodeAvailability
                )
            ));
            assert!(resumed_write.log_id().index() > bootstrap.log_id().index());
            authority2
                .wait_for_applied_index_at_least(
                    resumed_write.log_id().index(),
                    Duration::from_secs(1),
                    "second voter applied restarted leader write",
                )
                .await
                .unwrap();
            authority3
                .wait_for_applied_index_at_least(
                    resumed_write.log_id().index(),
                    Duration::from_secs(1),
                    "third voter applied restarted leader write",
                )
                .await
                .unwrap();

            let runtime_map = restarted_authority
                .linearized_runtime_map_snapshot(90_000)
                .await
                .unwrap();
            let expected_read_index = control_plane_log_id_from_raft(resumed_write.log_id())
                .expect("restarted leader command log id should be non-bootstrap");
            assert_eq!(
                runtime_map.freshness_proof().read_index(),
                Some(expected_read_index)
            );
            assert_eq!(runtime_map.freshness_proof().issued_at_ms(), Some(90_000));
            assert_eq!(
                runtime_map
                    .nodes()
                    .iter()
                    .map(|node| node.node_id())
                    .collect::<Vec<_>>(),
                vec![NodeId::new(901), NodeId::new(902), NodeId::new(903)]
            );
            let restarted_status = restarted_authority.status().await.unwrap();
            assert_eq!(restarted_status.server_state(), ServerState::Leader);
            assert!(restarted_status.local_leader());
            assert!(restarted_status.effective_voter());
            assert!(!restarted_status.effective_learner());
            assert!(restarted_status.applied_voter());
            assert!(!restarted_status.applied_learner());
            assert_eq!(
                restarted_status.linearized_authority_readiness(),
                ControlPlaneRaftLinearizedAuthorityReadiness::Serving
            );
            assert!(restarted_status.linearized_authority_serving());
            assert_eq!(restarted_status.applied(), Some(resumed_write.log_id()));
            assert_eq!(
                restarted_status.authority_incarnation(),
                runtime_map.freshness_proof().authority_incarnation()
            );
            assert_eq!(
                restarted_status.current_cluster_epoch(),
                runtime_map.cluster_epoch()
            );
            assert_eq!(restarted_status.oldest_storage_history_floor_epoch(), None);
            assert!(
                restarted_status.retained_history_count() > 0,
                "restarted leader should retain historical route state after epoch changes"
            );
            assert!(
                restarted_status.oldest_retained_history_epoch()
                    <= restarted_status.newest_retained_history_epoch()
            );
            assert!(restarted_status
                .newest_retained_history_epoch()
                .is_some_and(|epoch| epoch < restarted_status.current_cluster_epoch()));
            assert_eq!(restarted_status.storage_node_count(), 3);
            assert_eq!(restarted_status.joining_storage_node_count(), 0);
            assert_eq!(restarted_status.active_storage_node_count(), 3);
            assert_eq!(restarted_status.draining_storage_node_count(), 0);
            assert_eq!(restarted_status.out_storage_node_count(), 0);
            assert_eq!(restarted_status.removed_storage_node_count(), 0);
            assert_eq!(restarted_status.healthy_storage_node_count(), 0);
            assert_eq!(restarted_status.suspect_storage_node_count(), 2);
            assert_eq!(restarted_status.unavailable_storage_node_count(), 1);
            assert_eq!(restarted_status.pg_count(), 1);
            assert_eq!(restarted_status.active_pg_count(), 0);
            assert_eq!(restarted_status.peering_pg_count(), 1);
            assert_eq!(restarted_status.degraded_pg_count(), 0);
            assert_eq!(restarted_status.backfilling_pg_count(), 0);
            assert_eq!(restarted_status.inconsistent_pg_count(), 0);
            assert_eq!(restarted_status.active_primary_pg_count(), 0);
            assert_eq!(restarted_status.peering_metadata_transfer_pg_count(), 0);
            assert_eq!(restarted_status.metadata_transfer_fenced_pg_count(), 0);
            assert_eq!(restarted_status.storage_node_lease_deadline_count(), 0);
            assert_eq!(
                restarted_status.earliest_storage_node_lease_deadline_ms(),
                None
            );
            assert_eq!(
                restarted_status.latest_storage_node_lease_deadline_ms(),
                None
            );
            assert_eq!(
                restarted_status.metadata_transfer_fence_source_lease_deadline_count(),
                0
            );
            assert_eq!(
                restarted_status.earliest_metadata_transfer_fence_source_lease_deadline_ms(),
                None
            );
            assert_eq!(
                restarted_status.latest_metadata_transfer_fence_source_lease_deadline_ms(),
                None
            );

            let restarted_node903_availability = restarted_authority
                .raft()
                .with_state_machine(|state_machine| {
                    let availability = state_machine
                        .inner()
                        .snapshot()
                        .node(NodeId::new(903))
                        .map(|node| node.availability());
                    Box::pin(async move { availability })
                })
                .await
                .unwrap();
            assert_eq!(
                restarted_node903_availability,
                Some(NodeAvailabilityState::Unavailable)
            );

            restarted_authority.shutdown().await.unwrap();
            authority2.shutdown().await.unwrap();
            authority3.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_transferred_leader_continues_after_old_leader_loss() {
        ControlPlaneRaftTypeConfig::run(async {
            let ThreeVoterAuthorityFixture {
                network,
                config: _,
                leader_log_store: _,
                third_log_store: _,
                authority1,
                authority2,
                authority3,
            } = initialized_three_node_voter_authorities(
                "control-plane-raft-post-transfer-old-leader-loss-test",
                1001,
                1002,
                1003,
            )
            .await;

            let bootstrap = authority1
                .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![
                        (NodeId::new(1001), "node-1001".to_string()),
                        (NodeId::new(1002), "node-1002".to_string()),
                        (NodeId::new(1003), "node-1003".to_string()),
                    ],
                    pg_ids: vec![PgId::new(0)],
                })
                .await
                .unwrap();
            assert!(matches!(
                bootstrap.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::BootstrapInitialClusterMap
                )
            ));
            authority2
                .wait_for_applied_log_id(
                    bootstrap.log_id(),
                    Duration::from_secs(1),
                    "second voter applied bootstrap before leader loss",
                )
                .await
                .unwrap();
            authority3
                .wait_for_applied_log_id(
                    bootstrap.log_id(),
                    Duration::from_secs(1),
                    "third voter applied bootstrap before leader loss",
                )
                .await
                .unwrap();

            authority1.transfer_leadership_to(1002).await.unwrap();
            authority2
                .wait_for_current_leader(
                    1002,
                    Duration::from_secs(1),
                    "second voter accepted leadership before old leader loss",
                )
                .await
                .unwrap();
            authority3
                .wait_for_current_leader(
                    1002,
                    Duration::from_secs(1),
                    "third voter learned transferred leader before old leader loss",
                )
                .await
                .unwrap();

            authority1.shutdown().await.unwrap();
            network.unregister(1001);

            let failover_write = authority2
                .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(1001),
                    availability: NodeAvailabilityState::Unavailable,
                })
                .await
                .unwrap();
            assert!(matches!(
                failover_write.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::MarkNodeAvailability
                )
            ));
            assert_eq!(failover_write.log_id().committed_leader_id().node_id, 1002);
            assert!(
                failover_write.log_id().committed_leader_id().term
                    > bootstrap.log_id().committed_leader_id().term
            );
            authority3
                .wait_for_applied_index_at_least(
                    failover_write.log_id().index(),
                    Duration::from_secs(1),
                    "third voter applied failover leader write",
                )
                .await
                .unwrap();

            let follower_state = authority3
                .raft()
                .with_state_machine(|state_machine| {
                    let snapshot = state_machine.inner().snapshot();
                    let node_ids = snapshot
                        .nodes()
                        .map(|node| node.node_id())
                        .collect::<Vec<_>>();
                    let node1001_availability = snapshot
                        .node(NodeId::new(1001))
                        .map(|node| node.availability());
                    Box::pin(async move { (node_ids, node1001_availability) })
                })
                .await
                .unwrap();
            assert_eq!(
                follower_state.0,
                vec![NodeId::new(1001), NodeId::new(1002), NodeId::new(1003)]
            );
            assert_eq!(follower_state.1, Some(NodeAvailabilityState::Unavailable));

            authority2.shutdown().await.unwrap();
            authority3.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_restart_replays_committed_entries() {
        ControlPlaneRaftTypeConfig::run(async {
            let mut log_store = ControlPlaneRaftLogStore::empty();
            RaftLogStorage::append(
                &mut log_store,
                vec![
                    single_node_bootstrap_membership_entry(1),
                    blank_entry(3, 1, 1),
                    single_node_membership_entry(3, 1, 2),
                    blank_entry(3, 1, 3),
                ],
                IOFlushed::noop(),
            )
            .await
            .unwrap();
            let vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1);
            RaftLogStorage::save_vote(&mut log_store, &vote)
                .await
                .unwrap();
            RaftLogStorage::save_committed(&mut log_store, Some(raft_log_id(3, 1, 3)))
                .await
                .unwrap();

            let mut state_machine = ControlPlaneRaftStateMachine::empty();
            state_machine
                .apply_entry(bootstrap_membership_entry(1))
                .unwrap();
            state_machine.apply_entry(blank_entry(3, 1, 1)).unwrap();

            let artifact = ControlPlaneRaftRestartArtifact::capture(
                "test-cluster",
                &log_store,
                &state_machine,
            )
            .unwrap();
            let (mut restored_log_store, restored_state_machine) = artifact.restore().unwrap();
            assert_eq!(
                restored_state_machine.last_applied(),
                Some(raft_log_id(3, 1, 1))
            );

            let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
                1,
                test_raft_config("control-plane-raft-restart-replay-test"),
                UnreachableRaftNetworkFactory,
                restored_log_store.clone(),
                restored_state_machine,
            )
            .await
            .unwrap();

            assert!(raft.is_initialized().await.unwrap());
            let raft_state = raft
                .with_raft_state(|state| {
                    (
                        state.log_ids.last().cloned(),
                        state.local_committed().cloned(),
                        *state.membership_state.effective().log_id(),
                    )
                })
                .await
                .unwrap();
            assert_eq!(raft_state.0, Some(raft_log_id(3, 1, 3)));
            assert_eq!(raft_state.1, Some(raft_log_id(3, 1, 3)));
            assert_eq!(raft_state.2, Some(raft_log_id(3, 1, 2)));
            let applied_state = raft
                .with_state_machine(|state_machine| {
                    let applied_state = ControlPlaneRaftStateMachine::applied_state(state_machine);
                    Box::pin(async move { applied_state })
                })
                .await
                .unwrap();
            assert_eq!(applied_state.0, Some(raft_log_id(3, 1, 3)));
            assert_eq!(applied_state.1.log_id(), &Some(raft_log_id(3, 1, 2)));
            assert_eq!(
                RaftLogStorage::read_committed(&mut restored_log_store)
                    .await
                    .unwrap(),
                Some(raft_log_id(3, 1, 3))
            );

            raft.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_restart_replays_rejected_committed_entry() {
        ControlPlaneRaftTypeConfig::run(async {
            let mut log_store = ControlPlaneRaftLogStore::empty();
            RaftLogStorage::append(
                &mut log_store,
                vec![
                    bootstrap_membership_entry(1),
                    normal_entry(
                        3,
                        1,
                        1,
                        ControlPlaneCommand::BootstrapInitialClusterMap {
                            nodes: vec![(NodeId::new(1), "node-1".to_string())],
                            pg_ids: vec![PgId::new(0)],
                        },
                    ),
                    normal_entry(
                        3,
                        1,
                        2,
                        ControlPlaneCommand::MarkNodeAvailability {
                            node_id: NodeId::new(99),
                            availability: NodeAvailabilityState::Healthy,
                        },
                    ),
                ],
                IOFlushed::noop(),
            )
            .await
            .unwrap();
            let vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1);
            RaftLogStorage::save_vote(&mut log_store, &vote)
                .await
                .unwrap();
            RaftLogStorage::save_committed(&mut log_store, Some(raft_log_id(3, 1, 2)))
                .await
                .unwrap();

            let mut state_machine = ControlPlaneRaftStateMachine::empty();
            state_machine
                .apply_entry(bootstrap_membership_entry(1))
                .unwrap();

            let artifact = ControlPlaneRaftRestartArtifact::capture(
                "test-cluster",
                &log_store,
                &state_machine,
            )
            .unwrap();
            let (_, restored_state_machine) = artifact.restore().unwrap();
            let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
                1,
                test_raft_config("control-plane-raft-restart-rejection-test"),
                UnreachableRaftNetworkFactory,
                log_store,
                restored_state_machine,
            )
            .await
            .unwrap();

            let applied_state = raft
                .with_state_machine(|state_machine| {
                    let last_applied = state_machine.last_applied();
                    let inner_last_applied = state_machine.inner().last_applied();
                    let node_ids = state_machine
                        .inner()
                        .snapshot()
                        .nodes()
                        .map(|node| node.node_id())
                        .collect::<Vec<_>>();
                    Box::pin(async move { (last_applied, inner_last_applied, node_ids) })
                })
                .await
                .unwrap();
            assert_eq!(applied_state.0, Some(raft_log_id(3, 1, 2)));
            assert_eq!(applied_state.1, Some(ControlPlaneLogId::new(3, 2).unwrap()));
            assert_eq!(applied_state.2, vec![NodeId::new(1)]);

            raft.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_restart_restores_current_snapshot() {
        ControlPlaneRaftTypeConfig::run(async {
            let mut log_store = ControlPlaneRaftLogStore::empty();
            RaftLogStorage::append(
                &mut log_store,
                vec![
                    bootstrap_membership_entry(1),
                    normal_entry(
                        3,
                        1,
                        1,
                        ControlPlaneCommand::BootstrapInitialClusterMap {
                            nodes: vec![(NodeId::new(1), "node-1".to_string())],
                            pg_ids: vec![PgId::new(0)],
                        },
                    ),
                    blank_entry(3, 1, 2),
                ],
                IOFlushed::noop(),
            )
            .await
            .unwrap();
            let vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1);
            RaftLogStorage::save_vote(&mut log_store, &vote)
                .await
                .unwrap();
            RaftLogStorage::save_committed(&mut log_store, Some(raft_log_id(3, 1, 2)))
                .await
                .unwrap();
            RaftLogStorage::purge(&mut log_store, raft_log_id(3, 1, 2))
                .await
                .unwrap();

            let mut snapshot_source = ControlPlaneRaftStateMachine::empty();
            snapshot_source
                .apply_entry(bootstrap_membership_entry(1))
                .unwrap();
            snapshot_source
                .apply_entry(normal_entry(
                    3,
                    1,
                    1,
                    ControlPlaneCommand::BootstrapInitialClusterMap {
                        nodes: vec![(NodeId::new(1), "node-1".to_string())],
                        pg_ids: vec![PgId::new(0)],
                    },
                ))
                .unwrap();
            snapshot_source.apply_entry(blank_entry(3, 1, 2)).unwrap();
            let snapshot = snapshot_source.build_snapshot().unwrap();

            let mut state_machine = ControlPlaneRaftStateMachine::empty();
            state_machine.current_snapshot = Some(snapshot);

            let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
                1,
                test_raft_config("control-plane-raft-current-snapshot-recovery-test"),
                UnreachableRaftNetworkFactory,
                log_store.clone(),
                state_machine,
            )
            .await
            .unwrap();

            let raft_state = raft
                .with_raft_state(|state| {
                    (
                        state.local_committed().cloned(),
                        *state.membership_state.effective().log_id(),
                    )
                })
                .await
                .unwrap();
            assert_eq!(raft_state.0, Some(raft_log_id(3, 1, 2)));
            assert_eq!(raft_state.1, Some(raft_log_id(0, 1, 0)));
            let applied_state = raft
                .with_state_machine(|state_machine| {
                    let last_applied = state_machine.last_applied();
                    let node_ids = state_machine
                        .inner()
                        .snapshot()
                        .nodes()
                        .map(|node| node.node_id())
                        .collect::<Vec<_>>();
                    Box::pin(async move { (last_applied, node_ids) })
                })
                .await
                .unwrap();
            assert_eq!(applied_state.0, Some(raft_log_id(3, 1, 2)));
            assert_eq!(applied_state.1, vec![NodeId::new(1)]);

            let authority =
                ControlPlaneRaftAuthority::new_with_log_store(raft, log_store, "test-cluster");
            let status = authority.status().await.unwrap();
            assert_eq!(status.last_purged_index(), Some(2));
            assert_eq!(status.committed_index(), Some(2));
            assert_eq!(status.applied_index(), Some(2));
            assert_eq!(status.current_snapshot_index(), Some(2));
            assert_eq!(status.committed_to_applied_index_gap(), Some(0));
            assert!(status.applied_caught_up_to_committed());

            authority.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_raft_log_store_rejects_append_holes() {
        ControlPlaneRaftTypeConfig::run(async {
            let mut store = ControlPlaneRaftLogStore::empty();

            let err =
                RaftLogStorage::append(&mut store, vec![blank_entry(3, 1, 1)], IOFlushed::noop())
                    .await
                    .unwrap_err();
            assert!(err.to_string().contains("expected 0"));
            assert_eq!(
                RaftLogStorage::get_log_state(&mut store)
                    .await
                    .unwrap()
                    .last_log_id,
                None
            );

            RaftLogStorage::append(
                &mut store,
                vec![bootstrap_membership_entry(1)],
                IOFlushed::noop(),
            )
            .await
            .unwrap();
            let err = RaftLogStorage::append(
                &mut store,
                vec![blank_entry(3, 1, 1), blank_entry(3, 1, 3)],
                IOFlushed::noop(),
            )
            .await
            .unwrap_err();
            assert!(err.to_string().contains("log hole at index 2"));
            let entries = RaftLogReader::try_get_log_entries(&mut store, 1..5)
                .await
                .unwrap();
            assert!(entries.is_empty());
            let entries = RaftLogReader::try_get_log_entries(&mut store, 0..5)
                .await
                .unwrap();
            assert_eq!(
                entries.iter().map(|entry| entry.log_id).collect::<Vec<_>>(),
                vec![raft_log_id(0, 1, 0)]
            );
        });
    }

    #[test]
    fn control_plane_raft_log_store_rejects_append_after_max_index() {
        ControlPlaneRaftTypeConfig::run(async {
            let artifact = ControlPlaneRaftLogStoreRestartArtifact {
                vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
                committed: Some(raft_log_id(3, 1, u64::MAX)),
                last_purged_log_id: Some(raft_log_id(3, 1, u64::MAX)),
                ..Default::default()
            };
            let mut store = ControlPlaneRaftLogStore::from_restart_artifact(artifact).unwrap();

            let err = RaftLogStorage::append(
                &mut store,
                vec![blank_entry(3, 1, u64::MAX)],
                IOFlushed::noop(),
            )
            .await
            .unwrap_err();

            assert!(err
                .to_string()
                .contains("cannot append after u64::MAX OpenRaft log index"));
            let log_state = RaftLogStorage::get_log_state(&mut store).await.unwrap();
            assert_eq!(
                log_state.last_purged_log_id,
                Some(raft_log_id(3, 1, u64::MAX))
            );
            assert_eq!(log_state.last_log_id, Some(raft_log_id(3, 1, u64::MAX)));
        });
    }

    #[test]
    fn control_plane_raft_log_store_purges_and_truncates_without_holes() {
        ControlPlaneRaftTypeConfig::run(async {
            let mut store = ControlPlaneRaftLogStore::empty();
            RaftLogStorage::append(
                &mut store,
                vec![
                    bootstrap_membership_entry(1),
                    blank_entry(3, 1, 1),
                    blank_entry(3, 1, 2),
                    blank_entry(3, 1, 3),
                    blank_entry(3, 1, 4),
                ],
                IOFlushed::noop(),
            )
            .await
            .unwrap();

            let err = RaftLogStorage::purge(&mut store, raft_log_id(3, 1, 2))
                .await
                .unwrap_err();
            assert!(err.to_string().contains("no committed restart gate"));

            let vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1);
            RaftLogStorage::save_vote(&mut store, &vote).await.unwrap();
            RaftLogStorage::save_committed(&mut store, Some(raft_log_id(3, 1, 2)))
                .await
                .unwrap();
            RaftLogStorage::purge(&mut store, raft_log_id(3, 1, 2))
                .await
                .unwrap();
            let log_state = RaftLogStorage::get_log_state(&mut store).await.unwrap();
            assert_eq!(log_state.last_purged_log_id, Some(raft_log_id(3, 1, 2)));
            assert_eq!(log_state.last_log_id, Some(raft_log_id(3, 1, 4)));

            let entries = RaftLogReader::try_get_log_entries(&mut store, 1..5)
                .await
                .unwrap();
            assert_eq!(
                entries.iter().map(|entry| entry.log_id).collect::<Vec<_>>(),
                vec![raft_log_id(3, 1, 3), raft_log_id(3, 1, 4)]
            );

            RaftLogStorage::truncate_after(&mut store, Some(raft_log_id(3, 1, 3)))
                .await
                .unwrap();
            let log_state = RaftLogStorage::get_log_state(&mut store).await.unwrap();
            assert_eq!(log_state.last_purged_log_id, Some(raft_log_id(3, 1, 2)));
            assert_eq!(log_state.last_log_id, Some(raft_log_id(3, 1, 3)));

            RaftLogStorage::truncate_after(&mut store, Some(raft_log_id(3, 1, 2)))
                .await
                .unwrap();
            let log_state = RaftLogStorage::get_log_state(&mut store).await.unwrap();
            assert_eq!(log_state.last_purged_log_id, Some(raft_log_id(3, 1, 2)));
            assert_eq!(log_state.last_log_id, Some(raft_log_id(3, 1, 2)));

            RaftLogStorage::truncate_after(&mut store, None)
                .await
                .unwrap_err();
            let log_state = RaftLogStorage::get_log_state(&mut store).await.unwrap();
            assert_eq!(log_state.last_purged_log_id, Some(raft_log_id(3, 1, 2)));
            assert_eq!(log_state.last_log_id, Some(raft_log_id(3, 1, 2)));
        });
    }

    #[test]
    fn control_plane_raft_log_store_restores_restart_artifact() {
        ControlPlaneRaftTypeConfig::run(async {
            let mut store = ControlPlaneRaftLogStore::empty();
            let vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1);
            RaftLogStorage::save_vote(&mut store, &vote).await.unwrap();
            RaftLogStorage::append(
                &mut store,
                vec![
                    bootstrap_membership_entry(1),
                    blank_entry(3, 1, 1),
                    blank_entry(3, 1, 2),
                    blank_entry(3, 1, 3),
                    blank_entry(3, 1, 4),
                ],
                IOFlushed::noop(),
            )
            .await
            .unwrap();
            RaftLogStorage::save_committed(&mut store, Some(raft_log_id(3, 1, 2)))
                .await
                .unwrap();
            RaftLogStorage::purge(&mut store, raft_log_id(3, 1, 2))
                .await
                .unwrap();

            let artifact = store.export_restart_artifact().unwrap();
            let mut restored = ControlPlaneRaftLogStore::from_restart_artifact(artifact).unwrap();

            assert_eq!(
                RaftLogReader::read_vote(&mut restored).await.unwrap(),
                Some(vote)
            );
            assert_eq!(
                RaftLogStorage::read_committed(&mut restored).await.unwrap(),
                Some(raft_log_id(3, 1, 2))
            );
            let log_state = RaftLogStorage::get_log_state(&mut restored).await.unwrap();
            assert_eq!(log_state.last_purged_log_id, Some(raft_log_id(3, 1, 2)));
            assert_eq!(log_state.last_log_id, Some(raft_log_id(3, 1, 4)));

            let entries = RaftLogReader::try_get_log_entries(&mut restored, 0..5)
                .await
                .unwrap();
            assert_eq!(
                entries.iter().map(|entry| entry.log_id).collect::<Vec<_>>(),
                vec![raft_log_id(3, 1, 3), raft_log_id(3, 1, 4)]
            );

            RaftLogStorage::append(&mut restored, vec![blank_entry(3, 1, 5)], IOFlushed::noop())
                .await
                .unwrap();
            let log_state = RaftLogStorage::get_log_state(&mut restored).await.unwrap();
            assert_eq!(log_state.last_log_id, Some(raft_log_id(3, 1, 5)));
        });
    }

    #[test]
    fn control_plane_raft_combined_restart_restores_catchup_state() {
        ControlPlaneRaftTypeConfig::run(async {
            let mut log_store = ControlPlaneRaftLogStore::empty();
            RaftLogStorage::append(
                &mut log_store,
                vec![
                    bootstrap_membership_entry(1),
                    blank_entry(3, 1, 1),
                    blank_entry(3, 1, 2),
                    blank_entry(3, 1, 3),
                ],
                IOFlushed::noop(),
            )
            .await
            .unwrap();
            let vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1);
            RaftLogStorage::save_vote(&mut log_store, &vote)
                .await
                .unwrap();
            RaftLogStorage::save_committed(&mut log_store, Some(raft_log_id(3, 1, 3)))
                .await
                .unwrap();

            let mut state_machine = ControlPlaneRaftStateMachine::empty();
            state_machine
                .apply_entry(bootstrap_membership_entry(1))
                .unwrap();
            state_machine.apply_entry(blank_entry(3, 1, 1)).unwrap();
            state_machine.apply_entry(blank_entry(3, 1, 2)).unwrap();

            let artifact = ControlPlaneRaftRestartArtifact::capture(
                "test-cluster",
                &log_store,
                &state_machine,
            )
            .unwrap();
            let (mut restored_log_store, mut restored_state_machine) = artifact.restore().unwrap();

            assert_eq!(
                RaftLogStorage::read_committed(&mut restored_log_store)
                    .await
                    .unwrap(),
                Some(raft_log_id(3, 1, 3))
            );
            assert_eq!(
                restored_state_machine.last_applied(),
                Some(raft_log_id(3, 1, 2))
            );
            restored_state_machine
                .apply_entry(blank_entry(3, 1, 3))
                .unwrap();
            assert_eq!(
                restored_state_machine.last_applied(),
                Some(raft_log_id(3, 1, 3))
            );
        });
    }

    #[test]
    fn control_plane_raft_durable_restart_artifact_codec_round_trips() {
        ControlPlaneRaftTypeConfig::run(async {
            let bootstrap_membership = Membership::new(
                vec![BTreeSet::from([1])],
                BTreeMap::from([(1, BasicNode::new("raft-node-1"))]),
            )
            .unwrap();
            let bootstrap_entry = Entry {
                log_id: raft_log_id(0, 1, 0),
                payload: EntryPayload::Membership(bootstrap_membership),
            };
            let bootstrap_command = ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![(NodeId::new(1), "/tmp/node-1.sock".to_string())],
                pg_ids: vec![PgId::new(1)],
            };
            let command_entry = normal_entry(3, 1, 1, bootstrap_command);

            let mut log_store = ControlPlaneRaftLogStore::empty();
            RaftLogStorage::append(
                &mut log_store,
                vec![bootstrap_entry.clone(), command_entry.clone()],
                IOFlushed::noop(),
            )
            .await
            .unwrap();
            let vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1);
            RaftLogStorage::save_vote(&mut log_store, &vote)
                .await
                .unwrap();
            RaftLogStorage::save_committed(&mut log_store, Some(raft_log_id(3, 1, 1)))
                .await
                .unwrap();

            let mut state_machine = ControlPlaneRaftStateMachine::empty();
            state_machine.apply_entry(bootstrap_entry).unwrap();
            state_machine.apply_entry(command_entry).unwrap();
            let expected_snapshot = state_machine.inner().snapshot().clone();
            let artifact = ControlPlaneRaftRestartArtifact::capture(
                "test-cluster",
                &log_store,
                &state_machine,
            )
            .unwrap();

            let encoded = artifact.encode_durable_artifact().unwrap();
            let decoded = ControlPlaneRaftRestartArtifact::decode_durable_artifact(&encoded)
                .expect("durable restart artifact should decode");
            let (mut restored_log_store, restored_state_machine) = decoded.restore().unwrap();

            assert_eq!(
                RaftLogReader::read_vote(&mut restored_log_store)
                    .await
                    .unwrap(),
                Some(vote)
            );
            assert_eq!(
                RaftLogStorage::read_committed(&mut restored_log_store)
                    .await
                    .unwrap(),
                Some(raft_log_id(3, 1, 1))
            );
            assert_eq!(
                restored_state_machine.last_applied(),
                Some(raft_log_id(3, 1, 1))
            );
            assert_eq!(
                restored_state_machine.inner().snapshot(),
                &expected_snapshot
            );
            let restored_entries =
                RaftLogReader::try_get_log_entries(&mut restored_log_store, 0..2)
                    .await
                    .unwrap();
            assert_eq!(
                restored_entries
                    .iter()
                    .map(|entry| entry.log_id)
                    .collect::<Vec<_>>(),
                vec![raft_log_id(0, 1, 0), raft_log_id(3, 1, 1)]
            );
        });
    }

    #[test]
    fn control_plane_raft_durable_restart_artifact_file_round_trips() {
        let tmp = test_util::tempdir();
        let path = tmp.path().join("control-plane").join("raft.state");
        let artifact = ControlPlaneRaftRestartArtifact {
            cluster_name: "test-cluster".to_string(),
            log_store: ControlPlaneRaftLogStoreRestartArtifact {
                vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
                committed: Some(raft_log_id(3, 1, 1)),
                entries: vec![bootstrap_membership_entry(1), blank_entry(3, 1, 1)],
                ..Default::default()
            },
            state_machine: state_machine_restart_artifact_with_noops(3, 1, 1),
        };

        artifact.store_durable_artifact(&path).unwrap();
        assert!(!durable_artifact_tmp_path(&path).exists());

        let loaded = ControlPlaneRaftRestartArtifact::load_durable_artifact(&path)
            .expect("stored durable restart artifact should load");
        let (mut loaded_log_store, loaded_state_machine) = loaded.restore().unwrap();
        ControlPlaneRaftTypeConfig::run(async {
            assert_eq!(
                RaftLogStorage::read_committed(&mut loaded_log_store)
                    .await
                    .unwrap(),
                Some(raft_log_id(3, 1, 1))
            );
        });
        assert_eq!(
            loaded_state_machine.last_applied(),
            Some(raft_log_id(3, 1, 1))
        );
    }

    #[test]
    fn control_plane_raft_durable_restart_artifact_file_ignores_stale_temp_file() {
        let tmp = test_util::tempdir();
        let path = tmp.path().join("raft.state");
        let tmp_path = durable_artifact_tmp_path(&path);
        let committed_artifact = ControlPlaneRaftRestartArtifact {
            cluster_name: "test-cluster".to_string(),
            log_store: ControlPlaneRaftLogStoreRestartArtifact {
                vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
                committed: Some(raft_log_id(3, 1, 1)),
                entries: vec![bootstrap_membership_entry(1), blank_entry(3, 1, 1)],
                ..Default::default()
            },
            state_machine: state_machine_restart_artifact_with_noops(3, 1, 1),
        };
        let stale_temp_artifact = ControlPlaneRaftRestartArtifact {
            cluster_name: "test-cluster".to_string(),
            log_store: ControlPlaneRaftLogStoreRestartArtifact {
                vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(5, 1)),
                committed: Some(raft_log_id(5, 1, 2)),
                entries: vec![
                    bootstrap_membership_entry(1),
                    blank_entry(5, 1, 1),
                    blank_entry(5, 1, 2),
                ],
                ..Default::default()
            },
            state_machine: state_machine_restart_artifact_with_noops(5, 1, 2),
        };

        committed_artifact.store_durable_artifact(&path).unwrap();
        std::fs::write(
            &tmp_path,
            stale_temp_artifact.encode_durable_artifact().unwrap(),
        )
        .unwrap();

        let loaded = ControlPlaneRaftRestartArtifact::load_durable_artifact(&path)
            .expect("stable durable restart artifact should load despite stale temp file");
        let (mut loaded_log_store, loaded_state_machine) = loaded.restore().unwrap();
        ControlPlaneRaftTypeConfig::run(async {
            assert_eq!(
                RaftLogStorage::read_committed(&mut loaded_log_store)
                    .await
                    .unwrap(),
                Some(raft_log_id(3, 1, 1))
            );
        });
        assert_eq!(
            loaded_state_machine.last_applied(),
            Some(raft_log_id(3, 1, 1))
        );
    }

    #[test]
    fn control_plane_raft_durable_restart_artifact_file_preserves_existing_on_temp_failure() {
        let tmp = test_util::tempdir();
        let path = tmp.path().join("raft.state");
        let tmp_path = durable_artifact_tmp_path(&path);
        let committed_artifact = ControlPlaneRaftRestartArtifact {
            cluster_name: "test-cluster".to_string(),
            log_store: ControlPlaneRaftLogStoreRestartArtifact {
                vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
                committed: Some(raft_log_id(3, 1, 1)),
                entries: vec![bootstrap_membership_entry(1), blank_entry(3, 1, 1)],
                ..Default::default()
            },
            state_machine: state_machine_restart_artifact_with_noops(3, 1, 1),
        };
        let replacement_artifact = ControlPlaneRaftRestartArtifact {
            cluster_name: "test-cluster".to_string(),
            log_store: ControlPlaneRaftLogStoreRestartArtifact {
                vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(5, 1)),
                committed: Some(raft_log_id(5, 1, 2)),
                entries: vec![
                    bootstrap_membership_entry(1),
                    blank_entry(5, 1, 1),
                    blank_entry(5, 1, 2),
                ],
                ..Default::default()
            },
            state_machine: state_machine_restart_artifact_with_noops(5, 1, 2),
        };

        committed_artifact.store_durable_artifact(&path).unwrap();
        std::fs::create_dir(&tmp_path).unwrap();
        assert_error_contains(
            replacement_artifact.store_durable_artifact(&path),
            "create control-plane OpenRaft durable restart artifact temp file",
        );

        let loaded = ControlPlaneRaftRestartArtifact::load_durable_artifact(&path)
            .expect("previous durable restart artifact should remain after temp-file failure");
        let (mut loaded_log_store, loaded_state_machine) = loaded.restore().unwrap();
        ControlPlaneRaftTypeConfig::run(async {
            assert_eq!(
                RaftLogStorage::read_committed(&mut loaded_log_store)
                    .await
                    .unwrap(),
                Some(raft_log_id(3, 1, 1))
            );
        });
        assert_eq!(
            loaded_state_machine.last_applied(),
            Some(raft_log_id(3, 1, 1))
        );
    }

    #[test]
    fn control_plane_raft_durable_restart_artifact_file_rejects_corruption() {
        let tmp = test_util::tempdir();
        let path = tmp.path().join("raft.state");
        let artifact = ControlPlaneRaftRestartArtifact {
            cluster_name: "test-cluster".to_string(),
            log_store: ControlPlaneRaftLogStoreRestartArtifact::default(),
            state_machine: ControlPlaneRaftStateMachine::empty().export_restart_artifact(),
        };
        let mut encoded = artifact.encode_durable_artifact().unwrap();
        encoded[CONTROL_PLANE_RAFT_RESTART_MAGIC.len() + 2] ^= 1;
        std::fs::write(&path, encoded).unwrap();

        assert_error_contains(
            ControlPlaneRaftRestartArtifact::load_durable_artifact(&path),
            "checksum mismatch",
        );
    }

    #[test]
    fn control_plane_openraft_durable_single_node_starts_empty_without_artifact() {
        ControlPlaneRaftTypeConfig::run(async {
            let tmp = test_util::tempdir();
            let path = tmp.path().join("missing").join("raft.state");
            let authority = ControlPlaneRaftAuthority::new_experimental_single_node_durable(
                "control-plane-raft-durable-empty-start-test",
                1,
                &path,
            )
            .await
            .unwrap();

            assert!(!authority.is_initialized().await.unwrap());
            let status = authority.status().await.unwrap();
            assert_eq!(status.applied(), None);
            assert_eq!(status.committed(), None);
            assert_eq!(status.persisted_vote(), None);
        });
    }

    #[test]
    fn control_plane_openraft_durable_single_node_restores_artifact() {
        ControlPlaneRaftTypeConfig::run(async {
            let tmp = test_util::tempdir();
            let path = tmp.path().join("raft.state");
            let cluster_name = "control-plane-raft-durable-restore-test";

            let mut log_store = ControlPlaneRaftLogStore::empty();
            RaftLogStorage::append(
                &mut log_store,
                vec![
                    single_node_bootstrap_membership_entry(1),
                    blank_entry(3, 1, 1),
                    single_node_membership_entry(3, 1, 2),
                    blank_entry(3, 1, 3),
                ],
                IOFlushed::noop(),
            )
            .await
            .unwrap();
            RaftLogStorage::save_vote(
                &mut log_store,
                &Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1),
            )
            .await
            .unwrap();
            RaftLogStorage::save_committed(&mut log_store, Some(raft_log_id(3, 1, 3)))
                .await
                .unwrap();

            let mut state_machine = ControlPlaneRaftStateMachine::empty();
            state_machine
                .apply_entry(single_node_bootstrap_membership_entry(1))
                .unwrap();
            state_machine.apply_entry(blank_entry(3, 1, 1)).unwrap();
            ControlPlaneRaftRestartArtifact::capture(cluster_name, &log_store, &state_machine)
                .unwrap()
                .store_durable_artifact(&path)
                .unwrap();

            let authority = ControlPlaneRaftAuthority::new_experimental_single_node_durable(
                cluster_name,
                1,
                &path,
            )
            .await
            .unwrap();
            assert!(authority.is_initialized().await.unwrap());
            authority
                .wait_for_applied_log_id(
                    raft_log_id(3, 1, 3),
                    Duration::from_secs(1),
                    "durable single-node authority replayed committed suffix",
                )
                .await
                .unwrap();
            let status = authority.status().await.unwrap();
            assert_eq!(status.applied(), Some(raft_log_id(3, 1, 3)));
            assert_eq!(status.committed(), Some(raft_log_id(3, 1, 3)));
            assert_eq!(
                status.applied_membership_log_id(),
                Some(raft_log_id(3, 1, 2))
            );
        });
    }

    #[test]
    fn control_plane_openraft_durable_single_node_rejects_wrong_cluster_artifact() {
        ControlPlaneRaftTypeConfig::run(async {
            let tmp = test_util::tempdir();
            let path = tmp.path().join("raft.state");
            let artifact = ControlPlaneRaftRestartArtifact {
                cluster_name: "old-cluster".to_string(),
                log_store: ControlPlaneRaftLogStoreRestartArtifact::default(),
                state_machine: ControlPlaneRaftStateMachine::empty().export_restart_artifact(),
            };
            artifact.store_durable_artifact(&path).unwrap();

            assert_error_contains(
                ControlPlaneRaftAuthority::new_experimental_single_node_durable(
                    "new-cluster",
                    1,
                    &path,
                )
                .await,
                "belongs to cluster \"old-cluster\", not configured cluster \"new-cluster\"",
            );
        });
    }

    #[test]
    fn control_plane_openraft_durable_single_node_restores_current_snapshot_cache() {
        ControlPlaneRaftTypeConfig::run(async {
            let tmp = test_util::tempdir();
            let path = tmp.path().join("raft.state");
            let cluster_name = "control-plane-raft-durable-current-snapshot-cache-test";
            let bootstrap_entry = single_node_bootstrap_membership_entry(1);
            let bootstrap_command = normal_entry(
                3,
                1,
                1,
                ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![(NodeId::new(1), "/tmp/node-1.sock".to_string())],
                    pg_ids: vec![PgId::new(1)],
                },
            );

            let mut log_store = ControlPlaneRaftLogStore::empty();
            RaftLogStorage::append(
                &mut log_store,
                vec![bootstrap_entry.clone(), bootstrap_command.clone()],
                IOFlushed::noop(),
            )
            .await
            .unwrap();
            RaftLogStorage::save_vote(
                &mut log_store,
                &Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1),
            )
            .await
            .unwrap();
            RaftLogStorage::save_committed(&mut log_store, Some(raft_log_id(3, 1, 1)))
                .await
                .unwrap();

            let mut state_machine = ControlPlaneRaftStateMachine::empty();
            state_machine.apply_entry(bootstrap_entry).unwrap();
            state_machine.apply_entry(bootstrap_command).unwrap();
            let built_snapshot = state_machine.build_snapshot().unwrap();
            assert_eq!(built_snapshot.meta.last_log_id, Some(raft_log_id(3, 1, 1)));
            ControlPlaneRaftRestartArtifact::capture(cluster_name, &log_store, &state_machine)
                .unwrap()
                .store_durable_artifact(&path)
                .unwrap();

            let authority = ControlPlaneRaftAuthority::new_experimental_single_node_durable(
                cluster_name,
                1,
                &path,
            )
            .await
            .unwrap();

            let status = authority.status().await.unwrap();
            assert_eq!(status.applied(), Some(raft_log_id(3, 1, 1)));
            assert_eq!(status.committed(), Some(raft_log_id(3, 1, 1)));
            assert_eq!(status.current_snapshot(), Some(raft_log_id(3, 1, 1)));
            let restored_snapshot = authority.raft().get_snapshot().await.unwrap().unwrap();
            assert_eq!(
                restored_snapshot.meta.last_log_id,
                Some(raft_log_id(3, 1, 1))
            );
            assert_eq!(restored_snapshot.meta.snapshot_id, "control-plane-T3-N1-I1");

            authority.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_durable_single_node_validates_snapshot_suffix_after_purge() {
        ControlPlaneRaftTypeConfig::run(async {
            let tmp = test_util::tempdir();
            let path = tmp.path().join("raft.state");
            let cluster_name = "control-plane-raft-durable-snapshot-suffix-replay-test";
            let bootstrap_entry = single_node_bootstrap_membership_entry(1);
            let bootstrap_command = normal_entry(
                3,
                1,
                1,
                ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![(NodeId::new(1), "/tmp/node-1.sock".to_string())],
                    pg_ids: vec![PgId::new(1)],
                },
            );
            let suffix_entry_2 = blank_entry(3, 1, 2);
            let suffix_entry_3 = blank_entry(3, 1, 3);

            let mut log_store = ControlPlaneRaftLogStore::empty();
            RaftLogStorage::append(
                &mut log_store,
                vec![
                    bootstrap_entry.clone(),
                    bootstrap_command.clone(),
                    suffix_entry_2.clone(),
                    suffix_entry_3.clone(),
                ],
                IOFlushed::noop(),
            )
            .await
            .unwrap();
            RaftLogStorage::save_vote(
                &mut log_store,
                &Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1),
            )
            .await
            .unwrap();
            RaftLogStorage::save_committed(&mut log_store, Some(raft_log_id(3, 1, 3)))
                .await
                .unwrap();
            RaftLogStorage::purge(&mut log_store, raft_log_id(3, 1, 1))
                .await
                .unwrap();

            let mut state_machine = ControlPlaneRaftStateMachine::empty();
            state_machine.apply_entry(bootstrap_entry).unwrap();
            state_machine.apply_entry(bootstrap_command).unwrap();
            let built_snapshot = state_machine.build_snapshot().unwrap();
            assert_eq!(built_snapshot.meta.last_log_id, Some(raft_log_id(3, 1, 1)));
            state_machine.apply_entry(suffix_entry_2).unwrap();
            state_machine.apply_entry(suffix_entry_3).unwrap();
            assert_eq!(state_machine.last_applied(), Some(raft_log_id(3, 1, 3)));

            ControlPlaneRaftRestartArtifact::capture(cluster_name, &log_store, &state_machine)
                .unwrap()
                .store_durable_artifact(&path)
                .unwrap();

            let authority = ControlPlaneRaftAuthority::new_experimental_single_node_durable(
                cluster_name,
                1,
                &path,
            )
            .await
            .unwrap();
            authority
                .wait_for_applied_log_id(
                    raft_log_id(3, 1, 3),
                    Duration::from_secs(1),
                    "durable single-node authority restored suffix-validated snapshot",
                )
                .await
                .unwrap();

            let status = authority.status().await.unwrap();
            assert_eq!(status.last_purged_log_id(), Some(raft_log_id(3, 1, 1)));
            assert_eq!(status.current_snapshot(), Some(raft_log_id(3, 1, 1)));
            assert_eq!(status.committed(), Some(raft_log_id(3, 1, 3)));
            assert_eq!(status.applied(), Some(raft_log_id(3, 1, 3)));
            assert_eq!(status.committed_to_applied_index_gap(), Some(0));
            assert!(status.applied_caught_up_to_committed());

            let retained_entries = RaftLogReader::try_get_log_entries(&mut log_store, 0..4)
                .await
                .unwrap();
            assert_eq!(
                retained_entries
                    .iter()
                    .map(|entry| entry.log_id)
                    .collect::<Vec<_>>(),
                vec![raft_log_id(3, 1, 2), raft_log_id(3, 1, 3)]
            );

            authority.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_durable_single_node_rejects_multi_voter_artifact() {
        ControlPlaneRaftTypeConfig::run(async {
            let tmp = test_util::tempdir();
            let path = tmp.path().join("raft.state");
            let cluster_name = "control-plane-raft-durable-multi-voter-start-test";
            let artifact = ControlPlaneRaftRestartArtifact {
                cluster_name: cluster_name.to_string(),
                log_store: ControlPlaneRaftLogStoreRestartArtifact {
                    vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
                    committed: Some(raft_log_id(3, 1, 1)),
                    entries: vec![
                        single_node_bootstrap_membership_entry(1),
                        membership_entry(3, 1, 1),
                    ],
                    ..Default::default()
                },
                state_machine: ControlPlaneRaftStateMachineRestartArtifact {
                    inner: replicated_state_machine_with_noops(3, 1),
                    last_applied: Some(raft_log_id(3, 1, 1)),
                    last_membership: StoredMembership::new(
                        Some(raft_log_id(3, 1, 1)),
                        test_membership(),
                    ),
                    current_snapshot: None,
                },
            };
            artifact.store_durable_artifact(&path).unwrap();

            assert_error_contains(
                ControlPlaneRaftAuthority::new_experimental_single_node_durable(
                    cluster_name,
                    1,
                    &path,
                )
                .await,
                "must be single-node membership for local node 1",
            );
        });
    }

    #[test]
    fn control_plane_openraft_durable_single_node_rejects_unpositioned_multi_voter_membership() {
        ControlPlaneRaftTypeConfig::run(async {
            let tmp = test_util::tempdir();
            let path = tmp.path().join("raft.state");
            let cluster_name = "control-plane-raft-durable-unpositioned-multi-voter-start-test";
            let artifact = ControlPlaneRaftRestartArtifact {
                cluster_name: cluster_name.to_string(),
                log_store: ControlPlaneRaftLogStoreRestartArtifact {
                    vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
                    committed: Some(raft_log_id(3, 1, 1)),
                    entries: vec![
                        single_node_bootstrap_membership_entry(1),
                        blank_entry(3, 1, 1),
                    ],
                    ..Default::default()
                },
                state_machine: ControlPlaneRaftStateMachineRestartArtifact {
                    inner: replicated_state_machine_with_noops(3, 1),
                    last_applied: Some(raft_log_id(3, 1, 1)),
                    last_membership: StoredMembership::new(None, test_membership()),
                    current_snapshot: None,
                },
            };
            artifact.store_durable_artifact(&path).unwrap();

            assert_error_contains(
                ControlPlaneRaftAuthority::new_experimental_single_node_durable(
                    cluster_name,
                    1,
                    &path,
                )
                .await,
                "without log id must be empty uninitialized membership",
            );
        });
    }

    #[test]
    fn control_plane_openraft_durable_single_node_rejects_wrong_node_artifact() {
        ControlPlaneRaftTypeConfig::run(async {
            let tmp = test_util::tempdir();
            let path = tmp.path().join("raft.state");
            let cluster_name = "control-plane-raft-durable-wrong-node-start-test";
            let artifact = ControlPlaneRaftRestartArtifact {
                cluster_name: cluster_name.to_string(),
                log_store: ControlPlaneRaftLogStoreRestartArtifact {
                    vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
                    committed: Some(raft_log_id(3, 1, 1)),
                    entries: vec![
                        single_node_bootstrap_membership_entry(1),
                        blank_entry(3, 1, 1),
                    ],
                    ..Default::default()
                },
                state_machine: state_machine_restart_artifact_with_noops(3, 1, 1),
            };
            artifact.store_durable_artifact(&path).unwrap();

            assert_error_contains(
                ControlPlaneRaftAuthority::new_experimental_single_node_durable(
                    cluster_name,
                    2,
                    &path,
                )
                .await,
                "belongs to OpenRaft node 1, not local node 2",
            );
        });
    }

    #[test]
    fn control_plane_openraft_durable_single_node_rejects_corrupt_artifact() {
        ControlPlaneRaftTypeConfig::run(async {
            let tmp = test_util::tempdir();
            let path = tmp.path().join("raft.state");
            std::fs::write(&path, b"not a durable artifact").unwrap();

            assert_error_contains(
                ControlPlaneRaftAuthority::new_experimental_single_node_durable(
                    "control-plane-raft-durable-corrupt-start-test",
                    1,
                    &path,
                )
                .await,
                "checksum mismatch",
            );
        });
    }

    #[test]
    fn control_plane_raft_durable_restart_artifact_codec_rejects_malformed_frames() {
        let artifact = ControlPlaneRaftRestartArtifact {
            cluster_name: "test-cluster".to_string(),
            log_store: ControlPlaneRaftLogStoreRestartArtifact::default(),
            state_machine: ControlPlaneRaftStateMachine::empty().export_restart_artifact(),
        };
        assert!(matches!(
            ControlPlaneRaftRestartArtifact::decode_durable_artifact(b"short"),
            Err(ControlPlaneError::CommandDecode { .. })
        ));

        let encoded = artifact.encode_durable_artifact().unwrap();
        let mut bad_magic = encoded.clone();
        bad_magic[0] ^= 1;
        bad_magic.truncate(bad_magic.len() - CONTROL_PLANE_RAFT_RESTART_CHECKSUM_LEN);
        append_raft_artifact_checksum(&mut bad_magic);
        assert_error_contains(
            ControlPlaneRaftRestartArtifact::decode_durable_artifact(&bad_magic),
            "invalid control-plane OpenRaft durable restart artifact magic",
        );

        let mut unsupported_version = Vec::new();
        unsupported_version.extend_from_slice(CONTROL_PLANE_RAFT_RESTART_MAGIC);
        write_raft_u16(
            &mut unsupported_version,
            CONTROL_PLANE_RAFT_RESTART_VERSION + 1,
        );
        append_raft_artifact_checksum(&mut unsupported_version);
        assert_error_contains(
            ControlPlaneRaftRestartArtifact::decode_durable_artifact(&unsupported_version),
            "unsupported control-plane OpenRaft durable restart artifact version",
        );

        let mut truncated = encoded.clone();
        truncated.pop();
        assert_error_contains(
            ControlPlaneRaftRestartArtifact::decode_durable_artifact(&truncated),
            "checksum mismatch",
        );

        let mut unknown_entry_tag = Vec::new();
        unknown_entry_tag.extend_from_slice(CONTROL_PLANE_RAFT_RESTART_MAGIC);
        write_raft_u16(&mut unknown_entry_tag, CONTROL_PLANE_RAFT_RESTART_VERSION);
        write_raft_string(&mut unknown_entry_tag, "test-cluster").unwrap();
        write_raft_option_vote(&mut unknown_entry_tag, None);
        write_raft_option_log_id(&mut unknown_entry_tag, None);
        write_raft_option_log_id(&mut unknown_entry_tag, None);
        write_raft_u32(&mut unknown_entry_tag, 1);
        write_raft_log_id(&mut unknown_entry_tag, raft_log_id(0, 1, 0));
        write_raft_u8(&mut unknown_entry_tag, 99);
        append_raft_artifact_checksum(&mut unknown_entry_tag);
        assert_error_contains(
            ControlPlaneRaftRestartArtifact::decode_durable_artifact(&unknown_entry_tag),
            "unknown control-plane OpenRaft durable entry payload tag 99",
        );

        let index_zero_blank = ControlPlaneRaftRestartArtifact {
            cluster_name: "test-cluster".to_string(),
            log_store: ControlPlaneRaftLogStoreRestartArtifact {
                entries: vec![blank_entry(0, 1, 0)],
                ..Default::default()
            },
            state_machine: ControlPlaneRaftStateMachine::empty().export_restart_artifact(),
        }
        .encode_durable_artifact()
        .unwrap();
        assert_error_contains(
            ControlPlaneRaftRestartArtifact::decode_durable_artifact(&index_zero_blank),
            "log index 0 entry must be bootstrap membership",
        );

        let non_bootstrap_index_zero_membership = ControlPlaneRaftRestartArtifact {
            cluster_name: "test-cluster".to_string(),
            log_store: ControlPlaneRaftLogStoreRestartArtifact {
                entries: vec![membership_entry(1, 1, 0)],
                ..Default::default()
            },
            state_machine: ControlPlaneRaftStateMachine::empty().export_restart_artifact(),
        }
        .encode_durable_artifact()
        .unwrap();
        assert_error_contains(
            ControlPlaneRaftRestartArtifact::decode_durable_artifact(
                &non_bootstrap_index_zero_membership,
            ),
            "log index 0 entry must be bootstrap membership",
        );
    }

    #[test]
    fn control_plane_raft_durable_restart_artifact_decode_validates_restart_pair() {
        let artifact = ControlPlaneRaftRestartArtifact {
            cluster_name: "test-cluster".to_string(),
            log_store: ControlPlaneRaftLogStoreRestartArtifact {
                vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
                committed: Some(raft_log_id(3, 1, 2)),
                entries: vec![
                    bootstrap_membership_entry(1),
                    blank_entry(3, 1, 1),
                    blank_entry(3, 1, 2),
                    blank_entry(3, 1, 3),
                ],
                ..Default::default()
            },
            state_machine: state_machine_restart_artifact_with_noops(3, 1, 3),
        };
        let encoded = artifact.encode_durable_artifact().unwrap();

        assert_error_contains(
            ControlPlaneRaftRestartArtifact::decode_durable_artifact(&encoded),
            "after committed restart gate",
        );
    }

    #[test]
    fn control_plane_raft_combined_restart_rejects_inconsistent_artifacts() {
        let log_committed_through_two = ControlPlaneRaftLogStoreRestartArtifact {
            vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
            committed: Some(raft_log_id(3, 1, 2)),
            entries: vec![
                bootstrap_membership_entry(1),
                blank_entry(3, 1, 1),
                blank_entry(3, 1, 2),
                blank_entry(3, 1, 3),
            ],
            ..Default::default()
        };
        let applied_after_committed = ControlPlaneRaftRestartArtifact {
            cluster_name: "test-cluster".to_string(),
            log_store: log_committed_through_two.clone(),
            state_machine: state_machine_restart_artifact_with_noops(3, 1, 3),
        };
        let err = applied_after_committed.restore().unwrap_err();
        assert!(err.to_string().contains("after committed restart gate"));

        let missing_committed_gate = ControlPlaneRaftRestartArtifact {
            cluster_name: "test-cluster".to_string(),
            log_store: ControlPlaneRaftLogStoreRestartArtifact {
                entries: vec![bootstrap_membership_entry(1), blank_entry(3, 1, 1)],
                ..Default::default()
            },
            state_machine: state_machine_restart_artifact_with_noops(3, 1, 1),
        };
        let err = missing_committed_gate.restore().unwrap_err();
        assert!(err.to_string().contains("no committed restart gate"));

        let applied_unknown_to_log = ControlPlaneRaftRestartArtifact {
            cluster_name: "test-cluster".to_string(),
            log_store: ControlPlaneRaftLogStoreRestartArtifact {
                vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
                committed: Some(raft_log_id(3, 1, 2)),
                entries: vec![
                    bootstrap_membership_entry(1),
                    blank_entry(3, 1, 1),
                    blank_entry(3, 1, 2),
                ],
                ..Default::default()
            },
            state_machine: state_machine_restart_artifact_with_noops(3, 1, 3),
        };
        let err = applied_unknown_to_log.restore().unwrap_err();
        assert!(err
            .to_string()
            .contains("is not retained or purged in the log store"));

        let state_behind_purged_boundary = ControlPlaneRaftRestartArtifact {
            cluster_name: "test-cluster".to_string(),
            log_store: ControlPlaneRaftLogStoreRestartArtifact {
                vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
                committed: Some(raft_log_id(3, 1, 2)),
                last_purged_log_id: Some(raft_log_id(3, 1, 2)),
                entries: Vec::new(),
            },
            state_machine: state_machine_restart_artifact_with_noops(3, 1, 1),
        };
        let err = state_behind_purged_boundary.restore().unwrap_err();
        assert!(err.to_string().contains("behind purged boundary"));

        let bootstrap_entry = single_node_bootstrap_membership_entry(1);
        let bootstrap_command = normal_entry(
            3,
            1,
            1,
            ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![(NodeId::new(1), "/tmp/node-1.sock".to_string())],
                pg_ids: vec![PgId::new(1)],
            },
        );
        let mut state_machine = ControlPlaneRaftStateMachine::empty();
        state_machine.apply_entry(bootstrap_entry.clone()).unwrap();
        state_machine
            .apply_entry(bootstrap_command.clone())
            .unwrap();
        state_machine.build_snapshot().unwrap();
        state_machine.apply_entry(blank_entry(3, 1, 2)).unwrap();
        let mut artifact = ControlPlaneRaftRestartArtifact {
            cluster_name: "test-cluster".to_string(),
            log_store: ControlPlaneRaftLogStoreRestartArtifact {
                vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
                committed: Some(raft_log_id(3, 1, 2)),
                last_purged_log_id: Some(raft_log_id(3, 1, 1)),
                entries: vec![normal_entry(
                    3,
                    1,
                    2,
                    ControlPlaneCommand::MarkNodeAvailability {
                        node_id: NodeId::new(1),
                        availability: NodeAvailabilityState::Healthy,
                    },
                )],
            },
            state_machine: state_machine.export_restart_artifact(),
        };
        assert_eq!(
            artifact
                .state_machine
                .current_snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.meta.last_log_id),
            Some(raft_log_id(3, 1, 1))
        );
        let err = artifact.clone().restore().unwrap_err();
        assert!(err
            .to_string()
            .contains("cached OpenRaft snapshot plus retained suffix"));
        artifact.log_store.entries = vec![blank_entry(3, 1, 2)];
        artifact.clone().restore().unwrap();
        artifact.log_store.entries = vec![blank_entry(3, 1, 2), membership_entry(3, 1, 3)];
        artifact.restore().unwrap();

        let mut membership_state_machine = ControlPlaneRaftStateMachine::empty();
        membership_state_machine
            .apply_entry(bootstrap_entry.clone())
            .unwrap();
        membership_state_machine
            .apply_entry(bootstrap_command.clone())
            .unwrap();
        membership_state_machine.build_snapshot().unwrap();
        membership_state_machine
            .apply_entry(membership_entry(3, 1, 2))
            .unwrap();
        let mut stale_snapshot_wrong_membership = ControlPlaneRaftRestartArtifact {
            cluster_name: "test-cluster".to_string(),
            log_store: ControlPlaneRaftLogStoreRestartArtifact {
                vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
                committed: Some(raft_log_id(3, 1, 2)),
                last_purged_log_id: Some(raft_log_id(3, 1, 1)),
                entries: vec![membership_entry(3, 1, 2)],
            },
            state_machine: membership_state_machine.export_restart_artifact(),
        };
        stale_snapshot_wrong_membership
            .state_machine
            .current_snapshot
            .as_mut()
            .unwrap()
            .meta
            .last_membership = StoredMembership::new(Some(raft_log_id(0, 1, 0)), test_membership());
        let err = stale_snapshot_wrong_membership.restore().unwrap_err();
        assert!(err
            .to_string()
            .contains("cached OpenRaft snapshot membership"));
    }

    #[test]
    fn control_plane_raft_log_store_rejects_invalid_restart_artifacts() {
        let artifact_with_entry_at_purged_boundary = ControlPlaneRaftLogStoreRestartArtifact {
            last_purged_log_id: Some(raft_log_id(3, 1, 2)),
            entries: vec![blank_entry(3, 1, 2)],
            ..Default::default()
        };
        let err =
            ControlPlaneRaftLogStore::from_restart_artifact(artifact_with_entry_at_purged_boundary)
                .unwrap_err();
        assert!(err.to_string().contains("expected 3"));

        let artifact_with_log_hole = ControlPlaneRaftLogStoreRestartArtifact {
            entries: vec![
                bootstrap_membership_entry(1),
                blank_entry(3, 1, 1),
                blank_entry(3, 1, 3),
            ],
            ..Default::default()
        };
        let err =
            ControlPlaneRaftLogStore::from_restart_artifact(artifact_with_log_hole).unwrap_err();
        assert!(err.to_string().contains("log hole at index 2"));

        let artifact_with_future_committed = ControlPlaneRaftLogStoreRestartArtifact {
            committed: Some(raft_log_id(3, 1, 3)),
            entries: vec![
                bootstrap_membership_entry(1),
                blank_entry(3, 1, 1),
                blank_entry(3, 1, 2),
            ],
            ..Default::default()
        };
        let err = ControlPlaneRaftLogStore::from_restart_artifact(artifact_with_future_committed)
            .unwrap_err();
        assert!(err.to_string().contains("current last log id"));

        let artifact_with_mismatched_committed = ControlPlaneRaftLogStoreRestartArtifact {
            committed: Some(raft_log_id(4, 1, 2)),
            entries: vec![
                bootstrap_membership_entry(1),
                blank_entry(3, 1, 1),
                blank_entry(3, 1, 2),
            ],
            ..Default::default()
        };
        let err =
            ControlPlaneRaftLogStore::from_restart_artifact(artifact_with_mismatched_committed)
                .unwrap_err();
        assert!(err.to_string().contains("mismatched log id"));

        let artifact_with_committed_before_purge = ControlPlaneRaftLogStoreRestartArtifact {
            committed: Some(raft_log_id(3, 1, 1)),
            last_purged_log_id: Some(raft_log_id(3, 1, 2)),
            entries: vec![blank_entry(3, 1, 3)],
            ..Default::default()
        };
        let err =
            ControlPlaneRaftLogStore::from_restart_artifact(artifact_with_committed_before_purge)
                .unwrap_err();
        assert!(err.to_string().contains("before purged boundary"));

        let artifact_with_missing_vote = ControlPlaneRaftLogStoreRestartArtifact {
            committed: Some(raft_log_id(3, 1, 2)),
            entries: vec![
                bootstrap_membership_entry(1),
                blank_entry(3, 1, 1),
                blank_entry(3, 1, 2),
            ],
            ..Default::default()
        };
        let err = ControlPlaneRaftLogStore::from_restart_artifact(artifact_with_missing_vote)
            .unwrap_err();
        assert!(err.to_string().contains("missing vote state"));

        let artifact_with_stale_vote = ControlPlaneRaftLogStoreRestartArtifact {
            vote: Some(Vote::<ControlPlaneRaftLeaderId>::new(2, 99)),
            committed: Some(raft_log_id(3, 1, 2)),
            entries: vec![
                bootstrap_membership_entry(1),
                blank_entry(3, 1, 1),
                blank_entry(3, 1, 2),
            ],
            ..Default::default()
        };
        let err =
            ControlPlaneRaftLogStore::from_restart_artifact(artifact_with_stale_vote).unwrap_err();
        assert!(err.to_string().contains("does not cover"));

        let artifact_with_same_term_lower_node_vote = ControlPlaneRaftLogStoreRestartArtifact {
            vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 0)),
            committed: Some(raft_log_id(3, 1, 2)),
            entries: vec![
                bootstrap_membership_entry(1),
                blank_entry(3, 1, 1),
                blank_entry(3, 1, 2),
            ],
            ..Default::default()
        };
        let err = ControlPlaneRaftLogStore::from_restart_artifact(
            artifact_with_same_term_lower_node_vote,
        )
        .unwrap_err();
        assert!(err.to_string().contains("does not cover"));
    }
}
