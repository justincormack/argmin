use crate::config::ServerConfig;
use ec::EcConfig;
use placement::{
    ClusterMap, Level, NodeId, NodeInfo, PlacementConfig, PlacementConstraint, Placer, TopologyKey,
};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::OpenOptions;
use std::io::Read;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use storage::control_plane_raft::{
    ControlPlaneRaftPeerTransportLimits, ControlPlaneRaftPeerTransportPolicy,
};

const CLUSTER_MANIFEST_MAX_BYTES: u64 = 4 * 1024 * 1024;
const CLUSTER_MANIFEST_MAX_COLLECTION_ITEMS: usize = 4_096;
const CLUSTER_MANIFEST_MAX_ID_BYTES: usize = 128;
const CLUSTER_MANIFEST_MAX_CLUSTER_ID_BYTES: usize = 256;
const CLUSTER_MANIFEST_MAX_REGION_BYTES: usize = 128;
const CLUSTER_MANIFEST_MAX_URI_BYTES: usize = 4_096;
const CLUSTER_MANIFEST_MAX_PATH_BYTES: usize = 4_096;
const CLUSTER_MANIFEST_MAX_FRAME_BYTES: u64 = 64 * 1024 * 1024;
const CLUSTER_MANIFEST_MAX_CONNECTIONS: u32 = 65_536;
const CLUSTER_MANIFEST_MAX_TIMEOUT_MS: u64 = 60_000;
const CLUSTER_MANIFEST_MAX_RAFT_FRAME_BYTES: u64 = 16 * 1024 * 1024;
const CLUSTER_MANIFEST_MAX_SNAPSHOT_BYTES: u64 = 16 * 1024 * 1024;
const CLUSTER_MANIFEST_RAFT_APPEND_FIXED_FRAME_OVERHEAD_BYTES: u64 = 64 * 1024;
const CLUSTER_MANIFEST_RAFT_SNAPSHOT_FIXED_FRAME_OVERHEAD_BYTES: u64 = 64 * 1024;
const INITIAL_PG_PLACEMENT_KEY_DOMAIN: &[u8] = b"argmin-initial-pg-placement-v1";
const TOPOLOGY_IDENTITY_DOMAIN: &str = "argmin-static-cluster-topology-v1";
const PROCESS_IDENTITY_DOMAIN: &str = "argmin-static-cluster-process-identity-v1";
const FULL_CONFIG_FINGERPRINT_DOMAIN: &str = "argmin-static-cluster-full-config-v1";
const LEGACY_CLUSTER_ENV_KEYS: &[&str] = &[
    "ARGMIN_PROCESS_ROLE",
    "ARGMIN_HOST_ID",
    "ARGMIN_DATA_DIR",
    "ARGMIN_PG_COUNT",
    "ARGMIN_STORAGE_CLUSTER_EPOCH",
    "ARGMIN_STORAGE_PG_IDS",
    "ARGMIN_EC_K",
    "ARGMIN_EC_M",
    "ARGMIN_LOCAL_NODE_COUNT",
    "ARGMIN_STORAGE_NODE_ID",
    "ARGMIN_STORAGE_NODE_DATA_DIR",
    "ARGMIN_STORAGE_NODE_SOCKET_PATH",
    "ARGMIN_STORAGE_NODE_SOCKETS",
    "ARGMIN_CONTROL_PLANE_STATE_PATH",
    "ARGMIN_CONTROL_PLANE_SOCKET_PATH",
    "ARGMIN_CONTROL_PLANE_CLIENT_SOCKET_PATHS",
    "ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID",
    "ARGMIN_CONTROL_PLANE_STORAGE_AUTH_CREDENTIALS",
    "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_INSTANCE_ID",
    "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS",
    "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_INSTANCE_ID",
    "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS",
    "ARGMIN_CONTROL_PLANE_EXPERIMENTAL_RAFT",
    "ARGMIN_CONTROL_PLANE_RAFT_CLUSTER_NAME",
    "ARGMIN_CONTROL_PLANE_RAFT_NODE_ID",
    "ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKET_PATH",
    "ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKETS",
    "ARGMIN_CONTROL_PLANE_RAFT_AUTH_CREDENTIALS",
    "ARGMIN_REGION",
];

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
enum DeploymentMode {
    Standalone,
    Replicated,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
enum FailureDomain {
    None,
    Disk,
    Host,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
enum InternalAuth {
    Required,
    Disabled,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
enum ProcessKind {
    AllInOne,
    Frontend,
    StorageNode,
    Combined,
    ControlPlane,
}

impl ProcessKind {
    fn has_frontend(self) -> bool {
        matches!(self, Self::AllInOne | Self::Frontend | Self::Combined)
    }

    fn has_storage_node(self) -> bool {
        matches!(self, Self::AllInOne | Self::StorageNode | Self::Combined)
    }

    fn has_control_plane(self) -> bool {
        matches!(self, Self::AllInOne | Self::ControlPlane)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
enum AuthorityKind {
    Single,
    RaftVoter,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(rename_all = "kebab-case")]
enum EndpointProtocol {
    RaftPeer,
    ControlPlane,
    AuthorityClockRecovery,
    StorageRpc,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(rename_all = "kebab-case")]
enum AuthPrincipal {
    RaftPeer,
    StorageNode,
    Frontend,
    Admin,
    Maintenance,
}

#[derive(Clone, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct StaticClusterManifestInput {
    schema_version: u32,
    cluster: ClusterInput,
    deployment: DeploymentInput,
    storage: StorageInput,
    raft: RaftInput,
    transport_profiles: Vec<TransportProfileInput>,
    hosts: Vec<HostInput>,
    disks: Vec<DiskInput>,
    processes: Vec<ProcessInput>,
    authorities: Vec<AuthorityInput>,
    storage_nodes: Vec<StorageNodeInput>,
    endpoints: Vec<EndpointInput>,
    tls_identities: Vec<TlsIdentityInput>,
    tls_trust_bundles: Vec<TlsTrustBundleInput>,
    auth_credentials: Vec<AuthCredentialInput>,
}

#[derive(Clone, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct ClusterInput {
    id: String,
    topology_generation: u64,
    region: String,
}

#[derive(Clone, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct DeploymentInput {
    mode: DeploymentMode,
    failure_domain: FailureDomain,
    failure_tolerance: u8,
    internal_auth: InternalAuth,
}

#[derive(Clone, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct StorageInput {
    pg_count: u32,
    ec_data_shards: u8,
    ec_parity_shards: u8,
    initial_cluster_epoch: u64,
}

#[derive(Clone, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct RaftInput {
    max_append_entries: u64,
    max_append_bytes: u64,
    max_snapshot_bytes: u64,
}

#[derive(Clone, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct TransportProfileInput {
    id: String,
    max_frame_bytes: u64,
    max_connections: u32,
    connect_timeout_ms: u64,
    io_timeout_ms: u64,
}

#[derive(Clone, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct HostInput {
    id: String,
    zone: String,
    rack: String,
}

#[derive(Clone, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct DiskInput {
    id: String,
    host_id: String,
    mount_path: PathBuf,
}

#[derive(Clone, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct ProcessInput {
    id: String,
    host_id: String,
    kind: ProcessKind,
    frontend_instance_id: Option<String>,
    admin_instance_id: Option<String>,
    maintenance_instance_id: Option<String>,
}

#[derive(Clone, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct AuthorityInput {
    id: String,
    kind: AuthorityKind,
    raft_node_id: Option<u64>,
    process_id: String,
    disk_id: String,
    state_path: PathBuf,
}

#[derive(Clone, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct StorageNodeInput {
    node_id: u32,
    process_id: String,
    disk_id: String,
    data_dir: PathBuf,
}

#[derive(Clone, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct EndpointInput {
    id: String,
    owner_process_id: String,
    protocol: EndpointProtocol,
    priority: u32,
    listen: String,
    advertise: String,
    transport_profile_id: String,
    tls_identity_id: Option<String>,
    tls_trust_bundle_id: Option<String>,
    tls_server_name: Option<String>,
}

#[derive(Clone, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct TlsIdentityInput {
    id: String,
    certificate_ref: String,
    private_key_ref: String,
}

#[derive(Clone, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct TlsTrustBundleInput {
    id: String,
    ca_bundle_ref: String,
}

#[derive(Clone, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct AuthCredentialInput {
    principal: AuthPrincipal,
    node_id: Option<u64>,
    instance_id: Option<String>,
    credential_id: String,
    credential_version: u64,
    use_for_signing: bool,
    accept_from_ms: u64,
    accept_until_ms: Option<u64>,
    secret_ref: String,
}

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct ValidatedStaticClusterManifest {
    manifest: StaticClusterManifestInput,
    selected_process_index: usize,
    initial_pg_acting_sets: Vec<Vec<u32>>,
    canonical_raft_peer_endpoints: BTreeMap<u64, String>,
    topology_digest: String,
    process_identity_digest: String,
    full_config_fingerprint: String,
}

impl ValidatedStaticClusterManifest {
    pub(crate) fn cluster_id(&self) -> &str {
        &self.manifest.cluster.id
    }

    pub(crate) fn topology_generation(&self) -> u64 {
        self.manifest.cluster.topology_generation
    }

    pub(crate) fn selected_process_id(&self) -> &str {
        &self.manifest.processes[self.selected_process_index].id
    }

    pub(crate) fn deployment_mode(&self) -> &'static str {
        match self.manifest.deployment.mode {
            DeploymentMode::Standalone => "standalone",
            DeploymentMode::Replicated => "replicated",
        }
    }

    pub(crate) fn topology_digest(&self) -> &str {
        &self.topology_digest
    }

    pub(crate) fn process_identity_digest(&self) -> &str {
        &self.process_identity_digest
    }

    pub(crate) fn full_config_fingerprint(&self) -> &str {
        &self.full_config_fingerprint
    }

    fn standalone_legacy_server_config<F>(&self, get: F) -> Result<ServerConfig, String>
    where
        F: Fn(&str) -> Option<String>,
    {
        if self.manifest.deployment.mode != DeploymentMode::Standalone {
            return Err(
                "replicated cluster manifests require the static secret and transport runtime slices"
                    .to_string(),
            );
        }
        if self.manifest.deployment.internal_auth != InternalAuth::Disabled
            || !self.manifest.auth_credentials.is_empty()
            || !self.manifest.tls_identities.is_empty()
            || !self.manifest.tls_trust_bundles.is_empty()
            || self
                .manifest
                .endpoints
                .iter()
                .any(|endpoint| !endpoint.advertise.starts_with("unix://"))
        {
            return Err(
                "standalone manifest runtime mapping currently requires Unix endpoints with internal auth disabled and no unresolved secret references"
                    .to_string(),
            );
        }

        let selected = &self.manifest.processes[self.selected_process_index];
        if selected.kind != ProcessKind::AllInOne {
            return Err(
                "standalone manifest runtime mapping requires an all-in-one process".to_string(),
            );
        }
        let storage_node = self
            .manifest
            .storage_nodes
            .iter()
            .find(|storage_node| storage_node.process_id == selected.id)
            .ok_or_else(|| "all-in-one process has no storage node".to_string())?;
        let disk = self
            .manifest
            .disks
            .iter()
            .find(|disk| disk.id == storage_node.disk_id)
            .ok_or_else(|| "storage node disk disappeared after validation".to_string())?;
        let mut manifest_values = BTreeMap::<&'static str, String>::new();
        manifest_values.insert("ARGMIN_PROCESS_ROLE", "legacy-local".to_string());
        manifest_values.insert(
            "ARGMIN_DATA_DIR",
            disk.mount_path.to_string_lossy().into_owned(),
        );
        manifest_values.insert(
            "ARGMIN_PG_COUNT",
            self.manifest.storage.pg_count.to_string(),
        );
        manifest_values.insert(
            "ARGMIN_STORAGE_CLUSTER_EPOCH",
            self.manifest.storage.initial_cluster_epoch.to_string(),
        );
        manifest_values.insert(
            "ARGMIN_EC_K",
            self.manifest.storage.ec_data_shards.to_string(),
        );
        manifest_values.insert(
            "ARGMIN_EC_M",
            self.manifest.storage.ec_parity_shards.to_string(),
        );
        manifest_values.insert("ARGMIN_LOCAL_NODE_COUNT", "1".to_string());
        manifest_values.insert("ARGMIN_REGION", self.manifest.cluster.region.clone());
        manifest_values.insert("ARGMIN_HOST_ID", selected.host_id.clone());

        let mut config = ServerConfig::from_lookup(|key| {
            manifest_values.get(key).cloned().or_else(|| get(key))
        })?;
        config.storage_node_ids = vec![storage_node.node_id];
        config.storage_node_id = Some(storage_node.node_id);
        config.storage_node_data_dir = Some(storage_node.data_dir.to_string_lossy().into_owned());
        Ok(config)
    }

    #[cfg(test)]
    fn initial_pg_acting_sets(&self) -> &[Vec<u32>] {
        &self.initial_pg_acting_sets
    }

    #[cfg(test)]
    fn canonical_raft_peer_endpoints(&self) -> &BTreeMap<u64, String> {
        &self.canonical_raft_peer_endpoints
    }
}

pub(crate) fn load_server_config_from_environment() -> Result<ServerConfig, String> {
    let config_path = std::env::var_os("ARGMIN_CLUSTER_CONFIG_PATH");
    let process_id = std::env::var_os("ARGMIN_PROCESS_ID");
    let config_path = config_path.as_deref().map(Path::new);
    let process_id = process_id
        .as_deref()
        .map(|value| {
            value
                .to_str()
                .ok_or_else(|| "ARGMIN_PROCESS_ID must contain valid UTF-8".to_string())
        })
        .transpose()?;
    load_server_config_from_inputs(config_path, process_id, |key| std::env::var(key).ok())
}

fn load_server_config_from_inputs<F>(
    config_path: Option<&Path>,
    process_id: Option<&str>,
    get: F,
) -> Result<ServerConfig, String>
where
    F: Fn(&str) -> Option<String>,
{
    load_server_config_from_inputs_with_filesystem_validator(
        config_path,
        process_id,
        get,
        validate_selected_host_filesystem,
    )
}

fn load_server_config_from_inputs_with_filesystem_validator<F, V>(
    config_path: Option<&Path>,
    process_id: Option<&str>,
    get: F,
    validate_filesystem: V,
) -> Result<ServerConfig, String>
where
    F: Fn(&str) -> Option<String>,
    V: FnOnce(&ValidatedStaticClusterManifest) -> Result<(), String>,
{
    match (config_path, process_id) {
        (None, None) => ServerConfig::from_lookup(get),
        (Some(_), None) => {
            Err("ARGMIN_PROCESS_ID is required with ARGMIN_CLUSTER_CONFIG_PATH".to_string())
        }
        (None, Some(_)) => {
            Err("ARGMIN_CLUSTER_CONFIG_PATH is required with ARGMIN_PROCESS_ID".to_string())
        }
        (Some(config_path), Some(process_id)) => {
            for key in LEGACY_CLUSTER_ENV_KEYS {
                if get(key).is_some() {
                    return Err(format!(
                        "{key} cannot be set when ARGMIN_CLUSTER_CONFIG_PATH is active"
                    ));
                }
            }
            let manifest = load_static_cluster_manifest_structural(config_path, process_id)?;
            let config = manifest.standalone_legacy_server_config(get)?;
            validate_filesystem(&manifest)?;
            Ok(config)
        }
    }
}

impl fmt::Debug for ValidatedStaticClusterManifest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ValidatedStaticClusterManifest")
            .field("schema_version", &self.manifest.schema_version)
            .field("cluster_id", &self.manifest.cluster.id)
            .field(
                "topology_generation",
                &self.manifest.cluster.topology_generation,
            )
            .field("deployment_mode", &self.manifest.deployment.mode)
            .field("selected_process_id", &self.selected_process_id())
            .field("hosts", &self.manifest.hosts.len())
            .field("disks", &self.manifest.disks.len())
            .field("processes", &self.manifest.processes.len())
            .field("authorities", &self.manifest.authorities.len())
            .field("storage_nodes", &self.manifest.storage_nodes.len())
            .field("endpoints", &self.manifest.endpoints.len())
            .field("tls_identities", &self.manifest.tls_identities.len())
            .field("tls_trust_bundles", &self.manifest.tls_trust_bundles.len())
            .field("auth_credentials", &self.manifest.auth_credentials.len())
            .field("initial_pg_acting_sets", &self.initial_pg_acting_sets.len())
            .field(
                "canonical_raft_peer_endpoints",
                &self.canonical_raft_peer_endpoints.len(),
            )
            .field("topology_digest", &self.topology_digest)
            .field("process_identity_digest", &self.process_identity_digest)
            .field("full_config_fingerprint", &self.full_config_fingerprint)
            .finish()
    }
}

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
enum CredentialPrincipalId {
    Node(u64),
    Instance(String),
}

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
struct CredentialPrincipalKey {
    principal: AuthPrincipal,
    id: CredentialPrincipalId,
}

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
struct CredentialIdentity {
    principal: CredentialPrincipalKey,
    credential_id: String,
    credential_version: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum EndpointAddress {
    Unix(PathBuf),
    Tcp { host: String, port: u16 },
}

#[derive(Default)]
struct CanonicalEncoder {
    bytes: Vec<u8>,
}

impl CanonicalEncoder {
    fn field(&mut self, tag: u16, value: &[u8]) {
        self.bytes.extend_from_slice(&tag.to_be_bytes());
        self.bytes.extend_from_slice(
            &u64::try_from(value.len())
                .expect("validated manifest field length fits u64")
                .to_be_bytes(),
        );
        self.bytes.extend_from_slice(value);
    }

    fn string(&mut self, tag: u16, value: &str) {
        self.field(tag, value.as_bytes());
    }

    fn optional_string(&mut self, tag: u16, value: Option<&str>) {
        let mut encoded = Vec::new();
        match value {
            Some(value) => {
                encoded.push(1);
                encoded.extend_from_slice(
                    &u64::try_from(value.len())
                        .expect("validated manifest field length fits u64")
                        .to_be_bytes(),
                );
                encoded.extend_from_slice(value.as_bytes());
            }
            None => encoded.push(0),
        }
        self.field(tag, &encoded);
    }

    fn path(&mut self, tag: u16, value: &Path) {
        self.field(tag, value.as_os_str().as_encoded_bytes());
    }

    fn u8(&mut self, tag: u16, value: u8) {
        self.field(tag, &[value]);
    }

    fn u32(&mut self, tag: u16, value: u32) {
        self.field(tag, &value.to_be_bytes());
    }

    fn u64(&mut self, tag: u16, value: u64) {
        self.field(tag, &value.to_be_bytes());
    }

    fn optional_u64(&mut self, tag: u16, value: Option<u64>) {
        let mut encoded = Vec::with_capacity(9);
        match value {
            Some(value) => {
                encoded.push(1);
                encoded.extend_from_slice(&value.to_be_bytes());
            }
            None => encoded.push(0),
        }
        self.field(tag, &encoded);
    }

    fn boolean(&mut self, tag: u16, value: bool) {
        self.u8(tag, u8::from(value));
    }

    fn collection<I>(&mut self, tag: u16, values: I)
    where
        I: IntoIterator<Item = Vec<u8>>,
    {
        let values: Vec<Vec<u8>> = values.into_iter().collect();
        let mut encoded = Vec::new();
        encoded.extend_from_slice(
            &u32::try_from(values.len())
                .expect("validated manifest collection length fits u32")
                .to_be_bytes(),
        );
        for value in values {
            encoded.extend_from_slice(&1_u16.to_be_bytes());
            encoded.extend_from_slice(
                &u64::try_from(value.len())
                    .expect("validated manifest item length fits u64")
                    .to_be_bytes(),
            );
            encoded.extend_from_slice(&value);
        }
        self.field(tag, &encoded);
    }

    fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

fn topology_digest(
    manifest: &StaticClusterManifestInput,
    initial_pg_acting_sets: &[Vec<u32>],
    canonical_raft_peer_endpoints: &BTreeMap<u64, String>,
) -> String {
    let mut encoder = CanonicalEncoder::default();
    encoder.string(1, TOPOLOGY_IDENTITY_DOMAIN);
    encoder.u32(2, manifest.schema_version);
    encoder.string(3, &manifest.cluster.id);
    encoder.u64(4, manifest.cluster.topology_generation);

    encoder.field(5, &encode_deployment_topology(&manifest.deployment));
    encoder.field(6, &encode_storage_topology(&manifest.storage));
    encoder.field(7, &encode_raft_topology(&manifest.raft));
    encoder.collection(
        8,
        manifest
            .transport_profiles
            .iter()
            .map(encode_transport_profile_topology),
    );
    encoder.collection(9, manifest.hosts.iter().map(encode_host_topology));
    encoder.collection(10, manifest.disks.iter().map(encode_disk_topology));
    encoder.collection(11, manifest.processes.iter().map(encode_process_topology));
    encoder.collection(
        12,
        manifest.authorities.iter().map(encode_authority_topology),
    );
    encoder.collection(
        13,
        manifest
            .storage_nodes
            .iter()
            .map(encode_storage_node_topology),
    );
    encoder.collection(14, manifest.endpoints.iter().map(encode_endpoint_topology));
    encoder.collection(
        15,
        required_principal_identities(manifest)
            .iter()
            .map(encode_principal_identity),
    );
    encoder.collection(
        16,
        initial_pg_acting_sets
            .iter()
            .enumerate()
            .map(|(pg_id, acting_set)| encode_initial_pg_acting_set(pg_id, acting_set)),
    );
    encoder.collection(
        17,
        canonical_raft_peer_endpoints
            .iter()
            .map(|(node_id, endpoint)| encode_raft_peer_endpoint(*node_id, endpoint)),
    );
    auth::canonical::sha256_hex(&encoder.finish())
}

fn process_identity_digest(
    manifest: &StaticClusterManifestInput,
    selected_process_index: usize,
    topology_digest: &str,
) -> String {
    let selected = &manifest.processes[selected_process_index];
    let mut encoder = CanonicalEncoder::default();
    encoder.string(1, PROCESS_IDENTITY_DOMAIN);
    encoder.string(2, topology_digest);
    encoder.string(3, &selected.id);
    encoder.string(4, &selected.host_id);
    encoder.u8(5, process_kind_tag(selected.kind));
    encoder.collection(
        6,
        manifest
            .authorities
            .iter()
            .filter(|authority| authority.process_id == selected.id)
            .map(encode_authority_process_identity),
    );
    encoder.collection(
        7,
        manifest
            .storage_nodes
            .iter()
            .filter(|storage_node| storage_node.process_id == selected.id)
            .map(encode_storage_node_process_identity),
    );
    auth::canonical::sha256_hex(&encoder.finish())
}

fn full_config_fingerprint(manifest: &StaticClusterManifestInput) -> String {
    let mut encoder = CanonicalEncoder::default();
    encoder.string(1, FULL_CONFIG_FINGERPRINT_DOMAIN);
    encoder.u32(2, manifest.schema_version);
    encoder.field(3, &encode_cluster_full(&manifest.cluster));
    encoder.field(4, &encode_deployment_full(&manifest.deployment));
    encoder.field(5, &encode_storage_topology(&manifest.storage));
    encoder.field(6, &encode_raft_topology(&manifest.raft));
    encoder.collection(
        7,
        manifest
            .transport_profiles
            .iter()
            .map(encode_transport_profile_full),
    );
    encoder.collection(8, manifest.hosts.iter().map(encode_host_topology));
    encoder.collection(9, manifest.disks.iter().map(encode_disk_full));
    encoder.collection(10, manifest.processes.iter().map(encode_process_full));
    encoder.collection(11, manifest.authorities.iter().map(encode_authority_full));
    encoder.collection(
        12,
        manifest.storage_nodes.iter().map(encode_storage_node_full),
    );
    encoder.collection(13, manifest.endpoints.iter().map(encode_endpoint_full));
    encoder.collection(
        14,
        manifest.tls_identities.iter().map(encode_tls_identity_full),
    );
    encoder.collection(
        15,
        manifest
            .tls_trust_bundles
            .iter()
            .map(encode_tls_trust_bundle_full),
    );
    encoder.collection(
        16,
        manifest
            .auth_credentials
            .iter()
            .map(encode_auth_credential_full),
    );
    auth::canonical::sha256_hex(&encoder.finish())
}

fn encode_deployment_topology(deployment: &DeploymentInput) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.u8(1, deployment_mode_tag(deployment.mode));
    encoder.u8(2, failure_domain_tag(deployment.failure_domain));
    encoder.u8(3, deployment.failure_tolerance);
    encoder.u8(4, internal_auth_tag(deployment.internal_auth));
    encoder.finish()
}

fn encode_deployment_full(deployment: &DeploymentInput) -> Vec<u8> {
    encode_deployment_topology(deployment)
}

fn encode_cluster_full(cluster: &ClusterInput) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.string(1, &cluster.id);
    encoder.u64(2, cluster.topology_generation);
    encoder.string(3, &cluster.region);
    encoder.finish()
}

fn encode_storage_topology(storage: &StorageInput) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.u32(1, storage.pg_count);
    encoder.u8(2, storage.ec_data_shards);
    encoder.u8(3, storage.ec_parity_shards);
    encoder.u64(4, storage.initial_cluster_epoch);
    encoder.finish()
}

fn encode_raft_topology(raft: &RaftInput) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.u64(1, raft.max_append_entries);
    encoder.u64(2, raft.max_append_bytes);
    encoder.u64(3, raft.max_snapshot_bytes);
    encoder.finish()
}

fn encode_transport_profile_topology(profile: &TransportProfileInput) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.string(1, &profile.id);
    encoder.u64(2, profile.max_frame_bytes);
    encoder.finish()
}

fn encode_transport_profile_full(profile: &TransportProfileInput) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.string(1, &profile.id);
    encoder.u64(2, profile.max_frame_bytes);
    encoder.u32(3, profile.max_connections);
    encoder.u64(4, profile.connect_timeout_ms);
    encoder.u64(5, profile.io_timeout_ms);
    encoder.finish()
}

fn encode_host_topology(host: &HostInput) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.string(1, &host.id);
    encoder.string(2, &host.zone);
    encoder.string(3, &host.rack);
    encoder.finish()
}

fn encode_disk_topology(disk: &DiskInput) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.string(1, &disk.id);
    encoder.string(2, &disk.host_id);
    encoder.finish()
}

fn encode_disk_full(disk: &DiskInput) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.string(1, &disk.id);
    encoder.string(2, &disk.host_id);
    encoder.path(3, &disk.mount_path);
    encoder.finish()
}

fn encode_process_topology(process: &ProcessInput) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.string(1, &process.id);
    encoder.string(2, &process.host_id);
    encoder.u8(3, process_kind_tag(process.kind));
    encoder.optional_string(4, process.frontend_instance_id.as_deref());
    encoder.optional_string(5, process.admin_instance_id.as_deref());
    encoder.optional_string(6, process.maintenance_instance_id.as_deref());
    encoder.finish()
}

fn encode_process_full(process: &ProcessInput) -> Vec<u8> {
    encode_process_topology(process)
}

fn encode_authority_topology(authority: &AuthorityInput) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.string(1, &authority.id);
    encoder.u8(2, authority_kind_tag(authority.kind));
    encoder.optional_u64(3, authority.raft_node_id);
    encoder.string(4, &authority.process_id);
    encoder.string(5, &authority.disk_id);
    encoder.finish()
}

fn encode_authority_process_identity(authority: &AuthorityInput) -> Vec<u8> {
    encode_authority_topology(authority)
}

fn encode_authority_full(authority: &AuthorityInput) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.string(1, &authority.id);
    encoder.u8(2, authority_kind_tag(authority.kind));
    encoder.optional_u64(3, authority.raft_node_id);
    encoder.string(4, &authority.process_id);
    encoder.string(5, &authority.disk_id);
    encoder.path(6, &authority.state_path);
    encoder.finish()
}

fn encode_storage_node_topology(storage_node: &StorageNodeInput) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.u32(1, storage_node.node_id);
    encoder.string(2, &storage_node.process_id);
    encoder.string(3, &storage_node.disk_id);
    encoder.finish()
}

fn encode_storage_node_process_identity(storage_node: &StorageNodeInput) -> Vec<u8> {
    encode_storage_node_topology(storage_node)
}

fn encode_storage_node_full(storage_node: &StorageNodeInput) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.u32(1, storage_node.node_id);
    encoder.string(2, &storage_node.process_id);
    encoder.string(3, &storage_node.disk_id);
    encoder.path(4, &storage_node.data_dir);
    encoder.finish()
}

fn encode_endpoint_topology(endpoint: &EndpointInput) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.string(1, &endpoint.id);
    encoder.string(2, &endpoint.owner_process_id);
    encoder.u8(3, endpoint_protocol_tag(endpoint.protocol));
    encoder.u32(4, endpoint.priority);
    encoder.string(5, &endpoint.advertise);
    encoder.string(6, &endpoint.transport_profile_id);
    encoder.optional_string(7, endpoint.tls_identity_id.as_deref());
    encoder.optional_string(8, endpoint.tls_trust_bundle_id.as_deref());
    encoder.optional_string(9, endpoint.tls_server_name.as_deref());
    encoder.finish()
}

fn encode_endpoint_full(endpoint: &EndpointInput) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.string(1, &endpoint.id);
    encoder.string(2, &endpoint.owner_process_id);
    encoder.u8(3, endpoint_protocol_tag(endpoint.protocol));
    encoder.u32(4, endpoint.priority);
    encoder.string(5, &endpoint.listen);
    encoder.string(6, &endpoint.advertise);
    encoder.string(7, &endpoint.transport_profile_id);
    encoder.optional_string(8, endpoint.tls_identity_id.as_deref());
    encoder.optional_string(9, endpoint.tls_trust_bundle_id.as_deref());
    encoder.optional_string(10, endpoint.tls_server_name.as_deref());
    encoder.finish()
}

fn encode_tls_identity_full(identity: &TlsIdentityInput) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.string(1, &identity.id);
    encoder.string(2, &identity.certificate_ref);
    encoder.string(3, &identity.private_key_ref);
    encoder.finish()
}

fn encode_tls_trust_bundle_full(bundle: &TlsTrustBundleInput) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.string(1, &bundle.id);
    encoder.string(2, &bundle.ca_bundle_ref);
    encoder.finish()
}

fn encode_auth_credential_full(credential: &AuthCredentialInput) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.u8(1, auth_principal_tag(credential.principal));
    encoder.optional_u64(2, credential.node_id);
    encoder.optional_string(3, credential.instance_id.as_deref());
    encoder.string(4, &credential.credential_id);
    encoder.u64(5, credential.credential_version);
    encoder.boolean(6, credential.use_for_signing);
    encoder.u64(7, credential.accept_from_ms);
    encoder.optional_u64(8, credential.accept_until_ms);
    encoder.string(9, &credential.secret_ref);
    encoder.finish()
}

fn encode_principal_identity(principal: &CredentialPrincipalKey) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.u8(1, auth_principal_tag(principal.principal));
    match &principal.id {
        CredentialPrincipalId::Node(node_id) => {
            encoder.u8(2, 1);
            encoder.u64(3, *node_id);
        }
        CredentialPrincipalId::Instance(instance_id) => {
            encoder.u8(2, 2);
            encoder.string(3, instance_id);
        }
    }
    encoder.finish()
}

fn encode_initial_pg_acting_set(pg_id: usize, acting_set: &[u32]) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.u64(
        1,
        u64::try_from(pg_id).expect("validated PG index fits in u64"),
    );
    encoder.collection(
        2,
        acting_set
            .iter()
            .map(|node_id| node_id.to_be_bytes().to_vec()),
    );
    encoder.finish()
}

fn encode_raft_peer_endpoint(node_id: u64, endpoint: &str) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.u64(1, node_id);
    encoder.string(2, endpoint);
    encoder.finish()
}

fn required_principal_identities(
    manifest: &StaticClusterManifestInput,
) -> BTreeSet<CredentialPrincipalKey> {
    let raft_nodes = manifest
        .authorities
        .iter()
        .filter_map(|authority| authority.raft_node_id)
        .collect();
    let storage_nodes = manifest
        .storage_nodes
        .iter()
        .map(|storage_node| u64::from(storage_node.node_id))
        .collect();
    let frontend_ids = manifest
        .processes
        .iter()
        .filter_map(|process| process.frontend_instance_id.as_deref())
        .collect();
    let admin_ids = manifest
        .processes
        .iter()
        .filter_map(|process| process.admin_instance_id.as_deref())
        .collect();
    let maintenance_ids = manifest
        .processes
        .iter()
        .filter_map(|process| process.maintenance_instance_id.as_deref())
        .collect();
    required_auth_principals(
        &raft_nodes,
        &storage_nodes,
        &frontend_ids,
        &admin_ids,
        &maintenance_ids,
    )
}

const fn deployment_mode_tag(value: DeploymentMode) -> u8 {
    match value {
        DeploymentMode::Standalone => 1,
        DeploymentMode::Replicated => 2,
    }
}

const fn failure_domain_tag(value: FailureDomain) -> u8 {
    match value {
        FailureDomain::None => 1,
        FailureDomain::Disk => 2,
        FailureDomain::Host => 3,
    }
}

const fn internal_auth_tag(value: InternalAuth) -> u8 {
    match value {
        InternalAuth::Required => 1,
        InternalAuth::Disabled => 2,
    }
}

const fn process_kind_tag(value: ProcessKind) -> u8 {
    match value {
        ProcessKind::AllInOne => 1,
        ProcessKind::Frontend => 2,
        ProcessKind::StorageNode => 3,
        ProcessKind::Combined => 4,
        ProcessKind::ControlPlane => 5,
    }
}

const fn authority_kind_tag(value: AuthorityKind) -> u8 {
    match value {
        AuthorityKind::Single => 1,
        AuthorityKind::RaftVoter => 2,
    }
}

const fn endpoint_protocol_tag(value: EndpointProtocol) -> u8 {
    match value {
        EndpointProtocol::RaftPeer => 1,
        EndpointProtocol::ControlPlane => 2,
        EndpointProtocol::AuthorityClockRecovery => 3,
        EndpointProtocol::StorageRpc => 4,
    }
}

const fn auth_principal_tag(value: AuthPrincipal) -> u8 {
    match value {
        AuthPrincipal::RaftPeer => 1,
        AuthPrincipal::StorageNode => 2,
        AuthPrincipal::Frontend => 3,
        AuthPrincipal::Admin => 4,
        AuthPrincipal::Maintenance => 5,
    }
}

pub(crate) fn load_static_cluster_manifest(
    path: &Path,
    process_id: &str,
) -> Result<ValidatedStaticClusterManifest, String> {
    let manifest = load_static_cluster_manifest_structural(path, process_id)?;
    validate_selected_host_filesystem(&manifest)?;
    Ok(manifest)
}

fn load_static_cluster_manifest_structural(
    path: &Path,
    process_id: &str,
) -> Result<ValidatedStaticClusterManifest, String> {
    if !path.is_absolute() {
        return Err("cluster manifest path must be absolute".to_string());
    }
    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let file = options
        .open(path)
        .map_err(|error| format!("open no-follow cluster manifest: {error}"))?;
    let metadata = file
        .metadata()
        .map_err(|error| format!("read open cluster manifest metadata: {error}"))?;
    if !metadata.is_file() {
        return Err("cluster manifest must be a regular file".to_string());
    }
    if metadata.len() > CLUSTER_MANIFEST_MAX_BYTES {
        return Err(format!(
            "cluster manifest exceeds {} bytes",
            CLUSTER_MANIFEST_MAX_BYTES
        ));
    }

    let bounded_capacity = usize::try_from(metadata.len())
        .unwrap_or(usize::MAX)
        .min(CLUSTER_MANIFEST_MAX_BYTES as usize);
    let mut bytes = Vec::with_capacity(bounded_capacity);
    file.take(CLUSTER_MANIFEST_MAX_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("read cluster manifest: {error}"))?;
    if bytes.len() > CLUSTER_MANIFEST_MAX_BYTES as usize {
        return Err(format!(
            "cluster manifest exceeds {} bytes",
            CLUSTER_MANIFEST_MAX_BYTES
        ));
    }
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| "cluster manifest must contain valid UTF-8".to_string())?;
    parse_static_cluster_manifest(text, process_id)
}

fn validate_selected_host_filesystem(
    validated: &ValidatedStaticClusterManifest,
) -> Result<(), String> {
    validate_selected_host_filesystem_with_mount_validator(
        validated,
        validate_selected_host_mount_boundary,
    )
}

fn validate_selected_host_filesystem_with_mount_validator<V>(
    validated: &ValidatedStaticClusterManifest,
    mut validate_mount: V,
) -> Result<(), String>
where
    V: FnMut(&Path, &std::fs::Metadata, &str) -> Result<(), String>,
{
    let selected_process = &validated.manifest.processes[validated.selected_process_index];
    let selected_host_id = selected_process.host_id.as_str();
    let effective_uid = {
        // SAFETY: geteuid has no preconditions and does not mutate memory.
        unsafe { libc::geteuid() }
    };
    let local_disks: BTreeMap<&str, (PathBuf, PathBuf, u64)> = validated
        .manifest
        .disks
        .iter()
        .filter(|disk| disk.host_id == selected_host_id)
        .map(|disk| {
            let normalized_mount = normalize_absolute_path(&disk.mount_path, "disk mount path")?;
            let metadata = std::fs::symlink_metadata(&normalized_mount).map_err(|error| {
                format!(
                    "selected-host disk {} mount path cannot be inspected: {error}",
                    disk.id
                )
            })?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(format!(
                    "selected-host disk {} mount path must be a non-symlink directory",
                    disk.id
                ));
            }
            validate_selected_host_path_permissions(
                &metadata,
                effective_uid,
                &format!("selected-host disk {} mount path", disk.id),
            )?;
            validate_mount(
                &normalized_mount,
                &metadata,
                &format!("selected-host disk {} mount path", disk.id),
            )?;
            let canonical_mount = normalized_mount.canonicalize().map_err(|error| {
                format!(
                    "selected-host disk {} mount path cannot be canonicalized: {error}",
                    disk.id
                )
            })?;
            Ok((
                disk.id.as_str(),
                (normalized_mount, canonical_mount, metadata.dev()),
            ))
        })
        .collect::<Result<_, String>>()?;

    for authority in &validated.manifest.authorities {
        let process = validated
            .manifest
            .processes
            .iter()
            .find(|process| process.id == authority.process_id)
            .expect("validated authority process must exist");
        if process.host_id != selected_host_id {
            continue;
        }
        let (mount_path, canonical_mount, mount_device) = local_disks
            .get(authority.disk_id.as_str())
            .expect("validated authority disk must exist");
        validate_selected_host_durable_path(
            &authority.state_path,
            mount_path.as_path(),
            canonical_mount,
            *mount_device,
            effective_uid,
            SelectedHostDurablePathKind::StateFile,
            &format!("authority {} state path", authority.id),
        )?;
    }

    for storage_node in &validated.manifest.storage_nodes {
        let process = validated
            .manifest
            .processes
            .iter()
            .find(|process| process.id == storage_node.process_id)
            .expect("validated storage-node process must exist");
        if process.host_id != selected_host_id {
            continue;
        }
        let (mount_path, canonical_mount, mount_device) = local_disks
            .get(storage_node.disk_id.as_str())
            .expect("validated storage-node disk must exist");
        validate_selected_host_durable_path(
            &storage_node.data_dir,
            mount_path.as_path(),
            canonical_mount,
            *mount_device,
            effective_uid,
            SelectedHostDurablePathKind::DataDirectory,
            &format!("storage node {} data path", storage_node.node_id),
        )?;
    }
    Ok(())
}

fn validate_selected_host_mount_boundary(
    mount_path: &Path,
    mount_metadata: &std::fs::Metadata,
    label: &str,
) -> Result<(), String> {
    let parent = mount_path
        .parent()
        .ok_or_else(|| format!("{label} has no parent filesystem boundary"))?;
    let parent_metadata = std::fs::symlink_metadata(parent)
        .map_err(|error| format!("{label} parent cannot be inspected: {error}"))?;
    if parent_metadata.file_type().is_symlink() || !parent_metadata.is_dir() {
        return Err(format!("{label} parent must be a non-symlink directory"));
    }
    validate_distinct_mount_devices(mount_metadata.dev(), parent_metadata.dev(), label)
}

fn validate_distinct_mount_devices(
    mount_device: u64,
    parent_device: u64,
    label: &str,
) -> Result<(), String> {
    if mount_device == parent_device {
        return Err(format!(
            "{label} must be an exact distinct-device mount boundary"
        ));
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum SelectedHostDurablePathKind {
    StateFile,
    DataDirectory,
}

fn validate_selected_host_durable_path(
    path: &Path,
    mount_path: &Path,
    canonical_mount: &Path,
    mount_device: u64,
    effective_uid: u32,
    kind: SelectedHostDurablePathKind,
    label: &str,
) -> Result<(), String> {
    let normalized_path = normalize_absolute_path(path, label)?;
    let relative = normalized_path
        .strip_prefix(mount_path)
        .expect("validated durable path must be within its disk mount");
    let mut current = mount_path.to_path_buf();
    let mut deepest_existing = mount_path.to_path_buf();
    let mut final_metadata = None;

    for component in relative.components() {
        current.push(component.as_os_str());
        match std::fs::symlink_metadata(&current) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() {
                    return Err(format!(
                        "{label} must not traverse a symlink below its disk mount"
                    ));
                }
                if metadata.dev() != mount_device {
                    return Err(format!(
                        "{label} crosses away from its declared disk device"
                    ));
                }
                validate_selected_host_path_permissions(&metadata, effective_uid, label)?;
                deepest_existing = current.clone();
                final_metadata = Some(metadata);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => return Err(format!("{label} cannot be inspected: {error}")),
        }
    }

    let deepest_metadata = std::fs::symlink_metadata(&deepest_existing)
        .map_err(|error| format!("{label} existing ancestor cannot be inspected: {error}"))?;
    validate_selected_host_path_permissions(&deepest_metadata, effective_uid, label)?;
    let canonical_existing = deepest_existing
        .canonicalize()
        .map_err(|error| format!("{label} existing ancestor cannot be canonicalized: {error}"))?;
    if !canonical_existing.starts_with(canonical_mount) {
        return Err(format!("{label} resolves outside its declared disk mount"));
    }

    if deepest_existing == normalized_path {
        let metadata = final_metadata.expect("existing final path metadata must be retained");
        match kind {
            SelectedHostDurablePathKind::StateFile if !metadata.is_file() => {
                return Err(format!("{label} must be a regular file when it exists"));
            }
            SelectedHostDurablePathKind::DataDirectory if !metadata.is_dir() => {
                return Err(format!("{label} must be a directory when it exists"));
            }
            _ => {}
        }
    } else if !deepest_metadata.is_dir() {
        return Err(format!(
            "{label} existing parent component must be a directory"
        ));
    }
    Ok(())
}

fn validate_selected_host_path_permissions(
    metadata: &std::fs::Metadata,
    effective_uid: u32,
    label: &str,
) -> Result<(), String> {
    if metadata.uid() != effective_uid {
        return Err(format!(
            "{label} must be owned by the effective process user"
        ));
    }
    let mode = metadata.permissions().mode() & 0o777;
    if mode & 0o022 != 0 {
        return Err(format!(
            "{label} must not be writable by group or other users"
        ));
    }
    Ok(())
}

pub(crate) fn parse_static_cluster_manifest(
    text: &str,
    process_id: &str,
) -> Result<ValidatedStaticClusterManifest, String> {
    if text.len() > CLUSTER_MANIFEST_MAX_BYTES as usize {
        return Err(format!(
            "cluster manifest exceeds {} bytes",
            CLUSTER_MANIFEST_MAX_BYTES
        ));
    }
    let input: StaticClusterManifestInput = toml::from_str(text).map_err(|error| {
        let location = error
            .span()
            .map(|span| format!(" at bytes {}..{}", span.start, span.end))
            .unwrap_or_default();
        let category = toml_error_category(error.message());
        format!("invalid cluster manifest {category}{location}")
    })?;
    validate_static_cluster_manifest(input, process_id)
}

fn validate_static_cluster_manifest(
    mut manifest: StaticClusterManifestInput,
    process_id: &str,
) -> Result<ValidatedStaticClusterManifest, String> {
    if manifest.schema_version != 1 {
        return Err(format!(
            "unsupported cluster manifest schema version {}",
            manifest.schema_version
        ));
    }
    validate_identifier(
        &manifest.cluster.id,
        CLUSTER_MANIFEST_MAX_CLUSTER_ID_BYTES,
        "cluster id",
    )?;
    validate_identifier(
        &manifest.cluster.region,
        CLUSTER_MANIFEST_MAX_REGION_BYTES,
        "cluster region",
    )?;
    require_nonzero(
        manifest.cluster.topology_generation,
        "cluster topology generation",
    )?;
    require_nonzero(manifest.storage.pg_count, "storage PG count")?;
    require_nonzero(
        manifest.storage.initial_cluster_epoch,
        "initial cluster epoch",
    )?;
    EcConfig::new(
        manifest.storage.ec_data_shards,
        manifest.storage.ec_parity_shards,
    )
    .map_err(|error| format!("invalid cluster manifest EC shape: {error}"))?;
    validate_raft(&manifest.raft)?;

    validate_collection_len(manifest.transport_profiles.len(), "transport profiles")?;
    validate_collection_len(manifest.hosts.len(), "hosts")?;
    validate_collection_len(manifest.disks.len(), "disks")?;
    validate_collection_len(manifest.processes.len(), "processes")?;
    validate_collection_len(manifest.authorities.len(), "authorities")?;
    validate_collection_len(manifest.storage_nodes.len(), "storage nodes")?;
    validate_collection_len(manifest.endpoints.len(), "endpoints")?;
    validate_collection_len(manifest.tls_identities.len(), "TLS identities")?;
    validate_collection_len(manifest.tls_trust_bundles.len(), "TLS trust bundles")?;
    validate_collection_len(manifest.auth_credentials.len(), "auth credentials")?;

    manifest
        .transport_profiles
        .sort_by(|left, right| left.id.cmp(&right.id));
    manifest.hosts.sort_by(|left, right| left.id.cmp(&right.id));
    manifest.disks.sort_by(|left, right| left.id.cmp(&right.id));
    manifest
        .processes
        .sort_by(|left, right| left.id.cmp(&right.id));
    manifest
        .authorities
        .sort_by(|left, right| left.id.cmp(&right.id));
    manifest
        .storage_nodes
        .sort_by_key(|storage_node| storage_node.node_id);
    manifest
        .endpoints
        .sort_by(|left, right| left.id.cmp(&right.id));
    manifest
        .tls_identities
        .sort_by(|left, right| left.id.cmp(&right.id));
    manifest
        .tls_trust_bundles
        .sort_by(|left, right| left.id.cmp(&right.id));
    manifest
        .auth_credentials
        .sort_by(|left, right| credential_sort_key(left).cmp(&credential_sort_key(right)));

    let transport_profiles = validate_transport_profiles(&manifest.transport_profiles)?;
    let hosts = validate_hosts(&manifest.hosts)?;
    let disks = validate_disks(&manifest.disks, &hosts)?;
    let processes = validate_processes(&manifest.processes, &hosts)?;
    let selected_process_index = manifest
        .processes
        .binary_search_by(|process| process.id.as_str().cmp(process_id))
        .map_err(|_| format!("selected process id {process_id:?} is absent from manifest"))?;
    let authorities = validate_authorities(
        &manifest.authorities,
        &processes,
        &disks,
        &manifest.deployment,
    )?;
    let storage_nodes = validate_storage_nodes(
        &manifest.storage_nodes,
        &processes,
        &disks,
        &manifest.deployment,
    )?;
    let tls_identities = validate_tls_identities(&manifest.tls_identities)?;
    let tls_trust_bundles = validate_tls_trust_bundles(&manifest.tls_trust_bundles)?;
    validate_endpoints(
        &manifest.endpoints,
        &processes,
        &authorities,
        &storage_nodes,
        &transport_profiles,
        &tls_identities,
        &tls_trust_bundles,
        &manifest.deployment,
    )?;
    validate_global_durable_path_uniqueness(
        &manifest.authorities,
        &manifest.storage_nodes,
        &processes,
    )?;
    let initial_pg_acting_sets = validate_deployment(
        &manifest,
        &hosts,
        &disks,
        &processes,
        &authorities,
        &storage_nodes,
    )?;
    validate_auth_credentials(
        &manifest.auth_credentials,
        &processes,
        &authorities,
        &storage_nodes,
        manifest.deployment.internal_auth,
    )?;
    let canonical_raft_peer_endpoints =
        resolve_canonical_raft_peer_endpoints(&manifest, &authorities)?;
    validate_raft_transport_capacity(
        &manifest,
        &transport_profiles,
        &authorities,
        &canonical_raft_peer_endpoints,
    )?;
    let topology_digest = topology_digest(
        &manifest,
        &initial_pg_acting_sets,
        &canonical_raft_peer_endpoints,
    );
    let process_identity_digest =
        process_identity_digest(&manifest, selected_process_index, &topology_digest);
    let full_config_fingerprint = full_config_fingerprint(&manifest);

    Ok(ValidatedStaticClusterManifest {
        manifest,
        selected_process_index,
        initial_pg_acting_sets,
        canonical_raft_peer_endpoints,
        topology_digest,
        process_identity_digest,
        full_config_fingerprint,
    })
}

fn toml_error_category(message: &str) -> &'static str {
    if message.contains("unknown field") {
        "contains an unknown field"
    } else if message.contains("duplicate key") {
        "contains a duplicate key"
    } else if message.contains("unknown variant") {
        "contains an unknown variant"
    } else {
        "has invalid syntax or field types"
    }
}

fn validate_raft(raft: &RaftInput) -> Result<(), String> {
    let required_append_entries =
        u64::try_from(ControlPlaneRaftPeerTransportLimits::REPLICATION_REQUIRED_APPEND_ENTRIES)
            .expect("production Raft append-entry requirement fits u64");
    if raft.max_append_entries != required_append_entries {
        return Err(format!(
            "raft max_append_entries must equal the production replication batch size {required_append_entries}"
        ));
    }
    let required_append_bytes =
        u64::try_from(ControlPlaneRaftPeerTransportLimits::DEFAULT_MAX_APPEND_ENTRIES_BYTES)
            .expect("production Raft append-byte requirement fits u64");
    if raft.max_append_bytes != required_append_bytes {
        return Err(format!(
            "raft max_append_bytes must equal the production replication payload size {required_append_bytes}"
        ));
    }
    if raft.max_snapshot_bytes == 0 || raft.max_snapshot_bytes > CLUSTER_MANIFEST_MAX_SNAPSHOT_BYTES
    {
        return Err(format!(
            "raft max_snapshot_bytes must be in 1..={CLUSTER_MANIFEST_MAX_SNAPSHOT_BYTES}"
        ));
    }
    Ok(())
}

fn validate_transport_profiles(
    profiles: &[TransportProfileInput],
) -> Result<BTreeMap<&str, &TransportProfileInput>, String> {
    if profiles.is_empty() {
        return Err("cluster manifest requires at least one transport profile".to_string());
    }
    let mut result = BTreeMap::new();
    for profile in profiles {
        validate_identifier(
            &profile.id,
            CLUSTER_MANIFEST_MAX_ID_BYTES,
            "transport profile id",
        )?;
        if profile.max_frame_bytes == 0
            || profile.max_frame_bytes > CLUSTER_MANIFEST_MAX_FRAME_BYTES
        {
            return Err(format!(
                "transport profile {} max_frame_bytes must be in 1..={CLUSTER_MANIFEST_MAX_FRAME_BYTES}",
                profile.id
            ));
        }
        if profile.max_connections == 0
            || profile.max_connections > CLUSTER_MANIFEST_MAX_CONNECTIONS
        {
            return Err(format!(
                "transport profile {} max_connections must be in 1..={CLUSTER_MANIFEST_MAX_CONNECTIONS}",
                profile.id
            ));
        }
        validate_timeout(
            profile.connect_timeout_ms,
            &format!("transport profile {} connect_timeout_ms", profile.id),
        )?;
        validate_timeout(
            profile.io_timeout_ms,
            &format!("transport profile {} io_timeout_ms", profile.id),
        )?;
        if result.insert(profile.id.as_str(), profile).is_some() {
            return Err(format!("duplicate transport profile id {}", profile.id));
        }
    }
    Ok(result)
}

fn validate_hosts(hosts: &[HostInput]) -> Result<BTreeSet<&str>, String> {
    if hosts.is_empty() {
        return Err("cluster manifest requires at least one host".to_string());
    }
    let mut result = BTreeSet::new();
    for host in hosts {
        validate_identifier(&host.id, CLUSTER_MANIFEST_MAX_ID_BYTES, "host id")?;
        validate_identifier(&host.zone, CLUSTER_MANIFEST_MAX_ID_BYTES, "host zone")?;
        validate_identifier(&host.rack, CLUSTER_MANIFEST_MAX_ID_BYTES, "host rack")?;
        if !result.insert(host.id.as_str()) {
            return Err(format!("duplicate host id {}", host.id));
        }
    }
    Ok(result)
}

fn validate_disks<'a>(
    disks: &'a [DiskInput],
    hosts: &BTreeSet<&str>,
) -> Result<BTreeMap<&'a str, &'a DiskInput>, String> {
    if disks.is_empty() {
        return Err("cluster manifest requires at least one disk".to_string());
    }
    let mut result = BTreeMap::new();
    let mut host_paths = BTreeSet::new();
    for disk in disks {
        validate_identifier(&disk.id, CLUSTER_MANIFEST_MAX_ID_BYTES, "disk id")?;
        if !hosts.contains(disk.host_id.as_str()) {
            return Err(format!(
                "disk {} references unknown host {}",
                disk.id, disk.host_id
            ));
        }
        let normalized = normalize_absolute_path(&disk.mount_path, "disk mount path")?;
        if !host_paths.insert((disk.host_id.as_str(), normalized)) {
            return Err(format!(
                "duplicate disk mount path on host {}",
                disk.host_id
            ));
        }
        if result.insert(disk.id.as_str(), disk).is_some() {
            return Err(format!("duplicate disk id {}", disk.id));
        }
    }
    Ok(result)
}

fn validate_processes<'a>(
    processes: &'a [ProcessInput],
    hosts: &BTreeSet<&str>,
) -> Result<BTreeMap<&'a str, &'a ProcessInput>, String> {
    if processes.is_empty() {
        return Err("cluster manifest requires at least one process".to_string());
    }
    let mut result = BTreeMap::new();
    let mut frontend_ids = BTreeSet::new();
    let mut admin_ids = BTreeSet::new();
    let mut maintenance_ids = BTreeSet::new();
    for process in processes {
        validate_identifier(&process.id, CLUSTER_MANIFEST_MAX_ID_BYTES, "process id")?;
        if !hosts.contains(process.host_id.as_str()) {
            return Err(format!(
                "process {} references unknown host {}",
                process.id, process.host_id
            ));
        }
        validate_optional_role_id(
            process.frontend_instance_id.as_deref(),
            process.kind.has_frontend(),
            "frontend_instance_id",
            &process.id,
            &mut frontend_ids,
        )?;
        validate_optional_role_id(
            process.admin_instance_id.as_deref(),
            process.kind.has_frontend() || process.kind.has_control_plane(),
            "admin_instance_id",
            &process.id,
            &mut admin_ids,
        )?;
        validate_optional_role_id(
            process.maintenance_instance_id.as_deref(),
            true,
            "maintenance_instance_id",
            &process.id,
            &mut maintenance_ids,
        )?;
        if result.insert(process.id.as_str(), process).is_some() {
            return Err(format!("duplicate process id {}", process.id));
        }
    }
    Ok(result)
}

fn validate_optional_role_id<'a>(
    value: Option<&'a str>,
    role_allowed: bool,
    field: &str,
    process_id: &str,
    seen: &mut BTreeSet<&'a str>,
) -> Result<(), String> {
    let Some(value) = value else {
        return Ok(());
    };
    if !role_allowed {
        return Err(format!(
            "process {process_id} cannot configure {field} for its kind"
        ));
    }
    validate_identifier(value, CLUSTER_MANIFEST_MAX_ID_BYTES, field)?;
    if !seen.insert(value) {
        return Err(format!("duplicate {field} {value}"));
    }
    Ok(())
}

fn validate_authorities<'a>(
    authorities: &'a [AuthorityInput],
    processes: &BTreeMap<&str, &ProcessInput>,
    disks: &BTreeMap<&str, &DiskInput>,
    deployment: &DeploymentInput,
) -> Result<BTreeMap<&'a str, &'a AuthorityInput>, String> {
    if authorities.is_empty() {
        return Err("cluster manifest requires at least one authority".to_string());
    }
    let mut result = BTreeMap::new();
    let mut process_ids = BTreeSet::new();
    let mut raft_node_ids = BTreeSet::new();
    let mut host_paths = BTreeSet::new();
    for authority in authorities {
        validate_identifier(&authority.id, CLUSTER_MANIFEST_MAX_ID_BYTES, "authority id")?;
        let process = processes
            .get(authority.process_id.as_str())
            .ok_or_else(|| {
                format!(
                    "authority {} references unknown process {}",
                    authority.id, authority.process_id
                )
            })?;
        if !process.kind.has_control_plane() {
            return Err(format!(
                "authority {} process {} cannot host a control plane",
                authority.id, authority.process_id
            ));
        }
        if !process_ids.insert(authority.process_id.as_str()) {
            return Err(format!(
                "process {} hosts more than one authority",
                authority.process_id
            ));
        }
        let disk = disks.get(authority.disk_id.as_str()).ok_or_else(|| {
            format!(
                "authority {} references unknown disk {}",
                authority.id, authority.disk_id
            )
        })?;
        if disk.host_id != process.host_id {
            return Err(format!(
                "authority {} disk and process are on different hosts",
                authority.id
            ));
        }
        let state_path = normalize_absolute_path(&authority.state_path, "authority state path")?;
        let mount_path = normalize_absolute_path(&disk.mount_path, "disk mount path")?;
        if !state_path.starts_with(&mount_path) || state_path == mount_path {
            return Err(format!(
                "authority {} state path is not contained by disk {}",
                authority.id, authority.disk_id
            ));
        }
        if !host_paths.insert((process.host_id.as_str(), state_path)) {
            return Err(format!(
                "duplicate authority state path on host {}",
                process.host_id
            ));
        }
        match (authority.kind, authority.raft_node_id, deployment.mode) {
            (AuthorityKind::Single, None, DeploymentMode::Standalone) => {}
            (AuthorityKind::RaftVoter, Some(node_id), DeploymentMode::Replicated)
                if node_id != 0 =>
            {
                if !raft_node_ids.insert(node_id) {
                    return Err(format!("duplicate Raft node id {node_id}"));
                }
            }
            _ => {
                return Err(format!(
                    "authority {} kind/node id is incompatible with deployment mode",
                    authority.id
                ));
            }
        }
        if result.insert(authority.id.as_str(), authority).is_some() {
            return Err(format!("duplicate authority id {}", authority.id));
        }
    }
    Ok(result)
}

fn validate_storage_nodes<'a>(
    storage_nodes: &'a [StorageNodeInput],
    processes: &BTreeMap<&str, &ProcessInput>,
    disks: &BTreeMap<&str, &DiskInput>,
    deployment: &DeploymentInput,
) -> Result<BTreeMap<u32, &'a StorageNodeInput>, String> {
    if storage_nodes.is_empty() {
        return Err("cluster manifest requires at least one storage node".to_string());
    }
    let mut result = BTreeMap::new();
    let mut process_ids = BTreeSet::new();
    let mut host_paths = BTreeSet::new();
    for storage_node in storage_nodes {
        require_nonzero(storage_node.node_id, "storage node id")?;
        let process = processes
            .get(storage_node.process_id.as_str())
            .ok_or_else(|| {
                format!(
                    "storage node {} references unknown process {}",
                    storage_node.node_id, storage_node.process_id
                )
            })?;
        if !process.kind.has_storage_node() {
            return Err(format!(
                "storage node {} process {} cannot host storage",
                storage_node.node_id, storage_node.process_id
            ));
        }
        if !process_ids.insert(storage_node.process_id.as_str()) {
            return Err(format!(
                "process {} hosts more than one storage node",
                storage_node.process_id
            ));
        }
        let disk = disks.get(storage_node.disk_id.as_str()).ok_or_else(|| {
            format!(
                "storage node {} references unknown disk {}",
                storage_node.node_id, storage_node.disk_id
            )
        })?;
        if disk.host_id != process.host_id {
            return Err(format!(
                "storage node {} disk and process are on different hosts",
                storage_node.node_id
            ));
        }
        let data_dir = normalize_absolute_path(&storage_node.data_dir, "storage data path")?;
        let mount_path = normalize_absolute_path(&disk.mount_path, "disk mount path")?;
        if !data_dir.starts_with(&mount_path) || data_dir == mount_path {
            return Err(format!(
                "storage node {} data path is not contained by disk {}",
                storage_node.node_id, storage_node.disk_id
            ));
        }
        if !host_paths.insert((process.host_id.as_str(), data_dir)) {
            return Err(format!(
                "duplicate storage data path on host {}",
                process.host_id
            ));
        }
        if result.insert(storage_node.node_id, storage_node).is_some() {
            return Err(format!(
                "duplicate storage node id {}",
                storage_node.node_id
            ));
        }
    }
    if deployment.mode == DeploymentMode::Standalone && storage_nodes.len() != 1 {
        return Err("standalone deployment requires exactly one storage node".to_string());
    }
    Ok(result)
}

fn validate_global_durable_path_uniqueness(
    authorities: &[AuthorityInput],
    storage_nodes: &[StorageNodeInput],
    processes: &BTreeMap<&str, &ProcessInput>,
) -> Result<(), String> {
    let mut paths = BTreeSet::new();
    for authority in authorities {
        let process = processes[authority.process_id.as_str()];
        let path = normalize_absolute_path(&authority.state_path, "authority state path")?;
        if !paths.insert((process.host_id.as_str(), path)) {
            return Err(format!(
                "duplicate durable state/data path on host {}",
                process.host_id
            ));
        }
    }
    for storage_node in storage_nodes {
        let process = processes[storage_node.process_id.as_str()];
        let path = normalize_absolute_path(&storage_node.data_dir, "storage data path")?;
        if !paths.insert((process.host_id.as_str(), path)) {
            return Err(format!(
                "duplicate durable state/data path on host {}",
                process.host_id
            ));
        }
    }
    Ok(())
}

fn validate_tls_identities(identities: &[TlsIdentityInput]) -> Result<BTreeSet<&str>, String> {
    let mut result = BTreeSet::new();
    for identity in identities {
        validate_identifier(
            &identity.id,
            CLUSTER_MANIFEST_MAX_ID_BYTES,
            "TLS identity id",
        )?;
        validate_file_reference(&identity.certificate_ref, "TLS certificate reference")?;
        validate_file_reference(&identity.private_key_ref, "TLS private key reference")?;
        if !result.insert(identity.id.as_str()) {
            return Err(format!("duplicate TLS identity id {}", identity.id));
        }
    }
    Ok(result)
}

fn validate_tls_trust_bundles(bundles: &[TlsTrustBundleInput]) -> Result<BTreeSet<&str>, String> {
    let mut result = BTreeSet::new();
    for bundle in bundles {
        validate_identifier(
            &bundle.id,
            CLUSTER_MANIFEST_MAX_ID_BYTES,
            "TLS trust bundle id",
        )?;
        validate_file_reference(&bundle.ca_bundle_ref, "TLS CA bundle reference")?;
        if !result.insert(bundle.id.as_str()) {
            return Err(format!("duplicate TLS trust bundle id {}", bundle.id));
        }
    }
    Ok(result)
}

#[allow(clippy::too_many_arguments)]
fn validate_endpoints(
    endpoints: &[EndpointInput],
    processes: &BTreeMap<&str, &ProcessInput>,
    authorities: &BTreeMap<&str, &AuthorityInput>,
    storage_nodes: &BTreeMap<u32, &StorageNodeInput>,
    transport_profiles: &BTreeMap<&str, &TransportProfileInput>,
    tls_identities: &BTreeSet<&str>,
    tls_trust_bundles: &BTreeSet<&str>,
    deployment: &DeploymentInput,
) -> Result<(), String> {
    let authority_by_process: BTreeMap<&str, &AuthorityInput> = authorities
        .values()
        .map(|authority| (authority.process_id.as_str(), *authority))
        .collect();
    let storage_by_process: BTreeMap<&str, &StorageNodeInput> = storage_nodes
        .values()
        .map(|storage_node| (storage_node.process_id.as_str(), *storage_node))
        .collect();
    let mut endpoint_ids = BTreeSet::new();
    let mut priorities = BTreeSet::new();
    let mut unix_paths = BTreeSet::new();
    let mut protocols_by_process = BTreeSet::new();
    let mut tcp_protocols_by_process = BTreeSet::new();

    for endpoint in endpoints {
        validate_identifier(&endpoint.id, CLUSTER_MANIFEST_MAX_ID_BYTES, "endpoint id")?;
        if !endpoint_ids.insert(endpoint.id.as_str()) {
            return Err(format!("duplicate endpoint id {}", endpoint.id));
        }
        let process = processes
            .get(endpoint.owner_process_id.as_str())
            .ok_or_else(|| {
                format!(
                    "endpoint {} references unknown process {}",
                    endpoint.id, endpoint.owner_process_id
                )
            })?;
        require_nonzero(endpoint.priority, "endpoint priority")?;
        if !priorities.insert((
            endpoint.owner_process_id.as_str(),
            endpoint.protocol,
            endpoint.priority,
        )) {
            return Err(format!(
                "duplicate endpoint priority for process {} protocol {:?}",
                endpoint.owner_process_id, endpoint.protocol
            ));
        }
        let profile = transport_profiles
            .get(endpoint.transport_profile_id.as_str())
            .ok_or_else(|| {
                format!(
                    "endpoint {} references unknown transport profile {}",
                    endpoint.id, endpoint.transport_profile_id
                )
            })?;
        if endpoint.protocol == EndpointProtocol::RaftPeer
            && profile.max_frame_bytes > CLUSTER_MANIFEST_MAX_RAFT_FRAME_BYTES
        {
            return Err(format!(
                "Raft endpoint {} frame limit exceeds {}",
                endpoint.id, CLUSTER_MANIFEST_MAX_RAFT_FRAME_BYTES
            ));
        }

        let listen = parse_endpoint_address(&endpoint.listen, true)?;
        let advertise = parse_endpoint_address(&endpoint.advertise, false)?;
        if endpoint.listen != canonical_endpoint_uri(&listen) {
            return Err(format!(
                "endpoint {} listen URI is not canonical",
                endpoint.id
            ));
        }
        if endpoint.advertise != canonical_endpoint_uri(&advertise) {
            return Err(format!(
                "endpoint {} advertise URI is not canonical",
                endpoint.id
            ));
        }
        match (&listen, &advertise) {
            (EndpointAddress::Unix(listen), EndpointAddress::Unix(advertise)) => {
                if listen != advertise {
                    return Err(format!(
                        "Unix endpoint {} listen and advertise paths must match",
                        endpoint.id
                    ));
                }
                if endpoint.tls_identity_id.is_some()
                    || endpoint.tls_trust_bundle_id.is_some()
                    || endpoint.tls_server_name.is_some()
                {
                    return Err(format!(
                        "Unix endpoint {} must omit TLS fields",
                        endpoint.id
                    ));
                }
                if !unix_paths.insert((process.host_id.as_str(), listen.clone())) {
                    return Err(format!(
                        "duplicate Unix endpoint path on host {}",
                        process.host_id
                    ));
                }
            }
            (
                EndpointAddress::Tcp {
                    port: listen_port, ..
                },
                EndpointAddress::Tcp {
                    host: advertise_host,
                    port: advertise_port,
                },
            ) => {
                if listen_port != advertise_port {
                    return Err(format!(
                        "TCP endpoint {} listen and advertise ports must match",
                        endpoint.id
                    ));
                }
                let identity = endpoint.tls_identity_id.as_deref().ok_or_else(|| {
                    format!("TCP endpoint {} requires tls_identity_id", endpoint.id)
                })?;
                if !tls_identities.contains(identity) {
                    return Err(format!(
                        "TCP endpoint {} references unknown TLS identity {}",
                        endpoint.id, identity
                    ));
                }
                let trust_bundle = endpoint.tls_trust_bundle_id.as_deref().ok_or_else(|| {
                    format!("TCP endpoint {} requires tls_trust_bundle_id", endpoint.id)
                })?;
                if !tls_trust_bundles.contains(trust_bundle) {
                    return Err(format!(
                        "TCP endpoint {} references unknown TLS trust bundle {}",
                        endpoint.id, trust_bundle
                    ));
                }
                let server_name = endpoint.tls_server_name.as_deref().ok_or_else(|| {
                    format!("TCP endpoint {} requires tls_server_name", endpoint.id)
                })?;
                validate_server_name(server_name)?;
                if advertise_host != server_name {
                    return Err(format!(
                        "TCP endpoint {} tls_server_name must match its advertised host",
                        endpoint.id
                    ));
                }
                if deployment.internal_auth != InternalAuth::Required {
                    return Err(format!(
                        "TCP endpoint {} requires internal authentication",
                        endpoint.id
                    ));
                }
                tcp_protocols_by_process
                    .insert((endpoint.owner_process_id.as_str(), endpoint.protocol));
            }
            _ => {
                return Err(format!(
                    "endpoint {} listen and advertise transports differ",
                    endpoint.id
                ));
            }
        }

        match endpoint.protocol {
            EndpointProtocol::RaftPeer => {
                let authority = authority_by_process
                    .get(endpoint.owner_process_id.as_str())
                    .ok_or_else(|| {
                        format!(
                            "Raft endpoint {} owner does not host an authority",
                            endpoint.id
                        )
                    })?;
                if authority.kind != AuthorityKind::RaftVoter {
                    return Err(format!(
                        "Raft endpoint {} owner is not a Raft voter",
                        endpoint.id
                    ));
                }
            }
            EndpointProtocol::ControlPlane | EndpointProtocol::AuthorityClockRecovery => {
                if !authority_by_process.contains_key(endpoint.owner_process_id.as_str()) {
                    return Err(format!(
                        "control-plane endpoint {} owner does not host an authority",
                        endpoint.id
                    ));
                }
            }
            EndpointProtocol::StorageRpc => {
                if !storage_by_process.contains_key(endpoint.owner_process_id.as_str()) {
                    return Err(format!(
                        "storage endpoint {} owner does not host a storage node",
                        endpoint.id
                    ));
                }
            }
        }
        protocols_by_process.insert((endpoint.owner_process_id.as_str(), endpoint.protocol));
    }

    for authority in authorities.values() {
        let process = processes[authority.process_id.as_str()];
        let embedded_single =
            authority.kind == AuthorityKind::Single && process.kind == ProcessKind::AllInOne;
        if authority.kind == AuthorityKind::RaftVoter
            && !protocols_by_process
                .contains(&(authority.process_id.as_str(), EndpointProtocol::RaftPeer))
        {
            return Err(format!(
                "Raft voter {} is missing a raft-peer endpoint",
                authority.id
            ));
        }
        if authority.kind == AuthorityKind::RaftVoter
            && authorities
                .values()
                .any(|peer| processes[peer.process_id.as_str()].host_id != process.host_id)
            && !tcp_protocols_by_process
                .contains(&(authority.process_id.as_str(), EndpointProtocol::RaftPeer))
        {
            return Err(format!(
                "Raft voter {} requires a TCP raft-peer endpoint for cross-host peers",
                authority.id
            ));
        }
        for protocol in [
            EndpointProtocol::ControlPlane,
            EndpointProtocol::AuthorityClockRecovery,
        ] {
            if !embedded_single
                && !protocols_by_process.contains(&(authority.process_id.as_str(), protocol))
            {
                return Err(format!(
                    "authority {} is missing required {:?} endpoint",
                    authority.id, protocol
                ));
            }
        }
        let remote_control_plane_client = processes.values().any(|client| {
            (client.kind.has_frontend() || client.kind.has_storage_node())
                && client.host_id != process.host_id
        });
        if remote_control_plane_client
            && !tcp_protocols_by_process.contains(&(
                authority.process_id.as_str(),
                EndpointProtocol::ControlPlane,
            ))
        {
            return Err(format!(
                "authority {} requires a TCP control-plane endpoint for cross-host clients",
                authority.id
            ));
        }
        let remote_clock_client = processes
            .values()
            .any(|client| client.admin_instance_id.is_some() && client.host_id != process.host_id);
        if remote_clock_client
            && !tcp_protocols_by_process.contains(&(
                authority.process_id.as_str(),
                EndpointProtocol::AuthorityClockRecovery,
            ))
        {
            return Err(format!(
                "authority {} requires a TCP authority-clock-recovery endpoint for cross-host clients",
                authority.id
            ));
        }
    }
    for storage_node in storage_nodes.values() {
        if !protocols_by_process.contains(&(
            storage_node.process_id.as_str(),
            EndpointProtocol::StorageRpc,
        )) {
            return Err(format!(
                "storage node {} is missing a storage-rpc endpoint",
                storage_node.node_id
            ));
        }
        let process = processes[storage_node.process_id.as_str()];
        let remote_storage_client = processes.values().any(|client| {
            (client.kind.has_frontend() || client.kind.has_storage_node())
                && client.host_id != process.host_id
        });
        if remote_storage_client
            && !tcp_protocols_by_process.contains(&(
                storage_node.process_id.as_str(),
                EndpointProtocol::StorageRpc,
            ))
        {
            return Err(format!(
                "storage node {} requires a TCP storage-rpc endpoint for cross-host clients",
                storage_node.node_id
            ));
        }
    }
    Ok(())
}

fn validate_deployment(
    manifest: &StaticClusterManifestInput,
    hosts: &BTreeSet<&str>,
    disks: &BTreeMap<&str, &DiskInput>,
    processes: &BTreeMap<&str, &ProcessInput>,
    authorities: &BTreeMap<&str, &AuthorityInput>,
    storage_nodes: &BTreeMap<u32, &StorageNodeInput>,
) -> Result<Vec<Vec<u32>>, String> {
    match manifest.deployment.mode {
        DeploymentMode::Standalone => {
            if hosts.len() != 1 {
                return Err("standalone deployment requires exactly one host".to_string());
            }
            if manifest.deployment.failure_domain != FailureDomain::None
                || manifest.deployment.failure_tolerance != 0
            {
                return Err(
                    "standalone deployment requires failure_domain=none and tolerance=0"
                        .to_string(),
                );
            }
            if manifest.storage.ec_data_shards != 1 || manifest.storage.ec_parity_shards != 0 {
                return Err("standalone deployment requires EC 1+0".to_string());
            }
            if authorities.len() != 1
                || authorities
                    .values()
                    .any(|authority| authority.kind != AuthorityKind::Single)
            {
                return Err(
                    "standalone deployment requires exactly one single authority".to_string(),
                );
            }
            if storage_nodes.len() != 1 {
                return Err("standalone deployment requires exactly one storage node".to_string());
            }
        }
        DeploymentMode::Replicated => {
            if processes
                .values()
                .any(|process| process.kind == ProcessKind::AllInOne)
            {
                return Err(
                    "all-in-one processes are permitted only in standalone deployment".to_string(),
                );
            }
            if manifest.deployment.internal_auth != InternalAuth::Required {
                return Err("replicated deployment requires internal authentication".to_string());
            }
            if !matches!(
                manifest.deployment.failure_domain,
                FailureDomain::Disk | FailureDomain::Host
            ) || manifest.deployment.failure_tolerance == 0
            {
                return Err(
                    "replicated deployment requires disk/host failure domain and nonzero tolerance"
                        .to_string(),
                );
            }
            if manifest.storage.ec_parity_shards < manifest.deployment.failure_tolerance {
                return Err("replicated deployment parity must cover failure tolerance".to_string());
            }
            let required_storage_domains = usize::from(manifest.storage.ec_data_shards)
                + usize::from(manifest.storage.ec_parity_shards);
            let storage_domains: BTreeSet<&str> = storage_nodes
                .values()
                .map(|storage_node| {
                    let process = processes[storage_node.process_id.as_str()];
                    match manifest.deployment.failure_domain {
                        FailureDomain::Disk => storage_node.disk_id.as_str(),
                        FailureDomain::Host => process.host_id.as_str(),
                        FailureDomain::None => unreachable!(),
                    }
                })
                .collect();
            if storage_domains.len() < required_storage_domains {
                return Err(format!(
                    "replicated deployment requires at least {required_storage_domains} storage failure domains"
                ));
            }
            let required_voters = usize::from(manifest.deployment.failure_tolerance) * 2 + 1;
            if authorities.len() < required_voters
                || authorities
                    .values()
                    .any(|authority| authority.kind != AuthorityKind::RaftVoter)
            {
                return Err(format!(
                    "replicated deployment requires at least {required_voters} Raft voters"
                ));
            }
            let voter_domains: BTreeSet<&str> = authorities
                .values()
                .map(|authority| {
                    let process = processes[authority.process_id.as_str()];
                    match manifest.deployment.failure_domain {
                        FailureDomain::Disk => authority.disk_id.as_str(),
                        FailureDomain::Host => process.host_id.as_str(),
                        FailureDomain::None => unreachable!(),
                    }
                })
                .collect();
            if voter_domains.len() != authorities.len() {
                return Err("Raft voters must occupy distinct selected failure domains".to_string());
            }
        }
    }

    for process in processes.values() {
        let hosts_authority = authorities
            .values()
            .any(|authority| authority.process_id == process.id);
        let hosts_storage = storage_nodes
            .values()
            .any(|storage_node| storage_node.process_id == process.id);
        if process.kind.has_control_plane() != hosts_authority {
            return Err(format!(
                "process {} control-plane kind does not match hosted authority",
                process.id
            ));
        }
        if process.kind.has_storage_node() != hosts_storage {
            return Err(format!(
                "process {} storage kind does not match hosted storage node",
                process.id
            ));
        }
        if manifest.deployment.internal_auth == InternalAuth::Required
            && process.kind.has_frontend()
            && process.frontend_instance_id.is_none()
        {
            return Err(format!(
                "authenticated frontend process {} requires frontend_instance_id",
                process.id
            ));
        }
        if manifest.deployment.internal_auth == InternalAuth::Required
            && (process.kind.has_frontend() || process.kind.has_control_plane())
            && process.admin_instance_id.is_none()
        {
            return Err(format!(
                "authenticated admin-capable process {} requires admin_instance_id",
                process.id
            ));
        }
    }

    validate_initial_pg_placement(manifest, hosts, disks, processes, storage_nodes)
}

fn validate_initial_pg_placement(
    manifest: &StaticClusterManifestInput,
    hosts: &BTreeSet<&str>,
    disks: &BTreeMap<&str, &DiskInput>,
    processes: &BTreeMap<&str, &ProcessInput>,
    storage_nodes: &BTreeMap<u32, &StorageNodeInput>,
) -> Result<Vec<Vec<u32>>, String> {
    let pg_count = usize::try_from(manifest.storage.pg_count)
        .map_err(|_| "storage PG count does not fit this platform".to_string())?;
    validate_collection_len(pg_count, "storage PG count")?;
    let host_domains: BTreeMap<&str, u32> = hosts
        .iter()
        .enumerate()
        .map(|(index, host_id)| {
            let domain = u32::try_from(index + 1)
                .expect("manifest collection bound guarantees a u32 host domain");
            (*host_id, domain)
        })
        .collect();
    let disk_domains: BTreeMap<&str, u32> = disks
        .keys()
        .enumerate()
        .map(|(index, disk_id)| {
            let domain = u32::try_from(index + 1)
                .expect("manifest collection bound guarantees a u32 disk domain");
            (*disk_id, domain)
        })
        .collect();
    let placement_nodes: Vec<NodeInfo> = storage_nodes
        .values()
        .map(|storage_node| {
            let process = processes[storage_node.process_id.as_str()];
            let location = TopologyKey::new(&[
                (Level::MACHINE, host_domains[process.host_id.as_str()]),
                (Level::DISK, disk_domains[storage_node.disk_id.as_str()]),
            ])
            .expect("distinct built-in topology levels");
            NodeInfo {
                id: NodeId::new(storage_node.node_id),
                location,
                weight: 1.0,
            }
        })
        .collect();
    let cluster_map = ClusterMap::new(&placement_nodes)
        .map_err(|error| format!("initial PG placement cluster map is invalid: {error}"))?;
    let total_shards = usize::from(manifest.storage.ec_data_shards)
        + usize::from(manifest.storage.ec_parity_shards);
    let placement_config = PlacementConfig::new(total_shards)
        .map_err(|error| format!("initial PG placement shape is invalid: {error}"))?;
    let constraint = match manifest.deployment.failure_domain {
        FailureDomain::None => PlacementConstraint::none(),
        FailureDomain::Disk => PlacementConstraint::level_cap(Level::DISK, 1),
        FailureDomain::Host => PlacementConstraint::level_cap(Level::MACHINE, 1),
    };
    let placer = Placer::new(placement_config, &cluster_map, constraint)
        .map_err(|error| format!("initial PG placement is impossible: {error}"))?;
    let node_domains: BTreeMap<u32, &str> = storage_nodes
        .values()
        .map(|storage_node| {
            let process = processes[storage_node.process_id.as_str()];
            let domain = match manifest.deployment.failure_domain {
                FailureDomain::None => process.host_id.as_str(),
                FailureDomain::Disk => storage_node.disk_id.as_str(),
                FailureDomain::Host => process.host_id.as_str(),
            };
            (storage_node.node_id, domain)
        })
        .collect();

    let mut acting_sets = Vec::with_capacity(pg_count);
    let mut placement_key =
        Vec::with_capacity(INITIAL_PG_PLACEMENT_KEY_DOMAIN.len() + std::mem::size_of::<u32>());
    for pg_id in 0..manifest.storage.pg_count {
        placement_key.clear();
        placement_key.extend_from_slice(INITIAL_PG_PLACEMENT_KEY_DOMAIN);
        placement_key.extend_from_slice(&pg_id.to_be_bytes());
        let mut acting_set = vec![NodeId::new(0); total_shards];
        placer
            .place(&placement_key, &mut acting_set)
            .map_err(|error| {
                format!("initial placement for PG {pg_id} violates deployment policy: {error}")
            })?;
        if manifest.deployment.failure_domain != FailureDomain::None {
            let distinct_domains: BTreeSet<&str> = acting_set
                .iter()
                .map(|node_id| node_domains[&node_id.as_u32()])
                .collect();
            if distinct_domains.len() != total_shards {
                return Err(format!(
                    "initial placement for PG {pg_id} does not occupy {total_shards} distinct failure domains"
                ));
            }
        }
        acting_sets.push(acting_set.into_iter().map(NodeId::as_u32).collect());
    }
    Ok(acting_sets)
}

fn validate_auth_credentials(
    credentials: &[AuthCredentialInput],
    processes: &BTreeMap<&str, &ProcessInput>,
    authorities: &BTreeMap<&str, &AuthorityInput>,
    storage_nodes: &BTreeMap<u32, &StorageNodeInput>,
    internal_auth: InternalAuth,
) -> Result<(), String> {
    let raft_nodes: BTreeSet<u64> = authorities
        .values()
        .filter_map(|authority| authority.raft_node_id)
        .collect();
    let storage_node_ids: BTreeSet<u64> = storage_nodes
        .keys()
        .map(|node_id| u64::from(*node_id))
        .collect();
    let frontend_ids: BTreeSet<&str> = processes
        .values()
        .filter_map(|process| process.frontend_instance_id.as_deref())
        .collect();
    let admin_ids: BTreeSet<&str> = processes
        .values()
        .filter_map(|process| process.admin_instance_id.as_deref())
        .collect();
    let maintenance_ids: BTreeSet<&str> = processes
        .values()
        .filter_map(|process| process.maintenance_instance_id.as_deref())
        .collect();

    let mut identities = BTreeSet::new();
    let mut signing_by_principal: BTreeMap<CredentialPrincipalKey, Vec<(u64, Option<u64>)>> =
        BTreeMap::new();
    for credential in credentials {
        validate_identifier(
            &credential.credential_id,
            CLUSTER_MANIFEST_MAX_ID_BYTES,
            "credential id",
        )?;
        require_nonzero(credential.credential_version, "credential version")?;
        validate_file_reference(&credential.secret_ref, "credential secret reference")?;
        if credential
            .accept_until_ms
            .is_some_and(|until| until <= credential.accept_from_ms)
        {
            return Err(format!(
                "credential {} acceptance window is empty",
                credential.credential_id
            ));
        }
        let principal = credential_principal_key(credential)?;
        match (&principal.principal, &principal.id) {
            (AuthPrincipal::RaftPeer, CredentialPrincipalId::Node(node_id))
                if raft_nodes.contains(node_id) => {}
            (AuthPrincipal::StorageNode, CredentialPrincipalId::Node(node_id))
                if storage_node_ids.contains(node_id) => {}
            (AuthPrincipal::Frontend, CredentialPrincipalId::Instance(instance_id))
                if frontend_ids.contains(instance_id.as_str()) => {}
            (AuthPrincipal::Admin, CredentialPrincipalId::Instance(instance_id))
                if admin_ids.contains(instance_id.as_str()) => {}
            (AuthPrincipal::Maintenance, CredentialPrincipalId::Instance(instance_id))
                if maintenance_ids.contains(instance_id.as_str()) => {}
            _ => {
                return Err(format!(
                    "credential {} references an unknown or wrong-kind principal",
                    credential.credential_id
                ));
            }
        }
        let identity = CredentialIdentity {
            principal: principal.clone(),
            credential_id: credential.credential_id.clone(),
            credential_version: credential.credential_version,
        };
        if !identities.insert(identity) {
            return Err(format!(
                "duplicate credential identity {} version {}",
                credential.credential_id, credential.credential_version
            ));
        }
        if credential.use_for_signing {
            signing_by_principal
                .entry(principal)
                .or_default()
                .push((credential.accept_from_ms, credential.accept_until_ms));
        }
    }

    for windows in signing_by_principal.values_mut() {
        windows.sort_unstable_by_key(|window| window.0);
        for pair in windows.windows(2) {
            if pair[0].1.is_none_or(|until| until > pair[1].0) {
                return Err("signing credential acceptance windows overlap".to_string());
            }
        }
    }

    if internal_auth == InternalAuth::Required {
        let required = required_auth_principals(
            &raft_nodes,
            &storage_node_ids,
            &frontend_ids,
            &admin_ids,
            &maintenance_ids,
        );
        for principal in required {
            if !signing_by_principal.contains_key(&principal) {
                return Err("required principal has no signing credential".to_string());
            }
        }
    }
    Ok(())
}

fn resolve_canonical_raft_peer_endpoints(
    manifest: &StaticClusterManifestInput,
    authorities: &BTreeMap<&str, &AuthorityInput>,
) -> Result<BTreeMap<u64, String>, String> {
    let process_hosts: BTreeMap<&str, &str> = manifest
        .processes
        .iter()
        .map(|process| (process.id.as_str(), process.host_id.as_str()))
        .collect();
    let voter_hosts: BTreeSet<&str> = authorities
        .values()
        .filter(|authority| authority.kind == AuthorityKind::RaftVoter)
        .map(|authority| process_hosts[authority.process_id.as_str()])
        .collect();
    let mut resolved = BTreeMap::new();
    for authority in authorities
        .values()
        .filter(|authority| authority.kind == AuthorityKind::RaftVoter)
    {
        let target_host = process_hosts[authority.process_id.as_str()];
        let requires_tcp = voter_hosts
            .iter()
            .any(|source_host| *source_host != target_host);
        let endpoint = manifest
            .endpoints
            .iter()
            .filter(|endpoint| {
                endpoint.protocol == EndpointProtocol::RaftPeer
                    && endpoint.owner_process_id == authority.process_id
            })
            .filter(|endpoint| {
                !requires_tcp
                    || matches!(
                        parse_endpoint_address(&endpoint.advertise, false),
                        Ok(EndpointAddress::Tcp { .. })
                    )
            })
            .min_by_key(|endpoint| (endpoint.priority, endpoint.id.as_str()))
            .ok_or_else(|| {
                format!(
                    "Raft voter {} has no peer endpoint reachable from every configured voter",
                    authority.id
                )
            })?;
        let node_id = authority
            .raft_node_id
            .expect("replicated authority validation requires a Raft node id");
        resolved.insert(node_id, endpoint.advertise.clone());
    }
    Ok(resolved)
}

fn validate_raft_transport_capacity(
    manifest: &StaticClusterManifestInput,
    transport_profiles: &BTreeMap<&str, &TransportProfileInput>,
    authorities: &BTreeMap<&str, &AuthorityInput>,
    canonical_raft_peer_endpoints: &BTreeMap<u64, String>,
) -> Result<(), String> {
    let voter_processes: BTreeSet<&str> = authorities
        .values()
        .filter(|authority| authority.kind == AuthorityKind::RaftVoter)
        .map(|authority| authority.process_id.as_str())
        .collect();
    if voter_processes.is_empty() {
        return Ok(());
    }

    let mut max_advertised_uri_by_process = BTreeMap::new();
    for endpoint in manifest
        .endpoints
        .iter()
        .filter(|endpoint| endpoint.protocol == EndpointProtocol::RaftPeer)
    {
        max_advertised_uri_by_process
            .entry(endpoint.owner_process_id.as_str())
            .and_modify(|current: &mut usize| *current = (*current).max(endpoint.advertise.len()))
            .or_insert(endpoint.advertise.len());
    }
    let membership_metadata_bytes =
        voter_processes
            .iter()
            .try_fold(0_u64, |total, process_id| -> Result<u64, String> {
                let uri_bytes =
                    *max_advertised_uri_by_process
                        .get(process_id)
                        .ok_or_else(|| {
                            format!("Raft voter process {process_id} has no peer endpoint")
                        })?;
                let per_voter = u64::try_from(uri_bytes)
                    .map_err(|_| "Raft peer URI length does not fit u64".to_string())?
                    .checked_add(32)
                    .ok_or_else(|| "Raft snapshot metadata size overflows".to_string())?;
                total
                    .checked_add(per_voter)
                    .ok_or_else(|| "Raft snapshot metadata size overflows".to_string())
            })?;
    let max_credential_id_bytes = manifest
        .auth_credentials
        .iter()
        .filter(|credential| credential.principal == AuthPrincipal::RaftPeer)
        .map(|credential| credential.credential_id.len())
        .max()
        .unwrap_or(0);
    let identity_bytes = manifest
        .cluster
        .id
        .len()
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(max_credential_id_bytes))
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| "Raft snapshot identity metadata size overflows".to_string())?;
    let required_append_frame_bytes = manifest
        .raft
        .max_append_bytes
        .checked_add(CLUSTER_MANIFEST_RAFT_APPEND_FIXED_FRAME_OVERHEAD_BYTES)
        .and_then(|bytes| bytes.checked_add(identity_bytes))
        .ok_or_else(|| "Raft append frame size overflows".to_string())?;
    let required_frame_bytes = manifest
        .raft
        .max_snapshot_bytes
        .checked_add(CLUSTER_MANIFEST_RAFT_SNAPSHOT_FIXED_FRAME_OVERHEAD_BYTES)
        .and_then(|bytes| bytes.checked_add(membership_metadata_bytes))
        .and_then(|bytes| bytes.checked_add(identity_bytes))
        .ok_or_else(|| "Raft snapshot frame size overflows".to_string())?;

    for endpoint in manifest
        .endpoints
        .iter()
        .filter(|endpoint| endpoint.protocol == EndpointProtocol::RaftPeer)
    {
        let profile = transport_profiles[endpoint.transport_profile_id.as_str()];
        let limits = ControlPlaneRaftPeerTransportLimits {
            max_frame_bytes: usize::try_from(profile.max_frame_bytes)
                .map_err(|_| "Raft frame limit does not fit this platform".to_string())?,
            max_append_entries: usize::try_from(manifest.raft.max_append_entries)
                .map_err(|_| "Raft append-entry limit does not fit this platform".to_string())?,
            max_append_entries_bytes: usize::try_from(manifest.raft.max_append_bytes)
                .map_err(|_| "Raft append-byte limit does not fit this platform".to_string())?,
            max_snapshot_bytes: usize::try_from(manifest.raft.max_snapshot_bytes)
                .map_err(|_| "Raft snapshot limit does not fit this platform".to_string())?,
        };
        if profile.max_frame_bytes < required_append_frame_bytes {
            return Err(format!(
                "Raft endpoint {} frame limit cannot carry max append bytes plus identity and authentication overhead",
                endpoint.id
            ));
        }
        ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
            manifest.cluster.id.clone(),
            canonical_raft_peer_endpoints.clone(),
            limits,
        )
        .validate_replication_compatibility()
        .map_err(|_| {
            format!(
                "Raft endpoint {} transport limits are incompatible with production replication",
                endpoint.id
            )
        })?;
        if profile.max_frame_bytes < required_frame_bytes {
            return Err(format!(
                "Raft endpoint {} frame limit cannot carry max snapshot bytes plus metadata and authentication overhead",
                endpoint.id
            ));
        }
    }
    Ok(())
}

fn required_auth_principals(
    raft_nodes: &BTreeSet<u64>,
    storage_nodes: &BTreeSet<u64>,
    frontend_ids: &BTreeSet<&str>,
    admin_ids: &BTreeSet<&str>,
    maintenance_ids: &BTreeSet<&str>,
) -> BTreeSet<CredentialPrincipalKey> {
    let mut result = BTreeSet::new();
    result.extend(
        raft_nodes
            .iter()
            .copied()
            .map(|node_id| CredentialPrincipalKey {
                principal: AuthPrincipal::RaftPeer,
                id: CredentialPrincipalId::Node(node_id),
            }),
    );
    result.extend(
        storage_nodes
            .iter()
            .copied()
            .map(|node_id| CredentialPrincipalKey {
                principal: AuthPrincipal::StorageNode,
                id: CredentialPrincipalId::Node(node_id),
            }),
    );
    result.extend(
        frontend_ids
            .iter()
            .map(|instance_id| CredentialPrincipalKey {
                principal: AuthPrincipal::Frontend,
                id: CredentialPrincipalId::Instance((*instance_id).to_string()),
            }),
    );
    result.extend(admin_ids.iter().map(|instance_id| CredentialPrincipalKey {
        principal: AuthPrincipal::Admin,
        id: CredentialPrincipalId::Instance((*instance_id).to_string()),
    }));
    result.extend(
        maintenance_ids
            .iter()
            .map(|instance_id| CredentialPrincipalKey {
                principal: AuthPrincipal::Maintenance,
                id: CredentialPrincipalId::Instance((*instance_id).to_string()),
            }),
    );
    result
}

fn credential_principal_key(
    credential: &AuthCredentialInput,
) -> Result<CredentialPrincipalKey, String> {
    let id = match credential.principal {
        AuthPrincipal::RaftPeer | AuthPrincipal::StorageNode => {
            let node_id = credential.node_id.ok_or_else(|| {
                format!("credential {} requires node_id", credential.credential_id)
            })?;
            require_nonzero(node_id, "credential node id")?;
            if credential.instance_id.is_some() {
                return Err(format!(
                    "credential {} must omit instance_id",
                    credential.credential_id
                ));
            }
            CredentialPrincipalId::Node(node_id)
        }
        AuthPrincipal::Frontend | AuthPrincipal::Admin | AuthPrincipal::Maintenance => {
            let instance_id = credential.instance_id.as_deref().ok_or_else(|| {
                format!(
                    "credential {} requires instance_id",
                    credential.credential_id
                )
            })?;
            validate_identifier(
                instance_id,
                CLUSTER_MANIFEST_MAX_ID_BYTES,
                "credential instance id",
            )?;
            if credential.node_id.is_some() {
                return Err(format!(
                    "credential {} must omit node_id",
                    credential.credential_id
                ));
            }
            CredentialPrincipalId::Instance(instance_id.to_string())
        }
    };
    Ok(CredentialPrincipalKey {
        principal: credential.principal,
        id,
    })
}

fn credential_sort_key(
    credential: &AuthCredentialInput,
) -> (AuthPrincipal, Option<u64>, Option<&str>, &str, u64) {
    (
        credential.principal,
        credential.node_id,
        credential.instance_id.as_deref(),
        credential.credential_id.as_str(),
        credential.credential_version,
    )
}

fn canonical_endpoint_uri(address: &EndpointAddress) -> String {
    match address {
        EndpointAddress::Unix(path) => format!("unix://{}", path.display()),
        EndpointAddress::Tcp { host, port } if host.contains(':') => {
            format!("tcp://[{host}]:{port}")
        }
        EndpointAddress::Tcp { host, port } => format!("tcp://{host}:{port}"),
    }
}

fn parse_endpoint_address(value: &str, listener: bool) -> Result<EndpointAddress, String> {
    if value.len() > CLUSTER_MANIFEST_MAX_URI_BYTES {
        return Err("endpoint URI exceeds size limit".to_string());
    }
    if value.bytes().any(|byte| byte.is_ascii_control()) {
        return Err("endpoint URI contains a control character".to_string());
    }
    if let Some(path) = value.strip_prefix("unix://") {
        if path.contains(['?', '#']) {
            return Err("Unix endpoint URI must not contain query or fragment".to_string());
        }
        return Ok(EndpointAddress::Unix(normalize_absolute_path(
            Path::new(path),
            "Unix endpoint path",
        )?));
    }
    let Some(authority) = value.strip_prefix("tcp://") else {
        return Err("endpoint URI must use unix:// or tcp://".to_string());
    };
    if authority.contains(['/', '?', '#', '@']) {
        return Err("TCP endpoint URI contains unsupported components".to_string());
    }
    let (host, port_text) = if let Some(rest) = authority.strip_prefix('[') {
        let (host, suffix) = rest
            .split_once(']')
            .ok_or_else(|| "TCP IPv6 endpoint is missing closing bracket".to_string())?;
        let port = suffix
            .strip_prefix(':')
            .ok_or_else(|| "TCP endpoint is missing port".to_string())?;
        let address = host
            .parse::<Ipv6Addr>()
            .map_err(|_| "TCP endpoint has an invalid IPv6 address".to_string())?;
        let canonical = address.to_string();
        if host != canonical {
            return Err("TCP endpoint IPv6 address is not canonical".to_string());
        }
        (canonical, port)
    } else {
        let (host, port) = authority
            .rsplit_once(':')
            .ok_or_else(|| "TCP endpoint is missing port".to_string())?;
        if host.contains(':') {
            return Err("TCP IPv6 endpoint must use brackets".to_string());
        }
        validate_tcp_dns_or_ipv4_host(host)?;
        (host.to_string(), port)
    };
    if !listener && matches!(host.as_str(), "0.0.0.0" | "::") {
        return Err("advertised TCP endpoint must not use an unspecified host".to_string());
    }
    let port = port_text
        .parse::<u16>()
        .map_err(|_| "TCP endpoint port is invalid".to_string())?;
    if port == 0 {
        return Err("TCP endpoint port must be nonzero".to_string());
    }
    Ok(EndpointAddress::Tcp { host, port })
}

fn validate_server_name(value: &str) -> Result<(), String> {
    if let Ok(address) = value.parse::<Ipv6Addr>() {
        if address.to_string() == value {
            return Ok(());
        }
        return Err("TLS server IPv6 address is not canonical".to_string());
    }
    validate_tcp_dns_or_ipv4_host(value)
}

fn validate_tcp_dns_or_ipv4_host(value: &str) -> Result<(), String> {
    if value.is_empty() || value.len() > CLUSTER_MANIFEST_MAX_ID_BYTES {
        return Err("TLS/TCP server name is invalid".to_string());
    }
    if let Ok(address) = value.parse::<Ipv4Addr>() {
        return if address.to_string() == value {
            Ok(())
        } else {
            Err("TCP IPv4 address is not canonical".to_string())
        };
    }
    if value.bytes().any(|byte| byte.is_ascii_uppercase())
        || value.split('.').any(|label| {
            label.is_empty()
                || label.len() > 63
                || !label
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
                || label.starts_with('-')
                || label.ends_with('-')
        })
    {
        return Err("TLS/TCP DNS name is not canonical".to_string());
    }
    Ok(())
}

fn normalize_absolute_path(path: &Path, field: &str) -> Result<PathBuf, String> {
    if !path.is_absolute() {
        return Err(format!("{field} must be absolute"));
    }
    if path.as_os_str().as_encoded_bytes().len() > CLUSTER_MANIFEST_MAX_PATH_BYTES {
        return Err(format!("{field} exceeds path length limit"));
    }
    let mut result = PathBuf::from("/");
    for component in path.components() {
        match component {
            Component::RootDir => {}
            Component::Normal(component) => result.push(component),
            Component::CurDir => {}
            Component::ParentDir => {
                if !result.pop() {
                    return Err(format!("{field} escapes filesystem root"));
                }
            }
            Component::Prefix(_) => {
                return Err(format!("{field} contains an unsupported path prefix"));
            }
        }
    }
    if result.as_os_str().as_encoded_bytes() != path.as_os_str().as_encoded_bytes() {
        return Err(format!("{field} is not canonical"));
    }
    if result == Path::new("/") {
        return Err(format!("{field} must not be filesystem root"));
    }
    Ok(result)
}

fn validate_file_reference(value: &str, field: &str) -> Result<(), String> {
    let path = value
        .strip_prefix("file:")
        .ok_or_else(|| format!("{field} must use file: with an absolute path"))?;
    normalize_absolute_path(Path::new(path), field)?;
    Ok(())
}

fn validate_identifier(value: &str, max_len: usize, field: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > max_len
        || !value.bytes().all(|byte| byte.is_ascii_graphic())
    {
        return Err(format!(
            "{field} must be nonempty printable non-space ASCII no longer than {max_len} bytes"
        ));
    }
    Ok(())
}

fn validate_timeout(value: u64, field: &str) -> Result<(), String> {
    if value == 0 || value > CLUSTER_MANIFEST_MAX_TIMEOUT_MS {
        return Err(format!(
            "{field} must be in 1..={CLUSTER_MANIFEST_MAX_TIMEOUT_MS}"
        ));
    }
    Ok(())
}

fn validate_collection_len(len: usize, field: &str) -> Result<(), String> {
    if len > CLUSTER_MANIFEST_MAX_COLLECTION_ITEMS {
        return Err(format!(
            "{field} exceeds {CLUSTER_MANIFEST_MAX_COLLECTION_ITEMS} entries"
        ));
    }
    Ok(())
}

fn require_nonzero<T>(value: T, field: &str) -> Result<(), String>
where
    T: PartialEq + From<u8>,
{
    if value == T::from(0) {
        return Err(format!("{field} must be nonzero"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ProcessRole;
    use std::fmt::Write as _;
    use std::fs::File;
    use std::io::Write;
    use std::os::unix::fs::symlink;

    fn standalone_runtime_environment() -> BTreeMap<&'static str, String> {
        BTreeMap::from([
            ("ARGMIN_ACCOUNT_ID", "111122223333".to_string()),
            ("ARGMIN_ACCESS_KEY_ID", "test-access-key".to_string()),
            (
                "ARGMIN_SECRET_ACCESS_KEY",
                "test-secret-access-key".to_string(),
            ),
            (
                "ARGMIN_SSE_S3_WRAPPING_KEY",
                "dGVzdC13cmFwcGluZy1rZXk=".to_string(),
            ),
        ])
    }

    fn write_manifest(contents: &str) -> (test_util::TempDir, PathBuf) {
        let dir = test_util::tempdir();
        let path = dir.path().join("cluster.toml");
        std::fs::write(&path, contents).unwrap();
        (dir, path)
    }

    fn private_dir(path: &Path) {
        std::fs::create_dir_all(path).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
    }

    fn validate_test_selected_host_filesystem(
        validated: &ValidatedStaticClusterManifest,
    ) -> Result<(), String> {
        validate_selected_host_filesystem_with_mount_validator(
            validated,
            |_path, _metadata, _label| Ok(()),
        )
    }

    fn standalone_manifest_on_mount(mount_path: &Path) -> String {
        standalone_manifest()
            .replace(
                "mount_path = \"/srv/argmin\"",
                &format!("mount_path = \"{}\"", mount_path.display()),
            )
            .replace(
                "state_path = \"/srv/argmin/control.state\"",
                &format!(
                    "state_path = \"{}\"",
                    mount_path.join("control.state").display()
                ),
            )
            .replace(
                "data_dir = \"/srv/argmin/data\"",
                &format!("data_dir = \"{}\"", mount_path.join("data").display()),
            )
    }

    fn replicated_manifest_with_host_one_mounts(control_mount: &Path, data_mount: &Path) -> String {
        replicated_manifest()
            .replace("/srv/argmin/control-1", control_mount.to_str().unwrap())
            .replace("/srv/argmin/data-1", data_mount.to_str().unwrap())
    }

    fn standalone_manifest() -> String {
        r#"
schema_version = 1
tls_identities = []
tls_trust_bundles = []
auth_credentials = []

[cluster]
id = "test-cluster"
topology_generation = 1
region = "us-east-1"

[deployment]
mode = "standalone"
failure_domain = "none"
failure_tolerance = 0
internal_auth = "disabled"

[storage]
pg_count = 16
ec_data_shards = 1
ec_parity_shards = 0
initial_cluster_epoch = 1

[raft]
max_append_entries = 64
max_append_bytes = 8388608
max_snapshot_bytes = 16777216

[[transport_profiles]]
id = "control"
max_frame_bytes = 8388608
max_connections = 64
connect_timeout_ms = 1000
io_timeout_ms = 5000

[[hosts]]
id = "host-1"
zone = "zone-a"
rack = "rack-1"

[[disks]]
id = "disk-1"
host_id = "host-1"
mount_path = "/srv/argmin"

[[processes]]
id = "all-1"
host_id = "host-1"
kind = "all-in-one"

[[authorities]]
id = "authority-1"
kind = "single"
process_id = "all-1"
disk_id = "disk-1"
state_path = "/srv/argmin/control.state"

[[storage_nodes]]
node_id = 1
process_id = "all-1"
disk_id = "disk-1"
data_dir = "/srv/argmin/data"

[[endpoints]]
id = "control-1"
owner_process_id = "all-1"
protocol = "control-plane"
priority = 10
listen = "unix:///run/argmin/control.sock"
advertise = "unix:///run/argmin/control.sock"
transport_profile_id = "control"

[[endpoints]]
id = "clock-1"
owner_process_id = "all-1"
protocol = "authority-clock-recovery"
priority = 10
listen = "unix:///run/argmin/clock.sock"
advertise = "unix:///run/argmin/clock.sock"
transport_profile_id = "control"

[[endpoints]]
id = "storage-1"
owner_process_id = "all-1"
protocol = "storage-rpc"
priority = 10
listen = "unix:///run/argmin/storage.sock"
advertise = "unix:///run/argmin/storage.sock"
transport_profile_id = "control"
"#
        .to_string()
    }

    fn replicated_manifest() -> String {
        let mut manifest = r#"
schema_version = 1

[cluster]
id = "replicated-cluster"
topology_generation = 7
region = "us-east-1"

[deployment]
mode = "replicated"
failure_domain = "host"
failure_tolerance = 1
internal_auth = "required"

[storage]
pg_count = 16
ec_data_shards = 2
ec_parity_shards = 1
initial_cluster_epoch = 1

[raft]
max_append_entries = 64
max_append_bytes = 8388608
max_snapshot_bytes = 15728640

[[transport_profiles]]
id = "internal"
max_frame_bytes = 16777216
max_connections = 64
connect_timeout_ms = 1000
io_timeout_ms = 5000

[[tls_trust_bundles]]
id = "cluster-ca"
ca_bundle_ref = "file:/run/argmin-secrets/cluster-ca.pem"
"#
        .to_string();

        for host_number in 1..=3 {
            writeln!(
                manifest,
                r#"
[[hosts]]
id = "host-{host_number}"
zone = "zone-a"
rack = "rack-{host_number}"

[[disks]]
id = "host-{host_number}-control"
host_id = "host-{host_number}"
mount_path = "/srv/argmin/control-{host_number}"

[[disks]]
id = "host-{host_number}-data"
host_id = "host-{host_number}"
mount_path = "/srv/argmin/data-{host_number}"

[[processes]]
id = "control-{host_number}"
host_id = "host-{host_number}"
kind = "control-plane"
admin_instance_id = "control-{host_number}-admin"

[[processes]]
id = "storage-{host_number}"
host_id = "host-{host_number}"
kind = "storage-node"

[[authorities]]
id = "authority-{host_number}"
kind = "raft-voter"
raft_node_id = {raft_node_id}
process_id = "control-{host_number}"
disk_id = "host-{host_number}-control"
state_path = "/srv/argmin/control-{host_number}/control.state"

[[storage_nodes]]
node_id = {host_number}
process_id = "storage-{host_number}"
disk_id = "host-{host_number}-data"
data_dir = "/srv/argmin/data-{host_number}/node"

[[tls_identities]]
id = "host-{host_number}-identity"
certificate_ref = "file:/run/argmin-secrets/host-{host_number}.crt"
private_key_ref = "file:/run/argmin-secrets/host-{host_number}.key"
"#,
                raft_node_id = 100 + host_number
            )
            .unwrap();

            for (protocol, endpoint_name, port) in [
                ("raft-peer", "raft", 7400 + host_number),
                ("control-plane", "control", 7500 + host_number),
                ("authority-clock-recovery", "clock", 7600 + host_number),
            ] {
                writeln!(
                    manifest,
                    r#"
[[endpoints]]
id = "{endpoint_name}-{host_number}"
owner_process_id = "control-{host_number}"
protocol = "{protocol}"
priority = 10
listen = "tcp://0.0.0.0:{port}"
advertise = "tcp://control-{host_number}.internal:{port}"
transport_profile_id = "internal"
tls_identity_id = "host-{host_number}-identity"
tls_trust_bundle_id = "cluster-ca"
tls_server_name = "control-{host_number}.internal"
"#
                )
                .unwrap();
            }
            writeln!(
                manifest,
                r#"
[[endpoints]]
id = "storage-{host_number}"
owner_process_id = "storage-{host_number}"
protocol = "storage-rpc"
priority = 10
listen = "tcp://0.0.0.0:{storage_port}"
advertise = "tcp://storage-{host_number}.internal:{storage_port}"
transport_profile_id = "internal"
tls_identity_id = "host-{host_number}-identity"
tls_trust_bundle_id = "cluster-ca"
tls_server_name = "storage-{host_number}.internal"
"#,
                storage_port = 7700 + host_number
            )
            .unwrap();

            for (principal, id_field, id_value, credential_name) in [
                (
                    "raft-peer",
                    "node_id",
                    (100 + host_number).to_string(),
                    format!("raft-{host_number}"),
                ),
                (
                    "storage-node",
                    "node_id",
                    host_number.to_string(),
                    format!("storage-{host_number}"),
                ),
                (
                    "admin",
                    "instance_id",
                    format!("\"control-{host_number}-admin\""),
                    format!("admin-{host_number}"),
                ),
            ] {
                writeln!(
                    manifest,
                    r#"
[[auth_credentials]]
principal = "{principal}"
{id_field} = {id_value}
credential_id = "{credential_name}"
credential_version = 1
use_for_signing = true
accept_from_ms = 0
secret_ref = "file:/run/argmin-secrets/{credential_name}.key"
"#
                )
                .unwrap();
            }
        }
        manifest
    }

    fn replace_once(input: &str, from: &str, to: &str) -> String {
        assert_eq!(input.matches(from).count(), 1, "fixture replacement count");
        input.replacen(from, to, 1)
    }

    fn assert_only_full_fingerprint_changes(
        baseline: &ValidatedStaticClusterManifest,
        changed: &ValidatedStaticClusterManifest,
    ) {
        assert_eq!(changed.topology_digest(), baseline.topology_digest());
        assert_eq!(
            changed.process_identity_digest(),
            baseline.process_identity_digest()
        );
        assert_ne!(
            changed.full_config_fingerprint(),
            baseline.full_config_fingerprint()
        );
    }

    fn assert_topology_identity_changes(
        baseline: &ValidatedStaticClusterManifest,
        changed: &ValidatedStaticClusterManifest,
    ) {
        assert_ne!(changed.topology_digest(), baseline.topology_digest());
        assert_ne!(
            changed.process_identity_digest(),
            baseline.process_identity_digest()
        );
        assert_ne!(
            changed.full_config_fingerprint(),
            baseline.full_config_fingerprint()
        );
    }

    #[test]
    fn static_cluster_manifest_parses_valid_standalone_shape() {
        let manifest = parse_static_cluster_manifest(&standalone_manifest(), "all-1").unwrap();
        assert_eq!(manifest.cluster_id(), "test-cluster");
        assert_eq!(manifest.topology_generation(), 1);
        assert_eq!(manifest.selected_process_id(), "all-1");
        assert_eq!(manifest.deployment_mode(), "standalone");
        assert_eq!(manifest.initial_pg_acting_sets().len(), 16);
        assert!(manifest
            .initial_pg_acting_sets()
            .iter()
            .all(|acting_set| acting_set == &[1]));
        let debug = format!("{manifest:?}");
        assert!(debug.contains("test-cluster"));
        assert!(!debug.contains("/srv/argmin"));
    }

    #[test]
    fn static_cluster_manifest_maps_standalone_process_to_legacy_runtime_config() {
        let manifest = parse_static_cluster_manifest(&standalone_manifest(), "all-1").unwrap();
        let mut environment = standalone_runtime_environment();
        environment.insert("ARGMIN_LISTEN_ADDR", "127.0.0.1:19000".to_string());
        environment.insert("ARGMIN_WORKERS", "7".to_string());

        let config = manifest
            .standalone_legacy_server_config(|key| environment.get(key).cloned())
            .unwrap();

        assert_eq!(config.process_role, ProcessRole::LegacyLocal);
        assert_eq!(config.listen_addr, "127.0.0.1:19000");
        assert_eq!(config.workers, 7);
        assert_eq!(config.host_id.as_deref(), Some("host-1"));
        assert_eq!(config.region, "us-east-1");
        assert_eq!(config.data_dir, "/srv/argmin");
        assert_eq!(config.pg_count, 16);
        assert_eq!(config.storage_cluster_epoch, 1);
        assert_eq!((config.ec_k, config.ec_m), (1, 0));
        assert_eq!(config.storage_node_ids, vec![1]);
        assert_eq!(config.storage_node_id, Some(1));
        assert_eq!(
            config.storage_node_data_dir.as_deref(),
            Some("/srv/argmin/data")
        );
        assert_eq!(config.storage_node_socket_path, None);
        assert!(config.storage_node_sockets.is_empty());
        assert_eq!(config.control_plane_state_path, None);
        assert_eq!(config.control_plane_socket_path, None);
        assert!(config.control_plane_client_socket_paths.is_empty());
    }

    #[test]
    fn static_cluster_standalone_runtime_does_not_start_unserved_refresh_path() {
        let dir = test_util::tempdir();
        let disk_path = dir.path().join("disk");
        let socket_path = dir.path().join("run");
        std::fs::create_dir_all(&disk_path).unwrap();
        let source = standalone_manifest()
            .replace("/srv/argmin", disk_path.to_str().unwrap())
            .replace("/run/argmin", socket_path.to_str().unwrap());
        let manifest = parse_static_cluster_manifest(&source, "all-1").unwrap();
        let environment = standalone_runtime_environment();
        let config = manifest
            .standalone_legacy_server_config(|key| environment.get(key).cloned())
            .unwrap();
        let ec_config = EcConfig::new(config.ec_k, config.ec_m).unwrap();

        let cluster = crate::build_legacy_local_storage_cluster(&config, &ec_config).unwrap();
        let handle = storage::StorageClusterRuntimeMapHandle::new(cluster);

        assert!(crate::maybe_spawn_frontend_control_plane_refresh_loop(handle, &config).is_none());
        assert!(!socket_path.exists());
    }

    #[test]
    fn static_cluster_runtime_loader_requires_complete_file_mode_selection() {
        let environment = standalone_runtime_environment();

        assert!(
            load_server_config_from_inputs(Some(Path::new("/cluster.toml")), None, |key| {
                environment.get(key).cloned()
            })
            .unwrap_err()
            .contains("ARGMIN_PROCESS_ID is required")
        );
        assert!(load_server_config_from_inputs(None, Some("all-1"), |key| {
            environment.get(key).cloned()
        })
        .unwrap_err()
        .contains("ARGMIN_CLUSTER_CONFIG_PATH is required"));
    }

    #[test]
    fn static_cluster_runtime_loader_rejects_mixed_cluster_environment() {
        let (_dir, path) = write_manifest(&standalone_manifest());
        let mut environment = standalone_runtime_environment();
        environment.insert("ARGMIN_PG_COUNT", "999".to_string());

        let error = load_server_config_from_inputs(Some(&path), Some("all-1"), |key| {
            environment.get(key).cloned()
        })
        .unwrap_err();

        assert_eq!(
            error,
            "ARGMIN_PG_COUNT cannot be set when ARGMIN_CLUSTER_CONFIG_PATH is active"
        );
        assert!(!error.contains("999"));
    }

    #[test]
    fn static_cluster_runtime_loader_keeps_env_only_compatibility_mode() {
        let mut environment = standalone_runtime_environment();
        environment.insert("ARGMIN_PG_COUNT", "3".to_string());
        environment.insert("ARGMIN_EC_K", "1".to_string());
        environment.insert("ARGMIN_EC_M", "0".to_string());
        environment.insert("ARGMIN_LOCAL_NODE_COUNT", "1".to_string());

        let config =
            load_server_config_from_inputs(None, None, |key| environment.get(key).cloned())
                .unwrap();

        assert_eq!(config.process_role, ProcessRole::LegacyLocal);
        assert_eq!(config.pg_count, 3);
        assert_eq!(config.storage_node_ids, vec![0]);
    }

    #[test]
    fn static_cluster_runtime_loader_validates_selected_host_filesystem() {
        let dir = test_util::tempdir();
        let mount_path = dir.path().join("disk");
        private_dir(&mount_path);
        let manifest = standalone_manifest_on_mount(&mount_path);
        let manifest_path = dir.path().join("cluster.toml");
        std::fs::write(&manifest_path, manifest).unwrap();
        let environment = standalone_runtime_environment();

        let config = load_server_config_from_inputs_with_filesystem_validator(
            Some(&manifest_path),
            Some("all-1"),
            |key| environment.get(key).cloned(),
            |_manifest| Ok(()),
        )
        .unwrap();

        assert_eq!(
            config.storage_node_data_dir.as_deref(),
            Some(mount_path.join("data").to_str().unwrap())
        );
    }

    #[test]
    fn static_cluster_runtime_loader_rejects_unmounted_selected_host_disk() {
        let dir = test_util::tempdir();
        let mount_path = dir.path().join("unmounted-disk");
        private_dir(&mount_path);
        let manifest_path = dir.path().join("cluster.toml");
        std::fs::write(&manifest_path, standalone_manifest_on_mount(&mount_path)).unwrap();
        let environment = standalone_runtime_environment();

        let error = load_server_config_from_inputs(Some(&manifest_path), Some("all-1"), |key| {
            environment.get(key).cloned()
        })
        .unwrap_err();

        assert!(
            error.contains("must be an exact distinct-device mount boundary"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn static_cluster_mount_boundary_requires_distinct_device() {
        validate_distinct_mount_devices(11, 10, "test mount").unwrap();
        assert!(validate_distinct_mount_devices(10, 10, "test mount")
            .unwrap_err()
            .contains("exact distinct-device mount boundary"));
    }

    #[test]
    fn static_cluster_runtime_loader_rejects_profiles_not_yet_runtime_mapped() {
        let (_dir, replicated_path) = write_manifest(&replicated_manifest());
        let environment = standalone_runtime_environment();
        let error =
            load_server_config_from_inputs(Some(&replicated_path), Some("control-1"), |key| {
                environment.get(key).cloned()
            })
            .unwrap_err();
        assert!(error.contains("replicated cluster manifests require"));

        let credentialed = format!(
            "{}{}",
            replace_once(&standalone_manifest(), "auth_credentials = []", ""),
            r#"
[[auth_credentials]]
principal = "storage-node"
node_id = 1
credential_id = "storage-1"
credential_version = 1
use_for_signing = false
accept_from_ms = 0
secret_ref = "file:/run/argmin-secrets/storage-1.key"
"#
        );
        let (_dir, credentialed_path) = write_manifest(&credentialed);
        let error =
            load_server_config_from_inputs(Some(&credentialed_path), Some("all-1"), |key| {
                environment.get(key).cloned()
            })
            .unwrap_err();
        assert!(error.contains("no unresolved secret references"));
    }

    #[test]
    fn static_cluster_manifest_parses_valid_three_host_replicated_shape() {
        let manifest = parse_static_cluster_manifest(&replicated_manifest(), "control-2").unwrap();
        assert_eq!(manifest.cluster_id(), "replicated-cluster");
        assert_eq!(manifest.topology_generation(), 7);
        assert_eq!(manifest.selected_process_id(), "control-2");
        assert_eq!(manifest.deployment_mode(), "replicated");
        assert_eq!(manifest.initial_pg_acting_sets().len(), 16);
        assert!(manifest.initial_pg_acting_sets().iter().all(|acting_set| {
            acting_set.len() == 3
                && acting_set.iter().copied().collect::<BTreeSet<_>>() == BTreeSet::from([1, 2, 3])
        }));
    }

    #[test]
    fn static_cluster_manifest_identity_has_stable_vectors() {
        let standalone = parse_static_cluster_manifest(&standalone_manifest(), "all-1").unwrap();
        assert_eq!(
            (
                standalone.topology_digest(),
                standalone.process_identity_digest(),
                standalone.full_config_fingerprint(),
            ),
            (
                "0cb10dea5a8fb3748ead9a20bcb94dc6ab6792c1aaca0df08f4d6198f2d0e7ad",
                "2a416fa124fc47f744e82a8d0aced25db403b21e571b25e453709f82839ae294",
                "b628d1d43b3570afad9796232132c4314bf867b2b0c1e77ee0d29cfafde959af",
            )
        );

        let replicated =
            parse_static_cluster_manifest(&replicated_manifest(), "control-2").unwrap();
        assert_eq!(
            (
                replicated.topology_digest(),
                replicated.process_identity_digest(),
                replicated.full_config_fingerprint(),
            ),
            (
                "7350e19d22bdd52da757cc449d8c68ae38ebdc3118b0141b6a570346a0a0f5fb",
                "8d6b71bed787aeb1f206d54118c50e81d48f6bbc4176aae7bea7dedcc53f1a3d",
                "14fefe4097e94d42485566c17243f57b175a84dd033905f4f5cd5ebbe09f0c42",
            )
        );
    }

    #[test]
    fn static_cluster_manifest_identity_is_collection_order_independent() {
        let input: StaticClusterManifestInput = toml::from_str(&replicated_manifest()).unwrap();
        let canonical = validate_static_cluster_manifest(input.clone(), "control-2").unwrap();
        let mut reversed = input;
        reversed.transport_profiles.reverse();
        reversed.hosts.reverse();
        reversed.disks.reverse();
        reversed.processes.reverse();
        reversed.authorities.reverse();
        reversed.storage_nodes.reverse();
        reversed.endpoints.reverse();
        reversed.tls_identities.reverse();
        reversed.tls_trust_bundles.reverse();
        reversed.auth_credentials.reverse();
        let reversed = validate_static_cluster_manifest(reversed, "control-2").unwrap();

        assert_eq!(reversed.topology_digest(), canonical.topology_digest());
        assert_eq!(
            reversed.process_identity_digest(),
            canonical.process_identity_digest()
        );
        assert_eq!(
            reversed.full_config_fingerprint(),
            canonical.full_config_fingerprint()
        );
    }

    #[test]
    fn static_cluster_manifest_topology_identity_covers_compatibility_fields() {
        let source = replicated_manifest();
        let baseline = parse_static_cluster_manifest(&source, "control-1").unwrap();
        for changed in [
            replace_once(
                &source,
                "id = \"replicated-cluster\"",
                "id = \"replicated-cluster-next\"",
            ),
            replace_once(
                &source,
                "topology_generation = 7",
                "topology_generation = 8",
            ),
            replace_once(
                &source,
                "failure_domain = \"host\"",
                "failure_domain = \"disk\"",
            ),
            replace_once(&source, "pg_count = 16", "pg_count = 17"),
            replace_once(&source, "rack = \"rack-1\"", "rack = \"rack-next\""),
            source.replace("node_id = 101", "node_id = 111"),
            source.replace("control-1-admin", "control-1-admin-next"),
            source.replace("7401", "7491"),
            replace_once(
                &source,
                "max_snapshot_bytes = 15728640",
                "max_snapshot_bytes = 15728639",
            ),
        ] {
            let changed = parse_static_cluster_manifest(&changed, "control-1").unwrap();
            assert_topology_identity_changes(&baseline, &changed);
        }
    }

    #[test]
    fn static_cluster_manifest_durable_identity_excludes_rotation_and_local_tuning() {
        let source = replicated_manifest();
        let baseline = parse_static_cluster_manifest(&source, "control-1").unwrap();
        for changed in [
            replace_once(
                &source,
                "credential_id = \"raft-1\"",
                "credential_id = \"raft-1-rotated\"",
            ),
            replace_once(
                &source,
                "credential_id = \"raft-1\"\ncredential_version = 1",
                "credential_id = \"raft-1\"\ncredential_version = 2",
            ),
            replace_once(
                &source,
                "credential_id = \"raft-1\"\ncredential_version = 1\nuse_for_signing = true\naccept_from_ms = 0",
                "credential_id = \"raft-1\"\ncredential_version = 1\nuse_for_signing = true\naccept_from_ms = 1",
            ),
            replace_once(
                &source,
                "secret_ref = \"file:/run/argmin-secrets/raft-1.key\"",
                "secret_ref = \"file:/run/argmin-secrets/raft-1-next.key\"",
            ),
            replace_once(
                &source,
                "certificate_ref = \"file:/run/argmin-secrets/host-1.crt\"",
                "certificate_ref = \"file:/run/argmin-secrets/host-1-next.crt\"",
            ),
            replace_once(
                &source,
                "state_path = \"/srv/argmin/control-1/control.state\"",
                "state_path = \"/srv/argmin/control-1/relocated.state\"",
            ),
            replace_once(
                &source,
                "listen = \"tcp://0.0.0.0:7401\"",
                "listen = \"tcp://127.0.0.1:7401\"",
            ),
            replace_once(
                &source,
                "connect_timeout_ms = 1000",
                "connect_timeout_ms = 2000",
            ),
            replace_once(&source, "region = \"us-east-1\"", "region = \"us-west-2\""),
        ] {
            let changed = parse_static_cluster_manifest(&changed, "control-1").unwrap();
            assert_only_full_fingerprint_changes(&baseline, &changed);
        }

        let verify_only = format!(
            "{}{}",
            replace_once(&standalone_manifest(), "auth_credentials = []", ""),
            r#"
[[auth_credentials]]
principal = "storage-node"
node_id = 1
credential_id = "storage-1"
credential_version = 1
use_for_signing = false
accept_from_ms = 10
accept_until_ms = 20
secret_ref = "file:/run/argmin-secrets/storage-1.key"
"#
        );
        let signing = replace_once(
            &verify_only,
            "use_for_signing = false",
            "use_for_signing = true",
        );
        let verify_only = parse_static_cluster_manifest(&verify_only, "all-1").unwrap();
        let signing = parse_static_cluster_manifest(&signing, "all-1").unwrap();
        assert_only_full_fingerprint_changes(&verify_only, &signing);
    }

    #[test]
    fn static_cluster_manifest_process_identity_is_selection_specific() {
        let first = parse_static_cluster_manifest(&replicated_manifest(), "control-1").unwrap();
        let second = parse_static_cluster_manifest(&replicated_manifest(), "control-2").unwrap();
        assert_eq!(first.topology_digest(), second.topology_digest());
        assert_eq!(
            first.full_config_fingerprint(),
            second.full_config_fingerprint()
        );
        assert_ne!(
            first.process_identity_digest(),
            second.process_identity_digest()
        );
    }

    #[test]
    fn static_cluster_manifest_raft_policy_selects_globally_reachable_peer_endpoint() {
        let manifest = format!(
            "{}{}",
            replicated_manifest(),
            r#"
[[endpoints]]
id = "raft-local-1"
owner_process_id = "control-1"
protocol = "raft-peer"
priority = 5
listen = "unix:///run/argmin/raft-1.sock"
advertise = "unix:///run/argmin/raft-1.sock"
transport_profile_id = "internal"
"#
        );
        let validated = parse_static_cluster_manifest(&manifest, "control-1").unwrap();
        assert_eq!(
            validated
                .canonical_raft_peer_endpoints()
                .get(&101)
                .map(String::as_str),
            Some("tcp://control-1.internal:7401")
        );
    }

    #[test]
    fn static_cluster_manifest_rejects_unknown_and_duplicate_fields() {
        let unknown = replace_once(
            &standalone_manifest(),
            "topology_generation = 1",
            "topology_generation = 1\nunknown = true",
        );
        assert!(parse_static_cluster_manifest(&unknown, "all-1")
            .unwrap_err()
            .contains("unknown field"));

        let duplicate = replace_once(
            &standalone_manifest(),
            "topology_generation = 1",
            "topology_generation = 1\ntopology_generation = 2",
        );
        assert!(parse_static_cluster_manifest(&duplicate, "all-1")
            .unwrap_err()
            .contains("duplicate key"));
    }

    #[test]
    fn static_cluster_manifest_redacts_toml_field_and_enum_values() {
        let sentinel = "DO-NOT-LOG-THIS-MANIFEST-VALUE";
        let unknown_field = replace_once(
            &standalone_manifest(),
            "topology_generation = 1",
            &format!("topology_generation = 1\n{sentinel} = true"),
        );
        let field_error = parse_static_cluster_manifest(&unknown_field, "all-1").unwrap_err();
        assert!(field_error.contains("unknown field"));
        assert!(!field_error.contains(sentinel));

        let unknown_mode = replace_once(
            &standalone_manifest(),
            "mode = \"standalone\"",
            &format!("mode = \"{sentinel}\""),
        );
        let enum_error = parse_static_cluster_manifest(&unknown_mode, "all-1").unwrap_err();
        assert!(enum_error.contains("unknown variant"));
        assert!(!enum_error.contains(sentinel));
    }

    #[test]
    fn static_cluster_manifest_rejects_unknown_enum_and_duplicate_ids() {
        let unknown_mode = replace_once(
            &standalone_manifest(),
            "mode = \"standalone\"",
            "mode = \"future-mode\"",
        );
        assert!(parse_static_cluster_manifest(&unknown_mode, "all-1")
            .unwrap_err()
            .contains("unknown variant"));

        let duplicate_endpoint = replace_once(
            &standalone_manifest(),
            "id = \"clock-1\"",
            "id = \"control-1\"",
        );
        assert!(parse_static_cluster_manifest(&duplicate_endpoint, "all-1")
            .unwrap_err()
            .contains("duplicate endpoint id"));

        let duplicate_disk_path = format!(
            "{}\n[[disks]]\nid = \"disk-2\"\nhost_id = \"host-1\"\nmount_path = \"/srv/argmin\"\n",
            standalone_manifest()
        );
        assert!(parse_static_cluster_manifest(&duplicate_disk_path, "all-1")
            .unwrap_err()
            .contains("duplicate disk mount path"));
    }

    #[test]
    fn static_cluster_manifest_rejects_pg_ownership_field() {
        let manifest = replace_once(
            &standalone_manifest(),
            "data_dir = \"/srv/argmin/data\"",
            "data_dir = \"/srv/argmin/data\"\npg_ids = \"all\"",
        );
        assert!(parse_static_cluster_manifest(&manifest, "all-1")
            .unwrap_err()
            .contains("unknown field"));
    }

    #[test]
    fn static_cluster_manifest_rejects_missing_selected_process_and_dangling_reference() {
        assert!(
            parse_static_cluster_manifest(&standalone_manifest(), "missing")
                .unwrap_err()
                .contains("absent")
        );
        let dangling = replace_once(
            &standalone_manifest(),
            "disk_id = \"disk-1\"\nstate_path",
            "disk_id = \"missing\"\nstate_path",
        );
        assert!(parse_static_cluster_manifest(&dangling, "all-1")
            .unwrap_err()
            .contains("unknown disk"));
    }

    #[test]
    fn static_cluster_manifest_rejects_tcp_without_tls_or_auth() {
        let tcp = replace_once(
            &standalone_manifest(),
            "listen = \"unix:///run/argmin/control.sock\"\nadvertise = \"unix:///run/argmin/control.sock\"",
            "listen = \"tcp://0.0.0.0:7501\"\nadvertise = \"tcp://control.internal:7501\"",
        );
        assert!(parse_static_cluster_manifest(&tcp, "all-1")
            .unwrap_err()
            .contains("tls_identity_id"));
    }

    #[test]
    fn static_cluster_manifest_binds_tcp_server_name_to_advertised_host() {
        let mismatch = replicated_manifest().replacen(
            "tls_server_name = \"control-1.internal\"",
            "tls_server_name = \"wrong.internal\"",
            1,
        );
        assert!(parse_static_cluster_manifest(&mismatch, "control-1")
            .unwrap_err()
            .contains("must match its advertised host"));

        let invalid_ipv6 = replicated_manifest().replacen(
            "advertise = \"tcp://control-1.internal:7401\"",
            "advertise = \"tcp://[abcd::not-an-address]:7401\"",
            1,
        );
        assert!(parse_static_cluster_manifest(&invalid_ipv6, "control-1")
            .unwrap_err()
            .contains("invalid IPv6"));
    }

    #[test]
    fn static_cluster_manifest_scopes_paths_by_host() {
        let parsed = parse_endpoint_address("unix:///run/argmin/control.sock", true).unwrap();
        assert_eq!(
            parsed,
            EndpointAddress::Unix(PathBuf::from("/run/argmin/control.sock"))
        );

        let repeated_remote_paths = replicated_manifest()
            .replace("/srv/argmin/control-2", "/srv/argmin/control-1")
            .replace("/srv/argmin/data-2", "/srv/argmin/data-1");
        parse_static_cluster_manifest(&repeated_remote_paths, "control-1").unwrap();
    }

    #[test]
    fn static_cluster_filesystem_validation_probes_only_selected_host() {
        let dir = test_util::tempdir();
        let control_mount = dir.path().join("control");
        let data_mount = dir.path().join("data");
        private_dir(&control_mount);
        private_dir(&data_mount);
        let manifest = replicated_manifest_with_host_one_mounts(&control_mount, &data_mount);
        let validated = parse_static_cluster_manifest(&manifest, "control-1").unwrap();

        validate_test_selected_host_filesystem(&validated).unwrap();
    }

    #[test]
    fn static_cluster_filesystem_validation_rejects_local_symlink_traversal() {
        let dir = test_util::tempdir();
        let mount_path = dir.path().join("disk");
        let real_data = mount_path.join("real-data");
        private_dir(&mount_path);
        private_dir(&real_data);
        symlink(&real_data, mount_path.join("linked-data")).unwrap();
        let manifest = standalone_manifest_on_mount(&mount_path).replace(
            &format!("data_dir = \"{}\"", mount_path.join("data").display()),
            &format!(
                "data_dir = \"{}\"",
                mount_path.join("linked-data/node").display()
            ),
        );
        let validated = parse_static_cluster_manifest(&manifest, "all-1").unwrap();

        let error = validate_test_selected_host_filesystem(&validated).unwrap_err();

        assert!(
            error.contains("must not traverse a symlink"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn static_cluster_manifest_rejects_symlink_parent_component_escape() {
        let dir = test_util::tempdir();
        let mount_path = dir.path().join("disk");
        let outside_child = dir.path().join("outside/child");
        private_dir(&mount_path);
        private_dir(&outside_child);
        symlink(&outside_child, mount_path.join("jump")).unwrap();
        let noncanonical_data_dir = mount_path.join("jump/../node");
        let manifest = standalone_manifest_on_mount(&mount_path).replace(
            &format!("data_dir = \"{}\"", mount_path.join("data").display()),
            &format!("data_dir = \"{}\"", noncanonical_data_dir.display()),
        );

        let error = parse_static_cluster_manifest(&manifest, "all-1").unwrap_err();

        assert!(
            error.contains("storage data path is not canonical"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn static_cluster_filesystem_validation_rejects_missing_or_symlinked_mount() {
        let dir = test_util::tempdir();
        let missing_mount = dir.path().join("missing");
        let missing =
            parse_static_cluster_manifest(&standalone_manifest_on_mount(&missing_mount), "all-1")
                .unwrap();
        assert!(validate_test_selected_host_filesystem(&missing)
            .unwrap_err()
            .contains("mount path cannot be inspected"));

        let real_mount = dir.path().join("real");
        let linked_mount = dir.path().join("linked");
        private_dir(&real_mount);
        symlink(&real_mount, &linked_mount).unwrap();
        let linked =
            parse_static_cluster_manifest(&standalone_manifest_on_mount(&linked_mount), "all-1")
                .unwrap();
        assert!(validate_test_selected_host_filesystem(&linked)
            .unwrap_err()
            .contains("mount path must be a non-symlink directory"));
    }

    #[test]
    fn static_cluster_filesystem_validation_rejects_insecure_local_path() {
        let dir = test_util::tempdir();
        let mount_path = dir.path().join("disk");
        let data_path = mount_path.join("data");
        private_dir(&mount_path);
        private_dir(&data_path);
        std::fs::set_permissions(&data_path, std::fs::Permissions::from_mode(0o770)).unwrap();
        let manifest = standalone_manifest_on_mount(&mount_path);
        let validated = parse_static_cluster_manifest(&manifest, "all-1").unwrap();

        let error = validate_test_selected_host_filesystem(&validated).unwrap_err();

        assert!(error.contains("must not be writable by group or other users"));
    }

    #[test]
    fn static_cluster_filesystem_validation_rejects_wrong_existing_path_type() {
        let dir = test_util::tempdir();
        let mount_path = dir.path().join("disk");
        private_dir(&mount_path);
        private_dir(&mount_path.join("control.state"));
        let manifest = standalone_manifest_on_mount(&mount_path);
        let validated = parse_static_cluster_manifest(&manifest, "all-1").unwrap();

        let error = validate_test_selected_host_filesystem(&validated).unwrap_err();

        assert!(
            error.contains("state path must be a regular file"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn static_cluster_manifest_rejects_noncanonical_endpoint_uris() {
        let unix = standalone_manifest().replacen(
            "unix:///run/argmin/control.sock",
            "unix:///run/./argmin/control.sock",
            1,
        );
        assert!(parse_static_cluster_manifest(&unix, "all-1")
            .unwrap_err()
            .contains("not canonical"));

        let tcp = replace_once(
            &replicated_manifest(),
            "tcp://0.0.0.0:7401",
            "tcp://0.0.0.0:07401",
        );
        assert!(parse_static_cluster_manifest(&tcp, "control-1")
            .unwrap_err()
            .contains("URI is not canonical"));
    }

    #[test]
    fn static_cluster_manifest_rejects_relative_and_escaping_state_paths() {
        let relative = replace_once(
            &standalone_manifest(),
            "state_path = \"/srv/argmin/control.state\"",
            "state_path = \"control.state\"",
        );
        assert!(parse_static_cluster_manifest(&relative, "all-1")
            .unwrap_err()
            .contains("must be absolute"));

        let escaping = replace_once(
            &standalone_manifest(),
            "state_path = \"/srv/argmin/control.state\"",
            "state_path = \"/srv/argmin/../control.state\"",
        );
        assert!(parse_static_cluster_manifest(&escaping, "all-1")
            .unwrap_err()
            .contains("not canonical"));
    }

    #[test]
    fn static_cluster_manifest_rejects_replicated_policy_contradictions() {
        let undersized_append_batch = replace_once(
            &replicated_manifest(),
            "max_append_entries = 64",
            "max_append_entries = 32",
        );
        assert!(
            parse_static_cluster_manifest(&undersized_append_batch, "control-1")
                .unwrap_err()
                .contains("production replication batch size")
        );

        let undersized_append_payload = replace_once(
            &replicated_manifest(),
            "max_append_bytes = 8388608",
            "max_append_bytes = 4194304",
        );
        assert!(
            parse_static_cluster_manifest(&undersized_append_payload, "control-1")
                .unwrap_err()
                .contains("production replication payload size")
        );

        let unauthenticated = replace_once(
            &replicated_manifest(),
            "internal_auth = \"required\"",
            "internal_auth = \"disabled\"",
        );
        assert!(parse_static_cluster_manifest(&unauthenticated, "control-1")
            .unwrap_err()
            .contains("requires internal authentication"));

        let insufficient_parity = replace_once(
            &replicated_manifest(),
            "ec_parity_shards = 1",
            "ec_parity_shards = 0",
        );
        assert!(
            parse_static_cluster_manifest(&insufficient_parity, "control-1")
                .unwrap_err()
                .contains("parity must cover")
        );

        let incompatible_frame = replace_once(
            &replicated_manifest(),
            "max_frame_bytes = 16777216",
            "max_frame_bytes = 4194304",
        );
        assert!(
            parse_static_cluster_manifest(&incompatible_frame, "control-1")
                .unwrap_err()
                .contains("cannot carry max append bytes")
        );

        let below_production_frame = replace_once(
            &replicated_manifest(),
            "max_frame_bytes = 16777216",
            "max_frame_bytes = 9437184",
        );
        assert!(
            parse_static_cluster_manifest(&below_production_frame, "control-1")
                .unwrap_err()
                .contains("incompatible with production replication")
        );

        let missing_admin_identity = replace_once(
            &replicated_manifest(),
            "admin_instance_id = \"control-1-admin\"",
            "maintenance_instance_id = \"control-1-maintenance\"",
        );
        assert!(
            parse_static_cluster_manifest(&missing_admin_identity, "control-1")
                .unwrap_err()
                .contains("requires admin_instance_id")
        );

        let all_in_one = replace_once(
            &replicated_manifest(),
            "id = \"control-1\"\nhost_id = \"host-1\"\nkind = \"control-plane\"",
            "id = \"control-1\"\nhost_id = \"host-1\"\nkind = \"all-in-one\"",
        );
        assert!(parse_static_cluster_manifest(&all_in_one, "control-1")
            .unwrap_err()
            .contains("only in standalone"));
    }

    #[test]
    fn static_cluster_manifest_rejects_impossible_snapshot_transport() {
        let raw_append_only = replace_once(
            &replicated_manifest(),
            "max_frame_bytes = 16777216",
            "max_frame_bytes = 8388608",
        );
        assert!(parse_static_cluster_manifest(&raw_append_only, "control-1")
            .unwrap_err()
            .contains("identity and authentication overhead"));

        let no_overhead_space = replace_once(
            &replicated_manifest(),
            "max_snapshot_bytes = 15728640",
            "max_snapshot_bytes = 16777216",
        );
        assert!(
            parse_static_cluster_manifest(&no_overhead_space, "control-1")
                .unwrap_err()
                .contains("plus metadata and authentication overhead")
        );

        let above_snapshot_limit = replace_once(
            &replicated_manifest(),
            "max_snapshot_bytes = 15728640",
            "max_snapshot_bytes = 67108864",
        );
        assert!(
            parse_static_cluster_manifest(&above_snapshot_limit, "control-1")
                .unwrap_err()
                .contains("max_snapshot_bytes must be")
        );
    }

    #[test]
    fn static_cluster_manifest_rejects_cross_role_durable_path_collision() {
        let collision = replace_once(
            &standalone_manifest(),
            "data_dir = \"/srv/argmin/data\"",
            "data_dir = \"/srv/argmin/control.state\"",
        );
        assert!(parse_static_cluster_manifest(&collision, "all-1")
            .unwrap_err()
            .contains("duplicate durable state/data path"));
    }

    #[test]
    fn static_cluster_manifest_rejects_duplicate_credential_identity() {
        let duplicate = format!(
            "{}\n{}",
            replicated_manifest(),
            r#"
[[auth_credentials]]
principal = "raft-peer"
node_id = 101
credential_id = "raft-1"
credential_version = 1
use_for_signing = false
accept_from_ms = 0
secret_ref = "file:/run/argmin-secrets/duplicate.key"
"#
        );
        assert!(parse_static_cluster_manifest(&duplicate, "control-1")
            .unwrap_err()
            .contains("duplicate credential identity"));
    }

    #[test]
    fn static_cluster_manifest_parse_error_does_not_echo_values() {
        let secret = "DO-NOT-LOG-THIS-SECRET";
        let malformed = format!("{}\ninline_secret = \"{secret}\"\n", standalone_manifest());
        let error = parse_static_cluster_manifest(&malformed, "all-1").unwrap_err();
        assert!(!error.contains(secret));
    }

    #[test]
    fn static_cluster_manifest_loader_rejects_relative_and_oversized_files() {
        assert!(
            load_static_cluster_manifest(Path::new("relative.toml"), "all-1")
                .unwrap_err()
                .contains("absolute")
        );

        let dir = test_util::tempdir();
        let path = dir.path().join("cluster.toml");
        let mut file = File::create(&path).unwrap();
        file.write_all(&vec![b'x'; CLUSTER_MANIFEST_MAX_BYTES as usize + 1])
            .unwrap();
        file.sync_all().unwrap();
        assert!(load_static_cluster_manifest(&path, "all-1")
            .unwrap_err()
            .contains("exceeds"));
    }

    #[test]
    fn static_cluster_manifest_loader_rejects_non_utf8_and_symlink() {
        let dir = test_util::tempdir();
        let invalid_utf8_path = dir.path().join("invalid-utf8.toml");
        let mut file = File::create(&invalid_utf8_path).unwrap();
        file.write_all(&[0xff]).unwrap();
        file.sync_all().unwrap();
        assert!(load_static_cluster_manifest(&invalid_utf8_path, "all-1")
            .unwrap_err()
            .contains("UTF-8"));

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            let manifest_path = dir.path().join("cluster.toml");
            let link_path = dir.path().join("cluster-link.toml");
            std::fs::write(&manifest_path, standalone_manifest()).unwrap();
            symlink(&manifest_path, &link_path).unwrap();
            assert!(load_static_cluster_manifest(&link_path, "all-1")
                .unwrap_err()
                .contains("no-follow"));
        }
    }
}
