#[path = "runtime_host.rs"]
mod runtime_host;

use futures::StreamExt;
use libp2p::{
    autonat, connection_limits, dcutr,
    gossipsub::{self, IdentTopic, MessageAuthenticity, ValidationMode},
    identify,
    identity::{self, Keypair},
    kad,
    multiaddr::Protocol,
    noise, ping, relay, rendezvous,
    swarm::{NetworkBehaviour, Swarm, SwarmEvent},
    tcp, yamux, Multiaddr, PeerId, StreamProtocol, SwarmBuilder,
};
use reqwest::{Client as HttpClient, Url as HttpUrl};
use sha2::{Digest, Sha256};
use sha3::Keccak256;
use std::collections::{HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::str::FromStr;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tauri::{Emitter, Manager, RunEvent};
use tauri_plugin_updater::UpdaterExt;
use tokio::process::Command as TokioCommand;
use tokio::sync::{mpsc, oneshot};

use self::runtime_host::{ensure_local_runtime_host, LocalRuntimeHostState};
const COMPOSE_SYNAPSE_COLLECTION: &str = "compose";
const LOCAL_AGENT_HEARTBEAT_POLL_MS: u64 = 3_000;
const LOCAL_AGENT_HEARTBEAT_OK_TOKEN: &str = "HEARTBEAT_OK";

#[derive(Debug, serde::Serialize)]
struct LocalPaths {
    base_dir: String,
    state_file: String,
    agents_dir: String,
    skills_dir: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct DaemonPermissionPolicy {
    shell: String,
    filesystem_read: String,
    filesystem_write: String,
    filesystem_edit: String,
    filesystem_delete: String,
    camera: String,
    microphone: String,
    network: String,
}

impl Default for DaemonPermissionPolicy {
    fn default() -> Self {
        Self {
            shell: "deny".to_string(),
            filesystem_read: "deny".to_string(),
            filesystem_write: "deny".to_string(),
            filesystem_edit: "deny".to_string(),
            filesystem_delete: "deny".to_string(),
            camera: "deny".to_string(),
            microphone: "deny".to_string(),
            network: "deny".to_string(),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct DaemonAgentState {
    agent_wallet: String,
    runtime_id: Option<String>,
    desired_running: bool,
    running: bool,
    status: String,
    dna_hash: String,
    chain_id: u32,
    model_id: String,
    mcp_tools_hash: String,
    agent_card_cid: String,
    #[serde(default)]
    desired_permissions: DaemonPermissionPolicy,
    #[serde(default)]
    permissions: DaemonPermissionPolicy,
    logs_cursor: usize,
    last_error: Option<String>,
    updated_at: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct DaemonStateFile {
    version: u32,
    agents: HashMap<String, DaemonAgentState>,
}

impl Default for DaemonStateFile {
    fn default() -> Self {
        Self {
            version: 1,
            agents: HashMap::new(),
        }
    }
}

#[derive(Default)]
struct LocalDaemonState {
    state: Mutex<DaemonStateFile>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct DaemonInstallPayload {
    agent_wallet: String,
    agent_card_cid: String,
    chain_id: u32,
    model_id: String,
    mcp_tools_hash: String,
    dna_hash: String,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct DaemonLogTail {
    lines: Vec<String>,
    cursor: usize,
}

#[derive(Default)]
struct PendingDeepLinks(Mutex<Vec<String>>);

#[derive(Default)]
struct MeshRuntimeState {
    status: Mutex<MeshRuntimeStatus>,
    stop_tx: Mutex<Option<oneshot::Sender<()>>>,
    command_tx: Mutex<Option<mpsc::UnboundedSender<MeshLoopCommand>>>,
}

#[derive(Default)]
struct SessionBudgetTracker {
    last_budget_used: Mutex<Option<u64>>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct MeshAgentCard {
    name: String,
    description: String,
    model: String,
    framework: String,
    headline: String,
    status_line: String,
    capabilities: Vec<String>,
    updated_at: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct MeshManifest {
    agent_wallet: String,
    #[serde(rename = "userAddress")]
    user_wallet: String,
    device_id: String,
    peer_id: String,
    chain_id: u32,
    state_version: u64,
    state_root_hash: Option<String>,
    pdp_piece_cid: Option<String>,
    pdp_anchored_at: Option<u64>,
    name: String,
    description: String,
    model: String,
    framework: String,
    headline: String,
    status_line: String,
    skills: Vec<String>,
    mcp_servers: Vec<String>,
    a2a_endpoints: Vec<String>,
    capabilities: Vec<String>,
    agent_card_uri: String,
    listen_multiaddrs: Vec<String>,
    relay_peer_id: Option<String>,
    reputation_score: f64,
    total_conclaves: u64,
    successful_conclaves: u64,
    signed_at: u64,
    signature: String,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct MeshManifestUnsigned {
    agent_wallet: String,
    #[serde(rename = "userAddress")]
    user_wallet: String,
    device_id: String,
    peer_id: String,
    chain_id: u32,
    state_version: u64,
    state_root_hash: Option<String>,
    pdp_piece_cid: Option<String>,
    pdp_anchored_at: Option<u64>,
    name: String,
    description: String,
    model: String,
    framework: String,
    headline: String,
    status_line: String,
    skills: Vec<String>,
    mcp_servers: Vec<String>,
    a2a_endpoints: Vec<String>,
    capabilities: Vec<String>,
    agent_card_uri: String,
    listen_multiaddrs: Vec<String>,
    relay_peer_id: Option<String>,
    reputation_score: f64,
    total_conclaves: u64,
    successful_conclaves: u64,
    signed_at: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct MeshStateSnapshotRuntime {
    dna_hash: String,
    identity_hash: String,
    model_id: String,
    chain_id: u32,
    agent_card_cid: String,
    mcp_tools_hash: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct MeshStateSnapshotManifest {
    skills: Vec<String>,
    capabilities: Vec<String>,
    mcp_servers: Vec<String>,
    a2a_endpoints: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct MeshStateSnapshot {
    version: u64,
    created_at: u64,
    agent_wallet: String,
    #[serde(rename = "userAddress")]
    user_wallet: String,
    device_id: String,
    peer_id: String,
    runtime: MeshStateSnapshotRuntime,
    manifest: MeshStateSnapshotManifest,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct MeshStateSnapshotRequest {
    agent_wallet: String,
    chain_id: u32,
    peer_id: String,
    model_id: String,
    dna_hash: String,
    identity_hash: String,
    agent_card_cid: String,
    mcp_tools_hash: String,
    skills: Vec<String>,
    capabilities: Vec<String>,
    mcp_servers: Vec<String>,
    a2a_endpoints: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct MeshStateAnchorCommandRequest {
    api_url: String,
    compose_key_token: String,
    user_address: String,
    device_id: String,
    target_synapse_expiry: u64,
    snapshot: MeshStateSnapshotRequest,
    previous_state_root_hash: Option<String>,
    previous_pdp_piece_cid: Option<String>,
    previous_pdp_anchored_at: Option<u64>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct SignedMeshStateEnvelope {
    version: u32,
    kind: String,
    collection: String,
    hai_id: String,
    update_number: u64,
    path: String,
    peer_id: String,
    agent_wallet: String,
    #[serde(rename = "userAddress")]
    user_wallet: String,
    device_id: String,
    chain_id: u32,
    signed_at: u64,
    state_root_hash: String,
    snapshot: MeshStateSnapshot,
    signature: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct MeshHaiRuntimeRow {
    version: u32,
    agent_wallet: String,
    #[serde(rename = "userAddress")]
    user_wallet: String,
    device_id: String,
    hai_id: String,
    synapse_session_private_key: String,
    payer_address: Option<String>,
    session_key_expires_at: Option<u64>,
    next_update_number: u64,
    #[serde(default = "default_learning_number")]
    next_learning_number: u64,
    last_update_number: Option<u64>,
    last_path: Option<String>,
    last_state_root_hash: Option<String>,
    last_piece_cid: Option<String>,
    last_anchored_at: Option<u64>,
    updated_at: u64,
}

fn default_learning_number() -> u64 {
    1
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct MeshStateAnchorRuntimeResponse {
    hai_id: String,
    update_number: u64,
    path: String,
    file_name: String,
    latest_alias: String,
    state_root_hash: String,
    pdp_piece_cid: String,
    pdp_anchored_at: u64,
    payload_size: usize,
    provider_id: String,
    data_set_id: Option<String>,
    piece_id: Option<String>,
    retrieval_url: Option<String>,
    payer_address: String,
    session_key_expires_at: u64,
    source: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct MeshReputationRuntimeResponse {
    reputation_score: f64,
    total_conclaves: u64,
    successful_conclaves: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
enum MeshSharedArtifactKind {
    #[serde(rename = "learning")]
    Learning,
    #[serde(rename = "report")]
    Report,
    #[serde(rename = "resource")]
    Resource,
    #[serde(rename = "ticket")]
    Ticket,
}

impl MeshSharedArtifactKind {
    fn as_str(&self) -> &str {
        match self {
            Self::Learning => "learning",
            Self::Report => "report",
            Self::Resource => "resource",
            Self::Ticket => "ticket",
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
enum MeshPublicationQueueKind {
    #[serde(rename = "manifest.publish")]
    ManifestPublish,
    #[serde(rename = "learning.pin")]
    LearningPin,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct MeshPublicationQueueRequest {
    request_id: String,
    kind: MeshPublicationQueueKind,
    agent_wallet: String,
    requested_at: u64,
    reason: Option<String>,
    title: Option<String>,
    summary: Option<String>,
    content: Option<String>,
    access_price_usdc: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct MeshPublicationQueueResult {
    request_id: String,
    agent_wallet: Option<String>,
    kind: Option<MeshPublicationQueueKind>,
    success: bool,
    error: Option<String>,
    hai_id: Option<String>,
    update_number: Option<u64>,
    artifact_kind: Option<MeshSharedArtifactKind>,
    artifact_number: Option<u64>,
    path: Option<String>,
    latest_alias: Option<String>,
    root_cid: Option<String>,
    piece_cid: Option<String>,
    collection: Option<String>,
    state_root_hash: Option<String>,
    pdp_piece_cid: Option<String>,
    pdp_anchored_at: Option<u64>,
    manifest: Option<MeshManifest>,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct PersistedLocalState {
    settings: PersistedLocalSettings,
    identity: Option<PersistedLocalIdentity>,
    installed_agents: Vec<PersistedInstalledAgent>,
    installed_skills: Vec<PersistedInstalledSkill>,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct PersistedLocalSettings {
    api_url: String,
    mesh_enabled: bool,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct PersistedLocalIdentity {
    user_address: String,
    compose_key_token: String,
    session_id: String,
    budget: String,
    budget_used: String,
    duration: u64,
    chain_id: u32,
    expires_at: u64,
    device_id: String,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct ActiveSessionRefreshResponse {
    has_session: bool,
    key_id: String,
    token: String,
    budget_remaining: String,
    budget_used: String,
    expires_at: u64,
    chain_id: u32,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct PersistedInstalledSkill {
    id: String,
    enabled: bool,
    relative_path: String,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct PersistedInstalledAgent {
    agent_wallet: String,
    metadata: PersistedAgentMetadata,
    lock: PersistedAgentLock,
    network: PersistedAgentNetworkState,
    heartbeat: PersistedAgentHeartbeatState,
    #[serde(default)]
    desired_permissions: DaemonPermissionPolicy,
    #[serde(default)]
    permissions: DaemonPermissionPolicy,
    mcp_servers: Vec<String>,
    skill_states: HashMap<String, PersistedAgentSkillState>,
}

fn default_agent_heartbeat_enabled() -> bool {
    true
}

fn default_agent_heartbeat_interval_ms() -> u64 {
    30_000
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct PersistedAgentHeartbeatState {
    #[serde(default = "default_agent_heartbeat_enabled")]
    enabled: bool,
    #[serde(default = "default_agent_heartbeat_interval_ms")]
    interval_ms: u64,
    last_run_at: Option<u64>,
    last_result: Option<String>,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct PersistedAgentMetadata {
    name: String,
    description: String,
    agent_card_uri: String,
    model: String,
    framework: String,
    plugins: Vec<serde_json::Value>,
    endpoints: PersistedAgentEndpoints,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct PersistedAgentEndpoints {
    chat: String,
    stream: String,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct PersistedAgentLock {
    agent_wallet: String,
    agent_card_cid: String,
    model_id: String,
    mcp_tools_hash: String,
    chain_id: u32,
    dna_hash: String,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct PersistedAgentNetworkState {
    enabled: bool,
    public_card: Option<MeshAgentCard>,
    manifest: Option<MeshManifest>,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct PersistedAgentSkillState {
    skill_id: String,
    enabled: bool,
    eligible: bool,
    source: String,
    revision: String,
    updated_at: Option<u64>,
}

enum MeshLoopCommand {
    PublishManifest {
        manifest: MeshManifest,
        reply: oneshot::Sender<Result<MeshManifest, String>>,
    },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct MeshJoinRequest {
    user_address: String,
    device_id: String,
    chain_id: u32,
    gossip_topic: String,
    #[serde(default = "default_announce_topic")]
    announce_topic: String,
    #[serde(default = "default_manifest_topic")]
    manifest_topic: String,
    #[serde(default = "default_conclave_topic")]
    conclave_topic: String,
    #[serde(default = "default_mesh_heartbeat_ms")]
    heartbeat_ms: u64,
    #[serde(default = "default_kad_protocol")]
    kad_protocol: String,
    #[serde(default)]
    session_id: String,
    #[serde(default)]
    bootstrap_multiaddrs: Vec<String>,
    #[serde(default)]
    relay_multiaddrs: Vec<String>,
    #[serde(default)]
    published_agents: Vec<MeshPublishedAgent>,
    #[serde(default)]
    capabilities: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct MeshPublishedAgent {
    agent_wallet: String,
    dna_hash: String,
    capabilities_hash: String,
    capabilities: Vec<String>,
    public_card: Option<MeshAgentCard>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct MeshPublishedAgentStatus {
    agent_wallet: String,
    hai_id: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct MeshRuntimeStatus {
    running: bool,
    status: String,
    user_address: Option<String>,
    published_agents: Vec<MeshPublishedAgentStatus>,
    device_id: Option<String>,
    peer_id: Option<String>,
    listen_multiaddrs: Vec<String>,
    peers_discovered: u32,
    last_heartbeat_at: Option<u64>,
    last_error: Option<String>,
    updated_at: u64,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct LocalUpdateCheckResult {
    enabled: bool,
    available: bool,
    current_version: Option<String>,
    version: Option<String>,
    body: Option<String>,
    date: Option<String>,
}

impl Default for MeshRuntimeStatus {
    fn default() -> Self {
        Self {
            running: false,
            status: "dormant".to_string(),
            user_address: None,
            published_agents: Vec::new(),
            device_id: None,
            peer_id: None,
            listen_multiaddrs: Vec::new(),
            peers_discovered: 0,
            last_heartbeat_at: None,
            last_error: None,
            updated_at: now_ms(),
        }
    }
}

#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "MeshBehaviourEvent")]
struct MeshBehaviour {
    relay_client: relay::client::Behaviour,
    dcutr: dcutr::Behaviour,
    autonat: autonat::Behaviour,
    ping: ping::Behaviour,
    identify: identify::Behaviour,
    gossipsub: gossipsub::Behaviour,
    kad: kad::Behaviour<kad::store::MemoryStore>,
    rendezvous: rendezvous::client::Behaviour,
    connection_limits: connection_limits::Behaviour,
}

#[derive(Debug)]
enum MeshBehaviourEvent {
    RelayClient(relay::client::Event),
    Dcutr(dcutr::Event),
    Autonat(autonat::Event),
    Ping(ping::Event),
    Identify(identify::Event),
    Gossipsub(gossipsub::Event),
    Kad(kad::Event),
    Rendezvous(rendezvous::client::Event),
}

impl From<relay::client::Event> for MeshBehaviourEvent {
    fn from(event: relay::client::Event) -> Self {
        Self::RelayClient(event)
    }
}

impl From<dcutr::Event> for MeshBehaviourEvent {
    fn from(event: dcutr::Event) -> Self {
        Self::Dcutr(event)
    }
}

impl From<autonat::Event> for MeshBehaviourEvent {
    fn from(event: autonat::Event) -> Self {
        Self::Autonat(event)
    }
}

impl From<ping::Event> for MeshBehaviourEvent {
    fn from(event: ping::Event) -> Self {
        Self::Ping(event)
    }
}

impl From<identify::Event> for MeshBehaviourEvent {
    fn from(event: identify::Event) -> Self {
        Self::Identify(event)
    }
}

impl From<gossipsub::Event> for MeshBehaviourEvent {
    fn from(event: gossipsub::Event) -> Self {
        Self::Gossipsub(event)
    }
}

impl From<kad::Event> for MeshBehaviourEvent {
    fn from(event: kad::Event) -> Self {
        Self::Kad(event)
    }
}

impl From<rendezvous::client::Event> for MeshBehaviourEvent {
    fn from(event: rendezvous::client::Event) -> Self {
        Self::Rendezvous(event)
    }
}

impl From<void::Void> for MeshBehaviourEvent {
    fn from(event: void::Void) -> Self {
        match event {}
    }
}

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_millis() as u64
}

fn default_mesh_heartbeat_ms() -> u64 {
    30_000
}

fn default_announce_topic() -> String {
    "compose/announce/v1".to_string()
}

fn default_manifest_topic() -> String {
    "compose/manifest/v1".to_string()
}

fn default_conclave_topic() -> String {
    "compose/conclave/v1".to_string()
}

fn default_kad_protocol() -> String {
    "/compose-market/local/kad/1.0.0".to_string()
}

fn normalize_wallet(value: &str) -> Option<String> {
    let trimmed = value.trim().to_lowercase();
    if trimmed.len() != 42 || !trimmed.starts_with("0x") {
        return None;
    }
    if !trimmed.chars().skip(2).all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    Some(trimmed)
}

fn truncate_string(input: String, max_len: usize) -> String {
    let trimmed = input.trim().to_string();
    if trimmed.chars().count() <= max_len {
        return trimmed;
    }
    trimmed.chars().take(max_len).collect()
}

fn sanitize_mesh_agent_card(card: Option<MeshAgentCard>) -> Option<MeshAgentCard> {
    let Some(card) = card else {
        return None;
    };

    let name = truncate_string(card.name, 80);
    let description = truncate_string(card.description, 240);
    let model = truncate_string(card.model, 120);
    let framework = truncate_string(card.framework, 80);
    let headline = truncate_string(card.headline, 120);
    let status_line = truncate_string(card.status_line, 180);
    let capabilities = card
        .capabilities
        .into_iter()
        .map(|value| truncate_string(value, 48))
        .filter(|value| !value.is_empty())
        .take(24)
        .collect::<Vec<_>>();

    if name.is_empty() && headline.is_empty() && status_line.is_empty() {
        return None;
    }

    Some(MeshAgentCard {
        name,
        description,
        model,
        framework,
        headline,
        status_line,
        capabilities,
        updated_at: if card.updated_at == 0 {
            now_ms()
        } else {
            card.updated_at
        },
    })
}

fn normalize_device_id(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.len() < 8 || trimmed.len() > 128 {
        return None;
    }
    if !trimmed
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
    {
        return None;
    }
    Some(trimmed.to_string())
}

fn normalize_mesh_api_url_with_loopback_policy(value: &str, allow_loopback: bool) -> String {
    const DEFAULT_API_URL: &str = "https://api.compose.market";

    let trimmed = value.trim();
    if trimmed.is_empty() {
        return DEFAULT_API_URL.to_string();
    }

    let Ok(parsed) = HttpUrl::parse(trimmed) else {
        return DEFAULT_API_URL.to_string();
    };

    if !matches!(parsed.scheme(), "http" | "https") {
        return DEFAULT_API_URL.to_string();
    }

    let is_loopback_host = parsed
        .host_str()
        .map(|host| {
            matches!(
                host.to_ascii_lowercase().as_str(),
                "localhost" | "127.0.0.1" | "0.0.0.0" | "::1"
            )
        })
        .unwrap_or(false);
    if is_loopback_host && !allow_loopback {
        return DEFAULT_API_URL.to_string();
    }

    parsed.to_string().trim_end_matches('/').to_string()
}

fn normalize_mesh_api_url(value: &str) -> String {
    normalize_mesh_api_url_with_loopback_policy(value, cfg!(debug_assertions))
}

fn normalize_capability(raw: &str) -> Option<String> {
    let trimmed = raw.trim().to_lowercase();
    if trimmed.is_empty() || trimmed.len() > 96 {
        return None;
    }
    if !trimmed
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
    {
        return None;
    }
    Some(trimmed)
}

fn normalize_manifest_atom(raw: &str, max_len: usize) -> Option<String> {
    let trimmed = raw.trim().to_lowercase();
    if trimmed.is_empty() || trimmed.len() > max_len {
        return None;
    }
    if !trimmed
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | ':' | '/' | '@'))
    {
        return None;
    }
    Some(trimmed)
}

fn normalize_manifest_atoms(values: &[String], max_items: usize, max_len: usize) -> Vec<String> {
    let mut out = values
        .iter()
        .filter_map(|value| normalize_manifest_atom(value, max_len))
        .collect::<Vec<_>>();
    out.sort();
    out.dedup();
    out.truncate(max_items);
    out
}

fn normalize_manifest_urls(values: &[String], max_items: usize) -> Vec<String> {
    let mut out = values
        .iter()
        .map(|value| truncate_string(value.clone(), 256))
        .filter(|value| {
            let lower = value.to_lowercase();
            lower.starts_with("https://") || lower.starts_with("http://")
        })
        .collect::<Vec<_>>();
    out.sort();
    out.dedup();
    out.truncate(max_items);
    out
}

fn normalize_optional_hex_32(value: Option<String>) -> Result<Option<String>, String> {
    let Some(raw) = value else {
        return Ok(None);
    };
    let trimmed = raw.trim().to_lowercase();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed.len() != 64 || !trimmed.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err("stateRootHash must be a 64-character hex string".to_string());
    }
    Ok(Some(trimmed))
}

fn normalize_state_root_hash_for_compare(value: &str) -> Option<String> {
    let trimmed = value.trim().to_lowercase();
    let normalized = trimmed.strip_prefix("0x").unwrap_or(&trimmed);
    if normalized.len() != 64 || !normalized.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    Some(normalized.to_string())
}

fn same_state_root_hash(previous: Option<&str>, next: &str) -> bool {
    let Some(previous_normalized) = previous.and_then(normalize_state_root_hash_for_compare) else {
        return false;
    };
    let Some(next_normalized) = normalize_state_root_hash_for_compare(next) else {
        return false;
    };
    previous_normalized == next_normalized
}

fn normalize_optional_cid(value: Option<String>) -> Option<String> {
    let trimmed = value.unwrap_or_default().trim().to_string();
    if trimmed.is_empty() {
        None
    } else {
        Some(truncate_string(trimmed, 256))
    }
}

fn normalize_optional_peer_id(value: Option<String>) -> Result<Option<String>, String> {
    let Some(raw) = value else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    PeerId::from_str(trimmed).map_err(|err| format!("invalid relayPeerId: {err}"))?;
    Ok(Some(trimmed.to_string()))
}

fn normalize_multiaddr_strings(values: &[String]) -> Vec<String> {
    let mut out = values
        .iter()
        .filter_map(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return None;
            }
            trimmed
                .parse::<Multiaddr>()
                .ok()
                .map(|addr| addr.to_string())
        })
        .collect::<Vec<_>>();
    out.sort();
    out.dedup();
    out
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{:02x}", byte))
        .collect::<String>()
}

fn encode_hai_base36(mut value: u64) -> String {
    const ALPHABET: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut out = [b'0'; 6];
    for index in (0..6).rev() {
        out[index] = ALPHABET[(value % 36) as usize];
        value /= 36;
    }
    String::from_utf8_lossy(&out).to_string()
}

fn wallet_bytes(value: &str) -> [u8; 20] {
    let normalized = value.trim().trim_start_matches("0x");
    let mut out = [0u8; 20];
    for index in 0..20 {
        let start = index * 2;
        let end = start + 2;
        out[index] = u8::from_str_radix(&normalized[start..end], 16).unwrap_or(0);
    }
    out
}

fn derive_hai_id(agent_wallet: &str, user_address: &str, device_id: &str) -> String {
    let mut hasher = Keccak256::new();
    hasher.update(b":compose:hai:v1");
    hasher.update(wallet_bytes(user_address));
    hasher.update(wallet_bytes(agent_wallet));
    hasher.update(device_id.trim().as_bytes());
    let digest = hasher.finalize();
    let prefix = u64::from_be_bytes([
        digest[0], digest[1], digest[2], digest[3], digest[4], digest[5], digest[6], digest[7],
    ]);
    encode_hai_base36(prefix % 2_176_782_336)
}

fn compose_hai_path(hai_id: &str, update_number: u64) -> String {
    format!("compose-{}-{}", hai_id, update_number)
}

fn learning_hai_path(hai_id: &str, kind: MeshSharedArtifactKind, artifact_number: u64) -> String {
    format!("learning-{}-{}-#{}", hai_id, kind.as_str(), artifact_number)
}

fn persist_manifest_update(app: &tauri::AppHandle, manifest: &MeshManifest) -> Result<(), String> {
    let mut value = load_local_state_value(app)?;

    let Some(installed_agents) = value
        .get_mut("installedAgents")
        .and_then(|items| items.as_array_mut())
    else {
        return Ok(());
    };

    for agent in installed_agents.iter_mut() {
        let wallet = agent
            .get("agentWallet")
            .and_then(|entry| entry.as_str())
            .and_then(normalize_wallet);
        if wallet.as_deref() != Some(manifest.agent_wallet.as_str()) {
            continue;
        }

        if !agent.get("network").is_some_and(|entry| entry.is_object()) {
            agent["network"] = serde_json::json!({});
        }
        agent["network"]["manifest"] = serde_json::to_value(manifest)
            .map_err(|err| format!("failed to encode published manifest: {err}"))?;
        break;
    }

    save_local_state_value(app, &value)
}

fn normalize_persisted_url(value: &str) -> Option<String> {
    let trimmed = truncate_string(value.to_string(), 256);
    let lower = trimmed.to_lowercase();
    if lower.starts_with("https://") || lower.starts_with("http://") {
        Some(trimmed)
    } else {
        None
    }
}

fn normalize_agent_card_uri(metadata_uri: &str, fallback_cid: &str) -> Result<String, String> {
    let metadata_uri = metadata_uri.trim();
    if !metadata_uri.is_empty() {
        if metadata_uri.starts_with("ipfs://") {
            return Ok(metadata_uri.to_string());
        }
        return Ok(format!("ipfs://{}", metadata_uri.trim_start_matches('/')));
    }

    let fallback = fallback_cid.trim();
    if fallback.is_empty() {
        return Err("agentCardCid is required to build the mesh manifest".to_string());
    }
    Ok(format!("ipfs://{}", fallback))
}

fn extract_plugin_capabilities(values: &[serde_json::Value]) -> Vec<String> {
    let mut out = values
        .iter()
        .filter_map(|value| {
            if let Some(raw) = value.as_str() {
                return normalize_capability(raw);
            }
            value
                .as_object()
                .and_then(|entry| entry.get("registryId"))
                .and_then(|entry| entry.as_str())
                .and_then(normalize_capability)
        })
        .collect::<Vec<_>>();
    out.sort();
    out.dedup();
    out
}

fn merge_mesh_skill_ids(
    installed_skills: &[PersistedInstalledSkill],
    agent_skills: &HashMap<String, PersistedAgentSkillState>,
) -> Vec<String> {
    let mut values = installed_skills
        .iter()
        .filter(|skill| skill.enabled)
        .map(|skill| skill.id.clone())
        .collect::<Vec<_>>();

    values.extend(
        agent_skills
            .values()
            .filter(|state| state.enabled && state.eligible)
            .filter_map(|state| {
                if state.skill_id.trim().is_empty() {
                    None
                } else {
                    Some(state.skill_id.clone())
                }
            }),
    );

    normalize_manifest_atoms(&values, 128, 96)
}

fn manifest_comparable_payload(manifest: &MeshManifest) -> serde_json::Value {
    serde_json::json!({
        "agentWallet": manifest.agent_wallet,
        "userAddress": manifest.user_wallet,
        "deviceId": manifest.device_id,
        "chainId": manifest.chain_id,
        "stateRootHash": manifest.state_root_hash,
        "pdpPieceCid": manifest.pdp_piece_cid,
        "pdpAnchoredAt": manifest.pdp_anchored_at,
        "name": manifest.name,
        "description": manifest.description,
        "model": manifest.model,
        "framework": manifest.framework,
        "headline": manifest.headline,
        "statusLine": manifest.status_line,
        "skills": manifest.skills,
        "mcpServers": manifest.mcp_servers,
        "a2aEndpoints": manifest.a2a_endpoints,
        "capabilities": manifest.capabilities,
        "agentCardUri": manifest.agent_card_uri,
        "reputationScore": manifest.reputation_score,
        "totalConclaves": manifest.total_conclaves,
        "successfulConclaves": manifest.successful_conclaves,
    })
}

fn next_manifest_state_version(
    previous_manifest: Option<&MeshManifest>,
    next_manifest: &MeshManifest,
) -> u64 {
    match previous_manifest {
        None => 1,
        Some(previous) => {
            if manifest_comparable_payload(previous) == manifest_comparable_payload(next_manifest) {
                previous.state_version
            } else {
                previous.state_version.saturating_add(1)
            }
        }
    }
}

#[derive(Debug, Clone)]
struct MeshPubCtx {
    api_url: String,
    compose_key_token: String,
    user_wallet: String,
    device_id: String,
    chain_id: u32,
    target_synapse_expiry: u64,
    installed_skills: Vec<PersistedInstalledSkill>,
    agent: PersistedInstalledAgent,
}

async fn load_mesh_pub_ctx(
    app: &tauri::AppHandle,
    agent_wallet: &str,
) -> Result<MeshPubCtx, String> {
    let state = load_persisted_local_state(app)?;
    let client = HttpClient::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .map_err(|err| format!("failed to build compose-key session client: {err}"))?;
    let identity = refresh_local_identity_session(app, &state, &client)
        .await?
        .ok_or_else(|| {
            "an active compose-key session is required for mesh publication".to_string()
        })?;
    let user_wallet = normalize_wallet(&identity.user_address)
        .ok_or_else(|| "local identity userAddress is invalid".to_string())?;
    let device_id = normalize_device_id(&identity.device_id)
        .ok_or_else(|| "local identity deviceId is invalid".to_string())?;
    let compose_key_token = identity.compose_key_token.trim().to_string();
    if compose_key_token.is_empty() {
        return Err("local identity composeKeyToken is required for mesh publication".to_string());
    }

    let normalized_agent_wallet = normalize_wallet(agent_wallet)
        .ok_or_else(|| "mesh publication request agentWallet is invalid".to_string())?;
    let agent = state
        .installed_agents
        .iter()
        .find(|entry| {
            normalize_wallet(&entry.agent_wallet).as_deref()
                == Some(normalized_agent_wallet.as_str())
        })
        .cloned()
        .ok_or_else(|| {
            format!(
                "local state is missing installed agent {}",
                normalized_agent_wallet
            )
        })?;

    Ok(MeshPubCtx {
        api_url: normalize_persisted_url(&state.settings.api_url)
            .ok_or_else(|| "local settings apiUrl is invalid for mesh publication".to_string())?,
        compose_key_token,
        user_wallet,
        device_id,
        chain_id: if identity.chain_id > 0 {
            identity.chain_id
        } else {
            agent.lock.chain_id
        },
        target_synapse_expiry: identity.expires_at,
        installed_skills: state.installed_skills.clone(),
        agent,
    })
}

async fn build_current_mesh_publication(
    app: &tauri::AppHandle,
    agent_wallet: &str,
    live_status: &MeshRuntimeStatus,
) -> Result<(MeshManifest, MeshStateAnchorCommandRequest), String> {
    let ctx = load_mesh_pub_ctx(app, agent_wallet).await?;
    let normalized_agent_wallet = normalize_wallet(agent_wallet)
        .ok_or_else(|| "mesh publication request agentWallet is invalid".to_string())?;
    let agent = ctx.agent.clone();
    let workspace_state = load_manifest_workspace_state(app, &agent)?;
    let runtime_host = app.state::<LocalRuntimeHostState>();
    let runtime_status = ensure_local_runtime_host(app, runtime_host.inner())?;
    let reputation = fetch_mesh_reputation_via_local_runtime(
        &runtime_status.base_url,
        normalized_agent_wallet.as_str(),
    )
    .await?;

    let previous_manifest = agent.network.manifest.clone();
    let existing_card = agent.network.public_card.clone();
    let model_id = if !agent.lock.model_id.trim().is_empty() {
        agent.lock.model_id.clone()
    } else {
        agent.metadata.model.clone()
    };
    if model_id.trim().is_empty() {
        return Err("installed agent lock is missing modelId".to_string());
    }

    let framework = if !agent.metadata.framework.trim().is_empty() {
        agent.metadata.framework.clone()
    } else {
        "manowar".to_string()
    };
    let capabilities = {
        let base = existing_card
            .as_ref()
            .map(|card| card.capabilities.clone())
            .filter(|items| !items.is_empty())
            .unwrap_or_else(|| extract_plugin_capabilities(&agent.metadata.plugins));
        normalize_manifest_atoms(&base, 128, 96)
    };
    let skills = merge_mesh_skill_ids(&ctx.installed_skills, &agent.skill_states);
    let a2a_endpoints = normalize_manifest_urls(
        &[
            agent.metadata.endpoints.chat.clone(),
            agent.metadata.endpoints.stream.clone(),
        ]
        .into_iter()
        .filter(|value| !value.trim().is_empty())
        .collect::<Vec<_>>(),
        16,
    );
    let mcp_servers = normalize_manifest_atoms(&agent.mcp_servers, 64, 128);
    let peer_id = live_status
        .peer_id
        .clone()
        .ok_or_else(|| "mesh runtime does not have a live peerId yet".to_string())?;

    let mut manifest = MeshManifest {
        agent_wallet: normalized_agent_wallet.clone(),
        user_wallet: ctx.user_wallet.clone(),
        device_id: ctx.device_id.clone(),
        peer_id,
        chain_id: ctx.chain_id,
        state_version: 1,
        state_root_hash: previous_manifest
            .as_ref()
            .and_then(|value| value.state_root_hash.clone()),
        pdp_piece_cid: previous_manifest
            .as_ref()
            .and_then(|value| value.pdp_piece_cid.clone()),
        pdp_anchored_at: previous_manifest
            .as_ref()
            .and_then(|value| value.pdp_anchored_at),
        name: truncate_string(
            existing_card
                .as_ref()
                .map(|card| card.name.clone())
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| agent.metadata.name.clone()),
            80,
        ),
        description: truncate_string(agent.metadata.description.clone(), 240),
        model: truncate_string(
            existing_card
                .as_ref()
                .map(|card| card.model.clone())
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| model_id.clone()),
            120,
        ),
        framework: truncate_string(
            existing_card
                .as_ref()
                .map(|card| card.framework.clone())
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| framework.clone()),
            80,
        ),
        headline: truncate_string(
            existing_card
                .as_ref()
                .map(|card| card.headline.clone())
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| format!("{} on {}", agent.metadata.name, framework)),
            120,
        ),
        status_line: truncate_string(
            existing_card
                .as_ref()
                .map(|card| card.status_line.clone())
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| agent.metadata.description.clone()),
            180,
        ),
        skills: skills.clone(),
        mcp_servers: mcp_servers.clone(),
        a2a_endpoints: a2a_endpoints.clone(),
        capabilities: capabilities.clone(),
        agent_card_uri: normalize_agent_card_uri(
            &agent.metadata.agent_card_uri,
            &agent.lock.agent_card_cid,
        )?,
        listen_multiaddrs: normalize_multiaddr_strings(&live_status.listen_multiaddrs),
        relay_peer_id: previous_manifest
            .as_ref()
            .and_then(|value| value.relay_peer_id.clone())
            .or_else(|| {
                derive_relay_peer_id_from_listen_multiaddrs(&live_status.listen_multiaddrs)
            }),
        reputation_score: reputation.reputation_score,
        total_conclaves: reputation.total_conclaves,
        successful_conclaves: reputation.successful_conclaves,
        signed_at: 0,
        signature: String::new(),
    };
    manifest.state_version = next_manifest_state_version(previous_manifest.as_ref(), &manifest);

    let snapshot = MeshStateSnapshotRequest {
        agent_wallet: normalized_agent_wallet,
        chain_id: manifest.chain_id,
        peer_id: manifest.peer_id.clone(),
        model_id,
        dna_hash: workspace_state.dna_hash,
        identity_hash: workspace_state.identity_hash,
        agent_card_cid: agent.lock.agent_card_cid.clone(),
        mcp_tools_hash: workspace_state.mcp_tools_hash,
        skills,
        capabilities,
        mcp_servers: manifest.mcp_servers.clone(),
        a2a_endpoints,
    };

    let anchor_request = MeshStateAnchorCommandRequest {
        api_url: ctx.api_url,
        compose_key_token: ctx.compose_key_token,
        user_address: ctx.user_wallet,
        device_id: ctx.device_id,
        target_synapse_expiry: ctx.target_synapse_expiry,
        previous_state_root_hash: previous_manifest
            .as_ref()
            .and_then(|value| value.state_root_hash.clone()),
        previous_pdp_piece_cid: previous_manifest
            .as_ref()
            .and_then(|value| value.pdp_piece_cid.clone()),
        previous_pdp_anchored_at: previous_manifest
            .as_ref()
            .and_then(|value| value.pdp_anchored_at),
        snapshot,
    };

    Ok((manifest, anchor_request))
}

fn normalize_mesh_state_snapshot_request(
    request: &MeshStateAnchorCommandRequest,
    live_status: &MeshRuntimeStatus,
) -> Result<MeshStateSnapshot, String> {
    let user_wallet = normalize_wallet(&request.user_address)
        .ok_or_else(|| "userAddress must be a valid wallet address".to_string())?;
    let agent_wallet = normalize_wallet(&request.snapshot.agent_wallet)
        .ok_or_else(|| "snapshot.agentWallet must be a valid wallet address".to_string())?;
    let device_id = normalize_device_id(&request.device_id)
        .ok_or_else(|| "deviceId format is invalid".to_string())?;
    if !status_has_published_agent(live_status, agent_wallet.as_str()) {
        return Err("snapshot agentWallet does not match the running mesh agent".to_string());
    }
    if live_status.device_id.as_deref() != Some(device_id.as_str()) {
        return Err("snapshot deviceId does not match the running mesh device".to_string());
    }
    let peer_id = live_status
        .peer_id
        .clone()
        .ok_or_else(|| "mesh runtime does not have a live peerId yet".to_string())?;
    if peer_id != request.snapshot.peer_id.trim() {
        return Err("snapshot.peerId does not match the running mesh peer".to_string());
    }

    Ok(MeshStateSnapshot {
        version: 2,
        created_at: now_ms(),
        agent_wallet,
        user_wallet,
        device_id,
        peer_id,
        runtime: MeshStateSnapshotRuntime {
            dna_hash: truncate_string(request.snapshot.dna_hash.clone(), 256),
            identity_hash: truncate_string(request.snapshot.identity_hash.clone(), 256),
            model_id: truncate_string(request.snapshot.model_id.clone(), 128),
            chain_id: request.snapshot.chain_id,
            agent_card_cid: truncate_string(request.snapshot.agent_card_cid.clone(), 256),
            mcp_tools_hash: truncate_string(request.snapshot.mcp_tools_hash.clone(), 256),
        },
        manifest: MeshStateSnapshotManifest {
            skills: normalize_manifest_atoms(&request.snapshot.skills, 128, 96),
            capabilities: normalize_manifest_atoms(&request.snapshot.capabilities, 128, 96),
            mcp_servers: normalize_manifest_atoms(&request.snapshot.mcp_servers, 64, 128),
            a2a_endpoints: normalize_manifest_urls(&request.snapshot.a2a_endpoints, 64),
        },
    })
}

fn canonical_snapshot_json(snapshot: &MeshStateSnapshot) -> Result<String, String> {
    serde_json::to_string(snapshot)
        .map_err(|err| format!("failed to encode canonical state snapshot: {err}"))
}

fn sha256_hex_string(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    hex_encode(&hasher.finalize())
}

fn default_capabilities(agent_wallet: &str) -> Vec<String> {
    let wallet_suffix = agent_wallet.trim_start_matches("0x");
    vec![format!("agent-{wallet_suffix}")]
}

fn normalize_published_agent(agent: &MeshPublishedAgent) -> Result<MeshPublishedAgent, String> {
    let agent_wallet = normalize_wallet(&agent.agent_wallet)
        .ok_or_else(|| "publishedAgents.agentWallet must be a valid wallet address".to_string())?;
    let capabilities = {
        let normalized = agent
            .capabilities
            .iter()
            .filter_map(|cap| normalize_capability(cap))
            .collect::<Vec<_>>();
        if normalized.is_empty() {
            default_capabilities(&agent_wallet)
        } else {
            normalized
        }
    };

    Ok(MeshPublishedAgent {
        agent_wallet,
        dna_hash: truncate_string(agent.dna_hash.clone(), 256),
        capabilities_hash: truncate_string(agent.capabilities_hash.clone(), 256),
        capabilities,
        public_card: sanitize_mesh_agent_card(agent.public_card.clone()),
    })
}

fn request_published_statuses(request: &MeshJoinRequest) -> Vec<MeshPublishedAgentStatus> {
    request
        .published_agents
        .iter()
        .map(|agent| MeshPublishedAgentStatus {
            agent_wallet: agent.agent_wallet.clone(),
            hai_id: derive_hai_id(
                &agent.agent_wallet,
                &request.user_address,
                &request.device_id,
            ),
        })
        .collect()
}

fn status_has_published_agent(status: &MeshRuntimeStatus, agent_wallet: &str) -> bool {
    status
        .published_agents
        .iter()
        .any(|item| item.agent_wallet == agent_wallet)
}

fn normalize_manifest_publish_outcome(
    result: Result<gossipsub::MessageId, gossipsub::PublishError>,
    manifest: &MeshManifest,
) -> Result<MeshManifest, String> {
    match result {
        Ok(_) | Err(gossipsub::PublishError::InsufficientPeers) => Ok(manifest.clone()),
        Err(error) => Err(format!("manifest publish failed: {error}")),
    }
}

fn validate_mesh_join_request(request: &MeshJoinRequest) -> Result<MeshJoinRequest, String> {
    let user_address = normalize_wallet(&request.user_address)
        .ok_or_else(|| "userAddress must be a valid wallet address".to_string())?;
    let device_id = normalize_device_id(&request.device_id)
        .ok_or_else(|| "deviceId format is invalid".to_string())?;

    if request.chain_id == 0 {
        return Err("chainId must be positive".to_string());
    }
    if request.gossip_topic.trim().is_empty() {
        return Err("gossipTopic is required".to_string());
    }
    if request.announce_topic.trim().is_empty() {
        return Err("announceTopic is required".to_string());
    }
    if request.manifest_topic.trim().is_empty() {
        return Err("manifestTopic is required".to_string());
    }
    if request.conclave_topic.trim().is_empty() {
        return Err("conclaveTopic is required".to_string());
    }
    if request.heartbeat_ms < 1_000 || request.heartbeat_ms > 300_000 {
        return Err("heartbeatMs must be between 1000 and 300000".to_string());
    }
    if request.kad_protocol.trim().is_empty() {
        return Err("kadProtocol is required".to_string());
    }

    if request.published_agents.is_empty() {
        return Err("publishedAgents must contain at least one mesh-enabled agent".to_string());
    }

    let mut published_agents = request
        .published_agents
        .iter()
        .map(normalize_published_agent)
        .collect::<Result<Vec<_>, _>>()?;
    published_agents.sort_by(|left, right| left.agent_wallet.cmp(&right.agent_wallet));
    published_agents.dedup_by(|left, right| left.agent_wallet == right.agent_wallet);

    let capabilities = normalize_manifest_atoms(
        &published_agents
            .iter()
            .flat_map(|agent| agent.capabilities.iter().cloned())
            .collect::<Vec<_>>(),
        256,
        96,
    );

    Ok(MeshJoinRequest {
        user_address,
        device_id,
        chain_id: request.chain_id,
        gossip_topic: request.gossip_topic.trim().to_string(),
        announce_topic: request.announce_topic.trim().to_string(),
        manifest_topic: request.manifest_topic.trim().to_string(),
        conclave_topic: request.conclave_topic.trim().to_string(),
        heartbeat_ms: request.heartbeat_ms,
        kad_protocol: request.kad_protocol.trim().to_string(),
        session_id: truncate_string(request.session_id.clone(), 256),
        bootstrap_multiaddrs: request
            .bootstrap_multiaddrs
            .iter()
            .map(|addr| addr.trim().to_string())
            .filter(|addr| !addr.is_empty())
            .collect(),
        relay_multiaddrs: request
            .relay_multiaddrs
            .iter()
            .map(|addr| addr.trim().to_string())
            .filter(|addr| !addr.is_empty())
            .collect(),
        published_agents,
        capabilities,
    })
}

fn unsigned_manifest_bytes(value: &MeshManifestUnsigned) -> Result<Vec<u8>, String> {
    serde_cbor::to_vec(value).map_err(|err| format!("failed to encode unsigned manifest: {err}"))
}

fn sign_mesh_manifest(
    local_key: &identity::Keypair,
    manifest: &MeshManifest,
) -> Result<MeshManifest, String> {
    let unsigned = MeshManifestUnsigned {
        agent_wallet: manifest.agent_wallet.clone(),
        user_wallet: manifest.user_wallet.clone(),
        device_id: manifest.device_id.clone(),
        peer_id: manifest.peer_id.clone(),
        chain_id: manifest.chain_id,
        state_version: manifest.state_version,
        state_root_hash: manifest.state_root_hash.clone(),
        pdp_piece_cid: manifest.pdp_piece_cid.clone(),
        pdp_anchored_at: manifest.pdp_anchored_at,
        name: manifest.name.clone(),
        description: manifest.description.clone(),
        model: manifest.model.clone(),
        framework: manifest.framework.clone(),
        headline: manifest.headline.clone(),
        status_line: manifest.status_line.clone(),
        skills: manifest.skills.clone(),
        mcp_servers: manifest.mcp_servers.clone(),
        a2a_endpoints: manifest.a2a_endpoints.clone(),
        capabilities: manifest.capabilities.clone(),
        agent_card_uri: manifest.agent_card_uri.clone(),
        listen_multiaddrs: manifest.listen_multiaddrs.clone(),
        relay_peer_id: manifest.relay_peer_id.clone(),
        reputation_score: manifest.reputation_score,
        total_conclaves: manifest.total_conclaves,
        successful_conclaves: manifest.successful_conclaves,
        signed_at: manifest.signed_at,
    };
    let sign_bytes = unsigned_manifest_bytes(&unsigned)?;
    let sig = local_key
        .sign(&sign_bytes)
        .map_err(|err| format!("failed to sign manifest: {err}"))?;

    Ok(MeshManifest {
        signature: hex_encode(&sig),
        ..manifest.clone()
    })
}

fn validate_mesh_manifest(
    manifest: MeshManifest,
    status: &MeshRuntimeStatus,
) -> Result<MeshManifest, String> {
    let agent_wallet = normalize_wallet(&manifest.agent_wallet)
        .ok_or_else(|| "manifest.agentWallet must be a valid wallet address".to_string())?;
    let user_wallet = normalize_wallet(&manifest.user_wallet)
        .ok_or_else(|| "manifest.userAddress must be a valid wallet address".to_string())?;
    let device_id = normalize_device_id(&manifest.device_id)
        .ok_or_else(|| "manifest.deviceId format is invalid".to_string())?;

    if !status_has_published_agent(status, agent_wallet.as_str()) {
        return Err("manifest agentWallet does not match the running mesh agent".to_string());
    }
    if status.device_id.as_deref() != Some(device_id.as_str()) {
        return Err("manifest deviceId does not match the running mesh device".to_string());
    }
    if manifest.chain_id == 0 {
        return Err("manifest.chainId must be positive".to_string());
    }
    if manifest.state_version == 0 {
        return Err("manifest.stateVersion must be positive".to_string());
    }

    let peer_id = status
        .peer_id
        .clone()
        .ok_or_else(|| "mesh runtime does not have a live peerId yet".to_string())?;
    PeerId::from_str(&peer_id)
        .map_err(|err| format!("mesh runtime returned invalid peerId: {err}"))?;

    Ok(MeshManifest {
        agent_wallet,
        user_wallet,
        device_id,
        peer_id,
        chain_id: manifest.chain_id,
        state_version: manifest.state_version,
        state_root_hash: normalize_optional_hex_32(manifest.state_root_hash)?,
        pdp_piece_cid: normalize_optional_cid(manifest.pdp_piece_cid),
        pdp_anchored_at: manifest.pdp_anchored_at,
        name: truncate_string(manifest.name, 80),
        description: truncate_string(manifest.description, 240),
        model: truncate_string(manifest.model, 120),
        framework: truncate_string(manifest.framework, 80),
        headline: truncate_string(manifest.headline, 120),
        status_line: truncate_string(manifest.status_line, 180),
        skills: normalize_manifest_atoms(&manifest.skills, 128, 96),
        mcp_servers: normalize_manifest_atoms(&manifest.mcp_servers, 64, 128),
        a2a_endpoints: normalize_manifest_urls(&manifest.a2a_endpoints, 16),
        capabilities: normalize_manifest_atoms(&manifest.capabilities, 128, 96),
        agent_card_uri: truncate_string(manifest.agent_card_uri, 512),
        listen_multiaddrs: normalize_multiaddr_strings(&status.listen_multiaddrs),
        relay_peer_id: normalize_optional_peer_id(manifest.relay_peer_id)?,
        reputation_score: if manifest.reputation_score.is_finite() {
            manifest.reputation_score.clamp(0.0, 1.0)
        } else {
            0.0
        },
        total_conclaves: manifest.total_conclaves,
        successful_conclaves: manifest.successful_conclaves,
        signed_at: now_ms(),
        signature: String::new(),
    })
}

pub(crate) fn resolve_base_dir(app: &tauri::AppHandle) -> Result<PathBuf, String> {
    let app_data = app
        .path()
        .app_data_dir()
        .map_err(|err| format!("failed to resolve app data directory: {err}"))?;
    let override_file = app_data.join("base_dir_override.txt");
    if override_file.exists() {
        let raw = fs::read_to_string(&override_file)
            .map_err(|err| format!("failed to read base dir override: {err}"))?;
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            return Ok(PathBuf::from(trimmed));
        }
    }
    Ok(app_data)
}

fn daemon_state_path(app: &tauri::AppHandle) -> Result<PathBuf, String> {
    Ok(resolve_base_dir(app)?.join("daemon_state.json"))
}

fn read_daemon_state_from_disk(app: &tauri::AppHandle) -> Result<DaemonStateFile, String> {
    let file = daemon_state_path(app)?;
    if !file.exists() {
        return Ok(DaemonStateFile::default());
    }

    let raw =
        fs::read_to_string(&file).map_err(|err| format!("failed to read daemon state: {err}"))?;
    serde_json::from_str(&raw).map_err(|err| format!("failed to parse daemon state: {err}"))
}

fn write_daemon_state_to_disk(
    app: &tauri::AppHandle,
    state: &DaemonStateFile,
) -> Result<(), String> {
    let file = daemon_state_path(app)?;
    if let Some(parent) = file.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| format!("failed to create daemon state dir: {err}"))?;
    }
    let serialized = serde_json::to_string_pretty(state)
        .map_err(|err| format!("failed to serialize daemon state: {err}"))?;
    fs::write(&file, serialized).map_err(|err| format!("failed to persist daemon state: {err}"))?;
    Ok(())
}

fn local_state_path(app: &tauri::AppHandle) -> Result<PathBuf, String> {
    Ok(resolve_base_dir(app)?.join("state.json"))
}

fn write_string_atomically(path: &Path, contents: &str, label: &str) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| format!("failed to resolve {label} parent directory"))?;
    fs::create_dir_all(parent)
        .map_err(|err| format!("failed to create {label} parent directory: {err}"))?;

    let stem = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("state");
    let temp_path = parent.join(format!(".{}.{}.{}.tmp", stem, std::process::id(), now_ms()));

    fs::write(&temp_path, contents)
        .map_err(|err| format!("failed to write temporary {label} file: {err}"))?;
    fs::rename(&temp_path, path).map_err(|err| {
        let _ = fs::remove_file(&temp_path);
        format!("failed to replace {label} file atomically: {err}")
    })
}

fn normalize_local_state_json(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok("{}".to_string());
    }

    let mut state = serde_json::from_str::<serde_json::Value>(trimmed)
        .map_err(|err| format!("failed to parse local state: {err}"))?;
    let Some(root) = state.as_object_mut() else {
        return Err("local state root must be a JSON object".to_string());
    };

    let settings = root
        .entry("settings".to_string())
        .or_insert_with(|| serde_json::json!({}));
    if let Some(settings_object) = settings.as_object_mut() {
        let api_url = settings_object
            .get("apiUrl")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        settings_object.insert(
            "apiUrl".to_string(),
            serde_json::Value::String(normalize_mesh_api_url(api_url)),
        );
    }

    let identity = root
        .get("identity")
        .and_then(|value| value.as_object())
        .cloned()
        .unwrap_or_default();
    let user_address = identity
        .get("userAddress")
        .and_then(|value| value.as_str())
        .and_then(normalize_wallet);
    let device_id = identity
        .get("deviceId")
        .and_then(|value| value.as_str())
        .and_then(normalize_device_id);

    if let (Some(user_address), Some(device_id)) = (user_address, device_id) {
        if let Some(installed_agents) = root
            .get_mut("installedAgents")
            .and_then(|value| value.as_array_mut())
        {
            for agent in installed_agents {
                let Some(agent_object) = agent.as_object_mut() else {
                    continue;
                };
                let Some(agent_wallet) = agent_object
                    .get("agentWallet")
                    .and_then(|value| value.as_str())
                    .and_then(normalize_wallet)
                else {
                    continue;
                };

                let network_value = agent_object
                    .entry("network".to_string())
                    .or_insert_with(|| serde_json::json!({}));
                let Some(network_object) = network_value.as_object_mut() else {
                    continue;
                };

                let missing_hai = network_object
                    .get("haiId")
                    .and_then(|value| value.as_str())
                    .map(|value| value.trim().is_empty())
                    .unwrap_or(true);
                if missing_hai {
                    network_object.insert(
                        "haiId".to_string(),
                        serde_json::Value::String(derive_hai_id(
                            &agent_wallet,
                            &user_address,
                            &device_id,
                        )),
                    );
                }
            }
        }
    }

    serde_json::to_string(&state)
        .map_err(|err| format!("failed to serialize normalized local state: {err}"))
}

fn mesh_publication_requests_dir(app: &tauri::AppHandle) -> Result<PathBuf, String> {
    let dir = resolve_base_dir(app)?
        .join("mesh")
        .join("publications")
        .join("requests");
    fs::create_dir_all(&dir)
        .map_err(|err| format!("failed to create mesh publication requests dir: {err}"))?;
    Ok(dir)
}

fn mesh_publication_agent_requests_dir(
    app: &tauri::AppHandle,
    agent_wallet: &str,
) -> Result<PathBuf, String> {
    let dir = mesh_publication_requests_dir(app)?.join(agent_wallet.to_lowercase());
    fs::create_dir_all(&dir)
        .map_err(|err| format!("failed to create mesh publication agent requests dir: {err}"))?;
    Ok(dir)
}

fn mesh_publication_results_dir(app: &tauri::AppHandle) -> Result<PathBuf, String> {
    let dir = resolve_base_dir(app)?
        .join("mesh")
        .join("publications")
        .join("results");
    fs::create_dir_all(&dir)
        .map_err(|err| format!("failed to create mesh publication results dir: {err}"))?;
    Ok(dir)
}

fn mesh_publication_agent_results_dir(
    app: &tauri::AppHandle,
    agent_wallet: &str,
) -> Result<PathBuf, String> {
    let dir = mesh_publication_results_dir(app)?.join(agent_wallet.to_lowercase());
    fs::create_dir_all(&dir)
        .map_err(|err| format!("failed to create mesh publication agent results dir: {err}"))?;
    Ok(dir)
}

fn load_persisted_local_state(app: &tauri::AppHandle) -> Result<PersistedLocalState, String> {
    let file = local_state_path(app)?;
    let value = if !file.exists() {
        serde_json::json!({})
    } else {
        let raw = fs::read_to_string(&file)
            .map_err(|err| format!("failed to read local state: {err}"))?;
        serde_json::from_str(&raw)
            .map_err(|err| format!("failed to parse local state JSON: {err}"))?
    };
    serde_json::from_value(value).map_err(|err| format!("failed to parse local state: {err}"))
}

fn load_local_state_value(app: &tauri::AppHandle) -> Result<serde_json::Value, String> {
    let file = local_state_path(app)?;
    if !file.exists() {
        return Ok(serde_json::json!({}));
    }
    let raw =
        fs::read_to_string(&file).map_err(|err| format!("failed to read local state: {err}"))?;
    serde_json::from_str(&raw).map_err(|err| format!("failed to parse local state JSON: {err}"))
}

fn save_local_state_value(app: &tauri::AppHandle, state: &serde_json::Value) -> Result<(), String> {
    let file = local_state_path(app)?;
    let serialized = serde_json::to_string(state)
        .map_err(|err| format!("failed to serialize local state JSON: {err}"))?;
    let normalized = normalize_local_state_json(&serialized)?;
    write_string_atomically(&file, &normalized, "local state")
}

fn normalize_daemon_decision(value: &str) -> String {
    match value.trim().to_lowercase().as_str() {
        "allow" => "allow".to_string(),
        _ => "deny".to_string(),
    }
}

fn normalize_daemon_permission_policy(policy: DaemonPermissionPolicy) -> DaemonPermissionPolicy {
    DaemonPermissionPolicy {
        shell: normalize_daemon_decision(&policy.shell),
        filesystem_read: normalize_daemon_decision(&policy.filesystem_read),
        filesystem_write: normalize_daemon_decision(&policy.filesystem_write),
        filesystem_edit: normalize_daemon_decision(&policy.filesystem_edit),
        filesystem_delete: normalize_daemon_decision(&policy.filesystem_delete),
        camera: normalize_daemon_decision(&policy.camera),
        microphone: normalize_daemon_decision(&policy.microphone),
        network: normalize_daemon_decision(&policy.network),
    }
}

fn select_desired_permission_policy(
    normalized_permissions: &DaemonPermissionPolicy,
    normalized_desired_permissions: &DaemonPermissionPolicy,
) -> DaemonPermissionPolicy {
    if *normalized_desired_permissions == DaemonPermissionPolicy::default()
        && *normalized_permissions != DaemonPermissionPolicy::default()
    {
        normalized_permissions.clone()
    } else {
        normalized_desired_permissions.clone()
    }
}

fn mesh_key_path(app: &tauri::AppHandle) -> Result<PathBuf, String> {
    let base = resolve_base_dir(app)?;
    let dir = base.join("network");
    fs::create_dir_all(&dir).map_err(|err| format!("failed to create network directory: {err}"))?;
    Ok(dir.join("device_key.bin"))
}

fn load_or_create_mesh_identity(app: &tauri::AppHandle) -> Result<Keypair, String> {
    let path = mesh_key_path(app)?;

    if path.exists() {
        let bytes =
            fs::read(&path).map_err(|err| format!("failed to read mesh identity file: {err}"))?;
        return identity::Keypair::from_protobuf_encoding(&bytes)
            .map_err(|err| format!("failed to decode mesh identity: {err}"));
    }

    let keypair = identity::Keypair::generate_ed25519();
    let encoded = keypair
        .to_protobuf_encoding()
        .map_err(|err| format!("failed to encode mesh identity: {err}"))?;
    fs::write(&path, encoded)
        .map_err(|err| format!("failed to write mesh identity file: {err}"))?;
    Ok(keypair)
}

fn sanitize_relative_path(relative_path: &str) -> Result<PathBuf, String> {
    let rel = Path::new(relative_path);
    if rel.is_absolute() {
        return Err("path must be relative".to_string());
    }

    for component in rel.components() {
        match component {
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err("path traversal is not allowed".to_string());
            }
            Component::CurDir | Component::Normal(_) => {}
        }
    }

    Ok(rel.to_path_buf())
}

fn resolve_managed_path(app: &tauri::AppHandle, relative_path: &str) -> Result<PathBuf, String> {
    let base = resolve_base_dir(app)?;
    let safe_rel = sanitize_relative_path(relative_path)?;
    let full = base.join(safe_rel);
    Ok(full)
}

fn managed_relative_string(app: &tauri::AppHandle, full_path: &Path) -> Result<String, String> {
    let base = resolve_base_dir(app)?;
    full_path
        .strip_prefix(&base)
        .map_err(|_| "managed path escapes local base dir".to_string())
        .map(|path| path.to_string_lossy().replace('\\', "/"))
}

fn binary_exists(binary: &str) -> bool {
    if binary.trim().is_empty() {
        return false;
    }

    if let Some(paths) = std::env::var_os("PATH") {
        for path in std::env::split_paths(&paths) {
            let candidate = path.join(binary);
            if candidate.exists() {
                return true;
            }
            #[cfg(target_os = "windows")]
            {
                let candidate_exe = path.join(format!("{binary}.exe"));
                if candidate_exe.exists() {
                    return true;
                }
            }
        }
    }

    false
}

fn with_mesh_status<T>(
    app: &tauri::AppHandle,
    updater: impl FnOnce(&mut MeshRuntimeStatus) -> T,
) -> Option<T> {
    let state = app.state::<MeshRuntimeState>();
    let result = if let Ok(mut status) = state.status.lock() {
        Some(updater(&mut status))
    } else {
        None
    };
    result
}

fn mark_mesh_status(app: &tauri::AppHandle, request: &MeshJoinRequest, status_value: &str) {
    let _ = with_mesh_status(app, |status| {
        status.running = status_value != "dormant";
        status.status = status_value.to_string();
        status.user_address = Some(request.user_address.clone());
        status.published_agents = request_published_statuses(request);
        status.device_id = Some(request.device_id.clone());
        status.listen_multiaddrs = if status_value == "dormant" {
            Vec::new()
        } else {
            merge_mesh_listen_multiaddrs(&[], &request.relay_multiaddrs)
        };
        status.updated_at = now_ms();
        if status_value == "dormant" {
            status.published_agents.clear();
            status.peer_id = None;
            status.peers_discovered = 0;
            status.last_heartbeat_at = None;
            status.last_error = None;
        }
    });
}

fn mesh_error(app: &tauri::AppHandle, request: Option<&MeshJoinRequest>, message: String) {
    let _ = with_mesh_status(app, |status| {
        if let Some(req) = request {
            status.user_address = Some(req.user_address.clone());
            status.published_agents = request_published_statuses(req);
            status.device_id = Some(req.device_id.clone());
        }
        status.running = false;
        status.status = "error".to_string();
        status.last_error = Some(message);
        status.updated_at = now_ms();
    });
}

fn extract_peer_id_from_multiaddr(addr: &Multiaddr) -> Option<PeerId> {
    for protocol in addr.iter() {
        if let Protocol::P2p(peer_id) = protocol {
            return Some(peer_id);
        }
    }
    None
}

fn derive_relay_listen_multiaddrs(relay_multiaddrs: &[String]) -> Vec<Multiaddr> {
    let mut derived = Vec::new();
    let mut seen = HashSet::new();

    for raw in relay_multiaddrs {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }

        let Ok(addr) = trimmed.parse::<Multiaddr>() else {
            continue;
        };
        if extract_peer_id_from_multiaddr(&addr).is_none() {
            continue;
        }

        let mut circuit_addr = addr.clone();
        circuit_addr.push(Protocol::P2pCircuit);

        let circuit_key = circuit_addr.to_string();
        if seen.insert(circuit_key) {
            derived.push(circuit_addr);
        }
    }

    derived
}

fn derived_relay_listen_multiaddr_strings(relay_multiaddrs: &[String]) -> Vec<String> {
    derive_relay_listen_multiaddrs(relay_multiaddrs)
        .into_iter()
        .map(|addr| addr.to_string())
        .collect()
}

fn merge_mesh_listen_multiaddrs(existing: &[String], relay_multiaddrs: &[String]) -> Vec<String> {
    let mut merged = Vec::new();
    let mut seen = HashSet::new();

    for value in existing
        .iter()
        .chain(derived_relay_listen_multiaddr_strings(relay_multiaddrs).iter())
    {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            continue;
        }
        if seen.insert(trimmed.to_string()) {
            merged.push(trimmed.to_string());
        }
    }

    merged
}

fn derive_relay_peer_id_from_listen_multiaddrs(listen_multiaddrs: &[String]) -> Option<String> {
    for raw in listen_multiaddrs {
        let Ok(addr) = raw.trim().parse::<Multiaddr>() else {
            continue;
        };
        let mut first_peer_id: Option<String> = None;
        let mut has_circuit = false;
        for protocol in addr.iter() {
            match protocol {
                Protocol::P2p(peer_id) if first_peer_id.is_none() => {
                    first_peer_id = Some(peer_id.to_string());
                }
                Protocol::P2pCircuit => {
                    has_circuit = true;
                }
                _ => {}
            }
        }
        if has_circuit && first_peer_id.is_some() {
            return first_peer_id;
        }
    }

    None
}

const ENVELOPE_VERSION: u64 = 1;
const ENVELOPE_ALLOWED_SKEW_MS: u64 = 120_000;
const ENVELOPE_NONCE_WINDOW_MS: u64 = 5 * 60 * 1_000;
const PEER_STALE_MS: u64 = 90_000;
const PEER_EVICT_MS: u64 = 5 * 60 * 1_000;
const KAD_REFRESH_INTERVAL_MS: u64 = 5 * 60 * 1_000;
const RENDEZVOUS_DISCOVERY_INTERVAL_MS: u64 = 2 * 60 * 1_000;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct MeshEnvelope {
    v: u64,
    #[serde(rename = "type")]
    msg_type: String,
    peer_id: String,
    caps: Vec<String>,
    listen_addrs: Vec<String>,
    hai_id: String,
    agent_wallet: String,
    device_id: String,
    session_id: String,
    dna_hash: String,
    capabilities_hash: String,
    card: Option<MeshAgentCard>,
    ts_ms: u64,
    nonce: String,
    sig: Vec<u8>,
}

#[derive(Debug, Clone)]
struct MeshEnvelopeUnsigned {
    v: u64,
    msg_type: String,
    peer_id: String,
    caps: Vec<String>,
    listen_addrs: Vec<String>,
    hai_id: String,
    agent_wallet: String,
    device_id: String,
    session_id: String,
    dna_hash: String,
    capabilities_hash: String,
    card: Option<MeshAgentCard>,
    ts_ms: u64,
    nonce: String,
}

#[derive(Debug, Clone)]
struct PeerCacheEntry {
    last_seen_ms: u64,
    stale: bool,
    peer_id: String,
    hai_id: String,
    agent_wallet: String,
    device_id: String,
    caps: Vec<String>,
    listen_addrs: Vec<String>,
    card: Option<MeshAgentCard>,
    signal_count: u64,
    announce_count: u64,
    last_msg_type: String,
}

fn unsigned_envelope_bytes(value: &MeshEnvelopeUnsigned) -> Result<Vec<u8>, String> {
    serde_cbor::to_vec(&(
        value.v,
        &value.msg_type,
        &value.peer_id,
        &value.caps,
        &value.listen_addrs,
        &value.hai_id,
        &value.agent_wallet,
        &value.device_id,
        &value.session_id,
        &value.dna_hash,
        &value.capabilities_hash,
        &value.card,
        value.ts_ms,
        &value.nonce,
    ))
    .map_err(|err| format!("failed to encode unsigned CBOR envelope: {err}"))
}

fn build_signed_envelope_payload(
    local_key: &identity::Keypair,
    request: &MeshJoinRequest,
    published: &MeshPublishedAgent,
    hai_id: &str,
    msg_type: &str,
    peer_id: &str,
    caps: &[String],
    listen_multiaddrs: &[String],
    nonce: String,
) -> Result<Vec<u8>, String> {
    let computed_caps_hash = {
        let mut sorted = caps.to_vec();
        sorted.sort();
        sorted.join("|")
    };
    let unsigned = MeshEnvelopeUnsigned {
        v: ENVELOPE_VERSION,
        msg_type: msg_type.to_string(),
        peer_id: peer_id.to_string(),
        caps: caps.to_vec(),
        listen_addrs: listen_multiaddrs.to_vec(),
        hai_id: hai_id.to_string(),
        agent_wallet: published.agent_wallet.clone(),
        device_id: request.device_id.clone(),
        session_id: request.session_id.clone(),
        dna_hash: published.dna_hash.clone(),
        capabilities_hash: if published.capabilities_hash.trim().is_empty() {
            computed_caps_hash
        } else {
            published.capabilities_hash.clone()
        },
        card: published.public_card.clone(),
        ts_ms: now_ms(),
        nonce,
    };
    let sign_bytes = unsigned_envelope_bytes(&unsigned)?;
    let sig = local_key
        .sign(&sign_bytes)
        .map_err(|err| format!("failed to sign mesh envelope: {err}"))?;
    let envelope = MeshEnvelope {
        v: unsigned.v,
        msg_type: unsigned.msg_type,
        peer_id: unsigned.peer_id,
        caps: unsigned.caps,
        listen_addrs: unsigned.listen_addrs,
        hai_id: unsigned.hai_id,
        agent_wallet: unsigned.agent_wallet,
        device_id: unsigned.device_id,
        session_id: unsigned.session_id,
        dna_hash: unsigned.dna_hash,
        capabilities_hash: unsigned.capabilities_hash,
        card: unsigned.card,
        ts_ms: unsigned.ts_ms,
        nonce: unsigned.nonce,
        sig,
    };
    serde_cbor::to_vec(&envelope)
        .map_err(|err| format!("failed to encode signed CBOR envelope: {err}"))
}

fn decode_and_validate_envelope(
    payload: &[u8],
    seen_nonces: &mut HashMap<String, u64>,
) -> Result<MeshEnvelope, String> {
    let envelope = serde_cbor::from_slice::<MeshEnvelope>(payload)
        .map_err(|err| format!("invalid CBOR envelope: {err}"))?;

    if envelope.v != ENVELOPE_VERSION {
        return Err(format!("unsupported envelope version {}", envelope.v));
    }
    if envelope.msg_type != "presence" && envelope.msg_type != "announce" {
        return Err(format!("unsupported envelope type {}", envelope.msg_type));
    }
    if envelope.hai_id.len() != 6
        || !envelope
            .hai_id
            .chars()
            .all(|char| char.is_ascii_alphanumeric())
    {
        return Err("invalid envelope hai_id".to_string());
    }

    let now = now_ms();
    if envelope.ts_ms > now.saturating_add(ENVELOPE_ALLOWED_SKEW_MS)
        || now.saturating_sub(envelope.ts_ms) > ENVELOPE_ALLOWED_SKEW_MS
    {
        return Err("stale or future envelope timestamp".to_string());
    }

    seen_nonces.retain(|_, ts| now.saturating_sub(*ts) <= ENVELOPE_NONCE_WINDOW_MS);
    let nonce_key = format!("{}:{}", envelope.peer_id, envelope.nonce);
    if seen_nonces.contains_key(&nonce_key) {
        return Err("replay envelope nonce".to_string());
    }

    let peer_id = PeerId::from_str(&envelope.peer_id)
        .map_err(|err| format!("invalid envelope peer_id: {err}"))?;
    let public_key = {
        let multihash = peer_id.as_ref();
        if multihash.code() != 0 {
            return Err("peer_id does not contain inline public key material".to_string());
        }
        identity::PublicKey::try_decode_protobuf(multihash.digest())
            .map_err(|err| format!("failed to decode public key from peer_id: {err}"))?
    };

    let unsigned = MeshEnvelopeUnsigned {
        v: envelope.v,
        msg_type: envelope.msg_type.clone(),
        peer_id: envelope.peer_id.clone(),
        caps: envelope.caps.clone(),
        listen_addrs: envelope.listen_addrs.clone(),
        hai_id: envelope.hai_id.clone(),
        agent_wallet: envelope.agent_wallet.clone(),
        device_id: envelope.device_id.clone(),
        session_id: envelope.session_id.clone(),
        dna_hash: envelope.dna_hash.clone(),
        capabilities_hash: envelope.capabilities_hash.clone(),
        card: envelope.card.clone(),
        ts_ms: envelope.ts_ms,
        nonce: envelope.nonce.clone(),
    };
    let sign_bytes = unsigned_envelope_bytes(&unsigned)?;
    if !public_key.verify(&sign_bytes, &envelope.sig) {
        return Err("invalid envelope signature".to_string());
    }

    seen_nonces.insert(nonce_key, now);
    Ok(envelope)
}

fn next_nonce(counter: &mut u64, peer_id: &str) -> String {
    *counter = counter.saturating_add(1);
    format!("{}-{}", peer_id, *counter)
}

fn capability_dht_key(capability: &str) -> kad::RecordKey {
    kad::RecordKey::new(&format!("/compose/cap/{capability}/v1"))
}

fn capability_namespace(capability: &str) -> Result<rendezvous::Namespace, String> {
    rendezvous::Namespace::new(format!("compose/cap/{capability}/v1"))
        .map_err(|err| format!("invalid rendezvous namespace for capability '{capability}': {err}"))
}

fn extract_bootstrap_and_rendezvous_peers(
    request: &MeshJoinRequest,
) -> (Vec<Multiaddr>, HashSet<PeerId>) {
    let mut multiaddrs = Vec::new();
    let mut rendezvous_peers = HashSet::new();

    for raw in request
        .bootstrap_multiaddrs
        .iter()
        .chain(request.relay_multiaddrs.iter())
    {
        match raw.parse::<Multiaddr>() {
            Ok(addr) => {
                if let Some(peer_id) = extract_peer_id_from_multiaddr(&addr) {
                    rendezvous_peers.insert(peer_id);
                }
                multiaddrs.push(addr);
            }
            Err(err) => {
                eprintln!("[mesh] invalid bootstrap multiaddr '{}': {}", raw, err);
            }
        }
    }

    (multiaddrs, rendezvous_peers)
}

fn register_capabilities(
    swarm: &mut Swarm<MeshBehaviour>,
    request: &MeshJoinRequest,
    rendezvous_peers: &HashSet<PeerId>,
) {
    for capability in &request.capabilities {
        let key = capability_dht_key(capability);
        if let Err(err) = swarm.behaviour_mut().kad.start_providing(key) {
            eprintln!(
                "[mesh] kad start_providing failed for capability '{}': {}",
                capability, err
            );
        }
        let namespace = match capability_namespace(capability) {
            Ok(value) => value,
            Err(err) => {
                eprintln!("[mesh] {}", err);
                continue;
            }
        };
        for peer in rendezvous_peers {
            if let Err(err) =
                swarm
                    .behaviour_mut()
                    .rendezvous
                    .register(namespace.clone(), *peer, None)
            {
                eprintln!(
                    "[mesh] rendezvous register failed for capability '{}' @ {}: {}",
                    capability, peer, err
                );
            }
            swarm.behaviour_mut().rendezvous.discover(
                Some(namespace.clone()),
                None,
                Some(128),
                *peer,
            );
        }
    }
}

fn discover_capabilities(
    swarm: &mut Swarm<MeshBehaviour>,
    request: &MeshJoinRequest,
    rendezvous_peers: &HashSet<PeerId>,
) {
    for capability in &request.capabilities {
        let namespace = match capability_namespace(capability) {
            Ok(value) => value,
            Err(err) => {
                eprintln!("[mesh] {}", err);
                continue;
            }
        };
        for peer in rendezvous_peers {
            swarm.behaviour_mut().rendezvous.discover(
                Some(namespace.clone()),
                None,
                Some(128),
                *peer,
            );
        }
    }
}

fn apply_kad_mode_from_autonat(swarm: &mut Swarm<MeshBehaviour>, nat_status: &autonat::NatStatus) {
    match nat_status {
        autonat::NatStatus::Public(_) => {
            swarm.behaviour_mut().kad.set_mode(Some(kad::Mode::Server))
        }
        autonat::NatStatus::Private | autonat::NatStatus::Unknown => {
            swarm.behaviour_mut().kad.set_mode(Some(kad::Mode::Client))
        }
    }
}

fn mesh_runtime_state_label(
    listen_multiaddrs: &[String],
    active_peers: usize,
    connected_peers: usize,
) -> &'static str {
    if !listen_multiaddrs.is_empty() || active_peers > 0 || connected_peers > 0 {
        "online"
    } else {
        "connecting"
    }
}

fn recompute_peer_cache_status(peer_cache: &mut HashMap<String, PeerCacheEntry>) -> usize {
    let now = now_ms();
    let mut active = 0usize;
    peer_cache.retain(|_, entry| now.saturating_sub(entry.last_seen_ms) <= PEER_EVICT_MS);
    for entry in peer_cache.values_mut() {
        if now.saturating_sub(entry.last_seen_ms) > PEER_STALE_MS {
            entry.stale = true;
        } else {
            entry.stale = false;
            active += 1;
        }
    }
    active
}

fn peer_cache_key(peer_id: &str, agent_wallet: &str, hai_id: &str) -> String {
    let normalized_wallet =
        normalize_wallet(agent_wallet).unwrap_or_else(|| agent_wallet.trim().to_lowercase());
    format!("{peer_id}:{normalized_wallet}:{}", hai_id.trim())
}

fn emit_peer_index(app: &tauri::AppHandle, peer_cache: &HashMap<String, PeerCacheEntry>) {
    let peers = peer_cache
        .iter()
        .map(|(id, entry)| {
            let node_distance = if entry.stale {
                3
            } else if !entry.listen_addrs.is_empty() {
                1
            } else {
                2
            };
            serde_json::json!({
                "id": id,
                "peerId": entry.peer_id,
                "agentWallet": entry.agent_wallet,
                "haiId": entry.hai_id,
                "deviceId": entry.device_id,
                "lastSeenAt": entry.last_seen_ms,
                "stale": entry.stale,
                "caps": entry.caps,
                "listenMultiaddrs": entry.listen_addrs,
                "card": entry.card,
                "signalCount": entry.signal_count,
                "announceCount": entry.announce_count,
                "lastMessageType": if entry.last_msg_type.is_empty() { serde_json::Value::Null } else { serde_json::Value::String(entry.last_msg_type.clone()) },
                "nodeDistance": node_distance,
            })
        })
        .collect::<Vec<_>>();
    let _ = app.emit(
        "mesh-peer-index",
        serde_json::json!({ "peers": peers, "updatedAt": now_ms() }),
    );
}

fn build_mesh_swarm(
    local_key: identity::Keypair,
    request: &MeshJoinRequest,
) -> Result<
    (
        Swarm<MeshBehaviour>,
        IdentTopic,
        IdentTopic,
        IdentTopic,
        IdentTopic,
        HashSet<PeerId>,
    ),
    String,
> {
    let local_peer_id = PeerId::from(local_key.public());

    let gossipsub_config = gossipsub::ConfigBuilder::default()
        .validation_mode(ValidationMode::Strict)
        .heartbeat_interval(Duration::from_secs(1))
        .build()
        .map_err(|err| format!("failed to build gossipsub config: {err}"))?;
    let mut gossipsub = gossipsub::Behaviour::new(
        MessageAuthenticity::Signed(local_key.clone()),
        gossipsub_config,
    )
    .map_err(|err| format!("failed to initialize gossipsub: {err}"))?;

    let global_topic = IdentTopic::new(request.gossip_topic.clone());
    let announce_topic = IdentTopic::new(request.announce_topic.clone());
    let manifest_topic = IdentTopic::new(request.manifest_topic.clone());
    let conclave_topic = IdentTopic::new(request.conclave_topic.clone());
    gossipsub
        .subscribe(&global_topic)
        .map_err(|err| format!("failed to subscribe gossipsub global topic: {err}"))?;
    gossipsub
        .subscribe(&announce_topic)
        .map_err(|err| format!("failed to subscribe gossipsub announce topic: {err}"))?;
    gossipsub
        .subscribe(&manifest_topic)
        .map_err(|err| format!("failed to subscribe gossipsub manifest topic: {err}"))?;
    gossipsub
        .subscribe(&conclave_topic)
        .map_err(|err| format!("failed to subscribe gossipsub conclave topic: {err}"))?;

    let kad_stream_protocol = StreamProtocol::try_from_owned(request.kad_protocol.clone())
        .map_err(|_| format!("invalid kadProtocol '{}'", request.kad_protocol))?;
    let mut kad_config = kad::Config::new(kad_stream_protocol);
    kad_config.set_query_timeout(Duration::from_secs(30));
    let mut kad = kad::Behaviour::with_config(
        local_peer_id,
        kad::store::MemoryStore::new(local_peer_id),
        kad_config,
    );
    // Automatic mode: starts as client, flips to server when external reachability is confirmed.
    kad.set_mode(None);

    let connection_limits = connection_limits::Behaviour::new(
        connection_limits::ConnectionLimits::default()
            .with_max_pending_incoming(Some(128))
            .with_max_pending_outgoing(Some(128))
            .with_max_established_incoming(Some(384))
            .with_max_established_outgoing(Some(384))
            .with_max_established_per_peer(Some(8))
            .with_max_established(Some(512)),
    );

    let mut swarm = SwarmBuilder::with_existing_identity(local_key.clone())
        .with_tokio()
        .with_tcp(
            tcp::Config::default().nodelay(true),
            noise::Config::new,
            yamux::Config::default,
        )
        .map_err(|err| format!("failed to initialize tcp transport: {err}"))?
        .with_dns()
        .map_err(|err| format!("failed to initialize dns transport: {err}"))?
        .with_relay_client(noise::Config::new, yamux::Config::default)
        .map_err(|err| format!("failed to initialize relay transport: {err}"))?
        .with_behaviour(|key, relay_client| MeshBehaviour {
            relay_client,
            dcutr: dcutr::Behaviour::new(local_peer_id),
            autonat: autonat::Behaviour::new(local_peer_id, autonat::Config::default()),
            ping: ping::Behaviour::new(ping::Config::new()),
            identify: identify::Behaviour::new(identify::Config::new(
                "/compose-market/local/1.0.0".to_string(),
                key.public(),
            )),
            gossipsub,
            kad,
            rendezvous: rendezvous::client::Behaviour::new(key.clone()),
            connection_limits,
        })
        .map_err(|err| format!("failed to initialize mesh behaviour: {err}"))?
        .with_swarm_config(|config| config.with_idle_connection_timeout(Duration::from_secs(120)))
        .build();

    swarm
        .listen_on(
            "/ip4/0.0.0.0/tcp/0"
                .parse::<Multiaddr>()
                .map_err(|err| format!("invalid listen address: {err}"))?,
        )
        .map_err(|err| format!("failed to start listening: {err}"))?;

    let (bootstrap_multiaddrs, rendezvous_peers) = extract_bootstrap_and_rendezvous_peers(request);
    for multiaddr in bootstrap_multiaddrs {
        if let Some(remote_peer_id) = extract_peer_id_from_multiaddr(&multiaddr) {
            swarm
                .behaviour_mut()
                .kad
                .add_address(&remote_peer_id, multiaddr.clone());
            swarm
                .behaviour_mut()
                .autonat
                .add_server(remote_peer_id, Some(multiaddr.clone()));
        }
        if let Err(err) = swarm.dial(multiaddr.clone()) {
            eprintln!("[mesh] bootstrap dial failed for {}: {}", multiaddr, err);
        }
    }

    for relay_listen_addr in derive_relay_listen_multiaddrs(&request.relay_multiaddrs) {
        if let Err(err) = swarm.listen_on(relay_listen_addr.clone()) {
            eprintln!(
                "[mesh] relay circuit listen failed for {}: {}",
                relay_listen_addr, err
            );
        }
    }

    if let Err(err) = swarm.behaviour_mut().kad.bootstrap() {
        eprintln!("[mesh] initial kad bootstrap failed: {}", err);
    }

    Ok((
        swarm,
        global_topic,
        announce_topic,
        manifest_topic,
        conclave_topic,
        rendezvous_peers,
    ))
}

async fn run_mesh_loop(
    app: tauri::AppHandle,
    request: MeshJoinRequest,
    mut stop_rx: oneshot::Receiver<()>,
    mut command_rx: mpsc::UnboundedReceiver<MeshLoopCommand>,
) {
    let local_key = match load_or_create_mesh_identity(&app) {
        Ok(value) => value,
        Err(err) => {
            mesh_error(&app, Some(&request), err);
            return;
        }
    };
    let local_peer_id = PeerId::from(local_key.public()).to_string();

    let (
        mut swarm,
        global_topic,
        announce_topic,
        manifest_topic,
        _conclave_topic,
        rendezvous_peers,
    ) = match build_mesh_swarm(local_key.clone(), &request) {
        Ok(value) => value,
        Err(err) => {
            mesh_error(&app, Some(&request), err);
            return;
        }
    };

    register_capabilities(&mut swarm, &request, &rendezvous_peers);
    discover_capabilities(&mut swarm, &request, &rendezvous_peers);

    let mut connected_peers: HashSet<PeerId> = HashSet::new();
    let mut peer_cache: HashMap<String, PeerCacheEntry> = HashMap::new();
    let mut seen_nonces: HashMap<String, u64> = HashMap::new();
    let mut nonce_counter: u64 = 0;

    let mut heartbeat_interval = tokio::time::interval(Duration::from_millis(request.heartbeat_ms));
    let mut kad_refresh_interval =
        tokio::time::interval(Duration::from_millis(KAD_REFRESH_INTERVAL_MS));
    let mut peer_prune_interval = tokio::time::interval(Duration::from_secs(30));
    let mut rendezvous_discovery_interval =
        tokio::time::interval(Duration::from_millis(RENDEZVOUS_DISCOVERY_INTERVAL_MS));

    let _ = with_mesh_status(&app, |status| {
        status.running = true;
        status.status = "connecting".to_string();
        status.user_address = Some(request.user_address.clone());
        status.published_agents = request_published_statuses(&request);
        status.device_id = Some(request.device_id.clone());
        status.peer_id = Some(local_peer_id.clone());
        status.listen_multiaddrs = merge_mesh_listen_multiaddrs(&[], &request.relay_multiaddrs);
        status.peers_discovered = 0;
        status.last_heartbeat_at = None;
        status.last_error = None;
        status.updated_at = now_ms();
    });
    for published in &request.published_agents {
        let _ = queue_manifest_publication_request(
            &app,
            &published.agent_wallet,
            "mesh-runtime-online",
        );
    }

    loop {
        tokio::select! {
            _ = &mut stop_rx => {
                // Best-effort unregister namespaces from known rendezvous peers before shutdown.
                for capability in &request.capabilities {
                    if let Ok(namespace) = capability_namespace(capability) {
                        for peer in &rendezvous_peers {
                            swarm.behaviour_mut().rendezvous.unregister(namespace.clone(), *peer);
                        }
                    }
                }
                mark_mesh_status(&app, &request, "dormant");
                break;
            }
            Some(command) = command_rx.recv() => {
                match command {
                    MeshLoopCommand::PublishManifest { manifest, reply } => {
                        let result = serde_cbor::to_vec(&manifest)
                            .map_err(|err| format!("failed to encode signed manifest: {err}"))
                            .and_then(|payload| {
                                normalize_manifest_publish_outcome(
                                    swarm
                                        .behaviour_mut()
                                        .gossipsub
                                        .publish(manifest_topic.clone(), payload),
                                    &manifest,
                                )
                            });

                        if let Err(err) = &result {
                            let _ = with_mesh_status(&app, |status| {
                                status.last_error = Some(err.clone());
                                status.updated_at = now_ms();
                            });
                        } else {
                            let _ = with_mesh_status(&app, |status| {
                                status.updated_at = now_ms();
                            });
                        }
                        let _ = reply.send(result);
                    }
                }
            }
            _ = kad_refresh_interval.tick() => {
                if let Err(err) = swarm.behaviour_mut().kad.bootstrap() {
                    eprintln!("[mesh] periodic kad bootstrap failed: {}", err);
                }
                swarm
                    .behaviour_mut()
                    .kad
                    .get_closest_peers(local_peer_id.as_bytes().to_vec());
                let _ = with_mesh_status(&app, |status| {
                    status.updated_at = now_ms();
                });
            }
            _ = rendezvous_discovery_interval.tick() => {
                discover_capabilities(&mut swarm, &request, &rendezvous_peers);
            }
            _ = peer_prune_interval.tick() => {
                let active = recompute_peer_cache_status(&mut peer_cache);
                emit_peer_index(&app, &peer_cache);
                let _ = with_mesh_status(&app, |status| {
                    status.peers_discovered = active as u32;
                    status.status = mesh_runtime_state_label(
                        &status.listen_multiaddrs,
                        active,
                        connected_peers.len(),
                    )
                    .to_string();
                    status.updated_at = now_ms();
                });
            }
            _ = heartbeat_interval.tick() => {
                let listen_multiaddrs = with_mesh_status(&app, |status| status.listen_multiaddrs.clone()).unwrap_or_default();
                let mut heartbeat_error: Option<String> = None;

                for published in &request.published_agents {
                    let hai_id = derive_hai_id(&published.agent_wallet, &request.user_address, &request.device_id);
                    let caps = if published.capabilities.is_empty() {
                        default_capabilities(&published.agent_wallet)
                    } else {
                        published.capabilities.clone()
                    };

                    let presence_payload = build_signed_envelope_payload(
                        &local_key,
                        &request,
                        published,
                        &hai_id,
                        "presence",
                        &local_peer_id,
                        &caps,
                        &listen_multiaddrs,
                        next_nonce(&mut nonce_counter, &local_peer_id),
                    );
                    let announce_payload = build_signed_envelope_payload(
                        &local_key,
                        &request,
                        published,
                        &hai_id,
                        "announce",
                        &local_peer_id,
                        &caps,
                        &listen_multiaddrs,
                        next_nonce(&mut nonce_counter, &local_peer_id),
                    );

                    let publish_presence = presence_payload.and_then(|payload| {
                        swarm
                            .behaviour_mut()
                            .gossipsub
                            .publish(global_topic.clone(), payload)
                            .map_err(|err| format!("presence publish failed: {err}"))
                    });
                    let publish_announce = announce_payload.and_then(|payload| {
                        swarm
                            .behaviour_mut()
                            .gossipsub
                            .publish(announce_topic.clone(), payload)
                            .map_err(|err| format!("announce publish failed: {err}"))
                    });

                    if let Err(err) = publish_presence {
                        heartbeat_error.get_or_insert(err);
                    }
                    if let Err(err) = publish_announce {
                        heartbeat_error.get_or_insert(err);
                    }
                }

                let _ = with_mesh_status(&app, |status| {
                    status.last_heartbeat_at = Some(now_ms());
                    status.updated_at = now_ms();
                    if let Some(err) = heartbeat_error {
                        status.last_error = Some(err);
                        status.status = mesh_runtime_state_label(
                            &status.listen_multiaddrs,
                            status.peers_discovered as usize,
                            connected_peers.len(),
                        )
                        .to_string();
                    } else {
                        status.last_error = None;
                        status.status = "online".to_string();
                    }
                });
            }
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::NewListenAddr { address, .. } => {
                        let addr = address.to_string();
                        let mut inserted = false;
                        let _ = with_mesh_status(&app, |status| {
                            if !status.listen_multiaddrs.contains(&addr) {
                                status.listen_multiaddrs.push(addr.clone());
                                inserted = true;
                            }
                            if status.status == "connecting" {
                                status.status = "online".to_string();
                            }
                            status.updated_at = now_ms();
                        });
                        if inserted {
                            append_mesh_log_to_published_agents(
                                &app,
                                &request.published_agents,
                                &format!("mesh listen address ready: {addr}"),
                            );
                        }
                    }
                    SwarmEvent::ExternalAddrConfirmed { address } => {
                        let addr = address.to_string();
                        let mut inserted = false;
                        let _ = with_mesh_status(&app, |status| {
                            if !status.listen_multiaddrs.contains(&addr) {
                                status.listen_multiaddrs.push(addr.clone());
                                inserted = true;
                            }
                            status.status = "online".to_string();
                            status.updated_at = now_ms();
                        });
                        if inserted {
                            append_mesh_log_to_published_agents(
                                &app,
                                &request.published_agents,
                                &format!("mesh relay address confirmed: {addr}"),
                            );
                        }
                    }
                    SwarmEvent::ExternalAddrExpired { address } => {
                        let addr = address.to_string();
                        let _ = with_mesh_status(&app, |status| {
                            status.listen_multiaddrs.retain(|item| item != &addr);
                            status.status = mesh_runtime_state_label(
                                &status.listen_multiaddrs,
                                status.peers_discovered as usize,
                                connected_peers.len(),
                            )
                            .to_string();
                            status.updated_at = now_ms();
                        });
                    }
                    SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                        connected_peers.insert(peer_id);
                        let _ = with_mesh_status(&app, |status| {
                            status.peers_discovered = status.peers_discovered.max(connected_peers.len() as u32);
                            status.status = "online".to_string();
                            status.updated_at = now_ms();
                        });
                        append_mesh_log_to_published_agents(
                            &app,
                            &request.published_agents,
                            &format!("mesh peer connected: {peer_id}"),
                        );
                    }
                    SwarmEvent::ConnectionClosed { peer_id, .. } => {
                        connected_peers.remove(&peer_id);
                        let active = recompute_peer_cache_status(&mut peer_cache);
                        let _ = with_mesh_status(&app, |status| {
                            status.peers_discovered = active as u32;
                            status.status = mesh_runtime_state_label(
                                &status.listen_multiaddrs,
                                active,
                                connected_peers.len(),
                            )
                            .to_string();
                            status.updated_at = now_ms();
                        });
                        append_mesh_log_to_published_agents(
                            &app,
                            &request.published_agents,
                            &format!("mesh peer disconnected: {peer_id}"),
                        );
                    }
                    SwarmEvent::Behaviour(MeshBehaviourEvent::Identify(event)) => {
                        if let identify::Event::Received { peer_id, info, .. } = event {
                            for addr in info.listen_addrs {
                                swarm.behaviour_mut().kad.add_address(&peer_id, addr.clone());
                                swarm.behaviour_mut().autonat.add_server(peer_id, Some(addr));
                            }
                        }
                        let _ = with_mesh_status(&app, |status| {
                            status.updated_at = now_ms();
                        });
                    }
                    SwarmEvent::Behaviour(MeshBehaviourEvent::Autonat(event)) => {
                        if let autonat::Event::StatusChanged { new, .. } = &event {
                            apply_kad_mode_from_autonat(&mut swarm, new);
                            if let Err(err) = swarm.behaviour_mut().kad.bootstrap() {
                                eprintln!("[mesh] kad bootstrap after autonat change failed: {}", err);
                            }
                        }
                        let _ = with_mesh_status(&app, |status| {
                            status.updated_at = now_ms();
                        });
                    }
                    SwarmEvent::Behaviour(MeshBehaviourEvent::Gossipsub(event)) => {
                        if let gossipsub::Event::Message { message, .. } = event {
                            if message.topic == global_topic.hash() || message.topic == announce_topic.hash() {
                                match decode_and_validate_envelope(&message.data, &mut seen_nonces) {
                                    Ok(envelope) => {
                                        let cache_key = peer_cache_key(
                                            &envelope.peer_id,
                                            &envelope.agent_wallet,
                                            &envelope.hai_id,
                                        );
                                        let entry = peer_cache.entry(cache_key).or_insert(PeerCacheEntry {
                                            last_seen_ms: envelope.ts_ms,
                                            stale: false,
                                            peer_id: envelope.peer_id.clone(),
                                            hai_id: envelope.hai_id.clone(),
                                            agent_wallet: envelope.agent_wallet.clone(),
                                            device_id: envelope.device_id.clone(),
                                            caps: envelope.caps.clone(),
                                            listen_addrs: envelope.listen_addrs.clone(),
                                            card: envelope.card.clone(),
                                            signal_count: 0,
                                            announce_count: 0,
                                            last_msg_type: envelope.msg_type.clone(),
                                        });
                                        entry.last_seen_ms = envelope.ts_ms;
                                        entry.stale = false;
                                        entry.peer_id = envelope.peer_id.clone();
                                        entry.hai_id = envelope.hai_id.clone();
                                        entry.agent_wallet = envelope.agent_wallet.clone();
                                        entry.device_id = envelope.device_id.clone();
                                        entry.caps = envelope.caps;
                                        entry.listen_addrs = envelope.listen_addrs.clone();
                                        entry.card = envelope.card.clone();
                                        entry.signal_count = entry.signal_count.saturating_add(1);
                                        if envelope.msg_type == "announce" {
                                            entry.announce_count = entry.announce_count.saturating_add(1);
                                        }
                                        entry.last_msg_type = envelope.msg_type.clone();

                                        for raw_addr in envelope.listen_addrs {
                                            if let Ok(addr) = raw_addr.parse::<Multiaddr>() {
                                                if let Some(peer) = extract_peer_id_from_multiaddr(&addr) {
                                                    swarm.behaviour_mut().kad.add_address(&peer, addr);
                                                }
                                            }
                                        }

                                        let active = recompute_peer_cache_status(&mut peer_cache);
                                        emit_peer_index(&app, &peer_cache);
                                        let _ = with_mesh_status(&app, |status| {
                                            status.peers_discovered = active as u32;
                                            status.status = mesh_runtime_state_label(
                                                &status.listen_multiaddrs,
                                                active,
                                                connected_peers.len(),
                                            )
                                            .to_string();
                                            status.updated_at = now_ms();
                                        });
                                    }
                                    Err(err) => {
                                        let _ = with_mesh_status(&app, |status| {
                                            status.last_error = Some(format!("invalid gossip envelope: {err}"));
                                            status.updated_at = now_ms();
                                        });
                                    }
                                }
                            }
                        }
                    }
                    SwarmEvent::Behaviour(MeshBehaviourEvent::Kad(_event)) => {
                        let _ = with_mesh_status(&app, |status| {
                            status.updated_at = now_ms();
                        });
                    }
                    SwarmEvent::Behaviour(MeshBehaviourEvent::Rendezvous(event)) => {
                        if let rendezvous::client::Event::Discovered { registrations, .. } = event {
                            for registration in registrations {
                                let peer_id = registration.record.peer_id();
                                for addr in registration.record.addresses() {
                                    swarm.behaviour_mut().kad.add_address(&peer_id, addr.clone());
                                    let _ = swarm.dial(addr.clone());
                                }
                            }
                        }
                        let _ = with_mesh_status(&app, |status| {
                            status.updated_at = now_ms();
                        });
                    }
                    SwarmEvent::Behaviour(MeshBehaviourEvent::RelayClient(event)) => {
                        let _ = event;
                        let _ = with_mesh_status(&app, |status| {
                            status.updated_at = now_ms();
                        });
                    }
                    SwarmEvent::Behaviour(MeshBehaviourEvent::Dcutr(event)) => {
                        let _ = event;
                        let _ = with_mesh_status(&app, |status| {
                            status.updated_at = now_ms();
                        });
                    }
                    SwarmEvent::Behaviour(MeshBehaviourEvent::Ping(event)) => {
                        let _ = event;
                        let _ = with_mesh_status(&app, |status| {
                            status.updated_at = now_ms();
                        });
                    }
                    SwarmEvent::OutgoingConnectionError { error, .. } => {
                        let _ = with_mesh_status(&app, |status| {
                            status.last_error = Some(format!("outgoing connection failed: {error}"));
                            if status.peers_discovered == 0 {
                                status.status = "connecting".to_string();
                            }
                            status.updated_at = now_ms();
                        });
                    }
                    SwarmEvent::IncomingConnectionError { error, .. } => {
                        let _ = with_mesh_status(&app, |status| {
                            status.last_error = Some(format!("incoming connection failed: {error}"));
                            if status.peers_discovered == 0 {
                                status.status = "connecting".to_string();
                            }
                            status.updated_at = now_ms();
                        });
                    }
                    _ => {}
                }
            }
        }
    }

    if let Ok(mut guard) = app.state::<MeshRuntimeState>().command_tx.lock() {
        *guard = None;
    }
}

#[tauri::command]
fn get_local_paths(app: tauri::AppHandle) -> Result<LocalPaths, String> {
    let base_dir = resolve_base_dir(&app)?;
    let state_file = base_dir.join("state.json");
    let agents_dir = base_dir.join("agents");
    let skills_dir = base_dir.join("skills");

    fs::create_dir_all(&agents_dir)
        .map_err(|err| format!("failed to create agents directory: {err}"))?;
    fs::create_dir_all(&skills_dir)
        .map_err(|err| format!("failed to create skills directory: {err}"))?;

    Ok(LocalPaths {
        base_dir: base_dir.to_string_lossy().to_string(),
        state_file: state_file.to_string_lossy().to_string(),
        agents_dir: agents_dir.to_string_lossy().to_string(),
        skills_dir: skills_dir.to_string_lossy().to_string(),
    })
}

#[tauri::command]
fn set_local_base_dir(app: tauri::AppHandle, new_base_dir: String) -> Result<LocalPaths, String> {
    let trimmed = new_base_dir.trim();
    if trimmed.is_empty() {
        return Err("base directory path cannot be empty".to_string());
    }

    let new_path = PathBuf::from(trimmed);

    // Must be an absolute path
    if !new_path.is_absolute() {
        return Err("base directory must be an absolute path".to_string());
    }

    // Create the new directory tree
    fs::create_dir_all(&new_path)
        .map_err(|err| format!("failed to create new base directory: {err}"))?;
    fs::create_dir_all(new_path.join("agents"))
        .map_err(|err| format!("failed to create agents directory: {err}"))?;
    fs::create_dir_all(new_path.join("skills"))
        .map_err(|err| format!("failed to create skills directory: {err}"))?;

    // Copy existing state files from the old base to the new base (if they exist)
    let old_base = resolve_base_dir(&app)?;
    if old_base != new_path {
        for name in ["state.json", "daemon_state.json"] {
            let src = old_base.join(name);
            let dst = new_path.join(name);
            if src.exists() && !dst.exists() {
                let _ = fs::copy(&src, &dst);
            }
        }
    }

    // Persist the override
    let app_data = app
        .path()
        .app_data_dir()
        .map_err(|err| format!("failed to resolve app data directory: {err}"))?;
    fs::write(app_data.join("base_dir_override.txt"), trimmed)
        .map_err(|err| format!("failed to persist base dir override: {err}"))?;

    // Return refreshed paths
    get_local_paths(app)
}

#[tauri::command]
fn load_local_state(app: tauri::AppHandle) -> Result<String, String> {
    let mut state_value = load_local_state_value(&app)?;
    let summary = sync_all_local_agent_workspaces(&app, &mut state_value)?;
    if summary.state_dirty {
        save_local_state_value(&app, &state_value)?;
    }
    queue_manifest_publication_requests_from_state(
        &app,
        &state_value,
        &summary.manifest_dirty_agents,
        "local-agent-public-state-changed",
    )?;

    serde_json::to_string(&state_value)
        .map_err(|err| format!("failed to serialize local state JSON: {err}"))
        .and_then(|raw| normalize_local_state_json(&raw))
}

#[tauri::command]
fn save_local_state(app: tauri::AppHandle, state_json: String) -> Result<(), String> {
    let base_dir = resolve_base_dir(&app)?;
    let state_file = base_dir.join("state.json");
    let previous_state = if state_file.exists() {
        load_local_state_value(&app)?
    } else {
        serde_json::json!({})
    };

    let normalized = normalize_local_state_json(&state_json)?;
    let next_state = serde_json::from_str::<serde_json::Value>(&normalized)
        .map_err(|err| format!("failed to parse normalized state JSON: {err}"))?;
    write_string_atomically(&state_file, &normalized, "local state")?;
    let changed_wallets = changed_manifest_agent_wallets(&previous_state, &next_state);
    queue_manifest_publication_requests_from_state(
        &app,
        &next_state,
        &changed_wallets,
        "local-state-updated",
    )?;
    Ok(())
}

#[tauri::command]
fn ensure_local_dir(app: tauri::AppHandle, relative_path: String) -> Result<String, String> {
    let dir = resolve_managed_path(&app, &relative_path)?;
    fs::create_dir_all(&dir).map_err(|err| format!("failed to create directory: {err}"))?;
    Ok(dir.to_string_lossy().to_string())
}

fn collect_local_files_recursive(
    app: &tauri::AppHandle,
    dir: &Path,
    output: &mut Vec<String>,
) -> Result<(), String> {
    let entries = fs::read_dir(dir).map_err(|err| format!("failed to read directory: {err}"))?;
    for entry in entries {
        let entry = entry.map_err(|err| format!("failed to read directory entry: {err}"))?;
        let path = entry.path();
        if path.is_dir() {
            collect_local_files_recursive(app, &path, output)?;
        } else if path.is_file() {
            output.push(managed_relative_string(app, &path)?);
        }
    }
    Ok(())
}

#[tauri::command]
fn list_local_files(app: tauri::AppHandle, relative_path: String) -> Result<Vec<String>, String> {
    let target = resolve_managed_path(&app, &relative_path)?;
    if !target.exists() {
        return Ok(Vec::new());
    }

    let mut output = Vec::new();
    if target.is_file() {
        output.push(managed_relative_string(&app, &target)?);
    } else {
        collect_local_files_recursive(&app, &target, &mut output)?;
    }
    output.sort();
    Ok(output)
}

#[tauri::command]
fn write_local_file(
    app: tauri::AppHandle,
    relative_path: String,
    content: String,
) -> Result<String, String> {
    let file_path = resolve_managed_path(&app, &relative_path)?;
    if let Some(parent) = file_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| format!("failed to create parent directory: {err}"))?;
    }
    fs::write(&file_path, content).map_err(|err| format!("failed to write file: {err}"))?;
    Ok(file_path.to_string_lossy().to_string())
}

#[tauri::command]
fn read_local_file(app: tauri::AppHandle, relative_path: String) -> Result<String, String> {
    let file_path = resolve_managed_path(&app, &relative_path)?;
    if !file_path.exists() {
        return Err("file not found".to_string());
    }
    fs::read_to_string(&file_path).map_err(|err| format!("failed to read file: {err}"))
}

#[tauri::command]
fn remove_local_path(app: tauri::AppHandle, relative_path: String) -> Result<bool, String> {
    let target = resolve_managed_path(&app, &relative_path)?;
    if !target.exists() {
        return Ok(false);
    }

    if target.is_dir() {
        fs::remove_dir_all(&target).map_err(|err| format!("failed to remove directory: {err}"))?;
    } else {
        fs::remove_file(&target).map_err(|err| format!("failed to remove file: {err}"))?;
    }

    Ok(true)
}

#[tauri::command]
fn check_missing_binaries(binaries: Vec<String>) -> Vec<String> {
    binaries
        .into_iter()
        .filter(|bin| !binary_exists(bin))
        .collect()
}

#[tauri::command]
fn consume_pending_deep_links(state: tauri::State<PendingDeepLinks>) -> Vec<String> {
    if let Ok(mut guard) = state.0.lock() {
        let values = guard.clone();
        guard.clear();
        values
    } else {
        Vec::new()
    }
}

fn daemon_agent_workspace_relative(agent_wallet: &str) -> String {
    format!("agents/{}", agent_wallet.to_lowercase())
}

fn daemon_agent_workspace_path(
    app: &tauri::AppHandle,
    agent_wallet: &str,
) -> Result<PathBuf, String> {
    resolve_managed_path(app, &daemon_agent_workspace_relative(agent_wallet))
}

fn daemon_agent_logs_path(app: &tauri::AppHandle, agent_wallet: &str) -> Result<PathBuf, String> {
    Ok(daemon_agent_workspace_path(app, agent_wallet)?.join("runtime.log"))
}

fn bootstrap_agent_workspace(
    app: &tauri::AppHandle,
    payload: &DaemonInstallPayload,
) -> Result<(), String> {
    let workspace = daemon_agent_workspace_path(app, &payload.agent_wallet)?;
    fs::create_dir_all(&workspace)
        .map_err(|err| format!("failed to create agent workspace: {err}"))?;
    fs::create_dir_all(workspace.join("skills"))
        .map_err(|err| format!("failed to create skills dir: {err}"))?;
    fs::create_dir_all(workspace.join("skills").join("generated"))
        .map_err(|err| format!("failed to create generated skills dir: {err}"))?;

    Ok(())
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct LocalAgentStructuredSkill {
    name: String,
    markdown: String,
}

#[derive(Debug, Clone)]
struct LocalAgentHeartbeatOutcome {
    reply: String,
    skill_path: Option<String>,
    last_result: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct LocalAgentConversationMessage {
    role: String,
    content: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct LocalAgentToolAction {
    #[serde(rename = "type")]
    kind: String,
    service: Option<String>,
    method: Option<String>,
    path: Option<String>,
    content: Option<String>,
    command: Option<String>,
    args: Option<Vec<String>>,
    cwd: Option<String>,
    body: Option<serde_json::Value>,
    title: Option<String>,
    summary: Option<String>,
    access_price_usdc: Option<String>,
}

#[derive(Debug, Clone)]
struct LocalAgentStructuredReply {
    reply: String,
    report: Option<serde_json::Value>,
    skill: Option<LocalAgentStructuredSkill>,
    actions: Vec<LocalAgentToolAction>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct LocalAgentConversationResult {
    reply: String,
    report: Option<serde_json::Value>,
    skill: Option<LocalAgentStructuredSkill>,
    authored_skill_id: Option<String>,
    authored_skill_path: Option<String>,
    raw: String,
}

#[derive(Debug, Clone)]
struct LocalAgentTurnCharge {
    cost_micros: u64,
    tx_hash: Option<String>,
}

#[derive(Debug, Clone)]
struct LocalAgentModelTurn {
    raw: String,
    charge: Option<LocalAgentTurnCharge>,
}

fn local_agent_slug(value: &str) -> String {
    let mut output = String::new();
    let mut last_dash = false;
    for ch in value.trim().to_lowercase().chars() {
        if ch.is_ascii_alphanumeric() {
            output.push(ch);
            last_dash = false;
        } else if !last_dash {
            output.push('-');
            last_dash = true;
        }
    }
    let trimmed = output.trim_matches('-').to_string();
    if trimmed.is_empty() {
        "skill".to_string()
    } else {
        trimmed
    }
}

fn local_agent_authored_skill_id(name: &str) -> String {
    format!("agent:{}", local_agent_slug(name))
}

fn local_agent_reports_dir(app: &tauri::AppHandle, agent_wallet: &str) -> Result<PathBuf, String> {
    Ok(daemon_agent_workspace_path(app, agent_wallet)?.join("reports"))
}

fn format_usdc_micros(value: u64) -> String {
    format!("${}.{:06}", value / 1_000_000, value % 1_000_000)
}

fn json_u64(value: &serde_json::Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_i64().and_then(|item| u64::try_from(item).ok()))
        .or_else(|| {
            value
                .as_str()
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .and_then(|item| item.parse::<u64>().ok())
        })
}

fn persist_local_agent_workspace_report(
    app: &tauri::AppHandle,
    agent_wallet: &str,
    report: &serde_json::Value,
    fallback_kind: &str,
) -> Result<(), String> {
    let Some(object) = report.as_object() else {
        return Ok(());
    };

    let title = object
        .get("title")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "agent report title is required".to_string())?;
    let summary = object
        .get("summary")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "agent report summary is required".to_string())?;
    let kind = object
        .get("kind")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(fallback_kind);
    let outcome = object
        .get("outcome")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| matches!(*value, "success" | "warning" | "error" | "info"))
        .unwrap_or("info");
    let created_at = object
        .get("createdAt")
        .and_then(json_u64)
        .unwrap_or_else(now_ms);
    let id = object
        .get("id")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string())
        .unwrap_or_else(|| format!("{kind}-{created_at}"));

    let mut normalized = serde_json::Map::new();
    normalized.insert("id".to_string(), serde_json::Value::String(id.clone()));
    normalized.insert(
        "kind".to_string(),
        serde_json::Value::String(kind.to_string()),
    );
    normalized.insert(
        "title".to_string(),
        serde_json::Value::String(title.to_string()),
    );
    normalized.insert(
        "summary".to_string(),
        serde_json::Value::String(summary.to_string()),
    );
    normalized.insert(
        "outcome".to_string(),
        serde_json::Value::String(outcome.to_string()),
    );
    normalized.insert("createdAt".to_string(), serde_json::Value::from(created_at));

    if let Some(details) = object
        .get("details")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        normalized.insert(
            "details".to_string(),
            serde_json::Value::String(details.to_string()),
        );
    }
    if let Some(cost_micros) = object.get("costMicros").and_then(json_u64) {
        normalized.insert(
            "costMicros".to_string(),
            serde_json::Value::from(cost_micros),
        );
    }
    if let Some(revenue_micros) = object.get("revenueMicros").and_then(json_u64) {
        normalized.insert(
            "revenueMicros".to_string(),
            serde_json::Value::from(revenue_micros),
        );
    }
    if let Some(category) = object
        .get("economicsCategory")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| matches!(*value, "inference" | "heartbeat" | "peer-revenue"))
    {
        normalized.insert(
            "economicsCategory".to_string(),
            serde_json::Value::String(category.to_string()),
        );
    }
    if let Some(tx_hash) = object
        .get("txHash")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        normalized.insert(
            "txHash".to_string(),
            serde_json::Value::String(tx_hash.to_string()),
        );
    }

    let reports_dir = local_agent_reports_dir(app, agent_wallet)?;
    fs::create_dir_all(&reports_dir)
        .map_err(|err| format!("failed to create reports dir: {err}"))?;
    let file_name = format!("{}-{}.json", created_at, local_agent_slug(id.as_str()));
    let serialized = serde_json::to_string_pretty(&serde_json::Value::Object(normalized))
        .map_err(|err| format!("failed to encode agent report: {err}"))?;
    write_string_atomically(&reports_dir.join(file_name), &serialized, "agent report")
}

fn build_local_agent_economics_report(
    turn_charges: &[LocalAgentTurnCharge],
    heartbeat_mode: bool,
) -> Option<serde_json::Value> {
    let total_cost = turn_charges
        .iter()
        .fold(0_u64, |sum, charge| sum.saturating_add(charge.cost_micros));
    if total_cost == 0 {
        return None;
    }

    let mut tx_hashes = turn_charges
        .iter()
        .filter_map(|charge| charge.tx_hash.clone())
        .collect::<Vec<_>>();
    tx_hashes.sort();
    tx_hashes.dedup();

    let turn_count = turn_charges.len();
    let details = (!tx_hashes.is_empty()).then(|| {
        format!(
            "Settled across {turn_count} model turn{}.\n\nTransaction hashes:\n{}",
            if turn_count == 1 { "" } else { "s" },
            tx_hashes.join("\n")
        )
    });

    Some(serde_json::json!({
        "kind": "economics",
        "title": if heartbeat_mode { "Heartbeat spend settled" } else { "Inference spend settled" },
        "summary": format!(
            "{} paid across {} model turn{}.",
            format_usdc_micros(total_cost),
            turn_count,
            if turn_count == 1 { "" } else { "s" }
        ),
        "details": details,
        "outcome": "info",
        "costMicros": total_cost,
        "economicsCategory": if heartbeat_mode { "heartbeat" } else { "inference" },
        "txHash": tx_hashes.last().cloned(),
    }))
}

fn persist_local_agent_execution_reports(
    app: &tauri::AppHandle,
    agent_wallet: &str,
    report: Option<&serde_json::Value>,
    turn_charges: &[LocalAgentTurnCharge],
    heartbeat_mode: bool,
) -> Result<(), String> {
    if let Some(value) = report {
        persist_local_agent_workspace_report(
            app,
            agent_wallet,
            value,
            if heartbeat_mode {
                "heartbeat"
            } else {
                "runtime"
            },
        )?;
    }
    if let Some(economics) = build_local_agent_economics_report(turn_charges, heartbeat_mode) {
        persist_local_agent_workspace_report(app, agent_wallet, &economics, "economics")?;
    }
    Ok(())
}

fn append_daemon_log(
    app: &tauri::AppHandle,
    agent_wallet: &str,
    message: &str,
) -> Result<(), String> {
    let logs_path = daemon_agent_logs_path(app, agent_wallet)?;
    if let Some(parent) = logs_path.parent() {
        fs::create_dir_all(parent).map_err(|err| format!("failed to create log dir: {err}"))?;
    }
    let line = format!("{} {}\n", now_ms(), message.trim());
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(&logs_path)
        .and_then(|mut file| file.write_all(line.as_bytes()))
        .map_err(|err| format!("failed to append daemon log: {err}"))
}

fn append_mesh_log_to_published_agents(
    app: &tauri::AppHandle,
    published_agents: &[MeshPublishedAgent],
    message: &str,
) {
    for published in published_agents {
        let _ = append_daemon_log(app, &published.agent_wallet, message);
    }
}

fn collect_skill_markdown_files(root: &Path, output: &mut Vec<PathBuf>) -> Result<(), String> {
    if !root.exists() {
        return Ok(());
    }

    let entries = fs::read_dir(root).map_err(|err| format!("failed to read skill dir: {err}"))?;
    for entry in entries {
        let entry = entry.map_err(|err| format!("failed to read skill dir entry: {err}"))?;
        let path = entry.path();
        if path.is_dir() {
            collect_skill_markdown_files(&path, output)?;
            continue;
        }
        if path
            .file_name()
            .and_then(|value| value.to_str())
            .map(|value| value.eq_ignore_ascii_case("SKILL.md"))
            .unwrap_or(false)
        {
            output.push(path);
        }
    }
    Ok(())
}

fn base_relative_label(base_dir: &Path, file_path: &Path) -> String {
    file_path
        .strip_prefix(base_dir)
        .unwrap_or(file_path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn read_document_if_present(file_path: &Path, label: String) -> Option<(String, String)> {
    let content = fs::read_to_string(file_path).ok()?;
    let trimmed = content.trim().to_string();
    if trimmed.is_empty() {
        return None;
    }
    Some((label, trimmed))
}

#[derive(Debug, Clone)]
struct ManifestWorkspaceState {
    dna_hash: String,
    identity_hash: String,
    mcp_tools_hash: String,
}

fn workspace_document_hash(file_path: &Path) -> Option<String> {
    let raw = fs::read_to_string(file_path).ok()?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(format!("0x{}", sha256_hex_string(trimmed)))
}

fn load_manifest_workspace_state(
    app: &tauri::AppHandle,
    agent: &PersistedInstalledAgent,
) -> Result<ManifestWorkspaceState, String> {
    let workspace = daemon_agent_workspace_path(app, &agent.agent_wallet)?;

    Ok(ManifestWorkspaceState {
        dna_hash: workspace_document_hash(&workspace.join("DNA.md"))
            .unwrap_or_else(|| agent.lock.dna_hash.clone()),
        identity_hash: workspace_document_hash(&workspace.join("IDENTITY.md")).unwrap_or_else(
            || {
                format!(
                    "0x{}",
                    sha256_hex_string(&format!(
                        "agentWallet:{}\nagentCardCid:{}\nmodelId:{}\nchainId:{}",
                        agent.agent_wallet,
                        agent.lock.agent_card_cid,
                        agent.lock.model_id,
                        agent.lock.chain_id,
                    ))
                )
            },
        ),
        mcp_tools_hash: workspace_document_hash(&workspace.join("TOOLS.md"))
            .unwrap_or_else(|| agent.lock.mcp_tools_hash.clone()),
    })
}

fn local_agent_documents(
    app: &tauri::AppHandle,
    agent: &PersistedInstalledAgent,
    heartbeat_mode: bool,
) -> Result<Vec<(String, String)>, String> {
    let base_dir = resolve_base_dir(app)?;
    let workspace = daemon_agent_workspace_path(app, &agent.agent_wallet)?;
    let base_files = if heartbeat_mode {
        vec!["HEARTBEAT.md"]
    } else {
        vec!["AGENTS.md", "SOUL.md", "TOOLS.md", "IDENTITY.md", "USER.md"]
    };

    let mut output = Vec::new();
    for file_name in base_files {
        let file_path = workspace.join(file_name);
        if let Some(document) =
            read_document_if_present(&file_path, base_relative_label(&base_dir, &file_path))
        {
            output.push(document);
        }
    }

    Ok(output)
}

fn permission_allowed(value: &str) -> bool {
    value.trim().eq_ignore_ascii_case("allow")
}

fn desired_local_agent_permissions(agent: &PersistedInstalledAgent) -> DaemonPermissionPolicy {
    let normalized_permissions = normalize_daemon_permission_policy(agent.permissions.clone());
    let normalized_desired_permissions =
        normalize_daemon_permission_policy(agent.desired_permissions.clone());

    select_desired_permission_policy(&normalized_permissions, &normalized_desired_permissions)
}

fn resolve_local_agent_permissions(
    app: &tauri::AppHandle,
    agent: &PersistedInstalledAgent,
) -> DaemonPermissionPolicy {
    let desired_permissions = {
        let daemon_state = app.state::<LocalDaemonState>();
        daemon_state
            .state
            .lock()
            .ok()
            .and_then(|guard| guard.agents.get(&agent.agent_wallet).cloned())
            .map(|state| {
                let normalized_permissions =
                    normalize_daemon_permission_policy(state.permissions.clone());
                let normalized_desired_permissions =
                    normalize_daemon_permission_policy(state.desired_permissions.clone());
                select_desired_permission_policy(
                    &normalized_permissions,
                    &normalized_desired_permissions,
                )
            })
            .unwrap_or_else(|| desired_local_agent_permissions(agent))
    };

    // Per-agent permissions are pure app-level toggles — just normalize.
    normalize_daemon_permission_policy(desired_permissions)
}

fn normalize_api_base_for_local_agent(state: &PersistedLocalState) -> String {
    let trimmed = state.settings.api_url.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        "https://api.compose.market".to_string()
    } else {
        trimmed.to_string()
    }
}

fn normalize_session_budget_value(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() || !trimmed.chars().all(|char| char.is_ascii_digit()) {
        return None;
    }
    Some(trimmed.to_string())
}

fn apply_active_session_refresh(
    identity: &PersistedLocalIdentity,
    response: &ActiveSessionRefreshResponse,
    now: u64,
) -> Option<PersistedLocalIdentity> {
    if !response.has_session {
        return None;
    }

    let session_id = response.key_id.trim();
    let compose_key_token = response.token.trim();
    let budget = normalize_session_budget_value(&response.budget_remaining)?;
    let budget_used = normalize_session_budget_value(&response.budget_used)
        .unwrap_or_else(|| identity.budget_used.clone());
    if session_id.is_empty() || compose_key_token.is_empty() || response.expires_at <= now {
        return None;
    }

    Some(PersistedLocalIdentity {
        user_address: identity.user_address.clone(),
        compose_key_token: compose_key_token.to_string(),
        session_id: session_id.to_string(),
        budget,
        budget_used,
        duration: response.expires_at.saturating_sub(now),
        chain_id: if response.chain_id > 0 {
            response.chain_id
        } else {
            identity.chain_id
        },
        expires_at: response.expires_at,
        device_id: identity.device_id.clone(),
    })
}

fn same_local_identity_session(
    current: &PersistedLocalIdentity,
    refreshed: Option<&PersistedLocalIdentity>,
) -> bool {
    match refreshed {
        Some(next) => {
            current.compose_key_token == next.compose_key_token
                && current.session_id == next.session_id
                && current.budget == next.budget
                && current.budget_used == next.budget_used
                && current.duration == next.duration
                && current.chain_id == next.chain_id
                && current.expires_at == next.expires_at
        }
        None => {
            current.compose_key_token.trim().is_empty()
                && current.session_id.trim().is_empty()
                && current.budget.trim() == "0"
                && current.budget_used.trim() == "0"
                && current.duration == 0
                && current.expires_at == 0
        }
    }
}

fn persist_local_identity_session(
    app: &tauri::AppHandle,
    identity: &PersistedLocalIdentity,
    refreshed: Option<&PersistedLocalIdentity>,
) -> Result<(), String> {
    if same_local_identity_session(identity, refreshed) {
        return Ok(());
    }

    let mut state_value = load_local_state_value(app)?;
    if !state_value.is_object() {
        state_value = serde_json::json!({});
    }

    let state_object = state_value
        .as_object_mut()
        .ok_or_else(|| "local state root must be a JSON object".to_string())?;
    let identity_value = state_object
        .entry("identity".to_string())
        .or_insert_with(|| serde_json::json!({}));
    if !identity_value.is_object() {
        *identity_value = serde_json::json!({});
    }

    let identity_object = identity_value
        .as_object_mut()
        .ok_or_else(|| "local state identity must be a JSON object".to_string())?;
    identity_object.insert(
        "userAddress".to_string(),
        serde_json::Value::String(identity.user_address.clone()),
    );
    identity_object.insert(
        "deviceId".to_string(),
        serde_json::Value::String(identity.device_id.clone()),
    );

    match refreshed {
        Some(next) => {
            identity_object.insert(
                "composeKeyToken".to_string(),
                serde_json::Value::String(next.compose_key_token.clone()),
            );
            identity_object.insert(
                "composeKeyId".to_string(),
                serde_json::Value::String(next.session_id.clone()),
            );
            identity_object.insert(
                "sessionId".to_string(),
                serde_json::Value::String(next.session_id.clone()),
            );
            identity_object.insert(
                "budget".to_string(),
                serde_json::Value::String(next.budget.clone()),
            );
            identity_object.insert(
                "budgetUsed".to_string(),
                serde_json::Value::String(next.budget_used.clone()),
            );
            identity_object.insert(
                "duration".to_string(),
                serde_json::Value::from(next.duration),
            );
            identity_object.insert(
                "chainId".to_string(),
                serde_json::Value::from(next.chain_id),
            );
            identity_object.insert(
                "expiresAt".to_string(),
                serde_json::Value::from(next.expires_at),
            );
        }
        None => {
            identity_object.insert(
                "composeKeyToken".to_string(),
                serde_json::Value::String(String::new()),
            );
            identity_object.insert(
                "composeKeyId".to_string(),
                serde_json::Value::String(String::new()),
            );
            identity_object.insert(
                "sessionId".to_string(),
                serde_json::Value::String(String::new()),
            );
            identity_object.insert(
                "budget".to_string(),
                serde_json::Value::String("0".to_string()),
            );
            identity_object.insert(
                "budgetUsed".to_string(),
                serde_json::Value::String("0".to_string()),
            );
            identity_object.insert("duration".to_string(), serde_json::Value::from(0_u64));
            identity_object.insert(
                "chainId".to_string(),
                serde_json::Value::from(identity.chain_id),
            );
            identity_object.insert("expiresAt".to_string(), serde_json::Value::from(0_u64));
        }
    }

    save_local_state_value(app, &state_value)
}

async fn refresh_local_identity_session(
    app: &tauri::AppHandle,
    state: &PersistedLocalState,
    client: &HttpClient,
) -> Result<Option<PersistedLocalIdentity>, String> {
    let Some(identity) = state.identity.as_ref() else {
        return Ok(None);
    };

    let user_address = normalize_wallet(&identity.user_address)
        .ok_or_else(|| "local identity userAddress is invalid".to_string())?;
    if identity.chain_id == 0 {
        return Err("local identity chainId is invalid".to_string());
    }

    let mut request = client
        .get(format!(
            "{}/api/session",
            normalize_api_base_for_local_agent(state)
        ))
        .header("X-Session-User-Address", user_address.as_str())
        .header("X-Chain-ID", identity.chain_id.to_string());
    let compose_key_token = identity.compose_key_token.trim();
    if !compose_key_token.is_empty() {
        request = request.bearer_auth(compose_key_token);
    }

    let response = request
        .send()
        .await
        .map_err(|err| format!("failed to refresh compose-key session: {err}"))?;
    let payload = decode_remote_json(response, "compose-key session refresh").await?;
    let parsed = serde_json::from_value::<ActiveSessionRefreshResponse>(payload)
        .map_err(|err| format!("failed to decode compose-key session refresh: {err}"))?;
    let refreshed = apply_active_session_refresh(identity, &parsed, now_ms());
    persist_local_identity_session(app, identity, refreshed.as_ref())?;
    Ok(refreshed)
}

fn normalize_connector_base_for_local_agent() -> String {
    std::env::var("CONNECTOR_URL")
        .or_else(|_| std::env::var("VITE_CONNECTOR_URL"))
        .unwrap_or_else(|_| "https://services.compose.market/connector".to_string())
        .trim()
        .trim_end_matches('/')
        .to_string()
}

fn build_local_agent_prompt(
    agent: &PersistedInstalledAgent,
    documents: &[(String, String)],
    heartbeat_mode: bool,
    permissions: &DaemonPermissionPolicy,
) -> String {
    let bootstrap_section = documents
        .iter()
        .map(|(label, content)| format!("[{label}]\n{content}"))
        .collect::<Vec<_>>()
        .join("\n\n");

    let permission_lines = [
        format!(
            "- shell: {}",
            if permission_allowed(&permissions.shell) {
                "allow"
            } else {
                "deny"
            }
        ),
        format!(
            "- network: {}",
            if permission_allowed(&permissions.network) {
                "allow"
            } else {
                "deny"
            }
        ),
        format!(
            "- filesystemRead: {}",
            if permission_allowed(&permissions.filesystem_read) {
                "allow"
            } else {
                "deny"
            }
        ),
        format!(
            "- filesystemWrite: {}",
            if permission_allowed(&permissions.filesystem_write) {
                "allow"
            } else {
                "deny"
            }
        ),
        format!(
            "- filesystemEdit: {}",
            if permission_allowed(&permissions.filesystem_edit) {
                "allow"
            } else {
                "deny"
            }
        ),
    ]
    .join("\n");

    let mode_instruction = if heartbeat_mode {
        format!(
            "This is a heartbeat. Read HEARTBEAT.md below, take the smallest safe useful step if needed, and otherwise reply {LOCAL_AGENT_HEARTBEAT_OK_TOKEN}. Do not create reports, skills, or learnings for a no-op heartbeat."
        )
    } else {
        "This is a direct local conversation with the user. Answer clearly, keep work local, and use tools only when they materially help.".to_string()
    };

    [
        format!(
            "You are {}. Your original purpose is {}",agent.metadata.name.trim(), agent.metadata.description.trim()
        ),
        "You're a personal assistant running on this user's device. Read what you need, decide what to do, and keep your local state in your own workspace.".to_string(),
        "The app brokers permissions. If a permission is denied, adapt and explain what is missing instead of pretending it worked.".to_string(),
        mode_instruction,
        "Use the runtime memory tools for durable cross-turn memory. Do not treat workspace files as your memory system for user recall.".to_string(),
        "Read TOOLS.md for the exact JSON response shape, action schema, and Compose service contracts. Do not invent alternate formats.".to_string(),
        "Use your workspace for local state. Read shared built-in skills through global-skills/ only when needed.".to_string(),
        format!("If no heartbeat work is needed, set reply to {LOCAL_AGENT_HEARTBEAT_OK_TOKEN} and leave report, skill, and actions empty."),
        String::new(),
        "Current brokered permissions:".to_string(),
        permission_lines,
        String::new(),
        "Bootstrap files:".to_string(),
        bootstrap_section,
    ]
    .join("\n")
}

fn parse_local_agent_reply(raw: &str) -> LocalAgentStructuredReply {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return LocalAgentStructuredReply {
            reply: String::new(),
            report: None,
            skill: None,
            actions: Vec::new(),
        };
    }

    let Ok(parsed) = serde_json::from_str::<serde_json::Value>(trimmed) else {
        return LocalAgentStructuredReply {
            reply: trimmed.to_string(),
            report: None,
            skill: None,
            actions: Vec::new(),
        };
    };

    let reply = parsed
        .get("reply")
        .and_then(|value| value.as_str())
        .map(|value| value.trim().to_string())
        .unwrap_or_default();

    let report = parsed.get("report").and_then(|value| {
        let object = value.as_object()?;
        let title = object.get("title")?.as_str()?.trim();
        let summary = object.get("summary")?.as_str()?.trim();
        if title.is_empty() || summary.is_empty() {
            return None;
        }
        let outcome = object
            .get("outcome")
            .and_then(|value| value.as_str())
            .map(|value| value.trim())
            .filter(|value| matches!(*value, "success" | "warning" | "error" | "info"))
            .unwrap_or("info");
        let details = object
            .get("details")
            .and_then(|value| value.as_str())
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        Some(serde_json::json!({
            "title": title,
            "summary": summary,
            "details": details,
            "outcome": outcome,
        }))
    });

    let skill = parsed.get("skill").and_then(|value| {
        let object = value.as_object()?;
        let name = object.get("name")?.as_str()?.trim().to_string();
        let markdown = object.get("markdown")?.as_str()?.trim().to_string();
        if name.is_empty() || markdown.is_empty() {
            return None;
        }
        Some(LocalAgentStructuredSkill { name, markdown })
    });

    let actions = parsed
        .get("actions")
        .cloned()
        .and_then(|value| serde_json::from_value::<Vec<LocalAgentToolAction>>(value).ok())
        .unwrap_or_default()
        .into_iter()
        .filter(|action| !action.kind.trim().is_empty())
        .take(4)
        .collect::<Vec<_>>();

    LocalAgentStructuredReply {
        reply,
        report,
        skill,
        actions,
    }
}

fn expected_agent_authored_skill_path(
    app: &tauri::AppHandle,
    agent_wallet: &str,
    skill: &LocalAgentStructuredSkill,
) -> Option<String> {
    let workspace = daemon_agent_workspace_path(app, agent_wallet).ok()?;
    let file_path = workspace
        .join("skills")
        .join("generated")
        .join(local_agent_slug(&skill.name))
        .join("SKILL.md");
    if !file_path.exists() {
        return None;
    }

    let base_dir = resolve_base_dir(app).ok()?;
    Some(base_relative_label(&base_dir, &file_path))
}

fn extract_chat_completion_text(payload: &serde_json::Value) -> String {
    let Some(message_content) = payload
        .get("choices")
        .and_then(|value| value.as_array())
        .and_then(|values| values.first())
        .and_then(|value| value.get("message"))
        .and_then(|value| value.get("content"))
    else {
        return String::new();
    };

    if let Some(content) = message_content.as_str() {
        return content.to_string();
    }

    message_content
        .as_array()
        .map(|parts| {
            parts
                .iter()
                .filter_map(|part| part.get("text").and_then(|value| value.as_str()))
                .collect::<String>()
        })
        .unwrap_or_default()
}

fn apply_local_agent_auth(
    builder: reqwest::RequestBuilder,
    identity: &PersistedLocalIdentity,
) -> reqwest::RequestBuilder {
    builder
        .bearer_auth(identity.compose_key_token.trim())
        .header("X-Chain-ID", identity.chain_id.to_string())
        .header("X-Session-User-Address", identity.user_address.trim())
}

fn extract_remote_error_message(payload: &str, fallback: String) -> String {
    serde_json::from_str::<serde_json::Value>(payload)
        .ok()
        .and_then(|value| {
            value
                .get("error")
                .and_then(|err| {
                    err.as_str().map(|value| value.to_string()).or_else(|| {
                        err.get("message")
                            .and_then(|value| value.as_str())
                            .map(|value| value.to_string())
                    })
                })
                .or_else(|| {
                    value
                        .get("message")
                        .and_then(|value| value.as_str())
                        .map(|value| value.to_string())
                })
        })
        .unwrap_or(fallback)
}

async fn decode_remote_json(
    response: reqwest::Response,
    fallback_label: &str,
) -> Result<serde_json::Value, String> {
    let status = response.status();
    let payload = response.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(extract_remote_error_message(
            &payload,
            format!("{fallback_label}: {status}"),
        ));
    }

    if payload.trim().is_empty() {
        return Ok(serde_json::Value::Null);
    }

    serde_json::from_str::<serde_json::Value>(&payload)
        .map_err(|err| format!("failed to decode {fallback_label} response: {err}"))
}

fn read_response_header(response: &reqwest::Response, name: &str) -> Option<String> {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string())
}

fn local_agent_turn_charge_from_response(
    app: &tauri::AppHandle,
    identity: &PersistedLocalIdentity,
    response: &reqwest::Response,
) -> Option<LocalAgentTurnCharge> {
    let current_budget_used = read_response_header(response, "x-session-budget-used")
        .and_then(|value| value.parse::<u64>().ok());
    let explicit_cost = read_response_header(response, "x-compose-key-final-amount-wei")
        .and_then(|value| value.parse::<u64>().ok());
    let cost_micros = if let Some(value) = explicit_cost {
        if let Some(current_budget_used) = current_budget_used {
            if let Ok(mut tracker) = app.state::<SessionBudgetTracker>().last_budget_used.lock() {
                *tracker = Some(current_budget_used);
            }
        }
        value
    } else {
        let current_budget_used = current_budget_used?;
        let tracker_state = app.state::<SessionBudgetTracker>();
        let mut tracker = tracker_state.last_budget_used.lock().ok()?;
        let baseline = tracker.unwrap_or_else(|| {
            identity
                .budget_used
                .trim()
                .parse::<u64>()
                .unwrap_or(current_budget_used)
        });
        let delta = current_budget_used.saturating_sub(baseline);
        *tracker = Some(current_budget_used);
        delta
    };
    if cost_micros == 0 {
        return None;
    }
    Some(LocalAgentTurnCharge {
        cost_micros,
        tx_hash: read_response_header(response, "x-compose-key-tx-hash")
            .or_else(|| read_response_header(response, "x-transaction-hash")),
    })
}

fn normalize_remote_action_service(value: Option<&str>) -> Result<&'static str, String> {
    match value.map(|item| item.trim().to_lowercase()) {
        Some(value) if value == "api" => Ok("api"),
        Some(value) if value == "connector" => Ok("connector"),
        Some(value) if value == "runtime" => Ok("runtime"),
        _ => {
            Err("remote.request service must be \"api\", \"connector\", or \"runtime\"".to_string())
        }
    }
}

fn normalize_remote_action_method(value: Option<&str>) -> Result<&'static str, String> {
    match value
        .map(|item| item.trim().to_uppercase())
        .unwrap_or_else(|| "GET".to_string())
        .as_str()
    {
        "GET" => Ok("GET"),
        "POST" => Ok("POST"),
        _ => Err("remote.request method must be GET or POST".to_string()),
    }
}

fn normalize_remote_action_path(value: Option<&str>) -> Result<String, String> {
    let path = value
        .map(|item| item.trim())
        .filter(|item| !item.is_empty())
        .ok_or_else(|| "remote.request path is required".to_string())?;
    if !path.starts_with('/') {
        return Err("remote.request path must start with /".to_string());
    }
    if path.contains("..") || path.contains('\\') || path.contains(' ') {
        return Err("remote.request path is invalid".to_string());
    }
    Ok(path.to_string())
}

fn remote_action_path_allowed(service: &str, path: &str) -> bool {
    let route = path.split('?').next().unwrap_or(path);
    match service {
        "api" | "connector" => route.starts_with('/'),
        "runtime" => matches!(route, "/mesh/tools/execute" | "/mesh/conclave/run"),
        _ => false,
    }
}

fn local_agent_hai_id(
    agent: &PersistedInstalledAgent,
    identity: &PersistedLocalIdentity,
) -> String {
    derive_hai_id(
        &agent.agent_wallet,
        &identity.user_address,
        &identity.device_id,
    )
}

fn build_local_runtime_request_body(
    raw_body: Option<serde_json::Value>,
    agent: &PersistedInstalledAgent,
    identity: &PersistedLocalIdentity,
    thread_id: Option<&str>,
) -> Result<serde_json::Value, String> {
    let mut body = raw_body
        .and_then(|value| value.as_object().cloned())
        .ok_or_else(|| "runtime remote.request body must be a JSON object".to_string())?;
    let hai_id = local_agent_hai_id(agent, identity);
    let thread_id = thread_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "runtime remote.request requires an explicit local threadId".to_string())?;

    body.entry("agentWallet".to_string())
        .or_insert_with(|| serde_json::Value::String(agent.agent_wallet.clone()));
    body.entry("userAddress".to_string())
        .or_insert_with(|| serde_json::Value::String(identity.user_address.clone()));
    body.entry("haiId".to_string())
        .or_insert_with(|| serde_json::Value::String(hai_id.clone()));
    body.entry("threadId".to_string())
        .or_insert_with(|| serde_json::Value::String(thread_id.to_string()));

    Ok(serde_json::Value::Object(body))
}

async fn execute_local_runtime_tool_request(
    app: &tauri::AppHandle,
    client: &HttpClient,
    agent: &PersistedInstalledAgent,
    identity: &PersistedLocalIdentity,
    path: &str,
    body: serde_json::Value,
    thread_id: Option<&str>,
) -> Result<serde_json::Value, String> {
    let runtime_host_state = app.state::<LocalRuntimeHostState>();
    let runtime_status = ensure_local_runtime_host(app, runtime_host_state.inner())?;
    let response = client
        .post(format!(
            "{}{}",
            runtime_status.base_url.trim_end_matches('/'),
            path
        ))
        .header("Content-Type", "application/json")
        .json(&build_local_runtime_request_body(
            Some(body),
            agent,
            identity,
            thread_id,
        )?)
        .send()
        .await
        .map_err(|err| format!("failed to execute local runtime tool request: {err}"))?;

    decode_remote_json(response, "local runtime tool request").await
}

fn normalize_workspace_subpath(raw: Option<&str>) -> Result<PathBuf, String> {
    let value = raw.unwrap_or(".").trim();
    if value.is_empty() || value == "." {
        return Ok(PathBuf::new());
    }

    let mut output = PathBuf::new();
    for component in Path::new(value).components() {
        match component {
            Component::Normal(segment) => output.push(segment),
            Component::CurDir => {}
            _ => {
                return Err("workspace path must stay inside the local agent workspace".to_string())
            }
        }
    }
    Ok(output)
}

fn resolve_agent_workspace_target(
    app: &tauri::AppHandle,
    agent_wallet: &str,
    raw_path: Option<&str>,
) -> Result<(PathBuf, PathBuf), String> {
    let workspace = daemon_agent_workspace_path(app, agent_wallet)?;
    let relative = normalize_workspace_subpath(raw_path)?;
    Ok((workspace.clone(), workspace.join(relative)))
}

fn resolve_agent_read_target(
    app: &tauri::AppHandle,
    agent_wallet: &str,
    raw_path: Option<&str>,
) -> Result<(PathBuf, PathBuf), String> {
    let relative = normalize_workspace_subpath(raw_path)?;
    let mut components = relative.components();
    if let Some(Component::Normal(segment)) = components.next() {
        if segment.to_string_lossy() == "global-skills" {
            let root = resolve_base_dir(app)?.join("skills");
            let mut suffix = PathBuf::new();
            for component in components {
                if let Component::Normal(item) = component {
                    suffix.push(item);
                }
            }
            return Ok((root.clone(), root.join(suffix)));
        }
    }

    let workspace = daemon_agent_workspace_path(app, agent_wallet)?;
    Ok((workspace.clone(), workspace.join(relative)))
}

fn collect_workspace_files_recursive(
    workspace: &Path,
    dir: &Path,
    output: &mut Vec<String>,
) -> Result<(), String> {
    if !dir.exists() {
        return Ok(());
    }

    let entries =
        fs::read_dir(dir).map_err(|err| format!("failed to read workspace dir: {err}"))?;
    for entry in entries {
        let entry = entry.map_err(|err| format!("failed to read workspace entry: {err}"))?;
        let path = entry.path();
        if path.is_dir() {
            collect_workspace_files_recursive(workspace, &path, output)?;
        } else if path.is_file() {
            output.push(
                path.strip_prefix(workspace)
                    .unwrap_or(&path)
                    .to_string_lossy()
                    .replace('\\', "/"),
            );
        }
    }
    Ok(())
}

fn append_skill_path_to_report(
    report: Option<serde_json::Value>,
    skill_path: Option<&String>,
) -> Option<serde_json::Value> {
    let Some(report_value) = report else {
        return None;
    };
    let Some(object) = report_value.as_object() else {
        return Some(report_value);
    };

    let mut output = serde_json::Map::new();
    for (key, value) in object {
        output.insert(key.clone(), value.clone());
    }
    if let Some(path) = skill_path {
        let details = output
            .get("details")
            .and_then(|value| value.as_str())
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        output.insert(
            "details".to_string(),
            serde_json::Value::String(
                [details, Some(format!("Agent-authored skill: {path}"))]
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>()
                    .join("\n\n"),
            ),
        );
    }

    Some(serde_json::Value::Object(output))
}

fn normalize_shell_command(value: Option<&str>) -> Result<String, String> {
    value
        .map(|item| item.trim())
        .filter(|item| !item.is_empty())
        .map(|item| item.to_string())
        .ok_or_else(|| "shell.exec command is required".to_string())
}

fn normalize_shell_args(value: Option<&[String]>) -> Vec<String> {
    value
        .unwrap_or(&[])
        .iter()
        .map(|item| item.trim())
        .filter(|item| !item.is_empty())
        .map(|item| item.to_string())
        .take(32)
        .collect::<Vec<_>>()
}

async fn request_local_agent_model_turn(
    app: &tauri::AppHandle,
    client: &HttpClient,
    state: &PersistedLocalState,
    agent: &PersistedInstalledAgent,
    identity: &PersistedLocalIdentity,
    prompt: &str,
    transcript: &[LocalAgentConversationMessage],
) -> Result<LocalAgentModelTurn, String> {
    let messages = transcript
        .iter()
        .filter_map(|message| {
            let role = message.role.trim();
            if !matches!(role, "user" | "assistant") || message.content.trim().is_empty() {
                return None;
            }
            Some(serde_json::json!({
                "role": role,
                "content": message.content,
            }))
        })
        .collect::<Vec<_>>();
    let mut request_messages = vec![serde_json::json!({
        "role": "system",
        "content": prompt,
    })];
    request_messages.extend(messages);

    let response = apply_local_agent_auth(
        client
            .post(format!(
                "{}/v1/chat/completions",
                normalize_api_base_for_local_agent(state)
            ))
            .header("Content-Type", "application/json")
            .json(&serde_json::json!({
                "model": agent.lock.model_id,
                "temperature": 0.2,
                "response_format": { "type": "json_object" },
                "messages": request_messages,
            })),
        identity,
    )
    .send()
    .await
    .map_err(|err| format!("failed to execute local agent inference: {err}"))?;
    let charge = local_agent_turn_charge_from_response(app, identity, &response);
    let payload = decode_remote_json(response, "local agent inference").await?;
    Ok(LocalAgentModelTurn {
        raw: extract_chat_completion_text(&payload),
        charge,
    })
}

async fn execute_local_agent_action(
    app: &tauri::AppHandle,
    client: &HttpClient,
    state: &PersistedLocalState,
    agent: &PersistedInstalledAgent,
    identity: &PersistedLocalIdentity,
    permissions: &DaemonPermissionPolicy,
    action: &LocalAgentToolAction,
    runtime_thread_id: Option<&str>,
) -> Result<serde_json::Value, String> {
    let kind = action.kind.trim().to_lowercase();
    match kind.as_str() {
        "files.list" => {
            if !permission_allowed(&permissions.filesystem_read) {
                return Err("filesystemRead permission denied".to_string());
            }
            let (workspace, target) =
                resolve_agent_read_target(app, &agent.agent_wallet, action.path.as_deref())?;
            let mut entries = Vec::new();
            if target.is_file() {
                entries.push(
                    target
                        .strip_prefix(&workspace)
                        .unwrap_or(&target)
                        .to_string_lossy()
                        .replace('\\', "/"),
                );
            } else {
                collect_workspace_files_recursive(&workspace, &target, &mut entries)?;
            }
            entries.sort();
            entries.truncate(256);
            Ok(serde_json::json!({ "entries": entries }))
        }
        "files.read" => {
            if !permission_allowed(&permissions.filesystem_read) {
                return Err("filesystemRead permission denied".to_string());
            }
            let (_workspace, target) =
                resolve_agent_read_target(app, &agent.agent_wallet, action.path.as_deref())?;
            let content = fs::read_to_string(&target)
                .map_err(|err| format!("failed to read workspace file: {err}"))?;
            Ok(serde_json::json!({
                "path": action.path.clone().unwrap_or_else(|| ".".to_string()),
                "content": truncate_string(content, 16_000),
            }))
        }
        "files.write" | "files.append" => {
            if !(permission_allowed(&permissions.filesystem_write)
                || permission_allowed(&permissions.filesystem_edit))
            {
                return Err("filesystemWrite/filesystemEdit permission denied".to_string());
            }
            let (_workspace, target) =
                resolve_agent_workspace_target(app, &agent.agent_wallet, action.path.as_deref())?;
            let content = action
                .content
                .as_ref()
                .map(|value| value.to_string())
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| "content is required".to_string())?;
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)
                    .map_err(|err| format!("failed to create workspace parent dir: {err}"))?;
            }
            if kind == "files.append" {
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&target)
                    .and_then(|mut file| file.write_all(content.as_bytes()))
                    .map_err(|err| format!("failed to append workspace file: {err}"))?;
            } else {
                fs::write(&target, content)
                    .map_err(|err| format!("failed to write workspace file: {err}"))?;
            }
            Ok(serde_json::json!({
                "path": action.path.clone().unwrap_or_else(|| ".".to_string()),
                "ok": true,
            }))
        }
        "shell.exec" => {
            if !permission_allowed(&permissions.shell) {
                return Err("shell permission denied".to_string());
            }

            let command = normalize_shell_command(action.command.as_deref())?;
            let args = normalize_shell_args(action.args.as_deref());
            let (_workspace, cwd) =
                resolve_agent_workspace_target(app, &agent.agent_wallet, action.cwd.as_deref())?;

            let mut process = TokioCommand::new(&command);
            process.args(args);
            process.current_dir(cwd);
            process.kill_on_drop(true);

            let output = tokio::time::timeout(Duration::from_secs(30), process.output())
                .await
                .map_err(|_| "shell.exec timed out after 30 seconds".to_string())?
                .map_err(|err| format!("failed to execute shell command: {err}"))?;

            Ok(serde_json::json!({
                "command": command,
                "status": output.status.code(),
                "success": output.status.success(),
                "stdout": truncate_string(String::from_utf8_lossy(&output.stdout).to_string(), 16_000),
                "stderr": truncate_string(String::from_utf8_lossy(&output.stderr).to_string(), 8_000),
            }))
        }
        "remote.request" => {
            if !permission_allowed(&permissions.network) {
                return Err("network permission denied".to_string());
            }
            let service = normalize_remote_action_service(action.service.as_deref())?;
            let method = normalize_remote_action_method(action.method.as_deref())?;
            let path = normalize_remote_action_path(action.path.as_deref())?;
            if !remote_action_path_allowed(service, &path) {
                return Err(
                    "remote.request path is outside the allowed Compose surfaces".to_string(),
                );
            }

            if service == "runtime" {
                if method != "POST" {
                    return Err("runtime remote.request must use POST".to_string());
                }
                return execute_local_runtime_tool_request(
                    app,
                    client,
                    agent,
                    identity,
                    &path,
                    action
                        .body
                        .clone()
                        .ok_or_else(|| "runtime remote.request body is required".to_string())?,
                    runtime_thread_id,
                )
                .await;
            }

            let base = match service {
                "api" => normalize_api_base_for_local_agent(state),
                "connector" => normalize_connector_base_for_local_agent(),
                _ => unreachable!(),
            };
            let url = format!("{base}{path}");
            let builder = match method {
                "GET" => client.get(url),
                "POST" => client
                    .post(url)
                    .header("Content-Type", "application/json")
                    .json(&action.body.clone().unwrap_or(serde_json::Value::Null)),
                _ => unreachable!(),
            };
            let response = apply_local_agent_auth(builder, identity)
                .send()
                .await
                .map_err(|err| format!("failed to execute remote.request: {err}"))?;
            decode_remote_json(response, "remote.request").await
        }
        "mesh.publish_learning" => {
            if !permission_allowed(&permissions.network) {
                return Err("network permission denied".to_string());
            }
            let title = action
                .title
                .as_ref()
                .map(|value| value.trim())
                .filter(|value| !value.is_empty())
                .ok_or_else(|| "title is required".to_string())?;
            let summary = action
                .summary
                .as_ref()
                .map(|value| value.trim())
                .filter(|value| !value.is_empty())
                .ok_or_else(|| "summary is required".to_string())?;
            let content = action
                .content
                .as_ref()
                .map(|value| value.trim())
                .filter(|value| !value.is_empty())
                .ok_or_else(|| "content is required".to_string())?;
            let request_id = format!(
                "learning-{}-{}",
                now_ms(),
                local_agent_slug(&agent.agent_wallet)
            );
            let request = MeshPublicationQueueRequest {
                request_id: request_id.clone(),
                kind: MeshPublicationQueueKind::LearningPin,
                agent_wallet: agent.agent_wallet.clone(),
                requested_at: now_ms(),
                reason: Some("agent-authored-learning".to_string()),
                title: Some(title.to_string()),
                summary: Some(summary.to_string()),
                content: Some(content.to_string()),
                access_price_usdc: action.access_price_usdc.clone(),
            };
            let request_path = mesh_publication_agent_requests_dir(app, &agent.agent_wallet)?
                .join(format!("{}.json", request.request_id));
            fs::write(
                &request_path,
                serde_json::to_string_pretty(&request).map_err(|err| {
                    format!("failed to encode learning publication request: {err}")
                })?,
            )
            .map_err(|err| format!("failed to persist learning publication request: {err}"))?;
            Ok(serde_json::json!({ "requestId": request_id, "queued": true }))
        }
        _ => Err(format!(
            "unsupported local agent action: {}",
            action.kind.trim()
        )),
    }
}

async fn run_local_agent_execution(
    app: &tauri::AppHandle,
    state: &PersistedLocalState,
    agent: &PersistedInstalledAgent,
    identity: &PersistedLocalIdentity,
    mut transcript: Vec<LocalAgentConversationMessage>,
    heartbeat_mode: bool,
    memory_thread_id: Option<&str>,
) -> Result<LocalAgentConversationResult, String> {
    let documents = local_agent_documents(app, agent, heartbeat_mode)?;
    let client = HttpClient::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .map_err(|err| format!("failed to build local agent http client: {err}"))?;
    let permissions = resolve_local_agent_permissions(app, agent);
    let prompt = build_local_agent_prompt(agent, &documents, heartbeat_mode, &permissions);
    let mut turn_charges = Vec::new();

    for _round in 0..6 {
        let turn = match request_local_agent_model_turn(
            app,
            &client,
            state,
            agent,
            identity,
            &prompt,
            &transcript,
        )
        .await
        {
            Ok(value) => value,
            Err(error) => {
                persist_local_agent_execution_reports(
                    app,
                    &agent.agent_wallet,
                    None,
                    &turn_charges,
                    heartbeat_mode,
                )?;
                return Err(error);
            }
        };
        if let Some(charge) = turn.charge {
            turn_charges.push(charge);
        }
        let raw = turn.raw;
        let parsed = parse_local_agent_reply(&raw);
        if parsed.actions.is_empty() {
            let (authored_skill_id, authored_skill_path) =
                if let Some(skill) = parsed.skill.as_ref() {
                    (
                        Some(local_agent_authored_skill_id(&skill.name)),
                        expected_agent_authored_skill_path(app, &agent.agent_wallet, skill),
                    )
                } else {
                    (None, None)
                };
            let report = append_skill_path_to_report(parsed.report, authored_skill_path.as_ref());
            persist_local_agent_execution_reports(
                app,
                &agent.agent_wallet,
                report.as_ref(),
                &turn_charges,
                heartbeat_mode,
            )?;
            return Ok(LocalAgentConversationResult {
                reply: parsed.reply.trim().to_string(),
                report,
                skill: parsed.skill,
                authored_skill_id,
                authored_skill_path,
                raw,
            });
        }

        let mut action_results = Vec::new();
        for action in parsed.actions.iter().take(4) {
            match execute_local_agent_action(
                app,
                &client,
                state,
                agent,
                identity,
                &permissions,
                action,
                memory_thread_id,
            )
            .await
            {
                Ok(result) => {
                    action_results.push(serde_json::json!({
                        "type": action.kind,
                        "ok": true,
                        "result": result,
                    }));
                }
                Err(error) => {
                    action_results.push(serde_json::json!({
                        "type": action.kind,
                        "ok": false,
                        "error": error,
                    }));
                }
            }
        }

        transcript.push(LocalAgentConversationMessage {
            role: "assistant".to_string(),
            content: raw,
        });
        transcript.push(LocalAgentConversationMessage {
            role: "user".to_string(),
            content: format!(
                "Tool results (JSON). Continue reasoning, request more actions if needed, or finish.\n{}",
                serde_json::to_string_pretty(&action_results)
                    .map_err(|err| format!("failed to encode local agent tool results: {err}"))?
            ),
        });
    }

    persist_local_agent_execution_reports(
        app,
        &agent.agent_wallet,
        None,
        &turn_charges,
        heartbeat_mode,
    )?;
    Err("local agent exceeded the maximum tool/action rounds".to_string())
}

async fn request_local_agent_heartbeat(
    app: &tauri::AppHandle,
    state: &PersistedLocalState,
    agent: &PersistedInstalledAgent,
    identity: &PersistedLocalIdentity,
) -> Result<LocalAgentHeartbeatOutcome, String> {
    let heartbeat_thread_id = format!(
        "local-agent:{}:heartbeat",
        agent.agent_wallet.to_lowercase()
    );
    let outcome = run_local_agent_execution(
        app,
        state,
        agent,
        identity,
        vec![LocalAgentConversationMessage {
            role: "user".to_string(),
            content: format!(
                "Heartbeat tick. Follow HEARTBEAT.md strictly. If nothing needs attention, reply {LOCAL_AGENT_HEARTBEAT_OK_TOKEN}. Only take a concrete next step when it is genuinely needed."
            ),
        }],
        true,
        Some(heartbeat_thread_id.as_str()),
    )
    .await?;

    let normalized_reply = outcome.reply.trim().to_string();
    let last_result = if outcome.report.is_some()
        || outcome.authored_skill_path.is_some()
        || (!normalized_reply.is_empty()
            && !normalized_reply.eq_ignore_ascii_case(LOCAL_AGENT_HEARTBEAT_OK_TOKEN))
    {
        "alert".to_string()
    } else {
        "ok".to_string()
    };

    Ok(LocalAgentHeartbeatOutcome {
        reply: normalized_reply,
        skill_path: outcome.authored_skill_path,
        last_result,
    })
}

fn find_installed_agent_value_mut<'a>(
    state: &'a mut serde_json::Value,
    agent_wallet: &str,
) -> Option<&'a mut serde_json::Map<String, serde_json::Value>> {
    let agents = state.get_mut("installedAgents")?.as_array_mut()?;
    for agent in agents {
        let object = agent.as_object_mut()?;
        let wallet = object
            .get("agentWallet")
            .and_then(|value| value.as_str())
            .and_then(normalize_wallet)?;
        if wallet == agent_wallet {
            return Some(object);
        }
    }
    None
}

fn file_modified_at_ms(path: &Path) -> u64 {
    fs::metadata(path)
        .ok()
        .and_then(|metadata| metadata.modified().ok())
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_else(now_ms)
}

fn collect_json_files_recursive(root: &Path, output: &mut Vec<PathBuf>) -> Result<(), String> {
    if !root.exists() {
        return Ok(());
    }

    let entries = fs::read_dir(root).map_err(|err| format!("failed to read report dir: {err}"))?;
    for entry in entries {
        let entry = entry.map_err(|err| format!("failed to read report dir entry: {err}"))?;
        let path = entry.path();
        if path.is_dir() {
            collect_json_files_recursive(&path, output)?;
        } else if path
            .extension()
            .and_then(|value| value.to_str())
            .map(|value| value.eq_ignore_ascii_case("json"))
            .unwrap_or(false)
        {
            output.push(path);
        }
    }

    Ok(())
}

fn read_workspace_report(path: &Path) -> Option<serde_json::Value> {
    let raw = fs::read_to_string(path).ok()?;
    let parsed = serde_json::from_str::<serde_json::Value>(&raw).ok()?;
    let object = parsed.as_object()?;
    let title = object.get("title")?.as_str()?.trim();
    let summary = object.get("summary")?.as_str()?.trim();
    if title.is_empty() || summary.is_empty() {
        return None;
    }

    let kind = object
        .get("kind")
        .and_then(|value| value.as_str())
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .unwrap_or("runtime");
    let outcome = object
        .get("outcome")
        .and_then(|value| value.as_str())
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .unwrap_or("info");

    Some(serde_json::json!({
        "id": object
            .get("id")
            .and_then(|value| value.as_str())
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(|value| value.to_string())
            .unwrap_or_else(|| {
                path.file_stem()
                    .and_then(|value| value.to_str())
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| format!("report-{}", file_modified_at_ms(path)))
            }),
        "kind": kind,
        "title": title,
        "summary": summary,
        "details": object
            .get("details")
            .and_then(|value| value.as_str())
            .map(|value| value.trim())
            .filter(|value| !value.is_empty()),
        "outcome": outcome,
        "createdAt": object
            .get("createdAt")
            .and_then(|value| value.as_u64())
            .unwrap_or_else(|| file_modified_at_ms(path)),
        "costMicros": object.get("costMicros").and_then(json_u64),
        "revenueMicros": object.get("revenueMicros").and_then(json_u64),
        "economicsCategory": object
            .get("economicsCategory")
            .and_then(|value| value.as_str())
            .map(|value| value.trim())
            .filter(|value| matches!(*value, "inference" | "heartbeat" | "peer-revenue")),
        "txHash": object
            .get("txHash")
            .and_then(|value| value.as_str())
            .map(|value| value.trim())
            .filter(|value| !value.is_empty()),
    }))
}

fn extract_skill_name_from_markdown(markdown: &str) -> Option<String> {
    markdown.lines().find_map(|line| {
        line.trim()
            .strip_prefix('#')
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(|value| value.to_string())
    })
}

fn skill_state_is_agent_authored(value: &serde_json::Value, key: &str) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    let source = object
        .get("source")
        .and_then(|item| item.as_str())
        .map(|item| item.trim())
        .unwrap_or_default();
    source == "generated" || source == "agent" || key.starts_with("agent:")
}

#[derive(Debug, Clone, Copy, Default)]
struct LocalWorkspaceSyncOutcome {
    state_dirty: bool,
    manifest_dirty: bool,
}

#[derive(Debug, Clone, Default)]
struct LocalWorkspaceSyncSummary {
    state_dirty: bool,
    manifest_dirty_agents: Vec<String>,
}

fn sync_local_agent_workspace_reports(
    app: &tauri::AppHandle,
    agent_wallet: &str,
    agent_object: &mut serde_json::Map<String, serde_json::Value>,
) -> Result<bool, String> {
    let reports_dir = local_agent_reports_dir(app, agent_wallet)?;
    let mut files = Vec::new();
    collect_json_files_recursive(&reports_dir, &mut files)?;
    files.sort();

    let mut workspace_reports = files
        .into_iter()
        .filter_map(|path| read_workspace_report(&path))
        .collect::<Vec<_>>();
    workspace_reports.sort_by_key(|value| {
        value
            .get("createdAt")
            .and_then(|item| item.as_u64())
            .unwrap_or_default()
    });
    if workspace_reports.len() > 128 {
        let start = workspace_reports.len().saturating_sub(128);
        workspace_reports = workspace_reports[start..].to_vec();
    }

    let preserved_reports = agent_object
        .get("reports")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|value| {
            !matches!(
                value.get("kind").and_then(|item| item.as_str()),
                Some("runtime" | "heartbeat" | "skill" | "economics")
            )
        })
        .collect::<Vec<_>>();

    let mut combined_reports = preserved_reports;
    combined_reports.extend(workspace_reports);

    let mut seen = HashSet::new();
    let mut deduped_reports = Vec::new();
    for report in combined_reports.into_iter().rev() {
        let key = report
            .get("id")
            .and_then(|value| value.as_str())
            .map(|value| value.to_string())
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| serde_json::to_string(&report).unwrap_or_default());
        if seen.insert(key) {
            deduped_reports.push(report);
        }
    }
    deduped_reports.reverse();
    if deduped_reports.len() > 128 {
        let start = deduped_reports.len().saturating_sub(128);
        deduped_reports = deduped_reports[start..].to_vec();
    }

    let current_reports = agent_object
        .get("reports")
        .cloned()
        .unwrap_or_else(|| serde_json::json!([]));
    let next_reports_value = serde_json::Value::Array(deduped_reports);
    let changed = current_reports != next_reports_value;
    agent_object.insert("reports".to_string(), next_reports_value);
    Ok(changed)
}

fn sync_local_agent_workspace_skills(
    app: &tauri::AppHandle,
    agent_wallet: &str,
    agent_object: &mut serde_json::Map<String, serde_json::Value>,
) -> Result<bool, String> {
    let base_dir = resolve_base_dir(app)?;
    let mut skill_files = Vec::new();
    collect_skill_markdown_files(
        &daemon_agent_workspace_path(app, agent_wallet)?
            .join("skills")
            .join("generated"),
        &mut skill_files,
    )?;
    skill_files.sort();

    let current_skill_states = agent_object
        .get("skillStates")
        .and_then(|value| value.as_object())
        .cloned()
        .unwrap_or_default();

    let mut next_skill_states = current_skill_states
        .iter()
        .filter(|(key, value)| !skill_state_is_agent_authored(value, key))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<serde_json::Map<String, serde_json::Value>>();

    for skill_path in skill_files {
        let markdown = match fs::read_to_string(&skill_path) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let skill_name = extract_skill_name_from_markdown(&markdown).unwrap_or_else(|| {
            skill_path
                .parent()
                .and_then(|value| value.file_name())
                .and_then(|value| value.to_str())
                .unwrap_or("skill")
                .to_string()
        });
        let skill_id = local_agent_authored_skill_id(&skill_name);
        next_skill_states.insert(
            skill_id.clone(),
            serde_json::json!({
                "skillId": skill_id,
                "enabled": true,
                "eligible": true,
                "source": "generated",
                "revision": base_relative_label(&base_dir, &skill_path),
                "updatedAt": file_modified_at_ms(&skill_path),
            }),
        );
    }

    let next_skill_states_value = serde_json::Value::Object(next_skill_states);
    let current_skill_states_value = agent_object
        .get("skillStates")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let changed = current_skill_states_value != next_skill_states_value;
    agent_object.insert("skillStates".to_string(), next_skill_states_value);
    Ok(changed)
}

fn current_manifest_sync_hash(
    app: &tauri::AppHandle,
    agent_wallet: &str,
    agent_object: &serde_json::Map<String, serde_json::Value>,
) -> Result<String, String> {
    let workspace = daemon_agent_workspace_path(app, agent_wallet)?;
    let authored_skills = agent_object
        .get("skillStates")
        .and_then(|value| value.as_object())
        .map(|states| {
            let mut items = states
                .iter()
                .filter(|(key, value)| skill_state_is_agent_authored(value, key))
                .map(|(key, value)| {
                    serde_json::json!({
                        "key": key,
                        "skillId": value.get("skillId").and_then(|item| item.as_str()).unwrap_or_default(),
                        "revision": value.get("revision").and_then(|item| item.as_str()).unwrap_or_default(),
                    })
                })
                .collect::<Vec<_>>();
            items.sort_by(|left, right| left.to_string().cmp(&right.to_string()));
            items
        })
        .unwrap_or_default();

    let payload = serde_json::json!({
        "dna": workspace_document_hash(&workspace.join("DNA.md")),
        "identity": workspace_document_hash(&workspace.join("IDENTITY.md")),
        "tools": workspace_document_hash(&workspace.join("TOOLS.md")),
        "skills": authored_skills,
    });

    Ok(format!(
        "0x{}",
        sha256_hex_string(
            &serde_json::to_string(&payload)
                .map_err(|err| format!("failed to encode manifest sync payload: {err}"))?,
        )
    ))
}

fn sync_local_agent_workspace_manifest_state(
    app: &tauri::AppHandle,
    agent_wallet: &str,
    agent_object: &mut serde_json::Map<String, serde_json::Value>,
) -> Result<bool, String> {
    let next_hash = current_manifest_sync_hash(app, agent_wallet, agent_object)?;

    if !agent_object
        .get("network")
        .is_some_and(|value| value.is_object())
    {
        agent_object.insert("network".to_string(), serde_json::json!({}));
    }

    let Some(network) = agent_object
        .get_mut("network")
        .and_then(|value| value.as_object_mut())
    else {
        return Ok(false);
    };

    let changed = network
        .get("manifestSyncHash")
        .and_then(|value| value.as_str())
        .map(|value| value != next_hash)
        .unwrap_or(true);
    network.insert(
        "manifestSyncHash".to_string(),
        serde_json::Value::String(next_hash),
    );
    Ok(changed)
}

fn sync_local_agent_workspace_state(
    app: &tauri::AppHandle,
    agent_wallet: &str,
    agent_object: &mut serde_json::Map<String, serde_json::Value>,
) -> Result<LocalWorkspaceSyncOutcome, String> {
    let reports_dirty = sync_local_agent_workspace_reports(app, agent_wallet, agent_object)?;
    let skills_dirty = sync_local_agent_workspace_skills(app, agent_wallet, agent_object)?;
    let manifest_state_dirty =
        sync_local_agent_workspace_manifest_state(app, agent_wallet, agent_object)?;
    Ok(LocalWorkspaceSyncOutcome {
        state_dirty: reports_dirty || skills_dirty || manifest_state_dirty,
        manifest_dirty: skills_dirty || manifest_state_dirty,
    })
}

fn sync_all_local_agent_workspaces(
    app: &tauri::AppHandle,
    state_value: &mut serde_json::Value,
) -> Result<LocalWorkspaceSyncSummary, String> {
    let Some(agents) = state_value
        .get_mut("installedAgents")
        .and_then(|value| value.as_array_mut())
    else {
        return Ok(LocalWorkspaceSyncSummary::default());
    };

    let mut summary = LocalWorkspaceSyncSummary::default();
    for agent in agents {
        let Some(object) = agent.as_object_mut() else {
            continue;
        };
        let Some(agent_wallet) = object
            .get("agentWallet")
            .and_then(|value| value.as_str())
            .and_then(normalize_wallet)
        else {
            continue;
        };
        let outcome = sync_local_agent_workspace_state(app, &agent_wallet, object)?;
        summary.state_dirty |= outcome.state_dirty;
        if outcome.manifest_dirty {
            summary.manifest_dirty_agents.push(agent_wallet);
        }
    }

    Ok(summary)
}

fn json_string_list(value: Option<&serde_json::Value>) -> Vec<String> {
    let mut items = value
        .and_then(|entry| entry.as_array())
        .map(|entries| {
            entries
                .iter()
                .filter_map(|entry| entry.as_str())
                .map(|entry| entry.trim().to_string())
                .filter(|entry| !entry.is_empty())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    items.sort();
    items.dedup();
    items
}

fn enabled_installed_skill_ids(state_value: &serde_json::Value) -> Vec<String> {
    let mut skills = state_value
        .get("installedSkills")
        .and_then(|value| value.as_array())
        .map(|items| {
            items
                .iter()
                .filter(|item| item.get("enabled").and_then(|entry| entry.as_bool()) == Some(true))
                .filter_map(|item| item.get("id").and_then(|entry| entry.as_str()))
                .map(|item| item.trim().to_string())
                .filter(|item| !item.is_empty())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    skills.sort();
    skills.dedup();
    skills
}

fn local_manifest_state_projection(
    state_value: &serde_json::Value,
    agent_object: &serde_json::Map<String, serde_json::Value>,
) -> serde_json::Value {
    let metadata = agent_object.get("metadata");
    let network = agent_object.get("network");
    let lock = agent_object.get("lock");
    let mut skills = enabled_installed_skill_ids(state_value);
    skills.extend(
        agent_object
            .get("skillStates")
            .and_then(|value| value.as_object())
            .map(|states| {
                states
                    .values()
                    .filter(|state| {
                        state.get("enabled").and_then(|entry| entry.as_bool()) == Some(true)
                            && state.get("eligible").and_then(|entry| entry.as_bool()) == Some(true)
                    })
                    .filter_map(|state| state.get("skillId").and_then(|entry| entry.as_str()))
                    .map(|entry| entry.trim().to_string())
                    .filter(|entry| !entry.is_empty())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
    );
    skills.sort();
    skills.dedup();

    serde_json::json!({
        "agentWallet": agent_object.get("agentWallet").and_then(|value| value.as_str()).unwrap_or_default(),
        "networkEnabled": network.and_then(|value| value.get("enabled")).and_then(|value| value.as_bool()).unwrap_or(false),
        "metadata": {
            "name": metadata.and_then(|value| value.get("name")).and_then(|value| value.as_str()).unwrap_or_default(),
            "description": metadata.and_then(|value| value.get("description")).and_then(|value| value.as_str()).unwrap_or_default(),
            "model": metadata.and_then(|value| value.get("model")).and_then(|value| value.as_str()).unwrap_or_default(),
            "framework": metadata.and_then(|value| value.get("framework")).and_then(|value| value.as_str()).unwrap_or_default(),
            "agentCardUri": metadata.and_then(|value| value.get("agentCardUri")).and_then(|value| value.as_str()).unwrap_or_default(),
            "chatEndpoint": metadata.and_then(|value| value.get("endpoints")).and_then(|value| value.get("chat")).and_then(|value| value.as_str()).unwrap_or_default(),
            "streamEndpoint": metadata.and_then(|value| value.get("endpoints")).and_then(|value| value.get("stream")).and_then(|value| value.as_str()).unwrap_or_default(),
        },
        "lock": {
            "chainId": lock.and_then(|value| value.get("chainId")).and_then(|value| value.as_u64()).unwrap_or(0),
            "modelId": lock.and_then(|value| value.get("modelId")).and_then(|value| value.as_str()).unwrap_or_default(),
            "agentCardCid": lock.and_then(|value| value.get("agentCardCid")).and_then(|value| value.as_str()).unwrap_or_default(),
        },
        "publicCard": network.and_then(|value| value.get("publicCard")).cloned().unwrap_or(serde_json::Value::Null),
        "mcpServers": json_string_list(agent_object.get("mcpServers")),
        "skills": skills,
    })
}

fn installed_agent_object<'a>(
    state_value: &'a serde_json::Value,
    agent_wallet: &str,
) -> Option<&'a serde_json::Map<String, serde_json::Value>> {
    state_value
        .get("installedAgents")
        .and_then(|value| value.as_array())
        .and_then(|agents| {
            agents.iter().find_map(|agent| {
                let object = agent.as_object()?;
                let wallet = object
                    .get("agentWallet")
                    .and_then(|value| value.as_str())
                    .and_then(normalize_wallet)?;
                if wallet == agent_wallet {
                    Some(object)
                } else {
                    None
                }
            })
        })
}

fn changed_manifest_agent_wallets(
    previous_state: &serde_json::Value,
    next_state: &serde_json::Value,
) -> Vec<String> {
    let mut wallets = next_state
        .get("installedAgents")
        .and_then(|value| value.as_array())
        .map(|agents| {
            agents
                .iter()
                .filter_map(|agent| agent.as_object())
                .filter_map(|agent| {
                    let wallet = agent
                        .get("agentWallet")
                        .and_then(|value| value.as_str())
                        .and_then(normalize_wallet)?;
                    let next_projection = local_manifest_state_projection(next_state, agent);
                    let previous_projection =
                        installed_agent_object(previous_state, wallet.as_str()).map(|previous| {
                            local_manifest_state_projection(previous_state, previous)
                        });
                    if previous_projection.as_ref() == Some(&next_projection) {
                        None
                    } else {
                        Some(wallet)
                    }
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    wallets.sort();
    wallets.dedup();
    wallets
}

fn queue_manifest_publication_requests_from_state(
    app: &tauri::AppHandle,
    state_value: &serde_json::Value,
    agent_wallets: &[String],
    reason: &str,
) -> Result<(), String> {
    let mesh_enabled = state_value
        .get("settings")
        .and_then(|value| value.get("meshEnabled"))
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    if !mesh_enabled {
        return Ok(());
    }

    for wallet in agent_wallets {
        let Some(agent) = installed_agent_object(state_value, wallet) else {
            continue;
        };
        let network_enabled = agent
            .get("network")
            .and_then(|value| value.get("enabled"))
            .and_then(|value| value.as_bool())
            .unwrap_or(false);
        if !network_enabled {
            continue;
        }
        queue_manifest_publication_request(app, wallet, reason)?;
    }

    Ok(())
}

fn queue_manifest_publication_request(
    app: &tauri::AppHandle,
    agent_wallet: &str,
    reason: &str,
) -> Result<(), String> {
    let can_publish = with_mesh_status(app, |status| {
        status.running && status_has_published_agent(status, agent_wallet)
    })
    .unwrap_or(false);
    if !can_publish {
        return Ok(());
    }

    let requests_dir = mesh_publication_agent_requests_dir(app, agent_wallet)?;
    let has_pending_manifest = fs::read_dir(&requests_dir)
        .map_err(|err| format!("failed to read mesh publication request dir: {err}"))?
        .filter_map(|entry| entry.ok().map(|value| value.path()))
        .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("json"))
        .filter_map(|path| {
            path.file_stem()
                .and_then(|value| value.to_str())
                .map(|value| value.to_string())
        })
        .any(|value| value.starts_with("manifest-"));
    if has_pending_manifest {
        return Ok(());
    }

    let request = MeshPublicationQueueRequest {
        request_id: format!("manifest-{}-{}", now_ms(), local_agent_slug(agent_wallet)),
        kind: MeshPublicationQueueKind::ManifestPublish,
        agent_wallet: agent_wallet.to_string(),
        requested_at: now_ms(),
        reason: Some(reason.to_string()),
        title: None,
        summary: None,
        content: None,
        access_price_usdc: None,
    };
    let path = requests_dir.join(format!("{}.json", request.request_id));
    fs::write(
        &path,
        serde_json::to_string_pretty(&request)
            .map_err(|err| format!("failed to encode manifest publication request: {err}"))?,
    )
    .map_err(|err| format!("failed to persist manifest publication request: {err}"))?;

    let _ = append_daemon_log(
        app,
        agent_wallet,
        &format!("manifest publish queued: {reason}"),
    );

    Ok(())
}

fn update_agent_runtime_state(
    agent_object: &mut serde_json::Map<String, serde_json::Value>,
    agent_wallet: &str,
    running: bool,
    status: &str,
    last_error: Option<String>,
    last_heartbeat_at: Option<u64>,
) {
    agent_object.insert("running".to_string(), serde_json::Value::Bool(running));
    agent_object.insert(
        "runtimeId".to_string(),
        serde_json::Value::String(format!("mesh-daemon:{agent_wallet}")),
    );

    let worker_state = agent_object
        .entry("workerState".to_string())
        .or_insert_with(|| serde_json::json!({}));
    if let Some(object) = worker_state.as_object_mut() {
        object.insert("running".to_string(), serde_json::Value::Bool(running));
        object.insert(
            "status".to_string(),
            serde_json::Value::String(status.to_string()),
        );
        object.insert(
            "runtimeId".to_string(),
            serde_json::Value::String(format!("mesh-daemon:{agent_wallet}")),
        );
        object.insert(
            "lastHeartbeatAt".to_string(),
            last_heartbeat_at
                .map(serde_json::Value::from)
                .unwrap_or(serde_json::Value::Null),
        );
        object.insert(
            "lastError".to_string(),
            last_error
                .map(serde_json::Value::String)
                .unwrap_or(serde_json::Value::Null),
        );
        object.insert("updatedAt".to_string(), serde_json::Value::from(now_ms()));
    }
}

fn update_daemon_agent_state(
    app: &tauri::AppHandle,
    agent_wallet: &str,
    running: bool,
    status: &str,
    last_error: Option<String>,
) -> Result<(), String> {
    let daemon_state = app.state::<LocalDaemonState>();
    let mut guard = daemon_state
        .state
        .lock()
        .map_err(|_| "failed to lock daemon state".to_string())?;
    let Some(agent) = guard.agents.get_mut(agent_wallet) else {
        return Ok(());
    };

    agent.runtime_id = Some(format!("mesh-daemon:{agent_wallet}"));
    agent.running = running;
    agent.status = status.to_string();
    agent.last_error = last_error;
    agent.updated_at = now_ms();
    write_daemon_state_to_disk(app, &guard)
}

async fn process_local_agent_heartbeats(app: &tauri::AppHandle) -> Result<(), String> {
    let local_state = load_persisted_local_state(app)?;
    if local_state.identity.is_none() {
        return Ok(());
    }

    let daemon_snapshot = {
        let daemon_state = app.state::<LocalDaemonState>();
        let snapshot = daemon_state
            .state
            .lock()
            .map_err(|_| "failed to lock daemon state".to_string())?
            .clone();
        snapshot
    };
    let session_client = HttpClient::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .map_err(|err| format!("failed to build compose-key session client: {err}"))?;
    let live_identity =
        match refresh_local_identity_session(app, &local_state, &session_client).await {
            Ok(identity) => identity,
            Err(error) => {
                let message = format!("compose-key session refresh failed: {error}");
                for daemon_agent in daemon_snapshot.agents.values() {
                    let agent_wallet = daemon_agent.agent_wallet.to_lowercase();
                    let _ = update_daemon_agent_state(
                        app,
                        &agent_wallet,
                        false,
                        "stopped",
                        Some(message.clone()),
                    );
                    let _ = append_daemon_log(app, &agent_wallet, &message);
                }
                return Ok(());
            }
        };

    for daemon_agent in daemon_snapshot.agents.values() {
        let agent_wallet = daemon_agent.agent_wallet.to_lowercase();
        let Some(agent) = local_state
            .installed_agents
            .iter()
            .find(|item| item.agent_wallet.eq_ignore_ascii_case(&agent_wallet))
        else {
            continue;
        };

        if !daemon_agent.desired_running {
            let _ = update_daemon_agent_state(app, &agent_wallet, false, "stopped", None);
            continue;
        }

        if !agent.heartbeat.enabled {
            continue;
        }

        let interval_ms = agent.heartbeat.interval_ms.clamp(5_000, 300_000);
        let next_due_at = agent
            .heartbeat
            .last_run_at
            .unwrap_or(0)
            .saturating_add(interval_ms);
        if next_due_at > now_ms() {
            continue;
        }

        let Some(identity) = live_identity.as_ref() else {
            let message = "compose-key session inactive".to_string();
            let _ = update_daemon_agent_state(
                app,
                &agent_wallet,
                false,
                "stopped",
                Some(message.clone()),
            );
            let _ = append_daemon_log(app, &agent_wallet, &format!("heartbeat skipped: {message}"));
            continue;
        };

        match request_local_agent_heartbeat(app, &local_state, agent, identity).await {
            Ok(outcome) => {
                let completed_at = now_ms();
                let mut state_value = load_local_state_value(app)?;
                let mut manifest_dirty = false;
                if let Some(agent_object) =
                    find_installed_agent_value_mut(&mut state_value, &agent_wallet)
                {
                    let heartbeat = agent_object
                        .entry("heartbeat".to_string())
                        .or_insert_with(|| serde_json::json!({}));
                    if let Some(object) = heartbeat.as_object_mut() {
                        object.insert("enabled".to_string(), serde_json::Value::Bool(true));
                        object.insert(
                            "intervalMs".to_string(),
                            serde_json::Value::from(interval_ms),
                        );
                        object.insert(
                            "lastRunAt".to_string(),
                            serde_json::Value::from(completed_at),
                        );
                        object.insert(
                            "lastResult".to_string(),
                            serde_json::Value::String(outcome.last_result.clone()),
                        );
                    }

                    update_agent_runtime_state(
                        agent_object,
                        &agent_wallet,
                        true,
                        "running",
                        None,
                        Some(completed_at),
                    );
                    manifest_dirty |=
                        sync_local_agent_workspace_state(app, &agent_wallet, agent_object)?
                            .manifest_dirty;
                }
                save_local_state_value(app, &state_value)?;
                if manifest_dirty && local_state.settings.mesh_enabled && agent.network.enabled {
                    let _ = queue_manifest_publication_request(
                        app,
                        &agent_wallet,
                        "local-agent-runtime-changed",
                    );
                }

                let log_message = if let Some(path) = outcome.skill_path.as_ref() {
                    format!("heartbeat stored skill: {path}")
                } else if outcome
                    .reply
                    .eq_ignore_ascii_case(LOCAL_AGENT_HEARTBEAT_OK_TOKEN)
                    || outcome.reply.trim().is_empty()
                {
                    "heartbeat ok".to_string()
                } else {
                    format!("heartbeat alert: {}", outcome.reply.trim())
                };
                let _ = append_daemon_log(app, &agent_wallet, &log_message);
                let _ = update_daemon_agent_state(app, &agent_wallet, true, "running", None);
            }
            Err(error) => {
                let completed_at = now_ms();
                let mut state_value = load_local_state_value(app)?;
                if let Some(agent_object) =
                    find_installed_agent_value_mut(&mut state_value, &agent_wallet)
                {
                    let heartbeat = agent_object
                        .entry("heartbeat".to_string())
                        .or_insert_with(|| serde_json::json!({}));
                    if let Some(object) = heartbeat.as_object_mut() {
                        object.insert("enabled".to_string(), serde_json::Value::Bool(true));
                        object.insert(
                            "intervalMs".to_string(),
                            serde_json::Value::from(interval_ms),
                        );
                        object.insert(
                            "lastRunAt".to_string(),
                            serde_json::Value::from(completed_at),
                        );
                        object.insert(
                            "lastResult".to_string(),
                            serde_json::Value::String("error".to_string()),
                        );
                    }
                    update_agent_runtime_state(
                        agent_object,
                        &agent_wallet,
                        false,
                        "error",
                        Some(error.clone()),
                        Some(completed_at),
                    );
                }
                save_local_state_value(app, &state_value)?;
                let _ = append_daemon_log(app, &agent_wallet, &format!("heartbeat error: {error}"));
                let _ = update_daemon_agent_state(app, &agent_wallet, false, "error", Some(error));
            }
        }
    }

    Ok(())
}

fn with_daemon_state<T>(
    app: &tauri::AppHandle,
    state: &tauri::State<'_, LocalDaemonState>,
    updater: impl FnOnce(&mut DaemonStateFile) -> Result<T, String>,
) -> Result<T, String> {
    let mut guard = state
        .state
        .lock()
        .map_err(|_| "failed to lock daemon state".to_string())?;
    let output = updater(&mut guard)?;
    write_daemon_state_to_disk(app, &guard)?;
    Ok(output)
}

fn normalize_daemon_state_for_local_mode(daemon: &mut DaemonStateFile) {
    for agent in daemon.agents.values_mut() {
        let normalized_permissions = normalize_daemon_permission_policy(agent.permissions.clone());
        let normalized_desired_permissions =
            normalize_daemon_permission_policy(agent.desired_permissions.clone());

        agent.permissions = normalized_permissions.clone();
        agent.desired_permissions = select_desired_permission_policy(
            &normalized_permissions,
            &normalized_desired_permissions,
        );

        agent.desired_running = true;
        if agent.status.trim().is_empty() {
            agent.status = if agent.desired_running {
                "starting".to_string()
            } else {
                "stopped".to_string()
            };
        }
        if agent.updated_at == 0 {
            agent.updated_at = now_ms();
        }
    }
}

#[tauri::command]
fn daemon_install_agent(
    app: tauri::AppHandle,
    state: tauri::State<'_, LocalDaemonState>,
    payload: DaemonInstallPayload,
) -> Result<DaemonAgentState, String> {
    let normalized_wallet = normalize_wallet(&payload.agent_wallet)
        .ok_or_else(|| "agentWallet must be a valid wallet address".to_string())?;
    if payload.agent_card_cid.trim().is_empty() {
        return Err("agentCardCid is required".to_string());
    }
    if payload.model_id.trim().is_empty() {
        return Err("modelId is required".to_string());
    }
    if payload.mcp_tools_hash.trim().is_empty() {
        return Err("mcpToolsHash is required".to_string());
    }

    let normalized_payload = DaemonInstallPayload {
        agent_wallet: normalized_wallet.clone(),
        agent_card_cid: payload.agent_card_cid.trim().to_string(),
        chain_id: payload.chain_id,
        model_id: payload.model_id.trim().to_string(),
        mcp_tools_hash: payload.mcp_tools_hash.trim().to_string(),
        dna_hash: payload.dna_hash.trim().to_string(),
    };

    bootstrap_agent_workspace(&app, &normalized_payload)?;

    let installed = with_daemon_state(&app, &state, |daemon| {
        let entry = daemon
            .agents
            .entry(normalized_wallet.clone())
            .or_insert(DaemonAgentState {
                agent_wallet: normalized_wallet.clone(),
                runtime_id: None,
                desired_running: true,
                running: false,
                status: "stopped".to_string(),
                dna_hash: normalized_payload.dna_hash.clone(),
                chain_id: normalized_payload.chain_id,
                model_id: normalized_payload.model_id.clone(),
                mcp_tools_hash: normalized_payload.mcp_tools_hash.clone(),
                agent_card_cid: normalized_payload.agent_card_cid.clone(),
                desired_permissions: DaemonPermissionPolicy::default(),
                permissions: DaemonPermissionPolicy::default(),
                logs_cursor: 0,
                last_error: None,
                updated_at: now_ms(),
            });

        entry.chain_id = normalized_payload.chain_id;
        entry.model_id = normalized_payload.model_id.clone();
        entry.mcp_tools_hash = normalized_payload.mcp_tools_hash.clone();
        entry.agent_card_cid = normalized_payload.agent_card_cid.clone();
        entry.dna_hash = normalized_payload.dna_hash.clone();
        entry.desired_permissions =
            normalize_daemon_permission_policy(entry.desired_permissions.clone());
        entry.permissions = normalize_daemon_permission_policy(entry.permissions.clone());
        entry.desired_running = true;
        entry.running = false;
        entry.status = "stopped".to_string();
        entry.runtime_id = None;
        entry.last_error = None;
        entry.updated_at = now_ms();
        daemon
            .agents
            .get(&normalized_wallet)
            .cloned()
            .ok_or_else(|| format!("agent not installed: {normalized_wallet}"))
    })?;
    Ok(installed)
}

#[tauri::command]
fn daemon_remove_agent(
    app: tauri::AppHandle,
    state: tauri::State<'_, LocalDaemonState>,
    agent_wallet: String,
) -> Result<(), String> {
    let wallet =
        normalize_wallet(&agent_wallet).ok_or_else(|| "invalid agentWallet".to_string())?;

    with_daemon_state(&app, &state, |daemon| {
        daemon
            .agents
            .remove(&wallet)
            .ok_or_else(|| format!("agent not installed: {wallet}"))?;
        Ok(())
    })?;
    let mut state_value = load_local_state_value(&app)?;
    if let Some(installed_agents) = state_value
        .get_mut("installedAgents")
        .and_then(|value| value.as_array_mut())
    {
        installed_agents.retain(|agent| {
            agent
                .get("agentWallet")
                .and_then(|value| value.as_str())
                .and_then(normalize_wallet)
                .as_deref()
                != Some(wallet.as_str())
        });
    }
    save_local_state_value(&app, &state_value)?;

    Ok(())
}

#[tauri::command]
fn daemon_update_permissions(
    app: tauri::AppHandle,
    state: tauri::State<'_, LocalDaemonState>,
    agent_wallet: String,
    policy: DaemonPermissionPolicy,
) -> Result<DaemonAgentState, String> {
    let wallet =
        normalize_wallet(&agent_wallet).ok_or_else(|| "invalid agentWallet".to_string())?;
    let normalized_policy = normalize_daemon_permission_policy(policy);

    let updated = with_daemon_state(&app, &state, |daemon| {
        let entry = daemon
            .agents
            .get_mut(&wallet)
            .ok_or_else(|| format!("agent not installed: {wallet}"))?;
        entry.desired_permissions = normalized_policy.clone();
        entry.permissions = normalized_policy.clone();
        entry.updated_at = now_ms();
        Ok(entry.clone())
    })?;
    Ok(updated)
}

#[tauri::command]
fn daemon_get_agent_status(
    _app: tauri::AppHandle,
    state: tauri::State<'_, LocalDaemonState>,
    agent_wallet: String,
) -> Result<Option<DaemonAgentState>, String> {
    let wallet =
        normalize_wallet(&agent_wallet).ok_or_else(|| "invalid agentWallet".to_string())?;
    let guard = state
        .state
        .lock()
        .map_err(|_| "failed to lock daemon state".to_string())?;
    Ok(guard.agents.get(&wallet).cloned().map(|mut agent| {
        let normalized_permissions = normalize_daemon_permission_policy(agent.permissions.clone());
        let normalized_desired_permissions =
            normalize_daemon_permission_policy(agent.desired_permissions.clone());
        let desired_permissions = select_desired_permission_policy(
            &normalized_permissions,
            &normalized_desired_permissions,
        );
        agent.desired_permissions = desired_permissions.clone();
        agent.permissions = normalize_daemon_permission_policy(desired_permissions);
        agent
    }))
}

#[tauri::command]
fn daemon_tail_logs(
    app: tauri::AppHandle,
    agent_wallet: String,
    cursor: Option<usize>,
) -> Result<DaemonLogTail, String> {
    let wallet =
        normalize_wallet(&agent_wallet).ok_or_else(|| "invalid agentWallet".to_string())?;
    let logs_path = daemon_agent_logs_path(&app, &wallet)?;
    if !logs_path.exists() {
        return Ok(DaemonLogTail {
            lines: Vec::new(),
            cursor: cursor.unwrap_or(0),
        });
    }

    let raw =
        fs::read_to_string(&logs_path).map_err(|err| format!("failed to read logs: {err}"))?;
    let all_lines = raw.lines().map(|line| line.to_string()).collect::<Vec<_>>();
    let from = cursor.unwrap_or(0).min(all_lines.len());
    let slice = all_lines[from..].to_vec();

    Ok(DaemonLogTail {
        lines: slice,
        cursor: all_lines.len(),
    })
}

#[tauri::command]
async fn daemon_run_local_agent_conversation(
    app: tauri::AppHandle,
    agent_wallet: String,
    history: Vec<LocalAgentConversationMessage>,
    message: String,
    thread_id: String,
) -> Result<LocalAgentConversationResult, String> {
    let wallet =
        normalize_wallet(&agent_wallet).ok_or_else(|| "invalid agentWallet".to_string())?;
    let content = message.trim();
    if content.is_empty() {
        return Err("message is required".to_string());
    }
    let thread_id = thread_id.trim().to_string();
    if thread_id.is_empty() {
        return Err("threadId is required".to_string());
    }

    let state = load_persisted_local_state(&app)?;
    state
        .identity
        .as_ref()
        .ok_or_else(|| "Connect Local first so this device has a compose key.".to_string())?;
    let session_client = HttpClient::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .map_err(|err| format!("failed to build compose-key session client: {err}"))?;
    let identity = refresh_local_identity_session(&app, &state, &session_client)
        .await?
        .ok_or_else(|| "The compose-key session is inactive or expired.".to_string())?;
    if identity.expires_at <= now_ms() {
        return Err("The compose-key session is inactive or expired.".to_string());
    }

    let agent = state
        .installed_agents
        .iter()
        .find(|item| item.agent_wallet.eq_ignore_ascii_case(&wallet))
        .cloned()
        .ok_or_else(|| format!("agent not installed: {wallet}"))?;

    let mut transcript = history
        .into_iter()
        .filter_map(|entry| {
            let role = entry.role.trim();
            let content = entry.content.trim();
            if !matches!(role, "user" | "assistant") || content.is_empty() {
                return None;
            }
            Some(LocalAgentConversationMessage {
                role: role.to_string(),
                content: content.to_string(),
            })
        })
        .collect::<Vec<_>>();
    transcript.push(LocalAgentConversationMessage {
        role: "user".to_string(),
        content: content.to_string(),
    });

    let result = run_local_agent_execution(
        &app,
        &state,
        &agent,
        &identity,
        transcript,
        false,
        Some(thread_id.as_str()),
    )
    .await?;

    let mut manifest_dirty = false;
    let mut state_value = load_local_state_value(&app)?;
    if let Some(agent_object) = find_installed_agent_value_mut(&mut state_value, &wallet) {
        manifest_dirty |=
            sync_local_agent_workspace_state(&app, &wallet, agent_object)?.manifest_dirty;
    }
    save_local_state_value(&app, &state_value)?;
    if manifest_dirty && state.settings.mesh_enabled && agent.network.enabled {
        let _ = queue_manifest_publication_request(&app, &wallet, "local-agent-runtime-changed");
    }

    let log_message = if let Some(path) = result.authored_skill_path.as_ref() {
        format!("conversation stored skill: {path}")
    } else {
        "conversation completed".to_string()
    };
    let _ = append_daemon_log(&app, &wallet, &log_message);

    Ok(result)
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct OsPermissionSnapshot {
    camera: String,
    microphone: String,
    screen: String,
    full_disk_access: String,
    accessibility: String,
}

#[cfg(target_os = "macos")]
unsafe extern "C" {
    fn compose_mesh_camera_authorization_status() -> i32;
    fn compose_mesh_request_camera_access() -> bool;
    fn compose_mesh_microphone_authorization_status() -> i32;
    fn compose_mesh_request_microphone_access() -> bool;
    fn compose_mesh_preflight_screen_capture_access() -> bool;
    fn compose_mesh_request_screen_capture_access() -> bool;
    fn compose_mesh_accessibility_is_trusted() -> bool;
    fn compose_mesh_prompt_accessibility_access() -> bool;
}

fn query_tcc_status(service: &str) -> String {
    #[cfg(target_os = "macos")]
    {
        let db_path = format!(
            "{}/Library/Application Support/com.apple.TCC/TCC.db",
            std::env::var("HOME").unwrap_or_default()
        );
        let query = format!(
            "SELECT auth_value FROM access WHERE service='{}' AND client='compose.market.mesh' LIMIT 1",
            service
        );
        if let Ok(output) = std::process::Command::new("sqlite3")
            .arg(&db_path)
            .arg(&query)
            .output()
        {
            let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
            return match stdout.as_str() {
                "2" => "granted".to_string(),
                "0" => "denied".to_string(),
                _ => "denied".to_string(),
            };
        }
        "denied".to_string()
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = service;
        "denied".to_string()
    }
}

#[cfg(target_os = "macos")]
fn av_authorization_status(value: i32) -> String {
    match value {
        3 => "granted".to_string(),
        _ => "denied".to_string(),
    }
}

#[cfg(target_os = "macos")]
fn query_camera_permission() -> String {
    unsafe { av_authorization_status(compose_mesh_camera_authorization_status()) }
}

#[cfg(target_os = "macos")]
fn query_microphone_permission() -> String {
    unsafe { av_authorization_status(compose_mesh_microphone_authorization_status()) }
}

#[cfg(target_os = "macos")]
fn query_screen_permission() -> String {
    if unsafe { compose_mesh_preflight_screen_capture_access() } {
        "granted".to_string()
    } else {
        query_tcc_status("kTCCServiceScreenCapture")
    }
}

#[cfg(target_os = "macos")]
fn query_accessibility_permission() -> String {
    if unsafe { compose_mesh_accessibility_is_trusted() } {
        "granted".to_string()
    } else {
        query_tcc_status("kTCCServiceAccessibility")
    }
}

#[cfg(target_os = "macos")]
fn resolve_home_dir(app: Option<&tauri::AppHandle>) -> Option<PathBuf> {
    app.and_then(|handle| handle.path().home_dir().ok())
        .or_else(|| std::env::var_os("HOME").map(PathBuf::from))
}

#[cfg(target_os = "macos")]
fn query_full_disk_access_permission(app: Option<&tauri::AppHandle>) -> String {
    let Some(home_dir) = resolve_home_dir(app) else {
        return "denied".to_string();
    };

    let tcc_db = home_dir
        .join("Library")
        .join("Application Support")
        .join("com.apple.TCC")
        .join("TCC.db");

    match fs::File::open(&tcc_db) {
        Ok(_) => "granted".to_string(),
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => "denied".to_string(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => "denied".to_string(),
        Err(_) => query_tcc_status("kTCCServiceSystemPolicyAllFiles"),
    }
}

fn query_os_permissions_snapshot(app: Option<&tauri::AppHandle>) -> OsPermissionSnapshot {
    #[cfg(target_os = "macos")]
    {
        OsPermissionSnapshot {
            camera: query_camera_permission(),
            microphone: query_microphone_permission(),
            screen: query_screen_permission(),
            full_disk_access: query_full_disk_access_permission(app),
            accessibility: query_accessibility_permission(),
        }
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = app;
        OsPermissionSnapshot {
            camera: "denied".to_string(),
            microphone: "denied".to_string(),
            screen: "denied".to_string(),
            full_disk_access: "denied".to_string(),
            accessibility: "denied".to_string(),
        }
    }
}

#[tauri::command]
fn daemon_query_os_permissions(app: tauri::AppHandle) -> Result<OsPermissionSnapshot, String> {
    Ok(query_os_permissions_snapshot(Some(&app)))
}

/// Map a frontend permission key to the macOS System Preferences deep-link anchor.
fn permission_key_to_system_prefs_anchor(key: &str) -> &str {
    match key {
        "fullDiskAccess" => "Privacy_AllFiles",
        "camera" => "Privacy_Camera",
        "microphone" => "Privacy_Microphone",
        "screen" => "Privacy_ScreenCapture",
        "accessibility" => "Privacy_Accessibility",
        _ => "Privacy",
    }
}

#[tauri::command]
fn daemon_open_system_settings(permission_key: Option<String>) -> Result<(), String> {
    #[cfg(target_os = "macos")]
    {
        let anchor = permission_key
            .as_deref()
            .map(permission_key_to_system_prefs_anchor)
            .unwrap_or("Privacy");

        // macOS 13+ uses the new System Settings URL scheme
        let url = format!(
            "x-apple.systempreferences:com.apple.preference.security?{}",
            anchor
        );

        std::process::Command::new("open")
            .arg(&url)
            .spawn()
            .map_err(|e| format!("failed to open System Settings: {e}"))?;

        Ok(())
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = permission_key;
        Err("System Settings is only available on macOS".to_string())
    }
}

#[tauri::command]
fn daemon_request_os_permission(
    app: tauri::AppHandle,
    permission_key: String,
) -> Result<OsPermissionSnapshot, String> {
    #[cfg(target_os = "macos")]
    {
        match permission_key.as_str() {
            "camera" => {
                let _ = unsafe { compose_mesh_request_camera_access() };
            }
            "microphone" => {
                let _ = unsafe { compose_mesh_request_microphone_access() };
            }
            "screen" => {
                let _ = unsafe { compose_mesh_request_screen_capture_access() };
                let _ = daemon_open_system_settings(Some(permission_key.clone()));
            }
            "accessibility" => {
                let _ = unsafe { compose_mesh_prompt_accessibility_access() };
                let _ = daemon_open_system_settings(Some(permission_key.clone()));
            }
            "fullDiskAccess" => {
                let _ = daemon_open_system_settings(Some(permission_key.clone()));
            }
            _ => {
                let _ = daemon_open_system_settings(Some(permission_key.clone()));
            }
        }

        Ok(query_os_permissions_snapshot(Some(&app)))
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = permission_key;
        Ok(query_os_permissions_snapshot(Some(&app)))
    }
}

#[tauri::command]
fn local_network_status(
    state: tauri::State<MeshRuntimeState>,
) -> Result<MeshRuntimeStatus, String> {
    let status = state
        .status
        .lock()
        .map_err(|_| "failed to read mesh runtime status".to_string())?;
    Ok(status.clone())
}

#[tauri::command]
async fn local_network_join(
    app: tauri::AppHandle,
    state: tauri::State<'_, MeshRuntimeState>,
    request: MeshJoinRequest,
) -> Result<MeshRuntimeStatus, String> {
    let request = validate_mesh_join_request(&request)?;

    if let Ok(mut stop_guard) = state.stop_tx.lock() {
        if let Some(stop_tx) = stop_guard.take() {
            let _ = stop_tx.send(());
        }
    }

    mark_mesh_status(&app, &request, "connecting");
    append_mesh_log_to_published_agents(&app, &request.published_agents, "mesh join requested");

    let (stop_tx, stop_rx) = oneshot::channel();
    let (command_tx, command_rx) = mpsc::unbounded_channel();
    {
        let mut stop_guard = state
            .stop_tx
            .lock()
            .map_err(|_| "failed to update mesh stop channel".to_string())?;
        *stop_guard = Some(stop_tx);
    }
    {
        let mut command_guard = state
            .command_tx
            .lock()
            .map_err(|_| "failed to update mesh command channel".to_string())?;
        *command_guard = Some(command_tx);
    }

    let app_handle = app.clone();
    tauri::async_runtime::spawn(async move {
        run_mesh_loop(app_handle, request, stop_rx, command_rx).await;
    });

    local_network_status(state)
}

#[tauri::command]
async fn local_network_leave(
    app: tauri::AppHandle,
    state: tauri::State<'_, MeshRuntimeState>,
) -> Result<MeshRuntimeStatus, String> {
    if let Ok(mut stop_guard) = state.stop_tx.lock() {
        if let Some(stop_tx) = stop_guard.take() {
            let _ = stop_tx.send(());
        }
    }
    if let Ok(mut command_guard) = state.command_tx.lock() {
        *command_guard = None;
    }

    let _ = with_mesh_status(&app, |status| {
        *status = MeshRuntimeStatus::default();
    });

    local_network_status(state)
}

fn build_signed_state_envelope(
    local_key: &identity::Keypair,
    snapshot: &MeshStateSnapshot,
    hai_id: &str,
    update_number: u64,
) -> Result<(String, String, String), String> {
    let canonical = canonical_snapshot_json(snapshot)?;
    let state_root_hash = sha256_hex_string(&canonical);
    let signed_at = now_ms();
    let path = compose_hai_path(hai_id, update_number);
    let signature = local_key
        .sign(canonical.as_bytes())
        .map_err(|err| format!("failed to sign mesh state snapshot: {err}"))?;
    let envelope = SignedMeshStateEnvelope {
        version: 2,
        kind: "compose.mesh.state.v2".to_string(),
        collection: COMPOSE_SYNAPSE_COLLECTION.to_string(),
        hai_id: hai_id.to_string(),
        update_number,
        path,
        peer_id: snapshot.peer_id.clone(),
        agent_wallet: snapshot.agent_wallet.clone(),
        user_wallet: snapshot.user_wallet.clone(),
        device_id: snapshot.device_id.clone(),
        chain_id: snapshot.runtime.chain_id,
        signed_at,
        state_root_hash: format!("0x{}", state_root_hash),
        snapshot: snapshot.clone(),
        signature: hex_encode(&signature),
    };
    let envelope_json = serde_json::to_string(&envelope)
        .map_err(|err| format!("failed to encode signed mesh state envelope: {err}"))?;
    Ok((canonical, format!("0x{}", state_root_hash), envelope_json))
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct SignedMeshRequestEnvelope {
    version: u32,
    kind: String,
    action: String,
    collection: String,
    requester_hai_id: String,
    requester_agent_wallet: String,
    #[serde(rename = "requesterUserAddress")]
    requester_user_wallet: String,
    requester_device_id: String,
    requester_peer_id: String,
    target_path: String,
    target_piece_cid: Option<String>,
    target_data_set_id: Option<String>,
    target_piece_id: Option<String>,
    artifact_kind: Option<String>,
    file_name: Option<String>,
    root_cid: Option<String>,
    payload_sha256: Option<String>,
    signed_at: u64,
    signature: String,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct MeshLearningPayload {
    version: u32,
    kind: String,
    created_at: u64,
    title: String,
    summary: String,
    content: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct MeshSharedArtifactPinRuntimeResponse {
    hai_id: String,
    artifact_kind: MeshSharedArtifactKind,
    artifact_number: u64,
    path: String,
    latest_alias: String,
    root_cid: String,
    piece_cid: String,
    collection: String,
}

fn signed_mesh_request_bytes(envelope: &SignedMeshRequestEnvelope) -> Result<String, String> {
    serde_json::to_string(&serde_json::json!([
        envelope.version,
        envelope.kind,
        envelope.action,
        envelope.collection,
        envelope.requester_hai_id,
        envelope.requester_agent_wallet,
        envelope.requester_user_wallet,
        envelope.requester_device_id,
        envelope.requester_peer_id,
        envelope.target_path,
        envelope.target_piece_cid,
        envelope.target_data_set_id,
        envelope.target_piece_id,
        envelope.artifact_kind,
        envelope.file_name,
        envelope.root_cid,
        envelope.payload_sha256,
        envelope.signed_at,
    ]))
    .map_err(|err| format!("failed to encode signed mesh request bytes: {err}"))
}

fn build_signed_mesh_request_json(
    local_key: &identity::Keypair,
    live_status: &MeshRuntimeStatus,
    agent_wallet: &str,
    user_wallet: &str,
    device_id: &str,
    hai_id: &str,
    action: &str,
    collection: &str,
    path: &str,
    artifact_kind: Option<MeshSharedArtifactKind>,
    payload_sha256: Option<String>,
) -> Result<String, String> {
    let requester_peer_id = live_status
        .peer_id
        .clone()
        .ok_or_else(|| "mesh runtime does not have a live peerId yet".to_string())?;
    let unsigned = SignedMeshRequestEnvelope {
        version: 1,
        kind: "compose.mesh.request".to_string(),
        action: action.to_string(),
        collection: collection.to_string(),
        requester_hai_id: hai_id.to_string(),
        requester_agent_wallet: agent_wallet.to_string(),
        requester_user_wallet: user_wallet.to_string(),
        requester_device_id: device_id.to_string(),
        requester_peer_id,
        target_path: path.to_string(),
        target_piece_cid: None,
        target_data_set_id: None,
        target_piece_id: None,
        artifact_kind: artifact_kind
            .map(|value: MeshSharedArtifactKind| value.as_str().to_string()),
        file_name: None,
        root_cid: None,
        payload_sha256,
        signed_at: now_ms(),
        signature: String::new(),
    };
    let sign_bytes = signed_mesh_request_bytes(&unsigned)?;
    let signature = local_key
        .sign(sign_bytes.as_bytes())
        .map_err(|err| format!("failed to sign mesh request envelope: {err}"))?;
    let envelope = SignedMeshRequestEnvelope {
        signature: hex_encode(&signature),
        ..unsigned
    };
    serde_json::to_string(&envelope)
        .map_err(|err| format!("failed to encode signed mesh request envelope: {err}"))
}

fn build_learning_payload_json(request: &MeshPublicationQueueRequest) -> Result<String, String> {
    let title = truncate_string(request.title.clone().unwrap_or_default(), 160);
    let summary = truncate_string(request.summary.clone().unwrap_or_default(), 280);
    let content = request
        .content
        .clone()
        .unwrap_or_default()
        .trim()
        .to_string();
    if title.trim().is_empty() {
        return Err("mesh learning title is required".to_string());
    }
    if summary.trim().is_empty() {
        return Err("mesh learning summary is required".to_string());
    }
    if content.is_empty() {
        return Err("mesh learning content is required".to_string());
    }

    serde_json::to_string(&MeshLearningPayload {
        version: 1,
        kind: "compose.mesh.learning".to_string(),
        created_at: now_ms(),
        title,
        summary,
        content,
    })
    .map_err(|err| format!("failed to encode mesh learning payload: {err}"))
}

async fn runtime_error(route: &str, response: reqwest::Response) -> String {
    let status = response.status();
    let body = response.text().await.unwrap_or_else(|_| String::new());
    if status.as_u16() == 409 {
        let detail = serde_json::from_str::<serde_json::Value>(&body)
            .ok()
            .and_then(|value| {
                value
                    .get("error")
                    .and_then(|entry| entry.as_str())
                    .map(|entry| entry.trim().to_string())
            })
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| body.trim().to_string());
        if detail.is_empty() {
            return "a409: inconsistent agent identity".to_string();
        }
        if detail.to_lowercase().starts_with("a409:") {
            return detail;
        }
        return format!("a409: {detail}");
    }
    if body.trim().is_empty() {
        format!("{route} failed: HTTP {status}")
    } else {
        format!("{route} failed: HTTP {status}: {body}")
    }
}

async fn register_hai_via_local_runtime(
    base_url: &str,
    body: serde_json::Value,
) -> Result<MeshHaiRuntimeRow, String> {
    let client = HttpClient::new();
    let response = client
        .post(format!(
            "{}/mesh/hai/register",
            base_url.trim_end_matches('/')
        ))
        .json(&body)
        .send()
        .await
        .map_err(|err| format!("failed to call local runtime HAI route: {err}"))?;

    if !response.status().is_success() {
        return Err(runtime_error("local runtime HAI route", response).await);
    }

    response
        .json::<MeshHaiRuntimeRow>()
        .await
        .map_err(|err| format!("failed to decode local runtime HAI response: {err}"))
}

async fn anchor_mesh_state_via_local_runtime(
    base_url: &str,
    body: serde_json::Value,
) -> Result<MeshStateAnchorRuntimeResponse, String> {
    let client = HttpClient::new();
    let response = client
        .post(format!(
            "{}/mesh/synapse/anchor",
            base_url.trim_end_matches('/')
        ))
        .json(&body)
        .send()
        .await
        .map_err(|err| format!("failed to call local runtime Synapse route: {err}"))?;

    if !response.status().is_success() {
        return Err(runtime_error("local runtime Synapse route", response).await);
    }

    response
        .json::<MeshStateAnchorRuntimeResponse>()
        .await
        .map_err(|err| format!("failed to decode local runtime Synapse response: {err}"))
}

async fn pin_mesh_learning_via_local_runtime(
    base_url: &str,
    body: serde_json::Value,
) -> Result<MeshSharedArtifactPinRuntimeResponse, String> {
    let client = HttpClient::new();
    let response = client
        .post(format!(
            "{}/mesh/filecoin/pin",
            base_url.trim_end_matches('/')
        ))
        .json(&body)
        .send()
        .await
        .map_err(|err| format!("failed to call local runtime Filecoin Pin route: {err}"))?;

    if !response.status().is_success() {
        return Err(runtime_error("local runtime Filecoin Pin route", response).await);
    }

    response
        .json::<MeshSharedArtifactPinRuntimeResponse>()
        .await
        .map_err(|err| format!("failed to decode local runtime Filecoin Pin response: {err}"))
}

async fn fetch_mesh_reputation_via_local_runtime(
    base_url: &str,
    agent_wallet: &str,
) -> Result<MeshReputationRuntimeResponse, String> {
    let client = HttpClient::new();
    let response = client
        .get(format!(
            "{}/mesh/reputation/summary",
            base_url.trim_end_matches('/')
        ))
        .query(&[("agentWallet", agent_wallet)])
        .send()
        .await
        .map_err(|err| format!("failed to call local runtime reputation route: {err}"))?;

    if !response.status().is_success() {
        return Err(runtime_error("local runtime reputation route", response).await);
    }

    response
        .json::<MeshReputationRuntimeResponse>()
        .await
        .map_err(|err| format!("failed to decode local runtime reputation response: {err}"))
}

async fn anchor_mesh_state_from_command(
    app: &tauri::AppHandle,
    mesh_state: &MeshRuntimeState,
    runtime_host: &LocalRuntimeHostState,
    request: MeshStateAnchorCommandRequest,
) -> Result<MeshStateAnchorRuntimeResponse, String> {
    let live_status = mesh_state
        .status
        .lock()
        .map_err(|_| "failed to read mesh runtime status".to_string())?
        .clone();
    if !live_status.running {
        return Err("mesh runtime is not running".to_string());
    }

    let snapshot = normalize_mesh_state_snapshot_request(&request, &live_status)?;
    let runtime_status = ensure_local_runtime_host(app, runtime_host)?;
    let hai_row = register_hai_via_local_runtime(
        &runtime_status.base_url,
        serde_json::json!({
            "agentWallet": snapshot.agent_wallet,
            "userAddress": snapshot.user_wallet,
            "deviceId": snapshot.device_id,
        }),
    )
    .await?;

    let (canonical_snapshot_json, state_root_hash, envelope_json) = build_signed_state_envelope(
        &load_or_create_mesh_identity(app)?,
        &snapshot,
        &hai_row.hai_id,
        hai_row.next_update_number,
    )?;

    if same_state_root_hash(
        request.previous_state_root_hash.as_deref(),
        &state_root_hash,
    ) && request
        .previous_pdp_piece_cid
        .as_ref()
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false)
        && request.previous_pdp_anchored_at.unwrap_or(0) > 0
    {
        let last_update_number = hai_row
            .last_update_number
            .unwrap_or_else(|| hai_row.next_update_number.saturating_sub(1));
        let last_path = hai_row
            .last_path
            .clone()
            .unwrap_or_else(|| compose_hai_path(&hai_row.hai_id, last_update_number));
        let last_piece_cid = request
            .previous_pdp_piece_cid
            .clone()
            .or_else(|| hai_row.last_piece_cid.clone())
            .ok_or_else(|| "previous PDP piece CID is required for a skipped anchor".to_string())?;
        let last_anchored_at = request
            .previous_pdp_anchored_at
            .or(hai_row.last_anchored_at)
            .ok_or_else(|| {
                "previous PDP anchor timestamp is required for a skipped anchor".to_string()
            })?;

        return Ok(MeshStateAnchorRuntimeResponse {
            hai_id: hai_row.hai_id.clone(),
            update_number: last_update_number,
            path: last_path,
            file_name: compose_hai_path(&hai_row.hai_id, last_update_number),
            latest_alias: format!("compose-{}:latest", hai_row.hai_id),
            state_root_hash,
            pdp_piece_cid: last_piece_cid,
            pdp_anchored_at: last_anchored_at,
            payload_size: envelope_json.len(),
            provider_id: String::new(),
            data_set_id: None,
            piece_id: None,
            retrieval_url: None,
            payer_address: hai_row.payer_address.clone().unwrap_or_default(),
            session_key_expires_at: hai_row.session_key_expires_at.unwrap_or(0),
            source: "local-skip".to_string(),
        });
    }

    let response = anchor_mesh_state_via_local_runtime(
        &runtime_status.base_url,
        serde_json::json!({
            "apiUrl": request.api_url,
            "composeKeyToken": request.compose_key_token,
            "userAddress": snapshot.user_wallet,
            "agentWallet": snapshot.agent_wallet,
            "deviceId": snapshot.device_id,
            "chainId": snapshot.runtime.chain_id,
            "targetSynapseExpiry": request.target_synapse_expiry,
            "haiId": hai_row.hai_id,
            "updateNumber": hai_row.next_update_number,
            "path": compose_hai_path(&hai_row.hai_id, hai_row.next_update_number),
            "canonicalSnapshotJson": canonical_snapshot_json,
            "stateRootHash": state_root_hash,
            "envelopeJson": envelope_json,
            "sessionKeyPrivateKey": hai_row.synapse_session_private_key,
            "payerAddress": hai_row.payer_address,
            "sessionKeyExpiresAt": hai_row.session_key_expires_at,
        }),
    )
    .await?;

    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::{
        apply_active_session_refresh, build_local_runtime_request_body, compose_hai_path,
        derive_hai_id, derive_relay_listen_multiaddrs, derive_relay_peer_id_from_listen_multiaddrs,
        merge_mesh_listen_multiaddrs, normalize_daemon_state_for_local_mode,
        normalize_local_state_json, normalize_manifest_publish_outcome,
        normalize_mesh_api_url_with_loopback_policy, normalize_state_root_hash_for_compare, now_ms,
        remote_action_path_allowed, same_state_root_hash, write_string_atomically,
        ActiveSessionRefreshResponse, DaemonAgentState, DaemonPermissionPolicy, DaemonStateFile,
        MeshManifest, PersistedInstalledAgent, PersistedLocalIdentity,
    };
    use crate::gossipsub;
    use std::{collections::HashMap, fs};

    #[test]
    fn normalize_state_root_hash_handles_optional_prefix() {
        let bare = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let prefixed = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

        assert_eq!(
            normalize_state_root_hash_for_compare(bare).as_deref(),
            Some(bare)
        );
        assert_eq!(
            normalize_state_root_hash_for_compare(prefixed).as_deref(),
            Some(bare)
        );
    }

    #[test]
    fn same_state_root_hash_matches_prefixed_and_bare_forms() {
        let bare = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let prefixed = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

        assert!(same_state_root_hash(Some(bare), prefixed));
        assert!(same_state_root_hash(Some(prefixed), bare));
        assert!(!same_state_root_hash(Some("bbbb"), prefixed));
    }

    #[test]
    fn derive_hai_id_is_alphanumeric_and_stable() {
        let hai_id = derive_hai_id(
            "0x1111111111111111111111111111111111111111",
            "0x2222222222222222222222222222222222222222",
            "device-12345678",
        );

        assert_eq!(hai_id.len(), 6);
        assert!(hai_id.chars().all(|char| char.is_ascii_alphanumeric()));
        assert_eq!(
            hai_id,
            derive_hai_id(
                "0x1111111111111111111111111111111111111111",
                "0x2222222222222222222222222222222222222222",
                "device-12345678",
            )
        );
    }

    #[test]
    fn compose_hai_path_uses_runtime_schema() {
        assert_eq!(compose_hai_path("abc123", 7), "compose-abc123-7");
    }

    #[test]
    fn normalize_manifest_publish_outcome_accepts_insufficient_peers() {
        let manifest = MeshManifest {
            agent_wallet: "0x1111111111111111111111111111111111111111".to_string(),
            user_wallet: "0x2222222222222222222222222222222222222222".to_string(),
            device_id: "device-12345678".to_string(),
            peer_id: "12D3KooWTestPeer".to_string(),
            chain_id: 43113,
            state_version: 1,
            state_root_hash: None,
            pdp_piece_cid: None,
            pdp_anchored_at: None,
            name: "Test".to_string(),
            description: "Test manifest".to_string(),
            model: "gpt-4o".to_string(),
            framework: "manowar".to_string(),
            headline: "headline".to_string(),
            status_line: "status".to_string(),
            skills: Vec::new(),
            mcp_servers: Vec::new(),
            a2a_endpoints: Vec::new(),
            capabilities: Vec::new(),
            agent_card_uri: "cid".to_string(),
            listen_multiaddrs: Vec::new(),
            relay_peer_id: None,
            reputation_score: 0.0,
            total_conclaves: 0,
            successful_conclaves: 0,
            signed_at: 0,
            signature: String::new(),
        };

        let published = normalize_manifest_publish_outcome(
            Err(gossipsub::PublishError::InsufficientPeers),
            &manifest,
        )
        .expect("insufficient peers should not fail local manifest persistence");

        assert_eq!(published.agent_wallet, manifest.agent_wallet);
    }

    #[test]
    fn remote_action_path_allowed_keeps_runtime_tooling_scoped_but_leaves_api_and_connector_open() {
        assert!(remote_action_path_allowed("api", "/api/session"));
        assert!(remote_action_path_allowed(
            "connector",
            "/registry/servers/search?q=mcp"
        ));
        assert!(remote_action_path_allowed("runtime", "/mesh/tools/execute"));
        assert!(remote_action_path_allowed("runtime", "/mesh/conclave/run"));
        assert!(!remote_action_path_allowed("runtime", "/mesh/filecoin/pin"));
    }

    #[test]
    fn build_local_runtime_request_body_preserves_explicit_thread_id() {
        let agent = PersistedInstalledAgent {
            agent_wallet: "0x1111111111111111111111111111111111111111".to_string(),
            ..Default::default()
        };
        let identity = PersistedLocalIdentity {
            user_address: "0x2222222222222222222222222222222222222222".to_string(),
            device_id: "device-12345678".to_string(),
            ..Default::default()
        };

        let body = build_local_runtime_request_body(
            Some(serde_json::json!({
                "toolName": "search_memory",
                "args": {
                    "query": "recent goals"
                }
            })),
            &agent,
            &identity,
            Some("local-agent:0x1111111111111111111111111111111111111111:chat:thread-1"),
        )
        .expect("body should build");

        assert_eq!(
            body["threadId"],
            serde_json::Value::String(
                "local-agent:0x1111111111111111111111111111111111111111:chat:thread-1".to_string()
            )
        );
        assert_eq!(
            body["haiId"],
            serde_json::Value::String(derive_hai_id(
                "0x1111111111111111111111111111111111111111",
                "0x2222222222222222222222222222222222222222",
                "device-12345678",
            ))
        );
    }

    #[test]
    fn build_local_runtime_request_body_requires_explicit_thread_id() {
        let agent = PersistedInstalledAgent {
            agent_wallet: "0x1111111111111111111111111111111111111111".to_string(),
            ..Default::default()
        };
        let identity = PersistedLocalIdentity {
            user_address: "0x2222222222222222222222222222222222222222".to_string(),
            device_id: "device-12345678".to_string(),
            ..Default::default()
        };

        let error = build_local_runtime_request_body(
            Some(serde_json::json!({
                "toolName": "search_memory",
                "args": {
                    "query": "recent goals"
                }
            })),
            &agent,
            &identity,
            None,
        )
        .expect_err("missing threadId should fail");

        assert_eq!(
            error,
            "runtime remote.request requires an explicit local threadId".to_string()
        );
    }

    #[test]
    fn normalize_local_state_json_backfills_missing_agent_hai_ids() {
        let normalized = normalize_local_state_json(
            r#"{
                "identity": {
                    "userAddress": "0x2222222222222222222222222222222222222222",
                    "deviceId": "device-12345678"
                },
                "installedAgents": [
                    {
                        "agentWallet": "0x1111111111111111111111111111111111111111",
                        "network": {
                            "haiId": null
                        }
                    }
                ]
            }"#,
        )
        .expect("state should normalize");

        let parsed = serde_json::from_str::<serde_json::Value>(&normalized)
            .expect("normalized state should remain valid JSON");
        let hai_id = parsed["installedAgents"][0]["network"]["haiId"]
            .as_str()
            .expect("haiId should be present");
        assert_eq!(
            hai_id,
            derive_hai_id(
                "0x1111111111111111111111111111111111111111",
                "0x2222222222222222222222222222222222222222",
                "device-12345678",
            )
        );
    }

    #[test]
    fn normalize_mesh_api_url_rejects_loopback_when_not_explicitly_allowed() {
        assert_eq!(
            normalize_mesh_api_url_with_loopback_policy("http://127.0.0.1:3000", false),
            "https://api.compose.market"
        );
        assert_eq!(
            normalize_mesh_api_url_with_loopback_policy("http://127.0.0.1:3000", true),
            "http://127.0.0.1:3000"
        );
    }

    #[test]
    fn normalize_daemon_state_for_local_mode_keeps_agents_autonomous() {
        let mut daemon = DaemonStateFile {
            version: 1,
            agents: HashMap::from([(
                "0x1111111111111111111111111111111111111111".to_string(),
                DaemonAgentState {
                    agent_wallet: "0x1111111111111111111111111111111111111111".to_string(),
                    runtime_id: Some("local-runtime-host:4310".to_string()),
                    desired_running: true,
                    running: true,
                    status: "running".to_string(),
                    dna_hash: "dna".to_string(),
                    chain_id: 43113,
                    model_id: "gpt-4.1".to_string(),
                    mcp_tools_hash: "hash".to_string(),
                    agent_card_cid: "cid".to_string(),
                    desired_permissions: DaemonPermissionPolicy::default(),
                    permissions: DaemonPermissionPolicy::default(),
                    logs_cursor: 0,
                    last_error: Some("boom".to_string()),
                    updated_at: 1,
                },
            )]),
        };

        normalize_daemon_state_for_local_mode(&mut daemon);

        let agent = daemon
            .agents
            .get("0x1111111111111111111111111111111111111111")
            .expect("agent should remain present");
        assert!(agent.desired_running);
        assert!(agent.running);
        assert_eq!(agent.status, "running");
        assert_eq!(
            agent.runtime_id,
            Some("local-runtime-host:4310".to_string())
        );
        assert_eq!(agent.last_error, Some("boom".to_string()));
        assert_eq!(agent.updated_at, 1);
    }

    #[test]
    fn apply_active_session_refresh_updates_stale_local_identity_with_live_api_truth() {
        let identity = PersistedLocalIdentity {
            user_address: "0x2222222222222222222222222222222222222222".to_string(),
            compose_key_token: "compose-stale".to_string(),
            session_id: "stale-key".to_string(),
            budget: "0".to_string(),
            budget_used: "0".to_string(),
            duration: 0,
            chain_id: 43113,
            expires_at: 1,
            device_id: "device-12345678".to_string(),
        };
        let response = ActiveSessionRefreshResponse {
            has_session: true,
            key_id: "live-key".to_string(),
            token: "compose-live".to_string(),
            budget_remaining: "450000".to_string(),
            budget_used: "550000".to_string(),
            expires_at: 1_700_000_120_000,
            chain_id: 8453,
            ..ActiveSessionRefreshResponse::default()
        };

        let refreshed =
            apply_active_session_refresh(&identity, &response, 1_700_000_000_000).expect("session");

        assert_eq!(refreshed.compose_key_token, "compose-live");
        assert_eq!(refreshed.session_id, "live-key");
        assert_eq!(refreshed.budget, "450000");
        assert_eq!(refreshed.budget_used, "550000");
        assert_eq!(refreshed.duration, 120_000);
        assert_eq!(refreshed.chain_id, 8453);
        assert_eq!(refreshed.expires_at, 1_700_000_120_000);
        assert_eq!(refreshed.user_address, identity.user_address);
        assert_eq!(refreshed.device_id, identity.device_id);
    }

    #[test]
    fn apply_active_session_refresh_clears_identity_when_api_reports_no_session() {
        let identity = PersistedLocalIdentity {
            user_address: "0x2222222222222222222222222222222222222222".to_string(),
            compose_key_token: "compose-live".to_string(),
            session_id: "live-key".to_string(),
            budget: "450000".to_string(),
            budget_used: "550000".to_string(),
            duration: 120_000,
            chain_id: 43113,
            expires_at: 1_700_000_120_000,
            device_id: "device-12345678".to_string(),
        };

        let refreshed = apply_active_session_refresh(
            &identity,
            &ActiveSessionRefreshResponse {
                has_session: false,
                ..ActiveSessionRefreshResponse::default()
            },
            1_700_000_000_000,
        );

        assert!(refreshed.is_none());
    }

    #[test]
    fn derive_relay_listen_multiaddrs_appends_circuit_addresses_for_anchorable_relays() {
        let relay_multiaddrs = vec![
            "/dns4/relay.do.lon1.compose.market/tcp/4002/ws/p2p/12D3KooWQzwPXPUEMPU1Upbo6trSiEo82rhtRpTJGr7SzP2gD7jb".to_string(),
            "/ip4/206.189.203.231/tcp/4001/p2p/12D3KooWPRcHjairRTQuXQdtUux5326pbuWyxsBrrDEzgLdbRKyh".to_string(),
            "invalid".to_string(),
        ];

        let derived = derive_relay_listen_multiaddrs(&relay_multiaddrs)
            .into_iter()
            .map(|addr| addr.to_string())
            .collect::<Vec<_>>();

        assert_eq!(
            derived,
            vec![
                "/dns4/relay.do.lon1.compose.market/tcp/4002/ws/p2p/12D3KooWQzwPXPUEMPU1Upbo6trSiEo82rhtRpTJGr7SzP2gD7jb/p2p-circuit".to_string(),
                "/ip4/206.189.203.231/tcp/4001/p2p/12D3KooWPRcHjairRTQuXQdtUux5326pbuWyxsBrrDEzgLdbRKyh/p2p-circuit".to_string(),
            ]
        );
    }

    #[test]
    fn merge_mesh_listen_multiaddrs_keeps_direct_addrs_and_seeds_relay_circuits() {
        let merged = merge_mesh_listen_multiaddrs(
            &["/ip4/127.0.0.1/tcp/58534".to_string()],
            &[
                "/ip4/206.189.203.231/tcp/4001/p2p/12D3KooWPRcHjairRTQuXQdtUux5326pbuWyxsBrrDEzgLdbRKyh".to_string(),
            ],
        );

        assert_eq!(
            merged,
            vec![
                "/ip4/127.0.0.1/tcp/58534".to_string(),
                "/ip4/206.189.203.231/tcp/4001/p2p/12D3KooWPRcHjairRTQuXQdtUux5326pbuWyxsBrrDEzgLdbRKyh/p2p-circuit".to_string(),
            ]
        );
    }

    #[test]
    fn derive_relay_peer_id_from_listen_multiaddrs_prefers_circuit_relay_peer() {
        let relay_peer_id = derive_relay_peer_id_from_listen_multiaddrs(&[
            "/ip4/127.0.0.1/tcp/58534".to_string(),
            "/dns4/relay.do.lon1.compose.market/tcp/4002/ws/p2p/12D3KooWQzwPXPUEMPU1Upbo6trSiEo82rhtRpTJGr7SzP2gD7jb/p2p-circuit/p2p/12D3KooWDsQfMcprTuDZDk8hdQba6qgKzBEU2CGWtBKdza3Gv5BV".to_string(),
        ]);

        assert_eq!(
            relay_peer_id.as_deref(),
            Some("12D3KooWQzwPXPUEMPU1Upbo6trSiEo82rhtRpTJGr7SzP2gD7jb")
        );
    }

    #[test]
    fn write_string_atomically_replaces_existing_file() {
        let path = std::env::temp_dir().join(format!(
            "compose-mesh-atomic-write-{}-{}.json",
            std::process::id(),
            now_ms()
        ));

        write_string_atomically(&path, "{\"ok\":1}", "test state").expect("initial atomic write");
        write_string_atomically(&path, "{\"ok\":2}", "test state")
            .expect("replacement atomic write");

        let contents = fs::read_to_string(&path).expect("read atomic write result");
        assert_eq!(contents, "{\"ok\":2}");

        let _ = fs::remove_file(path);
    }
}

async fn publish_mesh_manifest_from_command(
    app: &tauri::AppHandle,
    mesh_state: &MeshRuntimeState,
    manifest: MeshManifest,
) -> Result<MeshManifest, String> {
    let live_status = mesh_state
        .status
        .lock()
        .map_err(|_| "failed to read mesh runtime status".to_string())?
        .clone();
    if !live_status.running {
        return Err("mesh runtime is not running".to_string());
    }

    let local_key = load_or_create_mesh_identity(app)?;
    let validated = validate_mesh_manifest(manifest, &live_status)?;
    let signed = sign_mesh_manifest(&local_key, &validated)?;

    let command_tx = mesh_state
        .command_tx
        .lock()
        .map_err(|_| "failed to read mesh command channel".to_string())?
        .clone()
        .ok_or_else(|| "mesh runtime command channel is unavailable".to_string())?;

    let (reply_tx, reply_rx) = oneshot::channel();
    command_tx
        .send(MeshLoopCommand::PublishManifest {
            manifest: signed.clone(),
            reply: reply_tx,
        })
        .map_err(|_| "mesh runtime is no longer accepting commands".to_string())?;

    reply_rx
        .await
        .map_err(|_| "mesh runtime dropped the manifest publish response".to_string())?
}

fn write_mesh_publication_result(
    app: &tauri::AppHandle,
    result: &MeshPublicationQueueResult,
) -> Result<(), String> {
    let dir = result
        .agent_wallet
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map(|value| mesh_publication_agent_results_dir(app, value))
        .transpose()?
        .unwrap_or(mesh_publication_results_dir(app)?);
    let stem = result
        .path
        .as_deref()
        .map(|value| value.trim().replace('/', "_"))
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| result.request_id.clone());
    let file = dir.join(format!("{stem}.json"));
    let serialized = serde_json::to_string_pretty(result)
        .map_err(|err| format!("failed to serialize mesh publication result: {err}"))?;
    fs::write(file, serialized)
        .map_err(|err| format!("failed to write mesh publication result: {err}"))
}

async fn process_mesh_learning_request(
    app: &tauri::AppHandle,
    runtime_host: &LocalRuntimeHostState,
    live_status: &MeshRuntimeStatus,
    request: &MeshPublicationQueueRequest,
) -> Result<MeshPublicationQueueResult, String> {
    let requested_wallet = normalize_wallet(&request.agent_wallet)
        .ok_or_else(|| "mesh publication request agentWallet is invalid".to_string())?;
    if !status_has_published_agent(live_status, requested_wallet.as_str()) {
        return Err(
            "mesh publication request agentWallet does not match the running mesh agent"
                .to_string(),
        );
    }

    let ctx = load_mesh_pub_ctx(app, &requested_wallet).await?;
    let runtime_status = ensure_local_runtime_host(app, runtime_host)?;
    let hai_row = register_hai_via_local_runtime(
        &runtime_status.base_url,
        serde_json::json!({
            "agentWallet": requested_wallet,
            "userAddress": ctx.user_wallet,
            "deviceId": ctx.device_id,
        }),
    )
    .await?;

    let payload_json = build_learning_payload_json(request)?;
    let artifact_kind = MeshSharedArtifactKind::Learning;
    let artifact_number = hai_row.next_learning_number;
    let path = learning_hai_path(&hai_row.hai_id, artifact_kind.clone(), artifact_number);
    let signed_request_json = build_signed_mesh_request_json(
        &load_or_create_mesh_identity(app)?,
        live_status,
        &requested_wallet,
        &ctx.user_wallet,
        &ctx.device_id,
        &hai_row.hai_id,
        "learning.pin",
        "learnings",
        &path,
        Some(artifact_kind.clone()),
        Some(format!("0x{}", sha256_hex_string(&payload_json))),
    )?;

    let response = pin_mesh_learning_via_local_runtime(
        &runtime_status.base_url,
        serde_json::json!({
            "apiUrl": ctx.api_url,
            "composeKeyToken": ctx.compose_key_token,
            "userAddress": ctx.user_wallet,
            "agentWallet": requested_wallet,
            "deviceId": ctx.device_id,
            "chainId": ctx.chain_id,
            "targetSynapseExpiry": ctx.target_synapse_expiry,
            "sessionKeyPrivateKey": hai_row.synapse_session_private_key,
            "payerAddress": hai_row.payer_address,
            "sessionKeyExpiresAt": hai_row.session_key_expires_at,
            "signedRequestJson": signed_request_json,
            "haiId": hai_row.hai_id,
            "artifactKind": artifact_kind,
            "artifactNumber": artifact_number,
            "path": path,
            "payloadJson": payload_json,
            "title": request.title.clone(),
            "summary": request.summary.clone(),
            "accessPriceUsdc": request.access_price_usdc.clone(),
        }),
    )
    .await?;

    Ok(MeshPublicationQueueResult {
        request_id: request.request_id.clone(),
        agent_wallet: Some(request.agent_wallet.clone()),
        kind: Some(MeshPublicationQueueKind::LearningPin),
        success: true,
        error: None,
        hai_id: Some(response.hai_id),
        update_number: None,
        artifact_kind: Some(response.artifact_kind),
        artifact_number: Some(response.artifact_number),
        path: Some(response.path),
        latest_alias: Some(response.latest_alias),
        root_cid: Some(response.root_cid),
        piece_cid: Some(response.piece_cid),
        collection: Some(response.collection),
        state_root_hash: None,
        pdp_piece_cid: None,
        pdp_anchored_at: None,
        manifest: None,
    })
}

async fn process_mesh_publication_request(
    app: &tauri::AppHandle,
    request: MeshPublicationQueueRequest,
) -> MeshPublicationQueueResult {
    let mesh_state = app.state::<MeshRuntimeState>();
    let runtime_host = app.state::<LocalRuntimeHostState>();

    let outcome = async {
        let live_status = mesh_state
            .status
            .lock()
            .map_err(|_| "failed to read mesh runtime status".to_string())?
            .clone();
        if !live_status.running {
            return Err("mesh runtime is not running".to_string());
        }

        match request.kind.clone() {
            MeshPublicationQueueKind::ManifestPublish => {
                let requested_wallet = normalize_wallet(&request.agent_wallet)
                    .ok_or_else(|| "mesh publication request agentWallet is invalid".to_string())?;
                if !status_has_published_agent(&live_status, requested_wallet.as_str()) {
                    return Err("mesh publication request agentWallet does not match the running mesh agent".to_string());
                }

                let reason = request
                    .reason
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .unwrap_or("unspecified");
                let _ = append_daemon_log(
                    app,
                    &requested_wallet,
                    &format!("manifest publish started: {reason}"),
                );

                let (mut manifest, anchor_request) =
                    build_current_mesh_publication(app, &requested_wallet, &live_status).await?;
                let anchor = anchor_mesh_state_from_command(
                    app,
                    mesh_state.inner(),
                    runtime_host.inner(),
                    anchor_request,
                )
                .await?;

                manifest.state_root_hash =
                    Some(anchor.state_root_hash.clone().trim_start_matches("0x").to_string());
                manifest.pdp_piece_cid = Some(anchor.pdp_piece_cid.clone());
                manifest.pdp_anchored_at = Some(anchor.pdp_anchored_at);
                let _ = append_daemon_log(
                    app,
                    &requested_wallet,
                    &format!(
                        "manifest anchored: path={} pdpPieceCid={} providerId={} dataSetId={} pieceId={}",
                        anchor.path,
                        anchor.pdp_piece_cid,
                        anchor.provider_id,
                        anchor.data_set_id.as_deref().unwrap_or("-"),
                        anchor.piece_id.as_deref().unwrap_or("-"),
                    ),
                );

                let published =
                    publish_mesh_manifest_from_command(app, mesh_state.inner(), manifest).await?;
                persist_manifest_update(app, &published)?;
                let _ = app.emit("mesh-manifest-updated", &published);
                let _ = append_daemon_log(
                    app,
                    &requested_wallet,
                    &format!(
                        "manifest published: path={} stateVersion={} pdpPieceCid={}",
                        anchor.path,
                        published.state_version,
                        anchor.pdp_piece_cid,
                    ),
                );

                Ok(MeshPublicationQueueResult {
                    request_id: request.request_id.clone(),
                    agent_wallet: Some(request.agent_wallet.clone()),
                    kind: Some(MeshPublicationQueueKind::ManifestPublish),
                    success: true,
                    error: None,
                    hai_id: Some(anchor.hai_id),
                    update_number: Some(anchor.update_number),
                    artifact_kind: None,
                    artifact_number: None,
                    path: Some(anchor.path),
                    latest_alias: Some(anchor.latest_alias),
                    root_cid: None,
                    piece_cid: None,
                    collection: None,
                    state_root_hash: Some(anchor.state_root_hash),
                    pdp_piece_cid: Some(anchor.pdp_piece_cid),
                    pdp_anchored_at: Some(anchor.pdp_anchored_at),
                    manifest: Some(published),
                })
            }
            MeshPublicationQueueKind::LearningPin => {
                process_mesh_learning_request(
                    app,
                    runtime_host.inner(),
                    &live_status,
                    &request,
                )
                .await
            }
        }
    }
    .await;

    match outcome {
        Ok(result) => result,
        Err(error) => {
            if matches!(request.kind, MeshPublicationQueueKind::ManifestPublish) {
                let _ = append_daemon_log(
                    app,
                    &request.agent_wallet,
                    &format!("manifest publish failed: {error}"),
                );
            }

            MeshPublicationQueueResult {
                request_id: request.request_id,
                agent_wallet: Some(request.agent_wallet),
                kind: Some(request.kind),
                success: false,
                error: Some(error),
                hai_id: None,
                update_number: None,
                artifact_kind: None,
                artifact_number: None,
                path: None,
                latest_alias: None,
                root_cid: None,
                piece_cid: None,
                collection: None,
                state_root_hash: None,
                pdp_piece_cid: None,
                pdp_anchored_at: None,
                manifest: None,
            }
        }
    }
}

async fn process_pending_mesh_publication_requests(app: &tauri::AppHandle) -> Result<(), String> {
    let requests_dir = mesh_publication_requests_dir(app)?;
    let mut files = Vec::new();
    collect_json_files_recursive(&requests_dir, &mut files)?;
    files.sort();

    for path in files {
        let raw = match fs::read_to_string(&path) {
            Ok(value) => value,
            Err(error) => {
                eprintln!(
                    "[mesh] failed to read mesh publication request {}: {}",
                    path.display(),
                    error
                );
                let _ = fs::remove_file(&path);
                continue;
            }
        };

        let request = match serde_json::from_str::<MeshPublicationQueueRequest>(&raw) {
            Ok(value) => value,
            Err(error) => {
                eprintln!(
                    "[mesh] failed to parse mesh publication request {}: {}",
                    path.display(),
                    error
                );
                let fallback_id = path
                    .file_stem()
                    .and_then(|value| value.to_str())
                    .unwrap_or("mesh-request")
                    .to_string();
                let _ = write_mesh_publication_result(
                    app,
                    &MeshPublicationQueueResult {
                        request_id: fallback_id,
                        agent_wallet: None,
                        kind: None,
                        success: false,
                        error: Some(format!("Invalid mesh publication request: {error}")),
                        hai_id: None,
                        update_number: None,
                        artifact_kind: None,
                        artifact_number: None,
                        path: None,
                        latest_alias: None,
                        root_cid: None,
                        piece_cid: None,
                        collection: None,
                        state_root_hash: None,
                        pdp_piece_cid: None,
                        pdp_anchored_at: None,
                        manifest: None,
                    },
                );
                let _ = fs::remove_file(&path);
                continue;
            }
        };

        let result = process_mesh_publication_request(app, request).await;
        if let Err(error) = write_mesh_publication_result(app, &result) {
            eprintln!(
                "[mesh] failed to persist mesh publication result: {}",
                error
            );
        }
        let _ = fs::remove_file(&path);
    }

    Ok(())
}

fn normalize_api_base(api_url: &str) -> Result<String, String> {
    let trimmed = api_url.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return Err("Local updater apiUrl is required".to_string());
    }
    if !trimmed.starts_with("https://") && !trimmed.starts_with("http://") {
        return Err("Local updater apiUrl must start with http:// or https://".to_string());
    }
    Ok(trimmed.to_string())
}

fn normalize_updater_pubkey(pubkey: &str) -> Result<String, String> {
    let normalized = pubkey.trim();
    if normalized.is_empty() {
        return Err("Local updater public key is required".to_string());
    }
    Ok(normalized.to_string())
}

fn build_local_update_endpoint(api_url: &str) -> Result<String, String> {
    let api_base = normalize_api_base(api_url)?;
    Ok(format!(
        "{api_base}/api/local/updates/{{{{target}}}}/{{{{arch}}}}/{{{{current_version}}}}"
    ))
}

fn build_local_updater(
    app: &tauri::AppHandle,
    api_url: &str,
    pubkey: &str,
) -> Result<tauri_plugin_updater::Updater, String> {
    let endpoint = build_local_update_endpoint(api_url)?;
    let pubkey = normalize_updater_pubkey(pubkey)?;
    let endpoint = endpoint
        .parse()
        .map_err(|error| format!("Invalid local updater endpoint: {error}"))?;

    app.updater_builder()
        .pubkey(pubkey)
        .endpoints(vec![endpoint])
        .map_err(|error| format!("Failed to configure local updater endpoints: {error}"))?
        .build()
        .map_err(|error| format!("Failed to initialize local updater: {error}"))
}

#[tauri::command]
async fn local_check_for_updates(
    app: tauri::AppHandle,
    api_url: String,
    pubkey: String,
) -> Result<LocalUpdateCheckResult, String> {
    let updater = build_local_updater(&app, &api_url, &pubkey)?;
    let current_version = app.package_info().version.to_string();
    let update = updater
        .check()
        .await
        .map_err(|error| format!("Failed to check for local updates: {error}"))?;

    Ok(match update {
        Some(update) => LocalUpdateCheckResult {
            enabled: true,
            available: true,
            current_version: Some(update.current_version),
            version: Some(update.version),
            body: update.body,
            date: update.date.map(|value| value.to_string()),
        },
        None => LocalUpdateCheckResult {
            enabled: true,
            available: false,
            current_version: Some(current_version),
            version: None,
            body: None,
            date: None,
        },
    })
}

#[tauri::command]
async fn local_install_update(
    app: tauri::AppHandle,
    api_url: String,
    pubkey: String,
) -> Result<(), String> {
    let updater = build_local_updater(&app, &api_url, &pubkey)?;
    let update = updater
        .check()
        .await
        .map_err(|error| format!("Failed to check for local updates: {error}"))?;
    let Some(update) = update else {
        return Err("Compose Local is already on the latest version".to_string());
    };

    update
        .download_and_install(|_, _| {}, || {})
        .await
        .map_err(|error| format!("Failed to install local update: {error}"))?;

    app.restart();
}

#[cfg(desktop)]
use tauri_plugin_deep_link::DeepLinkExt;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_shell::init())
        .plugin(tauri_plugin_deep_link::init())
        .plugin(tauri_plugin_updater::Builder::new().build())
        .manage(PendingDeepLinks::default())
        .manage(MeshRuntimeState::default())
        .manage(SessionBudgetTracker::default())
        .manage(LocalDaemonState::default())
        .manage(LocalRuntimeHostState::default())
        .invoke_handler(tauri::generate_handler![
            get_local_paths,
            set_local_base_dir,
            load_local_state,
            save_local_state,
            ensure_local_dir,
            list_local_files,
            write_local_file,
            read_local_file,
            remove_local_path,
            check_missing_binaries,
            consume_pending_deep_links,
            local_network_status,
            local_network_join,
            local_network_leave,
            daemon_install_agent,
            daemon_remove_agent,
            daemon_update_permissions,
            daemon_get_agent_status,
            daemon_tail_logs,
            daemon_run_local_agent_conversation,
            daemon_query_os_permissions,
            daemon_open_system_settings,
            daemon_request_os_permission,
            local_check_for_updates,
            local_install_update
        ])
        .setup(|app| {
            let mut daemon_disk_state =
                read_daemon_state_from_disk(&app.handle()).unwrap_or_default();
            normalize_daemon_state_for_local_mode(&mut daemon_disk_state);
            let daemon_state = app.state::<LocalDaemonState>();

            if let Ok(mut guard) = daemon_state.state.lock() {
                *guard = daemon_disk_state;
            }
            if let Ok(guard) = daemon_state.state.lock() {
                let _ = write_daemon_state_to_disk(&app.handle(), &guard);
            }

            {
                let handle = app.handle().clone();
                tauri::async_runtime::spawn(async move {
                    loop {
                        if let Err(error) = process_pending_mesh_publication_requests(&handle).await
                        {
                            eprintln!(
                                "[mesh] failed to process local mesh publication queue: {}",
                                error
                            );
                        }
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                });
            }

            {
                let handle = app.handle().clone();
                tauri::async_runtime::spawn(async move {
                    loop {
                        if let Err(error) = process_local_agent_heartbeats(&handle).await {
                            eprintln!("[mesh] failed to process local agent heartbeats: {}", error);
                        }
                        tokio::time::sleep(Duration::from_millis(LOCAL_AGENT_HEARTBEAT_POLL_MS))
                            .await;
                    }
                });
            }

            #[cfg(desktop)]
            {
                let handle = app.handle().clone();

                // Register deep link handler
                app.deep_link().on_open_url(move |event| {
                    let urls = event.urls();
                    for url in urls {
                        let url_str = url.to_string();
                        println!("[DeepLink] Received: {}", url_str);
                        if let Ok(mut guard) = handle.state::<PendingDeepLinks>().0.lock() {
                            guard.push(url_str.clone());
                        }

                        // Emit event to frontend
                        if let Some(window) = handle.get_webview_window("main") {
                            let _ = window.emit(
                                "deep-link",
                                serde_json::json!({
                                    "url": url_str,
                                    "scheme": url.scheme(),
                                    "host": url.host_str().unwrap_or(""),
                                    "path": url.path(),
                                    "query": url.query(),
                                }),
                            );
                        }
                    }
                });
            }

            Ok(())
        })
        .build(tauri::generate_context!())
        .expect("failed to build Compose Mesh")
        .run(|app, event| {
            if matches!(event, RunEvent::Exit) {
                let _ = runtime_host::stop_local_runtime_host(
                    app,
                    app.state::<LocalRuntimeHostState>().inner(),
                );
            }
        });
}
