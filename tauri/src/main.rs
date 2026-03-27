#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

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
use reqwest::Client as HttpClient;
use sha2::{Digest, Sha256};
use sha3::Keccak256;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::str::FromStr;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tauri::{Emitter, Manager, RunEvent};
use tauri_plugin_updater::UpdaterExt;
use tokio::sync::{mpsc, oneshot};

use runtime_host::{
    current_runtime_host_auth_token, LocalRuntimeHostState, LocalRuntimeHostStatus,
};

const LOCAL_RUNTIME_AUTH_HEADER: &str = "x-compose-local-runtime-token";
const COMPOSE_SYNAPSE_COLLECTION: &str = "compose";

#[derive(Debug, serde::Serialize)]
struct LocalPaths {
    base_dir: String,
    state_file: String,
    agents_dir: String,
    skills_dir: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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
struct DaemonSkillState {
    enabled: bool,
    eligible: bool,
    source: String,
    revision: String,
    updated_at: u64,
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
    permissions: DaemonPermissionPolicy,
    skills: HashMap<String, DaemonSkillState>,
    logs_cursor: usize,
    last_error: Option<String>,
    updated_at: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct PermissionDecisionTicket {
    id: String,
    agent_wallet: String,
    action: String,
    decision: String,
    issued_at: u64,
    expires_at: u64,
    nonce: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct DaemonStateFile {
    version: u32,
    agents: HashMap<String, DaemonAgentState>,
    tickets: HashMap<String, PermissionDecisionTicket>,
}

impl Default for DaemonStateFile {
    fn default() -> Self {
        Self {
            version: 1,
            agents: HashMap::new(),
            tickets: HashMap::new(),
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
struct MeshStateSnapshotReceipts {
    latest_conclave_ids: Vec<String>,
    latest_hypercert_ids: Vec<String>,
    latest_proof_ids: Vec<String>,
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
    receipts: MeshStateSnapshotReceipts,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct MeshStateSnapshotRequest {
    agent_wallet: String,
    chain_id: u32,
    peer_id: String,
    model_id: String,
    dna_hash: String,
    agent_card_cid: String,
    mcp_tools_hash: String,
    skills: Vec<String>,
    capabilities: Vec<String>,
    mcp_servers: Vec<String>,
    a2a_endpoints: Vec<String>,
    #[serde(default)]
    latest_conclave_ids: Vec<String>,
    #[serde(default)]
    latest_hypercert_ids: Vec<String>,
    #[serde(default)]
    latest_proof_ids: Vec<String>,
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
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct PersistedLocalIdentity {
    user_address: String,
    compose_key_token: String,
    chain_id: u32,
    expires_at: u64,
    device_id: String,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct PersistedInstalledSkill {
    id: String,
    enabled: bool,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct PersistedInstalledAgent {
    agent_wallet: String,
    metadata: PersistedAgentMetadata,
    lock: PersistedAgentLock,
    network: PersistedAgentNetworkState,
    skill_states: HashMap<String, PersistedAgentSkillState>,
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
    public_card: Option<MeshAgentCard>,
    manifest: Option<MeshManifest>,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct PersistedAgentSkillState {
    skill_id: String,
    enabled: bool,
    eligible: bool,
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

fn normalize_snapshot_receipt_ids(values: &[String]) -> Vec<String> {
    normalize_manifest_atoms(values, 64, 128)
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
    format!("compose-{}-#{}", hai_id, update_number)
}

fn knowledge_hai_path(hai_id: &str, kind: MeshSharedArtifactKind, artifact_number: u64) -> String {
    format!("knowledge-{}-{}-#{}", hai_id, kind.as_str(), artifact_number)
}

fn persist_manifest_update(app: &tauri::AppHandle, manifest: &MeshManifest) -> Result<(), String> {
    let state_path = local_state_path(app)?;
    if !state_path.exists() {
        return Ok(());
    }

    let raw = fs::read_to_string(&state_path)
        .map_err(|err| format!("failed to read local state for manifest update: {err}"))?;
    let mut value = serde_json::from_str::<serde_json::Value>(&raw)
        .map_err(|err| format!("failed to parse local state for manifest update: {err}"))?;

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

    let serialized = serde_json::to_string_pretty(&value)
        .map_err(|err| format!("failed to serialize updated local state: {err}"))?;
    fs::write(&state_path, serialized)
        .map_err(|err| format!("failed to persist updated local state: {err}"))
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

fn load_mesh_pub_ctx(app: &tauri::AppHandle, agent_wallet: &str) -> Result<MeshPubCtx, String> {
    let state = load_persisted_local_state(app)?;
    let identity = state
        .identity
        .as_ref()
        .ok_or_else(|| "local identity is required for mesh publication".to_string())?;
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

fn build_current_mesh_publication(
    app: &tauri::AppHandle,
    agent_wallet: &str,
    live_status: &MeshRuntimeStatus,
) -> Result<(MeshManifest, MeshStateAnchorCommandRequest), String> {
    let ctx = load_mesh_pub_ctx(app, agent_wallet)?;
    let normalized_agent_wallet = normalize_wallet(agent_wallet)
        .ok_or_else(|| "mesh publication request agentWallet is invalid".to_string())?;
    let agent = ctx.agent.clone();

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
        mcp_servers: previous_manifest
            .as_ref()
            .map(|value| value.mcp_servers.clone())
            .unwrap_or_default(),
        a2a_endpoints: a2a_endpoints.clone(),
        capabilities: capabilities.clone(),
        agent_card_uri: normalize_agent_card_uri(
            &agent.metadata.agent_card_uri,
            &agent.lock.agent_card_cid,
        )?,
        listen_multiaddrs: normalize_multiaddr_strings(&live_status.listen_multiaddrs),
        relay_peer_id: previous_manifest
            .as_ref()
            .and_then(|value| value.relay_peer_id.clone()),
        reputation_score: previous_manifest
            .as_ref()
            .map(|value| value.reputation_score)
            .unwrap_or(0.0),
        total_conclaves: previous_manifest
            .as_ref()
            .map(|value| value.total_conclaves)
            .unwrap_or(0),
        successful_conclaves: previous_manifest
            .as_ref()
            .map(|value| value.successful_conclaves)
            .unwrap_or(0),
        signed_at: 0,
        signature: String::new(),
    };
    manifest.state_version = next_manifest_state_version(previous_manifest.as_ref(), &manifest);

    let snapshot = MeshStateSnapshotRequest {
        agent_wallet: normalized_agent_wallet,
        chain_id: manifest.chain_id,
        peer_id: manifest.peer_id.clone(),
        model_id,
        dna_hash: agent.lock.dna_hash.clone(),
        agent_card_cid: agent.lock.agent_card_cid.clone(),
        mcp_tools_hash: agent.lock.mcp_tools_hash.clone(),
        skills,
        capabilities,
        mcp_servers: manifest.mcp_servers.clone(),
        a2a_endpoints,
        latest_conclave_ids: Vec::new(),
        latest_hypercert_ids: Vec::new(),
        latest_proof_ids: Vec::new(),
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
        receipts: MeshStateSnapshotReceipts {
            latest_conclave_ids: normalize_snapshot_receipt_ids(
                &request.snapshot.latest_conclave_ids,
            ),
            latest_hypercert_ids: normalize_snapshot_receipt_ids(
                &request.snapshot.latest_hypercert_ids,
            ),
            latest_proof_ids: normalize_snapshot_receipt_ids(&request.snapshot.latest_proof_ids),
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

    // Check for a user-configured base dir override
    let override_file = app_data.join("base_dir_override.txt");
    let base = if override_file.exists() {
        if let Ok(custom) = fs::read_to_string(&override_file) {
            let trimmed = custom.trim();
            if !trimmed.is_empty() {
                PathBuf::from(trimmed)
            } else {
                app_data.join("runtime")
            }
        } else {
            app_data.join("runtime")
        }
    } else {
        app_data.join("runtime")
    };

    fs::create_dir_all(&base)
        .map_err(|err| format!("failed to create app data directory: {err}"))?;
    Ok(base)
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

fn mesh_publication_requests_dir(app: &tauri::AppHandle) -> Result<PathBuf, String> {
    let dir = resolve_base_dir(app)?
        .join("mesh")
        .join("publications")
        .join("requests");
    fs::create_dir_all(&dir)
        .map_err(|err| format!("failed to create mesh publication requests dir: {err}"))?;
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

fn load_persisted_local_state(app: &tauri::AppHandle) -> Result<PersistedLocalState, String> {
    let file = local_state_path(app)?;
    if !file.exists() {
        return Ok(PersistedLocalState::default());
    }

    let raw =
        fs::read_to_string(&file).map_err(|err| format!("failed to read local state: {err}"))?;
    serde_json::from_str(&raw).map_err(|err| format!("failed to parse local state: {err}"))
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

fn check_agent_permission(
    daemon: &DaemonStateFile,
    agent_wallet: &str,
    permission_key: &str,
) -> Result<(), String> {
    let agent = daemon
        .agents
        .get(agent_wallet)
        .ok_or_else(|| format!("agent not installed: {}", agent_wallet))?;

    let decision = match permission_key {
        "shell" => &agent.permissions.shell,
        "fs.read" => &agent.permissions.filesystem_read,
        "fs.write" => &agent.permissions.filesystem_write,
        "fs.edit" => &agent.permissions.filesystem_edit,
        "fs.delete" => &agent.permissions.filesystem_delete,
        "camera" => &agent.permissions.camera,
        "microphone" => &agent.permissions.microphone,
        "network" => &agent.permissions.network,
        _ => return Err(format!("unknown permission key: {}", permission_key)),
    };

    if decision.as_str() != "allow" {
        return Err(format!(
            "permission '{}' is denied for agent {}",
            permission_key, agent_wallet
        ));
    }

    match permission_key {
        "camera" if query_tcc_status("kTCCServiceCamera") != "granted" => {
            Err("camera access is not granted to Compose Mesh".to_string())
        }
        "microphone" if query_tcc_status("kTCCServiceMicrophone") != "granted" => {
            Err("microphone access is not granted to Compose Mesh".to_string())
        }
        "fs.read" | "fs.write" | "fs.edit" | "fs.delete"
            if query_tcc_status("kTCCServiceSystemPolicyAllFiles") != "granted" =>
        {
            Err("full disk access is not granted to Compose Mesh".to_string())
        }
        _ => Ok(()),
    }
}

fn now_nonce() -> String {
    format!("{}-{}", now_ms(), std::process::id())
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
        status.updated_at = now_ms();
        if status_value == "dormant" {
            status.published_agents.clear();
            status.peer_id = None;
            status.listen_multiaddrs.clear();
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

fn emit_peer_index(app: &tauri::AppHandle, peer_cache: &HashMap<String, PeerCacheEntry>) {
    let peers = peer_cache
        .iter()
        .map(|(peer_id, entry)| {
            let node_distance = if entry.stale {
                3
            } else if !entry.listen_addrs.is_empty() {
                1
            } else {
                2
            };
            serde_json::json!({
                "peerId": peer_id,
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
        status.listen_multiaddrs.clear();
        status.peers_discovered = 0;
        status.last_heartbeat_at = None;
        status.last_error = None;
        status.updated_at = now_ms();
    });

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
                            .and_then(|payload| swarm
                                .behaviour_mut()
                                .gossipsub
                                .publish(manifest_topic.clone(), payload)
                                .map_err(|err| format!("manifest publish failed: {err}")))
                            .map(|_| manifest.clone());

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
                    if active == 0 && connected_peers.is_empty() {
                        status.status = "connecting".to_string();
                    }
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
                        if status.peers_discovered == 0 {
                            status.status = "connecting".to_string();
                        }
                    } else if status.peers_discovered > 0 || !connected_peers.is_empty() {
                        status.status = "online".to_string();
                    }
                });
            }
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::NewListenAddr { address, .. } => {
                        let addr = address.to_string();
                        let _ = with_mesh_status(&app, |status| {
                            if !status.listen_multiaddrs.contains(&addr) {
                                status.listen_multiaddrs.push(addr);
                            }
                            if status.status == "connecting" {
                                status.status = "online".to_string();
                            }
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
                    }
                    SwarmEvent::ConnectionClosed { peer_id, .. } => {
                        connected_peers.remove(&peer_id);
                        let active = recompute_peer_cache_status(&mut peer_cache);
                        let _ = with_mesh_status(&app, |status| {
                            status.peers_discovered = active as u32;
                            if active == 0 && connected_peers.is_empty() {
                                status.status = "connecting".to_string();
                            }
                            status.updated_at = now_ms();
                        });
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
                                        let entry = peer_cache.entry(envelope.peer_id.clone()).or_insert(PeerCacheEntry {
                                            last_seen_ms: envelope.ts_ms,
                                            stale: false,
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
                                            status.status = if active > 0 || !connected_peers.is_empty() {
                                                "online".to_string()
                                            } else {
                                                "connecting".to_string()
                                            };
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
    let base_dir = resolve_base_dir(&app)?;
    let state_file = base_dir.join("state.json");

    if !state_file.exists() {
        return Ok("{}".to_string());
    }

    fs::read_to_string(&state_file).map_err(|err| format!("failed to read state file: {err}"))
}

#[tauri::command]
fn save_local_state(app: tauri::AppHandle, state_json: String) -> Result<(), String> {
    let base_dir = resolve_base_dir(&app)?;
    let state_file = base_dir.join("state.json");

    if let Some(parent) = state_file.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| format!("failed to create state parent directory: {err}"))?;
    }
    fs::write(&state_file, state_json)
        .map_err(|err| format!("failed to write state file: {err}"))?;
    Ok(())
}

#[tauri::command]
fn ensure_local_dir(app: tauri::AppHandle, relative_path: String) -> Result<String, String> {
    let dir = resolve_managed_path(&app, &relative_path)?;
    fs::create_dir_all(&dir).map_err(|err| format!("failed to create directory: {err}"))?;
    Ok(dir.to_string_lossy().to_string())
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

    let files = vec![
        (
            workspace.join("DNA.md"),
            format!(
                "# DNA\nagentWallet: {}\nmodelId: {}\nchainId: {}\nagentCardCid: {}\nmcpToolsHash: {}\ndnaHash: {}\nlockedAt: {}\n",
                payload.agent_wallet.to_lowercase(),
                payload.model_id,
                payload.chain_id,
                payload.agent_card_cid,
                payload.mcp_tools_hash,
                payload.dna_hash,
                now_ms()
            ),
        ),
        (
            workspace.join("SOUL.md"),
            "# SOUL\n\nMutable behavior and persona notes for this local deployment.\n".to_string(),
        ),
        (
            workspace.join("AGENTS.md"),
            "# AGENTS\n\nPer-agent local operating instructions.\n".to_string(),
        ),
        (
            workspace.join("TOOLS.md"),
            "# TOOLS\n\nTool identities are immutable from DNA.md.\n".to_string(),
        ),
        (
            workspace.join("IDENTITY.md"),
            format!("# IDENTITY\n\nagentWallet: {}\n", payload.agent_wallet.to_lowercase()),
        ),
        (
            workspace.join("USER.md"),
            "# USER\n\nLocal user preferences and runtime instructions.\n".to_string(),
        ),
        (
            workspace.join("HEARTBEAT.md"),
            "# HEARTBEAT\n\nKeep checks lightweight. Reply HEARTBEAT_OK when idle.\n".to_string(),
        ),
        (
            workspace.join("runtime.log"),
            "".to_string(),
        ),
    ];

    for (file, content) in files {
        if !file.exists() {
            fs::write(&file, content)
                .map_err(|err| format!("failed to write bootstrap file: {err}"))?;
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

fn daemon_state_snapshot(
    state: &tauri::State<'_, LocalDaemonState>,
) -> Result<DaemonStateFile, String> {
    state
        .state
        .lock()
        .map(|guard| guard.clone())
        .map_err(|_| "failed to snapshot daemon state".to_string())
}

fn activate_installed_agents(daemon: &mut DaemonStateFile) {
    for agent in daemon.agents.values_mut() {
        agent.desired_running = true;
        if agent.status == "stopped" || agent.status == "stopping" {
            agent.status = "starting".to_string();
            agent.runtime_id = None;
            agent.last_error = None;
            agent.updated_at = now_ms();
        }
    }
}

fn fallback_runtime_host_status(
    runtime_host_state: &LocalRuntimeHostState,
    error: String,
) -> LocalRuntimeHostStatus {
    let mut status =
        runtime_host::current_runtime_host_status(runtime_host_state).unwrap_or_default();
    status.running = false;
    status.status = "error".to_string();
    status.last_error = Some(error);
    status.updated_at = now_ms();
    status
}

#[tauri::command]
fn daemon_install_agent(
    app: tauri::AppHandle,
    state: tauri::State<'_, LocalDaemonState>,
    runtime_host_state: tauri::State<'_, LocalRuntimeHostState>,
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

    with_daemon_state(&app, &state, |daemon| {
        let entry = daemon
            .agents
            .entry(normalized_wallet.clone())
            .or_insert(DaemonAgentState {
                agent_wallet: normalized_wallet.clone(),
                runtime_id: None,
                desired_running: true,
                running: false,
                status: "starting".to_string(),
                dna_hash: normalized_payload.dna_hash.clone(),
                chain_id: normalized_payload.chain_id,
                model_id: normalized_payload.model_id.clone(),
                mcp_tools_hash: normalized_payload.mcp_tools_hash.clone(),
                agent_card_cid: normalized_payload.agent_card_cid.clone(),
                permissions: DaemonPermissionPolicy::default(),
                skills: HashMap::new(),
                logs_cursor: 0,
                last_error: None,
                updated_at: now_ms(),
            });

        entry.chain_id = normalized_payload.chain_id;
        entry.model_id = normalized_payload.model_id.clone();
        entry.mcp_tools_hash = normalized_payload.mcp_tools_hash.clone();
        entry.agent_card_cid = normalized_payload.agent_card_cid.clone();
        entry.dna_hash = normalized_payload.dna_hash.clone();
        entry.desired_running = true;
        entry.running = false;
        entry.status = "starting".to_string();
        entry.runtime_id = None;
        entry.last_error = None;
        entry.updated_at = now_ms();
        Ok(())
    })?;

    let snapshot = daemon_state_snapshot(&state)?;
    let host_status =
        runtime_host::reconcile_local_runtime_host(&app, runtime_host_state.inner(), &snapshot)
            .unwrap_or_else(|error| fallback_runtime_host_status(runtime_host_state.inner(), error));

    with_daemon_state(&app, &state, |daemon| {
        runtime_host::apply_runtime_host_status(daemon, &host_status);
        daemon
            .agents
            .get(&normalized_wallet)
            .cloned()
            .ok_or_else(|| format!("agent not installed: {normalized_wallet}"))
    })
}

#[tauri::command]
fn daemon_remove_agent(
    app: tauri::AppHandle,
    state: tauri::State<'_, LocalDaemonState>,
    runtime_host_state: tauri::State<'_, LocalRuntimeHostState>,
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

    let snapshot = daemon_state_snapshot(&state)?;
    let host_status =
        runtime_host::reconcile_local_runtime_host(&app, runtime_host_state.inner(), &snapshot)
            .or_else(|_| runtime_host::current_runtime_host_status(runtime_host_state.inner()))?;

    with_daemon_state(&app, &state, |daemon| {
        runtime_host::apply_runtime_host_status(daemon, &host_status);
        Ok(())
    })?;

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

    with_daemon_state(&app, &state, |daemon| {
        let entry = daemon
            .agents
            .get_mut(&wallet)
            .ok_or_else(|| format!("agent not installed: {wallet}"))?;
        entry.permissions = normalized_policy.clone();
        entry.updated_at = now_ms();
        Ok(entry.clone())
    })
}

#[tauri::command]
fn daemon_update_skill(
    app: tauri::AppHandle,
    state: tauri::State<'_, LocalDaemonState>,
    agent_wallet: String,
    skill_key: String,
    enabled: bool,
) -> Result<DaemonAgentState, String> {
    let wallet =
        normalize_wallet(&agent_wallet).ok_or_else(|| "invalid agentWallet".to_string())?;
    let normalized_skill_key = skill_key.trim().to_string();
    if normalized_skill_key.is_empty() {
        return Err("skillKey is required".to_string());
    }

    with_daemon_state(&app, &state, |daemon| {
        let entry = daemon
            .agents
            .get_mut(&wallet)
            .ok_or_else(|| format!("agent not installed: {wallet}"))?;

        let updated_at = now_ms();
        let existing = entry.skills.get(&normalized_skill_key).cloned();
        entry.skills.insert(
            normalized_skill_key.clone(),
            DaemonSkillState {
                enabled,
                eligible: existing.as_ref().map(|v| v.eligible).unwrap_or(true),
                source: existing
                    .as_ref()
                    .map(|v| v.source.clone())
                    .unwrap_or_else(|| "agent".to_string()),
                revision: existing
                    .as_ref()
                    .map(|v| v.revision.clone())
                    .unwrap_or_else(|| format!("rev-{}", updated_at)),
                updated_at,
            },
        );
        entry.updated_at = updated_at;
        Ok(entry.clone())
    })
}

#[tauri::command]
fn daemon_get_agent_status(
    state: tauri::State<'_, LocalDaemonState>,
    agent_wallet: String,
) -> Result<Option<DaemonAgentState>, String> {
    let wallet =
        normalize_wallet(&agent_wallet).ok_or_else(|| "invalid agentWallet".to_string())?;
    let guard = state
        .state
        .lock()
        .map_err(|_| "failed to lock daemon state".to_string())?;
    Ok(guard.agents.get(&wallet).cloned())
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
fn daemon_issue_permission_ticket(
    app: tauri::AppHandle,
    state: tauri::State<'_, LocalDaemonState>,
    agent_wallet: String,
    action: String,
    decision: String,
    ttl_seconds: Option<u64>,
) -> Result<PermissionDecisionTicket, String> {
    let wallet =
        normalize_wallet(&agent_wallet).ok_or_else(|| "invalid agentWallet".to_string())?;
    let normalized_action = action.trim().to_string();
    if normalized_action.is_empty() {
        return Err("action is required".to_string());
    }
    let normalized_decision = normalize_daemon_decision(&decision);
    let ttl_ms = ttl_seconds.unwrap_or(120).clamp(1, 3600) * 1000;
    let issued_at = now_ms();

    with_daemon_state(&app, &state, |daemon| {
        let ticket = PermissionDecisionTicket {
            id: format!("ticket-{}-{}", wallet, issued_at),
            agent_wallet: wallet.clone(),
            action: normalized_action.clone(),
            decision: normalized_decision.clone(),
            issued_at,
            expires_at: issued_at.saturating_add(ttl_ms),
            nonce: now_nonce(),
        };
        daemon.tickets.insert(ticket.id.clone(), ticket.clone());
        Ok(ticket)
    })
}

#[tauri::command]
fn daemon_validate_permission_ticket(
    app: tauri::AppHandle,
    state: tauri::State<'_, LocalDaemonState>,
    ticket_id: String,
    action: String,
) -> Result<bool, String> {
    with_daemon_state(&app, &state, |daemon| {
        daemon
            .tickets
            .retain(|_, value| value.expires_at > now_ms());
        let Some(ticket) = daemon.tickets.get(&ticket_id).cloned() else {
            return Ok(false);
        };
        if ticket.expires_at <= now_ms() {
            daemon.tickets.remove(&ticket_id);
            return Ok(false);
        }
        if ticket.action != action.trim() {
            return Ok(false);
        }
        Ok(ticket.decision == "allow")
    })
}

#[tauri::command]
fn daemon_check_permission(
    app: tauri::AppHandle,
    state: tauri::State<'_, LocalDaemonState>,
    agent_wallet: String,
    permission_key: String,
) -> Result<bool, String> {
    let wallet =
        normalize_wallet(&agent_wallet).ok_or_else(|| "invalid agentWallet".to_string())?;
    let key = permission_key.trim().to_string();
    if key.is_empty() {
        return Err("permissionKey is required".to_string());
    }

    with_daemon_state(&app, &state, |daemon| {
        match check_agent_permission(daemon, &wallet, &key) {
            Ok(()) => Ok(true),
            Err(_) => Ok(false),
        }
    })
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
                "3" => "limited".to_string(),
                _ if stdout.is_empty() => "not-determined".to_string(),
                _ => "unknown".to_string(),
            };
        }
        "unknown".to_string()
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = service;
        "unsupported".to_string()
    }
}

#[tauri::command]
fn daemon_query_os_permissions() -> Result<OsPermissionSnapshot, String> {
    Ok(OsPermissionSnapshot {
        camera: query_tcc_status("kTCCServiceCamera"),
        microphone: query_tcc_status("kTCCServiceMicrophone"),
        screen: query_tcc_status("kTCCServiceScreenCapture"),
        full_disk_access: query_tcc_status("kTCCServiceSystemPolicyAllFiles"),
        accessibility: query_tcc_status("kTCCServiceAccessibility"),
    })
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
fn daemon_request_os_permission(permission_key: String) -> Result<OsPermissionSnapshot, String> {
    #[cfg(target_os = "macos")]
    {
        // For camera and microphone, we can trigger the native TCC prompt via AVFoundation.
        // For other permissions (fullDiskAccess, accessibility, screen), macOS does not allow
        // programmatic requests — we open System Settings instead and re-query.
        match permission_key.as_str() {
            "camera" => {
                // Trigger the TCC prompt by briefly requesting camera access via osascript
                let _ = std::process::Command::new("osascript")
                    .args(["-e", "tell application \"System Events\" to log \"\""])
                    .output();
                // Actually trigger camera TCC: use a swift one-liner via osascript
                let _ = std::process::Command::new("swift")
                    .args([
                        "-e",
                        r#"
                        import AVFoundation
                        import Foundation
                        let sem = DispatchSemaphore(value: 0)
                        AVCaptureDevice.requestAccess(for: .video) { _ in sem.signal() }
                        sem.wait()
                    "#,
                    ])
                    .output();
            }
            "microphone" => {
                let _ = std::process::Command::new("swift")
                    .args([
                        "-e",
                        r#"
                        import AVFoundation
                        import Foundation
                        let sem = DispatchSemaphore(value: 0)
                        AVCaptureDevice.requestAccess(for: .audio) { _ in sem.signal() }
                        sem.wait()
                    "#,
                    ])
                    .output();
            }
            "accessibility" | "fullDiskAccess" | "screen" => {
                // These cannot be requested programmatically — open System Settings
                let _ = daemon_open_system_settings(Some(permission_key));
            }
            _ => {
                let _ = daemon_open_system_settings(Some(permission_key));
            }
        }

        // Re-query and return fresh snapshot
        daemon_query_os_permissions()
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = permission_key;
        daemon_query_os_permissions()
    }
}

#[tauri::command]
fn daemon_install_launch_agent(app: tauri::AppHandle) -> Result<String, String> {
    let home = std::env::var("HOME").map_err(|_| "HOME is not set".to_string())?;
    let launch_agents_dir = Path::new(&home).join("Library").join("LaunchAgents");
    fs::create_dir_all(&launch_agents_dir)
        .map_err(|err| format!("failed to create LaunchAgents directory: {err}"))?;

    let plist_path = launch_agents_dir.join("compose.market.daemon.plist");
    let exe_path = std::env::current_exe()
        .map_err(|err| format!("failed to resolve current executable: {err}"))?;
    let label = "compose.market.daemon";

    let plist = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>{label}</string>
  <key>ProgramArguments</key>
  <array>
    <string>{}</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>ProcessType</key>
  <string>Background</string>
  <key>WorkingDirectory</key>
  <string>{}</string>
  <key>StandardOutPath</key>
  <string>{}</string>
  <key>StandardErrorPath</key>
  <string>{}</string>
</dict>
</plist>
"#,
        exe_path.display(),
        resolve_base_dir(&app)?.display(),
        resolve_base_dir(&app)?.join("daemon.stdout.log").display(),
        resolve_base_dir(&app)?.join("daemon.stderr.log").display(),
    );

    fs::write(&plist_path, plist)
        .map_err(|err| format!("failed to write LaunchAgent plist: {err}"))?;
    Ok(plist_path.to_string_lossy().to_string())
}

#[tauri::command]
fn daemon_launch_agent_status() -> Result<bool, String> {
    let home = std::env::var("HOME").map_err(|_| "HOME is not set".to_string())?;
    let plist_path = Path::new(&home)
        .join("Library")
        .join("LaunchAgents")
        .join("compose.market.daemon.plist");
    Ok(plist_path.exists())
}

#[tauri::command]
fn daemon_runtime_host_status(
    state: tauri::State<'_, LocalRuntimeHostState>,
) -> Result<LocalRuntimeHostStatus, String> {
    runtime_host::current_runtime_host_status(state.inner())
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
    file_name: String,
    latest_alias: String,
    root_cid: String,
    piece_cid: String,
    payload_size: usize,
    copy_count: Option<u64>,
    provider_id: String,
    data_set_id: Option<String>,
    piece_id: Option<String>,
    retrieval_url: Option<String>,
    payer_address: String,
    session_key_expires_at: u64,
    source: String,
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
        kind: "compose.mesh.request.v1".to_string(),
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
        artifact_kind: artifact_kind.map(|value: MeshSharedArtifactKind| value.as_str().to_string()),
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
    let content = request.content.clone().unwrap_or_default().trim().to_string();
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
        kind: "compose.mesh.learning.v1".to_string(),
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
        return "a409: inconsistent agent identity".to_string();
    }
    if body.trim().is_empty() {
        format!("{route} failed: HTTP {status}")
    } else {
        format!("{route} failed: HTTP {status}: {body}")
    }
}

async fn register_hai_via_local_runtime(
    base_url: &str,
    auth_token: &str,
    body: serde_json::Value,
) -> Result<MeshHaiRuntimeRow, String> {
    let client = HttpClient::new();
    let response = client
        .post(format!(
            "{}/mesh/hai/register",
            base_url.trim_end_matches('/')
        ))
        .header(LOCAL_RUNTIME_AUTH_HEADER, auth_token)
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
    auth_token: &str,
    body: serde_json::Value,
) -> Result<MeshStateAnchorRuntimeResponse, String> {
    let client = HttpClient::new();
    let response = client
        .post(format!(
            "{}/mesh/synapse/anchor",
            base_url.trim_end_matches('/')
        ))
        .header(LOCAL_RUNTIME_AUTH_HEADER, auth_token)
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
    auth_token: &str,
    body: serde_json::Value,
) -> Result<MeshSharedArtifactPinRuntimeResponse, String> {
    let client = HttpClient::new();
    let response = client
        .post(format!("{}/mesh/filecoin/pin", base_url.trim_end_matches('/')))
        .header(LOCAL_RUNTIME_AUTH_HEADER, auth_token)
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
    let runtime_status = runtime_host::current_runtime_host_status(runtime_host)?;
    if !runtime_status.running {
        return Err("local runtime host is not running".to_string());
    }
    let auth_token = current_runtime_host_auth_token(runtime_host)?;
    let hai_row = register_hai_via_local_runtime(
        &runtime_status.base_url,
        &auth_token,
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
            file_name: format!(
                "{}.json",
                compose_hai_path(&hai_row.hai_id, last_update_number)
            ),
            latest_alias: "manifest:latest".to_string(),
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
        &auth_token,
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
        compose_hai_path, derive_hai_id, normalize_state_root_hash_for_compare,
        same_state_root_hash,
    };

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
    fn compose_hai_path_uses_plain_hai_value() {
        assert_eq!(compose_hai_path("abc123", 7), "compose-abc123-#7");
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
    let file = mesh_publication_results_dir(app)?.join(format!("{}.json", result.request_id));
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
        return Err("mesh publication request agentWallet does not match the running mesh agent".to_string());
    }

    let ctx = load_mesh_pub_ctx(app, &requested_wallet)?;
    let runtime_status = runtime_host::current_runtime_host_status(runtime_host)?;
    if !runtime_status.running {
        return Err("local runtime host is not running".to_string());
    }
    let auth_token = current_runtime_host_auth_token(runtime_host)?;
    let hai_row = register_hai_via_local_runtime(
        &runtime_status.base_url,
        &auth_token,
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
    let path = knowledge_hai_path(&hai_row.hai_id, artifact_kind.clone(), artifact_number);
    let signed_request_json = build_signed_mesh_request_json(
        &load_or_create_mesh_identity(app)?,
        live_status,
        &requested_wallet,
        &ctx.user_wallet,
        &ctx.device_id,
        &hai_row.hai_id,
        "knowledge.pin.v1",
        "knowledge",
        &path,
        Some(artifact_kind.clone()),
        Some(format!("0x{}", sha256_hex_string(&payload_json))),
    )?;

    let response = pin_mesh_learning_via_local_runtime(
        &runtime_status.base_url,
        &auth_token,
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

                let (mut manifest, anchor_request) =
                    build_current_mesh_publication(app, &requested_wallet, &live_status)?;
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

                let published =
                    publish_mesh_manifest_from_command(app, mesh_state.inner(), manifest).await?;
                persist_manifest_update(app, &published)?;
                let _ = app.emit("mesh-manifest-updated", &published);

                Ok(MeshPublicationQueueResult {
                    request_id: request.request_id.clone(),
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
        Err(error) => MeshPublicationQueueResult {
            request_id: request.request_id,
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
        },
    }
}

async fn process_pending_mesh_publication_requests(app: &tauri::AppHandle) -> Result<(), String> {
    let requests_dir = mesh_publication_requests_dir(app)?;
    let mut files = fs::read_dir(&requests_dir)
        .map_err(|err| format!("failed to read mesh publication request dir: {err}"))?
        .filter_map(|entry| entry.ok().map(|value| value.path()))
        .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("json"))
        .collect::<Vec<_>>();
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

#[tauri::command]
async fn local_mesh_anchor_state(
    app: tauri::AppHandle,
    mesh_state: tauri::State<'_, MeshRuntimeState>,
    runtime_host: tauri::State<'_, LocalRuntimeHostState>,
    request: MeshStateAnchorCommandRequest,
) -> Result<MeshStateAnchorRuntimeResponse, String> {
    anchor_mesh_state_from_command(&app, mesh_state.inner(), runtime_host.inner(), request).await
}

#[tauri::command]
async fn local_mesh_publish_manifest(
    app: tauri::AppHandle,
    state: tauri::State<'_, MeshRuntimeState>,
    manifest: MeshManifest,
) -> Result<MeshManifest, String> {
    publish_mesh_manifest_from_command(&app, state.inner(), manifest).await
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

fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_shell::init())
        .plugin(tauri_plugin_deep_link::init())
        .plugin(tauri_plugin_updater::Builder::new().build())
        .manage(PendingDeepLinks::default())
        .manage(MeshRuntimeState::default())
        .manage(LocalDaemonState::default())
        .manage(LocalRuntimeHostState::default())
        .invoke_handler(tauri::generate_handler![
            get_local_paths,
            set_local_base_dir,
            load_local_state,
            save_local_state,
            ensure_local_dir,
            write_local_file,
            read_local_file,
            remove_local_path,
            check_missing_binaries,
            consume_pending_deep_links,
            local_network_status,
            local_network_join,
            local_network_leave,
            local_mesh_anchor_state,
            local_mesh_publish_manifest,
            daemon_install_agent,
            daemon_remove_agent,
            daemon_update_permissions,
            daemon_update_skill,
            daemon_get_agent_status,
            daemon_tail_logs,
            daemon_issue_permission_ticket,
            daemon_validate_permission_ticket,
            daemon_check_permission,
            daemon_query_os_permissions,
            daemon_open_system_settings,
            daemon_request_os_permission,
            daemon_install_launch_agent,
            daemon_launch_agent_status,
            daemon_runtime_host_status,
            local_check_for_updates,
            local_install_update
        ])
        .setup(|app| {
            let mut daemon_disk_state =
                read_daemon_state_from_disk(&app.handle()).unwrap_or_default();
            activate_installed_agents(&mut daemon_disk_state);
            let daemon_state = app.state::<LocalDaemonState>();
            let runtime_host_state = app.state::<LocalRuntimeHostState>();

            if let Ok(mut guard) = daemon_state.state.lock() {
                *guard = daemon_disk_state;
            }
            if let Ok(snapshot) = daemon_state_snapshot(&daemon_state) {
                match runtime_host::reconcile_local_runtime_host(
                    &app.handle(),
                    runtime_host_state.inner(),
                    &snapshot,
                ) {
                    Ok(host_status) => {
                        if let Ok(mut guard) = daemon_state.state.lock() {
                            runtime_host::apply_runtime_host_status(&mut guard, &host_status);
                            let _ = write_daemon_state_to_disk(&app.handle(), &guard);
                        }
                    }
                    Err(error) => {
                        eprintln!("[daemon] failed to reconcile local runtime host: {}", error);
                        if let Ok(host_status) =
                            runtime_host::current_runtime_host_status(runtime_host_state.inner())
                        {
                            if let Ok(mut guard) = daemon_state.state.lock() {
                                runtime_host::apply_runtime_host_status(&mut guard, &host_status);
                                let _ = write_daemon_state_to_disk(&app.handle(), &guard);
                            }
                        }
                    }
                }
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

                // Register the protocol on macOS
                #[cfg(target_os = "macos")]
                {
                    if let Err(e) = app.deep_link().register("manowar") {
                        eprintln!("[DeepLink] Failed to register 'manowar' scheme: {}", e);
                    } else {
                        println!("[DeepLink] Registered 'manowar' scheme");
                    }
                }
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
