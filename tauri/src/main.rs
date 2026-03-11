#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use futures::StreamExt;
use libp2p::{
    autonat,
    connection_limits,
    dcutr,
    gossipsub::{self, IdentTopic, MessageAuthenticity, ValidationMode},
    identify,
    identity::{self, Keypair},
    kad,
    relay, rendezvous,
    multiaddr::Protocol,
    noise, ping, SwarmBuilder,
    swarm::{NetworkBehaviour, Swarm, SwarmEvent},
    tcp, yamux, Multiaddr, PeerId, StreamProtocol,
};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::str::FromStr;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tauri::{Emitter, Manager};
use tauri_plugin_updater::UpdaterExt;
use tokio::sync::oneshot;

#[derive(Debug, serde::Serialize)]
struct DesktopPaths {
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
            shell: "ask".to_string(),
            filesystem_read: "ask".to_string(),
            filesystem_write: "ask".to_string(),
            filesystem_edit: "ask".to_string(),
            filesystem_delete: "deny".to_string(),
            camera: "ask".to_string(),
            microphone: "ask".to_string(),
            network: "allow".to_string(),
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
    mesh_enabled: bool,
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
struct DesktopDaemonState {
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
struct MeshJoinRequest {
    user_address: String,
    agent_wallet: String,
    device_id: String,
    chain_id: u32,
    gossip_topic: String,
    #[serde(default = "default_announce_topic")]
    announce_topic: String,
    #[serde(default = "default_mesh_heartbeat_ms")]
    heartbeat_ms: u64,
    #[serde(default = "default_kad_protocol")]
    kad_protocol: String,
    #[serde(default)]
    capabilities: Vec<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    dna_hash: Option<String>,
    #[serde(default)]
    capabilities_hash: Option<String>,
    #[serde(default)]
    public_card: Option<MeshAgentCard>,
    #[serde(default)]
    bootstrap_multiaddrs: Vec<String>,
    #[serde(default)]
    relay_multiaddrs: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct MeshRuntimeStatus {
    running: bool,
    status: String,
    user_address: Option<String>,
    agent_wallet: Option<String>,
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
struct DesktopUpdateCheckResult {
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
            agent_wallet: None,
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

fn now_ms() -> u64 {
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

fn default_kad_protocol() -> String {
    "/compose-market/desktop/kad/1.0.0".to_string()
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
        updated_at: if card.updated_at == 0 { now_ms() } else { card.updated_at },
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

fn default_capabilities(agent_wallet: &str) -> Vec<String> {
    let wallet_suffix = agent_wallet.trim_start_matches("0x");
    vec![format!("agent-{wallet_suffix}")]
}

fn validate_mesh_join_request(request: &MeshJoinRequest) -> Result<MeshJoinRequest, String> {
    let user_address = normalize_wallet(&request.user_address)
        .ok_or_else(|| "userAddress must be a valid wallet address".to_string())?;
    let agent_wallet = normalize_wallet(&request.agent_wallet)
        .ok_or_else(|| "agentWallet must be a valid wallet address".to_string())?;
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
    if request.heartbeat_ms < 1_000 || request.heartbeat_ms > 300_000 {
        return Err("heartbeatMs must be between 1000 and 300000".to_string());
    }
    if request.kad_protocol.trim().is_empty() {
        return Err("kadProtocol is required".to_string());
    }

    let capabilities = {
        let normalized = request
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

    Ok(MeshJoinRequest {
        user_address,
        agent_wallet,
        device_id,
        chain_id: request.chain_id,
        gossip_topic: request.gossip_topic.trim().to_string(),
        announce_topic: request.announce_topic.trim().to_string(),
        heartbeat_ms: request.heartbeat_ms,
        kad_protocol: request.kad_protocol.trim().to_string(),
        capabilities,
        session_id: request.session_id.clone(),
        dna_hash: request.dna_hash.clone(),
        capabilities_hash: request.capabilities_hash.clone(),
        public_card: sanitize_mesh_agent_card(request.public_card.clone()),
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
    })
}

fn resolve_base_dir(app: &tauri::AppHandle) -> Result<PathBuf, String> {
    let base = app
        .path()
        .app_data_dir()
        .map_err(|err| format!("failed to resolve app data directory: {err}"))?
        .join("runtime");
    fs::create_dir_all(&base).map_err(|err| format!("failed to create app data directory: {err}"))?;
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

    let raw = fs::read_to_string(&file).map_err(|err| format!("failed to read daemon state: {err}"))?;
    serde_json::from_str(&raw).map_err(|err| format!("failed to parse daemon state: {err}"))
}

fn write_daemon_state_to_disk(app: &tauri::AppHandle, state: &DaemonStateFile) -> Result<(), String> {
    let file = daemon_state_path(app)?;
    if let Some(parent) = file.parent() {
        fs::create_dir_all(parent).map_err(|err| format!("failed to create daemon state dir: {err}"))?;
    }
    let serialized = serde_json::to_string_pretty(state)
        .map_err(|err| format!("failed to serialize daemon state: {err}"))?;
    fs::write(&file, serialized).map_err(|err| format!("failed to persist daemon state: {err}"))?;
    Ok(())
}

fn normalize_daemon_decision(value: &str) -> String {
    match value.trim().to_lowercase().as_str() {
        "allow" => "allow".to_string(),
        "ask" => "ask".to_string(),
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
        let bytes = fs::read(&path).map_err(|err| format!("failed to read mesh identity file: {err}"))?;
        return identity::Keypair::from_protobuf_encoding(&bytes)
            .map_err(|err| format!("failed to decode mesh identity: {err}"));
    }

    let keypair = identity::Keypair::generate_ed25519();
    let encoded = keypair
        .to_protobuf_encoding()
        .map_err(|err| format!("failed to encode mesh identity: {err}"))?;
    fs::write(&path, encoded).map_err(|err| format!("failed to write mesh identity file: {err}"))?;
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

fn with_mesh_status<T>(app: &tauri::AppHandle, updater: impl FnOnce(&mut MeshRuntimeStatus) -> T) -> Option<T> {
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
        status.agent_wallet = Some(request.agent_wallet.clone());
        status.device_id = Some(request.device_id.clone());
        status.updated_at = now_ms();
        if status_value == "dormant" {
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
            status.agent_wallet = Some(req.agent_wallet.clone());
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
        agent_wallet: request.agent_wallet.clone(),
        device_id: request.device_id.clone(),
        session_id: request.session_id.clone().unwrap_or_default(),
        dna_hash: request.dna_hash.clone().unwrap_or_default(),
        capabilities_hash: request
            .capabilities_hash
            .clone()
            .unwrap_or(computed_caps_hash),
        card: request.public_card.clone(),
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
    serde_cbor::to_vec(&envelope).map_err(|err| format!("failed to encode signed CBOR envelope: {err}"))
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

fn extract_bootstrap_and_rendezvous_peers(request: &MeshJoinRequest) -> (Vec<Multiaddr>, HashSet<PeerId>) {
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
            eprintln!("[mesh] kad start_providing failed for capability '{}': {}", capability, err);
        }
        let namespace = match capability_namespace(capability) {
            Ok(value) => value,
            Err(err) => {
                eprintln!("[mesh] {}", err);
                continue;
            }
        };
        for peer in rendezvous_peers {
            if let Err(err) = swarm
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

fn apply_kad_mode_from_autonat(
    swarm: &mut Swarm<MeshBehaviour>,
    nat_status: &autonat::NatStatus,
) {
    match nat_status {
        autonat::NatStatus::Public(_) => swarm.behaviour_mut().kad.set_mode(Some(kad::Mode::Server)),
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
    let _ = app.emit("mesh-peer-index", serde_json::json!({ "peers": peers, "updatedAt": now_ms() }));
}

fn build_mesh_swarm(
    local_key: identity::Keypair,
    request: &MeshJoinRequest,
) -> Result<(Swarm<MeshBehaviour>, IdentTopic, IdentTopic, HashSet<PeerId>), String> {
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
    gossipsub
        .subscribe(&global_topic)
        .map_err(|err| format!("failed to subscribe gossipsub global topic: {err}"))?;
    gossipsub
        .subscribe(&announce_topic)
        .map_err(|err| format!("failed to subscribe gossipsub announce topic: {err}"))?;

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
                "/compose-market/desktop/1.0.0".to_string(),
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

    Ok((swarm, global_topic, announce_topic, rendezvous_peers))
}

async fn run_mesh_loop(app: tauri::AppHandle, request: MeshJoinRequest, mut stop_rx: oneshot::Receiver<()>) {
    let local_key = match load_or_create_mesh_identity(&app) {
        Ok(value) => value,
        Err(err) => {
            mesh_error(&app, Some(&request), err);
            return;
        }
    };
    let local_peer_id = PeerId::from(local_key.public()).to_string();

    let (mut swarm, global_topic, announce_topic, rendezvous_peers) =
        match build_mesh_swarm(local_key.clone(), &request) {
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
    let mut kad_refresh_interval = tokio::time::interval(Duration::from_millis(KAD_REFRESH_INTERVAL_MS));
    let mut peer_prune_interval = tokio::time::interval(Duration::from_secs(30));
    let mut rendezvous_discovery_interval =
        tokio::time::interval(Duration::from_millis(RENDEZVOUS_DISCOVERY_INTERVAL_MS));

    let _ = with_mesh_status(&app, |status| {
        status.running = true;
        status.status = "connecting".to_string();
        status.user_address = Some(request.user_address.clone());
        status.agent_wallet = Some(request.agent_wallet.clone());
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
                let presence_payload = build_signed_envelope_payload(
                    &local_key,
                    &request,
                    "presence",
                    &local_peer_id,
                    &request.capabilities,
                    &listen_multiaddrs,
                    next_nonce(&mut nonce_counter, &local_peer_id),
                );
                let announce_payload = build_signed_envelope_payload(
                    &local_key,
                    &request,
                    "announce",
                    &local_peer_id,
                    &request.capabilities,
                    &listen_multiaddrs,
                    next_nonce(&mut nonce_counter, &local_peer_id),
                );

                let publish_presence = presence_payload
                    .and_then(|payload| swarm
                        .behaviour_mut()
                        .gossipsub
                        .publish(global_topic.clone(), payload)
                        .map_err(|err| format!("presence publish failed: {err}")));
                let publish_announce = announce_payload
                    .and_then(|payload| swarm
                        .behaviour_mut()
                        .gossipsub
                        .publish(announce_topic.clone(), payload)
                        .map_err(|err| format!("announce publish failed: {err}")));

                let _ = with_mesh_status(&app, |status| {
                    status.last_heartbeat_at = Some(now_ms());
                    status.updated_at = now_ms();
                    match (publish_presence, publish_announce) {
                        (Err(err), _) | (_, Err(err)) => {
                            status.last_error = Some(err);
                            if status.peers_discovered == 0 {
                                status.status = "connecting".to_string();
                            }
                        }
                        _ => {
                            if status.peers_discovered > 0 || !connected_peers.is_empty() {
                                status.status = "online".to_string();
                            }
                        }
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
                            match decode_and_validate_envelope(&message.data, &mut seen_nonces) {
                                Ok(envelope) => {
                                    let entry = peer_cache.entry(envelope.peer_id.clone()).or_insert(PeerCacheEntry {
                                        last_seen_ms: envelope.ts_ms,
                                        stale: false,
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
}

#[tauri::command]
fn get_desktop_paths(app: tauri::AppHandle) -> Result<DesktopPaths, String> {
    let base_dir = resolve_base_dir(&app)?;
    let state_file = base_dir.join("state.json");
    let agents_dir = base_dir.join("agents");
    let skills_dir = base_dir.join("skills");

    fs::create_dir_all(&agents_dir).map_err(|err| format!("failed to create agents directory: {err}"))?;
    fs::create_dir_all(&skills_dir).map_err(|err| format!("failed to create skills directory: {err}"))?;

    Ok(DesktopPaths {
        base_dir: base_dir.to_string_lossy().to_string(),
        state_file: state_file.to_string_lossy().to_string(),
        agents_dir: agents_dir.to_string_lossy().to_string(),
        skills_dir: skills_dir.to_string_lossy().to_string(),
    })
}

#[tauri::command]
fn load_desktop_state(app: tauri::AppHandle) -> Result<String, String> {
    let base_dir = resolve_base_dir(&app)?;
    let state_file = base_dir.join("state.json");

    if !state_file.exists() {
        return Ok("{}".to_string());
    }

    fs::read_to_string(&state_file).map_err(|err| format!("failed to read state file: {err}"))
}

#[tauri::command]
fn save_desktop_state(app: tauri::AppHandle, state_json: String) -> Result<(), String> {
    let base_dir = resolve_base_dir(&app)?;
    let state_file = base_dir.join("state.json");

    if let Some(parent) = state_file.parent() {
        fs::create_dir_all(parent).map_err(|err| format!("failed to create state parent directory: {err}"))?;
    }
    fs::write(&state_file, state_json).map_err(|err| format!("failed to write state file: {err}"))?;
    Ok(())
}

#[tauri::command]
fn ensure_desktop_dir(app: tauri::AppHandle, relative_path: String) -> Result<String, String> {
    let dir = resolve_managed_path(&app, &relative_path)?;
    fs::create_dir_all(&dir).map_err(|err| format!("failed to create directory: {err}"))?;
    Ok(dir.to_string_lossy().to_string())
}

#[tauri::command]
fn write_desktop_file(
    app: tauri::AppHandle,
    relative_path: String,
    content: String,
) -> Result<String, String> {
    let file_path = resolve_managed_path(&app, &relative_path)?;
    if let Some(parent) = file_path.parent() {
        fs::create_dir_all(parent).map_err(|err| format!("failed to create parent directory: {err}"))?;
    }
    fs::write(&file_path, content).map_err(|err| format!("failed to write file: {err}"))?;
    Ok(file_path.to_string_lossy().to_string())
}

#[tauri::command]
fn read_desktop_file(app: tauri::AppHandle, relative_path: String) -> Result<String, String> {
    let file_path = resolve_managed_path(&app, &relative_path)?;
    if !file_path.exists() {
        return Err("file not found".to_string());
    }
    fs::read_to_string(&file_path).map_err(|err| format!("failed to read file: {err}"))
}

#[tauri::command]
fn remove_desktop_path(app: tauri::AppHandle, relative_path: String) -> Result<bool, String> {
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

fn daemon_agent_workspace_path(app: &tauri::AppHandle, agent_wallet: &str) -> Result<PathBuf, String> {
    resolve_managed_path(app, &daemon_agent_workspace_relative(agent_wallet))
}

fn daemon_agent_logs_path(app: &tauri::AppHandle, agent_wallet: &str) -> Result<PathBuf, String> {
    Ok(daemon_agent_workspace_path(app, agent_wallet)?.join("runtime.log"))
}

fn bootstrap_agent_workspace(app: &tauri::AppHandle, payload: &DaemonInstallPayload) -> Result<(), String> {
    let workspace = daemon_agent_workspace_path(app, &payload.agent_wallet)?;
    fs::create_dir_all(&workspace).map_err(|err| format!("failed to create agent workspace: {err}"))?;
    fs::create_dir_all(workspace.join("skills")).map_err(|err| format!("failed to create skills dir: {err}"))?;
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
            fs::write(&file, content).map_err(|err| format!("failed to write bootstrap file: {err}"))?;
        }
    }

    Ok(())
}

fn with_daemon_state<T>(
    app: &tauri::AppHandle,
    state: &tauri::State<'_, DesktopDaemonState>,
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

#[tauri::command]
fn daemon_install_agent(
    app: tauri::AppHandle,
    state: tauri::State<'_, DesktopDaemonState>,
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
        let entry = daemon.agents.entry(normalized_wallet.clone()).or_insert(DaemonAgentState {
            agent_wallet: normalized_wallet.clone(),
            runtime_id: None,
            desired_running: false,
            running: false,
            mesh_enabled: false,
            status: "stopped".to_string(),
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
        entry.updated_at = now_ms();

        Ok(entry.clone())
    })
}

#[tauri::command]
fn daemon_start_agent(
    app: tauri::AppHandle,
    state: tauri::State<'_, DesktopDaemonState>,
    agent_wallet: String,
) -> Result<DaemonAgentState, String> {
    let wallet = normalize_wallet(&agent_wallet).ok_or_else(|| "invalid agentWallet".to_string())?;

    with_daemon_state(&app, &state, |daemon| {
        let entry = daemon
            .agents
            .get_mut(&wallet)
            .ok_or_else(|| format!("agent not installed: {wallet}"))?;

        entry.desired_running = true;
        entry.running = true;
        entry.status = "running".to_string();
        entry.runtime_id = Some(format!("openclaw-{}-{}", wallet, now_ms()));
        entry.last_error = None;
        entry.updated_at = now_ms();
        Ok(entry.clone())
    })
}

#[tauri::command]
fn daemon_stop_agent(
    app: tauri::AppHandle,
    state: tauri::State<'_, DesktopDaemonState>,
    agent_wallet: String,
) -> Result<DaemonAgentState, String> {
    let wallet = normalize_wallet(&agent_wallet).ok_or_else(|| "invalid agentWallet".to_string())?;

    with_daemon_state(&app, &state, |daemon| {
        let entry = daemon
            .agents
            .get_mut(&wallet)
            .ok_or_else(|| format!("agent not installed: {wallet}"))?;

        entry.desired_running = false;
        entry.running = false;
        entry.status = "stopped".to_string();
        entry.updated_at = now_ms();
        Ok(entry.clone())
    })
}

#[tauri::command]
fn daemon_update_permissions(
    app: tauri::AppHandle,
    state: tauri::State<'_, DesktopDaemonState>,
    agent_wallet: String,
    policy: DaemonPermissionPolicy,
) -> Result<DaemonAgentState, String> {
    let wallet = normalize_wallet(&agent_wallet).ok_or_else(|| "invalid agentWallet".to_string())?;
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
    state: tauri::State<'_, DesktopDaemonState>,
    agent_wallet: String,
    skill_key: String,
    enabled: bool,
) -> Result<DaemonAgentState, String> {
    let wallet = normalize_wallet(&agent_wallet).ok_or_else(|| "invalid agentWallet".to_string())?;
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
    state: tauri::State<'_, DesktopDaemonState>,
    agent_wallet: String,
) -> Result<Option<DaemonAgentState>, String> {
    let wallet = normalize_wallet(&agent_wallet).ok_or_else(|| "invalid agentWallet".to_string())?;
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
    let wallet = normalize_wallet(&agent_wallet).ok_or_else(|| "invalid agentWallet".to_string())?;
    let logs_path = daemon_agent_logs_path(&app, &wallet)?;
    if !logs_path.exists() {
        return Ok(DaemonLogTail {
            lines: Vec::new(),
            cursor: cursor.unwrap_or(0),
        });
    }

    let raw = fs::read_to_string(&logs_path).map_err(|err| format!("failed to read logs: {err}"))?;
    let all_lines = raw
        .lines()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();
    let from = cursor.unwrap_or(0).min(all_lines.len());
    let slice = all_lines[from..].to_vec();

    Ok(DaemonLogTail {
        lines: slice,
        cursor: all_lines.len(),
    })
}

#[tauri::command]
fn daemon_mesh_set(
    app: tauri::AppHandle,
    state: tauri::State<'_, DesktopDaemonState>,
    agent_wallet: String,
    enabled: bool,
) -> Result<DaemonAgentState, String> {
    let wallet = normalize_wallet(&agent_wallet).ok_or_else(|| "invalid agentWallet".to_string())?;

    with_daemon_state(&app, &state, |daemon| {
        let entry = daemon
            .agents
            .get_mut(&wallet)
            .ok_or_else(|| format!("agent not installed: {wallet}"))?;
        entry.mesh_enabled = enabled;
        entry.updated_at = now_ms();
        Ok(entry.clone())
    })
}

#[tauri::command]
fn daemon_issue_permission_ticket(
    app: tauri::AppHandle,
    state: tauri::State<'_, DesktopDaemonState>,
    agent_wallet: String,
    action: String,
    decision: String,
    ttl_seconds: Option<u64>,
) -> Result<PermissionDecisionTicket, String> {
    let wallet = normalize_wallet(&agent_wallet).ok_or_else(|| "invalid agentWallet".to_string())?;
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
    state: tauri::State<'_, DesktopDaemonState>,
    ticket_id: String,
    action: String,
) -> Result<bool, String> {
    with_daemon_state(&app, &state, |daemon| {
        daemon.tickets.retain(|_, value| value.expires_at > now_ms());
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
fn daemon_install_launch_agent(app: tauri::AppHandle) -> Result<String, String> {
    let home = std::env::var("HOME").map_err(|_| "HOME is not set".to_string())?;
    let launch_agents_dir = Path::new(&home).join("Library").join("LaunchAgents");
    fs::create_dir_all(&launch_agents_dir)
        .map_err(|err| format!("failed to create LaunchAgents directory: {err}"))?;

    let plist_path = launch_agents_dir.join("compose.market.daemon.plist");
    let exe_path = std::env::current_exe().map_err(|err| format!("failed to resolve current executable: {err}"))?;
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

    fs::write(&plist_path, plist).map_err(|err| format!("failed to write LaunchAgent plist: {err}"))?;
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
fn desktop_network_status(state: tauri::State<MeshRuntimeState>) -> Result<MeshRuntimeStatus, String> {
    let status = state
        .status
        .lock()
        .map_err(|_| "failed to read mesh runtime status".to_string())?;
    Ok(status.clone())
}

#[tauri::command]
async fn desktop_network_join(
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
    {
        let mut stop_guard = state
            .stop_tx
            .lock()
            .map_err(|_| "failed to update mesh stop channel".to_string())?;
        *stop_guard = Some(stop_tx);
    }

    let app_handle = app.clone();
    tauri::async_runtime::spawn(async move {
        run_mesh_loop(app_handle, request, stop_rx).await;
    });

    desktop_network_status(state)
}

#[tauri::command]
async fn desktop_network_leave(
    app: tauri::AppHandle,
    state: tauri::State<'_, MeshRuntimeState>,
) -> Result<MeshRuntimeStatus, String> {
    if let Ok(mut stop_guard) = state.stop_tx.lock() {
        if let Some(stop_tx) = stop_guard.take() {
            let _ = stop_tx.send(());
        }
    }

    let _ = with_mesh_status(&app, |status| {
        *status = MeshRuntimeStatus::default();
    });

    desktop_network_status(state)
}

fn normalize_api_base(api_url: &str) -> Result<String, String> {
    let trimmed = api_url.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return Err("Desktop updater apiUrl is required".to_string());
    }
    if !trimmed.starts_with("https://") && !trimmed.starts_with("http://") {
        return Err("Desktop updater apiUrl must start with http:// or https://".to_string());
    }
    Ok(trimmed.to_string())
}

fn normalize_updater_pubkey(pubkey: &str) -> Result<String, String> {
    let normalized = pubkey.trim();
    if normalized.is_empty() {
        return Err("Desktop updater public key is required".to_string());
    }
    Ok(normalized.to_string())
}

fn build_desktop_update_endpoint(api_url: &str) -> Result<String, String> {
    let api_base = normalize_api_base(api_url)?;
    Ok(format!(
        "{api_base}/api/desktop/updates/{{{{target}}}}/{{{{arch}}}}/{{{{current_version}}}}"
    ))
}

fn build_desktop_updater(
    app: &tauri::AppHandle,
    api_url: &str,
    pubkey: &str,
) -> Result<tauri_plugin_updater::Updater, String> {
    let endpoint = build_desktop_update_endpoint(api_url)?;
    let pubkey = normalize_updater_pubkey(pubkey)?;
    let endpoint = endpoint
        .parse()
        .map_err(|error| format!("Invalid desktop updater endpoint: {error}"))?;

    app.updater_builder()
        .pubkey(pubkey)
        .endpoints(vec![endpoint])
        .map_err(|error| format!("Failed to configure desktop updater endpoints: {error}"))?
        .build()
        .map_err(|error| format!("Failed to initialize desktop updater: {error}"))
}

#[tauri::command]
async fn desktop_check_for_updates(
    app: tauri::AppHandle,
    api_url: String,
    pubkey: String,
) -> Result<DesktopUpdateCheckResult, String> {
    let updater = build_desktop_updater(&app, &api_url, &pubkey)?;
    let current_version = app.package_info().version.to_string();
    let update = updater
        .check()
        .await
        .map_err(|error| format!("Failed to check for desktop updates: {error}"))?;

    Ok(match update {
        Some(update) => DesktopUpdateCheckResult {
            enabled: true,
            available: true,
            current_version: Some(update.current_version),
            version: Some(update.version),
            body: update.body,
            date: update.date.map(|value| value.to_string()),
        },
        None => DesktopUpdateCheckResult {
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
async fn desktop_install_update(
    app: tauri::AppHandle,
    api_url: String,
    pubkey: String,
) -> Result<(), String> {
    let updater = build_desktop_updater(&app, &api_url, &pubkey)?;
    let update = updater
        .check()
        .await
        .map_err(|error| format!("Failed to check for desktop updates: {error}"))?;
    let Some(update) = update else {
        return Err("Compose Desktop is already on the latest version".to_string());
    };

    update
        .download_and_install(|_, _| {}, || {})
        .await
        .map_err(|error| format!("Failed to install desktop update: {error}"))?;

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
        .manage(DesktopDaemonState::default())
        .invoke_handler(tauri::generate_handler![
            get_desktop_paths,
            load_desktop_state,
            save_desktop_state,
            ensure_desktop_dir,
            write_desktop_file,
            read_desktop_file,
            remove_desktop_path,
            check_missing_binaries,
            consume_pending_deep_links,
            desktop_network_status,
            desktop_network_join,
            desktop_network_leave,
            daemon_install_agent,
            daemon_start_agent,
            daemon_stop_agent,
            daemon_update_permissions,
            daemon_update_skill,
            daemon_get_agent_status,
            daemon_tail_logs,
            daemon_mesh_set,
            daemon_issue_permission_ticket,
            daemon_validate_permission_ticket,
            daemon_install_launch_agent,
            daemon_launch_agent_status,
            desktop_check_for_updates,
            desktop_install_update
        ])
        .setup(|app| {
            let daemon_disk_state = read_daemon_state_from_disk(&app.handle()).unwrap_or_default();
            if let Ok(mut guard) = app.state::<DesktopDaemonState>().state.lock() {
                *guard = daemon_disk_state;
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
        .run(tauri::generate_context!())
        .expect("failed to run compose desktop");
}
