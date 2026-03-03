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
use tokio::sync::oneshot;

#[derive(Debug, serde::Serialize)]
struct DesktopPaths {
    base_dir: String,
    state_file: String,
    agents_dir: String,
    skills_dir: String,
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
    ts_ms: u64,
    nonce: String,
}

#[derive(Debug, Clone)]
struct PeerCacheEntry {
    last_seen_ms: u64,
    stale: bool,
    caps: Vec<String>,
    listen_addrs: Vec<String>,
}

fn unsigned_envelope_bytes(value: &MeshEnvelopeUnsigned) -> Result<Vec<u8>, String> {
    serde_cbor::to_vec(&(
        value.v,
        &value.msg_type,
        &value.peer_id,
        &value.caps,
        &value.listen_addrs,
        value.ts_ms,
        &value.nonce,
    ))
    .map_err(|err| format!("failed to encode unsigned CBOR envelope: {err}"))
}

fn build_signed_envelope_payload(
    local_key: &identity::Keypair,
    msg_type: &str,
    peer_id: &str,
    caps: &[String],
    listen_multiaddrs: &[String],
    nonce: String,
) -> Result<Vec<u8>, String> {
    let unsigned = MeshEnvelopeUnsigned {
        v: ENVELOPE_VERSION,
        msg_type: msg_type.to_string(),
        peer_id: peer_id.to_string(),
        caps: caps.to_vec(),
        listen_addrs: listen_multiaddrs.to_vec(),
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
            serde_json::json!({
                "peerId": peer_id,
                "lastSeenAt": entry.last_seen_ms,
                "stale": entry.stale,
                "caps": entry.caps,
                "listenMultiaddrs": entry.listen_addrs,
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
                    "presence",
                    &local_peer_id,
                    &request.capabilities,
                    &listen_multiaddrs,
                    next_nonce(&mut nonce_counter, &local_peer_id),
                );
                let announce_payload = build_signed_envelope_payload(
                    &local_key,
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
                                        caps: envelope.caps.clone(),
                                        listen_addrs: envelope.listen_addrs.clone(),
                                    });
                                    entry.last_seen_ms = envelope.ts_ms;
                                    entry.stale = false;
                                    entry.caps = envelope.caps;
                                    entry.listen_addrs = envelope.listen_addrs.clone();

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

#[cfg(desktop)]
use tauri_plugin_deep_link::DeepLinkExt;

fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_shell::init())
        .plugin(tauri_plugin_deep_link::init())
        .manage(PendingDeepLinks::default())
        .manage(MeshRuntimeState::default())
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
            desktop_network_leave
        ])
        .setup(|app| {
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
