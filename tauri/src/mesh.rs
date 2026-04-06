use super::*;
use crate::manifest::*;

pub(crate) const MESH_IDENTIFY_PROTOCOL: &str =
    concat!("/", env!("COMPOSE_MESH_PROTOCOL_NAMESPACE"));
pub(crate) const MESH_KAD_PROTOCOL: &str =
    concat!("/", env!("COMPOSE_MESH_PROTOCOL_NAMESPACE"), "/kad");
#[derive(Default)]
pub(crate) struct MeshRuntimeState {
    pub(crate) status: Mutex<MeshRuntimeStatus>,
    pub(crate) stop_tx: Mutex<Option<oneshot::Sender<()>>>,
    pub(crate) command_tx: Mutex<Option<mpsc::UnboundedSender<MeshLoopCommand>>>,
    pub(crate) run_generation: Mutex<u64>,
    pub(crate) active_request: Mutex<Option<MeshJoinRequest>>,
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MeshAgentCard {
    pub(crate) name: String,
    pub(crate) description: String,
    pub(crate) model: String,
    pub(crate) framework: String,
    pub(crate) headline: String,
    pub(crate) status_line: String,
    pub(crate) capabilities: Vec<String>,
    pub(crate) updated_at: u64,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct PersistedLocalState {
    pub(crate) settings: PersistedLocalSettings,
    pub(crate) identity: Option<PersistedLocalIdentity>,
    pub(crate) installed_agents: Vec<PersistedInstalledAgent>,
    pub(crate) installed_skills: Vec<PersistedInstalledSkill>,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct PersistedLocalSettings {
    pub(crate) api_url: String,
    pub(crate) mesh_enabled: bool,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct PersistedLocalIdentity {
    pub(crate) user_address: String,
    pub(crate) compose_key_token: String,
    pub(crate) session_id: String,
    pub(crate) budget: String,
    pub(crate) budget_used: String,
    pub(crate) duration: u64,
    pub(crate) chain_id: u32,
    pub(crate) expires_at: u64,
    pub(crate) device_id: String,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct ActiveSessionRefreshResponse {
    pub(crate) has_session: bool,
    pub(crate) key_id: String,
    pub(crate) token: String,
    pub(crate) budget_remaining: String,
    pub(crate) budget_used: String,
    pub(crate) expires_at: u64,
    pub(crate) chain_id: u32,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct PersistedInstalledSkill {
    pub(crate) id: String,
    pub(crate) enabled: bool,
    pub(crate) relative_path: String,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct PersistedInstalledAgent {
    pub(crate) agent_wallet: String,
    pub(crate) metadata: PersistedAgentMetadata,
    pub(crate) lock: PersistedAgentLock,
    pub(crate) network: PersistedAgentNetworkState,
    pub(crate) heartbeat: PersistedAgentHeartbeatState,
    #[serde(default)]
    pub(crate) desired_permissions: DaemonPermissionPolicy,
    #[serde(default)]
    pub(crate) permissions: DaemonPermissionPolicy,
    pub(crate) mcp_servers: Vec<String>,
    pub(crate) skill_states: HashMap<String, PersistedAgentSkillState>,
}

pub(crate) fn default_agent_heartbeat_enabled() -> bool {
    true
}

pub(crate) fn default_agent_heartbeat_interval_ms() -> u64 {
    30_000
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct PersistedAgentHeartbeatState {
    #[serde(default = "default_agent_heartbeat_enabled")]
    pub(crate) enabled: bool,
    #[serde(default = "default_agent_heartbeat_interval_ms")]
    pub(crate) interval_ms: u64,
    pub(crate) last_run_at: Option<u64>,
    pub(crate) last_result: Option<String>,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct PersistedAgentMetadata {
    pub(crate) name: String,
    pub(crate) description: String,
    pub(crate) agent_card_uri: String,
    pub(crate) model: String,
    pub(crate) framework: String,
    pub(crate) plugins: Vec<serde_json::Value>,
    pub(crate) endpoints: PersistedAgentEndpoints,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct PersistedAgentEndpoints {
    pub(crate) chat: String,
    pub(crate) stream: String,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct PersistedAgentLock {
    pub(crate) agent_wallet: String,
    pub(crate) agent_card_cid: String,
    pub(crate) model_id: String,
    pub(crate) mcp_tools_hash: String,
    pub(crate) chain_id: u32,
    pub(crate) dna_hash: String,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct PersistedAgentNetworkState {
    pub(crate) enabled: bool,
    pub(crate) public_card: Option<MeshAgentCard>,
    pub(crate) manifest: Option<MeshManifest>,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct PersistedAgentSkillState {
    pub(crate) skill_id: String,
    pub(crate) enabled: bool,
    pub(crate) eligible: bool,
    pub(crate) source: String,
    pub(crate) revision: String,
    pub(crate) updated_at: Option<u64>,
}

pub(crate) enum MeshLoopCommand {
    UpdateRequest {
        request: MeshJoinRequest,
        reply: oneshot::Sender<Result<MeshRuntimeStatus, String>>,
    },
    PublishManifest {
        manifest: MeshManifest,
        reply: oneshot::Sender<Result<MeshManifest, String>>,
    },
    VerifyManifest {
        request: MeshManifestVerificationRequest,
        reply: oneshot::Sender<Result<(), String>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MeshJoinRequest {
    pub(crate) user_address: String,
    pub(crate) device_id: String,
    pub(crate) chain_id: u32,
    pub(crate) gossip_topic: String,
    #[serde(default = "default_announce_topic")]
    pub(crate) announce_topic: String,
    #[serde(default = "default_manifest_topic")]
    pub(crate) manifest_topic: String,
    #[serde(default = "default_conclave_topic")]
    pub(crate) conclave_topic: String,
    #[serde(default = "default_mesh_heartbeat_ms")]
    pub(crate) heartbeat_ms: u64,
    #[serde(default = "default_kad_protocol")]
    pub(crate) kad_protocol: String,
    #[serde(default)]
    pub(crate) session_id: String,
    #[serde(default)]
    pub(crate) bootstrap_multiaddrs: Vec<String>,
    #[serde(default)]
    pub(crate) relay_multiaddrs: Vec<String>,
    #[serde(default)]
    pub(crate) published_agents: Vec<MeshPublishedAgent>,
    #[serde(default)]
    pub(crate) capabilities: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MeshPublishedAgent {
    pub(crate) agent_wallet: String,
    pub(crate) dna_hash: String,
    pub(crate) capabilities_hash: String,
    pub(crate) capabilities: Vec<String>,
    pub(crate) public_card: Option<MeshAgentCard>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MeshPublishedAgentStatus {
    pub(crate) agent_wallet: String,
    pub(crate) hai_id: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MeshRuntimeStatus {
    pub(crate) running: bool,
    pub(crate) status: String,
    pub(crate) user_address: Option<String>,
    pub(crate) published_agents: Vec<MeshPublishedAgentStatus>,
    pub(crate) device_id: Option<String>,
    pub(crate) peer_id: Option<String>,
    pub(crate) listen_multiaddrs: Vec<String>,
    pub(crate) relay_peer_id: Option<String>,
    pub(crate) peers_discovered: u32,
    pub(crate) last_heartbeat_at: Option<u64>,
    pub(crate) last_error: Option<String>,
    pub(crate) updated_at: u64,
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
            relay_peer_id: None,
            peers_discovered: 0,
            last_heartbeat_at: None,
            last_error: None,
            updated_at: now_ms(),
        }
    }
}

#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "MeshBehaviourEvent")]
pub(crate) struct MeshBehaviour {
    pub(crate) relay_client: relay::client::Behaviour,
    pub(crate) dcutr: dcutr::Behaviour,
    pub(crate) autonat: autonat::Behaviour,
    pub(crate) mdns: mdns::tokio::Behaviour,
    pub(crate) ping: ping::Behaviour,
    pub(crate) identify: identify::Behaviour,
    pub(crate) gossipsub: gossipsub::Behaviour,
    pub(crate) manifest_verify: request_response::cbor::Behaviour<
        MeshManifestVerificationRequest,
        MeshManifestVerificationResponse,
    >,
    pub(crate) kad: kad::Behaviour<kad::store::MemoryStore>,
    pub(crate) rendezvous: rendezvous::client::Behaviour,
    pub(crate) connection_limits: connection_limits::Behaviour,
}

#[derive(Debug)]
pub(crate) enum MeshBehaviourEvent {
    RelayClient(relay::client::Event),
    Dcutr(dcutr::Event),
    Autonat(autonat::Event),
    Mdns(mdns::Event),
    Ping(ping::Event),
    Identify(identify::Event),
    Gossipsub(gossipsub::Event),
    ManifestVerify(
        request_response::Event<MeshManifestVerificationRequest, MeshManifestVerificationResponse>,
    ),
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

impl From<mdns::Event> for MeshBehaviourEvent {
    fn from(event: mdns::Event) -> Self {
        Self::Mdns(event)
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

impl
    From<request_response::Event<MeshManifestVerificationRequest, MeshManifestVerificationResponse>>
    for MeshBehaviourEvent
{
    fn from(
        event: request_response::Event<
            MeshManifestVerificationRequest,
            MeshManifestVerificationResponse,
        >,
    ) -> Self {
        Self::ManifestVerify(event)
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

pub(crate) fn default_mesh_heartbeat_ms() -> u64 {
    30_000
}

pub(crate) fn default_announce_topic() -> String {
    "compose/announce/v1".to_string()
}

pub(crate) fn default_manifest_topic() -> String {
    "compose/manifest/v1".to_string()
}

pub(crate) fn default_conclave_topic() -> String {
    "compose/conclave/v1".to_string()
}

pub(crate) fn default_kad_protocol() -> String {
    MESH_KAD_PROTOCOL.to_string()
}

pub(crate) fn normalize_wallet(value: &str) -> Option<String> {
    let trimmed = value.trim().to_lowercase();
    if trimmed.len() != 42 || !trimmed.starts_with("0x") {
        return None;
    }
    if !trimmed.chars().skip(2).all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    Some(trimmed)
}

pub(crate) fn truncate_string(input: String, max_len: usize) -> String {
    let trimmed = input.trim().to_string();
    if trimmed.chars().count() <= max_len {
        return trimmed;
    }
    trimmed.chars().take(max_len).collect()
}

pub(crate) fn sanitize_mesh_agent_card(card: Option<MeshAgentCard>) -> Option<MeshAgentCard> {
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

pub(crate) fn normalize_device_id(value: &str) -> Option<String> {
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

pub(crate) fn normalize_mesh_api_url_with_loopback_policy(
    value: &str,
    allow_loopback: bool,
) -> String {
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

pub(crate) fn normalize_mesh_api_url(value: &str) -> String {
    normalize_mesh_api_url_with_loopback_policy(value, cfg!(debug_assertions))
}

pub(crate) fn normalize_capability(raw: &str) -> Option<String> {
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

pub(crate) fn default_capabilities(agent_wallet: &str) -> Vec<String> {
    let wallet_suffix = agent_wallet.trim_start_matches("0x");
    vec![format!("agent-{wallet_suffix}")]
}

pub(crate) fn normalize_published_agent(
    agent: &MeshPublishedAgent,
) -> Result<MeshPublishedAgent, String> {
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

pub(crate) fn request_published_statuses(
    request: &MeshJoinRequest,
) -> Vec<MeshPublishedAgentStatus> {
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

pub(crate) fn status_has_published_agent(status: &MeshRuntimeStatus, agent_wallet: &str) -> bool {
    status
        .published_agents
        .iter()
        .any(|item| item.agent_wallet == agent_wallet)
}

pub(crate) fn validate_mesh_join_request(
    request: &MeshJoinRequest,
) -> Result<MeshJoinRequest, String> {
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

pub(crate) fn mesh_request_requires_restart(
    current: &MeshJoinRequest,
    next: &MeshJoinRequest,
) -> bool {
    current.user_address != next.user_address
        || current.device_id != next.device_id
        || current.chain_id != next.chain_id
        || current.gossip_topic != next.gossip_topic
        || current.announce_topic != next.announce_topic
        || current.manifest_topic != next.manifest_topic
        || current.conclave_topic != next.conclave_topic
        || current.kad_protocol != next.kad_protocol
        || current.bootstrap_multiaddrs != next.bootstrap_multiaddrs
        || current.relay_multiaddrs != next.relay_multiaddrs
}

pub(crate) fn sync_mesh_request_capabilities(
    swarm: &mut Swarm<MeshBehaviour>,
    current: &MeshJoinRequest,
    next: &MeshJoinRequest,
    rendezvous_peers: &HashSet<PeerId>,
) {
    let current_capabilities = current.capabilities.iter().cloned().collect::<HashSet<_>>();
    let next_capabilities = next.capabilities.iter().cloned().collect::<HashSet<_>>();

    for capability in current_capabilities.difference(&next_capabilities) {
        let key = capability_dht_key(capability);
        swarm.behaviour_mut().kad.stop_providing(&key);

        if let Ok(namespace) = capability_namespace(capability) {
            for peer in rendezvous_peers {
                swarm
                    .behaviour_mut()
                    .rendezvous
                    .unregister(namespace.clone(), *peer);
            }
        }
    }

    for capability in next_capabilities.difference(&current_capabilities) {
        let key = capability_dht_key(capability);
        if let Err(err) = swarm.behaviour_mut().kad.start_providing(key) {
            eprintln!(
                "[mesh] kad start_providing failed for capability '{}': {}",
                capability, err
            );
        }
    }

    if current_capabilities != next_capabilities {
        refresh_rendezvous_registrations(swarm, next, rendezvous_peers);
        discover_capabilities(swarm, next, rendezvous_peers);
    }
}

pub(crate) fn with_mesh_status<T>(
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

pub(crate) fn next_mesh_run_generation(state: &MeshRuntimeState) -> Result<u64, String> {
    let mut guard = state
        .run_generation
        .lock()
        .map_err(|_| "failed to update mesh run generation".to_string())?;
    *guard = guard.saturating_add(1);
    Ok(*guard)
}

pub(crate) fn current_mesh_run_generation(app: &tauri::AppHandle) -> Option<u64> {
    let state = app.state::<MeshRuntimeState>();
    state.run_generation.lock().ok().map(|guard| *guard)
}

pub(crate) fn with_mesh_status_if_current<T>(
    app: &tauri::AppHandle,
    run_generation: u64,
    updater: impl FnOnce(&mut MeshRuntimeStatus) -> T,
) -> Option<T> {
    if current_mesh_run_generation(app) != Some(run_generation) {
        return None;
    }
    with_mesh_status(app, updater)
}

pub(crate) fn mark_mesh_status(
    app: &tauri::AppHandle,
    request: &MeshJoinRequest,
    status_value: &str,
    expected_run_generation: Option<u64>,
) {
    if let Some(run_generation) = expected_run_generation {
        if current_mesh_run_generation(app) != Some(run_generation) {
            return;
        }
    }
    let _ = with_mesh_status(app, |status| {
        status.running = status_value != "dormant";
        status.status = status_value.to_string();
        status.user_address = Some(request.user_address.clone());
        status.published_agents = request_published_statuses(request);
        status.device_id = Some(request.device_id.clone());
        status.listen_multiaddrs = Vec::new();
        status.relay_peer_id = None;
        status.updated_at = now_ms();
        if status_value != "online" {
            status.peers_discovered = 0;
            status.last_heartbeat_at = None;
            status.last_error = None;
        }
        if status_value == "dormant" {
            status.published_agents.clear();
            status.peer_id = None;
        }
    });
}

pub(crate) fn mesh_error(
    app: &tauri::AppHandle,
    request: Option<&MeshJoinRequest>,
    message: String,
    expected_run_generation: Option<u64>,
) {
    if let Some(run_generation) = expected_run_generation {
        if current_mesh_run_generation(app) != Some(run_generation) {
            return;
        }
    }
    let _ = with_mesh_status(app, |status| {
        if let Some(req) = request {
            status.user_address = Some(req.user_address.clone());
            status.published_agents = request_published_statuses(req);
            status.device_id = Some(req.device_id.clone());
        }
        status.running = false;
        status.status = "error".to_string();
        status.listen_multiaddrs = Vec::new();
        status.relay_peer_id = None;
        status.peers_discovered = 0;
        status.last_error = Some(message);
        status.updated_at = now_ms();
    });
}

pub(crate) fn extract_peer_id_from_multiaddr(addr: &Multiaddr) -> Option<PeerId> {
    for protocol in addr.iter() {
        if let Protocol::P2p(peer_id) = protocol {
            return Some(peer_id);
        }
    }
    None
}

pub(crate) fn derive_relay_listen_multiaddrs(relay_multiaddrs: &[String]) -> Vec<Multiaddr> {
    let mut derived = Vec::new();
    let mut peer_index: HashMap<String, usize> = HashMap::new();
    let mut peer_priority: Vec<u8> = Vec::new();

    for raw in relay_multiaddrs {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }

        let Ok(addr) = trimmed.parse::<Multiaddr>() else {
            continue;
        };
        let Some(peer_id) = extract_peer_id_from_multiaddr(&addr) else {
            continue;
        };

        let mut circuit_addr = addr.clone();
        circuit_addr.push(Protocol::P2pCircuit);
        let priority = if trimmed.contains("/ws") || trimmed.contains("/wss") {
            1
        } else {
            0
        };
        let peer_key = peer_id.to_string();

        if let Some(existing_index) = peer_index.get(&peer_key).copied() {
            if priority < peer_priority[existing_index] {
                derived[existing_index] = circuit_addr;
                peer_priority[existing_index] = priority;
            }
            continue;
        }

        peer_index.insert(peer_key, derived.len());
        peer_priority.push(priority);
        derived.push(circuit_addr);
    }

    derived
}

pub(crate) fn derive_relay_reservation_confirmed_multiaddrs(
    relay_multiaddrs: &[String],
    relay_peer_id: &PeerId,
    local_peer_id: &PeerId,
) -> Vec<String> {
    derive_relay_listen_multiaddrs(relay_multiaddrs)
        .into_iter()
        .filter(|addr| extract_peer_id_from_multiaddr(addr).as_ref() == Some(relay_peer_id))
        .map(|mut addr| {
            addr.push(Protocol::P2p(local_peer_id.to_owned()));
            addr.to_string()
        })
        .collect()
}

pub(crate) fn ensure_multiaddr_has_peer_id(addr: &Multiaddr, peer_id: &PeerId) -> String {
    if addr
        .iter()
        .any(|protocol| matches!(protocol, Protocol::P2p(_)))
    {
        return addr.to_string();
    }

    let mut next = addr.clone();
    next.push(Protocol::P2p(peer_id.to_owned()));
    next.to_string()
}

pub(crate) fn remove_confirmed_external_address(
    app: &tauri::AppHandle,
    address: &Multiaddr,
    local_peer_id: &PeerId,
    run_generation: u64,
    connected_peers: usize,
) {
    let addr_text = ensure_multiaddr_has_peer_id(address, local_peer_id);
    let _ = with_mesh_status_if_current(app, run_generation, |status| {
        status.listen_multiaddrs.retain(|item| item != &addr_text);
        status.status = mesh_runtime_state_label(
            &status.listen_multiaddrs,
            status.peers_discovered as usize,
            connected_peers,
        )
        .to_string();
        status.updated_at = now_ms();
    });
}

pub(crate) fn preferred_connected_relay_peer_id(
    connected_peers: &HashSet<PeerId>,
    relay_multiaddrs: &[String],
) -> Option<String> {
    for raw in relay_multiaddrs {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(addr) = trimmed.parse::<Multiaddr>() else {
            continue;
        };
        let Some(peer_id) = extract_peer_id_from_multiaddr(&addr) else {
            continue;
        };
        if connected_peers.contains(&peer_id) {
            return Some(peer_id.to_string());
        }
    }

    None
}

pub(crate) fn multiaddr_is_publicly_reachable_direct_path(value: &str) -> bool {
    let Ok(addr) = value.trim().parse::<Multiaddr>() else {
        return false;
    };

    for protocol in addr.iter() {
        match protocol {
            Protocol::P2pCircuit => return false,
            Protocol::Ip4(ip) => {
                return !ip.is_private()
                    && !ip.is_loopback()
                    && !ip.is_link_local()
                    && !ip.is_broadcast()
                    && !ip.is_multicast()
                    && !ip.is_unspecified();
            }
            Protocol::Ip6(ip) => {
                return !ip.is_loopback()
                    && !ip.is_unspecified()
                    && !ip.is_multicast()
                    && !ip.is_unicast_link_local()
                    && !ip.is_unique_local();
            }
            Protocol::Dns(_) | Protocol::Dns4(_) | Protocol::Dns6(_) | Protocol::Dnsaddr(_) => {
                return true
            }
            _ => {}
        }
    }

    false
}

pub(crate) fn preferred_advertised_listen_multiaddrs(listen_multiaddrs: &[String]) -> Vec<String> {
    let mut direct = Vec::new();
    let mut relay = Vec::new();
    let mut seen = HashSet::new();

    for value in listen_multiaddrs {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            continue;
        }
        if !seen.insert(trimmed.to_string()) {
            continue;
        }
        if trimmed.contains("/p2p-circuit") {
            relay.push(trimmed.to_string());
        } else if multiaddr_is_publicly_reachable_direct_path(trimmed) {
            direct.push(trimmed.to_string());
        }
    }

    if !direct.is_empty() {
        direct.extend(relay);
        direct
    } else {
        relay
    }
}

pub(crate) fn confirm_mesh_external_address(
    app: &tauri::AppHandle,
    request: &MeshJoinRequest,
    swarm: &mut Swarm<MeshBehaviour>,
    rendezvous_peers: &HashSet<PeerId>,
    address: &Multiaddr,
    local_peer_id: &PeerId,
    run_generation: u64,
) {
    let addr_text = ensure_multiaddr_has_peer_id(address, local_peer_id);
    if !multiaddr_is_publicly_reachable_direct_path(&addr_text) {
        return;
    }

    swarm.add_external_address(address.clone());

    let mut inserted = false;
    let _ = with_mesh_status_if_current(app, run_generation, |status| {
        if !status.listen_multiaddrs.contains(&addr_text) {
            status.listen_multiaddrs.push(addr_text.clone());
            inserted = true;
        }
        status.status = "online".to_string();
        status.updated_at = now_ms();
    });

    if inserted {
        append_mesh_log_to_published_agents(
            app,
            &request.published_agents,
            &format!("mesh external address confirmed: {addr_text}"),
        );
    }

    refresh_rendezvous_registrations(swarm, request, rendezvous_peers);
    queue_runtime_manifest_publications_if_ready(app, request, "mesh-runtime-online");
}

pub(crate) fn derive_relay_peer_id_from_listen_multiaddrs(
    listen_multiaddrs: &[String],
) -> Option<String> {
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

pub(crate) fn multiaddr_is_anchorable_path(raw: &str) -> bool {
    let Ok(addr) = raw.trim().parse::<Multiaddr>() else {
        return false;
    };

    let mut has_dns_host = false;
    let mut has_circuit = false;

    for protocol in addr.iter() {
        match protocol {
            Protocol::P2pCircuit => has_circuit = true,
            Protocol::Dns(_) | Protocol::Dns4(_) | Protocol::Dns6(_) | Protocol::Dnsaddr(_) => {
                has_dns_host = true
            }
            _ => {}
        }
    }

    has_circuit || has_dns_host
}

pub(crate) fn listen_multiaddrs_have_anchorable_path(listen_multiaddrs: &[String]) -> bool {
    listen_multiaddrs
        .iter()
        .any(|value| multiaddr_is_anchorable_path(value))
}

pub(crate) const ENVELOPE_VERSION: u64 = 1;
pub(crate) const ENVELOPE_ALLOWED_SKEW_MS: u64 = 120_000;
pub(crate) const ENVELOPE_NONCE_WINDOW_MS: u64 = 5 * 60 * 1_000;
pub(crate) const PEER_STALE_MS: u64 = 90_000;
pub(crate) const PEER_EVICT_MS: u64 = 5 * 60 * 1_000;
pub(crate) const KAD_REFRESH_INTERVAL_MS: u64 = 5 * 60 * 1_000;
pub(crate) const RENDEZVOUS_DISCOVERY_INTERVAL_MS: u64 = 2 * 60 * 1_000;
pub(crate) const RENDEZVOUS_REGISTER_REFRESH_INTERVAL_MS: u64 = 60_000;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct MeshEnvelope {
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
pub(crate) struct MeshEnvelopeUnsigned {
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
pub(crate) struct PeerCacheEntry {
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

pub(crate) fn unsigned_envelope_bytes(value: &MeshEnvelopeUnsigned) -> Result<Vec<u8>, String> {
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

pub(crate) fn build_signed_envelope_payload(
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

pub(crate) fn decode_and_validate_envelope(
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

pub(crate) fn next_nonce(counter: &mut u64, peer_id: &str) -> String {
    *counter = counter.saturating_add(1);
    format!("{}-{}", peer_id, *counter)
}

pub(crate) fn capability_dht_key(capability: &str) -> kad::RecordKey {
    kad::RecordKey::new(&format!("/compose/cap/{capability}/v1"))
}

pub(crate) fn capability_namespace(capability: &str) -> Result<rendezvous::Namespace, String> {
    rendezvous::Namespace::new(format!("compose/cap/{capability}/v1"))
        .map_err(|err| format!("invalid rendezvous namespace for capability '{capability}': {err}"))
}

pub(crate) fn extract_bootstrap_and_rendezvous_peers(
    request: &MeshJoinRequest,
) -> (Vec<Multiaddr>, HashSet<PeerId>) {
    let mut multiaddrs = Vec::new();
    let mut seen_multiaddrs = HashSet::new();
    let mut rendezvous_peers = HashSet::new();

    for raw in request
        .bootstrap_multiaddrs
        .iter()
        .chain(request.relay_multiaddrs.iter())
    {
        match raw.parse::<Multiaddr>() {
            Ok(addr) => {
                let addr_key = addr.to_string();
                if seen_multiaddrs.insert(addr_key) {
                    multiaddrs.push(addr);
                }
            }
            Err(err) => {
                eprintln!("[mesh] invalid bootstrap multiaddr '{}': {}", raw, err);
            }
        }
    }

    for raw in &request.relay_multiaddrs {
        match raw.parse::<Multiaddr>() {
            Ok(addr) => {
                if let Some(peer_id) = extract_peer_id_from_multiaddr(&addr) {
                    rendezvous_peers.insert(peer_id);
                }
            }
            Err(err) => {
                eprintln!(
                    "[mesh] invalid relay rendezvous multiaddr '{}': {}",
                    raw, err
                );
            }
        }
    }

    (multiaddrs, rendezvous_peers)
}

pub(crate) fn provide_capabilities(swarm: &mut Swarm<MeshBehaviour>, request: &MeshJoinRequest) {
    for capability in &request.capabilities {
        let key = capability_dht_key(capability);
        if let Err(err) = swarm.behaviour_mut().kad.start_providing(key) {
            eprintln!(
                "[mesh] kad start_providing failed for capability '{}': {}",
                capability, err
            );
        }
    }
}

pub(crate) fn register_capabilities(
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
            match swarm
                .behaviour_mut()
                .rendezvous
                .register(namespace.clone(), *peer, None)
            {
                Ok(()) => {}
                Err(rendezvous::client::RegisterError::NoExternalAddresses) => {}
                Err(err) => {
                    eprintln!(
                        "[mesh] rendezvous register failed for capability '{}' @ {}: {}",
                        capability, peer, err
                    );
                }
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

pub(crate) fn discover_capabilities(
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

pub(crate) fn refresh_rendezvous_registrations(
    swarm: &mut Swarm<MeshBehaviour>,
    request: &MeshJoinRequest,
    rendezvous_peers: &HashSet<PeerId>,
) {
    if rendezvous_peers.is_empty() {
        return;
    }

    register_capabilities(swarm, request, rendezvous_peers);
}

pub(crate) fn apply_kad_mode_from_autonat(
    swarm: &mut Swarm<MeshBehaviour>,
    nat_status: &autonat::NatStatus,
) {
    match nat_status {
        autonat::NatStatus::Public(_) => {
            swarm.behaviour_mut().kad.set_mode(Some(kad::Mode::Server))
        }
        autonat::NatStatus::Private | autonat::NatStatus::Unknown => {
            swarm.behaviour_mut().kad.set_mode(Some(kad::Mode::Client))
        }
    }
}

pub(crate) fn mesh_runtime_state_label(
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

pub(crate) fn a509_with_reason(reason: &str) -> String {
    let trimmed = reason.trim();
    if trimmed.is_empty() {
        A509_INCONSISTENT_AGENT_IDENTITY.to_string()
    } else {
        format!("a509: {trimmed}")
    }
}

pub(crate) fn is_a509_error(error: &str) -> bool {
    error.trim().to_lowercase().starts_with("a509:")
}

pub(crate) fn verify_mesh_manifest_signature(manifest: &MeshManifest) -> Result<(), String> {
    let peer_id = PeerId::from_str(&manifest.peer_id)
        .map_err(|err| format!("invalid manifest peerId: {err}"))?;
    let multihash = peer_id.as_ref();
    if multihash.code() != 0 {
        return Err(a509_with_reason(
            "manifest peerId must contain inline public key material",
        ));
    }
    let public_key = identity::PublicKey::try_decode_protobuf(multihash.digest())
        .map_err(|err| format!("failed to decode manifest public key: {err}"))?;
    let signature = decode_hex_string(&manifest.signature)
        .map_err(|err| format!("invalid manifest signature encoding: {err}"))?;
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
    if !public_key.verify(&sign_bytes, &signature) {
        return Err(a509_with_reason("manifest signature verification failed"));
    }
    Ok(())
}

pub(crate) async fn fetch_authoritative_mesh_state(
    retrieval_url: &str,
) -> Result<SignedMeshStateEnvelope, String> {
    let client = HttpClient::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|err| format!("failed to build manifest verification client: {err}"))?;
    let response = client
        .get(retrieval_url)
        .send()
        .await
        .map_err(|err| format!("failed to fetch latest anchored mesh state: {err}"))?;
    if !response.status().is_success() {
        return Err(format!(
            "latest anchored mesh state fetch failed: HTTP {}",
            response.status()
        ));
    }
    response
        .json::<SignedMeshStateEnvelope>()
        .await
        .map_err(|err| format!("failed to decode latest anchored mesh state JSON: {err}"))
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct RegisteredAgentAuthority {
    wallet_address: Option<String>,
    name: Option<String>,
    description: Option<String>,
    model: Option<String>,
    framework: Option<String>,
}

async fn fetch_registered_agent_authority(
    agent_wallet: &str,
) -> Result<RegisteredAgentAuthority, String> {
    let normalized_wallet =
        normalize_wallet(agent_wallet).ok_or_else(|| "agentWallet is invalid".to_string())?;
    let client = HttpClient::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|err| format!("failed to build registered agent client: {err}"))?;
    let response = client
        .get(format!(
            "https://api.compose.market/agent/{normalized_wallet}"
        ))
        .send()
        .await
        .map_err(|err| format!("failed to fetch registered agent metadata: {err}"))?;
    if !response.status().is_success() {
        return Err(format!(
            "registered agent metadata fetch failed: HTTP {}",
            response.status()
        ));
    }
    response
        .json::<RegisteredAgentAuthority>()
        .await
        .map_err(|err| format!("failed to decode registered agent metadata JSON: {err}"))
}

pub(crate) async fn verify_mesh_manifest_identity(
    request: &MeshManifestVerificationRequest,
) -> Result<(), String> {
    verify_mesh_manifest_signature(&request.manifest)?;

    let derived_hai = derive_hai_id(
        &request.manifest.agent_wallet,
        &request.manifest.user_wallet,
        &request.manifest.device_id,
    );
    if derived_hai != request.hai_id {
        return Err(a509_with_reason(
            "haiId does not match the manifest triplet",
        ));
    }

    let Some(current_state_root_hash) = request
        .manifest
        .state_root_hash
        .as_deref()
        .and_then(normalize_state_root_hash_for_compare)
    else {
        return Err(a509_with_reason(
            "stateRootHash is missing from the current manifest",
        ));
    };

    let Some(retrieval_url) = request
        .latest_retrieval_url
        .as_deref()
        .and_then(normalize_persisted_url)
    else {
        let authority = fetch_registered_agent_authority(&request.manifest.agent_wallet)
            .await
            .map_err(|err| a509_with_reason(err.as_str()))?;

        let authority_wallet = authority
            .wallet_address
            .as_deref()
            .and_then(normalize_wallet)
            .ok_or_else(|| {
                a509_with_reason("registered agent metadata walletAddress is invalid")
            })?;
        if authority_wallet != request.manifest.agent_wallet {
            return Err(a509_with_reason(
                "agentWallet does not match the registered agent metadata",
            ));
        }

        let authority_name = authority
            .name
            .map(|value| truncate_string(value, 80))
            .unwrap_or_default();
        if authority_name != request.manifest.name {
            return Err(a509_with_reason(
                "name does not match the registered agent metadata",
            ));
        }

        let authority_description = authority
            .description
            .map(|value| truncate_string(value, 240))
            .unwrap_or_default();
        if authority_description != request.manifest.description {
            return Err(a509_with_reason(
                "description does not match the registered agent metadata",
            ));
        }

        let authority_model = authority
            .model
            .map(|value| truncate_string(value, 120))
            .unwrap_or_default();
        if authority_model != request.manifest.model {
            return Err(a509_with_reason(
                "model does not match the registered agent metadata",
            ));
        }

        let authority_framework = authority
            .framework
            .map(|value| truncate_string(value, 80))
            .unwrap_or_default();
        if authority_framework != request.manifest.framework {
            return Err(a509_with_reason(
                "framework does not match the registered agent metadata",
            ));
        }

        return Ok(());
    };

    let authority = fetch_authoritative_mesh_state(retrieval_url.as_str())
        .await
        .map_err(|err| a509_with_reason(err.as_str()))?;

    if authority.hai_id != derived_hai {
        return Err(a509_with_reason(
            "haiId does not match the latest anchored state",
        ));
    }
    if authority.agent_wallet != request.manifest.agent_wallet {
        return Err(a509_with_reason(
            "agentWallet does not match the latest anchored state",
        ));
    }
    if authority.user_wallet != request.manifest.user_wallet {
        return Err(a509_with_reason(
            "userAddress does not match the latest anchored state",
        ));
    }
    if authority.device_id != request.manifest.device_id {
        return Err(a509_with_reason(
            "deviceId does not match the latest anchored state",
        ));
    }
    if authority.chain_id != request.manifest.chain_id {
        return Err(a509_with_reason(
            "chainId does not match the latest anchored state",
        ));
    }
    if !same_state_root_hash(
        Some(authority.state_root_hash.as_str()),
        current_state_root_hash.as_str(),
    ) {
        return Err(a509_with_reason(
            "stateRootHash does not match the latest anchored state",
        ));
    }

    Ok(())
}

pub(crate) fn recompute_peer_cache_status(
    peer_cache: &mut HashMap<String, PeerCacheEntry>,
) -> usize {
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

pub(crate) fn peer_cache_key(peer_id: &str, agent_wallet: &str, hai_id: &str) -> String {
    let normalized_wallet =
        normalize_wallet(agent_wallet).unwrap_or_else(|| agent_wallet.trim().to_lowercase());
    format!("{peer_id}:{normalized_wallet}:{}", hai_id.trim())
}

pub(crate) fn emit_peer_index(
    app: &tauri::AppHandle,
    peer_cache: &HashMap<String, PeerCacheEntry>,
) {
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

pub(crate) fn build_mesh_swarm(
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
        Vec<String>,
    ),
    String,
> {
    let local_peer_id = PeerId::from(local_key.public());
    let mut warnings = Vec::new();

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
    let manifest_verify = request_response::cbor::Behaviour::new(
        [(
            StreamProtocol::new(MESH_MANIFEST_VERIFY_PROTOCOL),
            request_response::ProtocolSupport::Full,
        )],
        request_response::Config::default(),
    );

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
        .with_quic()
        .with_dns()
        .map_err(|err| format!("failed to initialize dns transport: {err}"))?
        .with_relay_client(noise::Config::new, yamux::Config::default)
        .map_err(|err| format!("failed to initialize relay transport: {err}"))?
        .with_behaviour(|key, relay_client| {
            Ok(MeshBehaviour {
                relay_client,
                dcutr: dcutr::Behaviour::new(local_peer_id),
                autonat: autonat::Behaviour::new(local_peer_id, autonat::Config::default()),
                mdns: mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)?,
                ping: ping::Behaviour::new(ping::Config::new()),
                identify: identify::Behaviour::new(
                    identify::Config::new(MESH_IDENTIFY_PROTOCOL.to_string(), key.public())
                        .with_push_listen_addr_updates(true),
                ),
                gossipsub,
                manifest_verify,
                kad,
                rendezvous: rendezvous::client::Behaviour::new(key.clone()),
                connection_limits,
            })
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
    swarm
        .listen_on(
            "/ip4/0.0.0.0/udp/0/quic-v1"
                .parse::<Multiaddr>()
                .map_err(|err| format!("invalid quic listen address: {err}"))?,
        )
        .map_err(|err| format!("failed to start quic listening: {err}"))?;

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
            warnings.push(format!(
                "mesh bootstrap dial failed: {} :: {}",
                multiaddr, err
            ));
        }
    }

    for relay_listen_addr in derive_relay_listen_multiaddrs(&request.relay_multiaddrs) {
        if let Err(err) = swarm.listen_on(relay_listen_addr.clone()) {
            warnings.push(format!(
                "mesh relay circuit listen failed: {} :: {}",
                relay_listen_addr, err
            ));
        }
    }

    if let Err(err) = swarm.behaviour_mut().kad.bootstrap() {
        warnings.push(format!("mesh initial kad bootstrap failed: {err}"));
    }

    Ok((
        swarm,
        global_topic,
        announce_topic,
        manifest_topic,
        conclave_topic,
        rendezvous_peers,
        warnings,
    ))
}

pub(crate) async fn run_mesh_loop(
    app: tauri::AppHandle,
    request: MeshJoinRequest,
    mut stop_rx: oneshot::Receiver<()>,
    mut command_rx: mpsc::UnboundedReceiver<MeshLoopCommand>,
    run_generation: u64,
) {
    let mut request = request;
    let local_key = match load_or_create_mesh_identity(&app) {
        Ok(value) => value,
        Err(err) => {
            mesh_error(&app, Some(&request), err, Some(run_generation));
            return;
        }
    };
    let local_peer_id = PeerId::from(local_key.public());
    let local_peer_id_text = local_peer_id.to_string();

    let (
        mut swarm,
        global_topic,
        announce_topic,
        manifest_topic,
        _conclave_topic,
        rendezvous_peers,
        startup_warnings,
    ) = match build_mesh_swarm(local_key.clone(), &request) {
        Ok(value) => value,
        Err(err) => {
            mesh_error(&app, Some(&request), err, Some(run_generation));
            return;
        }
    };
    let rendezvous_peers = rendezvous_peers;

    for warning in startup_warnings {
        warn_mesh_published_agents(&app, &request, warning);
    }

    provide_capabilities(&mut swarm, &request);
    refresh_rendezvous_registrations(&mut swarm, &request, &rendezvous_peers);
    discover_capabilities(&mut swarm, &request, &rendezvous_peers);

    let mut connected_peers: HashSet<PeerId> = HashSet::new();
    let mut peer_cache: HashMap<String, PeerCacheEntry> = HashMap::new();
    let mut seen_nonces: HashMap<String, u64> = HashMap::new();
    let mut nonce_counter: u64 = 0;
    let mut pending_manifest_verifications: HashMap<
        request_response::OutboundRequestId,
        PendingManifestVerification,
    > = HashMap::new();

    let mut heartbeat_interval = tokio::time::interval(Duration::from_millis(request.heartbeat_ms));
    let mut kad_refresh_interval =
        tokio::time::interval(Duration::from_millis(KAD_REFRESH_INTERVAL_MS));
    let mut peer_prune_interval = tokio::time::interval(Duration::from_secs(30));
    let mut rendezvous_register_interval = tokio::time::interval(Duration::from_millis(
        RENDEZVOUS_REGISTER_REFRESH_INTERVAL_MS,
    ));
    let mut rendezvous_discovery_interval =
        tokio::time::interval(Duration::from_millis(RENDEZVOUS_DISCOVERY_INTERVAL_MS));

    let _ = with_mesh_status_if_current(&app, run_generation, |status| {
        status.running = true;
        status.status = "connecting".to_string();
        status.user_address = Some(request.user_address.clone());
        status.published_agents = request_published_statuses(&request);
        status.device_id = Some(request.device_id.clone());
        status.peer_id = Some(local_peer_id_text.clone());
        status.listen_multiaddrs = Vec::new();
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
                mark_mesh_status(&app, &request, "dormant", Some(run_generation));
                break;
            }
            Some(command) = command_rx.recv() => {
                match command {
                    MeshLoopCommand::UpdateRequest { request: next_request, reply } => {
                        if mesh_request_requires_restart(&request, &next_request) {
                            let _ = reply.send(Err("mesh request requires restart".to_string()));
                            continue;
                        }

                        if request != next_request {
                            sync_mesh_request_capabilities(
                                &mut swarm,
                                &request,
                                &next_request,
                                &rendezvous_peers,
                            );
                            if request.heartbeat_ms != next_request.heartbeat_ms {
                                heartbeat_interval = tokio::time::interval(Duration::from_millis(
                                    next_request.heartbeat_ms,
                                ));
                            }
                            request = next_request;
                            let _ = with_mesh_status_if_current(&app, run_generation, |status| {
                                status.user_address = Some(request.user_address.clone());
                                status.published_agents = request_published_statuses(&request);
                                status.device_id = Some(request.device_id.clone());
                                status.updated_at = now_ms();
                            });
                            queue_runtime_manifest_publications_if_ready(
                                &app,
                                &request,
                                "mesh-runtime-request-updated",
                            );
                        }

                        let status = with_mesh_status(&app, |status| status.clone())
                            .unwrap_or_default();
                        let _ = reply.send(Ok(status));
                    }
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
                    MeshLoopCommand::VerifyManifest { request: verify_request, reply } => {
                        let agent_wallet = verify_request.manifest.agent_wallet.clone();
                        start_manifest_verification_request(
                            &mut swarm,
                            &local_peer_id_text,
                            &connected_peers,
                            verify_request,
                            &mut pending_manifest_verifications,
                            &agent_wallet,
                            Some(reply),
                        );
                    }
                }
            }
            _ = kad_refresh_interval.tick() => {
                if let Err(err) = swarm.behaviour_mut().kad.bootstrap() {
                    warn_mesh_published_agents(
                        &app,
                        &request,
                        format!("mesh periodic kad bootstrap failed: {err}"),
                    );
                }
                swarm
                    .behaviour_mut()
                    .kad
                    .get_closest_peers(local_peer_id_text.as_bytes().to_vec());
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
            _ = rendezvous_register_interval.tick() => {
                refresh_rendezvous_registrations(&mut swarm, &request, &rendezvous_peers);
            }
            _ = heartbeat_interval.tick() => {
                let listen_multiaddrs = with_mesh_status(&app, |status| {
                    preferred_advertised_listen_multiaddrs(&status.listen_multiaddrs)
                })
                .unwrap_or_default();
                let mut heartbeat_error: Option<String> = None;

                for published in &request.published_agents {
                    match build_current_mesh_manifest_verification_request(
                        &app,
                        &published.agent_wallet,
                        &with_mesh_status(&app, |status| status.clone()).unwrap_or_default(),
                    ) {
                        Ok(verify_request) => {
                            start_manifest_verification_request(
                                &mut swarm,
                                &local_peer_id_text,
                                &connected_peers,
                                verify_request,
                                &mut pending_manifest_verifications,
                                &published.agent_wallet,
                                None,
                            );
                        }
                        Err(error) => {
                            heartbeat_error.get_or_insert(error);
                        }
                    }

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
                        &local_peer_id_text,
                        &caps,
                        &listen_multiaddrs,
                        next_nonce(&mut nonce_counter, &local_peer_id_text),
                    );
                    let announce_payload = build_signed_envelope_payload(
                        &local_key,
                        &request,
                        published,
                        &hai_id,
                        "announce",
                        &local_peer_id_text,
                        &caps,
                        &listen_multiaddrs,
                        next_nonce(&mut nonce_counter, &local_peer_id_text),
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
                        let _ = with_mesh_status_if_current(&app, run_generation, |status| {
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
                        queue_runtime_manifest_publications_if_ready(
                            &app,
                            &request,
                            "mesh-runtime-online",
                        );
                    }
                    SwarmEvent::NewExternalAddrCandidate { address } => {
                        confirm_mesh_external_address(
                            &app,
                            &request,
                            &mut swarm,
                            &rendezvous_peers,
                            &address,
                            &local_peer_id,
                            run_generation,
                        );
                    }
                    SwarmEvent::ExternalAddrConfirmed { address } => {
                        let addr = ensure_multiaddr_has_peer_id(&address, &local_peer_id);
                        let mut inserted = false;
                        let _ = with_mesh_status_if_current(&app, run_generation, |status| {
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
                                &format!("mesh external address confirmed: {addr}"),
                            );
                        }
                        refresh_rendezvous_registrations(&mut swarm, &request, &rendezvous_peers);
                        queue_runtime_manifest_publications_if_ready(
                            &app,
                            &request,
                            "mesh-runtime-online",
                        );
                    }
                    SwarmEvent::ExternalAddrExpired { address } => {
                        remove_confirmed_external_address(
                            &app,
                            &address,
                            &local_peer_id,
                            run_generation,
                            connected_peers.len(),
                        );
                    }
                    SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                        connected_peers.insert(peer_id);
                        let _ = with_mesh_status(&app, |status| {
                            status.peers_discovered = status.peers_discovered.max(connected_peers.len() as u32);
                            status.relay_peer_id =
                                preferred_connected_relay_peer_id(&connected_peers, &request.relay_multiaddrs);
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
                            status.relay_peer_id =
                                preferred_connected_relay_peer_id(&connected_peers, &request.relay_multiaddrs);
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
                            confirm_mesh_external_address(
                                &app,
                                &request,
                                &mut swarm,
                                &rendezvous_peers,
                                &info.observed_addr,
                                &local_peer_id,
                                run_generation,
                            );
                        }
                        let _ = with_mesh_status_if_current(&app, run_generation, |status| {
                            status.updated_at = now_ms();
                        });
                    }
                    SwarmEvent::Behaviour(MeshBehaviourEvent::Autonat(event)) => {
                        if let autonat::Event::OutboundProbe(
                            autonat::OutboundProbeEvent::Response { peer, address, .. },
                        ) = &event
                        {
                            append_mesh_log_to_published_agents(
                                &app,
                                &request.published_agents,
                                &format!("mesh autonat public address observed: peer={peer} addr={address}"),
                            );
                            confirm_mesh_external_address(
                                &app,
                                &request,
                                &mut swarm,
                                &rendezvous_peers,
                                address,
                                &local_peer_id,
                                run_generation,
                            );
                        }
                        if let autonat::Event::StatusChanged { new, .. } = &event {
                            apply_kad_mode_from_autonat(&mut swarm, new);
                            if let autonat::NatStatus::Public(address) = new {
                                confirm_mesh_external_address(
                                    &app,
                                    &request,
                                    &mut swarm,
                                    &rendezvous_peers,
                                    address,
                                    &local_peer_id,
                                    run_generation,
                                );
                            }
                            if let Err(err) = swarm.behaviour_mut().kad.bootstrap() {
                                warn_mesh_published_agents(
                                    &app,
                                    &request,
                                    format!("mesh kad bootstrap after autonat change failed: {err}"),
                                );
                            }
                        }
                        if let autonat::Event::StatusChanged { old, new } = &event {
                            if let autonat::NatStatus::Public(old_address) = old {
                                if !matches!(new, autonat::NatStatus::Public(current) if current == old_address) {
                                    swarm.remove_external_address(old_address);
                                    remove_confirmed_external_address(
                                        &app,
                                        old_address,
                                        &local_peer_id,
                                        run_generation,
                                        connected_peers.len(),
                                    );
                                }
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
                    SwarmEvent::Behaviour(MeshBehaviourEvent::ManifestVerify(event)) => {
                        match event {
                            request_response::Event::Message { message, .. } => {
                                match message {
                                    request_response::Message::Request { request, channel, .. } => {
                                        let response = match verify_mesh_manifest_identity(&request).await {
                                            Ok(()) => MeshManifestVerificationResponse {
                                                ok: true,
                                                error: None,
                                            },
                                            Err(error) => MeshManifestVerificationResponse {
                                                ok: false,
                                                error: Some(error),
                                            },
                                        };
                                        let _ = swarm
                                            .behaviour_mut()
                                            .manifest_verify
                                            .send_response(channel, response);
                                    }
                                    request_response::Message::Response { request_id, response } => {
                                        if let Some(pending) =
                                            pending_manifest_verifications.remove(&request_id)
                                        {
                                            let result = if response.ok {
                                                Ok(())
                                            } else {
                                                Err(response.error.unwrap_or_else(|| {
                                                    A509_INCONSISTENT_AGENT_IDENTITY.to_string()
                                                }))
                                            };

                                            if let Err(error) = &result {
                                                let _ = append_daemon_log(
                                                    &app,
                                                    &pending.agent_wallet,
                                                    error,
                                                );
                                                if is_a509_error(error) {
                                                    let _ = queue_manifest_reconcile_after_a509(
                                                        &app,
                                                        &pending.agent_wallet,
                                                    );
                                                }
                                                let _ = with_mesh_status(&app, |status| {
                                                    status.last_error = Some(error.clone());
                                                    status.updated_at = now_ms();
                                                });
                                            } else {
                                                let _ = with_mesh_status(&app, |status| {
                                                    status.updated_at = now_ms();
                                                });
                                            }

                                            if let Some(reply) = pending.reply {
                                                let _ = reply.send(result);
                                            }
                                        }
                                    }
                                }
                            }
                            request_response::Event::OutboundFailure { request_id, .. } => {
                                if let Some(pending) =
                                    pending_manifest_verifications.remove(&request_id)
                                {
                                    if let Some(reply) = pending.reply {
                                        let _ = reply.send(Ok(()));
                                    }
                                }
                                let _ = with_mesh_status(&app, |status| {
                                    status.updated_at = now_ms();
                                });
                            }
                            request_response::Event::InboundFailure { .. }
                            | request_response::Event::ResponseSent { .. } => {
                                let _ = with_mesh_status(&app, |status| {
                                    status.updated_at = now_ms();
                                });
                            }
                        }
                    }
                    SwarmEvent::Behaviour(MeshBehaviourEvent::Kad(_event)) => {
                        let _ = with_mesh_status(&app, |status| {
                            status.updated_at = now_ms();
                        });
                    }
                    SwarmEvent::Behaviour(MeshBehaviourEvent::Rendezvous(event)) => {
                        match event {
                            rendezvous::client::Event::Registered { rendezvous_node, namespace, ttl } => {
                                append_mesh_log_to_published_agents(
                                    &app,
                                    &request.published_agents,
                                    &format!(
                                        "mesh rendezvous registered: namespace={} peer={} ttl={}s",
                                        namespace,
                                        rendezvous_node,
                                        ttl
                                    ),
                                );
                            }
                            rendezvous::client::Event::RegisterFailed { rendezvous_node, namespace, error } => {
                                let _ = with_mesh_status(&app, |status| {
                                    status.last_error = Some(format!(
                                        "rendezvous register failed for {} @ {}: {:?}",
                                        namespace, rendezvous_node, error
                                    ));
                                    status.updated_at = now_ms();
                                });
                            }
                            rendezvous::client::Event::DiscoverFailed { rendezvous_node, namespace, error } => {
                                let namespace_label = namespace
                                    .map(|value| value.to_string())
                                    .unwrap_or_else(|| "all".to_string());
                                let _ = with_mesh_status(&app, |status| {
                                    status.last_error = Some(format!(
                                        "rendezvous discover failed for {} @ {}: {:?}",
                                        namespace_label, rendezvous_node, error
                                    ));
                                    status.updated_at = now_ms();
                                });
                            }
                            rendezvous::client::Event::Discovered { registrations, .. } => {
                                for registration in registrations {
                                    let peer_id = registration.record.peer_id();
                                    for addr in registration.record.addresses() {
                                        swarm.behaviour_mut().kad.add_address(&peer_id, addr.clone());
                                        let _ = swarm.dial(addr.clone());
                                    }
                                }
                            }
                            rendezvous::client::Event::Expired { peer } => {
                                append_mesh_log_to_published_agents(
                                    &app,
                                    &request.published_agents,
                                    &format!("mesh rendezvous registration expired: peer={peer}"),
                                );
                            }
                        }
                        let _ = with_mesh_status(&app, |status| {
                            status.updated_at = now_ms();
                        });
                    }
                    SwarmEvent::Behaviour(MeshBehaviourEvent::RelayClient(event)) => {
                        if let relay::client::Event::ReservationReqAccepted { relay_peer_id, renewal, .. } = &event {
                            let derived_relay_addrs = derive_relay_reservation_confirmed_multiaddrs(
                                &request.relay_multiaddrs,
                                relay_peer_id,
                                &local_peer_id,
                            );
                            let mut inserted_addrs = Vec::new();
                            let _ = with_mesh_status_if_current(&app, run_generation, |status| {
                                for addr in &derived_relay_addrs {
                                    if !status.listen_multiaddrs.contains(addr) {
                                        status.listen_multiaddrs.push(addr.clone());
                                        inserted_addrs.push(addr.clone());
                                    }
                                }
                                status.relay_peer_id = Some(relay_peer_id.to_string());
                                status.status = "online".to_string();
                                status.updated_at = now_ms();
                            });
                            append_mesh_log_to_published_agents(
                                &app,
                                &request.published_agents,
                                &format!(
                                    "mesh relay reservation accepted: relay={} renewal={}",
                                    relay_peer_id,
                                    renewal
                                ),
                            );
                            for addr in inserted_addrs {
                                append_mesh_log_to_published_agents(
                                    &app,
                                    &request.published_agents,
                                    &format!("mesh relay address ready: {addr}"),
                                );
                            }
                            refresh_rendezvous_registrations(&mut swarm, &request, &rendezvous_peers);
                            queue_runtime_manifest_publications_if_ready(
                                &app,
                                &request,
                                "mesh-runtime-online",
                            );
                        }
                        let _ = with_mesh_status(&app, |status| {
                            status.updated_at = now_ms();
                        });
                    }
                    SwarmEvent::Behaviour(MeshBehaviourEvent::Mdns(event)) => {
                        match event {
                            mdns::Event::Discovered(peers) => {
                                let mut discovered = Vec::new();
                                for (peer_id, addr) in peers {
                                    if peer_id == local_peer_id {
                                        continue;
                                    }
                                    swarm.behaviour_mut().kad.add_address(&peer_id, addr.clone());
                                    let _ = swarm.dial(addr.clone());
                                    discovered.push(format!("{peer_id} @ {addr}"));
                                }
                                if !discovered.is_empty() {
                                    append_mesh_log_to_published_agents(
                                        &app,
                                        &request.published_agents,
                                        &format!(
                                            "mesh lan peer discovered: {}",
                                            discovered.join(", ")
                                        ),
                                    );
                                }
                            }
                            mdns::Event::Expired(peers) => {
                                for (peer_id, addr) in peers {
                                    swarm.behaviour_mut().kad.remove_address(&peer_id, &addr);
                                }
                            }
                        }
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

    if current_mesh_run_generation(&app) == Some(run_generation) {
        if let Ok(mut guard) = app.state::<MeshRuntimeState>().command_tx.lock() {
            *guard = None;
        }
        if let Ok(mut active_request) = app.state::<MeshRuntimeState>().active_request.lock() {
            *active_request = None;
        }
    }
}

#[tauri::command]
pub fn local_network_status(
    state: tauri::State<MeshRuntimeState>,
) -> Result<MeshRuntimeStatus, String> {
    let status = state
        .status
        .lock()
        .map_err(|_| "failed to read mesh runtime status".to_string())?;
    Ok(status.clone())
}

#[tauri::command]
pub async fn local_network_join(
    app: tauri::AppHandle,
    state: tauri::State<'_, MeshRuntimeState>,
    request: MeshJoinRequest,
) -> Result<MeshRuntimeStatus, String> {
    let request = validate_mesh_join_request(&request)?;
    let live_status = state
        .status
        .lock()
        .map_err(|_| "failed to read mesh runtime status".to_string())?
        .clone();
    let current_request = state
        .active_request
        .lock()
        .map_err(|_| "failed to read active mesh request".to_string())?
        .clone();
    let command_tx = state
        .command_tx
        .lock()
        .map_err(|_| "failed to read mesh command channel".to_string())?
        .clone();

    if live_status.running {
        if let Some(active_request) = current_request {
            if !mesh_request_requires_restart(&active_request, &request) {
                if active_request == request && command_tx.is_some() {
                    return Ok(live_status);
                }
                if let Some(command_tx) = command_tx.clone() {
                    let (reply_tx, reply_rx) = oneshot::channel();
                    command_tx
                        .send(MeshLoopCommand::UpdateRequest {
                            request: request.clone(),
                            reply: reply_tx,
                        })
                        .map_err(|_| "mesh runtime update channel is closed".to_string())?;
                    let status = reply_rx
                        .await
                        .map_err(|_| "mesh runtime update dropped".to_string())??;
                    {
                        let mut active_request = state
                            .active_request
                            .lock()
                            .map_err(|_| "failed to update active mesh request".to_string())?;
                        *active_request = Some(request);
                    }
                    return Ok(status);
                }
            }
        }
    }
    let run_generation = next_mesh_run_generation(&state)?;

    if let Ok(mut stop_guard) = state.stop_tx.lock() {
        if let Some(stop_tx) = stop_guard.take() {
            let _ = stop_tx.send(());
        }
    }

    mark_mesh_status(&app, &request, "connecting", Some(run_generation));
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
    {
        let mut active_request = state
            .active_request
            .lock()
            .map_err(|_| "failed to update active mesh request".to_string())?;
        *active_request = Some(request.clone());
    }

    let app_handle = app.clone();
    tauri::async_runtime::spawn(async move {
        run_mesh_loop(app_handle, request, stop_rx, command_rx, run_generation).await;
    });

    local_network_status(state)
}

#[tauri::command]
pub async fn local_network_leave(
    app: tauri::AppHandle,
    state: tauri::State<'_, MeshRuntimeState>,
) -> Result<MeshRuntimeStatus, String> {
    let _ = next_mesh_run_generation(&state);
    if let Ok(mut stop_guard) = state.stop_tx.lock() {
        if let Some(stop_tx) = stop_guard.take() {
            let _ = stop_tx.send(());
        }
    }
    if let Ok(mut command_guard) = state.command_tx.lock() {
        *command_guard = None;
    }
    if let Ok(mut active_request) = state.active_request.lock() {
        *active_request = None;
    }

    let _ = with_mesh_status(&app, |status| {
        *status = MeshRuntimeStatus::default();
    });

    local_network_status(state)
}

#[cfg(test)]
mod tests {
    use super::{
        build_signed_envelope_payload, decode_and_validate_envelope, default_kad_protocol,
        derive_relay_listen_multiaddrs, derive_relay_peer_id_from_listen_multiaddrs,
        derive_relay_reservation_confirmed_multiaddrs, ensure_multiaddr_has_peer_id,
        extract_bootstrap_and_rendezvous_peers, listen_multiaddrs_have_anchorable_path,
        mesh_request_requires_restart, normalize_mesh_api_url_with_loopback_policy, now_ms,
        preferred_advertised_listen_multiaddrs, preferred_connected_relay_peer_id,
        ActiveSessionRefreshResponse, DaemonAgentState, DaemonPermissionPolicy, DaemonStateFile,
        MeshJoinRequest, MeshPublishedAgent, PersistedInstalledAgent, PersistedLocalIdentity,
        MESH_KAD_PROTOCOL,
    };
    use crate::gossipsub;
    use crate::manifest::{
        compose_hai_path, derive_hai_id, installed_agent_has_persisted_manifest,
        installed_agent_manifest_has_anchorable_transport, manifest_republish_on_a509_requested,
        normalize_manifest_publish_outcome, normalize_state_root_hash_for_compare,
        same_state_root_hash, should_queue_manifest_publication, sign_mesh_manifest, MeshManifest,
    };
    use crate::{
        apply_active_session_refresh, build_local_request_body, extract_skill_name_from_markdown,
        load_local_state_value_from_path, markdown_represents_agent_skill,
        normalize_daemon_state_for_local_mode, normalize_local_state_json, parse_local_agent_reply,
        remote_action_path_allowed, write_string_atomically,
    };
    use libp2p::{identity, Multiaddr, PeerId};
    use std::{
        collections::{HashMap, HashSet},
        fs,
        str::FromStr,
    };

    fn mesh_join_request_fixture() -> MeshJoinRequest {
        MeshJoinRequest {
            user_address: "0x1111111111111111111111111111111111111111".to_string(),
            device_id: "device-12345678".to_string(),
            chain_id: 43113,
            gossip_topic: "compose/global/v1".to_string(),
            announce_topic: "compose/announce/v1".to_string(),
            manifest_topic: "compose/manifest/v1".to_string(),
            conclave_topic: "compose/conclave/v1".to_string(),
            heartbeat_ms: 30_000,
            kad_protocol: default_kad_protocol(),
            session_id: "session-a".to_string(),
            bootstrap_multiaddrs: vec![
                "/ip4/64.225.35.57/tcp/4001/p2p/12D3KooWDdWJP82TKNbMemW5JtXR4qGrhE2tc455T9yZewEZ4rdD".to_string(),
            ],
            relay_multiaddrs: vec![
                "/dns4/mesh.do.lon1.compose.market/tcp/4002/ws/p2p/12D3KooWG22npb9WPpoehLSfo7xeEeEGDk7oH2Vw7dpy6pn77Cxr".to_string(),
            ],
            published_agents: vec![MeshPublishedAgent {
                agent_wallet: "0x1111111111111111111111111111111111111111".to_string(),
                dna_hash: "dna".to_string(),
                capabilities_hash: "search".to_string(),
                capabilities: vec!["search".to_string()],
                public_card: None,
            }],
            capabilities: vec!["search".to_string()],
        }
    }

    fn manifest_state_fixture(
        agent_wallet: &str,
        metadata_name: &str,
        installed_skill_enabled: bool,
        authored_skill_enabled: bool,
    ) -> serde_json::Value {
        serde_json::json!({
            "installedSkills": [
                {
                    "id": "skill:global",
                    "enabled": installed_skill_enabled
                }
            ],
            "installedAgents": [
                {
                    "agentWallet": agent_wallet,
                    "metadata": {
                        "name": metadata_name,
                        "description": "desc",
                        "model": "model-1",
                        "framework": "manowar",
                        "agentCardUri": "ipfs://card",
                        "endpoints": {
                            "chat": "https://chat.compose.market",
                            "stream": "https://stream.compose.market"
                        }
                    },
                    "lock": {
                        "chainId": 43113,
                        "modelId": "model-1",
                        "agentCardCid": "bafycard"
                    },
                    "mcpServers": ["memory"],
                    "skillStates": {
                        "skill:authored": {
                            "skillId": "skill:authored",
                            "enabled": authored_skill_enabled,
                            "eligible": true
                        }
                    },
                    "network": {
                        "enabled": true,
                        "publicCard": {
                            "name": metadata_name
                        },
                        "manifest": {
                            "agentWallet": agent_wallet
                        }
                    }
                }
            ]
        })
    }

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
    fn verify_mesh_manifest_signature_detects_tampering() {
        let local_key = identity::Keypair::generate_ed25519();
        let manifest = MeshManifest {
            agent_wallet: "0x1111111111111111111111111111111111111111".to_string(),
            user_wallet: "0x2222222222222222222222222222222222222222".to_string(),
            device_id: "device-12345678".to_string(),
            peer_id: local_key.public().to_peer_id().to_string(),
            chain_id: 43113,
            state_version: 3,
            state_root_hash: Some(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            ),
            pdp_piece_cid: None,
            pdp_anchored_at: None,
            name: "Test".to_string(),
            description: "Signed manifest".to_string(),
            model: "gpt-4.1".to_string(),
            framework: "manowar".to_string(),
            headline: "headline".to_string(),
            status_line: "status".to_string(),
            skills: vec!["skill:one".to_string()],
            mcp_servers: vec!["memory".to_string()],
            a2a_endpoints: vec!["https://chat.compose.market".to_string()],
            capabilities: vec!["search".to_string()],
            agent_card_uri: "ipfs://card".to_string(),
            listen_multiaddrs: vec![
                "/ip4/34.117.59.81/udp/4001/quic-v1/p2p/12D3KooWTestPeer".to_string()
            ],
            relay_peer_id: None,
            reputation_score: 0.0,
            total_conclaves: 0,
            successful_conclaves: 0,
            signed_at: 1_700_000_000_000,
            signature: String::new(),
        };

        let signed = sign_mesh_manifest(&local_key, &manifest).expect("sign manifest");
        super::verify_mesh_manifest_signature(&signed).expect("signature should verify");

        let mut tampered = signed.clone();
        tampered.name = "Tampered".to_string();
        let error = super::verify_mesh_manifest_signature(&tampered)
            .expect_err("tampered manifest should fail");
        assert!(error.contains("manifest signature verification failed"));
    }

    #[test]
    fn a509_with_reason_preserves_detail() {
        assert_eq!(
            super::a509_with_reason("stateRootHash does not match the latest anchored state"),
            "a509: stateRootHash does not match the latest anchored state"
        );
        assert_eq!(
            super::a509_with_reason("   "),
            super::A509_INCONSISTENT_AGENT_IDENTITY.to_string()
        );
    }

    #[test]
    fn decode_and_validate_envelope_rejects_replay_nonce() {
        let local_key = identity::Keypair::generate_ed25519();
        let request = mesh_join_request_fixture();
        let published = request.published_agents[0].clone();
        let payload = build_signed_envelope_payload(
            &local_key,
            &request,
            &published,
            "abc123",
            "announce",
            &local_key.public().to_peer_id().to_string(),
            &published.capabilities,
            &["/ip4/34.117.59.81/udp/4001/quic-v1".to_string()],
            "nonce-1".to_string(),
        )
        .expect("sign envelope");

        let mut seen_nonces = HashMap::new();
        let envelope =
            decode_and_validate_envelope(&payload, &mut seen_nonces).expect("decode envelope");
        assert_eq!(envelope.agent_wallet, published.agent_wallet);

        let error = decode_and_validate_envelope(&payload, &mut seen_nonces)
            .expect_err("replayed envelope should fail");
        assert_eq!(error, "replay envelope nonce");
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
    fn build_local_request_body_preserves_explicit_thread_id() {
        let agent = PersistedInstalledAgent {
            agent_wallet: "0x1111111111111111111111111111111111111111".to_string(),
            ..Default::default()
        };
        let identity = PersistedLocalIdentity {
            user_address: "0x2222222222222222222222222222222222222222".to_string(),
            device_id: "device-12345678".to_string(),
            ..Default::default()
        };

        let body = build_local_request_body(
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
    fn build_local_request_body_requires_explicit_thread_id() {
        let agent = PersistedInstalledAgent {
            agent_wallet: "0x1111111111111111111111111111111111111111".to_string(),
            ..Default::default()
        };
        let identity = PersistedLocalIdentity {
            user_address: "0x2222222222222222222222222222222222222222".to_string(),
            device_id: "device-12345678".to_string(),
            ..Default::default()
        };

        let error = build_local_request_body(
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
    fn load_local_state_value_from_path_repairs_trailing_bytes() {
        let path = std::env::temp_dir().join(format!(
            "compose-mesh-local-state-repair-{}-{}.json",
            std::process::id(),
            now_ms()
        ));

        fs::write(
            &path,
            r#"{"settings":{"apiUrl":"https://api.compose.market","meshEnabled":true}}d":true}}"#,
        )
        .expect("write corrupted local state");

        let (value, repaired) =
            load_local_state_value_from_path(&path).expect("repair corrupted local state");

        assert!(repaired);
        assert_eq!(
            value["settings"]["apiUrl"],
            serde_json::Value::String("https://api.compose.market".to_string())
        );
        assert_eq!(
            value["settings"]["meshEnabled"],
            serde_json::Value::Bool(true)
        );

        let repaired_raw = fs::read_to_string(&path).expect("read repaired local state");
        let repaired_value = serde_json::from_str::<serde_json::Value>(&repaired_raw)
            .expect("repaired local state should parse");
        assert_eq!(repaired_value, value);

        let _ = fs::remove_file(path);
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
    fn parse_local_agent_reply_unwraps_fenced_json() {
        let parsed = parse_local_agent_reply(
            "```json\n{\n  \"reply\": \"HEARTBEAT_OK\",\n  \"report\": \"\",\n  \"skill\": \"\",\n  \"actions\": []\n}\n```",
        );

        assert_eq!(parsed.reply, "HEARTBEAT_OK");
        assert!(parsed.report.is_none());
        assert!(parsed.skill.is_none());
        assert!(parsed.actions.is_empty());
    }

    #[test]
    fn parse_local_agent_reply_extracts_json_from_surrounding_text() {
        let parsed = parse_local_agent_reply(
            "I already did it.\n{\n  \"reply\": \"Done\",\n  \"report\": null,\n  \"skill\": null,\n  \"actions\": []\n}\nThanks.",
        );

        assert_eq!(parsed.reply, "Done");
        assert!(parsed.report.is_none());
        assert!(parsed.skill.is_none());
        assert!(parsed.actions.is_empty());
    }

    #[test]
    fn manifest_queue_runs_when_mesh_runtime_needs_anchorable_publication() {
        let agent_wallet = "0x1111111111111111111111111111111111111111";
        let state = manifest_state_fixture(agent_wallet, "Alpha", true, true);

        assert!(installed_agent_has_persisted_manifest(&state, agent_wallet));
        assert!(!manifest_republish_on_a509_requested(&state, agent_wallet));
        assert!(should_queue_manifest_publication(
            &state,
            agent_wallet,
            "mesh-runtime-online"
        ));

        let a509_state = serde_json::json!({
            "installedSkills": [],
            "installedAgents": [
                {
                    "agentWallet": agent_wallet,
                    "network": {
                        "enabled": true,
                        "manifest": {
                            "agentWallet": agent_wallet
                        },
                        "manifestRepublishOnA509": true
                    }
                }
            ]
        });
        assert!(manifest_republish_on_a509_requested(
            &a509_state,
            agent_wallet
        ));
        assert!(should_queue_manifest_publication(
            &a509_state,
            agent_wallet,
            "mesh-a509-reconcile"
        ));

        let first_publish_state = serde_json::json!({
            "installedSkills": [],
            "installedAgents": [
                {
                    "agentWallet": agent_wallet,
                    "network": {
                        "enabled": true
                    }
                }
            ]
        });
        assert!(!installed_agent_has_persisted_manifest(
            &first_publish_state,
            agent_wallet
        ));
        assert!(should_queue_manifest_publication(
            &first_publish_state,
            agent_wallet,
            "mesh-a509-reconcile"
        ));
    }

    #[test]
    fn mesh_runtime_online_does_not_queue_once_manifest_is_anchorable() {
        let agent_wallet = "0x1111111111111111111111111111111111111111";
        let state = serde_json::json!({
            "installedAgents": [
                {
                    "agentWallet": agent_wallet,
                    "network": {
                        "manifest": {
                            "agentWallet": agent_wallet,
                            "relayPeerId": "12D3KooWQzwPXPUEMPU1Upbo6trSiEo82rhtRpTJGr7SzP2gD7jb",
                            "listenMultiaddrs": [
                                "/ip4/206.189.203.231/tcp/4001/p2p/12D3KooWQzwPXPUEMPU1Upbo6trSiEo82rhtRpTJGr7SzP2gD7jb/p2p-circuit"
                            ]
                        },
                        "manifestSyncHash": "0x1",
                        "lastPublishedManifestSyncHash": "0x1"
                    }
                }
            ]
        });

        assert!(installed_agent_manifest_has_anchorable_transport(
            &state,
            agent_wallet
        ));
        assert!(!should_queue_manifest_publication(
            &state,
            agent_wallet,
            "mesh-runtime-online"
        ));
    }

    #[test]
    fn stale_relay_peer_id_without_anchorable_listen_multiaddrs_is_not_anchorable() {
        let agent_wallet = "0x1111111111111111111111111111111111111111";
        let state = serde_json::json!({
            "installedAgents": [
                {
                    "agentWallet": agent_wallet,
                    "network": {
                        "manifest": {
                            "agentWallet": agent_wallet,
                            "relayPeerId": "12D3KooWRelayPeer",
                            "listenMultiaddrs": [
                                "/ip4/127.0.0.1/tcp/55069",
                                "/ip4/192.168.1.6/udp/57451/quic-v1"
                            ]
                        }
                    }
                }
            ]
        });

        assert!(!installed_agent_manifest_has_anchorable_transport(
            &state,
            agent_wallet
        ));
    }

    #[test]
    fn non_runtime_manifest_reasons_never_queue_publication() {
        let agent_wallet = "0x1111111111111111111111111111111111111111";
        let state = manifest_state_fixture(agent_wallet, "Alpha", true, true);

        assert!(!should_queue_manifest_publication(
            &state,
            agent_wallet,
            "local-state-updated"
        ));
        assert!(!should_queue_manifest_publication(
            &state,
            agent_wallet,
            "local-agent-public-state-changed"
        ));
        assert!(!should_queue_manifest_publication(
            &state,
            agent_wallet,
            "local-agent-runtime-changed"
        ));
    }

    #[test]
    fn anchorable_path_detection_accepts_dns_and_circuit_multiaddrs() {
        assert!(listen_multiaddrs_have_anchorable_path(&[
            "/dns4/mesh.do.lon1.compose.market/tcp/4001/p2p/12D3KooWG22npb9WPpoehLSfo7xeEeEGDk7oH2Vw7dpy6pn77Cxr".to_string(),
        ]));
        assert!(listen_multiaddrs_have_anchorable_path(&[
            "/ip4/206.189.203.231/tcp/4001/p2p/12D3KooWPRcHjairRTQuXQdtUux5326pbuWyxsBrrDEzgLdbRKyh/p2p-circuit".to_string(),
        ]));
        assert!(!listen_multiaddrs_have_anchorable_path(&[
            "/ip4/34.117.59.81/udp/4001/quic-v1".to_string(),
            "/ip4/127.0.0.1/tcp/58534".to_string(),
        ]));
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
    fn derive_relay_listen_multiaddrs_prefers_one_transport_per_relay_peer() {
        let relay_multiaddrs = vec![
            "/dns4/mesh.do.lon1.compose.market/tcp/4002/ws/p2p/12D3KooWG22npb9WPpoehLSfo7xeEeEGDk7oH2Vw7dpy6pn77Cxr".to_string(),
            "/dns4/mesh.do.lon1.compose.market/tcp/4001/p2p/12D3KooWG22npb9WPpoehLSfo7xeEeEGDk7oH2Vw7dpy6pn77Cxr".to_string(),
            "/dns4/mesh.do.nyc1.compose.market/tcp/4001/p2p/12D3KooWPRcHjairRTQuXQdtUux5326pbuWyxsBrrDEzgLdbRKyh".to_string(),
            "/dns4/mesh.do.nyc1.compose.market/tcp/4002/ws/p2p/12D3KooWPRcHjairRTQuXQdtUux5326pbuWyxsBrrDEzgLdbRKyh".to_string(),
        ];

        let derived = derive_relay_listen_multiaddrs(&relay_multiaddrs)
            .into_iter()
            .map(|addr| addr.to_string())
            .collect::<Vec<_>>();

        assert_eq!(
            derived,
            vec![
                "/dns4/mesh.do.lon1.compose.market/tcp/4001/p2p/12D3KooWG22npb9WPpoehLSfo7xeEeEGDk7oH2Vw7dpy6pn77Cxr/p2p-circuit".to_string(),
                "/dns4/mesh.do.nyc1.compose.market/tcp/4001/p2p/12D3KooWPRcHjairRTQuXQdtUux5326pbuWyxsBrrDEzgLdbRKyh/p2p-circuit".to_string(),
            ]
        );
    }

    #[test]
    fn derive_relay_reservation_confirmed_multiaddrs_uses_only_the_accepted_relay() {
        let relay_multiaddrs = vec![
            "/dns4/mesh.do.lon1.compose.market/tcp/4001/p2p/12D3KooWG22npb9WPpoehLSfo7xeEeEGDk7oH2Vw7dpy6pn77Cxr".to_string(),
            "/dns4/mesh.do.ams3.compose.market/tcp/4001/p2p/12D3KooWQzwPXPUEMPU1Upbo6trSiEo82rhtRpTJGr7SzP2gD7jb".to_string(),
        ];
        let relay_peer_id =
            PeerId::from_str("12D3KooWQzwPXPUEMPU1Upbo6trSiEo82rhtRpTJGr7SzP2gD7jb").unwrap();
        let local_peer_id =
            PeerId::from_str("12D3KooWDsQfMcprTuDZDk8hdQba6qgKzBEU2CGWtBKdza3Gv5BV").unwrap();

        let derived = derive_relay_reservation_confirmed_multiaddrs(
            &relay_multiaddrs,
            &relay_peer_id,
            &local_peer_id,
        );

        assert_eq!(
            derived,
            vec![
                "/dns4/mesh.do.ams3.compose.market/tcp/4001/p2p/12D3KooWQzwPXPUEMPU1Upbo6trSiEo82rhtRpTJGr7SzP2gD7jb/p2p-circuit/p2p/12D3KooWDsQfMcprTuDZDk8hdQba6qgKzBEU2CGWtBKdza3Gv5BV".to_string(),
            ]
        );
    }

    #[test]
    fn ensure_multiaddr_has_peer_id_appends_missing_suffix() {
        let local_peer_id =
            PeerId::from_str("12D3KooWDsQfMcprTuDZDk8hdQba6qgKzBEU2CGWtBKdza3Gv5BV").unwrap();
        let addr = "/ip4/34.117.59.81/udp/4001/quic-v1"
            .parse::<Multiaddr>()
            .unwrap();

        assert_eq!(
            ensure_multiaddr_has_peer_id(&addr, &local_peer_id),
            "/ip4/34.117.59.81/udp/4001/quic-v1/p2p/12D3KooWDsQfMcprTuDZDk8hdQba6qgKzBEU2CGWtBKdza3Gv5BV"
        );
    }

    #[test]
    fn preferred_advertised_listen_multiaddrs_keeps_public_direct_paths_and_relay_fallbacks() {
        let preferred = preferred_advertised_listen_multiaddrs(&[
            "/ip4/34.117.59.81/udp/4001/quic-v1".to_string(),
            "/ip4/206.189.203.231/tcp/4001/p2p/12D3KooWPRcHjairRTQuXQdtUux5326pbuWyxsBrrDEzgLdbRKyh/p2p-circuit".to_string(),
        ]);

        assert_eq!(
            preferred,
            vec![
                "/ip4/34.117.59.81/udp/4001/quic-v1".to_string(),
                "/ip4/206.189.203.231/tcp/4001/p2p/12D3KooWPRcHjairRTQuXQdtUux5326pbuWyxsBrrDEzgLdbRKyh/p2p-circuit".to_string(),
            ]
        );
    }

    #[test]
    fn preferred_advertised_listen_multiaddrs_drops_private_direct_paths_when_relay_exists() {
        let preferred = preferred_advertised_listen_multiaddrs(&[
            "/ip4/127.0.0.1/tcp/58534".to_string(),
            "/ip4/192.168.1.6/udp/49488/quic-v1".to_string(),
            "/ip4/206.189.203.231/tcp/4001/p2p/12D3KooWPRcHjairRTQuXQdtUux5326pbuWyxsBrrDEzgLdbRKyh/p2p-circuit".to_string(),
        ]);

        assert_eq!(
            preferred,
            vec!["/ip4/206.189.203.231/tcp/4001/p2p/12D3KooWPRcHjairRTQuXQdtUux5326pbuWyxsBrrDEzgLdbRKyh/p2p-circuit".to_string()]
        );
    }

    #[test]
    fn preferred_advertised_listen_multiaddrs_falls_back_to_relays_when_needed() {
        let preferred = preferred_advertised_listen_multiaddrs(&[
            "/ip4/206.189.203.231/tcp/4001/p2p/12D3KooWPRcHjairRTQuXQdtUux5326pbuWyxsBrrDEzgLdbRKyh/p2p-circuit".to_string(),
            "/dns4/relay.do.lon1.compose.market/tcp/4002/ws/p2p/12D3KooWQzwPXPUEMPU1Upbo6trSiEo82rhtRpTJGr7SzP2gD7jb/p2p-circuit".to_string(),
        ]);

        assert_eq!(
            preferred,
            vec![
                "/ip4/206.189.203.231/tcp/4001/p2p/12D3KooWPRcHjairRTQuXQdtUux5326pbuWyxsBrrDEzgLdbRKyh/p2p-circuit".to_string(),
                "/dns4/relay.do.lon1.compose.market/tcp/4002/ws/p2p/12D3KooWQzwPXPUEMPU1Upbo6trSiEo82rhtRpTJGr7SzP2gD7jb/p2p-circuit".to_string(),
            ]
        );
    }

    #[test]
    fn extract_skill_name_from_markdown_strips_skill_prefix() {
        assert_eq!(
            extract_skill_name_from_markdown("# SKILL: Create Valuable Learning\n"),
            Some("Create Valuable Learning".to_string())
        );
    }

    #[test]
    fn markdown_represents_agent_skill_only_for_skill_headings() {
        assert!(markdown_represents_agent_skill(
            "# SKILL: Create Valuable Learning\n\nSteps..."
        ));
        assert!(!markdown_represents_agent_skill("# Notes\n\nNot a skill"));
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
    fn preferred_connected_relay_peer_id_follows_configured_relay_order() {
        let first =
            PeerId::from_str("12D3KooWG22npb9WPpoehLSfo7xeEeEGDk7oH2Vw7dpy6pn77Cxr").unwrap();
        let second =
            PeerId::from_str("12D3KooWQzwPXPUEMPU1Upbo6trSiEo82rhtRpTJGr7SzP2gD7jb").unwrap();
        let connected = HashSet::from([second, first]);
        let relay_multiaddrs = vec![
            "/dns4/mesh.do.lon1.compose.market/tcp/4001/p2p/12D3KooWG22npb9WPpoehLSfo7xeEeEGDk7oH2Vw7dpy6pn77Cxr".to_string(),
            "/dns4/mesh.do.ams3.compose.market/tcp/4001/p2p/12D3KooWQzwPXPUEMPU1Upbo6trSiEo82rhtRpTJGr7SzP2gD7jb".to_string(),
        ];
        let chosen = preferred_connected_relay_peer_id(&connected, &relay_multiaddrs);

        assert_eq!(
            chosen.as_deref(),
            Some("12D3KooWG22npb9WPpoehLSfo7xeEeEGDk7oH2Vw7dpy6pn77Cxr")
        );
    }

    #[test]
    fn default_kad_protocol_uses_local_namespace() {
        assert_eq!(default_kad_protocol(), MESH_KAD_PROTOCOL);
    }

    #[test]
    fn mesh_request_requires_restart_ignores_session_and_published_agent_churn() {
        let current = mesh_join_request_fixture();
        let mut next = current.clone();
        next.session_id = "session-b".to_string();
        next.published_agents[0].public_card = Some(super::MeshAgentCard {
            name: "Updated".to_string(),
            description: "Updated card".to_string(),
            model: "gpt-test".to_string(),
            framework: "compose".to_string(),
            headline: "Updated".to_string(),
            status_line: "Updated".to_string(),
            capabilities: vec!["search".to_string(), "memory".to_string()],
            updated_at: 2,
        });
        next.published_agents[0].capabilities = vec!["search".to_string(), "memory".to_string()];
        next.capabilities = vec!["search".to_string(), "memory".to_string()];
        next.heartbeat_ms = 45_000;

        assert!(!mesh_request_requires_restart(&current, &next));
    }

    #[test]
    fn mesh_request_requires_restart_for_transport_and_identity_changes() {
        let current = mesh_join_request_fixture();
        let mut next = current.clone();
        next.device_id = "device-87654321".to_string();

        assert!(mesh_request_requires_restart(&current, &next));
    }

    #[test]
    fn extract_bootstrap_and_rendezvous_peers_only_marks_relays_as_rendezvous_servers() {
        let request = MeshJoinRequest {
            user_address: "0x1111111111111111111111111111111111111111".to_string(),
            device_id: "device-12345678".to_string(),
            chain_id: 43113,
            gossip_topic: "compose/global/v1".to_string(),
            announce_topic: "compose/announce/v1".to_string(),
            manifest_topic: "compose/manifest/v1".to_string(),
            conclave_topic: "compose/conclave/v1".to_string(),
            heartbeat_ms: 30_000,
            kad_protocol: default_kad_protocol(),
            session_id: String::new(),
            bootstrap_multiaddrs: vec![
                "/ip4/64.225.35.57/tcp/4001/p2p/12D3KooWDdWJP82TKNbMemW5JtXR4qGrhE2tc455T9yZewEZ4rdD".to_string(),
            ],
            relay_multiaddrs: vec![
                "/dns4/mesh.do.lon1.compose.market/tcp/4002/ws/p2p/12D3KooWG22npb9WPpoehLSfo7xeEeEGDk7oH2Vw7dpy6pn77Cxr".to_string(),
            ],
            published_agents: vec![MeshPublishedAgent {
                agent_wallet: "0x1111111111111111111111111111111111111111".to_string(),
                dna_hash: "dna".to_string(),
                capabilities_hash: "search".to_string(),
                capabilities: vec!["search".to_string()],
                public_card: None,
            }],
            capabilities: vec!["search".to_string()],
        };

        let (bootstrap_multiaddrs, rendezvous_peers) =
            extract_bootstrap_and_rendezvous_peers(&request);

        assert_eq!(bootstrap_multiaddrs.len(), 2);
        assert_eq!(rendezvous_peers.len(), 1);
        assert!(rendezvous_peers.contains(
            &PeerId::from_str("12D3KooWG22npb9WPpoehLSfo7xeEeEGDk7oH2Vw7dpy6pn77Cxr")
                .expect("relay peer id"),
        ));
        assert!(!rendezvous_peers.contains(
            &PeerId::from_str("12D3KooWDdWJP82TKNbMemW5JtXR4qGrhE2tc455T9yZewEZ4rdD")
                .expect("bootstrap peer id"),
        ));
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

    #[test]
    fn mesh_a509_reconcile_requires_missing_manifest_or_retry_flag() {
        let no_manifest = serde_json::json!({
            "installedAgents": [
                {
                    "agentWallet": "0x1111111111111111111111111111111111111111",
                    "network": {}
                }
            ]
        });
        assert!(should_queue_manifest_publication(
            &no_manifest,
            "0x1111111111111111111111111111111111111111",
            "mesh-a509-reconcile",
        ));

        let with_manifest = serde_json::json!({
            "installedAgents": [
                {
                    "agentWallet": "0x1111111111111111111111111111111111111111",
                    "network": {
                        "manifest": {
                            "stateVersion": 3
                        }
                    }
                }
            ]
        });
        assert!(!should_queue_manifest_publication(
            &with_manifest,
            "0x1111111111111111111111111111111111111111",
            "mesh-a509-reconcile",
        ));

        let with_a509_retry = serde_json::json!({
            "installedAgents": [
                {
                    "agentWallet": "0x1111111111111111111111111111111111111111",
                    "network": {
                        "manifest": {
                            "stateVersion": 3
                        },
                        "manifestRepublishOnA509": true
                    }
                }
            ]
        });
        assert!(should_queue_manifest_publication(
            &with_a509_retry,
            "0x1111111111111111111111111111111111111111",
            "mesh-a509-reconcile",
        ));
    }

    #[test]
    fn non_a509_manifest_reasons_do_not_queue_with_existing_manifest() {
        let state = serde_json::json!({
            "installedAgents": [
                {
                    "agentWallet": "0x1111111111111111111111111111111111111111",
                    "network": {
                        "manifest": {
                            "stateVersion": 3
                        }
                    }
                }
            ]
        });

        assert!(!should_queue_manifest_publication(
            &state,
            "0x1111111111111111111111111111111111111111",
            "local-state-updated",
        ));
        assert!(!should_queue_manifest_publication(
            &state,
            "0x1111111111111111111111111111111111111111",
            "local-agent-runtime-changed",
        ));
    }

    #[test]
    fn manifest_retry_helpers_read_persisted_manifest_and_a509_flag() {
        let state = serde_json::json!({
            "installedAgents": [
                {
                    "agentWallet": "0x1111111111111111111111111111111111111111",
                    "network": {
                        "manifest": {
                            "stateVersion": 7
                        },
                        "manifestRepublishOnA509": true
                    }
                }
            ]
        });

        assert!(installed_agent_has_persisted_manifest(
            &state,
            "0x1111111111111111111111111111111111111111"
        ));
        assert!(manifest_republish_on_a509_requested(
            &state,
            "0x1111111111111111111111111111111111111111"
        ));
    }

    #[test]
    fn mesh_manifest_signature_round_trip_detects_tampering() {
        let local_key = identity::Keypair::generate_ed25519();
        let manifest = MeshManifest {
            agent_wallet: "0x1111111111111111111111111111111111111111".to_string(),
            user_wallet: "0x2222222222222222222222222222222222222222".to_string(),
            device_id: "device-12345678".to_string(),
            peer_id: local_key.public().to_peer_id().to_string(),
            chain_id: 43113,
            state_version: 7,
            state_root_hash: Some(format!("0x{}", "a".repeat(64))),
            pdp_piece_cid: Some("bafkreiatestpiececid".to_string()),
            pdp_anchored_at: Some(1_700_000_000_000),
            name: "Alpha".to_string(),
            description: "Test manifest".to_string(),
            model: "gpt-4.1".to_string(),
            framework: "manowar".to_string(),
            headline: "Alpha headline".to_string(),
            status_line: "Online".to_string(),
            skills: vec!["skill:alpha".to_string()],
            mcp_servers: vec!["memory".to_string()],
            a2a_endpoints: vec!["https://agent.compose.market/chat".to_string()],
            capabilities: vec!["search".to_string()],
            agent_card_uri: "ipfs://bafycard".to_string(),
            listen_multiaddrs: vec!["/ip4/34.117.59.81/udp/4001/quic-v1".to_string()],
            relay_peer_id: None,
            reputation_score: 0.0,
            total_conclaves: 0,
            successful_conclaves: 0,
            signed_at: 1_700_000_000_000,
            signature: String::new(),
        };

        let signed = sign_mesh_manifest(&local_key, &manifest).expect("sign manifest");
        super::verify_mesh_manifest_signature(&signed).expect("signature should verify");

        let mut tampered = signed.clone();
        tampered.name = "Beta".to_string();
        let error = super::verify_mesh_manifest_signature(&tampered)
            .expect_err("tampered manifest should fail");
        assert!(error.starts_with("a509:"));
    }

    #[test]
    fn decode_and_validate_envelope_rejects_replayed_nonce() {
        let local_key = identity::Keypair::generate_ed25519();
        let request = mesh_join_request_fixture();
        let published = request.published_agents[0].clone();
        let peer_id = local_key.public().to_peer_id().to_string();
        let hai_id = derive_hai_id(
            &published.agent_wallet,
            &request.user_address,
            &request.device_id,
        );
        let payload = build_signed_envelope_payload(
            &local_key,
            &request,
            &published,
            &hai_id,
            "presence",
            &peer_id,
            &published.capabilities,
            &["/ip4/34.117.59.81/udp/4001/quic-v1".to_string()],
            "nonce-1".to_string(),
        )
        .expect("sign envelope");

        let mut seen_nonces = HashMap::new();
        let decoded =
            decode_and_validate_envelope(&payload, &mut seen_nonces).expect("decode envelope");
        assert_eq!(decoded.nonce, "nonce-1");

        let error = decode_and_validate_envelope(&payload, &mut seen_nonces)
            .expect_err("replayed nonce should fail");
        assert_eq!(error, "replay envelope nonce");
    }
}
