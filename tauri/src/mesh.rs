use super::*;

pub(crate) const COMPOSE_SYNAPSE_COLLECTION: &str = "compose";
pub(crate) const A409_INCONSISTENT_AGENT_IDENTITY: &str = "a409: inconsistent agent identity";
pub(crate) const MESH_IDENTIFY_PROTOCOL: &str =
    concat!("/", env!("COMPOSE_MESH_PROTOCOL_NAMESPACE"));
pub(crate) const MESH_KAD_PROTOCOL: &str =
    concat!("/", env!("COMPOSE_MESH_PROTOCOL_NAMESPACE"), "/kad");
pub(crate) const MESH_MANIFEST_VERIFY_PROTOCOL: &str = concat!(
    "/",
    env!("COMPOSE_MESH_PROTOCOL_NAMESPACE"),
    "/manifest-verify"
);
#[derive(Default)]
pub(crate) struct MeshRuntimeState {
    status: Mutex<MeshRuntimeStatus>,
    stop_tx: Mutex<Option<oneshot::Sender<()>>>,
    command_tx: Mutex<Option<mpsc::UnboundedSender<MeshLoopCommand>>>,
    run_generation: Mutex<u64>,
    active_request: Mutex<Option<MeshJoinRequest>>,
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MeshAgentCard {
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
pub(crate) struct MeshManifest {
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
pub(crate) struct MeshManifestUnsigned {
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
pub(crate) struct MeshManifestVerificationRequest {
    hai_id: String,
    manifest: MeshManifest,
    latest_retrieval_url: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MeshManifestVerificationResponse {
    ok: bool,
    error: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MeshStateSnapshotRuntime {
    dna_hash: String,
    identity_hash: String,
    model_id: String,
    chain_id: u32,
    agent_card_cid: String,
    mcp_tools_hash: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MeshStateSnapshotManifest {
    skills: Vec<String>,
    capabilities: Vec<String>,
    mcp_servers: Vec<String>,
    a2a_endpoints: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MeshStateSnapshot {
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
pub(crate) struct MeshStateSnapshotRequest {
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
pub(crate) struct MeshStateAnchorCommandRequest {
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SignedMeshStateEnvelope {
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

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct LocalHaiState {
    version: u32,
    agent_wallet: String,
    #[serde(rename = "userAddress")]
    user_wallet: String,
    device_id: String,
    hai_id: String,
    synapse_session_private_key: String,
    next_update_number: u64,
    #[serde(default = "default_learning_number")]
    next_learning_number: u64,
    last_update_number: Option<u64>,
    last_learning_number: Option<u64>,
    last_anchor_path: Option<String>,
    last_learning_path: Option<String>,
    last_state_root_hash: Option<String>,
    last_anchor_piece_cid: Option<String>,
    last_learning_piece_cid: Option<String>,
    last_retrieval_url: Option<String>,
    last_anchored_at: Option<u64>,
    updated_at: u64,
}

pub(crate) fn default_learning_number() -> u64 {
    1
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MeshStateAnchorRuntimeResponse {
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
    source: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub(crate) enum MeshSharedArtifactKind {
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
pub(crate) enum MeshPublicationQueueKind {
    #[serde(rename = "manifest.publish")]
    ManifestPublish,
    #[serde(rename = "learning.pin")]
    LearningPin,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MeshPublicationQueueRequest {
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
pub(crate) struct MeshPublicationQueueResult {
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
pub(crate) struct PersistedLocalState {
    settings: PersistedLocalSettings,
    identity: Option<PersistedLocalIdentity>,
    installed_agents: Vec<PersistedInstalledAgent>,
    installed_skills: Vec<PersistedInstalledSkill>,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct PersistedLocalSettings {
    api_url: String,
    mesh_enabled: bool,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct PersistedLocalIdentity {
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
pub(crate) struct ActiveSessionRefreshResponse {
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
pub(crate) struct PersistedInstalledSkill {
    id: String,
    enabled: bool,
    relative_path: String,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct PersistedInstalledAgent {
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
    enabled: bool,
    #[serde(default = "default_agent_heartbeat_interval_ms")]
    interval_ms: u64,
    last_run_at: Option<u64>,
    last_result: Option<String>,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct PersistedAgentMetadata {
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
pub(crate) struct PersistedAgentEndpoints {
    chat: String,
    stream: String,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct PersistedAgentLock {
    agent_wallet: String,
    agent_card_cid: String,
    model_id: String,
    mcp_tools_hash: String,
    chain_id: u32,
    dna_hash: String,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct PersistedAgentNetworkState {
    enabled: bool,
    public_card: Option<MeshAgentCard>,
    manifest: Option<MeshManifest>,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct PersistedAgentSkillState {
    skill_id: String,
    enabled: bool,
    eligible: bool,
    source: String,
    revision: String,
    updated_at: Option<u64>,
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

pub(crate) struct PendingManifestVerification {
    agent_wallet: String,
    reply: Option<oneshot::Sender<Result<(), String>>>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MeshJoinRequest {
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

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MeshPublishedAgent {
    agent_wallet: String,
    dna_hash: String,
    capabilities_hash: String,
    capabilities: Vec<String>,
    public_card: Option<MeshAgentCard>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MeshPublishedAgentStatus {
    agent_wallet: String,
    hai_id: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MeshRuntimeStatus {
    running: bool,
    status: String,
    user_address: Option<String>,
    published_agents: Vec<MeshPublishedAgentStatus>,
    device_id: Option<String>,
    peer_id: Option<String>,
    listen_multiaddrs: Vec<String>,
    relay_peer_id: Option<String>,
    peers_discovered: u32,
    last_heartbeat_at: Option<u64>,
    last_error: Option<String>,
    updated_at: u64,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LocalUpdateCheckResult {
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
    relay_client: relay::client::Behaviour,
    dcutr: dcutr::Behaviour,
    autonat: autonat::Behaviour,
    mdns: mdns::tokio::Behaviour,
    ping: ping::Behaviour,
    identify: identify::Behaviour,
    gossipsub: gossipsub::Behaviour,
    manifest_verify: request_response::cbor::Behaviour<
        MeshManifestVerificationRequest,
        MeshManifestVerificationResponse,
    >,
    kad: kad::Behaviour<kad::store::MemoryStore>,
    rendezvous: rendezvous::client::Behaviour,
    connection_limits: connection_limits::Behaviour,
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

pub(crate) fn normalize_manifest_atom(raw: &str, max_len: usize) -> Option<String> {
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

pub(crate) fn normalize_manifest_atoms(
    values: &[String],
    max_items: usize,
    max_len: usize,
) -> Vec<String> {
    let mut out = values
        .iter()
        .filter_map(|value| normalize_manifest_atom(value, max_len))
        .collect::<Vec<_>>();
    out.sort();
    out.dedup();
    out.truncate(max_items);
    out
}

pub(crate) fn normalize_manifest_urls(values: &[String], max_items: usize) -> Vec<String> {
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

pub(crate) fn normalize_optional_hex_32(value: Option<String>) -> Result<Option<String>, String> {
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

pub(crate) fn normalize_state_root_hash_for_compare(value: &str) -> Option<String> {
    let trimmed = value.trim().to_lowercase();
    let normalized = trimmed.strip_prefix("0x").unwrap_or(&trimmed);
    if normalized.len() != 64 || !normalized.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    Some(normalized.to_string())
}

pub(crate) fn same_state_root_hash(previous: Option<&str>, next: &str) -> bool {
    let Some(previous_normalized) = previous.and_then(normalize_state_root_hash_for_compare) else {
        return false;
    };
    let Some(next_normalized) = normalize_state_root_hash_for_compare(next) else {
        return false;
    };
    previous_normalized == next_normalized
}

pub(crate) fn normalize_optional_cid(value: Option<String>) -> Option<String> {
    let trimmed = value.unwrap_or_default().trim().to_string();
    if trimmed.is_empty() {
        None
    } else {
        Some(truncate_string(trimmed, 256))
    }
}

pub(crate) fn normalize_multiaddr_strings(values: &[String]) -> Vec<String> {
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

pub(crate) fn hex_encode(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{:02x}", byte))
        .collect::<String>()
}

pub(crate) fn decode_hex_string(value: &str) -> Result<Vec<u8>, String> {
    let normalized = value.trim();
    if normalized.len() % 2 != 0 {
        return Err("hex string must have an even number of characters".to_string());
    }

    let mut out = Vec::with_capacity(normalized.len() / 2);
    let mut chars = normalized.chars();
    while let (Some(left), Some(right)) = (chars.next(), chars.next()) {
        let byte = u8::from_str_radix(format!("{left}{right}").as_str(), 16)
            .map_err(|_| "hex string contains invalid characters".to_string())?;
        out.push(byte);
    }
    Ok(out)
}

pub(crate) fn encode_hai_base36(mut value: u64) -> String {
    const ALPHABET: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut out = [b'0'; 6];
    for index in (0..6).rev() {
        out[index] = ALPHABET[(value % 36) as usize];
        value /= 36;
    }
    String::from_utf8_lossy(&out).to_string()
}

pub(crate) fn wallet_bytes(value: &str) -> [u8; 20] {
    let normalized = value.trim().trim_start_matches("0x");
    let mut out = [0u8; 20];
    for index in 0..20 {
        let start = index * 2;
        let end = start + 2;
        out[index] = u8::from_str_radix(&normalized[start..end], 16).unwrap_or(0);
    }
    out
}

pub(crate) fn derive_hai_id(agent_wallet: &str, user_address: &str, device_id: &str) -> String {
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

pub(crate) fn compose_hai_path(hai_id: &str, update_number: u64) -> String {
    format!("compose-{}-{}", hai_id, update_number)
}

pub(crate) fn learning_path_slug(title: &str) -> String {
    let slug = local_agent_slug(title);
    if slug == "skill" {
        "untitled".to_string()
    } else {
        truncate_string(slug, 80)
    }
}

pub(crate) fn learning_hai_path(hai_id: &str, title: &str, artifact_number: u64) -> String {
    format!(
        "compose-{}-{}-#{}",
        hai_id,
        learning_path_slug(title),
        artifact_number
    )
}

pub(crate) fn normalize_synapse_session_private_key(value: &str) -> Option<String> {
    let normalized = value.trim().to_lowercase();
    if normalized.len() != 66 || !normalized.starts_with("0x") {
        return None;
    }
    if normalized
        .chars()
        .skip(2)
        .all(|char| char.is_ascii_hexdigit())
    {
        Some(normalized)
    } else {
        None
    }
}

pub(crate) fn generate_synapse_session_private_key() -> String {
    let mut bytes = [0u8; 32];
    OsRng.fill_bytes(&mut bytes);
    format!("0x{}", hex_encode(&bytes))
}

pub(crate) fn local_hai_state_dir(app: &tauri::AppHandle) -> Result<PathBuf, String> {
    Ok(resolve_base_dir(app)?.join("mesh").join("hai"))
}

pub(crate) fn local_hai_state_path(
    app: &tauri::AppHandle,
    agent_wallet: &str,
    user_wallet: &str,
    device_id: &str,
) -> Result<PathBuf, String> {
    let agent_wallet = normalize_wallet(agent_wallet)
        .ok_or_else(|| "local HAI agentWallet must be a valid wallet address".to_string())?;
    let user_wallet = normalize_wallet(user_wallet)
        .ok_or_else(|| "local HAI userAddress must be a valid wallet address".to_string())?;
    let device_id = normalize_device_id(device_id)
        .ok_or_else(|| "local HAI deviceId format is invalid".to_string())?;
    let device_key = sha256_hex_string(device_id.as_str());
    Ok(local_hai_state_dir(app)?.join(format!(
        "{}__{}__{}.json",
        agent_wallet, user_wallet, device_key
    )))
}

pub(crate) fn normalize_local_hai_state(
    value: LocalHaiState,
    agent_wallet: &str,
    user_wallet: &str,
    device_id: &str,
) -> Result<LocalHaiState, String> {
    let agent_wallet = normalize_wallet(agent_wallet)
        .ok_or_else(|| "local HAI agentWallet must be a valid wallet address".to_string())?;
    let user_wallet = normalize_wallet(user_wallet)
        .ok_or_else(|| "local HAI userAddress must be a valid wallet address".to_string())?;
    let device_id = normalize_device_id(device_id)
        .ok_or_else(|| "local HAI deviceId format is invalid".to_string())?;

    Ok(LocalHaiState {
        version: 1,
        agent_wallet: agent_wallet.clone(),
        user_wallet: user_wallet.clone(),
        device_id: device_id.clone(),
        hai_id: derive_hai_id(&agent_wallet, &user_wallet, &device_id),
        synapse_session_private_key: normalize_synapse_session_private_key(
            &value.synapse_session_private_key,
        )
        .unwrap_or_else(generate_synapse_session_private_key),
        next_update_number: value.next_update_number.max(1),
        next_learning_number: value.next_learning_number.max(1),
        last_update_number: value.last_update_number.filter(|value| *value > 0),
        last_learning_number: value.last_learning_number.filter(|value| *value > 0),
        last_anchor_path: value
            .last_anchor_path
            .map(|path| path.trim().to_string())
            .filter(|path| !path.is_empty()),
        last_learning_path: value
            .last_learning_path
            .map(|path| path.trim().to_string())
            .filter(|path| !path.is_empty()),
        last_state_root_hash: value
            .last_state_root_hash
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty()),
        last_anchor_piece_cid: value
            .last_anchor_piece_cid
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty()),
        last_learning_piece_cid: value
            .last_learning_piece_cid
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty()),
        last_retrieval_url: value
            .last_retrieval_url
            .and_then(|value| normalize_persisted_url(value.as_str())),
        last_anchored_at: value.last_anchored_at.filter(|value| *value > 0),
        updated_at: now_ms(),
    })
}

pub(crate) fn save_local_hai_state(
    app: &tauri::AppHandle,
    value: &LocalHaiState,
) -> Result<(), String> {
    let path = local_hai_state_path(
        app,
        &value.agent_wallet,
        &value.user_wallet,
        &value.device_id,
    )?;
    let serialized = serde_json::to_string_pretty(value)
        .map_err(|err| format!("failed to encode local HAI state: {err}"))?;
    write_string_atomically(&path, &serialized, "local HAI state")
}

pub(crate) fn ensure_local_hai_state(
    app: &tauri::AppHandle,
    agent_wallet: &str,
    user_wallet: &str,
    device_id: &str,
) -> Result<LocalHaiState, String> {
    let path = local_hai_state_path(app, agent_wallet, user_wallet, device_id)?;
    let normalized = if path.exists() {
        let raw = fs::read_to_string(&path)
            .map_err(|err| format!("failed to read local HAI state: {err}"))?;
        let parsed = serde_json::from_str::<LocalHaiState>(&raw)
            .map_err(|err| format!("failed to parse local HAI state: {err}"))?;
        normalize_local_hai_state(parsed, agent_wallet, user_wallet, device_id)?
    } else {
        normalize_local_hai_state(
            LocalHaiState::default(),
            agent_wallet,
            user_wallet,
            device_id,
        )?
    };
    save_local_hai_state(app, &normalized)?;
    Ok(normalized)
}

pub(crate) fn record_local_hai_anchor(
    app: &tauri::AppHandle,
    state: &LocalHaiState,
    response: &MeshStateAnchorRuntimeResponse,
) -> Result<LocalHaiState, String> {
    let mut updated = state.clone();
    updated.hai_id = response.hai_id.clone();
    updated.next_update_number = response.update_number.saturating_add(1);
    updated.last_update_number = Some(response.update_number);
    updated.last_anchor_path = Some(response.path.clone());
    updated.last_state_root_hash = Some(response.state_root_hash.clone());
    updated.last_anchor_piece_cid = Some(response.pdp_piece_cid.clone());
    updated.last_retrieval_url = response
        .retrieval_url
        .clone()
        .and_then(|value| normalize_persisted_url(value.as_str()));
    updated.last_anchored_at = Some(response.pdp_anchored_at);
    updated.updated_at = now_ms();
    save_local_hai_state(app, &updated)?;
    Ok(updated)
}

pub(crate) fn record_local_hai_learning(
    app: &tauri::AppHandle,
    state: &LocalHaiState,
    response: &MeshSharedArtifactPinRuntimeResponse,
) -> Result<LocalHaiState, String> {
    let mut updated = state.clone();
    updated.hai_id = response.hai_id.clone();
    updated.next_learning_number = response.artifact_number.saturating_add(1);
    updated.last_learning_number = Some(response.artifact_number);
    updated.last_learning_path = Some(response.path.clone());
    updated.last_learning_piece_cid = Some(response.piece_cid.clone());
    updated.updated_at = now_ms();
    save_local_hai_state(app, &updated)?;
    Ok(updated)
}

pub(crate) fn persist_manifest_update(
    app: &tauri::AppHandle,
    manifest: &MeshManifest,
) -> Result<(), String> {
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

        let current_hash = agent
            .as_object()
            .ok_or_else(|| "installed agent entry must be a JSON object".to_string())
            .and_then(|object| {
                current_manifest_sync_hash(app, manifest.agent_wallet.as_str(), object)
            })?;

        if !agent.get("network").is_some_and(|entry| entry.is_object()) {
            agent["network"] = serde_json::json!({});
        }
        agent["network"]["manifest"] = serde_json::to_value(manifest)
            .map_err(|err| format!("failed to encode published manifest: {err}"))?;
        agent["network"]["manifestSyncHash"] = serde_json::Value::String(current_hash.clone());
        agent["network"]["lastPublishedManifestSyncHash"] = serde_json::Value::String(current_hash);
        agent["network"]["manifestRepublishOnA409"] = serde_json::Value::Bool(false);
        break;
    }

    save_local_state_value(app, &value)
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

pub(crate) fn sync_local_agent_workspace_manifest_state(
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

fn installed_agent_has_persisted_manifest(
    state_value: &serde_json::Value,
    agent_wallet: &str,
) -> bool {
    installed_agent_object(state_value, agent_wallet)
        .and_then(|agent| agent.get("network"))
        .and_then(|network| network.get("manifest"))
        .is_some_and(|manifest| manifest.is_object())
}

fn installed_agent_manifest_has_anchorable_transport(
    state_value: &serde_json::Value,
    agent_wallet: &str,
) -> bool {
    let Some(manifest) = installed_agent_object(state_value, agent_wallet)
        .and_then(|agent| agent.get("network"))
        .and_then(|network| network.get("manifest"))
    else {
        return false;
    };

    let listen_multiaddrs = manifest
        .get("listenMultiaddrs")
        .and_then(|value| value.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str())
                .map(|item| item.to_string())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    derive_relay_peer_id_from_listen_multiaddrs(&listen_multiaddrs).is_some()
        || listen_multiaddrs_have_anchorable_path(&listen_multiaddrs)
}

fn manifest_sync_hash(
    state_value: &serde_json::Value,
    agent_wallet: &str,
    key: &str,
) -> Option<String> {
    installed_agent_object(state_value, agent_wallet)
        .and_then(|agent| agent.get("network"))
        .and_then(|network| network.get(key))
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string())
}

fn manifest_publication_required(state_value: &serde_json::Value, agent_wallet: &str) -> bool {
    if !installed_agent_has_persisted_manifest(state_value, agent_wallet) {
        return true;
    }

    manifest_sync_hash(state_value, agent_wallet, "manifestSyncHash")
        != manifest_sync_hash(state_value, agent_wallet, "lastPublishedManifestSyncHash")
}

fn manifest_republish_on_a409_requested(
    state_value: &serde_json::Value,
    agent_wallet: &str,
) -> bool {
    installed_agent_object(state_value, agent_wallet)
        .and_then(|agent| agent.get("network"))
        .and_then(|network| network.get("manifestRepublishOnA409"))
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
}

fn set_manifest_republish_on_a409(
    app: &tauri::AppHandle,
    agent_wallet: &str,
    enabled: bool,
) -> Result<(), String> {
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
        if wallet.as_deref() != Some(agent_wallet) {
            continue;
        }

        if !agent.get("network").is_some_and(|entry| entry.is_object()) {
            agent["network"] = serde_json::json!({});
        }
        agent["network"]["manifestRepublishOnA409"] = serde_json::Value::Bool(enabled);
        break;
    }

    save_local_state_value(app, &value)
}

fn should_queue_manifest_publication(
    state_value: &serde_json::Value,
    agent_wallet: &str,
    reason: &str,
) -> bool {
    match reason {
        "mesh-runtime-online" => {
            manifest_publication_required(state_value, agent_wallet)
                || !installed_agent_manifest_has_anchorable_transport(state_value, agent_wallet)
        }
        "mesh-a409-reconcile" => {
            manifest_publication_required(state_value, agent_wallet)
                || manifest_republish_on_a409_requested(state_value, agent_wallet)
        }
        _ => false,
    }
}

pub(crate) fn preserve_internal_manifest_network_state(
    previous_state: &serde_json::Value,
    next_state: &mut serde_json::Value,
) {
    let Some(next_agents) = next_state
        .get_mut("installedAgents")
        .and_then(|value| value.as_array_mut())
    else {
        return;
    };

    for next_agent in next_agents.iter_mut() {
        let Some(next_wallet) = next_agent
            .get("agentWallet")
            .and_then(|value| value.as_str())
            .and_then(normalize_wallet)
        else {
            continue;
        };
        let Some(previous_agent) = installed_agent_object(previous_state, next_wallet.as_str())
        else {
            continue;
        };
        let Some(previous_network) = previous_agent
            .get("network")
            .and_then(|value| value.as_object())
        else {
            continue;
        };

        if !next_agent
            .get("network")
            .is_some_and(|value| value.is_object())
        {
            next_agent["network"] = serde_json::json!({});
        }
        let Some(next_network) = next_agent
            .get_mut("network")
            .and_then(|value| value.as_object_mut())
        else {
            continue;
        };

        for key in [
            "manifestSyncHash",
            "lastPublishedManifestSyncHash",
            "manifestRepublishOnA409",
        ] {
            if next_network.get(key).is_none() {
                if let Some(previous_value) = previous_network.get(key) {
                    next_network.insert(key.to_string(), previous_value.clone());
                }
            }
        }
    }
}

fn queue_manifest_publication_request(
    app: &tauri::AppHandle,
    agent_wallet: &str,
    reason: &str,
) -> Result<(), String> {
    let state_value = load_local_state_value(app)?;
    if !should_queue_manifest_publication(&state_value, agent_wallet, reason) {
        return Ok(());
    }

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

pub(crate) fn normalize_persisted_url(value: &str) -> Option<String> {
    let trimmed = truncate_string(value.to_string(), 256);
    let lower = trimmed.to_lowercase();
    if lower.starts_with("https://") || lower.starts_with("http://") {
        Some(trimmed)
    } else {
        None
    }
}

pub(crate) fn normalize_agent_card_uri(
    metadata_uri: &str,
    fallback_cid: &str,
) -> Result<String, String> {
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

pub(crate) fn extract_plugin_capabilities(values: &[serde_json::Value]) -> Vec<String> {
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

pub(crate) fn merge_mesh_skill_ids(
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

pub(crate) fn manifest_comparable_payload(manifest: &MeshManifest) -> serde_json::Value {
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

pub(crate) fn next_manifest_state_version(
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
pub(crate) struct MeshPubCtx {
    api_url: String,
    compose_key_token: String,
    user_wallet: String,
    device_id: String,
    chain_id: u32,
    target_synapse_expiry: u64,
    installed_skills: Vec<PersistedInstalledSkill>,
    agent: PersistedInstalledAgent,
}

pub(crate) async fn load_mesh_pub_ctx(
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

pub(crate) fn build_current_mesh_manifest_core(
    app: &tauri::AppHandle,
    user_wallet: &str,
    device_id: &str,
    chain_id: u32,
    installed_skills: &[PersistedInstalledSkill],
    agent: &PersistedInstalledAgent,
    live_status: &MeshRuntimeStatus,
) -> Result<(MeshManifest, MeshStateSnapshotRequest), String> {
    let normalized_agent_wallet = normalize_wallet(&agent.agent_wallet)
        .ok_or_else(|| "mesh publication request agentWallet is invalid".to_string())?;
    let workspace_state = load_manifest_workspace_state(app, agent)?;
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
    let skills = merge_mesh_skill_ids(installed_skills, &agent.skill_states);
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

    let advertised_listen_multiaddrs =
        preferred_advertised_listen_multiaddrs(&live_status.listen_multiaddrs);

    let mut manifest = MeshManifest {
        agent_wallet: normalized_agent_wallet.clone(),
        user_wallet: user_wallet.to_string(),
        device_id: device_id.to_string(),
        peer_id,
        chain_id,
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
        listen_multiaddrs: advertised_listen_multiaddrs.clone(),
        relay_peer_id: derive_relay_peer_id_from_listen_multiaddrs(&advertised_listen_multiaddrs),
        reputation_score: 0.0,
        total_conclaves: 0,
        successful_conclaves: 0,
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

    Ok((manifest, snapshot))
}

pub(crate) fn load_mesh_manifest_preview_inputs(
    app: &tauri::AppHandle,
    agent_wallet: &str,
) -> Result<
    (
        String,
        String,
        u32,
        Vec<PersistedInstalledSkill>,
        PersistedInstalledAgent,
    ),
    String,
> {
    let state = load_persisted_local_state(app)?;
    let identity = state
        .identity
        .clone()
        .ok_or_else(|| "local identity is required for mesh manifest verification".to_string())?;
    let user_wallet = normalize_wallet(&identity.user_address)
        .ok_or_else(|| "local identity userAddress is invalid".to_string())?;
    let device_id = normalize_device_id(&identity.device_id)
        .ok_or_else(|| "local identity deviceId is invalid".to_string())?;
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

    Ok((
        user_wallet,
        device_id,
        if identity.chain_id > 0 {
            identity.chain_id
        } else {
            agent.lock.chain_id
        },
        state.installed_skills,
        agent,
    ))
}

pub(crate) async fn build_current_mesh_publication(
    app: &tauri::AppHandle,
    agent_wallet: &str,
    live_status: &MeshRuntimeStatus,
) -> Result<(MeshManifest, MeshStateAnchorCommandRequest), String> {
    let ctx = load_mesh_pub_ctx(app, agent_wallet).await?;
    let (manifest, snapshot) = build_current_mesh_manifest_core(
        app,
        &ctx.user_wallet,
        &ctx.device_id,
        ctx.chain_id,
        &ctx.installed_skills,
        &ctx.agent,
        live_status,
    )?;
    let previous_manifest = ctx.agent.network.manifest.clone();

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

pub(crate) fn build_current_mesh_manifest_verification_request(
    app: &tauri::AppHandle,
    agent_wallet: &str,
    live_status: &MeshRuntimeStatus,
) -> Result<MeshManifestVerificationRequest, String> {
    let (user_wallet, device_id, chain_id, installed_skills, agent) =
        load_mesh_manifest_preview_inputs(app, agent_wallet)?;
    let (mut manifest, snapshot_request) = build_current_mesh_manifest_core(
        app,
        &user_wallet,
        &device_id,
        chain_id,
        &installed_skills,
        &agent,
        live_status,
    )?;
    let snapshot = normalize_mesh_state_snapshot_request(
        &MeshStateAnchorCommandRequest {
            api_url: String::new(),
            compose_key_token: String::new(),
            user_address: user_wallet.clone(),
            device_id: device_id.clone(),
            target_synapse_expiry: 0,
            snapshot: snapshot_request,
            previous_state_root_hash: None,
            previous_pdp_piece_cid: None,
            previous_pdp_anchored_at: None,
        },
        live_status,
    )?;
    let current_state_root_hash = sha256_hex_string(&canonical_snapshot_json(&snapshot)?);
    manifest.state_root_hash = Some(current_state_root_hash);
    manifest.state_version =
        next_manifest_state_version(agent.network.manifest.as_ref(), &manifest);

    let validated = validate_mesh_manifest(manifest, live_status)?;
    let signed = sign_mesh_manifest(&load_or_create_mesh_identity(app)?, &validated)?;
    let hai_state = ensure_local_hai_state(
        app,
        &signed.agent_wallet,
        &signed.user_wallet,
        &signed.device_id,
    )?;

    Ok(MeshManifestVerificationRequest {
        hai_id: hai_state.hai_id,
        manifest: signed,
        latest_retrieval_url: hai_state.last_retrieval_url.clone(),
    })
}

pub(crate) fn select_manifest_verification_peer(
    connected_peers: &HashSet<PeerId>,
    local_peer_id: &str,
) -> Option<PeerId> {
    connected_peers
        .iter()
        .copied()
        .find(|peer_id| peer_id.to_string() != local_peer_id)
}

pub(crate) fn start_manifest_verification_request(
    swarm: &mut Swarm<MeshBehaviour>,
    local_peer_id: &str,
    connected_peers: &HashSet<PeerId>,
    request: MeshManifestVerificationRequest,
    pending: &mut HashMap<request_response::OutboundRequestId, PendingManifestVerification>,
    agent_wallet: &str,
    reply: Option<oneshot::Sender<Result<(), String>>>,
) {
    let Some(peer_id) = select_manifest_verification_peer(connected_peers, local_peer_id) else {
        if let Some(reply) = reply {
            let _ = reply.send(Ok(()));
        }
        return;
    };

    let request_id = swarm
        .behaviour_mut()
        .manifest_verify
        .send_request(&peer_id, request);
    pending.insert(
        request_id,
        PendingManifestVerification {
            agent_wallet: agent_wallet.to_string(),
            reply,
        },
    );
}

pub(crate) fn normalize_mesh_state_snapshot_request(
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

pub(crate) fn canonical_snapshot_json(snapshot: &MeshStateSnapshot) -> Result<String, String> {
    serde_json::to_string(snapshot)
        .map_err(|err| format!("failed to encode canonical state snapshot: {err}"))
}

pub(crate) fn sha256_hex_string(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    hex_encode(&hasher.finalize())
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

pub(crate) fn normalize_manifest_publish_outcome(
    result: Result<gossipsub::MessageId, gossipsub::PublishError>,
    manifest: &MeshManifest,
) -> Result<MeshManifest, String> {
    match result {
        Ok(_) | Err(gossipsub::PublishError::InsufficientPeers) => Ok(manifest.clone()),
        Err(error) => Err(format!("manifest publish failed: {error}")),
    }
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

pub(crate) fn unsigned_manifest_bytes(value: &MeshManifestUnsigned) -> Result<Vec<u8>, String> {
    serde_cbor::to_vec(value).map_err(|err| format!("failed to encode unsigned manifest: {err}"))
}

pub(crate) fn sign_mesh_manifest(
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

pub(crate) fn a409_with_reason(_reason: &str) -> String {
    A409_INCONSISTENT_AGENT_IDENTITY.to_string()
}

pub(crate) fn is_a409_error(error: &str) -> bool {
    error.trim().to_lowercase().starts_with("a409:")
}

pub(crate) fn verify_mesh_manifest_signature(manifest: &MeshManifest) -> Result<(), String> {
    let peer_id = PeerId::from_str(&manifest.peer_id)
        .map_err(|err| format!("invalid manifest peerId: {err}"))?;
    let multihash = peer_id.as_ref();
    if multihash.code() != 0 {
        return Err(a409_with_reason(
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
        return Err(a409_with_reason("manifest signature verification failed"));
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
        return Err(a409_with_reason(
            "haiId does not match the manifest triplet",
        ));
    }

    let Some(current_state_root_hash) = request
        .manifest
        .state_root_hash
        .as_deref()
        .and_then(normalize_state_root_hash_for_compare)
    else {
        return Err(a409_with_reason(
            "stateRootHash is missing from the current manifest",
        ));
    };

    let Some(retrieval_url) = request
        .latest_retrieval_url
        .as_deref()
        .and_then(normalize_persisted_url)
    else {
        return Ok(());
    };

    let authority = match fetch_authoritative_mesh_state(retrieval_url.as_str()).await {
        Ok(value) => value,
        Err(_) => return Ok(()),
    };

    if authority.hai_id != derived_hai {
        return Err(a409_with_reason(
            "haiId does not match the latest anchored state",
        ));
    }
    if authority.agent_wallet != request.manifest.agent_wallet {
        return Err(a409_with_reason(
            "agentWallet does not match the latest anchored state",
        ));
    }
    if authority.user_wallet != request.manifest.user_wallet {
        return Err(a409_with_reason(
            "userAddress does not match the latest anchored state",
        ));
    }
    if authority.device_id != request.manifest.device_id {
        return Err(a409_with_reason(
            "deviceId does not match the latest anchored state",
        ));
    }
    if authority.chain_id != request.manifest.chain_id {
        return Err(a409_with_reason(
            "chainId does not match the latest anchored state",
        ));
    }
    if !same_state_root_hash(
        Some(authority.state_root_hash.as_str()),
        current_state_root_hash.as_str(),
    ) {
        return Err(a409_with_reason(
            "stateRootHash does not match the latest anchored state",
        ));
    }

    Ok(())
}

pub(crate) fn validate_mesh_manifest(
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

    let advertised_listen_multiaddrs = preferred_advertised_listen_multiaddrs(
        &normalize_multiaddr_strings(&status.listen_multiaddrs),
    );

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
        listen_multiaddrs: advertised_listen_multiaddrs.clone(),
        relay_peer_id: derive_relay_peer_id_from_listen_multiaddrs(&advertised_listen_multiaddrs),
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
                                                    A409_INCONSISTENT_AGENT_IDENTITY.to_string()
                                                }))
                                            };

                                            if let Err(error) = &result {
                                                let _ = append_daemon_log(
                                                    &app,
                                                    &pending.agent_wallet,
                                                    error,
                                                );
                                                if is_a409_error(error) {
                                                    let _ = queue_manifest_reconcile_after_a409(
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
pub(crate) async fn verify_manifest_with_mesh(
    app: &tauri::AppHandle,
    agent_wallet: &str,
) -> Result<(), String> {
    let mesh_state = app.state::<MeshRuntimeState>();
    let live_status = mesh_state
        .status
        .lock()
        .map_err(|_| "failed to read mesh runtime status".to_string())?
        .clone();
    if !live_status.running || !status_has_published_agent(&live_status, agent_wallet) {
        return Ok(());
    }

    let request =
        build_current_mesh_manifest_verification_request(app, agent_wallet, &live_status)?;
    let command_tx = mesh_state
        .command_tx
        .lock()
        .map_err(|_| "failed to read mesh command channel".to_string())?
        .clone()
        .ok_or_else(|| "mesh runtime command channel is unavailable".to_string())?;
    let (reply_tx, reply_rx) = oneshot::channel();
    command_tx
        .send(MeshLoopCommand::VerifyManifest {
            request,
            reply: reply_tx,
        })
        .map_err(|_| "mesh runtime is no longer accepting manifest verification".to_string())?;
    reply_rx
        .await
        .map_err(|_| "mesh runtime dropped the manifest verification response".to_string())?
}
pub(crate) fn queue_runtime_manifest_publications_if_ready(
    app: &tauri::AppHandle,
    request: &MeshJoinRequest,
    reason: &str,
) {
    let Some(live_status) = with_mesh_status(app, |status| status.clone()) else {
        return;
    };
    if !live_status.running
        || !listen_multiaddrs_have_anchorable_path(&preferred_advertised_listen_multiaddrs(
            &live_status.listen_multiaddrs,
        ))
    {
        return;
    }

    for published in &request.published_agents {
        if let Err(error) = queue_manifest_publication_request(app, &published.agent_wallet, reason)
        {
            eprintln!(
                "[mesh] failed to queue manifest publication for {}: {}",
                published.agent_wallet, error
            );
        }
    }
}

pub(crate) fn queue_manifest_reconcile_after_a409(
    app: &tauri::AppHandle,
    agent_wallet: &str,
) -> Result<(), String> {
    let normalized_wallet =
        normalize_wallet(agent_wallet).ok_or_else(|| "invalid agentWallet".to_string())?;
    let state_value = load_local_state_value(app)?;
    let already_requested =
        manifest_republish_on_a409_requested(&state_value, normalized_wallet.as_str());
    if !manifest_publication_required(&state_value, normalized_wallet.as_str())
        && !already_requested
    {
        return Ok(());
    }

    if !already_requested {
        let _ = append_daemon_log(
            app,
            normalized_wallet.as_str(),
            A409_INCONSISTENT_AGENT_IDENTITY,
        );
    }
    set_manifest_republish_on_a409(app, normalized_wallet.as_str(), true)?;
    queue_manifest_publication_request(app, normalized_wallet.as_str(), "mesh-a409-reconcile")
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

pub(crate) fn build_signed_state_envelope(
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
pub(crate) struct SignedMeshRequestEnvelope {
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
pub(crate) struct MeshLearningPayload {
    version: u32,
    kind: String,
    created_at: u64,
    title: String,
    summary: String,
    content: String,
    access_price_usdc: String,
    publisher_address: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MeshSharedArtifactPinRuntimeResponse {
    hai_id: String,
    artifact_kind: MeshSharedArtifactKind,
    artifact_number: u64,
    path: String,
    latest_alias: String,
    root_cid: String,
    piece_cid: String,
    collection: String,
}

pub(crate) fn signed_mesh_request_bytes(
    envelope: &SignedMeshRequestEnvelope,
) -> Result<String, String> {
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

pub(crate) fn build_signed_mesh_request_json(
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

pub(crate) fn build_learning_payload_json(
    request: &MeshPublicationQueueRequest,
    publisher_address: &str,
) -> Result<String, String> {
    let title = truncate_string(request.title.clone().unwrap_or_default(), 160);
    let summary = truncate_string(request.summary.clone().unwrap_or_default(), 280);
    let content = request
        .content
        .clone()
        .unwrap_or_default()
        .trim()
        .to_string();
    let access_price_usdc = request
        .access_price_usdc
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
    if access_price_usdc.is_empty() {
        return Err("mesh learning accessPriceUsdc is required".to_string());
    }

    serde_json::to_string(&MeshLearningPayload {
        version: 1,
        kind: "compose.mesh.learning".to_string(),
        created_at: now_ms(),
        title,
        summary,
        content,
        access_price_usdc,
        publisher_address: publisher_address.to_string(),
    })
    .map_err(|err| format!("failed to encode mesh learning payload: {err}"))
}

pub(crate) async fn runtime_error(route: &str, response: reqwest::Response) -> String {
    let status = response.status();
    let body = response.text().await.unwrap_or_else(|_| String::new());
    if status.as_u16() == 409 {
        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&body) {
            if let Some(error) = parsed.get("error").and_then(|value| value.as_str()) {
                let trimmed = error.trim();
                if !trimmed.is_empty() {
                    return trimmed.to_string();
                }
            }
        }
        let trimmed = body.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
        return A409_INCONSISTENT_AGENT_IDENTITY.to_string();
    }
    if body.trim().is_empty() {
        format!("{route} failed: HTTP {status}")
    } else {
        format!("{route} failed: HTTP {status}: {body}")
    }
}

pub(crate) async fn anchor_mesh_state_via_local_runtime(
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

pub(crate) async fn pin_mesh_learning_via_local_runtime(
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

pub(crate) async fn anchor_mesh_state_from_command(
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
    let hai_state = ensure_local_hai_state(
        app,
        &snapshot.agent_wallet,
        &snapshot.user_wallet,
        &snapshot.device_id,
    )?;

    let (canonical_snapshot_json, state_root_hash, envelope_json) = build_signed_state_envelope(
        &load_or_create_mesh_identity(app)?,
        &snapshot,
        &hai_state.hai_id,
        hai_state.next_update_number,
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
        let last_update_number = hai_state
            .last_update_number
            .unwrap_or_else(|| hai_state.next_update_number.saturating_sub(1));
        let last_path = hai_state
            .last_anchor_path
            .clone()
            .unwrap_or_else(|| compose_hai_path(&hai_state.hai_id, last_update_number));
        let last_piece_cid = request
            .previous_pdp_piece_cid
            .clone()
            .or_else(|| hai_state.last_anchor_piece_cid.clone())
            .ok_or_else(|| "previous PDP piece CID is required for a skipped anchor".to_string())?;
        let last_anchored_at = request
            .previous_pdp_anchored_at
            .or(hai_state.last_anchored_at)
            .ok_or_else(|| {
                "previous PDP anchor timestamp is required for a skipped anchor".to_string()
            })?;

        return Ok(MeshStateAnchorRuntimeResponse {
            hai_id: hai_state.hai_id.clone(),
            update_number: last_update_number,
            path: last_path,
            file_name: compose_hai_path(&hai_state.hai_id, last_update_number),
            latest_alias: format!("compose-{}:latest", hai_state.hai_id),
            state_root_hash,
            pdp_piece_cid: last_piece_cid,
            pdp_anchored_at: last_anchored_at,
            payload_size: envelope_json.len(),
            provider_id: String::new(),
            data_set_id: None,
            piece_id: None,
            retrieval_url: None,
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
            "haiId": hai_state.hai_id,
            "updateNumber": hai_state.next_update_number,
            "path": compose_hai_path(&hai_state.hai_id, hai_state.next_update_number),
            "canonicalSnapshotJson": canonical_snapshot_json,
            "stateRootHash": state_root_hash,
            "envelopeJson": envelope_json,
            "sessionKeyPrivateKey": hai_state.synapse_session_private_key,
        }),
    )
    .await?;

    let _ = record_local_hai_anchor(app, &hai_state, &response)?;
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::{
        apply_active_session_refresh, build_local_runtime_request_body,
        build_signed_envelope_payload, compose_hai_path, decode_and_validate_envelope,
        default_kad_protocol, derive_hai_id, derive_relay_listen_multiaddrs,
        derive_relay_peer_id_from_listen_multiaddrs, derive_relay_reservation_confirmed_multiaddrs,
        ensure_multiaddr_has_peer_id, extract_bootstrap_and_rendezvous_peers,
        extract_skill_name_from_markdown, installed_agent_has_persisted_manifest,
        installed_agent_manifest_has_anchorable_transport, listen_multiaddrs_have_anchorable_path,
        load_local_state_value_from_path, manifest_republish_on_a409_requested,
        markdown_represents_agent_skill, mesh_request_requires_restart,
        normalize_daemon_state_for_local_mode, normalize_local_state_json,
        normalize_manifest_publish_outcome, normalize_mesh_api_url_with_loopback_policy,
        normalize_state_root_hash_for_compare, now_ms, parse_local_agent_reply,
        preferred_advertised_listen_multiaddrs, preferred_connected_relay_peer_id,
        remote_action_path_allowed, same_state_root_hash, should_queue_manifest_publication,
        sign_mesh_manifest, verify_mesh_manifest_signature, write_string_atomically,
        ActiveSessionRefreshResponse, DaemonAgentState, DaemonPermissionPolicy, DaemonStateFile,
        MeshJoinRequest, MeshManifest, MeshPublishedAgent, PersistedInstalledAgent,
        PersistedLocalIdentity, MESH_KAD_PROTOCOL,
    };
    use crate::gossipsub;
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
        verify_mesh_manifest_signature(&signed).expect("signature should verify");

        let mut tampered = signed.clone();
        tampered.name = "Tampered".to_string();
        let error =
            verify_mesh_manifest_signature(&tampered).expect_err("tampered manifest should fail");
        assert!(error.contains("manifest signature verification failed"));
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
        assert!(!manifest_republish_on_a409_requested(&state, agent_wallet));
        assert!(should_queue_manifest_publication(
            &state,
            agent_wallet,
            "mesh-runtime-online"
        ));

        let a409_state = serde_json::json!({
            "installedSkills": [],
            "installedAgents": [
                {
                    "agentWallet": agent_wallet,
                    "network": {
                        "enabled": true,
                        "manifest": {
                            "agentWallet": agent_wallet
                        },
                        "manifestRepublishOnA409": true
                    }
                }
            ]
        });
        assert!(manifest_republish_on_a409_requested(
            &a409_state,
            agent_wallet
        ));
        assert!(should_queue_manifest_publication(
            &a409_state,
            agent_wallet,
            "mesh-a409-reconcile"
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
            "mesh-a409-reconcile"
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
    fn mesh_a409_reconcile_requires_missing_manifest_or_retry_flag() {
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
            "mesh-a409-reconcile",
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
            "mesh-a409-reconcile",
        ));

        let with_a409_retry = serde_json::json!({
            "installedAgents": [
                {
                    "agentWallet": "0x1111111111111111111111111111111111111111",
                    "network": {
                        "manifest": {
                            "stateVersion": 3
                        },
                        "manifestRepublishOnA409": true
                    }
                }
            ]
        });
        assert!(should_queue_manifest_publication(
            &with_a409_retry,
            "0x1111111111111111111111111111111111111111",
            "mesh-a409-reconcile",
        ));
    }

    #[test]
    fn non_a409_manifest_reasons_do_not_queue_with_existing_manifest() {
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
    fn manifest_retry_helpers_read_persisted_manifest_and_a409_flag() {
        let state = serde_json::json!({
            "installedAgents": [
                {
                    "agentWallet": "0x1111111111111111111111111111111111111111",
                    "network": {
                        "manifest": {
                            "stateVersion": 7
                        },
                        "manifestRepublishOnA409": true
                    }
                }
            ]
        });

        assert!(installed_agent_has_persisted_manifest(
            &state,
            "0x1111111111111111111111111111111111111111"
        ));
        assert!(manifest_republish_on_a409_requested(
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
        verify_mesh_manifest_signature(&signed).expect("signature should verify");

        let mut tampered = signed.clone();
        tampered.name = "Beta".to_string();
        let error =
            verify_mesh_manifest_signature(&tampered).expect_err("tampered manifest should fail");
        assert!(error.starts_with("a409:"));
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

pub(crate) async fn publish_mesh_manifest_from_command(
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

pub(crate) fn write_mesh_publication_result(
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

fn required_mesh_learning_field(value: Option<&str>, field: &str) -> Result<String, String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| format!("{field} is required"))
}

pub(crate) async fn publish_learning_from_local_agent_action(
    app: &tauri::AppHandle,
    agent_wallet: &str,
    title: Option<&str>,
    summary: Option<&str>,
    content: Option<&str>,
    access_price_usdc: Option<&str>,
) -> Result<serde_json::Value, String> {
    verify_manifest_with_mesh(app, agent_wallet).await?;

    let request = MeshPublicationQueueRequest {
        request_id: format!("learning-{}-{}", now_ms(), local_agent_slug(agent_wallet)),
        kind: MeshPublicationQueueKind::LearningPin,
        agent_wallet: agent_wallet.to_string(),
        requested_at: now_ms(),
        reason: Some("agent-authored-learning".to_string()),
        title: Some(required_mesh_learning_field(title, "title")?),
        summary: Some(required_mesh_learning_field(summary, "summary")?),
        content: Some(required_mesh_learning_field(content, "content")?),
        access_price_usdc: Some(required_mesh_learning_field(
            access_price_usdc,
            "accessPriceUsdc",
        )?),
    };
    let result = process_mesh_publication_request(app, request).await;
    let _ = write_mesh_publication_result(app, &result);
    if !result.success {
        return Err(result
            .error
            .clone()
            .unwrap_or_else(|| "learning publication failed".to_string()));
    }
    serde_json::to_value(&result)
        .map_err(|err| format!("failed to encode learning publication result: {err}"))
}

pub(crate) async fn process_mesh_learning_request(
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

    let learning_title = request
        .title
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("untitled");
    let _ = append_daemon_log(
        app,
        &requested_wallet,
        &format!("learning pin started: {learning_title}"),
    );

    let ctx = load_mesh_pub_ctx(app, &requested_wallet).await?;
    let runtime_status = ensure_local_runtime_host(app, runtime_host)?;
    let hai_state =
        ensure_local_hai_state(app, &requested_wallet, &ctx.user_wallet, &ctx.device_id)?;

    let payload_json = build_learning_payload_json(request, &ctx.user_wallet)?;
    let artifact_kind = MeshSharedArtifactKind::Learning;
    let artifact_number = hai_state.next_learning_number;
    let path = learning_hai_path(&hai_state.hai_id, learning_title, artifact_number);
    let signed_request_json = build_signed_mesh_request_json(
        &load_or_create_mesh_identity(app)?,
        live_status,
        &requested_wallet,
        &ctx.user_wallet,
        &ctx.device_id,
        &hai_state.hai_id,
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
            "targetSessionExpiry": ctx.target_synapse_expiry,
            "signedRequestJson": signed_request_json,
            "haiId": hai_state.hai_id,
            "artifactKind": artifact_kind,
            "artifactNumber": artifact_number,
            "path": path,
            "payloadJson": payload_json,
            "filecoinPinSessionKeyPrivateKey": hai_state.synapse_session_private_key,
        }),
    )
    .await?;
    let _ = record_local_hai_learning(app, &hai_state, &response)?;
    let _ = append_daemon_log(
        app,
        &requested_wallet,
        &format!(
            "learning pinned: path={} rootCid={} pieceCid={}",
            response.path, response.root_cid, response.piece_cid,
        ),
    );

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

pub(crate) async fn process_mesh_publication_request(
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
                let normalized_wallet = normalize_wallet(&request.agent_wallet)
                    .unwrap_or_else(|| request.agent_wallet.to_lowercase());
                let should_republish = error.to_lowercase().starts_with("a409:");
                let _ = set_manifest_republish_on_a409(app, &normalized_wallet, should_republish);
                let _ = append_daemon_log(
                    app,
                    &request.agent_wallet,
                    &format!("manifest publish failed: {error}"),
                );
            } else if matches!(request.kind, MeshPublicationQueueKind::LearningPin) {
                let _ = append_daemon_log(
                    app,
                    &request.agent_wallet,
                    &format!("learning pin failed: {error}"),
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

pub(crate) async fn process_pending_mesh_publication_requests(
    app: &tauri::AppHandle,
) -> Result<(), String> {
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
