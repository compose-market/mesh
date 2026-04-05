use super::*;
use crate::mesh::*;

pub(crate) const COMPOSE_SYNAPSE_COLLECTION: &str = "compose";
pub(crate) const A509_INCONSISTENT_AGENT_IDENTITY: &str = "a509: inconsistent agent identity";
pub(crate) const MESH_MANIFEST_VERIFY_PROTOCOL: &str = concat!(
    "/",
    env!("COMPOSE_MESH_PROTOCOL_NAMESPACE"),
    "/manifest-verify"
);

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MeshManifest {
    pub(crate) agent_wallet: String,
    #[serde(rename = "userAddress")]
    pub(crate) user_wallet: String,
    pub(crate) device_id: String,
    pub(crate) peer_id: String,
    pub(crate) chain_id: u32,
    pub(crate) state_version: u64,
    pub(crate) state_root_hash: Option<String>,
    pub(crate) pdp_piece_cid: Option<String>,
    pub(crate) pdp_anchored_at: Option<u64>,
    pub(crate) name: String,
    pub(crate) description: String,
    pub(crate) model: String,
    pub(crate) framework: String,
    pub(crate) headline: String,
    pub(crate) status_line: String,
    pub(crate) skills: Vec<String>,
    pub(crate) mcp_servers: Vec<String>,
    pub(crate) a2a_endpoints: Vec<String>,
    pub(crate) capabilities: Vec<String>,
    pub(crate) agent_card_uri: String,
    pub(crate) listen_multiaddrs: Vec<String>,
    pub(crate) relay_peer_id: Option<String>,
    pub(crate) reputation_score: f64,
    pub(crate) total_conclaves: u64,
    pub(crate) successful_conclaves: u64,
    pub(crate) signed_at: u64,
    pub(crate) signature: String,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MeshManifestUnsigned {
    pub(crate) agent_wallet: String,
    #[serde(rename = "userAddress")]
    pub(crate) user_wallet: String,
    pub(crate) device_id: String,
    pub(crate) peer_id: String,
    pub(crate) chain_id: u32,
    pub(crate) state_version: u64,
    pub(crate) state_root_hash: Option<String>,
    pub(crate) pdp_piece_cid: Option<String>,
    pub(crate) pdp_anchored_at: Option<u64>,
    pub(crate) name: String,
    pub(crate) description: String,
    pub(crate) model: String,
    pub(crate) framework: String,
    pub(crate) headline: String,
    pub(crate) status_line: String,
    pub(crate) skills: Vec<String>,
    pub(crate) mcp_servers: Vec<String>,
    pub(crate) a2a_endpoints: Vec<String>,
    pub(crate) capabilities: Vec<String>,
    pub(crate) agent_card_uri: String,
    pub(crate) listen_multiaddrs: Vec<String>,
    pub(crate) relay_peer_id: Option<String>,
    pub(crate) reputation_score: f64,
    pub(crate) total_conclaves: u64,
    pub(crate) successful_conclaves: u64,
    pub(crate) signed_at: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MeshManifestVerificationRequest {
    pub(crate) hai_id: String,
    pub(crate) manifest: MeshManifest,
    pub(crate) latest_retrieval_url: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MeshManifestVerificationResponse {
    pub(crate) ok: bool,
    pub(crate) error: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MeshStateSnapshotRuntime {
    pub(crate) dna_hash: String,
    pub(crate) identity_hash: String,
    pub(crate) model_id: String,
    pub(crate) chain_id: u32,
    pub(crate) agent_card_cid: String,
    pub(crate) mcp_tools_hash: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MeshStateSnapshotManifest {
    pub(crate) skills: Vec<String>,
    pub(crate) capabilities: Vec<String>,
    pub(crate) mcp_servers: Vec<String>,
    pub(crate) a2a_endpoints: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MeshStateSnapshot {
    pub(crate) version: u64,
    pub(crate) created_at: u64,
    pub(crate) agent_wallet: String,
    #[serde(rename = "userAddress")]
    pub(crate) user_wallet: String,
    pub(crate) device_id: String,
    pub(crate) peer_id: String,
    pub(crate) runtime: MeshStateSnapshotRuntime,
    pub(crate) manifest: MeshStateSnapshotManifest,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MeshStateSnapshotRequest {
    pub(crate) agent_wallet: String,
    pub(crate) chain_id: u32,
    pub(crate) peer_id: String,
    pub(crate) model_id: String,
    pub(crate) dna_hash: String,
    pub(crate) identity_hash: String,
    pub(crate) agent_card_cid: String,
    pub(crate) mcp_tools_hash: String,
    pub(crate) skills: Vec<String>,
    pub(crate) capabilities: Vec<String>,
    pub(crate) mcp_servers: Vec<String>,
    pub(crate) a2a_endpoints: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MeshStateAnchorCommandRequest {
    pub(crate) api_url: String,
    pub(crate) compose_key_token: String,
    pub(crate) user_address: String,
    pub(crate) device_id: String,
    pub(crate) target_synapse_expiry: u64,
    pub(crate) snapshot: MeshStateSnapshotRequest,
    pub(crate) previous_state_root_hash: Option<String>,
    pub(crate) previous_pdp_piece_cid: Option<String>,
    pub(crate) previous_pdp_anchored_at: Option<u64>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SignedMeshStateEnvelope {
    pub(crate) version: u32,
    pub(crate) kind: String,
    pub(crate) collection: String,
    pub(crate) hai_id: String,
    pub(crate) update_number: u64,
    pub(crate) path: String,
    pub(crate) peer_id: String,
    pub(crate) agent_wallet: String,
    #[serde(rename = "userAddress")]
    pub(crate) user_wallet: String,
    pub(crate) device_id: String,
    pub(crate) chain_id: u32,
    pub(crate) signed_at: u64,
    pub(crate) state_root_hash: String,
    pub(crate) snapshot: MeshStateSnapshot,
    pub(crate) signature: String,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct LocalHaiState {
    pub(crate) version: u32,
    pub(crate) agent_wallet: String,
    #[serde(rename = "userAddress")]
    pub(crate) user_wallet: String,
    pub(crate) device_id: String,
    pub(crate) hai_id: String,
    pub(crate) synapse_session_private_key: String,
    pub(crate) next_update_number: u64,
    pub(crate) last_update_number: Option<u64>,
    pub(crate) last_anchor_path: Option<String>,
    pub(crate) last_state_root_hash: Option<String>,
    pub(crate) last_anchor_piece_cid: Option<String>,
    pub(crate) last_retrieval_url: Option<String>,
    pub(crate) last_anchored_at: Option<u64>,
    pub(crate) updated_at: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MeshStateAnchorRuntimeResponse {
    pub(crate) hai_id: String,
    pub(crate) update_number: u64,
    pub(crate) path: String,
    pub(crate) file_name: String,
    pub(crate) latest_alias: String,
    pub(crate) state_root_hash: String,
    pub(crate) pdp_piece_cid: String,
    pub(crate) pdp_anchored_at: u64,
    pub(crate) payload_size: usize,
    pub(crate) provider_id: String,
    pub(crate) data_set_id: Option<String>,
    pub(crate) piece_id: Option<String>,
    pub(crate) retrieval_url: Option<String>,
    pub(crate) source: String,
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
        last_update_number: value.last_update_number.filter(|value| *value > 0),
        last_anchor_path: value
            .last_anchor_path
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
        agent["network"]["manifestRepublishOnA509"] = serde_json::Value::Bool(false);
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

pub(crate) fn installed_agent_has_persisted_manifest(
    state_value: &serde_json::Value,
    agent_wallet: &str,
) -> bool {
    installed_agent_object(state_value, agent_wallet)
        .and_then(|agent| agent.get("network"))
        .and_then(|network| network.get("manifest"))
        .is_some_and(|manifest| manifest.is_object())
}

pub(crate) fn installed_agent_manifest_has_anchorable_transport(
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

pub(crate) fn manifest_republish_on_a509_requested(
    state_value: &serde_json::Value,
    agent_wallet: &str,
) -> bool {
    installed_agent_object(state_value, agent_wallet)
        .and_then(|agent| agent.get("network"))
        .and_then(|network| network.get("manifestRepublishOnA509"))
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
}

pub(crate) fn set_manifest_republish_on_a509(
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
        agent["network"]["manifestRepublishOnA509"] = serde_json::Value::Bool(enabled);
        break;
    }

    save_local_state_value(app, &value)
}

pub(crate) fn should_queue_manifest_publication(
    state_value: &serde_json::Value,
    agent_wallet: &str,
    reason: &str,
) -> bool {
    match reason {
        "mesh-runtime-online" => {
            manifest_publication_required(state_value, agent_wallet)
                || !installed_agent_manifest_has_anchorable_transport(state_value, agent_wallet)
        }
        "mesh-a509-reconcile" => {
            manifest_publication_required(state_value, agent_wallet)
                || manifest_republish_on_a509_requested(state_value, agent_wallet)
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
            "manifestRepublishOnA509",
        ] {
            if next_network.get(key).is_none() {
                if let Some(previous_value) = previous_network.get(key) {
                    next_network.insert(key.to_string(), previous_value.clone());
                }
            }
        }
    }
}

pub(crate) fn queue_manifest_publication_request(
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
    pub(crate) api_url: String,
    pub(crate) compose_key_token: String,
    pub(crate) user_wallet: String,
    pub(crate) device_id: String,
    pub(crate) chain_id: u32,
    pub(crate) target_synapse_expiry: u64,
    pub(crate) installed_skills: Vec<PersistedInstalledSkill>,
    pub(crate) agent: PersistedInstalledAgent,
}

pub(crate) struct PendingManifestVerification {
    pub(crate) agent_wallet: String,
    pub(crate) reply: Option<oneshot::Sender<Result<(), String>>>,
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

pub(crate) fn normalize_manifest_publish_outcome(
    result: Result<gossipsub::MessageId, gossipsub::PublishError>,
    manifest: &MeshManifest,
) -> Result<MeshManifest, String> {
    match result {
        Ok(_) | Err(gossipsub::PublishError::InsufficientPeers) => Ok(manifest.clone()),
        Err(error) => Err(format!("manifest publish failed: {error}")),
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
pub(crate) fn a509_with_reason(_reason: &str) -> String {
    let trimmed = _reason.trim();
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

fn normalize_agent_card_uri_for_compare(value: &str) -> String {
    value.trim().trim_end_matches('/').to_string()

fn extract_agent_card_cid(agent_card_uri: &str) -> Result<String, String> {
    let trimmed = agent_card_uri.trim();
    let cid = trimmed
        .strip_prefix("ipfs://")
        .unwrap_or(trimmed)
        .trim_start_matches('/')
        .split('/')
        .next()
        .unwrap_or("")
        .trim();
    if cid.is_empty() {
        return Err("agentCardUri is invalid".to_string());
    }
    Ok(cid.to_string())
}

fn agent_card_gateway_urls() -> Vec<String> {
    let mut urls = Vec::new();
    for raw in [
        std::env::var("PINATA_GATEWAY_URL").ok(),
        std::env::var("VITE_PINATA_GATEWAY").ok(),
        option_env!("PINATA_GATEWAY_URL").map(str::to_string),
        option_env!("VITE_PINATA_GATEWAY").map(str::to_string),
    ]
    .into_iter()
    .flatten()
    {
        let host = raw
            .trim()
            .trim_start_matches("https://")
            .trim_start_matches("http://")
            .trim_end_matches('/');
        if !host.is_empty() {
            urls.push(format!("https://{host}/ipfs"));
        }
    }
    urls.push("https://compose.mypinata.cloud/ipfs".to_string());
    urls.sort();
    urls.dedup();
    urls
}

async fn fetch_registered_agent_card_authority(
    agent_card_uri: &str,
) -> Result<RegisteredAgentCardAuthority, String> {
    let cid = extract_agent_card_cid(agent_card_uri)?;
    let client = HttpClient::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|err| format!("failed to build agent card client: {err}"))?;

    let mut last_error: Option<String> = None;
    for gateway in agent_card_gateway_urls() {
        let response = match client
            .get(format!("{}/{}", gateway.trim_end_matches('/'), cid))
            .send()
            .await
        {
            Ok(value) => value,
            Err(err) => {
                last_error = Some(format!("failed to fetch agent card from {gateway}: {err}"));
                continue;
            }
        };
        if !response.status().is_success() {
            last_error = Some(format!(
                "agent card fetch failed from {gateway}: HTTP {}",
                response.status()
            ));
            continue;
        }
        return response
            .json::<RegisteredAgentCardAuthority>()
            .await
            .map_err(|err| format!("failed to decode agent card JSON from {gateway}: {err}"));
    }

    Err(last_error.unwrap_or_else(|| "failed to fetch registered agent card".to_string()))
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
        let authority = fetch_registered_agent_card_authority(&request.manifest.agent_card_uri)
            .await
            .map_err(|err| a509_with_reason(err.as_str()))?;

        let authority_wallet = authority
            .wallet_address
            .as_deref()
            .and_then(normalize_wallet)
            .ok_or_else(|| a509_with_reason("registered agent card walletAddress is invalid"))?;
        if authority_wallet != request.manifest.agent_wallet {
            return Err(a509_with_reason(
                "agentWallet does not match the registered agent card",
            ));
        }

        let authority_chain = authority
            .chain
            .filter(|value| *value > 0)
            .ok_or_else(|| a509_with_reason("registered agent card chain is invalid"))?;
        if authority_chain != request.manifest.chain_id {
            return Err(a509_with_reason(
                "chainId does not match the registered agent card",
            ));
        }

        let authority_name = authority
            .name
            .map(|value| truncate_string(value, 80))
            .unwrap_or_default();
        if authority_name != request.manifest.name {
            return Err(a509_with_reason(
                "name does not match the registered agent card",
            ));
        }

        let authority_description = authority
            .description
            .map(|value| truncate_string(value, 240))
            .unwrap_or_default();
        if authority_description != request.manifest.description {
            return Err(a509_with_reason(
                "description does not match the registered agent card",
            ));
        }

        let authority_model = authority
            .model
            .map(|value| truncate_string(value, 120))
            .unwrap_or_default();
        if authority_model != request.manifest.model {
            return Err(a509_with_reason(
                "model does not match the registered agent card",
            ));
        }

        let authority_framework = authority
            .framework
            .map(|value| truncate_string(value, 80))
            .unwrap_or_default();
        if authority_framework != request.manifest.framework {
            return Err(a509_with_reason(
                "framework does not match the registered agent card",
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

pub(crate) fn queue_manifest_reconcile_after_a509(
    app: &tauri::AppHandle,
    agent_wallet: &str,
) -> Result<(), String> {
    let normalized_wallet =
        normalize_wallet(agent_wallet).ok_or_else(|| "invalid agentWallet".to_string())?;
    let state_value = load_local_state_value(app)?;
    let already_requested =
        manifest_republish_on_a509_requested(&state_value, normalized_wallet.as_str());
    if !manifest_publication_required(&state_value, normalized_wallet.as_str())
        && !already_requested
    {
        return Ok(());
    }

    if !already_requested {
        let _ = append_daemon_log(
            app,
            normalized_wallet.as_str(),
            A509_INCONSISTENT_AGENT_IDENTITY,
        );
    }
    set_manifest_republish_on_a509(app, normalized_wallet.as_str(), true)?;
    queue_manifest_publication_request(app, normalized_wallet.as_str(), "mesh-a509-reconcile")
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
        return A509_INCONSISTENT_AGENT_IDENTITY.to_string();
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

pub(crate) async fn process_mesh_manifest_publication_request(
    app: &tauri::AppHandle,
    mesh_state: &MeshRuntimeState,
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
        build_current_mesh_publication(app, &requested_wallet, live_status).await?;
    let anchor =
        anchor_mesh_state_from_command(app, mesh_state, runtime_host, anchor_request).await?;

    manifest.state_root_hash = Some(
        anchor
            .state_root_hash
            .clone()
            .trim_start_matches("0x")
            .to_string(),
    );
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

    let published = publish_mesh_manifest_from_command(app, mesh_state, manifest).await?;
    persist_manifest_update(app, &published)?;
    let _ = app.emit("mesh-manifest-updated", &published);
    let _ = append_daemon_log(
        app,
        &requested_wallet,
        &format!(
            "manifest published: path={} stateVersion={} pdpPieceCid={}",
            anchor.path, published.state_version, anchor.pdp_piece_cid,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a509_with_reason_preserves_detail() {
        assert_eq!(
            a509_with_reason("stateRootHash does not match the latest anchored state"),
            "a509: stateRootHash does not match the latest anchored state"
        );
        assert_eq!(
            a509_with_reason("   "),
            A509_INCONSISTENT_AGENT_IDENTITY.to_string()
        );
    }

    #[test]
    fn extract_agent_card_cid_accepts_ipfs_uri_and_plain_cid() {
        assert_eq!(
            extract_agent_card_cid("ipfs://bafybeigdyrzt/card.json").expect("cid"),
            "bafybeigdyrzt"
        );
        assert_eq!(
            extract_agent_card_cid("bafybeigdyrzt").expect("cid"),
            "bafybeigdyrzt"
        );
        assert!(extract_agent_card_cid("ipfs://").is_err());
    }

    #[test]
    fn agent_card_gateway_urls_always_include_default_pinata_gateway() {
        assert!(agent_card_gateway_urls()
            .iter()
            .any(|value| value == "https://compose.mypinata.cloud/ipfs"));
    }
}
