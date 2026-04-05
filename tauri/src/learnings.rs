use super::*;
use crate::{manifest::*, mesh::*};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
    pub(crate) fn as_str(&self) -> &str {
        match self {
            Self::Learning => "learning",
            Self::Report => "report",
            Self::Resource => "resource",
            Self::Ticket => "ticket",
        }
    }
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

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct LocalLearningHaiState {
    pub(crate) version: u32,
    pub(crate) agent_wallet: String,
    #[serde(rename = "userAddress")]
    pub(crate) user_wallet: String,
    pub(crate) device_id: String,
    pub(crate) hai_id: String,
    pub(crate) next_learning_number: u64,
    pub(crate) last_learning_number: Option<u64>,
    pub(crate) last_learning_path: Option<String>,
    pub(crate) last_learning_piece_cid: Option<String>,
    pub(crate) updated_at: u64,
}

pub(crate) fn local_learning_hai_state_path(
    app: &tauri::AppHandle,
    agent_wallet: &str,
    user_wallet: &str,
    device_id: &str,
) -> Result<PathBuf, String> {
    let agent_wallet = normalize_wallet(agent_wallet)
        .ok_or_else(|| "local learning agentWallet must be a valid wallet address".to_string())?;
    let user_wallet = normalize_wallet(user_wallet)
        .ok_or_else(|| "local learning userAddress must be a valid wallet address".to_string())?;
    let device_id = normalize_device_id(device_id)
        .ok_or_else(|| "local learning deviceId format is invalid".to_string())?;
    let device_key = sha256_hex_string(device_id.as_str());
    Ok(local_hai_state_dir(app)?.join(format!(
        "{}__{}__{}.learning.json",
        agent_wallet, user_wallet, device_key
    )))
}

pub(crate) fn normalize_local_learning_hai_state(
    value: LocalLearningHaiState,
    agent_wallet: &str,
    user_wallet: &str,
    device_id: &str,
) -> Result<LocalLearningHaiState, String> {
    let agent_wallet = normalize_wallet(agent_wallet)
        .ok_or_else(|| "local learning agentWallet must be a valid wallet address".to_string())?;
    let user_wallet = normalize_wallet(user_wallet)
        .ok_or_else(|| "local learning userAddress must be a valid wallet address".to_string())?;
    let device_id = normalize_device_id(device_id)
        .ok_or_else(|| "local learning deviceId format is invalid".to_string())?;

    Ok(LocalLearningHaiState {
        version: 1,
        agent_wallet: agent_wallet.clone(),
        user_wallet: user_wallet.clone(),
        device_id: device_id.clone(),
        hai_id: derive_hai_id(&agent_wallet, &user_wallet, &device_id),
        next_learning_number: value.next_learning_number.max(1),
        last_learning_number: value.last_learning_number.filter(|value| *value > 0),
        last_learning_path: value
            .last_learning_path
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty()),
        last_learning_piece_cid: value
            .last_learning_piece_cid
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty()),
        updated_at: now_ms(),
    })
}

pub(crate) fn save_local_learning_hai_state(
    app: &tauri::AppHandle,
    state: &LocalLearningHaiState,
) -> Result<(), String> {
    let path = local_learning_hai_state_path(
        app,
        &state.agent_wallet,
        &state.user_wallet,
        &state.device_id,
    )?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| format!("failed to create local learning state directory: {err}"))?;
    }
    let serialized = serde_json::to_string_pretty(state)
        .map_err(|err| format!("failed to encode local learning state: {err}"))?;
    fs::write(&path, serialized)
        .map_err(|err| format!("failed to persist local learning state: {err}"))
}

pub(crate) fn ensure_local_learning_hai_state(
    app: &tauri::AppHandle,
    agent_wallet: &str,
    user_wallet: &str,
    device_id: &str,
) -> Result<LocalLearningHaiState, String> {
    let path = local_learning_hai_state_path(app, agent_wallet, user_wallet, device_id)?;
    let normalized = if path.exists() {
        let raw = fs::read_to_string(&path)
            .map_err(|err| format!("failed to read local learning state: {err}"))?;
        let parsed = serde_json::from_str::<LocalLearningHaiState>(&raw)
            .map_err(|err| format!("failed to parse local learning state: {err}"))?;
        normalize_local_learning_hai_state(parsed, agent_wallet, user_wallet, device_id)?
    } else {
        normalize_local_learning_hai_state(
            LocalLearningHaiState::default(),
            agent_wallet,
            user_wallet,
            device_id,
        )?
    };
    save_local_learning_hai_state(app, &normalized)?;
    Ok(normalized)
}

pub(crate) fn record_local_hai_learning(
    app: &tauri::AppHandle,
    state: &LocalLearningHaiState,
    response: &MeshSharedArtifactPinRuntimeResponse,
) -> Result<LocalLearningHaiState, String> {
    let mut updated = state.clone();
    updated.hai_id = response.hai_id.clone();
    updated.next_learning_number = response.artifact_number.saturating_add(1);
    updated.last_learning_number = Some(response.artifact_number);
    updated.last_learning_path = Some(response.path.clone());
    updated.last_learning_piece_cid = Some(response.piece_cid.clone());
    updated.updated_at = now_ms();
    save_local_learning_hai_state(app, &updated)?;
    Ok(updated)
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
    pub(crate) hai_id: String,
    pub(crate) artifact_kind: MeshSharedArtifactKind,
    pub(crate) artifact_number: u64,
    pub(crate) path: String,
    pub(crate) latest_alias: String,
    pub(crate) root_cid: String,
    pub(crate) piece_cid: String,
    pub(crate) collection: String,
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
        artifact_kind: artifact_kind.map(|value| value.as_str().to_string()),
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
    host: &LocalHostState,
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
    let status = ensure_local_host(app, host)?;
    let hai_state =
        ensure_local_hai_state(app, &requested_wallet, &ctx.user_wallet, &ctx.device_id)?;
    let learning_state =
        ensure_local_learning_hai_state(app, &requested_wallet, &ctx.user_wallet, &ctx.device_id)?;

    let payload_json = build_learning_payload_json(request, &ctx.user_wallet)?;
    let artifact_kind = MeshSharedArtifactKind::Learning;
    let artifact_number = learning_state.next_learning_number;
    let path = learning_hai_path(&learning_state.hai_id, learning_title, artifact_number);
    let signed_request_json = build_signed_mesh_request_json(
        &load_or_create_mesh_identity(app)?,
        live_status,
        &requested_wallet,
        &ctx.user_wallet,
        &ctx.device_id,
        &learning_state.hai_id,
        "learning.pin",
        "learnings",
        &path,
        Some(artifact_kind.clone()),
        Some(format!("0x{}", sha256_hex_string(&payload_json))),
    )?;

    let response = pin_mesh_learning_via_local_runtime(
        &status.base_url,
        serde_json::json!({
            "apiUrl": ctx.api_url,
            "composeKeyToken": ctx.compose_key_token,
            "userAddress": ctx.user_wallet,
            "agentWallet": requested_wallet,
            "deviceId": ctx.device_id,
            "chainId": ctx.chain_id,
            "targetSessionExpiry": ctx.target_synapse_expiry,
            "signedRequestJson": signed_request_json,
            "haiId": learning_state.hai_id,
            "artifactKind": artifact_kind,
            "artifactNumber": artifact_number,
            "path": path,
            "payloadJson": payload_json,
            "filecoinPinSessionKeyPrivateKey": hai_state.synapse_session_private_key,
        }),
    )
    .await?;
    let _ = record_local_hai_learning(app, &learning_state, &response)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn learning_path_slug_rewrites_skill_placeholder() {
        assert_eq!(learning_path_slug("Skill"), "untitled");
        assert_eq!(
            learning_path_slug("Useful Market Insight"),
            "useful-market-insight"
        );
    }

    #[test]
    fn learning_hai_path_uses_learning_schema() {
        assert_eq!(
            learning_hai_path("abc123", "Useful Market Insight", 7),
            "compose-abc123-useful-market-insight-#7"
        );
    }

    #[test]
    fn normalize_local_learning_hai_state_repairs_defaults() {
        let state = normalize_local_learning_hai_state(
            LocalLearningHaiState {
                next_learning_number: 0,
                last_learning_number: Some(0),
                last_learning_path: Some("   ".to_string()),
                last_learning_piece_cid: Some("  bafy-piece  ".to_string()),
                ..Default::default()
            },
            "0x1111111111111111111111111111111111111111",
            "0x2222222222222222222222222222222222222222",
            "device-12345678",
        )
        .expect("state should normalize");

        assert_eq!(state.next_learning_number, 1);
        assert_eq!(state.last_learning_number, None);
        assert_eq!(state.last_learning_path, None);
        assert_eq!(state.last_learning_piece_cid.as_deref(), Some("bafy-piece"));
        assert_eq!(state.hai_id.len(), 6);
    }

    #[test]
    fn build_learning_payload_json_requires_fields() {
        let request = MeshPublicationQueueRequest {
            request_id: "learning-1".to_string(),
            kind: MeshPublicationQueueKind::LearningPin,
            agent_wallet: "0x1111111111111111111111111111111111111111".to_string(),
            requested_at: 1,
            reason: None,
            title: Some("".to_string()),
            summary: Some("summary".to_string()),
            content: Some("content".to_string()),
            access_price_usdc: Some("1000".to_string()),
        };

        let error =
            build_learning_payload_json(&request, "0x2222222222222222222222222222222222222222")
                .expect_err("payload should reject missing title");
        assert_eq!(error, "mesh learning title is required");
    }
}
