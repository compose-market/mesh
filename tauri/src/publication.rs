use super::*;
use crate::{learnings::*, manifest::*, mesh::*};

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
    pub(crate) request_id: String,
    pub(crate) kind: MeshPublicationQueueKind,
    pub(crate) agent_wallet: String,
    pub(crate) requested_at: u64,
    pub(crate) reason: Option<String>,
    pub(crate) title: Option<String>,
    pub(crate) summary: Option<String>,
    pub(crate) content: Option<String>,
    pub(crate) access_price_usdc: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MeshPublicationQueueResult {
    pub(crate) request_id: String,
    pub(crate) agent_wallet: Option<String>,
    pub(crate) kind: Option<MeshPublicationQueueKind>,
    pub(crate) success: bool,
    pub(crate) error: Option<String>,
    pub(crate) hai_id: Option<String>,
    pub(crate) update_number: Option<u64>,
    pub(crate) artifact_kind: Option<MeshSharedArtifactKind>,
    pub(crate) artifact_number: Option<u64>,
    pub(crate) path: Option<String>,
    pub(crate) latest_alias: Option<String>,
    pub(crate) root_cid: Option<String>,
    pub(crate) piece_cid: Option<String>,
    pub(crate) collection: Option<String>,
    pub(crate) state_root_hash: Option<String>,
    pub(crate) pdp_piece_cid: Option<String>,
    pub(crate) pdp_anchored_at: Option<u64>,
    pub(crate) manifest: Option<MeshManifest>,
}

pub(crate) fn mesh_publication_requests_dir(app: &tauri::AppHandle) -> Result<PathBuf, String> {
    let dir = resolve_base_dir(app)?
        .join("mesh")
        .join("publications")
        .join("requests");
    fs::create_dir_all(&dir)
        .map_err(|err| format!("failed to create mesh publication requests dir: {err}"))?;
    Ok(dir)
}

pub(crate) fn mesh_publication_agent_requests_dir(
    app: &tauri::AppHandle,
    agent_wallet: &str,
) -> Result<PathBuf, String> {
    let dir = mesh_publication_requests_dir(app)?.join(agent_wallet.to_lowercase());
    fs::create_dir_all(&dir)
        .map_err(|err| format!("failed to create mesh publication agent requests dir: {err}"))?;
    Ok(dir)
}

pub(crate) fn mesh_publication_results_dir(app: &tauri::AppHandle) -> Result<PathBuf, String> {
    let dir = resolve_base_dir(app)?
        .join("mesh")
        .join("publications")
        .join("results");
    fs::create_dir_all(&dir)
        .map_err(|err| format!("failed to create mesh publication results dir: {err}"))?;
    Ok(dir)
}

pub(crate) fn mesh_publication_agent_results_dir(
    app: &tauri::AppHandle,
    agent_wallet: &str,
) -> Result<PathBuf, String> {
    let dir = mesh_publication_results_dir(app)?.join(agent_wallet.to_lowercase());
    fs::create_dir_all(&dir)
        .map_err(|err| format!("failed to create mesh publication agent results dir: {err}"))?;
    Ok(dir)
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

pub(crate) async fn process_mesh_publication_request(
    app: &tauri::AppHandle,
    request: MeshPublicationQueueRequest,
) -> MeshPublicationQueueResult {
    let mesh_state = app.state::<MeshRuntimeState>();
    let host = app.state::<LocalHostState>();

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
                process_mesh_manifest_publication_request(
                    app,
                    mesh_state.inner(),
                    host.inner(),
                    &live_status,
                    &request,
                )
                .await
            }
            MeshPublicationQueueKind::LearningPin => {
                process_mesh_learning_request(app, host.inner(), &live_status, &request).await
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
                let should_republish = error.to_lowercase().starts_with("a509:");
                let _ = set_manifest_republish_on_a509(app, &normalized_wallet, should_republish);
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
