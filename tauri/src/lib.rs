mod mesh;
#[path = "runtime_host.rs"]
mod runtime_host;

use futures::StreamExt;
use libp2p::{
    autonat, connection_limits, dcutr,
    gossipsub::{self, IdentTopic, MessageAuthenticity, ValidationMode},
    identify,
    identity::{self, Keypair},
    kad, mdns,
    multiaddr::Protocol,
    noise, ping, relay, rendezvous, request_response,
    swarm::{NetworkBehaviour, Swarm, SwarmEvent},
    tcp, yamux, Multiaddr, PeerId, StreamProtocol, SwarmBuilder,
};
use rand::{rngs::OsRng, RngCore};
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

use self::mesh::*;
use self::runtime_host::{ensure_local_runtime_host, LocalRuntimeHostState};

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
struct SessionBudgetTracker {
    last_budget_used: Mutex<Option<u64>>,
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

                if let Some(manifest_object) = network_object
                    .get_mut("manifest")
                    .and_then(|value| value.as_object_mut())
                {
                    let listen_multiaddrs = manifest_object
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
                    if derive_relay_peer_id_from_listen_multiaddrs(&listen_multiaddrs).is_none() {
                        manifest_object.insert("relayPeerId".to_string(), serde_json::Value::Null);
                    }
                }
            }
        }
    }

    serde_json::to_string(&state)
        .map_err(|err| format!("failed to serialize normalized local state: {err}"))
}

fn parse_local_state_json_with_recovery(raw: &str) -> Result<(serde_json::Value, bool), String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok((serde_json::json!({}), false));
    }

    let mut stream = serde_json::Deserializer::from_str(raw).into_iter::<serde_json::Value>();
    let value = match stream.next() {
        Some(Ok(value)) => value,
        Some(Err(err)) => return Err(format!("failed to parse local state JSON: {err}")),
        None => return Ok((serde_json::json!({}), false)),
    };

    let trailing = raw[stream.byte_offset()..].trim();
    Ok((value, !trailing.is_empty()))
}

fn save_local_state_value_to_path(file: &Path, state: &serde_json::Value) -> Result<(), String> {
    let serialized = serde_json::to_string(state)
        .map_err(|err| format!("failed to serialize local state JSON: {err}"))?;
    let normalized = normalize_local_state_json(&serialized)?;
    write_string_atomically(file, &normalized, "local state")
}

fn load_local_state_value_from_path(file: &Path) -> Result<(serde_json::Value, bool), String> {
    if !file.exists() {
        return Ok((serde_json::json!({}), false));
    }

    let raw =
        fs::read_to_string(file).map_err(|err| format!("failed to read local state: {err}"))?;
    let (value, repaired) = parse_local_state_json_with_recovery(&raw)?;
    if repaired {
        save_local_state_value_to_path(file, &value)?;
    }
    Ok((value, repaired))
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
    let (value, _) = load_local_state_value_from_path(&file)?;
    serde_json::from_value(value).map_err(|err| format!("failed to parse local state: {err}"))
}

fn load_local_state_value(app: &tauri::AppHandle) -> Result<serde_json::Value, String> {
    let file = local_state_path(app)?;
    let (value, _) = load_local_state_value_from_path(&file)?;
    Ok(value)
}

fn save_local_state_value(app: &tauri::AppHandle, state: &serde_json::Value) -> Result<(), String> {
    let file = local_state_path(app)?;
    save_local_state_value_to_path(&file, state)
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
    let mut next_state = serde_json::from_str::<serde_json::Value>(&normalized)
        .map_err(|err| format!("failed to parse normalized state JSON: {err}"))?;
    preserve_internal_manifest_network_state(&previous_state, &mut next_state);
    let serialized = serde_json::to_string(&next_state)
        .map_err(|err| format!("failed to serialize merged local state JSON: {err}"))?;
    let next_normalized = normalize_local_state_json(&serialized)?;
    write_string_atomically(&state_file, &next_normalized, "local state")?;
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

fn warn_mesh_published_agents(
    app: &tauri::AppHandle,
    request: &MeshJoinRequest,
    message: impl Into<String>,
) {
    let message = message.into();
    eprintln!("[mesh] {}", message);
    append_mesh_log_to_published_agents(app, &request.published_agents, &message);
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

fn extract_local_agent_json_payload(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    if trimmed.starts_with("```") {
        let mut lines = trimmed.lines();
        let _ = lines.next();
        let mut inner = Vec::new();
        for line in lines {
            if line.trim() == "```" {
                break;
            }
            inner.push(line);
        }
        let candidate = inner.join("\n").trim().to_string();
        if candidate.starts_with('{') && candidate.ends_with('}') {
            return Some(candidate);
        }
    }

    let start = trimmed.find('{')?;
    let end = trimmed.rfind('}')?;
    if end < start {
        return None;
    }

    Some(trimmed[start..=end].trim().to_string())
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

    let parsed_source = if serde_json::from_str::<serde_json::Value>(trimmed).is_ok() {
        trimmed.to_string()
    } else if let Some(candidate) = extract_local_agent_json_payload(trimmed) {
        candidate
    } else {
        return LocalAgentStructuredReply {
            reply: trimmed.to_string(),
            report: None,
            skill: None,
            actions: Vec::new(),
        };
    };

    let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&parsed_source) else {
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

fn persist_local_agent_structured_skill(
    app: &tauri::AppHandle,
    agent_wallet: &str,
    skill: &LocalAgentStructuredSkill,
) -> Result<String, String> {
    let workspace = daemon_agent_workspace_path(app, agent_wallet)?;
    let file_path = workspace
        .join("skills")
        .join("generated")
        .join(local_agent_slug(&skill.name))
        .join("SKILL.md");
    write_string_atomically(&file_path, skill.markdown.trim(), "agent skill")?;

    let base_dir = resolve_base_dir(app)?;
    Ok(base_relative_label(&base_dir, &file_path))
}

fn sync_local_agent_workspace_to_state(
    app: &tauri::AppHandle,
    agent_wallet: &str,
) -> Result<LocalWorkspaceSyncOutcome, String> {
    let normalized_wallet =
        normalize_wallet(agent_wallet).ok_or_else(|| "invalid agentWallet".to_string())?;
    let mut state_value = load_local_state_value(app)?;
    let outcome = if let Some(agent_object) =
        find_installed_agent_value_mut(&mut state_value, normalized_wallet.as_str())
    {
        sync_local_agent_workspace_state(app, normalized_wallet.as_str(), agent_object)?
    } else {
        LocalWorkspaceSyncOutcome::default()
    };
    save_local_state_value(app, &state_value)?;
    Ok(outcome)
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
    if path == "/mesh/conclave/run" {
        verify_manifest_with_mesh(app, &agent.agent_wallet).await?;
    }

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

    let result = decode_remote_json(response, "local runtime tool request").await;
    if result
        .as_ref()
        .err()
        .is_some_and(|error| is_a409_error(error))
    {
        let _ = queue_manifest_reconcile_after_a409(app, &agent.agent_wallet);
    }
    result
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
            let _ = sync_local_agent_workspace_to_state(app, &agent.agent_wallet);
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
            publish_learning_from_local_agent_action(
                app,
                &agent.agent_wallet,
                action.title.as_deref(),
                action.summary.as_deref(),
                action.content.as_deref(),
                action.access_price_usdc.as_deref(),
            )
            .await
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
            let (authored_skill_id, authored_skill_path) = if let Some(skill) =
                parsed.skill.as_ref()
            {
                let path = persist_local_agent_structured_skill(app, &agent.agent_wallet, skill)?;
                let _ = sync_local_agent_workspace_to_state(app, &agent.agent_wallet);
                (Some(local_agent_authored_skill_id(&skill.name)), Some(path))
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
        let heading = line.trim().strip_prefix('#')?.trim();
        if heading.is_empty() {
            return None;
        }
        let lower = heading.to_ascii_lowercase();
        let stripped = if lower.starts_with("skill:") {
            heading[6..].trim()
        } else if lower.starts_with("skill -") {
            heading[7..].trim()
        } else if lower.starts_with("skill ") {
            heading[6..].trim()
        } else {
            heading
        };
        if stripped.is_empty() {
            None
        } else {
            Some(stripped.to_string())
        }
    })
}

fn markdown_represents_agent_skill(markdown: &str) -> bool {
    markdown.lines().any(|line| {
        let Some(heading) = line.trim().strip_prefix('#').map(str::trim) else {
            return false;
        };
        let lower = heading.to_ascii_lowercase();
        lower == "skill"
            || lower.starts_with("skill:")
            || lower.starts_with("skill -")
            || lower.starts_with("skill ")
    })
}

fn collect_workspace_root_markdown_files(
    root: &Path,
    output: &mut Vec<PathBuf>,
) -> Result<(), String> {
    let entries =
        fs::read_dir(root).map_err(|err| format!("failed to read workspace root: {err}"))?;
    for entry in entries {
        let entry = entry.map_err(|err| format!("failed to read workspace root entry: {err}"))?;
        let path = entry.path();
        if path.is_file() && path.extension().and_then(|value| value.to_str()) == Some("md") {
            output.push(path);
        }
    }
    Ok(())
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
    let workspace = daemon_agent_workspace_path(app, agent_wallet)?;
    let generated_skills_root = workspace.join("skills").join("generated");
    let mut skill_files = Vec::new();
    collect_skill_markdown_files(&generated_skills_root, &mut skill_files)?;
    collect_workspace_root_markdown_files(&workspace, &mut skill_files)?;
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
        let is_generated = skill_path.starts_with(&generated_skills_root);
        if !is_generated && !markdown_represents_agent_skill(&markdown) {
            continue;
        }
        let skill_name = extract_skill_name_from_markdown(&markdown).unwrap_or_else(|| {
            skill_path
                .file_stem()
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
                "source": if is_generated { "generated" } else { "agent" },
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
    }

    Ok(summary)
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

fn daemon_agent_state_matches(
    agent: &DaemonAgentState,
    running: bool,
    status: &str,
    last_error: Option<&str>,
) -> bool {
    agent.running == running && agent.status == status && agent.last_error.as_deref() == last_error
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
                    let should_log = !daemon_agent_state_matches(
                        daemon_agent,
                        false,
                        "stopped",
                        Some(message.as_str()),
                    );
                    let _ = update_daemon_agent_state(
                        app,
                        &agent_wallet,
                        false,
                        "stopped",
                        Some(message.clone()),
                    );
                    if should_log {
                        let _ = append_daemon_log(app, &agent_wallet, &message);
                    }
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
            let should_log =
                !daemon_agent_state_matches(daemon_agent, false, "stopped", Some(message.as_str()));
            let _ = update_daemon_agent_state(
                app,
                &agent_wallet,
                false,
                "stopped",
                Some(message.clone()),
            );
            if should_log {
                let _ =
                    append_daemon_log(app, &agent_wallet, &format!("heartbeat skipped: {message}"));
            }
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
                let _ = manifest_dirty;

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
    let _ = manifest_dirty;

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
    location: String,
    camera: String,
    microphone: String,
    screen: String,
    full_disk_access: String,
    accessibility: String,
}

#[cfg(target_os = "macos")]
unsafe extern "C" {
    fn compose_mesh_location_authorization_status() -> i32;
    fn compose_mesh_request_location_access() -> bool;
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
fn location_authorization_status(value: i32) -> String {
    match value {
        3 | 4 => "granted".to_string(),
        _ => "denied".to_string(),
    }
}

#[cfg(target_os = "macos")]
fn query_location_permission() -> String {
    unsafe { location_authorization_status(compose_mesh_location_authorization_status()) }
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
            location: query_location_permission(),
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
            location: "denied".to_string(),
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
        "location" => "Privacy_LocationServices",
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
            "location" => {
                let _ = unsafe { compose_mesh_request_location_access() };
            }
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
        .manage(mesh::MeshRuntimeState::default())
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
            mesh::local_network_status,
            mesh::local_network_join,
            mesh::local_network_leave,
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
                        if let Err(error) =
                            mesh::process_pending_mesh_publication_requests(&handle).await
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
