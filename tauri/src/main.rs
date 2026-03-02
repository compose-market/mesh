#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use futures::StreamExt;
use libp2p::{
    core::upgrade,
    dns,
    gossipsub::{self, IdentTopic, MessageAuthenticity, ValidationMode},
    identify,
    identity::{self, Keypair},
    noise, ping,
    swarm::{NetworkBehaviour, Swarm, SwarmEvent},
    tcp, yamux, Multiaddr, PeerId, Transport,
};
use std::collections::HashSet;
use std::fs;
use std::path::{Component, Path, PathBuf};
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
    session_id: String,
    compose_key_id: String,
    device_id: String,
    chain_id: u32,
    gossip_topic: String,
    bootstrap_multiaddrs: Vec<String>,
    relay_multiaddrs: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct MeshRuntimeStatus {
    running: bool,
    status: String,
    user_address: Option<String>,
    agent_wallet: Option<String>,
    session_id: Option<String>,
    compose_key_id: Option<String>,
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
            session_id: None,
            compose_key_id: None,
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
    ping: ping::Behaviour,
    identify: identify::Behaviour,
    gossipsub: gossipsub::Behaviour,
}

#[derive(Debug)]
enum MeshBehaviourEvent {
    Ping(ping::Event),
    Identify(identify::Event),
    Gossipsub(gossipsub::Event),
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

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_millis() as u64
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

fn validate_mesh_join_request(request: &MeshJoinRequest) -> Result<MeshJoinRequest, String> {
    let user_address = normalize_wallet(&request.user_address)
        .ok_or_else(|| "userAddress must be a valid wallet address".to_string())?;
    let agent_wallet = normalize_wallet(&request.agent_wallet)
        .ok_or_else(|| "agentWallet must be a valid wallet address".to_string())?;
    let device_id = normalize_device_id(&request.device_id)
        .ok_or_else(|| "deviceId format is invalid".to_string())?;

    if request.session_id.trim().is_empty() {
        return Err("sessionId is required".to_string());
    }
    if request.compose_key_id.trim().is_empty() {
        return Err("composeKeyId is required".to_string());
    }
    if request.chain_id == 0 {
        return Err("chainId must be positive".to_string());
    }
    if request.gossip_topic.trim().is_empty() {
        return Err("gossipTopic is required".to_string());
    }

    Ok(MeshJoinRequest {
        user_address,
        agent_wallet,
        session_id: request.session_id.trim().to_string(),
        compose_key_id: request.compose_key_id.trim().to_string(),
        device_id,
        chain_id: request.chain_id,
        gossip_topic: request.gossip_topic.trim().to_string(),
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
        status.session_id = Some(request.session_id.clone());
        status.compose_key_id = Some(request.compose_key_id.clone());
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
            status.session_id = Some(req.session_id.clone());
            status.compose_key_id = Some(req.compose_key_id.clone());
            status.device_id = Some(req.device_id.clone());
        }
        status.running = false;
        status.status = "error".to_string();
        status.last_error = Some(message);
        status.updated_at = now_ms();
    });
}

fn build_mesh_swarm(
    local_key: identity::Keypair,
    request: &MeshJoinRequest,
) -> Result<(Swarm<MeshBehaviour>, IdentTopic), String> {
    let peer_id = PeerId::from(local_key.public());

    let gossipsub_config = gossipsub::ConfigBuilder::default()
        .validation_mode(ValidationMode::None)
        .heartbeat_interval(Duration::from_secs(10))
        .build()
        .map_err(|err| format!("failed to build gossipsub config: {err}"))?;

    let mut gossipsub = gossipsub::Behaviour::new(
        MessageAuthenticity::Signed(local_key.clone()),
        gossipsub_config,
    )
    .map_err(|err| format!("failed to initialize gossipsub: {err}"))?;

    let topic = IdentTopic::new(request.gossip_topic.clone());
    gossipsub
        .subscribe(&topic)
        .map_err(|err| format!("failed to subscribe gossipsub topic: {err}"))?;

    let behaviour = MeshBehaviour {
        ping: ping::Behaviour::new(ping::Config::new()),
        identify: identify::Behaviour::new(identify::Config::new(
            "/compose-market/desktop/1.0.0".to_string(),
            local_key.public(),
        )),
        gossipsub,
    };

    let base_transport = tcp::tokio::Transport::new(tcp::Config::default().nodelay(true));
    let transport = dns::tokio::Transport::system(base_transport)
        .map_err(|err| format!("failed to initialize dns transport: {err}"))?
        .upgrade(upgrade::Version::V1)
        .authenticate(noise::Config::new(&local_key).map_err(|err| format!("failed to initialize noise: {err}"))?)
        .multiplex(yamux::Config::default())
        .boxed();

    let mut swarm = Swarm::new(
        transport,
        behaviour,
        peer_id,
        libp2p::swarm::Config::with_tokio_executor(),
    );

    swarm
        .listen_on("/ip4/0.0.0.0/tcp/0".parse::<Multiaddr>().map_err(|err| format!("invalid listen address: {err}"))?)
        .map_err(|err| format!("failed to start listening: {err}"))?;

    for addr in request
        .bootstrap_multiaddrs
        .iter()
        .chain(request.relay_multiaddrs.iter())
    {
        match addr.parse::<Multiaddr>() {
            Ok(multiaddr) => {
                if let Err(err) = swarm.dial(multiaddr.clone()) {
                    eprintln!("[mesh] dial failed for {}: {}", multiaddr, err);
                }
            }
            Err(err) => {
                eprintln!("[mesh] invalid multiaddr '{}': {}", addr, err);
            }
        }
    }

    Ok((swarm, topic))
}

fn build_announce_payload(
    request: &MeshJoinRequest,
    peer_id: &str,
    listen_multiaddrs: &[String],
) -> Vec<u8> {
    serde_json::json!({
        "version": 1,
        "type": "presence",
        "timestamp": now_ms(),
        "chainId": request.chain_id,
        "agentWallet": request.agent_wallet,
        "userAddress": request.user_address,
        "sessionId": request.session_id,
        "composeKeyId": request.compose_key_id,
        "deviceId": request.device_id,
        "peerId": peer_id,
        "listenMultiaddrs": listen_multiaddrs,
    })
    .to_string()
    .into_bytes()
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

    let (mut swarm, topic) = match build_mesh_swarm(local_key, &request) {
        Ok(value) => value,
        Err(err) => {
            mesh_error(&app, Some(&request), err);
            return;
        }
    };

    let mut connected_peers: HashSet<PeerId> = HashSet::new();
    let mut heartbeat_interval = tokio::time::interval(Duration::from_secs(30));

    let _ = with_mesh_status(&app, |status| {
        status.running = true;
        status.status = "connecting".to_string();
        status.user_address = Some(request.user_address.clone());
        status.agent_wallet = Some(request.agent_wallet.clone());
        status.session_id = Some(request.session_id.clone());
        status.compose_key_id = Some(request.compose_key_id.clone());
        status.device_id = Some(request.device_id.clone());
        status.peer_id = Some(local_peer_id.clone());
        status.listen_multiaddrs.clear();
        status.peers_discovered = 0;
        status.last_error = None;
        status.updated_at = now_ms();
    });

    loop {
        tokio::select! {
            _ = &mut stop_rx => {
                mark_mesh_status(&app, &request, "dormant");
                break;
            }
            _ = heartbeat_interval.tick() => {
                let listen_multiaddrs = with_mesh_status(&app, |status| status.listen_multiaddrs.clone()).unwrap_or_default();
                let payload = build_announce_payload(&request, &local_peer_id, &listen_multiaddrs);
                let publish_result = swarm.behaviour_mut().gossipsub.publish(topic.clone(), payload);
                let _ = with_mesh_status(&app, |status| {
                    status.last_heartbeat_at = Some(now_ms());
                    status.updated_at = now_ms();
                    if let Err(err) = publish_result {
                        status.status = "error".to_string();
                        status.last_error = Some(format!("gossipsub publish failed: {err}"));
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
                        let count = connected_peers.len() as u32;
                        let _ = with_mesh_status(&app, |status| {
                            status.peers_discovered = count;
                            status.status = "online".to_string();
                            status.updated_at = now_ms();
                        });
                    }
                    SwarmEvent::ConnectionClosed { peer_id, .. } => {
                        connected_peers.remove(&peer_id);
                        let count = connected_peers.len() as u32;
                        let _ = with_mesh_status(&app, |status| {
                            status.peers_discovered = count;
                            if count == 0 {
                                status.status = "connecting".to_string();
                            }
                            status.updated_at = now_ms();
                        });
                    }
                    SwarmEvent::Behaviour(MeshBehaviourEvent::Ping(_event)) => {
                        let _ = with_mesh_status(&app, |status| {
                            status.updated_at = now_ms();
                        });
                    }
                    SwarmEvent::Behaviour(MeshBehaviourEvent::Identify(_event)) => {
                        let _ = with_mesh_status(&app, |status| {
                            status.updated_at = now_ms();
                        });
                    }
                    SwarmEvent::Behaviour(MeshBehaviourEvent::Gossipsub(event)) => {
                        if matches!(event, gossipsub::Event::Message { .. }) {
                            let _ = with_mesh_status(&app, |status| {
                                status.updated_at = now_ms();
                                if status.status == "connecting" {
                                    status.status = "online".to_string();
                                }
                            });
                        }
                    }
                    SwarmEvent::OutgoingConnectionError { error, .. } => {
                        let _ = with_mesh_status(&app, |status| {
                            status.status = "error".to_string();
                            status.last_error = Some(format!("outgoing connection failed: {error}"));
                            status.updated_at = now_ms();
                        });
                    }
                    SwarmEvent::IncomingConnectionError { error, .. } => {
                        let _ = with_mesh_status(&app, |status| {
                            status.status = "error".to_string();
                            status.last_error = Some(format!("incoming connection failed: {error}"));
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
