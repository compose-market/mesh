use rand::{rngs::OsRng, RngCore};
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream};
use std::path::PathBuf;
use std::process::Child;
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

use tauri::{AppHandle, Manager};

use crate::{now_ms, resolve_base_dir};

const LOCAL_RUNTIME_HOST: &str = "127.0.0.1";
const DEFAULT_RUNTIME_URL: &str = "https://runtime.compose.market";
pub const LOCAL_RUNTIME_DEFAULT_PORT: u16 = 4310;
const LOCAL_RUNTIME_STARTUP_TIMEOUT: Duration = Duration::from_secs(45);
const LOCAL_RUNTIME_POLL_INTERVAL: Duration = Duration::from_millis(250);
const LOCAL_RUNTIME_IO_TIMEOUT: Duration = Duration::from_secs(1);
const LOCAL_RUNTIME_API_VERSION: u64 = 2;
const LOCAL_RUNTIME_REQUIRED_CAPABILITIES: &[&str] = &[
    "mesh.reputation.summary",
    "mesh.filecoin.pin",
    "mesh.conclave.run",
];

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalRuntimeHostStatus {
    pub running: bool,
    pub status: String,
    pub port: u16,
    pub base_url: String,
    pub pid: Option<u32>,
    pub started_at: Option<u64>,
    pub last_error: Option<String>,
    pub updated_at: u64,
}

impl Default for LocalRuntimeHostStatus {
    fn default() -> Self {
        let base_url = configured_runtime_base_url();
        Self {
            running: false,
            status: "dormant".to_string(),
            port: extract_port_from_base_url(&base_url).unwrap_or(LOCAL_RUNTIME_DEFAULT_PORT),
            base_url,
            pid: None,
            started_at: None,
            last_error: None,
            updated_at: now_ms(),
        }
    }
}

pub struct LocalRuntimeHostState {
    status: Mutex<LocalRuntimeHostStatus>,
    child: Mutex<Option<Child>>,
    auth_token: Mutex<String>,
}

fn runtime_auth_token_path(app: &AppHandle) -> Result<PathBuf, String> {
    Ok(resolve_base_dir(app)?.join("runtime-host.auth"))
}

fn load_persisted_runtime_auth_token(app: &AppHandle) -> Result<Option<String>, String> {
    let file = runtime_auth_token_path(app)?;
    if !file.exists() {
        return Ok(None);
    }
    let raw = fs::read_to_string(&file)
        .map_err(|err| format!("failed to read local runtime auth token: {err}"))?;
    let token = raw.trim().to_string();
    if token.is_empty() {
        Ok(None)
    } else {
        Ok(Some(token))
    }
}

fn persist_runtime_auth_token(app: &AppHandle, token: &str) -> Result<(), String> {
    let file = runtime_auth_token_path(app)?;
    if let Some(parent) = file.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| format!("failed to create local runtime auth token dir: {err}"))?;
    }
    fs::write(&file, token)
        .map_err(|err| format!("failed to persist local runtime auth token: {err}"))
}

fn ensure_runtime_host_auth_token(
    app: &AppHandle,
    state: &LocalRuntimeHostState,
) -> Result<String, String> {
    let mut guard = state
        .auth_token
        .lock()
        .map_err(|_| "failed to lock local runtime host auth token".to_string())?;
    if !guard.trim().is_empty() {
        return Ok(guard.clone());
    }

    if let Some(token) = load_persisted_runtime_auth_token(app)? {
        *guard = token.clone();
        return Ok(token);
    }

    let token = generate_runtime_auth_token();
    persist_runtime_auth_token(app, &token)?;
    *guard = token.clone();
    Ok(token)
}

pub fn build_local_runtime_base_url(port: u16) -> String {
    format!("http://{LOCAL_RUNTIME_HOST}:{port}")
}

pub fn current_runtime_host_status(
    state: &LocalRuntimeHostState,
) -> Result<LocalRuntimeHostStatus, String> {
    state
        .status
        .lock()
        .map(|status| status.clone())
        .map_err(|_| "failed to read local runtime host status".to_string())
}

pub fn current_runtime_host_auth_token(state: &LocalRuntimeHostState) -> Result<String, String> {
    let token = state
        .auth_token
        .lock()
        .map_err(|_| "failed to read local runtime host auth token".to_string())?;
    if token.trim().is_empty() {
        let status = state
            .status
            .lock()
            .map_err(|_| "failed to read local runtime host status".to_string())?;
        if is_remote_runtime_base_url(&status.base_url) {
            return Ok(String::new());
        }
        return Err("local runtime host auth token is not initialized".to_string());
    }
    Ok(token.clone())
}

pub fn ensure_local_runtime_host(
    _app: &AppHandle,
    state: &LocalRuntimeHostState,
) -> Result<LocalRuntimeHostStatus, String> {
    let configured_base_url = configured_runtime_base_url();
    let port = extract_port_from_base_url(&configured_base_url).unwrap_or(443);

    update_status(state, |status| {
        status.running = true;
        status.status = "running".to_string();
        status.port = port;
        status.base_url = configured_base_url.clone();
        status.pid = None;
        if status.started_at.is_none() {
            status.started_at = Some(now_ms());
        }
        status.last_error = None;
        status.updated_at = now_ms();
    })?;

    current_runtime_host_status(state)
}

fn generate_runtime_auth_token() -> String {
    let mut bytes = [0u8; 24];
    OsRng.fill_bytes(&mut bytes);
    bytes
        .iter()
        .map(|byte| format!("{:02x}", byte))
        .collect::<String>()
}

pub fn stop_local_runtime_host(
    _app: &AppHandle,
    state: &LocalRuntimeHostState,
) -> Result<LocalRuntimeHostStatus, String> {
    let base_url = configured_runtime_base_url();
    let port = extract_port_from_base_url(&base_url).unwrap_or(443);

    update_status(state, |status| {
        status.running = false;
        status.status = "dormant".to_string();
        status.port = port;
        status.base_url = base_url.clone();
        status.pid = None;
        status.started_at = None;
        status.last_error = None;
        status.updated_at = now_ms();
    })?;
    current_runtime_host_status(state)
}

impl Default for LocalRuntimeHostState {
    fn default() -> Self {
        Self {
            status: Mutex::new(LocalRuntimeHostStatus::default()),
            child: Mutex::new(None),
            auth_token: Mutex::new(String::new()),
        }
    }
}

fn runtime_port() -> u16 {
    std::env::var("COMPOSE_LOCAL_RUNTIME_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .filter(|port| *port > 0)
        .unwrap_or(LOCAL_RUNTIME_DEFAULT_PORT)
}

fn configured_runtime_base_url() -> String {
    for candidate in [
        std::env::var("RUNTIME_URL").ok(),
        std::env::var("COMPOSE_RUNTIME_URL").ok(),
        std::env::var("VITE_RUNTIME_URL").ok(),
        option_env!("RUNTIME_URL").map(|value| value.to_string()),
        option_env!("COMPOSE_RUNTIME_URL").map(|value| value.to_string()),
        option_env!("VITE_RUNTIME_URL").map(|value| value.to_string()),
        Some(DEFAULT_RUNTIME_URL.to_string()),
    ] {
        if let Some(normalized) = candidate
            .as_deref()
            .and_then(normalize_runtime_base_url)
        {
            return normalized;
        }
    }

    DEFAULT_RUNTIME_URL.to_string()
}

fn normalize_runtime_base_url(value: &str) -> Option<String> {
    let trimmed = value.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return None;
    }
    if !(trimmed.starts_with("https://") || trimmed.starts_with("http://")) {
        return None;
    }
    Some(trimmed.to_string())
}

fn is_remote_runtime_base_url(base_url: &str) -> bool {
    let lower = base_url.trim().to_ascii_lowercase();
    lower.starts_with("https://")
        || (!lower.contains("://127.0.0.1")
            && !lower.contains("://localhost")
            && !lower.contains("://0.0.0.0")
            && !lower.contains("://[::1]")
            && !lower.contains("://::1"))
}

fn extract_port_from_base_url(base_url: &str) -> Option<u16> {
    let (_, rest) = base_url.split_once("://")?;
    let authority = rest.split('/').next()?.trim();
    if authority.is_empty() {
        return None;
    }

    if let Some(host_and_port) = authority.strip_prefix('[') {
        let (_, port) = host_and_port.split_once("]:")?;
        return port.parse::<u16>().ok();
    }

    let (_, port) = authority.rsplit_once(':')?;
    port.parse::<u16>().ok()
}

fn runtime_stdout_log_path(app: &AppHandle) -> Result<PathBuf, String> {
    let base_dir = resolve_base_dir(app)?;
    fs::create_dir_all(&base_dir)
        .map_err(|err| format!("failed to create runtime host log dir: {err}"))?;
    Ok(base_dir.join("runtime-host.stdout.log"))
}

fn runtime_stderr_log_path(app: &AppHandle) -> Result<PathBuf, String> {
    let base_dir = resolve_base_dir(app)?;
    fs::create_dir_all(&base_dir)
        .map_err(|err| format!("failed to create runtime host log dir: {err}"))?;
    Ok(base_dir.join("runtime-host.stderr.log"))
}

fn update_status(
    state: &LocalRuntimeHostState,
    updater: impl FnOnce(&mut LocalRuntimeHostStatus),
) -> Result<(), String> {
    let mut status = state
        .status
        .lock()
        .map_err(|_| "failed to lock local runtime host status".to_string())?;
    updater(&mut status);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_local_runtime_base_url_uses_loopback_host() {
        assert_eq!(build_local_runtime_base_url(4310), "http://127.0.0.1:4310");
    }

    #[test]
    fn normalize_runtime_base_url_rejects_non_http_values() {
        assert_eq!(normalize_runtime_base_url(""), None);
        assert_eq!(normalize_runtime_base_url("ws://runtime.compose.market"), None);
        assert_eq!(
            normalize_runtime_base_url("https://runtime.compose.market/"),
            Some("https://runtime.compose.market".to_string())
        );
    }

    #[test]
    fn is_remote_runtime_base_url_detects_non_loopback_hosts() {
        assert!(is_remote_runtime_base_url("https://runtime.compose.market"));
        assert!(is_remote_runtime_base_url("http://10.0.0.5:4310"));
        assert!(!is_remote_runtime_base_url("http://127.0.0.1:4310"));
        assert!(!is_remote_runtime_base_url("http://localhost:4310"));
    }

    #[test]
    fn extract_port_from_base_url_handles_explicit_and_default_ports() {
        assert_eq!(extract_port_from_base_url("https://runtime.compose.market"), None);
        assert_eq!(
            extract_port_from_base_url("https://runtime.compose.market:8443"),
            Some(8443)
        );
        assert_eq!(extract_port_from_base_url("http://127.0.0.1:4310"), Some(4310));
    }
}
