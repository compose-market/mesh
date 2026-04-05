use std::sync::Mutex;

use tauri::AppHandle;

use super::now_ms;

const DEFAULT_RUNTIME_URL: &str = "https://runtime.compose.market";
pub const DEFAULT_RUNTIME_PORT: u16 = 443;

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalHostStatus {
    pub running: bool,
    pub status: String,
    pub port: u16,
    pub base_url: String,
    pub pid: Option<u32>,
    pub started_at: Option<u64>,
    pub last_error: Option<String>,
    pub updated_at: u64,
}

impl Default for LocalHostStatus {
    fn default() -> Self {
        let base_url = configured_runtime_base_url();
        Self {
            running: false,
            status: "dormant".to_string(),
            port: extract_port_from_base_url(&base_url).unwrap_or(DEFAULT_RUNTIME_PORT),
            base_url,
            pid: None,
            started_at: None,
            last_error: None,
            updated_at: now_ms(),
        }
    }
}

#[derive(Default)]
pub struct LocalHostState {
    status: Mutex<LocalHostStatus>,
}

pub fn current_host_status(state: &LocalHostState) -> Result<LocalHostStatus, String> {
    state
        .status
        .lock()
        .map(|status| status.clone())
        .map_err(|_| "failed to read host status".to_string())
}

pub fn ensure_local_host(
    _app: &AppHandle,
    state: &LocalHostState,
) -> Result<LocalHostStatus, String> {
    let base_url = configured_runtime_base_url();
    let port = extract_port_from_base_url(&base_url).unwrap_or(DEFAULT_RUNTIME_PORT);
    let started_at = now_ms();

    update_status(state, |status| {
        status.running = true;
        status.status = "running".to_string();
        status.port = port;
        status.base_url = base_url.clone();
        status.pid = None;
        if status.started_at.is_none() {
            status.started_at = Some(started_at);
        }
        status.last_error = None;
        status.updated_at = now_ms();
    })?;

    current_host_status(state)
}

pub fn stop_local_host(
    _app: &AppHandle,
    state: &LocalHostState,
) -> Result<LocalHostStatus, String> {
    let base_url = configured_runtime_base_url();
    let port = extract_port_from_base_url(&base_url).unwrap_or(DEFAULT_RUNTIME_PORT);

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

    current_host_status(state)
}

fn configured_runtime_base_url() -> String {
    [
        std::env::var("RUNTIME_URL").ok(),
        std::env::var("COMPOSE_RUNTIME_URL").ok(),
        std::env::var("VITE_RUNTIME_URL").ok(),
        option_env!("RUNTIME_URL").map(str::to_string),
        option_env!("COMPOSE_RUNTIME_URL").map(str::to_string),
        option_env!("VITE_RUNTIME_URL").map(str::to_string),
        Some(DEFAULT_RUNTIME_URL.to_string()),
    ]
    .into_iter()
    .flatten()
    .find_map(|value| normalize_runtime_base_url(&value))
    .unwrap_or_else(|| DEFAULT_RUNTIME_URL.to_string())
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

fn update_status(
    state: &LocalHostState,
    updater: impl FnOnce(&mut LocalHostStatus),
) -> Result<(), String> {
    let mut status = state
        .status
        .lock()
        .map_err(|_| "failed to lock runtime host status".to_string())?;
    updater(&mut status);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_runtime_base_url_rejects_non_http_values() {
        assert_eq!(normalize_runtime_base_url(""), None);
        assert_eq!(
            normalize_runtime_base_url("ws://runtime.compose.market"),
            None
        );
        assert_eq!(
            normalize_runtime_base_url("https://runtime.compose.market/"),
            Some("https://runtime.compose.market".to_string())
        );
    }

    #[test]
    fn extract_port_from_base_url_handles_explicit_and_default_ports() {
        assert_eq!(
            extract_port_from_base_url("https://runtime.compose.market"),
            None
        );
        assert_eq!(
            extract_port_from_base_url("https://runtime.compose.market:8443"),
            Some(8443)
        );
        assert_eq!(
            extract_port_from_base_url("http://127.0.0.1:4310"),
            Some(4310)
        );
    }
}
