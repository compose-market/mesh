use rand::{rngs::OsRng, RngCore};
use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream};
use std::process::Child;
use std::sync::Mutex;
use std::time::Duration;

use tauri::AppHandle;

use crate::now_ms;

const LOCAL_RUNTIME_HOST: &str = "127.0.0.1";
pub const LOCAL_RUNTIME_DEFAULT_PORT: u16 = 4310;
const LOCAL_RUNTIME_IO_TIMEOUT: Duration = Duration::from_secs(1);

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
        Self {
            running: false,
            status: "dormant".to_string(),
            port: LOCAL_RUNTIME_DEFAULT_PORT,
            base_url: build_local_runtime_base_url(LOCAL_RUNTIME_DEFAULT_PORT),
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
    state
        .auth_token
        .lock()
        .map(|token| {
            if token.trim().is_empty() {
                generate_runtime_auth_token()
            } else {
                token.clone()
            }
        })
        .map_err(|_| "failed to read local runtime host auth token".to_string())
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
    let port = runtime_port();
    let base_url = build_local_runtime_base_url(port);

    {
        let mut child_guard = state
            .child
            .lock()
            .map_err(|_| "failed to lock local runtime host process state".to_string())?;
        if let Some(mut child) = child_guard.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    if runtime_health_check(port).is_ok() {
        let message =
            format!("Local runtime host on port {port} is still responding after shutdown");
        update_status(state, |status| {
            status.running = false;
            status.status = "error".to_string();
            status.port = port;
            status.base_url = base_url.clone();
            status.pid = None;
            status.started_at = None;
            status.last_error = Some(message.clone());
            status.updated_at = now_ms();
        })?;
        return Err(message);
    }

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
            auth_token: Mutex::new(generate_runtime_auth_token()),
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

fn runtime_health_check(port: u16) -> Result<(), String> {
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
    let mut stream = TcpStream::connect_timeout(&address, LOCAL_RUNTIME_IO_TIMEOUT)
        .map_err(|err| format!("failed to connect to local runtime host: {err}"))?;
    stream
        .set_read_timeout(Some(LOCAL_RUNTIME_IO_TIMEOUT))
        .map_err(|err| format!("failed to set local runtime read timeout: {err}"))?;
    stream
        .set_write_timeout(Some(LOCAL_RUNTIME_IO_TIMEOUT))
        .map_err(|err| format!("failed to set local runtime write timeout: {err}"))?;

    let request = format!(
        "GET /health HTTP/1.1\r\nHost: {LOCAL_RUNTIME_HOST}:{port}\r\nConnection: close\r\n\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .map_err(|err| format!("failed to write local runtime health probe: {err}"))?;

    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|err| format!("failed to read local runtime health response: {err}"))?;

    if !response.starts_with("HTTP/1.1 200") && !response.starts_with("HTTP/1.0 200") {
        return Err("local runtime health probe did not return HTTP 200".to_string());
    }
    if !response.contains("\"service\":\"mcp-runtime\"") {
        return Err("local runtime health probe did not identify the runtime service".to_string());
    }
    if !response.contains("\"hostMode\":\"local\"") {
        return Err("local runtime health probe did not confirm local mode".to_string());
    }

    Ok(())
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
}
