use rand::{rngs::OsRng, RngCore};
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

use tauri::{AppHandle, Manager};

use crate::{now_ms, resolve_base_dir, DaemonStateFile};

const LOCAL_RUNTIME_HOST: &str = "127.0.0.1";
pub const LOCAL_RUNTIME_DEFAULT_PORT: u16 = 4310;
const LOCAL_RUNTIME_STARTUP_TIMEOUT: Duration = Duration::from_secs(20);
const LOCAL_RUNTIME_POLL_INTERVAL: Duration = Duration::from_millis(250);
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

pub fn needs_local_runtime_host(daemon_state: &DaemonStateFile) -> bool {
    daemon_state
        .agents
        .values()
        .any(|agent| agent.desired_running)
}

pub fn apply_runtime_host_status(
    daemon_state: &mut DaemonStateFile,
    host_status: &LocalRuntimeHostStatus,
) {
    let runtime_id = host_status
        .running
        .then(|| format!("local-runtime-host:{}", host_status.port));
    let updated_at = now_ms();

    for agent in daemon_state.agents.values_mut() {
        if agent.desired_running {
            agent.running = host_status.running;
            agent.runtime_id = runtime_id.clone();
            agent.status = if host_status.running {
                "running".to_string()
            } else {
                host_status.status.clone()
            };
            agent.last_error = host_status.last_error.clone();
            agent.updated_at = updated_at;
            continue;
        }

        agent.running = false;
        agent.runtime_id = None;
        agent.status = "stopped".to_string();
        agent.last_error = None;
        agent.updated_at = updated_at;
    }
}

pub fn reconcile_local_runtime_host(
    app: &AppHandle,
    state: &LocalRuntimeHostState,
    daemon_state: &DaemonStateFile,
) -> Result<LocalRuntimeHostStatus, String> {
    if needs_local_runtime_host(daemon_state) {
        ensure_local_runtime_host(app, state)
    } else {
        stop_local_runtime_host(app, state)
    }
}

pub fn ensure_local_runtime_host(
    app: &AppHandle,
    state: &LocalRuntimeHostState,
) -> Result<LocalRuntimeHostStatus, String> {
    let port = runtime_port();
    let base_url = build_local_runtime_base_url(port);

    {
        let mut child_guard = state
            .child
            .lock()
            .map_err(|_| "failed to lock local runtime host process state".to_string())?;

        if let Some(child) = child_guard.as_mut() {
            if let Some(exit_status) = child
                .try_wait()
                .map_err(|err| format!("failed to inspect local runtime host process: {err}"))?
            {
                *child_guard = None;
                update_status(state, |status| {
                    status.running = false;
                    status.status = "error".to_string();
                    status.pid = None;
                    status.started_at = None;
                    status.port = port;
                    status.base_url = base_url.clone();
                    status.last_error = Some(format!(
                        "Local runtime host exited unexpectedly ({})",
                        format_exit_status(exit_status)
                    ));
                    status.updated_at = now_ms();
                })?;
            } else if runtime_health_check(port).is_ok() {
                let pid = child.id();
                update_status(state, |status| {
                    status.running = true;
                    status.status = "running".to_string();
                    status.port = port;
                    status.base_url = base_url.clone();
                    status.pid = Some(pid);
                    if status.started_at.is_none() {
                        status.started_at = Some(now_ms());
                    }
                    status.last_error = None;
                    status.updated_at = now_ms();
                })?;
                return current_runtime_host_status(state);
            }
        } else if runtime_health_check(port).is_ok() {
            let message = format!(
                "Local runtime host port {port} is already serving a local runtime outside the daemon supervisor"
            );
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
    }

    update_status(state, |status| {
        status.running = false;
        status.status = "starting".to_string();
        status.port = port;
        status.base_url = base_url.clone();
        status.pid = None;
        status.started_at = None;
        status.last_error = None;
        status.updated_at = now_ms();
    })?;

    let runtime_entry = resolve_runtime_server_entry(app)?;
    let runtime_dir = resolve_runtime_dir(&runtime_entry)?;
    let stdout = OpenOptions::new()
        .create(true)
        .append(true)
        .open(runtime_stdout_log_path(app)?)
        .map_err(|err| format!("failed to open local runtime stdout log: {err}"))?;
    let stderr = OpenOptions::new()
        .create(true)
        .append(true)
        .open(runtime_stderr_log_path(app)?)
        .map_err(|err| format!("failed to open local runtime stderr log: {err}"))?;

    let child = Command::new("node")
        .arg(&runtime_entry)
        .current_dir(&runtime_dir)
        .env("PORT", port.to_string())
        .env("MCP_PORT", port.to_string())
        .env("NODE_ENV", "production")
        .env("RUNTIME_HOST_MODE", "local")
        .env("RUNTIME_DISABLE_TEMPORAL_WORKERS", "true")
        .env("RUNTIME_URL", base_url.clone())
        .env(
            "COMPOSE_LOCAL_RUNTIME_AUTH_TOKEN",
            current_runtime_host_auth_token(state)?,
        )
        .env("COMPOSE_LOCAL_BASE_DIR", resolve_base_dir(app)?)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .map_err(|err| format!("failed to spawn local runtime host: {err}"))?;

    let pid = child.id();
    {
        let mut child_guard = state
            .child
            .lock()
            .map_err(|_| "failed to store local runtime host process".to_string())?;
        *child_guard = Some(child);
    }

    let started_at = now_ms();
    update_status(state, |status| {
        status.running = false;
        status.status = "starting".to_string();
        status.port = port;
        status.base_url = base_url.clone();
        status.pid = Some(pid);
        status.started_at = Some(started_at);
        status.last_error = None;
        status.updated_at = now_ms();
    })?;

    let deadline = Instant::now() + LOCAL_RUNTIME_STARTUP_TIMEOUT;
    loop {
        if runtime_health_check(port).is_ok() {
            update_status(state, |status| {
                status.running = true;
                status.status = "running".to_string();
                status.port = port;
                status.base_url = base_url.clone();
                status.pid = Some(pid);
                status.started_at = Some(started_at);
                status.last_error = None;
                status.updated_at = now_ms();
            })?;
            return current_runtime_host_status(state);
        }

        {
            let mut child_guard = state
                .child
                .lock()
                .map_err(|_| "failed to watch local runtime host process".to_string())?;
            let Some(current_child) = child_guard.as_mut() else {
                break;
            };
            if let Some(exit_status) = current_child
                .try_wait()
                .map_err(|err| format!("failed to inspect local runtime host process: {err}"))?
            {
                *child_guard = None;
                let message = format!(
                    "Local runtime host exited before becoming healthy ({})",
                    format_exit_status(exit_status)
                );
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
        }

        if Instant::now() >= deadline {
            break;
        }
        thread::sleep(LOCAL_RUNTIME_POLL_INTERVAL);
    }

    let message = format!(
        "Local runtime host did not become healthy within {} seconds",
        LOCAL_RUNTIME_STARTUP_TIMEOUT.as_secs()
    );
    let _ = stop_local_runtime_host(app, state);
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
    Err(message)
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

fn resolve_runtime_server_entry(app: &AppHandle) -> Result<PathBuf, String> {
    let mut candidates = Vec::new();

    if let Ok(resource_dir) = app.path().resource_dir() {
        candidates.push(resource_dir.join("runtime").join("dist").join("server.js"));
    }

    candidates.push(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("runtime")
            .join("dist")
            .join("server.js"),
    );

    for candidate in &candidates {
        if candidate.exists() {
            return Ok(candidate.clone());
        }
    }

    Err(format!(
        "Local runtime host entrypoint not found. Expected one of: {}",
        candidates
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    ))
}

fn resolve_runtime_dir(server_entry: &Path) -> Result<PathBuf, String> {
    let dist_dir = server_entry
        .parent()
        .ok_or_else(|| "local runtime host entrypoint is missing its dist directory".to_string())?;
    dist_dir
        .parent()
        .map(|dir| dir.to_path_buf())
        .ok_or_else(|| "local runtime host entrypoint is missing its runtime directory".to_string())
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

fn format_exit_status(status: ExitStatus) -> String {
    status
        .code()
        .map(|code| format!("code {code}"))
        .unwrap_or_else(|| "terminated by signal".to_string())
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
    use crate::{DaemonAgentState, DaemonPermissionPolicy};

    fn daemon_agent(desired_running: bool) -> DaemonAgentState {
        DaemonAgentState {
            agent_wallet: "0x0000000000000000000000000000000000000001".to_string(),
            runtime_id: None,
            desired_running,
            running: false,
            status: "stopped".to_string(),
            dna_hash: "dna".to_string(),
            chain_id: 1,
            model_id: "model".to_string(),
            mcp_tools_hash: "hash".to_string(),
            agent_card_cid: "bafyagent".to_string(),
            permissions: DaemonPermissionPolicy::default(),
            skills: std::collections::HashMap::new(),
            logs_cursor: 0,
            last_error: None,
            updated_at: 0,
        }
    }

    #[test]
    fn build_local_runtime_base_url_uses_loopback_host() {
        assert_eq!(build_local_runtime_base_url(4310), "http://127.0.0.1:4310");
    }

    #[test]
    fn needs_local_runtime_host_when_any_agent_is_desired_running() {
        let mut daemon_state = DaemonStateFile::default();
        daemon_state
            .agents
            .insert("a".to_string(), daemon_agent(false));
        daemon_state
            .agents
            .insert("b".to_string(), daemon_agent(true));

        assert!(needs_local_runtime_host(&daemon_state));
    }

    #[test]
    fn apply_runtime_host_status_marks_desired_agents_running() {
        let mut daemon_state = DaemonStateFile::default();
        daemon_state
            .agents
            .insert("a".to_string(), daemon_agent(true));

        apply_runtime_host_status(
            &mut daemon_state,
            &LocalRuntimeHostStatus {
                running: true,
                status: "running".to_string(),
                port: 4310,
                base_url: build_local_runtime_base_url(4310),
                pid: Some(42),
                started_at: Some(100),
                last_error: None,
                updated_at: 100,
            },
        );

        let agent = daemon_state.agents.get("a").expect("agent");
        assert!(agent.running);
        assert_eq!(agent.runtime_id.as_deref(), Some("local-runtime-host:4310"));
        assert_eq!(agent.status, "running");
    }
}
