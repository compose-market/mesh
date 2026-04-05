use super::*;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DaemonPermissionPolicy {
    pub(crate) shell: String,
    pub(crate) filesystem_read: String,
    pub(crate) filesystem_write: String,
    pub(crate) filesystem_edit: String,
    pub(crate) filesystem_delete: String,
    pub(crate) camera: String,
    pub(crate) microphone: String,
    pub(crate) network: String,
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

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OsPermissionSnapshot {
    pub(crate) location: String,
    pub(crate) camera: String,
    pub(crate) microphone: String,
    pub(crate) screen: String,
    pub(crate) full_disk_access: String,
    pub(crate) accessibility: String,
}

pub(crate) fn normalize_daemon_decision(value: &str) -> String {
    match value.trim().to_lowercase().as_str() {
        "allow" => "allow".to_string(),
        _ => "deny".to_string(),
    }
}

pub(crate) fn normalize_daemon_permission_policy(
    policy: DaemonPermissionPolicy,
) -> DaemonPermissionPolicy {
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

pub(crate) fn select_desired_permission_policy(
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

pub(crate) fn permission_allowed(value: &str) -> bool {
    value.trim().eq_ignore_ascii_case("allow")
}

pub(crate) fn ensure_filesystem_read_permission(
    permissions: &DaemonPermissionPolicy,
) -> Result<(), String> {
    if permission_allowed(&permissions.filesystem_read) {
        Ok(())
    } else {
        Err("filesystemRead permission denied".to_string())
    }
}

pub(crate) fn ensure_filesystem_write_or_edit_permission(
    permissions: &DaemonPermissionPolicy,
) -> Result<(), String> {
    if permission_allowed(&permissions.filesystem_write)
        || permission_allowed(&permissions.filesystem_edit)
    {
        Ok(())
    } else {
        Err("filesystemWrite/filesystemEdit permission denied".to_string())
    }
}

pub(crate) fn ensure_shell_permission(permissions: &DaemonPermissionPolicy) -> Result<(), String> {
    if permission_allowed(&permissions.shell) {
        Ok(())
    } else {
        Err("shell permission denied".to_string())
    }
}

pub(crate) fn ensure_network_permission(
    permissions: &DaemonPermissionPolicy,
) -> Result<(), String> {
    if permission_allowed(&permissions.network) {
        Ok(())
    } else {
        Err("network permission denied".to_string())
    }
}

pub(crate) fn desired_local_agent_permissions(
    agent: &PersistedInstalledAgent,
) -> DaemonPermissionPolicy {
    let normalized_permissions = normalize_daemon_permission_policy(agent.permissions.clone());
    let normalized_desired_permissions =
        normalize_daemon_permission_policy(agent.desired_permissions.clone());

    select_desired_permission_policy(&normalized_permissions, &normalized_desired_permissions)
}

pub(crate) fn resolve_local_agent_permissions(
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

    normalize_daemon_permission_policy(desired_permissions)
}

pub(crate) fn format_local_agent_permissions_for_prompt(
    permissions: &DaemonPermissionPolicy,
) -> String {
    [
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
    .join("\n")
}

pub(crate) fn normalize_daemon_agent_permissions_entry(agent: &mut DaemonAgentState) {
    let normalized_permissions = normalize_daemon_permission_policy(agent.permissions.clone());
    let normalized_desired_permissions =
        normalize_daemon_permission_policy(agent.desired_permissions.clone());

    agent.permissions = normalized_permissions.clone();
    agent.desired_permissions =
        select_desired_permission_policy(&normalized_permissions, &normalized_desired_permissions);
}

pub(crate) fn normalize_daemon_state_for_local_mode(daemon: &mut DaemonStateFile) {
    for agent in daemon.agents.values_mut() {
        normalize_daemon_agent_permissions_entry(agent);

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

pub(crate) fn resolved_daemon_agent_status(agent: &DaemonAgentState) -> DaemonAgentState {
    let mut snapshot = agent.clone();
    let desired_permissions = {
        let normalized_permissions =
            normalize_daemon_permission_policy(snapshot.permissions.clone());
        let normalized_desired_permissions =
            normalize_daemon_permission_policy(snapshot.desired_permissions.clone());
        select_desired_permission_policy(&normalized_permissions, &normalized_desired_permissions)
    };
    snapshot.desired_permissions = desired_permissions.clone();
    snapshot.permissions = normalize_daemon_permission_policy(desired_permissions);
    snapshot
}

#[tauri::command]
pub(crate) fn daemon_update_permissions(
    app: tauri::AppHandle,
    state: tauri::State<'_, LocalDaemonState>,
    agent_wallet: String,
    policy: DaemonPermissionPolicy,
) -> Result<DaemonAgentState, String> {
    let wallet =
        normalize_wallet(&agent_wallet).ok_or_else(|| "invalid agentWallet".to_string())?;
    let normalized_policy = normalize_daemon_permission_policy(policy);

    with_daemon_state(&app, &state, |daemon| {
        let entry = daemon
            .agents
            .get_mut(&wallet)
            .ok_or_else(|| format!("agent not installed: {wallet}"))?;
        entry.desired_permissions = normalized_policy.clone();
        entry.permissions = normalized_policy.clone();
        entry.updated_at = now_ms();
        Ok(entry.clone())
    })
}

#[tauri::command]
pub(crate) fn daemon_get_agent_status(
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
    Ok(guard.agents.get(&wallet).map(resolved_daemon_agent_status))
}

#[cfg(target_os = "macos")]
mod platform {
    use super::*;

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

    fn location_authorization_status(value: i32) -> String {
        match value {
            3 | 4 => "granted".to_string(),
            _ => "denied".to_string(),
        }
    }

    fn av_authorization_status(value: i32) -> String {
        match value {
            3 => "granted".to_string(),
            _ => "denied".to_string(),
        }
    }

    fn query_location_permission() -> String {
        unsafe { location_authorization_status(compose_mesh_location_authorization_status()) }
    }

    fn query_camera_permission() -> String {
        unsafe { av_authorization_status(compose_mesh_camera_authorization_status()) }
    }

    fn query_microphone_permission() -> String {
        unsafe { av_authorization_status(compose_mesh_microphone_authorization_status()) }
    }

    fn query_screen_permission() -> String {
        if unsafe { compose_mesh_preflight_screen_capture_access() } {
            "granted".to_string()
        } else {
            query_tcc_status("kTCCServiceScreenCapture")
        }
    }

    fn query_accessibility_permission() -> String {
        if unsafe { compose_mesh_accessibility_is_trusted() } {
            "granted".to_string()
        } else {
            query_tcc_status("kTCCServiceAccessibility")
        }
    }

    fn resolve_home_dir(app: Option<&tauri::AppHandle>) -> Option<PathBuf> {
        app.and_then(|handle| handle.path().home_dir().ok())
            .or_else(|| std::env::var_os("HOME").map(PathBuf::from))
    }

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
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                "denied".to_string()
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => "denied".to_string(),
            Err(_) => query_tcc_status("kTCCServiceSystemPolicyAllFiles"),
        }
    }

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

    pub(super) fn query_os_permissions_snapshot(
        app: Option<&tauri::AppHandle>,
    ) -> OsPermissionSnapshot {
        OsPermissionSnapshot {
            location: query_location_permission(),
            camera: query_camera_permission(),
            microphone: query_microphone_permission(),
            screen: query_screen_permission(),
            full_disk_access: query_full_disk_access_permission(app),
            accessibility: query_accessibility_permission(),
        }
    }

    pub(super) fn open_system_settings(permission_key: Option<String>) -> Result<(), String> {
        let anchor = permission_key
            .as_deref()
            .map(permission_key_to_system_prefs_anchor)
            .unwrap_or("Privacy");
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

    pub(super) fn request_os_permission(
        app: tauri::AppHandle,
        permission_key: String,
    ) -> Result<OsPermissionSnapshot, String> {
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
                let _ = open_system_settings(Some(permission_key.clone()));
            }
            "accessibility" => {
                let _ = unsafe { compose_mesh_prompt_accessibility_access() };
                let _ = open_system_settings(Some(permission_key.clone()));
            }
            "fullDiskAccess" => {
                let _ = open_system_settings(Some(permission_key.clone()));
            }
            _ => {
                let _ = open_system_settings(Some(permission_key.clone()));
            }
        }

        Ok(query_os_permissions_snapshot(Some(&app)))
    }
}

#[cfg(not(target_os = "macos"))]
fn denied_os_permissions_snapshot() -> OsPermissionSnapshot {
    OsPermissionSnapshot {
        location: "denied".to_string(),
        camera: "denied".to_string(),
        microphone: "denied".to_string(),
        screen: "denied".to_string(),
        full_disk_access: "denied".to_string(),
        accessibility: "denied".to_string(),
    }
}

#[cfg(not(target_os = "macos"))]
macro_rules! unsupported_permissions_platform {
    () => {
        pub(super) fn query_os_permissions_snapshot(
            _app: Option<&tauri::AppHandle>,
        ) -> OsPermissionSnapshot {
            denied_os_permissions_snapshot()
        }

        pub(super) fn open_system_settings(_permission_key: Option<String>) -> Result<(), String> {
            Err("System Settings is only available on macOS".to_string())
        }

        pub(super) fn request_os_permission(
            app: tauri::AppHandle,
            permission_key: String,
        ) -> Result<OsPermissionSnapshot, String> {
            let _ = permission_key;
            Ok(query_os_permissions_snapshot(Some(&app)))
        }
    };
}

#[cfg(target_os = "android")]
mod platform {
    use super::*;
    unsupported_permissions_platform!();
}

#[cfg(target_os = "windows")]
mod platform {
    use super::*;
    unsupported_permissions_platform!();
}

#[cfg(target_os = "linux")]
mod platform {
    use super::*;
    unsupported_permissions_platform!();
}

#[cfg(target_os = "ios")]
mod platform {
    use super::*;
    unsupported_permissions_platform!();
}

#[cfg(not(any(
    target_os = "macos",
    target_os = "android",
    target_os = "windows",
    target_os = "linux",
    target_os = "ios"
)))]
mod platform {
    use super::*;
    unsupported_permissions_platform!();
}

pub(crate) fn query_os_permissions_snapshot(
    app: Option<&tauri::AppHandle>,
) -> OsPermissionSnapshot {
    platform::query_os_permissions_snapshot(app)
}

#[tauri::command]
pub(crate) fn daemon_query_os_permissions(
    app: tauri::AppHandle,
) -> Result<OsPermissionSnapshot, String> {
    Ok(query_os_permissions_snapshot(Some(&app)))
}

#[tauri::command]
pub(crate) fn daemon_open_system_settings(permission_key: Option<String>) -> Result<(), String> {
    platform::open_system_settings(permission_key)
}

#[tauri::command]
pub(crate) fn daemon_request_os_permission(
    app: tauri::AppHandle,
    permission_key: String,
) -> Result<OsPermissionSnapshot, String> {
    platform::request_os_permission(app, permission_key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_daemon_permission_policy_coerces_unknown_values_to_deny() {
        let normalized = normalize_daemon_permission_policy(DaemonPermissionPolicy {
            shell: "ALLOW".to_string(),
            filesystem_read: "maybe".to_string(),
            filesystem_write: "allow".to_string(),
            filesystem_edit: " deny ".to_string(),
            filesystem_delete: String::new(),
            camera: "ALLOW".to_string(),
            microphone: "x".to_string(),
            network: "allow".to_string(),
        });

        assert_eq!(normalized.shell, "allow");
        assert_eq!(normalized.filesystem_read, "deny");
        assert_eq!(normalized.filesystem_write, "allow");
        assert_eq!(normalized.filesystem_edit, "deny");
        assert_eq!(normalized.filesystem_delete, "deny");
        assert_eq!(normalized.camera, "allow");
        assert_eq!(normalized.microphone, "deny");
        assert_eq!(normalized.network, "allow");
    }

    #[test]
    fn select_desired_permission_policy_prefers_existing_when_desired_is_default() {
        let current = DaemonPermissionPolicy {
            network: "allow".to_string(),
            ..DaemonPermissionPolicy::default()
        };
        let desired = DaemonPermissionPolicy::default();

        let selected = select_desired_permission_policy(&current, &desired);
        assert_eq!(selected, current);
    }

    #[test]
    fn format_local_agent_permissions_for_prompt_keeps_frontend_key_names() {
        let permissions = DaemonPermissionPolicy {
            shell: "allow".to_string(),
            filesystem_read: "allow".to_string(),
            filesystem_write: "deny".to_string(),
            filesystem_edit: "allow".to_string(),
            network: "deny".to_string(),
            ..DaemonPermissionPolicy::default()
        };

        let lines = format_local_agent_permissions_for_prompt(&permissions);
        assert!(lines.contains("- shell: allow"));
        assert!(lines.contains("- network: deny"));
        assert!(lines.contains("- filesystemRead: allow"));
        assert!(lines.contains("- filesystemWrite: deny"));
        assert!(lines.contains("- filesystemEdit: allow"));
    }

    #[test]
    fn permission_guards_return_expected_error_messages() {
        let denied = DaemonPermissionPolicy::default();

        assert_eq!(
            ensure_filesystem_read_permission(&denied).unwrap_err(),
            "filesystemRead permission denied"
        );
        assert_eq!(
            ensure_filesystem_write_or_edit_permission(&denied).unwrap_err(),
            "filesystemWrite/filesystemEdit permission denied"
        );
        assert_eq!(
            ensure_shell_permission(&denied).unwrap_err(),
            "shell permission denied"
        );
        assert_eq!(
            ensure_network_permission(&denied).unwrap_err(),
            "network permission denied"
        );
    }

    #[test]
    fn filesystem_write_or_edit_permission_accepts_either_capability() {
        let write_allowed = DaemonPermissionPolicy {
            filesystem_write: "allow".to_string(),
            ..DaemonPermissionPolicy::default()
        };
        let edit_allowed = DaemonPermissionPolicy {
            filesystem_edit: "allow".to_string(),
            ..DaemonPermissionPolicy::default()
        };

        assert!(ensure_filesystem_write_or_edit_permission(&write_allowed).is_ok());
        assert!(ensure_filesystem_write_or_edit_permission(&edit_allowed).is_ok());
    }

    #[cfg(not(target_os = "macos"))]
    #[test]
    fn non_macos_snapshot_defaults_to_denied() {
        let snapshot = query_os_permissions_snapshot(None);
        assert_eq!(snapshot.location, "denied");
        assert_eq!(snapshot.camera, "denied");
        assert_eq!(snapshot.microphone, "denied");
        assert_eq!(snapshot.screen, "denied");
        assert_eq!(snapshot.full_disk_access, "denied");
        assert_eq!(snapshot.accessibility, "denied");
    }
}
