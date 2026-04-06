use std::fs;
use std::path::{Path, PathBuf};

use tauri::Manager;

const DEFAULT_RUNTIME_ROOT_DIR_NAME: &str = "Compose Mesh";
const MANAGED_RUNTIME_ENTRIES: [&str; 4] = ["state.json", "daemon_state.json", "agents", "skills"];

#[derive(Debug, serde::Serialize)]
pub(crate) struct LocalPaths {
    pub(crate) base_dir: String,
    pub(crate) state_file: String,
    pub(crate) agents_dir: String,
    pub(crate) skills_dir: String,
}

fn internal_app_data_dir(app: &tauri::AppHandle) -> Result<PathBuf, String> {
    app.path()
        .app_data_dir()
        .map_err(|err| format!("failed to resolve app data directory: {err}"))
}

fn base_dir_override_path(app: &tauri::AppHandle) -> Result<PathBuf, String> {
    Ok(internal_app_data_dir(app)?.join("base_dir_override.txt"))
}

fn default_runtime_root(app: &tauri::AppHandle) -> Result<PathBuf, String> {
    let internal_dir = internal_app_data_dir(app)?;
    let parent = internal_dir
        .parent()
        .ok_or_else(|| "failed to resolve runtime root parent directory".to_string())?;
    Ok(parent.join(DEFAULT_RUNTIME_ROOT_DIR_NAME))
}

fn move_runtime_entry(source: &Path, target: &Path, label: &str) -> Result<(), String> {
    fs::rename(source, target).map_err(|err| format!("failed to migrate {label}: {err}"))
}

fn migrate_legacy_runtime_root(app: &tauri::AppHandle, target_root: &Path) -> Result<(), String> {
    let legacy_root = internal_app_data_dir(app)?;
    if legacy_root == target_root {
        return Ok(());
    }

    let has_managed_content = MANAGED_RUNTIME_ENTRIES
        .iter()
        .any(|name| legacy_root.join(name).exists());
    if !has_managed_content {
        return Ok(());
    }

    fs::create_dir_all(target_root)
        .map_err(|err| format!("failed to create Compose Mesh runtime root: {err}"))?;

    for name in MANAGED_RUNTIME_ENTRIES {
        let source = legacy_root.join(name);
        if !source.exists() {
            continue;
        }

        let target = target_root.join(name);
        if target.exists() {
            continue;
        }

        move_runtime_entry(&source, &target, name)?;
    }

    Ok(())
}

fn persist_base_dir_override(app: &tauri::AppHandle, base_dir: &Path) -> Result<(), String> {
    let default_root = default_runtime_root(app)?;
    let override_path = base_dir_override_path(app)?;

    if base_dir == default_root {
        if override_path.exists() {
            fs::remove_file(&override_path)
                .map_err(|err| format!("failed to clear base dir override: {err}"))?;
        }
        return Ok(());
    }

    let internal_dir = internal_app_data_dir(app)?;
    fs::create_dir_all(&internal_dir)
        .map_err(|err| format!("failed to create app data directory: {err}"))?;
    fs::write(&override_path, base_dir.to_string_lossy().as_ref())
        .map_err(|err| format!("failed to persist base dir override: {err}"))
}

pub(crate) fn resolve_base_dir(app: &tauri::AppHandle) -> Result<PathBuf, String> {
    let override_path = base_dir_override_path(app)?;
    if override_path.exists() {
        let raw = fs::read_to_string(&override_path)
            .map_err(|err| format!("failed to read base dir override: {err}"))?;
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            return Ok(PathBuf::from(trimmed));
        }
    }

    let default_root = default_runtime_root(app)?;
    migrate_legacy_runtime_root(app, &default_root)?;
    Ok(default_root)
}

#[tauri::command]
pub(crate) fn get_local_paths(app: tauri::AppHandle) -> Result<LocalPaths, String> {
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
pub(crate) fn set_local_base_dir(
    app: tauri::AppHandle,
    new_base_dir: String,
) -> Result<LocalPaths, String> {
    let trimmed = new_base_dir.trim();
    if trimmed.is_empty() {
        return Err("base directory path cannot be empty".to_string());
    }

    let new_path = PathBuf::from(trimmed);
    if !new_path.is_absolute() {
        return Err("base directory must be an absolute path".to_string());
    }

    fs::create_dir_all(&new_path)
        .map_err(|err| format!("failed to create new base directory: {err}"))?;
    fs::create_dir_all(new_path.join("agents"))
        .map_err(|err| format!("failed to create agents directory: {err}"))?;
    fs::create_dir_all(new_path.join("skills"))
        .map_err(|err| format!("failed to create skills directory: {err}"))?;

    let old_base = resolve_base_dir(&app)?;
    if old_base != new_path {
        for name in ["state.json", "daemon_state.json"] {
            let source = old_base.join(name);
            let target = new_path.join(name);
            if source.exists() && !target.exists() {
                let _ = fs::copy(&source, &target);
            }
        }
    }

    persist_base_dir_override(&app, &new_path)?;
    get_local_paths(app)
}
