#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::Mutex;
use tauri::{Emitter, Manager};

#[derive(Debug, serde::Serialize)]
struct DesktopPaths {
    base_dir: String,
    state_file: String,
    agents_dir: String,
    skills_dir: String,
}

#[derive(Default)]
struct PendingDeepLinks(Mutex<Vec<String>>);

fn resolve_base_dir(app: &tauri::AppHandle) -> Result<PathBuf, String> {
    let base = app
        .path()
        .app_data_dir()
        .map_err(|err| format!("failed to resolve app data directory: {err}"))?
        .join("runtime");
    fs::create_dir_all(&base).map_err(|err| format!("failed to create app data directory: {err}"))?;
    Ok(base)
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

#[cfg(desktop)]
use tauri_plugin_deep_link::DeepLinkExt;

fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_shell::init())
        .plugin(tauri_plugin_deep_link::init())
        .manage(PendingDeepLinks::default())
        .invoke_handler(tauri::generate_handler![
            get_desktop_paths,
            load_desktop_state,
            save_desktop_state,
            ensure_desktop_dir,
            write_desktop_file,
            read_desktop_file,
            remove_desktop_path,
            check_missing_binaries,
            consume_pending_deep_links
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
                            let _ = window.emit("deep-link", serde_json::json!({
                                "url": url_str,
                                "scheme": url.scheme(),
                                "host": url.host_str().unwrap_or(""),
                                "path": url.path(),
                                "query": url.query(),
                            }));
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
