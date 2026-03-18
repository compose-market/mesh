/**
 * Local auto-updater via tauri-plugin-updater.
 *
 * Endpoints are baked into tauri.conf.json → plugins.updater.endpoints
 * pointing at GitHub Releases latest.json.  The native plugin handles
 * signature verification, download, and installation.
 *
 * The Rust‐side `local_check_for_updates` / `local_install_update`
 * invoke commands remain as a fallback for runtime-configured endpoints.
 */

import { invoke } from "@tauri-apps/api/core";

/* ── Types ── */

export interface AvailableLocalUpdate {
  version: string;
  notes: string | null;
  publishedAt: string | null;
}

export interface LocalUpdateState {
  phase: "idle" | "checking" | "available" | "downloading" | "installing" | "error";
  enabled: boolean;
  currentVersion: string | null;
  available: AvailableLocalUpdate | null;
  checkedAt: number | null;
  error: string | null;
  downloadProgress: number | null;
}

/* ── State helpers ── */

export function createLocalUpdateState(): LocalUpdateState {
  return {
    phase: "idle",
    enabled: true,
    currentVersion: null,
    available: null,
    checkedAt: null,
    error: null,
    downloadProgress: null,
  };
}

export function setLocalUpdatePhase(
  current: LocalUpdateState,
  phase: LocalUpdateState["phase"],
): LocalUpdateState {
  return {
    ...current,
    phase,
    error: null,
  };
}

export function setLocalUpdateError(current: LocalUpdateState, message: string): LocalUpdateState {
  return {
    ...current,
    phase: "error",
    error: message,
  };
}

/* ── Runtime detection ── */

function isTauriRuntime(): boolean {
  return typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;
}

/* ── Native plugin API ── */

/**
 * Check for updates using the native tauri-plugin-updater.
 * Reads endpoints + pubkey from tauri.conf.json automatically.
 */
export async function checkForLocalUpdates(): Promise<LocalUpdateState> {
  if (!isTauriRuntime()) {
    return {
      ...createLocalUpdateState(),
      phase: "idle",
      enabled: false,
      checkedAt: Date.now(),
    };
  }

  try {
    const { check } = await import("@tauri-apps/plugin-updater");
    const update = await check();
    const checkedAt = Date.now();

    if (update) {
      return {
        phase: "available",
        enabled: true,
        currentVersion: update.currentVersion,
        available: {
          version: update.version,
          notes: update.body ?? null,
          publishedAt: update.date ?? null,
        },
        checkedAt,
        error: null,
        downloadProgress: null,
      };
    }

    return {
      phase: "idle",
      enabled: true,
      currentVersion: null,
      available: null,
      checkedAt,
      error: null,
      downloadProgress: null,
    };
  } catch (err) {
    return {
      ...createLocalUpdateState(),
      phase: "error",
      checkedAt: Date.now(),
      error: err instanceof Error ? err.message : String(err),
    };
  }
}

/**
 * Download and install the available update, then restart the app.
 * Calls the native plugin directly — no custom invoke commands needed.
 */
export async function installLocalUpdate(
  onProgress?: (percent: number) => void,
): Promise<void> {
  if (!isTauriRuntime()) {
    throw new Error("Local updater requires Tauri runtime");
  }

  const { check } = await import("@tauri-apps/plugin-updater");
  const { relaunch } = await import("@tauri-apps/plugin-process");

  const update = await check();
  if (!update) {
    throw new Error("No update available");
  }

  let downloaded = 0;
  let contentLength = 0;

  await update.downloadAndInstall((event) => {
    switch (event.event) {
      case "Started":
        contentLength = event.data.contentLength ?? 0;
        break;
      case "Progress":
        downloaded += event.data.chunkLength;
        if (contentLength > 0 && onProgress) {
          onProgress(Math.round((downloaded / contentLength) * 100));
        }
        break;
      case "Finished":
        onProgress?.(100);
        break;
    }
  });

  await relaunch();
}

/* ── Legacy invoke fallback (kept for runtime-configured endpoints) ── */

interface LocalUpdaterConfig {
  enabled: boolean;
  pubkey: string | null;
}

interface LocalUpdateCheckResult {
  enabled: boolean;
  available: boolean;
  currentVersion: string | null;
  version: string | null;
  body: string | null;
  date: string | null;
}

function normalizeBase(url: string): string {
  return url.replace(/\/+$/, "");
}

export function buildLocalUpdateEndpoint(apiUrl: string): string {
  return `${normalizeBase(apiUrl)}/api/local/updates/{{target}}/{{arch}}/{{current_version}}`;
}

export async function checkForLocalUpdatesViaApi(apiUrl: string): Promise<LocalUpdateState> {
  if (!isTauriRuntime()) {
    return { ...createLocalUpdateState(), enabled: false, checkedAt: Date.now() };
  }

  try {
    const response = await fetch(`${normalizeBase(apiUrl)}/api/local/updates/config`);
    if (!response.ok) throw new Error(`Config endpoint returned ${response.status}`);
    const config = (await response.json()) as LocalUpdaterConfig;

    if (!config.enabled || !config.pubkey) {
      return { ...createLocalUpdateState(), enabled: false, checkedAt: Date.now() };
    }

    const result = await invoke<LocalUpdateCheckResult>("local_check_for_updates", {
      apiUrl,
      pubkey: config.pubkey,
    });

    const checkedAt = Date.now();
    if (!result.available || !result.version) {
      return {
        phase: "idle",
        enabled: true,
        currentVersion: result.currentVersion,
        available: null,
        checkedAt,
        error: null,
        downloadProgress: null,
      };
    }

    return {
      phase: "available",
      enabled: true,
      currentVersion: result.currentVersion,
      available: {
        version: result.version,
        notes: result.body,
        publishedAt: result.date,
      },
      checkedAt,
      error: null,
      downloadProgress: null,
    };
  } catch (err) {
    return {
      ...createLocalUpdateState(),
      phase: "error",
      checkedAt: Date.now(),
      error: err instanceof Error ? err.message : String(err),
    };
  }
}
