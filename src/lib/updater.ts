import { invoke } from "@tauri-apps/api/core";

export interface DesktopUpdaterConfig {
  enabled: boolean;
  pubkey: string | null;
}

export interface DesktopUpdateCheckResult {
  enabled: boolean;
  available: boolean;
  currentVersion: string | null;
  version: string | null;
  body: string | null;
  date: string | null;
}

export interface AvailableDesktopUpdate {
  version: string;
  notes: string | null;
  publishedAt: string | null;
}

export interface DesktopUpdateState {
  phase: "idle" | "checking" | "available" | "installing" | "error";
  enabled: boolean;
  currentVersion: string | null;
  available: AvailableDesktopUpdate | null;
  checkedAt: number | null;
  error: string | null;
}

const DEFAULT_TIMEOUT_MS = 10_000;

function normalizeBase(url: string): string {
  return url.replace(/\/+$/, "");
}

function isTauriRuntime(): boolean {
  return typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;
}

function ensureTauriRuntime(): void {
  if (!isTauriRuntime()) {
    throw new Error("Desktop updater commands require Tauri runtime");
  }
}

async function requestJson<T>(url: string, init: RequestInit, timeoutMs = DEFAULT_TIMEOUT_MS): Promise<T> {
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(url, {
      ...init,
      signal: controller.signal,
      headers: {
        "Content-Type": "application/json",
        ...(init.headers || {}),
      },
    });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`HTTP ${response.status}: ${text || response.statusText}`);
    }
    return await response.json() as T;
  } finally {
    window.clearTimeout(timer);
  }
}

export function buildDesktopUpdateEndpoint(apiUrl: string): string {
  return `${normalizeBase(apiUrl)}/api/desktop/updates/{{target}}/{{arch}}/{{current_version}}`;
}

export function createDesktopUpdateState(): DesktopUpdateState {
  return {
    phase: "idle",
    enabled: false,
    currentVersion: null,
    available: null,
    checkedAt: null,
    error: null,
  };
}

export function applyDesktopUpdateCheck(
  current: DesktopUpdateState,
  result: DesktopUpdateCheckResult,
  checkedAt = Date.now(),
): DesktopUpdateState {
  if (!result.enabled) {
    return {
      ...current,
      phase: "idle",
      enabled: false,
      available: null,
      checkedAt,
      error: null,
    };
  }

  if (!result.available || !result.version) {
    return {
      phase: "idle",
      enabled: true,
      currentVersion: result.currentVersion,
      available: null,
      checkedAt,
      error: null,
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
  };
}

export function setDesktopUpdatePhase(
  current: DesktopUpdateState,
  phase: Extract<DesktopUpdateState["phase"], "checking" | "installing">,
): DesktopUpdateState {
  return {
    ...current,
    phase,
    error: null,
  };
}

export function setDesktopUpdateError(current: DesktopUpdateState, message: string): DesktopUpdateState {
  return {
    ...current,
    phase: "error",
    error: message,
  };
}

export async function fetchDesktopUpdaterConfig(apiUrl: string): Promise<DesktopUpdaterConfig> {
  return requestJson<DesktopUpdaterConfig>(`${normalizeBase(apiUrl)}/api/desktop/updates/config`, {
    method: "GET",
  });
}

export async function checkForDesktopUpdates(apiUrl: string): Promise<DesktopUpdateCheckResult> {
  const config = await fetchDesktopUpdaterConfig(apiUrl);
  if (!config.enabled || !config.pubkey) {
    return {
      enabled: false,
      available: false,
      currentVersion: null,
      version: null,
      body: null,
      date: null,
    };
  }

  ensureTauriRuntime();
  return invoke<DesktopUpdateCheckResult>("desktop_check_for_updates", {
    apiUrl,
    pubkey: config.pubkey,
  });
}

export async function installDesktopUpdate(apiUrl: string): Promise<void> {
  const config = await fetchDesktopUpdaterConfig(apiUrl);
  if (!config.enabled || !config.pubkey) {
    throw new Error("Desktop updater is not configured for this environment");
  }

  ensureTauriRuntime();
  await invoke("desktop_install_update", {
    apiUrl,
    pubkey: config.pubkey,
  });
}
