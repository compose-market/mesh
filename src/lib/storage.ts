import { invoke } from "@tauri-apps/api/core";
import type {
  AgentPermissionPolicy,
  DesktopPaths,
  DesktopRuntimeState,
  InstalledAgent,
  OsPermissionSnapshot,
} from "./types";

const STORAGE_FALLBACK_KEY = "compose_desktop_state_v1";

const defaultPermissions: AgentPermissionPolicy = {
  shell: false,
  filesystemRead: false,
  filesystemWrite: false,
  filesystemEdit: false,
  filesystemDelete: false,
  camera: false,
  microphone: false,
};

const defaultOsPermissions: OsPermissionSnapshot = {
  camera: "unknown",
  microphone: "unknown",
};

const defaultState: DesktopRuntimeState = {
  settings: {
    lambdaUrl: "https://api.compose.market",
    manowarUrl: "https://manowar.compose.market",
  },
  identity: null,
  permissions: { ...defaultPermissions },
  osPermissions: { ...defaultOsPermissions },
  installedAgents: [],
  installedSkills: [],
};

function isTauriRuntime(): boolean {
  return typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;
}

function cloneDefaultState(): DesktopRuntimeState {
  return {
    settings: { ...defaultState.settings },
    identity: null,
    permissions: { ...defaultPermissions },
    osPermissions: { ...defaultOsPermissions },
    installedAgents: [],
    installedSkills: [],
  };
}

function normalizeState(state: Partial<DesktopRuntimeState> | null | undefined): DesktopRuntimeState {
  const base = cloneDefaultState();
  if (!state) return base;

  return {
    settings: {
      lambdaUrl: state.settings?.lambdaUrl || base.settings.lambdaUrl,
      manowarUrl: state.settings?.manowarUrl || base.settings.manowarUrl,
    },
    identity: state.identity || null,
    permissions: {
      shell: Boolean(state.permissions?.shell ?? base.permissions.shell),
      filesystemRead: Boolean(state.permissions?.filesystemRead ?? base.permissions.filesystemRead),
      filesystemWrite: Boolean(state.permissions?.filesystemWrite ?? base.permissions.filesystemWrite),
      filesystemEdit: Boolean(state.permissions?.filesystemEdit ?? base.permissions.filesystemEdit),
      filesystemDelete: Boolean(state.permissions?.filesystemDelete ?? base.permissions.filesystemDelete),
      camera: Boolean(state.permissions?.camera ?? base.permissions.camera),
      microphone: Boolean(state.permissions?.microphone ?? base.permissions.microphone),
    },
    osPermissions: {
      camera: state.osPermissions?.camera || base.osPermissions.camera,
      microphone: state.osPermissions?.microphone || base.osPermissions.microphone,
    },
    installedAgents: Array.isArray(state.installedAgents) ? state.installedAgents : [],
    installedSkills: Array.isArray(state.installedSkills) ? state.installedSkills : [],
  };
}

async function readStateFromTauri(): Promise<DesktopRuntimeState> {
  const raw = await invoke<string>("load_desktop_state");
  const parsed = raw ? (JSON.parse(raw) as Partial<DesktopRuntimeState>) : null;
  return normalizeState(parsed);
}

async function writeStateToTauri(state: DesktopRuntimeState): Promise<void> {
  await invoke("save_desktop_state", {
    stateJson: JSON.stringify(state),
  });
}

function readStateFromFallback(): DesktopRuntimeState {
  const raw = localStorage.getItem(STORAGE_FALLBACK_KEY);
  if (!raw) {
    return cloneDefaultState();
  }
  try {
    return normalizeState(JSON.parse(raw) as Partial<DesktopRuntimeState>);
  } catch {
    return cloneDefaultState();
  }
}

function writeStateToFallback(state: DesktopRuntimeState): void {
  localStorage.setItem(STORAGE_FALLBACK_KEY, JSON.stringify(state));
}

export async function loadRuntimeState(): Promise<DesktopRuntimeState> {
  if (isTauriRuntime()) {
    try {
      return await readStateFromTauri();
    } catch (error) {
      console.error("[storage] Failed to load Tauri state, falling back to localStorage", error);
    }
  }
  return readStateFromFallback();
}

export async function saveRuntimeState(state: DesktopRuntimeState): Promise<void> {
  const normalized = normalizeState(state);
  if (isTauriRuntime()) {
    try {
      await writeStateToTauri(normalized);
      return;
    } catch (error) {
      console.error("[storage] Failed to save Tauri state, using localStorage fallback", error);
    }
  }
  writeStateToFallback(normalized);
}

export async function updateRuntimeState(
  updater: (current: DesktopRuntimeState) => DesktopRuntimeState,
): Promise<DesktopRuntimeState> {
  const current = await loadRuntimeState();
  const next = normalizeState(updater(current));
  await saveRuntimeState(next);
  return next;
}

export async function getDesktopPaths(): Promise<DesktopPaths | null> {
  if (!isTauriRuntime()) {
    return null;
  }
  try {
    return await invoke<DesktopPaths>("get_desktop_paths");
  } catch (error) {
    console.error("[storage] Failed to load desktop paths", error);
    return null;
  }
}

export async function ensureManagedDir(relativePath: string): Promise<string | null> {
  if (!isTauriRuntime()) {
    return null;
  }
  try {
    return await invoke<string>("ensure_desktop_dir", { relativePath });
  } catch (error) {
    console.error(`[storage] Failed to ensure directory ${relativePath}`, error);
    return null;
  }
}

export async function writeManagedFile(relativePath: string, content: string): Promise<string | null> {
  if (!isTauriRuntime()) {
    return null;
  }
  try {
    return await invoke<string>("write_desktop_file", { relativePath, content });
  } catch (error) {
    console.error(`[storage] Failed to write file ${relativePath}`, error);
    return null;
  }
}

export async function readManagedFile(relativePath: string): Promise<string | null> {
  if (!isTauriRuntime()) {
    return null;
  }
  try {
    return await invoke<string>("read_desktop_file", { relativePath });
  } catch {
    return null;
  }
}

export async function removeManagedPath(relativePath: string): Promise<boolean> {
  if (!isTauriRuntime()) {
    return false;
  }
  try {
    return await invoke<boolean>("remove_desktop_path", { relativePath });
  } catch {
    return false;
  }
}

export function getAgentWorkspaceRelativePath(agentWallet: string): string {
  return `agents/${agentWallet.toLowerCase()}`;
}

export function getAgentHeartbeatRelativePath(agentWallet: string): string {
  return `${getAgentWorkspaceRelativePath(agentWallet)}/HEARTBEAT.md`;
}

export function getAgentSkillsRelativePath(agentWallet: string): string {
  return `${getAgentWorkspaceRelativePath(agentWallet)}/skills`;
}

export function getGlobalSkillsRelativePath(): string {
  return "skills";
}

export async function ensureAgentWorkspace(agent: InstalledAgent): Promise<void> {
  const workspaceDir = getAgentWorkspaceRelativePath(agent.agentWallet);
  const skillsDir = getAgentSkillsRelativePath(agent.agentWallet);
  await ensureManagedDir(workspaceDir);
  await ensureManagedDir(skillsDir);

  const heartbeatPath = getAgentHeartbeatRelativePath(agent.agentWallet);
  const existing = await readManagedFile(heartbeatPath);
  if (existing === null) {
    const initial = `# HEARTBEAT\n\nKeep local agent checks lightweight. Reply HEARTBEAT_OK when no action is needed.\n`;
    await writeManagedFile(heartbeatPath, initial);
  }
}

export async function ensureSkillsRoot(): Promise<void> {
  await ensureManagedDir(getGlobalSkillsRelativePath());
}
