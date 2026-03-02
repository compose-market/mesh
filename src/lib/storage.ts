import { invoke } from "@tauri-apps/api/core";
import type {
  AgentNetworkState,
  AgentPermissionPolicy,
  DesktopPaths,
  DesktopRuntimeState,
  InstalledAgent,
  OsPermissionSnapshot,
} from "./types";

const STORAGE_FALLBACK_KEY = "compose_desktop_state_v1";
const DEFAULT_LAMBDA_URL = (
  import.meta.env.VITE_API_URL ||
  import.meta.env.VITE_LAMBDA_URL ||
  "https://api.compose.market"
).replace(/\/+$/, "");
const DEFAULT_MANOWAR_URL = (
  import.meta.env.VITE_MANOWAR_URL ||
  "https://manowar.compose.market"
).replace(/\/+$/, "");

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

const defaultAgentNetworkState: AgentNetworkState = {
  enabled: false,
  status: "dormant",
  peerId: null,
  listenMultiaddrs: [],
  peersDiscovered: 0,
  lastHeartbeatAt: null,
  lastError: null,
  updatedAt: 0,
};

function normalizePermissionPolicy(value: Partial<AgentPermissionPolicy> | null | undefined): AgentPermissionPolicy {
  return {
    shell: Boolean(value?.shell ?? defaultPermissions.shell),
    filesystemRead: Boolean(value?.filesystemRead ?? defaultPermissions.filesystemRead),
    filesystemWrite: Boolean(value?.filesystemWrite ?? defaultPermissions.filesystemWrite),
    filesystemEdit: Boolean(value?.filesystemEdit ?? defaultPermissions.filesystemEdit),
    filesystemDelete: Boolean(value?.filesystemDelete ?? defaultPermissions.filesystemDelete),
    camera: Boolean(value?.camera ?? defaultPermissions.camera),
    microphone: Boolean(value?.microphone ?? defaultPermissions.microphone),
  };
}

function normalizeNetworkState(value: Partial<AgentNetworkState> | null | undefined): AgentNetworkState {
  const status = value?.status;
  const normalizedStatus = (
    status === "dormant" ||
    status === "connecting" ||
    status === "online" ||
    status === "error"
  )
    ? status
    : defaultAgentNetworkState.status;

  return {
    enabled: Boolean(value?.enabled ?? defaultAgentNetworkState.enabled),
    status: normalizedStatus,
    peerId: typeof value?.peerId === "string" && value.peerId.trim().length > 0 ? value.peerId.trim() : null,
    listenMultiaddrs: Array.isArray(value?.listenMultiaddrs)
      ? value.listenMultiaddrs.filter((addr): addr is string => typeof addr === "string" && addr.trim().length > 0)
      : [],
    peersDiscovered: Number.isFinite(value?.peersDiscovered) ? Math.max(0, Number(value?.peersDiscovered)) : 0,
    lastHeartbeatAt: Number.isFinite(value?.lastHeartbeatAt) ? Number(value?.lastHeartbeatAt) : null,
    lastError: typeof value?.lastError === "string" && value.lastError.trim().length > 0 ? value.lastError : null,
    updatedAt: Number.isFinite(value?.updatedAt) ? Number(value?.updatedAt) : 0,
  };
}

function normalizeInstalledAgent(
  agent: InstalledAgent | Partial<InstalledAgent>,
  permissionDefaults: AgentPermissionPolicy,
): InstalledAgent | null {
  if (typeof agent.agentWallet !== "string" || agent.agentWallet.trim().length === 0) {
    return null;
  }
  if (!agent.metadata || !agent.lock || !agent.heartbeat || typeof agent.runtimeId !== "string") {
    return null;
  }

  const normalizedPermissions = normalizePermissionPolicy(
    agent.permissions as Partial<AgentPermissionPolicy> | undefined
      || (agent as { permissionPolicy?: Partial<AgentPermissionPolicy> }).permissionPolicy
      || permissionDefaults,
  );
  const normalizedNetwork = normalizeNetworkState(agent.network as Partial<AgentNetworkState> | undefined);

  return {
    ...(agent as InstalledAgent),
    agentWallet: agent.agentWallet.toLowerCase(),
    permissions: normalizedPermissions,
    network: normalizedNetwork,
  };
}

const defaultState: DesktopRuntimeState = {
  settings: {
    lambdaUrl: DEFAULT_LAMBDA_URL,
    manowarUrl: DEFAULT_MANOWAR_URL,
  },
  identity: null,
  permissionDefaults: { ...defaultPermissions },
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
    permissionDefaults: { ...defaultPermissions },
    osPermissions: { ...defaultOsPermissions },
    installedAgents: [],
    installedSkills: [],
  };
}

function normalizeState(state: Partial<DesktopRuntimeState> | null | undefined): DesktopRuntimeState {
  const base = cloneDefaultState();
  if (!state) return base;

  const migratedPermissions = normalizePermissionPolicy(
    state.permissionDefaults ||
    (state as { permissions?: Partial<AgentPermissionPolicy> }).permissions ||
    base.permissionDefaults,
  );
  const normalizedAgents = Array.isArray(state.installedAgents)
    ? state.installedAgents
      .map((agent) => normalizeInstalledAgent(agent, migratedPermissions))
      .filter((agent): agent is InstalledAgent => agent !== null)
    : [];

  return {
    settings: {
      lambdaUrl: state.settings?.lambdaUrl || base.settings.lambdaUrl,
      manowarUrl: state.settings?.manowarUrl || base.settings.manowarUrl,
    },
    identity: state.identity || null,
    permissionDefaults: migratedPermissions,
    osPermissions: {
      camera: state.osPermissions?.camera || base.osPermissions.camera,
      microphone: state.osPermissions?.microphone || base.osPermissions.microphone,
    },
    installedAgents: normalizedAgents,
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

export function getDefaultPermissionPolicy(): AgentPermissionPolicy {
  return { ...defaultPermissions };
}

export function getDefaultAgentNetworkState(): AgentNetworkState {
  return {
    ...defaultAgentNetworkState,
    listenMultiaddrs: [],
  };
}
