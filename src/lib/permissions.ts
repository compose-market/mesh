import { invoke } from "@tauri-apps/api/core";
import type { AgentPermissionPolicy, InstalledAgent, LocalRuntimeState, OsPermissionSnapshot, OsPermissionStatus } from "./types";

interface RawOsPermissionSnapshot {
  camera: string;
  microphone: string;
  screen: string;
  fullDiskAccess: string;
  accessibility: string;
}

export type OsPermissionKey = keyof RawOsPermissionSnapshot;

function deniedAgentPermissionPolicy(): AgentPermissionPolicy {
  return {
    shell: "deny",
    filesystemRead: "deny",
    filesystemWrite: "deny",
    filesystemEdit: "deny",
    filesystemDelete: "deny",
    camera: "deny",
    microphone: "deny",
    network: "deny",
  };
}

function desiredAgentPermissionPolicy(agent: InstalledAgent): AgentPermissionPolicy {
  return {
    ...(agent.desiredPermissions || agent.permissions),
  };
}

export function collapseAgentNetworkState(
  agent: Pick<InstalledAgent, "network">,
  updatedAt = Date.now(),
): InstalledAgent["network"] {
  return {
    ...agent.network,
    enabled: false,
    status: "dormant",
    haiId: null,
    peerId: null,
    listenMultiaddrs: [],
    peersDiscovered: 0,
    lastHeartbeatAt: null,
    lastError: null,
    updatedAt,
  };
}

function reconcileAgentWithOsPermissions(agent: InstalledAgent, osPermissions: OsPermissionSnapshot): InstalledAgent {
  const desiredPermissions = desiredAgentPermissionPolicy(agent);
  const updatedAt = Date.now();

  if (!hasGlobalMeshAccess(osPermissions)) {
    return {
      ...agent,
      desiredPermissions,
      permissions: deniedAgentPermissionPolicy(),
      network: collapseAgentNetworkState(agent, updatedAt),
    };
  }

  return {
    ...agent,
    desiredPermissions,
    permissions: { ...desiredPermissions },
    network: desiredPermissions.network === "allow"
      ? {
      ...agent.network,
      enabled: Boolean(agent.network.enabled),
      updatedAt,
    }
      : collapseAgentNetworkState(agent, updatedAt),
  };
}

function isTauriRuntime(): boolean {
  return typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;
}

function normalizeOsPermissionStatus(value: string): OsPermissionStatus {
  switch (value.trim().toLowerCase()) {
    case "authorized":
    case "granted":
      return "granted";
    case "restricted":
    case "denied":
      return "denied";
    case "limited":
      return "granted";
    case "not-determined":
      return "unknown";
    default:
      return "unknown";
  }
}

export function createDefaultOsPermissionSnapshot(status: OsPermissionStatus = "unknown"): OsPermissionSnapshot {
  return {
    camera: status,
    microphone: status,
    screen: status,
    fullDiskAccess: status,
    accessibility: status,
  };
}

function normalizeOsPermissionSnapshot(snapshot: Partial<RawOsPermissionSnapshot> | null | undefined): OsPermissionSnapshot {
  if (!snapshot) {
    return createDefaultOsPermissionSnapshot();
  }

  return {
    camera: normalizeOsPermissionStatus(snapshot.camera || "unknown"),
    microphone: normalizeOsPermissionStatus(snapshot.microphone || "unknown"),
    screen: normalizeOsPermissionStatus(snapshot.screen || "unknown"),
    fullDiskAccess: normalizeOsPermissionStatus(snapshot.fullDiskAccess || "unknown"),
    accessibility: normalizeOsPermissionStatus(snapshot.accessibility || "unknown"),
  };
}

export function formatOsPermissionStatus(status: OsPermissionStatus): string {
  switch (status) {
    case "granted":
      return "Granted";
    case "denied":
      return "Denied";
    case "unsupported":
      return "Unsupported";
    default:
      return "Unknown";
  }
}

export function hasGlobalMeshAccess(osPermissions: OsPermissionSnapshot): boolean {
  return osPermissions.fullDiskAccess === "granted";
}

export function reconcileStateWithOsPermissions(
  state: LocalRuntimeState,
  osPermissions: OsPermissionSnapshot,
): LocalRuntimeState {
  return {
    ...state,
    osPermissions,
    installedAgents: state.installedAgents.map((agent) => reconcileAgentWithOsPermissions(agent, osPermissions)),
  };
}

export function canAgentUseMesh(
  agent: Pick<InstalledAgent, "running" | "permissions"> | null | undefined,
  meshEnabled = true,
): boolean {
  return Boolean(meshEnabled && agent?.running && agent.permissions.network === "allow");
}

export function nextMissingGlobalPermission(osPermissions: OsPermissionSnapshot): OsPermissionKey | null {
  const orderedKeys: OsPermissionKey[] = ["fullDiskAccess", "accessibility", "screen", "camera", "microphone"];
  return orderedKeys.find((key) => osPermissions[key] !== "granted") || null;
}

export async function queryOsPermissions(): Promise<OsPermissionSnapshot> {
  if (!isTauriRuntime()) {
    return createDefaultOsPermissionSnapshot("unsupported");
  }

  const snapshot = await invoke<RawOsPermissionSnapshot>("daemon_query_os_permissions");
  return normalizeOsPermissionSnapshot(snapshot);
}

export async function requestOsPermission(permissionKey: OsPermissionKey): Promise<OsPermissionSnapshot> {
  if (!isTauriRuntime()) {
    return createDefaultOsPermissionSnapshot("unsupported");
  }

  const snapshot = await invoke<RawOsPermissionSnapshot>("daemon_request_os_permission", { permissionKey });
  return normalizeOsPermissionSnapshot(snapshot);
}

export async function openSystemPermissionSettings(permissionKey?: OsPermissionKey): Promise<void> {
  if (!isTauriRuntime()) {
    return;
  }

  await invoke("daemon_open_system_settings", { permissionKey });
}

export async function requestOrOpenMissingGlobalPermission(osPermissions: OsPermissionSnapshot): Promise<OsPermissionSnapshot> {
  const nextPermission = nextMissingGlobalPermission(osPermissions);
  if (!nextPermission) {
    return queryOsPermissions();
  }

  if (nextPermission === "fullDiskAccess") {
    await openSystemPermissionSettings(nextPermission);
    return queryOsPermissions();
  }

  const snapshot = await requestOsPermission(nextPermission);
  if (snapshot[nextPermission] !== "granted") {
    await openSystemPermissionSettings(nextPermission);
  }
  return snapshot;
}

export async function checkAgentPermission(agentWallet: string, permissionKey: string): Promise<boolean> {
  if (!isTauriRuntime()) {
    return false;
  }

  return invoke<boolean>("daemon_check_permission", { agentWallet, permissionKey });
}
