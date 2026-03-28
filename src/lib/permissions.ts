import { invoke } from "@tauri-apps/api/core";
import type {
  AgentPermissionPolicy,
  InstalledAgent,
  LocalRuntimeState,
  OsPermissionSnapshot,
  OsPermissionStatus,
} from "./types";

interface RawOsPermissionSnapshot {
  camera: string;
  microphone: string;
  screen: string;
  fullDiskAccess: string;
  accessibility: string;
}

export type OsPermissionKey = keyof RawOsPermissionSnapshot;

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

const DENY_ALL_POLICY: AgentPermissionPolicy = {
  shell: "deny",
  filesystemRead: "deny",
  filesystemWrite: "deny",
  filesystemEdit: "deny",
  filesystemDelete: "deny",
  camera: "deny",
  microphone: "deny",
  network: "deny",
};

function toEffectivePermissions(
  desired: AgentPermissionPolicy,
  osPermissions: OsPermissionSnapshot,
): AgentPermissionPolicy {
  if (osPermissions.fullDiskAccess !== "granted") {
    return { ...DENY_ALL_POLICY };
  }

  return {
    shell: desired.shell,
    filesystemRead: desired.filesystemRead,
    filesystemWrite: desired.filesystemWrite,
    filesystemEdit: desired.filesystemEdit,
    filesystemDelete: desired.filesystemDelete,
    camera: osPermissions.camera === "granted" ? desired.camera : "deny",
    microphone: osPermissions.microphone === "granted" ? desired.microphone : "deny",
    network: desired.network,
  };
}

export function reconcileStateWithOsPermissions(
  state: LocalRuntimeState,
  osPermissions: OsPermissionSnapshot,
): LocalRuntimeState {
  return {
    ...state,
    osPermissions,
    installedAgents: state.installedAgents.map((agent) => {
      const desiredPermissions = agent.desiredPermissions || agent.permissions;
      const permissions = toEffectivePermissions(desiredPermissions, osPermissions);
      const networkAllowed = permissions.network === "allow";

      return {
        ...agent,
        desiredPermissions: { ...desiredPermissions },
        permissions,
        network: {
          ...agent.network,
          enabled: networkAllowed ? agent.network.enabled : false,
          status: networkAllowed ? agent.network.status : "dormant",
        },
      };
    }),
  };
}

export function canAgentUseMesh(
  agent: Pick<InstalledAgent, "permissions"> | null | undefined,
  meshEnabled = true,
): boolean {
  return Boolean(meshEnabled && agent?.permissions.network === "allow");
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
