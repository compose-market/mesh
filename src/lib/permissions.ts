import { invoke } from "@tauri-apps/api/core";
import type {
  AgentPermissionPolicy,
  InstalledAgent,
  LocalRuntimeState,
  OsPermissionSnapshot,
  OsPermissionStatus,
} from "./types";

interface RawOsPermissionSnapshot {
  location: string;
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
    default:
      return "denied";
  }
}

export function createDefaultOsPermissionSnapshot(status: OsPermissionStatus = "denied"): OsPermissionSnapshot {
  return {
    location: status,
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
    location: normalizeOsPermissionStatus(snapshot.location || "denied"),
    camera: normalizeOsPermissionStatus(snapshot.camera || "denied"),
    microphone: normalizeOsPermissionStatus(snapshot.microphone || "denied"),
    screen: normalizeOsPermissionStatus(snapshot.screen || "denied"),
    fullDiskAccess: normalizeOsPermissionStatus(snapshot.fullDiskAccess || "denied"),
    accessibility: normalizeOsPermissionStatus(snapshot.accessibility || "denied"),
  };
}

export function formatOsPermissionStatus(status: OsPermissionStatus): string {
  return status === "granted" ? "Granted" : "Denied";
}

/**
 * Per-agent permissions are pure app-level toggles — the mesh app itself
 * holds macOS TCC grants (Full Disk Access, Camera, etc.).  Individual
 * agents are NOT gated by OS-level status.
 */
function toEffectivePermissions(
  desired: AgentPermissionPolicy,
  _osPermissions: OsPermissionSnapshot,
): AgentPermissionPolicy {
  return { ...desired };
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
    return createDefaultOsPermissionSnapshot("denied");
  }

  const snapshot = await invoke<RawOsPermissionSnapshot>("daemon_query_os_permissions");
  return normalizeOsPermissionSnapshot(snapshot);
}

export async function requestOsPermission(permissionKey: OsPermissionKey): Promise<OsPermissionSnapshot> {
  if (!isTauriRuntime()) {
    return createDefaultOsPermissionSnapshot("denied");
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
