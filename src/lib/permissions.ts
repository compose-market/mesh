import { invoke } from "@tauri-apps/api/core";
import type { OsPermissionStatus } from "./types";

interface TccSnapshot {
  camera: string;
  microphone: string;
  screen: string;
  fullDiskAccess: string;
  accessibility: string;
}

function isTauriRuntime(): boolean {
  return typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;
}

function tccToOsStatus(tcc: string): OsPermissionStatus {
  switch (tcc) {
    case "granted":
      return "granted";
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

export async function queryOsPermissions(): Promise<{
  camera: OsPermissionStatus;
  microphone: OsPermissionStatus;
  screen: OsPermissionStatus;
  fullDiskAccess: OsPermissionStatus;
  accessibility: OsPermissionStatus;
}> {
  if (!isTauriRuntime()) {
    return {
      camera: "unsupported",
      microphone: "unsupported",
      screen: "unsupported",
      fullDiskAccess: "unsupported",
      accessibility: "unsupported",
    };
  }

  const snapshot = await invoke<TccSnapshot>("daemon_query_os_permissions");
  return {
    camera: tccToOsStatus(snapshot.camera),
    microphone: tccToOsStatus(snapshot.microphone),
    screen: tccToOsStatus(snapshot.screen),
    fullDiskAccess: tccToOsStatus(snapshot.fullDiskAccess),
    accessibility: tccToOsStatus(snapshot.accessibility),
  };
}

export async function checkAgentPermission(agentWallet: string, permissionKey: string): Promise<boolean> {
  if (!isTauriRuntime()) {
    return false;
  }

  return invoke<boolean>("daemon_check_permission", { agentWallet, permissionKey });
}
