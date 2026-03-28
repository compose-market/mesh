import { invoke } from "@tauri-apps/api/core";
import type {
  AgentPermissionPolicy,
  AgentWorkerState,
  InstalledAgent,
} from "./types";

export interface DaemonInstallPayload {
  agentWallet: string;
  agentCardCid: string;
  chainId: number;
  modelId: string;
  mcpToolsHash: string;
  dnaHash: string;
}

export interface DaemonAgentStatus {
  agentWallet: string;
  runtimeId: string | null;
  running: boolean;
  status: "stopped" | "starting" | "running" | "stopping" | "error" | string;
  dnaHash: string;
  chainId: number;
  modelId: string;
  mcpToolsHash: string;
  agentCardCid: string;
  desiredPermissions?: AgentPermissionPolicy;
  permissions: AgentPermissionPolicy;
  logsCursor: number;
  lastError: string | null;
  updatedAt: number;
}

export interface DaemonLogTail {
  lines: string[];
  cursor: number;
}

function isTauriRuntime(): boolean {
  return typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;
}

function ensureTauriRuntime(): void {
  if (!isTauriRuntime()) {
    throw new Error("Local daemon commands require Tauri runtime");
  }
}

function toWorkerState(status: DaemonAgentStatus | null | undefined): AgentWorkerState {
  return {
    running: Boolean(status?.running),
    status: (status?.status as AgentWorkerState["status"]) || "stopped",
    runtimeId: status?.runtimeId || null,
    lastHeartbeatAt: null,
    lastError: status?.lastError || null,
    updatedAt: status?.updatedAt || Date.now(),
  };
}

export async function daemonInstallAgent(payload: DaemonInstallPayload): Promise<DaemonAgentStatus> {
  ensureTauriRuntime();
  return invoke<DaemonAgentStatus>("daemon_install_agent", { payload });
}

export async function daemonRemoveAgent(agentWallet: string): Promise<void> {
  ensureTauriRuntime();
  await invoke("daemon_remove_agent", { agentWallet });
}

export async function daemonUpdatePermissions(agentWallet: string, policy: AgentPermissionPolicy): Promise<DaemonAgentStatus> {
  ensureTauriRuntime();
  return invoke<DaemonAgentStatus>("daemon_update_permissions", { agentWallet, policy });
}

export async function daemonGetAgentStatus(agentWallet: string): Promise<DaemonAgentStatus | null> {
  ensureTauriRuntime();
  return invoke<DaemonAgentStatus | null>("daemon_get_agent_status", { agentWallet });
}

export async function daemonTailLogs(agentWallet: string, cursor?: number): Promise<DaemonLogTail> {
  ensureTauriRuntime();
  return invoke<DaemonLogTail>("daemon_tail_logs", { agentWallet, cursor });
}

export function daemonStatusToWorkerState(status: DaemonAgentStatus | null | undefined): AgentWorkerState {
  return toWorkerState(status);
}

export function mergeDaemonStatusIntoInstalledAgent(agent: InstalledAgent, status: DaemonAgentStatus | null | undefined): InstalledAgent {
  if (!status) {
    return agent;
  }

  return {
    ...agent,
    running: Boolean(status.running),
    runtimeId: status.runtimeId || agent.runtimeId,
    desiredPermissions: { ...(status.desiredPermissions || status.permissions) },
    permissions: { ...status.permissions },
    network: {
      ...agent.network,
      updatedAt: status.updatedAt || Date.now(),
    },
    workerState: toWorkerState(status),
  };
}
