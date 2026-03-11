import { fetchAgentMetadata } from "../../lib/api";
import type {
  AgentDnaLock,
  AgentMeshInteraction,
  AgentMetadata,
  AgentPermissionPolicy,
  AgentTaskReport,
  InstalledAgent,
  MeshAgentCard,
  MeshPeerSignal,
} from "../../lib/types";

const BASE_RUNTIME_GRANTS = ["runtime.main", "runtime.cron", "runtime.subagent"];
const REPORT_LIMIT = 128;
const SIGNAL_LIMIT = 32;
const INTERACTION_LIMIT = 64;

export type AgentDetailTab = "permissions" | "skills" | "history" | "mesh";

export interface AgentExecutionPolicy {
  grantedPermissions: string[];
  permissionPolicy: Record<string, "allow" | "ask" | "deny">;
}

export interface AgentEconomicsActivity {
  type: "session-spend" | "peer-revenue";
  amountMicros: number;
}

export interface AgentLockInput {
  walletAddress: string;
  agentCardUri: string;
  model: string;
  plugins: AgentMetadata["plugins"];
  chainId: number;
  dnaHash?: string;
}

interface CreateInstalledAgentInput {
  metadata: AgentMetadata;
  lock: AgentDnaLock;
  permissions: AgentPermissionPolicy;
  addedAt?: number;
  runtimeId?: string;
}

function clampList<T>(items: T[], limit: number): T[] {
  return items.length <= limit ? items : items.slice(0, limit);
}

function normalizeList(values: string[]): string[] {
  return [...new Set(values.map((value) => value.trim()).filter(Boolean))].sort((left, right) => left.localeCompare(right));
}

function grantedPermissions(policy: AgentPermissionPolicy): string[] {
  const grants = [...BASE_RUNTIME_GRANTS];

  if (policy.shell === "allow") grants.push("shell");
  if (policy.filesystemRead === "allow") grants.push("fs.read");
  if (policy.filesystemWrite === "allow") grants.push("fs.write");
  if (policy.filesystemEdit === "allow") grants.push("fs.edit");
  if (policy.filesystemDelete === "allow") grants.push("fs.delete");
  if (policy.camera === "allow") grants.push("camera");
  if (policy.microphone === "allow") grants.push("microphone");
  if (policy.network === "allow") grants.push("network");

  return grants;
}

function latestReportLine(agent: InstalledAgent): string {
  const latest = [...agent.reports].sort((left, right) => right.createdAt - left.createdAt)[0];
  if (!latest) {
    return agent.running ? "Running locally" : "Installed locally";
  }
  return latest.summary || latest.title;
}

function extractAgentCardCid(agentCardUri: string): string {
  const cid = agentCardUri.replace("ipfs://", "").split("/")[0];
  if (!cid || cid.length < 32) {
    throw new Error("Invalid agentCardUri CID");
  }
  return cid;
}

async function sha256Hex(input: string): Promise<string> {
  const bytes = new TextEncoder().encode(input);
  const hash = await crypto.subtle.digest("SHA-256", bytes);
  return Array.from(new Uint8Array(hash))
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

function signalSummary(signal: MeshPeerSignal): string {
  const cardName = signal.card?.name?.trim();
  if (cardName) {
    return `${cardName} signaled on the mesh`;
  }
  if (signal.agentWallet) {
    return `${signal.agentWallet.slice(0, 8)} signaled on the mesh`;
  }
  return `${signal.peerId} signaled on the mesh`;
}

function createMeshInteraction(signal: MeshPeerSignal): AgentMeshInteraction {
  return {
    id: `mesh-${signal.peerId}-${signal.lastSeenAt}`,
    peerId: signal.peerId,
    peerAgentWallet: signal.agentWallet,
    direction: "inbound",
    kind: signal.lastMessageType === "announce" ? "announce" : "signal",
    summary: signalSummary(signal),
    createdAt: signal.lastSeenAt,
  };
}

export function listPluginIds(plugins: AgentMetadata["plugins"]): string[] {
  return normalizeList(
    plugins.map((plugin) => (
      typeof plugin === "string" ? plugin : plugin.registryId || plugin.name || ""
    )),
  );
}

export async function buildAgentLock(input: AgentLockInput): Promise<AgentDnaLock> {
  return {
    agentWallet: input.walletAddress.toLowerCase(),
    agentCardCid: extractAgentCardCid(input.agentCardUri),
    modelId: input.model,
    mcpToolsHash: await sha256Hex(listPluginIds(input.plugins).join("|")),
    chainId: input.chainId,
    dnaHash: input.dnaHash || "",
    lockedAt: Date.now(),
  };
}

export function agentLocksMatch(current: AgentDnaLock, next: AgentDnaLock): boolean {
  return (
    current.modelId === next.modelId
    && current.mcpToolsHash === next.mcpToolsHash
    && current.agentCardCid === next.agentCardCid
    && current.chainId === next.chainId
    && current.dnaHash === next.dnaHash
  );
}

export async function validateAgentLock(agent: InstalledAgent, runtimeUrl: string): Promise<void> {
  const canonical = await fetchAgentMetadata({
    runtimeUrl,
    agentWallet: agent.agentWallet,
    agentCardCid: agent.lock.agentCardCid,
  });
  const canonicalLock = await buildAgentLock({
    walletAddress: canonical.walletAddress,
    agentCardUri: canonical.agentCardUri,
    model: canonical.model,
    plugins: canonical.plugins,
    chainId: agent.lock.chainId,
    dnaHash: canonical.dnaHash,
  });

  if (canonicalLock.modelId !== agent.lock.modelId) {
    throw new Error(`Model mismatch for ${agent.agentWallet}: local=${agent.lock.modelId} canonical=${canonicalLock.modelId}`);
  }
  if (canonicalLock.mcpToolsHash !== agent.lock.mcpToolsHash) {
    throw new Error(`MCP tools mismatch for ${agent.agentWallet}`);
  }
  if (canonicalLock.agentCardCid !== agent.lock.agentCardCid) {
    throw new Error(`agentCard CID mismatch for ${agent.agentWallet}`);
  }
}

export function createInstalledAgent(input: CreateInstalledAgentInput): InstalledAgent {
  const agent: InstalledAgent = {
    agentWallet: input.lock.agentWallet,
    metadata: input.metadata,
    lock: input.lock,
    addedAt: input.addedAt || Date.now(),
    running: false,
    runtimeId: input.runtimeId || crypto.randomUUID(),
    heartbeat: {
      enabled: true,
      intervalMs: 30_000,
      lastRunAt: null,
      lastResult: null,
    },
    permissions: { ...input.permissions },
    network: {
      enabled: false,
      status: "dormant",
      peerId: null,
      listenMultiaddrs: [],
      peersDiscovered: 0,
      lastHeartbeatAt: null,
      lastError: null,
      updatedAt: 0,
      publicCard: null,
      recentPings: [],
      interactions: [],
    },
    skillStates: {},
    reports: [],
  };

  return {
    ...agent,
    network: {
      ...agent.network,
      publicCard: buildMeshAgentCard(agent),
    },
  };
}

export function syncInstalledAgent(agent: InstalledAgent, metadata: AgentMetadata, lock: AgentDnaLock): InstalledAgent {
  const next = {
    ...agent,
    agentWallet: lock.agentWallet,
    metadata,
    lock,
  };

  return {
    ...next,
    network: {
      ...next.network,
      publicCard: buildMeshAgentCard(next),
    },
  };
}

export function buildAgentExecutionPolicy(policy: AgentPermissionPolicy): AgentExecutionPolicy {
  return {
    grantedPermissions: grantedPermissions(policy),
    permissionPolicy: {
      shell: policy.shell,
      "fs.read": policy.filesystemRead,
      "fs.write": policy.filesystemWrite,
      "fs.edit": policy.filesystemEdit,
      "fs.delete": policy.filesystemDelete,
      camera: policy.camera,
      microphone: policy.microphone,
      network: policy.network,
    },
  };
}

export function createAgentRoute(agentWallet: string, tab: AgentDetailTab): string {
  return `/agents/${agentWallet.toLowerCase()}/${tab}`;
}

export function mergeMeshPeerSignals(current: MeshPeerSignal[], incoming: MeshPeerSignal[]): MeshPeerSignal[] {
  const merged = new Map<string, MeshPeerSignal>();

  for (const peer of current) {
    merged.set(peer.peerId, {
      ...peer,
      caps: normalizeList(peer.caps),
      listenMultiaddrs: [...peer.listenMultiaddrs],
    });
  }

  for (const peer of incoming) {
    const existing = merged.get(peer.peerId);
    if (!existing || peer.lastSeenAt >= existing.lastSeenAt) {
      merged.set(peer.peerId, {
        ...peer,
        caps: normalizeList(peer.caps),
        listenMultiaddrs: [...peer.listenMultiaddrs],
      });
    }
  }

  return [...merged.values()].sort((left, right) => right.lastSeenAt - left.lastSeenAt);
}

export function summarizeAgentEconomics(activities: AgentEconomicsActivity[]): {
  revenueMicros: number;
  costMicros: number;
  netMicros: number;
} {
  let revenueMicros = 0;
  let costMicros = 0;

  for (const item of activities) {
    if (!Number.isFinite(item.amountMicros) || item.amountMicros <= 0) {
      continue;
    }
    if (item.type === "peer-revenue") {
      revenueMicros += item.amountMicros;
      continue;
    }
    costMicros += item.amountMicros;
  }

  return {
    revenueMicros,
    costMicros,
    netMicros: revenueMicros - costMicros,
  };
}

export function summarizeAgentReportEconomics(reports: AgentTaskReport[]): {
  revenueMicros: number;
  costMicros: number;
  netMicros: number;
} {
  return summarizeAgentEconomics(
    reports.flatMap((report) => {
      const activities: AgentEconomicsActivity[] = [];
      if (typeof report.costMicros === "number" && report.costMicros > 0) {
        activities.push({ type: "session-spend", amountMicros: report.costMicros });
      }
      if (typeof report.revenueMicros === "number" && report.revenueMicros > 0) {
        activities.push({ type: "peer-revenue", amountMicros: report.revenueMicros });
      }
      return activities;
    }),
  );
}

export function buildMeshAgentCard(agent: InstalledAgent): MeshAgentCard {
  return {
    name: agent.metadata.name,
    description: agent.metadata.description,
    model: agent.lock.modelId,
    framework: agent.metadata.framework,
    headline: `${agent.metadata.name} on ${agent.metadata.framework}`,
    statusLine: latestReportLine(agent),
    capabilities: listPluginIds(agent.metadata.plugins),
    updatedAt: Date.now(),
  };
}

export function appendAgentReport(
  agent: InstalledAgent,
  input: Omit<AgentTaskReport, "id" | "createdAt"> & { id?: string; createdAt?: number },
): InstalledAgent {
  const report: AgentTaskReport = {
    ...input,
    id: input.id || `${agent.agentWallet}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    createdAt: input.createdAt || Date.now(),
  };
  const next = {
    ...agent,
    reports: clampList([report, ...agent.reports], REPORT_LIMIT),
  };

  return {
    ...next,
    network: {
      ...next.network,
      publicCard: buildMeshAgentCard(next),
    },
  };
}

export function recordMeshPeerSignal(agent: InstalledAgent, signal: MeshPeerSignal): InstalledAgent {
  return {
    ...agent,
    network: {
      ...agent.network,
      recentPings: clampList(mergeMeshPeerSignals(agent.network.recentPings, [signal]), SIGNAL_LIMIT),
      interactions: clampList([createMeshInteraction(signal), ...agent.network.interactions], INTERACTION_LIMIT),
    },
  };
}
