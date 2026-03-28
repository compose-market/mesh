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

const REPORT_LIMIT = 128;
const SIGNAL_LIMIT = 32;
const INTERACTION_LIMIT = 64;

export type AgentDetailTab = "chat" | "permissions" | "skills" | "history" | "mesh";

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
    id: `mesh-${signal.id}-${signal.lastSeenAt}`,
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

export async function createInstalledAgent(input: CreateInstalledAgentInput): Promise<InstalledAgent> {
  const createdAt = input.addedAt || Date.now();
  const haiSeed = `${input.lock.agentWallet}:${input.lock.agentCardCid}:${input.lock.modelId}:${input.lock.chainId}`;
  const haiId = `hai-${(await sha256Hex(haiSeed)).slice(0, 40)}`;
  const agent: InstalledAgent = {
    agentWallet: input.lock.agentWallet,
    metadata: input.metadata,
    lock: input.lock,
    addedAt: createdAt,
    running: true,
    runtimeId: input.runtimeId || crypto.randomUUID(),
    heartbeat: {
      enabled: true,
      intervalMs: 30_000,
      lastRunAt: null,
      lastResult: null,
    },
    desiredPermissions: { ...input.permissions },
    permissions: { ...input.permissions },
    network: {
      enabled: false,
      status: "dormant",
      haiId,
      peerId: null,
      listenMultiaddrs: [],
      peersDiscovered: 0,
      lastHeartbeatAt: null,
      lastError: null,
      updatedAt: 0,
      publicCard: null,
      recentPings: [],
      interactions: [],
      manifest: null,
    },
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
    desiredPermissions: { ...(agent.desiredPermissions || agent.permissions) },
  };

  return {
    ...next,
    network: {
      ...next.network,
      publicCard: buildMeshAgentCard(next),
    },
  };
}

export function createAgentRoute(agentWallet: string, tab: AgentDetailTab): string {
  return `/agents/${agentWallet.toLowerCase()}/${tab}`;
}

export function mergeMeshPeerSignals(current: MeshPeerSignal[], incoming: MeshPeerSignal[]): MeshPeerSignal[] {
  const merged = new Map<string, MeshPeerSignal>();

  for (const peer of current) {
    merged.set(peer.id, {
      ...peer,
      caps: normalizeList(peer.caps),
      listenMultiaddrs: [...peer.listenMultiaddrs],
    });
  }

  for (const peer of incoming) {
    const existing = merged.get(peer.id);
    if (!existing || peer.lastSeenAt >= existing.lastSeenAt) {
      merged.set(peer.id, {
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
