import type {
  AgentMeshInteraction,
  AgentPermissionPolicy,
  AgentTaskReport,
  InstalledAgent,
  MeshAgentCard,
  MeshPeerSignal,
} from "./types";

export type AgentDetailTab = "permissions" | "skills" | "history" | "mesh";

export interface AgentExecutionPolicy {
  grantedPermissions: string[];
  permissionPolicy: Record<string, "allow" | "ask" | "deny">;
}

export interface AgentEconomicsActivity {
  type: "session-spend" | "peer-revenue";
  amountMicros: number;
}

function grantedListFromPolicy(policy: AgentPermissionPolicy): string[] {
  const grants = ["runtime.main", "runtime.cron", "runtime.subagent"];

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

export function buildAgentExecutionPolicy(policy: AgentPermissionPolicy): AgentExecutionPolicy {
  return {
    grantedPermissions: grantedListFromPolicy(policy),
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

function normalizeCaps(values: string[]): string[] {
  return [...new Set(values.map((value) => value.trim()).filter(Boolean))].sort((a, b) => a.localeCompare(b));
}

export function mergeMeshPeerSignals(
  current: MeshPeerSignal[],
  incoming: MeshPeerSignal[],
): MeshPeerSignal[] {
  const merged = new Map<string, MeshPeerSignal>();

  for (const peer of current) {
    merged.set(peer.peerId, { ...peer, caps: normalizeCaps(peer.caps), listenMultiaddrs: [...peer.listenMultiaddrs] });
  }

  for (const peer of incoming) {
    const existing = merged.get(peer.peerId);
    if (!existing || peer.lastSeenAt >= existing.lastSeenAt) {
      merged.set(peer.peerId, {
        ...peer,
        caps: normalizeCaps(peer.caps),
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

function clampList<T>(items: T[], limit: number): T[] {
  if (items.length <= limit) {
    return items;
  }
  return items.slice(0, limit);
}

function latestReportLine(agent: InstalledAgent): string {
  const latest = [...agent.reports].sort((left, right) => right.createdAt - left.createdAt)[0];
  if (!latest) {
    return agent.running ? "Running locally" : "Installed locally";
  }
  return latest.summary || latest.title;
}

export function buildMeshAgentCard(agent: InstalledAgent): MeshAgentCard {
  return {
    name: agent.metadata.name,
    description: agent.metadata.description,
    model: agent.lock.modelId,
    framework: agent.metadata.framework,
    headline: `${agent.metadata.name} on ${agent.metadata.framework}`,
    statusLine: latestReportLine(agent),
    capabilities: normalizeCaps(agent.metadata.plugins.map((plugin) => typeof plugin === "string" ? plugin : plugin.registryId || plugin.name || "")),
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
    reports: clampList([report, ...agent.reports], 128),
  };

  return {
    ...next,
    network: {
      ...next.network,
      publicCard: buildMeshAgentCard(next),
    },
  };
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

export function recordMeshPeerSignal(agent: InstalledAgent, signal: MeshPeerSignal): InstalledAgent {
  const nextPings = clampList(
    mergeMeshPeerSignals(agent.network.recentPings, [signal]),
    32,
  );
  const interaction: AgentMeshInteraction = {
    id: `mesh-${signal.peerId}-${signal.lastSeenAt}`,
    peerId: signal.peerId,
    peerAgentWallet: signal.agentWallet,
    direction: "inbound",
    kind: signal.lastMessageType === "announce" ? "announce" : "signal",
    summary: signalSummary(signal),
    createdAt: signal.lastSeenAt,
  };

  return {
    ...agent,
    network: {
      ...agent.network,
      recentPings: nextPings,
      interactions: clampList([interaction, ...agent.network.interactions], 64),
    },
  };
}
