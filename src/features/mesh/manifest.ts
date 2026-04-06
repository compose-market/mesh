import type { InstalledAgent, InstalledSkill, MeshManifest } from "../../lib/types";

export interface BuildManifestInput {
  agent: InstalledAgent;
  skills: InstalledSkill[];
  userAddress: string;
  deviceId: string;
  chainId: number;
  previousManifest: MeshManifest | null;
  stateRootHash: string | null;
  pdpPieceCid: string | null;
  pdpAnchoredAt: number | null;
}

export interface MeshManifestRuntimeFields {
  peerId: string;
  listenMultiaddrs: string[];
  relayPeerId?: string | null;
}

function dedupeSorted(values: string[]): string[] {
  return [...new Set(values.map((value) => value.trim()).filter(Boolean))].sort((left, right) => left.localeCompare(right));
}

function hasRelayReservationListenMultiaddrs(listenMultiaddrs: string[]): boolean {
  return listenMultiaddrs.some((value) => value.includes("/p2p-circuit"));
}

function manifestComparablePayload(manifest: MeshManifest): string {
  return JSON.stringify({
    agentWallet: manifest.agentWallet,
    userAddress: manifest.userAddress,
    deviceId: manifest.deviceId,
    chainId: manifest.chainId,
    stateRootHash: manifest.stateRootHash,
    pdpPieceCid: manifest.pdpPieceCid,
    pdpAnchoredAt: manifest.pdpAnchoredAt,
    name: manifest.name,
    description: manifest.description,
    model: manifest.model,
    framework: manifest.framework,
    headline: manifest.headline,
    statusLine: manifest.statusLine,
    skills: manifest.skills,
    mcpServers: manifest.mcpServers,
    a2aEndpoints: manifest.a2aEndpoints,
    capabilities: manifest.capabilities,
    agentCardUri: manifest.agentCardUri,
    reputationScore: manifest.reputationScore,
    totalConclaves: manifest.totalConclaves,
    successfulConclaves: manifest.successfulConclaves,
  });
}

export function canonicalManifestPayload(manifest: MeshManifest): string {
  return JSON.stringify({
    agentWallet: manifest.agentWallet,
    userAddress: manifest.userAddress,
    deviceId: manifest.deviceId,
    peerId: manifest.peerId,
    chainId: manifest.chainId,
    stateVersion: manifest.stateVersion,
    stateRootHash: manifest.stateRootHash,
    pdpPieceCid: manifest.pdpPieceCid,
    pdpAnchoredAt: manifest.pdpAnchoredAt,
    name: manifest.name,
    description: manifest.description,
    model: manifest.model,
    framework: manifest.framework,
    headline: manifest.headline,
    statusLine: manifest.statusLine,
    skills: manifest.skills,
    mcpServers: manifest.mcpServers,
    a2aEndpoints: manifest.a2aEndpoints,
    capabilities: manifest.capabilities,
    agentCardUri: manifest.agentCardUri,
    listenMultiaddrs: manifest.listenMultiaddrs,
    relayPeerId: manifest.relayPeerId,
    reputationScore: manifest.reputationScore,
    totalConclaves: manifest.totalConclaves,
    successfulConclaves: manifest.successfulConclaves,
    signedAt: manifest.signedAt,
    signature: manifest.signature,
  });
}

function nextStateVersion(previousManifest: MeshManifest | null, nextManifest: MeshManifest): number {
  if (!previousManifest) {
    return 1;
  }

  return manifestComparablePayload(previousManifest) === manifestComparablePayload(nextManifest)
    ? previousManifest.stateVersion
    : previousManifest.stateVersion + 1;
}

export function hydrateManifestNetworkFields(
  manifest: MeshManifest,
  runtime: MeshManifestRuntimeFields,
): MeshManifest {
  const listenMultiaddrs = dedupeSorted(runtime.listenMultiaddrs);
  return {
    ...manifest,
    peerId: runtime.peerId.trim(),
    listenMultiaddrs,
    relayPeerId: hasRelayReservationListenMultiaddrs(listenMultiaddrs)
      ? runtime.relayPeerId?.trim() || null
      : null,
  };
}

export function buildManifestPayload(input: BuildManifestInput): MeshManifest {
  const { agent, previousManifest } = input;
  const capabilities = dedupeSorted(agent.network.publicCard?.capabilities ?? []);
  const skills = dedupeSorted(
    [
      ...input.skills
        .filter((skill) => skill.enabled)
        .map((skill) => skill.id),
      ...Object.values(agent.skillStates || {})
        .filter((skillState) => skillState.enabled && skillState.eligible)
        .map((skillState) => skillState.skillId),
    ],
  );
  const a2aEndpoints = dedupeSorted(
    [agent.metadata.endpoints?.chat, agent.metadata.endpoints?.stream].filter((endpoint): endpoint is string => typeof endpoint === "string"),
  );
  const mcpServers = dedupeSorted([
    ...(previousManifest?.mcpServers ?? []),
    ...(agent.mcpServers ?? []),
  ]);
  const reputationScore = previousManifest?.reputationScore ?? 0;
  const totalConclaves = previousManifest?.totalConclaves ?? 0;
  const successfulConclaves = previousManifest?.successfulConclaves ?? 0;

  const draft: MeshManifest = {
    agentWallet: agent.agentWallet,
    userAddress: input.userAddress.toLowerCase(),
    deviceId: input.deviceId,
    peerId: "",
    chainId: input.chainId,
    stateVersion: 1,
    stateRootHash: input.stateRootHash,
    pdpPieceCid: input.pdpPieceCid,
    pdpAnchoredAt: input.pdpAnchoredAt,
    name: agent.network.publicCard?.name ?? agent.metadata.name,
    description: agent.metadata.description,
    model: agent.network.publicCard?.model ?? agent.metadata.model,
    framework: agent.network.publicCard?.framework ?? agent.metadata.framework,
    headline: agent.network.publicCard?.headline ?? `${agent.metadata.name} on ${agent.metadata.framework}`,
    statusLine: agent.network.publicCard?.statusLine ?? agent.metadata.description,
    skills,
    mcpServers,
    a2aEndpoints,
    capabilities,
    agentCardUri: agent.metadata.agentCardUri,
    listenMultiaddrs: [],
    relayPeerId: null,
    reputationScore,
    totalConclaves,
    successfulConclaves,
    signedAt: 0,
    signature: "",
  };

  return {
    ...draft,
    stateVersion: nextStateVersion(previousManifest, draft),
  };
}
