import { invoke } from "@tauri-apps/api/core";
import type {
  AgentMeshInteraction,
  AgentNetworkState,
  AgentDnaLock,
  AgentPermissionPolicy,
  MeshManifest,
  AgentTaskReport,
  LocalPaths,
  LocalRuntimeState,
  InstalledAgent,
  LinkedDeploymentIntent,
  MeshAgentCard,
  MeshPeerSignal,
  OsPermissionSnapshot,
  PermissionDecision,
} from "./types";

const STORAGE_FALLBACK_KEY = "compose_mesh_state_v1";

const DEFAULT_API_URL = (
  import.meta.env.VITE_API_URL ||
  import.meta.env.VITE_API_URL ||
  "https://api.compose.market"
).replace(/\/+$/, "");

const defaultPermissions: AgentPermissionPolicy = {
  shell: "deny",
  filesystemRead: "deny",
  filesystemWrite: "deny",
  filesystemEdit: "deny",
  filesystemDelete: "deny",
  camera: "deny",
  microphone: "deny",
  network: "deny",
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
  publicCard: null,
  recentPings: [],
  interactions: [],
  manifest: null,
};

function normalizeMeshAgentCard(value: Partial<MeshAgentCard> | null | undefined): MeshAgentCard | null {
  if (!value) {
    return null;
  }
  const name = typeof value.name === "string" ? value.name.trim() : "";
  const description = typeof value.description === "string" ? value.description.trim() : "";
  const model = typeof value.model === "string" ? value.model.trim() : "";
  const framework = typeof value.framework === "string" ? value.framework.trim() : "";
  const headline = typeof value.headline === "string" ? value.headline.trim() : "";
  const statusLine = typeof value.statusLine === "string" ? value.statusLine.trim() : "";
  const capabilities = Array.isArray(value.capabilities)
    ? [...new Set(value.capabilities.filter((item): item is string => typeof item === "string" && item.trim().length > 0).map((item) => item.trim()))]
    : [];

  if (!name && !headline && !statusLine) {
    return null;
  }

  return {
    name,
    description,
    model,
    framework,
    headline,
    statusLine,
    capabilities,
    updatedAt: Number.isFinite(value.updatedAt) ? Number(value.updatedAt) : Date.now(),
  };
}

function normalizeMeshPeerSignal(value: Partial<MeshPeerSignal> | null | undefined): MeshPeerSignal | null {
  if (!value || typeof value.peerId !== "string" || value.peerId.trim().length === 0) {
    return null;
  }

  const lastMessageType = value.lastMessageType === "presence" || value.lastMessageType === "announce"
    ? value.lastMessageType
    : null;

  return {
    peerId: value.peerId.trim(),
    agentWallet: typeof value.agentWallet === "string" && value.agentWallet.trim().length > 0 ? value.agentWallet.trim().toLowerCase() : null,
    deviceId: typeof value.deviceId === "string" && value.deviceId.trim().length > 0 ? value.deviceId.trim() : null,
    lastSeenAt: Number.isFinite(value.lastSeenAt) ? Number(value.lastSeenAt) : Date.now(),
    stale: Boolean(value.stale),
    caps: Array.isArray(value.caps)
      ? [...new Set(value.caps.filter((item): item is string => typeof item === "string" && item.trim().length > 0).map((item) => item.trim()))].sort()
      : [],
    listenMultiaddrs: Array.isArray(value.listenMultiaddrs)
      ? value.listenMultiaddrs.filter((item): item is string => typeof item === "string" && item.trim().length > 0)
      : [],
    relayPeerId: typeof value.relayPeerId === "string" && value.relayPeerId.trim().length > 0 ? value.relayPeerId.trim() : null,
    anchorHost: typeof value.anchorHost === "string" && value.anchorHost.trim().length > 0 ? value.anchorHost.trim() : null,
    anchorRegion: typeof value.anchorRegion === "string" && value.anchorRegion.trim().length > 0 ? value.anchorRegion.trim() : null,
    anchorProvider: typeof value.anchorProvider === "string" && value.anchorProvider.trim().length > 0 ? value.anchorProvider.trim() : null,
    nodeDistance: Number.isFinite(value.nodeDistance) ? Math.max(1, Number(value.nodeDistance)) : 1,
    signalCount: Number.isFinite(value.signalCount) ? Math.max(0, Number(value.signalCount)) : 0,
    announceCount: Number.isFinite(value.announceCount) ? Math.max(0, Number(value.announceCount)) : 0,
    lastMessageType,
    card: normalizeMeshAgentCard(value.card),
  };
}

function normalizeMeshManifest(value: Partial<MeshManifest> | null | undefined): MeshManifest | null {
  if (!value) {
    return null;
  }

  const agentWallet = typeof value.agentWallet === "string" ? value.agentWallet.trim().toLowerCase() : "";
  const userWallet = typeof value.userWallet === "string" ? value.userWallet.trim().toLowerCase() : "";
  const deviceId = typeof value.deviceId === "string" ? value.deviceId.trim() : "";
  const peerId = typeof value.peerId === "string" ? value.peerId.trim() : "";
  const name = typeof value.name === "string" ? value.name.trim() : "";

  if (!agentWallet || !userWallet || !deviceId || !name) {
    return null;
  }

  const dedupe = (items: unknown): string[] => (
    Array.isArray(items)
      ? [...new Set(items.filter((item): item is string => typeof item === "string" && item.trim().length > 0).map((item) => item.trim()))].sort()
      : []
  );

  return {
    agentWallet,
    userWallet,
    deviceId,
    peerId,
    chainId: Number.isFinite(value.chainId) ? Math.max(1, Number(value.chainId)) : 1,
    stateVersion: Number.isFinite(value.stateVersion) ? Math.max(1, Number(value.stateVersion)) : 1,
    stateRootHash: typeof value.stateRootHash === "string" && value.stateRootHash.trim().length > 0 ? value.stateRootHash.trim() : null,
    pdpPieceCid: typeof value.pdpPieceCid === "string" && value.pdpPieceCid.trim().length > 0 ? value.pdpPieceCid.trim() : null,
    pdpAnchoredAt: Number.isFinite(value.pdpAnchoredAt) ? Number(value.pdpAnchoredAt) : null,
    name,
    description: typeof value.description === "string" ? value.description.trim() : "",
    model: typeof value.model === "string" ? value.model.trim() : "",
    framework: typeof value.framework === "string" ? value.framework.trim() : "",
    headline: typeof value.headline === "string" ? value.headline.trim() : "",
    statusLine: typeof value.statusLine === "string" ? value.statusLine.trim() : "",
    skills: dedupe(value.skills),
    mcpServers: dedupe(value.mcpServers),
    a2aEndpoints: dedupe(value.a2aEndpoints),
    capabilities: dedupe(value.capabilities),
    agentCardUri: typeof value.agentCardUri === "string" ? value.agentCardUri.trim() : "",
    listenMultiaddrs: dedupe(value.listenMultiaddrs),
    relayPeerId: typeof value.relayPeerId === "string" && value.relayPeerId.trim().length > 0 ? value.relayPeerId.trim() : null,
    reputationScore: Number.isFinite(value.reputationScore) ? Math.max(0, Math.min(1, Number(value.reputationScore))) : 0,
    totalConclaves: Number.isFinite(value.totalConclaves) ? Math.max(0, Number(value.totalConclaves)) : 0,
    successfulConclaves: Number.isFinite(value.successfulConclaves) ? Math.max(0, Number(value.successfulConclaves)) : 0,
    signedAt: Number.isFinite(value.signedAt) ? Math.max(0, Number(value.signedAt)) : 0,
    signature: typeof value.signature === "string" ? value.signature.trim() : "",
  };
}

function normalizeMeshInteraction(value: Partial<AgentMeshInteraction> | null | undefined): AgentMeshInteraction | null {
  if (!value || typeof value.id !== "string" || typeof value.peerId !== "string") {
    return null;
  }
  const direction = value.direction === "outbound" ? "outbound" : "inbound";
  const kind = value.kind === "announce" || value.kind === "connect" || value.kind === "disconnect" ? value.kind : "signal";

  return {
    id: value.id,
    peerId: value.peerId,
    peerAgentWallet: typeof value.peerAgentWallet === "string" && value.peerAgentWallet.trim().length > 0
      ? value.peerAgentWallet.trim().toLowerCase()
      : null,
    direction,
    kind,
    summary: typeof value.summary === "string" ? value.summary : "",
    createdAt: Number.isFinite(value.createdAt) ? Number(value.createdAt) : Date.now(),
  };
}

function normalizeAgentReport(value: Partial<AgentTaskReport> | null | undefined): AgentTaskReport | null {
  if (!value || typeof value.id !== "string" || typeof value.title !== "string") {
    return null;
  }
  const kind = (
    value.kind === "deployment" ||
    value.kind === "runtime" ||
    value.kind === "heartbeat" ||
    value.kind === "permission" ||
    value.kind === "skill" ||
    value.kind === "mesh" ||
    value.kind === "economics"
  )
    ? value.kind
    : "runtime";

  const outcome = (
    value.outcome === "success" ||
    value.outcome === "warning" ||
    value.outcome === "error" ||
    value.outcome === "info"
  )
    ? value.outcome
    : "info";

  return {
    id: value.id,
    kind,
    title: value.title,
    summary: typeof value.summary === "string" ? value.summary : "",
    details: typeof value.details === "string" ? value.details : undefined,
    outcome,
    createdAt: Number.isFinite(value.createdAt) ? Number(value.createdAt) : Date.now(),
    costMicros: Number.isFinite(value.costMicros) ? Number(value.costMicros) : undefined,
    revenueMicros: Number.isFinite(value.revenueMicros) ? Number(value.revenueMicros) : undefined,
    peerId: typeof value.peerId === "string" && value.peerId.trim().length > 0 ? value.peerId.trim() : undefined,
  };
}

function normalizePermissionPolicy(value: Partial<AgentPermissionPolicy> | null | undefined): AgentPermissionPolicy {
  const toDecision = (input: unknown, fallback: PermissionDecision): PermissionDecision => {
    if (input === "allow" || input === "deny") {
      return input;
    }
    if (typeof input === "boolean") {
      return input ? "allow" : "deny";
    }
    return fallback;
  };

  return {
    shell: toDecision(value?.shell, defaultPermissions.shell),
    filesystemRead: toDecision(value?.filesystemRead, defaultPermissions.filesystemRead),
    filesystemWrite: toDecision(value?.filesystemWrite, defaultPermissions.filesystemWrite),
    filesystemEdit: toDecision(value?.filesystemEdit, defaultPermissions.filesystemEdit),
    filesystemDelete: toDecision(value?.filesystemDelete, defaultPermissions.filesystemDelete),
    camera: toDecision(value?.camera, defaultPermissions.camera),
    microphone: toDecision(value?.microphone, defaultPermissions.microphone),
    network: toDecision(value?.network, defaultPermissions.network),
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
    publicCard: normalizeMeshAgentCard(value?.publicCard),
    recentPings: Array.isArray(value?.recentPings)
      ? value.recentPings.map((item) => normalizeMeshPeerSignal(item)).filter((item): item is MeshPeerSignal => item !== null).slice(0, 32)
      : [],
    interactions: Array.isArray(value?.interactions)
      ? value.interactions.map((item) => normalizeMeshInteraction(item)).filter((item): item is AgentMeshInteraction => item !== null).slice(0, 64)
      : [],
    manifest: normalizeMeshManifest(value?.manifest),
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

  const lock = (agent.lock || {}) as Partial<AgentDnaLock>;

  return {
    ...(agent as InstalledAgent),
    agentWallet: agent.agentWallet.toLowerCase(),
    lock: {
      agentWallet: (lock.agentWallet || agent.agentWallet).toLowerCase(),
      agentCardCid: lock.agentCardCid || "",
      modelId: lock.modelId || "",
      mcpToolsHash: lock.mcpToolsHash || "",
      lockedAt: Number.isFinite(lock.lockedAt) ? Number(lock.lockedAt) : Date.now(),
      chainId: Number.isFinite(lock.chainId) ? Number(lock.chainId) : 0,
      dnaHash: typeof lock.dnaHash === "string" ? lock.dnaHash : "",
    },
    permissions: normalizedPermissions,
    network: normalizedNetwork,
    skillStates: typeof (agent as InstalledAgent).skillStates === "object" && (agent as InstalledAgent).skillStates !== null
      ? (agent as InstalledAgent).skillStates
      : {},
    reports: Array.isArray((agent as InstalledAgent).reports)
      ? (agent as InstalledAgent).reports.map((item) => normalizeAgentReport(item)).filter((item): item is AgentTaskReport => item !== null).slice(0, 128)
      : [],
  };
}

const defaultState: LocalRuntimeState = {
  settings: {
    apiUrl: DEFAULT_API_URL,
    runtimeUrl: DEFAULT_API_URL,
  },
  identity: null,
  linkedDeployment: null,
  permissionDefaults: { ...defaultPermissions },
  osPermissions: { ...defaultOsPermissions },
  installedAgents: [],
  installedSkills: [],
};

function isTauriRuntime(): boolean {
  return typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;
}

function cloneDefaultState(): LocalRuntimeState {
  return {
    settings: { ...defaultState.settings },
    identity: null,
    linkedDeployment: null,
    permissionDefaults: { ...defaultPermissions },
    osPermissions: { ...defaultOsPermissions },
    installedAgents: [],
    installedSkills: [],
  };
}

function normalizeLinkedDeploymentIntent(value: Partial<LinkedDeploymentIntent> | null | undefined): LinkedDeploymentIntent | null {
  if (!value || typeof value.agentWallet !== "string" || value.agentWallet.trim().length === 0) {
    return null;
  }

  const agentCardCid = typeof value.agentCardCid === "string" && value.agentCardCid.trim().length > 0
    ? value.agentCardCid.trim()
    : null;

  return {
    agentWallet: value.agentWallet.trim().toLowerCase(),
    agentCardCid,
    chainId: Number.isFinite(value.chainId) ? Number(value.chainId) : 0,
    source: "local-link",
    receivedAt: Number.isFinite(value.receivedAt) ? Number(value.receivedAt) : Date.now(),
  };
}

function normalizeState(state: Partial<LocalRuntimeState> | null | undefined): LocalRuntimeState {
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
      apiUrl: state.settings?.apiUrl || base.settings.apiUrl,
      runtimeUrl: state.settings?.runtimeUrl || base.settings.runtimeUrl,
    },
    identity: state.identity || null,
    linkedDeployment: normalizeLinkedDeploymentIntent(state.linkedDeployment),
    permissionDefaults: migratedPermissions,
    osPermissions: {
      camera: state.osPermissions?.camera || base.osPermissions.camera,
      microphone: state.osPermissions?.microphone || base.osPermissions.microphone,
    },
    installedAgents: normalizedAgents,
    installedSkills: Array.isArray(state.installedSkills) ? state.installedSkills : [],
  };
}

async function readStateFromTauri(): Promise<LocalRuntimeState> {
  const raw = await invoke<string>("load_local_state");
  const parsed = raw ? (JSON.parse(raw) as Partial<LocalRuntimeState>) : null;
  return normalizeState(parsed);
}

async function writeStateToTauri(state: LocalRuntimeState): Promise<void> {
  await invoke("save_local_state", {
    stateJson: JSON.stringify(state),
  });
}

function readStateFromFallback(): LocalRuntimeState {
  const raw = localStorage.getItem(STORAGE_FALLBACK_KEY);
  if (!raw) {
    return cloneDefaultState();
  }
  try {
    return normalizeState(JSON.parse(raw) as Partial<LocalRuntimeState>);
  } catch {
    return cloneDefaultState();
  }
}

function writeStateToFallback(state: LocalRuntimeState): void {
  localStorage.setItem(STORAGE_FALLBACK_KEY, JSON.stringify(state));
}

export async function loadRuntimeState(): Promise<LocalRuntimeState> {
  if (isTauriRuntime()) {
    try {
      return await readStateFromTauri();
    } catch (error) {
      console.error("[storage] Failed to load Tauri state, falling back to localStorage", error);
    }
  }
  return readStateFromFallback();
}

export async function saveRuntimeState(state: LocalRuntimeState): Promise<void> {
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
  updater: (current: LocalRuntimeState) => LocalRuntimeState,
): Promise<LocalRuntimeState> {
  const current = await loadRuntimeState();
  const next = normalizeState(updater(current));
  await saveRuntimeState(next);
  return next;
}

export async function getLocalPaths(): Promise<LocalPaths | null> {
  if (!isTauriRuntime()) {
    return null;
  }
  try {
    return await invoke<LocalPaths>("get_local_paths");
  } catch (error) {
    console.error("[storage] Failed to load local paths", error);
    return null;
  }
}

export async function ensureManagedDir(relativePath: string): Promise<string | null> {
  if (!isTauriRuntime()) {
    return null;
  }
  try {
    return await invoke<string>("ensure_local_dir", { relativePath });
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
    return await invoke<string>("write_local_file", { relativePath, content });
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
    return await invoke<string>("read_local_file", { relativePath });
  } catch {
    return null;
  }
}

export async function removeManagedPath(relativePath: string): Promise<boolean> {
  if (!isTauriRuntime()) {
    return false;
  }
  try {
    return await invoke<boolean>("remove_local_path", { relativePath });
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
  const generatedSkillsDir = `${skillsDir}/generated`;
  await ensureManagedDir(workspaceDir);
  await ensureManagedDir(skillsDir);
  await ensureManagedDir(generatedSkillsDir);

  const files: Array<{ path: string; content: string }> = [
    {
      path: getAgentHeartbeatRelativePath(agent.agentWallet),
      content: "# HEARTBEAT\n\nKeep local checks lightweight. Reply HEARTBEAT_OK when no action is needed.\n",
    },
    {
      path: `${workspaceDir}/DNA.md`,
      content: [
        "# DNA",
        `agentWallet: ${agent.lock.agentWallet}`,
        `modelId: ${agent.lock.modelId}`,
        `chainId: ${agent.lock.chainId}`,
        `agentCardCid: ${agent.lock.agentCardCid}`,
        `mcpToolsHash: ${agent.lock.mcpToolsHash}`,
        `dnaHash: ${agent.lock.dnaHash}`,
        `lockedAt: ${agent.lock.lockedAt}`,
        "",
      ].join("\n"),
    },
    {
      path: `${workspaceDir}/SOUL.md`,
      content: "# SOUL\n\nMutable behavior and persona notes for this local deployment.\n",
    },
    {
      path: `${workspaceDir}/AGENTS.md`,
      content: "# AGENTS\n\nPer-agent local operating instructions.\n",
    },
    {
      path: `${workspaceDir}/TOOLS.md`,
      content: "# TOOLS\n\nMCP/GOAT identities are immutable from DNA.md.\n",
    },
    {
      path: `${workspaceDir}/IDENTITY.md`,
      content: `# IDENTITY\n\nagentWallet: ${agent.agentWallet}\n`,
    },
    {
      path: `${workspaceDir}/USER.md`,
      content: "# USER\n\nLocal user preferences and instructions.\n",
    },
  ];

  for (const file of files) {
    const existing = await readManagedFile(file.path);
    if (existing === null) {
      await writeManagedFile(file.path, file.content);
    }
  }
}

export async function ensureSkillsRoot(): Promise<void> {
  await ensureManagedDir(getGlobalSkillsRelativePath());
}

export function getDefaultPermissionPolicy(): AgentPermissionPolicy {
  return { ...defaultPermissions };
}

export function permissionAllows(value: PermissionDecision): boolean {
  return value === "allow";
}

export function permissionPolicyToGrantedList(policy: AgentPermissionPolicy): string[] {
  const granted: string[] = ["runtime.main", "runtime.cron", "runtime.subagent"];
  if (policy.shell === "allow") granted.push("shell");
  if (policy.filesystemRead === "allow") granted.push("fs.read");
  if (policy.filesystemWrite === "allow") granted.push("fs.write");
  if (policy.filesystemEdit === "allow") granted.push("fs.edit");
  if (policy.filesystemDelete === "allow") granted.push("fs.delete");
  if (policy.camera === "allow") granted.push("camera");
  if (policy.microphone === "allow") granted.push("microphone");
  if (policy.network === "allow") granted.push("network");
  return granted;
}

export function getDefaultAgentNetworkState(): AgentNetworkState {
  return {
    ...defaultAgentNetworkState,
    listenMultiaddrs: [],
  };
}
