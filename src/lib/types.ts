export interface SessionState {
  active: boolean;
  expiresAt: number | null;
  budgetLimit: string | null;
  budgetUsed: string | null;
  budgetLocked: string | null;
  budgetRemaining: string | null;
  sessionId: string | null;
  duration: number | null;
  chainId: number | null;
  reason?: string;
}

export interface AgentMetadata {
  name: string;
  description: string;
  agentCardUri: string;
  creator: string;
  walletAddress: string;
  dnaHash: string;
  model: string;
  framework: string;
  plugins: Array<string | { registryId: string; name?: string; origin?: string }>;
  createdAt: string;
  lastExecutedAt?: string;
  endpoints?: {
    chat?: string;
    stream?: string;
  };
}

export interface ImmutableAgentLock {
  agentWallet: string;
  agentCardCid: string;
  modelId: string;
  mcpToolsHash: string;
  lockedAt: number;
}

export interface AgentDnaLock extends ImmutableAgentLock {
  chainId: number;
  dnaHash: string;
}

export interface AgentHeartbeatState {
  enabled: boolean;
  intervalMs: number;
  lastRunAt: number | null;
  lastResult: "ok" | "alert" | "error" | null;
}

export type AgentReportKind =
  | "deployment"
  | "runtime"
  | "heartbeat"
  | "permission"
  | "skill"
  | "mesh"
  | "economics";

export type AgentReportOutcome = "success" | "warning" | "error" | "info";

export interface AgentTaskReport {
  id: string;
  kind: AgentReportKind;
  title: string;
  summary: string;
  details?: string;
  outcome: AgentReportOutcome;
  createdAt: number;
  costMicros?: number;
  revenueMicros?: number;
  economicsCategory?: "inference" | "heartbeat" | "peer-revenue";
  peerId?: string;
  txHash?: string;
}

export interface InstalledAgent {
  agentWallet: string;
  metadata: AgentMetadata;
  lock: AgentDnaLock;
  addedAt: number;
  running: boolean;
  runtimeId: string;
  heartbeat: AgentHeartbeatState;
  desiredPermissions?: AgentPermissionPolicy;
  permissions: AgentPermissionPolicy;
  mcpServers?: string[];
  network: AgentNetworkState;
  workerState?: AgentWorkerState;
  skillStates?: Record<string, AgentSkillState>;
  reports: AgentTaskReport[];
}

export interface LocalIdentityContext {
  agentWallet: string;
  userAddress: string;
  composeKeyId: string;
  composeKeyToken: string;
  sessionId: string;
  budget: string;
  duration: number;
  chainId: number;
  expiresAt: number;
  deviceId: string;
}

export interface LocalSettings {
  apiUrl: string;
  meshEnabled: boolean;
}

export type PermissionDecision = "allow" | "deny";

export interface AgentPermissionPolicy {
  shell: PermissionDecision;
  filesystemRead: PermissionDecision;
  filesystemWrite: PermissionDecision;
  filesystemEdit: PermissionDecision;
  filesystemDelete: PermissionDecision;
  camera: PermissionDecision;
  microphone: PermissionDecision;
  network: PermissionDecision;
}

export type AgentNetworkStatus = "dormant" | "connecting" | "online" | "error";

export interface MeshAgentCard {
  name: string;
  description: string;
  model: string;
  framework: string;
  headline: string;
  statusLine: string;
  capabilities: string[];
  updatedAt: number;
}

export interface MeshManifest {
  agentWallet: string;
  userAddress: string;
  deviceId: string;
  peerId: string;
  chainId: number;
  stateVersion: number;
  stateRootHash: string | null;
  pdpPieceCid: string | null;
  pdpAnchoredAt: number | null;
  name: string;
  description: string;
  model: string;
  framework: string;
  headline: string;
  statusLine: string;
  skills: string[];
  mcpServers: string[];
  a2aEndpoints: string[];
  capabilities: string[];
  agentCardUri: string;
  listenMultiaddrs: string[];
  relayPeerId: string | null;
  reputationScore: number;
  totalConclaves: number;
  successfulConclaves: number;
  signedAt: number;
  signature: string;
}

export interface MeshPeerSignal {
  id: string;
  peerId: string;
  agentWallet: string | null;
  haiId: string | null;
  deviceId: string | null;
  lastSeenAt: number;
  stale: boolean;
  caps: string[];
  listenMultiaddrs: string[];
  relayPeerId: string | null;
  anchorHost: string | null;
  anchorRegion: string | null;
  anchorProvider: string | null;
  nodeDistance: number;
  signalCount: number;
  announceCount: number;
  lastMessageType: "presence" | "announce" | null;
  card: MeshAgentCard | null;
}

export interface AgentMeshInteraction {
  id: string;
  peerId: string;
  peerAgentWallet: string | null;
  direction: "inbound" | "outbound";
  kind: "signal" | "announce" | "connect" | "disconnect";
  summary: string;
  createdAt: number;
}

export interface AgentNetworkState {
  enabled: boolean;
  status: AgentNetworkStatus;
  haiId: string | null;
  peerId: string | null;
  listenMultiaddrs: string[];
  relayPeerId?: string | null;
  peersDiscovered: number;
  lastHeartbeatAt: number | null;
  lastError: string | null;
  updatedAt: number;
  publicCard: MeshAgentCard | null;
  recentPings: MeshPeerSignal[];
  interactions: AgentMeshInteraction[];
  manifest: MeshManifest | null;
}

export interface AgentWorkerState {
  running: boolean;
  status: "stopped" | "starting" | "running" | "stopping" | "error";
  runtimeId: string | null;
  lastHeartbeatAt: number | null;
  lastError: string | null;
  updatedAt: number;
}

export type OsPermissionStatus = "granted" | "denied";

export interface OsPermissionSnapshot {
  location: OsPermissionStatus;
  camera: OsPermissionStatus;
  microphone: OsPermissionStatus;
  screen: OsPermissionStatus;
  fullDiskAccess: OsPermissionStatus;
  accessibility: OsPermissionStatus;
}

export interface LocalRuntimeState {
  settings: LocalSettings;
  identity: LocalIdentityContext | null;
  linkedDeployment: LinkedDeploymentIntent | null;
  permissionDefaults: AgentPermissionPolicy;
  osPermissions: OsPermissionSnapshot;
  installedAgents: InstalledAgent[];
  installedSkills: InstalledSkill[];
}

export interface SkillSource {
  id: "clawhub" | "awesome-curated" | "built-in";
  name: string;
  description: string;
  catalogUrl: string;
}

export interface SkillRequirements {
  bins: string[];
  env: string[];
  os: string[];
  missing: string[];
  eligible: boolean;
}

export interface Skill {
  id: string;
  name: string;
  fullName: string;
  description: string;
  htmlUrl: string;
  source: SkillSource;
  stargazersCount: number;
  topics: string[];
  skillMdUrl: string;
  installRef: string;
  installSha?: string;
  requirements: SkillRequirements;
}

export interface InstalledSkill {
  id: string;
  name: string;
  fullName: string;
  description: string;
  htmlUrl: string;
  source: SkillSource;
  installedAt: number;
  enabled: boolean;
  localPath: string;
  relativePath: string;
  installRef: string;
  installSha?: string;
  requirements: SkillRequirements;
}

export interface AgentSkillState {
  skillId: string;
  enabled: boolean;
  eligible: boolean;
  source: "agent" | "shared" | "bundled" | "generated";
  revision: string;
  updatedAt: number;
}

export interface SkillsDiscoveryResult {
  skills: Skill[];
  total: number;
  page: number;
  limit: number;
}

export interface LocalPaths {
  base_dir: string;
  state_file: string;
  agents_dir: string;
  skills_dir: string;
}

export interface RedeemedLocalContext {
  agentWallet: string;
  userAddress: string;
  chainId: number;
  composeKey: {
    keyId: string;
    token: string;
    expiresAt: number;
  };
  session: {
    sessionId: string;
    budget: string;
    duration: number;
    expiresAt: number;
  };
  market: {
    entry: string;
    agentWallet: string;
    agentCardCid?: string | null;
  };
  deviceId: string;
  hasSession: boolean;
}

export interface LinkedDeploymentIntent {
  agentWallet: string;
  agentCardCid: string | null;
  chainId: number;
  source: "local-link" | "signed-install";
  receivedAt: number;
}

export interface CreateLinkTokenRequest {
  agentWallet?: string;
  agentCardCid?: string;
  userAddress: string;
  composeKeyId?: string;
  sessionId?: string;
  budget?: string | number;
  duration?: number;
  chainId?: number;
  deviceId?: string;
}

export interface BackpackConnectionInfo {
  slug: string;
  name: string;
  connected: boolean;
  accountId?: string;
  status?: string;
}
