export interface SessionState {
  active: boolean;
  expiresAt: number | null;
  budgetLimit: string | null;
  budgetUsed: string | null;
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
  peerId?: string;
}

export interface InstalledAgent {
  agentWallet: string;
  metadata: AgentMetadata;
  lock: AgentDnaLock;
  addedAt: number;
  running: boolean;
  runtimeId: string;
  heartbeat: AgentHeartbeatState;
  permissions: AgentPermissionPolicy;
  network: AgentNetworkState;
  workerState?: AgentWorkerState;
  skillStates: Record<string, AgentSkillState>;
  reports: AgentTaskReport[];
}

export interface DesktopIdentityContext {
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

export interface DesktopSettings {
  apiUrl: string;
  runtimeUrl: string;
}

export type PermissionDecision = "allow" | "ask" | "deny";

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

export interface MeshPeerSignal {
  peerId: string;
  agentWallet: string | null;
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
  peerId: string | null;
  listenMultiaddrs: string[];
  peersDiscovered: number;
  lastHeartbeatAt: number | null;
  lastError: string | null;
  updatedAt: number;
  publicCard: MeshAgentCard | null;
  recentPings: MeshPeerSignal[];
  interactions: AgentMeshInteraction[];
}

export interface AgentWorkerState {
  running: boolean;
  desiredRunning: boolean;
  status: "stopped" | "starting" | "running" | "stopping" | "error";
  runtimeId: string | null;
  lastHeartbeatAt: number | null;
  lastError: string | null;
  updatedAt: number;
}

export interface PermissionDecisionTicket {
  id: string;
  agentWallet: string;
  action: string;
  decision: "allow" | "deny";
  issuedAt: number;
  expiresAt: number;
  nonce: string;
}

export type OsPermissionStatus = "unknown" | "granted" | "denied" | "unsupported";

export interface OsPermissionSnapshot {
  camera: OsPermissionStatus;
  microphone: OsPermissionStatus;
}

export interface DesktopRuntimeState {
  settings: DesktopSettings;
  identity: DesktopIdentityContext | null;
  permissionDefaults: AgentPermissionPolicy;
  osPermissions: OsPermissionSnapshot;
  installedAgents: InstalledAgent[];
  installedSkills: InstalledSkill[];
}

export interface SkillSource {
  id: "clawhub" | "awesome-curated";
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

export interface DesktopPaths {
  base_dir: string;
  state_file: string;
  agents_dir: string;
  skills_dir: string;
}

export interface RedeemedDesktopContext {
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
  };
  deviceId: string;
  hasSession: boolean;
}

export interface SignedDesktopInstallPayload {
  agentWallet: string;
  agentCardCid: string;
  chainId: number;
  issuedAt: number;
  expiresAt: number;
  nonce: string;
  composeKey?: string;
}

export interface SignedDesktopInstallEnvelope {
  payload: SignedDesktopInstallPayload;
  signature: `0x${string}`;
  signer: `0x${string}`;
}

export interface CreateLinkTokenRequest {
  agentWallet?: string;
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
