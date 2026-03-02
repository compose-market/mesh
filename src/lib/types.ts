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

export interface AgentHeartbeatState {
  enabled: boolean;
  intervalMs: number;
  lastRunAt: number | null;
  lastResult: "ok" | "alert" | "error" | null;
}

export interface InstalledAgent {
  agentWallet: string;
  metadata: AgentMetadata;
  lock: ImmutableAgentLock;
  addedAt: number;
  running: boolean;
  runtimeId: string;
  heartbeat: AgentHeartbeatState;
  permissions: AgentPermissionPolicy;
  network: AgentNetworkState;
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
  lambdaUrl: string;
  manowarUrl: string;
}

export interface AgentPermissionPolicy {
  shell: boolean;
  filesystemRead: boolean;
  filesystemWrite: boolean;
  filesystemEdit: boolean;
  filesystemDelete: boolean;
  camera: boolean;
  microphone: boolean;
}

export type AgentNetworkStatus = "dormant" | "connecting" | "online" | "error";

export interface AgentNetworkState {
  enabled: boolean;
  status: AgentNetworkStatus;
  peerId: string | null;
  listenMultiaddrs: string[];
  peersDiscovered: number;
  lastHeartbeatAt: number | null;
  lastError: string | null;
  updatedAt: number;
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
