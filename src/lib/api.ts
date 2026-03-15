import { invoke } from "@tauri-apps/api/core";
import { extractSkillRequirements, extractSkillSummary } from "./skills";
import {
  ensureManagedDir,
  getDesktopPaths,
  getGlobalSkillsRelativePath,
  removeManagedPath,
  writeManagedFile,
} from "./storage";
import type {
  AgentMetadata,
  BackpackConnectionInfo,
  CreateLinkTokenRequest,
  DesktopIdentityContext,
  InstalledSkill,
  RedeemedDesktopContext,
  Skill,
  SkillRequirements,
  SkillsDiscoveryResult,
} from "./types";

const DEFAULT_TIMEOUT_MS = 20_000;
const CATALOG_CACHE_TTL_MS = 5 * 60 * 1000;
const GITHUB_HEADERS = {
  Accept: "application/vnd.github+json",
  "User-Agent": "compose-desktop/1.0",
};
const PINATA_GATEWAY_URL = "https://compose.mypinata.cloud";

export const SESSION_HEADERS = {
  userAddress: "x-session-user-address",
  chainId: "x-chain-id",
  active: "x-session-active",
  budgetRemaining: "x-session-budget-remaining",
} as const;

const EMPTY_REQUIREMENTS: SkillRequirements = {
  bins: [],
  env: [],
  os: [],
  missing: [],
  eligible: true,
};

const catalogCache = new Map<"clawhub" | "awesome-curated", { fetchedAt: number; skills: Skill[] }>();
const skillDetailCache = new Map<string, { name: string | null; description: string | null; requirements: SkillRequirements }>();

function normalizeBase(url: string): string {
  return url.replace(/\/+$/, "");
}

function withSessionHeaders(input: {
  userAddress: string;
  chainId?: number;
  active?: boolean;
  budgetRemaining?: string;
}): Record<string, string> {
  const headers: Record<string, string> = {
    [SESSION_HEADERS.userAddress]: input.userAddress,
  };
  if (typeof input.chainId === "number") {
    headers[SESSION_HEADERS.chainId] = String(input.chainId);
  }
  if (input.active) {
    headers[SESSION_HEADERS.active] = "true";
  }
  if (input.budgetRemaining) {
    headers[SESSION_HEADERS.budgetRemaining] = input.budgetRemaining;
  }
  return headers;
}

function cloneRequirements(requirements: SkillRequirements): SkillRequirements {
  return {
    bins: [...requirements.bins],
    env: [...requirements.env],
    os: [...requirements.os],
    missing: [...requirements.missing],
    eligible: requirements.eligible,
  };
}

function withDefaultRequirements(skill: Omit<Skill, "requirements">): Skill {
  return {
    ...skill,
    requirements: cloneRequirements(EMPTY_REQUIREMENTS),
  };
}

async function requestJson<T>(url: string, init: RequestInit, timeoutMs = DEFAULT_TIMEOUT_MS): Promise<T> {
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(url, {
      ...init,
      signal: controller.signal,
      headers: {
        "Content-Type": "application/json",
        ...(init.headers || {}),
      },
    });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`HTTP ${response.status}: ${text || response.statusText}`);
    }
    return (await response.json()) as T;
  } finally {
    window.clearTimeout(timer);
  }
}

async function mapWithConcurrency<T, R>(
  items: T[],
  limit: number,
  mapper: (item: T, index: number) => Promise<R>,
): Promise<R[]> {
  const output: R[] = new Array(items.length);
  let cursor = 0;

  async function worker(): Promise<void> {
    while (cursor < items.length) {
      const current = cursor;
      cursor += 1;
      output[current] = await mapper(items[current], current);
    }
  }

  const workers = Array.from({ length: Math.max(1, Math.min(limit, items.length)) }, () => worker());
  await Promise.all(workers);
  return output;
}

function walletPath(wallet: string): string {
  return wallet.toLowerCase();
}

function normalizeWalletAddress(value: string): string {
  const normalized = value.trim().toLowerCase();
  if (!/^0x[a-f0-9]{40}$/.test(normalized)) {
    throw new Error("Agent card walletAddress is invalid");
  }
  return normalized;
}

async function fetchAgentMetadataFromIpfs(agentCardCid: string, expectedWallet: string): Promise<AgentMetadata> {
  let lastError: Error | null = null;

  for (const pinataGateway of PINATA_GATEWAY_URL) {
    try {
      const response = await fetch(`${pinataGateway}/${agentCardCid}`, {
        headers: {
          Accept: "application/json",
        },
      });
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }

      const data = await response.json() as Partial<AgentMetadata> & {
        endpoint?: string;
      };
      const walletAddress = normalizeWalletAddress(data.walletAddress || "");
      const normalizedExpectedWallet = normalizeWalletAddress(expectedWallet);
      if (walletAddress !== normalizedExpectedWallet) {
        throw new Error("Linked agent card does not match the requested agent wallet");
      }

      return {
        name: data.name || "Unnamed Agent",
        description: data.description || "",
        agentCardUri: `ipfs://${agentCardCid}`,
        creator: data.creator || "",
        walletAddress,
        dnaHash: data.dnaHash || "",
        model: data.model || "",
        framework: data.framework || "openclaw",
        plugins: Array.isArray(data.plugins) ? data.plugins : [],
        createdAt: data.createdAt || new Date().toISOString(),
        endpoints: data.endpoints || (data.endpoint
          ? {
            chat: data.endpoint,
          }
          : undefined),
      };
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
    }
  }

  throw new Error(lastError?.message || "Failed to fetch linked agent card");
}

export async function fetchSessionInfo(params: {
  apiUrl: string;
  userAddress: string;
  chainId: number;
}): Promise<{
  hasSession: boolean;
  keyId: string;
  token: string;
  budgetRemaining: string;
  expiresAt: number;
  chainId: number;
} | null> {
  try {
    return await requestJson(`${normalizeBase(params.apiUrl)}/api/session`, {
      method: "GET",
      headers: withSessionHeaders({
        userAddress: params.userAddress,
        chainId: params.chainId,
      }),
    });
  } catch {
    return null;
  }
}

export async function redeemDesktopLinkToken(params: {
  apiUrl: string;
  token: string;
  deviceId: string;
}): Promise<RedeemedDesktopContext> {
  const response = await requestJson<{ success: boolean; context: RedeemedDesktopContext }>(
    `${normalizeBase(params.apiUrl)}/api/desktop/link-token/redeem`,
    {
      method: "POST",
      body: JSON.stringify({
        token: params.token,
        deviceId: params.deviceId,
      }),
    },
  );

  if (!response.success) {
    throw new Error("Desktop deep-link redemption failed");
  }

  return response.context;
}

export async function registerDesktopDeployment(params: {
  apiUrl: string;
  identity: DesktopIdentityContext;
  agentWallet: string;
  agentCardCid: string;
  desktopVersion: string;
  deployedAt: number;
}): Promise<void> {
  await requestJson(
    `${normalizeBase(params.apiUrl)}/api/desktop/deployments/register`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${params.identity.composeKeyToken}`,
        ...withSessionHeaders({
          userAddress: params.identity.userAddress,
          chainId: params.identity.chainId,
        }),
      },
      body: JSON.stringify({
        agentWallet: params.agentWallet,
        userAddress: params.identity.userAddress,
        composeKeyId: params.identity.composeKeyId,
        agentCardCid: params.agentCardCid,
        desktopVersion: params.desktopVersion,
        deployedAt: params.deployedAt,
        chainId: params.identity.chainId,
      }),
    },
  );
}

export async function fetchAgentMetadata(params: {
  runtimeUrl: string;
  agentWallet: string;
  agentCardCid?: string;
}): Promise<AgentMetadata> {
  try {
    return await requestJson<AgentMetadata>(
      `${normalizeBase(params.runtimeUrl)}/agent/${walletPath(params.agentWallet)}`,
      { method: "GET" },
    );
  } catch (error) {
    if (!params.agentCardCid) {
      throw error;
    }
    return fetchAgentMetadataFromIpfs(params.agentCardCid, params.agentWallet);
  }
}

export async function callAgent(params: {
  runtimeUrl: string;
  identity: DesktopIdentityContext;
  agentWallet: string;
  message: string;
  threadId?: string;
  userId?: string;
  grantedPermissions?: string[];
  permissionPolicy?: Record<string, "allow" | "ask" | "deny">;
  backpackAccounts?: BackpackConnectionInfo[];
}): Promise<{ output?: string; success?: boolean; error?: string }> {
  return requestJson(
    `${normalizeBase(params.runtimeUrl)}/agent/${walletPath(params.agentWallet)}/chat`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${params.identity.composeKeyToken}`,
        ...withSessionHeaders({
          userAddress: params.identity.userAddress,
          chainId: params.identity.chainId,
          active: true,
          budgetRemaining: params.identity.budget,
        }),
      },
      body: JSON.stringify({
        message: params.message,
        threadId: params.threadId,
        userId: params.userId,
        grantedPermissions: params.grantedPermissions,
        permissionPolicy: params.permissionPolicy,
        backpackAccounts: params.backpackAccounts,
      }),
    },
  );
}

export async function fetchBackpackConnections(params: {
  apiUrl: string;
  userId: string;
}): Promise<BackpackConnectionInfo[]> {
  const response = await requestJson<{ connections?: BackpackConnectionInfo[] }>(
    `${normalizeBase(params.apiUrl)}/api/backpack/connections?userId=${encodeURIComponent(params.userId)}`,
    { method: "GET" },
  );
  return Array.isArray(response.connections) ? response.connections : [];
}

function normalizeSkillId(prefix: string, value: string): string {
  return `${prefix}:${value}`.replace(/[^a-zA-Z0-9:_-]/g, "-").toLowerCase();
}

function shortDescription(value: string | null | undefined, fallback: string): string {
  if (!value || value.trim().length === 0) return fallback;
  return value.trim();
}

async function fetchSkillMd(url: string): Promise<string | null> {
  try {
    const response = await fetch(url, { headers: GITHUB_HEADERS });
    if (!response.ok) return null;
    return await response.text();
  } catch {
    return null;
  }
}

async function fetchSkillDetails(skill: Skill): Promise<Skill> {
  const cached = skillDetailCache.get(skill.skillMdUrl);
  if (cached) {
    return {
      ...skill,
      name: cached.name || skill.name,
      description: cached.description || skill.description,
      requirements: cloneRequirements(cached.requirements),
    };
  }

  const skillMd = await fetchSkillMd(skill.skillMdUrl);
  if (!skillMd) {
    return skill;
  }

  const summary = extractSkillSummary(skillMd);
  const requirements = await extractSkillRequirements(skillMd);
  skillDetailCache.set(skill.skillMdUrl, {
    name: summary.name,
    description: summary.description,
    requirements: cloneRequirements(requirements),
  });

  return {
    ...skill,
    name: summary.name || skill.name,
    description: summary.description || skill.description,
    requirements,
  };
}

function extractGithubRepos(markdown: string): string[] {
  const regex = /https:\/\/github\.com\/([A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+)/g;
  const repos = new Set<string>();
  let match: RegExpExecArray | null = regex.exec(markdown);
  while (match) {
    repos.add(match[1]);
    match = regex.exec(markdown);
  }
  return Array.from(repos);
}

async function resolveRepoSkillMd(repo: string): Promise<{ skillMdUrl: string; installRef: string } | null> {
  const attempts = [
    { branch: "main", path: "SKILL.md" },
    { branch: "master", path: "SKILL.md" },
    { branch: "main", path: "skills/SKILL.md" },
    { branch: "master", path: "skills/SKILL.md" },
  ];

  for (const attempt of attempts) {
    const candidate = `https://raw.githubusercontent.com/${repo}/${attempt.branch}/${attempt.path}`;
    try {
      const response = await fetch(candidate, { method: "HEAD", headers: GITHUB_HEADERS });
      if (response.ok) {
        return { skillMdUrl: candidate, installRef: attempt.branch };
      }
    } catch {
      continue;
    }
  }

  return null;
}

async function discoverClawHubSkillsUncached(): Promise<Skill[]> {
  type GitTreeResponse = {
    tree: Array<{
      path: string;
      type: "blob" | "tree";
      sha: string;
    }>;
  };

  const tree = await requestJson<GitTreeResponse>(
    "https://api.github.com/repos/openclaw/skills/git/trees/main?recursive=1",
    { method: "GET", headers: GITHUB_HEADERS },
  );

  const bestPathPerSkill = new Map<string, string>();
  for (const item of tree.tree) {
    if (item.type !== "blob") continue;
    if (!item.path.startsWith("skills/")) continue;
    if (!item.path.endsWith("/SKILL.md")) continue;

    const withoutFile = item.path.slice(0, -"/SKILL.md".length);
    const parts = withoutFile.split("/");
    if (parts.length < 3) continue;
    const author = parts[1];
    const slug = parts[2];
    if (!author || !slug) continue;

    const key = `${author}/${slug}`;
    const existing = bestPathPerSkill.get(key);
    if (!existing) {
      bestPathPerSkill.set(key, withoutFile);
      continue;
    }

    const existingDepth = existing.split("/").length;
    const candidateDepth = withoutFile.split("/").length;
    if (candidateDepth < existingDepth || (candidateDepth === existingDepth && withoutFile < existing)) {
      bestPathPerSkill.set(key, withoutFile);
    }
  }

  const skills = Array.from(bestPathPerSkill.entries()).map(([key, path]) => {
    const [author, slug] = key.split("/");
    const skillMdPath = `${path}/SKILL.md`;
    return withDefaultRequirements({
      id: normalizeSkillId("clawhub", `${author}/${slug}`),
      name: slug.replace(/[-_]/g, " "),
      fullName: `openclaw/skills/${author}/${slug}`,
      description: `OpenClaw skill from ${author}`,
      htmlUrl: `https://github.com/openclaw/skills/tree/main/${path}`,
      source: {
        id: "clawhub",
        name: "ClawHub",
        description: "OpenClaw skills registry",
        catalogUrl: "https://github.com/openclaw/skills/tree/main/skills",
      },
      stargazersCount: 0,
      topics: [],
      skillMdUrl: `https://raw.githubusercontent.com/openclaw/skills/main/${skillMdPath}`,
      installRef: "main",
    });
  });

  skills.sort((a, b) => a.name.localeCompare(b.name));
  return skills;
}

async function discoverAwesomeCuratedSkillsUncached(): Promise<Skill[]> {
  type GithubFile = {
    name: string;
    type: string;
    download_url: string | null;
  };

  const files = await requestJson<GithubFile[]>(
    "https://api.github.com/repos/hesamsheikh/awesome-openclaw-usecases/contents/usecases",
    { method: "GET", headers: GITHUB_HEADERS },
  );

  const markdownUrls = files
    .filter((entry) => entry.type === "file" && entry.name.endsWith(".md") && entry.download_url)
    .map((entry) => entry.download_url!)
    .slice(0, 30);

  const markdownBodies = await mapWithConcurrency(markdownUrls, 5, async (url) => {
    try {
      const response = await fetch(url, { headers: GITHUB_HEADERS });
      if (!response.ok) return "";
      return await response.text();
    } catch {
      return "";
    }
  });

  const repos = new Set<string>();
  for (const body of markdownBodies) {
    for (const repo of extractGithubRepos(body)) {
      const normalized = repo.toLowerCase();
      if (normalized === "openclaw/skills" || normalized === "openclaw/clawhub") {
        continue;
      }
      repos.add(repo);
    }
  }

  const repoCandidates = Array.from(repos).sort().slice(0, 80);
  const resolvedEntries = await mapWithConcurrency(repoCandidates, 8, async (repo) => {
    const resolved = await resolveRepoSkillMd(repo);
    if (!resolved) {
      return null;
    }

    const name = repo.split("/")[1] || repo;
    return withDefaultRequirements({
      id: normalizeSkillId("awesome", repo),
      name: name.replace(/[-_]/g, " "),
      fullName: repo,
      description: "Community curated OpenClaw-compatible skill",
      htmlUrl: `https://github.com/${repo}`,
      source: {
        id: "awesome-curated",
        name: "Awesome Curated",
        description: "Community repositories discovered from awesome-openclaw-usecases",
        catalogUrl: "https://github.com/hesamsheikh/awesome-openclaw-usecases",
      },
      stargazersCount: 0,
      topics: [],
      skillMdUrl: resolved.skillMdUrl,
      installRef: resolved.installRef,
    });
  });

  const skills = resolvedEntries.filter((entry): entry is Skill => entry !== null);
  skills.sort((a, b) => a.name.localeCompare(b.name));
  return skills;
}

async function discoverFromCacheOrSource(source: "clawhub" | "awesome-curated"): Promise<Skill[]> {
  const cached = catalogCache.get(source);
  if (cached && Date.now() - cached.fetchedAt <= CATALOG_CACHE_TTL_MS) {
    return cached.skills.map((skill) => ({ ...skill, requirements: cloneRequirements(skill.requirements) }));
  }

  const skills = source === "clawhub"
    ? await discoverClawHubSkillsUncached()
    : await discoverAwesomeCuratedSkillsUncached();

  catalogCache.set(source, { fetchedAt: Date.now(), skills });
  return skills.map((skill) => ({ ...skill, requirements: cloneRequirements(skill.requirements) }));
}

async function safeDiscover(source: "clawhub" | "awesome-curated"): Promise<Skill[]> {
  try {
    return await discoverFromCacheOrSource(source);
  } catch (error) {
    console.error(`[skills] discovery failed for ${source}`, error);
    return [];
  }
}

function paginateSkills(skills: Skill[], page: number, limit: number): SkillsDiscoveryResult {
  const offset = (page - 1) * limit;
  return {
    skills: skills.slice(offset, offset + limit),
    total: skills.length,
    page,
    limit,
  };
}

export async function discoverSkills(params?: {
  search?: string;
  source?: "clawhub" | "awesome-curated";
  page?: number;
  limit?: number;
}): Promise<SkillsDiscoveryResult> {
  const source = params?.source;
  const page = params?.page && params.page > 0 ? params.page : 1;
  const limit = params?.limit && params.limit > 0 ? params.limit : 12;
  const search = params?.search?.trim().toLowerCase() || "";

  const batches = await Promise.all([
    !source || source === "clawhub" ? safeDiscover("clawhub") : Promise.resolve([]),
    !source || source === "awesome-curated" ? safeDiscover("awesome-curated") : Promise.resolve([]),
  ]);

  let merged = batches.flat();
  if (search) {
    merged = merged.filter((skill) =>
      skill.name.toLowerCase().includes(search) ||
      skill.description.toLowerCase().includes(search) ||
      skill.fullName.toLowerCase().includes(search),
    );
  }

  merged.sort((a, b) => {
    if (b.stargazersCount !== a.stargazersCount) {
      return b.stargazersCount - a.stargazersCount;
    }
    return a.name.localeCompare(b.name);
  });

  const paged = paginateSkills(merged, page, limit);
  const hydrated = await mapWithConcurrency(paged.skills, 4, async (skill) => fetchSkillDetails(skill));

  return {
    ...paged,
    skills: hydrated,
  };
}

function normalizeSkillDir(skillId: string): string {
  return skillId.replace(/[^a-zA-Z0-9._-]/g, "_");
}

export async function installSkill(
  skill: Skill,
): Promise<{ success: boolean; installed?: InstalledSkill; error?: string; warning?: string }> {
  const skillMd = await fetchSkillMd(skill.skillMdUrl);
  if (!skillMd) {
    return { success: false, error: "Unable to fetch SKILL.md from source" };
  }

  const requirements = await extractSkillRequirements(skillMd);
  const summary = extractSkillSummary(skillMd);

  const safeDir = normalizeSkillDir(skill.id);
  const relativeDir = `${getGlobalSkillsRelativePath()}/${safeDir}`;
  const created = await ensureManagedDir(relativeDir);
  if (!created) {
    return { success: false, error: "Desktop managed storage is unavailable" };
  }

  const skillMdPath = await writeManagedFile(`${relativeDir}/SKILL.md`, skillMd);
  if (!skillMdPath) {
    return { success: false, error: "Failed to write SKILL.md locally" };
  }

  const metadataPath = await writeManagedFile(
    `${relativeDir}/source.json`,
    JSON.stringify(
      {
        source: skill.source,
        installRef: skill.installRef,
        installSha: skill.installSha || null,
        fullName: skill.fullName,
        htmlUrl: skill.htmlUrl,
      },
      null,
      2,
    ),
  );
  if (!metadataPath) {
    return { success: false, error: "Failed to write skill metadata locally" };
  }

  const paths = await getDesktopPaths();
  const localPath = paths ? `${paths.skills_dir}/${safeDir}` : relativeDir;
  const enabled = requirements.eligible;

  return {
    success: true,
    warning: enabled ? undefined : `Installed with missing requirements: ${requirements.missing.join(", ")}`,
    installed: {
      id: skill.id,
      name: summary.name || skill.name,
      fullName: skill.fullName,
      description: shortDescription(summary.description, skill.description),
      htmlUrl: skill.htmlUrl,
      source: skill.source,
      installedAt: Date.now(),
      enabled,
      localPath,
      installRef: skill.installRef,
      installSha: skill.installSha,
      requirements,
    },
  };
}

export async function uninstallSkill(skill: InstalledSkill): Promise<void> {
  const safeDir = normalizeSkillDir(skill.id);
  const removed = await removeManagedPath(`${getGlobalSkillsRelativePath()}/${safeDir}`);
  if (!removed) {
    throw new Error("Skill was not removed from local managed storage");
  }
}

export async function getMachineMissingBinaries(binaries: string[]): Promise<string[]> {
  try {
    return await invoke<string[]>("check_missing_binaries", { binaries });
  } catch {
    return [...binaries];
  }
}

export async function createDesktopLinkToken(params: {
  apiUrl: string;
  composeKeyToken: string;
  userAddress: string;
  chainId: number;
  payload: CreateLinkTokenRequest;
}): Promise<{ deepLinkUrl: string; token: string; expiresAt: number }> {
  return requestJson(`${normalizeBase(params.apiUrl)}/api/desktop/link-token`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${params.composeKeyToken}`,
      ...withSessionHeaders({
        userAddress: params.userAddress,
        chainId: params.chainId,
      }),
    },
    body: JSON.stringify(params.payload),
  });
}

export interface CreateSessionRequest {
  budgetLimit: number | string;
  expiresAt: number;
  purpose: "session" | "api";
  name?: string;
  chainId?: number;
}

export interface CreateSessionResponse {
  keyId: string;
  token: string;
  purpose?: "session" | "api";
  budgetLimit: string;
  budgetUsed: string;
  budgetRemaining: string;
  expiresAt: number;
  createdAt: number;
  name?: string;
  chainId?: number;
}

export interface ComposeKeyRecord {
  keyId: string;
  purpose?: "session" | "api";
  budgetLimit: string;
  budgetUsed: string;
  budgetRemaining: string;
  createdAt: number;
  expiresAt: number;
  revokedAt?: number;
  name?: string;
  lastUsedAt?: number;
  chainId?: number;
}

export interface ActiveSessionStatusResponse {
  hasSession: boolean;
  keyId?: string;
  token?: string;
  budgetLimit?: string;
  budgetUsed?: string;
  budgetLocked?: string;
  budgetRemaining?: string;
  expiresAt?: number;
  chainId?: number;
  name?: string;
  status?: {
    isActive: boolean;
    isExpired: boolean;
    expiresInSeconds: number;
    budgetPercentRemaining: number;
    warnings: {
      budgetDepleted: boolean;
      budgetLow: boolean;
      expiringSoon: boolean;
      expired: boolean;
    };
  };
}

function normalizeBigintString(value: unknown, fallback = "0"): string {
  if (typeof value === "string") {
    return value;
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return String(Math.trunc(value));
  }
  return fallback;
}

export async function createSession(params: {
  apiUrl: string;
  userAddress: string;
  payload: CreateSessionRequest;
}): Promise<CreateSessionResponse> {
  const response = await requestJson<{
    keyId: string;
    token: string;
    purpose?: "session" | "api";
    budgetLimit: number | string;
    budgetUsed?: number | string;
    budgetRemaining?: number | string;
    expiresAt: number;
    createdAt?: number;
    name?: string;
    chainId?: number;
  }>(
    `${normalizeBase(params.apiUrl)}/api/keys`,
    {
      method: "POST",
      headers: {
        ...withSessionHeaders({
          userAddress: params.userAddress,
          active: true,
        }),
      },
      body: JSON.stringify(params.payload),
    },
  );

  const budgetLimit = normalizeBigintString(response.budgetLimit);
  const budgetUsed = normalizeBigintString(response.budgetUsed, "0");
  const budgetRemaining = response.budgetRemaining !== undefined
    ? normalizeBigintString(response.budgetRemaining)
    : (BigInt(budgetLimit) - BigInt(budgetUsed)).toString();

  return {
    keyId: response.keyId,
    token: response.token,
    purpose: response.purpose,
    budgetLimit,
    budgetUsed,
    budgetRemaining,
    expiresAt: response.expiresAt,
    createdAt: response.createdAt ?? Date.now(),
    name: response.name,
    chainId: response.chainId,
  };
}

export async function listComposeKeys(params: {
  apiUrl: string;
  userAddress: string;
}): Promise<ComposeKeyRecord[]> {
  const response = await requestJson<{
    keys?: Array<{
      keyId: string;
      purpose?: "session" | "api";
      budgetLimit: number | string;
      budgetUsed: number | string;
      budgetRemaining?: number | string;
      createdAt: number;
      expiresAt: number;
      revokedAt?: number;
      name?: string;
      lastUsedAt?: number;
      chainId?: number;
    }>;
  }>(
    `${normalizeBase(params.apiUrl)}/api/keys`,
    {
      method: "GET",
      headers: withSessionHeaders({
        userAddress: params.userAddress,
      }),
    },
  );

  return (response.keys || []).map((item) => {
    const budgetLimit = normalizeBigintString(item.budgetLimit);
    const budgetUsed = normalizeBigintString(item.budgetUsed);
    const budgetRemaining = item.budgetRemaining !== undefined
      ? normalizeBigintString(item.budgetRemaining)
      : (BigInt(budgetLimit) - BigInt(budgetUsed)).toString();
    return {
      keyId: item.keyId,
      purpose: item.purpose,
      budgetLimit,
      budgetUsed,
      budgetRemaining,
      createdAt: item.createdAt,
      expiresAt: item.expiresAt,
      revokedAt: item.revokedAt,
      name: item.name,
      lastUsedAt: item.lastUsedAt,
      chainId: item.chainId,
    };
  });
}

export async function revokeComposeKey(params: {
  apiUrl: string;
  userAddress: string;
  keyId: string;
}): Promise<boolean> {
  try {
    await requestJson(
      `${normalizeBase(params.apiUrl)}/api/keys/${params.keyId}`,
      {
        method: "DELETE",
        headers: withSessionHeaders({
          userAddress: params.userAddress,
        }),
      },
    );
    return true;
  } catch {
    return false;
  }
}

export async function getActiveSessionStatus(params: {
  apiUrl: string;
  userAddress: string;
  chainId: number;
}): Promise<ActiveSessionStatusResponse | null> {
  try {
    const response = await requestJson<ActiveSessionStatusResponse>(
      `${normalizeBase(params.apiUrl)}/api/session`,
      {
        method: "GET",
        headers: withSessionHeaders({
          userAddress: params.userAddress,
          chainId: params.chainId,
        }),
      },
    );

    if (!response.hasSession) {
      return { hasSession: false };
    }

    return {
      hasSession: true,
      keyId: response.keyId,
      token: response.token,
      budgetLimit: response.budgetLimit,
      budgetUsed: response.budgetUsed,
      budgetLocked: response.budgetLocked,
      budgetRemaining: response.budgetRemaining,
      expiresAt: response.expiresAt,
      chainId: response.chainId,
      name: response.name,
      status: response.status,
    };
  } catch {
    return null;
  }
}

export const getSessionStatus = getActiveSessionStatus;
