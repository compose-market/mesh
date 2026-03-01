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

export async function fetchSessionInfo(params: {
  lambdaUrl: string;
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
    return await requestJson(`${normalizeBase(params.lambdaUrl)}/api/session`, {
      method: "GET",
      headers: {
        "x-session-user-address": params.userAddress,
        "x-chain-id": String(params.chainId),
      },
    });
  } catch {
    return null;
  }
}

export async function redeemDesktopLinkToken(params: {
  lambdaUrl: string;
  token: string;
  deviceId: string;
}): Promise<RedeemedDesktopContext> {
  const response = await requestJson<{ success: boolean; context: RedeemedDesktopContext }>(
    `${normalizeBase(params.lambdaUrl)}/api/desktop/link-token/redeem`,
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
  lambdaUrl: string;
  identity: DesktopIdentityContext;
  agentWallet: string;
  agentCardCid: string;
  desktopVersion: string;
  deployedAt: number;
}): Promise<void> {
  await requestJson(
    `${normalizeBase(params.lambdaUrl)}/api/desktop/deployments/register`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${params.identity.composeKeyToken}`,
        "x-session-user-address": params.identity.userAddress,
        "x-chain-id": String(params.identity.chainId),
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
  manowarUrl: string;
  agentWallet: string;
}): Promise<AgentMetadata> {
  return requestJson<AgentMetadata>(
    `${normalizeBase(params.manowarUrl)}/agent/${walletPath(params.agentWallet)}`,
    { method: "GET" },
  );
}

export async function callAgent(params: {
  manowarUrl: string;
  identity: DesktopIdentityContext;
  agentWallet: string;
  message: string;
  threadId?: string;
}): Promise<{ output?: string; success?: boolean; error?: string }> {
  return requestJson(
    `${normalizeBase(params.manowarUrl)}/agent/${walletPath(params.agentWallet)}/chat`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${params.identity.composeKeyToken}`,
        "x-session-user-address": params.identity.userAddress,
        "x-chain-id": String(params.identity.chainId),
        "x-session-active": "true",
        "x-session-budget-remaining": params.identity.budget,
      },
      body: JSON.stringify({
        message: params.message,
        threadId: params.threadId,
      }),
    },
  );
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
  lambdaUrl: string;
  composeKeyToken: string;
  userAddress: string;
  chainId: number;
  payload: CreateLinkTokenRequest;
}): Promise<{ deepLinkUrl: string; token: string; expiresAt: number }> {
  return requestJson(`${normalizeBase(params.lambdaUrl)}/api/desktop/link-token`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${params.composeKeyToken}`,
      "x-session-user-address": params.userAddress,
      "x-chain-id": String(params.chainId),
    },
    body: JSON.stringify(params.payload),
  });
}

export interface CreateSessionRequest {
  budgetLimit: number;
  expiresAt: number;
  name?: string;
  chainId?: number;
}

export interface CreateSessionResponse {
  keyId: string;
  token: string;
  budgetLimit: number;
  budgetUsed: number;
  budgetRemaining: number;
  expiresAt: number;
  createdAt: number;
  name?: string;
}

export async function createSession(params: {
  lambdaUrl: string;
  userAddress: string;
  payload: CreateSessionRequest;
}): Promise<CreateSessionResponse> {
  const response = await requestJson<{ keyId: string; token: string; budgetLimit: number; budgetUsed: number; expiresAt: number; createdAt: number; name?: string }>(
    `${normalizeBase(params.lambdaUrl)}/api/keys`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-session-user-address": params.userAddress,
        "x-session-active": "true",
      },
      body: JSON.stringify(params.payload),
    },
  );

  return {
    ...response,
    budgetRemaining: response.budgetLimit - (response.budgetUsed || 0),
  };
}

export async function getSessionStatus(params: {
  lambdaUrl: string;
  userAddress: string;
  chainId: number;
}): Promise<{
  hasSession: boolean;
  keyId?: string;
  token?: string;
  budgetLimit?: number;
  budgetUsed?: number;
  budgetRemaining?: number;
  expiresAt?: number;
  chainId?: number;
} | null> {
  try {
    const response = await requestJson<{
      hasSession: boolean;
      keyId?: string;
      token?: string;
      budgetLimit?: string;
      budgetUsed?: string;
      budgetRemaining?: string;
      expiresAt?: number;
      chainId?: number;
    }>(
      `${normalizeBase(params.lambdaUrl)}/api/session`,
      {
        method: "GET",
        headers: {
          "x-session-user-address": params.userAddress,
          "x-chain-id": String(params.chainId),
        },
      },
    );

    if (!response.hasSession) {
      return { hasSession: false };
    }

    return {
      hasSession: true,
      keyId: response.keyId,
      token: response.token,
      budgetLimit: response.budgetLimit ? parseInt(response.budgetLimit, 10) : undefined,
      budgetUsed: response.budgetUsed ? parseInt(response.budgetUsed, 10) : undefined,
      budgetRemaining: response.budgetRemaining ? parseInt(response.budgetRemaining, 10) : undefined,
      expiresAt: response.expiresAt,
      chainId: response.chainId,
    };
  } catch {
    return null;
  }
}
