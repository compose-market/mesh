import { invoke } from "@tauri-apps/api/core";
import { extractSkillRequirements, extractSkillSummary } from "./skills";
import {
  ensureManagedDir,
  getLocalPaths,
  getGlobalSkillsRelativePath,
  removeManagedPath,
  writeManagedFile,
} from "./storage";
import type {
  AgentMetadata,
  BackpackConnectionInfo,
  CreateLinkTokenRequest,
  LocalIdentityContext,
  InstalledSkill,
  RedeemedLocalContext,
  Skill,
  SkillRequirements,
  SkillsDiscoveryResult,
} from "./types";

declare const __APP_VERSION__: string;

const DEFAULT_TIMEOUT_MS = 20_000;
const CATALOG_CACHE_TTL_MS = 5 * 60 * 1000;
const GITHUB_HEADERS = {
  Accept: "application/vnd.github+json",
  "User-Agent": `compose-mesh/${__APP_VERSION__}`,
};
const PINATA_GATEWAY_URLS = ["https://compose.mypinata.cloud/ipfs"];

export const SESSION_HEADERS = {
  userAddress: "x-session-user-address",
  chainId: "x-chain-id",
  active: "x-session-active",
  budgetRemaining: "x-session-budget-remaining",
} as const;

export interface PaymentFetchParams {
  chainId: number;
  sessionToken: string;
  sessionUserAddress?: string;
  sessionBudgetRemaining?: string | number | null;
}

export interface ParsedSSEBlock {
  event: string;
  data: string;
}

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

export function createPaymentFetch(params: PaymentFetchParams): (input: RequestInfo | URL, init?: RequestInit) => Promise<Response> {
  if (!Number.isInteger(params.chainId) || params.chainId <= 0) {
    throw new Error("chainId is required");
  }

  const hasSessionContext = typeof params.sessionUserAddress === "string"
    && params.sessionUserAddress.length > 0
    && params.sessionBudgetRemaining !== null
    && params.sessionBudgetRemaining !== undefined
    && String(params.sessionBudgetRemaining).length > 0;

  return async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const headers = new Headers(init?.headers);
    headers.set("Authorization", `Bearer ${params.sessionToken}`);
    headers.set("X-Chain-ID", String(params.chainId));
    if (hasSessionContext) {
      headers.set("X-Session-Active", "true");
      headers.set("X-Session-User-Address", params.sessionUserAddress!);
      headers.set("X-Session-Budget-Remaining", String(params.sessionBudgetRemaining));
    }

    return fetch(input, {
      ...init,
      headers,
    });
  };
}

export async function* parseEventStream(
  reader: ReadableStreamDefaultReader<Uint8Array>
): AsyncGenerator<ParsedSSEBlock, void, unknown> {
  const decoder = new TextDecoder();
  let buffer = "";

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });

    while (true) {
      const separatorIndex = buffer.indexOf("\n\n");
      if (separatorIndex === -1) break;

      const rawBlock = buffer.slice(0, separatorIndex);
      buffer = buffer.slice(separatorIndex + 2);
      if (!rawBlock.trim()) continue;

      let event = "message";
      const dataLines: string[] = [];

      for (const line of rawBlock.split("\n")) {
        if (line.startsWith("event:")) {
          event = line.slice(6).trim() || "message";
          continue;
        }
        if (line.startsWith("data:")) {
          dataLines.push(line.slice(5).trimStart());
          continue;
        }
        if (line.trim() && !line.startsWith(":")) {
          dataLines.push(line);
        }
      }

      if (dataLines.length === 0) continue;

      yield {
        event,
        data: dataLines.join("\n"),
      };
    }
  }

  if (buffer.trim()) {
    yield {
      event: "message",
      data: buffer.trim(),
    };
  }
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
  const timer = globalThis.setTimeout(() => controller.abort(), timeoutMs);

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
    globalThis.clearTimeout(timer);
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

type AgentMetadataSource = Partial<AgentMetadata> & {
  cid?: string;
  endpoint?: string;
};

function asRecord(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("Agent metadata response is invalid");
  }
  return value as Record<string, unknown>;
}

function optionalString(value: unknown): string | null {
  if (typeof value !== "string") {
    return null;
  }
  const normalized = value.trim();
  return normalized.length > 0 ? normalized : null;
}

function requireAgentField(value: string | null, field: string): string {
  if (!value) {
    throw new Error(`Agent metadata is missing ${field}`);
  }
  return value;
}

function normalizeAgentCardUri(value: string | null, fallbackAgentCardCid?: string): string {
  if (value) {
    return value.startsWith("ipfs://") ? value : `ipfs://${value.replace(/^\/+/, "")}`;
  }
  const fallbackCid = optionalString(fallbackAgentCardCid);
  if (fallbackCid) {
    return `ipfs://${fallbackCid}`;
  }
  throw new Error("Agent metadata is missing agentCardUri");
}

function normalizeAgentEndpoints(source: AgentMetadataSource): AgentMetadata["endpoints"] {
  const chat = optionalString(source.endpoints?.chat);
  const stream = optionalString(source.endpoints?.stream);
  if (chat || stream) {
    return {
      ...(chat ? { chat } : {}),
      ...(stream ? { stream } : {}),
    };
  }

  const endpoint = optionalString(source.endpoint);
  if (!endpoint) {
    return undefined;
  }

  return {
    chat: endpoint,
  };
}

export function normalizeAgentMetadata(
  value: unknown,
  options: {
    expectedWallet?: string;
    fallbackAgentCardCid?: string;
  } = {},
): AgentMetadata {
  const source = asRecord(value) as AgentMetadataSource;
  const walletAddress = normalizeWalletAddress(
    requireAgentField(optionalString(source.walletAddress), "walletAddress"),
  );
  const expectedWallet = optionalString(options.expectedWallet);

  if (expectedWallet && walletAddress !== normalizeWalletAddress(expectedWallet)) {
    throw new Error("Agent metadata wallet does not match the requested agent wallet");
  }

  return {
    name: requireAgentField(optionalString(source.name), "name"),
    description: requireAgentField(optionalString(source.description), "description"),
    agentCardUri: normalizeAgentCardUri(
      optionalString(source.agentCardUri) || optionalString(source.cid),
      options.fallbackAgentCardCid || optionalString(source.cid) || undefined,
    ),
    creator: requireAgentField(optionalString(source.creator), "creator"),
    walletAddress,
    dnaHash: requireAgentField(optionalString(source.dnaHash), "dnaHash"),
    model: requireAgentField(optionalString(source.model), "model"),
    framework: optionalString(source.framework) || "manowar",
    plugins: Array.isArray(source.plugins) ? source.plugins as AgentMetadata["plugins"] : [],
    createdAt: optionalString(source.createdAt) || new Date().toISOString(),
    endpoints: normalizeAgentEndpoints(source),
  };
}

async function fetchAgentMetadataFromIpfs(agentCardCid: string, expectedWallet: string): Promise<AgentMetadata> {
  let lastError: Error | null = null;

  for (const pinataGateway of PINATA_GATEWAY_URLS) {
    try {
      const response = await fetch(`${pinataGateway}/${agentCardCid}`, {
        headers: {
          Accept: "application/json",
        },
      });
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }

      const data = await response.json() as unknown;
      return normalizeAgentMetadata(data, {
        expectedWallet,
        fallbackAgentCardCid: agentCardCid,
      });
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
  composeKeyToken?: string;
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
      headers: {
        ...(params.composeKeyToken ? { Authorization: `Bearer ${params.composeKeyToken}` } : {}),
        ...withSessionHeaders({
          userAddress: params.userAddress,
          chainId: params.chainId,
        }),
      },
    });
  } catch {
    return null;
  }
}

export async function redeemLocalLinkToken(params: {
  apiUrl: string;
  deviceId: string;
  token?: string;
  connectedUserAddress?: string;
}): Promise<RedeemedLocalContext> {
  const response = await requestJson<{ success: boolean; context: RedeemedLocalContext }>(
    `${normalizeBase(params.apiUrl)}/api/local/link-token/redeem`,
    {
      method: "POST",
      body: JSON.stringify({
        ...(params.token ? { token: params.token } : {}),
        ...(params.connectedUserAddress ? { connectedUserAddress: params.connectedUserAddress } : {}),
        deviceId: params.deviceId,
      }),
    },
  );

  if (!response.success) {
    throw new Error("Local deep-link redemption failed");
  }

  return response.context;
}

export async function registerLocalDeployment(params: {
  apiUrl: string;
  identity: LocalIdentityContext;
  agentWallet: string;
  agentCardCid: string;
  localVersion: string;
  deployedAt: number;
}): Promise<void> {
  await requestJson(
    `${normalizeBase(params.apiUrl)}/api/local/deployments/register`,
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
        localVersion: params.localVersion,
        deployedAt: params.deployedAt,
        chainId: params.identity.chainId,
      }),
    },
  );
}

export async function fetchAgentMetadata(params: {
  apiUrl: string;
  agentWallet: string;
  agentCardCid?: string;
}): Promise<AgentMetadata> {
  try {
    const response = await requestJson<unknown>(
      `${normalizeBase(params.apiUrl)}/agent/${walletPath(params.agentWallet)}`,
      { method: "GET" },
    );
    return normalizeAgentMetadata(response, {
      expectedWallet: params.agentWallet,
      fallbackAgentCardCid: params.agentCardCid,
    });
  } catch (error) {
    if (!params.agentCardCid) {
      throw error;
    }
    return fetchAgentMetadataFromIpfs(params.agentCardCid, params.agentWallet);
  }
}

export async function fetchBackpackConnections(params: {
  apiUrl: string;
  userAddress: string;
}): Promise<BackpackConnectionInfo[]> {
  const response = await requestJson<{ connections?: BackpackConnectionInfo[] }>(
    `${normalizeBase(params.apiUrl)}/api/backpack/connections?userAddress=${encodeURIComponent(params.userAddress)}`,
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
    return { success: false, error: "Local managed storage is unavailable" };
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

  const paths = await getLocalPaths();
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
      relativePath: relativeDir,
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

export async function createLocalLinkToken(params: {
  apiUrl: string;
  composeKeyToken: string;
  userAddress: string;
  chainId: number;
  payload: CreateLinkTokenRequest;
}): Promise<{ deepLinkUrl: string; token: string; expiresAt: number }> {
  return requestJson(`${normalizeBase(params.apiUrl)}/api/local/link-token`, {
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
  identity: Pick<LocalIdentityContext, "userAddress" | "chainId" | "composeKeyToken">;
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
        ...(params.identity.composeKeyToken
          ? { Authorization: `Bearer ${params.identity.composeKeyToken}` }
          : {}),
        ...withSessionHeaders({
          userAddress: params.identity.userAddress,
          chainId: params.identity.chainId,
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
  identity: Pick<LocalIdentityContext, "userAddress" | "chainId" | "composeKeyToken">;
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
      headers: {
        ...(params.identity.composeKeyToken
          ? { Authorization: `Bearer ${params.identity.composeKeyToken}` }
          : {}),
        ...withSessionHeaders({
          userAddress: params.identity.userAddress,
          chainId: params.identity.chainId,
        }),
      },
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
  identity: Pick<LocalIdentityContext, "userAddress" | "chainId" | "composeKeyToken">;
  keyId: string;
}): Promise<boolean> {
  try {
    await requestJson(
      `${normalizeBase(params.apiUrl)}/api/keys/${params.keyId}`,
      {
        method: "DELETE",
        headers: {
          ...(params.identity.composeKeyToken
            ? { Authorization: `Bearer ${params.identity.composeKeyToken}` }
            : {}),
          ...withSessionHeaders({
            userAddress: params.identity.userAddress,
            chainId: params.identity.chainId,
          }),
        },
      },
    );
    return true;
  } catch {
    return false;
  }
}

export async function getActiveSessionStatus(params: {
  apiUrl: string;
  identity: Pick<LocalIdentityContext, "userAddress" | "chainId" | "composeKeyToken">;
}): Promise<ActiveSessionStatusResponse | null> {
  const response = await requestJson<ActiveSessionStatusResponse>(
    `${normalizeBase(params.apiUrl)}/api/session`,
    {
      method: "GET",
      headers: {
        ...(params.identity.composeKeyToken
          ? { Authorization: `Bearer ${params.identity.composeKeyToken}` }
          : {}),
        ...withSessionHeaders({
          userAddress: params.identity.userAddress,
          chainId: params.identity.chainId,
        }),
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
    budgetLimit: response.budgetLimit,
    budgetUsed: response.budgetUsed,
    budgetLocked: response.budgetLocked,
    budgetRemaining: response.budgetRemaining,
    expiresAt: response.expiresAt,
    chainId: response.chainId,
    name: response.name,
    status: response.status,
  };
}

export const getSessionStatus = getActiveSessionStatus;
