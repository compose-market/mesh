import { invoke } from "@tauri-apps/api/core";
import { parse as parseYaml } from "yaml";
import type { SkillRequirements, SkillSource } from "./types";

interface ParsedFrontmatter {
  [key: string]: unknown;
}

export interface BuiltinSkillFile {
  relativePath: string;
  content: string;
}

export interface BuiltinSkillRoot {
  id: string;
  name: string;
  fullName: string;
  description: string;
  relativePath: string;
}

const BUILTIN_SKILL_SOURCE: SkillSource = {
  id: "built-in",
  name: "Built-in",
  description: "Built-in local skills",
  catalogUrl: "https://compose.market",
};

const ROOTS = [
  "skills/write-report",
  "skills/use-tools",
  "skills/start-convo",
  "skills/hello-mesh",
  "skills/use-mesh",
  "skills/ping-request",
] as const;

const RAW = import.meta.glob<string>("../../skills/global/**/SKILL.md", {
  eager: true,
  import: "default",
  query: "?raw",
});

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {};
}

function asStringArray(value: unknown): string[] {
  return Array.isArray(value)
    ? value.filter((entry): entry is string => typeof entry === "string").map((entry) => entry.trim()).filter(Boolean)
    : [];
}

function parseJsonString(value: string): unknown {
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

function metadataRecord(frontmatter: ParsedFrontmatter): Record<string, unknown> {
  const raw = frontmatter.metadata;
  return asRecord(typeof raw === "string" ? parseJsonString(raw) : raw);
}

function toLocalSkillPath(sourcePath: string): string {
  return sourcePath
    .replace(/^\.{2}\/\.{2}\/skills\/global\//, "skills/")
    .replace(/^\/+/, "");
}

const builtinSkillFiles: BuiltinSkillFile[] = Object.entries(RAW)
  .sort(([left], [right]) => left.localeCompare(right))
  .map(([sourcePath, content]) => ({
    relativePath: toLocalSkillPath(sourcePath),
    content,
  }));

const builtinSkillFileMap = new Map(
  builtinSkillFiles.map((entry) => [entry.relativePath, entry.content]),
);

export function parseSkillFrontmatter(content: string): ParsedFrontmatter {
  const match = content.match(/^---\n([\s\S]*?)\n---\n?/);
  if (!match) {
    return {};
  }

  try {
    return asRecord(parseYaml(match[1]));
  } catch {
    return {};
  }
}

export function extractSkillSummary(skillMd: string): {
  name: string | null;
  description: string | null;
} {
  const frontmatter = parseSkillFrontmatter(skillMd);
  const name = typeof frontmatter.name === "string" ? frontmatter.name.trim() : "";
  const description = typeof frontmatter.description === "string" ? frontmatter.description.trim() : "";
  return {
    name: name || null,
    description: description || null,
  };
}

const builtinSkillRoots: BuiltinSkillRoot[] = ROOTS.map((relativePath) => {
  const skillMd = builtinSkillFileMap.get(`${relativePath}/SKILL.md`) || "";
  const summary = extractSkillSummary(skillMd);
  const slug = relativePath.replace(/^skills\//, "");
  return {
    id: `built-in:${slug}`,
    name: summary.name || slug.toUpperCase(),
    fullName: `built-in/${slug}`,
    description: summary.description || `${slug} built-in skill`,
    relativePath,
  };
});

async function detectMissingBins(bins: string[]): Promise<string[]> {
  if (bins.length === 0) {
    return [];
  }
  try {
    return await invoke<string[]>("check_missing_binaries", { binaries: bins });
  } catch {
    return [...bins];
  }
}

function detectMissingEnv(envKeys: string[]): string[] {
  return envKeys.map((entry) => `env:${entry}`);
}

function detectUnsupportedOs(osList: string[]): string[] {
  if (osList.length === 0) {
    return [];
  }
  const normalized = navigator.platform.toLowerCase();
  return osList.some((candidate) => normalized.includes(candidate.toLowerCase()))
    ? []
    : [`os:${osList.join("|")}`];
}

export async function extractSkillRequirements(skillMd: string): Promise<SkillRequirements> {
  const frontmatter = parseSkillFrontmatter(skillMd);
  const metadata = metadataRecord(frontmatter);
  const requires = asRecord(metadata.requires);

  const bins = asStringArray(requires.bins);
  const anyBins = asStringArray(requires.anyBins);
  const env = asStringArray(requires.env);
  const os = asStringArray(metadata.os ?? requires.os);

  const missing: string[] = [];

  for (const bin of await detectMissingBins(bins)) {
    missing.push(`bin:${bin}`);
  }

  if (anyBins.length > 0) {
    const anyMissing = await detectMissingBins(anyBins);
    if (anyMissing.length === anyBins.length) {
      missing.push(`anyBin:${anyBins.join("|")}`);
    }
  }

  missing.push(...detectMissingEnv(env));
  missing.push(...detectUnsupportedOs(os));

  const hardMissing = missing.filter((entry) =>
    entry.startsWith("bin:") || entry.startsWith("anyBin:") || entry.startsWith("os:"),
  );

  return {
    bins: [...bins, ...anyBins],
    env,
    os,
    missing,
    eligible: hardMissing.length === 0,
  };
}

export function getBuiltinSkillSource(): SkillSource {
  return BUILTIN_SKILL_SOURCE;
}

export function listBuiltinSkillFiles(): BuiltinSkillFile[] {
  return builtinSkillFiles.map((entry) => ({ ...entry }));
}

export function listBuiltinSkillRoots(): BuiltinSkillRoot[] {
  return builtinSkillRoots.map((entry) => ({ ...entry }));
}
