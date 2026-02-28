import { invoke } from "@tauri-apps/api/core";
import { parse as parseYaml } from "yaml";
import type { SkillRequirements } from "./types";

interface ParsedFrontmatter {
  [key: string]: unknown;
}

function asRecord(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }
  return value as Record<string, unknown>;
}

function asStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value
    .filter((entry): entry is string => typeof entry === "string")
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);
}

function parseJsonString(value: string): unknown {
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

export function parseSkillFrontmatter(content: string): ParsedFrontmatter {
  const match = content.match(/^---\n([\s\S]*?)\n---\n?/);
  if (!match) {
    return {};
  }

  try {
    const parsed = parseYaml(match[1]);
    return asRecord(parsed);
  } catch {
    return {};
  }
}

function extractOpenClawMetadata(frontmatter: ParsedFrontmatter): Record<string, unknown> {
  const metadataRaw = frontmatter.metadata;
  const metadataValue = typeof metadataRaw === "string" ? parseJsonString(metadataRaw) : metadataRaw;
  const metadata = asRecord(metadataValue);
  return asRecord(metadata.openclaw);
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
  if (envKeys.length === 0) {
    return [];
  }

  // Desktop runtime does not expose host env vars like OpenClaw gateway does.
  // Surface env requirements as informational and let users configure keys later.
  return envKeys.map((entry) => `env:${entry}`);
}

function detectUnsupportedOs(osList: string[]): string[] {
  if (osList.length === 0) {
    return [];
  }
  const normalized = navigator.platform.toLowerCase();
  const supported = osList.some((candidate) => normalized.includes(candidate.toLowerCase()));
  return supported ? [] : [`os:${osList.join("|")}`];
}

export async function extractSkillRequirements(skillMd: string): Promise<SkillRequirements> {
  const frontmatter = parseSkillFrontmatter(skillMd);
  const openclaw = extractOpenClawMetadata(frontmatter);
  const requires = asRecord(openclaw.requires);

  const bins = asStringArray(requires.bins);
  const anyBins = asStringArray(requires.anyBins);
  const env = asStringArray(requires.env);
  const os = asStringArray(openclaw.os ?? requires.os);

  const missing: string[] = [];

  const missingBins = await detectMissingBins(bins);
  for (const bin of missingBins) {
    missing.push(`bin:${bin}`);
  }

  if (anyBins.length > 0) {
    const anyMissing = await detectMissingBins(anyBins);
    if (anyMissing.length === anyBins.length) {
      missing.push(`anyBin:${anyBins.join("|")}`);
    }
  }

  for (const missingEnv of detectMissingEnv(env)) {
    missing.push(missingEnv);
  }

  for (const unsupported of detectUnsupportedOs(os)) {
    missing.push(unsupported);
  }

  const hardMissing = missing.filter((entry) => entry.startsWith("bin:") || entry.startsWith("anyBin:") || entry.startsWith("os:"));
  return {
    bins: [...bins, ...anyBins],
    env,
    os,
    missing,
    eligible: hardMissing.length === 0,
  };
}
