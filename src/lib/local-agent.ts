import { invoke } from "@tauri-apps/api/core";
import type { AgentSkillState, AgentTaskReport, InstalledAgent, LocalRuntimeState } from "./types";

const CONTROL_INSTRUCTION = [
  "Return only a single JSON object.",
  'Use this shape: {"reply":string,"report":null|{"title":string,"summary":string,"details"?:string,"outcome":"success"|"warning"|"error"|"info"},"skill":null|{"name":string,"markdown":string},"actions":[]}.',
].join(" ");

interface ManagedDocument {
  label: string;
  content: string;
}

interface LocalAgentMessage {
  role: "user" | "assistant";
  content: string;
}

interface LocalSkillPayload {
  name: string;
  markdown: string;
}

export interface LocalAgentStructuredReply {
  reply: string;
  report: Omit<AgentTaskReport, "id" | "createdAt" | "kind"> | null;
  skill: LocalSkillPayload | null;
}

export interface LocalAgentConversationResult extends LocalAgentStructuredReply {
  authoredSkillId: string | null;
  authoredSkillPath: string | null;
  raw: string;
}

export function createLocalConversationThreadId(agentWallet: string): string {
  const randomPart = typeof crypto !== "undefined" && typeof crypto.randomUUID === "function"
    ? crypto.randomUUID()
    : `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;

  return `local-agent:${agentWallet.trim().toLowerCase()}:chat:${randomPart}`;
}

function slugify(value: string): string {
  const normalized = value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return normalized || "skill";
}

export function agentAuthoredSkillIdFromName(name: string): string {
  return `agent:${slugify(name)}`;
}

export function createAgentAuthoredSkillState(skillName: string, relativePath: string, updatedAt = Date.now()): AgentSkillState {
  return {
    skillId: agentAuthoredSkillIdFromName(skillName),
    enabled: true,
    eligible: true,
    source: "generated",
    revision: relativePath,
    updatedAt,
  };
}

function asString(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

function normalizeReport(value: unknown): LocalAgentStructuredReply["report"] {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }

  const record = value as Record<string, unknown>;
  const title = asString(record.title);
  const summary = asString(record.summary);
  const outcomeRaw = asString(record.outcome);
  const outcome = outcomeRaw === "success" || outcomeRaw === "warning" || outcomeRaw === "error" || outcomeRaw === "info"
    ? outcomeRaw
    : "info";

  if (!title || !summary) {
    return null;
  }

  const details = asString(record.details);
  return {
    title,
    summary,
    details: details || undefined,
    outcome,
  };
}

function normalizeSkill(value: unknown): LocalSkillPayload | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }

  const record = value as Record<string, unknown>;
  const name = asString(record.name);
  const markdown = asString(record.markdown);
  if (!name || !markdown) {
    return null;
  }
  return { name, markdown };
}

export function parseLocalAgentStructuredReply(raw: string): LocalAgentStructuredReply {
  const trimmed = raw.trim();
  if (!trimmed) {
    return {
      reply: "",
      report: null,
      skill: null,
    };
  }

  try {
    const parsed = JSON.parse(trimmed) as Record<string, unknown>;
    return {
      reply: asString(parsed.reply),
      report: normalizeReport(parsed.report),
      skill: normalizeSkill(parsed.skill),
    };
  } catch {
    return {
      reply: trimmed,
      report: null,
      skill: null,
    };
  }
}

function buildDocumentsSection(documents: ManagedDocument[]): string {
  return documents
    .map((document) => `[${document.label}]\n${document.content.trim()}`)
    .join("\n\n");
}

function buildSkillCatalog(documents: ManagedDocument[]): string {
  const labels = documents
    .filter((document) => document.label.endsWith("/SKILL.md"))
    .map((document) => document.label.replace(/\/SKILL\.md$/i, ""))
    .sort((left, right) => left.localeCompare(right));

  if (labels.length === 0) {
    return "No local skills are currently installed.";
  }

  return labels.map((label) => `- ${label}`).join("\n");
}

export function buildLocalAgentSystemPrompt(agent: InstalledAgent, documents: ManagedDocument[]): string {
  return [
    `You are ${agent.metadata.name}, your original purpose: ${agent.metadata.description}.`,
    "You're a personal assistant running on this user's device. Use the skills below or create new ones to keep improving",
    CONTROL_INSTRUCTION,
    "",
    "Available local skills:",
    buildSkillCatalog(documents),
    "",
    "Local operating files:",
    buildDocumentsSection(documents),
  ].join("\n");
}

function isTauriRuntime(): boolean {
  return typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;
}

function ensureTauriRuntime(): void {
  if (!isTauriRuntime()) {
    throw new Error("Local agent execution requires the mesh desktop app");
  }
}

export async function runLocalAgentConversation(input: {
  agent: InstalledAgent;
  state: LocalRuntimeState;
  history: LocalAgentMessage[];
  message: string;
  threadId: string;
}): Promise<LocalAgentConversationResult> {
  ensureTauriRuntime();
  return invoke<LocalAgentConversationResult>("daemon_run_local_agent_conversation", {
    agentWallet: input.agent.agentWallet,
    history: input.history,
    message: input.message,
    threadId: input.threadId,
  });
}
