import { getAgentHeartbeatRelativePath, readManagedFile } from "./storage";

const HEARTBEAT_OK_TOKEN = "HEARTBEAT_OK";
const BASE_HEARTBEAT_PROMPT = `Read HEARTBEAT.md and execute only what it asks for.
If no work is needed right now, respond exactly with HEARTBEAT_OK.`;
const DEDUP_WINDOW_MS = 6 * 60 * 60 * 1000;

interface DedupEntry {
  message: string;
  createdAt: number;
}

export interface HeartbeatStartConfig {
  agentWallet: string;
  intervalMs?: number;
  onExecute: (prompt: string) => Promise<string>;
  onAlert?: (message: string) => void;
  onTickComplete?: (result: "ok" | "alert" | "error") => void;
}

export class HeartbeatService {
  private timer: number | null = null;
  private running = false;
  private agentWallet: string | null = null;
  private intervalMs = 30000;
  private lastTickAt = 0;
  private onExecute: ((prompt: string) => Promise<string>) | null = null;
  private onAlert: ((message: string) => void) | null = null;
  private onTickComplete: ((result: "ok" | "alert" | "error") => void) | null = null;
  private dedupe = new Map<string, DedupEntry>();

  start(config: HeartbeatStartConfig): void {
    this.stop();
    this.agentWallet = config.agentWallet.toLowerCase();
    this.intervalMs = config.intervalMs ?? 30000;
    this.onExecute = config.onExecute;
    this.onAlert = config.onAlert ?? null;
    this.onTickComplete = config.onTickComplete ?? null;
    this.running = true;

    this.timer = window.setInterval(() => {
      void this.tick();
    }, this.intervalMs);

    void this.tick();
  }

  stop(): void {
    this.running = false;
    if (this.timer !== null) {
      window.clearInterval(this.timer);
      this.timer = null;
    }
    this.agentWallet = null;
    this.onExecute = null;
    this.onAlert = null;
    this.onTickComplete = null;
  }

  private normalizeAlert(message: string): string {
    return message.trim().toLowerCase().replace(/\s+/g, " ").slice(0, 200);
  }

  private shouldEmitAlert(message: string): boolean {
    const now = Date.now();
    for (const [key, value] of this.dedupe.entries()) {
      if (now - value.createdAt > DEDUP_WINDOW_MS) {
        this.dedupe.delete(key);
      }
    }

    const key = this.normalizeAlert(message);
    const existing = this.dedupe.get(key);
    if (existing && now - existing.createdAt < DEDUP_WINDOW_MS) {
      return false;
    }
    this.dedupe.set(key, { message, createdAt: now });
    return true;
  }

  private async buildPrompt(): Promise<string | null> {
    if (!this.agentWallet) {
      return null;
    }
    const heartbeatPath = getAgentHeartbeatRelativePath(this.agentWallet);
    const heartbeatMd = await readManagedFile(heartbeatPath);
    if (!heartbeatMd || heartbeatMd.trim().length === 0) {
      return null;
    }
    return `${BASE_HEARTBEAT_PROMPT}\n\n[HEARTBEAT.md]\n${heartbeatMd.trim()}`;
  }

  private async tick(): Promise<void> {
    if (!this.running || !this.onExecute) {
      return;
    }
    const now = Date.now();
    if (now - this.lastTickAt < Math.floor(this.intervalMs * 0.8)) {
      return;
    }
    this.lastTickAt = now;

    const prompt = await this.buildPrompt();
    if (!prompt) {
      this.onTickComplete?.("ok");
      return;
    }

    try {
      const response = (await this.onExecute(prompt)).trim();
      const upper = response.toUpperCase();
      const isOk = upper === HEARTBEAT_OK_TOKEN || upper.startsWith(`${HEARTBEAT_OK_TOKEN}\n`);
      if (isOk) {
        this.onTickComplete?.("ok");
        return;
      }

      if (response.length > 0 && this.shouldEmitAlert(response)) {
        this.onAlert?.(response);
      }
      this.onTickComplete?.("alert");
    } catch (error) {
      console.error("[heartbeat] tick failed", error);
      this.onTickComplete?.("error");
    }
  }
}

export const heartbeatService = new HeartbeatService();
