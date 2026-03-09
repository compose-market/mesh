import { useMemo, useState } from "react";
import { Eye, Globe, Loader2, Play, ShieldCheck, Square, Trash2 } from "lucide-react";
import { ShellButton, ShellEmptyState, ShellNotice, ShellPageHeader } from "@compose-market/theme/shell";
import { fetchAgentMetadata } from "../lib/api";
import { appendAgentReport, buildMeshAgentCard } from "../lib/agent";
import {
  daemonInstallAgent,
  daemonMeshSet,
  daemonStartAgent,
  daemonStatusToWorkerState,
  daemonStopAgent,
} from "../lib/daemon";
import { ensureAgentWorkspace, permissionAllows } from "../lib/storage";
import type {
  AgentDnaLock,
  DesktopRuntimeState,
  InstalledAgent,
  SessionState,
} from "../lib/types";

interface AgentManagerProps {
  state: DesktopRuntimeState;
  session: SessionState;
  onStateChange: (state: DesktopRuntimeState) => Promise<void>;
  onActivateAgent: (agentWallet: string | null) => void;
  onOpenAgent: (agentWallet: string) => void;
  onBrowseMarket: () => void;
}

async function sha256(text: string): Promise<string> {
  const input = new TextEncoder().encode(text);
  const hash = await crypto.subtle.digest("SHA-256", input);
  const bytes = Array.from(new Uint8Array(hash));
  return bytes.map((b) => b.toString(16).padStart(2, "0")).join("");
}

function pluginIds(plugins: Array<string | { registryId: string; name?: string; origin?: string }>): string[] {
  return plugins
    .map((item) => (typeof item === "string" ? item : item.registryId))
    .filter((item) => item.trim().length > 0)
    .map((item) => item.trim().toLowerCase())
    .sort();
}

function extractCid(agentCardUri: string): string {
  const cid = agentCardUri.replace("ipfs://", "").split("/")[0];
  if (!cid || cid.length < 32) {
    throw new Error("Invalid agentCardUri CID");
  }
  return cid;
}

async function buildLock(metadata: {
  walletAddress: string;
  agentCardUri: string;
  model: string;
  plugins: Array<string | { registryId: string }>;
  chainId: number;
  dnaHash?: string;
}): Promise<AgentDnaLock> {
  const ids = pluginIds(metadata.plugins);
  const mcpToolsHash = await sha256(ids.join("|"));
  return {
    agentWallet: metadata.walletAddress.toLowerCase(),
    agentCardCid: extractCid(metadata.agentCardUri),
    modelId: metadata.model,
    mcpToolsHash,
    chainId: metadata.chainId,
    dnaHash: metadata.dnaHash || "",
    lockedAt: Date.now(),
  };
}

async function validateImmutableLock(agent: InstalledAgent, runtimeUrl: string): Promise<void> {
  const canonical = await fetchAgentMetadata({
    runtimeUrl,
    agentWallet: agent.agentWallet,
  });
  const canonicalLock = await buildLock({
    walletAddress: canonical.walletAddress,
    agentCardUri: canonical.agentCardUri,
    model: canonical.model,
    plugins: canonical.plugins,
    chainId: agent.lock.chainId,
    dnaHash: canonical.dnaHash,
  });

  if (canonicalLock.modelId !== agent.lock.modelId) {
    throw new Error(`Model mismatch for ${agent.agentWallet}: local=${agent.lock.modelId} canonical=${canonicalLock.modelId}`);
  }
  if (canonicalLock.mcpToolsHash !== agent.lock.mcpToolsHash) {
    throw new Error(`MCP tools mismatch for ${agent.agentWallet}`);
  }
  if (canonicalLock.agentCardCid !== agent.lock.agentCardCid) {
    throw new Error(`agentCard CID mismatch for ${agent.agentWallet}`);
  }
}

export function AgentManager({
  state,
  session,
  onStateChange,
  onActivateAgent,
  onOpenAgent,
  onBrowseMarket,
}: AgentManagerProps) {
  const [loading, setLoading] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const activeIdentity = state.identity;
  const runningCount = useMemo(() => state.installedAgents.filter((agent) => agent.running).length, [state.installedAgents]);

  const deployCurrentAgent = async () => {
    if (!activeIdentity) {
      setError("Deep-link context required before deployment.");
      return;
    }
    if (!permissionAllows(state.permissionDefaults.filesystemWrite) || !permissionAllows(state.permissionDefaults.filesystemEdit)) {
      setError("Enable Filesystem Write and Filesystem Edit in Settings before deploying local agents.");
      return;
    }

    const agentWallet = activeIdentity.agentWallet.toLowerCase();
    setLoading(`deploy:${agentWallet}`);
    setError(null);

    try {
      const metadata = await fetchAgentMetadata({
        runtimeUrl: state.settings.runtimeUrl,
        agentWallet,
      });

      const lock = await buildLock({
        walletAddress: metadata.walletAddress,
        agentCardUri: metadata.agentCardUri,
        model: metadata.model,
        plugins: metadata.plugins,
        chainId: activeIdentity.chainId,
        dnaHash: metadata.dnaHash,
      });

      const existing = state.installedAgents.find((agent) => agent.agentWallet === agentWallet);
      if (existing) {
        if (
          existing.lock.modelId !== lock.modelId ||
          existing.lock.mcpToolsHash !== lock.mcpToolsHash ||
          existing.lock.agentCardCid !== lock.agentCardCid
        ) {
          throw new Error("Local agent lock does not match canonical agentCard. Re-deploy is required.");
        }
      }

      const installed: InstalledAgent = existing || {
        agentWallet,
        metadata,
        lock,
        addedAt: Date.now(),
        running: false,
        runtimeId: crypto.randomUUID(),
        heartbeat: {
          enabled: true,
          intervalMs: 30000,
          lastRunAt: null,
          lastResult: null,
        },
        permissions: {
          ...state.permissionDefaults,
        },
        network: {
          enabled: false,
          status: "dormant",
          peerId: null,
          listenMultiaddrs: [],
          peersDiscovered: 0,
          lastHeartbeatAt: null,
          lastError: null,
          updatedAt: 0,
          publicCard: null,
          recentPings: [],
          interactions: [],
        },
        skillStates: {},
        reports: [],
      };

      await ensureAgentWorkspace(installed);
      const daemonStatus = await daemonInstallAgent({
        agentWallet: installed.agentWallet,
        agentCardCid: installed.lock.agentCardCid,
        chainId: installed.lock.chainId,
        modelId: installed.lock.modelId,
        mcpToolsHash: installed.lock.mcpToolsHash,
        dnaHash: installed.lock.dnaHash,
      });
      const reportSeed = appendAgentReport(
        {
          ...installed,
          workerState: daemonStatusToWorkerState(daemonStatus),
          network: {
            ...installed.network,
            publicCard: buildMeshAgentCard(installed),
          },
        },
        {
          kind: "deployment",
          title: existing ? "Local deployment refreshed" : "Local deployment created",
          summary: `${metadata.name} is now available on this device.`,
          outcome: "success",
        },
      );

      const nextState: DesktopRuntimeState = {
        ...state,
        installedAgents: existing
          ? state.installedAgents.map((agent) => (agent.agentWallet === reportSeed.agentWallet ? reportSeed : agent))
          : [...state.installedAgents, reportSeed],
      };
      await onStateChange(nextState);
      onOpenAgent(agentWallet);
    } catch (deployError) {
      console.error("[agents] deployment failed", deployError);
      setError(deployError instanceof Error ? deployError.message : "Failed to deploy local agent.");
    } finally {
      setLoading(null);
    }
  };

  const toggleAgent = async (agentWallet: string) => {
    const target = state.installedAgents.find((agent) => agent.agentWallet === agentWallet);
    if (!target) return;

    if (!target.running && !session.active) {
      setError("Session is inactive. Start a session from the Desktop header and try again.");
      return;
    }

    setLoading(`toggle:${agentWallet}`);
    setError(null);
    try {
      if (!target.running) {
        await validateImmutableLock(target, state.settings.runtimeUrl);
      }

      const shouldRun = !target.running;
      const daemonStatus = shouldRun
        ? await daemonStartAgent(agentWallet)
        : await daemonStopAgent(agentWallet);
      const nextAgents = state.installedAgents.map((agent) =>
        agent.agentWallet === agentWallet
          ? appendAgentReport(
            {
              ...agent,
              running: shouldRun,
              workerState: daemonStatusToWorkerState(daemonStatus),
            },
            {
              kind: "runtime",
              title: shouldRun ? "Agent started" : "Agent stopped",
              summary: shouldRun
                ? `${agent.metadata.name} is running with the current session budget.`
                : `${agent.metadata.name} stopped on this device.`,
              outcome: "info",
            },
          )
          : shouldRun
            ? { ...agent, running: false }
            : agent,
      );

      await onStateChange({ ...state, installedAgents: nextAgents });
      onActivateAgent(shouldRun ? agentWallet : null);
    } catch (toggleError) {
      setError(toggleError instanceof Error ? toggleError.message : "Failed to toggle local agent.");
    } finally {
      setLoading(null);
    }
  };

  const removeAgent = async (agentWallet: string) => {
    const target = state.installedAgents.find((agent) => agent.agentWallet === agentWallet);
    if (!target) {
      return;
    }
    if (!permissionAllows(target.permissions.filesystemDelete)) {
      setError("Enable Filesystem Delete in Settings before removing local deployments.");
      return;
    }
    const nextAgents = state.installedAgents.filter((agent) => agent.agentWallet !== agentWallet);
    await onStateChange({ ...state, installedAgents: nextAgents });
    onActivateAgent(null);
  };

  const toggleAgentNetwork = async (agentWallet: string) => {
    const target = state.installedAgents.find((agent) => agent.agentWallet === agentWallet);
    if (!target) {
      return;
    }

    const nextEnabled = !target.network.enabled;
    await daemonMeshSet(agentWallet, nextEnabled);
    const nextAgents = state.installedAgents.map((agent) => {
      if (agent.agentWallet !== agentWallet) {
        return agent;
      }
      return {
        ...appendAgentReport(agent, {
          kind: "mesh",
          title: nextEnabled ? "Mesh signaling enabled" : "Mesh signaling disabled",
          summary: nextEnabled
            ? `${agent.metadata.name} is now broadcasting on the local mesh.`
            : `${agent.metadata.name} left the local mesh.`,
          outcome: "info",
        }),
        network: {
          ...agent.network,
          enabled: nextEnabled,
          status: nextEnabled ? agent.network.status : "dormant",
          lastError: nextEnabled ? agent.network.lastError : null,
          publicCard: buildMeshAgentCard(agent),
          updatedAt: Date.now(),
        },
      };
    });
    await onStateChange({ ...state, installedAgents: nextAgents });
  };

  return (
    <div className="agent-manager">
      <ShellPageHeader
        eyebrow="My Agents"
        title="Local Agents"
        subtitle={`${state.installedAgents.length} deployed · ${runningCount} running`}
        actions={(
          <>
            <ShellButton tone="secondary" onClick={onBrowseMarket}>
            Browse Market
            </ShellButton>
            <ShellButton
              tone="primary"
              disabled={!activeIdentity || loading !== null}
              onClick={() => {
                void deployCurrentAgent();
              }}
            >
              {loading?.startsWith("deploy:") ? <Loader2 size={14} className="spinner" /> : null}
              Deploy Linked Agent
            </ShellButton>
          </>
        )}
      />

      {error ? <ShellNotice tone="error" className="notification">{error}</ShellNotice> : null}

      {state.installedAgents.length === 0 ? (
        <ShellEmptyState
          title="No local agents deployed"
          description="Open the desktop flow from market or from an agent page to deploy the canonical agent locally."
        />
      ) : (
        <div className="agents-list">
          {state.installedAgents.map((agent) => {
            const pluginNames = pluginIds(agent.metadata.plugins);
            return (
              <div key={agent.agentWallet} className={`agent-card ${agent.running ? "running" : ""}`}>
                <div className="agent-info">
                  <h3>{agent.metadata.name}</h3>
                  <p>{agent.metadata.description}</p>
                  <div className="agent-meta">
                    <span>Model: {agent.lock.modelId}</span>
                    <span>Framework: {agent.metadata.framework}</span>
                    <span>CID: {agent.lock.agentCardCid.slice(0, 12)}...</span>
                  </div>
                  <div className="agent-plugins">
                    {pluginNames.map((name) => (
                      <span key={name} className="plugin-tag">{name}</span>
                    ))}
                  </div>
                  <div className="agent-meta">
                    <span><ShieldCheck size={12} /> Immutable lock active</span>
                    <span>
                      <Globe size={12} />
                      Mesh: {agent.network.enabled ? agent.network.status : "disabled"}
                    </span>
                  </div>
                  <button
                    className="agent-open-link"
                    onClick={() => onOpenAgent(agent.agentWallet)}
                  >
                    Open agent settings
                  </button>
                </div>

                <div className="agent-actions">
                  <button
                    className={`icon-btn ${agent.running ? "running" : ""}`}
                    onClick={() => {
                      void toggleAgent(agent.agentWallet);
                    }}
                    disabled={loading !== null}
                    title={agent.running ? "Stop local agent" : "Start local agent"}
                  >
                    {loading === `toggle:${agent.agentWallet}` ? (
                      <Loader2 size={16} className="spinner" />
                    ) : agent.running ? (
                      <Square size={18} />
                    ) : (
                      <Play size={18} />
                    )}
                  </button>
                  <button
                    className="icon-btn"
                    onClick={() => onOpenAgent(agent.agentWallet)}
                    title="Open agent settings"
                  >
                    <Eye size={18} />
                  </button>
                  <button
                    className={`icon-btn ${agent.network.enabled ? "running" : ""}`}
                    onClick={() => {
                      void toggleAgentNetwork(agent.agentWallet);
                    }}
                    disabled={loading !== null}
                    title={agent.network.enabled ? "Disable mesh networking" : "Enable mesh networking (--network)"}
                  >
                    <Globe size={18} />
                  </button>
                  <button
                    className="icon-btn danger"
                    onClick={() => {
                      void removeAgent(agent.agentWallet);
                    }}
                    title="Remove local deployment"
                  >
                    <Trash2 size={18} />
                  </button>
                </div>
              </div>
            );
          })}
        </div>
      )}

    </div>
  );
}
