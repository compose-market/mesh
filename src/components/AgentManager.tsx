import { useMemo, useState } from "react";
import { Eye, Globe, Loader2, Play, ShieldCheck, Square, Trash2 } from "lucide-react";
import {
  fetchAgentMetadata,
  registerDesktopDeployment,
} from "../lib/api";
import { ensureAgentWorkspace } from "../lib/storage";
import type {
  DesktopRuntimeState,
  ImmutableAgentLock,
  InstalledAgent,
  SessionState,
} from "../lib/types";

interface AgentManagerProps {
  state: DesktopRuntimeState;
  session: SessionState;
  appVersion: string;
  onStateChange: (state: DesktopRuntimeState) => Promise<void>;
  onActivateAgent: (agentWallet: string | null) => void;
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
}): Promise<ImmutableAgentLock> {
  const ids = pluginIds(metadata.plugins);
  const mcpToolsHash = await sha256(ids.join("|"));
  return {
    agentWallet: metadata.walletAddress.toLowerCase(),
    agentCardCid: extractCid(metadata.agentCardUri),
    modelId: metadata.model,
    mcpToolsHash,
    lockedAt: Date.now(),
  };
}

async function validateImmutableLock(agent: InstalledAgent, manowarUrl: string): Promise<void> {
  const canonical = await fetchAgentMetadata({
    manowarUrl,
    agentWallet: agent.agentWallet,
  });
  const canonicalLock = await buildLock({
    walletAddress: canonical.walletAddress,
    agentCardUri: canonical.agentCardUri,
    model: canonical.model,
    plugins: canonical.plugins,
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
  appVersion,
  onStateChange,
  onActivateAgent,
}: AgentManagerProps) {
  const [loading, setLoading] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [viewing, setViewing] = useState<InstalledAgent | null>(null);

  const activeIdentity = state.identity;
  const runningCount = useMemo(() => state.installedAgents.filter((agent) => agent.running).length, [state.installedAgents]);

  const deployCurrentAgent = async () => {
    if (!activeIdentity) {
      setError("Deep-link context required before deployment.");
      return;
    }
    if (!state.permissionDefaults.filesystemWrite || !state.permissionDefaults.filesystemEdit) {
      setError("Enable Filesystem Write and Filesystem Edit in Settings before deploying local agents.");
      return;
    }

    const agentWallet = activeIdentity.agentWallet.toLowerCase();
    setLoading(`deploy:${agentWallet}`);
    setError(null);

    try {
      const metadata = await fetchAgentMetadata({
        manowarUrl: state.settings.manowarUrl,
        agentWallet,
      });

      const lock = await buildLock({
        walletAddress: metadata.walletAddress,
        agentCardUri: metadata.agentCardUri,
        model: metadata.model,
        plugins: metadata.plugins,
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
        },
      };

      await ensureAgentWorkspace(installed);
      await registerDesktopDeployment({
        lambdaUrl: state.settings.lambdaUrl,
        identity: activeIdentity,
        agentWallet,
        agentCardCid: installed.lock.agentCardCid,
        desktopVersion: appVersion,
        deployedAt: Date.now(),
      });

      const nextState: DesktopRuntimeState = {
        ...state,
        installedAgents: existing
          ? state.installedAgents.map((agent) => (agent.agentWallet === installed.agentWallet ? installed : agent))
          : [...state.installedAgents, installed],
      };
      await onStateChange(nextState);
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
        await validateImmutableLock(target, state.settings.manowarUrl);
      }

      const shouldRun = !target.running;
      const nextAgents = state.installedAgents.map((agent) =>
        agent.agentWallet === agentWallet
          ? { ...agent, running: shouldRun }
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
    if (!target.permissions.filesystemDelete) {
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
    const nextAgents = state.installedAgents.map((agent) => {
      if (agent.agentWallet !== agentWallet) {
        return agent;
      }
      return {
        ...agent,
        network: {
          ...agent.network,
          enabled: nextEnabled,
          status: nextEnabled ? agent.network.status : "dormant",
          lastError: nextEnabled ? agent.network.lastError : null,
          updatedAt: Date.now(),
        },
      };
    });
    await onStateChange({ ...state, installedAgents: nextAgents });
  };

  return (
    <div className="agent-manager">
      <div className="skills-manager-header">
        <div>
          <h2>Local Agents</h2>
          <p className="subtitle">{state.installedAgents.length} deployed · {runningCount} running</p>
        </div>
        <button
          className="primary"
          disabled={!activeIdentity || loading !== null}
          onClick={() => {
            void deployCurrentAgent();
          }}
        >
          {loading?.startsWith("deploy:") ? <Loader2 size={14} className="spinner" /> : null}
          Deploy Current Agent
        </button>
      </div>

      {error ? <div className="notification notification-error">{error}</div> : null}

      {state.installedAgents.length === 0 ? (
        <div className="empty-state">
          <h3>No local agents deployed</h3>
          <p>Open desktop from market or agent page to deploy the canonical agent locally.</p>
        </div>
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
                    onClick={() => setViewing(agent)}
                    title="View lock details"
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

      {viewing ? (
        <div className="modal-overlay" onClick={() => setViewing(null)}>
          <div className="modal" onClick={(event) => event.stopPropagation()}>
            <h3>Immutable Agent Lock</h3>
            <pre className="agent-json">{JSON.stringify(viewing.lock, null, 2)}</pre>
            <button onClick={() => setViewing(null)}>Close</button>
          </div>
        </div>
      ) : null}
    </div>
  );
}
