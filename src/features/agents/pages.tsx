import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  Activity,
  ArrowLeft,
  BadgeDollarSign,
  Clock3,
  Cpu,
  ExternalLink,
  Eye,
  FileText,
  Globe,
  Loader2,
  MessageSquare,
  Play,
  Shield,
  ShieldCheck,
  Sparkles,
  Trash2,
} from "lucide-react";

import { ShellButton, ShellInput, ShellNotice, ShellPanel } from "@compose-market/theme/shell";
import {
  daemonRemoveAgent,
  daemonInstallAgent,
  mergeDaemonStatusIntoInstalledAgent,
  daemonTailLogs,
  daemonUpdatePermissions,
} from "../../lib/daemon";
import { fetchAgentMetadata } from "../../lib/api";
import { runLocalAgentConversation } from "../../lib/local-agent";

import {
  getDefaultPermissionPolicy,
  loadRuntimeState,
} from "../../lib/storage";
import type { AgentPermissionPolicy, LocalRuntimeState, InstalledAgent, MeshPeerSignal, SessionState } from "../../lib/types";
import { SkillsManager } from "../../components/skills-manager";
import { SkillsMarketplace } from "../../components/skills-store";
import { PermissionsPanel, nextPermissionDecision } from "../permissions";
import {
  appendAgentReport,
  buildAgentLock,
  createInstalledAgent,
  listPluginIds,
  summarizeAgentReportEconomics,
  syncInstalledAgent,
} from "./model";

type SkillsTab = "installed" | "browse";

interface AgentManagerPageProps {
  state: LocalRuntimeState;
  onStateChange: (state: LocalRuntimeState) => Promise<void>;
  onOpenAgent: (agentWallet: string) => void;
  onBrowse: () => void;
}

interface AgentDetailPageProps {
  agent: InstalledAgent;
  state: LocalRuntimeState;
  session: SessionState;
  meshPeers: MeshPeerSignal[];
  onBack: () => void;
  onStateChange: (next: LocalRuntimeState) => Promise<void>;
  onNotify: (type: "success" | "error", message: string) => void;
}

interface LocalChatMessage {
  id: string;
  role: "user" | "assistant";
  content: string;
  createdAt: number;
  failed?: boolean;
}

function formatChatTimestamp(value: number): string {
  return new Date(value).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
  });
}

function formatMicros(value: number): string {
  return `$${(value / 1_000_000).toFixed(2)}`;
}

function shortWallet(value: string): string {
  return `${value.slice(0, 8)}...${value.slice(-4)}`;
}

function getErrorMessage(error: unknown, fallback: string): string {
  if (error instanceof Error && error.message.trim().length > 0) {
    return error.message;
  }
  if (typeof error === "string" && error.trim().length > 0) {
    return error;
  }
  if (error && typeof error === "object") {
    const record = error as Record<string, unknown>;
    if (typeof record.message === "string" && record.message.trim().length > 0) {
      return record.message;
    }
    if (typeof record.error === "string" && record.error.trim().length > 0) {
      return record.error;
    }
  }
  return fallback;
}

type FleetSort = "name" | "cost" | "activity" | "status";

const SORT_LABELS: Record<FleetSort, string> = {
  name: "Name",
  cost: "Cost ↓",
  activity: "Activity ↓",
  status: "Status",
};

const SORT_CYCLE: FleetSort[] = ["name", "cost", "activity", "status"];

export function AgentManagerPage({
  state,
  onStateChange,
  onOpenAgent,
  onBrowse,
}: AgentManagerPageProps) {
  const [loading, setLoading] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [sortBy, setSortBy] = useState<FleetSort>("status");
  const runningCount = useMemo(() => state.installedAgents.filter((agent) => agent.running).length, [state.installedAgents]);

  const fleetEconomics = useMemo(() => {
    let totalCost = 0;
    let totalRevenue = 0;
    for (const agent of state.installedAgents) {
      const econ = summarizeAgentReportEconomics(agent.reports);
      totalCost += econ.costMicros;
      totalRevenue += econ.revenueMicros;
    }
    return { totalCost, totalRevenue };
  }, [state.installedAgents]);

  const sortedAgents = useMemo(() => {
    const agents = [...state.installedAgents];
    switch (sortBy) {
      case "name":
        return agents.sort((a, b) => a.metadata.name.localeCompare(b.metadata.name));
      case "cost":
        return agents.sort((a, b) => {
          const aCost = summarizeAgentReportEconomics(b.reports).costMicros;
          const bCost = summarizeAgentReportEconomics(a.reports).costMicros;
          return aCost - bCost;
        });
      case "activity":
        return agents.sort((a, b) => b.reports.length - a.reports.length);
      case "status":
        return agents.sort((a, b) => {
          if (a.running === b.running) {
            return a.metadata.name.localeCompare(b.metadata.name);
          }
          return a.running ? -1 : 1;
        });
      default:
        return agents;
    }
  }, [state.installedAgents, sortBy]);

  const cycleSort = () => {
    const currentIndex = SORT_CYCLE.indexOf(sortBy);
    setSortBy(SORT_CYCLE[(currentIndex + 1) % SORT_CYCLE.length]);
  };

  const removeAgent = async (agentWallet: string) => {
    const target = state.installedAgents.find((agent) => agent.agentWallet === agentWallet);
    if (!target) {
      return;
    }

    setLoading(`remove:${agentWallet}`);
    setError(null);
    try {
      await daemonRemoveAgent(agentWallet);
      await onStateChange({
        ...state,
        installedAgents: state.installedAgents.filter((agent) => agent.agentWallet !== agentWallet),
      });
    } catch (removeError) {
      setError(getErrorMessage(removeError, "Failed to remove local agent."));
    } finally {
      setLoading(null);
    }
  };



  // Auto-deploy linked agent when a valid linked deployment arrives
  const autoDeployTriggeredRef = useRef<string | null>(null);
  useEffect(() => {
    const cid = state.linkedDeployment?.agentCardCid;
    const wallet = state.linkedDeployment?.agentWallet;
    if (!cid || !wallet) {
      return;
    }
    // Skip if we already triggered for this exact CID
    if (autoDeployTriggeredRef.current === cid) {
      return;
    }
    autoDeployTriggeredRef.current = cid;

    const run = async () => {
      setLoading(`deploy:${wallet}`);
      setError(null);
      try {
        const metadata = await fetchAgentMetadata({
          apiUrl: state.settings.apiUrl,
          agentWallet: wallet,
          agentCardCid: cid,
        });
        const lock = await buildAgentLock({
          walletAddress: metadata.walletAddress,
          agentCardUri: metadata.agentCardUri,
          model: metadata.model,
          plugins: metadata.plugins,
          chainId: state.linkedDeployment!.chainId,
          dnaHash: metadata.dnaHash,
        });
        const existing = state.installedAgents.find((agent) => agent.agentWallet === lock.agentWallet);
        const synced = existing
          ? syncInstalledAgent(existing, metadata, lock)
          : await createInstalledAgent({
            metadata,
            lock,
            permissions: getDefaultPermissionPolicy(),
          });
        const daemonStatus = await daemonInstallAgent({
          agentWallet: lock.agentWallet,
          agentCardCid: lock.agentCardCid,
          chainId: lock.chainId,
          modelId: lock.modelId,
          mcpToolsHash: lock.mcpToolsHash,
          dnaHash: lock.dnaHash,
        });
        const deployed = appendAgentReport(
          mergeDaemonStatusIntoInstalledAgent(
            synced,
            daemonStatus,
          ),
          {
            kind: "deployment",
            title: existing ? "Agent refreshed" : "Agent installed",
            summary: existing
              ? `${metadata.name} was refreshed from the marketplace.`
              : `${metadata.name} was installed from the marketplace.`,
            outcome: "success",
          },
        );

        await onStateChange({
          ...state,
          linkedDeployment: null,
          installedAgents: existing
            ? state.installedAgents.map((agent) => (
              agent.agentWallet === deployed.agentWallet ? deployed : agent
            ))
            : [...state.installedAgents, deployed],
        });
      } catch (deployError) {
        setError(getErrorMessage(deployError, "Failed to install agent."));
      } finally {
        setLoading(null);
      }
    };

    void run();
  }, [state.linkedDeployment?.agentCardCid, state.linkedDeployment?.agentWallet]); // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <div className="jarvis-fleet">
      {/* Fleet Header */}
      <div className="jarvis-fleet__header">
        <div className="jarvis-fleet__count">
          <strong>{state.installedAgents.length}</strong> agents · <strong>{runningCount}</strong> running
        </div>
        <div className="jarvis-fleet__spacer" />
        <button className="jarvis-fleet__sort" onClick={cycleSort} type="button" title="Sort agents">
          <Activity size={10} />
          {SORT_LABELS[sortBy]}
        </button>
        <ShellButton tone="secondary" size="sm" onClick={onBrowse}>
          <ExternalLink size={12} />
          Browse
        </ShellButton>
      </div>

      {error ? <ShellNotice tone="error">{error}</ShellNotice> : null}

      {/* Fleet KPI Strip */}
      <div className="jarvis-fleet__kpi">
        <div className="jarvis-stat" data-tone="cyan">
          <span>{state.installedAgents.length}</span>
          <label>Total</label>
        </div>
        <div className="jarvis-stat" data-tone="green">
          <span>{runningCount}</span>
          <label>Running</label>
        </div>
        <div className="jarvis-stat" data-tone="fuchsia">
          <span>{formatMicros(fleetEconomics.totalCost)}</span>
          <label>Cost</label>
        </div>
        <div className="jarvis-stat">
          <span>{formatMicros(fleetEconomics.totalRevenue)}</span>
          <label>Revenue</label>
        </div>
      </div>

      {/* Agent Rows */}
      {sortedAgents.length === 0 ? (
        <div className="jarvis-fleet__empty">
          No agents deployed. Browse the marketplace to install one.
        </div>
      ) : (
        <div className="jarvis-fleet__list">
          {sortedAgents.map((agent) => {
            const econ = summarizeAgentReportEconomics(agent.reports);
            return (
              <div
                key={agent.agentWallet}
                className="jarvis-agent-row"
                onClick={() => onOpenAgent(agent.agentWallet)}
                role="button"
                tabIndex={0}
                onKeyDown={(e) => { if (e.key === "Enter") onOpenAgent(agent.agentWallet); }}
              >
                <span className={`jarvis-agent-row__status${agent.running ? " jarvis-agent-row__status--running" : ""}`} />
                <span className="jarvis-agent-row__avatar">
                  {agent.metadata.name.slice(0, 2).toUpperCase()}
                </span>
                <span className="jarvis-agent-row__name">{agent.metadata.name}</span>
                <div className="jarvis-agent-row__metrics">
                  <span className="jarvis-agent-row__metric" data-tone="cyan">
                    <Cpu size={10} />
                    <span className="jarvis-agent-row__metric-value">{agent.lock.modelId}</span>
                  </span>
                  <span className="jarvis-agent-row__metric" data-tone="fuchsia">
                    <Sparkles size={10} />
                    <span className="jarvis-agent-row__metric-value">{agent.metadata.framework}</span>
                  </span>
                  <span className="jarvis-agent-row__metric">
                    <BadgeDollarSign size={10} />
                    {formatMicros(econ.costMicros)}
                  </span>
                  <span className="jarvis-agent-row__metric" data-tone="green">
                    <BadgeDollarSign size={10} />
                    {formatMicros(econ.netMicros)}
                  </span>
                  <span
                    className={`jarvis-agent-row__hb${agent.heartbeat.lastResult === "ok" ? " jarvis-agent-row__hb--ok" : ""}`}
                    title={`HB: ${agent.heartbeat.lastResult || "—"}`}
                  />
                  <span className="jarvis-agent-row__metric">
                    <Globe size={10} />
                    {agent.network.peersDiscovered}
                  </span>
                </div>
                <div className="jarvis-agent-row__actions" onClick={(e) => e.stopPropagation()}>
                  <ShellButton tone="secondary" size="sm" iconOnly onClick={() => onOpenAgent(agent.agentWallet)} title="Open agent">
                    <Eye size={14} />
                  </ShellButton>
                  <ShellButton
                    tone="danger"
                    size="sm"
                    iconOnly
                    disabled={loading !== null}
                    onClick={() => { void removeAgent(agent.agentWallet); }}
                    title="Remove"
                  >
                    <Trash2 size={14} />
                  </ShellButton>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

export function AgentDetailPage({
  agent,
  state,
  session,
  meshPeers,
  onBack,
  onStateChange,
  onNotify,
}: AgentDetailPageProps) {
  const [skillsTab, setSkillsTab] = useState<SkillsTab>("installed");
  const [logLines, setLogLines] = useState<string[]>([]);
  const [logCursor, setLogCursor] = useState<number | undefined>(undefined);
  const [permissionBusy, setPermissionBusy] = useState<null | keyof AgentPermissionPolicy>(null);
  const [chatMessages, setChatMessages] = useState<LocalChatMessage[]>([]);
  const [chatInput, setChatInput] = useState("");
  const [chatBusy, setChatBusy] = useState(false);
  const [chatError, setChatError] = useState<string | null>(null);
  const economics = useMemo(() => summarizeAgentReportEconomics(agent.reports), [agent.reports]);
  const visiblePeers = useMemo(() => meshPeers.filter((peer) => peer.agentWallet !== agent.agentWallet), [agent.agentWallet, meshPeers]);
  const chatEndRef = useRef<HTMLDivElement | null>(null);
  const identity = state.identity;

  const updateAssistantMessage = useCallback((assistantId: string, content: string) => {
    setChatMessages((current) => current.map((message) => (
      message.id === assistantId
        ? { ...message, content }
        : message
    )));
  }, []);

  useEffect(() => {
    setChatMessages([]);
    setChatInput("");
    setChatError(null);
  }, [agent.agentWallet]);

  useEffect(() => {
    chatEndRef.current?.scrollIntoView({ behavior: "smooth", block: "end" });
  }, [chatBusy, chatMessages]);

  useEffect(() => {
    let cancelled = false;

    async function pollLogs(): Promise<void> {
      try {
        const result = await daemonTailLogs(agent.agentWallet, logCursor);
        if (cancelled) {
          return;
        }
        if (result.lines.length > 0) {
          setLogLines((current) => [...current, ...result.lines].slice(-200));
        }
        setLogCursor(result.cursor);
      } catch {
        // Ignore log polling failures in the UI.
      }
    }

    void pollLogs();
    const timer = window.setInterval(() => {
      void pollLogs();
    }, 4_000);

    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [agent.agentWallet, logCursor]);

  const togglePermission = async (key: keyof AgentPermissionPolicy) => {
    if (permissionBusy) {
      return;
    }
    setPermissionBusy(key);

    try {
      const desiredPermissions = agent.desiredPermissions || agent.permissions;
      const nextValue = nextPermissionDecision(desiredPermissions[key]);
      const nextPermissions: AgentPermissionPolicy = { ...desiredPermissions, [key]: nextValue };
      const daemonStatus = await daemonUpdatePermissions(agent.agentWallet, nextPermissions);
      await onStateChange({
        ...state,
        installedAgents: state.installedAgents.map((item) => (
          item.agentWallet === agent.agentWallet
            ? appendAgentReport(
              mergeDaemonStatusIntoInstalledAgent(item, daemonStatus),
              {
                kind: "permission",
                title: `${key} permission updated`,
                summary: `${key} is now ${nextValue}.`,
                outcome: "info",
              },
            )
            : item
        )),
      });
      onNotify("success", `${key} permission updated`);
    } catch (error) {
      onNotify("error", error instanceof Error ? error.message : `Failed to update ${key}`);
    } finally {
      setPermissionBusy(null);
    }
  };




  const sendChatMessage = async () => {
    const content = chatInput.trim();
    if (!content || chatBusy) {
      return;
    }

    if (!identity?.userAddress || !identity?.chainId) {
      const message = "Connect Local first so this device has a compose key.";
      setChatError(message);
      onNotify("error", message);
      return;
    }

    const userMessage: LocalChatMessage = {
      id: crypto.randomUUID(),
      role: "user",
      content,
      createdAt: Date.now(),
    };
    const assistantId = crypto.randomUUID();
    const priorMessages = chatMessages.map((message) => ({
      role: message.role,
      content: message.content,
    }));

    setChatMessages((current) => [
      ...current,
      userMessage,
      {
        id: assistantId,
        role: "assistant",
        content: "",
        createdAt: Date.now(),
      },
    ]);
    setChatInput("");
    setChatBusy(true);
    setChatError(null);

    try {
      const result = await runLocalAgentConversation({
        agent,
        state,
        history: priorMessages,
        message: content,
      });
      const fullResponse = result.reply.trim() || "No response received.";
      updateAssistantMessage(assistantId, fullResponse);
      const refreshedState = await loadRuntimeState();
      await onStateChange(refreshedState);
    } catch (error) {
      const message = getErrorMessage(error, "Local chat failed");
      setChatError(message);
      setChatMessages((current) => current.map((chatMessage) => (
        chatMessage.id === assistantId
          ? { ...chatMessage, content: `Error: ${message}`, failed: true }
          : chatMessage
      )));
      onNotify("error", message);
    } finally {
      setChatBusy(false);
    }
  };

  const handleChatInputKeyDown = (event: React.KeyboardEvent<HTMLInputElement>) => {
    if (event.key !== "Enter" || event.shiftKey) {
      return;
    }
    event.preventDefault();
    void sendChatMessage();
  };

  // Build agent card data (mirrors web/src/components/agent-card.tsx pattern)
  const runtimeStatus = agent.workerState?.status || (agent.running ? "running" : "stopped");

  const [permissionsModalOpen, setPermissionsModalOpen] = useState(false);
  const [skillsModalOpen, setSkillsModalOpen] = useState(false);

  const feedEndRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    feedEndRef.current?.scrollIntoView({ behavior: "smooth", block: "end" });
  }, [logLines.length, agent.reports.length]);

  const permissionKeys = Object.keys(agent.permissions) as Array<keyof AgentPermissionPolicy>;

  return (
    <section className="jarvis-hud">
      {/* Compact Header Bar */}
      <div className="jarvis-header">
        <ShellButton tone="secondary" size="sm" onClick={onBack}>
          <ArrowLeft size={14} />
          Back
        </ShellButton>
        <div className="jarvis-header__agent">
          <div className="jarvis-header__avatar">
            {agent.metadata.name.slice(0, 2).toUpperCase()}
          </div>
          <h2 className="jarvis-header__name">{agent.metadata.name}</h2>
        </div>
        <div className="jarvis-header__badges">
          <span className="jarvis-badge" data-tone={runtimeStatus === "running" ? "green" : undefined}>
            {runtimeStatus === "running" ? <Play size={10} /> : null}
            {runtimeStatus === "running" ? "Running" : runtimeStatus === "starting" ? "Syncing" : "Ready"}
          </span>
          <span className="jarvis-badge" data-tone={agent.permissions.network === "allow" ? "fuchsia" : undefined}>
            <Globe size={10} />
            {agent.permissions.network === "allow" ? "Network" : "Offline"}
          </span>
        </div>
      </div>

      {/* Main Grid */}
      <div className="jarvis-grid">
        {/* ── Identity Panel (top-left) ── */}
        <div className="jarvis-panel jarvis-identity">
          <div className="jarvis-panel__header">
            <ShieldCheck size={12} />
            <span>Identity</span>
          </div>
          <div className="jarvis-identity__body">
            <p className="jarvis-identity__desc">{agent.metadata.description}</p>
            <div className="jarvis-field">
              <span className="jarvis-field__label">Model</span>
              <span className="jarvis-field__value" data-tone="cyan">
                <Cpu size={11} /> {agent.lock.modelId}
              </span>
            </div>
            <div className="jarvis-field">
              <span className="jarvis-field__label">Framework</span>
              <span className="jarvis-field__value" data-tone="fuchsia">
                <Sparkles size={11} /> {agent.metadata.framework}
              </span>
            </div>
            <div className="jarvis-field">
              <span className="jarvis-field__label">Wallet</span>
              <span className="jarvis-field__value">{shortWallet(agent.agentWallet)}</span>
            </div>
            <div className="jarvis-field">
              <span className="jarvis-field__label">CID</span>
              <span className="jarvis-field__value">{agent.lock.agentCardCid.slice(0, 16)}…</span>
            </div>
            <div className="jarvis-field">
              <span className="jarvis-field__label">Network</span>
              <span className="jarvis-field__value">
                {agent.permissions.network === "allow" ? agent.network.status : "denied"}
              </span>
            </div>
          </div>
          {listPluginIds(agent.metadata.plugins).length > 0 ? (
            <div className="jarvis-identity__plugins">
              {listPluginIds(agent.metadata.plugins).map((name) => (
                <span key={name} className="jarvis-plugin-tag">{name}</span>
              ))}
            </div>
          ) : null}
        </div>

        {/* ── Chat Panel (top-center) ── */}
        <div className="jarvis-panel jarvis-chat">
          <div className="jarvis-panel__header">
            <MessageSquare size={12} />
            <span>Chat</span>
            <ShellButton
              tone="secondary"
              size="sm"
              onClick={() => { setChatMessages([]); setChatError(null); }}
              disabled={chatBusy || chatMessages.length === 0}
            >
              Clear
            </ShellButton>
          </div>
          <div className="jarvis-chat__body">
            {chatError ? <ShellNotice tone="error">{chatError}</ShellNotice> : null}
            <div className="jarvis-chat__thread">
              {chatMessages.length === 0 ? (
                <div className="jarvis-chat__empty">Hello, friend.</div>
              ) : (
                chatMessages.map((message) => (
                  <article
                    key={message.id}
                    className={`jarvis-chat__msg jarvis-chat__msg--${message.role}${message.failed ? " jarvis-chat__msg--failed" : ""}`}
                  >
                    <div className="jarvis-chat__msg-head">
                      <strong>{message.role === "user" ? "You" : agent.metadata.name}</strong>
                      <span>{formatChatTimestamp(message.createdAt)}</span>
                    </div>
                    <p>{message.content || (chatBusy && message.role === "assistant" ? "Thinking..." : " ")}</p>
                  </article>
                ))
              )}
              <div ref={chatEndRef} />
            </div>
            <div className="jarvis-chat__composer">
              <ShellInput
                value={chatInput}
                onChange={(event) => setChatInput(event.target.value)}
                onKeyDown={handleChatInputKeyDown}
                placeholder="Message your local agent..."
                disabled={chatBusy}
              />
              <ShellButton
                tone="primary"
                onClick={() => void sendChatMessage()}
                disabled={chatBusy || !chatInput.trim()}
              >
                {chatBusy ? <Loader2 size={14} className="spinner" /> : null}
                Send
              </ShellButton>
            </div>
          </div>
        </div>

        {/* ── Live Feed Panel (top-right) ── */}
        <div className="jarvis-panel jarvis-feed">
          <div className="jarvis-panel__header">
            <Activity size={12} />
            <span>Live Feed</span>
          </div>
          <div className="jarvis-feed__body">
            {agent.reports.length > 0 ? (
              <>
                <div className="jarvis-feed__section-label">Reports</div>
                {agent.reports.slice().reverse().map((report) => (
                  <article key={report.id} className="jarvis-feed__entry" data-outcome={report.outcome}>
                    <div className="jarvis-feed__entry-head">
                      <span className="jarvis-feed__entry-kind">{report.kind}</span>
                      <span className="jarvis-feed__entry-time">{new Date(report.createdAt).toLocaleTimeString()}</span>
                    </div>
                    <div className="jarvis-feed__entry-text">{report.title}: {report.summary}</div>
                  </article>
                ))}
              </>
            ) : null}
            {logLines.length > 0 ? (
              <>
                <div className="jarvis-feed__section-label">Runtime Log</div>
                {logLines.map((line, index) => (
                  <div key={`log-${index}-${line.slice(0, 12)}`} className="jarvis-feed__log-line">{line}</div>
                ))}
              </>
            ) : null}
            {agent.reports.length === 0 && logLines.length === 0 ? (
              <div className="jarvis-chat__empty">No activity yet.</div>
            ) : null}
            <div ref={feedEndRef} />
          </div>
        </div>

        {/* ── KPI: Mesh Peers (bottom-left) ── */}
        <div className="jarvis-panel jarvis-kpi jarvis-kpi--mesh">
          <div className="jarvis-panel__header">
            <Globe size={12} />
            <span>Mesh</span>
          </div>
          <div className="jarvis-kpi__body">
            <div className="jarvis-kpi__stats">
              <div className="jarvis-stat" data-tone="cyan">
                <span>{agent.network.peersDiscovered}</span>
                <label>Peers</label>
              </div>
              <div className="jarvis-stat" data-tone="green">
                <span>{agent.network.recentPings.length}</span>
                <label>Signals</label>
              </div>
              <div className="jarvis-stat">
                <span>{visiblePeers.length}</span>
                <label>Visible</label>
              </div>
            </div>
            <div className="jarvis-peer-feed">
              {agent.network.recentPings.slice(0, 5).map((peer) => (
                <div key={`${peer.peerId}-${peer.lastSeenAt}`} className="jarvis-peer">
                  <strong>{peer.card?.name || peer.peerId.slice(0, 12)}</strong>
                  <span>{new Date(peer.lastSeenAt).toLocaleTimeString()}</span>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* ── KPI: Economics + Heartbeat (bottom-center) ── */}
        <div className="jarvis-panel jarvis-kpi jarvis-kpi--economics">
          <div className="jarvis-panel__header">
            <BadgeDollarSign size={12} />
            <span>Economics</span>
          </div>
          <div className="jarvis-kpi__body">
            <div className="jarvis-kpi__stats">
              <div className="jarvis-stat" data-tone="green">
                <span>{formatMicros(economics.revenueMicros)}</span>
                <label>Revenue</label>
              </div>
              <div className="jarvis-stat" data-tone="fuchsia">
                <span>{formatMicros(economics.costMicros)}</span>
                <label>Cost</label>
              </div>
              <div className="jarvis-stat" data-tone="cyan">
                <span>{formatMicros(economics.netMicros)}</span>
                <label>Net</label>
              </div>
            </div>
            <div className="jarvis-kpi__row">
              <div className="jarvis-heartbeat">
                <Clock3 size={10} />
                <span>HB</span>
                <strong className={`jarvis-heartbeat__indicator${agent.heartbeat.lastResult === "ok" ? " jarvis-heartbeat__indicator--ok" : ""}`}>
                  {agent.heartbeat.lastResult || "—"}
                </strong>
                <span className="jarvis-heartbeat__time">
                  {agent.network.lastHeartbeatAt ? new Date(agent.network.lastHeartbeatAt).toLocaleTimeString() : "—"}
                </span>
              </div>
              <button className="jarvis-skills-count" onClick={() => setSkillsModalOpen(true)} type="button">
                <Sparkles size={10} />
                <span>Skills</span>
                <strong>{state.installedSkills.length}</strong>
              </button>
            </div>
          </div>
        </div>

        {/* ── KPI: Permissions (bottom-right) ── */}
        <button
          className="jarvis-panel jarvis-kpi jarvis-kpi--permissions"
          onClick={() => setPermissionsModalOpen(true)}
          type="button"
        >
          <div className="jarvis-panel__header">
            <Shield size={12} />
            <span>Permissions</span>
          </div>
          <div className="jarvis-perm-grid">
            {permissionKeys.map((key) => (
              <div key={key} className={`jarvis-perm${agent.permissions[key] === "allow" ? " jarvis-perm--on" : ""}`}>
                <span className="jarvis-perm__dot" />
                <span className="jarvis-perm__label">{key}</span>
              </div>
            ))}
          </div>
        </button>
      </div>

      {/* ── Permissions Modal ── */}
      {permissionsModalOpen ? (
        <ShellPanel className="jarvis-modal-skills" style={{ position: "fixed", inset: 0, zIndex: 60, background: "hsl(var(--background) / 0.96)", backdropFilter: "blur(12px)", padding: 24, overflow: "auto" }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
            <h3 style={{ fontFamily: "var(--font-display, Orbitron), sans-serif", letterSpacing: "0.06em" }}>Permissions</h3>
            <ShellButton tone="secondary" size="sm" onClick={() => setPermissionsModalOpen(false)}>Close</ShellButton>
          </div>
          <PermissionsPanel
            permissions={agent.desiredPermissions || agent.permissions}
            agentWallet={agent.agentWallet}
            permissionBusy={permissionBusy}
            onToggle={togglePermission}
          />
        </ShellPanel>
      ) : null}

      {/* ── Skills Modal ── */}
      {skillsModalOpen ? (
        <ShellPanel className="jarvis-modal-skills" style={{ position: "fixed", inset: 0, zIndex: 60, background: "hsl(var(--background) / 0.96)", backdropFilter: "blur(12px)", padding: 24, overflow: "auto" }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
            <h3 style={{ fontFamily: "var(--font-display, Orbitron), sans-serif", letterSpacing: "0.06em" }}>Skills</h3>
            <ShellButton tone="secondary" size="sm" onClick={() => setSkillsModalOpen(false)}>Close</ShellButton>
          </div>
          <div className="jarvis-modal-skills__tabs">
            <ShellButton tone={skillsTab === "installed" ? "primary" : "secondary"} onClick={() => setSkillsTab("installed")}>Installed</ShellButton>
            <ShellButton tone={skillsTab === "browse" ? "primary" : "secondary"} onClick={() => setSkillsTab("browse")}>Browse</ShellButton>
          </div>
          {skillsTab === "installed" ? (
            <SkillsManager state={state} onStateChange={onStateChange} />
          ) : (
            <SkillsMarketplace state={state} onStateChange={onStateChange} />
          )}
        </ShellPanel>
      ) : null}
    </section>
  );
}
