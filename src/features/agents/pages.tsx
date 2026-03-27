import { useEffect, useMemo, useRef, useState } from "react";
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
import { ComposeAgentCard, type ComposeAgentBadge, type ComposeAgentMetric, type ComposeAgentMetaRow, type ComposeAgentTag } from "@compose-market/theme/agents";
import { ShellButton, ShellEmptyState, ShellInput, ShellNotice, ShellPageHeader, ShellPanel, ShellTab, ShellTabStrip } from "@compose-market/theme/shell";
import {
  daemonRemoveAgent,
  daemonInstallAgent,
  mergeDaemonStatusIntoInstalledAgent,
  daemonStatusToWorkerState,
  daemonTailLogs,
  daemonUpdatePermissions,
} from "../../lib/daemon";
import { fetchAgentMetadata } from "../../lib/api";
import {
  queryOsPermissions,
  reconcileStateWithOsPermissions,
} from "../../lib/permissions";
import { getDefaultPermissionPolicy } from "../../lib/storage";
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

type DetailTab = "chat" | "permissions" | "skills" | "history" | "mesh";
type SkillsTab = "installed" | "browse";

const DETAIL_TABS: Array<{ id: DetailTab; label: string; icon: typeof Shield }> = [
  { id: "chat", label: "Chat", icon: MessageSquare },
  { id: "permissions", label: "Permissions", icon: Shield },
  { id: "skills", label: "Skills", icon: Sparkles },
  { id: "history", label: "Reports / History", icon: FileText },
  { id: "mesh", label: "Peer / Network", icon: Globe },
];

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

async function* parseSseDataBlocks(reader: ReadableStreamDefaultReader<Uint8Array>): AsyncGenerator<string, void, void> {
  const decoder = new TextDecoder();
  let buffer = "";

  while (true) {
    const { value, done } = await reader.read();
    if (done) {
      break;
    }

    buffer += decoder.decode(value, { stream: true });

    while (true) {
      const boundary = buffer.indexOf("\n\n");
      if (boundary === -1) {
        break;
      }

      const rawBlock = buffer.slice(0, boundary);
      buffer = buffer.slice(boundary + 2);

      const data = rawBlock
        .split(/\r?\n/)
        .filter((line) => line.startsWith("data:"))
        .map((line) => line.slice(5).trimStart())
        .join("\n")
        .trim();

      if (data) {
        yield data;
      }
    }
  }

  const trailing = buffer.trim();
  if (!trailing) {
    return;
  }

  const data = trailing
    .split(/\r?\n/)
    .filter((line) => line.startsWith("data:"))
    .map((line) => line.slice(5).trimStart())
    .join("\n")
    .trim();

  if (data) {
    yield data;
  }
}

function extractChatStreamText(payload: unknown): string | null {
  if (!payload || typeof payload !== "object") {
    return null;
  }

  const record = payload as Record<string, unknown>;
  const delta = Array.isArray(record.choices)
    ? record.choices[0] as { delta?: { content?: string } } | undefined
    : undefined;

  if (typeof delta?.delta?.content === "string" && delta.delta.content.length > 0) {
    return delta.delta.content;
  }

  if (typeof record.content === "string" && record.content.length > 0) {
    return record.content;
  }

  if (typeof record.text === "string" && record.text.length > 0) {
    return record.text;
  }

  return null;
}

function formatChatTimestamp(value: number): string {
  return new Date(value).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
  });
}

function normalizeUrlBase(value: string): string {
  return value.trim().replace(/\/+$/, "");
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

export function AgentManagerPage({
  state,
  onStateChange,
  onOpenAgent,
  onBrowse,
}: AgentManagerPageProps) {
  const [loading, setLoading] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const runningCount = useMemo(() => state.installedAgents.filter((agent) => agent.running).length, [state.installedAgents]);

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
          runtimeUrl: state.settings.runtimeUrl,
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
          : createInstalledAgent({
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
            {
              ...synced,
              runtimeId: daemonStatus.runtimeId || synced.runtimeId,
              workerState: daemonStatusToWorkerState(daemonStatus),
            },
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
    <div className="agent-manager">
      <ShellPageHeader
        eyebrow="My Agents"
        title="Local Agents"
        subtitle={`${state.installedAgents.length} deployed · ${runningCount} running`}
        actions={
          <ShellButton tone="secondary" onClick={onBrowse}>
            <ExternalLink size={14} />
            Browse
          </ShellButton>
        }
      />
      {error ? <ShellNotice tone="error" className="notification">{error}</ShellNotice> : null}

      {state.installedAgents.length === 0 ? (
        <ShellEmptyState
          title="No local agents deployed"
          description="Link Local from the web app, then deploy the linked agent into this runtime."
        />
      ) : (
        <div className="cm-card-grid">
          {state.installedAgents.map((agent) => {
            const badges: ComposeAgentBadge[] = [
              {
                label: agent.running ? "Running" : "Installed",
                tone: agent.running ? "green" : "cyan",
                icon: agent.running ? <Play size={12} /> : <ShieldCheck size={12} />,
              },
              {
                label: agent.permissions.network === "allow" ? "Network Allowed" : "Network Denied",
                tone: agent.permissions.network === "allow" ? "fuchsia" : "neutral",
                icon: <Globe size={12} />,
              },
            ];
            const metrics: ComposeAgentMetric[] = [
              {
                label: "Model",
                value: agent.lock.modelId,
                icon: <Cpu size={16} />,
                tone: "cyan",
              },
              {
                label: "Framework",
                value: agent.metadata.framework,
                icon: <Sparkles size={16} />,
                tone: "fuchsia",
              },
              {
                label: "Reports",
                value: agent.reports.length,
                icon: <FileText size={16} />,
                tone: "neutral",
              },
              {
                label: "Peers",
                value: agent.network.peersDiscovered,
                icon: <Activity size={16} />,
                tone: "green",
              },
            ];
            const metaRows: ComposeAgentMetaRow[] = [
              {
                label: "Wallet",
                value: shortWallet(agent.agentWallet),
              },
              {
                label: "CID",
                value: `${agent.lock.agentCardCid.slice(0, 12)}...`,
              },
              {
                label: "Network",
                value: agent.permissions.network === "allow" ? agent.network.status : "denied",
              },
            ];
            const tags: ComposeAgentTag[] = listPluginIds(agent.metadata.plugins).map((name) => ({
              label: name,
            }));

            return (
              <ComposeAgentCard
                key={agent.agentWallet}
                status={agent.running ? "running" : "default"}
                avatarAlt={agent.metadata.name}
                avatarFallback={agent.metadata.name.slice(0, 2).toUpperCase()}
                title={agent.metadata.name}
                description={agent.metadata.description}
                badges={badges}
                metrics={metrics}
                focusLabel="Immutable Lock"
                focusValue={agent.lock.agentCardCid}
                focusIcon={<ShieldCheck size={18} />}
                tagsTitle={`Plugins (${tags.length})`}
                tags={tags}
                metaRows={metaRows}
                footer={(
                  <ShellButton tone="ghost" size="sm" onClick={() => onOpenAgent(agent.agentWallet)}>
                    Open agent settings
                  </ShellButton>
                )}
                actions={(
                  <div className="cm-agent-card__action-stack">
                    <ShellButton
                      tone="secondary"
                      size="sm"
                      iconOnly
                      disabled
                      title={agent.running ? "Running locally" : "Syncing local runtime"}
                    >
                      {agent.running ? <ShieldCheck size={16} /> : <Loader2 size={16} className="spinner" />}
                    </ShellButton>
                    <ShellButton tone="secondary" size="sm" iconOnly onClick={() => onOpenAgent(agent.agentWallet)} title="Open agent settings">
                      <Eye size={16} />
                    </ShellButton>

                    <ShellButton
                      tone="danger"
                      size="sm"
                      iconOnly
                      disabled={loading !== null}
                      onClick={() => {
                        void removeAgent(agent.agentWallet);
                      }}
                      title="Remove local deployment"
                    >
                      <Trash2 size={16} />
                    </ShellButton>
                  </div>
                )}
              />
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
  const [activeTab, setActiveTab] = useState<DetailTab>("chat");
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
  const chatThreadIdRef = useRef<string>(`mesh-local-${agent.agentWallet}-${crypto.randomUUID()}`);
  const chatEndRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    chatThreadIdRef.current = `mesh-local-${agent.agentWallet}-${crypto.randomUUID()}`;
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
      const nextValue = nextPermissionDecision(agent.permissions[key]);
      const nextPermissions: AgentPermissionPolicy = { ...agent.permissions, [key]: nextValue };
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
                summary: `${key} is now ${daemonStatus.permissions[key]}.`,
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

  const refreshOsPermissions = async () => {
    const osStatus = await queryOsPermissions();

    await onStateChange(reconcileStateWithOsPermissions(state, osStatus));
    onNotify("success", "Local OS permissions refreshed");
  };

  const [mobileCardOpen, setMobileCardOpen] = useState(false);

  const sendChatMessage = async () => {
    const content = chatInput.trim();
    if (!content || chatBusy) {
      return;
    }

    const identity = state.identity;
    if (!identity) {
      const message = "Connect Local first so this device has a user identity and compose key.";
      setChatError(message);
      onNotify("error", message);
      return;
    }

    if (!session.active || !identity.composeKeyToken || !identity.budget) {
      const message = "An active session is required. Start one from the Local header, then try again.";
      setChatError(message);
      onNotify("error", message);
      return;
    }

    const runtimeUrl = normalizeUrlBase(state.settings.runtimeUrl || "");
    if (!runtimeUrl) {
      const message = "Local runtime URL is not configured.";
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
      const composeRunId = crypto.randomUUID();
      const response = await fetch(`${runtimeUrl}/agent/${agent.agentWallet}/stream`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${identity.composeKeyToken}`,
          "x-chain-id": String(identity.chainId),
          "x-compose-run-id": composeRunId,
          "x-session-active": "true",
          "x-session-user-address": identity.userAddress,
          "x-session-budget-remaining": identity.budget,
        },
        body: JSON.stringify({
          message: content,
          threadId: chatThreadIdRef.current,
          composeRunId,
          userAddress: identity.userAddress,
        }),
      });

      if (!response.ok) {
        const payload = await response.json().catch(() => null) as
          | { error?: string; code?: string }
          | null;
        throw new Error(payload?.error || payload?.code || `Chat failed with HTTP ${response.status}`);
      }

      const reader = response.body?.getReader();
      if (!reader) {
        throw new Error("Local runtime returned no response body.");
      }

      let fullResponse = "";

      for await (const block of parseSseDataBlocks(reader)) {
        if (block === "[DONE]") {
          continue;
        }

        let payload: unknown = null;
        try {
          payload = JSON.parse(block);
        } catch {
          fullResponse += block;
          setChatMessages((current) => current.map((message) => (
            message.id === assistantId
              ? { ...message, content: fullResponse }
              : message
          )));
          continue;
        }

        if (
          payload
          && typeof payload === "object"
          && (payload as Record<string, unknown>).type === "error"
        ) {
          const message = typeof (payload as Record<string, unknown>).error === "string"
            ? (payload as Record<string, unknown>).error as string
            : typeof (payload as Record<string, unknown>).content === "string"
              ? (payload as Record<string, unknown>).content as string
              : "Local agent stream failed";
          throw new Error(message);
        }

        if (
          payload
          && typeof payload === "object"
          && (payload as Record<string, unknown>).type === "done"
        ) {
          break;
        }

        const chunk = extractChatStreamText(payload);
        if (!chunk) {
          continue;
        }

        fullResponse += chunk;
        setChatMessages((current) => current.map((message) => (
          message.id === assistantId
            ? { ...message, content: fullResponse }
            : message
        )));
      }

      if (!fullResponse.trim()) {
        setChatMessages((current) => current.map((message) => (
          message.id === assistantId
            ? { ...message, content: "No response received." }
            : message
        )));
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : "Local chat failed";
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
  const runtimeStatus = agent.workerState?.status || (agent.running ? "running" : "starting");
  const agentBadges: ComposeAgentBadge[] = [
    {
      label: runtimeStatus === "running" ? "Running" : "Syncing",
      tone: runtimeStatus === "running" ? "green" : "neutral",
    },
    { label: agent.permissions.network === "allow" ? "Network On" : "Network Off", tone: agent.permissions.network === "allow" ? "fuchsia" : "neutral" },
  ];
  const agentMetrics: ComposeAgentMetric[] = [
    { label: "Model", value: agent.lock.modelId, icon: <Cpu size={16} />, tone: "cyan" },
    { label: "Framework", value: agent.metadata.framework, icon: <Sparkles size={16} />, tone: "fuchsia" },
    { label: "Reports", value: agent.reports.length, icon: <FileText size={16} />, tone: "neutral" },
    { label: "Peers", value: agent.network.peersDiscovered, icon: <Activity size={16} />, tone: "green" },
  ];
  const agentTags: ComposeAgentTag[] = listPluginIds(agent.metadata.plugins).map((name) => ({ label: name }));
  const agentMetaRows: ComposeAgentMetaRow[] = [
    { label: "Wallet", value: shortWallet(agent.agentWallet) },
    { label: "CID", value: `${agent.lock.agentCardCid.slice(0, 12)}...` },
    { label: "Network", value: agent.permissions.network === "allow" ? agent.network.status : "denied" },
  ];

  return (
    <section className="agent-detail-page">
      {/* Compact Header */}
      <div className="agent-detail-header">
        <ShellButton tone="secondary" className="detail-back-btn" onClick={onBack}>
          <ArrowLeft size={14} />
          Back
        </ShellButton>
        <ShellButton
          tone="ghost"
          size="sm"
          className="agent-detail-card-toggle"
          onClick={() => setMobileCardOpen(!mobileCardOpen)}
        >
          <Eye size={14} />
          Agent Info
        </ShellButton>
      </div>

      {/* Main Layout: Tabs (2/3) + Card (1/3) — mirrors web/src/pages/agent.tsx */}
      <div className="agent-detail-layout">
        {/* Left: Tab content area */}
        <div className="agent-detail-tabs-area">
          <ShellTabStrip className="detail-tab-row">
            {DETAIL_TABS.map((tab) => (
              <ShellTab
                key={tab.id}
                className={`detail-tab-btn ${activeTab === tab.id ? "active" : ""}`}
                active={activeTab === tab.id}
                onClick={() => setActiveTab(tab.id)}
              >
                <tab.icon size={14} />
                {tab.label}
              </ShellTab>
            ))}
          </ShellTabStrip>

          {activeTab === "chat" ? (
            <ShellPanel className="detail-panel">
              <div className="detail-panel-header">
                <h3>Local Chat</h3>
                <ShellButton
                  tone="secondary"
                  size="sm"
                  onClick={() => {
                    setChatMessages([]);
                    setChatError(null);
                    chatThreadIdRef.current = `mesh-local-${agent.agentWallet}-${crypto.randomUUID()}`;
                  }}
                  disabled={chatBusy || chatMessages.length === 0}
                >
                  Clear
                </ShellButton>
              </div>

              {!session.active ? (
                <ShellNotice tone="warning">
                  Start a session from the Local header to send paid/local chat requests with the same compose-key session flow used by web.
                </ShellNotice>
              ) : null}

              {chatError ? (
                <ShellNotice tone="error">{chatError}</ShellNotice>
              ) : null}

              <div className="local-chat-thread">
                {chatMessages.length === 0 ? (
                  <div className="empty-inline">
                    Hello, friend.
                  </div>
                ) : (
                  chatMessages.map((message) => (
                    <article
                      key={message.id}
                      className={`local-chat-message local-chat-message--${message.role}${message.failed ? " local-chat-message--failed" : ""}`}
                    >
                      <div className="local-chat-message-head">
                        <strong>{message.role === "user" ? "You" : agent.metadata.name}</strong>
                        <span>{formatChatTimestamp(message.createdAt)}</span>
                      </div>
                      <p>{message.content || (chatBusy && message.role === "assistant" ? "Thinking..." : " ")}</p>
                    </article>
                  ))
                )}
                <div ref={chatEndRef} />
              </div>

              <div className="local-chat-composer">
                <ShellInput
                  value={chatInput}
                  onChange={(event) => setChatInput(event.target.value)}
                  onKeyDown={handleChatInputKeyDown}
                  placeholder="Message your local agent..."
                  disabled={chatBusy || !session.active}
                />
                <ShellButton
                  tone="primary"
                  onClick={() => void sendChatMessage()}
                  disabled={chatBusy || !chatInput.trim() || !session.active}
                >
                  {chatBusy ? <Loader2 size={14} className="spinner" /> : null}
                  Send
                </ShellButton>
              </div>
            </ShellPanel>
          ) : null}

          {activeTab === "permissions" ? (
            <PermissionsPanel
              permissions={agent.permissions}
              osPermissions={state.osPermissions}
              agentWallet={agent.agentWallet}
              permissionBusy={permissionBusy}
              onToggle={togglePermission}
              onRefresh={() => void refreshOsPermissions()}
            />
          ) : null}

          {activeTab === "skills" ? (
            <ShellPanel className="detail-panel">
              <div className="detail-panel-header">
                <h3>Skills</h3>
                <div className="detail-inline-tabs">
                  <ShellButton tone={skillsTab === "installed" ? "primary" : "secondary"} className={skillsTab === "installed" ? "active-inline-tab" : ""} onClick={() => setSkillsTab("installed")}>Installed</ShellButton>
                  <ShellButton tone={skillsTab === "browse" ? "primary" : "secondary"} className={skillsTab === "browse" ? "active-inline-tab" : ""} onClick={() => setSkillsTab("browse")}>Browse</ShellButton>
                </div>
              </div>
              {skillsTab === "installed" ? (
                <SkillsManager state={state} onStateChange={onStateChange} />
              ) : (
                <SkillsMarketplace state={state} onStateChange={onStateChange} />
              )}
            </ShellPanel>
          ) : null}

          {activeTab === "history" ? (
            <div className="detail-grid">
              <ShellPanel className="detail-panel">
                <div className="detail-panel-header">
                  <h3>Local Reports</h3>
                </div>
                <div className="report-list">
                  {agent.reports.length === 0 ? (
                    <div className="empty-inline">No local reports yet.</div>
                  ) : (
                    agent.reports.map((report) => (
                      <article key={report.id} className={`report-card report-${report.outcome}`}>
                        <div className="report-card-head">
                          <strong>{report.title}</strong>
                          <span>{new Date(report.createdAt).toLocaleString()}</span>
                        </div>
                        <p>{report.summary}</p>
                        {report.details ? <pre>{report.details}</pre> : null}
                      </article>
                    ))
                  )}
                </div>
              </ShellPanel>

              <ShellPanel className="detail-panel">
                <div className="detail-panel-header">
                  <h3>Runtime Log</h3>
                </div>
                <div className="log-console">
                  {logLines.length === 0 ? (
                    <div className="empty-inline">No runtime log lines yet.</div>
                  ) : (
                    logLines.map((line, index) => (
                      <div key={`${index}-${line.slice(0, 12)}`} className="log-line">{line}</div>
                    ))
                  )}
                </div>
              </ShellPanel>
            </div>
          ) : null}

          {activeTab === "mesh" ? (
            <div className="detail-grid">
              <ShellPanel className="detail-panel">
                <div className="detail-panel-header">
                  <h3>Mesh Signals</h3>
                </div>
                <div className="mesh-kpi-row">
                  <div className="detail-stat-card">
                    <Activity size={14} />
                    <span>Pings</span>
                    <strong>{agent.network.recentPings.length}</strong>
                  </div>
                  <div className="detail-stat-card">
                    <Clock3 size={14} />
                    <span>Last heartbeat</span>
                    <strong>{agent.network.lastHeartbeatAt ? new Date(agent.network.lastHeartbeatAt).toLocaleTimeString() : "Never"}</strong>
                  </div>
                  <div className="detail-stat-card">
                    <BadgeDollarSign size={14} />
                    <span>Net</span>
                    <strong>{formatMicros(economics.netMicros)}</strong>
                  </div>
                </div>
                <div className="report-list">
                  {agent.network.recentPings.length === 0 ? (
                    <div className="empty-inline">No peer signals received yet.</div>
                  ) : (
                    agent.network.recentPings.map((peer) => (
                      <article key={`${peer.peerId}-${peer.lastSeenAt}`} className="peer-signal-card">
                        <div className="peer-signal-head">
                          <strong>{peer.card?.name || peer.peerId}</strong>
                          <span>{new Date(peer.lastSeenAt).toLocaleTimeString()}</span>
                        </div>
                        <p>{peer.card?.statusLine || peer.card?.headline || peer.agentWallet || "Unknown peer"}</p>
                        <div className="agent-plugins">
                          {peer.caps.map((cap) => <span key={`${peer.peerId}-${cap}`} className="plugin-tag">{cap}</span>)}
                        </div>
                      </article>
                    ))
                  )}
                </div>
              </ShellPanel>

              <ShellPanel className="detail-panel">
                <div className="detail-panel-header">
                  <h3>Interactions &amp; Economics</h3>
                </div>
                <div className="detail-stat-stack">
                  <div className="detail-stat-card">
                    <span>Revenue</span>
                    <strong>{formatMicros(economics.revenueMicros)}</strong>
                  </div>
                  <div className="detail-stat-card">
                    <span>Costs</span>
                    <strong>{formatMicros(economics.costMicros)}</strong>
                  </div>
                  <div className="detail-stat-card">
                    <span>Visible peers</span>
                    <strong>{visiblePeers.length}</strong>
                  </div>
                </div>
                <div className="report-list">
                  {agent.network.interactions.length === 0 ? (
                    <div className="empty-inline">No mesh interactions recorded yet.</div>
                  ) : (
                    agent.network.interactions.map((interaction) => (
                      <article key={interaction.id} className="report-card report-info">
                        <div className="report-card-head">
                          <strong>{interaction.kind}</strong>
                          <span>{new Date(interaction.createdAt).toLocaleString()}</span>
                        </div>
                        <p>{interaction.summary}</p>
                      </article>
                    ))
                  )}
                </div>
              </ShellPanel>
            </div>
          ) : null}
        </div>

        {/* Right: Foldable Agent Card (1/3 on desktop, hidden on mobile) */}
        <div className={`agent-detail-card-col ${mobileCardOpen ? "mobile-open" : ""}`}>
          <ComposeAgentCard
            interactive
            status={agent.running ? "running" : "default"}
            avatarAlt={agent.metadata.name}
            avatarFallback={agent.metadata.name.slice(0, 2).toUpperCase()}
            title={agent.metadata.name}
            description={agent.metadata.description}
            badges={agentBadges}
            metrics={agentMetrics}
            focusLabel="Immutable Lock"
            focusValue={agent.lock.agentCardCid}
            focusIcon={<ShieldCheck size={18} />}
            tagsTitle={`Plugins (${agentTags.length})`}
            tags={agentTags}
            metaRows={agentMetaRows}
            footer={(
              <div className="cm-agent-card__footer-stack">
                <div className="cm-agent-card__endpoint">
                  <div className="cm-agent-card__endpoint-label">Agent Wallet</div>
                  <div className="cm-agent-card__endpoint-row">
                    <code className="cm-agent-card__endpoint-code">{agent.agentWallet}</code>
                  </div>
                </div>
                <div className="cm-agent-card__creator">
                  <div className="cm-agent-card__creator-label">Net Revenue</div>
                  <div className="cm-agent-card__creator-value">{formatMicros(economics.netMicros)}</div>
                </div>
              </div>
            )}
          />
          {/* Close button for mobile overlay */}
          {mobileCardOpen ? (
            <ShellButton tone="secondary" className="agent-detail-card-close" onClick={() => setMobileCardOpen(false)}>
              Close
            </ShellButton>
          ) : null}
        </div>
      </div>
    </section>
  );
}
