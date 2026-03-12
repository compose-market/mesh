import { useEffect, useMemo, useState } from "react";
import {
  Activity,
  ArrowLeft,
  BadgeDollarSign,
  Clock3,
  Cpu,
  Eye,
  FileText,
  Globe,
  Loader2,
  Play,
  Shield,
  ShieldCheck,
  Sparkles,
  Square,
  Trash2,
} from "lucide-react";
import { ComposeAgentCard, type ComposeAgentBadge, type ComposeAgentMetric, type ComposeAgentMetaRow, type ComposeAgentTag } from "@compose-market/theme/agents";
import { ShellButton, ShellEmptyState, ShellNotice, ShellPageHeader, ShellPanel, ShellTab, ShellTabStrip } from "@compose-market/theme/shell";
import { fetchAgentMetadata } from "../../lib/api";
import {
  daemonInstallAgent,
  daemonMeshSet,
  daemonStartAgent,
  daemonStatusToWorkerState,
  daemonStopAgent,
  daemonTailLogs,
  daemonUpdatePermissions,
} from "../../lib/daemon";
import { queryMediaPermission, requestMediaPermission } from "../../lib/permissions";
import { ensureAgentWorkspace, permissionAllows } from "../../lib/storage";
import type { AgentPermissionPolicy, DesktopRuntimeState, InstalledAgent, MeshPeerSignal, SessionState } from "../../lib/types";
import { SkillsManager } from "../../components/skills-manager";
import { SkillsMarketplace } from "../../components/skills-store";
import {
  agentLocksMatch,
  appendAgentReport,
  buildAgentLock,
  createInstalledAgent,
  listPluginIds,
  summarizeAgentReportEconomics,
  syncInstalledAgent,
  validateAgentLock,
} from "./model";

type DetailTab = "permissions" | "skills" | "history" | "mesh";
type SkillsTab = "installed" | "browse";

const DETAIL_TABS: Array<{ id: DetailTab; label: string; icon: typeof Shield }> = [
  { id: "permissions", label: "Permissions", icon: Shield },
  { id: "skills", label: "Skills", icon: Sparkles },
  { id: "history", label: "Reports / History", icon: FileText },
  { id: "mesh", label: "Peer / Mesh", icon: Globe },
];
const PERMISSION_ORDER: Array<AgentPermissionPolicy[keyof AgentPermissionPolicy]> = ["deny", "ask", "allow"];

interface AgentManagerPageProps {
  state: DesktopRuntimeState;
  session: SessionState;
  onStateChange: (state: DesktopRuntimeState) => Promise<void>;
  onActivateAgent: (agentWallet: string | null) => void;
  onOpenAgent: (agentWallet: string) => void;
  onBrowseMarket: () => void;
}

interface AgentDetailPageProps {
  agent: InstalledAgent;
  state: DesktopRuntimeState;
  meshPeers: MeshPeerSignal[];
  onBack: () => void;
  onStateChange: (next: DesktopRuntimeState) => Promise<void>;
  onNotify: (type: "success" | "error", message: string) => void;
}

function nextPermissionDecision(value: AgentPermissionPolicy[keyof AgentPermissionPolicy]): AgentPermissionPolicy[keyof AgentPermissionPolicy] {
  return PERMISSION_ORDER[(PERMISSION_ORDER.indexOf(value) + 1) % PERMISSION_ORDER.length];
}

function permissionDecisionLabel(value: AgentPermissionPolicy[keyof AgentPermissionPolicy]): string {
  if (value === "allow") return "Allow";
  if (value === "ask") return "Ask";
  return "Deny";
}

function formatMicros(value: number): string {
  return `$${(value / 1_000_000).toFixed(2)}`;
}

function shortWallet(value: string): string {
  return `${value.slice(0, 8)}...${value.slice(-4)}`;
}

export function AgentManagerPage({
  state,
  session,
  onStateChange,
  onActivateAgent,
  onOpenAgent,
  onBrowseMarket,
}: AgentManagerPageProps) {
  const [loading, setLoading] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const deployableLinkedAgent = state.linkedDeployment?.agentCardCid ? state.linkedDeployment : null;
  const runningCount = useMemo(() => state.installedAgents.filter((agent) => agent.running).length, [state.installedAgents]);

  const deployCurrentAgent = async () => {
    if (!permissionAllows(state.permissionDefaults.filesystemWrite) || !permissionAllows(state.permissionDefaults.filesystemEdit)) {
      setError("Enable Filesystem Write and Filesystem Edit in Settings before deploying local agents.");
      return;
    }
    if (!deployableLinkedAgent) {
      onBrowseMarket();
      return;
    }

    const agentWallet = deployableLinkedAgent.agentWallet.toLowerCase();
    setLoading(`deploy:${agentWallet}`);
    setError(null);

    try {
      const metadata = await fetchAgentMetadata({
        runtimeUrl: state.settings.runtimeUrl,
        agentWallet,
        agentCardCid: deployableLinkedAgent.agentCardCid || undefined,
      });
      const lock = await buildAgentLock({
        walletAddress: metadata.walletAddress,
        agentCardUri: metadata.agentCardUri,
        model: metadata.model,
        plugins: metadata.plugins,
        chainId: deployableLinkedAgent.chainId,
        dnaHash: metadata.dnaHash,
      });
      const existing = state.installedAgents.find((agent) => agent.agentWallet === agentWallet) || null;
      if (existing && !agentLocksMatch(existing.lock, lock)) {
        throw new Error("Local agent lock does not match canonical agentCard. Re-deploy is required.");
      }

      const installed = existing
        ? syncInstalledAgent(existing, metadata, lock)
        : createInstalledAgent({
          metadata,
          lock,
          permissions: state.permissionDefaults,
        });

      await ensureAgentWorkspace(installed);
      const daemonStatus = await daemonInstallAgent({
        agentWallet: installed.agentWallet,
        agentCardCid: installed.lock.agentCardCid,
        chainId: installed.lock.chainId,
        modelId: installed.lock.modelId,
        mcpToolsHash: installed.lock.mcpToolsHash,
        dnaHash: installed.lock.dnaHash,
      });
      const deployed = appendAgentReport(
        {
          ...installed,
          workerState: daemonStatusToWorkerState(daemonStatus),
        },
        {
          kind: "deployment",
          title: existing ? "Local deployment refreshed" : "Local deployment created",
          summary: `${metadata.name} is now available on this device.`,
          outcome: "success",
        },
      );

      await onStateChange({
        ...state,
        installedAgents: existing
          ? state.installedAgents.map((agent) => (agent.agentWallet === deployed.agentWallet ? deployed : agent))
          : [...state.installedAgents, deployed],
      });
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
    if (!target) {
      return;
    }
    if (!target.running && !session.active) {
      setError("Session is inactive. Start a session from the Desktop header and try again.");
      return;
    }

    setLoading(`toggle:${agentWallet}`);
    setError(null);
    try {
      if (!target.running) {
        await validateAgentLock(target, state.settings.runtimeUrl);
      }

      const shouldRun = !target.running;
      const daemonStatus = shouldRun ? await daemonStartAgent(agentWallet) : await daemonStopAgent(agentWallet);
      await onStateChange({
        ...state,
        installedAgents: state.installedAgents.map((agent) => (
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
              : agent
        )),
      });
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

    await onStateChange({
      ...state,
      installedAgents: state.installedAgents.filter((agent) => agent.agentWallet !== agentWallet),
    });
    onActivateAgent(null);
  };

  const toggleAgentNetwork = async (agentWallet: string) => {
    const target = state.installedAgents.find((agent) => agent.agentWallet === agentWallet);
    if (!target) {
      return;
    }

    const nextEnabled = !target.network.enabled;
    await daemonMeshSet(agentWallet, nextEnabled);
    await onStateChange({
      ...state,
      installedAgents: state.installedAgents.map((agent) => (
        agent.agentWallet === agentWallet
          ? appendAgentReport(
            {
              ...agent,
              network: {
                ...agent.network,
                enabled: nextEnabled,
                status: nextEnabled ? agent.network.status : "dormant",
                lastError: nextEnabled ? agent.network.lastError : null,
                updatedAt: Date.now(),
              },
            },
            {
              kind: "mesh",
              title: nextEnabled ? "Mesh signaling enabled" : "Mesh signaling disabled",
              summary: nextEnabled
                ? `${agent.metadata.name} is now broadcasting on the local mesh.`
                : `${agent.metadata.name} left the local mesh.`,
              outcome: "info",
            },
          )
          : agent
      )),
    });
  };

  return (
    <div className="agent-manager">
      <ShellPageHeader
        eyebrow="My Agents"
        title="Local Agents"
        subtitle={deployableLinkedAgent
          ? `${state.installedAgents.length} deployed · ${runningCount} running · linked ${deployableLinkedAgent.agentWallet.slice(0, 6)}...${deployableLinkedAgent.agentWallet.slice(-4)}`
          : `${state.installedAgents.length} deployed · ${runningCount} running`}
        actions={(
          <>
            <ShellButton tone="secondary" onClick={onBrowseMarket}>Browse Market</ShellButton>
            <ShellButton
              tone={deployableLinkedAgent ? "primary" : "secondary"}
              disabled={loading !== null}
              onClick={() => {
                void deployCurrentAgent();
              }}
            >
              {loading?.startsWith("deploy:") ? <Loader2 size={14} className="spinner" /> : null}
              {deployableLinkedAgent ? "Deploy Linked Agent" : "Link Agent From Market"}
            </ShellButton>
          </>
        )}
      />

      {error ? <ShellNotice tone="error" className="notification">{error}</ShellNotice> : null}
      {!deployableLinkedAgent && state.identity ? (
        <ShellNotice tone="warning" className="notification">
          Open any agent from the web market and use the desktop deep-link again to attach its immutable agent card before deploying locally.
        </ShellNotice>
      ) : null}

      {state.installedAgents.length === 0 ? (
        <ShellEmptyState
          title="No local agents deployed"
          description="Open the desktop flow from market or from an agent page to deploy the canonical agent locally."
        />
      ) : (
        <div className="agents-list">
          {state.installedAgents.map((agent) => {
            const badges: ComposeAgentBadge[] = [
              {
                label: agent.running ? "Running" : "Installed",
                tone: agent.running ? "green" : "cyan",
                icon: agent.running ? <Play size={12} /> : <ShieldCheck size={12} />,
              },
              {
                label: agent.network.enabled ? "Mesh Enabled" : "Mesh Disabled",
                tone: agent.network.enabled ? "fuchsia" : "neutral",
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
                label: "Mesh",
                value: agent.network.enabled ? agent.network.status : "disabled",
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
                      tone={agent.running ? "danger" : "secondary"}
                      size="sm"
                      iconOnly
                      onClick={() => {
                        void toggleAgent(agent.agentWallet);
                      }}
                      disabled={loading !== null}
                      title={agent.running ? "Stop local agent" : "Start local agent"}
                    >
                      {loading === `toggle:${agent.agentWallet}` ? (
                        <Loader2 size={16} className="spinner" />
                      ) : agent.running ? (
                        <Square size={16} />
                      ) : (
                        <Play size={16} />
                      )}
                    </ShellButton>
                    <ShellButton tone="secondary" size="sm" iconOnly onClick={() => onOpenAgent(agent.agentWallet)} title="Open agent settings">
                      <Eye size={16} />
                    </ShellButton>
                    <ShellButton
                      tone={agent.network.enabled ? "primary" : "secondary"}
                      size="sm"
                      iconOnly
                      onClick={() => {
                        void toggleAgentNetwork(agent.agentWallet);
                      }}
                      disabled={loading !== null}
                      title={agent.network.enabled ? "Disable mesh networking" : "Enable mesh networking (--mesh)"}
                    >
                      <Globe size={16} />
                    </ShellButton>
                    <ShellButton
                      tone="danger"
                      size="sm"
                      iconOnly
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
  meshPeers,
  onBack,
  onStateChange,
  onNotify,
}: AgentDetailPageProps) {
  const [activeTab, setActiveTab] = useState<DetailTab>("permissions");
  const [skillsTab, setSkillsTab] = useState<SkillsTab>("installed");
  const [logLines, setLogLines] = useState<string[]>([]);
  const [logCursor, setLogCursor] = useState<number | undefined>(undefined);
  const [permissionBusy, setPermissionBusy] = useState<null | keyof AgentPermissionPolicy>(null);
  const economics = useMemo(() => summarizeAgentReportEconomics(agent.reports), [agent.reports]);
  const visiblePeers = useMemo(() => meshPeers.filter((peer) => peer.agentWallet !== agent.agentWallet), [agent.agentWallet, meshPeers]);

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
      let nextPermissions: AgentPermissionPolicy = { ...agent.permissions, [key]: nextValue };
      let nextOsPermissions = { ...state.osPermissions };

      if (key === "camera" && nextValue === "allow") {
        const status = await requestMediaPermission("camera");
        nextOsPermissions = { ...nextOsPermissions, camera: status };
        if (status !== "granted") {
          nextPermissions = { ...nextPermissions, camera: "deny" };
          onNotify("error", "Camera permission was not granted by macOS");
        }
      }

      if (key === "microphone" && nextValue === "allow") {
        const status = await requestMediaPermission("microphone");
        nextOsPermissions = { ...nextOsPermissions, microphone: status };
        if (status !== "granted") {
          nextPermissions = { ...nextPermissions, microphone: "deny" };
          onNotify("error", "Microphone permission was not granted by macOS");
        }
      }

      await daemonUpdatePermissions(agent.agentWallet, nextPermissions);
      await onStateChange({
        ...state,
        osPermissions: nextOsPermissions,
        installedAgents: state.installedAgents.map((item) => (
          item.agentWallet === agent.agentWallet
            ? appendAgentReport(
              {
                ...item,
                permissions: nextPermissions,
              },
              {
                kind: "permission",
                title: `${key} permission updated`,
                summary: `${key} is now ${nextPermissions[key]}.`,
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
    const [camera, microphone] = await Promise.all([
      queryMediaPermission("camera"),
      queryMediaPermission("microphone"),
    ]);

    await onStateChange({
      ...state,
      osPermissions: {
        camera,
        microphone,
      },
    });
    onNotify("success", "Local OS permissions refreshed");
  };

  return (
    <section className="agent-detail-page">
      <div className="agent-detail-hero">
        <ShellButton tone="secondary" className="detail-back-btn" onClick={onBack}>
          <ArrowLeft size={14} />
          Back to My Agents
        </ShellButton>
        <div className="agent-detail-copy">
          <h2>{agent.metadata.name}</h2>
          <p>{agent.metadata.description}</p>
          <div className="agent-detail-meta">
            <span>{agent.agentWallet.slice(0, 8)}...{agent.agentWallet.slice(-4)}</span>
            <span>{agent.lock.modelId}</span>
            <span>{agent.metadata.framework}</span>
            <span>{agent.running ? "Running" : "Stopped"}</span>
          </div>
        </div>
        <div className="agent-detail-stats">
          <div>
            <span>Peers seen</span>
            <strong>{agent.network.peersDiscovered}</strong>
          </div>
          <div>
            <span>Local reports</span>
            <strong>{agent.reports.length}</strong>
          </div>
          <div>
            <span>Net</span>
            <strong>{formatMicros(economics.netMicros)}</strong>
          </div>
        </div>
      </div>

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

      {activeTab === "permissions" ? (
        <div className="detail-grid">
          <ShellPanel className="detail-panel">
            <div className="detail-panel-header">
              <h3>Per-Agent Authority</h3>
              <ShellButton tone="secondary" onClick={() => void refreshOsPermissions()}>
                Refresh macOS status
              </ShellButton>
            </div>
            <p className="detail-copy">
              These permissions are enforced per local deployment and passed into runtime execution as hard policy, not only as UI preference.
            </p>
            <div className="permissions-grid">
              {(Object.keys(agent.permissions) as Array<keyof AgentPermissionPolicy>).map((key) => (
                <div key={key} className={`permission-toggle ${agent.permissions[key] === "allow" ? "enabled" : ""}`}>
                  <div className="permission-copy">
                    <div className="permission-label">{key}</div>
                    <p>{key === "network" ? "Mesh signaling and remote tool access." : `Local ${key} access for this deployment.`}</p>
                  </div>
                  <ShellButton
                    className={`permission-btn ${agent.permissions[key] === "allow" ? "enabled" : ""}`}
                    tone={agent.permissions[key] === "allow" ? "primary" : "secondary"}
                    disabled={permissionBusy === key}
                    onClick={() => {
                      void togglePermission(key);
                    }}
                  >
                    {permissionDecisionLabel(agent.permissions[key])}
                  </ShellButton>
                </div>
              ))}
            </div>
          </ShellPanel>

          <ShellPanel className="detail-panel">
            <div className="detail-panel-header">
              <h3>Machine Status</h3>
            </div>
            <div className="detail-stat-stack">
              <div className="detail-stat-card">
                <span>Camera</span>
                <strong>{state.osPermissions.camera}</strong>
              </div>
              <div className="detail-stat-card">
                <span>Microphone</span>
                <strong>{state.osPermissions.microphone}</strong>
              </div>
              <div className="detail-stat-card">
                <span>Managed workspace</span>
                <strong>{agent.agentWallet}</strong>
              </div>
            </div>
          </ShellPanel>
        </div>
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
            <SkillsManager state={state} onStateChange={onStateChange} agentWallet={agent.agentWallet} />
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
              <h3>Interactions & Economics</h3>
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
    </section>
  );
}
