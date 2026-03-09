import { useEffect, useMemo, useState } from "react";
import { Activity, ArrowLeft, BadgeDollarSign, Clock3, FileText, Globe, Shield, Sparkles } from "lucide-react";
import { ShellButton, ShellPanel, ShellTab, ShellTabStrip } from "@compose-market/theme/shell";
import { daemonTailLogs, daemonUpdatePermissions } from "../lib/daemon";
import { summarizeAgentEconomics } from "../lib/agent";
import { queryMediaPermission, requestMediaPermission } from "../lib/permissions";
import { SkillsManager } from "./skills-manager";
import { SkillsMarketplace } from "./skills-store";
import type { AgentPermissionPolicy, AgentTaskReport, DesktopRuntimeState, InstalledAgent, MeshPeerSignal } from "../lib/types";

type DetailTab = "permissions" | "skills" | "history" | "mesh";
type SkillsTab = "installed" | "browse";

interface AgentDetailPageProps {
  agent: InstalledAgent;
  state: DesktopRuntimeState;
  meshPeers: MeshPeerSignal[];
  onBack: () => void;
  onStateChange: (next: DesktopRuntimeState) => Promise<void>;
  onNotify: (type: "success" | "error", message: string) => void;
}

const DETAIL_TABS: Array<{ id: DetailTab; label: string; icon: typeof Shield }> = [
  { id: "permissions", label: "Permissions", icon: Shield },
  { id: "skills", label: "Skills", icon: Sparkles },
  { id: "history", label: "Reports / History", icon: FileText },
  { id: "mesh", label: "Peer / Mesh", icon: Globe },
];

const PERMISSION_ORDER: AgentPermissionPolicy[keyof AgentPermissionPolicy][] = ["deny", "ask", "allow"];

function nextDecision(value: AgentPermissionPolicy[keyof AgentPermissionPolicy]): AgentPermissionPolicy[keyof AgentPermissionPolicy] {
  const index = PERMISSION_ORDER.indexOf(value);
  return PERMISSION_ORDER[(index + 1) % PERMISSION_ORDER.length];
}

function decisionLabel(value: AgentPermissionPolicy[keyof AgentPermissionPolicy]): string {
  if (value === "allow") return "Allow";
  if (value === "ask") return "Ask";
  return "Deny";
}

function formatMicros(value: number): string {
  return `$${(value / 1_000_000).toFixed(2)}`;
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

  const economics = useMemo(() => summarizeAgentEconomics(
    agent.reports.flatMap((report) => {
      const items: Array<{ type: "session-spend" | "peer-revenue"; amountMicros: number }> = [];
      if (typeof report.costMicros === "number" && report.costMicros > 0) {
        items.push({ type: "session-spend", amountMicros: report.costMicros });
      }
      if (typeof report.revenueMicros === "number" && report.revenueMicros > 0) {
        items.push({ type: "peer-revenue", amountMicros: report.revenueMicros });
      }
      return items;
    }),
  ), [agent.reports]);

  const peerCards = useMemo(
    () => meshPeers.filter((peer) => peer.agentWallet !== agent.agentWallet),
    [agent.agentWallet, meshPeers],
  );

  useEffect(() => {
    let cancelled = false;

    async function pollLogs(): Promise<void> {
      try {
        const result = await daemonTailLogs(agent.agentWallet, logCursor);
        if (cancelled) return;
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
    }, 4000);

    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [agent.agentWallet, logCursor]);

  const togglePermission = async (key: keyof AgentPermissionPolicy) => {
    if (permissionBusy) return;
    setPermissionBusy(key);

    try {
      const nextValue = nextDecision(agent.permissions[key]);
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
      const permissionReport: AgentTaskReport = {
        id: `${agent.agentWallet}-permission-${Date.now()}`,
        kind: "permission",
        title: `${key} permission updated`,
        summary: `${key} is now ${nextPermissions[key]}.`,
        outcome: "info",
        createdAt: Date.now(),
      };
      await onStateChange({
        ...state,
        osPermissions: nextOsPermissions,
        installedAgents: state.installedAgents.map((item) => (
          item.agentWallet === agent.agentWallet
            ? {
              ...item,
              permissions: nextPermissions,
              reports: [
                permissionReport,
                ...item.reports,
              ].slice(0, 128),
            }
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
                    {decisionLabel(agent.permissions[key])}
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
                <strong>{peerCards.length}</strong>
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
