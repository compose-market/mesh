import { useCallback, useEffect, useState } from "react";
import { AlertTriangle, Check, Link2, RefreshCw, Shield } from "lucide-react";
import { open as openUrl } from "@tauri-apps/plugin-shell";
import { AgentManager } from "./components/AgentManager";
import { DeepLinkHandler } from "./components/DeepLinkHandler";
import { SessionIndicator } from "./components/session";
import { SkillsManager } from "./components/SkillsManager";
import { SkillsMarketplace } from "./components/SkillsMarketplace";
import { callAgent, getActiveSessionStatus } from "./lib/api";
import { heartbeatService } from "./lib/heartbeat";
import { desktopMeshService, type MeshRuntimeStatus } from "./lib/network";
import { queryMediaPermission, requestMediaPermission } from "./lib/permissions";
import {
  ensureSkillsRoot,
  getDesktopPaths,
  loadRuntimeState,
  saveRuntimeState,
} from "./lib/storage";
import type {
  AgentPermissionPolicy,
  DesktopIdentityContext,
  DesktopRuntimeState,
  OsPermissionStatus,
  RedeemedDesktopContext,
  SessionState,
} from "./lib/types";
import "./styles.css";

const APP_VERSION = "1.0.0";
const WEB_APP_URL = "https://compose.market";
const CONNECT_DESKTOP_PATH = "/connect-desktop";
type Tab = "agents" | "skills" | "marketplace" | "settings";

const defaultSessionState: SessionState = {
  active: false,
  expiresAt: null,
  budgetLimit: null,
  budgetUsed: null,
  budgetRemaining: null,
  sessionId: null,
  duration: null,
  chainId: null,
};

function toIdentityContext(context: RedeemedDesktopContext): DesktopIdentityContext | null {
  if (!context.hasSession) {
    return {
      agentWallet: context.agentWallet.toLowerCase(),
      userAddress: context.userAddress.toLowerCase(),
      composeKeyId: "",
      composeKeyToken: "",
      sessionId: "",
      budget: "0",
      duration: 0,
      chainId: context.chainId,
      expiresAt: 0,
      deviceId: context.deviceId,
    };
  }

  return {
    agentWallet: context.agentWallet.toLowerCase(),
    userAddress: context.userAddress.toLowerCase(),
    composeKeyId: context.composeKey.keyId,
    composeKeyToken: context.composeKey.token,
    sessionId: context.session.sessionId,
    budget: context.session.budget,
    duration: context.session.duration,
    chainId: context.chainId,
    expiresAt: context.composeKey.expiresAt,
    deviceId: context.deviceId,
  };
}

function sessionFromIdentity(identity: DesktopIdentityContext | null): SessionState {
  if (!identity) {
    return { ...defaultSessionState };
  }

  const now = Date.now();
  const active = identity.expiresAt > now && (() => {
    try {
      return BigInt(identity.budget || "0") > 0n;
    } catch {
      return false;
    }
  })();
  return {
    active,
    expiresAt: identity.expiresAt,
    budgetLimit: null,
    budgetUsed: null,
    budgetRemaining: identity.budget,
    sessionId: identity.sessionId,
    duration: identity.duration,
    chainId: identity.chainId,
    reason: active ? undefined : "session-inactive",
  };
}

function getOrCreateDeviceId(): string {
  const key = "compose_desktop_device_id";
  const existing = localStorage.getItem(key);
  if (existing) return existing;
  const created = crypto.randomUUID();
  localStorage.setItem(key, created);
  return created;
}

function mergeMeshStatusIntoState(
  current: DesktopRuntimeState,
  status: MeshRuntimeStatus,
  deviceId: string,
): DesktopRuntimeState {
  const targetWallet = status.agentWallet?.toLowerCase() || null;
  const nextAgents = current.installedAgents.map((agent) => {
    if (!agent.network.enabled) {
      if (
        agent.network.status !== "dormant" ||
        agent.network.peerId !== null ||
        agent.network.listenMultiaddrs.length > 0 ||
        agent.network.peersDiscovered !== 0 ||
        agent.network.lastError !== null ||
        agent.network.lastHeartbeatAt !== null
      ) {
        return {
          ...agent,
          network: {
            ...agent.network,
            status: "dormant" as const,
            peerId: null,
            listenMultiaddrs: [],
            peersDiscovered: 0,
            lastError: null,
            lastHeartbeatAt: null,
            updatedAt: Date.now(),
          },
        };
      }
      return agent;
    }

    if (!targetWallet || targetWallet !== agent.agentWallet || status.deviceId !== deviceId) {
      if (agent.network.status === "dormant" && !agent.network.lastError) {
        return agent;
      }
      return {
        ...agent,
        network: {
          ...agent.network,
          status: "dormant" as const,
          peerId: null,
          listenMultiaddrs: [],
          peersDiscovered: 0,
          lastError: null,
          lastHeartbeatAt: null,
          updatedAt: Date.now(),
        },
      };
    }

    return {
      ...agent,
      network: {
        ...agent.network,
        status: status.status,
        peerId: status.peerId,
        listenMultiaddrs: [...status.listenMultiaddrs],
        peersDiscovered: status.peersDiscovered,
        lastHeartbeatAt: status.lastHeartbeatAt,
        lastError: status.lastError,
        updatedAt: status.updatedAt,
      },
    };
  });

  return {
    ...current,
    installedAgents: nextAgents,
  };
}

export default function App() {
  const [state, setState] = useState<DesktopRuntimeState | null>(null);
  const [activeTab, setActiveTab] = useState<Tab>("agents");
  const [session, setSession] = useState<SessionState>({ ...defaultSessionState });
  const [activeAgentWallet, setActiveAgentWallet] = useState<string | null>(null);
  const [paths, setPaths] = useState<Awaited<ReturnType<typeof getDesktopPaths>>>(null);
  const [notification, setNotification] = useState<{ type: "success" | "error"; message: string } | null>(null);
  const [deviceId] = useState(getOrCreateDeviceId);
  const [connectModalOpen, setConnectModalOpen] = useState(false);

  const wallet = state?.identity?.userAddress || null;
  const lambdaUrl = state?.settings.lambdaUrl || "https://api.compose.market";

  const showNotification = useCallback((type: "success" | "error", message: string) => {
    setNotification({ type, message });
    window.setTimeout(() => setNotification(null), 4000);
  }, []);

  const persistState = useCallback(async (next: DesktopRuntimeState) => {
    setState(next);
    await saveRuntimeState(next);
  }, []);

  useEffect(() => {
    void (async () => {
      const loaded = await loadRuntimeState();
      await ensureSkillsRoot();
      const resolvedPaths = await getDesktopPaths();
      setPaths(resolvedPaths);

      setState(loaded);
      setSession(sessionFromIdentity(loaded.identity));

      const running = loaded.installedAgents.find((agent) => agent.running);
      setActiveAgentWallet(running?.agentWallet || null);
    })();
  }, []);

  useEffect(() => {
    const navigateHandler = (event: Event) => {
      const detail = (event as CustomEvent<{ wallet?: string }>).detail;
      if (detail?.wallet) {
        setActiveTab("agents");
      }
    };
    window.addEventListener("navigate-to-agent", navigateHandler);
    return () => window.removeEventListener("navigate-to-agent", navigateHandler);
  }, []);

  useEffect(() => {
    if (!state?.identity || !activeAgentWallet) {
      heartbeatService.stop();
      return;
    }

    const activeAgent = state.installedAgents.find((agent) => agent.agentWallet === activeAgentWallet && agent.running);
    if (!activeAgent || !activeAgent.heartbeat.enabled) {
      heartbeatService.stop();
      return;
    }

    heartbeatService.start({
      agentWallet: activeAgent.agentWallet,
      intervalMs: activeAgent.heartbeat.intervalMs,
      onExecute: async (prompt) => {
        const response = await callAgent({
          manowarUrl: state.settings.manowarUrl,
          identity: state.identity!,
          agentWallet: activeAgent.agentWallet,
          message: prompt,
          threadId: `heartbeat-${activeAgent.agentWallet}`,
        });
        return response.output || response.error || "HEARTBEAT_OK";
      },
      onAlert: (message) => {
        showNotification("error", `Heartbeat alert: ${message.slice(0, 160)}`);
      },
      onTickComplete: (result) => {
        const updatedAgents = state.installedAgents.map((agent) =>
          agent.agentWallet === activeAgent.agentWallet
            ? {
                ...agent,
                heartbeat: {
                  ...agent.heartbeat,
                  lastRunAt: Date.now(),
                  lastResult: result,
                },
              }
            : agent,
        );
        void persistState({ ...state, installedAgents: updatedAgents });
      },
    });

    return () => heartbeatService.stop();
  }, [activeAgentWallet, persistState, showNotification, state]);

  useEffect(() => {
    desktopMeshService.configure((status) => {
      setState((current) => {
        if (!current) {
          return current;
        }
        return mergeMeshStatusIntoState(current, status, deviceId);
      });
    });

    return () => {
      desktopMeshService.configure(null);
      void desktopMeshService.setDesiredState(null);
    };
  }, [deviceId]);

  const runningNetworkAgent = state?.installedAgents.find((agent) => agent.running && agent.network.enabled) || null;

  useEffect(() => {
    if (
      !state?.identity ||
      !runningNetworkAgent
    ) {
      void desktopMeshService.setDesiredState(null);
      return;
    }

    void desktopMeshService.setDesiredState({
      enabled: true,
      identity: state.identity,
      agentWallet: runningNetworkAgent.agentWallet,
      deviceId,
    });
  }, [
    deviceId,
    runningNetworkAgent?.agentWallet,
    runningNetworkAgent?.running,
    runningNetworkAgent?.network.enabled,
    state?.identity,
  ]);

  const handleSessionUpdate = useCallback((active: boolean, expiresAt: number | null, budget: string | null, sessionId?: string, duration?: number) => {
    setSession((prev) => ({
      ...prev,
      active,
      expiresAt,
      budgetRemaining: budget,
      sessionId: sessionId || null,
      duration: duration ?? null,
      reason: active ? undefined : "session-inactive",
    }));

    if (!state || !state.identity) return;
    const nextIdentity = {
      ...state.identity,
      expiresAt: expiresAt ?? 0,
      budget: budget ?? "0",
      sessionId: sessionId || "",
      composeKeyId: sessionId || "",
      duration: duration ?? 0,
      composeKeyToken: !active
        ? ""
        : sessionId && sessionId !== state.identity.composeKeyId
          ? ""
          : state.identity.composeKeyToken,
    };
    void persistState({ ...state, identity: nextIdentity });
  }, [persistState, state]);

  const refreshSessionFromBackend = useCallback(async () => {
    if (!state?.identity?.userAddress) {
      return;
    }

    const response = await getActiveSessionStatus({
      lambdaUrl,
      userAddress: state.identity.userAddress,
      chainId: state.identity.chainId,
    });

    if (!response || !response.hasSession || !response.keyId || !response.expiresAt) {
      setSession((prev) => ({
        ...prev,
        active: false,
        expiresAt: null,
        budgetLimit: null,
        budgetUsed: null,
        budgetRemaining: "0",
        sessionId: null,
        duration: null,
        chainId: state.identity?.chainId || null,
        reason: "session-inactive",
      }));

      if (state.identity.composeKeyId || state.identity.composeKeyToken || state.identity.sessionId || state.identity.expiresAt > 0) {
        const nextIdentity = {
          ...state.identity,
          composeKeyId: "",
          composeKeyToken: "",
          sessionId: "",
          budget: "0",
          expiresAt: 0,
          duration: 0,
        };
        await persistState({ ...state, identity: nextIdentity });
      }
      return;
    }

    const budgetLimit = response.budgetLimit || "0";
    const budgetUsed = response.budgetUsed || "0";
    const budgetRemaining = response.budgetRemaining || "0";
    const chainId = response.chainId || state.identity.chainId;
    const duration = Math.max(0, response.expiresAt - Date.now());
    const active = response.expiresAt > Date.now() && BigInt(budgetRemaining) > 0n;

    setSession({
      active,
      expiresAt: response.expiresAt,
      budgetLimit,
      budgetUsed,
      budgetRemaining,
      sessionId: response.keyId,
      duration,
      chainId,
      reason: active ? undefined : "session-inactive",
    });

    const nextIdentity = {
      ...state.identity,
      composeKeyId: response.keyId,
      composeKeyToken: response.token || state.identity.composeKeyToken,
      sessionId: response.keyId,
      budget: budgetRemaining,
      duration,
      expiresAt: response.expiresAt,
      chainId,
    };
    const identityChanged = (
      nextIdentity.composeKeyId !== state.identity.composeKeyId ||
      nextIdentity.composeKeyToken !== state.identity.composeKeyToken ||
      nextIdentity.sessionId !== state.identity.sessionId ||
      nextIdentity.budget !== state.identity.budget ||
      nextIdentity.duration !== state.identity.duration ||
      nextIdentity.expiresAt !== state.identity.expiresAt ||
      nextIdentity.chainId !== state.identity.chainId
    );
    if (identityChanged) {
      await persistState({ ...state, identity: nextIdentity });
    }
  }, [lambdaUrl, persistState, state]);

  useEffect(() => {
    if (!state?.identity?.userAddress) {
      return;
    }

    void refreshSessionFromBackend();

    const sync = () => {
      void refreshSessionFromBackend();
    };

    const intervalId = window.setInterval(sync, 15_000);
    const handleVisibility = () => {
      if (!document.hidden) {
        sync();
      }
    };

    window.addEventListener("visibilitychange", handleVisibility);
    window.addEventListener("focus", sync);

    return () => {
      window.clearInterval(intervalId);
      window.removeEventListener("visibilitychange", handleVisibility);
      window.removeEventListener("focus", sync);
    };
  }, [refreshSessionFromBackend, state?.identity?.chainId, state?.identity?.userAddress]);

  const handleContextRedeemed = useCallback((context: RedeemedDesktopContext) => {
    if (!state) return;
    const identity = toIdentityContext(context);
    const next = { ...state, identity };
    void persistState(next);
    setSession(sessionFromIdentity(identity));
    if (identity) {
      window.setTimeout(() => {
        void refreshSessionFromBackend();
      }, 0);
    }
    setConnectModalOpen(false);
    if (context.hasSession) {
      showNotification("success", "Desktop app connected with active session");
    } else {
      showNotification("success", "Desktop app connected. Create a session to get started.");
    }
  }, [persistState, refreshSessionFromBackend, showNotification, state]);

  const openConnectModal = useCallback(() => {
    setConnectModalOpen(true);
  }, []);

  const activateAgent = useCallback((agentWallet: string | null) => {
    setActiveAgentWallet(agentWallet);
  }, []);

  const stateReady = state !== null;

  return (
    <div className="app">
      {!stateReady ? (
        <div className="empty-state">
          <h2>Loading Desktop Runtime...</h2>
        </div>
      ) : (
        <>
          <DeepLinkHandler
            lambdaUrl={lambdaUrl}
            activeWallet={state.identity?.userAddress || null}
            chainId={state.identity?.chainId || null}
            deviceId={deviceId}
            onContextRedeemed={handleContextRedeemed}
            onSessionUpdate={handleSessionUpdate}
          />

          <header className="header">
            <div className="header-left">
              <h1>Compose Desktop</h1>
              <span className="subtitle">Local Agent Runtime</span>
            </div>
            <div className="header-right">
              {state.identity ? (
                <SessionIndicator
                  lambdaUrl={lambdaUrl}
                  identity={state.identity}
                  session={session}
                  onRefreshSession={refreshSessionFromBackend}
                  onNotify={showNotification}
                />
              ) : null}
              <button className="secondary connect-btn" onClick={openConnectModal}>
                <Link2 size={14} />
                {wallet ? "Reconnect" : "Connect"}
              </button>
              {wallet ? (
                <div className="wallet-badge">
                  <span className="wallet-address">{wallet.slice(0, 6)}...{wallet.slice(-4)}</span>
                </div>
              ) : (
                <span className="wallet-hint">Not connected</span>
              )}
            </div>
          </header>

          {notification ? (
            <div className={`notification notification-${notification.type}`}>
              {notification.type === "success" ? <Check size={16} /> : <AlertTriangle size={16} />}
              {notification.message}
            </div>
          ) : null}

          <nav className="nav">
            <button className={`nav-btn ${activeTab === "agents" ? "active" : ""}`} onClick={() => setActiveTab("agents")}>Agents</button>
            <button className={`nav-btn ${activeTab === "skills" ? "active" : ""}`} onClick={() => setActiveTab("skills")}>My Skills</button>
            <button className={`nav-btn ${activeTab === "marketplace" ? "active" : ""}`} onClick={() => setActiveTab("marketplace")}>Marketplace</button>
            <button className={`nav-btn ${activeTab === "settings" ? "active" : ""}`} onClick={() => setActiveTab("settings")}>Settings</button>
          </nav>

          <main className="main">
            {activeTab === "agents" ? (
              <AgentManager
                state={state}
                session={session}
                appVersion={APP_VERSION}
                onStateChange={persistState}
                onActivateAgent={activateAgent}
              />
            ) : null}

            {activeTab === "skills" ? (
              <SkillsManager
                state={state}
                onStateChange={persistState}
              />
            ) : null}

            {activeTab === "marketplace" ? (
              <SkillsMarketplace
                state={state}
                onStateChange={persistState}
              />
            ) : null}

            {activeTab === "settings" ? (
              <SettingsPanel
                state={state}
                paths={paths}
                onStateChange={persistState}
                onNotify={showNotification}
              />
            ) : null}
          </main>

          {!wallet ? (
            <div className="empty-state">
              <h2>Desktop is not connected</h2>
              <p>Connect your wallet through the web app to link your account.</p>
              <button className="primary" onClick={openConnectModal}>Connect Desktop</button>
            </div>
          ) : null}

          <ConnectModal
            open={connectModalOpen}
            deviceId={deviceId}
            onClose={() => setConnectModalOpen(false)}
          />
        </>
      )}
    </div>
  );
}

function permissionStatusText(status: OsPermissionStatus): string {
  if (status === "granted") return "Granted";
  if (status === "denied") return "Denied";
  if (status === "unsupported") return "Unsupported";
  return "Unknown";
}

function SettingsPanel({
  state,
  paths,
  onStateChange,
  onNotify,
}: {
  state: DesktopRuntimeState;
  paths: Awaited<ReturnType<typeof getDesktopPaths>>;
  onStateChange: (next: DesktopRuntimeState) => Promise<void>;
  onNotify: (type: "success" | "error", message: string) => void;
}) {
  const [lambdaUrl, setLambdaUrl] = useState(state.settings.lambdaUrl);
  const [manowarUrl, setManowarUrl] = useState(state.settings.manowarUrl);
  const [permissionBusy, setPermissionBusy] = useState<null | keyof AgentPermissionPolicy>(null);
  const [permissionTarget, setPermissionTarget] = useState<string>("default");

  useEffect(() => {
    if (permissionTarget === "default") {
      return;
    }
    const stillExists = state.installedAgents.some((agent) => agent.agentWallet === permissionTarget);
    if (!stillExists) {
      setPermissionTarget("default");
    }
  }, [permissionTarget, state.installedAgents]);

  const selectedAgent = permissionTarget === "default"
    ? null
    : state.installedAgents.find((agent) => agent.agentWallet === permissionTarget) || null;
  const activePermissions = selectedAgent?.permissions || state.permissionDefaults;

  const updatePermissionState = async (
    nextPermissions: AgentPermissionPolicy,
    nextOsPermissions: DesktopRuntimeState["osPermissions"],
  ) => {
    if (selectedAgent) {
      const nextAgents = state.installedAgents.map((agent) => (
        agent.agentWallet === selectedAgent.agentWallet
          ? { ...agent, permissions: nextPermissions }
          : agent
      ));
      await onStateChange({
        ...state,
        installedAgents: nextAgents,
        osPermissions: nextOsPermissions,
      });
      return;
    }

    await onStateChange({
      ...state,
      permissionDefaults: nextPermissions,
      osPermissions: nextOsPermissions,
    });
  };

  const save = async () => {
    const next: DesktopRuntimeState = {
      ...state,
      settings: {
        lambdaUrl: lambdaUrl.trim(),
        manowarUrl: manowarUrl.trim(),
      },
    };
    await onStateChange(next);
    onNotify("success", "Desktop settings saved");
  };

  const refreshMacPermissions = async () => {
    setPermissionBusy("camera");
    try {
      const [camera, microphone] = await Promise.all([
        queryMediaPermission("camera"),
        queryMediaPermission("microphone"),
      ]);

      const nextPermissions: AgentPermissionPolicy = {
        ...activePermissions,
        camera: camera === "granted" ? activePermissions.camera : false,
        microphone: microphone === "granted" ? activePermissions.microphone : false,
      };
      await updatePermissionState(nextPermissions, {
        camera,
        microphone,
      });
      onNotify("success", "macOS permission status refreshed");
    } finally {
      setPermissionBusy(null);
    }
  };

  const togglePermission = async (key: keyof AgentPermissionPolicy) => {
    if (permissionBusy) return;
    setPermissionBusy(key);

    try {
      const nextEnabled = !activePermissions[key];
      let nextPermissions: AgentPermissionPolicy = { ...activePermissions, [key]: nextEnabled };
      let nextOsPermissions = { ...state.osPermissions };

      if (key === "camera" && nextEnabled) {
        const status = await requestMediaPermission("camera");
        nextOsPermissions = { ...nextOsPermissions, camera: status };
        if (status !== "granted") {
          nextPermissions = { ...nextPermissions, camera: false };
          onNotify("error", "Camera permission was not granted by macOS");
        } else {
          onNotify("success", "Camera permission granted");
        }
      }

      if (key === "microphone" && nextEnabled) {
        const status = await requestMediaPermission("microphone");
        nextOsPermissions = { ...nextOsPermissions, microphone: status };
        if (status !== "granted") {
          nextPermissions = { ...nextPermissions, microphone: false };
          onNotify("error", "Microphone permission was not granted by macOS");
        } else {
          onNotify("success", "Microphone permission granted");
        }
      }

      await updatePermissionState(nextPermissions, nextOsPermissions);
    } finally {
      setPermissionBusy(null);
    }
  };

  return (
    <div className="settings">
      <h2>Settings</h2>

      <div className="settings-section">
        <h3>API</h3>
        <div className="form-group">
          <label>API URL</label>
          <input type="text" value={lambdaUrl} onChange={(event) => setLambdaUrl(event.target.value)} />
        </div>
        <div className="form-group">
          <label>Manowar URL</label>
          <input type="text" value={manowarUrl} onChange={(event) => setManowarUrl(event.target.value)} />
        </div>
      </div>

      <div className="settings-section">
        <h3>Identity</h3>
        <div className="form-group">
          <label>User Wallet</label>
          <input type="text" value={state.identity?.userAddress || "Not linked"} disabled />
        </div>
        <div className="form-group">
          <label>Compose Key</label>
          <input type="text" value={state.identity?.composeKeyId || "Not linked"} disabled />
        </div>
      </div>

      <div className="settings-section">
        <h3>Agent Permissions</h3>
        <p className="settings-hint">Permissions are scoped per-agent. Defaults apply to newly deployed agents.</p>
        <div className="form-group">
          <label>Permission Target</label>
          <select
            value={permissionTarget}
            onChange={(event) => setPermissionTarget(event.target.value)}
          >
            <option value="default">Defaults (new agents)</option>
            {state.installedAgents.map((agent) => (
              <option key={agent.agentWallet} value={agent.agentWallet}>
                {agent.metadata.name} ({agent.agentWallet.slice(0, 6)}...{agent.agentWallet.slice(-4)})
              </option>
            ))}
          </select>
        </div>
        <div className="permissions-grid">
          <PermissionToggle
            label="Shell Execution"
            description="Allow local command execution for local skills."
            enabled={activePermissions.shell}
            busy={permissionBusy === "shell"}
            onToggle={() => {
              void togglePermission("shell");
            }}
          />
          <PermissionToggle
            label="Filesystem Read"
            description="Allow agents to read local files in managed workspace."
            enabled={activePermissions.filesystemRead}
            busy={permissionBusy === "filesystemRead"}
            onToggle={() => {
              void togglePermission("filesystemRead");
            }}
          />
          <PermissionToggle
            label="Filesystem Write"
            description="Allow creating new files and folders for skills/runtime."
            enabled={activePermissions.filesystemWrite}
            busy={permissionBusy === "filesystemWrite"}
            onToggle={() => {
              void togglePermission("filesystemWrite");
            }}
          />
          <PermissionToggle
            label="Filesystem Edit"
            description="Allow modifying existing files in managed workspace."
            enabled={activePermissions.filesystemEdit}
            busy={permissionBusy === "filesystemEdit"}
            onToggle={() => {
              void togglePermission("filesystemEdit");
            }}
          />
          <PermissionToggle
            label="Filesystem Delete"
            description="Allow deleting local files and installed skills."
            enabled={activePermissions.filesystemDelete}
            busy={permissionBusy === "filesystemDelete"}
            onToggle={() => {
              void togglePermission("filesystemDelete");
            }}
          />
          <PermissionToggle
            label="Camera"
            description={`macOS status: ${permissionStatusText(state.osPermissions.camera)}`}
            enabled={activePermissions.camera}
            busy={permissionBusy === "camera"}
            onToggle={() => {
              void togglePermission("camera");
            }}
          />
          <PermissionToggle
            label="Microphone"
            description={`macOS status: ${permissionStatusText(state.osPermissions.microphone)}`}
            enabled={activePermissions.microphone}
            busy={permissionBusy === "microphone"}
            onToggle={() => {
              void togglePermission("microphone");
            }}
          />
        </div>
        <button className="secondary permission-refresh-btn" onClick={() => void refreshMacPermissions()}>
          <RefreshCw size={14} />
          Refresh macOS Permission Status
        </button>
      </div>

      <div className="settings-section">
        <h3>Storage</h3>
        <div className="form-group">
          <label>Runtime Directory</label>
          <input type="text" value={paths?.base_dir || "Browser fallback mode"} disabled />
        </div>
        <div className="form-group">
          <label>Skills Directory</label>
          <input type="text" value={paths?.skills_dir || "Browser fallback mode"} disabled />
        </div>
      </div>

      <button className="primary" onClick={() => void save()}>Save Settings</button>
    </div>
  );
}

function PermissionToggle({
  label,
  description,
  enabled,
  busy,
  onToggle,
}: {
  label: string;
  description: string;
  enabled: boolean;
  busy: boolean;
  onToggle: () => void;
}) {
  return (
    <div className={`permission-toggle ${enabled ? "enabled" : ""}`}>
      <div className="permission-copy">
        <div className="permission-label">
          <Shield size={14} />
          {label}
        </div>
        <p>{description}</p>
      </div>
      <button className={`permission-btn ${enabled ? "enabled" : ""}`} onClick={onToggle} disabled={busy}>
        {enabled ? "On" : "Off"}
      </button>
    </div>
  );
}

function ConnectModal({
  open,
  deviceId,
  onClose,
}: {
  open: boolean;
  deviceId: string;
  onClose: () => void;
}) {
  if (!open) {
    return null;
  }

  const handleConnect = async () => {
    const connectUrl = `${WEB_APP_URL}${CONNECT_DESKTOP_PATH}?device_id=${encodeURIComponent(deviceId)}`;
    await openUrl(connectUrl);
    onClose();
  };

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal connect-modal" onClick={(event) => event.stopPropagation()}>
        <h3>Connect Desktop</h3>
        <p>Click the button below to open the Compose web app and authorize this desktop application.</p>
        <div className="connect-modal-actions">
          <button onClick={onClose} className="secondary">Cancel</button>
          <button className="primary" onClick={handleConnect}>
            <Link2 size={14} style={{ marginRight: "8px" }} />
            Authorize in Browser
          </button>
        </div>
      </div>
    </div>
  );
}
