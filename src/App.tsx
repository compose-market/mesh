import { useCallback, useEffect, useState } from "react";
import { AlertTriangle, Check, Link2, RefreshCw, Settings2, Shield, Waypoints } from "lucide-react";
import { ComposeAppShell } from "@compose-market/theme/app";
import {
  ShellBanner,
  ShellButton,
  ShellEmptyState,
  ShellModal,
  ShellNotice,
  ShellPageHeader,
  ShellPanel,
  ShellPill,
  ShellTab,
  ShellTabStrip,
} from "@compose-market/theme/shell";
import { open as openUrl } from "@tauri-apps/plugin-shell";
import { DeepLinkHandler } from "./components/deep-link";
import { SessionIndicator } from "./components/session";
import { AgentDetailPage, AgentManagerPage } from "./features/agents/pages";
import {
  appendAgentReport,
  buildAgentExecutionPolicy,
  mergeMeshPeerSignals,
} from "./features/agents/model";
import {
  buildMeshDesiredState,
  desktopMeshService,
  mergeMeshStatusIntoState,
  mergePeerIndexIntoState,
  MeshPage,
  resolveLocalMeshBootstrap,
  resolveMeshBootstrap,
  type MeshBootstrapResolution,
} from "./features/mesh";
import { callAgent, getActiveSessionStatus } from "./lib/api";
import { daemonInstallLaunchAgent, daemonLaunchAgentStatus, daemonUpdatePermissions } from "./lib/daemon";
import { heartbeatService } from "./lib/heartbeat";
import { deriveLinkedDeploymentIntent } from "./lib/local-deploy";
import { queryMediaPermission, requestMediaPermission } from "./lib/permissions";
import {
  applyDesktopUpdateCheck,
  checkForDesktopUpdates,
  createDesktopUpdateState,
  installDesktopUpdate,
  setDesktopUpdateError,
  setDesktopUpdatePhase,
} from "./lib/updater";
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
  MeshPeerSignal,
  OsPermissionStatus,
  PermissionDecision,
  RedeemedDesktopContext,
  SessionState,
} from "./lib/types";
import "./styles.css";

const WEB_APP_URL = "https://compose.market";
const WEB_MARKET_URL = `${WEB_APP_URL}/market`;
const CONNECT_DESKTOP_PATH = "/connect-desktop";
type BasePage = "agents" | "network";

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

function microsBigIntToUsd(value: bigint): string {
  const bounded = value > BigInt(Number.MAX_SAFE_INTEGER)
    ? BigInt(Number.MAX_SAFE_INTEGER)
    : value < 0n
      ? 0n
      : value;
  return `$${(Number(bounded) / 1_000_000).toFixed(2)}`;
}

export default function App() {
  const [state, setState] = useState<DesktopRuntimeState | null>(null);
  const [activePage, setActivePage] = useState<BasePage>("agents");
  const [session, setSession] = useState<SessionState>({ ...defaultSessionState });
  const [activeAgentWallet, setActiveAgentWallet] = useState<string | null>(null);
  const [selectedAgentWallet, setSelectedAgentWallet] = useState<string | null>(null);
  const [meshPeers, setMeshPeers] = useState<MeshPeerSignal[]>([]);
  const [meshBootstrap, setMeshBootstrap] = useState<MeshBootstrapResolution>(() => resolveLocalMeshBootstrap());
  const [paths, setPaths] = useState<Awaited<ReturnType<typeof getDesktopPaths>>>(null);
  const [notification, setNotification] = useState<{ type: "success" | "error"; message: string } | null>(null);
  const [deviceId] = useState(getOrCreateDeviceId);
  const [connectModalOpen, setConnectModalOpen] = useState(false);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [appUpdate, setAppUpdate] = useState(() => createDesktopUpdateState());

  const wallet = state?.identity?.userAddress || null;
  const apiUrl = state?.settings.apiUrl || "https://api.compose.market";

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
        setActivePage("agents");
        setSelectedAgentWallet(detail.wallet.toLowerCase());
      }
    };
    window.addEventListener("navigate-to-agent", navigateHandler);
    return () => window.removeEventListener("navigate-to-agent", navigateHandler);
  }, []);

  useEffect(() => {
    let cancelled = false;

    const refreshBootstrap = async () => {
      const resolved = await resolveMeshBootstrap();
      if (!cancelled) {
        setMeshBootstrap(resolved);
      }
    };

    void refreshBootstrap();
    const intervalId = window.setInterval(() => {
      void refreshBootstrap();
    }, 5 * 60_000);

    return () => {
      cancelled = true;
      window.clearInterval(intervalId);
    };
  }, []);

  const refreshDesktopUpdate = useCallback(async (options?: { showChecking?: boolean; showErrors?: boolean }) => {
    if (!state?.settings.apiUrl) {
      return;
    }

    if (options?.showChecking) {
      setAppUpdate((current) => setDesktopUpdatePhase(current, "checking"));
    }

    try {
      const checkedAt = Date.now();
      const result = await checkForDesktopUpdates(state.settings.apiUrl);
      setAppUpdate((current) => applyDesktopUpdateCheck(current, result, checkedAt));
    } catch (error) {
      console.error("[updater] Failed to check for desktop updates", error);
      if (!options?.showErrors) {
        return;
      }
      const message = error instanceof Error ? error.message : "Failed to check for desktop updates.";
      setAppUpdate((current) => setDesktopUpdateError(current, message));
    }
  }, [state?.settings.apiUrl]);

  useEffect(() => {
    if (!state?.settings.apiUrl) {
      return;
    }

    void refreshDesktopUpdate();
    const intervalId = window.setInterval(() => {
      void refreshDesktopUpdate();
    }, 30 * 60_000);

    return () => window.clearInterval(intervalId);
  }, [refreshDesktopUpdate, state?.settings.apiUrl]);

  useEffect(() => {
    if (!state || !selectedAgentWallet) {
      return;
    }
    const exists = state.installedAgents.some((agent) => agent.agentWallet === selectedAgentWallet);
    if (!exists) {
      setSelectedAgentWallet(null);
    }
  }, [selectedAgentWallet, state]);

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
        const executionPolicy = buildAgentExecutionPolicy(activeAgent.permissions);
        const response = await callAgent({
          runtimeUrl: state.settings.runtimeUrl,
          identity: state.identity!,
          agentWallet: activeAgent.agentWallet,
          message: prompt,
          threadId: `heartbeat-${activeAgent.agentWallet}`,
          grantedPermissions: executionPolicy.grantedPermissions,
          permissionPolicy: executionPolicy.permissionPolicy,
        });
        return response.output || response.error || "HEARTBEAT_OK";
      },
      onAlert: (message) => {
        showNotification("error", `Heartbeat alert: ${message.slice(0, 160)}`);
      },
      onTickComplete: (result) => {
        const updatedAgents = state.installedAgents.map((agent) =>
          agent.agentWallet === activeAgent.agentWallet
            ? (
              result === "ok"
                ? {
                  ...agent,
                  heartbeat: {
                    ...agent.heartbeat,
                    lastRunAt: Date.now(),
                    lastResult: result,
                  },
                }
                : appendAgentReport(
                  {
                    ...agent,
                    heartbeat: {
                      ...agent.heartbeat,
                      lastRunAt: Date.now(),
                      lastResult: result,
                    },
                  },
                  {
                    kind: "heartbeat",
                    title: result === "alert" ? "Heartbeat alert" : "Heartbeat execution failed",
                    summary: result === "alert"
                      ? `${agent.metadata.name} raised a local heartbeat alert.`
                      : `${agent.metadata.name} failed its most recent local heartbeat execution.`,
                    outcome: result === "alert" ? "warning" : "error",
                  },
                )
            )
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

  useEffect(() => {
    void desktopMeshService.configurePeerIndex((payload) => {
      setMeshPeers((current) => mergeMeshPeerSignals(current, payload.peers));
      setState((current) => {
        if (!current) {
          return current;
        }
        const next = mergePeerIndexIntoState(current, payload.peers, deviceId);
        if (next !== current) {
          void saveRuntimeState(next);
        }
        return next;
      });
    });

    return () => {
      void desktopMeshService.configurePeerIndex(null);
    };
  }, [deviceId]);

  const runningNetworkAgent = (
    state?.installedAgents.find((agent) => agent.agentWallet === activeAgentWallet && agent.running && agent.network.enabled) ||
    state?.installedAgents.find((agent) => agent.running && agent.network.enabled) ||
    null
  );

  useEffect(() => {
    if (!runningNetworkAgent) {
      setMeshPeers([]);
    }
  }, [runningNetworkAgent?.agentWallet]);

  useEffect(() => {
    void desktopMeshService.setDesiredState(buildMeshDesiredState(runningNetworkAgent, state?.identity || null, deviceId));
  }, [deviceId, runningNetworkAgent, state?.identity]);

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
      apiUrl,
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
    const previousBudget = BigInt(state.identity.budget || "0");
    const nextBudget = BigInt(budgetRemaining || "0");
    const spentMicros = previousBudget > nextBudget ? previousBudget - nextBudget : 0n;

    let nextState = { ...state, identity: nextIdentity };
    if (spentMicros > 0n) {
      nextState = {
        ...nextState,
        installedAgents: nextState.installedAgents.map((agent) => (
          agent.agentWallet === state.identity?.agentWallet
            ? appendAgentReport(agent, {
              kind: "economics",
              title: "Session spend recorded",
              summary: `${microsBigIntToUsd(spentMicros)} consumed from the active compose-key budget.`,
              outcome: "info",
              costMicros: Number(spentMicros > BigInt(Number.MAX_SAFE_INTEGER) ? BigInt(Number.MAX_SAFE_INTEGER) : spentMicros),
            })
            : agent
        )),
      };
    }

    if (identityChanged || nextState.installedAgents !== state.installedAgents) {
      await persistState(nextState);
    }
  }, [apiUrl, persistState, state]);

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
    const linkedDeployment = deriveLinkedDeploymentIntent(context);
    const next = {
      ...state,
      identity,
      linkedDeployment: linkedDeployment || state.linkedDeployment,
    };
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

  const openAgent = useCallback((agentWallet: string) => {
    setActivePage("agents");
    setSelectedAgentWallet(agentWallet.toLowerCase());
  }, []);

  const closeAgent = useCallback(() => {
    setSelectedAgentWallet(null);
  }, []);

  const browseMarket = useCallback(() => {
    void openUrl(WEB_MARKET_URL);
  }, []);

  const handleInstallUpdate = useCallback(async () => {
    if (!state?.settings.apiUrl) {
      return;
    }

    setAppUpdate((current) => setDesktopUpdatePhase(current, "installing"));
    try {
      await installDesktopUpdate(state.settings.apiUrl);
    } catch (error) {
      console.error("[updater] Failed to install desktop update", error);
      const message = error instanceof Error ? error.message : "Failed to install desktop update.";
      setAppUpdate((current) => setDesktopUpdateError(current, message));
      showNotification("error", message);
    }
  }, [showNotification, state?.settings.apiUrl]);

  const dismissUpdateBanner = useCallback(() => {
    setAppUpdate((current) => ({
      ...current,
      phase: "idle",
      available: null,
      error: null,
    }));
  }, []);

  const stateReady = state !== null;
  const selectedAgent = state?.installedAgents.find((agent) => agent.agentWallet === selectedAgentWallet) || null;
  const visibleMeshPeers = runningNetworkAgent
    ? meshPeers.filter((peer) => !(peer.deviceId === deviceId && peer.agentWallet === runningNetworkAgent.agentWallet))
    : [];

  return (
    <ComposeAppShell contentClassName="app">
      {!stateReady ? (
        <div className="main">
          <ShellEmptyState
            title="Loading Desktop Runtime"
            description="Restoring local state, runtime paths, permissions, and mesh identity for this device."
          />
        </div>
      ) : (
        <>
          <DeepLinkHandler
            apiUrl={apiUrl}
            activeWallet={state.identity?.userAddress || null}
            chainId={state.identity?.chainId || null}
            deviceId={deviceId}
            onContextRedeemed={handleContextRedeemed}
            onSessionUpdate={handleSessionUpdate}
          />

          <ShellPanel className="header-shell" padded={false}>
            <ShellPageHeader
              eyebrow="Compose Desktop"
              title="A P2P Network of autonomous agents."
              subtitle="Customize your local agent, and let it collaborate with a Network of its peers."              
              actions={(
                <>
                  {state.identity ? (
                    <SessionIndicator
                      apiUrl={apiUrl}
                      identity={state.identity}
                      session={session}
                      onRefreshSession={refreshSessionFromBackend}
                      onNotify={showNotification}
                    />
                  ) : null}
                  <ShellButton tone="secondary" className="connect-btn" onClick={openConnectModal}>
                    <Link2 size={14} />
                    {wallet ? "Reconnect" : "Connect"}
                  </ShellButton>
                  <ShellButton tone="secondary" className="connect-btn" onClick={() => setSettingsOpen(true)}>
                    <Settings2 size={14} />
                    Settings
                  </ShellButton>
                  <ShellPill>
                    {wallet ? (
                      <span>{wallet.slice(0, 6)}...{wallet.slice(-4)}</span>
                    ) : (
                      <span>Not connected</span>
                    )}
                  </ShellPill>
                </>
              )}
            />
          </ShellPanel>

          {notification ? (
            <ShellNotice tone={notification.type === "success" ? "success" : "error"} className="notification">
              {notification.type === "success" ? <Check size={16} /> : <AlertTriangle size={16} />}
              {notification.message}
            </ShellNotice>
          ) : null}

          {appUpdate.available ? (
            <ShellBanner
              className="connect-banner update-banner"
              title={appUpdate.phase === "error"
                ? `Compose Desktop ${appUpdate.available.version} could not be installed.`
                : appUpdate.phase === "installing"
                  ? `Installing Compose Desktop ${appUpdate.available.version}...`
                  : `Compose Desktop ${appUpdate.available.version} is available.`}
              subtitle={appUpdate.phase === "error"
                ? (appUpdate.error || "The update check succeeded, but installation failed. Retry directly from the app.")
                : (appUpdate.available.notes || `Current version ${appUpdate.currentVersion || "unknown"} can be upgraded in place from this desktop shell.`)}
              actions={(
                <>
                  {appUpdate.phase === "installing" ? (
                    <ShellButton tone="secondary" disabled>
                      <RefreshCw size={14} className="cm-spinner" />
                      Installing
                    </ShellButton>
                  ) : (
                    <ShellButton tone="primary" onClick={() => void handleInstallUpdate()}>
                      <RefreshCw size={14} />
                      {appUpdate.phase === "error" ? "Retry Update" : "Install Update"}
                    </ShellButton>
                  )}
                  {appUpdate.phase !== "installing" ? (
                    <ShellButton tone="ghost" onClick={dismissUpdateBanner}>
                      Later
                    </ShellButton>
                  ) : null}
                </>
              )}
            />
          ) : null}

          <nav className="nav shell-nav">
            <ShellTabStrip>
              <ShellTab active={activePage === "agents"} onClick={() => setActivePage("agents")}>
                My Agents
              </ShellTab>
              <ShellTab active={activePage === "network"} onClick={() => setActivePage("network")}>
                <Waypoints size={14} />
                Mesh
              </ShellTab>
            </ShellTabStrip>
          </nav>

          {!wallet ? (
            <ShellBanner
              className="connect-banner"
              title="Desktop is not connected."
              subtitle="Link the current device from the web app to deploy local agents and refresh the active compose-key."
              actions={<ShellButton tone="secondary" onClick={openConnectModal}>Connect Desktop</ShellButton>}
            />
          ) : null}

          <main className="main">
            {activePage === "agents" ? (
              selectedAgent ? (
                <AgentDetailPage
                  agent={selectedAgent}
                  state={state}
                  meshPeers={visibleMeshPeers}
                  onBack={closeAgent}
                  onStateChange={persistState}
                  onNotify={showNotification}
                />
              ) : (
                <AgentManagerPage
                  state={state}
                  session={session}
                  onStateChange={persistState}
                  onActivateAgent={activateAgent}
                  onOpenAgent={openAgent}
                  onBrowseMarket={browseMarket}
                />
              )
            ) : (
              <MeshPage
                agent={runningNetworkAgent}
                peers={visibleMeshPeers}
                bootstrapResolution={meshBootstrap}
              />
            )}
          </main>

          <ShellModal
            open={settingsOpen}
            title="Desktop Settings"
            subtitle="Runtime endpoints, per-agent defaults, always-on launch behavior, and managed local paths."
            onClose={() => setSettingsOpen(false)}
            className="settings-modal-shell"
          >
            <SettingsPanel
              state={state}
              paths={paths}
              onStateChange={persistState}
              onNotify={showNotification}
            />
          </ShellModal>

          <ConnectModal
            open={connectModalOpen}
            deviceId={deviceId}
            onClose={() => setConnectModalOpen(false)}
          />
        </>
      )}
    </ComposeAppShell>
  );
}

function permissionStatusText(status: OsPermissionStatus): string {
  if (status === "granted") return "Granted";
  if (status === "denied") return "Denied";
  if (status === "unsupported") return "Unsupported";
  return "Unknown";
}

const PERMISSION_ORDER: PermissionDecision[] = ["deny", "ask", "allow"];

function nextDecision(value: PermissionDecision): PermissionDecision {
  const index = PERMISSION_ORDER.indexOf(value);
  return PERMISSION_ORDER[(index + 1) % PERMISSION_ORDER.length];
}

function decisionLabel(value: PermissionDecision): string {
  if (value === "allow") return "Allow";
  if (value === "ask") return "Ask";
  return "Deny";
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
  const [apiUrl, setApiUrl] = useState(state.settings.apiUrl);
  const [runtimeUrl, setRuntimeUrl] = useState(state.settings.runtimeUrl);
  const [permissionBusy, setPermissionBusy] = useState<null | keyof AgentPermissionPolicy>(null);
  const [permissionTarget, setPermissionTarget] = useState<string>("default");
  const [launchAgentInstalled, setLaunchAgentInstalled] = useState<boolean>(false);

  useEffect(() => {
    void (async () => {
      try {
        const installed = await daemonLaunchAgentStatus();
        setLaunchAgentInstalled(installed);
      } catch {
        setLaunchAgentInstalled(false);
      }
    })();
  }, []);

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
      try {
        await daemonUpdatePermissions(selectedAgent.agentWallet, nextPermissions);
      } catch (error) {
        onNotify("error", error instanceof Error ? error.message : "Failed to update daemon permissions");
      }
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
        apiUrl: apiUrl.trim(),
        runtimeUrl: runtimeUrl.trim(),
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
        camera: camera === "granted" ? activePermissions.camera : "deny",
        microphone: microphone === "granted" ? activePermissions.microphone : "deny",
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
      const nextDecisionValue = nextDecision(activePermissions[key]);
      let nextPermissions: AgentPermissionPolicy = { ...activePermissions, [key]: nextDecisionValue };
      let nextOsPermissions = { ...state.osPermissions };

      if (key === "camera" && nextDecisionValue === "allow") {
        const status = await requestMediaPermission("camera");
        nextOsPermissions = { ...nextOsPermissions, camera: status };
        if (status !== "granted") {
          nextPermissions = { ...nextPermissions, camera: "deny" };
          onNotify("error", "Camera permission was not granted by macOS");
        } else {
          onNotify("success", "Camera permission granted");
        }
      }

      if (key === "microphone" && nextDecisionValue === "allow") {
        const status = await requestMediaPermission("microphone");
        nextOsPermissions = { ...nextOsPermissions, microphone: status };
        if (status !== "granted") {
          nextPermissions = { ...nextPermissions, microphone: "deny" };
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
          <input type="text" value={apiUrl} onChange={(event) => setApiUrl(event.target.value)} />
        </div>
        <div className="form-group">
          <label>Runtime URL</label>
          <input type="text" value={runtimeUrl} onChange={(event) => setRuntimeUrl(event.target.value)} />
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
            decision={activePermissions.shell}
            busy={permissionBusy === "shell"}
            onToggle={() => {
              void togglePermission("shell");
            }}
          />
          <PermissionToggle
            label="Filesystem Read"
            description="Allow agents to read local files in managed workspace."
            decision={activePermissions.filesystemRead}
            busy={permissionBusy === "filesystemRead"}
            onToggle={() => {
              void togglePermission("filesystemRead");
            }}
          />
          <PermissionToggle
            label="Filesystem Write"
            description="Allow creating new files and folders for skills/runtime."
            decision={activePermissions.filesystemWrite}
            busy={permissionBusy === "filesystemWrite"}
            onToggle={() => {
              void togglePermission("filesystemWrite");
            }}
          />
          <PermissionToggle
            label="Filesystem Edit"
            description="Allow modifying existing files in managed workspace."
            decision={activePermissions.filesystemEdit}
            busy={permissionBusy === "filesystemEdit"}
            onToggle={() => {
              void togglePermission("filesystemEdit");
            }}
          />
          <PermissionToggle
            label="Filesystem Delete"
            description="Allow deleting local files and installed skills."
            decision={activePermissions.filesystemDelete}
            busy={permissionBusy === "filesystemDelete"}
            onToggle={() => {
              void togglePermission("filesystemDelete");
            }}
          />
          <PermissionToggle
            label="Network"
            description="Allow network calls for MCP/GOAT tool execution."
            decision={activePermissions.network}
            busy={permissionBusy === "network"}
            onToggle={() => {
              void togglePermission("network");
            }}
          />
          <PermissionToggle
            label="Camera"
            description={`macOS status: ${permissionStatusText(state.osPermissions.camera)}`}
            decision={activePermissions.camera}
            busy={permissionBusy === "camera"}
            onToggle={() => {
              void togglePermission("camera");
            }}
          />
          <PermissionToggle
            label="Microphone"
            description={`macOS status: ${permissionStatusText(state.osPermissions.microphone)}`}
            decision={activePermissions.microphone}
            busy={permissionBusy === "microphone"}
            onToggle={() => {
              void togglePermission("microphone");
            }}
          />
        </div>
        <ShellButton tone="secondary" className="permission-refresh-btn" onClick={() => void refreshMacPermissions()}>
          <RefreshCw size={14} />
          Refresh macOS Permission Status
        </ShellButton>
      </div>

      <div className="settings-section">
        <h3>Daemon</h3>
        <div className="form-group">
          <label>LaunchAgent</label>
          <input type="text" value={launchAgentInstalled ? "Installed" : "Not installed"} disabled />
        </div>
        <ShellButton
          tone="secondary"
          onClick={() => {
            void (async () => {
              try {
                await daemonInstallLaunchAgent();
                setLaunchAgentInstalled(true);
                onNotify("success", "LaunchAgent installed for always-on runtime");
              } catch (error) {
                onNotify("error", error instanceof Error ? error.message : "Failed to install LaunchAgent");
              }
            })();
          }}
        >
          Install LaunchAgent
        </ShellButton>
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

      <ShellButton tone="primary" onClick={() => void save()}>Save Settings</ShellButton>
    </div>
  );
}

function PermissionToggle({
  label,
  description,
  decision,
  busy,
  onToggle,
}: {
  label: string;
  description: string;
  decision: PermissionDecision;
  busy: boolean;
  onToggle: () => void;
}) {
  const active = decision === "allow";
  const asking = decision === "ask";

  return (
    <div className={`permission-toggle ${active ? "enabled" : asking ? "ask" : ""}`}>
      <div className="permission-copy">
        <div className="permission-label">
          <Shield size={14} />
          {label}
        </div>
        <p>{description}</p>
      </div>
      <ShellButton
        tone={active ? "primary" : asking ? "ghost" : "secondary"}
        className={`permission-btn ${active ? "enabled" : asking ? "ask" : ""}`}
        onClick={onToggle}
        disabled={busy}
      >
        {decisionLabel(decision)}
      </ShellButton>
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
  const handleConnect = async () => {
    const connectUrl = `${WEB_APP_URL}${CONNECT_DESKTOP_PATH}?device_id=${encodeURIComponent(deviceId)}`;
    await openUrl(connectUrl);
    onClose();
  };

  return (
    <ShellModal
      open={open}
      title="Connect Desktop"
      subtitle="Open the Compose web app and authorize this desktop device from the browser flow."
      onClose={onClose}
      className="connect-modal"
    >
      <div className="connect-modal-copy">
        Click the button below to open the Compose web app and authorize this desktop application.
      </div>
        <div className="connect-modal-actions">
          <ShellButton tone="secondary" onClick={onClose}>Cancel</ShellButton>
          <ShellButton tone="primary" onClick={handleConnect}>
            <Link2 size={14} style={{ marginRight: "8px" }} />
            Authorize in Browser
          </ShellButton>
        </div>
    </ShellModal>
  );
}
