import { useCallback, useEffect, useRef, useState } from "react";
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
  mergeManifestIntoState,
  desktopMeshService,
  mergeMeshStatusIntoState,
  mergePeerIndexIntoState,
  MeshPage,
  resolveLocalMeshBootstrap,
  resolveMeshBootstrap,
  type MeshBootstrapResolution,
} from "./features/mesh";
import { callAgent, getActiveSessionStatus } from "./lib/api";
import { heartbeatService } from "./lib/heartbeat";
import { deriveLinkedDeploymentIntent } from "./lib/local-deploy";
import { queryMediaPermission } from "./lib/permissions";
import {
  applyDesktopUpdateCheck,
  checkForDesktopUpdates,
  createDesktopUpdateState,
  DesktopUpdateState,
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
  DesktopIdentityContext,
  DesktopRuntimeState,
  MeshPeerSignal,
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

function identityFromRedeemedDesktopContext(context: RedeemedDesktopContext): DesktopIdentityContext {
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

function applyRedeemedDesktopContext(
  current: DesktopRuntimeState,
  context: RedeemedDesktopContext,
): DesktopRuntimeState {
  return {
    ...current,
    identity: identityFromRedeemedDesktopContext(context),
  };
}

function applyDesktopSessionUpdate(
  current: DesktopRuntimeState,
  update: {
    active: boolean;
    expiresAt: number | null;
    budget: string | null;
    sessionId?: string;
    duration?: number;
  },
): DesktopRuntimeState {
  if (!current.identity) {
    return current;
  }

  const nextIdentity = {
    ...current.identity,
    expiresAt: update.expiresAt ?? 0,
    budget: update.budget ?? "0",
    sessionId: update.sessionId || "",
    composeKeyId: update.sessionId || "",
    duration: update.duration ?? 0,
    composeKeyToken: !update.active
      ? ""
      : update.sessionId && update.sessionId !== current.identity.composeKeyId
        ? ""
        : current.identity.composeKeyToken,
  };

  return {
    ...current,
    identity: nextIdentity,
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
  const stateRef = useRef<DesktopRuntimeState | null>(null);
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

  const persistState = useCallback(async (
    nextOrUpdater: DesktopRuntimeState | ((current: DesktopRuntimeState) => DesktopRuntimeState),
  ) => {
    const current = stateRef.current;
    if (!current) {
      if (typeof nextOrUpdater === "function") {
        return;
      }
      stateRef.current = nextOrUpdater;
      setState(nextOrUpdater);
      await saveRuntimeState(nextOrUpdater);
      return;
    }

    const next = typeof nextOrUpdater === "function"
      ? nextOrUpdater(current)
      : nextOrUpdater;

    stateRef.current = next;
    setState(next);
    await saveRuntimeState(next);
  }, []);

  useEffect(() => {
    stateRef.current = state;
  }, [state]);

  useEffect(() => {
    void (async () => {
      const loaded = await loadRuntimeState();
      await ensureSkillsRoot();
      const resolvedPaths = await getDesktopPaths();
      setPaths(resolvedPaths);

      stateRef.current = loaded;
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

  useEffect(() => {
    desktopMeshService.configureManifest((manifest) => {
      setState((current) => {
        if (!current) {
          return current;
        }
        const next = mergeManifestIntoState(current, manifest);
        if (next !== current) {
          void saveRuntimeState(next);
        }
        return next;
      });
    });

    return () => {
      desktopMeshService.configureManifest(null);
    };
  }, []);

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
    void desktopMeshService.setDesiredState(buildMeshDesiredState(runningNetworkAgent, state?.identity || null, deviceId, state?.installedSkills || []));
  }, [deviceId, runningNetworkAgent, state?.identity, state?.installedSkills]);

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

    void persistState((current) => applyDesktopSessionUpdate(current, {
      active,
      expiresAt,
      budget,
      sessionId,
      duration,
    }));
  }, [persistState]);

  const refreshSessionFromBackend = useCallback(async () => {
    const current = stateRef.current;
    if (!current?.identity?.userAddress) {
      return;
    }

    const requestUserAddress = current.identity.userAddress;
    const requestChainId = current.identity.chainId;
    const currentApiUrl = current.settings.apiUrl;

    const response = await getActiveSessionStatus({
      apiUrl: currentApiUrl,
      userAddress: requestUserAddress,
      chainId: requestChainId,
    });

    const latest = stateRef.current;
    if (
      !latest?.identity
      || latest.identity.userAddress !== requestUserAddress
      || latest.identity.chainId !== requestChainId
    ) {
      return;
    }
    const latestIdentity = latest.identity;

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
        chainId: latestIdentity.chainId,
        reason: "session-inactive",
      }));

      if (latestIdentity.composeKeyId || latestIdentity.composeKeyToken || latestIdentity.sessionId || latestIdentity.expiresAt > 0) {
        const nextIdentity = {
          ...latestIdentity,
          composeKeyId: "",
          composeKeyToken: "",
          sessionId: "",
          budget: "0",
          expiresAt: 0,
          duration: 0,
        };
        await persistState({ ...latest, identity: nextIdentity });
      }
      return;
    }

    const budgetLimit = response.budgetLimit || "0";
    const budgetUsed = response.budgetUsed || "0";
    const budgetRemaining = response.budgetRemaining || "0";
    const chainId = response.chainId || latestIdentity.chainId;
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
      ...latestIdentity,
      composeKeyId: response.keyId,
      composeKeyToken: response.token || latestIdentity.composeKeyToken,
      sessionId: response.keyId,
      budget: budgetRemaining,
      duration,
      expiresAt: response.expiresAt,
      chainId,
    };
    const identityChanged = (
      nextIdentity.composeKeyId !== latestIdentity.composeKeyId ||
      nextIdentity.composeKeyToken !== latestIdentity.composeKeyToken ||
      nextIdentity.sessionId !== latestIdentity.sessionId ||
      nextIdentity.budget !== latestIdentity.budget ||
      nextIdentity.duration !== latestIdentity.duration ||
      nextIdentity.expiresAt !== latestIdentity.expiresAt ||
      nextIdentity.chainId !== latestIdentity.chainId
    );
    const previousBudget = BigInt(latestIdentity.budget || "0");
    const nextBudget = BigInt(budgetRemaining || "0");
    const spentMicros = previousBudget > nextBudget ? previousBudget - nextBudget : 0n;

    let nextState: DesktopRuntimeState = { ...latest, identity: nextIdentity };
    if (spentMicros > 0n) {
      nextState = {
        ...nextState,
        installedAgents: nextState.installedAgents.map((agent) => (
          agent.agentWallet === latestIdentity.agentWallet
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

    if (identityChanged || nextState.installedAgents !== latest.installedAgents) {
      await persistState(nextState);
    }
  }, [persistState]);

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
    const identity = identityFromRedeemedDesktopContext(context);
    const linkedDeployment = deriveLinkedDeploymentIntent(context);

    void persistState((current) => ({
      ...applyRedeemedDesktopContext(current, context),
      linkedDeployment: linkedDeployment || current.linkedDeployment,
    }));
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
  }, [persistState, refreshSessionFromBackend, showNotification]);

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
            onError={(message) => showNotification("error", message)}
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
                    Connect
                  </ShellButton>
                  <ShellButton tone="secondary" className="connect-btn" onClick={() => setSettingsOpen(true)}>
                    <Settings2 size={14} />
                    Settings
                  </ShellButton>
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
            subtitle="Updater, macOS permissions, and managed local storage."
            onClose={() => setSettingsOpen(false)}
            className="settings-modal-shell"
          >
            <SettingsPanel
              state={state}
              paths={paths}
              appUpdate={appUpdate}
              onStateChange={persistState}
              onCheckForUpdates={() => refreshDesktopUpdate({ showChecking: true, showErrors: true })}
              onInstallUpdate={handleInstallUpdate}
              onOpenSystemPermissions={async () => {
                try {
                  await openUrl("x-help-action://openPrefPane?bundleId=com.apple.settings.PrivacySecurity.extension");
                } catch (error) {
                  showNotification("error", error instanceof Error ? error.message : "Failed to open macOS Privacy & Security");
                }
              }}
              onOpenPath={async (path) => {
                try {
                  await openUrl(path);
                } catch (error) {
                  showNotification("error", error instanceof Error ? error.message : "Failed to open local path");
                }
              }}
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

function SettingsPanel({
  state,
  paths,
  appUpdate,
  onStateChange,
  onCheckForUpdates,
  onInstallUpdate,
  onOpenSystemPermissions,
  onOpenPath,
  onNotify,
}: {
  state: DesktopRuntimeState;
  paths: Awaited<ReturnType<typeof getDesktopPaths>>;
  appUpdate: DesktopUpdateState;
  onStateChange: (next: DesktopRuntimeState) => Promise<void>;
  onCheckForUpdates: () => Promise<void>;
  onInstallUpdate: () => Promise<void>;
  onOpenSystemPermissions: () => Promise<void>;
  onOpenPath: (path: string) => Promise<void>;
  onNotify: (type: "success" | "error", message: string) => void;
}) {
  const [refreshingPermissions, setRefreshingPermissions] = useState(false);

  const refreshMacPermissions = async () => {
    setRefreshingPermissions(true);
    try {
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
      onNotify("success", "macOS permission status refreshed");
    } finally {
      setRefreshingPermissions(false);
    }
  };
  const checkedAt = appUpdate.checkedAt ? new Date(appUpdate.checkedAt).toLocaleString() : "Never";
  const currentVersion = appUpdate.currentVersion || "Unknown";
  const availableVersion = appUpdate.available?.version || "Latest";

  return (
    <div className="settings settings--compact">
      <div className="settings-section">
        <h3>Updates</h3>
        <p className="settings-hint">
          Compose Desktop checks a signed updater manifest through the first-party Compose API route at
          {" "}
          <code>{state.settings.apiUrl}/api/desktop/updates</code>.
        </p>
        <div className="detail-stat-stack">
          <div className="detail-stat-card">
            <span>Current Version</span>
            <strong>{currentVersion}</strong>
          </div>
          <div className="detail-stat-card">
            <span>Available</span>
            <strong>{availableVersion}</strong>
          </div>
          <div className="detail-stat-card">
            <span>Last Checked</span>
            <strong>{checkedAt}</strong>
          </div>
        </div>
        <div className="settings-actions">
          <ShellButton tone="secondary" onClick={() => void onCheckForUpdates()}>
            <RefreshCw size={14} />
            Check Now
          </ShellButton>
          {appUpdate.available ? (
            <ShellButton tone="primary" onClick={() => void onInstallUpdate()}>
              <RefreshCw size={14} />
              Install {appUpdate.available.version}
            </ShellButton>
          ) : null}
        </div>
      </div>

      <div className="settings-section">
        <h3>Permissions</h3>
        <p className="settings-hint">
          Per-agent authority lives on each agent page. Use this shortcut to open macOS Privacy & Security and
          grant Compose Desktop Full System Access.
        </p>
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
            <span>Agent Controls</span>
            <strong>Scoped per selected agent</strong>
          </div>
        </div>
        <div className="settings-actions">
          <ShellButton tone="primary" onClick={() => void onOpenSystemPermissions()}>
            <Shield size={14} />
            Give Full Permissions
          </ShellButton>
          <ShellButton tone="secondary" disabled={refreshingPermissions} onClick={() => void refreshMacPermissions()}>
            <RefreshCw size={14} className={refreshingPermissions ? "cm-spinner" : undefined} />
            Refresh Local Status
          </ShellButton>
        </div>
      </div>

      <div className="settings-section">
        <h3>Storage</h3>
        <p className="settings-hint">
          Compose Desktop stores runtime state, local agent workspaces, and shared skill installs inside the managed
          desktop runtime root.
        </p>
        <div className="settings-path-list">
          <div className="settings-path-row">
            <span>Runtime Root</span>
            <strong>{paths?.base_dir || "Browser fallback mode"}</strong>
          </div>
          <div className="settings-path-row">
            <span>State File</span>
            <strong>{paths?.state_file || "Browser fallback mode"}</strong>
          </div>
          <div className="settings-path-row">
            <span>Agents Directory</span>
            <strong>{paths?.agents_dir || "Browser fallback mode"}</strong>
          </div>
          <div className="settings-path-row">
            <span>Skills Directory</span>
            <strong>{paths?.skills_dir || "Browser fallback mode"}</strong>
          </div>
        </div>
        <div className="settings-actions">
          {paths?.base_dir ? (
            <ShellButton tone="secondary" onClick={() => void onOpenPath(paths.base_dir)}>
              Open Runtime Folder
            </ShellButton>
          ) : null}
          {paths?.skills_dir ? (
            <ShellButton tone="secondary" onClick={() => void onOpenPath(paths.skills_dir)}>
              Open Skills Folder
            </ShellButton>
          ) : null}
        </div>
      </div>
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
