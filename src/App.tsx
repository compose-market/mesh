import { useCallback, useEffect, useRef, useState } from "react";
import { AlertTriangle, Check, ChevronDown, Copy, Link2, LogOut, RefreshCw, Settings2, Shield, Waypoints } from "lucide-react";
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
  localMeshService,
  mergeMeshStatusIntoState,
  mergePeerIndexIntoState,
  MeshPage,
  resolveLocalMeshBootstrap,
  resolveMeshBootstrap,
  type MeshBootstrapResolution,
} from "./features/mesh";
import { callAgent, getActiveSessionStatus } from "./lib/api";
import { heartbeatService } from "./lib/heartbeat";
import {
  clearLocalConnectionState,
  createLocalWalletDisplay,
  deriveLinkedDeploymentIntent,
  resolveInheritedLocalChainId,
} from "./lib/deploy";
import { queryOsPermissions } from "./lib/permissions";
import { GlobalPermissionsSection } from "./features/permissions";
import {
  checkForLocalUpdates,
  createLocalUpdateState,
  LocalUpdateState,
  installLocalUpdate,
  setLocalUpdateError,
  setLocalUpdatePhase,
} from "./lib/updater";
import {
  ensureSkillsRoot,
  getLocalPaths,
  loadRuntimeState,
  saveRuntimeState,
} from "./lib/storage";
import type {
  LocalIdentityContext,
  LocalRuntimeState,
  MeshPeerSignal,
  RedeemedLocalContext,
  SessionState,
} from "./lib/types";
import "./styles.css";

const WEB_APP_URL = "https://compose.market";
const CONNECT_LOCAL_PATH = "/connect-local";
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

function identityFromRedeemedLocalContext(context: RedeemedLocalContext): LocalIdentityContext {
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

function applyRedeemedLocalContext(
  current: LocalRuntimeState,
  context: RedeemedLocalContext,
): LocalRuntimeState {
  return {
    ...current,
    identity: identityFromRedeemedLocalContext(context),
  };
}

function applyLocalSessionUpdate(
  current: LocalRuntimeState,
  update: {
    active: boolean;
    expiresAt: number | null;
    budget: string | null;
    sessionId?: string;
    duration?: number;
  },
): LocalRuntimeState {
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

function sessionFromIdentity(identity: LocalIdentityContext | null): SessionState {
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
  const key = "compose_mesh_device_id";
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
  const [state, setState] = useState<LocalRuntimeState | null>(null);
  const stateRef = useRef<LocalRuntimeState | null>(null);
  const [activePage, setActivePage] = useState<BasePage>("agents");
  const [session, setSession] = useState<SessionState>({ ...defaultSessionState });
  const [activeAgentWallet, setActiveAgentWallet] = useState<string | null>(null);
  const [selectedAgentWallet, setSelectedAgentWallet] = useState<string | null>(null);
  const [meshPeers, setMeshPeers] = useState<MeshPeerSignal[]>([]);
  const [meshBootstrap, setMeshBootstrap] = useState<MeshBootstrapResolution>(() => resolveLocalMeshBootstrap());
  const [paths, setPaths] = useState<Awaited<ReturnType<typeof getLocalPaths>>>(null);
  const [notification, setNotification] = useState<{ type: "success" | "error"; message: string } | null>(null);
  const [deviceId] = useState(getOrCreateDeviceId);
  const [connectModalOpen, setConnectModalOpen] = useState(false);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [appUpdate, setAppUpdate] = useState(() => createLocalUpdateState());

  const wallet = state?.identity?.userAddress || null;
  const apiUrl = state?.settings.apiUrl || "https://api.compose.market";

  const showNotification = useCallback((type: "success" | "error", message: string) => {
    setNotification({ type, message });
    window.setTimeout(() => setNotification(null), 4000);
  }, []);

  const persistState = useCallback(async (
    nextOrUpdater: LocalRuntimeState | ((current: LocalRuntimeState) => LocalRuntimeState),
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
      const resolvedPaths = await getLocalPaths();
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

  const refreshLocalUpdate = useCallback(async (options?: { showChecking?: boolean; showErrors?: boolean }) => {
    if (options?.showChecking) {
      setAppUpdate((current) => setLocalUpdatePhase(current, "checking"));
    }

    try {
      const result = await checkForLocalUpdates();
      setAppUpdate(result);
    } catch (error) {
      console.error("[updater] Failed to check for local updates", error);
      if (!options?.showErrors) {
        return;
      }
      const message = error instanceof Error ? error.message : "Failed to check for local updates.";
      setAppUpdate((current) => setLocalUpdateError(current, message));
    }
  }, []);

  useEffect(() => {
    if (!state?.settings.apiUrl) {
      return;
    }

    void refreshLocalUpdate();
    const intervalId = window.setInterval(() => {
      void refreshLocalUpdate();
    }, 30 * 60_000);

    return () => window.clearInterval(intervalId);
  }, [refreshLocalUpdate, state?.settings.apiUrl]);

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
    localMeshService.configure((status) => {
      setState((current) => {
        if (!current) {
          return current;
        }
        return mergeMeshStatusIntoState(current, status, deviceId);
      });
    });

    return () => {
      localMeshService.configure(null);
      void localMeshService.setDesiredState(null);
    };
  }, [deviceId]);

  useEffect(() => {
    void localMeshService.configurePeerIndex((payload) => {
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
      void localMeshService.configurePeerIndex(null);
    };
  }, [deviceId]);

  useEffect(() => {
    localMeshService.configureManifest((manifest) => {
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
      localMeshService.configureManifest(null);
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
    void localMeshService.setDesiredState(buildMeshDesiredState(
      runningNetworkAgent,
      state?.identity || null,
      deviceId,
      state?.installedSkills || [],
      state?.settings.apiUrl || "",
    ));
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

    void persistState((current) => applyLocalSessionUpdate(current, {
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
      identity: current.identity,
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
    const chainId = resolveInheritedLocalChainId(latestIdentity.chainId, response.chainId);
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

    let nextState: LocalRuntimeState = { ...latest, identity: nextIdentity };
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

  const handleContextRedeemed = useCallback((context: RedeemedLocalContext) => {
    const identity = identityFromRedeemedLocalContext(context);
    const linkedDeployment = deriveLinkedDeploymentIntent(context);

    void persistState((current) => ({
      ...applyRedeemedLocalContext(current, context),
      linkedDeployment,
    }));
    setActivePage("agents");
    setSelectedAgentWallet(null);
    setSession(sessionFromIdentity(identity));
    if (identity) {
      window.setTimeout(() => {
        void refreshSessionFromBackend();
      }, 0);
    }
    setConnectModalOpen(false);
    if (context.hasSession) {
      showNotification("success", "Local app connected with active session");
    } else {
      showNotification("success", "Local app connected. Create a session to get started.");
    }
  }, [persistState, refreshSessionFromBackend, showNotification]);

  const openConnectModal = useCallback(() => {
    setConnectModalOpen(true);
  }, []);

  const disconnectLocalWallet = useCallback(() => {
    const current = stateRef.current;
    if (!current?.identity) {
      return;
    }

    setSession({ ...defaultSessionState });
    setConnectModalOpen(false);
    void persistState((runtime) => clearLocalConnectionState(runtime));
    showNotification("success", "Local wallet disconnected");
  }, [persistState, showNotification]);

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

  const handleInstallUpdate = useCallback(async () => {
    setAppUpdate((current) => setLocalUpdatePhase(current, "installing"));
    try {
      await installLocalUpdate((percent) => {
        setAppUpdate((current) => ({ ...current, phase: "downloading", downloadProgress: percent }));
      });
    } catch (error) {
      console.error("[updater] Failed to install local update", error);
      const message = error instanceof Error ? error.message : "Failed to install local update.";
      setAppUpdate((current) => setLocalUpdateError(current, message));
      showNotification("error", message);
    }
  }, [showNotification]);

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
            title="Loading Local Runtime"
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
              eyebrow="Compose Local"
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
                  {state.identity ? (
                    <LocalWalletMenu
                      identity={state.identity}
                      onSwitch={openConnectModal}
                      onDisconnect={disconnectLocalWallet}
                      onNotify={showNotification}
                    />
                  ) : (
                    <button type="button" className="mesh-wallet-connect-btn" onClick={openConnectModal}>
                      CONNECT
                    </button>
                  )}
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
                ? `Compose Local ${appUpdate.available.version} could not be installed.`
                : appUpdate.phase === "installing"
                  ? `Installing Compose Local ${appUpdate.available.version}...`
                  : `Compose Local ${appUpdate.available.version} is available.`}
              subtitle={appUpdate.phase === "error"
                ? (appUpdate.error || "The update check succeeded, but installation failed. Retry directly from the app.")
                : (appUpdate.available.notes || `Current version ${appUpdate.currentVersion || "unknown"} can be upgraded in place from this local shell.`)}
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
              title="Local is not connected."
              subtitle="Link the current device from the web app to deploy local agents and refresh the active compose-key."
              actions={<ShellButton tone="secondary" onClick={openConnectModal}>Connect Local</ShellButton>}
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
                  onBrowse={() => void openUrl(`${WEB_APP_URL}/market`)}
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
            title="Local Settings"
            subtitle="Updater, macOS permissions, and managed local storage."
            onClose={() => setSettingsOpen(false)}
            className="settings-modal-shell"
          >
            <SettingsPanel
              state={state}
              paths={paths}
              appUpdate={appUpdate}
              onStateChange={persistState}
              onCheckForUpdates={() => refreshLocalUpdate({ showChecking: true, showErrors: true })}
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
  state: LocalRuntimeState;
  paths: Awaited<ReturnType<typeof getLocalPaths>>;
  appUpdate: LocalUpdateState;
  onStateChange: (next: LocalRuntimeState) => Promise<void>;
  onCheckForUpdates: () => Promise<void>;
  onInstallUpdate: () => Promise<void>;
  onOpenSystemPermissions: () => Promise<void>;
  onOpenPath: (path: string) => Promise<void>;
  onNotify: (type: "success" | "error", message: string) => void;
}) {
  const [activeSettingsTab, setActiveSettingsTab] = useState<"permissions" | "storage">("permissions");
  const [refreshingPermissions, setRefreshingPermissions] = useState(false);

  const refreshMacPermissions = async () => {
    setRefreshingPermissions(true);
    try {
      const osStatus = await queryOsPermissions();

      await onStateChange({
        ...state,
        osPermissions: {
          camera: osStatus.camera,
          microphone: osStatus.microphone,
        },
      });
      onNotify("success", "macOS permission status refreshed");
    } finally {
      setRefreshingPermissions(false);
    }
  };

  return (
    <div className="settings settings--compact">
      <ShellTabStrip className="settings-tab-row">
        <ShellTab active={activeSettingsTab === "permissions"} onClick={() => setActiveSettingsTab("permissions")} className={`detail-tab-btn ${activeSettingsTab === "permissions" ? "active" : ""}`}>
          <Shield size={14} />
          Permissions
        </ShellTab>
        <ShellTab active={activeSettingsTab === "storage"} onClick={() => setActiveSettingsTab("storage")} className={`detail-tab-btn ${activeSettingsTab === "storage" ? "active" : ""}`}>
          <Settings2 size={14} />
          Storage
        </ShellTab>
      </ShellTabStrip>

      {activeSettingsTab === "permissions" ? (
        <>
          <GlobalPermissionsSection
            osPermissions={state.osPermissions}
            refreshing={refreshingPermissions}
            onOpenSystemPermissions={() => void onOpenSystemPermissions()}
            onRefresh={() => void refreshMacPermissions()}
          />

          {/* Compact update row */}
          <div className="settings-section settings-update-row">
            <div className="settings-update-info">
              <span className="settings-update-version">v{appUpdate.currentVersion || "?"}</span>
              {appUpdate.available ? (
                <span className="settings-update-badge">Update available: {appUpdate.available.version}</span>
              ) : (
                <span className="settings-update-hint">Up to date</span>
              )}
            </div>
            {appUpdate.available ? (
              <ShellButton tone="primary" onClick={() => void onInstallUpdate()}>
                Install {appUpdate.available.version}
              </ShellButton>
            ) : (
              <ShellButton tone="secondary" onClick={() => void onCheckForUpdates()}>
                <RefreshCw size={14} />
                Check for Updates
              </ShellButton>
            )}
          </div>
        </>
      ) : null}

      {activeSettingsTab === "storage" ? (
        <div className="settings-section">
          <p className="settings-hint">
            Compose Local stores runtime state, agent workspaces, and shared skill installs inside the managed local runtime root.
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
      ) : null}
    </div>
  );
}

function LocalWalletMenu({
  identity,
  onSwitch,
  onDisconnect,
  onNotify,
}: {
  identity: LocalIdentityContext;
  onSwitch: () => void;
  onDisconnect: () => void;
  onNotify: (type: "success" | "error", message: string) => void;
}) {
  const [open, setOpen] = useState(false);
  const [copied, setCopied] = useState(false);
  const rootRef = useRef<HTMLDivElement | null>(null);
  const { shortAddress, chainLabel, accentTone } = createLocalWalletDisplay(identity);

  useEffect(() => {
    if (!open) {
      return;
    }

    const handlePointerDown = (event: PointerEvent) => {
      if (!rootRef.current?.contains(event.target as Node)) {
        setOpen(false);
      }
    };

    window.addEventListener("pointerdown", handlePointerDown);
    return () => {
      window.removeEventListener("pointerdown", handlePointerDown);
    };
  }, [open]);

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(identity.userAddress);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 2000);
    } catch (error) {
      onNotify("error", error instanceof Error ? error.message : "Failed to copy wallet address");
    }
  };

  return (
    <div className="mesh-wallet-menu" ref={rootRef}>
      <button
        type="button"
        className="mesh-wallet-trigger"
        aria-haspopup="menu"
        aria-expanded={open}
        onClick={() => setOpen((current) => !current)}
      >
        <span className={`mesh-wallet-trigger__indicator mesh-wallet-trigger__indicator--${accentTone}`} />
        <span className="mesh-wallet-trigger__address">{shortAddress}</span>
        <ChevronDown size={12} className={`mesh-wallet-trigger__chevron ${open ? "open" : ""}`} />
      </button>

      {open ? (
        <div className="mesh-wallet-dropdown" role="menu">
          <div className="mesh-wallet-dropdown__header">
            <div className="mesh-wallet-dropdown__chain-row">
              <span className={`mesh-wallet-trigger__indicator mesh-wallet-trigger__indicator--${accentTone}`} />
              <span className="mesh-wallet-dropdown__chain">{chainLabel}</span>
            </div>
          </div>

          <div className="mesh-wallet-dropdown__address-row">
            <span className="mesh-wallet-dropdown__address">{shortAddress}</span>
            <button
              type="button"
              className="mesh-wallet-dropdown__icon"
              onClick={handleCopy}
              aria-label="Copy connected wallet address"
            >
              {copied ? <Check size={14} className="mesh-wallet-dropdown__icon mesh-wallet-dropdown__icon--success" /> : <Copy size={14} />}
            </button>
          </div>

          <button
            type="button"
            className="mesh-wallet-dropdown__item"
            role="menuitem"
            onClick={() => {
              setOpen(false);
              onSwitch();
            }}
          >
            <Link2 size={16} />
            Switch
          </button>

          <button
            type="button"
            className="mesh-wallet-dropdown__item mesh-wallet-dropdown__item--danger"
            role="menuitem"
            onClick={() => {
              setOpen(false);
              onDisconnect();
            }}
          >
            <LogOut size={16} />
            Disconnect
          </button>
        </div>
      ) : null}
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
    const connectUrl = `${WEB_APP_URL}${CONNECT_LOCAL_PATH}?device_id=${encodeURIComponent(deviceId)}`;
    await openUrl(connectUrl);
    onClose();
  };

  return (
    <ShellModal
      open={open}
      title="Connect Local"
      subtitle="Open the Compose web app and authorize this local device from the browser flow."
      onClose={onClose}
      className="connect-modal"
    >
      <div className="connect-modal-copy">
        Click the button below to open the Compose web app and authorize this local application.
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
