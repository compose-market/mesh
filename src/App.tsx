import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { AlertTriangle, Check, ChevronDown, Copy, Link2, LogOut, RefreshCw, Settings2, Waypoints } from "lucide-react";
import { ComposeAppShell } from "@compose-market/theme/app";
import {
  ShellBanner,
  ShellButton,
  ShellEmptyState,
  ShellFormGroup,
  ShellInput,
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
import { getActiveSessionStatus } from "./lib/api";
import { daemonGetAgentStatus, mergeDaemonStatusIntoInstalledAgent } from "./lib/daemon";
import {
  clearLocalConnectionState,
  createLocalWalletDisplay,
  deriveLinkedDeploymentIntent,
  resolveInheritedLocalChainId,
} from "./lib/deploy";
import {
  canAgentUseMesh,
  formatOsPermissionStatus,
  openSystemPermissionSettings,
  reconcileStateWithOsPermissions,
  queryOsPermissions,
  requestOsPermission,
  type OsPermissionKey,
} from "./lib/permissions";

import {
  checkForLocalUpdates,
  createLocalUpdateState,
  LocalUpdateState,
  installLocalUpdate,
  setLocalUpdateError,
  setLocalUpdatePhase,
} from "./lib/updater";
import {
  ensureBuiltinSkillsInstalled,
  ensureSkillsRoot,
  getLocalPaths,
  setLocalBaseDir,
  loadRuntimeState,
  saveRuntimeState,
  syncAgentLocalFiles,
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
type BasePage = "agents" | "network" | "settings";

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

type LocalSessionUpdate = {
  active: boolean;
  expiresAt?: number | null;
  budgetLimit?: string | null;
  budgetUsed?: string | null;
  budgetRemaining?: string | null;
  sessionId?: string | null;
  duration?: number | null;
  chainId?: number | null;
  composeKeyToken?: string | null;
};

function microsBigIntToUsd(value: bigint): string {
  const whole = value / 1_000_000n;
  const cents = (value % 1_000_000n) / 10_000n;
  return `$${whole.toString()}.${cents.toString().padStart(2, "0")}`;
}

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
  update: LocalSessionUpdate,
): LocalRuntimeState {
  if (!current.identity) {
    return current;
  }

  const nextChainId = update.chainId ?? current.identity.chainId;
  const nextSessionId = update.active
    ? ((update.sessionId && update.sessionId.trim()) || current.identity.sessionId || current.identity.composeKeyId)
    : "";
  const nextIdentity = {
    ...current.identity,
    expiresAt: update.active ? (update.expiresAt ?? current.identity.expiresAt) : 0,
    budget: update.active ? (update.budgetRemaining ?? current.identity.budget) : "0",
    sessionId: nextSessionId,
    composeKeyId: nextSessionId,
    duration: update.active ? (update.duration ?? current.identity.duration) : 0,
    composeKeyToken: !update.active
      ? ""
      : (update.composeKeyToken ?? current.identity.composeKeyToken),
    chainId: nextChainId,
  };

  return {
    ...current,
    identity: nextIdentity,
  };
}

function mergeSessionUpdate(previous: SessionState, update: LocalSessionUpdate): SessionState {
  return {
    ...previous,
    active: update.active,
    expiresAt: update.active ? (update.expiresAt ?? previous.expiresAt) : null,
    budgetLimit: update.active ? (update.budgetLimit ?? previous.budgetLimit) : null,
    budgetUsed: update.active ? (update.budgetUsed ?? previous.budgetUsed) : null,
    budgetRemaining: update.active ? (update.budgetRemaining ?? previous.budgetRemaining) : "0",
    sessionId: update.active ? (update.sessionId ?? previous.sessionId) : null,
    duration: update.active ? (update.duration ?? previous.duration) : null,
    chainId: update.chainId ?? previous.chainId,
    reason: update.active ? undefined : "session-inactive",
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



async function syncInstalledAgentsWithDaemon(state: LocalRuntimeState): Promise<LocalRuntimeState> {
  if (typeof window === "undefined" || !("__TAURI_INTERNALS__" in window) || state.installedAgents.length === 0) {
    return state;
  }

  const statuses = await Promise.all(
    state.installedAgents.map(async (agent) => [agent.agentWallet, await daemonGetAgentStatus(agent.agentWallet)] as const),
  );
  const statusByWallet = new Map(
    statuses.filter((entry): entry is readonly [string, NonNullable<Awaited<ReturnType<typeof daemonGetAgentStatus>>>] => entry[1] !== null),
  );

  return {
    ...state,
    installedAgents: state.installedAgents.map((agent) => (
      mergeDaemonStatusIntoInstalledAgent(agent, statusByWallet.get(agent.agentWallet) || null)
    )),
  };
}

export default function App() {
  const [state, setState] = useState<LocalRuntimeState | null>(null);
  const stateRef = useRef<LocalRuntimeState | null>(null);
  const sessionSyncRef = useRef<{
    userAddress: string;
    chainId: number;
    apiUrl: string;
    promise: Promise<void>;
  } | null>(null);
  const [activePage, setActivePage] = useState<BasePage>("agents");
  const [meshMounted, setMeshMounted] = useState(false);
  const [session, setSession] = useState<SessionState>({ ...defaultSessionState });
  const [selectedAgentWallet, setSelectedAgentWallet] = useState<string | null>(null);
  const [meshPeers, setMeshPeers] = useState<MeshPeerSignal[]>([]);
  const [meshBootstrap, setMeshBootstrap] = useState<MeshBootstrapResolution>(() => resolveLocalMeshBootstrap());
  const [paths, setPaths] = useState<Awaited<ReturnType<typeof getLocalPaths>>>(null);
  const [notification, setNotification] = useState<{ type: "success" | "error"; message: string } | null>(null);
  const [deviceId] = useState(getOrCreateDeviceId);
  const [connectModalOpen, setConnectModalOpen] = useState(false);

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
    await Promise.all(next.installedAgents.map((agent) => syncAgentLocalFiles(agent)));
  }, []);

  useEffect(() => {
    stateRef.current = state;
  }, [state]);

  useEffect(() => {
    void (async () => {
      const loaded = await loadRuntimeState();
      const osPermissions = await queryOsPermissions().catch(() => loaded.osPermissions);
      const daemonHydratedState = await syncInstalledAgentsWithDaemon(loaded).catch(() => loaded);
      const hydratedState = reconcileStateWithOsPermissions(daemonHydratedState, osPermissions);
      await ensureSkillsRoot();
      await ensureBuiltinSkillsInstalled();
      const resolvedPaths = await getLocalPaths();
      setPaths(resolvedPaths);
      await Promise.all(hydratedState.installedAgents.map((agent) => syncAgentLocalFiles(agent)));

      stateRef.current = hydratedState;
      setState(hydratedState);
      setSession(sessionFromIdentity(hydratedState.identity));
    })();
  }, []);

  useEffect(() => {
    if (typeof window === "undefined" || !("__TAURI_INTERNALS__" in window)) {
      return;
    }

    let cancelled = false;

    const refresh = async () => {
      const current = stateRef.current;
      if (!current) {
        return;
      }

      const loaded = await loadRuntimeState();
      const daemonHydratedState = await syncInstalledAgentsWithDaemon(loaded).catch(() => loaded);
      const hydratedState = reconcileStateWithOsPermissions({
        ...daemonHydratedState,
        identity: current.identity,
      }, current.osPermissions);

      if (cancelled) {
        return;
      }

      stateRef.current = hydratedState;
      setState(hydratedState);
    };

    const intervalId = window.setInterval(() => {
      void refresh();
    }, 3_000);

    return () => {
      cancelled = true;
      window.clearInterval(intervalId);
    };
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
    if (activePage === "network") setMeshMounted(true);
  }, [activePage]);

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

  const publishedNetworkAgents = useMemo(
    () => state?.installedAgents.filter((agent) => canAgentUseMesh(agent, state.settings.meshEnabled)) || [],
    [state?.installedAgents, state?.settings.meshEnabled],
  );

  useEffect(() => {
    if (publishedNetworkAgents.length === 0) {
      setMeshPeers([]);
    }
  }, [publishedNetworkAgents.length]);

  useEffect(() => {
    void localMeshService.setDesiredState(buildMeshDesiredState(
      state?.installedAgents || [],
      state?.identity || null,
      deviceId,
      state?.settings.meshEnabled ?? false,
    ));
  }, [deviceId, state?.identity, state?.installedAgents, state?.settings.meshEnabled]);

  const applySessionSnapshot = useCallback((update: LocalSessionUpdate) => {
    setSession((previous) => mergeSessionUpdate(previous, update));
  }, []);

  const handleSessionUpdate = useCallback((update: LocalSessionUpdate) => {
    applySessionSnapshot(update);
    void persistState((current) => applyLocalSessionUpdate(current, update));
  }, [applySessionSnapshot, persistState]);

  const refreshSessionFromBackend = useCallback(async () => {
    const current = stateRef.current;
    if (!current?.identity?.userAddress) {
      return;
    }
    const currentIdentity = current.identity;

    const requestUserAddress = currentIdentity.userAddress;
    const requestChainId = currentIdentity.chainId;
    const currentApiUrl = current.settings.apiUrl;
    const activeSync = sessionSyncRef.current;
    if (
      activeSync
      && activeSync.userAddress === requestUserAddress
      && activeSync.chainId === requestChainId
      && activeSync.apiUrl === currentApiUrl
    ) {
      return activeSync.promise;
    }

    const request = (async () => {
      let response;
      try {
        response = await getActiveSessionStatus({
          apiUrl: currentApiUrl,
          identity: currentIdentity,
        });
      } catch (error) {
        console.warn("[session] Failed to refresh active session from backend", error);
        return;
      }

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
        applySessionSnapshot({
          active: false,
          budgetRemaining: "0",
          chainId: latestIdentity.chainId,
        });

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
      const active = response.status?.isActive ?? (response.expiresAt > Date.now() && BigInt(budgetRemaining) > 0n);

      applySessionSnapshot({
        active,
        expiresAt: response.expiresAt,
        budgetLimit,
        budgetUsed,
        budgetRemaining,
        sessionId: response.keyId,
        duration,
        chainId,
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
    })();

    sessionSyncRef.current = {
      userAddress: requestUserAddress,
      chainId: requestChainId,
      apiUrl: currentApiUrl,
      promise: request,
    };

    try {
      await request;
    } finally {
      if (sessionSyncRef.current?.promise === request) {
        sessionSyncRef.current = null;
      }
    }
  }, [persistState]);

  useEffect(() => {
    if (!state?.identity?.userAddress) {
      return;
    }

    void refreshSessionFromBackend();

    const handleVisibility = () => {
      if (!document.hidden) {
        void refreshSessionFromBackend();
      }
    };

    document.addEventListener("visibilitychange", handleVisibility);

    return () => {
      document.removeEventListener("visibilitychange", handleVisibility);
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
    if (context.hasSession) {
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
  const visibleMeshPeers = meshPeers.filter((peer) => peer.deviceId !== deviceId);

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
            sessionActive={session.active}
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
                  <ShellButton tone="secondary" className="connect-btn" onClick={() => setActivePage("settings")}>
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
            <div style={{ display: activePage === "agents" ? "contents" : "none" }}>
              {selectedAgent ? (
                <AgentDetailPage
                  agent={selectedAgent}
                  state={state}
                  session={session}
                  meshPeers={visibleMeshPeers}
                  onBack={closeAgent}
                  onStateChange={persistState}
                  onNotify={showNotification}
                />
              ) : (
                <AgentManagerPage
                  state={state}
                  onStateChange={persistState}
                  onOpenAgent={openAgent}
                  onBrowse={() => void openUrl(`${WEB_APP_URL}/market`)}
                />
              )}
            </div>
            {meshMounted ? (
              <div style={{ display: activePage === "network" ? "contents" : "none" }}>
                <MeshPage
                  agents={publishedNetworkAgents}
                  peers={visibleMeshPeers}
                  bootstrapResolution={meshBootstrap}
                />
              </div>
            ) : null}
            {activePage === "settings" ? (
              <SettingsPage
                state={state}
                paths={paths}
                appUpdate={appUpdate}
                onStateChange={persistState}
                onCheckForUpdates={() => refreshLocalUpdate({ showChecking: true, showErrors: true })}
                onInstallUpdate={handleInstallUpdate}
                onOpenPath={async (path) => {
                  try {
                    await openUrl(path);
                  } catch (error) {
                    showNotification("error", error instanceof Error ? error.message : "Failed to open local path");
                  }
                }}
                onNotify={showNotification}
                onBack={() => setActivePage("agents")}
                onPathsChange={setPaths}
              />
            ) : null}
          </main>

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

function SettingsPage({
  state,
  paths,
  appUpdate,
  onStateChange,
  onCheckForUpdates,
  onInstallUpdate,
  onOpenPath,
  onNotify,
  onBack,
  onPathsChange,
}: {
  state: LocalRuntimeState;
  paths: Awaited<ReturnType<typeof getLocalPaths>>;
  appUpdate: LocalUpdateState;
  onStateChange: (next: LocalRuntimeState) => Promise<void>;
  onCheckForUpdates: () => Promise<void>;
  onInstallUpdate: () => Promise<void>;
  onOpenPath: (path: string) => Promise<void>;
  onNotify: (type: "success" | "error", message: string) => void;
  onBack: () => void;
  onPathsChange: (paths: Awaited<ReturnType<typeof getLocalPaths>>) => void;
}) {
  const [busyPermission, setBusyPermission] = useState<string | null>(null);
  const [editingBaseDir, setEditingBaseDir] = useState(paths?.base_dir || "");

  const togglePermission = async (key: OsPermissionKey) => {
    const current = state.osPermissions[key];
    if (current === "granted") {
      // Already granted — just open Settings so user can revoke if they want
      await openSystemPermissionSettings(key);
      onNotify("success", `Opened macOS Privacy → ${key}`);
      return;
    }
    setBusyPermission(key);
    try {
      const snapshot = await requestOsPermission(key);
      await onStateChange(reconcileStateWithOsPermissions({ ...state, osPermissions: snapshot }, snapshot));
      if (snapshot[key] !== "granted") {
        await openSystemPermissionSettings(key);
      }
      onNotify("success", `${key}: ${snapshot[key]}`);
    } catch (error) {
      onNotify("error", error instanceof Error ? error.message : `Failed to request ${key}`);
    } finally {
      setBusyPermission(null);
    }
  };

  const refreshPermissions = async () => {
    setBusyPermission("__all__");
    try {
      const osStatus = await queryOsPermissions();
      await onStateChange(reconcileStateWithOsPermissions(state, osStatus));
      onNotify("success", "Permissions refreshed");
    } catch (error) {
      onNotify("error", error instanceof Error ? error.message : "Refresh failed");
    } finally {
      setBusyPermission(null);
    }
  };

  const commitBaseDir = async () => {
    const trimmed = editingBaseDir.trim();
    if (!trimmed || trimmed === paths?.base_dir) return;
    try {
      const newPaths = await setLocalBaseDir(trimmed);
      onPathsChange(newPaths);
      setEditingBaseDir(newPaths.base_dir);
      onNotify("success", `Runtime root relocated to: ${newPaths.base_dir}`);
    } catch (error) {
      onNotify("error", error instanceof Error ? error.message : "Failed to update runtime root");
      setEditingBaseDir(paths?.base_dir || "");
    }
  };

  const meshEnabled = state.settings.meshEnabled ?? false;
  const toggleMesh = async () => {
    await onStateChange({
      ...state,
      settings: { ...state.settings, meshEnabled: !meshEnabled },
    });
    onNotify("success", !meshEnabled ? "Mesh network enabled" : "Mesh network disabled");
  };

  type PermRow = { key: OsPermissionKey; label: string };
  const PERM_ROWS: PermRow[] = [
    { key: "fullDiskAccess", label: "Full Disk Access" },
    { key: "camera", label: "Camera" },
    { key: "microphone", label: "Microphone" },
    { key: "accessibility", label: "Accessibility" },
  ];

  return (
    <div className="sp">
      {/* ── Back button ── */}
      <div className="sp-topbar">
        <ShellButton tone="secondary" onClick={onBack}>← Back</ShellButton>
      </div>

      {/* ── Grid: 2 columns on desktop, 1 on mobile ── */}
      <div className="sp-grid">

        {/* ▸ Column 1: System Permissions */}
        <div className="sp-col">
          <div className="sp-card">
            <div className="sp-card-head">
              <h3>System Permissions</h3>
              <ShellButton
                tone="secondary"
                disabled={busyPermission != null}
                onClick={() => void refreshPermissions()}
              >
                <RefreshCw size={12} />
              </ShellButton>
            </div>
            <div className="sp-perm-list">
              {PERM_ROWS.map(({ key, label }) => {
                const status = state.osPermissions[key];
                const granted = status === "granted";
                const busy = busyPermission === key || busyPermission === "__all__";
                return (
                  <button
                    key={key}
                    type="button"
                    className={`sp-perm-row ${granted ? "sp-perm-row--on" : ""}`}
                    disabled={busy}
                    onClick={() => void togglePermission(key)}
                  >
                    <span className="sp-perm-label">{label}</span>
                    <span className={`sp-perm-badge ${granted ? "sp-perm-badge--granted" : ""}`}>
                      {busy ? "…" : formatOsPermissionStatus(status)}
                    </span>
                    <div className={`perm-toggle-switch ${granted ? "perm-toggle-switch--on" : ""}`}>
                      <div className="perm-toggle-thumb" />
                    </div>
                  </button>
                );
              })}
            </div>
          </div>

          {/* ▸ Mesh Toggle */}
          <div className="sp-card">
            <div className="sp-card-head"><h3>Mesh Network</h3></div>
            <button
              type="button"
              className={`sp-perm-row ${meshEnabled ? "sp-perm-row--on" : ""}`}
              onClick={() => void toggleMesh()}
            >
              <span className="sp-perm-label">{meshEnabled ? "Mesh Enabled" : "Mesh Disabled"}</span>
              <div className={`perm-toggle-switch ${meshEnabled ? "perm-toggle-switch--on" : ""}`}>
                <div className="perm-toggle-thumb" />
              </div>
            </button>
          </div>
        </div>

        {/* ▸ Column 2: Storage + Update */}
        <div className="sp-col">
          <div className="sp-card">
            <div className="sp-card-head"><h3>Local Storage</h3></div>
            <div className="sp-path-group">
              <ShellFormGroup label="Runtime Root">
                <ShellInput
                  value={editingBaseDir}
                  onChange={(e) => setEditingBaseDir(e.target.value)}
                  onBlur={() => void commitBaseDir()}
                  onKeyDown={(e) => { if (e.key === "Enter") void commitBaseDir(); }}
                  placeholder="/path/to/compose-local"
                />
              </ShellFormGroup>
              <div className="sp-path-readonly">
                <span className="sp-path-label">State</span>
                <span className="sp-path-value">{paths?.state_file || "Browser mode"}</span>
              </div>
              <div className="sp-path-readonly">
                <span className="sp-path-label">Agents</span>
                <span className="sp-path-value">{paths?.agents_dir || "Browser mode"}</span>
              </div>
              <div className="sp-path-readonly">
                <span className="sp-path-label">Skills</span>
                <span className="sp-path-value">{paths?.skills_dir || "Browser mode"}</span>
              </div>
            </div>
            <div className="sp-path-actions">
              {paths?.base_dir ? (
                <ShellButton tone="secondary" onClick={() => void onOpenPath(paths.base_dir)}>Open Root</ShellButton>
              ) : null}
              {paths?.skills_dir ? (
                <ShellButton tone="secondary" onClick={() => void onOpenPath(paths.skills_dir)}>Open Skills</ShellButton>
              ) : null}
            </div>
          </div>

          {/* ▸ Version / Update */}
          <div className="sp-card sp-card--row">
            <div className="sp-version-info">
              <strong>v{appUpdate.currentVersion || "?"}</strong>
              {appUpdate.available ? (
                <span className="settings-update-badge">Update: {appUpdate.available.version}</span>
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
                <RefreshCw size={12} /> Check
              </ShellButton>
            )}
          </div>
        </div>
      </div>
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
