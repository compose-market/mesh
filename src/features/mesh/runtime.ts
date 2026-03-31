import { invoke } from "@tauri-apps/api/core";
import { listen, type UnlistenFn } from "@tauri-apps/api/event";
import type { InstalledAgent, LocalIdentityContext, LocalRuntimeState, MeshAgentCard, MeshManifest, MeshPeerSignal } from "../../lib/types";
import { buildMeshAgentCard, listPluginIds, recordMeshPeerSignal } from "../agents/model";
import { canAgentUseMesh } from "../../lib/permissions";
import {
  deriveBootstrapAnchors,
  derivePeerAnchor,
  resolveLocalMeshBootstrap,
  resolveMeshBootstrap,
  type MeshBootstrapAnchor,
  type MeshBootstrapResolution,
} from "./model";

interface MeshPublishedAgent {
  agentWallet: string;
  dnaHash: string;
  capabilitiesHash: string;
  capabilities: string[];
  publicCard?: MeshAgentCard;
}

interface MeshJoinRequest {
  userAddress: string;
  deviceId: string;
  chainId: number;
  sessionId: string;
  gossipTopic: string;
  announceTopic: string;
  manifestTopic: string;
  conclaveTopic: string;
  kadProtocol: string;
  heartbeatMs: number;
  bootstrapMultiaddrs: string[];
  relayMultiaddrs: string[];
  publishedAgents: MeshPublishedAgent[];
}

interface MeshBootstrapConfig {
  bootstrapMultiaddrs: string[];
  relayMultiaddrs: string[];
  gossipTopic: string;
  announceTopic: string;
  manifestTopic: string;
  conclaveTopic: string;
  kadProtocol: string;
  heartbeatMs: number;
  anchorsByPeerId: Record<string, MeshBootstrapAnchor>;
}

export interface MeshPublishedAgentStatus {
  agentWallet: string;
  haiId: string;
}

export interface MeshRuntimeStatus {
  running: boolean;
  status: "dormant" | "connecting" | "online" | "error";
  userAddress: string | null;
  publishedAgents: MeshPublishedAgentStatus[];
  deviceId: string | null;
  peerId: string | null;
  listenMultiaddrs: string[];
  peersDiscovered: number;
  lastHeartbeatAt: number | null;
  lastError: string | null;
  updatedAt: number;
}

export interface MeshDesiredState {
  enabled: boolean;
  identity: LocalIdentityContext;
  deviceId: string;
  publishedAgents: MeshPublishedAgent[];
}

export interface MeshPeerIndexPayload {
  peers: MeshPeerSignal[];
  updatedAt: number;
}

function isTauriRuntime(): boolean {
  return typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;
}

function createDormantStatus(): MeshRuntimeStatus {
  return {
    running: false,
    status: "dormant",
    userAddress: null,
    publishedAgents: [],
    deviceId: null,
    peerId: null,
    listenMultiaddrs: [],
    peersDiscovered: 0,
    lastHeartbeatAt: null,
    lastError: null,
    updatedAt: Date.now(),
  };
}

function normalizeBootstrap(resolution: MeshBootstrapResolution): MeshBootstrapConfig {
  return {
    bootstrapMultiaddrs: [...resolution.bootstrapMultiaddrs],
    relayMultiaddrs: [...resolution.relayMultiaddrs],
    gossipTopic: resolution.gossipTopic,
    announceTopic: resolution.announceTopic,
    manifestTopic: resolution.manifestTopic,
    conclaveTopic: resolution.conclaveTopic,
    kadProtocol: resolution.kadProtocol,
    heartbeatMs: resolution.heartbeatMs,
    anchorsByPeerId: deriveBootstrapAnchors(resolution),
  };
}

function signalKey(signal: Pick<MeshPeerSignal, "id" | "haiId" | "peerId" | "agentWallet">): string {
  return signal.id || signal.haiId || `${signal.peerId}:${signal.agentWallet || "unknown"}`;
}

function samePublishedAgentSet(status: MeshRuntimeStatus, desired: MeshDesiredState): boolean {
  if (status.publishedAgents.length !== desired.publishedAgents.length) {
    return false;
  }
  const current = new Set(status.publishedAgents.map((agent) => agent.agentWallet.toLowerCase()));
  return desired.publishedAgents.every((agent) => current.has(agent.agentWallet.toLowerCase()));
}

function resetAgentMeshState(agent: InstalledAgent, updatedAt: number): InstalledAgent {
  if (
    agent.network.status === "dormant"
    && agent.network.haiId === null
    && agent.network.peerId === null
    && agent.network.listenMultiaddrs.length === 0
    && agent.network.peersDiscovered === 0
    && agent.network.lastError === null
    && agent.network.lastHeartbeatAt === null
  ) {
    return agent;
  }
  return {
    ...agent,
    network: {
      ...agent.network,
      enabled: false,
      status: "dormant",
      haiId: null,
      peerId: null,
      listenMultiaddrs: [],
      peersDiscovered: 0,
      lastHeartbeatAt: null,
      lastError: null,
      updatedAt,
    },
  };
}

async function getMeshStatusFromRuntime(): Promise<MeshRuntimeStatus> {
  if (!isTauriRuntime()) {
    return createDormantStatus();
  }
  try {
    return await invoke<MeshRuntimeStatus>("local_network_status");
  } catch (error) {
    return {
      ...createDormantStatus(),
      status: "error",
      lastError: error instanceof Error ? error.message : String(error),
    };
  }
}

async function joinMeshRuntime(request: MeshJoinRequest): Promise<MeshRuntimeStatus> {
  return invoke<MeshRuntimeStatus>("local_network_join", { request });
}

async function leaveMeshRuntime(): Promise<MeshRuntimeStatus> {
  return invoke<MeshRuntimeStatus>("local_network_leave");
}

export function buildMeshDesiredState(
  agents: InstalledAgent[],
  identity: LocalIdentityContext | null,
  deviceId: string,
  meshEnabled: boolean,
): MeshDesiredState | null {
  if (!identity) {
    return null;
  }

  const publishedAgents = agents
    .filter((agent) => canAgentUseMesh(agent, meshEnabled))
    .map((agent) => ({
      agentWallet: agent.agentWallet,
      dnaHash: agent.lock.dnaHash || "",
      capabilitiesHash: listPluginIds(agent.metadata.plugins).join("|"),
      capabilities: agent.network.publicCard?.capabilities || listPluginIds(agent.metadata.plugins),
      publicCard: agent.network.publicCard || buildMeshAgentCard(agent),
    }))
    .sort((left, right) => left.agentWallet.localeCompare(right.agentWallet));

  if (publishedAgents.length === 0) {
    return null;
  }

  return {
    enabled: true,
    identity,
    deviceId,
    publishedAgents,
  };
}

export function mergeMeshStatusIntoState(
  current: LocalRuntimeState,
  status: MeshRuntimeStatus,
  deviceId: string,
): LocalRuntimeState {
  const updatedAt = status.updatedAt || Date.now();
  const publishedAgents = new Map(
    status.publishedAgents.map((agent) => [agent.agentWallet.toLowerCase(), agent.haiId]),
  );

  return {
    ...current,
    installedAgents: current.installedAgents.map((agent) => {
      if (!canAgentUseMesh(agent, current.settings.meshEnabled)) {
        return resetAgentMeshState(agent, updatedAt);
      }

      const haiId = publishedAgents.get(agent.agentWallet.toLowerCase());
      if (!haiId || status.deviceId !== deviceId) {
        return resetAgentMeshState(agent, updatedAt);
      }

      return {
        ...agent,
        network: {
          ...agent.network,
          enabled: true,
          status: status.status,
          haiId,
          peerId: status.peerId,
          listenMultiaddrs: [...status.listenMultiaddrs],
          peersDiscovered: status.peersDiscovered,
          lastHeartbeatAt: status.lastHeartbeatAt,
          lastError: status.lastError,
          updatedAt,
        },
      };
    }),
  };
}

export function mergePeerIndexIntoState(
  current: LocalRuntimeState,
  incoming: MeshPeerSignal[],
  deviceId: string,
): LocalRuntimeState {
  const targetAgents = current.installedAgents.filter((agent) => canAgentUseMesh(agent, current.settings.meshEnabled));
  if (targetAgents.length === 0 || incoming.length === 0) {
    return current;
  }

  const remoteSignals = incoming.filter((signal) => signal.deviceId !== deviceId);
  if (remoteSignals.length === 0) {
    return current;
  }

  let changed = false;
  const nextByWallet = new Map(targetAgents.map((agent) => [agent.agentWallet, agent]));

  for (const targetAgent of targetAgents) {
    let nextAgent = targetAgent;

    for (const signal of remoteSignals) {
      const existing = nextAgent.network.recentPings.find((item) => signalKey(item) === signalKey(signal));
      const isUpdated = (
        !existing
        || signal.lastSeenAt > existing.lastSeenAt
        || signal.signalCount !== existing.signalCount
        || signal.announceCount !== existing.announceCount
        || signal.lastMessageType !== existing.lastMessageType
      );
      if (!isUpdated) {
        continue;
      }

      nextAgent = recordMeshPeerSignal(nextAgent, signal);
      changed = true;
    }

    nextByWallet.set(targetAgent.agentWallet, nextAgent);
  }

  return changed
    ? {
      ...current,
      installedAgents: current.installedAgents.map((agent) => nextByWallet.get(agent.agentWallet) || agent),
    }
    : current;
}

export function mergeManifestIntoState(
  current: LocalRuntimeState,
  manifest: MeshManifest,
): LocalRuntimeState {
  let changed = false;

  const installedAgents = current.installedAgents.map((agent) => {
    if (agent.agentWallet !== manifest.agentWallet.toLowerCase()) {
      return agent;
    }

    changed = true;
    return {
      ...agent,
      network: {
        ...agent.network,
        manifest,
        publicCard: {
          name: manifest.name,
          description: manifest.description,
          model: manifest.model,
          framework: manifest.framework,
          headline: manifest.headline,
          statusLine: manifest.statusLine,
          capabilities: [...manifest.capabilities],
          updatedAt: manifest.signedAt || Date.now(),
        },
      },
    };
  });

  return changed ? { ...current, installedAgents } : current;
}

class LocalMeshService {
  private desired: MeshDesiredState | null = null;
  private syncTimer: number | null = null;
  private syncInFlight = false;
  private bootstrap: MeshBootstrapConfig | null = null;
  private bootstrapResolvedAt = 0;
  private lastJoinFingerprint: string | null = null;
  private lastStatus: MeshRuntimeStatus = createDormantStatus();
  private onStatus: ((status: MeshRuntimeStatus) => void) | null = null;
  private peerUnlisten: UnlistenFn | null = null;
  private manifestUnlisten: UnlistenFn | null = null;
  private onPeerIndex: ((payload: MeshPeerIndexPayload) => void) | null = null;
  private onManifest: ((manifest: MeshManifest) => void) | null = null;

  private desiredKey(state: MeshDesiredState | null): string {
    return state
      ? [
        state.identity.userAddress.toLowerCase(),
        String(state.identity.chainId),
        state.deviceId,
        state.identity.sessionId || "",
        ...state.publishedAgents.map((agent) => (
          [
            agent.agentWallet.toLowerCase(),
            agent.dnaHash,
            agent.capabilitiesHash,
            agent.capabilities.join(","),
          ].join("|")
        )),
      ].join("|")
      : "none";
  }

  private joinFingerprint(state: MeshDesiredState, bootstrap: MeshBootstrapConfig): string {
    return [
      this.desiredKey(state),
      bootstrap.gossipTopic,
      bootstrap.announceTopic,
      bootstrap.manifestTopic,
      bootstrap.conclaveTopic,
      bootstrap.kadProtocol,
      String(bootstrap.heartbeatMs),
      bootstrap.bootstrapMultiaddrs.join(","),
      bootstrap.relayMultiaddrs.join(","),
    ].join("|");
  }

  private publishStatus(status: MeshRuntimeStatus): void {
    this.lastStatus = status;
    this.onStatus?.(status);
  }

  private clearTimer(): void {
    if (this.syncTimer !== null) {
      window.clearInterval(this.syncTimer);
      this.syncTimer = null;
    }
  }

  private ensureTimer(): void {
    if (this.syncTimer === null) {
      this.syncTimer = window.setInterval(() => {
        void this.syncOnce();
      }, 12_000);
    }
  }

  private async ensureBootstrap(): Promise<void> {
    if (this.bootstrap && (Date.now() - this.bootstrapResolvedAt) < 120_000) {
      return;
    }

    try {
      this.bootstrap = normalizeBootstrap(await resolveMeshBootstrap());
    } catch {
      this.bootstrap = normalizeBootstrap(resolveLocalMeshBootstrap());
    }
    this.bootstrapResolvedAt = Date.now();
  }

  private async stopAndCleanup(): Promise<void> {
    if (!isTauriRuntime()) {
      this.bootstrap = null;
      this.bootstrapResolvedAt = 0;
      this.lastJoinFingerprint = null;
      return;
    }

    try {
      this.publishStatus(await leaveMeshRuntime());
    } catch (error) {
      this.publishStatus({
        ...createDormantStatus(),
        status: "error",
        lastError: error instanceof Error ? error.message : String(error),
      });
    } finally {
      this.bootstrap = null;
      this.bootstrapResolvedAt = 0;
      this.lastJoinFingerprint = null;
    }
  }

  private async syncOnce(): Promise<void> {
    if (!this.desired || this.syncInFlight || !isTauriRuntime()) {
      return;
    }

    this.syncInFlight = true;
    try {
      await this.ensureBootstrap();
      if (!this.desired || !this.bootstrap) {
        return;
      }

      const runtimeStatus = await getMeshStatusFromRuntime();
      const joinFingerprint = this.joinFingerprint(this.desired, this.bootstrap);
      const shouldRestart = (
        !runtimeStatus.running
        || runtimeStatus.deviceId !== this.desired.deviceId
        || !samePublishedAgentSet(runtimeStatus, this.desired)
        || this.lastJoinFingerprint !== joinFingerprint
      );

      const status = shouldRestart
        ? await joinMeshRuntime({
          userAddress: this.desired.identity.userAddress,
          deviceId: this.desired.deviceId,
          chainId: this.desired.identity.chainId,
          sessionId: this.desired.identity.sessionId || "",
          gossipTopic: this.bootstrap.gossipTopic,
          announceTopic: this.bootstrap.announceTopic,
          manifestTopic: this.bootstrap.manifestTopic,
          conclaveTopic: this.bootstrap.conclaveTopic,
          kadProtocol: this.bootstrap.kadProtocol,
          heartbeatMs: this.bootstrap.heartbeatMs,
          bootstrapMultiaddrs: this.bootstrap.bootstrapMultiaddrs,
          relayMultiaddrs: this.bootstrap.relayMultiaddrs,
          publishedAgents: this.desired.publishedAgents,
        })
        : runtimeStatus;

      this.lastJoinFingerprint = joinFingerprint;
      this.publishStatus({
        ...status,
        updatedAt: Date.now(),
      });
    } catch (error) {
      this.lastJoinFingerprint = null;
      this.publishStatus({
        ...createDormantStatus(),
        status: "error",
        lastError: error instanceof Error ? error.message : String(error),
        updatedAt: Date.now(),
      });
    } finally {
      this.syncInFlight = false;
    }
  }

  public configure(listener: ((status: MeshRuntimeStatus) => void) | null): void {
    this.onStatus = listener;
    if (listener) {
      listener(this.lastStatus);
    }
  }

  public configureManifest(listener: ((manifest: MeshManifest) => void) | null): void {
    this.onManifest = listener;
    if (this.manifestUnlisten) {
      this.manifestUnlisten();
      this.manifestUnlisten = null;
    }
    if (!listener || !isTauriRuntime()) {
      return;
    }

    void listen<MeshManifest>("mesh-manifest-updated", (event) => {
      this.onManifest?.(event.payload);
    }).then((unlisten) => {
      if (this.onManifest === listener) {
        this.manifestUnlisten = unlisten;
      } else {
        unlisten();
      }
    }).catch(() => {
      this.manifestUnlisten = null;
    });
  }

  public async configurePeerIndex(listener: ((payload: MeshPeerIndexPayload) => void) | null): Promise<void> {
    this.onPeerIndex = listener;
    if (this.peerUnlisten) {
      this.peerUnlisten();
      this.peerUnlisten = null;
    }
    if (!listener || !isTauriRuntime()) {
      return;
    }

    this.peerUnlisten = await listen<MeshPeerIndexPayload>("mesh-peer-index", (event) => {
      this.onPeerIndex?.({
        ...event.payload,
        peers: event.payload.peers.map((peer) => ({
          ...peer,
          ...derivePeerAnchor(peer.listenMultiaddrs, this.bootstrap?.anchorsByPeerId || {}),
        })),
      });
    });
  }

  public async setDesiredState(state: MeshDesiredState | null): Promise<void> {
    const nextDesired = state?.enabled ? state : null;
    const desiredChanged = this.desiredKey(this.desired) !== this.desiredKey(nextDesired);

    if (desiredChanged && this.desired) {
      await this.stopAndCleanup();
    }

    this.desired = nextDesired;
    if (!this.desired) {
      await this.stopAndCleanup();
      this.publishStatus(await getMeshStatusFromRuntime());
      this.clearTimer();
      return;
    }

    if (!isTauriRuntime()) {
      this.publishStatus({
        ...createDormantStatus(),
        status: "error",
        lastError: "Mesh networking requires Tauri runtime",
      });
      return;
    }

    this.ensureTimer();
    await this.syncOnce();
  }

  public getLastStatus(): MeshRuntimeStatus {
    return this.lastStatus;
  }
}

export const localMeshService = new LocalMeshService();
