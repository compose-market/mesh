import { invoke } from "@tauri-apps/api/core";
import { listen, type UnlistenFn } from "@tauri-apps/api/event";
import type { DesktopIdentityContext, DesktopRuntimeState, InstalledAgent, InstalledSkill, MeshAgentCard, MeshManifest, MeshPeerSignal } from "../../lib/types";
import { buildMeshAgentCard, listPluginIds, recordMeshPeerSignal } from "../agents/model";
import { buildManifestPayload, canonicalManifestPayload, hydrateManifestNetworkFields, signAndPublishManifest } from "./manifest";
import {
  deriveBootstrapAnchors,
  derivePeerAnchor,
  resolveLocalMeshBootstrap,
  resolveMeshBootstrap,
  type MeshBootstrapAnchor,
  type MeshBootstrapResolution,
} from "./model";

interface MeshJoinRequest {
  userAddress: string;
  agentWallet: string;
  deviceId: string;
  chainId: number;
  sessionId?: string;
  dnaHash?: string;
  capabilitiesHash?: string;
  gossipTopic: string;
  announceTopic: string;
  manifestTopic: string;
  conclaveTopic: string;
  kadProtocol: string;
  heartbeatMs: number;
  capabilities: string[];
  bootstrapMultiaddrs: string[];
  relayMultiaddrs: string[];
  publicCard?: MeshAgentCard;
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

export interface MeshRuntimeStatus {
  running: boolean;
  status: "dormant" | "connecting" | "online" | "error";
  userAddress: string | null;
  agentWallet: string | null;
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
  identity: DesktopIdentityContext;
  agentWallet: string;
  deviceId: string;
  sessionId?: string;
  dnaHash?: string;
  capabilitiesHash?: string;
  publicCard?: MeshAgentCard;
  manifest: MeshManifest;
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
    agentWallet: null,
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

function resetAgentMeshState(agent: InstalledAgent, updatedAt: number): InstalledAgent {
  if (
    agent.network.status === "dormant"
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
      status: "dormant",
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
    return await invoke<MeshRuntimeStatus>("desktop_network_status");
  } catch (error) {
    return {
      ...createDormantStatus(),
      status: "error",
      lastError: error instanceof Error ? error.message : String(error),
    };
  }
}

async function joinMeshRuntime(request: MeshJoinRequest): Promise<MeshRuntimeStatus> {
  return invoke<MeshRuntimeStatus>("desktop_network_join", { request });
}

async function leaveMeshRuntime(): Promise<MeshRuntimeStatus> {
  return invoke<MeshRuntimeStatus>("desktop_network_leave");
}

export function buildMeshDesiredState(
  agent: InstalledAgent | null,
  identity: DesktopIdentityContext | null,
  deviceId: string,
  installedSkills: InstalledSkill[],
): MeshDesiredState | null {
  if (!agent || !identity || !agent.running || !agent.network.enabled) {
    return null;
  }

  return {
    enabled: true,
    identity,
    agentWallet: agent.agentWallet,
    deviceId,
    sessionId: identity.sessionId || "",
    dnaHash: agent.lock.dnaHash || "",
    capabilitiesHash: listPluginIds(agent.metadata.plugins).join("|"),
    publicCard: agent.network.publicCard || buildMeshAgentCard(agent),
    manifest: buildManifestPayload({
      agent,
      skills: installedSkills,
      userAddress: identity.userAddress,
      deviceId,
      chainId: identity.chainId,
      previousManifest: agent.network.manifest,
      stateRootHash: agent.network.manifest?.stateRootHash ?? null,
      pdpPieceCid: agent.network.manifest?.pdpPieceCid ?? null,
      pdpAnchoredAt: agent.network.manifest?.pdpAnchoredAt ?? null,
    }),
  };
}

export function mergeMeshStatusIntoState(
  current: DesktopRuntimeState,
  status: MeshRuntimeStatus,
  deviceId: string,
): DesktopRuntimeState {
  const updatedAt = status.updatedAt || Date.now();
  const targetWallet = status.agentWallet?.toLowerCase() || null;

  return {
    ...current,
    installedAgents: current.installedAgents.map((agent) => {
      if (!agent.network.enabled) {
        return resetAgentMeshState(agent, updatedAt);
      }

      if (!targetWallet || targetWallet !== agent.agentWallet || status.deviceId !== deviceId) {
        return resetAgentMeshState(agent, updatedAt);
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
          updatedAt,
        },
      };
    }),
  };
}

export function mergePeerIndexIntoState(
  current: DesktopRuntimeState,
  incoming: MeshPeerSignal[],
  deviceId: string,
): DesktopRuntimeState {
  const targetAgent = current.installedAgents.find((agent) => agent.running && agent.network.enabled);
  if (!targetAgent || incoming.length === 0) {
    return current;
  }

  let nextTarget = targetAgent;
  let changed = false;

  for (const signal of incoming) {
    if (signal.deviceId === deviceId && signal.agentWallet === targetAgent.agentWallet) {
      continue;
    }

    const existing = nextTarget.network.recentPings.find((item) => item.peerId === signal.peerId);
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

    nextTarget = recordMeshPeerSignal(nextTarget, signal);
    changed = true;
  }

  return changed
    ? {
      ...current,
      installedAgents: current.installedAgents.map((agent) => (
        agent.agentWallet === nextTarget.agentWallet ? nextTarget : agent
      )),
    }
    : current;
}

export function mergeManifestIntoState(
  current: DesktopRuntimeState,
  manifest: MeshManifest,
): DesktopRuntimeState {
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

class DesktopMeshService {
  private desired: MeshDesiredState | null = null;
  private syncTimer: number | null = null;
  private syncInFlight = false;
  private bootstrap: MeshBootstrapConfig | null = null;
  private bootstrapResolvedAt = 0;
  private lastJoinFingerprint: string | null = null;
  private lastManifestFingerprint: string | null = null;
  private lastStatus: MeshRuntimeStatus = createDormantStatus();
  private onStatus: ((status: MeshRuntimeStatus) => void) | null = null;
  private peerUnlisten: UnlistenFn | null = null;
  private onPeerIndex: ((payload: MeshPeerIndexPayload) => void) | null = null;
  private onManifest: ((manifest: MeshManifest) => void) | null = null;

  private desiredKey(state: MeshDesiredState | null): string {
    return state
      ? [
        state.identity.userAddress.toLowerCase(),
        String(state.identity.chainId),
        state.agentWallet.toLowerCase(),
        state.deviceId,
        state.sessionId || "",
        state.dnaHash || "",
        state.capabilitiesHash || "",
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
      this.lastManifestFingerprint = null;
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
      this.lastManifestFingerprint = null;
    }
  }

  private async publishManifestIfNeeded(status: MeshRuntimeStatus): Promise<void> {
    if (!this.desired || !status.running || !status.peerId) {
      return;
    }

    const manifest = hydrateManifestNetworkFields(this.desired.manifest, {
      peerId: status.peerId,
      listenMultiaddrs: status.listenMultiaddrs,
    });
    const fingerprint = canonicalManifestPayload(manifest);
    if (this.lastManifestFingerprint === fingerprint) {
      return;
    }

    const signed = await signAndPublishManifest(manifest);
    this.lastManifestFingerprint = fingerprint;
    this.onManifest?.(signed);
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
        || runtimeStatus.agentWallet !== this.desired.agentWallet.toLowerCase()
        || runtimeStatus.deviceId !== this.desired.deviceId
        || this.lastJoinFingerprint !== joinFingerprint
      );

      const status = shouldRestart
        ? await joinMeshRuntime({
          userAddress: this.desired.identity.userAddress,
          agentWallet: this.desired.agentWallet,
          deviceId: this.desired.deviceId,
          chainId: this.desired.identity.chainId,
          sessionId: this.desired.sessionId,
          dnaHash: this.desired.dnaHash,
          capabilitiesHash: this.desired.capabilitiesHash,
          gossipTopic: this.bootstrap.gossipTopic,
          announceTopic: this.bootstrap.announceTopic,
          manifestTopic: this.bootstrap.manifestTopic,
          conclaveTopic: this.bootstrap.conclaveTopic,
          kadProtocol: this.bootstrap.kadProtocol,
          heartbeatMs: this.bootstrap.heartbeatMs,
          capabilities: [`agent-${this.desired.agentWallet.toLowerCase().replace(/^0x/, "")}`],
          bootstrapMultiaddrs: this.bootstrap.bootstrapMultiaddrs,
          relayMultiaddrs: this.bootstrap.relayMultiaddrs,
          publicCard: this.desired.publicCard,
        })
        : runtimeStatus;

      let manifestError: string | null = null;
      try {
        await this.publishManifestIfNeeded(status);
      } catch (error) {
        this.lastManifestFingerprint = null;
        manifestError = error instanceof Error ? error.message : String(error);
      }

      this.lastJoinFingerprint = joinFingerprint;
      this.publishStatus({
        ...status,
        lastError: manifestError || status.lastError,
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

export const desktopMeshService = new DesktopMeshService();
