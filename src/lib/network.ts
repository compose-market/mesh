import { invoke } from "@tauri-apps/api/core";
import { listen, type UnlistenFn } from "@tauri-apps/api/event";
import {
  deriveBootstrapAnchors,
  derivePeerAnchor,
  resolveMeshBootstrap,
  resolveLocalMeshBootstrap,
  type MeshBootstrapAnchor,
  type MeshBootstrapResolution,
} from "./mesh-bootstrap";
import type { DesktopIdentityContext, MeshAgentCard, MeshPeerSignal } from "./types";

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
  kadProtocol: string;
  heartbeatMs: number;
  source: "dns" | "local";
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
}

export interface MeshPeerIndexPayload {
  peers: MeshPeerSignal[];
  updatedAt: number;
}

function isTauriRuntime(): boolean {
  return typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;
}

function dormantStatus(): MeshRuntimeStatus {
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

async function getMeshStatusFromRuntime(): Promise<MeshRuntimeStatus> {
  if (!isTauriRuntime()) {
    return dormantStatus();
  }
  try {
    return await invoke<MeshRuntimeStatus>("desktop_network_status");
  } catch (error) {
    return {
      ...dormantStatus(),
      status: "error",
      lastError: error instanceof Error ? error.message : String(error),
    };
  }
}

async function startMeshRuntime(request: MeshJoinRequest): Promise<MeshRuntimeStatus> {
  return invoke<MeshRuntimeStatus>("desktop_network_join", { request });
}

async function stopMeshRuntime(): Promise<MeshRuntimeStatus> {
  return invoke<MeshRuntimeStatus>("desktop_network_leave");
}

function normalizeBootstrap(resolution: MeshBootstrapResolution): MeshBootstrapConfig {
  return {
    bootstrapMultiaddrs: [...resolution.bootstrapMultiaddrs],
    relayMultiaddrs: [...resolution.relayMultiaddrs],
    gossipTopic: resolution.gossipTopic,
    announceTopic: resolution.announceTopic,
    kadProtocol: resolution.kadProtocol,
    heartbeatMs: resolution.heartbeatMs,
    source: resolution.source,
    anchorsByPeerId: deriveBootstrapAnchors(resolution),
  };
}

class DesktopMeshService {
  private desired: MeshDesiredState | null = null;
  private syncTimer: number | null = null;
  private syncInFlight = false;
  private bootstrap: MeshBootstrapConfig | null = null;
  private bootstrapResolvedAt = 0;
  private lastJoinFingerprint: string | null = null;
  private lastStatus: MeshRuntimeStatus = dormantStatus();
  private onStatus: ((status: MeshRuntimeStatus) => void) | null = null;
  private peerUnlisten: UnlistenFn | null = null;
  private onPeerIndex: ((payload: MeshPeerIndexPayload) => void) | null = null;

  private desiredKey(state: MeshDesiredState | null): string {
    if (!state) {
      return "none";
    }
    return [
      state.identity.userAddress.toLowerCase(),
      String(state.identity.chainId),
      state.agentWallet.toLowerCase(),
      state.deviceId,
      state.sessionId || "",
      state.dnaHash || "",
      state.capabilitiesHash || "",
    ].join("|");
  }

  private buildJoinFingerprint(state: MeshDesiredState, bootstrap: MeshBootstrapConfig): string {
    return [
      state.identity.userAddress.toLowerCase(),
      state.agentWallet.toLowerCase(),
      state.deviceId,
      String(state.identity.chainId),
      state.sessionId || "",
      state.dnaHash || "",
      state.capabilitiesHash || "",
      bootstrap.gossipTopic,
      bootstrap.announceTopic,
      bootstrap.kadProtocol,
      String(bootstrap.heartbeatMs),
      bootstrap.bootstrapMultiaddrs.join(","),
      bootstrap.relayMultiaddrs.join(","),
    ].join("|");
  }

  public configure(listener: ((status: MeshRuntimeStatus) => void) | null): void {
    this.onStatus = listener;
    if (listener) {
      listener(this.lastStatus);
    }
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
      if (this.onPeerIndex) {
        const peers = event.payload.peers.map((peer) => ({
          ...peer,
          ...derivePeerAnchor(
            peer.listenMultiaddrs,
            this.bootstrap?.anchorsByPeerId || {},
          ),
        }));
        this.onPeerIndex({
          ...event.payload,
          peers,
        });
      }
    });
  }

  public async setDesiredState(state: MeshDesiredState | null): Promise<void> {
    const previous = this.desired;
    const nextDesired = state && state.enabled ? state : null;
    const desiredChanged = this.desiredKey(previous) !== this.desiredKey(nextDesired);
    let stoppedDuringTransition = false;

    if (desiredChanged && previous) {
      await this.stopAndCleanup();
      stoppedDuringTransition = true;
    }

    this.desired = nextDesired;

    if (!this.desired) {
      if (!stoppedDuringTransition) {
        await this.stopAndCleanup();
      }
      this.publishStatus(await getMeshStatusFromRuntime());
      this.clearTimer();
      return;
    }

    if (!isTauriRuntime()) {
      this.publishStatus({
        ...dormantStatus(),
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

  private ensureTimer(): void {
    if (this.syncTimer !== null) {
      return;
    }
    this.syncTimer = window.setInterval(() => {
      void this.syncOnce();
    }, 12_000);
  }

  private clearTimer(): void {
    if (this.syncTimer !== null) {
      window.clearInterval(this.syncTimer);
      this.syncTimer = null;
    }
  }

  private publishStatus(status: MeshRuntimeStatus): void {
    this.lastStatus = status;
    if (this.onStatus) {
      this.onStatus(status);
    }
  }

  private async ensureBootstrap(): Promise<void> {
    const now = Date.now();
    if (this.bootstrap && (now - this.bootstrapResolvedAt) < 120_000) {
      return;
    }

    try {
      const resolved = await resolveMeshBootstrap();
      this.bootstrap = normalizeBootstrap(resolved);
    } catch {
      this.bootstrap = normalizeBootstrap(resolveLocalMeshBootstrap());
    }
    this.bootstrapResolvedAt = now;
  }

  private async stopAndCleanup(): Promise<void> {
    if (!isTauriRuntime()) {
      this.bootstrap = null;
      this.bootstrapResolvedAt = 0;
      this.lastJoinFingerprint = null;
      return;
    }

    try {
      const status = await stopMeshRuntime();
      this.publishStatus(status);
    } catch (error) {
      this.publishStatus({
        ...dormantStatus(),
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
      const joinFingerprint = this.buildJoinFingerprint(this.desired, this.bootstrap);
      const mustRestart = (
        !runtimeStatus.running ||
        runtimeStatus.agentWallet !== this.desired.agentWallet.toLowerCase() ||
        runtimeStatus.deviceId !== this.desired.deviceId ||
        this.lastJoinFingerprint !== joinFingerprint
      );

      let activeStatus = runtimeStatus;
      if (mustRestart) {
        const request: MeshJoinRequest = {
          userAddress: this.desired.identity.userAddress,
          agentWallet: this.desired.agentWallet,
          deviceId: this.desired.deviceId,
          chainId: this.desired.identity.chainId,
          sessionId: this.desired.sessionId,
          dnaHash: this.desired.dnaHash,
          capabilitiesHash: this.desired.capabilitiesHash,
          gossipTopic: this.bootstrap.gossipTopic,
          announceTopic: this.bootstrap.announceTopic,
          kadProtocol: this.bootstrap.kadProtocol,
          heartbeatMs: this.bootstrap.heartbeatMs,
          capabilities: [`agent-${this.desired.agentWallet.toLowerCase().replace(/^0x/, "")}`],
          bootstrapMultiaddrs: this.bootstrap.bootstrapMultiaddrs,
          relayMultiaddrs: this.bootstrap.relayMultiaddrs,
          publicCard: this.desired.publicCard,
        };
        activeStatus = await startMeshRuntime(request);
        this.lastJoinFingerprint = joinFingerprint;
      }

      this.publishStatus({
        ...activeStatus,
        updatedAt: Date.now(),
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.publishStatus({
        ...dormantStatus(),
        status: "error",
        lastError: message,
        updatedAt: Date.now(),
      });
      this.lastJoinFingerprint = null;
    } finally {
      this.syncInFlight = false;
    }
  }
}

export const desktopMeshService = new DesktopMeshService();
