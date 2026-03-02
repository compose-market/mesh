import { invoke } from "@tauri-apps/api/core";
import {
  createDesktopNetworkToken,
  deleteDesktopNetworkPresence,
  fetchDesktopNetworkBootstrap,
  upsertDesktopNetworkPresence,
  type DesktopNetworkBootstrapResponse,
} from "./api";
import type { DesktopIdentityContext } from "./types";

interface MeshJoinRequest {
  userAddress: string;
  agentWallet: string;
  sessionId: string;
  composeKeyId: string;
  deviceId: string;
  chainId: number;
  gossipTopic: string;
  bootstrapMultiaddrs: string[];
  relayMultiaddrs: string[];
}

export interface MeshRuntimeStatus {
  running: boolean;
  status: "dormant" | "connecting" | "online" | "error";
  userAddress: string | null;
  agentWallet: string | null;
  sessionId: string | null;
  composeKeyId: string | null;
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
  lambdaUrl: string;
  identity: DesktopIdentityContext;
  agentWallet: string;
  deviceId: string;
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
    sessionId: null,
    composeKeyId: null,
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

class DesktopMeshService {
  private desired: MeshDesiredState | null = null;
  private syncTimer: number | null = null;
  private syncInFlight = false;
  private token: string | null = null;
  private tokenExpiresAt = 0;
  private bootstrap: DesktopNetworkBootstrapResponse | null = null;
  private lastStatus: MeshRuntimeStatus = dormantStatus();
  private onStatus: ((status: MeshRuntimeStatus) => void) | null = null;

  private desiredKey(state: MeshDesiredState | null): string {
    if (!state) {
      return "none";
    }
    return [
      state.identity.userAddress.toLowerCase(),
      state.identity.composeKeyId,
      state.identity.sessionId,
      state.agentWallet.toLowerCase(),
      state.deviceId,
      state.lambdaUrl,
    ].join("|");
  }

  public configure(listener: ((status: MeshRuntimeStatus) => void) | null): void {
    this.onStatus = listener;
    if (listener) {
      listener(this.lastStatus);
    }
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

  private invalidateToken(): void {
    this.token = null;
    this.tokenExpiresAt = 0;
    this.bootstrap = null;
  }

  private async ensureTokenAndBootstrap(): Promise<void> {
    if (!this.desired) {
      throw new Error("mesh desired state not configured");
    }

    const now = Date.now();
    const tokenStillValid = this.token && this.tokenExpiresAt > now + 10_000;
    if (tokenStillValid && this.bootstrap) {
      return;
    }

    const tokenResponse = await createDesktopNetworkToken({
      lambdaUrl: this.desired.lambdaUrl,
      identity: this.desired.identity,
      agentWallet: this.desired.agentWallet,
      deviceId: this.desired.deviceId,
      chainId: this.desired.identity.chainId,
    });
    this.token = tokenResponse.token;
    this.tokenExpiresAt = tokenResponse.expiresAt;
    this.bootstrap = await fetchDesktopNetworkBootstrap({
      lambdaUrl: this.desired.lambdaUrl,
      networkToken: this.token,
    });
  }

  private async stopAndCleanup(): Promise<void> {
    if (!isTauriRuntime()) {
      this.invalidateToken();
      return;
    }

    const token = this.token;
    const desired = this.desired;
    try {
      if (token && desired) {
        await deleteDesktopNetworkPresence({
          lambdaUrl: desired.lambdaUrl,
          networkToken: token,
        });
      }
    } catch {
      // Ignore cleanup failures during shutdown.
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
      this.invalidateToken();
    }
  }

  private async syncOnce(): Promise<void> {
    if (!this.desired || this.syncInFlight || !isTauriRuntime()) {
      return;
    }

    this.syncInFlight = true;
    try {
      await this.ensureTokenAndBootstrap();
      if (!this.desired || !this.bootstrap || !this.token) {
        return;
      }

      const runtimeStatus = await getMeshStatusFromRuntime();
      const mustRestart = (
        !runtimeStatus.running ||
        runtimeStatus.agentWallet !== this.desired.agentWallet.toLowerCase() ||
        runtimeStatus.deviceId !== this.desired.deviceId
      );

      let activeStatus = runtimeStatus;
      if (mustRestart) {
        const request: MeshJoinRequest = {
          userAddress: this.desired.identity.userAddress,
          agentWallet: this.desired.agentWallet,
          sessionId: this.desired.identity.sessionId,
          composeKeyId: this.desired.identity.composeKeyId,
          deviceId: this.desired.deviceId,
          chainId: this.desired.identity.chainId,
          gossipTopic: this.bootstrap.bootstrap.gossipTopic,
          bootstrapMultiaddrs: this.bootstrap.bootstrap.bootstrapMultiaddrs,
          relayMultiaddrs: this.bootstrap.bootstrap.relayMultiaddrs,
        };
        activeStatus = await startMeshRuntime(request);
      }

      if (activeStatus.running && activeStatus.peerId) {
        await upsertDesktopNetworkPresence({
          lambdaUrl: this.desired.lambdaUrl,
          networkToken: this.token,
          payload: {
            peerId: activeStatus.peerId,
            announceMultiaddrs: activeStatus.listenMultiaddrs,
            metadata: {
              runtime: "compose-desktop-tauri",
              mode: "dormant-enabled",
            },
            ttlSeconds: this.bootstrap.bootstrap.presenceTtlSeconds,
          },
        });
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
      if (message.toLowerCase().includes("token")) {
        this.invalidateToken();
      }
    } finally {
      this.syncInFlight = false;
    }
  }
}

export const desktopMeshService = new DesktopMeshService();
