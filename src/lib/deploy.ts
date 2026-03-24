import type {
  LocalIdentityContext,
  LocalRuntimeState,
  LinkedDeploymentIntent,
  RedeemedLocalContext,
} from "./types";

const ETH_ADDRESS_REGEX = /^0x[a-f0-9]{40}$/;
const LOCAL_CHAIN_ACCENT_BY_ID: Record<number, "red" | "blue" | "cyan" | "yellow"> = {
  43113: "red",
  43114: "red",
  338: "blue",
  25: "blue",
  421614: "cyan",
  42161: "cyan",
  97: "yellow",
  56: "yellow",
};

export interface RecentValueGate {
  claim: (value: string, now?: number) => boolean;
}

export interface LocalWalletDisplay {
  shortAddress: string;
  chainLabel: string;
  accentTone: "red" | "blue" | "cyan" | "yellow" | "default";
}

function normalizeWallet(value: string | null | undefined): string | null {
  if (!value) {
    return null;
  }
  const normalized = value.trim().toLowerCase();
  return ETH_ADDRESS_REGEX.test(normalized) ? normalized : null;
}

function normalizeCid(value: string | null | undefined): string | null {
  if (!value) {
    return null;
  }
  const normalized = value.trim();
  return normalized.length >= 32 ? normalized : null;
}

export function createRecentValueGate(windowMs = 15_000): RecentValueGate {
  const seen = new Map<string, number>();
  const ttlMs = Math.max(1, windowMs);

  return {
    claim(value: string, now = Date.now()) {
      const normalized = value.trim();
      if (!normalized) {
        return false;
      }

      const cutoff = now - ttlMs;
      for (const [key, timestamp] of seen) {
        if (timestamp <= cutoff) {
          seen.delete(key);
        }
      }

      const lastSeenAt = seen.get(normalized);
      if (lastSeenAt !== undefined && now - lastSeenAt < ttlMs) {
        return false;
      }

      seen.set(normalized, now);
      return true;
    },
  };
}

export function createLocalWalletDisplay(
  identity: Pick<LocalIdentityContext, "userAddress" | "chainId">,
): LocalWalletDisplay {
  return {
    shortAddress: `${identity.userAddress.slice(0, 6)}...${identity.userAddress.slice(-4)}`,
    chainLabel: `Chain ID ${identity.chainId}`,
    accentTone: LOCAL_CHAIN_ACCENT_BY_ID[identity.chainId] || "default",
  };
}

export function resolveInheritedLocalChainId(identityChainId: number, _sessionChainId?: number): number {
  return identityChainId;
}

export function deriveLinkedDeploymentIntent(context: RedeemedLocalContext): LinkedDeploymentIntent | null {
  const agentWallet = normalizeWallet(context.market?.agentWallet);
  if (!agentWallet || !Number.isFinite(context.chainId) || context.chainId <= 0) {
    return null;
  }

  const source = context.market?.entry === "local-signed" ? "signed-install" : "local-link";

  return {
    agentWallet,
    agentCardCid: normalizeCid(context.market?.agentCardCid),
    chainId: context.chainId,
    source,
    receivedAt: Date.now(),
  };
}

export function hasDeployableLinkedIntent(intent: LinkedDeploymentIntent | null | undefined): intent is LinkedDeploymentIntent & { agentCardCid: string } {
  return Boolean(intent?.agentWallet && intent.agentCardCid);
}

export function clearLocalConnectionState(state: LocalRuntimeState): LocalRuntimeState {
  return {
    ...state,
    identity: null,
    linkedDeployment: null,
  };
}
