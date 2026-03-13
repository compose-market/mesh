import type { LinkedDeploymentIntent, RedeemedDesktopContext } from "./types";

const ETH_ADDRESS_REGEX = /^0x[a-f0-9]{40}$/;

export interface RecentValueGate {
  claim: (value: string, now?: number) => boolean;
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

export function deriveLinkedDeploymentIntent(context: RedeemedDesktopContext): LinkedDeploymentIntent | null {
  const agentWallet = normalizeWallet(context.market?.agentWallet);
  if (!agentWallet || !Number.isFinite(context.chainId) || context.chainId <= 0) {
    return null;
  }

  return {
    agentWallet,
    agentCardCid: normalizeCid(context.market?.agentCardCid),
    chainId: context.chainId,
    source: context.market?.entry === "desktop-signed" ? "signed-install" : "desktop-link",
    receivedAt: Date.now(),
  };
}

export function hasDeployableLinkedIntent(intent: LinkedDeploymentIntent | null | undefined): intent is LinkedDeploymentIntent & { agentCardCid: string } {
  return Boolean(intent?.agentWallet && intent.agentCardCid);
}
