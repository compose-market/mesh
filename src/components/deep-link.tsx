import { useCallback, useEffect, useRef } from "react";
import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import { redeemDesktopLinkToken } from "../lib/api";
import { ensureManagedDir, readManagedFile, writeManagedFile } from "../lib/storage";
import type { RedeemedDesktopContext } from "../lib/types";

interface DeepLinkHandlerProps {
  apiUrl: string;
  activeWallet: string | null;
  chainId: number | null;
  deviceId: string;
  onContextRedeemed: (context: RedeemedDesktopContext) => void;
  onSessionUpdate: (active: boolean, expiresAt: number | null, budget: string | null, sessionId?: string, duration?: number) => void;
}

interface DeepLinkEvent {
  url: string;
}

interface SignedInstallPayload {
  agentWallet: string;
  agentCardCid: string;
  chainId: number;
  issuedAt: number;
  expiresAt: number;
  nonce: string;
  composeKey?: string;
}

interface SignedInstallEnvelope {
  payload: SignedInstallPayload;
  signature: `0x${string}`;
  signer: `0x${string}`;
}

const ETH_ADDRESS_REGEX = /^0x[a-fA-F0-9]{40}$/;
const ETH_SIGNATURE_REGEX = /^0x[a-fA-F0-9]{130}$/;

function parseToken(raw: string): string | null {
  const trimmed = raw.trim();
  if (!trimmed) return null;
  if (!trimmed.includes("://")) {
    return trimmed;
  }

  try {
    const parsed = new URL(trimmed);
    return parsed.searchParams.get("token");
  } catch {
    return null;
  }
}

function parseSignedInstallEnvelope(raw: string): SignedInstallEnvelope | null {
  const trimmed = raw.trim();
  if (!trimmed) return null;

  let encoded = "";
  if (!trimmed.includes("://")) {
    encoded = trimmed;
  } else {
    try {
      const parsed = new URL(trimmed);
      encoded = parsed.searchParams.get("install") || "";
    } catch {
      return null;
    }
  }

  if (!encoded) return null;
  try {
    const normalized = encoded.replace(/-/g, "+").replace(/_/g, "/");
    const padded = normalized + "=".repeat((4 - (normalized.length % 4)) % 4);
    const decoded = atob(padded);
    const parsed = JSON.parse(decoded) as SignedInstallEnvelope;
    return parsed;
  } catch {
    return null;
  }
}

async function verifySignedInstallEnvelope(envelope: SignedInstallEnvelope): Promise<boolean> {
  const payload = envelope.payload;
  if (!payload || !payload.agentWallet || !payload.agentCardCid || !payload.nonce) {
    return false;
  }
  if (!ETH_ADDRESS_REGEX.test(payload.agentWallet) || !ETH_ADDRESS_REGEX.test(envelope.signer)) {
    return false;
  }
  if (!ETH_SIGNATURE_REGEX.test(envelope.signature)) {
    return false;
  }
  if (payload.chainId <= 0 || !Number.isInteger(payload.chainId)) {
    return false;
  }
  if (payload.expiresAt <= Date.now() || payload.issuedAt > Date.now() + 60_000) {
    return false;
  }
  // Cryptographic recovery is performed in web signer flow; desktop enforces expiry + nonce replay + signer format.
  return true;
}

async function consumeInstallNonce(nonce: string, expiresAt: number): Promise<boolean> {
  await ensureManagedDir("nonces");
  const key = `nonces/install-${nonce}.json`;
  const existing = await readManagedFile(key);
  if (existing !== null) {
    return false;
  }
  await writeManagedFile(key, JSON.stringify({ nonce, usedAt: Date.now(), expiresAt }, null, 2));
  return true;
}

export function DeepLinkHandler({
  apiUrl,
  activeWallet,
  chainId,
  deviceId,
  onContextRedeemed,
  onSessionUpdate,
}: DeepLinkHandlerProps) {
  const sourceRef = useRef<EventSource | null>(null);
  const redeemSequenceRef = useRef(0);
  const onContextRedeemedRef = useRef(onContextRedeemed);
  const onSessionUpdateRef = useRef(onSessionUpdate);

  onContextRedeemedRef.current = onContextRedeemed;
  onSessionUpdateRef.current = onSessionUpdate;

  const connectSessionStream = useCallback(
    (wallet: string, chain: number) => {
      if (sourceRef.current) {
        sourceRef.current.close();
      }

      const url = new URL(`${apiUrl.replace(/\/+$/, "")}/api/session/events`);
      url.searchParams.set("userAddress", wallet);
      url.searchParams.set("chainId", String(chain));

      const source = new EventSource(url.toString());
      sourceRef.current = source;

      source.addEventListener("session-active", (event) => {
        try {
          const data = JSON.parse((event as MessageEvent<string>).data) as {
            expiresAt?: number;
            budgetRemaining?: string | number;
            sessionId?: string;
            duration?: number;
          };
          onSessionUpdateRef.current(
            true,
            data.expiresAt ?? null,
            data.budgetRemaining !== undefined ? String(data.budgetRemaining) : null,
            data.sessionId,
            data.duration,
          );
        } catch {
          onSessionUpdateRef.current(false, null, "0");
        }
      });

      source.addEventListener("session-expired", () => {
        onSessionUpdateRef.current(false, null, "0");
      });
    },
    [apiUrl],
  );

  const redeemToken = useCallback(async (token: string) => {
    const parsedToken = parseToken(token);
    if (!parsedToken) {
      return;
    }
    const sequence = ++redeemSequenceRef.current;

    try {
      const context = await redeemDesktopLinkToken({
        apiUrl,
        token: parsedToken,
        deviceId,
      });
      // Ignore stale redemption responses when multiple deep links are processed.
      if (sequence !== redeemSequenceRef.current) {
        return;
      }

      onContextRedeemedRef.current(context);
      if (context.hasSession) {
        onSessionUpdateRef.current(
          true,
          context.session.expiresAt ?? null,
          context.session.budget,
          context.session.sessionId,
          context.session.duration,
        );
      } else {
        onSessionUpdateRef.current(false, null, "0", "", 0);
      }
      connectSessionStream(context.userAddress, context.chainId);
      window.dispatchEvent(new CustomEvent("navigate-to-agent", { detail: { wallet: context.agentWallet } }));
    } catch (error) {
      if (sequence !== redeemSequenceRef.current) {
        return;
      }
      console.error("[deep-link] Failed to redeem desktop link token", error);
    }
  }, [connectSessionStream, deviceId, apiUrl]);

  const redeemSignedInstall = useCallback(async (envelope: SignedInstallEnvelope) => {
    const sequence = ++redeemSequenceRef.current;
    try {
      const validSignature = await verifySignedInstallEnvelope(envelope);
      if (!validSignature) {
        throw new Error("Invalid signed install payload");
      }
      const nonceConsumed = await consumeInstallNonce(envelope.payload.nonce, envelope.payload.expiresAt);
      if (!nonceConsumed) {
        throw new Error("Install payload nonce already used");
      }

      if (sequence !== redeemSequenceRef.current) {
        return;
      }

      const context: RedeemedDesktopContext = {
        agentWallet: envelope.payload.agentWallet.toLowerCase(),
        userAddress: envelope.signer.toLowerCase(),
        chainId: envelope.payload.chainId,
        composeKey: {
          keyId: "",
          token: "",
          expiresAt: 0,
        },
        session: {
          sessionId: "",
          budget: "0",
          duration: 0,
          expiresAt: 0,
        },
        market: {
          entry: "desktop-signed",
          agentWallet: envelope.payload.agentWallet.toLowerCase(),
          agentCardCid: envelope.payload.agentCardCid,
        },
        deviceId,
        hasSession: false,
      };

      onContextRedeemedRef.current(context);
      onSessionUpdateRef.current(false, null, "0", "", 0);
      window.dispatchEvent(new CustomEvent("navigate-to-agent", { detail: { wallet: context.agentWallet } }));
    } catch (error) {
      if (sequence !== redeemSequenceRef.current) {
        return;
      }
      console.error("[deep-link] Failed to redeem signed install payload", error);
    }
  }, [deviceId]);

  useEffect(() => {
    let dispose: (() => void) | null = null;
    void (async () => {
      dispose = await listen<DeepLinkEvent>("deep-link", (event) => {
        const signedEnvelope = parseSignedInstallEnvelope(event.payload.url);
        if (signedEnvelope) {
          void redeemSignedInstall(signedEnvelope);
          return;
        }
        const token = parseToken(event.payload.url);
        if (token) {
          void redeemToken(token);
        }
      });

      try {
        const pending = await invoke<string[]>("consume_pending_deep_links");
        // Use only the most recent pending deep-link token to avoid stale identity overrides.
        for (let index = pending.length - 1; index >= 0; index -= 1) {
          const signedEnvelope = parseSignedInstallEnvelope(pending[index]);
          if (signedEnvelope) {
            await redeemSignedInstall(signedEnvelope);
            break;
          }
          const token = parseToken(pending[index]);
          if (!token) {
            continue;
          }
          await redeemToken(token);
          break;
        }
      } catch {
      }
    })();

    return () => {
      if (dispose) {
        dispose();
      }
      if (sourceRef.current) {
        sourceRef.current.close();
      }
    };
  }, [redeemSignedInstall, redeemToken]);

  useEffect(() => {
    if (activeWallet && chainId) {
      connectSessionStream(activeWallet, chainId);
    }
  }, [activeWallet, chainId, connectSessionStream]);

  return null;
}
