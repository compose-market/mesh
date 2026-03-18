import { useCallback, useEffect, useRef } from "react";
import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import { redeemLocalLinkToken } from "../lib/api";
import type { RedeemedLocalContext } from "../lib/types";

interface DeepLinkHandlerProps {
  apiUrl: string;
  activeWallet: string | null;
  chainId: number | null;
  deviceId: string;
  onContextRedeemed: (context: RedeemedLocalContext) => void;
  onSessionUpdate: (active: boolean, expiresAt: number | null, budget: string | null, sessionId?: string, duration?: number) => void;
  onError: (message: string) => void;
}

interface DeepLinkEvent {
  url: string;
}

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


export function DeepLinkHandler({
  apiUrl,
  activeWallet,
  chainId,
  deviceId,
  onContextRedeemed,
  onSessionUpdate,
  onError,
}: DeepLinkHandlerProps) {
  const sourceRef = useRef<EventSource | null>(null);
  const redeemSequenceRef = useRef(0);
  const onContextRedeemedRef = useRef(onContextRedeemed);
  const onSessionUpdateRef = useRef(onSessionUpdate);
  const onErrorRef = useRef(onError);

  onContextRedeemedRef.current = onContextRedeemed;
  onSessionUpdateRef.current = onSessionUpdate;
  onErrorRef.current = onError;

  const connectSessionStream = useCallback(
    (wallet: string, chain: number, composeKeyToken?: string | null) => {
      if (!composeKeyToken) {
        if (sourceRef.current) {
          sourceRef.current.close();
          sourceRef.current = null;
        }
        return;
      }
      if (sourceRef.current) {
        sourceRef.current.close();
      }

      const url = new URL(`${apiUrl.replace(/\/+$/, "")}/api/session/events`);
      url.searchParams.set("userAddress", wallet);
      url.searchParams.set("chainId", String(chain));
      url.searchParams.set("token", composeKeyToken);

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
      const context = await redeemLocalLinkToken({
        apiUrl,
        token: parsedToken,
        deviceId,
        connectedUserAddress: activeWallet || undefined,
      });
      // Ignore stale redemption responses when multiple deep links are processed.
      if (sequence !== redeemSequenceRef.current) {
        return;
      }
      if (
        activeWallet
        && activeWallet.trim().toLowerCase() !== context.userAddress.trim().toLowerCase()
      ) {
        throw new Error("Deep-link userAddress does not match the connected Mesh wallet");
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
      connectSessionStream(context.userAddress, context.chainId, context.composeKey.token || null);
      window.dispatchEvent(new CustomEvent("navigate-to-agent", { detail: { wallet: context.agentWallet } }));
    } catch (error) {
      if (sequence !== redeemSequenceRef.current) {
        return;
      }
      console.error("[deep-link] Failed to redeem local link token", error);
      const msg = error instanceof Error ? error.message : "Failed to redeem local link token";
      onErrorRef.current(msg);
    }
  }, [activeWallet, connectSessionStream, deviceId, apiUrl]);

  useEffect(() => {
    let dispose: (() => void) | null = null;
    void (async () => {
      dispose = await listen<DeepLinkEvent>("deep-link", (event) => {
        const token = parseToken(event.payload.url);
        if (token) {
          void redeemToken(token);
        }
      });

      try {
        const pending = await invoke<string[]>("consume_pending_deep_links");
        // Use only the most recent pending deep-link token to avoid stale identity overrides.
        for (let index = pending.length - 1; index >= 0; index -= 1) {
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
  }, [redeemToken]);

  useEffect(() => {
    if (activeWallet && chainId) {
      connectSessionStream(activeWallet, chainId, null);
      return;
    }

    if (sourceRef.current) {
      sourceRef.current.close();
      sourceRef.current = null;
    }
  }, [activeWallet, chainId, connectSessionStream]);

  return null;
}
