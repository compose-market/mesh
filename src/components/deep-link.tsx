import { useCallback, useEffect, useRef } from "react";
import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import { redeemLocalLinkToken } from "../lib/api";
import type { RedeemedLocalContext } from "../lib/types";

interface DeepLinkHandlerProps {
  apiUrl: string;
  activeWallet: string | null;
  chainId: number | null;
  sessionActive: boolean;
  deviceId: string;
  onContextRedeemed: (context: RedeemedLocalContext) => void;
  onSessionUpdate: (update: {
    active: boolean;
    expiresAt?: number | null;
    budgetLimit?: string | null;
    budgetUsed?: string | null;
    budgetRemaining?: string | null;
    sessionId?: string | null;
    duration?: number | null;
    chainId?: number | null;
  }) => void;
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
  sessionActive,
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
            budgetLimit?: string | number;
            budgetUsed?: string | number;
            budgetRemaining?: string | number;
            chainId?: number;
          };
          onSessionUpdateRef.current({
            active: true,
            expiresAt: data.expiresAt ?? null,
            budgetLimit: data.budgetLimit !== undefined ? String(data.budgetLimit) : undefined,
            budgetUsed: data.budgetUsed !== undefined ? String(data.budgetUsed) : undefined,
            budgetRemaining: data.budgetRemaining !== undefined ? String(data.budgetRemaining) : undefined,
            chainId: typeof data.chainId === "number" ? data.chainId : chain,
          });
        } catch (error) {
          console.warn("[deep-link] Ignoring malformed session-active event", error);
        }
      });

      source.addEventListener("session-expired", (event) => {
        try {
          const data = JSON.parse((event as MessageEvent<string>).data) as { chainId?: number };
          onSessionUpdateRef.current({
            active: false,
            budgetRemaining: "0",
            chainId: typeof data.chainId === "number" ? data.chainId : chain,
          });
        } catch {
          onSessionUpdateRef.current({
            active: false,
            budgetRemaining: "0",
            chainId: chain,
          });
        }
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
      });
      // Ignore stale redemption responses when multiple deep links are processed.
      if (sequence !== redeemSequenceRef.current) {
        return;
      }

      onContextRedeemedRef.current(context);
      window.dispatchEvent(new CustomEvent("navigate-to-agent", { detail: { wallet: context.agentWallet } }));
    } catch (error) {
      if (sequence !== redeemSequenceRef.current) {
        return;
      }
      console.error("[deep-link] Failed to redeem local link token", error);
      const msg = error instanceof Error ? error.message : "Failed to redeem local link token";
      onErrorRef.current(msg);
    }
  }, [deviceId, apiUrl]);

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
    if (sessionActive && activeWallet && chainId) {
      connectSessionStream(activeWallet, chainId);
      return;
    }

    if (sourceRef.current) {
      sourceRef.current.close();
      sourceRef.current = null;
    }
  }, [activeWallet, chainId, connectSessionStream, sessionActive]);

  return null;
}
