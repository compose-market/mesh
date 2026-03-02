import { useCallback, useEffect, useRef } from "react";
import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import { redeemDesktopLinkToken } from "../lib/api";
import type { RedeemedDesktopContext } from "../lib/types";

interface DeepLinkHandlerProps {
  lambdaUrl: string;
  activeWallet: string | null;
  chainId: number | null;
  deviceId: string;
  onContextRedeemed: (context: RedeemedDesktopContext) => void;
  onSessionUpdate: (active: boolean, expiresAt: number | null, budget: string | null, sessionId?: string, duration?: number) => void;
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
  lambdaUrl,
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

      const url = new URL(`${lambdaUrl.replace(/\/+$/, "")}/api/session/events`);
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
    [lambdaUrl],
  );

  const redeemToken = useCallback(async (token: string) => {
    const parsedToken = parseToken(token);
    if (!parsedToken) {
      return;
    }
    const sequence = ++redeemSequenceRef.current;

    try {
      const context = await redeemDesktopLinkToken({
        lambdaUrl,
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
  }, [connectSessionStream, deviceId, lambdaUrl]);

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
      connectSessionStream(activeWallet, chainId);
    }
  }, [activeWallet, chainId, connectSessionStream]);

  return null;
}
