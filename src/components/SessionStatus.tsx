import { Activity, Clock, Wallet } from "lucide-react";
import type { SessionState } from "../lib/types";

interface SessionStatusProps {
  wallet: string;
  session: SessionState;
}

export function SessionStatus({ wallet, session }: SessionStatusProps) {
  const formatAddress = (addr: string) => {
    if (!addr) return "";
    return `${addr.slice(0, 6)}...${addr.slice(-4)}`;
  };

  const formatTime = (timestamp: number | null | undefined) => {
    if (!timestamp) return "N/A";
    return new Date(timestamp).toLocaleTimeString();
  };

  const formatBudget = (budgetStr: string | null | undefined) => {
    if (!budgetStr) return "N/A";
    try {
      const wei = BigInt(budgetStr);
      const usdc = Number(wei) / 1_000_000;
      return `$${usdc.toFixed(2)}`;
    } catch {
      return budgetStr;
    }
  };

  return (
    <div className="session-status">
      <div className="session-item">
        <Wallet size={16} />
        <span className="label">Wallet:</span>
        <span className="value">{formatAddress(wallet)}</span>
      </div>
      <div className="session-item">
        <Activity size={16} />
        <span className="label">Session:</span>
        <span className={`value ${session.active ? "active" : "inactive"}`}>
          {session.active ? "Active" : "Inactive"}
        </span>
      </div>
      <div className="session-item">
        <Clock size={16} />
        <span className="label">Expires:</span>
        <span className="value">{formatTime(session.expiresAt)}</span>
      </div>
      <div className="session-item">
        <span className="label">Budget:</span>
        <span className="value budget">{formatBudget(session.budgetRemaining)}</span>
      </div>
    </div>
  );
}