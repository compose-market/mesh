import { useCallback, useEffect, useMemo, useState } from "react";
import {
  Check,
  ChevronDown,
  Clock,
  Copy,
  Key,
  Plus,
  Shield,
  Trash2,
  Wallet,
  Zap,
} from "lucide-react";
import {
  createSession,
  listComposeKeys,
  revokeComposeKey,
  type ComposeKeyRecord,
} from "../lib/api";
import type { DesktopIdentityContext, SessionState } from "../lib/types";

const BUDGET_PRESETS = [
  { label: "$1", value: "1000000" },
  { label: "$10", value: "10000000" },
  { label: "$50", value: "50000000" },
  { label: "$100", value: "100000000" },
] as const;

const DURATION_OPTIONS = [1, 6, 12, 24] as const;
const USDC_DECIMALS = 1_000_000n;

interface SessionIndicatorProps {
  apiUrl: string;
  identity: DesktopIdentityContext | null;
  session: SessionState;
  onRefreshSession: () => Promise<void>;
  onNotify: (type: "success" | "error", message: string) => void;
}

interface SessionBudgetDialogProps {
  open: boolean;
  apiUrl: string;
  identity: DesktopIdentityContext;
  onClose: () => void;
  onRefreshSession: () => Promise<void>;
  onNotify: (type: "success" | "error", message: string) => void;
}

interface SessionManageDialogProps {
  open: boolean;
  apiUrl: string;
  identity: DesktopIdentityContext;
  session: SessionState;
  onClose: () => void;
  onRefreshSession: () => Promise<void>;
  onNotify: (type: "success" | "error", message: string) => void;
}

interface ComposeKeyDialogProps {
  open: boolean;
  apiUrl: string;
  identity: DesktopIdentityContext;
  session: SessionState;
  onClose: () => void;
  onRefreshSession: () => Promise<void>;
  onNotify: (type: "success" | "error", message: string) => void;
}

function formatUsdc(wei: string, precision = 2): string {
  try {
    const amount = BigInt(wei);
    const safePrecision = Math.max(0, Math.min(6, precision));
    const whole = amount / USDC_DECIMALS;
    const fraction = amount % USDC_DECIMALS;
    const scale = 10n ** BigInt(safePrecision);
    const rounded = (fraction * scale + USDC_DECIMALS / 2n) / USDC_DECIMALS;
    if (safePrecision === 0) {
      return whole.toString();
    }
    const padded = rounded.toString().padStart(safePrecision, "0");
    return `${whole.toString()}.${padded}`;
  } catch {
    return "0.00";
  }
}

function formatTimeRemaining(expiresAt: number): string {
  const remaining = expiresAt - Date.now();
  if (remaining <= 0) return "Expired";

  const totalMinutes = Math.floor(remaining / (1000 * 60));
  const hours = Math.floor(totalMinutes / 60);
  const minutes = totalMinutes % 60;
  if (hours >= 24) {
    const days = Math.floor(hours / 24);
    return `${days}d ${hours % 24}h`;
  }
  if (hours > 0) {
    return `${hours}h ${minutes}m`;
  }
  return `${minutes}m`;
}

function isActiveKey(key: ComposeKeyRecord): boolean {
  return !key.revokedAt && key.expiresAt > Date.now();
}

export function SessionIndicator({
  apiUrl,
  identity,
  session,
  onRefreshSession,
  onNotify,
}: SessionIndicatorProps) {
  const [menuOpen, setMenuOpen] = useState(false);
  const [budgetDialogOpen, setBudgetDialogOpen] = useState(false);
  const [manageDialogOpen, setManageDialogOpen] = useState(false);
  const [keyDialogOpen, setKeyDialogOpen] = useState(false);

  if (!identity) {
    return null;
  }

  if (!session.active) {
    return (
      <>
        <button
          className="session-indicator-btn"
          onClick={() => setBudgetDialogOpen(true)}
          title="Start Session"
        >
          <Zap size={14} />
          Start Session
        </button>
        <SessionBudgetDialog
          open={budgetDialogOpen}
          apiUrl={apiUrl}
          identity={identity}
          onClose={() => setBudgetDialogOpen(false)}
          onRefreshSession={onRefreshSession}
          onNotify={onNotify}
        />
      </>
    );
  }

  const expiresLabel = session.expiresAt ? formatTimeRemaining(session.expiresAt) : "Never";
  const budgetLabel = `$${formatUsdc(session.budgetRemaining || "0", 2)}`;

  return (
    <>
      <div className="session-menu">
        <button
          className="session-indicator-btn active"
          onClick={() => setMenuOpen((open) => !open)}
          title="Session Actions"
        >
          <Zap size={14} />
          Session
          <span className="session-indicator-mobile">{budgetLabel}</span>
          <ChevronDown size={12} />
        </button>

        {menuOpen ? (
          <div className="session-menu-dropdown">
            <div className="session-menu-header">
              <div className="session-menu-row">
                <span>Budget</span>
                <strong>{budgetLabel}</strong>
              </div>
              <div className="session-menu-row">
                <span>Expires</span>
                <strong>{expiresLabel}</strong>
              </div>
            </div>
            <button
              className="session-menu-item"
              onClick={() => {
                setKeyDialogOpen(true);
                setMenuOpen(false);
              }}
            >
              <Key size={14} />
              Generate API Key
            </button>
            <button
              className="session-menu-item"
              onClick={() => {
                setManageDialogOpen(true);
                setMenuOpen(false);
              }}
            >
              <Wallet size={14} />
              Manage Sessions
            </button>
          </div>
        ) : null}
      </div>

      <SessionManageDialog
        open={manageDialogOpen}
        apiUrl={apiUrl}
        identity={identity}
        session={session}
        onClose={() => setManageDialogOpen(false)}
        onRefreshSession={onRefreshSession}
        onNotify={onNotify}
      />

      <ComposeKeyDialog
        open={keyDialogOpen}
        apiUrl={apiUrl}
        identity={identity}
        session={session}
        onClose={() => setKeyDialogOpen(false)}
        onRefreshSession={onRefreshSession}
        onNotify={onNotify}
      />
    </>
  );
}

export function SessionBudgetDialog({
  open,
  apiUrl,
  identity,
  onClose,
  onRefreshSession,
  onNotify,
}: SessionBudgetDialogProps) {
  const [selectedBudget, setSelectedBudget] = useState<string>(BUDGET_PRESETS[1].value);
  const [durationHours, setDurationHours] = useState<number>(24);
  const [creating, setCreating] = useState(false);
  const [error, setError] = useState<string | null>(null);

  if (!open) {
    return null;
  }

  const handleCreateSession = async () => {
    setCreating(true);
    setError(null);
    try {
      const expiresAt = Date.now() + durationHours * 60 * 60 * 1000;
      await createSession({
        apiUrl,
        userAddress: identity.userAddress,
        payload: {
          budgetLimit: Number.parseInt(selectedBudget, 10),
          expiresAt,
          chainId: identity.chainId,
          name: `Desktop Session ${new Date().toISOString().slice(0, 10)}`,
        },
      });
      await onRefreshSession();
      onNotify("success", "Session created");
      onClose();
    } catch (createError) {
      const message = createError instanceof Error ? createError.message : "Failed to create session";
      setError(message);
      onNotify("error", "Failed to create session");
    } finally {
      setCreating(false);
    }
  };

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal session-modal" onClick={(event) => event.stopPropagation()}>
        <div className="modal-header">
          <h3>
            <Shield size={18} />
            Session Budget
          </h3>
          <button className="close-btn" onClick={onClose} aria-label="Close">
            ×
          </button>
        </div>

        <p className="session-modal-copy">
          Set a spending limit to skip wallet signatures for each AI call.
          One approval, unlimited inference within your budget.
        </p>

        <div className="session-section">
          <label>Budget Limit (USDC)</label>
          <div className="session-presets">
            {BUDGET_PRESETS.map((preset) => (
              <button
                key={preset.value}
                className={`session-preset-btn ${selectedBudget === preset.value ? "active" : ""}`}
                onClick={() => setSelectedBudget(preset.value)}
                type="button"
              >
                {preset.label}
              </button>
            ))}
          </div>
        </div>

        <div className="session-section">
          <label>
            <Clock size={13} />
            Session Duration
          </label>
          <div className="session-presets">
            {DURATION_OPTIONS.map((hours) => (
              <button
                key={hours}
                className={`session-preset-btn ${durationHours === hours ? "active" : ""}`}
                onClick={() => setDurationHours(hours)}
                type="button"
              >
                {hours}h
              </button>
            ))}
          </div>
        </div>

        <div className="session-info-box">
          <div className="session-info-row">
            <span>Max Spend</span>
            <strong>${formatUsdc(selectedBudget, 2)} USDC</strong>
          </div>
          <div className="session-info-row">
            <span>Expires After</span>
            <strong>{durationHours} hours</strong>
          </div>
          <div className="session-info-row">
            <span>Approvals Required</span>
            <strong>1 (now)</strong>
          </div>
        </div>

        {error ? <div className="session-error">{error}</div> : null}

        <div className="modal-footer">
          <button className="secondary" onClick={onClose} disabled={creating}>
            Cancel
          </button>
          <button className="primary session-action-btn" onClick={() => void handleCreateSession()} disabled={creating}>
            {creating ? "Creating..." : "Approve & Start Session"}
          </button>
        </div>
      </div>
    </div>
  );
}

export function SessionManageDialog({
  open,
  apiUrl,
  identity,
  session,
  onClose,
  onRefreshSession,
  onNotify,
}: SessionManageDialogProps) {
  const [keys, setKeys] = useState<ComposeKeyRecord[]>([]);
  const [loading, setLoading] = useState(false);
  const [createDialogOpen, setCreateDialogOpen] = useState(false);
  const [copiedKeyId, setCopiedKeyId] = useState<string | null>(null);

  const activeKeys = useMemo(
    () => keys.filter((key) => isActiveKey(key)),
    [keys],
  );

  const fetchKeys = useCallback(async () => {
    setLoading(true);
    try {
      const response = await listComposeKeys({
        apiUrl,
        userAddress: identity.userAddress,
      });
      setKeys(response);
    } catch (error) {
      console.error("[session] Failed to list keys", error);
      onNotify("error", "Failed to fetch sessions");
    } finally {
      setLoading(false);
    }
  }, [identity.userAddress, apiUrl, onNotify]);

  useEffect(() => {
    if (open) {
      void fetchKeys();
    }
  }, [open, fetchKeys]);

  if (!open) {
    return null;
  }

  const handleRevoke = async (keyId: string) => {
    const success = await revokeComposeKey({
      apiUrl,
      userAddress: identity.userAddress,
      keyId,
    });
    if (!success) {
      onNotify("error", "Failed to revoke session");
      return;
    }
    await Promise.all([fetchKeys(), onRefreshSession()]);
    onNotify("success", "Session revoked");
  };

  const handleCopyMaskedKey = async (keyId: string) => {
    const masked = `compose-${keyId.slice(0, 8)}***`;
    await navigator.clipboard.writeText(masked);
    setCopiedKeyId(keyId);
    onNotify("success", "Key ID copied");
    window.setTimeout(() => setCopiedKeyId(null), 2000);
  };

  return (
    <>
      <div className="modal-overlay" onClick={onClose}>
        <div className="modal session-modal" onClick={(event) => event.stopPropagation()}>
          <div className="modal-header">
            <h3>
              <Wallet size={18} />
              Manage Sessions
            </h3>
            <button className="close-btn" onClick={onClose} aria-label="Close">
              ×
            </button>
          </div>

          <p className="session-modal-copy">
            View and manage your active API sessions and keys.
          </p>

          {session.active ? (
            <div className="session-current-card">
              <div className="session-current-title">
                <Zap size={14} />
                Current Session
              </div>
              <div className="session-current-grid">
                <div>
                  <span>Remaining</span>
                  <strong>${formatUsdc(session.budgetRemaining || "0", 2)}</strong>
                </div>
                <div>
                  <span>Expires</span>
                  <strong>{session.expiresAt ? formatTimeRemaining(session.expiresAt) : "Never"}</strong>
                </div>
              </div>
            </div>
          ) : null}

          <div className="session-keys-header">
            <span>API Keys</span>
            <button className="secondary compact" onClick={() => setCreateDialogOpen(true)}>
              <Plus size={14} />
              New Key
            </button>
          </div>

          {loading ? (
            <div className="session-loading">Loading sessions...</div>
          ) : activeKeys.length === 0 ? (
            <div className="session-empty-state">
              No API keys created yet.
              <button onClick={() => setCreateDialogOpen(true)}>Generate your first key</button>
            </div>
          ) : (
            <div className="session-keys-list">
              {activeKeys.map((key) => (
                <div key={key.keyId} className="session-key-card">
                  <div className="session-key-header">
                    <strong>{key.name || "Unnamed Key"}</strong>
                    <div className="session-key-actions">
                      <button
                        className="icon-btn"
                        onClick={() => void handleCopyMaskedKey(key.keyId)}
                        title="Copy key ID"
                      >
                        {copiedKeyId === key.keyId ? <Check size={13} /> : <Copy size={13} />}
                      </button>
                      <button
                        className="icon-btn danger"
                        onClick={() => void handleRevoke(key.keyId)}
                        title="Revoke key"
                      >
                        <Trash2 size={13} />
                      </button>
                    </div>
                  </div>
                  <div className="session-key-id">compose-{key.keyId.slice(0, 8)}***</div>
                  <div className="session-key-grid">
                    <div>
                      <span>Budget</span>
                      <strong>
                        ${formatUsdc(key.budgetRemaining, 2)} / ${formatUsdc(key.budgetLimit, 2)}
                      </strong>
                    </div>
                    <div>
                      <span>Expires</span>
                      <strong>{formatTimeRemaining(key.expiresAt)}</strong>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      <ComposeKeyDialog
        open={createDialogOpen}
        apiUrl={apiUrl}
        identity={identity}
        session={session}
        onClose={() => {
          setCreateDialogOpen(false);
          void fetchKeys();
        }}
        onRefreshSession={onRefreshSession}
        onNotify={onNotify}
      />
    </>
  );
}

export function ComposeKeyDialog({
  open,
  apiUrl,
  identity,
  session,
  onClose,
  onRefreshSession,
  onNotify,
}: ComposeKeyDialogProps) {
  const [keyName, setKeyName] = useState("Desktop");
  const [generating, setGenerating] = useState(false);
  const [generatedKey, setGeneratedKey] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);

  if (!open) {
    return null;
  }

  const handleClose = () => {
    setKeyName("Desktop");
    setGeneratedKey(null);
    setCopied(false);
    onClose();
  };

  const handleGenerate = async () => {
    if (!session.active || !session.expiresAt) {
      onNotify("error", "Start a session first");
      return;
    }

    setGenerating(true);
    try {
      const response = await createSession({
        apiUrl,
        userAddress: identity.userAddress,
        payload: {
          budgetLimit: Number.parseInt(session.budgetRemaining || "0", 10),
          expiresAt: session.expiresAt,
          chainId: identity.chainId,
          name: keyName.trim() || "Desktop",
        },
      });
      setGeneratedKey(response.token);
      await onRefreshSession();
      onNotify("success", "API key generated");
    } catch (error) {
      console.error("[session] Failed to generate key", error);
      onNotify("error", "Failed to generate API key");
    } finally {
      setGenerating(false);
    }
  };

  const handleCopy = async () => {
    if (!generatedKey) return;
    await navigator.clipboard.writeText(generatedKey);
    setCopied(true);
    onNotify("success", "Copied to clipboard");
    window.setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="modal-overlay" onClick={handleClose}>
      <div className="modal session-modal" onClick={(event) => event.stopPropagation()}>
        <div className="modal-header">
          <h3>
            <Key size={18} />
            Generate API Key
          </h3>
          <button className="close-btn" onClick={handleClose} aria-label="Close">
            ×
          </button>
        </div>

        {!generatedKey ? (
          <>
            <p className="session-modal-copy">
              Create a key for external tools like Cursor or OpenCode.
              Uses your current session budget.
            </p>
            <div className="session-section">
              <label>Key Name</label>
              <input
                type="text"
                value={keyName}
                onChange={(event) => setKeyName(event.target.value)}
                placeholder="e.g., Cursor, OpenCode"
              />
            </div>

            <div className="session-info-box">
              <div className="session-info-row">
                <span>Budget</span>
                <strong>${formatUsdc(session.budgetRemaining || "0", 2)} USDC</strong>
              </div>
              <div className="session-info-row">
                <span>Expires</span>
                <strong>{session.expiresAt ? new Date(session.expiresAt).toLocaleString() : "Never"}</strong>
              </div>
            </div>

            <div className="modal-footer">
              <button className="secondary" onClick={handleClose} disabled={generating}>
                Cancel
              </button>
              <button className="primary session-action-btn" onClick={() => void handleGenerate()} disabled={generating}>
                {generating ? "Generating..." : "Generate Key"}
              </button>
            </div>
          </>
        ) : (
          <>
            <div className="session-section">
              <label>Your API Key</label>
              <div className="session-generated-row">
                <input type="text" readOnly value={generatedKey} />
                <button className="secondary compact" onClick={() => void handleCopy()}>
                  {copied ? <Check size={13} /> : <Copy size={13} />}
                </button>
              </div>
            </div>

            <div className="session-warning">
              Save this key now. You will not be able to see it again.
            </div>

            <div className="session-info-box">
              <div className="session-info-row">
                <span>Usage</span>
                <strong>Authorization: Bearer compose-...</strong>
              </div>
            </div>

            <div className="modal-footer">
              <button className="secondary" onClick={handleClose}>
                Close
              </button>
              <button className="primary session-action-btn" onClick={() => void handleCopy()}>
                {copied ? "Copied!" : "Copy to Clipboard"}
              </button>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
