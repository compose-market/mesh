import { useCallback, useEffect, useMemo, useState } from "react";
import { Check, ChevronDown, Clock, Copy, Key, Plus, Shield, Trash2, Wallet, Zap } from "lucide-react";
import {
  ComposeKeyDialogShell,
  SessionBudgetDialogShell,
  SessionIndicatorShell,
  SessionManageDialogShell,
  type SessionManageKey,
  type SessionSummaryRow,
} from "@compose-market/theme/session";
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
    return `${whole.toString()}.${rounded.toString().padStart(safePrecision, "0")}`;
  } catch {
    return "0.00";
  }
}

function formatTimeRemaining(expiresAt: number): string {
  const remaining = expiresAt - Date.now();
  if (remaining <= 0) {
    return "Expired";
  }

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
  const [budgetDialogOpen, setBudgetDialogOpen] = useState(false);
  const [manageDialogOpen, setManageDialogOpen] = useState(false);
  const [keyDialogOpen, setKeyDialogOpen] = useState(false);

  if (!identity) {
    return null;
  }

  return (
    <>
      <SessionIndicatorShell
        active={session.active}
        budgetLabel={`$${formatUsdc(session.budgetRemaining || "0", 2)}`}
        expiresLabel={session.expiresAt ? formatTimeRemaining(session.expiresAt) : "Never"}
        mobileBudgetLabel={`$${formatUsdc(session.budgetRemaining || "0", 0)}`}
        startLabel="Start Session"
        activeLabel="Session"
        leadingIcon={<Zap size={14} />}
        trailingIcon={<ChevronDown size={12} />}
        keyIcon={<Key size={14} />}
        manageIcon={<Wallet size={14} />}
        onStart={() => setBudgetDialogOpen(true)}
        onOpenKey={() => setKeyDialogOpen(true)}
        onOpenManage={() => setManageDialogOpen(true)}
      />

      <SessionBudgetDialog
        open={budgetDialogOpen}
        apiUrl={apiUrl}
        identity={identity}
        onClose={() => setBudgetDialogOpen(false)}
        onRefreshSession={onRefreshSession}
        onNotify={onNotify}
      />

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

function SessionBudgetDialog({
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

  const summaryRows: SessionSummaryRow[] = useMemo(() => [
    {
      label: "Max Spend",
      value: `$${formatUsdc(selectedBudget, 2)} USDC`,
    },
    {
      label: "Expires After",
      value: `${durationHours} hours`,
    },
    {
      label: "Approvals Required",
      value: "1 (now)",
    },
  ], [durationHours, selectedBudget]);

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
          purpose: "session",
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
    <SessionBudgetDialogShell
      open={open}
      title="Session Budget"
      subtitle="Set a spending limit to skip wallet signatures for each AI call. One approval, unlimited inference within your budget."
      titleIcon={<Shield size={18} />}
      budgetLabel="Budget Limit (USDC)"
      durationLabel="Session Duration"
      durationIcon={<Clock size={14} />}
      budgetChoices={BUDGET_PRESETS.map((preset) => ({
        label: preset.label,
        active: selectedBudget === preset.value,
        onSelect: () => setSelectedBudget(preset.value),
      }))}
      durationChoices={DURATION_OPTIONS.map((hours) => ({
        label: `${hours}h`,
        active: durationHours === hours,
        onSelect: () => setDurationHours(hours),
      }))}
      summaryRows={summaryRows}
      error={error || undefined}
      onClose={onClose}
      onSubmit={() => void handleCreateSession()}
      submitting={creating}
      submitLabel="Approve & Start Session"
      submittingLabel="Creating..."
    />
  );
}

function SessionManageDialog({
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
    () => keys.filter((key) => key.purpose === "api" && isActiveKey(key)),
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
  }, [apiUrl, identity.userAddress, onNotify]);

  useEffect(() => {
    if (open) {
      void fetchKeys();
    }
  }, [fetchKeys, open]);

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
    await navigator.clipboard.writeText(`compose-${keyId.slice(0, 8)}***`);
    setCopiedKeyId(keyId);
    onNotify("success", "Key ID copied");
    window.setTimeout(() => setCopiedKeyId(null), 2000);
  };

  const mappedKeys: SessionManageKey[] = activeKeys.map((key) => ({
    id: key.keyId,
    title: key.name || "Unnamed Key",
    maskedValue: `compose-${key.keyId.slice(0, 8)}***`,
    summaryRows: [
      {
        label: "Budget",
        value: `$${formatUsdc(key.budgetRemaining, 2)} / $${formatUsdc(key.budgetLimit, 2)}`,
      },
      {
        label: "Expires",
        value: formatTimeRemaining(key.expiresAt),
      },
    ],
    copyIcon: <Copy size={14} />,
    copiedIcon: <Check size={14} />,
    revokeIcon: <Trash2 size={14} />,
    copied: copiedKeyId === key.keyId,
    onCopy: () => void handleCopyMaskedKey(key.keyId),
    onRevoke: () => void handleRevoke(key.keyId),
  }));

  return (
    <>
      <SessionManageDialogShell
        open={open}
        title="Manage Sessions"
        subtitle="View and manage your active API sessions and keys."
        titleIcon={<Wallet size={18} />}
        currentSessionTitle={session.active ? "Current Session" : undefined}
        currentSessionIcon={session.active ? <Zap size={14} /> : undefined}
        currentSessionRows={session.active ? [
          {
            label: "Remaining",
            value: `$${formatUsdc(session.budgetRemaining || "0", 2)}`,
          },
          {
            label: "Expires",
            value: session.expiresAt ? formatTimeRemaining(session.expiresAt) : "Never",
          },
        ] : []}
        sectionLabel="API Keys"
        newKeyLabel="New Key"
        newKeyIcon={<Plus size={14} />}
        loading={loading}
        keys={mappedKeys}
        emptyState={{
          title: "No API keys created yet.",
          actionLabel: "Generate your first key",
          onAction: () => setCreateDialogOpen(true),
        }}
        onClose={onClose}
        onCreateKey={() => setCreateDialogOpen(true)}
      />

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

function ComposeKeyDialog({
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
          purpose: "api",
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
    if (!generatedKey) {
      return;
    }

    await navigator.clipboard.writeText(generatedKey);
    setCopied(true);
    onNotify("success", "Copied to clipboard");
    window.setTimeout(() => setCopied(false), 2000);
  };

  return (
    <ComposeKeyDialogShell
      open={open}
      title="Generate API Key"
      subtitle="Create a key for external tools like Cursor or OpenCode. Uses your current session budget."
      titleIcon={<Key size={18} />}
      keyName={keyName}
      keyNameLabel="Key Name"
      keyNamePlaceholder="e.g., Cursor, OpenCode"
      onKeyNameChange={setKeyName}
      summaryRows={[
        {
          label: "Budget",
          value: `$${formatUsdc(session.budgetRemaining || "0", 2)} USDC`,
        },
        {
          label: "Expires",
          value: session.expiresAt ? new Date(session.expiresAt).toLocaleString() : "Never",
        },
      ]}
      generatedKey={generatedKey}
      warning="Save this key now. You will not be able to see it again."
      usageLabel="Usage"
      usageValue="Authorization: Bearer compose-..."
      onClose={handleClose}
      onGenerate={() => void handleGenerate()}
      generating={generating}
      generateLabel="Generate Key"
      generatingLabel="Generating..."
      onCopy={() => void handleCopy()}
      copied={copied}
      copyLabel="Copy to Clipboard"
      copiedLabel="Copied!"
      copyIcon={<Copy size={14} />}
      copiedIcon={<Check size={14} />}
    />
  );
}
