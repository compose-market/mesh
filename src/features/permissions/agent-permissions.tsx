import { Camera, FileSearch, FilePen, FileX, FolderOpen, Mic, Network, Terminal } from "lucide-react";
import type { AgentPermissionPolicy } from "../../lib/types";
import { ShellButton, ShellPanel } from "@compose-market/theme/shell";
import type { ReactNode } from "react";

/* ── Helpers ── */

export function nextPermissionDecision(value: AgentPermissionPolicy[keyof AgentPermissionPolicy]): AgentPermissionPolicy[keyof AgentPermissionPolicy] {
  return value === "allow" ? "deny" : "allow";
}

export function permissionDecisionLabel(value: AgentPermissionPolicy[keyof AgentPermissionPolicy]): string {
  return value === "allow" ? "Allow" : "Deny";
}

/* ── Permission metadata ── */

interface PermissionMeta {
  icon: ReactNode;
  label: string;
  hint: string;
}

const PERM_META: Record<keyof AgentPermissionPolicy, PermissionMeta> = {
  shell:           { icon: <Terminal size={16} />,   label: "Shell",         hint: "Execute CLI commands" },
  filesystemRead:  { icon: <FileSearch size={16} />, label: "Read files",    hint: "Read local files" },
  filesystemWrite: { icon: <FolderOpen size={16} />, label: "Write files",   hint: "Write new files" },
  filesystemEdit:  { icon: <FilePen size={16} />,    label: "Edit files",    hint: "Modify existing" },
  filesystemDelete:{ icon: <FileX size={16} />,      label: "Delete files",  hint: "Remove files" },
  camera:          { icon: <Camera size={16} />,     label: "Camera",        hint: "Video capture" },
  microphone:      { icon: <Mic size={16} />,        label: "Microphone",    hint: "Audio capture" },
  network:         { icon: <Network size={16} />,    label: "Network",       hint: "Mesh & remote tools" },
};

/* ── Components ── */

interface PermissionToggleProps {
  permKey: keyof AgentPermissionPolicy;
  value: AgentPermissionPolicy[keyof AgentPermissionPolicy];
  busy: boolean;
  onToggle: (key: keyof AgentPermissionPolicy) => void;
}

export function PermissionToggle({ permKey, value, busy, onToggle }: PermissionToggleProps) {
  const meta = PERM_META[permKey];
  const isAllowed = value === "allow";

  return (
    <button
      type="button"
      className={`perm-toggle-card ${isAllowed ? "perm-toggle-card--on" : ""}`}
      disabled={busy}
      onClick={() => void onToggle(permKey)}
      title={`${meta.label}: ${isAllowed ? "Allowed" : "Denied"} — click to toggle`}
    >
      <div className={`perm-toggle-icon ${isAllowed ? "perm-toggle-icon--on" : ""}`}>
        {meta.icon}
      </div>
      <div className="perm-toggle-text">
        <span className="perm-toggle-label">{meta.label}</span>
        <span className="perm-toggle-hint">{meta.hint}</span>
      </div>
      <div className={`perm-toggle-switch ${isAllowed ? "perm-toggle-switch--on" : ""}`}>
        <div className="perm-toggle-thumb" />
      </div>
    </button>
  );
}

interface MachineStatusCardProps {
  label: string;
  value: string;
}

export function MachineStatusCard({ label, value }: MachineStatusCardProps) {
  return (
    <div className="detail-stat-card">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

interface PermissionsPanelProps {
  permissions: AgentPermissionPolicy;
  osPermissions: { camera: string; microphone: string };
  agentWallet: string;
  permissionBusy: keyof AgentPermissionPolicy | null;
  onToggle: (key: keyof AgentPermissionPolicy) => void;
  onRefresh: () => void;
}

export function PermissionsPanel({ permissions, osPermissions, agentWallet, permissionBusy, onToggle, onRefresh }: PermissionsPanelProps) {
  return (
    <ShellPanel className="detail-panel">
      <div className="detail-panel-header">
        <h3>Agent Permissions</h3>
        <ShellButton tone="secondary" size="sm" onClick={onRefresh}>
          Refresh
        </ShellButton>
      </div>
      <div className="perm-toggle-grid">
        {(Object.keys(PERM_META) as Array<keyof AgentPermissionPolicy>).map((key) => (
          <PermissionToggle key={key} permKey={key} value={permissions[key]} busy={permissionBusy === key} onToggle={onToggle} />
        ))}
      </div>
      <div className="perm-machine-row">
        <MachineStatusCard label="Camera (OS)" value={osPermissions.camera} />
        <MachineStatusCard label="Mic (OS)" value={osPermissions.microphone} />
        <MachineStatusCard label="Wallet" value={`${agentWallet.slice(0, 8)}...${agentWallet.slice(-4)}`} />
      </div>
    </ShellPanel>
  );
}
