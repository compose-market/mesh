import type { OsPermissionSnapshot } from "../../lib/types";
import { ShellButton } from "@compose-market/theme/shell";
import { formatOsPermissionStatus } from "../../lib/permissions";
import { MachineStatusCard } from "./agent-permissions";

interface GlobalPermissionsSectionProps {
  osPermissions: OsPermissionSnapshot;
  refreshing: boolean;
  onOpenSystemPermissions: () => void;
  onRefresh: () => void;
}

/**
 * Global OS-level permissions section rendered inside Settings.
 * Shows real macOS TCC status for camera, microphone, and guides the user
 * to System Settings for Full Disk Access and other system-wide permissions.
 *
 * Per-agent permissions are managed separately on each agent's detail page.
 * This section controls what the Compose Mesh application itself is allowed
 * to access on macOS — agents cannot exceed these bounds.
 */
export function GlobalPermissionsSection({ osPermissions, refreshing, onOpenSystemPermissions, onRefresh }: GlobalPermissionsSectionProps) {
  return (
    <div className="settings-section">
      <h3>System Permissions</h3>
      <p className="settings-hint">
        Compose Mesh requires full system access to operate agents autonomously.
        Grant permissions in macOS System Settings → Privacy &amp; Security.
        Per-agent permissions are managed on each agent's detail page.
      </p>
      <div className="detail-stat-stack">
        <MachineStatusCard label="Location" value={formatOsPermissionStatus(osPermissions.location)} />
        <MachineStatusCard label="Camera" value={formatOsPermissionStatus(osPermissions.camera)} />
        <MachineStatusCard label="Microphone" value={formatOsPermissionStatus(osPermissions.microphone)} />
        <MachineStatusCard label="Full Disk Access" value={formatOsPermissionStatus(osPermissions.fullDiskAccess)} />
        <MachineStatusCard label="Accessibility" value={formatOsPermissionStatus(osPermissions.accessibility)} />
      </div>
      <div className="settings-actions">
        <ShellButton tone="primary" onClick={onOpenSystemPermissions}>
          Open System Settings
        </ShellButton>
        <ShellButton tone="secondary" disabled={refreshing} onClick={onRefresh}>
          Refresh Local Status
        </ShellButton>
      </div>
    </div>
  );
}
