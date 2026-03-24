import { useMemo, useState } from "react";
import { ExternalLink, Loader2, ToggleLeft, ToggleRight, Trash2 } from "lucide-react";
import { open as openUrl } from "@tauri-apps/plugin-shell";
import { uninstallSkill } from "../lib/api";
import { permissionAllows } from "../lib/storage";
import type { LocalRuntimeState } from "../lib/types";

interface SkillsManagerProps {
  state: LocalRuntimeState;
  onStateChange: (next: LocalRuntimeState) => Promise<void>;
}

export function SkillsManager({ state, onStateChange }: SkillsManagerProps) {
  const [busySkill, setBusySkill] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const enabledCount = useMemo(
    () => state.installedSkills.filter((skill) => skill.enabled).length,
    [state.installedSkills],
  );

  const toggleSkill = async (skillId: string) => {
    const nextSkills = state.installedSkills.map((skill) =>
      skill.id === skillId ? { ...skill, enabled: !skill.enabled } : skill,
    );

    await onStateChange({ ...state, installedSkills: nextSkills });
  };

  const removeSkill = async (skillId: string) => {
    if (!permissionAllows(state.permissionDefaults.filesystemDelete)) {
      setError("Enable Filesystem Delete permission in Settings to uninstall skills.");
      return;
    }

    const target = state.installedSkills.find((skill) => skill.id === skillId);
    if (!target) return;

    setBusySkill(skillId);
    setError(null);
    try {
      await uninstallSkill(target);
      await onStateChange({
        ...state,
        installedSkills: state.installedSkills.filter((skill) => skill.id !== skillId),
      });
    } catch (removeError) {
      console.error("[skills] remove failed", removeError);
      setError(removeError instanceof Error ? removeError.message : "Failed to remove skill");
    } finally {
      setBusySkill(null);
    }
  };

  return (
    <div className="skills-manager">
      <div className="skills-manager-header">
        <div>
          <h2>Installed Skills</h2>
          <p className="subtitle">{state.installedSkills.length} installed · {enabledCount} enabled</p>
        </div>
      </div>

      {error ? <div className="notification notification-error">{error}</div> : null}

      {state.installedSkills.length === 0 ? (
        <div className="empty-state">
          <h3>No local skills installed</h3>
          <p>Use Marketplace to install skills from ClawHub and curated repositories.</p>
        </div>
      ) : (
        <div className="installed-list">
          {state.installedSkills.map((skill) => {
            const isBuiltin = skill.source.id === "built-in";
            return (
              <div key={skill.id} className={`installed-skill ${skill.enabled ? "" : "disabled"}`}>
                <div className="skill-info">
                  <div className="skill-title-row">
                    <h3>{skill.name}</h3>
                    <span className="source-tag">{skill.source.name}</span>
                  </div>
                  <p className="skill-desc">{skill.description}</p>
                  <div className="skill-meta-row">
                    <span className="meta-date">Installed: {new Date(skill.installedAt).toLocaleString()}</span>
                    <span className="meta-path" title={skill.localPath}>{skill.localPath}</span>
                  </div>
                </div>

                <div className="skill-actions">
                  <button
                    className={`icon-btn toggle-btn ${skill.enabled ? "enabled" : ""}`}
                    onClick={() => {
                      void toggleSkill(skill.id);
                    }}
                    title={isBuiltin ? "Built-in skill" : skill.enabled ? "Disable skill" : "Enable skill"}
                    disabled={isBuiltin}
                  >
                    {skill.enabled ? <ToggleRight size={20} /> : <ToggleLeft size={20} />}
                  </button>
                  <button
                    className="icon-btn"
                    onClick={() => window.open(skill.htmlUrl, "_blank", "noopener,noreferrer")}
                    title="Open source repository"
                  >
                    <ExternalLink size={18} />
                  </button>
                  <button
                    className="icon-btn"
                    onClick={() => {
                      void openUrl(`${skill.localPath}/SKILL.md`);
                    }}
                    title="Open local SKILL.md"
                  >
                    <ExternalLink size={18} />
                  </button>
                  <button
                    className="icon-btn danger"
                    onClick={() => {
                      void removeSkill(skill.id);
                    }}
                    disabled={busySkill === skill.id || isBuiltin}
                    title={isBuiltin ? "Built-in skill" : "Uninstall skill"}
                  >
                    {busySkill === skill.id ? <Loader2 size={16} className="spinner" /> : <Trash2 size={18} />}
                  </button>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
