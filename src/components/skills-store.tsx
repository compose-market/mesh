import { useCallback, useEffect, useMemo, useState } from "react";
import { Check, Download, ExternalLink, Filter, Loader2, RefreshCw, Search, X } from "lucide-react";
import { discoverSkills, installSkill } from "../lib/api";
import { permissionAllows } from "../lib/storage";
import type { LocalRuntimeState, Skill } from "../lib/types";

interface SkillsMarketplaceProps {
  state: LocalRuntimeState;
  onStateChange: (next: LocalRuntimeState) => Promise<void>;
}

export function SkillsMarketplace({ state, onStateChange }: SkillsMarketplaceProps) {
  const [query, setQuery] = useState("");
  const [source, setSource] = useState<"" | "clawhub" | "awesome-curated">("");
  const [loading, setLoading] = useState(false);
  const [installingId, setInstallingId] = useState<string | null>(null);
  const [skills, setSkills] = useState<Skill[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [notification, setNotification] = useState<{ type: "success" | "error"; message: string } | null>(null);

  const installedSet = useMemo(() => new Set(state.installedSkills.map((skill) => skill.id)), [state.installedSkills]);

  const showNotification = useCallback((type: "success" | "error", message: string) => {
    setNotification({ type, message });
    window.setTimeout(() => setNotification(null), 4000);
  }, []);

  const loadSkills = useCallback(async (requestedPage = 1) => {
    setLoading(true);
    try {
      const result = await discoverSkills({
        search: query,
        source: source || undefined,
        page: requestedPage,
        limit: 12,
      });
      setSkills(result.skills);
      setTotal(result.total);
      setPage(result.page);
    } catch (error) {
      console.error("[skills] discovery failed", error);
      showNotification("error", "Failed to fetch skills catalog");
    } finally {
      setLoading(false);
    }
  }, [query, showNotification, source]);

  useEffect(() => {
    const timeout = window.setTimeout(() => {
      void loadSkills(1);
    }, 350);
    return () => window.clearTimeout(timeout);
  }, [loadSkills]);

  const handleInstall = async (skill: Skill) => {
    if (!permissionAllows(state.permissionDefaults.filesystemWrite)) {
      showNotification("error", "Enable Filesystem Write permission in Settings to install skills.");
      return;
    }

    setInstallingId(skill.id);
    try {
      const result = await installSkill(skill);
      if (!result.success || !result.installed) {
        showNotification("error", result.error || "Skill installation failed");
        return;
      }

      await onStateChange({
        ...state,
        installedSkills: [...state.installedSkills, result.installed],
      });
      showNotification(
        "success",
        result.warning ? `Installed with warnings: ${result.warning}` : `"${skill.name}" installed`,
      );
    } catch (error) {
      console.error("[skills] installation failed", error);
      showNotification("error", "Skill installation failed");
    } finally {
      setInstallingId(null);
    }
  };

  const totalPages = Math.max(1, Math.ceil(total / 12));

  return (
    <div className="marketplace">
      <div className="marketplace-header">
        <h2>Skills Marketplace</h2>
        <p className="subtitle">Install OpenClaw-compatible local skills (MCP tools remain immutable from agentCard)</p>
      </div>

      {notification ? (
        <div className={`notification notification-${notification.type}`}>
          {notification.type === "success" ? <Check size={16} /> : <X size={16} />}
          {notification.message}
        </div>
      ) : null}

      <div className="search-section">
        <div className="search-bar">
          <Search size={18} className="search-icon" />
          <input
            type="text"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Search skill catalogs..."
            className="search-input"
          />
          {loading ? <Loader2 size={18} className="spinner" /> : null}
        </div>
        <div className="filters">
          <button className={`filter-btn ${source === "" ? "active" : ""}`} onClick={() => setSource("")}>All</button>
          <button className={`filter-btn ${source === "clawhub" ? "active" : ""}`} onClick={() => setSource("clawhub")}>ClawHub</button>
          <button className={`filter-btn ${source === "awesome-curated" ? "active" : ""}`} onClick={() => setSource("awesome-curated")}>Curated GitHub</button>
          <button className="filter-btn" onClick={() => void loadSkills(page)}>
            <RefreshCw size={14} /> Refresh
          </button>
        </div>
      </div>

      <div className="skills-stats">
        <span>{total} skills available</span>
      </div>

      <div className="skills-grid">
        {skills.map((skill) => {
          const installed = installedSet.has(skill.id);
          const isInstalling = installingId === skill.id;
          return (
            <div key={skill.id} className={`skill-card ${installed ? "installed" : ""}`}>
              <div className="skill-header">
                <h3>{skill.name}</h3>
                <span className="source-badge source-default">{skill.source.name}</span>
              </div>
              <p className="skill-description">{skill.description}</p>

              <div className="skill-meta">
                <span className="meta-item">{skill.fullName}</span>
              </div>
              <div className="skill-topics">
                {skill.requirements.bins.map((bin) => (
                  <span className="topic-tag" key={`bin-${skill.id}-${bin}`}>bin:{bin}</span>
                ))}
                {skill.requirements.env.map((env) => (
                  <span className="topic-tag" key={`env-${skill.id}-${env}`}>env:{env}</span>
                ))}
                {skill.requirements.os.map((os) => (
                  <span className="topic-tag" key={`os-${skill.id}-${os}`}>os:{os}</span>
                ))}
                {!skill.requirements.eligible ? (
                  <span className="topic-tag">missing:{skill.requirements.missing.join(", ")}</span>
                ) : null}
              </div>

              <div className="skill-actions">
                <button
                  className="action-btn secondary"
                  onClick={() => window.open(skill.htmlUrl, "_blank", "noopener,noreferrer")}
                  title="Open source repository"
                >
                  <ExternalLink size={14} />
                </button>
                <button
                  className={`action-btn primary ${installed ? "installed-btn" : ""}`}
                  disabled={installed || isInstalling || !permissionAllows(state.permissionDefaults.filesystemWrite)}
                  onClick={() => {
                    void handleInstall(skill);
                  }}
                >
                  {isInstalling ? (
                    <Loader2 size={14} className="spinner" />
                  ) : installed ? (
                    <>
                      <Check size={14} />
                      Installed
                    </>
                  ) : (
                    <>
                      <Download size={14} />
                      Install
                    </>
                  )}
                </button>
              </div>
            </div>
          );
        })}
      </div>

      {!loading && skills.length === 0 ? (
        <div className="empty-state">
          <Filter size={48} className="empty-icon" />
          <h3>No skills found</h3>
          <p>Adjust search filters or refresh the catalog.</p>
        </div>
      ) : null}

      {totalPages > 1 ? (
        <div className="pagination">
          <button
            className="page-btn"
            disabled={page <= 1}
            onClick={() => void loadSkills(page - 1)}
          >
            Previous
          </button>
          <span className="page-info">Page {page} of {totalPages}</span>
          <button
            className="page-btn"
            disabled={page >= totalPages}
            onClick={() => void loadSkills(page + 1)}
          >
            Next
          </button>
        </div>
      ) : null}
    </div>
  );
}
