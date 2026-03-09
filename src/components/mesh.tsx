import { useMemo, useState } from "react";
import { Activity, Radio, ScanSearch, Wallet } from "lucide-react";
import { ShellEmptyState, ShellPageHeader, ShellPill, ShellPanel } from "@compose-market/theme/shell";
import type { InstalledAgent, MeshPeerSignal } from "../lib/types";

interface MeshNetworkPageProps {
  agent: InstalledAgent | null;
  peers: MeshPeerSignal[];
}

interface PositionedPeer {
  peer: MeshPeerSignal;
  x: number;
  y: number;
  anchorKey: string;
}

function describePeer(peer: MeshPeerSignal): string {
  if (peer.card?.statusLine) return peer.card.statusLine;
  if (peer.card?.headline) return peer.card.headline;
  if (peer.agentWallet) return peer.agentWallet;
  return peer.peerId;
}

function describeAnchor(peer: MeshPeerSignal): string {
  if (peer.anchorRegion && peer.anchorProvider) {
    return `${peer.anchorRegion} · ${peer.anchorProvider}`;
  }
  if (peer.anchorHost) {
    return peer.anchorHost;
  }
  if (peer.relayPeerId) {
    return `relay ${peer.relayPeerId.slice(0, 8)}...`;
  }
  return "unanchored";
}

function anchorKey(peer: MeshPeerSignal): string {
  return peer.anchorHost || peer.relayPeerId || `unanchored:${peer.peerId}`;
}

function positionPeers(peers: MeshPeerSignal[]): PositionedPeer[] {
  const grouped = new Map<string, MeshPeerSignal[]>();
  for (const peer of peers) {
    const key = anchorKey(peer);
    const items = grouped.get(key) || [];
    items.push(peer);
    grouped.set(key, items);
  }

  const result: PositionedPeer[] = [];
  const centerX = 50;
  const centerY = 50;
  const groups = [...grouped.entries()].sort((left, right) => {
    const leftPeer = left[1][0];
    const rightPeer = right[1][0];
    return describeAnchor(leftPeer).localeCompare(describeAnchor(rightPeer));
  });
  const sector = (Math.PI * 2) / Math.max(1, groups.length);

  groups.forEach(([key, rawItems], groupIndex) => {
    const items = [...rawItems].sort((left, right) => left.nodeDistance - right.nodeDistance || right.lastSeenAt - left.lastSeenAt);
    const baseAngle = (-Math.PI / 2) + (groupIndex * sector);
    items.forEach((peer, index) => {
      const ring = Math.max(1, Math.min(4, peer.nodeDistance || 1));
      const radius = 16 + ring * 11 + Math.floor(index / 2) * 4;
      const spread = groups.length === 1
        ? 0.24
        : Math.min(0.24, sector * 0.28);
      const angleOffset = items.length === 1
        ? 0
        : (index - ((items.length - 1) / 2)) * spread;
      const angle = baseAngle + angleOffset;
      result.push({
        peer,
        x: centerX + Math.cos(angle) * radius,
        y: centerY + Math.sin(angle) * radius,
        anchorKey: key,
      });
    });
  });

  return result;
}

export function MeshNetworkPage({ agent, peers }: MeshNetworkPageProps) {
  const [selectedPeerId, setSelectedPeerId] = useState<string | null>(null);
  const [hoveredPeerId, setHoveredPeerId] = useState<string | null>(null);

  const positionedPeers = useMemo(() => positionPeers(peers), [peers]);
  const anchorCount = useMemo(
    () => new Set(peers.map((peer) => anchorKey(peer))).size,
    [peers],
  );
  const selectedPeer = peers.find((peer) => peer.peerId === selectedPeerId) || peers[0] || null;
  const hoveredPeer = peers.find((peer) => peer.peerId === hoveredPeerId) || null;

  if (!agent) {
    return (
      <ShellEmptyState
        className="mesh-empty"
        icon={<ScanSearch size={40} />}
        title="No agent is signaling on the mesh"
        description="Start a local agent and enable mesh signaling to render the live network map."
      />
    );
  }

  return (
    <section className="mesh-page">
      <ShellPageHeader
        className="mesh-page-header"
        eyebrow="Network"
        title="Mesh"
        subtitle="Live libp2p presence from this device. No registry, no backend, only local runtime state and peer signals."
        actions={(
          <div className="mesh-header-stats">
            <ShellPill className="mesh-stat-pill">
            <Activity size={14} />
            <span>{peers.length} peers visible</span>
            </ShellPill>
            <ShellPill className="mesh-stat-pill">
            <Radio size={14} />
            <span>{anchorCount} anchors</span>
            </ShellPill>
            <ShellPill className="mesh-stat-pill">
            <Radio size={14} />
            <span>{agent.network.status}</span>
            </ShellPill>
            <ShellPill className="mesh-stat-pill">
            <Wallet size={14} />
            <span>{agent.agentWallet.slice(0, 8)}...{agent.agentWallet.slice(-4)}</span>
            </ShellPill>
          </div>
        )}
      />

      <div className="mesh-layout">
        <div className="mesh-radar">
          <div className="mesh-radar-grid" />
          <div className="mesh-radar-sweep" />
          <svg className="mesh-edge-layer" viewBox="0 0 100 100" preserveAspectRatio="none">
            {positionedPeers.map(({ peer, x, y }) => (
              <line
                key={`edge-${peer.peerId}`}
                x1="50"
                y1="50"
                x2={x}
                y2={y}
                className={`mesh-edge ${selectedPeer?.peerId === peer.peerId ? "active" : ""}`}
              />
            ))}
          </svg>

          <div className="mesh-center-node">
            <div className="mesh-center-core" />
            <div className="mesh-center-copy">
              <strong>{agent.metadata.name}</strong>
              <span>{agent.network.status}</span>
            </div>
          </div>

          {positionedPeers.map(({ peer, x, y }, index) => (
            <button
              key={peer.peerId}
              className={`mesh-peer-node ${selectedPeer?.peerId === peer.peerId ? "selected" : ""} ${peer.stale ? "stale" : ""}`}
              style={{
                left: `${x}%`,
                top: `${y}%`,
                animationDelay: `${index * 120}ms`,
              }}
              onMouseEnter={() => setHoveredPeerId(peer.peerId)}
              onMouseLeave={() => setHoveredPeerId((current) => (current === peer.peerId ? null : current))}
              onClick={() => setSelectedPeerId(peer.peerId)}
            >
              <span className="mesh-peer-pulse" />
              <span className="mesh-peer-dot" />
              {hoveredPeerId === peer.peerId ? (
                <span className="mesh-peer-preview">
                  <strong>{peer.card?.name || peer.peerId}</strong>
                  <span>{describePeer(peer)}</span>
                  <span>{describeAnchor(peer)}</span>
                </span>
              ) : null}
            </button>
          ))}
        </div>

        <aside className="mesh-sidebar">
          <ShellPanel className="mesh-sidebar-card">
            <h3>{selectedPeer?.card?.name || "No peer selected"}</h3>
            <p>{selectedPeer ? describePeer(selectedPeer) : "Click a peer to inspect its latest public card."}</p>

            {selectedPeer ? (
              <>
                <div className="mesh-sidebar-meta">
                  <span>Peer ID</span>
                  <strong>{selectedPeer.peerId}</strong>
                </div>
                <div className="mesh-sidebar-meta">
                  <span>Agent wallet</span>
                  <strong>{selectedPeer.agentWallet || "Unknown"}</strong>
                </div>
                <div className="mesh-sidebar-meta">
                  <span>Signals</span>
                  <strong>{selectedPeer.signalCount}</strong>
                </div>
                <div className="mesh-sidebar-meta">
                  <span>Anchor</span>
                  <strong>{describeAnchor(selectedPeer)}</strong>
                </div>
                <div className="mesh-sidebar-meta">
                  <span>Announces</span>
                  <strong>{selectedPeer.announceCount}</strong>
                </div>
                {selectedPeer.anchorHost ? (
                  <div className="mesh-sidebar-meta">
                    <span>Host</span>
                    <strong>{selectedPeer.anchorHost}</strong>
                  </div>
                ) : null}
                {selectedPeer.relayPeerId ? (
                  <div className="mesh-sidebar-meta">
                    <span>Relay peer</span>
                    <strong>{selectedPeer.relayPeerId}</strong>
                  </div>
                ) : null}
                <div className="mesh-sidebar-meta">
                  <span>Last seen</span>
                  <strong>{new Date(selectedPeer.lastSeenAt).toLocaleTimeString()}</strong>
                </div>
                <div className="mesh-sidebar-tags">
                  {selectedPeer.caps.map((cap) => (
                    <span key={`${selectedPeer.peerId}-${cap}`} className="plugin-tag">{cap}</span>
                  ))}
                </div>
              </>
            ) : null}
          </ShellPanel>

          <ShellPanel className="mesh-sidebar-feed">
            <h4>Recent Signals</h4>
            <div className="mesh-feed-list">
              {peers.length === 0 ? (
                <div className="empty-inline">No peer signals yet.</div>
              ) : (
                peers.slice(0, 12).map((peer) => (
                  <button key={`feed-${peer.peerId}-${peer.lastSeenAt}`} className="mesh-feed-item" onClick={() => setSelectedPeerId(peer.peerId)}>
                    <strong>{peer.card?.name || peer.peerId}</strong>
                    <span>{describePeer(peer)}</span>
                    <small>{describeAnchor(peer)}</small>
                  </button>
                ))
              )}
            </div>
          </ShellPanel>

          {hoveredPeer ? (
            <ShellPanel className="mesh-sidebar-card muted">
              <h4>Hover Preview</h4>
              <p>{hoveredPeer.card?.headline || hoveredPeer.peerId}</p>
              <span>{describeAnchor(hoveredPeer)}</span>
            </ShellPanel>
          ) : null}
        </aside>
      </div>
    </section>
  );
}
