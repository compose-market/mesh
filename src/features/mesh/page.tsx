import { useMemo, useState } from "react";
import { Activity, Radio, Radar, Wallet } from "lucide-react";
import { ShellPageHeader, ShellPill, ShellPanel } from "@compose-market/theme/shell";
import type { InstalledAgent, MeshPeerSignal } from "../../lib/types";
import { buildMeshScene, type MeshAnchorNode, type MeshBootstrapResolution } from "./model";

interface MeshPageProps {
  agent: InstalledAgent | null;
  peers: MeshPeerSignal[];
  bootstrapResolution: MeshBootstrapResolution;
}

function describePeer(peer: MeshPeerSignal): string {
  return peer.card?.statusLine || peer.card?.headline || peer.agentWallet || peer.peerId;
}

function describePeerAnchor(peer: MeshPeerSignal): string {
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

function describeAnchor(anchor: MeshAnchorNode): string {
  return anchor.region && anchor.provider
    ? `${anchor.region} · ${anchor.provider}`
    : anchor.host || "bootstrap relay";
}

function MetaRow({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="mesh-sidebar-meta">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

export function MeshPage({ agent, peers, bootstrapResolution }: MeshPageProps) {
  const [selectedPeerId, setSelectedPeerId] = useState<string | null>(null);
  const [selectedAnchorId, setSelectedAnchorId] = useState<string | null>(null);
  const [hoveredPeerId, setHoveredPeerId] = useState<string | null>(null);
  const [hoveredAnchorId, setHoveredAnchorId] = useState<string | null>(null);
  const scene = useMemo(() => buildMeshScene({ peers, resolution: bootstrapResolution }), [bootstrapResolution, peers]);
  const selectedPeer = peers.find((peer) => peer.peerId === selectedPeerId) || null;
  const selectedAnchor = scene.anchors.find((anchor) => anchor.id === selectedAnchorId) || scene.anchors[0] || null;
  const hoveredPeer = peers.find((peer) => peer.peerId === hoveredPeerId) || null;
  const hoveredAnchor = scene.anchors.find((anchor) => anchor.id === hoveredAnchorId) || null;
  const focusPeer = selectedPeer || hoveredPeer;
  const centerTitle = agent?.metadata.name || "Compose Mesh";
  const centerStatus = agent ? agent.network.status : `bootstrap ${bootstrapResolution.source}`;

  return (
    <section className="mesh-page">
      <ShellPageHeader
        className="mesh-page-header"
        eyebrow="Network"
        title="Mesh Topology"
        subtitle="Bootstrapped rendezvous regions are always visible. Live peer signals overlay the same topology when a local agent joins the mesh."
        actions={(
          <div className="mesh-header-stats">
            <ShellPill className="mesh-stat-pill">
              <Activity size={14} />
              <span>{peers.length} peers visible</span>
            </ShellPill>
            <ShellPill className="mesh-stat-pill">
              <Radio size={14} />
              <span>{scene.anchors.length} rendezvous regions</span>
            </ShellPill>
            <ShellPill className="mesh-stat-pill">
              <Radar size={14} />
              <span>{bootstrapResolution.source} bootstrap</span>
            </ShellPill>
            <ShellPill className="mesh-stat-pill">
              <Wallet size={14} />
              <span>{agent ? `${agent.agentWallet.slice(0, 8)}...${agent.agentWallet.slice(-4)}` : "No local agent"}</span>
            </ShellPill>
          </div>
        )}
      />

      <div className="mesh-layout">
        <div className="mesh-radar">
          <div className="mesh-radar-grid" />
          <div className="mesh-radar-plane" />
          <div className="mesh-radar-sweep" />

          <svg className="mesh-edge-layer" viewBox="0 0 100 100" preserveAspectRatio="none">
            {scene.anchors.map((anchor) => (
              <line
                key={`anchor-edge-${anchor.id}`}
                x1="50"
                y1="50"
                x2={anchor.x}
                y2={anchor.y}
                className={`mesh-edge mesh-anchor-edge ${selectedAnchor?.id === anchor.id ? "active" : ""}`}
              />
            ))}
            {scene.peers.map((node) => {
              const anchor = scene.anchors.find((item) => item.id === node.anchorNodeId);
              return (
                <line
                  key={`peer-edge-${node.peer.peerId}`}
                  x1={anchor ? anchor.x : 50}
                  y1={anchor ? anchor.y : 50}
                  x2={node.x}
                  y2={node.y}
                  className={`mesh-edge ${focusPeer?.peerId === node.peer.peerId ? "active" : ""}`}
                />
              );
            })}
          </svg>

          <div className="mesh-center-node">
            <div className="mesh-center-core" />
            <div className="mesh-center-copy">
              <strong>{centerTitle}</strong>
              <span>{centerStatus}</span>
            </div>
          </div>

          {scene.anchors.map((anchor, index) => (
            <button
              key={anchor.id}
              className={`mesh-anchor-node ${selectedAnchor?.id === anchor.id ? "selected" : ""}`}
              style={{ left: `${anchor.x}%`, top: `${anchor.y}%`, animationDelay: `${index * 120}ms` }}
              onMouseEnter={() => setHoveredAnchorId(anchor.id)}
              onMouseLeave={() => setHoveredAnchorId((current) => (current === anchor.id ? null : current))}
              onClick={() => {
                setSelectedAnchorId(anchor.id);
                setSelectedPeerId(null);
              }}
            >
              <span className="mesh-anchor-ring" />
              <span className="mesh-anchor-core" />
              {hoveredAnchorId === anchor.id ? (
                <span className="mesh-peer-preview">
                  <strong>{describeAnchor(anchor)}</strong>
                  <span>{anchor.host || "Peer-mapped bootstrap relay"}</span>
                  <span>{anchor.peerIds.length} relay ids</span>
                </span>
              ) : null}
            </button>
          ))}

          {scene.peers.map((node, index) => (
            <button
              key={node.peer.peerId}
              className={`mesh-peer-node ${selectedPeer?.peerId === node.peer.peerId ? "selected" : ""} ${node.peer.stale ? "stale" : ""}`}
              style={{ left: `${node.x}%`, top: `${node.y}%`, animationDelay: `${index * 120}ms` }}
              onMouseEnter={() => setHoveredPeerId(node.peer.peerId)}
              onMouseLeave={() => setHoveredPeerId((current) => (current === node.peer.peerId ? null : current))}
              onClick={() => {
                setSelectedPeerId(node.peer.peerId);
                setSelectedAnchorId(node.anchorNodeId);
              }}
            >
              <span className="mesh-peer-pulse" />
              <span className="mesh-peer-dot" />
              {hoveredPeerId === node.peer.peerId ? (
                <span className="mesh-peer-preview">
                  <strong>{node.peer.card?.name || node.peer.peerId}</strong>
                  <span>{describePeer(node.peer)}</span>
                  <span>{describePeerAnchor(node.peer)}</span>
                </span>
              ) : null}
            </button>
          ))}
        </div>

        <aside className="mesh-sidebar">
          <ShellPanel className="mesh-sidebar-card">
            <h3>{selectedPeer ? (selectedPeer.card?.name || "Selected peer") : (selectedAnchor ? describeAnchor(selectedAnchor) : "Bootstrap topology")}</h3>
            <p>
              {selectedPeer
                ? describePeer(selectedPeer)
                : selectedAnchor
                  ? `${selectedAnchor.peerIds.length} rendezvous relay ids are currently seeded for this region.`
                  : "Bootstrap topology is available even before a local agent joins the mesh."}
            </p>

            {selectedPeer ? (
              <>
                <MetaRow label="Peer ID" value={selectedPeer.peerId} />
                <MetaRow label="Agent wallet" value={selectedPeer.agentWallet || "Unknown"} />
                <MetaRow label="Signals" value={selectedPeer.signalCount} />
                <MetaRow label="Anchor" value={describePeerAnchor(selectedPeer)} />
                <MetaRow label="Announces" value={selectedPeer.announceCount} />
                <MetaRow label="Last seen" value={new Date(selectedPeer.lastSeenAt).toLocaleTimeString()} />
                <div className="mesh-sidebar-tags">
                  {selectedPeer.caps.map((cap) => (
                    <span key={`${selectedPeer.peerId}-${cap}`} className="plugin-tag">{cap}</span>
                  ))}
                </div>
              </>
            ) : selectedAnchor ? (
              <>
                <MetaRow label="Region" value={selectedAnchor.region || "Unknown"} />
                <MetaRow label="Provider" value={selectedAnchor.provider || "Unknown"} />
                <MetaRow label="Host" value={selectedAnchor.host || "Peer-mapped relay"} />
                <MetaRow label="Relay IDs" value={selectedAnchor.peerIds.length} />
              </>
            ) : null}
          </ShellPanel>

          <ShellPanel className="mesh-sidebar-feed">
            <h4>Bootstrap Regions</h4>
            <div className="mesh-feed-list">
              {scene.anchors.map((anchor) => (
                <button
                  key={anchor.id}
                  className="mesh-feed-item"
                  onClick={() => {
                    setSelectedAnchorId(anchor.id);
                    setSelectedPeerId(null);
                  }}
                >
                  <strong>{describeAnchor(anchor)}</strong>
                  <span>{anchor.peerIds.length} relay ids</span>
                </button>
              ))}
            </div>
          </ShellPanel>

          <ShellPanel className="mesh-sidebar-feed">
            <h4>Recent Signals</h4>
            <div className="mesh-feed-list">
              {peers.length === 0 ? (
                <div className="mesh-feed-empty">No live peers visible yet.</div>
              ) : (
                peers.slice(0, 8).map((peer) => (
                  <button
                    key={`${peer.peerId}-${peer.lastSeenAt}`}
                    className="mesh-feed-item"
                    onClick={() => {
                      setSelectedPeerId(peer.peerId);
                      setSelectedAnchorId(null);
                    }}
                  >
                    <strong>{peer.card?.name || peer.peerId}</strong>
                    <span>{describePeerAnchor(peer)}</span>
                  </button>
                ))
              )}
            </div>
          </ShellPanel>
        </aside>
      </div>
    </section>
  );
}
