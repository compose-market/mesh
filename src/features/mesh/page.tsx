import { useId, useMemo, useState } from "react";
import { Activity, Radio, Radar, Wallet } from "lucide-react";
import { ShellPageHeader, ShellPill, ShellPanel } from "@compose-market/theme/shell";
import type { InstalledAgent, MeshPeerSignal } from "../../lib/types";
import {
  buildMeshScene,
  projectMeshCoordinate,
  type MeshAnchorNode,
  type MeshBootstrapResolution,
  type MeshScenePeerNode,
} from "./model";

interface MeshPageProps {
  agent: InstalledAgent | null;
  peers: MeshPeerSignal[];
  bootstrapResolution: MeshBootstrapResolution;
}

const LATITUDE_LINES = [-45, -15, 15, 45];
const LONGITUDE_LINES = [-150, -90, -30, 30, 90, 150];
const LANDMASSES: Array<{ id: string; points: Array<[number, number]> }> = [
  {
    id: "americas",
    points: [
      [62, -164],
      [72, -142],
      [60, -118],
      [50, -108],
      [36, -95],
      [17, -82],
      [-8, -76],
      [-32, -69],
      [-53, -74],
      [-48, -92],
      [-12, -118],
      [18, -138],
      [45, -154],
      [62, -164],
    ],
  },
  {
    id: "eurafrica",
    points: [
      [70, -12],
      [62, 18],
      [55, 42],
      [44, 40],
      [34, 26],
      [8, 10],
      [-24, 16],
      [-35, 4],
      [-8, -12],
      [28, -14],
      [48, -18],
      [70, -12],
    ],
  },
  {
    id: "asiapacific",
    points: [
      [66, 42],
      [60, 78],
      [52, 110],
      [34, 132],
      [12, 126],
      [-12, 112],
      [-34, 132],
      [-44, 156],
      [-18, 170],
      [10, 154],
      [34, 122],
      [54, 96],
      [66, 42],
    ],
  },
];

function formatProvider(value: string | null): string {
  if (!value) {
    return "Unknown";
  }
  return value.slice(0, 1).toUpperCase() + value.slice(1);
}

function describePeer(peer: MeshPeerSignal): string {
  return peer.card?.statusLine || peer.card?.headline || peer.agentWallet || peer.peerId;
}

function describePeerAnchor(peer: MeshPeerSignal): string {
  if (peer.anchorRegion && peer.anchorProvider) {
    return `${peer.anchorRegion.toUpperCase()} · ${formatProvider(peer.anchorProvider)}`;
  }
  if (peer.anchorHost) {
    return peer.anchorHost;
  }
  if (peer.relayPeerId) {
    return `Relay ${peer.relayPeerId.slice(0, 8)}...`;
  }
  return "Unanchored";
}

function anchorTitle(anchor: MeshAnchorNode): string {
  return anchor.city || anchor.region?.toUpperCase() || anchor.host || "Bootstrap relay";
}

function anchorSubtitle(anchor: MeshAnchorNode): string {
  const parts = [
    anchor.region?.toUpperCase() || null,
    anchor.provider ? formatProvider(anchor.provider) : null,
  ].filter(Boolean);
  return parts.length > 0 ? parts.join(" · ") : (anchor.host || "Rendezvous relay");
}

function formatSeen(timestamp: number): string {
  return new Date(timestamp).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
  });
}

function buildGeoPath(points: Array<[number, number]>): string {
  return points
    .map(([lat, lon], index) => {
      const point = projectMeshCoordinate(lat, lon);
      return `${index === 0 ? "M" : "L"} ${point.x.toFixed(2)} ${point.y.toFixed(2)}`;
    })
    .join(" ");
}

function buildCurvePath(from: { x: number; y: number }, to: { x: number; y: number }, lift = 0): string {
  const midpointX = (from.x + to.x) / 2;
  const arcHeight = Math.max(5, Math.abs(from.x - to.x) * 0.14) + lift;
  const midpointY = Math.min(from.y, to.y) - arcHeight;
  return `M ${from.x.toFixed(2)} ${from.y.toFixed(2)} Q ${midpointX.toFixed(2)} ${midpointY.toFixed(2)} ${to.x.toFixed(2)} ${to.y.toFixed(2)}`;
}

function Preview({ title, subtitle, detail }: { title: string; subtitle: string; detail: string }) {
  return (
    <span className="mesh-peer-preview">
      <strong>{title}</strong>
      <span>{subtitle}</span>
      <span>{detail}</span>
    </span>
  );
}

function MetaRow({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="mesh-selection-card__row">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

export function MeshPage({ agent, peers, bootstrapResolution }: MeshPageProps) {
  const clipId = useId().replace(/:/g, "");
  const [selectedPeerId, setSelectedPeerId] = useState<string | null>(null);
  const [selectedAnchorId, setSelectedAnchorId] = useState<string | null>(null);
  const [hoveredPeerId, setHoveredPeerId] = useState<string | null>(null);
  const [hoveredAnchorId, setHoveredAnchorId] = useState<string | null>(null);
  const scene = useMemo(() => buildMeshScene({ peers, resolution: bootstrapResolution }), [bootstrapResolution, peers]);
  const selectedPeerNode = scene.peers.find((node) => node.peer.peerId === selectedPeerId) || null;
  const hoveredPeerNode = scene.peers.find((node) => node.peer.peerId === hoveredPeerId) || null;
  const selectedAnchor = scene.anchors.find((anchor) => anchor.id === selectedAnchorId) || null;
  const hoveredAnchor = scene.anchors.find((anchor) => anchor.id === hoveredAnchorId) || null;
  const focusPeerNode = selectedPeerNode || hoveredPeerNode || null;
  const focusAnchor = (
    focusPeerNode
      ? scene.anchors.find((anchor) => anchor.id === focusPeerNode.anchorNodeId) || null
      : selectedAnchor || hoveredAnchor || scene.anchors[0] || null
  );
  const centerStatus = agent ? agent.network.status : `bootstrap ${bootstrapResolution.source}`;
  const activeWallet = agent ? `${agent.agentWallet.slice(0, 8)}...${agent.agentWallet.slice(-4)}` : "No local agent";
  const backboneEdges = useMemo(() => {
    const ordered = [...scene.anchors].sort((left, right) => (left.lon ?? left.x) - (right.lon ?? right.x));
    if (ordered.length <= 1) {
      return [];
    }

    return ordered.map((anchor, index) => ({
      from: anchor,
      to: ordered[(index + 1) % ordered.length],
      wrap: index === ordered.length - 1,
    }));
  }, [scene.anchors]);
  const graticuleLatitudePaths = useMemo(
    () => LATITUDE_LINES.map((lat) => buildGeoPath(Array.from({ length: 13 }, (_, index) => [lat, -180 + (index * 30)] as [number, number]))),
    [],
  );
  const graticuleLongitudePaths = useMemo(
    () => LONGITUDE_LINES.map((lon) => buildGeoPath(Array.from({ length: 12 }, (_, index) => [65 - (index * 11), lon] as [number, number]))),
    [],
  );

  return (
    <section className="mesh-page">
      <ShellPageHeader
        className="mesh-page-header"
        eyebrow="Network"
        title="Global Mesh Topology"
        subtitle="Let your local agents discover & connect with their peers."
        actions={(
          <div className="mesh-toolbar">
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
              <span>{centerStatus}</span>
            </ShellPill>
            <ShellPill className="mesh-stat-pill">
              <Wallet size={14} />
              <span>{activeWallet}</span>
            </ShellPill>
          </div>
        )}
      />

      <ShellPanel className="mesh-stage" padded={false}>
        <div className="mesh-stage__canvas">
          <div className="mesh-stage__aurora" aria-hidden="true" />
          <div className="mesh-stage__noise" aria-hidden="true" />

          <svg className="mesh-map" viewBox="0 0 100 100" preserveAspectRatio="none" aria-hidden="true">
            <defs>
              <clipPath id={clipId}>
                <rect x="4" y="6" width="92" height="88" rx="8" ry="8" />
              </clipPath>
              <linearGradient id={`${clipId}-backbone`} x1="0%" y1="0%" x2="100%" y2="0%">
                <stop offset="0%" stopColor="hsl(var(--primary) / 0.12)" />
                <stop offset="55%" stopColor="hsl(var(--primary) / 0.32)" />
                <stop offset="100%" stopColor="hsl(var(--accent) / 0.18)" />
              </linearGradient>
            </defs>

            <rect className="mesh-map__frame" x="4" y="6" width="92" height="88" rx="8" ry="8" />
            <g clipPath={`url(#${clipId})`}>
              <rect className="mesh-map__ocean" x="4" y="6" width="92" height="88" />

              {graticuleLatitudePaths.map((path, index) => (
                <path key={`lat-${index}`} d={path} className="mesh-map__graticule mesh-map__graticule--lat" />
              ))}

              {graticuleLongitudePaths.map((path, index) => (
                <path key={`lon-${index}`} d={path} className="mesh-map__graticule mesh-map__graticule--lon" />
              ))}

              {LANDMASSES.map((landmass) => (
                <path key={landmass.id} d={buildGeoPath(landmass.points)} className="mesh-map__landmass" />
              ))}

              {backboneEdges.map(({ from, to, wrap }) => (
                <path
                  key={`${from.id}-${to.id}`}
                  d={buildCurvePath(from, to, wrap ? 14 : 8)}
                  className={`mesh-map__backbone ${focusAnchor && (focusAnchor.id === from.id || focusAnchor.id === to.id) ? "active" : ""}`}
                  stroke={wrap ? "hsl(var(--primary) / 0.12)" : `url(#${clipId}-backbone)`}
                />
              ))}

              {scene.peers.map((node) => {
                const anchor = scene.anchors.find((item) => item.id === node.anchorNodeId);
                if (!anchor) {
                  return null;
                }

                return (
                  <path
                    key={`peer-edge-${node.peer.peerId}`}
                    d={buildCurvePath(anchor, node, 2)}
                    className={`mesh-map__peer-edge ${focusPeerNode?.peer.peerId === node.peer.peerId ? "active" : ""}`}
                  />
                );
              })}
            </g>
          </svg>

          <div className="mesh-stage__legend">
            <span><i className="mesh-stage__legend-dot anchor" /> Rendezvous region</span>
            <span><i className="mesh-stage__legend-dot peer" /> Live peer signal</span>
            <span><i className="mesh-stage__legend-dot route" /> Active mesh backbone</span>
          </div>

          {scene.anchors.map((anchor, index) => {
            const isSelected = focusAnchor?.id === anchor.id;
            const labelSide = anchor.x > 74 ? "west" : anchor.x < 24 ? "east" : anchor.y > 60 ? "north" : "south";

            return (
              <button
                key={anchor.id}
                type="button"
                className={`mesh-anchor-node ${isSelected ? "selected" : ""}`}
                data-side={labelSide}
                style={{ left: `${anchor.x}%`, top: `${anchor.y}%`, animationDelay: `${index * 90}ms` }}
                onMouseEnter={() => setHoveredAnchorId(anchor.id)}
                onMouseLeave={() => setHoveredAnchorId((current) => (current === anchor.id ? null : current))}
                onClick={() => {
                  setSelectedAnchorId(anchor.id);
                  setSelectedPeerId(null);
                }}
              >
                <span className="mesh-anchor-node__halo" />
                <span className="mesh-anchor-node__core" />
                <span className="mesh-anchor-node__label">
                  <strong>{anchorTitle(anchor)}</strong>
                  <span>{anchorSubtitle(anchor)}</span>
                </span>
                {hoveredAnchorId === anchor.id ? (
                  <Preview
                    title={anchorTitle(anchor)}
                    subtitle={anchorSubtitle(anchor)}
                    detail={`${anchor.peerIds.length} relay ids`}
                  />
                ) : null}
              </button>
            );
          })}

          {scene.peers.map((node, index) => (
            <button
              key={node.peer.peerId}
              type="button"
              className={`mesh-peer-node ${focusPeerNode?.peer.peerId === node.peer.peerId ? "selected" : ""} ${node.peer.stale ? "stale" : ""}`}
              style={{ left: `${node.x}%`, top: `${node.y}%`, animationDelay: `${index * 100}ms` }}
              onMouseEnter={() => setHoveredPeerId(node.peer.peerId)}
              onMouseLeave={() => setHoveredPeerId((current) => (current === node.peer.peerId ? null : current))}
              onClick={() => {
                setSelectedPeerId(node.peer.peerId);
                setSelectedAnchorId(node.anchorNodeId);
              }}
            >
              <span className="mesh-peer-node__pulse" />
              <span className="mesh-peer-node__dot" />
              {hoveredPeerId === node.peer.peerId ? (
                <Preview
                  title={node.peer.card?.name || node.peer.peerId}
                  subtitle={describePeer(node.peer)}
                  detail={describePeerAnchor(node.peer)}
                />
              ) : null}
            </button>
          ))}

          <div className="mesh-selection-card">
            <div className="mesh-selection-card__eyebrow">
              {focusPeerNode ? "Live peer" : "Bootstrap region"}
            </div>
            <h3>{focusPeerNode ? (focusPeerNode.peer.card?.name || "Selected peer") : (focusAnchor ? anchorTitle(focusAnchor) : "Mesh topology")}</h3>
            <p>
              {focusPeerNode
                ? describePeer(focusPeerNode.peer)
                : focusAnchor
                  ? `${anchorSubtitle(focusAnchor)} seeded as a rendezvous lane for Compose desktop bootstrap and relay discovery.`
                  : "Bootstrap topology is always available, even before a local agent joins the network."}
            </p>

            {focusPeerNode ? (
              <>
                <MetaRow label="Peer ID" value={focusPeerNode.peer.peerId} />
                <MetaRow label="Anchor" value={describePeerAnchor(focusPeerNode.peer)} />
                <MetaRow label="Signals" value={focusPeerNode.peer.signalCount} />
                <MetaRow label="Last seen" value={formatSeen(focusPeerNode.peer.lastSeenAt)} />
                {focusPeerNode.peer.caps.length > 0 ? (
                  <div className="mesh-selection-card__tags">
                    {focusPeerNode.peer.caps.map((cap) => (
                      <span key={`${focusPeerNode.peer.peerId}-${cap}`} className="plugin-tag">{cap}</span>
                    ))}
                  </div>
                ) : null}
              </>
            ) : focusAnchor ? (
              <>
                <MetaRow label="Region" value={focusAnchor.region?.toUpperCase() || "Unknown"} />
                <MetaRow label="Provider" value={formatProvider(focusAnchor.provider)} />
                <MetaRow label="Relay IDs" value={focusAnchor.peerIds.length} />
                <MetaRow label="Host" value={focusAnchor.host || "Peer-mapped relay"} />
              </>
            ) : null}
          </div>
        </div>
      </ShellPanel>
    </section>
  );
}
