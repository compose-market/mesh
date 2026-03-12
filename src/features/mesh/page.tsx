import { useMemo, useState } from "react";
import { Activity, Minus, Radio, Wallet, Waypoints, X } from "lucide-react";
import { ComposableMap, Geographies, Geography, Line, Marker, ZoomableGroup } from "react-simple-maps";
import worldAtlas from "world-atlas/countries-110m.json";
import { ShellPageHeader, ShellPill, ShellPanel } from "@compose-market/theme/shell";
import type { InstalledAgent, MeshPeerSignal } from "../../lib/types";
import { buildMeshScene, type MeshBootstrapResolution } from "./model";
import { buildMeshStageModel } from "./stage-model";

interface MeshPageProps {
  agent: InstalledAgent | null;
  peers: MeshPeerSignal[];
  bootstrapResolution: MeshBootstrapResolution;
}

function shortWallet(value: string | null): string {
  if (!value) {
    return "No local agent";
  }
  return `${value.slice(0, 8)}...${value.slice(-4)}`;
}

export function MeshPage({ agent, peers, bootstrapResolution }: MeshPageProps) {
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [selectedRegionId, setSelectedRegionId] = useState<string | null>(null);
  const [cardOpen, setCardOpen] = useState(false);
  const [cardCollapsed, setCardCollapsed] = useState(false);

  const scene = useMemo(() => buildMeshScene({ peers, resolution: bootstrapResolution }), [bootstrapResolution, peers]);
  const stage = useMemo(
    () => buildMeshStageModel({ agent, peers, scene, selectedNodeId, selectedRegionId }),
    [agent, peers, scene, selectedNodeId, selectedRegionId],
  );

  const nodesById = useMemo(() => new Map(stage.nodes.map((node) => [node.id, node])), [stage.nodes]);
  const focusedRegion = (
    stage.regions.find((region) => region.id === selectedRegionId)
    || (selectedNodeId ? stage.regions.find((region) => region.id === nodesById.get(selectedNodeId)?.regionId) : null)
    || null
  );
  const mapCenter = focusedRegion && focusedRegion.lon !== null && focusedRegion.lat !== null
    ? [focusedRegion.lon, focusedRegion.lat] as [number, number]
    : [8, 18] as [number, number];
  const mapZoom = focusedRegion ? 2.25 : 1.05;
  const localNode = stage.nodes.find((node) => node.kind === "local") || null;
  const selectedManifest = stage.selectedManifest;

  const handleSelectRegion = (regionId: string) => {
    setSelectedRegionId((current) => (current === regionId ? null : regionId));
    if (selectedNodeId && nodesById.get(selectedNodeId)?.regionId !== regionId) {
      setSelectedNodeId(null);
      setCardOpen(false);
    }
  };

  const handleSelectNode = (nodeId: string, regionId: string | null) => {
    setSelectedNodeId(nodeId);
    setSelectedRegionId(regionId);
    setCardOpen(true);
    setCardCollapsed(false);
  };

  return (
    <section className="mesh-page">
      <ShellPageHeader
        className="mesh-page-header"
        eyebrow="Network"
        title="Global Mesh Topology"
        subtitle="Select a region to expand the mesh footprint. Select an agent signal to inspect its latest broadcast manifest."
        actions={(
          <div className="mesh-toolbar">
            <ShellPill className="mesh-stat-pill">
              <Activity size={14} />
              <span>{peers.length} peers visible</span>
            </ShellPill>
            <ShellPill className="mesh-stat-pill">
              <Radio size={14} />
              <span>{stage.regions.length} rendezvous regions</span>
            </ShellPill>
            <ShellPill className="mesh-stat-pill">
              <Waypoints size={14} />
              <span>{agent?.network.status || "No local agent"}</span>
            </ShellPill>
            <ShellPill className="mesh-stat-pill">
              <Wallet size={14} />
              <span>{shortWallet(agent?.agentWallet || null)}</span>
            </ShellPill>
          </div>
        )}
      />

      <ShellPanel className="mesh-stage" padded={false}>
        <div className="mesh-stage__canvas">
          <div className="mesh-stage__aurora" aria-hidden="true" />
          <div className="mesh-stage__noise" aria-hidden="true" />

          <div className="mesh-map-world">
            <ComposableMap
              projection="geoEqualEarth"
              projectionConfig={{ scale: 170 }}
              className="mesh-map"
              aria-label="Compose mesh world map"
            >
              <ZoomableGroup center={mapCenter} zoom={mapZoom}>
                <Geographies geography={worldAtlas}>
                  {({ geographies }: { geographies: any[] }) => geographies.map((geography: any) => (
                    <Geography
                      key={geography.rsmKey}
                      geography={geography}
                      className="mesh-map__geography"
                    />
                  ))}
                </Geographies>

                {stage.routes.map((route) => {
                  const from = nodesById.get(route.fromNodeId);
                  const to = nodesById.get(route.toNodeId);
                  if (!from || !to || from.lon === null || from.lat === null || to.lon === null || to.lat === null) {
                    return null;
                  }

                  return (
                    <Line
                      key={`${route.fromNodeId}-${route.toNodeId}`}
                      from={[from.lon, from.lat]}
                      to={[to.lon, to.lat]}
                      className="mesh-map__route"
                    />
                  );
                })}

                {stage.regions.map((region) => {
                  if (region.lon === null || region.lat === null) {
                    return null;
                  }

                  const selected = selectedRegionId === region.id;
                  const activeCount = region.peerCount + (region.localNodeId ? 1 : 0);

                  return (
                    <Marker key={region.id} coordinates={[region.lon, region.lat]}>
                      <g
                        className={`mesh-region-marker ${selected ? "selected" : ""}`}
                        onClick={() => handleSelectRegion(region.id)}
                      >
                        <circle className="mesh-region-marker__halo" r={selected ? 8.2 : 6.8} />
                        <circle className="mesh-region-marker__core" r={selected ? 3.8 : 3.2} />
                        <text className="mesh-region-marker__label" y={selected ? -12 : -10}>
                          {region.city}
                        </text>
                        {activeCount > 0 ? (
                          <text className="mesh-region-marker__count" y={selected ? 16 : 14}>
                            {activeCount}
                          </text>
                        ) : null}
                      </g>
                    </Marker>
                  );
                })}

                {stage.nodes.map((node) => {
                  if (node.lon === null || node.lat === null) {
                    return null;
                  }

                  const selected = selectedNodeId === node.id;
                  return (
                    <Marker key={node.id} coordinates={[node.lon, node.lat]}>
                      <g
                        className={`mesh-agent-marker mesh-agent-marker--${node.kind} ${selected ? "selected" : ""}`}
                        onClick={() => handleSelectNode(node.id, node.regionId)}
                      >
                        <circle className="mesh-agent-marker__halo" r={node.kind === "local" ? 7 : 5.5} />
                        <circle className="mesh-agent-marker__core" r={node.kind === "local" ? 3.5 : 2.7} />
                        {selected ? (
                          <text className="mesh-agent-marker__label" y={node.kind === "local" ? -12 : -10}>
                            {node.title}
                          </text>
                        ) : null}
                      </g>
                    </Marker>
                  );
                })}
              </ZoomableGroup>
            </ComposableMap>
          </div>

          <div className="mesh-stage__legend">
            <span><i className="mesh-stage__legend-dot anchor" /> Rendezvous region</span>
            <span><i className="mesh-stage__legend-dot local" /> Local agent</span>
            <span><i className="mesh-stage__legend-dot peer" /> Live peer signal</span>
            <span><i className="mesh-stage__legend-dot route" /> Selected route</span>
          </div>

          {!localNode ? (
            <div className="mesh-stage__empty">
              <strong>No local mesh broadcaster</strong>
              <span>Start a local agent to publish its manifest and draw peer routes across the map.</span>
            </div>
          ) : null}

          {cardOpen && selectedManifest ? (
            <div className={`mesh-manifest-card ${cardCollapsed ? "collapsed" : ""}`}>
              <div className="mesh-manifest-card__header">
                <div>
                  <div className="mesh-manifest-card__eyebrow">
                    {selectedManifest.kind === "local" ? "Local agent manifest" : "Peer manifest"}
                  </div>
                  <strong>{selectedManifest.title}</strong>
                </div>
                <div className="mesh-manifest-card__actions">
                  <button
                    type="button"
                    className="cm-icon-btn"
                    aria-label={cardCollapsed ? "Expand manifest card" : "Collapse manifest card"}
                    onClick={() => setCardCollapsed((current) => !current)}
                  >
                    <Minus size={14} />
                  </button>
                  <button
                    type="button"
                    className="cm-icon-btn"
                    aria-label="Close manifest card"
                    onClick={() => {
                      setCardOpen(false);
                      setSelectedNodeId(null);
                    }}
                  >
                    <X size={14} />
                  </button>
                </div>
              </div>

              {!cardCollapsed ? (
                <div className="mesh-manifest-card__body">
                  <p>{selectedManifest.description}</p>
                  <div className="mesh-manifest-card__subtitle">{selectedManifest.subtitle}</div>

                  <div className="mesh-manifest-card__rows">
                    {selectedManifest.rows.map((row) => (
                      <div key={`${selectedManifest.nodeId}-${row.label}`} className="mesh-manifest-card__row">
                        <span>{row.label}</span>
                        <strong>{row.value}</strong>
                      </div>
                    ))}
                  </div>

                  {selectedManifest.tags.length > 0 ? (
                    <div className="mesh-manifest-card__tags">
                      {selectedManifest.tags.map((tag) => (
                        <span key={`${selectedManifest.nodeId}-${tag}`} className="plugin-tag">{tag}</span>
                      ))}
                    </div>
                  ) : null}
                </div>
              ) : null}
            </div>
          ) : null}
        </div>
      </ShellPanel>
    </section>
  );
}
