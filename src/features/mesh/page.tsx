import { startTransition, useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { DeckProps, PickingInfo } from "@deck.gl/core";
import { ArcLayer, GeoJsonLayer, PathLayer, ScatterplotLayer } from "@deck.gl/layers";
import { MapboxOverlay } from "@deck.gl/mapbox";
import { Activity, Minus, Radio, ScanSearch, Wallet, Waypoints, X } from "lucide-react";
import maplibregl, { type StyleSpecification } from "maplibre-gl";
import MapView, { Marker, useControl, type MapRef } from "react-map-gl/maplibre";
import { feature } from "topojson-client";
import worldAtlas from "world-atlas/countries-110m.json";
import { ShellPanel, ShellPill } from "@compose-market/theme/shell";
import type { InstalledAgent, MeshPeerSignal } from "../../lib/types";
import { buildMeshScene, type MeshBootstrapResolution } from "./model";
import { buildMeshStageModel } from "./stage-model";

interface MeshPageProps {
  agents: InstalledAgent[];
  peers: MeshPeerSignal[];
  bootstrapResolution: MeshBootstrapResolution;
}

interface RegionPoint {
  kind: "region";
  id: string;
  city: string;
  code: string | null;
  country: string | null;
  longitude: number;
  latitude: number;
  activityCount: number;
  peerCount: number;
  selected: boolean;
  highlighted: boolean;
}

interface NodePoint {
  kind: "node";
  id: string;
  regionId: string | null;
  title: string;
  subtitle: string;
  kindLabel: "local" | "peer";
  wallet: string | null;
  peerId: string | null;
  longitude: number;
  latitude: number;
  selected: boolean;
  highlighted: boolean;
  stale: boolean;
  signalCount: number;
  announceCount: number;
  nodeDistance: number;
}

interface RegionArcDatum {
  id: string;
  sourcePosition: [number, number];
  targetPosition: [number, number];
  count: number;
  selected: boolean;
}

interface NodeLinkDatum {
  id: string;
  sourcePosition: [number, number];
  targetPosition: [number, number];
  kind: "anchor" | "observed";
  selected: boolean;
  intensity: number;
}

interface HoverTarget {
  kind: "region" | "node";
  id: string;
  x: number;
  y: number;
  title: string;
  subtitle: string;
  detail: string;
}

const CYAN: [number, number, number] = [34, 211, 238];
const CYAN_SOFT: [number, number, number] = [103, 232, 249];
const MAGENTA: [number, number, number] = [217, 70, 239];
const SLATE: [number, number, number] = [148, 163, 184];
const MAP_STYLE: StyleSpecification = {
  version: 8,
  name: "Compose Mesh Blank",
  sources: {},
  layers: [
    {
      id: "mesh-background",
      type: "background",
      paint: {
        "background-color": "#020617",
      },
    },
  ],
};
const INITIAL_VIEW_STATE = {
  longitude: 10,
  latitude: 18,
  zoom: 1.15,
  bearing: -6,
  pitch: 24,
};
const WORLD_GEOJSON = feature(
  worldAtlas as never,
  (worldAtlas as { objects: { countries: unknown } }).objects.countries as never,
) as unknown as GeoJSON.FeatureCollection;
const GRATICULE_PATHS = buildGraticulePaths();

function DeckGLOverlay(props: DeckProps) {
  const overlay = useControl<MapboxOverlay>(() => new MapboxOverlay({ interleaved: false, ...props }));
  overlay.setProps(props);
  return null;
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function buildGraticulePaths(): Array<{ id: string; path: [number, number][]; emphasis: boolean }> {
  const paths: Array<{ id: string; path: [number, number][]; emphasis: boolean }> = [];

  for (let longitude = -150; longitude <= 180; longitude += 30) {
    const path: [number, number][] = [];
    for (let latitude = -70; latitude <= 80; latitude += 5) {
      path.push([longitude, latitude]);
    }
    paths.push({
      id: `lon-${longitude}`,
      path,
      emphasis: longitude % 60 === 0,
    });
  }

  for (let latitude = -60; latitude <= 60; latitude += 20) {
    const path: [number, number][] = [];
    for (let longitude = -180; longitude <= 180; longitude += 6) {
      path.push([longitude, latitude]);
    }
    paths.push({
      id: `lat-${latitude}`,
      path,
      emphasis: latitude === 0,
    });
  }

  return paths;
}

function shortWallet(value: string | null): string {
  if (!value) {
    return "No local agent";
  }
  return `${value.slice(0, 8)}...${value.slice(-4)}`;
}

function formatNodeMeta(node: NodePoint): string {
  if (node.kindLabel === "local") {
    return node.wallet ? shortWallet(node.wallet) : "This device";
  }
  if (node.wallet) {
    return shortWallet(node.wallet);
  }
  if (node.peerId) {
    return `${node.peerId.slice(0, 10)}...${node.peerId.slice(-6)}`;
  }
  return "Mesh peer";
}

function formatRegionMeta(region: RegionPoint): string {
  const label = region.code ? region.code.toUpperCase() : "GLOBAL";
  return region.country ? `${label} / ${region.country}` : label;
}

function labelSide(region: { x: number; y: number }): "east" | "west" | "north" | "south" {
  if (region.x <= 34) {
    return "east";
  }
  if (region.x >= 66) {
    return "west";
  }
  return region.y < 38 ? "south" : "north";
}

export function MeshPage({ agents, peers, bootstrapResolution }: MeshPageProps) {
  const mapRef = useRef<MapRef | null>(null);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [selectedRegionId, setSelectedRegionId] = useState<string | null>(null);
  const [cardCollapsed, setCardCollapsed] = useState(false);
  const [hoverTarget, setHoverTarget] = useState<HoverTarget | null>(null);
  const [animationTick, setAnimationTick] = useState(0);

  useEffect(() => {
    let frameId = 0;
    let previousCommit = 0;

    const step = (timestamp: number) => {
      if (timestamp - previousCommit >= 48) {
        previousCommit = timestamp;
        setAnimationTick(timestamp);
      }
      frameId = window.requestAnimationFrame(step);
    };

    frameId = window.requestAnimationFrame(step);
    return () => window.cancelAnimationFrame(frameId);
  }, []);

  const scene = useMemo(() => buildMeshScene({ peers, resolution: bootstrapResolution }), [bootstrapResolution, peers]);
  const stage = useMemo(
    () => buildMeshStageModel({ agents, peers, scene, selectedNodeId, selectedRegionId }),
    [agents, peers, scene, selectedNodeId, selectedRegionId],
  );

  const nodesById = useMemo(() => new Map(stage.nodes.map((node) => [node.id, node])), [stage.nodes]);
  const regionsById = useMemo(() => new Map(stage.regions.map((region) => [region.id, region])), [stage.regions]);
  const primaryAgent = agents[0] || null;
  const localNode = useMemo(() => stage.nodes.find((node) => node.kind === "local") || null, [stage.nodes]);
  const selectedNode = selectedNodeId ? nodesById.get(selectedNodeId) || null : null;
  const focusedRegion = (
    (selectedRegionId ? regionsById.get(selectedRegionId) : null)
    || (selectedNode?.regionId ? regionsById.get(selectedNode.regionId) : null)
    || null
  );
  const focusedRegionId = focusedRegion?.id || null;
  const activeRegions = stage.regions.filter((region) => region.peerCount > 0 || region.localNodeIds.length > 0).length;
  const viewerDormant = !localNode || localNode.lon === null || localNode.lat === null;

  useEffect(() => {
    if (selectedNodeId && !nodesById.has(selectedNodeId)) {
      setSelectedNodeId(null);
      setCardCollapsed(false);
    }
  }, [nodesById, selectedNodeId]);

  const flyToWorld = useCallback(() => {
    mapRef.current?.flyTo({
      center: [INITIAL_VIEW_STATE.longitude, INITIAL_VIEW_STATE.latitude],
      zoom: INITIAL_VIEW_STATE.zoom,
      bearing: INITIAL_VIEW_STATE.bearing,
      pitch: INITIAL_VIEW_STATE.pitch,
      duration: 900,
      essential: true,
    });
  }, []);

  const flyToRegion = useCallback((regionId: string | null) => {
    if (!regionId) {
      flyToWorld();
      return;
    }

    const region = regionsById.get(regionId);
    if (!region || region.lon === null || region.lat === null) {
      flyToWorld();
      return;
    }

    mapRef.current?.flyTo({
      center: [region.lon, region.lat],
      zoom: 3.35,
      bearing: -10,
      pitch: 36,
      duration: 900,
      essential: true,
    });
  }, [flyToWorld, regionsById]);

  const handleSelectRegion = useCallback((regionId: string) => {
    startTransition(() => {
      setHoverTarget(null);
      setSelectedRegionId((current) => {
        const next = current === regionId ? null : regionId;
        window.requestAnimationFrame(() => flyToRegion(next));
        return next;
      });
      setSelectedNodeId((current) => {
        if (!current) {
          return current;
        }
        return nodesById.get(current)?.regionId === regionId ? current : null;
      });
      setCardCollapsed(false);
    });
  }, [flyToRegion, nodesById]);

  const handleSelectNode = useCallback((nodeId: string, regionId: string | null) => {
    startTransition(() => {
      setHoverTarget(null);
      setSelectedNodeId(nodeId);
      setSelectedRegionId(regionId);
      setCardCollapsed(false);
      if (regionId) {
        window.requestAnimationFrame(() => flyToRegion(regionId));
      }
    });
  }, [flyToRegion]);

  const regionPoints = useMemo(() => stage.regions.flatMap((region) => {
    if (region.lon === null || region.lat === null) {
      return [];
    }

    return [{
      kind: "region" as const,
      id: region.id,
      city: region.city,
      code: region.code,
      country: region.country,
      longitude: region.lon,
      latitude: region.lat,
      activityCount: region.peerCount + region.localNodeIds.length,
      peerCount: region.peerCount,
      selected: focusedRegionId === region.id,
      highlighted: Boolean(
        focusedRegionId === region.id
        || hoverTarget?.id === region.id
        || (selectedNode && selectedNode.regionId === region.id),
      ),
    }];
  }), [focusedRegionId, hoverTarget?.id, selectedNode, stage.regions]);

  const nodePoints = useMemo(() => stage.nodes.flatMap((node) => {
    if (node.lon === null || node.lat === null) {
      return [];
    }

    return [{
      kind: "node" as const,
      id: node.id,
      regionId: node.regionId,
      title: node.title,
      subtitle: node.subtitle,
      kindLabel: node.kind,
      wallet: node.wallet,
      peerId: node.peerId,
      longitude: node.lon,
      latitude: node.lat,
      selected: selectedNodeId === node.id,
      highlighted: Boolean(
        selectedNodeId === node.id
        || hoverTarget?.id === node.id
        || (focusedRegionId && node.regionId === focusedRegionId),
      ),
      stale: node.stale,
      signalCount: node.signalCount,
      announceCount: node.announceCount,
      nodeDistance: node.nodeDistance,
    }];
  }), [focusedRegionId, hoverTarget?.id, selectedNodeId, stage.nodes]);

  const regionLinks = useMemo(() => stage.regionLinks.flatMap((link) => {
    const from = regionsById.get(link.fromRegionId);
    const to = regionsById.get(link.toRegionId);
    if (!from || !to || from.lon === null || from.lat === null || to.lon === null || to.lat === null) {
      return [];
    }

    return [{
      id: link.id,
      sourcePosition: [from.lon, from.lat] as [number, number],
      targetPosition: [to.lon, to.lat] as [number, number],
      count: link.count,
      selected: link.selected,
    }];
  }), [regionsById, stage.regionLinks]);

  const nodeLinks = useMemo(() => stage.nodeLinks.flatMap((link) => {
    const fromNode = nodesById.get(link.fromNodeId) || regionsById.get(link.fromNodeId) || null;
    const toNode = nodesById.get(link.toNodeId) || regionsById.get(link.toNodeId) || null;
    if (!fromNode || !toNode || fromNode.lon === null || fromNode.lat === null || toNode.lon === null || toNode.lat === null) {
      return [];
    }

    return [{
      id: link.id,
      sourcePosition: [fromNode.lon, fromNode.lat] as [number, number],
      targetPosition: [toNode.lon, toNode.lat] as [number, number],
      kind: link.kind,
      selected: link.selected,
      intensity: link.intensity,
    }];
  }), [nodesById, regionsById, stage.nodeLinks]);

  const observedNodeLinks = useMemo(() => nodeLinks.filter((link) => link.kind === "observed"), [nodeLinks]);
  const anchorNodeLinks = useMemo(() => nodeLinks.filter((link) => link.kind === "anchor"), [nodeLinks]);

  const handleHover = useCallback((info: PickingInfo<RegionPoint | NodePoint>) => {
    const item = info.object;
    if (!item) {
      setHoverTarget(null);
      return;
    }

    if (item.kind === "region") {
      setHoverTarget({
        kind: "region",
        id: item.id,
        x: info.x,
        y: info.y,
        title: item.city,
        subtitle: `${item.activityCount} active node${item.activityCount === 1 ? "" : "s"}`,
        detail: formatRegionMeta(item),
      });
      return;
    }

    setHoverTarget({
      kind: "node",
      id: item.id,
      x: info.x,
      y: info.y,
      title: item.title,
      subtitle: item.subtitle,
      detail: formatNodeMeta(item),
    });
  }, []);

  const handleLayerClick = useCallback((info: PickingInfo<RegionPoint | NodePoint>) => {
    const item = info.object;
    if (!item) {
      setHoverTarget(null);
      setSelectedNodeId(null);
      setSelectedRegionId(null);
      setCardCollapsed(false);
      flyToWorld();
      return;
    }

    if (item.kind === "region") {
      handleSelectRegion(item.id);
      return;
    }

    handleSelectNode(item.id, item.regionId);
  }, [flyToWorld, handleSelectNode, handleSelectRegion]);

  const handleHoverRef = useRef(handleHover);
  const handleLayerClickRef = useRef(handleLayerClick);
  useEffect(() => { handleHoverRef.current = handleHover; }, [handleHover]);
  useEffect(() => { handleLayerClickRef.current = handleLayerClick; }, [handleLayerClick]);
  const stableHover = useCallback((info: PickingInfo<RegionPoint | NodePoint>) => handleHoverRef.current(info), []);
  const stableClick = useCallback((info: PickingInfo<RegionPoint | NodePoint>) => handleLayerClickRef.current(info), []);

  const staticLayers = useMemo(() => [
    new GeoJsonLayer({
      id: "mesh-land",
      data: WORLD_GEOJSON,
      pickable: false,
      stroked: true,
      filled: true,
      lineWidthUnits: "pixels",
      lineWidthMinPixels: 1,
      getFillColor: [6, 15, 30, 168],
      getLineColor: [38, 76, 102, 124],
      getLineWidth: 1,
    }),
    new PathLayer({
      id: "mesh-graticule",
      data: GRATICULE_PATHS,
      pickable: false,
      widthUnits: "pixels",
      rounded: true,
      getPath: (path) => path.path,
      getWidth: (path) => (path.emphasis ? 1.2 : 0.75),
      getColor: (path) => (path.emphasis ? [CYAN[0], CYAN[1], CYAN[2], 50] : [CYAN[0], CYAN[1], CYAN[2], 24]),
    }),
    new ArcLayer<RegionArcDatum>({
      id: "mesh-region-links",
      data: regionLinks,
      pickable: false,
      widthUnits: "pixels",
      getSourcePosition: (link) => link.sourcePosition,
      getTargetPosition: (link) => link.targetPosition,
      getSourceColor: (link) => [CYAN[0], CYAN[1], CYAN[2], link.selected ? 184 : 96],
      getTargetColor: (link) => [MAGENTA[0], MAGENTA[1], MAGENTA[2], link.selected ? 168 : 86],
      getWidth: (link) => 1.2 + (link.count * 0.9),
      getHeight: (link) => 0.18 + (link.count * 0.05),
      getTilt: () => 12,
    }),
    new ArcLayer<NodeLinkDatum>({
      id: "mesh-observed-links",
      data: observedNodeLinks,
      pickable: false,
      widthUnits: "pixels",
      getSourcePosition: (link) => link.sourcePosition,
      getTargetPosition: (link) => link.targetPosition,
      getSourceColor: (link) => [CYAN_SOFT[0], CYAN_SOFT[1], CYAN_SOFT[2], link.selected ? 204 : 62],
      getTargetColor: (link) => [MAGENTA[0], MAGENTA[1], MAGENTA[2], link.selected ? 184 : 54],
      getWidth: (link) => 0.8 + (link.intensity * 2.1),
      getHeight: (link) => 0.12 + (link.intensity * 0.08),
      getTilt: () => -10,
    }),
    new ArcLayer<NodeLinkDatum>({
      id: "mesh-anchor-links",
      data: anchorNodeLinks,
      pickable: false,
      widthUnits: "pixels",
      getSourcePosition: (link) => link.sourcePosition,
      getTargetPosition: (link) => link.targetPosition,
      getSourceColor: (link) => [CYAN[0], CYAN[1], CYAN[2], link.selected ? 126 : 52],
      getTargetColor: (link) => [CYAN_SOFT[0], CYAN_SOFT[1], CYAN_SOFT[2], link.selected ? 172 : 72],
      getWidth: (link) => 0.7 + (link.intensity * 1.2),
      getHeight: (link) => 0.08 + (link.intensity * 0.05),
      getTilt: () => 0,
    }),
    new ScatterplotLayer<RegionPoint>({
      id: "mesh-region-cores",
      data: regionPoints,
      pickable: false,
      stroked: false,
      filled: true,
      radiusUnits: "pixels",
      getPosition: (region) => [region.longitude, region.latitude],
      getRadius: (region) => 3.8 + Math.min(region.activityCount, 4),
      getFillColor: (region) => region.selected ? [CYAN_SOFT[0], CYAN_SOFT[1], CYAN_SOFT[2], 255] : [CYAN[0], CYAN[1], CYAN[2], 224],
    }),
    new ScatterplotLayer<NodePoint>({
      id: "mesh-node-cores",
      data: nodePoints,
      pickable: false,
      stroked: true,
      filled: true,
      radiusUnits: "pixels",
      lineWidthUnits: "pixels",
      getPosition: (node) => [node.longitude, node.latitude],
      getRadius: (node) => (node.kindLabel === "local" ? 6.6 : node.selected ? 4.8 : 3.5),
      getFillColor: (node) => {
        if (node.kindLabel === "local") {
          return [CYAN_SOFT[0], CYAN_SOFT[1], CYAN_SOFT[2], 255];
        }
        if (node.stale) {
          return [SLATE[0], SLATE[1], SLATE[2], 205];
        }
        return [MAGENTA[0], MAGENTA[1], MAGENTA[2], 240];
      },
      getLineColor: (node) => {
        if (node.kindLabel === "local") {
          return [CYAN[0], CYAN[1], CYAN[2], 255];
        }
        return node.selected
          ? [CYAN_SOFT[0], CYAN_SOFT[1], CYAN_SOFT[2], 255]
          : [19, 32, 44, 255];
      },
      getLineWidth: (node) => (node.selected ? 2.2 : 1.1),
    }),
  ], [anchorNodeLinks, nodePoints, observedNodeLinks, regionLinks, regionPoints]);

  const animatedLayers = useMemo(() => {
    const wave = (Math.sin(animationTick / 420) + 1) / 2;

    return [
      new ScatterplotLayer<RegionPoint>({
        id: "mesh-region-halos",
        data: regionPoints,
        pickable: true,
        stroked: true,
        filled: true,
        radiusUnits: "pixels",
        lineWidthUnits: "pixels",
        getPosition: (region) => [region.longitude, region.latitude],
        getRadius: (region) => 12 + (region.activityCount * 2.8) + (wave * (region.activityCount > 0 ? 12 : 5)),
        getFillColor: (region) => [CYAN[0], CYAN[1], CYAN[2], region.highlighted ? 34 : 18],
        getLineColor: (region) => region.selected ? [CYAN_SOFT[0], CYAN_SOFT[1], CYAN_SOFT[2], 210] : [CYAN[0], CYAN[1], CYAN[2], 126],
        getLineWidth: (region) => region.selected ? 2.8 : 1.2,
        onHover: stableHover,
        onClick: stableClick,
      }),
      new ScatterplotLayer<NodePoint>({
        id: "mesh-node-pulses",
        data: nodePoints,
        pickable: true,
        stroked: false,
        filled: true,
        radiusUnits: "pixels",
        getPosition: (node) => [node.longitude, node.latitude],
        getRadius: (node) => {
          if (node.kindLabel === "local") {
            return 12 + (wave * 14);
          }
          const freshness = node.stale ? 0.55 : 1;
          return 5 + (freshness * 9) + (wave * (6 + Math.min(node.signalCount, 4)));
        },
        getFillColor: (node) => {
          if (node.kindLabel === "local") {
            return [CYAN[0], CYAN[1], CYAN[2], node.selected ? 62 : 34];
          }
          if (node.stale) {
            return [SLATE[0], SLATE[1], SLATE[2], 28];
          }
          return [MAGENTA[0], MAGENTA[1], MAGENTA[2], node.selected ? 68 : 34];
        },
        onHover: stableHover,
        onClick: stableClick,
      }),
    ];
  }, [animationTick, nodePoints, regionPoints, stableClick, stableHover]);

  const layers = useMemo(() => [...staticLayers, ...animatedLayers], [staticLayers, animatedLayers]);

  return (
    <section className="mesh-page">
      <ShellPanel className="mesh-stage" padded={false}>
        <div className="mesh-stage__canvas">
          <div className="mesh-stage__aurora" aria-hidden="true" />
          <div className="mesh-stage__noise" aria-hidden="true" />
          <div className="mesh-stage__scanband" aria-hidden="true" />
          <div className="mesh-stage__vignette" aria-hidden="true" />

          <div className="mesh-map">
            <MapView
              ref={mapRef}
              mapLib={maplibregl}
              reuseMaps
              mapStyle={MAP_STYLE}
              initialViewState={INITIAL_VIEW_STATE}
              maxZoom={6}
              minZoom={0.8}
              attributionControl={false}
              renderWorldCopies={false}
              style={{ width: "100%", height: "100%" }}
            >
              <DeckGLOverlay layers={layers} />

              {regionPoints.map((region) => (
                <Marker key={region.id} longitude={region.longitude} latitude={region.latitude} anchor="center">
                  <div className={`mesh-region-label ${region.selected ? "selected" : ""}`} data-side={labelSide(regionsById.get(region.id) || { x: 50, y: 50 })}>
                    <strong>{region.city}</strong>
                    <span>{formatRegionMeta(region)}</span>
                  </div>
                </Marker>
              ))}
            </MapView>
          </div>

          <div className="mesh-stage__hud">
            <div className="mesh-stage__titleblock">
              <div className="mesh-stage__eyebrow">Network</div>
              <h1>Mesh Topology</h1>
              <p>Let your agent <i>mesh</i> with its peers, in real time.</p>
            </div>

            <div className="mesh-toolbar">
              <ShellPill className="mesh-stat-pill">
                <Activity size={14} />
                <span>{peers.length} peers available</span>
              </ShellPill>
              <ShellPill className="mesh-stat-pill">
                <Radio size={14} />
                <span>{activeRegions || stage.regions.length} active regions</span>
              </ShellPill>
              <ShellPill className="mesh-stat-pill">
                <Waypoints size={14} />
                <span>{primaryAgent?.network.status || "observer"}</span>
              </ShellPill>
              <ShellPill className="mesh-stat-pill">
                <Wallet size={14} />
                <span>{shortWallet(localNode?.wallet || null)}</span>
              </ShellPill>
            </div>
          </div>

          {viewerDormant ? (
            <div className="mesh-viewer-node" aria-hidden="true">
              <div className="mesh-viewer-node__rings">
                <span />
                <span />
                <span />
              </div>
              <div className="mesh-viewer-node__core">
                <ScanSearch size={18} />
              </div>
              <div className="mesh-viewer-node__label">
                <strong>This Device</strong>
                <span>{localNode ? "Locating..." : "Waiting for Mesh activation"}</span>
              </div>
            </div>
          ) : null}

          <div className="mesh-stage__legend">
            <span><i className="mesh-stage__legend-dot anchor" /> Relay region</span>
            <span><i className="mesh-stage__legend-dot peer" /> Live peer</span>
            <span><i className="mesh-stage__legend-dot link" /> Observed path</span>
            <span><i className="mesh-stage__legend-dot field" /> Cross-region corridor</span>
          </div>

          {hoverTarget && !(hoverTarget.kind === "node" && hoverTarget.id === selectedNodeId) ? (
            <div
              className="mesh-hover-card"
              style={{
                left: clamp(hoverTarget.x + 18, 16, window.innerWidth - 280),
                top: clamp(hoverTarget.y + 18, 16, window.innerHeight - 160),
              }}
            >
              <div className="mesh-hover-card__eyebrow">{hoverTarget.kind === "node" ? "Peer signal" : "Relay region"}</div>
              <strong>{hoverTarget.title}</strong>
              <span>{hoverTarget.subtitle}</span>
              <small>{hoverTarget.detail}</small>
            </div>
          ) : null}

          {stage.selectedManifest ? (
            <div className={`mesh-manifest-card ${cardCollapsed ? "collapsed" : ""}`}>
              <div className="mesh-manifest-card__header">
                <div>
                  <div className="mesh-manifest-card__eyebrow">
                    {stage.selectedManifest.kind === "local" ? "Local agent manifest" : "Peer manifest"}
                  </div>
                  <strong>{stage.selectedManifest.title}</strong>
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
                      setSelectedNodeId(null);
                      setCardCollapsed(false);
                    }}
                  >
                    <X size={14} />
                  </button>
                </div>
              </div>

              {!cardCollapsed ? (
                <div className="mesh-manifest-card__body">
                  <p>{stage.selectedManifest.description}</p>
                  <div className="mesh-manifest-card__subtitle">{stage.selectedManifest.subtitle}</div>

                  <div className="mesh-manifest-card__rows">
                    {stage.selectedManifest.rows.map((row) => (
                      <div key={`${stage.selectedManifest?.nodeId}-${row.label}`} className="mesh-manifest-card__row">
                        <span>{row.label}</span>
                        <strong>{row.value}</strong>
                      </div>
                    ))}
                  </div>

                  {stage.selectedManifest.tags.length > 0 ? (
                    <div className="mesh-manifest-card__tags">
                      {stage.selectedManifest.tags.map((tag) => (
                        <span key={`${stage.selectedManifest?.nodeId}-${tag}`} className="plugin-tag">{tag}</span>
                      ))}
                    </div>
                  ) : null}
                </div>
              ) : null}
            </div>
          ) : null}

          {!localNode ? (
            <div className="mesh-stage__status">
              <strong>No agent broadcasting.</strong>
              <span>Enable "Mesh" in your Agent's Settings to join the network.</span>
            </div>
          ) : peers.length === 0 ? (
            <div className="mesh-stage__status">
              <strong>Online. Scanning for peers.</strong>
              <span>Your agent is <i>meshing</i>. Peers will appear here as they come online.</span>
            </div>
          ) : null}
        </div>
      </ShellPanel>
    </section>
  );
}
