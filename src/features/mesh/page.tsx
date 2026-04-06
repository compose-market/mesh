import { startTransition, useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { DeckProps, PickingInfo } from "@deck.gl/core";
import { ArcLayer, GeoJsonLayer, PathLayer, ScatterplotLayer, TextLayer } from "@deck.gl/layers";
import { MapboxOverlay } from "@deck.gl/mapbox";
import { Activity, MapPin, Minus, Radio, ScanSearch, Wallet, Waypoints, X } from "lucide-react";
import maplibregl, { type StyleSpecification } from "maplibre-gl";
import MapView, { useControl, type MapRef } from "react-map-gl/maplibre";
import { feature } from "topojson-client";
import worldAtlas from "world-atlas/countries-110m.json";
import { ShellButton, ShellNotice, ShellPanel, ShellPill } from "@compose-market/theme/shell";
import type { InstalledAgent, MeshPeerSignal, OsPermissionStatus } from "../../lib/types";
import {
  buildMeshScene,
  extractPublicDirectMeshEndpoint,
  type MeshBootstrapResolution,
} from "./model";
import { buildMeshStageModel, type MeshLocalDeviceLocation } from "./stage-model";
import type { MeshRuntimeStatus } from "./runtime";

interface MeshPageProps {
  agents: InstalledAgent[];
  peers: MeshPeerSignal[];
  bootstrapResolution: MeshBootstrapResolution;
  runtimeStatus: MeshRuntimeStatus;
  locationPermission: OsPermissionStatus;
  onEnableLocation: () => Promise<void>;
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

interface RegionLabelDatum {
  id: string;
  position: [number, number];
  city: string;
  code: string;
  selected: boolean;
  side: "east" | "west" | "north" | "south";
}

const DEVICE_LOCATION_CACHE_KEY = "compose_mesh_local_device_location_v1";
const DEVICE_LOCATION_CACHE_TTL_MS = 30 * 60 * 1_000;

/* ── Brand-exact color palette ───────────────────────────────────── */
/* Primary  hsl(188 95% 43%)  → rgb(5, 175, 214)  */
/* Accent   hsl(292 85% 55%)  → rgb(198, 41, 224) */
const CYAN: [number, number, number] = [5, 175, 214];
const CYAN_SOFT: [number, number, number] = [76, 211, 235];
const MAGENTA: [number, number, number] = [198, 41, 224];
const SLATE: [number, number, number] = [107, 114, 128];

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

/* ── DeckGL overlay with imperative ref ──────────────────────────── */

function DeckGLOverlay(props: DeckProps & { overlayRef?: React.RefObject<MapboxOverlay | null> }) {
  const overlay = useControl<MapboxOverlay>(() => new MapboxOverlay({ interleaved: false, ...props }));
  overlay.setProps(props);
  if (props.overlayRef && "current" in props.overlayRef) {
    (props.overlayRef as React.MutableRefObject<MapboxOverlay | null>).current = overlay;
  }
  return null;
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function buildGraticulePaths(): Array<{ id: string; path: [number, number][]; emphasis: boolean; isLatitude: boolean }> {
  const paths: Array<{ id: string; path: [number, number][]; emphasis: boolean; isLatitude: boolean }> = [];

  for (let longitude = -150; longitude <= 180; longitude += 30) {
    const path: [number, number][] = [];
    for (let latitude = -70; latitude <= 80; latitude += 5) {
      path.push([longitude, latitude]);
    }
    paths.push({
      id: `lon-${longitude}`,
      path,
      emphasis: longitude % 60 === 0,
      isLatitude: false,
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
      isLatitude: true,
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

function labelAnchor(side: "east" | "west" | "north" | "south"): "start" | "end" | "middle" {
  if (side === "east") return "start";
  if (side === "west") return "end";
  return "middle";
}

function labelOffset(side: "east" | "west" | "north" | "south"): [number, number] {
  if (side === "east") return [20, -2];
  if (side === "west") return [-20, -2];
  if (side === "north") return [0, -18];
  return [0, 18];
}

function readCachedDeviceLocation(): MeshLocalDeviceLocation | null {
  if (typeof window === "undefined") {
    return null;
  }
  const raw = window.localStorage.getItem(DEVICE_LOCATION_CACHE_KEY);
  if (!raw) {
    return null;
  }
  try {
    const parsed = JSON.parse(raw) as MeshLocalDeviceLocation & { updatedAt?: number };
    const updatedAt = Number.isFinite(parsed.updatedAt) ? Number(parsed.updatedAt) : 0;
    if (Date.now() - updatedAt > DEVICE_LOCATION_CACHE_TTL_MS) {
      return null;
    }
    if (!Number.isFinite(parsed.lat) || !Number.isFinite(parsed.lon)) {
      return null;
    }
    return {
      lat: Number(parsed.lat),
      lon: Number(parsed.lon),
      city: typeof parsed.city === "string" ? parsed.city : null,
      country: typeof parsed.country === "string" ? parsed.country : null,
      label: typeof parsed.label === "string" && parsed.label.trim().length > 0 ? parsed.label : "Current device",
    };
  } catch {
    return null;
  }
}

function persistDeviceLocation(location: MeshLocalDeviceLocation): void {
  if (typeof window === "undefined") {
    return;
  }
  window.localStorage.setItem(DEVICE_LOCATION_CACHE_KEY, JSON.stringify({
    ...location,
    updatedAt: Date.now(),
  }));
}

function readFiniteNumber(value: unknown): number | null {
  return Number.isFinite(value) ? Number(value) : null;
}

async function resolveBrowserDeviceLocation(): Promise<MeshLocalDeviceLocation | null> {
  if (typeof navigator === "undefined" || !("geolocation" in navigator)) {
    return null;
  }

  return new Promise((resolve) => {
    navigator.geolocation.getCurrentPosition(
      (position) => {
        resolve({
          lat: position.coords.latitude,
          lon: position.coords.longitude,
          city: null,
          country: null,
          label: "Current device",
        });
      },
      () => resolve(null),
      {
        enableHighAccuracy: true,
        timeout: 8_000,
        maximumAge: 5 * 60 * 1_000,
      },
    );
  });
}

async function resolveIpDeviceLocation(): Promise<MeshLocalDeviceLocation | null> {
  return resolveIpDeviceLocationFor(null);
}

async function resolveIpDeviceLocationFor(ipAddress: string | null): Promise<MeshLocalDeviceLocation | null> {
  const controller = new AbortController();
  const timeoutId = window.setTimeout(() => controller.abort(), 4_000);
  try {
    const lookupTarget = ipAddress ? `${encodeURIComponent(ipAddress)}/` : "";
    const response = await fetch(`https://ipapi.co/${lookupTarget}json/`, {
      signal: controller.signal,
    });
    if (!response.ok) {
      return null;
    }
    const payload = await response.json() as {
      latitude?: number;
      longitude?: number;
      city?: string;
      country_name?: string;
      country?: string;
    };
    const lat = readFiniteNumber(payload.latitude);
    const lon = readFiniteNumber(payload.longitude);
    if (lat === null || lon === null) {
      return null;
    }
    const city = typeof payload.city === "string" && payload.city.trim().length > 0 ? payload.city.trim() : null;
    const country = typeof payload.country_name === "string" && payload.country_name.trim().length > 0
      ? payload.country_name.trim()
      : typeof payload.country === "string" && payload.country.trim().length > 0
        ? payload.country.trim()
        : null;
    const label = city
      ? country ? `${city}, ${country}` : city
      : country || "Current device";
    return { lat, lon, city, country, label };
  } catch {
    return null;
  } finally {
    window.clearTimeout(timeoutId);
  }
}

export function MeshPage({
  agents,
  peers,
  bootstrapResolution,
  runtimeStatus,
  locationPermission,
  onEnableLocation,
}: MeshPageProps) {
  const mapRef = useRef<MapRef | null>(null);
  const overlayRef = useRef<MapboxOverlay | null>(null);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [selectedRegionId, setSelectedRegionId] = useState<string | null>(null);
  const [cardCollapsed, setCardCollapsed] = useState(false);
  const [hoverTarget, setHoverTarget] = useState<HoverTarget | null>(null);
  const [localDeviceLocation, setLocalDeviceLocation] = useState<MeshLocalDeviceLocation | null>(() => readCachedDeviceLocation());
  const [requestingLocation, setRequestingLocation] = useState(false);
  const directMeshEndpoint = useMemo(() => {
    for (const agent of agents) {
      if (!agent.network.enabled) {
        continue;
      }
      const endpoint = extractPublicDirectMeshEndpoint([
        ...agent.network.listenMultiaddrs,
        ...(agent.network.manifest?.listenMultiaddrs || []),
      ]);
      if (endpoint) {
        return endpoint;
      }
    }
    return null;
  }, [agents]);

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      const resolved = await resolveBrowserDeviceLocation();
      if (!resolved || cancelled) {
        if (localDeviceLocation) {
          return;
        }
        const fallback = await resolveIpDeviceLocation();
        if (!fallback || cancelled) {
          return;
        }
        persistDeviceLocation(fallback);
        setLocalDeviceLocation((current) => current || fallback);
        return;
      }
      persistDeviceLocation(resolved);
      setLocalDeviceLocation((current) => {
        if (
          current
          && current.lat === resolved.lat
          && current.lon === resolved.lon
          && current.city === resolved.city
          && current.country === resolved.country
          && current.label === resolved.label
        ) {
          return current;
        }
        return resolved;
      });
    })();
    return () => {
      cancelled = true;
    };
  }, [locationPermission]);

  useEffect(() => {
    if (!directMeshEndpoint) {
      return;
    }
    let cancelled = false;
    void (async () => {
      const resolved = await resolveIpDeviceLocationFor(directMeshEndpoint.value);
      if (!resolved || cancelled) {
        return;
      }
      persistDeviceLocation(resolved);
      setLocalDeviceLocation((current) => {
        if (
          current
          && current.lat === resolved.lat
          && current.lon === resolved.lon
          && current.city === resolved.city
          && current.country === resolved.country
          && current.label === resolved.label
        ) {
          return current;
        }
        return resolved;
      });
    })();
    return () => {
      cancelled = true;
    };
  }, [directMeshEndpoint]);

  const scene = useMemo(() => buildMeshScene({ peers, resolution: bootstrapResolution }), [bootstrapResolution, peers]);
  const stage = useMemo(
    () => buildMeshStageModel({
      agents,
      peers,
      scene,
      selectedNodeId,
      selectedRegionId,
      localDeviceLocation,
      runtimeRelayPeerId: runtimeStatus.relayPeerId,
    }),
    [agents, localDeviceLocation, peers, runtimeStatus.relayPeerId, scene, selectedNodeId, selectedRegionId],
  );

  const nodesById = useMemo(() => new Map(stage.nodes.map((node) => [node.id, node])), [stage.nodes]);
  const regionsById = useMemo(() => new Map(stage.regions.map((region) => [region.id, region])), [stage.regions]);
  const localNodes = useMemo(() => stage.nodes.filter((node) => node.kind === "local"), [stage.nodes]);
  const localNode = localNodes[0] || null;
  const selectedNode = selectedNodeId ? nodesById.get(selectedNodeId) || null : null;
  const focusedRegion = (
    (selectedRegionId ? regionsById.get(selectedRegionId) : null)
    || (selectedNode?.regionId ? regionsById.get(selectedNode.regionId) : null)
    || null
  );
  const focusedRegionId = focusedRegion?.id || null;
  const activeRegions = stage.regions.filter((region) => region.peerCount > 0 || region.localNodeIds.length > 0).length;
  const viewerDormant = localNodes.length === 0 || localNodes.every((node) => node.lon === null || node.lat === null);
  const hasExactLocalPlacement = localNodes.some((node) => node.regionId === "__local_device__");
  const showLocationPrompt = agents.some((agent) => agent.network.enabled)
    && locationPermission !== "granted"
    && !hasExactLocalPlacement;

  const handleEnableLocation = useCallback(() => {
    setRequestingLocation(true);
    void onEnableLocation()
      .catch(() => {})
      .finally(() => {
        setRequestingLocation(false);
      });
  }, [onEnableLocation]);

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

  /* ── GPU text labels derived from region points ──────────────── */
  const regionLabels = useMemo(() => regionPoints.map((region) => {
    const side = labelSide(regionsById.get(region.id) || { x: 50, y: 50 });
    return {
      id: region.id,
      position: [region.longitude, region.latitude] as [number, number],
      city: region.city,
      code: formatRegionMeta(region),
      selected: region.selected,
      side,
    };
  }), [regionPoints, regionsById]);

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

  /* ── Static layers (rebuilt only when data changes) ──────────── */
  const staticLayers = useMemo(() => [
    new GeoJsonLayer({
      id: "mesh-land",
      data: WORLD_GEOJSON,
      pickable: false,
      stroked: true,
      filled: true,
      lineWidthUnits: "pixels",
      lineWidthMinPixels: 1,
      getFillColor: [4, 12, 28, 178],
      getLineColor: [CYAN[0], CYAN[1], CYAN[2], 72],
      getLineWidth: 1.4,
    }),
    new PathLayer({
      id: "mesh-graticule",
      data: GRATICULE_PATHS,
      pickable: false,
      widthUnits: "pixels",
      rounded: true,
      getPath: (path) => path.path,
      getWidth: (path) => (path.emphasis ? 1.2 : 0.65),
      getColor: (path) => {
        if (path.emphasis) {
          return [CYAN[0], CYAN[1], CYAN[2], 55];
        }
        return path.isLatitude
          ? [MAGENTA[0], MAGENTA[1], MAGENTA[2], 18]
          : [CYAN[0], CYAN[1], CYAN[2], 22];
      },
    }),
    new ArcLayer<RegionArcDatum>({
      id: "mesh-region-links",
      data: regionLinks,
      pickable: false,
      widthUnits: "pixels",
      getSourcePosition: (link) => link.sourcePosition,
      getTargetPosition: (link) => link.targetPosition,
      getSourceColor: (link) => [CYAN[0], CYAN[1], CYAN[2], link.selected ? 200 : 110],
      getTargetColor: (link) => [MAGENTA[0], MAGENTA[1], MAGENTA[2], link.selected ? 185 : 100],
      getWidth: (link) => 1.6 + (link.count * 1.1),
      getHeight: (link) => 0.22 + (link.count * 0.06),
      getTilt: () => 12,
    }),
    new ArcLayer<NodeLinkDatum>({
      id: "mesh-observed-links",
      data: observedNodeLinks,
      pickable: false,
      widthUnits: "pixels",
      getSourcePosition: (link) => link.sourcePosition,
      getTargetPosition: (link) => link.targetPosition,
      getSourceColor: (link) => [CYAN_SOFT[0], CYAN_SOFT[1], CYAN_SOFT[2], link.selected ? 215 : 72],
      getTargetColor: (link) => [MAGENTA[0], MAGENTA[1], MAGENTA[2], link.selected ? 200 : 62],
      getWidth: (link) => 0.9 + (link.intensity * 2.4),
      getHeight: (link) => 0.16 + (link.intensity * 0.1),
      getTilt: () => -10,
    }),
    new ArcLayer<NodeLinkDatum>({
      id: "mesh-anchor-links",
      data: anchorNodeLinks,
      pickable: false,
      widthUnits: "pixels",
      getSourcePosition: (link) => link.sourcePosition,
      getTargetPosition: (link) => link.targetPosition,
      getSourceColor: (link) => [CYAN[0], CYAN[1], CYAN[2], link.selected ? 140 : 58],
      getTargetColor: (link) => [CYAN_SOFT[0], CYAN_SOFT[1], CYAN_SOFT[2], link.selected ? 185 : 82],
      getWidth: (link) => 0.8 + (link.intensity * 1.4),
      getHeight: (link) => 0.1 + (link.intensity * 0.06),
      getTilt: () => 0,
    }),
    new ScatterplotLayer<RegionPoint>({
      id: "mesh-region-cores",
      data: regionPoints,
      pickable: false,
      stroked: true,
      filled: true,
      radiusUnits: "pixels",
      lineWidthUnits: "pixels",
      getPosition: (region) => [region.longitude, region.latitude],
      getRadius: (region) => 4.2 + Math.min(region.activityCount, 5),
      getFillColor: (region) => region.selected
        ? [CYAN_SOFT[0], CYAN_SOFT[1], CYAN_SOFT[2], 255]
        : [CYAN[0], CYAN[1], CYAN[2], 235],
      getLineColor: (region) => region.selected
        ? [CYAN_SOFT[0], CYAN_SOFT[1], CYAN_SOFT[2], 180]
        : [CYAN[0], CYAN[1], CYAN[2], 110],
      getLineWidth: (region) => region.selected ? 2.4 : 1.2,
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
      getRadius: (node) => (node.kindLabel === "local" ? 7 : node.selected ? 5.2 : 3.8),
      getFillColor: (node) => {
        if (node.kindLabel === "local") {
          return [CYAN_SOFT[0], CYAN_SOFT[1], CYAN_SOFT[2], 255];
        }
        if (node.stale) {
          return [SLATE[0], SLATE[1], SLATE[2], 195];
        }
        return [MAGENTA[0], MAGENTA[1], MAGENTA[2], 245];
      },
      getLineColor: (node) => {
        if (node.kindLabel === "local") {
          return [CYAN[0], CYAN[1], CYAN[2], 255];
        }
        return node.selected
          ? [CYAN_SOFT[0], CYAN_SOFT[1], CYAN_SOFT[2], 255]
          : [12, 24, 38, 255];
      },
      getLineWidth: (node) => (node.selected ? 2.4 : 1.2),
    }),
    /* GPU-rendered region labels — city name */
    new TextLayer<RegionLabelDatum>({
      id: "mesh-label-city",
      data: regionLabels,
      pickable: false,
      getPosition: (d) => d.position,
      getText: (d) => d.city,
      getColor: (d) => d.selected
        ? [CYAN[0], CYAN[1], CYAN[2], 255]
        : [210, 220, 230, 215],
      getSize: 11,
      getTextAnchor: (d) => labelAnchor(d.side),
      getAlignmentBaseline: "center",
      getPixelOffset: (d) => labelOffset(d.side),
      fontFamily: "Orbitron, sans-serif",
      fontWeight: 700,
      fontSettings: { sdf: true },
      outlineWidth: 4,
      outlineColor: [2, 6, 17, 200],
      sizeUnits: "pixels",
      sizeScale: 1,
      characterSet: "auto",
    }),
    /* GPU-rendered region labels — region code */
    new TextLayer<RegionLabelDatum>({
      id: "mesh-label-code",
      data: regionLabels,
      pickable: false,
      getPosition: (d) => d.position,
      getText: (d) => d.code,
      getColor: [SLATE[0], SLATE[1], SLATE[2], 180],
      getSize: 9,
      getTextAnchor: (d) => labelAnchor(d.side),
      getAlignmentBaseline: "center",
      getPixelOffset: (d) => {
        const base = labelOffset(d.side);
        if (d.side === "north") return [base[0], base[1] - 13];
        if (d.side === "south") return [base[0], base[1] + 13];
        return [base[0], base[1] + 13];
      },
      fontFamily: "'Fira Code', monospace",
      fontWeight: 500,
      fontSettings: { sdf: true },
      outlineWidth: 3,
      outlineColor: [2, 6, 17, 180],
      sizeUnits: "pixels",
      sizeScale: 1,
      characterSet: "auto",
    }),
  ], [anchorNodeLinks, nodePoints, observedNodeLinks, regionLabels, regionLinks, regionPoints]);

  /* ── Ref-based animated layers (no React re-renders) ─────────── */
  const staticLayersRef = useRef(staticLayers);
  useEffect(() => { staticLayersRef.current = staticLayers; }, [staticLayers]);

  const regionPointsRef = useRef(regionPoints);
  const nodePointsRef = useRef(nodePoints);
  useEffect(() => { regionPointsRef.current = regionPoints; }, [regionPoints]);
  useEffect(() => { nodePointsRef.current = nodePoints; }, [nodePoints]);

  const stableHoverRef = useRef(stableHover);
  const stableClickRef = useRef(stableClick);
  useEffect(() => { stableHoverRef.current = stableHover; }, [stableHover]);
  useEffect(() => { stableClickRef.current = stableClick; }, [stableClick]);

  useEffect(() => {
    let frameId = 0;
    let previousCommit = 0;

    const step = (timestamp: number) => {
      if (timestamp - previousCommit >= 48) {
        previousCommit = timestamp;
        const wave = (Math.sin(timestamp / 420) + 1) / 2;
        const rp = regionPointsRef.current;
        const np = nodePointsRef.current;

        const animatedLayers = [
          new ScatterplotLayer<RegionPoint>({
            id: "mesh-region-halos",
            data: rp,
            pickable: true,
            stroked: true,
            filled: true,
            radiusUnits: "pixels",
            lineWidthUnits: "pixels",
            getPosition: (region) => [region.longitude, region.latitude],
            getRadius: (region) => 14 + (region.activityCount * 3.2) + (wave * (region.activityCount > 0 ? 14 : 6)),
            getFillColor: (region) => [CYAN[0], CYAN[1], CYAN[2], region.highlighted ? 38 : 20],
            getLineColor: (region) => region.selected
              ? [CYAN_SOFT[0], CYAN_SOFT[1], CYAN_SOFT[2], 220]
              : [CYAN[0], CYAN[1], CYAN[2], 130 + Math.round(wave * 40)],
            getLineWidth: (region) => region.selected ? 3 : 1.4 + (wave * 0.8),
            onHover: stableHoverRef.current,
            onClick: stableClickRef.current,
          }),
          new ScatterplotLayer<NodePoint>({
            id: "mesh-node-pulses",
            data: np,
            pickable: true,
            stroked: false,
            filled: true,
            radiusUnits: "pixels",
            getPosition: (node) => [node.longitude, node.latitude],
            getRadius: (node) => {
              if (node.kindLabel === "local") {
                return 14 + (wave * 16);
              }
              const freshness = node.stale ? 0.45 : 1;
              return 6 + (freshness * 10) + (wave * (7 + Math.min(node.signalCount, 5)));
            },
            getFillColor: (node) => {
              if (node.kindLabel === "local") {
                return [CYAN[0], CYAN[1], CYAN[2], node.selected ? 68 : 38];
              }
              if (node.stale) {
                return [SLATE[0], SLATE[1], SLATE[2], 24];
              }
              return [MAGENTA[0], MAGENTA[1], MAGENTA[2], node.selected ? 74 : 38];
            },
            onHover: stableHoverRef.current,
            onClick: stableClickRef.current,
          }),
        ];

        const overlay = overlayRef.current;
        if (overlay) {
          overlay.setProps({ layers: [...staticLayersRef.current, ...animatedLayers] });
        }
      }
      frameId = window.requestAnimationFrame(step);
    };

    frameId = window.requestAnimationFrame(step);
    return () => window.cancelAnimationFrame(frameId);
  }, []);

  /* ── Initial layers for first paint ──────────────────────────── */
  const initialLayers = useMemo(() => staticLayers, [staticLayers]);

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
              <DeckGLOverlay layers={initialLayers} overlayRef={overlayRef} />
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
                <span>{localNodes.length > 0 ? `${localNodes.length} local agents` : "observer"}</span>
              </ShellPill>
              <ShellPill className="mesh-stat-pill">
                <Wallet size={14} />
                <span>{localNodes.length > 1 ? `${localNodes.length} wallets` : shortWallet(localNode?.wallet || null)}</span>
              </ShellPill>
            </div>
          </div>

          {showLocationPrompt ? (
            <div className="mesh-stage__location-notice">
              <ShellNotice tone="info">
                <MapPin size={14} />
                <div className="mesh-stage__location-copy">
                  <strong>Enable Location for exact local-agent placement</strong>
                  <span>
                    Turn on macOS Location Services so Compose Mesh can place this device&apos;s local agents at their real position instead of waiting on temporary mesh fallback routing.
                  </span>
                </div>
                <ShellButton
                  tone="primary"
                  size="sm"
                  disabled={requestingLocation}
                  onClick={handleEnableLocation}
                >
                  {requestingLocation ? "Opening…" : "Open System Settings"}
                </ShellButton>
              </ShellNotice>
            </div>
          ) : null}

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
                <span>{localNodes.length > 0 ? "Locating agents..." : "Waiting for Mesh activation"}</span>
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

          {localNodes.length === 0 ? (
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
