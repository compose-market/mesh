import type { InstalledAgent, MeshAgentCard, MeshManifest, MeshPeerSignal } from "../../lib/types";
import {
  derivePeerAnchor,
  hasPublicDirectMeshPath,
  projectMeshCoordinate,
  resolveMeshRegionProfile,
  type MeshAnchorNode,
  type MeshBootstrapAnchor,
  type MeshScene,
  type MeshScenePeerNode,
} from "./model";

export interface MeshStageRegion {
  id: string;
  code: string | null;
  city: string;
  country: string | null;
  x: number;
  y: number;
  lon: number | null;
  lat: number | null;
  peerCount: number;
  peerNodeIds: string[];
  localNodeIds: string[];
}

export interface MeshStageNode {
  id: string;
  kind: "local" | "peer";
  x: number;
  y: number;
  lon: number | null;
  lat: number | null;
  regionId: string | null;
  peerId: string | null;
  wallet: string | null;
  title: string;
  subtitle: string;
  statusLine: string;
  capabilities: string[];
  lastSeenAt: number | null;
  stale: boolean;
  signalCount: number;
  announceCount: number;
  nodeDistance: number;
}

export interface MeshStageNodeLink {
  id: string;
  fromNodeId: string;
  toNodeId: string;
  kind: "anchor" | "observed";
  selected: boolean;
  intensity: number;
}

export interface MeshStageRegionLink {
  id: string;
  fromRegionId: string;
  toRegionId: string;
  count: number;
  selected: boolean;
}

export interface MeshSelectedManifest {
  nodeId: string;
  kind: "local" | "peer";
  title: string;
  subtitle: string;
  description: string;
  tags: string[];
  rows: Array<{ label: string; value: string }>;
}

export interface MeshStageModel {
  regions: MeshStageRegion[];
  nodes: MeshStageNode[];
  nodeLinks: MeshStageNodeLink[];
  regionLinks: MeshStageRegionLink[];
  selectedManifest: MeshSelectedManifest | null;
}

export interface MeshLocalDeviceLocation {
  lat: number;
  lon: number;
  city: string | null;
  country: string | null;
  label: string;
}

interface BuildMeshStageModelInput {
  agents: InstalledAgent[];
  peers: MeshPeerSignal[];
  scene: MeshScene;
  selectedNodeId: string | null;
  selectedRegionId: string | null;
  localDeviceLocation?: MeshLocalDeviceLocation | null;
  runtimeRelayPeerId?: string | null;
}

const LOCAL_DEVICE_REGION_ID = "__local_device__";

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function offsetLatLonMeters(
  origin: MeshLocalDeviceLocation,
  eastMeters: number,
  northMeters: number,
): { lon: number; lat: number } {
  const metersPerDegreeLat = 111_320;
  const metersPerDegreeLon = metersPerDegreeLat * Math.max(0.1, Math.cos((origin.lat * Math.PI) / 180));
  return {
    lon: origin.lon + (eastMeters / metersPerDegreeLon),
    lat: origin.lat + (northMeters / metersPerDegreeLat),
  };
}

function offsetCoordinates(anchor: MeshAnchorNode | null, x: number, y: number): { lon: number | null; lat: number | null } {
  if (anchor?.lon === null || anchor?.lon === undefined || anchor?.lat === null || anchor?.lat === undefined) {
    return {
      lon: anchor?.lon ?? null,
      lat: anchor?.lat ?? null,
    };
  }

  const lonDelta = (x - anchor.x) * 1.15;
  const latDelta = (anchor.y - y) * 0.95;
  return {
    lon: anchor.lon + lonDelta,
    lat: anchor.lat + latDelta,
  };
}

function anchorRecord(anchor: MeshAnchorNode, peerId: string): MeshBootstrapAnchor {
  return {
    peerId,
    host: anchor.host,
    region: anchor.region,
    provider: anchor.provider,
  };
}

function buildAnchorsByPeerId(scene: MeshScene): Record<string, MeshBootstrapAnchor> {
  const result: Record<string, MeshBootstrapAnchor> = {};
  for (const anchor of scene.anchors) {
    for (const peerId of anchor.peerIds) {
      result[peerId] = anchorRecord(anchor, peerId);
    }
  }
  return result;
}

function formatRegion(anchor: MeshAnchorNode): string {
  if (anchor.city) {
    return anchor.country ? `${anchor.city}, ${anchor.country}` : anchor.city;
  }
  if (anchor.region) {
    return anchor.region.toUpperCase();
  }
  return "Unassigned region";
}

function toManifest(input: {
  nodeId: string;
  kind: "local" | "peer";
  title: string;
  description: string;
  card: MeshAgentCard | null;
  manifest?: MeshManifest | null;
  wallet: string | null;
  peerId: string | null;
  regionLabel: string;
  updatedAt: number | null;
}): MeshSelectedManifest {
  const card = input.card;
  const description = card?.description || input.description;
  const rows = [
    { label: "Region", value: input.regionLabel },
    { label: "Model", value: card?.model || "Unknown" },
    { label: "Framework", value: card?.framework || "Unknown" },
  ];

  if (input.wallet) {
    rows.push({ label: "Wallet", value: input.wallet });
  }
  if (input.peerId) {
    rows.push({ label: "Peer ID", value: input.peerId });
  }
  if (input.updatedAt) {
    rows.push({ label: "Updated", value: new Date(input.updatedAt).toLocaleString() });
  }
  if (input.manifest) {
    rows.push({
      label: "Reputation",
      value: `${Math.round(Math.max(0, Math.min(1, input.manifest.reputationScore)) * 100)}%`,
    });
    rows.push({
      label: "Conclaves",
      value: `${input.manifest.successfulConclaves}/${input.manifest.totalConclaves}`,
    });
  }

  return {
    nodeId: input.nodeId,
    kind: input.kind,
    title: input.title,
    subtitle: card?.headline || card?.statusLine || input.regionLabel,
    description,
    tags: card?.capabilities || [],
    rows,
  };
}

function findAnchorByMatch(
  scene: MeshScene,
  match: {
    anchorHost: string | null;
    anchorRegion: string | null;
  },
): MeshAnchorNode | null {
  if (match.anchorHost) {
    const byHost = scene.anchors.find((anchor) => anchor.host === match.anchorHost);
    if (byHost) {
      return byHost;
    }
  }
  if (match.anchorRegion) {
    const byRegion = scene.anchors.find((anchor) => anchor.region === match.anchorRegion);
    if (byRegion) {
      return byRegion;
    }
  }
  return null;
}

function findPeerNode(scene: MeshScene, signalId: string): MeshScenePeerNode | null {
  return scene.peers.find((node) => node.peer.id === signalId) || null;
}

function linkIntensity(signalCount: number, announceCount: number, stale: boolean): number {
  const weightedSignals = Math.max(1, signalCount) + (announceCount * 0.35);
  const freshness = stale ? 0.38 : 1;
  return clamp((Math.log2(weightedSignals + 1) / 3.2) * freshness, 0.24, 1);
}

function regionPairId(left: string, right: string): string {
  return left.localeCompare(right) <= 0 ? `${left}__${right}` : `${right}__${left}`;
}

function buildLocalNode(
  agent: InstalledAgent,
  scene: MeshScene,
  anchorsByPeerId: Record<string, MeshBootstrapAnchor>,
  localIndex: number,
  localCount: number,
  localDeviceLocation: MeshLocalDeviceLocation | null,
  runtimeRelayPeerId: string | null,
): {
  node: MeshStageNode;
  manifest: MeshSelectedManifest;
} | null {
  if (!agent.network.enabled) {
    return null;
  }

  const runtimeAnchorMatch = derivePeerAnchor(agent.network.listenMultiaddrs, anchorsByPeerId);
  const manifestAnchorMatch = agent.network.manifest
    ? derivePeerAnchor(agent.network.manifest.listenMultiaddrs, anchorsByPeerId)
    : null;
  const runtimeRelayAnchor = runtimeRelayPeerId ? anchorsByPeerId[runtimeRelayPeerId] || null : null;
  const relayAnchor = (
    runtimeAnchorMatch.relayPeerId
    ? anchorsByPeerId[runtimeAnchorMatch.relayPeerId]
    : null
  ) || (
    agent.network.relayPeerId
      ? anchorsByPeerId[agent.network.relayPeerId]
      : null
  ) || (
    manifestAnchorMatch?.relayPeerId
      ? anchorsByPeerId[manifestAnchorMatch.relayPeerId]
      : null
  ) || runtimeRelayAnchor || (
    runtimeRelayPeerId
      ? anchorsByPeerId[runtimeRelayPeerId]
      : null
  ) || null;

  const localAnchorMatch = {
    relayPeerId: runtimeAnchorMatch.relayPeerId
      || agent.network.relayPeerId
      || manifestAnchorMatch?.relayPeerId
      || runtimeRelayPeerId
      || null,
    anchorHost: runtimeAnchorMatch.anchorHost
      || manifestAnchorMatch?.anchorHost
      || relayAnchor?.host
      || null,
    anchorRegion: runtimeAnchorMatch.anchorRegion
      || manifestAnchorMatch?.anchorRegion
      || relayAnchor?.region
      || null,
    anchorProvider: runtimeAnchorMatch.anchorProvider
      || manifestAnchorMatch?.anchorProvider
      || relayAnchor?.provider
      || null,
  };

  const localAnchor = findAnchorByMatch(scene, localAnchorMatch)
    || (
      relayAnchor
        ? findAnchorByMatch(scene, {
          anchorHost: relayAnchor.host,
          anchorRegion: relayAnchor.region,
        })
        : null
    );
  const hasDirectMeshPath = hasPublicDirectMeshPath([
    ...agent.network.listenMultiaddrs,
    ...(agent.network.manifest?.listenMultiaddrs || []),
  ]);
  const useLocalDeviceLocation = Boolean(localDeviceLocation) && (hasDirectMeshPath || !localAnchor);
  const localRegionProfile = useLocalDeviceLocation
    ? null
    : resolveMeshRegionProfile(localAnchor?.region || localAnchorMatch.anchorRegion || null);
  const localRegionLabel = useLocalDeviceLocation && localDeviceLocation
    ? (
      localDeviceLocation.city
        ? localDeviceLocation.country
          ? `${localDeviceLocation.city}, ${localDeviceLocation.country}`
          : localDeviceLocation.city
        : localDeviceLocation.label
    )
    : localAnchor
      ? formatRegion(localAnchor)
      : localRegionProfile
        ? `${localRegionProfile.city}, ${localRegionProfile.country}`
        : "Unassigned region";

  const angle = localCount > 1 ? ((Math.PI * 2 * localIndex) / localCount) - (Math.PI / 2) : -Math.PI / 4;
  const radius = localCount > 1 ? 3.4 : 2.8;
  const baseProjection = useLocalDeviceLocation && localDeviceLocation
    ? projectMeshCoordinate(localDeviceLocation.lat, localDeviceLocation.lon)
    : null;
  const x = clamp((baseProjection?.x ?? localAnchor?.x ?? 50) + (Math.cos(angle) * radius), 6, 94);
  const y = clamp((baseProjection?.y ?? localAnchor?.y ?? 50) + (Math.sin(angle) * radius * 0.72), 10, 90);
  const geographicPosition = useLocalDeviceLocation && localDeviceLocation
    ? (localCount <= 1
      ? { lon: localDeviceLocation.lon, lat: localDeviceLocation.lat }
      : offsetLatLonMeters(
        localDeviceLocation,
        Math.cos(angle) * 18,
        Math.sin(angle) * 14,
      ))
    : localAnchor
      ? offsetCoordinates(localAnchor, x, y)
      : localRegionProfile
        ? {
          lon: localRegionProfile.lon + (Math.cos(angle) * 1.35),
          lat: localRegionProfile.lat + (Math.sin(angle) * 0.95),
        }
        : { lon: null, lat: null };
  const nodeId = `local:${agent.agentWallet}`;
  const node: MeshStageNode = {
    ...geographicPosition,
    id: nodeId,
    kind: "local",
    x,
    y,
    regionId: useLocalDeviceLocation ? LOCAL_DEVICE_REGION_ID : localAnchor?.id || null,
    peerId: agent.network.peerId,
    wallet: agent.agentWallet,
    title: agent.network.publicCard?.name || agent.metadata.name,
    subtitle: agent.network.publicCard?.headline || localRegionLabel,
    statusLine: agent.network.publicCard?.statusLine || agent.metadata.description,
    capabilities: agent.network.publicCard?.capabilities || [],
    lastSeenAt: agent.network.updatedAt || null,
    stale: agent.network.status !== "online",
    signalCount: 1,
    announceCount: 0,
    nodeDistance: 0,
  };

  return {
    node,
    manifest: toManifest({
      nodeId,
      kind: "local",
      title: node.title,
      description: agent.metadata.description,
      card: agent.network.publicCard,
      manifest: agent.network.manifest,
      wallet: agent.agentWallet,
      peerId: agent.network.peerId,
      regionLabel: localRegionLabel,
      updatedAt: agent.network.publicCard?.updatedAt || agent.network.updatedAt || null,
    }),
  };
}

export function buildMeshStageModel({
  agents,
  peers,
  scene,
  selectedNodeId,
  selectedRegionId,
  localDeviceLocation = null,
  runtimeRelayPeerId = null,
}: BuildMeshStageModelInput): MeshStageModel {
  const anchorsByPeerId = buildAnchorsByPeerId(scene);
  const peerNodes: MeshStageNode[] = peers.flatMap((peer) => {
    const scenePeer = findPeerNode(scene, peer.id);
    if (!scenePeer) {
      return [];
    }
    const anchor = scene.anchors.find((item) => item.id === scenePeer.anchorNodeId) || null;
    return [{
      ...offsetCoordinates(anchor, scenePeer.x, scenePeer.y),
      id: `peer:${peer.id}`,
      kind: "peer" as const,
      x: scenePeer.x,
      y: scenePeer.y,
      regionId: anchor?.id || null,
      peerId: peer.peerId,
      wallet: peer.agentWallet,
      title: peer.card?.name || peer.peerId,
      subtitle: peer.card?.headline || peer.card?.statusLine || (anchor ? formatRegion(anchor) : "Mesh peer"),
      statusLine: peer.card?.statusLine || peer.card?.headline || peer.agentWallet || peer.peerId,
      capabilities: peer.card?.capabilities || peer.caps,
      lastSeenAt: peer.lastSeenAt,
      stale: peer.stale,
      signalCount: peer.signalCount,
      announceCount: peer.announceCount,
      nodeDistance: peer.nodeDistance,
    }];
  });

  const enabledAgents = agents.filter((agent) => agent.network.enabled);
  const localNodes = enabledAgents
    .map((agent, index) => buildLocalNode(
      agent,
      scene,
      anchorsByPeerId,
      index,
      enabledAgents.length,
      localDeviceLocation,
      runtimeRelayPeerId,
    ))
    .filter((entry): entry is { node: MeshStageNode; manifest: MeshSelectedManifest } => entry !== null);
  const localManifestById = new Map(localNodes.map((entry) => [entry.node.id, entry.manifest]));
  const nodes = [...localNodes.map((entry) => entry.node), ...peerNodes];

  const regions = scene.anchors.map((anchor) => {
    const peerNodeIds = peerNodes.filter((node) => node.regionId === anchor.id).map((node) => node.id);
    const localNodeIds = localNodes.filter((entry) => entry.node.regionId === anchor.id).map((entry) => entry.node.id);
    return {
      id: anchor.id,
      code: anchor.region,
      city: anchor.city || anchor.region?.toUpperCase() || "Unknown region",
      country: anchor.country,
      x: anchor.x,
      y: anchor.y,
      lon: anchor.lon,
      lat: anchor.lat,
      peerCount: peerNodeIds.length,
      peerNodeIds,
      localNodeIds,
    };
  });
  const localDeviceNodeIds = localNodes
    .filter((entry) => entry.node.regionId === LOCAL_DEVICE_REGION_ID)
    .map((entry) => entry.node.id);
  if (localDeviceLocation && localDeviceNodeIds.length > 0) {
    const projection = projectMeshCoordinate(localDeviceLocation.lat, localDeviceLocation.lon);
    regions.push({
      id: LOCAL_DEVICE_REGION_ID,
      code: null,
      city: localDeviceLocation.city || localDeviceLocation.label,
      country: localDeviceLocation.country,
      x: projection.x,
      y: projection.y,
      lon: localDeviceLocation.lon,
      lat: localDeviceLocation.lat,
      peerCount: 0,
      peerNodeIds: [],
      localNodeIds: localDeviceNodeIds,
    });
  }

  const selectedNode = nodes.find((node) => node.id === selectedNodeId) || null;
  const selectedPeer = selectedNode?.kind === "peer"
    ? peers.find((peer) => `peer:${peer.id}` === selectedNode.id) || null
    : null;

  const selectedManifest = selectedNode?.kind === "local"
    ? localManifestById.get(selectedNode.id) || null
    : selectedPeer && selectedNode
      ? toManifest({
        nodeId: selectedNode.id,
        kind: "peer",
        title: selectedNode.title,
        description: selectedPeer.card?.description || selectedNode.statusLine,
        card: selectedPeer.card,
        wallet: selectedPeer.agentWallet,
        peerId: selectedPeer.peerId,
        regionLabel: selectedNode.regionId
          ? (regions.find((region) => region.id === selectedNode.regionId)?.city || "Unassigned region")
          : "Unassigned region",
        updatedAt: selectedPeer.card?.updatedAt || selectedPeer.lastSeenAt,
      })
      : null;

  const activeRegionId = selectedRegionId || selectedNode?.regionId || null;
  const nodeLinks: MeshStageNodeLink[] = [];

  for (const peerNode of peerNodes) {
    if (!peerNode.regionId) {
      continue;
    }

    nodeLinks.push({
      id: `${peerNode.regionId}::${peerNode.id}::anchor`,
      fromNodeId: peerNode.regionId,
      toNodeId: peerNode.id,
      kind: "anchor",
      selected: selectedNodeId === peerNode.id || activeRegionId === peerNode.regionId,
      intensity: linkIntensity(peerNode.signalCount, peerNode.announceCount, peerNode.stale),
    });
  }

  for (const localNode of localNodes.map((entry) => entry.node)) {
    if (localNode.regionId) {
      nodeLinks.push({
        id: `${localNode.regionId}::${localNode.id}::anchor`,
        fromNodeId: localNode.regionId,
        toNodeId: localNode.id,
        kind: "anchor",
        selected: selectedNodeId === localNode.id || activeRegionId === localNode.regionId,
        intensity: 1,
      });
    }

    for (const peerNode of peerNodes) {
      nodeLinks.push({
        id: `${localNode.id}::${peerNode.id}::observed`,
        fromNodeId: localNode.id,
        toNodeId: peerNode.id,
        kind: "observed",
        selected: selectedNodeId === localNode.id || selectedNodeId === peerNode.id || activeRegionId === peerNode.regionId,
        intensity: linkIntensity(peerNode.signalCount, peerNode.announceCount, peerNode.stale),
      });
    }
  }

  const regionLinksById = new Map<string, MeshStageRegionLink>();
  for (const localNode of localNodes.map((entry) => entry.node)) {
    if (!localNode.regionId) {
      continue;
    }

    for (const peerNode of peerNodes) {
      if (!peerNode.regionId || peerNode.regionId === localNode.regionId) {
        continue;
      }

      const id = regionPairId(localNode.regionId, peerNode.regionId);
      const existing = regionLinksById.get(id);
      if (existing) {
        existing.count += 1;
        existing.selected = existing.selected || selectedNodeId === peerNode.id || activeRegionId === peerNode.regionId;
        continue;
      }

      regionLinksById.set(id, {
        id,
        fromRegionId: localNode.regionId,
        toRegionId: peerNode.regionId,
        count: 1,
        selected: selectedNodeId === localNode.id || selectedNodeId === peerNode.id || activeRegionId === peerNode.regionId,
      });
    }
  }

  return {
    regions,
    nodes,
    nodeLinks,
    regionLinks: [...regionLinksById.values()].sort((left, right) => (
      left.fromRegionId.localeCompare(right.fromRegionId)
      || left.toRegionId.localeCompare(right.toRegionId)
    )),
    selectedManifest,
  };
}
