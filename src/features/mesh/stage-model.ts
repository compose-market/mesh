import type { InstalledAgent, MeshAgentCard, MeshPeerSignal } from "../../lib/types";
import {
  derivePeerAnchor,
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
  localNodeId: string | null;
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

interface BuildMeshStageModelInput {
  agent: InstalledAgent | null;
  peers: MeshPeerSignal[];
  scene: MeshScene;
  selectedNodeId: string | null;
  selectedRegionId: string | null;
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
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

function findPeerNode(scene: MeshScene, peerId: string): MeshScenePeerNode | null {
  return scene.peers.find((node) => node.peer.peerId === peerId) || null;
}

function linkIntensity(signalCount: number, announceCount: number, stale: boolean): number {
  const weightedSignals = Math.max(1, signalCount) + (announceCount * 0.35);
  const freshness = stale ? 0.38 : 1;
  return clamp((Math.log2(weightedSignals + 1) / 3.2) * freshness, 0.24, 1);
}

function regionPairId(left: string, right: string): string {
  return left.localeCompare(right) <= 0 ? `${left}__${right}` : `${right}__${left}`;
}

export function buildMeshStageModel({
  agent,
  peers,
  scene,
  selectedNodeId,
  selectedRegionId,
}: BuildMeshStageModelInput): MeshStageModel {
  const anchorsByPeerId = buildAnchorsByPeerId(scene);
  const peerNodes: MeshStageNode[] = peers.flatMap((peer) => {
      const scenePeer = findPeerNode(scene, peer.peerId);
      if (!scenePeer) {
        return [];
      }
      const anchor = scene.anchors.find((item) => item.id === scenePeer.anchorNodeId) || null;
      return [{
        ...offsetCoordinates(anchor, scenePeer.x, scenePeer.y),
        id: `peer:${peer.peerId}`,
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

  let localNode: MeshStageNode | null = null;
  let localManifest: MeshSelectedManifest | null = null;

  if (agent && agent.network.enabled) {
    const localAnchorMatch = derivePeerAnchor(agent.network.listenMultiaddrs, anchorsByPeerId);
    const localAnchor = findAnchorByMatch(scene, localAnchorMatch);
    const localRegionProfile = resolveMeshRegionProfile(localAnchor?.region || null);
    const localRegionLabel = localAnchor
      ? formatRegion(localAnchor)
      : localRegionProfile
        ? `${localRegionProfile.city}, ${localRegionProfile.country}`
        : "Unassigned region";

    localNode = {
      ...offsetCoordinates(localAnchor, clamp((localAnchor?.x ?? 50) + 3.8, 6, 94), clamp((localAnchor?.y ?? 50) - 3.2, 10, 90)),
      id: "local-agent",
      kind: "local",
      x: clamp((localAnchor?.x ?? 50) + 3.8, 6, 94),
      y: clamp((localAnchor?.y ?? 50) - 3.2, 10, 90),
      regionId: localAnchor?.id || null,
      peerId: agent.network.peerId,
      wallet: agent.agentWallet,
      title: agent.network.publicCard?.name || agent.metadata.name,
      subtitle: agent.network.publicCard?.headline || localRegionLabel,
      statusLine: agent.network.publicCard?.statusLine || agent.metadata.description,
      capabilities: agent.network.publicCard?.capabilities || [],
      lastSeenAt: agent.network.updatedAt || null,
      stale: agent.network.status !== "online",
      signalCount: Math.max(1, peers.length),
      announceCount: 0,
      nodeDistance: 0,
    };

    localManifest = toManifest({
      nodeId: localNode.id,
      kind: "local",
      title: localNode.title,
      description: agent.metadata.description,
      card: agent.network.publicCard,
      wallet: agent.agentWallet,
      peerId: agent.network.peerId,
      regionLabel: localRegionLabel,
      updatedAt: agent.network.publicCard?.updatedAt || agent.network.updatedAt || null,
    });
  }

  const nodes = localNode ? [localNode, ...peerNodes] : peerNodes;
  const regions = scene.anchors.map((anchor) => {
    const regionPeerNodeIds = peerNodes.filter((node) => node.regionId === anchor.id).map((node) => node.id);
    return {
      id: anchor.id,
      code: anchor.region,
      city: anchor.city || anchor.region?.toUpperCase() || "Unknown region",
      country: anchor.country,
      x: anchor.x,
      y: anchor.y,
      lon: anchor.lon,
      lat: anchor.lat,
      peerCount: regionPeerNodeIds.length,
      peerNodeIds: regionPeerNodeIds,
      localNodeId: localNode?.regionId === anchor.id ? localNode.id : null,
    };
  });

  const selectedNode = nodes.find((node) => node.id === selectedNodeId) || null;
  const selectedPeer = selectedNode?.kind === "peer"
    ? peers.find((peer) => `peer:${peer.peerId}` === selectedNode.id) || null
    : null;

  const selectedManifest = selectedNode?.kind === "local"
    ? localManifest
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

  if (localNode?.regionId) {
    nodeLinks.push({
      id: `${localNode.regionId}::${localNode.id}::anchor`,
      fromNodeId: localNode.regionId,
      toNodeId: localNode.id,
      kind: "anchor",
      selected: selectedNodeId === localNode.id || activeRegionId === localNode.regionId,
      intensity: 1,
    });
  }

  for (const node of peerNodes) {
    if (node.regionId) {
      nodeLinks.push({
        id: `${node.regionId}::${node.id}::anchor`,
        fromNodeId: node.regionId,
        toNodeId: node.id,
        kind: "anchor",
        selected: selectedNodeId === node.id || activeRegionId === node.regionId,
        intensity: linkIntensity(node.signalCount, node.announceCount, node.stale),
      });
    }

    if (localNode) {
      nodeLinks.push({
        id: `${localNode.id}::${node.id}::observed`,
        fromNodeId: localNode.id,
        toNodeId: node.id,
        kind: "observed",
        selected: selectedNodeId === localNode.id || selectedNodeId === node.id || activeRegionId === node.regionId,
        intensity: linkIntensity(node.signalCount, node.announceCount, node.stale),
      });
    }
  }

  const regionLinksById = new Map<string, MeshStageRegionLink>();
  if (localNode?.regionId) {
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
