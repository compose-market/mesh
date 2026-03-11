import type { MeshPeerSignal } from "../../lib/types";

const DEFAULT_GOSSIP_TOPIC = "compose/global/v1";
const DEFAULT_ANNOUNCE_TOPIC = "compose/announce/v1";
const DEFAULT_KAD_PROTOCOL = "/compose-market/desktop/kad/1.0.0";
const DEFAULT_HEARTBEAT_MS = 30_000;
const DEFAULT_BOOTSTRAP_DNS_ROOTS = ["_dnsaddr.compose.market"];
const DEFAULT_FALLBACK_MULTIADDRS = [
  "/ip4/206.189.203.231/tcp/4001/p2p/12D3KooWPRcHjairRTQuXQdtUux5326pbuWyxsBrrDEzgLdbRKyh",
  "/ip4/206.189.203.231/tcp/4002/ws/p2p/12D3KooWPRcHjairRTQuXQdtUux5326pbuWyxsBrrDEzgLdbRKyh",
  "/ip4/134.122.34.135/tcp/4001/p2p/12D3KooW9qchwdUL4iZ8KyTT1CjN37pc49eFRFAkHTu8TYU1yVCz",
  "/ip4/134.122.34.135/tcp/4002/ws/p2p/12D3KooW9qchwdUL4iZ8KyTT1CjN37pc49eFRFAkHTu8TYU1yVCz",
  "/ip4/64.225.35.57/tcp/4001/p2p/12D3KooWDdWJP82TKNbMemW5JtXR4qGrhE2tc455T9yZewEZ4rdD",
  "/ip4/64.225.35.57/tcp/4002/ws/p2p/12D3KooWDdWJP82TKNbMemW5JtXR4qGrhE2tc455T9yZewEZ4rdD",
  "/ip4/188.166.59.149/tcp/4001/p2p/12D3KooWQzwPXPUEMPU1Upbo6trSiEo82rhtRpTJGr7SzP2gD7jb",
  "/ip4/188.166.59.149/tcp/4002/ws/p2p/12D3KooWQzwPXPUEMPU1Upbo6trSiEo82rhtRpTJGr7SzP2gD7jb",
  "/ip4/164.90.230.221/tcp/4001/p2p/12D3KooWGoiuj2h5jqFK75tN14EnqSvXhAxT7V8JrfddwxgQZUka",
  "/ip4/164.90.230.221/tcp/4002/ws/p2p/12D3KooWGoiuj2h5jqFK75tN14EnqSvXhAxT7V8JrfddwxgQZUka",
  "/ip4/161.35.33.12/tcp/4001/p2p/12D3KooWG22npb9WPpoehLSfo7xeEeEGDk7oH2Vw7dpy6pn77Cxr",
  "/ip4/161.35.33.12/tcp/4002/ws/p2p/12D3KooWG22npb9WPpoehLSfo7xeEeEGDk7oH2Vw7dpy6pn77Cxr",
  "/ip4/206.189.84.32/tcp/4001/p2p/12D3KooWSLexJ4Ni84zYepiNArUDZuunGiwoUxZ5xhHoGABHNDUx",
  "/ip4/206.189.84.32/tcp/4002/ws/p2p/12D3KooWSLexJ4Ni84zYepiNArUDZuunGiwoUxZ5xhHoGABHNDUx",
  "/ip4/139.59.2.252/tcp/4001/p2p/12D3KooWLvw8Qdp5Bc5ryPv2ZYkJn1CsmLoaxVEhzsH8x9cunnoW",
  "/ip4/139.59.2.252/tcp/4002/ws/p2p/12D3KooWLvw8Qdp5Bc5ryPv2ZYkJn1CsmLoaxVEhzsH8x9cunnoW",
  "/ip4/134.199.145.253/tcp/4001/p2p/12D3KooWNTpWNjwgc4EBGor1d4BgrGmmuUxVaeEGdNmFMCnws6dG",
  "/ip4/134.199.145.253/tcp/4002/ws/p2p/12D3KooWNTpWNjwgc4EBGor1d4BgrGmmuUxVaeEGdNmFMCnws6dG",
];

export interface MeshBootstrapResolution {
  bootstrapDnsRoots: string[];
  fallbackMultiaddrs: string[];
  bootstrapMultiaddrs: string[];
  relayMultiaddrs: string[];
  topics: string[];
  gossipTopic: string;
  announceTopic: string;
  kadProtocol: string;
  heartbeatMs: number;
  source: "dns" | "local";
}

export interface MeshBootstrapAnchor {
  peerId: string;
  host: string | null;
  region: string | null;
  provider: string | null;
}

export interface MeshBootstrapRegion {
  id: string;
  host: string | null;
  region: string | null;
  provider: string | null;
  peerIds: string[];
}

export interface MeshAnchorNode extends MeshBootstrapRegion {
  x: number;
  y: number;
  depth: number;
}

export interface MeshScenePeerNode {
  peer: MeshPeerSignal;
  x: number;
  y: number;
  depth: number;
  anchorNodeId: string | null;
  anchorHost: string | null;
}

export interface MeshScene {
  anchors: MeshAnchorNode[];
  peers: MeshScenePeerNode[];
}

function unique(values: string[]): string[] {
  return Array.from(new Set(values));
}

function parseCsv(value: string | undefined): string[] {
  return value
    ? value.split(",").map((item) => item.trim()).filter(Boolean)
    : [];
}

function parsePositiveInt(value: string | undefined, fallback: number, min: number, max: number): number {
  if (!value) {
    return fallback;
  }
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? Math.max(min, Math.min(max, parsed)) : fallback;
}

function parseProvider(code: string | null): string | null {
  switch (code) {
    case "do":
      return "digitalocean";
    case "az":
      return "azure";
    case "gcp":
      return "gcp";
    default:
      return code;
  }
}

function stripTxtQuotes(value: string): string {
  const trimmed = value.trim();
  if (trimmed.startsWith("\"") && trimmed.endsWith("\"") && trimmed.length >= 2) {
    return trimmed.slice(1, -1).replace(/\\"/g, "\"");
  }
  return trimmed;
}

function parseDnsAddrTxt(data: string): string[] {
  const normalized = stripTxtQuotes(data);
  if (!normalized.toLowerCase().startsWith("dnsaddr=")) {
    return [];
  }
  const addr = normalized.slice("dnsaddr=".length).trim();
  return addr.startsWith("/") ? [addr] : [];
}

function parseMultiaddrParts(input: string): { host: string | null; peerIds: string[]; hasCircuit: boolean } {
  const parts = input.split("/").filter(Boolean);
  let host: string | null = null;
  const peerIds: string[] = [];
  let hasCircuit = false;

  for (let index = 0; index < parts.length; index += 1) {
    const protocol = parts[index];
    const value = parts[index + 1];
    if (!value) {
      continue;
    }
    if (protocol === "dns4" || protocol === "dns6") {
      host = value;
      index += 1;
      continue;
    }
    if (protocol === "p2p") {
      peerIds.push(value);
      index += 1;
      continue;
    }
    if (protocol === "p2p-circuit") {
      hasCircuit = true;
    }
  }

  return { host, peerIds, hasCircuit };
}

function anchorFromHost(host: string | null): Pick<MeshBootstrapAnchor, "host" | "region" | "provider"> {
  if (!host) {
    return { host: null, region: null, provider: null };
  }
  const parts = host.toLowerCase().split(".").filter(Boolean);
  if (parts.length < 4) {
    return { host, region: null, provider: null };
  }
  return {
    host,
    provider: parseProvider(parts[1] || null),
    region: parts[2] || null,
  };
}

function compareNullable(left: string | null, right: string | null): number {
  return (left || "").localeCompare(right || "");
}

function anchorGroupKey(anchor: MeshBootstrapAnchor): string {
  return anchor.host || anchor.peerId;
}

function orbitPosition(index: number, total: number, radius: number): { x: number; y: number; depth: number } {
  const angle = (-Math.PI / 2) + ((Math.PI * 2 * index) / Math.max(1, total));
  return {
    x: 50 + (Math.cos(angle) * radius),
    y: 50 + (Math.sin(angle) * radius * 0.56) + ((index % 2 === 0 ? -1 : 1) * 2.4),
    depth: 0.4 + ((Math.sin(angle) + 1) / 2),
  };
}

async function queryDnsAddrRecords(root: string): Promise<string[]> {
  const response = await fetch(`https://cloudflare-dns.com/dns-query?name=${encodeURIComponent(root)}&type=TXT`, {
    headers: {
      Accept: "application/dns-json",
    },
  });

  if (!response.ok) {
    throw new Error(`DNS query failed (${response.status}) for ${root}`);
  }

  const body = await response.json() as { Answer?: Array<{ data?: string }> };
  return unique((body.Answer || []).flatMap((answer) => parseDnsAddrTxt(answer.data || "")));
}

export function resolveLocalMeshBootstrap(): MeshBootstrapResolution {
  const env = import.meta.env as Record<string, string | undefined>;
  const bootstrapDnsRoots = parseCsv(env.VITE_LIBP2P_BOOTSTRAP_DNS_ROOTS);
  const fallbackMultiaddrs = unique([
    ...DEFAULT_FALLBACK_MULTIADDRS,
    ...parseCsv(env.VITE_LIBP2P_BOOTSTRAP_MULTIADDRS),
    ...parseCsv(env.VITE_LIBP2P_RELAY_MULTIADDRS),
  ]);
  const gossipTopic = env.VITE_LIBP2P_GOSSIP_TOPIC?.trim() || DEFAULT_GOSSIP_TOPIC;
  const announceTopic = env.VITE_LIBP2P_ANNOUNCE_TOPIC?.trim() || DEFAULT_ANNOUNCE_TOPIC;
  const kadProtocol = env.VITE_LIBP2P_KAD_PROTOCOL?.trim() || DEFAULT_KAD_PROTOCOL;
  const heartbeatMs = parsePositiveInt(env.VITE_LIBP2P_HEARTBEAT_MS, DEFAULT_HEARTBEAT_MS, 1_000, 300_000);

  return {
    bootstrapDnsRoots: bootstrapDnsRoots.length > 0 ? bootstrapDnsRoots : [...DEFAULT_BOOTSTRAP_DNS_ROOTS],
    fallbackMultiaddrs,
    bootstrapMultiaddrs: fallbackMultiaddrs,
    relayMultiaddrs: fallbackMultiaddrs,
    topics: [gossipTopic, announceTopic],
    gossipTopic,
    announceTopic,
    kadProtocol,
    heartbeatMs,
    source: "local",
  };
}

export async function resolveMeshBootstrap(): Promise<MeshBootstrapResolution> {
  const local = resolveLocalMeshBootstrap();
  const discovered: string[] = [];

  for (const root of local.bootstrapDnsRoots) {
    try {
      discovered.push(...await queryDnsAddrRecords(root));
    } catch {
      // Fallback multiaddrs preserve mesh bootstrap when DNS discovery fails.
    }
  }

  const merged = unique([...discovered, ...local.fallbackMultiaddrs]);
  return merged.length === 0
    ? local
    : {
      ...local,
      bootstrapMultiaddrs: merged,
      relayMultiaddrs: merged,
      source: discovered.length > 0 ? "dns" : "local",
    };
}

export function deriveBootstrapAnchors(
  resolution: Pick<MeshBootstrapResolution, "bootstrapMultiaddrs" | "relayMultiaddrs">,
): Record<string, MeshBootstrapAnchor> {
  const anchorsByPeerId: Record<string, MeshBootstrapAnchor> = {};

  for (const multiaddr of unique([...resolution.bootstrapMultiaddrs, ...resolution.relayMultiaddrs])) {
    const parsed = parseMultiaddrParts(multiaddr);
    const peerId = parsed.peerIds[parsed.peerIds.length - 1];
    if (!peerId) {
      continue;
    }
    anchorsByPeerId[peerId] = {
      peerId,
      ...anchorFromHost(parsed.host),
    };
  }

  return anchorsByPeerId;
}

export function derivePeerAnchor(
  listenMultiaddrs: string[],
  anchorsByPeerId: Record<string, MeshBootstrapAnchor>,
): {
  relayPeerId: string | null;
  anchorHost: string | null;
  anchorRegion: string | null;
  anchorProvider: string | null;
} {
  for (const multiaddr of listenMultiaddrs) {
    const parsed = parseMultiaddrParts(multiaddr);
    if (parsed.host) {
      const anchor = anchorFromHost(parsed.host);
      return {
        relayPeerId: parsed.hasCircuit ? parsed.peerIds[0] || null : null,
        anchorHost: anchor.host,
        anchorRegion: anchor.region,
        anchorProvider: anchor.provider,
      };
    }

    const mappedPeerId = (
      parsed.hasCircuit ? parsed.peerIds[0] : null
    ) || parsed.peerIds.find((peerId) => Boolean(anchorsByPeerId[peerId])) || null;
    if (mappedPeerId && anchorsByPeerId[mappedPeerId]) {
      const anchor = anchorsByPeerId[mappedPeerId];
      return {
        relayPeerId: mappedPeerId,
        anchorHost: anchor.host,
        anchorRegion: anchor.region,
        anchorProvider: anchor.provider,
      };
    }
  }

  return {
    relayPeerId: null,
    anchorHost: null,
    anchorRegion: null,
    anchorProvider: null,
  };
}

export function buildBootstrapRegions(
  resolution: Pick<MeshBootstrapResolution, "bootstrapMultiaddrs" | "relayMultiaddrs">,
): MeshBootstrapRegion[] {
  const grouped = new Map<string, MeshBootstrapRegion>();

  for (const anchor of Object.values(deriveBootstrapAnchors(resolution))) {
    const key = anchorGroupKey(anchor);
    const current = grouped.get(key);
    if (current) {
      current.peerIds.push(anchor.peerId);
      continue;
    }
    grouped.set(key, {
      id: key,
      host: anchor.host,
      region: anchor.region,
      provider: anchor.provider,
      peerIds: [anchor.peerId],
    });
  }

  return [...grouped.values()]
    .map((region) => ({ ...region, peerIds: unique(region.peerIds).sort() }))
    .sort((left, right) => (
      compareNullable(left.region, right.region)
      || compareNullable(left.provider, right.provider)
      || compareNullable(left.host, right.host)
      || left.id.localeCompare(right.id)
    ));
}

export function buildMeshScene(input: { peers: MeshPeerSignal[]; resolution: MeshBootstrapResolution }): MeshScene {
  const anchors = buildBootstrapRegions(input.resolution).map((region, index, all) => ({
    ...region,
    ...orbitPosition(index, all.length, 35 + ((index % 3) * 3)),
  }));
  const anchorsByPeerId = deriveBootstrapAnchors(input.resolution);
  const anchorNodeById = new Map(anchors.map((anchor) => [anchor.id, anchor]));
  const anchorNodeByHost = new Map(
    anchors.filter((anchor) => anchor.host).map((anchor) => [anchor.host as string, anchor]),
  );
  const peerGroups = new Map<string, MeshPeerSignal[]>();

  for (const peer of input.peers) {
    const fallbackAnchor = derivePeerAnchor(peer.listenMultiaddrs, anchorsByPeerId);
    const anchorHost = peer.anchorHost || fallbackAnchor.anchorHost;
    const anchorId = anchorHost
      ? anchorNodeByHost.get(anchorHost)?.id || anchorHost
      : peer.relayPeerId && anchorsByPeerId[peer.relayPeerId]
        ? anchorGroupKey(anchorsByPeerId[peer.relayPeerId])
        : `peer:${peer.peerId}`;
    const current = peerGroups.get(anchorId) || [];
    current.push({
      ...peer,
      anchorHost,
      relayPeerId: peer.relayPeerId || fallbackAnchor.relayPeerId,
      anchorRegion: peer.anchorRegion || fallbackAnchor.anchorRegion,
      anchorProvider: peer.anchorProvider || fallbackAnchor.anchorProvider,
    });
    peerGroups.set(anchorId, current);
  }

  const peers: MeshScenePeerNode[] = [];
  for (const [anchorId, groupedPeers] of peerGroups.entries()) {
    const anchorNode = anchorNodeById.get(anchorId) || null;
    const baseX = anchorNode?.x ?? 50;
    const baseY = anchorNode?.y ?? 50;
    const spread = (Math.PI * 2) / Math.max(6, groupedPeers.length * 3);
    const sortedPeers = [...groupedPeers].sort((left, right) => (
      left.nodeDistance - right.nodeDistance || right.lastSeenAt - left.lastSeenAt
    ));

    sortedPeers.forEach((peer, index) => {
      const ring = Math.max(1, Math.min(4, peer.nodeDistance || 1));
      const radius = 5 + (ring * 3.2) + Math.floor(index / 3) * 2.4;
      const angle = (-Math.PI / 2) + (index * spread);
      peers.push({
        peer,
        x: baseX + (Math.cos(angle) * radius),
        y: baseY + (Math.sin(angle) * radius * 0.58),
        depth: (anchorNode?.depth ?? 0.5) + 0.08,
        anchorNodeId: anchorNode?.id || null,
        anchorHost: peer.anchorHost || anchorNode?.host || null,
      });
    });
  }

  return { anchors, peers };
}
