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

function parseCsv(value: string | undefined): string[] {
  if (!value) return [];
  return value
    .split(",")
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);
}

function parsePositiveInt(value: string | undefined, fallback: number, min: number, max: number): number {
  if (!value) return fallback;
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  return Math.max(min, Math.min(max, parsed));
}

function unique(values: string[]): string[] {
  return Array.from(new Set(values));
}

function stripTxtQuotes(value: string): string {
  let trimmed = value.trim();
  if (trimmed.startsWith('"') && trimmed.endsWith('"') && trimmed.length >= 2) {
    trimmed = trimmed.slice(1, -1);
  }
  return trimmed.replace(/\\"/g, '"');
}

function parseDnsAddrTxt(data: string): string[] {
  const normalized = stripTxtQuotes(data);
  if (!normalized.toLowerCase().startsWith("dnsaddr=")) {
    return [];
  }
  const addr = normalized.slice("dnsaddr=".length).trim();
  if (!addr.startsWith("/")) {
    return [];
  }
  return [addr];
}

async function queryDnsAddrRecords(root: string): Promise<string[]> {
  const url = `https://cloudflare-dns.com/dns-query?name=${encodeURIComponent(root)}&type=TXT`;
  const response = await fetch(url, {
    method: "GET",
    headers: {
      Accept: "application/dns-json",
    },
  });

  if (!response.ok) {
    throw new Error(`DNS query failed (${response.status}) for ${root}`);
  }

  const body = (await response.json()) as { Answer?: Array<{ data?: string }> };
  const answers = Array.isArray(body.Answer) ? body.Answer : [];
  const addresses = answers.flatMap((answer) => parseDnsAddrTxt(answer.data || ""));
  return unique(addresses);
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
      const addrs = await queryDnsAddrRecords(root);
      discovered.push(...addrs);
    } catch {
      // ignore and continue; local fallback handles bootstrap continuity
    }
  }

  const merged = unique([...discovered, ...local.fallbackMultiaddrs]);
  if (merged.length === 0) {
    return local;
  }

  return {
    ...local,
    bootstrapMultiaddrs: merged,
    relayMultiaddrs: merged,
    source: discovered.length > 0 ? "dns" : "local",
  };
}
