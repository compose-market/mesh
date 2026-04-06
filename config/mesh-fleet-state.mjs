import { existsSync, readFileSync } from "node:fs";
import { isAbsolute, resolve } from "node:path";

function unique(values) {
  return Array.from(new Set(values));
}

function normalizeString(value) {
  return typeof value === "string" ? value.trim() : "";
}

function resolveFleetStatePath() {
  const raw = normalizeString(process.env.MESH_FLEET_STATE_PATH);
  if (!raw) {
    return null;
  }

  return isAbsolute(raw) ? raw : resolve(process.cwd(), raw);
}

function sanitizeNode(node) {
  const relayMultiaddrs = Array.isArray(node?.relayMultiaddrs)
    ? node.relayMultiaddrs.map(normalizeString).filter(Boolean)
    : [];
  const announceMultiaddrs = Array.isArray(node?.announceMultiaddrs)
    ? node.announceMultiaddrs.map(normalizeString).filter(Boolean)
    : [];

  return {
    peerId: normalizeString(node?.peerId),
    provider: normalizeString(node?.provider),
    region: normalizeString(node?.region),
    relayMultiaddrs,
    announceMultiaddrs,
  };
}

export function loadMeshFleetState() {
  const fleetStatePath = resolveFleetStatePath();
  if (!fleetStatePath) {
    return {
      path: null,
      nodes: [],
      multiaddrs: [],
    };
  }

  if (!existsSync(fleetStatePath)) {
    throw new Error(`Mesh fleet state not found at ${fleetStatePath}`);
  }

  const parsed = JSON.parse(readFileSync(fleetStatePath, "utf-8"));
  const nodes = Array.isArray(parsed?.nodes)
    ? parsed.nodes
      .map(sanitizeNode)
      .filter((node) => node.peerId && node.provider && node.region)
      .map((node) => ({
        peerId: node.peerId,
        provider: node.provider,
        region: node.region,
        relayMultiaddrs: node.relayMultiaddrs,
        announceMultiaddrs: node.announceMultiaddrs,
      }))
    : [];

  const multiaddrs = unique(
    nodes.flatMap((node) => (
      node.relayMultiaddrs.length > 0 ? node.relayMultiaddrs : node.announceMultiaddrs
    )),
  ).sort((left, right) => left.localeCompare(right));

  return {
    path: fleetStatePath,
    nodes: nodes
      .map((node) => ({
        peerId: node.peerId,
        provider: node.provider,
        region: node.region,
      }))
      .sort((left, right) => (
        left.region.localeCompare(right.region)
        || left.provider.localeCompare(right.provider)
        || left.peerId.localeCompare(right.peerId)
      )),
    multiaddrs,
  };
}
