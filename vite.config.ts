import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const pkg = JSON.parse(readFileSync("./package.json", "utf-8"));

function loadMeshFleetNodes(): Array<{ peerId: string; provider: string; region: string }> {
  const fleetStatePath = resolve(process.cwd(), "state", "fleet-state.json");
  if (!existsSync(fleetStatePath)) {
    return [];
  }

  const parsed = JSON.parse(readFileSync(fleetStatePath, "utf-8")) as { nodes?: unknown[] };
  return Array.isArray(parsed.nodes)
    ? parsed.nodes.map((node) => ({
      peerId: typeof (node as { peerId?: unknown }).peerId === "string" ? (node as { peerId: string }).peerId : "",
      provider: typeof (node as { provider?: unknown }).provider === "string" ? (node as { provider: string }).provider : "",
      region: typeof (node as { region?: unknown }).region === "string" ? (node as { region: string }).region : "",
    })).filter((node) => node.peerId.length > 0 && node.provider.length > 0 && node.region.length > 0)
    : [];
}

const meshFleetNodes = loadMeshFleetNodes();

export default defineConfig({
  plugins: [react()],
  clearScreen: false,
  define: {
    __APP_VERSION__: JSON.stringify(pkg.version),
    __MESH_FLEET_NODES__: JSON.stringify(meshFleetNodes),
  },
  server: {
    port: 1420,
    strictPort: true,
  },
});
