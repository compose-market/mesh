import { readFileSync } from "node:fs";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const pkg = JSON.parse(readFileSync("./package.json", "utf-8"));
const meshFleetState = JSON.parse(readFileSync(new URL("../services/mesh/state/fleet-state.json", import.meta.url), "utf-8"));
const meshFleetNodes = Array.isArray(meshFleetState.nodes)
  ? meshFleetState.nodes.map((node: Record<string, unknown>) => ({
    peerId: typeof node.peerId === "string" ? node.peerId : "",
    provider: typeof node.provider === "string" ? node.provider : "",
    region: typeof node.region === "string" ? node.region : "",
  })).filter((node: { peerId: string; provider: string; region: string }) => (
    node.peerId.length > 0 && node.provider.length > 0 && node.region.length > 0
  ))
  : [];

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
