import { readFileSync } from "node:fs";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { loadMeshFleetState } from "./config/mesh-fleet-state.mjs";

const pkg = JSON.parse(readFileSync("./package.json", "utf-8"));
const meshFleetState = loadMeshFleetState();

export default defineConfig({
  plugins: [react()],
  clearScreen: false,
  define: {
    __APP_NAME__: JSON.stringify(pkg.name),
    __APP_VERSION__: JSON.stringify(pkg.version),
    __MESH_FLEET_NODES__: JSON.stringify(meshFleetState.nodes),
    __MESH_FLEET_MULTIADDRS__: JSON.stringify(meshFleetState.multiaddrs),
  },
  server: {
    port: 1420,
    strictPort: true,
  },
});
