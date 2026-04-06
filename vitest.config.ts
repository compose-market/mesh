import { readFileSync } from "node:fs";
import { defineConfig } from "vitest/config";
import { loadMeshFleetState } from "./config/mesh-fleet-state.mjs";

const pkg = JSON.parse(readFileSync("./package.json", "utf-8"));
const meshFleetState = loadMeshFleetState();

export default defineConfig({
  define: {
    __APP_NAME__: JSON.stringify(pkg.name),
    __APP_VERSION__: JSON.stringify(pkg.version),
    __MESH_FLEET_NODES__: JSON.stringify(meshFleetState.nodes),
    __MESH_FLEET_MULTIADDRS__: JSON.stringify(meshFleetState.multiaddrs),
  },
  test: {
    environment: "node",
    globals: true,
    include: ["src/**/*.test.ts", "tests/**/*.test.ts"],
    coverage: {
      provider: "v8",
      reporter: ["text", "lcov"],
      include: ["src/lib/**/*.ts"],
    },
  },
});
