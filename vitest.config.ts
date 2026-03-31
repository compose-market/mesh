import { readFileSync } from "node:fs";
import { defineConfig } from "vitest/config";

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
  define: {
    __APP_VERSION__: JSON.stringify("test"),
    __MESH_FLEET_NODES__: JSON.stringify(meshFleetNodes),
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
