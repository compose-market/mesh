export { MeshPage } from "./page";
export {
  buildBootstrapRegions,
  buildMeshScene,
  deriveBootstrapAnchors,
  derivePeerAnchor,
  resolveLocalMeshBootstrap,
  resolveMeshBootstrap,
  type MeshAnchorNode,
  type MeshBootstrapAnchor,
  type MeshBootstrapRegion,
  type MeshBootstrapResolution,
  type MeshScene,
  type MeshScenePeerNode,
} from "./model";
export {
  buildMeshDesiredState,
  desktopMeshService,
  mergeManifestIntoState,
  mergeMeshStatusIntoState,
  mergePeerIndexIntoState,
  type MeshDesiredState,
  type MeshPeerIndexPayload,
  type MeshRuntimeStatus,
} from "./runtime";
export {
  buildManifestPayload,
  canonicalManifestPayload,
  hydrateManifestNetworkFields,
  signAndPublishManifest,
  broadcastAgentManifest,
  type BuildManifestInput,
} from "./manifest";
export type { MeshManifest as DesktopMeshManifest } from "../../lib/types";
