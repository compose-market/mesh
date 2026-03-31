/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_THIRDWEB_CLIENT_ID: string;
  readonly VITE_API_URL?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}

declare module "react-simple-maps";
declare module "world-atlas/countries-110m.json";

declare const __MESH_FLEET_NODES__: ReadonlyArray<{
  peerId: string;
  provider: string;
  region: string;
}>;
