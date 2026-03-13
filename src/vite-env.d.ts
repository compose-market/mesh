/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_THIRDWEB_CLIENT_ID: string;
  readonly VITE_API_URL?: string;
  readonly VITE_LAMBDA_URL?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}

declare module "react-simple-maps";
declare module "world-atlas/countries-110m.json";
