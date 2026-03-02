/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_THIRDWEB_CLIENT_ID: string;
  readonly VITE_API_URL?: string;
  readonly VITE_LAMBDA_URL?: string;
  readonly VITE_MANOWAR_URL?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
