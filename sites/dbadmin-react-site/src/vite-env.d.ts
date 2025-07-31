/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_GR8R_ADMIN_TOKEN: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
