import { build } from "esbuild";

build({
  entryPoints: ["src/index.js"],
  bundle: true,
  outfile: "dist/index.mjs",
  format: "esm",
  platform: "browser",
  target: "es2022"
}).catch(() => process.exit(1));
