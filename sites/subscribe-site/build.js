const fs = require("fs");
const path = require("path");
const { execSync } = require("child_process");

const DIST = path.join(__dirname, "dist");

// Remove existing dist
if (fs.existsSync(DIST)) {
  fs.rmSync(DIST, { recursive: true, force: true });
}

// Recreate dist
fs.mkdirSync(DIST);

// Copy HTML files
fs.readdirSync(__dirname)
  .filter(f => f.endsWith(".html"))
  .forEach(f => fs.copyFileSync(path.join(__dirname, f), path.join(DIST, f)));

// Copy assets folder if it exists
const assetsDir = path.join(__dirname, "assets");
if (fs.existsSync(assetsDir)) {
  fs.cpSync(assetsDir, path.join(DIST, "assets"), { recursive: true });
}

console.log("✅ Build complete. Files copied to dist/");
