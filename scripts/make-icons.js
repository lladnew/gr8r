// scripts/make-icons.js
// Usage: node scripts/make-icons.js <source.png> <outDir>
//
// Example:
//   node scripts/make-icons.js assets/GR8R_logo_ascending_grow_stairs.png sites/dbadmin-react-site/public
//
// Requires: npm i -D sharp

const fs = require("fs");
const path = require("path");
const sharp = require("sharp");

async function main() {
  const src = process.argv[2];
  const outDir = process.argv[3] || ".";

  if (!src) {
    console.error("Missing source image. Usage: node scripts/make-icons.js <source.png> <outDir>");
    process.exit(1);
  }
  if (!fs.existsSync(src)) {
    console.error(`Source not found: ${src}`);
    process.exit(1);
  }
  if (!fs.existsSync(outDir)) {
    fs.mkdirSync(outDir, { recursive: true });
  }

  const sizes = [
    { w: 16, h: 16, name: "favicon-16.png" },
    { w: 32, h: 32, name: "favicon-32.png" },
    { w: 180, h: 180, name: "apple-touch-icon.png" },
  ];

  // If your logo isn’t square, this will center-crop to square and keep transparency
  const commonResize = (w, h) => ({
    width: w,
    height: h,
    fit: "cover",
    position: "center",
    background: { r: 0, g: 0, b: 0, alpha: 0 },
  });

  for (const s of sizes) {
    const outPath = path.join(outDir, s.name);
    await sharp(src).resize(commonResize(s.w, s.h)).png().toFile(outPath);
    console.log(`Wrote ${outPath}`);
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
