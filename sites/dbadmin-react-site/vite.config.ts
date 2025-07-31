import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  base: './', // ✅ use relative paths for local + Pages preview
  build: {
    outDir: 'dist' // ✅ ensure output lands in dist/
  }
});
