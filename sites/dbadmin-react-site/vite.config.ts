// vite.config.ts
import { defineConfig, loadEnv } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig(({ mode }) => {
  // Load .env / .env.local without leaking to client
  const env = loadEnv(mode, process.cwd(), ''); // no prefix filter

  const ACCESS_ID = env.DB1_SERVICE_TOKEN_ID || '';
  const ACCESS_SECRET = env.DB1_SERVICE_TOKEN_SECRET || '';
  const INTERNAL_KEY = env.DB1_INTERNAL_KEY || '';

  return {
    plugins: [react()],
    base: './',
    build: { outDir: 'dist' },
    server: {
      port: 5173,
      proxy: {
        // Proxy only relative /db1/* calls during dev
        '/db1': {
          target: 'https://admin.gr8r.com',
          changeOrigin: true,
          secure: true,
          configure: (proxy) => {
            proxy.on('proxyReq', (proxyReq) => {
              // ✅ Satisfy Cloudflare Access at the edge (no user cookie needed)
              if (ACCESS_ID && ACCESS_SECRET) {
                proxyReq.setHeader('CF-Access-Client-Id', ACCESS_ID);
                proxyReq.setHeader('CF-Access-Client-Secret', ACCESS_SECRET);
              }
              // ✅ Bypass your Worker’s JWT path as “internal”
              if (INTERNAL_KEY) {
                proxyReq.setHeader('Authorization', `Bearer ${INTERNAL_KEY}`);
              }
              // ✅ Ensure your Worker’s CORS allowlist matches
              proxyReq.setHeader('Origin', 'https://admin.gr8r.com');
            });
          },
        },
      },
    },
  };
});
