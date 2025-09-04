// lib/grafana.js  v1.0.0 (no hardcoded source; per-worker setup)
// Direct Loki push using secrets resolved via lib/secrets.js
import { getSecret } from "./secrets.js";

/**
 * Create a logger bound to a specific worker/source label.
 * Usage in a worker:
 *   import { createLogger } from "./lib/grafana.js";
 *   const log = createLogger({ source: "gr8r-db1-worker" });
 *   await log(env, { service: "db1-upsert", level: "info", message: "Upserted", meta: {...} });
 */
export function createLogger({ source }) {
  const src = (typeof source === "string" && source.trim()) ? source.trim() : "gr8r-unknown";

  return async function logToGrafana(env, entry) {
    if (!entry || typeof entry !== "object") return;

    const level   = validLevel(entry.level) ? entry.level : "info";
    const message = typeof entry.message === "string" ? entry.message : "";
    const service = typeof entry.service === "string" && entry.service.trim()
      ? entry.service.trim()
      : "unknown";

    // Labels (strings only). Keep low-cardinality.
    const labels = { level, source: src, service };

    // Coerce meta primitives; drop objects/arrays/functions.
    const meta = {};
    if (entry.meta && typeof entry.meta === "object") {
      for (const [k, v] of Object.entries(entry.meta)) {
        if (v == null) { meta[k] = null; continue; }
        const t = typeof v;
        if (t === "string" || t === "number" || t === "boolean") {
          meta[k] = t === "string" && v.length > 8000 ? v.slice(0, 8000) : v;
        }
      }
    }

    const lineObj = { message };
    if (Object.keys(meta).length) lineObj.meta = meta;

    const timestamp = (Date.now() * 1_000_000).toString(); // ns
    const payload = JSON.stringify({
      streams: [{ stream: labels, values: [[timestamp, JSON.stringify(lineObj)]] }]
    });

    // Resolve secrets (from Secrets Store or plain env)
    const [url, user, key] = await Promise.all([
      getSecret(env, "GRAFANACLOUD_GR8R_LOGS_URL"),
      getSecret(env, "GRAFANACLOUD_GR8R_LOGS_USER"),
      getSecret(env, "GRAFANACLOUD_GR8R_LOGS_KEY"),
    ]);

    const auth = "Basic " + btoa(`${user}:${key}`);

    const resp = await fetch(`${url}/loki/api/v1/push`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "Authorization": auth },
      body: payload
    });

    if (!resp.ok) {
      const text = await resp.text().catch(() => "");
      throw new Error(`Loki push failed: ${resp.status} ${text.slice(0, 500)}`);
    }
  };
}

function validLevel(l) {
  return l === "debug" || l === "info" || l === "warn" || l === "error";
}
