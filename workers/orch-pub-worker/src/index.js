// v0.1.0 gr8r-orch-pub-worker ADDED: dev-first placeholder with __dev routes
// - No public surface (403), but exposes __dev helpers for local testing
// - Safe logging (console for info/debug; warn/error to Grafana)
// - Queue producer for YouTube (PUB_YOUTUBE_Q)
// - Optional DB1 claim (only if you set DB1_INTERNAL_KEY via dev vars)

import { getSecret } from "../../../lib/secrets.js";
import { createLogger } from "../../../lib/grafana.js";

// ---------- logging (matches your policy) ----------
const _logger = createLogger({ source: "gr8r-orch-pub-worker" });
async function safeLog(env, entry) {
  try {
    const level = entry?.level || "info";
    if (level === "debug" || level === "info") {
      console[level === "debug" ? "debug" : "log"](
        `[${entry?.service || "orch"}] ${entry?.message || ""}`,
        entry?.meta || {}
      );
      return;
    }
    await _logger(env, entry); // warn/error → Grafana
  } catch (e) {
    console.log("LOG_FAIL", entry?.service || "unknown", e?.stack || e?.message || e);
  }
}

// ---------- utils ----------
const uuid = () => {
  try { return crypto.randomUUID(); }
  catch { return Date.now().toString(36) + Math.random().toString(36).slice(2); }
};
const ms = (t0) => Date.now() - t0;

// ---------- worker defaults (posting options live elsewhere) ----------
const WORKER_DEFAULTS = {
  set_publish_at: true,
  privacy_status: "private",
  category_id: 22,
  made_for_kids: false,
  default_language: "en",
  tags: [],
  publish_timezone: "America/New_York",
  auto_shorts: true,
};

async function db1Request(env, method, path, body) {
  const key = await getSecret(env, "DB1_INTERNAL_KEY");

  // Ensure leading slash, but DO NOT force /db1 here—some endpoints are special (e.g. /publishing/claim)
  const urlPath = path.startsWith("/") ? path : `/${path}`;

  const headers = new Headers({ "Authorization": `Bearer ${key}` });
  if (method !== "GET") headers.set("Content-Type", "application/json");

  // *** BEST PRACTICE: Service Binding call (private Worker→Worker) ***
  const req = new Request("https://internal" + urlPath, {
    method,
    headers,
    body: method === "GET" ? undefined : JSON.stringify(body || {}),
  });

  const resp = await env.DB1.fetch(req);   // <— uses [[services]] binding "DB1"
  const txt = await resp.text();
  if (!resp.ok) throw new Error(`DB1 ${method} ${urlPath} ${resp.status} ${txt.slice(0,300)}`);
  return txt ? JSON.parse(txt) : null;
}

// ---------- DB1 helper (optional in dev) ----------
async function db1Fetch(env, path, body) {
  const t0 = Date.now();
  const data = await db1Request(env, "POST", path, body);  // e.g. "/publishing/claim"
  return { ok: true, data, duration_ms: ms(t0) };
}

async function db1Get(env, pathAndQuery) {
  return db1Request(env, "GET", pathAndQuery);  // e.g. "/publishing?status=scheduling&limit=50"
}

async function db1Upsert(env, table, payload) {
  // Generic table upsert: always target /db1/:table
  return db1Request(env, "POST", `/db1/${table}`, payload);  // e.g. /db1/videos
}

async function fetchChannelDefaults(env, channel_key) {
  try {
    const rows = await db1Get(env, `/db1/channels?key=${encodeURIComponent(channel_key)}&limit=1`);
    const raw = rows?.[0]?.json_defaults || null;
    try { return raw ? JSON.parse(raw) : null; } catch { return null; }
  } catch {
    return null; // ok in dev
  }
}

// ---------- queue ----------
async function enqueueYouTube(env, msg) {
  await env.PUB_YOUTUBE_Q.send(JSON.stringify(msg));
}

function buildQueueMessage({ request_id, row, channel_defaults }) {
  // row fields expected by consumer later; keep names stable
  return {
    kind: "upload",
    request_id,
    publishing_id: row.publishing_id,
    video_id: row.video_id,
    channel_key: row.channel_key,
    scheduled_at: row.scheduled_at,
    r2: row.r2_key ? { key: row.r2_key } : undefined,
    media_url: row.media_url,
    title: row.title,
    hook: row.hook,
    body: row.body,
    cta: row.cta,
    hashtags: row.hashtags,
    options_json: row.options_json || null,
    channel_defaults,
    enqueued_at: new Date().toISOString(),
  };
}

// ---------- dev helpers (no DB1 required) ----------
function makeFakeRow(overrides = {}) {
  // Minimal fields the consumer will want eventually
  return {
    publishing_id: 999001,
    video_id: 777001,
    channel_key: "youtube",
    scheduled_at: new Date(Date.now() + 5 * 60 * 1000).toISOString(), // 5 min from now
    media_url: overrides.media_url || "https://download.samplelib.com/mp4/sample-5s.mp4",
    title: overrides.title || "Dev test #Shorts",
    hook: overrides.hook || "Hook text",
    body: overrides.body || "Body text",
    cta: overrides.cta || "Subscribe for more",
    hashtags: overrides.hashtags || "#shorts #dev",
    options_json: overrides.options_json || null,
    r2_key: overrides.r2_key || undefined, // if you want to test R2 later
  };
}
// ---------- worker entrypoints ----------
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const rawPath = url.pathname;
    // remove leading "/orch" if present so our dev routes match
    const route = rawPath.replace(/^\/orch(?=\/|$)/, "");
    const method = request.method;
    const request_id = uuid();
    const t0 = Date.now();

    // 403 for everything except explicitly whitelisted __dev endpoints
    if (!route.startsWith("/__dev")) {
      return new Response("Forbidden", { status: 403 });
    }

    // GET /__dev/ping  → quick health check
    if (method === "GET" && route === "/__dev/ping") {
      return new Response(JSON.stringify({ ok: true, worker: "gr8r-orch-pub-worker" }), {
        status: 200, headers: { "Content-Type": "application/json" },
      });
    }

    // POST /__dev/enqueue-fake  (no DB1; enqueues one fake YouTube job)
    if (method === "POST" && route === "/__dev/enqueue-fake") {
      try {
        const body = await request.json().catch(() => ({}));
        const fake = makeFakeRow(body || {});
        const channel_defaults =
          (await fetchChannelDefaults(env, "youtube")) || WORKER_DEFAULTS;

        const msg = buildQueueMessage({ request_id, row: fake, channel_defaults });
        await enqueueYouTube(env, msg);

        console.log("(__dev) enqueued fake", { publishing_id: fake.publishing_id });

        return new Response(JSON.stringify({ ok: true, enqueued: 1, publishing_id: fake.publishing_id }), {
          status: 200, headers: { "Content-Type": "application/json" },
        });
      } catch (err) {
        await safeLog(env, {
          level: "error", service: "__dev", message: "enqueue-fake failed",
          meta: { request_id, route, method, ok: false, status_code: 500, error: String(err), duration_ms: ms(t0) },
        });
        return new Response(JSON.stringify({ error: "internal", message: String(err) }), {
          status: 500, headers: { "Content-Type": "application/json" },
        });
      }
    }

    // POST /__dev/claim  (optional: uses DB1 if DB1_INTERNAL_KEY is set)
    if (method === "POST" && route === "/__dev/claim") {
      try {
        const channel_key = "youtube";
        let rows = [];
        let usedDb1 = false;

        // try DB1 if key present; otherwise, fall back to fake
        try {
          const _k = await getSecret(env, "DB1_INTERNAL_KEY");
          if (_k) {
            const { data } = await db1Fetch(env, "/publishing/claim", {
              channel_key, limit: Number(env.CLAIM_LIMIT || 5), request_id,
            });
            rows = Array.isArray(data?.rows) ? data.rows : [];
            usedDb1 = true;
          }
        } catch {
          // ignore → we’ll go fake
        }

        if (!rows.length) rows = [makeFakeRow()];

        const chDefaults = (await fetchChannelDefaults(env, channel_key)) || WORKER_DEFAULTS;

        for (const r of rows) {
          const msg = buildQueueMessage({ request_id, row: r, channel_defaults: chDefaults });
          await enqueueYouTube(env, msg);
        }

        return new Response(JSON.stringify({ ok: true, usedDb1, enqueued: rows.length }), {
          status: 200, headers: { "Content-Type": "application/json" },
        });
      } catch (err) {
        await safeLog(env, {
          level: "error", service: "__dev", message: "claim failed",
          meta: { request_id, route, method, ok: false, status_code: 500, error: String(err), duration_ms: ms(t0) },
        });
        return new Response(JSON.stringify({ error: "internal", message: String(err) }), {
          status: 500, headers: { "Content-Type": "application/json" },
        });
      }
    }
        // POST /__dev/seed-queued
    if (method === "POST" && route === "/__dev/seed-queued") {
      const request_id = uuid(); const t0 = Date.now();
      try {
        // 1) Ensure channel exists (idempotent upsert)
        await db1Upsert(env, "channels", {
          key: "youtube",
          display_name: "YouTube",
          json_defaults: JSON.stringify({ privacy_status: "private", auto_shorts: true })
        });

        // 2) Create a video
        const title = `Dev Seed ${new Date().toISOString()}`;
        const v = await db1Upsert(env, "videos", {
          title,
          status: "Post Ready",
          video_type: "Other",
          r2_url: "https://download.samplelib.com/mp4/sample-5s.mp4",
          social_copy_hook: "Hook text",
          social_copy_body: "Body text",
          social_copy_cta: "Subscribe!",
          hashtags: "#shorts #dev"
        });
        const video_id = v?.video_id;

        // 3) Create publishing (queued)
        const scheduled_at = new Date(Date.now() + 10*60*1000).toISOString(); // 10 min out
        await db1Upsert(env, "publishing", {
          video_id,
          channel_key: "youtube",
          status: "queued",
          scheduled_at,
          options_json: JSON.stringify({ category_id: 22 })
        });

        return new Response(JSON.stringify({ ok: true, video_id, scheduled_at }), {
          status: 200, headers: { "Content-Type": "application/json" }
        });
      } catch (e) {
        await safeLog(env, { level: "error", service: "__dev", message: "seed-queued failed",
          meta: { error: String(e) }});
        return new Response(JSON.stringify({ error: "internal", message: String(e) }), {
          status: 500, headers: { "Content-Type": "application/json" }
        });
      }
    }

    // POST /__dev/claim-and-check
    if (method === "POST" && route === "/__dev/claim-and-check") {
      const request_id = uuid(); const t0 = Date.now();
      try {
        // 1) claim (queued -> scheduling) via DB1
        const { data } = await db1Fetch(env, "/publishing/claim", {
          channel_key: "youtube",
          limit: Number(env.CLAIM_LIMIT || 5),
          request_id
        });
        const claimed = Array.isArray(data?.rows) ? data.rows : [];

        // 2) enqueue each (so you also test Queues path)
        for (const r of claimed) {
          const msg = buildQueueMessage({ request_id, row: r, channel_defaults: null });
          await enqueueYouTube(env, msg);
        }

        // 3) check DB1 for rows now in 'scheduling'
        const scheduled = await db1Get(env, "/publishing?status=scheduling&limit=50");

        return new Response(JSON.stringify({
          ok: true,
          claimed_count: claimed.length,
          scheduling_count: Array.isArray(scheduled) ? scheduled.length : 0,
          sample: claimed[0] || null
        }), { status: 200, headers: { "Content-Type": "application/json" }});
      } catch (e) {
        await safeLog(env, { level: "error", service: "__dev", message: "claim-and-check failed",
          meta: { error: String(e) }});
        return new Response(JSON.stringify({ error: "internal", message: String(e) }), {
          status: 500, headers: { "Content-Type": "application/json" }
        });
      }
    }

    // anything else under /__dev
    return new Response(JSON.stringify({ error: "Not Found" }), {
      status: 404, headers: { "Content-Type": "application/json" },
    });
  },

  // You can enable cron later by setting ENABLE_CRON=1 and adding a trigger in wrangler.toml
  async scheduled(event, env) {
    if (!env.ENABLE_CRON) return;
    const request_id = uuid();
    const t0 = Date.now();
    try {
      // In v0.1 we do nothing on cron – dev-only endpoints are enough.
      console.log("[cron] noop", { request_id });
    } catch (e) {
      await safeLog(env, {
        level: "error", service: "cron", message: "cron failed",
        meta: { request_id, error: String(e), duration_ms: ms(t0) },
      });
    }
  },
};

