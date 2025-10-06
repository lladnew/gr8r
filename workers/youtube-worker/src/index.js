// gr8r-youtube-consumer-worker v0.1.0
// ADD: YouTube Queues consumer + manual poll endpoint + DB1 write-backs
// Standards: Service Binding (DB1), safeLog to Grafana on warn/error, console on success

import { getSecret } from "../../../lib/secrets.js";
import { createLogger } from "../../../lib/grafana.js";

// ---------- logging (matches your policy) ----------
const _logger = createLogger({ source: "gr8r-youtube-worker" });
async function safeLog(env, entry) {
  try {
    const level = entry?.level || "info";
    if (level === "debug" || level === "info") {
      console[level === "debug" ? "debug" : "log"](
        `[${entry?.service || "youtube"}] ${entry?.message || ""}`,
        entry?.meta || {}
      );
      return;
    }
    await _logger(env, entry); // warn/error → Grafana
  } catch (e) {
    console.log("LOG_FAIL", entry?.service || "unknown", e?.stack || e?.message || e);
  }
}

const SOURCE  = "gr8r-youtube-worker";
const SERVICE = "youtube-worker";

// ----- tiny helpers -----
const nowIso = () => new Date().toISOString();
const shortId = () => crypto.randomUUID().slice(0, 8);

function buildDescription({ hook, body, cta, hashtags, template, append_shorts = true }) {
  let desc;
  if (template && typeof template === "string") {
    // minimal templating with placeholders
    desc = template
      .replaceAll("<<hook>>", hook || "")
      .replaceAll("<<body>>", body || "")
      .replaceAll("<<cta>>", cta || "")
      .replaceAll("<<hashtags>>", hashtags || "");
  } else {
    desc = [hook, body, cta, hashtags].filter(Boolean).join("\n\n");
  }
  desc = (desc || "").trim();
  if (append_shorts && !/#Shorts\b/i.test(desc)) {
    desc += (desc ? " " : "") + "#Shorts";
  }
  return desc;
}


async function logError(env, message, meta = {}) {
  await safeLog(env, {
    level: "error",
    service: SERVICE,
    message,
    meta: { ...meta },
  });
}

async function logWarn(env, message, meta = {}) {
  await safeLog(env, {
    level: "warn",
    service: SERVICE,
    message,
    meta: { ...meta },
  });
}

// ----- OAuth (Refresh → Access Token) -----
async function getAccessToken(env) {
  const t0 = Date.now();

  // Prefer Secrets Store via getSecret(); fall back to env bindings if needed
  const client_id = (await getSecret(env, "YOUTUBE_CLIENT_ID"))     || env.YOUTUBE_CLIENT_ID;
  const client_secret = (await getSecret(env, "YOUTUBE_CLIENT_SECRET")) || env.YOUTUBE_CLIENT_SECRET;
  const refresh_token = (await getSecret(env, "YOUTUBE_REFRESH_TOKEN")) || env.YOUTUBE_REFRESH_TOKEN;

  if (!client_id || !client_secret || !refresh_token) {
    throw new Error("missing_youtube_oauth_secrets(client_id|client_secret|refresh_token)");
  }

  const body = new URLSearchParams({
    grant_type: "refresh_token",
    client_id,
    client_secret,
    refresh_token,
  });

  const res = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body,
  });

  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`oauth_refresh_failed_${res.status}:${txt}`);
  }
  const json = await res.json(); // { access_token, expires_in, token_type, scope }
  return { token: json.access_token, duration_ms: Date.now() - t0 };
}


// ----- YouTube Resumable Upload -----
async function createResumableSession(token, metadata, contentType, contentLength) {
  const url = "https://www.googleapis.com/upload/youtube/v3/videos?uploadType=resumable&part=snippet,status";

  const headers = {
    "Authorization": `Bearer ${token}`,
    "Content-Type": "application/json; charset=UTF-8",
    "X-Upload-Content-Type": contentType || "video/*",
  };
  if (contentLength && Number.isFinite(contentLength)) {
    headers["X-Upload-Content-Length"] = String(contentLength);
  }

  const res = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify(metadata),
  });

  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`resumable_init_failed_${res.status}:${txt}`);
  }

  const uploadUrl = res.headers.get("Location");
  if (!uploadUrl) throw new Error("missing_resumable_location");
  return uploadUrl;
}

async function uploadToSession(uploadUrl, streamBody, contentType, contentLength) {
  const headers = { "Content-Type": contentType || "video/*" };
  if (contentLength && Number.isFinite(contentLength)) {
    headers["Content-Length"] = String(contentLength);
  }

  const put = await fetch(uploadUrl, {
    method: "PUT",
    headers,
    body: streamBody,
  });

  if (!put.ok) {
    const txt = await put.text().catch(() => "");
    throw new Error(`resumable_put_failed_${put.status}:${txt}`);
  }

  // On success, Google replies with JSON video resource
  return put.json();
}

// ----- YouTube Status Poll -----
async function videosGetStatuses(token, ids) {
  if (!ids.length) return {};
  const url = `https://www.googleapis.com/youtube/v3/videos?part=status,snippet&id=${encodeURIComponent(ids.join(","))}`;
  const res = await fetch(url, { headers: { "Authorization": `Bearer ${token}` } });
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`videos_list_failed_${res.status}:${txt}`);
  }
  const json = await res.json();
  const out = {};
  for (const item of json.items || []) {
    out[item.id] = item.status || null;
  }
  return out;
}

// ----- DB1 helpers (Service Binding) -----
async function db1Update(env, payload, reqMeta) {
  const res = await env.DB1.fetch("http://db1/publishing/update", {
    method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-request-id": reqMeta?.request_id || shortId(),
        "Authorization": `Bearer ${env.DB1_INTERNAL_KEY}`,
      },
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`db1_update_failed_${res.status}:${txt}`);
  }
  return res.json().catch(() => ({}));
}

async function db1ListScheduled(env, payload, reqMeta) {
  const res = await env.DB1.fetch("http://db1/publishing/list-scheduled", {
    method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-request-id": reqMeta?.request_id || shortId(),
        "Authorization": `Bearer ${env.DB1_INTERNAL_KEY}`,
      },
    body: JSON.stringify(payload || { platform: "youtube", limit: 50 }),
  });
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`db1_list_scheduled_failed_${res.status}:${txt}`);
  }
  return res.json();
}

// ----- DB1: get channel defaults by channel key (e.g., 'youtube') -----
async function db1GetChannelDefaults(env, payload, reqMeta) {
  const res = await env.DB1.fetch("http://db1/channels/get-defaults", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-request-id": reqMeta?.request_id || shortId(),
      "Authorization": `Bearer ${env.DB1_INTERNAL_KEY}`,
    },
    // Expecting DB1 route to accept { key: "youtube" }
    body: JSON.stringify(payload || { key: "youtube" }),
  });
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`db1_get_defaults_failed_${res.status}:${txt}`);
  }
  return res.json(); // expect { json_defaults: {...} }
}


// ----- Main processing for a single message -----
async function processMessage(env, msg, reqId) {
  const t0 = Date.now();
  const route = "queue/PUB_YOUTUBE_Q";
  let payload = msg.body;
  if (typeof payload === "string") {
    try { payload = JSON.parse(payload); } catch { /* keep as raw string */ }
  }
  if (!payload || typeof payload !== "object") {
    throw new Error("invalid_message_payload");
  }

  const {
    publishing_id,
    video_id,
    media_url,
    title,
    hook,
    body,
    cta,
    hashtags,
    tags,
    category_id,
    privacy_status,       // "public" | "unlisted" | "private" (immediate publish uses public/unlisted)
    publish_at,           // ISO string → future = scheduled
    content_length,       // optional; if missing, we'll try HEAD
    content_type          // optional; fallback via HEAD
  } = payload;

  // channel key (Channels.key); default to 'youtube' unless explicitly provided
  const channel_key = payload.channel_key || "youtube";

  // Fetch defaults from DB1 and merge
  const defaultsResp = await db1GetChannelDefaults(env, { key: channel_key }, { request_id: reqId })
    .catch(() => null);
  const defaults = defaultsResp?.json_defaults || {};

  // merge: queue payload wins; otherwise fallback to defaults; otherwise hardcoded fallback
  const mergedPrivacy  = privacy_status || defaults.privacy_status || "public";
  const mergedCategory = category_id || defaults.category_id || "22";
  const mergedTags     = Array.isArray(tags) ? tags :
                         (Array.isArray(defaults.tags) ? defaults.tags : []);

  // Respect either field name: append_shorts (new) or auto_shorts (existing)
  const appendShorts = (defaults.append_shorts ?? defaults.auto_shorts ?? true);

  // Optional description template from defaults
  const descTemplate = (typeof defaults.description_template === "string")
    ? defaults.description_template
    : null;

  if (!publishing_id || !media_url || !title) {
    throw new Error("missing_required_fields(publishing_id|media_url|title)");
  }

  // try to learn content-length/content-type (R2 should provide them; HEAD preferred)
  let finalLen = Number(content_length) || 0;
  let finalType = content_type || "";
  if (!finalLen || !finalType) {
    const head = await fetch(media_url, { method: "HEAD" });
    if (head.ok) {
      if (!finalLen)  finalLen  = Number(head.headers.get("content-length") || 0);
      if (!finalType) finalType = head.headers.get("content-type") || "video/*";
    }
  }
  if (!finalLen || !Number.isFinite(finalLen)) {
    throw new Error("missing_content_length"); // keep resumable single-shot strict to avoid partials
  }
  if (!finalType) {
    finalType = "video/*";
  }

  // build snippet/status
      const description = buildDescription({
        hook, body, cta, hashtags,
        template: descTemplate,
        append_shorts: appendShorts,
      });

      const scheduled = publish_at && new Date(publish_at) > new Date();
      const status = scheduled
        ? { privacyStatus: "private", publishAt: new Date(publish_at).toISOString() }
        : { privacyStatus: mergedPrivacy };

      const snippet = {
        title,
        description,
        tags: mergedTags,
        categoryId: mergedCategory,
      };



  // OAuth
  const { token } = await getAccessToken(env);

  // init resumable
  const uploadUrl = await createResumableSession(token, { snippet, status }, finalType, finalLen);

  // stream media (GET) -> PUT to session
  const mediaResp = await fetch(media_url);
  if (!mediaResp.ok || !mediaResp.body) {
    throw new Error(`media_fetch_failed_${mediaResp.status}`);
  }

  const putJson = await uploadToSession(uploadUrl, mediaResp.body, finalType, finalLen);
  const ytId = putJson?.id;
  if (!ytId) throw new Error("youtube_missing_video_id");

  // write back to DB1
  const updatePayload = {
    publishing_id,
    youtube_video_id: ytId,
    youtube_url: `https://youtu.be/${ytId}`,
    status: scheduled ? "scheduled" : "posted",
    posted_at: scheduled ? null : nowIso(),
  };
  await db1Update(env, updatePayload, { request_id: reqId });

  console.log(`[${SERVICE}] upload ok publishing_id=${publishing_id} yt=${ytId} scheduled=${scheduled}`);
  return { ok: true, ytId, scheduled, duration_ms: Date.now() - t0 };
}

// ----- Poll path (HTTP + Cron) -----
async function pollScheduled(env, reqId) {
  const route = "POST /yt/poll-scheduled";
  const list = await db1ListScheduled(env, { platform: "youtube", limit: 50 }, { request_id: reqId });
  const rows = list?.items || [];
  if (!rows.length) return { ok: true, checked: 0, updated: 0 };

  const { token } = await getAccessToken(env);
  const idMap = rows
    .filter(r => r.youtube_video_id)
    .reduce((acc, r) => (acc[r.youtube_video_id] = r, acc), {});
  const ids = Object.keys(idMap);
  if (!ids.length) return { ok: true, checked: 0, updated: 0 };

  const statuses = await videosGetStatuses(token, ids);
  let updated = 0;
  for (const ytId of ids) {
    const st = statuses[ytId];
    // When published, YouTube status.privacyStatus becomes "public" (or "unlisted")
    if (st && (st.privacyStatus === "public" || st.privacyStatus === "unlisted")) {
      const publishing_id = idMap[ytId].publishing_id;
      await db1Update(env, { publishing_id, status: "posted", posted_at: nowIso() }, { request_id: reqId });
      updated++;
    }
  }
  console.log(`[${SERVICE}] poll ok checked=${rows.length} updated=${updated}`);
  return { ok: true, checked: rows.length, updated };
}

export default {
  // ----- Queues consumer -----
  async queue(batch, env, ctx) {
    for (const msg of batch.messages) {
      const reqId = shortId();
      const t0 = Date.now();
      try {
        const result = await processMessage(env, msg, reqId);
        // explicit ack on success
        msg.ack();
      } catch (err) {
        await logError(env, "youtube_consumer_failed", {
          request_id: reqId,
          route: "queue/PUB_YOUTUBE_Q",
          method: "queue",
          error: String(err?.message || err),
          ok: false,
          status_code: 500,
          duration_ms: Date.now() - t0,
        });
        // hand back to queue
        msg.retry();
      }
    }
  },

  // ----- Minimal HTTP surface: manual poll path -----
  async fetch(req, env, ctx) {
    const url = new URL(req.url);
    const reqId = shortId();
    const t0 = Date.now();

    try {
      if (req.method === "POST" && url.pathname === "/yt/poll-scheduled") {
        const out = await pollScheduled(env, reqId);
        return new Response(JSON.stringify(out), {
          headers: { "Content-Type": "application/json" },
          status: 200,
        });
      }
      return new Response("Not Found", { status: 404 });
    } catch (err) {
      await logError(env, "poll_scheduled_failed", {
        request_id: reqId,
        route: "POST /yt/poll-scheduled",
        method: "POST",
        error: String(err?.message || err),
        ok: false,
        status_code: 500,
        duration_ms: Date.now() - t0,
      });
      return new Response("Internal Error", { status: 500 });
    }
  },

  // ----- Cron hook (disabled until you add a trigger in wrangler.toml) -----
  async scheduled(controller, env, ctx) {
    const reqId = shortId();
    try {
      await pollScheduled(env, reqId);
    } catch (err) {
      await logError(env, "cron_poll_failed", {
        request_id: reqId,
        route: "CRON /yt/poll-scheduled",
        method: "SCHEDULED",
        error: String(err?.message || err),
        ok: false,
        status_code: 500,
      });
    }
  },
};
