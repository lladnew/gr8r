// v1.0.3 gr8r-youtube-worker CHANGE: switch to chunked resumable uploads to avoid 413; add richer error capture & last-range logging
// v1.0.2 gr8r-youtube-worker ADDED: retry_count logging and tweaked Grafana logging message
// gr8r-youtube-worker v1.0.1 ADDED: check for existing platform_media_id and guard to stop past schedules and existing posts via matching media_ID
// gr8r-youtube-consumer-worker v1.0.0 live version with cron
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

// ----- internal auth helper (mirror orch) -----
async function getDb1Key(env) {
  const key = await getSecret(env, "DB1_INTERNAL_KEY");
  if (!key) throw new Error("missing_DB1_INTERNAL_KEY");
  return key;
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

// ADDED: upload sizing (CF Workers subrequest body is limited)
const MAX_SINGLESHOT = 95 * 1024 * 1024;         // ~95 MiB safety margin
const CHUNK_SIZE     = 8  * 1024 * 1024;         // 8 MiB (multiple of 256 KiB)

/** ADDED: fetch a byte range from media_url as an ArrayBuffer */
async function fetchRange(url, start, end) {
  const res = await fetch(url, { headers: { Range: `bytes=${start}-${end}` }});
  if (!(res.status === 206 || res.status === 200)) {
    const txt = await res.text().catch(() => "");
    throw new Error(`media_range_fetch_failed_${res.status}:${txt.slice(0,200)}`);
  }
  const buf = await res.arrayBuffer();
  // sanity: ensure server honored our range
  const expectedLen = (end - start + 1);
  if (res.status === 206 && buf.byteLength !== expectedLen) {
    throw new Error(`media_range_length_mismatch:${buf.byteLength}!=${expectedLen}`);
  }
  // if res.status===200, we allow it only when start===0 and it exactly equals expectedLen
  if (res.status === 200 && !(start === 0 && buf.byteLength === expectedLen)) {
    throw new Error(`media_range_unexpected_200:got=${buf.byteLength},want=${expectedLen},start=${start}`);
  }

  return new Uint8Array(buf);
}

/** ADDED: chunked resumable PUT loop (returns final JSON with id) */
async function uploadToSessionChunked(uploadUrl, mediaUrl, totalLen, contentType, logMeta, tokenForLogsOnly) {
  let offset = 0;
  let lastRangeAck = null;

  while (offset < totalLen) {
    const chunkEnd   = Math.min(offset + CHUNK_SIZE - 1, totalLen - 1);
    const chunkBytes = await fetchRange(mediaUrl, offset, chunkEnd);
    const chunkLen   = chunkBytes.byteLength;

    const headers = {
      "Content-Type":  contentType || "video/*",
      "Content-Length": String(chunkLen),
      "Content-Range": `bytes ${offset}-${offset + chunkLen - 1}/${totalLen}`,
    };

    const put = await fetch(uploadUrl, { method: "PUT", headers, body: chunkBytes });

    // 308 = partial accepted; server may echo Range header like "bytes=0-8388607"
    if (put.status === 308) {
      lastRangeAck = put.headers.get("Range") || null;
      offset += chunkLen;
      continue;
    }

    if (!put.ok) {
      const txt = await put.text().catch(() => "");
      // ADDED: better context in the thrown message
      throw new Error(`resumable_put_failed_${put.status}:${headers["Content-Range"]}:${(txt||"").slice(0,300)}`);
    }

    // Final chunk: server returns 200 + JSON Video resource
    try {
      const json = await put.json();
      return json;
    } catch (e) {
      throw new Error(`resumable_put_final_parse_failed:${headers["Content-Range"]}:${e?.message||e}`);
    }
  }

  // Should never get here
  throw new Error(`resumable_loop_exhausted:last_range_ack=${lastRangeAck||"none"}`);
}
/** REPLACED: chunked resumable PUT from a single streaming GET with low-copy accumulation */
async function uploadToSessionChunkedFromStream(uploadUrl, mediaUrl, totalLen, contentType) {
  const res = await fetch(mediaUrl);
  if (!res.ok || !res.body) {
    const txt = await res.text().catch(() => "");
    throw new Error(`media_stream_fetch_failed_${res.status}:${(txt||"").slice(0,200)}`);
  }

  const reader = res.body.getReader();
  let offset = 0;

  // Accumulate small incoming pieces until we hit CHUNK_SIZE, then do one copy per chunk.
  let parts = [];          // Array<Uint8Array>
  let partsBytes = 0;      // total bytes in parts

  // Helper: one allocation+copy per flushed chunk
  const flushChunk = async (finalFlush = false) => {
    if (partsBytes === 0) return null;

    // If last chunk AND smaller than CHUNK_SIZE, send exactly what's left.
    const sendLen = partsBytes;

    // Single allocation, copy each part (O(n), once per chunk)
    const chunk = new Uint8Array(sendLen);
    let pos = 0;
    for (const p of parts) {
      chunk.set(p, pos);
      pos += p.byteLength;
    }

    // Reset accumulators before network (lets GC free memory sooner)
    parts = [];
    partsBytes = 0;

    const end = offset + sendLen - 1;
    const headers = {
      "Content-Type":  contentType || "video/*",
      "Content-Length": String(sendLen),
      "Content-Range": `bytes ${offset}-${end}/${totalLen}`,
    };

    const put = await fetch(uploadUrl, { method: "PUT", headers, body: chunk });

    if (put.status === 308) {
      offset += sendLen;
      return { done: false, json: null };
    }
    if (!put.ok) {
      const txt = await put.text().catch(() => "");
      throw new Error(`resumable_put_failed_${put.status}:${headers["Content-Range"]}:${(txt||"").slice(0,300)}`);
    }
    // Final PUT returns JSON
    const json = await put.json().catch((e) => {
      throw new Error(`resumable_put_final_parse_failed:${headers["Content-Range"]}:${e?.message||e}`);
    });
    offset += sendLen;
    return { done: true, json };
  };

  while (true) {
    const { done, value } = await reader.read();
    if (done) {
      // Flush whatever remains (may be < CHUNK_SIZE)
      const final = await flushChunk(true);
      if (final && final.done) return final.json;
      // If stream ended exactly on a boundary, the last PUT would have already returned JSON
      // and we’d have exited above. Otherwise, this is an early end.
      if (offset !== totalLen) {
        throw new Error(`media_stream_ended_early_at_${offset}_of_${totalLen}`);
      }
      // Shouldn’t reach here normally
      throw new Error(`resumable_loop_exhausted_at_${offset}_of_${totalLen}`);
    }

    if (value && value.byteLength) {
      parts.push(value);
      partsBytes += value.byteLength;

      // If we’ve met/exceeded CHUNK_SIZE, flush exactly CHUNK_SIZE bytes.
      // We may have slightly overfilled; we’ll split the tail and keep remainder.
      if (partsBytes >= CHUNK_SIZE) {
        // Build a CHUNK_SIZE slice without copying twice:
        let need = CHUNK_SIZE;
        const toSend = [];
        let keep = [];
        for (const p of parts) {
          if (need === 0) { keep.push(p); continue; }
          if (p.byteLength <= need) {
            toSend.push(p);
            need -= p.byteLength;
          } else {
            // split p into head (toSend) + tail (keep)
            toSend.push(p.subarray(0, need));
            keep.push(p.subarray(need));
            need = 0;
          }
        }
        // Swap parts to keep only the remainder
        parts = keep;
        // Recompute sizes
        let toSendBytes = 0;
        for (const p of toSend) toSendBytes += p.byteLength;
        let keepBytes = 0;
        for (const p of keep) keepBytes += p.byteLength;
        partsBytes = keepBytes;

        // One allocation for exactly CHUNK_SIZE
        const chunk = new Uint8Array(toSendBytes);
        let pos = 0;
        for (const p of toSend) { chunk.set(p, pos); pos += p.byteLength; }

        const end = offset + chunk.byteLength - 1;
        const headers = {
          "Content-Type":  contentType || "video/*",
          "Content-Length": String(chunk.byteLength),
          "Content-Range": `bytes ${offset}-${end}/${totalLen}`,
        };

        const put = await fetch(uploadUrl, { method: "PUT", headers, body: chunk });

        if (put.status === 308) {
          offset += chunk.byteLength;
          // continue reading
        } else if (!put.ok) {
          const txt = await put.text().catch(() => "");
          throw new Error(`resumable_put_failed_${put.status}:${headers["Content-Range"]}:${(txt||"").slice(0,300)}`);
        } else {
          // Final JSON
          const json = await put.json().catch((e) => {
            throw new Error(`resumable_put_final_parse_failed:${headers["Content-Range"]}:${e?.message||e}`);
          });
          offset += chunk.byteLength;
          return json;
        }
      }
    }
  }
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

async function db1GetPublishingById(env, publishing_id, reqMeta) {
  const url = new URL("http://db1/db1/publishing");
  url.searchParams.set("id", String(publishing_id));
  url.searchParams.set("limit", "1");
  const res = await env.DB1.fetch(url.toString(), {
    method: "GET",
    headers: {
      "x-request-id": reqMeta?.request_id || shortId(),
      "Authorization": `Bearer ${await getDb1Key(env)}`,
    },
  });
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`db1_get_publishing_failed_${res.status}:${txt}`);
  }
  const rows = await res.json().catch(() => []);
  return Array.isArray(rows) && rows.length ? rows[0] : null;
}

// ----- YouTube Resumable Upload -----
async function createResumableSession(token, metadata, contentType, contentLength) {
  const url = "https://www.googleapis.com/upload/youtube/v3/videos?uploadType=resumable&part=snippet,status";

  const headers = {
    "Authorization": `Bearer ${token}`,
    "Content-Type": "application/json; charset=UTF-8",
    "X-Upload-Content-Type": contentType || "video/*",
  };

  if (Number.isFinite(contentLength) && contentLength > 0) {
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
  if (!Number.isFinite(contentLength) || contentLength <= 0) {
    throw new Error("missing_content_length");
  }

  const end = contentLength - 1;
  const rangeHdr = `bytes 0-${end}/${contentLength}`;
  const headers = {
    "Content-Type": contentType || "video/*",
    "Content-Length": String(contentLength),
    "Content-Range": rangeHdr,
  };

  const put = await fetch(uploadUrl, {
    method: "PUT",
    headers,
    body: streamBody,
  });

  // 308 = resumable "incomplete" (server didn’t get all bytes). Treat as error for now.
  if (put.status === 308) {
    const range = put.headers.get("Range"); // e.g., "bytes=0-1048575"
    throw new Error(`resumable_incomplete_308:${range || "no-range"}`);
  }

  if (!put.ok) {
    const txt = await put.text().catch(() => "");
    throw new Error(`resumable_put_failed_${put.status}:${rangeHdr}:${(txt||"").slice(0,300)}`);
  }

  // Success → JSON Video resource, including id
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

// DB1 expects { publishing_id, patch: { ...allowed columns... } }
async function db1Update(env, publishing_id, patch, reqMeta) {
  const body = { publishing_id, patch };
  const res = await env.DB1.fetch("http://db1/publishing/update", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-request-id": reqMeta?.request_id || shortId(),
      "Authorization": `Bearer ${await getDb1Key(env)}`,
    },
    body: JSON.stringify(body),
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
      "Authorization": `Bearer ${await getDb1Key(env)}`,
    },
    // DB1 expects { channel_key, limit }
    body: JSON.stringify(payload || { channel_key: "youtube", limit: 50 }),
  });
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`db1_list_scheduled_failed_${res.status}:${txt}`);
  }
  // returns { rows: [{ publishing_id, platform_media_id }, ...] }
  return res.json();
}


// ----- DB1: get channel defaults by channel key (e.g., 'youtube') -----
async function db1GetChannelDefaults(env, payload, reqMeta) {
  const res = await env.DB1.fetch("http://db1/channels/get-defaults", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-request-id": reqMeta?.request_id || shortId(),
      "Authorization": `Bearer ${await getDb1Key(env)}`,
    },
    // DB1 expects { channel_key: "youtube" }
    body: JSON.stringify(payload || { channel_key: "youtube" }),
  });
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`db1_get_defaults_failed_${res.status}:${txt}`);
  }
  return res.json(); // -> { json_defaults }
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
    privacy_status,       // "public" | "unlisted" | "private"
    publish_at,           // legacy; ISO string
    content_length,       // optional
    content_type,         // optional
    platform_media_id: existingYtId, // NEW: carry-through from queue
  } = payload;

  // Per-post overrides (payload.options_json may be string or object)
  let perPost = {};
  try {
    perPost = typeof payload.options_json === "string"
      ? JSON.parse(payload.options_json)
      : (payload.options_json && typeof payload.options_json === "object" ? payload.options_json : {});
  } catch { perPost = {}; }

  // Use either publish_at (legacy name) or scheduled_at (what orch sends)
  const scheduledAt =
    perPost.publish_at ??
    payload.publish_at ??
    payload.scheduled_at ??
    null;

  // Guard: require a future schedule (toggle via env if you want)
  if (env.REQUIRE_FUTURE_SCHEDULE === "1") {
    const isFuture = scheduledAt && new Date(scheduledAt) > new Date();
    if (!isFuture) {
      throw new Error("require_future_schedule: missing or past scheduled_at");
    }
  }

  // If the DB row already has a YouTube ID, don't re-upload.
  if (existingYtId) {
    const scheduled = scheduledAt && new Date(scheduledAt) > new Date();
    await logWarn(env, "skip_upload_existing_platform_id", {
      request_id: reqId,
      publishing_id,
      youtube_video_id: existingYtId,
      scheduledAt,
    });

    // Keep DB coherent
    await db1Update(
      env,
      publishing_id,
      {
        status: scheduled ? "scheduled" : "posted",
        posted_at: scheduled ? null : nowIso(),
      },
      { request_id: reqId }
    );

    return { ok: true, skipped: true, reason: "existing_platform_id" };
  }

  // channel key (Channels.key); default to 'youtube' unless explicitly provided
   const channel_key = payload.channel_key || "youtube";

  // Fetch defaults from DB1 and merge
  const defaultsResp = await db1GetChannelDefaults(env, { channel_key }, { request_id: reqId })
    .catch(() => null);

  const defaults = defaultsResp?.json_defaults || {};

  // merge order: per-post overrides > payload fields > channel defaults > hardcoded fallback
  const mergedPrivacy  = (perPost.privacy_status ?? privacy_status) || defaults.privacy_status || "public";
  const mergedCategory = (perPost.category_id    ?? category_id)    || defaults.category_id    || "22";

  const mergedTags =
    Array.isArray(perPost.tags) ? perPost.tags :
    (Array.isArray(tags) ? tags :
    (Array.isArray(defaults.tags) ? defaults.tags : []));

  const descTemplate = (typeof perPost.description_template === "string")
    ? perPost.description_template
    : (typeof defaults.description_template === "string" ? defaults.description_template : null);

  const appendShorts = (perPost.append_shorts ?? perPost.auto_shorts ??
                        defaults.append_shorts ?? defaults.auto_shorts ?? true);

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

    // ADDED: detect Range support from origin (if HEAD provided header)
    let supportsRanges = false;
    try {
      const head = await fetch(media_url, { method: "HEAD" });
      if (head.ok) {
        const ar = (head.headers.get("accept-ranges") || "").toLowerCase();
        supportsRanges = ar.includes("bytes");
      }
    } catch (_) {
      // if HEAD fails here, we’ll just assume false and use streaming fallback
      supportsRanges = false;
    }

  // build snippet/status
      const description = buildDescription({
        hook, body, cta, hashtags,
        template: descTemplate,
        append_shorts: appendShorts,
      });

      const scheduled = scheduledAt && new Date(scheduledAt) > new Date();
      const status = scheduled
        ? { privacyStatus: "private", publishAt: new Date(scheduledAt).toISOString() }
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

  // CHANGED: choose chunked path for big files to avoid 413
  let putJson;
  if (finalLen > MAX_SINGLESHOT) {
    if (supportsRanges) {
      // Prefer Range-based chunking when origin honors Range
      putJson = await uploadToSessionChunked(
        uploadUrl,
        media_url,
        finalLen,
        finalType,
        { publishing_id, title },
        token
      );
    } else {
      // Fallback: single streaming GET → client-side chunking
      putJson = await uploadToSessionChunkedFromStream(
        uploadUrl,
        media_url,
        finalLen,
        finalType
      );
    }
  } else {
    // Existing single-shot path for small files
    const mediaResp = await fetch(media_url);
    if (!mediaResp.ok || !mediaResp.body) {
      throw new Error(`media_fetch_failed_${mediaResp.status}`);
    }
    putJson = await uploadToSession(uploadUrl, mediaResp.body, finalType, finalLen);
  }

  const ytId = putJson?.id;
  if (!ytId) throw new Error("youtube_missing_video_id");

  // write back to DB1
  await db1Update(
    env,
    publishing_id,
    {
      platform_media_id: ytId,
      platform_url: `https://youtu.be/${ytId}`,
      status: scheduled ? "scheduled" : "posted",
      posted_at: scheduled ? null : nowIso(),
    },
    { request_id: reqId }
  );


  console.log(`[${SERVICE}] upload ok publishing_id=${publishing_id} yt=${ytId} scheduled=${scheduled}`);
  return { ok: true, ytId, scheduled, duration_ms: Date.now() - t0 };
}

// ----- Poll path (HTTP + Cron) -----
async function pollScheduled(env, reqId) {
  const route = "POST /yt/poll-scheduled";
  const list = await db1ListScheduled(env, { channel_key: "youtube", limit: 50 }, { request_id: reqId });
  const rows = Array.isArray(list?.rows) ? list.rows : [];
  if (!rows.length) return { ok: true, checked: 0, updated: 0 };

  const { token } = await getAccessToken(env);

  // Map platform_media_id (YouTube ID) → publishing row
  const idMap = rows.reduce((acc, r) => {
    if (r.platform_media_id) acc[r.platform_media_id] = r;
    return acc;
  }, {});
  const ids = Object.keys(idMap);
  if (!ids.length) return { ok: true, checked: rows.length, updated: 0 };


  const statuses = await videosGetStatuses(token, ids);
  let updated = 0;
  for (const ytId of ids) {
    const st = statuses[ytId];
    // When published, YouTube status.privacyStatus becomes "public" (or "unlisted")
    if (st && (st.privacyStatus === "public" || st.privacyStatus === "unlisted")) {
      const publishing_id = idMap[ytId].publishing_id;
      await db1Update(env, publishing_id, { status: "posted", posted_at: nowIso() }, { request_id: reqId });
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
              const errorText = String(err?.message || err);

              // Pull identifiers for better logging & DB updates
              let pubId = null;
              let videoTitle = null;
              try {
                const raw = typeof msg.body === "string" ? JSON.parse(msg.body) : msg.body;
                pubId = raw?.publishing_id ?? null;
                videoTitle = raw?.title ?? null;
              } catch (_) {}

              // Decide retryability (same rules you already had)
              const nonRetryable =
                errorText.startsWith("require_future_schedule") ||
                errorText.startsWith("skip_upload_existing_platform_id") ||
                errorText.startsWith("missing_content_length") ||
                (/^resumable_put_failed_4\d\d/.test(errorText) && !/429/.test(errorText)) ||
                errorText.startsWith("oauth_refresh_failed_400");

              // If we can, increment retry_count for visibility
              if (pubId) {
                try {
                  const row = await db1GetPublishingById(env, pubId, { request_id: reqId });
                  const current = Number(row?.retry_count || 0);
                  const next = nonRetryable ? current : current + 1; // don't bump on terminal failures
                  await db1Update(env, pubId, {
                    retry_count: next,
                    last_error: errorText.slice(0, 1000),
                  }, { request_id: reqId });
                } catch (_) {
                  // ignore metrics write failures
                }
              }

              // 🔎 Better log message + richer meta
              await logError(env, `youtube_worker failed on "${videoTitle || "(untitled)"}"`, {
                request_id: reqId,
                route: "queue/PUB_YOUTUBE_Q",
                method: "queue",
                publishing_id: pubId ?? undefined,
                video_title: videoTitle ?? undefined,
                error: errorText,
                ok: false,
                status_code: 500,
                duration_ms: Date.now() - t0,
              });

              if (nonRetryable) {
                if (pubId) {
                  try {
                    const terminalStatus =
                      errorText.startsWith("require_future_schedule") ||
                      errorText.startsWith("skip_upload_existing_platform_id")
                        ? "skipped"
                        : "error";
                    await db1Update(env, pubId, { status: terminalStatus }, { request_id: reqId });
                  } catch (_) {}
                }
                msg.ack();
                continue;
              }

              // Retryable: keep status as-is (scheduling), we already bumped retry_count & last_error
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
