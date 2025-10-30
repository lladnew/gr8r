// gr8r-ytplsync-worker v1.0.2
// CHANGE: add safeLog shim w/ INFO_TO_GRAFANA + promote gate; DEBUG toggles
// CHANGE: add fetchWithRetry (429/5xx backoff, Retry-After) + response body previews
// CHANGE: add getAccessToken(env) helper; addToPlaylist(env,{videoId,playlistId,request_id})
// CHANGE: playlist id from YT_PIVOT_PLAYLIST_ID (secret/env) w/ CONFIG fallback
// CHANGE: clearer 401 path; richer success/error logs; no external URLs required by caller
// gr8r-ytplsync-worker v0.2.1
// CHANGE: Add CONFIG.LOG_SUCCESS_TO_GRAFANA toggle
//   - When true: successes log to Grafana (level=info) + console
//   - When false: successes log to console only
// PRIOR CHANGES: align secret names; static CONFIG for playlist/regex; errors -> Grafana

import { createLogger } from "../../../lib/grafana.js";
import { getSecret }   from "../../../lib/secrets.js";

// ---------- STATIC CONFIG (single edit point) ----------
const CONFIG = {
  // Set your Pivot Year playlist ID here (e.g., "PLxxxxxxxxxxxxxxxx")
  PLAYLIST_ID: "PLEIW-sxRCY_1h01CHXwu6DOdeB3F8OWJt",

  // Regex must capture the numeric day in group 1
  TITLE_REGEX: /Pivot\s*Year\s*Day\s*(\d+)/i,
};
// ------------------------------------------------------

const SOURCE = "gr8r-ytplsync-worker";
// ---------- DEBUG / VERBOSE flags (env) ----------
// Enable via worker vars/secrets: YTPLSYNC_DEBUG=1, VERBOSE_YTPLSYNC=1, INFO_TO_GRAFANA=1
const bool = (v) => v === "1" || v === "true" || v === "on";
function envFlag(env, key, def=false){ try{ return bool(String(env?.[key] ?? "")); }catch{ return def; } }

// Tiny utils
const sleep = (ms)=> new Promise(r=> setTimeout(r, ms));
const shortId = ()=> Math.random().toString(36).slice(2,8);

// ---------- safeLog shim ----------
const _logger = createLogger({ source: SOURCE });
// Policy: error/warn => always Grafana. info => Grafana only if INFO_TO_GRAFANA=1 or meta.promote===true.
async function safeLog(env, { level="info", msg, ...meta } = {}) {
  const INFO_TO_GRAFANA = envFlag(env, "INFO_TO_GRAFANA", false);
  const entry = { msg, level, ...meta };

  // Console always
  (level === "error" ? console.error :
   level === "warn"  ? console.warn  :
                       console.log)(JSON.stringify({ source: SOURCE, ...entry }));

  // Decide whether to ship to Grafana
  const shouldShip =
    level === "error" || level === "warn" ||
    (level === "info" && (INFO_TO_GRAFANA || meta?.promote === true));

  if (!shouldShip) return;
  try { await _logger(env, entry); } catch { /* never escalate logging failures */ }
}

function okJson(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "content-type": "application/json" },
  });
}

export default {
  async fetch(req, env) {
    const url = new URL(req.url);

    if (req.method === "POST" && url.pathname === "/internal/api/youtube/playlist-sync") {
      return handleSync(req, env);
    }

    return new Response("Not found", { status: 404 });
  },
};

async function handleSync(req, env) {
  const started = Date.now();
  const request_id = shortId();

  // ---- Auth (internal bearer) ----
  const auth = req.headers.get("authorization") || "";
  const token = auth.startsWith("Bearer ") ? auth.slice(7) : "";
  let INTERNAL_KEY;
  try {
    INTERNAL_KEY = await getSecret(env, "INTERNAL_WORKER_KEY");
  } catch (e) {
    await safeLog(env, { level:"error", msg: "Missing INTERNAL_WORKER_KEY or Secrets Store error", err: String(e), request_id });
    return new Response("Server misconfigured", { status: 500 });
  }

  if (!INTERNAL_KEY || token !== INTERNAL_KEY) {
    return new Response("Unauthorized", {
      status: 401,
      headers: { "www-authenticate": "Bearer realm=ytplsync" }
    });
  }

  // ---- Playlist ID from env/secret with CONFIG fallback ----
  let playlistId = "";
  try {
    playlistId = (await getSecret(env, "YT_PIVOT_PLAYLIST_ID")) || "";
  } catch {}
  if (!playlistId) playlistId = CONFIG.PLAYLIST_ID; // fallback
  if (!playlistId || playlistId.startsWith("<SET_")) {
    await safeLog(env, { level:"error", msg:"Missing YT_PIVOT_PLAYLIST_ID and no valid CONFIG fallback", request_id });
    return new Response("Server misconfigured", { status: 500 });
  }

  // ---- Parse body ----
  let body;
  try {
    body = await req.json();
  } catch {
    return new Response("Invalid JSON", { status: 400 });
  }
  const videoId = (body?.videoId || "").trim();
  const title   = (body?.title || "").trim();
  if (!videoId || !title) {
    return new Response("Missing videoId or title", { status: 400 });
  }

  // ---- Secrets for YouTube OAuth ----
  let CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN;
  try {
    CLIENT_ID     = await getSecret(env, "YOUTUBE_CLIENT_ID");
    CLIENT_SECRET = await getSecret(env, "YOUTUBE_CLIENT_SECRET");
    REFRESH_TOKEN = await getSecret(env, "YOUTUBE_REFRESH_TOKEN");
  } catch (e) {
    await safeLog(env, { level:"error", msg: "Failed to read YouTube secrets", err: String(e), request_id });
    return new Response("Server misconfigured", { status: 500 });
  }

  if (!CLIENT_ID || !CLIENT_SECRET || !REFRESH_TOKEN) {
    await safeLog(env, {
      level: "error",
      msg: "YouTube secrets missing",
      hasId: !!CLIENT_ID,
      hasSecret: !!CLIENT_SECRET,
      hasRefresh: !!REFRESH_TOKEN,
      request_id
    });
    return new Response("Server misconfigured", { status: 500 });
  }

  // ---- Access token ----
  // ---- Access token ----
  let accessToken;
  try {
    accessToken = await getAccessToken(CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN);
  } catch (e) {
    await safeLog(env, { level:"error", msg: "OAuth token exchange failed", err: String(e), request_id });
    return new Response("YouTube auth error", { status: 502 });
  }

  // ---- Fetch playlist items ----
  let items;
    try {
      items = await listAllPlaylistItems(accessToken, playlistId);
    } catch (e) {
      await safeLog(env, { level:"error", msg: "playlistItems.list failed", err: String(e), request_id });
      return new Response("YouTube API error", { status: 502 });
    }

  // Build sets for de-dupe
  const byVideo = new Map();
  const dups = [];
  for (const it of items) {
    const vid = it?.contentDetails?.videoId;
    if (!vid) continue;
    if (!byVideo.has(vid)) byVideo.set(vid, it);
    else dups.push(it);
  }

  // ---- Ensure present ----
  if (!byVideo.has(videoId)) {
   try {
      // Use the new helper (does its own token exchange + DEBUG A/B + retry + preview)
      await addToPlaylist(env, { videoId, playlistId, request_id });

      // Success breadcrumb (promoted)
      await safeLog(env, {
        level: "info",
        msg: `playlist sync added "${title}|${videoId}"`,
        promote: true,
        playlist_id: playlistId,
        request_id
      });

      // Refresh items after insert
      items = await listAllPlaylistItems(accessToken, playlistId);
      byVideo.clear(); dups.length = 0;
      for (const it of items) {
        const vid = it?.contentDetails?.videoId;
        if (!vid) continue;
        if (!byVideo.has(vid)) byVideo.set(vid, it);
        else dups.push(it);
      }
    } catch (e) {
      await safeLog(env, {
        level:"error",
        msg: "playlistItems.insert failed",
        videoId,
        status: e?.status ?? null,
        body_preview: e?.body_preview ?? "",
        request_id,
        promote: true
      });
      return okJson({ ok:false, error:"YouTube API error", status: e?.status ?? 0, body_preview: e?.body_preview ?? "" }, 502);
    }
  }

  // ---- Remove duplicates ----
  for (const dup of dups) {
    try {
      await deletePlaylistItem(accessToken, dup.id);
    } catch (e) {
      await safeLog(env, { level:"error", msg: "playlistItems.delete failed", playlistItemId: dup.id, err: String(e), request_id });
      // continue; not fatal for ordering
    }
  }

  // ---- Compute desired order ----
  const seen = new Set();
  const rows = [];
  for (const it of items) {
    const vid = it?.contentDetails?.videoId;
    if (!vid || seen.has(vid)) continue;
    seen.add(vid);
    const t = it?.snippet?.title || "";
    rows.push({
      itemId: it.id,
      videoId: vid,
      title: t,
      day: parseDay(t, CONFIG.TITLE_REGEX),
      publishedAt: toDate(it?.contentDetails?.videoPublishedAt) || new Date(0),
    });
  }

  const desired = [
    ...rows.filter(r => Number.isInteger(r.day)).sort((a, b) => a.day - b.day || a.publishedAt - b.publishedAt),
    ...rows.filter(r => !Number.isInteger(r.day)).sort((a, b) => a.publishedAt - b.publishedAt),
  ];

  // ---- Apply order ----
  let moved = 0;
  for (let i = 0; i < desired.length; i++) {
    const r = desired[i];
    try {
      await updatePlaylistPosition(accessToken, r.itemId, playlistId, r.videoId, i);
      moved++;
    } catch (e) {
      await safeLog(env, { level:"error", msg: "playlistItems.update failed", playlistItemId: r.itemId, position: i, err: String(e), request_id });
      return new Response("YouTube API error", { status: 502 });
    }
  }

  const duration_ms = Date.now() - started;

  await safeLog(env, {
    level: "info",
    promote: true,
    msg: "Playlist sync complete",
    ok: true,
    playlist_id: playlistId,
    video_id: videoId,
    removed_dups: dups.length,
    moved_count: moved,
    duration_ms,
    request_id,
  });

  return okJson({
    ok: true,
    playlistId,
    videoId,
    removedDuplicates: dups.length,
    movedCount: moved,
    duration_ms,
  });
}

// ------------- YouTube helpers -------------

function parseDay(title, regex) {
  const m = title?.match(regex);
  if (m && m[1]) {
    const n = parseInt(m[1], 10);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

function toDate(s) {
  try { return s ? new Date(s) : null; } catch { return null; }
}

// Pulls OAuth creds from secrets/env and exchanges for an access token
async function getAccessToken_env(env) {
  let CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN;
  try {
    CLIENT_ID     = await getSecret(env, "YOUTUBE_CLIENT_ID");
    CLIENT_SECRET = await getSecret(env, "YOUTUBE_CLIENT_SECRET");
    REFRESH_TOKEN = await getSecret(env, "YOUTUBE_REFRESH_TOKEN");
  } catch (e) {
    throw new Error(`YouTube secrets read failed: ${String(e)}`);
  }
  if (!CLIENT_ID || !CLIENT_SECRET || !REFRESH_TOKEN) {
    throw new Error("YouTube secrets missing");
  }
  return getAccessToken(CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN);
}

async function getAccessToken(clientId, clientSecret, refreshToken) {
  const res = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      client_id: clientId,
      client_secret: clientSecret,
      refresh_token: refreshToken,
      grant_type: "refresh_token",
    }),
  });
  if (!res.ok) throw new Error(`token exchange failed: ${res.status}`);
  const data = await res.json();
  return data.access_token;
}

async function listAllPlaylistItems(accessToken, playlistId) {
  const out = [];
  let pageToken = "";
  do {
    const u = new URL("https://www.googleapis.com/youtube/v3/playlistItems");
    u.searchParams.set("part", "snippet,contentDetails");
    u.searchParams.set("playlistId", playlistId);
    u.searchParams.set("maxResults", "50");
    if (pageToken) u.searchParams.set("pageToken", pageToken);

    const res = await fetch(u, { headers: { Authorization: `Bearer ${accessToken}` } });
    if (!res.ok) throw new Error(`playlistItems.list failed: ${res.status}`);
    const data = await res.json();
    out.push(...(data.items || []));
    pageToken = data.nextPageToken || "";
  } while (pageToken);
  return out;
}

async function insertIntoPlaylist(accessToken, playlistId, videoId) {
  const res = await fetch("https://www.googleapis.com/youtube/v3/playlistItems?part=snippet", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${accessToken}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({
      snippet: {
        playlistId,
        // omit position => append; we reorder after
        resourceId: { kind: "youtube#video", videoId },
      },
    }),
  });
  if (!res.ok) throw new Error(`playlistItems.insert failed: ${res.status}`);
}

async function deletePlaylistItem(accessToken, playlistItemId) {
  const u = new URL("https://www.googleapis.com/youtube/v3/playlistItems");
  u.searchParams.set("id", playlistItemId);
  const res = await fetch(u, {
    method: "DELETE",
    headers: { Authorization: `Bearer ${accessToken}` },
  });
  if (!res.ok) throw new Error(`playlistItems.delete failed: ${res.status}`);
}

async function updatePlaylistPosition(accessToken, playlistItemId, playlistId, videoId, position) {
  const res = await fetch("https://www.googleapis.com/youtube/v3/playlistItems?part=snippet", {
    method: "PUT",
    headers: {
      Authorization: `Bearer ${accessToken}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({
      id: playlistItemId,
      snippet: {
        playlistId,
        position,
        resourceId: { kind: "youtube#video", videoId },
      },
    }),
  });
  if (!res.ok) throw new Error(`playlistItems.update failed: ${res.status}`);
}

async function fetchWithRetry(url, init={}, opts={}) {
  const {
    attempts = 4,
    baseDelayMs = 400,
    maxDelayMs = 4000,
    request_id = shortId(),
  } = opts;

  let lastErr, lastPreview="";
  let delay = baseDelayMs;

  for (let i=1; i<=attempts; i++){
    let res;
    try {
      res = await fetch(url, init);
    } catch (e) {
      lastErr = e;
      if (i===attempts) break;
      await sleep(delay);
      delay = Math.min(maxDelayMs, delay*2);
      continue;
    }

    const ct = res.headers.get("content-type") || "";
    const text = await res.text().catch(()=> "");
    lastPreview = text.slice(0, 500);

    // Retry for 429 and 5xx
    if (res.status === 429 || (res.status >= 500 && res.status <= 599)) {
      if (i === attempts) return { ok:false, res, status:res.status, ct, body_preview:lastPreview };
      const ra = Number(res.headers.get("retry-after") || 0);
      const wait = ra > 0 ? ra*1000 : delay;
      await sleep(wait);
      delay = Math.min(maxDelayMs, delay*2);
      continue;
    }

    return { ok: res.ok, res, status: res.status, ct, body_preview: lastPreview };
  }

  return { ok:false, status: 0, ct: "", body_preview: String(lastErr || "") };
}
async function addToPlaylist(env, { videoId, playlistId, request_id }) {
  const DEBUG = envFlag(env, "YTPLSYNC_DEBUG", false);
  const VERBOSE = envFlag(env, "VERBOSE_YTPLSYNC", false);
  const rid = request_id || shortId();

  if (DEBUG) {
    await safeLog(env, {
      level: "info",
      msg: "DEBUG#A about-to-insert",
      request_id: rid,
      playlistId,
      videoId,
      promote: VERBOSE,
    });
  }

  const accessToken = await getAccessToken_env(env);
  const url = "https://www.googleapis.com/youtube/v3/playlistItems?part=snippet";
  const init = {
    method: "POST",
    headers: {
      Authorization: `Bearer ${accessToken}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({
      snippet: {
        playlistId,
        resourceId: { kind: "youtube#video", videoId },
      },
    }),
  };

  const r = await fetchWithRetry(url, init, { attempts: 4, request_id: rid });

  if (DEBUG) {
    await safeLog(env, {
      level: "info",
      msg: "DEBUG#B insert-response",
      request_id: rid,
      status: r.status,
      ok: r.ok,
      content_type: r.ct,
      body_preview: r.body_preview,
      promote: VERBOSE,
    });
  }

  if (!r.ok) {
    const err = new Error(`playlistItems.insert failed: ${r.status}`);
    err.body_preview = r.body_preview;
    err.status = r.status;
    throw err;
  }

  return { ok: true, request_id: rid };
}
