// gr8r-ytplsync-worker v0.2.1
// CHANGE: Add CONFIG.LOG_SUCCESS_TO_GRAFANA toggle
//   - When true: successes log to Grafana (level=info) + console
//   - When false: successes log to console only
// PRIOR CHANGES: align secret names; static CONFIG for playlist/regex; errors -> Grafana

import { createLogger } from "../../../lib/grafana.js";
import { getSecret }   from "../../../lib/secrets.js";

const SOURCE = "gr8r-ytplsync-worker";

// ---------- STATIC CONFIG (single edit point) ----------
const CONFIG = {
  // Toggle success logs to Grafana (keep console success either way)
  LOG_SUCCESS_TO_GRAFANA: true, // set to false after testing

  // Set your Pivot Year playlist ID here (e.g., "PLxxxxxxxxxxxxxxxx")
  PLAYLIST_ID: "PLEIW-sxRCY_1h01CHXwu6DOdeB3F8OWJt",

  // Regex must capture the numeric day in group 1
  TITLE_REGEX: /Pivot\s*Year\s*Day\s*(\d+)/i,
};
// ------------------------------------------------------

const _logger = createLogger({ source: SOURCE });

async function logError(env, entry) {
  // Only errors always go to Grafana
  try {
    await _logger(env, { level: "error", ...entry });
  } catch {
    console.error(JSON.stringify({ source: SOURCE, level: "error", msg: "Grafana log failed", entry }));
  }
}

async function logSuccess(env, entry) {
  // Success: console always; Grafana only when enabled
  console.log(JSON.stringify({ source: SOURCE, level: "info", ...entry }));
  if (!CONFIG.LOG_SUCCESS_TO_GRAFANA) return;
  try {
    await _logger(env, { level: "info", ...entry });
  } catch {
    // don't escalate on success-log failure
    console.warn(JSON.stringify({ source: SOURCE, level: "warn", msg: "Grafana success log failed" }));
  }
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

  // ---- Auth (internal bearer) ----
  const auth = req.headers.get("authorization") || "";
  const token = auth.startsWith("Bearer ") ? auth.slice(7) : "";
  let INTERNAL_KEY;
  try {
    INTERNAL_KEY = await getSecret(env, "INTERNAL_WORKER_KEY");
  } catch (e) {
    await logError(env, { msg: "Missing INTERNAL_WORKER_KEY or Secrets Store error", err: String(e) });
    return new Response("Server misconfigured", { status: 500 });
  }

  if (!INTERNAL_KEY || token !== INTERNAL_KEY) {
    return new Response("Unauthorized", { status: 401 });
  }

  // ---- Config sanity ----
  if (!CONFIG.PLAYLIST_ID || CONFIG.PLAYLIST_ID.startsWith("<SET_")) {
    await logError(env, { msg: "PLAYLIST_ID not configured in code CONFIG" });
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
    await logError(env, { msg: "Failed to read YouTube secrets", err: String(e) });
    return new Response("Server misconfigured", { status: 500 });
  }
  if (!CLIENT_ID || !CLIENT_SECRET || !REFRESH_TOKEN) {
    await logError(env, { msg: "YouTube secrets missing", hasId: !!CLIENT_ID, hasSecret: !!CLIENT_SECRET, hasRefresh: !!REFRESH_TOKEN });
    return new Response("Server misconfigured", { status: 500 });
  }

  // ---- Access token ----
  let accessToken;
  try {
    accessToken = await getAccessToken(CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN);
  } catch (e) {
    await logError(env, { msg: "OAuth token exchange failed", err: String(e) });
    return new Response("YouTube auth error", { status: 502 });
  }

  // ---- Fetch playlist items ----
  let items;
  try {
    items = await listAllPlaylistItems(accessToken, CONFIG.PLAYLIST_ID);
  } catch (e) {
    await logError(env, { msg: "playlistItems.list failed", err: String(e) });
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
      await insertIntoPlaylist(accessToken, CONFIG.PLAYLIST_ID, videoId);
      // refresh items after insert
      items = await listAllPlaylistItems(accessToken, CONFIG.PLAYLIST_ID);
      byVideo.clear(); dups.length = 0;
      for (const it of items) {
        const vid = it?.contentDetails?.videoId;
        if (!vid) continue;
        if (!byVideo.has(vid)) byVideo.set(vid, it);
        else dups.push(it);
      }
    } catch (e) {
      await logError(env, { msg: "playlistItems.insert failed", videoId, err: String(e) });
      return new Response("YouTube API error", { status: 502 });
    }
  }

  // ---- Remove duplicates ----
  for (const dup of dups) {
    try {
      await deletePlaylistItem(accessToken, dup.id);
    } catch (e) {
      await logError(env, { msg: "playlistItems.delete failed", playlistItemId: dup.id, err: String(e) });
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
      await updatePlaylistPosition(accessToken, r.itemId, CONFIG.PLAYLIST_ID, r.videoId, i);
      moved++;
    } catch (e) {
      await logError(env, { msg: "playlistItems.update failed", playlistItemId: r.itemId, position: i, err: String(e) });
      return new Response("YouTube API error", { status: 502 });
    }
  }

  const duration_ms = Date.now() - started;

  await logSuccess(env, {
    msg: "Playlist sync complete",
    ok: true,
    playlist_id: CONFIG.PLAYLIST_ID,
    video_id: videoId,
    removed_dups: dups.length,
    moved_count: moved,
    duration_ms,
  });

  return okJson({
    ok: true,
    playlistId: CONFIG.PLAYLIST_ID,
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
