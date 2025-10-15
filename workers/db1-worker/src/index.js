//gr8r-db1-worker v1.5.0 ADDED: add update videos.record_modified when pre-signed URLs added/changed
//gr8r-db1-worker v1.4.9 FIXES: add correction for malformed pre-signed URLs
//gr8r-db1-worker v1.4.8 FIXES: add content lenght and type to POST /videos/get-presigned route
//gr8r-db1-worker v1.4.7 FIXES: fixt for channel.retry_count default
//gr8r-db1-worker v1.4.6 FIXES: removed extra get-presigned handler... chatGPT... RMEs
//gr8r-db1-worker v1.4.5 FIXES: if request missing URL grabs from db1.videos.r2url
//gr8r-db1-worker v1.4.4 FIXES: misses and errors from previous attempt
//gr8r-db1-worker v1.4.3 CHANGE: add logic for new R2access presign columns and route and refresh logic to the new r2access-presign-worker
//v1.4.2 gr8r-db1-worker CHANGE: add logic for new column in publishing table retry_count
//gr8r-db1-worker v1.4.1 CHANGE: updated Select statement to return existing platform_media_id and media_url - line 780
//gr8r-db1-worker v1.4.1 CHANGE: updates for orch-pub-worker and added platform_url column dev_index.js
//gr8r-db1-worker v1.4.0 CHANGE: new routes for orch-pub-worker promote to index.js
//gr8r-db1-worker v1.3.9 EDIT: added 'scheduling' as a publishing.status option
//gr8r-db1-worker v1.3.8 CHANGE: switch to safeLog approach and tighten logging by removing success messages
//gr8r-db1-worker v1.3.7 ADDED: 'Error' to the allowed video_status field
//gr8r-db1-worker v1.3.6 FIX: mass updates creating new phantom records
//gr8r-db1-worker v1.3.6 CHANGE: DELETE allowed by ID or Title rather than just title
//gr8r-db1-worker v1.3.5 CHANGE: allowed 'Post Ready' as a status in Videos table
//gr8r-db1-worker v1.3.5 ADD: "scheduled" as a status for publishing table
//gr8r-db1-worker v1.3.4 ADD: return video_id for videos
//gr8r-db1-worker v1.3.3 ADD: generic /db1/:table GET/POST router + DELETE by unique keys; retains videos behavior but significant code mods
//gr8r-db1-worker v1.3.2 MODIFY: replaced Secret Store process with new getSecret() and Grafana-worker with new log()
//gr8r-db1-worker v1.3.1 	
// ADD: import for secrets.js and grafana.js
// MODIFY: 6 spots calling grafana-worker with new grafana.js script and improved consistency approach
//gr8r-db1-worker v1.3.0 ADD: server-side data validation for status and video_type
//gr8r-db1-worker v1.2.9 ADD: support for force clearing certain database cells: scheduled_at, social_copy_hook, social_copy_body, social_copy_cta, and hashtags
//gr8r-db1-worker v1.2.8 modified origin for CORS checks - fighting with dev browser issues
//gr8r-db1-worker v1.2.7 modified GET to return All sorted by most recent record_modified
//gr8r-db1-worker v1.2.6
//Removing console logging lines that did not use "optional chaining" stmt.args.length and crashed the worker!
//gr8r-db1-worker v1.2.5
//Removing a ? value from the UPSERT binding
//gr8r-db1-worker v1.2.4
//Added one extra ? value to the UPSERT binding
//gr8r-db1-worker v1.2.3
//updating binding placeholders to 18 fields (chatGPT has issues counting it seems)
//added some extra console logging (binding stmt.args and length) for troubleshooting
//gr8r-db1-worker v1.2.2
//added console log to verify code version UPSERT with 17 fields
//added console log to display the bindings
//added console log to show Binding values length
//updated bind to use fullPayload values to pass something that DB1 can handle each time
//gr8r-db1-worker v1.2.1
//UPDATED: field values to null when first declared so that if not overwritten they wil be null and not throw the undefined error
//ADDED: key caching for this worker
//Removed: full header dump and console log that was added for troubleshooting auth

// new getSecret function module
import { getSecret } from "../../../lib/secrets.js";

// Grafana logging shared script
import { createLogger } from "../../../lib/grafana.js";

// Safe logger: always use this; caches underlying logger internally
let _logger;
const safeLog = async (env, entry) => {
  try {
    _logger = _logger || createLogger({ source: "gr8r-db1-worker" });
    await _logger(env, entry);
  } catch (e) {
    // Never throw from logging
    console.log('LOG_FAIL', entry?.service || 'unknown', e?.stack || e?.message || e);
  }
};

// --- Secrets-backed internal key (cached) ---
let _internalKeyCache = null;
async function getInternalKey(env) {
  if (_internalKeyCache) return _internalKeyCache;
  const raw = await getSecret(env, "DB1_INTERNAL_KEY");
  _internalKeyCache = (raw ?? "").toString().trim();
  return _internalKeyCache;
}

function getCorsHeaders(origin) {
  const allowedOrigins = [
    "https://admin.gr8r.com",
    "https://test.admin.gr8r.com",
    "http://localhost:5173",
    "https://dbadmin-react-site.pages.dev",
  ];

  const headers = {
    "Access-Control-Allow-Headers": "Authorization, Content-Type, Cf-Access-Jwt-Assertion",
    "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
    "Access-Control-Allow-Credentials": "true",
  };

  if (origin && allowedOrigins.includes(origin)) {
    headers["Access-Control-Allow-Origin"] = origin;
    headers["Vary"] = "Origin";
  }

  return headers;
}

function base64urlToUint8Array(base64url) {
  const base64 = base64url.replace(/-/g, "+").replace(/_/g, "/");
  const binary = atob(base64);
  return Uint8Array.from(binary, (c) => c.charCodeAt(0));
}

// Check for internal Bearer key auth using Secrets Store (cached)
async function checkInternalKey(request, env) {
  const authHeader = request.headers.get("Authorization");
  if (!authHeader?.startsWith("Bearer ")) return false;

  // DEFINE providedKey first
  const providedKey = authHeader.slice(7).trim(); // Skip "Bearer ", trim in case of newline/space

  const secret = await getInternalKey(env);
  return providedKey && secret && (providedKey === secret);
}

// v1.2.9 ADD: whitelist for editable columns and "clearable" subset
const EDITABLE_COLS = [
  "status", "video_type", "scheduled_at", "r2_url", "r2_transcript_url",
  "video_filename", "content_type", "file_size_bytes", "transcript_id",
  "planly_media_id", "social_copy_hook", "social_copy_body", "social_copy_cta", "hashtags"
];
// Only these may be force-cleared to NULL via `clears`
const CLEARABLE_COLS = new Set([
  "scheduled_at", "social_copy_hook", "social_copy_body", "social_copy_cta", "hashtags"
]);
// v1.2.10 ADD: server-side validation enums
const ALLOWED_STATUS = new Set([
  "Scheduled",
  "Post Ready",
  "Working",
  "Hold",
  "Pending Transcription",
  "Error",
]);

const ALLOWED_VIDEO_TYPE = new Set([
  "Pivot Year",
  "Newsletter",
  "Other",
  "Unlisted",
]);

// ADDED v1.3.3 — per-table configuration & small utilities

/**
 * For each table:
 * - table: D1 table name
 * - uniqueBy: array of columns forming the UPSERT key
 * - editableCols: columns allowed to be edited (besides timestamps)
 * - clearableCols: Set of editable columns that may be forced to NULL via body.clears[]
 * - enumValidators: { colName: Set([...]) } (optional)
 * - searchable: columns used by ?q= for LIKE search
 * - defaultSort: ORDER BY fallback
 * - filterMap: querystring -> "SQL_with_1_placeholder"
 */
const TABLES = {
  //videos table config
  videos: {
    table: "videos",
    uniqueBy: ["title"],
    editableCols: EDITABLE_COLS,        // reuse your existing constant
    clearableCols: CLEARABLE_COLS,      // reuse your existing constant
    enumValidators: {
      status: ALLOWED_STATUS,
      video_type: ALLOWED_VIDEO_TYPE,
    },
    searchable: ["title", "hashtags", "social_copy_hook", "social_copy_body", "social_copy_cta"],
    defaultSort: "record_modified DESC",
    filterMap: {
      status: "status = ?",
      type: "video_type = ?",
      title: "title = ?",
      since: "record_modified >= ?",
      before: "record_modified < ?",
    },
  },

  // Publishing table config
  publishing: {
    table: "Publishing",   // case-insensitive; keep matching your schema
    uniqueBy: ["video_id", "channel_key"],

    // editable fields for UPSERT (besides timestamps)
    editableCols: [
      "scheduled_at",
      "status",
      "platform_media_id",
      "platform_url",
      "last_error",
      "posted_at",
      "options_json",
      "retry_count"
    ],

    // Only fields that may be forced to NULL via clears[]
    // NOTE: posted_at is NOT clearable
    clearableCols: new Set([
      "last_error",
      "platform_media_id",
      "platform_url",
      "scheduled_at",
      "options_json"
    ]),

    // status enum per your schema comment
    enumValidators: {
      status: new Set(["pending", "queued", "scheduling", "scheduled", "posted", "error", "skipped"]),
      // pending =a waiting transcription and social copy, 
      // queued = transcription and SC complete - ready for worker scheduling - workers search this status
      // scheduling = set by worker while worker is actively processing and trying to schedule
      // scheduled = processed and scheduled by applicable worker/platform
      // posted = post has actually gone live
      // error = error state of some kind
      // skipped = used if a schedule created, transcription and SC complete, but post should no longer be scheduled or posted
      // channel_key validation will be dynamic against Channels table (see handler patch below)
    },

        // ADDED: default values to apply on INSERT when body omits the field
    insertDefaults: {                       
      retry_count: 0                        
    },                                      

    searchable: ["channel_key", "last_error", "options_json"],
    defaultSort: "record_modified DESC",
    filterMap: {
      video_id: "video_id = ?",
      channel_key: "channel_key = ?",
      status: "status = ?",
      since: "record_modified >= ?",
      before: "record_modified < ?"
    }
  },
  // Channels table config
  channels: {
    table: "Channels",             // case-insensitive in D1/SQLite
    uniqueBy: ["key"],

    editableCols: [
      "display_name",               // key is immutable; upserts use uniqueBy
      "json_defaults"
    ],

    clearableCols: new Set([
      // none; display_name shouldn't be NULL
    ]),

    // Search, sort, filters
    searchable: ["key", "display_name", "json_defaults"],
    defaultSort: "display_name ASC",
    filterMap: {
      id: "id = ?",
      key: "key = ?",
      since: "record_modified >= ?",
      before: "record_modified < ?"
    }
  },

  // Add new tables below by mirroring the shape above
  // transcripts: { ... },
  // social_posts: { ... },
};

function parsePathAsTable(pathname) {
  const m = /^\/db1\/([a-zA-Z0-9_]+)$/.exec(pathname);
  return m ? m[1] : null;
}

function coerceISOorNull(v) {
  if (!v) return null;
  const d = new Date(v);
  return isNaN(d.getTime()) ? null : d.toISOString();
}

function ensureEnums(tableCfg, body) {
  const enums = tableCfg.enumValidators || {};
  for (const [col, allowed] of Object.entries(enums)) {
    const val = body[col];
    if (val !== null && val !== undefined && !allowed.has(val)) {
      return { ok: false, message: `${col} must be one of: ${[...allowed].join(", ")}` };
    }
  }
  return { ok: true };
}
function pickUniqueForBody(tableCfg, body) {
  // If the request body has a non-null id, prefer id as the conflict target.
  if (body && body.id != null) return ["id"];
  // Otherwise fall back to the table’s configured uniqueBy (e.g., ["title"])
  return tableCfg.uniqueBy || ["title"];
}

function buildQueryParts(tableCfg, url) {
  const { searchParams } = url;
  const where = [];
  const binds = [];

  // mapped filters
  for (const [qs, clause] of Object.entries(tableCfg.filterMap || {})) {
    const raw = searchParams.get(qs);
    if (raw !== null && raw !== "") {
      const val = (qs === "since" || qs === "before") ? coerceISOorNull(raw) : raw;
      if (val !== null) { where.push(clause); binds.push(val); }
    }
  }

  // free-text search
  const q = searchParams.get("q");
  if (q && tableCfg.searchable?.length) {
    const like = `%${q}%`;
    where.push("(" + tableCfg.searchable.map(c => `${c} LIKE ?`).join(" OR ") + ")");
    for (let i = 0; i < tableCfg.searchable.length; i++) binds.push(like);
  }

  // sort: ?sort=column,ASC|DESC
  const sortParam = searchParams.get("sort");
  let orderBy = tableCfg.defaultSort || "record_modified DESC";
  if (sortParam) {
    const [col, dirRaw] = sortParam.split(",").map(s => (s || "").trim());
    const dir = (dirRaw || "DESC").toUpperCase();
    const safeDir = (dir === "ASC" || dir === "DESC") ? dir : "DESC";

    const sortable = new Set([
      ...(tableCfg.uniqueBy || []),
      ...(tableCfg.editableCols || []),
      ...(tableCfg.searchable || []),
      "record_created", "record_modified", "rowid",
    ]);
    if (col && sortable.has(col)) orderBy = `${col} ${safeDir}`;
  }

  // pagination
  const limit = Math.min(Math.max(parseInt(searchParams.get("limit") || "100", 10), 1), 500);
  const offset = Math.max(parseInt(searchParams.get("offset") || "0", 10), 0);

  return { where, binds, orderBy, limit, offset };
}
// ---- JSON + time helpers ----
function jsonResponse(status, obj) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

/** Returns ms remaining until ISO expiry; -1 if invalid/missing */
function msUntilExpiry(iso) {
  if (!iso) return -1;
  const t = Date.parse(iso);
  return Number.isFinite(t) ? (t - Date.now()) : -1;
}

/** Clamp TTL seconds to sane bounds */
function clampTtlSeconds(v, def = 1800, min = 300, max = 7200) {
  const n = Number(v);
  if (!Number.isFinite(n)) return def;
  return Math.min(Math.max(n, min), max);
}

// ---- D1 helpers for videos.presign fields ----
async function d1GetVideoById(env, id) {
  const r = await env.DB
    .prepare(`SELECT id, r2_url, r2presigned, r2presigned_expires_at FROM videos WHERE id = ? LIMIT 1`)
    .bind(id)
    .first();
  return r || null;
}

async function db1UpdatePresign(env, id, url, expiresIso) {
  const now = new Date().toISOString();
  await env.DB
    .prepare(`
      UPDATE videos
      SET r2presigned = ?, r2presigned_expires_at = ?, record_modified = ?
      WHERE id = ?
    `)
    .bind(url, expiresIso, now, id)
    .run();
}

function buildUpsertSQL(tableCfg, body) {
  const { table, editableCols, clearableCols } = tableCfg;
  const uniqueCols = pickUniqueForBody(tableCfg, body);   // <-- NEW
  const now = new Date().toISOString();

  // Insert columns include the chosen unique cols, plus editables and timestamps
  const allInsertCols = [...uniqueCols, ...(editableCols || []), "record_created", "record_modified"];
  const insertDefaults = tableCfg.insertDefaults || {};    
  const payload = {};
  for (const c of allInsertCols) {
    if (c === "record_created" || c === "record_modified") {
      payload[c] = now;
    } else if (body?.hasOwnProperty(c) && body[c] !== undefined) {
      payload[c] = body[c];
    } else if (Object.prototype.hasOwnProperty.call(insertDefaults, c)) { // ADDED
      payload[c] = insertDefaults[c];                                     // ADDED
    } else {
      payload[c] = null;
    }
  }

  const rawClears = Array.isArray(body?.clears) ? body.clears : [];
  const clears = rawClears.filter(k => typeof k === "string" && clearableCols?.has?.(k));

  // Only update columns that are explicitly provided or cleared
  const bodyCols = new Set(Object.keys(body || {}));
  const colsToUpdate = (editableCols || []).filter(col => bodyCols.has(col) || clears.includes(col));

  const updateAssignments = [
    ...colsToUpdate.map(col =>
      clears.includes(col)
        ? `${col} = NULL`
        : `${col} = COALESCE(excluded.${col}, ${table}.${col})`
    ),
    `record_modified = ?`,
  ].join(", ");

  const qMarks = allInsertCols.map(() => "?").join(", ");
  const conflictTarget = uniqueCols.join(",");             // <-- NEW

  const sql = `
    INSERT INTO ${table} (${allInsertCols.join(", ")})
    VALUES (${qMarks})
    ON CONFLICT(${conflictTarget}) DO UPDATE SET
      ${updateAssignments}
    RETURNING
      id,
      rowid AS _rid,
      CASE WHEN rowid = last_insert_rowid() THEN 'insert' ELSE 'update' END AS _action
  `;

  const binds = [...allInsertCols.map(c => payload[c]), now];
  return { sql, binds, now, clearsCount: rawClears.length };
}

// ADDED v1.3.3 — generic handlers used by all tables

async function handlePostUpsert(tableCfg, body, env, logMeta, origin) {
  // enum checks
  const ev = ensureEnums(tableCfg, body);
  if (!ev.ok) {
    await safeLog(env, {
      level: "warn",
      service: "db1-upsert",
      message: "Validation failed",
      meta: { ...logMeta, ok: false, status_code: 422, reason: "invalid_enum", duration_ms: Date.now() - (logMeta?.t0 ?? Date.now()) }
    });
    return new Response(JSON.stringify({ success: false, error: "Invalid enum", message: ev.message }), {
      status: 422, headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) },
    });
  }
  // ADDED v1.3.3 — publishing: validate channel_key dynamically
  if (tableCfg.table.toLowerCase() === "publishing") {
    const check = await assertChannelKeyExists(env, body?.channel_key);
    if (!check.ok) {
      await safeLog(env, {
        level: "warn",
        service: "db1-upsert",
        message: "Validation failed",
        meta: { ...logMeta, ok: false, status_code: 422, reason: "invalid_channel_key", duration_ms: Date.now() - (logMeta?.t0 ?? Date.now()) }
      });
      return new Response(JSON.stringify({ success: false, error: "Invalid channel_key", message: check.message }), {
        status: 422,
        headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) },
      });
    }

  }

  const { sql, binds, clearsCount } = buildUpsertSQL(tableCfg, body);
  const upsertRes = await env.DB.prepare(sql).bind(...binds).all();
  const action = upsertRes?.results?.[0]?._action || "unknown";

  // COMPAT: Preserve your old /db1/videos POST response shape to avoid breaking existing workers.
  if (tableCfg.table === "videos") {
    const now = new Date().toISOString();
    const video_id = upsertRes?.results?.[0]?.id ?? null;
    const db1Data = {
      title: body?.title ?? null,
      status: body?.status ?? null,
      video_type: body?.video_type ?? null,
      scheduled_at: body?.scheduled_at ?? null,
      r2_url: body?.r2_url ?? null,
      r2_transcript_url: body?.r2_transcript_url ?? null,
      video_filename: body?.video_filename ?? null,
      content_type: body?.content_type ?? null,
      file_size_bytes: body?.file_size_bytes ?? null,
      transcript_id: body?.transcript_id ?? null,
      planly_media_id: body?.planly_media_id ?? null,
      social_copy_hook: body?.social_copy_hook ?? null,
      social_copy_body: body?.social_copy_body ?? null,
      social_copy_cta: body?.social_copy_cta ?? null,
      hashtags: body?.hashtags ?? null,
      record_created: now,
      record_modified: now,
    };
    return new Response(JSON.stringify({ success: true, db1Data, video_id, action }), {
      status: 200, headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) },
    });
  }

  // Default generalized response for other tables
  return new Response(JSON.stringify({ success: true, action }), {
    status: 200, headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) },
  });
}
// ADDED v1.3.3 — dynamic FK-like check for Channels.key
async function assertChannelKeyExists(env, channel_key) {
  if (channel_key == null) return { ok: false, message: "channel_key is required" };
  const q = `SELECT 1 FROM Channels WHERE key = ? LIMIT 1`; // table name case-insensitive
  const r = await env.DB.prepare(q).bind(channel_key).all();
  const exists = Array.isArray(r?.results) && r.results.length > 0;
  return exists ? { ok: true } : { ok: false, message: `Unknown channel_key: ${channel_key}` };
}

async function handleGetQuery(tableCfg, url, env, logMeta, origin) {
  const { where, binds, orderBy, limit, offset } = buildQueryParts(tableCfg, url);
  let sql = `SELECT * FROM ${tableCfg.table}`;
  if (where.length) sql += ` WHERE ${where.join(" AND ")}`;
  sql += ` ORDER BY ${orderBy} LIMIT ? OFFSET ?`;

  const results = await env.DB.prepare(sql).bind(...binds, limit, offset).all();

  return new Response(JSON.stringify(results.results ?? [], null, 2), {
    headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) },
  });
}
// NEW: normalize delete keys from JSON body
function normalizeDeleteKeys(body) {
  const ids = new Set();
  const titles = new Set();

  if (Array.isArray(body?.keys)) {
    for (const k of body.keys) {
      if (k?.id != null && k.id !== "") ids.add(Number(k.id));
      if (typeof k?.title === "string" && k.title.trim()) titles.add(k.title.trim());
    }
  }
  if (Array.isArray(body?.ids)) {
    for (const v of body.ids) {
      if (v != null && v !== "") ids.add(Number(v));
    }
  }
  if (Array.isArray(body?.titles)) {
    for (const t of body.titles) {
      if (typeof t === "string" && t.trim()) titles.add(t.trim());
    }
  }

  return { ids: Array.from(ids), titles: Array.from(titles) };
}

// NEW: bulk delete by id and/or title with parameterized IN-lists
async function handleBulkDeleteByBody(tableCfg, request, env, logMeta, origin) {
  let body = {};

  try {
    body = await request.json();
  } catch (_) {
    await safeLog(env, {
      level: "warn",
      service: "db1-delete",
      message: "Bad JSON body",
      meta: { ...logMeta, ok: false, status_code: 400, reason: "json_parse_error", duration_ms: Date.now() - (logMeta?.t0 ?? Date.now()) }
    });
    return new Response(JSON.stringify({ success: false, error: "Expected JSON body" }), {
      status: 400, headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) },
    });
  }

  const { ids, titles } = normalizeDeleteKeys(body);
  if (!ids.length && !titles.length) {
    await safeLog(env, {
      level: "warn",
      service: "db1-delete",
      message: "Delete validation failed",
      meta: { ...logMeta, ok: false, status_code: 400, reason: "bulk_delete_keys_missing", duration_ms: Date.now() - (logMeta?.t0 ?? Date.now()) }
    });
    return new Response(JSON.stringify({ success: false, error: "Provide at least one id or title" }), {
      status: 400, headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) },
    });
  }

  let totalDeleted = 0;

  if (ids.length) {
    const qMarks = ids.map(() => "?").join(",");
    const sql = `DELETE FROM ${tableCfg.table} WHERE id IN (${qMarks})`;
    const r = await env.DB.prepare(sql).bind(...ids).run();
    totalDeleted += r.meta?.changes ?? 0;
  }

  if (titles.length) {
    const qMarks = titles.map(() => "?").join(",");
    const sql = `DELETE FROM ${tableCfg.table} WHERE title IN (${qMarks})`;
    const r = await env.DB.prepare(sql).bind(...titles).run();
    totalDeleted += r.meta?.changes ?? 0;
  }

  return new Response(JSON.stringify({ success: true, deleted: totalDeleted }), {
    status: 200, headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) },
  });
}

async function handleDeleteByUnique(tableCfg, url, env, logMeta, origin) {
  // Requires each uniqueBy key as a querystring param
  const binds = [];
  const where = [];
  for (const col of (tableCfg.uniqueBy || [])) {
    const val = url.searchParams.get(col);
    if (val === null || val === "") {
      await safeLog(env, {
        level: "warn",
        service: "db1-delete",
        message: "Delete validation failed",
        meta: { ...logMeta, ok: false, status_code: 400, reason: "missing_unique_key", duration_ms: Date.now() - (logMeta?.t0 ?? Date.now()) }

      });
      return new Response(JSON.stringify({ success: false, error: `Missing unique key '${col}'` }), {
        status: 400, headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) },
      });
    }
    where.push(`${col} = ?`);
    binds.push(val);
  }
  if (!where.length) {
    await safeLog(env, {
      level: "warn",
      service: "db1-delete",
      message: "Delete validation failed",
      meta: { ...logMeta, ok: false, status_code: 400, reason: "no_unique_keys", duration_ms: Date.now() - (logMeta?.t0 ?? Date.now()) }
    });
    return new Response(JSON.stringify({ success: false, error: "No unique keys provided" }), {
      status: 400, headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) },
    });
  }

  const sql = `DELETE FROM ${tableCfg.table} WHERE ${where.join(" AND ")}`;
  const res = await env.DB.prepare(sql).bind(...binds).run(); // run() => meta.changes

  return new Response(JSON.stringify({ success: true, deleted: res.meta?.changes ?? 0 }), {
    headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) },
  });
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const origin = request.headers.get("origin");
    // Trace seeds for consistent meta
    const request_id = crypto.randomUUID();
    const t0 = Date.now();

    console.log("[db1] incoming", request.method, url.pathname);

    //Handle CORS preflight requests dynamically
    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: getCorsHeaders(origin),
      });
    }

    // ADDED: Replace legacy internal header check with Authorization: Bearer
    const isInternal = await checkInternalKey(request, env);

    if (!isInternal) {
      // 🔒 JWT required for external requests
      const jwt = request.headers.get("Cf-Access-Jwt-Assertion");

      if (!jwt) {
        await safeLog(env, {
          level: "info",
          service: "request",
          message: "Request denied",
          meta: { request_id, route: url.pathname, method: request.method, origin, internal: false, ok: false, status_code: 401, reason: "missing_jwt", duration_ms: Date.now() - t0 }
        });
        return new Response("Missing JWT", {
          status: 401,
          headers: {
            "Content-Type": "text/plain",
            ...getCorsHeaders(origin),
          },
        });
      }


      const accessURL = "https://gr8r.cloudflareaccess.com";
      let keys;
      try {
        const verifyResponse = await fetch(`${accessURL}/cdn-cgi/access/certs`);
        const data = await verifyResponse.json();
        keys = data?.keys;
      } catch (err) {
        await safeLog(env, {
          level: "info",
          service: "request",
          message: "Request denied",
          meta: {
            request_id, route: url.pathname, method: request.method, origin,
            internal: false, ok: false, status_code: 401, reason: "access_certs_fetch_error",
            error: err?.message, duration_ms: Date.now() - t0
          }
        });
        return new Response("Unable to validate JWT (certs fetch error)", {
          status: 401, headers: { "Content-Type": "text/plain", ...getCorsHeaders(origin) }
        });
      }


      if (!keys || keys.length === 0) {
        await safeLog(env, {
          level: "info",
          service: "request",
          message: "Request denied",
          meta: { request_id, route: url.pathname, method: request.method, origin, internal: false, ok: false, status_code: 401, reason: "jwt_no_certs", duration_ms: Date.now() - t0 }

        });
        return new Response("Unable to validate JWT (no certs)", {
          status: 401,
          headers: {
            "Content-Type": "text/plain",
            ...getCorsHeaders(origin),
          },
        });
      }

      let valid = false;

      for (const jwk of keys) {
        try {
          const key = await crypto.subtle.importKey(
            "jwk",
            jwk,
            {
              name: "RSASSA-PKCS1-v1_5",
              hash: "SHA-256",
            },
            false,
            ["verify"]
          );

          const isValid = await crypto.subtle.verify(
            "RSASSA-PKCS1-v1_5",
            key,
            base64urlToUint8Array(jwt.split(".")[2]),
            new TextEncoder().encode(jwt.split(".")[0] + "." + jwt.split(".")[1])
          );

          if (isValid) {
            valid = true;
            break;
          }
        } catch (err) {
          // silently ignore bad certs
        }
      }

      if (!valid) {
        await safeLog(env, {
          level: "info",
          service: "request",
          message: "Request denied",
          meta: { request_id, route: url.pathname, method: request.method, origin, internal: false, ok: false, status_code: 401, reason: "invalid_jwt", duration_ms: Date.now() - t0 }
        });
        return new Response("Invalid JWT", {
          status: 401,
          headers: {
            "Content-Type": "text/plain",
            ...getCorsHeaders(origin),
          },
        });
      }
    }

    // Handle POST /channels/get-defaults or /db1/channels/get-defaults
    if (request.method === "POST" &&
    (url.pathname === "/channels/get-defaults" || url.pathname === "/db1/channels/get-defaults")) {
    const req_id = crypto.randomUUID(); const tStart = Date.now();
    try {
        const body = await request.json().catch(() => ({}));
        const channel_key = (body?.channel_key || "").trim();
        if (!channel_key) {
        await safeLog(env, { level: "warn", service: "db1-chdefaults", message: "Missing channel_key",
            meta: { request_id: req_id, route: url.pathname, method: request.method, ok: false, status_code: 400, duration_ms: Date.now() - tStart }});
        return new Response(JSON.stringify({ error: "channel_key required" }), {
            status: 400, headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) }
        });
        }
        const r = await env.DB.prepare(`SELECT json_defaults FROM Channels WHERE key = ? LIMIT 1`).bind(channel_key).all();
        const raw = r?.results?.[0]?.json_defaults || null;
        let json_defaults = null;
        try { json_defaults = raw ? JSON.parse(raw) : null; } catch { json_defaults = null; }
        return new Response(JSON.stringify({ json_defaults }), {
        status: 200, headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) }
        });
    } catch (err) {
        await safeLog(env, { level: "error", service: "db1-chdefaults", message: "Defaults fetch failed",
        meta: { request_id: req_id, route: url.pathname, method: request.method, ok: false, status_code: 500, error: err?.message, duration_ms: Date.now() - tStart }});
        return new Response(JSON.stringify({ error: "Server error" }), {
        status: 500, headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) }
        });
    }
    }
    // Handle POST /publishing/claim or /db1/publishing/claim
    if (request.method === "POST" &&
    (url.pathname === "/publishing/claim" || url.pathname === "/db1/publishing/claim")) {
    const req_id = crypto.randomUUID(); const tStart = Date.now();
    try {
        const body = await request.json().catch(() => ({}));
        const channel_key = (body?.channel_key || "").trim();
        const limit = Math.min(Math.max(parseInt(body?.limit || "5", 10), 1), 50);

        if (!channel_key) {
        await safeLog(env, { level: "warn", service: "db1-claim", message: "Missing channel_key",
            meta: { request_id: req_id, route: url.pathname, method: request.method, ok: false, status_code: 400, duration_ms: Date.now() - tStart }});
        return new Response(JSON.stringify({ error: "channel_key required" }), {
            status: 400, headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) }
        });
        }

        // 1) Flip queued → scheduling for up to :limit rows (atomic)
        const now = new Date().toISOString();
        const sqlUpdate = `
        WITH cte AS (
            SELECT id FROM Publishing
            WHERE status = 'queued' AND channel_key = ?
            ORDER BY COALESCE(scheduled_at, '9999-12-31T00:00:00Z') ASC, id
            LIMIT ?
        )
        UPDATE Publishing
        SET status = 'scheduling', record_modified = ?
        WHERE id IN (SELECT id FROM cte)
        RETURNING id, video_id, channel_key, scheduled_at, options_json
        `;
        const u = await env.DB.prepare(sqlUpdate).bind(channel_key, limit, now).all();
        const claimed = u?.results || [];
        if (!claimed.length) {
        return new Response(JSON.stringify({ rows: [] }), {
            status: 200, headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) }
        });
        }

        // 2) Join Videos to pull content fields needed by the worker
        const ids = claimed.map(r => r.id);
        const qMarks = ids.map(() => "?").join(",");
        const sqlJoin = `
        SELECT
            p.id                AS publishing_id,
            p.video_id          AS video_id,
            p.channel_key       AS channel_key,
            p.scheduled_at      AS scheduled_at,
            p.options_json      AS options_json,
            p.platform_media_id AS platform_media_id,   -- <-- add
            p.platform_url      AS platform_url,        -- <-- add (if you created the column)
            v.title             AS title,
            v.social_copy_hook  AS hook,
            v.social_copy_body  AS body,
            v.social_copy_cta   AS cta,
            v.hashtags          AS hashtags,
            v.r2_url            AS media_url
            FROM Publishing p
            JOIN videos v ON v.id = p.video_id
            WHERE p.id IN (${qMarks})
            ORDER BY p.scheduled_at IS NULL, p.scheduled_at ASC, p.id ASC
        `;
        const joined = await env.DB.prepare(sqlJoin).bind(...ids).all();

        await safeLog(env, { level: "info", service: "db1-claim", message: "claimed",
        meta: { request_id: req_id, channel_key, count: joined?.results?.length || 0, status_code: 200, ok: true, duration_ms: Date.now() - tStart }});

        return new Response(JSON.stringify({ rows: joined?.results || [] }), {
        status: 200, headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) }
        });
    } catch (err) {
        await safeLog(env, { level: "error", service: "db1-claim", message: "claim failed",
        meta: { request_id: req_id, route: url.pathname, method: request.method, ok: false, status_code: 500, error: err?.message, duration_ms: Date.now() - tStart }});
        return new Response(JSON.stringify({ error: "Server error" }), {
        status: 500, headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) }
        });
    }
    } 
    // Handle POST /publishing/update or /db1/publishing/update
    if (request.method === "POST" &&
    (url.pathname === "/publishing/update" || url.pathname === "/db1/publishing/update")) {
    const req_id = crypto.randomUUID(); const tStart = Date.now();
    try {
        const body = await request.json().catch(() => ({}));

        // Accept either style:
        //  A) { publishing_id, patch: { ...columns... } }
        //  B) { publishing_id, youtube_video_id?, youtube_url?, status?, posted_at?, scheduled_at?, last_error?, options_json? }
        const publishing_id = body?.publishing_id ?? body?.id;
        if (!publishing_id) {
        await safeLog(env, { level: "warn", service: "db1-patch", message: "Bad input (missing id)",
            meta: { request_id: req_id, ok: false, status_code: 400, duration_ms: Date.now() - tStart }});
        return new Response(JSON.stringify({ error: "publishing_id required" }), {
            status: 400, headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) }
        });
        }

        // Start from provided patch if present, else empty object
        const patch = (body && typeof body.patch === "object" && body.patch) ? { ...body.patch } : {};

        // Map flat YouTube-style fields into DB columns if they weren't provided in patch
        if (body.youtube_video_id && patch.platform_media_id == null) {
        patch.platform_media_id = body.youtube_video_id;
        }
        if (body.youtube_url && patch.platform_url == null) {
        patch.platform_url = body.youtube_url;
        }

        // Copy common fields if present and not already set in patch
        for (const k of ["status","posted_at","scheduled_at","last_error","options_json","platform_media_id","platform_url"]) {
        if (body[k] != null && patch[k] == null) patch[k] = body[k];
        }

        // Allowed columns to patch (now includes platform_url)
        const allowed = new Set(["status","platform_media_id","platform_url","last_error","posted_at","scheduled_at","options_json","retry_count"]);

        // Validate status enum if provided
        if (patch.status && !["pending","queued","scheduling","scheduled","posted","error","skipped"].includes(patch.status)) {
        return new Response(JSON.stringify({ error: "invalid status" }), {
            status: 422, headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) }
        });
        }

        // Build SETs and bind values
        const sets = [];
        const binds = [];
        for (const [k, v] of Object.entries(patch)) {
        if (!allowed.has(k)) continue;
        sets.push(`${k} = ?`);
        binds.push(v);
        }
        sets.push(`record_modified = ?`); binds.push(new Date().toISOString());

        if (sets.length === 1) { // only record_modified added → nothing to update
        return new Response(JSON.stringify({ success: true, updated: 0 }), {
            status: 200, headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) }
        });
        }

        const sql = `UPDATE Publishing SET ${sets.join(", ")} WHERE id = ?`;
        const r = await env.DB.prepare(sql).bind(...binds, publishing_id).run();

        return new Response(JSON.stringify({ success: true, updated: r.meta?.changes ?? 0 }), {
        status: 200, headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) }
        });
    } catch (err) {
        await safeLog(env, { level: "error", service: "db1-patch", message: "update failed",
        meta: { request_id: req_id, route: url.pathname, method: request.method, ok: false, status_code: 500, error: err?.message, duration_ms: Date.now() - tStart }});
        return new Response(JSON.stringify({ error: "Server error" }), {
        status: 500, headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) }
        });
    }
    }

    // Handle POST /publishing/list-scheduled or /db1/publishing/list-scheduled
    if (request.method === "POST" &&
    (url.pathname === "/publishing/list-scheduled" || url.pathname === "/db1/publishing/list-scheduled")) {
    const req_id = crypto.randomUUID(); const tStart = Date.now();
    try {
        const body = await request.json().catch(() => ({}));
        const channel_key = (body?.channel_key || "").trim();
        const limit = Math.min(Math.max(parseInt(body?.limit || "50", 10), 1), 200);

        if (!channel_key) {
        return new Response(JSON.stringify({ error: "channel_key required" }), {
            status: 400, headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) }
        });
        }

        const sql = `
        SELECT id AS publishing_id, platform_media_id
        FROM Publishing
        WHERE status = 'scheduled' AND channel_key = ? AND platform_media_id IS NOT NULL
        ORDER BY scheduled_at ASC, id ASC
        LIMIT ?
        `;
        const r = await env.DB.prepare(sql).bind(channel_key, limit).all();
        return new Response(JSON.stringify({ rows: r?.results || [] }), {
        status: 200, headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) }
        });
    } catch (err) {
        await safeLog(env, { level: "error", service: "db1-list", message: "list scheduled failed",
        meta: { request_id: req_id, route: url.pathname, method: request.method, ok: false, status_code: 500, error: err?.message, duration_ms: Date.now() - tStart }});
        return new Response(JSON.stringify({ error: "Server error" }), {
        status: 500, headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) }
        });
    }
    }

    // ---- Outbound: call presigner worker via Service Binding (secured with INTERNAL_WORKER_KEY) ----
    async function callPresigner(env, payload) {
      if (!env.R2PRESIGNER || typeof env.R2PRESIGNER.fetch !== "function") {
        return { ok: false, status: 500, error: "presigner_binding_missing" };
      }

      // INTERNAL_WORKER_KEY (Secrets Store) — used only outbound from DB1 -> presigner
      let interKey = "";
      try {
        interKey = (await getSecret(env, "INTERNAL_WORKER_KEY"))?.toString().trim() || "";
      } catch (_) {}
      if (!interKey) {
        return { ok: false, status: 500, error: "internal_worker_key_missing" };
      }

      const reqId = crypto.randomUUID().slice(0, 8);

      // Path matters; host is ignored for service bindings
      const res = await env.R2PRESIGNER.fetch("https://presign.internal/presign", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${interKey}`,
          "X-Request-ID": reqId,
        },
        body: JSON.stringify(payload),
      });

      const text = await res.text().catch(() => "");
      if (!res.ok) {
        return { ok: false, status: res.status, error: text.slice(0, 200) };
      }

      let json = null;
      try { json = JSON.parse(text); } catch { /* noop */ }

      const url = json?.url || null;
      const expires_at = json?.expires_at || null; // ISO8601 UTC
      if (!url || !expires_at) {
        return { ok: false, status: 502, error: "presigner_bad_response" };
      }

      return { ok: true, status: 200, url, expires_at };
    }

        // Handle POST /videos/get-presigned or /db1/videos/get-presigned
    if (request.method === "POST" &&
       (url.pathname === "/videos/get-presigned" || url.pathname === "/db1/videos/get-presigned")) {
      const req_id = crypto.randomUUID(); const tStart = Date.now();
      try {
        // internal-only
        const isOk = await checkInternalKey(request, env);
        if (!isOk) {
          await safeLog(env, { level: "info", service: "db1-presign", message: "Unauthorized",
            meta: { request_id: req_id, route: url.pathname, method: request.method, ok: false, status_code: 401, duration_ms: Date.now() - tStart }});
          return new Response(JSON.stringify({ ok:false, error:"unauthorized" }), {
            status: 401, headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) }
          });
        }

        const body = await request.json().catch(() => ({}));
        const video_id  = Number(body?.video_id || 0) || null;
        const requester = String(body?.requester || "unknown");
        const reason    = String(body?.reason || "");
        const ttl       = clampTtlSeconds(body?.ttl_seconds, 1800); // 30m default
        let   r2_url    = (typeof body?.r2_url === "string" ? body.r2_url.trim() : "");

        if (!video_id) {
          return new Response(JSON.stringify({ ok:false, error:"video_id_required" }), {
            status: 400, headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) }
          });
        }

        // pull from DB if caller didn't supply r2_url
        if (!r2_url) {
          const row = await d1GetVideoById(env, video_id);
          if (!row) {
            return new Response(JSON.stringify({ ok:false, error:"video_not_found" }), {
              status: 404, headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) }
            });
          }
          r2_url = (row.r2_url || "").trim();
          if (!r2_url) {
            return new Response(JSON.stringify({ ok:false, error:"missing_r2_url_on_video" }), {
              status: 400, headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) }
            });
          }
        }

        if (!r2_url) {
          return new Response(JSON.stringify({ ok:false, error:"missing_r2_url_after_resolve" }), {
            status: 400, headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) }
          });
        }

        // call presigner via service binding (INTERNAL_WORKER_KEY)
        const pres = await callPresigner(env, {
          r2_url,
          ttl_seconds: ttl,
          requester,
          reason,
          video_id,
        });

        if (!pres.ok) {
          await safeLog(env, {
            level: "error",
            service: "db1-presign",
            message: "presign failed",
            meta: {
              request_id: req_id, video_id, requester, reason,
              error: pres.error, status_code: pres.status || 502, ok: false,
              duration_ms: Date.now() - tStart
            }
          });
          return new Response(JSON.stringify({ ok:false, error:"presign_failed", detail: JSON.stringify({ ok:false, error: pres.error }) }), {
            status: 502, headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) }
          });
        }
        // --- HOTFIX: normalize double-encoded slashes in X-Amz-Credential (e.g., %252F -> %2F)
        let presignedUrl = pres.url;
        if (/%252F/i.test(presignedUrl)) {
          const fixed = presignedUrl.replace(/%252F/g, "%2F");
          await safeLog(env, {
            level: "warn",
            service: "db1-presign",
            message: "normalized double-encoded credential slashes in presigned URL",
            meta: { request_id: req_id, video_id, status_code: 200, ok: true }
          });
          presignedUrl = fixed;
        }
        // (optional) also normalize any accidental %252C -> %2C
        if (/%252C/i.test(presignedUrl)) {
          presignedUrl = presignedUrl.replace(/%252C/g, "%2C");
        }

        // persist to videos
        const expiresIso = pres.expires_at;
        await db1UpdatePresign(env, video_id, presignedUrl, expiresIso);

        const remaining = msUntilExpiry(expiresIso);

        await safeLog(env, {
          level: "info",
          service: "db1-presign",
          message: "presign ok",
          meta: {
            request_id: req_id, video_id, requester, reason,
            presign_expires_in_ms: remaining, ok: true, status_code: 200,
            duration_ms: Date.now() - tStart
          }
        });

        // fetch content_type and file_size_bytes for the response contract
        const metaRow = await env.DB
          .prepare(`SELECT content_type, file_size_bytes FROM videos WHERE id = ? LIMIT 1`)
          .bind(video_id)
          .first();

        return new Response(JSON.stringify({
          ok: true,
          video_id,
          // Return an OBJECT with url + sizes/types (what the youtube worker expects)
          r2presigned: {
            url: presignedUrl,
            contentType: metaRow?.content_type || null,
            contentLength: Number(metaRow?.file_size_bytes || 0)
          },
          r2presigned_expires_at: expiresIso,
          presign_expires_in_ms: remaining
        }), {
          status: 200,
          headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) }
        });

      } catch (err) {
        await safeLog(env, {
          level: "error",
          service: "db1-presign",
          message: "server error",
          meta: { request_id: req_id, ok: false, status_code: 500, error: err?.message, duration_ms: Date.now() - tStart }
        });
        return new Response(JSON.stringify({ ok:false, error:"server_error" }), {
          status: 500, headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) }
        });
      }
    }

    //UPSERT code includes time/date stamping for record_created and/or record_modified
    // ADDED v1.3.3 — generic /db1/:table router for GET/POST/DELETE (with error logging + t0 for duration)
    const tableKey = parsePathAsTable(url.pathname);
    if (tableKey) {
      const tableCfg = TABLES[tableKey];
      if (!tableCfg) {
        await safeLog(env, {
          level: "warn",
          service: "request",
          message: "Bad request",
          meta: { request_id, route: url.pathname, method: request.method, origin, internal: isInternal, ok: false, status_code: 400, reason: "unknown_table", duration_ms: Date.now() - t0 }
        });
        return new Response(JSON.stringify({ success: false, error: `Unknown table '${tableKey}'` }), {
          status: 400,
          headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) },
        });
      }

      if (request.method === "GET") {
        try {
          return await handleGetQuery(
            tableCfg,
            url,
            env,
            { request_id, route: url.pathname, method: request.method, origin, t0 },
            origin
          );
        } catch (err) {
          await safeLog(env, {
            level: "error",
            service: "db1-fetch",
            message: `GET ${tableCfg.table} failed`,
            meta: {
              request_id, route: url.pathname, method: request.method, origin,
              ok: false, status_code: 500, error: err?.message, stack: err?.stack,

              duration_ms: Date.now() - t0
            }
          });

          return new Response(JSON.stringify({ success: false, error: err?.message }), {
            status: 500,
            headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) },
          });
        }
      }

      if (request.method === "POST") {
        try {
          const body = await request.json();
          return await handlePostUpsert(
            tableCfg,
            body,
            env,
            { request_id, route: url.pathname, method: request.method, origin, t0 },
            origin
          );
        } catch (err) {
          await safeLog(env, {
            level: "warn",
            service: "db1-upsert",
            message: `Upsert failed for ${tableCfg.table}`,
            meta: {
              request_id, route: url.pathname, method: request.method, origin,
              ok: false, status_code: 400, error: err?.message,
              duration_ms: Date.now() - t0
            }
          });

          return new Response(JSON.stringify({ success: false, error: "Upsert Error", message: err?.message }), {
            status: 400,
            headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) },
          });
        }
      }

      if (request.method === "DELETE") {
        try {
          // Try JSON body bulk-delete first (ids/titles/keys)
          const ct = request.headers.get("Content-Type") || "";
          if (ct.includes("application/json")) {
            // Peek body safely; if invalid JSON, helper will return 400
            return await handleBulkDeleteByBody(
              tableCfg,
              request,
              env,
              { request_id, route: url.pathname, method: request.method, origin, t0 },
              origin
            );
          }

          // Fallback: legacy querystring unique delete (e.g., ?title=...)
          return await handleDeleteByUnique(
            tableCfg,
            url,
            env,
            { request_id, route: url.pathname, method: request.method, origin, t0 },
            origin
          );
        } catch (err) {
          await safeLog(env, {
            level: "error",
            service: "db1-delete",
            message: `DELETE ${tableCfg.table} failed`,
            meta: {
              request_id, route: url.pathname, method: request.method, origin,
              ok: false, status_code: 500, error: err?.message, stack: err?.stack,
              duration_ms: Date.now() - t0
            }
          });
          return new Response(JSON.stringify({ success: false, error: err?.message }), {
            status: 500,
            headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) },
          });
        }
      }

      return new Response("Method Not Allowed", { status: 405, headers: getCorsHeaders(origin) });
    }

    return new Response("Not found", { status: 404, headers: getCorsHeaders(origin) });

  },
};
