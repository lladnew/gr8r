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

// new Grafana logging shared script
import { createLogger } from "../../../lib/grafana.js";
const log = createLogger({ source: "gr8r-db1-worker" });

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
  "status","video_type","scheduled_at","r2_url","r2_transcript_url",
  "video_filename","content_type","file_size_bytes","transcript_id",
  "planly_media_id","social_copy_hook","social_copy_body","social_copy_cta","hashtags"
];
// Only these may be force-cleared to NULL via `clears`
const CLEARABLE_COLS = new Set([
  "scheduled_at","social_copy_hook","social_copy_body","social_copy_cta","hashtags"
]);
// v1.2.10 ADD: server-side validation enums
const ALLOWED_STATUS = new Set([
  "Scheduled",
  "Post Ready",
  "Working",
  "Hold",
  "Pending Transcription",
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
    searchable: ["title","hashtags","social_copy_hook","social_copy_body","social_copy_cta"],
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
    uniqueBy: ["video_id","channel_key"],

    // editable fields for UPSERT (besides timestamps)
    editableCols: [
      "scheduled_at",
      "status",
      "platform_media_id",
      "last_error",
      "posted_at",
      "options_json"
    ],

    // Only fields that may be forced to NULL via clears[]
    // NOTE: posted_at is NOT clearable
    clearableCols: new Set([
      "last_error",
      "platform_media_id",
      "scheduled_at",
      "options_json"
    ]),

    // status enum per your schema comment
    enumValidators: {
      status: new Set(["pending","queued","scheduled","posted","failed","skipped"]),
      // channel_key validation will be dynamic against Channels table (see handler patch below)
    },

    searchable: ["channel_key","last_error","options_json"],
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
      "display_name"               // key is immutable; upserts use uniqueBy
    ],

    clearableCols: new Set([
      // none; display_name shouldn't be NULL
    ]),

    // Search, sort, filters
    searchable: ["key","display_name"],
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
      return { ok:false, message: `${col} must be one of: ${[...allowed].join(", ")}` };
    }
  }
  return { ok:true };
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
    for (let i=0;i<tableCfg.searchable.length;i++) binds.push(like);
  }

  // sort: ?sort=column,ASC|DESC
  const sortParam = searchParams.get("sort");
  let orderBy = tableCfg.defaultSort || "record_modified DESC";
  if (sortParam) {
    const [col, dirRaw] = sortParam.split(",").map(s => (s||"").trim());
    const dir = (dirRaw || "DESC").toUpperCase();
    const safeDir = (dir === "ASC" || dir === "DESC") ? dir : "DESC";

    const sortable = new Set([
      ...(tableCfg.uniqueBy || []),
      ...(tableCfg.editableCols || []),
      ...(tableCfg.searchable || []),
      "record_created","record_modified","rowid",
    ]);
    if (col && sortable.has(col)) orderBy = `${col} ${safeDir}`;
  }

  // pagination
  const limit = Math.min(Math.max(parseInt(searchParams.get("limit") || "100", 10), 1), 500);
  const offset = Math.max(parseInt(searchParams.get("offset") || "0", 10), 0);

  return { where, binds, orderBy, limit, offset };
}

function buildUpsertSQL(tableCfg, body) {
  const { table, editableCols, clearableCols } = tableCfg;
  const uniqueCols = pickUniqueForBody(tableCfg, body);   // <-- NEW
  const now = new Date().toISOString();

  // Insert columns include the chosen unique cols, plus editables and timestamps
  const allInsertCols = [...uniqueCols, ...(editableCols || []), "record_created","record_modified"];
  const payload = {};
  for (const c of allInsertCols) {
    payload[c] = (c === "record_created" || c === "record_modified") ? now : (body?.[c] ?? null);
  }

  const rawClears = Array.isArray(body?.clears) ? body.clears : [];
  const clears = rawClears.filter(k => typeof k === "string" && clearableCols?.has?.(k));

  const updateAssignments = [
    ...(editableCols || []).map(col => clears.includes(col)
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
    return new Response(JSON.stringify({ success:false, error:"Invalid enum", message: ev.message }), {
      status: 422, headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) },
    });
  }
  // ADDED v1.3.3 — publishing: validate channel_key dynamically
  if (tableCfg.table.toLowerCase() === "publishing") {
    const check = await assertChannelKeyExists(env, body?.channel_key);
    if (!check.ok) {
      return new Response(JSON.stringify({ success:false, error:"Invalid channel_key", message: check.message }), {
        status: 422,
        headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) },
      });
    }
  
  }

  const { sql, binds, clearsCount } = buildUpsertSQL(tableCfg, body);
  const upsertRes = await env.DB.prepare(sql).bind(...binds).all();
  const action = upsertRes?.results?.[0]?._action || "unknown";

  await log(env, {
	level: "info",
	service: "db1-upsert",
	message: `Upserted ${tableCfg.table}`,
	meta: {
		...logMeta,
		ok: true,
		status: 200,
		table: tableCfg.table,
		unique: Object.fromEntries((tableCfg.uniqueBy || []).map(k => [k, body?.[k] ?? null])),
		title: body?.title ?? null,
		video_type: body?.video_type ?? null,
		scheduled_at: body?.scheduled_at ?? null,
		clears_count: clearsCount,
		action,
		// ADDED v1.3.3 explicit duration
		duration_ms: Date.now() - (logMeta?.t0 ?? Date.now())
	}
	});

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
  if (channel_key == null) return { ok:false, message: "channel_key is required" };
  const q = `SELECT 1 FROM Channels WHERE key = ? LIMIT 1`; // table name case-insensitive
  const r = await env.DB.prepare(q).bind(channel_key).all();
  const exists = Array.isArray(r?.results) && r.results.length > 0;
  return exists ? { ok:true } : { ok:false, message: `Unknown channel_key: ${channel_key}` };
}

async function handleGetQuery(tableCfg, url, env, logMeta, origin) {
  const { where, binds, orderBy, limit, offset } = buildQueryParts(tableCfg, url);
  let sql = `SELECT * FROM ${tableCfg.table}`;
  if (where.length) sql += ` WHERE ${where.join(" AND ")}`;
  sql += ` ORDER BY ${orderBy} LIMIT ? OFFSET ?`;

  const results = await env.DB.prepare(sql).bind(...binds, limit, offset).all();

  await log(env, {
	level: "debug",
	service: "db1-fetch",
	message: `GET ${tableCfg.table} ok`,
	meta: {
		...logMeta,
		ok: true,
		status: 200,
		table: tableCfg.table,
		filters_applied: where.length,
		limit,
		offset,
		count: results.results?.length ?? 0,
		// ADDED v1.3.3 explicit duration
		duration_ms: Date.now() - (logMeta?.t0 ?? Date.now())
		}
	});

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
    return new Response(JSON.stringify({ success:false, error:"Expected JSON body" }), {
      status: 400, headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) },
    });
  }

  const { ids, titles } = normalizeDeleteKeys(body);
  if (!ids.length && !titles.length) {
    return new Response(JSON.stringify({ success:false, error:"Provide at least one id or title" }), {
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

  await log(env, {
    level: "info",
    service: "db1-delete",
    message: `DELETE ${tableCfg.table} (bulk by body)`,
    meta: {
      ...logMeta,
      ok: true,
      status: 200,
      table: tableCfg.table,
      ids: ids.length,
      titles: titles.length,
      deleted: totalDeleted,
      duration_ms: Date.now() - (logMeta?.t0 ?? Date.now()),
    }
  });

  return new Response(JSON.stringify({ success:true, deleted: totalDeleted }), {
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
      return new Response(JSON.stringify({ success:false, error:`Missing unique key '${col}'` }), {
        status: 400, headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) },
      });
    }
    where.push(`${col} = ?`);
    binds.push(val);
  }
  if (!where.length) {
    return new Response(JSON.stringify({ success:false, error:"No unique keys provided" }), {
      status: 400, headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) },
    });
  }

  const sql = `DELETE FROM ${tableCfg.table} WHERE ${where.join(" AND ")}`;
  const res = await env.DB.prepare(sql).bind(...binds).run(); // run() => meta.changes

  await log(env, {
	level: "info",
	service: "db1-delete",
	message: `DELETE ${tableCfg.table}`,
	meta: {
		...logMeta,
		ok: true,
		status: 200,
		table: tableCfg.table,
		unique: Object.fromEntries((tableCfg.uniqueBy || []).map((k,i)=>[k,binds[i]])),
		changes: res.meta?.changes ?? null,
		// ADDED v1.3.3 explicit duration
		duration_ms: Date.now() - (logMeta?.t0 ?? Date.now())
		}
	});

  return new Response(JSON.stringify({ success:true, deleted: res.meta?.changes ?? 0 }), {
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

		await log(env, {
			level: "debug",
			service: "request",
			message: "Incoming request",
			meta: {
				request_id, route: url.pathname, method: request.method,
				origin, ok: true
			}
		});		

		//Handle CORS preflight requests dynamically
		if (request.method === "OPTIONS") {
			return new Response(null, {
				status: 204,
				headers: getCorsHeaders(origin),
			});
		}
		
		// ADDED: Replace legacy internal header check with Authorization: Bearer
		const isInternal = await checkInternalKey(request, env);

		await log(env, {
		level: "debug",
		service: "auth",
		message: "Caller identity checked",
		meta: {
			request_id, route: url.pathname, method: request.method,
			origin, internal: isInternal, ok: true
			}
		});	

		if (!isInternal) {
		// 🔒 JWT required for external requests
		const jwt = request.headers.get("Cf-Access-Jwt-Assertion");

		if (!jwt) {
			return new Response("Missing JWT", {
			status: 401,
			headers: {
				"Content-Type": "text/plain",
				...getCorsHeaders(origin),
			},
			});
		}

		const accessURL = "https://gr8r.cloudflareaccess.com";
		const verifyResponse = await fetch(`${accessURL}/cdn-cgi/access/certs`);
		const { keys } = await verifyResponse.json();

		if (!keys || keys.length === 0) {
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
				await log(env, {
					level: "info",
					service: "auth",
					message: "Request denied",
					meta: {
						request_id, route: url.pathname, method: request.method,
						origin, ok: false, status: 401, reason: "invalid_jwt"
					}
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

//UPSERT code includes time/date stamping for record_created and/or record_modified

// ADDED v1.3.3 — generic /db1/:table router for GET/POST/DELETE (with error logging + t0 for duration)
const tableKey = parsePathAsTable(url.pathname);
if (tableKey) {
  const tableCfg = TABLES[tableKey];
  if (!tableCfg) {
    return new Response(JSON.stringify({ success:false, error:`Unknown table '${tableKey}'` }), {
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
      await log(env, {
        level: "error",
        service: "db1-fetch",
        message: `GET ${tableCfg.table} failed`,
        meta: {
          request_id, route: url.pathname, method: request.method, origin,
          ok: false, status: 500, error: err?.message, stack: err?.stack,
          duration_ms: Date.now() - t0
        }
      });
      return new Response(JSON.stringify({ success:false, error: err?.message }), {
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
      await log(env, {
        level: "warn",
        service: "db1-upsert",
        message: `Upsert failed for ${tableCfg.table}`,
        meta: {
          request_id, route: url.pathname, method: request.method, origin,
          ok: false, status: 400, error: err?.message,
          duration_ms: Date.now() - t0
        }
      });
      return new Response(JSON.stringify({ success:false, error:"Upsert Error", message: err?.message }), {
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
      await log(env, {
        level: "error",
        service: "db1-delete",
        message: `DELETE ${tableCfg.table} failed`,
        meta: {
          request_id, route: url.pathname, method: request.method, origin,
          ok: false, status: 500, error: err?.message, stack: err?.stack,
          duration_ms: Date.now() - t0
        }
      });
      return new Response(JSON.stringify({ success:false, error: err?.message }), {
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
