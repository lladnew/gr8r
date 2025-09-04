//gr8r-db1-worker v1.4.0 	
// ADD: import for secrets.js and grafana.js
// MODIFY: 6 spots calling grafana-worker with new grafana.js script and improved consistency approach
//gr8r-db1-worker v1.3.0 ADD: server-side validation 
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

// new Grafan logging shared script
import { createLogger } from "../../../lib/grafana.js";
const log = createLogger({ source: "gr8r-db1-worker" });


function getCorsHeaders(origin) {
	const allowedOrigins = [
		"https://admin.gr8r.com",
		"https://test.admin.gr8r.com",
		"http://localhost:5173",
		"https://dbadmin-react-site.pages.dev",
	];

	const headers = {
		"Access-Control-Allow-Headers": "Authorization, Content-Type",
		"Access-Control-Allow-Methods": "GET, POST, OPTIONS",
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

let cachedInternalKey = null;

// Check for internal Bearer key auth and added key caching for this worker
async function checkInternalKey(request, env) {
  const authHeader = request.headers.get("Authorization");
  if (!authHeader?.startsWith("Bearer ")) return false;

  // DEFINE providedKey first
  const providedKey = authHeader.slice(7).trim(); // Skip "Bearer ", trim in case of newline/space

  // Only fetch secret once per instance
  if (!cachedInternalKey) {
    // If this binding is a Secret Store object, .get() returns the value.
    // If it's a plain string secret, .get() won't exist — adjust as needed.
    cachedInternalKey = await env.DB1_INTERNAL_KEY.get();
    if (typeof cachedInternalKey === "string") {
      cachedInternalKey = cachedInternalKey.trim();
    }
  }

  return providedKey === cachedInternalKey;
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
  "Pending Schedule",
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

export default {
	async fetch(request, env, ctx) {
		const url = new URL(request.url);
		const origin = request.headers.get("origin");
		// Trace seeds for consistent meta
		const request_id = crypto.randomUUID();
		const t0 = Date.now();

		await log(env, {
			level: "debug",
			service: "bootstrap",
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

		if (request.method === "POST" && url.pathname === "/db1/videos") {
		try {
			const body = await request.json();

			const {
				title = null,
				status = null,
				video_type = null,
				scheduled_at = null,
				r2_url = null,
				r2_transcript_url = null,
				video_filename = null,
				content_type = null,
				file_size_bytes = null,
				transcript_id = null,
				planly_media_id = null,
				social_copy_hook = null,
				social_copy_body = null,
				social_copy_cta = null,
				hashtags = null
				} = body;

			const now = new Date().toISOString();
			// v1.2.10 ADD: value checks for enums (if provided)
			if (status !== null && status !== undefined && !ALLOWED_STATUS.has(status)) {
			return new Response(JSON.stringify({
				success: false,
				error: "Invalid status",
				message: `Status must be one of: ${[...ALLOWED_STATUS].join(", ")}`,
			}), {
				status: 400,
				headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) },
			});
			}

			if (video_type !== null && video_type !== undefined && !ALLOWED_VIDEO_TYPE.has(video_type)) {
			return new Response(JSON.stringify({
				success: false,
				error: "Invalid video_type",
				message: `Video type must be one of: ${[...ALLOWED_VIDEO_TYPE].join(", ")}`,
			}), {
				status: 400,
				headers: { "Content-Type": "application/json", ...getCorsHeaders(origin) },
			});
			}
			const fullPayload = {
				title,
				status,
				video_type,
				scheduled_at,
				r2_url,
				r2_transcript_url,
				video_filename,
				content_type,
				file_size_bytes,
				transcript_id,
				planly_media_id,
				social_copy_hook,
				social_copy_body,
				social_copy_cta,
				hashtags,
				record_created: now,
				record_modified: now
				};

// v1.2.9 ADD: sanitize clears[]
const rawClears = Array.isArray(body?.clears) ? body.clears : [];
const clears = rawClears
  .filter((k) => typeof k === "string" && CLEARABLE_COLS.has(k));

	// v1.2.9 CHANGED: dynamic DO UPDATE SET to honor `clears`
const updateAssignments = [
  // For each editable column: if in `clears` -> NULL, else COALESCE(excluded.col, videos.col)
  ...EDITABLE_COLS.map((col) => {
    if (clears.includes(col)) return `${col} = NULL`;
    return `${col} = COALESCE(excluded.${col}, videos.${col})`;
  }),
  // Always bump record_modified on UPSERT
  `record_modified = ?`
].join(",\n                ");

			const stmt = env.DB.prepare(`
			INSERT INTO videos (
				title, status, video_type, scheduled_at, r2_url, r2_transcript_url,
				video_filename, content_type, file_size_bytes, transcript_id,
				planly_media_id, social_copy_hook, social_copy_body, social_copy_cta,
				hashtags, record_created, record_modified
			)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(title) DO UPDATE SET
				${updateAssignments}
				RETURNING
					rowid AS _rid,
					CASE WHEN rowid = last_insert_rowid()
						THEN 'insert' ELSE 'update' END AS _action
				`)
			
			.bind(
			fullPayload.title,
			fullPayload.status,
			fullPayload.video_type,
			fullPayload.scheduled_at,
			fullPayload.r2_url,
			fullPayload.r2_transcript_url,
			fullPayload.video_filename,
			fullPayload.content_type,
			fullPayload.file_size_bytes,
			fullPayload.transcript_id,
			fullPayload.planly_media_id,
			fullPayload.social_copy_hook,
			fullPayload.social_copy_body,
			fullPayload.social_copy_cta,
			fullPayload.hashtags,
			fullPayload.record_created,
			fullPayload.record_modified, // VALUES(... last)
			fullPayload.record_modified  // for DO UPDATE record_modified = ?
			);

			const upsertRes = await stmt.all();
			const action = upsertRes?.results?.[0]?._action || "unknown";
		
			await log(env, {
			level: "info",
			service: "db1-upsert",
			message: "Upserted video",
			meta: {
				request_id, route: url.pathname, method: request.method,
				ok: true, status: 200,
				// domain-safe fields:
				title, video_type, scheduled_at,
				clears_count: Array.isArray(body?.clears) ? body.clears.length : 0,
				action,                  // "insert" | "update" | "unknown"
				duration_ms: Date.now() - t0
				}
			});
			const db1Data = {
				title,
				status,
				video_type,
				scheduled_at,
				r2_url,
				r2_transcript_url,
				video_filename,
				content_type,
				file_size_bytes,
				transcript_id,
				planly_media_id,
				social_copy_hook,
				social_copy_body,
				social_copy_cta,
				hashtags,
				record_created: now,
				record_modified: now
				};

			return new Response(JSON.stringify({ success: true, db1Data }), {
				status: 200,
				headers: { "Content-Type": "application/json",
				...getCorsHeaders(origin)
					}
				});


		} catch (err) {					
			await log(env, {
				level: "warn",
				service: "db1-upsert",
				message: "Upsert failed",
				meta: {
					request_id, route: url.pathname, method: request.method,
					ok: false, status: 400,
					error: err?.message,
					duration_ms: Date.now() - t0
				}
			});

			return new Response(JSON.stringify({
				success: false,
				error: "Upsert Error",
				message: err.message
				}), {
				status: 400,
				headers: { "Content-Type": "application/json",
					...getCorsHeaders(origin)
				}
				});
		}
		}

		// Only handle GET /videos
		if (request.method === "GET" && url.pathname === "/db1/videos") {
			try {
				const { searchParams } = url;
				const status = searchParams.get("status");
				const type = searchParams.get("type");

				let query = "SELECT * FROM videos";
				let conditions = [];
				let params = [];

				if (status) {
					conditions.push("status = ?");
					params.push(status);
				}
				if (type) {
					conditions.push("video_type = ?");
					params.push(type);
				}

				if (conditions.length > 0) {
					query += " WHERE " + conditions.join(" AND ");
				}

				query += " ORDER BY record_modified DESC";

				const results = await env.DB.prepare(query).bind(...params).all();

				// CHANGED: Apply dynamic CORS headers
				return new Response(JSON.stringify(results.results, null, 2), {
					headers: {
						"Content-Type": "application/json",
						...getCorsHeaders(origin),
					},
				});

			} catch (err) {
				await env.GRAFANA_WORKER.fetch("https://log", {
					method: "POST",
					headers: { "Content-Type": "application/json" },
					body: JSON.stringify({
						source: "gr8r-db1-worker",
						level: "error",
						message: "GET /videos failed",
						meta: {
							error: err.message,
							stack: err.stack
						},
					}),
				});

				await log(env, {
					level: "error",
					service: "db1-fetch",
					message: "GET /db1/videos failed",
					meta: {
						request_id, route: url.pathname, method: request.method,
						ok: false, status: 500,
						error: err?.message, stack: err?.stack,
						duration_ms: Date.now() - t0
					}
				});

				// CHANGED: Apply dynamic CORS headers on error
				return new Response(JSON.stringify({
				success: false,
				error: err.message
				}), {
				status: 500,
				headers: {
					"Content-Type": "application/json",
					...getCorsHeaders(origin),
				},
				});

			}
		}

return new Response("Not found", { status: 404, headers: getCorsHeaders(origin) });

	},
};
