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
	};

	if (origin && allowedOrigins.includes(origin)) {
		headers["Access-Control-Allow-Origin"] = origin;
	}

	return headers;
}

function base64urlToUint8Array(base64url) {
	const base64 = base64url.replace(/-/g, "+").replace(/_/g, "/");
	const binary = atob(base64);
	return Uint8Array.from(binary, (c) => c.charCodeAt(0));
}

// Check for internal Bearer key auth and added key caching for this worker
async function checkInternalKey(request, env) {
	const authHeader = request.headers.get("Authorization");
	if (!authHeader?.startsWith("Bearer ")) return false;

	// Only fetch secret once per instance
	if (!cachedInternalKey) {
		cachedInternalKey = await env.DB1_INTERNAL_KEY.get();
	}

	const providedKey = authHeader.slice(7); // Skip "Bearer "
	return providedKey === cachedInternalKey;
}

let cachedInternalKey = null;

export default {
	async fetch(request, env, ctx) {
		const url = new URL(request.url);
		const origin = request.headers.get("Origin");
		
		await env.GRAFANA_WORKER.fetch("https://log", {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({
				source: "gr8r-db1-worker",
				level: "debug",
				message: "Incoming request",
				meta: {
					origin,
					method: request.method,
					url: request.url,
				},
			}),
		});
		//Handle CORS preflight requests dynamically
		if (request.method === "OPTIONS") {
			return new Response(null, {
				status: 204,
				headers: getCorsHeaders(request.headers.get("Origin")), // CHANGED
			});
		}
		
		// ADDED: Replace legacy internal header check with Authorization: Bearer
		const isInternal = await checkInternalKey(request, env);

		await env.GRAFANA_WORKER.fetch("https://log", {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: JSON.stringify({
			source: "gr8r-db1-worker",
			level: "debug",
			message: "Caller identity check",
			meta: {
			auth_header: request.headers.get("Authorization")?.slice(0, 15), // truncated
			internal: isInternal,
			},
		}),
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
			await env.GRAFANA_WORKER.fetch("https://log", {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({
				source: "gr8r-db1-worker",
				level: "error",
				message: "JWT verification failed",
				meta: { origin, jwtStart: jwt?.slice(0, 15) }, // truncate for privacy
			}),
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

// new UPSERT code includes time/date stamping for record_created and/or record_modified
console.log("✅ Running db1-worker UPSERT with 18 bindings (17fiels + conflict"); //DEBUG

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

console.log("🧪 Binding values length:", [
  title, status, video_type, scheduled_at, r2_url, r2_transcript_url,
  video_filename, content_type, file_size_bytes, transcript_id,
  planly_media_id, social_copy_hook, social_copy_body, social_copy_cta,
  hashtags, now, now
].length);

			// CHANGED: Bind 18 parameters (last one is for record_modified in ON CONFLICT)
			const stmt = env.DB.prepare(`
			INSERT INTO videos (
				title, status, video_type, scheduled_at, r2_url, r2_transcript_url,
				video_filename, content_type, file_size_bytes, transcript_id,
				planly_media_id, social_copy_hook, social_copy_body, social_copy_cta,
				hashtags, record_created, record_modified
			)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(title) DO UPDATE SET
				status = COALESCE(excluded.status, videos.status),
				video_type = COALESCE(excluded.video_type, videos.video_type),
				scheduled_at = COALESCE(excluded.scheduled_at, videos.scheduled_at),
				r2_url = COALESCE(excluded.r2_url, videos.r2_url),
				r2_transcript_url = COALESCE(excluded.r2_transcript_url, videos.r2_transcript_url),
				video_filename = COALESCE(excluded.video_filename, videos.video_filename),
				content_type = COALESCE(excluded.content_type, videos.content_type),
				file_size_bytes = COALESCE(excluded.file_size_bytes, videos.file_size_bytes),
				transcript_id = COALESCE(excluded.transcript_id, videos.transcript_id),
				planly_media_id = COALESCE(excluded.planly_media_id, videos.planly_media_id),
				social_copy_hook = COALESCE(excluded.social_copy_hook, videos.social_copy_hook),
				social_copy_body = COALESCE(excluded.social_copy_body, videos.social_copy_body),
				social_copy_cta = COALESCE(excluded.social_copy_cta, videos.social_copy_cta),
				hashtags = COALESCE(excluded.hashtags, videos.hashtags),
				record_modified = ?
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
				fullPayload.record_modified, // <-- for VALUES(...)
				fullPayload.record_modified  // <-- for ON CONFLICT clause
			);
console.log("🧪 Final bind count:", stmt.args.length); //DEBUG
console.log("🧪 Bound values:", stmt.args); //DEBUG
console.log("✅ DB1 binding fields:", { //DEBUG
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
  now
});
			await stmt.run();

			await env.GRAFANA_WORKER.fetch("https://log", {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({
				source: "gr8r-db1-worker",
				level: "info",
				message: "Upserted video",
				meta: { title, scheduled_at, video_type }
			}),
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
			await env.GRAFANA_WORKER.fetch("https://log", {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({
				source: "gr8r-db1-worker",
				level: "error",
				message: "Failed to upsert video",
				meta: { error: err.message }
			}),
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

				query += " ORDER BY scheduled_at DESC LIMIT 100";

				const results = await env.DB.prepare(query).bind(...params).all();

				// CHANGED: Apply dynamic CORS headers
				return new Response(JSON.stringify(results.results, null, 2), {
					headers: {
						"Content-Type": "application/json",
						...getCorsHeaders(request.headers.get("Origin")), // CHANGED
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

				// CHANGED: Apply dynamic CORS headers on error
				return new Response(JSON.stringify({
				success: false,
				error: err.message
				}), {
				status: 500,
				headers: {
					"Content-Type": "application/json",
					...getCorsHeaders(request.headers.get("Origin")),
				},
				});

			}
		}

		return new Response("Not found", { status: 404 });
	},
};
