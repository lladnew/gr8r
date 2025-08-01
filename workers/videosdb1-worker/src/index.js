//videosdb1-worker v1.0.3 removed secret and added Cf-Access-Jwt assertion and Pages
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

function isExternalRequest(request) {
	const isFromWorker = request.headers.get("cf-worker") !== null;
	const isInternalIP = request.headers.get("cf-connecting-ip")?.startsWith("127.");
	return !(isFromWorker || isInternalIP);
}

export default {
	async fetch(request, env, ctx) {
		const origin = request.headers.get("Origin");
		// TEMPORARY: Log all headers for debugging
		const headersDump = {};
		for (const [key, value] of request.headers.entries()) {
			headersDump[key] = value;
		}

		await env.GRAFANA_WORKER.fetch("http://log", {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({
				source: "gr8r-videosdb1-worker",
				level: "debug",
				message: "Full header dump for JWT debugging",
				meta: {
					method: request.method,
					url: request.url,
					headers: headersDump,
				},
			}),
		});

		await env.GRAFANA_WORKER.fetch("http://log", {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({
				source: "gr8r-videosdb1-worker",
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
		//commenting out check for Bearer secret below - added new Cf-Access-Jwt assertion and Pages
		//		if (isExternalRequest(request)) {
		//			const authHeader = request.headers.get("Authorization");
		//			const expected = `Bearer ${await env.ADMIN_TOKEN.get()}`;
		//			if (authHeader !== expected) {
		//				return new Response("Unauthorized", {
		//					status: 401,
		//					headers: {
		//						"Content-Type": "text/plain",
		//						...getCorsHeaders(origin),
		//					},
		//					});
		//			}
		//		}
		// ADD: Temporary test route to verify Access callback
		if (new URL(request.url).pathname === "/videosdb1/test") {
			return new Response("Test OK", {
				status: 200,
				headers: {
					"Content-Type": "text/plain",
					...getCorsHeaders(origin),
				},
			});
		}

		if (isExternalRequest(request)) {
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

			const accessURL = "https://gr8r.cloudflareaccess.com"; // ← replace with your Access team domain if different

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

			const valid = await crypto.subtle
				.importKey(
					"jwk",
					keys[0],
					{
						name: "RSASSA-PKCS1-v1_5",
						hash: "SHA-256",
					},
					false,
					["verify"]
				)
				.then((key) =>
					crypto.subtle.verify(
						"RSASSA-PKCS1-v1_5",
						key,
						base64urlToUint8Array(jwt.split(".")[2]),
						new TextEncoder().encode(jwt.split(".")[0] + "." + jwt.split(".")[1])
					)
				);

			if (!valid) {
				await env.GRAFANA_WORKER.fetch("http://log", {
					method: "POST",
					headers: { "Content-Type": "application/json" },
					body: JSON.stringify({
						source: "gr8r-videosdb1-worker",
						level: "warn",
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

		const url = new URL(request.url);

		if (request.method === "POST" && url.pathname === "/import") {
			try {
				const cfConnectingIp = request.headers.get("cf-connecting-ip");
				if (cfConnectingIp !== "127.0.0.1") {
					return new Response("Forbidden", { status: 403 });
				}

				const body = await request.json();

				const {
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
					record_created,
					record_modified
				} = body;

				const stmt = env.DB.prepare(`
      INSERT INTO videos (
        title, status, video_type, scheduled_at, r2_url, r2_transcript_url,
        video_filename, content_type, file_size_bytes, transcript_id,
        planly_media_id, social_copy_hook, social_copy_body, social_copy_cta,
        hashtags, record_created, record_modified
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(title) DO NOTHING
    `).bind(
					title, status, video_type, scheduled_at, r2_url, r2_transcript_url,
					video_filename, content_type, file_size_bytes, transcript_id,
					planly_media_id, social_copy_hook, social_copy_body, social_copy_cta,
					hashtags, record_created, record_modified
				);

				await stmt.run();

				await env.GRAFANA_WORKER.fetch("http://log", {
					method: "POST",
					headers: { "Content-Type": "application/json" },
					body: JSON.stringify({
						source: "gr8r-videosdb1-worker",
						level: "info",
						message: "Imported video",
						meta: { title, scheduled_at, video_type }
					}),
				});

				return new Response("Imported", { status: 200 });

			} catch (err) {
				await env.GRAFANA_WORKER.fetch("http://log", {
					method: "POST",
					headers: { "Content-Type": "application/json" },
					body: JSON.stringify({
						source: "gr8r-videosdb1-worker",
						level: "error",
						message: "Failed to import video",
						meta: { error: err.message, title }
					}),
				});

				return new Response("Import Error: " + err.message, { status: 400 });
			}
		}

		// Only handle GET /videos
		if (request.method === "GET" && url.pathname === "/videos") {
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
				await env.GRAFANA_WORKER.fetch("http://log", {
					method: "POST",
					headers: { "Content-Type": "application/json" },
					body: JSON.stringify({
						source: "gr8r-videosdb1-worker",
						level: "error",
						message: "GET /videos failed",
						meta: {
							error: err.message,
							stack: err.stack
						},
					}),
				});

				// CHANGED: Apply dynamic CORS headers on error
				return new Response(`Error: ${err.message}`, {
					status: 500,
					headers: {
						"Content-Type": "text/plain",
						...getCorsHeaders(request.headers.get("Origin")), // CHANGED
					},
				});
			}
		}

		return new Response("Not found", { status: 404 });
	},
};
