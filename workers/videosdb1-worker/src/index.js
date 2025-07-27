function isExternalRequest(request) {
	const isFromWorker = request.headers.get("cf-worker") !== null;
	const isInternalIP = request.headers.get("cf-connecting-ip")?.startsWith("127.");
	return !(isFromWorker || isInternalIP);
}

export default {
	async fetch(request, env, ctx) {
		// ADDED: Handle CORS preflight requests
		if (request.method === "OPTIONS") {
			return new Response(null, {
				status: 204,
				headers: {
					"Access-Control-Allow-Origin": "https://admin.gr8r.com",
					"Access-Control-Allow-Methods": "GET, POST, OPTIONS",
					"Access-Control-Allow-Headers": "Authorization, Content-Type",
				},
			});
		}

		if (isExternalRequest(request)) {
			const authHeader = request.headers.get("Authorization");
			const expected = `Bearer ${await env.ADMIN_TOKEN.get()}`;
			if (authHeader !== expected) {
				return new Response("Unauthorized", { status: 401 });
			}
		}

		const url = new URL(request.url);

		// Handle POST /import to insert a new video
		if (request.method === "POST" && url.pathname === "/import") {
			try {
				// ✅ OPTIONAL: Only allow internal Cloudflare Worker calls
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

				// ADDED: Grafana logging for successful import
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
				// ADDED: Grafana logging for errors
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
				// CHANGED: Moved query vars *inside* try block for safe logging
				const { searchParams } = url;
				const status = searchParams.get("status");
				const type = searchParams.get("type");

				let query = "SELECT * FROM videos";        // CHANGED
				let conditions = [];                       // CHANGED
				let params = [];                           // CHANGED

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

				return new Response(JSON.stringify(results.results, null, 2), {
					headers: {
						"Content-Type": "application/json",
						"Access-Control-Allow-Origin": "https://admin.gr8r.com",
						"Access-Control-Allow-Headers": "Authorization",
						"Access-Control-Allow-Methods": "GET, POST, OPTIONS",
					},
				});

			} catch (err) {
				// CHANGED: Removed logging of query/params to avoid ReferenceError
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

				return new Response(`Error: ${err.message}`, {
					status: 500,
					headers: {
						"Access-Control-Allow-Origin": "https://admin.gr8r.com",
						"Access-Control-Allow-Headers": "Authorization",
						"Access-Control-Allow-Methods": "GET, POST, OPTIONS",
					},
				});
			}
		}

		return new Response("Not found", { status: 404 });
	},
};
