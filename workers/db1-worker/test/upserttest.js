//db1-worker-test-minimal v1.0.0
//Stripped down version for troubleshooting UPSERT functionality
//Removed: Auth, CORS, logging - focus only on database operations

export default {
	async fetch(request, env, ctx) {
		const url = new URL(request.url);
		
		// Simple CORS for testing
		const corsHeaders = {
			"Access-Control-Allow-Origin": "*",
			"Access-Control-Allow-Methods": "GET, POST, OPTIONS",
			"Access-Control-Allow-Headers": "Content-Type"
		};

		if (request.method === "OPTIONS") {
			return new Response(null, { status: 204, headers: corsHeaders });
		}

		// POST /videos - UPSERT operation
		if (request.method === "POST" && url.pathname === "/videos") {
			try {
				const body = await request.json();
				console.log("📥 Received payload:", body);

				// Extract fields with defaults to null
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
				
				console.log("🔢 Field count check - should be 17:");
				const fieldNames = [
					'title', 'status', 'video_type', 'scheduled_at', 'r2_url', 
					'r2_transcript_url', 'video_filename', 'content_type', 'file_size_bytes', 
					'transcript_id', 'planly_media_id', 'social_copy_hook', 'social_copy_body', 
					'social_copy_cta', 'hashtags', 'record_created', 'record_modified'
				];
				console.log("📊 Fields:", fieldNames.length, fieldNames);

				// Prepare binding values
				const insertValues = [
					title, status, video_type, scheduled_at, r2_url, r2_transcript_url,
					video_filename, content_type, file_size_bytes, transcript_id,
					planly_media_id, social_copy_hook, social_copy_body, social_copy_cta,
					hashtags, now, now  // record_created, record_modified
				];
				
				console.log("🎯 INSERT binding count:", insertValues.length);
				console.log("🎯 INSERT values:", insertValues);

				// FIXED: 17 fields = 17 placeholders in VALUES
				const stmt = env.DB.prepare(`
					INSERT INTO test_videos (
						title, status, video_type, scheduled_at, r2_url, r2_transcript_url,
						video_filename, content_type, file_size_bytes, transcript_id,
						planly_media_id, social_copy_hook, social_copy_body, social_copy_cta,
						hashtags, record_created, record_modified
					)
					VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
					ON CONFLICT(title) DO UPDATE SET
						status = COALESCE(excluded.status, test_videos.status),
						video_type = COALESCE(excluded.video_type, test_videos.video_type),
						scheduled_at = COALESCE(excluded.scheduled_at, test_videos.scheduled_at),
						r2_url = COALESCE(excluded.r2_url, test_videos.r2_url),
						r2_transcript_url = COALESCE(excluded.r2_transcript_url, test_videos.r2_transcript_url),
						video_filename = COALESCE(excluded.video_filename, test_videos.video_filename),
						content_type = COALESCE(excluded.content_type, test_videos.content_type),
						file_size_bytes = COALESCE(excluded.file_size_bytes, test_videos.file_size_bytes),
						transcript_id = COALESCE(excluded.transcript_id, test_videos.transcript_id),
						planly_media_id = COALESCE(excluded.planly_media_id, test_videos.planly_media_id),
						social_copy_hook = COALESCE(excluded.social_copy_hook, test_videos.social_copy_hook),
						social_copy_body = COALESCE(excluded.social_copy_body, test_videos.social_copy_body),
						social_copy_cta = COALESCE(excluded.social_copy_cta, test_videos.social_copy_cta),
						hashtags = COALESCE(excluded.hashtags, test_videos.hashtags),
						record_modified = ?
				`)
				.bind(
					// INSERT values (17 bindings)
					title, status, video_type, scheduled_at, r2_url, r2_transcript_url,
					video_filename, content_type, file_size_bytes, transcript_id,
					planly_media_id, social_copy_hook, social_copy_body, social_copy_cta,
					hashtags, now, now,
					// ON CONFLICT value (1 binding)
					now  // record_modified for conflict clause
				);

				console.log("🔧 Final statement args length:", stmt.args?.length || 'undefined');
				console.log("🔧 Final statement args:", stmt.args);

				const result = await stmt.run();
				console.log("✅ Database operation result:", result);

				return new Response(JSON.stringify({ 
					success: true, 
					message: "UPSERT completed",
					meta: {
						title,
						binding_count: stmt.args?.length,
						timestamp: now
					}
				}), {
					status: 200,
					headers: { "Content-Type": "application/json", ...corsHeaders }
				});

			} catch (err) {
				console.error("❌ UPSERT Error:", err);
				return new Response(JSON.stringify({
					success: false,
					error: err.message,
					stack: err.stack
				}), {
					status: 500,
					headers: { "Content-Type": "application/json", ...corsHeaders }
				});
			}
		}

		// GET /videos - Simple query
		if (request.method === "GET" && url.pathname === "/videos") {
			try {
				const results = await env.DB.prepare("SELECT * FROM test_videos ORDER BY record_modified DESC LIMIT 10").all();
				
				return new Response(JSON.stringify({
					success: true,
					count: results.results?.length || 0,
					videos: results.results
				}, null, 2), {
					headers: { "Content-Type": "application/json", ...corsHeaders }
				});

			} catch (err) {
				console.error("❌ GET Error:", err);
				return new Response(JSON.stringify({
					success: false,
					error: err.message
				}), {
					status: 500,
					headers: { "Content-Type": "application/json", ...corsHeaders }
				});
			}
		}

		// Health check
		if (request.method === "GET" && url.pathname === "/health") {
			return new Response(JSON.stringify({
				status: "ok",
				timestamp: new Date().toISOString(),
				version: "db1-worker-test-minimal v1.0.0"
			}), {
				headers: { "Content-Type": "application/json", ...corsHeaders }
			});
		}

		return new Response("Not found", { 
			status: 404,
			headers: corsHeaders
		});
	}
};