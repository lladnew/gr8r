// v1.4.1 gr8r-videouploads-worker CHANGED: migrate logging to lib/grafana.js with Best Practices and migrate secrets to lib/secrets.js  ADDED: temp cosole log starting line 82 to show the incoming body for testing
// v1.4.0 gr8r-videouploads-worker fixes made going live with D1 UPSERT code
// v1.3.8 gr8r-videouploads-worker added key caching function and updated both DB1 calls to utilize
// ADDED: utilization of sanitize function for 2nd DB1 call
// v1.3.7 gr8r-videouploads-worker revised santizeForDB1 function for null and empty values

// Shared libs
// new getSecret function module
import { getSecret } from "../../../lib/secrets.js";

// new Grafana logging shared script
import { createLogger } from "../../../lib/grafana.js";
const log = createLogger({ source: "gr8r-videouploads-worker" });

function sanitizeForDB1(obj) {
  return Object.fromEntries(
    Object.entries(obj).filter(([_, value]) =>
      value !== undefined &&
      value !== null &&
      value !== ""
    )
  );
}
// Timing helper functions
function now() { return Date.now(); }
function durationMs(t0) { return Date.now() - t0; }

// Base meta for standardized logging use
function baseMeta({ request_id, route, method, origin }) {
  return { request_id, route, method, origin };
}

export default {
  async fetch(request, env, ctx) {
    console.log('[videouploads-worker] Handler triggered');

    const contentType = request.headers.get('content-type') || 'none';
    console.log('[videouploads-worker] Content-Type:', contentType);

    // ADDED: per-request context for logging
        const request_id = crypto.randomUUID();
        const route = new URL(request.url).pathname;
        const method = request.method;
        const origin = request.headers.get('origin') || null;

        // ADDED: initial request trace (debug)
        try {
        await log(env, {
            level: "debug",
            service: "request",
            message: "request received",
            meta: {
            ...baseMeta({ request_id, route, method, origin }),
            content_type: request.headers.get('content-type') || 'none',
            ok: true
            }
        });
        } catch {}

    if (request.method !== 'POST') {
        try {
            await log(env, {
            level: "warn",
            service: "request",
            message: "method not allowed",
            meta: {
                ...baseMeta({ request_id, route, method, origin }),
                status: 405,
                ok: false,
                reason: "method_not_allowed"
            }
            });
        } catch {}
        return new Response("Method Not Allowed", { status: 405 });
        }

    const url = new URL(request.url);
    const { pathname } = url;

    if (pathname === '/upload-video') {
        const reqStart = now();
      try {
        // Accept JSON body instead of multipart/form-data
        const body = await request.json();

        // TEMP: log full incoming payload for troubleshooting
        console.log("[videouploads-worker] Incoming request body:", JSON.stringify(body, null, 2));

        const { filename, title, videoType, scheduleDateTime = "" } = body;

        // ADDED: safe parse log
        try {
        await log(env, {
            level: "debug",
            service: "request",
            message: "parsed request body",
            meta: {
            ...baseMeta({ request_id, route, method, origin }),
            ok: true,
            title: (title || "").slice(0, 120),
            video_type: videoType || null,
            scheduled_at: scheduleDateTime || null
            }
        });
        } catch {}

        console.log('[videouploads-worker] Parsed fields:');
        console.log('  title:', title);
        console.log('  videoType:', videoType);
        console.log('  scheduleDateTime:', scheduleDateTime);
        console.log('  filename:', filename);
        
        if (!(filename && title && videoType)) {
        try {
            await log(env, {
            level: "warn",
            service: "request",
            message: "missing required fields",
            meta: {
                ...baseMeta({ request_id, route, method, origin }),
                status: 400,
                ok: false,
                reason: "missing_fields",
                title: title || null,
                video_type: videoType || null
                }
                });
            } catch {}
            return new Response("Missing required fields", { status: 400 });
            }


        const objectKey = filename; // CHANGED: Using filename directly
        const publicUrl = `https://videos.gr8r.com/${objectKey}`; // CHANGED: Constructing R2 URL

        // Check that the file exists in R2 without downloading it
        const r2CheckStart = now();
        const object = await env.VIDEO_BUCKET.get(objectKey);
        if (!object) {
        try {
            await log(env, {
            level: "warn",
            service: "r2-check",
            message: "R2 object missing",
            meta: {
                ...baseMeta({ request_id, route, method, origin }),
                duration_ms: durationMs(r2CheckStart),
                status: 404,
                ok: false,
                reason: "r2_not_found",
                object_key: objectKey,
                title: title.slice(0, 120)
            }
            });
        } catch {}
        return new Response("Video file not found in R2", { status: 404 });
        }
        
        try {
            await log(env, {
                level: "info",
                service: "r2-check",
                message: "R2 object verified",
                meta: {
                ...baseMeta({ request_id, route, method, origin }),
                duration_ms: durationMs(r2CheckStart),
                status: 200,
                ok: true,
                object_key: objectKey,
                title: title.slice(0, 120)
                }
        });
        } catch {}

        // Attempt to read metadata from R2 object
        let contentType = object.httpMetadata?.contentType || "unknown";
        let contentLength = object.size || null;
        const atStart = now();
        // First Airtable update with new fields
        const airtableResponse = await env.AIRTABLE.fetch(new Request("https://internal/api/airtable/update", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            table: "tblQKTuBRVrpJLmJp",
            title,
            fields: {
              "R2 URL": publicUrl,
              "Schedule Date-Time": scheduleDateTime,
              "Video Type": videoType,
              "Video Filename": filename,
              "Content Type": contentType,
              // "Video File Size": humanSize,  //commenting out since field is depracated
              "Video File Size Number": contentLength,
              "Status": "Working"
            }
          })
        }));

        let airtableData = null;
        
        if (airtableResponse.ok) {
        try { airtableData = await airtableResponse.json(); } catch { airtableData = null; }
        try {
            await log(env, {
            level: "info",
            service: "airtable-upsert",
            message: "Airtable upsert ok",
            meta: {
                ...baseMeta({ request_id, route, method, origin }),
                duration_ms: durationMs(atStart),
                status: 200,
                ok: true,
                title: title.slice(0, 120),
                video_type: videoType,
                scheduled_at: scheduleDateTime || null
            }
            });
        } catch {}
        } else {
        const atStatus = airtableResponse.status;
        try {
            await log(env, {
            level: "error",
            service: "airtable-upsert",
            message: "Airtable upsert failed",
            meta: {
                ...baseMeta({ request_id, route, method, origin }),
                duration_ms: durationMs(atStart),
                status: atStatus,
                ok: false,
                reason: "airtable_error",
                title: title.slice(0, 120)
            }
            });
        } catch {}
        const text = await airtableResponse.text();
        throw new Error(`Airtable create failed: ${text}`);
        }

        // DB1 update to mirror Airtable

        const db1Body = sanitizeForDB1({
          title,
          video_type: videoType,
          scheduled_at: scheduleDateTime,
          r2_url: publicUrl,
          content_type: contentType,
          video_filename: filename,
          file_size_bytes: contentLength,
          status: "Working"
        });

console.log("[DB1 Body] Payload:", JSON.stringify(db1Body, null, 2));

        const db1Key = await getSecret(env, 'DB1_INTERNAL_KEY');
        const db1Start = now();
        const db1Response = await env.DB1.fetch("https://gr8r-db1-worker/db1/videos", {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            "Authorization": `Bearer ${db1Key}`,
        },
        body: JSON.stringify(db1Body),
        });

        const db1Status = db1Response.status;
        const db1Text = await db1Response.text();
        let db1Data = null;
        try { db1Data = JSON.parse(db1Text); } catch { db1Data = { raw: db1Text }; }

        if (!db1Response.ok) {
        try {
            await log(env, {
            level: "error",
            service: "db1-upsert",
            message: "DB1 upsert failed",
            meta: {
                ...baseMeta({ request_id, route, method, origin }),
                duration_ms: durationMs(db1Start),
                status: db1Status,
                ok: false,
                reason: "db1_error",
                title: title.slice(0, 120)
            }
            });
        } catch {}
        throw new Error(`DB1 update failed: ${db1Text}`);
        } else {
        try {
            await log(env, {
            level: "info",
            service: "db1-upsert",
            message: "DB1 upsert ok",
            meta: {
                ...baseMeta({ request_id, route, method, origin }),
                duration_ms: durationMs(db1Start),
                status: db1Status,
                ok: true,
                title: title.slice(0, 120)
            }
            });
        } catch {}
        }

        // Rev.ai logic
        const revStart = now();
        const revaiResponse = await env.REVAI.fetch(new Request("https://internal/api/revai/transcribe", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            media_url: publicUrl,
            metadata: title,
            callback_url: "https://callback.gr8r.com/api/revai/callback"
          })
        }));

        let revaiJson = null;
        try { revaiJson = await revaiResponse.json(); } catch {}

        if (!revaiResponse.ok || !revaiJson?.id) {
        try {
            await log(env, {
            level: "error",
            service: "revai-submit",
            message: "Revai submission failed",
            meta: {
                ...baseMeta({ request_id, route, method, origin }),
                duration_ms: durationMs(revStart),
                status: revaiResponse.status,
                ok: false,
                reason: "revai_error",
                title: title.slice(0, 120)
            }
            });
        } catch {}
        return new Response(JSON.stringify({
            error: "Rev.ai job failed",
            message: revaiJson
        }), {
            status: 502,
            headers: { "Content-Type": "application/json" }
        });
        }

        try {
        await log(env, {
            level: "info",
            service: "revai-submit",
            message: "Revai submission ok",
            meta: {
            ...baseMeta({ request_id, route, method, origin }),
            duration_ms: durationMs(revStart),
            status: 200,
            ok: true,
            revai_job_id: revaiJson.id,
            title: title.slice(0, 120)
            }
        });
        } catch {}

        // Airtable update for Rev.ai Transcript ID      
        const atFollowStart = now();
        const atFollow = await env.AIRTABLE.fetch(new Request("https://internal/api/airtable/update", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
            table: "tblQKTuBRVrpJLmJp",
            title,
            fields: {
            "Status": "Pending Transcription",
            "Transcript ID": revaiJson.id
            }
        })
        }));
        if (!atFollow.ok) {
        try {
            await log(env, {
            level: "error",
            service: "airtable-followup",
            message: "Airtable transcript failed",
            meta: {
                ...baseMeta({ request_id, route, method, origin }),
                duration_ms: durationMs(atFollowStart),
                status: atFollow.status,
                ok: false,
                reason: "airtable_error",
                revai_job_id: revaiJson.id,
                title: title.slice(0, 120)
            }
            });
        } catch {}
        } else {
        try {
            await log(env, {
            level: "info",
            service: "airtable-followup",
            message: "Airtable transcript ok",
            meta: {
                ...baseMeta({ request_id, route, method, origin }),
                duration_ms: durationMs(atFollowStart),
                status: 200,
                ok: true,
                revai_job_id: revaiJson.id,
                title: title.slice(0, 120)
            }
            });
        } catch {}
        }

        // DB1 follow-up update for Rev.ai job
        try {
        const db1FollowupBody = sanitizeForDB1({
            title,
            status: "Pending Transcription",
            transcript_id: revaiJson.id
        });

        const db1FollowStart = now();
        const db1FollowupResponse = await env.DB1.fetch("https://gr8r-db1-worker/db1/videos", {
            method: "POST",
            headers: {
            "Content-Type": "application/json",
            "Authorization": `Bearer ${db1Key}` // reuse earlier secret
            },
            body: JSON.stringify(db1FollowupBody)
        });

        const db1FollowStatus = db1FollowupResponse.status;
        await db1FollowupResponse.text(); // drain body; do not log it

        if (!db1FollowupResponse.ok) {
            try {
            await log(env, {
                level: "error",
                service: "db1-followup",
                message: "DB1 transcript failed",
                meta: {
                ...baseMeta({ request_id, route, method, origin }),
                duration_ms: durationMs(db1FollowStart),
                status: db1FollowStatus,
                ok: false,
                reason: "db1_error",
                revai_job_id: revaiJson.id,
                title: title.slice(0, 120)
                }
            });
            } catch {}
            throw new Error(`DB1 transcript update failed: status ${db1FollowStatus}`);
        }

        try {
            await log(env, {
            level: "info",
            service: "db1-followup",
            message: "DB1 transcript ok",
            meta: {
                ...baseMeta({ request_id, route, method, origin }),
                duration_ms: durationMs(db1FollowStart),
                status: db1FollowStatus,
                ok: true,
                revai_job_id: revaiJson.id,
                title: title.slice(0, 120)
            }
            });
        } catch {}

        } catch (err) {
        try {
            await log(env, {
            level: "error",
            service: "db1-followup",
            message: "DB1 transcript exception",
            meta: {
                ...baseMeta({ request_id, route, method, origin }),
                status: 500,
                ok: false,
                error: err.message,
                stack: err.stack,
                title: title ? title.slice(0, 120) : null
            }
            });
        } catch {}
        throw err;
        }

        // RETAINED: Final response to Apple Shortcut
        const responseBody = {
          message: "Video upload complete",
          objectKey,
          publicUrl,
          title,
          scheduleDateTime,
          videoType,
          fileSizeMB: contentLength ? parseFloat((contentLength / 1048576).toFixed(2)) : null,
          contentType,
          transcriptId: revaiJson.id,
          airtableData,
          db1Data
        };

        try {
            await log(env, {
                level: "info",
                service: "response",
                message: "request complete",
                meta: {
                ...baseMeta({ request_id, route, method, origin }),
                status: 200,
                ok: true,
                duration_ms: durationMs(reqStart),
                title: title.slice(0, 120)
                }
            });
            } catch {}

        return new Response(JSON.stringify(responseBody), {
          status: 200,
          headers: { "Content-Type": "application/json" }
        });

      } catch (err) {
        try {
        await log(env, {
            level: "error",
            service: "response",
            message: "request failed",
            meta: {
            ...baseMeta({ request_id, route, method, origin }),
            status: 500,
            ok: false,
            error: err.message,
            stack: err.stack
            }
         });
        } catch {}

        return new Response(JSON.stringify({
          error: "Unhandled upload failure",
          message: err.message,
          name: err.name,
          stack: err.stack
        }), {
          status: 500,
          headers: { "Content-Type": "application/json" }
        });
      }
    }

    try {
        await log(env, {
            level: "warn",
            service: "request",
            message: "forbidden route",
            meta: {
            ...baseMeta({ request_id, route, method, origin }),
            status: 403,
            ok: false,
            reason: "forbidden_route"
            }
        });
        } catch {}
        return new Response("Forbidden", { status: 403 });
  }
};
