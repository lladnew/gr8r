// v1.4.9 gr8r-videouploads-worker CHANGED: removed channel match grafana success log missed previously
// v1.4.8 gr8r-videouploads-worker CHANGED: updated to safeLog function and tightened logs to revised standards
// v1.4.7 gr8r-videouploads-worker FIXED: publishing_status issue
// v1.4.6 gr8r-videouploads-worker FIXED: video_id issue
// v1.4.5 gr8r-videouploads-worker ADDED: DEBUG logging tweaks
// v1.4.4 gr8r-videouploads-worker FIXED: inclued channel ID for puglishing posts and added video_ID
// v1.4.3 gr8r-videouploads-worker ADDED: schedule rows to Publishing table for all channels listed in upload
// v1.4.2 gr8r-videouploads-worker ADDED: parsing for Channels list incoming and logging for that
// v1.4.1 gr8r-videouploads-worker CHANGED: migrate logging to lib/grafana.js with Best Practices and migrate secrets to lib/secrets.js  ADDED: temp cosole log starting line 82 to show the incoming body for testing
// v1.4.0 gr8r-videouploads-worker fixes made going live with D1 UPSERT code
// v1.3.8 gr8r-videouploads-worker added key caching function and updated both DB1 calls to utilize
// ADDED: utilization of sanitize function for 2nd DB1 call
// v1.3.7 gr8r-videouploads-worker revised santizeForDB1 function for null and empty values

// Shared libs
// new getSecret function module
import { getSecret } from "../../../lib/secrets.js";

// Grafana logging shared script
import { createLogger } from "../../../lib/grafana.js";
// Safe logger: always use this; caches underlying logger internally
let _logger;
const safeLog = async (env, entry) => {
  try {
    _logger = _logger || createLogger({ source: "gr8r-videouploads-worker" });
    await _logger(env, entry);
  } catch (e) {
    // Never throw from logging
    console.log('LOG_FAIL', entry?.service || 'unknown', e?.stack || e?.message || e);
  }
};

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
    
    // Summary flags & counters for final Grafana entry
    let r2_ok = false;
    let airtable_ok = false;
    let db1_ok = false;
    let publishing_requested = 0;
    let publishing_matched = 0;
    let publishing_unmatched = 0;
    let publishing_inserted = 0; // increment when a publishing POST returns ok
    let revai_ok = false;
    let revai_job_id = null;
  
    // ADDED: per-request context for logging
        const request_id = crypto.randomUUID();
        const route = new URL(request.url).pathname;
        const method = request.method;
        const origin = request.headers.get('origin') || null;

        console.log("[request] received", {
            content_type: contentType || 'none'
        });

    if (request.method !== 'POST') {
        await safeLog(env, {
            level: "warn",
            service: "request",
            message: "method not allowed",
            meta: {
                ...baseMeta({ request_id, route, method, origin }),
                status_code: 405,
                ok: false,
                reason: "method_not_allowed"
            }
            });        return new Response("Method Not Allowed", { status: 405 });
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

        const { filename, title, videoType, channels, scheduleDateTime = "" } = body;

        // ADDED: normalize channels -> array of trimmed names
        const channelsList = Array.isArray(channels)
        ? channels
        : String(channels || "")
            .split(/\r?\n/)        // split on newlines from Shortcut
            .map(s => s.trim())
            .filter(Boolean);

        console.log("[request] parsed body", {
            title: (title || "").slice(0, 120),
            video_type: videoType || null,
            scheduled_at: scheduleDateTime || null,
            channels: channelsList || null
        });

        // TEMP: log normalized channels for verification
        console.log("[videouploads-worker] Channels (normalized):", JSON.stringify(channelsList));

        console.log('[videouploads-worker] Parsed fields:');
        console.log('  title:', title);
        console.log('  videoType:', videoType);
        console.log('  scheduleDateTime:', scheduleDateTime);
        console.log('  filename:', filename);
        
        if (!(filename && title && videoType)) {
        await safeLog(env, {
            level: "warn",
            service: "request",
            message: "missing required fields",
            meta: {
                ...baseMeta({ request_id, route, method, origin }),
                status_code: 400,
                ok: false,
                reason: "missing_fields",
                title: title || null,
                video_type: videoType || null
                }
                });            
                return new Response("Missing required fields", { status: 400 });
            }


        const objectKey = filename; // CHANGED: Using filename directly
        const publicUrl = `https://videos.gr8r.com/${objectKey}`; // CHANGED: Constructing R2 URL

        // Check that the file exists in R2 without downloading it
        const r2CheckStart = now();
        const object = await env.VIDEO_BUCKET.get(objectKey);
        if (!object) {
        await safeLog(env, {
            level: "warn",
            service: "r2-check",
            message: "R2 object missing",
            meta: {
                ...baseMeta({ request_id, route, method, origin }),
                duration_ms: durationMs(r2CheckStart),
                status_code: 404,
                ok: false,
                reason: "r2_not_found",
                object_key: objectKey,
                title: title.slice(0, 120)
            }
            });        return new Response("Video file not found in R2", { status: 404 });
        }
        
        r2_ok = true;
        console.log("[r2-check] verified", { objectKey, title: title?.slice(0,120) });

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
        console.log("[airtable-upsert] ok", {
            title: title?.slice(0,120),
            videoType,
            scheduleDateTime
        });
        airtable_ok = true;
        } else {
        const atStatus = airtableResponse.status;
        await safeLog(env, {
            level: "error",
            service: "airtable-upsert",
            message: "Airtable upsert failed",
            meta: {
                ...baseMeta({ request_id, route, method, origin }),
                duration_ms: durationMs(atStart),
                status_code: atStatus,
                ok: false,
                reason: "airtable_error",
                title: title.slice(0, 120)
            }
            });        const text = await airtableResponse.text();
        throw new Error(`Airtable create failed: ${text}`);
        }

        // DB1 update diverging from Airtable in v1.4.2

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
        //extract video_id when db1-worker returns it
        let videoId = db1Data?.video_id ?? null;

        if (!db1Response.ok) {
        await safeLog(env, {
            level: "error",
            service: "db1-upsert",
            message: "DB1 upsert failed",
            meta: {
                ...baseMeta({ request_id, route, method, origin }),
                duration_ms: durationMs(db1Start),
                status_code: db1Status,
                ok: false,
                reason: "db1_error",
                title: title.slice(0, 120)
            }
            });        throw new Error(`DB1 update failed: ${db1Text}`);
        } else {
        db1_ok = true;
        console.log("[db1-upsert] ok", {
            title: title?.slice(0,120),
            videoId
        });
        }
        // === Publishing rows for selected channels (only when scheduled_at is present) ===
        try {
        // Guard: requires schedule AND at least one channel
        if (!scheduleDateTime || !channelsList.length) {
            await safeLog(env, {
                level: "info",
                service: "publishing",
                message: "publishing skipped",
                meta: {
                ...baseMeta({ request_id, route, method, origin }),
                ok: true,
                status_code: 200,
                reason: !scheduleDateTime ? "no_schedule" : "no_channels",
                scheduled_at: scheduleDateTime || null,
                channels_count: channelsList.length
                }
            });        } else {
            // Fetch channels list from DB1 and match case-insensitively on display_name or key
            const chStart = now();
            const channelsResp = await env.DB1.fetch("https://gr8r-db1-worker/db1/channels", {
            method: "GET",
            headers: {
                "Authorization": `Bearer ${db1Key}`
            }
            });

            if (!channelsResp.ok) {
            await safeLog(env, {
                level: "error",
                service: "channels",
                message: "channels fetch failed",
                meta: {
                    ...baseMeta({ request_id, route, method, origin }),
                    duration_ms: durationMs(chStart),
                    status_code: channelsResp.status,
                    ok: false,
                    reason: "channels_fetch_error"
                }
                });            throw new Error(`Channels fetch failed: ${channelsResp.status}`);
            }

            const chJson = await channelsResp.json(); // expected array: [{ id, key, display_name }, ...]
            // Build fast lookup map (lowercased)
            const nameToChannel = new Map();
            for (const c of Array.isArray(chJson) ? chJson : []) {
            if (c?.display_name) nameToChannel.set(String(c.display_name).trim().toLowerCase(), c);
            if (c?.key)          nameToChannel.set(String(c.key).trim().toLowerCase(), c);
            }

            const matched = [];
            const unmatched = [];
            for (const raw of channelsList) {
            const norm = String(raw).trim().toLowerCase();
            const hit = nameToChannel.get(norm) || null;
            if (hit) matched.push({ name: raw, channel_id: hit.id, key: hit.key, display_name: hit.display_name });
            else unmatched.push(raw);
            }
            publishing_requested = channelsList.length;
            publishing_matched = matched.length;
            publishing_unmatched = unmatched.length;

            // Log each unmatched as error and skip
            for (const name of unmatched) {
            await safeLog(env, {
                level: "error",
                service: "publishing",
                message: "channel not found; publishing skipped",
                meta: {
                    ...baseMeta({ request_id, route, method, origin }),
                    ok: false,
                    status_code: 404,
                    reason: "channel_not_found",
                    channel_name: name,
                    title: (title || "").slice(0, 120),
                    scheduled_at: scheduleDateTime || null
                }
                });            }

            // Insert a Publishing row per matched channel
            for (const m of matched) {
                const pubStart = now();
                const pubBody = sanitizeForDB1({
                    video_id: videoId || undefined,
                    title,
                    channel_id: m.channel_id,
                    channel_key: m.key, // REQUIRED by db1-worker
                    scheduled_at: scheduleDateTime,
                    status: "pending"
                });
                // TEMP: show what we're sending
                console.log("[videouploads-worker] Publishing payload:", JSON.stringify(pubBody)); //DEBUG

                const pubResp = await env.DB1.fetch("https://gr8r-db1-worker/db1/publishing", {
                    method: "POST",
                    headers: {
                    "Content-Type": "application/json",
                    "Authorization": `Bearer ${db1Key}`
                    },
                    body: JSON.stringify(pubBody)
                });

                if (!pubResp.ok) {
                // TEMP: capture DB1 response body
                let errText = "";
                try { errText = await pubResp.text(); } catch {}

                // Console for quick tail reading
                console.log("[videouploads-worker] Publishing response:", pubResp.status, errText.slice(0, 800));

                // Structured log (short snippet)
                await safeLog(env, {
                    level: "error",
                    service: "publishing",
                    message: "publishing upsert failed",
                    meta: {
                        ...baseMeta({ request_id, route, method, origin }),
                        duration_ms: durationMs(pubStart),
                        status_code: pubResp.status,
                        ok: false,
                        reason: "publishing_error",
                        title: (title || "").slice(0, 120),
                        channel_id: m.channel_id,
                        channel_name: m.name,
                        channel_key: m.key,
                        video_id: videoId ?? null,
                        scheduled_at: scheduleDateTime || null,
                        server_msg: errText.slice(0, 200)
                    }
                    });            continue;
                }
                publishing_inserted++;
                console.log("[publishing] upsert ok", {
                    channel_id: m.channel_id,
                    channel_key: m.key,
                    used_video_id: !!videoId
                });
           }
        }
        } catch (err) {
        await safeLog(env, {
            level: "error",
            service: "publishing",
            message: "publishing exception",
            meta: {
                ...baseMeta({ request_id, route, method, origin }),
                status_code: 500,
                ok: false,
                error: err.message,
                stack: err.stack
            }
            });        // do not rethrow; publishing is auxiliary to main video flow
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
        await safeLog(env, {
            level: "error",
            service: "revai-submit",
            message: "Revai submission failed",
            meta: {
                ...baseMeta({ request_id, route, method, origin }),
                duration_ms: durationMs(revStart),
                status_code: revaiResponse.status,
                ok: false,
                reason: "revai_error",
                title: title.slice(0, 120)
            }
            });        return new Response(JSON.stringify({
            error: "Rev.ai job failed",
            message: revaiJson
        }), {
            status: 502,
            headers: { "Content-Type": "application/json" }
        });
        }
        revai_ok = true;
        revai_job_id = revaiJson.id;
        console.log("[revai-submit] ok", {
            revai_job_id: revaiJson?.id,
            title: title?.slice(0,120)
        });

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
        await safeLog(env, {
            level: "error",
            service: "airtable-followup",
            message: "Airtable transcript failed",
            meta: {
                ...baseMeta({ request_id, route, method, origin }),
                duration_ms: durationMs(atFollowStart),
                status_code: atFollow.status,
                ok: false,
                reason: "airtable_error",
                revai_job_id: revaiJson.id,
                title: title.slice(0, 120)
            }
            });        
        } else {
            console.log("[airtable-followup] transcript ok", { revai_job_id: revaiJson?.id });
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
            await safeLog(env, {
                level: "error",
                service: "db1-followup",
                message: "DB1 transcript failed",
                meta: {
                ...baseMeta({ request_id, route, method, origin }),
                duration_ms: durationMs(db1FollowStart),
                status_code: db1FollowStatus,
                ok: false,
                reason: "db1_error",
                revai_job_id: revaiJson.id,
                title: title.slice(0, 120)
                }
            });            throw new Error(`DB1 transcript update failed: status ${db1FollowStatus}`);
        }

        console.log("[db1-followup] transcript ok", { revai_job_id: revaiJson?.id });

        } catch (err) {
        await safeLog(env, {
            level: "error",
            service: "db1-followup",
            message: "DB1 transcript exception",
            meta: {
                ...baseMeta({ request_id, route, method, origin }),
                status_code: 500,
                ok: false,
                error: err.message,
                stack: err.stack,
                title: title ? title.slice(0, 120) : null
            }
            });        throw err;
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

        await safeLog(env, {
            level: "info",
            service: "response",
            message: "request complete",
            meta: {
                ...baseMeta({ request_id, route, method, origin }),
                status_code: 200,
                ok: true,
                duration_ms: durationMs(reqStart),

                // domain summary
                title: title?.slice(0, 120) || null,
                video_type: videoType || null,
                scheduled_at: scheduleDateTime || null,

                r2_ok,
                airtable_ok,
                db1_ok,
                video_id: videoId ?? null,

                publishing: {
                requested: publishing_requested,
                matched: publishing_matched,
                unmatched: publishing_unmatched,
                inserted: publishing_inserted
                },

                revai_ok,
                revai_job_id
            }
            });

        return new Response(JSON.stringify(responseBody), {
          status: 200,
          headers: { "Content-Type": "application/json" }
        });

      } catch (err) {
        await safeLog(env, {
            level: "error",
            service: "response",
            message: "request failed",
            meta: {
            ...baseMeta({ request_id, route, method, origin }),
            status_code: 500,
            ok: false,
            error: err.message,
            stack: err.stack
            }
         });
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

    await safeLog(env, {
            level: "warn",
            service: "request",
            message: "forbidden route",
            meta: {
            ...baseMeta({ request_id, route, method, origin }),
            status_code: 403,
            ok: false,
            reason: "forbidden_route"
            }
        });        return new Response("Forbidden", { status: 403 });
  }
};
