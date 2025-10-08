// v1.4.6 gr8r-revai-callback-worker  EDIT: tweaked grafana logging success message to include video title
// v1.4.5 gr8r-revai-callback-worker ADDED: function db1ErrorWriteback() to set videos_status to 'Error' if this worker fails for any reason, removed late video_id check
// v1.4.4 gr8r-revai-callback-worker ADDED: update to publishing table to mark rows 'queued' and ready for publishing, changed logging to safeLog to keep worker from crashing on log issues
// REMOVED: text parsing of callback in favor of json only
// REPLACED: grafana success logs with console only versions
// ADDED: halt for missing video and transcript ID match and error logging for that
// v1.4.3 gr8r-revai-callback-worker
// CHANGE: migrate to lib/grafana.js + lib/secrets.js logging; add request_id & timings; remove raw body/content logging
// CHANGE: standardize labels (service) & meta (snake_case); keep webhook 200 ACK semantics
// CHANGE: set next_status = 'Post Ready' on social copy success; 'Hold' on failure (unchanged behavior from 1.4.1)
// v1.4.2 gr8r-revai-callback-worker EDIT: ChatGPT proactively changed my status update to a variable (nextStatus) for error handling and used with Airtable which broke... correcting Airtable update to Pending Schedule
// v1.4.1 gr8r-revai-callback-worker CHANGE: changed DB1 UPSERT from 'Pending Schedule' to 'Post Ready' after successful Social Copy
// v1.4.0 gr8r-revai-callback-worker FIXED: proper acknowledgement of rev.ai callback even if another error like no transcript
// v1.3.0 gr8r-revai-callback-worker On ANY Social Copy failure status set to 'Hold' instead of 'Pending Schedule'
// v1.2.9 gr8r-revai-callback-worker adding DB1 UPSERT capabilities
// v1.2.8 gr8r-revai-callback-worker
// removing r2_Transcript_Url from line 164 and adding const at line 274
// v1.2.7 gr8r-revai-callback-worker
// line 206 moved to line 162... variables must be outside try block added r2_Transcript_Url variable
// v1.2.6 gr8r-revai-callback-worker
// Updated line 204 to add socialCopy as a variable to be reused
// Updated line 251 removing const strict variable definition
// Changed Airtable Status to Pending Schedule line 303
// v1.2.5 gr8r-revai-callback-worker
// Updated R2 text upload to include Social Copy
// Updated Airtable update to include Social Copy
// v1.2.4 gr8r-revai-callback-worker
// added try { const socialCopyResponse = await... for fetching Social Copy from OpenAi via the socialcopy-worker - logging only for now to view output
// v1.2.3 gr8r-revai-callback-worker
// added sanitizing for R2 transcript URL title
// v1.2.2 gr8r-revai-callback-worker
// adding airtable error logging at line 255
// v1.2.1 gr8r-revai-callback-worker
// undoing eroneous "fix" in 1.1.9
// v1.2.0 gr8r-revai-callback-worker
// Removed "env.ASSETS.fetch('r2/put') and replaced with direct evn.VIDEO_BUCKET.put(...)

// Shared libs
// getSecret function module
import { getSecret } from "../../../lib/secrets.js";

// Grafana logging shared script
import { createLogger } from "../../../lib/grafana.js";
// Safe logger: always use this; caches underlying logger internally
let _logger;
const safeLog = async (env, entry) => {
  try {
    _logger = _logger || createLogger({ source: "gr8r-revai-callback-worker" });
    await _logger(env, entry);
  } catch (e) {
    // Never throw from logging
    console.log('LOG_FAIL', entry?.service || 'unknown', e?.stack || e?.message || e);
  }
};

export default {
  async fetch(request, env, ctx) {
    let video_id;                 // set in Step 0 (DB1 videos check)
    let vSuccessMeta = null;      // capture videos-upsert success metrics for later
    let pSummary = null;          // capture publishing summary for consolidated success
    let title; // set from DB1 record (ignore rev.ai metadata)
    let hadPublishingFailure = false;
    let db1Key; // used across normal flow and global catch


    const url = new URL(request.url);
    if (url.pathname === '/api/revai/callback' && request.method === 'POST') {
      // ADDED: request-scoped context
      const request_id = (globalThis.crypto?.randomUUID?.() ?? `${Date.now()}-${Math.random()}`);
      const route = '/api/revai/callback';
      const method = 'POST';
      const t0 = Date.now();

      let body;
      try {
        body = await request.json();
      } catch (e) {
        await safeLog(env, {
          service: "callback",
          level: "error",
          message: "failed to parse json",
          meta: { request_id, route, method, ok: false, status_code: 400, reason: "bad_json" }
        });
        
        return new Response('Bad JSON', { status: 400 });
      }

      const job = body.job;
      if (!job || !job.id || !job.status) {
        await safeLog(env, {
          service: "callback",
          level: "error",
          message: "missing required job fields",
          meta: { request_id, route, method, ok: false, status_code: 400, reason: "missing_job_fields" }
        });
        
        return new Response('Missing required job fields', { status: 400 });
      }

      const { id, status } = job;
      // Title will be sourced from DB1 after Step 0

      await safeLog(env, {
        service: "callback",
        level: "debug",
        message: `rev.ai job completed: ${id}`,
        meta: { request_id, route, method, ok: true, job_id: id, status }
      });

      let fetchResp, fetchText, socialCopy;
      if (status !== 'transcribed') {
        if (status === 'failed') {
          await safeLog(env, {
            service: "callback",
            level: "error",
            message: "rev.ai job failed before transcription",
            meta: { request_id, route, method, ok: false, status_code: 200, job_id: id, status, reason: "revai_failed_status" }
          });
        }
        return new Response('Callback ignored: status not transcribed', { status: 200 });
      }

      let socialCopyFailed = false;

      try {
        // Step 0: Check DB1 for existing video by transcript_id; capture video_id and dedupe
        db1Key = await getDB1InternalKey(env);
        const v0 = Date.now();

        const videosCheckResp = await env.DB1.fetch(
          `https://gr8r-db1-worker/db1/videos?transcript_id=${encodeURIComponent(id)}`,
          { method: 'GET', headers: { 'Authorization': `Bearer ${db1Key}` } }
        );
        const vDur = Date.now() - v0;

        if (!videosCheckResp.ok) {
          await safeLog(env, {
            service: "db1-videos-check",
            level: "error",
            message: "db1 videos check failed",
            meta: { request_id, job_id: id, ok: false, status_code: videosCheckResp.status, duration_ms: vDur, reason: "db1_videos_check_failed" }
          });
          // Acknowledge webhook so rev.ai doesn't spam retries
          return new Response('DB1 videos check failed', { status: 200 });
        }

        const videosPayload = await videosCheckResp.json();
        // tolerate either {data: [...] } or plain array
        const videosArr = Array.isArray(videosPayload?.data) ? videosPayload.data
          : Array.isArray(videosPayload) ? videosPayload
            : [];
        const found = videosArr.length > 0;

        if (!found) {
          await safeLog(env, {
            service: "db1-videos-check",
            level: "error",
            message: "no video found with this transcript_id",
            meta: { request_id, job_id: id, ok: false, reason: "no_video_for_transcript_id" }
          });
          return new Response('no video found with this transcript_id', { status: 200 });
        }

        const rec = videosArr[0] || {};
        video_id = Number(rec.id);

        // Source of truth for Title comes from DB1
        title = (typeof rec.title === 'string' ? rec.title.trim() : '') || '';

        if (!title) {
          await safeLog(env, {
            service: "db1-videos-check",
            level: "error",
            message: "missing title for transcript_id",
            meta: { request_id, job_id: id, ok: false, ...(video_id ? { video_id } : {}), reason: "missing_db1_title" }
          });
          return new Response('missing DB1 title', { status: 200 });
        }

        // Dedupe: only if status is Post Ready
        const alreadyDone = rec.status === 'Post Ready';

        if (alreadyDone) {
          await safeLog(env, {
            service: "db1-videos-check",
            level: "info",
            message: "already processed, skipping",
            meta: { request_id, job_id: id, ok: true, found: true, already_done: true, video_id, status: rec.status }
          });
          return new Response(JSON.stringify({ success: false, reason: 'Already processed' }), { status: 200 });
        }


        let fDur; // ADDED: will be set after the fetch
        let f0; // ADDED: start time visible to catch

        // Step 1: Fetch transcript text (plain text)
        try {
          f0 = Date.now();

          fetchResp = await env.REVAIFETCH.fetch('https://internal/api/revai/fetch-transcript', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ job_id: id })
          });
          fetchText = await fetchResp.text();
          fDur = Date.now() - f0; // CHANGED: assign to outer let
        } catch (err) {
          fDur = Date.now() - f0; // CHANGED: assign to outer let
          await safeLog(env, {
            service: "revai-fetch",
            level: "error",
            message: "revai fetch exception",
            meta: { request_id, job_id: id, ok: false, duration_ms: fDur, reason: "revai_fetch_exception" }
          });
          await db1ErrorWriteback(env, db1Key, video_id, request_id, id);
          return new Response('Failed to fetch transcript', { status: 200 });
        }

        if (!fetchResp.ok) {
          await safeLog(env, {
            service: "revai-fetch",
            level: "error",
            message: "revai fetch error",
            meta: { request_id, job_id: id, ok: false, status_code: fetchResp.status, duration_ms: fDur, reason: "revai_fetch_bad_status" }
          });
          await db1ErrorWriteback(env, db1Key, video_id, request_id, id);
          return new Response('Transcript fetch failed', { status: 200 });
        }
      if (!fetchText || !fetchText.trim()) {
        await safeLog(env, {
          service: "revai-fetch",
          level: "error",
          message: "transcript empty",
          meta: { request_id, job_id: id, ok: false, duration_ms: fDur, reason: "transcript_empty" }
        });
        await db1ErrorWriteback(env, db1Key, video_id, request_id, id);
        return new Response('Transcript empty', { status: 200 }); // stop early per Step 4
      } else {
        // success grafana log removed
        console.log('[revai-callback] transcript fetched', {
          request_id,
          job_id: id,
          duration_ms: fDur,
          transcript_len: (fetchText?.length ?? 0)
        });
      }

        // Step 1.5: Generate Social Copy from transcript
        if (fetchText && fetchText.trim()) {
          let sDur;
          const s0 = Date.now();
          try {
            const socialCopyResponse = await env.SOCIALCOPY_WORKER.fetch('https://internal/api/socialcopy', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ transcript: fetchText, title })
            });

            if (!socialCopyResponse.ok) {
              sDur = Date.now() - s0;
              await safeLog(env, {
                service: "socialcopy",
                level: "error",
                message: "socialcopy error",
                meta: { request_id, job_id: id, ok: false, status_code: socialCopyResponse.status, duration_ms: sDur, reason: "socialcopy_bad_status" }
              });
              socialCopyFailed = true;
            } else {
              socialCopy = await socialCopyResponse.json();
              sDur = Date.now() - s0;

              const has_hook = !!(socialCopy?.hook && String(socialCopy.hook).trim());
              const has_body = !!(socialCopy?.body && String(socialCopy.body).trim());
              const has_cta = !!(socialCopy?.cta && String(socialCopy.cta).trim());
              const has_hashtags = !!(socialCopy?.hashtags && String(socialCopy.hashtags).trim());

              if (!(has_hook || has_body || has_cta || has_hashtags)) {
                await safeLog(env, {
                  service: "socialcopy",
                  level: "error",
                  message: "socialcopy empty",
                  meta: { request_id, job_id: id, ok: false, duration_ms: sDur, reason: "socialcopy_empty" }
                });
                socialCopyFailed = true;
              } else {
                // success grafana log removed.
                console.log('[revai-callback] social copy generated', {
                  request_id,
                  job_id: id,
                  duration_ms: sDur,
                  has_hook,
                  has_body,
                  has_cta,
                  has_hashtags
                });

              }
            }
          } catch (err) {
            sDur = Date.now() - s0;
            await safeLog(env, {
              service: "socialcopy",
              level: "error",
              message: "socialcopy exception",
              meta: { request_id, job_id: id, ok: false, duration_ms: sDur, reason: "socialcopy_exception" }
            });
            socialCopyFailed = true;
          }
        } // end: only run SocialCopy when transcript present

        // Step 2: Upload transcript + Social Copy to R2
        const sanitizedTitle = title.replace(/[^a-zA-Z0-9 _-]/g, "").replace(/\s+/g, "_");
        const r2Key = `transcripts/${sanitizedTitle}.txt`;
        const r2TranscriptUrl = 'https://videos.gr8r.com/' + r2Key;

        let fullTextToUpload = fetchText || '';

        // CHANGED: append social copy only if generated successfully
        if (!socialCopyFailed && (socialCopy?.hook || socialCopy?.body || socialCopy?.cta || socialCopy?.hashtags)) {
          fullTextToUpload += `\n\n${socialCopy.hook || ''}\n${socialCopy.body || ''}\n${socialCopy.cta || ''}\n\n${socialCopy.hashtags || ''}`.trimEnd();
        }

        let rDur; // ADDED
        let r0;   // ADDED
        try {
          r0 = Date.now(); // CHANGED

          await env.VIDEO_BUCKET.put(r2Key, fullTextToUpload, {
            httpMetadata: { contentType: 'text/plain' }
          });
          rDur = Date.now() - r0; // CHANGED: compute duration
          // success grafana log removed
          console.log('[revai-callback] r2 upload complete', {
            request_id,
            job_id: id,
            duration_ms: rDur,
            r2_key: r2Key,
            has_social_copy: !socialCopyFailed
          });

        } catch (err) {
          rDur = Date.now() - r0; // CHANGED: compute duration
          await safeLog(env, {
            service: "r2-upload",
            level: "error",
            message: "r2 upload failed",
            meta: { request_id, job_id: id, ok: false, duration_ms: rDur, r2_key: r2Key, reason: "r2_upload_failed" }
          });
          await db1ErrorWriteback(env, db1Key, video_id, request_id, id);
          throw new Error(`R2 upload failed: ${err.message}`);
        }

        // Step 2.5: Upsert to DB1
        const nextStatus = socialCopyFailed ? 'Error' : 'Post Ready'; // ADDED
        const db1Body = sanitizeForDB1({
          title,
          transcript_id: id,
          r2_transcript_url: r2TranscriptUrl,
          status: nextStatus, // CHANGED
          ...(!socialCopyFailed && socialCopy?.hook && { social_copy_hook: socialCopy.hook }),
          ...(!socialCopyFailed && socialCopy?.body && { social_copy_body: socialCopy.body }),
          ...(!socialCopyFailed && socialCopy?.cta && { social_copy_cta: socialCopy.cta }),
          ...(!socialCopyFailed && socialCopy?.hashtags && {
            hashtags: Array.isArray(socialCopy.hashtags)
              ? socialCopy.hashtags.join(' ')
              : socialCopy.hashtags
          })
        });

        const d0 = Date.now();
        const db1Resp = await env.DB1.fetch('https://gr8r-db1-worker/db1/videos', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${db1Key}`
          },
          body: JSON.stringify(db1Body)
        });

        const db1Text = await db1Resp.text();
        const dDur = Date.now() - d0;

        if (!db1Resp.ok) {
          await safeLog(env, {
            service: "db1-upsert",
            level: "error",
            message: "db1 upsert failed",
            meta: { request_id, job_id: id, ok: false, status_code: db1Resp.status, duration_ms: dDur, next_status: nextStatus, title, reason: "db1_upsert_failed" }
          });

          throw new Error(`DB1 update failed: ${db1Text}`);
        }

        const db1_action = (() => { try { const j = JSON.parse(db1Text); return typeof j?.action === 'string' ? j.action : undefined; } catch { return undefined; } })();

        vSuccessMeta = {
          status_code: db1Resp.status,
          duration_ms: dDur,
          next_status: nextStatus,
          ...(db1_action ? { db1_action } : {}),
          ...(video_id ? { video_id } : {})
        };

        // Step 2.6: When the video becomes Post Ready, queue all associated Publishing rows
        if (nextStatus === 'Post Ready') {
            // 1) Fetch all Publishing rows for this video
            const pListStart = Date.now();
            const pubListResp = await env.DB1.fetch(
              `https://gr8r-db1-worker/db1/publishing?video_id=${encodeURIComponent(video_id)}`,
              { method: 'GET', headers: { 'Authorization': `Bearer ${db1Key}` } }
            );
            const pListDur = Date.now() - pListStart;

            if (!pubListResp.ok) {
              await safeLog(env, {
                service: "publishing-upsert",
                level: "error",
                message: "error queueing publishing list",
                meta: { request_id, job_id: id, title, reason: "publishing_list_bad_status"  }
              });
              await db1ErrorWriteback(env, db1Key, video_id, request_id, id);
              // continue; do not throw
            } else {
              const publishingPayload = await pubListResp.json();
              const rows = Array.isArray(publishingPayload?.data)
                ? publishingPayload.data
                : (Array.isArray(publishingPayload) ? publishingPayload : []);

              if (!rows.length) {
                // Quiet success summary (no log noise); consolidated success below will still fire
                pSummary = {
                  count_total: 0,
                  count_ok: 0,
                  count_failed: 0,
                  duration_ms_list: pListDur,
                  duration_ms_upserts: 0
                };
              } else {
                // 2) Upsert status:"queued" for each (video_id, channel_key)
                const upStart = Date.now();
                const reqs = rows.map(r =>
                  env.DB1.fetch('https://gr8r-db1-worker/db1/publishing', {
                    method: 'POST',
                    headers: {
                      'Content-Type': 'application/json',
                      'Authorization': `Bearer ${db1Key}`
                    },
                    body: JSON.stringify({
                      video_id,
                      channel_key: r.channel_key,
                      status: "queued"
                    })
                  })
                );

                const settled = await Promise.allSettled(reqs);
                let okCount = 0;
                const failures = [];

                for (let i = 0; i < settled.length; i++) {
                  const res = settled[i];
                  const ch = rows[i]?.channel_key;
                  if (res.status === 'fulfilled' && res.value?.ok) {
                    okCount++;
                  } else {
                    let status_code = 'fetch_error';
                    let body_snippet = '';
                    if (res.status === 'fulfilled') {
                      status_code = res.value.status;
                      try { body_snippet = (await res.value.text()).slice(0, 200); } catch { }
                    }
                    failures.push({ channel_key: ch, status_code, body_snippet });
                  }
                }

                const upDur = Date.now() - upStart;
                pSummary = {
                  count_total: rows.length,
                  count_ok: okCount,
                  count_failed: rows.length - okCount,
                  duration_ms_list: pListDur,
                  duration_ms_upserts: upDur
                };
                hadPublishingFailure = pSummary.count_failed > 0;

                if (hadPublishingFailure) {
                  await safeLog(env, {
                    service: "publishing-upsert",
                    level: "error",
                    message: "publishing upserts had failures",
                    meta: {
                      request_id, job_id: id, video_id, title,
                      duration_ms: upDur, count_total: rows.length,
                      count_ok: okCount, count_failed: pSummary.count_failed,
                      failures
                    }
                  });
                  await db1ErrorWriteback(env, db1Key, video_id, request_id, id);
                }
              }
            
          }
        }

        // Consolidated success (videos + publishing) — only if both succeeded
        if (nextStatus === 'Post Ready' && vSuccessMeta && pSummary && !hadPublishingFailure) {
          // success grafana log removed
          console.log('[revai-callback] videos & publishing updates ok', {
            request_id,
            job_id: id,
            title,
            video_id,
            videos: vSuccessMeta,   // { status_code, duration_ms, next_status, db1_action?, video_id? }
            publishing: pSummary    // { count_total, count_ok, count_failed, duration_ms_list, duration_ms_upserts }
          });
        }

        // Step 3: Update Airtable
        const at0 = Date.now();

        const airtableStatus = socialCopyFailed ? 'Hold' : 'Pending Schedule';
        const airtableResp = await env.AIRTABLE.fetch('https://internal/api/airtable/update', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            table: 'tblQKTuBRVrpJLmJp',
            matchField: 'Transcript ID',
            matchValue: id,
            fields: {
              'R2 Transcript URL': r2TranscriptUrl,
              Status: airtableStatus, // CHANGED: decouple Airtable from DB1
              ...(!socialCopyFailed && socialCopy?.hook && { 'Social Copy Hook': socialCopy.hook }),
              ...(!socialCopyFailed && socialCopy?.body && { 'Social Copy Body': socialCopy.body }),
              ...(!socialCopyFailed && socialCopy?.cta && { 'Social Copy Call to Action': socialCopy.cta }),
              ...(!socialCopyFailed && socialCopy?.hashtags && { Hashtags: socialCopy.hashtags })
            }
          })
        });
        const atDur = Date.now() - at0;

        if (!airtableResp.ok) {
          const errorText = await airtableResp.text();
          await safeLog(env, {
            service: "airtable-update",
            level: "error",
            message: "airtable update failed",
            meta: { request_id, job_id: id, ok: false, status_code: airtableResp.status, duration_ms: atDur, title, next_status: nextStatus, reason: "airtable_update_failed" }
          });

          throw new Error(`Airtable update failed: ${airtableResp.status} - ${errorText}`);
        }

        // success grafana log removed.
        console.log('[revai-callback] airtable update ok', {
          request_id,
          job_id: id,
          duration_ms: atDur,
          status_code: airtableResp.status,
          title,
          next_status: nextStatus
        });


        const total_duration_ms = Date.now() - t0;
        await safeLog(env, {
          service: "callback",
          level: "info",
          message: `Transcript and SC complete: ${title}`,
          meta: {
            request_id, route, method,
            job_id: id, title,
            ok: true, status_code: 200,
            social_copy_failed: socialCopyFailed,
            next_status: nextStatus,
            total_duration_ms,
            ...(vSuccessMeta ? { videos: vSuccessMeta } : {}),
            ...(pSummary ? { publishing: pSummary } : {})
          }
        });


        return new Response(JSON.stringify({ success: true }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' }
        });
      } catch (err) { // ADDED: big catch for the whole business logic
        const total_duration_ms = Date.now() - t0;
        if (video_id && db1Key) {
          await db1ErrorWriteback(env, db1Key, video_id, request_id, id);
        }
        await safeLog(env, {
          service: "callback",
          level: "error",
          message: "callback processing error",
          meta: { request_id, route, method, ok: false, status_code: 200, reason: "callback_processing_error", total_duration_ms }
        });
        return new Response(JSON.stringify({ success: false }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' }
        });
      } // <-- closes the big try/catch

    } // <-- closes: if (url.pathname === ...)

    return new Response('Not found', { status: 404 });
  } // <-- closes: async fetch

}; // <-- closes: export default

function sanitizeForDB1(obj) {
  // Remove only undefined, null, and empty strings. Keep Dates and 0/false.
  const out = {};
  for (const [k, v] of Object.entries(obj)) {
    if (v === undefined || v === null) continue;
    if (typeof v === 'string' && v.trim() === '') continue;
    out[k] = v;
  }
  return out;
}

async function getDB1InternalKey(env) {
  const key = await getSecret(env, "DB1_INTERNAL_KEY");
  if (!key) throw new Error('DB1_INTERNAL_KEY empty after resolution');
  return key;
}

async function db1ErrorWriteback(env, db1Key, video_id, request_id, job_id) {
  if (!db1Key || !video_id) return; // only after Step 0
  try {
    const resp = await env.DB1.fetch('https://gr8r-db1-worker/db1/videos', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${db1Key}`
      },
      body: JSON.stringify({ id: video_id, status: 'Error' })
    });
    if (!resp.ok) {
      const t = await resp.text().catch(() => '');
      await safeLog(env, {
        service: "db1-error-writeback",
        level: "error",
        message: "failed to set DB1 status Error",
        meta: { request_id, job_id, video_id, status_code: resp.status, body_snippet: t.slice(0, 200) }
      });
    }
  } catch (e) {
    await safeLog(env, {
      service: "db1-error-writeback",
      level: "error",
      message: "exception during DB1 Error writeback",
      meta: { request_id, job_id, video_id, reason: "writeback_exception" }
    });
  }
}
