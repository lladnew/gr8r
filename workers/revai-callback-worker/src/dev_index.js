// v1.4.2 gr8r-revai-callback-worker
// CHANGE: migrate to lib/grafana.js + lib/secrets.js logging; add request_id & timings; remove raw body/content logging
// CHANGE: standardize labels (service) & meta (snake_case); keep webhook 200 ACK semantics
// CHANGE: set next_status = 'Post Ready' on social copy success; 'Hold' on failure (unchanged behavior from 1.4.1)
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
// new getSecret function module
import { getSecret } from "../../../lib/secrets.js";

// new Grafana logging shared script
import { createLogger } from "../../../lib/grafana.js";
const log = createLogger({ source: "gr8r-revai-callback-worker" });

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (url.pathname === '/api/revai/callback' && request.method === 'POST') {
       // ADDED: request-scoped context
      const request_id = (globalThis.crypto?.randomUUID?.() ?? `${Date.now()}-${Math.random()}`);
      const route = '/api/revai/callback';
      const method = 'POST';
      const t0 = Date.now();

let rawBody, body;

// Step 1: Try to get raw text safely
try {
  rawBody = await request.clone().text();
  console.log('[revai-callback] Raw body successfully read'); // <== NEW marker
} catch (e) {
  await log(env, {
    service: "callback",
    level: "error",
    message: "failed to read request body",
    meta: { request_id, route, method, ok: false, status_code: 400, reason: "body_read_failed" }
  });

  return new Response('Body read failed', { status: 400 });
}

// Step 2: Try to parse JSON
try {
  body = JSON.parse(rawBody);
} catch (e) {
  await log(env, {
    service: "callback",
    level: "error",
    message: "failed to parse json",
    meta: { request_id, route, method, ok: false, status_code: 400, reason: "bad_json" }
  });

  return new Response('Bad JSON', { status: 400 });
}

      const job = body.job;
      if (!job || !job.id || !job.status) {
        await log(env, {
          service: "callback",
          level: "error",
          message: "missing required job fields",
          meta: { request_id, route, method, ok: false, status_code: 400, reason: "missing_job_fields" }
        });

        return new Response('Missing required job fields', { status: 400 });
      }

      const { id, status, metadata } = job;
      const title = metadata || 'Untitled';

      await log(env, {
        service: "callback",
        level: "debug",
        message: "rev.ai job completion received",
        meta: { request_id, route, method, ok: true, job_id: id, status, title }
      });

let fetchResp, fetchText, socialCopy; 
      if (status !== 'transcribed') {
        if (status === 'failed') {
          await log(env, {
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
        // Step 0: Check Airtable for existing record
        
        const checkResp = await env.AIRTABLE.fetch('https://internal/api/airtable/get', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            table: 'tblQKTuBRVrpJLmJp',
            matchField: 'Transcript ID',
            matchValue: id
          })
        });

       if (!checkResp.ok) {
  const errorText = await checkResp.text();
  await log(env, {
    service: "airtable-update",
    level: "error",
    message: "airtable check failed",
    meta: { request_id, job_id: id, ok: false, status_code: checkResp.status, duration_ms: aDur, reason: "airtable_check_failed" }
  });

  return new Response('Airtable check failed', { status: 200 });
}

const checkData = await checkResp.json();
const found = Array.isArray(checkData.records) && checkData.records.length > 0;

let alreadyDone = false;
if (found) {
  const f = checkData.records[0].fields || {};
  const processedStatuses = new Set([
    'Pending Schedule',
    'Hold',
    'Scheduled',
    'Published',
    'Transcription Complete'
  ]);

  // Consider processed if we’ve already written the transcript URL OR advanced status
  alreadyDone = Boolean(f['R2 Transcript URL']) || processedStatuses.has(f.Status);
}

if (alreadyDone) {
  await log(env, {
    service: "airtable-update",
    level: "info",
    message: "already processed, skipping",
    meta: { request_id, job_id: id, title, ok: true, status_code: 200, found: true, already_done: true }
  });

  return new Response(JSON.stringify({ success: false, reason: 'Already processed' }), { status: 200 });
}

// Step 1: Fetch transcript text (plain text)
try {
  fetchResp = await env.REVAIFETCH.fetch('https://internal/api/revai/fetch-transcript', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ job_id: id })
  });
  fetchText = await fetchResp.text();
  } catch (err) {
  console.error('[revai-callback] REVAIFETCH fetch error:', err.message);
  await log(env, {
    service: "revai-fetch",
    level: "error",
    message: "revai fetch exception",
    meta: { request_id, job_id: id, ok: false, duration_ms: fDur, reason: "revai_fetch_exception" }
  });

  return new Response('Failed to fetch transcript', { status: 200 });
}

if (!fetchResp.ok) {
  await log(env, {
    service: "revai-fetch",
    level: "error",
    message: "revai fetch error",
    meta: { request_id, job_id: id, ok: false, status_code: fetchResp.status, duration_ms: fDur, reason: "revai_fetch_bad_status" }
  });

  return new Response('Transcript fetch failed', { status: 200 });
}
// ADDED: mark failure if transcript is empty/blank
if (!fetchText || !fetchText.trim()) {
  await log(env, {
    service: "revai-fetch",
    level: "error",
    message: "transcript empty",
    meta: { request_id, job_id: id, ok: false, duration_ms: fDur, reason: "transcript_empty" }
  });

  socialCopyFailed = true;
}
  await log(env, {
    service: "revai-fetch",
    level: "info",
    message: "transcript fetched",
    meta: { request_id, job_id: id, ok: true, status_code: 200, duration_ms: fDur, transcript_len: fetchText.length }
  });
// Step 1.5: Generate Social Copy from transcript
try {
  const socialCopyResponse = await env.SOCIALCOPY_WORKER.fetch('https://internal/api/socialcopy', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ transcript: fetchText, title })
  });

  if (!socialCopyResponse.ok) {
    const errText = await socialCopyResponse.text();
    await log(env, {
      service: "socialcopy",
      level: "error",
      message: "socialcopy error",
      meta: { request_id, job_id: id, ok: false, status_code: socialCopyResponse.status, duration_ms: sDur, reason: "socialcopy_bad_status" }
    });

    socialCopyFailed = true; // ADDED
  } else {
    socialCopy = await socialCopyResponse.json();

    // ADDED: validate payload has at least one meaningful field
    const hasAny =
      (socialCopy?.hook && String(socialCopy.hook).trim()) ||
      (socialCopy?.body && String(socialCopy.body).trim()) ||
      (socialCopy?.cta && String(socialCopy.cta).trim()) ||
      (socialCopy?.hashtags && String(socialCopy.hashtags).trim());

    if (!hasAny) {
      await logToGrafana(env, 'error', 'SocialCopy worker returned empty content', {
        source: 'revai-callback-worker',
        title
      });
      socialCopyFailed = true; // ADDED
    } 
    const has_hook = !!(socialCopy?.hook && String(socialCopy.hook).trim());
    const has_body = !!(socialCopy?.body && String(socialCopy.body).trim());
    const has_cta  = !!(socialCopy?.cta  && String(socialCopy.cta).trim());
    const has_hashtags = !!(socialCopy?.hashtags && String(socialCopy.hashtags).trim());
    if (!(has_hook || has_body || has_cta || has_hashtags)) {
      await log(env, {
        service: "socialcopy",
        level: "error",
        message: "socialcopy empty",
        meta: { request_id, job_id: id, ok: false, duration_ms: sDur, reason: "socialcopy_empty" }
      });
      socialCopyFailed = true;
    } else {
      await log(env, {
        service: "socialcopy",
        level: "info",
        message: "social copy generated",
        meta: { request_id, job_id: id, ok: true, status_code: 200, duration_ms: sDur, has_hook, has_body, has_cta, has_hashtags }
      });
    }

  }
} catch (err) {
  await log(env, {
    service: "socialcopy",
    level: "error",
    message: "socialcopy exception",
    meta: { request_id, job_id: id, ok: false, duration_ms: sDur, reason: "socialcopy_exception" }
  });

  socialCopyFailed = true; // ADDED
}

       // Step 2: Upload transcript + Social Copy to R2
const sanitizedTitle = title.replace(/[^a-zA-Z0-9 _-]/g, "").replace(/\s+/g, "_");
const r2Key = `transcripts/${sanitizedTitle}.txt`;
const r2TranscriptUrl = 'https://videos.gr8r.com/' + r2Key;

let fullTextToUpload = fetchText;

// CHANGED: append social copy only if generated successfully
if (!socialCopyFailed && (socialCopy?.hook || socialCopy?.body || socialCopy?.cta || socialCopy?.hashtags)) {
  fullTextToUpload += `\n\n${socialCopy.hook || ''}\n${socialCopy.body || ''}\n${socialCopy.cta || ''}\n\n${socialCopy.hashtags || ''}`.trimEnd();
}

try {
  await env.VIDEO_BUCKET.put(r2Key, fullTextToUpload, {
    httpMetadata: { contentType: 'text/plain' }
  });

  await log(env, {
    service: "r2-upload",
    level: "info",
    message: "r2 upload complete",
    meta: { request_id, job_id: id, ok: true, duration_ms: rDur, r2_key: r2Key, has_social_copy: !socialCopyFailed }
  });

} catch (err) {
  throw new Error(`R2 upload failed: ${err.message}`);
}
// Step 2.5: Upsert to DB1
const nextStatus = socialCopyFailed ? 'Hold' : 'Post Ready'; // ADDED
const db1Body = sanitizeForDB1({
  title,
  transcript_id: id,
  r2_transcript_url: r2TranscriptUrl,
  status: nextStatus, // CHANGED
  ...( !socialCopyFailed && socialCopy?.hook && { social_copy_hook: socialCopy.hook } ),
  ...( !socialCopyFailed && socialCopy?.body && { social_copy_body: socialCopy.body } ),
  ...( !socialCopyFailed && socialCopy?.cta && {  social_copy_cta:  socialCopy.cta } ),
  ...( !socialCopyFailed && socialCopy?.hashtags && {
    hashtags: Array.isArray(socialCopy.hashtags)
      ? socialCopy.hashtags.join(' ')
      : socialCopy.hashtags
  })
});

const db1Key = await getDB1InternalKey(env);

const db1Resp = await env.DB1.fetch('https://gr8r-db1-worker/db1/videos', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${db1Key}`
  },
  body: JSON.stringify(db1Body)
});

const db1Text = await db1Resp.text();
let db1Data;
try {
  db1Data = JSON.parse(db1Text);
} catch {
  db1Data = { raw: db1Text };
}

if (!db1Resp.ok) {
  await log(env, {
    service: "db1-upsert",
    level: "error",
    message: "db1 upsert failed",
    meta: { request_id, job_id: id, ok: false, status_code: db1Resp.status, duration_ms: dDur, next_status: nextStatus, title, reason: "db1_upsert_failed" }
  });

  throw new Error(`DB1 update failed: ${db1Text}`);
}

const db1_action = (() => { try { const j = JSON.parse(db1Text); return typeof j?.action === 'string' ? j.action : undefined; } catch { return undefined; } })();
await log(env, {
  service: "db1-upsert",
  level: "info",
  message: "db1 upsert ok",
  meta: { request_id, job_id: id, ok: true, status_code: db1Resp.status, duration_ms: dDur, next_status: nextStatus, title, ...(db1_action ? { db1_action } : {}) }
});

       
        // Step 3: Update Airtable
        const airtableResp = await env.AIRTABLE.fetch('https://internal/api/airtable/update', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            table: 'tblQKTuBRVrpJLmJp',
            matchField: 'Transcript ID',
            matchValue: id,
            fields: {
              'R2 Transcript URL': r2TranscriptUrl,
              Status: nextStatus, // CHANGED
              ...( !socialCopyFailed && socialCopy?.hook && { 'Social Copy Hook': socialCopy.hook } ),
              ...( !socialCopyFailed && socialCopy?.body && { 'Social Copy Body': socialCopy.body } ),
              ...( !socialCopyFailed && socialCopy?.cta && { 'Social Copy Call to Action': socialCopy.cta } ),
              ...( !socialCopyFailed && socialCopy?.hashtags && { Hashtags: socialCopy.hashtags })
            }
          })
        });

if (!airtableResp.ok) {
  const errorText = await airtableResp.text();
  console.error('[revai-callback] Airtable update failed:', airtableResp.status, errorText);
  await log(env, {
    service: "airtable-update",
    level: "error",
    message: "airtable update failed",
    meta: { request_id, job_id: id, ok: false, status_code: airtableResp.status, duration_ms: atDur, title, next_status: nextStatus, reason: "airtable_update_failed" }
  });

  throw new Error(`Airtable update failed: ${airtableResp.status} - ${errorText}`);
}

await log(env, {
  service: "airtable-update",
  level: "info",
  message: "airtable update ok",
  meta: { request_id, job_id: id, ok: true, status_code: airtableResp.status, duration_ms: atDur, title, next_status: nextStatus }
});

const total_duration_ms = Date.now() - t0;
await log(env, {
  service: "callback",
  level: "info",
  message: "transcript and social copy complete",
  meta: { request_id, job_id: id, title, ok: true, status_code: 200, social_copy_failed: socialCopyFailed, next_status: nextStatus, total_duration_ms }
});

        return new Response(JSON.stringify({ success: false }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' }
        });
      }
    }

    return new Response('Not found', { status: 404 });
  }
};

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