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

console.log('[revai-callback] Worker loaded'); // Logs when the Worker is initialized (cold start)
// cache for DB1 internal key
let CACHED_DB1_INTERNAL_KEY = null;

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (url.pathname === '/api/revai/callback' && request.method === 'POST') {
      await logToGrafana(env, 'debug', 'Revai-callback triggered');

let rawBody, body;

// Step 1: Try to get raw text safely
try {
  rawBody = await request.clone().text();
  console.log('[revai-callback] Raw body successfully read'); // <== NEW marker
  console.log('[revai-callback] Raw body:', rawBody);
} catch (e) {
  await logToGrafana(env, 'error', 'Failed to read raw body', {
    error: e.message
  });
  return new Response('Body read failed', { status: 400 });
}

// Step 2: Try to parse JSON
try {
  body = JSON.parse(rawBody);
} catch (e) {
  await logToGrafana(env, 'error', 'Failed to parse JSON body', {
    rawBody,
    error: e.message
  });
  return new Response('Bad JSON', { status: 400 });
}

      const job = body.job;
      if (!job || !job.id || !job.status) {
        return new Response('Missing required job fields', { status: 400 });
      }

      const { id, status, metadata } = job;
      const title = metadata || 'Untitled';

      await logToGrafana(env, 'debug', 'Parsed callback body', {
        id,
        status,
        title
      });
let fetchResp, fetchText, socialCopy; 
      if (status !== 'transcribed') {
        return new Response('Callback ignored: status not transcribed', { status: 200 });
      }

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
  await logToGrafana(env, 'error', 'Airtable fetch failed', {
    job_id: id,
    status: checkResp.status,
    response: errorText
  });
  return new Response('Airtable check failed', { status: 200 });
}

const checkData = await checkResp.json();
const found = Array.isArray(checkData.records) && checkData.records.length > 0;
const alreadyDone = found && checkData.records[0].fields?.Status === 'Transcription Complete';

if (alreadyDone) {
  await logToGrafana(env, 'info', 'Transcript already processed, skipping', {
    job_id: id,
    title
  });
  return new Response(JSON.stringify({ success: false, reason: 'Already complete' }), { status: 200 });
}

// Step 1: Fetch transcript text (plain text)
await logToGrafana(env, 'debug', 'Sending request to REVAIFETCH', {
  job_id: id,
  fetch_payload: { job_id: id }
});

try {
  fetchResp = await env.REVAIFETCH.fetch('https://internal/api/revai/fetch-transcript', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ job_id: id })
  });
  fetchText = await fetchResp.text();
  } catch (err) {
  console.error('[revai-callback] REVAIFETCH fetch error:', err.message);
  await logToGrafana(env, 'error', 'REVAIFETCH fetch threw error', {
    job_id: id,
    error: err.message
  });
  return new Response('Failed to fetch transcript', { status: 200 });
}

if (!fetchResp.ok) {
  await logToGrafana(env, 'error', 'REVAIFETCH returned error response', {
    job_id: id,
    status: fetchResp.status,
    text: fetchText
  });
  return new Response('Transcript fetch failed', { status: 200 });
}
// Step 1.5: Generate Social Copy from transcript
try {
  const socialCopyResponse = await env.SOCIALCOPY_WORKER.fetch('https://internal/api/socialcopy', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ transcript: fetchText, title })
  });

  if (!socialCopyResponse.ok) {
    const errText = await socialCopyResponse.text();
    await logToGrafana(env, 'error', 'SocialCopy worker failed', {
      status: socialCopyResponse.status,
      response: errText,
      source: 'revai-callback-worker'
    });
  } else {
    socialCopy = await socialCopyResponse.json();
    console.log('[revai-callback] ✅ Social Copy generated:', JSON.stringify(socialCopy, null, 2));
    await logToGrafana(env, 'info', 'Received Social Copy from worker', {
      ...socialCopy,
      source: 'revai-callback-worker',
      title
    });
  }
} catch (err) {
  console.error('[revai-callback] 💥 Exception while calling SocialCopy worker:', err);
  await logToGrafana(env, 'error', 'Exception while calling SocialCopy worker', {
    message: err.message,
    stack: err.stack,
    source: 'revai-callback-worker',
    title
  });
}
       // Step 2: Upload transcript + Social Copy to R2
const sanitizedTitle = title.replace(/[^a-zA-Z0-9 _-]/g, "").replace(/\s+/g, "_");
const r2Key = `transcripts/${sanitizedTitle}.txt`;
const r2TranscriptUrl = 'https://videos.gr8r.com/' + r2Key;

let fullTextToUpload = fetchText;

// Append social copy if available
if (socialCopy?.hook || socialCopy?.body || socialCopy?.cta || socialCopy?.hashtags) {
  fullTextToUpload += `\n\n${socialCopy.hook || ''}\n${socialCopy.body || ''}\n${socialCopy.cta || ''}\n\n${socialCopy.hashtags || ''}`.trimEnd();
}

await logToGrafana(env, 'debug', 'Uploading transcript + social copy to R2', {
  r2_key: r2Key,
  has_social_copy: !!socialCopy
});

try {
  await env.VIDEO_BUCKET.put(r2Key, fullTextToUpload, {
    httpMetadata: { contentType: 'text/plain' }
  });
} catch (err) {
  throw new Error(`R2 upload failed: ${err.message}`);
}
// Step 2.5: Upsert to DB1 (mirror Airtable fields)
await logToGrafana(env, 'debug', 'Upserting DB1 record (revai-callback)', {
  title,
  job_id: id,
  r2_transcript_url: r2TranscriptUrl
});

const db1Body = sanitizeForDB1({
  title,
  transcript_id: id,
  r2_transcript_url: r2TranscriptUrl,
  status: 'Pending Schedule',
  ...(socialCopy?.hook && { social_copy_hook: socialCopy.hook }),
  ...(socialCopy?.body && { social_copy_body: socialCopy.body }),
  ...(socialCopy?.cta && {  social_copy_cta:  socialCopy.cta }),
  ...(socialCopy?.hashtags && {
    hashtags: Array.isArray(socialCopy.hashtags)
      ? socialCopy.hashtags.join(' ')
      : socialCopy.hashtags
  })
});

const db1Key = await getDB1InternalKey(env);

// TEMP DEBUG: log the full key we are sending
await logToGrafana(env, 'debug', 'DB1 key (sender) FULL', {
  key: db1Key,
  header: `Bearer ${db1Key}`
});

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
  await logToGrafana(env, 'error', 'DB1 video upsert failed (revai-callback)', {
    title,
    job_id: id,
    db1Status: db1Resp.status,
    db1ResponseText: db1Text
  });
  throw new Error(`DB1 update failed: ${db1Text}`);
}

await logToGrafana(env, 'info', 'DB1 update successful (revai-callback)', {
  title,
  job_id: id,
  db1Response: db1Data
});
       
        // Step 3: Update Airtable
        await logToGrafana(env, 'debug', 'Updating Airtable record', { title, job_id: id });

        const airtableResp = await env.AIRTABLE.fetch('https://internal/api/airtable/update', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            table: 'tblQKTuBRVrpJLmJp',
            matchField: 'Transcript ID',
            matchValue: id,
            fields: {
  'R2 Transcript URL': r2TranscriptUrl,
  Status: 'Pending Schedule',
  ...(socialCopy?.hook && { 'Social Copy Hook': socialCopy.hook }),
  ...(socialCopy?.body && { 'Social Copy Body': socialCopy.body }),
  ...(socialCopy?.cta && { 'Social Copy Call to Action': socialCopy.cta }),
  ...(socialCopy?.hashtags && { Hashtags: socialCopy.hashtags })
}
          })
        });

if (!airtableResp.ok) {
  const errorText = await airtableResp.text();
  console.error('[revai-callback] Airtable update failed:', airtableResp.status, errorText);
  await logToGrafana(env, 'error', 'Airtable update failed', {
    title,
    r2_transcript_url: r2TranscriptUrl,
    job_id: id,
    status: airtableResp.status,
    response: errorText
  });
  throw new Error(`Airtable update failed: ${airtableResp.status} - ${errorText}`);
}

        await logToGrafana(env, 'info', 'Airtable update successful', { title, r2_transcript_url: r2TranscriptUrl });

        return new Response(JSON.stringify({ success: true }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' }
        });

      } catch (err) {
        await logToGrafana(env, 'error', 'Callback processing error', {
          title,
          job_id: id,
          error: err.message
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

async function logToGrafana(env, level, message, meta = {}) {
  const payload = {
    level,
    message,
    meta: {
      source: meta.source || 'gr8r-revai-callback-worker',
      service: meta.service || 'callback',
      ...meta
    }
  };

  try {
    const res = await env.GRAFANA.fetch('https://internal/api/grafana', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });

    const resText = await res.text();
    console.log('📤 Sent to Grafana:', JSON.stringify(payload));
    console.log('📨 Grafana response:', res.status, resText);

    if (!res.ok) {
      throw new Error(`Grafana log failed: ${res.status} - ${resText}`);
    }
  } catch (err) {
    console.error('📛 Logger failed:', err.message, '📤 Original payload:', payload);
  }
}
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
  if (CACHED_DB1_INTERNAL_KEY) return CACHED_DB1_INTERNAL_KEY;
  const key = env.DB1_INTERNAL_KEY;
  if (!key) {
    await logToGrafana(env, 'error', 'Missing DB1_INTERNAL_KEY in env', { source: 'revai-callback-worker' });
    throw new Error('DB1 internal key not configured');
  }
  CACHED_DB1_INTERNAL_KEY = key;
  return key;
}
