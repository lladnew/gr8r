# gr8r-revai-callback-worker
# v1.1.3 gr8r-revai-callback-worker
# - FIXED: all internal fetch() calls now explicitly use env.<BINDING>.fetch (v1.1.3)
# - RETAINED: entire logic and structure from v1.1.2 unchanged except binding fix (v1.1.3)
# v1.1.2 gr8r-revai-callback-worker
# - FIXED: Rev.ai retry loop by ensuring 200 OK is always returned after processing (v1.1.2)
# - ADDED: wraps transcript fetch, R2 upload, and Airtable update in try/catch block (v1.1.2)
# - ADDED: on failure, logs error to Grafana but still returns 200 OK with { success: false } (v1.1.2)
# - ADDED: skips processing if Airtable record for job.id exists and has Status = 'Transcription Complete' (v1.1.2)
# - RETAINED: exact log structure, R2 key format, Airtable field mappings, and internal service calls (v1.1.2)
# - RETAINED: all v1.1.1 behavior including use of metadata as title and job.id as Transcript ID match
# v1.1.1 gr8r-revai-callback-worker
# FIXED: parses Rev.ai callback correctly using `body.job` structure (v1.1.1)
# - CHANGED: destructures fields from `body.job` instead of `body`
# - CHANGED: transcript fetch request now passes `{ job_id }` instead of `{ transcript_url }`
# - CHANGED: Airtable update now matches on field `Transcript ID = job.id`
# - CHANGED: continues to use `job.metadata` as transcript title for R2 key (`transcripts/{title}.txt`) (v1.1.1)
# - RETAINED: R2 upload via `ASSETS`, Airtable update via `AIRTABLE`, all Grafana debug/info/error logs (v1.1.1)
# - RETAINED: all debug-level Grafana logs added in v1.1.0 (v1.1.1)
# v1.0.10 gr8r-revai-callback-worker
# ADDED: debug-level logToGrafana() trace logs for all major steps (v1.0.10)
# - ADDED: 'Callback triggered', 'Parsed body', 'Fetching transcript', 'Uploading to R2', 'Updating Airtable'
# - RETAINED: info/error logs for transcript fetch, R2 upload, and Airtable update (v1.0.10)
# - RETAINED: console.log() and console.error() for local tail logs (v1.0.10)
# v1.0.9 gr8r-revai-callback-worker
# RESTORED: R2 transcript upload and Airtable update logic removed without changelog in v1.0.6–v1.0.8
# - ADDED: uploads transcript text to R2 via `ASSETS` binding at key `transcripts/{title}.txt`
# - ADDED: updates Airtable via `AIRTABLE` binding using table ID `tblQKTuBRVrpJLmJp`
# - ADDED: logs success/failure of R2 upload and Airtable update to Grafana (v1.0.9)
# - RETAINED: Rev.ai transcript fetch via `REVAIFETCH`, full error capture, and raw_payload logging (v1.0.9)
# v1.0.8 gr8r-revai-callback-worker
# ENHANCED: added `title` and `transcript_url` to both success and error Grafana logs (v1.0.8)
# OPTIONAL: added check to skip callbacks if status is not 'completed' (v1.0.8)
# RETAINED: all previous behavior, error handling, and metadata logging (v1.0.8)
# v1.0.7 gr8r-revai-callback-worker
# FIXED: properly calls internal binding to gr8r-revai-worker using env.REVAI (v1.0.7)
# - CHANGED: fetch URL to 'https:#internal/api/revai/fetch-transcript' and uses env.REVAI (v1.0.7)
# - FIXED: sends correct POST body { transcript_url } to match revai-worker input (v1.0.7)
# - RETAINED: raw_payload logging, structured error capture, and R2/Airtable update logic (v1.0.7)
# v1.0.6 gr8r-revai-callback-worker
# CHANGED: replaced direct Rev.ai transcript fetch with internal call to gr8r-revai-worker (v1.0.6)
# - NEW: constructs internal fetch to https:#revai.gr8r.com/api/revai/fetch-transcript (v1.0.6)
# - REMOVED: direct Rev.ai fetch using transcript URL from payload (v1.0.6)
# - RETAINED: raw_payload logging, metadata capture, structured error logging (v1.0.6)
# - ADDED: logs for successful transcript fetch via internal API (v1.0.6)
# - ADDED: logs error response body from fetch failures (v1.0.6)
# v1.0.5 gr8r-revai-callback-worker
# ADDED: creates R2 transcript file and updates Airtable with R2 URL (v1.0.5)
# - ADDED: structured R2 key naming using `transcripts/{title}.txt` (v1.0.5)
# - ADDED: updates Airtable field 'R2 Transcript URL' and sets Status to 'Transcription Complete' (v1.0.5)
# - RETAINED: metadata and full Grafana logging (v1.0.5)
# v1.0.4 gr8r-revai-callback-worker
# CHANGED: Updated logToGrafana to match v1.0.9 format of grafana-worker (v1.0.4)
# - WRAPPED all meta fields inside a `meta` object (v1.0.4)
# - REMOVED top-level `source` and `service`, now embedded inside `meta` (v1.0.4)
# - RETAINED: full raw_payload, transcription metadata, and body capture (v1.0.4)
# v1.0.3 gr8r-revai-callback-worker
# CHANGED: flattened Grafana logging payload to surface meta fields at top level (v1.0.3)
# RETAINED: full raw_payload capture, metadata, and structured logging (v1.0.3)
# v1.0.2
# CHANGED: added request.clone().text() to capture the full raw payload (v1.0.2)
# ADDED: raw_payload to Grafana logs for successful callbacks (v1.0.2)
# v1.0.1
# added code starting line 16 to add transcription ID and metadata to grafana logs (v1.0.1)
