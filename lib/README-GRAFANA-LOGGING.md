# GR8R Logging Best Practices (Loki/Grafana Cloud)

> Scope: for Cloudflare Workers that push logs **directly** to Grafana Cloud Loki via `lib/grafana.js` and secrets resolved by `lib/secrets.js`.

## Goals

- Make logs easy to query, filter, and chart.  
- Keep sensitive data out of logs.  
- **Never let logging failures impact request success or latency.**  
- Prefer **limited success** logs to Grafana; keep **verbose success** in `wrangler tail` (console) for on-demand troubleshooting.

---

## Labels (Loki stream labels)

**Always include**
- `source`: fixed service origin (e.g., `gr8r-db1-worker`)
- `service`: coarse component within the worker (e.g., `auth`, `db1-upsert`, `db1-fetch`, `request`)
- `level`: `debug | info | warn | error`

> Keep labels short and stable (low cardinality). **Never** put user/dynamic values in labels.

---

## Meta (JSON payload fields)

Put **all per-entry metadata** inside the JSON line (not labels):

**Common**
- `request_id` (uuid per incoming request)
- `route` (e.g., `/api/revai/callback`)
- `method` (HTTP method)
- `status_code` (HTTP status we returned for this step)
- `ok` (boolean)
- `duration_ms` (ms for the step)
- `reason` (short machine code for the condition, e.g., `revai_fetch_bad_status`)

**Optional, domain-safe**
- For POST success: `title`, `video_type`, `scheduled_at`, `clears_count`
- For GET success: `count`, `status_filter`, `type_filter`
- For webhook pipelines: `job_id`, `video_id`

**Error fields**
- `error`: short message (no user data)
- `stack`: **only** for unexpected 5xx; omit for 4xx/expected denials

**Truncation**
- Truncate any user-controlled strings (`body_snippet`, etc.) to ≤200 chars.
- Prefer counts/ids over raw values.

---

## Level policy

- `debug`: noisy plumbing (initial receipt, internal decision breadcrumbs)
- `info`: milestone/summary successes; **expected** denials (auth missing/invalid)
- `warn`: client/validation errors you intentionally 4xx
- `error`: unexpected/operational failures; retries; downstream outages

---

## Safety

- **Never** log credentials, tokens, headers, or raw payloads.  
- If absolutely necessary, log a deterministic hash or a short `reason` code.  
- Success logs to Grafana should be **summaries**; keep verbose detail in console only.

---

## Structured entry shape

Each push to Loki serializes a JSON **line**:

```json
{
  "message": "Upserted video",
  "meta": {
    "request_id": "d3dd…",
    "route": "/db1/videos",
    "method": "POST",
    "status_code": 200,
    "ok": true,
    "duration_ms": 42,
    "title": "My Video",
    "video_type": "Pivot Year",
    "scheduled_at": "2025-09-04T16:30:00Z",
    "clears_count": 2
  }
}
```

The Loki envelope uses **labels**:

```json
{
  "streams": [
    {
      "stream": { "level": "info", "source": "gr8r-db1-worker", "service": "db1-upsert" },
      "values": [["<timestamp_ns>", "<json-line>"]]
    }
  ]
}
```

---

## Helper APIs & patterns

### 1) **Default: `safeLog` (recommended)**

Use a wrapper that **never throws**; it caches the underlying logger and falls back to console on failure.

```ts
// Safe logger: default everywhere (prod and dev)
import { createLogger } from "../../../lib/grafana.js";
let _logger;
export const safeLog = async (env, entry) => {
  try {
    _logger = _logger || createLogger({ source: "my-worker" });
    await _logger(env, entry);
  } catch (e) {
    // Never throw from logging
    console.log("LOG_FAIL", entry?.service || "unknown", e?.message || e);
  }
};
```

**Use `safeLog` for all runtime paths** so logging can’t break requests or webhooks.


### 3) Consolidated success

For pipelines (e.g., callback → fetch → generate → store → upsert → queue), prefer:
- **Console**: verbose step-by-step “ok” breadcrumbs.
- **Grafana**: **one** final `info` with a compact summary object (e.g., `{ videos: {...}, publishing: {...} }`), plus individual **error** entries for any failed step.

### 4) Error write-back pattern (webhooks)

When a pipeline step fails, write a **deterministic status** back to the system of record (e.g., set `videos.status = 'Error'`). This allows operators to locate stuck rows without scraping logs.

```ts
// Example: db1ErrorWriteback(env, db1Key, video_id, request_id, job_id)
```

---

## Reason code naming

Use **snake_case** with a subsystem prefix when helpful:
- `revai_failed_status`, `revai_fetch_exception`
- `socialcopy_empty`, `socialcopy_bad_status`
- `db1_videos_check_failed`, `db1_upsert_failed`
- `r2_upload_failed`, `airtable_update_failed`

---

## Timestamps

`grafana.js` should convert to **nanoseconds** (`Date.now() * 1_000_000`) before sending to Loki.

---

## Versioning & placement

- Keep this README next to `lib/grafana.js`.
- Treat `grafana.js` as a leaf utility (no cross-worker deps).
- Bump versions with any field/behavior changes.

**# lib/grafana.js  v1.2.0**  
**# Direct Loki push using secrets resolved via lib/secrets.js**  
**# Default pattern: `safeLog` (non-throwing), optional `logStrict` for local dev**
