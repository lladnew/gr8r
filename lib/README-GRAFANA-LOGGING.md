# GR8R Logging Best Practices (Loki/Grafana Cloud)

> Scope: for Cloudflare Workers that push logs **directly** to Grafana Cloud Loki via `lib/grafana.js` and secrets resolved by `lib/secrets.js`.

## Goals

* Make logs easy to query, filter, and chart.
* Keep sensitive data out of logs.
* Keep request latency unaffected by logging failures.

---

## Labels (Loki stream labels)

**Always include**

* `source`: fixed service origin (e.g., `gr8r-db1-worker`)
* `service`: coarse component within the worker (e.g., `auth`, `db1-upsert`, `db1-fetch`, `request`)
* `level`: `debug | info | warn | error`

> Keep labels short and stable. Labels should be low‑cardinality strings; do not place dynamic/user input in labels.

---

## Meta (JSON payload fields)

**Per-entry metadata** should be inside the JSON line (not labels):

* `request_id`: `uuid` (per incoming request)
* `route`: path (e.g., `/db1/videos`)
* `method`: HTTP method
* `status`: HTTP status returned for this step
* `ok`: boolean success flag
* `duration_ms`: request or step duration (ms)
* `origin`: request origin header when relevant

**Optional domain-safe fields**

* For POST success: `title`, `video_type`, `scheduled_at`, `clears_count`
* For GET success: `count`, `status_filter`, `type_filter`

**Error fields**

* `error`: short message
* `stack`: include **only** for unexpected 5xx; omit for 4xx and auth denials
* `reason`: short code for expected denials (e.g., `missing_jwt`, `invalid_jwt`, `not_internal`)

---

## Level policy

* `debug`: noisy plumbing (initial request trace, internal decisions)
* `info`: normal successes; **expected** denials (auth missing/invalid)
* `warn`: client/validation errors you intentionally 4xx
* `error`: unexpected failures/5xx paths

---

## Safety

* **Never** log credentials, tokens, headers, or body payloads. If absolutely necessary, log a deterministic hash or a short code (e.g., `reason`).
* Truncate potentially large or user-controlled strings before logging.
* Prefer counts/ids over raw values.

---

## Structured entry shape

Each push to Loki should serialize a JSON *line* with:

```json
{
  "message": "Upserted video",
  "meta": {
    "request_id": "d3dd…",
    "route": "/db1/videos",
    "method": "POST",
    "status": 200,
    "ok": true,
    "duration_ms": 42,
    "title": "My Video",
    "video_type": "Pivot Year",
    "scheduled_at": "2025-09-04T16:30:00Z",
    "clears_count": 2
  }
}
```

The outer Loki envelope should contain **labels**:

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

## Minimal helper API (recommended)

`logToGrafana(env, entry)` where `entry` is:

```ts
{
  level?: "debug"|"info"|"warn"|"error";
  message: string;
  service: string;        // label
  meta?: Record<string, string|number|boolean|null>;
}
```

* Coerce all label values to **strings**; reject objects/arrays in labels.
* In `meta`, allow primitives only; drop functions/objects to avoid bloat.
* Convert timestamps to **nanoseconds** (`Date.now() * 1_000_000`).
* If the push fails, throw in dev; callers should wrap in `try/catch` so the request code never fails solely due to logging errors.

---

## Versioning & placement

* Place this README next to `lib/grafana.js`.
* Treat `grafana.js` as a leaf utility with **no** cross-worker dependencies.
* Version both README and `grafana.js` via small semantic bumps when fields change.
  
# lib/grafana.js  v1.1.0
# Direct Loki push using secrets resolved via lib/secrets.js