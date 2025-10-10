// gr8r-r2access-presign-worker v1.0.2 — fixing issues primarily parseR2URL was breaking... ChatGPT RMEs
// gr8r-r2access-presign-worker v1.0.1 — presign GET for R2 objects with Grafana logging

import { getSecret } from "../../../lib/secrets.js";
import { createLogger } from "../../../lib/grafana.js";

// ---------- logging (matches your policy) ----------
const _logger = createLogger({ source: "gr8r-r2access-presign-worker" });
async function safeLog(env, entry) {
  try {
    const level = entry?.level || "info";
    if (level === "debug" || level === "info") {
      console[level === "debug" ? "debug" : "log"](
        `[${entry?.service || "r2presign"}] ${entry?.message || ""}`,
        entry?.meta || {}
      );
      return;
    }
    await _logger(env, entry); // warn/error → Grafana
  } catch (e) {
    console.log("LOG_FAIL", entry?.service || "unknown", e?.stack || e?.message || e);
  }
}

const SERVICE = "r2access-presign-worker";

// ---- config you can tweak in one place ----
const BUCKET = "videos";                 // single source of truth for your R2 bucket name
const CUSTOM_VIDEO_HOST = "videos.gr8r.com"; // how r2_url will usually look

// ---- helpers ----
function json(status, obj) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

async function requireInternalAuth(req, env) {
  const auth = req.headers.get("authorization") || "";
  const provided = auth.startsWith("Bearer ") ? auth.slice(7).trim() : "";
  if (!provided) throw new Response("Unauthorized", { status: 401 });

  const expected = await getSecret(env, "INTERNAL_WORKER_KEY");
  if (!expected || provided !== expected) throw new Response("Unauthorized", { status: 401 });
}

function toISOInSecondsFromNow(ttlSeconds) {
  const ms = Math.max(1, Math.floor(ttlSeconds)) * 1000;
  return new Date(Date.now() + ms).toISOString();
}

function parseR2Url(r2_url) {
  if (r2_url == null) return { ok: false, error: "bad_r2_url_empty" };

  // normalize & trim
  const raw = String(r2_url).trim();
  if (!raw) return { ok: false, error: "bad_r2_url_empty" };

  // Case 1: r2://bucket/key
  if (raw.startsWith("r2://")) {
    const m = /^r2:\/\/([^/]+)\/(.+)$/.exec(raw);
    if (!m) return { ok: false, error: "bad_r2_url_parse" };
    const bucket = m[1];
    const key = decodeURIComponent(m[2]).replace(/^\/+/, "");
    if (!key) return { ok: false, error: "missing_key" };
    return { ok: true, bucket, key };
  }

  // Case 2: bare key (no scheme/host); accept as-is for your single bucket
  if (!/^[a-z]+:\/\//i.test(raw) && !raw.startsWith("//")) {
    const key = decodeURIComponent(raw).replace(/^\/+/, "");
    if (!key) return { ok: false, error: "missing_key" };
    return { ok: true, bucket: BUCKET, key };
  }

  // Case 3: https://videos.gr8r.com/<key> (your stored form)
  try {
    const u = new URL(raw);
    const host = u.hostname.toLowerCase();

    if (host !== CUSTOM_VIDEO_HOST) {
      // keep this strict for security; if you ever want to relax, handle here
      return { ok: false, error: "unexpected_host", host };
    }

    const key = decodeURIComponent(u.pathname.replace(/^\/+/, ""));
    if (!key) return { ok: false, error: "missing_key" };

    return { ok: true, bucket: BUCKET, key };
  } catch {
    return { ok: false, error: "bad_r2_url_parse" };
  }
}

// ---- SigV4 core (R2 S3-compatible) ----
async function hmacRaw(keyBytes, msgStr) {
  const enc = new TextEncoder();
  const cryptoKey = await crypto.subtle.importKey(
    "raw",
    keyBytes,
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const sig = await crypto.subtle.sign("HMAC", cryptoKey, enc.encode(msgStr));
  return new Uint8Array(sig);
}

async function hmac(key, msg) {
  // key can be string or Uint8Array
  const bytes = typeof key === "string" ? new TextEncoder().encode(key) : key;
  return hmacRaw(bytes, msg);
}

async function hmacHex(key, msg) {
  const bytes = await hmac(key, msg);
  return [...bytes].map(b => b.toString(16).padStart(2, "0")).join("");
}

async function sha256Hex(str) {
  const data = new TextEncoder().encode(str);
  const buf = await crypto.subtle.digest("SHA-256", data);
  return [...new Uint8Array(buf)].map(b => b.toString(16).padStart(2, "0")).join("");
}

async function signV4QueryGET({ accountId, accessKeyId, secretAccessKey, bucket, key, region, ttlSeconds }) {
  // R2 S3 endpoint (path-style)
  // https://<accountId>.r2.cloudflarestorage.com/<bucket>/<key>?X-Amz-Algorithm=AWS4-HMAC-SHA256...
  const host = `${accountId}.r2.cloudflarestorage.com`;
  const endpoint = `https://${host}`;
  const encodedKey = key.split("/").map(encodeURIComponent).join("/");
  const canonicalUri = `/${encodeURIComponent(bucket)}/${encodedKey}`;

  const now = new Date();
  const amzDate = now.toISOString().replace(/[:-]|\.\d{3}/g, ""); // YYYYMMDDTHHMMSSZ
  const datestamp = amzDate.slice(0, 8);                          // YYYYMMDD
  const algorithm = "AWS4-HMAC-SHA256";
  const service = "s3";
  const signedHeaders = "host";
  const credentialScope = `${datestamp}/${region}/${service}/aws4_request`;

  const params = new URLSearchParams({
    "X-Amz-Algorithm": algorithm,
    "X-Amz-Credential": `${encodeURIComponent(accessKeyId + "/" + credentialScope)}`,
    "X-Amz-Date": amzDate,
    "X-Amz-Expires": String(Math.max(1, Math.min(ttlSeconds, 604800))), // max 7 days
    "X-Amz-SignedHeaders": signedHeaders,
  });

  const payloadHash = "UNSIGNED-PAYLOAD";

  const canonicalQuery = params.toString();
  const canonicalHeaders = `host:${host}\n`;
  const canonicalRequest = [
    "GET",
    canonicalUri,
    canonicalQuery,
    canonicalHeaders,
    signedHeaders,
    payloadHash,
  ].join("\n");

  const canonicalRequestHash = await sha256Hex(canonicalRequest);

  const stringToSign = [
    algorithm,
    amzDate,
    credentialScope,
    canonicalRequestHash,
  ].join("\n");

  // Derive signing key
  const kDate    = await hmac(`AWS4${secretAccessKey}`, datestamp);
  const kRegion  = await hmac(kDate, region);
  const kService = await hmac(kRegion, service);
  const kSigning = await hmac(kService, "aws4_request");

  const signature = await hmacHex(kSigning, stringToSign);

  return `${endpoint}${canonicalUri}?${canonicalQuery}&X-Amz-Signature=${signature}`;
}

// ---- main worker ----
export default {
  async fetch(req, env, ctx) {
    const url = new URL(req.url);

    if (req.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "POST,OPTIONS",
          "Access-Control-Allow-Headers": "Authorization,Content-Type",
          "Access-Control-Max-Age": "3600",
        },
      });
    }

    if (req.method === "POST" && url.pathname === "/presign") {
      const request_id = crypto.randomUUID().slice(0, 8);
      const t0 = Date.now();
      try {
        await requireInternalAuth(req, env);

        const body = await req.json().catch(() => ({}));
        const r2_url   = body?.r2_url;
        const ttl      = Number(body?.ttl_seconds) || 1800;
        const requester = String(body?.requester || "unknown");
        const reason    = String(body?.reason || "");
        const video_id  = Number(body?.video_id || 0) || null;

        const parsed = parseR2Url(r2_url);
        if (!parsed.ok) {
          await safeLog(env, {
            level: "warn",
            service: SERVICE,
            message: "bad r2_url",
            meta: { request_id, requester, video_id, r2_url, error: parsed.error, ok: false, status_code: 400, duration_ms: Date.now() - t0 }
          });
          return json(400, { ok: false, error: parsed.error });
        }

        const accountId       = await getSecret(env, "R2_ACCOUNT_ID");
        const accessKeyId     = await getSecret(env, "R2_ACCESS_KEY_ID");
        const secretAccessKey = await getSecret(env, "R2_SECRET_ACCESS_KEY");

        // Cloudflare R2 region is "auto"
        const region = "auto";

        const signedUrl = await signV4QueryGET({
          accountId,
          accessKeyId,
          secretAccessKey,
          bucket: parsed.bucket,
          key: parsed.key,
          region,
          ttlSeconds: ttl,
        });

        const expires_at = toISOInSecondsFromNow(ttl);

        await safeLog(env, {
          level: "info",
          service: SERVICE,
          message: "presign ok",
          meta: {
            request_id, requester, reason, video_id,
            key: parsed.key, bucket: parsed.bucket, ttl_sec: ttl,
            ok: true, status_code: 200, duration_ms: Date.now() - t0
          }
        });

        return json(200, { ok: true, url: signedUrl, expires_at });
      } catch (err) {
        // err can be a Response (auth) or an Error
        const status = err instanceof Response ? err.status : 500;
        const error  = err instanceof Response ? "unauthorized" : (err?.message || "server_error");

        await safeLog(env, {
          level: "error",
          service: SERVICE,
          message: "presign failed",
          meta: {
            request_id,
            ok: false,
            status_code: status,
            error,
          }
        });

        if (err instanceof Response) return err;
        return json(500, { ok: false, error: "server_error" });
      }
    }

    return new Response("Not Found", { status: 404 });
  },
};
