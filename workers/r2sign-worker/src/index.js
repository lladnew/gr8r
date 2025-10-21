// gr8r-r2sign-worker v1.4.2 EDIT: edited log success message to include filename for quick reference
// gr8r-r2sign-worker v1.4.1 CHANGE: migrate to safeLog + Secrets Store and update to current logging best practice standard
// gr8r-r2sign-worker v1.4.0
// added logging to Grafana and a worker response
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
// Shared libs
import { getSecret } from "../../../lib/secrets.js";
import { createLogger } from "../../../lib/grafana.js";
// Safe logger: caches underlying logger; never throws
let _logger;
  const safeLog = async (env, entry) => {
    try {
      _logger = _logger || createLogger({ source: "gr8r-r2sign-worker" });
      await _logger(env, entry);
    } catch (e) {
      // Never throw from logging; keep console-only for failures
      console.log("LOG_FAIL", entry?.service || "unknown", e?.message || e);
    }
  };

export default {
  async fetch(request, env) {
    const t0 = Date.now();
    const { pathname } = new URL(request.url);
    const request_id =
      (globalThis.crypto?.randomUUID && crypto.randomUUID()) ||
      `${Date.now()}-${Math.random().toString(36).slice(2)}`;

    const baseLog = {
      source: "gr8r-r2sign-worker",
      service: "presign",
      request_id,
      route: pathname,
      method: request.method,
    };
    if (pathname !== "/") {
      const status_code = 404;
      const duration_ms = Date.now() - t0;
      await safeLog(env, {
        ...baseLog,
        level: "warn",
        message: "Not Found",
        status_code,
        ok: false,
        duration_ms,
        meta: { requested_path: pathname },
      });
      return new Response(
        JSON.stringify({ error: "Not Found", message: "Route not found" }),
        { status: status_code, headers: { "Content-Type": "application/json" } }
      );
    }

   if (request.method !== "POST") {
      const status_code = 405;
      const duration_ms = Date.now() - t0;
      await safeLog(env, {
        ...baseLog,
        level: "warn",
        message: "Method Not Allowed",
        status_code,
        ok: false,
        duration_ms,
        meta: { allowed_methods: "POST" },
      });
      return new Response(
        JSON.stringify({ error: "Method Not Allowed", message: "Use POST" }),
        { status: status_code, headers: { "Content-Type": "application/json" } }
      );
    }


    try {
      // Secrets (via Secrets Store or per-worker secrets)
      const R2_ACCOUNT_ID = await getSecret(env, "R2_ACCOUNT_ID");
      const R2_ACCESS_KEY_ID = await getSecret(env, "R2_ACCESS_KEY_ID");
      const R2_SECRET_ACCESS_KEY = await getSecret(env, "R2_SECRET_ACCESS_KEY");

      if (!R2_ACCOUNT_ID || !R2_ACCESS_KEY_ID || !R2_SECRET_ACCESS_KEY) {
        const status_code = 500;
        const duration_ms = Date.now() - t0;
        await safeLog(env, {
          ...baseLog,
          level: "error",
          message: "Missing required R2 secrets",
          status_code,
          ok: false,
          duration_ms,
          meta: {
            has_r2_account_id: Boolean(R2_ACCOUNT_ID),
            has_r2_access_key_id: Boolean(R2_ACCESS_KEY_ID),
            has_r2_secret_access_key: Boolean(R2_SECRET_ACCESS_KEY),
          },
        });
        return new Response(
          JSON.stringify({
            error: "Server Misconfiguration",
            message: "Required R2 secrets are missing",
          }),
          { status: status_code, headers: { "Content-Type": "application/json" } }
        );
      }

      const client = new S3Client({
        region: "auto",
        endpoint: `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
        credentials: {
          accessKeyId: R2_ACCESS_KEY_ID,
          secretAccessKey: R2_SECRET_ACCESS_KEY,
        },
      });

      const { filename, contentType } = await request.json().catch(() => ({}));
      if (!filename || !contentType) {
        const status_code = 400;
        const duration_ms = Date.now() - t0;
        await safeLog(env, {
          ...baseLog,
          level: "warn",
          message: "Missing required upload fields",
          status_code,
          ok: false,
          duration_ms,
          meta: { filename_present: Boolean(filename), content_type_present: Boolean(contentType) },
        });
        return new Response(
          JSON.stringify({ error: "Bad Request", message: "Missing filename or contentType" }),
          { status: status_code, headers: { "Content-Type": "application/json" } }
        );
      }


      const command = new PutObjectCommand({
        Bucket: "videos-gr8r",
        Key: filename,
        ContentType: contentType,
      });

      const signedUrl = await getSignedUrl(client, command, { expiresIn: 3600 });

      console.info(
        JSON.stringify({
          ...baseLog,
          level: "info",
          message: "Presigned upload URL generated",
          status_code: 200,
          ok: true,
          duration_ms: Date.now() - t0,
          meta: { filename, content_type: contentType, expires_in: 3600 }
          })
      );
      await safeLog(env, {
        ...baseLog,
        level: "info",
        message: `Presigned upload URL generated: ${filename}`,
        status_code: 200,
        ok: true,
        duration_ms: Date.now() - t0,
        meta: { filename, content_type: contentType, expires_in: 3600 }
      });

      return new Response(
        JSON.stringify({ status: "success", signedUrl, filename }),
        {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }
      );
   }  catch (err) {
      const status_code = 500;
      const duration_ms = Date.now() - t0;
      await safeLog(env, {
        ...baseLog,
        level: "error",
        message: "Failed to generate presigned upload URL",
        status_code,
        ok: false,
        duration_ms,
        meta: { error: err?.message || String(err) },
      });
      return new Response(
        JSON.stringify({ error: "Internal Error", message: "Unable to generate presigned URL" }),
        { status: status_code, headers: { "Content-Type": "application/json" } }
      );
    }
  },
};

