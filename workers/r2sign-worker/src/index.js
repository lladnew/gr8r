// gr8r-r2sign-worker v1.4.0
// added logging to Grafana and a worker response
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

export default {
  async fetch(request, env) {
    const log = (level, message, meta = {}) =>
      env.GRAFANA_WORKER.fetch("http://logger", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          source: "gr8r-r2sign-worker",
          level,
          message,
          meta,
        }),
      });

    const client = new S3Client({
      region: "auto",
      endpoint: `https://${env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
      credentials: {
        accessKeyId: env.R2_ACCESS_KEY_ID,
        secretAccessKey: env.R2_SECRET_ACCESS_KEY,
      },
    });

    if (request.method !== "POST") {
      return new Response("Method Not Allowed", { status: 405 });
    }

    try {
      const { filename, contentType } = await request.json();

      if (!filename || !contentType) {
        await log("warn", "Missing required upload fields", { filename, contentType });
        return new Response("Missing filename or contentType", { status: 400 });
      }

      const command = new PutObjectCommand({
        Bucket: "videos-gr8r",
        Key: filename,
        ContentType: contentType,
      });

      const signedUrl = await getSignedUrl(client, command, { expiresIn: 3600 });

      await log("info", "Presigned upload URL generated", {
        filename,
        contentType,
        signedUrl,
      });

      return new Response(
        JSON.stringify({ status: "success", signedUrl, filename }),
        {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }
      );
    } catch (err) {
      await log("error", "Failed to generate presigned upload URL", {
        error: err.message,
        stack: err.stack,
      });

      return new Response("Internal Error", { status: 500 });
    }
  },
};

