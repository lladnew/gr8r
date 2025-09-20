// v1.1.0 gr8r-revai-worker CHANGED: added getSecret and createLogger using safeLog - new standards
// v1.0.9 gr8r-revai-worker CHANGED: removed grafana success logging to reduce noise
// v1.0.8 gr8r-revai-worker
// - ADDED: "strict" custom vocab enforcement line:46
// v1.0.7 gr8r-revai-worker
// - ADDED: debug-level logging of transcript fetch response to console and Grafana (v1.0.7)
// - INSERTED: console.log and transcript snippet log after successful fetch in /fetch-transcript (v1.0.7)
// - RETAINED: existing error handling, response structure, and logging format (v1.0.7)
// v1.0.6 gr8r-revai-worker (roll back)
// CHANGED: fetch-transcript endpoint now accepts { job_id } instead of { transcript_url } (v1.0.6)
// - FETCHES: transcript via Rev.ai API GET /jobs/{job_id}/transcript (v1.0.6)
// - RETAINED: error handling and Grafana logging (v1.0.6)
// v1.0.5 gr8r-revai-worker
// - ADDED: POST /api/revai/fetch-transcript endpoint to retrieve transcript from Rev.ai with API key (v1.0.5)
// - PRESERVED: existing /transcribe job creation logic unchanged (v1.0.5)
// - PRESERVED: Grafana logging and clean Rev.ai dashboard metadata (v1.0.5)
// v1.0.4 gr8r-revai-worker
// - CHANGED: Now returns full parsed Rev.ai job object (not just text)
// - CHANGED: Sends metadata as plain title string instead of full JSON
// - RETAINED: Clean job name in Rev.ai dashboard
// - PRESERVED: Grafana logging and error capture
// v1.0.3 gr8r-revai-worker
// - ADDED: `name` field set to `metadata.title` for cleaner display in Rev.ai dashboard

// Shared libs
import { getSecret } from "../../../lib/secrets.js";
import { createLogger } from "../../../lib/grafana.js";

// Safe logger: always use this; caches underlying logger internally
let _logger;
const safeLog = async (env, entry) => {
  try {
    _logger = _logger || createLogger({ source: "gr8r-revai-worker" });
    await _logger(env, entry);
  } catch (e) {
    // Never throw from logging
    console.log("LOG_FAIL", entry?.service || "unknown", e?.stack || e?.message || e);
  }
};

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const route = url.pathname;
    const method = request.method;
    const request_id = (globalThis.crypto && crypto.randomUUID) ? crypto.randomUUID() : `${Date.now()}-${Math.random().toString(16).slice(2)}`;

    // === Transcription Job Creation ===
    if (route === "/api/revai/transcribe" && method === "POST") {
      const t0 = Date.now();
      try {
        const body = await request.json();
        const { media_url, metadata, callback_url } = body;
                if (!media_url || !metadata || !callback_url) {
                  await safeLog(env, {
                    level: "warn",
                    service: "transcribe",
                    message: "Missing required fields",
                    meta: {
                      request_id,
                      route,
                      method,
                      status_code: 400,
                      ok: false,
                      duration_ms: Date.now() - t0,
                      reason: "missing_required_fields"
                    }
                  });
                  return new Response(JSON.stringify({ error: "Bad Request", message: "Missing required fields" }), {
                    status: 400,
                    headers: { "Content-Type": "application/json" }
                  });

                }
        const title = typeof metadata === "string" ? metadata : metadata.title || "Untitled";

        const revPayload = {
          media_url,
          metadata: title,
          name: title,
          callback_url,
          custom_vocabulary_id: "cvjFZZkyCf3NryGNlL",
          custom_vocabulary_parameters: {
            strict: true
          }
        };

        const revResponse = await fetch("https://api.rev.ai/speechtotext/v1/jobs", {
          method: "POST",
          headers: {
            "Authorization": `Bearer ${await getSecret(env, "REVAI_API_KEY")}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify(revPayload)
        });

        const resultText = await revResponse.text();
        const success = revResponse.ok;

        let resultJson;
        try {
          resultJson = JSON.parse(resultText);
        } catch {
          resultJson = null; // treat as unknown/invalid JSON
        }

        if (!success || !resultJson || !resultJson.id) {
          // error path (keeps your existing meta)
          await safeLog(env, {
            level: "error",
            service: "transcribe",
            message: "Rev.ai job create failed",
            meta: {
              request_id,
              route,
              method,
              status_code: revResponse.status,
              ok: false,
              duration_ms: Date.now() - t0,
              reason: success ? "revai_success_but_invalid_json" : "revai_failed_status",
              media_url,
              callback_url,
              title,
              rev_response_snippet: resultText.slice(0, 200)
            }
          });
        } else {
          // success breadcrumb w/ job id
          const job_id = resultJson?.id ?? resultJson?.job?.id;
          console.log("[revai-worker] transcribe ok", {
            title,
            job_id,
            status: revResponse.status,
            duration_ms: Date.now() - t0
          });
        }

        return new Response(JSON.stringify(resultJson ?? { raw: resultText }), {
          status: revResponse.status,
          headers: { "Content-Type": "application/json" }
        });


      } catch (err) {
        await safeLog(env, {
          level: "error",
          service: "transcribe",
          message: "Unhandled Rev.ai job error",
          meta: {
            request_id,
            route,
            method,
            status_code: 500,
            ok: false,
            duration_ms: Date.now() - t0,
            reason: "transcribe_exception",
            error: err.message,
            stack: err.stack
          }
        });
        return new Response(JSON.stringify({ error: "Internal error", message: err.message }), {
          status: 500,
          headers: { "Content-Type": "application/json" }
        });
      }
    }
    // === Transcript Retrieval ===
    if (route === "/api/revai/fetch-transcript" && method === "POST") {
      const t0 = Date.now();
      let job_id;
      try {
        const body = await request.json();
        job_id = body?.job_id;
          if (!job_id) {
            await safeLog(env, {
              level: "warn",
              service: "fetch-transcript",
              message: "Missing job_id",
              meta: {
                request_id,
                route,
                method,
                status_code: 400,
                ok: false,
                duration_ms: Date.now() - t0,
                reason: "missing_job_id"
              }
            });
            return new Response(JSON.stringify({ error: "Bad Request", message: "Missing job_id" }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }

        const revFetch = await fetch(`https://api.rev.ai/speechtotext/v1/jobs/${job_id}/transcript`, {
          method: "GET",
          headers: {
            Authorization: `Bearer ${await getSecret(env, "REVAI_API_KEY")}`,
            Accept: "text/plain"
          }
        });

        const transcriptText = await revFetch.text();
        console.log("[revai-worker] Fetched transcript (snippet):", transcriptText.slice(0, 200)); 
        
        if (!revFetch.ok) {
          throw new Error(`Transcript fetch failed: ${revFetch.status}`);
        }

        return new Response(transcriptText, {
          status: 200,
          headers: { "Content-Type": "text/plain" }
        });

      } catch (err) {
          await safeLog(env, {
            level: "error",
            service: "fetch-transcript",
            message: "Transcript fetch failure",
            meta: {
              request_id,
              route,
              method,
              status_code: 500,
              ok: false,
              duration_ms: Date.now() - t0,
              reason: "revai_fetch_exception",
              job_id,
              error: err.message,
              stack: err.stack
            }
          });
        return new Response(JSON.stringify({ error: "Transcript fetch failed", message: err.message }), {
          status: 500,
          headers: { "Content-Type": "application/json" }
        });
      }
    }

    return new Response(JSON.stringify({ error: "Not Found" }), {
      status: 404,
      headers: { "Content-Type": "application/json" }
    });
  }
};
