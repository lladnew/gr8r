// v1.0.8 gr8r-revai-worker CHANGED: removed grafana success logging to reduce noise
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

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // === Transcription Job Creation ===
    if (url.pathname === "/api/revai/transcribe" && request.method === "POST") {
      try {
        const body = await request.json();
        const { media_url, metadata, callback_url } = body;

        if (!media_url || !metadata || !callback_url) {
          return new Response("Missing required fields", { status: 400 });
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
            "Authorization": `Bearer ${env.REVAI_API_KEY}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify(revPayload)
        });

        const resultText = await revResponse.text();
        const success = revResponse.ok;

        let resultJson = {};
        try {
          resultJson = JSON.parse(resultText);
        } catch (e) {
          resultJson = { raw: resultText };
        }

      // Only log if there's an error
      if (!success) {
        await env.GRAFANA.fetch("https://internal/api/grafana", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            level: "error",
            message: `Rev.ai error: ${resultText}`,
            meta: {
              source: "gr8r-revai-worker",
              service: "transcribe",
              media_url,
              metadata: title,
              callback_url,
              revStatus: revResponse.status,
              revResponse: resultText
            }
          })
        });
      }

        return new Response(JSON.stringify(resultJson), {
          status: revResponse.status,
          headers: { "Content-Type": "application/json" }
        });

      } catch (err) {
        await env.GRAFANA.fetch("https://internal/api/grafana", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            level: "error",
            message: "Unhandled Rev.ai job error",
            meta: {
              source: "gr8r-revai-worker",
              service: "transcribe",
              error: err.message,
              stack: err.stack
            }
          })
        });

        return new Response(JSON.stringify({
          error: "Internal error",
          message: err.message
        }), { status: 500, headers: { "Content-Type": "application/json" } });
      }
    }

    // === Transcript Retrieval ===
    if (url.pathname === "/api/revai/fetch-transcript" && request.method === "POST") {
      try {
        const { job_id } = await request.json();

        if (!job_id) {
          return new Response("Missing job_id", { status: 400 });
        }

        const revFetch = await fetch(`https://api.rev.ai/speechtotext/v1/jobs/${job_id}/transcript`, {
          method: "GET",
          headers: {
            Authorization: `Bearer ${env.REVAI_API_KEY}`,
            Accept: "text/plain"
          }
        });

        const transcriptText = await revFetch.text();
//new logging block inserted v1.0.7        
console.log('[revai-worker] Fetched transcript:', transcriptText);

        if (!revFetch.ok) {
          throw new Error(`Transcript fetch failed: ${revFetch.status}`);
        }

        return new Response(transcriptText, {
          status: 200,
          headers: { "Content-Type": "text/plain" }
        });

      } catch (err) {
        await env.GRAFANA.fetch("https://internal/api/grafana", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            level: "error",
            message: "Transcript fetch failure",
            meta: {
              source: "gr8r-revai-worker",
              service: "fetch-transcript",
              error: err.message,
              stack: err.stack
            }
          })
        });

        return new Response(JSON.stringify({
          error: "Transcript fetch failed",
          message: err.message
        }), { status: 500, headers: { "Content-Type": "application/json" } });
      }
    }

    return new Response("Not found", { status: 404 });
  }
};
