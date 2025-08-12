// v1.0.9 gr8r-grafana-worker
// - ADDED: Flattens top-level primitive `meta` fields into Loki labels for dashboard filtering
// - ADDED: Embeds full `meta` object in the log message (as JSON) for postmortem inspection
// - RETAINED: Basic auth, verbose logging, and fallback defaults for source/service
// v1.0.8 gr8r-grafana-worker: compatible with service bindings + retains diagnostics
//ADDED support for internal Worker-to-Worker calls via service bindings
//RETAINED route support for direct HTTP POSTs (api.gr8r.com/api/grafana)
//RETAINED verbose logging for incoming requests and Loki response
//MAINTAINED fallback source/service label defaults for Grafana visibility

export default {
  async fetch(request, env) {
    console.log("📥 Incoming request to Grafana Worker");

    if (request.method !== "POST") {
      return new Response("Method not allowed", { status: 405 });
    }

    try {
      const body = await request.json();
      const { level = "info", message = "No message provided", meta = {} } = body;

      const source = (typeof meta.source === "string" && meta.source.trim()) ? meta.source : "gr8r-fallback";
      const service = (typeof meta.service === "string" && meta.service.trim()) ? meta.service : "gr8r-unknown";

      const timestamp = Date.now() * 1_000_000; // nanoseconds for Loki

      const labels = {
        level,
        source,
        service_name: service,
      };

      // Flatten top-level primitive fields from meta into labels
      for (const [key, value] of Object.entries(meta)) {
        if (key !== "source" && key !== "service") {
          if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
            labels[key] = String(value);
          }
        }
      }

      // Embed full structured meta in the message payload for drill-down
      const messagePayload = {
        message,
        ...(Object.keys(meta).length > 0 ? { meta } : {})
      };

      const stream = {
        stream: labels,
        values: [[`${timestamp}`, JSON.stringify(messagePayload)]]
      };

      const lokiUrl = env.GRAFANA_LOKI_URL;
      const username = env.GRAFANA_USERNAME;
      const apiKey = env.GRAFANA_API_KEY;
      const authHeader = 'Basic ' + btoa(`${username}:${apiKey}`);

      const payload = JSON.stringify({ streams: [stream] });

      console.log("📤 Loki Payload:", payload);

      const response = await fetch(`${lokiUrl}/loki/api/v1/push`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": authHeader
        },
        body: payload
      });

      const responseText = await response.text();
      console.log("✅ Loki Response Code:", response.status);
      console.log("📨 Loki Response Body:", responseText);

      if (!response.ok) {
        console.error("❌ Loki push failed:", response.status, responseText);
        return new Response(`Loki error: ${responseText}`, { status: 500 });
      }

      return new Response(JSON.stringify({ success: true }), {
        headers: { "Content-Type": "application/json" }
      });
    } catch (err) {
      console.error("❌ Unexpected Worker Error:", err.message);
      return new Response(`Unexpected error: ${err.message}`, { status: 500 });
    }
  }
}
