// v1.0.4 gr8r-socialcopy-worker CHANGE: Secrets Store for OPENAI_API_KEY via getSecret(); add INTERNAL auth check via INTERNAL_WORKER_KEY; add timeout & JSON errors
// v1.0.3 gr8r-socialcopy-worker simply changed capitalization of default Pivot Year hashtag to match BriannaWiest
// v1.0.2 gr8r-socialcopy-worker
// updated line 20 to match any case of the string pivot year
// tweaked prompt to always include GR8R hashtag
// v1.0.1 gr8r-socialcopy-worker
// Generates Social Copy (Hashtags, Hook, Body, CTA) from transcript
// Adjusted: Static hashtags included only if title contains 'Pivot Year'

// --- ADD (Secrets helper import) ---
import { getSecret } from "../../../lib/secrets.js";

// --- ADD (static config & secret caches) ---
const CONFIG = {
  MODEL: "gpt-4o",
  OPENAI_URL: "https://api.openai.com/v1/chat/completions",
  TIMEOUT_MS: 60000, // 60s
};

let _openaiKey; let _internalKey;
async function getOpenAIKey(env) {
  if (_openaiKey) return _openaiKey;
  _openaiKey = (await getSecret(env, "OPENAI_API_KEY"))?.toString().trim() || "";
  return _openaiKey;
}
async function getInternalKey(env) {
  if (_internalKey) return _internalKey;
  _internalKey = (await getSecret(env, "INTERNAL_WORKER_KEY"))?.toString().trim() || "";
  return _internalKey;
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const pathname = url.pathname;
    // --- ADD (authorization guard) ---
    const auth = request.headers.get("authorization") || "";
    const bearer = auth.startsWith("Bearer ") ? auth.slice(7).trim() : "";
    const internalKey = await getInternalKey(env);
    if (!internalKey || bearer !== internalKey) {
      return Response.json({ ok: false, error: "Unauthorized" }, { status: 401 });
    }

    if (pathname === '/api/socialcopy' && request.method === 'POST') {
      try {
        const { transcript, title } = await request.json();

        if (!transcript || !title) {
          return Response.json({ ok: false, error: 'Missing required fields: transcript or title' }, { status: 400 });
        }

        const isPivotYear = title.toLowerCase().includes('pivot year');
        const prompt = `Your Tasks:
1. Generate Hashtags
- Always include the static hashtag: #GR8R
- ${(isPivotYear
  ? `Also include these static hashtags: #ThePivotYear #BriannaWiest and generate 3 additional trending hashtags based on the video’s key themes.`
  : `Generate 5 additional trending hashtags based on the video’s key themes.`)}
- Ensure all hashtags use CamelCase (e.g., #MindsetShift).
- Do not repeat or modify static hashtags.

2. Generate social media copy (hook + body + Call to Action). Keep the content compelling and concise while maintaining the brand voice. Ensure sentences are complete and do not get cut off mid-thought.
- Format the copy as follows:
Hook: A compelling, curiosity-driven opener.
Body: A reflection on the video’s theme, avoiding generic motivational phrases. Max limit of 350 characters for the Body.
Call to Action: A question or prompt that encourages audience engagement.
Align with Gr8ter Things’ brand voice:
- Authentic, philosophical, and real
- Relatable and genuine
- Focused on mental blocks and headspace challenges
- Avoid generic motivation

3. Safety Check ***IMPORTANT***
Combine the outputs of Hashtags and Social Media Copy together (including Hook, Body, and Call to Action) and compute the total character count. If it exceeds 500 characters, you must edit one or more outputs to bring it to 500 or fewer characters.

Desired Output Format:
1) Hashtags: #Trend1 #Trend2 #Trend3 [#GR8R #ThePivotYear #BriannaWiest if Pivot Year]
2) Social Media Copy:
   **Hook:** [Compelling Opener]
   **Body:** [Engaging Message]
   **Call to Action:** [Prompt for Engagement]`;
        // --- FIX (get key + build headers BEFORE fetch) ---
        const key = await getOpenAIKey(env);
        if (!key) {
          return Response.json({ ok: false, error: "Missing OPENAI_API_KEY secret" }, { status: 500 });
        }
        const headers = {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${key}`,
          "Accept": "application/json",
        };

        // --- ADD (timeout wrapper) ---
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort("timeout"), CONFIG.TIMEOUT_MS);

        let response;
        try {
          response = await fetch(CONFIG.OPENAI_URL, {
            method: "POST",
            headers,
            body: JSON.stringify({
              model: CONFIG.MODEL,
              messages: [
                { role: "system", content: prompt },
                { role: "user", content: `Transcript:\n${transcript}` },
              ],
              temperature: 0.7,
            }),
            signal: controller.signal,
          });
        } catch (e) {
          clearTimeout(timeout);
          const reason = e?.name === "AbortError" ? "OpenAI request timed out" : (e?.message || "OpenAI request failed");
          return Response.json({ ok: false, error: reason }, { status: 504 });
        } finally {
          clearTimeout(timeout);
        }

        if (!response.ok) {
          const errorText = await response.text().catch(() => "");
          return Response.json(
            { ok: false, error: `OpenAI error ${response.status}`, detail: errorText?.slice(0, 2000) },
            { status: 502 }
          );
        }

        const { choices } = await response.json();
        const fullText = choices?.[0]?.message?.content || '';

        // Extract output parts using regex
        const hashtagsMatch = fullText.match(/(?<=1\)\s*Hashtags:)([\s\S]*?)(?=\n\s*2\)|$)/i);
        const hookMatch = fullText.match(/\*\*Hook:\*\*\s*(.*)/i);
        const bodyMatch = fullText.match(/\*\*Body:\*\*\s*(.*)/i);
        const ctaMatch = fullText.match(/\*\*Call to Action:\*\*\s*(.*)/i);


        return Response.json({
          ok: true,
          hashtags: hashtagsMatch?.[1]?.trim() || '',
          hook: hookMatch?.[1]?.trim() || '',
          body: bodyMatch?.[1]?.trim() || '',
          cta: ctaMatch?.[1]?.trim() || ''
        });

      } catch (err) {
        return Response.json(
          { ok: false, error: 'Unhandled error', detail: err?.message || String(err) },
          { status: 500 }
        );
      }

    }
    return Response.json({ ok: false, error: 'Not Found' }, { status: 404 });
  }
};
