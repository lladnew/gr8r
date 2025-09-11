// v1.0.3 gr8r-socialcopy-worker simply changed capitalization of default Pivot Year hashtag to match BriannaWeist
// v1.0.2 gr8r-socialcopy-worker
// updated line 20 to match any case of the string pivot year
// tweaked prompt to always include GR8R hashtag
// v1.0.1 gr8r-socialcopy-worker
// Generates Social Copy (Hashtags, Hook, Body, CTA) from transcript
// Adjusted: Static hashtags included only if title contains 'Pivot Year'

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const pathname = url.pathname;

    if (pathname === '/api/socialcopy' && request.method === 'POST') {
      try {
        const { transcript, title } = await request.json();

        if (!transcript || !title) {
          return new Response('Missing required fields: transcript or title', { status: 400 });
        }

        const isPivotYear = title.toLowerCase().includes('pivot year');
        const prompt = `Your Tasks:
1. Generate Hashtags
- Always include the static hashtag: #GR8R
- ${(isPivotYear
  ? `Also include these static hashtags: #ThePivotYear #briannawiest and generate 3 additional trending hashtags based on the video’s key themes.`
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

        const response = await fetch('https://api.openai.com/v1/chat/completions', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${env.OPENAI_API_KEY}`
          },
          body: JSON.stringify({
            model: 'gpt-4o',
            messages: [
              { role: 'system', content: prompt },
              { role: 'user', content: `Transcript:\n${transcript}` }
            ],
            temperature: 0.7
          })
        });

        if (!response.ok) {
          const errorText = await response.text();
          return new Response(`OpenAI error: ${response.status} ${errorText}`, { status: 502 });
        }

        const { choices } = await response.json();
        const fullText = choices?.[0]?.message?.content || '';

        // Extract output parts using regex
        const hashtagsMatch = fullText.match(/(?<=1\) Hashtags:)([\s\S]*?)(?=2\)|$)/);
        const hookMatch = fullText.match(/\*\*Hook:\*\*\s*(.*)/);
        const bodyMatch = fullText.match(/\*\*Body:\*\*\s*(.*)/);
        const ctaMatch = fullText.match(/\*\*Call to Action:\*\*\s*(.*)/);

        return Response.json({
          hashtags: hashtagsMatch?.[1]?.trim() || '',
          hook: hookMatch?.[1]?.trim() || '',
          body: bodyMatch?.[1]?.trim() || '',
          cta: ctaMatch?.[1]?.trim() || ''
        });
      } catch (err) {
        return new Response(`Error generating social copy: ${err.message}`, { status: 500 });
      }
    }

    return new Response('Not Found', { status: 404 });
  }
};
