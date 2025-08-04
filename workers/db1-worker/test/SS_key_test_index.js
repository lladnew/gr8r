export default {
  async fetch(_req, env, _ctx) {
    let val;
    let result = {};

    try {
      val = await env.DB1_INTERNAL_KEY.get(); // ✅ Secrets Store correct access
      result = {
        success: true,
        length: val?.length,
        start: val?.slice?.(0, 5),
        end: val?.slice?.(-5),
      };
    } catch (err) {
      result = {
        success: false,
        error: err.message || String(err),
      };
    }

    const body = JSON.stringify({ DB1_INTERNAL_KEY: result }, null, 2);

    return new Response(body, {
      headers: { "Content-Type": "application/json" },
    });
  }
};
