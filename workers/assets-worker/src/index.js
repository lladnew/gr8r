// v1.1.1 assets-worker:
// - ADDED: CORS headers on all responses
// - ADDED: Support for HEAD and OPTIONS (preflight)
// - RETAINED: GET handler for serving assets from R2
// - RETAINED: Simple caching header

const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET,HEAD,OPTIONS',
  'Access-Control-Allow-Headers': '*',
  'Access-Control-Max-Age': '3600',
  // Helps CDNs/browsers vary on origin if you ever tighten ACAO
  'Vary': 'Origin'
};

function withCors(headers = {}) {
  return { ...headers, ...CORS_HEADERS };
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const { pathname, hostname } = url;
    const method = request.method.toUpperCase();

    if (method === 'OPTIONS') {
      // CORS preflight / keep it simple
      return new Response(null, { status: 204, headers: withCors() });
    }

    if (method !== 'GET' && method !== 'HEAD') {
      return new Response('Not found', { status: 404, headers: withCors() });
    }

    const key = decodeURIComponent(pathname.slice(1));
    const bucket = hostname === 'videos.gr8r.com' ? env.VIDEOS_BUCKET : env.ASSETS_BUCKET;

    // Fetch object (use .get; it's fine for HEAD too, we just won't stream the body)
    const object = await bucket.get(key);
    if (!object) {
      return new Response('Not found', { status: 404, headers: withCors() });
    }

    const headers = withCors({
      'Content-Type': object.httpMetadata?.contentType || 'application/octet-stream',
      'Cache-Control': 'public, max-age=3600',
    });

    // Nice-to-have metadata, if available
    if (object.httpEtag) headers['ETag'] = object.httpEtag;
    if (object.uploaded) headers['Last-Modified'] = new Date(object.uploaded).toUTCString();
    if (typeof object.size === 'number') headers['Content-Length'] = String(object.size);

    // HEAD: send headers only
    if (method === 'HEAD') {
      return new Response(null, { status: 200, headers });
    }

    // GET: stream the body
    return new Response(object.body, { status: 200, headers });
  }
};
