// Simple Secrets Store helper with a fixed 10-minute TTL cache.
// No purge hook, no env vars — minimal and consistent everywhere.

const SECRET_CACHE = new Map();
const TTL_MS = 10 * 60 * 1000;

export async function getSecret(env, bindingName) {
  const now = Date.now();

  // Fresh cache hit?
  const hit = SECRET_CACHE.get(bindingName);
  if (hit && hit.expiresAt > now) return hit.value;

  // Secrets Store binding must expose .get()
  const handle = env[bindingName];
  if (!handle || typeof handle.get !== "function") {
    throw new Error(`Secrets Store binding ${bindingName} missing or invalid (.get not found)`);
  }

  const raw = await handle.get();
  const val = (raw || "").trim();
  if (!val) throw new Error(`Secret ${bindingName} resolved empty`);

  SECRET_CACHE.set(bindingName, { value: val, expiresAt: now + TTL_MS });
  return val;
}

// Optional (useful for tests only)
export function _clearSecretCache() { SECRET_CACHE.clear(); }
