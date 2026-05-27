// Standalone Facebook roster: GET list, POST add+fetch, DELETE remove, PATCH update.
// Reads/writes Redis key `facebook-roster`.

const REDIS_KEY = 'facebook-roster';
const SMM_PATH = '/facebook-user-account';
const SMM_WRAPPER = 'profile';
const PROFILE_URL = (handle) => `https://www.facebook.com/${handle}`;

const SMM_HOST = 'social-media-master.p.rapidapi.com';
const KV_URL = process.env.KV_REST_API_URL;
const KV_TOKEN = process.env.KV_REST_API_TOKEN;
const SMM_KEY = process.env.SMM_RAPIDAPI_KEY;
const ADMIN_PW = process.env.ADMIN_PASSWORD;

async function kvGet(key) {
  const r = await fetch(`${KV_URL}/get/${encodeURIComponent(key)}`, {
    headers: { Authorization: `Bearer ${KV_TOKEN}` }
  });
  if (!r.ok) return null;
  const j = await r.json();
  let val = j.result;
  while (typeof val === 'string') {
    try { val = JSON.parse(val); } catch (e) { break; }
  }
  if (Array.isArray(val) && val.length === 1 && typeof val[0] === 'string') {
    try { val = JSON.parse(val[0]); } catch (e) { /* keep */ }
  }
  while (typeof val === 'string') {
    try { val = JSON.parse(val); } catch (e) { break; }
  }
  return val;
}

async function kvSet(key, value) {
  const body = JSON.stringify([JSON.stringify(value)]);
  const r = await fetch(`${KV_URL}/set/${encodeURIComponent(key)}`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${KV_TOKEN}`,
      'Content-Type': 'application/json'
    },
    body
  });
  return r.ok;
}

async function smmLookup(handle) {
  const url = `https://${SMM_HOST}${SMM_PATH}?url=${encodeURIComponent(PROFILE_URL(handle))}`;
  const r = await fetch(url, {
    headers: {
      'x-rapidapi-host': SMM_HOST,
      'x-rapidapi-key': SMM_KEY
    }
  });
  if (!r.ok) return { ok: false, status: `error_${r.status}` };
  const data = await r.json();
  const wrapped = data && data[SMM_WRAPPER];
  if (!wrapped || !wrapped.masterID) return { ok: false, status: 'not_found' };
  return {
    ok: true,
    masterID: wrapped.masterID,
    nickname: wrapped.name || null,
    profilePic: wrapped.image || null,
    followers: (data.stats && typeof data.stats.followersCount === 'number')
      ? data.stats.followersCount : null
  };
}

function isAdmin(req) {
  const pw = req.headers['x-admin-password'] || (req.query && req.query.pw);
  return pw === ADMIN_PW;
}

function normalizeHandle(h) {
  if (!h || typeof h !== 'string') return null;
  return h.trim().replace(/^@/, '').toLowerCase();
}

export default async function handler(req, res) {
  try {
    if (req.method === 'GET') {
      const list = await kvGet(REDIS_KEY);
      return res.status(200).json(Array.isArray(list) ? list : []);
    }

    if (req.method === 'POST') {
      if (!isAdmin(req)) return res.status(401).json({ error: 'Unauthorized' });
      const body = req.body || {};
      const handle = normalizeHandle(body.handle);
      if (!handle) return res.status(400).json({ error: 'handle required' });

      const current = (await kvGet(REDIS_KEY)) || [];
      if (current.find(e => e.handle === handle)) {
        return res.status(409).json({ error: 'handle already exists' });
      }

      const lookup = await smmLookup(handle);
      const entry = {
        handle,
        masterID: lookup.ok ? lookup.masterID : null,
        nickname: lookup.ok ? lookup.nickname : null,
        profilePic: lookup.ok ? lookup.profilePic : null,
        followers: lookup.ok ? lookup.followers : null,
        status: lookup.ok ? 'ok' : (lookup.status || 'pending'),
        updatedAt: Date.now(),
        is_fan_page: !!body.is_fan_page
      };

      current.push(entry);
      const ok = await kvSet(REDIS_KEY, current);
      if (!ok) return res.status(500).json({ error: 'write failed' });
      return res.status(200).json(entry);
    }

    if (req.method === 'DELETE') {
      if (!isAdmin(req)) return res.status(401).json({ error: 'Unauthorized' });
      const handle = normalizeHandle(
        (req.body && req.body.handle) || (req.query && req.query.handle)
      );
      if (!handle) return res.status(400).json({ error: 'handle required' });

      const current = (await kvGet(REDIS_KEY)) || [];
      const filtered = current.filter(e => e.handle !== handle);
      if (filtered.length === current.length) {
        return res.status(404).json({ error: 'handle not found' });
      }
      const ok = await kvSet(REDIS_KEY, filtered);
      if (!ok) return res.status(500).json({ error: 'write failed' });
      return res.status(200).json({ deleted: handle, remaining: filtered.length });
    }

    if (req.method === 'PATCH') {
      if (!isAdmin(req)) return res.status(401).json({ error: 'Unauthorized' });
      const body = req.body || {};
      const handle = normalizeHandle(body.handle);
      if (!handle) return res.status(400).json({ error: 'handle required' });

      const current = (await kvGet(REDIS_KEY)) || [];
      const idx = current.findIndex(e => e.handle === handle);
      if (idx === -1) return res.status(404).json({ error: 'handle not found' });

      if (typeof body.is_fan_page === 'boolean') {
        current[idx].is_fan_page = body.is_fan_page;
      }
      const ok = await kvSet(REDIS_KEY, current);
      if (!ok) return res.status(500).json({ error: 'write failed' });
      return res.status(200).json(current[idx]);
    }

    return res.status(405).json({ error: 'Method not allowed' });
  } catch (e) {
    return res.status(500).json({ error: e.message, stack: e.stack });
  }
}
