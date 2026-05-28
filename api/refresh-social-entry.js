// Re-fetch a single platform-roster entry via SMM and update it in place.
// Admin-gated. POST { platform, handle, password }  (or x-admin-password header).

const PLATFORMS = {
  tiktok:   { rosterKey: 'tiktok-roster',   urlPath: '/tiktok-user-account',     wrapper: 'profile', url: h => `https://www.tiktok.com/@${h}` },
  youtube:  { rosterKey: 'youtube-roster',  urlPath: '/youtube-channel-details', wrapper: 'channel', url: h => `https://www.youtube.com/@${h}` },
  twitter:  { rosterKey: 'twitter-roster',  urlPath: '/twitter-user-account',    wrapper: 'profile', url: h => `https://x.com/${h}` },
  facebook: { rosterKey: 'facebook-roster', urlPath: '/facebook-user-account',   wrapper: 'profile', url: h => `https://www.facebook.com/${h}` }
};

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
  while (typeof val === 'string') { try { val = JSON.parse(val); } catch (e) { break; } }
  if (Array.isArray(val) && val.length === 1 && typeof val[0] === 'string') {
    try { val = JSON.parse(val[0]); } catch (e) { /* keep */ }
  }
  while (typeof val === 'string') { try { val = JSON.parse(val); } catch (e) { break; } }
  return val;
}

async function kvSet(key, value) {
  const body = JSON.stringify([JSON.stringify(value)]);
  const r = await fetch(`${KV_URL}/set/${encodeURIComponent(key)}`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${KV_TOKEN}`, 'Content-Type': 'application/json' },
    body
  });
  return r.ok;
}

async function smmByMasterID(masterID) {
  const url = `https://${SMM_HOST}/universal-profile-id?id=${encodeURIComponent(masterID)}`;
  const r = await fetch(url, { headers: { 'x-rapidapi-host': SMM_HOST, 'x-rapidapi-key': SMM_KEY } });
  if (!r.ok) return { ok: false, status: `error_${r.status}` };
  const data = await r.json();
  const p = data && data.profile;
  if (!p || !p.masterID) return { ok: false, status: 'not_found' };
  return {
    ok: true, masterID: p.masterID, nickname: p.name || null, profilePic: p.image || null,
    followers: (data.stats && typeof data.stats.followersCount === 'number') ? data.stats.followersCount : null
  };
}

async function smmByURL(handle, platform) {
  const url = `https://${SMM_HOST}${platform.urlPath}?url=${encodeURIComponent(platform.url(handle))}`;
  const r = await fetch(url, { headers: { 'x-rapidapi-host': SMM_HOST, 'x-rapidapi-key': SMM_KEY } });
  if (!r.ok) return { ok: false, status: `error_${r.status}` };
  const data = await r.json();
  const w = data && data[platform.wrapper];
  if (!w || !w.masterID) return { ok: false, status: 'not_found' };
  return {
    ok: true, masterID: w.masterID, nickname: w.name || null, profilePic: w.image || null,
    followers: (data.stats && typeof data.stats.followersCount === 'number') ? data.stats.followersCount : null
  };
}

function isAdmin(req) {
  const pw = req.headers['x-admin-password'] || (req.body && req.body.password) || (req.query && req.query.pw);
  return pw === ADMIN_PW;
}

export default async function handler(req, res) {
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });
  if (!isAdmin(req)) return res.status(401).json({ error: 'Unauthorized' });

  const body = req.body || {};
  const platform = PLATFORMS[body.platform];
  if (!platform) return res.status(400).json({ error: 'invalid platform' });
  const handle = (body.handle || '').trim().replace(/^@/, '').toLowerCase();
  if (!handle) return res.status(400).json({ error: 'handle required' });

  try {
    const roster = await kvGet(platform.rosterKey);
    if (!Array.isArray(roster)) return res.status(500).json({ error: 'roster read failed; aborting' });
    const idx = roster.findIndex(e => e.handle === handle);
    if (idx === -1) return res.status(404).json({ error: 'handle not in roster' });
    const entry = roster[idx];

    let lookup;
    if (entry.masterID) {
      lookup = await smmByMasterID(entry.masterID);
      if (!lookup.ok) lookup = await smmByURL(handle, platform);
    } else {
      lookup = await smmByURL(handle, platform);
    }

    if (lookup.ok) {
      entry.masterID = lookup.masterID || entry.masterID;
      if (lookup.nickname) entry.nickname = lookup.nickname;
      if (lookup.profilePic) entry.profilePic = lookup.profilePic;
      entry.followers = lookup.followers;
      entry.status = 'ok';
    } else {
      entry.status = lookup.status || 'error';
    }
    entry.updatedAt = Date.now();
    roster[idx] = entry;

    const ok = await kvSet(platform.rosterKey, roster);
    if (!ok) return res.status(500).json({ error: 'write failed' });
    return res.status(200).json({ ok: true, entry });
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
}
