// Daily YouTube/Twitter/Facebook snapshot cron. Loops the 3 platforms internally
// (no query strings in cron paths per Vercel constraint). Same pattern as TT cron.

const PLATFORMS = [
  {
    name: 'youtube',
    rosterKey: 'youtube-roster',
    snapshotsKey: 'youtube-snapshots',
    lastRefreshedKey: 'youtube-last-refreshed',
    smmUrlPath: '/youtube-channel-details',
    smmUrlWrapper: 'channel',
    profileUrl: (h) => `https://www.youtube.com/@${h}`
  },
  {
    name: 'twitter',
    rosterKey: 'twitter-roster',
    snapshotsKey: 'twitter-snapshots',
    lastRefreshedKey: 'twitter-last-refreshed',
    smmUrlPath: '/twitter-user-account',
    smmUrlWrapper: 'profile',
    profileUrl: (h) => `https://x.com/${h}`
  },
  {
    name: 'facebook',
    rosterKey: 'facebook-roster',
    snapshotsKey: 'facebook-snapshots',
    lastRefreshedKey: 'facebook-last-refreshed',
    smmUrlPath: '/facebook-user-account',
    smmUrlWrapper: 'profile',
    profileUrl: (h) => `https://www.facebook.com/${h}`
  }
];

const SMM_HOST = 'social-media-master.p.rapidapi.com';
const KV_URL = process.env.KV_REST_API_URL;
const KV_TOKEN = process.env.KV_REST_API_TOKEN;
const SMM_KEY = process.env.SMM_RAPIDAPI_KEY;
const CRON_SECRET = process.env.CRON_SECRET;

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
    headers: { Authorization: `Bearer ${KV_TOKEN}`, 'Content-Type': 'application/json' },
    body
  });
  return r.ok;
}

async function kvSetTimestamp(key, value) {
  const body = JSON.stringify(value);
  const r = await fetch(`${KV_URL}/set/${encodeURIComponent(key)}`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${KV_TOKEN}`, 'Content-Type': 'application/json' },
    body
  });
  return r.ok;
}

async function smmByMasterID(masterID) {
  const url = `https://${SMM_HOST}/universal-profile-id?id=${encodeURIComponent(masterID)}`;
  const r = await fetch(url, {
    headers: { 'x-rapidapi-host': SMM_HOST, 'x-rapidapi-key': SMM_KEY }
  });
  if (!r.ok) return { ok: false, status: `error_${r.status}` };
  const data = await r.json();
  const p = data && data.profile;
  if (!p || !p.masterID) return { ok: false, status: 'not_found' };
  return {
    ok: true,
    masterID: p.masterID,
    nickname: p.name || null,
    profilePic: p.image || null,
    followers: (data.stats && typeof data.stats.followersCount === 'number')
      ? data.stats.followersCount : null
  };
}

async function smmByURL(handle, platform) {
  const url = `https://${SMM_HOST}${platform.smmUrlPath}?url=${encodeURIComponent(platform.profileUrl(handle))}`;
  const r = await fetch(url, {
    headers: { 'x-rapidapi-host': SMM_HOST, 'x-rapidapi-key': SMM_KEY }
  });
  if (!r.ok) return { ok: false, status: `error_${r.status}` };
  const data = await r.json();
  const w = data && data[platform.smmUrlWrapper];
  if (!w || !w.masterID) return { ok: false, status: 'not_found' };
  return {
    ok: true,
    masterID: w.masterID,
    nickname: w.name || null,
    profilePic: w.image || null,
    followers: (data.stats && typeof data.stats.followersCount === 'number')
      ? data.stats.followersCount : null
  };
}

function utcDateString() {
  const d = new Date();
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth()+1).padStart(2,'0')}-${String(d.getUTCDate()).padStart(2,'0')}`;
}

async function processPlatform(platform) {
  const roster = await kvGet(platform.rosterKey);
  if (!Array.isArray(roster)) {
    return { platform: platform.name, error: 'roster not array', skipped: true };
  }
  if (roster.length === 0) {
    return { platform: platform.name, message: 'empty roster', skipped: true };
  }

  const rawSnapshots = await kvGet(platform.snapshotsKey);
  if (rawSnapshots === null) {
    return { platform: platform.name, error: 'failed to read snapshots; skipped', skipped: true };
  }
  let snapshotsArray, snapshotsShape;
  if (Array.isArray(rawSnapshots)) {
    snapshotsArray = rawSnapshots;
    snapshotsShape = 'array';
  } else if (rawSnapshots && Array.isArray(rawSnapshots.snapshots)) {
    snapshotsArray = rawSnapshots.snapshots;
    snapshotsShape = 'wrapped';
  } else {
    return { platform: platform.name, error: 'unexpected snapshots shape; skipped', skipped: true };
  }

  const ts = Date.now();
  const date = utcDateString();
  const artists = {};
  let okCount = 0, errCount = 0;

  for (const entry of roster) {
    const handle = entry.handle;
    if (!handle) continue;

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
      entry.updatedAt = ts;
      okCount++;
      artists[handle] = { followers: lookup.followers, status: 'ok' };
    } else {
      entry.status = lookup.status || 'error';
      entry.updatedAt = ts;
      errCount++;
      artists[handle] = {
        followers: typeof entry.followers === 'number' ? entry.followers : null,
        status: lookup.status || 'error'
      };
    }
  }

  snapshotsArray.push({ ts, date, artists });
  const snapshotsValue = snapshotsShape === 'wrapped'
    ? { ...(typeof rawSnapshots === 'object' ? rawSnapshots : {}), snapshots: snapshotsArray }
    : snapshotsArray;

  const writes = await Promise.all([
    kvSet(platform.rosterKey, roster),
    kvSet(platform.snapshotsKey, snapshotsValue),
    kvSetTimestamp(platform.lastRefreshedKey, ts)
  ]);

  if (writes.some(w => !w)) {
    return { platform: platform.name, error: 'one or more writes failed', writes, ok: okCount, err: errCount };
  }

  return { platform: platform.name, date, ts, total: roster.length, ok: okCount, errors: errCount };
}

export default async function handler(req, res) {
  const auth = req.headers.authorization || '';
  if (auth !== `Bearer ${CRON_SECRET}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  try {
    const results = [];
    for (const platform of PLATFORMS) {
      const r = await processPlatform(platform);
      results.push(r);
    }
    return res.status(200).json({ results });
  } catch (e) {
    return res.status(500).json({ error: e.message, stack: e.stack });
  }
}
