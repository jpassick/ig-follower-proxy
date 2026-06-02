// Daily YouTube/Twitter/Facebook snapshot cron. Loops the 3 platforms internally.
// YouTube uses Google YouTube Data API v3 (exact counts).
// Twitter/Facebook use SMM as before.

const SMM_PLATFORMS = [
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
const YOUTUBE_API_KEY = process.env.YOUTUBE_API_KEY;
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

// YouTube Data API v3 — supports both @handle and UC... channel ID
async function youtubeByHandle(handle) {
  if (!YOUTUBE_API_KEY) return { ok: false, status: 'no_api_key' };
  // Detect UC... channel ID vs @handle
  const isChannelId = /^UC[\w-]{22}$/.test(handle);
  const param = isChannelId
    ? `id=${encodeURIComponent(handle)}`
    : `forHandle=${encodeURIComponent(handle)}`;
  const url = `https://www.googleapis.com/youtube/v3/channels?part=statistics,snippet&${param}&key=${YOUTUBE_API_KEY}`;
  try {
    const r = await fetch(url);
    if (!r.ok) return { ok: false, status: `error_${r.status}` };
    const data = await r.json();
    if (!data.items || data.items.length === 0) return { ok: false, status: 'not_found' };
    const item = data.items[0];
    const subscribers = item.statistics?.subscriberCount;
    if (subscribers == null) return { ok: false, status: 'hidden_count' };
    return {
      ok: true,
      followers: parseInt(subscribers, 10),
      nickname: item.snippet?.title || null,
      profilePic: item.snippet?.thumbnails?.default?.url || null
    };
  } catch (e) {
    return { ok: false, status: 'fetch_error', message: e.message };
  }
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

async function processYoutube() {
  const platform = { name: 'youtube' };
  const roster = await kvGet('youtube-roster');
  if (!Array.isArray(roster)) return { platform: 'youtube', error: 'roster not array', skipped: true };
  if (roster.length === 0) return { platform: 'youtube', message: 'empty roster', skipped: true };

  const rawSnapshots = await kvGet('youtube-snapshots');
  if (rawSnapshots === null) return { platform: 'youtube', error: 'failed to read snapshots; skipped', skipped: true };
  let snapshotsArray, snapshotsShape;
  if (Array.isArray(rawSnapshots)) {
    snapshotsArray = rawSnapshots; snapshotsShape = 'array';
  } else if (rawSnapshots && Array.isArray(rawSnapshots.snapshots)) {
    snapshotsArray = rawSnapshots.snapshots; snapshotsShape = 'wrapped';
  } else {
    return { platform: 'youtube', error: 'unexpected snapshots shape; skipped', skipped: true };
  }

  const ts = Date.now();
  const date = utcDateString();
  const artists = {};
  let okCount = 0, errCount = 0;

  for (const entry of roster) {
    const handle = entry.handle;
    if (!handle) continue;

    const lookup = await youtubeByHandle(handle);

    if (lookup.ok) {
      entry.followers = lookup.followers;
      if (lookup.nickname) entry.nickname = lookup.nickname;
      if (lookup.profilePic) entry.profilePic = lookup.profilePic;
      entry.status = 'ok';
      entry.updatedAt = ts;
      okCount++;
      artists[handle] = { youtube_handle: handle, followers: lookup.followers, status: 'ok' };
    } else {
      entry.status = lookup.status || 'error';
      entry.updatedAt = ts;
      errCount++;
      artists[handle] = {
        youtube_handle: handle,
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
    kvSet('youtube-roster', roster),
    kvSet('youtube-snapshots', snapshotsValue),
    kvSetTimestamp('youtube-last-refreshed', ts)
  ]);

  if (writes.some(w => !w)) {
    return { platform: 'youtube', error: 'one or more writes failed', writes, ok: okCount, err: errCount };
  }
  return { platform: 'youtube', date, ts, total: roster.length, ok: okCount, errors: errCount };
}

async function processSmmPlatform(platform) {
  const roster = await kvGet(platform.rosterKey);
  if (!Array.isArray(roster)) return { platform: platform.name, error: 'roster not array', skipped: true };
  if (roster.length === 0) return { platform: platform.name, message: 'empty roster', skipped: true };

  const rawSnapshots = await kvGet(platform.snapshotsKey);
  if (rawSnapshots === null) return { platform: platform.name, error: 'failed to read snapshots; skipped', skipped: true };
  let snapshotsArray, snapshotsShape;
  if (Array.isArray(rawSnapshots)) {
    snapshotsArray = rawSnapshots; snapshotsShape = 'array';
  } else if (rawSnapshots && Array.isArray(rawSnapshots.snapshots)) {
    snapshotsArray = rawSnapshots.snapshots; snapshotsShape = 'wrapped';
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
      artists[handle] = { [`${platform.name}_handle`]: handle, followers: lookup.followers, status: 'ok' };
    } else {
      entry.status = lookup.status || 'error';
      entry.updatedAt = ts;
      errCount++;
      artists[handle] = {
        [`${platform.name}_handle`]: handle,
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
    results.push(await processYoutube());
    for (const platform of SMM_PLATFORMS) {
      results.push(await processSmmPlatform(platform));
    }
    return res.status(200).json({ results });
  } catch (e) {
    return res.status(500).json({ error: e.message, stack: e.stack });
  }
}

export const config = {
  maxDuration: 300
};
