// Daily TikTok snapshot cron. Reads tiktok-roster, fetches SMM per artist,
// appends new snapshot entry keyed by platform handle. Preserves historical.

const PLATFORM = 'tiktok';
const ROSTER_KEY = 'tiktok-roster';
const SNAPSHOTS_KEY = 'tiktok-snapshots';
const LAST_REFRESHED_KEY = 'tiktok-last-refreshed';
const SMM_URL_PATH = '/tiktok-user-account';
const SMM_URL_WRAPPER = 'profile';
const PROFILE_URL = (handle) => `https://www.tiktok.com/@${handle}`;

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

async function smmByURL(handle) {
  const url = `https://${SMM_HOST}${SMM_URL_PATH}?url=${encodeURIComponent(PROFILE_URL(handle))}`;
  const r = await fetch(url, {
    headers: { 'x-rapidapi-host': SMM_HOST, 'x-rapidapi-key': SMM_KEY }
  });
  if (!r.ok) return { ok: false, status: `error_${r.status}` };
  const data = await r.json();
  const w = data && data[SMM_URL_WRAPPER];
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

export default async function handler(req, res) {
  const auth = req.headers.authorization || '';
  if (auth !== `Bearer ${CRON_SECRET}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  try {
    const roster = await kvGet(ROSTER_KEY);
    if (!Array.isArray(roster)) {
      return res.status(500).json({ error: 'roster not array; aborting', got: typeof roster });
    }
    if (roster.length === 0) {
      return res.status(200).json({ message: 'empty roster' });
    }

    // Read existing snapshots — preserve shape exactly to avoid touching historical data
    const rawSnapshots = await kvGet(SNAPSHOTS_KEY);
    if (rawSnapshots === null) {
      return res.status(500).json({ error: 'failed to read snapshots; aborting (no writes)' });
    }
    let snapshotsArray, snapshotsShape;
    if (Array.isArray(rawSnapshots)) {
      snapshotsArray = rawSnapshots;
      snapshotsShape = 'array';
    } else if (rawSnapshots && Array.isArray(rawSnapshots.snapshots)) {
      snapshotsArray = rawSnapshots.snapshots;
      snapshotsShape = 'wrapped';
    } else {
      return res.status(500).json({ error: 'unexpected snapshots shape; aborting' });
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
        if (!lookup.ok) lookup = await smmByURL(handle);
      } else {
        lookup = await smmByURL(handle);
      }

      if (lookup.ok) {
        entry.masterID = lookup.masterID || entry.masterID;
        if (lookup.nickname) entry.nickname = lookup.nickname;
        if (lookup.profilePic) entry.profilePic = lookup.profilePic;
        entry.followers = lookup.followers;
        entry.status = 'ok';
        entry.updatedAt = ts;
        okCount++;
        artists[handle] = { tiktok_handle: handle, followers: lookup.followers, status: 'ok' };
      } else {
        entry.status = lookup.status || 'error';
        entry.updatedAt = ts;
        errCount++;
        artists[handle] = {
          tiktok_handle: handle,
          followers: typeof entry.followers === 'number' ? entry.followers : null,
          status: lookup.status || 'error'
        };
      }
    }

    // Append new snapshot, preserve outer shape
    snapshotsArray.push({ ts, date, artists });
    const snapshotsValue = snapshotsShape === 'wrapped'
      ? { ...(typeof rawSnapshots === 'object' ? rawSnapshots : {}), snapshots: snapshotsArray }
      : snapshotsArray;

    const writes = await Promise.all([
      kvSet(ROSTER_KEY, roster),
      kvSet(SNAPSHOTS_KEY, snapshotsValue),
      kvSetTimestamp(LAST_REFRESHED_KEY, ts)
    ]);

    if (writes.some(w => !w)) {
      return res.status(500).json({ error: 'one or more writes failed', writes, ok: okCount, err: errCount });
    }

    return res.status(200).json({
      platform: PLATFORM, date, ts,
      total: roster.length, ok: okCount, errors: errCount
    });
  } catch (e) {
    return res.status(500).json({ error: e.message, stack: e.stack });
  }
}
