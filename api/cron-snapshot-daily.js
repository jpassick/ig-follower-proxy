// api/cron-snapshot-daily.js
const RAPIDAPI_KEY = process.env.RAPIDAPI_KEY;
const KV_URL = process.env.KV_REST_API_URL;
const KV_TOKEN = process.env.KV_REST_API_TOKEN;

async function kvGet(key) {
  const r = await fetch(`${KV_URL}/get/${key}`, {
    headers: { Authorization: `Bearer ${KV_TOKEN}` }
  });
  const data = await r.json();
  if (!data.result) return null;
  let val = data.result;
  while (typeof val === 'string') {
    try { val = JSON.parse(val); } catch(e) { break; }
  }
  if (Array.isArray(val) && val.length === 1 && typeof val[0] === 'string') {
    try { val = JSON.parse(val[0]); } catch(e) {}
  }
  return val;
}

async function kvSet(key, value) {
  const r = await fetch(`${KV_URL}/set/${key}`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${KV_TOKEN}`, 'Content-Type': 'application/json' },
    body: JSON.stringify([JSON.stringify(value)])
  });
  if (!r.ok) {
    const text = await r.text();
    throw new Error(`kvSet failed for key ${key}: ${r.status} ${text}`);
  }
}

async function fetchFollowerCount(handle) {
  const url = `https://instagram-best-experience.p.rapidapi.com/user/info?username=${handle}`;
  const res = await fetch(url, {
    headers: {
      'x-rapidapi-key': RAPIDAPI_KEY,
      'x-rapidapi-host': 'instagram-best-experience.p.rapidapi.com',
    },
  });
  if (!res.ok) throw new Error(`RapidAPI error for ${handle}: ${res.status}`);
  const data = await res.json();
  const user = data?.data?.user;
  if (!user) throw new Error(`No user data for ${handle}`);
  return {
    followers: user.edge_followed_by?.count ?? null,
    profilePic: user.profile_pic_url_hd || user.profile_pic_url || null,
  };
}

export default async function handler(req, res) {
  const authHeader = req.headers['authorization'];
  if (authHeader !== `Bearer ${process.env.CRON_SECRET}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  try {
    const roster = await kvGet('roster');
    if (!Array.isArray(roster) || roster.length === 0) {
      console.log('No artists in roster or failed to load roster');
      return res.status(200).json({ message: 'No artists in roster' });
    }
    console.log(`Starting daily cron for ${roster.length} artists`);
    const now = Date.now();
    const results = [];
    const errors = [];

    for (const artist of roster) {
      if (!artist.handle || typeof artist.handle !== 'string' || !artist.handle.trim()) {
        console.warn('Skipping artist with invalid handle:', JSON.stringify(artist));
        continue;
      }
      try {
        const { followers, profilePic } = await fetchFollowerCount(artist.handle);
        artist.followers = followers;
        if (profilePic) artist.profilePic = profilePic;
        artist.updatedAt = now;
        results.push({ handle: artist.handle, followers });
        console.log(`✓ ${artist.handle}: ${followers}`);
        await new Promise(r => setTimeout(r, 1100));
      } catch (err) {
        errors.push({ handle: artist.handle, error: err.message });
        console.error(`✗ ${artist.handle}: ${err.message}`);
      }
    }

    await kvSet('roster', roster);

    let snapshots = await kvGet('snapshots');
    if (!Array.isArray(snapshots)) snapshots = [];
    const dayOfWeek = new Date().getUTCDay();
    const snapshot = {
  date: new Date().toISOString(),
  ts: Date.now(),
  type: dayOfWeek === 1 ? 'weekly' : 'daily',
  data: roster
    .filter(a => a.handle && a.followers != null)
    .map(a => ({ handle: a.handle, followers: a.followers })),
};
    snapshots.push(snapshot);
    if (snapshots.length > 120) snapshots.splice(0, snapshots.length - 120);
    await kvSet('snapshots', snapshots);

    console.log(`Daily cron complete: ${results.length} refreshed, ${errors.length} errors`);
    return res.status(200).json({
      success: true,
      refreshed: results.length,
      errors,
      snapshotType: snapshot.type,
      timestamp: new Date().toISOString(),
    });
  } catch (err) {
    console.error('Daily cron failed:', err);
    return res.status(500).json({ error: err.message });
  }
}
