const RAPIDAPI_KEY = process.env.RAPIDAPI_KEY;

async function fetchFollowerCount(handle) {
  const res = await fetch(`https://instagram-best-experience.p.rapidapi.com/profile?username=${encodeURIComponent(handle)}`, {
    headers: {
      'x-rapidapi-key': RAPIDAPI_KEY,
      'x-rapidapi-host': 'instagram-best-experience.p.rapidapi.com'
    }
  });
  const data = await res.json();
  const followers = data?.data?.followers_count ?? data?.followers_count ?? null;
  const profilePic = data?.data?.profile_pic_url ?? data?.profile_pic_url ?? '';
  return { followers, profilePic };
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

export default async function handler(req, res) {
  const authHeader = req.headers['authorization'];
  if (authHeader !== `Bearer ${process.env.CRON_SECRET}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const url = process.env.KV_REST_API_URL;
  const token = process.env.KV_REST_API_TOKEN;
  if (!url || !token) return res.status(500).json({ error: 'Redis not configured' });

  // Load roster
  const rosterRes = await fetch(`${url}/get/roster`, { headers: { Authorization: `Bearer ${token}` } });
  const rosterData = await rosterRes.json();
  let roster = [];
  if (rosterData.result) {
    let val = rosterData.result;
    while (typeof val === 'string') { try { val = JSON.parse(val); } catch(e) { break; } }
    if (Array.isArray(val) && val.length === 1 && typeof val[0] === 'string') { try { val = JSON.parse(val[0]); } catch(e) {} }
    if (Array.isArray(val)) roster = val;
  }
  if (!roster.length) return res.status(200).json({ message: 'No roster found' });

  // Refresh every artist from RapidAPI
  for (let i = 0; i < roster.length; i++) {
    try {
      const { followers, profilePic } = await fetchFollowerCount(roster[i].handle);
      if (followers != null) {
        roster[i].followers = followers;
        roster[i].status = 'ok';
        roster[i].updatedAt = Date.now();
      }
      if (profilePic) roster[i].profilePic = profilePic;
    } catch(e) {
      console.error(`Failed to refresh ${roster[i].handle}:`, e.message);
    }
    if (i < roster.length - 1) await sleep(1100);
  }

  // Save updated roster back to Redis
  await fetch(`${url}/set/roster`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify([JSON.stringify(roster)])
  });

  // Load existing snapshots
  const snapRes = await fetch(`${url}/get/snapshots`, { headers: { Authorization: `Bearer ${token}` } });
  const snapData = await snapRes.json();
  let snapshots = [];
  if (snapData.result) {
    let val = snapData.result;
    while (typeof val === 'string') { try { val = JSON.parse(val); } catch(e) { break; } }
    if (Array.isArray(val) && val.length === 1 && typeof val[0] === 'string') { try { val = JSON.parse(val[0]); } catch(e) {} }
    if (Array.isArray(val)) snapshots = val;
  }

  // Save snapshot of the fresh numbers
  const now = new Date();
  const snap = {
    date: now.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric', timeZone: 'America/New_York' }),
    ts: Date.now(),
    data: roster.filter(r => r.followers != null).map(r => ({ handle: r.handle, followers: r.followers }))
  };
  snapshots.push(snap);

  await fetch(`${url}/set/snapshots`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify([JSON.stringify(snapshots)])
  });

  return res.status(200).json({ ok: true, artists: snap.data.length, date: snap.date });
}
