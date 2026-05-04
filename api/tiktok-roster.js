export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  const url = process.env.KV_REST_API_URL;
  const token = process.env.KV_REST_API_TOKEN;
  if (!url || !token) return res.status(500).json({ error: 'Redis not configured' });

  const r = await fetch(`${url}/get/roster`, {
    headers: { Authorization: `Bearer ${token}` }
  });
  const data = await r.json();

  let roster = [];
  if (data.result) {
    let val = data.result;
    while (typeof val === 'string') {
      try { val = JSON.parse(val); } catch(e) { break; }
    }
    if (Array.isArray(val) && val.length === 1 && typeof val[0] === 'string') {
      try { val = JSON.parse(val[0]); } catch(e) {}
    }
    if (Array.isArray(val)) roster = val;
  }

  // Filter to valid roster entries with a handle (mirrors roster.js exactly)
  roster = roster.filter(a => a && typeof a.handle === 'string' && a.handle.length > 0);

  // Project to TikTok-relevant view: keep handle (the IG one acts as the artist key), 
  // plus all tiktok_* fields. Frontend uses this to render the TikTok view.
  const tiktokRoster = roster.map(a => ({
    handle: a.handle,
    tiktok_handle: a.tiktok_handle ?? null,
    tiktok_nickname: a.tiktok_nickname ?? null,
    tiktok_profilePic: a.tiktok_profilePic ?? null,
    tiktok_followers: a.tiktok_followers ?? null,
    tiktok_status: a.tiktok_status ?? null,
    tiktok_lastUpdated: a.tiktok_lastUpdated ?? null
  }));

  return res.status(200).json({ roster: tiktokRoster });
}
