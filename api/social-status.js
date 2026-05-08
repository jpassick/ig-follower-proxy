const VALID_PLATFORMS = ['youtube', 'twitter', 'facebook'];

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  const url = process.env.KV_REST_API_URL;
  const token = process.env.KV_REST_API_TOKEN;
  if (!url || !token) return res.status(500).json({ error: 'Redis not configured' });

  const platform = typeof req.query?.platform === 'string' ? req.query.platform.trim().toLowerCase() : null;
  if (!platform || !VALID_PLATFORMS.includes(platform)) {
    return res.status(400).json({
      error: `Missing or invalid platform. Must be one of: ${VALID_PLATFORMS.join(', ')}`
    });
  }

  const lastRefreshedKey = `${platform}-last-refreshed`;

  const r = await fetch(`${url}/get/${lastRefreshedKey}`, {
    headers: { Authorization: `Bearer ${token}` }
  });
  const data = await r.json();

  let lastRefreshed = null;
  if (data.result) {
    let val = data.result;
    while (typeof val === 'string') {
      try { val = JSON.parse(val); } catch(e) { break; }
    }
    if (typeof val === 'number') lastRefreshed = val;
  }

  return res.status(200).json({
    platform,
    lastRefreshed,
    lastRefreshedISO: lastRefreshed ? new Date(lastRefreshed).toISOString() : null
  });
}
