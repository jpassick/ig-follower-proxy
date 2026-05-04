export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  const url = process.env.KV_REST_API_URL;
  const token = process.env.KV_REST_API_TOKEN;
  if (!url || !token) return res.status(500).json({ error: 'Redis not configured' });

  const r = await fetch(`${url}/get/tiktok-last-refreshed`, {
    headers: { Authorization: `Bearer ${token}` }
  });
  const data = await r.json();

  let lastRefreshed = null;
  if (data.result) {
    let val = data.result;
    // Unwrap if stringified
    while (typeof val === 'string') {
      try {
        const parsed = JSON.parse(val);
        val = parsed;
      } catch(e) {
        break;
      }
    }
    if (typeof val === 'number') lastRefreshed = val;
    else if (typeof val === 'string' && !isNaN(Number(val))) lastRefreshed = Number(val);
  }

  return res.status(200).json({ lastRefreshed });
}
