export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const url = process.env.KV_REST_API_URL;
  const token = process.env.KV_REST_API_TOKEN;

  if (!url || !token) return res.status(500).json({ error: 'Redis not configured' });

  if (req.method === 'GET') {
    const r = await fetch(`${url}/get/roster`, {
      headers: { Authorization: `Bearer ${token}` }
    });
    const data = await r.json();
    const roster = data.result ? JSON.parse(data.result) : [];
    return res.status(200).json({ roster });
  }

  if (req.method === 'POST') {
    let body = req.body;
    if (typeof body === 'string') {
      try { body = JSON.parse(body); } catch(e) { body = {}; }
    }
    const roster = Array.isArray(body?.roster) ? body.roster : [];
    const clean = roster.filter(r => r && typeof r.handle === 'string' && r.handle.length > 0);
    const r = await fetch(`${url}/set/roster`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify([JSON.stringify(clean)])
    });
    const result = await r.json();
    return res.status(200).json({ ok: true, saved: clean.length });
  }

  return res.status(405).json({ error: 'Method not allowed' });
}
