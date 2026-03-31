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
    let roster = [];
    if (data.result) {
      let val = data.result;
      // unwrap as many times as needed
      while (typeof val === 'string') {
        try { val = JSON.parse(val); } catch(e) { break; }
      }
      // if it's an array with one string element, unwrap that too
      if (Array.isArray(val) && val.length === 1 && typeof val[0] === 'string') {
        try { val = JSON.parse(val[0]); } catch(e) {}
      }
      if (Array.isArray(val)) roster = val;
    }
    roster = roster.filter(r => r && typeof r.handle === 'string' && r.handle.length > 0);
    return res.status(200).json({ roster });
  }

  if (req.method === 'POST') {
    let body = req.body;
    if (typeof body === 'string') {
      try { body = JSON.parse(body); } catch(e) { body = {}; }
    }
    const roster = Array.isArray(body?.roster) ? body.roster : [];
    const clean = roster.filter(r => r && typeof r.handle === 'string' && r.handle.length > 0);
    await fetch(`${url}/set/roster`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify([JSON.stringify(clean)])
    });
    return res.status(200).json({ ok: true, saved: clean.length });
  }

  return res.status(405).json({ error: 'Method not allowed' });
}
