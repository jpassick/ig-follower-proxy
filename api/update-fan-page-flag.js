export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const url = process.env.KV_REST_API_URL;
  const token = process.env.KV_REST_API_TOKEN;
  const adminPassword = process.env.ADMIN_PASSWORD;
  if (!url || !token) return res.status(500).json({ error: 'Redis not configured' });
  if (!adminPassword) return res.status(500).json({ error: 'ADMIN_PASSWORD not configured' });

  let body = req.body;
  if (typeof body === 'string') {
    try { body = JSON.parse(body); } catch(e) { body = {}; }
  }

  if (body?.password !== adminPassword) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const igHandle = typeof body?.handle === 'string' ? body.handle.trim().toLowerCase() : null;
  if (!igHandle) return res.status(400).json({ error: 'Missing handle parameter (artist IG handle)' });

  if (typeof body?.isFanPage !== 'boolean') {
    return res.status(400).json({ error: 'Missing or invalid isFanPage parameter (must be boolean true or false)' });
  }
  const isFanPage = body.isFanPage;

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

  if (roster.length === 0) {
    return res.status(404).json({ error: 'Roster is empty or could not be read' });
  }

  const idx = roster.findIndex(a => a && typeof a.handle === 'string' && a.handle.toLowerCase() === igHandle);
  if (idx === -1) {
    return res.status(404).json({ error: `Artist not found in roster: ${igHandle}` });
  }

  roster[idx] = {
    ...roster[idx],
    is_fan_page: isFanPage
  };

  const clean = roster.filter(a => a && typeof a.handle === 'string' && a.handle.length > 0);

  const writeR = await fetch(`${url}/set/roster`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify([JSON.stringify(clean)])
  });

  if (!writeR.ok) {
    return res.status(500).json({ error: 'Failed to write roster', status: writeR.status });
  }

  return res.status(200).json({
    ok: true,
    action: isFanPage ? 'flagged_as_fan_page' : 'unflagged_as_fan_page',
    artist: clean[idx]
  });
}
