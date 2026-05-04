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

  // Read current roster (mirrors roster.js unwrapping logic exactly)
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

  // Add TikTok fields if not already present (idempotent)
  let modified = 0;
  let alreadyMigrated = 0;
  const migrated = roster.map(artist => {
    if (!artist || typeof artist.handle !== 'string') return artist;

    const updated = { ...artist };
    let changed = false;

    if (!('tiktok_handle' in updated)) {
      updated.tiktok_handle = null;
      changed = true;
    }
    if (!('tiktok_nickname' in updated)) {
      updated.tiktok_nickname = null;
      changed = true;
    }
    if (!('tiktok_profilePic' in updated)) {
      updated.tiktok_profilePic = null;
      changed = true;
    }
    if (!('tiktok_lastUpdated' in updated)) {
      updated.tiktok_lastUpdated = null;
      changed = true;
    }

    if (changed) modified++;
    else alreadyMigrated++;
    return updated;
  });

  const clean = migrated.filter(a => a && typeof a.handle === 'string' && a.handle.length > 0);

  // Write back using exact same pattern as roster.js POST
  const writeR = await fetch(`${url}/set/roster`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify([JSON.stringify(clean)])
  });

  if (!writeR.ok) {
    return res.status(500).json({
      error: 'Failed to write roster back to Redis',
      status: writeR.status
    });
  }

  return res.status(200).json({
    ok: true,
    total_artists: clean.length,
    newly_modified: modified,
    already_had_fields: alreadyMigrated,
    sample: clean.slice(0, 3)
  });
}
