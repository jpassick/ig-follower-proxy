export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const url   = process.env.KV_REST_API_URL;
  const token = process.env.KV_REST_API_TOKEN;
  const adminPassword = process.env.ADMIN_PASSWORD;
  const smmKey = process.env.SMM_RAPIDAPI_KEY;

  if (!url || !token) return res.status(500).json({ error: 'Redis not configured' });
  if (!adminPassword) return res.status(500).json({ error: 'ADMIN_PASSWORD not configured' });
  if (!smmKey) return res.status(500).json({ error: 'SMM_RAPIDAPI_KEY not configured' });

  let body = req.body;
  if (typeof body === 'string') {
    try { body = JSON.parse(body); } catch(e) { body = {}; }
  }
  if (body?.password !== adminPassword) return res.status(401).json({ error: 'Unauthorized' });

  // dry_run=true means we log what we'd do without writing anything
  const dryRun = body?.dry_run === true;

  // Read roster
  const r = await fetch(`${url}/get/roster`, { headers: { Authorization: `Bearer ${token}` } });
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
  if (!roster.length) return res.status(404).json({ error: 'Roster empty or unreadable' });

  const smmHeaders = {
    'x-rapidapi-host': 'social-media-master.p.rapidapi.com',
    'x-rapidapi-key': smmKey,
    'Content-Type': 'application/json'
  };

  const results = [];
  let updated = 0, skipped = 0, failed = 0;

  for (const artist of roster) {
    if (!artist || typeof artist.handle !== 'string') continue;

    // Skip fan pages and artists that already have a masterID
    if (artist.instagram_masterID) {
      skipped++;
      results.push({ handle: artist.handle, status: 'already_has_masterid', masterID: artist.instagram_masterID });
      continue;
    }

    const profileUrl = `https://www.instagram.com/${artist.handle}/`;
    const apiUrl = `https://social-media-master.p.rapidapi.com/instagram-user-account?url=${encodeURIComponent(profileUrl)}`;

    try {
      const resp = await fetch(apiUrl, { headers: smmHeaders });
      if (!resp.ok) {
        failed++;
        results.push({ handle: artist.handle, status: `error_${resp.status}` });
        continue;
      }
      const json = await resp.json();
      const profile = json?.profile;
      const masterID = profile?.masterID || null;

      if (!masterID) {
        failed++;
        results.push({ handle: artist.handle, status: 'no_masterid_in_response' });
        continue;
      }

      artist.instagram_masterID = masterID;
      updated++;
      results.push({ handle: artist.handle, status: 'ok', masterID });
    } catch(err) {
      failed++;
      results.push({ handle: artist.handle, status: `fetch_error: ${err.message}` });
    }

    // 300ms between calls — SMM is fast and we don't want to burn through quota
    await new Promise(r => setTimeout(r, 300));
  }

  // Write back unless dry run
  if (!dryRun && updated > 0) {
    const clean = roster.filter(a => a && typeof a.handle === 'string' && a.handle.length > 0);
    const writeR = await fetch(`${url}/set/roster`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify([JSON.stringify(clean)])
    });
    if (!writeR.ok) {
      return res.status(500).json({ error: 'Failed to write roster back to Redis', results });
    }
  }

  return res.status(200).json({
    ok: true,
    dry_run: dryRun,
    updated,
    skipped,
    failed,
    total: roster.length,
    results
  });
}
