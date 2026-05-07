export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const url = process.env.KV_REST_API_URL;
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

  if (body?.password !== adminPassword) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const dryRun = body?.dryRun === true;

  // Read current roster
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

  // Identify migration scope
  const withTiktokHandle = roster.filter(a => a && typeof a.tiktok_handle === 'string' && a.tiktok_handle.length > 0);
  const needsMigration = withTiktokHandle.filter(a => !a.tiktok_masterID);
  const alreadyMigrated = withTiktokHandle.filter(a => a.tiktok_masterID);

  // Dry run: report the plan without making API calls or writing
  if (dryRun) {
    return res.status(200).json({
      mode: 'dry_run',
      total_artists: roster.length,
      with_tiktok_handle: withTiktokHandle.length,
      already_have_masterID: alreadyMigrated.length,
      need_migration: needsMigration.length,
      estimated_duration_seconds: Math.ceil(needsMigration.length * 5.1),
      handles_to_migrate: needsMigration.map(a => a.tiktok_handle),
      handles_already_migrated: alreadyMigrated.map(a => ({
        tiktok_handle: a.tiktok_handle,
        tiktok_masterID: a.tiktok_masterID
      }))
    });
  }

  // Live run
  const results = {
    mode: 'live',
    total_artists: roster.length,
    with_tiktok_handle: withTiktokHandle.length,
    already_have_masterID: alreadyMigrated.length,
    needed_migration: needsMigration.length,
    succeeded: 0,
    failed: 0,
    failures: [],
    successes: []
  };

  for (let i = 0; i < roster.length; i++) {
    const artist = roster[i];
    if (!artist || typeof artist.tiktok_handle !== 'string' || !artist.tiktok_handle) continue;
    if (artist.tiktok_masterID) continue;

    const handle = artist.tiktok_handle;
    const tiktokUrl = `https://www.tiktok.com/@${handle}`;
    const apiUrl = `https://social-media-master.p.rapidapi.com/tiktok-user-account?url=${encodeURIComponent(tiktokUrl)}`;

    try {
      const apiRes = await fetch(apiUrl, {
        headers: {
          'x-rapidapi-host': 'social-media-master.p.rapidapi.com',
          'x-rapidapi-key': smmKey,
          'Content-Type': 'application/json'
        }
      });

      if (!apiRes.ok) {
        results.failed++;
        results.failures.push({ handle, error: `HTTP ${apiRes.status}` });
        await new Promise(r => setTimeout(r, 1100));
        continue;
      }

      const apiJson = await apiRes.json();
      const masterID = apiJson?.profile?.masterID;

      if (!masterID) {
        results.failed++;
        results.failures.push({ handle, error: 'No masterID in response' });
        await new Promise(r => setTimeout(r, 1100));
        continue;
      }

      roster[i] = {
        ...roster[i],
        tiktok_masterID: masterID
      };
      results.succeeded++;
      results.successes.push({ handle, tiktok_masterID: masterID });
    } catch (err) {
      results.failed++;
      results.failures.push({ handle, error: `Fetch failed: ${err.message}` });
    }

    await new Promise(r => setTimeout(r, 1100));
  }

  // Write updated roster only if any updates succeeded
  if (results.succeeded > 0) {
    const clean = roster.filter(a => a && typeof a.handle === 'string' && a.handle.length > 0);
    const writeR = await fetch(`${url}/set/roster`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify([JSON.stringify(clean)])
    });

    if (!writeR.ok) {
      return res.status(500).json({
        error: 'Failed to write updated roster to Redis',
        write_status: writeR.status,
        partial_results: results
      });
    }
  }

  return res.status(200).json(results);
}
