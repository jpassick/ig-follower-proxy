export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const url = process.env.KV_REST_API_URL;
  const token = process.env.KV_REST_API_TOKEN;
  const adminPassword = process.env.ADMIN_PASSWORD;
  const apiKey = process.env.TIKTOK_RAPIDAPI_KEY;
  if (!url || !token) return res.status(500).json({ error: 'Redis not configured' });
  if (!adminPassword) return res.status(500).json({ error: 'ADMIN_PASSWORD not configured' });
  if (!apiKey) return res.status(500).json({ error: 'TIKTOK_RAPIDAPI_KEY not configured' });

  let body = req.body;
  if (typeof body === 'string') {
    try { body = JSON.parse(body); } catch(e) { body = {}; }
  }

  if (body?.password !== adminPassword) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const igHandle = typeof body?.handle === 'string' ? body.handle.trim().toLowerCase() : null;
  if (!igHandle) return res.status(400).json({ error: 'Missing handle parameter (artist IG handle)' });

  // tiktok_handle can be null/empty (to clear) or a string
  let newTiktokHandle = body?.tiktok_handle;
  if (newTiktokHandle === undefined) {
    return res.status(400).json({ error: 'Missing tiktok_handle parameter' });
  }
  if (newTiktokHandle === null || newTiktokHandle === '') {
    newTiktokHandle = null;
  } else if (typeof newTiktokHandle === 'string') {
    newTiktokHandle = newTiktokHandle.trim().replace(/^@/, '').toLowerCase();
    if (!newTiktokHandle) newTiktokHandle = null;
  } else {
    return res.status(400).json({ error: 'tiktok_handle must be a string or null' });
  }

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

  // Find the artist
  const idx = roster.findIndex(a => a && typeof a.handle === 'string' && a.handle.toLowerCase() === igHandle);
  if (idx === -1) {
    return res.status(404).json({ error: `Artist not found in roster: ${igHandle}` });
  }

  // If clearing handle, just save null and return
  if (newTiktokHandle === null) {
    roster[idx] = {
      ...roster[idx],
      tiktok_handle: null,
      tiktok_nickname: null,
      tiktok_profilePic: null,
      tiktok_followers: null,
      tiktok_status: null,
      tiktok_lastUpdated: Date.now()
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
      action: 'cleared',
      artist: clean[idx]
    });
  }

  // Otherwise, fetch TikTok data immediately so photo + follower count populate
  let fetchedData = null;
  let fetchError = null;
  try {
    const tiktokR = await fetch(
      `https://tiktok-best-experience.p.rapidapi.com/user/${encodeURIComponent(newTiktokHandle)}`,
      {
        headers: {
          'x-rapidapi-host': 'tiktok-best-experience.p.rapidapi.com',
          'x-rapidapi-key': apiKey
        }
      }
    );

    if (tiktokR.ok) {
      const tiktokJson = await tiktokR.json();
      if (tiktokJson.status === 'ok' && tiktokJson.data?.status_code === 0 && tiktokJson.data?.user) {
        const u = tiktokJson.data.user;
        let avatarUrl = null;
        if (u.avatar_300x300?.url_list?.length > 0) avatarUrl = u.avatar_300x300.url_list[0];
        else if (u.avatar_medium?.url_list?.length > 0) avatarUrl = u.avatar_medium.url_list[0];
        else if (u.avatar_thumb?.url_list?.length > 0) avatarUrl = u.avatar_thumb.url_list[0];

        fetchedData = {
          followers: u.follower_count ?? 0,
          nickname: u.nickname ?? newTiktokHandle,
          avatar_url: avatarUrl
        };
      } else {
        fetchError = `TikTok API returned no user data for "${newTiktokHandle}"`;
      }
    } else {
      fetchError = `RapidAPI returned status ${tiktokR.status}`;
    }
  } catch (err) {
    fetchError = `Fetch failed: ${err.message}`;
  }

  // Update roster with handle (and data if fetched successfully)
  const now = Date.now();
  roster[idx] = {
    ...roster[idx],
    tiktok_handle: newTiktokHandle,
    tiktok_nickname: fetchedData?.nickname ?? roster[idx].tiktok_nickname ?? null,
    tiktok_profilePic: fetchedData?.avatar_url ?? roster[idx].tiktok_profilePic ?? null,
    tiktok_followers: fetchedData?.followers ?? roster[idx].tiktok_followers ?? null,
    tiktok_status: fetchedData ? 'ok' : 'pending',
    tiktok_lastUpdated: now
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
    action: fetchedData ? 'updated_with_data' : 'updated_handle_only',
    artist: clean[idx],
    fetch_error: fetchError
  });
}
