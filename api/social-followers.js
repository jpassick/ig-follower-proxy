const PLATFORMS = {
  youtube: {
    handleEndpoint: '/youtube-channel-details',
    profileUrl: handle => `https://www.youtube.com/@${handle}`,
    wrapperKey: 'channel'
  },
  twitter: {
    handleEndpoint: '/twitter-user-account',
    profileUrl: handle => `https://x.com/${handle}`,
    wrapperKey: 'profile'
  },
  facebook: {
    handleEndpoint: '/facebook-user-account',
    profileUrl: handle => `https://www.facebook.com/${handle}`,
    wrapperKey: 'profile'
  }
};

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

  const platform = typeof body?.platform === 'string' ? body.platform.trim().toLowerCase() : null;
  if (!platform || !PLATFORMS[platform]) {
    return res.status(400).json({ error: `Missing or invalid platform. Must be one of: ${Object.keys(PLATFORMS).join(', ')}` });
  }
  const config = PLATFORMS[platform];

  const igHandle = typeof body?.handle === 'string' ? body.handle.trim().toLowerCase() : null;
  if (!igHandle) return res.status(400).json({ error: 'Missing handle parameter (artist IG handle)' });

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

  if (roster.length === 0) return res.status(404).json({ error: 'Roster is empty or could not be read' });

  const idx = roster.findIndex(a => a && typeof a.handle === 'string' && a.handle.toLowerCase() === igHandle);
  if (idx === -1) return res.status(404).json({ error: `Artist not found in roster: ${igHandle}` });

  const fHandle      = `${platform}_handle`;
  const fMasterID    = `${platform}_masterID`;
  const fNickname    = `${platform}_nickname`;
  const fProfilePic  = `${platform}_profilePic`;
  const fFollowers   = `${platform}_followers`;
  const fStatus      = `${platform}_status`;
  const fLastUpdated = `${platform}_lastUpdated`;

  const socialHandle = roster[idx][fHandle];
  const masterID = roster[idx][fMasterID] || null;

  if (!socialHandle) {
    return res.status(400).json({
      error: `Artist ${igHandle} has no ${fHandle} set. Use update-social-handle to add one first.`
    });
  }

  const useFastPath = !!masterID;
  const apiUrl = useFastPath
    ? `https://social-media-master.p.rapidapi.com/universal-profile-id?id=${encodeURIComponent(masterID)}`
    : `https://social-media-master.p.rapidapi.com${config.handleEndpoint}?url=${encodeURIComponent(config.profileUrl(socialHandle))}`;

  let fetchedData = null;
  let fetchError = null;

  try {
    const smmR = await fetch(apiUrl, {
      headers: {
        'x-rapidapi-host': 'social-media-master.p.rapidapi.com',
        'x-rapidapi-key': smmKey,
        'Content-Type': 'application/json'
      }
    });

    if (smmR.ok) {
      const smmJson = await smmR.json();
      const wrapper = useFastPath ? smmJson?.profile : smmJson?.[config.wrapperKey];
      const stats = smmJson?.stats;
      if (wrapper && stats) {
        fetchedData = {
          masterID: wrapper.masterID || masterID || null,
          followers: stats.followersCount ?? 0,
          nickname: wrapper.name ?? socialHandle,
          avatar_url: wrapper.image ?? null
        };
      } else {
        fetchError = `SMM API returned 200 but missing wrapper/stats for "${socialHandle}"`;
      }
    } else {
      fetchError = `RapidAPI returned status ${smmR.status}`;
    }
  } catch (err) {
    fetchError = `Fetch failed: ${err.message}`;
  }

  const now = Date.now();
  if (fetchedData) {
    roster[idx] = {
      ...roster[idx],
      [fFollowers]: fetchedData.followers,
      [fNickname]: fetchedData.nickname,
      [fProfilePic]: fetchedData.avatar_url,
      [fStatus]: 'ok',
      [fLastUpdated]: now,
      [fMasterID]: fetchedData.masterID
    };
  } else {
    roster[idx] = {
      ...roster[idx],
      [fStatus]: 'error',
      [fLastUpdated]: now
    };
  }

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
    ok: !!fetchedData,
    platform,
    artist: clean[idx],
    fetch_error: fetchError
  });
}
