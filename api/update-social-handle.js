// Sets/updates a handle for youtube, twitter, or facebook.
// TikTok stays on its own file (update-tiktok-handle.js).

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

  // platform: 'youtube', 'twitter', or 'facebook'
  const platform = typeof body?.platform === 'string' ? body.platform.trim().toLowerCase() : null;
  if (!platform || !PLATFORMS[platform]) {
    return res.status(400).json({
      error: `Missing or invalid platform. Must be one of: ${Object.keys(PLATFORMS).join(', ')}`
    });
  }
  const config = PLATFORMS[platform];

  // handle: the artist's IG handle (their unique key in the roster)
  const igHandle = typeof body?.handle === 'string' ? body.handle.trim().toLowerCase() : null;
  if (!igHandle) return res.status(400).json({ error: 'Missing handle parameter (artist IG handle)' });

  // social_handle: the new handle for the specified platform (or null/empty to clear)
  let newSocialHandle = body?.social_handle;
  if (newSocialHandle === undefined) {
    return res.status(400).json({ error: 'Missing social_handle parameter' });
  }
  if (newSocialHandle === null || newSocialHandle === '') {
    newSocialHandle = null;
  } else if (typeof newSocialHandle === 'string') {
    newSocialHandle = newSocialHandle.trim().replace(/^@/, '').toLowerCase();
    if (!newSocialHandle) newSocialHandle = null;
  } else {
    return res.status(400).json({ error: 'social_handle must be a string or null' });
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

  // Find the artist by IG handle
  const idx = roster.findIndex(a => a && typeof a.handle === 'string' && a.handle.toLowerCase() === igHandle);
  if (idx === -1) {
    return res.status(404).json({ error: `Artist not found in roster: ${igHandle}` });
  }

  // Field names for this platform on the roster object
  const fHandle      = `${platform}_handle`;
  const fMasterID    = `${platform}_masterID`;
  const fNickname    = `${platform}_nickname`;
  const fProfilePic  = `${platform}_profilePic`;
  const fFollowers   = `${platform}_followers`;
  const fStatus      = `${platform}_status`;
  const fLastUpdated = `${platform}_lastUpdated`;

  const oldHandle = roster[idx][fHandle] ?? null;
  const handleChanged = oldHandle !== newSocialHandle;

  // If clearing handle, save nulls and return
  if (newSocialHandle === null) {
    roster[idx] = {
      ...roster[idx],
      [fHandle]: null,
      [fNickname]: null,
      [fProfilePic]: null,
      [fFollowers]: null,
      [fStatus]: null,
      [fLastUpdated]: Date.now(),
      [fMasterID]: null
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
      platform,
      artist: clean[idx]
    });
  }

  // Otherwise, fetch from SMM immediately so masterID + photo + follower count populate
  let fetchedData = null;
  let fetchError = null;
  const profileUrl = config.profileUrl(newSocialHandle);
  const apiUrl = `https://social-media-master.p.rapidapi.com${config.handleEndpoint}?url=${encodeURIComponent(profileUrl)}`;

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
      const wrapper = smmJson?.[config.wrapperKey];
      const stats = smmJson?.stats;
      if (wrapper && stats) {
        fetchedData = {
          masterID: wrapper.masterID || null,
          followers: stats.followersCount ?? 0,
          nickname: wrapper.name ?? newSocialHandle,
          avatar_url: wrapper.image ?? null
        };
      } else {
        fetchError = `SMM API returned 200 but missing ${config.wrapperKey}/stats for "${newSocialHandle}"`;
      }
    } else {
      fetchError = `RapidAPI returned status ${smmR.status}`;
    }
  } catch (err) {
    fetchError = `Fetch failed: ${err.message}`;
  }

  // When handle changes and fetch fails, stale values from the OLD handle would mislead the cron.
  // Null them out to force fresh resolution on next cron run.
  const preservedNickname   = handleChanged ? null : (roster[idx][fNickname] ?? null);
  const preservedProfilePic = handleChanged ? null : (roster[idx][fProfilePic] ?? null);
  const preservedFollowers  = handleChanged ? null : (roster[idx][fFollowers] ?? null);
  const preservedMasterID   = handleChanged ? null : (roster[idx][fMasterID] ?? null);

  const now = Date.now();
  roster[idx] = {
    ...roster[idx],
    [fHandle]: newSocialHandle,
    [fNickname]: fetchedData?.nickname ?? preservedNickname,
    [fProfilePic]: fetchedData?.avatar_url ?? preservedProfilePic,
    [fFollowers]: fetchedData?.followers ?? preservedFollowers,
    [fStatus]: fetchedData ? 'ok' : 'pending',
    [fLastUpdated]: now,
    [fMasterID]: fetchedData?.masterID ?? preservedMasterID
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
    platform,
    artist: clean[idx],
    fetch_error: fetchError
  });
}
