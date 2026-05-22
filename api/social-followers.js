const PLATFORMS = {
  tiktok: {
    handleEndpoint: '/tiktok-user-account',
    profileUrl: handle => `https://www.tiktok.com/@${handle}`,
    wrapperKey: 'profile'
  },
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
  const ytApiKey = process.env.YOUTUBE_API_KEY;
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

  if (platform === 'youtube' && !ytApiKey) {
    return res.status(500).json({ error: 'YOUTUBE_API_KEY not configured' });
  }

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
      error: `Artist ${igHandle} has no ${fHandle} set. Use update-social-handle (or update-tiktok-handle for TikTok) to add one first.`
    });
  }

  const smmHeaders = {
    'x-rapidapi-host': 'social-media-master.p.rapidapi.com',
    'x-rapidapi-key': smmKey,
    'Content-Type': 'application/json'
  };

  async function smmGet(apiUrl) {
    try {
      const r = await fetch(apiUrl, { headers: smmHeaders });
      if (r.ok) {
        const json = await r.json();
        return { ok: true, data: json };
      }
      return { ok: false, status: `error_${r.status}` };
    } catch (err) {
      return { ok: false, status: `fetch_error: ${err.message}` };
    }
  }

  // YouTube Data API v3 fetcher (channel-ID aware). One call returns subscriber count
  // + channel name + avatar. If the stored handle is a channel ID (UC...), look up by
  // id= — that works for channels with no modern @handle (e.g. PARTYNEXTDOOR).
  async function fetchYouTubeData(handle) {
    const cleanHandle = handle.replace(/^@+/, '');
    const isChannelId = /^UC[\w-]{22}$/.test(cleanHandle);
    const lookupParam = isChannelId
      ? `id=${encodeURIComponent(cleanHandle)}`
      : `forHandle=${encodeURIComponent(cleanHandle)}`;
    const apiUrl = `https://www.googleapis.com/youtube/v3/channels?part=statistics,snippet&${lookupParam}&key=${ytApiKey}`;
    try {
      const r = await fetch(apiUrl);
      if (!r.ok) return { ok: false, status: `error_${r.status}` };
      const json = await r.json();
      const items = json?.items;
      if (!Array.isArray(items) || items.length === 0) return { ok: false, status: 'not_found' };
      const channel = items[0];
      const stats = channel?.statistics || {};
      const snippet = channel?.snippet || {};
      const nickname = snippet.title || null;
      const avatar = snippet?.thumbnails?.default?.url
                  || snippet?.thumbnails?.medium?.url
                  || snippet?.thumbnails?.high?.url
                  || null;
      if (stats.hiddenSubscriberCount === true) {
        return { ok: false, status: 'hidden_count', nickname, avatar };
      }
      const subStr = stats.subscriberCount;
      const followers = typeof subStr === 'string' ? parseInt(subStr, 10)
                      : typeof subStr === 'number' ? subStr
                      : null;
      if (followers === null || Number.isNaN(followers) || followers < 0) {
        return { ok: false, status: 'invalid_count', nickname, avatar };
      }
      return { ok: true, followers, nickname, avatar };
    } catch (err) {
      return { ok: false, status: `fetch_error: ${err.message}` };
    }
  }

  let fetchedData = null;
  let fetchError = null;

  if (platform === 'youtube') {
    // -------- YouTube Data API v3 path --------
    const yt = await fetchYouTubeData(socialHandle);
    if (yt.ok) {
      fetchedData = {
        masterID: masterID || null,   // legacy SMM field — leave untouched
        followers: yt.followers,
        nickname: yt.nickname ?? socialHandle,
        avatar_url: yt.avatar ?? null,
        followers_source: 'youtube_data_api'
      };
    } else {
      fetchError = `YouTube API: ${yt.status}`;
    }
  } else {
    // -------- SMM path (tiktok / twitter / facebook) --------
    const useFastPath = !!masterID;
    const profileApiUrl = useFastPath
      ? `https://social-media-master.p.rapidapi.com/universal-profile-id?id=${encodeURIComponent(masterID)}`
      : `https://social-media-master.p.rapidapi.com${config.handleEndpoint}?url=${encodeURIComponent(config.profileUrl(socialHandle))}`;

    // For TikTok specifically: also pull from /tiktok-account-stats for the more accurate primary
    // follower count, mirroring cron-tiktok-snapshot-daily.js. When masterID is already known,
    // run both calls in parallel.
    let profileResp;
    let tiktokStatsResp = null;

    if (platform === 'tiktok' && useFastPath) {
      const statsApiUrl = `https://social-media-master.p.rapidapi.com/tiktok-account-stats?id=${encodeURIComponent(masterID)}&days=1`;
      [profileResp, tiktokStatsResp] = await Promise.all([
        smmGet(profileApiUrl),
        smmGet(statsApiUrl)
      ]);
    } else {
      profileResp = await smmGet(profileApiUrl);
      // First-time TikTok refresh (no stored masterID): discover masterID, then fetch account-stats.
      if (platform === 'tiktok' && profileResp.ok) {
        const discoveredMasterID = profileResp.data?.profile?.masterID;
        if (discoveredMasterID) {
          const statsApiUrl = `https://social-media-master.p.rapidapi.com/tiktok-account-stats?id=${encodeURIComponent(discoveredMasterID)}&days=1`;
          tiktokStatsResp = await smmGet(statsApiUrl);
        }
      }
    }

    if (profileResp.ok) {
      const smmJson = profileResp.data;
      const wrapper = useFastPath ? smmJson?.profile : smmJson?.[config.wrapperKey];
      const stats = smmJson?.stats;
      if (wrapper && stats) {
        let chosenFollowers = stats.followersCount ?? 0;
        let followersSource = 'profile-details';

        if (platform === 'tiktok' && tiktokStatsResp?.ok) {
          const sumF = tiktokStatsResp.data?.summaryStats?.followers;
          if (typeof sumF === 'number' && sumF > 0) {
            chosenFollowers = sumF;
            followersSource = 'account-stats';
          }
        }

        fetchedData = {
          masterID: wrapper.masterID || masterID || null,
          followers: chosenFollowers,
          nickname: wrapper.name ?? socialHandle,
          avatar_url: wrapper.image ?? null,
          followers_source: followersSource
        };
      } else {
        fetchError = `SMM API returned 200 but missing wrapper/stats for "${socialHandle}"`;
      }
    } else {
      fetchError = `RapidAPI ${profileResp.status}`;
    }
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
    followers_source: fetchedData?.followers_source ?? null,
    fetch_error: fetchError
  });
}
