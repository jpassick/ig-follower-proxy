// Daily cron for YouTube, X/Twitter, Facebook. Reads platform from ?platform= query string.
// TikTok stays on its own cron (cron-tiktok-snapshot-daily.js) — don't touch.

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

  const cronSecret = process.env.CRON_SECRET;
  const authHeader = req.headers.authorization || '';
  const expected = `Bearer ${cronSecret}`;
  if (cronSecret && authHeader !== expected) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const platform = typeof req.query?.platform === 'string' ? req.query.platform.trim().toLowerCase() : null;
  if (!platform || !PLATFORMS[platform]) {
    return res.status(400).json({
      error: `Missing or invalid platform query param. Must be one of: ${Object.keys(PLATFORMS).join(', ')}`
    });
  }
  const config = PLATFORMS[platform];

  const url = process.env.KV_REST_API_URL;
  const token = process.env.KV_REST_API_TOKEN;
  const smmKey = process.env.SMM_RAPIDAPI_KEY;
  if (!url || !token) return res.status(500).json({ error: 'Redis not configured' });
  if (!smmKey) return res.status(500).json({ error: 'SMM_RAPIDAPI_KEY not configured' });

  const startTime = Date.now();
  const TIME_BUDGET_MS = 270 * 1000;

  const fHandle      = `${platform}_handle`;
  const fMasterID    = `${platform}_masterID`;
  const fNickname    = `${platform}_nickname`;
  const fProfilePic  = `${platform}_profilePic`;
  const fFollowers   = `${platform}_followers`;
  const fStatus      = `${platform}_status`;
  const fLastUpdated = `${platform}_lastUpdated`;

  const snapshotsKey = `${platform}-snapshots`;
  const lastRefreshedKey = `${platform}-last-refreshed`;

  // Fetch helper — masterID fast path with URL fallback. Retry once after 2s on failure.
  async function fetchProfile({ masterID, handle }) {
    const useFastPath = !!masterID;
    const apiUrl = useFastPath
      ? `https://social-media-master.p.rapidapi.com/universal-profile-id?id=${encodeURIComponent(masterID)}`
      : `https://social-media-master.p.rapidapi.com${config.handleEndpoint}?url=${encodeURIComponent(config.profileUrl(handle))}`;

    const headers = {
      'x-rapidapi-host': 'social-media-master.p.rapidapi.com',
      'x-rapidapi-key': smmKey,
      'Content-Type': 'application/json'
    };

    for (let attempt = 1; attempt <= 2; attempt++) {
      try {
        const r = await fetch(apiUrl, { headers });
        if (r.ok) {
          const json = await r.json();
          // Universal endpoint always returns data.profile.* (normalized).
          // URL fallback uses platform-specific wrapper key.
          const wrapper = useFastPath ? json?.profile : json?.[config.wrapperKey];
          const stats = json?.stats;
          if (wrapper && stats) {
            return { ok: true, wrapper, stats, attempt };
          }
          return { ok: false, status: 'not_found', attempt };
        }
        if (attempt === 1) {
          await new Promise(resolve => setTimeout(resolve, 2000));
          continue;
        }
        return { ok: false, status: `error_${r.status}`, attempt };
      } catch (err) {
        if (attempt === 1) {
          await new Promise(resolve => setTimeout(resolve, 2000));
          continue;
        }
        return { ok: false, status: 'fetch_error', attempt };
      }
    }
  }

  // Read roster
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

  // Filter to artists with this platform's handle set
  const targets = roster
    .map((a, idx) => ({ artist: a, idx }))
    .filter(x => x.artist && typeof x.artist.handle === 'string' && typeof x.artist[fHandle] === 'string' && x.artist[fHandle].length > 0);

  if (targets.length === 0) {
    await fetch(`${url}/set/${lastRefreshedKey}`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(Date.now())
    });
    return res.status(200).json({
      ok: true,
      platform,
      message: `No artists have ${fHandle} set yet - nothing to snapshot`,
      duration_ms: Date.now() - startTime
    });
  }

  const snapshotEntry = {
    ts: Date.now(),
    date: new Date().toISOString().slice(0, 10),
    artists: {}
  };

  let successCount = 0;
  let failCount = 0;
  let retryCount = 0;
  let noMasterIDFallbacks = 0;
  let skippedTimeout = 0;
  let processedTargets = 0;

  for (let i = 0; i < targets.length; i++) {
    if (Date.now() - startTime > TIME_BUDGET_MS) {
      skippedTimeout = targets.length - i;
      break;
    }

    const { artist, idx } = targets[i];
    const socialHandle = artist[fHandle];
    const masterID = artist[fMasterID] || null;
    const now = Date.now();
    processedTargets++;

    if (!masterID) noMasterIDFallbacks++;

    const result = await fetchProfile({ masterID, handle: socialHandle });

    if (result.attempt > 1) retryCount++;

    if (result.ok) {
      const { wrapper, stats } = result;
      const newMasterID = wrapper.masterID || masterID || null;

      roster[idx] = {
        ...roster[idx],
        [fFollowers]: stats.followersCount ?? 0,
        [fNickname]: wrapper.name ?? socialHandle,
        [fProfilePic]: wrapper.image ?? null,
        [fStatus]: 'ok',
        [fLastUpdated]: now,
        [fMasterID]: newMasterID
      };

      snapshotEntry.artists[artist.handle] = {
        [fHandle]: socialHandle,
        followers: stats.followersCount ?? 0,
        status: 'ok'
      };

      successCount++;
    } else {
      roster[idx] = {
        ...roster[idx],
        [fStatus]: result.status,
        [fLastUpdated]: now
      };
      snapshotEntry.artists[artist.handle] = {
        [fHandle]: socialHandle,
        followers: roster[idx][fFollowers] ?? null,
        status: result.status
      };
      failCount++;
    }

    if (i < targets.length - 1) {
      await new Promise(resolve => setTimeout(resolve, 1100));
    }
  }

  // Write updated roster
  const cleanRoster = roster.filter(a => a && typeof a.handle === 'string' && a.handle.length > 0);
  const writeRoster = await fetch(`${url}/set/roster`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify([JSON.stringify(cleanRoster)])
  });

  if (!writeRoster.ok) {
    return res.status(500).json({
      error: 'Failed to write roster',
      status: writeRoster.status,
      stats: { successCount, failCount, retryCount, noMasterIDFallbacks, skippedTimeout }
    });
  }

  // Read snapshots, replace today's entry or append, cap at 1095
  const snapR = await fetch(`${url}/get/${snapshotsKey}`, {
    headers: { Authorization: `Bearer ${token}` }
  });
  const snapData = await snapR.json();

  let snapshots = [];
  if (snapData.result) {
    let val = snapData.result;
    while (typeof val === 'string') {
      try { val = JSON.parse(val); } catch(e) { break; }
    }
    if (Array.isArray(val) && val.length === 1 && typeof val[0] === 'string') {
      try { val = JSON.parse(val[0]); } catch(e) {}
    }
    if (Array.isArray(val)) snapshots = val;
  }

  const today = snapshotEntry.date;
  const existingIndex = snapshots.findIndex(s => s && s.date === today);
  if (existingIndex >= 0) {
    snapshots[existingIndex] = snapshotEntry;
  } else {
    snapshots.push(snapshotEntry);
  }

  if (snapshots.length > 1095) {
    snapshots = snapshots.slice(-1095);
  }

  const writeSnap = await fetch(`${url}/set/${snapshotsKey}`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify([JSON.stringify(snapshots)])
  });

  if (!writeSnap.ok) {
    return res.status(500).json({
      error: 'Failed to write snapshots',
      status: writeSnap.status,
      stats: { successCount, failCount, retryCount, noMasterIDFallbacks, skippedTimeout }
    });
  }

  await fetch(`${url}/set/${lastRefreshedKey}`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(Date.now())
  });

  return res.status(200).json({
    ok: true,
    platform,
    targets_total: targets.length,
    targets_processed: processedTargets,
    successCount,
    failCount,
    retryCount,
    noMasterIDFallbacks,
    skippedTimeout,
    snapshot_count: snapshots.length,
    duration_ms: Date.now() - startTime
  });
}

export const config = {
  maxDuration: 300
};
