const PLATFORMS = {
  youtube: {
    endpoint: '/youtube-channel-details',
    profileUrl: handle => `https://www.youtube.com/@${handle}`,
    wrapperKey: 'channel'
  },
  twitter: {
    endpoint: '/twitter-user-account',
    profileUrl: handle => `https://x.com/${handle}`,
    wrapperKey: 'profile'
  },
  facebook: {
    endpoint: '/facebook-user-account',
    profileUrl: handle => `https://www.facebook.com/${handle}`,
    wrapperKey: 'profile'
  }
};

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');

  const cronSecret = process.env.CRON_SECRET;
  const authHeader = req.headers?.authorization || '';
  if (cronSecret && authHeader !== `Bearer ${cronSecret}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const url = process.env.KV_REST_API_URL;
  const token = process.env.KV_REST_API_TOKEN;
  const smmKey = process.env.SMM_RAPIDAPI_KEY;
  if (!url || !token) return res.status(500).json({ error: 'Redis not configured' });
  if (!smmKey) return res.status(500).json({ error: 'SMM_RAPIDAPI_KEY not configured' });

  const startTime = Date.now();
  const TIME_BUDGET_MS = 270 * 1000;

  // SMM fetch — ALWAYS uses URL-based endpoint, never the masterID fast-path.
  // The masterID fast-path (/universal-profile-id) returns SMM-cached stale
  // follower counts that refresh only every several days. URL-based returns
  // fresh data per request.
  async function fetchProfile(platform, config, socialHandle) {
    const profileUrl = config.profileUrl(socialHandle);
    const apiUrl = `https://social-media-master.p.rapidapi.com${config.endpoint}?url=${encodeURIComponent(profileUrl)}`;
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
          const wrapper = json?.[config.wrapperKey];
          const stats = json?.stats;
          if (wrapper && stats) {
            return { ok: true, wrapper, stats, attempt };
          }
          // 200 but missing expected fields — no retry
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
  const rosterR = await fetch(`${url}/get/roster`, {
    headers: { Authorization: `Bearer ${token}` }
  });
  const rosterData = await rosterR.json();
  let roster = [];
  if (rosterData.result) {
    let val = rosterData.result;
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

  const today = new Date().toISOString().slice(0, 10);
  const results = [];

  for (const [platform, config] of Object.entries(PLATFORMS)) {
    if (Date.now() - startTime > TIME_BUDGET_MS) {
      results.push({ platform, skipped: 'time_budget_exceeded' });
      continue;
    }

    const fHandle      = `${platform}_handle`;
    const fMasterID    = `${platform}_masterID`;
    const fNickname    = `${platform}_nickname`;
    const fProfilePic  = `${platform}_profilePic`;
    const fFollowers   = `${platform}_followers`;
    const fStatus      = `${platform}_status`;
    const fLastUpdated = `${platform}_lastUpdated`;

    const targets = roster
      .map((a, idx) => ({ artist: a, idx }))
      .filter(x =>
        x.artist &&
        typeof x.artist.handle === 'string' &&
        typeof x.artist[fHandle] === 'string' &&
        x.artist[fHandle].length > 0
      );

    let successCount = 0;
    let failCount = 0;
    let retryCount = 0;
    let processedTargets = 0;
    const snapshotEntry = { ts: Date.now(), date: today, artists: {} };

    for (let i = 0; i < targets.length; i++) {
      if (Date.now() - startTime > TIME_BUDGET_MS) {
        console.log(`[${platform}] Time budget exceeded after ${processedTargets} artists`);
        break;
      }
      const { artist, idx } = targets[i];
      const socialHandle = roster[idx][fHandle];
      const now = Date.now();
      processedTargets++;

      const result = await fetchProfile(platform, config, socialHandle);
      if (result.attempt > 1) retryCount++;

      if (result.ok) {
        const wrapper = result.wrapper;
        const stats = result.stats;
        const newMasterID = wrapper.masterID || roster[idx][fMasterID] || null;

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
        // Keep prior follower count on failure (don't zero it out)
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
        console.log(`[${platform}] @${artist.handle} (${socialHandle}) failed: ${result.status}`);
      }

      // 1.1s sleep between requests (skip on last iteration of this platform)
      if (i < targets.length - 1) {
        await new Promise(resolve => setTimeout(resolve, 1100));
      }
    }

    // Read snapshots, dedup today's entry, append, cap at 1095
    const snapshotsKey = `${platform}-snapshots`;
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
    const existingIdx = snapshots.findIndex(s => s && s.date === today);
    if (existingIdx >= 0) snapshots[existingIdx] = snapshotEntry;
    else snapshots.push(snapshotEntry);
    if (snapshots.length > 1095) snapshots = snapshots.slice(-1095);

    await fetch(`${url}/set/${snapshotsKey}`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify([JSON.stringify(snapshots)])
    });

    // Write last-refreshed using TikTok cron's single-wrap pattern
    // (matches what tiktok-status.js reads successfully)
    await fetch(`${url}/set/${platform}-last-refreshed`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(Date.now())
    });

    results.push({
      platform,
      total: targets.length,
      processed: processedTargets,
      success: successCount,
      fail: failCount,
      retries: retryCount
    });
  }

  // Write updated roster back
  const cleanRoster = roster.filter(a => a && typeof a.handle === 'string' && a.handle.length > 0);
  await fetch(`${url}/set/roster`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify([JSON.stringify(cleanRoster)])
  });

  return res.status(200).json({
    ok: true,
    duration_ms: Date.now() - startTime,
    results
  });
}

export const config = {
  maxDuration: 300
};
