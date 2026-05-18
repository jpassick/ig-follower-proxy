const SMM_BASE = 'https://social-media-master.p.rapidapi.com';

const PLATFORMS = {
  youtube: {
    dailyEndpoint: '/youtube-channel-daily-stats',
    daysParam: 'dayRange',
    daysValue: '14',
    detailsEndpoint: '/youtube-channel-details',
    detailsProfileUrl: handle => `https://www.youtube.com/@${handle}`,
    detailsWrapper: 'channel',
    statsArrayKey: 'stats',
    followersField: 'subscriberCount',
    hasSummaryStats: false
  },
  twitter: {
    dailyEndpoint: '/twitter-account-stats',
    daysParam: 'days',
    daysValue: '7',
    detailsEndpoint: '/twitter-user-account',
    detailsProfileUrl: handle => `https://x.com/${handle}`,
    detailsWrapper: 'profile',
    statsArrayKey: 'dailyStats',
    followersField: 'followers',
    hasSummaryStats: true
  },
  facebook: {
    dailyEndpoint: '/facebook-account-stats',
    daysParam: 'days',
    daysValue: '7',
    detailsEndpoint: '/facebook-user-account',
    detailsProfileUrl: handle => `https://www.facebook.com/${handle}`,
    detailsWrapper: 'profile',
    statsArrayKey: 'dailyStats',
    followersField: 'followers',
    hasSummaryStats: true
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
  const smmHeaders = {
    'x-rapidapi-host': 'social-media-master.p.rapidapi.com',
    'x-rapidapi-key': smmKey,
    'Content-Type': 'application/json'
  };

  async function smmFetch(apiUrl) {
    for (let attempt = 1; attempt <= 2; attempt++) {
      try {
        const r = await fetch(apiUrl, { headers: smmHeaders });
        if (r.ok) {
          const json = await r.json();
          return { ok: true, data: json };
        }
        if (attempt === 1) {
          await new Promise(resolve => setTimeout(resolve, 2000));
          continue;
        }
        return { ok: false, status: `error_${r.status}` };
      } catch (err) {
        if (attempt === 1) {
          await new Promise(resolve => setTimeout(resolve, 2000));
          continue;
        }
        return { ok: false, status: 'fetch_error' };
      }
    }
  }

  function parseDailyStats(data, config) {
    // Twitter/FB: prefer summaryStats.followers (the "current" snapshot)
    if (config.hasSummaryStats) {
      const f = data?.summaryStats?.followers;
      if (typeof f === 'number') {
        return { followers: f, source: 'summaryStats' };
      }
    }
    // YouTube: find the most recent entry in stats[] by dateISO
    const arr = data?.[config.statsArrayKey];
    if (!Array.isArray(arr) || arr.length === 0) return null;
    const sorted = arr.slice().sort((a, b) => {
      const aTs = a?.dateISO ? new Date(a.dateISO).getTime() : 0;
      const bTs = b?.dateISO ? new Date(b.dateISO).getTime() : 0;
      return bTs - aTs;
    });
    const latest = sorted[0];
    const f = latest?.[config.followersField];
    if (typeof f !== 'number') return null;
    return { followers: f, source: 'dailyStatsArray', latestDate: latest.dateISO || latest.date };
  }

  function parseDetails(data, config) {
    const wrapper = data?.[config.detailsWrapper];
    const stats = data?.stats || wrapper?.stats;
    if (!stats) return null;
    const followers = (typeof stats.followersCount === 'number') ? stats.followersCount
                    : (typeof stats.followers === 'number') ? stats.followers
                    : null;
    if (followers === null) return null;
    return {
      followers,
      masterID: wrapper?.masterID || null,
      nickname: wrapper?.name || null,
      avatar: wrapper?.image || null
    };
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
    let dailyStatsHits = 0;
    let detailsFallbacks = 0;
    let masterIDFetches = 0;
    const snapshotEntry = { ts: Date.now(), date: today, artists: {} };

    for (let i = 0; i < targets.length; i++) {
      if (Date.now() - startTime > TIME_BUDGET_MS) {
        console.log(`[${platform}] Time budget exceeded after ${i} artists`);
        break;
      }
      const { artist, idx } = targets[i];
      const socialHandle = roster[idx][fHandle];
      let masterID = roster[idx][fMasterID] || null;
      const now = Date.now();

      let followers = null;
      let nickname = null;
      let avatar = null;
      let resultStatus = null;

      // Step 1: if we have a masterID, try the daily-stats endpoint
      if (masterID) {
        const dailyUrl = `${SMM_BASE}${config.dailyEndpoint}?id=${encodeURIComponent(masterID)}&${config.daysParam}=${config.daysValue}`;
        const dailyR = await smmFetch(dailyUrl);
        if (dailyR.ok) {
          const parsed = parseDailyStats(dailyR.data, config);
          if (parsed) {
            followers = parsed.followers;
            dailyStatsHits++;
          }
        } else {
          resultStatus = dailyR.status;
        }
      }

      // Step 2: fall back to details endpoint if daily-stats had no usable value
      if (followers === null) {
        const detailsUrl = `${SMM_BASE}${config.detailsEndpoint}?url=${encodeURIComponent(config.detailsProfileUrl(socialHandle))}`;
        const detailsR = await smmFetch(detailsUrl);
        if (detailsR.ok) {
          const parsed = parseDetails(detailsR.data, config);
          if (parsed) {
            followers = parsed.followers;
            nickname = parsed.nickname;
            avatar = parsed.avatar;
            if (!masterID && parsed.masterID) {
              masterID = parsed.masterID;
              masterIDFetches++;
            }
            detailsFallbacks++;
          } else {
            resultStatus = resultStatus || 'not_found';
          }
        } else {
          resultStatus = resultStatus || detailsR.status;
        }
      }

      if (followers !== null) {
        const updates = {
          [fFollowers]: followers,
          [fStatus]: 'ok',
          [fLastUpdated]: now
        };
        if (masterID) updates[fMasterID] = masterID;
        if (nickname) updates[fNickname] = nickname;
        if (avatar) updates[fProfilePic] = avatar;
        roster[idx] = { ...roster[idx], ...updates };

        snapshotEntry.artists[artist.handle] = {
          [fHandle]: socialHandle,
          followers,
          status: 'ok'
        };
        successCount++;
      } else {
        roster[idx] = {
          ...roster[idx],
          [fStatus]: resultStatus || 'error',
          [fLastUpdated]: now
        };
        snapshotEntry.artists[artist.handle] = {
          [fHandle]: socialHandle,
          followers: roster[idx][fFollowers] ?? null,
          status: resultStatus || 'error'
        };
        failCount++;
        console.log(`[${platform}] @${artist.handle} (${socialHandle}) failed: ${resultStatus || 'unknown'}`);
      }

      // 1.1s sleep between artists (skip on last iteration of this platform)
      if (i < targets.length - 1) {
        await new Promise(resolve => setTimeout(resolve, 1100));
      }
    }

    // Read snapshots, dedup today, append, cap at 1095
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

    await fetch(`${url}/set/${platform}-last-refreshed`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(Date.now())
    });

    results.push({
      platform,
      total: targets.length,
      success: successCount,
      fail: failCount,
      dailyStatsHits,
      detailsFallbacks,
      masterIDFetches
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
