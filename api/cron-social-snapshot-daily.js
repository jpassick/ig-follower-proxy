const SMM_BASE = 'https://social-media-master.p.rapidapi.com';
const YT_BASE = 'https://www.googleapis.com/youtube/v3';

const PLATFORMS = {
  youtube: {
    provider: 'youtube_data_api'
  },
  twitter: {
    provider: 'smm',
    dailyEndpoint: '/twitter-account-stats',
    daysParam: 'days',
    daysValue: '7',
    extraParams: '',
    detailsEndpoint: '/twitter-user-account',
    detailsProfileUrl: handle => `https://x.com/${handle}`,
    detailsWrapper: 'profile',
    statsArrayKey: 'dailyStats',
    followersField: 'followers',
    hasSummaryStats: true
  },
  facebook: {
    provider: 'smm',
    dailyEndpoint: '/facebook-account-stats',
    daysParam: 'days',
    daysValue: '7',
    extraParams: '',
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
  const ytApiKey = process.env.YOUTUBE_API_KEY;
  if (!url || !token) return res.status(500).json({ error: 'Redis not configured' });
  if (!smmKey) return res.status(500).json({ error: 'SMM_RAPIDAPI_KEY not configured' });
  // YOUTUBE_API_KEY is checked per-platform below — missing key skips YT, twitter/fb still run

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

  // YouTube Data API v3 fetcher. One call returns subscriber count + channel name + avatar.
  // Handle can be passed with or without leading "@" — strip it just in case.
  async function fetchYouTubeData(handle) {
    const cleanHandle = handle.replace(/^@+/, '');
    // Channel IDs are 24 chars starting with "UC" — look those up by id= (works for
    // channels that never claimed a modern @handle, e.g. PARTYNEXTDOOR). Everything
    // else uses forHandle=.
    const isChannelId = /^UC[\w-]{22}$/.test(cleanHandle);
    const lookupParam = isChannelId
      ? `id=${encodeURIComponent(cleanHandle)}`
      : `forHandle=${encodeURIComponent(cleanHandle)}`;
    const apiUrl = `${YT_BASE}/channels?part=statistics,snippet&${lookupParam}&key=${ytApiKey}`;
    for (let attempt = 1; attempt <= 2; attempt++) {
      try {
        const r = await fetch(apiUrl);
        if (r.ok) {
          const json = await r.json();
          const items = json?.items;
          if (!Array.isArray(items) || items.length === 0) {
            return { ok: false, status: 'not_found' };
          }
          const channel = items[0];
          const stats = channel?.statistics || {};
          const snippet = channel?.snippet || {};
          const hidden = stats.hiddenSubscriberCount === true;
          const nickname = snippet.title || null;
          const avatar = snippet?.thumbnails?.default?.url
                      || snippet?.thumbnails?.medium?.url
                      || snippet?.thumbnails?.high?.url
                      || null;
          if (hidden) {
            // Subscriber count is hidden by the channel owner. Preserve prior follower
            // value and still update nickname/avatar — those are unaffected by the hide setting.
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
        }
        // Non-OK: 403 (quota exceeded / bad key), 400 (bad request), 5xx (transient)
        // Retry once on transient errors
        if (attempt === 1 && r.status >= 500) {
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

  // Returns { followers, source } only if followers is a POSITIVE number.
  // SMM returns 0/null for accounts it has no Daily Stats data for —
  // we must reject those and force a fallback to the Details endpoint.
  function parseDailyStats(data, config) {
    if (config.hasSummaryStats) {
      const f = data?.summaryStats?.followers;
      if (typeof f === 'number' && f > 0) {
        return { followers: f, source: 'summaryStats' };
      }
    }
    const arr = data?.[config.statsArrayKey];
    if (!Array.isArray(arr) || arr.length === 0) return null;
    const sorted = arr.slice().sort((a, b) => {
      const aTs = a?.dateISO ? new Date(a.dateISO).getTime() : 0;
      const bTs = b?.dateISO ? new Date(b.dateISO).getTime() : 0;
      return bTs - aTs;
    });
    const latest = sorted[0];
    const f = latest?.[config.followersField];
    if (typeof f !== 'number' || f <= 0) return null;
    return { followers: f, source: 'dailyStatsArray', latestDate: latest.dateISO || latest.date };
  }

  function parseDetails(data, config) {
    const wrapper = data?.[config.detailsWrapper];
    const stats = data?.stats || wrapper?.stats;
    if (!stats) return null;
    const followers = (typeof stats.followersCount === 'number') ? stats.followersCount
                    : (typeof stats.followers === 'number') ? stats.followers
                    : null;
    if (followers === null || followers <= 0) return null;
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

    // YouTube requires its own API key. If missing, skip platform entirely.
    if (config.provider === 'youtube_data_api' && !ytApiKey) {
      results.push({ platform, skipped: 'no_youtube_api_key' });
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
    let dailyStatsHits = 0;       // SMM-only
    let detailsFallbacks = 0;     // SMM-only
    let masterIDFetches = 0;      // SMM-only
    let hiddenCounts = 0;          // YT-only
    let notFound = 0;              // YT mostly
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

      if (config.provider === 'youtube_data_api') {
        // -------- YouTube Data API v3 path --------
        const ytData = await fetchYouTubeData(socialHandle);
        if (ytData.ok) {
          followers = ytData.followers;
          nickname = ytData.nickname;
          avatar = ytData.avatar;
        } else {
          resultStatus = ytData.status;
          if (ytData.status === 'hidden_count') {
            hiddenCounts++;
            // Preserve nickname/avatar even when count is hidden
            if (ytData.nickname) nickname = ytData.nickname;
            if (ytData.avatar) avatar = ytData.avatar;
          } else if (ytData.status === 'not_found') {
            notFound++;
          }
        }
      } else {
        // -------- SMM path (twitter / facebook) --------
        if (masterID) {
          const dailyUrl = `${SMM_BASE}${config.dailyEndpoint}?id=${encodeURIComponent(masterID)}&${config.daysParam}=${config.daysValue}${config.extraParams || ''}`;
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
      }

      // Common update logic for all providers
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
        // No fresh follower value — keep prior value in snapshot rather than zero/null.
        // For YouTube hidden_count: still update nickname/avatar if we got them.
        const priorFollowers = roster[idx][fFollowers] ?? null;
        const updates = {
          [fStatus]: resultStatus || 'error',
          [fLastUpdated]: now
        };
        if (nickname) updates[fNickname] = nickname;
        if (avatar) updates[fProfilePic] = avatar;
        roster[idx] = { ...roster[idx], ...updates };

        snapshotEntry.artists[artist.handle] = {
          [fHandle]: socialHandle,
          followers: priorFollowers,
          status: resultStatus || 'error'
        };
        failCount++;
        console.log(`[${platform}] @${artist.handle} (${socialHandle}) failed: ${resultStatus || 'unknown'}`);
      }

      // Inter-artist sleep — needed for SMM rate-limit safety, not needed for YT Data API
      if (i < targets.length - 1) {
        if (config.provider === 'smm') {
          await new Promise(resolve => setTimeout(resolve, 1100));
        }
        // YouTube: no sleep (Google's quota is unit-based, not rate-limited)
      }
    }

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

    const result = {
      platform,
      provider: config.provider,
      total: targets.length,
      success: successCount,
      fail: failCount
    };
    if (config.provider === 'smm') {
      result.dailyStatsHits = dailyStatsHits;
      result.detailsFallbacks = detailsFallbacks;
      result.masterIDFetches = masterIDFetches;
    } else if (config.provider === 'youtube_data_api') {
      result.hiddenCounts = hiddenCounts;
      result.notFound = notFound;
    }
    results.push(result);
  }

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
