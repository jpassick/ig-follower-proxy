// /api/test-smm-resolution.js
// One-shot diagnostic endpoint. Calls every plausible SMM endpoint for a single
// artist on one platform IN PARALLEL and returns raw responses + extracted
// follower counts so we can see which (if any) returns unrounded values.
//
// Usage (browser or curl):
//   /api/test-smm-resolution?password=<ADMIN_PASSWORD>&platform=tiktok&handle=fattmack_
//   /api/test-smm-resolution?password=<ADMIN_PASSWORD>&platform=twitter&handle=1raqbaby
//   /api/test-smm-resolution?password=<ADMIN_PASSWORD>&platform=youtube&handle=1raqbaby
//   /api/test-smm-resolution?password=<ADMIN_PASSWORD>&platform=facebook&handle=rodwave
//
// `handle` is the IG handle (how artists are keyed in roster). The endpoint
// resolves the per-platform social handle + masterID from the roster.
//
// Endpoints flagged "(guess)" may 404 — that's expected and useful information.

const SMM_BASE = 'https://social-media-master.p.rapidapi.com';

const TESTS = {
  tiktok: {
    socialUrl: h => `https://www.tiktok.com/@${h}`,
    endpoints: [
      { name: 'tiktok-user-account (Details, current live URL path)',
        needsMasterID: false,
        build: (h, m) => `/tiktok-user-account?url=${encodeURIComponent(`https://www.tiktok.com/@${h}`)}` },
      { name: 'universal-profile-id (current live fast path)',
        needsMasterID: true,
        build: (h, m) => `/universal-profile-id?id=${encodeURIComponent(m)}` },
      { name: 'tiktok-account-stats (guess: Daily Stats by ID, matching twitter/facebook pattern)',
        needsMasterID: true,
        build: (h, m) => `/tiktok-account-stats?id=${encodeURIComponent(m)}&days=7` },
      { name: 'tiktok-user-stats (guess: real-time stats by URL)',
        needsMasterID: false,
        build: (h, m) => `/tiktok-user-stats?url=${encodeURIComponent(`https://www.tiktok.com/@${h}`)}` }
    ]
  },
  twitter: {
    socialUrl: h => `https://x.com/${h}`,
    endpoints: [
      { name: 'twitter-user-account (Details, current live URL path)',
        needsMasterID: false,
        build: (h, m) => `/twitter-user-account?url=${encodeURIComponent(`https://x.com/${h}`)}` },
      { name: 'universal-profile-id (current live fast path)',
        needsMasterID: true,
        build: (h, m) => `/universal-profile-id?id=${encodeURIComponent(m)}` },
      { name: 'twitter-account-stats (Daily Stats, current cron path)',
        needsMasterID: true,
        build: (h, m) => `/twitter-account-stats?id=${encodeURIComponent(m)}&days=7` },
      { name: 'twitter-user-stats (guess: real-time stats by URL)',
        needsMasterID: false,
        build: (h, m) => `/twitter-user-stats?url=${encodeURIComponent(`https://x.com/${h}`)}` }
    ]
  },
  youtube: {
    socialUrl: h => `https://www.youtube.com/@${h}`,
    endpoints: [
      { name: 'youtube-channel-details (Details, current live URL path)',
        needsMasterID: false,
        build: (h, m) => `/youtube-channel-details?url=${encodeURIComponent(`https://www.youtube.com/@${h}`)}` },
      { name: 'universal-profile-id (current live fast path)',
        needsMasterID: true,
        build: (h, m) => `/universal-profile-id?id=${encodeURIComponent(m)}` },
      { name: 'youtube-channel-daily-stats (Daily Stats, current cron path)',
        needsMasterID: true,
        build: (h, m) => `/youtube-channel-daily-stats?id=${encodeURIComponent(m)}&dayRange=7&sortDesc=false&includeProfile=false` }
    ]
  },
  facebook: {
    socialUrl: h => `https://www.facebook.com/${h}`,
    endpoints: [
      { name: 'facebook-user-account (Details, current live URL path)',
        needsMasterID: false,
        build: (h, m) => `/facebook-user-account?url=${encodeURIComponent(`https://www.facebook.com/${h}`)}` },
      { name: 'universal-profile-id (current live fast path)',
        needsMasterID: true,
        build: (h, m) => `/universal-profile-id?id=${encodeURIComponent(m)}` },
      { name: 'facebook-account-stats (Daily Stats, current cron path)',
        needsMasterID: true,
        build: (h, m) => `/facebook-account-stats?id=${encodeURIComponent(m)}&days=7` }
    ]
  }
};

// Try every place we've seen a follower count live in SMM responses so we can
// extract a single number per endpoint without parsing the raw JSON by hand.
function extractFollowers(data) {
  if (!data || typeof data !== 'object') return null;
  const candidates = [];
  const push = (where, val) => {
    if (typeof val === 'number' && val > 0) candidates.push({ where, val });
  };
  push('stats.followersCount',           data?.stats?.followersCount);
  push('stats.followers',                data?.stats?.followers);
  push('stats.subscriberCount',          data?.stats?.subscriberCount);
  push('summaryStats.followers',         data?.summaryStats?.followers);
  push('summaryStats.followersCount',    data?.summaryStats?.followersCount);
  push('summaryStats.subscriberCount',   data?.summaryStats?.subscriberCount);
  push('profile.stats.followersCount',   data?.profile?.stats?.followersCount);
  push('profile.followersCount',         data?.profile?.followersCount);
  push('channel.stats.subscriberCount',  data?.channel?.stats?.subscriberCount);
  push('channel.subscriberCount',        data?.channel?.subscriberCount);

  // dailyStats / stats arrays — take latest by date
  const arrayKeys = ['dailyStats', 'stats'];
  for (const k of arrayKeys) {
    const arr = data?.[k];
    if (Array.isArray(arr) && arr.length > 0) {
      const sorted = arr.slice().sort((a, b) => {
        const aTs = a?.dateISO ? new Date(a.dateISO).getTime() : (a?.date ? new Date(a.date).getTime() : 0);
        const bTs = b?.dateISO ? new Date(b.dateISO).getTime() : (b?.date ? new Date(b.date).getTime() : 0);
        return bTs - aTs;
      });
      const latest = sorted[0];
      push(`${k}[latest].followers`,       latest?.followers);
      push(`${k}[latest].followersCount`,  latest?.followersCount);
      push(`${k}[latest].subscriberCount`, latest?.subscriberCount);
    }
  }
  return candidates;
}

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');

  const url = process.env.KV_REST_API_URL;
  const token = process.env.KV_REST_API_TOKEN;
  const smmKey = process.env.SMM_RAPIDAPI_KEY;
  const adminPassword = process.env.ADMIN_PASSWORD;

  if (!url || !token)     return res.status(500).json({ error: 'Redis not configured' });
  if (!smmKey)            return res.status(500).json({ error: 'SMM_RAPIDAPI_KEY not configured' });
  if (!adminPassword)     return res.status(500).json({ error: 'ADMIN_PASSWORD not configured' });

  // Accept password from query (GET) or body (POST)
  const q = req.query || {};
  let body = req.body;
  if (typeof body === 'string') { try { body = JSON.parse(body); } catch(e) { body = {}; } }
  const password = q.password || body?.password;
  if (password !== adminPassword) return res.status(401).json({ error: 'Unauthorized' });

  const platform = (q.platform || body?.platform || '').toString().toLowerCase();
  const igHandle = (q.handle   || body?.handle   || '').toString().toLowerCase();

  if (!platform || !TESTS[platform]) {
    return res.status(400).json({ error: `Invalid or missing platform. Must be one of: ${Object.keys(TESTS).join(', ')}` });
  }
  if (!igHandle) {
    return res.status(400).json({ error: 'Missing handle param (the artist IG handle, e.g. fattmack_)' });
  }

  // Read roster to find masterID + social handle for this artist
  const rosterR = await fetch(`${url}/get/roster`, { headers: { Authorization: `Bearer ${token}` } });
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

  const artist = roster.find(a => a && a.handle === igHandle);
  if (!artist) return res.status(404).json({ error: `Artist not found in roster: ${igHandle}` });

  const fHandle      = `${platform}_handle`;
  const fMasterID    = `${platform}_masterID`;
  const fFollowers   = `${platform}_followers`;
  const socialHandle = artist[fHandle];
  const masterID     = artist[fMasterID] || null;
  const currentStoredFollowers = artist[fFollowers] ?? null;

  if (!socialHandle) {
    return res.status(400).json({ error: `Artist @${igHandle} has no ${fHandle} set` });
  }

  const headers = {
    'x-rapidapi-host': 'social-media-master.p.rapidapi.com',
    'x-rapidapi-key': smmKey
  };

  const tests = TESTS[platform].endpoints;
  const startedAt = Date.now();

  const results = await Promise.all(tests.map(async (t) => {
    if (t.needsMasterID && !masterID) {
      return { endpoint: t.name, skipped: 'no masterID stored for this artist' };
    }
    const path = t.build(socialHandle, masterID);
    const apiUrl = `${SMM_BASE}${path}`;
    const t0 = Date.now();
    try {
      const r = await fetch(apiUrl, { headers });
      const durationMs = Date.now() - t0;
      const status = r.status;
      let raw = null;
      try { raw = await r.json(); } catch(e) { raw = '(non-JSON response)'; }
      const followerCandidates = (status >= 200 && status < 300) ? extractFollowers(raw) : [];
      return {
        endpoint: t.name,
        url: apiUrl,
        status,
        durationMs,
        follower_candidates: followerCandidates,
        raw_response: raw
      };
    } catch (err) {
      return {
        endpoint: t.name,
        url: apiUrl,
        durationMs: Date.now() - t0,
        error: err.message
      };
    }
  }));

  // Compact summary at the top for at-a-glance comparison
  const summary = results.map(r => {
    if (r.skipped) return { endpoint: r.endpoint, status: 'skipped', reason: r.skipped };
    if (r.error)   return { endpoint: r.endpoint, status: 'error',   error: r.error };
    if (r.status !== 200) return { endpoint: r.endpoint, status: r.status };
    const best = (r.follower_candidates && r.follower_candidates[0]) || null;
    return {
      endpoint: r.endpoint,
      status: 200,
      durationMs: r.durationMs,
      best_extracted_followers: best ? best.val : null,
      best_extracted_from: best ? best.where : null,
      all_extracted: r.follower_candidates
    };
  });

  return res.status(200).json({
    platform,
    artist_ig_handle: igHandle,
    social_handle: socialHandle,
    masterID: masterID || '(none stored)',
    current_stored_followers: currentStoredFollowers,
    total_duration_ms: Date.now() - startedAt,
    summary,
    full_results: results
  });
}

export const config = {
  maxDuration: 30
};
