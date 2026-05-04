export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');

  // Verify cron secret (required for Vercel scheduled crons)
  const cronSecret = process.env.CRON_SECRET;
  const authHeader = req.headers.authorization || '';
  const expected = `Bearer ${cronSecret}`;
  if (cronSecret && authHeader !== expected) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const url = process.env.KV_REST_API_URL;
  const token = process.env.KV_REST_API_TOKEN;
  const apiKey = process.env.TIKTOK_RAPIDAPI_KEY;
  if (!url || !token) return res.status(500).json({ error: 'Redis not configured' });
  if (!apiKey) return res.status(500).json({ error: 'TIKTOK_RAPIDAPI_KEY not configured' });

  const startTime = Date.now();

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

  // Filter to artists with a tiktok_handle set
  const targets = roster
    .map((a, idx) => ({ artist: a, idx }))
    .filter(x => x.artist && typeof x.artist.handle === 'string' && typeof x.artist.tiktok_handle === 'string' && x.artist.tiktok_handle.length > 0);

  if (targets.length === 0) {
    // Nothing to do today - still update last-refreshed so frontend shows it ran
    await fetch(`${url}/set/tiktok-last-refreshed`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(Date.now())
    });
    return res.status(200).json({
      ok: true,
      message: 'No artists have tiktok_handle set yet - nothing to snapshot',
      duration_ms: Date.now() - startTime
    });
  }

  // Iterate through targets, fetch each, sleep ~1.1s between requests
  const snapshotEntry = {
    ts: Date.now(),
    date: new Date().toISOString().slice(0, 10), // YYYY-MM-DD
    artists: {}
  };

  let successCount = 0;
  let failCount = 0;

  for (let i = 0; i < targets.length; i++) {
    const { artist, idx } = targets[i];
    const tiktokHandle = artist.tiktok_handle;
    const now = Date.now();

    try {
      const tiktokR = await fetch(
        `https://tiktok-best-experience.p.rapidapi.com/user/${encodeURIComponent(tiktokHandle)}`,
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

          // Update roster entry
          roster[idx] = {
            ...roster[idx],
            tiktok_followers: u.follower_count ?? 0,
            tiktok_nickname: u.nickname ?? tiktokHandle,
            tiktok_profilePic: avatarUrl,
            tiktok_status: 'ok',
            tiktok_lastUpdated: now
          };

          // Add to snapshot (keyed by IG handle since that's the artist key)
          snapshotEntry.artists[artist.handle] = {
            tiktok_handle: tiktokHandle,
            followers: u.follower_count ?? 0,
            status: 'ok'
          };

          successCount++;
        } else {
          // API returned success status but no user data
          roster[idx] = {
            ...roster[idx],
            tiktok_status: 'not_found',
            tiktok_lastUpdated: now
          };
          snapshotEntry.artists[artist.handle] = {
            tiktok_handle: tiktokHandle,
            followers: roster[idx].tiktok_followers ?? null,
            status: 'not_found'
          };
          failCount++;
        }
      } else {
        // HTTP error from RapidAPI
        roster[idx] = {
          ...roster[idx],
          tiktok_status: `error_${tiktokR.status}`,
          tiktok_lastUpdated: now
        };
        snapshotEntry.artists[artist.handle] = {
          tiktok_handle: tiktokHandle,
          followers: roster[idx].tiktok_followers ?? null,
          status: `error_${tiktokR.status}`
        };
        failCount++;
      }
    } catch (err) {
      // Network/fetch error
      roster[idx] = {
        ...roster[idx],
        tiktok_status: 'fetch_error',
        tiktok_lastUpdated: now
      };
      snapshotEntry.artists[artist.handle] = {
        tiktok_handle: tiktokHandle,
        followers: roster[idx].tiktok_followers ?? null,
        status: 'fetch_error'
      };
      failCount++;
    }

    // Sleep ~1.1s between requests (skip on last iteration)
    if (i < targets.length - 1) {
      await new Promise(resolve => setTimeout(resolve, 1100));
    }
  }

  // Write updated roster back
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
      stats: { successCount, failCount }
    });
  }

  // Read existing snapshots, append new one, cap at 1095 entries
  const snapR = await fetch(`${url}/get/tiktok-snapshots`, {
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

  snapshots.push(snapshotEntry);

  // Cap retention at 1095 entries (~3 years of daily snapshots)
  if (snapshots.length > 1095) {
    snapshots = snapshots.slice(-1095);
  }

  const writeSnap = await fetch(`${url}/set/tiktok-snapshots`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify([JSON.stringify(snapshots)])
  });

  if (!writeSnap.ok) {
    return res.status(500).json({
      error: 'Failed to write snapshots',
      status: writeSnap.status,
      stats: { successCount, failCount }
    });
  }

  // Update last-refreshed timestamp
  await fetch(`${url}/set/tiktok-last-refreshed`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(Date.now())
  });

  return res.status(200).json({
    ok: true,
    targets_attempted: targets.length,
    successCount,
    failCount,
    snapshot_count: snapshots.length,
    duration_ms: Date.now() - startTime
  });
}

export const config = {
  maxDuration: 300
};
