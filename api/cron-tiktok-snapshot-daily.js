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
  const smmKey = process.env.SMM_RAPIDAPI_KEY;
  if (!url || !token) return res.status(500).json({ error: 'Redis not configured' });
  if (!smmKey) return res.status(500).json({ error: 'SMM_RAPIDAPI_KEY not configured' });

  const startTime = Date.now();
  const TIME_BUDGET_MS = 270 * 1000; // Bail out before Vercel kills us at 300s

  // SMM fetch helper — retries once on non-200 / network error after 2s wait
  async function fetchProfile({ masterID, handle }) {
    const apiUrl = masterID
      ? `https://social-media-master.p.rapidapi.com/universal-profile-id?id=${encodeURIComponent(masterID)}`
      : `https://social-media-master.p.rapidapi.com/tiktok-user-account?url=${encodeURIComponent(`https://www.tiktok.com/@${handle}`)}`;

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
          if (json?.profile && json?.stats) {
            return { ok: true, data: json, attempt };
          }
          // 200 but missing expected fields — treat as not_found, no retry
          return { ok: false, status: 'not_found', attempt };
        }
        // Non-OK: retry once
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

  // Filter to artists with a tiktok_handle set
  const targets = roster
    .map((a, idx) => ({ artist: a, idx }))
    .filter(x => x.artist && typeof x.artist.handle === 'string' && typeof x.artist.tiktok_handle === 'string' && x.artist.tiktok_handle.length > 0);

  if (targets.length === 0) {
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

  const snapshotEntry = {
    ts: Date.now(),
    date: new Date().toISOString().slice(0, 10), // YYYY-MM-DD
    artists: {}
  };

  let successCount = 0;
  let failCount = 0;
  let retryCount = 0;
  let noMasterIDFallbacks = 0;
  let skippedTimeout = 0;
  let processedTargets = 0;

  for (let i = 0; i < targets.length; i++) {
    // Time budget bailout
    if (Date.now() - startTime > TIME_BUDGET_MS) {
      skippedTimeout = targets.length - i;
      break;
    }

    const { artist, idx } = targets[i];
    const tiktokHandle = artist.tiktok_handle;
    const masterID = artist.tiktok_masterID || null;
    const now = Date.now();
    processedTargets++;

    if (!masterID) noMasterIDFallbacks++;

    const result = await fetchProfile({ masterID, handle: tiktokHandle });

    if (result.attempt > 1) retryCount++;

    if (result.ok) {
      const profile = result.data.profile || {};
      const stats = result.data.stats || {};
      const newMasterID = profile.masterID || masterID || null;

      // Update roster entry
      roster[idx] = {
        ...roster[idx],
        tiktok_followers: stats.followersCount ?? 0,
        tiktok_nickname: profile.name ?? tiktokHandle,
        tiktok_profilePic: profile.image ?? null,
        tiktok_status: 'ok',
        tiktok_lastUpdated: now,
        tiktok_masterID: newMasterID
      };

      // Add to snapshot (keyed by IG handle since that's the artist key)
      snapshotEntry.artists[artist.handle] = {
        tiktok_handle: tiktokHandle,
        followers: stats.followersCount ?? 0,
        status: 'ok'
      };

      successCount++;
    } else {
      // Failure: keep prior follower count (don't zero it out)
      roster[idx] = {
        ...roster[idx],
        tiktok_status: result.status,
        tiktok_lastUpdated: now
      };
      snapshotEntry.artists[artist.handle] = {
        tiktok_handle: tiktokHandle,
        followers: roster[idx].tiktok_followers ?? null,
        status: result.status
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
      stats: { successCount, failCount, retryCount, noMasterIDFallbacks, skippedTimeout }
    });
  }

  // Read existing snapshots, replace today's entry if exists or append, cap at 1095
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

  // Idempotent dedup: replace today's snapshot if one exists, else append
  const today = snapshotEntry.date;
  const existingIndex = snapshots.findIndex(s => s && s.date === today);
  if (existingIndex >= 0) {
    snapshots[existingIndex] = snapshotEntry;
  } else {
    snapshots.push(snapshotEntry);
  }

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
      stats: { successCount, failCount, retryCount, noMasterIDFallbacks, skippedTimeout }
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
