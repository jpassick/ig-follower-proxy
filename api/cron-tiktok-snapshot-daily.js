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
  const TIME_BUDGET_MS = 270 * 1000;

  const smmHeaders = {
    'x-rapidapi-host': 'social-media-master.p.rapidapi.com',
    'x-rapidapi-key': smmKey,
    'Content-Type': 'application/json'
  };

  // SMM fetch helper — retries once on non-200 / network error after 2s wait.
  async function smmFetch(apiUrl) {
    for (let attempt = 1; attempt <= 2; attempt++) {
      try {
        const r = await fetch(apiUrl, { headers: smmHeaders });
        if (r.ok) {
          const json = await r.json();
          return { ok: true, data: json, attempt };
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

  // SMM dailyStats dates come back as "DD-MM-YYYY" — convert to our snapshot "YYYY-MM-DD" key.
  function smmDateToISO(ddmmyyyy) {
    if (typeof ddmmyyyy !== 'string') return null;
    const parts = ddmmyyyy.split('-');
    if (parts.length !== 3) return null;
    const [dd, mm, yyyy] = parts;
    if (!/^\d{4}$/.test(yyyy) || !/^\d{2}$/.test(mm) || !/^\d{2}$/.test(dd)) return null;
    return `${yyyy}-${mm}-${dd}`;
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
    .filter(x =>
      x.artist &&
      typeof x.artist.handle === 'string' &&
      typeof x.artist.tiktok_handle === 'string' &&
      x.artist.tiktok_handle.length > 0
    );

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

  const todayDate = new Date().toISOString().slice(0, 10); // YYYY-MM-DD
  const todaySnapshotEntry = {
    ts: Date.now(),
    date: todayDate,
    artists: {}
  };

  // Aggregate dailyStats from every artist for end-of-run backfill into historical snapshots.
  // Shape: { "YYYY-MM-DD": { igHandle: { tiktok_handle, followers, status }, ... }, ... }
  const dailyStatsBackfill = {};

  let successCount = 0;
  let failCount = 0;
  let dailyStatsHits = 0;       // artists where account-stats gave us the primary follower number
  let detailsFallbacks = 0;     // artists where we had to fall back to profile-details for follower count
  let masterIDFetches = 0;      // artists where we discovered a new masterID this run
  let retryCount = 0;
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
    let masterID = artist.tiktok_masterID || null;
    const now = Date.now();
    processedTargets++;

    // -------- Step 1: profile lookup --------
    // /universal-profile-id (by masterID) or /tiktok-user-account (by URL) both return profile + stats.
    // This call gives us name + image, discovers masterID on first-time artists, and provides
    // a fallback follower count if Daily Stats fails or hits the zero-trap for niche accounts.
    const profileUrl = masterID
      ? `https://social-media-master.p.rapidapi.com/universal-profile-id?id=${encodeURIComponent(masterID)}`
      : `https://social-media-master.p.rapidapi.com/tiktok-user-account?url=${encodeURIComponent(`https://www.tiktok.com/@${tiktokHandle}`)}`;

    const profileResp = await smmFetch(profileUrl);
    if (profileResp.attempt > 1) retryCount++;

    let profile = null;
    let fallbackFollowers = null;
    if (profileResp.ok && profileResp.data?.profile && profileResp.data?.stats) {
      profile = profileResp.data.profile;
      if (!masterID && profile.masterID) {
        masterID = profile.masterID;
        masterIDFetches++;
      }
      const f = profileResp.data.stats.followersCount;
      if (typeof f === 'number' && f > 0) fallbackFollowers = f;
    }

    // -------- Step 2: Daily Stats lookup (primary follower source + history) --------
    // /tiktok-account-stats returns summaryStats.followers (today's value, fresher than Details)
    // plus a dailyStats[] array of the past N days. We use the latter to backfill history.
    let primaryFollowers = null;
    let dailyStats = [];

    if (masterID) {
      const statsUrl = `https://social-media-master.p.rapidapi.com/tiktok-account-stats?id=${encodeURIComponent(masterID)}&days=7`;
      const statsResp = await smmFetch(statsUrl);
      if (statsResp.attempt > 1) retryCount++;
      if (statsResp.ok && statsResp.data) {
        const sumF = statsResp.data?.summaryStats?.followers;
        if (typeof sumF === 'number' && sumF > 0) {
          primaryFollowers = sumF;
          dailyStatsHits++;
        }
        if (Array.isArray(statsResp.data?.dailyStats)) {
          dailyStats = statsResp.data.dailyStats;
        }
      }
    }

    // Pick best follower count: primary (Daily Stats) > fallback (profile-details)
    const followers = primaryFollowers ?? fallbackFollowers;
    if (followers !== null && primaryFollowers === null) detailsFallbacks++;

    if (followers !== null) {
      // Update roster entry
      const updates = {
        tiktok_followers: followers,
        tiktok_status: 'ok',
        tiktok_lastUpdated: now
      };
      if (masterID) updates.tiktok_masterID = masterID;
      if (profile?.name) updates.tiktok_nickname = profile.name;
      if (profile?.image) updates.tiktok_profilePic = profile.image;
      roster[idx] = { ...roster[idx], ...updates };

      // Today's snapshot entry
      todaySnapshotEntry.artists[artist.handle] = {
        tiktok_handle: tiktokHandle,
        followers,
        status: 'ok'
      };

      // Aggregate dailyStats[] for historical backfill — skip today (we already set it above
      // with the freshest value from summaryStats), only positive counts.
      for (const day of dailyStats) {
        const isoDate = smmDateToISO(day?.date);
        if (!isoDate || isoDate === todayDate) continue;
        const dayF = day.followers;
        if (typeof dayF !== 'number' || dayF <= 0) continue;
        if (!dailyStatsBackfill[isoDate]) dailyStatsBackfill[isoDate] = {};
        dailyStatsBackfill[isoDate][artist.handle] = {
          tiktok_handle: tiktokHandle,
          followers: dayF,
          status: 'ok'
        };
      }

      successCount++;
    } else {
      // Failure: keep prior follower count (don't zero it out)
      const priorFollowers = roster[idx].tiktok_followers ?? null;
      const statusCode = profileResp.ok ? 'no_followers' : (profileResp.status || 'error');
      roster[idx] = {
        ...roster[idx],
        tiktok_status: statusCode,
        tiktok_lastUpdated: now
      };
      todaySnapshotEntry.artists[artist.handle] = {
        tiktok_handle: tiktokHandle,
        followers: priorFollowers,
        status: statusCode
      };
      failCount++;
      console.log(`[tiktok] @${artist.handle} (${tiktokHandle}) failed: ${statusCode}`);
    }

    // Sleep ~1.1s between artists (not after the last one)
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
      stats: { successCount, failCount, dailyStatsHits, detailsFallbacks, masterIDFetches, retryCount, skippedTimeout }
    });
  }

  // Read existing snapshots
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

  // Idempotent dedup: replace today's snapshot if it exists, else append.
  const existingTodayIdx = snapshots.findIndex(s => s && s.date === todayDate);
  if (existingTodayIdx >= 0) {
    snapshots[existingTodayIdx] = todaySnapshotEntry;
  } else {
    snapshots.push(todaySnapshotEntry);
  }

  // Backfill historical snapshots from aggregated dailyStats.
  // For each date that came back in dailyStats from any artist:
  //   - If a snapshot exists for that date, MERGE the new artist entries into existing.artists
  //     (replaces our entries for those specific artists with SMM's authoritative history,
  //      preserves entries for artists we didn't touch this run)
  //   - If not, create a new snapshot entry for that date
  let backfillUpdates = 0;
  let backfillNewSnapshots = 0;
  for (const [isoDate, artistsForDate] of Object.entries(dailyStatsBackfill)) {
    const existingIdx = snapshots.findIndex(s => s && s.date === isoDate);
    if (existingIdx >= 0) {
      snapshots[existingIdx].artists = {
        ...snapshots[existingIdx].artists,
        ...artistsForDate
      };
      backfillUpdates++;
    } else {
      // Synthetic ts at the historical date's normal cron hour (14:40 UTC)
      const ts = new Date(`${isoDate}T14:40:00Z`).getTime();
      snapshots.push({ ts, date: isoDate, artists: artistsForDate });
      backfillNewSnapshots++;
    }
  }

  // Sort snapshots chronologically by date (YYYY-MM-DD strings sort correctly with localeCompare)
  snapshots.sort((a, b) => {
    const aDate = a?.date || '';
    const bDate = b?.date || '';
    return aDate.localeCompare(bDate);
  });

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
      stats: { successCount, failCount, dailyStatsHits, detailsFallbacks, masterIDFetches, retryCount, backfillUpdates, backfillNewSnapshots, skippedTimeout }
    });
  }

  // Update last-refreshed timestamp (single-wrap — matches existing TikTok pattern)
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
    dailyStatsHits,
    detailsFallbacks,
    masterIDFetches,
    retryCount,
    backfillUpdates,
    backfillNewSnapshots,
    skippedTimeout,
    snapshot_count: snapshots.length,
    duration_ms: Date.now() - startTime
  });
}

export const config = {
  maxDuration: 300
};
