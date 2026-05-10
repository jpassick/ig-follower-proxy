export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');

  // Auth via CRON_SECRET (Vercel sends this automatically; manual curl needs the header)
  const cronSecret = process.env.CRON_SECRET;
  const authHeader = req.headers.authorization || '';
  if (!cronSecret || authHeader !== `Bearer ${cronSecret}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const url = process.env.KV_REST_API_URL;
  const token = process.env.KV_REST_API_TOKEN;
  const apiKey = process.env.RAPIDAPI_KEY;
  if (!url || !token) return res.status(500).json({ error: 'Redis not configured' });
  if (!apiKey) return res.status(500).json({ error: 'RAPIDAPI_KEY not configured' });

  const startTime = Date.now();

  // Helper: fetch a single IG profile with one retry on transient failure
  async function fetchIgProfile(handle) {
    const fetchOnce = async () => {
      const r = await fetch(
        `https://instagram-best-experience.p.rapidapi.com/profile?username=${encodeURIComponent(handle)}`,
        { headers: { 'x-rapidapi-host': 'instagram-best-experience.p.rapidapi.com', 'x-rapidapi-key': apiKey } }
      );
      if (!r.ok) return { ok: false, errorStatus: r.status };
      const data = await r.json();
      if (!data || Object.keys(data).length === 0) {
        return { ok: false, rateLimited: true };
      }
      if (data.follower_count == null) {
        return { ok: false, notFound: true };
      }
      return {
        ok: true,
        follower_count: data.follower_count,
        profile_pic_url: data.profile_pic_url || data.profile_pic_url_hd || ''
      };
    };

    try {
      const first = await fetchOnce();
      if (first.ok) return first;
      if (first.notFound) return first;
      await new Promise(r => setTimeout(r, 2000));
      try { return await fetchOnce(); }
      catch (err2) { return { ok: false, errorStatus: 'fetch_error', message: err2.message }; }
    } catch (err) {
      await new Promise(r => setTimeout(r, 2000));
      try { return await fetchOnce(); }
      catch (err2) { return { ok: false, errorStatus: 'fetch_error', message: err2.message }; }
    }
  }

  // Read roster
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
  if (roster.length === 0) {
    return res.status(404).json({ error: 'Roster is empty or could not be read' });
  }

  let successCount = 0, failCount = 0;

  for (let i = 0; i < roster.length; i++) {
    if (Date.now() - startTime > 270000) {
      console.log('Time budget exceeded, stopping at artist', i);
      break;
    }
    const artist = roster[i];
    if (!artist || typeof artist.handle !== 'string' || artist.handle.length === 0) continue;

    const result = await fetchIgProfile(artist.handle);
    const now = Date.now();

    if (result.ok) {
      roster[i] = {
        ...roster[i],
        followers: result.follower_count,
        profilePic: result.profile_pic_url || roster[i].profilePic,
        status: 'ok',
        updatedAt: now
      };
      successCount++;
    } else {
      roster[i] = {
        ...roster[i],
        status: 'error',
        updatedAt: now
      };
      failCount++;
    }

    if (i < roster.length - 1) {
      await new Promise(r => setTimeout(r, 1100));
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

  // Build today's snapshot — successful artists only, matching the existing IG snapshot shape
  const snapshotEntry = {
    date: new Date().toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' }),
    ts: Date.now(),
    data: cleanRoster
      .filter(a => typeof a.followers === 'number')
      .map(a => ({ handle: a.handle, followers: a.followers }))
  };

  // Read existing snapshots
  const snapR = await fetch(`${url}/get/snapshots`, { headers: { Authorization: `Bearer ${token}` } });
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

  // Dedup by date — replace today's snapshot if one already exists
  const todayDate = snapshotEntry.date;
  const existingIdx = snapshots.findIndex(s => s.date === todayDate);
  if (existingIdx !== -1) {
    snapshots[existingIdx] = snapshotEntry;
  } else {
    snapshots.push(snapshotEntry);
  }

  // Cap at 1095 entries (~3 years of daily snapshots)
  if (snapshots.length > 1095) snapshots = snapshots.slice(-1095);

  const writeSnap = await fetch(`${url}/set/snapshots`, {
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

  return res.status(200).json({
    ok: true,
    targets_attempted: roster.length,
    successCount,
    failCount,
    snapshot_count: snapshots.length,
    duration_ms: Date.now() - startTime
  });
}

export const config = {
  maxDuration: 300
};
