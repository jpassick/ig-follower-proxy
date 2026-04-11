// api/cron-anr-batch.js
const RAPIDAPI_KEY = process.env.RAPIDAPI_KEY;
const KV_URL = process.env.KV_REST_API_URL;
const KV_TOKEN = process.env.KV_REST_API_TOKEN;
const ANR_SNAPSHOTS_KEY = 'anr-snapshots';
const ANR_LAST_REFRESHED_KEY = 'anr-last-refreshed';
const BATCH_SIZE = 150;

async function kvGet(key) {
  const r = await fetch(`${KV_URL}/get/${key}`, {
    headers: { Authorization: `Bearer ${KV_TOKEN}` }
  });
  const data = await r.json();
  if (!data.result) return null;
  let val = data.result;
  while (typeof val === 'string') {
    try { val = JSON.parse(val); } catch(e) { break; }
  }
  if (Array.isArray(val) && val.length === 1 && typeof val[0] === 'string') {
    try { val = JSON.parse(val[0]); } catch(e) {}
  }
  return val;
}

async function kvSet(key, value) {
  const r = await fetch(`${KV_URL}/set/${key}`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${KV_TOKEN}`, 'Content-Type': 'application/json' },
    body: JSON.stringify([JSON.stringify(value)])
  });
  if (!r.ok) {
    const text = await r.text();
    throw new Error(`kvSet failed for key ${key}: ${r.status} ${text}`);
  }
}

async function fetchFollowerCount(handle) {
  const url = `https://instagram-best-experience.p.rapidapi.com/user/info?username=${handle}`;
  const res = await fetch(url, {
    headers: {
      'x-rapidapi-key': RAPIDAPI_KEY,
      'x-rapidapi-host': 'instagram-best-experience.p.rapidapi.com',
    },
  });
  if (!res.ok) throw new Error(`RapidAPI error for ${handle}: ${res.status}`);
  const data = await res.json();
  const user = data?.data?.user;
  if (!user) throw new Error(`No user data for ${handle}`);
  return {
    followers: user.edge_followed_by?.count ?? null,
    profilePic: user.profile_pic_url_hd || user.profile_pic_url || null,
  };
}

async function loadFullRoster() {
  const meta = await kvGet('anr-roster:meta');
  if (meta && meta.chunks) {
    const chunks = await Promise.all(
      Array.from({ length: meta.chunks }, (_, i) => kvGet(`anr-roster:chunk:${i}`))
    );
    return chunks.map(c => (Array.isArray(c) ? c : [])).flat().filter(Boolean);
  }
  const roster = await kvGet('anr-roster');
  return Array.isArray(roster) ? roster : [];
}

async function saveFullRoster(roster) {
  const CHUNK_SIZE = 500;

  const existingMeta = await kvGet('anr-roster:meta');
  const existingTotal = existingMeta?.total || 0;

  if (roster.length === 0 && existingTotal > 10) {
    const msg = `BLOCKED empty write — existing roster has ${existingTotal} artists`;
    console.error(msg);
    throw new Error(msg);
  }
  if (existingTotal > 100 && roster.length < existingTotal * 0.5) {
    const msg = `BLOCKED suspicious write — trying to save ${roster.length} artists but ${existingTotal} exist (>50% drop)`;
    console.error(msg);
    throw new Error(msg);
  }

  const chunks = [];
  for (let i = 0; i < roster.length; i += CHUNK_SIZE) {
    chunks.push(roster.slice(i, i + CHUNK_SIZE));
  }

  if (existingMeta && existingMeta.chunks > chunks.length) {
    await Promise.all(
      Array.from({ length: existingMeta.chunks - chunks.length }, (_, i) =>
        fetch(`${KV_URL}/del/anr-roster:chunk:${chunks.length + i}`, {
          method: 'POST',
          headers: { Authorization: `Bearer ${KV_TOKEN}` }
        })
      )
    );
  }

  await Promise.all(chunks.map((chunk, i) => kvSet(`anr-roster:chunk:${i}`, chunk)));
  await kvSet('anr-roster:meta', { chunks: chunks.length, total: roster.length });
  console.log(`saveFullRoster: wrote ${roster.length} artists in ${chunks.length} chunks`);
}

export default async function handler(req, res) {
  const authHeader = req.headers['authorization'];
  if (authHeader !== `Bearer ${process.env.CRON_SECRET}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  try {
    const roster = await loadFullRoster();
    if (!roster.length) {
      console.log('No A&R prospects in roster');
      return res.status(200).json({ message: 'No A&R prospects in roster' });
    }

    const now = Date.now();
    const TWELVE_HOURS = 12 * 60 * 60 * 1000;

    // Identify stale artists
    const staleArtists = roster.filter(a => !a.lastUpdated || (now - a.lastUpdated > TWELVE_HOURS));
    const staleCount = staleArtists.length;

    if (staleCount === 0) {
      console.log('All artists are up to date for today.');
      return res.status(200).json({ message: 'All artists already refreshed today.' });
    }

    // Sort all artists oldest-first, take the top BATCH_SIZE
    const sortedForUpdate = [...roster].sort((a, b) => {
      const timeA = a.lastUpdated || 0;
      const timeB = b.lastUpdated || 0;
      return timeA - timeB;
    });

    const batch = sortedForUpdate.slice(0, BATCH_SIZE);
    console.log(`Processing ${batch.length} oldest artists. ${staleCount} total stale remaining.`);

    const results = [];
    const errors = [];

    for (const artist of batch) {
      if (!artist.handle || typeof artist.handle !== 'string' || !artist.handle.trim()) {
        continue;
      }
      try {
        const { followers, profilePic } = await fetchFollowerCount(artist.handle);

        // Update in place on the main roster array
        const target = roster.find(a => a.handle === artist.handle);
        if (target) {
          target.followers = followers;
          if (profilePic) target.profilePic = profilePic;
          target.lastUpdated = now;
        }

        results.push({ handle: artist.handle, followers });
        await new Promise(r => setTimeout(r, 1500));
      } catch (err) {
        errors.push({ handle: artist.handle, error: err.message });
        console.error(`Error refreshing A&R ${artist.handle}:`, err.message);
      }
    }

    await saveFullRoster(roster);
    await kvSet(ANR_LAST_REFRESHED_KEY, now);

    // If stale count at start of this run was <= batch size, this is the final run
    const isLastBatch = staleCount <= BATCH_SIZE;

    if (isLastBatch) {
      let snapshots = await kvGet(ANR_SNAPSHOTS_KEY);
      if (!Array.isArray(snapshots)) snapshots = [];
      const dayOfWeek = new Date().getUTCDay();
      const snapshot = {
        date: new Date().toISOString(),
        type: dayOfWeek === 1 ? 'weekly' : 'daily',
        data: roster
          .filter(a => a.handle && a.followers != null)
          .map(a => ({ handle: a.handle, followers: a.followers })),
      };
      snapshots.push(snapshot);
      if (snapshots.length > 120) snapshots.splice(0, snapshots.length - 120);
      await kvSet(ANR_SNAPSHOTS_KEY, snapshots);
      console.log('A&R cycle complete (LAST BATCH) — snapshot saved');
    } else {
      console.log(`A&R batch complete — ${results.length} refreshed, ${errors.length} errors.`);
    }

    return res.status(200).json({
      success: true,
      refreshed: results.length,
      errors,
      snapshotSaved: isLastBatch,
      remainingStale: Math.max(0, staleCount - batch.length),
      timestamp: new Date().toISOString(),
    });

  } catch (err) {
    console.error('A&R cron batch failed:', err);
    return res.status(500).json({ error: err.message });
  }
}
