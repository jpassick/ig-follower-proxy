// api/cron-anr-batch.js
// Called by each batch cron with a batchIndex parameter
import { Redis } from '@upstash/redis';
const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
});
const RAPIDAPI_KEY = process.env.RAPIDAPI_KEY;
const ANR_KEY = 'anr-roster';
const ANR_SNAPSHOTS_KEY = 'anr-snapshots';
const ANR_LAST_REFRESHED_KEY = 'anr-last-refreshed';
const BATCH_SIZE = 600;

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
  const meta = await redis.get(`${ANR_KEY}:meta`);
  if (meta && meta.chunks) {
    const chunks = await Promise.all(
      Array.from({ length: meta.chunks }, (_, i) => redis.get(`${ANR_KEY}:chunk:${i}`))
    );
    return chunks.map(c => (Array.isArray(c) ? c : [])).flat().filter(Boolean);
  }
  return await redis.get(ANR_KEY) || [];
}

async function saveFullRoster(roster) {
  const CHUNK_SIZE = 500;
  const chunks = [];
  for (let i = 0; i < roster.length; i += CHUNK_SIZE) {
    chunks.push(roster.slice(i, i + CHUNK_SIZE));
  }
  const oldMeta = await redis.get(`${ANR_KEY}:meta`);
  if (oldMeta && oldMeta.chunks > chunks.length) {
    const deleteKeys = Array.from(
      { length: oldMeta.chunks - chunks.length },
      (_, i) => `${ANR_KEY}:chunk:${chunks.length + i}`
    );
    await Promise.all(deleteKeys.map(k => redis.del(k)));
  }
  await Promise.all(chunks.map((chunk, i) => redis.set(`${ANR_KEY}:chunk:${i}`, chunk)));
  await redis.set(`${ANR_KEY}:meta`, { chunks: chunks.length, total: roster.length });
}

export default async function handler(req, res) {
  const authHeader = req.headers['authorization'];
  if (authHeader !== `Bearer ${process.env.CRON_SECRET}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const batchIndex = parseInt(req.query.batch || '0');

  try {
    const roster = await loadFullRoster();
    if (!roster.length) {
      return res.status(200).json({ message: 'No A&R prospects in roster' });
    }

    const totalBatches = Math.ceil(roster.length / BATCH_SIZE);
    const start = batchIndex * BATCH_SIZE;
    const end = Math.min(start + BATCH_SIZE, roster.length);
    const batch = roster.slice(start, end);

    const now = Date.now();
    const results = [];
    const errors = [];

    for (const artist of batch) {
      if (!artist.handle || typeof artist.handle !== 'string' || !artist.handle.trim()) {
        console.warn('Skipping invalid handle:', JSON.stringify(artist));
        continue;
      }
      try {
        const { followers, profilePic } = await fetchFollowerCount(artist.handle);
        artist.followers = followers;
        if (profilePic) artist.profilePic = profilePic;
        artist.lastUpdated = now;
        results.push({ handle: artist.handle, followers });
        await new Promise(r => setTimeout(r, 1100));
      } catch (err) {
        errors.push({ handle: artist.handle, error: err.message });
        console.error(`Error refreshing A&R ${artist.handle}:`, err.message);
      }
    }

    // Write updated batch back into full roster
    for (let i = start; i < end; i++) {
      roster[i] = batch[i - start];
    }

    await saveFullRoster(roster);

    // Update last refreshed timestamp for A&R section
    await redis.set(ANR_LAST_REFRESHED_KEY, now);

    // If this is the last batch, save a snapshot
    const isLastBatch = batchIndex >= totalBatches - 1;
    if (isLastBatch) {
      const snapshots = await redis.get(ANR_SNAPSHOTS_KEY) || [];
      const snapshot = {
        date: new Date().toISOString(),
        type: 'daily',
        data: roster
          .filter(a => a.handle && a.followers != null)
          .map(a => ({ handle: a.handle, followers: a.followers })),
      };
      const dayOfWeek = new Date().getUTCDay();
      if (dayOfWeek === 1) snapshot.type = 'weekly';
      snapshots.push(snapshot);
      if (snapshots.length > 120) snapshots.splice(0, snapshots.length - 120);
      await redis.set(ANR_SNAPSHOTS_KEY, snapshots);
      console.log(`A&R cron batch ${batchIndex} complete (LAST) — snapshot saved`);
    } else {
      console.log(`A&R cron batch ${batchIndex} complete — ${results.length} refreshed, ${errors.length} errors`);
    }

    return res.status(200).json({
      success: true,
      batch: batchIndex,
      totalBatches,
      refreshed: results.length,
      errors,
      snapshotSaved: isLastBatch,
      timestamp: new Date().toISOString(),
    });
  } catch (err) {
    console.error(`A&R cron batch ${batchIndex} failed:`, err);
    return res.status(500).json({ error: err.message });
  }
}
