// api/cron-snapshot-anr.js
// Runs daily at 9:30am EST (30 14 * * *)
// Refreshes all A&R prospect follower counts from RapidAPI, then saves a snapshot

import { Redis } from '@upstash/redis';

const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
});

const RAPIDAPI_KEY = process.env.RAPIDAPI_KEY;
const ANR_KEY = 'anr-roster';
const ANR_SNAPSHOTS_KEY = 'anr-snapshots';

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

export default async function handler(req, res) {
  const authHeader = req.headers['authorization'];
  if (authHeader !== `Bearer ${process.env.CRON_SECRET}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  try {
    const roster = await redis.get(ANR_KEY) || [];
    if (!roster.length) {
      return res.status(200).json({ message: 'No A&R prospects in roster' });
    }

    const now = Date.now();
    const results = [];
    const errors = [];

    for (const artist of roster) {
      try {
        const { followers, profilePic } = await fetchFollowerCount(artist.handle);
        artist.followers = followers;
        if (profilePic) artist.profilePic = profilePic;
        artist.lastUpdated = now;
        results.push({ handle: artist.handle, followers });
        await new Promise(r => setTimeout(r, 100));
      } catch (err) {
        errors.push({ handle: artist.handle, error: err.message });
        console.error(`Error refreshing A&R ${artist.handle}:`, err.message);
      }
    }

    await redis.set(ANR_KEY, roster);

    const snapshots = await redis.get(ANR_SNAPSHOTS_KEY) || [];
    const snapshot = {
      date: new Date().toISOString(),
      type: 'daily',
      data: roster.map(a => ({
        handle: a.handle,
        followers: a.followers,
      })),
    };

    const dayOfWeek = new Date().getUTCDay();
    if (dayOfWeek === 1) snapshot.type = 'weekly';

    snapshots.push(snapshot);
    if (snapshots.length > 120) snapshots.splice(0, snapshots.length - 120);
    await redis.set(ANR_SNAPSHOTS_KEY, snapshots);

    return res.status(200).json({
      success: true,
      refreshed: results.length,
      errors,
      snapshotType: snapshot.type,
      timestamp: new Date().toISOString(),
    });

  } catch (err) {
    console.error('A&R cron failed:', err);
    return res.status(500).json({ error: err.message });
  }
}
