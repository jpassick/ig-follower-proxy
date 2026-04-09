// api/cron-snapshot-daily.js
// Runs daily at 10:00am EST (0 15 * * *)
// Refreshes all follower counts from RapidAPI, then saves a snapshot
import { Redis } from '@upstash/redis';
const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
});
const RAPIDAPI_KEY = process.env.RAPIDAPI_KEY;
const ROSTER_KEY = 'roster';
const SNAPSHOTS_KEY = 'snapshots';

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
    const roster = await redis.get(ROSTER_KEY) || [];
    if (!roster.length) {
      return res.status(200).json({ message: 'No artists in roster' });
    }
    const now = Date.now();
    const results = [];
    const errors = [];

    for (const artist of roster) {
      // Skip any entry with a missing or invalid handle
      if (!artist.handle || typeof artist.handle !== 'string' || !artist.handle.trim()) {
        console.warn('Skipping artist with invalid handle:', JSON.stringify(artist));
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
        console.error(`Error refreshing ${artist.handle}:`, err.message);
      }
    }

    // Save updated roster
    await redis.set(ROSTER_KEY, roster);

    // Save daily snapshot
    const snapshots = await redis.get(SNAPSHOTS_KEY) || [];
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
    await redis.set(SNAPSHOTS_KEY, snapshots);

    console.log(`Daily cron complete: ${results.length} refreshed, ${errors.length} errors`);
    return res.status(200).json({
      success: true,
      refreshed: results.length,
      errors,
      snapshotType: snapshot.type,
      timestamp: new Date().toISOString(),
    });
  } catch (err) {
    console.error('Daily cron failed:', err);
    return res.status(500).json({ error: err.message });
  }
}
