import { Redis } from '@upstash/redis';
const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
});

export default async function handler(req, res) {
  try {
    const lastRefreshed = await redis.get('anr-last-refreshed');
    return res.status(200).json({ lastRefreshed: lastRefreshed || null });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
}
