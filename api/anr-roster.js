import { Redis } from '@upstash/redis';

const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
});

const ANR_KEY = 'anr-roster';

export default async function handler(req, res) {
  if (req.method === 'GET') {
    try {
      const roster = await redis.get(ANR_KEY) || [];
      return res.status(200).json({ roster });
    } catch (e) {
      return res.status(500).json({ error: e.message });
    }
  }

  if (req.method === 'POST') {
    try {
      const { roster } = req.body;
      await redis.set(ANR_KEY, roster);
      return res.status(200).json({ ok: true });
    } catch (e) {
      return res.status(500).json({ error: e.message });
    }
  }

  return res.status(405).json({ error: 'Method not allowed' });
}
