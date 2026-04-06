import { Redis } from '@upstash/redis';

const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
});

const ANR_SNAPSHOTS_KEY = 'anr-snapshots';

export default async function handler(req, res) {
  if (req.method === 'GET') {
    try {
      const snapshots = await redis.get(ANR_SNAPSHOTS_KEY) || [];
      return res.status(200).json({ snapshots });
    } catch (e) {
      return res.status(500).json({ error: e.message });
    }
  }

  if (req.method === 'POST') {
    try {
      const { snapshots } = req.body;
      await redis.set(ANR_SNAPSHOTS_KEY, snapshots);
      return res.status(200).json({ ok: true });
    } catch (e) {
      return res.status(500).json({ error: e.message });
    }
  }

  return res.status(405).json({ error: 'Method not allowed' });
}
