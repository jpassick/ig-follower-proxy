import { Redis } from '@upstash/redis';
const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
});

export default async function handler(req, res) {
  const authHeader = req.headers['authorization'];
  if (authHeader !== `Bearer ${process.env.CRON_SECRET}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const snapshots = await redis.get('snapshots') || [];
  const before = snapshots.length;

  const cleaned = snapshots.filter(s => 
    s && s.data && Array.isArray(s.data) && 
    s.data.length > 0 && 
    s.data.some(d => d.handle && d.followers != null)
  );

  await redis.set('snapshots', cleaned);

  return res.status(200).json({ 
    before, 
    after: cleaned.length, 
    removed: before - cleaned.length 
  });
}
