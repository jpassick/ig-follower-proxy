import { Redis } from '@upstash/redis';

const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
});

const ANR_KEY = 'anr-roster';
const CHUNK_SIZE = 500;

export default async function handler(req, res) {
  if (req.method === 'GET') {
    try {
      const meta = await redis.get(`${ANR_KEY}:meta`);
      if (meta && meta.chunks) {
        const chunks = await Promise.all(
          Array.from({ length: meta.chunks }, (_, i) => redis.get(`${ANR_KEY}:chunk:${i}`))
        );
        const roster = chunks
          .map(c => (Array.isArray(c) ? c : []))
          .flat()
          .filter(Boolean);
        return res.status(200).json({ roster });
      }
      // fallback to old single key
      const roster = await redis.get(ANR_KEY) || [];
      return res.status(200).json({ roster });
    } catch (e) {
      return res.status(500).json({ error: e.message });
    }
  }

  if (req.method === 'POST') {
    try {
      const { roster } = req.body;
      if (!Array.isArray(roster)) {
        return res.status(400).json({ error: 'roster must be an array' });
      }

      const chunks = [];
      for (let i = 0; i < roster.length; i += CHUNK_SIZE) {
        chunks.push(roster.slice(i, i + CHUNK_SIZE));
      }

      // Delete any old chunks that may now be stale (e.g. roster shrank)
      const oldMeta = await redis.get(`${ANR_KEY}:meta`);
      if (oldMeta && oldMeta.chunks > chunks.length) {
        const deleteKeys = Array.from(
          { length: oldMeta.chunks - chunks.length },
          (_, i) => `${ANR_KEY}:chunk:${chunks.length + i}`
        );
        await Promise.all(deleteKeys.map(k => redis.del(k)));
      }

      // Write all chunks first, then update meta
      await Promise.all(
        chunks.map((chunk, i) => redis.set(`${ANR_KEY}:chunk:${i}`, chunk))
      );
      await redis.set(`${ANR_KEY}:meta`, { chunks: chunks.length, total: roster.length });

      return res.status(200).json({ ok: true });
    } catch (e) {
      return res.status(500).json({ error: e.message });
    }
  }

  return res.status(405).json({ error: 'Method not allowed' });
}
