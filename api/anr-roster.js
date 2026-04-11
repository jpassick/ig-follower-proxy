// api/anr-roster.js
const KV_URL = process.env.KV_REST_API_URL;
const KV_TOKEN = process.env.KV_REST_API_TOKEN;
const ANR_KEY = 'anr-roster';
const CHUNK_SIZE = 500;

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

async function kvDel(key) {
  await fetch(`${KV_URL}/del/${key}`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${KV_TOKEN}` }
  });
}

export default async function handler(req, res) {
  if (req.method === 'GET') {
    try {
      const meta = await kvGet(`${ANR_KEY}:meta`);
      if (meta && meta.chunks) {
        const chunks = await Promise.all(
          Array.from({ length: meta.chunks }, (_, i) => kvGet(`${ANR_KEY}:chunk:${i}`))
        );
        const roster = chunks
          .map(c => (Array.isArray(c) ? c : []))
          .flat()
          .filter(Boolean);
        return res.status(200).json({ roster });
      }
      const roster = await kvGet(ANR_KEY) || [];
      return res.status(200).json({ roster });
    } catch (e) {
      return res.status(500).json({ error: e.message });
    }
  }

  if (req.method === 'POST') {
    try {
      const { roster, overrideSafetyGuard } = req.body;
      if (!Array.isArray(roster)) {
        return res.status(400).json({ error: 'roster must be an array' });
      }

      // Safety guard — block suspicious writes unless explicitly overridden
      if (!overrideSafetyGuard) {
        const existingMeta = await kvGet(`${ANR_KEY}:meta`);
        const existingTotal = existingMeta?.total || 0;
        if (roster.length === 0 && existingTotal > 10) {
          return res.status(400).json({ error: `Blocked: attempt to overwrite ${existingTotal} artists with empty array. Pass overrideSafetyGuard: true to force.` });
        }
        if (existingTotal > 100 && roster.length < existingTotal * 0.5) {
          return res.status(400).json({ error: `Blocked: attempt to drop from ${existingTotal} to ${roster.length} artists (>50% drop). Pass overrideSafetyGuard: true to force.` });
        }
      }

      const chunks = [];
      for (let i = 0; i < roster.length; i += CHUNK_SIZE) {
        chunks.push(roster.slice(i, i + CHUNK_SIZE));
      }

      const existingMeta = await kvGet(`${ANR_KEY}:meta`);
      if (existingMeta && existingMeta.chunks > chunks.length) {
        await Promise.all(
          Array.from(
            { length: existingMeta.chunks - chunks.length },
            (_, i) => kvDel(`${ANR_KEY}:chunk:${chunks.length + i}`)
          )
        );
      }

      await Promise.all(
        chunks.map((chunk, i) => kvSet(`${ANR_KEY}:chunk:${i}`, chunk))
      );
      await kvSet(`${ANR_KEY}:meta`, { chunks: chunks.length, total: roster.length });

      console.log(`A&R roster saved: ${roster.length} artists in ${chunks.length} chunks`);
      return res.status(200).json({ ok: true, total: roster.length, chunks: chunks.length });
    } catch (e) {
      console.error('anr-roster POST error:', e);
      return res.status(500).json({ error: e.message });
    }
  }

  return res.status(405).json({ error: 'Method not allowed' });
}
