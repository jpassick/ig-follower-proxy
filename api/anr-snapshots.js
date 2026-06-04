// api/anr-snapshots.js
const KV_URL = process.env.KV_REST_API_URL;
const KV_TOKEN = process.env.KV_REST_API_TOKEN;

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

async function loadChunkedSnapshots() {
  const meta = await kvGet('anr-snapshots:meta');
  if (meta && meta.chunks) {
    const chunks = await Promise.all(
      Array.from({ length: meta.chunks }, (_, i) => kvGet(`anr-snapshots:chunk:${i}`))
    );
    return chunks.map(c => (Array.isArray(c) ? c : [])).flat().filter(Boolean);
  }
  // Fall back to legacy single key
  const legacy = await kvGet('anr-snapshots');
  return Array.isArray(legacy) ? legacy : [];
}

export default async function handler(req, res) {
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });
  try {
    const snapshots = await loadChunkedSnapshots();
    return res.status(200).json({ snapshots });
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
}
