// api/fix-snapshots.js
// One-time migration: moves legacy anr-snapshots single key → chunked keys
// Also fixes any corrupted data. Safe to run multiple times.
const KV_URL = process.env.KV_REST_API_URL;
const KV_TOKEN = process.env.KV_REST_API_TOKEN;
const SNAPSHOTS_PER_CHUNK = 15;
const MAX_TOTAL_SNAPSHOTS = 1095;

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
  const authHeader = req.headers['authorization'];
  if (authHeader !== `Bearer ${process.env.CRON_SECRET}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  try {
    // Try to read legacy single key first
    let snapshots = [];
    const legacy = await kvGet('anr-snapshots');
    if (Array.isArray(legacy) && legacy.length > 0) {
      snapshots = legacy;
      console.log(`Found legacy anr-snapshots key with ${snapshots.length} entries`);
    } else {
      // Already chunked — read existing chunks
      const meta = await kvGet('anr-snapshots:meta');
      if (meta && meta.chunks) {
        const chunks = await Promise.all(
          Array.from({ length: meta.chunks }, (_, i) => kvGet(`anr-snapshots:chunk:${i}`))
        );
        snapshots = chunks.map(c => (Array.isArray(c) ? c : [])).flat().filter(Boolean);
        console.log(`Found chunked anr-snapshots with ${snapshots.length} entries across ${meta.chunks} chunks`);
      }
    }

    // Clean: remove malformed entries
    const before = snapshots.length;
    snapshots = snapshots.filter(s =>
      s && s.date && s.data && Array.isArray(s.data) && s.data.length > 0
    );

    // Trim to max
    if (snapshots.length > MAX_TOTAL_SNAPSHOTS) {
      snapshots.splice(0, snapshots.length - MAX_TOTAL_SNAPSHOTS);
    }
    const after = snapshots.length;

    // Write chunked
    const chunks = [];
    for (let i = 0; i < snapshots.length; i += SNAPSHOTS_PER_CHUNK) {
      chunks.push(snapshots.slice(i, i + SNAPSHOTS_PER_CHUNK));
    }

    // Clean up any old chunks beyond new count
    const existingMeta = await kvGet('anr-snapshots:meta');
    if (existingMeta && existingMeta.chunks > chunks.length) {
      await Promise.all(
        Array.from({ length: existingMeta.chunks - chunks.length }, (_, i) =>
          kvDel(`anr-snapshots:chunk:${chunks.length + i}`)
        )
      );
    }

    await Promise.all(chunks.map((chunk, i) => kvSet(`anr-snapshots:chunk:${i}`, chunk)));
    await kvSet('anr-snapshots:meta', { chunks: chunks.length, total: snapshots.length });

    // Delete legacy single key now that chunks are written
    await kvDel('anr-snapshots');
    console.log(`Migration complete: ${before} → ${after} snapshots in ${chunks.length} chunks`);

    return res.status(200).json({
      success: true,
      before,
      after,
      removed: before - after,
      chunks: chunks.length,
    });

  } catch (err) {
    console.error('fix-snapshots failed:', err);
    return res.status(500).json({ error: err.message });
  }
}
