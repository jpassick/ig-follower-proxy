// api/restore-backup.js
// Restores snapshots and roster from latest backup or a specific dated backup

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

async function kvSet(key, value) {
  await fetch(`${KV_URL}/set/${key}`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${KV_TOKEN}`, 'Content-Type': 'application/json' },
    body: JSON.stringify([JSON.stringify(value)])
  });
}

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  // Require admin password
  const { password, date } = req.method === 'POST' ? (req.body || {}) : req.query;
  if (password !== process.env.ADMIN_PASSWORD) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  try {
    // Use dated backup if provided, otherwise use latest
    const suffix = date || 'latest';
    const snapshotsKey = `snapshots-backup-${suffix}`;
    const rosterKey = `roster-backup-${suffix}`;

    const snapshots = await kvGet(snapshotsKey);
    const roster = await kvGet(rosterKey);

    if (!Array.isArray(snapshots) || snapshots.length === 0) {
      return res.status(404).json({ 
        error: `No backup found for key: ${snapshotsKey}` 
      });
    }

    // Restore snapshots
    await kvSet('snapshots', snapshots);
    console.log(`Restored ${snapshots.length} snapshots from ${snapshotsKey}`);

    // Restore roster if available
    let rosterRestored = 0;
    if (Array.isArray(roster) && roster.length > 0) {
      await kvSet('roster', roster);
      rosterRestored = roster.length;
      console.log(`Restored ${roster.length} roster artists from ${rosterKey}`);
    }

    return res.status(200).json({
      success: true,
      snapshotsRestored: snapshots.length,
      rosterRestored,
      restoredFrom: suffix,
      timestamp: new Date().toISOString(),
    });
  } catch (err) {
    console.error('Restore failed:', err);
    return res.status(500).json({ error: err.message });
  }
}
