// api/restore-backup.js
// Restores snapshots, roster, and tiktok-snapshots from latest backup or a specific dated backup
// Requires ADMIN_PASSWORD

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
    const tiktokSnapshotsKey = `tiktok-snapshots-backup-${suffix}`;

    const snapshots = await kvGet(snapshotsKey);
    const roster = await kvGet(rosterKey);
    const tiktokSnapshots = await kvGet(tiktokSnapshotsKey);

    // Require at least one of the three to exist for restore to be meaningful
    const haveSnapshots = Array.isArray(snapshots) && snapshots.length > 0;
    const haveRoster = Array.isArray(roster) && roster.length > 0;
    const haveTiktokSnapshots = Array.isArray(tiktokSnapshots) && tiktokSnapshots.length > 0;

    if (!haveSnapshots && !haveRoster && !haveTiktokSnapshots) {
      return res.status(404).json({
        error: `No backup found for suffix: ${suffix}`,
        checked: [snapshotsKey, rosterKey, tiktokSnapshotsKey]
      });
    }

    const result = {
      success: true,
      timestamp: new Date().toISOString(),
      restoredFrom: suffix,
      snapshotsRestored: 0,
      rosterRestored: 0,
      tiktokSnapshotsRestored: 0
    };

    // Restore Instagram snapshots
    if (haveSnapshots) {
      await kvSet('snapshots', snapshots);
      result.snapshotsRestored = snapshots.length;
      console.log(`Restored ${snapshots.length} snapshots from ${snapshotsKey}`);
    }

    // Restore roster
    if (haveRoster) {
      await kvSet('roster', roster);
      result.rosterRestored = roster.length;
      console.log(`Restored ${roster.length} roster artists from ${rosterKey}`);
    }

    // Restore TikTok snapshots
    if (haveTiktokSnapshots) {
      await kvSet('tiktok-snapshots', tiktokSnapshots);
      result.tiktokSnapshotsRestored = tiktokSnapshots.length;
      console.log(`Restored ${tiktokSnapshots.length} TikTok snapshots from ${tiktokSnapshotsKey}`);
    }

    return res.status(200).json(result);
  } catch (err) {
    console.error('Restore failed:', err);
    return res.status(500).json({ error: err.message });
  }
}
