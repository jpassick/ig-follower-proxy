// api/cron-backup-snapshots.js
// Runs daily at 11:00pm EST (4 4 * * *)
// Copies snapshots to a dated backup key — no RapidAPI calls, just Redis

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
  const authHeader = req.headers['authorization'];
  if (authHeader !== `Bearer ${process.env.CRON_SECRET}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  try {
    // Back up snapshots
    const snapshots = await kvGet('snapshots');
    if (!Array.isArray(snapshots) || snapshots.length === 0) {
      console.log('No snapshots to back up');
      return res.status(200).json({ message: 'No snapshots to back up' });
    }

    // Back up roster
    const roster = await kvGet('roster');

    // Write dated backup
    const dateStr = new Date().toISOString().slice(0, 10); // e.g. 2026-04-10
    await kvSet(`snapshots-backup-${dateStr}`, snapshots);
    console.log(`Backed up ${snapshots.length} snapshots to snapshots-backup-${dateStr}`);

    // Also keep a rolling "latest backup" key as safety net
    await kvSet('snapshots-backup-latest', snapshots);

    // Back up roster too
    if (Array.isArray(roster) && roster.length > 0) {
      await kvSet(`roster-backup-${dateStr}`, roster);
      await kvSet('roster-backup-latest', roster);
      console.log(`Backed up ${roster.length} roster artists to roster-backup-${dateStr}`);
    }

    return res.status(200).json({
      success: true,
      snapshotsBacked: snapshots.length,
      rosterBacked: Array.isArray(roster) ? roster.length : 0,
      backupKey: `snapshots-backup-${dateStr}`,
      timestamp: new Date().toISOString(),
    });
  } catch (err) {
    console.error('Backup cron failed:', err);
    return res.status(500).json({ error: err.message });
  }
}
