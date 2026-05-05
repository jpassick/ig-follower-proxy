// api/cron-backup-snapshots.js
// Runs daily at 12:00am EDT (0 4 * * * UTC)
// Copies snapshots, roster, and tiktok-snapshots to dated backup keys
// Zero RapidAPI calls — pure Redis operation

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
    const dateStr = new Date().toISOString().slice(0, 10); // YYYY-MM-DD
    const result = {
      success: true,
      timestamp: new Date().toISOString(),
      date: dateStr,
      snapshotsBacked: 0,
      rosterBacked: 0,
      tiktokSnapshotsBacked: 0
    };

    // Back up Instagram snapshots
    const snapshots = await kvGet('snapshots');
    if (Array.isArray(snapshots) && snapshots.length > 0) {
      await kvSet(`snapshots-backup-${dateStr}`, snapshots);
      await kvSet('snapshots-backup-latest', snapshots);
      result.snapshotsBacked = snapshots.length;
      console.log(`Backed up ${snapshots.length} snapshots to snapshots-backup-${dateStr}`);
    } else {
      console.log('No IG snapshots to back up');
    }

    // Back up roster (includes both IG and TikTok handle data)
    const roster = await kvGet('roster');
    if (Array.isArray(roster) && roster.length > 0) {
      await kvSet(`roster-backup-${dateStr}`, roster);
      await kvSet('roster-backup-latest', roster);
      result.rosterBacked = roster.length;
      console.log(`Backed up ${roster.length} roster artists to roster-backup-${dateStr}`);
    } else {
      console.log('No roster to back up');
    }

    // Back up TikTok snapshots
    const tiktokSnapshots = await kvGet('tiktok-snapshots');
    if (Array.isArray(tiktokSnapshots) && tiktokSnapshots.length > 0) {
      await kvSet(`tiktok-snapshots-backup-${dateStr}`, tiktokSnapshots);
      await kvSet('tiktok-snapshots-backup-latest', tiktokSnapshots);
      result.tiktokSnapshotsBacked = tiktokSnapshots.length;
      console.log(`Backed up ${tiktokSnapshots.length} TikTok snapshots to tiktok-snapshots-backup-${dateStr}`);
    } else {
      console.log('No TikTok snapshots to back up yet');
    }

    return res.status(200).json(result);
  } catch (err) {
    console.error('Backup cron failed:', err);
    return res.status(500).json({ error: err.message });
  }
}
