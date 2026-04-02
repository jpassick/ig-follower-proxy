export default async function handler(req, res) {
  const authHeader = req.headers['authorization'];
  if (authHeader !== `Bearer ${process.env.CRON_SECRET}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const url = process.env.KV_REST_API_URL;
  const token = process.env.KV_REST_API_TOKEN;

  if (!url || !token) return res.status(500).json({ error: 'Redis not configured' });

  const rosterRes = await fetch(`${url}/get/roster`, {
    headers: { Authorization: `Bearer ${token}` }
  });
  const rosterData = await rosterRes.json();
  let roster = [];
  if (rosterData.result) {
    let val = rosterData.result;
    while (typeof val === 'string') { try { val = JSON.parse(val); } catch(e) { break; } }
    if (Array.isArray(val) && val.length === 1 && typeof val[0] === 'string') { try { val = JSON.parse(val[0]); } catch(e) {} }
    if (Array.isArray(val)) roster = val;
  }

  if (!roster.length) return res.status(200).json({ message: 'No roster found' });

  const snapRes = await fetch(`${url}/get/snapshots`, {
    headers: { Authorization: `Bearer ${token}` }
  });
  const snapData = await snapRes.json();
  let snapshots = [];
  if (snapData.result) {
    let val = snapData.result;
    while (typeof val === 'string') { try { val = JSON.parse(val); } catch(e) { break; } }
    if (Array.isArray(val) && val.length === 1 && typeof val[0] === 'string') { try { val = JSON.parse(val[0]); } catch(e) {} }
    if (Array.isArray(val)) snapshots = val;
  }

  const now = new Date();
  const snap = {
    date: now.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric', timeZone: 'America/New_York' }),
    ts: Date.now(),
    data: roster.filter(r => r.followers != null).map(r => ({ handle: r.handle, followers: r.followers }))
  };

  snapshots.push(snap);

  await fetch(`${url}/set/snapshots`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify([JSON.stringify(snapshots)])
  });

  return res.status(200).json({ ok: true, artists: snap.data.length, date: snap.date });
}
