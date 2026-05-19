export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');

  const adminPw = (req.headers?.authorization || '').replace('Bearer ', '');
  if (adminPw !== process.env.ADMIN_PASSWORD) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const url = process.env.KV_REST_API_URL;
  const token = process.env.KV_REST_API_TOKEN;
  if (!url || !token) return res.status(500).json({ error: 'Redis not configured' });

  const r = await fetch(`${url}/get/twitter-snapshots`, {
    headers: { Authorization: `Bearer ${token}` }
  });
  const data = await r.json();
  let snapshots = [];
  if (data.result) {
    let val = data.result;
    while (typeof val === 'string') {
      try { val = JSON.parse(val); } catch(e) { break; }
    }
    if (Array.isArray(val) && val.length === 1 && typeof val[0] === 'string') {
      try { val = JSON.parse(val[0]); } catch(e) {}
    }
    if (Array.isArray(val)) snapshots = val;
  }

  const may18 = snapshots.find(s => s && s.date === '2026-05-18');
  if (!may18) return res.status(404).json({ error: 'May 18 snapshot not found' });

  const before = {
    raqbaby: may18.artists?.['1raqbaby']?.followers,
    chenayders: may18.artists?.['chenayders']?.followers
  };

  if (may18.artists?.['1raqbaby'] && may18.artists['1raqbaby'].followers === 0) {
    may18.artists['1raqbaby'].followers = 4319;
  }
  if (may18.artists?.['chenayders'] && may18.artists['chenayders'].followers === 0) {
    may18.artists['chenayders'].followers = 1157;
  }

  await fetch(`${url}/set/twitter-snapshots`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify([JSON.stringify(snapshots)])
  });

  return res.status(200).json({
    ok: true,
    before,
    after: {
      raqbaby: may18.artists?.['1raqbaby']?.followers,
      chenayders: may18.artists?.['chenayders']?.followers
    }
  });
}
