export default async function handler(req, res) {
  const authHeader = req.headers['authorization'];
  if (authHeader !== `Bearer ${process.env.CRON_SECRET}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const url = process.env.KV_REST_API_URL;
  const token = process.env.KV_REST_API_TOKEN;
  const rapidKey = process.env.RAPIDAPI_KEY;
  const resendKey = process.env.RESEND_API_KEY;

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

  const updated = await Promise.all(roster.map(async (r) => {
    try {
      const igRes = await fetch(`https://instagram-best-experience.p.rapidapi.com/profile?username=${r.handle}`, {
        headers: { 'x-rapidapi-key': rapidKey, 'x-rapidapi-host': 'instagram-best-experience.p.rapidapi.com' }
      });
      const igData = await igRes.json();
      return { ...r, followers: igData.follower_count || r.followers, status: 'ok' };
    } catch(e) { return r; }
  }));

  await fetch(`${url}/set/roster`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify([JSON.stringify(updated)])
  });

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
  const isMonday = now.getDay() === 1;

  const snap = {
    date: now.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric', timeZone: 'America/New_York' }),
    ts: Date.now(),
    data: updated.filter(r => r.followers != null).map(r => ({ handle: r.handle, followers: r.followers }))
  };
  snapshots.push(snap);

  await fetch(`${url}/set/snapshots`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify([JSON.stringify(snapshots)])
  });

  if (isMonday && resendKey && snapshots.length >= 2) {
    const latest = snapshots[snapshots.length - 1];
    const prev = snapshots[snapshots.length - 2];

    const fmt = (n) => {
      if (n == null) return '—';
      if (n >= 1000000) return (n/1000000).toFixed(2) + 'M';
      if (n >= 1000) return (n/1000).toFixed(1) + 'K';
      return Number(n).toLocaleString();
    };

    const sorted = [...latest.data].sort((a, b) => b.followers - a.followers);

    const rows = sorted.map(d => {
      const pe = prev.data.find(p => p.handle === d.handle);
      const diff = pe ? d.followers - pe.followers : null;
      const pct = pe ? ((d.followers - pe.followers) / pe.followers * 100) : null;
      const growthStr = diff !== null ? `${diff >= 0 ? '+' : ''}${Number(diff).toLocaleString()}` : '—';
      const pctStr = pct !== null ? `${pct >= 0 ? '+' : ''}${pct.toFixed(2)}%` : '—';
      const color = diff > 0 ? '#27ae60' : diff < 0 ? '#c0392b' : '#888';
      return `<tr>
        <td style="padding:8px 12px;border-bottom:1px solid #eee;"><a href="https://www.instagram.com/${d.handle}/" style="color:#1a1a1a;text-decoration:none;">@${d.handle}</a></td>
        <td style="padding:8px 12px;border-bottom:1px solid #eee;font-weight:600;">${fmt(d.followers)}</td>
        <td style="padding:8px 12px;border-bottom:1px solid #eee;color:${color};">${growthStr}</td>
        <td style="padding:8px 12px;border-bottom:1px solid #eee;color:${color};">${pctStr}</td>
      </tr>`;
    }).join('');

    const html = `
      <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;max-width:600px;margin:0 auto;padding:2rem;">
        <h1 style="font-size:20px;font-weight:600;margin-bottom:4px;">Instagram Weekly Report</h1>
        <p style="font-size:13px;color:#888;margin-bottom:1.5rem;">Alamo Records — ${latest.date}</p>
        <table style="width:100%;border-collapse:collapse;font-size:14px;">
          <thead>
            <tr style="background:#f9f9f7;">
              <th style="text-align:left;padding:8px 12px;font-size:12px;color:#888;font-weight:500;border-bottom:2px solid #eee;">Handle</th>
              <th style="text-align:left;padding:8px 12px;font-size:12px;color:#888;font-weight:500;border-bottom:2px solid #eee;">Followers</th>
              <th style="text-align:left;padding:8px 12px;font-size:12px;color:#888;font-weight:500;border-bottom:2px solid #eee;">Growth</th>
              <th style="text-align:left;padding:8px 12px;font-size:12px;color:#888;font-weight:500;border-bottom:2px solid #eee;">Growth %</th>
            </tr>
          </thead>
          <tbody>${rows}</tbody>
        </table>
        <p style="font-size:12px;color:#aaa;margin-top:1.5rem;">Auto-generated by Alamo IG Tracker</p>
      </div>`;

    await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${resendKey}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        from: 'Alamo IG Tracker <onboarding@resend.dev>',
        to: ['jpassick@gmail.com', 'jacob@alamo-records.com'],
        subject: `Alamo IG Weekly Report — ${latest.date}`,
        html
      })
    });
  }

  return res.status(200).json({ ok: true, artists: updated.length, date: snap.date, emailSent: isMonday });
}
