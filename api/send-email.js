export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const url = process.env.KV_REST_API_URL;
  const token = process.env.KV_REST_API_TOKEN;
  const resendKey = process.env.RESEND_API_KEY;

  if (!url || !token || !resendKey) return res.status(500).json({ error: 'Missing config' });

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

  if (!snapshots.length) return res.status(400).json({ error: 'No snapshots to report on' });

  const latest = snapshots[snapshots.length - 1];

  const fmt = (n) => {
    if (n == null) return '—';
    if (n >= 1000000) return (n/1000000).toFixed(2) + 'M';
    if (n >= 1000) return (n/1000).toFixed(1) + 'K';
    return Number(n).toLocaleString();
  };

  // Find the snapshot closest to X hours ago
  function getSnapshotForWindow(hoursAgo) {
    const cutoff = latest.ts - (hoursAgo * 60 * 60 * 1000);
    let best = null;
    for (let i = snapshots.length - 2; i >= 0; i--) {
      if (snapshots[i].ts <= cutoff) { best = snapshots[i]; break; }
      best = snapshots[i];
    }
    return best;
  }

  const prev24h  = getSnapshotForWindow(24);
  const prev7d   = getSnapshotForWindow(24 * 7);
  const prev30d  = getSnapshotForWindow(24 * 30);

  function buildSection(title, dateRange, prevSnap) {
    const withGrowth = latest.data.map(d => {
      const pe = prevSnap ? prevSnap.data.find(p => p.handle === d.handle) : null;
      const diff = pe ? d.followers - pe.followers : null;
      const pct = pe ? ((d.followers - pe.followers) / pe.followers * 100) : null;
      return { ...d, diff, pct };
    }).filter(d => d.diff !== null)
      .sort((a, b) => (b.diff ?? -Infinity) - (a.diff ?? -Infinity));

    if (!withGrowth.length) return `
      <h2 style="font-size:16px;font-weight:600;margin:2rem 0 4px;">${title}</h2>
      <p style="font-size:13px;color:#aaa;margin-bottom:1rem;">${dateRange}</p>
      <p style="font-size:13px;color:#aaa;">Not enough snapshot history yet for this window.</p>`;

    const rows = withGrowth.map(d => {
      const growthStr = d.diff !== null ? `${d.diff >= 0 ? '+' : ''}${Number(d.diff).toLocaleString()}` : '—';
      const pctStr = d.pct !== null ? `${d.pct >= 0 ? '+' : ''}${d.pct.toFixed(2)}%` : '—';
      const color = d.diff > 0 ? '#27ae60' : d.diff < 0 ? '#c0392b' : '#888';
      return `<tr>
        <td style="padding:8px 12px;border-bottom:1px solid #eee;"><a href="https://www.instagram.com/${d.handle}/" style="color:#1a1a1a;text-decoration:none;">@${d.handle}</a></td>
        <td style="padding:8px 12px;border-bottom:1px solid #eee;font-weight:600;">${fmt(d.followers)}</td>
        <td style="padding:8px 12px;border-bottom:1px solid #eee;color:${color};">${growthStr}</td>
        <td style="padding:8px 12px;border-bottom:1px solid #eee;color:${color};">${pctStr}</td>
      </tr>`;
    }).join('');

    return `
      <h2 style="font-size:16px;font-weight:600;margin:2rem 0 4px;">${title}</h2>
      <p style="font-size:13px;color:#888;margin-bottom:1rem;">${dateRange}</p>
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
      </table>`;
  }

  function formatDate(ts) {
    return new Date(ts).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric', timeZone: 'America/New_York' });
  }

  const section24h = buildSection(
    '24 Hour Growth',
    prev24h ? `${formatDate(prev24h.ts)} → ${formatDate(latest.ts)}` : 'Not enough data yet',
    prev24h
  );

  const section7d = buildSection(
    '7 Day Growth',
    prev7d ? `${formatDate(prev7d.ts)} → ${formatDate(latest.ts)}` : 'Not enough data yet',
    prev7d
  );

  const section30d = buildSection(
    '30 Day Growth',
    prev30d ? `${formatDate(prev30d.ts)} → ${formatDate(latest.ts)}` : 'Not enough data yet',
    prev30d
  );

  const html = `
    <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;max-width:600px;margin:0 auto;padding:2rem;">
      <div style="text-align:center;margin-bottom:2rem;">
        <a href="https://ig-follower-proxy.vercel.app" style="display:inline-block;background:#1a1a1a;color:#fff;font-size:13px;font-weight:500;padding:10px 24px;border-radius:8px;text-decoration:none;letter-spacing:0.03em;">View Live Dashboard →</a>
      </div>
      <h1 style="font-size:20px;font-weight:600;margin-bottom:4px;">Instagram Weekly Report</h1>
      <p style="font-size:13px;color:#888;margin-bottom:2rem;">Alamo Records / Santa Anna Roster — ${latest.date}</p>
      ${section24h}
      ${section7d}
      ${section30d}
      <p style="font-size:12px;color:#aaa;margin-top:2rem;text-align:center;">Auto-generated by Alamo IG Tracker</p>
    </div>`;

  const emailRes = await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${resendKey}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      from: 'Alamo IG Tracker <onboarding@resend.dev>',
      to: ['jacobpassickdigital@gmail.com'],
      subject: `Alamo IG Weekly Report — ${latest.date}`,
      html
    })
  });

  const emailData = await emailRes.json();
  if (emailRes.ok) return res.status(200).json({ ok: true });
  return res.status(500).json({ error: emailData.message || 'Email failed' });
}
