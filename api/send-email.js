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

  function getSnapshotForWindow(hoursAgo) {
    const cutoff = latest.ts - (hoursAgo * 60 * 60 * 1000);
    let best = null;
    for (let i = snapshots.length - 2; i >= 0; i--) {
      if (snapshots[i].ts <= cutoff) { best = snapshots[i]; break; }
      best = snapshots[i];
    }
    return best;
  }

  function formatDate(ts) {
    return new Date(ts).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric', timeZone: 'America/New_York' });
  }

  function growthCell(current, prevSnap, handle) {
    const pe = prevSnap ? prevSnap.data.find(p => p.handle === handle) : null;
    if (!pe) return { diff: null, pct: null };
    const diff = current - pe.followers;
    const pct = (diff / pe.followers) * 100;
    return { diff, pct };
  }

  function renderCell(diff, pct) {
    if (diff === null) return '<td style="padding:8px 12px;border-bottom:1px solid #eee;color:#ccc;font-size:13px;">—</td>';
    const color = diff > 0 ? '#27ae60' : diff < 0 ? '#c0392b' : '#888';
    const sign = diff >= 0 ? '+' : '';
    return `<td style="padding:8px 12px;border-bottom:1px solid #eee;color:${color};font-size:13px;">${sign}${Number(diff).toLocaleString()} (${sign}${pct.toFixed(2)}%)</td>`;
  }

  const prev24h = getSnapshotForWindow(24);
  const prev7d  = getSnapshotForWindow(24 * 7);
  const prev30d = getSnapshotForWindow(24 * 30);

  // Build combined rows sorted by 7d growth
  const combined = latest.data.map(d => {
    const g24 = growthCell(d.followers, prev24h, d.handle);
    const g7  = growthCell(d.followers, prev7d,  d.handle);
    const g30 = growthCell(d.followers, prev30d, d.handle);
    return { ...d, g24, g7, g30 };
  }).sort((a, b) => (b.g7.diff ?? -Infinity) - (a.g7.diff ?? -Infinity));

  const rows = combined.map(d => `
    <tr>
      <td style="padding:8px 12px;border-bottom:1px solid #eee;font-size:13px;">
        <a href="https://www.instagram.com/${d.handle}/" style="color:#1a1a1a;text-decoration:none;">@${d.handle}</a>
      </td>
      <td style="padding:8px 12px;border-bottom:1px solid #eee;font-weight:600;font-size:13px;">${fmt(d.followers)}</td>
      ${renderCell(d.g24.diff, d.g24.pct)}
      ${renderCell(d.g7.diff,  d.g7.pct)}
      ${renderCell(d.g30.diff, d.g30.pct)}
    </tr>`).join('');

  const th = (label, sub) => `
    <th style="text-align:left;padding:8px 12px;font-size:11px;color:#888;font-weight:500;border-bottom:2px solid #eee;">
      ${label}<br/><span style="font-weight:400;color:#bbb;">${sub}</span>
    </th>`;

  const html = `
    <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;max-width:700px;margin:0 auto;padding:2rem;">
      <div style="text-align:center;margin-bottom:2rem;">
        <a href="https://ig-follower-proxy.vercel.app" style="display:inline-block;background:#1a1a1a;color:#fff;font-size:13px;font-weight:500;padding:10px 24px;border-radius:8px;text-decoration:none;letter-spacing:0.03em;">View Live Dashboard →</a>
      </div>
      <h1 style="font-size:20px;font-weight:600;margin-bottom:4px;">Instagram Weekly Report</h1>
      <p style="font-size:13px;color:#888;margin-bottom:1.5rem;">Alamo Records / Santa Anna Roster — ${latest.date}</p>
      <table style="width:100%;border-collapse:collapse;">
        <thead>
          <tr style="background:#f9f9f7;">
            ${th('Handle', '')}
            ${th('Followers', '')}
            ${th('24h Growth', prev24h ? `${formatDate(prev24h.ts)} → ${formatDate(latest.ts)}` : 'not enough data')}
            ${th('7d Growth',  prev7d  ? `${formatDate(prev7d.ts)}  → ${formatDate(latest.ts)}` : 'not enough data')}
            ${th('30d Growth', prev30d ? `${formatDate(prev30d.ts)} → ${formatDate(latest.ts)}` : 'not enough data')}
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
      <p style="font-size:12px;color:#aaa;margin-top:1.5rem;text-align:center;">Auto-generated by Alamo IG Tracker</p>
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
