// Social follower report email — all 5 platforms, any growth period.
// POST (dashboard button): body { startTs, endTs, label } — falls back to Last 7 Days if missing/invalid.
// GET  (optional cron):   requires Authorization: Bearer CRON_SECRET — always Last 7 Days.
// Growth math mirrors index.html (getSnapPairForRange / resolveCurrent) with day boundaries in America/New_York.

const NY_TZ = 'America/New_York';
const DAY_MS = 24 * 60 * 60 * 1000;
const PLATFORM_ORDER = ['instagram', 'tiktok', 'youtube', 'twitter', 'facebook'];
const LABELS = { instagram: 'Instagram', tiktok: 'TikTok', youtube: 'YouTube', twitter: 'X', facebook: 'Facebook' };
const PROFILE_URL = {
  instagram: h => `https://www.instagram.com/${h}/`,
  tiktok:    h => `https://www.tiktok.com/@${h}`,
  youtube:   h => `https://www.youtube.com/@${h}`,
  twitter:   h => `https://x.com/${h}`,
  facebook:  h => `https://www.facebook.com/${h}`
};
const NOTES = {
  youtube: 'YouTube only publishes rounded subscriber counts (e.g. 7.33M moves in steps of 10,000; 36.2K in steps of 100). A growth of 0 means the rounded number didn\'t change — not necessarily that the channel didn\'t grow.',
  tiktok: 'TikTok follower counts are rounded above 10K (to the nearest 100, or the nearest 100K above 1M). Small changes may show as 0 growth.'
};

async function kvGet(url, token, key) {
  const r = await fetch(`${url}/get/${key}`, { headers: { Authorization: `Bearer ${token}` } });
  const data = await r.json();
  if (!data.result) return null;
  let val = data.result;
  while (typeof val === 'string') { try { val = JSON.parse(val); } catch (e) { break; } }
  if (Array.isArray(val) && val.length === 1 && typeof val[0] === 'string') { try { val = JSON.parse(val[0]); } catch (e) {} }
  return val;
}

async function getJSON(u) {
  const r = await fetch(u);
  if (!r.ok) throw new Error(`${u} returned ${r.status}`);
  return r.json();
}

function dayKey(ts) {
  return new Intl.DateTimeFormat('en-CA', { timeZone: NY_TZ, year: 'numeric', month: '2-digit', day: '2-digit' }).format(new Date(ts));
}
function snapTs(s) { return s.ts || new Date(s.date).getTime(); }

function getSnapPairForRange(handle, range, snapList) {
  if (!snapList.length) return { cur: null, prev: null, curTs: null };
  const endDay = dayKey(range.endTs), startDay = dayKey(range.startTs);
  let endSnap = null, startSnap = null, startSnapIdx = -1, endSnapIdx = -1;
  for (let i = 0; i < snapList.length; i++) {
    const d = dayKey(snapTs(snapList[i]));
    if (d <= endDay)   { endSnap = snapList[i];   endSnapIdx = i; }
    if (d <= startDay) { startSnap = snapList[i]; startSnapIdx = i; }
  }
  const curEntry = endSnap ? endSnap.data.find(d => d.handle === handle) : null;
  const curTs = (endSnap && curEntry) ? snapTs(endSnap) : null;
  let prevEntry = startSnap ? startSnap.data.find(d => d.handle === handle) : null;

  if (endSnap && startSnap && endSnap === startSnap) {
    let alt = null;
    for (let i = endSnapIdx - 1; i >= 0; i--) {
      const e = snapList[i].data.find(d => d.handle === handle);
      if (e) { alt = e; break; }
    }
    return { cur: curEntry ? curEntry.followers : null, prev: alt ? alt.followers : null, curTs };
  }
  if (startSnap && !prevEntry) {
    for (let i = startSnapIdx + 1; i < snapList.length; i++) {
      if (snapList[i] === endSnap) break;
      const e = snapList[i].data.find(d => d.handle === handle);
      if (e) { prevEntry = e; break; }
    }
  }
  return { cur: curEntry ? curEntry.followers : null, prev: prevEntry ? prevEntry.followers : null, curTs };
}

function resolveCurrent(r, pair, range) {
  const upd = Number(r.updatedAt);
  const liveUsable = r.followers != null && r.status === 'ok' && upd && !isNaN(upd)
    && dayKey(upd) <= dayKey(range.endTs)
    && (pair.curTs == null || upd > pair.curTs);
  if (liveUsable) return r.followers;
  return pair.cur ?? r.followers;
}

function normalizeSocialSnapshots(platform, data) {
  return (data.snapshots || []).map(s => ({
    date: s.date,
    ts: s.ts,
    data: Object.entries(s.artists || {}).map(([outerKey, info]) => ({
      handle: info[`${platform}_handle`] || outerKey,
      followers: info.followers,
      status: info.status
    }))
  }));
}

function buildRows(platform, roster, snaps, range) {
  return roster.filter(r => r && typeof r.handle === 'string').map(r => {
    const pair = getSnapPairForRange(r.handle, range, snaps);
    const cur = platform === 'instagram' ? resolveCurrent(r, pair, range) : (pair.cur ?? r.followers);
    const failed = !!(r.status && r.status !== 'ok');
    const diff = (!failed && cur != null && pair.prev != null) ? cur - pair.prev : null;
    const pct = (diff != null && pair.prev > 0) ? diff / pair.prev * 100 : null;
    return { handle: r.handle, followers: cur, diff, pct, failed, isFan: r.is_fan_page === true };
  });
}

function esc(s) {
  return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function fmt(n, exact) {
  if (n == null) return '—';
  if (exact) return Number(n).toLocaleString('en-US');
  if (n >= 1000000) return (n / 1000000).toFixed(2) + 'M';
  if (n >= 1000) return (n / 1000).toFixed(1) + 'K';
  return Number(n).toLocaleString('en-US');
}

function formatDate(ts) {
  return new Date(ts).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric', timeZone: NY_TZ });
}

// Shared styles live in one <style> block (Gmail supports it) to keep the email well under Gmail's ~102KB clip limit.
const EMAIL_CSS = `
  .w{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;max-width:640px;margin:0 auto;padding:2rem;}
  table.t{width:100%;border-collapse:collapse;}
  table.t th{text-align:left;padding:6px 10px;font-size:11px;color:#888;font-weight:500;border-bottom:2px solid #eee;background:#f9f9f7;}
  table.t td{padding:6px 10px;border-bottom:1px solid #eee;font-size:13px;}
  table.t td.f{font-weight:600;}
  table.t a{color:#1a1a1a;text-decoration:none;}
  .g{color:#27ae60;} .r{color:#c0392b;} .m{color:#888;}
  .tag{font-size:9px;border-radius:10px;padding:1px 6px;font-weight:600;text-transform:uppercase;margin-left:4px;}
  .fan{background:#f0f0f0;color:#888;} .fail{background:#fdf0ee;color:#c0392b;}
  td.fh{padding:14px 10px 6px;font-size:11px;color:#888;font-weight:600;letter-spacing:0.08em;text-transform:uppercase;border-bottom:2px solid #eee;background:#f9f9f7;}
  h2{font-size:16px;font-weight:600;margin:2rem 0 4px;}
  .note{font-size:11px;color:#aaa;margin:0 0 8px;}
`;

function renderRow(platform, d) {
  const cls = d.diff > 0 ? 'g' : d.diff < 0 ? 'r' : 'm';
  const diffStr = d.diff != null ? `${d.diff >= 0 ? '+' : ''}${Number(d.diff).toLocaleString('en-US')}` : '—';
  const pctStr = d.pct != null ? `${d.pct >= 0 ? '+' : ''}${d.pct.toFixed(2)}%` : '—';
  const fanTag = d.isFan ? '<span class="tag fan">FAN</span>' : '';
  const failTag = d.failed ? '<span class="tag fail">lookup failed</span>' : '';
  return `<tr><td><a href="${PROFILE_URL[platform](encodeURIComponent(d.handle))}">@${esc(d.handle)}</a>${fanTag}${failTag}</td><td class="f">${fmt(d.followers, platform === 'youtube')}</td><td class="${cls}">${pctStr}</td><td class="${cls}">${diffStr}</td></tr>`;
}

function renderSection(platform, rows, label, loadError) {
  const title = `<h2>${LABELS[platform]}</h2>`;
  if (loadError) return `${title}<p class="note" style="color:#c0392b;">Couldn't load ${LABELS[platform]} data for this email.</p>`;
  if (!rows.length) return `${title}<p class="note">No ${LABELS[platform]} accounts tracked.</p>`;
  const byPct = (a, b) => {
    const av = (a.pct == null || !isFinite(a.pct)) ? -Infinity : a.pct;
    const bv = (b.pct == null || !isFinite(b.pct)) ? -Infinity : b.pct;
    return bv - av;
  };
  const main = rows.filter(r => !r.isFan).sort(byPct);
  const fans = rows.filter(r => r.isFan).sort(byPct);
  const fanHeader = fans.length ? '<tr><td colspan="4" class="fh">Fan Pages</td></tr>' : '';
  const note = NOTES[platform] ? `<p class="note">${NOTES[platform]}</p>` : '';
  return `${title}${note}<table class="t"><thead><tr><th>Handle</th><th>Followers</th><th>Growth %</th><th>Growth</th></tr></thead><tbody>${main.map(r => renderRow(platform, r)).join('')}${fanHeader}${fans.map(r => renderRow(platform, r)).join('')}</tbody></table>`;
}

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const url = process.env.KV_REST_API_URL;
  const token = process.env.KV_REST_API_TOKEN;
  const resendKey = process.env.RESEND_API_KEY;
  if (!url || !token || !resendKey) return res.status(500).json({ error: 'Missing config' });

  const now = Date.now();
  let startTs = now - 7 * DAY_MS, endTs = now, label = 'Last 7 Days';

  if (req.method === 'GET') {
    const auth = req.headers.authorization || '';
    if (!process.env.CRON_SECRET || auth !== `Bearer ${process.env.CRON_SECRET}`) return res.status(401).json({ error: 'Unauthorized' });
  } else if (req.method === 'POST') {
    let body = req.body;
    if (typeof body === 'string') { try { body = JSON.parse(body); } catch (e) { body = {}; } }
    const s = Number(body && body.startTs), e = Number(body && body.endTs);
    if (isFinite(s) && isFinite(e) && s > 0 && s < e) {
      startTs = s;
      endTs = Math.min(e, now);
      label = (body && typeof body.label === 'string' && body.label.trim()) ? body.label.trim().slice(0, 60) : 'Custom range';
    }
  } else {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const range = { startTs, endTs };
  const host = req.headers['x-forwarded-host'] || req.headers.host;
  const base = `https://${host}`;

  // Instagram — Redis directly (same keys/unwind pattern as before)
  const igLoad = (async () => {
    const [snaps, roster] = await Promise.all([kvGet(url, token, 'snapshots'), kvGet(url, token, 'roster')]);
    return { snaps: Array.isArray(snaps) ? snaps : [], roster: Array.isArray(roster) ? roster : [] };
  })();

  // Other platforms — the same endpoints the dashboard reads, so shapes match exactly
  const socialLoads = ['tiktok', 'youtube', 'twitter', 'facebook'].map(p => (async () => {
    const snapUrl = p === 'tiktok' ? `${base}/api/tiktok-snapshots` : `${base}/api/social-snapshots?platform=${p}`;
    const [snapData, rosterData] = await Promise.all([getJSON(snapUrl), getJSON(`${base}/api/${p}-roster`)]);
    return { snaps: normalizeSocialSnapshots(p, snapData), roster: Array.isArray(rosterData) ? rosterData : [] };
  })());

  const results = await Promise.allSettled([igLoad, ...socialLoads]);

  const sections = PLATFORM_ORDER.map((p, i) => {
    const r = results[i];
    if (r.status !== 'fulfilled') {
      console.error(`send-email: ${p} load failed:`, r.reason && r.reason.message);
      return renderSection(p, [], label, true);
    }
    return renderSection(p, buildRows(p, r.value.roster, r.value.snaps, range), label, false);
  }).join('');

  if (results[0].status === 'fulfilled' && !results[0].value.snaps.length) {
    return res.status(400).json({ error: 'No snapshots to report on' });
  }

  const dateRange = `${formatDate(startTs)} → ${formatDate(endTs)}`;
  const html = `<!DOCTYPE html><html><head><meta charset="UTF-8"><style>${EMAIL_CSS}</style></head><body>
<div class="w">
<div style="text-align:center;margin-bottom:2rem;"><a href="https://ig-follower-proxy.vercel.app" style="display:inline-block;background:#1a1a1a;color:#fff;font-size:13px;font-weight:500;padding:10px 24px;border-radius:8px;text-decoration:none;">View Live Dashboard →</a></div>
<h1 style="font-size:20px;font-weight:600;margin-bottom:4px;">Social Follower Report — ${esc(label)}</h1>
<p style="font-size:13px;color:#888;margin-bottom:0.5rem;">Alamo Records / Santa Anna Roster — ${dateRange}</p>
${sections}
<p style="font-size:12px;color:#aaa;margin-top:2rem;text-align:center;">Auto-generated by Alamo Social Tracker</p>
</div></body></html>`;

  const emailRes = await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: { Authorization: `Bearer ${resendKey}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      from: 'Alamo Social Tracker <onboarding@resend.dev>',
      to: ['jacobpassickdigital@gmail.com'],
      subject: `Alamo Social Report — ${label} (${dateRange})`,
      html
    })
  });
  const emailData = await emailRes.json();
  if (emailRes.ok) return res.status(200).json({ ok: true, label, dateRange, bytes: html.length });
  return res.status(500).json({ error: emailData.message || 'Email failed' });
}
