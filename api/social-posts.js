export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  const smmKey = process.env.SMM_RAPIDAPI_KEY;
  if (!smmKey) return res.status(500).json({ error: 'SMM_RAPIDAPI_KEY not configured' });

  const masterID = typeof req.query.masterID === 'string' ? req.query.masterID.trim() : null;
  const platform = typeof req.query.platform === 'string' ? req.query.platform.trim().toLowerCase() : null;

  if (!masterID) return res.status(400).json({ error: 'Missing masterID' });
  if (!platform) return res.status(400).json({ error: 'Missing platform' });

  const VALID_PLATFORMS = ['tiktok', 'youtube', 'twitter', 'facebook', 'instagram'];
  if (!VALID_PLATFORMS.includes(platform)) {
    return res.status(400).json({ error: `Invalid platform. Must be one of: ${VALID_PLATFORMS.join(', ')}` });
  }

  const now = new Date();
  const defaultMonth = `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, '0')}-01`;
  const monthParam = (typeof req.query.month === 'string' && req.query.month.trim())
    ? req.query.month.trim()
    : defaultMonth;

  const params = new URLSearchParams({
    id: masterID,
    month: monthParam,
    type: 'posts',
    includeProfile: 'false'
  });

  const apiUrl = `https://social-media-master.p.rapidapi.com/universal-profile-posts?${params.toString()}`;

  let raw;
  try {
    const r = await fetch(apiUrl, {
      headers: {
        'x-rapidapi-host': 'social-media-master.p.rapidapi.com',
        'x-rapidapi-key': smmKey
      }
    });
    if (!r.ok) return res.status(502).json({ error: `SMM returned ${r.status}` });
    raw = await r.json();
  } catch (err) {
    return res.status(502).json({ error: `Fetch failed: ${err.message}` });
  }

  const rawPosts = Array.isArray(raw?.posts) ? raw.posts : [];

  const posts = rawPosts.slice(0, 8).map(p => {
    const d = p.postDetails || {};
    const s = p.postStats   || {};
    const fullText = typeof d.text === 'string' ? d.text : '';
    const title = fullText.split('\n')[0].trim() || d.postID || '';
    return {
      postID:    d.postID    || null,
      postUrl:   d.postUrl   || null,
      postImage: d.postImage || null,
      title,
      date:      d.date      || null,
      type:      d.type      || null,
      views:     d.videoViews ?? d.views ?? 0,
      likes:     d.likes     ?? 0,
      comments:  d.comments  ?? 0,
      er:        typeof s.er === 'number' ? parseFloat((s.er * 100).toFixed(2)) : null,
      grade:     s.mainGrade || null
    };
  });

  return res.status(200).json({
    ok: true,
    platform,
    masterID,
    period: raw?.about?.period || null,
    posts
  });
}
