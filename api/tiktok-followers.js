export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  const apiKey = process.env.TIKTOK_RAPIDAPI_KEY;
  if (!apiKey) return res.status(500).json({ error: 'TIKTOK_RAPIDAPI_KEY not configured' });

  const handle = req.query.handle;
  if (!handle || typeof handle !== 'string') {
    return res.status(400).json({ error: 'Missing handle parameter' });
  }

  // Strip @ if present, trim whitespace, lowercase (TikTok handles are case-insensitive)
  const cleanHandle = handle.trim().replace(/^@/, '').toLowerCase();
  if (!cleanHandle) return res.status(400).json({ error: 'Invalid handle' });

  try {
    const r = await fetch(
      `https://tiktok-best-experience.p.rapidapi.com/user/${encodeURIComponent(cleanHandle)}`,
      {
        headers: {
          'x-rapidapi-host': 'tiktok-best-experience.p.rapidapi.com',
          'x-rapidapi-key': apiKey
        }
      }
    );

    if (!r.ok) {
      return res.status(r.status).json({
        error: `RapidAPI returned ${r.status}`,
        handle: cleanHandle
      });
    }

    const json = await r.json();

    if (json.status !== 'ok' || !json.data) {
      return res.status(404).json({
        error: 'Artist not found or invalid response',
        handle: cleanHandle,
        raw_status: json.status
      });
    }

    const d = json.data;

    // Extract avatar URL — prefer 300x300, fall back through chain
    let avatarUrl = null;
    if (d.avatar_300x300?.url_list?.length > 0) {
      avatarUrl = d.avatar_300x300.url_list[0];
    } else if (d.avatar_medium?.url_list?.length > 0) {
      avatarUrl = d.avatar_medium.url_list[0];
    } else if (d.avatar_thumb?.url_list?.length > 0) {
      avatarUrl = d.avatar_thumb.url_list[0];
    }

    return res.status(200).json({
      ok: true,
      handle: cleanHandle,
      data: {
        follower_count: d.follower_count ?? 0,
        nickname: d.nickname ?? cleanHandle,
        unique_id: d.unique_id ?? cleanHandle,
        uid: d.uid ?? null,
        avatar_url: avatarUrl,
        following_count: d.following_count ?? 0,
        total_favorited: d.total_favorited ?? 0,
        aweme_count: d.aweme_count ?? 0,
        signature: d.signature ?? '',
        verified: (d.verification_type ?? 0) !== 0,
        region: d.region ?? ''
      }
    });
  } catch (err) {
    return res.status(500).json({
      error: 'Failed to fetch TikTok data',
      message: err.message,
      handle: cleanHandle
    });
  }
}
