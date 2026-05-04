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

    // Verify success: status must be "ok", data.status_code must be 0, and user object must exist
    if (json.status !== 'ok' || !json.data || json.data.status_code !== 0 || !json.data.user) {
      return res.status(404).json({
        error: 'Artist not found or invalid response',
        handle: cleanHandle,
        api_status: json.status,
        api_status_code: json.data?.status_code
      });
    }

    const u = json.data.user;

    // Extract avatar URL — prefer 300x300, fall back through chain
    let avatarUrl = null;
    if (u.avatar_300x300?.url_list?.length > 0) {
      avatarUrl = u.avatar_300x300.url_list[0];
    } else if (u.avatar_medium?.url_list?.length > 0) {
      avatarUrl = u.avatar_medium.url_list[0];
    } else if (u.avatar_thumb?.url_list?.length > 0) {
      avatarUrl = u.avatar_thumb.url_list[0];
    }

    return res.status(200).json({
      ok: true,
      handle: cleanHandle,
      data: {
        follower_count: u.follower_count ?? 0,
        nickname: u.nickname ?? cleanHandle,
        unique_id: u.unique_id ?? cleanHandle,
        uid: u.uid ?? null,
        avatar_url: avatarUrl,
        following_count: u.following_count ?? 0,
        total_favorited: u.total_favorited ?? 0,
        aweme_count: u.aweme_count ?? 0,
        signature: u.signature ?? '',
        verified: !!(u.enterprise_verify_reason && u.enterprise_verify_reason.length > 0),
        region: u.region ?? ''
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
