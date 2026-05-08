export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  const smmKey = process.env.SMM_RAPIDAPI_KEY;
  if (!smmKey) return res.status(500).json({ error: 'SMM_RAPIDAPI_KEY not configured' });

  const handle = req.query.handle;
  if (!handle || typeof handle !== 'string') {
    return res.status(400).json({ error: 'Missing handle parameter' });
  }
  // Strip @ if present, trim whitespace, lowercase (TikTok handles are case-insensitive)
  const cleanHandle = handle.trim().replace(/^@/, '').toLowerCase();
  if (!cleanHandle) return res.status(400).json({ error: 'Invalid handle' });

  const tiktokUrl = `https://www.tiktok.com/@${cleanHandle}`;
  const apiUrl = `https://social-media-master.p.rapidapi.com/tiktok-user-account?url=${encodeURIComponent(tiktokUrl)}`;

  try {
    const r = await fetch(apiUrl, {
      headers: {
        'x-rapidapi-host': 'social-media-master.p.rapidapi.com',
        'x-rapidapi-key': smmKey,
        'Content-Type': 'application/json'
      }
    });

    if (!r.ok) {
      return res.status(r.status).json({
        error: `RapidAPI returned ${r.status}`,
        handle: cleanHandle
      });
    }

    const json = await r.json();
    const profile = json?.profile;
    const stats = json?.stats;

    if (!profile || !stats) {
      return res.status(404).json({
        error: 'Artist not found or invalid response',
        handle: cleanHandle,
        api_status: json?.status?.code
      });
    }

    return res.status(200).json({
      ok: true,
      handle: cleanHandle,
      data: {
        follower_count: stats.followersCount ?? 0,
        nickname: profile.name ?? cleanHandle,
        unique_id: profile.username ?? cleanHandle,
        uid: profile.userID ?? null,
        avatar_url: profile.image ?? null,
        // The fields below are not provided by SMM (only by the old API):
        following_count: null,
        total_favorited: null,
        aweme_count: null,
        signature: profile.description ?? '',
        verified: !!profile.verified,
        region: profile.countryCode ?? '',
        masterID: profile.masterID ?? null  // new: SMM masterID for downstream use
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
