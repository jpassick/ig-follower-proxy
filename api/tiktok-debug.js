export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  const apiKey = process.env.TIKTOK_RAPIDAPI_KEY;
  if (!apiKey) return res.status(500).json({ error: 'TIKTOK_RAPIDAPI_KEY not configured' });

  const handle = req.query.handle || 'nike';

  try {
    const r = await fetch(
      `https://tiktok-best-experience.p.rapidapi.com/user/${encodeURIComponent(handle)}`,
      {
        headers: {
          'x-rapidapi-host': 'tiktok-best-experience.p.rapidapi.com',
          'x-rapidapi-key': apiKey
        }
      }
    );

    const status = r.status;
    const headers = {};
    r.headers.forEach((v, k) => { headers[k] = v; });
    const text = await r.text();

    let parsed = null;
    try { parsed = JSON.parse(text); } catch(e) {}

    return res.status(200).json({
      http_status: status,
      response_headers: headers,
      raw_response: text,
      parsed_response: parsed,
      key_first_chars: apiKey.substring(0, 8) + '...',
      key_length: apiKey.length
    });
  } catch (err) {
    return res.status(500).json({
      error: 'Fetch failed',
      message: err.message
    });
  }
}
