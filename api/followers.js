export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const { username } = req.query;
  if (!username) return res.status(400).json({ error: 'username required' });

  const apiKey = process.env.RAPIDAPI_KEY;

  try {
    const response = await fetch(`https://instagram-best-experience.p.rapidapi.com/profile?username=${username}`, {
      headers: {
        'x-rapidapi-key': apiKey,
        'x-rapidapi-host': 'instagram-best-experience.p.rapidapi.com'
      }
    });
    const data = await response.json();
    res.status(200).json({
      follower_count: data.follower_count,
      full_name: data.full_name,
      username: data.username
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
}
