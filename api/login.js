// api/login.js
export default async function handler(req, res) {
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const { password } = req.body;

  if (password === process.env.ADMIN_PASSWORD) {
    return res.status(200).json({ role: 'admin' });
  } else if (password === process.env.VIEW_PASSWORD) {
    return res.status(200).json({ role: 'viewer' });
  } else if (password === process.env.ANR_PASSWORD) {
    return res.status(200).json({ role: 'anr' });
  } else {
    return res.status(401).json({ error: 'Incorrect password' });
  }
}
