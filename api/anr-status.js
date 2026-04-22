// api/anr-status.js
const KV_URL = process.env.KV_REST_API_URL;
const KV_TOKEN = process.env.KV_REST_API_TOKEN;
const ANR_LAST_REFRESHED_KEY = 'anr-last-refreshed';

async function kvGet(key) {
  const r = await fetch(`${KV_URL}/get/${key}`, {
    headers: { Authorization: `Bearer ${KV_TOKEN}` }
  });
  const data = await r.json();
  if (!data.result) return null;
  let val = data.result;
  while (typeof val === 'string') {
    try { val = JSON.parse(val); } catch(e) { break; }
  }
  if (Array.isArray(val) && val.length === 1 && typeof val[0] === 'string') {
    try { val = JSON.parse(val[0]); } catch(e) {}
  }
  return val;
}

export default async function handler(req, res) {
  try {
    const lastRefreshed = await kvGet(ANR_LAST_REFRESHED_KEY);
    return res.status(200).json({ lastRefreshed: lastRefreshed || null });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
}
