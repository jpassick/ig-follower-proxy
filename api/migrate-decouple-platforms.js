// One-time migration: extract tiktok_*, youtube_*, twitter_*, facebook_* fields
// from the IG roster into 4 standalone platform rosters.
// Admin-gated. Supports ?dryRun=true (no writes).
// DELETE this file after successful run.

const KV_URL = process.env.KV_REST_API_URL;
const KV_TOKEN = process.env.KV_REST_API_TOKEN;

async function kvGet(key) {
  const r = await fetch(`${KV_URL}/get/${encodeURIComponent(key)}`, {
    headers: { Authorization: `Bearer ${KV_TOKEN}` }
  });
  if (!r.ok) return null;
  const j = await r.json();
  let val = j.result;
  while (typeof val === 'string') {
    try { val = JSON.parse(val); } catch (e) { break; }
  }
  if (Array.isArray(val) && val.length === 1 && typeof val[0] === 'string') {
    try { val = JSON.parse(val[0]); } catch (e) { /* keep as-is */ }
  }
  while (typeof val === 'string') {
    try { val = JSON.parse(val); } catch (e) { break; }
  }
  return val;
}

async function kvSet(key, value) {
  // Double-wrap pattern (Jacob's standard)
  const body = JSON.stringify([JSON.stringify(value)]);
  const r = await fetch(`${KV_URL}/set/${encodeURIComponent(key)}`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${KV_TOKEN}`,
      'Content-Type': 'application/json'
    },
    body
  });
  return r.ok;
}

function extractPlatform(artist, prefix) {
  const handle = artist[`${prefix}_handle`];
  if (!handle) return null;
  return {
    handle,
    masterID: artist[`${prefix}_masterID`] || null,
    nickname: artist[`${prefix}_nickname`] || null,
    profilePic: artist[`${prefix}_profilePic`] || null,
    followers: typeof artist[`${prefix}_followers`] === 'number' ? artist[`${prefix}_followers`] : null,
    status: artist[`${prefix}_status`] || null,
    updatedAt: artist[`${prefix}_lastUpdated`] || null,
    is_fan_page: false
  };
}

module.exports = async (req, res) => {
  // Admin gate
  const pw = req.headers['x-admin-password'] || (req.query && req.query.pw);
  if (pw !== process.env.ADMIN_PASSWORD) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const dryRun = req.query && (req.query.dryRun === 'true' || req.query.dryRun === '1');

  try {
    const roster = await kvGet('roster');
    if (!Array.isArray(roster)) {
      return res.status(500).json({
        error: 'roster is not an array after unwrap',
        got: typeof roster,
        preview: roster
      });
    }

    const tiktokRoster = [];
    const youtubeRoster = [];
    const twitterRoster = [];
    const facebookRoster = [];

    const cleanedRoster = roster.map(artist => {
      const tt = extractPlatform(artist, 'tiktok');
      if (tt) tiktokRoster.push(tt);
      const yt = extractPlatform(artist, 'youtube');
      if (yt) youtubeRoster.push(yt);
      const tw = extractPlatform(artist, 'twitter');
      if (tw) twitterRoster.push(tw);
      const fb = extractPlatform(artist, 'facebook');
      if (fb) facebookRoster.push(fb);

      // Strip all platform-prefixed fields, keep only IG fields
      const cleaned = {};
      for (const k of Object.keys(artist)) {
        if (k.startsWith('tiktok_') ||
            k.startsWith('youtube_') ||
            k.startsWith('twitter_') ||
            k.startsWith('facebook_')) {
          continue;
        }
        cleaned[k] = artist[k];
      }
      return cleaned;
    });

    const summary = {
      dryRun,
      input: { ig_roster_size: roster.length },
      extracted: {
        tiktok: tiktokRoster.length,
        youtube: youtubeRoster.length,
        twitter: twitterRoster.length,
        facebook: facebookRoster.length
      },
      output: {
        cleaned_ig_roster_size: cleanedRoster.length,
        sample_cleaned_artist_keys: cleanedRoster[0] ? Object.keys(cleanedRoster[0]).sort() : []
      }
    };

    if (dryRun) {
      summary.message = 'DRY RUN — no writes performed.';
      summary.samples = {
        tiktok: tiktokRoster.slice(0, 2),
        youtube: youtubeRoster.slice(0, 2),
        twitter: twitterRoster.slice(0, 2),
        facebook: facebookRoster.slice(0, 2),
        ig_cleaned_first: cleanedRoster[0] || null
      };
      return res.status(200).json(summary);
    }

    // Backup original roster
    const backupOk = await kvSet('roster-pre-decouple-backup', roster);
    if (!backupOk) {
      return res.status(500).json({
        error: 'Backup write failed; aborting before any destructive change',
        summary
      });
    }

    // Write 4 new platform rosters
    const writes = await Promise.all([
      kvSet('tiktok-roster', tiktokRoster),
      kvSet('youtube-roster', youtubeRoster),
      kvSet('twitter-roster', twitterRoster),
      kvSet('facebook-roster', facebookRoster)
    ]);

    if (writes.some(w => !w)) {
      return res.status(500).json({
        error: 'One or more platform roster writes failed; IG roster NOT yet modified. Original is safe.',
        writes,
        summary
      });
    }

    // Overwrite IG roster with cleaned version
    const cleanedOk = await kvSet('roster', cleanedRoster);
    if (!cleanedOk) {
      return res.status(500).json({
        error: 'Cleaned IG roster write failed. Platform rosters were written but IG roster may now be inconsistent. Restore from roster-pre-decouple-backup.',
        summary
      });
    }

    summary.message = 'Migration complete. Rollback key: roster-pre-decouple-backup';
    return res.status(200).json(summary);

  } catch (e) {
    return res.status(500).json({ error: e.message, stack: e.stack });
  }
};
