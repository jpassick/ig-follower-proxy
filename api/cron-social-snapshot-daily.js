const PLATFORMS = {
  youtube: {
    handleEndpoint: '/youtube-channel-details',
    profileUrl: handle => `https://www.youtube.com/@${handle}`,
    wrapperKey: 'channel'
  },
  twitter: {
    handleEndpoint: '/twitter-user-account',
    profileUrl: handle => `https://x.com/${handle}`,
    wrapperKey: 'profile'
  },
  facebook: {
    handleEndpoint: '/facebook-user-account',
    profileUrl: handle => `https://www.facebook.com/${handle}`,
    wrapperKey: 'profile'
  }
};

function ymd(ts) {
  const d = new Date(ts);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const day = String(d.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

async function readRedis(url, token, key) {
  const r = await fetch(`${url}/get/${key}`, { headers: { Authorization: `Bearer ${token}` } });
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

async function writeRedis(url, token, key, value) {
  const r = await fetch(`${url}/set/${key}`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify([JSON.stringify(value)])
  });
  return r.ok;
}

async function fetchSMM(config, socialHandle, masterID, smmKey) {
  const useFastPath = !!masterID;
  const apiUrl = useFastPath
    ? `https://social-media-master.p.rapidapi.com/universal-profile-id?id=${encodeURIComponent(masterID)}`
    : `https://social-media-master.p.rapidapi.com${config.handleEndpoint}?url=${encodeURIComponent(config.profileUrl(socialHandle))}`;
  const r = await fetch(apiUrl, {
    headers: {
      'x-rapidapi-host': 'social-media-master.p.rapidapi.com',
      'x-rapidapi-key': smmKey,
      'Content-Type': 'application/json'
    }
  });
  if (!r.ok) throw new Error(`SMM HTTP ${r.status}`);
  const json = await r.json();
  const wrapper = useFastPath ? json?.profile : json?.[config.wrapperKey];
  const stats = json?.stats;
  if (!wrapper || !stats) throw new Error('Missing wrapper or stats');
  return {
    masterID: wrapper.masterID || masterID || null,
    followers: stats.followersCount ?? 0,
    nickname: wrapper.name ?? socialHandle,
    avatar: wrapper.image ?? null
  };
}

async function processPlatform(platform, config, roster, kvUrl, kvToken, smmKey, startTime) {
  const fHandle      = `${platform}_handle`;
  const fMasterID    = `${platform}_masterID`;
  const fNickname    = `${platform}_nickname`;
  const fProfilePic  = `${platform}_profilePic`;
  const fFollowers   = `${platform}_followers`;
  const fStatus      = `${platform}_status`;
  const fLastUpdated = `${platform}_lastUpdated`;

  const targets = roster.filter(a => typeof a[fHandle] === 'string' && a[fHandle].length > 0);
  let success = 0, fail = 0;

  for (const artist of targets) {
    if (Date.now() - startTime > 270000) {
      console.log(`[${platform}] Time budget exceeded, stopping`);
      break;
    }
    const idx = roster.findIndex(a => a.handle === artist.handle);
    if (idx === -1) continue;

    const socialHandle = roster[idx][fHandle];
    const masterID = roster[idx][fMasterID] || null;

    let fetched = null;
    try {
      fetched = await fetchSMM(config, socialHandle, masterID, smmKey);
    } catch (e) {
      await new Promise(r => setTimeout(r, 2000));
      try {
        fetched = await fetchSMM(config, socialHandle, masterID, smmKey);
      } catch (e2) {
        console.log(`[${platform}] @${artist.handle} failed twice: ${e2.message}`);
      }
    }

    const now = Date.now();
    if (fetched) {
      roster[idx] = {
        ...roster[idx],
        [fFollowers]: fetched.followers,
        [fNickname]: fetched.nickname,
        [fProfilePic]: fetched.avatar,
        [fStatus]: 'ok',
        [fLastUpdated]: now,
        [fMasterID]: fetched.masterID
      };
      success++;
    } else {
      roster[idx] = {
        ...roster[idx],
        [fStatus]: 'error',
        [fLastUpdated]: now
      };
      fail++;
    }
  }

  const today = ymd(Date.now());
  const snapshotEntry = { ts: Date.now(), date: today, artists: {} };
  for (const a of targets) {
    const idx = roster.findIndex(r => r.handle === a.handle);
    if (idx === -1) continue;
    snapshotEntry.artists[a.handle] = {
      [fHandle]: roster[idx][fHandle],
      followers: roster[idx][fFollowers] ?? null,
      status: roster[idx][fStatus] ?? null
    };
  }

  const snapshotsKey = `${platform}-snapshots`;
  let snapshots = (await readRedis(kvUrl, kvToken, snapshotsKey)) || [];
  if (!Array.isArray(snapshots)) snapshots = [];
  const existingIdx = snapshots.findIndex(s => s.date === today);
  if (existingIdx !== -1) snapshots[existingIdx] = snapshotEntry;
  else snapshots.push(snapshotEntry);

  await writeRedis(kvUrl, kvToken, snapshotsKey, snapshots);
  await writeRedis(kvUrl, kvToken, `${platform}-last-refreshed`, Date.now());

  return { platform, total: targets.length, success, fail };
}

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');

  const cronSecret = process.env.CRON_SECRET;
  const authHeader = req.headers?.authorization;
  if (!cronSecret || authHeader !== `Bearer ${cronSecret}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const kvUrl = process.env.KV_REST_API_URL;
  const kvToken = process.env.KV_REST_API_TOKEN;
  const smmKey = process.env.SMM_RAPIDAPI_KEY;
  if (!kvUrl || !kvToken) return res.status(500).json({ error: 'Redis not configured' });
  if (!smmKey) return res.status(500).json({ error: 'SMM_RAPIDAPI_KEY not configured' });

  const startTime = Date.now();

  let roster = (await readRedis(kvUrl, kvToken, 'roster')) || [];
  if (!Array.isArray(roster)) roster = [];
  if (roster.length === 0) return res.status(500).json({ error: 'Roster is empty' });

  const results = [];
  for (const [platform, config] of Object.entries(PLATFORMS)) {
    const r = await processPlatform(platform, config, roster, kvUrl, kvToken, smmKey, startTime);
    results.push(r);
    if (Date.now() - startTime > 270000) {
      console.log('Total time budget exceeded after platform:', platform);
      break;
    }
  }

  const clean = roster.filter(a => a && typeof a.handle === 'string' && a.handle.length > 0);
  await writeRedis(kvUrl, kvToken, 'roster', clean);

  return res.status(200).json({
    ok: true,
    duration_ms: Date.now() - startTime,
    results
  });
}
