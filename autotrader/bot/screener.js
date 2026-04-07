// ============================================================
// AutoTrader — Premarket Screener
// Runs at 9:00 AM ET every weekday.
// Scans universe, filters gap-ups, scores candidates,
// writes top picks to Supabase config.watchlist.
// ============================================================

const TD = 'https://api.twelvedata.com';

const SB_URL = process.env.SUPABASE_URL;
const SB_KEY = process.env.SUPABASE_SERVICE_KEY;

async function sbGet(table, query = '') {
  const r = await fetch(`${SB_URL}/rest/v1/${table}${query}`, {
    headers: { apikey: SB_KEY, Authorization: `Bearer ${SB_KEY}` }
  });
  return r.json();
}

async function sbPatch(table, data, query = '') {
  await fetch(`${SB_URL}/rest/v1/${table}${query}`, {
    method: 'PATCH',
    headers: { apikey: SB_KEY, Authorization: `Bearer ${SB_KEY}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(data)
  });
}

async function sbPost(table, data) {
  await fetch(`${SB_URL}/rest/v1/${table}`, {
    method: 'POST',
    headers: { apikey: SB_KEY, Authorization: `Bearer ${SB_KEY}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(data)
  });
}

async function log(type, message) {
  console.log(`[SCREENER][${type.toUpperCase()}] ${message}`);
  try { await sbPost('activity_log', { type, message }); } catch (e) {}
}

// ─── BATCH QUOTE FETCH ───────────────────────────────────────
// Twelve Data supports up to 120 symbols per call (comma-separated)
// Each symbol in the batch counts as 1 API credit
async function fetchBatchQuotes(symbols, tdKey) {
  const url = `${TD}/quote?symbol=${symbols.join(',')}&apikey=${tdKey}`;
  const r   = await fetch(url);
  const j   = await r.json();
  if (symbols.length === 1) {
    return j.status === 'error' ? {} : { [symbols[0]]: j };
  }
  return j;
}

// ─── SCORING ─────────────────────────────────────────────────
function scoreCandidate(quote, gapPct) {
  let score = 0;

  // Gap size
  if      (gapPct >= 5)   score += 40;
  else if (gapPct >= 3)   score += 30;
  else if (gapPct >= 2)   score += 20;
  else if (gapPct >= 1.5) score += 10;

  // Volume vs average
  const vol      = +quote.volume || 0;
  const avgVol   = +quote.average_volume || vol;
  const volRatio = avgVol > 0 ? vol / avgVol : 1;
  if      (volRatio >= 3)   score += 30;
  else if (volRatio >= 2)   score += 20;
  else if (volRatio >= 1.5) score += 10;

  // Price sweet spot $15-$200
  const px = +quote.close || +quote.price || 0;
  if      (px >= 15 && px <= 200) score += 15;
  else if (px > 200)              score += 8;

  // Not already at 52-week high
  const hi52 = +quote.fifty_two_week?.high || 0;
  if (hi52 > 0 && px < hi52 * 0.85) score += 10;

  return score;
}

// ─── MAIN SCREENER ───────────────────────────────────────────
export async function runScreener() {
  await log('scan', 'Premarket screener starting...');

  const cfgRows = await sbGet('config', '?id=eq.1');
  const cfg     = cfgRows[0];
  if (!cfg)          { await log('err', 'No config row found'); return; }
  if (!cfg.screener_enabled) { await log('info', 'Screener disabled — skipping'); return; }
  if (!cfg.td_key)   { await log('err', 'No Twelve Data key in config'); return; }

  const universe = (cfg.screener_universe || '').split(',').map(s => s.trim()).filter(Boolean);
  const minGap   = cfg.screener_min_gap    || 1.5;
  const minPrice = cfg.screener_min_price  || 10;
  const maxPrice = cfg.screener_max_price  || 500;
  const minVol   = cfg.screener_min_volume || 500000;
  const maxPicks = cfg.screener_max_picks  || 15;

  if (!universe.length) { await log('err', 'Screener universe is empty'); return; }

  await log('scan', `Scanning ${universe.length} stocks | min gap ${minGap}% | min vol ${minVol.toLocaleString()}`);

  const BATCH = 50;
  const candidates = [];

  for (let i = 0; i < universe.length; i += BATCH) {
    const batch = universe.slice(i, i + BATCH);
    try {
      const quotes = await fetchBatchQuotes(batch, cfg.td_key);
      for (const sym of batch) {
        const q = quotes[sym];
        if (!q || q.status === 'error') continue;

        const px        = +q.close || +q.price || 0;
        const prevClose = +q.previous_close || 0;
        const vol       = +q.volume || 0;
        const changePct = +q.percent_change || 0;

        if (px < minPrice || px > maxPrice) continue;
        if (vol < minVol)                   continue;
        if (changePct < minGap)             continue;
        if (prevClose <= 0)                 continue;

        const gapPct = (px - prevClose) / prevClose * 100;
        if (gapPct < minGap) continue;

        candidates.push({ sym, px, gapPct: +gapPct.toFixed(2), vol, score: scoreCandidate(q, gapPct) });
      }
    } catch (e) {
      await log('err', `Batch error (${batch[0]}...): ${e.message}`);
    }
    // Rate limit buffer between batches
    if (i + BATCH < universe.length) await new Promise(r => setTimeout(r, 1500));
  }

  await log('scan', `Found ${candidates.length} gap-up candidates`);

  if (!candidates.length) {
    await log('info', 'No candidates today — keeping existing watchlist');
    return;
  }

  // Sort by score, take top N
  candidates.sort((a, b) => b.score - a.score);
  const picks     = candidates.slice(0, maxPicks);
  const pickSyms  = picks.map(p => p.sym);

  // Keep open position in watchlist no matter what
  const stateRows = await sbGet('bot_state', '?id=eq.1');
  const openSym   = stateRows[0]?.open_trade?.symbol;
  if (openSym && !pickSyms.includes(openSym)) pickSyms.push(openSym);

  const summary = picks.map(p => `${p.sym}(+${p.gapPct}%)`).join(', ');
  await log('info', `Screener picks: ${summary}`);

  await sbPatch('config', {
    watchlist:            pickSyms.join(','),
    screener_last_run:    new Date().toISOString(),
    screener_last_picks:  summary,
    updated_at:           new Date().toISOString()
  }, '?id=eq.1');

  try {
    await sbPost('screener_log', {
      universe_size:    universe.length,
      candidates_found: candidates.length,
      picks:            pickSyms.join(','),
      details:          picks
    });
  } catch (e) {}

  await log('info', `Watchlist updated — ${pickSyms.length} stocks for today`);
}

// ─── SCHEDULER ───────────────────────────────────────────────
// Fires at 9:00 AM ET Mon-Fri (30 min before market open)
export function startScreenerScheduler() {
  console.log('[SCREENER] Scheduler active — runs 9:00 AM ET weekdays');
  let lastRunDate = '';
  setInterval(async () => {
    const et  = new Date(new Date().toLocaleString('en-US', { timeZone: 'America/New_York' }));
    const day = et.getDay();
    const h   = et.getHours();
    const m   = et.getMinutes();
    const today = et.toDateString();
    if (day >= 1 && day <= 5 && h === 9 && m === 0 && lastRunDate !== today) {
      lastRunDate = today;
      try { await runScreener(); } catch (e) { console.error('[SCREENER]', e.message); }
    }
  }, 60 * 1000);
}
