// ============================================================
// AutoTrader — Premarket Screener v2
// Uses Yahoo Finance (no API key needed)
// Auto-generates its own universe from most active + gainers
// Runs for ALL active users and updates each user's watchlist
// Fires at 9:00 AM ET every weekday
// ============================================================

const SB_URL = process.env.SUPABASE_URL;
const SB_KEY = process.env.SUPABASE_SERVICE_KEY;

// ─── SUPABASE ────────────────────────────────────────────────
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

async function addLog(type, message) {
  console.log(`[SCREENER][${type.toUpperCase()}] ${message}`);
  try { await sbPost('activity_log', { type, message }); } catch (e) {}
}

// ─── YAHOO FINANCE SCREENER ──────────────────────────────────
// Pulls from Yahoo's built-in screeners — no API key needed
// Uses the same data Yahoo Finance website uses

const YAHOO_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
  'Accept': 'application/json',
  'Accept-Language': 'en-US,en;q=0.9'
};

// Fetch one Yahoo screener endpoint — returns array of quote objects
async function fetchYahooScreen(screenerName, count = 100) {
  const url = `https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?formatted=false&scrIds=${screenerName}&count=${count}&start=0`;
  try {
    const r = await fetch(url, { headers: YAHOO_HEADERS });
    if (!r.ok) return [];
    const j = await r.json();
    return j?.finance?.result?.[0]?.quotes || [];
  } catch (e) {
    console.error(`[SCREENER] Yahoo ${screenerName} error:`, e.message);
    return [];
  }
}

// Fetch detailed quote for a symbol from Yahoo
async function fetchYahooQuote(sym) {
  try {
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${sym}?interval=1d&range=2d`;
    const r   = await fetch(url, { headers: YAHOO_HEADERS });
    if (!r.ok) return null;
    const j   = await r.json();
    const meta = j?.chart?.result?.[0]?.meta;
    if (!meta) return null;
    return {
      symbol:        sym,
      price:         meta.regularMarketPrice || 0,
      prevClose:     meta.chartPreviousClose || meta.previousClose || 0,
      volume:        meta.regularMarketVolume || 0,
      avgVolume:     meta.averageDailyVolume10Day || meta.averageDailyVolume3Month || 0,
      high52:        meta['52WeekHigh'] || 0,
      low52:         meta['52WeekLow'] || 0,
      marketCap:     meta.marketCap || 0
    };
  } catch (e) { return null; }
}

// ─── GATHER UNIVERSE FROM YAHOO SCREENERS ────────────────────
// Pulls most active, day gainers, and high volume from Yahoo
// Returns deduplicated list of symbols
async function gatherUniverse() {
  console.log('[SCREENER] Gathering universe from Yahoo Finance screeners...');

  const [mostActive, dayGainers, highVolume] = await Promise.all([
    fetchYahooScreen('most_actives',   150),
    fetchYahooScreen('day_gainers',    100),
    fetchYahooScreen('high_yield_bond', 50)  // fallback — high volume
  ]);

  // Combine all, deduplicate by symbol
  const seen = new Set();
  const all  = [...mostActive, ...dayGainers, ...highVolume];
  const universe = [];

  for (const q of all) {
    const sym = q.symbol;
    if (!sym || seen.has(sym)) continue;
    // Skip non-US, ETFs with dots, warrants, preferred shares
    if (sym.includes('.') || sym.includes('-') || sym.length > 5) continue;
    // Skip obvious ETFs and funds that aren't interesting for gap trading
    if (['SPY','QQQ','IWM','DIA','VTI','VOO','XLF','XLE','XLK','GLD','SLV','TLT','HYG','LQD'].includes(sym)) continue;
    seen.add(sym);
    universe.push({
      symbol:     sym,
      price:      q.regularMarketPrice       || 0,
      prevClose:  q.regularMarketPreviousClose || 0,
      volume:     q.regularMarketVolume       || 0,
      avgVolume:  q.averageDailyVolume10Day   || q.averageDailyVolume3Month || 0,
      changePct:  q.regularMarketChangePercent || 0,
      high52:     q.fiftyTwoWeekHigh          || 0
    });
  }

  console.log(`[SCREENER] Universe: ${universe.length} unique symbols from Yahoo`);
  return universe;
}

// ─── SCORE CANDIDATE ─────────────────────────────────────────
function scoreCandidate(q, gapPct) {
  let score = 0;

  // Gap size — the main signal
  if      (gapPct >= 8)  score += 50;
  else if (gapPct >= 5)  score += 40;
  else if (gapPct >= 3)  score += 30;
  else if (gapPct >= 2)  score += 20;
  else if (gapPct >= 1.5)score += 10;

  // Volume vs average — confirms the move
  const volRatio = q.avgVolume > 0 ? q.volume / q.avgVolume : 1;
  if      (volRatio >= 5)  score += 35;
  else if (volRatio >= 3)  score += 25;
  else if (volRatio >= 2)  score += 15;
  else if (volRatio >= 1.5)score += 8;

  // Price sweet spot — avoid penny stocks and ultra-high priced
  if      (q.price >= 15  && q.price <= 100) score += 15;
  else if (q.price >= 100 && q.price <= 300) score += 10;
  else if (q.price > 300)                    score += 5;

  // Not already extended to 52-week high — room to run
  if (q.high52 > 0 && q.price < q.high52 * 0.90) score += 10;

  return score;
}

// ─── MAIN SCREENER ───────────────────────────────────────────
export async function runScreener() {
  await addLog('scan', 'Premarket screener starting — Yahoo Finance auto-universe');

  // Load global screener config (use first config row for thresholds)
  // In multi-user these are shared thresholds — same candidates for everyone
  const cfgRows = await sbGet('config', '?id=eq.1');
  const cfg     = cfgRows[0];
  const minGap   = cfg?.screener_min_gap    || 1.5;
  const minPrice = cfg?.screener_min_price  || 10;
  const maxPrice = cfg?.screener_max_price  || 500;
  const minVol   = cfg?.screener_min_volume || 300000;
  const maxPicks = cfg?.screener_max_picks  || 15;

  // Step 1 — Get universe from Yahoo screeners
  const universe = await gatherUniverse();
  if (!universe.length) {
    await addLog('err', 'Could not fetch universe from Yahoo — keeping existing watchlists');
    return;
  }

  // Step 2 — Filter and score
  const candidates = [];
  for (const q of universe) {
    // Skip if price out of range
    if (q.price < minPrice || q.price > maxPrice) continue;
    // Skip if volume too low
    if (q.volume < minVol) continue;
    // Calculate gap vs previous close
    const prevClose = q.prevClose || q.price;
    if (prevClose <= 0) continue;
    const gapPct = (q.price - prevClose) / prevClose * 100;
    // Must be gapping up
    if (gapPct < minGap) continue;
    const score = scoreCandidate(q, gapPct);
    candidates.push({ sym: q.symbol, px: q.price, gapPct: +gapPct.toFixed(2), vol: q.volume, score });
  }

  await addLog('scan', `${candidates.length} candidates pass filters from ${universe.length} universe`);

  if (!candidates.length) {
    await addLog('info', 'No gap-up candidates today — watchlists unchanged');
    return;
  }

  // Step 3 — Sort by score, take top picks
  candidates.sort((a, b) => b.score - a.score);
  const picks    = candidates.slice(0, maxPicks);
  const pickSyms = picks.map(p => p.sym);
  const summary  = picks.map(p => `${p.sym}(+${p.gapPct}%)`).join(', ');

  await addLog('info', `Today's picks: ${summary}`);

  // Step 4 — Update ALL active users' watchlists
  // Get all config rows (one per user in multi-user setup)
  const allConfigs = await sbGet('config', '?select=id,user_id');

  let updatedCount = 0;
  for (const row of allConfigs) {
    // Keep open position in watchlist if user has one
    const query   = row.user_id
      ? `?user_id=eq.${row.user_id}&id=eq.1`
      : `?id=eq.1`;
    const stateRows = await sbGet('bot_state', query);
    const openSym   = stateRows[0]?.open_trade?.symbol;

    let userPicks = [...pickSyms];
    if (openSym && !userPicks.includes(openSym)) userPicks.push(openSym);

    await sbPatch('config', {
      watchlist:           userPicks.join(','),
      screener_last_run:   new Date().toISOString(),
      screener_last_picks: summary,
      updated_at:          new Date().toISOString()
    }, row.user_id ? `?user_id=eq.${row.user_id}` : `?id=eq.1`);

    updatedCount++;
  }

  // Step 5 — Log the screener run
  try {
    await sbPost('screener_log', {
      universe_size:    universe.length,
      candidates_found: candidates.length,
      picks:            pickSyms.join(','),
      details:          picks
    });
  } catch (e) {}

  await addLog('info', `Watchlists updated for ${updatedCount} user(s) — ${pickSyms.length} stocks`);
}

// ─── SCHEDULER ───────────────────────────────────────────────
// Runs at 9:00 AM ET Mon-Fri — 30 min before market open
export function startScreenerScheduler() {
  console.log('[SCREENER] Scheduler active — runs 9:00 AM ET weekdays for all users');
  let lastRunDate = '';
  setInterval(async () => {
    const et    = new Date(new Date().toLocaleString('en-US', { timeZone: 'America/New_York' }));
    const day   = et.getDay();
    const today = et.toDateString();
    if (day >= 1 && day <= 5 && et.getHours() === 9 && et.getMinutes() === 0 && lastRunDate !== today) {
      lastRunDate = today;
      try { await runScreener(); } catch (e) { console.error('[SCREENER]', e.message); }
    }
  }, 60 * 1000);
}
