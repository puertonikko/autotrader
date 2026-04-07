// ============================================================
// AutoTrader Bot — Railway Server v2.0
// Twelve Data market data + Claude AI grading + E*Trade orders
// Premarket screener auto-populates watchlist at 9 AM ET
// Runs 24/7 on Railway, idles outside market hours
// ============================================================
import express from 'express';
import { runScreener, startScreenerScheduler } from './screener.js';

const app = express();
app.use(express.json());

const SB_URL = process.env.SUPABASE_URL;
const SB_KEY = process.env.SUPABASE_SERVICE_KEY;
if (!SB_URL || !SB_KEY) {
  console.error('SUPABASE_URL and SUPABASE_SERVICE_KEY are required');
  process.exit(1);
}

// ─── SUPABASE HELPERS ────────────────────────────────────────
const SB_HDR = { apikey: SB_KEY, Authorization: `Bearer ${SB_KEY}`, 'Content-Type': 'application/json' };

async function sbGet(table, query = '') {
  const r = await fetch(`${SB_URL}/rest/v1/${table}${query}`, { headers: SB_HDR });
  return r.json();
}
async function sbPatch(table, data, query = '') {
  const r = await fetch(`${SB_URL}/rest/v1/${table}${query}`, {
    method: 'PATCH', headers: { ...SB_HDR, Prefer: 'return=representation' },
    body: JSON.stringify(data)
  });
  return r.json();
}
async function sbPost(table, data) {
  const r = await fetch(`${SB_URL}/rest/v1/${table}`, {
    method: 'POST', headers: { ...SB_HDR, Prefer: 'return=representation' },
    body: JSON.stringify(data)
  });
  return r.json();
}

const getConfig  = () => sbGet('config',    '?id=eq.1').then(r => r[0]);
const getState   = () => sbGet('bot_state', '?id=eq.1').then(r => r[0]);
const patchState = d  => sbPatch('bot_state', { ...d, updated_at: new Date().toISOString() }, '?id=eq.1');

async function addLog(type, message) {
  console.log(`[${type.toUpperCase()}] ${message}`);
  try { await sbPost('activity_log', { type, message }); } catch (e) {}
}
async function addTrade(t)     { return sbPost('trades', t); }
async function updateTrade(id, d) { return sbPatch('trades', d, `?id=eq.${id}`); }

// ─── INDICATOR MATH ──────────────────────────────────────────
const sma = (a, p) => a.map((_, i) => i < p - 1 ? null : a.slice(i - p + 1, i + 1).reduce((s, v) => s + v, 0) / p);

function ema(a, p) {
  const k = 2 / (p + 1), r = new Array(a.length).fill(null);
  r[p - 1] = a.slice(0, p).reduce((s, v) => s + v, 0) / p;
  for (let i = p; i < a.length; i++) r[i] = a[i] * k + r[i - 1] * (1 - k);
  return r;
}

function rsi(c, p = 14) {
  const r = new Array(c.length).fill(null); let g = 0, l = 0;
  for (let i = 1; i <= p; i++) { const d = c[i] - c[i - 1]; d > 0 ? g += d : l -= d; }
  let ag = g / p, al = l / p;
  r[p] = 100 - 100 / (1 + (al === 0 ? 1e10 : ag / al));
  for (let i = p + 1; i < c.length; i++) {
    const d = c[i] - c[i - 1];
    ag = (ag * (p - 1) + (d > 0 ? d : 0)) / p;
    al = (al * (p - 1) + (d < 0 ? -d : 0)) / p;
    r[i] = 100 - 100 / (1 + (al === 0 ? 1e10 : ag / al));
  }
  return r;
}

function macd(c, f = 12, s = 26, sg = 9) {
  const ef = ema(c, f), es = ema(c, s);
  const ml = c.map((_, i) => ef[i] != null && es[i] != null ? ef[i] - es[i] : null);
  const vals = ml.filter(v => v != null), off = ml.findIndex(v => v != null);
  const se = ema(vals, sg);
  const sl = new Array(ml.length).fill(null), hl = new Array(ml.length).fill(null);
  for (let i = 0; i < se.length; i++) {
    const x = off + i;
    if (se[i] != null) { sl[x] = se[i]; hl[x] = ml[x] - se[i]; }
  }
  return { ml, sl, hl };
}

function bbands(c, p = 20, m = 2) {
  const mid = sma(c, p), up = [], lo = [];
  for (let i = 0; i < c.length; i++) {
    if (mid[i] == null) { up.push(null); lo.push(null); continue; }
    const sl = c.slice(i - p + 1, i + 1), mv = mid[i];
    const std = Math.sqrt(sl.reduce((a, v) => a + (v - mv) ** 2, 0) / p);
    up.push(mv + m * std); lo.push(mv - m * std);
  }
  return { up, mid, lo };
}

function calcVwap(data) {
  let cv = 0, cq = 0;
  return data.map(c => { const tp = (c.high + c.low + c.close) / 3; cv += tp * c.volume; cq += c.volume; return cq > 0 ? cv / cq : tp; });
}

function rmsVol(c, p = 20) {
  const r = new Array(c.length).fill(null);
  for (let i = p; i < c.length; i++) {
    const rets = [];
    for (let j = i - p + 1; j <= i; j++) rets.push((c[j] - c[j - 1]) / c[j - 1]);
    r[i] = Math.sqrt(rets.reduce((s, v) => s + v * v, 0) / rets.length) * 100;
  }
  return r;
}

function calcATR(data, p = 14) {
  const tr = data.map((c, i) => i === 0 ? c.high - c.low
    : Math.max(c.high - c.low, Math.abs(c.high - data[i - 1].close), Math.abs(c.low - data[i - 1].close)));
  return sma(tr, p);
}

function calcSlope(c, p = 20) {
  if (c.length < p) return 0;
  const s = c.slice(-p), n = s.length;
  const sx = n * (n - 1) / 2, sx2 = n * (n - 1) * (2 * n - 1) / 6;
  const sy = s.reduce((a, v) => a + v, 0), sxy = s.reduce((a, v, i) => a + i * v, 0);
  return (n * sxy - sx * sy) / (n * sx2 - sx * sx);
}

function momentum(c, p = 10) {
  const n = c.length - 1; return n < p ? 0 : (c[n] - c[n - p]) / c[n - p] * 100;
}

function analyze(data, liveVwap, coefs) {
  const closes = data.map(d => d.close), n = data.length - 1, px = data[n].close;
  const rv = rsi(closes, 14), md = macd(closes, 12, 26, 9), bb = bbands(closes, 20, 2);
  const sm20 = sma(closes, 20), sm50 = sma(closes, 50), e12 = ema(closes, 12), e26 = ema(closes, 26);
  const vwArr = calcVwap(data), rms = rmsVol(closes, 20), at = calcATR(data, 14);
  const rsiV = rv[n], mlV = md.ml[n], slV = md.sl[n], hlV = md.hl[n], hlP = md.hl[n - 1];
  const buV = bb.up[n], bmV = bb.mid[n], blV = bb.lo[n];
  const bbPct = buV && blV ? (px - blV) / (buV - blV) * 100 : 50;
  const vwV = liveVwap || vwArr[n], s20 = sm20[n], s50 = sm50[n], e1 = e12[n], e2 = e26[n];
  const sigs = {};

  if      (rsiV < 30) sigs.RSI = { score: 86, label: 'Oversold',      bull: true };
  else if (rsiV < 42) sigs.RSI = { score: 68, label: 'Bullish zone',   bull: true };
  else if (rsiV > 70) sigs.RSI = { score: 14, label: 'Overbought',     bull: false };
  else if (rsiV > 58) sigs.RSI = { score: 34, label: 'Bearish zone',   bull: false };
  else                sigs.RSI = { score: 50, label: 'Neutral',         bull: null };

  if (mlV != null && slV != null) {
    if      (hlV > 0 && hlP <= 0)  sigs.MACD = { score: 91, label: 'Bull cross', bull: true };
    else if (hlV < 0 && hlP >= 0)  sigs.MACD = { score: 9,  label: 'Bear cross', bull: false };
    else if (mlV > slV && mlV > 0) sigs.MACD = { score: 72, label: 'Bullish',    bull: true };
    else if (mlV < slV && mlV < 0) sigs.MACD = { score: 28, label: 'Bearish',    bull: false };
    else                            sigs.MACD = { score: 50, label: 'Mixed',      bull: null };
  } else sigs.MACD = { score: 50, label: 'No data', bull: null };

  if (blV != null) {
    if      (px <= blV * 1.005) sigs.BB = { score: 84, label: 'At lower band', bull: true };
    else if (px >= buV * 0.995) sigs.BB = { score: 16, label: 'At upper band', bull: false };
    else if (bbPct < 35)        sigs.BB = { score: 64, label: 'Lower half',    bull: true };
    else if (bbPct > 65)        sigs.BB = { score: 37, label: 'Upper half',    bull: false };
    else                        sigs.BB = { score: 50, label: 'Mid band',      bull: null };
  } else sigs.BB = { score: 50, label: 'Calculating', bull: null };

  const vwD = vwV ? (px - vwV) / vwV * 100 : 0;
  if      (px > vwV * 1.005) sigs.VWAP = { score: 66, label: `+${vwD.toFixed(2)}% above`, bull: true };
  else if (px < vwV * 0.995) sigs.VWAP = { score: 35, label: `${vwD.toFixed(2)}% below`,  bull: false };
  else                        sigs.VWAP = { score: 50, label: 'At VWAP',                    bull: null };

  if (s20 && s50) {
    if      (px > s20 && s20 > s50) sigs.SMA = { score: 76, label: 'Full uptrend',   bull: true };
    else if (px < s20 && s20 < s50) sigs.SMA = { score: 24, label: 'Full downtrend', bull: false };
    else if (px > s20 && s20 < s50) sigs.SMA = { score: 58, label: 'Recovery',       bull: true };
    else                             sigs.SMA = { score: 42, label: 'Below SMA20',    bull: false };
  } else sigs.SMA = { score: 50, label: 'Calculating', bull: null };

  if (e1 && e2) {
    if      (e1 > e2 && px > e1) sigs.EMA = { score: 74, label: 'Bull momentum', bull: true };
    else if (e1 < e2 && px < e1) sigs.EMA = { score: 26, label: 'Bear momentum', bull: false };
    else if (e1 > e2)             sigs.EMA = { score: 62, label: 'Bullish cross', bull: true };
    else                          sigs.EMA = { score: 38, label: 'Bearish cross', bull: false };
  } else sigs.EMA = { score: 50, label: 'Calculating', bull: null };

  const totW = Object.keys(sigs).reduce((s, k) => s + (coefs[k] || 1), 0);
  const comp = +(Object.entries(sigs).reduce((s, [k, v]) => s + v.score * (coefs[k] || 1), 0) / totW).toFixed(1);

  return {
    sigs, comp, px, vwap: vwV, rsiV, mlV, slV, hlV, hlP,
    buV, bmV, blV, bbPct: +bbPct.toFixed(1), s20, s50, e1, e2,
    rms: rms[n], atr: at[n], slp: calcSlope(closes, 20), mom: momentum(closes, 10)
  };
}

// ─── TWELVE DATA API ─────────────────────────────────────────
const TD = 'https://api.twelvedata.com';

async function fetchCandles(sym, interval, lookback, tdKey) {
  const url = `${TD}/time_series?symbol=${sym}&interval=${interval}&outputsize=${lookback}&order=ASC&apikey=${tdKey}`;
  const r = await fetch(url);
  const j = await r.json();
  if (j.status === 'error') throw new Error('TD: ' + j.message);
  if (!j.values?.length)    throw new Error('TD: no data for ' + sym);
  return j.values.map(v => ({ date: v.datetime, open: +v.open, high: +v.high, low: +v.low, close: +v.close, volume: +(v.volume || 0) }));
}

async function fetchQuote(sym, tdKey) {
  try {
    const r = await fetch(`${TD}/quote?symbol=${sym}&apikey=${tdKey}`);
    const j = await r.json();
    if (j.status === 'error') return null;
    return { price: +(j.close || j.price || 0), changePct: +(j.percent_change || 0) };
  } catch (e) { return null; }
}

async function fetchVWAP(sym, interval, tdKey) {
  if (interval === '1day') return null;
  try {
    const r = await fetch(`${TD}/vwap?symbol=${sym}&interval=${interval}&outputsize=1&apikey=${tdKey}`);
    const j = await r.json();
    if (j.status === 'error') return null;
    return +(j.values?.[0]?.vwap || 0) || null;
  } catch (e) { return null; }
}

// ─── CLAUDE AI GRADER ────────────────────────────────────────
async function aiGrade(sym, ind, antKey) {
  if (!antKey) return null;
  const prompt = `You are a quant signal grader for an autonomous trading bot. Return ONLY valid JSON, no other text.
TICKER:${sym} PRICE:$${ind.px.toFixed(2)} COMPOSITE:${ind.comp}/100
RSI=${ind.rsiV?.toFixed(2)} [${ind.sigs.RSI?.label}]
MACD=${ind.mlV?.toFixed(4)} SIG=${ind.slV?.toFixed(4)} HIST=${ind.hlV?.toFixed(4)} [${ind.sigs.MACD?.label}]
BB_UP=$${ind.buV?.toFixed(2)} MID=$${ind.bmV?.toFixed(2)} LO=$${ind.blV?.toFixed(2)} PCT=${ind.bbPct}% [${ind.sigs.BB?.label}]
VWAP=$${ind.vwap?.toFixed(2)} [${ind.sigs.VWAP?.label}]
SMA20=$${ind.s20?.toFixed(2)} SMA50=$${ind.s50?.toFixed(2)} [${ind.sigs.SMA?.label}]
EMA12=$${ind.e1?.toFixed(2)} EMA26=$${ind.e2?.toFixed(2)} [${ind.sigs.EMA?.label}]
RMS=${ind.rms?.toFixed(3)}% ATR=$${ind.atr?.toFixed(2)} SLOPE=${ind.slp?.toFixed(5)} MOM=${ind.mom?.toFixed(2)}%
STRATEGY: autonomous bot, target +1% per trade, max 3 business day hold, T+3 clearing enforced.
These stocks were pre-screened for premarket gap-up momentum.
Return ONLY this JSON: {"grade":"B","score":72,"action":"BUY","confidence":70,"reason":"one sentence","target":${(ind.px * 1.012).toFixed(2)},"stop":${(ind.px * 0.988).toFixed(2)},"strongest":"MACD","avoid":false}`;
  try {
    const r = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': antKey, 'anthropic-version': '2023-06-01' },
      body: JSON.stringify({ model: 'claude-sonnet-4-20250514', max_tokens: 200, messages: [{ role: 'user', content: prompt }] })
    });
    if (!r.ok) return null;
    const d = await r.json();
    return JSON.parse(d.content.map(c => c.text || '').join('').replace(/```json|```/g, '').trim());
  } catch (e) { await addLog('err', 'Claude: ' + e.message); return null; }
}

// ─── MARKET HOURS + CLEARING ─────────────────────────────────
function bizAdd(d, n) {
  let r = new Date(d), c = 0;
  while (c < n) { r.setDate(r.getDate() + 1); if (r.getDay() !== 0 && r.getDay() !== 6) c++; }
  return r;
}
const clearOk = lt => !lt || new Date() >= bizAdd(new Date(lt), 3);

function isMarketOpen() {
  const et  = new Date(new Date().toLocaleString('en-US', { timeZone: 'America/New_York' }));
  const day = et.getDay();
  if (day === 0 || day === 6) return false;
  const mins = et.getHours() * 60 + et.getMinutes();
  return mins >= 570 && mins < 960; // 9:30am-4:00pm ET
}

function countBizDays(s, e) {
  let c = 0, d = new Date(s);
  while (d < e) { d.setDate(d.getDate() + 1); if (d.getDay() !== 0 && d.getDay() !== 6) c++; }
  return c;
}

// ─── E*TRADE ORDER PLACEMENT ─────────────────────────────────
// POST https://api.etrade.com/v1/accounts/{accountIdKey}/orders/place
// Requires OAuth 1.0a HMAC-SHA1 signing — must be done server-side.
//
// To enable live trading:
// 1. npm install oauth-1.0a
// 2. Import and implement signAndPost() below
// 3. Flip sandbox to false in Config
//
// const OAuth = (await import('oauth-1.0a')).default;
// const crypto = await import('crypto');
// const oauth = new OAuth({
//   consumer: { key: cfg.e_key, secret: cfg.e_secret },
//   signature_method: 'HMAC-SHA1',
//   hash_function(base, key) {
//     return crypto.createHmac('sha1', key).update(base).digest('base64');
//   }
// });
// const url = `https://api.etrade.com/v1/accounts/${cfg.e_account}/orders/place`;
// const header = oauth.toHeader(oauth.authorize({ url, method: 'POST' }, { key: cfg.e_token, secret: cfg.e_token_secret }));
// const r = await fetch(url, { method: 'POST', headers: { ...header, 'Content-Type': 'application/json' }, body: JSON.stringify(body) });

async function placeOrder(sym, action, shares, price, cfg) {
  if (cfg.sandbox) {
    await addLog('skip', `[SANDBOX] ${action} ${shares}sh ${sym} @ $${price.toFixed(2)} — not sent to E*Trade`);
    return { success: true, sandboxed: true };
  }
  if (!cfg.e_key || !cfg.e_token || !cfg.e_account) {
    await addLog('err', 'E*Trade credentials missing in config');
    return { success: false };
  }
  // Uncomment OAuth implementation above when ready for live
  await addLog('err', 'E*Trade OAuth not yet wired — staying in sandbox');
  return { success: false };
}

// ─── COEFFICIENT LEARNING ────────────────────────────────────
function updateCoefs(coefs, indSnap, result) {
  const clamp  = v => +Math.max(0.05, Math.min(3, v)).toFixed(3);
  const win    = result === 'WIN';
  const updated = { ...coefs };
  Object.keys(updated).forEach(k => {
    const s = indSnap?.[k];
    if (!s) return;
    const correct = (win && s.bull === true) || (!win && s.bull === false);
    updated[k] = clamp(updated[k] + (s.bull === null ? 0 : correct ? 0.1 : -0.06));
  });
  return updated;
}

// ─── POSITION MONITOR ────────────────────────────────────────
async function monitorPosition(state, cfg) {
  const t = state.open_trade;
  if (!t) return;

  let px = t.entry_price;
  try { const q = await fetchQuote(t.symbol, cfg.td_key); if (q?.price > 0) px = q.price; } catch (e) {}

  const pnl  = (px - t.entry_price) / t.entry_price * 100;
  const days = countBizDays(new Date(t.entry_time), new Date());

  const hitTarget = pnl >= 1.0;
  const hitStop   = px  <= t.stop_price;
  const expired   = days >= 3;

  if (hitTarget || hitStop || expired) {
    const why    = hitTarget ? '+1% target hit' : hitStop ? 'stop hit' : '3-day expiry';
    const result = pnl >= 0 ? 'WIN' : 'LOSS';
    await addLog(result === 'WIN' ? 'win' : 'loss',
      `${t.symbol} CLOSE — ${why} | ${pnl >= 0 ? '+' : ''}${pnl.toFixed(2)}% @ $${px.toFixed(2)}`);

    const res = await placeOrder(t.symbol, 'SELL', t.shares, px, cfg);
    if (!res.success && !res.sandboxed) return;

    const newCoefs = updateCoefs(state.coefs, t.ind_snapshot, result);
    const newBal   = state.balance * (1 + pnl / 100);

    if (t.db_id) {
      await updateTrade(t.db_id, { exit_price: px, result, pnl: +pnl.toFixed(3), exit_time: new Date().toISOString() });
    }

    await patchState({
      balance:           newBal,
      open_trade:        null,
      last_trade_date:   new Date().toISOString(),
      coefs:             newCoefs,
      status_text:       `SOLD ${t.symbol} ${pnl >= 0 ? '+' : ''}${pnl.toFixed(2)}%`
    });
  } else {
    await patchState({
      status_text: `Holding ${t.symbol} ${pnl >= 0 ? '+' : ''}${pnl.toFixed(2)}% | tgt $${t.target?.toFixed(2)} stop $${t.stop_price?.toFixed(2)} | day ${days}/3`
    });
  }
}

// ─── MAIN SCAN CYCLE ─────────────────────────────────────────
let scanning = false;

async function scanCycle() {
  if (scanning) return;
  scanning = true;
  try {
    const [state, cfg] = await Promise.all([getState(), getConfig()]);
    if (!state?.running) { scanning = false; return; }

    // Always monitor open position first
    if (state.open_trade) {
      await monitorPosition(state, cfg);
      scanning = false;
      return;
    }

    // Market hours gate
    if (!isMarketOpen()) {
      await patchState({ status_text: 'Market closed — bot idle' });
      scanning = false;
      return;
    }

    // T+3 clearing gate
    if (!clearOk(state.last_trade_date)) {
      const next = bizAdd(new Date(state.last_trade_date), 3);
      await patchState({ status_text: `Clearing lock — next trade: ${next.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })}` });
      scanning = false;
      return;
    }

    // Pick next ticker from screener-populated watchlist
    const watchlist = (cfg.watchlist || '').split(',').map(s => s.trim()).filter(Boolean);
    if (!watchlist.length) {
      await patchState({ status_text: 'Watchlist is empty — screener runs at 9 AM ET' });
      scanning = false;
      return;
    }

    const idx    = (state.scan_idx || 0) % watchlist.length;
    const sym    = watchlist[idx];
    const newIdx = (state.scan_idx || 0) + 1;

    await addLog('scan', `Scanning ${sym} [${idx + 1}/${watchlist.length}] cycle ${Math.ceil(newIdx / watchlist.length)}`);
    await patchState({ scan_idx: newIdx, status_text: `Scanning ${sym}...` });

    // Fetch data from Twelve Data
    const candles = await fetchCandles(sym, cfg.interval, cfg.lookback, cfg.td_key);
    let liveVwap  = null;
    if (cfg.interval !== '1day') {
      try { liveVwap = await fetchVWAP(sym, cfg.interval, cfg.td_key); } catch (e) {}
    }

    // Calculate all indicators
    const ind = analyze(candles, liveVwap, state.coefs);
    await patchState({ last_analysis: { symbol: sym, ...ind } });

    // Get live quote for accurate fill price
    let px = ind.px;
    try { const q = await fetchQuote(sym, cfg.td_key); if (q?.price > 0) px = q.price; } catch (e) {}

    // AI grade from Claude
    let grade = null;
    if (cfg.ant_key) {
      grade = await aiGrade(sym, ind, cfg.ant_key);
      if (grade) await patchState({ last_grade: grade });
    }

    // Decision logic
    const GRADE_ORDER = ['A', 'B', 'C', 'D', 'F'];
    const scoreOk = ind.comp >= cfg.min_score;
    const gOk     = grade
      ? GRADE_ORDER.indexOf(grade.grade) <= GRADE_ORDER.indexOf(cfg.min_grade) && !grade.avoid
      : ind.comp >= (cfg.min_score + 8);
    const isBuy   = grade ? grade.action === 'BUY' : ind.comp >= cfg.min_score;

    if (scoreOk && gOk && isBuy) {
      const shares = Math.max(1, Math.floor(state.balance * (cfg.pos_pct / 100) / px));
      const target = grade?.target ? +grade.target : +(px * 1.012).toFixed(2);
      const stop   = grade?.stop   ? +grade.stop   : +(px * 0.988).toFixed(2);

      await addLog('buy',
        `SIGNAL ${sym} | Grade=${grade?.grade || 'rule'} Score=${ind.comp} Conf=${grade?.confidence || '--'}% | BUY ${shares}sh @ $${px.toFixed(2)} | tgt $${target.toFixed(2)} stop $${stop.toFixed(2)}`);

      const res = await placeOrder(sym, 'BUY', shares, px, cfg);
      if (res.success || res.sandboxed) {
        const rows  = await addTrade({
          symbol: sym, action: 'BUY', shares, entry_price: px, target,
          stop_price: stop, result: 'OPEN', grade: grade?.grade || '--',
          score: ind.comp, entry_time: new Date().toISOString(), ind_snapshot: ind.sigs
        });
        const dbId = Array.isArray(rows) ? rows[0]?.id : null;

        await patchState({
          open_trade: {
            db_id: dbId, symbol: sym, shares, entry_price: px, target,
            stop_price: stop, entry_time: new Date().toISOString(),
            grade: grade?.grade || '--', score: ind.comp, ind_snapshot: ind.sigs
          },
          last_trade_date: new Date().toISOString(),
          status_text: `BOUGHT ${shares}sh ${sym} @ $${px.toFixed(2)} — watching for +1%`
        });
      }
    } else {
      const why = !scoreOk ? `score ${ind.comp} < ${cfg.min_score}`
                : !gOk     ? `grade ${grade?.grade} below ${cfg.min_grade}${grade?.avoid ? ' (avoid)' : ''}`
                : `signal=${grade?.action || 'neutral'}`;
      await addLog('skip', `${sym} — no trade (${why})`);
      await patchState({ status_text: `${sym} skipped — ${why}` });
    }

  } catch (err) {
    await addLog('err', err.message);
    try { await patchState({ status_text: 'Error: ' + err.message }); } catch (e) {}
  }
  scanning = false;
}

// ─── BOT LOOP ────────────────────────────────────────────────
let botInterval = null;

async function startLoop() {
  const cfg      = await getConfig();
  const cycleSec = cfg?.cycle_seconds || 60;
  await addLog('info', `Bot started — scanning every ${cycleSec}s | sandbox=${cfg?.sandbox}`);
  await patchState({ status_text: 'Bot started' });
  botInterval = setInterval(async () => {
    const state = await getState();
    if (!state?.running) {
      clearInterval(botInterval);
      botInterval = null;
      await addLog('info', 'Bot stopped (running=false in Supabase)');
      await patchState({ status_text: 'Stopped' });
      return;
    }
    scanCycle();
  }, cycleSec * 1000);
  scanCycle(); // Run immediately on start
}

// ─── API ENDPOINTS ───────────────────────────────────────────
app.get('/',        (req, res) => res.json({ status: 'AutoTrader running', time: new Date().toISOString(), loopActive: !!botInterval }));
app.get('/health',  (req, res) => res.json({ ok: true }));

app.post('/bot/start', async (req, res) => {
  await patchState({ running: true });
  if (!botInterval) await startLoop();
  res.json({ ok: true, message: 'Bot started' });
});

app.post('/bot/stop', async (req, res) => {
  await patchState({ running: false });
  res.json({ ok: true, message: 'Bot stopping' });
});

app.post('/screener/run', async (req, res) => {
  res.json({ ok: true, message: 'Screener running in background' });
  try { await runScreener(); } catch (e) { await addLog('err', 'Manual screener: ' + e.message); }
});

// ─── STARTUP ─────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, async () => {
  console.log(`AutoTrader bot listening on port ${PORT}`);
  startScreenerScheduler(); // Schedule 9 AM ET screener
  try {
    const state = await getState();
    if (state?.running) {
      console.log('Resuming — bot was running before restart');
      await startLoop();
    } else {
      console.log('Bot is stopped — waiting for START command from dashboard');
    }
  } catch (e) {
    console.error('Startup error:', e.message);
  }
});
