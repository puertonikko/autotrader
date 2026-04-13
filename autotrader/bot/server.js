// ============================================================
// AutoTrader v3 -- USER ENGINE
// Runs on Railway Instance 2
// Reads shared market_scans every 30s
// Applies each user's personal thresholds + coefs
// Places orders for users whose criteria are met
// Handles position monitoring for all active users
// ============================================================
import express from 'express';

const app = express();
app.use(express.json());

const SB_URL = process.env.SUPABASE_URL;
const SB_KEY = process.env.SUPABASE_SERVICE_KEY;
if (!SB_URL || !SB_KEY) { console.error('Missing env vars'); process.exit(1); }

const H = { apikey: SB_KEY, Authorization: `Bearer ${SB_KEY}`, 'Content-Type': 'application/json' };
const sbGet  = (t,q='')  => fetch(`${SB_URL}/rest/v1/${t}${q}`,{headers:H}).then(r=>r.json());
const sbPatch = (t,d,q='') => fetch(`${SB_URL}/rest/v1/${t}${q}`,{method:'PATCH',headers:H,body:JSON.stringify(d)});
const sbPost  = (t,d)    => fetch(`${SB_URL}/rest/v1/${t}`,{method:'POST',headers:{...H,Prefer:'return=representation'},body:JSON.stringify(d)}).then(r=>r.json());

const getAllUsers    = () => sbGet('users','?running=eq.true');
const getUserById   = id => sbGet('users',`?id=eq.${id}`).then(r=>r[0]);
const getScans      = () => sbGet('market_scans','?order=scanned_at.desc');
const getScan       = sym => sbGet('market_scans',`?symbol=eq.${sym}`).then(r=>r[0]);
const getCfg        = () => sbGet('global_config','?id=eq.1').then(r=>r[0]);

async function patchUser(id, data) {
  return sbPatch('users', { ...data, updated_at: new Date().toISOString() }, `?id=eq.${id}`);
}
async function userLog(userId, type, message) {
  console.log(`[UE][${type.toUpperCase()}][${userId.slice(0,8)}] ${message}`);
  try { await sbPost('user_logs', { user_id: userId, type, message }); } catch(e) {}
}
async function addUserTrade(trade) { return sbPost('user_trades', trade); }
async function updateUserTrade(id, data) { return sbPatch('user_trades', data, `?id=eq.${id}`); }

// ── WEIGHTED COMPOSITE (per user coefs) ──────────────────────
function weightedComp(sigs, coefs) {
  const totW = Object.keys(sigs).reduce((s,k) => s + (coefs[k]||1), 0);
  return +(Object.entries(sigs).reduce((s,[k,v]) => s + v.score * (coefs[k]||1), 0) / totW).toFixed(1);
}

// ── CLEARING RULE ───────────────────────────────────────────
function bizAdd(d,n){let r=new Date(d),c=0;while(c<n){r.setDate(r.getDate()+1);if(r.getDay()!==0&&r.getDay()!==6)c++;}return r;}
const clearOk = lt => !lt || new Date() >= bizAdd(new Date(lt), 3);
function countBizDays(s,e){let c=0,d=new Date(s);while(d<e){d.setDate(d.getDate()+1);if(d.getDay()!==0&&d.getDay()!==6)c++;}return c;}
function isMarketOpen(){const et=new Date(new Date().toLocaleString('en-US',{timeZone:'America/New_York'}));if(et.getDay()===0||et.getDay()===6)return false;const m=et.getHours()*60+et.getMinutes();return m>=570&&m<960;}

// ── E*TRADE ORDER ───────────────────────────────────────────
// Per-user E*Trade credentials -- each user trades their own account
// POST https://api.etrade.com/v1/accounts/{accountIdKey}/orders/place
// Requires OAuth 1.0a HMAC-SHA1 -- implement signing proxy per user
async function placeOrder(user, sym, action, shares, price) {
  if (user.sandbox) {
    await userLog(user.id, 'skip', `[SANDBOX] ${action} ${shares}sh ${sym} @ $${price.toFixed(2)}`);
    return { success: true, sandboxed: true };
  }
  if (!user.e_key || !user.e_token || !user.e_account) {
    await userLog(user.id, 'err', 'E*Trade credentials missing');
    return { success: false };
  }
  // Add OAuth signing here per user when ready for live
  await userLog(user.id, 'err', 'E*Trade OAuth not yet wired');
  return { success: false };
}

// ── COEFFICIENT LEARNING ────────────────────────────────────
function updateCoefs(coefs, indSnap, result) {
  const clamp = v => +Math.max(0.05, Math.min(3, v)).toFixed(3);
  const win = result === 'WIN';
  const updated = { ...coefs };
  Object.keys(updated).forEach(k => {
    const s = indSnap?.[k]; if (!s) return;
    const correct = (win && s.bull === true) || (!win && s.bull === false);
    updated[k] = clamp(updated[k] + (s.bull === null ? 0 : correct ? 0.1 : -0.06));
  });
  return updated;
}

// ── POSITION MONITOR (per user) ──────────────────────────────
async function monitorPosition(user) {
  const t = user.open_trade; if (!t) return;

  // Get latest price from shared market_scans
  const scan = await getScan(t.symbol);
  const px = scan?.price || t.entry_price;
  const pnl = (px - t.entry_price) / t.entry_price * 100;

  // TRAILING STOP LOGIC
  // - No time limit on holding -- hold as long as trade is working
  // - Hard stop at -1.2% protects capital immediately
  // - Once price rises 1% from entry, trailing stop activates
  // - Trailing stop follows price up, 1% below the peak
  // - T+3 clearing wait still applies AFTER the sell
  const TRAIL_PCT = 1.0;   // trailing distance %
  const HARD_STOP = -1.2;  // hard stop loss %
  const ACTIVATE  = 1.0;   // pnl% to activate trailing stop

  const prevPeak = t.peak_price || t.entry_price;
  const newPeak  = Math.max(prevPeak, px);
  const peakPnl  = (newPeak - t.entry_price) / t.entry_price * 100;
  const trailActive = peakPnl >= ACTIVATE;
  const trailStop   = trailActive ? newPeak * (1 - TRAIL_PCT / 100) : null;

  const hitTrailStop = trailActive && px <= trailStop;
  const hitHardStop  = pnl <= HARD_STOP;

  if (hitTrailStop || hitHardStop) {
    const why    = hitTrailStop ? `trailing stop (peak +${peakPnl.toFixed(2)}%)` : `hard stop`;
    const result = pnl >= 0 ? 'WIN' : 'LOSS';
    await userLog(user.id, result === 'WIN' ? 'win' : 'loss',
      `${t.symbol} CLOSE -- ${why} | ${pnl >= 0 ? '+' : ''}${pnl.toFixed(2)}% @ $${px.toFixed(2)} | peak $${newPeak.toFixed(2)}`);
    const res = await placeOrder(user, t.symbol, 'SELL', t.shares, px);
    if (!res.success && !res.sandboxed) return;
    const newCoefs = updateCoefs(user.coefs, t.ind_snapshot, result);
    const newBal   = user.balance * (1 + pnl / 100);
    if (t.db_id) await updateUserTrade(t.db_id, { exit_price: px, result, pnl: +pnl.toFixed(3), exit_time: new Date().toISOString() });
    await patchUser(user.id, {
      balance:         newBal,
      open_trade:      null,
      last_trade_date: new Date().toISOString(),
      coefs:           newCoefs,
      status_text:     `SOLD ${t.symbol} ${pnl >= 0 ? '+' : ''}${pnl.toFixed(2)}% | peak was +${peakPnl.toFixed(2)}%`
    });
  } else {
    // Update peak price stored in the open trade
    const statusMsg = `Holding ${t.symbol} ${pnl >= 0 ? '+' : ''}${pnl.toFixed(2)}% | peak +${peakPnl.toFixed(2)}%` +
      (trailActive ? ` | trail stop $${trailStop.toFixed(2)}` : ` | trail activates at +${ACTIVATE}%`);
    await patchUser(user.id, {
      open_trade:  { ...t, peak_price: newPeak },
      status_text: statusMsg
    });
  }
}

// ── PROCESS ONE USER AGAINST LATEST SCANS ───────────────────
async function processUser(user, scans, cfg) {
  // Skip if user has open position -- monitor it instead
  if (user.open_trade) { await monitorPosition(user); return; }
  if (!isMarketOpen()) { await patchUser(user.id, { status_text: 'Market closed' }); return; }
  if (!clearOk(user.last_trade_date)) {
    const next = bizAdd(new Date(user.last_trade_date), 3);
    await patchUser(user.id, { status_text: `Clearing lock -- ${next.toLocaleDateString('en-US',{month:'short',day:'numeric'})}` });
    return;
  }

  // Get user's watchlist -- either custom or global screener picks
  const watchlist = (!user.use_global_watchlist && user.custom_watchlist)
    ? user.custom_watchlist.split(',').map(s=>s.trim()).filter(Boolean)
    : (cfg.active_watchlist||'').split(',').map(s=>s.trim()).filter(Boolean);

  // Find best opportunity from shared scans matching user's watchlist
  const candidates = scans
    .filter(s => watchlist.includes(s.symbol))
    .filter(s => {
      // Apply user's personal coefs to get their weighted score
      if (!s.sigs) return false;
      const userComp = weightedComp(s.sigs, user.coefs);
      s._userComp = userComp; // store for use below
      return userComp >= user.min_score;
    })
    .filter(s => {
      // Check grade threshold
      if (!s.grade) return s._userComp >= (user.min_score + 8); // no grade = require higher score
      const GRADES = ['A','B','C','D','F'];
      return GRADES.indexOf(s.grade) <= GRADES.indexOf(user.min_grade) &&
             s.grade_action === 'BUY' && !s.avoid;
    })
    .sort((a,b) => (b._userComp||0) - (a._userComp||0));

  if (!candidates.length) {
    await patchUser(user.id, { status_text: `No signals meeting grade ${user.min_grade} score ${user.min_score}` });
    return;
  }

  // Take the best candidate
  const best = candidates[0];
  const px = best.price;
  const shares = Math.max(1, Math.floor(user.balance * (user.pos_pct / 100) / px));
  const target = best.grade_target || +(px * 1.012).toFixed(2);
  const stop   = best.grade_stop   || +(px * 0.988).toFixed(2);

  await userLog(user.id, 'buy',
    `SIGNAL ${best.symbol} | Grade=${best.grade||'rule'} Score=${best._userComp} | BUY ${shares}sh @ $${px.toFixed(2)} | tgt $${target.toFixed(2)}`);

  const res = await placeOrder(user, best.symbol, 'BUY', shares, px);
  if (res.success || res.sandboxed) {
    const rows  = await addUserTrade({
      user_id: user.id, symbol: best.symbol, action: 'BUY', shares,
      entry_price: px, target, stop_price: stop, result: 'OPEN',
      grade: best.grade||'--', score: best._userComp,
      entry_time: new Date().toISOString(), ind_snapshot: best.sigs
    });
    const dbId = Array.isArray(rows) ? rows[0]?.id : null;
    await patchUser(user.id, {
      open_trade: { db_id: dbId, symbol: best.symbol, shares, entry_price: px, target, stop_price: stop, entry_time: new Date().toISOString(), grade: best.grade||'--', score: best._userComp, ind_snapshot: best.sigs },
      last_trade_date: new Date().toISOString(),
      status_text: `BOUGHT ${shares}sh ${best.symbol} @ $${px.toFixed(2)}`
    });
  }
}

// ── MAIN LOOP ───────────────────────────────────────────────
let loopInterval = null;

async function runLoop() {
  try {
    const [users, scans, cfg] = await Promise.all([getAllUsers(), getScans(), getCfg()]);
    if (!users?.length) return;
    // Check scan freshness -- skip if data is older than 5 minutes
    const freshScans = scans.filter(s => s.scanned_at && (Date.now() - new Date(s.scanned_at).getTime()) < 5 * 60 * 1000);
    if (!freshScans.length) { console.log('[UE] No fresh scan data yet -- waiting for market engine'); return; }
    // Process all running users in parallel
    await Promise.all(users.map(u => processUser(u, freshScans, cfg).catch(e => console.error(`[UE] User ${u.id.slice(0,8)}: ${e.message}`))));
  } catch(e) { console.error('[UE] Loop error:', e.message); }
}

// ── ROUTES ──────────────────────────────────────────────────
app.get('/', (req,res) => res.json({ service: 'AutoTrader User Engine', running: !!loopInterval, time: new Date().toISOString() }));
app.get('/health', (req,res) => res.json({ ok: true }));

// Register or get a user
app.post('/users/register', async (req, res) => {
  const { email } = req.body;
  if (!email) return res.status(400).json({ error: 'email required' });
  try {
    const existing = await sbGet('users', `?email=eq.${encodeURIComponent(email)}`);
    if (existing.length) return res.json({ user: existing[0], created: false });
    const rows = await sbPost('users', { email, balance: 10000, capital: 10000 });
    res.json({ user: Array.isArray(rows) ? rows[0] : rows, created: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Start/stop a specific user's bot
app.post('/users/:id/start', async (req,res) => { await patchUser(req.params.id,{running:true}); res.json({ok:true}); });
app.post('/users/:id/stop',  async (req,res) => { await patchUser(req.params.id,{running:false}); res.json({ok:true}); });

// Get a user's trades and logs
app.get('/users/:id/trades', async (req,res) => { res.json(await sbGet('user_trades',`?user_id=eq.${req.params.id}&order=created_at.desc&limit=100`)); });
app.get('/users/:id/logs',   async (req,res) => { res.json(await sbGet('user_logs',`?user_id=eq.${req.params.id}&order=created_at.desc&limit=200`)); });

const PORT = process.env.PORT || 3002;
app.listen(PORT, () => {
  console.log(`User Engine on port ${PORT}`);
  // Run every 30 seconds -- checks all active users against latest market scans
  loopInterval = setInterval(runLoop, 30 * 1000);
  runLoop(); // immediate first run
});
