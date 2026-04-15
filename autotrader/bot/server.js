// ============================================================
// AutoTrader Bot — Railway Server v7.0 — Multi-User Scale
// Supports 1000+ users simultaneously
// Each user has their own:
//   - Schwab/E*Trade credentials & tokens
//   - Watchlist, config, and trading rules
//   - Bot state, trades, and balance
//   - Market data calls (via their own API credentials)
// ============================================================
import express from 'express';
import crypto from 'crypto';
import { runScreener, startScreenerScheduler } from './screener.js';

const app = express();

// CORS — allow dashboard to call Railway API
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || 'https://autotrader-ruby.vercel.app';
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', ALLOWED_ORIGIN);
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();
  next();
});

// Stripe webhook needs raw body before json parser
app.use('/stripe/webhook', express.raw({ type: 'application/json' }));
app.use(express.json());

const SB_URL = process.env.SUPABASE_URL;
const SB_KEY = process.env.SUPABASE_SERVICE_KEY;
if (!SB_URL || !SB_KEY) { console.error('SUPABASE_URL and SUPABASE_SERVICE_KEY required'); process.exit(1); }

// ─── SUPABASE ────────────────────────────────────────────────
const SB_HDR = { apikey: SB_KEY, Authorization: `Bearer ${SB_KEY}`, 'Content-Type': 'application/json' };

async function sbGet(table, query = '') {
  const r = await fetch(`${SB_URL}/rest/v1/${table}${query}`, { headers: SB_HDR });
  return r.json();
}
async function sbPatch(table, data, query = '') {
  const r = await fetch(`${SB_URL}/rest/v1/${table}${query}`, {
    method: 'PATCH',
    headers: { ...SB_HDR, Prefer: 'return=representation' },
    body: JSON.stringify(data)
  });
  return r.json();
}
async function sbPost(table, data) {
  const r = await fetch(`${SB_URL}/rest/v1/${table}`, {
    method: 'POST',
    headers: { ...SB_HDR, Prefer: 'return=representation' },
    body: JSON.stringify(data)
  });
  return r.json();
}
async function sbUpsert(table, data) {
  const r = await fetch(`${SB_URL}/rest/v1/${table}`, {
    method: 'POST',
    headers: { ...SB_HDR, Prefer: 'resolution=merge-duplicates,return=representation' },
    body: JSON.stringify(data)
  });
  return r.json();
}

// Per-user data functions
const getActiveUsers = () => sbGet('config', '?subscription_status=eq.active&select=*');
const getUserState   = uid => sbGet('bot_state', `?user_id=eq.${uid}`).then(r => r[0]);
const patchUserState = (uid, d) => sbPatch('bot_state', { ...d, updated_at: new Date().toISOString() }, `?user_id=eq.${uid}`);
const patchUserConfig = (uid, d) => sbPatch('config', { ...d, updated_at: new Date().toISOString() }, `?user_id=eq.${uid}`);
const addUserLog = async (uid, type, message) => {
  console.log(`[${uid.substring(0,8)}] [${type.toUpperCase()}] ${message}`);
  try { await sbPost('activity_log', { user_id: uid, type, message }); } catch (e) {}
};
const addUserTrade = (uid, t) => sbPost('trades', { ...t, user_id: uid });
const updateUserTrade = (id, d) => sbPatch('trades', d, `?id=eq.${id}`);

// ─── ONESIGNAL ───────────────────────────────────────────────
async function sendPush(cfg, title, message, url = '') {
  if (!cfg.onesignal_app_id || !cfg.onesignal_api_key) return;
  try {
    await fetch('https://onesignal.com/api/v1/notifications', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Basic ${cfg.onesignal_api_key}` },
      body: JSON.stringify({
        app_id: cfg.onesignal_app_id, included_segments: ['All'],
        headings: { en: title }, contents: { en: message },
        url: url || undefined, ttl: 3600, priority: 10
      })
    });
  } catch (e) {}
}

// ─── EMAIL (RESEND) ──────────────────────────────────────────
const RESEND_API_KEY = process.env.RESEND_API_KEY;
const FROM_EMAIL     = process.env.FROM_EMAIL || 'AutoTrader <onboarding@resend.dev>';
const ADMIN_EMAIL    = 'quantautotraderai@gmail.com';

async function sendEmail(to, subject, html) {
  if (!RESEND_API_KEY || !to) return;
  try {
    const r = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${RESEND_API_KEY}` },
      body: JSON.stringify({ from: FROM_EMAIL, to: [to], subject, html })
    });
    const j = await r.json();
    if (j.id) console.log(`[EMAIL] Sent to ${to}`);
    else console.log('[EMAIL] Error:', JSON.stringify(j));
  } catch (e) { console.log('[EMAIL] Failed:', e.message); }
}

async function getNotificationEmail(cfg) {
  if (cfg.notification_email) return cfg.notification_email;
  try {
    const r = await fetch(`${SB_URL}/auth/v1/admin/users/${cfg.user_id}`, {
      headers: { apikey: SB_KEY, Authorization: `Bearer ${SB_KEY}` }
    });
    const j = await r.json();
    return j.email || null;
  } catch (e) { return null; }
}

function tradeEmailHtml({ type, symbol, shares, entryPrice, exitPrice, pnl, balance, grade, score, reason }) {
  const isWin = type === 'WIN';
  const isBuy = type === 'BUY';
  const color = isWin ? '#00e891' : type === 'LOSS' ? '#f0364a' : '#f5a623';
  const emoji = isWin ? '🟢' : type === 'LOSS' ? '🔴' : isBuy ? '🟡' : '⚪';
  return `<!DOCTYPE html><html><body style="margin:0;padding:0;background:#050a0f;font-family:monospace">
<div style="max-width:480px;margin:0 auto;padding:24px">
<div style="background:#091422;border:1px solid #162d47;border-radius:10px;overflow:hidden">
<div style="background:#0d1c30;padding:16px 20px;border-bottom:1px solid #162d47">
  <div style="font-size:11px;font-weight:700;letter-spacing:.14em;color:#f5a623">AUTOTRADER</div>
  <div style="font-size:18px;font-weight:700;color:#b8d4ee;margin-top:4px">${emoji} ${type} — ${symbol}</div>
</div>
<div style="padding:20px">
  ${isBuy ? `
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:16px">
    <div style="background:#0d1c30;border-radius:6px;padding:12px">
      <div style="font-size:9px;color:#4a7090;text-transform:uppercase">Entry Price</div>
      <div style="font-size:20px;font-weight:700;color:#b8d4ee">$${(+entryPrice).toFixed(2)}</div>
    </div>
    <div style="background:#0d1c30;border-radius:6px;padding:12px">
      <div style="font-size:9px;color:#4a7090;text-transform:uppercase">Shares</div>
      <div style="font-size:20px;font-weight:700;color:#b8d4ee">${shares}</div>
    </div>
  </div>
  <div style="background:#0d1c30;border-radius:6px;padding:12px">
    <div style="font-size:9px;color:#4a7090;text-transform:uppercase">AI Grade / Score</div>
    <div style="font-size:16px;font-weight:700;color:#f5a623">Grade ${grade||'--'} | Score ${score||'--'}/100</div>
  </div>` : `
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:16px">
    <div style="background:#0d1c30;border-radius:6px;padding:12px">
      <div style="font-size:9px;color:#4a7090;text-transform:uppercase">Entry</div>
      <div style="font-size:18px;font-weight:700;color:#b8d4ee">$${(+entryPrice).toFixed(2)}</div>
    </div>
    <div style="background:#0d1c30;border-radius:6px;padding:12px">
      <div style="font-size:9px;color:#4a7090;text-transform:uppercase">Exit</div>
      <div style="font-size:18px;font-weight:700;color:#b8d4ee">$${(+exitPrice).toFixed(2)}</div>
    </div>
  </div>
  <div style="background:#0d1c30;border-radius:6px;padding:14px;margin-bottom:16px;text-align:center">
    <div style="font-size:9px;color:#4a7090;text-transform:uppercase">P&L</div>
    <div style="font-size:28px;font-weight:700;color:${color}">${pnl>=0?'+':''}${(+pnl).toFixed(2)}%</div>
  </div>
  <div style="background:#0d1c30;border-radius:6px;padding:12px">
    <div style="font-size:9px;color:#4a7090;text-transform:uppercase">New Balance</div>
    <div style="font-size:18px;font-weight:700;color:#b8d4ee">$${(+balance).toFixed(2)}</div>
  </div>`}
  ${reason?`<div style="background:#0d1c30;border-radius:6px;padding:12px;margin-top:12px;font-size:11px;color:#4a7090"><strong style="color:#b8d4ee">Reason:</strong> ${reason}</div>`:''}
</div>
<div style="background:#050a0f;padding:12px 20px;font-size:9px;color:#243c52;text-align:center">
  AutoTrader AI • ${new Date().toLocaleString('en-US',{timeZone:'America/New_York'})} ET
</div>
</div></div></body></html>`;
}

async function sendTradeEmail(cfg, type, data) {
  const notifEmail = await getNotificationEmail(cfg);
  const subject = `AutoTrader — ${type} ${data.symbol}${data.pnl !== undefined ? ' ' + (data.pnl >= 0 ? '+' : '') + (+data.pnl).toFixed(2) + '%' : ''}`;
  const html = tradeEmailHtml({ type, ...data });
  if (notifEmail) await sendEmail(notifEmail, subject, html);
  if (type === 'LOSS') {
    const userEmail = notifEmail || cfg.user_id;
    await sendEmail(ADMIN_EMAIL, `[LOSS ALERT] ${data.symbol} — User: ${userEmail}`, html.replace('</div></div></body>', `<div style="background:#2a0a0a;border:1px solid #f0364a;border-radius:6px;padding:12px;margin:12px 20px;font-size:11px;color:#f0364a"><strong>User:</strong> ${userEmail}<br><strong>ID:</strong> ${cfg.user_id}</div></div></div></body>`));
  }
}

// ─── CLEARING & MARKET HOURS ─────────────────────────────────
// CLEARING_DAYS = 0 → disabled (testing)
// CLEARING_DAYS = 1 → T+1 (production)
const CLEARING_DAYS = 0;
function bizAdd(d, n) {
  let r = new Date(d), c = 0;
  while (c < n) { r.setDate(r.getDate() + 1); if (r.getDay() !== 0 && r.getDay() !== 6) c++; }
  return r;
}
const clearOk = lt => !lt || CLEARING_DAYS === 0 || new Date() >= bizAdd(new Date(lt), CLEARING_DAYS);
function isMarketOpen() {
  const et = new Date(new Date().toLocaleString('en-US', { timeZone: 'America/New_York' }));
  if (et.getDay() === 0 || et.getDay() === 6) return false;
  const m = et.getHours() * 60 + et.getMinutes();
  return m >= 570 && m < 960;
}
function countBizDays(s, e) {
  let c = 0, d = new Date(s);
  while (d < e) { d.setDate(d.getDate() + 1); if (d.getDay() !== 0 && d.getDay() !== 6) c++; }
  return c;
}

// ═══════════════════════════════════════════════════════════════
// STRIPE PAYMENTS
// ═══════════════════════════════════════════════════════════════
const STRIPE_SECRET         = process.env.STRIPE_SECRET_KEY;
const STRIPE_PRICE_ID       = process.env.STRIPE_PRICE_ID;
const STRIPE_WEBHOOK_SECRET = process.env.STRIPE_WEBHOOK_SECRET;
const STRIPE_API            = 'https://api.stripe.com/v1';

function stripeAuth() { return 'Basic ' + Buffer.from(`${STRIPE_SECRET}:`).toString('base64'); }
async function stripePost(endpoint, params) {
  const r = await fetch(`${STRIPE_API}${endpoint}`, {
    method: 'POST',
    headers: { Authorization: stripeAuth(), 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams(params)
  });
  return r.json();
}
async function stripeGet(endpoint) {
  const r = await fetch(`${STRIPE_API}${endpoint}`, { headers: { Authorization: stripeAuth() } });
  return r.json();
}

app.post('/stripe/checkout', async (req, res) => {
  const { email, userId } = req.body;
  if (!email) return res.json({ ok: false, error: 'Missing email' });
  if (!userId) return res.json({ ok: false, error: 'Missing userId' });
  if (!STRIPE_SECRET || !STRIPE_PRICE_ID) return res.json({ ok: false, error: 'Stripe not configured on server' });
  console.log('[STRIPE] Checkout — email:', email, 'userId:', userId);
  try {
    const customer = await stripePost('/customers', {
      email,
      'metadata[supabase_user_id]': userId
    });
    if (!customer.id) return res.json({ ok: false, error: 'Could not create customer: ' + (customer.error?.message || JSON.stringify(customer)) });
    const railwayUrl = `https://${process.env.RAILWAY_PUBLIC_DOMAIN || 'autotrader-production-5b5d.up.railway.app'}`;
    const session = await stripePost('/checkout/sessions', {
      mode:                                   'subscription',
      'payment_method_types[0]':              'card',
      'line_items[0][price]':                 STRIPE_PRICE_ID,
      'line_items[0][quantity]':              1,
      customer:                               customer.id,
      'metadata[user_id]':                    userId,
      success_url:                            `${railwayUrl}/stripe/success?session_id={CHECKOUT_SESSION_ID}`,
      cancel_url:                             `${railwayUrl}/stripe/cancel`,
      'subscription_data[metadata][user_id]': userId
    });
    if (!session.url) return res.json({ ok: false, error: 'Session missing URL: ' + (session.error?.message || JSON.stringify(session)) });
    res.json({ ok: true, url: session.url, sessionId: session.id });
  } catch (e) { res.json({ ok: false, error: e.message }); }
});

app.get('/stripe/success', async (req, res) => {
  const { session_id } = req.query;
  if (!session_id) return res.redirect('/');
  try {
    const session = await stripeGet(`/checkout/sessions/${session_id}?expand[]=subscription`);
    const userId  = session.metadata?.user_id;
    const subId   = session.subscription?.id || session.subscription;
    const custId  = session.customer;
    if (userId && subId) {
      await sbUpsert('config', {
        user_id:               userId,
        stripe_customer_id:    custId,
        stripe_subscription_id: subId,
        subscription_status:   'active',
        subscription_end:      null
      });
      await sbUpsert('bot_state', { user_id: userId });
      console.log('[STRIPE] Activated:', userId);
    }
    res.send(`<!DOCTYPE html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"><style>body{font-family:monospace;background:#050a0f;color:#b8d4ee;padding:30px;max-width:540px;margin:0 auto}.box{background:#0a2a1a;border:1px solid #00e891;border-radius:8px;padding:24px}h2{color:#00e891}p{line-height:1.8;margin-top:8px}.btn{display:block;background:#00e891;color:#050a0f;padding:14px;border-radius:6px;text-align:center;text-decoration:none;font-weight:700;font-size:15px;margin-top:18px}</style></head><body>
    <div class="box"><h2>✓ Payment Successful!</h2><p>Your AutoTrader subscription is now active.</p><p style="color:#4a7090">$20.00/month — renews automatically. Cancel anytime.</p><a class="btn" href="https://autotrader-ruby.vercel.app?activated=true">Open AutoTrader →</a></div>
    </body></html>`);
  } catch (e) { res.send(`<html><body style="font-family:monospace;background:#050a0f;color:#f0364a;padding:30px"><h2>Error</h2><p>${e.message}</p></body></html>`); }
});

app.get('/stripe/cancel', (req, res) => {
  res.send(`<!DOCTYPE html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"><style>body{font-family:monospace;background:#050a0f;color:#b8d4ee;padding:30px;max-width:540px;margin:0 auto}.box{background:#2a0a0a;border:1px solid #f0364a;border-radius:8px;padding:24px}h2{color:#f5a623}.btn{display:block;background:#091422;border:1px solid #162d47;color:#b8d4ee;padding:14px;border-radius:6px;text-align:center;text-decoration:none;font-weight:700;margin-top:16px}</style></head><body>
  <div class="box"><h2>Payment Cancelled</h2><p>No charge was made. You can try again whenever you're ready.</p><a class="btn" href="https://autotrader-ruby.vercel.app">Return to AutoTrader</a></div>
  </body></html>`);
});

app.post('/stripe/webhook', express.raw({ type: 'application/json' }), async (req, res) => {
  const sig = req.headers['stripe-signature'];
  if (!sig || !STRIPE_WEBHOOK_SECRET) return res.status(400).json({ error: 'No signature' });
  let event;
  try {
    const payload   = req.body.toString();
    const parts     = sig.split(',');
    const timestamp = parts.find(p => p.startsWith('t=')).split('=')[1];
    const signature = parts.find(p => p.startsWith('v1=')).split('=')[1];
    const signed    = crypto.createHmac('sha256', STRIPE_WEBHOOK_SECRET).update(`${timestamp}.${payload}`).digest('hex');
    if (signed !== signature) return res.status(400).json({ error: 'Invalid signature' });
    event = JSON.parse(payload);
  } catch (e) { return res.status(400).json({ error: e.message }); }
  const sub    = event.data?.object;
  const userId = sub?.metadata?.user_id;
  try {
    switch (event.type) {
      case 'invoice.payment_succeeded':
        if (userId) {
          await sbUpsert('config', { user_id: userId, stripe_customer_id: sub.customer, stripe_subscription_id: sub.subscription, subscription_status: 'active', subscription_end: null });
          console.log('[STRIPE] Renewed:', userId);
        }
        break;
      case 'invoice.payment_failed':
        if (userId) {
          await sbPatch('config', { subscription_status: 'past_due' }, `?user_id=eq.${userId}`);
          const rows = await sbGet('config', `?user_id=eq.${userId}`);
          if (rows?.[0]) await sendPush(rows[0], 'AutoTrader — Payment Failed', 'Your $20/mo payment failed. Update your card to keep the bot running.');
        }
        break;
      case 'customer.subscription.deleted':
        if (userId) {
          await sbPatch('config', { subscription_status: 'canceled', subscription_end: new Date().toISOString() }, `?user_id=eq.${userId}`);
          await sbPatch('bot_state', { running: false, status_text: 'Subscription cancelled' }, `?user_id=eq.${userId}`);
        }
        break;
      case 'customer.subscription.updated':
        if (userId) await sbPatch('config', { subscription_status: sub.status === 'active' ? 'active' : sub.status }, `?user_id=eq.${userId}`);
        break;
    }
  } catch (e) { console.log('[STRIPE] Webhook error:', e.message); }
  res.json({ received: true });
});

function checkSubscription(cfg) {
  if (!STRIPE_SECRET) return true;
  return cfg.subscription_status === 'active' || cfg.subscription_status === 'past_due';
}

// ═══════════════════════════════════════════════════════════════
// SCHWAB API — Per-user credentials (Path A)
// ═══════════════════════════════════════════════════════════════
const SCHWAB_MARKET    = 'https://api.schwabapi.com/marketdata/v1';
const SCHWAB_TRADER    = 'https://api.schwabapi.com/trader/v1';
const SCHWAB_TOKEN_URL = 'https://api.schwabapi.com/v1/oauth/token';
const SCHWAB_AUTH_URL  = 'https://api.schwabapi.com/v1/oauth/authorize';

function schwabBasicAuth(cfg) {
  return 'Basic ' + Buffer.from(`${cfg.schwab_client_id}:${cfg.schwab_client_secret}`).toString('base64');
}

async function refreshSchwabToken(cfg) {
  if (!cfg.schwab_refresh_token || !cfg.schwab_client_id) return null;
  try {
    const r = await fetch(SCHWAB_TOKEN_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded', Authorization: schwabBasicAuth(cfg) },
      body: new URLSearchParams({ grant_type: 'refresh_token', refresh_token: cfg.schwab_refresh_token })
    });
    const j = await r.json();
    if (!j.access_token) return null;
    await patchUserConfig(cfg.user_id, {
      schwab_access_token:  j.access_token,
      schwab_refresh_token: j.refresh_token || cfg.schwab_refresh_token,
      schwab_token_expiry:  new Date(Date.now() + (j.expires_in || 1800) * 1000).toISOString()
    });
    return j.access_token;
  } catch (e) { return null; }
}

async function getSchwabToken(cfg) {
  if (!cfg.schwab_access_token) return null;
  const expiry = cfg.schwab_token_expiry ? new Date(cfg.schwab_token_expiry) : null;
  if (!expiry || expiry - new Date() < 5 * 60 * 1000) {
    const newToken = await refreshSchwabToken(cfg);
    if (!newToken) {
      await addUserLog(cfg.user_id, 'warn', 'Schwab token expired — user needs to re-authorize');
      await sendPush(cfg, 'AutoTrader — Schwab Re-auth Needed', 'Tap to reconnect your Schwab account.', `https://${process.env.RAILWAY_PUBLIC_DOMAIN || ''}/schwab/auth`);
      return null;
    }
    return newToken;
  }
  return cfg.schwab_access_token;
}

async function schwabFetchCandles(sym, interval, lookback, token) {
  const periodMap = {
    '1min':  { frequencyType: 'minute', frequency: 1,  periodType: 'day',   period: 1 },
    '5min':  { frequencyType: 'minute', frequency: 5,  periodType: 'day',   period: 1 },
    '15min': { frequencyType: 'minute', frequency: 15, periodType: 'day',   period: 1 },
    '1day':  { frequencyType: 'daily',  frequency: 1,  periodType: 'month', period: 1 }
  };
  const p = periodMap[interval] || periodMap['5min'];
  const daysNeeded = interval === '1day' ? Math.ceil(lookback / 5) + 5 : 3;
  const startTime  = Date.now() - daysNeeded * 24 * 60 * 60 * 1000;
  const params = new URLSearchParams({ symbol: sym, periodType: p.periodType, period: p.period, frequencyType: p.frequencyType, frequency: p.frequency, startDate: startTime, endDate: Date.now(), needExtendedHoursData: false });
  const r = await fetch(`${SCHWAB_MARKET}/pricehistory?${params}`, { headers: { Authorization: `Bearer ${token}` } });
  const j = await r.json();
  if (!j.candles?.length) throw new Error(`Schwab: no candles for ${sym}`);
  return j.candles.slice(-lookback).map(c => ({ date: new Date(c.datetime).toISOString(), open: c.open, high: c.high, low: c.low, close: c.close, volume: c.volume }));
}

async function schwabFetchQuote(sym, token) {
  try {
    const r = await fetch(`${SCHWAB_MARKET}/quotes/${sym}`, { headers: { Authorization: `Bearer ${token}` } });
    const j = await r.json();
    const q = j[sym]?.quote || j[sym];
    if (!q) return null;
    return { price: q.lastPrice || q.mark || 0, changePct: q.netPercentChangeInDouble || 0, volume: q.totalVolume || 0 };
  } catch (e) { return null; }
}

async function schwabGetBalance(token, accountHash) {
  try {
    const r = await fetch(`${SCHWAB_TRADER}/accounts/${accountHash}?fields=balances`, { headers: { Authorization: `Bearer ${token}` } });
    const j = await r.json();
    return j?.securitiesAccount?.currentBalances?.cashAvailableForTrading || j?.securitiesAccount?.currentBalances?.availableFunds || null;
  } catch (e) { return null; }
}

async function schwabPlaceOrder(sym, action, shares, price, token, accountHash, uid) {
  const body = { orderType: 'MARKET', session: 'NORMAL', duration: 'DAY', orderStrategyType: 'SINGLE', orderLegCollection: [{ instruction: action, quantity: shares, instrument: { symbol: sym, assetType: 'EQUITY' } }] };
  try {
    const r = await fetch(`${SCHWAB_TRADER}/accounts/${accountHash}/orders`, { method: 'POST', headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    if (r.status === 201) { const orderId = r.headers.get('location')?.split('/').pop() || 'unknown'; await addUserLog(uid, 'buy', `Schwab order placed — orderId: ${orderId}`); return { success: true, orderId }; }
    const err = await r.text(); await addUserLog(uid, 'err', `Schwab order failed (${r.status}): ${err}`); return { success: false };
  } catch (e) { await addUserLog(uid, 'err', 'Schwab order error: ' + e.message); return { success: false }; }
}

// Schwab OAuth routes
app.get('/schwab/auth', async (req, res) => {
  const userId = req.query.user_id;
  if (!userId) return res.send('<html><body style="font-family:monospace;background:#050a0f;color:#f0364a;padding:30px"><h2>Missing user_id parameter</h2></body></html>');
  const cfgArr = await sbGet('config', `?user_id=eq.${userId}`);
  const cfg = cfgArr?.[0];
  if (!cfg?.schwab_client_id || !cfg?.schwab_client_secret) return res.send(`<html><body style="font-family:monospace;background:#050a0f;color:#b8d4ee;padding:30px"><h2 style="color:#f5a623">Add Schwab Client ID and Secret in Config first</h2></body></html>`);
  const redirectUri = `https://${process.env.RAILWAY_PUBLIC_DOMAIN || req.hostname}/schwab/callback`;
  const authUrl = `${SCHWAB_AUTH_URL}?client_id=${cfg.schwab_client_id}&redirect_uri=${encodeURIComponent(redirectUri)}&state=${userId}`;
  res.send(`<!DOCTYPE html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"><style>body{font-family:monospace;background:#050a0f;color:#b8d4ee;padding:24px;max-width:540px;margin:0 auto}.btn{display:block;background:#00e891;color:#050a0f;padding:16px;border-radius:6px;text-align:center;text-decoration:none;font-weight:700;font-size:16px;margin-top:14px}</style></head><body>
  <h2 style="color:#f5a623">Connect Charles Schwab</h2>
  <p style="color:#4a7090;line-height:1.8;margin:12px 0">Tap below to authorize AutoTrader on your Schwab account. Tokens refresh automatically — only needs to be done once.</p>
  <a class="btn" href="${authUrl}">Connect to Charles Schwab →</a>
  </body></html>`);
});

app.get('/schwab/callback', async (req, res) => {
  const { code, error, state: userId } = req.query;
  if (error || !code || !userId) return res.send(`<html><body style="font-family:monospace;background:#050a0f;color:#f0364a;padding:30px"><h2>Authorization failed</h2><p>${error || 'Missing code or user ID'}</p></body></html>`);
  const cfgArr = await sbGet('config', `?user_id=eq.${userId}`);
  const cfg = cfgArr?.[0];
  if (!cfg) return res.send('<html><body style="font-family:monospace;background:#050a0f;color:#f0364a;padding:30px"><h2>User not found</h2></body></html>');
  const redirectUri = `https://${process.env.RAILWAY_PUBLIC_DOMAIN || req.hostname}/schwab/callback`;
  try {
    const r = await fetch(SCHWAB_TOKEN_URL, { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded', Authorization: schwabBasicAuth(cfg) }, body: new URLSearchParams({ grant_type: 'authorization_code', code, redirect_uri: redirectUri }) });
    const j = await r.json();
    if (!j.access_token) return res.send(`<html><body style="font-family:monospace;background:#050a0f;color:#f0364a;padding:30px"><h2>Token exchange failed</h2><pre>${JSON.stringify(j,null,2)}</pre></body></html>`);
    await patchUserConfig(userId, { schwab_access_token: j.access_token, schwab_refresh_token: j.refresh_token, schwab_token_expiry: new Date(Date.now() + (j.expires_in || 1800) * 1000).toISOString(), broker: 'schwab' });
    let accountHash = '';
    try {
      const ar = await fetch(`${SCHWAB_TRADER}/accounts`, { headers: { Authorization: `Bearer ${j.access_token}` } });
      const accounts = await ar.json();
      accountHash = accounts?.[0]?.hashValue || '';
      if (accountHash) await patchUserConfig(userId, { schwab_account_hash: accountHash });
    } catch (e) {}
    await addUserLog(userId, 'info', `Schwab connected — account hash: ${accountHash}`);
    await patchUserState(userId, { status_text: 'Schwab connected — bot ready' });
    const freshCfgArr = await sbGet('config', `?user_id=eq.${userId}`);
    if (freshCfgArr?.[0]) await sendPush(freshCfgArr[0], 'AutoTrader — Schwab Connected!', 'Your Schwab account is now linked.');
    res.send(`<!DOCTYPE html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"><style>body{font-family:monospace;background:#050a0f;color:#b8d4ee;padding:30px;max-width:540px;margin:0 auto}.box{background:#0a2a1a;border:1px solid #00e891;border-radius:8px;padding:24px}h2{color:#00e891}</style></head><body><div class="box"><h2>✓ Schwab Connected!</h2><p>Your account has been linked. Tokens refresh automatically.</p><p style="margin-top:12px;color:#f5a623">Close this page and return to the AutoTrader app.</p></div></body></html>`);
  } catch (e) { res.send(`<html><body style="font-family:monospace;background:#050a0f;color:#f0364a;padding:30px"><h2>Error</h2><pre>${e.message}</pre></body></html>`); }
});

// ═══════════════════════════════════════════════════════════════
// E*TRADE — OAuth 1.0a (legacy)
// ═══════════════════════════════════════════════════════════════
function oauthSign(method,url,params,cs,ts=''){const s=Object.keys(params).sort().map(k=>`${encodeURIComponent(k)}=${encodeURIComponent(params[k])}`).join('&');const b=`${method}&${encodeURIComponent(url)}&${encodeURIComponent(s)}`;const sk=`${encodeURIComponent(cs)}&${encodeURIComponent(ts)}`;return crypto.createHmac('sha1',sk).update(b).digest('base64');}
function makeOAuthHeader(method,url,cfg,extra={}){const p={oauth_consumer_key:cfg.e_key,oauth_nonce:crypto.randomBytes(16).toString('hex'),oauth_signature_method:'HMAC-SHA1',oauth_timestamp:Math.floor(Date.now()/1000).toString(),oauth_version:'1.0',...extra};if(cfg.e_token&&!extra.oauth_callback)p.oauth_token=cfg.e_token;p.oauth_signature=oauthSign(method,url,p,cfg.e_secret,cfg.e_token_secret||'');return 'OAuth '+Object.keys(p).sort().map(k=>`${encodeURIComponent(k)}="${encodeURIComponent(p[k])}"`).join(', ');}
function parseQS(text){const r={};text.split('&').forEach(pair=>{const[k,v]=pair.split('=');if(k)r[decodeURIComponent(k)]=decodeURIComponent(v||'');});return r;}

// E*Trade token renewal per user
const etradeRenewals = new Map(); // user_id → last renewal time
async function checkAndRenewEtrade(cfg) {
  if (!cfg.e_token || cfg.sandbox) return;
  const last = etradeRenewals.get(cfg.user_id);
  const now = new Date();
  if (last && (now - last) < 90 * 60 * 1000) return;
  const base = cfg.sandbox ? 'https://apisb.etrade.com' : 'https://api.etrade.com';
  try {
    const r = await fetch(`${base}/oauth/renew_access_token`, { headers: { Authorization: makeOAuthHeader('GET', `${base}/oauth/renew_access_token`, cfg) }, credentials: 'omit' });
    const text = await r.text();
    if (r.ok && text.includes('renewed')) { etradeRenewals.set(cfg.user_id, now); await addUserLog(cfg.user_id, 'info', 'E*Trade token renewed'); }
  } catch (e) {}
}

app.get('/etrade/auth', async (req, res) => {
  const userId = req.query.user_id;
  if (!userId) return res.send('<html><body style="font-family:monospace;background:#050a0f;color:#f0364a;padding:30px"><h2>Missing user_id</h2></body></html>');
  const cfgArr = await sbGet('config', `?user_id=eq.${userId}`);
  const cfg = cfgArr?.[0];
  if (!cfg?.e_key || !cfg?.e_secret) return res.send('<html><body style="font-family:monospace;background:#050a0f;color:#f5a623;padding:30px"><h2>Add E*Trade Consumer Key and Secret in Config first</h2></body></html>');
  const sandbox = cfg.sandbox;
  const apiBase = sandbox ? 'https://apisb.etrade.com' : 'https://api.etrade.com';
  try {
    const tmpCfg = { ...cfg, e_token: '', e_token_secret: '' };
    const r = await fetch(`${apiBase}/oauth/request_token`, { headers: { Authorization: makeOAuthHeader('GET', `${apiBase}/oauth/request_token`, tmpCfg, { oauth_callback: 'oob' }) }, credentials: 'omit' });
    const text = await r.text(); const p = parseQS(text);
    if (!p.oauth_token) return res.send(`<html><body style="font-family:monospace;background:#050a0f;color:#f0364a;padding:30px"><h2>Error</h2><pre>${text}</pre></body></html>`);
    const authUrl = `https://us.etrade.com/e/t/etws/authorize?key=${cfg.e_key}&token=${p.oauth_token}`;
    res.send(`<!DOCTYPE html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"><style>body{font-family:monospace;background:#050a0f;color:#b8d4ee;padding:20px;max-width:540px;margin:0 auto}h2{color:#f5a623}.box{background:#091422;border:1px solid #162d47;border-radius:6px;padding:16px;margin:12px 0}.btn-link{display:block;background:#162d47;border:1px solid #2d9cff;color:#2d9cff;padding:14px;border-radius:5px;text-align:center;text-decoration:none;margin-top:10px;font-weight:700}input{width:100%;background:#0d1c30;border:1px solid #1c3a59;border-radius:4px;padding:12px;color:#b8d4ee;font-family:monospace;font-size:18px;box-sizing:border-box;margin:8px 0;letter-spacing:.1em;text-transform:uppercase}button{background:#00e891;color:#050a0f;border:none;border-radius:5px;padding:14px;font-family:monospace;font-size:14px;font-weight:700;cursor:pointer;width:100%;margin-top:8px}#msg{margin-top:14px;padding:14px;border-radius:5px;display:none}</style></head><body>
    <h2>E*Trade Authorization</h2>
    <div class="box"><p>Log in to E*Trade and copy the PIN.</p><a class="btn-link" href="${authUrl}" target="_blank">Authorize on E*Trade →</a></div>
    <div class="box"><input type="text" id="pin" placeholder="ENTER PIN" inputmode="text" maxlength="10" autocomplete="off" autocorrect="off" spellcheck="false"/><button onclick="go()">Complete Authorization</button></div>
    <div id="msg"></div>
    <script>document.getElementById('pin').focus();async function go(){const pin=document.getElementById('pin').value.trim().toUpperCase();if(!pin){alert('Enter PIN');return;}document.getElementById('msg').style.cssText='display:block;background:#162d47;padding:14px;border-radius:5px;color:#f5a623';document.getElementById('msg').innerHTML='Saving...';try{const r=await fetch('/etrade/callback?rt=${p.oauth_token}&user_id=${userId}&pin='+encodeURIComponent(pin),{credentials:'omit'});const j=await r.json();const el=document.getElementById('msg');if(j.ok){el.style.cssText='display:block;background:#0a2a1a;border:1px solid #00e891;padding:16px;border-radius:5px';el.innerHTML='<p style="color:#00e891;margin:0">Done! Close this page.</p>';}else{el.style.cssText='display:block;background:#2a0a0a;border:1px solid #f0364a;padding:16px;border-radius:5px';el.innerHTML='<p style="color:#f0364a;margin:0">'+j.error+'</p>';}}catch(e){document.getElementById('msg').innerHTML='Error';}}document.getElementById('pin').addEventListener('keydown',e=>{if(e.key==='Enter')go();});</script>
    </body></html>`);
  } catch (e) { res.send(`<html><body style="font-family:monospace;background:#050a0f;color:#f0364a;padding:30px"><h2>Error</h2><pre>${e.message}</pre></body></html>`); }
});

app.get('/etrade/callback', async (req, res) => {
  const { rt, pin, user_id: userId } = req.query;
  if (!rt || !pin || !userId) return res.json({ ok: false, error: 'Missing token, PIN, or user_id' });
  const cfgArr = await sbGet('config', `?user_id=eq.${userId}`);
  const cfg = cfgArr?.[0];
  if (!cfg) return res.json({ ok: false, error: 'User not found' });
  const apiBase = cfg.sandbox ? 'https://apisb.etrade.com' : 'https://api.etrade.com';
  try {
    const tmpCfg = { ...cfg, e_token: rt, e_token_secret: '' };
    const r = await fetch(`${apiBase}/oauth/access_token`, { headers: { Authorization: makeOAuthHeader('GET', `${apiBase}/oauth/access_token`, tmpCfg, { oauth_verifier: pin.toUpperCase() }) }, credentials: 'omit' });
    const text = await r.text(); const p = parseQS(text);
    if (!p.oauth_token) return res.json({ ok: false, error: `PIN rejected (HTTP ${r.status})` });
    await patchUserConfig(userId, { e_token: p.oauth_token, e_token_secret: p.oauth_token_secret });
    etradeRenewals.set(userId, new Date());
    await addUserLog(userId, 'info', 'E*Trade authorized');
    await patchUserState(userId, { status_text: 'E*Trade authorized — bot ready' });
    const freshCfgArr = await sbGet('config', `?user_id=eq.${userId}`);
    if (freshCfgArr?.[0]) await sendPush(freshCfgArr[0], 'AutoTrader — E*Trade Authorized', 'E*Trade connected.');
    res.json({ ok: true });
  } catch (e) { res.json({ ok: false, error: e.message }); }
});

// ═══════════════════════════════════════════════════════════════
// MARKET DATA
// Priority: Schwab (user's own API) → Yahoo Finance (free, no key)
// Yahoo Finance replaces Twelve Data for all E*Trade/sandbox users
// Switch to Schwab when dev account approved — zero cost at scale
// ═══════════════════════════════════════════════════════════════

// Map our interval to Yahoo Finance interval format
function toYahooInterval(interval) {
  const map = { '1min': '1m', '5min': '5m', '15min': '15m', '1day': '1d' };
  return map[interval] || '5m';
}

// Fetch candles from Yahoo Finance — free, no API key, no rate limits
async function yahooFetchCandles(sym, interval, lookback) {
  const yhInterval = toYahooInterval(interval);
  // For intraday use 5d range, for daily use 3mo
  const range = interval === '1day' ? '3mo' : '5d';
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${sym}?interval=${yhInterval}&range=${range}&includePrePost=false`;
  
  const r = await fetch(url, {
    headers: { 'User-Agent': 'Mozilla/5.0' }
  });
  const j = await r.json();
  
  const result = j?.chart?.result?.[0];
  if (!result) throw new Error(`Yahoo: no data for ${sym}`);
  
  const timestamps = result.timestamp;
  const q = result.indicators?.quote?.[0];
  if (!timestamps || !q) throw new Error(`Yahoo: empty data for ${sym}`);
  
  const candles = timestamps.map((ts, i) => ({
    date:   new Date(ts * 1000).toISOString(),
    open:   q.open?.[i]   || 0,
    high:   q.high?.[i]   || 0,
    low:    q.low?.[i]    || 0,
    close:  q.close?.[i]  || 0,
    volume: q.volume?.[i] || 0
  })).filter(c => c.close > 0); // filter null candles

  if (!candles.length) throw new Error(`Yahoo: no valid candles for ${sym}`);
  return candles.slice(-lookback);
}

// Fetch live quote from Yahoo Finance
async function yahooFetchQuote(sym) {
  try {
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${sym}?interval=1m&range=1d&includePrePost=false`;
    const r = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
    const j = await r.json();
    const result = j?.chart?.result?.[0];
    const meta   = result?.meta;
    if (!meta) return null;
    return {
      price:     meta.regularMarketPrice || meta.previousClose || 0,
      changePct: meta.regularMarketChangePercent || 0,
      volume:    meta.regularMarketVolume || 0
    };
  } catch (e) { return null; }
}

async function fetchCandles(sym, cfg, token) {
  // Use Schwab if connected
  if (cfg.broker === 'schwab' && token) {
    return schwabFetchCandles(sym, cfg.interval, cfg.lookback || 90, token);
  }
  // Yahoo Finance for all other users — free, no key needed
  return yahooFetchCandles(sym, cfg.interval, cfg.lookback || 90);
}

async function fetchQuote(sym, cfg, token) {
  try {
    if (cfg.broker === 'schwab' && token) return schwabFetchQuote(sym, token);
    return yahooFetchQuote(sym);
  } catch (e) { return null; }
}

// ═══════════════════════════════════════════════════════════════
// ORDER PLACEMENT (Schwab or E*Trade per user)
// ═══════════════════════════════════════════════════════════════
async function getAvailableBalance(cfg, token, dbBalance) {
  if (cfg.sandbox) return dbBalance;
  if (cfg.broker === 'schwab' && cfg.schwab_account_hash && token) {
    const real = await schwabGetBalance(token, cfg.schwab_account_hash);
    if (real !== null) { await addUserLog(cfg.user_id, 'info', `Real Schwab balance: $${(+real).toFixed(2)}`); return +real; }
  }
  return dbBalance;
}

async function placeOrder(sym, action, shares, price, cfg, token) {
  if (cfg.sandbox) {
    await addUserLog(cfg.user_id, 'skip', `[SANDBOX] ${action} ${shares}sh ${sym} @ $${price.toFixed(2)}`);
    await sendPush(cfg, `AutoTrader — [SB] ${action}`, `${action} ${shares}sh ${sym} @ $${price.toFixed(2)}`);
    return { success: true, sandboxed: true };
  }
  if (cfg.broker === 'schwab') {
    if (!token || !cfg.schwab_account_hash) { await addUserLog(cfg.user_id, 'err', 'Schwab not authorized'); return { success: false }; }
    const res = await schwabPlaceOrder(sym, action, shares, price, token, cfg.schwab_account_hash, cfg.user_id);
    if (res.success) await sendPush(cfg, `AutoTrader — ${action} Executed`, `${action} ${shares}sh ${sym} @ ~$${price.toFixed(2)}`);
    return res;
  }
  if (!cfg.e_key || !cfg.e_token || !cfg.e_account) { await addUserLog(cfg.user_id, 'err', 'E*Trade tokens missing'); return { success: false }; }
  const url = `https://api.etrade.com/v1/accounts/${cfg.e_account}/orders/place`;
  const body = { PlaceOrderRequest: { orderType:'EQ', clientOrderId:`BOT-${Date.now()}`, Order:[{Instrument:[{Product:{securityType:'EQ',symbol:sym},orderAction:action,quantityType:'QUANTITY',quantity:shares}],orderTerm:'GOOD_FOR_DAY',marketSession:'REGULAR',priceType:'MARKET'}] } };
  try {
    const r = await fetch(url, { method:'POST', credentials:'omit', headers:{Authorization:makeOAuthHeader('POST',url,cfg),'Content-Type':'application/json'}, body:JSON.stringify(body) });
    const j = await r.json();
    if (r.status === 401) { await sendPush(cfg,'AutoTrader — Trade Failed',`${action} failed — session expired.`); return { success:false }; }
    const orderId = j?.PlaceOrderResponse?.OrderIds?.[0]?.orderId;
    if (orderId) { await addUserLog(cfg.user_id,'buy',`E*Trade order: ${orderId}`); await sendPush(cfg,`AutoTrader — ${action}`,`${action} ${shares}sh ${sym} @ $${price.toFixed(2)}`); return { success:true, orderId }; }
    await addUserLog(cfg.user_id,'err',`E*Trade failed: ${JSON.stringify(j)}`); return { success:false };
  } catch (e) { await addUserLog(cfg.user_id,'err','E*Trade error: '+e.message); return { success:false }; }
}

// ─── INDICATOR MATH ──────────────────────────────────────────
const sma=(a,p)=>a.map((_,i)=>i<p-1?null:a.slice(i-p+1,i+1).reduce((s,v)=>s+v,0)/p);
function ema(a,p){const k=2/(p+1),r=new Array(a.length).fill(null);r[p-1]=a.slice(0,p).reduce((s,v)=>s+v,0)/p;for(let i=p;i<a.length;i++)r[i]=a[i]*k+r[i-1]*(1-k);return r;}
function rsi(c,p=14){const r=new Array(c.length).fill(null);let g=0,l=0;for(let i=1;i<=p;i++){const d=c[i]-c[i-1];d>0?g+=d:l-=d;}let ag=g/p,al=l/p;r[p]=100-100/(1+(al===0?1e10:ag/al));for(let i=p+1;i<c.length;i++){const d=c[i]-c[i-1];ag=(ag*(p-1)+(d>0?d:0))/p;al=(al*(p-1)+(d<0?-d:0))/p;r[i]=100-100/(1+(al===0?1e10:ag/al));}return r;}
function macd(c,f=12,s=26,sg=9){const ef=ema(c,f),es=ema(c,s);const ml=c.map((_,i)=>ef[i]!=null&&es[i]!=null?ef[i]-es[i]:null);const vals=ml.filter(v=>v!=null),off=ml.findIndex(v=>v!=null);const se=ema(vals,sg);const sl=new Array(ml.length).fill(null),hl=new Array(ml.length).fill(null);for(let i=0;i<se.length;i++){const x=off+i;if(se[i]!=null){sl[x]=se[i];hl[x]=ml[x]-se[i];}}return{ml,sl,hl};}
function bbands(c,p=20,m=2){const mid=sma(c,p),up=[],lo=[];for(let i=0;i<c.length;i++){if(mid[i]==null){up.push(null);lo.push(null);continue;}const sl=c.slice(i-p+1,i+1),mv=mid[i];const std=Math.sqrt(sl.reduce((a,v)=>a+(v-mv)**2,0)/p);up.push(mv+m*std);lo.push(mv-m*std);}return{up,mid,lo};}
function calcVwap(data){let cv=0,cq=0;return data.map(c=>{const tp=(c.high+c.low+c.close)/3;cv+=tp*c.volume;cq+=c.volume;return cq>0?cv/cq:tp;});}
function rmsVol(c,p=20){const r=new Array(c.length).fill(null);for(let i=p;i<c.length;i++){const rets=[];for(let j=i-p+1;j<=i;j++)rets.push((c[j]-c[j-1])/c[j-1]);r[i]=Math.sqrt(rets.reduce((s,v)=>s+v*v,0)/rets.length)*100;}return r;}
function calcATR(data,p=14){const tr=data.map((c,i)=>i===0?c.high-c.low:Math.max(c.high-c.low,Math.abs(c.high-data[i-1].close),Math.abs(c.low-data[i-1].close)));return sma(tr,p);}
function calcSlope(c,p=20){if(c.length<p)return 0;const s=c.slice(-p),n=s.length,sx=n*(n-1)/2,sx2=n*(n-1)*(2*n-1)/6,sy=s.reduce((a,v)=>a+v,0),sxy=s.reduce((a,v,i)=>a+i*v,0);return(n*sxy-sx*sy)/(n*sx2-sx*sx);}
function momentum(c,p=10){const n=c.length-1;return n<p?0:(c[n]-c[n-p])/c[n-p]*100;}

function analyze(data, liveVwap, coefs) {
  const closes=data.map(d=>d.close),n=data.length-1,px=data[n].close;
  const rv=rsi(closes,14),md=macd(closes,12,26,9),bb=bbands(closes,20,2);
  const sm20=sma(closes,20),sm50=sma(closes,50),e12=ema(closes,12),e26=ema(closes,26);
  const vwArr=calcVwap(data),rms=rmsVol(closes,20),at=calcATR(data,14);
  const rsiV=rv[n],mlV=md.ml[n],slV=md.sl[n],hlV=md.hl[n],hlP=md.hl[n-1];
  const buV=bb.up[n],bmV=bb.mid[n],blV=bb.lo[n];
  const bbPct=buV&&blV?(px-blV)/(buV-blV)*100:50;
  const vwV=liveVwap||vwArr[n],s20=sm20[n],s50=sm50[n],e1=e12[n],e2=e26[n];
  const sigs={};
  if(rsiV<30)sigs.RSI={score:86,label:'Oversold',bull:true};else if(rsiV<42)sigs.RSI={score:68,label:'Bullish zone',bull:true};else if(rsiV>70)sigs.RSI={score:14,label:'Overbought',bull:false};else if(rsiV>58)sigs.RSI={score:34,label:'Bearish zone',bull:false};else sigs.RSI={score:50,label:'Neutral',bull:null};
  if(mlV!=null&&slV!=null){if(hlV>0&&hlP<=0)sigs.MACD={score:91,label:'Bull cross',bull:true};else if(hlV<0&&hlP>=0)sigs.MACD={score:9,label:'Bear cross',bull:false};else if(mlV>slV&&mlV>0)sigs.MACD={score:72,label:'Bullish',bull:true};else if(mlV<slV&&mlV<0)sigs.MACD={score:28,label:'Bearish',bull:false};else sigs.MACD={score:50,label:'Mixed',bull:null};}else sigs.MACD={score:50,label:'No data',bull:null};
  if(blV!=null){if(px<=blV*1.005)sigs.BB={score:84,label:'At lower band',bull:true};else if(px>=buV*0.995)sigs.BB={score:16,label:'At upper band',bull:false};else if(bbPct<35)sigs.BB={score:64,label:'Lower half',bull:true};else if(bbPct>65)sigs.BB={score:37,label:'Upper half',bull:false};else sigs.BB={score:50,label:'Mid band',bull:null};}else sigs.BB={score:50,label:'Calculating',bull:null};
  const vwD=vwV?(px-vwV)/vwV*100:0;
  if(px>vwV*1.005)sigs.VWAP={score:66,label:`+${vwD.toFixed(2)}% above`,bull:true};else if(px<vwV*0.995)sigs.VWAP={score:35,label:`${vwD.toFixed(2)}% below`,bull:false};else sigs.VWAP={score:50,label:'At VWAP',bull:null};
  if(s20&&s50){if(px>s20&&s20>s50)sigs.SMA={score:76,label:'Full uptrend',bull:true};else if(px<s20&&s20<s50)sigs.SMA={score:24,label:'Full downtrend',bull:false};else if(px>s20&&s20<s50)sigs.SMA={score:58,label:'Recovery',bull:true};else sigs.SMA={score:42,label:'Below SMA20',bull:false};}else sigs.SMA={score:50,label:'Calculating',bull:null};
  if(e1&&e2){if(e1>e2&&px>e1)sigs.EMA={score:74,label:'Bull momentum',bull:true};else if(e1<e2&&px<e1)sigs.EMA={score:26,label:'Bear momentum',bull:false};else if(e1>e2)sigs.EMA={score:62,label:'Bullish cross',bull:true};else sigs.EMA={score:38,label:'Bearish cross',bull:false};}else sigs.EMA={score:50,label:'Calculating',bull:null};
  const totW=Object.keys(sigs).reduce((s,k)=>s+(coefs[k]||1),0);
  const comp=+(Object.entries(sigs).reduce((s,[k,v])=>s+v.score*(coefs[k]||1),0)/totW).toFixed(1);
  return{sigs,comp,px,vwap:vwV,rsiV,mlV,slV,hlV,hlP,buV,bmV,blV,bbPct:+bbPct.toFixed(1),s20,s50,e1,e2,rms:rms[n],atr:at[n],slp:calcSlope(closes,20),mom:momentum(closes,10)};
}

// ─── CLAUDE GRADER ───────────────────────────────────────────
async function aiGrade(sym,ind,antKey){
  if(!antKey)return null;
  const prompt=`You are a quant signal grader for an autonomous trading bot. Return ONLY valid JSON.
TICKER:${sym} PRICE:$${ind.px.toFixed(2)} COMPOSITE:${ind.comp}/100
RSI=${ind.rsiV?.toFixed(2)} [${ind.sigs.RSI?.label}] MACD=${ind.mlV?.toFixed(4)} SIG=${ind.slV?.toFixed(4)} HIST=${ind.hlV?.toFixed(4)} [${ind.sigs.MACD?.label}]
BB=$${ind.buV?.toFixed(2)}/$${ind.bmV?.toFixed(2)}/$${ind.blV?.toFixed(2)} PCT=${ind.bbPct}% [${ind.sigs.BB?.label}]
VWAP=$${ind.vwap?.toFixed(2)} [${ind.sigs.VWAP?.label}] SMA20=$${ind.s20?.toFixed(2)} SMA50=$${ind.s50?.toFixed(2)} [${ind.sigs.SMA?.label}]
EMA12=$${ind.e1?.toFixed(2)} EMA26=$${ind.e2?.toFixed(2)} [${ind.sigs.EMA?.label}]
STRATEGY: target +1% per trade. Two-phase stop: fixed 1.2% below entry then trailing 0.5% after +1% hit.
Return ONLY: {"grade":"B","score":72,"action":"BUY","confidence":70,"reason":"one sentence","target":${(ind.px*1.012).toFixed(2)},"stop":${(ind.px*.988).toFixed(2)},"strongest":"MACD","avoid":false}`;
  try{const r=await fetch('https://api.anthropic.com/v1/messages',{method:'POST',headers:{'Content-Type':'application/json','x-api-key':antKey,'anthropic-version':'2023-06-01'},body:JSON.stringify({model:'claude-sonnet-4-20250514',max_tokens:200,messages:[{role:'user',content:prompt}]})});if(!r.ok)return null;const d=await r.json();return JSON.parse(d.content.map(c=>c.text||'').join('').replace(/```json|```/g,'').trim());}catch(e){return null;}
}

// ─── COEFFICIENTS ────────────────────────────────────────────
function updateCoefs(coefs,indSnap,result){const clamp=v=>+Math.max(0.05,Math.min(3,v)).toFixed(3);const win=result==='WIN';const out={...coefs};Object.keys(out).forEach(k=>{const s=indSnap?.[k];if(!s)return;const correct=(win&&s.bull===true)||(!win&&s.bull===false);out[k]=clamp(out[k]+(s.bull===null?0:correct?0.1:-0.06));});return out;}

// ─── TWO-PHASE STOP LOSS ─────────────────────────────────────
async function closePosition(t, exitPx, result, pnl, state, cfg, token) {
  const res = await placeOrder(t.symbol, 'SELL', t.shares, exitPx, cfg, token);
  if (!res.success && !res.sandboxed) return;
  const newCoefs = updateCoefs(state.coefs || {RSI:1,MACD:1,BB:1,VWAP:1,SMA:1,EMA:1}, t.ind_snapshot, result);
  const newBal   = (state.balance || cfg.capital || 10000) * (1 + pnl / 100);
  if (t.db_id) await updateUserTrade(t.db_id, { exit_price: exitPx, result, pnl: +pnl.toFixed(3), exit_time: new Date().toISOString() });
  await patchUserState(cfg.user_id, { balance: newBal, open_trade: null, last_trade_date: new Date().toISOString(), coefs: newCoefs, status_text: `SOLD ${t.symbol} ${pnl>=0?'+':''}${pnl.toFixed(2)}%` });
  await sendPush(cfg, `AutoTrader — ${result} ${pnl>=0?'+':''}${pnl.toFixed(2)}%`, `${t.symbol} | ${pnl>=0?'+':''}${pnl.toFixed(2)}% | Balance: $${newBal.toFixed(2)}`);
  await sendTradeEmail(cfg, result, { symbol: t.symbol, shares: t.shares, entryPrice: t.entry_price, exitPrice: exitPx, pnl, balance: newBal, grade: t.grade, score: t.score, reason: result === 'WIN' ? 'Target/trail stop hit' : 'Stop loss triggered' });
}

async function monitorPosition(state, cfg, token) {
  const t = state.open_trade; if (!t) return;
  let px = t.entry_price;
  try { const q = await fetchQuote(t.symbol, cfg, token); if (q?.price > 0) px = q.price; } catch (e) {}
  const pnl  = (px - t.entry_price) / t.entry_price * 100;
  const days = countBizDays(new Date(t.entry_time), new Date());
  const highestPrice = Math.max(px, t.highest_price || t.entry_price);

  if (t.trailing_active) {
    const trailStop  = highestPrice * 0.995;
    const currentPnl = (px - t.entry_price) / t.entry_price * 100;
    await patchUserState(cfg.user_id, { open_trade: { ...t, highest_price: highestPrice, trail_stop: trailStop }, status_text: `${t.symbol} TRAILING +${currentPnl.toFixed(2)}% | peak $${highestPrice.toFixed(2)} | trail $${trailStop.toFixed(2)}` });
    if (px <= trailStop || days >= 3) {
      const why    = px <= trailStop ? `trail stop @ $${trailStop.toFixed(2)}` : '3-day expiry';
      const result = currentPnl >= 0 ? 'WIN' : 'LOSS';
      await addUserLog(cfg.user_id, 'win', `${t.symbol} CLOSE — ${why} | +${currentPnl.toFixed(2)}% @ $${px.toFixed(2)}`);
      await closePosition(t, px, result, currentPnl, state, cfg, token);
    }
    return;
  }

  if (pnl >= 1.0) {
    const trailStop = px * 0.995;
    await addUserLog(cfg.user_id, 'info', `${t.symbol} +1% HIT @ $${px.toFixed(2)} — switching to trailing stop`);
    await patchUserState(cfg.user_id, { open_trade: { ...t, trailing_active: true, highest_price: px, trail_stop: trailStop }, status_text: `${t.symbol} trailing — peak $${px.toFixed(2)} trail $${trailStop.toFixed(2)}` });
    await sendPush(cfg, `AutoTrader — ${t.symbol} +1% Hit!`, `Trailing stop activated | Peak: $${px.toFixed(2)} | Trail: $${trailStop.toFixed(2)}`);
  } else if (px <= t.stop_price || days >= 3) {
    const why    = px <= t.stop_price ? `fixed stop @ $${t.stop_price?.toFixed(2)}` : '3-day expiry';
    const result = pnl >= 0 ? 'WIN' : 'LOSS';
    await addUserLog(cfg.user_id, result === 'WIN' ? 'win' : 'loss', `${t.symbol} CLOSE — ${why} | ${pnl>=0?'+':''}${pnl.toFixed(2)}% @ $${px.toFixed(2)}`);
    await closePosition(t, px, result, pnl, state, cfg, token);
  } else {
    await patchUserState(cfg.user_id, { open_trade: { ...t, highest_price: highestPrice }, status_text: `Holding ${t.symbol} ${pnl>=0?'+':''}${pnl.toFixed(2)}% | stop $${t.stop_price?.toFixed(2)} | day ${days}/3` });
  }
}

// ═══════════════════════════════════════════════════════════════
// MULTI-USER SCAN CYCLE
// Runs for ALL active subscribers simultaneously every 60s
// Each user is fully isolated — their own credentials, state, trades
// ═══════════════════════════════════════════════════════════════
const userScanIdx = new Map(); // user_id → current watchlist index

async function scanUser(cfg) {
  const uid = cfg.user_id;
  if (!checkSubscription(cfg)) return;

  // Get fresh state for this user
  const state = await getUserState(uid);
  if (!state?.running) return;

  // Get token for this user's broker
  let token = null;
  if (cfg.broker === 'schwab') {
    token = await getSchwabToken(cfg);
    if (!token && !cfg.sandbox) { await patchUserState(uid, { status_text: 'Schwab token expired — re-authorization needed' }); return; }
  } else if (!cfg.sandbox) {
    await checkAndRenewEtrade(cfg);
  }

  // Monitor open position
  if (state.open_trade) { await monitorPosition(state, cfg, token); return; }

  // Market hours check
  if (!isMarketOpen()) { await patchUserState(uid, { status_text: 'Market closed — bot idle' }); return; }

  // Clearing lock
  if (!clearOk(state.last_trade_date)) {
    const next = bizAdd(new Date(state.last_trade_date), CLEARING_DAYS);
    await patchUserState(uid, { status_text: `T+${CLEARING_DAYS} clearing — next trade: ${next.toLocaleDateString('en-US',{month:'short',day:'numeric'})}` });
    return;
  }

  // Scan watchlist
  const watchlist = (cfg.watchlist || '').split(',').map(s => s.trim()).filter(Boolean);
  if (!watchlist.length) { await patchUserState(uid, { status_text: 'Watchlist empty' }); return; }

  const idx    = (userScanIdx.get(uid) || 0) % watchlist.length;
  const sym    = watchlist[idx];
  userScanIdx.set(uid, idx + 1);

  await addUserLog(uid, 'scan', `Scanning ${sym} [${idx+1}/${watchlist.length}] | ${cfg.broker||'etrade'} | ${cfg.sandbox?'sandbox':'LIVE'}`);
  await patchUserState(uid, { scan_idx: idx + 1, status_text: `Scanning ${sym}...` });

  try {
    const candles   = await fetchCandles(sym, cfg, token);
    const vwapArr   = calcVwap(candles);
    const liveVwap  = vwapArr[vwapArr.length - 1];
    const coefs     = state.coefs || { RSI:1, MACD:1, BB:1, VWAP:1, SMA:1, EMA:1 };
    const ind       = analyze(candles, liveVwap, coefs);

    await patchUserState(uid, { last_analysis: { symbol: sym, ...ind } });

    let px = ind.px;
    try { const q = await fetchQuote(sym, cfg, token); if (q?.price > 0) px = q.price; } catch (e) {}

    let grade = null;
    if (cfg.ant_key) { grade = await aiGrade(sym, ind, cfg.ant_key); if (grade) await patchUserState(uid, { last_grade: grade }); }

    const GR = ['A','B','C','D','F'];
    const scoreOk = ind.comp >= (cfg.min_score || 65);
    const gOk     = grade ? GR.indexOf(grade.grade) <= GR.indexOf(cfg.min_grade || 'B') && !grade.avoid : ind.comp >= ((cfg.min_score || 65) + 8);
    const isBuy   = grade ? grade.action === 'BUY' : ind.comp >= (cfg.min_score || 65);

    if (scoreOk && gOk && isBuy) {
      const bal     = await getAvailableBalance(cfg, token, state.balance || cfg.capital || 10000);
      const shares  = Math.max(1, Math.floor(bal * ((cfg.pos_pct || 95) / 100) / px));
      const target  = grade?.target ? +grade.target : +(px * 1.012).toFixed(2);
      const stop    = grade?.stop   ? +grade.stop   : +(px * 0.988).toFixed(2);

      await addUserLog(uid, 'buy', `SIGNAL ${sym} | Grade=${grade?.grade||'rule'} Score=${ind.comp} | BUY ${shares}sh @ $${px.toFixed(2)} | bal=$${bal.toFixed(2)}`);
      const res = await placeOrder(sym, 'BUY', shares, px, cfg, token);

      if (res.success || res.sandboxed) {
        const rows = await addUserTrade(uid, { symbol:sym, action:'BUY', shares, entry_price:px, target, stop_price:stop, result:'OPEN', grade:grade?.grade||'--', score:ind.comp, entry_time:new Date().toISOString(), ind_snapshot:ind.sigs });
        const dbId = Array.isArray(rows) ? rows[0]?.id : null;
        await patchUserState(uid, {
          open_trade: { db_id:dbId, symbol:sym, shares, entry_price:px, target, stop_price:stop, entry_time:new Date().toISOString(), grade:grade?.grade||'--', score:ind.comp, ind_snapshot:ind.sigs, trailing_active:false, highest_price:px, trail_stop:null },
          last_trade_date: new Date().toISOString(),
          status_text: `BOUGHT ${shares}sh ${sym} @ $${px.toFixed(2)} | stop $${stop.toFixed(2)} → trails at +1%`
        });
        await sendTradeEmail(cfg, 'BUY', { symbol:sym, shares, entryPrice:px, grade:grade?.grade||'--', score:ind.comp, reason:grade?.reason||'Signal threshold met' });
      }
    } else {
      const why = !scoreOk ? `score ${ind.comp}<${cfg.min_score||65}` : !gOk ? `grade ${grade?.grade} below ${cfg.min_grade||'B'}` : `signal=${grade?.action||'neutral'}`;
      await addUserLog(uid, 'skip', `${sym} — no trade (${why})`);
      await patchUserState(uid, { status_text: `${sym} skipped — ${why}` });
    }
  } catch (err) {
    await addUserLog(uid, 'err', `${sym} scan error: ${err.message}`);
    await patchUserState(uid, { status_text: `Error scanning ${sym}: ${err.message}` });
  }
}

// ─── MAIN BOT LOOP ───────────────────────────────────────────
let botInterval = null;

async function runAllUsers() {
  try {
    const activeUsers = await getActiveUsers();
    if (!activeUsers?.length) return;
    const running = activeUsers.filter(cfg => cfg.user_id);
    if (running.length === 0) return;
    console.log(`[BOT] Scanning ${running.length} active user(s)`);
    // Run all users in parallel — each fully isolated
    await Promise.allSettled(running.map(cfg => scanUser(cfg)));
  } catch (e) {
    console.error('[BOT] Main loop error:', e.message);
  }
}

async function startLoop() {
  console.log(`AutoTrader v7.0 — Multi-user mode | clearing=${CLEARING_DAYS===0?'OFF':'T+'+CLEARING_DAYS}`);
  // Check for users whose bot_state.running = true and resume them
  const activeUsers = await getActiveUsers();
  const resuming    = [];
  for (const cfg of (activeUsers || [])) {
    const state = await getUserState(cfg.user_id);
    if (state?.running) resuming.push(cfg.user_id.substring(0, 8));
  }
  if (resuming.length) console.log(`[BOT] Resuming ${resuming.length} user(s): ${resuming.join(', ')}`);

  botInterval = setInterval(runAllUsers, 60000);
  runAllUsers(); // Run immediately on start
}

// ─── API ROUTES ──────────────────────────────────────────────
app.get('/', (req, res) => res.json({ status: 'AutoTrader v7.0 running', time: new Date().toISOString(), mode: 'multi-user' }));
app.get('/health', (req, res) => res.json({ ok: true }));

// Start/stop per user (called from dashboard)
app.post('/bot/start', async (req, res) => {
  const { user_id } = req.body;
  if (!user_id) return res.json({ ok: false, error: 'Missing user_id' });
  await patchUserState(user_id, { running: true });
  res.json({ ok: true });
});

app.post('/bot/stop', async (req, res) => {
  const { user_id } = req.body;
  if (!user_id) return res.json({ ok: false, error: 'Missing user_id' });
  await patchUserState(user_id, { running: false });
  res.json({ ok: true });
});

app.post('/screener/run', async (req, res) => {
  res.json({ ok: true });
  try { await runScreener(); } catch (e) { console.log('[SCREENER] Error:', e.message); }
});

// ─── STARTUP ─────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, async () => {
  console.log(`AutoTrader v7.0 listening on port ${PORT}`);
  console.log(`Stripe: ${STRIPE_SECRET ? 'configured' : 'NOT configured'}`);
  startScreenerScheduler();
  await startLoop();
});
