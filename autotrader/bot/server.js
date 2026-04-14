// ============================================================
// AutoTrader Bot — Railway Server v6.0
// Path A: Each user brings their own Schwab dev credentials
// - Schwab market data (replaces Twelve Data completely)
// - Schwab order placement
// - Schwab OAuth 2.0 (auto-refresh, no daily PIN)
// - Falls back to Twelve Data if broker = etrade
// - Sandbox: DB balance | Live: real Schwab balance
// - Two-phase stop: fixed 1.2% → trailing 0.5% after +1%
// - T+1 clearing (CLEARING_DAYS=0 for testing, 1 for production)
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

// Stripe webhook needs raw body — must be before express.json()
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
    method: 'PATCH', headers: { ...SB_HDR, Prefer: 'return=representation' }, body: JSON.stringify(data)
  });
  return r.json();
}
async function sbPost(table, data) {
  const r = await fetch(`${SB_URL}/rest/v1/${table}`, {
    method: 'POST', headers: { ...SB_HDR, Prefer: 'return=representation' }, body: JSON.stringify(data)
  });
  return r.json();
}
const getConfig   = () => sbGet('config',    '?id=eq.1').then(r => r[0]);
const getState    = () => sbGet('bot_state', '?id=eq.1').then(r => r[0]);
const patchState  = d  => sbPatch('bot_state', { ...d, updated_at: new Date().toISOString() }, '?id=eq.1');
const patchConfig = d  => sbPatch('config',    { ...d, updated_at: new Date().toISOString() }, '?id=eq.1');
async function addLog(type, message) {
  console.log(`[${type.toUpperCase()}] ${message}`);
  try { await sbPost('activity_log', { type, message }); } catch (e) {}
}
async function addTrade(t)        { return sbPost('trades', t); }
async function updateTrade(id, d) { return sbPatch('trades', d, `?id=eq.${id}`); }

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
  } catch (e) { console.log('[PUSH] Failed:', e.message); }
}

// ═══════════════════════════════════════════════════════════════
// STRIPE PAYMENTS
// $20/mo subscription — users pay immediately on signup
// No free trial — access granted the moment payment clears
// ═══════════════════════════════════════════════════════════════

const STRIPE_SECRET     = process.env.STRIPE_SECRET_KEY;
const STRIPE_PRICE_ID   = process.env.STRIPE_PRICE_ID;      // your $20/mo price ID
const STRIPE_WEBHOOK_SECRET = process.env.STRIPE_WEBHOOK_SECRET;
const STRIPE_API        = 'https://api.stripe.com/v1';

function stripeAuth() {
  return 'Basic ' + Buffer.from(`${STRIPE_SECRET}:`).toString('base64');
}

async function stripePost(endpoint, params) {
  const r = await fetch(`${STRIPE_API}${endpoint}`, {
    method: 'POST',
    headers: { Authorization: stripeAuth(), 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams(params)
  });
  return r.json();
}

async function stripeGet(endpoint) {
  const r = await fetch(`${STRIPE_API}${endpoint}`, {
    headers: { Authorization: stripeAuth() }
  });
  return r.json();
}

// Create Stripe checkout session — user pays $20 immediately
// Returns a URL to redirect the user to Stripe's hosted payment page
app.post('/stripe/checkout', async (req, res) => {
  const { email, userId } = req.body;
  if (!email || !userId) return res.json({ ok: false, error: 'Missing email or userId' });
  if (!STRIPE_SECRET || !STRIPE_PRICE_ID) return res.json({ ok: false, error: 'Stripe not configured on server' });

  try {
    // Create or retrieve Stripe customer
    const customer = await stripePost('/customers', {
      email,
      metadata: { supabase_user_id: userId }
    });

    const railwayUrl = `https://${process.env.RAILWAY_PUBLIC_DOMAIN || 'your-railway-url.up.railway.app'}`;

    // Create checkout session for $20/mo subscription
    const session = await stripePost('/checkout/sessions', {
      mode:                        'subscription',
      'payment_method_types[0]':   'card',
      'line_items[0][price]':      STRIPE_PRICE_ID,
      'line_items[0][quantity]':   1,
      customer:                    customer.id,
      'metadata[user_id]':         userId,
      success_url:                 `${railwayUrl}/stripe/success?session_id={CHECKOUT_SESSION_ID}`,
      cancel_url:                  `${railwayUrl}/stripe/cancel`,
      'subscription_data[metadata][user_id]': userId
    });

    res.json({ ok: true, url: session.url, sessionId: session.id });
  } catch (e) {
    res.json({ ok: false, error: e.message });
  }
});

// Success page — shown after successful payment
// Activates the user's subscription in Supabase immediately
app.get('/stripe/success', async (req, res) => {
  const { session_id } = req.query;
  if (!session_id) return res.redirect('/');

  try {
    // Retrieve session from Stripe to confirm payment
    const session = await stripeGet(`/checkout/sessions/${session_id}?expand[]=subscription`);
    const userId  = session.metadata?.user_id;
    const subId   = session.subscription?.id || session.subscription;
    const custId  = session.customer;

    if (userId && subId) {
      // Activate subscription in Supabase immediately
      await sbPatch('config', {
        stripe_customer_id:    custId,
        stripe_subscription_id: subId,
        subscription_status:   'active',
        subscription_end:      null
      }, `?user_id=eq.${userId}`);

      await addLog('info', `Subscription activated for user ${userId} — sub: ${subId}`);
    }

    res.send(`<!DOCTYPE html><html><head><meta name="viewport" content="width=device-width,initial-scale=1">
    <style>body{font-family:monospace;background:#050a0f;color:#b8d4ee;padding:30px;max-width:540px;margin:0 auto}.box{background:#0a2a1a;border:1px solid #00e891;border-radius:8px;padding:24px}h2{color:#00e891}p{line-height:1.8;margin-top:8px}.btn{display:block;background:#00e891;color:#050a0f;padding:14px;border-radius:6px;text-align:center;text-decoration:none;font-weight:700;font-size:15px;margin-top:18px}</style>
    </head><body>
    <div class="box">
      <h2>✓ Payment Successful!</h2>
      <p>Your AutoTrader subscription is now active.</p>
      <p style="color:#4a7090">$20.00/month — renews automatically. Cancel anytime in your account settings.</p>
      <a class="btn" href="https://autotrader-ruby.vercel.app">Open AutoTrader →</a>
    </div>
    </body></html>`);
  } catch (e) {
    res.send(`<html><body style="font-family:monospace;background:#050a0f;color:#f0364a;padding:30px"><h2>Error confirming payment</h2><p>${e.message}</p><p>Contact support — your payment may have gone through.</p></body></html>`);
  }
});

// Cancel page — shown if user backs out of checkout
app.get('/stripe/cancel', (req, res) => {
  res.send(`<!DOCTYPE html><html><head><meta name="viewport" content="width=device-width,initial-scale=1">
  <style>body{font-family:monospace;background:#050a0f;color:#b8d4ee;padding:30px;max-width:540px;margin:0 auto}.box{background:#2a0a0a;border:1px solid #f0364a;border-radius:8px;padding:24px}h2{color:#f5a623}.btn{display:block;background:#091422;border:1px solid #162d47;color:#b8d4ee;padding:14px;border-radius:6px;text-align:center;text-decoration:none;font-weight:700;margin-top:16px}</style>
  </head><body>
  <div class="box">
    <h2>Payment Cancelled</h2>
    <p>No charge was made. You can try again whenever you're ready.</p>
    <a class="btn" href="https://autotrader-ruby.vercel.app">Return to AutoTrader</a>
  </div>
  </body></html>`);
});

// Stripe webhook — handles subscription events automatically
// Stripe calls this when payments succeed, fail, or subscriptions cancel
app.post('/stripe/webhook', express.raw({ type: 'application/json' }), async (req, res) => {
  const sig = req.headers['stripe-signature'];
  if (!sig || !STRIPE_WEBHOOK_SECRET) return res.status(400).json({ error: 'No signature' });

  // Verify webhook signature (prevents fake events)
  let event;
  try {
    // Manual HMAC verification (no Stripe SDK needed)
    const payload   = req.body.toString();
    const parts     = sig.split(',');
    const timestamp = parts.find(p => p.startsWith('t=')).split('=')[1];
    const signature = parts.find(p => p.startsWith('v1=')).split('=')[1];
    const signed    = crypto.createHmac('sha256', STRIPE_WEBHOOK_SECRET)
                        .update(`${timestamp}.${payload}`)
                        .digest('hex');
    if (signed !== signature) return res.status(400).json({ error: 'Invalid signature' });
    event = JSON.parse(payload);
  } catch (e) {
    return res.status(400).json({ error: e.message });
  }

  const sub    = event.data?.object;
  const userId = sub?.metadata?.user_id;

  try {
    switch (event.type) {
      // Payment succeeded — activate or renew subscription
      case 'invoice.payment_succeeded': {
        const subId  = sub.subscription;
        const custId = sub.customer;
        if (userId) {
          await sbPatch('config', {
            stripe_customer_id:    custId,
            stripe_subscription_id: subId,
            subscription_status:   'active',
            subscription_end:      null
          }, `?user_id=eq.${userId}`);
          await addLog('info', `Payment succeeded — subscription renewed for user ${userId}`);
        }
        break;
      }

      // Payment failed — warn user but keep access briefly (Stripe retries)
      case 'invoice.payment_failed': {
        if (userId) {
          await sbPatch('config', {
            subscription_status: 'past_due'
          }, `?user_id=eq.${userId}`);
          // Get user config for push notification
          const rows = await sbGet('config', `?user_id=eq.${userId}`);
          if (rows?.[0]) await sendPush(rows[0], 'AutoTrader — Payment Failed', 'Your $20/mo payment failed. Update your card to keep the bot running.');
          await addLog('warn', `Payment failed for user ${userId}`);
        }
        break;
      }

      // Subscription cancelled — revoke access immediately
      case 'customer.subscription.deleted': {
        if (userId) {
          await sbPatch('config', {
            subscription_status: 'canceled',
            subscription_end:    new Date().toISOString()
          }, `?user_id=eq.${userId}`);
          // Stop the bot
          await sbPatch('bot_state', {
            running: false,
            status_text: 'Subscription cancelled'
          }, `?user_id=eq.${userId}`);
          await addLog('info', `Subscription cancelled for user ${userId}`);
        }
        break;
      }

      // Subscription updated (plan change, etc.)
      case 'customer.subscription.updated': {
        const status = sub.status; // active, past_due, canceled, etc.
        if (userId) {
          await sbPatch('config', {
            subscription_status: status === 'active' ? 'active' : status
          }, `?user_id=eq.${userId}`);
        }
        break;
      }
    }
  } catch (e) {
    await addLog('err', `Stripe webhook error (${event.type}): ${e.message}`);
  }

  res.json({ received: true });
});

// Check if user has active subscription before starting bot
async function checkSubscription(cfg) {
  if (!STRIPE_SECRET) return true; // Stripe not configured — allow (dev mode)
  if (cfg.subscription_status === 'active') return true;
  if (cfg.subscription_status === 'past_due') {
    // Past due — give 3 day grace period then block
    await addLog('warn', 'Subscription past due — payment retry pending');
    return true; // Still allow for now, Stripe will retry
  }
  return false; // inactive or canceled — block
}



// ═══════════════════════════════════════════════════════════════
// EMAIL NOTIFICATIONS via Resend
// Free tier: 100 emails/day — get API key at resend.com
// All losses also copied to admin: quantautotraderai@gmail.com
// ═══════════════════════════════════════════════════════════════
const RESEND_API_KEY  = process.env.RESEND_API_KEY;
const FROM_EMAIL      = process.env.FROM_EMAIL || 'AutoTrader <notifications@yourdomain.com>';
const ADMIN_EMAIL     = 'quantautotraderai@gmail.com';

async function sendEmail(to, subject, html) {
  if (!RESEND_API_KEY) { console.log('[EMAIL] RESEND_API_KEY not set — skipping'); return; }
  if (!to) { console.log('[EMAIL] No recipient — skipping'); return; }
  try {
    const r = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${RESEND_API_KEY}` },
      body: JSON.stringify({ from: FROM_EMAIL, to: [to], subject, html })
    });
    const j = await r.json();
    if (j.id) console.log(`[EMAIL] Sent to ${to} — id: ${j.id}`);
    else console.log('[EMAIL] Error:', JSON.stringify(j));
  } catch (e) { console.log('[EMAIL] Failed:', e.message); }
}

// Get notification email — user's custom address or their account email
async function getNotificationEmail(cfg) {
  if (cfg.notification_email) return cfg.notification_email;
  // Fall back to Supabase auth email
  try {
    const r = await fetch(`${SB_URL}/auth/v1/admin/users/${cfg.user_id}`, {
      headers: { apikey: SB_KEY, Authorization: `Bearer ${SB_KEY}` }
    });
    const j = await r.json();
    return j.email || null;
  } catch (e) { return null; }
}

function tradeEmailHtml({ type, symbol, shares, entryPrice, exitPrice, pnl, balance, reason, grade, score }) {
  const isWin  = type === 'WIN';
  const isBuy  = type === 'BUY';
  const isSell = type === 'SELL' || type === 'WIN' || type === 'LOSS';
  const color  = isWin ? '#00e891' : type === 'LOSS' ? '#f0364a' : '#f5a623';
  const emoji  = isWin ? '🟢' : type === 'LOSS' ? '🔴' : isBuy ? '🟡' : '⚪';

  return `<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#050a0f;font-family:monospace">
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
          <div style="font-size:9px;color:#4a7090;text-transform:uppercase;letter-spacing:.08em">Entry Price</div>
          <div style="font-size:20px;font-weight:700;color:#b8d4ee;margin-top:3px">$${(+entryPrice).toFixed(2)}</div>
        </div>
        <div style="background:#0d1c30;border-radius:6px;padding:12px">
          <div style="font-size:9px;color:#4a7090;text-transform:uppercase;letter-spacing:.08em">Shares</div>
          <div style="font-size:20px;font-weight:700;color:#b8d4ee;margin-top:3px">${shares}</div>
        </div>
      </div>
      <div style="background:#0d1c30;border-radius:6px;padding:12px;margin-bottom:16px">
        <div style="font-size:9px;color:#4a7090;text-transform:uppercase;letter-spacing:.08em">AI Grade / Score</div>
        <div style="font-size:16px;font-weight:700;color:#f5a623;margin-top:3px">Grade ${grade || '--'} &nbsp;|&nbsp; Score ${score || '--'}/100</div>
      </div>
      ` : `
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:16px">
        <div style="background:#0d1c30;border-radius:6px;padding:12px">
          <div style="font-size:9px;color:#4a7090;text-transform:uppercase;letter-spacing:.08em">Entry</div>
          <div style="font-size:18px;font-weight:700;color:#b8d4ee;margin-top:3px">$${(+entryPrice).toFixed(2)}</div>
        </div>
        <div style="background:#0d1c30;border-radius:6px;padding:12px">
          <div style="font-size:9px;color:#4a7090;text-transform:uppercase;letter-spacing:.08em">Exit</div>
          <div style="font-size:18px;font-weight:700;color:#b8d4ee;margin-top:3px">$${(+exitPrice).toFixed(2)}</div>
        </div>
      </div>
      <div style="background:#0d1c30;border-radius:6px;padding:14px;margin-bottom:16px;text-align:center">
        <div style="font-size:9px;color:#4a7090;text-transform:uppercase;letter-spacing:.08em;margin-bottom:4px">P&L</div>
        <div style="font-size:28px;font-weight:700;color:${color}">${pnl >= 0 ? '+' : ''}${(+pnl).toFixed(2)}%</div>
      </div>
      <div style="background:#0d1c30;border-radius:6px;padding:12px;margin-bottom:16px">
        <div style="font-size:9px;color:#4a7090;text-transform:uppercase;letter-spacing:.08em">New Balance</div>
        <div style="font-size:18px;font-weight:700;color:#b8d4ee;margin-top:3px">$${(+balance).toFixed(2)}</div>
      </div>
      `}
      ${reason ? `<div style="background:#0d1c30;border-radius:6px;padding:12px;font-size:11px;color:#4a7090;line-height:1.6"><strong style="color:#b8d4ee">Reason:</strong> ${reason}</div>` : ''}
    </div>
    <div style="background:#050a0f;padding:12px 20px;font-size:9px;color:#243c52;text-align:center">
      AutoTrader AI &nbsp;•&nbsp; ${new Date().toLocaleString('en-US',{timeZone:'America/New_York'})} ET
    </div>
  </div>
</div>
</body></html>`;
}

async function sendTradeEmail(cfg, type, data) {
  const notifEmail = await getNotificationEmail(cfg);
  const subject = `AutoTrader — ${type} ${data.symbol}${data.pnl !== undefined ? ' ' + (data.pnl >= 0 ? '+' : '') + (+data.pnl).toFixed(2) + '%' : ''}`;
  const html = tradeEmailHtml({ type, ...data });

  // Send to user
  if (notifEmail) await sendEmail(notifEmail, subject, html);

  // Send losses to admin with user info
  if (type === 'LOSS') {
    const adminHtml = tradeEmailHtml({ type, ...data }).replace(
      '</div></div></body>',
      `<div style="background:#2a0a0a;border:1px solid #f0364a;border-radius:6px;padding:12px;margin:12px 20px;font-size:11px;color:#f0364a">
        <strong>User Account:</strong> ${notifEmail || 'unknown'}<br>
        <strong>User ID:</strong> ${cfg.user_id || 'unknown'}
      </div></div></div></body>`
    );
    await sendEmail(ADMIN_EMAIL, `[LOSS ALERT] ${data.symbol} — User: ${notifEmail || 'unknown'}`, adminHtml);
  }
}

// ─── CLEARING & MARKET HOURS ─────────────────────────────────
// CLEARING_DAYS = 0 → disabled (testing)
// CLEARING_DAYS = 1 → T+1 (production — all US brokers since May 2024)
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
// SCHWAB API — Market Data + Orders + OAuth 2.0
// Each user brings their own Client ID + Secret (Path A)
// Their calls hit their own rate limit — scales to any user count
// ═══════════════════════════════════════════════════════════════
const SCHWAB_BASE        = 'https://api.schwabapi.com';
const SCHWAB_MARKET      = 'https://api.schwabapi.com/marketdata/v1';
const SCHWAB_TRADER      = 'https://api.schwabapi.com/trader/v1';
const SCHWAB_TOKEN_URL   = 'https://api.schwabapi.com/v1/oauth/token';
const SCHWAB_AUTH_URL    = 'https://api.schwabapi.com/v1/oauth/authorize';

function schwabBasicAuth(cfg) {
  return 'Basic ' + Buffer.from(`${cfg.schwab_client_id}:${cfg.schwab_client_secret}`).toString('base64');
}

// ── Token Management ─────────────────────────────────────────
async function refreshSchwabToken(cfg) {
  if (!cfg.schwab_refresh_token || !cfg.schwab_client_id) return false;
  try {
    const r = await fetch(SCHWAB_TOKEN_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded', Authorization: schwabBasicAuth(cfg) },
      body: new URLSearchParams({ grant_type: 'refresh_token', refresh_token: cfg.schwab_refresh_token })
    });
    const j = await r.json();
    if (!j.access_token) { await addLog('err', 'Schwab refresh failed: ' + JSON.stringify(j)); return false; }
    await patchConfig({
      schwab_access_token:  j.access_token,
      schwab_refresh_token: j.refresh_token || cfg.schwab_refresh_token,
      schwab_token_expiry:  new Date(Date.now() + (j.expires_in || 1800) * 1000).toISOString()
    });
    await addLog('info', 'Schwab token refreshed automatically');
    return true;
  } catch (e) { await addLog('err', 'Schwab refresh error: ' + e.message); return false; }
}

async function ensureSchwabToken(cfg) {
  if (!cfg.schwab_access_token) return null;
  const expiry = cfg.schwab_token_expiry ? new Date(cfg.schwab_token_expiry) : null;
  if (!expiry || expiry - new Date() < 5 * 60 * 1000) {
    const ok = await refreshSchwabToken(cfg);
    if (!ok) {
      const authUrl = `https://${process.env.RAILWAY_PUBLIC_DOMAIN || ''}/schwab/auth`;
      await sendPush(cfg, 'AutoTrader — Schwab Re-auth Needed', 'Tap to reconnect your Schwab account.', authUrl);
      return null;
    }
    return (await getConfig()).schwab_access_token;
  }
  return cfg.schwab_access_token;
}

// ── Market Data ──────────────────────────────────────────────
// Fetches OHLCV candles — replaces Twelve Data time_series
async function schwabFetchCandles(sym, interval, lookback, token) {
  // Map our interval format to Schwab's format
  const periodMap = {
    '1min':  { frequencyType: 'minute', frequency: 1,  periodType: 'day',   period: 1 },
    '5min':  { frequencyType: 'minute', frequency: 5,  periodType: 'day',   period: 1 },
    '15min': { frequencyType: 'minute', frequency: 15, periodType: 'day',   period: 1 },
    '1day':  { frequencyType: 'daily',  frequency: 1,  periodType: 'month', period: 1 }
  };
  const p = periodMap[interval] || periodMap['5min'];

  // Calculate start time based on lookback candles
  // For intraday we need enough days to get lookback candles
  const daysNeeded = interval === '1day' ? Math.ceil(lookback / 5) + 5
                   : Math.ceil((lookback * (interval === '1min' ? 1 : +interval.replace('min','')) / 390)) + 2;
  const startTime = Date.now() - daysNeeded * 24 * 60 * 60 * 1000;

  const params = new URLSearchParams({
    symbol:        sym,
    periodType:    p.periodType,
    period:        p.period,
    frequencyType: p.frequencyType,
    frequency:     p.frequency,
    startDate:     startTime,
    endDate:       Date.now(),
    needExtendedHoursData: false
  });

  const r = await fetch(`${SCHWAB_MARKET}/pricehistory?${params}`, {
    headers: { Authorization: `Bearer ${token}` }
  });
  const j = await r.json();
  if (!j.candles?.length) throw new Error(`Schwab: no candles for ${sym}`);

  // Convert to our standard format and take last N candles
  const candles = j.candles.map(c => ({
    date:   new Date(c.datetime).toISOString(),
    open:   c.open,
    high:   c.high,
    low:    c.low,
    close:  c.close,
    volume: c.volume
  }));

  return candles.slice(-lookback);
}

// Fetch single quote — replaces Twelve Data quote
async function schwabFetchQuote(sym, token) {
  try {
    const r = await fetch(`${SCHWAB_MARKET}/quotes/${sym}`, {
      headers: { Authorization: `Bearer ${token}` }
    });
    const j = await r.json();
    const q = j[sym]?.quote || j[sym];
    if (!q) return null;
    return {
      price:     q.lastPrice || q.mark || 0,
      changePct: q.netPercentChangeInDouble || 0,
      volume:    q.totalVolume || 0
    };
  } catch (e) { return null; }
}

// Batch quotes for screener — one call for all symbols
async function schwabBatchQuotes(symbols, token) {
  try {
    const r = await fetch(`${SCHWAB_MARKET}/quotes?symbols=${symbols.join(',')}&fields=quote`, {
      headers: { Authorization: `Bearer ${token}` }
    });
    return r.json();
  } catch (e) { return {}; }
}

// Top movers — replaces our manual gap screener
// Returns stocks with biggest premarket moves on major indices
async function schwabGetMovers(token) {
  try {
    // Get movers on all three major indices
    const [spx, ndx, djx] = await Promise.all([
      fetch(`${SCHWAB_MARKET}/movers/%24SPX?sort=PERCENT_CHANGE_UP&frequency=0`, { headers: { Authorization: `Bearer ${token}` } }).then(r => r.json()),
      fetch(`${SCHWAB_MARKET}/movers/%24COMPX?sort=PERCENT_CHANGE_UP&frequency=0`, { headers: { Authorization: `Bearer ${token}` } }).then(r => r.json()),
      fetch(`${SCHWAB_MARKET}/movers/%24DJI?sort=PERCENT_CHANGE_UP&frequency=0`, { headers: { Authorization: `Bearer ${token}` } }).then(r => r.json())
    ]);
    const all = [...(spx.screeners || []), ...(ndx.screeners || []), ...(djx.screeners || [])];
    // Deduplicate by symbol
    const seen = new Set();
    return all.filter(m => { if (seen.has(m.symbol)) return false; seen.add(m.symbol); return true; });
  } catch (e) { await addLog('err', 'Schwab movers error: ' + e.message); return []; }
}

// Calculate VWAP from candles (Schwab doesn't have a dedicated VWAP endpoint)
function calcVwapFromCandles(candles) {
  let cv = 0, cq = 0;
  const result = candles.map(c => {
    const tp = (c.high + c.low + c.close) / 3;
    cv += tp * c.volume; cq += c.volume;
    return cq > 0 ? cv / cq : tp;
  });
  return result[result.length - 1]; // Return latest VWAP value
}

// ── Account Balance ──────────────────────────────────────────
async function schwabGetBalance(token, accountHash) {
  try {
    const r = await fetch(`${SCHWAB_TRADER}/accounts/${accountHash}?fields=balances`, {
      headers: { Authorization: `Bearer ${token}` }
    });
    const j = await r.json();
    return j?.securitiesAccount?.currentBalances?.cashAvailableForTrading
        || j?.securitiesAccount?.currentBalances?.availableFunds
        || null;
  } catch (e) { await addLog('err', 'Schwab balance error: ' + e.message); return null; }
}

// ── Order Placement ──────────────────────────────────────────
async function schwabPlaceOrder(sym, action, shares, price, token, accountHash) {
  const body = {
    orderType: 'MARKET',
    session:   'NORMAL',
    duration:  'DAY',
    orderStrategyType: 'SINGLE',
    orderLegCollection: [{
      instruction: action,
      quantity:    shares,
      instrument:  { symbol: sym, assetType: 'EQUITY' }
    }]
  };
  try {
    const r = await fetch(`${SCHWAB_TRADER}/accounts/${accountHash}/orders`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });
    if (r.status === 201) {
      const orderId = r.headers.get('location')?.split('/').pop() || 'unknown';
      return { success: true, orderId };
    }
    const err = await r.text();
    await addLog('err', `Schwab order failed (${r.status}): ${err}`);
    return { success: false };
  } catch (e) { await addLog('err', 'Schwab order error: ' + e.message); return { success: false }; }
}

// ── OAuth Flow ───────────────────────────────────────────────
app.get('/schwab/auth', async (req, res) => {
  const cfg = await getConfig();
  if (!cfg?.schwab_client_id || !cfg?.schwab_client_secret) {
    return res.send(`<!DOCTYPE html><html><body style="font-family:monospace;background:#050a0f;color:#b8d4ee;padding:30px;max-width:540px;margin:0 auto">
      <h2 style="color:#f5a623">Schwab Setup Required</h2>
      <p>Add your Schwab Client ID and Client Secret in CONFIG first.</p>
      <p style="margin-top:12px;color:#4a7090">Get them at <a href="https://developer.schwab.com" style="color:#2d9cff">developer.schwab.com</a> → Create App → wait for approval → copy credentials.</p>
      <p style="margin-top:12px;color:#4a7090">Callback URL to use when creating your app:<br>
      <strong style="color:#b8d4ee">https://${req.hostname}/schwab/callback</strong></p>
    </body></html>`);
  }
  const redirectUri = `https://${process.env.RAILWAY_PUBLIC_DOMAIN || req.hostname}/schwab/callback`;
  const authUrl = `${SCHWAB_AUTH_URL}?client_id=${cfg.schwab_client_id}&redirect_uri=${encodeURIComponent(redirectUri)}`;
  res.send(`<!DOCTYPE html><html><head><meta name="viewport" content="width=device-width,initial-scale=1">
  <style>body{font-family:monospace;background:#050a0f;color:#b8d4ee;padding:24px;max-width:540px;margin:0 auto}h2{color:#f5a623}.box{background:#091422;border:1px solid #162d47;border-radius:8px;padding:18px;margin:12px 0}.btn{display:block;background:#00e891;color:#050a0f;padding:16px;border-radius:6px;text-align:center;text-decoration:none;font-weight:700;font-size:16px;margin-top:14px}.note{font-size:11px;color:#4a7090;margin-top:8px;line-height:1.7}.tick{color:#00e891;margin-right:6px}</style>
  </head><body>
  <h2>Connect Charles Schwab</h2>
  <div class="box">
    <p>Tap the button below to authorize AutoTrader on your Schwab account.</p>
    <div style="margin:12px 0;font-size:11px;color:#4a7090;line-height:1.8">
      <div><span class="tick">✓</span>Uses your own Schwab dev credentials</div>
      <div><span class="tick">✓</span>Market data calls hit your own rate limit</div>
      <div><span class="tick">✓</span>Tokens refresh automatically — no daily PIN</div>
      <div><span class="tick">✓</span>Only needs to be done once</div>
    </div>
    <a class="btn" href="${authUrl}">Connect to Charles Schwab →</a>
    <p class="note">You will be redirected to Schwab's secure login page. After approving, you will be redirected back here automatically with everything saved.</p>
  </div>
  </body></html>`);
});

app.get('/schwab/callback', async (req, res) => {
  const { code, error } = req.query;
  if (error || !code) return res.send(`<html><body style="font-family:monospace;background:#050a0f;color:#f0364a;padding:30px"><h2>Authorization failed</h2><p>${error || 'No code received'}</p></body></html>`);
  const cfg = await getConfig();
  const redirectUri = `https://${process.env.RAILWAY_PUBLIC_DOMAIN || req.hostname}/schwab/callback`;
  try {
    const r = await fetch(SCHWAB_TOKEN_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded', Authorization: schwabBasicAuth(cfg) },
      body: new URLSearchParams({ grant_type: 'authorization_code', code, redirect_uri: redirectUri })
    });
    const j = await r.json();
    if (!j.access_token) return res.send(`<html><body style="font-family:monospace;background:#050a0f;color:#f0364a;padding:30px"><h2>Token exchange failed</h2><pre>${JSON.stringify(j,null,2)}</pre></body></html>`);

    await patchConfig({
      schwab_access_token:  j.access_token,
      schwab_refresh_token: j.refresh_token,
      schwab_token_expiry:  new Date(Date.now() + (j.expires_in || 1800) * 1000).toISOString(),
      broker: 'schwab'
    });

    // Auto-fetch account hash and save it
    let accountHash = '', accountInfo = '';
    try {
      const ar = await fetch(`${SCHWAB_TRADER}/accounts`, { headers: { Authorization: `Bearer ${j.access_token}` } });
      const accounts = await ar.json();
      if (accounts?.[0]) {
        accountHash = accounts[0].hashValue || '';
        const acct = accounts[0].securitiesAccount;
        accountInfo = acct ? `${acct.type} ending in ${acct.accountNumber?.slice(-4)}` : '';
        if (accountHash) await patchConfig({ schwab_account_hash: accountHash });
      }
    } catch (e) { await addLog('warn', 'Could not fetch Schwab account list: ' + e.message); }

    await addLog('info', `Schwab connected — account: ${accountInfo || accountHash}`);
    await patchState({ status_text: 'Schwab connected — bot ready to trade' });
    const freshCfg = await getConfig();
    await sendPush(freshCfg, 'AutoTrader — Schwab Connected!', `Account linked: ${accountInfo || 'ready to trade'}`);

    res.send(`<!DOCTYPE html><html><head><meta name="viewport" content="width=device-width,initial-scale=1">
    <style>body{font-family:monospace;background:#050a0f;color:#b8d4ee;padding:30px;max-width:540px;margin:0 auto}.box{background:#0a2a1a;border:1px solid #00e891;border-radius:8px;padding:24px}h2{color:#00e891}p{line-height:1.7;margin-top:8px}.detail{color:#4a7090;font-size:11px;margin-top:6px}</style>
    </head><body>
    <div class="box">
      <h2>✓ Schwab Connected!</h2>
      <p>Your Charles Schwab account has been linked to AutoTrader successfully.</p>
      ${accountInfo ? `<p class="detail">Account: ${accountInfo}</p>` : ''}
      <p style="margin-top:16px;color:#f5a623">Tokens refresh automatically — you will never need to do this again.</p>
      <p style="margin-top:12px">You can close this page and return to the AutoTrader app.</p>
    </div>
    </body></html>`);
  } catch (e) { res.send(`<html><body style="font-family:monospace;background:#050a0f;color:#f0364a;padding:30px"><h2>Error</h2><pre>${e.message}</pre></body></html>`); }
});

// ═══════════════════════════════════════════════════════════════
// E*TRADE — OAuth 1.0a (kept for existing users / sandbox testing)
// ═══════════════════════════════════════════════════════════════
function oauthSign(method, url, params, cs, ts=''){const s=Object.keys(params).sort().map(k=>`${encodeURIComponent(k)}=${encodeURIComponent(params[k])}`).join('&');const b=`${method}&${encodeURIComponent(url)}&${encodeURIComponent(s)}`;const sk=`${encodeURIComponent(cs)}&${encodeURIComponent(ts)}`;return crypto.createHmac('sha1',sk).update(b).digest('base64');}
function makeOAuthHeader(method,url,cfg,extra={}){const p={oauth_consumer_key:cfg.e_key,oauth_nonce:crypto.randomBytes(16).toString('hex'),oauth_signature_method:'HMAC-SHA1',oauth_timestamp:Math.floor(Date.now()/1000).toString(),oauth_version:'1.0',...extra};if(cfg.e_token&&!extra.oauth_callback)p.oauth_token=cfg.e_token;p.oauth_signature=oauthSign(method,url,p,cfg.e_secret,cfg.e_token_secret||'');return 'OAuth '+Object.keys(p).sort().map(k=>`${encodeURIComponent(k)}="${encodeURIComponent(p[k])}"`).join(', ');}
function parseQS(text){const r={};text.split('&').forEach(pair=>{const[k,v]=pair.split('=');if(k)r[decodeURIComponent(k)]=decodeURIComponent(v||'');});return r;}
async function etradeOAuthFetch(url,header){return fetch(url,{headers:{Authorization:header},credentials:'omit'});}

let lastEtradeRenewal=null,etradeRenewalWarned=false;
async function checkAndRenewEtrade(){
  const cfg=await getConfig();if(!cfg?.e_token||cfg?.sandbox)return;
  const now=new Date();
  if(!lastEtradeRenewal||(now-lastEtradeRenewal)>=90*60*1000){
    const base=cfg.sandbox?'https://apisb.etrade.com':'https://api.etrade.com';
    try{const r=await etradeOAuthFetch(`${base}/oauth/renew_access_token`,makeOAuthHeader('GET',`${base}/oauth/renew_access_token`,cfg));const text=await r.text();if(r.ok&&text.includes('renewed')){lastEtradeRenewal=new Date();etradeRenewalWarned=false;await addLog('info','E*Trade token renewed');return;}}catch(e){}
    if(!etradeRenewalWarned||(now-etradeRenewalWarned)>=30*60*1000){
      etradeRenewalWarned=now;
      await sendPush(cfg,'AutoTrader — E*Trade Re-auth Required','Tap to re-authorize.',`https://${process.env.RAILWAY_PUBLIC_DOMAIN||''}/etrade/auth`);
      await patchState({status_text:'E*Trade token expired — re-authorization required'});
    }
  }
}

app.get('/etrade/auth', async (req, res) => {
  const cfg=await getConfig();
  if(!cfg?.e_key||!cfg?.e_secret)return res.send(`<html><body style="font-family:monospace;background:#050a0f;color:#b8d4ee;padding:30px"><h2 style="color:#f5a623">Missing E*Trade Keys</h2><p>Add Consumer Key and Secret in Config first.</p></body></html>`);
  const sandbox=cfg.sandbox;const apiBase=sandbox?'https://apisb.etrade.com':'https://api.etrade.com';
  try{
    const tmpCfg={...cfg,e_token:'',e_token_secret:''};
    const r=await etradeOAuthFetch(`${apiBase}/oauth/request_token`,makeOAuthHeader('GET',`${apiBase}/oauth/request_token`,tmpCfg,{oauth_callback:'oob'}));
    const text=await r.text();const p=parseQS(text);
    if(!p.oauth_token)return res.send(`<html><body style="font-family:monospace;background:#050a0f;color:#f0364a;padding:30px"><h2>Error</h2><pre>${text}</pre><p>HTTP: ${r.status}</p></body></html>`);
    const authUrl=`https://us.etrade.com/e/t/etws/authorize?key=${cfg.e_key}&token=${p.oauth_token}`;
    res.send(`<!DOCTYPE html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"><style>body{font-family:monospace;background:#050a0f;color:#b8d4ee;padding:20px;max-width:540px;margin:0 auto}h2{color:#f5a623}h3{color:#00e891;font-size:14px;margin:0 0 8px}.box{background:#091422;border:1px solid #162d47;border-radius:6px;padding:16px;margin:12px 0}.btn-link{display:block;background:#162d47;border:1px solid #2d9cff;color:#2d9cff;padding:14px;border-radius:5px;text-align:center;text-decoration:none;margin-top:10px;font-weight:700}input{width:100%;background:#0d1c30;border:1px solid #1c3a59;border-radius:4px;padding:12px;color:#b8d4ee;font-family:monospace;font-size:20px;box-sizing:border-box;margin:8px 0;letter-spacing:.15em;text-transform:uppercase}button{background:#00e891;color:#050a0f;border:none;border-radius:5px;padding:14px;font-family:monospace;font-size:15px;font-weight:700;cursor:pointer;width:100%;margin-top:8px}.note{font-size:11px;color:#4a7090;margin-top:6px;line-height:1.5}#msg{margin-top:14px;padding:14px;border-radius:5px;display:none}</style></head><body>
    <h2>E*Trade Authorization</h2>
    <div style="display:inline-block;padding:3px 10px;border-radius:3px;font-size:11px;font-weight:700;margin-bottom:14px;background:rgba(245,166,35,.15);color:#f5a623">${sandbox?'SANDBOX':'LIVE'}</div>
    <div class="box"><h3>Step 1 — Authorize on E*Trade</h3><p>Tap the button, log in, copy the <strong style="color:#00e891">PIN</strong> immediately.</p><a class="btn-link" href="${authUrl}" target="_blank">Tap to Authorize on E*Trade</a><p class="note">Come back immediately after you see the PIN.</p></div>
    <div class="box"><h3>Step 2 — Enter PIN</h3><input type="text" id="pin" placeholder="XXXXX" inputmode="text" maxlength="10" autocomplete="off" autocorrect="off" spellcheck="false"/><button onclick="go()">Complete Authorization</button></div>
    <div id="msg"></div>
    <script>document.getElementById('pin').focus();async function go(){const pin=document.getElementById('pin').value.trim().toUpperCase();if(!pin){alert('Enter PIN first');return;}document.getElementById('msg').style.cssText='display:block;background:#162d47;padding:14px;border-radius:5px;color:#f5a623';document.getElementById('msg').innerHTML='Saving...';try{const r=await fetch('/etrade/callback?rt=${p.oauth_token}&pin='+encodeURIComponent(pin),{credentials:'omit'});const j=await r.json();const el=document.getElementById('msg');if(j.ok){el.style.cssText='display:block;background:#0a2a1a;border:1px solid #00e891;border-radius:5px;padding:16px';el.innerHTML='<p style="color:#00e891;margin:0">Done! Close this page.</p>';}else{el.style.cssText='display:block;background:#2a0a0a;border:1px solid #f0364a;border-radius:5px;padding:16px';el.innerHTML='<p style="color:#f0364a;margin:0">'+j.error+'</p>';}}catch(e){document.getElementById('msg').innerHTML='Network error';}}document.getElementById('pin').addEventListener('keydown',e=>{if(e.key==='Enter')go();});</script></body></html>`);
  }catch(e){res.send(`<html><body style="font-family:monospace;background:#050a0f;color:#f0364a;padding:30px"><h2>Error</h2><pre>${e.message}</pre></body></html>`);}
});

app.get('/etrade/callback', async (req, res) => {
  const{rt,pin}=req.query;if(!rt||!pin)return res.json({ok:false,error:'Missing token or PIN'});
  const cfg=await getConfig();const apiBase=cfg.sandbox?'https://apisb.etrade.com':'https://api.etrade.com';
  try{const tmpCfg={...cfg,e_token:rt,e_token_secret:''};const r=await etradeOAuthFetch(`${apiBase}/oauth/access_token`,makeOAuthHeader('GET',`${apiBase}/oauth/access_token`,tmpCfg,{oauth_verifier:pin.toUpperCase()}));const text=await r.text();const p=parseQS(text);if(!p.oauth_token)return res.json({ok:false,error:`PIN rejected (HTTP ${r.status}). Try again.`});await patchConfig({e_token:p.oauth_token,e_token_secret:p.oauth_token_secret});lastEtradeRenewal=new Date();etradeRenewalWarned=false;await addLog('info','E*Trade OAuth complete');await patchState({status_text:'E*Trade authorized — bot ready'});const freshCfg=await getConfig();await sendPush(freshCfg,'AutoTrader — E*Trade Authorized','E*Trade connected.');res.json({ok:true});}catch(e){res.json({ok:false,error:e.message});}
});

// ═══════════════════════════════════════════════════════════════
// UNIFIED MARKET DATA
// Schwab broker → use Schwab API (user's own credentials)
// E*Trade broker → use Twelve Data (existing behavior)
// ═══════════════════════════════════════════════════════════════
async function fetchCandles(sym, cfg, token) {
  if (cfg.broker === 'schwab' && token) {
    return schwabFetchCandles(sym, cfg.interval, cfg.lookback || 90, token);
  }
  // Twelve Data fallback for E*Trade users
  const TD = 'https://api.twelvedata.com';
  const r = await fetch(`${TD}/time_series?symbol=${sym}&interval=${cfg.interval}&outputsize=${cfg.lookback||90}&order=ASC&apikey=${cfg.td_key}`);
  const j = await r.json();
  if (j.status === 'error') throw new Error('TD: ' + j.message);
  if (!j.values?.length) throw new Error('TD: no data for ' + sym);
  return j.values.map(v => ({ date: v.datetime, open: +v.open, high: +v.high, low: +v.low, close: +v.close, volume: +(v.volume||0) }));
}

async function fetchQuote(sym, cfg, token) {
  try {
    if (cfg.broker === 'schwab' && token) {
      return schwabFetchQuote(sym, token);
    }
    const TD = 'https://api.twelvedata.com';
    const r = await fetch(`${TD}/quote?symbol=${sym}&apikey=${cfg.td_key}`);
    const j = await r.json();
    if (j.status === 'error') return null;
    return { price: +(j.close||j.price||0), changePct: +(j.percent_change||0) };
  } catch (e) { return null; }
}

// ═══════════════════════════════════════════════════════════════
// UNIFIED ORDER PLACEMENT + BALANCE
// ═══════════════════════════════════════════════════════════════
async function getAvailableBalance(cfg, token, dbBalance) {
  if (cfg.sandbox) return dbBalance;
  if (cfg.broker === 'schwab' && cfg.schwab_account_hash && token) {
    const real = await schwabGetBalance(token, cfg.schwab_account_hash);
    if (real !== null) {
      await addLog('info', `Real Schwab balance: $${(+real).toFixed(2)} (DB: $${dbBalance.toFixed(2)})`);
      return +real;
    }
    await addLog('warn', 'Schwab balance unavailable — using DB balance');
  }
  return dbBalance;
}

async function placeOrder(sym, action, shares, price, cfg, token) {
  if (cfg.sandbox) {
    await addLog('skip', `[SANDBOX] ${action} ${shares}sh ${sym} @ $${price.toFixed(2)} — not sent`);
    await sendPush(cfg, `AutoTrader — [SANDBOX] ${action}`, `${action} ${shares}sh ${sym} @ $${price.toFixed(2)}`);
    return { success: true, sandboxed: true };
  }
  if (cfg.broker === 'schwab') {
    if (!token || !cfg.schwab_account_hash) { await addLog('err', 'Schwab not authorized — visit /schwab/auth'); return { success: false }; }
    const res = await schwabPlaceOrder(sym, action, shares, price, token, cfg.schwab_account_hash);
    if (res.success) await sendPush(cfg, `AutoTrader — ${action} Executed`, `${action} ${shares}sh ${sym} @ ~$${price.toFixed(2)}`);
    return res;
  }
  // E*Trade
  if (!cfg.e_key || !cfg.e_token || !cfg.e_account) { await addLog('err', 'E*Trade tokens missing'); return { success: false }; }
  const url = `https://api.etrade.com/v1/accounts/${cfg.e_account}/orders/place`;
  const body = { PlaceOrderRequest: { orderType:'EQ', clientOrderId:`BOT-${Date.now()}`, Order:[{Instrument:[{Product:{securityType:'EQ',symbol:sym},orderAction:action,quantityType:'QUANTITY',quantity:shares}],orderTerm:'GOOD_FOR_DAY',marketSession:'REGULAR',priceType:'MARKET'}] } };
  try {
    const r = await fetch(url, { method:'POST', credentials:'omit', headers:{Authorization:makeOAuthHeader('POST',url,cfg),'Content-Type':'application/json'}, body:JSON.stringify(body) });
    const j = await r.json();
    if (r.status === 401) { await sendPush(cfg,'AutoTrader — Trade Failed',`${action} order failed — session expired.`); return { success:false, expired:true }; }
    const orderId = j?.PlaceOrderResponse?.OrderIds?.[0]?.orderId;
    if (orderId) { await addLog('buy',`E*Trade order confirmed — orderId: ${orderId}`); await sendPush(cfg,`AutoTrader — ${action} Executed`,`${action} ${shares}sh ${sym} @ $${price.toFixed(2)}`); return { success:true, orderId }; }
    await addLog('err',`E*Trade order failed: ${JSON.stringify(j)}`); return { success:false };
  } catch(e) { await addLog('err','E*Trade error: '+e.message); return { success:false }; }
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
RMS=${ind.rms?.toFixed(3)}% ATR=$${ind.atr?.toFixed(2)} SLOPE=${ind.slp?.toFixed(5)} MOM=${ind.mom?.toFixed(2)}%
STRATEGY: target +1% per trade, two-phase stop: fixed 1.2% then trailing 0.5% after +1% hit.
Return ONLY: {"grade":"B","score":72,"action":"BUY","confidence":70,"reason":"one sentence","target":${(ind.px*1.012).toFixed(2)},"stop":${(ind.px*.988).toFixed(2)},"strongest":"MACD","avoid":false}`;
  try{const r=await fetch('https://api.anthropic.com/v1/messages',{method:'POST',headers:{'Content-Type':'application/json','x-api-key':antKey,'anthropic-version':'2023-06-01'},body:JSON.stringify({model:'claude-sonnet-4-20250514',max_tokens:200,messages:[{role:'user',content:prompt}]})});if(!r.ok)return null;const d=await r.json();return JSON.parse(d.content.map(c=>c.text||'').join('').replace(/```json|```/g,'').trim());}catch(e){await addLog('err','Claude: '+e.message);return null;}
}

// ─── COEFFICIENTS ────────────────────────────────────────────
function updateCoefs(coefs,indSnap,result){const clamp=v=>+Math.max(0.05,Math.min(3,v)).toFixed(3);const win=result==='WIN';const out={...coefs};Object.keys(out).forEach(k=>{const s=indSnap?.[k];if(!s)return;const correct=(win&&s.bull===true)||(!win&&s.bull===false);out[k]=clamp(out[k]+(s.bull===null?0:correct?0.1:-0.06));});return out;}

// ─── TWO-PHASE STOP LOSS ─────────────────────────────────────
async function monitorPosition(state,cfg,token){
  const t=state.open_trade;if(!t)return;
  let px=t.entry_price;
  try{const q=await fetchQuote(t.symbol,cfg,token);if(q?.price>0)px=q.price;}catch(e){}
  const pnl=(px-t.entry_price)/t.entry_price*100;
  const days=countBizDays(new Date(t.entry_time),new Date());
  const highestPrice=Math.max(px,t.highest_price||t.entry_price);

  if(t.trailing_active){
    const trailStop=highestPrice*0.995;
    const currentPnl=(px-t.entry_price)/t.entry_price*100;
    await patchState({open_trade:{...t,highest_price:highestPrice,trail_stop:trailStop},status_text:`${t.symbol} TRAILING +${currentPnl.toFixed(2)}% | peak $${highestPrice.toFixed(2)} | trail $${trailStop.toFixed(2)}`});
    if(px<=trailStop||days>=3){
      const why=px<=trailStop?`trail stop hit @ $${trailStop.toFixed(2)}`:'3-day expiry';
      const result=currentPnl>=0?'WIN':'LOSS';
      await addLog('win',`${t.symbol} CLOSE — ${why} | +${currentPnl.toFixed(2)}% @ $${px.toFixed(2)}`);
      await closePosition(t,px,result,currentPnl,state,cfg,token);
    }
    return;
  }

  if(pnl>=1.0){
    const trailStop=px*0.995;
    await addLog('info',`${t.symbol} +1% HIT @ $${px.toFixed(2)} — switching to trailing stop`);
    await patchState({open_trade:{...t,trailing_active:true,highest_price:px,trail_stop:trailStop},status_text:`${t.symbol} trailing — peak $${px.toFixed(2)} trail $${trailStop.toFixed(2)}`});
    await sendPush(cfg,`AutoTrader — ${t.symbol} +1% Hit!`,`Trailing stop activated | Peak: $${px.toFixed(2)} | Trail: $${trailStop.toFixed(2)}`);
  }else if(px<=t.stop_price||days>=3){
    const why=px<=t.stop_price?`fixed stop @ $${t.stop_price?.toFixed(2)}`:'3-day expiry';
    const result=pnl>=0?'WIN':'LOSS';
    await addLog(result==='WIN'?'win':'loss',`${t.symbol} CLOSE — ${why} | ${pnl>=0?'+':''}${pnl.toFixed(2)}% @ $${px.toFixed(2)}`);
    await closePosition(t,px,result,pnl,state,cfg,token);
  }else{
    await patchState({open_trade:{...t,highest_price:highestPrice},status_text:`Holding ${t.symbol} ${pnl>=0?'+':''}${pnl.toFixed(2)}% | stop $${t.stop_price?.toFixed(2)} | day ${days}/3`});
  }
}

async function closePosition(t,exitPx,result,pnl,state,cfg,token){
  const res=await placeOrder(t.symbol,'SELL',t.shares,exitPx,cfg,token);
  if(!res.success&&!res.sandboxed)return;
  const newCoefs=updateCoefs(state.coefs,t.ind_snapshot,result);
  const newBal=state.balance*(1+pnl/100);
  if(t.db_id)await updateTrade(t.db_id,{exit_price:exitPx,result,pnl:+pnl.toFixed(3),exit_time:new Date().toISOString()});
  await patchState({balance:newBal,open_trade:null,last_trade_date:new Date().toISOString(),coefs:newCoefs,status_text:`SOLD ${t.symbol} ${pnl>=0?'+':''}${pnl.toFixed(2)}%`});
  await sendPush(cfg,`AutoTrader — ${result} ${pnl>=0?'+':''}${pnl.toFixed(2)}%`,`${t.symbol} | ${pnl>=0?'+':''}${pnl.toFixed(2)}% | Balance: $${newBal.toFixed(2)}`);
  await sendTradeEmail(cfg, result, { symbol: t.symbol, shares: t.shares, entryPrice: t.entry_price, exitPrice: exitPx, pnl, balance: newBal, reason: result === 'WIN' ? 'Target reached or trail stop triggered' : 'Stop loss triggered', grade: t.grade, score: t.score });
}

// ─── SCAN CYCLE ──────────────────────────────────────────────
let scanning=false;
async function scanCycle(){
  if(scanning)return;scanning=true;
  try{
    const[state,cfg]=await Promise.all([getState(),getConfig()]);
    if(!state?.running){scanning=false;return;}

    // Get fresh Schwab token if using Schwab
    let token=null;
    if(cfg.broker==='schwab'){
      token=await ensureSchwabToken(cfg);
      if(!token&&!cfg.sandbox){await patchState({status_text:'Schwab token expired — re-authorization needed'});scanning=false;return;}
    }else if(!cfg.sandbox){
      await checkAndRenewEtrade();
    }

    // Check subscription is active before scanning
    const subOk = await checkSubscription(cfg);
    if (!subOk) {
      await patchState({ status_text: 'Subscription inactive — visit autotrader-ruby.vercel.app to resubscribe', running: false });
      scanning = false; return;
    }

    if(state.open_trade){await monitorPosition(state,cfg,token);scanning=false;return;}
    if(!isMarketOpen()){await patchState({status_text:'Market closed — bot idle'});scanning=false;return;}
    if(!clearOk(state.last_trade_date)){
      const next=bizAdd(new Date(state.last_trade_date),CLEARING_DAYS);
      await patchState({status_text:`T+${CLEARING_DAYS} clearing lock — next trade: ${next.toLocaleDateString('en-US',{month:'short',day:'numeric'})}`});
      scanning=false;return;
    }

    const watchlist=(cfg.watchlist||'').split(',').map(s=>s.trim()).filter(Boolean);
    if(!watchlist.length){await patchState({status_text:'Watchlist empty — screener runs at 9 AM ET'});scanning=false;return;}

    const idx=(state.scan_idx||0)%watchlist.length;
    const sym=watchlist[idx];
    const newIdx=(state.scan_idx||0)+1;

    const broker=cfg.broker||'etrade';
    await addLog('scan',`Scanning ${sym} [${idx+1}/${watchlist.length}] | ${broker} | ${cfg.sandbox?'sandbox':'LIVE'}`);
    await patchState({scan_idx:newIdx,status_text:`Scanning ${sym}...`});

    // Fetch candles — Schwab API or Twelve Data depending on broker
    const candles=await fetchCandles(sym,cfg,token);

    // Calculate VWAP from candles (both brokers)
    const vwapArr=calcVwap(candles);
    const liveVwap=vwapArr[vwapArr.length-1];

    const ind=analyze(candles,liveVwap,state.coefs);
    await patchState({last_analysis:{symbol:sym,...ind}});

    // Get live quote
    let px=ind.px;
    try{const q=await fetchQuote(sym,cfg,token);if(q?.price>0)px=q.price;}catch(e){}

    // Claude grade
    let grade=null;
    if(cfg.ant_key){grade=await aiGrade(sym,ind,cfg.ant_key);if(grade)await patchState({last_grade:grade});}

    const GR=['A','B','C','D','F'];
    const scoreOk=ind.comp>=cfg.min_score;
    const gOk=grade?GR.indexOf(grade.grade)<=GR.indexOf(cfg.min_grade)&&!grade.avoid:ind.comp>=(cfg.min_score+8);
    const isBuy=grade?grade.action==='BUY':ind.comp>=cfg.min_score;

    if(scoreOk&&gOk&&isBuy){
      // Get real balance from Schwab in live mode, DB balance in sandbox
      const availBalance=await getAvailableBalance(cfg,token,state.balance);
      const shares=Math.max(1,Math.floor(availBalance*(cfg.pos_pct/100)/px));
      const target=grade?.target?+grade.target:+(px*1.012).toFixed(2);
      const stop=grade?.stop?+grade.stop:+(px*0.988).toFixed(2);

      await addLog('buy',`SIGNAL ${sym} | Grade=${grade?.grade||'rule'} Score=${ind.comp} | BUY ${shares}sh @ $${px.toFixed(2)} | avail=$${availBalance.toFixed(2)} | stop $${stop.toFixed(2)} → trail 0.5% after +1%`);
      const res=await placeOrder(sym,'BUY',shares,px,cfg,token);
      if(res.success||res.sandboxed){
        const rows=await addTrade({symbol:sym,action:'BUY',shares,entry_price:px,target,stop_price:stop,result:'OPEN',grade:grade?.grade||'--',score:ind.comp,entry_time:new Date().toISOString(),ind_snapshot:ind.sigs});
        const dbId=Array.isArray(rows)?rows[0]?.id:null;
        await patchState({
          open_trade:{db_id:dbId,symbol:sym,shares,entry_price:px,target,stop_price:stop,entry_time:new Date().toISOString(),grade:grade?.grade||'--',score:ind.comp,ind_snapshot:ind.sigs,trailing_active:false,highest_price:px,trail_stop:null},
          last_trade_date:new Date().toISOString(),
          status_text:`BOUGHT ${shares}sh ${sym} @ $${px.toFixed(2)} | stop $${stop.toFixed(2)} → trails at +1%`
        });
        await sendTradeEmail(cfg, 'BUY', { symbol: sym, shares, entryPrice: px, grade: grade?.grade || '--', score: ind.comp, reason: grade?.reason || 'Signal threshold met' });
      }
    }else{
      const why=!scoreOk?`score ${ind.comp}<${cfg.min_score}`:!gOk?`grade ${grade?.grade} below ${cfg.min_grade}`:`signal=${grade?.action||'neutral'}`;
      await addLog('skip',`${sym} — no trade (${why})`);
      await patchState({status_text:`${sym} skipped — ${why}`});
    }
  }catch(err){await addLog('err',err.message);try{await patchState({status_text:'Error: '+err.message});}catch(e){}}
  scanning=false;
}

// ─── BOT LOOP ────────────────────────────────────────────────
let botInterval=null;
async function startLoop(){
  const cfg=await getConfig();
  const sec=cfg?.cycle_seconds||60;
  const broker=cfg?.broker||'etrade';
  await addLog('info',`Bot started — ${sec}s | broker=${broker} | data=${broker==='schwab'?'Schwab API':'Twelve Data'} | clearing=${CLEARING_DAYS===0?'OFF':'T+'+CLEARING_DAYS} | ${cfg?.sandbox?'SANDBOX':'LIVE'}`);
  await patchState({status_text:'Bot started'});
  botInterval=setInterval(async()=>{
    const state=await getState();
    if(!state?.running){clearInterval(botInterval);botInterval=null;await addLog('info','Bot stopped');await patchState({status_text:'Stopped'});return;}
    scanCycle();
  },sec*1000);
  scanCycle();
}

// ─── ROUTES ──────────────────────────────────────────────────
app.get('/',(req,res)=>res.json({status:'AutoTrader running',time:new Date().toISOString(),loopActive:!!botInterval}));
app.get('/health',(req,res)=>res.json({ok:true}));
app.post('/bot/start',async(req,res)=>{await patchState({running:true});if(!botInterval)await startLoop();res.json({ok:true});});
app.post('/bot/stop',async(req,res)=>{await patchState({running:false});res.json({ok:true});});
app.post('/screener/run',async(req,res)=>{res.json({ok:true});try{await runScreener();}catch(e){await addLog('err','Screener: '+e.message);}});

// ─── STARTUP ─────────────────────────────────────────────────
const PORT=process.env.PORT||3000;
app.listen(PORT,async()=>{
  console.log(`AutoTrader v6.0 listening on port ${PORT}`);
  console.log(`Stripe: ${STRIPE_SECRET ? 'configured' : 'NOT configured (payments disabled)'}`);
  startScreenerScheduler();
  try{const state=await getState();if(state?.running){console.log('Resuming bot...');await startLoop();}else console.log('Bot stopped — waiting for START');}
  catch(e){console.error('Startup error:',e.message);}
});
