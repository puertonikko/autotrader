// ============================================================
// AutoTrader Bot — Railway Server v5.0
// Supports both E*Trade and Charles Schwab
// - Sandbox mode: uses database balance for position sizing
// - Live mode: fetches real account balance before every trade
// - Schwab OAuth 2.0 with auto refresh (no daily PIN needed)
// - E*Trade OAuth 1.0a (legacy, kept for existing users)
// - Two-phase stop: fixed 1.2% → trailing 0.5% after +1%
// - T+1 clearing (disabled for testing, enable for production)
// ============================================================
import express from 'express';
import crypto from 'crypto';
import { runScreener, startScreenerScheduler } from './screener.js';

const app = express();
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
        app_id: cfg.onesignal_app_id,
        included_segments: ['All'],
        headings: { en: title },
        contents: { en: message },
        url: url || undefined,
        ttl: 3600, priority: 10
      })
    });
  } catch (e) { console.log('[PUSH] Failed:', e.message); }
}

// ─── CLEARING LOCK ───────────────────────────────────────────
// CLEARING_DAYS = 0 → disabled (testing)
// CLEARING_DAYS = 1 → T+1 (production — all US brokers since May 2024)
const CLEARING_DAYS = 0; // change to 1 for production
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
// CHARLES SCHWAB — OAuth 2.0
// Much better than E*Trade: auto-refreshing tokens, no daily PIN
// ═══════════════════════════════════════════════════════════════
const SCHWAB_BASE  = 'https://api.schwabapi.com';
const SCHWAB_AUTH  = 'https://api.schwabapi.com/v1/oauth/authorize';
const SCHWAB_TOKEN = 'https://api.schwabapi.com/v1/oauth/token';

function schwabBasicAuth(cfg) {
  return 'Basic ' + Buffer.from(`${cfg.schwab_client_id}:${cfg.schwab_client_secret}`).toString('base64');
}

// Refresh Schwab access token using refresh token (auto, no user needed)
async function refreshSchwabToken(cfg) {
  if (!cfg.schwab_refresh_token) return false;
  try {
    const r = await fetch(SCHWAB_TOKEN, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded', Authorization: schwabBasicAuth(cfg) },
      body: new URLSearchParams({ grant_type: 'refresh_token', refresh_token: cfg.schwab_refresh_token })
    });
    const j = await r.json();
    if (!j.access_token) { await addLog('err', 'Schwab token refresh failed: ' + JSON.stringify(j)); return false; }
    await patchConfig({
      schwab_access_token:  j.access_token,
      schwab_refresh_token: j.refresh_token || cfg.schwab_refresh_token,
      schwab_token_expiry:  new Date(Date.now() + (j.expires_in || 1800) * 1000).toISOString()
    });
    await addLog('info', 'Schwab access token refreshed automatically');
    return true;
  } catch (e) { await addLog('err', 'Schwab refresh error: ' + e.message); return false; }
}

// Ensure Schwab token is fresh before API calls
async function ensureSchwabToken(cfg) {
  if (!cfg.schwab_access_token) return false;
  // Refresh if expiring within 5 minutes
  const expiry = cfg.schwab_token_expiry ? new Date(cfg.schwab_token_expiry) : null;
  if (!expiry || expiry - new Date() < 5 * 60 * 1000) {
    const ok = await refreshSchwabToken(cfg);
    if (!ok) {
      const authUrl = `https://${process.env.RAILWAY_PUBLIC_DOMAIN || ''}/schwab/auth`;
      await sendPush(cfg, 'AutoTrader — Schwab Re-auth Needed', 'Tap to reconnect your Schwab account.', authUrl);
      return false;
    }
    return true;
  }
  return true;
}

// Get real Schwab account balance
async function getSchwabBalance(cfg) {
  const fresh = await ensureSchwabToken(await getConfig());
  if (!fresh) return null;
  const freshCfg = await getConfig();
  try {
    const r = await fetch(`${SCHWAB_BASE}/trader/v1/accounts/${freshCfg.schwab_account_hash}?fields=balances`, {
      headers: { Authorization: `Bearer ${freshCfg.schwab_access_token}` }
    });
    const j = await r.json();
    // Cash available for trading
    const cash = j?.securitiesAccount?.currentBalances?.cashAvailableForTrading
               || j?.securitiesAccount?.currentBalances?.availableFunds
               || null;
    if (cash !== null) await addLog('info', `Schwab balance: $${(+cash).toFixed(2)}`);
    return cash !== null ? +cash : null;
  } catch (e) { await addLog('err', 'Schwab balance fetch error: ' + e.message); return null; }
}

// Place Schwab order
async function placeSchwabOrder(sym, action, shares, price, cfg) {
  const fresh = await ensureSchwabToken(cfg);
  if (!fresh) return { success: false };
  const freshCfg = await getConfig();
  const body = {
    orderType:    'MARKET',
    session:      'NORMAL',
    duration:     'DAY',
    orderStrategyType: 'SINGLE',
    orderLegCollection: [{
      instruction: action === 'BUY' ? 'BUY' : 'SELL',
      quantity: shares,
      instrument: { symbol: sym, assetType: 'EQUITY' }
    }]
  };
  try {
    const r = await fetch(`${SCHWAB_BASE}/trader/v1/accounts/${freshCfg.schwab_account_hash}/orders`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${freshCfg.schwab_access_token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });
    if (r.status === 201) {
      const orderId = r.headers.get('location')?.split('/').pop() || 'unknown';
      await addLog('buy', `Schwab order placed — orderId: ${orderId}`);
      await sendPush(freshCfg, `AutoTrader — ${action} Executed`, `${action} ${shares}sh ${sym} @ ~$${price.toFixed(2)}`);
      return { success: true, orderId };
    }
    const err = await r.text();
    await addLog('err', `Schwab order failed (${r.status}): ${err}`);
    return { success: false };
  } catch (e) { await addLog('err', 'Schwab order error: ' + e.message); return { success: false }; }
}

// Schwab OAuth flow — visit /schwab/auth once to connect
app.get('/schwab/auth', async (req, res) => {
  const cfg = await getConfig();
  if (!cfg?.schwab_client_id || !cfg?.schwab_client_secret) {
    return res.send(`<html><body style="font-family:monospace;background:#050a0f;color:#b8d4ee;padding:30px">
      <h2 style="color:#f5a623">Missing Schwab Keys</h2>
      <p>Add your Schwab Client ID and Client Secret in your phone app Config first.</p>
      <p>Get them at <a href="https://developer.schwab.com" style="color:#2d9cff">developer.schwab.com</a></p>
    </body></html>`);
  }
  const redirectUri = `https://${process.env.RAILWAY_PUBLIC_DOMAIN || req.hostname}/schwab/callback`;
  const authUrl = `${SCHWAB_AUTH}?client_id=${cfg.schwab_client_id}&redirect_uri=${encodeURIComponent(redirectUri)}`;
  res.send(`<!DOCTYPE html><html><head><meta name="viewport" content="width=device-width,initial-scale=1">
  <style>body{font-family:monospace;background:#050a0f;color:#b8d4ee;padding:20px;max-width:540px;margin:0 auto}h2{color:#f5a623}.box{background:#091422;border:1px solid #162d47;border-radius:6px;padding:16px;margin:12px 0}.btn{display:block;background:#00e891;color:#050a0f;padding:16px;border-radius:6px;text-align:center;text-decoration:none;font-weight:700;font-size:16px;margin-top:12px}.note{font-size:11px;color:#4a7090;margin-top:8px;line-height:1.6}</style>
  </head><body>
  <h2>Connect Charles Schwab</h2>
  <div class="box">
    <p>Tap the button below to log in to Schwab and authorize AutoTrader.</p>
    <p>Unlike E*Trade, this only needs to be done <strong style="color:#00e891">once</strong> — tokens refresh automatically.</p>
    <a class="btn" href="${authUrl}">Connect to Charles Schwab →</a>
    <p class="note">You will be redirected to Schwab's login page. After approving, you will be redirected back here automatically.</p>
  </div>
  </body></html>`);
});

// Schwab OAuth callback — Schwab redirects here after user approves
app.get('/schwab/callback', async (req, res) => {
  const { code, error } = req.query;
  if (error || !code) {
    return res.send(`<html><body style="font-family:monospace;background:#050a0f;color:#f0364a;padding:30px"><h2>Authorization failed</h2><p>${error || 'No code received'}</p></body></html>`);
  }
  const cfg = await getConfig();
  const redirectUri = `https://${process.env.RAILWAY_PUBLIC_DOMAIN || req.hostname}/schwab/callback`;
  try {
    // Exchange code for tokens
    const r = await fetch(SCHWAB_TOKEN, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded', Authorization: schwabBasicAuth(cfg) },
      body: new URLSearchParams({ grant_type: 'authorization_code', code, redirect_uri: redirectUri })
    });
    const j = await r.json();
    if (!j.access_token) {
      return res.send(`<html><body style="font-family:monospace;background:#050a0f;color:#f0364a;padding:30px"><h2>Token exchange failed</h2><pre>${JSON.stringify(j)}</pre></body></html>`);
    }

    // Save tokens
    await patchConfig({
      schwab_access_token:  j.access_token,
      schwab_refresh_token: j.refresh_token,
      schwab_token_expiry:  new Date(Date.now() + (j.expires_in || 1800) * 1000).toISOString(),
      broker: 'schwab'
    });

    // Auto-fetch account hash
    let accountHash = '';
    try {
      const ar = await fetch(`${SCHWAB_BASE}/trader/v1/accounts`, {
        headers: { Authorization: `Bearer ${j.access_token}` }
      });
      const accounts = await ar.json();
      accountHash = accounts?.[0]?.hashValue || '';
      if (accountHash) await patchConfig({ schwab_account_hash: accountHash });
    } catch (e) {}

    await addLog('info', 'Schwab OAuth complete — tokens saved' + (accountHash ? `, account hash: ${accountHash}` : ''));
    await patchState({ status_text: 'Schwab connected — bot ready' });
    const freshCfg = await getConfig();
    await sendPush(freshCfg, 'AutoTrader — Schwab Connected', 'Charles Schwab account linked. Bot is ready to trade.');

    res.send(`<!DOCTYPE html><html><head><meta name="viewport" content="width=device-width,initial-scale=1">
    <style>body{font-family:monospace;background:#050a0f;color:#b8d4ee;padding:30px;max-width:540px;margin:0 auto}h2{color:#00e891}.box{background:#0a2a1a;border:1px solid #00e891;border-radius:6px;padding:20px}</style>
    </head><body>
    <div class="box">
      <h2>Schwab Connected!</h2>
      <p>Your Charles Schwab account has been linked successfully.</p>
      ${accountHash ? `<p style="color:#4a7090">Account hash: ${accountHash}</p>` : ''}
      <p style="margin-top:16px">Tokens refresh automatically — you will never need to do this again unless you revoke access.</p>
      <p style="margin-top:12px;color:#f5a623">You can close this page and return to the AutoTrader app.</p>
    </div>
    </body></html>`);
  } catch (e) {
    res.send(`<html><body style="font-family:monospace;background:#050a0f;color:#f0364a;padding:30px"><h2>Error</h2><pre>${e.message}</pre></body></html>`);
  }
});

// ═══════════════════════════════════════════════════════════════
// E*TRADE — OAuth 1.0a (legacy, kept for existing users)
// ═══════════════════════════════════════════════════════════════
function oauthSign(method, url, params, consumerSecret, tokenSecret = '') {
  const sorted = Object.keys(params).sort().map(k => `${encodeURIComponent(k)}=${encodeURIComponent(params[k])}`).join('&');
  const base   = `${method}&${encodeURIComponent(url)}&${encodeURIComponent(sorted)}`;
  const sigKey = `${encodeURIComponent(consumerSecret)}&${encodeURIComponent(tokenSecret)}`;
  return crypto.createHmac('sha1', sigKey).update(base).digest('base64');
}
function makeOAuthHeader(method, url, cfg, extra = {}) {
  const p = { oauth_consumer_key: cfg.e_key, oauth_nonce: crypto.randomBytes(16).toString('hex'), oauth_signature_method: 'HMAC-SHA1', oauth_timestamp: Math.floor(Date.now()/1000).toString(), oauth_version: '1.0', ...extra };
  if (cfg.e_token && !extra.oauth_callback) p.oauth_token = cfg.e_token;
  p.oauth_signature = oauthSign(method, url, p, cfg.e_secret, cfg.e_token_secret || '');
  return 'OAuth ' + Object.keys(p).sort().map(k => `${encodeURIComponent(k)}="${encodeURIComponent(p[k])}"`).join(', ');
}
function parseQS(text) {
  const r = {}; text.split('&').forEach(pair => { const [k,v]=pair.split('='); if(k)r[decodeURIComponent(k)]=decodeURIComponent(v||''); }); return r;
}
async function etradeOAuthFetch(url, header) { return fetch(url, { headers: { Authorization: header }, credentials: 'omit' }); }

let lastRenewal = null, renewalWarned = false;
const RENEW_MS = 90*60*1000, WARN_MS = 30*60*1000;
async function renewEtradeToken(cfg) {
  if (!cfg.e_key || !cfg.e_token) return false;
  const base = cfg.sandbox ? 'https://apisb.etrade.com' : 'https://api.etrade.com';
  try {
    const r = await etradeOAuthFetch(`${base}/oauth/renew_access_token`, makeOAuthHeader('GET', `${base}/oauth/renew_access_token`, cfg));
    const text = await r.text();
    if (r.ok && text.includes('renewed')) { lastRenewal = new Date(); renewalWarned = false; await addLog('info', 'E*Trade token renewed'); return true; }
    return false;
  } catch (e) { return false; }
}
async function checkAndRenewEtrade() {
  const cfg = await getConfig();
  if (!cfg?.e_token || cfg?.sandbox) return;
  const now = new Date();
  if (!lastRenewal || (now - lastRenewal) >= RENEW_MS) {
    const ok = await renewEtradeToken(cfg);
    if (!ok && (!renewalWarned || (now - renewalWarned) >= WARN_MS)) {
      renewalWarned = now;
      const authUrl = `https://${process.env.RAILWAY_PUBLIC_DOMAIN || ''}/etrade/auth`;
      await sendPush(cfg, 'AutoTrader — E*Trade Re-auth Required', 'Tap to re-authorize your E*Trade session.', authUrl);
      await patchState({ status_text: 'E*Trade token expired — re-authorization required' });
    }
  }
}

// E*Trade OAuth flow
app.get('/etrade/auth', async (req, res) => {
  const cfg = await getConfig();
  if (!cfg?.e_key || !cfg?.e_secret) return res.send(`<html><body style="font-family:monospace;background:#050a0f;color:#b8d4ee;padding:30px"><h2 style="color:#f5a623">Missing E*Trade Keys</h2><p>Add Consumer Key and Secret in Config first.</p></body></html>`);
  const sandbox = cfg.sandbox;
  const apiBase = sandbox ? 'https://apisb.etrade.com' : 'https://api.etrade.com';
  try {
    const tmpCfg = { ...cfg, e_token: '', e_token_secret: '' };
    const r = await etradeOAuthFetch(`${apiBase}/oauth/request_token`, makeOAuthHeader('GET', `${apiBase}/oauth/request_token`, tmpCfg, { oauth_callback: 'oob' }));
    const text = await r.text(); const p = parseQS(text);
    if (!p.oauth_token) return res.send(`<html><body style="font-family:monospace;background:#050a0f;color:#f0364a;padding:30px"><h2>Error</h2><pre>${text}</pre><p>HTTP: ${r.status}</p></body></html>`);
    const authUrl = `https://us.etrade.com/e/t/etws/authorize?key=${cfg.e_key}&token=${p.oauth_token}`;
    res.send(`<!DOCTYPE html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"><style>body{font-family:monospace;background:#050a0f;color:#b8d4ee;padding:20px;max-width:540px;margin:0 auto}h2{color:#f5a623}h3{color:#00e891;font-size:14px;margin:0 0 8px}.box{background:#091422;border:1px solid #162d47;border-radius:6px;padding:16px;margin:12px 0}.btn-link{display:block;background:#162d47;border:1px solid #2d9cff;color:#2d9cff;padding:14px;border-radius:5px;text-align:center;text-decoration:none;margin-top:10px;font-weight:700;font-size:15px}input{width:100%;background:#0d1c30;border:1px solid #1c3a59;border-radius:4px;padding:12px;color:#b8d4ee;font-family:monospace;font-size:20px;box-sizing:border-box;margin:8px 0;letter-spacing:.15em;text-transform:uppercase}button{background:#00e891;color:#050a0f;border:none;border-radius:5px;padding:14px;font-family:monospace;font-size:15px;font-weight:700;cursor:pointer;width:100%;margin-top:8px}.note{font-size:11px;color:#4a7090;margin-top:6px;line-height:1.5}#msg{margin-top:14px;padding:14px;border-radius:5px;display:none}</style></head><body>
    <h2>E*Trade Authorization</h2>
    <div style="background:var(--ambd);display:inline-block;padding:3px 10px;border-radius:3px;font-size:11px;font-weight:700;margin-bottom:14px;background:rgba(245,166,35,.15);color:#f5a623">${sandbox?'SANDBOX MODE':'LIVE TRADING'}</div>
    <div class="box"><h3>Step 1 — Authorize on E*Trade</h3><p>Tap the button, log in, approve the bot. Copy the <strong style="color:#00e891">PIN</strong> immediately.</p><a class="btn-link" href="${authUrl}" target="_blank">Tap to Authorize on E*Trade</a><p class="note">Come back here immediately after you see the PIN.</p></div>
    <div class="box"><h3>Step 2 — Enter PIN immediately</h3><input type="text" id="pin" placeholder="XXXXX" inputmode="text" maxlength="10" autocomplete="off" autocorrect="off" spellcheck="false"/><button onclick="go()">Complete Authorization</button><p class="note">Tokens save permanently to Supabase.</p></div>
    <div id="msg"></div>
    <script>document.getElementById('pin').focus();async function go(){const pin=document.getElementById('pin').value.trim().toUpperCase();if(!pin){alert('Enter the PIN first');return;}document.getElementById('msg').style.cssText='display:block;background:#162d47;padding:14px;border-radius:5px;color:#f5a623';document.getElementById('msg').innerHTML='Saving tokens...';try{const r=await fetch('/etrade/callback?rt=${p.oauth_token}&pin='+encodeURIComponent(pin),{credentials:'omit'});const j=await r.json();const el=document.getElementById('msg');if(j.ok){el.style.cssText='display:block;background:#0a2a1a;border:1px solid #00e891;border-radius:5px;padding:16px';el.innerHTML='<p style="color:#00e891;font-size:16px;margin:0">Authorization complete! Close this page.</p>';}else{el.style.cssText='display:block;background:#2a0a0a;border:1px solid #f0364a;border-radius:5px;padding:16px';el.innerHTML='<p style="color:#f0364a;margin:0">Error: '+j.error+'<br><br>Tap E*Trade button again for a fresh PIN.</p>';}}catch(e){document.getElementById('msg').innerHTML='Network error — try again';}}document.getElementById('pin').addEventListener('keydown',e=>{if(e.key==='Enter')go();});</script></body></html>`);
  } catch (e) { res.send(`<html><body style="font-family:monospace;background:#050a0f;color:#f0364a;padding:30px"><h2>Error</h2><pre>${e.message}</pre></body></html>`); }
});

app.get('/etrade/callback', async (req, res) => {
  const { rt, pin } = req.query;
  if (!rt || !pin) return res.json({ ok: false, error: 'Missing token or PIN' });
  const cfg = await getConfig();
  const apiBase = cfg.sandbox ? 'https://apisb.etrade.com' : 'https://api.etrade.com';
  try {
    const tmpCfg = { ...cfg, e_token: rt, e_token_secret: '' };
    const r = await etradeOAuthFetch(`${apiBase}/oauth/access_token`, makeOAuthHeader('GET', `${apiBase}/oauth/access_token`, tmpCfg, { oauth_verifier: pin.toUpperCase() }));
    const text = await r.text(); const p = parseQS(text);
    if (!p.oauth_token) return res.json({ ok: false, error: `PIN rejected (HTTP ${r.status}). Try again.` });
    await patchConfig({ e_token: p.oauth_token, e_token_secret: p.oauth_token_secret });
    lastRenewal = new Date(); renewalWarned = false;
    await addLog('info', 'E*Trade OAuth complete — tokens saved');
    await patchState({ status_text: 'E*Trade authorized — bot ready' });
    const freshCfg = await getConfig();
    await sendPush(freshCfg, 'AutoTrader — E*Trade Authorized', 'E*Trade connected. Bot is active.');
    res.json({ ok: true });
  } catch (e) { res.json({ ok: false, error: e.message }); }
});

// ═══════════════════════════════════════════════════════════════
// UNIFIED ORDER PLACEMENT
// Sandbox: logs only, uses DB balance
// Live Schwab: fetches real balance, places real order
// Live E*Trade: places real order (balance from DB until Schwab)
// ═══════════════════════════════════════════════════════════════
async function getAvailableBalance(cfg, dbBalance) {
  // Sandbox → always use database balance (for testing)
  if (cfg.sandbox) return dbBalance;

  // Live Schwab → fetch real account balance
  if (cfg.broker === 'schwab' && cfg.schwab_account_hash) {
    const realBalance = await getSchwabBalance(cfg);
    if (realBalance !== null) {
      await addLog('info', `Using real Schwab balance: $${realBalance.toFixed(2)} (DB was: $${dbBalance.toFixed(2)})`);
      return realBalance;
    }
    await addLog('warn', 'Could not fetch Schwab balance — using DB balance as fallback');
    return dbBalance;
  }

  // Live E*Trade → use DB balance (real balance check not yet implemented)
  return dbBalance;
}

async function placeOrder(sym, action, shares, price, cfg) {
  // Sandbox mode — log only, no real orders
  if (cfg.sandbox) {
    await addLog('skip', `[SANDBOX] ${action} ${shares}sh ${sym} @ $${price.toFixed(2)} — not sent`);
    await sendPush(cfg, `AutoTrader — [SANDBOX] ${action}`, `${action} ${shares}sh ${sym} @ $${price.toFixed(2)} (sandbox, no real order)`);
    return { success: true, sandboxed: true };
  }

  // Live Schwab
  if (cfg.broker === 'schwab') {
    if (!cfg.schwab_access_token || !cfg.schwab_account_hash) {
      await addLog('err', 'Schwab not authorized — visit /schwab/auth');
      return { success: false };
    }
    return placeSchwabOrder(sym, action, shares, price, cfg);
  }

  // Live E*Trade
  if (!cfg.e_key || !cfg.e_token || !cfg.e_account) {
    await addLog('err', 'E*Trade tokens missing — visit /etrade/auth');
    return { success: false };
  }
  const url = `https://api.etrade.com/v1/accounts/${cfg.e_account}/orders/place`;
  const body = { PlaceOrderRequest: { orderType: 'EQ', clientOrderId: `BOT-${Date.now()}`, Order: [{ Instrument: [{ Product: { securityType: 'EQ', symbol: sym }, orderAction: action, quantityType: 'QUANTITY', quantity: shares }], orderTerm: 'GOOD_FOR_DAY', marketSession: 'REGULAR', priceType: 'MARKET' }] } };
  try {
    const r = await fetch(url, { method: 'POST', credentials: 'omit', headers: { Authorization: makeOAuthHeader('POST', url, cfg), 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    const j = await r.json();
    if (r.status === 401) { await sendPush(cfg, 'AutoTrader — Trade Failed', `${action} order for ${sym} failed — session expired.`); return { success: false, expired: true }; }
    const orderId = j?.PlaceOrderResponse?.OrderIds?.[0]?.orderId;
    if (orderId) { await addLog('buy', `E*Trade order confirmed — orderId: ${orderId}`); await sendPush(cfg, `AutoTrader — ${action} Executed`, `${action} ${shares}sh ${sym} @ $${price.toFixed(2)}`); return { success: true, orderId }; }
    await addLog('err', `E*Trade order failed: ${JSON.stringify(j)}`); return { success: false };
  } catch (e) { await addLog('err', `E*Trade error: ${e.message}`); return { success: false }; }
}

// ─── INDICATOR MATH ──────────────────────────────────────────
const sma = (a,p) => a.map((_,i)=>i<p-1?null:a.slice(i-p+1,i+1).reduce((s,v)=>s+v,0)/p);
function ema(a,p){const k=2/(p+1),r=new Array(a.length).fill(null);r[p-1]=a.slice(0,p).reduce((s,v)=>s+v,0)/p;for(let i=p;i<a.length;i++)r[i]=a[i]*k+r[i-1]*(1-k);return r;}
function rsi(c,p=14){const r=new Array(c.length).fill(null);let g=0,l=0;for(let i=1;i<=p;i++){const d=c[i]-c[i-1];d>0?g+=d:l-=d;}let ag=g/p,al=l/p;r[p]=100-100/(1+(al===0?1e10:ag/al));for(let i=p+1;i<c.length;i++){const d=c[i]-c[i-1];ag=(ag*(p-1)+(d>0?d:0))/p;al=(al*(p-1)+(d<0?-d:0))/p;r[i]=100-100/(1+(al===0?1e10:ag/al));}return r;}
function macd(c,f=12,s=26,sg=9){const ef=ema(c,f),es=ema(c,s);const ml=c.map((_,i)=>ef[i]!=null&&es[i]!=null?ef[i]-es[i]:null);const vals=ml.filter(v=>v!=null),off=ml.findIndex(v=>v!=null);const se=ema(vals,sg);const sl=new Array(ml.length).fill(null),hl=new Array(ml.length).fill(null);for(let i=0;i<se.length;i++){const x=off+i;if(se[i]!=null){sl[x]=se[i];hl[x]=ml[x]-se[i];}}return{ml,sl,hl};}
function bbands(c,p=20,m=2){const mid=sma(c,p),up=[],lo=[];for(let i=0;i<c.length;i++){if(mid[i]==null){up.push(null);lo.push(null);continue;}const sl=c.slice(i-p+1,i+1),mv=mid[i];const std=Math.sqrt(sl.reduce((a,v)=>a+(v-mv)**2,0)/p);up.push(mv+m*std);lo.push(mv-m*std);}return{up,mid,lo};}
function calcVwap(data){let cv=0,cq=0;return data.map(c=>{const tp=(c.high+c.low+c.close)/3;cv+=tp*c.volume;cq+=c.volume;return cq>0?cv/cq:tp;});}
function rmsVol(c,p=20){const r=new Array(c.length).fill(null);for(let i=p;i<c.length;i++){const rets=[];for(let j=i-p+1;j<=i;j++)rets.push((c[j]-c[j-1])/c[j-1]);r[i]=Math.sqrt(rets.reduce((s,v)=>s+v*v,0)/rets.length)*100;}return r;}
function calcATR(data,p=14){const tr=data.map((c,i)=>i===0?c.high-c.low:Math.max(c.high-c.low,Math.abs(c.high-data[i-1].close),Math.abs(c.low-data[i-1].close)));return sma(tr,p);}
function calcSlope(c,p=20){if(c.length<p)return 0;const s=c.slice(-p),n=s.length,sx=n*(n-1)/2,sx2=n*(n-1)*(2*n-1)/6,sy=s.reduce((a,v)=>a+v,0),sxy=s.reduce((a,v,i)=>a+i*v,0);return(n*sxy-sx*sy)/(n*sx2-sx*sx);}
function momentum(c,p=10){const n=c.length-1;return n<p?0:(c[n]-c[n-p])/c[n-p]*100;}

function analyze(data,liveVwap,coefs){
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

// ─── TWELVE DATA ─────────────────────────────────────────────
const TD='https://api.twelvedata.com';
async function fetchCandles(sym,interval,lookback,tdKey){const r=await fetch(`${TD}/time_series?symbol=${sym}&interval=${interval}&outputsize=${lookback}&order=ASC&apikey=${tdKey}`);const j=await r.json();if(j.status==='error')throw new Error('TD: '+j.message);if(!j.values?.length)throw new Error('TD: no data for '+sym);return j.values.map(v=>({date:v.datetime,open:+v.open,high:+v.high,low:+v.low,close:+v.close,volume:+(v.volume||0)}));}
async function fetchQuote(sym,tdKey){try{const r=await fetch(`${TD}/quote?symbol=${sym}&apikey=${tdKey}`);const j=await r.json();if(j.status==='error')return null;return{price:+(j.close||j.price||0),changePct:+(j.percent_change||0)};}catch(e){return null;}}
async function fetchVWAP(sym,interval,tdKey){if(interval==='1day')return null;try{const r=await fetch(`${TD}/vwap?symbol=${sym}&interval=${interval}&outputsize=1&apikey=${tdKey}`);const j=await r.json();if(j.status==='error')return null;return+(j.values?.[0]?.vwap||0)||null;}catch(e){return null;}}

// ─── CLAUDE GRADER ───────────────────────────────────────────
async function aiGrade(sym,ind,antKey){
  if(!antKey)return null;
  const prompt=`You are a quant signal grader for an autonomous trading bot. Return ONLY valid JSON.
TICKER:${sym} PRICE:$${ind.px.toFixed(2)} COMPOSITE:${ind.comp}/100
RSI=${ind.rsiV?.toFixed(2)} [${ind.sigs.RSI?.label}] MACD=${ind.mlV?.toFixed(4)} SIG=${ind.slV?.toFixed(4)} HIST=${ind.hlV?.toFixed(4)} [${ind.sigs.MACD?.label}]
BB_UP=$${ind.buV?.toFixed(2)} MID=$${ind.bmV?.toFixed(2)} LO=$${ind.blV?.toFixed(2)} PCT=${ind.bbPct}% [${ind.sigs.BB?.label}]
VWAP=$${ind.vwap?.toFixed(2)} [${ind.sigs.VWAP?.label}] SMA20=$${ind.s20?.toFixed(2)} SMA50=$${ind.s50?.toFixed(2)} [${ind.sigs.SMA?.label}]
EMA12=$${ind.e1?.toFixed(2)} EMA26=$${ind.e2?.toFixed(2)} [${ind.sigs.EMA?.label}]
RMS=${ind.rms?.toFixed(3)}% ATR=$${ind.atr?.toFixed(2)} SLOPE=${ind.slp?.toFixed(5)} MOM=${ind.mom?.toFixed(2)}%
STRATEGY: autonomous bot, target +1% per trade, two-phase stop: fixed 1.2% below entry then trailing 0.5% after +1% hit.
Return ONLY: {"grade":"B","score":72,"action":"BUY","confidence":70,"reason":"one sentence","target":${(ind.px*1.012).toFixed(2)},"stop":${(ind.px*.988).toFixed(2)},"strongest":"MACD","avoid":false}`;
  try{const r=await fetch('https://api.anthropic.com/v1/messages',{method:'POST',headers:{'Content-Type':'application/json','x-api-key':antKey,'anthropic-version':'2023-06-01'},body:JSON.stringify({model:'claude-sonnet-4-20250514',max_tokens:200,messages:[{role:'user',content:prompt}]})});if(!r.ok)return null;const d=await r.json();return JSON.parse(d.content.map(c=>c.text||'').join('').replace(/```json|```/g,'').trim());}catch(e){await addLog('err','Claude: '+e.message);return null;}
}

// ─── COEFFICIENTS ────────────────────────────────────────────
function updateCoefs(coefs,indSnap,result){const clamp=v=>+Math.max(0.05,Math.min(3,v)).toFixed(3);const win=result==='WIN';const out={...coefs};Object.keys(out).forEach(k=>{const s=indSnap?.[k];if(!s)return;const correct=(win&&s.bull===true)||(!win&&s.bull===false);out[k]=clamp(out[k]+(s.bull===null?0:correct?0.1:-0.06));});return out;}

// ─── TWO-PHASE STOP LOSS ─────────────────────────────────────
async function monitorPosition(state,cfg){
  const t=state.open_trade;if(!t)return;
  let px=t.entry_price;
  try{const q=await fetchQuote(t.symbol,cfg.td_key);if(q?.price>0)px=q.price;}catch(e){}
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
      await addLog('win',`${t.symbol} CLOSE — ${why} | +${currentPnl.toFixed(2)}% @ $${px.toFixed(2)} (trailed from +1%)`);
      await closePosition(t,px,result,currentPnl,state,cfg);
    }
    return;
  }

  if(pnl>=1.0){
    const trailStop=px*0.995;
    await addLog('info',`${t.symbol} +1% HIT @ $${px.toFixed(2)} — switching to trailing stop (0.5%)`);
    await patchState({open_trade:{...t,trailing_active:true,highest_price:px,trail_stop:trailStop},status_text:`${t.symbol} trailing — peak $${px.toFixed(2)} trail $${trailStop.toFixed(2)}`});
    await sendPush(cfg,`AutoTrader — ${t.symbol} +1% Hit!`,`Trailing stop activated. Peak: $${px.toFixed(2)} | Trail: $${trailStop.toFixed(2)}`);
  }else if(px<=t.stop_price||days>=3){
    const why=px<=t.stop_price?`fixed stop hit @ $${t.stop_price?.toFixed(2)}`:'3-day expiry';
    const result=pnl>=0?'WIN':'LOSS';
    await addLog(result==='WIN'?'win':'loss',`${t.symbol} CLOSE — ${why} | ${pnl>=0?'+':''}${pnl.toFixed(2)}% @ $${px.toFixed(2)}`);
    await closePosition(t,px,result,pnl,state,cfg);
  }else{
    await patchState({open_trade:{...t,highest_price:highestPrice},status_text:`Holding ${t.symbol} ${pnl>=0?'+':''}${pnl.toFixed(2)}% | stop $${t.stop_price?.toFixed(2)} | target $${t.target?.toFixed(2)} | day ${days}/3`});
  }
}

async function closePosition(t,exitPx,result,pnl,state,cfg){
  const res=await placeOrder(t.symbol,'SELL',t.shares,exitPx,cfg);
  if(!res.success&&!res.sandboxed)return;
  const newCoefs=updateCoefs(state.coefs,t.ind_snapshot,result);
  const newBal=state.balance*(1+pnl/100);
  if(t.db_id)await updateTrade(t.db_id,{exit_price:exitPx,result,pnl:+pnl.toFixed(3),exit_time:new Date().toISOString()});
  await patchState({balance:newBal,open_trade:null,last_trade_date:new Date().toISOString(),coefs:newCoefs,status_text:`SOLD ${t.symbol} ${pnl>=0?'+':''}${pnl.toFixed(2)}%`});
  await sendPush(cfg,`AutoTrader — ${result} ${pnl>=0?'+':''}${pnl.toFixed(2)}%`,`${t.symbol} closed | ${pnl>=0?'+':''}${pnl.toFixed(2)}% | Balance: $${newBal.toFixed(2)}`);
}

// ─── SCAN CYCLE ──────────────────────────────────────────────
let scanning=false;
async function scanCycle(){
  if(scanning)return;scanning=true;
  try{
    const[state,cfg]=await Promise.all([getState(),getConfig()]);
    if(!state?.running){scanning=false;return;}
    if(isMarketOpen()&&!cfg.sandbox){
      if(cfg.broker==='schwab')await ensureSchwabToken(cfg);
      else await checkAndRenewEtrade();
    }
    if(state.open_trade){await monitorPosition(state,cfg);scanning=false;return;}
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
    await addLog('scan',`Scanning ${sym} [${idx+1}/${watchlist.length}] | broker=${cfg.broker||'etrade'} | ${cfg.sandbox?'sandbox':'LIVE'}`);
    await patchState({scan_idx:newIdx,status_text:`Scanning ${sym}...`});
    const candles=await fetchCandles(sym,cfg.interval,cfg.lookback,cfg.td_key);
    let lv=null;if(cfg.interval!=='1day'){try{lv=await fetchVWAP(sym,cfg.interval,cfg.td_key);}catch(e){}}
    const ind=analyze(candles,lv,state.coefs);
    await patchState({last_analysis:{symbol:sym,...ind}});
    let px=ind.px;try{const q=await fetchQuote(sym,cfg.td_key);if(q?.price>0)px=q.price;}catch(e){}
    let grade=null;if(cfg.ant_key){grade=await aiGrade(sym,ind,cfg.ant_key);if(grade)await patchState({last_grade:grade});}
    const GR=['A','B','C','D','F'];
    const scoreOk=ind.comp>=cfg.min_score;
    const gOk=grade?GR.indexOf(grade.grade)<=GR.indexOf(cfg.min_grade)&&!grade.avoid:ind.comp>=(cfg.min_score+8);
    const isBuy=grade?grade.action==='BUY':ind.comp>=cfg.min_score;
    if(scoreOk&&gOk&&isBuy){
      // Get real balance for position sizing (sandbox=DB, live=broker API)
      const availableBalance=await getAvailableBalance(cfg,state.balance);
      const shares=Math.max(1,Math.floor(availableBalance*(cfg.pos_pct/100)/px));
      const target=grade?.target?+grade.target:+(px*1.012).toFixed(2);
      const stop=grade?.stop?+grade.stop:+(px*0.988).toFixed(2);
      await addLog('buy',`SIGNAL ${sym} | Grade=${grade?.grade||'rule'} Score=${ind.comp} | BUY ${shares}sh @ $${px.toFixed(2)} | balance=$${availableBalance.toFixed(2)} | stop $${stop.toFixed(2)} → trail 0.5% after +1%`);
      const res=await placeOrder(sym,'BUY',shares,px,cfg);
      if(res.success||res.sandboxed){
        const rows=await addTrade({symbol:sym,action:'BUY',shares,entry_price:px,target,stop_price:stop,result:'OPEN',grade:grade?.grade||'--',score:ind.comp,entry_time:new Date().toISOString(),ind_snapshot:ind.sigs});
        const dbId=Array.isArray(rows)?rows[0]?.id:null;
        await patchState({
          open_trade:{db_id:dbId,symbol:sym,shares,entry_price:px,target,stop_price:stop,entry_time:new Date().toISOString(),grade:grade?.grade||'--',score:ind.comp,ind_snapshot:ind.sigs,trailing_active:false,highest_price:px,trail_stop:null},
          last_trade_date:new Date().toISOString(),
          status_text:`BOUGHT ${shares}sh ${sym} @ $${px.toFixed(2)} | stop $${stop.toFixed(2)} → trails at +1%`
        });
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
  await addLog('info',`Bot started — ${sec}s cycle | broker=${broker} | clearing=${CLEARING_DAYS===0?'OFF (testing)':'T+'+CLEARING_DAYS} | ${cfg?.sandbox?'SANDBOX':'LIVE'}`);
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
  console.log(`AutoTrader listening on port ${PORT}`);
  startScreenerScheduler();
  try{const state=await getState();if(state?.running){console.log('Resuming bot...');await startLoop();}else console.log('Bot stopped — waiting for START');}
  catch(e){console.error('Startup error:',e.message);}
});
