// ============================================================
// AutoTrader — Schwab OAuth 2.0 Handler
// Much cleaner than E*Trade OAuth 1.0a:
//   - Standard OAuth 2.0 with PKCE
//   - Access tokens expire every 30 min — auto-refreshed by bot
//   - Refresh tokens last 7 days — authorize once a week
//   - No daily reauth, no PIN copying
//
// Flow:
//   1. /schwab/auth/start    → redirect to Schwab login
//   2. /schwab/auth/callback → Schwab sends code here automatically
//   3. Exchange code for tokens, save to Supabase
//   4. Bot auto-refreshes access token every 25 min
// ============================================================
import crypto from 'crypto';

const SCHWAB_AUTH_URL  = 'https://api.schwabapi.com/v1/oauth/authorize';
const SCHWAB_TOKEN_URL = 'https://api.schwabapi.com/v1/oauth/token';
const SCHWAB_API_BASE  = 'https://api.schwabapi.com/trader/v1';

// ─── PKCE HELPERS ────────────────────────────────────────────
export function generatePKCE() {
  const verifier  = crypto.randomBytes(32).toString('base64url');
  const challenge = crypto.createHash('sha256').update(verifier).digest('base64url');
  return { verifier, challenge };
}

// ─── STEP 1: BUILD AUTHORIZE URL ─────────────────────────────
export function buildAuthorizeUrl(clientId, redirectUri, state, codeChallenge) {
  const params = new URLSearchParams({
    response_type:         'code',
    client_id:             clientId,
    redirect_uri:          redirectUri,
    scope:                 'readonly trading',
    state,
    code_challenge:        codeChallenge,
    code_challenge_method: 'S256'
  });
  return `${SCHWAB_AUTH_URL}?${params.toString()}`;
}

// ─── STEP 2: EXCHANGE CODE FOR TOKENS ────────────────────────
export async function exchangeCodeForTokens(code, clientId, clientSecret, redirectUri, codeVerifier) {
  const credentials = Buffer.from(`${clientId}:${clientSecret}`).toString('base64');
  const body = new URLSearchParams({
    grant_type:    'authorization_code',
    code,
    redirect_uri:  redirectUri,
    code_verifier: codeVerifier
  });

  const r = await fetch(SCHWAB_TOKEN_URL, {
    method: 'POST',
    headers: {
      Authorization:  `Basic ${credentials}`,
      'Content-Type': 'application/x-www-form-urlencoded'
    },
    body: body.toString()
  });

  if (!r.ok) {
    const text = await r.text();
    throw new Error(`Token exchange failed (${r.status}): ${text}`);
  }

  const data = await r.json();
  return {
    accessToken:           data.access_token,
    refreshToken:          data.refresh_token,
    accessTokenExpiresIn:  data.expires_in || 1800,      // 30 min
    refreshTokenExpiresIn: data.refresh_token_expires_in || 604800 // 7 days
  };
}

// ─── AUTO REFRESH ACCESS TOKEN ───────────────────────────────
// Called automatically by bot every 25 min — no user action needed
export async function refreshAccessToken(refreshToken, clientId, clientSecret) {
  const credentials = Buffer.from(`${clientId}:${clientSecret}`).toString('base64');
  const body = new URLSearchParams({
    grant_type:    'refresh_token',
    refresh_token: refreshToken
  });

  const r = await fetch(SCHWAB_TOKEN_URL, {
    method: 'POST',
    headers: {
      Authorization:  `Basic ${credentials}`,
      'Content-Type': 'application/x-www-form-urlencoded'
    },
    body: body.toString()
  });

  if (!r.ok) {
    const text = await r.text();
    throw new Error(`Token refresh failed (${r.status}): ${text}`);
  }

  const data = await r.json();
  return {
    accessToken:          data.access_token,
    refreshToken:         data.refresh_token || refreshToken, // Schwab may or may not rotate
    accessTokenExpiresIn: data.expires_in || 1800
  };
}

// ─── CHECK TOKEN STATUS ───────────────────────────────────────
export function tokenNeedsRefresh(accessTokenUpdatedAt) {
  if (!accessTokenUpdatedAt) return true;
  const age = (Date.now() - new Date(accessTokenUpdatedAt).getTime()) / 1000;
  return age > 1500; // refresh after 25 min (before 30 min expiry)
}

export function refreshTokenExpired(refreshTokenUpdatedAt) {
  if (!refreshTokenUpdatedAt) return true;
  const age = (Date.now() - new Date(refreshTokenUpdatedAt).getTime()) / 1000;
  return age > 604800; // 7 days
}

// ─── GET ACCOUNT NUMBER ───────────────────────────────────────
// Returns the encrypted account hash needed for order placement
export async function getAccountNumbers(accessToken) {
  const r = await fetch(`${SCHWAB_API_BASE}/accounts/accountNumbers`, {
    headers: { Authorization: `Bearer ${accessToken}` }
  });
  if (!r.ok) throw new Error(`getAccountNumbers failed (${r.status})`);
  const data = await r.json();
  // Returns array of { accountNumber, hashValue }
  return data;
}

// ─── PLACE ORDER ─────────────────────────────────────────────
export async function placeSchwabOrder(sym, action, shares, accessToken, accountHash) {
  const orderBody = {
    orderType:          'MARKET',
    session:            'NORMAL',
    duration:           'DAY',
    orderStrategyType:  'SINGLE',
    orderLegCollection: [{
      instruction:  action === 'BUY' ? 'BUY' : 'SELL',
      quantity:     shares,
      instrument: {
        symbol:        sym,
        assetType:     'EQUITY'
      }
    }]
  };

  const r = await fetch(`${SCHWAB_API_BASE}/accounts/${accountHash}/orders`, {
    method:  'POST',
    headers: {
      Authorization:  `Bearer ${accessToken}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(orderBody)
  });

  // Schwab returns 201 with no body on success
  if (r.status === 201) return { success: true, orderId: r.headers.get('location')?.split('/').pop() };
  if (!r.ok) {
    const text = await r.text();
    throw new Error(`Order failed (${r.status}): ${text}`);
  }
  return { success: true };
}

// ─── GET QUOTE ────────────────────────────────────────────────
export async function getSchwabQuote(sym, accessToken) {
  try {
    const r = await fetch(`${SCHWAB_API_BASE}/quotes?symbols=${sym}&fields=quote`, {
      headers: { Authorization: `Bearer ${accessToken}` }
    });
    if (!r.ok) return null;
    const data = await r.json();
    const q = data[sym]?.quote;
    if (!q) return null;
    return { price: q.lastPrice || q.regularMarketLastPrice || 0, changePct: q.regularMarketPercentChangeInDouble || 0 };
  } catch (e) { return null; }
}
