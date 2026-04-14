// ============================================================
// AutoTrader Bot — Railway Server v4.1
// Fixed: E*Trade OAuth requests now include credentials:'omit'
// to prevent cookie interference causing 401 errors
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

// ─── E*TRADE OAUTH HELPERS ───────────────────────────────────
function oauthSign(method, url, params, consumerSecret, tokenSecret = '') {
  const sorted = Object.keys(params).sort()
    .map(k => `${encodeURIComponent(k)}=${encodeURIComponent(params[k])}`).join('&');
  const base   = `${method}&${encodeURIComponent(url)}&${encodeURIComponent(sorted)}`;
  const sigKey = `${encodeURIComponent(consumerSecret)}&${encodeURIComponent(tokenSecret)}`;
  return crypto.createHmac('sha1', sigKey).update(base).digest('base64');
}

function makeOAuthHeader(method, url, cfg, extra = {}) {
  const p = {
    oauth_consumer_key:     cfg.e_key,
    oauth_nonce:            crypto.randomBytes(16).toString('hex'),
    oauth_signature_method: 'HMAC-SHA1',
    oauth_timestamp:        Math.floor(Date.now() / 1000).toString(),
    oauth_version:          '1.0',
    ...extra
  };
  if (cfg.e_token && !extra.oauth_callback) p.oauth_token = cfg.e_token;
  p.oauth_signature = oauthSign(method, url, p, cfg.e_secret, cfg.e_token_secret || '');
  return 'OAuth ' + Object.keys(p).sort()
    .map(k => `${encodeURIComponent(k)}="${encodeURIComponent(p[k])}"`)
    .join(', ');
}

function parseQS(text) {
  const r = {};
  text.split('&').forEach(pair => {
    const [k, v] = pair.split('=');
    if (k) r[decodeURIComponent(k)] = decodeURIComponent(v || '');
  });
  return r;
}

// KEY FIX: all E*Trade OAuth calls use credentials:'omit'
// to prevent browser cookies from interfering with auth
async function etradeOAuthFetch(url, header) {
  return fetch(url, {
    headers: { Authorization: header },
    credentials: 'omit'   // prevents cookie interference causing 401s
  });
}

// ─── TOKEN RENEWAL ───────────────────────────────────────────
let lastRenewal   = null;
let renewalWarned = false;
const RENEW_MS    = 90 * 60 * 1000;
const WARN_MS     = 30 * 60 * 1000;

async function renewToken(cfg) {
  if (!cfg.e_key || !cfg.e_token) return false;
  const base     = cfg.sandbox ? 'https://apisb.etrade.com' : 'https://api.etrade.com';
  const renewUrl = `${base}/oauth/renew_access_token`;
  try {
    const r    = await etradeOAuthFetch(renewUrl, makeOAuthHeader('GET', renewUrl, cfg));
    const text = await r.text();
    if (r.ok && text.includes('renewed')) {
      lastRenewal = new Date(); renewalWarned = false;
      await addLog('info', 'E*Trade token renewed');
      return true;
    }
    await addLog('err', `Token renewal failed: ${text}`);
    return false;
  } catch (e) { await addLog('err', `Token renewal error: ${e.message}`); return false; }
}

async function checkAndRenewToken() {
  const cfg = await getConfig();
  if (!cfg?.e_token || cfg?.sandbox) return;
  const now = new Date();
  if (!lastRenewal || (now - lastRenewal) >= RENEW_MS) {
    const ok = await renewToken(cfg);
    if (!ok) {
      if (!renewalWarned || (now - renewalWarned) >= WARN_MS) {
        renewalWarned = now;
        const authUrl = `https://${process.env.RAILWAY_PUBLIC_DOMAIN || ''}/etrade/auth`;
        await sendPush(cfg, 'AutoTrader — Action Required', 'E*Trade session expired. Tap to re-authorize.', authUrl);
        await patchState({ status_text: 'E*Trade token expired — tap push notification to re-authorize' });
      }
    }
  }
}

// ─── ORDER PLACEMENT ─────────────────────────────────────────
async function placeOrder(sym, action, shares, price, cfg) {
  if (cfg.sandbox) {
    await addLog('skip', `[SANDBOX] ${action} ${shares}sh ${sym} @ $${price.toFixed(2)} — not sent`);
    return { success: true, sandboxed: true };
  }
  if (!cfg.e_key || !cfg.e_token || !cfg.e_account) {
    await addLog('err', 'E*Trade tokens missing — visit /etrade/auth');
    return { success: false };
  }
  const url  = `https://api.etrade.com/v1/accounts/${cfg.e_account}/orders/place`;
  const body = {
    PlaceOrderRequest: {
      orderType: 'EQ', clientOrderId: `BOT-${Date.now()}`,
      Order: [{ Instrument: [{ Product: { securityType: 'EQ', symbol: sym },
        orderAction: action, quantityType: 'QUANTITY', quantity: shares }],
        orderTerm: 'GOOD_FOR_DAY', marketSession: 'REGULAR', priceType: 'MARKET' }]
    }
  };
  try {
    const r = await fetch(url, {
      method: 'POST', credentials: 'omit',
      headers: { Authorization: makeOAuthHeader('POST', url, cfg), 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });
    const j = await r.json();
    if (r.status === 401) {
      const authUrl = `https://${process.env.RAILWAY_PUBLIC_DOMAIN || ''}/etrade/auth`;
      await sendPush(cfg, 'AutoTrader — Trade Failed', `${action} order for ${sym} failed — session expired.`, authUrl);
      return { success: false, expired: true };
    }
    const orderId = j?.PlaceOrderResponse?.OrderIds?.[0]?.orderId;
    if (orderId) {
      await addLog('buy', `E*Trade order confirmed — orderId: ${orderId}`);
      await sendPush(cfg, `AutoTrader — ${action} Executed`, `${action} ${shares}sh ${sym} @ $${price.toFixed(2)}`);
      return { success: true, orderId };
    }
    await addLog('err', `E*Trade order failed: ${JSON.stringify(j)}`);
    return { success: false };
  } catch (e) { await addLog('err', `E*Trade error: ${e.message}`); return { success: false }; }
}

// ─── E*TRADE OAUTH FLOW ──────────────────────────────────────
app.get('/etrade/auth', async (req, res) => {
  const cfg = await getConfig();
  if (!cfg?.e_key || !cfg?.e_secret) {
    return res.send(`<html><body style="font-family:monospace;background:#050a0f;color:#b8d4ee;padding:30px">
      <h2 style="color:#f5a623">Missing E*Trade Keys</h2>
      <p>Add Consumer Key and Secret in your phone app Config first.</p>
    </body></html>`);
  }
  const sandbox = cfg.sandbox;
  const apiBase = sandbox ? 'https://apisb.etrade.com' : 'https://api.etrade.com';
  const reqUrl  = `${apiBase}/oauth/request_token`;
  try {
    const tmpCfg = { ...cfg, e_token: '', e_token_secret: '' };
    const header = makeOAuthHeader('GET', reqUrl, tmpCfg, { oauth_callback: 'oob' });
    // credentials:'omit' prevents cookie interference
    const r    = await etradeOAuthFetch(reqUrl, header);
    const text = await r.text();
    const p    = parseQS(text);
    if (!p.oauth_token) {
      return res.send(`<html><body style="font-family:monospace;background:#050a0f;color:#f0364a;padding:30px">
        <h2>Error getting request token</h2>
        <pre>${text}</pre>
        <p>HTTP Status: ${r.status}</p>
        <p>Check that your Consumer Key and Secret are correct in Config.</p>
      </body></html>`);
    }
    const authUrl = `https://us.etrade.com/e/t/etws/authorize?key=${cfg.e_key}&token=${p.oauth_token}`;
    res.send(`<!DOCTYPE html><html><head>
      <meta name="viewport" content="width=device-width,initial-scale=1">
      <style>
        body{font-family:monospace;background:#050a0f;color:#b8d4ee;padding:20px;max-width:540px;margin:0 auto}
        h2{color:#f5a623} h3{color:#00e891;font-size:14px;margin:0 0 8px}
        .box{background:#091422;border:1px solid #162d47;border-radius:6px;padding:16px;margin:12px 0}
        .btn-link{display:block;background:#162d47;border:1px solid #2d9cff;color:#2d9cff;padding:14px;border-radius:5px;text-align:center;text-decoration:none;margin-top:10px;font-weight:700;font-size:15px}
        input{width:100%;background:#0d1c30;border:1px solid #1c3a59;border-radius:4px;padding:12px;color:#b8d4ee;font-family:monospace;font-size:20px;box-sizing:border-box;margin:8px 0;letter-spacing:.15em;text-transform:uppercase}
        button{background:#00e891;color:#050a0f;border:none;border-radius:5px;padding:14px;font-family:monospace;font-size:15px;font-weight:700;cursor:pointer;width:100%;margin-top:8px}
        .note{font-size:11px;color:#4a7090;margin-top:6px;line-height:1.5}
        #msg{margin-top:14px;padding:14px;border-radius:5px;display:none}
        .mode{display:inline-block;padding:3px 10px;border-radius:3px;font-size:11px;font-weight:700;margin-bottom:14px}
      </style>
    </head><body>
      <h2>E*Trade Authorization</h2>
      <div class="mode" style="background:${sandbox?'rgba(245,166,35,.15)':'rgba(240,54,74,.15)'};color:${sandbox?'#f5a623':'#f0364a'}">${sandbox?'SANDBOX MODE':'LIVE TRADING'}</div>
      <div class="box">
        <h3>Step 1 — Authorize on E*Trade</h3>
        <p>Tap the button below. Log in to E*Trade and approve the bot. You will see a <strong style="color:#00e891">PIN code</strong> — copy it.</p>
        <a class="btn-link" href="${authUrl}" target="_blank">Tap to Authorize on E*Trade</a>
        <p class="note">Come back here immediately after you see the PIN. Do not wait — PINs expire in about 2 minutes.</p>
      </div>
      <div class="box">
        <h3>Step 2 — Enter PIN immediately</h3>
        <input type="text" id="pin" placeholder="XXXXX" inputmode="text" maxlength="10" autocomplete="off" autocorrect="off" spellcheck="false"/>
        <button onclick="go()">Complete Authorization</button>
        <p class="note">Tokens save permanently to Supabase. Bot resumes automatically.</p>
      </div>
      <div id="msg"></div>
      <script>
        // Focus the PIN field as soon as the page loads
        document.getElementById('pin').focus();
        async function go() {
          const pin = document.getElementById('pin').value.trim().toUpperCase();
          if (!pin) { alert('Enter the PIN from E*Trade first'); return; }
          document.getElementById('msg').style.display='block';
          document.getElementById('msg').style.cssText='display:block;background:#162d47;padding:14px;border-radius:5px;color:#f5a623';
          document.getElementById('msg').innerHTML='Saving tokens...';
          try {
            const r = await fetch('/etrade/callback?rt=${p.oauth_token}&pin='+encodeURIComponent(pin), { credentials: 'omit' });
            const j = await r.json();
            const el = document.getElementById('msg');
            if (j.ok) {
              el.style.cssText='display:block;background:#0a2a1a;border:1px solid #00e891;border-radius:5px;padding:16px';
              el.innerHTML='<p style="color:#00e891;font-size:16px;margin:0">Authorization complete! Trading resumed. Close this page.</p>';
            } else {
              el.style.cssText='display:block;background:#2a0a0a;border:1px solid #f0364a;border-radius:5px;padding:16px';
              el.innerHTML='<p style="color:#f0364a;margin:0">Error: '+j.error+'<br><br>Tap the E*Trade button again for a fresh PIN and enter it immediately.</p>';
            }
          } catch(e) {
            document.getElementById('msg').innerHTML='Network error — try again';
          }
        }
        // Allow pressing Enter to submit
        document.getElementById('pin').addEventListener('keydown', e => { if(e.key==='Enter') go(); });
      </script>
    </body></html>`);
  } catch (e) {
    res.send(`<html><body style="font-family:monospace;background:#050a0f;color:#f0364a;padding:30px">
      <h2>Server Error</h2><pre>${e.message}</pre>
    </body></html>`);
  }
});

app.get('/etrade/callback', async (req, res) => {
  const { rt, pin } = req.query;
  if (!rt || !pin) return res.json({ ok: false, error: 'Missing token or PIN' });
  const cfg     = await getConfig();
  const apiBase = cfg.sandbox ? 'https://apisb.etrade.com' : 'https://api.etrade.com';
  const accUrl  = `${apiBase}/oauth/access_token`;
  try {
    const tmpCfg = { ...cfg, e_token: rt, e_token_secret: '' };
    const header = makeOAuthHeader('GET', accUrl, tmpCfg, { oauth_verifier: pin.toUpperCase() });
    // credentials:'omit' is the key fix — prevents cookie interference
    const r    = await etradeOAuthFetch(accUrl, header);
    const text = await r.text();
    console.log(`[OAUTH] Access token response (${r.status}): ${text.substring(0, 200)}`);
    const p    = parseQS(text);
    if (!p.oauth_token) {
      return res.json({ ok: false, error: `PIN rejected (HTTP ${r.status}). Try again — tap E*Trade button for a fresh PIN.` });
    }
    await patchConfig({ e_token: p.oauth_token, e_token_secret: p.oauth_token_secret });
    lastRenewal = new Date(); renewalWarned = false;
    await addLog('info', 'E*Trade OAuth complete — tokens saved');
    await patchState({ status_text: 'E*Trade authorized — bot ready' });
    const freshCfg = await getConfig();
    await sendPush(freshCfg, 'AutoTrader — Authorized', 'E*Trade connected. Bot is now active.');
    res.json({ ok: true });
  } catch (e) { res.json({ ok: false, error: e.message }); }
});

// ─── INDICATOR MATH ──────────────────────────────────────────
const sma = (a, p) => a.map((_, i) => i < p - 1 ? null : a.slice(i - p + 1, i + 1).reduce((s, v) => s + v, 0) / p);
function ema(a, p) { const k=2/(p+1),r=new Array(a.length).fill(null);r[p-1]=a.slice(0,p).reduce((s,v)=>s+v,0)/p;for(let i=p;i<a.length;i++)r[i]=a[i]*k+r[i-1]*(1-k);return r; }
function rsi(c, p=14) { const r=new Array(c.length).fill(null);let g=0,l=0;for(let i=1;i<=p;i++){const d=c[i]-c[i-1];d>0?g+=d:l-=d;}let ag=g/p,al=l/p;r[p]=100-100/(1+(al===0?1e10:ag/al));for(let i=p+1;i<c.length;i++){const d=c[i]-c[i-1];ag=(ag*(p-1)+(d>0?d:0))/p;al=(al*(p-1)+(d<0?-d:0))/p;r[i]=100-100/(1+(al===0?1e10:ag/al));}return r; }
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
  if(rsiV<30)sigs.RSI={score:86,label:'Oversold',bull:true};
  else if(rsiV<42)sigs.RSI={score:68,label:'Bullish zone',bull:true};
  else if(rsiV>70)sigs.RSI={score:14,label:'Overbought',bull:false};
  else if(rsiV>58)sigs.RSI={score:34,label:'Bearish zone',bull:false};
  else sigs.RSI={score:50,label:'Neutral',bull:null};
  if(mlV!=null&&slV!=null){
    if(hlV>0&&hlP<=0)sigs.MACD={score:91,label:'Bull cross',bull:true};
    else if(hlV<0&&hlP>=0)sigs.MACD={score:9,label:'Bear cross',bull:false};
    else if(mlV>slV&&mlV>0)sigs.MACD={score:72,label:'Bullish',bull:true};
    else if(mlV<slV&&mlV<0)sigs.MACD={score:28,label:'Bearish',bull:false};
    else sigs.MACD={score:50,label:'Mixed',bull:null};
  }else sigs.MACD={score:50,label:'No data',bull:null};
  if(blV!=null){
    if(px<=blV*1.005)sigs.BB={score:84,label:'At lower band',bull:true};
    else if(px>=buV*0.995)sigs.BB={score:16,label:'At upper band',bull:false};
    else if(bbPct<35)sigs.BB={score:64,label:'Lower half',bull:true};
    else if(bbPct>65)sigs.BB={score:37,label:'Upper half',bull:false};
    else sigs.BB={score:50,label:'Mid band',bull:null};
  }else sigs.BB={score:50,label:'Calculating',bull:null};
  const vwD=vwV?(px-vwV)/vwV*100:0;
  if(px>vwV*1.005)sigs.VWAP={score:66,label:`+${vwD.toFixed(2)}% above`,bull:true};
  else if(px<vwV*0.995)sigs.VWAP={score:35,label:`${vwD.toFixed(2)}% below`,bull:false};
  else sigs.VWAP={score:50,label:'At VWAP',bull:null};
  if(s20&&s50){
    if(px>s20&&s20>s50)sigs.SMA={score:76,label:'Full uptrend',bull:true};
    else if(px<s20&&s20<s50)sigs.SMA={score:24,label:'Full downtrend',bull:false};
    else if(px>s20&&s20<s50)sigs.SMA={score:58,label:'Recovery',bull:true};
    else sigs.SMA={score:42,label:'Below SMA20',bull:false};
  }else sigs.SMA={score:50,label:'Calculating',bull:null};
  if(e1&&e2){
    if(e1>e2&&px>e1)sigs.EMA={score:74,label:'Bull momentum',bull:true};
    else if(e1<e2&&px<e1)sigs.EMA={score:26,label:'Bear momentum',bull:false};
    else if(e1>e2)sigs.EMA={score:62,label:'Bullish cross',bull:true};
    else sigs.EMA={score:38,label:'Bearish cross',bull:false};
  }else sigs.EMA={score:50,label:'Calculating',bull:null};
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
STRATEGY: autonomous bot, target +1% per trade, max 3 business day hold, T+3 clearing enforced.
Return ONLY: {"grade":"B","score":72,"action":"BUY","confidence":70,"reason":"one sentence","target":${(ind.px*1.012).toFixed(2)},"stop":${(ind.px*.988).toFixed(2)},"strongest":"MACD","avoid":false}`;
  try{const r=await fetch('https://api.anthropic.com/v1/messages',{method:'POST',headers:{'Content-Type':'application/json','x-api-key':antKey,'anthropic-version':'2023-06-01'},body:JSON.stringify({model:'claude-sonnet-4-20250514',max_tokens:200,messages:[{role:'user',content:prompt}]})});if(!r.ok)return null;const d=await r.json();return JSON.parse(d.content.map(c=>c.text||'').join('').replace(/```json|```/g,'').trim());}catch(e){await addLog('err','Claude: '+e.message);return null;}
}

// ─── MARKET HOURS ────────────────────────────────────────────
function bizAdd(d,n){let r=new Date(d),c=0;while(c<n){r.setDate(r.getDate()+1);if(r.getDay()!==0&&r.getDay()!==6)c++;}return r;}
const clearOk=lt=>!lt||new Date()>=bizAdd(new Date(lt),3);
function isMarketOpen(){const et=new Date(new Date().toLocaleString('en-US',{timeZone:'America/New_York'}));if(et.getDay()===0||et.getDay()===6)return false;const m=et.getHours()*60+et.getMinutes();return m>=570&&m<960;}
function countBizDays(s,e){let c=0,d=new Date(s);while(d<e){d.setDate(d.getDate()+1);if(d.getDay()!==0&&d.getDay()!==6)c++;}return c;}

// ─── COEFFICIENTS ────────────────────────────────────────────
function updateCoefs(coefs,indSnap,result){const clamp=v=>+Math.max(0.05,Math.min(3,v)).toFixed(3);const win=result==='WIN';const out={...coefs};Object.keys(out).forEach(k=>{const s=indSnap?.[k];if(!s)return;const correct=(win&&s.bull===true)||(!win&&s.bull===false);out[k]=clamp(out[k]+(s.bull===null?0:correct?0.1:-0.06));});return out;}

// ─── POSITION MONITOR ────────────────────────────────────────
async function monitorPosition(state,cfg){
  const t=state.open_trade;if(!t)return;
  let px=t.entry_price;
  try{const q=await fetchQuote(t.symbol,cfg.td_key);if(q?.price>0)px=q.price;}catch(e){}
  const pnl=(px-t.entry_price)/t.entry_price*100;
  const days=countBizDays(new Date(t.entry_time),new Date());
  if(pnl>=1.0||px<=t.stop_price||days>=3){
    const why=pnl>=1.0?'+1% target hit':px<=t.stop_price?'stop hit':'3-day expiry';
    const result=pnl>=0?'WIN':'LOSS';
    await addLog(result==='WIN'?'win':'loss',`${t.symbol} CLOSE — ${why} | ${pnl>=0?'+':''}${pnl.toFixed(2)}% @ $${px.toFixed(2)}`);
    const res=await placeOrder(t.symbol,'SELL',t.shares,px,cfg);
    if(!res.success&&!res.sandboxed)return;
    const newCoefs=updateCoefs(state.coefs,t.ind_snapshot,result);
    if(t.db_id)await updateTrade(t.db_id,{exit_price:px,result,pnl:+pnl.toFixed(3),exit_time:new Date().toISOString()});
    await patchState({balance:state.balance*(1+pnl/100),open_trade:null,last_trade_date:new Date().toISOString(),coefs:newCoefs,status_text:`SOLD ${t.symbol} ${pnl>=0?'+':''}${pnl.toFixed(2)}%`});
    await sendPush(cfg,`AutoTrader — ${result} ${pnl>=0?'+':''}${pnl.toFixed(2)}%`,`${t.symbol} closed: ${why} | ${pnl>=0?'+':''}${pnl.toFixed(2)}%`);
  }else{
    await patchState({status_text:`Holding ${t.symbol} ${pnl>=0?'+':''}${pnl.toFixed(2)}% | tgt $${t.target?.toFixed(2)} stop $${t.stop_price?.toFixed(2)} | day ${days}/3`});
  }
}

// ─── SCAN CYCLE ──────────────────────────────────────────────
let scanning=false;
async function scanCycle(){
  if(scanning)return;scanning=true;
  try{
    const[state,cfg]=await Promise.all([getState(),getConfig()]);
    if(!state?.running){scanning=false;return;}
    if(isMarketOpen()&&!cfg.sandbox)await checkAndRenewToken();
    if(state.open_trade){await monitorPosition(state,cfg);scanning=false;return;}
    if(!isMarketOpen()){await patchState({status_text:'Market closed — bot idle'});scanning=false;return;}
    if(!clearOk(state.last_trade_date)){const next=bizAdd(new Date(state.last_trade_date),3);await patchState({status_text:`Clearing lock — next trade: ${next.toLocaleDateString('en-US',{month:'short',day:'numeric'})}`});scanning=false;return;}
    const watchlist=(cfg.watchlist||'').split(',').map(s=>s.trim()).filter(Boolean);
    if(!watchlist.length){await patchState({status_text:'Watchlist empty — screener runs at 9 AM ET'});scanning=false;return;}
    const idx=(state.scan_idx||0)%watchlist.length;
    const sym=watchlist[idx];
    const newIdx=(state.scan_idx||0)+1;
    await addLog('scan',`Scanning ${sym} [${idx+1}/${watchlist.length}] cycle ${Math.ceil(newIdx/watchlist.length)}`);
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
      const shares=Math.max(1,Math.floor(state.balance*(cfg.pos_pct/100)/px));
      const target=grade?.target?+grade.target:+(px*1.012).toFixed(2);
      const stop=grade?.stop?+grade.stop:+(px*0.988).toFixed(2);
      await addLog('buy',`SIGNAL ${sym} | Grade=${grade?.grade||'rule'} Score=${ind.comp} | BUY ${shares}sh @ $${px.toFixed(2)} | tgt $${target.toFixed(2)} stop $${stop.toFixed(2)}`);
      const res=await placeOrder(sym,'BUY',shares,px,cfg);
      if(res.success||res.sandboxed){
        const rows=await addTrade({symbol:sym,action:'BUY',shares,entry_price:px,target,stop_price:stop,result:'OPEN',grade:grade?.grade||'--',score:ind.comp,entry_time:new Date().toISOString(),ind_snapshot:ind.sigs});
        const dbId=Array.isArray(rows)?rows[0]?.id:null;
        await patchState({open_trade:{db_id:dbId,symbol:sym,shares,entry_price:px,target,stop_price:stop,entry_time:new Date().toISOString(),grade:grade?.grade||'--',score:ind.comp,ind_snapshot:ind.sigs},last_trade_date:new Date().toISOString(),status_text:`BOUGHT ${shares}sh ${sym} @ $${px.toFixed(2)} — watching for +1%`});
      }
    }else{
      const why=!scoreOk?`score ${ind.comp}<${cfg.min_score}`:!gOk?`grade ${grade?.grade} below ${cfg.min_grade}`:grade?.avoid?'avoid flag':'no buy signal';
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
  await addLog('info',`Bot started — scanning every ${sec}s | sandbox=${cfg?.sandbox}`);
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
