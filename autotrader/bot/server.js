// ============================================================
// AutoTrader Bot — Railway Server v3.1 (Schwab edition)
// Multi-user, OAuth 2.0, auto token refresh every 25 min
// ============================================================
import express from 'express';
import crypto from 'crypto';
import { generatePKCE, buildAuthorizeUrl, exchangeCodeForTokens, refreshAccessToken, tokenNeedsRefresh, refreshTokenExpired, getAccountNumbers, placeSchwabOrder, getSchwabQuote } from './schwab.js';
import { runScreenerForUser, startScreenerScheduler } from './screener.js';

const app = express();
app.use(express.json());

const SB_URL     = process.env.SUPABASE_URL;
const SB_KEY     = process.env.SUPABASE_SERVICE_KEY;
const BOT_URL    = process.env.RAILWAY_PUBLIC_DOMAIN ? `https://${process.env.RAILWAY_PUBLIC_DOMAIN}` : (process.env.BOT_URL || '');

if (!SB_URL || !SB_KEY) { console.error('SUPABASE_URL and SUPABASE_SERVICE_KEY required'); process.exit(1); }

// ─── SUPABASE ────────────────────────────────────────────────
const HDR = { apikey:SB_KEY, Authorization:`Bearer ${SB_KEY}`, 'Content-Type':'application/json' };
async function sbGet(table,q=''){const r=await fetch(`${SB_URL}/rest/v1/${table}${q}`,{headers:HDR});return r.json();}
async function sbPatch(table,data,q=''){const r=await fetch(`${SB_URL}/rest/v1/${table}${q}`,{method:'PATCH',headers:{...HDR,Prefer:'return=representation'},body:JSON.stringify(data)});return r.json();}
async function sbPost(table,data){const r=await fetch(`${SB_URL}/rest/v1/${table}`,{method:'POST',headers:{...HDR,Prefer:'return=representation'},body:JSON.stringify(data)});return r.json();}
async function sbUpsert(table,data,q=''){const r=await fetch(`${SB_URL}/rest/v1/${table}${q}`,{method:'POST',headers:{...HDR,Prefer:'resolution=merge-duplicates,return=representation'},body:JSON.stringify(data)});return r.json();}

const getCfg    = uid => sbGet('config',`?user_id=eq.${uid}`).then(r=>r[0]);
const getState  = uid => sbGet('bot_state',`?user_id=eq.${uid}`).then(r=>r[0]);
const getSchwab = uid => sbGet('schwab_oauth',`?user_id=eq.${uid}`).then(r=>r[0]);
const patchState = (uid,d) => sbPatch('bot_state',{...d,updated_at:new Date().toISOString()},`?user_id=eq.${uid}`);

async function log(userId,type,message){
  console.log(`[${userId.slice(0,8)}][${type.toUpperCase()}] ${message}`);
  try{await sbPost('activity_log',{user_id:userId,type,message});}catch(e){}
}

// ─── TOKEN MANAGER ───────────────────────────────────────────
// Auto-refreshes Schwab access token before it expires (every 25 min)
async function ensureValidToken(userId, cfg) {
  const oauth = await getSchwab(userId);
  if (!oauth?.authorized) throw new Error('Schwab not authorized — user must connect');
  if (refreshTokenExpired(oauth.refresh_token_updated_at)) throw new Error('Schwab refresh token expired — user must reconnect (weekly)');

  if (tokenNeedsRefresh(oauth.access_token_updated_at)) {
    console.log(`[${userId.slice(0,8)}] Refreshing Schwab access token...`);
    const { accessToken, refreshToken } = await refreshAccessToken(oauth.refresh_token, cfg.schwab_client_id, cfg.schwab_client_secret);
    await sbUpsert('schwab_oauth', {
      user_id:                  userId,
      access_token:             accessToken,
      refresh_token:            refreshToken,
      access_token_updated_at:  new Date().toISOString(),
      authorized:               true
    }, '?user_id=eq.' + userId);
    return accessToken;
  }
  return oauth.access_token;
}

// ─── INDICATOR MATH ──────────────────────────────────────────
const sma=(a,p)=>a.map((_,i)=>i<p-1?null:a.slice(i-p+1,i+1).reduce((s,v)=>s+v,0)/p);
function ema(a,p){const k=2/(p+1),r=new Array(a.length).fill(null);r[p-1]=a.slice(0,p).reduce((s,v)=>s+v,0)/p;for(let i=p;i<a.length;i++)r[i]=a[i]*k+r[i-1]*(1-k);return r;}
function rsi(c,p=14){const r=new Array(c.length).fill(null);let g=0,l=0;for(let i=1;i<=p;i++){const d=c[i]-c[i-1];d>0?g+=d:l-=d;}let ag=g/p,al=l/p;r[p]=100-100/(1+(al===0?1e10:ag/al));for(let i=p+1;i<c.length;i++){const d=c[i]-c[i-1];ag=(ag*(p-1)+(d>0?d:0))/p;al=(al*(p-1)+(d<0?-d:0))/p;r[i]=100-100/(1+(al===0?1e10:ag/al));}return r;}
function macd(c,f=12,s=26,sg=9){const ef=ema(c,f),es=ema(c,s);const ml=c.map((_,i)=>ef[i]!=null&&es[i]!=null?ef[i]-es[i]:null);const vals=ml.filter(v=>v!=null),off=ml.findIndex(v=>v!=null);const se=ema(vals,sg);const sl=new Array(ml.length).fill(null),hl=new Array(ml.length).fill(null);for(let i=0;i<se.length;i++){const x=off+i;if(se[i]!=null){sl[x]=se[i];hl[x]=ml[x]-se[i];}}return{ml,sl,hl};}
function bbands(c,p=20,m=2){const mid=sma(c,p),up=[],lo=[];for(let i=0;i<c.length;i++){if(mid[i]==null){up.push(null);lo.push(null);continue;}const sl=c.slice(i-p+1,i+1),mv=mid[i];const std=Math.sqrt(sl.reduce((a,v)=>a+(v-mv)**2,0)/p);up.push(mv+m*std);lo.push(mv-m*std);}return{up,mid,lo};}
function calcVwap(d){let cv=0,cq=0;return d.map(c=>{const tp=(c.high+c.low+c.close)/3;cv+=tp*c.volume;cq+=c.volume;return cq>0?cv/cq:tp;});}
function rmsVol(c,p=20){const r=new Array(c.length).fill(null);for(let i=p;i<c.length;i++){const rets=[];for(let j=i-p+1;j<=i;j++)rets.push((c[j]-c[j-1])/c[j-1]);r[i]=Math.sqrt(rets.reduce((s,v)=>s+v*v,0)/rets.length)*100;}return r;}
function calcATR(d,p=14){const tr=d.map((c,i)=>i===0?c.high-c.low:Math.max(c.high-c.low,Math.abs(c.high-d[i-1].close),Math.abs(c.low-d[i-1].close)));return sma(tr,p);}
function calcSlope(c,p=20){if(c.length<p)return 0;const s=c.slice(-p),n=s.length,sx=n*(n-1)/2,sx2=n*(n-1)*(2*n-1)/6,sy=s.reduce((a,v)=>a+v,0),sxy=s.reduce((a,v,i)=>a+i*v,0);return(n*sxy-sx*sy)/(n*sx2-sx*sx);}
function momentum(c,p=10){const n=c.length-1;return n<p?0:(c[n]-c[n-p])/c[n-p]*100;}

function analyze(data,lv,coefs){
  const closes=data.map(d=>d.close),n=data.length-1,px=data[n].close;
  const rv=rsi(closes,14),md=macd(closes),bb=bbands(closes);
  const sm20=sma(closes,20),sm50=sma(closes,50),e12=ema(closes,12),e26=ema(closes,26);
  const vwArr=calcVwap(data),rms=rmsVol(closes),at=calcATR(data);
  const rsiV=rv[n],mlV=md.ml[n],slV=md.sl[n],hlV=md.hl[n],hlP=md.hl[n-1];
  const buV=bb.up[n],bmV=bb.mid[n],blV=bb.lo[n];
  const bbPct=buV&&blV?(px-blV)/(buV-blV)*100:50;
  const vwV=lv||vwArr[n],s20=sm20[n],s50=sm50[n],e1=e12[n],e2=e26[n];
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
  return{sigs,comp,px,vwap:vwV,rsiV,mlV,slV,hlV,hlP,buV,bmV,blV,bbPct:+bbPct.toFixed(1),s20,s50,e1,e2,rms:rms[n],atr:at[n],slp:calcSlope(closes),mom:momentum(closes)};
}

// ─── TWELVE DATA ─────────────────────────────────────────────
const TD='https://api.twelvedata.com';
async function fetchCandles(sym,interval,lookback,key){
  const r=await fetch(`${TD}/time_series?symbol=${sym}&interval=${interval}&outputsize=${lookback}&order=ASC&apikey=${key}`);
  const j=await r.json();if(j.status==='error')throw new Error('TD:'+j.message);
  return j.values.map(v=>({date:v.datetime,open:+v.open,high:+v.high,low:+v.low,close:+v.close,volume:+(v.volume||0)}));
}
async function fetchQuote(sym,key){try{const r=await fetch(`${TD}/quote?symbol=${sym}&apikey=${key}`);const j=await r.json();if(j.status==='error')return null;return{price:+(j.close||j.price||0),changePct:+(j.percent_change||0)};}catch(e){return null;}}
async function fetchVWAP(sym,interval,key){if(interval==='1day')return null;try{const r=await fetch(`${TD}/vwap?symbol=${sym}&interval=${interval}&outputsize=1&apikey=${key}`);const j=await r.json();if(j.status==='error')return null;return+(j.values?.[0]?.vwap||0)||null;}catch(e){return null;}}

// ─── CLAUDE GRADER ───────────────────────────────────────────
async function aiGrade(sym,ind,antKey){
  if(!antKey)return null;
  const p=`You are a quant signal grader. Return ONLY valid JSON.
TICKER:${sym} PRICE:$${ind.px.toFixed(2)} COMPOSITE:${ind.comp}/100
RSI=${ind.rsiV?.toFixed(2)}[${ind.sigs.RSI?.label}] MACD_HIST=${ind.hlV?.toFixed(4)}[${ind.sigs.MACD?.label}]
BB_PCT=${ind.bbPct}%[${ind.sigs.BB?.label}] VWAP[${ind.sigs.VWAP?.label}] SMA[${ind.sigs.SMA?.label}] EMA[${ind.sigs.EMA?.label}]
ATR=$${ind.atr?.toFixed(2)} SLOPE=${ind.slp?.toFixed(5)} MOM=${ind.mom?.toFixed(2)}%
STRATEGY: target +1% per trade, max 3 business day hold, T+3 clearing. Stocks pre-screened for gap-up momentum.
Return ONLY: {"grade":"B","score":72,"action":"BUY","confidence":70,"reason":"one sentence","target":${(ind.px*1.012).toFixed(2)},"stop":${(ind.px*.988).toFixed(2)},"strongest":"MACD","avoid":false}`;
  try{
    const r=await fetch('https://api.anthropic.com/v1/messages',{method:'POST',headers:{'Content-Type':'application/json','x-api-key':antKey,'anthropic-version':'2023-06-01'},body:JSON.stringify({model:'claude-sonnet-4-20250514',max_tokens:200,messages:[{role:'user',content:p}]})});
    if(!r.ok)return null;const d=await r.json();
    return JSON.parse(d.content.map(c=>c.text||'').join('').replace(/```json|```/g,'').trim());
  }catch(e){return null;}
}

// ─── MARKET RULES ────────────────────────────────────────────
function bizAdd(d,n){let r=new Date(d),c=0;while(c<n){r.setDate(r.getDate()+1);if(r.getDay()!==0&&r.getDay()!==6)c++;}return r;}
const clearOk=lt=>!lt||new Date()>=bizAdd(new Date(lt),3);
function isMarketOpen(){const et=new Date(new Date().toLocaleString('en-US',{timeZone:'America/New_York'}));if(et.getDay()===0||et.getDay()===6)return false;const m=et.getHours()*60+et.getMinutes();return m>=570&&m<960;}
function countBizDays(s,e){let c=0,d=new Date(s);while(d<e){d.setDate(d.getDate()+1);if(d.getDay()!==0&&d.getDay()!==6)c++;}return c;}

// ─── ORDER PLACEMENT ─────────────────────────────────────────
async function placeOrder(userId,sym,action,shares,price,cfg){
  if(cfg.sandbox){await log(userId,'skip',`[SANDBOX] ${action} ${shares}sh ${sym} @ $${price.toFixed(2)}`);return{success:true,sandboxed:true};}
  if(!cfg.schwab_client_id||!cfg.schwab_account_hash){await log(userId,'err','Schwab not configured');return{success:false};}
  try{
    const accessToken=await ensureValidToken(userId,cfg);
    await placeSchwabOrder(sym,action,shares,accessToken,cfg.schwab_account_hash);
    return{success:true};
  }catch(e){await log(userId,'err',`Schwab order error: ${e.message}`);return{success:false};}
}

// ─── LEARNING ────────────────────────────────────────────────
function updateCoefs(coefs,snap,result){
  const clamp=v=>+Math.max(0.05,Math.min(3,v)).toFixed(3);
  const win=result==='WIN',updated={...coefs};
  Object.keys(updated).forEach(k=>{const s=snap?.[k];if(!s)return;const correct=(win&&s.bull===true)||(!win&&s.bull===false);updated[k]=clamp(updated[k]+(s.bull===null?0:correct?0.1:-0.06));});
  return updated;
}

// ─── POSITION MONITOR ────────────────────────────────────────
async function monitorPosition(userId,state,cfg){
  const t=state.open_trade;if(!t)return;
  let px=t.entry_price;
  try{
    if(!cfg.sandbox&&cfg.schwab_client_id){const at=await ensureValidToken(userId,cfg);const q=await getSchwabQuote(t.symbol,at);if(q?.price>0)px=q.price;}
    else{const q=await fetchQuote(t.symbol,cfg.td_key);if(q?.price>0)px=q.price;}
  }catch(e){}
  const pnl=(px-t.entry_price)/t.entry_price*100;
  const days=countBizDays(new Date(t.entry_time),new Date());
  if(pnl>=1.0||px<=t.stop_price||days>=3){
    const why=pnl>=1.0?'+1% target':px<=t.stop_price?'stop hit':'3-day expiry';
    const result=pnl>=0?'WIN':'LOSS';
    await log(userId,result==='WIN'?'win':'loss',`${t.symbol} CLOSE — ${why} | ${pnl>=0?'+':''}${pnl.toFixed(2)}% @ $${px.toFixed(2)}`);
    const res=await placeOrder(userId,t.symbol,'SELL',t.shares,px,cfg);
    if(!res.success&&!res.sandboxed)return;
    const newCoefs=updateCoefs(state.coefs,t.ind_snapshot,result);
    const newBal=state.balance*(1+pnl/100);
    if(t.db_id)await sbPatch('trades',{exit_price:px,result,pnl:+pnl.toFixed(3),exit_time:new Date().toISOString()},`?id=eq.${t.db_id}`);
    await patchState(userId,{balance:newBal,open_trade:null,last_trade_date:new Date().toISOString(),coefs:newCoefs,status_text:`SOLD ${t.symbol} ${pnl>=0?'+':''}${pnl.toFixed(2)}%`});
  }else{
    await patchState(userId,{status_text:`Holding ${t.symbol} ${pnl>=0?'+':''}${pnl.toFixed(2)}% | day ${days}/3`});
  }
}

// ─── SCAN CYCLE ──────────────────────────────────────────────
const scanning=new Set();
async function scanUser(userId){
  if(scanning.has(userId))return;scanning.add(userId);
  try{
    const[state,cfg]=await Promise.all([getState(userId),getCfg(userId)]);
    if(!state?.running){scanning.delete(userId);return;}
    if(state.open_trade){await monitorPosition(userId,state,cfg);scanning.delete(userId);return;}
    if(!isMarketOpen()){await patchState(userId,{status_text:'Market closed — idle'});scanning.delete(userId);return;}
    if(!clearOk(state.last_trade_date)){const next=bizAdd(new Date(state.last_trade_date),3);await patchState(userId,{status_text:`Clearing lock — next: ${next.toLocaleDateString('en-US',{month:'short',day:'numeric'})}`});scanning.delete(userId);return;}
    if(!cfg.sandbox&&cfg.schwab_client_id){
      const oauth=await getSchwab(userId);
      if(!oauth?.authorized||refreshTokenExpired(oauth.refresh_token_updated_at)){await patchState(userId,{status_text:'Schwab auth required — open app to connect'});scanning.delete(userId);return;}
    }
    const watchlist=(cfg.watchlist||'').split(',').map(s=>s.trim()).filter(Boolean);
    if(!watchlist.length){await patchState(userId,{status_text:'Watchlist empty — screener runs 9AM ET'});scanning.delete(userId);return;}
    const idx=(state.scan_idx||0)%watchlist.length,sym=watchlist[idx];
    await log(userId,'scan',`${sym} [${idx+1}/${watchlist.length}]`);
    await patchState(userId,{scan_idx:(state.scan_idx||0)+1,status_text:`Scanning ${sym}...`});
    const candles=await fetchCandles(sym,cfg.interval,cfg.lookback,cfg.td_key);
    let lv=null;if(cfg.interval!=='1day'){try{lv=await fetchVWAP(sym,cfg.interval,cfg.td_key);}catch(e){}}
    const ind=analyze(candles,lv,state.coefs);
    await patchState(userId,{last_analysis:{symbol:sym,...ind}});
    let px=ind.px;try{const q=await fetchQuote(sym,cfg.td_key);if(q?.price>0)px=q.price;}catch(e){}
    let grade=null;if(cfg.ant_key){grade=await aiGrade(sym,ind,cfg.ant_key);if(grade)await patchState(userId,{last_grade:grade});}
    const GRADES=['A','B','C','D','F'];
    const scoreOk=ind.comp>=cfg.min_score;
    const gOk=grade?GRADES.indexOf(grade.grade)<=GRADES.indexOf(cfg.min_grade)&&!grade.avoid:ind.comp>=(cfg.min_score+8);
    const isBuy=grade?grade.action==='BUY':ind.comp>=cfg.min_score;
    if(scoreOk&&gOk&&isBuy){
      const shares=Math.max(1,Math.floor(state.balance*(cfg.pos_pct/100)/px));
      const target=grade?.target?+grade.target:+(px*1.012).toFixed(2);
      // ATR-based stop: gives more room in volatile markets, tighter in calm ones
      // Uses 1.5x ATR distance, clamped between 1.2% and 3% below entry
      const atrStop = ind.atr ? px - (ind.atr * 1.5) : null;
      const stop = grade?.stop ? +grade.stop
        : atrStop ? +Math.min(px * 0.988, Math.max(px * 0.970, atrStop)).toFixed(2)
        : +(px * 0.988).toFixed(2);
      await log(userId,'buy',`SIGNAL ${sym} | Grade=${grade?.grade||'rule'} Score=${ind.comp} | BUY ${shares}sh @ $${px.toFixed(2)}`);
      const res=await placeOrder(userId,sym,'BUY',shares,px,cfg);
      if(res.success||res.sandboxed){
        const rows=await sbPost('trades',{user_id:userId,symbol:sym,action:'BUY',shares,entry_price:px,target,stop_price:stop,result:'OPEN',grade:grade?.grade||'--',score:ind.comp,entry_time:new Date().toISOString(),ind_snapshot:ind.sigs});
        const dbId=Array.isArray(rows)?rows[0]?.id:null;
        await patchState(userId,{open_trade:{db_id:dbId,symbol:sym,shares,entry_price:px,target,stop_price:stop,entry_time:new Date().toISOString(),grade:grade?.grade||'--',score:ind.comp,ind_snapshot:ind.sigs},last_trade_date:new Date().toISOString(),status_text:`BOUGHT ${shares}sh ${sym} @ $${px.toFixed(2)}`});
      }
    }else{
      const why=!scoreOk?`score ${ind.comp}<${cfg.min_score}`:`grade/signal`;
      await patchState(userId,{status_text:`${sym} skipped — ${why}`});
    }
  }catch(err){await log(userId,'err',err.message);try{await patchState(userId,{status_text:'Error: '+err.message});}catch(e){}}
  scanning.delete(userId);
}

// ─── MULTI-USER LOOP ─────────────────────────────────────────
let masterInterval=null;
async function getAllActiveUsers(){
  const states=await sbGet('bot_state','?running=eq.true');
  const result=[];
  for(const s of states){const cfg=await getCfg(s.user_id);if(cfg)result.push({userId:s.user_id,cfg});}
  return result;
}
function startMasterLoop(){
  if(masterInterval)return;
  console.log('[BOT] Master loop started');
  masterInterval=setInterval(async()=>{
    try{const users=await getAllActiveUsers();for(const{userId}of users)scanUser(userId);}
    catch(e){console.error('[BOT]',e.message);}
  },60*1000);
  getAllActiveUsers().then(users=>users.forEach(({userId})=>scanUser(userId)));
}

// ─── SCHWAB OAUTH ROUTES ─────────────────────────────────────
// Callback URL to register at developer.schwab.com:
//   https://your-railway-url.railway.app/schwab/auth/callback

app.get('/schwab/auth/start', async(req,res)=>{
  const userId=req.query.user_id;
  if(!userId)return res.status(400).json({error:'user_id required'});
  try{
    const cfg=await getCfg(userId);
    if(!cfg?.schwab_client_id)return res.status(400).json({error:'Schwab client ID not configured in Config'});
    const{verifier,challenge}=generatePKCE();
    const state=crypto.randomBytes(16).toString('hex');
    const redirectUri=`${BOT_URL}/schwab/auth/callback`;
    // Save verifier temporarily
    await sbUpsert('schwab_oauth',{user_id:userId,code_verifier:verifier,authorized:false},'?user_id=eq.'+userId);
    const authUrl=buildAuthorizeUrl(cfg.schwab_client_id,redirectUri,state,challenge);
    res.redirect(authUrl);
  }catch(e){console.error('[SCHWAB AUTH]',e.message);res.status(500).json({error:e.message});}
});

app.get('/schwab/auth/callback', async(req,res)=>{
  const{code,state,error}=req.query;
  // user_id passed via state parameter or session — for simplicity we use a pending row
  if(error)return res.status(400).send(`Schwab authorization denied: ${error}`);
  if(!code)return res.status(400).send('No authorization code received');
  try{
    // Find pending oauth row (most recently created unauthorized)
    const rows=await sbGet('schwab_oauth','?authorized=eq.false&order=created_at.desc&limit=1');
    const pending=rows[0];
    if(!pending)return res.status(400).send('No pending OAuth session');
    const cfg=await getCfg(pending.user_id);
    const redirectUri=`${BOT_URL}/schwab/auth/callback`;
    const{accessToken,refreshToken,accessTokenExpiresIn}=await exchangeCodeForTokens(code,cfg.schwab_client_id,cfg.schwab_client_secret,redirectUri,pending.code_verifier);
    const now=new Date().toISOString();
    await sbUpsert('schwab_oauth',{user_id:pending.user_id,access_token:accessToken,refresh_token:refreshToken,access_token_updated_at:now,refresh_token_updated_at:now,authorized:true,code_verifier:null},'?user_id=eq.'+pending.user_id);
    await log(pending.user_id,'info','Schwab authorized — tokens saved. Valid for 7 days.');
    res.send(`<!DOCTYPE html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"><style>body{font-family:monospace;background:#050a0f;color:#00e891;display:flex;align-items:center;justify-content:center;height:100vh;margin:0;text-align:center;}p{color:#4a7090;}</style></head><body><div><h1>✓ Schwab Connected</h1><p>Authorization successful. Tokens auto-refresh every 30 min.</p><p style="margin-top:12px;font-size:11px;color:#243c52">Valid for 7 days — you'll get a prompt to reconnect next week.</p><p style="margin-top:20px">You can close this tab.</p></div></body></html>`);
  }catch(e){console.error('[SCHWAB CALLBACK]',e.message);res.status(500).send(`Authorization failed: ${e.message}`);}
});

app.get('/schwab/status', async(req,res)=>{
  const userId=req.query.user_id;
  if(!userId)return res.status(400).json({error:'user_id required'});
  const oauth=await getSchwab(userId);
  res.json({authorized:!!oauth?.authorized,needsRefresh:tokenNeedsRefresh(oauth?.access_token_updated_at),refreshExpired:refreshTokenExpired(oauth?.refresh_token_updated_at),refreshUpdatedAt:oauth?.refresh_token_updated_at||null});
});

// ─── BOT CONTROL ─────────────────────────────────────────────
app.get('/',(req,res)=>res.json({status:'AutoTrader v3.1 (Schwab)',time:new Date().toISOString(),masterActive:!!masterInterval}));
app.get('/health',(req,res)=>res.json({ok:true}));
app.post('/bot/start',async(req,res)=>{const{user_id}=req.body;if(user_id)await sbPatch('bot_state',{running:true,updated_at:new Date().toISOString()},`?user_id=eq.${user_id}`);startMasterLoop();res.json({ok:true});});
app.post('/bot/stop',async(req,res)=>{const{user_id}=req.body;if(user_id)await sbPatch('bot_state',{running:false,status_text:'Stopped',updated_at:new Date().toISOString()},`?user_id=eq.${user_id}`);res.json({ok:true});});
app.post('/screener/run',async(req,res)=>{const{user_id}=req.body;res.json({ok:true});if(user_id){const cfg=await getCfg(user_id);if(cfg)runScreenerForUser(user_id,cfg,SB_URL,SB_KEY).catch(console.error);}else{getAllActiveUsers().then(users=>users.forEach(({userId,cfg})=>runScreenerForUser(userId,cfg,SB_URL,SB_KEY)));}});

// ─── STARTUP ─────────────────────────────────────────────────
const PORT=process.env.PORT||3000;
app.listen(PORT,async()=>{
  console.log(`AutoTrader v3.1 (Schwab) on port ${PORT}`);
  console.log(`Bot URL: ${BOT_URL||'(BOT_URL not set)'}`);
  startScreenerScheduler(getAllActiveUsers,SB_URL,SB_KEY);
  try{const running=await sbGet('bot_state','?running=eq.true');if(running.length){console.log(`Resuming ${running.length} user(s)`);startMasterLoop();}}catch(e){console.error('Startup:',e.message);}
});
