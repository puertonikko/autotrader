// ============================================================
// AutoTrader — Premarket Screener (multi-user)
// ============================================================
const TD = 'https://api.twelvedata.com';

export async function runScreenerForUser(userId, cfg, sbUrl, sbKey) {
  const log = async (type, msg) => {
    console.log(`[SCREENER][${userId.slice(0,8)}][${type}] ${msg}`);
    try { await fetch(`${sbUrl}/rest/v1/activity_log`, { method:'POST', headers:{apikey:sbKey,Authorization:`Bearer ${sbKey}`,'Content-Type':'application/json'}, body:JSON.stringify({user_id:userId,type,message:msg}) }); } catch(e) {}
  };
  if (!cfg.screener_enabled) { await log('info','Screener disabled'); return; }
  if (!cfg.td_key)           { await log('err','No Twelve Data key'); return; }

  const universe = (cfg.screener_universe||'').split(',').map(s=>s.trim()).filter(Boolean);
  const minGap=cfg.screener_min_gap||1.5, minPrice=cfg.screener_min_price||10;
  const maxPrice=cfg.screener_max_price||500, minVol=cfg.screener_min_volume||500000;
  const maxPicks=cfg.screener_max_picks||15;

  await log('scan', `Scanning ${universe.length} stocks for gap-ups...`);
  const BATCH=50, candidates=[];

  for (let i=0; i<universe.length; i+=BATCH) {
    const batch=universe.slice(i,i+BATCH);
    try {
      const r=await fetch(`${TD}/quote?symbol=${batch.join(',')}&apikey=${cfg.td_key}`);
      const j=await r.json();
      const quotes=batch.length===1?(j.status==='error'?{}:{[batch[0]]:j}):j;
      for (const sym of batch) {
        const q=quotes[sym]; if(!q||q.status==='error') continue;
        const px=+q.close||+q.price||0, prevClose=+q.previous_close||0;
        const vol=+q.volume||0, changePct=+q.percent_change||0;
        if(px<minPrice||px>maxPrice||vol<minVol||changePct<minGap||prevClose<=0) continue;
        const gapPct=(px-prevClose)/prevClose*100; if(gapPct<minGap) continue;
        let score=0;
        if(gapPct>=5)score+=40; else if(gapPct>=3)score+=30; else if(gapPct>=2)score+=20; else score+=10;
        const avgVol=+q.average_volume||vol, vr=avgVol>0?vol/avgVol:1;
        if(vr>=3)score+=30; else if(vr>=2)score+=20; else if(vr>=1.5)score+=10;
        if(px>=15&&px<=200)score+=15;
        candidates.push({sym,px,gapPct:+gapPct.toFixed(2),vol,score});
      }
    } catch(e) { await log('err',`Batch error: ${e.message}`); }
    if(i+BATCH<universe.length) await new Promise(r=>setTimeout(r,1500));
  }

  if(!candidates.length) { await log('info','No gap-up candidates found'); return; }
  candidates.sort((a,b)=>b.score-a.score);
  const picks=candidates.slice(0,maxPicks), pickSyms=picks.map(p=>p.sym);
  const stateRows=await (await fetch(`${sbUrl}/rest/v1/bot_state?user_id=eq.${userId}`,{headers:{apikey:sbKey,Authorization:`Bearer ${sbKey}`}})).json();
  const openSym=stateRows[0]?.open_trade?.symbol;
  if(openSym&&!pickSyms.includes(openSym)) pickSyms.push(openSym);
  const summary=picks.map(p=>`${p.sym}(+${p.gapPct}%)`).join(', ');
  await log('info',`Picks: ${summary}`);
  await fetch(`${sbUrl}/rest/v1/config?user_id=eq.${userId}`,{method:'PATCH',headers:{apikey:sbKey,Authorization:`Bearer ${sbKey}`,'Content-Type':'application/json'},body:JSON.stringify({watchlist:pickSyms.join(','),screener_last_run:new Date().toISOString(),screener_last_picks:summary,updated_at:new Date().toISOString()})});
  await log('info',`Watchlist updated — ${pickSyms.length} picks`);
}

export function startScreenerScheduler(getAllActiveUsers, sbUrl, sbKey) {
  console.log('[SCREENER] Scheduler active — 9:00 AM ET weekdays');
  let lastRunDate='';
  setInterval(async()=>{
    const et=new Date(new Date().toLocaleString('en-US',{timeZone:'America/New_York'}));
    const today=et.toDateString();
    if(et.getDay()>=1&&et.getDay()<=5&&et.getHours()===9&&et.getMinutes()===0&&lastRunDate!==today){
      lastRunDate=today;
      try{const users=await getAllActiveUsers();for(const{userId,cfg}of users){try{await runScreenerForUser(userId,cfg,sbUrl,sbKey);}catch(e){console.error('[SCREENER]',e.message);}}}catch(e){console.error('[SCREENER]',e.message);}
    }
  },60*1000);
}
