module.exports = async function handler(req,res){
  const KEY='st250-673-20260814-k9r4';
  if(String(req.query?.key||'')!==KEY) return res.status(403).json({error:'forbidden'});
  const n=673;
  const base=process.env.SUPABASE_URL;
  const service=process.env.SUPABASE_SERVICE_ROLE_KEY;
  if(!base||!service) return res.status(500).json({error:'Supabase env missing'});
  const url=`${base}/rest/v1/rpc/adstable_stress250_user`;
  const t0=Date.now();
  const jobs=Array.from({length:n},async(_,idx)=>{
    const user=idx+1; const s=Date.now();
    try{
      const r=await fetch(url,{method:'POST',headers:{Authorization:`Bearer ${service}`,apikey:service,'Content-Type':'application/json'},body:JSON.stringify({p_user:user}),signal:AbortSignal.timeout(180000)});
      const text=await r.text(); let body=null; try{body=JSON.parse(text)}catch{}
      return {user,ok:r.ok,status:r.status,ms:Date.now()-s,body:body||text.slice(0,300)};
    }catch(e){ return {user,ok:false,status:0,ms:Date.now()-s,error:String(e?.message||e)}; }
  });
  const out=await Promise.all(jobs);
  const times=out.map(x=>x.ms).sort((a,b)=>a-b);
  const pct=p=>times[Math.min(times.length-1,Math.max(0,Math.ceil(times.length*p)-1))]||0;
  const success=out.filter(x=>x.ok).length;
  const status_counts={}; for(const x of out) status_counts[x.status]=(status_counts[x.status]||0)+1;
  res.setHeader('Cache-Control','no-store');
  res.status(200).json({scenario:{users:n,entities_per_user:250,total_entities:n*250},success,failed:n-success,elapsed_ms:Date.now()-t0,min_ms:times[0]||0,avg_ms:Math.round(times.reduce((a,b)=>a+b,0)/times.length),p50_ms:pct(.50),p95_ms:pct(.95),p99_ms:pct(.99),max_ms:times[times.length-1]||0,status_counts,failures:out.filter(x=>!x.ok).slice(0,30)});
};
