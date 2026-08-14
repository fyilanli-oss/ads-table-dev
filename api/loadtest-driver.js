module.exports = async function handler(req,res){
  const n=Math.max(1,Math.min(500,Number(req.query?.n||168)));
  const delay=Math.max(0,Math.min(60000,Number(req.query?.delay||30000)));
  const host=req.headers.host;
  const url=`https://${host}/api/loadtest?delay=${delay}`;
  const t0=Date.now();
  const jobs=Array.from({length:n},async(_,i)=>{
    const s=Date.now();
    try{
      const r=await fetch(url,{cache:'no-store',headers:{'x-loadtest-id':String(i+1)}});
      const text=await r.text();
      let body=null; try{body=JSON.parse(text)}catch{}
      return {i:i+1,ok:r.ok,status:r.status,ms:Date.now()-s,body};
    }catch(e){return {i:i+1,ok:false,status:0,ms:Date.now()-s,error:String(e?.message||e)}}
  });
  const out=await Promise.all(jobs);
  const times=out.map(x=>x.ms).sort((a,b)=>a-b);
  const pct=p=>times[Math.min(times.length-1,Math.max(0,Math.ceil(times.length*p)-1))]||0;
  const success=out.filter(x=>x.ok).length;
  const statusCounts={}; for(const x of out) statusCounts[x.status]=(statusCounts[x.status]||0)+1;
  res.setHeader('Cache-Control','no-store');
  res.status(200).json({n,delay_ms:delay,success,failed:n-success,elapsed_ms:Date.now()-t0,min_ms:times[0]||0,avg_ms:Math.round(times.reduce((a,b)=>a+b,0)/times.length),p50_ms:pct(.50),p95_ms:pct(.95),p99_ms:pct(.99),max_ms:times[times.length-1]||0,status_counts:statusCounts,failures:out.filter(x=>!x.ok).slice(0,20)});
};
