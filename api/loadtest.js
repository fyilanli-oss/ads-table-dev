module.exports = async function handler(req,res){
  const started=Date.now();
  const delayMs=Math.max(0,Math.min(60000,Number(req.query?.delay||30000)));
  await new Promise(r=>setTimeout(r,delayMs));
  res.setHeader('Cache-Control','no-store');
  res.status(200).json({ok:true,delay_ms:delayMs,elapsed_ms:Date.now()-started,ts:new Date().toISOString()});
};
