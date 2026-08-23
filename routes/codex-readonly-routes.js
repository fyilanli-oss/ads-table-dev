"use strict";

const crypto=require("node:crypto");
const express=require("express");
const {CONTRACT_VERSION,OPERATIONS}=require("../security/codex-readonly-contract");

function createRateLimiter({limit=30,windowMs=60_000,now=Date.now}={}){
  let started=now(),count=0;
  return ()=>{const current=now();if(current-started>=windowMs){started=current;count=0;}count+=1;return count<=limit;};
}

function createCodexReadonlyRouter({auth,service,logger=console,rateLimiter=createRateLimiter()}={}){
  if(!auth||!service)throw new TypeError("auth and service are required");
  const router=express.Router();
  router.use((req,res,next)=>{res.set("Cache-Control","no-store");next();});
  router.get("/:operation",async(req,res)=>{
    const started=process.hrtime.bigint();
    const requestId=typeof req.get("X-Request-ID")==="string"&&/^[A-Za-z0-9._-]{1,80}$/.test(req.get("X-Request-ID"))?req.get("X-Request-ID"):crypto.randomUUID();
    const operation=String(req.params.operation||"");
    let status=500,rateLimitResult="allowed";
    res.set("X-Request-ID",requestId);
    try{
      if(!OPERATIONS.includes(operation)){status=404;return res.status(status).json({error:"Not found",requestId});}
      const result=auth.authenticate(req.get("Authorization"));
      if(result.configurationUnavailable){status=503;return res.status(status).json({error:"Service unavailable",requestId});}
      if(!result.ok){status=401;return res.status(status).json({error:"Unauthorized",requestId});}
      if(Object.keys(req.query).length){status=400;return res.status(status).json({error:"Query parameters are not accepted",requestId});}
      if(!rateLimiter()){status=429;rateLimitResult="limited";return res.status(status).json({error:"Too many requests",requestId});}
      const payload=await service.execute(operation);
      status=200;
      return res.status(status).json(payload);
    }catch(_error){status=502;return res.status(status).json({error:"Read-only gateway request failed",requestId});}
    finally{
      const durationMs=Number(process.hrtime.bigint()-started)/1e6;
      logger.info(JSON.stringify({requestId,operation,status,durationMs:Number(durationMs.toFixed(3)),contractVersion:CONTRACT_VERSION,rateLimitResult}));
    }
  });
  return router;
}

module.exports={createRateLimiter,createCodexReadonlyRouter};
