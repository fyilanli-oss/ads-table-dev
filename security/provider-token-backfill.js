"use strict";

const crypto=require("node:crypto");

const DEFAULT_BATCH_SIZE=25;
const MAX_BATCH_SIZE=100;

function normalizeBatchSize(value){
  const size=Number(value||DEFAULT_BATCH_SIZE);
  if(!Number.isInteger(size)||size<1||size>MAX_BATCH_SIZE)throw new RangeError(`batchSize must be between 1 and ${MAX_BATCH_SIZE}`);
  return size;
}
function encodeCursor(offset){return Buffer.from(JSON.stringify({offset}),"utf8").toString("base64url");}
function redactUserId(userId){return crypto.createHash("sha256").update(String(userId)).digest("hex").slice(0,12);}
function decodeCursor(cursor){
  if(!cursor)return 0;
  try{const value=JSON.parse(Buffer.from(String(cursor),"base64url").toString("utf8"));if(!Number.isInteger(value.offset)||value.offset<0)throw new Error();return value.offset;}
  catch{throw Object.assign(new Error("Invalid provider token backfill cursor"),{code:"INVALID_BACKFILL_CURSOR"});}
}

function createProviderTokenBackfill({client,tokenStore}){
  if(!client||typeof client.from!=="function")throw new TypeError("Provider token backfill requires a Supabase client");
  if(!tokenStore||typeof tokenStore.resolve!=="function"||typeof tokenStore.write!=="function")throw new TypeError("Provider token backfill requires a token store");
  async function listConnections(offset,batchSize){
    const {data,error}=await client.from("platform_connections").select("user_id,platform,access_token,refresh_token").eq("connected",true).order("user_id",{ascending:true}).order("platform",{ascending:true}).range(offset,offset+batchSize-1);
    if(error)throw new Error(error.message);
    return data||[];
  }
  async function runBatch({cursor=null,batchSize=DEFAULT_BATCH_SIZE,dryRun=true}={}){
    const size=normalizeBatchSize(batchSize),offset=decodeCursor(cursor),rows=await listConnections(offset,size);
    const summary={scanned:rows.length,eligible:0,written:0,alreadyEncrypted:0,rotationCandidates:0,empty:0,failed:0};
    const failures=[];
    for(const row of rows){
      try{
        const resolved=await tokenStore.resolve({userId:row.user_id,platform:row.platform,legacyAccessToken:row.access_token||null,legacyRefreshToken:row.refresh_token||null});
        if(resolved.source==="encrypted"&&!resolved.needsRotation){summary.alreadyEncrypted+=1;continue;}
        if(!resolved.accessToken&&!resolved.refreshToken){summary.empty+=1;continue;}
        summary.eligible+=1;
        if(resolved.needsRotation)summary.rotationCandidates+=1;
        if(!dryRun){await tokenStore.write({userId:row.user_id,platform:row.platform,accessToken:resolved.accessToken,refreshToken:resolved.refreshToken});summary.written+=1;}
      }catch(error){summary.failed+=1;failures.push({userRef:redactUserId(row.user_id),platform:row.platform,code:error.code||"BACKFILL_ROW_FAILED"});}
    }
    return {dryRun:Boolean(dryRun),summary,nextCursor:rows.length===size?encodeCursor(offset+rows.length):null,failures};
  }
  return Object.freeze({runBatch});
}

module.exports={DEFAULT_BATCH_SIZE,MAX_BATCH_SIZE,encodeCursor,decodeCursor,redactUserId,createProviderTokenBackfill};
