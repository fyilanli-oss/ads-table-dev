"use strict";

const crypto=require("node:crypto");

const DEFAULT_BATCH_SIZE=25;
const MAX_BATCH_SIZE=100;

function normalizeBatchSize(value){
  const size=Number(value||DEFAULT_BATCH_SIZE);
  if(!Number.isInteger(size)||size<1||size>MAX_BATCH_SIZE)throw new RangeError(`batchSize must be between 1 and ${MAX_BATCH_SIZE}`);
  return size;
}
function encodeCursor({userId,platform}){
  if(!userId||!platform)throw new TypeError("Provider token backfill cursor identity is incomplete");
  return Buffer.from(JSON.stringify({userId:String(userId),platform:String(platform)}),"utf8").toString("base64url");
}
function redactUserId(userId){return crypto.createHash("sha256").update(String(userId)).digest("hex").slice(0,12);}
function decodeCursor(cursor){
  if(!cursor)return null;
  try{const value=JSON.parse(Buffer.from(String(cursor),"base64url").toString("utf8"));if(!value.userId||!value.platform)throw new Error();return {userId:String(value.userId),platform:String(value.platform)};}
  catch{throw Object.assign(new Error("Invalid provider token backfill cursor"),{code:"INVALID_BACKFILL_CURSOR"});}
}

function createProviderTokenBackfill({client,tokenStore}){
  if(!client||typeof client.from!=="function")throw new TypeError("Provider token backfill requires a Supabase client");
  if(!tokenStore||typeof tokenStore.resolve!=="function"||typeof tokenStore.write!=="function")throw new TypeError("Provider token backfill requires a token store");
  async function listConnections(cursor,batchSize){
    let query=client.from("platform_connections").select("user_id,platform,access_token,refresh_token").eq("connected",true).order("user_id",{ascending:true}).order("platform",{ascending:true}).limit(batchSize);
    if(cursor)query=query.or(`user_id.gt.${cursor.userId},and(user_id.eq.${cursor.userId},platform.gt.${cursor.platform})`);
    const {data,error}=await query;
    if(error)throw new Error(error.message);
    return data||[];
  }
  async function runBatch({cursor=null,batchSize=DEFAULT_BATCH_SIZE,dryRun=true}={}){
    const size=normalizeBatchSize(batchSize),decodedCursor=decodeCursor(cursor),rows=await listConnections(decodedCursor,size);
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
    const lastRow=rows.at(-1);
    return {dryRun:Boolean(dryRun),summary,nextCursor:rows.length===size?encodeCursor({userId:lastRow.user_id,platform:lastRow.platform}):null,failures};
  }
  return Object.freeze({runBatch});
}

module.exports={DEFAULT_BATCH_SIZE,MAX_BATCH_SIZE,encodeCursor,decodeCursor,redactUserId,createProviderTokenBackfill};
