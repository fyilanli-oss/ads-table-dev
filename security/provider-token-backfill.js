"use strict";

const crypto=require("node:crypto");

const DEFAULT_BATCH_SIZE=25;
const MAX_BATCH_SIZE=100;

function cursorKey(secret){if(!secret)throw Object.assign(new TypeError("Cursor secret is required"),{code:"INVALID_CURSOR"});return crypto.createHash("sha256").update(String(secret)).digest();}

function encodeCursor({userId,platform},secret){
  if(typeof userId!=="string"||!userId||typeof platform!=="string"||!platform)throw Object.assign(new TypeError("Invalid backfill cursor key"),{code:"INVALID_CURSOR"});
  const iv=crypto.randomBytes(12);
  const cipher=crypto.createCipheriv("aes-256-gcm",cursorKey(secret),iv);
  const ciphertext=Buffer.concat([cipher.update(JSON.stringify({u:userId,p:platform}),"utf8"),cipher.final()]);
  return ["v1",iv.toString("base64url"),cipher.getAuthTag().toString("base64url"),ciphertext.toString("base64url")].join(".");
}

function decodeCursor(value,secret){
  if(value===null||value===undefined||value==="")return null;
  try{
    const [version,iv,tag,ciphertext,...extra]=String(value).split(".");
    if(version!=="v1"||!iv||!tag||!ciphertext||extra.length)throw new Error();
    const decipher=crypto.createDecipheriv("aes-256-gcm",cursorKey(secret),Buffer.from(iv,"base64url"));
    decipher.setAuthTag(Buffer.from(tag,"base64url"));
    const parsed=JSON.parse(Buffer.concat([decipher.update(Buffer.from(ciphertext,"base64url")),decipher.final()]).toString("utf8"));
    if(typeof parsed.u!=="string"||!parsed.u||typeof parsed.p!=="string"||!parsed.p||Object.keys(parsed).length!==2)throw new Error();
    return {userId:parsed.u,platform:parsed.p};
  }catch{throw Object.assign(new TypeError("Invalid backfill cursor"),{code:"INVALID_CURSOR"});}
}

function validateBatchSize(value){
  const size=value===undefined?DEFAULT_BATCH_SIZE:value;
  if(!Number.isInteger(size)||size<1||size>MAX_BATCH_SIZE)throw Object.assign(new RangeError("Batch size must be an integer from 1 to 100"),{code:"INVALID_BATCH_SIZE"});
  return size;
}

function safeErrorCode(error){
  const code=String(error?.code||"");
  return /^[A-Z][A-Z0-9_]{0,63}$/.test(code)?code:"BACKFILL_ROW_FAILED";
}

function userRef(userId,secret){return crypto.createHmac("sha256",secret).update(String(userId)).digest("hex");}

async function selectConnections(client,cursor,limit){
  let query=client.from("platform_connections").select("user_id,platform,access_token,refresh_token").eq("connected",true).order("user_id",{ascending:true}).order("platform",{ascending:true}).limit(limit);
  if(cursor)query=query.or(`user_id.gt.${cursor.userId},and(user_id.eq.${cursor.userId},platform.gt.${cursor.platform})`);
  const {data,error}=await query;
  if(error)throw Object.assign(new Error("Connection scan failed"),{code:"BACKFILL_SCAN_FAILED"});
  return data||[];
}

function createProviderTokenBackfill({client,tokenStore,userRefSecret,listConnections=selectConnections}){
  if(!client||typeof client.from!=="function")throw new TypeError("Backfill requires a database client");
  if(!tokenStore||typeof tokenStore.resolve!=="function"||typeof tokenStore.write!=="function")throw new TypeError("Backfill requires an encrypted token store");
  if(!userRefSecret)throw new TypeError("Backfill requires a user reference secret");

  return async function run(options={}){
    const dryRun=options.dryRun!==false;
    const batchSize=validateBatchSize(options.batchSize);
    const cursor=decodeCursor(options.cursor,userRefSecret);
    const rows=await listConnections(client,cursor,batchSize);
    const result={scanned:0,eligible:0,written:0,alreadyEncrypted:0,rotationCandidates:0,empty:0,failed:0,nextCursor:null,failures:[]};
    for(const row of rows.slice(0,batchSize)){
      result.scanned++;
      try{
        const resolved=await tokenStore.resolve({userId:row.user_id,platform:row.platform,legacyAccessToken:row.access_token||null,legacyRefreshToken:row.refresh_token||null});
        if(!resolved.accessToken&&!resolved.refreshToken){result.empty++;continue;}
        if(resolved.source==="encrypted"&&!resolved.needsRotation){result.alreadyEncrypted++;continue;}
        result.eligible++;
        if(resolved.needsRotation)result.rotationCandidates++;
        if(!dryRun){await tokenStore.write({userId:row.user_id,platform:row.platform,accessToken:resolved.accessToken,refreshToken:resolved.refreshToken});result.written++;}
      }catch(error){
        result.failed++;
        result.failures.push({userRef:userRef(row.user_id,userRefSecret),platform:String(row.platform||"unknown"),code:safeErrorCode(error)});
      }
    }
    if(rows.length===batchSize)result.nextCursor=encodeCursor({userId:rows[rows.length-1].user_id,platform:rows[rows.length-1].platform},userRefSecret);
    return result;
  };
}

module.exports={DEFAULT_BATCH_SIZE,MAX_BATCH_SIZE,encodeCursor,decodeCursor,validateBatchSize,createProviderTokenBackfill};
