"use strict";

const crypto=require("node:crypto");
const {createClient}=require("@supabase/supabase-js");
const {parseKeyring,createProviderTokenVaultFromEnv}=require("./provider-token-vault");
const {createProviderTokenStore}=require("./provider-token-store");
const {DEFAULT_BATCH_SIZE,decodeCursor,createProviderTokenBackfill}=require("./provider-token-backfill");

const CONTRACT_VERSION="v1";
const REQUIRED_ENV=["SUPABASE_URL","SUPABASE_PROJECT_REF","SUPABASE_SERVICE_ROLE_KEY","PROVIDER_TOKEN_ACTIVE_KEY_ID","PROVIDER_TOKEN_ENCRYPTION_KEYS","PROVIDER_TOKEN_BACKFILL_REFERENCE_SECRET"];
const COUNTERS=["scanned","eligible","written","alreadyEncrypted","rotationCandidates","empty","failed"];
const WRITE_ARGUMENT=/^(?:--?(?:write|execute|apply)(?:=.*)?|--?dry-?run(?:=.*)?|dryRun(?:=.*)?)$/i;

class BackfillOperatorError extends Error{
  constructor(code){super(code);this.name="BackfillOperatorError";this.code=code;}
}

function fail(code){throw new BackfillOperatorError(code);}

function sameSecret(left,right){
  const leftHash=crypto.createHash("sha256").update(left).digest();
  const rightHash=crypto.createHash("sha256").update(right).digest();
  return crypto.timingSafeEqual(leftHash,rightHash);
}

function decodeReferenceSecret(value){
  if(!/^[A-Za-z0-9+/]{43}=$/.test(value))fail("BACKFILL_CONFIG_INVALID");
  const decoded=Buffer.from(value,"base64");
  if(decoded.length!==32||decoded.toString("base64")!==value)fail("BACKFILL_CONFIG_INVALID");
  if(new Set(decoded).size<16)fail("BACKFILL_CONFIG_INVALID");
  return decoded;
}

function parseArguments(argv=[]){
  const result={batchSize:DEFAULT_BATCH_SIZE,cursor:null};
  for(let index=0;index<argv.length;index++){
    const argument=String(argv[index]);
    if(WRITE_ARGUMENT.test(argument))fail("BACKFILL_WRITE_MODE_FORBIDDEN");
    if(argument==="--batch-size"){
      const value=argv[++index];
      if(value===undefined||!/^[0-9]+$/.test(String(value)))fail("BACKFILL_ARGUMENT_INVALID");
      result.batchSize=Number(value);
      if(!Number.isInteger(result.batchSize)||result.batchSize<1||result.batchSize>100)fail("BACKFILL_ARGUMENT_INVALID");
    }else if(argument==="--cursor"){
      const value=argv[++index];
      if(value===undefined||!String(value))fail("BACKFILL_ARGUMENT_INVALID");
      result.cursor=String(value);
    }else fail("BACKFILL_ARGUMENT_INVALID");
  }
  return Object.freeze(result);
}

function readConfig(env={}){
  if(REQUIRED_ENV.some(name=>typeof env[name]!=="string"||!env[name].trim()))fail("BACKFILL_CONFIG_MISSING");
  const referenceSecret=env.PROVIDER_TOKEN_BACKFILL_REFERENCE_SECRET;
  const referenceBytes=decodeReferenceSecret(referenceSecret);
  const sensitiveValues=[env.SUPABASE_SERVICE_ROLE_KEY,env.SUPABASE_DB_PASSWORD,env.SUPABASE_ACCESS_TOKEN].filter(value=>typeof value==="string"&&value);
  if(sensitiveValues.some(value=>sameSecret(referenceSecret,value)))fail("BACKFILL_CONFIG_INVALID");
  let keyring;
  try{keyring=parseKeyring(env);}catch{fail("BACKFILL_CONFIG_INVALID");}
  if([...keyring.keys.values()].some(key=>key.length===referenceBytes.length&&crypto.timingSafeEqual(key,referenceBytes)))fail("BACKFILL_CONFIG_INVALID");
  const projectRef=env.SUPABASE_PROJECT_REF.trim();
  if(!/^[a-z0-9]{20}$/.test(projectRef))fail("BACKFILL_CONFIG_INVALID");
  let url;
  try{url=new URL(env.SUPABASE_URL);}catch{fail("BACKFILL_CONFIG_INVALID");}
  if(url.protocol!=="https:"||url.host!==`${projectRef}.supabase.co`||url.username||url.password||url.pathname!=="/"||url.search||url.hash)fail("BACKFILL_CONFIG_INVALID");
  return Object.freeze({supabaseUrl:env.SUPABASE_URL,serviceRoleKey:env.SUPABASE_SERVICE_ROLE_KEY,referenceSecret});
}

function safeEvidence(result,batchSize,referenceSecret){
  if(!result||COUNTERS.some(name=>!Number.isInteger(result[name])||result[name]<0)||result.written!==0)fail("BACKFILL_DRY_RUN_INVARIANT_FAILED");
  if(!Array.isArray(result.failures)||result.failures.length!==result.failed)fail("BACKFILL_DRY_RUN_INVARIANT_FAILED");
  if(result.failures.some(item=>!item||typeof item!=="object"||
    typeof item.userRef!=="string"||!/^[a-f0-9]{64}$/.test(item.userRef)||
    typeof item.platform!=="string"||!/^[a-z0-9_-]{1,32}$/i.test(item.platform)||
    typeof item.code!=="string"||!/^[A-Z][A-Z0-9_]{0,63}$/.test(item.code)))fail("BACKFILL_DRY_RUN_INVARIANT_FAILED");
  if(result.nextCursor!==null){
    if(typeof result.nextCursor!=="string"||!result.nextCursor)fail("BACKFILL_DRY_RUN_INVARIANT_FAILED");
    try{decodeCursor(result.nextCursor,referenceSecret);}catch{fail("BACKFILL_DRY_RUN_INVARIANT_FAILED");}
  }
  const failures=result.failures.map(item=>({userRef:item.userRef,platform:item.platform,code:item.code}));
  return {ok:true,mode:"dry-run",contractVersion:CONTRACT_VERSION,scanned:result.scanned,eligible:result.eligible,written:result.written,alreadyEncrypted:result.alreadyEncrypted,rotationCandidates:result.rotationCandidates,empty:result.empty,failed:result.failed,nextCursor:typeof result.nextCursor==="string"?result.nextCursor:null,failures,batchSize};
}

async function runDryRunOperator({argv=[],env=process.env,dependencies={}}={}){
  const args=parseArguments(argv);
  const config=readConfig(env);
  try{decodeCursor(args.cursor,config.referenceSecret);}catch{fail("BACKFILL_CURSOR_INVALID");}
  const makeClient=dependencies.createClient||createClient;
  const makeVault=dependencies.createVault||createProviderTokenVaultFromEnv;
  const makeStore=dependencies.createStore||createProviderTokenStore;
  const makeBackfill=dependencies.createBackfill||createProviderTokenBackfill;
  try{
    const vault=makeVault(env);
    const client=makeClient(config.supabaseUrl,config.serviceRoleKey,{auth:{persistSession:false,autoRefreshToken:false,detectSessionInUrl:false}});
    const tokenStore=makeStore({client,vault,legacyReadsEnabled:true});
    const run=makeBackfill({client,tokenStore,userRefSecret:config.referenceSecret});
    const result=await run({dryRun:true,batchSize:args.batchSize,cursor:args.cursor});
    return safeEvidence(result,args.batchSize,config.referenceSecret);
  }catch(error){
    if(error instanceof BackfillOperatorError)throw error;
    if(error?.code==="INVALID_CURSOR")fail("BACKFILL_CURSOR_INVALID");
    if(error?.code==="BACKFILL_SCAN_FAILED")fail("BACKFILL_SCAN_FAILED");
    if(error?.code==="TOKEN_VAULT_CONFIG_ERROR")fail("BACKFILL_CONFIG_INVALID");
    fail("BACKFILL_DRY_RUN_FAILED");
  }
}

module.exports={CONTRACT_VERSION,BackfillOperatorError,parseArguments,readConfig,safeEvidence,runDryRunOperator};
