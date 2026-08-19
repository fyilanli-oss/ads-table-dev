"use strict";

const {createClient}=require("@supabase/supabase-js");
const {createProviderTokenVaultFromEnv}=require("./provider-token-vault");
const {createProviderTokenStore}=require("./provider-token-store");
const {DEFAULT_BATCH_SIZE,decodeCursor,createProviderTokenBackfill}=require("./provider-token-backfill");

const CONTRACT_VERSION="v1";
const REQUIRED_ENV=["SUPABASE_URL","SUPABASE_SERVICE_ROLE_KEY","PROVIDER_TOKEN_ACTIVE_KEY_ID","PROVIDER_TOKEN_ENCRYPTION_KEYS","PROVIDER_TOKEN_BACKFILL_REFERENCE_SECRET"];
const WRITE_ARGUMENT=/^(?:--?(?:write|execute|apply)(?:=.*)?|--?dry-?run(?:=.*)?|dryRun(?:=.*)?)$/i;

class BackfillOperatorError extends Error{
  constructor(code){super(code);this.name="BackfillOperatorError";this.code=code;}
}

function fail(code){throw new BackfillOperatorError(code);}

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
  if(Buffer.byteLength(referenceSecret,"utf8")<32)fail("BACKFILL_CONFIG_INVALID");
  const forbidden=[env.SUPABASE_SERVICE_ROLE_KEY,env.SUPABASE_DB_PASSWORD,env.SUPABASE_ACCESS_TOKEN,env.PROVIDER_TOKEN_ENCRYPTION_KEYS].filter(Boolean);
  if(forbidden.includes(referenceSecret))fail("BACKFILL_CONFIG_INVALID");
  let url;
  try{url=new URL(env.SUPABASE_URL);}catch{fail("BACKFILL_CONFIG_INVALID");}
  if(url.protocol!=="https:")fail("BACKFILL_CONFIG_INVALID");
  return Object.freeze({supabaseUrl:env.SUPABASE_URL,serviceRoleKey:env.SUPABASE_SERVICE_ROLE_KEY,referenceSecret});
}

function safeEvidence(result,batchSize){
  const failures=Array.isArray(result?.failures)?result.failures.map(item=>({
    userRef:typeof item?.userRef==="string"&&/^[a-f0-9]{64}$/.test(item.userRef)?item.userRef:"",
    platform:typeof item?.platform==="string"&&/^[a-z0-9_-]{1,32}$/i.test(item.platform)?item.platform:"unknown",
    code:typeof item?.code==="string"&&/^[A-Z][A-Z0-9_]{0,63}$/.test(item.code)?item.code:"BACKFILL_ROW_FAILED"
  })):[];
  return {ok:true,mode:"dry-run",contractVersion:CONTRACT_VERSION,scanned:Number(result?.scanned)||0,eligible:Number(result?.eligible)||0,written:0,alreadyEncrypted:Number(result?.alreadyEncrypted)||0,rotationCandidates:Number(result?.rotationCandidates)||0,empty:Number(result?.empty)||0,failed:Number(result?.failed)||0,nextCursor:typeof result?.nextCursor==="string"?result.nextCursor:null,failures,batchSize};
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
    return safeEvidence(result,args.batchSize);
  }catch(error){
    if(error?.code==="INVALID_CURSOR")fail("BACKFILL_CURSOR_INVALID");
    if(error?.code==="BACKFILL_SCAN_FAILED")fail("BACKFILL_SCAN_FAILED");
    if(error?.code==="TOKEN_VAULT_CONFIG_ERROR")fail("BACKFILL_CONFIG_INVALID");
    fail("BACKFILL_DRY_RUN_FAILED");
  }
}

module.exports={CONTRACT_VERSION,BackfillOperatorError,parseArguments,readConfig,safeEvidence,runDryRunOperator};
