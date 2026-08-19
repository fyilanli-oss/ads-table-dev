"use strict";

const {createClient}=require("@supabase/supabase-js");
const {createProviderTokenVaultFromEnv}=require("./provider-token-vault");
const {createProviderTokenStore}=require("./provider-token-store");
const {createProviderTokenBackfill}=require("./provider-token-backfill");
const {readConfig}=require("./provider-token-backfill-operator");

const CONFIRMATION="E1-T6D-PRODUCTION-WRITE";
const BATCH_SIZE=25;
const COUNTERS=["scanned","eligible","written","alreadyEncrypted","rotationCandidates","empty","failed"];

class BackfillWriteError extends Error{
  constructor(code,stage,evidence){super(code);this.name="BackfillWriteError";this.code=code;this.stage=stage;if(evidence)this.evidence=evidence;}
}
function fail(code,stage,evidence){throw new BackfillWriteError(code,stage,evidence);}

function parseWriteArguments(argv=[]){
  let expectedTotal,confirmation;
  for(let i=0;i<argv.length;i++){
    const argument=String(argv[i]);
    if(argument==="--expected-total"&&expectedTotal===undefined)expectedTotal=argv[++i];
    else if(argument==="--confirm"&&confirmation===undefined)confirmation=argv[++i];
    else fail("BACKFILL_WRITE_ARGUMENT_INVALID","runtime");
  }
  if(confirmation!==CONFIRMATION)fail("BACKFILL_WRITE_CONFIRMATION_REQUIRED","runtime");
  if(typeof expectedTotal!=="string"||!/^\d+$/.test(expectedTotal))fail("BACKFILL_WRITE_EXPECTED_TOTAL_INVALID","runtime");
  const parsed=Number(expectedTotal);
  if(!Number.isInteger(parsed)||parsed<1||parsed>BATCH_SIZE)fail("BACKFILL_WRITE_EXPECTED_TOTAL_INVALID","runtime");
  return Object.freeze({expectedTotal:parsed});
}

function assertRuntime(env={}){
  if(env.GITHUB_ACTIONS!=="true"||env.GITHUB_REF!=="refs/heads/main"||!/^\d+$/.test(env.GITHUB_RUN_ID||"")||!/^[a-f0-9]{40}$/i.test(env.GITHUB_SHA||""))fail("BACKFILL_WRITE_RUNTIME_FORBIDDEN","runtime");
}
function validResult(result){return result&&COUNTERS.every(name=>Number.isInteger(result[name])&&result[name]>=0)&&Array.isArray(result.failures)&&result.failures.length===result.failed;}
function phaseEvidence(result){const evidence={};for(const name of COUNTERS)evidence[name]=result[name];return evidence;}
function safeFailures(result){
  if(!Array.isArray(result?.failures))return [];
  return result.failures.filter(item=>item&&/^[a-f0-9]{64}$/.test(item.userRef||"")&&/^[a-z0-9_-]{1,32}$/i.test(item.platform||"")&&/^[A-Z][A-Z0-9_]{0,63}$/.test(item.code||"")).map(({userRef,platform,code})=>({userRef,platform,code}));
}
function partialEvidence(result){return {scanned:Number.isInteger(result?.scanned)?result.scanned:0,written:Number.isInteger(result?.written)?result.written:0,alreadyEncrypted:Number.isInteger(result?.alreadyEncrypted)?result.alreadyEncrypted:0,failed:Number.isInteger(result?.failed)?result.failed:0,failures:safeFailures(result)};}

function assertPreflight(result,expected){
  if(!validResult(result)||result.scanned!==expected||result.written!==0||result.failed!==0||result.failures.length||result.empty!==0||result.nextCursor!==null||result.eligible+result.alreadyEncrypted!==expected)fail("BACKFILL_WRITE_PREFLIGHT_REJECTED","preflight");
}
function assertWrite(result,preflight,expected){
  if(result?.failed>0)fail("BACKFILL_WRITE_PARTIAL_FAILURE","write",partialEvidence(result));
  if(!validResult(result)||result.scanned!==expected||result.failures.length||result.empty!==0||result.nextCursor!==null||result.written!==preflight.eligible||result.alreadyEncrypted!==preflight.alreadyEncrypted||result.written+result.alreadyEncrypted!==expected)fail("BACKFILL_WRITE_INVARIANT_FAILED","write",partialEvidence(result));
}
function assertVerification(result,expected){
  if(!validResult(result)||result.scanned!==expected||result.eligible!==0||result.written!==0||result.alreadyEncrypted!==expected||result.rotationCandidates!==0||result.empty!==0||result.failed!==0||result.failures.length||result.nextCursor!==null)fail("BACKFILL_WRITE_VERIFICATION_FAILED","verification");
}

async function runWriteOperator({argv=[],env=process.env,dependencies={}}={}){
  const args=parseWriteArguments(argv);
  assertRuntime(env);
  let config;
  try{config=readConfig(env);}catch(error){fail(error?.code==="BACKFILL_CONFIG_MISSING"?"BACKFILL_CONFIG_MISSING":"BACKFILL_CONFIG_INVALID","config");}
  const makeClient=dependencies.createClient||createClient;
  const makeVault=dependencies.createVault||createProviderTokenVaultFromEnv;
  const makeStore=dependencies.createStore||createProviderTokenStore;
  const makeBackfill=dependencies.createBackfill||createProviderTokenBackfill;
  try{
    const vault=makeVault(env);
    const client=makeClient(config.supabaseUrl,config.serviceRoleKey,{auth:{persistSession:false,autoRefreshToken:false,detectSessionInUrl:false}});
    const tokenStore=makeStore({client,vault,legacyReadsEnabled:true});
    const run=makeBackfill({client,tokenStore,userRefSecret:config.referenceSecret});
    const options= dryRun=>({dryRun,batchSize:BATCH_SIZE,cursor:null});
    let preflight;try{preflight=await run(options(true));}catch{fail("BACKFILL_WRITE_PREFLIGHT_FAILED","preflight");}assertPreflight(preflight,args.expectedTotal);
    let write;try{write=await run(options(false));}catch{fail("BACKFILL_WRITE_FAILED","write");}assertWrite(write,preflight,args.expectedTotal);
    let verification;try{verification=await run(options(true));}catch{fail("BACKFILL_WRITE_VERIFICATION_FAILED","verification");}assertVerification(verification,args.expectedTotal);
    return {ok:true,mode:"encrypted-backfill-write",contractVersion:"v1",expectedTotal:args.expectedTotal,preflight:phaseEvidence(preflight),write:phaseEvidence(write),verification:phaseEvidence(verification)};
  }catch(error){
    if(error instanceof BackfillWriteError)throw error;
    if(error?.code==="TOKEN_VAULT_CONFIG_ERROR")fail("BACKFILL_CONFIG_INVALID","config");
    fail("BACKFILL_WRITE_FAILED","write");
  }
}

module.exports={CONFIRMATION,BATCH_SIZE,BackfillWriteError,parseWriteArguments,assertRuntime,runWriteOperator};
