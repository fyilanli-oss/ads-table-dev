"use strict";

const crypto=require("node:crypto");
const {createClient}=require("@supabase/supabase-js");
const {createProviderTokenVaultFromEnv}=require("../security/provider-token-vault");
const {createProviderTokenStore}=require("../security/provider-token-store");
const {createProviderTokenBackfill,DEFAULT_BATCH_SIZE}=require("../security/provider-token-backfill");

const WRITE_CONFIRMATION="WRITE_ENCRYPTED_TOKENS";
function operatorError(code,message){return Object.assign(new Error(message),{code});}

function parseArgs(argv=[]){
  const options={dryRun:true,batchSize:DEFAULT_BATCH_SIZE,cursor:null,projectRef:null,confirmation:null};
  for(let index=0;index<argv.length;index++){
    const argument=argv[index];
    if(argument==="--write"){options.dryRun=false;continue;}
    const next=argv[++index];
    if(next===undefined)throw operatorError("INVALID_OPERATOR_ARGUMENT",`Missing value for ${argument}`);
    if(argument==="--batch-size")options.batchSize=Number(next);
    else if(argument==="--cursor")options.cursor=next;
    else if(argument==="--project-ref")options.projectRef=next;
    else if(argument==="--confirm")options.confirmation=next;
    else throw operatorError("INVALID_OPERATOR_ARGUMENT",`Unknown argument: ${argument}`);
  }
  return options;
}

function projectOrigin(value){try{return new URL(String(value||"")).origin;}catch{throw operatorError("INVALID_SUPABASE_URL","SUPABASE_URL must be a valid HTTPS project URL");}}
function projectRefFromOrigin(origin){const url=new URL(origin);if(url.protocol!=="https:")throw operatorError("INVALID_SUPABASE_URL","SUPABASE_URL must use HTTPS");const match=/^([a-z0-9]+)\.supabase\.co$/i.exec(url.hostname);if(!match)throw operatorError("INVALID_SUPABASE_URL","SUPABASE_URL must identify a Supabase project origin");return match[1];}

function validateOperatorConfig(options,env={}){
  const origin=projectOrigin(env.SUPABASE_URL);
  const actualProjectRef=projectRefFromOrigin(origin);
  const expectedProjectRef=String(options.projectRef||env.SUPABASE_PROJECT_REF||"").trim();
  if(!expectedProjectRef)throw operatorError("PROJECT_REF_REQUIRED","A project ref guard is required");
  if(expectedProjectRef!==actualProjectRef)throw operatorError("PROJECT_REF_MISMATCH","The target Supabase project does not match the project ref guard");
  if(!env.SUPABASE_SERVICE_ROLE_KEY)throw operatorError("SERVICE_ROLE_REQUIRED","SUPABASE_SERVICE_ROLE_KEY is required");
  if(!env.PROVIDER_TOKEN_BACKFILL_SECRET)throw operatorError("BACKFILL_SECRET_REQUIRED","PROVIDER_TOKEN_BACKFILL_SECRET is required");
  if(!options.dryRun){
    if(env.PROVIDER_TOKEN_BACKFILL_WRITE_ENABLED!=="true")throw operatorError("WRITE_NOT_ENABLED","Write mode is not enabled by environment policy");
    if(options.confirmation!==WRITE_CONFIRMATION)throw operatorError("WRITE_CONFIRMATION_REQUIRED","Write mode requires explicit confirmation");
  }
  return {origin,projectRef:actualProjectRef};
}

function safeFailure(error){const code=String(error?.code||"");return {ok:false,code:/^[A-Z][A-Z0-9_]{0,63}$/.test(code)?code:"TOKEN_BACKFILL_FAILED"};}

async function runOperator({argv=process.argv.slice(2),env=process.env,stdout=process.stdout,stderr=process.stderr}={}){
  try{
    const options=parseArgs(argv),config=validateOperatorConfig(options,env);
    const client=createClient(config.origin,env.SUPABASE_SERVICE_ROLE_KEY,{auth:{persistSession:false,autoRefreshToken:false}});
    const vault=createProviderTokenVaultFromEnv(env);
    const tokenStore=createProviderTokenStore({client,vault,legacyReadsEnabled:true});
    const runBackfill=createProviderTokenBackfill({client,tokenStore,userRefSecret:env.PROVIDER_TOKEN_BACKFILL_SECRET});
    const result=await runBackfill({dryRun:options.dryRun,batchSize:options.batchSize,cursor:options.cursor});
    const evidence={ok:true,runId:crypto.randomUUID(),mode:options.dryRun?"dry-run":"write",projectRef:config.projectRef,batchSize:options.batchSize,...result};
    stdout.write(`${JSON.stringify(evidence)}\n`);
    return evidence;
  }catch(error){const failure=safeFailure(error);stderr.write(`${JSON.stringify(failure)}\n`);return failure;}
}

if(require.main===module)runOperator().then(result=>{if(!result.ok)process.exitCode=1;});
module.exports={WRITE_CONFIRMATION,parseArgs,projectOrigin,projectRefFromOrigin,validateOperatorConfig,safeFailure,runOperator};
